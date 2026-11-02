//! Disposable-script undo. Only touched documents, metadata entries and cursors are retained.
//! Dropped indexes and CLEAR's catalog are moved into the journal, never eagerly copied.
use super::*;

struct Metadata {
    next_id: DocId,
    total_length: usize,
    errors: u64,
    last_error: Option<String>,
    last_error_key: Option<Vec<u8>>,
    generation: u64,
    deadline: Option<i64>,
    time: f64,
    uses: u64,
    dialects: [u64; 4],
}
#[expect(
    clippy::large_enum_variant,
    reason = "Undo records own moved state without an extra allocation for each captured index."
)]
enum Undo {
    Document(String, DocId, Option<Document>),
    Index(String, Option<Index>),
    Schema(String, Schema),
    Metadata(String, Metadata),
    Alias(String, Option<String>),
    Dictionary(String, bool, Vec<(String, bool)>),
    Synonym(String, String, Option<BTreeSet<String>>),
    FieldError(String, String, Option<(u64, String, Vec<u8>)>),
    Cursor(u64, Option<Cursor>),
    Config(Config),
    Clear(Box<State>),
}
pub(super) struct Journal {
    undo: Vec<Undo>,
    seen_docs: BTreeSet<(u64, String, DocId)>,
    seen_metadata: BTreeSet<(u64, String)>,
    seen_cursors: BTreeSet<(u64, u64)>,
    epoch: u64,
    next_cursor: u64,
    revision: u64,
    gc: gc::Runtime,
    generation: u64,
}
impl Journal {
    pub(super) fn clear(&mut self, old: State) {
        self.undo.push(Undo::Clear(Box::new(old)));
        self.epoch += 1;
    }
}
impl State {
    pub(in super::super) fn begin_rollback(&mut self) -> io::Result<()> {
        if self.rollback.is_some() {
            return Err(io::Error::other("search rollback already active"));
        }
        self.records.undo = Some(BTreeMap::new());
        self.rollback = Some(Journal {
            undo: Vec::new(),
            seen_docs: BTreeSet::new(),
            seen_metadata: BTreeSet::new(),
            seen_cursors: BTreeSet::new(),
            epoch: 0,
            next_cursor: self.next_cursor,
            revision: self.metadata_revision,
            gc: self.gc.clone(),
            generation: self.generation,
        });
        Ok(())
    }
    pub(in super::super) fn save_index_metadata(&mut self, name: &str) {
        let Some(j) = &mut self.rollback else { return };
        let Some(i) = self.indexes.get(name) else {
            return;
        };
        if !j.seen_metadata.insert((j.epoch, name.to_owned())) {
            return;
        }
        j.undo.push(Undo::Metadata(
            name.to_owned(),
            Metadata {
                next_id: i.next_id,
                total_length: i.total_length,
                errors: i.errors,
                last_error: i.last_error.clone(),
                last_error_key: i.last_error_key.clone(),
                generation: i.synonym_generation,
                deadline: i.schema.deadline,
                time: i.indexing_time_ms,
                uses: i.uses,
                dialects: i.dialect_uses,
            },
        ));
    }
    pub(in super::super) fn save_config(&mut self) {
        if let Some(j) = &mut self.rollback {
            j.undo.push(Undo::Config(self.config.clone()));
        }
        self.generation = self.generation.saturating_add(1);
    }
    pub(in super::super) fn take_drop_document_keys(&mut self, name: &str) -> Vec<Bytes> {
        if self.rollback.is_some() {
            self.indexes
                .get(name)
                .into_iter()
                .flat_map(|i| i.docs.values().map(|d| Bytes::copy_from_slice(&d.key)))
                .collect()
        } else {
            self.indexes
                .remove(name)
                .into_iter()
                .flat_map(|i| i.docs.into_values().map(|d| Bytes::from(d.key)))
                .collect()
        }
    }
    pub(super) fn save_cursor(&mut self, id: u64) {
        if let Some(j) = &mut self.rollback
            && j.seen_cursors.insert((j.epoch, id))
        {
            j.undo
                .push(Undo::Cursor(id, self.cursors.get(&id).cloned()));
        }
    }
    fn save_document(&mut self, name: &str, id: DocId) {
        self.save_index_metadata(name);
        let Some(j) = &mut self.rollback else { return };
        if j.seen_docs.insert((j.epoch, name.to_owned(), id))
            && let Some(index) = self.indexes.get_mut(name)
        {
            for field in index.fields.values_mut() {
                if let FieldIndex::Vector(v) = field {
                    v.begin_rollback();
                }
            }
            j.undo.push(Undo::Document(
                name.to_owned(),
                id,
                index.docs.get(&id).cloned(),
            ));
        }
    }
    pub(super) fn save_effect(&mut self, e: &Effect) -> Result<(), String> {
        if self.rollback.is_none() {
            return Ok(());
        }
        match e {
            Effect::Header { index, id, .. }
            | Effect::Payload { index, id, .. }
            | Effect::Field { index, id, .. }
            | Effect::Remove { index, id } => self.save_document(index, *id),
            Effect::Create(s) => {
                if self.indexes.contains_key(&s.name) {
                    return Err("Index already exists".into());
                }
                let j = self.rollback.as_mut().unwrap();
                j.epoch += 1;
                j.undo.push(Undo::Index(s.name.clone(), None));
            }
            Effect::Alter(s) => {
                let old = self
                    .indexes
                    .get(&s.name)
                    .ok_or("Unknown Index name")?
                    .schema
                    .clone();
                self.rollback
                    .as_mut()
                    .unwrap()
                    .undo
                    .push(Undo::Schema(s.name.clone(), old));
            }
            Effect::Drop(name) => {
                let aliases = self
                    .aliases
                    .iter()
                    .filter(|(_, target)| *target == name)
                    .map(|(alias, target)| (alias.clone(), target.clone()))
                    .collect::<Vec<_>>();
                let cursors = self
                    .cursors
                    .iter()
                    .filter(|(_, c)| &c.index == name)
                    .map(|(id, _)| *id)
                    .collect::<Vec<_>>();
                let old = self.indexes.remove(name);
                let j = self.rollback.as_mut().unwrap();
                j.epoch += 1;
                j.undo.push(Undo::Index(name.clone(), old));
                for (alias, target) in aliases {
                    j.undo.push(Undo::Alias(alias, Some(target)));
                }
                for id in cursors {
                    self.save_cursor(id);
                }
            }
            Effect::Alias { alias, .. } => self
                .rollback
                .as_mut()
                .unwrap()
                .undo
                .push(Undo::Alias(alias.clone(), self.aliases.get(alias).cloned())),
            Effect::DictionaryAdd { name, terms } | Effect::DictionaryRemove { name, terms } => {
                let old = self.dictionaries.get(name);
                let values = terms
                    .iter()
                    .map(|term| (term.clone(), old.is_some_and(|d| d.contains(term))))
                    .collect();
                self.rollback.as_mut().unwrap().undo.push(Undo::Dictionary(
                    name.clone(),
                    old.is_some(),
                    values,
                ));
            }
            Effect::Synonyms { index, group, .. } => {
                self.save_index_metadata(index);
                let old = self
                    .indexes
                    .get(index)
                    .ok_or("Unknown Index name")?
                    .synonyms
                    .get(group)
                    .cloned();
                self.rollback.as_mut().unwrap().undo.push(Undo::Synonym(
                    index.clone(),
                    group.clone(),
                    old,
                ));
            }
            Effect::FieldError { index, field, .. } => {
                let old = self
                    .indexes
                    .get(index)
                    .ok_or("Unknown Index name")?
                    .field_errors
                    .get(field)
                    .cloned();
                self.rollback.as_mut().unwrap().undo.push(Undo::FieldError(
                    index.clone(),
                    field.clone(),
                    old,
                ));
            }
            Effect::Rejection { index, .. }
            | Effect::Touch { index, .. }
            | Effect::Generation { index, .. } => self.save_index_metadata(index),
            Effect::Clear => {}
        }
        Ok(())
    }
    pub(in super::super) fn rollback(&mut self) -> io::Result<()> {
        let Some(mut journal) = self.rollback.take() else {
            return Err(io::Error::other("search rollback not active"));
        };
        let mut failure = None;
        while let Some(undo) = journal.undo.pop() {
            match undo {
                Undo::Clear(old) => *self = *old,
                Undo::Index(name, old) => {
                    self.indexes.remove(&name);
                    self.deadlines.retain(|(_, n, _)| n != &name);
                    self.temp_deadlines.retain(|(_, n)| n != &name);
                    if let Some(index) = old {
                        for doc in index.docs.values() {
                            if let Some(d) =
                                doc.expires_at.into_iter().chain(doc.field_deadline).min()
                            {
                                self.deadlines.insert((d, name.clone(), doc.id));
                            }
                        }
                        if let Some(d) = index.schema.deadline {
                            self.temp_deadlines.insert((d, name.clone()));
                        }
                        self.indexes.insert(name, index);
                    }
                }
                Undo::Document(name, id, old) => {
                    if let Some(index) = self.indexes.get_mut(&name) {
                        if let Some(doc) = index.docs.get(&id)
                            && let Some(d) =
                                doc.expires_at.into_iter().chain(doc.field_deadline).min()
                        {
                            self.deadlines.remove(&(d, name.clone(), id));
                        }
                        indexing::remove_for_rollback(index, id);
                        if let Some(doc) = old {
                            for (field, value) in &doc.fields {
                                if let Err(e) =
                                    indexing::insert_for_rollback(index, id, field, value)
                                {
                                    failure.get_or_insert_with(|| io::Error::other(e));
                                }
                            }
                            if let Some(d) =
                                doc.expires_at.into_iter().chain(doc.field_deadline).min()
                            {
                                self.deadlines.insert((d, name.clone(), id));
                            }
                            index.keys.insert(doc.key.clone(), id);
                            index.docs.insert(id, doc);
                        }
                    } else {
                        failure.get_or_insert_with(|| {
                            io::Error::other("missing search index during document rollback")
                        });
                    }
                }
                Undo::Schema(name, schema) => {
                    if let Some(index) = self.indexes.get_mut(&name) {
                        if let Some(d) = index.schema.deadline {
                            self.temp_deadlines.remove(&(d, name.clone()));
                        }
                        if let Some(d) = schema.deadline {
                            self.temp_deadlines.insert((d, name.clone()));
                        }
                        index
                            .fields
                            .retain(|name, _| schema.fields.iter().any(|f| &f.name == name));
                        index
                            .suffixes
                            .retain(|name, _| schema.fields.iter().any(|f| &f.name == name));
                        index
                            .missing
                            .retain(|name, _| schema.fields.iter().any(|f| &f.name == name));
                        index
                            .empty
                            .retain(|name, _| schema.fields.iter().any(|f| &f.name == name));
                        index.schema = schema;
                    }
                }
                Undo::Metadata(name, m) => {
                    if let Some(i) = self.indexes.get_mut(&name) {
                        if let Some(d) = i.schema.deadline {
                            self.temp_deadlines.remove(&(d, name.clone()));
                        }
                        if let Some(d) = m.deadline {
                            self.temp_deadlines.insert((d, name.clone()));
                        }
                        i.next_id = m.next_id;
                        i.total_length = m.total_length;
                        i.errors = m.errors;
                        i.last_error = m.last_error;
                        i.last_error_key = m.last_error_key;
                        i.synonym_generation = m.generation;
                        i.schema.deadline = m.deadline;
                        i.indexing_time_ms = m.time;
                        i.uses = m.uses;
                        i.dialect_uses = m.dialects;
                    }
                }
                Undo::Alias(name, old) => {
                    self.aliases.remove(&name);
                    if let Some(old) = old {
                        self.aliases.insert(name, old);
                    }
                }
                Undo::Dictionary(name, existed, terms) => {
                    if existed {
                        let dict = self.dictionaries.entry(name).or_default();
                        for (term, present) in terms {
                            if present {
                                dict.insert(term);
                            } else {
                                dict.remove(&term);
                            }
                        }
                    } else {
                        self.dictionaries.remove(&name);
                    }
                }
                Undo::Synonym(name, group, old) => {
                    if let Some(i) = self.indexes.get_mut(&name) {
                        i.synonyms.remove(&group);
                        if let Some(old) = old {
                            i.synonyms.insert(group, old);
                        }
                    }
                }
                Undo::FieldError(name, field, old) => {
                    if let Some(i) = self.indexes.get_mut(&name) {
                        i.field_errors.remove(&field);
                        if let Some(old) = old {
                            i.field_errors.insert(field, old);
                        }
                    }
                }
                Undo::Cursor(id, old) => {
                    self.cursor_remove(id);
                    if let Some(old) = old {
                        self.cursor_insert(id, old);
                    }
                }
                Undo::Config(old) => self.config = old,
            }
        }
        self.rollback = None;
        if let Some(records) = self.records.undo.take() {
            for (key, old) in records {
                if let Some(old) = old {
                    self.records.set(key, old);
                } else {
                    self.records.remove(&key);
                }
            }
        }
        let touched = journal
            .seen_docs
            .iter()
            .map(|(_, name, _)| name)
            .collect::<BTreeSet<_>>();
        for name in touched {
            if let Some(index) = self.indexes.get_mut(name) {
                for field in index.fields.values_mut() {
                    if let FieldIndex::Vector(v) = field {
                        v.rollback();
                    }
                }
            }
        }
        self.next_cursor = journal.next_cursor;
        self.metadata_revision = journal.revision;
        self.gc = journal.gc;
        self.generation = journal.generation;
        failure.map_or(Ok(()), Err)
    }
}
