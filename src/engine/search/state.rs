use super::{indexing, model::*};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    io,
    sync::Arc,
};
const CHUNK: usize = 256;
const MAX_EFFECT: usize = 16 * 1024 * 1024;
#[path = "gc.rs"]
pub(super) mod gc;
#[path = "rollback.rs"]
mod rollback;
#[derive(Serialize, Deserialize)]
pub enum Effect {
    Clear,
    Create(Schema),
    Alter(Schema),
    Drop(String),
    Alias {
        alias: String,
        index: Option<String>,
    },
    DictionaryAdd {
        name: String,
        terms: BTreeSet<String>,
    },
    DictionaryRemove {
        name: String,
        terms: BTreeSet<String>,
    },
    Synonyms {
        index: String,
        group: String,
        terms: BTreeSet<String>,
    },
    Header {
        index: String,
        id: DocId,
        key: Vec<u8>,
        score: f64,
        expires_at: Option<i64>,
        field_deadline: Option<i64>,
        length: usize,
        synonym_generation: u64,
    },
    Payload {
        index: String,
        id: DocId,
        value: Option<Vec<u8>>,
    },
    Field {
        index: String,
        id: DocId,
        name: String,
        value: IndexedValue,
    },
    Remove {
        index: String,
        id: DocId,
    },
    Rejection {
        index: String,
        count: u64,
        message: String,
        key: Vec<u8>,
    },
    FieldError {
        index: String,
        field: String,
        count: u64,
        message: String,
        key: Vec<u8>,
    },
    Touch {
        index: String,
        deadline: i64,
    },
    Generation {
        index: String,
        generation: u64,
    },
}
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
enum RecordKey {
    Schema(String),
    Alias(String),
    Dictionary(String, Option<String>),
    Synonym(String, String),
    Header(String, DocId),
    Payload(String, DocId),
    Field(String, DocId, String),
    Error(String),
    FieldError(String, String),
    Touch(String),
    Generation(String),
}
#[derive(Default)]
struct Records {
    chunks: [Vec<Arc<Vec<Option<Bytes>>>>; 8],
    slots: BTreeMap<RecordKey, (usize, usize)>,
    free: [Vec<usize>; 8],
    undo: Option<BTreeMap<RecordKey, Option<Bytes>>>,
}
impl Records {
    fn remember(&mut self, key: &RecordKey) {
        if self.undo.as_ref().is_some_and(|u| !u.contains_key(key)) {
            let old = self
                .slots
                .get(key)
                .and_then(|&(group, slot)| self.chunks[group][slot / CHUNK][slot % CHUNK].clone());
            self.undo
                .as_mut()
                .expect("active record rollback")
                .insert(key.clone(), old);
        }
    }
    fn set(&mut self, key: RecordKey, value: Bytes) {
        self.remember(&key);
        let group = match &key {
            RecordKey::Schema(_) => 0,
            RecordKey::Synonym(..) => 1,
            RecordKey::Header(..) => 2,
            RecordKey::Field(..) | RecordKey::Payload(..) => 3,
            RecordKey::Alias(_) => 4,
            RecordKey::Dictionary(..) => 5,
            RecordKey::Error(_) | RecordKey::FieldError(..) => 6,
            RecordKey::Touch(_) | RecordKey::Generation(_) => 7,
        };
        let slot = if let Some(&(_, slot)) = self.slots.get(&key) {
            slot
        } else {
            let slot = self.free[group].pop().unwrap_or_else(|| {
                if self.chunks[group].last().is_none_or(|c| c.len() == CHUNK) {
                    self.chunks[group].push(Arc::new(Vec::with_capacity(CHUNK)));
                }
                let c = self.chunks[group].len() - 1;
                let n = self.chunks[group][c].len();
                Arc::make_mut(&mut self.chunks[group][c]).push(None);
                c * CHUNK + n
            });
            self.slots.insert(key, (group, slot));
            slot
        };
        Arc::make_mut(&mut self.chunks[group][slot / CHUNK])[slot % CHUNK] = Some(value);
    }
    fn remove(&mut self, key: &RecordKey) {
        self.remember(key);
        if let Some((group, slot)) = self.slots.remove(key) {
            Arc::make_mut(&mut self.chunks[group][slot / CHUNK])[slot % CHUNK] = None;
            self.free[group].push(slot);
        }
    }
    fn drop_index(&mut self, name: &str) {
        let keys: Vec<_> = self
            .slots
            .keys()
            .filter(|k| match k {
                RecordKey::Schema(n)
                | RecordKey::Synonym(n, _)
                | RecordKey::Header(n, _)
                | RecordKey::Payload(n, _)
                | RecordKey::Field(n, _, _)
                | RecordKey::Error(n)
                | RecordKey::FieldError(n, _)
                | RecordKey::Touch(n)
                | RecordKey::Generation(n) => n == name,
                _ => false,
            })
            .cloned()
            .collect();
        for k in keys {
            self.remove(&k)
        }
    }
}
pub struct SearchSnapshot {
    chunks: Vec<Arc<Vec<Option<Bytes>>>>,
    chunk: usize,
    offset: usize,
    clear: bool,
}
impl SearchSnapshot {
    pub(super) fn empty() -> Self {
        Self {
            chunks: Vec::new(),
            chunk: 0,
            offset: 0,
            clear: true,
        }
    }
}
impl Iterator for SearchSnapshot {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.clear {
            self.clear = false;
            return Some(encode(&Effect::Clear));
        }
        while let Some(c) = self.chunks.get(self.chunk) {
            while self.offset < c.len() {
                let pos = self.offset;
                self.offset += 1;
                if let Some(b) = &c[pos] {
                    return Some(Ok(b.clone()));
                }
            }
            self.chunk += 1;
            self.offset = 0;
        }
        None
    }
}
#[derive(Clone)]
pub struct Cursor {
    pub index: String,
    pub rows: VecDeque<Row>,
    pub count: usize,
    pub max_idle: u64,
    pub deadline: i64,
    pub total: usize,
}
pub struct State {
    pub indexes: BTreeMap<String, Index>,
    pub aliases: BTreeMap<String, String>,
    pub dictionaries: BTreeMap<String, BTreeSet<String>>,
    pub config: Config,
    pub cursors: BTreeMap<u64, Cursor>,
    pub next_cursor: u64,
    pub deadlines: BTreeSet<(i64, String, DocId)>,
    pub temp_deadlines: BTreeSet<(i64, String)>,
    pub cursor_deadlines: BTreeSet<(i64, u64)>,
    pub metadata_revision: u64,
    records: Records,
    rollback: Option<rollback::Journal>,
    pub gc: gc::Runtime,
    generation: u64,
}
impl Default for State {
    fn default() -> Self {
        Self {
            indexes: BTreeMap::new(),
            aliases: BTreeMap::new(),
            dictionaries: BTreeMap::new(),
            config: Config::default(),
            cursors: BTreeMap::new(),
            next_cursor: 1,
            deadlines: BTreeSet::new(),
            temp_deadlines: BTreeSet::new(),
            cursor_deadlines: BTreeSet::new(),
            metadata_revision: 0,
            records: Records::default(),
            rollback: None,
            gc: gc::Runtime::default(),
            generation: 0,
        }
    }
}
pub fn encode(e: &Effect) -> io::Result<Bytes> {
    let b = serde_json::to_vec(e).map_err(io::Error::other)?;
    if b.len() > MAX_EFFECT {
        return Err(io::Error::other("search effect limit exceeded"));
    }
    Ok(Bytes::from(b))
}
pub fn decode(b: &[u8]) -> io::Result<Effect> {
    if b.len() > MAX_EFFECT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "search effect limit exceeded",
        ));
    }
    serde_json::from_slice(b).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}
impl State {
    pub fn cursor_insert(&mut self, id: u64, cursor: Cursor) {
        self.save_cursor(id);
        if let Some(old) = self.cursors.insert(id, cursor) {
            self.cursor_deadlines.remove(&(old.deadline, id));
        }
        self.cursor_deadlines
            .insert((self.cursors[&id].deadline, id));
    }
    pub fn cursor_remove(&mut self, id: u64) -> Option<Cursor> {
        self.save_cursor(id);
        let cursor = self.cursors.remove(&id)?;
        self.cursor_deadlines.remove(&(cursor.deadline, id));
        Some(cursor)
    }
    pub fn reap_cursors(&mut self, now: i64) {
        while let Some(&(_, id)) = self.cursor_deadlines.first().filter(|(d, _)| *d <= now) {
            self.cursor_remove(id);
        }
    }
    /// Reachable heap allocations only; B-tree allocator metadata is estimated.
    pub fn record_memory_footprint(&self) -> usize {
        use std::mem::size_of;
        let mut bytes = 0_usize;
        let mut chunks = BTreeSet::new();
        let mut payloads = BTreeSet::new();
        for group in &self.records.chunks {
            bytes = bytes.saturating_add(
                group
                    .capacity()
                    .saturating_mul(size_of::<Arc<Vec<Option<Bytes>>>>()),
            );
            for chunk in group {
                if !chunks.insert(Arc::as_ptr(chunk) as usize) {
                    continue;
                }
                bytes =
                    bytes.saturating_add(size_of::<Vec<Option<Bytes>>>() + 2 * size_of::<usize>());
                bytes = bytes
                    .saturating_add(chunk.capacity().saturating_mul(size_of::<Option<Bytes>>()));
                for record in chunk.iter().flatten() {
                    if payloads.insert((record.as_ptr() as usize, record.len())) {
                        bytes = bytes
                            .saturating_add(record.len())
                            .saturating_add(3 * size_of::<usize>());
                    }
                }
            }
        }
        for free in &self.records.free {
            bytes = bytes.saturating_add(free.capacity().saturating_mul(size_of::<usize>()));
        }
        for key in self.records.slots.keys() {
            bytes = bytes
                .saturating_add(size_of::<(RecordKey, (usize, usize))>() + 3 * size_of::<usize>());
            let owned = match key {
                RecordKey::Schema(a)
                | RecordKey::Alias(a)
                | RecordKey::Header(a, _)
                | RecordKey::Payload(a, _)
                | RecordKey::Error(a)
                | RecordKey::Touch(a)
                | RecordKey::Generation(a) => a.capacity(),
                RecordKey::Dictionary(a, b) => a
                    .capacity()
                    .saturating_add(b.as_ref().map_or(0, String::capacity)),
                RecordKey::Synonym(a, b)
                | RecordKey::Field(a, _, b)
                | RecordKey::FieldError(a, b) => a.capacity().saturating_add(b.capacity()),
            };
            bytes = bytes.saturating_add(owned);
        }
        bytes
    }
    pub fn resolve(&self, name: &str) -> Result<&str, String> {
        if let Some((key, _)) = self.indexes.get_key_value(name) {
            Ok(key)
        } else {
            self.aliases
                .get(name)
                .map(String::as_str)
                .ok_or_else(|| "Unknown Index name".into())
        }
    }
    pub fn snapshot(&self) -> SearchSnapshot {
        SearchSnapshot {
            chunks: self
                .records
                .chunks
                .iter()
                .flat_map(|group| group.iter().cloned())
                .collect(),
            chunk: 0,
            offset: 0,
            clear: true,
        }
    }
    pub fn apply_bytes(&mut self, b: Bytes) -> io::Result<()> {
        let e = decode(&b)?;
        self.apply(e, b)
            .map_err(|s| io::Error::new(io::ErrorKind::InvalidData, s))
    }
    pub fn apply(&mut self, e: Effect, b: Bytes) -> Result<(), String> {
        let revision = if matches!(
            &e,
            Effect::Clear
                | Effect::Create(_)
                | Effect::Alter(_)
                | Effect::Drop(_)
                | Effect::Alias { .. }
                | Effect::DictionaryAdd { .. }
                | Effect::DictionaryRemove { .. }
                | Effect::Synonyms { .. }
                | Effect::Generation { .. }
        ) {
            self.metadata_revision
                .checked_add(1)
                .ok_or("Search metadata revision exhausted")?
        } else {
            self.metadata_revision
        };
        let generation = self
            .generation
            .checked_add(1)
            .ok_or("Search catalog generation exhausted")?;
        self.gc_dirty(&e);
        self.save_effect(&e)?;
        let record = match e {
            Effect::Clear => {
                let journal = self.rollback.take();
                let config = self.config.clone();
                let next_cursor = self.next_cursor;
                let metadata_revision = self.metadata_revision;
                let old = std::mem::take(self);
                self.config = config;
                self.next_cursor = next_cursor;
                self.metadata_revision = metadata_revision;
                if let Some(mut journal) = journal {
                    journal.clear(old);
                    self.rollback = Some(journal);
                    self.records.undo = Some(BTreeMap::new());
                }
                None
            }
            Effect::Create(s) => {
                if self.indexes.contains_key(&s.name) {
                    return Err("Index already exists".into());
                }
                if let Some(deadline) = s.deadline {
                    self.temp_deadlines.insert((deadline, s.name.clone()));
                }
                let key = RecordKey::Schema(s.name.clone());
                self.indexes.insert(s.name.clone(), indexing::new_index(s));
                Some(key)
            }
            Effect::Alter(s) => {
                let index = self.indexes.get_mut(&s.name).ok_or("Unknown Index name")?;
                if let Some(old) = index.schema.deadline {
                    self.temp_deadlines.remove(&(old, s.name.clone()));
                }
                if let Some(deadline) = s.deadline {
                    self.temp_deadlines.insert((deadline, s.name.clone()));
                }
                let key = RecordKey::Schema(s.name.clone());
                index.schema = s;
                indexing::reset_fields(index);
                let canonical =
                    encode(&Effect::Create(index.schema.clone())).map_err(|e| e.to_string())?;
                self.records.set(key, canonical);
                None
            }
            Effect::Drop(name) => {
                self.indexes.remove(&name);
                let aliases: Vec<_> = self
                    .aliases
                    .iter()
                    .filter(|(_, n)| **n == name)
                    .map(|(a, _)| a.clone())
                    .collect();
                for a in aliases {
                    self.aliases.remove(&a);
                    self.records.remove(&RecordKey::Alias(a));
                }
                let cursors: Vec<_> = self
                    .cursors
                    .iter()
                    .filter(|(_, c)| c.index == name)
                    .map(|(id, _)| *id)
                    .collect();
                for id in cursors {
                    self.cursor_remove(id);
                }
                self.deadlines.retain(|(_, n, _)| n != &name);
                self.temp_deadlines.retain(|(_, n)| n != &name);
                self.records.drop_index(&name);
                None
            }
            Effect::Alias { alias, index } => {
                let key = RecordKey::Alias(alias.clone());
                if let Some(index) = index {
                    if !self.indexes.contains_key(&index) {
                        return Err("Unknown Index name".into());
                    }
                    self.aliases.insert(alias, index);
                    Some(key)
                } else {
                    self.aliases.remove(&alias);
                    self.records.remove(&key);
                    None
                }
            }
            Effect::DictionaryAdd { name, terms } => {
                let dict = self.dictionaries.entry(name.clone()).or_default();
                let header = Effect::DictionaryAdd {
                    name: name.clone(),
                    terms: BTreeSet::new(),
                };
                self.records.set(
                    RecordKey::Dictionary(name.clone(), None),
                    encode(&header).map_err(|e| e.to_string())?,
                );
                for term in terms {
                    dict.insert(term.clone());
                    let e = Effect::DictionaryAdd {
                        name: name.clone(),
                        terms: BTreeSet::from([term.clone()]),
                    };
                    self.records.set(
                        RecordKey::Dictionary(name.clone(), Some(term)),
                        encode(&e).map_err(|e| e.to_string())?,
                    );
                }
                None
            }
            Effect::DictionaryRemove { name, terms } => {
                if let Some(dict) = self.dictionaries.get_mut(&name) {
                    for term in terms {
                        dict.remove(&term);
                        self.records
                            .remove(&RecordKey::Dictionary(name.clone(), Some(term)));
                    }
                }
                None
            }
            Effect::Synonyms {
                index,
                group,
                terms,
            } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                let key = RecordKey::Synonym(index.clone(), group.clone());
                i.synonym_generation = i
                    .synonym_generation
                    .checked_add(1)
                    .ok_or("Synonym generation exhausted")?;
                i.synonyms.insert(group, terms);
                let generation = i.synonym_generation;
                let canonical = encode(&Effect::Generation {
                    index: index.clone(),
                    generation,
                })
                .map_err(|e| e.to_string())?;
                self.records.set(RecordKey::Generation(index), canonical);
                Some(key)
            }
            Effect::Generation { index, generation } => {
                self.indexes
                    .get_mut(&index)
                    .ok_or("Unknown Index name")?
                    .synonym_generation = generation;
                Some(RecordKey::Generation(index))
            }
            Effect::Header {
                index,
                id,
                key,
                score,
                expires_at,
                field_deadline,
                length,
                synonym_generation,
            } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                if let Some(doc) = i.docs.get_mut(&id) {
                    if let Some(d) = doc.expires_at.into_iter().chain(doc.field_deadline).min() {
                        self.deadlines.remove(&(d, index.clone(), id));
                    }
                    i.total_length = i
                        .total_length
                        .saturating_sub(doc.length)
                        .saturating_add(length);
                    doc.score = score;
                    doc.expires_at = expires_at;
                    doc.field_deadline = field_deadline;
                    doc.length = length;
                    doc.synonym_generation = synonym_generation;
                } else {
                    i.total_length = i.total_length.saturating_add(length);
                    i.keys.insert(key.clone(), id);
                    i.docs.insert(
                        id,
                        Document {
                            id,
                            key,
                            fields: BTreeMap::new(),
                            score,
                            payload: None,
                            expires_at,
                            field_deadline,
                            length,
                            synonym_generation,
                        },
                    );
                }
                if let Some(d) = expires_at.into_iter().chain(field_deadline).min() {
                    self.deadlines.insert((d, index.clone(), id));
                }
                i.next_id = i.next_id.max(id.saturating_add(1));
                Some(RecordKey::Header(index, id))
            }
            Effect::Payload { index, id, value } => {
                self.indexes
                    .get_mut(&index)
                    .ok_or("Unknown Index name")?
                    .docs
                    .get_mut(&id)
                    .ok_or("Missing document header")?
                    .payload = value;
                Some(RecordKey::Payload(index, id))
            }
            Effect::Field {
                index,
                id,
                name,
                value,
            } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                let old = i
                    .docs
                    .get_mut(&id)
                    .ok_or("Missing document header")?
                    .fields
                    .remove(&name);
                if let Some(old) = old {
                    indexing::unindex(i, id, &name, &old);
                }
                indexing::insert(i, id, &name, &value)?;
                i.docs
                    .get_mut(&id)
                    .expect("document exists")
                    .fields
                    .insert(name.clone(), value);
                Some(RecordKey::Field(index, id, name))
            }
            Effect::Remove { index, id } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                if let Some(doc) = i.docs.get(&id) {
                    if let Some(d) = doc.expires_at.into_iter().chain(doc.field_deadline).min() {
                        self.deadlines.remove(&(d, index.clone(), id));
                    }
                    i.total_length = i.total_length.saturating_sub(doc.length);
                    let fields: Vec<_> = doc.fields.keys().cloned().collect();
                    for name in fields {
                        self.records
                            .remove(&RecordKey::Field(index.clone(), id, name));
                    }
                }
                indexing::remove(i, id);
                self.records.remove(&RecordKey::Payload(index.clone(), id));
                self.records.remove(&RecordKey::Header(index, id));
                None
            }
            Effect::Rejection {
                index,
                count,
                message,
                key,
            } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                i.errors = count;
                i.last_error = Some(message);
                i.last_error_key = Some(key);
                Some(RecordKey::Error(index))
            }
            Effect::FieldError {
                index,
                field,
                count,
                message,
                key,
            } => {
                self.indexes
                    .get_mut(&index)
                    .ok_or("Unknown Index name")?
                    .field_errors
                    .insert(field.clone(), (count, message, key));
                Some(RecordKey::FieldError(index, field))
            }
            Effect::Touch { index, deadline } => {
                let i = self.indexes.get_mut(&index).ok_or("Unknown Index name")?;
                if let Some(old) = i.schema.deadline {
                    self.temp_deadlines.remove(&(old, index.clone()));
                }
                i.schema.deadline = Some(deadline);
                self.temp_deadlines.insert((deadline, index.clone()));
                Some(RecordKey::Touch(index))
            }
        };
        if let Some(key) = record {
            self.records.set(key, b)
        }
        self.metadata_revision = revision;
        self.generation = generation;
        Ok(())
    }
    pub fn effect(&mut self, e: Effect) -> Result<Bytes, String> {
        let b = encode(&e).map_err(|e| e.to_string())?;
        self.apply(e, b.clone())?;
        Ok(b)
    }
}
