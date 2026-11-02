//! Slot migration transfers resolved coverage, never a scan of source Redis values.
//! The caller pins export under its initial barrier, preflights globals under the
//! destination barrier, and journals every returned destination effect exactly once.
use super::super::Engine;
use super::{
    model::DocId,
    state::{self, Effect, SearchSnapshot, State},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
};

const VERSION: u8 = 1;
const MAX_RECORD: usize = 64 * 1024 * 1024;

type Identity = (String, DocId);

#[derive(Serialize, Deserialize)]
struct Envelope {
    version: u8,
    record: Record,
}
#[derive(Serialize, Deserialize)]
enum Record {
    Metadata(Vec<u8>),
    MetadataEnd,
    Document(Vec<u8>),
    SnapshotEnd,
    Live(Vec<u8>),
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}
fn pack(record: Record) -> io::Result<Bytes> {
    let bytes = serde_json::to_vec(&Envelope {
        version: VERSION,
        record,
    })
    .map_err(io::Error::other)?;
    if bytes.len() > MAX_RECORD {
        return Err(invalid("search migration record exceeds limit"));
    }
    Ok(Bytes::from(bytes))
}
fn unpack(bytes: &[u8]) -> io::Result<Record> {
    if bytes.len() > MAX_RECORD {
        return Err(invalid("search migration record exceeds limit"));
    }
    let envelope: Envelope = serde_json::from_slice(bytes).map_err(|e| invalid(e.to_string()))?;
    if envelope.version != VERSION {
        return Err(invalid("unsupported search migration version"));
    }
    Ok(envelope.record)
}
fn ranges_owned(ranges: &[(u16, u16)]) -> io::Result<Vec<(u16, u16)>> {
    if ranges.is_empty() || ranges.iter().any(|&(a, b)| a > b || b >= 16384) {
        return Err(invalid("invalid search migration slot ranges"));
    }
    Ok(ranges.to_vec())
}
fn selected(ranges: &[(u16, u16)], key: &[u8]) -> bool {
    let slot = crate::cluster::key_slot(key);
    ranges
        .iter()
        .any(|&(start, end)| start <= slot && slot <= end)
}
fn identity(effect: &Effect) -> Option<(&str, DocId)> {
    match effect {
        Effect::Header { index, id, .. }
        | Effect::Payload { index, id, .. }
        | Effect::Field { index, id, .. }
        | Effect::Remove { index, id } => Some((index, *id)),
        _ => None,
    }
}

/// Pinned chunk iterators retain source values while updates replace only chunks.
/// Metadata is sent first so incompatibilities are discovered before documents.
pub(crate) struct SearchMigrationSnapshot {
    metadata: SearchSnapshot,
    documents: SearchSnapshot,
    selected: BTreeMap<String, BTreeSet<DocId>>,
    phase: u8,
}
impl Iterator for SearchMigrationSnapshot {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.phase {
                0 => match self.metadata.next() {
                    Some(Ok(bytes)) => match state::decode(&bytes) {
                        Ok(Effect::Clear) => continue,
                        Ok(effect) if identity(&effect).is_some() => continue,
                        Ok(_) => return Some(pack(Record::Metadata(bytes.to_vec()))),
                        Err(error) => {
                            self.phase = 3;
                            return Some(Err(error));
                        }
                    },
                    Some(Err(error)) => {
                        self.phase = 3;
                        return Some(Err(error));
                    }
                    None => {
                        self.phase = 1;
                        return Some(pack(Record::MetadataEnd));
                    }
                },
                1 => match self.documents.next() {
                    Some(Ok(bytes)) => match state::decode(&bytes) {
                        Ok(effect) => {
                            if let Some((index, id)) = identity(&effect)
                                && self
                                    .selected
                                    .get(index)
                                    .is_some_and(|ids| ids.contains(&id))
                            {
                                return Some(pack(Record::Document(bytes.to_vec())));
                            }
                        }
                        Err(error) => {
                            self.phase = 3;
                            return Some(Err(error));
                        }
                    },
                    Some(Err(error)) => {
                        self.phase = 3;
                        return Some(Err(error));
                    }
                    None => {
                        self.phase = 2;
                        return Some(pack(Record::SnapshotEnd));
                    }
                },
                _ => return None,
            }
        }
    }
}

/// This is seeded from initial Headers, not the already-mutated live catalog.
/// In particular a Remove still resolves after the source document has vanished.
pub(crate) struct SearchMigrationFilter {
    ranges: Vec<(u16, u16)>,
    keys: BTreeMap<Identity, Vec<u8>>,
    failed: bool,
}
struct IndexImport {
    existing: bool,
    synonyms: usize,
    field_errors: usize,
    rejection: bool,
    generation: u64,
    deadline: Option<i64>,
}
struct DictionaryImport {
    existing: bool,
    terms: usize,
}

/// IDs are local to each catalog. Only the binary key crosses that identity seam.
pub(crate) struct SearchMigrationImport {
    ranges: Vec<(u16, u16)>,
    indexes: BTreeMap<String, IndexImport>,
    dictionaries: BTreeMap<String, DictionaryImport>,
    documents: BTreeMap<Identity, (DocId, Vec<u8>)>,
    coverage: BTreeMap<String, BTreeSet<Vec<u8>>>,
    revision: Option<u64>,
    phase: u8,
    failed: bool,
}
impl SearchMigrationImport {
    pub(crate) fn new(ranges: &[(u16, u16)]) -> io::Result<Self> {
        Ok(Self {
            ranges: ranges_owned(ranges)?,
            indexes: BTreeMap::new(),
            dictionaries: BTreeMap::new(),
            documents: BTreeMap::new(),
            coverage: BTreeMap::new(),
            revision: None,
            phase: 0,
            failed: false,
        })
    }
    pub(crate) fn is_ready(&self) -> bool {
        self.phase == 2 && !self.failed
    }
}

// Deadlines have their own canonical Touch record; the original Create record
// need not have been rewritten when a temporary index's idle deadline moved.
fn same_schema(a: &super::model::Schema, b: &super::model::Schema) -> io::Result<bool> {
    let mut a = serde_json::to_value(a).map_err(io::Error::other)?;
    let mut b = serde_json::to_value(b).map_err(io::Error::other)?;
    a["deadline"] = serde_json::Value::Null;
    b["deadline"] = serde_json::Value::Null;
    Ok(a == b)
}
fn metadata(
    state: &State,
    context: &mut SearchMigrationImport,
    effect: &Effect,
) -> io::Result<bool> {
    let conflict = || invalid("incompatible destination search migration metadata");
    match effect {
        Effect::Create(schema) => {
            if context.indexes.contains_key(&schema.name)
                || state.aliases.contains_key(&schema.name)
            {
                return Err(conflict());
            }
            let existing = state.indexes.get(&schema.name);
            if let Some(index) = existing
                && !same_schema(&index.schema, schema)?
            {
                return Err(conflict());
            }
            context.indexes.insert(
                schema.name.clone(),
                IndexImport {
                    existing: existing.is_some(),
                    synonyms: 0,
                    field_errors: 0,
                    rejection: false,
                    generation: 0,
                    deadline: schema.deadline,
                },
            );
            Ok(existing.is_none())
        }
        Effect::Synonyms {
            index,
            group,
            terms,
        } => {
            let info = context.indexes.get_mut(index).ok_or_else(conflict)?;
            info.synonyms = info.synonyms.checked_add(1).ok_or_else(conflict)?;
            if info.existing && state.indexes[index].synonyms.get(group) != Some(terms) {
                return Err(conflict());
            }
            Ok(!info.existing)
        }
        Effect::Generation { index, generation } => {
            let info = context.indexes.get_mut(index).ok_or_else(conflict)?;
            info.generation = *generation;
            if info.existing && state.indexes[index].synonym_generation != *generation {
                return Err(conflict());
            }
            Ok(!info.existing)
        }
        Effect::Alias {
            alias,
            index: Some(index),
        } => {
            if !context.indexes.contains_key(index) || state.indexes.contains_key(alias) {
                return Err(conflict());
            }
            match state.aliases.get(alias) {
                Some(destination) if destination != index => Err(conflict()),
                Some(_) => Ok(false),
                None => Ok(true),
            }
        }
        Effect::DictionaryAdd { name, terms } => {
            let info =
                context
                    .dictionaries
                    .entry(name.clone())
                    .or_insert_with(|| DictionaryImport {
                        existing: state.dictionaries.contains_key(name),
                        terms: 0,
                    });
            info.terms = info.terms.checked_add(terms.len()).ok_or_else(conflict)?;
            if info.existing && !terms.is_subset(&state.dictionaries[name]) {
                return Err(conflict());
            }
            Ok(!info.existing)
        }
        Effect::Rejection {
            index,
            count,
            message,
            key,
        } => {
            let info = context.indexes.get_mut(index).ok_or_else(conflict)?;
            if info.rejection {
                return Err(conflict());
            }
            info.rejection = true;
            if info.existing {
                let destination = &state.indexes[index];
                if destination.errors != *count
                    || destination.last_error.as_ref() != Some(message)
                    || destination.last_error_key.as_ref() != Some(key)
                {
                    return Err(conflict());
                }
            }
            Ok(!info.existing)
        }
        Effect::FieldError {
            index,
            field,
            count,
            message,
            key,
        } => {
            let info = context.indexes.get_mut(index).ok_or_else(conflict)?;
            info.field_errors = info.field_errors.checked_add(1).ok_or_else(conflict)?;
            if info.existing {
                match state.indexes[index].field_errors.get(field) {
                    Some((c, m, k)) if c == count && m == message && k == key => (),
                    _ => return Err(conflict()),
                }
            }
            Ok(!info.existing)
        }
        Effect::Touch { index, deadline } => {
            let info = context.indexes.get_mut(index).ok_or_else(conflict)?;
            info.deadline = Some(*deadline);
            if info.existing && state.indexes[index].schema.deadline != Some(*deadline) {
                return Err(conflict());
            }
            Ok(!info.existing)
        }
        _ => Err(invalid(
            "destructive or unexpected search migration metadata",
        )),
    }
}
fn metadata_end(state: &State, context: &SearchMigrationImport) -> io::Result<()> {
    for (name, info) in &context.indexes {
        if !info.existing {
            continue;
        }
        let index = state
            .indexes
            .get(name)
            .ok_or_else(|| invalid("destination search index disappeared"))?;
        if index.synonyms.len() != info.synonyms
            || index.field_errors.len() != info.field_errors
            || index.synonym_generation != info.generation
            || index.schema.deadline != info.deadline
            || (!info.rejection
                && (index.errors != 0
                    || index.last_error.is_some()
                    || index.last_error_key.is_some()))
        {
            return Err(invalid("incompatible destination search index metadata"));
        }
    }
    for (name, info) in &context.dictionaries {
        if info.existing && state.dictionaries[name].len() != info.terms {
            return Err(invalid("incompatible destination search dictionary"));
        }
    }
    Ok(())
}

impl Engine {
    pub(crate) fn export_search_migration(
        &self,
        ranges: &[(u16, u16)],
    ) -> io::Result<(SearchMigrationSnapshot, SearchMigrationFilter)> {
        let ranges = ranges_owned(ranges)?;
        let (metadata, documents, headers) = if let Some(catalog) = &self.search {
            let state = catalog.lock();
            (state.snapshot(), state.snapshot(), state.snapshot())
        } else {
            (
                SearchSnapshot::empty(),
                SearchSnapshot::empty(),
                SearchSnapshot::empty(),
            )
        };
        let mut keys = BTreeMap::new();
        let mut ids: BTreeMap<String, BTreeSet<DocId>> = BTreeMap::new();
        for bytes in headers {
            if let Effect::Header { index, id, key, .. } = state::decode(&bytes?)?
                && selected(&ranges, &key)
            {
                ids.entry(index.clone()).or_default().insert(id);
                keys.insert((index, id), key);
            }
        }
        Ok((
            SearchMigrationSnapshot {
                metadata,
                documents,
                selected: ids,
                phase: 0,
            },
            SearchMigrationFilter {
                ranges,
                keys,
                failed: false,
            },
        ))
    }

    pub(crate) fn search_filter_migration_effect(
        context: &mut SearchMigrationFilter,
        bytes: &[u8],
    ) -> io::Result<Option<Bytes>> {
        if context.failed {
            return Err(invalid("search migration source filter already aborted"));
        }
        let result = (|| {
            let effect = state::decode(bytes)?;
            let include = match effect {
                Effect::Header { index, id, key, .. } => {
                    if let Some(previous) = context.keys.get(&(index.clone(), id))
                        && previous != &key
                    {
                        return Err(invalid("search migration source document identity changed"));
                    }
                    if selected(&context.ranges, &key) {
                        context.keys.insert((index, id), key);
                        true
                    } else {
                        false
                    }
                }
                Effect::Field { index, id, .. } | Effect::Payload { index, id, .. } => {
                    context.keys.contains_key(&(index, id))
                }
                Effect::Remove { index, id } => context.keys.remove(&(index, id)).is_some(),
                _ => {
                    return Err(invalid(
                        "search metadata changed during migration; restart from a new barrier",
                    ));
                }
            };
            if include {
                pack(Record::Live(bytes.to_vec())).map(Some)
            } else {
                Ok(None)
            }
        })();
        if result.is_err() {
            context.failed = true;
        }
        result
    }

    /// The caller holds the same destination pause across preflight and import.
    /// This checks every metadata record before any namespace is published.
    pub(crate) fn validate_search_migration(
        &self,
        effects: &[Bytes],
        ranges: &[(u16, u16)],
    ) -> io::Result<()> {
        let empty = State::default();
        let guard = self.search.as_ref().map(|catalog| catalog.lock());
        let state = guard.as_deref().unwrap_or(&empty);
        let mut context = SearchMigrationImport::new(ranges)?;
        for bytes in effects {
            match unpack(bytes)? {
                Record::Metadata(bytes) if context.phase == 0 => {
                    metadata(state, &mut context, &state::decode(&bytes)?)?;
                }
                Record::MetadataEnd if context.phase == 0 => {
                    metadata_end(state, &context)?;
                    context.phase = 1;
                }
                Record::Document(bytes) if context.phase == 1 => {
                    let effect = state::decode(&bytes)?;
                    let (index, id) = identity(&effect)
                        .ok_or_else(|| invalid("non-document search snapshot record"))?;
                    if !context.indexes.contains_key(index) {
                        return Err(invalid("search document references missing schema"));
                    }
                    let source = (index.to_owned(), id);
                    match effect {
                        Effect::Header { key, .. } => {
                            if !selected(&context.ranges, &key) {
                                return Err(invalid("search document outside migration ranges"));
                            }
                            if context.documents.insert(source, (id, key)).is_some() {
                                return Err(invalid("duplicate search snapshot Header"));
                            }
                        }
                        Effect::Field { .. } | Effect::Payload { .. } => {
                            if !context.documents.contains_key(&source) {
                                return Err(invalid("search snapshot document missing Header"));
                            }
                        }
                        _ => return Err(invalid("unexpected search snapshot document mutation")),
                    }
                }
                Record::SnapshotEnd if context.phase == 1 => context.phase = 2,
                _ => return Err(invalid("invalid search migration snapshot ordering")),
            }
        }
        if !context.is_ready() {
            return Err(invalid("truncated search migration snapshot"));
        }
        Ok(())
    }

    pub(crate) fn search_merge_migration_effect(
        &mut self,
        context: &mut SearchMigrationImport,
        bytes: &[u8],
    ) -> io::Result<Vec<Bytes>> {
        if context.failed {
            return Err(invalid("search migration importer already aborted"));
        }
        let result = (|| {
            let record = unpack(bytes)?;
            let catalog = self.search_catalog();
            let mut state = catalog.lock();
            if let Some(revision) = context.revision
                && state.metadata_revision != revision
            {
                return Err(invalid(
                    "destination search metadata changed during migration",
                ));
            }
            let mut output = Vec::new();
            match record {
                Record::Metadata(bytes) if context.phase == 0 => {
                    let effect = state::decode(&bytes)?;
                    if metadata(&state, context, &effect)? {
                        output.push(state.effect(effect).map_err(invalid)?);
                    }
                }
                Record::MetadataEnd if context.phase == 0 => {
                    metadata_end(&state, context)?;
                    context.phase = 1;
                }
                Record::SnapshotEnd if context.phase == 1 => context.phase = 2,
                Record::Document(bytes) if context.phase == 1 => {
                    merge_document(&mut state, context, &bytes, &mut output)?
                }
                Record::Live(bytes) if context.phase == 2 => {
                    merge_document(&mut state, context, &bytes, &mut output)?
                }
                _ => return Err(invalid("invalid search migration stream ordering")),
            }
            context.revision = Some(state.metadata_revision);
            catalog.update_count(&state);
            Ok(output)
        })();
        if result.is_err() {
            context.failed = true;
        }
        result
    }

    /// A replaced raw key can have no source Header (SKIPINITIALSCAN). Remove
    /// only that key's stale coverage in source-declared indexes, not unrelated
    /// destination namespaces or other keys in the same migrating slot.
    pub(crate) fn search_reconcile_migration_key(
        &mut self,
        context: &mut SearchMigrationImport,
        key: &[u8],
    ) -> io::Result<Vec<Bytes>> {
        if !context.is_ready() || !selected(&context.ranges, key) {
            return Err(invalid(
                "search migration key reconciliation outside active snapshot",
            ));
        }
        let result = (|| {
            let catalog = self.search_catalog();
            let mut state = catalog.lock();
            if context.revision != Some(state.metadata_revision) {
                return Err(invalid(
                    "destination search metadata changed during migration",
                ));
            }
            let mut output = Vec::new();
            for name in context.indexes.keys() {
                if context
                    .coverage
                    .get(name)
                    .is_some_and(|keys| keys.contains(key))
                {
                    continue;
                }
                let id = state
                    .indexes
                    .get(name)
                    .ok_or_else(|| invalid("destination search index disappeared"))?
                    .keys
                    .get(key)
                    .copied();
                if let Some(id) = id {
                    output.push(
                        state
                            .effect(Effect::Remove {
                                index: name.clone(),
                                id,
                            })
                            .map_err(invalid)?,
                    );
                }
            }
            Ok(output)
        })();
        if result.is_err() {
            context.failed = true;
        }
        result
    }
}

fn merge_document(
    state: &mut State,
    context: &mut SearchMigrationImport,
    bytes: &[u8],
    output: &mut Vec<Bytes>,
) -> io::Result<()> {
    let mut effect = state::decode(bytes)?;
    let (index, source_id) =
        identity(&effect).ok_or_else(|| invalid("unexpected live search metadata"))?;
    let source = (index.to_owned(), source_id);
    if !context.indexes.contains_key(index) {
        return Err(invalid("search document references undeclared schema"));
    }
    let destination = state
        .indexes
        .get(index)
        .ok_or_else(|| invalid("destination search index disappeared"))?;
    let destination_id = if let Effect::Header { key, .. } = &effect {
        if !selected(&context.ranges, key) {
            return Err(invalid("search document outside migration ranges"));
        }
        if let Some((id, previous)) = context.documents.get(&source) {
            if previous != key || destination.docs.get(id).is_none_or(|doc| doc.key != *key) {
                return Err(invalid("destination search document identity changed"));
            }
            *id
        } else {
            let id = match destination.keys.get(key) {
                Some(id) => *id,
                None => {
                    let id = destination.next_id;
                    if id == u64::MAX || destination.docs.contains_key(&id) {
                        return Err(invalid("destination search document IDs exhausted"));
                    }
                    id
                }
            };
            // Replacing a selected key must remove stale indexed fields/payload;
            // Header alone intentionally preserves them on ordinary live updates.
            if destination.docs.contains_key(&id) {
                output.push(
                    state
                        .effect(Effect::Remove {
                            index: source.0.clone(),
                            id,
                        })
                        .map_err(invalid)?,
                );
            }
            context.documents.insert(source.clone(), (id, key.clone()));
            context
                .coverage
                .entry(source.0.clone())
                .or_default()
                .insert(key.clone());
            id
        }
    } else {
        let (id, key) = context
            .documents
            .get(&source)
            .ok_or_else(|| invalid("search migration document missing Header"))?;
        if destination.docs.get(id).is_none_or(|doc| doc.key != *key) {
            return Err(invalid("destination search document identity changed"));
        }
        *id
    };
    match &mut effect {
        Effect::Header { id, .. }
        | Effect::Payload { id, .. }
        | Effect::Field { id, .. }
        | Effect::Remove { id, .. } => *id = destination_id,
        _ => unreachable!(),
    }
    let remove = matches!(effect, Effect::Remove { .. });
    output.push(state.effect(effect).map_err(invalid)?);
    if remove
        && let Some((_, key)) = context.documents.remove(&source)
        && let Some(keys) = context.coverage.get_mut(&source.0)
    {
        keys.remove(&key);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::model::Schema;
    use super::*;

    fn schema(name: &str) -> Schema {
        Schema {
            name: name.into(),
            json: false,
            prefixes: vec![Bytes::new()],
            fields: Vec::new(),
            filter: None,
            language: "english".into(),
            language_field: None,
            score: 1.0,
            score_field: None,
            payload_field: None,
            stopwords: BTreeSet::new(),
            nooffsets: false,
            nohl: false,
            nofields: false,
            nofreqs: false,
            maxtextfields: false,
            indexall: false,
            temporary_ms: None,
            deadline: None,
            wide_text_fields: false,
        }
    }
    fn header(id: DocId, key: &[u8]) -> Effect {
        Effect::Header {
            index: "idx".into(),
            id,
            key: key.to_vec(),
            score: 1.0,
            expires_at: None,
            field_deadline: None,
            length: 0,
            synonym_generation: 0,
        }
    }
    fn declared(state: &mut State) -> SearchMigrationImport {
        state.effect(Effect::Create(schema("idx"))).unwrap();
        let mut context = SearchMigrationImport::new(&[(0, 16383)]).unwrap();
        metadata(state, &mut context, &Effect::Create(schema("idx"))).unwrap();
        context
    }
    #[test]
    fn live_remove_uses_seeded_identity_and_lifecycle_aborts() {
        let mut context = SearchMigrationFilter {
            ranges: vec![(0, 16383)],
            keys: BTreeMap::from([(("idx".into(), 7), b"binary\0key".to_vec())]),
            failed: false,
        };
        let remove = state::encode(&Effect::Remove {
            index: "idx".into(),
            id: 7,
        })
        .unwrap();
        let filtered = Engine::search_filter_migration_effect(&mut context, &remove)
            .unwrap()
            .unwrap();
        assert!(matches!(unpack(&filtered).unwrap(), Record::Live(_)));
        assert!(
            Engine::search_filter_migration_effect(&mut context, &remove)
                .unwrap()
                .is_none()
        );
        let drop = state::encode(&Effect::Drop("idx".into())).unwrap();
        assert!(Engine::search_filter_migration_effect(&mut context, &drop).is_err());
        assert!(Engine::search_filter_migration_effect(&mut context, &remove).is_err());
    }
    #[test]
    fn remapping_preserves_other_documents_and_replaces_stale_payload() {
        let mut state = State::default();
        let mut context = declared(&mut state);
        state.effect(header(1, b"unrelated")).unwrap();
        state.effect(header(9, b"selected")).unwrap();
        state
            .effect(Effect::Payload {
                index: "idx".into(),
                id: 9,
                value: Some(b"stale".to_vec()),
            })
            .unwrap();
        let mut output = Vec::new();
        merge_document(
            &mut state,
            &mut context,
            &state::encode(&header(1, b"selected")).unwrap(),
            &mut output,
        )
        .unwrap();
        assert_eq!(output.len(), 2);
        assert_eq!(state.indexes["idx"].keys[b"unrelated".as_slice()], 1);
        assert_eq!(state.indexes["idx"].keys[b"selected".as_slice()], 9);
        assert!(state.indexes["idx"].docs[&9].payload.is_none());
        output.clear();
        merge_document(
            &mut state,
            &mut context,
            &state::encode(&Effect::Payload {
                index: "idx".into(),
                id: 1,
                value: Some(b"resolved".to_vec()),
            })
            .unwrap(),
            &mut output,
        )
        .unwrap();
        assert_eq!(
            state.indexes["idx"].docs[&9].payload.as_deref(),
            Some(b"resolved".as_slice())
        );
        merge_document(
            &mut state,
            &mut context,
            &state::encode(&Effect::Remove {
                index: "idx".into(),
                id: 1,
            })
            .unwrap(),
            &mut output,
        )
        .unwrap();
        assert!(
            state.indexes["idx"]
                .keys
                .contains_key(b"unrelated".as_slice())
        );
        assert!(
            !state.indexes["idx"]
                .keys
                .contains_key(b"selected".as_slice())
        );
    }
    #[test]
    fn incompatible_metadata_is_rejected_without_modification() {
        let mut state = State::default();
        let mut context = declared(&mut state);
        let mut incompatible = schema("idx");
        incompatible.score = 0.25;
        let mut fresh = SearchMigrationImport::new(&[(0, 16383)]).unwrap();
        assert!(metadata(&state, &mut fresh, &Effect::Create(incompatible)).is_err());
        state
            .effect(Effect::Synonyms {
                index: "idx".into(),
                group: "g".into(),
                terms: BTreeSet::from(["word".into()]),
            })
            .unwrap();
        assert!(metadata_end(&state, &context).is_err());
        assert_eq!(state.indexes["idx"].schema.score, 1.0);
        context.failed = true;
        assert!(!context.is_ready());
    }
    #[test]
    fn snapshot_emits_metadata_before_resolved_documents() {
        let mut state = State::default();
        state.effect(Effect::Create(schema("idx"))).unwrap();
        state.effect(header(3, b"selected")).unwrap();
        state.effect(header(4, b"excluded")).unwrap();
        state
            .effect(Effect::DictionaryAdd {
                name: "dict".into(),
                terms: BTreeSet::from(["term".into()]),
            })
            .unwrap();
        let snapshot = SearchMigrationSnapshot {
            metadata: state.snapshot(),
            documents: state.snapshot(),
            selected: BTreeMap::from([("idx".into(), BTreeSet::from([3]))]),
            phase: 0,
        };
        let mut phase = 0;
        let mut documents = 0;
        for bytes in snapshot {
            match unpack(&bytes.unwrap()).unwrap() {
                Record::Metadata(_) => assert_eq!(phase, 0),
                Record::MetadataEnd => {
                    assert_eq!(phase, 0);
                    phase = 1;
                }
                Record::Document(bytes) => {
                    assert_eq!(phase, 1);
                    assert!(matches!(
                        state::decode(&bytes).unwrap(),
                        Effect::Header { id: 3, .. }
                    ));
                    documents += 1;
                }
                Record::SnapshotEnd => {
                    assert_eq!(phase, 1);
                    phase = 2;
                }
                _ => panic!("unexpected snapshot phase"),
            }
        }
        assert_eq!(phase, 2);
        assert_eq!(documents, 1);
    }
}
