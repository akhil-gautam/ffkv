use super::super::{Engine, Mutation, Value, typed};
use super::{
    model::*,
    state::{Effect, State},
    string,
};
use bytes::Bytes;
use serde_json::Value as J;
use std::collections::{BTreeMap, BTreeSet};
fn strings(values: &[J]) -> Result<Vec<String>, String> {
    let mut out = Vec::new();
    for v in values {
        match v {
            J::String(s) => out.push(s.clone()),
            J::Array(a) => out.extend(strings(a)?),
            J::Null => {}
            _ => return Err("Expected a string value".into()),
        }
    }
    Ok(out)
}
fn numbers(values: &[J]) -> Result<Vec<f64>, String> {
    let mut out = Vec::new();
    for v in values {
        match v {
            J::Number(n) => out.push(n.as_f64().ok_or("Invalid numeric value")?),
            J::Array(a) => out.extend(numbers(a)?),
            J::Null => {}
            _ => return Err("JSON numeric fields require numeric values".into()),
        }
    }
    if out.iter().any(|n| !n.is_finite()) {
        return Err("Invalid numeric value".into());
    }
    Ok(out)
}
fn geo(s: &str) -> Result<[f64; 2], String> {
    let (x, y) = s.split_once(',').ok_or("Invalid GEO value")?;
    let x = x.trim().parse::<f64>().map_err(|_| "Invalid longitude")?;
    let y = y.trim().parse::<f64>().map_err(|_| "Invalid latitude")?;
    if !x.is_finite()
        || !y.is_finite()
        || !(-180.0..=180.0).contains(&x)
        || !(-85.05112878..=85.05112878).contains(&y)
    {
        return Err("Invalid GEO coordinates".into());
    }
    Ok([x, y])
}
impl Engine {
    pub(in crate::engine) fn search_after_mutation(
        &mut self,
        m: &Mutation,
        now: i64,
    ) -> Vec<Bytes> {
        if let Mutation::SwapDatabases { first, second } = m {
            return match Self::search_reconcile_swapped(
                std::slice::from_mut(self),
                *first,
                *second,
                now,
            ) {
                Ok(effects) => effects,
                Err(error) => {
                    self.failure.get_or_insert(error);
                    Vec::new()
                }
            };
        }
        if matches!(
            m,
            Mutation::Search { .. } | Mutation::ResetGlobals | Mutation::ClearNativePosition
        ) || self.database() != 0
            || self.search.as_ref().is_none_or(|c| !c.has_indexes())
        {
            return Vec::new();
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let mut effects = Vec::new();
        if matches!(m, Mutation::Clear) {
            if let Ok(e) = state.effect(Effect::Clear) {
                effects.push(e)
            }
            catalog.update_count(&state);
            return effects;
        }
        let keys: Vec<&Bytes> = match m {
            Mutation::Rename { from, to } => vec![from, to],
            Mutation::Set { key, .. }
            | Mutation::Delete { key }
            | Mutation::Push { key, .. }
            | Mutation::Pop { key, .. }
            | Mutation::Expire { key, .. }
            | Mutation::SetAdd { key, .. }
            | Mutation::SetRemove { key, .. }
            | Mutation::HashSet { key, .. }
            | Mutation::HashDelete { key, .. }
            | Mutation::SortedSetAdd { key, .. }
            | Mutation::SortedSetRemove { key, .. }
            | Mutation::ListRemove { key, .. }
            | Mutation::ListSet { key, .. }
            | Mutation::Queue { key, .. }
            | Mutation::Typed { key, .. } => vec![key],
            _ => Vec::new(),
        };
        let names: Vec<_> = state.indexes.keys().cloned().collect();
        for key in keys {
            for name in &names {
                let changed = match m {
                    Mutation::HashSet { fields, .. } => Some(
                        fields
                            .iter()
                            .map(|(f, _, _)| f.as_ref())
                            .collect::<BTreeSet<_>>(),
                    ),
                    Mutation::HashDelete { fields, .. } => {
                        Some(fields.iter().map(Bytes::as_ref).collect::<BTreeSet<_>>())
                    }
                    _ => None,
                };
                if let Err(message) =
                    self.search_refresh(&mut state, name, key, now, changed.as_ref(), &mut effects)
                    && let Err(e) =
                        Self::search_reject(&mut state, name, key, message, &mut effects)
                {
                    self.failure.get_or_insert_with(|| std::io::Error::other(e));
                }
            }
        }
        effects
    }
    /// Reconcile only after every physical shard has completed SWAPDB. No source
    /// values leave their owning shard; returned effects belong in one durable batch.
    pub(crate) fn search_reconcile_swapped(
        engines: &mut [Engine],
        first: u32,
        second: u32,
        now: i64,
    ) -> std::io::Result<Vec<Bytes>> {
        if first == second || (first != 0 && second != 0) {
            return Ok(Vec::new());
        }
        let Some(catalog) = engines.iter().find_map(|engine| engine.search.clone()) else {
            return Ok(Vec::new());
        };
        if engines
            .iter()
            .filter_map(|engine| engine.search.as_ref())
            .any(|other| !std::sync::Arc::ptr_eq(other, &catalog))
        {
            return Err(std::io::Error::other(
                "SWAPDB search reconciliation requires one shared catalog",
            ));
        }
        let previous = engines.iter().map(Engine::database).collect::<Vec<_>>();
        for engine in engines.iter_mut() {
            engine.switch_database(0);
        }
        let result = (|| {
            let mut state = catalog.lock();
            let mut effects = Vec::new();
            if state.indexes.is_empty() {
                return Ok(effects);
            }
            let keys = state
                .indexes
                .values()
                .flat_map(|index| {
                    index
                        .docs
                        .values()
                        .map(|doc| Bytes::copy_from_slice(&doc.key))
                })
                .chain(
                    engines
                        .iter()
                        .flat_map(|engine| engine.entries.keys().cloned()),
                )
                .collect::<BTreeSet<_>>();
            let names = state.indexes.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let source = &engines[Engine::shard_for(&key, engines.len())];
                for name in &names {
                    if let Err(message) =
                        source.search_refresh(&mut state, name, &key, now, None, &mut effects)
                    {
                        Self::search_reject(&mut state, name, &key, message, &mut effects)
                            .map_err(std::io::Error::other)?;
                    }
                }
            }
            Ok(effects)
        })();
        for (engine, database) in engines.iter_mut().zip(previous) {
            engine.switch_database(database);
        }
        result
    }
    pub(crate) fn search_due_keys(&self, now: i64) -> Vec<Bytes> {
        let Some(catalog) = &self.search else {
            return Vec::new();
        };
        let state = catalog.lock();
        let mut keys = BTreeSet::new();
        for (_, name, id) in state.deadlines.iter().take_while(|(d, _, _)| *d <= now) {
            if let Some(doc) = state.indexes.get(name).and_then(|i| i.docs.get(id)) {
                keys.insert(Bytes::copy_from_slice(&doc.key));
            }
        }
        for (_, name) in state.temp_deadlines.iter().take_while(|(d, _)| *d <= now) {
            if let Some(index) = state.indexes.get(name) {
                for doc in index.docs.values() {
                    keys.insert(Bytes::copy_from_slice(&doc.key));
                }
            }
        }
        keys.into_iter().collect()
    }
    pub(crate) fn search_reap(&mut self, now: i64) {
        let Some(catalog) = self.search.clone() else {
            return;
        };
        catalog.collect(now);
        if !catalog.has_indexes() {
            return;
        }
        let mut state = catalog.lock();
        state.reap_cursors(now);
        let expired: Vec<_> = state
            .temp_deadlines
            .iter()
            .take_while(|(d, _)| *d <= now)
            .map(|(_, name)| name.clone())
            .collect();
        let mut deletes = BTreeSet::new();
        let mut effects = Vec::new();
        for name in expired {
            deletes.extend(state.take_drop_document_keys(&name));
            match state.effect(Effect::Drop(name)) {
                Ok(e) => effects.push(e),
                Err(e) => {
                    drop(state);
                    self.fail_io(std::io::Error::other(e));
                    return;
                }
            }
        }
        let due: Vec<_> = state
            .deadlines
            .iter()
            .take_while(|(deadline, _, _)| *deadline <= now)
            .filter_map(|(_, name, id)| {
                state
                    .indexes
                    .get(name)?
                    .docs
                    .get(id)
                    .map(|d| (name.clone(), Bytes::copy_from_slice(&d.key)))
            })
            .collect();
        for (name, key) in due {
            if let Err(message) =
                self.search_refresh(&mut state, &name, &key, now, None, &mut effects)
                && let Err(e) = Self::search_reject(&mut state, &name, &key, message, &mut effects)
            {
                drop(state);
                self.fail_io(std::io::Error::other(e));
                return;
            }
        }
        catalog.update_count(&state);
        drop(state);
        self.search_record_effects(effects, now);
        // Never retain a catalog guard while source deletion invokes indexing hooks.
        for key in deletes {
            if self.remove(&key).is_some() {
                self.record(now, || Mutation::Delete { key });
            }
        }
    }
    pub(super) fn search_reject(
        state: &mut State,
        name: &str,
        key: &Bytes,
        message: String,
        effects: &mut Vec<Bytes>,
    ) -> Result<(), String> {
        let index = &state.indexes[name];
        let count = index.errors.saturating_add(1);
        let id = index.keys.get(key.as_ref()).copied();
        let field = message
            .split_once(": ")
            .map(|(f, _)| f)
            .filter(|f| index.schema.fields.iter().any(|field| field.name == *f))
            .map(str::to_owned);
        let field_count = field.as_ref().map(|f| {
            index
                .field_errors
                .get(f)
                .map_or(1, |(n, _, _)| n.saturating_add(1))
        });
        if let Some(id) = id {
            effects.push(state.effect(Effect::Remove {
                index: name.into(),
                id,
            })?)
        }
        if let Some(field) = field {
            effects.push(state.effect(Effect::FieldError {
                index: name.into(),
                field,
                count: field_count.unwrap_or(1),
                message: message.clone(),
                key: key.to_vec(),
            })?)
        }
        effects.push(state.effect(Effect::Rejection {
            index: name.into(),
            count,
            message,
            key: key.to_vec(),
        })?);
        Ok(())
    }
    pub(super) fn search_refresh(
        &self,
        state: &mut State,
        name: &str,
        key: &Bytes,
        now: i64,
        changed: Option<&BTreeSet<&[u8]>>,
        effects: &mut Vec<Bytes>,
    ) -> Result<(), String> {
        let started = std::time::Instant::now();
        let index = state.indexes.get(name).ok_or("Unknown Index name")?;
        let old_id = index.keys.get(key.as_ref()).copied();
        let source = self
            .entries
            .get(key)
            .filter(|e| e.expires_at.is_none_or(|d| d > now));
        let eligible = source.is_some_and(|e| {
            index.schema.prefixes.iter().any(|p| key.starts_with(p))
                && if index.schema.json {
                    matches!(&e.value,Value::Typed(d)if matches!(&**d,typed::Data::Json(_)))
                } else {
                    matches!(&e.value, Value::Hash(_))
                }
        });
        if !eligible {
            if let Some(id) = old_id {
                effects.push(state.effect(Effect::Remove {
                    index: name.into(),
                    id,
                })?)
            }
            return Ok(());
        }
        let source = source.expect("eligible source");
        let schema = &index.schema;
        if let Some(filter) = &schema.filter {
            let row = self.search_source_row(key, &schema.fields, now)?;
            let value = super::expression::evaluate(filter, &row)?;
            let pass = match value {
                J::Bool(v) => v,
                J::Number(n) => n.as_f64().is_some_and(|n| n != 0.),
                J::Null => false,
                _ => true,
            };
            if !pass {
                if let Some(id) = old_id {
                    effects.push(state.effect(Effect::Remove {
                        index: name.into(),
                        id,
                    })?)
                }
                return Ok(());
            }
        }
        let raw = |field: &str| -> Option<&Bytes> {
            match &source.value {
                Value::Hash(h) => h.search_field(field.as_bytes(), now),
                _ => None,
            }
        };
        let language = if let Some(field) = &schema.language_field {
            if schema.json {
                self.search_json_values(key, field)?
                    .first()
                    .and_then(J::as_str)
                    .map(str::to_owned)
                    .unwrap_or_else(|| schema.language.clone())
            } else {
                raw(field)
                    .map(|b| String::from_utf8_lossy(b).into_owned())
                    .unwrap_or_else(|| schema.language.clone())
            }
        } else {
            schema.language.clone()
        };
        if !super::schema::language(&language) {
            return Err("Unsupported language".into());
        }
        let mut score = schema.score;
        if let Some(field) = &schema.score_field {
            let v = if schema.json {
                self.search_json_values(key, field)?.first().and_then(|v| {
                    v.as_f64()
                        .or_else(|| v.as_str().and_then(|s| s.parse().ok()))
                })
            } else {
                raw(field).and_then(|b| std::str::from_utf8(b).ok()?.parse().ok())
            };
            if let Some(v) = v {
                score = v
            }
            if !score.is_finite() || !(0.0..=1.0).contains(&score) {
                return Err("Invalid document score".into());
            }
        }
        let json_payload = if schema.json {
            if let Some(field) = &schema.payload_field {
                self.search_json_values(key, field)?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };
        let payload = if let Some(field) = &schema.payload_field {
            if schema.json {
                json_payload.first().and_then(J::as_str).map(str::as_bytes)
            } else {
                raw(field).map(Bytes::as_ref)
            }
        } else {
            None
        };
        let field_deadline = match &source.value {
            Value::Hash(h) => h.snapshot(now).filter_map(|(_, _, d)| d).min(),
            _ => None,
        };
        let id = old_id.unwrap_or(index.next_id);
        let old = old_id.and_then(|id| index.docs.get(&id));
        let mut changes = Vec::new();
        let mut length = old.map_or(0, |d| d.length);
        let payload_update = if old.and_then(|d| d.payload.as_deref()) != payload {
            Some(payload.map(<[u8]>::to_vec))
        } else {
            None
        };
        let synonym_generation = index.synonym_generation;
        let retokenize = old.is_some_and(|d| d.synonym_generation != synonym_generation)
            || changed.is_some_and(|fields| {
                schema
                    .language_field
                    .as_ref()
                    .is_some_and(|name| fields.contains(name.as_bytes()))
            });
        for field in &schema.fields {
            let previous = old.and_then(|d| d.fields.get(&field.name));
            if !retokenize
                && previous.is_some()
                && changed.is_some_and(|fields| !fields.contains(field.source.as_bytes()))
            {
                continue;
            }
            let value = if schema.json {
                self.search_field_value(
                    field,
                    self.search_json_values(key, &field.source)?,
                    None,
                    &language,
                    schema,
                    &index.synonyms,
                    state.config.min_stem,
                )
            } else {
                let value = raw(&field.source);
                self.search_field_value(
                    field,
                    Vec::new(),
                    value.map(Bytes::as_ref),
                    &language,
                    schema,
                    &index.synonyms,
                    state.config.min_stem,
                )
            }
            .map_err(|e| format!("{}: {e}", field.name))?;
            if previous == Some(&value) {
                continue;
            }
            if let Some(IndexedValue::Text { tokens, .. }) = previous {
                length = length.saturating_sub(
                    tokens
                        .iter()
                        .filter(|t| !t.term.starts_with(&['+', '$', '~'][..]))
                        .count(),
                )
            }
            if let IndexedValue::Text { tokens, .. } = &value {
                length = length.saturating_add(
                    tokens
                        .iter()
                        .filter(|t| !t.term.starts_with(&['+', '$', '~'][..]))
                        .count(),
                )
            }
            changes.push((field.name.clone(), value));
        }
        // Validate all derived values before publishing any field, preserving rejected-document atomicity.
        for (field, value) in &changes {
            match value {
                IndexedValue::Geometry(wkt) => {
                    let f = schema
                        .fields
                        .iter()
                        .find(|f| &f.name == field)
                        .expect("schema field");
                    super::spatial::validate_geometry(wkt, f.spherical)
                        .map_err(|e| format!("{field}: {e}"))?
                }
                IndexedValue::Vector(values) => {
                    let f = schema
                        .fields
                        .iter()
                        .find(|f| &f.name == field)
                        .expect("schema field");
                    let config = f.vector.as_ref().expect("vector config");
                    for value in values {
                        super::vector::validate_values(config, value)
                            .map_err(|e| format!("{field}: {e}"))?
                    }
                }
                _ => {}
            }
        }
        let expires_at = source.expires_at;
        let deadline = schema.temporary_ms.map(|ms| now.saturating_add(ms));
        let header_changed = old.is_none_or(|d| {
            d.score != score
                || d.expires_at != expires_at
                || d.field_deadline != field_deadline
                || d.length != length
                || d.synonym_generation != synonym_generation
        });
        state.save_index_metadata(name);
        if header_changed {
            effects.push(state.effect(Effect::Header {
                index: name.into(),
                id,
                key: key.to_vec(),
                score,
                expires_at,
                field_deadline,
                length,
                synonym_generation,
            })?)
        }
        if let Some(value) = payload_update {
            effects.push(state.effect(Effect::Payload {
                index: name.into(),
                id,
                value,
            })?)
        }
        for (field, value) in changes {
            effects.push(state.effect(Effect::Field {
                index: name.into(),
                id,
                name: field,
                value,
            })?)
        }
        if let Some(deadline) = deadline {
            effects.push(state.effect(Effect::Touch {
                index: name.into(),
                deadline,
            })?)
        }
        if let Some(i) = state.indexes.get_mut(name) {
            i.indexing_time_ms += started.elapsed().as_secs_f64() * 1000.;
        }
        Ok(())
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "A field is indexed with its schema and the complete configured text-analysis context."
    )]
    fn search_field_value(
        &self,
        f: &Field,
        values: Vec<J>,
        raw: Option<&[u8]>,
        language: &str,
        s: &Schema,
        synonyms: &BTreeMap<String, BTreeSet<String>>,
        min_stem: usize,
    ) -> Result<IndexedValue, String> {
        if (!s.json && raw.is_none())
            || (s.json && (values.is_empty() || values.iter().all(J::is_null)))
        {
            return Ok(IndexedValue::Missing);
        }
        match f.kind {
            FieldKind::Text => {
                let strings = if s.json {
                    strings(&values)?
                } else {
                    vec![String::from_utf8_lossy(raw.unwrap_or_default()).into_owned()]
                };
                if strings.iter().all(String::is_empty) {
                    return Ok(IndexedValue::Empty);
                }
                let mut tokens = Vec::new();
                let mut position = 0;
                for value in &strings {
                    let mut more = super::text::tokenize_indexed(
                        value,
                        language,
                        f.nostem,
                        &s.stopwords,
                        min_stem,
                    );
                    for t in &mut more {
                        t.position = t.position.saturating_add(position)
                    }
                    position = more
                        .iter()
                        .map(|t| t.position)
                        .max()
                        .unwrap_or(position)
                        .saturating_add(100);
                    super::indexing::synonyms(&mut more, synonyms, f.phonetic.as_deref());
                    tokens.extend(more)
                }
                Ok(IndexedValue::Text { strings, tokens })
            }
            FieldKind::Tag => {
                let strings = if s.json {
                    strings(&values)?
                } else {
                    vec![String::from_utf8_lossy(raw.unwrap_or_default()).into_owned()]
                };
                let mut tags = BTreeSet::new();
                for value in strings {
                    let parts: Vec<_> = if let Some(separator) = f.separator {
                        value.split(separator).collect()
                    } else {
                        vec![value.as_str()]
                    };
                    for tag in parts {
                        let tag = tag.trim();
                        if tag.is_empty() && !f.index_empty {
                            continue;
                        }
                        tags.insert(if f.case_sensitive {
                            tag.into()
                        } else {
                            super::text::normalize(tag)
                        });
                    }
                }
                if tags.is_empty() {
                    Ok(IndexedValue::Empty)
                } else {
                    Ok(IndexedValue::Tag(tags.into_iter().collect()))
                }
            }
            FieldKind::Numeric => {
                let nums = if s.json {
                    numbers(&values)?
                } else {
                    let v = string(raw.unwrap_or_default())?
                        .trim()
                        .parse::<f64>()
                        .map_err(|_| "Invalid numeric value")?;
                    if !v.is_finite() {
                        return Err("Invalid numeric value".into());
                    }
                    vec![v]
                };
                Ok(IndexedValue::Numeric(nums))
            }
            FieldKind::Geo => {
                let strs = if s.json {
                    strings(&values)?
                } else {
                    vec![string(raw.unwrap_or_default())?.into()]
                };
                Ok(IndexedValue::Geo(
                    strs.iter().map(|v| geo(v)).collect::<Result<_, _>>()?,
                ))
            }
            FieldKind::GeoShape => {
                let values = if s.json {
                    strings(&values)?
                } else {
                    vec![string(raw.unwrap_or_default())?.into()]
                };
                if values.len() != 1 {
                    return Err("GEOSHAPE does not support multiple values".into());
                }
                Ok(IndexedValue::Geometry(values[0].clone()))
            }
            FieldKind::Vector => {
                let config = f.vector.as_ref().ok_or("Missing vector configuration")?;
                let vectors = if s.json {
                    let mut out = Vec::new();
                    for v in values {
                        let a = v.as_array().ok_or("JSON vector must be an array")?;
                        if a.first().is_some_and(J::is_array) {
                            for v in a {
                                out.push(numbers(std::slice::from_ref(v))?)
                            }
                        } else {
                            out.push(numbers(a)?)
                        }
                    }
                    if out.iter().any(|v| v.len() != config.dim) {
                        return Err("Invalid vector length".into());
                    }
                    out
                } else {
                    vec![super::vector::decode(config, raw.unwrap_or_default())?]
                };
                Ok(IndexedValue::Vector(vectors))
            }
        }
    }
    pub(super) fn search_json_values(&self, key: &Bytes, path: &str) -> Result<Vec<J>, String> {
        let entry = self.entries.get(key).ok_or("Missing source document")?;
        let Value::Typed(data) = &entry.value else {
            return Err("Expected JSON document".into());
        };
        let typed::Data::Json(json) = &**data else {
            return Err("Expected JSON document".into());
        };
        json.search_values(path)
            .map_err(|_| "Invalid JSONPath".into())
    }
    pub(super) fn search_source_row(
        &self,
        key: &Bytes,
        fields: &[Field],
        now: i64,
    ) -> Result<Row, String> {
        let mut row = Row::new();
        let entry = self.entries.get(key).ok_or("Missing source document")?;
        match &entry.value {
            Value::Hash(hash) => {
                for (k, v, _) in hash.snapshot(now) {
                    row.insert(
                        String::from_utf8_lossy(k).into_owned(),
                        super::value::from_bytes(v),
                    );
                }
            }
            Value::Typed(data) => {
                if let typed::Data::Json(_) = &**data {
                    let root = self
                        .search_json_values(key, "$")?
                        .into_iter()
                        .next()
                        .unwrap_or(J::Null);
                    row.insert("$".into(), super::value::from_json(root));
                    for f in fields {
                        let values = self.search_json_values(key, &f.source)?;
                        let v = if values.len() == 1 {
                            values.into_iter().next().unwrap_or(J::Null)
                        } else {
                            J::Array(values)
                        };
                        let v = super::value::from_json(v);
                        row.insert(f.source.clone(), v.clone());
                        row.insert(f.name.clone(), v);
                    }
                }
            }
            _ => {}
        }
        for f in fields {
            if !row.contains_key(&f.name)
                && let Some(v) = row.get(&f.source).cloned()
            {
                row.insert(f.name.clone(), v);
            }
        }
        row.insert("__key".into(), super::value::from_bytes(key));
        Ok(row)
    }
}
