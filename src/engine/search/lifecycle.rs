use super::super::{Engine, Mutation, Resp, Value, typed};
use super::{
    bulk,
    model::*,
    schema,
    state::{Effect, State},
    string, text,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

fn arg(a: &[Bytes], n: usize) -> Result<&str, String> {
    string(a.get(n).ok_or("Wrong number of arguments")?)
}
fn arity(a: &[Bytes], n: usize) -> Result<(), String> {
    if a.len() == n {
        Ok(())
    } else {
        Err("Wrong number of arguments".into())
    }
}
fn text_count(fields: &[Field]) -> usize {
    fields
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::Text))
        .count()
}
fn integer(value: usize) -> Resp {
    Resp::Unsigned(value as u64)
}
fn live(doc: &Document, now: i64) -> bool {
    doc.expires_at.is_none_or(|d| d > now) && doc.field_deadline.is_none_or(|d| d > now)
}
fn document_frequency(index: &Index, fields: &[String], word: &str, now: i64) -> usize {
    let mut documents = BTreeSet::new();
    for (name, field) in &index.fields {
        if !fields.is_empty() && !fields.contains(name) {
            continue;
        }
        if let FieldIndex::Text(terms) = field
            && let Some(postings) = terms.get(word)
        {
            documents.extend(
                postings
                    .keys()
                    .copied()
                    .filter(|id| index.docs.get(id).is_some_and(|d| live(d, now))),
            );
        }
    }
    documents.len()
}

impl Engine {
    fn search_metadata_pairs(&self, values: Vec<(&str, Resp)>) -> Resp {
        if matches!(self.command_version(), crate::protocol::Version::Resp3) {
            Resp::Map(values.into_iter().map(|(k, v)| (text(k), v)).collect())
        } else {
            Resp::Array(Some(
                values.into_iter().flat_map(|(k, v)| [text(k), v]).collect(),
            ))
        }
    }
    fn search_initial_keys(&self, schema: &Schema, now: i64) -> Vec<Bytes> {
        self.entries.iter().filter(|(key, entry)| {
            entry.expires_at.is_none_or(|d| d > now)
                && schema.prefixes.iter().any(|p| key.starts_with(p))
                && if schema.json { matches!(&entry.value, Value::Typed(data) if matches!(&**data, typed::Data::Json(_))) }
                   else { matches!(&entry.value, Value::Hash(_)) }
        }).map(|(key, _)| key.clone()).collect()
    }
    fn search_initial_scan(
        &self,
        state: &mut State,
        name: &str,
        keys: Vec<Bytes>,
        now: i64,
        effects: &mut Vec<Bytes>,
    ) -> Result<(), String> {
        for key in keys {
            if let Err(message) = self.search_refresh(state, name, &key, now, None, effects) {
                Self::search_reject(state, name, &key, message, effects)?;
            }
        }
        Ok(())
    }
    pub(super) fn search_create(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        if self.database() != 0 {
            return Err("Cannot create index on db != 0".into());
        }
        let (schema, skip) = schema::create(a, now)?;
        if !self.command_prefixes_readable(&schema.prefixes) {
            return Err("NOPERM User does not have permissions to access the index".into());
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        if state.indexes.contains_key(&schema.name) || state.aliases.contains_key(&schema.name) {
            return Err("Index already exists".into());
        }
        let keys = if skip {
            Vec::new()
        } else {
            self.search_initial_keys(&schema, now)
        };
        let name = schema.name.clone();
        let mut effects = vec![state.effect(Effect::Create(schema))?];
        self.search_initial_scan(&mut state, &name, keys, now, &mut effects)?;
        catalog.update_count(&state);
        drop(state);
        self.search_record_effects(effects, now);
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_alter(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?.to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let mut at = 2;
        let skip = a
            .get(at)
            .is_some_and(|v| v.eq_ignore_ascii_case(b"SKIPINITIALSCAN"));
        if skip {
            at += 1;
        }
        if !a.get(at).is_some_and(|v| v.eq_ignore_ascii_case(b"SCHEMA"))
            || !a
                .get(at + 1)
                .is_some_and(|v| v.eq_ignore_ascii_case(b"ADD"))
        {
            return Err("Expected SCHEMA ADD".into());
        }
        let extra = schema::fields(&a[at + 2..], index.schema.json)?;
        if extra.is_empty() {
            return Err("Schema must contain at least one field".into());
        }
        if index.schema.fields.len() + extra.len() > 1024 {
            return Err("Too many schema attributes".into());
        }
        if extra
            .iter()
            .any(|f| index.schema.fields.iter().any(|old| old.name == f.name))
        {
            return Err("Duplicate field attribute".into());
        }
        let old_text = text_count(&index.schema.fields);
        let new_text = old_text + text_count(&extra);
        if new_text > 128 || (!index.schema.wide_text_fields && new_text > 32) {
            return Err("Too many TEXT fields; specify MAXTEXTFIELDS at index creation".into());
        }
        let mut schema = index.schema.clone();
        schema.fields.extend(extra);
        let keys = if skip {
            Vec::new()
        } else {
            self.search_initial_keys(&schema, now)
        };
        let mut effects = vec![state.effect(Effect::Alter(schema))?];
        self.search_initial_scan(&mut state, &name, keys, now, &mut effects)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_drop(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        if a.len() != 2 && (a.len() != 3 || !a[2].eq_ignore_ascii_case(b"DD")) {
            return Err("Expected index [DD]".into());
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?.to_owned();
        let index = &state.indexes[&name];
        let delete_documents = a.len() == 3;
        if delete_documents {
            if let Some(user) = &self.command_user {
                let keys = index
                    .docs
                    .values()
                    .filter_map(|doc| {
                        self.entries
                            .get_key_value(doc.key.as_slice())
                            .map(|(key, _)| (key, crate::auth::KeyAccess::Write))
                    })
                    .collect::<Vec<_>>();
                user.authorize_prefixes_and_keys_logged(
                    b"ft.dropindex",
                    &["search", "write"],
                    &index.schema.prefixes,
                    crate::auth::KeyAccess::Read,
                    &keys,
                    self.command_audit().as_ref(),
                )
                .map_err(|_| {
                    "NOPERM User does not have permissions to access or delete indexed documents"
                        .to_string()
                })?;
            }
        } else {
            self.search_authorize(index)?;
        }
        let keys = if delete_documents {
            state.take_drop_document_keys(&name)
        } else {
            Vec::new()
        };
        let effects = vec![state.effect(Effect::Drop(name))?];
        catalog.update_count(&state);
        drop(state);
        self.search_record_effects(effects, now);
        for key in keys {
            if self.remove(&key).is_some() {
                self.record(now, || Mutation::Delete { key });
            }
        }
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_alias(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let op = a[0].to_ascii_uppercase();
        let list = op.as_slice() == b"FT.ALIASLIST";
        let del = op.as_slice() == b"FT.ALIASDEL";
        arity(a, if list || del { 2 } else { 3 })?;
        let alias = arg(a, 1)?.to_owned();
        if alias.is_empty() {
            return Err("Alias must not be empty".into());
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let mut effects = Vec::new();
        if list {
            let index = state.indexes.get(&alias).ok_or("Unknown Index name")?;
            self.search_authorize(index)?;
            let values = state
                .aliases
                .iter()
                .filter(|(_, target)| **target == alias)
                .map(|(name, _)| text(name))
                .collect();
            return Ok(self.search_set(values));
        }
        if del {
            let name = state
                .aliases
                .get(&alias)
                .ok_or("Alias does not exist")?
                .clone();
            self.search_authorize(&state.indexes[&name])?;
            effects.push(state.effect(Effect::Alias { alias, index: None })?);
        } else {
            let name = arg(a, 2)?.to_owned();
            let index = state.indexes.get(&name).ok_or("Unknown Index name")?;
            self.search_authorize(index)?;
            if state.indexes.contains_key(&alias) {
                return Err("Alias conflicts with an index name".into());
            }
            if op.as_slice() == b"FT.ALIASADD" && state.aliases.contains_key(&alias) {
                return Err("Alias already exists".into());
            }
            if let Some(old) = state.aliases.get(&alias) {
                self.search_authorize(&state.indexes[old])?;
            }
            effects.push(state.effect(Effect::Alias {
                alias,
                index: Some(name),
            })?);
        }
        drop(state);
        self.search_record_effects(effects, now);
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_dictionary(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let dump = a[0].eq_ignore_ascii_case(b"FT.DICTDUMP");
        if dump {
            arity(a, 2)?;
        } else if a.len() < 3 {
            return Err("Wrong number of arguments".into());
        }
        let name = arg(a, 1)?.to_owned();
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        if dump {
            return Ok(self.search_set(
                state
                    .dictionaries
                    .get(&name)
                    .into_iter()
                    .flat_map(|terms| terms.iter().map(text))
                    .collect(),
            ));
        }
        let add = a[0].eq_ignore_ascii_case(b"FT.DICTADD");
        let current = state.dictionaries.get(&name);
        let mut terms = BTreeSet::new();
        for term in &a[2..] {
            let term = string(term)?.to_owned();
            if current.is_some_and(|s| s.contains(&term)) != add {
                terms.insert(term);
            }
        }
        let count = terms.len();
        let effects = if terms.is_empty() {
            Vec::new()
        } else {
            vec![state.effect(if add {
                Effect::DictionaryAdd { name, terms }
            } else {
                Effect::DictionaryRemove { name, terms }
            })?]
        };
        catalog.update_count(&state);
        drop(state);
        self.search_record_effects(effects, now);
        Ok(integer(count))
    }
    pub(super) fn search_synonym(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let dump = a[0].eq_ignore_ascii_case(b"FT.SYNDUMP");
        if dump {
            arity(a, 2)?;
        } else if a.len() < 4 {
            return Err("Wrong number of arguments".into());
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?.to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let mut effects = Vec::new();
        if dump {
            let mut terms: BTreeMap<&str, Vec<Resp>> = BTreeMap::new();
            for (group, values) in &index.synonyms {
                for term in values {
                    terms.entry(term).or_default().push(text(group));
                }
            }
            let response = self.search_metadata_pairs(
                terms
                    .into_iter()
                    .map(|(term, groups)| (term, Resp::Array(Some(groups))))
                    .collect(),
            );
            return Ok(response);
        }
        let group = arg(a, 2)?.to_owned();
        let skip = a
            .get(3)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"SKIPINITIALSCAN"));
        let start = if skip { 4 } else { 3 };
        if start == a.len() {
            return Err("Missing synonym terms".into());
        }
        let mut terms = index.synonyms.get(&group).cloned().unwrap_or_default();
        for term in &a[start..] {
            terms.insert(string(term)?.to_lowercase());
        }
        let effect = Effect::Synonyms {
            index: name.clone(),
            group,
            terms,
        };
        let keys = if skip {
            Vec::new()
        } else {
            self.search_initial_keys(&index.schema, now)
        };
        effects.push(state.effect(effect)?);
        self.search_initial_scan(&mut state, &name, keys, now, &mut effects)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_tagvals(&mut self, a: &[Bytes]) -> Result<Resp, String> {
        arity(a, 3)?;
        let catalog = self.search_catalog();
        let state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?;
        let index = &state.indexes[name];
        self.search_authorize(index)?;
        let field = arg(a, 2)?;
        if !index
            .schema
            .fields
            .iter()
            .any(|f| f.name == field && matches!(f.kind, FieldKind::Tag))
        {
            return Err("No such TAG field".into());
        }
        let Some(FieldIndex::Tag(values)) = index.fields.get(field) else {
            return Err("No such TAG field".into());
        };
        Ok(self.search_set(values.keys().map(text).collect()))
    }
    pub(super) fn search_config(&mut self, a: &[Bytes]) -> Result<Resp, String> {
        let op = arg(a, 1)?.to_ascii_uppercase();
        if op == "GET" {
            arity(a, 3)?;
        } else if op == "SET" {
            arity(a, 4)?;
        } else {
            return Err("Unknown FT.CONFIG subcommand".into());
        }
        let name = arg(a, 2)?.to_ascii_uppercase();
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        if op == "GET" {
            let values = config_values(&state.config);
            let values: Vec<_> = values
                .into_iter()
                .filter(|(key, _)| name == "*" || *key == name)
                .collect();
            return Ok(
                if matches!(self.command_version(), crate::protocol::Version::Resp3) {
                    Resp::Map(
                        values
                            .into_iter()
                            .map(|(k, v)| (text(k), text(v)))
                            .collect(),
                    )
                } else {
                    Resp::Array(Some(
                        values
                            .into_iter()
                            .map(|(k, v)| Resp::Array(Some(vec![text(k), text(v)])))
                            .collect(),
                    ))
                },
            );
        }
        let value = arg(a, 3)?;
        state.save_config();
        let c = &mut state.config;
        let reschedule_gc = name == "FORK_GC_RUN_INTERVAL";
        match name.as_str() {
            "DEFAULT_DIALECT" => c.default_dialect = positive_range(value, 1, 4)? as u8,
            "TIMEOUT" => c.timeout_ms = nonnegative(value)?,
            "MINPREFIX" => c.min_prefix = positive_range(value, 1, usize::MAX as u64)? as usize,
            "MINSTEMLEN" => c.min_stem = positive_range(value, 2, u32::MAX as u64)? as usize,
            "MAXEXPANSIONS" | "MAXPREFIXEXPANSIONS" => {
                c.max_expansions = positive_range(value, 1, usize::MAX as u64)? as usize
            }
            "MAXSEARCHRESULTS" => c.max_results = result_limit(value)?,
            "MAXAGGREGATERESULTS" => c.max_aggregate = result_limit(value)?,
            "CURSOR_MAX_IDLE" => c.cursor_max_idle = positive_range(value, 0, i64::MAX as u64)?,
            "ON_TIMEOUT" => {
                let value = value.to_ascii_uppercase();
                if !matches!(value.as_str(), "RETURN" | "FAIL" | "RETURN_STRICT") {
                    return Err("ON_TIMEOUT must be RETURN, FAIL or RETURN_STRICT".into());
                }
                if value == "RETURN_STRICT" && c.workers == 0 {
                    return Err("RETURN_STRICT requires WORKERS greater than zero".into());
                }
                c.on_timeout_fail = value == "FAIL";
                c.on_timeout_strict = value == "RETURN_STRICT";
                c.extras.remove("ON_TIMEOUT");
            }
            "DEFAULT_SCORER" => {
                let value = value.to_ascii_uppercase();
                if !matches!(
                    value.as_str(),
                    "TFIDF"
                        | "TFIDF.DOCNORM"
                        | "BM25"
                        | "BM25STD"
                        | "BM25STD.NORM"
                        | "BM25STD.TANH"
                        | "DISMAX"
                        | "DOCSCORE"
                        | "HAMMING"
                ) {
                    return Err("Unknown scorer".into());
                }
                c.extras.insert(name, value);
            }
            "MIN_PHONETIC_TERM_LEN" | "MAX_AGGREGATE_GROUPS" => {
                let value = positive_range(value, 1, usize::MAX as u64)?.to_string();
                c.extras.insert(name, value);
            }
            "BM25STD_TANH_FACTOR" => {
                let factor = value
                    .parse::<f64>()
                    .map_err(|_| "Invalid positive number")?;
                if !factor.is_finite() || factor <= 0.0 {
                    return Err("Invalid positive number".into());
                }
                c.extras.insert(name, factor.to_string());
            }
            "WORKERS" => c.set_workers(positive_range(value, 0, 8192)? as usize)?,
            "FORK_GC_CLEAN_THRESHOLD" => {
                c.gc.threshold = positive_range(value, 1, i64::MAX as u64)?
            }
            "FORK_GC_RETRY_INTERVAL" => c.gc.retry = positive_range(value, 1, i64::MAX as u64)?,
            "FORK_GC_RUN_INTERVAL" => c.gc.interval = positive_range(value, 1, i64::MAX as u64)?,
            "FORKGC_SLEEP_BEFORE_EXIT" => c.gc.sleep = positive_range(value, 0, i64::MAX as u64)?,
            "_MAX_FOREGROUND_TIMEOUT_LIMIT" => {
                c.foreground_timeout_ms = positive_range(value, 0, i64::MAX as u64)?
            }
            "INDEX_CURSOR_LIMIT"
            | "FORK_GC_CLEAN_NUMERIC_EMPTY_NODES"
            | "GCSCANSIZE"
            | "SEARCH_THREADS"
            | "SEARCH_IO_THREADS"
            | "BG_INDEX_SLEEP_DURATION_US" => return Err("Not modifiable at runtime".into()),
            "EXTLOAD"
            | "NOGC"
            | "MAXDOCTABLESIZE"
            | "WORKER_THREADS"
            | "MT_MODE"
            | "TIERED_HNSW_BUFFER_LIMIT"
            | "PRIVILEGED_THREADS_NUM"
            | "WORKERS_PRIORITY_BIAS_THRESHOLD"
            | "FRISOINI"
            | "GC_POLICY"
            | "NO_MEM_POOLS"
            | "PARTIAL_INDEXED_DOCS"
            | "UPGRADE_INDEX"
            | "RAW_DOCID_ENCODING"
            | "MULTI_TEXT_SLOP"
            | "BG_INDEX_SLEEP_GAP" => return Err("Not modifiable at runtime".into()),
            _ => return Err("Unknown configuration option".into()),
        }
        if reschedule_gc {
            state.gc.next = super::super::unix_milliseconds().saturating_add(
                state
                    .config
                    .gc
                    .interval
                    .saturating_mul(1000)
                    .min(i64::MAX as u64) as i64,
            );
        }
        Ok(Resp::Simple("OK"))
    }
    pub(super) fn search_spellcheck(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        if a.len() < 3 {
            return Err("Wrong number of arguments".into());
        }
        let catalog = self.search_catalog();
        let state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?.to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let mut distance = 1;
        let mut include = Vec::new();
        let mut exclude = Vec::new();
        let mut options = super::query::Options::default();
        let mut at = 3;
        while at < a.len() {
            match arg(a, at)?.to_ascii_uppercase().as_str() {
                "DISTANCE" => {
                    distance = positive_range(arg(a, at + 1)?, 1, 4)? as usize;
                    at += 2;
                }
                "DIALECT" => {
                    options.dialect = positive_range(arg(a, at + 1)?, 1, 4)? as u32;
                    at += 2;
                }
                "TERMS" => {
                    let mode = arg(a, at + 1)?.to_ascii_uppercase();
                    if !matches!(mode.as_str(), "INCLUDE" | "EXCLUDE") {
                        return Err("TERMS requires INCLUDE or EXCLUDE".into());
                    }
                    at += 2;
                    loop {
                        let dictionary = arg(a, at)?;
                        let dict = state
                            .dictionaries
                            .get(dictionary)
                            .ok_or("Dictionary does not exist")?;
                        if mode == "INCLUDE" {
                            include.push(dict);
                        } else {
                            exclude.push(dict);
                        }
                        at += 1;
                        if at == a.len()
                            || matches!(
                                arg(a, at)?.to_ascii_uppercase().as_str(),
                                "DISTANCE" | "DIALECT" | "TERMS" | "PARAMS"
                            )
                        {
                            break;
                        }
                    }
                }
                "PARAMS" => {
                    let n = nonnegative(arg(a, at + 1)?)? as usize;
                    at += 2;
                    if !n.is_multiple_of(2) || n > a.len().saturating_sub(at) {
                        return Err("Invalid PARAMS count".into());
                    }
                    for pair in a[at..at + n].as_chunks::<2>().0 {
                        if options
                            .params
                            .insert(string(&pair[0])?.to_owned(), pair[1].to_vec())
                            .is_some()
                        {
                            return Err("Duplicate parameter".into());
                        }
                    }
                    at += n;
                }
                _ => return Err("Unknown SPELLCHECK option".into()),
            }
        }
        let terms = super::query::spellcheck_terms(index, arg(a, 2)?, &options, &state.config)?;
        let total = index.docs.values().filter(|d| live(d, now)).count();
        let resp3 = matches!(self.command_version(), crate::protocol::Version::Resp3);
        let mut output = if resp3 {
            Resp::Map(Vec::new())
        } else {
            Resp::Array(Some(Vec::new()))
        };
        let mut checked = BTreeSet::new();
        for (term, fields) in terms {
            if !checked.insert(term.clone())
                || exclude
                    .iter()
                    .any(|dict| dict.iter().any(|word| super::text::normalize(word) == term))
            {
                continue;
            }
            if document_frequency(index, &fields, &term, now) > 0 {
                continue;
            }
            let mut candidates = BTreeSet::new();
            for (field, posting) in &index.fields {
                if !fields.is_empty() && !fields.contains(field) {
                    continue;
                }
                if let FieldIndex::Text(postings) = posting {
                    for word in postings.keys() {
                        if !word.starts_with(['+', '$', '~'])
                            && super::query::levenshtein(&term, word, distance) <= distance
                        {
                            candidates.insert(word.as_str());
                        }
                    }
                }
            }
            for dict in &include {
                for word in *dict {
                    if super::query::levenshtein(&term, &super::text::normalize(word), distance)
                        <= distance
                    {
                        candidates.insert(word.as_str());
                    }
                }
            }
            let mut suggestions: Vec<_> = candidates
                .into_iter()
                .filter_map(|word| {
                    let frequency =
                        document_frequency(index, &fields, &super::text::normalize(word), now);
                    (frequency > 0 || include.iter().any(|dict| dict.contains(word)))
                        .then_some((word, frequency))
                })
                .collect();
            suggestions.sort_by(|(a, ac), (b, bc)| bc.cmp(ac).then_with(|| a.cmp(b)));
            let suggestions = suggestions
                .into_iter()
                .map(|(word, count)| {
                    let score = if total == 0 {
                        0.0
                    } else {
                        count as f64 / total as f64
                    };
                    if resp3 {
                        Resp::Map(vec![(text(word), Resp::Double(score))])
                    } else {
                        Resp::Array(Some(vec![bulk(score.to_string()), text(word)]))
                    }
                })
                .collect();
            match &mut output {
                Resp::Map(values) => values.push((text(term), Resp::Array(Some(suggestions)))),
                Resp::Array(Some(values)) => values.push(Resp::Array(Some(vec![
                    text("TERM"),
                    text(term),
                    Resp::Array(Some(suggestions)),
                ]))),
                _ => unreachable!(),
            }
        }
        Ok(if resp3 {
            Resp::Map(vec![(text("results"), output)])
        } else {
            output
        })
    }
    pub(super) fn search_info(&mut self, a: &[Bytes], _now: i64) -> Result<Resp, String> {
        arity(a, 2)?;
        let catalog = self.search_catalog();
        let state = catalog.lock();
        let name = state.resolve(arg(a, 1)?)?.to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let s = &index.schema;
        let mut options = Vec::new();
        for (flag, enabled) in [
            ("NOOFFSETS", s.nooffsets),
            ("NOHL", s.nohl),
            ("NOFIELDS", s.nofields),
            ("NOFREQS", s.nofreqs),
            ("MAXTEXTFIELDS", s.maxtextfields),
        ] {
            if enabled {
                options.push(text(flag));
            }
        }
        let mut definition = vec![
            ("key_type", text(if s.json { "JSON" } else { "HASH" })),
            (
                "prefixes",
                Resp::Array(Some(s.prefixes.iter().cloned().map(bulk).collect())),
            ),
            ("default_score", Resp::Double(s.score)),
            (
                "indexes_all",
                text(if s.indexall { "true" } else { "false" }),
            ),
        ];
        if let Some(filter) = &s.filter {
            definition.push(("filter", text(filter)));
        }
        if let Some(field) = &s.language_field {
            definition.push(("language_field", text(field)));
        }
        if let Some(field) = &s.score_field {
            definition.push(("score_field", text(field)));
        }
        if let Some(field) = &s.payload_field {
            definition.push(("payload_field", text(field)));
        }
        let attributes = s
            .fields
            .iter()
            .map(|f| {
                let kind = match f.kind {
                    FieldKind::Text => "TEXT",
                    FieldKind::Tag => "TAG",
                    FieldKind::Numeric => "NUMERIC",
                    FieldKind::Geo => "GEO",
                    FieldKind::GeoShape => "GEOSHAPE",
                    FieldKind::Vector => "VECTOR",
                };
                let mut fields = vec![
                    ("identifier", text(&f.source)),
                    ("attribute", text(&f.name)),
                    ("type", text(kind)),
                ];
                if matches!(f.kind, FieldKind::Text) {
                    fields.push(("WEIGHT", Resp::Double(f.weight)));
                }
                if matches!(f.kind, FieldKind::Tag) {
                    fields.push((
                        "SEPARATOR",
                        f.separator
                            .map_or(Resp::Bulk(None), |s| text(s.to_string())),
                    ));
                }
                if let Some(phonetic) = &f.phonetic {
                    fields.push(("PHONETIC", text(phonetic)));
                }
                if matches!(f.kind, FieldKind::GeoShape) {
                    fields.push((
                        "coord_system",
                        text(if f.spherical { "SPHERICAL" } else { "FLAT" }),
                    ));
                }
                if let Some(v) = &f.vector {
                    fields.extend([
                        ("algorithm", text(&v.algorithm)),
                        ("data_type", text(v.datatype())),
                        ("dim", integer(v.dims())),
                        ("distance_metric", text(v.metric())),
                    ]);
                    match v.algorithm.as_str() {
                        "HNSW" => fields.extend([
                            ("M", integer(v.m)),
                            ("ef_construction", integer(v.ef_construction)),
                            ("ef_runtime", integer(v.ef_runtime)),
                            ("epsilon", Resp::Double(v.epsilon)),
                        ]),
                        "SVS-VAMANA" => {
                            fields.extend([
                                (
                                    "construction_window_size",
                                    integer(v.construction_window_size),
                                ),
                                ("graph_max_degree", integer(v.graph_max_degree)),
                                ("search_window_size", integer(v.search_window_size)),
                                ("epsilon", Resp::Double(v.epsilon)),
                                ("training_threshold", integer(v.training_threshold)),
                            ]);
                            if let Some(compression) = &v.compression {
                                fields.push(("compression", text(compression)));
                            }
                            if let Some(reduce) = v.reduce {
                                fields.push(("reduce", integer(reduce)));
                            }
                        }
                        _ => {}
                    }
                }
                let mut result = self.search_metadata_pairs(fields);
                let flags: Vec<_> = [
                    ("SORTABLE", f.sortable),
                    ("UNF", f.sortable && f.unf),
                    ("NOINDEX", f.noindex),
                    ("NOSTEM", f.nostem),
                    ("CASESENSITIVE", f.case_sensitive),
                    ("INDEXEMPTY", f.index_empty),
                    ("INDEXMISSING", f.index_missing),
                    ("WITHSUFFIXTRIE", f.suffix),
                ]
                .into_iter()
                .filter(|(_, enabled)| *enabled)
                .map(|(name, _)| text(name))
                .collect();
                match &mut result {
                    Resp::Array(Some(values)) => values.extend(flags),
                    Resp::Map(values) => {
                        values.push((text("flags"), Resp::Array(Some(flags))));
                    }
                    _ => {}
                }
                result
            })
            .collect();
        let mut vocabulary = BTreeSet::new();
        let mut records = 0usize;
        let mut offsets = 0usize;
        let mut inverted = 0usize;
        let mut vector_bytes = 0usize;
        let mut shape_bytes = 0usize;
        let mut text_overhead = 0usize;
        let mut tag_overhead = 0usize;
        let mut blocks = 0usize;
        for field in index.fields.values() {
            match field {
                FieldIndex::Text(terms) => {
                    text_overhead += std::mem::size_of_val(terms);
                    for (term, docs) in terms {
                        vocabulary.insert(term.as_str());
                        records += docs.len();
                        blocks += docs.len();
                        offsets += docs.values().map(Vec::len).sum::<usize>();
                        inverted += term.len()
                            + docs.len() * std::mem::size_of::<DocId>()
                            + docs
                                .values()
                                .map(|v| v.len() * std::mem::size_of::<u32>())
                                .sum::<usize>();
                        text_overhead += std::mem::size_of::<String>()
                            + std::mem::size_of_val(docs)
                            + docs.len() * std::mem::size_of::<Vec<u32>>();
                    }
                }
                FieldIndex::Tag(terms) => {
                    tag_overhead += std::mem::size_of_val(terms);
                    for (term, docs) in terms {
                        records += docs.len();
                        blocks += usize::from(!docs.is_empty());
                        inverted += term.len() + docs.len() * std::mem::size_of::<DocId>();
                        tag_overhead += std::mem::size_of::<String>() + std::mem::size_of_val(docs);
                    }
                }
                FieldIndex::Numeric(numbers) => {
                    for docs in numbers.values() {
                        records += docs.len();
                        blocks += usize::from(!docs.is_empty());
                        inverted +=
                            std::mem::size_of::<f64>() + docs.len() * std::mem::size_of::<DocId>();
                    }
                }
                FieldIndex::Vector(vectors) => vector_bytes += vectors.memory_bytes(),
                FieldIndex::Geo(points) => inverted += points.memory_bytes(),
                FieldIndex::Shape(shapes) => shape_bytes += shapes.memory_bytes(),
            }
        }
        let mut document_bytes = 0usize;
        let mut sortable_bytes = 0usize;
        for doc in index.docs.values() {
            document_bytes += std::mem::size_of::<Document>()
                + doc.key.len()
                + doc.payload.as_ref().map_or(0, Vec::len);
            for (name, value) in &doc.fields {
                let bytes = match value {
                    IndexedValue::Text { strings, tokens } => {
                        strings.iter().map(String::len).sum::<usize>()
                            + tokens
                                .iter()
                                .map(|t| std::mem::size_of::<Token>() + t.term.len())
                                .sum::<usize>()
                    }
                    IndexedValue::Tag(v) => v.iter().map(String::len).sum(),
                    IndexedValue::Numeric(v) => v.len() * std::mem::size_of::<f64>(),
                    IndexedValue::Geo(v) => v.len() * std::mem::size_of::<[f64; 2]>(),
                    IndexedValue::Geometry(v) => v.len(),
                    IndexedValue::Vector(v) => {
                        v.iter().map(|v| v.len() * std::mem::size_of::<f64>()).sum()
                    }
                    IndexedValue::Empty | IndexedValue::Missing => 0,
                };
                document_bytes += name.len() + bytes;
                if s.fields.iter().any(|f| f.name == *name && f.sortable) {
                    sortable_bytes += bytes;
                }
            }
        }
        let docs = index.docs.len();
        let mb = |n: usize| Resp::Double(n as f64 / (1024.0 * 1024.0));
        let errors = self.search_metadata_pairs(vec![
            ("indexing failures", Resp::Unsigned(index.errors)),
            (
                "last indexing error",
                index.last_error.as_ref().map_or_else(|| text("N/A"), text),
            ),
            (
                "last indexing error key",
                index
                    .last_error_key
                    .as_ref()
                    .map_or_else(|| text("N/A"), |k| bulk(Bytes::copy_from_slice(k))),
            ),
            ("background indexing status", text("OK")),
        ]);
        let field_stats = s
            .fields
            .iter()
            .map(|f| {
                let error = index.field_errors.get(&f.name);
                self.search_metadata_pairs(vec![
                    ("identifier", text(&f.source)),
                    ("attribute", text(&f.name)),
                    (
                        "Index Errors",
                        self.search_metadata_pairs(vec![
                            (
                                "indexing failures",
                                Resp::Unsigned(error.map_or(0, |e| e.0)),
                            ),
                            (
                                "last indexing error",
                                error.map_or_else(|| text("N/A"), |e| text(&e.1)),
                            ),
                            (
                                "last indexing error key",
                                error.map_or_else(
                                    || text("N/A"),
                                    |e| bulk(Bytes::copy_from_slice(&e.2)),
                                ),
                            ),
                        ]),
                    ),
                ])
            })
            .collect();
        let cursors = state.cursors.values().filter(|c| c.index == name).count();
        let response = self.search_metadata_pairs(vec![
            ("index_name", text(&name)),
            ("index_options", Resp::Array(Some(options))),
            ("index_definition", self.search_metadata_pairs(definition)),
            ("attributes", Resp::Array(Some(attributes))),
            ("num_docs", integer(docs)),
            (
                "max_doc_id",
                Resp::Unsigned(index.next_id.saturating_sub(1)),
            ),
            ("num_terms", integer(vocabulary.len())),
            ("num_records", integer(records)),
            ("inverted_sz_mb", mb(inverted)),
            ("vector_index_sz_mb", mb(vector_bytes)),
            (
                "offset_vectors_sz_mb",
                mb(offsets * std::mem::size_of::<u32>()),
            ),
            ("doc_table_size_mb", mb(document_bytes)),
            ("sortable_values_size_mb", mb(sortable_bytes)),
            (
                "key_table_size_mb",
                mb(index.keys.keys().map(Vec::len).sum()),
            ),
            ("geoshapes_sz_mb", mb(shape_bytes)),
            ("text_overhead_sz_mb", mb(text_overhead)),
            ("tag_overhead_sz_mb", mb(tag_overhead)),
            (
                "total_index_memory_sz_mb",
                mb(inverted
                    + text_overhead
                    + tag_overhead
                    + vector_bytes
                    + shape_bytes
                    + document_bytes),
            ),
            ("total_inverted_index_blocks", integer(blocks)),
            (
                "offset_bits_per_record_avg",
                Resp::Double(if records == 0 {
                    0.0
                } else {
                    (offsets * u32::BITS as usize) as f64 / records as f64
                }),
            ),
            (
                "records_per_doc_avg",
                Resp::Double(if docs == 0 {
                    0.0
                } else {
                    records as f64 / docs as f64
                }),
            ),
            (
                "bytes_per_record_avg",
                Resp::Double(if records == 0 {
                    0.0
                } else {
                    inverted as f64 / records as f64
                }),
            ),
            (
                "offsets_per_term_avg",
                Resp::Double(if records == 0 {
                    0.0
                } else {
                    offsets as f64 / records as f64
                }),
            ),
            ("hash_indexing_failures", Resp::Unsigned(index.errors)),
            ("total_indexing_time", Resp::Double(index.indexing_time_ms)),
            ("number_of_uses", Resp::Unsigned(index.uses)),
            ("indexing", Resp::Integer(0)),
            ("cleaning", Resp::Integer(i64::from(catalog.gc_running()))),
            ("percent_indexed", Resp::Double(1.0)),
            (
                "gc_stats",
                self.search_metadata_pairs(vec![
                    ("scope", text("catalog")),
                    ("bytes_collected", Resp::Unsigned(state.gc.bytes_collected)),
                    ("total_ms", Resp::Double(state.gc.total_ms)),
                    ("total_cycles", Resp::Unsigned(state.gc.cycles)),
                    (
                        "average_cycle_time_ms",
                        Resp::Double(if state.gc.cycles == 0 {
                            0.
                        } else {
                            state.gc.total_ms / state.gc.cycles as f64
                        }),
                    ),
                    ("last_run_time_ms", Resp::Double(state.gc.last_ms)),
                    ("gc_blocks_denied", Resp::Unsigned(state.gc.denied)),
                    ("gc_failures", Resp::Unsigned(state.gc.failures)),
                ]),
            ),
            (
                "cursor_stats",
                self.search_metadata_pairs(vec![
                    ("global_total", integer(state.cursors.len())),
                    ("global_idle", integer(state.cursors.len())),
                    ("index_total", integer(cursors)),
                    (
                        "index_capacity",
                        integer(
                            state
                                .config
                                .extras
                                .get("INDEX_CURSOR_LIMIT")
                                .and_then(|v| v.parse().ok())
                                .unwrap_or(128),
                        ),
                    ),
                ]),
            ),
            (
                "dialect_stats",
                self.search_metadata_pairs(vec![
                    ("dialect_1", Resp::Unsigned(index.dialect_uses[0])),
                    ("dialect_2", Resp::Unsigned(index.dialect_uses[1])),
                    ("dialect_3", Resp::Unsigned(index.dialect_uses[2])),
                    ("dialect_4", Resp::Unsigned(index.dialect_uses[3])),
                ]),
            ),
            ("Index Errors", errors),
            ("field statistics", Resp::Array(Some(field_stats))),
            (
                "stopwords_list",
                Resp::Array(Some(s.stopwords.iter().map(text).collect())),
            ),
        ]);
        Ok(response)
    }
}
fn nonnegative(value: &str) -> Result<u64, String> {
    value
        .parse()
        .map_err(|_| "Expected nonnegative integer".into())
}
fn positive_range(value: &str, min: u64, max: u64) -> Result<u64, String> {
    let n = nonnegative(value)?;
    if n < min || n > max {
        Err("Configuration value out of range".into())
    } else {
        Ok(n)
    }
}
fn result_limit(value: &str) -> Result<usize, String> {
    if value == "-1" {
        Ok(usize::MAX)
    } else {
        usize::try_from(nonnegative(value)?).map_err(|_| "Result limit out of range".into())
    }
}
fn config_values(c: &Config) -> BTreeMap<String, String> {
    let mut values: BTreeMap<String, String> = [
        ("DEFAULT_DIALECT", c.default_dialect.to_string()),
        ("TIMEOUT", c.timeout_ms.to_string()),
        ("MINPREFIX", c.min_prefix.to_string()),
        ("MINSTEMLEN", c.min_stem.to_string()),
        ("MAXEXPANSIONS", c.max_expansions.to_string()),
        ("MAXPREFIXEXPANSIONS", c.max_expansions.to_string()),
        (
            "MAXSEARCHRESULTS",
            if c.max_results == usize::MAX {
                "-1".into()
            } else {
                c.max_results.to_string()
            },
        ),
        (
            "MAXAGGREGATERESULTS",
            if c.max_aggregate == usize::MAX {
                "-1".into()
            } else {
                c.max_aggregate.to_string()
            },
        ),
        ("CURSOR_MAX_IDLE", c.cursor_max_idle.to_string()),
        (
            "ON_TIMEOUT",
            if c.on_timeout_fail {
                "FAIL".into()
            } else if c.on_timeout_strict {
                "RETURN_STRICT".into()
            } else {
                "RETURN".into()
            },
        ),
        ("DEFAULT_SCORER", "BM25STD".into()),
        ("MIN_PHONETIC_TERM_LEN", "3".into()),
        ("BM25STD_TANH_FACTOR", "4".into()),
        ("INDEX_CURSOR_LIMIT", "128".into()),
        ("MAX_AGGREGATE_GROUPS", "1000000".into()),
        ("WORKERS", c.workers.to_string()),
        (
            "_MAX_FOREGROUND_TIMEOUT_LIMIT",
            c.foreground_timeout_ms.to_string(),
        ),
        ("FORK_GC_CLEAN_THRESHOLD", c.gc.threshold.to_string()),
        ("FORK_GC_RETRY_INTERVAL", c.gc.retry.to_string()),
        ("FORK_GC_RUN_INTERVAL", c.gc.interval.to_string()),
        ("FORKGC_SLEEP_BEFORE_EXIT", c.gc.sleep.to_string()),
    ]
    .into_iter()
    .map(|(k, v)| (k.into(), v))
    .collect();
    values.extend(c.extras.iter().map(|(k, v)| (k.clone(), v.clone())));
    values
}
