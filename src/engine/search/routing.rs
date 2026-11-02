//! Routing uses the real query result, never an index-wide source-key estimate.
use super::super::{Engine, Resp};
use super::{
    failure, hybrid,
    model::{DocId, Index, QueryResult},
    options, pipeline, query, schema, string,
};
use crate::protocol::Version;
use bytes::Bytes;
use std::time::Instant;

pub(crate) struct SearchPlan {
    keys: Option<Vec<Bytes>>,
    index: Option<String>,
    pub(super) prepared: Option<PreparedExecution>,
}

pub(super) enum PreparedExecution {
    Search {
        input: String,
        options: options::SearchOptions,
        result: QueryResult,
        parsing_ms: f64,
        query_ms: f64,
        started: Instant,
    },
    Aggregate {
        input: String,
        prepared: pipeline::Prepared,
        result: QueryResult,
        parsing_ms: f64,
        query_ms: f64,
        started: Instant,
    },
    Hybrid {
        prepared: hybrid::PreparedHybrid,
    },
}

impl SearchPlan {
    /// `None` is reserved for commands that genuinely perform an initial scan.
    pub(crate) fn source_keys(&self) -> Option<&[Bytes]> {
        self.keys.as_deref()
    }

    pub(super) fn into_prepared(self, index: &str) -> Result<Option<PreparedExecution>, String> {
        if self.prepared.is_some() && self.index.as_deref() != Some(index) {
            return Err("Prepared search index does not match execution index".into());
        }
        Ok(self.prepared)
    }
}

fn millis(started: Instant) -> f64 {
    started.elapsed().as_secs_f64() * 1000.
}

/// PROFILE has the same prepared execution as its enclosed command.
pub(super) fn profile_command(a: &[Bytes]) -> Result<(Vec<Bytes>, bool), String> {
    let mode = string(a.get(2).ok_or("Missing profile mode")?)?.to_ascii_uppercase();
    let mut p = 3;
    let limited = a.get(p).is_some_and(|b| b.eq_ignore_ascii_case(b"LIMITED"));
    if limited {
        p += 1;
    }
    if !a.get(p).is_some_and(|b| b.eq_ignore_ascii_case(b"QUERY")) {
        return Err("FT.PROFILE requires QUERY".into());
    }
    p += 1;
    let mut command = Vec::with_capacity(a.len() - p + 2);
    command.push(Bytes::from(format!("FT.{mode}")));
    command.push(a.get(1).ok_or("Missing index")?.clone());
    command.extend_from_slice(&a[p..]);
    if !matches!(mode.as_str(), "SEARCH" | "AGGREGATE" | "HYBRID") {
        return Err("FT.PROFILE supports SEARCH, AGGREGATE or HYBRID".into());
    }
    Ok((command, limited))
}

impl Engine {
    /// Root must gather and reap `search_due_keys(now)` before preparing a query.
    /// In particular, preparing on an empty routing engine must never refresh sources.
    pub(crate) fn search_prepare(&mut self, a: &[Bytes], now: i64) -> Result<SearchPlan, Resp> {
        self.search_prepare_inner(a, now).map_err(failure)
    }

    pub(crate) fn search_set_plan(&mut self, plan: SearchPlan) {
        self.search_plan = Some(plan);
    }

    fn search_plan_key(&self, index: &Index, id: DocId, now: i64) -> Option<Bytes> {
        let doc = index.docs.get(&id)?;
        if doc.expires_at.is_some_and(|d| d <= now) || doc.field_deadline.is_some_and(|d| d <= now)
        {
            return None;
        }
        let key = Bytes::copy_from_slice(&doc.key);
        self.command_key_readable(&key).then_some(key)
    }

    fn search_plan_keys(
        &self,
        index: &Index,
        ids: impl IntoIterator<Item = DocId>,
        now: i64,
    ) -> Vec<Bytes> {
        ids.into_iter()
            .filter_map(|id| self.search_plan_key(index, id, now))
            .collect()
    }

    fn search_prepare_inner(&mut self, a: &[Bytes], now: i64) -> Result<SearchPlan, String> {
        if self.database() != 0 {
            return Err("Cannot access search indexes on db != 0".into());
        }
        let catalog = self.search_catalog();
        if catalog.has_due(now) {
            return Err("Search source expiry must be reaped before preparation".into());
        }
        let profile;
        let a = if a
            .first()
            .is_some_and(|c| c.eq_ignore_ascii_case(b"FT.PROFILE"))
        {
            profile = profile_command(a)?.0;
            profile.as_slice()
        } else {
            a
        };
        let command = a.first().ok_or("Missing search command")?;
        let mut plan = SearchPlan {
            keys: Some(Vec::new()),
            index: None,
            prepared: None,
        };
        if command.eq_ignore_ascii_case(b"FT.CREATE") {
            let (_, skip) = schema::create(a, now)?;
            if !skip {
                plan.keys = None;
            }
            return Ok(plan);
        }
        if command.eq_ignore_ascii_case(b"FT.ALTER") {
            if !a
                .get(2)
                .is_some_and(|v| v.eq_ignore_ascii_case(b"SKIPINITIALSCAN"))
            {
                plan.keys = None;
            }
            return Ok(plan);
        }
        if command.eq_ignore_ascii_case(b"FT.SYNUPDATE") {
            if !a
                .get(3)
                .is_some_and(|v| v.eq_ignore_ascii_case(b"SKIPINITIALSCAN"))
            {
                plan.keys = None;
            }
            return Ok(plan);
        }
        let state = catalog.lock();
        if command.eq_ignore_ascii_case(b"FT.DROPINDEX")
            && a.get(2).is_some_and(|v| v.eq_ignore_ascii_case(b"DD"))
        {
            let name = state.resolve(string(a.get(1).ok_or("Missing index")?)?)?;
            let index = &state.indexes[name];
            self.search_authorize(index)?;
            // Include every indexed source: execution must also reject inaccessible DD keys.
            plan.keys = Some(
                index
                    .docs
                    .values()
                    .map(|d| Bytes::copy_from_slice(&d.key))
                    .collect(),
            );
            return Ok(plan);
        }
        if command.eq_ignore_ascii_case(b"FT.CURSOR")
            && a.get(1).is_some_and(|v| v.eq_ignore_ascii_case(b"READ"))
        {
            let name = state.resolve(string(a.get(2).ok_or("Missing index")?)?)?;
            let index = &state.indexes[name];
            self.search_authorize(index)?;
            let id = string(a.get(3).ok_or("Missing cursor ID")?)?
                .parse::<u64>()
                .ok()
                .filter(|id| *id != 0)
                .ok_or("Bad cursor ID")?;
            let cursor = state
                .cursors
                .get(&id)
                .filter(|c| c.index == name && c.deadline > now)
                .ok_or("Cursor does not exist")?;
            let count = if a.len() == 6 {
                if !a[4].eq_ignore_ascii_case(b"COUNT") {
                    return Err("Unknown cursor option".into());
                }
                string(&a[5])?
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or("Cursor COUNT must be positive")?
            } else {
                cursor.count
            };
            let mut keys = Vec::new();
            let mut accepted = 0;
            for row in &cursor.rows {
                if accepted == count {
                    break;
                }
                if let Some(id) = row
                    .get("__search_doc_id")
                    .and_then(serde_json::Value::as_u64)
                {
                    let Some(key) = self.search_plan_key(index, id, now) else {
                        continue;
                    };
                    keys.push(key);
                }
                accepted += 1;
            }
            plan.keys = Some(keys);
            return Ok(plan);
        }
        let is_search = command.eq_ignore_ascii_case(b"FT.SEARCH");
        let is_aggregate = command.eq_ignore_ascii_case(b"FT.AGGREGATE");
        let is_hybrid = command.eq_ignore_ascii_case(b"FT.HYBRID");
        if !is_search && !is_aggregate && !is_hybrid {
            return Ok(plan);
        }
        let started = Instant::now();
        let name = state
            .resolve(string(a.get(1).ok_or("Missing index")?)?)?
            .to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        plan.index = Some(name);
        if is_hybrid {
            let prepared = hybrid::prepare(index, &a[2..], &state.config, now)?;
            plan.keys =
                Some(self.search_plan_keys(index, prepared.candidate_ids().iter().copied(), now));
            plan.prepared = Some(PreparedExecution::Hybrid { prepared });
        } else {
            let input = string(a.get(2).ok_or("Missing query")?)?.to_owned();
            if is_search {
                let options = options::prepare(
                    index,
                    &a[3..],
                    &state.config,
                    self.command_version() == Version::Resp3,
                )?;
                let parsing_ms = millis(started);
                let q = Instant::now();
                let result = query::execute(index, &input, &options.query, &state.config, now)?;
                let query_ms = millis(q);
                plan.keys =
                    Some(self.search_plan_keys(index, result.hits.iter().map(|h| h.id), now));
                plan.prepared = Some(PreparedExecution::Search {
                    input,
                    options,
                    result,
                    parsing_ms,
                    query_ms,
                    started,
                });
            } else {
                let prepared = pipeline::prepare(&a[3..], &state.config)?;
                if prepared.format_expand
                    && (!index.schema.json || self.command_version() != Version::Resp3)
                {
                    return Err(
                        "FORMAT EXPAND is supported only with JSON indexes and RESP3".into(),
                    );
                }
                let parsing_ms = millis(started);
                let q = Instant::now();
                let result = query::execute(index, &input, &prepared.options, &state.config, now)?;
                let query_ms = millis(q);
                plan.keys =
                    Some(self.search_plan_keys(index, result.hits.iter().map(|h| h.id), now));
                plan.prepared = Some(PreparedExecution::Aggregate {
                    input,
                    prepared,
                    result,
                    parsing_ms,
                    query_ms,
                    started,
                });
            }
        }
        Ok(plan)
    }
}
