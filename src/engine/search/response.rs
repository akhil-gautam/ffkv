use super::super::{Engine, Resp, Value, typed};
use super::{
    hybrid,
    model::*,
    options::{self, Projection, SearchOptions},
    pipeline, query,
    routing::{self, PreparedExecution},
    state::{Cursor, Effect, State},
    string, text,
};
use crate::protocol::Version;
use bytes::Bytes;
use serde_json::Value as J;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    time::Instant,
};

fn array(v: Vec<Resp>) -> Resp {
    Resp::Array(Some(v))
}
fn null() -> Resp {
    Resp::Bulk(None)
}
fn integer(n: usize) -> Resp {
    Resp::Integer(n.min(i64::MAX as usize) as i64)
}
fn millis(t: Instant) -> f64 {
    t.elapsed().as_secs_f64() * 1000.
}
fn named(resp3: bool, fields: Vec<(&str, Resp)>) -> Resp {
    if resp3 {
        Resp::Map(fields.into_iter().map(|(k, v)| (text(k), v)).collect())
    } else {
        array(fields.into_iter().flat_map(|(k, v)| [text(k), v]).collect())
    }
}
fn json_reply(v: &J, expand: bool) -> Resp {
    match v {
        J::Null => null(),
        J::String(s) => text(s),
        J::Number(n) if expand => Resp::Double(n.as_f64().unwrap_or(0.)),
        J::Number(n) if n.is_f64() => {
            let mut buffer = ryu::Buffer::new();
            let value = buffer.format_finite(n.as_f64().expect("JSON number is finite"));
            text(value.strip_suffix(".0").unwrap_or(value))
        }
        J::Bool(b) if expand => Resp::Boolean(*b),
        J::Array(a) if expand => array(a.iter().map(|v| json_reply(v, true)).collect()),
        J::Object(o) if expand => Resp::Map(
            o.iter()
                .map(|(k, v)| (text(k), json_reply(v, true)))
                .collect(),
        ),
        _ => text(v.to_string()),
    }
}
fn binary_key(row: &Row) -> Option<Bytes> {
    let a = row.get("__search_binary_key")?.as_array()?;
    let bytes = a
        .iter()
        .map(|v| u8::try_from(v.as_u64()?).ok())
        .collect::<Option<Vec<_>>>()?;
    Some(Bytes::from(bytes))
}
fn sort_key(value: J) -> Resp {
    if let Some(bytes) = super::value::bytes(&value) {
        let mut out = Vec::with_capacity(bytes.len() + 1);
        out.push(b'$');
        out.extend_from_slice(&bytes);
        Resp::Bulk(Some(Bytes::from(out)))
    } else {
        match value {
            J::Null => null(),
            J::Number(n) => text(format!("#{n}")),
            v => text(format!("${v}")),
        }
    }
}
fn row_value(v: &J, resp3: bool, expand: bool) -> Resp {
    render_value(v, resp3, expand, false)
}
fn render_value(v: &J, resp3: bool, expand: bool, native: bool) -> Resp {
    if let Some(string) = super::value::projection_string(v) {
        return if expand {
            render_value(
                super::value::projection_native(v).expect("projection has native value"),
                resp3,
                true,
                true,
            )
        } else {
            text(string)
        };
    }
    if let Some(bytes) = super::value::bytes(v) {
        return Resp::Bulk(Some(match bytes {
            std::borrow::Cow::Borrowed(bytes) => Bytes::copy_from_slice(bytes),
            std::borrow::Cow::Owned(bytes) => Bytes::from(bytes),
        }));
    }
    let object = super::value::json_object(v).or_else(|| v.as_object());
    if let Some(values) = object {
        let fields = values
            .iter()
            .map(|(k, v)| (text(k), render_value(v, resp3, expand, native)))
            .collect::<Vec<_>>();
        return if resp3 {
            Resp::Map(fields)
        } else {
            array(fields.into_iter().flat_map(|(k, v)| [k, v]).collect())
        };
    }
    match v {
        J::Array(values) => array(
            values
                .iter()
                .map(|v| render_value(v, resp3, expand, native))
                .collect(),
        ),
        _ => json_reply(v, native),
    }
}
fn pipeline_buffers(args: &[Bytes]) -> Result<bool, String> {
    let mut p = 0;
    while p < args.len() {
        if args[p].eq_ignore_ascii_case(b"GROUPBY") || args[p].eq_ignore_ascii_case(b"SORTBY") {
            return Ok(true);
        }
        p = pipeline::stage_end(args, p)?;
    }
    Ok(false)
}
fn count_dialect(state: &mut State, name: &str, dialect: u32) {
    state.save_index_metadata(name);
    if let Some(counter) = state
        .indexes
        .get_mut(name)
        .and_then(|i| i.dialect_uses.get_mut(dialect.saturating_sub(1) as usize))
    {
        *counter = counter.saturating_add(1)
    }
}
fn public_row(row: Row, resp3: bool, expand: bool) -> Resp {
    let key = binary_key(&row);
    let fields = row
        .into_iter()
        .filter(|(k, _)| !k.starts_with("__search_") && !k.starts_with("__matched_"))
        .map(|(k, v)| {
            let value = if k == "__key" {
                key.as_ref().map_or_else(
                    || row_value(&v, resp3, expand),
                    |k| Resp::Bulk(Some(k.clone())),
                )
            } else if k == "score" && v.is_array() {
                render_value(&v, resp3, expand, true)
            } else {
                row_value(&v, resp3, expand)
            };
            (text(k), value)
        })
        .collect::<Vec<_>>();
    if resp3 {
        Resp::Map(fields)
    } else {
        array(fields.into_iter().flat_map(|(k, v)| [k, v]).collect())
    }
}
#[derive(Default)]
struct Work {
    parse: f64,
    query: f64,
    pipeline: f64,
    total: f64,
    visited: usize,
    rows: usize,
    plan: String,
    warning: bool,
    branches: Vec<(&'static str, f64, usize)>,
}
impl Work {
    fn reply(self, resp3: bool, limited: bool) -> Resp {
        let iterator = |kind: &str, time: f64, count: usize, plan: Option<&str>| {
            let mut fields = vec![
                ("Type", text(kind)),
                ("Time", Resp::Double(time)),
                ("Number of reading operations", integer(count)),
            ];
            if !limited && let Some(plan) = plan {
                fields.push(("Query plan", text(plan)))
            }
            named(resp3, fields)
        };
        let processor = |kind: &str, time: f64, count: usize| {
            named(
                resp3,
                vec![
                    ("Type", text(kind)),
                    ("Time", Resp::Double(time)),
                    ("Results processed", integer(count)),
                ],
            )
        };
        let common = |time: f64, iter: Resp, processors: Vec<Resp>| {
            named(
                resp3,
                vec![
                    ("Total profile time", Resp::Double(time)),
                    ("Parsing time", Resp::Double(self.parse)),
                    (
                        "Warning",
                        array(vec![text(if self.warning {
                            "Timeout limit was reached"
                        } else {
                            "None"
                        })]),
                    ),
                    ("Iterators profile", iter),
                    ("Result processors profile", array(processors)),
                ],
            )
        };
        if self.branches.is_empty() {
            let kind = self.plan.lines().next().unwrap_or("EMPTY");
            let shard = common(
                self.total,
                iterator(kind, self.query, self.visited, Some(&self.plan)),
                vec![
                    processor("Index", self.query, self.visited),
                    processor("Result Processor", self.pipeline, self.rows),
                ],
            );
            named(
                resp3,
                vec![
                    ("Shards", array(vec![shard])),
                    ("Coordinator", named(resp3, Vec::new())),
                ],
            )
        } else {
            let branches = self
                .branches
                .iter()
                .filter(|(name, _, _)| *name != "COMBINE")
                .map(|(name, time, count)| {
                    (
                        *name,
                        common(
                            *time,
                            iterator(
                                name,
                                *time,
                                *count,
                                if *name == "SEARCH" {
                                    Some(self.plan.as_str())
                                } else {
                                    None
                                },
                            ),
                            vec![processor("Index", *time, *count)],
                        ),
                    )
                })
                .collect();
            let fusion = self.branches.iter().find(|(name, _, _)| *name == "COMBINE");
            let mut processors = Vec::new();
            if let Some((_, time, count)) = fusion {
                processors.push(processor("Hybrid Merger", *time, *count))
            }
            processors.push(processor(
                "Result Processor",
                self.pipeline - fusion.map_or(0., |(_, time, _)| *time),
                self.rows,
            ));
            let coordinator = common(
                self.total,
                iterator("HYBRID", self.query, self.visited, None),
                processors,
            );
            named(
                resp3,
                vec![
                    ("Shards", array(vec![named(resp3, branches)])),
                    ("Coordinator", coordinator),
                ],
            )
        }
    }
}
fn touch(state: &mut State, name: &str, now: i64) -> Result<Vec<Bytes>, String> {
    if let Some(ttl) = state.indexes[name].schema.temporary_ms {
        Ok(vec![state.effect(Effect::Touch {
            index: name.to_owned(),
            deadline: now.saturating_add(ttl),
        })?])
    } else {
        Ok(Vec::new())
    }
}
fn cursor_handoff(
    state: &mut State,
    name: &str,
    rows: Vec<Row>,
    total: usize,
    cursor: Option<pipeline::CursorOptions>,
    now: i64,
) -> Result<(Vec<Row>, Option<u64>), String> {
    let Some(options) = cursor else {
        return Ok((rows, None));
    };
    let count = options.count;
    if count == 0 {
        return Err("Cursor COUNT must be positive".into());
    }
    let mut remaining: VecDeque<_> = rows.into();
    let chunk = remaining.drain(..remaining.len().min(count)).collect();
    if remaining.is_empty() {
        return Ok((chunk, Some(0)));
    }
    let capacity = state
        .config
        .extras
        .get("INDEX_CURSOR_LIMIT")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(128);
    if state.cursors.values().filter(|c| c.index == name).count() >= capacity {
        return Err("Too many cursors allocated for index".into());
    }
    let id = state.next_cursor;
    state.next_cursor = id.checked_add(1).ok_or("Cursor ID exhausted")?;
    let idle = options.max_idle.min(state.config.cursor_max_idle);
    state.cursor_insert(
        id,
        Cursor {
            index: name.to_owned(),
            rows: remaining,
            count,
            max_idle: idle,
            deadline: now.saturating_add(idle.min(i64::MAX as u64) as i64),
            total,
        },
    );
    Ok((chunk, Some(id)))
}
impl Engine {
    fn search_document_live(&self, index: &Index, id: DocId, now: i64) -> bool {
        let Some(doc) = index.docs.get(&id) else {
            return false;
        };
        let key = Bytes::copy_from_slice(&doc.key);
        !doc.expires_at.is_some_and(|d| d <= now)
            && !doc.field_deadline.is_some_and(|d| d <= now)
            && self.command_key_readable(&key)
            && self
                .entries
                .get(&key)
                .is_some_and(|e| !e.expires_at.is_some_and(|d| d <= now))
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Chosen source IDs must be loaded under the query's dialect, clock, deadline, and configuration."
    )]
    fn search_load_hits(
        &self,
        index: &Index,
        ids: &[DocId],
        args: &[Bytes],
        dialect: u32,
        now: i64,
        deadline: Option<Instant>,
        config: &Config,
    ) -> Result<BTreeMap<DocId, Row>, String> {
        let mut loaded = BTreeMap::new();
        let mut paths = BTreeSet::new();
        let mut needs_source = index.schema.fields.iter().any(|f| f.sortable);
        let mut p = 0;
        while p < args.len() {
            let next = pipeline::stage_end(args, p)?;
            if args[p].eq_ignore_ascii_case(b"LOAD") {
                needs_source = true;
                if index.schema.json && !args.get(p + 1).is_some_and(|b| b.as_ref() == b"*") {
                    let mut field = p + 2;
                    while field < next {
                        let path = string(&args[field])?.trim_start_matches('@');
                        if path.starts_with('$') {
                            paths.insert(path.to_owned());
                        }
                        field += 1;
                        if field < next && args[field].eq_ignore_ascii_case(b"AS") {
                            field += 2
                        }
                    }
                }
            }
            p = next;
        }
        for path in &paths {
            super::super::json::JsonValue::search_validate_path(path)
                .map_err(|_| "Invalid JSONPath".to_string())?;
        }
        for id in ids {
            if super::runtime::expired(deadline) {
                if config.on_timeout_fail {
                    return Err("Timeout limit was reached".into());
                }
                break;
            }
            if !self.search_document_live(index, *id, now) {
                continue;
            }
            let doc = &index.docs[id];
            let key = Bytes::copy_from_slice(&doc.key);
            let mut row = if !needs_source {
                Row::new()
            } else if index.schema.json {
                let mut row = Row::new();
                row.insert(
                    "$".into(),
                    super::value::projection(self.search_json_values(&key, "$")?, dialect, true),
                );
                for field in &index.schema.fields {
                    let values = self.search_json_values(&key, &field.source)?;
                    if !values.is_empty() {
                        let value = super::value::projection(values, dialect, false);
                        if field.source != field.name {
                            row.insert(field.source.clone(), value.clone());
                        }
                        row.insert(field.name.clone(), value);
                    }
                }
                row
            } else {
                self.search_source_row(&key, &index.schema.fields, now)?
            };
            for path in &paths {
                let values = self.search_json_values(&key, path)?;
                if !values.is_empty() {
                    row.insert(
                        path.clone(),
                        super::value::projection(values, dialect, path == "$"),
                    );
                }
            }
            if super::runtime::expired(deadline) {
                if config.on_timeout_fail {
                    return Err("Timeout limit was reached".into());
                }
                break;
            }
            loaded.insert(*id, row);
        }
        Ok(loaded)
    }
    pub(super) fn search_query(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        self.search_query_run(a, now).map(|(r, _)| r)
    }
    fn search_query_run(&mut self, a: &[Bytes], now: i64) -> Result<(Resp, Work), String> {
        let started = Instant::now();
        let plan = self.search_plan.take();
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state
            .resolve(string(a.get(1).ok_or("Missing index")?)?)?
            .to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let cached = plan.map(|p| p.into_prepared(&name)).transpose()?.flatten();
        let (input, options, mut result, parse, query_time, started) = match cached {
            Some(PreparedExecution::Search {
                input,
                options,
                result,
                parsing_ms,
                query_ms,
                started,
            }) => (input, options, result, parsing_ms, query_ms, started),
            Some(_) => {
                return Err("Prepared search command does not match execution command".into());
            }
            None => {
                let input = string(a.get(2).ok_or("Missing query")?)?.to_owned();
                let options = options::prepare(
                    index,
                    &a[3..],
                    &state.config,
                    self.command_version() == Version::Resp3,
                )?;
                let parse = millis(started);
                let q = Instant::now();
                let result = query::execute(index, &input, &options.query, &state.config, now)?;
                (input, options, result, parse, millis(q), started)
            }
        };
        if a.get(2).is_none_or(|a| a.as_ref() != input.as_bytes()) {
            return Err("Prepared search query does not match execution query".into());
        }
        let pipeline_started = Instant::now();
        let aliases = query::distance_aliases(&input, &options.query, &state.config)?;
        result
            .hits
            .retain(|h| self.search_document_live(index, h.id, now));
        let deadline = if state.config.enforce_deadline() {
            result.deadline
        } else {
            None
        };
        if let Some((field, asc)) = &options.sort {
            if !aliases.contains(field) && !index.schema.fields.iter().any(|f| f.name == *field) {
                return Err(format!("Unknown sort field '{field}'"));
            }
            let mut values = BTreeMap::new();
            for hit in &result.hits {
                if super::runtime::expired(deadline) {
                    result.timed_out = true;
                    break;
                }
                let value = if aliases.contains(field) {
                    hit.vector_distances
                        .get(field)
                        .copied()
                        .map(J::from)
                        .unwrap_or(J::Null)
                } else {
                    self.search_sort_value(index, &index.docs[&hit.id], field, now)?
                };
                values.insert(hit.id, value);
            }
            result.hits.retain(|h| values.contains_key(&h.id));
            if !super::runtime::sort(
                &mut result.hits,
                |a, b| {
                    let order = super::expression::compare(&values[&a.id], &values[&b.id]);
                    (if *asc { order } else { order.reverse() }).then_with(|| a.id.cmp(&b.id))
                },
                deadline,
            ) {
                result.timed_out = true;
            }
        } else if !super::runtime::sort(
            &mut result.hits,
            |a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)),
            deadline,
        ) {
            result.timed_out = true;
        }
        if result.timed_out && state.config.on_timeout_fail {
            return Err("Timeout limit was reached".into());
        }
        let total = result.hits.len();
        let resp3 = self.command_version() == Version::Resp3;
        let mut rows = Vec::new();
        let mut flat = if resp3 {
            Vec::new()
        } else {
            vec![integer(total)]
        };
        let mut returned = 0;
        let implicit = if options.projection.is_none() {
            let mut fields = BTreeSet::new();
            if let Some(h) = &options.highlight {
                fields.extend(h.fields.iter().map(String::as_str))
            }
            if let Some(s) = &options.summary {
                fields.extend(s.fields.iter().map(String::as_str))
            }
            fields
                .into_iter()
                .map(|field| Projection {
                    field: field.to_owned(),
                    alias: field.to_owned(),
                })
                .collect::<Vec<_>>()
        } else {
            Vec::new()
        };
        let selected = options
            .projection
            .as_deref()
            .or_else(|| (!implicit.is_empty()).then_some(implicit.as_slice()));
        for hit in result.hits.iter().skip(options.offset).take(options.count) {
            if super::runtime::expired(deadline) {
                result.timed_out = true;
                if state.config.on_timeout_fail {
                    return Err("Timeout limit was reached".into());
                }
                break;
            }
            let doc = &index.docs[&hit.id];
            let key = Bytes::copy_from_slice(&doc.key);
            let score = if options.explain {
                let tree = result
                    .score_explanations
                    .get(&hit.id)
                    .ok_or("Missing score explanation")?;
                array(vec![
                    Resp::Double(hit.score),
                    render_value(tree, resp3, true, true),
                ])
            } else {
                Resp::Double(hit.score)
            };
            let payload = if options.payloads {
                doc.payload
                    .as_ref()
                    .map_or_else(null, |v| Resp::Bulk(Some(Bytes::copy_from_slice(v))))
            } else {
                null()
            };
            let sortkey = if options.sortkeys {
                if let Some((field, _)) = &options.sort {
                    sort_key(if aliases.contains(field) {
                        hit.vector_distances
                            .get(field)
                            .copied()
                            .map(J::from)
                            .unwrap_or(J::Null)
                    } else {
                        self.search_sort_value(index, doc, field, now)?
                    })
                } else {
                    null()
                }
            } else {
                null()
            };
            let content = if options.no_content {
                Vec::new()
            } else {
                self.search_content(index, doc, hit, &options, selected, now)?
            };
            if super::runtime::expired(deadline) {
                result.timed_out = true;
                if state.config.on_timeout_fail {
                    return Err("Timeout limit was reached".into());
                }
                break;
            }
            if resp3 {
                let mut fields = vec![("id", Resp::Bulk(Some(key)))];
                if options.scores {
                    fields.push(("score", score))
                }
                if options.payloads {
                    fields.push(("payload", payload))
                }
                if options.sortkeys {
                    fields.push(("sortkey", sortkey))
                }
                if !options.no_content {
                    fields.push(("extra_attributes", Resp::Map(content)))
                }
                fields.push(("values", array(Vec::new())));
                rows.push(named(true, fields));
            } else {
                flat.push(Resp::Bulk(Some(key)));
                if options.scores {
                    flat.push(score)
                }
                if options.payloads {
                    flat.push(payload)
                }
                if options.sortkeys {
                    flat.push(sortkey)
                }
                if !options.no_content {
                    flat.push(array(
                        content.into_iter().flat_map(|(k, v)| [k, v]).collect(),
                    ))
                }
            }
            returned += 1;
        }
        let mut warnings = Vec::new();
        if resp3 {
            if result.timed_out {
                warnings.push(text("Timeout limit was reached"))
            }
            if result.timeout_capped {
                warnings.push(text("MaxTimeoutCapped"))
            }
        }
        let reply = if resp3 {
            named(
                true,
                vec![
                    ("attributes", array(Vec::new())),
                    (
                        "format",
                        text(if options.expand { "EXPAND" } else { "STRING" }),
                    ),
                    ("results", array(rows)),
                    ("total_results", integer(total)),
                    ("warning", array(warnings)),
                ],
            )
        } else {
            array(flat)
        };
        let work = Work {
            parse,
            query: query_time,
            pipeline: millis(pipeline_started),
            total: millis(started),
            visited: result.visited,
            rows: returned,
            plan: result.explanation,
            warning: result.timed_out,
            branches: Vec::new(),
        };
        count_dialect(&mut state, &name, options.query.dialect);
        let effects = touch(&mut state, &name, now)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok((reply, work))
    }
    fn search_sort_value(
        &self,
        index: &Index,
        doc: &Document,
        field: &str,
        now: i64,
    ) -> Result<J, String> {
        let f = index
            .schema
            .fields
            .iter()
            .find(|f| f.name == field)
            .ok_or("Unknown sort field")?;
        let key = Bytes::copy_from_slice(&doc.key);
        let mut value = if index.schema.json {
            self.search_json_values(&key, &f.source)?
                .into_iter()
                .next()
                .unwrap_or(J::Null)
        } else {
            match self.entries.get(&key).map(|e| &e.value) {
                Some(Value::Hash(h)) => h
                    .snapshot(now)
                    .find(|(k, _, _)| k.as_ref() == f.source.as_bytes())
                    .map(|(_, v, _)| super::value::from_bytes(v))
                    .unwrap_or(J::Null),
                _ => J::Null,
            }
        };
        if matches!(f.kind, FieldKind::Numeric) {
            if let Some(n) = super::expression::numeric(&value) {
                value = J::from(n)
            }
        } else if !f.unf
            && !index.schema.json
            && let J::String(s) = &value
        {
            value = J::String(super::text::normalize(s))
        }
        Ok(value)
    }
    fn search_content(
        &self,
        index: &Index,
        doc: &Document,
        hit: &Hit,
        o: &SearchOptions,
        selected: Option<&[Projection]>,
        now: i64,
    ) -> Result<Vec<(Resp, Resp)>, String> {
        let key = Bytes::copy_from_slice(&doc.key);
        let mut out = Vec::new();
        if let Some(projection) = selected {
            for p in projection {
                if let Some(d) = hit.vector_distances.get(&p.field) {
                    out.push((text(&p.alias), text(d.to_string())));
                    continue;
                }
                let f = index.schema.fields.iter().find(|f| f.name == p.field);
                let source = f.map_or(p.field.as_str(), |f| f.source.as_str());
                let value = if index.schema.json {
                    let values = self.search_json_values(&key, source)?;
                    if values.is_empty() {
                        continue;
                    }
                    self.search_json_projection(values, o)
                } else {
                    let Some(Value::Hash(hash)) = self.entries.get(&key).map(|e| &e.value) else {
                        continue;
                    };
                    let Some((_, v, _)) = hash
                        .snapshot(now)
                        .find(|(k, _, _)| k.as_ref() == source.as_bytes())
                    else {
                        continue;
                    };
                    Resp::Bulk(Some(v.clone()))
                };
                out.push((
                    text(&p.alias),
                    self.search_decorate(doc, hit, &p.field, value, o),
                ));
            }
        } else if index.schema.json {
            let entry = self.entries.get(&key).ok_or("Missing source document")?;
            let Value::Typed(data) = &entry.value else {
                return Err("Expected JSON document".into());
            };
            let typed::Data::Json(json) = &**data else {
                return Err("Expected JSON document".into());
            };
            let bytes = json
                .search_document()
                .map_err(|_| "JSON serialization failed")?;
            let reply = if o.expand {
                json_reply(
                    &serde_json::from_slice::<J>(&bytes).map_err(|_| "Invalid JSON")?,
                    true,
                )
            } else {
                Resp::Bulk(Some(bytes))
            };
            out.push((text("$"), reply));
        } else if let Some(Value::Hash(hash)) = self.entries.get(&key).map(|e| &e.value) {
            for (k, v, _) in hash.snapshot(now) {
                let field = index
                    .schema
                    .fields
                    .iter()
                    .find(|f| f.source.as_bytes() == k.as_ref())
                    .map_or_else(
                        || String::from_utf8_lossy(k).into_owned(),
                        |f| f.name.clone(),
                    );
                out.push((
                    Resp::Bulk(Some(k.clone())),
                    self.search_decorate(doc, hit, &field, Resp::Bulk(Some(v.clone())), o),
                ));
            }
        }
        if o.projection.is_none() {
            for (alias, d) in &hit.vector_distances {
                out.push((text(alias), text(d.to_string())))
            }
        }
        Ok(out)
    }
    fn search_json_projection(&self, values: Vec<J>, o: &SearchOptions) -> Resp {
        if o.expand {
            return json_reply(&J::Array(values), true);
        }
        if o.query.dialect >= 3 {
            text(J::Array(values).to_string())
        } else {
            match values.into_iter().next().unwrap_or(J::Null) {
                J::String(s) => text(s),
                v => text(v.to_string()),
            }
        }
    }
    fn search_decorate(
        &self,
        doc: &Document,
        hit: &Hit,
        field: &str,
        value: Resp,
        o: &SearchOptions,
    ) -> Resp {
        let highlight = o.highlight.as_ref().filter(|h| {
            o.projection.is_some() || h.fields.is_empty() || h.fields.iter().any(|f| f == field)
        });
        let summary = o.summary.as_ref().filter(|s| {
            o.projection.is_some() || s.fields.is_empty() || s.fields.iter().any(|f| f == field)
        });
        if highlight.is_none() && summary.is_none() {
            return value;
        }
        let Resp::Bulk(Some(bytes)) = &value else {
            return value;
        };
        let Ok(input) = std::str::from_utf8(bytes) else {
            return value;
        };
        let Some(IndexedValue::Text { strings, tokens }) = doc.fields.get(field) else {
            return value;
        };
        if strings.first().is_none_or(|s| s != input) {
            return value;
        }
        let mut ranges = tokens
            .iter()
            .filter(|t| {
                hit.matched_terms.contains(&t.term)
                    && t.end <= input.len()
                    && input.is_char_boundary(t.start)
                    && input.is_char_boundary(t.end)
            })
            .map(|t| (t.start, t.end))
            .collect::<Vec<_>>();
        ranges.sort_unstable();
        ranges.dedup();
        let mut merged: Vec<(usize, usize)> = Vec::new();
        for (a, b) in ranges {
            if let Some(last) = merged.last_mut()
                && a <= last.1
            {
                last.1 = last.1.max(b);
                continue;
            }
            merged.push((a, b))
        }
        let windows = if let Some(s) = summary {
            summary_windows(input, &merged, s.words, s.fragments)
        } else {
            vec![(0, input.len())]
        };
        let mut out = String::new();
        for (i, (start, end)) in windows.iter().copied().enumerate() {
            if i > 0 {
                out.push_str(summary.map_or("", |s| s.separator.as_str()))
            }
            let mut p = start;
            for (a, b) in merged
                .iter()
                .copied()
                .filter(|(a, b)| *a >= start && *b <= end)
            {
                out.push_str(&input[p..a]);
                if let Some(h) = highlight {
                    out.push_str(&h.open)
                }
                out.push_str(&input[a..b]);
                if let Some(h) = highlight {
                    out.push_str(&h.close)
                }
                p = b
            }
            out.push_str(&input[p..end]);
        }
        if let Some(s) = summary
            && windows.last().is_some_and(|(_, end)| *end < input.len())
        {
            out.push_str(&s.separator)
        }
        text(out)
    }
    pub(super) fn search_aggregate(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        self.search_aggregate_run(a, now, false).map(|(r, _)| r)
    }
    fn search_aggregate_run(
        &mut self,
        a: &[Bytes],
        now: i64,
        profile: bool,
    ) -> Result<(Resp, Work), String> {
        let started = Instant::now();
        let plan = self.search_plan.take();
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state
            .resolve(string(a.get(1).ok_or("Missing index")?)?)?
            .to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let cached = plan.map(|p| p.into_prepared(&name)).transpose()?.flatten();
        let (input, prepared, mut query, parse, query_time, started) = match cached {
            Some(PreparedExecution::Aggregate {
                input,
                prepared,
                result,
                parsing_ms,
                query_ms,
                started,
            }) => (input, prepared, result, parsing_ms, query_ms, started),
            Some(_) => {
                return Err("Prepared search command does not match execution command".into());
            }
            None => {
                let input = string(a.get(2).ok_or("Missing query")?)?.to_owned();
                let prepared = pipeline::prepare(&a[3..], &state.config)?;
                let parse = millis(started);
                let q = Instant::now();
                let result = query::execute(index, &input, &prepared.options, &state.config, now)?;
                (input, prepared, result, parse, millis(q), started)
            }
        };
        if a.get(2).is_none_or(|a| a.as_ref() != input.as_bytes()) {
            return Err("Prepared search query does not match execution query".into());
        }
        if prepared.format_expand
            && (!index.schema.json || self.command_version() != Version::Resp3)
        {
            return Err("FORMAT EXPAND is supported only with JSON indexes and RESP3".into());
        }
        let p = Instant::now();
        query
            .hits
            .retain(|h| self.search_document_live(index, h.id, now));
        let ids = query.hits.iter().map(|h| h.id).collect::<Vec<_>>();
        let loaded = self.search_load_hits(
            index,
            &ids,
            &prepared.stages,
            prepared.options.dialect,
            now,
            if state.config.enforce_deadline() {
                query.deadline
            } else {
                None
            },
            &state.config,
        )?;
        let mut result = state.config.run(|| {
            pipeline::execute_prepared(
                index,
                &query.hits,
                &prepared,
                loaded,
                &state.config,
                now,
                query.deadline,
            )
        })?;
        if profile && result.cursor.is_some() {
            return Err("FT.PROFILE does not support cursor".into());
        }
        let work = Work {
            parse,
            query: query_time,
            pipeline: millis(p),
            total: millis(started),
            visited: query.visited,
            rows: result.rows.len(),
            plan: query.explanation,
            warning: query.timed_out || !result.warnings.is_empty(),
            branches: Vec::new(),
        };
        let buffered = pipeline_buffers(&prepared.stages)?;
        let total = if prepared.withcount {
            result.total
        } else {
            result
                .cursor
                .as_ref()
                .filter(|_| !buffered)
                .map_or(result.total, |c| result.rows.len().min(c.count))
        };
        if result.cursor.is_some() {
            for row in &mut result.rows {
                row.insert("__search_expand".into(), J::Bool(prepared.format_expand));
                row.insert("__search_cursor_buffered".into(), J::Bool(buffered));
                row.insert(
                    "__search_cursor_withcount".into(),
                    J::Bool(prepared.withcount),
                );
            }
        }
        let mut warnings = result.warnings;
        if query.timed_out {
            warnings.push("Timeout limit was reached".into())
        }
        if query.timeout_capped {
            warnings.push("MaxTimeoutCapped".into())
        }
        let (rows, cursor) =
            cursor_handoff(&mut state, &name, result.rows, total, result.cursor, now)?;
        count_dialect(&mut state, &name, prepared.options.dialect);
        let reply =
            self.search_aggregate_reply(rows, total, cursor, warnings, prepared.format_expand);
        let effects = touch(&mut state, &name, now)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok((reply, work))
    }
    fn search_aggregate_reply(
        &self,
        rows: Vec<Row>,
        total: usize,
        cursor: Option<u64>,
        warnings: Vec<String>,
        expand: bool,
    ) -> Resp {
        let resp3 = self.command_version() == Version::Resp3;
        let rows = rows
            .into_iter()
            .map(|r| {
                let row = public_row(r, resp3, expand);
                if resp3 {
                    named(
                        true,
                        vec![("extra_attributes", row), ("values", array(Vec::new()))],
                    )
                } else {
                    row
                }
            })
            .collect::<Vec<_>>();
        let result = if resp3 {
            named(
                true,
                vec![
                    ("attributes", array(Vec::new())),
                    ("format", text(if expand { "EXPAND" } else { "STRING" })),
                    ("results", array(rows)),
                    ("total_results", integer(total)),
                    ("warning", array(warnings.into_iter().map(text).collect())),
                ],
            )
        } else {
            let mut result = vec![integer(total)];
            result.extend(rows);
            array(result)
        };
        match cursor {
            None => result,
            Some(id) => array(vec![result, Resp::Unsigned(id)]),
        }
    }
    pub(super) fn search_hybrid(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        self.search_hybrid_run(a, now, false).map(|(r, _)| r)
    }
    fn search_hybrid_run(
        &mut self,
        a: &[Bytes],
        now: i64,
        profile: bool,
    ) -> Result<(Resp, Work), String> {
        let plan = self.search_plan.take();
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state
            .resolve(string(a.get(1).ok_or("Missing index")?)?)?
            .to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let cached = plan.map(|p| p.into_prepared(&name)).transpose()?.flatten();
        let dialect = u32::from(state.config.default_dialect).max(2);
        let mut result = match cached {
            Some(PreparedExecution::Hybrid { prepared }) => hybrid::execute_prepared_with_loader(
                index,
                prepared,
                &state.config,
                now,
                |ids, stages, deadline| {
                    self.search_load_hits(index, ids, stages, dialect, now, deadline, &state.config)
                },
            )?,
            Some(_) => {
                return Err("Prepared search command does not match execution command".into());
            }
            None => hybrid::execute_with_loader(
                index,
                &a[2..],
                &state.config,
                now,
                |ids, stages, deadline| {
                    self.search_load_hits(index, ids, stages, dialect, now, deadline, &state.config)
                },
            )?,
        };
        if result.format_expand && (!index.schema.json || self.command_version() != Version::Resp3)
        {
            return Err("FORMAT EXPAND is supported only with JSON indexes and RESP3".into());
        }
        if profile && result.cursor.is_some() {
            return Err("FT.PROFILE does not support cursor".into());
        }
        let profile = result.profile;
        let work = Work {
            parse: profile.parsing_time_ms,
            query: profile.text_time_ms + profile.vector_time_ms,
            pipeline: profile.pipeline_time_ms + profile.fusion_time_ms,
            total: result.execution_time_ms,
            visited: profile.text_candidates + profile.vector_candidates,
            rows: result.rows.len(),
            plan: profile.text_plan,
            warning: !result.warnings.is_empty(),
            branches: vec![
                ("SEARCH", profile.text_time_ms, profile.text_candidates),
                ("VSIM", profile.vector_time_ms, profile.vector_candidates),
                ("COMBINE", profile.fusion_time_ms, profile.fused_candidates),
            ],
        };
        let total = result.total;
        if result.cursor.is_some() {
            for row in &mut result.rows {
                row.insert("__search_hybrid".into(), J::Bool(true));
                row.insert("__search_expand".into(), J::Bool(result.format_expand));
                row.insert("__search_cursor_buffered".into(), J::Bool(true));
            }
        }
        let (rows, cursor) =
            cursor_handoff(&mut state, &name, result.rows, total, result.cursor, now)?;
        count_dialect(&mut state, &name, dialect);
        let reply = self.search_hybrid_reply(
            rows,
            total,
            cursor,
            result.warnings,
            result.execution_time_ms,
            result.format_expand,
        );
        let effects = touch(&mut state, &name, now)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok((reply, work))
    }
    fn search_hybrid_reply(
        &self,
        rows: Vec<Row>,
        total: usize,
        cursor: Option<u64>,
        warnings: Vec<String>,
        elapsed: f64,
        expand: bool,
    ) -> Resp {
        let resp3 = self.command_version() == Version::Resp3;
        let reply = named(
            resp3,
            vec![
                ("total_results", integer(total)),
                (
                    "results",
                    array(
                        rows.into_iter()
                            .map(|r| public_row(r, resp3, expand))
                            .collect(),
                    ),
                ),
                (
                    "warnings",
                    array(
                        warnings
                            .into_iter()
                            .filter(|s| resp3 || s != "MaxTimeoutCapped")
                            .map(text)
                            .collect(),
                    ),
                ),
                ("execution_time", Resp::Double(elapsed)),
            ],
        );
        match cursor {
            None => reply,
            Some(id) => array(vec![reply, Resp::Unsigned(id)]),
        }
    }
    pub(super) fn search_profile(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let (command, limited) = routing::profile_command(a)?;
        let mode = string(&a[2])?.to_ascii_uppercase();
        let (result, profile) = match mode.as_str() {
            "SEARCH" => self.search_query_run(&command, now)?,
            "AGGREGATE" => self.search_aggregate_run(&command, now, true)?,
            "HYBRID" => self.search_hybrid_run(&command, now, true)?,
            _ => return Err("FT.PROFILE supports SEARCH, AGGREGATE or HYBRID".into()),
        };
        let resp3 = self.command_version() == Version::Resp3;
        let profile = profile.reply(resp3, limited);
        Ok(if mode == "HYBRID" {
            match result {
                Resp::Map(mut fields) => {
                    fields.push((text("Profile"), profile));
                    Resp::Map(fields)
                }
                Resp::Array(Some(mut fields)) => {
                    fields.push(profile);
                    array(fields)
                }
                _ => return Err("Invalid hybrid response".into()),
            }
        } else if resp3 {
            named(true, vec![("Results", result), ("Profile", profile)])
        } else {
            array(vec![result, profile])
        })
    }
    pub(super) fn search_explain(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        if a.len() != 3 && a.len() != 5 {
            return Err("wrong number of arguments".into());
        }
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state.resolve(string(&a[1])?)?.to_owned();
        let index = &state.indexes[&name];
        self.search_authorize(index)?;
        let mut options = query::Options {
            dialect: state.config.default_dialect as u32,
            ..Default::default()
        };
        if a.len() == 5 {
            if !a[3].eq_ignore_ascii_case(b"DIALECT") {
                return Err("Unknown EXPLAIN option".into());
            }
            options.dialect = string(&a[4])?.parse().map_err(|_| "Invalid DIALECT")?;
        }
        let plan = query::explain(index, string(&a[2])?, &options, &state.config)?;
        let reply = if a[0].eq_ignore_ascii_case(b"FT.EXPLAINCLI") {
            array(plan.split('\n').map(text).collect())
        } else {
            text(plan)
        };
        let effects = touch(&mut state, &name, now)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok(reply)
    }
    pub(super) fn search_cursor(&mut self, a: &[Bytes], now: i64) -> Result<Resp, String> {
        let started = Instant::now();
        let action = string(a.get(1).ok_or("Missing cursor action")?)?;
        let read = action.eq_ignore_ascii_case("READ");
        if !read && !action.eq_ignore_ascii_case("DEL") {
            return Err("Unknown cursor subcommand".into());
        }
        if (!read && a.len() != 4) || (read && a.len() != 4 && a.len() != 6) {
            return Err("wrong number of arguments".into());
        }
        let id = string(&a[3])?
            .parse::<u64>()
            .ok()
            .filter(|id| *id != 0)
            .ok_or("Bad cursor ID")?;
        let count = if a.len() == 6 {
            if !a[4].eq_ignore_ascii_case(b"COUNT") {
                return Err("Unknown cursor option".into());
            }
            Some(
                string(&a[5])?
                    .parse::<usize>()
                    .ok()
                    .filter(|n| *n > 0)
                    .ok_or("Cursor COUNT must be positive")?,
            )
        } else {
            None
        };
        let catalog = self.search_catalog();
        let mut state = catalog.lock();
        let name = state.resolve(string(&a[2])?)?.to_owned();
        self.search_authorize(&state.indexes[&name])?;
        let cursor = state.cursors.get(&id).ok_or("Cursor does not exist")?;
        if cursor.index != name || cursor.deadline <= now {
            return Err("Cursor does not exist".into());
        }
        if !read {
            state.cursor_remove(id);
            let effects = touch(&mut state, &name, now)?;
            drop(state);
            self.search_record_effects(effects, now);
            return Ok(Resp::Simple("OK"));
        }
        let mut cursor = state.cursor_remove(id).ok_or("Cursor does not exist")?;
        if let Some(n) = count {
            cursor.count = n
        }
        let deadline = state.config.deadline(started, state.config.timeout_ms);
        let mut warnings = Vec::new();
        if state.config.timeout_capped(state.config.timeout_ms) {
            warnings.push("MaxTimeoutCapped".into())
        }
        let flag = |key: &str| {
            cursor
                .rows
                .front()
                .and_then(|r| r.get(key))
                .and_then(J::as_bool)
                .unwrap_or(false)
        };
        let hybrid = flag("__search_hybrid");
        let expand = flag("__search_expand");
        let buffered = flag("__search_cursor_buffered");
        let withcount = flag("__search_cursor_withcount");
        let mut rows = Vec::new();
        while rows.len() < cursor.count {
            if super::runtime::expired(deadline) {
                if state.config.on_timeout_fail {
                    return Err("Timeout limit was reached".into());
                }
                warnings.push("Timeout limit was reached".into());
                break;
            }
            let Some(row) = cursor.rows.pop_front() else {
                break;
            };
            if let Some(id) = row.get("__search_doc_id").and_then(J::as_u64)
                && !self.search_document_live(&state.indexes[&name], id, now)
            {
                continue;
            }
            rows.push(row);
        }
        let total = if withcount {
            cursor.total
        } else if buffered {
            0
        } else {
            rows.len()
        };
        let next = if cursor.rows.is_empty() {
            0
        } else {
            cursor.deadline = now.saturating_add(cursor.max_idle.min(i64::MAX as u64) as i64);
            state.cursor_insert(id, cursor);
            id
        };
        let reply = if hybrid {
            self.search_hybrid_reply(rows, total, Some(next), warnings, millis(started), expand)
        } else {
            self.search_aggregate_reply(rows, total, Some(next), warnings, expand)
        };
        let effects = touch(&mut state, &name, now)?;
        drop(state);
        self.search_record_effects(effects, now);
        Ok(reply)
    }
}
fn summary_windows(
    input: &str,
    matches: &[(usize, usize)],
    words: usize,
    fragments: usize,
) -> Vec<(usize, usize)> {
    let mut bounds = Vec::new();
    let mut start = None;
    for (i, c) in input.char_indices() {
        if c.is_whitespace() {
            if let Some(s) = start.take() {
                bounds.push((s, i))
            }
        } else if start.is_none() {
            start = Some(i)
        }
    }
    if let Some(s) = start {
        bounds.push((s, input.len()))
    }
    if bounds.is_empty() {
        return vec![(0, input.len())];
    }
    let mut windows: Vec<(usize, usize)> = Vec::new();
    for &(at, _) in matches {
        let word = bounds.partition_point(|(_, end)| *end <= at);
        let lo = word.saturating_sub(words / 2);
        let hi = lo.saturating_add(words).min(bounds.len());
        let window = (
            bounds[lo.min(bounds.len() - 1)].0,
            bounds[hi.saturating_sub(1)].1,
        );
        if let Some(last) = windows.last_mut()
            && window.0 <= last.1
        {
            last.1 = last.1.max(window.1);
            continue;
        }
        if windows.len() >= fragments {
            break;
        }
        windows.push(window)
    }
    if windows.is_empty() {
        windows.push((0, bounds[words.min(bounds.len()) - 1].1))
    }
    windows
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn aggregate_numbers_are_canonical_strings_unless_expanded() {
        for resp3 in [false, true] {
            for (value, encoded) in [(2.0, "2"), (1970.0, "1970"), (2.5, "2.5")] {
                let number = J::from(value);
                assert!(
                    matches!(row_value(&number,resp3,false),Resp::Bulk(Some(bytes))if bytes.as_ref()==encoded.as_bytes())
                );
            }
        }
        assert!(matches!(json_reply(&J::from(2.0),true),Resp::Double(value)if value==2.0));
    }
}
