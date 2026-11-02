use super::{
    model::*,
    pipeline::{self, CursorOptions},
    query, vector,
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

pub(super) struct HybridResult {
    pub rows: Vec<Row>,
    pub total: usize,
    pub cursor: Option<CursorOptions>,
    pub execution_time_ms: f64,
    pub warnings: Vec<String>,
    pub format_expand: bool,
    pub profile: HybridProfile,
}
pub(super) struct HybridProfile {
    pub parsing_time_ms: f64,
    pub text_time_ms: f64,
    pub vector_time_ms: f64,
    pub fusion_time_ms: f64,
    pub pipeline_time_ms: f64,
    pub text_candidates: usize,
    pub vector_candidates: usize,
    pub fused_candidates: usize,
    pub text_plan: String,
}

/// Owns the actual branch results and fused rows until source loading can finish.
/// Candidate IDs are the exact rows offered to the source loader, before pipeline stages.
pub(super) struct PreparedHybrid {
    started: Instant,
    parsed: Parsed,
    text_result: QueryResult,
    rows: Vec<(DocId, Row)>,
    explanations: BTreeMap<DocId, Value>,
    available: BTreeSet<String>,
    ids: Vec<DocId>,
    filter_timed_out: bool,
    parsing_time_ms: f64,
    text_time_ms: f64,
    vector_time_ms: f64,
    fusion_time_ms: f64,
    vector_candidates: usize,
    fused_candidates: usize,
}
impl PreparedHybrid {
    pub(super) fn candidate_ids(&self) -> &[DocId] {
        &self.ids
    }
}
fn arg(args: &[Bytes], p: usize) -> Result<&str, String> {
    args.get(p)
        .ok_or_else(|| "Missing argument".to_string())
        .and_then(pipeline::text)
}
fn integer(args: &[Bytes], p: usize, positive: bool) -> Result<usize, String> {
    let n = arg(args, p)?
        .parse::<usize>()
        .map_err(|_| "Invalid argument count: expected an unsigned integer".to_string())?;
    if positive && n == 0 {
        return Err("Expected a positive integer".into());
    }
    Ok(n)
}
fn real(s: &str) -> Result<f64, String> {
    let n = s
        .parse::<f64>()
        .map_err(|_| "Expected a number".to_string())?;
    if !n.is_finite() {
        return Err("Expected a finite number".into());
    }
    Ok(n)
}
fn block(args: &[Bytes], p: usize, positive: bool) -> Result<(usize, usize), String> {
    let n = integer(args, p, positive)?;
    if n % 2 != 0 {
        return Err("Option argument count must be even".into());
    }
    let start = p + 1;
    let end = start
        .checked_add(n)
        .filter(|e| *e <= args.len())
        .ok_or_else(|| format!("Expected arguments {n}, but fewer were provided"))?;
    Ok((start, end))
}
fn alias(slot: &mut Option<String>, value: &str) -> Result<(), String> {
    if slot.is_some() {
        return Err("YIELD_SCORE_AS specified more than once".into());
    }
    if value.is_empty() || value.starts_with('@') {
        return Err("Invalid score alias".into());
    }
    *slot = Some(value.to_owned());
    Ok(())
}
fn no_vectors(query: &str) -> Result<(), String> {
    let bytes = query.as_bytes();
    let (mut p, mut quote) = (0, None);
    while p < bytes.len() {
        let c = bytes[p];
        p += 1;
        if c == b'\\' {
            p = (p + 1).min(bytes.len());
            continue;
        }
        if let Some(q) = quote {
            if c == q {
                quote = None
            }
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
            continue;
        }
        if c == b'[' {
            let rest = query[p..].trim_start();
            let head = rest.split_whitespace().next().unwrap_or("");
            if head.eq_ignore_ascii_case("KNN") || head.eq_ignore_ascii_case("VECTOR_RANGE") {
                return Err(
                    "Vector expressions are not allowed in FT.HYBRID SEARCH or FILTER".into(),
                );
            }
        }
    }
    Ok(())
}
#[derive(Clone, Copy)]
enum Method {
    Rrf,
    Linear,
}
struct Parsed {
    text: String,
    field: String,
    parameter: String,
    options: query::Options,
    k: usize,
    radius: Option<f64>,
    runtime: BTreeMap<String, String>,
    filter: Option<String>,
    method: Method,
    constant: f64,
    window: usize,
    alpha: f64,
    beta: f64,
    text_alias: Option<String>,
    vector_alias: Option<String>,
    combined_alias: Option<String>,
    stages: Vec<Bytes>,
    nosort: bool,
    explain: bool,
    format_expand: bool,
}
fn parse(args: &[Bytes], config: &Config) -> Result<Parsed, String> {
    if !arg(args, 0)?.eq_ignore_ascii_case("SEARCH") {
        return Err("FT.HYBRID requires SEARCH".into());
    }
    let text = arg(args, 1)?.to_owned();
    no_vectors(&text)?;
    let mut options = query::Options {
        dialect: config.default_dialect.max(2) as u32,
        scorer: "BM25STD".into(),
        timeout_ms: config.timeout_ms,
        ..Default::default()
    };
    let mut p = 2;
    let mut text_alias = None;
    let mut seen_scorer = false;
    loop {
        match arg(args, p)?.to_ascii_uppercase().as_str() {
            "SCORER" => {
                if seen_scorer {
                    return Err("Duplicate SCORER".into());
                }
                seen_scorer = true;
                options.scorer = arg(args, p + 1)?.to_owned();
                p += 2
            }
            "YIELD_SCORE_AS" => {
                alias(&mut text_alias, arg(args, p + 1)?)?;
                p += 2
            }
            "DIALECT" => {
                return Err(
                    "DIALECT is not supported in FT.HYBRID or any of its subqueries".into(),
                );
            }
            "VSIM" => break,
            _ => return Err("Expected VSIM after SEARCH clause".into()),
        }
    }
    let field = arg(args, p + 1)?
        .strip_prefix('@')
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "VSIM vector field must begin with @".to_string())?
        .to_owned();
    let parameter = arg(args, p + 2)?
        .strip_prefix('$')
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "VSIM vector argument must be a $parameter".to_string())?
        .to_owned();
    p += 3;
    let mut parsed = Parsed {
        text,
        field,
        parameter,
        options,
        k: 10,
        radius: None,
        runtime: BTreeMap::new(),
        filter: None,
        method: Method::Rrf,
        constant: 60.,
        window: 20,
        alpha: 0.3,
        beta: 0.7,
        text_alias,
        vector_alias: None,
        combined_alias: None,
        stages: Vec::new(),
        nosort: false,
        explain: false,
        format_expand: false,
    };
    let mut vector_mode = false;
    let mut combined = false;
    let mut post = false;
    let mut params_seen = false;
    let mut sort_seen = false;
    let mut limit_seen = false;
    let mut timeout_seen = false;
    let mut format_seen = false;
    while p < args.len() {
        let key = arg(args, p)?.to_ascii_uppercase();
        match key.as_str() {
            "DIALECT" => {
                return Err(
                    "DIALECT is not supported in FT.HYBRID or any of its subqueries".into(),
                );
            }
            "KNN" | "RANGE" if !post && !combined => {
                if vector_mode {
                    return Err("Only one KNN or RANGE clause is allowed".into());
                }
                vector_mode = true;
                let (start, e) = block(args, p + 1, true)?;
                let mut seen = BTreeSet::new();
                for pair in args[start..e].chunks(2) {
                    let name = pipeline::text(&pair[0])?.to_ascii_uppercase();
                    let value = pipeline::text(&pair[1])?;
                    if !seen.insert(name.clone()) {
                        return Err(format!("Duplicate {name}"));
                    }
                    match name.as_str() {
                        "K" if key == "KNN" => {
                            parsed.k = value
                                .parse::<usize>()
                                .ok()
                                .filter(|k| *k > 0)
                                .ok_or_else(|| "Invalid K value".to_string())?
                        }
                        "RADIUS" if key == "RANGE" => {
                            let n = real(value)?;
                            if n < 0. {
                                return Err("Invalid RADIUS value".into());
                            }
                            parsed.radius = Some(n)
                        }
                        "EF_RUNTIME" if key == "KNN" => {
                            if value.parse::<usize>().ok().filter(|n| *n > 0).is_none() {
                                return Err("Invalid EF_RUNTIME value".into());
                            }
                            parsed.runtime.insert(name, value.into());
                        }
                        "SHARD_K_RATIO" if key == "KNN" => {
                            if real(value)? <= 0. {
                                return Err("Invalid SHARD_K_RATIO value".into());
                            }
                            parsed.runtime.insert(name, value.into());
                        }
                        "EPSILON" if key == "RANGE" => {
                            if real(value)? <= 0. {
                                return Err("Invalid EPSILON value".into());
                            }
                            parsed.runtime.insert(name, value.into());
                        }
                        "YIELD_SCORE_AS" => alias(&mut parsed.vector_alias, value)?,
                        _ => return Err(format!("Unknown argument `{name}` in {key}")),
                    }
                }
                if key == "KNN" && !seen.contains("K") {
                    return Err("Missing required argument K".into());
                }
                if key == "RANGE" && !seen.contains("RADIUS") {
                    return Err("Missing required argument RADIUS".into());
                }
                p = e;
            }
            "YIELD_SCORE_AS" if !post => {
                if combined {
                    alias(&mut parsed.combined_alias, arg(args, p + 1)?)?
                } else {
                    alias(&mut parsed.vector_alias, arg(args, p + 1)?)?
                }
                p += 2;
            }
            "FILTER" if !post && !combined => {
                if parsed.filter.is_some() {
                    return Err("Duplicate VSIM FILTER".into());
                }
                let first = arg(args, p + 1)?;
                if let Ok(n) = first.parse::<usize>() {
                    if n == 0 || n % 2 == 0 {
                        return Err("FILTER count must include a query and option pairs".into());
                    }
                    let start = p + 2;
                    let e = start
                        .checked_add(n)
                        .filter(|e| *e <= args.len())
                        .ok_or_else(|| {
                            "FILTER argument count exceeds available arguments".to_string()
                        })?;
                    let filter = arg(args, start)?.to_owned();
                    no_vectors(&filter)?;
                    parsed.filter = Some(filter);
                    let mut seen = BTreeSet::new();
                    for pair in args[start + 1..e].chunks(2) {
                        let key = pipeline::text(&pair[0])?.to_ascii_uppercase();
                        let val = pipeline::text(&pair[1])?;
                        if !seen.insert(key.clone()) {
                            return Err("Duplicate FILTER option".into());
                        }
                        match key.as_str() {
                            "POLICY" => {
                                let policy = match val.to_ascii_uppercase().as_str() {
                                    "ADHOC" => "ADHOC_BF",
                                    "BATCHES" => "BATCHES",
                                    _ => return Err("Invalid HYBRID filter policy".into()),
                                };
                                parsed.runtime.insert("HYBRID_POLICY".into(), policy.into());
                            }
                            "BATCH_SIZE" => {
                                if val.parse::<usize>().ok().filter(|n| *n > 0).is_none() {
                                    return Err("Invalid BATCH_SIZE".into());
                                }
                                parsed.runtime.insert(key, val.into());
                            }
                            _ => return Err(format!("Unknown FILTER option {key}")),
                        }
                    }
                    p = e;
                } else {
                    no_vectors(first)?;
                    parsed.filter = Some(first.to_owned());
                    p += 2;
                }
            }
            "COMBINE" if !post => {
                if combined {
                    return Err("Duplicate COMBINE".into());
                }
                combined = true;
                parsed.method = match arg(args, p + 1)?.to_ascii_uppercase().as_str() {
                    "RRF" => Method::Rrf,
                    "LINEAR" => Method::Linear,
                    _ => return Err("Unknown COMBINE method".into()),
                };
                let (start, e) = block(args, p + 2, false)?;
                let mut seen = BTreeSet::new();
                for pair in args[start..e].chunks(2) {
                    let key = pipeline::text(&pair[0])?.to_ascii_uppercase();
                    let value = pipeline::text(&pair[1])?;
                    if !seen.insert(key.clone()) {
                        return Err(format!("Duplicate COMBINE option {key}"));
                    }
                    match key.as_str() {
                        "CONSTANT" if matches!(parsed.method, Method::Rrf) => {
                            parsed.constant = real(value)?;
                            if parsed.constant < 0. {
                                return Err("CONSTANT must not be negative".into());
                            }
                        }
                        "ALPHA" if matches!(parsed.method, Method::Linear) => {
                            parsed.alpha = real(value)?;
                            if parsed.alpha < 0. {
                                return Err("ALPHA must not be negative".into());
                            }
                        }
                        "BETA" if matches!(parsed.method, Method::Linear) => {
                            parsed.beta = real(value)?;
                            if parsed.beta < 0. {
                                return Err("BETA must not be negative".into());
                            }
                        }
                        "WINDOW" => {
                            parsed.window = value
                                .parse::<usize>()
                                .ok()
                                .filter(|n| *n > 0)
                                .ok_or_else(|| "Invalid WINDOW value".to_string())?
                        }
                        "YIELD_SCORE_AS" => alias(&mut parsed.combined_alias, value)?,
                        _ => return Err(format!("Unknown COMBINE option {key}")),
                    }
                }
                if seen.contains("ALPHA") != seen.contains("BETA") {
                    return Err(if seen.contains("ALPHA") {
                        "Missing value for BETA"
                    } else {
                        "Missing value for ALPHA"
                    }
                    .into());
                }
                p = e;
            }
            "PARAMS" => {
                if params_seen {
                    return Err("Duplicate PARAMS".into());
                }
                params_seen = true;
                let (start, e) = block(args, p + 1, false)?;
                for pair in args[start..e].chunks(2) {
                    let name = pipeline::text(&pair[0])?.to_owned();
                    if parsed
                        .options
                        .params
                        .insert(name, pair[1].to_vec())
                        .is_some()
                    {
                        return Err("Duplicate parameter".into());
                    }
                }
                p = e;
            }
            "TIMEOUT" => {
                if timeout_seen {
                    return Err("Duplicate TIMEOUT".into());
                }
                timeout_seen = true;
                parsed.options.timeout_ms = integer(args, p + 1, false)? as u64;
                p += 2;
            }
            "FORMAT" => {
                if format_seen {
                    return Err("Duplicate FORMAT".into());
                }
                format_seen = true;
                parsed.format_expand = match arg(args, p + 1)?.to_ascii_uppercase().as_str() {
                    "EXPAND" => true,
                    "STRING" => false,
                    _ => return Err("FORMAT must be STRING or EXPAND".into()),
                };
                p += 2;
            }
            "EXPLAINSCORE" => {
                if parsed.explain {
                    return Err("Duplicate EXPLAINSCORE".into());
                }
                parsed.explain = true;
                parsed.options.explain_score = true;
                p += 1;
            }
            "NOSORT" => {
                if sort_seen || parsed.nosort {
                    return Err("NOSORT and SORTBY are mutually exclusive".into());
                }
                parsed.nosort = true;
                post = true;
                p += 1;
            }
            "LOAD" | "GROUPBY" | "REDUCE" | "APPLY" | "FILTER" | "SORTBY" | "LIMIT"
            | "WITHCURSOR" => {
                post = true;
                if key == "SORTBY" {
                    if parsed.nosort {
                        return Err("NOSORT and SORTBY are mutually exclusive".into());
                    }
                    sort_seen = true;
                }
                if key == "LIMIT" {
                    limit_seen = true;
                }
                let e = pipeline::stage_end(args, p)?;
                parsed.stages.extend_from_slice(&args[p..e]);
                p = e;
            }
            _ => return Err(format!("Unknown FT.HYBRID argument {key}")),
        }
    }
    validate_stage_fields(&parsed.stages)?;
    if !parsed.options.params.contains_key(&parsed.parameter) {
        return Err(format!("No such parameter {}", parsed.parameter));
    }
    if parsed.explain {
        let mut p = 0;
        while p < parsed.stages.len() {
            if parsed.stages[p].eq_ignore_ascii_case(b"GROUPBY") {
                return Err("EXPLAINSCORE is not supported with GROUPBY".into());
            }
            p = pipeline::stage_end(&parsed.stages, p)?;
        }
    }
    // Insert implicit ordering before the first pagination stage, never after it.
    if !sort_seen && !parsed.nosort {
        let mut pos = 0;
        let mut grouped = false;
        while pos < parsed.stages.len() {
            if parsed.stages[pos].eq_ignore_ascii_case(b"GROUPBY") {
                grouped = true
            }
            if parsed.stages[pos].eq_ignore_ascii_case(b"LIMIT") {
                break;
            }
            pos = pipeline::stage_end(&parsed.stages, pos)?;
        }
        if !grouped {
            parsed.stages.splice(
                pos..pos,
                [
                    Bytes::from_static(b"SORTBY"),
                    Bytes::from_static(b"2"),
                    Bytes::from_static(b"@__score"),
                    Bytes::from_static(b"DESC"),
                ],
            );
        }
    }
    if !limit_seen {
        parsed.stages.extend([
            Bytes::from_static(b"LIMIT"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"10"),
        ]);
    }
    Ok(parsed)
}
fn score(metric: &str, distance: f64) -> f64 {
    match metric {
        "L2" => 1. / (1. + distance),
        "COSINE" => (2. - distance) / 2.,
        "IP" => (1. + distance) / 2.,
        _ => f64::NAN,
    }
}
fn prefixed(field: &str, stage: &str) -> Result<(), String> {
    if !field.starts_with('@') && !field.starts_with('$') {
        return Err(format!(
            "Missing prefix: name requires '@' prefix, JSON path require '$' prefix, got: {field} in {stage}"
        ));
    }
    Ok(())
}
fn validate_stage_fields(args: &[Bytes]) -> Result<(), String> {
    let mut p = 0;
    while p < args.len() {
        let stage = arg(args, p)?.to_ascii_uppercase();
        let e = pipeline::stage_end(args, p)?;
        match stage.as_str() {
            "LOAD" if arg(args, p + 1)? != "*" => {
                let mut q = p + 2;
                while q < e {
                    prefixed(arg(args, q)?, &stage)?;
                    q += 1;
                    if q < e && arg(args, q)?.eq_ignore_ascii_case("AS") {
                        q += 2;
                        if q > e {
                            return Err("LOAD alias exceeds argument count".into());
                        }
                    }
                }
            }
            "GROUPBY" => {
                for q in p + 2..e {
                    prefixed(arg(args, q)?, &stage)?
                }
            }
            "SORTBY" => {
                let n = integer(args, p + 1, false)?;
                let mut q = p + 2;
                while q < p + 2 + n {
                    prefixed(arg(args, q)?, &stage)?;
                    q += 1;
                    if q < p + 2 + n
                        && matches!(arg(args, q)?.to_ascii_uppercase().as_str(), "ASC" | "DESC")
                    {
                        q += 1
                    }
                }
            }
            "REDUCE" => {
                let name = arg(args, p + 1)?.to_ascii_uppercase();
                let n = integer(args, p + 2, false)?;
                let end = p + 3 + n;
                if name == "COLLECT" {
                    let mut q = p + 3;
                    while q < end {
                        let option = arg(args, q)?.to_ascii_uppercase();
                        match option.as_str() {
                            "FIELDS" => {
                                if arg(args, q + 1)? == "*" {
                                    q += 2
                                } else {
                                    let count = integer(args, q + 1, false)?;
                                    let stop = (q + 2)
                                        .checked_add(count)
                                        .filter(|stop| *stop <= end)
                                        .ok_or_else(|| {
                                            "COLLECT field count exceeds available arguments"
                                                .to_string()
                                        })?;
                                    for f in q + 2..stop {
                                        prefixed(arg(args, f)?, &name)?
                                    }
                                    q = stop
                                }
                            }
                            "SORTBY" => {
                                let count = integer(args, q + 1, false)?;
                                let stop = (q + 2)
                                    .checked_add(count)
                                    .filter(|stop| *stop <= end)
                                    .ok_or_else(|| {
                                    "COLLECT sort count exceeds available arguments".to_string()
                                })?;
                                q += 2;
                                while q < stop {
                                    prefixed(arg(args, q)?, &name)?;
                                    q += 1;
                                    if q < stop
                                        && matches!(
                                            arg(args, q)?.to_ascii_uppercase().as_str(),
                                            "ASC" | "DESC"
                                        )
                                    {
                                        q += 1
                                    }
                                }
                            }
                            "LIMIT" => {
                                q += 3;
                                if q > end {
                                    return Err("Missing COLLECT LIMIT argument".into());
                                }
                            }
                            "DISTINCT" => q += 1,
                            _ => return Err(format!("Unknown COLLECT option {option}")),
                        }
                    }
                } else if n > 0 {
                    prefixed(arg(args, p + 3)?, &name)?;
                    if name == "FIRST_VALUE" && n > 1 {
                        prefixed(arg(args, p + 5)?, &name)?
                    }
                }
            }
            _ => {}
        }
        p = e;
    }
    Ok(())
}
#[derive(Default)]
struct Fusion {
    text: Option<(usize, f64)>,
    vector: Option<(usize, f64, f64)>,
}
pub(super) fn execute_with_loader(
    index: &Index,
    args: &[Bytes],
    config: &Config,
    now: i64,
    loader: impl FnMut(&[DocId], &[Bytes], Option<Instant>) -> Result<BTreeMap<DocId, Row>, String>,
) -> Result<HybridResult, String> {
    let prepared = prepare(index, args, config, now)?;
    execute_prepared_with_loader(index, prepared, config, now, loader)
}

pub(super) fn prepare(
    index: &Index,
    args: &[Bytes],
    config: &Config,
    now: i64,
) -> Result<PreparedHybrid, String> {
    config.run(|| prepare_inner(index, args, config, now))
}
fn prepare_inner(
    index: &Index,
    args: &[Bytes],
    config: &Config,
    now: i64,
) -> Result<PreparedHybrid, String> {
    let started = Instant::now();
    let parsed = parse(args, config)?;
    let parsing_time_ms = started.elapsed().as_secs_f64() * 1000.;
    let deadline = config.deadline(Instant::now(), parsed.options.timeout_ms);
    let _deadline_guard = super::runtime::DeadlineGuard::enter(if config.enforce_deadline() {
        deadline
    } else {
        None
    });
    let field = index
        .schema
        .fields
        .iter()
        .find(|f| f.name == parsed.field)
        .ok_or_else(|| "Unknown vector field".to_string())?;
    let vector_config = field
        .vector
        .as_ref()
        .ok_or_else(|| "VSIM requires a VECTOR field".to_string())?;
    let FieldIndex::Vector(vector_index) = index
        .fields
        .get(&parsed.field)
        .ok_or_else(|| "Vector field is not indexed".to_string())?
    else {
        return Err("VSIM requires a VECTOR field".into());
    };
    let blob = parsed
        .options
        .params
        .get(&parsed.parameter)
        .ok_or_else(|| "Missing vector parameter".to_string())?;
    let q = vector::decode(vector_config, blob)?;
    let text_started = Instant::now();
    let mut text_result = query::execute(index, &parsed.text, &parsed.options, config, now)?;
    text_result.deadline = deadline;
    let text_time_ms = text_started.elapsed().as_secs_f64() * 1000.;
    let vector_started = Instant::now();
    let mut filter_timed_out = false;
    let filter = if let Some(filter) = &parsed.filter {
        let result = query::execute(index, filter, &parsed.options, config, now)?;
        filter_timed_out = result.timed_out;
        Some(result.hits.into_iter().map(|h| h.id).collect::<DocSet>())
    } else {
        None
    };
    let mut vectors = if let Some(radius) = parsed.radius {
        vector_index.range(&q, radius, filter.as_ref(), &parsed.runtime)?
    } else {
        vector_index.query(&q, parsed.k, filter.as_ref(), &parsed.runtime)?
    };
    vectors.retain(|(id, _)| {
        index.docs.get(id).is_some_and(|d| {
            !d.expires_at.is_some_and(|t| t <= now) && !d.field_deadline.is_some_and(|t| t <= now)
        })
    });
    vectors.sort_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
    let vector_time_ms = vector_started.elapsed().as_secs_f64() * 1000.;
    let fusion_started = Instant::now();
    let mut union: BTreeMap<DocId, Fusion> = BTreeMap::new();
    for (rank, hit) in text_result.hits.iter().take(parsed.window).enumerate() {
        union.entry(hit.id).or_default().text = Some((rank + 1, hit.score));
    }
    for (rank, (id, distance)) in vectors.iter().take(parsed.window).enumerate() {
        union.entry(*id).or_default().vector = Some((
            rank + 1,
            score(&vector_config.distance_metric, *distance),
            *distance,
        ));
    }
    let mut rows = Vec::with_capacity(union.len());
    let mut explanations = BTreeMap::new();
    for (id, fusion) in union {
        if config.enforce_deadline() && super::runtime::expired(deadline) {
            filter_timed_out = true;
            if config.on_timeout_fail {
                return Err("Timeout limit was reached".into());
            }
            break;
        }
        let Some(doc) = index.docs.get(&id) else {
            continue;
        };
        let text_contribution = match (parsed.method, fusion.text) {
            (Method::Rrf, Some((rank, _))) => 1. / (parsed.constant + rank as f64),
            (Method::Linear, Some((_, value))) => parsed.alpha * value,
            (_, None) => 0.,
        };
        let vector_contribution = match (parsed.method, fusion.vector) {
            (Method::Rrf, Some((rank, _, _))) => 1. / (parsed.constant + rank as f64),
            (Method::Linear, Some((_, value, _))) => parsed.beta * value,
            (_, None) => 0.,
        };
        let combined = text_contribution + vector_contribution;
        let mut row = Row::new();
        row.insert("__key".into(), super::value::from_bytes(&doc.key));
        row.insert("__score".into(), json!(combined));
        if let Some(alias) = &parsed.text_alias
            && let Some((_, value)) = fusion.text
        {
            row.insert(alias.clone(), json!(value));
        }
        if let Some(alias) = &parsed.vector_alias
            && let Some((_, value, _)) = fusion.vector
        {
            row.insert(alias.clone(), json!(value));
        }
        if let Some(alias) = &parsed.combined_alias {
            row.insert(alias.clone(), json!(combined));
        }
        if parsed.explain {
            let text = if let Some((rank, value)) = fusion.text {
                let detail = text_result
                    .score_explanations
                    .get(&id)
                    .ok_or_else(|| "Missing text score explanation".to_string())?;
                json!([
                    format!(
                        "SEARCH scorer {} rank {rank}, score {value}, contribution {text_contribution}",
                        parsed.options.scorer
                    ),
                    [detail]
                ])
            } else {
                json!(["SEARCH <no match>", []])
            };
            let vector = if let Some((rank, value, distance)) = fusion.vector {
                let mode = if let Some(radius) = parsed.radius {
                    let epsilon = parsed
                        .runtime
                        .get("EPSILON")
                        .and_then(|s| s.parse::<f64>().ok())
                        .unwrap_or(vector_config.epsilon);
                    format!(
                        "RANGE radius {radius}, epsilon {epsilon}, matched within radius: {}",
                        distance <= radius
                    )
                } else {
                    format!("KNN K {}", parsed.k)
                };
                json!([
                    format!(
                        "VSIM {mode}, rank {rank}, distance {distance}, normalized score {value}, contribution {vector_contribution}"
                    ),
                    []
                ])
            } else {
                json!(["VSIM <no match>", []])
            };
            let formula = match parsed.method {
                Method::Rrf => format!(
                    "final score: RRF window {} constant {}: {} + {} = {}",
                    parsed.window,
                    parsed.constant,
                    text_contribution,
                    vector_contribution,
                    combined
                ),
                Method::Linear => format!(
                    "final score: LINEAR window {} alpha {} beta {}: {} + {} = {}",
                    parsed.window,
                    parsed.alpha,
                    parsed.beta,
                    text_contribution,
                    vector_contribution,
                    combined
                ),
            };
            explanations.insert(id, json!([combined, [formula, [text, vector]]]));
        }
        rows.push((id, row));
    }
    let fused_candidates = rows.len();
    let fusion_time_ms = fusion_started.elapsed().as_secs_f64() * 1000.;
    let mut available: BTreeSet<String> = ["__key".to_owned(), "__score".to_owned()]
        .into_iter()
        .collect();
    available.extend(
        [
            parsed.text_alias.clone(),
            parsed.vector_alias.clone(),
            parsed.combined_alias.clone(),
        ]
        .into_iter()
        .flatten(),
    );
    let matched: BTreeMap<DocId, &Vec<String>> = text_result
        .hits
        .iter()
        .map(|h| (h.id, &h.matched_terms))
        .collect();
    for (id, row) in &mut rows {
        if let Some(terms) = matched.get(id) {
            row.insert(
                "__matched_terms".into(),
                Value::Array(terms.iter().map(|s| Value::String(s.clone())).collect()),
            );
        }
    }
    let ids = rows.iter().map(|(id, _)| *id).collect();
    Ok(PreparedHybrid {
        started,
        parsed,
        text_result,
        rows,
        explanations,
        available,
        ids,
        filter_timed_out,
        parsing_time_ms,
        text_time_ms,
        vector_time_ms,
        fusion_time_ms,
        vector_candidates: vectors.len(),
        fused_candidates,
    })
}

pub(super) fn execute_prepared_with_loader(
    index: &Index,
    prepared: PreparedHybrid,
    config: &Config,
    _now: i64,
    mut loader: impl FnMut(&[DocId], &[Bytes], Option<Instant>) -> Result<BTreeMap<DocId, Row>, String>,
) -> Result<HybridResult, String> {
    let PreparedHybrid {
        started,
        parsed,
        text_result,
        mut rows,
        explanations,
        available,
        ids,
        filter_timed_out,
        parsing_time_ms,
        text_time_ms,
        vector_time_ms,
        fusion_time_ms,
        vector_candidates,
        fused_candidates,
    } = prepared;
    let pipeline_started = Instant::now();
    let loaded = loader(
        &ids,
        &parsed.stages,
        if config.enforce_deadline() {
            text_result.deadline
        } else {
            None
        },
    )?;
    rows.retain(|(id, _)| loaded.contains_key(id));
    let result = config.run(|| {
        pipeline::execute_rows(
            index,
            rows,
            &parsed.stages,
            loaded,
            config,
            available,
            parsed.options.timeout_ms,
            text_result.deadline,
        )
    })?;
    let pipeline_time_ms = pipeline_started.elapsed().as_secs_f64() * 1000.;
    let mut final_rows = result.rows;
    if parsed.explain {
        for row in &mut final_rows {
            if let Some(id) = row.get("__search_doc_id").and_then(Value::as_u64)
                && let Some(explanation) = explanations.get(&id)
            {
                row.insert("score".into(), explanation.clone());
            }
        }
    }
    let execution_time_ms = started.elapsed().as_secs_f64() * 1000.;
    let timed_out =
        text_result.timed_out || filter_timed_out || super::runtime::expired(text_result.deadline);
    if timed_out && config.on_timeout_fail {
        return Err("Timeout limit was reached".into());
    }
    let mut warnings = result.warnings;
    if timed_out && !warnings.iter().any(|s| s == "Timeout limit was reached") {
        warnings.push("Timeout limit was reached".into());
    }
    if text_result.timeout_capped {
        warnings.push("MaxTimeoutCapped".into())
    }
    Ok(HybridResult {
        rows: final_rows,
        total: result.total,
        cursor: result.cursor,
        execution_time_ms,
        warnings,
        format_expand: parsed.format_expand,
        profile: HybridProfile {
            parsing_time_ms,
            text_time_ms,
            vector_time_ms,
            fusion_time_ms,
            pipeline_time_ms,
            text_candidates: text_result.hits.len(),
            vector_candidates,
            fused_candidates,
            text_plan: text_result.explanation,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(parts: &[&str]) -> Vec<Bytes> {
        parts
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }
    fn command(extra: &[&str]) -> Vec<Bytes> {
        let mut a = args(&["SEARCH", "shoes", "VSIM", "@embedding", "$blob"]);
        a.extend(args(extra));
        a.extend(args(&["PARAMS", "2", "blob"]));
        a.push(Bytes::from_static(&[0, 255, 0, 128]));
        a
    }
    #[test]
    fn binary_parameters_and_default_fusion_are_preserved() {
        let parsed = parse(&command(&[]), &Config::default()).unwrap();
        assert_eq!(parsed.options.params["blob"], vec![0, 255, 0, 128]);
        assert_eq!(parsed.options.dialect, 2);
        assert_eq!(parsed.k, 10);
        assert_eq!(parsed.window, 20);
        assert_eq!(parsed.constant, 60.);
        assert_eq!(
            parsed.stages,
            args(&["SORTBY", "2", "@__score", "DESC", "LIMIT", "0", "10"])
        );
    }
    #[test]
    fn branch_filter_and_post_filter_have_distinct_grammars() {
        let parsed = parse(
            &command(&[
                "KNN",
                "2",
                "K",
                "3",
                "FILTER",
                "3",
                "@price:[0 50]",
                "POLICY",
                "ADHOC",
                "COMBINE",
                "LINEAR",
                "4",
                "ALPHA",
                "2",
                "BETA",
                "3",
                "LOAD",
                "1",
                "@price",
                "FILTER",
                "@price > 4",
            ]),
            &Config::default(),
        )
        .unwrap();
        assert_eq!(parsed.filter.as_deref(), Some("@price:[0 50]"));
        assert_eq!(parsed.runtime["HYBRID_POLICY"], "ADHOC_BF");
        assert_eq!(parsed.alpha, 2.);
        assert_eq!(parsed.beta, 3.);
    }
    #[test]
    fn malformed_public_options_are_errors() {
        for extra in [
            vec!["DIALECT", "2"],
            vec!["KNN", "2", "K", "0"],
            vec!["RANGE", "2", "EPSILON", "1"],
            vec!["RANGE", "4", "RADIUS", "1", "EPSILON", "0"],
            vec!["COMBINE", "LINEAR", "2", "ALPHA", "0.5"],
            vec!["NOSORT", "SORTBY", "2", "@__score", "DESC"],
            vec!["EXPLAINSCORE", "GROUPBY", "0"],
            vec!["LOAD", "1", "price"],
            vec!["KNN", "4", "K", "2", "K", "3"],
            vec!["FILTER", "3", "*", "POLICY", "ADHOC_BF"],
        ] {
            assert!(
                parse(&command(&extra), &Config::default()).is_err(),
                "{extra:?}"
            );
        }
    }
    #[test]
    fn counted_and_positional_fusion_aliases_are_equivalent() {
        let a = parse(
            &command(&[
                "COMBINE",
                "RRF",
                "4",
                "CONSTANT",
                "10",
                "YIELD_SCORE_AS",
                "fused",
            ]),
            &Config::default(),
        )
        .unwrap();
        let b = parse(
            &command(&[
                "COMBINE",
                "RRF",
                "2",
                "CONSTANT",
                "10",
                "YIELD_SCORE_AS",
                "fused",
            ]),
            &Config::default(),
        )
        .unwrap();
        assert_eq!(a.combined_alias, b.combined_alias);
        assert_eq!(a.constant, b.constant);
    }
    #[test]
    fn metric_normalizers_preserve_pinned_inner_product_behavior() {
        assert_eq!(score("L2", 3.), 0.25);
        assert_eq!(score("COSINE", 0.5), 0.75);
        assert_eq!(score("IP", 0.5), 0.75);
    }
}
