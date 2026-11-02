use super::{expression, model::*, query};
use bytes::Bytes;
use rand::Rng;
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub(super) struct CursorOptions {
    pub count: usize,
    pub max_idle: u64,
}
pub(super) struct PipelineResult {
    pub rows: Vec<Row>,
    pub total: usize,
    pub cursor: Option<CursorOptions>,
    pub warnings: Vec<String>,
}
pub(super) struct Prepared {
    pub options: query::Options,
    pub stages: Vec<Bytes>,
    pub addscores: bool,
    pub withcount: bool,
    pub format_expand: bool,
}
pub(super) fn text(b: &Bytes) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "Invalid UTF-8 in command option".into())
}
fn token(args: &[Bytes], p: usize) -> Result<&str, String> {
    args.get(p)
        .ok_or_else(|| "Missing argument".to_string())
        .and_then(text)
}
fn unsigned(args: &[Bytes], p: usize) -> Result<usize, String> {
    token(args, p)?
        .parse()
        .map_err(|_| "Expected a non-negative integer".into())
}
fn positive(args: &[Bytes], p: usize) -> Result<usize, String> {
    let n = unsigned(args, p)?;
    if n == 0 {
        return Err("Expected a positive integer".into());
    }
    Ok(n)
}
fn property(s: &str) -> String {
    s.strip_prefix('@').unwrap_or(s).to_owned()
}
fn end(args: &[Bytes], start: usize, n: usize) -> Result<usize, String> {
    start
        .checked_add(n)
        .filter(|e| *e <= args.len())
        .ok_or_else(|| "Argument count exceeds available arguments".into())
}
fn pipeline_keyword(s: &str) -> bool {
    matches!(
        s.to_ascii_uppercase().as_str(),
        "LOAD" | "GROUPBY" | "REDUCE" | "SORTBY" | "APPLY" | "FILTER" | "LIMIT" | "WITHCURSOR"
    )
}
pub(super) fn prepare(args: &[Bytes], config: &Config) -> Result<Prepared, String> {
    let mut options = query::Options {
        dialect: config.default_dialect as u32,
        timeout_ms: config.timeout_ms,
        ..Default::default()
    };
    if let Some(scorer) = config.extras.get("DEFAULT_SCORER") {
        options.scorer = scorer.clone();
    }
    let mut stages = Vec::new();
    let mut addscores = false;
    let mut withcount = false;
    let mut count_option = false;
    let mut format_expand = false;
    let mut format_option = false;
    let mut p = 0;
    while p < args.len() {
        let key = token(args, p)?.to_ascii_uppercase();
        match key.as_str() {
            "VERBATIM" => {
                options.verbatim = true;
                p += 1
            }
            "NOSTOPWORDS" => {
                options.nostopwords = true;
                p += 1
            }
            "ADDSCORES" => {
                addscores = true;
                p += 1
            }
            "WITHCOUNT" | "WITHOUTCOUNT" => {
                if count_option {
                    return Err("WITHCOUNT and WITHOUTCOUNT specified more than once".into());
                }
                count_option = true;
                withcount = key == "WITHCOUNT";
                p += 1;
            }
            "FORMAT" => {
                if format_option {
                    return Err("Duplicate FORMAT".into());
                }
                format_option = true;
                format_expand = match token(args, p + 1)?.to_ascii_uppercase().as_str() {
                    "EXPAND" => true,
                    "STRING" => false,
                    _ => return Err("FORMAT must be STRING or EXPAND".into()),
                };
                p += 2;
            }
            "DIALECT" => {
                let n = unsigned(args, p + 1)?;
                if !(1..=4).contains(&n) {
                    return Err("DIALECT must be between 1 and 4".into());
                }
                options.dialect = n as u32;
                p += 2
            }
            "TIMEOUT" => {
                options.timeout_ms = unsigned(args, p + 1)? as u64;
                p += 2
            }
            "SCORER" => {
                options.scorer = token(args, p + 1)?.to_owned();
                p += 2
            }
            "LANGUAGE" => {
                options.language = token(args, p + 1)?.to_owned();
                p += 2
            }
            "EXPANDER" => {
                options.expander = token(args, p + 1)?.to_owned();
                p += 2
            }
            "PARAMS" => {
                let n = unsigned(args, p + 1)?;
                if n % 2 != 0 {
                    return Err("PARAMS requires name/value pairs".into());
                }
                let e = end(args, p + 2, n)?;
                for pair in args[p + 2..e].chunks(2) {
                    let name = text(&pair[0])?.to_owned();
                    if options.params.insert(name, pair[1].to_vec()).is_some() {
                        return Err("Duplicate parameter".into());
                    }
                }
                p = e
            }
            _ if pipeline_keyword(&key) => {
                let start = p;
                p = stage_end(args, p)?;
                stages.extend_from_slice(&args[start..p]);
            }
            _ => return Err(format!("Unknown argument '{key}'")),
        }
    }
    if !options.params.is_empty() && options.dialect < 2 {
        return Err("PARAMS requires DIALECT 2 or greater".into());
    }
    Ok(Prepared {
        options,
        stages,
        addscores,
        withcount,
        format_expand,
    })
}
pub(super) fn stage_end(args: &[Bytes], p: usize) -> Result<usize, String> {
    match token(args, p)?.to_ascii_uppercase().as_str() {
        "LOAD" => {
            if token(args, p + 1)? == "*" {
                Ok(p + 2)
            } else {
                end(args, p + 2, unsigned(args, p + 1)?)
            }
        }
        "GROUPBY" => end(args, p + 2, unsigned(args, p + 1)?),
        "REDUCE" => {
            let e = end(args, p + 3, unsigned(args, p + 2)?)?;
            if args.get(e).is_some_and(|s| s.eq_ignore_ascii_case(b"AS")) {
                end(args, e + 1, 1)
            } else {
                Ok(e)
            }
        }
        "APPLY" => end(args, p + 1, 3),
        "FILTER" => end(args, p + 1, 1),
        "LIMIT" => end(args, p + 1, 2),
        "SORTBY" => {
            let e = end(args, p + 2, unsigned(args, p + 1)?)?;
            if args.get(e).is_some_and(|s| s.eq_ignore_ascii_case(b"MAX")) {
                end(args, e + 1, 1)
            } else {
                Ok(e)
            }
        }
        "WITHCURSOR" => {
            let mut e = p + 1;
            while args.get(e).is_some_and(|s| {
                s.eq_ignore_ascii_case(b"COUNT") || s.eq_ignore_ascii_case(b"MAXIDLE")
            }) {
                e = end(args, e + 1, 1)?
            }
            Ok(e)
        }
        _ => Err("Invalid pipeline stage".into()),
    }
}
#[derive(Clone)]
struct Entry {
    id: Option<DocId>,
    row: Row,
}
#[derive(Clone)]
struct Sort {
    field: String,
    desc: bool,
}
#[derive(Clone)]
struct Collect {
    fields: Option<Vec<String>>,
    distinct: bool,
    sort: Vec<Sort>,
    offset: usize,
    count: usize,
}
struct Reducer {
    name: String,
    args: Vec<String>,
    alias: String,
    collect: Option<Collect>,
}
fn sort_spec(args: &[Bytes]) -> Result<Vec<Sort>, String> {
    let mut out = Vec::new();
    let mut p = 0;
    while p < args.len() {
        let field = property(text(&args[p])?);
        if field.is_empty() || matches!(field.to_ascii_uppercase().as_str(), "ASC" | "DESC") {
            return Err("Invalid SORTBY property".into());
        }
        p += 1;
        let desc = if args
            .get(p)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"ASC") || s.eq_ignore_ascii_case(b"DESC"))
        {
            let d = args[p].eq_ignore_ascii_case(b"DESC");
            p += 1;
            d
        } else {
            false
        };
        out.push(Sort { field, desc })
    }
    if out.is_empty() {
        return Err("SORTBY requires at least one property".into());
    }
    Ok(out)
}
fn sort_rows(a: &Row, b: &Row, spec: &[Sort]) -> Ordering {
    for s in spec {
        let c = expression::compare(
            a.get(&s.field).unwrap_or(&Value::Null),
            b.get(&s.field).unwrap_or(&Value::Null),
        );
        if c != Ordering::Equal {
            return if s.desc { c.reverse() } else { c };
        }
    }
    Ordering::Equal
}
fn require(available: &BTreeSet<String>, field: &str) -> Result<(), String> {
    if !available.contains(field) {
        return Err(format!("Property '{field}' not loaded nor in schema"));
    }
    Ok(())
}
fn collect_spec(args: &[Bytes], config: &Config) -> Result<Collect, String> {
    if !token(args, 0)?.eq_ignore_ascii_case("FIELDS") {
        return Err("COLLECT requires FIELDS".into());
    }
    let mut p = 2;
    let fields = if token(args, 1)? == "*" {
        None
    } else {
        let n = unsigned(args, 1)?;
        let e = end(args, 2, n)?;
        p = e;
        let fields = args[2..e]
            .iter()
            .map(|s| text(s).map(property))
            .collect::<Result<Vec<_>, _>>()?;
        if fields.len() == 1 && fields[0] == "*" {
            None
        } else {
            Some(fields)
        }
    };
    let mut distinct = false;
    let mut sort = Vec::new();
    let mut offset = 0;
    let mut count = config.max_aggregate;
    let mut seen = BTreeSet::new();
    while p < args.len() {
        let key = token(args, p)?.to_ascii_uppercase();
        if !seen.insert(key.clone()) {
            return Err(format!("Duplicate COLLECT option {key}"));
        }
        match key.as_str() {
            "DISTINCT" => {
                distinct = true;
                p += 1
            }
            "SORTBY" => {
                let n = unsigned(args, p + 1)?;
                let e = end(args, p + 2, n)?;
                sort = sort_spec(&args[p + 2..e])?;
                p = e
            }
            "LIMIT" => {
                offset = unsigned(args, p + 1)?;
                count = unsigned(args, p + 2)?;
                if offset > config.max_aggregate
                    || count > config.max_aggregate
                    || offset.checked_add(count).is_none()
                {
                    return Err("COLLECT LIMIT exceeds maximum aggregate results".into());
                }
                p += 3
            }
            _ => return Err(format!("Unknown COLLECT option {key}")),
        }
    }
    Ok(Collect {
        fields,
        distinct,
        sort,
        offset,
        count,
    })
}
fn parse_reducer(args: &[Bytes], p: &mut usize, config: &Config) -> Result<Reducer, String> {
    let name = token(args, *p + 1)?.to_ascii_uppercase();
    let n = unsigned(args, *p + 2)?;
    let e = end(args, *p + 3, n)?;
    let raw = &args[*p + 3..e];
    let fields = raw
        .iter()
        .map(|b| text(b).map(str::to_owned))
        .collect::<Result<Vec<_>, _>>()?;
    let valid = match name.as_str() {
        "COUNT" => n == 0,
        "COUNT_DISTINCT" | "COUNT_DISTINCTISH" | "SUM" | "MIN" | "MAX" | "AVG" | "STDDEV"
        | "TOLIST" | "HLL" | "HLL_SUM" => n == 1,
        "QUANTILE" | "RANDOM_SAMPLE" => n == 2,
        "FIRST_VALUE" => {
            n == 1
                || (n == 3 || n == 4) && fields.get(1).is_some_and(|s| s.eq_ignore_ascii_case("BY"))
        }
        "COLLECT" => n >= 2,
        _ => false,
    };
    if !valid {
        return Err(format!(
            "Unknown reducer or invalid number of arguments for {name}"
        ));
    }
    if name == "QUANTILE" {
        let q = fields[1]
            .parse::<f64>()
            .map_err(|_| "Invalid quantile".to_string())?;
        if !(0.0..=1.0).contains(&q) {
            return Err("QUANTILE must be between 0 and 1".into());
        }
    }
    if name == "RANDOM_SAMPLE"
        && fields[1]
            .parse::<usize>()
            .ok()
            .filter(|n| *n > 0 && *n <= config.max_aggregate)
            .is_none()
    {
        return Err("Invalid RANDOM_SAMPLE size".into());
    }
    if name == "FIRST_VALUE"
        && n == 4
        && !matches!(fields[3].to_ascii_uppercase().as_str(), "ASC" | "DESC")
    {
        return Err("Invalid FIRST_VALUE order".into());
    }
    let collect = if name == "COLLECT" {
        Some(collect_spec(raw, config)?)
    } else {
        None
    };
    *p = e;
    let alias = if args.get(*p).is_some_and(|s| s.eq_ignore_ascii_case(b"AS")) {
        let alias = token(args, *p + 1)?.to_owned();
        *p += 2;
        alias
    } else {
        format!("{}({})", name.to_ascii_lowercase(), fields.join(","))
    };
    Ok(Reducer {
        name,
        args: fields,
        alias,
        collect,
    })
}
fn values<'a>(rows: &'a [&'a Row], field: &'a str) -> impl Iterator<Item = &'a Value> + 'a {
    let deadline = super::runtime::current_deadline();
    rows.iter()
        .take_while(move |_| !super::runtime::expired(deadline))
        .filter_map(move |row| row.get(field))
        .map(super::value::logical)
        .flat_map(|value| {
            match value {
                Value::Array(a) => a.as_slice(),
                value => std::slice::from_ref(value),
            }
            .iter()
        })
        .filter(|v| !v.is_null())
}
fn json_number(n: f64) -> Value {
    serde_json::Number::from_f64(n)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}
fn canonical(v: &Value) -> String {
    v.to_string()
}
fn hll(rows: &[&Row], field: &str) -> Vec<u8> {
    let mut registers = vec![0u8; 1024];
    for v in values(rows, field) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        canonical(v).hash(&mut h);
        let x = h.finish();
        let slot = (x & 1023) as usize;
        let rank = ((x >> 10).leading_zeros().saturating_sub(10) + 1).min(55) as u8;
        registers[slot] = registers[slot].max(rank)
    }
    registers
}
fn hll_count(registers: &[u8]) -> f64 {
    let m = registers.len() as f64;
    let estimate = 0.7213 / (1. + 1.079 / m) * m * m
        / registers
            .iter()
            .map(|r| 2f64.powi(-(*r as i32)))
            .sum::<f64>();
    let zeros = registers.iter().filter(|r| **r == 0).count();
    if estimate <= 2.5 * m && zeros > 0 {
        m * (m / zeros as f64).ln()
    } else {
        estimate
    }
}
fn reduce(r: &Reducer, rows: &[&Row]) -> Value {
    if let Some(c) = &r.collect {
        let source: std::borrow::Cow<'_, [&Row]> = if c.sort.is_empty() {
            std::borrow::Cow::Borrowed(rows)
        } else {
            let mut source = rows.to_vec();
            if !super::runtime::sort(
                &mut source,
                |a, b| sort_rows(a, b, &c.sort),
                super::runtime::current_deadline(),
            ) {
                return Value::Null;
            }
            std::borrow::Cow::Owned(source)
        };
        let mut seen = BTreeSet::new();
        let records = source
            .iter()
            .copied()
            .take_while(|_| !super::runtime::cancelled())
            .filter_map(|row| {
                let record: serde_json::Map<String, Value> = match &c.fields {
                    None => row
                        .iter()
                        .filter(|(k, _)| {
                            !k.starts_with("__matched_") && !k.starts_with("__search_")
                        })
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect(),
                    Some(fields) => fields
                        .iter()
                        .filter_map(|f| row.get(f).map(|v| (f.clone(), v.clone())))
                        .collect(),
                };
                let v = super::value::from_object_encoded(record);
                if c.distinct && !seen.insert(canonical(&v)) {
                    None
                } else {
                    Some(v)
                }
            })
            .skip(c.offset)
            .take(c.count)
            .collect();
        return Value::Array(records);
    }
    if r.name == "COUNT" {
        return json_number(rows.len() as f64);
    }
    let field = property(&r.args[0]);
    let vals = values(rows, &field);
    match r.name.as_str() {
        "COUNT_DISTINCT" => json_number(vals.map(canonical).collect::<BTreeSet<_>>().len() as f64),
        "COUNT_DISTINCTISH" => json_number(hll_count(&hll(rows, &field)).round()),
        "HLL" => Value::Array(hll(rows, &field).into_iter().map(Value::from).collect()),
        "HLL_SUM" => {
            let mut registers = vec![0u8; 1024];
            for row in rows.iter().take_while(|_| !super::runtime::cancelled()) {
                if let Some(Value::Array(a)) = row.get(&field)
                    && a.len() == 1024
                {
                    for (i, v) in a.iter().enumerate() {
                        registers[i] = registers[i].max(v.as_u64().unwrap_or(0).min(55) as u8)
                    }
                }
            }
            json_number(hll_count(&registers).round())
        }
        "TOLIST" => {
            let mut seen = BTreeSet::new();
            Value::Array(
                vals.into_iter()
                    .filter(|v| seen.insert(canonical(v)))
                    .cloned()
                    .collect(),
            )
        }
        "FIRST_VALUE" => {
            let selected = if r.args.len() > 1 {
                let sort = Sort {
                    field: property(&r.args[2]),
                    desc: r
                        .args
                        .get(3)
                        .is_some_and(|s| s.eq_ignore_ascii_case("DESC")),
                };
                rows.iter()
                    .copied()
                    .take_while(|_| !super::runtime::cancelled())
                    .min_by(|a, b| sort_rows(a, b, std::slice::from_ref(&sort)))
            } else {
                rows.first().copied()
            };
            selected
                .and_then(|r| r.get(&field))
                .map(super::value::logical)
                .cloned()
                .unwrap_or(Value::Null)
        }
        "RANDOM_SAMPLE" => {
            let k = r.args[1].parse::<usize>().unwrap_or(0);
            let mut sample = Vec::with_capacity(k.min(rows.len()));
            let mut rng = rand::rng();
            for (i, v) in vals.enumerate() {
                if i < k {
                    sample.push(v.clone())
                } else {
                    let j = rng.random_range(0..=i);
                    if j < k {
                        sample[j] = v.clone()
                    }
                }
            }
            Value::Array(sample)
        }
        "MIN" | "MAX" => {
            let selected = if r.name == "MIN" {
                vals.into_iter().min_by(|a, b| expression::compare(a, b))
            } else {
                vals.into_iter().max_by(|a, b| expression::compare(a, b))
            };
            selected.cloned().unwrap_or(Value::Null)
        }
        "QUANTILE" => {
            let mut nums: Vec<f64> = vals
                .filter_map(expression::numeric)
                .filter(|n| n.is_finite())
                .collect();
            if nums.is_empty() {
                return Value::Null;
            }
            if !super::runtime::sort(
                &mut nums,
                f64::total_cmp,
                super::runtime::current_deadline(),
            ) {
                return Value::Null;
            }
            let q = r.args[1].parse::<f64>().unwrap_or(0.5);
            let rank = q * (nums.len() - 1) as f64;
            let i = rank.floor() as usize;
            json_number(nums[i] + (nums[(i + 1).min(nums.len() - 1)] - nums[i]) * rank.fract())
        }
        _ => {
            let (mut count, mut sum, mut mean, mut m2) = (0usize, 0., 0., 0.);
            for value in vals
                .filter_map(expression::numeric)
                .filter(|n| n.is_finite())
            {
                count += 1;
                sum += value;
                let delta = value - mean;
                mean += delta / count as f64;
                m2 += delta * (value - mean);
            }
            match r.name.as_str() {
                "SUM" => json_number(sum),
                "AVG" => {
                    if rows.is_empty() {
                        Value::Null
                    } else {
                        json_number(sum / rows.len() as f64)
                    }
                }
                "STDDEV" => {
                    if count == 0 {
                        Value::Null
                    } else if count == 1 {
                        json_number(0.)
                    } else {
                        json_number((m2 / (count - 1) as f64).sqrt())
                    }
                }
                _ => Value::Null,
            }
        }
    }
}
fn validate_limit(offset: usize, count: usize, max: usize) -> Result<(), String> {
    if count == 0 && offset != 0 {
        return Err("LIMIT offset must be zero when count is zero".into());
    }
    let end = offset
        .checked_add(count)
        .ok_or_else(|| "LIMIT overflow".to_string())?;
    if end > max {
        return Err("LIMIT exceeds maximum aggregate results".into());
    }
    Ok(())
}
fn indexed_value(v: &IndexedValue) -> Value {
    match v {
        IndexedValue::Text { strings, .. } | IndexedValue::Tag(strings) => {
            if strings.len() == 1 {
                Value::String(strings[0].clone())
            } else {
                Value::Array(strings.iter().cloned().map(Value::String).collect())
            }
        }
        IndexedValue::Numeric(nums) => {
            if nums.len() == 1 {
                json_number(nums[0])
            } else {
                Value::Array(nums.iter().map(|n| json_number(*n)).collect())
            }
        }
        IndexedValue::Geo(points) => Value::Array(
            points
                .iter()
                .map(|p| Value::String(format!("{},{}", p[0], p[1])))
                .collect(),
        ),
        IndexedValue::Geometry(s) => Value::String(s.clone()),
        _ => Value::Null,
    }
}
pub(super) fn execute_prepared(
    index: &Index,
    hits: &[Hit],
    prepared: &Prepared,
    loaded: BTreeMap<DocId, Row>,
    config: &Config,
    now: i64,
    deadline: Option<std::time::Instant>,
) -> Result<PipelineResult, String> {
    let mut entries = Vec::with_capacity(hits.len());
    let mut available: BTreeSet<String> = index
        .schema
        .fields
        .iter()
        .filter(|f| f.sortable)
        .map(|f| f.name.clone())
        .collect();
    if prepared.addscores {
        available.insert("__score".into());
    }
    for hit in hits {
        if config.enforce_deadline() && super::runtime::expired(deadline) {
            break;
        }
        let Some(doc) = index.docs.get(&hit.id) else {
            continue;
        };
        if doc.expires_at.is_some_and(|t| t <= now) || doc.field_deadline.is_some_and(|t| t <= now)
        {
            continue;
        }
        let mut row = Row::new();
        row.insert(
            "__search_source_key".into(),
            super::value::from_bytes(&doc.key),
        );
        row.insert(
            "__search_binary_key".into(),
            Value::Array(doc.key.iter().map(|b| Value::from(*b)).collect()),
        );
        row.insert(
            "__matched_terms".into(),
            Value::Array(
                hit.matched_terms
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
        for (name, distance) in &hit.vector_distances {
            available.insert(name.clone());
            row.insert(name.clone(), json_number(*distance));
        }
        if prepared.addscores {
            row.insert("__score".into(), json_number(hit.score));
        }
        add_sortables(index, doc, loaded.get(&hit.id), &mut row);
        entries.push(Entry {
            id: Some(hit.id),
            row,
        });
    }
    run(
        entries,
        available,
        &prepared.stages,
        &loaded,
        config,
        prepared.options.timeout_ms,
        prepared.withcount,
        index.schema.json,
        deadline,
    )
}
fn add_sortables(index: &Index, doc: &Document, source: Option<&Row>, row: &mut Row) {
    use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};
    for f in index.schema.fields.iter().filter(|f| f.sortable) {
        if let Some(mut value) = source
            .and_then(|s| s.get(&f.name).or_else(|| s.get(&f.source)))
            .cloned()
            .or_else(|| doc.fields.get(&f.name).map(indexed_value))
        {
            if matches!(f.kind, FieldKind::Numeric) {
                if let Some(n) = expression::numeric(&value) {
                    value = json_number(n)
                }
            } else if !index.schema.json
                && !f.unf
                && let Value::String(s) = &value
            {
                value = Value::String(
                    s.nfkd()
                        .filter(|c| !is_combining_mark(*c))
                        .flat_map(char::to_lowercase)
                        .collect(),
                )
            }
            row.insert(f.name.clone(), value);
        }
    }
}
/// Hybrid rows already carry the public score fields. Source LOAD still reads only matched identities.
#[expect(
    clippy::too_many_arguments,
    reason = "Hybrid rows retain the same loading, configuration, counting, and deadline context as the parsed pipeline."
)]
pub(super) fn execute_rows(
    index: &Index,
    rows: Vec<(DocId, Row)>,
    args: &[Bytes],
    loaded: BTreeMap<DocId, Row>,
    config: &Config,
    mut available: BTreeSet<String>,
    timeout_ms: u64,
    deadline: Option<std::time::Instant>,
) -> Result<PipelineResult, String> {
    let mut visible = available.clone();
    let mut load_all = false;
    let mut grouped = false;
    let mut p = 0;
    while p < args.len() {
        let stage = token(args, p)?.to_ascii_uppercase();
        let e = stage_end(args, p)?;
        match stage.as_str() {
            "LOAD" => {
                if token(args, p + 1)? == "*" {
                    load_all = true
                } else {
                    let mut q = p + 2;
                    while q < e {
                        let field = property(token(args, q)?);
                        q += 1;
                        let alias = if q < e && token(args, q)?.eq_ignore_ascii_case("AS") {
                            let alias = token(args, q + 1)?.to_owned();
                            q += 2;
                            alias
                        } else {
                            field
                        };
                        visible.insert(alias);
                    }
                }
            }
            "APPLY" => {
                visible.insert(token(args, p + 3)?.to_owned());
            }
            "GROUPBY" => grouped = true,
            _ => {}
        }
        p = e;
    }
    available.extend(
        index
            .schema
            .fields
            .iter()
            .filter(|f| f.sortable)
            .map(|f| f.name.clone()),
    );
    let mut entries = Vec::with_capacity(rows.len());
    for (id, mut row) in rows {
        available.extend(
            row.keys()
                .filter(|k| !k.starts_with("__matched_") && !k.starts_with("__search_"))
                .cloned(),
        );
        if let Some(doc) = index.docs.get(&id) {
            row.insert(
                "__search_binary_key".into(),
                Value::Array(doc.key.iter().map(|b| Value::from(*b)).collect()),
            );
            add_sortables(index, doc, loaded.get(&id), &mut row)
        }
        entries.push(Entry { id: Some(id), row });
    }
    let mut result = run(
        entries,
        available,
        args,
        &loaded,
        config,
        timeout_ms,
        true,
        index.schema.json,
        deadline,
    )?;
    if !load_all && !grouped {
        for row in &mut result.rows {
            row.retain(|key, _| key.starts_with("__search_") || visible.contains(key));
        }
    }
    Ok(result)
}
#[expect(
    clippy::too_many_arguments,
    reason = "Pipeline execution borrows row/loading state and independent wire, count, and deadline modes."
)]
fn run(
    mut entries: Vec<Entry>,
    mut available: BTreeSet<String>,
    args: &[Bytes],
    loaded: &BTreeMap<DocId, Row>,
    config: &Config,
    timeout_ms: u64,
    withcount: bool,
    json: bool,
    deadline: Option<std::time::Instant>,
) -> Result<PipelineResult, String> {
    let started = std::time::Instant::now();
    let deadline = if config.enforce_deadline() {
        deadline.or_else(|| config.deadline(started, timeout_ms))
    } else {
        None
    };
    let _deadline_guard = super::runtime::DeadlineGuard::enter(deadline);
    let mut p = 0;
    let mut cursor = None;
    let mut total = None;
    let mut grouped = false;
    let mut timed_out = false;
    let expired = || super::runtime::expired(deadline);
    while p < args.len() {
        if expired() {
            if config.on_timeout_fail {
                return Err("Timeout limit was reached".into());
            }
            timed_out = true;
            entries.clear();
            break;
        }
        let key = token(args, p)?.to_ascii_uppercase();
        match key.as_str() {
            "LOAD" => {
                if grouped {
                    return Err("LOAD cannot be applied after GROUPBY".into());
                }
                if token(args, p + 1)? == "*" {
                    let mut processed = 0;
                    for entry in &mut entries {
                        if expired() {
                            timed_out = true;
                            break;
                        }
                        if let Some(source) = entry.id.and_then(|id| loaded.get(&id)) {
                            for (k, v) in source {
                                if json && k != "$"
                                    || matches!(k.as_str(), "__key" | "__score" | "__matched_terms")
                                {
                                    continue;
                                }
                                entry.row.insert(k.clone(), v.clone());
                                available.insert(k.clone());
                            }
                        }
                        processed += 1;
                    }
                    entries.truncate(processed);
                    if !timed_out {
                        for source in loaded.values() {
                            available.extend(
                                source
                                    .keys()
                                    .filter(|k| {
                                        (!json || k.as_str() == "$")
                                            && !matches!(
                                                k.as_str(),
                                                "__key" | "__score" | "__matched_terms"
                                            )
                                    })
                                    .cloned(),
                            )
                        }
                    }
                    if json {
                        available.insert("$".into());
                    }
                    p += 2;
                } else {
                    let n = unsigned(args, p + 1)?;
                    let e = end(args, p + 2, n)?;
                    let mut q = p + 2;
                    while q < e {
                        let field = property(token(args, q)?);
                        q += 1;
                        let alias = if q < e && token(args, q)?.eq_ignore_ascii_case("AS") {
                            let a = token(args, q + 1)?.to_owned();
                            q += 2;
                            if q > e {
                                return Err("LOAD alias exceeds argument count".into());
                            }
                            a
                        } else {
                            field.clone()
                        };
                        available.insert(alias.clone());
                        let mut processed = 0;
                        for entry in &mut entries {
                            if expired() {
                                timed_out = true;
                                break;
                            }
                            let value = if field == "__key" {
                                entry
                                    .row
                                    .get("__key")
                                    .or_else(|| entry.row.get("__search_source_key"))
                            } else if field == "__score" {
                                entry.row.get(&field)
                            } else {
                                entry
                                    .id
                                    .and_then(|id| loaded.get(&id))
                                    .and_then(|r| r.get(&field))
                            };
                            if let Some(v) = value.cloned() {
                                entry.row.insert(alias.clone(), v);
                            }
                            processed += 1;
                        }
                        entries.truncate(processed);
                    }
                    p = e;
                }
            }
            "APPLY" => {
                let program = expression::compile(token(args, p + 1)?)?;
                if !token(args, p + 2)?.eq_ignore_ascii_case("AS") {
                    return Err("APPLY requires AS alias".into());
                }
                let alias = token(args, p + 3)?.to_owned();
                for field in &program.properties {
                    require(&available, field)?
                }
                let mut processed = 0;
                for entry in &mut entries {
                    if !timed_out && processed % 128 == 0 && expired() {
                        timed_out = true;
                        break;
                    }
                    let value = program.evaluate(&entry.row);
                    entry.row.insert(alias.clone(), value);
                    processed += 1;
                }
                entries.truncate(processed);
                available.insert(alias);
                p += 4;
            }
            "FILTER" => {
                let program = expression::compile(token(args, p + 1)?)?;
                for field in &program.properties {
                    require(&available, field)?
                }
                let mut filtered = Vec::with_capacity(entries.len());
                for (i, entry) in entries.into_iter().enumerate() {
                    if !timed_out && i % 128 == 0 && expired() {
                        timed_out = true;
                        break;
                    }
                    if expression::truth(&program.evaluate(&entry.row)) {
                        filtered.push(entry);
                    }
                }
                entries = filtered;
                total = None;
                p += 2;
            }
            "SORTBY" => {
                let n = unsigned(args, p + 1)?;
                let e = end(args, p + 2, n)?;
                let spec = if n == 0 {
                    Vec::new()
                } else {
                    sort_spec(&args[p + 2..e])?
                };
                for s in &spec {
                    require(&available, &s.field)?
                }
                if !spec.is_empty()
                    && !super::runtime::sort(
                        &mut entries,
                        |a, b| sort_rows(&a.row, &b.row, &spec),
                        deadline,
                    )
                {
                    timed_out = true;
                }
                p = e;
                let explicit_max = if args.get(p).is_some_and(|b| b.eq_ignore_ascii_case(b"MAX")) {
                    let max = unsigned(args, p + 1)?;
                    if max > config.max_aggregate {
                        return Err("SORTBY MAX exceeds maximum aggregate results".into());
                    }
                    p += 2;
                    Some(max)
                } else {
                    None
                };
                let mut downstream_limit = None;
                let mut next = p;
                while next < args.len() {
                    match token(args, next)?.to_ascii_uppercase().as_str() {
                        "LIMIT" => {
                            let offset = unsigned(args, next + 1)?;
                            let count = unsigned(args, next + 2)?;
                            validate_limit(offset, count, config.max_aggregate)?;
                            downstream_limit = Some(offset + count);
                            break;
                        }
                        "GROUPBY" | "SORTBY" | "FILTER" => break,
                        _ => next = stage_end(args, next)?,
                    }
                }
                let max = downstream_limit
                    .or(explicit_max)
                    .or_else(|| (!spec.is_empty()).then_some(10));
                if let Some(max) = max {
                    total = Some(entries.len());
                    entries.truncate(max);
                }
            }
            "LIMIT" => {
                let offset = unsigned(args, p + 1)?;
                let count = unsigned(args, p + 2)?;
                validate_limit(offset, count, config.max_aggregate)?;
                total.get_or_insert(entries.len());
                entries = entries.into_iter().skip(offset).take(count).collect();
                p += 3;
            }
            "GROUPBY" => {
                let n = unsigned(args, p + 1)?;
                let e = end(args, p + 2, n)?;
                let fields = args[p + 2..e]
                    .iter()
                    .map(|s| text(s).map(property))
                    .collect::<Result<Vec<_>, _>>()?;
                for f in &fields {
                    require(&available, f)?
                }
                p = e;
                let mut reducers = Vec::new();
                while args
                    .get(p)
                    .is_some_and(|s| s.eq_ignore_ascii_case(b"REDUCE"))
                {
                    let r = parse_reducer(args, &mut p, config)?;
                    if let Some(c) = &r.collect {
                        if let Some(fields) = &c.fields {
                            for f in fields {
                                require(&available, f)?
                            }
                        }
                        for s in &c.sort {
                            require(&available, &s.field)?
                        }
                    } else if r.name != "COUNT" {
                        require(&available, &property(&r.args[0]))?;
                        if r.name == "FIRST_VALUE" && r.args.len() > 1 {
                            require(&available, &property(&r.args[2]))?
                        }
                    }
                    reducers.push(r)
                }
                let mut groups: BTreeMap<String, (Row, Vec<&Row>)> = BTreeMap::new();
                let max_groups = config
                    .extras
                    .get("MAX_AGGREGATE_GROUPS")
                    .and_then(|s| s.parse::<usize>().ok())
                    .unwrap_or(1_000_000);
                for (i, entry) in entries.iter().enumerate() {
                    if !timed_out && i % 128 == 0 && expired() {
                        timed_out = true;
                        break;
                    }
                    let mut keys = vec![Row::new()];
                    for field in &fields {
                        let value =
                            super::value::logical(entry.row.get(field).unwrap_or(&Value::Null));
                        let values = if let Value::Array(a) = value {
                            if a.is_empty() {
                                vec![Value::Null]
                            } else {
                                a.clone()
                            }
                        } else {
                            vec![value.clone()]
                        };
                        let mut next = Vec::new();
                        for key in keys {
                            for v in &values {
                                let mut key = key.clone();
                                key.insert(field.clone(), v.clone());
                                next.push(key);
                                if next.len() > max_groups {
                                    return Err("Maximum aggregate groups exceeded".into());
                                }
                            }
                        }
                        keys = next;
                    }
                    let mut distinct = BTreeSet::new();
                    for key in keys {
                        let encoded = serde_json::to_string(&key).map_err(|e| e.to_string())?;
                        if distinct.insert(encoded.clone()) {
                            groups
                                .entry(encoded)
                                .or_insert_with(|| (key, Vec::new()))
                                .1
                                .push(&entry.row);
                        }
                        if groups.len() > max_groups {
                            return Err("Maximum aggregate groups exceeded".into());
                        }
                    }
                }
                if groups.is_empty() && fields.is_empty() {
                    groups.insert(String::new(), (Row::new(), Vec::new()));
                }
                let mut next = Vec::with_capacity(groups.len());
                for (_, (mut row, group)) in groups {
                    if expired() {
                        timed_out = true;
                        break;
                    }
                    for r in &reducers {
                        row.insert(r.alias.clone(), reduce(r, &group));
                    }
                    if expired() {
                        timed_out = true;
                        break;
                    }
                    next.push(Entry { id: None, row });
                }
                entries = next;
                available = fields
                    .into_iter()
                    .chain(reducers.into_iter().map(|r| r.alias))
                    .collect();
                grouped = true;
                total = None;
            }
            "WITHCURSOR" => {
                if cursor.is_some() {
                    return Err("WITHCURSOR specified more than once".into());
                }
                let mut options = CursorOptions {
                    count: 1000,
                    max_idle: config.cursor_max_idle,
                };
                let mut seen = BTreeSet::new();
                p += 1;
                while let Some(b) = args.get(p) {
                    let k = text(b)?.to_ascii_uppercase();
                    match k.as_str() {
                        "COUNT" | "MAXIDLE" => {
                            if !seen.insert(k.clone()) {
                                return Err("Duplicate cursor option".into());
                            }
                            let n = positive(args, p + 1)?;
                            if k == "COUNT" {
                                options.count = n
                            } else {
                                options.max_idle = (n as u64).min(config.cursor_max_idle)
                            }
                            p += 2;
                        }
                        _ => break,
                    }
                }
                cursor = Some(options);
            }
            "REDUCE" => return Err("REDUCE must follow GROUPBY".into()),
            _ => return Err(format!("Unknown pipeline stage {key}")),
        }
        if !timed_out && expired() {
            timed_out = true;
        }
        if timed_out && config.on_timeout_fail {
            return Err("Timeout limit was reached".into());
        }
        if timed_out && config.on_timeout_strict && p < args.len() {
            entries.clear();
            break;
        }
    }
    if expired() {
        timed_out = true;
        if config.on_timeout_fail {
            return Err("Timeout limit was reached".into());
        }
    }
    entries.truncate(config.max_aggregate);
    let total = if withcount {
        total.unwrap_or(entries.len())
    } else {
        entries.len()
    };
    let rows = entries
        .into_iter()
        .map(|e| {
            let mut row = e.row;
            row.remove("__matched_terms");
            if let Some(id) = e.id {
                row.insert("__search_doc_id".into(), Value::from(id));
            }
            row
        })
        .collect();
    let warnings = if timed_out {
        vec!["Timeout limit was reached".into()]
    } else {
        Vec::new()
    };
    Ok(PipelineResult {
        rows,
        total,
        cursor,
        warnings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn args(a: &[&str]) -> Vec<Bytes> {
        a.iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }
    fn rows() -> Vec<Entry> {
        [("a", 1), ("a", 3), ("b", 8)]
            .into_iter()
            .map(|(g, n)| Entry {
                id: None,
                row: Row::from([("g".into(), json!(g)), ("n".into(), json!(n))]),
            })
            .collect()
    }
    fn process(a: &[&str]) -> PipelineResult {
        run(
            rows(),
            ["g".into(), "n".into()].into_iter().collect(),
            &args(a),
            &BTreeMap::new(),
            &Config::default(),
            0,
            false,
            false,
            None,
        )
        .unwrap()
    }
    #[test]
    fn stages_are_ordered_and_reentrant() {
        let before = process(&[
            "LIMIT", "0", "1", "GROUPBY", "1", "@g", "REDUCE", "SUM", "1", "@n", "AS", "s",
        ]);
        assert_eq!(before.rows.len(), 1);
        assert_eq!(before.rows[0]["s"].as_f64(), Some(1.0));
        let after = process(&[
            "GROUPBY", "1", "@g", "REDUCE", "SUM", "1", "@n", "AS", "s", "SORTBY", "2", "@s",
            "DESC", "LIMIT", "0", "1", "APPLY", "@s * 2", "AS", "double", "GROUPBY", "0", "REDUCE",
            "SUM", "1", "@double", "AS", "total",
        ]);
        assert_eq!(after.rows.len(), 1);
        assert_eq!(after.rows[0]["total"].as_f64(), Some(16.0));
    }
    #[test]
    fn collect_distinct_keeps_the_best_sort_representative() {
        let result = process(&[
            "GROUPBY", "0", "REDUCE", "COLLECT", "11", "FIELDS", "1", "@g", "DISTINCT", "SORTBY",
            "2", "@n", "DESC", "LIMIT", "0", "2", "AS", "records",
        ]);
        assert_eq!(result.rows.len(), 1);
        let records = result.rows[0]["records"].as_array().unwrap();
        let plain: Vec<Value> = records
            .iter()
            .map(|v| Value::Object(super::super::value::json_object(v).unwrap().clone()))
            .collect();
        assert_eq!(plain, vec![json!({"g":"b"}), json!({"g":"a"})]);
    }
    #[test]
    fn reducers_and_reservoir_are_real() {
        let result = process(&[
            "GROUPBY",
            "0",
            "REDUCE",
            "COUNT",
            "0",
            "AS",
            "count",
            "REDUCE",
            "COUNT_DISTINCT",
            "1",
            "@g",
            "AS",
            "distinct",
            "REDUCE",
            "COUNT_DISTINCTISH",
            "1",
            "@g",
            "AS",
            "estimate",
            "REDUCE",
            "STDDEV",
            "1",
            "@n",
            "AS",
            "stddev",
            "REDUCE",
            "QUANTILE",
            "2",
            "@n",
            "0.5",
            "AS",
            "median",
            "REDUCE",
            "RANDOM_SAMPLE",
            "2",
            "@n",
            "2",
            "AS",
            "sample",
        ]);
        assert_eq!(result.rows.len(), 1);
        let row = &result.rows[0];
        assert_eq!(row["count"].as_f64(), Some(3.0));
        assert_eq!(row["distinct"].as_f64(), Some(2.0));
        assert_eq!(row["estimate"].as_f64(), Some(2.0));
        assert_eq!(row["median"].as_f64(), Some(3.0));
        assert!((row["stddev"].as_f64().unwrap() - 13.0_f64.sqrt()).abs() < 1e-12);
        let sample = row["sample"].as_array().unwrap();
        assert_eq!(sample.len(), 2);
        assert!(
            sample
                .iter()
                .all(|v| v.as_f64().is_some_and(|n| [1.0, 3.0, 8.0].contains(&n)))
        );
        assert_ne!(sample[0], sample[1]);
    }
    #[test]
    fn empty_input_keeps_one_global_group_but_no_keyed_groups() {
        let global = process(&[
            "FILTER", "@n < 0", "GROUPBY", "0", "REDUCE", "COUNT", "0", "AS", "count", "REDUCE",
            "SUM", "1", "@n", "AS", "sum", "REDUCE", "QUANTILE", "2", "@n", "0.5", "AS", "median",
            "REDUCE", "COLLECT", "3", "FIELDS", "1", "@g", "AS", "records",
        ]);
        assert_eq!(global.rows.len(), 1);
        let row = &global.rows[0];
        assert_eq!(row["count"].as_f64(), Some(0.0));
        assert_eq!(row["sum"].as_f64(), Some(0.0));
        assert!(row["median"].is_null());
        assert!(row["records"].as_array().unwrap().is_empty());
        let keyed = process(&[
            "FILTER", "@n < 0", "GROUPBY", "1", "@g", "REDUCE", "COUNT", "0", "AS", "count",
        ]);
        assert!(keyed.rows.is_empty());
    }
    #[test]
    fn preparation_preserves_binary_parameters_and_cursor_count() {
        let mut a = args(&["PARAMS", "2", "blob"]);
        a.push(Bytes::from_static(&[0, 255, 0]));
        a.extend(args(&[
            "DIALECT",
            "2",
            "LOAD",
            "3",
            "@g",
            "AS",
            "group",
            "WITHCURSOR",
            "COUNT",
            "1",
            "MAXIDLE",
            "9999999",
        ]));
        let prepared = prepare(&a, &Config::default()).unwrap();
        assert_eq!(prepared.options.params["blob"], vec![0, 255, 0]);
        let result = process(&["WITHCURSOR", "COUNT", "1", "MAXIDLE", "9999999"]);
        let cursor = result.cursor.unwrap();
        assert_eq!(cursor.count, 1);
    }
    #[test]
    fn unavailable_fields_and_invalid_reducers_are_errors() {
        let available = ["g".into(), "n".into()].into_iter().collect();
        assert!(
            run(
                rows(),
                available,
                &args(&["APPLY", "@unloaded + 1", "AS", "x"]),
                &BTreeMap::new(),
                &Config::default(),
                0,
                false,
                false,
                None
            )
            .is_err()
        );
        let available = ["g".into(), "n".into()].into_iter().collect();
        assert!(
            run(
                rows(),
                available,
                &args(&["GROUPBY", "0", "REDUCE", "QUANTILE", "2", "@n", "1.1"]),
                &BTreeMap::new(),
                &Config::default(),
                0,
                false,
                false,
                None
            )
            .is_err()
        );
    }
}
