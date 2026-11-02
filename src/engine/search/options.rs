use super::{
    model::{Config, FieldKind, Index},
    query,
};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone)]
pub(super) struct Projection {
    pub field: String,
    pub alias: String,
}
pub(super) struct Highlight {
    pub fields: Vec<String>,
    pub open: String,
    pub close: String,
}
pub(super) struct Summary {
    pub fields: Vec<String>,
    pub fragments: usize,
    pub words: usize,
    pub separator: String,
}
pub(super) struct SearchOptions {
    pub query: query::Options,
    pub no_content: bool,
    pub scores: bool,
    pub payloads: bool,
    pub sortkeys: bool,
    pub explain: bool,
    pub projection: Option<Vec<Projection>>,
    pub highlight: Option<Highlight>,
    pub summary: Option<Summary>,
    pub keys: Option<BTreeSet<Vec<u8>>>,
    pub filters: Vec<String>,
    pub sort: Option<(String, bool)>,
    pub offset: usize,
    pub count: usize,
    pub expand: bool,
}
fn arg(a: &[Bytes], p: usize) -> Result<&str, String> {
    a.get(p)
        .ok_or_else(|| "Missing argument".to_string())
        .and_then(|b| super::string(b))
}
fn unsigned(a: &[Bytes], p: usize) -> Result<usize, String> {
    arg(a, p)?
        .parse()
        .map_err(|_| "Expected a non-negative integer".into())
}
fn positive(a: &[Bytes], p: usize) -> Result<usize, String> {
    let n = unsigned(a, p)?;
    if n == 0 {
        Err("Expected a positive integer".into())
    } else {
        Ok(n)
    }
}
fn end(a: &[Bytes], p: usize, n: usize) -> Result<usize, String> {
    p.checked_add(n)
        .filter(|e| *e <= a.len())
        .ok_or_else(|| "Argument count exceeds supplied arguments".into())
}
fn fields(a: &[Bytes], p: &mut usize) -> Result<Vec<String>, String> {
    let n = positive(a, *p + 1)?;
    let e = end(a, *p + 2, n)?;
    let out = a[*p + 2..e]
        .iter()
        .map(|b| super::string(b).map(str::to_owned))
        .collect::<Result<_, _>>()?;
    *p = e;
    Ok(out)
}
pub(super) fn prepare(
    index: &Index,
    a: &[Bytes],
    config: &Config,
    resp3: bool,
) -> Result<SearchOptions, String> {
    let mut options = parse(a, config)?;
    options.query.filters = std::mem::take(&mut options.filters);
    options.query.keys = options.keys.as_ref().map(|keys| {
        keys.iter()
            .filter_map(|key| index.keys.get(key.as_slice()).copied())
            .collect()
    });
    if options.expand && (!index.schema.json || !resp3) {
        return Err("FORMAT EXPAND is supported only with JSON indexes and RESP3".into());
    }
    if index.schema.json && (options.highlight.is_some() || options.summary.is_some()) {
        return Err("HIGHLIGHT/SUMMARIZE is not supported with JSON indexes".into());
    }
    if index.schema.nohl && (options.highlight.is_some() || options.summary.is_some()) {
        return Err("Highlighting is not supported by this index".into());
    }
    for field in options
        .highlight
        .iter()
        .flat_map(|h| &h.fields)
        .chain(options.summary.iter().flat_map(|s| &s.fields))
    {
        if !index
            .schema
            .fields
            .iter()
            .any(|f| f.name == *field && matches!(f.kind, FieldKind::Text))
        {
            return Err(format!("Unknown TEXT field '{field}'"));
        }
    }
    if index.schema.json
        && let Some(projection) = &options.projection
    {
        for p in projection {
            let path = index
                .schema
                .fields
                .iter()
                .find(|f| f.name == p.field)
                .map_or(p.field.as_str(), |f| f.source.as_str());
            super::super::json::JsonValue::search_validate_path(path)
                .map_err(|_| "Invalid JSONPath".to_string())?;
        }
    }
    Ok(options)
}
pub(super) fn parse(a: &[Bytes], config: &Config) -> Result<SearchOptions, String> {
    let mut q = query::Options {
        dialect: config.default_dialect as u32,
        timeout_ms: config.timeout_ms,
        ..Default::default()
    };
    if let Some(v) = config.extras.get("DEFAULT_SCORER") {
        q.scorer = v.clone()
    }
    if let Some(v) = config.extras.get("BM25STD_TANH_FACTOR") {
        q.bm25std_tanh_factor = v.parse().unwrap_or(4.)
    }
    let mut o = SearchOptions {
        query: q,
        no_content: false,
        scores: false,
        payloads: false,
        sortkeys: false,
        explain: false,
        projection: None,
        highlight: None,
        summary: None,
        keys: None,
        filters: Vec::new(),
        sort: None,
        offset: 0,
        count: 10,
        expand: false,
    };
    let mut seen = BTreeSet::new();
    let mut p = 0;
    while p < a.len() {
        let key = arg(a, p)?.to_ascii_uppercase();
        if !matches!(key.as_str(), "FILTER" | "GEOFILTER") && !seen.insert(key.clone()) {
            return Err(format!("Duplicate option {key}"));
        }
        match key.as_str() {
            "NOCONTENT" => {
                o.no_content = true;
                p += 1
            }
            "WITHSCORES" => {
                o.scores = true;
                p += 1
            }
            "WITHPAYLOADS" => {
                o.payloads = true;
                p += 1
            }
            "WITHSORTKEYS" => {
                o.sortkeys = true;
                p += 1
            }
            "EXPLAINSCORE" => {
                o.explain = true;
                p += 1
            }
            "VERBATIM" => {
                o.query.verbatim = true;
                p += 1
            }
            "NOSTOPWORDS" => {
                o.query.nostopwords = true;
                p += 1
            }
            "INORDER" => {
                o.query.inorder = true;
                p += 1
            }
            "WITHCOUNT" | "WITHOUTCOUNT" => {
                if seen.contains(if key == "WITHCOUNT" {
                    "WITHOUTCOUNT"
                } else {
                    "WITHCOUNT"
                }) {
                    return Err("WITHCOUNT and WITHOUTCOUNT are mutually exclusive".into());
                }
                p += 1
            }
            "DIALECT" => {
                let n = unsigned(a, p + 1)?;
                if !(1..=4).contains(&n) {
                    return Err("DIALECT must be between 1 and 4".into());
                }
                o.query.dialect = n as u32;
                p += 2
            }
            "TIMEOUT" => {
                o.query.timeout_ms = unsigned(a, p + 1)? as u64;
                p += 2
            }
            "SLOP" => {
                o.query.slop = arg(a, p + 1)?
                    .parse::<i64>()
                    .map_err(|_| "Invalid SLOP".to_string())?;
                if o.query.slop < 0 {
                    return Err("SLOP must be non-negative".into());
                }
                p += 2
            }
            "LANGUAGE" => {
                let s = arg(a, p + 1)?;
                if !super::schema::language(s) {
                    return Err("Unsupported language".into());
                }
                o.query.language = s.to_owned();
                p += 2
            }
            "SCORER" => {
                o.query.scorer = arg(a, p + 1)?.to_owned();
                p += 2
            }
            "EXPANDER" => {
                o.query.expander = arg(a, p + 1)?.to_owned();
                p += 2
            }
            "BM25STD_TANH_FACTOR" => {
                let n = arg(a, p + 1)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid BM25STD_TANH_FACTOR".to_string())?;
                if !n.is_finite() || n <= 0. {
                    return Err("BM25STD_TANH_FACTOR must be positive".into());
                }
                o.query.bm25std_tanh_factor = n;
                p += 2
            }
            "PAYLOAD" => {
                o.query.payload = a.get(p + 1).ok_or("Missing PAYLOAD")?.to_vec();
                p += 2
            }
            "PARAMS" => {
                let n = unsigned(a, p + 1)?;
                if n % 2 != 0 {
                    return Err("PARAMS requires name/value pairs".into());
                }
                let e = end(a, p + 2, n)?;
                let mut params = BTreeMap::new();
                for pair in a[p + 2..e].as_chunks::<2>().0 {
                    let name = super::string(&pair[0])?.to_owned();
                    if name.is_empty() || params.insert(name, pair[1].to_vec()).is_some() {
                        return Err("Invalid or duplicate parameter".into());
                    }
                }
                o.query.params = params;
                p = e
            }
            "INKEYS" => {
                let n = unsigned(a, p + 1)?;
                let e = end(a, p + 2, n)?;
                o.keys = Some(a[p + 2..e].iter().map(|b| b.to_vec()).collect());
                p = e
            }
            "INFIELDS" => o.query.fields = fields(a, &mut p)?,
            "RETURN" => {
                let n = unsigned(a, p + 1)?;
                let e = end(a, p + 2, n)?;
                p += 2;
                let mut projection = Vec::new();
                while p < e {
                    let field = arg(a, p)?.to_owned();
                    p += 1;
                    let alias = if p < e && arg(a, p)?.eq_ignore_ascii_case("AS") {
                        if p + 1 >= e {
                            return Err("RETURN AS requires an alias within argument count".into());
                        }
                        let alias = arg(a, p + 1)?.to_owned();
                        p += 2;
                        alias
                    } else {
                        field.clone()
                    };
                    projection.push(Projection { field, alias })
                }
                if n == 0 {
                    o.no_content = true
                }
                o.projection = Some(projection)
            }
            "LIMIT" => {
                o.offset = unsigned(a, p + 1)?;
                o.count = unsigned(a, p + 2)?;
                let n = o.offset.checked_add(o.count).ok_or("LIMIT overflow")?;
                if n > config.max_results {
                    return Err("LIMIT exceeds maximum search results".into());
                }
                if o.count == 0 && o.offset != 0 {
                    return Err("LIMIT offset must be zero when count is zero".into());
                }
                p += 3
            }
            "SORTBY" => {
                let field = arg(a, p + 1)?.trim_start_matches('@').to_owned();
                p += 2;
                let mut asc = true;
                if a.get(p).is_some_and(|b| {
                    b.eq_ignore_ascii_case(b"ASC") || b.eq_ignore_ascii_case(b"DESC")
                }) {
                    asc = a[p].eq_ignore_ascii_case(b"ASC");
                    p += 1
                }
                if a.get(p)
                    .is_some_and(|b| b.eq_ignore_ascii_case(b"WITHCOUNT"))
                {
                    p += 1
                }
                o.sort = Some((field, asc))
            }
            "FILTER" => {
                let field = arg(a, p + 1)?.trim_start_matches('@');
                if field.is_empty() {
                    return Err("FILTER requires a field".into());
                }
                let min = arg(a, p + 2)?;
                let max = arg(a, p + 3)?;
                for bound in [min, max] {
                    let value = bound
                        .strip_prefix('(')
                        .unwrap_or(bound)
                        .parse::<f64>()
                        .map_err(|_| "Invalid numeric filter bound")?;
                    if value.is_nan() {
                        return Err("Invalid numeric filter bound".into());
                    }
                }
                o.filters
                    .push(format!("@{}:[{} {}]", escape_field(field), min, max));
                p += 4
            }
            "GEOFILTER" => {
                let field = arg(a, p + 1)?.trim_start_matches('@');
                if field.is_empty() {
                    return Err("GEOFILTER requires a field".into());
                }
                let lon = arg(a, p + 2)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid longitude")?;
                let lat = arg(a, p + 3)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid latitude")?;
                super::spatial::validate_geo(lon, lat)?;
                let radius = arg(a, p + 4)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid radius")?;
                if !radius.is_finite() || radius < 0. {
                    return Err("Invalid radius".into());
                }
                let unit = arg(a, p + 5)?.to_ascii_lowercase();
                if !matches!(unit.as_str(), "m" | "km" | "mi" | "ft") {
                    return Err("Invalid geographic unit".into());
                }
                o.filters.push(format!(
                    "@{}:[{} {} {} {}]",
                    escape_field(field),
                    lon,
                    lat,
                    radius,
                    unit
                ));
                p += 6
            }
            "HIGHLIGHT" => {
                p += 1;
                let mut h = Highlight {
                    fields: Vec::new(),
                    open: "<b>".into(),
                    close: "</b>".into(),
                };
                let mut sub = BTreeSet::new();
                while p < a.len() {
                    let k = arg(a, p)?.to_ascii_uppercase();
                    if !matches!(k.as_str(), "FIELDS" | "TAGS") {
                        break;
                    }
                    if !sub.insert(k.clone()) {
                        return Err("Duplicate HIGHLIGHT option".into());
                    }
                    if k == "FIELDS" {
                        h.fields = fields(a, &mut p)?
                    } else {
                        h.open = arg(a, p + 1)?.to_owned();
                        h.close = arg(a, p + 2)?.to_owned();
                        p += 3
                    }
                }
                o.highlight = Some(h)
            }
            "SUMMARIZE" => {
                p += 1;
                let mut s = Summary {
                    fields: Vec::new(),
                    fragments: 3,
                    words: 20,
                    separator: "... ".into(),
                };
                let mut sub = BTreeSet::new();
                while p < a.len() {
                    let k = arg(a, p)?.to_ascii_uppercase();
                    if !matches!(k.as_str(), "FIELDS" | "FRAGS" | "LEN" | "SEPARATOR") {
                        break;
                    }
                    if !sub.insert(k.clone()) {
                        return Err("Duplicate SUMMARIZE option".into());
                    }
                    match k.as_str() {
                        "FIELDS" => s.fields = fields(a, &mut p)?,
                        "FRAGS" => {
                            s.fragments = positive(a, p + 1)?;
                            p += 2
                        }
                        "LEN" => {
                            s.words = positive(a, p + 1)?;
                            p += 2
                        }
                        _ => {
                            s.separator = arg(a, p + 1)?.to_owned();
                            p += 2
                        }
                    }
                }
                o.summary = Some(s)
            }
            "FORMAT" => {
                o.expand = match arg(a, p + 1)?.to_ascii_uppercase().as_str() {
                    "STRING" => false,
                    "EXPAND" => true,
                    _ => return Err("FORMAT must be STRING or EXPAND".into()),
                };
                p += 2
            }
            _ => return Err(format!("Unknown argument '{key}'")),
        }
    }
    if o.explain && !o.scores {
        return Err("EXPLAINSCORE requires WITHSCORES".into());
    }
    if !o.query.params.is_empty() && o.query.dialect < 2 {
        return Err("PARAMS requires DIALECT 2 or greater".into());
    }
    o.query.explain_score = o.explain;
    Ok(o)
}
fn escape_field(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        if !c.is_alphanumeric() && c != '_' {
            out.push('\\')
        }
        out.push(c)
    }
    out
}
