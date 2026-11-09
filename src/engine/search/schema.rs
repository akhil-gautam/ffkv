use super::model::*;
use bytes::Bytes;
use std::collections::BTreeSet;
fn word(b: &Bytes) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "Invalid UTF-8 schema argument".into())
}
fn upper(b: &Bytes) -> Result<String, String> {
    Ok(word(b)?.to_ascii_uppercase())
}
fn count(b: &Bytes) -> Result<usize, String> {
    word(b)?
        .parse()
        .map_err(|_| "Expected nonnegative integer".into())
}
fn take<'a>(a: &'a [Bytes], i: &mut usize) -> Result<&'a Bytes, String> {
    let v = a.get(*i).ok_or("Missing argument")?;
    *i += 1;
    Ok(v)
}
pub fn language(s: &str) -> bool {
    let s = s.to_ascii_lowercase();
    (super::text::CHINESE_SEGMENTATION && s == "chinese")
        || matches!(
            s.as_str(),
            "arabic"
                | "armenian"
                | "basque"
                | "catalan"
                | "danish"
                | "dutch"
                | "english"
                | "finnish"
                | "french"
                | "german"
                | "greek"
                | "hindi"
                | "hungarian"
                | "indonesian"
                | "irish"
                | "italian"
                | "lithuanian"
                | "malay"
                | "nepali"
                | "norwegian"
                | "portuguese"
                | "romanian"
                | "russian"
                | "serbian"
                | "spanish"
                | "swedish"
                | "tagalog"
                | "tamil"
                | "turkish"
                | "yiddish"
        )
}
pub fn create(a: &[Bytes], now: i64) -> Result<(Schema, bool), String> {
    let name = word(a.get(1).ok_or("Missing index name")?)?.to_owned();
    if name.is_empty() {
        return Err("Index name must not be empty".into());
    }
    let mut s=Schema{name,json:false,prefixes:vec![Bytes::new()],fields:vec![],filter:None,language:"english".into(),language_field:None,score:1.,score_field:None,payload_field:None,stopwords:"a is the an and are as at be but by for if in into it no not of on or such that their then there these they this to was will with".split_whitespace().map(str::to_owned).collect(),nooffsets:false,nohl:false,nofields:false,nofreqs:false,maxtextfields:false,wide_text_fields:false,indexall:false,temporary_ms:None,deadline:None};
    let mut skip = false;
    let mut i = 2;
    while i < a.len() {
        let op = upper(take(a, &mut i)?)?;
        match op.as_str() {
            "SCHEMA" => {
                s.fields = fields(&a[i..], s.json)?;
                break;
            }
            "ON" => {
                s.json = match upper(take(a, &mut i)?)?.as_str() {
                    "HASH" => false,
                    "JSON" => true,
                    _ => return Err("ON must be HASH or JSON".into()),
                }
            }
            "PREFIX" => {
                let n = count(take(a, &mut i)?)?;
                if n > 1024 || n > a.len().saturating_sub(i) {
                    return Err("Invalid PREFIX count".into());
                }
                s.prefixes = a[i..i + n]
                    .iter()
                    .map(|x| Bytes::copy_from_slice(x))
                    .collect();
                i += n;
                if n == 0 {
                    s.prefixes.push(Bytes::new())
                }
            }
            "FILTER" => {
                let v = word(take(a, &mut i)?)?.to_owned();
                super::expression::validate(&v)?;
                s.filter = Some(v)
            }
            "LANGUAGE" => {
                let v = word(take(a, &mut i)?)?.to_ascii_lowercase();
                if !language(&v) {
                    return Err("Unsupported language".into());
                }
                s.language = v
            }
            "LANGUAGE_FIELD" => s.language_field = Some(word(take(a, &mut i)?)?.to_owned()),
            "SCORE" => {
                let v = word(take(a, &mut i)?)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid score")?;
                if !v.is_finite() || !(0.0..=1.0).contains(&v) {
                    return Err("Score must be between 0 and 1".into());
                }
                s.score = v
            }
            "SCORE_FIELD" => s.score_field = Some(word(take(a, &mut i)?)?.to_owned()),
            "PAYLOAD_FIELD" => s.payload_field = Some(word(take(a, &mut i)?)?.to_owned()),
            "MAXTEXTFIELDS" => s.maxtextfields = true,
            "NOOFFSETS" => {
                s.nooffsets = true;
                s.nohl = true
            }
            "NOHL" => s.nohl = true,
            "NOFIELDS" => s.nofields = true,
            "NOFREQS" => s.nofreqs = true,
            "SKIPINITIALSCAN" => skip = true,
            "TEMPORARY" => {
                let v = word(take(a, &mut i)?)?
                    .parse::<f64>()
                    .map_err(|_| "Invalid temporary duration")?;
                if !v.is_finite() || v <= 0.0 || v > i64::MAX as f64 / 1000. {
                    return Err("Invalid temporary duration".into());
                }
                let ms = (v * 1000.) as i64;
                s.temporary_ms = Some(ms);
                s.deadline = Some(now.saturating_add(ms))
            }
            "STOPWORDS" => {
                let n = count(take(a, &mut i)?)?;
                if n > 65536 || n > a.len().saturating_sub(i) {
                    return Err("Invalid stopword count".into());
                }
                s.stopwords = a[i..i + n]
                    .iter()
                    .map(|b| word(b).map(str::to_owned))
                    .collect::<Result<_, _>>()?;
                i += n
            }
            "INDEXALL" => {
                s.indexall = match upper(take(a, &mut i)?)?.as_str() {
                    "ENABLE" => true,
                    "DISABLE" => false,
                    _ => return Err("INDEXALL requires ENABLE or DISABLE".into()),
                }
            }
            _ => return Err(format!("Unknown argument {op}")),
        }
    }
    if s.fields.is_empty() {
        return Err("Schema must contain at least one field".into());
    }
    s.wide_text_fields = s.maxtextfields
        || s.fields
            .iter()
            .filter(|f| matches!(f.kind, FieldKind::Text))
            .count()
            >= 32;
    Ok((s, skip))
}
pub fn fields(a: &[Bytes], json: bool) -> Result<Vec<Field>, String> {
    let mut out = Vec::new();
    let mut names = BTreeSet::new();
    let mut i = 0;
    while i < a.len() {
        if out.len() >= 1024 {
            return Err("Too many schema attributes".into());
        }
        let source = word(take(a, &mut i)?)?.to_owned();
        if source.is_empty() {
            return Err("Field name cannot be empty".into());
        }
        if json {
            super::super::json::JsonValue::search_validate_path(&source)
                .map_err(|_| "Invalid JSONPath in schema")?;
        }
        let mut name = source.clone();
        if a.get(i).is_some_and(|b| b.eq_ignore_ascii_case(b"AS")) {
            i += 1;
            name = word(take(a, &mut i)?)?.to_owned()
        }
        if name.is_empty() || !names.insert(name.clone()) {
            return Err("Duplicate or empty field attribute".into());
        }
        let kind = match upper(take(a, &mut i)?)?.as_str() {
            "TEXT" => FieldKind::Text,
            "TAG" => FieldKind::Tag,
            "NUMERIC" => FieldKind::Numeric,
            "GEO" => FieldKind::Geo,
            "GEOSHAPE" => FieldKind::GeoShape,
            "VECTOR" => FieldKind::Vector,
            _ => return Err("Invalid field type".into()),
        };
        let mut f = Field {
            source,
            name,
            kind,
            sortable: false,
            unf: json,
            noindex: false,
            nostem: false,
            weight: 1.,
            phonetic: None,
            separator: if json { None } else { Some(',') },
            case_sensitive: false,
            index_empty: false,
            index_missing: false,
            suffix: false,
            vector: None,
            spherical: true,
        };
        if matches!(f.kind, FieldKind::Vector) {
            let algorithm = word(take(a, &mut i)?)?;
            let n = count(take(a, &mut i)?)?;
            if n % 2 != 0 || n > a.len().saturating_sub(i) {
                return Err("Invalid vector argument count".into());
            }
            f.vector = Some(super::vector::parse_config(algorithm, &a[i..i + n])?);
            i += n;
        } else if matches!(f.kind, FieldKind::GeoShape) {
            if a.get(i).is_some_and(|b| b.eq_ignore_ascii_case(b"FLAT")) {
                i += 1;
                f.spherical = false
            } else if a
                .get(i)
                .is_some_and(|b| b.eq_ignore_ascii_case(b"SPHERICAL"))
            {
                i += 1
            }
        }
        while let Some(b) = a.get(i) {
            match upper(b)?.as_str() {
                "SORTABLE" => {
                    if matches!(f.kind, FieldKind::Vector | FieldKind::GeoShape) {
                        return Err("Field type does not support SORTABLE".into());
                    }
                    i += 1;
                    f.sortable = true;
                    if a.get(i).is_some_and(|b| b.eq_ignore_ascii_case(b"UNF")) {
                        f.unf = true;
                        i += 1
                    }
                }
                "NOINDEX" => {
                    i += 1;
                    f.noindex = true
                }
                "INDEXMISSING" => {
                    i += 1;
                    f.index_missing = true
                }
                "INDEXEMPTY" => {
                    if !matches!(f.kind, FieldKind::Text | FieldKind::Tag) {
                        return Err("INDEXEMPTY requires TEXT or TAG".into());
                    }
                    i += 1;
                    f.index_empty = true
                }
                "NOSTEM" => {
                    if !matches!(f.kind, FieldKind::Text) {
                        return Err("NOSTEM requires TEXT".into());
                    }
                    i += 1;
                    f.nostem = true
                }
                "WEIGHT" => {
                    if !matches!(f.kind, FieldKind::Text) {
                        return Err("WEIGHT requires TEXT".into());
                    }
                    i += 1;
                    f.weight = word(take(a, &mut i)?)?
                        .parse()
                        .map_err(|_| "Invalid weight")?;
                    if !f.weight.is_finite() || f.weight <= 0. {
                        return Err("Invalid weight".into());
                    }
                }
                "PHONETIC" => {
                    if !matches!(f.kind, FieldKind::Text) {
                        return Err("PHONETIC requires TEXT".into());
                    }
                    i += 1;
                    let v = word(take(a, &mut i)?)?;
                    if !matches!(v, "dm:en" | "dm:fr" | "dm:pt" | "dm:es") {
                        return Err("Invalid phonetic matcher".into());
                    }
                    f.phonetic = Some(v.into())
                }
                "SEPARATOR" => {
                    if !matches!(f.kind, FieldKind::Tag) {
                        return Err("SEPARATOR requires TAG".into());
                    }
                    i += 1;
                    let mut chars = word(take(a, &mut i)?)?.chars();
                    f.separator = chars.next();
                    if f.separator.is_none() || chars.next().is_some() {
                        return Err("TAG separator must be one character".into());
                    }
                }
                "CASESENSITIVE" => {
                    if !matches!(f.kind, FieldKind::Tag) {
                        return Err("CASESENSITIVE requires TAG".into());
                    }
                    i += 1;
                    f.case_sensitive = true
                }
                "WITHSUFFIXTRIE" => {
                    if !matches!(f.kind, FieldKind::Tag | FieldKind::Text) {
                        return Err("WITHSUFFIXTRIE requires TEXT or TAG".into());
                    }
                    i += 1;
                    f.suffix = true
                }
                _ => break,
            }
        }
        if f.noindex && f.index_missing {
            return Err("NOINDEX cannot be combined with INDEXMISSING".into());
        }
        out.push(f);
    }
    if out
        .iter()
        .filter(|f| matches!(f.kind, FieldKind::Text))
        .count()
        > 128
    {
        return Err("Too many TEXT fields".into());
    }
    Ok(out)
}
