use super::ApiError;
use crate::protocol::Resp;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::{Value, json};
pub const MAX_RESPONSE: usize = 8 * 1024 * 1024;
const SAFE: i64 = 9_007_199_254_740_991;
pub fn integer(n: i64) -> Value {
    if (-SAFE..=SAFE).contains(&n) {
        json!(n)
    } else {
        json!(n.to_string())
    }
}
pub fn double(n: f64) -> Value {
    if n.is_finite() {
        json!(n)
    } else if n.is_nan() {
        json!("NaN")
    } else if n.is_sign_positive() {
        json!("Infinity")
    } else {
        json!("-Infinity")
    }
}
pub fn bytes(data: &[u8], reveal: bool, sensitive: bool) -> Value {
    let redacted = || json!({"base64":"","utf8":null,"bytes":data.len(),"redacted":true});
    if !reveal {
        return redacted();
    }
    let json_container = data
        .iter()
        .find(|b| !b.is_ascii_whitespace())
        .is_some_and(|b| matches!(*b, b'{' | b'['));
    let sanitized = if !sensitive && json_container {
        // A valid but deeply nested JSON document can exceed serde's recursion
        // limit. Never turn a sanitizer failure into an unmasked reveal.
        let Ok(mut value) = serde_json::from_slice::<Value>(data) else {
            return redacted();
        };
        mask(&mut value, 0);
        let Ok(value) = serde_json::to_vec(&value) else {
            return redacted();
        };
        Some(value)
    } else {
        None
    };
    let value = sanitized.as_deref().unwrap_or(data);
    json!({"base64":STANDARD.encode(value),"utf8":std::str::from_utf8(value).ok(),"bytes":value.len(),"redacted":sanitized.as_ref().is_some_and(|v|v.as_slice()!=data)})
}
fn mask(value: &mut Value, depth: usize) {
    if depth > 32 {
        *value = json!("[redacted]");
        return;
    }
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                let key = key
                    .chars()
                    .filter(|c| c.is_ascii_alphanumeric())
                    .map(|c| c.to_ascii_lowercase())
                    .collect::<String>();
                if [
                    "password",
                    "passwd",
                    "passphrase",
                    "secret",
                    "token",
                    "authorization",
                    "credential",
                    "cookie",
                    "apikey",
                    "privatekey",
                    "accesskey",
                ]
                .iter()
                .any(|p| key.contains(p))
                    || key == "pwd"
                {
                    *value = json!("[redacted]");
                } else {
                    mask(value, depth + 1);
                }
            }
        }
        Value::Array(values) => {
            for v in values {
                mask(v, depth + 1);
            }
        }
        _ => {}
    }
}
pub fn response(resp: &Resp, reveal: bool, sensitive: bool) -> Result<Value, ApiError> {
    let mut budget = MAX_RESPONSE / 2;
    convert(resp, reveal, sensitive, 0, &mut budget)
}
fn charge(budget: &mut usize, size: usize) -> Result<(), ApiError> {
    *budget = budget
        .checked_sub(size)
        .ok_or_else(|| ApiError::bad("response exceeds management limit; use a smaller page"))?;
    Ok(())
}
fn convert(
    resp: &Resp,
    reveal: bool,
    sensitive: bool,
    depth: usize,
    budget: &mut usize,
) -> Result<Value, ApiError> {
    if depth > 32 {
        return Err(ApiError::bad("response nesting exceeds limit"));
    }
    charge(budget, 128)?;
    Ok(match resp {
        Resp::Simple(s) => json!({"type":"simple","value":s}),
        Resp::Status(b) => {
            charge(budget, if reveal { b.len().saturating_mul(2) } else { 0 })?;
            let view = bytes(b, reveal, sensitive);
            if !reveal {
                json!({"type":"simple","value":"[redacted]"})
            } else if view["utf8"].is_string() {
                json!({"type":"simple","value":view["utf8"]})
            } else {
                json!({"type":"bulk","value":view})
            }
        }
        Resp::Verbatim { format, value } => {
            charge(
                budget,
                if reveal {
                    value.len().saturating_mul(2)
                } else {
                    0
                },
            )?;
            json!({"type":"verbatim","format":String::from_utf8_lossy(format),"value":bytes(value,reveal,sensitive)})
        }
        Resp::Error(_) => json!({"type":"error","message":"command failed"}),
        Resp::Integer(n) => json!({"type":"integer","value":integer(*n)}),
        Resp::Boolean(value) => json!({"type":"boolean","value":value}),
        Resp::Unsigned(value) => {
            json!({"type":"integer","value":if *value <= SAFE as u64 { json!(value) } else { json!(value.to_string()) }})
        }
        Resp::BigNumber(value) => {
            charge(budget, value.len().saturating_mul(2))?;
            json!({"type":"integer","value":String::from_utf8_lossy(value)})
        }
        Resp::InfoField(field, value) => {
            charge(
                budget,
                if reveal {
                    field.len().saturating_mul(2)
                } else {
                    0
                },
            )?;
            json!({"type":"map","value":[[{"type":"bulk","value":bytes(field,reveal,sensitive)},convert(value,reveal,sensitive,depth+1,budget)?]]})
        }
        Resp::Bulk(None) | Resp::Array(None) => json!({"type":"null"}),
        Resp::Bulk(Some(b)) => {
            charge(budget, if reveal { b.len().saturating_mul(2) } else { 0 })?;
            json!({"type":"bulk","value":bytes(b,reveal,sensitive)})
        }
        Resp::Array(Some(v)) | Resp::Set(v) => {
            let values = v
                .iter()
                .map(|r| convert(r, reveal, sensitive, depth + 1, budget))
                .collect::<Result<Vec<_>, _>>()?;
            json!({"type":if matches!(resp,Resp::Set(_)){"set"}else{"array"},"value":values})
        }
        Resp::Map(v) => {
            let values = v
                .iter()
                .map(|(k, v)| {
                    Ok([
                        convert(k, reveal, sensitive, depth + 1, budget)?,
                        convert(v, reveal, sensitive, depth + 1, budget)?,
                    ])
                })
                .collect::<Result<Vec<_>, ApiError>>()?;
            json!({"type":"map","value":values})
        }
        Resp::Double(n) => json!({"type":"double","value":double(*n)}),
        Resp::Scored(v) => {
            let mut values = Vec::new();
            for (member, score) in v {
                charge(
                    budget,
                    (if reveal {
                        member.len().saturating_mul(2)
                    } else {
                        0
                    })
                    .saturating_add(128),
                )?;
                values
                    .push(json!({"member":bytes(member,reveal,sensitive),"score":double(*score)}));
            }
            json!({"type":"scored","value":values})
        }
        Resp::VectorResults(entries) => {
            let mut values = Vec::with_capacity(entries.len());
            for (member, score, attr) in entries {
                let size = member.len().saturating_add(
                    attr.as_ref()
                        .and_then(|value| value.as_ref())
                        .map_or(0, |value| value.len()),
                );
                charge(
                    budget,
                    256usize.saturating_add(if reveal { size.saturating_mul(2) } else { 0 }),
                )?;
                let mut value = json!({"member":bytes(member,reveal,sensitive)});
                if let Some(score) = score {
                    value["score"] = double(*score);
                }
                if let Some(attr) = attr {
                    value["attributes"] = attr
                        .as_ref()
                        .map_or(Value::Null, |value| bytes(value, reveal, sensitive));
                }
                values.push(value);
            }
            json!({"type":"vector-results","value":values})
        }
    })
}
/// Preserve integers in any ancillary JSON (metrics, storage, timestamps) too.
pub fn js_safe(value: &mut Value) {
    match value {
        Value::Number(n) => {
            if n.as_u64().is_some_and(|n| n > SAFE as u64) || n.as_i64().is_some_and(|n| n < -SAFE)
            {
                *value = Value::String(n.to_string());
            }
        }
        Value::Array(v) => {
            for x in v {
                js_safe(x);
            }
        }
        Value::Object(v) => {
            for x in v.values_mut() {
                js_safe(x);
            }
        }
        _ => {}
    }
}
pub fn text(resp: &Resp) -> Option<&[u8]> {
    match resp {
        Resp::Bulk(Some(b)) | Resp::Status(b) | Resp::Verbatim { value: b, .. } => Some(b),
        Resp::Simple(s) => Some(s.as_bytes()),
        _ => None,
    }
}
pub fn int(resp: &Resp) -> Option<i64> {
    match resp {
        Resp::Integer(n) => Some(*n),
        _ => None,
    }
}
pub fn list(resp: &Resp) -> Option<&[Resp]> {
    match resp {
        Resp::Array(Some(v)) | Resp::Set(v) => Some(v),
        _ => None,
    }
}
pub fn field<'a>(resp: &'a Resp, name: &str) -> Option<&'a Resp> {
    if let Resp::Map(fields) = resp {
        fields
            .iter()
            .find(|(k, _)| text(k) == Some(name.as_bytes()))
            .map(|(_, v)| v)
    } else {
        let fields = list(resp)?;
        fields
            .as_chunks::<2>()
            .0
            .iter()
            .find(|pair| text(&pair[0]) == Some(name.as_bytes()))
            .map(|pair| &pair[1])
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn masks_json_in_base64_too() {
        let view = bytes(
            br#"{"token":"do-not-leak","nested":{"password":"hidden"},"ok":1}"#,
            true,
            false,
        );
        let decoded = STANDARD.decode(view["base64"].as_str().unwrap()).unwrap();
        assert!(!String::from_utf8(decoded).unwrap().contains("do-not-leak"));
        assert!(view["redacted"].as_bool().unwrap());
    }
    #[test]
    fn binary_and_large_numbers() {
        let v = bytes(&[255, 0, 128], true, false);
        assert!(v["utf8"].is_null());
        assert_eq!(
            STANDARD.decode(v["base64"].as_str().unwrap()).unwrap(),
            vec![255, 0, 128]
        );
        assert!(integer(i64::MAX).is_string());
        assert_eq!(double(f64::INFINITY), "Infinity");
    }
}

#[cfg(test)]
mod sanitization_limits {
    use super::*;
    #[test]
    fn deeply_nested_json_never_falls_back_to_unmasked_bytes() {
        let payload = format!(
            "{}{{\"password\":\"must-not-escape\"}}{}",
            "[".repeat(200),
            "]".repeat(200)
        );
        let view = bytes(payload.as_bytes(), true, false);
        assert_eq!(view["redacted"], true);
        assert_eq!(view["base64"], "");
        assert!(view["utf8"].is_null());
        assert_eq!(view["bytes"], payload.len());
    }
}
