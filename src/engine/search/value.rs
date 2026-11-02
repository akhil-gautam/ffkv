//! Internal values preserve RESP byte strings without confusing them with user JSON objects.
use serde_json::{Map, Value};
use std::borrow::Cow;

const BYTES: &str = "\0ffkv.search.bytes";
const OBJECT: &str = "\0ffkv.search.json";
const PROJECTION: &str = "\0ffkv.search.projection";

pub(super) fn from_bytes(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(s) => Value::String(s.to_owned()),
        Err(_) => Value::Object(Map::from_iter([(
            BYTES.to_owned(),
            Value::Array(bytes.iter().map(|b| Value::from(*b)).collect()),
        )])),
    }
}
pub(super) fn from_owned_bytes(bytes: Vec<u8>) -> Value {
    match String::from_utf8(bytes) {
        Ok(s) => Value::String(s),
        Err(e) => from_bytes(e.as_bytes()),
    }
}
/// Construct a record whose values already have source provenance encoded.
pub(super) fn from_object_encoded(map: Map<String, Value>) -> Value {
    Value::Object(Map::from_iter([(OBJECT.to_owned(), Value::Object(map))]))
}
/// Call only at the source boundary, never on generated pipeline records.
pub(super) fn from_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            from_object_encoded(map.into_iter().map(|(k, v)| (k, from_json(v))).collect())
        }
        Value::Array(values) => Value::Array(values.into_iter().map(from_json).collect()),
        scalar => scalar,
    }
}
/// Keep logical evaluation independent from protocol projection formatting.
pub(super) fn projection(values: Vec<Value>, dialect: u32, root: bool) -> Value {
    let first = values.first().unwrap_or(&Value::Null);
    let string = if root {
        first.to_string()
    } else if dialect < 3 {
        match first {
            Value::String(s) => s.clone(),
            value => value.to_string(),
        }
    } else {
        serde_json::to_string(&values).expect("JSON values always serialize")
    };
    let logical = from_json(first.clone());
    let native = if root {
        from_json(values.into_iter().next().unwrap_or(Value::Null))
    } else {
        Value::Array(values.into_iter().map(from_json).collect())
    };
    Value::Object(Map::from_iter([(
        PROJECTION.to_owned(),
        Value::Array(vec![logical, Value::String(string), native]),
    )]))
}
fn projection_parts(value: &Value) -> Option<&[Value]> {
    let map = value.as_object()?;
    if map.len() != 1 {
        return None;
    }
    let parts = map.get(PROJECTION)?.as_array()?;
    if parts.len() != 3 {
        return None;
    }
    Some(parts)
}
pub(super) fn logical(value: &Value) -> &Value {
    projection_parts(value)
        .and_then(|parts| parts.first())
        .unwrap_or(value)
}
pub(super) fn projection_string(value: &Value) -> Option<&str> {
    projection_parts(value)?.get(1)?.as_str()
}
pub(super) fn projection_native(value: &Value) -> Option<&Value> {
    projection_parts(value)?.get(2)
}
pub(super) fn bytes(value: &Value) -> Option<Cow<'_, [u8]>> {
    let value = logical(value);
    match value {
        Value::String(s) => Some(Cow::Borrowed(s.as_bytes())),
        Value::Object(map) if map.len() == 1 => {
            let values = map.get(BYTES)?.as_array()?;
            let bytes = values
                .iter()
                .map(|v| v.as_u64().filter(|n| *n <= 255).map(|n| n as u8))
                .collect::<Option<Vec<_>>>()?;
            Some(Cow::Owned(bytes))
        }
        _ => None,
    }
}
/// The returned source map's keys are literal user keys. Decode its values recursively,
/// but do not run tag recognition on the map itself a second time.
pub(super) fn json_object(value: &Value) -> Option<&Map<String, Value>> {
    let value = logical(value);
    let map = value.as_object()?;
    if map.len() != 1 {
        return None;
    }
    map.get(OBJECT)?.as_object()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn byte_strings_preserve_all_octets() {
        let bytes = [0, 255, 128, b'x'];
        let v = from_bytes(&bytes);
        assert_eq!(super::bytes(&v).unwrap().as_ref(), bytes);
    }
    #[test]
    fn source_objects_cannot_impersonate_binary_tags() {
        let original = json!({"\0ffkv.search.bytes":[255],"nested":{"\0ffkv.search.json":{"x":1}}});
        let encoded = from_json(original.clone());
        assert!(bytes(&encoded).is_none());
        let object = json_object(&encoded).unwrap();
        assert_eq!(object[BYTES], json!([255]));
        assert!(json_object(&object["nested"]).is_some());
    }
    #[test]
    fn projections_keep_logical_string_and_native_representations_distinct() {
        let old = projection(vec![json!("hello"), json!("world")], 1, false);
        assert_eq!(logical(&old), &json!("hello"));
        assert_eq!(projection_string(&old), Some("hello"));
        assert_eq!(projection_native(&old), Some(&json!(["hello", "world"])));
        let new = projection(vec![json!(4), json!(8)], 3, false);
        assert_eq!(projection_string(&new), Some("[4,8]"));
        assert_eq!(projection_native(&new), Some(&json!([4, 8])));
        let root = projection(vec![json!({"x":1})], 3, true);
        assert_eq!(projection_string(&root), Some("{\"x\":1}"));
        assert!(json_object(projection_native(&root).unwrap()).is_some());
    }
}
