use base64::{Engine, engine::general_purpose::STANDARD};
use serde_json::{Value, json};

use crate::client::Frame;

#[derive(Clone, Copy)]
pub enum Format {
    Human,
    Raw,
    Json,
}

pub fn render(frame: &Frame, format: Format) -> Vec<u8> {
    let mut result = Vec::new();
    match format {
        Format::Human => human(frame, "", &mut result),
        Format::Raw => raw(frame, &mut result),
        Format::Json => {
            // Every non-finite double and non-UTF-8 string has an explicit tag,
            // so serialization cannot silently replace data or emit invalid JSON.
            serde_json::to_writer(&mut result, &json_value(frame))
                .expect("JSON value serialization");
            result.push(b'\n');
        }
    }
    result
}

fn binary(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(text) => Value::String(text.to_owned()),
        Err(_) => json!({"type":"bytes", "base64": STANDARD.encode(bytes)}),
    }
}

fn pairs(items: &[(Frame, Frame)]) -> Vec<Value> {
    items
        .iter()
        .map(|(key, value)| json!([json_value(key), json_value(value)]))
        .collect()
}

fn json_value(frame: &Frame) -> Value {
    match frame {
        Frame::Simple(bytes) => binary(bytes),
        Frame::Error(bytes) => json!({"type":"error", "value":binary(bytes)}),
        Frame::Bulk(bytes) => binary(bytes),
        Frame::Integer(value) => json!(*value),
        Frame::Null => Value::Null,
        Frame::Boolean(value) => json!(*value),
        Frame::Double(value) if value.is_finite() => json!(*value),
        Frame::Double(value) => {
            json!({"type":"double", "value":if value.is_nan() { "nan" } else if value.is_sign_negative() { "-inf" } else { "inf" }})
        }
        Frame::BigNumber(bytes) => json!({"type":"big-number", "value":binary(bytes)}),
        Frame::Verbatim { format, data } => {
            json!({"type":"verbatim", "format":binary(format), "value":binary(data)})
        }
        Frame::Array(items) => Value::Array(items.iter().map(json_value).collect()),
        Frame::Map(items) => {
            let mut object = serde_json::Map::new();
            for (key, value) in items {
                let key = match key {
                    Frame::Bulk(bytes) | Frame::Simple(bytes) => std::str::from_utf8(bytes).ok(),
                    _ => None,
                };
                let Some(key) = key else {
                    return json!({"type":"map", "entries":pairs(items)});
                };
                if object.insert(key.to_owned(), json_value(value)).is_some() {
                    return json!({"type":"map", "entries":pairs(items)});
                }
            }
            Value::Object(object)
        }
        Frame::Push(items) => {
            json!({"type":"push", "values":items.iter().map(json_value).collect::<Vec<_>>()})
        }
        Frame::Set(items) => {
            json!({"type":"set", "values":items.iter().map(json_value).collect::<Vec<_>>()})
        }
        Frame::Attribute(items, value) => {
            json!({"type":"attribute", "entries":pairs(items), "value":json_value(value)})
        }
    }
}

fn escaped(bytes: &[u8], quoted: bool) -> String {
    let mut result = String::new();
    if quoted {
        result.push('"');
    }
    for &byte in bytes {
        match byte {
            b'\\' => result.push_str("\\\\"),
            b'"' => result.push_str("\\\""),
            b'\n' => result.push_str("\\n"),
            b'\r' => result.push_str("\\r"),
            b'\t' => result.push_str("\\t"),
            0x20..=0x7e => result.push(char::from(byte)),
            _ => {
                use std::fmt::Write;
                write!(&mut result, "\\x{byte:02x}").expect("string formatting");
            }
        }
    }
    if quoted {
        result.push('"');
    }
    result
}

fn line(output: &mut Vec<u8>, prefix: &str, text: &str) {
    output.extend_from_slice(prefix.as_bytes());
    output.extend_from_slice(text.as_bytes());
    output.push(b'\n');
}

fn human(frame: &Frame, prefix: &str, output: &mut Vec<u8>) {
    match frame {
        Frame::Simple(bytes) => line(output, prefix, &escaped(bytes, false)),
        Frame::Error(bytes) => line(
            output,
            prefix,
            &format!("(error) {}", escaped(bytes, false)),
        ),
        Frame::Bulk(bytes) => line(output, prefix, &escaped(bytes, true)),
        Frame::Integer(value) => line(output, prefix, &format!("(integer) {value}")),
        Frame::Null => line(output, prefix, "(nil)"),
        Frame::Boolean(value) => line(output, prefix, &format!("(boolean) {value}")),
        Frame::Double(value) => line(output, prefix, &format!("(double) {value}")),
        Frame::BigNumber(bytes) => line(
            output,
            prefix,
            &format!("(big number) {}", escaped(bytes, false)),
        ),
        Frame::Verbatim { format, data } => line(
            output,
            prefix,
            &format!("({}) {}", escaped(format, false), escaped(data, true)),
        ),
        Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => {
            if items.is_empty() {
                line(output, prefix, "(empty collection)");
            }
            for (index, item) in items.iter().enumerate() {
                let marker = format!("{}) ", index + 1);
                output.extend_from_slice(prefix.as_bytes());
                output.extend_from_slice(marker.as_bytes());
                let mut nested = Vec::new();
                human(
                    item,
                    &format!("{prefix}{}", " ".repeat(marker.len())),
                    &mut nested,
                );
                // The first item starts on the numbered line; subsequent lines
                // retain indentation generated by the nested renderer.
                let skip = prefix.len() + marker.len();
                output.extend_from_slice(&nested[skip..]);
            }
        }
        Frame::Map(items) => {
            if items.is_empty() {
                line(output, prefix, "(empty map)");
            }
            for (index, (key, value)) in items.iter().enumerate() {
                line(output, prefix, &format!("{}) key:", index + 1));
                human(key, &format!("{prefix}   "), output);
                line(output, prefix, "   value:");
                human(value, &format!("{prefix}   "), output);
            }
        }
        Frame::Attribute(items, value) => {
            line(output, prefix, "(attributes)");
            for (key, value) in items {
                human(key, &format!("{prefix}   "), output);
                human(value, &format!("{prefix}   "), output);
            }
            human(value, prefix, output);
        }
    }
}

fn raw(frame: &Frame, output: &mut Vec<u8>) {
    match frame {
        Frame::Simple(bytes)
        | Frame::Error(bytes)
        | Frame::Bulk(bytes)
        | Frame::BigNumber(bytes) => {
            output.extend_from_slice(bytes);
            output.push(b'\n');
        }
        Frame::Verbatim { data, .. } => {
            output.extend_from_slice(data);
            output.push(b'\n');
        }
        Frame::Integer(value) => line(output, "", &value.to_string()),
        Frame::Double(value) => line(output, "", &value.to_string()),
        Frame::Boolean(value) => line(output, "", if *value { "true" } else { "false" }),
        Frame::Null => output.push(b'\n'),
        Frame::Array(items) | Frame::Set(items) | Frame::Push(items) => {
            for item in items {
                raw(item, output);
            }
        }
        Frame::Map(items) => {
            for (key, value) in items {
                raw(key, output);
                raw(value, output);
            }
        }
        Frame::Attribute(items, value) => {
            for (key, value) in items {
                raw(key, output);
                raw(value, output);
            }
            raw(value, output);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    #[test]
    fn raw_preserves_all_bytes_and_adds_exactly_one_newline() {
        let bytes = Bytes::from_static(b"a\0\xff\r\n");
        assert_eq!(render(&Frame::Bulk(bytes), Format::Raw), b"a\0\xff\r\n\n");
    }

    #[test]
    fn json_is_valid_and_binary_data_round_trips() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"\0\xff\n")),
            Frame::Error(Bytes::from_static(b"ERR nested")),
        ]);
        let json: Value = serde_json::from_slice(&render(&frame, Format::Json)).unwrap();
        assert_eq!(json[0]["type"], "bytes");
        assert_eq!(
            STANDARD
                .decode(json[0]["base64"].as_str().unwrap())
                .unwrap(),
            b"\0\xff\n"
        );
        assert_eq!(json[1]["type"], "error");
        assert_eq!(json[1]["value"], "ERR nested");
        assert!(frame.has_error());
    }

    #[test]
    fn human_controls_are_escaped_and_nested_errors_truthful() {
        let frame = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"\x1b\0\xff")),
            Frame::Array(vec![
                Frame::Error(Bytes::from_static(b"ERR\r\nbad")),
                Frame::Null,
            ]),
        ]);
        assert_eq!(
            String::from_utf8(render(&frame, Format::Human)).unwrap(),
            "1) \"\\x1b\\x00\\xff\"\n2) 1) (error) ERR\\r\\nbad\n   2) (nil)\n"
        );
    }

    #[test]
    fn json_nonfinite_doubles_and_binary_map_keys_are_lossless() {
        let frame = Frame::Map(vec![(
            Frame::Bulk(Bytes::from_static(b"\xff")),
            Frame::Double(f64::INFINITY),
        )]);
        let json: Value = serde_json::from_slice(&render(&frame, Format::Json)).unwrap();
        assert_eq!(json["entries"][0][0]["base64"], "/w==");
        assert_eq!(json["entries"][0][1]["value"], "inf");
    }

    #[test]
    fn json_ordinary_maps_are_objects_but_duplicate_keys_are_preserved() {
        let key = || Frame::Bulk(Bytes::from_static(b"server"));
        let value = || Frame::Simple(Bytes::from_static(b"ffkv"));
        let ordinary = Frame::Map(vec![(key(), value())]);
        let json: Value = serde_json::from_slice(&render(&ordinary, Format::Json)).unwrap();
        assert_eq!(json, json!({"server":"ffkv"}));
        let duplicates = Frame::Map(vec![(key(), value()), (key(), Frame::Integer(2))]);
        let json: Value = serde_json::from_slice(&render(&duplicates, Format::Json)).unwrap();
        assert_eq!(json["type"], "map");
        assert_eq!(json["entries"].as_array().unwrap().len(), 2);
        assert_eq!(json["entries"][1][1], 2);
    }
}
