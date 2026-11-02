use indexmap::IndexMap;
use serde_json::{Number, Value};
use std::sync::Arc;

use super::{MAX_BYTES, MAX_DEPTH, MAX_NODES, invalid, limit};
pub(super) type V = Arc<Node>;
#[derive(Clone)]
pub(super) enum Kind {
    Null,
    Bool(bool),
    Number(Number),
    String(String),
    Array(Vec<V>),
    Object(IndexMap<String, V>),
}
#[derive(Clone)]
pub(super) struct Node {
    pub kind: Kind,
    pub bytes: usize,
    pub nodes: usize,
    pub depth: usize,
}
impl Node {
    pub fn new(kind: Kind) -> V {
        let (bytes, nodes, depth) = match &kind {
            Kind::Null => (4, 1, 1),
            Kind::Bool(b) => (if *b { 4 } else { 5 }, 1, 1),
            Kind::Number(n) => (n.as_str().len(), 1, 1),
            Kind::String(s) => (quoted_len(s), 1, 1),
            Kind::Array(a) => (
                2 + a.len().saturating_sub(1) + a.iter().map(|v| v.bytes).sum::<usize>(),
                1 + a.iter().map(|v| v.nodes).sum::<usize>(),
                1 + a.iter().map(|v| v.depth).max().unwrap_or(0),
            ),
            Kind::Object(o) => (
                2 + o.len().saturating_sub(1)
                    + o.iter()
                        .map(|(k, v)| quoted_len(k) + 1 + v.bytes)
                        .sum::<usize>(),
                1 + o.values().map(|v| v.nodes).sum::<usize>(),
                1 + o.values().map(|v| v.depth).max().unwrap_or(0),
            ),
        };
        Arc::new(Self {
            kind,
            bytes,
            nodes,
            depth,
        })
    }
    pub fn parse(bytes: &[u8]) -> Result<V, super::Resp> {
        if bytes.len() > MAX_BYTES {
            return Err(limit());
        }
        let value: Value = serde_json::from_slice(bytes).map_err(|_| invalid())?;
        Self::from_value(value)
    }
    pub fn from_value(value: Value) -> Result<V, super::Resp> {
        fn convert(v: Value, depth: usize) -> Result<V, super::Resp> {
            if depth > MAX_DEPTH {
                return Err(limit());
            }
            let kind = match v {
                Value::Null => Kind::Null,
                Value::Bool(b) => Kind::Bool(b),
                Value::Number(n) => {
                    validate_number(n.as_str())?;
                    Kind::Number(n)
                }
                Value::String(s) => Kind::String(s),
                Value::Array(a) => Kind::Array(
                    a.into_iter()
                        .map(|v| convert(v, depth + 1))
                        .collect::<Result<_, _>>()?,
                ),
                Value::Object(o) => Kind::Object(
                    o.into_iter()
                        .map(|(k, v)| Ok((k, convert(v, depth + 1)?)))
                        .collect::<Result<_, super::Resp>>()?,
                ),
            };
            let node = Node::new(kind);
            check(&node)?;
            Ok(node)
        }
        convert(value, 1)
    }
    pub fn name(&self) -> &'static str {
        match &self.kind {
            Kind::Null => "null",
            Kind::Bool(_) => "boolean",
            Kind::Number(n) => {
                if n.as_str().contains(['.', 'e', 'E']) {
                    "number"
                } else {
                    "integer"
                }
            }
            Kind::String(_) => "string",
            Kind::Array(_) => "array",
            Kind::Object(_) => "object",
        }
    }
    pub fn write(&self, out: &mut Vec<u8>) {
        match &self.kind {
            Kind::Null => out.extend_from_slice(b"null"),
            Kind::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
            Kind::Number(n) => out.extend_from_slice(n.as_str().as_bytes()),
            Kind::String(s) => write_string(s, out),
            Kind::Array(a) => {
                out.push(b'[');
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    v.write(out);
                }
                out.push(b']');
            }
            Kind::Object(o) => {
                out.push(b'{');
                for (i, (k, v)) in o.iter().enumerate() {
                    if i > 0 {
                        out.push(b',');
                    }
                    write_string(k, out);
                    out.push(b':');
                    v.write(out);
                }
                out.push(b'}');
            }
        }
    }
    pub fn json(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.bytes);
        self.write(&mut out);
        out
    }
}
pub(super) fn quoted_len(s: &str) -> usize {
    2 + s
        .bytes()
        .map(|c| match c {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
            0..=31 => 6,
            _ => 1,
        })
        .sum::<usize>()
}
pub(super) fn write_string(s: &str, out: &mut Vec<u8>) {
    out.push(b'"');
    for c in s.bytes() {
        match c {
            b'"' => out.extend_from_slice(b"\\\""),
            b'\\' => out.extend_from_slice(b"\\\\"),
            b'\n' => out.extend_from_slice(b"\\n"),
            b'\r' => out.extend_from_slice(b"\\r"),
            b'\t' => out.extend_from_slice(b"\\t"),
            8 => out.extend_from_slice(b"\\b"),
            12 => out.extend_from_slice(b"\\f"),
            0..=31 => {
                const HEX: &[u8] = b"0123456789abcdef";
                out.extend_from_slice(&[
                    b'\\',
                    b'u',
                    b'0',
                    b'0',
                    HEX[(c >> 4) as usize],
                    HEX[(c & 15) as usize],
                ]);
            }
            _ => out.push(c),
        }
    }
    out.push(b'"');
}
pub(super) fn check(v: &Node) -> Result<(), super::Resp> {
    if v.bytes > MAX_BYTES || v.nodes > MAX_NODES || v.depth > MAX_DEPTH {
        Err(limit())
    } else {
        Ok(())
    }
}
pub(super) fn validate_number(s: &str) -> Result<(), super::Resp> {
    if s.len() > 4096 {
        return Err(limit());
    }
    if let Some(i) = s.find(['e', 'E']) {
        let exponent = s[i + 1..]
            .parse::<i64>()
            .map_err(|_| super::error("ERR numeric overflow"))?;
        if exponent.unsigned_abs() > 4096 {
            return Err(super::error("ERR numeric overflow"));
        }
    }
    if !s.parse::<f64>().is_ok_and(f64::is_finite) {
        return Err(super::error("ERR numeric overflow"));
    }
    Ok(())
}
pub(super) fn number(f: f64) -> Result<V, super::Resp> {
    let n = Number::from_f64(f).ok_or_else(|| super::error("ERR numeric overflow"))?;
    Ok(Node::new(Kind::Number(n)))
}
// Compare decimal tokens without converting integral operands through f64.
pub(super) fn numeric_cmp(a: &Number, b: &Number) -> std::cmp::Ordering {
    fn parts(s: &str) -> (bool, &str, i64, usize) {
        let negative = s.starts_with('-');
        let s = s.trim_start_matches('-');
        let (coefficient, exponent) = s.find(['e', 'E']).map_or((s, 0), |i| {
            (
                &s[..i],
                s[i + 1..].parse::<i64>().expect("validated exponent"),
            )
        });
        let fraction = coefficient
            .find('.')
            .map_or(0, |i| (coefficient.len() - i - 1) as i64);
        let Some(first) = coefficient.bytes().position(|b| b != b'0' && b != b'.') else {
            return (false, "", 0, 0);
        };
        let last = coefficient
            .bytes()
            .rposition(|b| b != b'0' && b != b'.')
            .unwrap();
        let trailing = coefficient[last + 1..]
            .bytes()
            .filter(|b| *b != b'.')
            .count() as i64;
        let digits = &coefficient[first..=last];
        let count = digits.bytes().filter(|b| *b != b'.').count();
        (negative, digits, exponent - fraction + trailing, count)
    }
    let (an, ad, ae, ac) = parts(a.as_str());
    let (bn, bd, be, bc) = parts(b.as_str());
    if an != bn {
        return if an {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        };
    }
    let order = if ac == 0 || bc == 0 {
        ac.cmp(&bc)
    } else {
        (ac as i64 + ae).cmp(&(bc as i64 + be)).then_with(|| {
            ad.bytes()
                .filter(|b| *b != b'.')
                .chain(std::iter::repeat(b'0'))
                .take(ac.max(bc))
                .cmp(
                    bd.bytes()
                        .filter(|b| *b != b'.')
                        .chain(std::iter::repeat(b'0'))
                        .take(ac.max(bc)),
                )
        })
    };
    if an { order.reverse() } else { order }
}
pub(super) fn equal(a: &Node, b: &Node) -> bool {
    match (&a.kind, &b.kind) {
        (Kind::Null, Kind::Null) => true,
        (Kind::Bool(a), Kind::Bool(b)) => a == b,
        (Kind::Number(a), Kind::Number(b)) => numeric_cmp(a, b).is_eq(),
        (Kind::String(a), Kind::String(b)) => a == b,
        (Kind::Array(a), Kind::Array(b)) => {
            a.len() == b.len() && a.iter().zip(b).all(|(a, b)| equal(a, b))
        }
        (Kind::Object(a), Kind::Object(b)) => {
            a.len() == b.len() && a.iter().all(|(k, a)| b.get(k).is_some_and(|b| equal(a, b)))
        }
        _ => false,
    }
}
