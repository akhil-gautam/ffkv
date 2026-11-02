use super::{model::Row, value};
use chrono::{DateTime, Datelike, NaiveDate, NaiveDateTime, TimeZone, Utc};
use serde_json::{Number, Value};
use std::cmp::Ordering;

#[derive(Clone, Debug)]
enum Expr {
    Literal(Value),
    Field(String),
    Unary(String, Box<Expr>),
    Binary(String, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
}
#[derive(Clone, Debug, PartialEq)]
enum Token {
    Number(f64),
    String(String),
    Field(String),
    Name(String),
    Op(String),
    Open,
    Close,
    Comma,
    End,
}
struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}
fn number(n: f64) -> Value {
    if n.is_infinite() {
        Value::String(if n.is_sign_negative() { "-inf" } else { "inf" }.into())
    } else {
        Number::from_f64(n)
            .map(Value::Number)
            .unwrap_or(Value::Null)
    }
}
pub(super) fn numeric(v: &Value) -> Option<f64> {
    match value::logical(v) {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}
pub(super) fn truth(v: &Value) -> bool {
    match value::logical(v) {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}
pub(super) fn compare(a: &Value, b: &Value) -> Ordering {
    let a = value::logical(a);
    let b = value::logical(b);
    if let (Some(a), Some(b)) = (value::bytes(a), value::bytes(b)) {
        return a.as_ref().cmp(b.as_ref());
    }
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::String(a), Value::String(b)) => a.cmp(b),
        _ => match (numeric(a), numeric(b)) {
            (Some(a), Some(b)) => a.total_cmp(&b),
            _ => a.to_string().cmp(&b.to_string()),
        },
    }
}
fn lex(input: &str) -> Result<Vec<Token>, String> {
    let c: Vec<char> = input.chars().collect();
    let mut p = 0;
    let mut out = Vec::new();
    while p < c.len() {
        let ch = c[p];
        if ch.is_whitespace() {
            p += 1;
            continue;
        }
        match ch {
            '(' => {
                out.push(Token::Open);
                p += 1
            }
            ')' => {
                out.push(Token::Close);
                p += 1
            }
            ',' => {
                out.push(Token::Comma);
                p += 1
            }
            '\'' | '"' => {
                let q = ch;
                p += 1;
                let mut s = String::new();
                let mut closed = false;
                while p < c.len() {
                    let x = c[p];
                    p += 1;
                    if x == q {
                        closed = true;
                        break;
                    }
                    if x == '\\' {
                        if p == c.len() {
                            return Err("Unterminated string escape".into());
                        }
                        let x = c[p];
                        p += 1;
                        s.push(match x {
                            'n' => '\n',
                            'r' => '\r',
                            't' => '\t',
                            x => x,
                        });
                    } else {
                        s.push(x)
                    }
                }
                if !closed {
                    return Err("Unterminated string literal".into());
                }
                out.push(Token::String(s));
            }
            '@' => {
                p += 1;
                let start = p;
                while p < c.len() && (c[p].is_alphanumeric() || matches!(c[p], '_' | '.' | '$')) {
                    p += 1
                }
                if start == p {
                    return Err("Expected property after @".into());
                }
                out.push(Token::Field(c[start..p].iter().collect()));
            }
            '0'..='9' | '.' => {
                let start = p;
                p += 1;
                while p < c.len() && (c[p].is_ascii_digit() || c[p] == '.') {
                    p += 1
                }
                if p < c.len() && matches!(c[p], 'e' | 'E') {
                    p += 1;
                    if p < c.len() && matches!(c[p], '+' | '-') {
                        p += 1
                    }
                    while p < c.len() && c[p].is_ascii_digit() {
                        p += 1
                    }
                }
                let s: String = c[start..p].iter().collect();
                out.push(Token::Number(
                    s.parse()
                        .map_err(|_| "Invalid number in expression".to_string())?,
                ));
            }
            '+' | '-' | '*' | '/' | '%' | '^' | '=' | '!' | '<' | '>' | '&' | '|' => {
                let mut s = ch.to_string();
                p += 1;
                if p < c.len()
                    && ((c[p] == '=' && matches!(ch, '=' | '!' | '<' | '>'))
                        || c[p] == ch && matches!(ch, '&' | '|'))
                {
                    s.push(c[p]);
                    p += 1
                }
                if matches!(s.as_str(), "=" | "&" | "|") {
                    return Err(format!("Invalid expression operator {s}"));
                }
                out.push(Token::Op(s));
            }
            _ if ch.is_alphabetic() || ch == '_' => {
                let start = p;
                p += 1;
                while p < c.len() && (c[p].is_alphanumeric() || c[p] == '_') {
                    p += 1
                }
                out.push(Token::Name(
                    c[start..p].iter().collect::<String>().to_ascii_lowercase(),
                ));
            }
            _ => return Err(format!("Unexpected character '{ch}' in expression")),
        }
    }
    out.push(Token::End);
    Ok(out)
}
fn arity(name: &str, n: usize) -> bool {
    match name {
        "exists" | "log" | "abs" | "ceil" | "floor" | "log2" | "exp" | "sqrt" | "upper"
        | "lower" | "strlen" | "day" | "hour" | "minute" | "month" | "dayofweek" | "dayofmonth"
        | "dayofyear" | "year" | "monthofyear" => n == 1,
        "startswith" | "contains" => n == 2,
        "substr" => n == 3,
        "timefmt" | "parsetime" => n == 1 || n == 2,
        "split" => (1..=3).contains(&n),
        "format" => n >= 1,
        "matched_terms" => n <= 1,
        "geodistance" => n == 2 || n == 3 || n == 4,
        "case" => n == 3,
        _ => false,
    }
}
impl Parser {
    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }
    fn take(&mut self) -> Token {
        let t = self.tokens[self.pos].clone();
        self.pos += 1;
        t
    }
    fn expr(&mut self, min: u8) -> Result<Expr, String> {
        let mut lhs = match self.take() {
            Token::Number(n) => Expr::Literal(number(n)),
            Token::String(s) => Expr::Literal(Value::String(s)),
            Token::Field(s) => Expr::Field(s),
            Token::Op(op) if matches!(op.as_str(), "+" | "-" | "!") => {
                Expr::Unary(op, Box::new(self.expr(13)?))
            }
            Token::Open => {
                let e = self.expr(0)?;
                if self.take() != Token::Close {
                    return Err("Missing closing parenthesis".into());
                }
                e
            }
            Token::Name(name) => {
                if self.peek() != &Token::Open {
                    Expr::Literal(match name.as_str() {
                        "null" => Value::Null,
                        "true" => Value::Bool(true),
                        "false" => Value::Bool(false),
                        "inf" => number(f64::INFINITY),
                        _ => return Err(format!("Unknown symbol '{name}'")),
                    })
                } else {
                    self.take();
                    let mut args = Vec::new();
                    if self.peek() != &Token::Close {
                        loop {
                            args.push(self.expr(0)?);
                            if self.peek() != &Token::Comma {
                                break;
                            }
                            self.take();
                        }
                    }
                    if self.take() != Token::Close {
                        return Err("Missing function closing parenthesis".into());
                    }
                    if !arity(&name, args.len()) {
                        return Err(format!(
                            "Unknown function or invalid argument count: {name}"
                        ));
                    }
                    Expr::Call(name, args)
                }
            }
            _ => return Err("Expected expression operand".into()),
        };
        while let Token::Op(op) = self.peek() {
            let (l, r) = match op.as_str() {
                "||" => (1, 2),
                "&&" => (3, 4),
                "==" | "!=" => (5, 6),
                "<" | "<=" | ">" | ">=" => (7, 8),
                "+" | "-" => (9, 10),
                "*" | "/" | "%" => (11, 12),
                "^" => (14, 13),
                _ => break,
            };
            if l < min {
                break;
            }
            let op = op.clone();
            self.take();
            let rhs = self.expr(r)?;
            lhs = Expr::Binary(op, Box::new(lhs), Box::new(rhs));
        }
        Ok(lhs)
    }
}
fn parse(expr: &str) -> Result<Expr, String> {
    let mut p = Parser {
        tokens: lex(expr)?,
        pos: 0,
    };
    let e = p.expr(0)?;
    if p.peek() != &Token::End {
        return Err("Unexpected trailing expression input".into());
    }
    Ok(e)
}
pub(super) fn validate(expr: &str) -> Result<(), String> {
    parse(expr).map(|_| ())
}
pub(super) fn evaluate(expr: &str, row: &Row) -> Result<Value, String> {
    Ok(eval(&parse(expr)?, row))
}
pub(super) struct Program {
    expr: Expr,
    pub properties: Vec<String>,
}
pub(super) fn compile(expr: &str) -> Result<Program, String> {
    let expr = parse(expr)?;
    let mut properties = Vec::new();
    fn walk(e: &Expr, out: &mut Vec<String>) {
        match e {
            Expr::Field(s) => {
                if !out.contains(s) {
                    out.push(s.clone())
                }
            }
            Expr::Unary(_, a) => walk(a, out),
            Expr::Binary(_, a, b) => {
                walk(a, out);
                walk(b, out)
            }
            Expr::Call(_, a) => {
                for e in a {
                    walk(e, out)
                }
            }
            _ => {}
        }
    }
    walk(&expr, &mut properties);
    Ok(Program { expr, properties })
}
impl Program {
    pub fn evaluate(&self, row: &Row) -> Value {
        eval(&self.expr, row)
    }
}
fn eval(e: &Expr, row: &Row) -> Value {
    match e {
        Expr::Literal(v) => v.clone(),
        Expr::Field(s) => row
            .get(s)
            .map(|v| value::logical(v).clone())
            .unwrap_or(Value::Null),
        Expr::Unary(op, e) => {
            let v = eval(e, row);
            if op == "!" {
                return number((!truth(&v)) as u8 as f64);
            }
            let Some(n) = numeric(&v) else {
                return Value::Null;
            };
            number(if op == "-" { -n } else { n })
        }
        Expr::Binary(op, a, b) => {
            let a = eval(a, row);
            if op == "&&" && !truth(&a) {
                return number(0.);
            }
            if op == "||" && truth(&a) {
                return number(1.);
            }
            let b = eval(b, row);
            if op == "&&" || op == "||" {
                return number(truth(&b) as u8 as f64);
            }
            if matches!(op.as_str(), "==" | "!=" | "<" | "<=" | ">" | ">=") {
                let equality = op == "==" || op == "!=";
                let c = if a.is_null() || b.is_null() {
                    if !equality {
                        return Value::Null;
                    }
                    compare(&a, &b)
                } else if let (Some(a), Some(b)) = (value::bytes(&a), value::bytes(&b)) {
                    a.as_ref().cmp(b.as_ref())
                } else if let (Some(a), Some(b)) = (numeric(&a), numeric(&b)) {
                    a.total_cmp(&b)
                } else if let (Value::Bool(a), Value::Bool(b)) = (&a, &b) {
                    a.cmp(b)
                } else {
                    return Value::Null;
                };
                return number(match op.as_str() {
                    "==" => c == Ordering::Equal,
                    "!=" => c != Ordering::Equal,
                    "<" => c == Ordering::Less,
                    "<=" => c != Ordering::Greater,
                    ">" => c == Ordering::Greater,
                    _ => c != Ordering::Less,
                } as u8 as f64);
            }
            let (Some(a), Some(b)) = (numeric(&a), numeric(&b)) else {
                return Value::Null;
            };
            number(match op.as_str() {
                "+" => a + b,
                "-" => a - b,
                "*" => a * b,
                "/" => a / b,
                "%" => a % b,
                "^" => a.powf(b),
                _ => f64::NAN,
            })
        }
        Expr::Call(name, args) => {
            if name == "case" {
                for pair in args.chunks(2) {
                    if pair.len() == 1 {
                        return eval(&pair[0], row);
                    }
                    if truth(&eval(&pair[0], row)) {
                        return eval(&pair[1], row);
                    }
                }
                return Value::Null;
            }
            let values: Vec<Value> = args.iter().map(|a| eval(a, row)).collect();
            function(name, &values, row)
        }
    }
}
fn format_value(v: &Value, out: &mut Vec<u8>, quoted: bool) {
    let v = value::logical(v);
    if let Some(bytes) = value::bytes(v) {
        if quoted {
            out.push(b'"');
        }
        for b in bytes.iter().copied() {
            match b {
                b'"' | b'\\' if quoted => {
                    out.push(b'\\');
                    out.push(b);
                }
                0..=31 if quoted => {
                    const HEX: &[u8; 16] = b"0123456789abcdef";
                    out.extend_from_slice(b"\\u00");
                    out.push(HEX[(b >> 4) as usize]);
                    out.push(HEX[(b & 15) as usize]);
                }
                _ => out.push(b),
            }
        }
        if quoted {
            out.push(b'"');
        }
        return;
    }
    if let Some(map) = value::json_object(v) {
        out.push(b'{');
        for (i, (key, value)) in map.iter().enumerate() {
            if i > 0 {
                out.push(b',')
            }
            format_value(&Value::String(key.clone()), out, true);
            out.push(b':');
            format_value(value, out, true);
        }
        out.push(b'}');
        return;
    }
    match v {
        Value::Array(values) => {
            out.push(b'[');
            for (i, v) in values.iter().enumerate() {
                if i > 0 {
                    out.push(b',')
                }
                format_value(v, out, true);
            }
            out.push(b']');
        }
        Value::Number(n) => {
            if let Some(n) = n.as_f64() {
                out.extend_from_slice(n.to_string().as_bytes())
            }
        }
        _ => out.extend_from_slice(v.to_string().as_bytes()),
    }
}
fn string_function(name: &str, a: &[Value]) -> Value {
    let Some(s) = value::bytes(&a[0]) else {
        return Value::Null;
    };
    match name {
        "upper" | "lower" => {
            if let Value::String(s) = &a[0] {
                return Value::String(if name == "upper" {
                    s.to_uppercase()
                } else {
                    s.to_lowercase()
                });
            }
            let mut bytes = s.into_owned();
            if name == "upper" {
                bytes.make_ascii_uppercase()
            } else {
                bytes.make_ascii_lowercase()
            }
            value::from_owned_bytes(bytes)
        }
        "strlen" => number(s.len() as f64),
        "startswith" | "contains" => {
            let Some(needle) = value::bytes(&a[1]) else {
                return Value::Null;
            };
            if name == "startswith" {
                return number(s.starts_with(&needle) as u8 as f64);
            }
            if needle.is_empty() {
                return number((s.len() + 1) as f64);
            }
            let (mut offset, mut count) = (0, 0usize);
            while offset + needle.len() <= s.len() {
                let Some(found) = s[offset..]
                    .windows(needle.len())
                    .position(|w| w == needle.as_ref())
                else {
                    break;
                };
                count += 1;
                offset += found + needle.len();
            }
            number(count as f64)
        }
        "substr" => {
            let (Some(offset), Some(count)) = (numeric(&a[1]), numeric(&a[2])) else {
                return Value::Null;
            };
            if !offset.is_finite() || !count.is_finite() || count < 0. && count != -1. {
                return Value::Null;
            }
            let len = s.len() as i64;
            let start = if offset < 0. {
                len.saturating_add(offset as i64).max(0)
            } else {
                offset as i64
            }
            .min(len) as usize;
            let end = if count == -1. {
                s.len()
            } else {
                start.saturating_add(count as usize).min(s.len())
            };
            value::from_bytes(&s[start..end])
        }
        "format" => {
            let mut out = Vec::with_capacity(s.len());
            let (mut p, mut i) = (0, 1);
            while p < s.len() {
                if s[p] != b'%' {
                    out.push(s[p]);
                    p += 1;
                    continue;
                }
                p += 1;
                let Some(spec) = s.get(p) else {
                    return Value::Null;
                };
                p += 1;
                if *spec == b'%' {
                    out.push(b'%');
                    continue;
                }
                if *spec != b's' {
                    return Value::Null;
                }
                let Some(v) = a.get(i) else {
                    return Value::Null;
                };
                i += 1;
                format_value(v, &mut out, false);
            }
            value::from_owned_bytes(out)
        }
        "split" => {
            let sep = if a.len() > 1 {
                let Some(s) = value::bytes(&a[1]) else {
                    return Value::Null;
                };
                s
            } else {
                std::borrow::Cow::Borrowed(b",".as_slice())
            };
            let strip = if a.len() > 2 {
                let Some(s) = value::bytes(&a[2]) else {
                    return Value::Null;
                };
                s
            } else {
                std::borrow::Cow::Borrowed(b" ".as_slice())
            };
            Value::Array(
                s.split(|c| sep.contains(c))
                    .map(|mut part| {
                        while part.first().is_some_and(|c| strip.contains(c)) {
                            part = &part[1..]
                        }
                        while part.last().is_some_and(|c| strip.contains(c)) {
                            part = &part[..part.len() - 1]
                        }
                        value::from_bytes(part)
                    })
                    .collect(),
            )
        }
        _ => Value::Null,
    }
}
fn function(name: &str, a: &[Value], row: &Row) -> Value {
    let n = || numeric(&a[0]);
    let s = || a[0].as_str();
    let string = |s: String| Value::String(s);
    match name {
        "exists" => number((!a[0].is_null()) as u8 as f64),
        "log" | "abs" | "ceil" | "floor" | "log2" | "exp" | "sqrt" => n()
            .map(|n| {
                number(match name {
                    "log" => n.ln(),
                    "abs" => n.abs(),
                    "ceil" => n.ceil(),
                    "floor" => n.floor(),
                    "log2" => n.log2(),
                    "exp" => n.exp(),
                    _ => n.sqrt(),
                })
            })
            .unwrap_or(Value::Null),
        "upper" | "lower" | "strlen" | "startswith" | "contains" | "substr" | "format"
        | "split" => string_function(name, a),
        "matched_terms" => {
            let values = row
                .get("__matched_terms")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let limit = if let Some(value) = a.first() {
                let Some(n) = numeric(value) else {
                    return Value::Null;
                };
                if n < 0. {
                    return Value::Null;
                }
                n as usize
            } else {
                100
            };
            Value::Array(values.into_iter().take(limit.min(100)).collect())
        }
        "timefmt" => {
            let Some(t) = n().and_then(|n| DateTime::from_timestamp(n as i64, 0)) else {
                return Value::Null;
            };
            let fmt = if a.len() == 2 {
                let Some(s) = a[1].as_str() else {
                    return Value::Null;
                };
                s
            } else {
                "%Y-%m-%dT%H:%M:%SZ"
            };
            if chrono::format::StrftimeItems::new(fmt)
                .any(|i| matches!(i, chrono::format::Item::Error))
            {
                return Value::Null;
            }
            string(t.format(fmt).to_string())
        }
        "parsetime" => {
            let Some(s) = s() else { return Value::Null };
            let fmt = if a.len() == 2 {
                let Some(fmt) = a[1].as_str() else {
                    return Value::Null;
                };
                fmt
            } else {
                "%Y-%m-%dT%H:%M:%SZ"
            };
            let dt = DateTime::parse_from_str(s, fmt)
                .map(|t| t.timestamp())
                .ok()
                .or_else(|| {
                    NaiveDateTime::parse_from_str(s, fmt)
                        .ok()
                        .map(|t| t.and_utc().timestamp())
                })
                .or_else(|| {
                    NaiveDate::parse_from_str(s, fmt)
                        .ok()
                        .and_then(|d| d.and_hms_opt(0, 0, 0))
                        .map(|t| t.and_utc().timestamp())
                });
            dt.map(|t| number(t as f64)).unwrap_or(Value::Null)
        }
        "day" | "hour" | "minute" | "month" | "dayofweek" | "dayofmonth" | "dayofyear" | "year"
        | "monthofyear" => {
            let Some(t) = n().and_then(|n| DateTime::from_timestamp(n as i64, 0)) else {
                return Value::Null;
            };
            let v = match name {
                "day" => (t.timestamp().div_euclid(86400) * 86400) as f64,
                "hour" => (t.timestamp().div_euclid(3600) * 3600) as f64,
                "minute" => (t.timestamp().div_euclid(60) * 60) as f64,
                "month" => Utc
                    .with_ymd_and_hms(t.year(), t.month(), 1, 0, 0, 0)
                    .single()
                    .map(|t| t.timestamp() as f64)
                    .unwrap_or(f64::NAN),
                "dayofweek" => t.weekday().num_days_from_sunday() as f64,
                "dayofmonth" => t.day() as f64,
                "dayofyear" => t.ordinal0() as f64,
                "year" => t.year() as f64,
                _ => t.month0() as f64,
            };
            number(v)
        }
        "geodistance" => {
            fn point(v: &Value) -> Option<(f64, f64)> {
                if let Some(s) = v.as_str() {
                    let mut parts = s.split(',');
                    let p = (
                        parts.next()?.trim().parse().ok()?,
                        parts.next()?.trim().parse().ok()?,
                    );
                    if parts.next().is_some() {
                        return None;
                    }
                    Some(p)
                } else {
                    let a = v.as_array()?;
                    if a.len() != 2 {
                        return None;
                    }
                    Some((numeric(a.first()?)?, numeric(a.get(1)?)?))
                }
            }
            let points = match a.len() {
                2 => point(&a[0]).zip(point(&a[1])),
                3 => point(&a[0])
                    .zip(numeric(&a[1]).zip(numeric(&a[2])))
                    .or_else(|| numeric(&a[0]).zip(numeric(&a[1])).zip(point(&a[2]))),
                4 => numeric(&a[0])
                    .zip(numeric(&a[1]))
                    .zip(numeric(&a[2]).zip(numeric(&a[3]))),
                _ => None,
            };
            let Some(((x1, y1), (x2, y2))) = points else {
                return Value::Null;
            };
            if ![x1, y1, x2, y2].iter().all(|n| n.is_finite())
                || x1.abs() > 180.
                || x2.abs() > 180.
                || y1.abs() > 90.
                || y2.abs() > 90.
            {
                return Value::Null;
            }
            let p1 = y1.to_radians();
            let p2 = y2.to_radians();
            let h = ((p2 - p1) / 2.).sin().powi(2)
                + p1.cos() * p2.cos() * ((x2 - x1).to_radians() / 2.).sin().powi(2);
            number(6372797.560856 * 2. * h.clamp(0., 1.).sqrt().asin())
        }
        _ => Value::Null,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn arithmetic_and_lazy_conditionals() {
        let row = Row::from([("n".into(), json!(9)), ("label".into(), json!("Hello"))]);
        assert_eq!(evaluate("sqrt(@n) + 2^3^2", &row).unwrap(), json!(515.0));
        assert_eq!(
            evaluate("case(exists(@missing), @missing / 0, lower(@label))", &row).unwrap(),
            json!("hello")
        );
        assert_eq!(evaluate("true && !false", &row).unwrap(), json!(1.0));
        assert_eq!(evaluate("'10' < '2'", &row).unwrap(), json!(1.0));
    }
    #[test]
    fn runtime_type_errors_are_null_and_syntax_is_an_error() {
        assert_eq!(
            evaluate("sqrt('not numeric')", &Row::new()).unwrap(),
            Value::Null
        );
        assert_eq!(
            evaluate("contains(5, 'x')", &Row::new()).unwrap(),
            Value::Null
        );
        for expr in ["unknown(1)", "1 +", "sqrt(1,2)", "(1 + 2", "case(1,2)"] {
            assert!(validate(expr).is_err(), "{expr}");
        }
    }
    #[test]
    fn string_datetime_and_geo_functions() {
        let row = Row::new();
        assert_eq!(evaluate("contains('abc', '')", &row).unwrap(), json!(4.0));
        assert_eq!(
            evaluate("substr('abcdef', -3, -1)", &row).unwrap(),
            json!("def")
        );
        assert_eq!(
            evaluate("split('a, b|c', ',|')", &row).unwrap(),
            json!(["a", "b", "c"])
        );
        assert_eq!(
            evaluate("format('%s:%s', 'age', 42)", &row).unwrap(),
            json!("age:42")
        );
        assert_eq!(
            evaluate("timefmt(parsetime('2024-02-29T12:34:56Z'))", &row).unwrap(),
            json!("2024-02-29T12:34:56Z")
        );
        assert_eq!(
            evaluate("dayofyear(parsetime('2024-02-29T00:00:00Z'))", &row).unwrap(),
            json!(59.0)
        );
        assert_eq!(evaluate("monthofyear(0)", &row).unwrap(), json!(0.0));
        assert_eq!(
            evaluate("geodistance(1, 2, '1,2')", &row).unwrap(),
            json!(0.0)
        );
    }
}

#[cfg(test)]
mod encoded_value_tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn binary_fields_survive_string_operations() {
        let row = Row::from([("raw".into(), value::from_bytes(&[255, b'a', 0, b'b']))]);
        let upper = evaluate("upper(@raw)", &row).unwrap();
        assert_eq!(
            value::bytes(&upper).unwrap().as_ref(),
            &[255, b'A', 0, b'B']
        );
        let part = evaluate("substr(@raw, 0, 2)", &row).unwrap();
        assert_eq!(value::bytes(&part).unwrap().as_ref(), &[255, b'a']);
        let formatted = evaluate("format('x%s', @raw)", &row).unwrap();
        assert_eq!(
            value::bytes(&formatted).unwrap().as_ref(),
            &[b'x', 255, b'a', 0, b'b']
        );
        assert_eq!(evaluate("strlen(@raw)", &row).unwrap(), json!(4.0));
    }
    #[test]
    fn source_projection_format_does_not_change_expression_types() {
        let row = Row::from([
            (
                "n".into(),
                value::projection(vec![json!(3), json!(8)], 3, false),
            ),
            (
                "name".into(),
                value::projection(vec![json!("hello")], 3, false),
            ),
        ]);
        assert_eq!(evaluate("@n * 2", &row).unwrap(), json!(6.0));
        assert_eq!(evaluate("upper(@name)", &row).unwrap(), json!("HELLO"));
        assert_eq!(evaluate("split(@name) + 1", &row).unwrap(), Value::Null);
    }
}
