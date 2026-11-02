//! Bounded, side-effect-free top-level JSON attribute expressions.
use serde_json::Value;
use std::borrow::Cow;

const MAX_TOKENS: usize = 4096;
const MAX_DEPTH: usize = 64;
#[derive(Clone, Debug, PartialEq)]
enum Token {
    Number(f64),
    String(String),
    Field(String),
    Literal(Value),
    Op(String),
    Left,
    Right,
    Open,
    Close,
    Comma,
}
#[derive(Debug)]
pub(super) enum Expr {
    Literal(Value),
    Field(String),
    Array(Vec<Expr>),
    Unary(String, Box<Expr>),
    Binary(String, Box<Expr>, Box<Expr>),
}

pub(super) fn compile(input: &[u8]) -> Result<Expr, &'static str> {
    if input.len() > 65536 {
        return Err("filter expression too large");
    }
    let text = std::str::from_utf8(input).map_err(|_| "invalid filter UTF-8")?;
    let mut tokens = Vec::new();
    let mut p = 0;
    let bytes = text.as_bytes();
    while p < bytes.len() {
        if bytes[p].is_ascii_whitespace() {
            p += 1;
            continue;
        }
        if tokens.len() >= MAX_TOKENS {
            return Err("filter expression too complex");
        }
        let start = p;
        let token = match bytes[p] {
            b'(' => {
                p += 1;
                Token::Left
            }
            b')' => {
                p += 1;
                Token::Right
            }
            b'[' => {
                p += 1;
                Token::Open
            }
            b']' => {
                p += 1;
                Token::Close
            }
            b',' => {
                p += 1;
                Token::Comma
            }
            b'\'' | b'"' => {
                let quote = bytes[p];
                p += 1;
                let mut value = String::new();
                let mut closed = false;
                while p < bytes.len() {
                    if bytes[p] == quote {
                        p += 1;
                        closed = true;
                        break;
                    }
                    if bytes[p] == b'\\' {
                        p += 1;
                        let escape = *bytes.get(p).ok_or("unfinished filter escape")?;
                        match escape {
                            b'n' => value.push('\n'),
                            b'r' => value.push('\r'),
                            b't' => value.push('\t'),
                            b'b' => value.push('\u{8}'),
                            b'f' => value.push('\u{c}'),
                            b'\\' => value.push('\\'),
                            b'\'' => value.push('\''),
                            b'"' => value.push('"'),
                            b'/' => value.push('/'),
                            b'u' => {
                                let end = p
                                    .checked_add(5)
                                    .filter(|end| *end <= bytes.len())
                                    .ok_or("invalid unicode escape")?;
                                if !bytes[p + 1..end].iter().all(u8::is_ascii_hexdigit) {
                                    return Err("invalid unicode escape");
                                }
                                let code = u16::from_str_radix(&text[p + 1..end], 16)
                                    .map_err(|_| "invalid unicode escape")?;
                                let end = if (0xD800..=0xDBFF).contains(&code) {
                                    let end2 = end
                                        .checked_add(6)
                                        .filter(|end2| *end2 <= bytes.len())
                                        .ok_or("invalid surrogate pair")?;
                                    if &bytes[end..end + 2] != b"\\u"
                                        || !bytes[end + 2..end2].iter().all(u8::is_ascii_hexdigit)
                                    {
                                        return Err("invalid surrogate pair");
                                    }
                                    end2
                                } else {
                                    end
                                };
                                let mut escaped = Vec::with_capacity(end - p + 3);
                                escaped.push(b'"');
                                escaped.extend_from_slice(&bytes[p - 1..end]);
                                escaped.push(b'"');
                                value.push_str(
                                    &serde_json::from_slice::<String>(&escaped)
                                        .map_err(|_| "invalid unicode escape")?,
                                );
                                p = end;
                                continue;
                            }
                            _ => return Err("invalid filter escape"),
                        }
                        p += 1;
                    } else {
                        let ch = text[p..].chars().next().ok_or("invalid string")?;
                        if ch.is_control() {
                            return Err("control character in filter string");
                        }
                        value.push(ch);
                        p += ch.len_utf8();
                    }
                }
                if !closed {
                    return Err("unterminated filter string");
                }
                Token::String(value)
            }
            b'.' if bytes.get(p + 1).is_some_and(u8::is_ascii_digit) => {
                p += 1;
                while p < bytes.len() && bytes[p].is_ascii_digit() {
                    p += 1;
                }
                if p < bytes.len() && matches!(bytes[p], b'e' | b'E') {
                    p += 1;
                    if p < bytes.len() && matches!(bytes[p], b'+' | b'-') {
                        p += 1;
                    }
                    while p < bytes.len() && bytes[p].is_ascii_digit() {
                        p += 1;
                    }
                }
                let n = text[start..p]
                    .parse::<f64>()
                    .map_err(|_| "invalid number")?;
                if !n.is_finite() {
                    return Err("non-finite filter number");
                }
                Token::Number(n)
            }
            b'.' => {
                p += 1;
                let field = p;
                while p < bytes.len()
                    && (bytes[p].is_ascii_alphanumeric() || bytes[p] == b'_' || bytes[p] == b'-')
                {
                    p += 1;
                }
                if field == p {
                    return Err("missing field name");
                }
                Token::Field(text[field..p].to_owned())
            }
            b'0'..=b'9' => {
                p += 1;
                while p < bytes.len() && (bytes[p].is_ascii_digit() || bytes[p] == b'.') {
                    p += 1;
                }
                if p < bytes.len() && matches!(bytes[p], b'e' | b'E') {
                    p += 1;
                    if p < bytes.len() && matches!(bytes[p], b'+' | b'-') {
                        p += 1;
                    }
                    while p < bytes.len() && bytes[p].is_ascii_digit() {
                        p += 1;
                    }
                }
                let number = text[start..p]
                    .parse::<f64>()
                    .map_err(|_| "invalid filter number")?;
                if !number.is_finite() {
                    return Err("non-finite filter number");
                }
                Token::Number(number)
            }
            b'a'..=b'z' => {
                while p < bytes.len() && bytes[p].is_ascii_alphabetic() {
                    p += 1;
                }
                match &text[start..p] {
                    "and" | "or" | "not" | "in" => Token::Op(text[start..p].to_owned()),
                    "true" => Token::Literal(Value::Bool(true)),
                    "false" => Token::Literal(Value::Bool(false)),
                    "null" => Token::Literal(Value::Null),
                    _ => return Err("unknown filter identifier"),
                }
            }
            _ => {
                if !bytes[p].is_ascii() {
                    return Err("invalid filter operator");
                }
                p += 1;
                if p < bytes.len()
                    && matches!(
                        &bytes[start..=p],
                        b"**" | b"==" | b"!=" | b">=" | b"<=" | b"&&" | b"||"
                    )
                {
                    p += 1;
                }
                let op = &text[start..p];
                if !matches!(
                    op,
                    "+" | "-"
                        | "*"
                        | "/"
                        | "%"
                        | "**"
                        | "=="
                        | "!="
                        | ">"
                        | "<"
                        | ">="
                        | "<="
                        | "!"
                        | "&&"
                        | "||"
                ) {
                    return Err("invalid filter operator");
                }
                Token::Op(op.to_owned())
            }
        };
        tokens.push(token);
    }
    let mut parser = Parser {
        tokens: &tokens,
        position: 0,
    };
    let expression = parser.parse(0, 0)?;
    if parser.position != tokens.len() {
        return Err("unexpected filter token");
    }
    let mut pending = vec![(&expression, 0)];
    while let Some((node, depth)) = pending.pop() {
        if depth > MAX_DEPTH {
            return Err("filter evaluation nesting too deep");
        }
        match node {
            Expr::Unary(_, e) => pending.push((e, depth + 1)),
            Expr::Binary(_, a, b) => {
                pending.push((a, depth + 1));
                pending.push((b, depth + 1));
            }
            Expr::Array(items) => pending.extend(items.iter().map(|e| (e, depth + 1))),
            _ => {}
        }
    }
    Ok(expression)
}
struct Parser<'a> {
    tokens: &'a [Token],
    position: usize,
}
impl Parser<'_> {
    fn parse(&mut self, minimum: u8, depth: usize) -> Result<Expr, &'static str> {
        if depth > MAX_DEPTH {
            return Err("filter nesting too deep");
        }
        let token = self
            .tokens
            .get(self.position)
            .ok_or("incomplete filter expression")?
            .clone();
        self.position += 1;
        let mut left = match token {
            Token::Number(n) => Expr::Literal(Value::from(n)),
            Token::String(s) => Expr::Literal(Value::String(s)),
            Token::Literal(v) => Expr::Literal(v),
            Token::Field(s) => Expr::Field(s),
            Token::Op(op) if matches!(op.as_str(), "!" | "not" | "+" | "-") => {
                Expr::Unary(op, Box::new(self.parse(8, depth + 1)?))
            }
            Token::Left => {
                let e = self.parse(0, depth + 1)?;
                if self.tokens.get(self.position) != Some(&Token::Right) {
                    return Err("missing closing parenthesis");
                }
                self.position += 1;
                e
            }
            Token::Open => {
                let mut values = Vec::new();
                if self.tokens.get(self.position) != Some(&Token::Close) {
                    loop {
                        values.push(self.parse(0, depth + 1)?);
                        if self.tokens.get(self.position) != Some(&Token::Comma) {
                            break;
                        }
                        self.position += 1;
                    }
                }
                if self.tokens.get(self.position) != Some(&Token::Close) {
                    return Err("missing closing bracket");
                }
                self.position += 1;
                Expr::Array(values)
            }
            _ => return Err("unexpected filter operand"),
        };
        while let Some(Token::Op(op)) = self.tokens.get(self.position) {
            let precedence = match op.as_str() {
                "or" | "||" => 1,
                "and" | "&&" => 2,
                "==" | "!=" | ">" | "<" | ">=" | "<=" | "in" => 3,
                "+" | "-" => 4,
                "*" | "/" | "%" => 5,
                "**" => 6,
                _ => break,
            };
            if precedence < minimum {
                break;
            }
            let op = op.clone();
            self.position += 1;
            let right = self.parse(
                if op == "**" {
                    precedence
                } else {
                    precedence + 1
                },
                depth + 1,
            )?;
            left = Expr::Binary(op, Box::new(left), Box::new(right));
        }
        Ok(left)
    }
}
fn number(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        Value::Null => Some(0.0),
        Value::String(s) => s.parse::<f64>().ok().filter(|n| n.is_finite()),
        _ => None,
    }
}
fn truth(v: &Value) -> bool {
    match v {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(_) => number(v).is_some_and(|n| n != 0.0),
        Value::String(s) => !s.is_empty(),
        _ => true,
    }
}
fn equal(a: &Value, b: &Value) -> bool {
    if matches!(
        (a, b),
        (Value::String(_), Value::String(_)) | (Value::Null, _) | (_, Value::Null)
    ) {
        a == b
    } else {
        match (number(a), number(b)) {
            (Some(x), Some(y)) => x == y,
            _ => a == b,
        }
    }
}
impl Expr {
    fn eval<'a>(&'a self, attrs: &'a Value, budget: &mut usize) -> Option<Cow<'a, Value>> {
        *budget = budget.checked_sub(1)?;
        match self {
            Self::Literal(v) => Some(Cow::Borrowed(v)),
            Self::Field(s) => attrs.get(s).map(Cow::Borrowed),
            Self::Array(items) => Some(Cow::Owned(Value::Array(
                items
                    .iter()
                    .map(|e| e.eval(attrs, budget).map(Cow::into_owned))
                    .collect::<Option<_>>()?,
            ))),
            Self::Unary(op, e) => {
                let v = e.eval(attrs, budget)?;
                match op.as_str() {
                    "!" | "not" => Some(Cow::Owned(Value::Bool(!truth(&v)))),
                    "+" => Some(Cow::Owned(Value::from(number(&v)?))),
                    "-" => Some(Cow::Owned(Value::from(-number(&v)?))),
                    _ => None,
                }
            }
            Self::Binary(op, a, b) => {
                // Deliberately evaluate both sides: any missing selector invalidates this node.
                let a = a.eval(attrs, budget)?;
                let b = b.eval(attrs, budget)?;
                let boolean = match op.as_str() {
                    "and" | "&&" => Some(truth(&a) && truth(&b)),
                    "or" | "||" => Some(truth(&a) || truth(&b)),
                    "==" => Some(equal(&a, &b)),
                    "!=" => Some(!equal(&a, &b)),
                    "in" => Some(match b.as_ref() {
                        Value::Array(items) => items.iter().any(|v| equal(&a, v)),
                        Value::String(s) => a.as_str().is_some_and(|needle| s.contains(needle)),
                        _ => false,
                    }),
                    ">" => Some(number(&a)? > number(&b)?),
                    "<" => Some(number(&a)? < number(&b)?),
                    ">=" => Some(number(&a)? >= number(&b)?),
                    "<=" => Some(number(&a)? <= number(&b)?),
                    _ => None,
                };
                if let Some(b) = boolean {
                    return Some(Cow::Owned(Value::Bool(b)));
                }
                let (x, y) = (number(&a)?, number(&b)?);
                let result = match op.as_str() {
                    "+" => x + y,
                    "-" => x - y,
                    "*" => x * y,
                    "/" if y != 0.0 => x / y,
                    "%" if y != 0.0 => x % y,
                    "**" => x.powf(y),
                    _ => return None,
                };
                result.is_finite().then(|| Cow::Owned(Value::from(result)))
            }
        }
    }
    pub(super) fn matches(&self, attributes: Option<&[u8]>) -> bool {
        let Some(bytes) = attributes else {
            return false;
        };
        let Ok(value) = serde_json::from_slice::<Value>(bytes) else {
            return false;
        };
        let mut budget = MAX_TOKENS;
        value.is_object()
            && self
                .eval(&value, &mut budget)
                .as_ref()
                .is_some_and(|v| truth(v))
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn arithmetic_strings_arrays_and_missing() {
        let e =
            compile(br#"(.year - 2000) ** 2 < 100 and .genre in ['action',"drama"] and .ok == 1"#)
                .unwrap();
        assert!(e.matches(Some(br#"{"year":2004,"genre":"drama","ok":true}"#)));
        assert!(!e.matches(Some(br#"{"year":2004,"genre":"drama"}"#)));
        assert!(!compile(b"1 or .missing").unwrap().matches(Some(b"{}")));
        assert!(!compile(b".x / 0 > 1").unwrap().matches(Some(br#"{"x":2}"#)));
    }
    #[test]
    fn null_equality_is_not_numeric_but_other_coercions_are_preserved() {
        for attrs in [
            br#"{"x":0}"#.as_slice(),
            br#"{"x":false}"#,
            br#"{"x":"0"}"#,
            br#"{"x":""}"#,
        ] {
            for filter in [".x == null", "null == .x", ".x in [null]"] {
                assert!(!compile(filter.as_bytes()).unwrap().matches(Some(attrs)));
            }
            for filter in [".x != null", "null != .x"] {
                assert!(compile(filter.as_bytes()).unwrap().matches(Some(attrs)));
            }
        }
        let null = Some(br#"{"x":null}"#.as_slice());
        for filter in [".x == null", "null == .x", ".x in [null]", ".x + 0 == 0"] {
            assert!(compile(filter.as_bytes()).unwrap().matches(null));
        }
        for filter in [
            ".x != null",
            ".x == 0",
            ".x == false",
            ".x in [0, false, '0']",
        ] {
            assert!(!compile(filter.as_bytes()).unwrap().matches(null));
        }
        for filter in [".x == false", ".x == '0'", ".x in [false, '0']"] {
            assert!(
                compile(filter.as_bytes())
                    .unwrap()
                    .matches(Some(br#"{"x":0}"#))
            );
        }
    }
    #[test]
    fn rejects_malformed_and_deep_filters() {
        for input in [
            b".a ==".as_slice(),
            b".a.b",
            b"system(1)",
            b"[1,]",
            b"\"unterminated",
        ] {
            assert!(compile(input).is_err());
        }
        assert!(compile(format!("{}1{}", "(".repeat(70), ")".repeat(70)).as_bytes()).is_err());
        assert!(!compile(b"1").unwrap().matches(Some(b"not-json")));
    }
    #[test]
    fn rejects_unquoted_unicode_without_breaking_quoted_strings() {
        for input in ["é", "😀", ".x == é"] {
            assert!(compile(input.as_bytes()).is_err());
        }
        let expr = compile(".name == 'café'".as_bytes()).unwrap();
        assert!(expr.matches(Some("{\"name\":\"café\"}".as_bytes())));
    }
}
