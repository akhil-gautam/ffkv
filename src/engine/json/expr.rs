use super::node::{self, Kind, Node, V};
use super::path::{self, Path, Work};
use super::{MAX_DEPTH, Resp, limit};

// A query shares one cumulative budget, including values later consumed by a
// scalar aggregate. Referenced document subtrees remain shared; only newly
// allocated nodes, copied text and intermediate value slots are charged here.
const NODE_ALLOCATION: usize = std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
fn value_slots(work: &mut Work, len: usize) -> Result<(), Resp> {
    work.charge(
        len.checked_mul(std::mem::size_of::<V>())
            .ok_or_else(limit)?,
    )
}
fn key_projection(values: &[V], work: &mut Work) -> Result<Vec<V>, Resp> {
    let mut count = 0_usize;
    for v in values {
        if let Kind::Object(o) = &v.kind {
            for key in o.keys() {
                work.charge(
                    key.len()
                        .checked_add(NODE_ALLOCATION + std::mem::size_of::<V>())
                        .ok_or_else(limit)?,
                )?;
                count = count.checked_add(1).ok_or_else(limit)?;
            }
        }
    }
    let mut keys = Vec::with_capacity(count);
    for v in values {
        if let Kind::Object(o) = &v.kind {
            keys.extend(o.keys().map(|key| Node::new(Kind::String(key.clone()))));
        }
    }
    Ok(keys)
}
#[derive(Clone)]
pub(super) enum Expr {
    Literal(V),
    Path(bool, Path),
    Unary(String, Box<Expr>),
    Binary(String, Box<Expr>, Box<Expr>),
    Call(String, Vec<Expr>),
    Keys(Box<Expr>),
}
impl Expr {
    pub fn parse(s: &str) -> Result<Self, Resp> {
        Self::parse_depth(s.trim(), 0)
    }
    fn parse_depth(s: &str, depth: usize) -> Result<Self, Resp> {
        if depth > MAX_DEPTH || s.len() > 16384 {
            return Err(limit());
        }
        if s.is_empty() {
            return Err(path::path_error());
        }
        if s.starts_with('(') && path::closing(s, 0, b'(', b')')? == s.len() - 1 {
            return Self::parse_depth(s[1..s.len() - 1].trim(), depth + 1);
        }
        for ops in [
            &["||"][..],
            &["&&"][..],
            &[
                "==", "!=", "<=", ">=", "=~", "<", ">", "in", "nin", "subsetof", "anyof", "noneof",
                "size", "sizeof", "empty",
            ][..],
            &["+", "-"][..],
            &["*", "/", "%"][..],
        ] {
            if let Some((i, op)) = operator(s, ops)? {
                let a = Self::parse_depth(s[..i].trim(), depth + 1)?;
                let b = Self::parse_depth(s[i + op.len()..].trim(), depth + 1)?;
                return Ok(Self::Binary(op.to_owned(), Box::new(a), Box::new(b)));
            }
        }
        if let Some(rest) = s.strip_prefix('!') {
            return Ok(Self::Unary(
                "!".into(),
                Box::new(Self::parse_depth(rest.trim(), depth + 1)?),
            ));
        }
        if s.starts_with(['+', '-']) && s[1..].trim().starts_with(['@', '$', '(']) {
            return Ok(Self::Unary(
                s[..1].into(),
                Box::new(Self::parse_depth(s[1..].trim(), depth + 1)?),
            ));
        }
        if let Some(rest) = s.strip_suffix('~') {
            return Ok(Self::Keys(Box::new(Self::parse_depth(
                rest.trim(),
                depth + 1,
            )?)));
        }
        if s.ends_with(')')
            && let Some(i) = call_open(s)?
        {
            let prefix = s[..i].trim();
            let mut args = Vec::new();
            let name = if let Some(dot) = prefix.rfind('.') {
                args.push(Self::parse_depth(&prefix[..dot], depth + 1)?);
                &prefix[dot + 1..]
            } else {
                prefix
            };
            if name.is_empty() || !name.bytes().all(|b| b.is_ascii_alphabetic() || b == b'_') {
                return Err(path::path_error());
            }
            let inner = &s[i + 1..s.len() - 1];
            if !inner.trim().is_empty() {
                for arg in path::split(inner, b',')? {
                    args.push(Self::parse_depth(arg.trim(), depth + 1)?);
                }
            }
            return Ok(Self::Call(name.to_ascii_lowercase(), args));
        }
        if s.starts_with(['@', '$']) {
            let root = s.starts_with('$');
            let mut owned = s.to_owned();
            owned.replace_range(..1, "$");
            return Ok(Self::Path(root, Path::parse(owned.as_bytes())?));
        }
        let value = if s.starts_with('\'') {
            Node::new(Kind::String(path::quoted(s)?))
        } else {
            Node::parse(s.as_bytes()).map_err(|_| path::path_error())?
        };
        Ok(Self::Literal(value))
    }
    pub fn test(&self, root: &V, current: &V, work: &mut Work) -> Result<bool, Resp> {
        let values = self.eval(root, current, work)?;
        // Paths are existence tests, including present false/null values.
        Ok(if matches!(self, Self::Path(..)) {
            !values.is_empty()
        } else {
            values.iter().any(|v| truth(v))
        })
    }
    pub fn eval(&self, root: &V, current: &V, work: &mut Work) -> Result<Vec<V>, Resp> {
        work.charge(1 + NODE_ALLOCATION + std::mem::size_of::<V>())?;
        Ok(match self {
            Self::Literal(v) => vec![v.clone()],
            Self::Path(is_root, path) => {
                let start = if *is_root { root } else { current };
                let paths = path.resolve_from(root, start, work)?;
                value_slots(work, paths.len())?;
                paths
                    .iter()
                    .filter_map(|p| path::at(start, p).cloned())
                    .collect()
            }
            Self::Unary(op, a) => {
                if op == "!" {
                    vec![boolean(!a.test(root, current, work)?)]
                } else {
                    let a = a.eval(root, current, work)?;
                    if a.len() != 1 {
                        Vec::new()
                    } else if let Kind::Number(n) = &a[0].kind {
                        if op == "+" {
                            a
                        } else if let Some(n) = n.as_i64() {
                            n.checked_neg()
                                .map(|n| Node::new(Kind::Number(n.into())))
                                .into_iter()
                                .collect()
                        } else if !n.as_str().contains(['.', 'e', 'E']) {
                            vec![]
                        } else {
                            node::number(-n.as_f64().unwrap()).into_iter().collect()
                        }
                    } else {
                        Vec::new()
                    }
                }
            }
            Self::Keys(a) => key_projection(&a.eval(root, current, work)?, work)?,
            Self::Binary(op, a, b) => {
                if op == "&&" || op == "||" {
                    let a = a.test(root, current, work)?;
                    let yes = if op == "&&" {
                        a && b.test(root, current, work)?
                    } else {
                        a || b.test(root, current, work)?
                    };
                    vec![boolean(yes)]
                } else {
                    let mut av = a.eval(root, current, work)?;
                    let mut bv = b.eval(root, current, work)?;
                    if matches!(op.as_str(), "+" | "-" | "*" | "/" | "%") {
                        if av.len() != 1 || bv.len() != 1 {
                            Vec::new()
                        } else {
                            arithmetic(op, &av[0], &bv[0]).into_iter().collect()
                        }
                    } else {
                        if matches!(op.as_str(), "size" | "sizeof" | "empty") && av.len() != 1 {
                            work.charge(NODE_ALLOCATION)?;
                            av = vec![Node::new(Kind::Array(av))];
                        }
                        if matches!(op.as_str(), "in" | "nin" | "subsetof" | "anyof" | "noneof")
                            && bv.len() != 1
                        {
                            work.charge(NODE_ALLOCATION)?;
                            bv = vec![Node::new(Kind::Array(bv))];
                        }
                        let mut yes = false;
                        for a in &av {
                            for b in &bv {
                                work.charge(1 + a.nodes + b.nodes)?;
                                if compare(op, a, b, work)? {
                                    yes = true;
                                    break;
                                }
                            }
                            if yes {
                                break;
                            }
                        }
                        vec![boolean(yes)]
                    }
                }
            }
            Self::Call(name, args) => {
                work.charge(
                    args.len()
                        .checked_mul(std::mem::size_of::<Vec<V>>())
                        .ok_or_else(limit)?,
                )?;
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(arg.eval(root, current, work)?);
                }
                call(name, values, work)?
            }
        })
    }
}
fn truth(v: &Node) -> bool {
    match &v.kind {
        Kind::Null => false,
        Kind::Bool(b) => *b,
        Kind::Number(n) => n.as_f64().is_some_and(|f| f != 0.0),
        Kind::String(s) => !s.is_empty(),
        Kind::Array(a) => !a.is_empty(),
        Kind::Object(o) => !o.is_empty(),
    }
}
fn boolean(b: bool) -> V {
    Node::new(Kind::Bool(b))
}
fn length(v: &Node) -> Option<usize> {
    match &v.kind {
        Kind::Array(a) => Some(a.len()),
        Kind::Object(o) => Some(o.len()),
        Kind::String(s) => Some(s.chars().count()),
        _ => None,
    }
}
fn integer(n: usize) -> V {
    Node::new(Kind::Number(serde_json::Number::from(n as u64)))
}
fn arithmetic(op: &str, a: &Node, b: &Node) -> Option<V> {
    let (Kind::Number(a), Kind::Number(b)) = (&a.kind, &b.kind) else {
        return None;
    };
    if !a.as_str().contains(['.', 'e', 'E']) && !b.as_str().contains(['.', 'e', 'E']) {
        let (ai, bi) = (a.as_i64()?, b.as_i64()?);
        let value = match op {
            "+" => ai.checked_add(bi),
            "-" => ai.checked_sub(bi),
            "*" => ai.checked_mul(bi),
            "%" => ai.checked_rem(bi),
            "/" => return node::number(ai as f64 / bi as f64).ok(),
            _ => None,
        };
        return value.map(|v| Node::new(Kind::Number(v.into())));
    }
    let (a, b) = (a.as_f64()?, b.as_f64()?);
    let f = match op {
        "+" => a + b,
        "-" => a - b,
        "*" => a * b,
        "/" => a / b,
        "%" => a % b,
        _ => return None,
    };
    node::number(f).ok()
}
fn compare(op: &str, a: &Node, b: &Node, work: &mut Work) -> Result<bool, Resp> {
    Ok(match op {
        "==" => node::equal(a, b),
        "!=" => !node::equal(a, b),
        "<" | "<=" | ">" | ">=" => {
            let order = match (&a.kind, &b.kind) {
                (Kind::Number(a), Kind::Number(b)) => Some(node::numeric_cmp(a, b)),
                (Kind::String(a), Kind::String(b)) => Some(a.cmp(b)),
                _ => None,
            };
            order.is_some_and(|o| match op {
                "<" => o.is_lt(),
                "<=" => !o.is_gt(),
                ">" => o.is_gt(),
                _ => !o.is_lt(),
            })
        }
        "=~" => {
            if let (Kind::String(a), Kind::String(b)) = (&a.kind, &b.kind) {
                work.regex_match(a, b, false)?.unwrap_or(false)
            } else {
                false
            }
        }
        "in" | "nin" => {
            if let Kind::Array(values) = &b.kind {
                work.charge(
                    a.nodes
                        .checked_mul(values.len())
                        .and_then(|n| n.checked_add(b.nodes))
                        .ok_or_else(limit)?,
                )?;
                let found = values.iter().any(|b| node::equal(a, b));
                if op == "in" { found } else { !found }
            } else {
                op == "nin"
            }
        }
        "subsetof" | "anyof" | "noneof" => {
            if let (Kind::Array(a), Kind::Array(b)) = (&a.kind, &b.kind) {
                let an = a.iter().map(|v| v.nodes).sum::<usize>();
                let bn = b.iter().map(|v| v.nodes).sum::<usize>();
                work.charge(
                    an.checked_mul(b.len())
                        .and_then(|n| bn.checked_mul(a.len()).and_then(|m| n.checked_add(m)))
                        .ok_or_else(limit)?,
                )?;
                let found = |a: &V| b.iter().any(|b| node::equal(a, b));
                match op {
                    "subsetof" => a.iter().all(found),
                    "anyof" => a.iter().any(found),
                    _ => !a.iter().any(found),
                }
            } else {
                false
            }
        }
        "size" | "sizeof" => length(a).is_some_and(|n| {
            if let Kind::Number(b) = &b.kind {
                b.as_f64().is_some_and(|v| v.trunc() == n as f64)
            } else {
                false
            }
        }),
        "empty" => {
            if let Kind::Bool(b) = b.kind {
                length(a).is_some_and(|n| (n == 0) == b)
            } else {
                false
            }
        }
        _ => false,
    })
}
fn call(name: &str, args: Vec<Vec<V>>, work: &mut Work) -> Result<Vec<V>, Resp> {
    let one = |i: usize| args.get(i).filter(|v| v.len() == 1).and_then(|v| v.first());
    let singleton = |v: Option<V>| v.into_iter().collect::<Vec<_>>();
    let result = match name {
        "count" if args.len() == 1 => vec![integer(args[0].len())],
        "value" if args.len() == 1 => {
            if args[0].len() == 1 {
                args[0].clone()
            } else {
                vec![]
            }
        }
        "length" | "size" | "sizeof" if args.len() == 1 => {
            singleton(one(0).and_then(|v| length(v).map(integer)))
        }
        "keys" if args.len() == 1 => key_projection(&args[0], work)?,
        "abs" | "ceiling" | "floor" if args.len() == 1 => singleton(one(0).and_then(|v| {
            if let Kind::Number(n) = &v.kind {
                if let Some(i) = n.as_i64() {
                    let i = if name == "abs" { i.checked_abs()? } else { i };
                    Some(Node::new(Kind::Number(i.into())))
                } else if !n.as_str().contains(['.', 'e', 'E']) {
                    None
                } else {
                    let f = n.as_f64()?;
                    node::number(match name {
                        "abs" => f.abs(),
                        "ceiling" => f.ceil(),
                        _ => f.floor(),
                    })
                    .ok()
                }
            } else {
                None
            }
        })),
        "match" | "search" if args.len() == 2 => {
            let result = match (one(0), one(1)) {
                (Some(a), Some(b)) => {
                    if let (Kind::String(a), Kind::String(b)) = (&a.kind, &b.kind) {
                        Some(work.regex_match(a, b, name == "match")?.unwrap_or(false))
                    } else {
                        None
                    }
                }
                _ => None,
            };
            singleton(result.map(boolean))
        }
        "concat" if !args.is_empty() => {
            let mut s = String::new();
            let mut valid = true;
            for a in &args {
                if a.len() != 1 {
                    valid = false;
                    break;
                }
                if let Kind::String(a) = &a[0].kind {
                    work.charge(a.len())?;
                    s.push_str(a);
                } else {
                    valid = false;
                    break;
                }
            }
            if valid {
                vec![Node::new(Kind::String(s))]
            } else {
                vec![]
            }
        }
        "first" | "last" if args.len() == 1 => {
            let values = receiver(&args[0]);
            singleton(if name == "first" {
                values.first().cloned()
            } else {
                values.last().cloned()
            })
        }
        "index" if args.len() == 2 => {
            let values = receiver(&args[0]);
            singleton(one(1).and_then(|i| {
                if let Kind::Number(i) = &i.kind {
                    i.as_f64()
                        .map(f64::trunc)
                        .filter(|i| *i >= -(values.len() as f64) && *i < values.len() as f64)
                        .and_then(|i| path::index(i as i64, values.len()))
                        .map(|i| values[i].clone())
                } else {
                    None
                }
            }))
        }
        "append" if args.len() >= 2 => append(args, work)?,
        "min" | "max" | "avg" | "sum" | "stddev" if args.len() == 1 => {
            if let Some(a) = one(0) {
                if let Kind::Array(a) = &a.kind {
                    work.charge(a.len())?;
                    singleton(aggregate(name, a))
                } else {
                    vec![]
                }
            } else {
                vec![]
            }
        }
        _ => vec![],
    };
    for v in &result {
        node::check(v)?;
    }
    Ok(result)
}
fn receiver(values: &[V]) -> &[V] {
    if values.len() == 1
        && let Kind::Array(a) = &values[0].kind
    {
        return a;
    }
    values
}
fn append(args: Vec<Vec<V>>, work: &mut Work) -> Result<Vec<V>, Resp> {
    if args.iter().any(Vec::is_empty) {
        return Ok(vec![]);
    }
    let mut args = args.into_iter();
    let first = args.next().unwrap();
    let len = receiver(&first)
        .len()
        .checked_add(args.len())
        .ok_or_else(limit)?;
    value_slots(work, len)?;
    for arg in args.as_slice() {
        if arg.len() > 1 {
            work.charge(NODE_ALLOCATION)?;
        }
    }
    let mut values = if first.len() == 1 {
        if let Kind::Array(a) = &first[0].kind {
            let mut values = Vec::with_capacity(len);
            values.extend(a.iter().cloned());
            values
        } else {
            first
        }
    } else {
        first
    };
    values.reserve(len - values.len());
    for mut arg in args {
        values.push(if arg.len() == 1 {
            arg.pop().unwrap()
        } else {
            Node::new(Kind::Array(arg))
        });
    }
    Ok(values)
}
fn aggregate(name: &str, values: &[V]) -> Option<V> {
    if values.is_empty() || values.iter().any(|v| !matches!(v.kind, Kind::Number(_))) {
        return None;
    }
    if name == "min" || name == "max" {
        let mut selected = &values[0];
        for value in &values[1..] {
            let (Kind::Number(a), Kind::Number(b)) = (&value.kind, &selected.kind) else {
                unreachable!()
            };
            let order = node::numeric_cmp(a, b);
            if (name == "min" && order.is_lt()) || (name == "max" && order.is_gt()) {
                selected = value;
            }
        }
        return Some(selected.clone());
    }
    if name == "sum"
        && values.iter().all(|v| {
            if let Kind::Number(n) = &v.kind {
                !n.as_str().contains(['.', 'e', 'E'])
            } else {
                false
            }
        })
    {
        let mut sum = 0_i64;
        for value in values {
            let Kind::Number(n) = &value.kind else {
                unreachable!()
            };
            sum = sum.checked_add(n.as_i64()?)?;
        }
        return Some(Node::new(Kind::Number(sum.into())));
    }
    let (mut sum, mut mean, mut m2): (f64, f64, f64) = (0.0, 0.0, 0.0);
    for (i, value) in values.iter().enumerate() {
        let Kind::Number(n) = &value.kind else {
            unreachable!()
        };
        let value = n.as_f64()?;
        sum += value;
        let delta = value - mean;
        mean += delta / (i + 1) as f64;
        m2 += delta * (value - mean);
        if !sum.is_finite() || !mean.is_finite() || !m2.is_finite() {
            return None;
        }
    }
    node::number(match name {
        "sum" => sum,
        "avg" => mean,
        _ => (m2 / values.len() as f64).sqrt(),
    })
    .ok()
}
fn operator<'a>(s: &str, ops: &[&'a str]) -> Result<Option<(usize, &'a str)>, Resp> {
    let b = s.as_bytes();
    let mut quote = None;
    let mut escape = false;
    let mut stack = Vec::new();
    let mut found = None;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if let Some(q) = quote {
            if escape {
                escape = false;
            } else if c == b'\\' {
                escape = true;
            } else if c == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        if matches!(c, b'\'' | b'"') {
            quote = Some(c);
            i += 1;
            continue;
        }
        if matches!(c, b'(' | b'[' | b'{') {
            stack.push(c);
            i += 1;
            continue;
        }
        if matches!(c, b')' | b']' | b'}') {
            stack.pop();
            i += 1;
            continue;
        }
        if stack.is_empty() {
            for &op in ops {
                if s[i..].starts_with(op) {
                    let word = op.as_bytes()[0].is_ascii_alphabetic();
                    let arithmetic = matches!(op, "+" | "-" | "*" | "/" | "%");
                    let spaced = i > 0
                        && i + op.len() < b.len()
                        && b[i - 1].is_ascii_whitespace()
                        && b[i + op.len()].is_ascii_whitespace();
                    if (word || arithmetic) && !spaced {
                        continue;
                    }
                    if matches!(op, "<" | ">") && b.get(i + 1) == Some(&b'=') {
                        continue;
                    }
                    found = Some((i, op));
                    i += op.len() - 1;
                    break;
                }
            }
        }
        i += 1;
        while i < b.len() && !s.is_char_boundary(i) {
            i += 1;
        }
    }
    Ok(found)
}
fn call_open(s: &str) -> Result<Option<usize>, Resp> {
    let mut i = 0;
    let b = s.as_bytes();
    while i < b.len() {
        match b[i] {
            b'[' => i = path::closing(s, i, b'[', b']')? + 1,
            b'(' => {
                let end = path::closing(s, i, b'(', b')')?;
                if end == s.len() - 1 {
                    return Ok(Some(i));
                }
                i = end + 1;
            }
            _ => i += 1,
        }
    }
    Ok(None)
}
