use super::expr::Expr;
use super::node::{Kind, V};
use super::{MAX_DEPTH, MAX_MATCHES, MAX_WORK, Resp, error, limit};
use std::collections::{HashMap, HashSet};

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub(super) enum Part {
    Field(String),
    Index(usize),
}
pub(super) type Location = Vec<Part>;
#[derive(Clone)]
pub(super) enum Selector {
    Field(String),
    Index(i64),
    Wildcard,
    Slice(Option<i64>, Option<i64>, i64),
    Filter(Box<Expr>),
}
#[derive(Clone)]
pub(super) struct Segment {
    pub recursive: bool,
    pub selectors: Vec<Selector>,
}
#[derive(Clone)]
pub(super) struct Path {
    pub modern: bool,
    pub segments: Vec<Segment>,
}
pub(super) struct Work {
    remaining: usize,
    regexes: HashMap<(String, bool), Option<regex::Regex>>,
}
impl Work {
    pub fn new() -> Self {
        Self {
            remaining: MAX_WORK,
            regexes: HashMap::new(),
        }
    }
    pub fn charge(&mut self, n: usize) -> Result<(), Resp> {
        self.remaining = self.remaining.checked_sub(n).ok_or_else(limit)?;
        Ok(())
    }
    fn location(&mut self, path: &[Part]) -> Result<(), Resp> {
        self.charge(
            1 + path
                .iter()
                .map(|p| {
                    std::mem::size_of::<Part>()
                        + match p {
                            Part::Field(k) => k.len(),
                            Part::Index(_) => 0,
                        }
                })
                .sum::<usize>(),
        )
    }
    fn extension(&mut self, path: &[Part], key_bytes: usize) -> Result<(), Resp> {
        self.location(path)?;
        self.charge(std::mem::size_of::<Part>() + key_bytes)
    }
    pub fn regex_match(
        &mut self,
        text: &str,
        pattern: &str,
        anchored: bool,
    ) -> Result<Option<bool>, Resp> {
        self.charge(text.len() + pattern.len())?;
        if let Some(regex) = self.regexes.iter().find_map(|((p, a), r)| {
            if p == pattern && *a == anchored {
                Some(r)
            } else {
                None
            }
        }) {
            return Ok(regex.as_ref().map(|r| r.is_match(text)));
        }
        if self.regexes.len() >= 32 {
            return Err(limit());
        }
        self.charge(pattern.len() + 1024)?;
        let compiled = if anchored {
            format!("\\A(?:{pattern})\\z")
        } else {
            pattern.to_owned()
        };
        let regex = regex::RegexBuilder::new(&compiled)
            .size_limit(1024 * 1024)
            .dfa_size_limit(1024 * 1024)
            .build()
            .ok();
        let result = regex.as_ref().map(|r| r.is_match(text));
        self.regexes.insert((pattern.to_owned(), anchored), regex);
        Ok(result)
    }
}
pub(super) fn path_error() -> Resp {
    error("ERR invalid JSONPath expression")
}
pub(super) fn index(i: i64, len: usize) -> Option<usize> {
    let i = if i < 0 {
        (len as i128) + (i as i128)
    } else {
        i as i128
    };
    usize::try_from(i).ok().filter(|i| *i < len)
}
pub(super) fn at<'a>(root: &'a V, path: &[Part]) -> Option<&'a V> {
    let mut v = root;
    for p in path {
        v = match (p, &v.kind) {
            (Part::Field(k), Kind::Object(o)) => o.get(k)?,
            (Part::Index(i), Kind::Array(a)) => a.get(*i)?,
            _ => return None,
        };
    }
    Some(v)
}

impl Path {
    pub fn parse(bytes: &[u8]) -> Result<Self, Resp> {
        let mut s = std::str::from_utf8(bytes).map_err(|_| path_error())?;
        if s.len() > 16384 || s.is_empty() {
            return Err(path_error());
        }
        let mut parentheses = 0;
        while s.starts_with('(') && closing(s, 0, b'(', b')')? == s.len() - 1 {
            parentheses += 1;
            if parentheses > MAX_DEPTH {
                return Err(limit());
            }
            s = s[1..s.len() - 1].trim();
        }
        if s.is_empty() {
            return Err(path_error());
        }
        let modern = s == "$"
            || (s.starts_with('$')
                && s.as_bytes()
                    .get(1)
                    .is_some_and(|b| matches!(b, b'.' | b'[' | b'~')));
        if modern && s.len() > 1 && !matches!(s.as_bytes()[1], b'.' | b'[') {
            return Err(path_error());
        }
        let mut i = if modern { 1 } else { 0 };
        let mut segments = Vec::new();
        while i < s.len() {
            let mut recursive = false;
            if s.as_bytes()[i] == b'.' {
                i += 1;
                if i < s.len() && s.as_bytes()[i] == b'.' {
                    recursive = true;
                    i += 1;
                }
                if i == s.len() {
                    if segments.is_empty() && !modern && !recursive {
                        break;
                    }
                    return Err(path_error());
                }
            }
            let selectors = if s.as_bytes()[i] == b'[' {
                let end = closing(s, i, b'[', b']')?;
                let content = s[i + 1..end].trim();
                i = end + 1;
                parse_selectors(content)?
            } else if s.as_bytes()[i] == b'*' {
                i += 1;
                vec![Selector::Wildcard]
            } else {
                let start = i;
                while i < s.len()
                    && !matches!(s.as_bytes()[i], b'.' | b'[' | b']' | b'(' | b')' | b'~')
                    && !s.as_bytes()[i].is_ascii_whitespace()
                {
                    i += 1;
                }
                if i == start {
                    return Err(path_error());
                }
                vec![Selector::Field(s[start..i].to_owned())]
            };
            segments.push(Segment {
                recursive,
                selectors,
            });
            if segments.len() > MAX_DEPTH {
                return Err(limit());
            }
            if i < s.len() && !matches!(s.as_bytes()[i], b'.' | b'[') {
                return Err(path_error());
            }
        }
        Ok(Self { modern, segments })
    }
    pub fn resolve(&self, root: &V, work: &mut Work) -> Result<Vec<Location>, Resp> {
        self.resolve_from(root, root, work)
    }
    // Mutations address each node once; reads retain every selection occurrence.
    pub fn targets(&self, root: &V, work: &mut Work) -> Result<Vec<Location>, Resp> {
        self.resolve_with(root, root, work, true)
    }
    pub fn resolve_from(
        &self,
        root: &V,
        current: &V,
        work: &mut Work,
    ) -> Result<Vec<Location>, Resp> {
        self.resolve_with(root, current, work, false)
    }
    fn resolve_with(
        &self,
        root: &V,
        current: &V,
        work: &mut Work,
        unique: bool,
    ) -> Result<Vec<Location>, Resp> {
        let mut paths = vec![Vec::new()];
        for segment in &self.segments {
            let mut next = Vec::new();
            let mut seen = unique.then(HashSet::new);
            for p in paths {
                let Some(v) = at(current, &p) else { continue };
                if segment.recursive {
                    Self::walk(root, v, &p, segment, work, &mut next, &mut seen)?;
                } else {
                    select(root, v, &p, &segment.selectors, work, &mut next, &mut seen)?;
                }
            }
            paths = next;
        }
        Ok(paths)
    }
    fn walk(
        root: &V,
        v: &V,
        p: &Location,
        segment: &Segment,
        work: &mut Work,
        out: &mut Vec<Location>,
        seen: &mut Option<HashSet<Location>>,
    ) -> Result<(), Resp> {
        work.location(p)?;
        select(root, v, p, &segment.selectors, work, out, seen)?;
        match &v.kind {
            Kind::Array(a) => {
                for (i, v) in a.iter().enumerate() {
                    work.extension(p, 0)?;
                    let mut p = p.clone();
                    p.push(Part::Index(i));
                    Self::walk(root, v, &p, segment, work, out, seen)?;
                }
            }
            Kind::Object(o) => {
                for (k, v) in o {
                    work.extension(p, k.len())?;
                    let mut p = p.clone();
                    p.push(Part::Field(k.clone()));
                    Self::walk(root, v, &p, segment, work, out, seen)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    pub fn writable(&self, root: &V, work: &mut Work) -> Result<Vec<Location>, Resp> {
        if self.segments.is_empty() {
            return Ok(vec![vec![]]);
        }
        let mut parent = self.clone();
        let last = parent.segments.pop().unwrap();
        let mut matches = self.targets(root, work)?;
        let static_path = self.segments.iter().all(|s| {
            !s.recursive
                && s.selectors.len() == 1
                && matches!(s.selectors[0], Selector::Field(_) | Selector::Index(_))
        });
        if matches.is_empty()
            && static_path
            && let [Selector::Index(i)] = last.selectors.as_slice()
        {
            for p in parent.targets(root, work)? {
                if let Some(v) = at(root, &p)
                    && let Kind::Array(a) = &v.kind
                    && index(*i, a.len()).is_none()
                {
                    return Err(error("ERR index out of bounds"));
                }
            }
        }
        if !last.recursive
            && last
                .selectors
                .iter()
                .all(|s| matches!(s, Selector::Field(_)))
        {
            let mut seen = Some(HashSet::new());
            for mut p in parent.targets(root, work)? {
                if let Some(v) = at(root, &p)
                    && let Kind::Object(o) = &v.kind
                {
                    for s in &last.selectors {
                        let Selector::Field(k) = s else {
                            unreachable!()
                        };
                        if !o.contains_key(k) {
                            work.charge(k.len() + std::mem::size_of::<Part>())?;
                            p.push(Part::Field(k.clone()));
                            push(p.clone(), work, &mut matches, &mut seen)?;
                            p.pop();
                        }
                    }
                }
            }
        }
        Ok(matches)
    }
}
fn push(
    path: Location,
    work: &mut Work,
    out: &mut Vec<Location>,
    seen: &mut Option<HashSet<Location>>,
) -> Result<(), Resp> {
    work.location(&path)?;
    if out.len() >= MAX_MATCHES {
        return Err(limit());
    }
    if seen.as_mut().is_none_or(|seen| seen.insert(path.clone())) {
        out.push(path);
    }
    Ok(())
}
fn select(
    root: &V,
    v: &V,
    p: &Location,
    selectors: &[Selector],
    work: &mut Work,
    out: &mut Vec<Location>,
    seen: &mut Option<HashSet<Location>>,
) -> Result<(), Resp> {
    for s in selectors {
        work.charge(1)?;
        match s {
            Selector::Field(k) => {
                if let Kind::Object(o) = &v.kind
                    && o.contains_key(k)
                {
                    work.extension(p, k.len())?;
                    let mut p = p.clone();
                    p.push(Part::Field(k.clone()));
                    push(p, work, out, seen)?;
                }
            }
            Selector::Index(i) => {
                if let Kind::Array(a) = &v.kind
                    && let Some(i) = index(*i, a.len())
                {
                    work.extension(p, 0)?;
                    let mut p = p.clone();
                    p.push(Part::Index(i));
                    push(p, work, out, seen)?;
                }
            }
            Selector::Wildcard | Selector::Filter(_) => {
                let mut visit =
                    |field: Option<&String>, index: usize, child: &V| -> Result<(), Resp> {
                        work.charge(1)?;
                        let yes = if let Selector::Filter(expr) = s {
                            expr.test(root, child, work)?
                        } else {
                            true
                        };
                        if yes {
                            work.extension(p, field.map_or(0, String::len))?;
                            let mut p = p.clone();
                            p.push(if let Some(k) = field {
                                Part::Field(k.clone())
                            } else {
                                Part::Index(index)
                            });
                            push(p, work, out, seen)?;
                        }
                        Ok(())
                    };
                match &v.kind {
                    Kind::Array(a) => {
                        for (i, v) in a.iter().enumerate() {
                            visit(None, i, v)?;
                        }
                    }
                    Kind::Object(o) => {
                        for (k, v) in o {
                            visit(Some(k), 0, v)?;
                        }
                    }
                    _ => {}
                }
            }
            Selector::Slice(start, end, step) => {
                if let Kind::Array(a) = &v.kind {
                    let len = a.len() as i128;
                    let positive = *step > 0;
                    let normalize = |i: i64| {
                        let i = i as i128;
                        if i < 0 { len + i } else { i }
                    };
                    let (mut i, end) = if positive {
                        (
                            start.map(normalize).unwrap_or(0).clamp(0, len),
                            end.map(normalize).unwrap_or(len).clamp(0, len),
                        )
                    } else {
                        (
                            start.map(normalize).unwrap_or(len - 1).clamp(-1, len - 1),
                            end.map(normalize).unwrap_or(-1).clamp(-1, len - 1),
                        )
                    };
                    while if positive { i < end } else { i > end } {
                        work.charge(1)?;
                        work.extension(p, 0)?;
                        let mut p = p.clone();
                        p.push(Part::Index(i as usize));
                        push(p, work, out, seen)?;
                        i += *step as i128;
                    }
                }
            }
        }
    }
    Ok(())
}
fn parse_selectors(s: &str) -> Result<Vec<Selector>, Resp> {
    if let Some(s) = s.strip_prefix('?') {
        let s = s.trim();
        let s = if s.starts_with('(') && closing(s, 0, b'(', b')')? == s.len() - 1 {
            &s[1..s.len() - 1]
        } else {
            s
        };
        return Ok(vec![Selector::Filter(Box::new(Expr::parse(s)?))]);
    }
    let mut out = Vec::new();
    for part in split(s, b',')? {
        let part = part.trim();
        out.push(if part == "*" {
            Selector::Wildcard
        } else if part.starts_with(['\'', '"']) {
            Selector::Field(quoted(part)?)
        } else if part.contains(':') {
            let pieces: Vec<_> = part.split(':').collect();
            if !(2..=3).contains(&pieces.len()) {
                return Err(path_error());
            }
            let parse = |s: &str| {
                if s.trim().is_empty() {
                    Ok(None)
                } else {
                    s.trim().parse::<i64>().map(Some).map_err(|_| path_error())
                }
            };
            let step = if pieces.len() == 3 {
                parse(pieces[2])?.unwrap_or(1)
            } else {
                1
            };
            if step == 0 {
                return Err(path_error());
            }
            Selector::Slice(parse(pieces[0])?, parse(pieces[1])?, step)
        } else {
            Selector::Index(part.parse().map_err(|_| path_error())?)
        });
        if out.len() > MAX_MATCHES {
            return Err(limit());
        }
    }
    if out.is_empty() {
        return Err(path_error());
    }
    Ok(out)
}
pub(super) fn quoted(s: &str) -> Result<String, Resp> {
    if s.starts_with('"') {
        return serde_json::from_str(s).map_err(|_| path_error());
    }
    if !s.starts_with('\'') || !s.ends_with('\'') || s.len() < 2 {
        return Err(path_error());
    }
    let mut out = String::new();
    let mut chars = s[1..s.len() - 1].chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            let n = chars.next().ok_or_else(path_error)?;
            match n {
                '\'' => out.push('\''),
                '"' => out.push('"'),
                '\\' => out.push('\\'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                't' => out.push('\t'),
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'u' => {
                    let mut encoded = String::from("\"\\u");
                    for _ in 0..4 {
                        encoded.push(chars.next().ok_or_else(path_error)?);
                    }
                    if encoded[3..].starts_with(['d', 'D']) {
                        let mut rest = chars.clone();
                        if rest.next() == Some('\\') && rest.next() == Some('u') {
                            encoded.push_str("\\u");
                            chars.next();
                            chars.next();
                            for _ in 0..4 {
                                encoded.push(chars.next().ok_or_else(path_error)?);
                            }
                        }
                    }
                    encoded.push('"');
                    out.push_str(
                        &serde_json::from_str::<String>(&encoded).map_err(|_| path_error())?,
                    );
                }
                _ => return Err(path_error()),
            }
        } else {
            if c == '\'' || c.is_control() {
                return Err(path_error());
            }
            out.push(c);
        }
    }
    Ok(out)
}
pub(super) fn closing(s: &str, start: usize, open: u8, close: u8) -> Result<usize, Resp> {
    let b = s.as_bytes();
    let mut depth = 0;
    let mut quote = None;
    let mut escape = false;
    for (i, &c) in b.iter().enumerate().skip(start) {
        if let Some(q) = quote {
            if escape {
                escape = false;
            } else if c == b'\\' {
                escape = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
        } else if c == open {
            depth += 1;
            if depth > MAX_DEPTH {
                return Err(limit());
            }
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                return Ok(i);
            }
        }
    }
    Err(path_error())
}
pub(super) fn split(s: &str, delimiter: u8) -> Result<Vec<&str>, Resp> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut quote = None;
    let mut escape = false;
    let mut stack = Vec::new();
    for (i, c) in s.bytes().enumerate() {
        if let Some(q) = quote {
            if escape {
                escape = false;
            } else if c == b'\\' {
                escape = true;
            } else if c == q {
                quote = None;
            }
            continue;
        }
        if c == b'\'' || c == b'"' {
            quote = Some(c);
        } else if matches!(c, b'[' | b'(' | b'{') {
            stack.push(c);
            if stack.len() > MAX_DEPTH {
                return Err(limit());
            }
        } else if matches!(c, b']' | b')' | b'}') {
            let expected = match c {
                b']' => b'[',
                b')' => b'(',
                _ => b'{',
            };
            if stack.pop() != Some(expected) {
                return Err(path_error());
            }
        } else if c == delimiter && stack.is_empty() {
            out.push(&s[start..i]);
            start = i + 1;
        }
    }
    if quote.is_some() || !stack.is_empty() {
        return Err(path_error());
    }
    out.push(&s[start..]);
    Ok(out)
}
