use super::model::*;
use super::{text, vector};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::time::{Duration, Instant};

#[derive(Clone, Debug)]
pub struct Options {
    pub dialect: u32,
    pub verbatim: bool,
    pub nostopwords: bool,
    pub scorer: String,
    pub slop: i64,
    pub inorder: bool,
    pub params: BTreeMap<String, Vec<u8>>,
    pub fields: Vec<String>,
    pub language: String,
    pub expander: String,
    pub timeout_ms: u64,
    pub payload: Vec<u8>,
    pub bm25std_tanh_factor: f64,
    pub explain_score: bool,
    pub filters: Vec<String>,
    pub keys: Option<DocSet>,
}
impl Default for Options {
    fn default() -> Self {
        Self {
            dialect: 0,
            verbatim: false,
            nostopwords: false,
            scorer: String::new(),
            slop: -1,
            inorder: false,
            params: BTreeMap::new(),
            fields: Vec::new(),
            language: String::new(),
            expander: "DEFAULT".into(),
            timeout_ms: 0,
            payload: Vec::new(),
            bm25std_tanh_factor: 0.0,
            explain_score: false,
            filters: Vec::new(),
            keys: None,
        }
    }
}
#[derive(Clone, Debug)]
enum TermKind {
    Plain,
    Prefix,
    Suffix,
    Contains,
    Wildcard,
    Fuzzy(usize),
}
#[derive(Clone, Debug)]
struct Attributes {
    weight: f64,
    slop: Option<i64>,
    inorder: Option<bool>,
    phonetic: Option<bool>,
    distance: Option<String>,
    runtime: BTreeMap<String, String>,
}
impl Default for Attributes {
    fn default() -> Self {
        Self {
            weight: 1.0,
            slop: None,
            inorder: None,
            phonetic: None,
            distance: None,
            runtime: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Debug)]
enum Node {
    All,
    None,
    Term(String, TermKind),
    Phrase(String),
    Scope(Vec<String>, Box<Node>),
    And(Vec<Node>),
    Or(Vec<Node>),
    Not(Box<Node>),
    Optional(Box<Node>),
    Numeric(String, f64, bool, f64, bool),
    Tag(String, Vec<(String, TermKind)>),
    Geo(String, f64, f64, f64, String),
    Shape(String, String, String),
    Missing(String),
    VectorRange(String, f64, String, BTreeMap<String, String>),
    Knn(
        Box<Node>,
        String,
        usize,
        String,
        BTreeMap<String, String>,
        String,
    ),
    Attributes(Box<Node>, Attributes),
}
struct Parser<'a> {
    input: &'a str,
    at: usize,
    dialect: u32,
    options: &'a Options,
    depth: usize,
    nodes: usize,
}
impl<'a> Parser<'a> {
    fn new(input: &'a str, options: &'a Options, config: &Config) -> Result<Self, String> {
        let dialect = if options.dialect == 0 {
            u32::from(config.default_dialect)
        } else {
            options.dialect
        };
        if !(1..=4).contains(&dialect) {
            return Err("DIALECT must be between 1 and 4".into());
        }
        if input.len() > 1_048_576 {
            return Err("Query exceeds maximum length".into());
        }
        if dialect == 1 && !options.params.is_empty() {
            return Err("PARAMS requires DIALECT 2 or greater".into());
        }
        Ok(Self {
            input,
            at: 0,
            dialect,
            options,
            depth: 0,
            nodes: 0,
        })
    }
    fn rest(&self) -> &str {
        &self.input[self.at..]
    }
    fn peek(&self) -> Option<char> {
        self.rest().chars().next()
    }
    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.at += c.len_utf8();
        Some(c)
    }
    fn ws(&mut self) {
        while self.peek().is_some_and(char::is_whitespace) {
            self.bump();
        }
    }
    fn error(&self, message: &str) -> String {
        format!("Syntax error at offset {}: {message}", self.at)
    }
    fn take(&mut self, s: &str) -> bool {
        if self.rest().starts_with(s) {
            self.at += s.len();
            true
        } else {
            false
        }
    }
    fn parse(mut self) -> Result<Node, String> {
        self.ws();
        if self.peek().is_none() {
            return Ok(Node::None);
        }
        let node = self.expr()?;
        self.ws();
        let node = self.postfix(node, true)?;
        self.ws();
        if self.peek().is_some() {
            return Err(self.error("unexpected token"));
        }
        Ok(node)
    }
    fn expr(&mut self) -> Result<Node, String> {
        self.depth += 1;
        if self.depth > 128 {
            return Err(self.error("query nesting limit exceeded"));
        }
        let result = if self.dialect == 1 {
            self.and_v1()
        } else {
            self.or_v2()
        };
        self.depth -= 1;
        result
    }
    fn ends(&mut self) -> bool {
        self.ws();
        self.peek().is_none() || self.peek() == Some(')') || self.rest().starts_with("=>")
    }
    fn and_v1(&mut self) -> Result<Node, String> {
        let mut children = Vec::new();
        while !self.ends() {
            let mut union = vec![self.unary()?];
            self.ws();
            while self.take("|") {
                self.ws();
                if self.ends() {
                    return Err(self.error("missing union operand"));
                }
                union.push(self.unary()?);
                self.ws();
            }
            children.push(if union.len() == 1 {
                union.pop().unwrap()
            } else {
                Node::Or(union)
            });
        }
        if children.is_empty() {
            Err(self.error("missing expression"))
        } else if children.len() == 1 {
            Ok(children.pop().unwrap())
        } else {
            Ok(Node::And(children))
        }
    }
    fn or_v2(&mut self) -> Result<Node, String> {
        let mut children = vec![self.and_v2()?];
        self.ws();
        while self.take("|") {
            self.ws();
            children.push(self.and_v2()?);
            self.ws();
        }
        if children.len() == 1 {
            Ok(children.pop().unwrap())
        } else {
            Ok(Node::Or(children))
        }
    }
    fn and_v2(&mut self) -> Result<Node, String> {
        let mut children = Vec::new();
        while !self.ends() && self.peek() != Some('|') {
            children.push(self.unary()?);
        }
        if children.is_empty() {
            Err(self.error("missing expression"))
        } else if children.len() == 1 {
            Ok(children.pop().unwrap())
        } else {
            Ok(Node::And(children))
        }
    }
    fn unary(&mut self) -> Result<Node, String> {
        self.depth += 1;
        if self.depth > 128 {
            return Err(self.error("query nesting limit exceeded"));
        }
        let result = self.unary_inner();
        self.depth -= 1;
        result
    }
    fn unary_inner(&mut self) -> Result<Node, String> {
        self.ws();
        self.nodes += 1;
        if self.nodes > 4096 {
            return Err(self.error("too many query nodes"));
        }
        if self.take("-") {
            let child = if self.dialect == 1 {
                self.expr()?
            } else {
                self.unary()?
            };
            return Ok(Node::Not(Box::new(child)));
        }
        if self.take("~") {
            return Ok(Node::Optional(Box::new(self.unary()?)));
        }
        let node = self.atom()?;
        self.postfix(node, false)
    }
    fn postfix(&mut self, mut node: Node, allow_knn: bool) -> Result<Node, String> {
        loop {
            self.ws();
            if !allow_knn
                && self
                    .rest()
                    .strip_prefix("=>")
                    .is_some_and(|s| s.trim_start().starts_with('['))
            {
                break;
            }
            if !self.take("=>") {
                break;
            }
            self.ws();
            if self.take("{") {
                node = Node::Attributes(Box::new(node), self.attributes()?);
            } else if self.take("[") {
                if self.dialect < 2 {
                    return Err(self.error("vector queries require DIALECT 2"));
                }
                let parts = self.bracket_parts()?;
                if parts.len() < 4 || !parts[0].eq_ignore_ascii_case("KNN") {
                    return Err(self.error("expected KNN query"));
                }
                let k = self
                    .value(&parts[1])?
                    .parse::<usize>()
                    .map_err(|_| self.error("invalid KNN count"))?;
                let field = parts[2]
                    .strip_prefix('@')
                    .ok_or_else(|| self.error("vector field must begin with @"))?
                    .to_string();
                let blob = parts[3].clone();
                if !blob.starts_with('$') {
                    return Err(self.error("vector must be a parameter"));
                }
                let mut alias = format!("__{field}_score");
                let mut runtime = BTreeMap::new();
                let mut i = 4;
                let mut seen = BTreeSet::new();
                while i < parts.len() {
                    if i + 1 >= parts.len() {
                        return Err(self.error("missing vector option value"));
                    }
                    let name = parts[i].to_ascii_uppercase();
                    let value = self.value(&parts[i + 1])?;
                    if !seen.insert(name.clone()) {
                        return Err(self.error("duplicate vector option"));
                    }
                    if name == "AS" {
                        if value.is_empty() {
                            return Err(self.error("empty distance alias"));
                        }
                        alias = value;
                    } else {
                        runtime.insert(name, value);
                    }
                    i += 2;
                }
                node = Node::Knn(Box::new(node), field, k, blob, runtime, alias);
            } else {
                return Err(self.error("expected query attributes or vector clause"));
            }
        }
        Ok(node)
    }
    fn atom(&mut self) -> Result<Node, String> {
        self.ws();
        if self.take("(") {
            let node = self.expr()?;
            self.ws();
            if !self.take(")") {
                return Err(self.error("unclosed group"));
            }
            return Ok(node);
        }
        if self
            .rest()
            .get(..10)
            .is_some_and(|s| s.eq_ignore_ascii_case("ismissing("))
            && {
                self.at += 10;
                true
            }
        {
            self.ws();
            if !self.take("@") {
                return Err(self.error("ismissing requires a field"));
            }
            let field = self.read_until(&[')', ' ', '\t']);
            self.ws();
            if !self.take(")") {
                return Err(self.error("unclosed ismissing"));
            }
            if self.dialect < 2 {
                return Err(self.error("ismissing requires DIALECT 2"));
            }
            return Ok(Node::Missing(field));
        }
        if self.take("@") {
            return self.field();
        }
        if self.peek() == Some('"') {
            return Ok(Node::Phrase(self.quoted('"')?));
        }
        if self.rest().starts_with("w'") || self.rest().starts_with("W'") {
            if self.dialect < 2 {
                return Err(self.error("wildcard patterns require DIALECT 2"));
            }
            self.bump();
            let pattern = self.quoted('\'')?;
            let pattern = self.value(&pattern)?;
            return Ok(Node::Term(text::normalize(&pattern), TermKind::Wildcard));
        }
        let token = self.word();
        if token.is_empty() {
            return Err(self.error("expected term"));
        }
        if token == "*" {
            return Ok(Node::All);
        }
        let value = self.value(&token)?;
        let (value, kind) = term_pattern(&value)?;
        Ok(Node::Term(text::normalize(&value), kind))
    }
    fn field(&mut self) -> Result<Node, String> {
        let mut fields = Vec::new();
        loop {
            let name = self.read_until(&['|', ':', '=', '!', '>', '<', ' ', '\t', '(', ')']);
            if name.is_empty() {
                return Err(self.error("missing field name"));
            }
            fields.push(name);
            if !self.take("|") {
                break;
            }
            self.take("@");
        }
        self.ws();
        if self
            .peek()
            .is_some_and(|c| matches!(c, '=' | '!' | '>' | '<'))
        {
            if self.dialect < 2 || fields.len() != 1 {
                return Err(self.error("numeric comparison requires DIALECT 2 and one field"));
            }
            let mut op = String::new();
            op.push(self.bump().unwrap());
            if self.peek() == Some('=') {
                op.push(self.bump().unwrap());
            }
            self.ws();
            let word = self.word();
            let number = parse_number(&self.value(&word)?)?;
            let f = fields.pop().unwrap();
            return Ok(match op.as_str() {
                "==" => Node::Numeric(f, number, true, number, true),
                "!=" => Node::Not(Box::new(Node::Numeric(f, number, true, number, true))),
                ">" => Node::Numeric(f, number, false, f64::INFINITY, true),
                ">=" => Node::Numeric(f, number, true, f64::INFINITY, true),
                "<" => Node::Numeric(f, f64::NEG_INFINITY, true, number, false),
                "<=" => Node::Numeric(f, f64::NEG_INFINITY, true, number, true),
                _ => return Err(self.error("invalid numeric comparison")),
            });
        }
        if !self.take(":") {
            return Err(self.error("expected colon after field"));
        }
        self.ws();
        if self.take("{") {
            if fields.len() != 1 {
                return Err(self.error("TAG query requires one field"));
            }
            let mut tags = Vec::new();
            let mut tag = String::new();
            let mut escaped = false;
            let mut quoted = false;
            let mut closed = false;
            while let Some(c) = self.bump() {
                if escaped {
                    tag.push('\\');
                    tag.push(c);
                    escaped = false;
                    continue;
                }
                if c == '\\' {
                    escaped = true;
                    continue;
                }
                if c == '"' {
                    quoted = !quoted;
                    continue;
                }
                if !quoted && (c == '|' || c == '}') {
                    let value = self.value(tag.trim())?;
                    tags.push(term_pattern(&value)?);
                    tag.clear();
                    if c == '}' {
                        closed = true;
                        break;
                    }
                } else {
                    tag.push(c);
                }
            }
            if !closed || quoted || escaped {
                return Err(self.error("unclosed TAG query"));
            }
            return Ok(Node::Tag(fields.pop().unwrap(), tags));
        }
        if self.take("[") {
            if fields.len() != 1 {
                return Err(self.error("range query requires one field"));
            }
            let parts = self.bracket_parts()?;
            let field = fields.pop().unwrap();
            if parts.is_empty() {
                return Err(self.error("empty range"));
            }
            if parts[0].eq_ignore_ascii_case("VECTOR_RANGE") {
                if self.dialect < 2 || parts.len() < 3 || (parts.len() - 3) % 2 != 0 {
                    return Err(self.error("invalid VECTOR_RANGE query"));
                }
                let radius = parse_number(&self.value(&parts[1])?)?;
                if radius < 0.0 || !radius.is_finite() {
                    return Err(self.error("invalid vector radius"));
                }
                let blob = parts[2].clone();
                if !blob.starts_with('$') {
                    return Err(self.error("vector must be a parameter"));
                }
                let mut runtime = BTreeMap::new();
                for pair in parts[3..].as_chunks::<2>().0 {
                    if runtime
                        .insert(pair[0].to_ascii_uppercase(), self.value(&pair[1])?)
                        .is_some()
                    {
                        return Err(self.error("duplicate vector parameter"));
                    }
                }
                return Ok(Node::VectorRange(field, radius, blob, runtime));
            }
            if ["WITHIN", "CONTAINS", "INTERSECTS", "DISJOINT"]
                .iter()
                .any(|v| parts[0].eq_ignore_ascii_case(v))
            {
                if self.dialect < 3 || parts.len() < 2 {
                    return Err(self.error("GEOSHAPE requires DIALECT 3"));
                }
                let geometry = parts[1..]
                    .iter()
                    .map(|p| self.value(p))
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" ");
                return Ok(Node::Shape(field, parts[0].to_ascii_uppercase(), geometry));
            }
            if parts.len() == 4 {
                return Ok(Node::Geo(
                    field,
                    parse_number(&self.value(&parts[0])?)?,
                    parse_number(&self.value(&parts[1])?)?,
                    parse_number(&self.value(&parts[2])?)?,
                    self.value(&parts[3])?,
                ));
            }
            if parts.len() != 2 && !(parts.len() == 1 && self.dialect >= 2) {
                return Err(self.error("numeric range needs two bounds"));
            }
            let bound = |s: &str| -> Result<(f64, bool), String> {
                let inclusive = !s.starts_with('(');
                let v = if inclusive { s } else { &s[1..] };
                Ok((parse_number(&self.value(v)?)?, inclusive))
            };
            let (min, mi) = bound(&parts[0])?;
            let (max, ma) = if parts.len() == 1 {
                (min, mi)
            } else {
                bound(&parts[1])?
            };
            return Ok(Node::Numeric(field, min, mi, max, ma));
        }
        let node = if self.peek() == Some('(') {
            self.atom()?
        } else if self.dialect == 1 {
            self.expr()?
        } else {
            self.unary()?
        };
        Ok(Node::Scope(fields, Box::new(node)))
    }
    fn value(&self, word: &str) -> Result<String, String> {
        if (word.starts_with("w'") || word.starts_with("W'")) && word.ends_with('\'') {
            return Ok(format!(
                "w'{}'",
                self.parameter_value(&word[2..word.len() - 1])?
            ));
        }
        self.parameter_value(word)
    }
    fn parameter_value(&self, word: &str) -> Result<String, String> {
        let prefix = word.chars().take_while(|c| matches!(c, '%' | '*')).count();
        let body = &word[prefix..];
        if let Some(raw) = body.strip_prefix('$') {
            if self.dialect < 2 {
                return Err(self.error("parameters require DIALECT 2"));
            }
            let (name, suffix) = if self.options.params.contains_key(raw) {
                (raw, "")
            } else {
                let end = raw.trim_end_matches(['%', '*', '?']).len();
                (&raw[..end], &raw[end..])
            };
            let value = self
                .options
                .params
                .get(name)
                .ok_or_else(|| format!("No such parameter `{name}`"))?;
            let value = std::str::from_utf8(value)
                .map_err(|_| format!("Parameter `{name}` is not valid UTF-8"))?;
            return Ok(format!("{}{value}{suffix}", &word[..prefix]));
        }
        Ok(word.to_owned())
    }
    fn quoted(&mut self, quote: char) -> Result<String, String> {
        if self.bump() != Some(quote) {
            return Err(self.error("expected quote"));
        }
        let mut value = String::new();
        let mut escaped = false;
        while let Some(c) = self.bump() {
            if escaped {
                value.push('\\');
                value.push(c);
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == quote {
                return Ok(value);
            } else {
                value.push(c);
            }
        }
        Err(self.error("unclosed quote"))
    }
    fn word(&mut self) -> String {
        let mut word = String::new();
        let mut escaped = false;
        while let Some(c) = self.peek() {
            if !escaped
                && (c.is_whitespace()
                    || matches!(
                        c,
                        '|' | '(' | ')' | '{' | '}' | '[' | ']' | '"' | '@' | ':' | ';'
                    )
                    || self.rest().starts_with("=>"))
            {
                break;
            }
            self.bump();
            word.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            }
        }
        word
    }
    fn read_until(&mut self, delimiters: &[char]) -> String {
        let mut out = String::new();
        let mut escaped = false;
        while let Some(c) = self.peek() {
            if !escaped && delimiters.contains(&c) {
                break;
            }
            self.bump();
            if escaped {
                out.push(c);
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else {
                out.push(c);
            }
        }
        out
    }
    fn bracket_parts(&mut self) -> Result<Vec<String>, String> {
        let mut parts = Vec::new();
        let mut current = String::new();
        let mut escaped = false;
        let mut quote = None;
        while let Some(c) = self.bump() {
            if escaped {
                current.push('\\');
                current.push(c);
                escaped = false;
                continue;
            }
            if c == '\\' {
                escaped = true;
                continue;
            }
            if let Some(q) = quote {
                if c == q {
                    quote = None;
                } else {
                    current.push(c);
                }
                continue;
            }
            if c == '"' || c == '\'' {
                quote = Some(c);
                continue;
            }
            if c == ']' {
                if !current.is_empty() {
                    parts.push(current);
                }
                return Ok(parts);
            }
            if c.is_whitespace() {
                if !current.is_empty() {
                    parts.push(std::mem::take(&mut current));
                }
            } else {
                current.push(c);
            }
        }
        Err(self.error("unclosed range"))
    }
    fn attributes(&mut self) -> Result<Attributes, String> {
        let mut attrs = Attributes::default();
        let mut names = BTreeSet::new();
        loop {
            self.ws();
            if self.take("}") {
                break;
            }
            self.take("$");
            let name = self.read_until(&[':', ' ', '\t']).to_ascii_uppercase();
            self.ws();
            if !names.insert(name.clone()) {
                return Err(self.error("duplicate query attribute"));
            }
            if !self.take(":") {
                return Err(self.error("expected attribute value"));
            }
            self.ws();
            let raw = if self.peek() == Some('"') {
                self.quoted('"')?
            } else {
                self.read_until(&[';', '}'])
            };
            let value = self.value(raw.trim())?;
            match name.as_str() {
                "WEIGHT" => {
                    attrs.weight = parse_number(&value)?;
                    if attrs.weight < 0.0 || !attrs.weight.is_finite() {
                        return Err(self.error("weight must be nonnegative"));
                    }
                }
                "SLOP" => {
                    let n = value
                        .parse::<i64>()
                        .map_err(|_| self.error("invalid slop"))?;
                    if n < -1 {
                        return Err(self.error("invalid slop"));
                    }
                    attrs.slop = Some(n);
                }
                "INORDER" => attrs.inorder = Some(parse_bool(&value)?),
                "PHONETIC" => attrs.phonetic = Some(parse_bool(&value)?),
                "YIELD_DISTANCE_AS" => {
                    if value.is_empty() {
                        return Err(self.error("empty distance alias"));
                    }
                    attrs.distance = Some(value);
                }
                "EF_RUNTIME"
                | "EPSILON"
                | "HYBRID_POLICY"
                | "BATCH_SIZE"
                | "SHARD_K_RATIO"
                | "SEARCH_WINDOW_SIZE"
                | "USE_SEARCH_HISTORY"
                | "SEARCH_BUFFER_CAPACITY" => {
                    attrs.runtime.insert(name, value);
                }
                _ => return Err(format!("Unknown query attribute `{name}`")),
            }
            self.ws();
            if self.take("}") {
                break;
            }
            if !self.take(";") {
                return Err(self.error("expected semicolon"));
            }
        }
        Ok(attrs)
    }
}
fn unescape(value: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for c in value.chars() {
        if escaped {
            out.push(c);
            escaped = false;
        } else if c == '\\' {
            escaped = true;
        } else {
            out.push(c);
        }
    }
    if escaped {
        out.push('\\');
    }
    out
}
fn parse_number(value: &str) -> Result<f64, String> {
    let n = match value.to_ascii_lowercase().as_str() {
        "inf" | "+inf" => f64::INFINITY,
        "-inf" => f64::NEG_INFINITY,
        _ => value
            .parse::<f64>()
            .map_err(|_| format!("Invalid numeric value `{value}`"))?,
    };
    if n.is_nan() {
        Err("NaN is not a valid numeric bound".into())
    } else {
        Ok(n)
    }
}
fn parse_bool(value: &str) -> Result<bool, String> {
    match value.to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!("Invalid boolean `{value}`")),
    }
}
fn term_pattern(value: &str) -> Result<(String, TermKind), String> {
    let percent = value.chars().take_while(|c| *c == '%').count();
    if percent > 0 {
        if percent > 3 || !value.ends_with(&"%".repeat(percent)) || value.len() <= percent * 2 {
            return Err("Invalid fuzzy query".into());
        }
        return Ok((
            unescape(&value[percent..value.len() - percent]),
            TermKind::Fuzzy(percent),
        ));
    }
    if value.len() >= 3
        && (value.starts_with("w'") || value.starts_with("W'"))
        && value.ends_with('\'')
    {
        return Ok((value[2..value.len() - 1].to_owned(), TermKind::Wildcard));
    }
    if value == "*" {
        return Ok((String::new(), TermKind::Wildcard));
    }
    let start = value.starts_with('*');
    let preceding = value
        .strip_suffix('*')
        .unwrap_or(value)
        .chars()
        .rev()
        .take_while(|c| *c == '\\')
        .count();
    let end = value.ends_with('*') && preceding % 2 == 0;
    let kind = match (start, end) {
        (true, true) => TermKind::Contains,
        (true, false) => TermKind::Suffix,
        (false, true) => TermKind::Prefix,
        _ => TermKind::Plain,
    };
    let from = usize::from(start);
    let to = value.len() - usize::from(end);
    if from > to {
        return Err("Invalid wildcard query".into());
    }
    Ok((
        if matches!(kind, TermKind::Plain) {
            value.to_owned()
        } else {
            unescape(&value[from..to])
        },
        kind,
    ))
}

#[derive(Clone, Default)]
struct Match {
    score: f64,
    distance: Option<f64>,
    terms: BTreeSet<String>,
    distances: BTreeMap<String, f64>,
    positions: BTreeMap<String, Vec<u32>>,
    parts: Vec<serde_json::Value>,
}
type Matches = BTreeMap<DocId, Match>;
struct Evaluator<'a> {
    index: &'a Index,
    options: &'a Options,
    config: &'a Config,
    now: i64,
    started: Instant,
    timeout: Duration,
    visited: usize,
    work: usize,
    scorer: String,
    halted: bool,
}
impl<'a> Evaluator<'a> {
    fn tick(&mut self, n: usize) -> Result<(), String> {
        self.visited = self.visited.saturating_add(n);
        self.work = self.work.saturating_add(n);
        if self.work > 50_000_000 {
            return Err("Query work limit exceeded".into());
        }
        if !self.timeout.is_zero() && self.started.elapsed() > self.timeout {
            if self.config.on_timeout_fail {
                return Err("Timeout limit was reached".into());
            }
            self.halted = true;
        }
        Ok(())
    }
    fn live(&self, id: DocId) -> bool {
        self.options
            .keys
            .as_ref()
            .is_none_or(|keys| keys.contains(&id))
            && self
                .index
                .docs
                .get(&id)
                .is_some_and(|d| d.expires_at.is_none_or(|t| t > self.now))
    }
    fn universe(&mut self) -> Result<Matches, String> {
        let mut result = Matches::new();
        for id in self.index.docs.keys() {
            self.tick(1)?;
            if self.halted {
                break;
            }
            if self.live(*id) {
                result.insert(*id, Match::default());
            }
        }
        Ok(result)
    }
    fn matches_from_ids(&mut self, ids: &DocSet) -> Result<Matches, String> {
        let mut result = Matches::new();
        for id in ids {
            self.tick(1)?;
            if self.halted {
                break;
            }
            if self.live(*id) {
                result.insert(*id, Match::default());
            }
        }
        Ok(result)
    }
    fn field(&self, name: &str, kind: FieldKind) -> Result<&'a Field, String> {
        let f = self
            .index
            .schema
            .fields
            .iter()
            .find(|f| f.name == name)
            .ok_or_else(|| format!("Unknown field `{name}`"))?;
        if std::mem::discriminant(&f.kind) != std::mem::discriminant(&kind) {
            return Err(format!("Wrong field type for `{name}`"));
        }
        if f.noindex {
            return Err(format!("Field `{name}` is not indexed"));
        }
        Ok(f)
    }
    fn text_fields(&self, scope: &[String]) -> Result<Vec<&'a Field>, String> {
        if scope.is_empty() {
            return Ok(self
                .index
                .schema
                .fields
                .iter()
                .filter(|f| matches!(f.kind, FieldKind::Text) && !f.noindex)
                .collect());
        }
        scope
            .iter()
            .map(|s| self.field(s, FieldKind::Text))
            .collect()
    }
    fn eval(
        &mut self,
        node: &Node,
        scope: &[String],
        attrs: &Attributes,
    ) -> Result<Matches, String> {
        self.tick(1)?;
        if self.halted {
            return Ok(Matches::new());
        }
        match node {
            Node::All => self.universe(),
            Node::None => Ok(BTreeMap::new()),
            Node::Term(value, kind) => self.term(value, kind, scope, attrs),
            Node::Phrase(value) => self.phrase(value, scope, attrs),
            Node::Scope(fields, child) => {
                self.text_fields(fields)?;
                self.eval(
                    child,
                    if self.index.schema.nofields {
                        &[]
                    } else {
                        fields
                    },
                    attrs,
                )
            }
            Node::Attributes(child, a) => {
                let mut merged = attrs.clone();
                merged.weight *= a.weight;
                if a.slop.is_some() {
                    merged.slop = a.slop;
                }
                if a.inorder.is_some() {
                    merged.inorder = a.inorder;
                }
                if a.phonetic.is_some() {
                    merged.phonetic = a.phonetic;
                }
                if a.distance.is_some() {
                    merged.distance = a.distance.clone();
                }
                merged.runtime.extend(a.runtime.clone());
                let mut result = self.eval(child, scope, &merged)?;
                for m in result.values_mut() {
                    m.score *= a.weight;
                    if self.options.explain_score {
                        m.parts = vec![score_node(
                            format!("WEIGHT {} => {}", a.weight, m.score),
                            std::mem::take(&mut m.parts),
                        )];
                    }
                }
                Ok(result)
            }
            Node::Or(children) => {
                let mut result = Matches::new();
                for child in children {
                    let next = self.eval(child, scope, attrs)?;
                    self.tick(next.len())?;
                    for (id, m) in next {
                        merge_match(result.entry(id).or_default(), m, self.scorer == "DISMAX");
                    }
                }
                Ok(result)
            }
            Node::And(children) => self.intersect(children, scope, attrs),
            Node::Not(child) => {
                let excluded = self.eval(child, scope, attrs)?;
                if self.halted {
                    return Ok(Matches::new());
                }
                let mut all = self.universe()?;
                all.retain(|id, _| !excluded.contains_key(id));
                Ok(all)
            }
            Node::Optional(child) => {
                let optional = self.eval(child, scope, attrs)?;
                let mut all = self.universe()?;
                for (id, m) in optional {
                    all.insert(id, m);
                }
                Ok(all)
            }
            Node::Numeric(name, min, mi, max, ma) => {
                self.field(name, FieldKind::Numeric)?;
                if min > max || (min == max && (!mi || !ma)) {
                    return Ok(Matches::new());
                }
                let Some(FieldIndex::Numeric(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let lower = if *mi {
                    Bound::Included(Number(*min))
                } else {
                    Bound::Excluded(Number(*min))
                };
                let upper = if *ma {
                    Bound::Included(Number(*max))
                } else {
                    Bound::Excluded(Number(*max))
                };
                let mut result = Matches::new();
                for (_, ids) in values.range((lower, upper)) {
                    for id in ids {
                        self.tick(1)?;
                        if self.halted {
                            break;
                        }
                        if self.live(*id) {
                            result.entry(*id).or_default();
                        }
                    }
                    if self.halted {
                        break;
                    }
                }
                Ok(result)
            }
            Node::Tag(name, tags) => {
                let field = self.field(name, FieldKind::Tag)?;
                let Some(FieldIndex::Tag(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let mut result = Matches::new();
                for (value, kind) in tags {
                    if value.is_empty() && matches!(kind, TermKind::Plain) {
                        if !field.index_empty {
                            return Err(format!(
                                "Field `{name}` does not support empty queries; use INDEXEMPTY"
                            ));
                        }
                        if let Some(ids) = self.index.empty.get(name) {
                            for (id, m) in self.matches_from_ids(ids)? {
                                result.insert(id, m);
                            }
                        }
                        continue;
                    }
                    let value = if matches!(kind, TermKind::Wildcard) {
                        value.clone()
                    } else {
                        unescape(value)
                    };
                    let value = if field.case_sensitive {
                        value
                    } else {
                        text::normalize(&value)
                    };
                    let suffixes = if field.suffix {
                        self.index.suffixes.get(name)
                    } else {
                        None
                    };
                    let terms =
                        expand_keys(values, &value, kind, self.config, self, false, suffixes)?;
                    for term in terms {
                        if let Some(ids) = values.get(&term) {
                            for id in ids {
                                self.tick(1)?;
                                if self.halted {
                                    break;
                                }
                                if self.live(*id) {
                                    result.entry(*id).or_default();
                                }
                            }
                        }
                        if self.halted {
                            break;
                        }
                    }
                }
                Ok(result)
            }
            Node::Geo(name, lon, lat, radius, unit) => {
                self.field(name, FieldKind::Geo)?;
                let Some(FieldIndex::Geo(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let ids = values.query(*lon, *lat, *radius, unit)?;
                self.matches_from_ids(&ids)
            }
            Node::Shape(name, predicate, wkt) => {
                self.field(name, FieldKind::GeoShape)?;
                let Some(FieldIndex::Shape(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let ids = values.query(predicate, wkt)?;
                self.matches_from_ids(&ids)
            }
            Node::Missing(name) => {
                let field = self
                    .index
                    .schema
                    .fields
                    .iter()
                    .find(|f| f.name == *name)
                    .ok_or_else(|| format!("Unknown field `{name}`"))?;
                if !field.index_missing {
                    return Err(format!(
                        "Field `{name}` does not support missing queries; use INDEXMISSING"
                    ));
                }
                if let Some(ids) = self.index.missing.get(name) {
                    self.matches_from_ids(ids)
                } else {
                    Ok(Matches::new())
                }
            }
            Node::VectorRange(name, radius, param, runtime) => {
                let field = self.field(name, FieldKind::Vector)?;
                let Some(FieldIndex::Vector(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let q = vector::decode(
                    field
                        .vector
                        .as_ref()
                        .ok_or("Missing vector configuration")?,
                    self.parameter(param)?,
                )?;
                let mut runtime = runtime.clone();
                runtime.extend(attrs.runtime.clone());
                let rows = values.range(&q, *radius, None, &runtime)?;
                self.tick(rows.len())?;
                Ok(rows
                    .into_iter()
                    .filter(|(id, _)| self.live(*id))
                    .map(|(id, d)| {
                        let mut m = Match {
                            distance: Some(d),
                            ..Match::default()
                        };
                        if let Some(alias) = &attrs.distance {
                            m.distances.insert(alias.clone(), d);
                        }
                        (id, m)
                    })
                    .collect())
            }
            Node::Knn(child, name, k, param, runtime, alias) => {
                let field = self.field(name, FieldKind::Vector)?;
                let q = vector::decode(
                    field
                        .vector
                        .as_ref()
                        .ok_or("Missing vector configuration")?,
                    self.parameter(param)?,
                )?;
                let mut candidates = self.eval(child, scope, attrs)?;
                let ids = candidates.keys().copied().collect::<DocSet>();
                let Some(FieldIndex::Vector(values)) = self.index.fields.get(name) else {
                    return Ok(Matches::new());
                };
                let mut runtime = runtime.clone();
                runtime.extend(attrs.runtime.clone());
                let rows = values.query(&q, *k, Some(&ids), &runtime)?;
                self.tick(rows.len())?;
                Ok(rows
                    .into_iter()
                    .filter_map(|(id, d)| {
                        candidates.remove(&id).map(|mut m| {
                            m.distance = Some(d);
                            m.distances.insert(alias.clone(), d);
                            if let Some(alias) = &attrs.distance {
                                m.distances.insert(alias.clone(), d);
                            }
                            (id, m)
                        })
                    })
                    .collect())
            }
        }
    }
    fn parameter(&self, param: &str) -> Result<&[u8], String> {
        let name = param
            .strip_prefix('$')
            .ok_or("Vector value must be a parameter")?;
        self.options
            .params
            .get(name)
            .map(Vec::as_slice)
            .ok_or_else(|| format!("No such parameter `{name}`"))
    }
    fn eval_modifier(
        &mut self,
        node: &Node,
        scope: &[String],
        attrs: &Attributes,
    ) -> Result<Matches, String> {
        match node {
            Node::Scope(fields, child) => self.eval_modifier(
                child,
                if self.index.schema.nofields {
                    &[]
                } else {
                    fields
                },
                attrs,
            ),
            Node::Attributes(child, a) => {
                let mut merged = attrs.clone();
                if a.slop.is_some() {
                    merged.slop = a.slop;
                }
                if a.inorder.is_some() {
                    merged.inorder = a.inorder;
                }
                if a.phonetic.is_some() {
                    merged.phonetic = a.phonetic;
                }
                let mut rows = self.eval_modifier(child, scope, &merged)?;
                for row in rows.values_mut() {
                    row.score *= a.weight;
                    if self.options.explain_score {
                        row.parts = vec![score_node(
                            format!("WEIGHT {} => {}", a.weight, row.score),
                            std::mem::take(&mut row.parts),
                        )];
                    }
                }
                Ok(rows)
            }
            Node::Not(child) | Node::Optional(child) => self.eval(child, scope, attrs),
            _ => self.eval(node, scope, attrs),
        }
    }
    fn intersect(
        &mut self,
        children: &[Node],
        scope: &[String],
        attrs: &Attributes,
    ) -> Result<Matches, String> {
        let mut required = Vec::new();
        let mut optional = Vec::new();
        let mut excluded = Vec::new();
        for child in children {
            match child {
                child if modifier_kind(child) == Some(false) => {
                    optional.push(self.eval_modifier(child, scope, attrs)?)
                }
                child if modifier_kind(child) == Some(true) => {
                    excluded.push(self.eval_modifier(child, scope, attrs)?)
                }
                _ => required.push(self.eval(child, scope, attrs)?),
            }
        }
        if self.halted && !excluded.is_empty() {
            return Ok(Matches::new());
        }
        if required.is_empty() {
            required.push(self.universe()?);
        }
        let positional = attrs.slop.unwrap_or(self.options.slop) >= 0
            || attrs.inorder.unwrap_or(self.options.inorder);
        let mut lists: Vec<Matches> = required;
        let mut base_at = 0;
        for i in 1..lists.len() {
            if lists[i].len() < lists[base_at].len() {
                base_at = i;
            }
        }
        let base_positions = lists[base_at]
            .iter()
            .map(|(id, m)| (*id, m.positions.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut result = std::mem::take(&mut lists[base_at]);
        for (i, list) in lists.iter().enumerate() {
            if i == base_at {
                continue;
            }
            self.tick(result.len())?;
            result.retain(|id, m| {
                if let Some(next) = list.get(id) {
                    merge_match(m, next.clone(), false);
                    true
                } else {
                    false
                }
            });
        }
        for list in excluded {
            self.tick(result.len())?;
            result.retain(|id, _| !list.contains_key(id));
        }
        if positional
            || matches!(
                self.scorer.as_str(),
                "TFIDF" | "TFIDF.DOCNORM" | "BM25" | "BM25STD" | "BM25STD.NORM" | "BM25STD.TANH"
            )
        {
            // Position constraints use the original clause order, not intersection size order.
            result.retain(|id, m| {
                let maps: Vec<&BTreeMap<String, Vec<u32>>> = lists
                    .iter()
                    .enumerate()
                    .filter_map(|(i, l)| {
                        if i == base_at {
                            base_positions.get(id)
                        } else {
                            l.get(id).map(|m| &m.positions)
                        }
                    })
                    .filter(|p| !p.is_empty())
                    .collect();
                if maps.len() < 2 {
                    return true;
                }
                let offsets = (0..maps.len()).map(|n| n as u32).collect::<Vec<_>>();
                let slop = attrs.slop.unwrap_or(self.options.slop);
                let inorder = attrs.inorder.unwrap_or(self.options.inorder);
                let mut best = None;
                for field in maps[0].keys() {
                    let positions: Option<Vec<&[u32]>> = maps
                        .iter()
                        .map(|p| p.get(field).map(Vec::as_slice))
                        .collect();
                    if let Some(positions) = positions
                        && (!positional
                            || position_match(&positions, &offsets, slop, inorder).is_some())
                    {
                        let cost = positional_penalty(&positions);
                        best = Some(best.map_or(cost, |v: f64| v.min(cost)));
                    }
                }
                if let Some(penalty) = best {
                    if self.scorer != "DISMAX" {
                        m.score /= penalty.max(1.0);
                        if self.options.explain_score {
                            m.parts.push(serde_json::json!([
                                format!("POSITIONAL penalty={} => {}", penalty.max(1.0), m.score),
                                []
                            ]));
                        }
                    }
                    true
                } else {
                    !positional
                }
            });
        }
        for list in optional {
            for (id, m) in list {
                if let Some(current) = result.get_mut(&id) {
                    merge_match(current, m, false);
                }
            }
        }
        Ok(result)
    }
    fn term(
        &mut self,
        value: &str,
        kind: &TermKind,
        scope: &[String],
        attrs: &Attributes,
    ) -> Result<Matches, String> {
        let language = if self.options.language.is_empty() {
            &self.index.schema.language
        } else {
            &self.options.language
        };
        let empty = BTreeSet::new();
        let stops = if self.options.nostopwords {
            &empty
        } else {
            &self.index.schema.stopwords
        };
        let normalized;
        let value = if matches!(kind, TermKind::Plain) {
            let tokens = text::tokenize(value, language, true, stops);
            if tokens.is_empty() {
                return Ok(Matches::new());
            }
            if tokens.len() > 1 {
                let nodes = tokens
                    .into_iter()
                    .map(|t| Node::Term(escape_token(&t.term), TermKind::Plain))
                    .collect::<Vec<_>>();
                return self.intersect(&nodes, scope, attrs);
            }
            normalized = tokens.into_iter().next().unwrap().term;
            normalized.as_str()
        } else {
            value
        };
        let fields = self.text_fields(scope)?;
        let mut result = Matches::new();
        let mut weighted_frequency: BTreeMap<DocId, f64> = BTreeMap::new();
        for field in fields {
            let Some(FieldIndex::Text(postings)) = self.index.fields.get(&field.name) else {
                continue;
            };
            let suffixes = if field.suffix {
                self.index.suffixes.get(&field.name)
            } else {
                None
            };
            let mut terms = expand_keys(postings, value, kind, self.config, self, true, suffixes)?;
            if matches!(kind, TermKind::Plain)
                && !self.options.verbatim
                && !self.options.expander.eq_ignore_ascii_case("NOEXPANDER")
            {
                if !field.nostem {
                    let key = format!("+{}", text::stem(value, language));
                    if postings.contains_key(&key) {
                        terms.push(key);
                    }
                }
                for (group, synonyms) in &self.index.synonyms {
                    if synonyms.contains(value) {
                        let key = format!("~{group}");
                        if postings.contains_key(&key) {
                            terms.push(key);
                        }
                    }
                }
                if let Some(mode) = &field.phonetic {
                    let minimum = self
                        .config
                        .extras
                        .get("MIN_PHONETIC_TERM_LEN")
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(3);
                    if attrs.phonetic.unwrap_or(true) && value.chars().count() >= minimum {
                        for key in text::phonetic(value, mode) {
                            let key = format!("${key}");
                            if postings.contains_key(&key) {
                                terms.push(key);
                            }
                        }
                    }
                } else if attrs.phonetic == Some(true) {
                    return Err(format!("Field `{}` is not PHONETIC", field.name));
                }
            }
            terms.sort();
            terms.dedup();
            // Expansion alternatives count each occurrence once even if both surface and
            // stem/phonetic aliases point at the same position.
            let mut field_matches: BTreeMap<DocId, (BTreeSet<u32>, BTreeSet<String>)> =
                BTreeMap::new();
            if self.halted {
                break;
            }
            for term in terms {
                let Some(ids) = postings.get(&term) else {
                    continue;
                };
                self.tick(ids.len())?;
                for (id, positions) in ids {
                    self.tick(positions.len().saturating_add(1))?;
                    if self.halted {
                        break;
                    }
                    if !self.live(*id) {
                        continue;
                    }
                    let entry = field_matches.entry(*id).or_default();
                    entry.0.extend(positions);
                    if !term.starts_with(['+', '$', '~']) {
                        entry.1.insert(term.clone());
                    } else if let Some(doc) = self.index.docs.get(id)
                        && let Some(IndexedValue::Text { tokens, .. }) = doc.fields.get(&field.name)
                    {
                        for token in tokens {
                            if !token.term.starts_with(['+', '$', '~'])
                                && positions.binary_search(&token.position).is_ok()
                            {
                                entry.1.insert(token.term.clone());
                            }
                        }
                    }
                }
            }
            for (id, (positions, terms)) in field_matches {
                *weighted_frequency.entry(id).or_default() += if self.index.schema.nofreqs {
                    field.weight
                } else {
                    positions.len() as f64 * field.weight
                };
                let mut m = Match {
                    terms,
                    ..Match::default()
                };
                m.positions
                    .insert(field.name.clone(), positions.into_iter().collect());
                merge_match(result.entry(id).or_default(), m, false);
            }
        }
        let df = result.len();
        for (id, m) in &mut result {
            let tf = weighted_frequency.get(id).copied().unwrap_or(0.0);
            m.score = self.term_score(*id, tf, df);
            if self.options.explain_score {
                m.parts
                    .push(self.term_explanation(*id, tf, df, format!("TERM {value} {kind:?}")));
            }
        }
        Ok(result)
    }
    fn term_score(&self, id: DocId, tf: f64, df: usize) -> f64 {
        let Some(doc) = self.index.docs.get(&id) else {
            return 0.0;
        };
        let n = self.index.docs.len().max(1) as f64;
        match self.scorer.as_str() {
            "DISMAX" => tf,
            "DOCSCORE" | "HAMMING" => 0.0,
            "TFIDF" | "TFIDF.DOCNORM" => {
                let norm = if self.scorer == "TFIDF.DOCNORM" {
                    weighted_length(doc, &self.index.schema)
                } else {
                    max_frequency(doc, &self.index.schema)
                };
                tf * (1.0 + n / df.max(1) as f64).log2() / norm.max(1.0)
            }
            "BM25" => {
                let avg = self.index.total_length as f64 / n;
                let norm = 1.2 * (0.5 + 0.5 * doc.length as f64 / avg.max(1.0));
                let idf = (1.0 + n / df.max(1) as f64).log2();
                idf * tf / (tf + norm)
            }
            _ => {
                let avg = self.index.total_length as f64 / n;
                let idf = (1.0 + (n - df as f64 + 0.5) / (df as f64 + 0.5)).ln();
                let denominator = tf + 1.2 * (0.25 + 0.75 * doc.length as f64 / avg.max(1.0));
                idf * tf * 2.2 / denominator.max(f64::MIN_POSITIVE)
            }
        }
    }
    fn term_explanation(&self, id: DocId, tf: f64, df: usize, label: String) -> serde_json::Value {
        let n = self.index.docs.len().max(1) as f64;
        let doc = self.index.docs.get(&id);
        let length = doc.map_or(0, |d| d.length);
        let normalization = doc.map_or(1.0, |d| {
            if self.scorer == "TFIDF.DOCNORM" {
                weighted_length(d, &self.index.schema)
            } else {
                max_frequency(d, &self.index.schema)
            }
        });
        serde_json::json!([
            format!("{label}: contribution={}", self.term_score(id, tf, df)),
            [
                format!("weighted_tf={tf}, document_frequency={df}, N={n}"),
                format!(
                    "document_length={length}, average_length={}, frequency_normalization={normalization}",
                    self.index.total_length as f64 / n
                ),
                format!(
                    "standard_idf={}, legacy_tfidf_idf={}",
                    (1.0 + (n - df as f64 + 0.5) / (df as f64 + 0.5)).ln(),
                    (1.0 + n / df.max(1) as f64).log2()
                )
            ]
        ])
    }
    fn phrase(
        &mut self,
        value: &str,
        scope: &[String],
        attrs: &Attributes,
    ) -> Result<Matches, String> {
        let fields = self.text_fields(scope)?;
        if value.is_empty() {
            let mut result = Matches::new();
            if !fields.iter().any(|f| f.index_empty) {
                return Err("Empty text queries require INDEXEMPTY".into());
            }
            for field in fields {
                if !field.index_empty {
                    if !scope.is_empty() {
                        return Err(format!(
                            "Field `{}` does not support empty queries; use INDEXEMPTY",
                            field.name
                        ));
                    }
                    continue;
                }
                if let Some(ids) = self.index.empty.get(&field.name) {
                    for (id, m) in self.matches_from_ids(ids)? {
                        result.insert(id, m);
                    }
                }
            }
            return Ok(result);
        }
        let language = if self.options.language.is_empty() {
            &self.index.schema.language
        } else {
            &self.options.language
        };
        let no_stops = BTreeSet::new();
        let stops = if self.options.nostopwords {
            &no_stops
        } else {
            &self.index.schema.stopwords
        };
        let tokens = text::tokenize(value, language, true, stops);
        if tokens.is_empty() {
            return Ok(Matches::new());
        }
        let offsets = tokens.iter().map(|t| t.position).collect::<Vec<_>>();
        let mut result = Matches::new();
        for field in fields {
            let Some(FieldIndex::Text(postings)) = self.index.fields.get(&field.name) else {
                continue;
            };
            let readers: Option<Vec<_>> = tokens.iter().map(|t| postings.get(&t.term)).collect();
            let Some(readers) = readers else {
                continue;
            };
            let smallest = readers.iter().min_by_key(|p| p.len()).unwrap();
            self.tick(smallest.len())?;
            for id in smallest.keys() {
                if !self.live(*id) {
                    continue;
                }
                let positions: Option<Vec<&[u32]>> = readers
                    .iter()
                    .map(|r| r.get(id).map(Vec::as_slice))
                    .collect();
                let Some(positions) = positions else {
                    continue;
                };
                self.tick(positions.iter().map(|p| p.len()).sum())?;
                if self.halted {
                    break;
                }
                let slop = attrs.slop.unwrap_or(if self.options.slop >= 0 {
                    self.options.slop
                } else {
                    0
                });
                let inorder = attrs.inorder.unwrap_or(true);
                let penalty = if self.index.schema.nooffsets {
                    Some(1.0)
                } else {
                    position_match(&positions, &offsets, slop, inorder)
                        .map(|_| positional_penalty(&positions))
                };
                if let Some(penalty) = penalty {
                    let mut m = Match::default();
                    for (i, token) in tokens.iter().enumerate() {
                        let tf = if self.index.schema.nofreqs {
                            field.weight
                        } else {
                            positions[i].len() as f64 * field.weight
                        };
                        m.score += self.term_score(*id, tf, readers[i].len());
                        if self.options.explain_score {
                            m.parts.push(self.term_explanation(
                                *id,
                                tf,
                                readers[i].len(),
                                format!(
                                    "EXACT TOKEN {} @{} weight={}",
                                    token.term, field.name, field.weight
                                ),
                            ));
                        }
                        m.terms.insert(token.term.clone());
                    }
                    if self.scorer != "DISMAX" {
                        m.score /= penalty.max(1.0);
                    }
                    if self.options.explain_score {
                        m.parts.push(serde_json::json!([
                            format!(
                                "EXACT positional_penalty={} => {}",
                                penalty.max(1.0),
                                m.score
                            ),
                            []
                        ]));
                    }
                    let mut all = positions
                        .iter()
                        .flat_map(|p| p.iter().copied())
                        .collect::<Vec<_>>();
                    all.sort_unstable();
                    all.dedup();
                    m.positions.insert(field.name.clone(), all);
                    merge_match(result.entry(*id).or_default(), m, false);
                }
            }
        }
        Ok(result)
    }
}

fn merge_match(target: &mut Match, source: Match, maximum: bool) {
    target.score = if maximum {
        target.score.max(source.score)
    } else {
        target.score + source.score
    };
    if source.distance.is_some() {
        target.distance = source.distance;
    }
    target.distances.extend(source.distances);
    target.terms.extend(source.terms);
    if maximum && !target.parts.is_empty() && !source.parts.is_empty() {
        target.parts = vec![score_node(
            format!("DISMAX UNION maximum={}", target.score),
            vec![
                serde_json::Value::Array(std::mem::take(&mut target.parts)),
                serde_json::Value::Array(source.parts),
            ],
        )];
    } else {
        target.parts.extend(source.parts);
    }
    for (field, positions) in source.positions {
        let current = target.positions.entry(field).or_default();
        current.extend(positions);
        current.sort_unstable();
        current.dedup();
    }
}
fn weighted_length(doc: &Document, schema: &Schema) -> f64 {
    schema
        .fields
        .iter()
        .filter_map(|f| match doc.fields.get(&f.name) {
            Some(IndexedValue::Text { tokens, .. }) => Some(
                tokens
                    .iter()
                    .filter(|t| !t.term.starts_with(['+', '$', '~']))
                    .count() as f64
                    * f.weight,
            ),
            _ => None,
        })
        .sum()
}
fn max_frequency(doc: &Document, schema: &Schema) -> f64 {
    let mut counts: BTreeMap<&str, f64> = BTreeMap::new();
    for field in &schema.fields {
        if let Some(IndexedValue::Text { tokens, .. }) = doc.fields.get(&field.name) {
            for t in tokens {
                if !t.term.starts_with(['+', '$', '~']) {
                    *counts.entry(&t.term).or_default() += field.weight;
                }
            }
        }
    }
    counts.values().copied().fold(1.0, f64::max)
}

/// Find a positional path with bounded polynomial work. Ordered paths use a
/// sliding minimum over preceding positions; unordered paths use the smallest
/// window containing an occurrence of each query term.
fn position_match(lists: &[&[u32]], offsets: &[u32], slop: i64, inorder: bool) -> Option<f64> {
    if lists.is_empty() {
        return Some(1.0);
    }
    if lists.iter().any(|p| p.is_empty()) {
        return None;
    }
    if lists.len() == 1 {
        return Some(1.0);
    }
    if inorder {
        let mut previous: Vec<(u32, u64, f64)> = lists[0].iter().map(|p| (*p, 0, 0.0)).collect();
        for i in 1..lists.len() {
            let gap = u64::from(offsets[i].saturating_sub(offsets[i - 1]).max(1));
            let mut next = Vec::new();
            let mut cursor = 0;
            let mut best: Option<(i128, u32, f64)> = None;
            for &position in lists[i] {
                while cursor < previous.len()
                    && u64::from(previous[cursor].0) + gap <= u64::from(position)
                {
                    let (p, cost, penalty) = previous[cursor];
                    let adjusted = i128::from(cost) - i128::from(p);
                    if best.is_none_or(|b| adjusted < b.0) {
                        best = Some((adjusted, p, penalty));
                    }
                    cursor += 1;
                }
                if let Some((adjusted, p, penalty)) = best {
                    let cost = (adjusted + i128::from(position) - i128::from(gap)).max(0) as u64;
                    if slop < 0 || cost <= slop as u64 {
                        let distance = f64::from(position - p);
                        next.push((position, cost, penalty + distance * distance));
                    }
                }
            }
            if next.is_empty() {
                return None;
            }
            previous = next;
        }
        previous.iter().map(|p| p.2.sqrt()).min_by(f64::total_cmp)
    } else {
        let mut groups: BTreeMap<&[u32], usize> = BTreeMap::new();
        for list in lists {
            *groups.entry(*list).or_default() += 1;
        }
        let mut events = Vec::new();
        let mut needed = Vec::with_capacity(groups.len());
        for (i, (list, multiplicity)) in groups.into_iter().enumerate() {
            needed.push(multiplicity);
            for &p in list {
                events.push((p, i));
            }
        }
        events.sort_unstable();
        let mut count = vec![0usize; needed.len()];
        let mut present = 0;
        let mut left = 0;
        let mut best = None;
        let mut position_counts = BTreeMap::<u32, usize>::new();
        for right in 0..events.len() {
            let term = events[right].1;
            count[term] += 1;
            if count[term] == needed[term] {
                present += 1;
            }
            *position_counts.entry(events[right].0).or_default() += 1;
            while present == needed.len() {
                let width = events[right].0 - events[left].0;
                let missing = width.saturating_sub(
                    offsets
                        .last()
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(offsets[0]),
                );
                if position_counts.len() >= lists.len()
                    && (slop < 0 || u64::from(missing) <= slop as u64)
                {
                    let penalty = (f64::from(width).powi(2) / (lists.len() - 1) as f64)
                        .sqrt()
                        .max(1.0);
                    best = Some(best.map_or(penalty, |v: f64| v.min(penalty)));
                }
                let term = events[left].1;
                if count[term] == needed[term] {
                    present -= 1;
                }
                count[term] -= 1;
                let position = events[left].0;
                if let Some(count) = position_counts.get_mut(&position) {
                    *count -= 1;
                    if *count == 0 {
                        position_counts.remove(&position);
                    }
                }
                left += 1;
            }
        }
        best
    }
}

fn expand_keys<V>(
    values: &BTreeMap<String, V>,
    value: &str,
    kind: &TermKind,
    config: &Config,
    evaluator: &mut Evaluator<'_>,
    skip_aliases: bool,
    suffixes: Option<&text::SuffixIndex>,
) -> Result<Vec<String>, String> {
    if matches!(kind, TermKind::Plain) {
        return Ok(if values.contains_key(value) {
            vec![value.to_string()]
        } else {
            Vec::new()
        });
    }
    if matches!(
        kind,
        TermKind::Prefix | TermKind::Suffix | TermKind::Contains
    ) && value.chars().count() < config.min_prefix
    {
        return Err(format!(
            "Wildcard term must have at least {} characters",
            config.min_prefix
        ));
    }
    let mut result = Vec::new();
    let pattern = if matches!(kind, TermKind::Wildcard) {
        compile_wildcard(value)
    } else {
        Vec::new()
    };
    if let Some(suffixes) = suffixes {
        if matches!(kind, TermKind::Suffix | TermKind::Contains) {
            let mut found = BTreeSet::new();
            let mut accept = |term: &str| -> Result<bool, String> {
                evaluator.tick(1)?;
                if evaluator.halted {
                    return Ok(false);
                }
                if !skip_aliases || !term.starts_with(['+', '$', '~']) {
                    found.insert(term.to_owned());
                }
                Ok(found.len() < config.max_expansions)
            };
            if matches!(kind, TermKind::Suffix) {
                for term in suffixes.suffix(value) {
                    if !accept(term)? {
                        break;
                    }
                }
            } else {
                for term in suffixes.infix(value) {
                    if !accept(term)? {
                        break;
                    }
                }
            }
            return Ok(found.into_iter().collect());
        }
        if matches!(kind, TermKind::Wildcard) {
            let mut literal = String::new();
            let mut longest = String::new();
            for item in &pattern {
                match item {
                    Glob::Literal(c) => literal.push(*c),
                    _ => {
                        if literal.len() > longest.len() {
                            longest = std::mem::take(&mut literal);
                        } else {
                            literal.clear();
                        }
                    }
                }
            }
            if literal.len() > longest.len() {
                longest = literal;
            }
            if !longest.is_empty() {
                let mut seen = BTreeSet::new();
                for term in suffixes.infix(&longest) {
                    evaluator.tick(1)?;
                    if evaluator.halted {
                        break;
                    }
                    if !seen.insert(term) || (skip_aliases && term.starts_with(['+', '$', '~'])) {
                        continue;
                    }
                    if wildcard(&pattern, term, evaluator)? {
                        result.push(term.to_owned());
                        if result.len() >= config.max_expansions {
                            break;
                        }
                    }
                }
                return Ok(result);
            }
        }
    }
    match kind {
        TermKind::Prefix => {
            for (key, _) in values.range(value.to_owned()..) {
                evaluator.tick(1)?;
                if evaluator.halted || !key.starts_with(value) {
                    break;
                }
                result.push(key.clone());
                if result.len() >= config.max_expansions {
                    break;
                }
            }
        }
        _ => {
            for key in values.keys() {
                evaluator.tick(key.len().saturating_add(1))?;
                if evaluator.halted {
                    break;
                }
                if skip_aliases && key.starts_with(['+', '$', '~']) {
                    continue;
                }
                let matched = match kind {
                    TermKind::Suffix => key.ends_with(value),
                    TermKind::Contains => key.contains(value),
                    TermKind::Wildcard => value.is_empty() || wildcard(&pattern, key, evaluator)?,
                    TermKind::Fuzzy(max) => levenshtein(value, key, *max) <= *max,
                    _ => false,
                };
                if matched {
                    result.push(key.clone());
                    if result.len() >= config.max_expansions {
                        break;
                    }
                }
            }
        }
    }
    Ok(result)
}
#[derive(Clone, Copy)]
enum Glob {
    Literal(char),
    One,
    Many,
}
fn compile_wildcard(pattern: &str) -> Vec<Glob> {
    let mut out = Vec::new();
    let mut escaped = false;
    for c in pattern.chars() {
        if escaped {
            out.push(Glob::Literal(c));
            escaped = false;
        } else {
            match c {
                '\\' => escaped = true,
                '*' => out.push(Glob::Many),
                '?' => out.push(Glob::One),
                c => out.push(Glob::Literal(c)),
            }
        }
    }
    if escaped {
        out.push(Glob::Literal('\\'));
    }
    out
}
fn wildcard(pattern: &[Glob], value: &str, evaluator: &mut Evaluator<'_>) -> Result<bool, String> {
    let (mut i, mut j, mut star, mut restart) = (0, 0, None, 0);
    let mut steps = 0usize;
    while j < value.len() {
        steps += 1;
        if steps.is_multiple_of(256) {
            evaluator.tick(256)?;
            if evaluator.halted {
                return Ok(false);
            }
        }
        let c = value[j..].chars().next().unwrap();
        if i < pattern.len()
            && match pattern[i] {
                Glob::One => true,
                Glob::Literal(p) => p == c,
                Glob::Many => false,
            }
        {
            i += 1;
            j += c.len_utf8();
        } else if i < pattern.len() && matches!(pattern[i], Glob::Many) {
            star = Some(i);
            i += 1;
            restart = j;
        } else if let Some(s) = star {
            let Some(c) = value[restart..].chars().next() else {
                return Ok(false);
            };
            restart += c.len_utf8();
            j = restart;
            i = s + 1;
        } else {
            return Ok(false);
        }
    }
    while i < pattern.len() && matches!(pattern[i], Glob::Many) {
        i += 1;
    }
    Ok(i == pattern.len())
}

/// Banded Unicode edit distance. Values beyond `max` return `max + 1`.
pub fn levenshtein(left: &str, right: &str, max: usize) -> usize {
    let a = left.chars().collect::<Vec<_>>();
    let b = right.chars().collect::<Vec<_>>();
    let over = max.saturating_add(1);
    if a.len().abs_diff(b.len()) > max {
        return over;
    }
    if a.is_empty() {
        return b.len().min(over);
    }
    if b.is_empty() {
        return a.len().min(over);
    }
    let mut previous = (0..=b.len()).map(|n| n.min(over)).collect::<Vec<_>>();
    let mut current = vec![over; b.len() + 1];
    for (i, ac) in a.iter().enumerate() {
        current.fill(over);
        current[0] = (i + 1).min(over);
        let from = (i + 1).saturating_sub(max).max(1);
        let to = (i + 1).saturating_add(max).min(b.len());
        let mut row_min = over;
        for j in from..=to {
            let cost = usize::from(*ac != b[j - 1]);
            current[j] = (previous[j].saturating_add(1))
                .min(current[j - 1].saturating_add(1))
                .min(previous[j - 1].saturating_add(cost))
                .min(over);
            row_min = row_min.min(current[j]);
        }
        if row_min > max && from > 0 {
            return over;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()].min(over)
}

fn validate(node: &Node, index: &Index, options: &Options, config: &Config) -> Result<(), String> {
    fn field<'a>(
        index: &'a Index,
        name: &str,
        kind: Option<FieldKind>,
    ) -> Result<&'a Field, String> {
        let f = index
            .schema
            .fields
            .iter()
            .find(|f| f.name == name)
            .ok_or_else(|| format!("Unknown field `{name}`"))?;
        if f.noindex {
            return Err(format!("Field `{name}` is not indexed"));
        }
        if kind.is_some_and(|kind| std::mem::discriminant(&f.kind) != std::mem::discriminant(&kind))
        {
            return Err(format!("Wrong field type for `{name}`"));
        }
        Ok(f)
    }
    fn walk(
        node: &Node,
        index: &Index,
        options: &Options,
        knn: &mut usize,
        depth: usize,
    ) -> Result<(), String> {
        if depth > 128 {
            return Err("Query nesting limit exceeded".into());
        }
        match node {
            Node::Scope(names, child) => {
                for name in names {
                    let f = field(index, name, Some(FieldKind::Text))?;
                    if matches!(child.as_ref(),Node::Phrase(v) if v.is_empty()) && !f.index_empty {
                        return Err(format!("Field `{name}` requires INDEXEMPTY"));
                    }
                }
                walk(child, index, options, knn, depth + 1)?;
            }
            Node::Numeric(name, ..) => {
                field(index, name, Some(FieldKind::Numeric))?;
            }
            Node::Tag(name, tags) => {
                let f = field(index, name, Some(FieldKind::Tag))?;
                if !f.index_empty
                    && tags
                        .iter()
                        .any(|(v, k)| v.is_empty() && matches!(k, TermKind::Plain))
                {
                    return Err(format!("Field `{name}` requires INDEXEMPTY"));
                }
            }
            Node::Geo(name, lon, lat, radius, unit) => {
                field(index, name, Some(FieldKind::Geo))?;
                super::spatial::validate_geo(*lon, *lat)?;
                if !radius.is_finite() || *radius < 0.0 {
                    return Err("Invalid geo radius".into());
                }
                if !["m", "km", "mi", "ft"].contains(&unit.to_ascii_lowercase().as_str()) {
                    return Err("Invalid geo unit".into());
                }
            }
            Node::Shape(name, _, geometry) => {
                let f = field(index, name, Some(FieldKind::GeoShape))?;
                super::spatial::validate_geometry(geometry, f.spherical)?;
            }
            Node::Missing(name) => {
                if !field(index, name, None)?.index_missing {
                    return Err(format!("Field `{name}` requires INDEXMISSING"));
                }
            }
            Node::VectorRange(name, _, param, runtime)
            | Node::Knn(_, name, _, param, runtime, _) => {
                let f = field(index, name, Some(FieldKind::Vector))?;
                let name = param
                    .strip_prefix('$')
                    .ok_or("Vector value must be a parameter")?;
                let blob = options
                    .params
                    .get(name)
                    .ok_or_else(|| format!("No such parameter `{name}`"))?;
                vector::decode(
                    f.vector.as_ref().ok_or("Missing vector configuration")?,
                    blob,
                )?;
                validate_runtime(runtime)?;
                if let Node::Knn(child, ..) = node {
                    *knn += 1;
                    if *knn > 1 {
                        return Err("Only one KNN clause is permitted".into());
                    }
                    walk(child, index, options, knn, depth + 1)?;
                }
            }
            Node::And(children) | Node::Or(children) => {
                for child in children {
                    walk(child, index, options, knn, depth + 1)?;
                }
            }
            Node::Not(child) | Node::Optional(child) => {
                walk(child, index, options, knn, depth + 1)?
            }
            Node::Attributes(child, attrs) => {
                if (!attrs.runtime.is_empty() || attrs.distance.is_some())
                    && !matches!(child.as_ref(), Node::Knn(..) | Node::VectorRange(..))
                {
                    return Err("Vector attributes require a vector query".into());
                }
                validate_runtime(&attrs.runtime)?;
                walk(child, index, options, knn, depth + 1)?;
            }
            _ => {}
        }
        Ok(())
    }
    if !matches!(
        effective_scorer(options, config)
            .to_ascii_uppercase()
            .as_str(),
        "BM25STD"
            | "BM25STD.NORM"
            | "BM25STD.TANH"
            | "BM25"
            | "TFIDF"
            | "TFIDF.DOCNORM"
            | "DISMAX"
            | "DOCSCORE"
            | "HAMMING"
    ) {
        return Err(format!(
            "Unknown scorer `{}`",
            effective_scorer(options, config)
        ));
    }
    if !matches!(
        options.expander.to_ascii_uppercase().as_str(),
        "DEFAULT" | "NOEXPANDER" | "STEMMER"
    ) {
        return Err(format!("Unknown expander `{}`", options.expander));
    }
    if !options.language.is_empty() && !text::language_supported(&options.language) {
        return Err(format!("Unsupported language `{}`", options.language));
    }
    if options.slop < -1 {
        return Err("SLOP must be nonnegative".into());
    }
    let factor = effective_tanh_factor(options, config);
    if !factor.is_finite() || factor <= 0.0 {
        return Err("BM25STD_TANH_FACTOR must be positive".into());
    }
    if config.max_expansions == 0 {
        return Err("MAXEXPANSIONS must be positive".into());
    }
    for name in &options.fields {
        field(index, name, Some(FieldKind::Text))?;
    }
    walk(node, index, options, &mut 0, 0)
}
fn validate_runtime(runtime: &BTreeMap<String, String>) -> Result<(), String> {
    for (name, value) in runtime {
        match name.as_str() {
            "EF_RUNTIME" | "BATCH_SIZE" | "SEARCH_WINDOW_SIZE" | "SEARCH_BUFFER_CAPACITY" => {
                if value.parse::<usize>().ok().filter(|n| *n > 0).is_none() {
                    return Err(format!("{name} must be a positive integer"));
                }
            }
            "EPSILON" | "SHARD_K_RATIO" => {
                let n = parse_number(value)?;
                if !n.is_finite() || n <= 0.0 {
                    return Err(format!("{name} must be positive"));
                }
            }
            "USE_SEARCH_HISTORY" => {
                if !matches!(value.to_ascii_uppercase().as_str(), "ON" | "OFF" | "AUTO") {
                    return Err("USE_SEARCH_HISTORY must be ON, OFF or AUTO".into());
                }
            }
            "HYBRID_POLICY" => {
                if !matches!(value.to_ascii_uppercase().as_str(), "ADHOC_BF" | "BATCHES") {
                    return Err("Invalid HYBRID_POLICY".into());
                }
            }
            _ => return Err(format!("Unknown vector query option `{name}`")),
        }
    }
    Ok(())
}
fn parse(index: &Index, query: &str, options: &Options, config: &Config) -> Result<Node, String> {
    let node = Parser::new(query, options, config)?.parse()?;
    validate(&node, index, options, config)?;
    let mut node = remove_stopwords(node, index, options);
    let mut filters = Vec::with_capacity(options.filters.len());
    for filter in &options.filters {
        let filter = Parser::new(filter, options, config)?.parse()?;
        validate(&filter, index, options, config)?;
        if contains_knn(&filter) {
            return Err("KNN cannot appear in a legacy filter".into());
        }
        filters.push(remove_stopwords(filter, index, options));
    }
    if !filters.is_empty() {
        let mut pending = Some(filters);
        inject_knn_filters(&mut node, &mut pending);
        if let Some(mut filters) = pending {
            filters.insert(0, node);
            node = Node::And(filters);
        }
    }
    Ok(node)
}
fn remove_stopwords(node: Node, index: &Index, options: &Options) -> Node {
    if options.nostopwords {
        return node;
    }
    let union = matches!(&node, Node::Or(_));
    if let Node::Term(word, TermKind::Plain) = &node
        && index.schema.stopwords.contains(word)
    {
        return Node::None;
    }
    match node {
        Node::And(children) | Node::Or(children) => {
            let mut children = children
                .into_iter()
                .map(|c| remove_stopwords(c, index, options))
                .filter(|c| !matches!(c, Node::None))
                .collect::<Vec<_>>();
            if children.is_empty() {
                Node::None
            } else if children.len() == 1 {
                children.pop().unwrap()
            } else if union {
                Node::Or(children)
            } else {
                Node::And(children)
            }
        }
        Node::Scope(fields, child) => {
            Node::Scope(fields, Box::new(remove_stopwords(*child, index, options)))
        }
        Node::Not(child) => {
            let child = remove_stopwords(*child, index, options);
            if matches!(child, Node::None) {
                Node::None
            } else {
                Node::Not(Box::new(child))
            }
        }
        Node::Optional(child) => {
            let child = remove_stopwords(*child, index, options);
            if matches!(child, Node::None) {
                Node::None
            } else {
                Node::Optional(Box::new(child))
            }
        }
        Node::Attributes(child, attrs) => {
            Node::Attributes(Box::new(remove_stopwords(*child, index, options)), attrs)
        }
        Node::Knn(child, f, k, p, r, a) => Node::Knn(
            Box::new(remove_stopwords(*child, index, options)),
            f,
            k,
            p,
            r,
            a,
        ),
        node => node,
    }
}

pub fn execute(
    index: &Index,
    query: &str,
    options: &Options,
    config: &Config,
    now: i64,
) -> Result<QueryResult, String> {
    config.run(|| execute_inner(index, query, options, config, now))
}
fn execute_inner(
    index: &Index,
    query: &str,
    options: &Options,
    config: &Config,
    now: i64,
) -> Result<QueryResult, String> {
    let node = parse(index, query, options, config)?;
    let scorer = effective_scorer(options, config).to_ascii_uppercase();
    let started = Instant::now();
    let deadline = config.deadline(started, options.timeout_ms);
    let timeout = deadline.map_or(Duration::ZERO, |d| d.saturating_duration_since(started));
    let _deadline_guard = super::runtime::DeadlineGuard::enter(if config.enforce_deadline() {
        deadline
    } else {
        None
    });
    let mut evaluator = Evaluator {
        index,
        options,
        config,
        now,
        started,
        timeout,
        visited: 0,
        work: 0,
        scorer: scorer.clone(),
        halted: false,
    };
    let attrs = Attributes::default();
    let matches = evaluator.eval(&node, &options.fields, &attrs)?;
    let mut hits = Vec::with_capacity(matches.len().min(config.max_results));
    let mut score_explanations = BTreeMap::new();
    for (id, m) in matches {
        if config.enforce_deadline() && super::runtime::expired(deadline) {
            evaluator.halted = true;
            if config.on_timeout_fail {
                return Err("Timeout limit was reached".into());
            }
            break;
        }
        let Some(doc) = index.docs.get(&id) else {
            continue;
        };
        let mut score = match scorer.as_str() {
            "DOCSCORE" => doc.score,
            "HAMMING" => match &doc.payload {
                Some(payload)
                    if !options.payload.is_empty() && payload.len() == options.payload.len() =>
                {
                    let distance: u64 = payload
                        .iter()
                        .zip(&options.payload)
                        .map(|(a, b)| u64::from((a ^ b).count_ones()))
                        .sum();
                    1.0 / (1.0 + distance as f64)
                }
                _ => 0.0,
            },
            "DISMAX" => m.score,
            _ => m.score * doc.score,
        };
        if scorer == "BM25STD.TANH" {
            score = (score / effective_tanh_factor(options, config)).tanh();
        }
        if options.explain_score {
            score_explanations.insert(id,score_node(
                format!("{scorer}: raw_score={score}, document_score={}, document_length={}, payload_bytes={}",doc.score,doc.length,doc.payload.as_ref().map_or(0,Vec::len)),
                m.parts
            ));
        }
        hits.push(Hit {
            id,
            score,
            vector_distance: m.distance,
            vector_distances: m.distances,
            matched_terms: m.terms.into_iter().collect(),
        });
    }
    if scorer == "BM25STD.NORM" && !hits.is_empty() {
        let min = hits.iter().map(|h| h.score).fold(f64::INFINITY, f64::min);
        let max = hits
            .iter()
            .map(|h| h.score)
            .fold(f64::NEG_INFINITY, f64::max);
        for hit in &mut hits {
            hit.score = if max > min {
                (hit.score - min) / (max - min)
            } else {
                1.0
            };
        }
    }
    if options.explain_score {
        for hit in &hits {
            if let Some(parts) = score_explanations.get_mut(&hit.id) {
                let detail = std::mem::take(parts);
                *parts = score_node(format!("{scorer}: final_score={}", hit.score), vec![detail]);
            }
        }
    }
    let ordering_deadline = if config.enforce_deadline() {
        deadline
    } else {
        None
    };
    let sorted = if contains_knn(&node) {
        super::runtime::sort(
            &mut hits,
            |a, b| {
                a.vector_distance
                    .unwrap_or(f64::INFINITY)
                    .total_cmp(&b.vector_distance.unwrap_or(f64::INFINITY))
                    .then_with(|| a.id.cmp(&b.id))
            },
            ordering_deadline,
        )
    } else {
        super::runtime::sort(
            &mut hits,
            |a, b| b.score.total_cmp(&a.score).then_with(|| a.id.cmp(&b.id)),
            ordering_deadline,
        )
    };
    if !sorted {
        evaluator.halted = true;
        if config.on_timeout_fail {
            return Err("Timeout limit was reached".into());
        }
    }
    let mut explanation = String::new();
    render(&node, 0, &mut explanation);
    explanation.push_str(&format!(
        "SCORER {scorer} N={} AVGDL={} DOCSCORE={} SLOP={} INORDER={}\n",
        index.docs.len(),
        index.total_length as f64 / index.docs.len().max(1) as f64,
        !matches!(scorer.as_str(), "DISMAX" | "HAMMING"),
        options.slop,
        options.inorder
    ));
    explanation.push_str(match scorer.as_str(){
        "TFIDF"=>"sum(weight*tf*log2(1+N/df))/max_frequency * document_score / positional_penalty\n",
        "TFIDF.DOCNORM"=>"sum(weight*tf*log2(1+N/df))/weighted_document_length * document_score / positional_penalty\n",
        "BM25"=>"sum(log2(1+N/df)*weighted_tf/(weighted_tf+1.2*(0.5+0.5*dl/avgdl))) * document_score / positional_penalty\n",
        "BM25STD"=>"sum(ln(1+(N-df+0.5)/(df+0.5))*2.2*weighted_tf/(weighted_tf+1.2*(0.25+0.75*dl/avgdl))) * document_score / positional_penalty\n",
        "BM25STD.NORM"=>"BM25STD with result min-max normalization\n",
        "BM25STD.TANH"=>"tanh(BM25STD / BM25STD_TANH_FACTOR)\n",
        "DISMAX"=>"sum(weighted_tf) for INTERSECT; max(weighted_tf) for UNION\n",
        "DOCSCORE"=>"document_score\n","HAMMING"=>"1/(1+bit_hamming(query_payload,document_payload)); unmatched payload = 0\n",
        _=>"",
    });
    if evaluator.halted {
        explanation.push_str("TIMEOUT: partial results from completed predicate evaluations\n");
    }
    Ok(QueryResult {
        hits,
        explanation,
        visited: evaluator.visited,
        timed_out: evaluator.halted,
        score_explanations,
        deadline,
        timeout_capped: config.timeout_capped(options.timeout_ms),
    })
}

pub fn explain(
    index: &Index,
    query: &str,
    options: &Options,
    config: &Config,
) -> Result<String, String> {
    let node = parse(index, query, options, config)?;
    let mut out = String::new();
    render(&node, 0, &mut out);
    Ok(out)
}
fn render(node: &Node, depth: usize, out: &mut String) {
    use std::fmt::Write;
    let indent = "  ".repeat(depth);
    match node {
        Node::All => {
            let _ = writeln!(out, "{indent}WILDCARD");
        }
        Node::None => {
            let _ = writeln!(out, "{indent}EMPTY");
        }
        Node::Term(value, kind) => {
            let _ = writeln!(
                out,
                "{indent}{} {{{value}}}",
                match kind {
                    TermKind::Plain => "TOKEN",
                    TermKind::Prefix => "PREFIX",
                    TermKind::Suffix => "SUFFIX",
                    TermKind::Contains => "INFIX",
                    TermKind::Wildcard => "WILDCARD_PATTERN",
                    TermKind::Fuzzy(_) => "FUZZY",
                }
            );
        }
        Node::Phrase(value) => {
            let _ = writeln!(out, "{indent}EXACT {{\"{value}\"}}");
        }
        Node::Scope(fields, child) => {
            let _ = writeln!(out, "{indent}FIELD @{} {{", fields.join("|"));
            render(child, depth + 1, out);
            let _ = writeln!(out, "{indent}}}");
        }
        Node::And(children) | Node::Or(children) => {
            let _ = writeln!(
                out,
                "{indent}{} {{",
                if matches!(node, Node::And(_)) {
                    "INTERSECT"
                } else {
                    "UNION"
                }
            );
            for child in children {
                render(child, depth + 1, out);
            }
            let _ = writeln!(out, "{indent}}}");
        }
        Node::Not(child) | Node::Optional(child) => {
            let _ = writeln!(
                out,
                "{indent}{} {{",
                if matches!(node, Node::Not(_)) {
                    "NOT"
                } else {
                    "OPTIONAL"
                }
            );
            render(child, depth + 1, out);
            let _ = writeln!(out, "{indent}}}");
        }
        Node::Numeric(field, min, mi, max, ma) => {
            let _ = writeln!(
                out,
                "{indent}NUMERIC @{field} {}{min} {max}{}",
                if *mi { "[" } else { "(" },
                if *ma { "]" } else { ")" }
            );
        }
        Node::Tag(field, values) => {
            let _ = writeln!(out, "{indent}TAG @{field} {{");
            for (value, kind) in values {
                let _ = writeln!(out, "{indent}  {kind:?} {{{value}}}");
            }
            let _ = writeln!(out, "{indent}}}");
        }
        Node::Geo(field, lon, lat, radius, unit) => {
            let _ = writeln!(out, "{indent}GEO @{field} {{{lon} {lat} {radius} {unit}}}");
        }
        Node::Shape(field, predicate, geometry) => {
            let _ = writeln!(out, "{indent}GEOSHAPE @{field} {predicate} {{{geometry}}}");
        }
        Node::Missing(field) => {
            let _ = writeln!(out, "{indent}MISSING @{field}");
        }
        Node::VectorRange(field, radius, param, runtime) => {
            let _ = writeln!(
                out,
                "{indent}VECTOR_RANGE @{field} radius={radius} {param} {runtime:?}"
            );
        }
        Node::Knn(child, field, k, param, runtime, alias) => {
            let _ = writeln!(
                out,
                "{indent}VECTOR_KNN @{field} k={k} {param} AS {alias} {runtime:?} {{"
            );
            render(child, depth + 1, out);
            let _ = writeln!(out, "{indent}}}");
        }
        Node::Attributes(child, attrs) => {
            let _ = writeln!(
                out,
                "{indent}ATTRIBUTES weight={} slop={:?} inorder={:?} phonetic={:?} distance={:?} {{",
                attrs.weight, attrs.slop, attrs.inorder, attrs.phonetic, attrs.distance
            );
            render(child, depth + 1, out);
            let _ = writeln!(out, "{indent}}}");
        }
    }
}

fn escape_token(value: &str) -> String {
    let mut out = String::new();
    for c in value.chars() {
        if !c.is_alphanumeric() && c != '_' {
            out.push('\\');
        }
        out.push(c);
    }
    out
}

fn effective_scorer<'a>(options: &'a Options, config: &'a Config) -> &'a str {
    if options.scorer.is_empty() {
        config
            .extras
            .get("DEFAULT_SCORER")
            .map(String::as_str)
            .unwrap_or("BM25STD")
    } else {
        &options.scorer
    }
}
fn effective_tanh_factor(options: &Options, config: &Config) -> f64 {
    if options.bm25std_tanh_factor == 0.0 {
        config
            .extras
            .get("BM25STD_TANH_FACTOR")
            .and_then(|s| s.parse().ok())
            .unwrap_or(4.0)
    } else {
        options.bm25std_tanh_factor
    }
}
fn contains_knn(node: &Node) -> bool {
    match node {
        Node::Knn(..) => true,
        Node::Attributes(c, _) | Node::Scope(_, c) | Node::Not(c) | Node::Optional(c) => {
            contains_knn(c)
        }
        Node::And(c) | Node::Or(c) => c.iter().any(contains_knn),
        _ => false,
    }
}

pub fn spellcheck_terms(
    index: &Index,
    query: &str,
    options: &Options,
    config: &Config,
) -> Result<Vec<(String, Vec<String>)>, String> {
    let node = parse(index, query, options, config)?;
    fn walk(
        node: &Node,
        scope: &[String],
        language: &str,
        stops: &BTreeSet<String>,
        out: &mut Vec<(String, Vec<String>)>,
    ) {
        match node {
            Node::Term(value, _) | Node::Phrase(value) => {
                for token in text::tokenize(value, language, true, stops) {
                    out.push((token.term, scope.to_vec()));
                }
            }
            Node::Scope(fields, child) => walk(child, fields, language, stops, out),
            Node::And(children) | Node::Or(children) => {
                for child in children {
                    walk(child, scope, language, stops, out);
                }
            }
            Node::Not(child)
            | Node::Optional(child)
            | Node::Attributes(child, _)
            | Node::Knn(child, ..) => walk(child, scope, language, stops, out),
            _ => {}
        }
    }
    let language = if options.language.is_empty() {
        &index.schema.language
    } else {
        &options.language
    };
    let no_stops = BTreeSet::new();
    let stops = if options.nostopwords {
        &no_stops
    } else {
        &index.schema.stopwords
    };
    let mut result = Vec::new();
    walk(&node, &options.fields, language, stops, &mut result);
    Ok(result)
}

pub fn distance_aliases(
    query: &str,
    options: &Options,
    config: &Config,
) -> Result<Vec<String>, String> {
    let node = Parser::new(query, options, config)?.parse()?;
    fn walk(node: &Node, out: &mut Vec<String>) {
        match node {
            Node::Knn(child, _, _, _, _, alias) => {
                out.push(alias.clone());
                walk(child, out);
            }
            Node::Attributes(child, attrs) => {
                if let Some(alias) = &attrs.distance {
                    out.push(alias.clone());
                }
                walk(child, out);
            }
            Node::Scope(_, child) | Node::Not(child) | Node::Optional(child) => walk(child, out),
            Node::And(children) | Node::Or(children) => {
                for child in children {
                    walk(child, out);
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(&node, &mut out);
    let mut seen = BTreeSet::new();
    out.retain(|a| seen.insert(a.clone()));
    Ok(out)
}

fn score_node(label: String, children: Vec<serde_json::Value>) -> serde_json::Value {
    serde_json::Value::Array(vec![
        serde_json::Value::String(label),
        serde_json::Value::Array(children),
    ])
}
fn positional_penalty(lists: &[&[u32]]) -> f64 {
    let mut sum = 0.0;
    for pair in lists.windows(2) {
        let (mut i, mut j) = (0, 0);
        let mut distance = u32::MAX;
        while i < pair[0].len() && j < pair[1].len() {
            let a = pair[0][i];
            let b = pair[1][j];
            distance = distance.min(a.abs_diff(b));
            if a < b {
                i += 1;
            } else {
                j += 1;
            }
        }
        if distance != u32::MAX {
            sum += f64::from(distance).powi(2);
        }
    }
    sum.sqrt().max(1.0)
}

fn modifier_kind(node: &Node) -> Option<bool> {
    match node {
        Node::Not(_) => Some(true),
        Node::Optional(_) => Some(false),
        Node::Attributes(child, _) | Node::Scope(_, child) => modifier_kind(child),
        _ => None,
    }
}

fn inject_knn_filters(node: &mut Node, filters: &mut Option<Vec<Node>>) {
    match node {
        Node::Knn(child, ..) => {
            if let Some(mut clauses) = filters.take() {
                clauses.insert(0, std::mem::replace(child.as_mut(), Node::None));
                **child = Node::And(clauses);
            }
        }
        Node::Scope(_, child)
        | Node::Not(child)
        | Node::Optional(child)
        | Node::Attributes(child, _) => inject_knn_filters(child, filters),
        Node::And(children) | Node::Or(children) => {
            for child in children {
                if filters.is_none() {
                    break;
                }
                inject_knn_filters(child, filters);
            }
        }
        _ => {}
    }
}
