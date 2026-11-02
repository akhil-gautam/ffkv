mod bson_input;
mod expr;
mod node;
mod path;

use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{
    CommandSpec, Engine, Entry, Mutation, ReplyBudget, Resp, Value, array, error, integer,
    integer_error, syntax_error, wrong_arity, wrong_type,
};
use bytes::Bytes;
use node::{Kind, Node, V};
use path::{Location, Part, Path, Work};
use serde_json::Number;
use std::{collections::BTreeMap, io, sync::Arc};

// Bounds apply equally to command plans and recovery. Effects never contain a
// serialized copy of an existing document: only resolved paths and new subtrees.
const MAX_BYTES: usize = 4 * 1024 * 1024;
const MAX_NODES: usize = 262_144;
const MAX_DEPTH: usize = 128;
const MAX_MATCHES: usize = 65_536;
const MAX_WORK: usize = 1_048_576;
fn limit() -> Resp {
    error("ERR JSON document/path work limit exceeded")
}
fn invalid() -> Resp {
    error("ERR invalid JSON value")
}
fn missing() -> Resp {
    error("ERR could not perform this operation on a key that doesn't exist")
}
fn path_type() -> Resp {
    error("WRONGTYPE wrong type of path value")
}
fn invalid_effect() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid JSON effect")
}
fn bulk(bytes: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(bytes.into()))
}
fn text(s: &str) -> Resp {
    bulk(Bytes::copy_from_slice(s.as_bytes()))
}
fn bounded(reply: Resp, limit: usize) -> Result<Resp, Resp> {
    ReplyBudget::new(limit).response(&reply)?;
    Ok(reply)
}

pub(super) struct JsonValue {
    root: V,
}
#[derive(Clone)]
enum Edit {
    Set(V),
    Remove,
    Splice {
        start: usize,
        delete: usize,
        insert: Vec<V>,
    },
    Append(String),
}
#[derive(Clone)]
struct Delta {
    path: Location,
    edit: Edit,
}

impl JsonValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        fn visit(value: &V, samples: usize, seen: &mut std::collections::HashSet<usize>) -> usize {
            use super::memory::{allocation, hash_bytes, sampled};
            let identity = Arc::as_ptr(value) as usize;
            if !seen.insert(identity) {
                return 0;
            }
            let owned =
                match &value.kind {
                    Kind::String(value) => value.capacity(),
                    Kind::Number(value) => value.as_str().len(),
                    Kind::Array(values) => allocation::<V>(values.capacity()).saturating_add(
                        sampled(values.iter(), samples, |value| visit(value, samples, seen)),
                    ),
                    Kind::Object(values) => allocation::<(usize, String, V)>(values.capacity())
                        .saturating_add(hash_bytes::<u64, usize>(values.capacity()))
                        .saturating_add(sampled(values.iter(), samples, |(key, value)| {
                            key.capacity().saturating_add(visit(value, samples, seen))
                        })),
                    _ => 0,
                };
            std::mem::size_of::<Node>()
                .saturating_add(2 * std::mem::size_of::<usize>())
                .saturating_add(owned)
        }
        // Documents already enforce MAX_NODES and MAX_DEPTH on construction.
        // Identity tracking counts COW-shared subtrees only once.
        visit(&self.root, samples, &mut std::collections::HashSet::new())
    }

    pub(super) fn search_validate_path(raw: &str) -> Result<(), Resp> {
        Query::parse(raw.as_bytes()).map(|_| ())
    }

    pub(super) fn search_values(&self, raw: &str) -> Result<Vec<serde_json::Value>, Resp> {
        fn convert(value: &V) -> serde_json::Value {
            match &value.kind {
                Kind::Null => serde_json::Value::Null,
                Kind::Bool(value) => serde_json::Value::Bool(*value),
                Kind::Number(value) => serde_json::Value::Number(value.clone()),
                Kind::String(value) => serde_json::Value::String(value.clone()),
                Kind::Array(values) => {
                    serde_json::Value::Array(values.iter().map(convert).collect())
                }
                Kind::Object(values) => serde_json::Value::Object(
                    values
                        .iter()
                        .map(|(key, value)| (key.clone(), convert(value)))
                        .collect(),
                ),
            }
        }
        let query = Query::parse(raw.as_bytes())?;
        let (_, values) = query.evaluate(&self.root, &mut Work::new())?;
        let mut budget = ReplyBudget::new(MAX_BYTES);
        for value in &values {
            budget.charge(1, value.bytes)?;
        }
        Ok(values.iter().map(convert).collect())
    }

    pub(super) fn search_document(&self) -> Result<Bytes, Resp> {
        Ok(Bytes::from(self.root.json()))
    }

    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (init, deltas) = decode(effect)?;
        if !init || deltas.len() != 1 || !deltas[0].path.is_empty() {
            return Err(invalid_effect());
        }
        let Edit::Set(root) = &deltas[0].edit else {
            return Err(invalid_effect());
        };
        Ok(Self { root: root.clone() })
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (init, deltas) = decode(effect)?;
        if init {
            return Err(invalid_effect());
        }
        // Recovery is transactional even for corrupt/malicious effects. COW only
        // copies ancestors of changed subtrees, never the untouched document.
        let mut staged = self.root.clone();
        for d in &deltas {
            validate_delta(&staged, d).map_err(|_| invalid_effect())?;
            apply_delta(&mut staged, d).map_err(|_| invalid_effect())?;
            node::check(&staged).map_err(|_| invalid_effect())?;
        }
        self.root = staged;
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot { value: Some(self) }
    }
}
pub(super) struct Snapshot<'a> {
    value: Option<&'a JsonValue>,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        self.value.take().map(|v| {
            encode(
                true,
                &[Delta {
                    path: vec![],
                    edit: Edit::Set(v.root.clone()),
                }],
            )
        })
    }
}

fn effect_bound(deltas: &[Delta]) -> Result<(), Resp> {
    let mut size = 6_usize;
    let mut charge = |n: usize| -> Result<(), Resp> {
        size = size
            .checked_add(n)
            .filter(|n| *n <= super::typed::codec::MAX_EFFECT_BYTES)
            .ok_or_else(limit)?;
        Ok(())
    };
    if deltas.len() > MAX_MATCHES {
        return Err(limit());
    }
    for d in deltas {
        charge(5)?;
        for p in &d.path {
            charge(match p {
                Part::Field(k) => 5 + k.len(),
                Part::Index(_) => 9,
            })?;
        }
        match &d.edit {
            Edit::Set(v) => charge(4 + v.bytes)?,
            Edit::Remove => {}
            Edit::Append(s) => charge(4 + s.len())?,
            Edit::Splice { insert, .. } => {
                charge(20)?;
                for v in insert {
                    charge(4 + v.bytes)?;
                }
            }
        }
    }
    Ok(())
}
fn encode(init: bool, deltas: &[Delta]) -> io::Result<Bytes> {
    let mut e = Encoder::new(1, if init { 0 } else { 1 });
    e.u32(u32::try_from(deltas.len()).map_err(|_| invalid_effect())?)?;
    for d in deltas {
        e.u32(d.path.len() as u32)?;
        for p in &d.path {
            match p {
                Part::Field(s) => {
                    e.u8(0)?;
                    e.bytes(s.as_bytes())?;
                }
                Part::Index(i) => {
                    e.u8(1)?;
                    e.u64(*i as u64)?;
                }
            }
        }
        match &d.edit {
            Edit::Set(v) => {
                e.u8(0)?;
                e.bytes(&v.json())?;
            }
            Edit::Remove => {
                e.u8(1)?;
            }
            Edit::Splice {
                start,
                delete,
                insert,
            } => {
                e.u8(2)?;
                e.u64(*start as u64)?;
                e.u64(*delete as u64)?;
                e.u32(insert.len() as u32)?;
                for v in insert {
                    e.bytes(&v.json())?;
                }
            }
            Edit::Append(s) => {
                e.u8(3)?;
                e.bytes(s.as_bytes())?;
            }
        }
    }
    Ok(e.finish())
}
fn decode(effect: &[u8]) -> io::Result<(bool, Vec<Delta>)> {
    let (op, mut d) = Decoder::new(effect, 1)?;
    if op > 1 {
        return Err(invalid_effect());
    }
    let count = d.u32()? as usize;
    if count > MAX_MATCHES {
        return Err(invalid_effect());
    }
    let mut out = Vec::new();
    for _ in 0..count {
        let depth = d.u32()? as usize;
        if depth > MAX_DEPTH {
            return Err(invalid_effect());
        }
        let mut path = Vec::new();
        for _ in 0..depth {
            path.push(match d.u8()? {
                0 => Part::Field(
                    std::str::from_utf8(d.bytes()?)
                        .map_err(|_| invalid_effect())?
                        .to_owned(),
                ),
                1 => Part::Index(usize::try_from(d.u64()?).map_err(|_| invalid_effect())?),
                _ => return Err(invalid_effect()),
            });
        }
        let edit = match d.u8()? {
            0 => Edit::Set(Node::parse(d.bytes()?).map_err(|_| invalid_effect())?),
            1 => Edit::Remove,
            2 => {
                let start = usize::try_from(d.u64()?).map_err(|_| invalid_effect())?;
                let delete = usize::try_from(d.u64()?).map_err(|_| invalid_effect())?;
                let count = d.u32()? as usize;
                if count > MAX_NODES {
                    return Err(invalid_effect());
                }
                let mut insert = Vec::new();
                for _ in 0..count {
                    insert.push(Node::parse(d.bytes()?).map_err(|_| invalid_effect())?);
                }
                Edit::Splice {
                    start,
                    delete,
                    insert,
                }
            }
            3 => Edit::Append(
                std::str::from_utf8(d.bytes()?)
                    .map_err(|_| invalid_effect())?
                    .to_owned(),
            ),
            _ => return Err(invalid_effect()),
        };
        out.push(Delta { path, edit });
    }
    d.finish()?;
    Ok((op == 0, out))
}
fn delta_stats(root: &V, d: &Delta) -> Result<(usize, usize, usize, usize, usize), Resp> {
    let old = path::at(root, &d.path);
    match &d.edit {
        Edit::Set(v) => {
            let mut extra = 0;
            if old.is_none() {
                let Some(Part::Field(k)) = d.path.last() else {
                    return Err(path_type());
                };
                let parent = path::at(root, &d.path[..d.path.len() - 1]).ok_or_else(path_type)?;
                let Kind::Object(o) = &parent.kind else {
                    return Err(path_type());
                };
                extra = node::quoted_len(k) + 1 + usize::from(!o.is_empty());
            }
            Ok((
                old.map_or(0, |v| v.bytes),
                old.map_or(0, |v| v.nodes),
                v.bytes + extra,
                v.nodes,
                d.path.len() + v.depth,
            ))
        }
        Edit::Remove => {
            let v = old.ok_or_else(path_type)?;
            if d.path.is_empty() {
                return Err(path_type());
            }
            let parent = path::at(root, &d.path[..d.path.len() - 1]).ok_or_else(path_type)?;
            let extra = match (d.path.last().unwrap(), &parent.kind) {
                (Part::Field(k), Kind::Object(o)) => {
                    node::quoted_len(k) + 1 + usize::from(o.len() > 1)
                }
                (Part::Index(_), Kind::Array(a)) => usize::from(a.len() > 1),
                _ => return Err(path_type()),
            };
            Ok((v.bytes + extra, v.nodes, 0, 0, 0))
        }
        Edit::Splice {
            start,
            delete,
            insert,
        } => {
            let v = old.ok_or_else(path_type)?;
            let Kind::Array(a) = &v.kind else {
                return Err(path_type());
            };
            let end = start
                .checked_add(*delete)
                .filter(|end| *end <= a.len())
                .ok_or_else(path_type)?;
            let oldbytes =
                a[*start..end].iter().map(|v| v.bytes).sum::<usize>() + a.len().saturating_sub(1);
            let oldnodes = a[*start..end].iter().map(|v| v.nodes).sum();
            let len = a.len() - delete + insert.len();
            let newbytes = insert.iter().map(|v| v.bytes).sum::<usize>() + len.saturating_sub(1);
            let newnodes = insert.iter().map(|v| v.nodes).sum();
            let depth = d.path.len() + 1 + insert.iter().map(|v| v.depth).max().unwrap_or(0);
            Ok((oldbytes, oldnodes, newbytes, newnodes, depth))
        }
        Edit::Append(s) => {
            let v = old.ok_or_else(path_type)?;
            let Kind::String(_) = &v.kind else {
                return Err(path_type());
            };
            Ok((0, 0, node::quoted_len(s) - 2, 0, 0))
        }
    }
}
fn validate_delta(root: &V, d: &Delta) -> Result<(), Resp> {
    let (ob, on, nb, nn, depth) = delta_stats(root, d)?;
    if root
        .bytes
        .checked_sub(ob)
        .and_then(|n| n.checked_add(nb))
        .is_none_or(|n| n > MAX_BYTES)
        || root
            .nodes
            .checked_sub(on)
            .and_then(|n| n.checked_add(nn))
            .is_none_or(|n| n > MAX_NODES)
        || depth > MAX_DEPTH
    {
        return Err(limit());
    }
    Ok(())
}
fn apply_delta(root: &mut V, d: &Delta) -> Result<(), Resp> {
    fn descend(v: &mut V, path: &[Part], edit: &Edit) -> Result<(), Resp> {
        if path.is_empty() {
            match edit {
                Edit::Set(new) => *v = new.clone(),
                Edit::Append(s) => {
                    let v = Arc::make_mut(v);
                    let Kind::String(old) = &mut v.kind else {
                        return Err(path_type());
                    };
                    old.push_str(s);
                    v.bytes += node::quoted_len(s) - 2;
                }
                Edit::Splice {
                    start,
                    delete,
                    insert,
                } => {
                    let v = Arc::make_mut(v);
                    let Kind::Array(a) = &mut v.kind else {
                        return Err(path_type());
                    };
                    let end = start
                        .checked_add(*delete)
                        .filter(|end| *end <= a.len())
                        .ok_or_else(path_type)?;
                    let oldbytes = a[*start..end].iter().map(|v| v.bytes).sum::<usize>()
                        + a.len().saturating_sub(1);
                    let oldnodes = a[*start..end].iter().map(|v| v.nodes).sum::<usize>();
                    let newbytes = insert.iter().map(|v| v.bytes).sum::<usize>()
                        + (a.len() - delete + insert.len()).saturating_sub(1);
                    let newnodes = insert.iter().map(|v| v.nodes).sum::<usize>();
                    let depth = 1 + insert.iter().map(|v| v.depth).max().unwrap_or(0);
                    a.splice(*start..end, insert.iter().cloned());
                    v.bytes = v.bytes - oldbytes + newbytes;
                    v.nodes = v.nodes - oldnodes + newnodes;
                    v.depth = v.depth.max(depth);
                }
                Edit::Remove => return Err(path_type()),
            }
            return Ok(());
        }
        let parent = Arc::make_mut(v);
        if path.len() == 1 {
            match (&path[0], &mut parent.kind, edit) {
                (Part::Field(k), Kind::Object(o), Edit::Remove) => {
                    let old = o.shift_remove(k).ok_or_else(path_type)?;
                    parent.bytes -=
                        old.bytes + node::quoted_len(k) + 1 + usize::from(!o.is_empty());
                    parent.nodes -= old.nodes;
                    return Ok(());
                }
                (Part::Index(i), Kind::Array(a), Edit::Remove) => {
                    if *i >= a.len() {
                        return Err(path_type());
                    }
                    let old = a.remove(*i);
                    parent.bytes -= old.bytes + usize::from(!a.is_empty());
                    parent.nodes -= old.nodes;
                    return Ok(());
                }
                (Part::Field(k), Kind::Object(o), Edit::Set(new)) if !o.contains_key(k) => {
                    parent.bytes +=
                        new.bytes + node::quoted_len(k) + 1 + usize::from(!o.is_empty());
                    parent.nodes += new.nodes;
                    parent.depth = parent.depth.max(1 + new.depth);
                    o.insert(k.clone(), new.clone());
                    return Ok(());
                }
                _ => {}
            }
        }
        let child = match (&path[0], &mut parent.kind) {
            (Part::Field(k), Kind::Object(o)) => o.get_mut(k),
            (Part::Index(i), Kind::Array(a)) => a.get_mut(*i),
            _ => None,
        }
        .ok_or_else(path_type)?;
        let (bytes, nodes) = (child.bytes, child.nodes);
        descend(child, &path[1..], edit)?;
        parent.bytes = parent.bytes - bytes + child.bytes;
        parent.nodes = parent.nodes - nodes + child.nodes;
        parent.depth = parent.depth.max(1 + child.depth);
        Ok(())
    }
    descend(root, &d.path, &d.edit)
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "json.set",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_set,
    ),
    CommandSpec::handler("json.get", -2, &["readonly"], (1, 1, 1), Engine::json_get),
    CommandSpec::handler(
        "json.mget",
        -3,
        &["readonly"],
        (1, -2, 1),
        Engine::json_mget,
    ),
    CommandSpec::handler(
        "json.mset",
        -4,
        &["write", "denyoom"],
        (1, -1, 3),
        Engine::json_mset,
    ),
    CommandSpec::handler(
        "json.merge",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_merge,
    ),
    CommandSpec::handler("json.del", -2, &["write"], (1, 1, 1), Engine::json_del),
    CommandSpec::handler("json.forget", -2, &["write"], (1, 1, 1), Engine::json_del),
    CommandSpec::handler(
        "json.type",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::json_read,
    ),
    CommandSpec::handler(
        "json.strlen",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::json_read,
    ),
    CommandSpec::handler(
        "json.arrlen",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::json_read,
    ),
    CommandSpec::handler(
        "json.objlen",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::json_read,
    ),
    CommandSpec::handler(
        "json.objkeys",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::json_read,
    ),
    CommandSpec::handler("json.resp", -2, &["readonly"], (1, 1, 1), Engine::json_resp),
    CommandSpec::handler(
        "json.debug",
        -2,
        &["readonly"],
        (0, 0, 0),
        Engine::json_debug,
    ),
    CommandSpec::handler(
        "json.numincrby",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_number,
    ),
    CommandSpec::handler(
        "json.nummultby",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_number,
    ),
    CommandSpec::handler(
        "json.numpowby",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_number,
    ),
    CommandSpec::handler(
        "json.strappend",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_mutate,
    ),
    CommandSpec::handler(
        "json.arrappend",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_mutate,
    ),
    CommandSpec::handler(
        "json.arrinsert",
        -5,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::json_mutate,
    ),
    CommandSpec::handler(
        "json.arrindex",
        -4,
        &["readonly"],
        (1, 1, 1),
        Engine::json_arrindex,
    ),
    CommandSpec::handler(
        "json.arrpop",
        -2,
        &["write"],
        (1, 1, 1),
        Engine::json_mutate,
    ),
    CommandSpec::handler(
        "json.arrtrim",
        5,
        &["write"],
        (1, 1, 1),
        Engine::json_mutate,
    ),
    CommandSpec::handler("json.clear", -2, &["write"], (1, 1, 1), Engine::json_mutate),
    CommandSpec::handler("json.toggle", 3, &["write"], (1, 1, 1), Engine::json_mutate),
];

impl Engine {
    fn json_value(&mut self, key: &Bytes, now: i64) -> Result<Option<&JsonValue>, Resp> {
        match self.value(key, now) {
            None => Ok(None),
            Some(Value::Typed(v)) => match &**v {
                Data::Json(v) => Ok(Some(v)),
                _ => Err(wrong_type()),
            },
            _ => Err(wrong_type()),
        }
    }
    fn json_commit(&mut self, key: &Bytes, mut deltas: Vec<Delta>, reply: Resp, now: i64) -> Resp {
        if let Err(reply) = ReplyBudget::new(self.reply_limit).response(&reply) {
            return reply;
        }
        if deltas.is_empty() {
            return reply;
        }
        if deltas.len() > MAX_MATCHES {
            return limit();
        }
        // Reverse document order: array deletions cannot shift remaining indices;
        // descendants are applied before replacing/removing their ancestors.
        deltas.sort_by(|a, b| b.path.cmp(&a.path));
        let root = match self.json_value(key, now) {
            Ok(Some(v)) => &v.root,
            Ok(None) => return missing(),
            Err(e) => return e,
        };
        let overlapping = deltas
            .windows(2)
            .any(|pair| pair[0].path.starts_with(&pair[1].path));
        let mut staged = None;
        if overlapping {
            let mut candidate = root.clone();
            let mut actual = Vec::with_capacity(deltas.len());
            for d in deltas {
                if matches!(d.edit, Edit::Remove) && path::at(&candidate, &d.path).is_none() {
                    continue;
                }
                if let Err(e) = validate_delta(&candidate, &d) {
                    return e;
                }
                if let Err(e) = apply_delta(&mut candidate, &d) {
                    return e;
                }
                actual.push(d);
            }
            deltas = actual;
            staged = Some(candidate);
        } else {
            let (mut bytes, mut nodes) = (root.bytes, root.nodes);
            // Disjoint children can still share a parent. Track only changed
            // container lengths so comma charges match sequential effect replay
            // without copying or serializing the existing document.
            let mut parent_lengths: BTreeMap<&[Part], usize> = BTreeMap::new();
            for d in &deltas {
                let (mut ob, on, mut nb, nn, depth) = match delta_stats(root, d) {
                    Ok(s) => s,
                    Err(e) => return e,
                };
                if !d.path.is_empty() {
                    let parent_path = &d.path[..d.path.len() - 1];
                    match &d.edit {
                        Edit::Set(_) if path::at(root, &d.path).is_none() => {
                            let Some(parent) = path::at(root, parent_path) else {
                                return path_type();
                            };
                            let Kind::Object(o) = &parent.kind else {
                                return path_type();
                            };
                            let len = parent_lengths.entry(parent_path).or_insert(o.len());
                            nb = nb - usize::from(!o.is_empty()) + usize::from(*len > 0);
                            *len += 1;
                        }
                        Edit::Remove => {
                            let Some(parent) = path::at(root, parent_path) else {
                                return path_type();
                            };
                            let original = match &parent.kind {
                                Kind::Object(o) => o.len(),
                                Kind::Array(a) => a.len(),
                                _ => return path_type(),
                            };
                            let len = parent_lengths.entry(parent_path).or_insert(original);
                            ob = ob - usize::from(original > 1) + usize::from(*len > 1);
                            *len -= 1;
                        }
                        _ => {}
                    }
                }
                bytes = match bytes.checked_sub(ob).and_then(|n| n.checked_add(nb)) {
                    Some(n) => n,
                    None => return limit(),
                };
                nodes = match nodes.checked_sub(on).and_then(|n| n.checked_add(nn)) {
                    Some(n) => n,
                    None => return limit(),
                };
                if bytes > MAX_BYTES || nodes > MAX_NODES || depth > MAX_DEPTH {
                    return limit();
                }
            }
        }
        if let Err(e) = effect_bound(&deltas) {
            return e;
        }
        let effect = match encode(false, &deltas) {
            Ok(e) => e,
            Err(e) => return self.fail_io(e),
        };
        let Some(Entry {
            value: Value::Typed(data),
            ..
        }) = self.entries.get_mut(key)
        else {
            unreachable!()
        };
        let Data::Json(value) = &mut **data else {
            unreachable!()
        };
        if let Some(root) = staged {
            value.root = root;
        } else {
            for d in &deltas {
                if let Err(e) = apply_delta(&mut value.root, d) {
                    return e;
                }
            }
        }
        self.record_typed(key, 2, effect, now);
        reply
    }
    fn json_insert(&mut self, key: &Bytes, root: V, now: i64) -> Resp {
        if let Err(e) = node::check(&root) {
            return e;
        }
        if let Err(e) = ReplyBudget::new(self.reply_limit).response(&Resp::Simple("OK")) {
            return e;
        }
        let deltas = [Delta {
            path: vec![],
            edit: Edit::Set(root.clone()),
        }];
        if let Err(e) = effect_bound(&deltas) {
            return e;
        }
        let effect = match encode(true, &deltas) {
            Ok(e) => e,
            Err(e) => return self.fail_io(e),
        };
        self.write_entry(
            key.clone(),
            Entry {
                value: Value::Typed(Box::new(Data::Json(JsonValue { root }))),
                expires_at: None,
                access: crate::engine::AccessMetadata::default(),
            },
            now,
        );
        self.record_typed(key, 2, effect, now);
        Resp::Simple("OK")
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Condition {
    Any,
    Nx,
    Xx,
}
fn set_plan(
    root: &V,
    path: &Path,
    value: &V,
    condition: Condition,
    work: &mut Work,
) -> Result<Vec<Delta>, Resp> {
    let existing = path.targets(root, work)?;
    if condition == Condition::Nx && !existing.is_empty() {
        return Ok(vec![]);
    }
    if condition == Condition::Xx && existing.is_empty() {
        return Ok(vec![]);
    }
    let matches = if condition == Condition::Xx {
        existing
    } else {
        path.writable(root, work)?
    };
    Ok(matches
        .into_iter()
        .map(|path| Delta {
            path,
            edit: Edit::Set(value.clone()),
        })
        .collect())
}
#[derive(Clone, Copy)]
enum InputFormat {
    Json,
    Bson,
}
impl InputFormat {
    fn parse(value: &[u8]) -> Result<Self, Resp> {
        if value.eq_ignore_ascii_case(b"JSON") || value.eq_ignore_ascii_case(b"STRING") {
            Ok(Self::Json)
        } else if value.eq_ignore_ascii_case(b"BSON") {
            Ok(Self::Bson)
        } else {
            Err(error("ERR wrong format"))
        }
    }
    fn value(self, bytes: &[u8]) -> Result<V, Resp> {
        match self {
            Self::Json => Node::parse(bytes),
            Self::Bson => bson_input::parse(bytes),
        }
    }
}
fn parse_set_options(args: &[Bytes]) -> Result<(Condition, Option<&[u8]>, InputFormat), Resp> {
    let mut condition = Condition::Any;
    let mut fpha = None;
    let mut input = InputFormat::Json;
    let mut i = 0;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"NX") || args[i].eq_ignore_ascii_case(b"XX") {
            if condition != Condition::Any {
                return Err(syntax_error());
            }
            condition = if args[i].eq_ignore_ascii_case(b"NX") {
                Condition::Nx
            } else {
                Condition::Xx
            };
            i += 1;
        } else if args[i].eq_ignore_ascii_case(b"FPHA") {
            if fpha.is_some() {
                return Err(syntax_error());
            }
            let value = args.get(i + 1).ok_or_else(syntax_error)?;
            if ![b"FP16".as_slice(), b"BF16", b"FP32", b"FP64"]
                .iter()
                .any(|v| value.eq_ignore_ascii_case(v))
            {
                return Err(syntax_error());
            }
            fpha = Some(value.as_ref());
            i += 2;
        } else if args[i].eq_ignore_ascii_case(b"FORMAT") {
            input = InputFormat::parse(args.get(i + 1).ok_or_else(syntax_error)?)?;
            i += 2;
        } else {
            return Err(syntax_error());
        }
    }
    Ok((condition, fpha, input))
}
fn float_array_type(value: V, format: &[u8]) -> Result<V, Resp> {
    fn round_binary(f: f64, fraction: i32, min_exponent: i32, max_exponent: i32) -> f64 {
        if f == 0.0 {
            return f;
        }
        let exponent = ((f.to_bits() >> 52) & 2047) as i32 - 1023;
        if exponent > max_exponent {
            return f64::INFINITY.copysign(f);
        }
        if exponent < min_exponent - fraction - 1 {
            return 0.0_f64.copysign(f);
        }
        let step = 2_f64.powi(exponent.max(min_exponent) - fraction);
        let magnitude = (f.abs() / step).round_ties_even() * step;
        let largest = (2.0 - 2_f64.powi(-fraction)) * 2_f64.powi(max_exponent);
        if magnitude > largest {
            f64::INFINITY.copysign(f)
        } else {
            magnitude.copysign(f)
        }
    }
    fn convert(v: &V, format: &[u8]) -> Result<V, Resp> {
        match &v.kind {
            Kind::Array(a)
                if !a.is_empty() && a.iter().all(|v| matches!(v.kind, Kind::Number(_))) =>
            {
                let values = a
                    .iter()
                    .map(|v| {
                        let Kind::Number(n) = &v.kind else {
                            unreachable!()
                        };
                        let f = n.as_f64().ok_or_else(invalid)?;
                        let f = if format.eq_ignore_ascii_case(b"FP64") {
                            f
                        } else if format.eq_ignore_ascii_case(b"FP32") {
                            f as f32 as f64
                        } else if format.eq_ignore_ascii_case(b"FP16") {
                            round_binary(f, 10, -14, 15)
                        } else {
                            round_binary(f, 7, -126, 127)
                        };
                        node::number(f)
                            .map_err(|_| error("ERR value out of range for floating point array"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Node::new(Kind::Array(values)))
            }
            Kind::Array(a) => Ok(Node::new(Kind::Array(
                a.iter()
                    .map(|v| convert(v, format))
                    .collect::<Result<_, _>>()?,
            ))),
            Kind::Object(o) => Ok(Node::new(Kind::Object(
                o.iter()
                    .map(|(k, v)| Ok((k.clone(), convert(v, format)?)))
                    .collect::<Result<_, Resp>>()?,
            ))),
            _ => Ok(v.clone()),
        }
    }
    convert(&value, format)
}

impl Engine {
    fn json_set(&mut self, args: &[Bytes], now: i64) -> Resp {
        let path = match Path::parse(&args[2]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let (condition, fpha, input) = match parse_set_options(&args[4..]) {
            Ok(o) => o,
            Err(e) => return e,
        };
        let mut value = match input.value(&args[3]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        if let Some(fpha) = fpha {
            value = match float_array_type(value, fpha) {
                Ok(v) => v,
                Err(e) => return e,
            };
        }
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(Some(v)) => &v.root,
            Ok(None) => {
                if condition == Condition::Xx {
                    return Resp::Bulk(None);
                }
                if !path.segments.is_empty() {
                    return error("ERR new objects must be created at the root");
                }
                return self.json_insert(&args[1], value, now);
            }
        };
        let plan = match set_plan(root, &path, &value, condition, &mut Work::new()) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if plan.is_empty() {
            return Resp::Bulk(None);
        }
        self.json_commit(&args[1], plan, Resp::Simple("OK"), now)
    }
    fn json_mset(&mut self, args: &[Bytes], now: i64) -> Resp {
        if !(args.len() - 1).is_multiple_of(3) {
            return wrong_arity("json.mset");
        }
        struct Stage {
            root: V,
            new: bool,
            deltas: Vec<Delta>,
        }
        let mut stages: BTreeMap<Bytes, Stage> = BTreeMap::new();
        let mut work = Work::new();
        for triplet in args[1..].as_chunks::<3>().0 {
            let key = &triplet[0];
            let path = match Path::parse(&triplet[1]) {
                Ok(p) => p,
                Err(e) => return e,
            };
            let value = match Node::parse(&triplet[2]) {
                Ok(v) => v,
                Err(e) => return e,
            };
            if !stages.contains_key(key) {
                match self.json_value(key, now) {
                    Err(e) => return e,
                    Ok(Some(v)) => {
                        stages.insert(
                            key.clone(),
                            Stage {
                                root: v.root.clone(),
                                new: false,
                                deltas: vec![],
                            },
                        );
                    }
                    Ok(None) => {
                        if !path.segments.is_empty() {
                            return error("ERR new objects must be created at the root");
                        }
                        stages.insert(
                            key.clone(),
                            Stage {
                                root: value,
                                new: true,
                                deltas: vec![],
                            },
                        );
                        continue;
                    }
                }
            }
            let stage = stages.get_mut(key).unwrap();
            let mut plan = match set_plan(&stage.root, &path, &value, Condition::Any, &mut work) {
                Ok(p) => p,
                Err(e) => return e,
            };
            if plan.is_empty() {
                return error("ERR Path does not exist");
            }
            plan.sort_by(|a, b| b.path.cmp(&a.path));
            for d in &plan {
                if let Err(e) = validate_delta(&stage.root, d) {
                    return e;
                }
                if let Err(e) = apply_delta(&mut stage.root, d) {
                    return e;
                }
            }
            stage.deltas.extend(plan);
            if stage.deltas.len() > MAX_MATCHES {
                return limit();
            }
        }
        if let Err(e) = ReplyBudget::new(self.reply_limit).response(&Resp::Simple("OK")) {
            return e;
        }
        let mut ready = Vec::new();
        for (key, stage) in stages {
            let initialization = [Delta {
                path: vec![],
                edit: Edit::Set(stage.root.clone()),
            }];
            let deltas = if stage.new {
                initialization.as_slice()
            } else {
                stage.deltas.as_slice()
            };
            if let Err(e) = effect_bound(deltas) {
                return e;
            }
            let effect = match encode(stage.new, deltas) {
                Ok(e) => e,
                Err(e) => return self.fail_io(e),
            };
            ready.push((key, stage, effect));
        }
        for (key, stage, effect) in ready {
            if stage.new {
                self.write_entry(
                    key.clone(),
                    Entry {
                        value: Value::Typed(Box::new(Data::Json(JsonValue { root: stage.root }))),
                        expires_at: None,
                        access: crate::engine::AccessMetadata::default(),
                    },
                    now,
                );
            } else {
                let Some(Entry {
                    value: Value::Typed(data),
                    ..
                }) = self.entries.get_mut(&key)
                else {
                    unreachable!()
                };
                let Data::Json(value) = &mut **data else {
                    unreachable!()
                };
                value.root = stage.root;
            }
            self.record_typed(&key, 2, effect, now);
        }
        Resp::Simple("OK")
    }
    fn json_merge(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut input = InputFormat::Json;
        for pair in args[4..].chunks(2) {
            if pair.len() != 2 || !pair[0].eq_ignore_ascii_case(b"FORMAT") {
                return syntax_error();
            }
            input = match InputFormat::parse(&pair[1]) {
                Ok(f) => f,
                Err(e) => return e,
            };
        }
        let path = match Path::parse(&args[2]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let patch = match input.value(&args[3]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(Some(v)) => &v.root,
            Ok(None) => {
                if !path.segments.is_empty() {
                    return error("ERR new objects must be created at the root");
                }
                let root = merge_new(&patch);
                return self.json_insert(&args[1], root, now);
            }
        };
        let mut work = Work::new();
        let matches = match path.writable(root, &mut work) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if matches.is_empty() {
            return error("ERR Path does not exist");
        }
        let mut deltas = Vec::new();
        for p in matches {
            if let Err(e) = merge_plan(path::at(root, &p), &patch, p, &mut deltas, &mut work) {
                return e;
            }
        }
        self.json_commit(&args[1], deltas, Resp::Simple("OK"), now)
    }
    fn json_del(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() > 3 {
            return wrong_arity("json.del");
        }
        let path = match Path::parse(args.get(2).map_or(b".".as_slice(), Bytes::as_ref)) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return Resp::Integer(0),
            Ok(Some(v)) => &v.root,
        };
        let matches = match path.targets(root, &mut Work::new()) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if matches.iter().any(Vec::is_empty) {
            if let Err(e) = ReplyBudget::new(self.reply_limit).response(&Resp::Integer(1)) {
                return e;
            }
            self.remove(&args[1]);
            self.record(now, || Mutation::Delete {
                key: args[1].clone(),
            });
            return Resp::Integer(1);
        }
        // Removing an ancestor subsumes descendants and counts one deletion.
        let mut matches = matches;
        matches.sort();
        let mut paths: Vec<Location> = Vec::new();
        for p in matches {
            if paths.last().is_none_or(|parent| !p.starts_with(parent)) {
                paths.push(p);
            }
        }
        let count = paths.len();
        let plan = paths
            .into_iter()
            .map(|path| Delta {
                path,
                edit: Edit::Remove,
            })
            .collect();
        let reply = self.json_commit(&args[1], plan, Resp::Integer(count as i64), now);
        if matches!(reply,Resp::Integer(n) if n>0) {
            let empty = match self.json_value(&args[1], now) {
                Ok(Some(v)) => match &v.root.kind {
                    Kind::Array(a) => a.is_empty(),
                    Kind::Object(o) => o.is_empty(),
                    _ => false,
                },
                _ => false,
            };
            if empty {
                self.remove(&args[1]);
                self.record(now, || Mutation::Delete {
                    key: args[1].clone(),
                });
            }
        }
        reply
    }
}
fn merge_new(patch: &V) -> V {
    match &patch.kind {
        Kind::Object(o) => Node::new(Kind::Object(
            o.iter()
                .filter(|(_, v)| !matches!(v.kind, Kind::Null))
                .map(|(k, v)| (k.clone(), merge_new(v)))
                .collect(),
        )),
        _ => patch.clone(),
    }
}
fn merge_plan(
    old: Option<&V>,
    patch: &V,
    path: Location,
    out: &mut Vec<Delta>,
    work: &mut Work,
) -> Result<(), Resp> {
    work.charge(1)?;
    if out.len() > MAX_MATCHES {
        return Err(limit());
    }
    if let (Some(old), Kind::Object(patch)) = (old, &patch.kind)
        && let Kind::Object(old) = &old.kind
    {
        for (k, patch) in patch {
            let mut p = path.clone();
            p.push(Part::Field(k.clone()));
            if matches!(patch.kind, Kind::Null) {
                if old.contains_key(k) {
                    out.push(Delta {
                        path: p,
                        edit: Edit::Remove,
                    });
                }
            } else {
                merge_plan(old.get(k), patch, p, out, work)?;
            }
        }
        return Ok(());
    }
    out.push(Delta {
        path,
        edit: Edit::Set(merge_new(patch)),
    });
    Ok(())
}

#[derive(Default)]
struct Formatting {
    indent: Vec<u8>,
    newline: Vec<u8>,
    space: Vec<u8>,
}
struct Writer {
    bytes: Vec<u8>,
    remaining: usize,
}
impl Writer {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            remaining: limit.saturating_sub(std::mem::size_of::<Resp>()),
        }
    }
    fn put(&mut self, bytes: &[u8]) -> Result<(), Resp> {
        self.remaining = self
            .remaining
            .checked_sub(bytes.len())
            .ok_or_else(super::reply_limit_error)?;
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    fn quoted(&mut self, s: &str) -> Result<(), Resp> {
        let n = node::quoted_len(s);
        if n > self.remaining {
            return Err(super::reply_limit_error());
        }
        self.remaining -= n;
        node::write_string(s, &mut self.bytes);
        Ok(())
    }
    fn line(&mut self, depth: usize, f: &Formatting) -> Result<(), Resp> {
        self.put(&f.newline)?;
        for _ in 0..depth {
            self.put(&f.indent)?;
        }
        Ok(())
    }
    fn value(&mut self, v: &Node, depth: usize, f: &Formatting) -> Result<(), Resp> {
        match &v.kind {
            Kind::Array(a) => {
                self.put(b"[")?;
                for (i, v) in a.iter().enumerate() {
                    if i > 0 {
                        self.put(b",")?;
                    }
                    self.line(depth + 1, f)?;
                    self.value(v, depth + 1, f)?;
                }
                if !a.is_empty() {
                    self.line(depth, f)?;
                }
                self.put(b"]")
            }
            Kind::Object(o) => {
                self.put(b"{")?;
                for (i, (k, v)) in o.iter().enumerate() {
                    if i > 0 {
                        self.put(b",")?;
                    }
                    self.line(depth + 1, f)?;
                    self.quoted(k)?;
                    self.put(b":")?;
                    self.put(&f.space)?;
                    self.value(v, depth + 1, f)?;
                }
                if !o.is_empty() {
                    self.line(depth, f)?;
                }
                self.put(b"}")
            }
            Kind::String(s) => self.quoted(s),
            Kind::Number(n) => self.put(n.as_str().as_bytes()),
            Kind::Null => self.put(b"null"),
            Kind::Bool(b) => self.put(if *b { b"true" } else { b"false" }),
        }
    }
    fn values(&mut self, values: &[V], f: &Formatting, depth: usize) -> Result<(), Resp> {
        self.put(b"[")?;
        for (i, v) in values.iter().enumerate() {
            if i > 0 {
                self.put(b",")?;
            }
            self.line(depth + 1, f)?;
            self.value(v, depth + 1, f)?;
        }
        if !values.is_empty() {
            self.line(depth, f)?;
        }
        self.put(b"]")
    }
    fn reply(self) -> Resp {
        bulk(self.bytes)
    }
}
enum Query {
    Path(Path),
    Expression(expr::Expr),
}
impl Query {
    fn parse(raw: &[u8]) -> Result<Self, Resp> {
        if let Ok(path) = Path::parse(raw) {
            return Ok(Self::Path(path));
        }
        let s = std::str::from_utf8(raw).map_err(|_| path::path_error())?;
        Ok(Self::Expression(expr::Expr::parse(s)?))
    }
    fn evaluate(&self, root: &V, work: &mut Work) -> Result<(bool, Vec<V>), Resp> {
        match self {
            Self::Path(path) => Ok((
                path.modern,
                path.resolve(root, work)?
                    .iter()
                    .filter_map(|p| path::at(root, p).cloned())
                    .collect(),
            )),
            Self::Expression(expr) => Ok((true, expr.eval(root, root, work)?)),
        }
    }
}
fn query(root: &V, raw: &[u8], work: &mut Work) -> Result<(bool, Vec<V>), Resp> {
    Query::parse(raw)?.evaluate(root, work)
}
fn resp_value(v: &Node, budget: &mut ReplyBudget, resp3: bool) -> Result<Resp, Resp> {
    budget.charge(1, 0)?;
    Ok(match &v.kind {
        Kind::Null => Resp::Bulk(None),
        Kind::Bool(b) => Resp::Simple(if *b { "true" } else { "false" }),
        Kind::Number(n) => {
            if let Some(n) = n.as_i64() {
                Resp::Integer(n)
            } else if resp3 && n.as_str().contains(['.', 'e', 'E']) {
                Resp::Double(n.as_f64().ok_or_else(invalid)?)
            } else {
                budget.charge(0, n.as_str().len())?;
                text(n.as_str())
            }
        }
        Kind::String(s) => {
            budget.charge(0, s.len())?;
            text(s)
        }
        Kind::Array(a) => {
            budget.charge(a.len() + 1, 0)?;
            let mut values = Vec::with_capacity(a.len() + 1);
            values.push(Resp::Simple("["));
            for v in a {
                values.push(resp_value(v, budget, resp3)?);
            }
            array(values)
        }
        Kind::Object(o) => {
            budget.charge(o.len() * 2 + 1, 0)?;
            let mut values = Vec::with_capacity(o.len() * 2 + 1);
            values.push(Resp::Simple("{"));
            for (k, v) in o {
                budget.charge(0, k.len())?;
                values.push(text(k));
                values.push(resp_value(v, budget, resp3)?);
            }
            array(values)
        }
    })
}
fn expanded(
    v: &Node,
    deep: bool,
    budget: &mut ReplyBudget,
    format: &Formatting,
) -> Result<Resp, Resp> {
    budget.charge(1, 0)?;
    Ok(match &v.kind {
        Kind::Null => Resp::Bulk(None),
        Kind::Bool(b) => Resp::Boolean(*b),
        Kind::String(s) if deep => {
            budget.charge(0, s.len())?;
            text(s)
        }
        Kind::Number(n) => {
            if let Some(n) = n.as_i64() {
                Resp::Integer(n)
            } else {
                Resp::Double(n.as_f64().ok_or_else(invalid)?)
            }
        }
        Kind::Array(a) if deep => {
            let mut replies = Vec::new();
            for v in a {
                replies.push(expanded(v, true, budget, format)?);
            }
            array(replies)
        }
        Kind::Object(o) if deep => {
            let mut replies = Vec::new();
            for (k, v) in o {
                budget.charge(1, k.len())?;
                replies.push((text(k), expanded(v, true, budget, format)?));
            }
            Resp::Map(replies)
        }
        _ => {
            let mut writer = Writer {
                bytes: Vec::new(),
                remaining: budget.remaining,
            };
            writer.value(v, 0, format)?;
            budget.charge(0, writer.bytes.len())?;
            writer.reply()
        }
    })
}

impl Engine {
    fn json_get(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut format = Formatting::default();
        let mut paths = Vec::new();
        let mut seen: std::collections::HashSet<&[u8]> = std::collections::HashSet::new();
        let mut mode = 0;
        let mut i = 2;
        while i < args.len() {
            let option = &args[i];
            if option.eq_ignore_ascii_case(b"INDENT")
                || option.eq_ignore_ascii_case(b"NEWLINE")
                || option.eq_ignore_ascii_case(b"SPACE")
            {
                let Some(value) = args.get(i + 1) else {
                    return syntax_error();
                };
                let target = if option.eq_ignore_ascii_case(b"INDENT") {
                    &mut format.indent
                } else if option.eq_ignore_ascii_case(b"NEWLINE") {
                    &mut format.newline
                } else {
                    &mut format.space
                };
                *target = value.to_vec();
                i += 2;
            } else if option.eq_ignore_ascii_case(b"FORMAT") {
                if paths.is_empty() && self.command_version() != crate::protocol::Version::Resp3 {
                    return error("ERR FORMAT argument is not supported on RESP2");
                }
                let Some(value) = args.get(i + 1) else {
                    return syntax_error();
                };
                mode = if value.eq_ignore_ascii_case(b"STRING") {
                    0
                } else if value.eq_ignore_ascii_case(b"EXPAND1") {
                    1
                } else if value.eq_ignore_ascii_case(b"EXPAND") {
                    2
                } else {
                    return syntax_error();
                };
                i += 2;
            } else if option.eq_ignore_ascii_case(b"NOESCAPE") {
                i += 1;
            } else {
                if seen.insert(option.as_ref()) {
                    paths.push(option.as_ref());
                }
                i += 1;
            }
        }
        if paths.is_empty() {
            paths.push(b".");
        }
        if mode != 0 && self.command_version() != crate::protocol::Version::Resp3 {
            return error("ERR FORMAT EXPAND is only supported with RESP3");
        }
        let limit = self.reply_limit;
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return Resp::Bulk(None),
            Ok(Some(v)) => &v.root,
        };
        let mut work = Work::new();
        let mut matches = Vec::new();
        let mut modern = false;
        for raw in &paths {
            let (m, values) = match query(root, raw, &mut work) {
                Ok(q) => q,
                Err(e) => return e,
            };
            modern |= m;
            matches.push(values);
        }
        if mode != 0 {
            let mut budget = ReplyBudget::new(limit);
            let mut paths = Vec::new();
            if let Err(e) = budget.charge(1, 0) {
                return e;
            }
            for values in &matches {
                if let Err(e) = budget.charge(1, 0) {
                    return e;
                }
                let mut replies = Vec::new();
                for v in values {
                    match expanded(v, mode == 2, &mut budget, &format) {
                        Ok(v) => replies.push(v),
                        Err(e) => return e,
                    }
                }
                paths.push(array(replies));
            }
            return array(paths);
        }
        let mut writer = Writer::new(limit);
        let result = (|| -> Result<(), Resp> {
            if paths.len() == 1 {
                if modern {
                    writer.values(&matches[0], &format, 0)?;
                } else if let Some(v) = matches[0].first() {
                    writer.value(v, 0, &format)?;
                } else {
                    return Err(error("ERR Path does not exist"));
                }
            } else {
                writer.put(b"{")?;
                for (i, (raw, values)) in paths.iter().zip(&matches).enumerate() {
                    if i > 0 {
                        writer.put(b",")?;
                    }
                    writer.line(1, &format)?;
                    writer.quoted(std::str::from_utf8(raw).map_err(|_| path::path_error())?)?;
                    writer.put(b":")?;
                    writer.put(&format.space)?;
                    if modern {
                        writer.values(values, &format, 1)?;
                    } else if let Some(v) = values.first() {
                        writer.value(v, 1, &format)?;
                    } else {
                        return Err(error("ERR Path does not exist"));
                    }
                }
                writer.line(0, &format)?;
                writer.put(b"}")?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => writer.reply(),
            Err(e) => e,
        }
    }
    fn json_mget(&mut self, args: &[Bytes], now: i64) -> Resp {
        let raw = &args[args.len() - 1];
        let mut work = Work::new();
        let limit = self.reply_limit;
        let mut budget = ReplyBudget::new(limit);
        let mut replies = Vec::new();
        let query = match Query::parse(raw) {
            Ok(q) => q,
            Err(e) => return e,
        };
        for key in &args[1..args.len() - 1] {
            let reply = match self.json_value(key, now) {
                Err(_) | Ok(None) => Resp::Bulk(None),
                Ok(Some(v)) => {
                    let (modern, values) = match query.evaluate(&v.root, &mut work) {
                        Ok(q) => q,
                        Err(e) => return e,
                    };
                    let mut writer = Writer::new(limit);
                    let result = if modern {
                        writer.values(&values, &Formatting::default(), 0)
                    } else if let Some(v) = values.first() {
                        writer.value(v, 0, &Formatting::default())
                    } else {
                        let reply = Resp::Bulk(None);
                        if let Err(e) = budget.response(&reply) {
                            return e;
                        }
                        replies.push(reply);
                        continue;
                    };
                    if let Err(e) = result {
                        return e;
                    }
                    writer.reply()
                }
            };
            if let Err(e) = budget.response(&reply) {
                return e;
            }
            replies.push(reply);
        }
        array(replies)
    }
    fn json_read(&mut self, args: &[Bytes], now: i64) -> Resp {
        let resp3 = self.command_version() == crate::protocol::Version::Resp3;
        if args.len() > 3 {
            return wrong_arity("json.type");
        }
        let path = match Path::parse(args.get(2).map_or(b".".as_slice(), Bytes::as_ref)) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let limit = self.reply_limit;
        let object_read = args[0].eq_ignore_ascii_case(b"JSON.OBJLEN")
            || args[0].eq_ignore_ascii_case(b"JSON.OBJKEYS");
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => {
                return if object_read {
                    missing()
                } else {
                    Resp::Bulk(None)
                };
            }
            Ok(Some(v)) => &v.root,
        };
        let paths = match path.resolve(root, &mut Work::new()) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let mut replies = Vec::new();
        let mut budget = ReplyBudget::new(limit);
        for p in paths {
            let v = path::at(root, &p).unwrap();
            let reply = if args[0].eq_ignore_ascii_case(b"JSON.TYPE") {
                let name = text(v.name());
                if resp3 { array(vec![name]) } else { name }
            } else if args[0].eq_ignore_ascii_case(b"JSON.STRLEN") {
                if let Kind::String(s) = &v.kind {
                    Resp::Integer(s.len() as i64)
                } else {
                    Resp::Bulk(None)
                }
            } else if args[0].eq_ignore_ascii_case(b"JSON.ARRLEN") {
                if let Kind::Array(a) = &v.kind {
                    Resp::Integer(a.len() as i64)
                } else {
                    Resp::Bulk(None)
                }
            } else if args[0].eq_ignore_ascii_case(b"JSON.OBJLEN") {
                if let Kind::Object(o) = &v.kind {
                    Resp::Integer(o.len() as i64)
                } else {
                    Resp::Bulk(None)
                }
            } else if let Kind::Object(o) = &v.kind {
                let mut keys = Vec::new();
                let mut local = ReplyBudget::new(limit);
                for k in o.keys() {
                    let reply = text(k);
                    if let Err(e) = local.response(&reply) {
                        return e;
                    }
                    keys.push(reply);
                }
                array(keys)
            } else {
                Resp::Bulk(None)
            };
            if !path.modern {
                if matches!(reply, Resp::Bulk(None)) && !args[0].eq_ignore_ascii_case(b"JSON.TYPE")
                {
                    return path_type();
                }
                return bounded(reply, limit).unwrap_or_else(|e| e);
            }
            if let Err(e) = budget.response(&reply) {
                return e;
            }
            replies.push(reply);
        }
        if !path.modern {
            return if args[0].eq_ignore_ascii_case(b"JSON.TYPE") || object_read {
                Resp::Bulk(None)
            } else {
                error("ERR Path does not exist")
            };
        }
        array(replies)
    }
    fn json_resp(&mut self, args: &[Bytes], now: i64) -> Resp {
        let resp3 = self.command_version() == crate::protocol::Version::Resp3;
        if args.len() > 3 {
            return wrong_arity("json.resp");
        }
        let raw = args.get(2).map_or(b".".as_slice(), Bytes::as_ref);
        let limit = self.reply_limit;
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return Resp::Bulk(None),
            Ok(Some(v)) => &v.root,
        };
        let (modern, values) = match query(root, raw, &mut Work::new()) {
            Ok(q) => q,
            Err(e) => return e,
        };
        let mut budget = ReplyBudget::new(limit);
        let mut replies = Vec::new();
        for v in values {
            let reply = match resp_value(&v, &mut budget, resp3) {
                Ok(r) => r,
                Err(e) => return e,
            };
            if !modern {
                return reply;
            }
            replies.push(reply);
        }
        if modern {
            array(replies)
        } else {
            Resp::Bulk(None)
        }
    }
    fn json_debug(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[1].eq_ignore_ascii_case(b"HELP") {
            if args.len() != 2 {
                return wrong_arity("json.debug");
            }
            return array(vec![
                text("MEMORY <key> [path] -- report allocated JSON value bytes"),
                text("HELP -- show JSON.DEBUG subcommands"),
            ]);
        }
        if !args[1].eq_ignore_ascii_case(b"MEMORY") {
            return error("ERR unknown JSON.DEBUG subcommand");
        }
        if !(3..=4).contains(&args.len()) {
            return wrong_arity("json.debug");
        }
        let path = match Path::parse(args.get(3).map_or(b".".as_slice(), Bytes::as_ref)) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let limit = self.reply_limit;
        let root = match self.json_value(&args[2], now) {
            Err(e) => return e,
            Ok(None) => return Resp::Bulk(None),
            Ok(Some(v)) => &v.root,
        };
        let mut work = Work::new();
        let paths = match path.resolve(root, &mut work) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let mut replies = Vec::new();
        let mut budget = ReplyBudget::new(limit);
        for p in paths {
            let v = path::at(root, &p).unwrap();
            if let Err(e) = work.charge(v.nodes) {
                return e;
            }
            let reply = Resp::Integer(memory(v) as i64);
            if let Err(e) = budget.response(&reply) {
                return e;
            }
            if !path.modern {
                return reply;
            }
            replies.push(reply);
        }
        if path.modern {
            array(replies)
        } else {
            Resp::Bulk(None)
        }
    }
}
fn memory(v: &Node) -> usize {
    let base = std::mem::size_of::<Node>() + 2 * std::mem::size_of::<usize>();
    base + match &v.kind {
        Kind::String(s) => s.capacity(),
        Kind::Number(n) => n.as_str().len(),
        Kind::Array(a) => {
            a.capacity() * std::mem::size_of::<V>() + a.iter().map(|v| memory(v)).sum::<usize>()
        }
        Kind::Object(o) => o
            .iter()
            .map(|(k, v)| {
                k.capacity()
                    + std::mem::size_of::<(String, V)>()
                    + 3 * std::mem::size_of::<usize>()
                    + memory(v)
            })
            .sum(),
        _ => 0,
    }
}

fn calculate(a: &Number, b: &Number, operation: &str) -> Result<V, Resp> {
    let integral = |n: &Number| !n.as_str().contains(['.', 'e', 'E']);
    if integral(a) && integral(b) {
        let (a, b) = (
            a.as_str()
                .parse::<i64>()
                .map_err(|_| error("ERR numeric overflow"))?,
            b.as_str()
                .parse::<i64>()
                .map_err(|_| error("ERR numeric overflow"))?,
        );
        let value = match operation {
            "add" => a.checked_add(b),
            "mul" => a.checked_mul(b),
            "pow" => {
                if b < 0 {
                    None
                } else if a == 0 {
                    Some(i64::from(b == 0))
                } else if a == 1 {
                    Some(1)
                } else if a == -1 {
                    Some(if b % 2 == 0 { 1 } else { -1 })
                } else {
                    u32::try_from(b).ok().and_then(|b| a.checked_pow(b))
                }
            }
            _ => None,
        }
        .ok_or_else(|| error("ERR numeric overflow"))?;
        return Ok(Node::new(Kind::Number(Number::from(value))));
    }
    let (a, b) = (
        a.as_f64().ok_or_else(invalid)?,
        b.as_f64().ok_or_else(invalid)?,
    );
    let value = match operation {
        "add" => a + b,
        "mul" => a * b,
        "pow" => a.powf(b),
        _ => return Err(invalid()),
    };
    if value.is_nan() {
        return Err(error("ERR result is not a number"));
    }
    node::number(value)
}
impl Engine {
    fn json_number(&mut self, args: &[Bytes], now: i64) -> Resp {
        let resp3 = self.command_version() == crate::protocol::Version::Resp3;
        let path = match Path::parse(&args[2]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let operand = match Node::parse(&args[3]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let Kind::Number(operand) = &operand.kind else {
            return invalid();
        };
        let operation = if args[0].eq_ignore_ascii_case(b"JSON.NUMINCRBY") {
            "add"
        } else if args[0].eq_ignore_ascii_case(b"JSON.NUMMULTBY") {
            "mul"
        } else {
            "pow"
        };
        let limit = self.reply_limit;
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return missing(),
            Ok(Some(v)) => &v.root,
        };
        let paths = match path.targets(root, &mut Work::new()) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let mut deltas = Vec::new();
        let mut values = Vec::new();
        let mut native = Vec::new();
        let mut last = None;
        for p in paths {
            let v = path::at(root, &p).unwrap();
            if let Kind::Number(n) = &v.kind {
                let value = match calculate(n, operand, operation) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                last = Some(value.clone());
                if resp3 {
                    let Kind::Number(n) = &value.kind else {
                        unreachable!()
                    };
                    native.push(if let Some(n) = n.as_i64() {
                        Resp::Integer(n)
                    } else {
                        Resp::Double(n.as_f64().unwrap())
                    });
                } else {
                    values.push(value.clone());
                }
                deltas.push(Delta {
                    path: p,
                    edit: Edit::Set(value),
                });
            } else if resp3 {
                native.push(Resp::Bulk(None));
            } else {
                values.push(Node::new(Kind::Null));
            }
        }
        let reply = if resp3 {
            array(native)
        } else {
            let mut writer = Writer::new(limit);
            let result = if path.modern {
                writer.values(&values, &Formatting::default(), 0)
            } else if let Some(value) = last {
                writer.value(&value, 0, &Formatting::default())
            } else {
                return path_type();
            };
            if let Err(e) = result {
                return e;
            }
            writer.reply()
        };
        self.json_commit(&args[1], deltas, reply, now)
    }
    fn json_arrindex(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() > 6 {
            return wrong_arity("json.arrindex");
        }
        let path = match Path::parse(&args[2]) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let needle = match Node::parse(&args[3]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let start = match args.get(4) {
            None => 0,
            Some(v) => match integer(v) {
                Some(v) => v,
                None => return integer_error(),
            },
        };
        let stop = match args.get(5) {
            None => 0,
            Some(v) => match integer(v) {
                Some(v) => v,
                None => return integer_error(),
            },
        };
        let limit = self.reply_limit;
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return missing(),
            Ok(Some(v)) => &v.root,
        };
        let mut work = Work::new();
        let paths = match path.resolve(root, &mut work) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let mut replies = Vec::new();
        for p in paths {
            let v = path::at(root, &p).unwrap();
            let reply = if let Kind::Array(a) = &v.kind {
                let len = a.len() as i128;
                let start = (if start < 0 {
                    len + start as i128
                } else {
                    start as i128
                })
                .clamp(0, (len - 1).max(0)) as usize;
                let stop = (if stop == 0 {
                    len
                } else if stop < 0 {
                    len + stop as i128
                } else {
                    stop as i128
                })
                .clamp(0, len) as usize;
                let mut found = None;
                if start < stop {
                    for (i, v) in a[start..stop].iter().enumerate() {
                        if let Err(e) = work.charge(1 + v.nodes + needle.nodes) {
                            return e;
                        }
                        if node::equal(v, &needle) {
                            found = Some(start + i);
                            break;
                        }
                    }
                }
                Resp::Integer(found.map_or(-1, |i| i as i64))
            } else {
                Resp::Bulk(None)
            };
            if !path.modern {
                return bounded(reply, limit).unwrap_or_else(|e| e);
            }
            replies.push(reply);
        }
        bounded(
            if path.modern {
                array(replies)
            } else {
                Resp::Bulk(None)
            },
            limit,
        )
        .unwrap_or_else(|e| e)
    }
    fn json_mutate(&mut self, args: &[Bytes], now: i64) -> Resp {
        let name = std::str::from_utf8(&args[0])
            .unwrap_or("")
            .to_ascii_lowercase();
        let (mut raw, mut index_arg, mut start_arg, mut stop_arg) =
            (b".".as_slice(), None, None, None);
        let mut values = Vec::new();
        let mut string = None;
        let mut pop_format = 0;
        match name.as_str() {
            "json.strappend" => {
                if !(3..=4).contains(&args.len()) {
                    return wrong_arity("json.strappend");
                }
                let value = if args.len() == 3 {
                    &args[2]
                } else {
                    raw = &args[2];
                    &args[3]
                };
                let value = match Node::parse(value) {
                    Ok(v) => v,
                    Err(e) => return e,
                };
                let Kind::String(s) = &value.kind else {
                    return path_type();
                };
                string = Some(s.clone());
            }
            "json.arrappend" => {
                raw = &args[2];
                for value in &args[3..] {
                    match Node::parse(value) {
                        Ok(v) => values.push(v),
                        Err(e) => return e,
                    }
                }
            }
            "json.arrinsert" => {
                raw = &args[2];
                index_arg = Some(&args[3]);
                for value in &args[4..] {
                    match Node::parse(value) {
                        Ok(v) => values.push(v),
                        Err(e) => return e,
                    }
                }
            }
            "json.arrtrim" => {
                raw = &args[2];
                start_arg = Some(&args[3]);
                stop_arg = Some(&args[4]);
            }
            "json.arrpop" => {
                let mut i = 2;
                if args
                    .get(i)
                    .is_some_and(|a| a.eq_ignore_ascii_case(b"FORMAT"))
                {
                    let Some(format) = args.get(i + 1) else {
                        return syntax_error();
                    };
                    pop_format = if format.eq_ignore_ascii_case(b"STRINGS") {
                        0
                    } else if format.eq_ignore_ascii_case(b"EXPAND1") {
                        1
                    } else if format.eq_ignore_ascii_case(b"EXPAND") {
                        2
                    } else {
                        return syntax_error();
                    };
                    i += 2;
                }
                if let Some(path) = args.get(i) {
                    raw = path;
                    i += 1;
                }
                if let Some(index) = args.get(i) {
                    index_arg = Some(index);
                    i += 1;
                }
                if i != args.len() {
                    return wrong_arity("json.arrpop");
                }
            }
            "json.clear" => {
                if args.len() > 3 {
                    return wrong_arity("json.clear");
                }
                if let Some(path) = args.get(2) {
                    raw = path;
                }
            }
            "json.toggle" => {
                raw = &args[2];
            }
            _ => unreachable!(),
        }
        let parse_int = |arg: Option<&Bytes>, default| -> Result<i64, Resp> {
            arg.map_or(Ok(default), |v| integer(v).ok_or_else(integer_error))
        };
        let index = match parse_int(index_arg, -1) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let start = match parse_int(start_arg, 0) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let stop = match parse_int(stop_arg, -1) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let path = match Path::parse(raw) {
            Ok(p) => p,
            Err(e) => return e,
        };
        if pop_format != 0 && !path.modern {
            return error("ERR FORMAT EXPAND requires a JSONPath");
        }
        if pop_format != 0 && self.command_version() != crate::protocol::Version::Resp3 {
            return error("ERR FORMAT EXPAND is only supported with RESP3");
        }
        let limit = self.reply_limit;
        let root = match self.json_value(&args[1], now) {
            Err(e) => return e,
            Ok(None) => return missing(),
            Ok(Some(v)) => &v.root,
        };
        let paths = match path.targets(root, &mut Work::new()) {
            Ok(p) => p,
            Err(e) => return e,
        };
        let mut deltas = Vec::new();
        let mut replies = Vec::new();
        let mut budget = ReplyBudget::new(limit);
        let mut last = None;
        let mut cleared = 0;
        for p in paths {
            let v = path::at(root, &p).unwrap();
            let mut edit = None;
            let reply = match (name.as_str(), &v.kind) {
                ("json.strappend", Kind::String(s)) => {
                    let suffix = string.as_ref().unwrap();
                    if !suffix.is_empty() {
                        edit = Some(Edit::Append(suffix.clone()));
                    }
                    Resp::Integer((s.len() + suffix.len()) as i64)
                }
                ("json.arrappend", Kind::Array(a)) => {
                    edit = Some(Edit::Splice {
                        start: a.len(),
                        delete: 0,
                        insert: values.clone(),
                    });
                    Resp::Integer((a.len() + values.len()) as i64)
                }
                ("json.arrinsert", Kind::Array(a)) => {
                    let i = if index < 0 {
                        a.len() as i128 + index as i128
                    } else {
                        index as i128
                    };
                    let Ok(i) = usize::try_from(i) else {
                        return error("ERR index out of bounds");
                    };
                    if i > a.len() {
                        return error("ERR index out of bounds");
                    }
                    edit = Some(Edit::Splice {
                        start: i,
                        delete: 0,
                        insert: values.clone(),
                    });
                    Resp::Integer((a.len() + values.len()) as i64)
                }
                ("json.arrpop", Kind::Array(a)) => {
                    if a.is_empty() {
                        Resp::Bulk(None)
                    } else {
                        let i = (if index < 0 {
                            a.len() as i128 + index as i128
                        } else {
                            index as i128
                        })
                        .clamp(0, a.len() as i128 - 1) as usize;
                        let reply = if pop_format == 0 {
                            if let Err(e) = ReplyBudget::new(limit).charge(1, a[i].bytes) {
                                return e;
                            }
                            bulk(a[i].json())
                        } else {
                            match expanded(
                                &a[i],
                                pop_format == 2,
                                &mut ReplyBudget::new(limit),
                                &Formatting::default(),
                            ) {
                                Ok(r) => r,
                                Err(e) => return e,
                            }
                        };
                        edit = Some(Edit::Splice {
                            start: i,
                            delete: 1,
                            insert: vec![],
                        });
                        reply
                    }
                }
                ("json.arrtrim", Kind::Array(a)) => {
                    let len = a.len() as i128;
                    let from = (if start < 0 {
                        len + start as i128
                    } else {
                        start as i128
                    })
                    .max(0);
                    let to = (if stop < 0 {
                        len + stop as i128
                    } else {
                        stop as i128
                    })
                    .min(len - 1);
                    let (from, to) = if from >= len || from > to {
                        (0, 0)
                    } else {
                        (from as usize, (to + 1) as usize)
                    };
                    // Two splices retain existing child Arcs and never serialize/copy
                    // retained values. Tail removal precedes prefix removal.
                    if to < a.len() {
                        deltas.push(Delta {
                            path: p.clone(),
                            edit: Edit::Splice {
                                start: to,
                                delete: a.len() - to,
                                insert: vec![],
                            },
                        });
                    }
                    if from > 0 {
                        deltas.push(Delta {
                            path: p.clone(),
                            edit: Edit::Splice {
                                start: 0,
                                delete: from,
                                insert: vec![],
                            },
                        });
                    }
                    Resp::Integer((to - from) as i64)
                }
                ("json.toggle", Kind::Bool(b)) => {
                    edit = Some(Edit::Set(Node::new(Kind::Bool(!b))));
                    Resp::Integer(i64::from(!b))
                }
                ("json.clear", Kind::Number(n)) => {
                    if n.as_f64() != Some(0.0) {
                        edit = Some(Edit::Set(Node::new(Kind::Number(Number::from(0)))));
                        cleared += 1;
                    }
                    Resp::Integer(0)
                }
                ("json.clear", Kind::Array(a)) => {
                    if !a.is_empty() {
                        edit = Some(Edit::Set(Node::new(Kind::Array(vec![]))));
                        cleared += 1;
                    }
                    Resp::Integer(0)
                }
                ("json.clear", Kind::Object(o)) => {
                    if !o.is_empty() {
                        edit = Some(Edit::Set(Node::new(
                            Kind::Object(indexmap::IndexMap::new()),
                        )));
                        cleared += 1;
                    }
                    Resp::Integer(0)
                }
                _ => Resp::Bulk(None),
            };
            if !matches!(reply, Resp::Bulk(None)) {
                last = Some(replies.len());
            }
            if let Err(e) = budget.response(&reply) {
                return e;
            }
            replies.push(reply);
            if let Some(edit) = edit {
                deltas.push(Delta { path: p, edit });
            }
        }
        let reply = if name == "json.clear" {
            Resp::Integer(cleared)
        } else if path.modern {
            array(replies)
        } else if let Some(last) = last {
            replies.swap_remove(last)
        } else if name == "json.arrpop" {
            Resp::Bulk(None)
        } else {
            return path_type();
        };
        self.json_commit(&args[1], deltas, reply, now)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|v| Bytes::copy_from_slice(v.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    fn get(engine: &mut Engine, key: &str, path: &str) -> Resp {
        run(engine, &["JSON.GET", key, path])
    }
    fn json(s: &str) -> Resp {
        text(s)
    }
    fn ints(values: &[i64]) -> Resp {
        array(values.iter().copied().map(Resp::Integer).collect())
    }
    fn assert_error(reply: Resp) {
        assert!(matches!(reply, Resp::Error(_)));
    }
    #[test]
    fn defaults_root_types_and_legacy_modern_shapes_are_distinct() {
        let mut engine = Engine::new();
        assert_eq!(
            run(
                &mut engine,
                &[
                    "JSON.SET",
                    "j",
                    "$",
                    "{\"z\":2,\"a\":1,\"nested\":{\"a\":3}}"
                ]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["JSON.GET", "j"]),
            json("{\"z\":2,\"a\":1,\"nested\":{\"a\":3}}")
        );
        assert_eq!(get(&mut engine, "j", "$..a"), json("[1,3]"));
        assert_eq!(get(&mut engine, "j", "..a"), json("1"));
        assert_eq!(
            run(&mut engine, &["JSON.GET", "j", "$.z", ".a"]),
            json("{\"$.z\":[2],\".a\":[1]}")
        );
        assert_eq!(
            run(&mut engine, &["JSON.GET", "j", ".z", ".a"]),
            json("{\".z\":2,\".a\":1}")
        );
        assert_eq!(
            run(&mut engine, &["JSON.TYPE", "j", "$.z"]),
            array(vec![text("integer")])
        );
        assert_eq!(run(&mut engine, &["TYPE", "j"]), Resp::Simple("ReJSON-RL"));
        for value in ["null", "true", "123", "1.25", "\"hello\"", "[]", "{}"] {
            assert_eq!(
                run(&mut engine, &["JSON.SET", "j", ".", value]),
                Resp::Simple("OK")
            );
            assert_eq!(get(&mut engine, "j", "."), json(value));
        }
    }
    #[test]
    fn ordered_wildcards_recursive_unions_slices_and_filters() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "JSON.SET",
                "j",
                "$",
                "{\"items\":[{\"n\":1,\"s\":\"alpha\"},{\"n\":2,\"s\":\"beta\"},{\"n\":3,\"s\":\"alpine\"},{\"n\":4}],\"threshold\":2,\"a.b\":{\"😀\":7}}",
            ],
        );
        assert_eq!(get(&mut engine, "j", "$.items[3,0,2].n"), json("[4,1,3]"));
        assert_eq!(get(&mut engine, "j", "$.items[::-2].n"), json("[4,2]"));
        assert_eq!(get(&mut engine, "j", "$.items[1:4:2].n"), json("[2,4]"));
        assert_eq!(get(&mut engine, "j", "$['a.b']['😀']"), json("[7]"));
        assert_eq!(
            get(
                &mut engine,
                "j",
                "$.items[?(@.n >= $.threshold && @.s =~ '^al')].n"
            ),
            json("[3]")
        );
        assert_eq!(get(&mut engine, "j", "$.items[?(!@.s)].n"), json("[4]"));
        assert_eq!(
            get(&mut engine, "j", "$.items[?(@.n in [1.0,3])].n"),
            json("[1,3]")
        );
        assert_eq!(
            get(&mut engine, "j", "$.items[?(@.n + 1 == 4)].n"),
            json("[3]")
        );
        assert_eq!(
            run(
                &mut engine,
                &["JSON.SET", "j", "$.items[?(@.n > 2)].new", "true"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "j", "$..new"), json("[true,true]"));
        for path in ["$[::0]", "$..", "$[?(@.n >)]", "$['unterminated]"] {
            assert_error(get(&mut engine, "j", path));
        }
    }
    #[test]
    fn repeated_read_selections_keep_order_and_projection_multiplicity() {
        let mut engine = Engine::new();
        run(&mut engine, &["JSON.SET", "j", "$", "[10,20]"]);
        assert_eq!(get(&mut engine, "j", "$[1,0,1]"), json("[20,10,20]"));
        assert_eq!(get(&mut engine, "j", "$[1,-1,0]"), json("[20,20,10]"));
        assert_eq!(
            run(&mut engine, &["JSON.MGET", "j", "missing", "j", "$[1,0,1]"]),
            array(vec![
                json("[20,10,20]"),
                Resp::Bulk(None),
                json("[20,10,20]")
            ])
        );
        assert_eq!(get(&mut engine, "j", "count($[1,0,1])"), json("[3]"));
        run(
            &mut engine,
            &[
                "JSON.SET",
                "nested",
                "$",
                r#"{"a":[{"n":1},{"n":2}],"b":3}"#,
            ],
        );
        assert_eq!(
            get(&mut engine, "nested", "$['a','a'][1,0,1].n"),
            json("[2,1,2,2,1,2]")
        );
        assert_eq!(
            get(&mut engine, "nested", "$['b','a','b']"),
            json(r#"[3,[{"n":1},{"n":2}],3]"#)
        );
    }
    #[test]
    fn repeated_mutation_targets_are_applied_once_in_first_selection_order() {
        let mut engine = Engine::new();
        run(&mut engine, &["JSON.SET", "j", "$", "[10,20,30]"]);
        assert_eq!(
            run(&mut engine, &["JSON.NUMINCRBY", "j", "$[1,0,1]", "1"]),
            json("[21,11]")
        );
        assert_eq!(get(&mut engine, "j", "$"), json("[[11,21,30]]"));
        assert_eq!(
            run(&mut engine, &["JSON.DEL", "j", "$[1,0,1]"]),
            Resp::Integer(2)
        );
        assert_eq!(get(&mut engine, "j", "$"), json("[[30]]"));
        run(
            &mut engine,
            &["JSON.SET", "j", "$", r#"{"s":"a","b":true,"a":[1]}"#],
        );
        assert_eq!(
            run(
                &mut engine,
                &["JSON.STRAPPEND", "j", "$['s','s']", r#""b""#]
            ),
            ints(&[2])
        );
        assert_eq!(
            run(&mut engine, &["JSON.TOGGLE", "j", "$['b','b']"]),
            ints(&[0])
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$['new','new']", "4"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$['new','new']", "5", "XX"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(
                &mut engine,
                &["JSON.MERGE", "j", "$['o','o']", r#"{"n":1}"#]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            get(&mut engine, "j", "$"),
            json(r#"[{"s":"ab","b":false,"a":[1],"new":5,"o":{"n":1}}]"#)
        );
    }
    #[test]
    fn current_projection_functions_are_read_only_and_chainable() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "JSON.SET",
                "j",
                "$",
                "{\"a\":2,\"b\":4,\"arr\":[1,2,3],\"obj\":{\"x\":1,\"y\":2},\"s\":\"café\"}",
            ],
        );
        for (path, expected) in [
            ("$.a + 1", "[3]"),
            ("($.a + $.b) / 2", "[3.0]"),
            ("$.a / 0", "[]"),
            ("$.arr.length()", "[3]"),
            ("count($.missing)", "[0]"),
            ("value($.a)", "[2]"),
            ("$.obj.keys()", "[\"x\",\"y\"]"),
            ("$.obj.keys().count()", "[2]"),
            ("$.obj~", "[\"x\",\"y\"]"),
            ("concat('a','b')", "[\"ab\"]"),
            ("$.s.length()", "[4]"),
            ("abs(-2)", "[2]"),
            ("ceiling(1.2)", "[2.0]"),
            ("floor(1.9)", "[1.0]"),
            ("first($.arr)", "[1]"),
            ("last($.arr)", "[3]"),
            ("index($.arr,-1)", "[3]"),
            ("sum($.arr)", "[6]"),
            ("min($.arr)", "[1]"),
            ("max($.arr)", "[3]"),
            ("avg($.arr)", "[2.0]"),
            ("$.arr.append(4,5)", "[1,2,3,4,5]"),
            ("match('ab','a|ab')", "[true]"),
            ("search('abc','b')", "[true]"),
        ] {
            assert_eq!(get(&mut engine, "j", path), json(expected), "{path}");
        }
        assert_eq!(get(&mut engine, "j", "$.arr"), json("[[1,2,3]]"));
        assert_error(run(&mut engine, &["JSON.SET", "j", "$.arr.length()", "4"]));
    }
    #[test]
    fn key_projections_share_an_allocation_budget_across_eager_arguments() {
        let mut engine = Engine::new();
        let key = "k".repeat(65_536);
        let document = format!("{{\"{key}\":0}}");
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$", &document]),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "j", "count($~)"), json("[1]"));
        assert_eq!(get(&mut engine, "j", "count(keys($))"), json("[1]"));
        for projection in ["$~", "keys($)"] {
            let args = std::iter::repeat_n(projection, 32)
                .collect::<Vec<_>>()
                .join(",");
            assert_error(get(&mut engine, "j", &format!("count(append({args}))")));
        }
        let args = std::iter::repeat_n("'x'", 16).collect::<Vec<_>>().join(",");
        let expression = format!("count(append({args}))");
        assert_eq!(get(&mut engine, "j", &expression), json("[16]"));
        assert_eq!(run(&mut engine, &["JSON.OBJLEN", "j", "$"]), ints(&[1]));
    }
    #[test]
    fn append_projection_charges_copied_receiver_slots_before_scalar_aggregation() {
        let mut engine = Engine::new();
        let count = MAX_WORK / std::mem::size_of::<V>() + 1;
        let document = format!(
            "[{}]",
            std::iter::repeat_n("0", count)
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$", &document]),
            Resp::Simple("OK")
        );
        assert_error(get(&mut engine, "j", "count(append($,0))"));
        assert_eq!(
            run(&mut engine, &["JSON.ARRLEN", "j", "$"]),
            ints(&[count as i64])
        );
    }
    #[test]
    fn wildcard_projection_checks_field_path_bytes_before_copying_keys() {
        let mut engine = Engine::new();
        let key = "k".repeat(MAX_WORK + 1);
        let document = format!("{{\"{key}\":0}}");
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$", &document]),
            Resp::Simple("OK")
        );
        assert_error(get(&mut engine, "j", "count($.*)"));
        assert_eq!(run(&mut engine, &["JSON.OBJLEN", "j", "$"]), ints(&[1]));
    }
    #[test]
    fn object_reads_distinguish_missing_keys_paths_and_wrong_path_types() {
        let mut engine = Engine::new();
        for command in ["JSON.OBJLEN", "JSON.OBJKEYS"] {
            assert_error(run(&mut engine, &[command, "absent"]));
            assert_error(run(&mut engine, &[command, "absent", "$"]));
            assert_error(run(&mut engine, &[command, "absent", ".missing"]));
        }
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$", "{\"n\":0}"]),
            Resp::Simple("OK")
        );
        for command in ["JSON.OBJLEN", "JSON.OBJKEYS"] {
            assert_eq!(
                run(&mut engine, &[command, "j", ".missing"]),
                Resp::Bulk(None)
            );
            assert_eq!(
                run(&mut engine, &[command, "j", "$.missing"]),
                array(vec![])
            );
            assert_error(run(&mut engine, &[command, "j", ".n"]));
            assert_eq!(
                run(&mut engine, &[command, "j", "$.n"]),
                array(vec![Resp::Bulk(None)])
            );
        }
        assert_eq!(
            run(&mut engine, &["JSON.TYPE", "absent", "$"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut engine, &["JSON.STRLEN", "absent", "$"]),
            Resp::Bulk(None)
        );
        assert_error(run(&mut engine, &["JSON.STRLEN", "j", ".missing"]));
    }
    #[test]
    fn conditional_sets_and_multikey_mset_validation_are_atomic() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["JSON.SET", "missing", "$", "1", "XX"]),
            Resp::Bulk(None)
        );
        assert_error(run(&mut engine, &["JSON.SET", "missing", "$.a", "1"]));
        run(&mut engine, &["JSON.SET", "a", "$", "{\"n\":1}"]);
        run(&mut engine, &["JSON.SET", "b", "$", "{\"n\":2}"]);
        assert_eq!(
            run(&mut engine, &["JSON.SET", "a", "$.n", "8", "NX"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "a", "$.new", "8", "XX"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "a", "$.new", "8", "NX"]),
            Resp::Simple("OK")
        );
        assert_error(run(
            &mut engine,
            &["JSON.MSET", "a", "$.n", "9", "b", "$.missing.child", "4"],
        ));
        assert_eq!(get(&mut engine, "a", "$.n"), json("[1]"));
        assert_error(run(
            &mut engine,
            &["JSON.MSET", "a", "$.n", "9", "b", "$.n", "1e999"],
        ));
        assert_eq!(get(&mut engine, "a", "$.n"), json("[1]"));
        assert_eq!(
            run(
                &mut engine,
                &[
                    "JSON.MSET",
                    "a",
                    "$",
                    "{\"parent\":{}}",
                    "a",
                    "$.parent.child",
                    "4",
                    "b",
                    "$.n",
                    "5"
                ]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "a", "$.parent.child"), json("[4]"));
        assert_eq!(
            run(&mut engine, &["JSON.MGET", "b", "absent", "a", "$.n"]),
            array(vec![json("[5]"), Resp::Bulk(None), json("[]")])
        );
        run(&mut engine, &["SET", "wrong", "ordinary"]);
        assert_error(run(
            &mut engine,
            &["JSON.MSET", "b", "$.n", "8", "wrong", "$", "3"],
        ));
        assert_eq!(get(&mut engine, "b", "$.n"), json("[5]"));
    }
    #[test]
    fn numeric_operations_preserve_integer_precision_and_reject_all_overflows_atomically() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "JSON.SET",
                "j",
                "$",
                "{\"big\":9007199254740993,\"max\":9223372036854775807,\"mixed\":[2,\"no\",3]}",
            ],
        );
        assert_eq!(
            run(&mut engine, &["JSON.NUMINCRBY", "j", "$.big", "1"]),
            json("[9007199254740994]")
        );
        assert_eq!(
            run(&mut engine, &["JSON.NUMMULTBY", "j", "$.mixed[*]", "3"]),
            json("[6,null,9]")
        );
        assert_eq!(
            run(&mut engine, &["JSON.NUMPOWBY", "j", "$.mixed[0]", "2"]),
            json("[36]")
        );
        for args in [
            vec!["JSON.NUMINCRBY", "j", "$.*", "1"],
            vec!["JSON.NUMPOWBY", "j", "$.big", "-1"],
            vec!["JSON.NUMMULTBY", "j", "$.big", "1e308"],
            vec!["JSON.NUMPOWBY", "j", "$.big", "0.5e999"],
        ] {
            assert_error(run(&mut engine, &args));
        }
        assert_eq!(get(&mut engine, "j", "$.big"), json("[9007199254740994]"));
        assert_eq!(
            run(&mut engine, &["JSON.NUMPOWBY", "j", "$.mixed[2]", "-1.0"]),
            json("[0.1111111111111111]")
        );
        engine.set_command_version(crate::protocol::Version::Resp3);
        assert_eq!(
            run(&mut engine, &["JSON.NUMINCRBY", "j", "$.mixed[0]", "1"]),
            ints(&[37])
        );
    }
    #[test]
    fn strings_arrays_objects_clear_toggle_and_merge_are_real_mutations() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "JSON.SET",
                "j",
                "$",
                "{\"s\":\"é\",\"a\":[1,2,3],\"b\":true,\"o\":{\"x\":1,\"y\":2}}",
            ],
        );
        assert_eq!(
            run(&mut engine, &["JSON.STRAPPEND", "j", "$.s", "\"😀\""]),
            ints(&[6])
        );
        assert_eq!(run(&mut engine, &["JSON.STRLEN", "j", "$.s"]), ints(&[6]));
        assert_eq!(
            run(&mut engine, &["JSON.ARRAPPEND", "j", "$.a", "4", "5"]),
            ints(&[5])
        );
        assert_eq!(
            run(&mut engine, &["JSON.ARRINSERT", "j", "$.a", "-1", "9"]),
            ints(&[6])
        );
        assert_eq!(
            run(&mut engine, &["JSON.ARRINDEX", "j", "$.a", "9"]),
            ints(&[4])
        );
        assert_eq!(
            run(&mut engine, &["JSON.ARRPOP", "j", "$.a", "999"]),
            array(vec![json("5")])
        );
        assert_eq!(
            run(&mut engine, &["JSON.ARRTRIM", "j", "$.a", "1", "3"]),
            ints(&[3])
        );
        assert_eq!(get(&mut engine, "j", "$.a"), json("[[2,3,4]]"));
        assert_eq!(run(&mut engine, &["JSON.ARRLEN", "j", "$.a"]), ints(&[3]));
        assert_eq!(run(&mut engine, &["JSON.TOGGLE", "j", "$.b"]), ints(&[0]));
        assert_eq!(run(&mut engine, &["JSON.OBJLEN", "j", "$.o"]), ints(&[2]));
        assert_eq!(
            run(&mut engine, &["JSON.OBJKEYS", "j", "$.o"]),
            array(vec![array(vec![text("x"), text("y")])])
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "JSON.MERGE",
                    "j",
                    "$.o",
                    "{\"x\":null,\"z\":{\"nil\":null,\"n\":7}}"
                ]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            get(&mut engine, "j", "$.o"),
            json("[{\"y\":2,\"z\":{\"n\":7}}]")
        );
        assert_eq!(
            run(&mut engine, &["JSON.CLEAR", "j", "$.a"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut engine, &["JSON.CLEAR", "j", "$.a"]),
            Resp::Integer(0)
        );
        assert_eq!(get(&mut engine, "j", "$.a"), json("[[]]"));
        assert_eq!(
            run(&mut engine, &["JSON.FORGET", "j", "$.o.z.n"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["JSON.DEL", "j", "$"]), Resp::Integer(1));
        assert_eq!(run(&mut engine, &["EXISTS", "j"]), Resp::Integer(0));
    }
    #[test]
    fn multi_array_errors_and_reply_limits_never_partially_mutate() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &["JSON.SET", "j", "$", "{\"a\":[1,2],\"b\":[]}"],
        );
        assert_error(run(&mut engine, &["JSON.ARRINSERT", "j", "$.*", "2", "3"]));
        assert_eq!(get(&mut engine, "j", "$.a"), json("[[1,2]]"));
        run(
            &mut engine,
            &["JSON.SET", "pop", "$", "[\"a long popped value\"]"],
        );
        let original = engine.reply_limit;
        engine.reply_limit = std::mem::size_of::<Resp>() + 1;
        assert_error(run(&mut engine, &["JSON.ARRPOP", "pop", "$"]));
        engine.reply_limit = original;
        assert_eq!(
            get(&mut engine, "pop", "."),
            json("[\"a long popped value\"]")
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "JSON.ARRTRIM",
                    "j",
                    "$.a",
                    "-9223372036854775808",
                    "9223372036854775807"
                ]
            ),
            ints(&[2])
        );
    }
    #[test]
    fn ttl_and_resolved_delta_recovery_survive_updates_and_snapshot() {
        let mut source = Engine::new();
        let mut recovered = Engine::new();
        source.set_recording(true);
        let padding = "x".repeat(8192);
        let value = format!("{{\"padding\":\"{padding}\",\"items\":[1,2],\"n\":7}}");
        for args in [
            vec!["JSON.SET", "j", "$", &value],
            vec!["PEXPIRE", "j", "5000"],
            vec!["JSON.NUMINCRBY", "j", "$.n", "1"],
            vec!["JSON.ARRAPPEND", "j", "$.items", "3"],
        ] {
            let reply = run(&mut source, &args);
            assert!(!matches!(reply, Resp::Error(_)));
            let batch = source.take_batch().unwrap();
            recovered.replay(batch).unwrap();
        }
        assert_eq!(get(&mut recovered, "j", "$.n"), json("[8]"));
        assert_eq!(get(&mut recovered, "j", "$.items"), json("[[1,2,3]]"));
        assert_eq!(run(&mut recovered, &["PTTL", "j"]), Resp::Integer(5000));
        let value = source
            .json_value(&Bytes::from_static(b"j"), 1000)
            .unwrap()
            .unwrap();
        let effects = value.snapshot().collect::<io::Result<Vec<_>>>().unwrap();
        let restored = JsonValue::restore(&effects[0]).unwrap();
        assert_eq!(restored.root.json(), value.root.json());
        let mut restored = restored;
        let invalid = encode(
            false,
            &[
                Delta {
                    path: vec![Part::Field("n".into())],
                    edit: Edit::Set(Node::new(Kind::Number(99.into()))),
                },
                Delta {
                    path: vec![Part::Field("absent".into())],
                    edit: Edit::Remove,
                },
            ],
        )
        .unwrap();
        assert!(restored.apply(&invalid).is_err());
        assert_eq!(restored.root.json(), value.root.json());
    }
    #[test]
    fn sibling_insert_byte_boundary_is_atomic_and_recorded_effects_replay_with_ttl() {
        let mut source = Engine::new();
        let mut recovered = Engine::new();
        source.set_recording(true);
        for args in [
            vec!["JSON.SET", "j", "$", "{}"],
            vec!["PEXPIRE", "j", "5000"],
        ] {
            assert!(!matches!(run(&mut source, &args), Resp::Error(_)));
            recovered.replay(source.take_batch().unwrap()).unwrap();
        }
        // Both new members need one shared comma, even though their original
        // parent is empty. The rejected document would be MAX_BYTES + 1.
        let too_large = format!("\"{}\"", "x".repeat(2_097_145));
        assert_error(run(
            &mut source,
            &["JSON.SET", "j", "$[\"a\",\"b\"]", &too_large],
        ));
        assert!(source.take_batch().is_none());
        assert_eq!(get(&mut source, "j", "."), json("{}"));
        assert_eq!(run(&mut source, &["PTTL", "j"]), Resp::Integer(5000));
        let boundary = format!("\"{}\"", "x".repeat(2_097_144));
        assert_eq!(
            run(&mut source, &["JSON.SET", "j", "$[\"a\",\"b\"]", &boundary]),
            Resp::Simple("OK")
        );
        recovered.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(
            run(&mut source, &["JSON.STRAPPEND", "j", "$.a", "\"x\""]),
            ints(&[2_097_145])
        );
        let batch = source.take_batch().unwrap();
        recovered.replay(batch).unwrap();
        for engine in [&mut source, &mut recovered] {
            let value = engine
                .json_value(&Bytes::from_static(b"j"), 1000)
                .unwrap()
                .unwrap();
            assert_eq!(value.root.bytes, MAX_BYTES);
            assert_eq!(value.root.bytes, value.root.json().len());
            assert_eq!(
                run(engine, &["JSON.STRLEN", "j", "$[\"a\",\"b\"]"]),
                ints(&[2_097_145, 2_097_144])
            );
            assert_eq!(run(engine, &["PTTL", "j"]), Resp::Integer(5000));
        }
        assert_error(run(&mut source, &["JSON.STRAPPEND", "j", "$.b", "\"x\""]));
        assert!(source.take_batch().is_none());
        assert_eq!(
            run(&mut source, &["JSON.STRLEN", "j", "$.b"]),
            ints(&[2_097_144])
        );
    }
    #[test]
    fn floating_point_array_options_and_resp_conversion() {
        let mut engine = Engine::new();
        assert_eq!(
            run(
                &mut engine,
                &["JSON.SET", "j", "$", "[1,2.5]", "FPHA", "FP16"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "j", "."), json("[1.0,2.5]"));
        assert_error(run(
            &mut engine,
            &["JSON.SET", "j", "$", "[70000]", "FPHA", "FP16"],
        ));
        assert_eq!(get(&mut engine, "j", "."), json("[1.0,2.5]"));
        run(
            &mut engine,
            &["JSON.SET", "j", "$", "{\"a\":[1,true,null]}"],
        );
        assert_eq!(
            run(&mut engine, &["JSON.RESP", "j", ".a"]),
            array(vec![
                Resp::Simple("["),
                Resp::Integer(1),
                Resp::Simple("true"),
                Resp::Bulk(None)
            ])
        );
        assert!(matches!(run(&mut engine,&["JSON.DEBUG","MEMORY","j"]),Resp::Integer(n) if n>0));
    }
    #[test]
    fn work_depth_and_document_limits_reject_before_mutating() {
        let mut engine = Engine::new();
        let document = format!(
            "[{}]",
            std::iter::repeat_n("1", 50_000)
                .collect::<Vec<_>>()
                .join(",")
        );
        assert_eq!(
            run(&mut engine, &["JSON.SET", "j", "$", &document]),
            Resp::Simple("OK")
        );
        assert_error(run(&mut engine, &["JSON.SET", "j", "$[*]", "2"]));
        assert_eq!(get(&mut engine, "j", "$[0,-1]"), json("[1,1]"));
        let deep = format!(
            "{}0{}",
            "[".repeat(MAX_DEPTH + 1),
            "]".repeat(MAX_DEPTH + 1)
        );
        assert_error(run(&mut engine, &["JSON.SET", "j", "$", &deep]));
        assert_eq!(get(&mut engine, "j", "$[0]"), json("[1]"));
        let huge = format!("\"{}\"", "x".repeat(MAX_BYTES));
        assert_error(run(&mut engine, &["JSON.SET", "j", "$", &huge]));
        assert_eq!(get(&mut engine, "j", "$[0]"), json("[1]"));
    }
    #[test]
    fn filter_set_relations_sizes_deep_equality_and_nothing_rules() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "JSON.SET",
                "j",
                "$",
                "[{\"tags\":[1,2],\"s\":\"é\"},{\"tags\":[],\"s\":\"ab\"},{\"tags\":[3],\"s\":\"\"}]",
            ],
        );
        for (path, expected) in [
            ("$[?(@.tags subsetof [1,2,3])].s", "[\"é\",\"ab\",\"\"]"),
            ("$[?(@.tags anyof [2])].s", "[\"é\"]"),
            ("$[?(@.tags noneof [2])].s", "[\"ab\",\"\"]"),
            ("$[?(@.s sizeof 1.9)].s", "[\"é\"]"),
            ("$[?(@.tags empty true)].s", "[\"ab\"]"),
            ("$[?(@.tags == [1.0,2])].s", "[\"é\"]"),
            ("$[?(@.tags in [[1,2],[3]])].s", "[\"é\",\"\"]"),
            ("$[?(@.s nin 1)].s", "[\"é\",\"ab\",\"\"]"),
            ("$[?(@.tags / 0 > 0)].s", "[]"),
        ] {
            assert_eq!(get(&mut engine, "j", path), json(expected), "{path}");
        }
        assert_eq!(get(&mut engine, "j", "stddev([1,1])"), json("[0.0]"));
        assert_eq!(get(&mut engine, "j", "sum([1,\"bad\"])"), json("[]"));
        assert_eq!(get(&mut engine, "j", "concat()"), json("[]"));
    }
    #[test]
    fn resp3_expanded_pop_rejects_resp2_without_losing_data() {
        let mut engine = Engine::new();
        run(&mut engine, &["JSON.SET", "j", "$", "[{\"n\":1},true]"]);
        assert_error(run(
            &mut engine,
            &["JSON.ARRPOP", "j", "FORMAT", "EXPAND", "$"],
        ));
        assert_eq!(get(&mut engine, "j", "."), json("[{\"n\":1},true]"));
        engine.set_command_version(crate::protocol::Version::Resp3);
        assert_eq!(
            run(&mut engine, &["JSON.ARRPOP", "j", "FORMAT", "EXPAND", "$"]),
            array(vec![Resp::Boolean(true)])
        );
        assert_eq!(
            run(&mut engine, &["JSON.TYPE", "j", "$"]),
            array(vec![array(vec![text("array")])])
        );
        assert_eq!(
            run(&mut engine, &["JSON.GET", "j", "FORMAT", "EXPAND", "$[0]"]),
            array(vec![array(vec![Resp::Map(vec![(
                text("n"),
                Resp::Integer(1)
            )])])])
        );
        assert_eq!(get(&mut engine, "j", "."), json("[{\"n\":1}]"));
    }
    #[test]
    fn bounded_bson_input_matches_json_input_and_rejects_invalid_frames() {
        let mut engine = Engine::new();
        let value = ::bson::to_vec(&::bson::doc! {"n":7_i64,"a":[1,2]}).unwrap();
        let command = |name: &'static [u8], payload: Vec<u8>| {
            vec![
                Bytes::from_static(name),
                Bytes::from_static(b"j"),
                Bytes::from_static(b"$"),
                Bytes::from(payload),
                Bytes::from_static(b"FORMAT"),
                Bytes::from_static(b"BSON"),
            ]
        };
        assert_eq!(
            engine.execute_at(&command(b"JSON.SET", value.clone()), 1000),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "j", "."), json("{\"n\":7,\"a\":[1,2]}"));
        let patch = ::bson::to_vec(&::bson::doc! {"n":8_i64}).unwrap();
        assert_eq!(
            engine.execute_at(&command(b"JSON.MERGE", patch), 1000),
            Resp::Simple("OK")
        );
        assert_eq!(get(&mut engine, "j", "$.n"), json("[8]"));
        for payload in [
            value[..value.len() - 1].to_vec(),
            ::bson::to_vec(&::bson::doc! {"n":f64::NAN}).unwrap(),
        ] {
            assert_error(engine.execute_at(&command(b"JSON.SET", payload), 1000));
            assert_eq!(get(&mut engine, "j", "$.n"), json("[8]"));
        }
        let mut nested = vec![5, 0, 0, 0, 0];
        for _ in 0..MAX_DEPTH + 1 {
            let size = (nested.len() + 8) as i32;
            let mut outer = size.to_le_bytes().to_vec();
            outer.extend_from_slice(&[3, b'k', 0]);
            outer.extend_from_slice(&nested);
            outer.push(0);
            nested = outer;
        }
        assert_error(engine.execute_at(&command(b"JSON.SET", nested), 1000));
        assert_eq!(get(&mut engine, "j", "$.n"), json("[8]"));
    }
}
