//! Sparse unsigned-index arrays. Journal effects contain resolved positions, never commands.
use std::collections::{BTreeMap, BTreeSet};
use std::io;

use bytes::{Bytes, BytesMut};
use regex::bytes::{Regex, RegexBuilder};

use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{
    CommandSpec, Engine, Mutation, ReplyBudget, Resp, Value, error, integer,
    syntax_error as syntax, wrong_arity, wrong_type,
};

const KIND: u8 = 1;
const MAX_INDEX: u64 = u64::MAX - 1;
const CHUNK: usize = 1024 * 1024;
const RANGE_LIMIT: u64 = 1_000_000;
const SLICE_SIZE: u64 = 4096;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "arcount",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler(
        "ardel",
        -3,
        &["write", "fast"],
        (1, 1, 1),
        Engine::array_delete,
    ),
    CommandSpec::handler(
        "ardelrange",
        -4,
        &["write"],
        (1, 1, 1),
        Engine::array_delete,
    ),
    CommandSpec::handler(
        "arget",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler(
        "argetrange",
        4,
        &["readonly"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler("argrep", -6, &["readonly"], (1, 1, 1), Engine::array_grep),
    CommandSpec::handler("arinfo", -2, &["readonly"], (1, 1, 1), Engine::array_info).max(3),
    CommandSpec::handler(
        "arinsert",
        -3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::array_write,
    ),
    CommandSpec::handler(
        "arlastitems",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::array_last,
    )
    .max(4),
    CommandSpec::handler(
        "arlen",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler(
        "armget",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler(
        "armset",
        -4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::array_write,
    ),
    CommandSpec::handler(
        "arnext",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::array_read,
    ),
    CommandSpec::handler("arop", -5, &["readonly"], (1, 1, 1), Engine::array_op).max(6),
    CommandSpec::handler(
        "arring",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::array_write,
    ),
    CommandSpec::handler("arscan", -4, &["readonly"], (1, 1, 1), Engine::array_read).max(6),
    CommandSpec::handler(
        "arseek",
        3,
        &["write", "fast"],
        (1, 1, 1),
        Engine::array_write,
    ),
    CommandSpec::handler(
        "arset",
        -4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::array_write,
    ),
];

#[derive(Default)]
pub(super) struct ArrayValue {
    items: BTreeMap<u64, Bytes>,
    // Actual sparse buckets, used for honest ARINFO storage statistics.
    slices: BTreeMap<u64, usize>,
    next: u64,
    ring: u64,
    pending: Option<Fragment>,
}
struct Fragment {
    index: u64,
    total: usize,
    bytes: BytesMut,
}

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid array effect")
}
fn unsigned(value: u64) -> Resp {
    if value <= i64::MAX as u64 {
        Resp::Integer(value as i64)
    } else {
        Resp::Unsigned(value)
    }
}
fn index(value: &[u8], terminal: bool) -> Result<u64, Resp> {
    if value.is_empty()
        || (value.len() > 1 && value[0] == b'0')
        || !value.iter().all(u8::is_ascii_digit)
    {
        return Err(error("ERR invalid array index"));
    }
    let n = std::str::from_utf8(value)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| terminal || *n <= MAX_INDEX)
        .ok_or_else(|| error("ERR invalid array index"))?;
    Ok(n)
}
fn positive(value: &[u8]) -> Result<usize, Resp> {
    integer(value)
        .filter(|n| *n > 0)
        .and_then(|n| usize::try_from(n).ok())
        .ok_or_else(|| error("ERR value must be a positive integer"))
}
fn bulk(value: Option<&Bytes>) -> Resp {
    Resp::Bulk(value.cloned())
}
fn pair(index: u64, value: &Bytes) -> Resp {
    Resp::Array(Some(vec![unsigned(index), bulk(Some(value))]))
}

impl ArrayValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{sampled, tree_bytes};
        tree_bytes::<(u64, Bytes)>(self.items.len())
            .saturating_add(tree_bytes::<(u64, usize)>(self.slices.len()))
            .saturating_add(sampled(self.items.values(), samples, Bytes::len))
            .saturating_add(
                self.pending
                    .as_ref()
                    .map_or(0, |part| part.bytes.capacity()),
            )
    }

    fn len(&self) -> u64 {
        self.items.last_key_value().map_or(0, |(i, _)| i + 1)
    }
    fn put(&mut self, index: u64, value: Bytes) {
        if self.items.insert(index, value).is_none() {
            *self.slices.entry(index / SLICE_SIZE).or_default() += 1;
        }
    }
    fn delete(&mut self, index: u64) {
        if self.items.remove(&index).is_some() {
            let slice = index / SLICE_SIZE;
            let count = self.slices.get_mut(&slice).expect("occupied slice");
            *count -= 1;
            if *count == 0 {
                self.slices.remove(&slice);
            }
        }
    }
    fn delete_range(&mut self, start: u64, end: u64) {
        let indices: Vec<_> = self.items.range(start..=end).map(|(i, _)| *i).collect();
        for i in indices {
            self.delete(i);
        }
    }
    fn clear(&mut self) {
        self.items.clear();
        self.slices.clear();
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, _) = Decoder::new(effect, 1)?;
        if op != 0 {
            return Err(invalid());
        }
        let mut result = Self::default();
        result.apply(effect)?;
        Ok(result)
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        match op {
            0 => {
                let clear = match d.u8()? {
                    0 => false,
                    1 => true,
                    _ => return Err(invalid()),
                };
                let next = d.u64()?;
                let ring = d.u64()?;
                let count = d.u32()?;
                let mut ranges = Vec::new();
                for _ in 0..count {
                    let a = d.u64()?;
                    let b = d.u64()?;
                    if a > b || b > MAX_INDEX {
                        return Err(invalid());
                    }
                    ranges.push((a, b));
                }
                d.finish()?;
                if self.pending.is_some() {
                    return Err(invalid());
                }
                if clear {
                    self.clear();
                }
                for (a, b) in ranges {
                    self.delete_range(a, b);
                }
                self.next = next;
                self.ring = ring;
            }
            1 => {
                let count = d.u32()?;
                let mut rows = Vec::new();
                for _ in 0..count {
                    let i = d.u64()?;
                    if i > MAX_INDEX {
                        return Err(invalid());
                    }
                    rows.push((i, Bytes::copy_from_slice(d.bytes()?)));
                }
                d.finish()?;
                if self.pending.is_some() {
                    return Err(invalid());
                }
                for (i, v) in rows {
                    self.put(i, v);
                }
            }
            2 => {
                let i = d.u64()?;
                let total = usize::try_from(d.u64()?).map_err(|_| invalid())?;
                let offset = usize::try_from(d.u64()?).map_err(|_| invalid())?;
                let chunk = d.bytes()?;
                d.finish()?;
                let end = offset
                    .checked_add(chunk.len())
                    .filter(|e| *e <= total)
                    .ok_or_else(invalid)?;
                if i > MAX_INDEX || total == 0 || chunk.is_empty() {
                    return Err(invalid());
                }
                if offset == 0 {
                    if self.pending.is_some() {
                        return Err(invalid());
                    }
                    self.pending = Some(Fragment {
                        index: i,
                        total,
                        bytes: BytesMut::new(),
                    });
                } else if self
                    .pending
                    .as_ref()
                    .is_none_or(|p| p.index != i || p.total != total || p.bytes.len() != offset)
                {
                    return Err(invalid());
                }
                let pending = self.pending.as_mut().expect("validated fragment");
                pending.bytes.extend_from_slice(chunk);
                if end == total {
                    let complete = self.pending.take().unwrap();
                    self.put(i, complete.bytes.freeze());
                }
            }
            _ => return Err(invalid()),
        }
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            value: self,
            initialized: false,
            rows: self.items.iter(),
            fragment: None,
        }
    }
}

fn metadata(clear: bool, next: u64, ring: u64, ranges: &[(u64, u64)]) -> io::Result<Bytes> {
    let mut e = Encoder::new(1, 0);
    e.u8(u8::from(clear))?;
    e.u64(next)?;
    e.u64(ring)?;
    e.u32(u32::try_from(ranges.len()).map_err(|_| invalid())?)?;
    for (a, b) in ranges {
        e.u64(*a)?;
        e.u64(*b)?;
    }
    Ok(e.finish())
}
fn row_effect(rows: &[(u64, Bytes)]) -> io::Result<Bytes> {
    let mut e = Encoder::new(1, 1);
    e.u32(u32::try_from(rows.len()).map_err(|_| invalid())?)?;
    for (i, v) in rows {
        e.u64(*i)?;
        e.bytes(v)?;
    }
    Ok(e.finish())
}
fn fragment_effect(i: u64, value: &[u8], offset: usize) -> io::Result<Bytes> {
    let mut e = Encoder::new(1, 2);
    e.u64(i)?;
    e.u64(value.len() as u64)?;
    e.u64(offset as u64)?;
    e.bytes(&value[offset..value.len().min(offset + CHUNK)])?;
    Ok(e.finish())
}

pub(super) struct Snapshot<'a> {
    value: &'a ArrayValue,
    initialized: bool,
    rows: std::collections::btree_map::Iter<'a, u64, Bytes>,
    fragment: Option<(u64, &'a Bytes, usize)>,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if !self.initialized {
            self.initialized = true;
            return Some(metadata(false, self.value.next, self.value.ring, &[]));
        }
        if let Some((i, v, offset)) = self.fragment.take() {
            if offset + CHUNK < v.len() {
                self.fragment = Some((i, v, offset + CHUNK));
            }
            return Some(fragment_effect(i, v, offset));
        }
        let (i, v) = self.rows.next()?;
        if v.len() > CHUNK {
            self.fragment = Some((*i, v, CHUNK));
            Some(fragment_effect(*i, v, 0))
        } else {
            Some(row_effect(&[(*i, v.clone())]))
        }
    }
}

struct Plan {
    clear: bool,
    next: u64,
    ring: u64,
    ranges: Vec<(u64, u64)>,
    rows: Vec<(u64, Bytes)>,
}
impl Plan {
    fn new(array: Option<&ArrayValue>) -> Self {
        Self {
            clear: false,
            next: array.map_or(0, |a| a.next),
            ring: array.map_or(0, |a| a.ring),
            ranges: Vec::new(),
            rows: Vec::new(),
        }
    }
    fn effects(&self) -> io::Result<Vec<Bytes>> {
        let mut effects = Vec::new();
        if self.ranges.is_empty() {
            effects.push(metadata(self.clear, self.next, self.ring, &[])?);
        } else {
            for (n, ranges) in self.ranges.chunks(4096).enumerate() {
                effects.push(metadata(
                    self.clear && n == 0,
                    self.next,
                    self.ring,
                    ranges,
                )?);
            }
        }
        let mut start = 0;
        while start < self.rows.len() {
            let (i, v) = &self.rows[start];
            if v.len() > CHUNK {
                for offset in (0..v.len()).step_by(CHUNK) {
                    effects.push(fragment_effect(*i, v, offset)?);
                }
                start += 1;
            } else {
                let mut end = start;
                let mut bytes = 6;
                while end < self.rows.len()
                    && self.rows[end].1.len() <= CHUNK
                    && (end == start || bytes + 12 + self.rows[end].1.len() <= CHUNK)
                {
                    bytes += 12 + self.rows[end].1.len();
                    end += 1;
                }
                effects.push(row_effect(&self.rows[start..end])?);
                start = end;
            }
        }
        Ok(effects)
    }
    fn apply(self, array: &mut ArrayValue) {
        if self.clear {
            array.clear();
        }
        for (a, b) in self.ranges {
            array.delete_range(a, b);
        }
        for (i, v) in self.rows {
            array.put(i, v);
        }
        array.next = self.next;
        array.ring = self.ring;
    }
}

impl Engine {
    fn array_value(&mut self, key: &Bytes, now: i64) -> Result<Option<&ArrayValue>, Resp> {
        match self.value(key, now) {
            Some(Value::Typed(data)) => match data.as_ref() {
                Data::Array(a) => Ok(Some(a)),
                _ => Err(wrong_type()),
            },
            Some(_) => Err(wrong_type()),
            None => Ok(None),
        }
    }
    fn array_commit(&mut self, key: &Bytes, now: i64, plan: Plan) -> Result<(), Resp> {
        let effects = if self.journal_enabled {
            match plan.effects() {
                Ok(e) => e,
                Err(e) => return Err(self.fail_io(e)),
            }
        } else {
            Vec::new()
        };
        let Value::Typed(data) = self.collection_entry(key, now, || {
            Value::Typed(Box::new(Data::Array(ArrayValue::default())))
        }) else {
            unreachable!("array validated")
        };
        let Data::Array(array) = data.as_mut() else {
            unreachable!("array validated")
        };
        plan.apply(array);
        for effect in effects {
            self.record_typed(key, KIND, effect, now);
        }
        Ok(())
    }
    fn array_write(&mut self, args: &[Bytes], now: i64) -> Resp {
        let cmd = args[0].as_ref();
        let set = cmd.eq_ignore_ascii_case(b"ARSET");
        let mset = cmd.eq_ignore_ascii_case(b"ARMSET");
        let seek = cmd.eq_ignore_ascii_case(b"ARSEEK");
        let ring = cmd.eq_ignore_ascii_case(b"ARRING");
        if mset && !args.len().is_multiple_of(2) {
            return wrong_arity("armset");
        }
        let position = if ring {
            match integer(&args[2]) {
                Some(n) if n > 0 => n as u64,
                Some(_) => return error("ERR size must be positive"),
                None => return error("ERR invalid size"),
            }
        } else if set || seek {
            match index(&args[2], seek) {
                Ok(i) => i,
                Err(e) => return e,
            }
        } else {
            0
        };
        let mut explicit = Vec::new();
        if mset {
            for p in args[2..].as_chunks::<2>().0 {
                let i = match index(&p[0], false) {
                    Ok(i) => i,
                    Err(e) => return e,
                };
                explicit.push((i, p[1].clone()));
            }
        }
        let array = match self.array_value(&args[1], now) {
            Ok(a) => a,
            Err(e) => return e,
        };
        if seek && array.is_none() {
            return Resp::Integer(0);
        }
        let mut plan = Plan::new(array);
        let result;
        if seek {
            if plan.next == position {
                return Resp::Integer(1);
            }
            plan.next = position;
            result = Resp::Integer(1);
        } else if mset {
            let mut seen = BTreeSet::new();
            let mut count = 0;
            for (i, _) in &explicit {
                if seen.insert(*i) && array.is_none_or(|a| !a.items.contains_key(i)) {
                    count += 1;
                }
            }
            plan.rows = explicit;
            result = Resp::Integer(count);
        } else if ring {
            let old_len = array.map_or(0, ArrayValue::len);
            let mut cursor = plan.next;
            // Resize preserves the recent contiguous tail. A hole terminates
            // the retained run; ordinary recent reads, unlike resize, keep holes.
            let repack =
                old_len > position || (old_len < position && cursor != 0 && cursor < old_len);
            if repack {
                let array = array.expect("nonempty resize");
                let mut retained = Vec::new();
                let mut i = if cursor == 0 {
                    old_len - 1
                } else {
                    (cursor - 1) % old_len
                };
                for _ in 0..old_len.min(position) {
                    let Some(v) = array.items.get(&i) else {
                        break;
                    };
                    retained.push(v.clone());
                    i = if i == 0 { old_len - 1 } else { i - 1 };
                }
                retained.reverse();
                cursor = retained.len() as u64;
                plan.clear = true;
                plan.rows
                    .extend(retained.into_iter().enumerate().map(|(i, v)| (i as u64, v)));
            }
            let mut last = 0;
            for v in &args[3..] {
                last = cursor % position;
                plan.rows.push((last, v.clone()));
                cursor = last + 1;
            }
            plan.next = cursor;
            plan.ring = position;
            result = unsigned(last);
        } else {
            let start = if set { position } else { plan.next };
            let values = &args[if set { 3 } else { 2 }..];
            let Some(last) = start
                .checked_add(values.len() as u64 - 1)
                .filter(|i| *i <= MAX_INDEX)
            else {
                return error("ERR insert index overflow");
            };
            let mut count = 0;
            for (offset, v) in values.iter().enumerate() {
                let i = start + offset as u64;
                if array.is_none_or(|a| !a.items.contains_key(&i)) {
                    count += 1;
                }
                plan.rows.push((i, v.clone()));
            }
            if set {
                result = Resp::Integer(count);
            } else {
                plan.next = last + 1;
                plan.ring = 0;
                result = unsigned(last);
            }
        }
        match self.array_commit(&args[1], now, plan) {
            Ok(()) => result,
            Err(e) => e,
        }
    }
    fn array_delete(&mut self, args: &[Bytes], now: i64) -> Resp {
        let ranges = args[0].eq_ignore_ascii_case(b"ARDELRANGE");
        if ranges && !args.len().is_multiple_of(2) {
            return wrong_arity("ardelrange");
        }
        let mut parsed = Vec::new();
        for arg in &args[2..] {
            match index(arg, false) {
                Ok(i) => parsed.push(i),
                Err(e) => return e,
            }
        }
        let mut requested: Vec<_> = if ranges {
            parsed
                .as_chunks::<2>()
                .0
                .iter()
                .map(|p| (p[0].min(p[1]), p[0].max(p[1])))
                .collect()
        } else {
            parsed.into_iter().map(|i| (i, i)).collect()
        };
        requested.sort_unstable();
        let mut merged: Vec<(u64, u64)> = Vec::new();
        for (a, b) in requested {
            if let Some(last) = merged
                .last_mut()
                .filter(|last| a <= last.1.saturating_add(1))
            {
                last.1 = last.1.max(b);
            } else {
                merged.push((a, b));
            }
        }
        let array = match self.array_value(&args[1], now) {
            Ok(Some(a)) => a,
            Ok(None) => return Resp::Integer(0),
            Err(e) => return e,
        };
        let count: usize = merged
            .iter()
            .map(|(a, b)| array.items.range(*a..=*b).count())
            .sum();
        if count == 0 {
            return Resp::Integer(0);
        }
        if count == array.items.len() {
            self.remove(&args[1]);
            self.record(now, || Mutation::Delete {
                key: args[1].clone(),
            });
        } else {
            let mut plan = Plan::new(Some(array));
            plan.ranges = merged;
            if let Err(e) = self.array_commit(&args[1], now, plan) {
                return e;
            }
        }
        Resp::Integer(count as i64)
    }
    fn array_read(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.array_read_result(args, now) {
            Ok(r) => r,
            Err(e) => e,
        }
    }
    fn array_read_result(&mut self, args: &[Bytes], now: i64) -> Result<Resp, Resp> {
        let cmd = args[0].as_ref();
        let get = cmd.eq_ignore_ascii_case(b"ARGET");
        let mget = cmd.eq_ignore_ascii_case(b"ARMGET");
        let range = cmd.eq_ignore_ascii_case(b"ARGETRANGE");
        let scan = cmd.eq_ignore_ascii_case(b"ARSCAN");
        let mut indices = Vec::new();
        if get || mget || range || scan {
            for arg in &args[2..if scan { 4 } else { args.len() }] {
                indices.push(index(arg, false)?);
            }
        }
        let mut limit = usize::MAX;
        if scan && args.len() > 4 {
            if args.len() != 6 || !args[4].eq_ignore_ascii_case(b"LIMIT") {
                return Err(syntax());
            }
            limit = positive(&args[5])?;
        }
        let mut budget = ReplyBudget::new(self.reply_limit);
        if range {
            let span = indices[0]
                .abs_diff(indices[1])
                .checked_add(1)
                .ok_or_else(|| error("ERR range exceeds maximum of 1000000 items"))?;
            if span > RANGE_LIMIT {
                return Err(error("ERR range exceeds maximum of 1000000 items"));
            }
            budget.charge(span as usize, 0)?;
        }
        let array = self.array_value(&args[1], now)?;
        if get {
            let value = array.and_then(|a| a.items.get(&indices[0]));
            budget.charge(1, value.map_or(0, Bytes::len))?;
            return Ok(bulk(value));
        }
        if mget {
            budget.charge(indices.len(), 0)?;
            let mut rows = Vec::new();
            for i in indices {
                let v = array.and_then(|a| a.items.get(&i));
                budget.charge(0, v.map_or(0, Bytes::len))?;
                rows.push(bulk(v));
            }
            return Ok(Resp::Array(Some(rows)));
        }
        if range {
            let (a, b) = (indices[0], indices[1]);
            let mut rows = Vec::new();
            for offset in 0..=a.abs_diff(b) {
                let i = if a <= b { a + offset } else { a - offset };
                let v = array.and_then(|a| a.items.get(&i));
                budget.charge(0, v.map_or(0, Bytes::len))?;
                rows.push(bulk(v));
            }
            return Ok(Resp::Array(Some(rows)));
        }
        if scan {
            let mut rows = Vec::new();
            if let Some(array) = array {
                let (a, b) = (indices[0], indices[1]);
                let mut iter = array.items.range(a.min(b)..=a.max(b));
                while rows.len() < limit {
                    let Some((i, v)) = (if a <= b {
                        iter.next()
                    } else {
                        iter.next_back()
                    }) else {
                        break;
                    };
                    budget.charge(3, v.len() + 20)?;
                    rows.push(pair(*i, v));
                }
            }
            return Ok(Resp::Array(Some(rows)));
        }
        if cmd.eq_ignore_ascii_case(b"ARNEXT") {
            return Ok(match array.map_or(0, |a| a.next) {
                u64::MAX => Resp::Bulk(None),
                i => unsigned(i),
            });
        }
        Ok(unsigned(if cmd.eq_ignore_ascii_case(b"ARLEN") {
            array.map_or(0, ArrayValue::len)
        } else {
            array.map_or(0, |a| a.items.len() as u64)
        }))
    }
    fn array_last(&mut self, args: &[Bytes], now: i64) -> Resp {
        let count = match integer(&args[2]) {
            Some(n) => n,
            None => return super::integer_error(),
        };
        if count <= 0 {
            return Resp::Array(Some(Vec::new()));
        }
        let rev = args.len() == 4;
        if rev && !args[3].eq_ignore_ascii_case(b"REV") {
            return syntax();
        }
        let mut budget = ReplyBudget::new(self.reply_limit);
        let array = match self.array_value(&args[1], now) {
            Ok(Some(a)) => a,
            Ok(None) => return Resp::Array(Some(Vec::new())),
            Err(e) => return e,
        };
        let len = array.len();
        if len == 0 {
            return Resp::Array(Some(Vec::new()));
        }
        let count = (count as u64).min(len);
        if count > RANGE_LIMIT && count > array.items.len() as u64 {
            return error("ERR recent range exceeds maximum sparse walk");
        }
        if let Err(e) = budget.charge(count as usize, 0) {
            return e;
        }
        let anchor = if array.next == 0 || array.next > len {
            len - 1
        } else {
            array.next - 1
        };
        let mut rows = Vec::new();
        for offset in 0..count {
            let i = if offset <= anchor {
                anchor - offset
            } else {
                len - (offset - anchor)
            };
            let value = array.items.get(&i);
            if let Err(e) = budget.charge(0, value.map_or(0, Bytes::len)) {
                return e;
            }
            rows.push(bulk(value));
        }
        if !rev {
            rows.reverse();
        }
        Resp::Array(Some(rows))
    }
    fn array_info(&mut self, args: &[Bytes], now: i64) -> Resp {
        let full = args.len() == 3;
        if full && !args[2].eq_ignore_ascii_case(b"FULL") {
            return syntax();
        }
        let array = match self.array_value(&args[1], now) {
            Ok(Some(a)) => a,
            Ok(None) => return error("ERR no such key"),
            Err(e) => return e,
        };
        let slices = array.slices.len() as u64;
        let mut fields = vec![
            ("count", unsigned(array.items.len() as u64)),
            ("len", unsigned(array.len())),
            (
                "next-insert-index",
                unsigned(if array.next == u64::MAX {
                    0
                } else {
                    array.next
                }),
            ),
            ("slices", unsigned(slices)),
            ("directory-size", unsigned(slices)),
            ("super-dir-entries", Resp::Integer(0)),
            ("slice-size", unsigned(SLICE_SIZE)),
        ];
        if full {
            fields.extend([
                ("dense-slices", Resp::Integer(0)),
                ("sparse-slices", unsigned(slices)),
                ("avg-dense-size", Resp::Double(0.0)),
                ("avg-dense-fill", Resp::Double(0.0)),
                (
                    "avg-sparse-size",
                    Resp::Double(if slices == 0 {
                        0.0
                    } else {
                        array.items.len() as f64 / slices as f64
                    }),
                ),
            ]);
        }
        Resp::Map(
            fields
                .into_iter()
                .map(|(k, v)| (Resp::Bulk(Some(Bytes::from_static(k.as_bytes()))), v))
                .collect(),
        )
    }
    fn array_op(&mut self, args: &[Bytes], now: i64) -> Resp {
        let start = match index(&args[2], false) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let end = match index(&args[3], false) {
            Ok(i) => i,
            Err(e) => return e,
        };
        let op = args[4].as_ref();
        let matching = op.eq_ignore_ascii_case(b"MATCH");
        let used = op.eq_ignore_ascii_case(b"USED");
        let sum = op.eq_ignore_ascii_case(b"SUM");
        let min = op.eq_ignore_ascii_case(b"MIN");
        let max = op.eq_ignore_ascii_case(b"MAX");
        let and = op.eq_ignore_ascii_case(b"AND");
        let or = op.eq_ignore_ascii_case(b"OR");
        let xor = op.eq_ignore_ascii_case(b"XOR");
        if (!matching && args.len() != 5)
            || (matching && args.len() != 6)
            || !(matching || used || sum || min || max || and || or || xor)
        {
            return syntax();
        }
        let array = match self.array_value(&args[1], now) {
            Ok(a) => a,
            Err(e) => return e,
        };
        let mut count = 0u64;
        let mut number: Option<ArrayNumber> = None;
        let mut bits: Option<i64> = None;
        if let Some(array) = array {
            for (_, v) in array.items.range(start.min(end)..=start.max(end)) {
                if used || (matching && v == &args[5]) {
                    count += 1;
                }
                if matching || used {
                    continue;
                }
                let n = if let Some(value) = integer(v) {
                    ArrayNumber::Integer(value as i128)
                } else if let Some(value) = numeric(v) {
                    ArrayNumber::Float(value)
                } else {
                    continue;
                };
                if sum || min || max {
                    number = Some(match number {
                        None => n,
                        Some(old) if sum => old.add(n),
                        Some(old)
                            if (min && n.compare(old).is_lt())
                                || (max && n.compare(old).is_gt()) =>
                        {
                            n
                        }
                        Some(old) => old,
                    });
                } else {
                    let n = match n {
                        ArrayNumber::Integer(value) => value as i64,
                        ArrayNumber::Float(value)
                            if value >= i64::MIN as f64 && value < 9_223_372_036_854_775_808.0 =>
                        {
                            value.trunc() as i64
                        }
                        _ => continue,
                    };
                    bits = Some(match bits {
                        None => n,
                        Some(old) if and => old & n,
                        Some(old) if or => old | n,
                        Some(old) => old ^ n,
                    });
                }
            }
        }
        if used || matching {
            unsigned(count)
        } else if sum || min || max {
            Resp::Bulk(number.map(ArrayNumber::bytes))
        } else {
            bits.map_or(Resp::Bulk(None), Resp::Integer)
        }
    }
}

#[derive(Clone, Copy)]
enum ArrayNumber {
    Integer(i128),
    Float(f64),
}
impl ArrayNumber {
    fn float(self) -> f64 {
        match self {
            Self::Integer(n) => n as f64,
            Self::Float(n) => n,
        }
    }
    fn add(self, other: Self) -> Self {
        match (self, other) {
            // At most usize::MAX signed-i64 inputs fit in an i128 accumulator.
            (Self::Integer(a), Self::Integer(b)) => Self::Integer(a + b),
            (a, b) => Self::Float(a.float() + b.float()),
        }
    }
    fn compare(self, other: Self) -> std::cmp::Ordering {
        match (self, other) {
            (Self::Integer(a), Self::Integer(b)) => a.cmp(&b),
            (Self::Float(a), Self::Float(b)) => a.total_cmp(&b),
            (Self::Integer(a), Self::Float(b)) => {
                if b >= 9_223_372_036_854_775_808.0 {
                    return std::cmp::Ordering::Less;
                }
                if b < i64::MIN as f64 {
                    return std::cmp::Ordering::Greater;
                }
                a.cmp(&(b as i128))
                    .then_with(|| 0.0_f64.total_cmp(&b.fract()))
            }
            (Self::Float(a), Self::Integer(b)) => {
                Self::Integer(b).compare(Self::Float(a)).reverse()
            }
        }
    }
    fn bytes(self) -> Bytes {
        Bytes::from(match self {
            Self::Integer(n) => n.to_string(),
            Self::Float(n) => n.to_string(),
        })
    }
}

fn numeric(value: &[u8]) -> Option<f64> {
    if value.is_empty()
        || value.first()?.is_ascii_whitespace()
        || value.last()?.is_ascii_whitespace()
    {
        return None;
    }
    let n = std::str::from_utf8(value).ok()?.parse::<f64>().ok()?;
    if n.is_nan() {
        return None;
    }
    if n.is_infinite()
        && ![
            b"inf".as_slice(),
            b"+inf",
            b"-inf",
            b"infinity",
            b"+infinity",
            b"-infinity",
        ]
        .iter()
        .any(|v| value.eq_ignore_ascii_case(v))
    {
        return None;
    }
    if n == 0.0
        && value
            .split(|b| *b == b'e' || *b == b'E')
            .next()?
            .iter()
            .any(|b| (b'1'..=b'9').contains(b))
    {
        return None;
    }
    Some(n)
}

enum Predicate {
    Exact(Bytes),
    Contains(Bytes),
    Glob(Bytes),
    Regex(Regex),
}
impl Predicate {
    fn matches(&self, value: &[u8]) -> bool {
        match self {
            Self::Exact(s) => value == s.as_ref(),
            Self::Contains(s) => {
                s.is_empty() || value.windows(s.len()).any(|part| part == s.as_ref())
            }
            Self::Glob(p) => super::scan::glob_matches(p, value),
            Self::Regex(re) => re.is_match(value),
        }
    }
}
impl Engine {
    fn array_grep(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.array_grep_result(args, now) {
            Ok(r) => r,
            Err(e) => e,
        }
    }
    fn array_grep_result(&mut self, args: &[Bytes], now: i64) -> Result<Resp, Resp> {
        let start = if args[2].as_ref() == b"-" {
            0
        } else if args[2].as_ref() == b"+" {
            MAX_INDEX
        } else {
            index(&args[2], false)?
        };
        let end = if args[3].as_ref() == b"+" {
            MAX_INDEX
        } else if args[3].as_ref() == b"-" {
            0
        } else {
            index(&args[3], false)?
        };
        let mut raw = Vec::new();
        let mut and = false;
        let mut nocase = false;
        let mut values = false;
        let mut limit = usize::MAX;
        let mut p = 4;
        while p < args.len() {
            let arg = args[p].as_ref();
            if [b"EXACT".as_slice(), b"MATCH", b"GLOB", b"RE"]
                .iter()
                .any(|name| arg.eq_ignore_ascii_case(name))
            {
                let pattern = args.get(p + 1).ok_or_else(syntax)?;
                if raw.len() == 250 {
                    return Err(error("ERR too many predicates (maximum 250)"));
                }
                raw.push((arg, pattern));
                p += 2;
            } else if arg.eq_ignore_ascii_case(b"AND") {
                and = true;
                p += 1;
            } else if arg.eq_ignore_ascii_case(b"OR") {
                and = false;
                p += 1;
            } else if arg.eq_ignore_ascii_case(b"NOCASE") {
                nocase = true;
                p += 1;
            } else if arg.eq_ignore_ascii_case(b"WITHVALUES") {
                values = true;
                p += 1;
            } else if arg.eq_ignore_ascii_case(b"LIMIT") {
                limit = positive(args.get(p + 1).ok_or_else(syntax)?)?;
                p += 2;
            } else {
                return Err(syntax());
            }
        }
        if raw.is_empty() {
            return Err(syntax());
        }
        let mut predicates = Vec::new();
        for (kind, pattern) in raw {
            if kind.eq_ignore_ascii_case(b"RE") {
                if pattern.is_empty() || pattern.len() > 2048 {
                    return Err(error("ERR regular expression must contain 1 to 2048 bytes"));
                }
                let mut text = String::with_capacity(pattern.len());
                for byte in pattern.iter().copied() {
                    if byte.is_ascii() {
                        text.push(byte as char);
                    } else {
                        use std::fmt::Write;
                        write!(&mut text, "\\x{byte:02x}").expect("writing a string");
                    }
                }
                // Thompson-NFA/DFA regex engine: no backtracking or backreferences,
                // bounded compile memory, O(pattern * input) search even on hostile input.
                let re = RegexBuilder::new(&text)
                    .unicode(false)
                    .case_insensitive(nocase)
                    .size_limit(2 * 1024 * 1024)
                    .dfa_size_limit(2 * 1024 * 1024)
                    .build()
                    .map_err(|_| {
                        error("ERR invalid regular expression (backreferences are unsupported)")
                    })?;
                predicates.push(Predicate::Regex(re));
            } else {
                let pattern = if nocase {
                    Bytes::from(pattern.to_ascii_lowercase())
                } else {
                    pattern.clone()
                };
                predicates.push(if kind.eq_ignore_ascii_case(b"EXACT") {
                    Predicate::Exact(pattern)
                } else if kind.eq_ignore_ascii_case(b"MATCH") {
                    Predicate::Contains(pattern)
                } else {
                    Predicate::Glob(pattern)
                });
            }
        }
        let mut budget = ReplyBudget::new(self.reply_limit);
        let array = self.array_value(&args[1], now)?;
        let mut rows = Vec::new();
        if let Some(array) = array {
            let start = if args[2].as_ref() == b"+" {
                array.len().saturating_sub(1)
            } else {
                start
            };
            let end = if args[3].as_ref() == b"+" {
                array.len().saturating_sub(1)
            } else {
                end
            };
            let mut iter = array.items.range(start.min(end)..=start.max(end));
            let needs_fold = nocase && predicates.iter().any(|p| !matches!(p, Predicate::Regex(_)));
            let mut folded = Vec::new();
            while let Some((i, v)) = if start <= end {
                iter.next()
            } else {
                iter.next_back()
            } {
                let candidate = if needs_fold {
                    folded.clear();
                    folded.extend(v.iter().map(u8::to_ascii_lowercase));
                    folded.as_slice()
                } else {
                    v.as_ref()
                };
                let matched = if and {
                    predicates.iter().all(|p| p.matches(candidate))
                } else {
                    predicates.iter().any(|p| p.matches(candidate))
                };
                if matched {
                    budget.charge(
                        if values { 3 } else { 1 },
                        if values { v.len() + 20 } else { 20 },
                    )?;
                    rows.push(if values { pair(*i, v) } else { unsigned(*i) });
                    if rows.len() == limit {
                        break;
                    }
                }
            }
        }
        Ok(Resp::Array(Some(rows)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            now,
        )
    }
    fn values(items: &[Option<&str>]) -> Resp {
        Resp::Array(Some(
            items
                .iter()
                .map(|s| Resp::Bulk(s.map(|s| Bytes::copy_from_slice(s.as_bytes()))))
                .collect(),
        ))
    }
    fn indices(items: &[u64]) -> Resp {
        Resp::Array(Some(items.iter().copied().map(unsigned).collect()))
    }
    fn assert_error(reply: Resp) {
        assert!(matches!(reply, Resp::Error(_)), "{reply:?}");
    }

    #[test]
    fn unsigned_sparse_boundaries_and_contiguous_overflow_are_atomic() {
        let mut e = Engine::new();
        assert_eq!(
            run(
                &mut e,
                1000,
                &["ARSET", "a", "18446744073709551614", "tail"]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut e, 1000, &["ARLEN", "a"]), Resp::Unsigned(u64::MAX));
        assert_eq!(run(&mut e, 1000, &["ARCOUNT", "a"]), Resp::Integer(1));
        assert_eq!(
            e.array_value(&Bytes::from_static(b"a"), 1000)
                .unwrap()
                .unwrap()
                .items
                .len(),
            1
        );
        assert_eq!(
            run(&mut e, 1000, &["PEXPIRE", "a", "10000"]),
            Resp::Integer(1)
        );
        assert_error(run(
            &mut e,
            1001,
            &["ARSET", "a", "18446744073709551614", "bad", "overflow"],
        ));
        assert_error(run(
            &mut e,
            1001,
            &["ARMSET", "a", "0", "partial", "18446744073709551615", "bad"],
        ));
        assert_eq!(
            run(&mut e, 1001, &["ARMGET", "a", "0", "18446744073709551614"]),
            values(&[None, Some("tail")])
        );
        assert_eq!(run(&mut e, 1001, &["PTTL", "a"]), Resp::Integer(9999));
        assert_eq!(
            run(&mut e, 1001, &["ARSEEK", "a", "18446744073709551614"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut e, 1001, &["ARINSERT", "a", "last"]),
            Resp::Unsigned(MAX_INDEX)
        );
        assert_eq!(run(&mut e, 1001, &["ARNEXT", "a"]), Resp::Bulk(None));
        assert_error(run(&mut e, 1001, &["ARINSERT", "a", "overflow"]));
        assert_eq!(
            run(&mut e, 1001, &["ARGET", "a", "18446744073709551614"]),
            Resp::Bulk(Some(Bytes::from_static(b"last")))
        );
        assert_error(run(
            &mut e,
            1001,
            &["ARGET", "missing", "18446744073709551615"],
        ));
        assert_error(run(&mut e, 1001, &["ARGET", "missing", "-1"]));
        assert_eq!(
            run(&mut e, 1001, &["ARSEEK", "missing", "18446744073709551615"]),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut e, 1001, &["EXISTS", "missing"]), Resp::Integer(0));
    }

    #[test]
    fn ranges_preserve_holes_reverse_order_and_missing_key_positions() {
        let mut e = Engine::new();
        assert_eq!(
            run(
                &mut e,
                0,
                &["ARMSET", "a", "2", "two", "5", "five", "2", "last"]
            ),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "a", "5", "2"]),
            values(&[Some("five"), None, None, Some("last")])
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "missing", "8", "6"]),
            values(&[None, None, None])
        );
        assert_eq!(
            run(&mut e, 0, &["ARMGET", "a", "5", "2", "5", "0"]),
            values(&[Some("five"), Some("last"), Some("five"), None])
        );
        assert_eq!(
            run(&mut e, 0, &["ARSCAN", "a", "100", "0", "LIMIT", "1"]),
            Resp::Array(Some(vec![pair(5, &Bytes::from_static(b"five"))]))
        );
        assert_error(run(&mut e, 0, &["ARGETRANGE", "missing", "0", "1000000"]));
        assert_error(run(
            &mut e,
            0,
            &["ARSCAN", "missing", "0", "1", "LIMIT", "0"],
        ));
        assert_error(run(&mut e, 0, &["ARSCAN", "missing", "0", "1", "unknown"]));
    }

    #[test]
    fn overlapping_deletion_counts_once_updates_length_and_removes_empty_key() {
        let mut e = Engine::new();
        run(&mut e, 0, &["ARSET", "a", "0", "a", "b", "c", "d", "e"]);
        assert_eq!(
            run(&mut e, 0, &["ARDELRANGE", "a", "3", "1", "2", "4"]),
            Resp::Integer(4)
        );
        assert_eq!(run(&mut e, 0, &["ARLEN", "a"]), Resp::Integer(1));
        assert_eq!(
            run(&mut e, 0, &["ARDEL", "a", "0", "0", "9"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut e, 0, &["EXISTS", "a"]), Resp::Integer(0));
        run(&mut e, 0, &["ARSET", "a", "7", "safe"]);
        assert_error(run(&mut e, 0, &["ARDELRANGE", "a", "0", "9", "bad", "10"]));
        assert_eq!(
            run(&mut e, 0, &["ARGET", "a", "7"]),
            Resp::Bulk(Some(Bytes::from_static(b"safe")))
        );
    }

    #[test]
    fn ring_wrap_resize_holes_and_explicit_cursor_override() {
        let mut e = Engine::new();
        assert_eq!(
            run(
                &mut e,
                0,
                &[
                    "ARRING", "r", "5", "v0", "v1", "v2", "v3", "v4", "v5", "v6", "v7"
                ]
            ),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut e, 0, &["ARLASTITEMS", "r", "5"]),
            values(&[Some("v3"), Some("v4"), Some("v5"), Some("v6"), Some("v7")])
        );
        assert_eq!(
            run(&mut e, 0, &["ARRING", "r", "8", "grown"]),
            Resp::Integer(5)
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "r", "0", "5"]),
            values(&[
                Some("v3"),
                Some("v4"),
                Some("v5"),
                Some("v6"),
                Some("v7"),
                Some("grown")
            ])
        );
        assert_eq!(
            run(&mut e, 0, &["ARRING", "r", "3", "small"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "r", "0", "2"]),
            values(&[Some("small"), Some("v7"), Some("grown")])
        );
        run(&mut e, 0, &["ARSEEK", "r", "0"]);
        assert_eq!(
            run(&mut e, 0, &["ARRING", "r", "9", "override"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "r", "0", "2"]),
            values(&[Some("override"), Some("v7"), Some("grown")])
        );
        run(
            &mut e,
            0,
            &["ARRING", "holes", "5", "a", "b", "c", "d", "e"],
        );
        run(&mut e, 0, &["ARDEL", "holes", "3"]);
        assert_eq!(
            run(&mut e, 0, &["ARRING", "holes", "3", "new"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut e, 0, &["ARGETRANGE", "holes", "0", "2"]),
            values(&[Some("e"), Some("new"), None])
        );
        assert_error(run(&mut e, 0, &["ARRING", "holes", "0", "bad"]));
        assert_eq!(run(&mut e, 0, &["ARNEXT", "holes"]), Resp::Integer(2));
    }

    #[test]
    fn recent_walk_preserves_holes_and_clamps_cursor_to_surviving_tail() {
        let mut e = Engine::new();
        run(&mut e, 0, &["ARINSERT", "a", "a", "b", "c", "d", "e"]);
        run(&mut e, 0, &["ARDEL", "a", "1"]);
        assert_eq!(
            run(&mut e, 0, &["ARLASTITEMS", "a", "10"]),
            values(&[Some("a"), None, Some("c"), Some("d"), Some("e")])
        );
        assert_eq!(
            run(&mut e, 0, &["ARLASTITEMS", "a", "10", "REV"]),
            values(&[Some("e"), Some("d"), Some("c"), None, Some("a")])
        );
        run(&mut e, 0, &["ARSEEK", "a", "100"]);
        assert_eq!(
            run(&mut e, 0, &["ARLASTITEMS", "a", "2"]),
            values(&[Some("d"), Some("e")])
        );
        run(&mut e, 0, &["ARSET", "sparse", "10000000", "tail"]);
        assert_error(run(&mut e, 0, &["ARLASTITEMS", "sparse", "1000001"]));
    }

    #[test]
    fn aggregates_ignore_ineligible_values_and_truncate_bitwise_floats() {
        let mut e = Engine::new();
        run(
            &mut e,
            0,
            &[
                "ARMSET", "a", "0", "7.9", "1", "3.2", "2", "1.8", "3", "text", "8", "",
            ],
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "8", "0", "USED"]),
            Resp::Integer(5)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "8", "AND"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "8", "OR"]),
            Resp::Integer(7)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "8", "XOR"]),
            Resp::Integer(5)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "8", "MIN"]),
            Resp::Bulk(Some(Bytes::from_static(b"1.8")))
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "8", "MATCH", ""]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "3", "8", "SUM"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "missing", "0", "8", "USED"]),
            Resp::Integer(0)
        );
        assert_error(run(&mut e, 0, &["AROP", "missing", "0", "8", "MATCH"]));
        run(
            &mut e,
            0,
            &[
                "ARSET",
                "limits",
                "0",
                "9223372036854775807",
                "9223372036854775808",
            ],
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "limits", "0", "1", "OR"]),
            Resp::Integer(i64::MAX)
        );
    }

    #[test]
    fn integer_aggregates_preserve_precision_beyond_binary64() {
        let mut e = Engine::new();
        run(
            &mut e,
            0,
            &["ARSET", "a", "0", "9007199254740993", "-9007199254740992"],
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "1", "SUM"]),
            bulk(Some(&Bytes::from_static(b"1")))
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "1", "MAX"]),
            bulk(Some(&Bytes::from_static(b"9007199254740993")))
        );
        run(&mut e, 0, &["ARSET", "a", "1", "9007199254740992.0"]);
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "1", "MIN"]),
            bulk(Some(&Bytes::from_static(b"9007199254740992")))
        );
        run(
            &mut e,
            0,
            &[
                "ARSET",
                "a",
                "0",
                "9223372036854775807",
                "9223372036854775807",
            ],
        );
        assert_eq!(
            run(&mut e, 0, &["AROP", "a", "0", "1", "SUM"]),
            bulk(Some(&Bytes::from_static(b"18446744073709551614")))
        );
    }

    #[test]
    fn predicates_combine_all_modes_and_validate_even_for_missing_keys() {
        let mut e = Engine::new();
        run(
            &mut e,
            0,
            &[
                "ARSET",
                "a",
                "0",
                "boot: ok",
                "warn: disk",
                "ERROR: cpu",
                "info: ready",
                "error: net",
            ],
        );
        assert_eq!(
            run(
                &mut e,
                0,
                &["ARGREP", "a", "-", "+", "MATCH", "error", "NOCASE"]
            ),
            indices(&[2, 4])
        );
        assert_eq!(
            run(
                &mut e,
                0,
                &[
                    "ARGREP",
                    "a",
                    "+",
                    "-",
                    "GLOB",
                    "error:*",
                    "NOCASE",
                    "RE",
                    "(cpu|net)$",
                    "AND",
                    "LIMIT",
                    "1"
                ]
            ),
            indices(&[4])
        );
        assert_eq!(
            run(
                &mut e,
                0,
                &[
                    "ARGREP",
                    "a",
                    "0",
                    "4",
                    "EXACT",
                    "info: ready",
                    "MATCH",
                    "disk"
                ]
            ),
            indices(&[1, 3])
        );
        assert_eq!(
            run(
                &mut e,
                0,
                &[
                    "ARGREP",
                    "a",
                    "0",
                    "4",
                    "MATCH",
                    "ERROR",
                    "WITHVALUES",
                    "LIMIT",
                    "2"
                ]
            ),
            Resp::Array(Some(vec![pair(2, &Bytes::from_static(b"ERROR: cpu"))]))
        );
        assert_error(run(&mut e, 0, &["ARGREP", "missing", "-", "+", "RE", "("]));
        assert_error(run(
            &mut e,
            0,
            &["ARGREP", "missing", "-", "+", "RE", "(a)\\1"],
        ));
        assert_error(run(&mut e, 0, &["ARGREP", "missing", "-", "+", "RE", ""]));
        assert_error(run(
            &mut e,
            0,
            &["ARGREP", "missing", "-", "+", "MATCH", "a", "LIMIT", "0"],
        ));
        let mut too_many = vec![
            Bytes::from_static(b"ARGREP"),
            Bytes::from_static(b"missing"),
            Bytes::from_static(b"-"),
            Bytes::from_static(b"+"),
        ];
        for _ in 0..251 {
            too_many.extend([Bytes::from_static(b"EXACT"), Bytes::new()]);
        }
        assert_error(e.execute_at(&too_many, 0));
    }

    #[test]
    fn binary_values_patterns_and_adversarial_regex_are_supported() {
        let mut e = Engine::new();
        let value = Bytes::from_static(b"\xff\0Ab\r\n");
        assert_eq!(
            e.execute_at(
                &[
                    Bytes::from_static(b"ARSET"),
                    Bytes::from_static(b"binary"),
                    Bytes::from_static(b"9"),
                    value.clone()
                ],
                0
            ),
            Resp::Integer(1)
        );
        assert_eq!(
            e.execute_at(
                &[
                    Bytes::from_static(b"ARGREP"),
                    Bytes::from_static(b"binary"),
                    Bytes::from_static(b"-"),
                    Bytes::from_static(b"+"),
                    Bytes::from_static(b"EXACT"),
                    Bytes::from_static(b"\xff\0ab\r\n"),
                    Bytes::from_static(b"NOCASE")
                ],
                0
            ),
            indices(&[9])
        );
        assert_eq!(
            e.execute_at(
                &[
                    Bytes::from_static(b"ARGREP"),
                    Bytes::from_static(b"binary"),
                    Bytes::from_static(b"-"),
                    Bytes::from_static(b"+"),
                    Bytes::from_static(b"RE"),
                    Bytes::from_static(b"^\xff\\x00[A-Z]b")
                ],
                0
            ),
            indices(&[9])
        );
        e.execute_at(
            &[
                Bytes::from_static(b"ARSET"),
                Bytes::from_static(b"long"),
                Bytes::from_static(b"0"),
                Bytes::from(vec![b'a'; 100_000]),
            ],
            0,
        );
        assert_eq!(
            run(&mut e, 0, &["ARGREP", "long", "-", "+", "RE", "^(a+)+b$"]),
            indices(&[])
        );
    }

    #[test]
    fn snapshot_and_incremental_effects_preserve_large_rows_cursor_and_ring() {
        let mut a = ArrayValue {
            next: u64::MAX,
            ring: 17,
            ..ArrayValue::default()
        };
        a.put(MAX_INDEX, Bytes::from(vec![0xff; 3 * CHUNK + 7]));
        a.put(3, Bytes::from_static(b"\0binary"));
        let effects: Vec<_> = a.snapshot().map(Result::unwrap).collect();
        assert!(effects.iter().all(|effect| effect.len() <= CHUNK + 64));
        let mut recovered = ArrayValue::restore(&effects[0]).unwrap();
        for effect in &effects[1..] {
            recovered.apply(effect).unwrap();
        }
        assert_eq!(recovered.items, a.items);
        assert_eq!(recovered.slices, a.slices);
        assert_eq!((recovered.next, recovered.ring), (u64::MAX, 17));
        assert!(recovered.pending.is_none());
        let mut plan = Plan::new(Some(&a));
        plan.ranges.push((3, 3));
        plan.next = 5;
        plan.rows.push((100, Bytes::from_static(b"delta")));
        for effect in plan.effects().unwrap() {
            recovered.apply(&effect).unwrap();
        }
        plan.apply(&mut a);
        assert_eq!(recovered.items, a.items);
        assert_eq!((recovered.next, recovered.ring), (a.next, a.ring));
    }

    #[test]
    fn malformed_effects_and_fragments_never_partially_modify_state() {
        let mut a = ArrayValue::default();
        a.put(4, Bytes::from_static(b"safe"));
        let mut bad = metadata(true, 100, 2, &[(4, 4)]).unwrap().to_vec();
        bad.push(0);
        assert!(a.apply(&bad).is_err());
        assert_eq!(a.items.get(&4).unwrap().as_ref(), b"safe");
        assert_eq!(a.next, 0);
        let row = row_effect(&[
            (5, Bytes::from_static(b"first")),
            (6, Bytes::from_static(b"second")),
        ])
        .unwrap();
        assert!(a.apply(&row[..row.len() - 1]).is_err());
        assert!(!a.items.contains_key(&5));
        let large = vec![0x42; CHUNK + 3];
        a.apply(&fragment_effect(9, &large, 0).unwrap()).unwrap();
        let offset = a.pending.as_ref().unwrap().bytes.len();
        let mut bad = fragment_effect(9, &large, CHUNK).unwrap().to_vec();
        bad.push(1);
        assert!(a.apply(&bad).is_err());
        assert_eq!(a.pending.as_ref().unwrap().bytes.len(), offset);
        assert!(!a.items.contains_key(&9));
        a.apply(&fragment_effect(9, &large, CHUNK).unwrap())
            .unwrap();
        assert_eq!(a.items.get(&9).unwrap().as_ref(), large.as_slice());
    }

    #[test]
    fn journal_replay_preserves_resize_cursor_and_ttl_and_queries_are_bounded() {
        let mut source = Engine::new();
        source.journal_enabled = true;
        let mut recovered = Engine::new();
        for args in [
            vec!["ARRING", "r", "3", "a", "b", "c", "d"],
            vec!["PEXPIRE", "r", "20000"],
            vec!["ARRING", "r", "5", "e"],
            vec!["ARSEEK", "r", "1"],
            vec!["ARINSERT", "r", "replacement"],
            vec!["ARDEL", "r", "0"],
        ] {
            assert!(!matches!(run(&mut source, 1000, &args), Resp::Error(_)));
            if let Some(batch) = source.take_batch() {
                recovered.replay(batch).unwrap();
            }
        }
        for args in [
            vec!["ARSCAN", "r", "0", "100"],
            vec!["ARLEN", "r"],
            vec!["ARCOUNT", "r"],
            vec!["ARNEXT", "r"],
            vec!["ARLASTITEMS", "r", "10"],
            vec!["PTTL", "r"],
        ] {
            assert_eq!(
                run(&mut source, 1001, &args),
                run(&mut recovered, 1001, &args)
            );
        }
        source.reply_limit = 1;
        assert_error(run(&mut source, 1001, &["ARGETRANGE", "r", "0", "100"]));
        source.reply_limit = usize::MAX;
        assert_eq!(run(&mut source, 1001, &["PTTL", "r"]), Resp::Integer(19999));
        assert_eq!(
            run(&mut source, 1001, &["ARCOUNT", "r"]),
            run(&mut recovered, 1001, &["ARCOUNT", "r"])
        );
    }
}

#[cfg(test)]
mod metadata_tests {
    use super::*;

    #[test]
    fn info_reports_real_sparse_buckets_and_terminal_cursor_without_dense_fabrication() {
        let mut e = Engine::new();
        let args = |words: &[&str]| {
            words
                .iter()
                .map(|w| Bytes::copy_from_slice(w.as_bytes()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            e.execute_at(&args(&["ARMSET", "a", "1", "first", "8192", "second"]), 0),
            Resp::Integer(2)
        );
        e.execute_at(&args(&["ARSEEK", "a", "18446744073709551615"]), 0);
        let Resp::Map(fields) = e.execute_at(&args(&["ARINFO", "a", "FULL"]), 0) else {
            panic!("expected map");
        };
        let field = |name: &[u8]| {
            &fields
                .iter()
                .find(|(key, _)| matches!(key,Resp::Bulk(Some(key)) if key.as_ref() == name))
                .unwrap()
                .1
        };
        assert_eq!(field(b"count"), &Resp::Integer(2));
        assert_eq!(field(b"len"), &Resp::Integer(8193));
        assert_eq!(field(b"next-insert-index"), &Resp::Integer(0));
        assert_eq!(field(b"slices"), &Resp::Integer(2));
        assert_eq!(field(b"dense-slices"), &Resp::Integer(0));
        assert_eq!(field(b"sparse-slices"), &Resp::Integer(2));
        assert_eq!(field(b"avg-sparse-size"), &Resp::Double(1.0));
        assert!(matches!(
            e.execute_at(&args(&["ARINFO", "missing"]), 0),
            Resp::Error(_)
        ));
        assert!(matches!(
            e.execute_at(
                &args(&["ARRING", "missing", "9223372036854775808", "bad"]),
                0
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            e.execute_at(&args(&["EXISTS", "missing"]), 0),
            Resp::Integer(0)
        );
    }
}
