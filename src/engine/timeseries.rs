// Redis TimeSeries command semantics:
// https://redis.io/docs/latest/develop/data-types/timeseries/
// https://redis.io/docs/latest/commands/ts.range/
// https://redis.io/docs/latest/commands/ts.createrule/
// Chunks bound sample rewrites; rule accumulators retain the open bucket even
// when source retention removes its raw samples. Journal effects store resolved
// sample/policy decisions and independent rule-state deltas, never series images.
use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{CommandSpec, Engine, Entry, Resp, Value, array, bulk, error, integer, wrong_type};
use bytes::Bytes;
use std::collections::{BTreeMap, BTreeSet};
use std::io;

const KIND: u8 = 4;
const MISSING: &str = "TSDB: the key does not exist";
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid time series effect")
}
fn syntax() -> Resp {
    error("TSDB: invalid arguments")
}
fn number(v: &[u8], nan: bool) -> Option<f64> {
    if nan && v.eq_ignore_ascii_case(b"nan") {
        return Some(f64::NAN);
    }
    let mut i = usize::from(v.first() == Some(&b'-'));
    match v.get(i).copied()? {
        b'0' => i += 1,
        b'1'..=b'9' => {
            i += 1;
            while v.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
        }
        _ => return None,
    }
    if v.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while v.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if start == i {
            return None;
        }
    }
    if v.get(i).is_some_and(|b| matches!(b, b'e' | b'E')) {
        i += 1;
        if v.get(i).is_some_and(|b| matches!(b, b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while v.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if start == i {
            return None;
        }
    }
    if i != v.len() {
        return None;
    }
    let n = std::str::from_utf8(v).ok()?.parse::<f64>().ok()?;
    n.is_finite().then_some(n)
}
fn filter_number(v: &[u8]) -> Option<f64> {
    if v.eq_ignore_ascii_case(b"-inf") {
        Some(f64::NEG_INFINITY)
    } else if v.eq_ignore_ascii_case(b"+inf") || v.eq_ignore_ascii_case(b"inf") {
        Some(f64::INFINITY)
    } else {
        number(v, false)
    }
}
fn timestamp(v: &[u8]) -> Option<i64> {
    integer(v).filter(|n| *n >= 0)
}
fn eq(v: &[u8], s: &[u8]) -> bool {
    v.eq_ignore_ascii_case(s)
}
fn bytes(v: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(v.into()))
}
fn float(v: f64) -> Resp {
    let mut buffer = ryu::Buffer::new();
    let text = if v.is_nan() {
        "nan"
    } else if v == f64::INFINITY {
        "inf"
    } else if v == f64::NEG_INFINITY {
        "-inf"
    } else {
        let text = buffer.format_finite(v);
        text.strip_suffix(".0").unwrap_or(text)
    };
    Resp::Status(Bytes::copy_from_slice(text.as_bytes()))
}
fn float_for(v: f64, resp3: bool) -> Resp {
    if resp3 { Resp::Double(v) } else { float(v) }
}
fn sample_pair(t: i64, v: f64, resp3: bool) -> Resp {
    array(vec![Resp::Integer(t), float_for(v, resp3)])
}
#[cfg(test)]
fn pair(t: i64, v: f64) -> Resp {
    sample_pair(t, v, false)
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum Policy {
    Block,
    First,
    Last,
    Min,
    Max,
    Sum,
}
impl Policy {
    fn parse(v: &[u8]) -> Option<Self> {
        [
            Self::Block,
            Self::First,
            Self::Last,
            Self::Min,
            Self::Max,
            Self::Sum,
        ]
        .into_iter()
        .find(|p| eq(v, p.name().as_bytes()))
    }
    fn name(self) -> &'static str {
        match self {
            Self::Block => "block",
            Self::First => "first",
            Self::Last => "last",
            Self::Min => "min",
            Self::Max => "max",
            Self::Sum => "sum",
        }
    }
    fn decode(n: u8) -> io::Result<Self> {
        [
            Self::Block,
            Self::First,
            Self::Last,
            Self::Min,
            Self::Max,
            Self::Sum,
        ]
        .get(n as usize)
        .copied()
        .ok_or_else(invalid)
    }
    fn resolve(self, old: f64, new: f64) -> Result<f64, Resp> {
        if self == Self::Block {
            return Err(error(
                "TSDB: Error at upsert, update is not supported when DUPLICATE_POLICY is set to BLOCK mode",
            ));
        }
        if matches!(self, Self::Min | Self::Max | Self::Sum) && old.is_nan() != new.is_nan() {
            return Err(error(
                "TSDB: cannot apply duplicate policy to NaN and non-NaN values",
            ));
        }
        if old.is_nan() {
            return Ok(if self == Self::First { old } else { new });
        }
        if new.is_nan() {
            return Ok(if self == Self::Last { new } else { old });
        }
        Ok(match self {
            Self::First => old,
            Self::Last => new,
            Self::Min => old.min(new),
            Self::Max => old.max(new),
            Self::Sum => old + new,
            Self::Block => unreachable!(),
        })
    }
}
#[derive(Clone)]
struct Config {
    retention: i64,
    chunk_size: usize,
    compressed: bool,
    policy: Option<Policy>,
    ignore_time: i64,
    ignore_value: f64,
    labels: BTreeMap<Bytes, Bytes>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            retention: 0,
            chunk_size: 4096,
            compressed: true,
            policy: None,
            ignore_time: 0,
            ignore_value: 0.0,
            labels: BTreeMap::new(),
        }
    }
}

// Chunks are actual independently allocated storage blocks. Compressed blocks use
// delta timestamps and XOR value varints, so identical readings consume two bytes.
struct Chunk {
    data: Vec<u8>,
    count: usize,
    first: i64,
    last: i64,
    compressed: bool,
    allocation: usize,
}
fn put_var(mut n: u64, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push((n as u8) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn get_var(input: &[u8], p: &mut usize) -> u64 {
    let mut n = 0;
    let mut shift = 0;
    loop {
        let b = input[*p];
        *p += 1;
        n |= ((b & 127) as u64) << shift;
        if b < 128 {
            return n;
        }
        shift += 7;
    }
}
impl Chunk {
    fn new(rows: &[(i64, f64)], compressed: bool, allocation: usize) -> Self {
        let mut data = Vec::with_capacity(allocation.max(rows.len() * 2));
        let (mut t, mut v) = (0u64, 0u64);
        for &(ts, x) in rows {
            if compressed {
                put_var(ts as u64 - t, &mut data);
                put_var(x.to_bits() ^ v, &mut data);
            } else {
                data.extend_from_slice(&ts.to_le_bytes());
                data.extend_from_slice(&x.to_bits().to_le_bytes());
            }
            t = ts as u64;
            v = x.to_bits();
        }
        Self {
            data,
            count: rows.len(),
            first: rows.first().map_or(0, |s| s.0),
            last: rows.last().map_or(0, |s| s.0),
            compressed,
            allocation,
        }
    }
    fn rows(&self) -> Vec<(i64, f64)> {
        let mut out = Vec::with_capacity(self.count);
        let (mut p, mut t, mut v) = (0, 0u64, 0u64);
        for _ in 0..self.count {
            if self.compressed {
                t += get_var(&self.data, &mut p);
                v ^= get_var(&self.data, &mut p);
            } else {
                t = u64::from_le_bytes(self.data[p..p + 8].try_into().unwrap());
                v = u64::from_le_bytes(self.data[p + 8..p + 16].try_into().unwrap());
                p += 16;
            }
            out.push((t as i64, f64::from_bits(v)));
        }
        out
    }
}
#[derive(Clone, Copy, Debug, PartialEq)]
enum Agg {
    Avg,
    Sum,
    Min,
    Max,
    Range,
    Count,
    First,
    Last,
    StdP,
    StdS,
    VarP,
    VarS,
    Twa,
    CountNan,
    CountAll,
}
impl Agg {
    fn name(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Sum => "sum",
            Self::Min => "min",
            Self::Max => "max",
            Self::Range => "range",
            Self::Count => "count",
            Self::First => "first",
            Self::Last => "last",
            Self::StdP => "std.p",
            Self::StdS => "std.s",
            Self::VarP => "var.p",
            Self::VarS => "var.s",
            Self::Twa => "twa",
            Self::CountNan => "countNaN",
            Self::CountAll => "countAll",
        }
    }
    fn all() -> [Self; 15] {
        [
            Self::Avg,
            Self::Sum,
            Self::Min,
            Self::Max,
            Self::Range,
            Self::Count,
            Self::First,
            Self::Last,
            Self::StdP,
            Self::StdS,
            Self::VarP,
            Self::VarS,
            Self::Twa,
            Self::CountNan,
            Self::CountAll,
        ]
    }
    fn parse(v: &[u8]) -> Option<Self> {
        Self::all().into_iter().find(|a| eq(v, a.name().as_bytes()))
    }
    fn decode(v: u8) -> io::Result<Self> {
        Self::all().get(v as usize).copied().ok_or_else(invalid)
    }
}
struct Rule {
    dest: Bytes,
    agg: Agg,
    duration: i64,
    align: i64,
    current: Option<i64>,
    active: BTreeMap<i64, f64>,
    previous: Option<(i64, f64)>,
    stats: Stats,
}
#[derive(Default)]
struct Stats {
    n: usize,
    nan: usize,
    sum: f64,
    mean: f64,
    m2: f64,
    ordered: BTreeMap<u64, usize>,
    finite: BTreeMap<i64, f64>,
    area: f64,
}
fn float_key(v: f64) -> u64 {
    let b = v.to_bits();
    if b >> 63 != 0 { !b } else { b ^ (1 << 63) }
}
fn key_float(k: u64) -> f64 {
    f64::from_bits(if k >> 63 != 0 { k ^ (1 << 63) } else { !k })
}
fn trapezoid(a: (i64, f64), b: (i64, f64)) -> f64 {
    (a.1 * 0.5 + b.1 * 0.5) * (b.0 - a.0) as f64
}
impl Stats {
    fn change(&mut self, t: i64, old: Option<f64>, new: Option<f64>) {
        if let Some(v) = old {
            if v.is_nan() {
                self.nan -= 1;
            } else {
                self.edge_remove(t);
                self.n -= 1;
                self.sum -= v;
                if self.n == 0 {
                    self.mean = 0.0;
                    self.m2 = 0.0;
                } else {
                    let next = self.mean + (self.mean - v) / self.n as f64;
                    self.m2 -= (v - self.mean) * (v - next);
                    self.mean = next;
                }
                let k = float_key(v);
                let count = self.ordered.get_mut(&k).unwrap();
                *count -= 1;
                if *count == 0 {
                    self.ordered.remove(&k);
                }
            }
        }
        if let Some(v) = new {
            if v.is_nan() {
                self.nan += 1;
            } else {
                self.edge_insert(t, v);
                self.n += 1;
                self.sum += v;
                let old_mean = self.mean;
                self.mean = old_mean * ((self.n - 1) as f64 / self.n as f64) + v / self.n as f64;
                self.m2 += (v - old_mean) * (v - self.mean);
                *self.ordered.entry(float_key(v)).or_default() += 1;
            }
        }
    }
    fn edge_remove(&mut self, t: i64) {
        let prev = self.finite.range(..t).next_back().map(|(&t, &v)| (t, v));
        let next = self
            .finite
            .range((std::ops::Bound::Excluded(t), std::ops::Bound::Unbounded))
            .next()
            .map(|(&t, &v)| (t, v));
        if let Some(v) = self.finite.remove(&t) {
            if let Some(p) = prev {
                self.area -= trapezoid(p, (t, v));
            }
            if let Some(n) = next {
                self.area -= trapezoid((t, v), n);
            }
            if let (Some(p), Some(n)) = (prev, next) {
                self.area += trapezoid(p, n);
            }
        }
    }
    fn edge_insert(&mut self, t: i64, v: f64) {
        let prev = self.finite.range(..t).next_back().map(|(&t, &v)| (t, v));
        let next = self
            .finite
            .range((std::ops::Bound::Excluded(t), std::ops::Bound::Unbounded))
            .next()
            .map(|(&t, &v)| (t, v));
        if let (Some(p), Some(n)) = (prev, next) {
            self.area -= trapezoid(p, n);
        }
        if let Some(p) = prev {
            self.area += trapezoid(p, (t, v));
        }
        if let Some(n) = next {
            self.area += trapezoid((t, v), n);
        }
        self.finite.insert(t, v);
    }
}
impl Rule {
    fn update(&mut self, b: i64, t: i64, v: Option<f64>, previous: Option<(i64, f64)>) {
        if self.current != Some(b) {
            self.active.clear();
            self.stats = Stats::default();
            self.current = Some(b);
            self.previous = previous;
        }
        let old = self.active.remove(&t);
        self.stats.change(t, old, v);
        if let Some(v) = v {
            self.active.insert(t, v);
        }
    }
    fn value(&self, after: Option<(i64, f64)>) -> f64 {
        let s = &self.stats;
        let n = s.n;
        let a = self.agg;
        if a == Agg::CountAll {
            return (n + s.nan) as f64;
        }
        if a == Agg::CountNan {
            return s.nan as f64;
        }
        if a == Agg::Count {
            return n as f64;
        }
        if a == Agg::Twa {
            let start = self.current.unwrap_or(0);
            let end = bucket_end(start, self.duration, self.align);
            let first = s.finite.first_key_value().map(|(&t, &v)| (t, v));
            let last = s.finite.last_key_value().map(|(&t, &v)| (t, v));
            let mut area = s.area;
            let mut lo = first.map_or(start, |p| p.0);
            let mut hi = last.map_or(start, |p| p.0);
            if let (Some(p), Some(f)) = (self.previous.filter(|p| !p.1.is_nan()), first)
                && f.0 > p.0
            {
                let v = p.1 + (f.1 - p.1) * (start - p.0) as f64 / (f.0 - p.0) as f64;
                area += trapezoid((start, v), f);
                lo = start;
            }
            if let (Some(l), Some(p)) = (last, after.filter(|p| !p.1.is_nan()))
                && p.0 > l.0
            {
                let v = l.1 + (p.1 - l.1) * (end - l.0) as f64 / (p.0 - l.0) as f64;
                area += trapezoid(l, (end, v));
                hi = end;
            }
            return if hi > lo {
                area / (hi - lo) as f64
            } else {
                first.map_or(f64::NAN, |p| p.1)
            };
        }
        if n == 0 {
            return if a == Agg::Sum { 0.0 } else { f64::NAN };
        }
        let min = key_float(*s.ordered.first_key_value().unwrap().0);
        let max = key_float(*s.ordered.last_key_value().unwrap().0);
        match a {
            Agg::Avg => s.mean,
            Agg::Sum => s.sum,
            Agg::Min => min,
            Agg::Max => max,
            Agg::Range => max - min,
            Agg::First => *s.finite.first_key_value().unwrap().1,
            Agg::Last => *s.finite.last_key_value().unwrap().1,
            Agg::StdP => (s.m2 / n as f64).max(0.0).sqrt(),
            Agg::StdS => {
                if n > 1 {
                    (s.m2 / (n - 1) as f64).max(0.0).sqrt()
                } else {
                    f64::NAN
                }
            }
            Agg::VarP => s.m2 / n as f64,
            Agg::VarS => {
                if n > 1 {
                    s.m2 / (n - 1) as f64
                } else {
                    f64::NAN
                }
            }
            _ => unreachable!(),
        }
    }
}
pub(super) struct TimeSeriesValue {
    config: Config,
    chunks: BTreeMap<i64, Chunk>,
    count: usize,
    rules: BTreeMap<Bytes, Rule>,
    source: Option<Bytes>,
    pending: Option<(i64, f64)>,
    expired: Option<i64>,
}
impl TimeSeriesValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{sampled, tree_bytes};
        tree_bytes::<(Bytes, Bytes)>(self.config.labels.len())
            .saturating_add(sampled(
                self.config.labels.iter(),
                samples,
                |(key, value)| key.len().saturating_add(value.len()),
            ))
            .saturating_add(tree_bytes::<(i64, Chunk)>(self.chunks.len()))
            .saturating_add(sampled(self.chunks.values(), samples, |chunk| {
                chunk.data.capacity()
            }))
            .saturating_add(tree_bytes::<(Bytes, Rule)>(self.rules.len()))
            .saturating_add(sampled(self.rules.iter(), samples, |(key, rule)| {
                key.len()
                    .saturating_add(rule.dest.len())
                    .saturating_add(tree_bytes::<(i64, f64)>(rule.active.len()))
                    .saturating_add(tree_bytes::<(u64, usize)>(rule.stats.ordered.len()))
                    .saturating_add(tree_bytes::<(i64, f64)>(rule.stats.finite.len()))
            }))
            .saturating_add(self.source.as_ref().map_or(0, Bytes::len))
    }

    fn new(config: Config) -> Self {
        let mut chunks = BTreeMap::new();
        chunks.insert(0, Chunk::new(&[], config.compressed, config.chunk_size));
        Self {
            config,
            chunks,
            count: 0,
            rules: BTreeMap::new(),
            source: None,
            pending: None,
            expired: None,
        }
    }
    fn iter(&self, lo: i64, hi: i64) -> impl DoubleEndedIterator<Item = (i64, f64)> + '_ {
        let start = self.chunks.range(..=lo).next_back().map_or(lo, |(k, _)| *k);
        self.chunks
            .range(start..=hi.max(start))
            .flat_map(|(_, c)| c.rows())
            .filter(move |(t, _)| *t >= lo && *t <= hi)
    }
    fn rows(&self, lo: i64, hi: i64) -> Vec<(i64, f64)> {
        self.iter(lo, hi).collect()
    }
    fn before(&self, t: i64) -> Option<(i64, f64)> {
        if t <= 0 {
            return None;
        }
        self.iter(0, t - 1).rev().find(|p| !p.1.is_nan())
    }
    fn after(&self, t: i64) -> Option<(i64, f64)> {
        self.iter(t, i64::MAX).find(|p| !p.1.is_nan())
    }
    fn first(&self) -> Option<(i64, f64)> {
        self.chunks
            .first_key_value()
            .and_then(|(_, c)| c.rows().first().copied())
    }
    fn last(&self) -> Option<(i64, f64)> {
        self.chunks
            .last_key_value()
            .and_then(|(_, c)| c.rows().last().copied())
    }
    fn last_with_pending(&self) -> Option<(i64, f64)> {
        match (self.last(), self.pending) {
            (Some(a), Some(b)) => Some(if b.0 >= a.0 { b } else { a }),
            (a, b) => a.or(b),
        }
    }
    fn get(&self, t: i64) -> Option<f64> {
        let c = self.chunks.range(..=t).next_back()?.1;
        c.rows().into_iter().find(|s| s.0 == t).map(|s| s.1)
    }
    fn set(&mut self, t: i64, v: f64) {
        let key = self
            .chunks
            .range(..=t)
            .next_back()
            .map(|(k, _)| *k)
            .or_else(|| self.chunks.first_key_value().map(|(k, _)| *k));
        let Some(key) = key else {
            self.chunks.insert(
                t,
                Chunk::new(&[(t, v)], self.config.compressed, self.config.chunk_size),
            );
            self.count += 1;
            return;
        };
        let chunk = self.chunks.remove(&key).unwrap();
        let mut rows = chunk.rows();
        match rows.binary_search_by_key(&t, |s| s.0) {
            Ok(i) => rows[i].1 = v,
            Err(i) => {
                rows.insert(i, (t, v));
                self.count += 1;
            }
        }
        let capacity = (chunk.allocation / 16).max(3);
        if rows.len() > capacity {
            let right = rows.split_off(rows.len() / 2);
            self.chunks.insert(
                right[0].0,
                Chunk::new(&right, self.config.compressed, self.config.chunk_size),
            );
        }
        self.chunks.insert(
            rows[0].0,
            Chunk::new(&rows, chunk.compressed, chunk.allocation),
        );
    }
    fn delete(&mut self, lo: i64, hi: i64) -> usize {
        if lo > hi {
            return 0;
        }
        let start = self.chunks.range(..=lo).next_back().map_or(lo, |(k, _)| *k);
        let keys: Vec<_> = self.chunks.range(start..=hi).map(|(k, _)| *k).collect();
        let mut removed = 0;
        for k in keys {
            let c = self.chunks.remove(&k).unwrap();
            let mut rows = c.rows();
            let n = rows.len();
            rows.retain(|s| s.0 < lo || s.0 > hi);
            removed += n - rows.len();
            if !rows.is_empty() {
                self.chunks
                    .insert(rows[0].0, Chunk::new(&rows, c.compressed, c.allocation));
            }
        }
        self.count -= removed;
        if self.chunks.is_empty() {
            self.chunks.insert(
                0,
                Chunk::new(&[], self.config.compressed, self.config.chunk_size),
            );
        }
        removed
    }
    fn cutoff(&self) -> i64 {
        if self.config.retention == 0 {
            0
        } else {
            self.last()
                .map_or(0, |s| s.0.saturating_sub(self.config.retention))
        }
    }
    fn trim(&mut self) {
        let cut = self.cutoff();
        if cut > 0 {
            let expired = self.iter(0, cut - 1).next_back().map(|p| p.0);
            self.prune(cut, expired);
        }
    }
    fn prune(&mut self, cut: i64, expired: Option<i64>) {
        self.expired = match (self.expired, expired) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        };
        if cut > 0 {
            self.delete(0, cut - 1);
        }
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let op = decode_effect(effect)?;
        if let Effect::Config(c) = op {
            Ok(Self::new(c))
        } else {
            Err(invalid())
        }
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let op = decode_effect(effect)?;
        match &op {
            Effect::Active(dest, ..) | Effect::Previous(dest, _)
                if !self.rules.contains_key(dest) =>
            {
                return Err(invalid());
            }
            Effect::Active(dest, b, t, v, previous) => {
                let r = &self.rules[dest];
                if *b < 0
                    || bucket(*b, r.duration, r.align) != *b
                    || r.current.is_some_and(|current| *b < current)
                    || (v.is_some() && bucket(*t, r.duration, r.align) != *b)
                    || previous.is_some_and(|p| p.0 >= *b)
                {
                    return Err(invalid());
                }
            }
            Effect::Previous(dest, p)
                if p.is_some_and(|p| self.rules[dest].current.is_none_or(|b| p.0 >= b)) =>
            {
                return Err(invalid());
            }
            Effect::Chunk(rows, ..) if rows.is_empty() && self.count > 0 => return Err(invalid()),
            Effect::Chunk(rows, ..)
                if !rows.is_empty()
                    && self.chunks.values().any(|c| {
                        c.count > 0 && c.first <= rows.last().unwrap().0 && c.last >= rows[0].0
                    }) =>
            {
                return Err(invalid());
            }
            _ => {}
        }
        self.apply_decoded(op);
        Ok(())
    }
    fn apply_decoded(&mut self, e: Effect) {
        match e {
            Effect::Config(c) => self.config = c,
            Effect::Set(t, v, trim) => {
                self.set(t, v);
                if trim {
                    self.trim();
                }
            }
            Effect::Delete(lo, hi) => {
                self.delete(lo, hi);
            }
            Effect::Rule(dest, agg, duration, align) => {
                self.rules.insert(
                    dest.clone(),
                    Rule {
                        dest,
                        agg,
                        duration,
                        align,
                        current: None,
                        active: BTreeMap::new(),
                        previous: None,
                        stats: Stats::default(),
                    },
                );
            }
            Effect::Unrule(dest) => {
                self.rules.remove(&dest);
            }
            Effect::Source(s) => self.source = s,
            Effect::Pending(p) => self.pending = p,
            Effect::Active(dest, bucket, t, v, previous) => {
                if let Some(r) = self.rules.get_mut(&dest) {
                    r.update(bucket, t, v, previous);
                }
            }
            Effect::Previous(dest, p) => {
                if let Some(r) = self.rules.get_mut(&dest) {
                    r.previous = p;
                }
            }
            Effect::Chunk(rows, compressed, allocation) => {
                if self.count == 0 {
                    self.chunks.clear();
                }
                let c = Chunk::new(&rows, compressed, allocation);
                self.count += c.count;
                self.chunks.insert(c.first, c);
            }
            Effect::Prune(cut, expired) => self.prune(cut, expired),
        }
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            value: self,
            stage: 0,
            chunks: self.chunks.values(),
            rules: self.rules.values(),
            active: None,
            cursor: None,
        }
    }
}
enum Effect {
    Config(Config),
    Set(i64, f64, bool),
    Delete(i64, i64),
    Rule(Bytes, Agg, i64, i64),
    Unrule(Bytes),
    Source(Option<Bytes>),
    Active(Bytes, i64, i64, Option<f64>, Option<(i64, f64)>),
    Pending(Option<(i64, f64)>),
    Chunk(Vec<(i64, f64)>, bool, usize),
    Previous(Bytes, Option<(i64, f64)>),
    Prune(i64, Option<i64>),
}
fn encode(e: &Effect) -> io::Result<Bytes> {
    let mut w = Encoder::new(
        1,
        match e {
            Effect::Config(_) => 0,
            Effect::Set(..) => 1,
            Effect::Delete(..) => 2,
            Effect::Rule(..) => 3,
            Effect::Unrule(_) => 4,
            Effect::Source(_) => 5,
            Effect::Active(..) => 6,
            Effect::Pending(_) => 7,
            Effect::Chunk(..) => 8,
            Effect::Previous(..) => 9,
            Effect::Prune(..) => 10,
        },
    );
    match e {
        Effect::Config(c) => {
            w.i64(c.retention)?;
            w.u32(c.chunk_size as u32)?;
            w.u8(c.compressed as u8)?;
            w.u8(c.policy.map_or(255, |p| p as u8))?;
            w.i64(c.ignore_time)?;
            w.f64(c.ignore_value)?;
            w.u32(c.labels.len() as u32)?;
            for (k, v) in &c.labels {
                w.bytes(k)?;
                w.bytes(v)?;
            }
        }
        Effect::Set(t, v, trim) => {
            w.i64(*t)?;
            w.u64(v.to_bits())?;
            w.u8(*trim as u8)?;
        }
        Effect::Delete(lo, hi) => {
            w.i64(*lo)?;
            w.i64(*hi)?;
        }
        Effect::Rule(d, a, b, al) => {
            w.bytes(d)?;
            w.u8(*a as u8)?;
            w.i64(*b)?;
            w.i64(*al)?;
        }
        Effect::Unrule(d) => w.bytes(d)?,
        Effect::Source(s) => {
            w.u8(s.is_some() as u8)?;
            if let Some(s) = s {
                w.bytes(s)?;
            }
        }
        Effect::Active(d, b, t, v, p) => {
            w.bytes(d)?;
            w.i64(*b)?;
            w.i64(*t)?;
            w.u8(v.is_some() as u8)?;
            if let Some(v) = v {
                w.u64(v.to_bits())?;
            }
            w.u8(p.is_some() as u8)?;
            if let Some((t, v)) = p {
                w.i64(*t)?;
                w.u64(v.to_bits())?;
            }
        }
        Effect::Pending(p) => {
            w.u8(p.is_some() as u8)?;
            if let Some((t, v)) = p {
                w.i64(*t)?;
                w.u64(v.to_bits())?;
            }
        }
        Effect::Chunk(rows, compressed, allocation) => {
            w.u8(*compressed as u8)?;
            w.u32(*allocation as u32)?;
            w.u32(rows.len() as u32)?;
            for (t, v) in rows {
                w.i64(*t)?;
                w.u64(v.to_bits())?;
            }
        }
        Effect::Previous(d, p) => {
            w.bytes(d)?;
            w.u8(p.is_some() as u8)?;
            if let Some((t, v)) = p {
                w.i64(*t)?;
                w.u64(v.to_bits())?;
            }
        }
        Effect::Prune(cut, expired) => {
            w.i64(*cut)?;
            w.u8(expired.is_some() as u8)?;
            if let Some(t) = expired {
                w.i64(*t)?;
            }
        }
    }
    Ok(w.finish())
}
fn decode_effect(b: &[u8]) -> io::Result<Effect> {
    let (op, mut r) = Decoder::new(b, 1)?;
    let e = match op {
        0 => {
            let retention = r.i64()?;
            let chunk_size = r.u32()? as usize;
            let compressed = r.u8()? != 0;
            let p = r.u8()?;
            let policy = if p == 255 {
                None
            } else {
                Some(Policy::decode(p)?)
            };
            let ignore_time = r.i64()?;
            let ignore_value = r.f64()?;
            if retention < 0
                || ignore_time < 0
                || !ignore_value.is_finite()
                || ignore_value < 0.0
                || !(48..=1048576).contains(&chunk_size)
                || !chunk_size.is_multiple_of(8)
            {
                return Err(invalid());
            }
            let n = r.u32()?;
            let mut labels = BTreeMap::new();
            for _ in 0..n {
                labels.insert(
                    Bytes::copy_from_slice(r.bytes()?),
                    Bytes::copy_from_slice(r.bytes()?),
                );
            }
            Effect::Config(Config {
                retention,
                chunk_size,
                compressed,
                policy,
                ignore_time,
                ignore_value,
                labels,
            })
        }
        1 => {
            let t = r.i64()?;
            let v = f64::from_bits(r.u64()?);
            let trim = r.u8()? != 0;
            if t < 0 {
                return Err(invalid());
            }
            Effect::Set(t, v, trim)
        }
        2 => {
            let a = r.i64()?;
            let b = r.i64()?;
            if a < 0 || b < a {
                return Err(invalid());
            }
            Effect::Delete(a, b)
        }
        3 => {
            let d = Bytes::copy_from_slice(r.bytes()?);
            let a = Agg::decode(r.u8()?)?;
            let b = r.i64()?;
            let al = r.i64()?;
            if b <= 0 || al < 0 || d.len() > super::typed::codec::MAX_EFFECT_BYTES - 48 {
                return Err(invalid());
            }
            Effect::Rule(d, a, b, al)
        }
        4 => Effect::Unrule(Bytes::copy_from_slice(r.bytes()?)),
        5 => Effect::Source(if r.u8()? != 0 {
            Some(Bytes::copy_from_slice(r.bytes()?))
        } else {
            None
        }),
        6 => {
            let d = Bytes::copy_from_slice(r.bytes()?);
            let b = r.i64()?;
            let t = r.i64()?;
            let v = if r.u8()? != 0 {
                Some(f64::from_bits(r.u64()?))
            } else {
                None
            };
            let p = if r.u8()? != 0 {
                Some((r.i64()?, f64::from_bits(r.u64()?)))
            } else {
                None
            };
            if t < 0
                || v.is_some_and(f64::is_infinite)
                || p.is_some_and(|(t, v)| t < 0 || v.is_infinite())
            {
                return Err(invalid());
            }
            Effect::Active(d, b, t, v, p)
        }
        7 => {
            let p = if r.u8()? != 0 {
                Some((r.i64()?, f64::from_bits(r.u64()?)))
            } else {
                None
            };
            if p.is_some_and(|(t, _)| t < 0) {
                return Err(invalid());
            }
            Effect::Pending(p)
        }
        8 => {
            let compressed = r.u8()? != 0;
            let allocation = r.u32()? as usize;
            let n = r.u32()?;
            if !(48..=1048576).contains(&allocation) || !allocation.is_multiple_of(8) || n > 65537 {
                return Err(invalid());
            }
            let mut rows = Vec::new();
            for _ in 0..n {
                let t = r.i64()?;
                let v = f64::from_bits(r.u64()?);
                if t < 0 || rows.last().is_some_and(|p: &(i64, f64)| p.0 >= t) {
                    return Err(invalid());
                }
                rows.push((t, v));
            }
            Effect::Chunk(rows, compressed, allocation)
        }
        9 => {
            let d = Bytes::copy_from_slice(r.bytes()?);
            let p = if r.u8()? != 0 {
                Some((r.i64()?, f64::from_bits(r.u64()?)))
            } else {
                None
            };
            if p.is_some_and(|(t, v)| t < 0 || !v.is_finite()) {
                return Err(invalid());
            }
            Effect::Previous(d, p)
        }
        10 => {
            let cut = r.i64()?;
            let expired = if r.u8()? != 0 { Some(r.i64()?) } else { None };
            if cut < 0 || expired.is_some_and(|t| t < 0) {
                return Err(invalid());
            }
            Effect::Prune(cut, expired)
        }
        _ => return Err(invalid()),
    };
    r.finish()?;
    Ok(e)
}
pub(super) struct Snapshot<'a> {
    value: &'a TimeSeriesValue,
    stage: u8,
    chunks: std::collections::btree_map::Values<'a, i64, Chunk>,
    rules: std::collections::btree_map::Values<'a, Bytes, Rule>,
    active: Option<(&'a Rule, std::collections::btree_map::Iter<'a, i64, f64>)>,
    cursor: Option<&'a Rule>,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.stage {
                0 => {
                    self.stage = 1;
                    return Some(encode(&Effect::Config(self.value.config.clone())));
                }
                1 => {
                    self.stage = 2;
                    return Some(encode(&Effect::Source(self.value.source.clone())));
                }
                2 => {
                    if let Some(c) = self.chunks.next() {
                        return Some(encode(&Effect::Chunk(c.rows(), c.compressed, c.allocation)));
                    } else {
                        self.stage = 3;
                    }
                }
                3 => {
                    if let Some(r) = self.cursor.take() {
                        return Some(encode(&Effect::Active(
                            r.dest.clone(),
                            r.current.unwrap(),
                            0,
                            None,
                            r.previous,
                        )));
                    }
                    if let Some((r, it)) = &mut self.active {
                        if let Some((&t, &v)) = it.next() {
                            return Some(encode(&Effect::Active(
                                r.dest.clone(),
                                r.current.unwrap(),
                                t,
                                Some(v),
                                r.previous,
                            )));
                        }
                        self.active = None;
                    }
                    if let Some(r) = self.rules.next() {
                        self.active = Some((r, r.active.iter()));
                        self.cursor = r.current.map(|_| r);
                        return Some(encode(&Effect::Rule(
                            r.dest.clone(),
                            r.agg,
                            r.duration,
                            r.align,
                        )));
                    }
                    self.stage = 4;
                }
                4 => {
                    self.stage = 5;
                    return Some(encode(&Effect::Pending(self.value.pending)));
                }
                5 => {
                    self.stage = 6;
                    return Some(encode(&Effect::Prune(0, self.value.expired)));
                }
                _ => return None,
            }
        }
    }
}

fn bucket(t: i64, d: i64, a: i64) -> i64 {
    let t = t as i128;
    let d = d as i128;
    let a = (a as i128).rem_euclid(d);
    ((t - a).div_euclid(d) * d + a).max(0).min(i64::MAX as i128) as i64
}
// The clipped first bucket ends on the alignment grid, not at 0 + duration.
fn bucket_end(start: i64, duration: i64, align: i64) -> i64 {
    let offset = align.rem_euclid(duration);
    if start == 0 && offset != 0 {
        offset
    } else {
        start.saturating_add(duration)
    }
}
fn aggregate(
    a: Agg,
    rows: &[(i64, f64)],
    start: i64,
    end: i64,
    before: Option<(i64, f64)>,
    after: Option<(i64, f64)>,
) -> f64 {
    if a == Agg::CountAll {
        return rows.len() as f64;
    }
    if a == Agg::CountNan {
        return rows.iter().filter(|s| s.1.is_nan()).count() as f64;
    }
    let values: Vec<_> = rows.iter().copied().filter(|s| !s.1.is_nan()).collect();
    let n = values.len();
    if a == Agg::Twa {
        if n == 0 && (before.is_none() || after.is_none()) {
            return f64::NAN;
        }
        let mut points = Vec::with_capacity(n + 2);
        if let Some(p) = before.filter(|p| !p.1.is_nan()) {
            points.push(p);
        }
        points.extend_from_slice(&values);
        if let Some(p) = after.filter(|p| !p.1.is_nan()) {
            points.push(p);
        }
        if points.len() == 1 {
            return points[0].1;
        }
        let mut area = 0.0;
        let mut span = 0.0;
        for w in points.windows(2) {
            let lo = w[0].0.max(start);
            let hi = w[1].0.min(end);
            if hi <= lo || w[1].0 == w[0].0 {
                continue;
            }
            let slope = (w[1].1 - w[0].1) / (w[1].0 - w[0].0) as f64;
            let l = w[0].1 + slope * (lo - w[0].0) as f64;
            let h = w[0].1 + slope * (hi - w[0].0) as f64;
            area += (l + h) * 0.5 * (hi - lo) as f64;
            span += (hi - lo) as f64;
        }
        // As Redis TwaFinalize does for a zero-width bucket containing one
        // sample, return that sample rather than letting an outside neighbor
        // turn a valid boundary sample into NaN.
        return if span > 0.0 {
            area / span
        } else {
            values.first().map_or(f64::NAN, |p| p.1)
        };
    }
    if n == 0 {
        return match a {
            Agg::Count | Agg::Sum => 0.0,
            Agg::Last => before.map_or(f64::NAN, |p| p.1),
            _ => f64::NAN,
        };
    }
    let (mut mean, mut m2, mut sum, mut min, mut max) =
        (0.0, 0.0, 0.0, f64::INFINITY, f64::NEG_INFINITY);
    for (i, (_, x)) in values.iter().enumerate() {
        sum += x;
        min = min.min(*x);
        max = max.max(*x);
        let old = mean;
        mean = old * (i as f64 / (i + 1) as f64) + x / (i + 1) as f64;
        m2 += (x - old) * (x - mean);
    }
    match a {
        Agg::Avg => mean,
        Agg::Sum => sum,
        Agg::Min => min,
        Agg::Max => max,
        Agg::Range => max - min,
        Agg::Count => n as f64,
        Agg::First => values[0].1,
        Agg::Last => values[n - 1].1,
        Agg::StdP => (m2 / n as f64).max(0.0).sqrt(),
        Agg::StdS => {
            if n > 1 {
                (m2 / (n - 1) as f64).max(0.0).sqrt()
            } else {
                f64::NAN
            }
        }
        Agg::VarP => m2 / n as f64,
        Agg::VarS => {
            if n > 1 {
                m2 / (n - 1) as f64
            } else {
                f64::NAN
            }
        }
        _ => unreachable!(),
    }
}

struct CreateOptions {
    config: Config,
    on_duplicate: Option<Policy>,
    time: Option<i64>,
}
fn create_options(
    args: &[Bytes],
    mut i: usize,
    mut config: Config,
    now: i64,
    mode: u8,
) -> Result<CreateOptions, Resp> {
    let (mut duplicate, mut time) = (None, None);
    while i < args.len() {
        let opt = &args[i];
        i += 1;
        if eq(opt, b"LABELS") {
            if !(args.len() - i).is_multiple_of(2) {
                return Err(syntax());
            }
            config.labels.clear();
            while i < args.len() {
                if args[i].is_empty()
                    || args[i + 1].is_empty()
                    || config.labels.contains_key(&args[i])
                {
                    return Err(error("TSDB: invalid labels"));
                }
                config.labels.insert(args[i].clone(), args[i + 1].clone());
                i += 2;
            }
            break;
        }
        if eq(opt, b"UNCOMPRESSED") && mode != 1 {
            config.compressed = false;
            continue;
        }
        if i >= args.len() {
            return Err(syntax());
        }
        let value = &args[i];
        i += 1;
        if eq(opt, b"RETENTION") {
            config.retention = timestamp(value).ok_or_else(syntax)?;
        } else if eq(opt, b"CHUNK_SIZE") {
            let n = timestamp(value).ok_or_else(syntax)? as usize;
            if !(48..=1048576).contains(&n) || !n.is_multiple_of(8) {
                return Err(error(
                    "TSDB: CHUNK_SIZE must be a multiple of 8 in the range [48 .. 1048576]",
                ));
            }
            config.chunk_size = n;
        } else if eq(opt, b"ENCODING") && mode != 1 {
            config.compressed = if eq(value, b"COMPRESSED") {
                true
            } else if eq(value, b"UNCOMPRESSED") {
                false
            } else {
                return Err(syntax());
            };
        } else if eq(opt, b"DUPLICATE_POLICY") {
            config.policy = Some(Policy::parse(value).ok_or_else(syntax)?);
        } else if eq(opt, b"ON_DUPLICATE") && mode == 2 {
            duplicate = Some(Policy::parse(value).ok_or_else(syntax)?);
        } else if eq(opt, b"TIMESTAMP") && mode == 3 {
            time = Some(if value.as_ref() == b"*" {
                now.max(0)
            } else {
                timestamp(value).ok_or_else(syntax)?
            });
        } else if eq(opt, b"IGNORE") {
            config.ignore_time = timestamp(value).ok_or_else(syntax)?;
            if i == args.len() {
                return Err(syntax());
            }
            config.ignore_value = number(&args[i], false)
                .filter(|v| *v >= 0.0)
                .ok_or_else(syntax)?;
            i += 1;
        } else {
            return Err(syntax());
        }
    }
    Ok(CreateOptions {
        config,
        on_duplicate: duplicate,
        time,
    })
}

#[derive(Default)]
struct Query {
    lo: i64,
    hi: i64,
    latest: bool,
    timestamps: Option<BTreeSet<i64>>,
    values: Option<(f64, f64)>,
    count: Option<usize>,
    align: i64,
    aggs: Vec<Agg>,
    duration: i64,
    bucket_time: u8,
    empty: bool,
    labels: Option<Vec<Bytes>>,
    all_labels: bool,
    exclude_empty: bool,
}
fn query_options(args: &[Bytes], start: usize, multi: bool) -> Result<(Query, usize), Resp> {
    let mut q = Query::default();
    let mut aggregation_option = false;
    q.lo = if args[start].as_ref() == b"-" {
        0
    } else {
        timestamp(&args[start]).ok_or_else(syntax)?
    };
    q.hi = if args[start + 1].as_ref() == b"+" {
        i64::MAX
    } else {
        timestamp(&args[start + 1]).ok_or_else(syntax)?
    };
    let mut i = start + 2;
    while i < args.len() {
        let o = &args[i];
        i += 1;
        if multi && eq(o, b"FILTER") {
            i -= 1;
            break;
        }
        if eq(o, b"EMPTY") || eq(o, b"ALIGN") || eq(o, b"BUCKETTIMESTAMP") {
            aggregation_option = true;
        }
        if eq(o, b"LATEST") {
            q.latest = true;
        } else if eq(o, b"EMPTY") {
            q.empty = true;
        } else if eq(o, b"EXCLUDEEMPTY") && multi {
            q.exclude_empty = true;
        } else if eq(o, b"WITHLABELS") && multi {
            if q.labels.is_some() {
                return Err(syntax());
            }
            q.all_labels = true;
        } else if eq(o, b"SELECTED_LABELS") && multi {
            if q.all_labels {
                return Err(syntax());
            }
            let begin = i;
            while i < args.len() && !is_query_keyword(&args[i]) {
                i += 1;
            }
            if begin == i {
                return Err(syntax());
            }
            q.labels = Some(args[begin..i].to_vec());
        } else if eq(o, b"FILTER_BY_TS") {
            let mut ts = BTreeSet::new();
            while i < args.len() {
                if let Some(t) = timestamp(&args[i]) {
                    ts.insert(t);
                    i += 1;
                } else {
                    break;
                }
            }
            if ts.is_empty() {
                return Err(syntax());
            }
            q.timestamps = Some(ts);
        } else if eq(o, b"FILTER_BY_VALUE") {
            if i + 1 >= args.len() {
                return Err(syntax());
            }
            let lo = filter_number(&args[i]).ok_or_else(syntax)?;
            let hi = filter_number(&args[i + 1]).ok_or_else(syntax)?;
            if lo > hi {
                return Err(syntax());
            }
            q.values = Some((lo, hi));
            i += 2;
        } else if eq(o, b"COUNT") {
            if i == args.len() {
                return Err(syntax());
            }
            q.count = Some(timestamp(&args[i]).filter(|n| *n > 0).ok_or_else(syntax)? as usize);
            i += 1;
        } else if eq(o, b"ALIGN") {
            if i == args.len() {
                return Err(syntax());
            }
            q.align = if eq(&args[i], b"START") || args[i].as_ref() == b"-" {
                if args[start].as_ref() == b"-" {
                    return Err(syntax());
                }
                q.lo
            } else if eq(&args[i], b"END") || args[i].as_ref() == b"+" {
                if args[start + 1].as_ref() == b"+" {
                    return Err(syntax());
                }
                q.hi
            } else {
                timestamp(&args[i]).ok_or_else(syntax)?
            };
            i += 1;
        } else if eq(o, b"AGGREGATION") {
            if i + 1 >= args.len() {
                return Err(syntax());
            }
            q.aggs = args[i]
                .split(|b| *b == b',')
                .map(|a| Agg::parse(a).ok_or_else(syntax))
                .collect::<Result<_, _>>()?;
            q.duration = timestamp(&args[i + 1])
                .filter(|n| *n > 0)
                .ok_or_else(syntax)?;
            i += 2;
        } else if eq(o, b"BUCKETTIMESTAMP") {
            if i == args.len() {
                return Err(syntax());
            }
            q.bucket_time = if eq(&args[i], b"START") || args[i].as_ref() == b"-" {
                0
            } else if eq(&args[i], b"END") || args[i].as_ref() == b"+" {
                1
            } else if eq(&args[i], b"MID") || args[i].as_ref() == b"~" {
                2
            } else {
                return Err(syntax());
            };
            i += 1;
        } else {
            return Err(syntax());
        }
    }
    if q.aggs.is_empty() && aggregation_option {
        return Err(syntax());
    }
    Ok((q, i))
}
fn is_query_keyword(s: &[u8]) -> bool {
    [
        b"LATEST".as_slice(),
        b"FILTER",
        b"FILTER_BY_TS",
        b"FILTER_BY_VALUE",
        b"COUNT",
        b"ALIGN",
        b"AGGREGATION",
        b"BUCKETTIMESTAMP",
        b"EMPTY",
        b"WITHLABELS",
        b"SELECTED_LABELS",
        b"EXCLUDEEMPTY",
    ]
    .iter()
    .any(|k| eq(s, k))
}
struct Filter {
    label: Bytes,
    negative: bool,
    values: Vec<Bytes>,
    existence: bool,
}
fn filters(args: &[Bytes]) -> Result<Vec<Filter>, Resp> {
    let mut out = Vec::new();
    let mut positive = false;
    for s in args {
        let Some(p) = s.iter().position(|b| *b == b'=') else {
            return Err(syntax());
        };
        let negative = p > 0 && s[p - 1] == b'!';
        let label = &s[..p - usize::from(negative)];
        if label.is_empty() {
            return Err(syntax());
        }
        let mut v = &s[p + 1..];
        if v.len() >= 2
            && ((v[0] == b'\"' && v[v.len() - 1] == b'\"')
                || (v[0] == b'\'' && v[v.len() - 1] == b'\''))
        {
            v = &v[1..v.len() - 1];
        }
        let existence = v.is_empty();
        let values = if v.first() == Some(&b'(') {
            if v.last() != Some(&b')') {
                return Err(syntax());
            }
            v[1..v.len() - 1]
                .split(|b| *b == b',')
                .map(Bytes::copy_from_slice)
                .collect()
        } else {
            vec![Bytes::copy_from_slice(v)]
        };
        if !negative && !existence {
            positive = true;
        }
        out.push(Filter {
            label: Bytes::copy_from_slice(label),
            negative,
            values,
            existence,
        });
    }
    if !positive {
        return Err(error("TSDB: please provide at least one matcher"));
    }
    Ok(out)
}
fn matches(c: &Config, f: &[Filter]) -> bool {
    f.iter().all(|f| {
        let v = c.labels.get(&f.label);
        if f.existence {
            v.is_some() == f.negative
        } else {
            let found = v.is_some_and(|v| f.values.contains(v));
            found != f.negative
        }
    })
}
fn query_rows(
    s: &TimeSeriesValue,
    q: &Query,
    reverse: bool,
    limit: usize,
) -> Result<Vec<(i64, Vec<f64>)>, Resp> {
    if q.lo > q.hi {
        return Ok(Vec::new());
    }
    let mut budget = super::ReplyBudget::new(limit);
    let mut out = Vec::new();
    let passes = |(t, v): &(i64, f64)| {
        q.timestamps.as_ref().is_none_or(|ts| ts.contains(t))
            && q.values.is_none_or(|(lo, hi)| *v >= lo && *v <= hi)
    };
    if q.aggs.is_empty() {
        let count = q.count.unwrap_or(usize::MAX);
        let rows = s.iter(q.lo, q.hi);
        let mut selected = Vec::new();
        if reverse {
            for p in rows.rev().filter(passes).take(count) {
                budget.charge(3, 32)?;
                selected.push(p);
            }
        } else {
            for p in rows.filter(passes).take(count) {
                budget.charge(3, 32)?;
                selected.push(p);
            }
        }
        if q.latest
            && let Some(p) = s
                .pending
                .filter(|p| p.0 >= q.lo && p.0 <= q.hi && passes(p))
        {
            selected.retain(|s| s.0 != p.0);
            selected.push(p);
            selected.sort_by_key(|p| p.0);
            if reverse {
                selected.reverse();
            }
            selected.truncate(count);
        }
        return Ok(selected.into_iter().map(|(t, v)| (t, vec![v])).collect());
    }
    let mut selected = s.rows(q.lo, q.hi);
    if q.latest
        && let Some(p) = s.pending.filter(|p| p.0 >= q.lo && p.0 <= q.hi)
    {
        match selected.binary_search_by_key(&p.0, |s| s.0) {
            Ok(i) => selected[i] = p,
            Err(i) => selected.insert(i, p),
        }
    }
    selected.retain(passes);
    let mut groups: BTreeMap<i64, Vec<(i64, f64)>> = BTreeMap::new();
    for p in selected {
        groups
            .entry(bucket(p.0, q.duration, q.align))
            .or_default()
            .push(p);
    }
    if q.empty
        && let (Some(first), Some(last)) = (
            s.first().or(if q.latest { s.pending } else { None }),
            if q.latest {
                s.last_with_pending()
            } else {
                s.last()
            },
        )
    {
        let first = first.0.max(q.lo);
        let last = last.0.min(q.hi);
        if first <= last {
            let (begin, end) = (
                bucket(first, q.duration, q.align),
                bucket(last, q.duration, q.align),
            );
            let mut b = if reverse { end } else { begin };
            let mut empty_budget = super::ReplyBudget::new(limit);
            for _ in 0..q.count.unwrap_or(usize::MAX) {
                if let std::collections::btree_map::Entry::Vacant(e) = groups.entry(b) {
                    empty_budget.charge(2 + q.aggs.len(), 32 * q.aggs.len())?;
                    e.insert(Vec::new());
                }
                if (!reverse && b == end) || (reverse && b == begin) {
                    break;
                }
                let next = if reverse {
                    b.saturating_sub(q.duration).max(0)
                } else {
                    bucket_end(b, q.duration, q.align)
                };
                if next == b || next < begin || next > end {
                    break;
                }
                b = next;
            }
        }
    }
    let mut rows = groups.into_iter();
    for _ in 0..q.count.unwrap_or(usize::MAX) {
        let Some((b, rows)) = (if reverse {
            rows.next_back()
        } else {
            rows.next()
        }) else {
            break;
        };
        budget.charge(2 + q.aggs.len(), 32 * q.aggs.len())?;
        let end = bucket_end(b, q.duration, q.align);
        let before = s.before(b);
        let mut after = s.after(end);
        if q.latest
            && let Some(p) = s.pending.filter(|p| p.0 >= end && !p.1.is_nan())
            && after.is_none_or(|a| p.0 < a.0)
        {
            after = Some(p);
        }
        let vals = q
            .aggs
            .iter()
            .map(|a| aggregate(*a, &rows, b, end, before, after))
            .collect();
        // Redis labels END/MID relative to the normalized start, even when the
        // first bucket's actual right edge precedes start + duration.
        // https://github.com/RedisTimeSeries/RedisTimeSeries/blob/master/src/filter_iterator.c
        let t = match q.bucket_time {
            1 => b.saturating_add(q.duration),
            2 => b.saturating_add(q.duration / 2),
            _ => b,
        };
        out.push((t, vals));
    }
    Ok(out)
}
#[cfg(test)]
fn rows_reply(rows: Vec<(i64, Vec<f64>)>) -> Resp {
    array(
        rows.into_iter()
            .map(|(t, v)| {
                let mut r = vec![Resp::Integer(t)];
                r.extend(v.into_iter().map(float));
                array(r)
            })
            .collect(),
    )
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "ts.create",
        -2,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::ts_create,
    ),
    CommandSpec::handler("ts.alter", -2, &["write"], (1, 1, 1), Engine::ts_alter),
    CommandSpec::handler(
        "ts.add",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::ts_add,
    ),
    CommandSpec::handler(
        "ts.madd",
        -4,
        &["write", "denyoom"],
        (1, -1, 3),
        Engine::ts_madd,
    ),
    CommandSpec::handler(
        "ts.incrby",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::ts_incrby,
    ),
    CommandSpec::handler(
        "ts.decrby",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::ts_decrby,
    ),
    CommandSpec::handler("ts.del", 4, &["write"], (1, 1, 1), Engine::ts_del),
    CommandSpec::handler("ts.get", -2, &["readonly"], (1, 1, 1), Engine::ts_get).max(3),
    CommandSpec::handler("ts.range", -4, &["readonly"], (1, 1, 1), Engine::ts_range),
    CommandSpec::handler(
        "ts.revrange",
        -4,
        &["readonly"],
        (1, 1, 1),
        Engine::ts_revrange,
    ),
    CommandSpec::handler("ts.mget", -3, &["readonly"], (0, 0, 0), Engine::ts_mget),
    CommandSpec::handler("ts.mrange", -5, &["readonly"], (0, 0, 0), Engine::ts_mrange),
    CommandSpec::handler(
        "ts.mrevrange",
        -5,
        &["readonly"],
        (0, 0, 0),
        Engine::ts_mrevrange,
    ),
    CommandSpec::handler("ts.info", -2, &["readonly"], (1, 1, 1), Engine::ts_info).max(3),
    CommandSpec::handler(
        "ts.queryindex",
        -2,
        &["readonly"],
        (0, 0, 0),
        Engine::ts_queryindex,
    ),
    CommandSpec::handler(
        "ts.createrule",
        -6,
        &["write"],
        (1, 2, 1),
        Engine::ts_createrule,
    )
    .max(7),
    CommandSpec::handler(
        "ts.deleterule",
        3,
        &["write"],
        (1, 2, 1),
        Engine::ts_deleterule,
    ),
    CommandSpec::handler("ts.nrange", -5, &["readonly"], (0, 0, 0), Engine::ts_nrange),
    CommandSpec::handler(
        "ts.nrevrange",
        -5,
        &["readonly"],
        (0, 0, 0),
        Engine::ts_nrevrange,
    ),
    CommandSpec::handler(
        "ts.querylabels",
        -2,
        &["readonly"],
        (0, 0, 0),
        Engine::ts_querylabels,
    ),
    CommandSpec::handler(
        "ts.read",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::ts_read_immediate,
    ),
];

impl Engine {
    fn ts_ref(&self, key: &Bytes, now: i64) -> Result<&TimeSeriesValue, Resp> {
        let Some(e) = self
            .entries
            .get(key)
            .filter(|e| e.expires_at.is_none_or(|t| t > now))
        else {
            return Err(error(MISSING));
        };
        match &e.value {
            Value::Typed(d) => match d.as_ref() {
                Data::TimeSeries(s) => Ok(s),
                _ => Err(wrong_type()),
            },
            _ => Err(wrong_type()),
        }
    }
    fn ts_apply(&mut self, key: &Bytes, e: Effect, now: i64) -> Result<(), Resp> {
        let value = self.ts_ref(key, now)?;
        let same = |a: Option<(i64, f64)>, b: Option<(i64, f64)>| match (a, b) {
            (None, None) => true,
            (Some(a), Some(b)) => a.0 == b.0 && a.1.to_bits() == b.1.to_bits(),
            _ => false,
        };
        match &e {
            Effect::Delete(lo, hi) if value.iter(*lo, *hi).next().is_none() => return Ok(()),
            Effect::Set(t, v, trim)
                if value
                    .get(*t)
                    .is_some_and(|old| old.to_bits() == v.to_bits())
                    && (!trim || value.first().is_none_or(|p| p.0 >= value.cutoff())) =>
            {
                return Ok(());
            }
            Effect::Pending(p) if same(value.pending, *p) => return Ok(()),
            Effect::Source(source) if value.source == *source => return Ok(()),
            Effect::Unrule(dest) if !value.rules.contains_key(dest) => return Ok(()),
            Effect::Previous(dest, p)
                if value.rules.get(dest).is_some_and(|r| same(r.previous, *p)) =>
            {
                return Ok(());
            }
            _ => {}
        }
        let payload = if self.journal_enabled
            || matches!(&e, Effect::Config(_) | Effect::Rule(..) | Effect::Source(_))
        {
            encode(&e).map_err(|_| error("ERR time series effect exceeds maximum size"))?
        } else {
            Bytes::new()
        };
        if let Some(Entry {
            value: Value::Typed(d),
            ..
        }) = self.entries.get_mut(key)
        {
            if let Data::TimeSeries(s) = d.as_mut() {
                s.apply_decoded(e);
            } else {
                return Err(wrong_type());
            }
        } else {
            return Err(error(MISSING));
        }
        if payload.is_empty() {
            if self.ready_tracking {
                self.ready_keys.push((self.active_database, key.clone()));
            }
        } else {
            self.record_typed(key, KIND, payload, now);
        }
        Ok(())
    }
    fn ts_insert(&mut self, key: &Bytes, c: Config, now: i64) -> Result<(), Resp> {
        // Config is validated before installation; initialization is the first WAL effect.
        let payload = encode(&Effect::Config(c.clone()))
            .map_err(|_| error("ERR time series metadata exceeds maximum size"))?;
        let key = self.own_key(key);
        self.write_entry(
            key.clone(),
            Entry {
                value: Value::Typed(Box::new(Data::TimeSeries(TimeSeriesValue::new(c)))),
                expires_at: None,
                access: crate::engine::AccessMetadata::default(),
            },
            now,
        );
        self.record_typed(&key, KIND, payload, now);
        Ok(())
    }
    fn ts_create(&mut self, args: &[Bytes], now: i64) -> Resp {
        let c = match create_options(args, 2, Config::default(), now, 0) {
            Ok(c) => c.config,
            Err(e) => return e,
        };
        if self.value(&args[1], now).is_some() {
            return error("TSDB: key already exists");
        }
        match self.ts_insert(&args[1], c, now) {
            Ok(()) => Resp::Simple("OK"),
            Err(e) => e,
        }
    }
    fn ts_alter(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.discard_expired(&args[1], now);
        let old = match self.ts_ref(&args[1], now) {
            Ok(s) => s.config.clone(),
            Err(e) => return e,
        };
        let c = match create_options(args, 2, old, now, 1) {
            Ok(c) => c.config,
            Err(e) => return e,
        };
        match self.ts_apply(&args[1], Effect::Config(c), now) {
            Ok(()) => Resp::Simple("OK"),
            Err(e) => e,
        }
    }
    fn ts_add(&mut self, args: &[Bytes], now: i64) -> Resp {
        let t = if args[2].as_ref() == b"*" {
            now.max(0)
        } else {
            match timestamp(&args[2]) {
                Some(t) => t,
                None => return error("TSDB: invalid timestamp"),
            }
        };
        let Some(v) = number(&args[3], true) else {
            return error("TSDB: invalid value");
        };
        let opts = match create_options(args, 4, Config::default(), now, 2) {
            Ok(o) => o,
            Err(e) => return e,
        };
        self.discard_expired(&args[1], now);
        if !self.entries.contains_key(&args[1])
            && let Err(e) = self.ts_insert(&args[1], opts.config, now)
        {
            return e;
        }
        match self.ts_resolve(&args[1], t, v, opts.on_duplicate, now) {
            Ok((t, Some(v))) => match self.ts_write(&args[1], t, v, now) {
                Ok(()) => Resp::Integer(t),
                Err(e) => e,
            },
            Ok((t, None)) => Resp::Integer(t),
            Err(e) => e,
        }
    }
    fn ts_resolve(
        &self,
        key: &Bytes,
        t: i64,
        v: f64,
        policy: Option<Policy>,
        now: i64,
    ) -> Result<(i64, Option<f64>), Resp> {
        let s = self.ts_ref(key, now)?;
        if t < s.cutoff() {
            return Err(error("TSDB: Timestamp is older than retention"));
        }
        let p = policy.or(s.config.policy).unwrap_or(Policy::Block);
        if s.source
            .as_ref()
            .is_none_or(|src| !self.ts_rule_live(src, key, now))
            && s.config.policy == Some(Policy::Last)
            && let Some((last, x)) = s.last()
            && t >= last
            && t - last <= s.config.ignore_time
            && (v - x).abs() <= s.config.ignore_value
        {
            return Ok((last, None));
        }
        let value = if let Some(old) = s.get(t) {
            p.resolve(old, v)?
        } else {
            v
        };
        if value.is_infinite() {
            return Err(error("TSDB: invalid value"));
        }
        if s.get(t).is_some_and(|old| old.to_bits() == value.to_bits()) {
            return Ok((t, None));
        }
        Ok((t, Some(value)))
    }
    fn ts_madd(&mut self, args: &[Bytes], now: i64) -> Resp {
        if !(args.len() - 1).is_multiple_of(3) {
            return syntax();
        }
        if let Err(e) = super::ReplyBudget::new(self.reply_limit).charge((args.len() - 1) / 3, 0) {
            return e;
        }
        let mut replies = Vec::with_capacity((args.len() - 1) / 3);
        for row in args[1..].as_chunks::<3>().0 {
            let reply: Result<Resp, Resp> = (|| {
                let t = if row[1].as_ref() == b"*" {
                    now.max(0)
                } else {
                    timestamp(&row[1]).ok_or_else(|| error("TSDB: invalid timestamp"))?
                };
                let v = number(&row[2], true).ok_or_else(|| error("TSDB: invalid value"))?;
                self.discard_expired(&row[0], now);
                match self.ts_resolve(&row[0], t, v, None, now)? {
                    (t, Some(v)) => {
                        self.ts_write(&row[0], t, v, now)?;
                        Ok(Resp::Integer(t))
                    }
                    (t, None) => Ok(Resp::Integer(t)),
                }
            })();
            replies.push(match reply {
                Ok(r) => r,
                Err(e) => e,
            });
        }
        array(replies)
    }
    fn ts_incrby(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_increment(args, now, false)
    }
    fn ts_decrby(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_increment(args, now, true)
    }
    fn ts_increment(&mut self, args: &[Bytes], now: i64, negative: bool) -> Resp {
        let Some(mut delta) = number(&args[2], false) else {
            return error("TSDB: invalid value");
        };
        if negative {
            delta = -delta;
        }
        let opts = match create_options(args, 3, Config::default(), now, 3) {
            Ok(o) => o,
            Err(e) => return e,
        };
        let t = opts.time.unwrap_or(now.max(0));
        self.discard_expired(&args[1], now);
        let last = if self.entries.contains_key(&args[1]) {
            match self.ts_ref(&args[1], now) {
                Ok(s) => s.last(),
                Err(e) => return e,
            }
        } else {
            None
        };
        if last.is_some_and(|p| t < p.0) {
            return error(
                "TSDB: timestamp must be equal to or higher than the maximum existing timestamp",
            );
        }
        let v = last.map_or(delta, |p| p.1 + delta);
        if !v.is_finite() {
            return error("TSDB: invalid value");
        }
        if !self.entries.contains_key(&args[1])
            && let Err(e) = self.ts_insert(&args[1], opts.config, now)
        {
            return e;
        }
        match self.ts_resolve(&args[1], t, v, Some(Policy::Last), now) {
            Ok((t, Some(v))) => match self.ts_write(&args[1], t, v, now) {
                Ok(()) => Resp::Integer(t),
                Err(e) => e,
            },
            Ok((t, None)) => Resp::Integer(t),
            Err(e) => e,
        }
    }
}

impl Engine {
    fn ts_rule_live(&self, source: &Bytes, dest: &Bytes, now: i64) -> bool {
        self.ts_ref(dest, now)
            .is_ok_and(|d| d.source.as_ref() == Some(source))
            && self
                .ts_ref(source, now)
                .is_ok_and(|s| s.rules.contains_key(dest))
    }
    pub(crate) fn timeseries_key_exists(&self, key: &Bytes, now: i64) -> bool {
        self.ts_ref(key, now).is_ok()
    }
    fn ts_write(&mut self, key: &Bytes, t: i64, v: f64, now: i64) -> Result<(), Resp> {
        let mut closed = Vec::new();
        let mut state = Vec::new();
        let mut late = Vec::new();
        let mut stale = Vec::new();
        let mut previous_changes = Vec::new();
        {
            let s = self.ts_ref(key, now)?;
            for (dest, r) in &s.rules {
                if !self.ts_rule_live(key, dest, now) {
                    stale.push(dest.clone());
                    continue;
                }
                let b = bucket(t, r.duration, r.align);
                if let Some(current) = r.current {
                    if r.agg == Agg::Twa {
                        for neighbor in [s.before(t), t.checked_add(1).and_then(|t| s.after(t))]
                            .into_iter()
                            .flatten()
                        {
                            let nb = bucket(neighbor.0, r.duration, r.align);
                            if nb < current && self.ts_ref(dest, now)?.get(nb).is_some() {
                                late.push((dest.clone(), r.agg, r.duration, nb));
                            }
                        }
                        if t < current {
                            previous_changes.push((dest.clone(), current, r.previous));
                        }
                    }
                    if b < current {
                        late.push((dest.clone(), r.agg, r.duration, b));
                        continue;
                    }
                    if b > current {
                        if !r.active.is_empty() {
                            closed.push((dest.clone(), current, r.value(Some((t, v)))));
                        }
                        let previous = r
                            .stats
                            .finite
                            .last_key_value()
                            .map(|(&t, &v)| (t, v))
                            .or(r.previous);
                        state.push((dest.clone(), b, previous));
                        continue;
                    }
                }
                state.push((dest.clone(), b, r.previous));
            }
        }
        // Rule endpoints were checked before applying this resolved sample delta.
        for (dest, b, value) in closed {
            self.ts_apply(&dest, Effect::Set(b, value, true), now)?;
        }
        for dest in stale {
            self.ts_apply(key, Effect::Unrule(dest), now)?;
        }
        self.ts_apply(key, Effect::Set(t, v, false), now)?;
        for (dest, b, p) in state {
            self.ts_apply(key, Effect::Active(dest.clone(), b, t, Some(v), p), now)?;
            let value = self.ts_ref(key, now)?.rules.get(&dest).unwrap().value(None);
            self.ts_apply(&dest, Effect::Pending(Some((b, value))), now)?;
        }
        for (dest, current, old) in previous_changes {
            let fresh = self.ts_ref(key, now)?.before(current);
            // Retention may evict the cached predecessor without invalidating
            // interpolation. An explicit NaN overwrite of that same sample does.
            let old = old.filter(|p| p.0 != t || !v.is_nan());
            let previous = match (old, fresh) {
                (Some(a), Some(b)) => Some(if b.0 >= a.0 { b } else { a }),
                (a, b) => a.or(b),
            };
            self.ts_apply(key, Effect::Previous(dest.clone(), previous), now)?;
            let r = &self.ts_ref(key, now)?.rules[&dest];
            let pending = (!r.active.is_empty()).then(|| (current, r.value(None)));
            self.ts_apply(&dest, Effect::Pending(pending), now)?;
        }
        late.sort_by(|a, b| a.0.cmp(&b.0).then(a.3.cmp(&b.3)));
        late.dedup_by(|a, b| a.0 == b.0 && a.3 == b.3);
        for (dest, a, d, b) in late {
            let s = self.ts_ref(key, now)?;
            let end = bucket_end(b, d, s.rules[&dest].align);
            let rows = s.rows(b, end.saturating_sub(1));
            if rows.is_empty() {
                continue;
            }
            let before = s.before(b);
            let after = s.after(end);
            let value = aggregate(a, &rows, b, end, before, after);
            self.ts_apply(&dest, Effect::Set(b, value, true), now)?;
        }
        let cut = self.ts_ref(key, now)?.cutoff();
        if cut > 0 {
            let expired = self
                .ts_ref(key, now)?
                .iter(0, cut - 1)
                .next_back()
                .map(|p| p.0);
            if expired.is_some() {
                self.ts_apply(key, Effect::Prune(cut, expired), now)?;
            }
        }
        Ok(())
    }
    fn ts_createrule(&mut self, args: &[Bytes], now: i64) -> Resp {
        if !eq(&args[3], b"AGGREGATION") {
            return syntax();
        }
        let Some(a) = Agg::parse(&args[4]) else {
            return syntax();
        };
        let Some(d) = timestamp(&args[5]).filter(|d| *d > 0) else {
            return syntax();
        };
        let align = if args.len() == 7 {
            match timestamp(&args[6]) {
                Some(t) => t,
                None => return syntax(),
            }
        } else {
            0
        };
        if args[1] == args[2] {
            return error("TSDB: source and destination keys are the same");
        }
        self.discard_expired(&args[1], now);
        self.discard_expired(&args[2], now);
        let source = match self.ts_ref(&args[1], now) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if source
            .source
            .as_ref()
            .is_some_and(|k| self.ts_rule_live(k, &args[1], now))
        {
            return error("TSDB: source key is a compaction");
        }
        let dest = match self.ts_ref(&args[2], now) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if dest
            .source
            .as_ref()
            .is_some_and(|k| self.ts_rule_live(k, &args[2], now))
            || dest
                .rules
                .keys()
                .any(|k| self.ts_rule_live(&args[2], k, now))
        {
            return error("TSDB: destination key already has a compaction rule");
        }
        if self.ts_rule_live(&args[1], &args[2], now) {
            return error("TSDB: compaction rule already exists");
        }
        // An open-bucket delta has a larger envelope than its rule descriptor.
        // Validate both endpoints before installing either half of the rule.
        if args[2].len() > super::typed::codec::MAX_EFFECT_BYTES - 48
            || args[1].len() > super::typed::codec::MAX_EFFECT_BYTES - 7
        {
            return error("ERR time series effect exceeds maximum size");
        }
        if let Err(e) = self.ts_apply(&args[1], Effect::Rule(args[2].clone(), a, d, align), now) {
            return e;
        }
        if let Err(e) = self.ts_apply(&args[2], Effect::Source(Some(args[1].clone())), now) {
            return e;
        }
        if let Err(e) = self.ts_apply(&args[2], Effect::Pending(None), now) {
            return e;
        }
        Resp::Simple("OK")
    }
    fn ts_deleterule(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.discard_expired(&args[1], now);
        self.discard_expired(&args[2], now);
        if let Err(e) = self.ts_ref(&args[1], now) {
            return e;
        }
        if let Err(e) = self.ts_ref(&args[2], now) {
            return e;
        }
        if !self.ts_rule_live(&args[1], &args[2], now) {
            return error("TSDB: compaction rule does not exist");
        }
        if let Err(e) = self.ts_apply(&args[1], Effect::Unrule(args[2].clone()), now) {
            return e;
        }
        if let Err(e) = self.ts_apply(&args[2], Effect::Source(None), now) {
            return e;
        }
        if let Err(e) = self.ts_apply(&args[2], Effect::Pending(None), now) {
            return e;
        }
        Resp::Simple("OK")
    }
    fn ts_del(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(lo) = timestamp(&args[2]) else {
            return syntax();
        };
        let Some(hi) = timestamp(&args[3]) else {
            return syntax();
        };
        if hi < lo {
            return syntax();
        }
        self.discard_expired(&args[1], now);
        let s = match self.ts_ref(&args[1], now) {
            Ok(s) => s,
            Err(e) => return e,
        };
        if lo < s.cutoff() {
            return error("TSDB: Timestamp is older than retention");
        }
        let deleted = s.rows(lo, hi);
        let mut buckets = BTreeSet::new();
        let mut active = Vec::new();
        let mut previous_changes = Vec::new();
        if deleted.is_empty() {
            return Resp::Integer(0);
        }
        let last_before = s.last().unwrap().0;
        for (dest, r) in &s.rules {
            if !self.ts_rule_live(&args[1], dest, now) {
                continue;
            }
            if r.agg == Agg::Twa
                && let Some(current) = r.current
            {
                previous_changes.push((dest.clone(), current));
                for neighbor in [s.before(lo), hi.checked_add(1).and_then(|t| s.after(t))]
                    .into_iter()
                    .flatten()
                {
                    let b = bucket(neighbor.0, r.duration, r.align);
                    if b < current && self.ts_ref(dest, now).is_ok_and(|s| s.get(b).is_some()) {
                        if s.expired
                            .is_some_and(|t| bucket(t, r.duration, r.align) >= b)
                        {
                            return error(
                                "TSDB: cannot delete samples with expired compaction bucket originals",
                            );
                        }
                        buckets.insert((dest.clone(), r.agg as u8, r.duration, b));
                    }
                }
            }
            for &(t, _) in &deleted {
                let b = bucket(t, r.duration, r.align);
                if s.expired
                    .is_some_and(|t| bucket(t, r.duration, r.align) >= b)
                {
                    return error(
                        "TSDB: cannot delete samples with expired compaction bucket originals",
                    );
                }
                if r.current == Some(b) {
                    active.push((dest.clone(), b, t, r.previous));
                } else {
                    buckets.insert((dest.clone(), r.agg as u8, r.duration, b));
                }
            }
        }
        if let Err(e) = self.ts_apply(&args[1], Effect::Delete(lo, hi), now) {
            return e;
        }
        for (dest, b, t, p) in active {
            if let Err(e) =
                self.ts_apply(&args[1], Effect::Active(dest.clone(), b, t, None, p), now)
            {
                return e;
            }
            let r = &self.ts_ref(&args[1], now).unwrap().rules[&dest];
            let pending = (!r.active.is_empty()).then(|| (b, r.value(None)));
            if let Err(e) = self.ts_apply(&dest, Effect::Pending(pending), now) {
                return e;
            }
        }
        for (dest, current) in previous_changes {
            let previous = self.ts_ref(&args[1], now).unwrap().before(current);
            if let Err(e) = self.ts_apply(&args[1], Effect::Previous(dest.clone(), previous), now) {
                return e;
            }
            let r = &self.ts_ref(&args[1], now).unwrap().rules[&dest];
            let pending = (!r.active.is_empty()).then(|| (current, r.value(None)));
            if let Err(e) = self.ts_apply(&dest, Effect::Pending(pending), now) {
                return e;
            }
        }
        for (dest, a, d, b) in buckets {
            let s = self.ts_ref(&args[1], now).unwrap();
            let end = bucket_end(b, d, s.rules[&dest].align);
            let rows = s.rows(b, end.saturating_sub(1));
            let effect = if rows.is_empty() {
                Effect::Delete(b, b)
            } else {
                let before = s.before(b);
                let after = s.after(end);
                Effect::Set(
                    b,
                    aggregate(Agg::decode(a).unwrap(), &rows, b, end, before, after),
                    true,
                )
            };
            if let Err(e) = self.ts_apply(&dest, effect, now) {
                return e;
            }
        }
        // Redis reopens the most recent destination bucket when deletion empties
        // the source's latest bucket. This is a state transition: remove the
        // closed sample, reset the cursor, and rebuild the accumulator so later
        // writes (and replay/checkpoint recovery) cannot treat it as closed.
        let s = self.ts_ref(&args[1], now).unwrap();
        let reopen: Vec<_> = s
            .rules
            .iter()
            .filter_map(|(dest, r)| {
                let current = r.current?;
                (self.ts_rule_live(&args[1], dest, now)
                    && deleted.last().is_some_and(|p| p.0 >= current)
                    && s.iter(
                        current,
                        bucket_end(current, r.duration, r.align).saturating_sub(1),
                    )
                    .next()
                    .is_none())
                .then(|| (dest.clone(), r.agg, r.duration, r.align))
            })
            .collect();
        for (dest, agg, duration, align) in reopen {
            let closed = last_before
                .checked_sub(1)
                .and_then(|end| self.ts_ref(&dest, now).unwrap().iter(0, end).next_back());
            let b = bucket(closed.map_or(0, |p| p.0), duration, align);
            let s = self.ts_ref(&args[1], now).unwrap();
            let rows = s.rows(b, bucket_end(b, duration, align).saturating_sub(1));
            let previous = s.before(b);
            if let Some((t, _)) = closed
                && let Err(e) = self.ts_apply(&dest, Effect::Delete(t, t), now)
            {
                return e;
            }
            // Reinstalling the descriptor durably clears current/active/stats.
            if let Err(e) = self.ts_apply(
                &args[1],
                Effect::Rule(dest.clone(), agg, duration, align),
                now,
            ) {
                return e;
            }
            for (t, v) in rows {
                if let Err(e) = self.ts_apply(
                    &args[1],
                    Effect::Active(dest.clone(), b, t, Some(v), previous),
                    now,
                ) {
                    return e;
                }
            }
            let r = &self.ts_ref(&args[1], now).unwrap().rules[&dest];
            let pending = r.current.map(|b| (b, r.value(None)));
            if let Err(e) = self.ts_apply(&dest, Effect::Pending(pending), now) {
                return e;
            }
        }
        Resp::Integer(deleted.len() as i64)
    }
    fn ts_get(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() == 3 && !eq(&args[2], b"LATEST") {
            return syntax();
        }
        self.discard_expired(&args[1], now);
        match self.ts_ref(&args[1], now) {
            Ok(s) => {
                let latest = args.len() == 3
                    && s.source.as_ref().is_some_and(|src| {
                        self.command_key_readable(src) && self.ts_rule_live(src, &args[1], now)
                    });
                let last = if latest {
                    s.last_with_pending()
                } else {
                    s.last()
                };
                last.map_or_else(
                    || array(Vec::new()),
                    |p| {
                        sample_pair(
                            p.0,
                            p.1,
                            self.command_version == crate::protocol::Version::Resp3,
                        )
                    },
                )
            }
            Err(e) => e,
        }
    }
    fn ts_range(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_range_impl(args, now, false)
    }
    fn ts_revrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_range_impl(args, now, true)
    }
    fn ts_range_impl(&mut self, args: &[Bytes], now: i64, reverse: bool) -> Resp {
        let (mut q, _) = match query_options(args, 2, false) {
            Ok(q) => q,
            Err(e) => return e,
        };
        self.discard_expired(&args[1], now);
        let s = match self.ts_ref(&args[1], now) {
            Ok(s) => s,
            Err(e) => return e,
        };
        q.latest &= s.source.as_ref().is_some_and(|src| {
            self.command_key_readable(src) && self.ts_rule_live(src, &args[1], now)
        });
        match query_rows(s, &q, reverse, self.reply_limit) {
            Ok(rows) => sample_reply_borrowed(
                &rows,
                self.command_version == crate::protocol::Version::Resp3,
            ),
            Err(e) => e,
        }
    }
}

impl Engine {
    fn ts_keys_matching(&self, f: &[Filter], now: i64) -> Vec<Bytes> {
        let mut keys: Vec<_> = self
            .entries
            .keys()
            .filter_map(|key| {
                self.ts_ref(key, now)
                    .ok()
                    .filter(|s| matches(&s.config, f))
                    .map(|_| key.clone())
            })
            .collect();
        keys.sort();
        keys
    }
    fn ts_queryindex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let f = match filters(&args[1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        match super::bounded_replies(
            self.ts_keys_matching(&f, now).into_iter().map(bytes),
            self.reply_limit,
        ) {
            Resp::Array(Some(r)) => Resp::Set(r),
            e => e,
        }
    }
    fn ts_querylabels(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (label, i) = if eq(&args[1], b"LABELS") {
            (None, 2)
        } else if eq(&args[1], b"VALUES") && args.len() > 2 {
            (Some(&args[2]), 3)
        } else {
            return syntax();
        };
        let f = if i == args.len() {
            Vec::new()
        } else {
            if !eq(&args[i], b"FILTER") {
                return syntax();
            }
            match filters(&args[i + 1..]) {
                Ok(f) => f,
                Err(e) => return e,
            }
        };
        let mut result = BTreeSet::new();
        for key in self.ts_keys_matching(&f, now) {
            if !self.command_key_readable(&key) {
                continue;
            }
            let s = self.ts_ref(&key, now).unwrap();
            if let Some(label) = label {
                if let Some(v) = s.config.labels.get(label) {
                    result.insert(v.clone());
                }
            } else {
                result.extend(s.config.labels.keys().cloned());
            }
        }
        match super::bounded_replies(result.into_iter().map(bytes), self.reply_limit) {
            Resp::Array(Some(r)) => Resp::Set(r),
            e => e,
        }
    }
    fn ts_mget(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut q = Query::default();
        let mut i = 1;
        while i < args.len() && !eq(&args[i], b"FILTER") {
            if eq(&args[i], b"LATEST") {
                q.latest = true;
                i += 1;
            } else if eq(&args[i], b"WITHLABELS") {
                if q.labels.is_some() {
                    return syntax();
                }
                q.all_labels = true;
                i += 1;
            } else if eq(&args[i], b"SELECTED_LABELS") {
                if q.all_labels {
                    return syntax();
                }
                let begin = i + 1;
                i = begin;
                while i < args.len() && !eq(&args[i], b"FILTER") {
                    i += 1;
                }
                if begin == i {
                    return syntax();
                }
                q.labels = Some(args[begin..i].to_vec());
            } else {
                return syntax();
            }
        }
        if i == args.len() {
            return syntax();
        }
        let f = match filters(&args[i + 1..]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let resp3 = self.command_version == crate::protocol::Version::Resp3;
        let mut out = Vec::new();
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        for key in self.ts_keys_matching(&f, now) {
            let s = self.ts_ref(&key, now).unwrap();
            let latest = q.latest
                && s.source.as_ref().is_some_and(|src| {
                    self.command_key_readable(src) && self.ts_rule_live(src, &key, now)
                });
            let p = if latest {
                s.last_with_pending()
            } else {
                s.last()
            };
            let reply = array(vec![
                label_reply_as(&s.config, &q, resp3),
                p.map_or_else(|| array(Vec::new()), |p| sample_pair(p.0, p.1, resp3)),
            ]);
            let key = bytes(key);
            if let Err(e) = budget.response(&key) {
                return e;
            }
            if let Err(e) = budget.response(&reply) {
                return e;
            }
            out.push((key, reply));
        }
        series_replies(out, resp3)
    }
    fn ts_mrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_multi_range(args, now, false)
    }
    fn ts_mrevrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_multi_range(args, now, true)
    }
    fn ts_multi_range(&mut self, args: &[Bytes], now: i64, reverse: bool) -> Resp {
        let (mut q, i) = match query_options(args, 1, true) {
            Ok(q) => q,
            Err(e) => return e,
        };
        if i == args.len() || !eq(&args[i], b"FILTER") {
            return syntax();
        }
        let end = args[i + 1..]
            .iter()
            .position(|a| eq(a, b"GROUPBY") || eq(a, b"EXCLUDEEMPTY"))
            .map_or(args.len(), |p| i + 1 + p);
        let f = match filters(&args[i + 1..end]) {
            Ok(f) => f,
            Err(e) => return e,
        };
        let mut group = None;
        let mut j = end;
        while j < args.len() {
            if eq(&args[j], b"EXCLUDEEMPTY") {
                q.exclude_empty = true;
                j += 1;
            } else if eq(&args[j], b"GROUPBY") && j + 3 < args.len() && eq(&args[j + 2], b"REDUCE")
            {
                let Some(a) = Agg::parse(&args[j + 3])
                    .filter(|a| !matches!(a, Agg::First | Agg::Last | Agg::Twa))
                else {
                    return syntax();
                };
                group = Some((args[j + 1].clone(), a));
                j += 4;
            } else {
                return syntax();
            }
        }
        if group.is_some() && (q.exclude_empty || q.aggs.len() > 1) {
            return syntax();
        }
        let resp3 = self.command_version == crate::protocol::Version::Resp3;
        let keys = self.ts_keys_matching(&f, now);
        let mut out = Vec::new();
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        let mut group_budget = super::ReplyBudget::new(self.reply_limit);
        if let Some((label, reducer)) = group {
            #[derive(Default)]
            struct ReductionGroup {
                sources: Vec<Bytes>,
                rows: BTreeMap<i64, Vec<(i64, f64)>>,
            }
            let mut groups: BTreeMap<Bytes, ReductionGroup> = BTreeMap::new();
            for key in keys {
                let s = self.ts_ref(&key, now).unwrap();
                let Some(value) = s.config.labels.get(&label) else {
                    continue;
                };
                let requested = q.latest;
                q.latest = requested
                    && s.source.as_ref().is_some_and(|src| {
                        self.command_key_readable(src) && self.ts_rule_live(src, &key, now)
                    });
                let rows = match query_rows(s, &q, reverse, self.reply_limit) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                q.latest = requested;
                let g = groups.entry(value.clone()).or_default();
                g.sources.push(key);
                for (t, v) in rows {
                    if let Err(e) = group_budget.charge(2, 16) {
                        return e;
                    }
                    g.rows.entry(t).or_default().push((t, v[0]));
                }
            }
            for (value, ReductionGroup { sources, rows }) in groups {
                let mut name = label.to_vec();
                name.push(b'=');
                name.extend_from_slice(&value);
                let mut source_list = Vec::new();
                for (i, s) in sources.iter().enumerate() {
                    if i > 0 {
                        source_list.push(b',');
                    }
                    source_list.extend_from_slice(s);
                }
                let mut config = Config::default();
                if q.all_labels {
                    config.labels.insert(label.clone(), value.clone());
                    config.labels.insert(
                        Bytes::from_static(b"__reducer__"),
                        Bytes::from(reducer.name()),
                    );
                    config
                        .labels
                        .insert(Bytes::from_static(b"__source__"), Bytes::from(source_list));
                } else if q.labels.is_some()
                    && let Some(first) = sources.first()
                {
                    config.labels = self.ts_ref(first, now).unwrap().config.labels.clone();
                    for source in &sources[1..] {
                        let labels = &self.ts_ref(source, now).unwrap().config.labels;
                        config.labels.retain(|k, v| labels.get(k) == Some(v));
                    }
                }
                let mut result: Vec<_> = rows
                    .into_iter()
                    .map(|(t, v)| {
                        let a = if reducer == Agg::Sum && v.iter().all(|p| p.1.is_nan()) {
                            f64::NAN
                        } else {
                            aggregate(reducer, &v, t, t, None, None)
                        };
                        (t, vec![a])
                    })
                    .collect();
                if reverse {
                    result.reverse();
                }
                if let Some(n) = q.count {
                    result.truncate(n);
                }
                let (key, reply) = series_entry(
                    Bytes::from(name),
                    &config,
                    &q,
                    &result,
                    Some((reducer, &sources)),
                    resp3,
                );
                if let Err(e) = budget.response(&key) {
                    return e;
                }
                if let Err(e) = budget.response(&reply) {
                    return e;
                }
                out.push((key, reply));
            }
        } else {
            for key in keys {
                let s = self.ts_ref(&key, now).unwrap();
                let requested = q.latest;
                q.latest = requested
                    && s.source.as_ref().is_some_and(|src| {
                        self.command_key_readable(src) && self.ts_rule_live(src, &key, now)
                    });
                let rows = match query_rows(s, &q, reverse, self.reply_limit) {
                    Ok(r) => r,
                    Err(e) => return e,
                };
                q.latest = requested;
                if q.exclude_empty && rows.is_empty() {
                    continue;
                }
                let (key, reply) = series_entry(key, &s.config, &q, &rows, None, resp3);
                if let Err(e) = budget.response(&key) {
                    return e;
                }
                if let Err(e) = budget.response(&reply) {
                    return e;
                }
                out.push((key, reply));
            }
        }
        series_replies(out, resp3)
    }
    fn ts_nrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_explicit_range(args, now, false)
    }
    fn ts_nrevrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.ts_explicit_range(args, now, true)
    }
    fn ts_explicit_range(&mut self, args: &[Bytes], now: i64, reverse: bool) -> Resp {
        let Some(n) = timestamp(&args[1])
            .filter(|n| *n > 0)
            .and_then(|n| usize::try_from(n).ok())
        else {
            return syntax();
        };
        let Some(start) = 2usize
            .checked_add(n)
            .filter(|i| i.checked_add(1).is_some_and(|j| j < args.len()))
        else {
            return syntax();
        };
        let mut normalized = args.to_vec();
        let mut per_key = None;
        if let Some(pos) = args[start + 2..]
            .iter()
            .position(|a| eq(a, b"AGGREGATION"))
            .map(|p| p + start + 2)
        {
            if pos.checked_add(n + 1).is_none_or(|i| i >= args.len()) {
                return syntax();
            }
            let aggs = match args[pos + 1..pos + 1 + n]
                .iter()
                .map(|s| {
                    s.split(|b| *b == b',')
                        .map(|s| Agg::parse(s).ok_or_else(syntax))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
            {
                Ok(a) => a,
                Err(e) => return e,
            };
            normalized.drain(pos + 2..pos + 1 + n);
            per_key = Some(aggs);
        }
        let (mut q, _) = match query_options(&normalized, start, false) {
            Ok(q) => q,
            Err(e) => return e,
        };
        let count = q.count.take();
        for key in &args[2..start] {
            self.discard_expired(key, now);
        }
        let widths: Vec<_> = per_key
            .as_ref()
            .map_or_else(|| vec![1; n], |a| a.iter().map(Vec::len).collect());
        let total: usize = widths.iter().sum();
        let mut joined: BTreeMap<i64, Vec<f64>> = BTreeMap::new();
        let mut offset = 0;
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        for (idx, key) in args[2..start].iter().enumerate() {
            let s = match self.ts_ref(key, now) {
                Ok(s) => s,
                Err(e) => return e,
            };
            if let Some(a) = &per_key {
                q.aggs = a[idx].clone();
            }
            let requested = q.latest;
            q.latest = requested
                && s.source.as_ref().is_some_and(|src| {
                    self.command_key_readable(src) && self.ts_rule_live(src, key, now)
                });
            let rows = match query_rows(s, &q, false, self.reply_limit) {
                Ok(r) => r,
                Err(e) => return e,
            };
            q.latest = requested;
            for (t, values) in rows {
                if !joined.contains_key(&t)
                    && let Err(e) = budget.charge(total + 3, total.saturating_mul(32))
                {
                    return e;
                }
                let row = joined.entry(t).or_insert_with(|| vec![f64::NAN; total]);
                row[offset..offset + values.len()].copy_from_slice(&values);
            }
            offset += widths[idx];
        }
        let mut rows: Vec<_> = joined.into_iter().collect();
        if reverse {
            rows.reverse();
        }
        if let Some(n) = count {
            rows.truncate(n);
        }
        let resp3 = self.command_version == crate::protocol::Version::Resp3;
        array(
            rows.into_iter()
                .map(|(t, v)| {
                    array(vec![
                        Resp::Integer(t),
                        array(v.into_iter().map(|v| float_for(v, resp3)).collect()),
                    ])
                })
                .collect(),
        )
    }
    fn ts_info(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() == 3 && !eq(&args[2], b"DEBUG") {
            return syntax();
        }
        self.discard_expired(&args[1], now);
        let s = match self.ts_ref(&args[1], now) {
            Ok(s) => s,
            Err(e) => return e,
        };
        let label_size: usize = s
            .config
            .labels
            .iter()
            .map(|(k, v)| k.len() + v.len() + std::mem::size_of::<(Bytes, Bytes)>())
            .sum();
        let chunk_size: usize = s
            .chunks
            .values()
            .map(|c| c.data.capacity() + std::mem::size_of::<Chunk>())
            .sum();
        let rule_size: usize = s
            .rules
            .values()
            .map(|r| {
                std::mem::size_of::<Rule>()
                    + r.dest.len()
                    + r.active.len() * std::mem::size_of::<(i64, f64)>()
            })
            .sum();
        let source = s
            .source
            .as_ref()
            .filter(|src| self.ts_rule_live(src, &args[1], now));
        let rules = s
            .rules
            .values()
            .filter(|r| self.ts_rule_live(&args[1], &r.dest, now))
            .map(|r| {
                array(vec![
                    bytes(r.dest.clone()),
                    Resp::Integer(r.duration),
                    bulk(r.agg.name()),
                    Resp::Integer(r.align),
                ])
            })
            .collect();
        let mut out = vec![
            bulk("totalSamples"),
            Resp::Integer(s.count as i64),
            bulk("memoryUsage"),
            Resp::Integer(
                (std::mem::size_of::<TimeSeriesValue>() + label_size + chunk_size + rule_size)
                    as i64,
            ),
            bulk("firstTimestamp"),
            Resp::Integer(s.first().map_or(0, |p| p.0)),
            bulk("lastTimestamp"),
            Resp::Integer(s.last().map_or(0, |p| p.0)),
            bulk("retentionTime"),
            Resp::Integer(s.config.retention),
            bulk("chunkCount"),
            Resp::Integer(s.chunks.len() as i64),
            bulk("chunkSize"),
            Resp::Integer(s.config.chunk_size as i64),
            bulk("chunkType"),
            bulk(if s.config.compressed {
                "compressed"
            } else {
                "uncompressed"
            }),
            bulk("duplicatePolicy"),
            s.config.policy.map_or(Resp::Bulk(None), |p| bulk(p.name())),
            bulk("labels"),
            label_reply_as(
                &s.config,
                &Query {
                    all_labels: true,
                    ..Query::default()
                },
                self.command_version == crate::protocol::Version::Resp3,
            ),
            bulk("sourceKey"),
            source.map_or(Resp::Bulk(None), |s| bytes(s.clone())),
            bulk("rules"),
            array(rules),
            bulk("ignoreMaxTimeDiff"),
            Resp::Integer(s.config.ignore_time),
            bulk("ignoreMaxValDiff"),
            Resp::Double(s.config.ignore_value),
        ];
        if args.len() == 3 {
            out.extend([
                bulk("keySelfName"),
                bytes(args[1].clone()),
                bulk("Chunks"),
                array(
                    s.chunks
                        .values()
                        .map(|c| {
                            field_map(vec![
                                bulk("startTimestamp"),
                                Resp::Integer(c.first),
                                bulk("endTimestamp"),
                                Resp::Integer(c.last),
                                bulk("samples"),
                                Resp::Integer(c.count as i64),
                                bulk("size"),
                                Resp::Integer(c.data.capacity() as i64),
                                bulk("bytesPerSample"),
                                Resp::Double(c.data.capacity() as f64 / c.count as f64),
                            ])
                        })
                        .collect(),
                ),
            ]);
        }
        let reply = field_map(out);
        match super::ReplyBudget::new(self.reply_limit).response(&reply) {
            Ok(()) => reply,
            Err(e) => e,
        }
    }
}

struct ReadOptions {
    cursor: u64,
    timeout: Option<std::time::Duration>,
    minimum: usize,
    maximum: Option<usize>,
}
impl Engine {
    fn ts_read_options(&self, args: &[Bytes], now: i64) -> Result<ReadOptions, Resp> {
        if args.len() < 3 {
            return Err(syntax());
        }
        let s = match self.ts_ref(&args[1], now) {
            Ok(s) => Some(s),
            Err(e) => {
                if self
                    .entries
                    .get(&args[1])
                    .is_some_and(|e| e.expires_at.is_none_or(|t| t > now))
                {
                    return Err(e);
                } else {
                    None
                }
            }
        };
        let cursor = match args[2].as_ref() {
            b"-" => s.and_then(TimeSeriesValue::first).map_or(0, |p| p.0 as u64),
            b"+" => s.and_then(TimeSeriesValue::last).map_or(0, |p| p.0 as u64),
            b"$" => s
                .and_then(TimeSeriesValue::last)
                .map_or(0, |p| p.0 as u64 + 1),
            v => {
                if v.is_empty() || !v.iter().all(u8::is_ascii_digit) {
                    return Err(syntax());
                }
                std::str::from_utf8(v)
                    .ok()
                    .and_then(|s| s.parse::<u64>().ok())
                    .filter(|n| *n <= i64::MAX as u64 + 1)
                    .ok_or_else(syntax)?
            }
        };
        let (mut i, mut timeout, mut minimum, mut maximum, mut block) = (3, None, 1, None, false);
        while i < args.len() {
            if eq(&args[i], b"BLOCK") && !block && i + 2 < args.len() {
                let ms = timestamp(&args[i + 1]).ok_or_else(syntax)?;
                minimum = timestamp(&args[i + 2])
                    .filter(|n| *n > 0)
                    .ok_or_else(syntax)? as usize;
                timeout = (ms != 0).then(|| std::time::Duration::from_millis(ms as u64));
                block = true;
                i += 3;
            } else if eq(&args[i], b"MAX_COUNT") && maximum.is_none() && i + 1 < args.len() {
                maximum = Some(
                    timestamp(&args[i + 1])
                        .filter(|n| *n > 0)
                        .ok_or_else(syntax)? as usize,
                );
                i += 2;
            } else {
                return Err(syntax());
            }
        }
        if block && maximum.is_some_and(|n| n < minimum) {
            return Err(syntax());
        }
        Ok(ReadOptions {
            cursor,
            timeout,
            minimum: if block { minimum } else { 0 },
            maximum,
        })
    }
    pub(crate) fn timeseries_read(
        &mut self,
        args: &[Bytes],
        now: i64,
        timeout: bool,
    ) -> super::Execution {
        let options = match self.ts_read_options(args, now) {
            Ok(o) => o,
            Err(e) => return super::Execution::Reply(e),
        };
        self.discard_expired(&args[1], now);
        let mut rows = Vec::new();
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        if options.cursor <= i64::MAX as u64
            && let Ok(s) = self.ts_ref(&args[1], now)
        {
            for p in s
                .iter(options.cursor as i64, i64::MAX)
                .take(options.maximum.unwrap_or(usize::MAX))
            {
                if let Err(e) = budget.charge(3, 32) {
                    return super::Execution::Reply(e);
                }
                rows.push(p);
            }
        }
        if !timeout && options.minimum > rows.len() {
            let mut resolved = args.to_vec();
            resolved[2] = Bytes::from(options.cursor.to_string());
            let retry_after = self
                .entries
                .get(&args[1])
                .and_then(|e| e.expires_at)
                .map(|at| std::time::Duration::from_millis(at.saturating_sub(now).max(1) as u64));
            return super::Execution::StreamBlock(super::BlockingStream {
                args: resolved,
                keys: vec![args[1].clone()],
                timeout: options.timeout,
                retry_after,
                timeseries: true,
                timeseries_existed: self.ts_ref(&args[1], now).is_ok(),
                group_tokens: Vec::new(),
            });
        }
        let count = options.maximum.unwrap_or(usize::MAX);
        let resp3 = self.command_version == crate::protocol::Version::Resp3;
        super::Execution::Reply(super::bounded_replies(
            rows.into_iter()
                .take(count)
                .map(|(t, v)| sample_pair(t, v, resp3)),
            self.reply_limit,
        ))
    }
    fn ts_read_immediate(&mut self, args: &[Bytes], now: i64) -> Resp {
        // The execution layer intercepts blocking reads before command handlers.
        match self.timeseries_read(args, now, true) {
            super::Execution::Reply(r) => r,
            _ => unreachable!(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(e: &mut Engine, args: &[&str]) -> Resp {
        let args: Vec<_> = args
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        e.execute_at(&args, 1000)
    }
    fn rows(e: &Engine, key: &str) -> Vec<(i64, f64)> {
        e.ts_ref(&Bytes::copy_from_slice(key.as_bytes()), 1000)
            .unwrap()
            .rows(0, i64::MAX)
    }
    #[test]
    fn retention_is_sample_time_based_and_boundary_is_inclusive() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "a", "RETENTION", "10"]);
        run(&mut e, &["TS.ADD", "a", "100", "1"]);
        run(&mut e, &["TS.ADD", "a", "110", "2"]);
        assert_eq!(rows(&e, "a"), vec![(100, 1.0), (110, 2.0)]);
        run(&mut e, &["TS.ADD", "a", "111", "3"]);
        assert_eq!(rows(&e, "a"), vec![(110, 2.0), (111, 3.0)]);
        assert!(matches!(
            run(&mut e, &["TS.ADD", "a", "100", "4"]),
            Resp::Error(_)
        ));
        assert_eq!(rows(&e, "a"), vec![(110, 2.0), (111, 3.0)]);
    }
    #[test]
    fn multi_add_preserves_item_errors_and_ordered_successes() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "a"]);
        run(&mut e, &["TS.CREATE", "b"]);
        let response = run(
            &mut e,
            &[
                "TS.MADD", "a", "1", "2", "missing", "2", "3", "b", "3", "4", "a", "1", "5",
            ],
        );
        let Resp::Array(Some(reply)) = response else {
            panic!("expected item replies");
        };
        assert_eq!(reply.len(), 4);
        assert!(matches!(reply[1], Resp::Error(_)));
        assert!(matches!(reply[3], Resp::Error(_)));
        assert_eq!(rows(&e, "a"), vec![(1, 2.0)]);
        assert_eq!(rows(&e, "b"), vec![(3, 4.0)]);
    }
    #[test]
    fn compaction_closes_updates_and_deletes_real_destination_buckets() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "raw"]);
        run(&mut e, &["TS.CREATE", "avg"]);
        assert_eq!(
            run(
                &mut e,
                &["TS.CREATERULE", "raw", "avg", "AGGREGATION", "avg", "10"]
            ),
            Resp::Simple("OK")
        );
        run(&mut e, &["TS.ADD", "raw", "1", "2"]);
        run(&mut e, &["TS.ADD", "raw", "5", "6"]);
        assert!(rows(&e, "avg").is_empty());
        assert_eq!(run(&mut e, &["TS.GET", "avg", "LATEST"]), pair(0, 4.0));
        run(&mut e, &["TS.ADD", "raw", "10", "8"]);
        assert_eq!(rows(&e, "avg"), vec![(0, 4.0)]);
        run(
            &mut e,
            &["TS.ADD", "raw", "1", "10", "ON_DUPLICATE", "LAST"],
        );
        assert_eq!(rows(&e, "avg"), vec![(0, 8.0)]);
        run(&mut e, &["TS.DEL", "raw", "1", "5"]);
        assert!(rows(&e, "avg").is_empty());
    }
    #[test]
    fn aggregation_filters_alignment_empty_and_variance() {
        let mut s = TimeSeriesValue::new(Config::default());
        s.set(10, 2.0);
        s.set(30, 6.0);
        let q = Query {
            lo: 0,
            hi: 40,
            aggs: vec![Agg::Sum, Agg::Avg, Agg::Count],
            duration: 10,
            empty: true,
            ..Query::default()
        };
        let r = query_rows(&s, &q, false, 1_000_000).unwrap();
        assert_eq!(r.len(), 3);
        assert_eq!(r[1].0, 20);
        assert_eq!(r[1].1[0], 0.0);
        assert!(r[1].1[1].is_nan());
        assert_eq!(r[1].1[2], 0.0);
        assert_eq!(
            aggregate(Agg::VarP, &[(0, 2.0), (1, 4.0)], 0, 2, None, None),
            1.0
        );
        assert_eq!(
            aggregate(
                Agg::Twa,
                &[(10, 10.0)],
                0,
                20,
                Some((0, 0.0)),
                Some((20, 20.0))
            ),
            10.0
        );
    }
    #[test]
    fn snapshot_preserves_open_compaction_and_nan_payloads() {
        let mut s = TimeSeriesValue::new(Config {
            retention: 5,
            ..Config::default()
        });
        s.set(100, f64::NAN);
        s.apply_decoded(Effect::Rule(Bytes::from_static(b"d"), Agg::Avg, 1000, 0));
        s.apply_decoded(Effect::Active(
            Bytes::from_static(b"d"),
            0,
            1,
            Some(2.0),
            None,
        ));
        s.apply_decoded(Effect::Active(
            Bytes::from_static(b"d"),
            0,
            2,
            Some(6.0),
            None,
        ));
        let mut snap = s.snapshot();
        let mut restored = TimeSeriesValue::restore(&snap.next().unwrap().unwrap()).unwrap();
        for effect in snap {
            restored.apply(&effect.unwrap()).unwrap();
        }
        assert!(restored.get(100).unwrap().is_nan());
        assert_eq!(restored.rules[&Bytes::from_static(b"d")].value(None), 4.0);
        let malformed = [1, 1, 0];
        assert!(restored.apply(&malformed).is_err());
        assert!(restored.get(100).unwrap().is_nan());
    }
    #[test]
    fn compressed_chunks_survive_out_of_order_updates_and_size_changes() {
        let mut s = TimeSeriesValue::new(Config {
            chunk_size: 48,
            ..Config::default()
        });
        for t in (0..20).rev() {
            s.set(t, t as f64);
        }
        s.set(8, 80.0);
        s.config.chunk_size = 96;
        s.set(30, 30.0);
        assert_eq!(s.count, 21);
        assert_eq!(s.get(8), Some(80.0));
        assert_eq!(s.delete(5, 10), 6);
        assert_eq!(s.count, 15);
        assert!(s.chunks.len() > 1);
        assert!(
            s.chunks
                .values()
                .all(|c| c.rows().windows(2).all(|w| w[0].0 < w[1].0))
        );
    }
}

fn label_reply_as(c: &Config, q: &Query, resp3: bool) -> Resp {
    let pairs: Vec<_> = if q.all_labels {
        c.labels
            .iter()
            .map(|(k, v)| (k.clone(), Some(v.clone())))
            .collect()
    } else {
        q.labels
            .iter()
            .flatten()
            .map(|k| {
                let found = c
                    .labels
                    .iter()
                    .find(|(label, _)| label.eq_ignore_ascii_case(k));
                found.map_or_else(|| (k.clone(), None), |(k, v)| (k.clone(), Some(v.clone())))
            })
            .collect()
    };
    if resp3 {
        Resp::Map(
            pairs
                .into_iter()
                .map(|(k, v)| (bytes(k), Resp::Bulk(v)))
                .collect(),
        )
    } else {
        array(
            pairs
                .into_iter()
                .map(|(k, v)| array(vec![bytes(k), Resp::Bulk(v)]))
                .collect(),
        )
    }
}
fn sample_reply_borrowed(rows: &[(i64, Vec<f64>)], resp3: bool) -> Resp {
    array(
        rows.iter()
            .map(|(t, values)| {
                let mut reply = Vec::with_capacity(values.len() + 1);
                reply.push(Resp::Integer(*t));
                reply.extend(values.iter().copied().map(|v| float_for(v, resp3)));
                array(reply)
            })
            .collect(),
    )
}
fn series_entry(
    key: Bytes,
    c: &Config,
    q: &Query,
    rows: &[(i64, Vec<f64>)],
    group: Option<(Agg, &[Bytes])>,
    resp3: bool,
) -> (Resp, Resp) {
    let labels = if resp3 && group.is_some() && q.all_labels {
        let mut labels = c.clone();
        labels.labels.remove(b"__source__".as_slice());
        labels.labels.remove(b"__reducer__".as_slice());
        label_reply_as(&labels, q, true)
    } else {
        label_reply_as(c, q, resp3)
    };
    let mut value = vec![labels];
    if resp3 {
        if let Some((reducer, sources)) = group {
            value.push(Resp::Map(vec![(
                bulk("reducers"),
                array(vec![bulk(reducer.name())]),
            )]));
            value.push(Resp::Map(vec![(
                bulk("sources"),
                array(sources.iter().cloned().map(bytes).collect()),
            )]));
        } else {
            value.push(Resp::Map(vec![(
                bulk("aggregators"),
                array(
                    q.aggs
                        .iter()
                        .map(|a| bytes(a.name().to_ascii_lowercase()))
                        .collect(),
                ),
            )]));
        }
    }
    value.push(sample_reply_borrowed(rows, resp3));
    (bytes(key), array(value))
}
fn series_replies(entries: Vec<(Resp, Resp)>, resp3: bool) -> Resp {
    if resp3 {
        return Resp::Map(entries);
    }
    array(
        entries
            .into_iter()
            .map(|(key, value)| {
                let Resp::Array(Some(mut value)) = value else {
                    unreachable!("series values are arrays");
                };
                value.insert(0, key);
                array(value)
            })
            .collect(),
    )
}
fn field_map(fields: Vec<Resp>) -> Resp {
    let mut fields = fields.into_iter();
    let mut pairs = Vec::new();
    while let Some(key) = fields.next() {
        pairs.push((key, fields.next().expect("information fields are paired")));
    }
    Resp::Map(pairs)
}

#[cfg(test)]
mod boundary_tests {
    use super::*;
    fn run(e: &mut Engine, args: &[&str]) -> Resp {
        e.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    fn snapshot_copy(s: &TimeSeriesValue) -> TimeSeriesValue {
        let mut snap = s.snapshot();
        let mut copy = TimeSeriesValue::restore(&snap.next().unwrap().unwrap()).unwrap();
        for effect in snap {
            copy.apply(&effect.unwrap()).unwrap();
        }
        copy
    }
    #[test]
    fn nan_duplicate_policies_and_increment_errors_do_not_mutate() {
        let mut e = Engine::new();
        run(&mut e, &["TS.ADD", "n", "1", "NaN"]);
        for policy in ["MIN", "MAX", "SUM"] {
            assert!(matches!(
                run(&mut e, &["TS.ADD", "n", "1", "2", "ON_DUPLICATE", policy]),
                Resp::Error(_)
            ));
        }
        assert!(matches!(
            run(&mut e, &["TS.INCRBY", "n", "1", "TIMESTAMP", "2"]),
            Resp::Error(_)
        ));
        assert!(
            e.ts_ref(&Bytes::from_static(b"n"), 1000)
                .unwrap()
                .get(1)
                .unwrap()
                .is_nan()
        );
        assert_eq!(
            run(&mut e, &["TS.ADD", "n", "1", "4", "ON_DUPLICATE", "LAST"]),
            Resp::Integer(1)
        );
    }
    #[test]
    fn filters_grouping_and_explicit_pivot_keep_distinct_series_columns() {
        let mut e = Engine::new();
        run(
            &mut e,
            &["TS.CREATE", "a", "LABELS", "kind", "temp", "room", "x"],
        );
        run(
            &mut e,
            &["TS.CREATE", "b", "LABELS", "kind", "temp", "room", "x"],
        );
        run(
            &mut e,
            &["TS.MADD", "a", "10", "1", "a", "20", "3", "b", "10", "5"],
        );
        let reply = run(
            &mut e,
            &[
                "TS.MRANGE",
                "-",
                "+",
                "FILTER",
                "kind=(temp,pressure)",
                "room!=",
                "GROUPBY",
                "room",
                "REDUCE",
                "max",
            ],
        );
        let Resp::Array(Some(series)) = reply else {
            panic!("series array");
        };
        let Resp::Array(Some(entry)) = &series[0] else {
            panic!("series entry");
        };
        assert_eq!(entry[0], bytes("room=x"));
        assert_eq!(entry[2], rows_reply(vec![(10, vec![5.0]), (20, vec![3.0])]));
        let pivot = run(
            &mut e,
            &[
                "TS.NRANGE",
                "3",
                "a",
                "b",
                "a",
                "-",
                "+",
                "AGGREGATION",
                "min,max",
                "sum",
                "avg",
                "10",
            ],
        );
        let Resp::Array(Some(rows)) = pivot else {
            panic!("pivot rows");
        };
        let Resp::Array(Some(first)) = &rows[0] else {
            panic!("pivot row");
        };
        assert_eq!(first[0], Resp::Integer(10));
        assert_eq!(
            first[1],
            array(vec![float(1.0), float(1.0), float(5.0), float(1.0)])
        );
        assert!(matches!(
            run(
                &mut e,
                &[
                    "TS.MRANGE",
                    "-",
                    "+",
                    "FILTER",
                    "kind=temp",
                    "GROUPBY",
                    "room",
                    "REDUCE",
                    "max",
                    "EXCLUDEEMPTY"
                ]
            ),
            Resp::Error(_)
        ));
    }
    #[test]
    fn twa_late_updates_recompute_interpolated_neighbor_buckets() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "raw"]);
        run(&mut e, &["TS.CREATE", "twa"]);
        run(
            &mut e,
            &["TS.CREATERULE", "raw", "twa", "AGGREGATION", "twa", "10"],
        );
        run(
            &mut e,
            &[
                "TS.MADD", "raw", "0", "0", "raw", "10", "10", "raw", "20", "20",
            ],
        );
        assert_eq!(
            e.ts_ref(&Bytes::from_static(b"twa"), 1000)
                .unwrap()
                .rows(0, 100),
            vec![(0, 5.0), (10, 15.0)]
        );
        run(
            &mut e,
            &["TS.ADD", "raw", "10", "30", "ON_DUPLICATE", "LAST"],
        );
        assert_eq!(
            e.ts_ref(&Bytes::from_static(b"twa"), 1000)
                .unwrap()
                .rows(0, 100),
            vec![(0, 15.0), (10, 25.0)]
        );
        run(&mut e, &["TS.DEL", "raw", "10", "10"]);
        assert_eq!(
            e.ts_ref(&Bytes::from_static(b"twa"), 1000)
                .unwrap()
                .rows(0, 100),
            vec![(0, 5.0)]
        );
    }
    #[test]
    fn retention_does_not_erase_open_rule_accumulators_on_checkpoint() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "raw", "RETENTION", "5"]);
        run(&mut e, &["TS.CREATE", "d"]);
        run(
            &mut e,
            &["TS.CREATERULE", "raw", "d", "AGGREGATION", "avg", "100"],
        );
        run(
            &mut e,
            &[
                "TS.MADD", "raw", "0", "0", "raw", "10", "10", "raw", "20", "20",
            ],
        );
        let raw = e.ts_ref(&Bytes::from_static(b"raw"), 1000).unwrap();
        assert_eq!(raw.count, 1);
        let restored = snapshot_copy(raw);
        assert_eq!(restored.rules[&Bytes::from_static(b"d")].stats.n, 3);
        assert_eq!(restored.rules[&Bytes::from_static(b"d")].value(None), 10.0);
        run(&mut e, &["TS.ADD", "raw", "100", "100"]);
        assert_eq!(
            e.ts_ref(&Bytes::from_static(b"d"), 1000).unwrap().get(0),
            Some(10.0)
        );
    }
    #[test]
    fn snapshot_preserves_actual_chunk_configuration_and_empty_rule_cursor() {
        let mut s = TimeSeriesValue::new(Config {
            chunk_size: 48,
            ..Config::default()
        });
        for t in 1..20 {
            s.set(t, t as f64);
        }
        s.config.chunk_size = 96;
        s.set(100, 100.0);
        s.apply_decoded(Effect::Rule(Bytes::from_static(b"d"), Agg::Count, 10, 0));
        s.apply_decoded(Effect::Active(
            Bytes::from_static(b"d"),
            100,
            100,
            None,
            Some((90, 1.0)),
        ));
        let restored = snapshot_copy(&s);
        assert_eq!(
            s.chunks.keys().collect::<Vec<_>>(),
            restored.chunks.keys().collect::<Vec<_>>()
        );
        assert_eq!(
            s.chunks.values().map(|c| c.allocation).collect::<Vec<_>>(),
            restored
                .chunks
                .values()
                .map(|c| c.allocation)
                .collect::<Vec<_>>()
        );
        assert_eq!(restored.rules[&Bytes::from_static(b"d")].current, Some(100));
        assert!(restored.rules[&Bytes::from_static(b"d")].active.is_empty());
    }
    #[test]
    fn read_freezes_inclusive_and_new_cursors_and_flushes_partial_timeout() {
        let mut e = Engine::new();
        run(&mut e, &["TS.ADD", "s", "10", "2"]);
        let args =
            ["TS.READ", "s", "+", "BLOCK", "0", "2"].map(|s| Bytes::from_static(s.as_bytes()));
        let super::super::Execution::StreamBlock(wait) = e.timeseries_read(&args, 1000, false)
        else {
            panic!("must block below threshold");
        };
        assert_eq!(wait.args[2], Bytes::from_static(b"10"));
        assert!(wait.timeseries_existed);
        let super::super::Execution::Reply(reply) = e.timeseries_read(&wait.args, 1000, true)
        else {
            panic!("timeout must flush");
        };
        assert_eq!(reply, array(vec![pair(10, 2.0)]));
        run(&mut e, &["TS.ADD", "s", "20", "3"]);
        let super::super::Execution::Reply(reply) = e.timeseries_read(&wait.args, 1000, false)
        else {
            panic!("threshold reached");
        };
        assert_eq!(reply, array(vec![pair(10, 2.0), pair(20, 3.0)]));
    }
    #[test]
    fn default_empty_chunk_and_reply_count_limits_are_real() {
        let mut s = TimeSeriesValue::new(Config::default());
        assert_eq!(s.chunks.len(), 1);
        assert!(s.chunks.values().next().unwrap().data.capacity() >= 4096);
        s.set(0, 1.0);
        s.set(1_000_000, 2.0);
        let q = Query {
            lo: 0,
            hi: i64::MAX,
            count: Some(1),
            aggs: vec![Agg::Sum],
            duration: 1,
            empty: true,
            ..Query::default()
        };
        assert_eq!(
            query_rows(&s, &q, false, 1024).unwrap(),
            vec![(0, vec![1.0])]
        );
        assert_eq!(
            query_rows(&s, &q, true, 1024).unwrap(),
            vec![(1_000_000, vec![2.0])]
        );
        let q = Query {
            lo: 0,
            hi: i64::MAX,
            count: Some(1),
            ..Query::default()
        };
        assert_eq!(query_rows(&s, &q, false, 1024).unwrap().len(), 1);
    }
}

impl TimeSeriesValue {
    pub(super) fn related_keys(&self) -> impl Iterator<Item = &Bytes> {
        self.source.iter().chain(self.rules.keys())
    }
}

#[cfg(test)]
mod integration_regressions {
    use super::super::Mutation;
    use super::*;
    fn run(e: &mut Engine, args: &[&str]) -> Resp {
        e.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    #[test]
    fn memory_only_source_and_compaction_writes_both_notify_readers() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "r"]);
        run(&mut e, &["TS.CREATE", "d"]);
        run(
            &mut e,
            &["TS.CREATERULE", "r", "d", "AGGREGATION", "avg", "10"],
        );
        e.set_recording(false);
        e.set_ready_tracking(true);
        e.take_ready_keys();
        run(&mut e, &["TS.ADD", "r", "1", "2"]);
        e.take_ready_keys();
        assert!(e.failure.is_none());
        run(&mut e, &["TS.ADD", "r", "10", "4"]);
        e.take_ready_keys();
        assert_eq!(
            e.ts_ref(&Bytes::from_static(b"d"), 1000).unwrap().get(0),
            Some(2.0)
        );
    }
    #[test]
    fn resp3_queries_use_native_numbers_maps_and_aggregation_metadata() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "s", "LABELS", "kind", "temp"]);
        run(&mut e, &["TS.ADD", "s", "1", "2.5"]);
        e.set_command_version(crate::protocol::Version::Resp3);
        assert_eq!(
            run(&mut e, &["TS.GET", "s"]),
            array(vec![Resp::Integer(1), Resp::Double(2.5)])
        );
        let Resp::Map(series) = run(
            &mut e,
            &[
                "TS.MRANGE",
                "-",
                "+",
                "WITHLABELS",
                "AGGREGATION",
                "avg",
                "10",
                "FILTER",
                "kind=temp",
            ],
        ) else {
            panic!("RESP3 multi-range map");
        };
        let Resp::Array(Some(value)) = &series[0].1 else {
            panic!("series value");
        };
        assert!(matches!(&value[0], Resp::Map(_)));
        assert_eq!(
            value[1],
            Resp::Map(vec![(bulk("aggregators"), array(vec![bytes("avg")]))])
        );
        assert_eq!(
            value[2],
            array(vec![array(vec![Resp::Integer(0), Resp::Double(2.5)])])
        );
    }
    #[test]
    fn new_cursor_at_maximum_timestamp_never_replays_last_sample() {
        let mut e = Engine::new();
        run(&mut e, &["TS.ADD", "s", "9223372036854775807", "1"]);
        let args =
            ["TS.READ", "s", "$", "BLOCK", "0", "1"].map(|s| Bytes::from_static(s.as_bytes()));
        let super::super::Execution::StreamBlock(wait) = e.timeseries_read(&args, 1000, false)
        else {
            panic!("must wait for new data");
        };
        assert_eq!(wait.args[2], Bytes::from_static(b"9223372036854775808"));
        let super::super::Execution::Reply(reply) = e.timeseries_read(&wait.args, 1000, true)
        else {
            panic!("must flush");
        };
        assert_eq!(reply, array(Vec::new()));
    }
    #[test]
    fn removed_compaction_endpoints_do_not_resurrect_rules_on_same_name_recreation() {
        let mut e = Engine::new();
        run(&mut e, &["TS.CREATE", "r"]);
        run(&mut e, &["TS.CREATE", "d"]);
        run(
            &mut e,
            &["TS.CREATERULE", "r", "d", "AGGREGATION", "avg", "10"],
        );
        run(&mut e, &["TS.ADD", "r", "1", "2"]);
        run(&mut e, &["DEL", "d"]);
        run(&mut e, &["TS.CREATE", "d"]);
        run(&mut e, &["TS.ADD", "r", "10", "3"]);
        assert!(!e.ts_rule_live(&Bytes::from_static(b"r"), &Bytes::from_static(b"d"), 1000));
        assert_eq!(e.ts_ref(&Bytes::from_static(b"d"), 1000).unwrap().count, 0);
        assert!(
            e.ts_ref(&Bytes::from_static(b"r"), 1000)
                .unwrap()
                .rules
                .is_empty()
        );
    }
    #[test]
    fn compaction_delete_distinguishes_actual_expiry_from_empty_bucket_prefix() {
        fn setup() -> Engine {
            let mut e = Engine::new();
            run(&mut e, &["TS.CREATE", "r", "RETENTION", "5"]);
            run(&mut e, &["TS.CREATE", "d"]);
            run(
                &mut e,
                &["TS.CREATERULE", "r", "d", "AGGREGATION", "avg", "10"],
            );
            e
        }
        let mut fresh = setup();
        run(&mut fresh, &["TS.ADD", "r", "9", "4"]);
        assert_eq!(
            run(&mut fresh, &["TS.DEL", "r", "9", "9"]),
            Resp::Integer(1)
        );
        let mut expired = setup();
        run(&mut expired, &["TS.MADD", "r", "1", "2", "r", "9", "4"]);
        assert!(matches!(
            run(&mut expired, &["TS.DEL", "r", "9", "9"]),
            Resp::Error(_)
        ));
        assert_eq!(
            expired
                .ts_ref(&Bytes::from_static(b"r"), 1000)
                .unwrap()
                .get(9),
            Some(4.0)
        );
        let source = expired.ts_ref(&Bytes::from_static(b"r"), 1000).unwrap();
        let mut snap = source.snapshot();
        let mut restored = TimeSeriesValue::restore(&snap.next().unwrap().unwrap()).unwrap();
        for e in snap {
            restored.apply(&e.unwrap()).unwrap();
        }
        assert_eq!(restored.expired, Some(1));
    }
    fn persisted_run(source: &mut Engine, replayed: &mut Engine, args: &[&str]) -> Resp {
        source.set_recording(true);
        let reply = run(source, args);
        assert!(!matches!(&reply, Resp::Error(_)), "{args:?}: {reply:?}");
        if let Some(batch) = source.take_batch() {
            // Resolved sample/rule effects stay bounded, including late corrections.
            for mutation in &batch.mutations {
                if let Mutation::Typed { effect, .. } = mutation {
                    assert!(effect.len() < 256);
                }
            }
            replayed.replay(batch).unwrap();
        }
        reply
    }
    fn checkpoint(source: &Engine) -> Engine {
        let mut restored = Engine::new();
        for batch in source.snapshot_batches(1000) {
            restored.replay(batch.unwrap()).unwrap();
        }
        restored
    }
    fn assert_exact_series(source: &Engine, recovered: &Engine) {
        for key in [Bytes::from_static(b"r"), Bytes::from_static(b"d")] {
            let expected = source
                .ts_ref(&key, 1000)
                .unwrap()
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap();
            let actual = recovered
                .ts_ref(&key, 1000)
                .unwrap()
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap();
            assert_eq!(actual, expected);
        }
    }
    #[test]
    fn deleting_latest_bucket_reopens_previous_bucket_across_replay_and_checkpoint() {
        for aggregation in ["avg", "twa"] {
            let mut source = Engine::new();
            let mut replayed = Engine::new();
            for args in [
                vec!["TS.CREATE", "r"],
                vec!["TS.CREATE", "d"],
                vec!["TS.CREATERULE", "r", "d", "AGGREGATION", aggregation, "10"],
                vec!["TS.MADD", "r", "1", "2", "r", "10", "8"],
            ] {
                persisted_run(&mut source, &mut replayed, &args);
            }
            assert_eq!(
                persisted_run(&mut source, &mut replayed, &["TS.DEL", "r", "10", "10"]),
                Resp::Integer(1)
            );
            assert_exact_series(&source, &replayed);
            let mut restored = checkpoint(&source);
            assert_exact_series(&source, &restored);
            for engine in [&mut source, &mut replayed, &mut restored] {
                assert_eq!(
                    run(engine, &["TS.RANGE", "d", "-", "+"]),
                    rows_reply(vec![])
                );
                assert_eq!(run(engine, &["TS.GET", "d"]), rows_reply(vec![]));
                assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(0, 2.0));
                assert_eq!(
                    run(engine, &["TS.RANGE", "d", "-", "+", "LATEST"]),
                    rows_reply(vec![(0, vec![2.0])])
                );
            }
            persisted_run(&mut source, &mut replayed, &["TS.ADD", "r", "5", "6"]);
            run(&mut restored, &["TS.ADD", "r", "5", "6"]);
            assert_exact_series(&source, &replayed);
            assert_exact_series(&source, &restored);
            persisted_run(&mut source, &mut replayed, &["TS.ADD", "r", "10", "8"]);
            run(&mut restored, &["TS.ADD", "r", "10", "8"]);
            assert_exact_series(&source, &replayed);
            assert_exact_series(&source, &restored);
            for engine in [&mut source, &mut replayed, &mut restored] {
                let Resp::Array(Some(closed)) = run(engine, &["TS.RANGE", "d", "-", "+"]) else {
                    panic!("expected closed bucket samples");
                };
                assert_eq!(closed.len(), 1);
                assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(10, 8.0));
            }
        }
    }
    #[test]
    fn deleting_all_samples_resets_compaction_cursor_before_earlier_reinsertion() {
        let mut source = Engine::new();
        let mut replayed = Engine::new();
        for args in [
            vec!["TS.CREATE", "r"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATERULE", "r", "d", "AGGREGATION", "avg", "10"],
            vec!["TS.MADD", "r", "21", "2", "r", "30", "8"],
            vec!["TS.DEL", "r", "0", "100"],
        ] {
            persisted_run(&mut source, &mut replayed, &args);
        }
        assert_exact_series(&source, &replayed);
        let mut restored = checkpoint(&source);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(
                run(engine, &["TS.RANGE", "r", "-", "+"]),
                rows_reply(vec![])
            );
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![])
            );
            assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), rows_reply(vec![]));
        }
        persisted_run(&mut source, &mut replayed, &["TS.ADD", "r", "1", "4"]);
        run(&mut restored, &["TS.ADD", "r", "1", "4"]);
        assert_exact_series(&source, &replayed);
        assert_exact_series(&source, &restored);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![])
            );
            assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(0, 4.0));
        }
    }
    #[test]
    fn compaction_rewind_keeps_older_closed_buckets_and_respects_each_rule_window() {
        let mut source = Engine::new();
        let mut replayed = Engine::new();
        for args in [
            vec!["TS.CREATE", "r"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATE", "wide"],
            vec!["TS.CREATERULE", "r", "d", "AGGREGATION", "avg", "10"],
            vec!["TS.CREATERULE", "r", "wide", "AGGREGATION", "avg", "20"],
            vec![
                "TS.MADD", "r", "1", "2", "r", "11", "4", "r", "21", "6", "r", "31", "8",
            ],
            vec!["TS.DEL", "r", "31", "31"],
        ] {
            persisted_run(&mut source, &mut replayed, &args);
        }
        assert_eq!(run(&mut source, &["TS.GET", "d", "LATEST"]), pair(20, 6.0));
        assert_eq!(
            run(&mut source, &["TS.GET", "wide", "LATEST"]),
            pair(20, 6.0)
        );
        assert_eq!(
            run(&mut source, &["TS.RANGE", "d", "-", "+"]),
            rows_reply(vec![(0, vec![2.0]), (10, vec![4.0])])
        );
        persisted_run(&mut source, &mut replayed, &["TS.DEL", "r", "21", "21"]);
        assert_exact_series(&source, &replayed);
        let mut restored = checkpoint(&source);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![(0, vec![2.0])])
            );
            assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(10, 4.0));
            assert_eq!(
                run(engine, &["TS.RANGE", "wide", "-", "+"]),
                rows_reply(vec![])
            );
            assert_eq!(run(engine, &["TS.GET", "wide", "LATEST"]), pair(0, 3.0));
        }
    }
    #[test]
    fn twa_nan_overwrite_invalidates_open_predecessor_and_recomputes_closed_buckets() {
        let mut source = Engine::new();
        let mut replayed = Engine::new();
        for args in [
            vec!["TS.CREATE", "r"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATERULE", "r", "d", "AGGREGATION", "twa", "10"],
            vec!["TS.MADD", "r", "5", "0", "r", "15", "100", "r", "25", "20"],
            vec!["TS.ADD", "r", "15", "nan", "ON_DUPLICATE", "LAST"],
        ] {
            persisted_run(&mut source, &mut replayed, &args);
        }
        assert_exact_series(&source, &replayed);
        let mut restored = checkpoint(&source);
        assert_exact_series(&source, &restored);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(20, 17.5));
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![(0, vec![2.5]), (10, vec![10.0])])
            );
            assert!(
                engine.take_batch().is_none(),
                "read-only queries must not record mutations"
            );
        }
        persisted_run(&mut source, &mut replayed, &["TS.ADD", "r", "35", "40"]);
        run(&mut restored, &["TS.ADD", "r", "35", "40"]);
        restored.take_batch();
        assert_exact_series(&source, &replayed);
        assert_exact_series(&source, &restored);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![(0, vec![2.5]), (10, vec![10.0]), (20, vec![21.25])])
            );
            assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(30, 35.0));
        }
    }
    #[test]
    fn twa_predecessor_distinguishes_nan_overwrite_from_retention_eviction() {
        for (retention, timestamp, value, expected) in
            [("10", "15", "nan", 20.0), ("5", "25", "40", 55.0)]
        {
            let mut source = Engine::new();
            let mut replayed = Engine::new();
            for args in [
                vec!["TS.CREATE", "r", "RETENTION", retention],
                vec!["TS.CREATE", "d"],
                vec!["TS.CREATERULE", "r", "d", "AGGREGATION", "twa", "10"],
                vec!["TS.MADD", "r", "5", "0", "r", "15", "100", "r", "25", "20"],
            ] {
                persisted_run(&mut source, &mut replayed, &args);
            }
            let mut restored = checkpoint(&source);
            let overwrite = ["TS.ADD", "r", timestamp, value, "ON_DUPLICATE", "LAST"];
            persisted_run(&mut source, &mut replayed, &overwrite);
            run(&mut restored, &overwrite);
            restored.take_batch();
            assert_exact_series(&source, &replayed);
            assert_exact_series(&source, &restored);
            let mut after = checkpoint(&source);
            assert_exact_series(&source, &after);
            for engine in [&mut source, &mut replayed, &mut restored, &mut after] {
                assert_eq!(run(engine, &["TS.GET", "d", "LATEST"]), pair(20, expected));
            }
        }
    }
    #[test]
    fn aligned_clipped_first_bucket_preserves_grid_in_both_directions() {
        let mut e = Engine::new();
        run(&mut e, &["TS.ADD", "s", "0", "1"]);
        run(&mut e, &["TS.ADD", "s", "25", "2"]);
        e.take_batch();
        for (label, times) in [
            ("START", [0, 5, 15, 25]),
            ("END", [10, 15, 25, 35]),
            ("MID", [5, 10, 20, 30]),
        ] {
            for reverse in [false, true] {
                let command = if reverse { "TS.REVRANGE" } else { "TS.RANGE" };
                let mut expected = vec![
                    (times[0], vec![1.0]),
                    (times[1], vec![0.0]),
                    (times[2], vec![0.0]),
                    (times[3], vec![2.0]),
                ];
                if reverse {
                    expected.reverse();
                }
                assert_eq!(
                    run(
                        &mut e,
                        &[
                            command,
                            "s",
                            "-",
                            "+",
                            "ALIGN",
                            "5",
                            "AGGREGATION",
                            "sum",
                            "10",
                            "BUCKETTIMESTAMP",
                            label,
                            "EMPTY"
                        ]
                    ),
                    rows_reply(expected.clone())
                );
                expected.truncate(2);
                assert_eq!(
                    run(
                        &mut e,
                        &[
                            command,
                            "s",
                            "-",
                            "+",
                            "COUNT",
                            "2",
                            "ALIGN",
                            "5",
                            "AGGREGATION",
                            "sum",
                            "10",
                            "BUCKETTIMESTAMP",
                            label,
                            "EMPTY"
                        ]
                    ),
                    rows_reply(expected)
                );
            }
        }
        assert!(e.take_batch().is_none());
    }
    #[test]
    fn clipped_aligned_twa_uses_true_edge_for_queries_and_compaction() {
        let mut source = Engine::new();
        let mut replayed = Engine::new();
        for args in [
            vec!["TS.CREATE", "r"],
            vec!["TS.CREATE", "d"],
            vec!["TS.CREATERULE", "r", "d", "AGGREGATION", "twa", "10", "5"],
            vec![
                "TS.MADD", "r", "0", "0", "r", "2", "4", "r", "5", "10", "r", "25", "50",
            ],
        ] {
            persisted_run(&mut source, &mut replayed, &args);
        }
        let mut restored = checkpoint(&source);
        assert_exact_series(&source, &replayed);
        assert_exact_series(&source, &restored);
        for engine in [&mut source, &mut replayed, &mut restored] {
            assert_eq!(
                run(engine, &["TS.RANGE", "d", "-", "+"]),
                rows_reply(vec![(0, vec![5.0]), (5, vec![20.0])])
            );
            for reverse in [false, true] {
                let mut expected = vec![
                    (0, vec![5.0]),
                    (5, vec![20.0]),
                    (15, vec![40.0]),
                    (25, vec![50.0]),
                ];
                if reverse {
                    expected.reverse();
                }
                assert_eq!(
                    run(
                        engine,
                        &[
                            if reverse { "TS.REVRANGE" } else { "TS.RANGE" },
                            "r",
                            "-",
                            "+",
                            "ALIGN",
                            "5",
                            "AGGREGATION",
                            "twa",
                            "10",
                            "EMPTY"
                        ]
                    ),
                    rows_reply(expected)
                );
            }
            assert!(engine.take_batch().is_none());
        }
        for args in [
            vec!["TS.ADD", "r", "0", "10", "ON_DUPLICATE", "LAST"],
            vec!["TS.DEL", "r", "0", "0"],
        ] {
            persisted_run(&mut source, &mut replayed, &args);
            run(&mut restored, &args);
            restored.take_batch();
            assert_exact_series(&source, &replayed);
            assert_exact_series(&source, &restored);
            for engine in [&mut source, &mut replayed, &mut restored] {
                assert_eq!(
                    run(engine, &["TS.RANGE", "d", "0", "0"]),
                    rows_reply(vec![(0, vec![7.0])])
                );
            }
        }
    }
}
