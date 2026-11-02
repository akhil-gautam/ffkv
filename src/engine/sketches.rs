//! Bounded count-min, HeavyKeeper and merging t-digest sketches.
//!
//! Journal effects contain resolved counters/heap slots/centroids, never random
//! draws or a command that would need to be executed a second time on recovery.
use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::*;
use std::collections::BTreeMap;
use std::io;

const CMS_KIND: u8 = 7;
const TOPK_KIND: u8 = 8;
const TD_KIND: u8 = 9;
const MAX_CELLS: usize = 8 * 1024 * 1024;
const MAX_COMPRESSION: usize = 1_000_000;
const ROW: usize = 4096;
const MAX_COUNT: u64 = i64::MAX as u64;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "cms.initbydim",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::cms_init,
    ),
    CommandSpec::handler(
        "cms.initbyprob",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::cms_init,
    ),
    CommandSpec::handler(
        "cms.incrby",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::cms_incrby,
    ),
    CommandSpec::handler("cms.query", -3, &["readonly"], (1, 1, 1), Engine::cms_query),
    CommandSpec::handler(
        "cms.merge",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::cms_merge,
    ),
    CommandSpec::handler("cms.info", 2, &["readonly"], (1, 1, 1), Engine::cms_info),
    CommandSpec::handler(
        "topk.reserve",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::topk_reserve,
    ),
    CommandSpec::handler(
        "topk.add",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::topk_update,
    ),
    CommandSpec::handler(
        "topk.incrby",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::topk_update,
    ),
    CommandSpec::handler(
        "topk.query",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::topk_query,
    ),
    CommandSpec::handler(
        "topk.count",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::topk_query,
    ),
    CommandSpec::handler("topk.list", -2, &["readonly"], (1, 1, 1), Engine::topk_list),
    CommandSpec::handler("topk.info", 2, &["readonly"], (1, 1, 1), Engine::topk_info),
    CommandSpec::handler(
        "tdigest.create",
        -2,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::td_create,
    ),
    CommandSpec::handler(
        "tdigest.reset",
        2,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::td_reset,
    ),
    CommandSpec::handler(
        "tdigest.add",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::td_add,
    ),
    CommandSpec::handler(
        "tdigest.merge",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::td_merge,
    ),
    CommandSpec::handler("tdigest.min", 2, &["readonly"], (1, 1, 1), Engine::td_query),
    CommandSpec::handler("tdigest.max", 2, &["readonly"], (1, 1, 1), Engine::td_query),
    CommandSpec::handler(
        "tdigest.quantile",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.cdf",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.trimmed_mean",
        4,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.rank",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.revrank",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.byrank",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler(
        "tdigest.byrevrank",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::td_query,
    ),
    CommandSpec::handler("tdigest.info", 2, &["readonly"], (1, 1, 1), Engine::td_info),
];

fn bad_effect() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid sketch effect")
}
fn dimensions(width: usize, depth: usize) -> io::Result<usize> {
    width
        .checked_mul(depth)
        .filter(|&n| width > 0 && depth > 0 && n <= MAX_CELLS)
        .ok_or_else(bad_effect)
}
fn positive(value: &[u8]) -> Option<usize> {
    integer(value)
        .and_then(|v| usize::try_from(v).ok())
        .filter(|&v| v > 0)
}
fn number(value: &[u8]) -> Option<f64> {
    let text = std::str::from_utf8(value).ok()?;
    if text.trim() != text || text.is_empty() {
        return None;
    }
    text.parse::<f64>().ok().filter(|v| !v.is_nan())
}
fn finite(value: &[u8]) -> Option<f64> {
    number(value).filter(|v| v.is_finite())
}
fn info(values: Vec<(&'static str, Resp)>) -> Resp {
    Resp::Map(
        values
            .into_iter()
            .map(|(name, value)| (bulk(name), value))
            .collect(),
    )
}
fn checked_count(value: i128, max: u64) -> io::Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|&v| v <= max)
        .ok_or_else(bad_effect)
}
// MurmurHash64A is public-domain; the wrapping operations are part of the hash.
fn hash(data: &[u8], seed: u64) -> u64 {
    const M: u64 = 0xc6a4a7935bd1e995;
    let mut h = seed ^ (data.len() as u64).wrapping_mul(M);
    let (chunks, remainder) = data.as_chunks::<8>();
    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> 47;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    let mut tail = 0u64;
    for (i, &b) in remainder.iter().enumerate() {
        tail |= u64::from(b) << (8 * i);
    }
    h ^= tail;
    if !remainder.is_empty() {
        h = h.wrapping_mul(M);
    }
    h ^= h >> 47;
    h = h.wrapping_mul(M);
    h ^ (h >> 47)
}

pub(super) struct CmsValue {
    width: usize,
    depth: usize,
    cell_size: u8,
    count: u64,
    cells: Vec<u64>,
}
impl CmsValue {
    pub(super) fn memory_footprint(&self, _samples: usize) -> usize {
        super::memory::allocation::<u64>(self.cells.capacity())
    }

    fn new(width: usize, depth: usize, cell_size: u8) -> io::Result<Self> {
        let size = dimensions(width, depth)?;
        if !matches!(cell_size, 1 | 2 | 4 | 8) {
            return Err(bad_effect());
        }
        Ok(Self {
            width,
            depth,
            cell_size,
            count: 0,
            cells: vec![0; size],
        })
    }
    fn max(&self) -> u64 {
        if self.cell_size == 8 {
            MAX_COUNT
        } else {
            (1u64 << (self.cell_size * 8)) - 1
        }
    }
    fn indices<'a>(&'a self, item: &'a [u8]) -> impl Iterator<Item = usize> + 'a {
        (0..self.depth)
            .map(move |row| row * self.width + hash(item, row as u64) as usize % self.width)
    }
    fn query(&self, item: &[u8]) -> u64 {
        self.indices(item).map(|i| self.cells[i]).min().unwrap_or(0)
    }
    fn init_effect(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 1);
        e.u32(self.width as u32)?;
        e.u32(self.depth as u32)?;
        e.u8(self.cell_size)?;
        Ok(e.finish())
    }
    fn row_effect(&self, start: usize, end: usize) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 2);
        e.u64(self.count)?;
        e.u32(start as u32)?;
        e.u32((end - start) as u32)?;
        for &cell in &self.cells[start..end] {
            e.u64(cell)?;
        }
        Ok(e.finish())
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op != 1 {
            return Err(bad_effect());
        }
        let width = d.u32()? as usize;
        let depth = d.u32()? as usize;
        let size = d.u8()?;
        d.finish()?;
        Self::new(width, depth, size)
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op != 2 && op != 3 {
            return Err(bad_effect());
        }
        let total = d.u64()?;
        if total > MAX_COUNT {
            return Err(bad_effect());
        }
        let start = if op == 2 { d.u32()? as usize } else { 0 };
        let n = d.u32()? as usize;
        if n > self.cells.len()
            || (op == 2
                && start
                    .checked_add(n)
                    .is_none_or(|end| end > self.cells.len()))
        {
            return Err(bad_effect());
        }
        let mut updates = Vec::new();
        for offset in 0..n {
            let index = if op == 2 {
                start + offset
            } else {
                d.u32()? as usize
            };
            let value = d.u64()?;
            if index >= self.cells.len() || value > self.max() || value > total {
                return Err(bad_effect());
            }
            updates.push((index, value));
        }
        d.finish()?;
        for (index, value) in updates {
            self.cells[index] = value;
        }
        self.count = total;
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot::Cms {
            value: self,
            cursor: 0,
            initialized: false,
        }
    }
    fn plan(&self, pairs: &[(Bytes, i64)]) -> Result<(Vec<Bytes>, Vec<Resp>), Resp> {
        let mut updates = BTreeMap::new();
        let mut total = self.count;
        let mut replies = Vec::with_capacity(pairs.len());
        for (item, delta) in pairs {
            let fail = || {
                error(if *delta < 0 {
                    "CMS: INCRBY underflow"
                } else {
                    "CMS: INCRBY overflow"
                })
            };
            total = checked_count(i128::from(total) + i128::from(*delta), MAX_COUNT)
                .map_err(|_| fail())?;
            let mut estimate = MAX_COUNT;
            for index in self.indices(item) {
                let old = updates.get(&index).copied().unwrap_or(self.cells[index]);
                let value = checked_count(i128::from(old) + i128::from(*delta), self.max())
                    .map_err(|_| fail())?;
                updates.insert(index, value);
                estimate = estimate.min(value);
            }
            replies.push(Resp::Integer(estimate as i64));
        }
        let mut effects = Vec::new();
        let rows: Vec<_> = updates.into_iter().collect();
        for chunk in rows.chunks(ROW) {
            let mut e = Encoder::new(1, 3);
            e.u64(total).map_err(codec_error)?;
            e.u32(chunk.len() as u32).map_err(codec_error)?;
            for &(index, value) in chunk {
                e.u32(index as u32).map_err(codec_error)?;
                e.u64(value).map_err(codec_error)?;
            }
            effects.push(e.finish());
        }
        Ok((effects, replies))
    }
}

#[derive(Clone, Copy, Default)]
struct Bucket {
    fingerprint: u64,
    count: u64,
}
#[derive(Clone)]
struct HeapItem {
    item: Bytes,
    count: u64,
}
pub(super) struct TopKValue {
    k: usize,
    width: usize,
    depth: usize,
    decay: f64,
    rng: u64,
    buckets: Vec<Bucket>,
    heap: Vec<HeapItem>,
    positions: HashMap<Bytes, usize>,
}
fn draw(state: &mut u64) -> f64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
    // Strictly between zero and one: geometric waiting times must be finite.
    (((z ^ (z >> 31)) >> 11) as f64 / 9007199254740992.0 + 0.5 / 9007199254740992.0)
        .min(f64::from_bits(1.0f64.to_bits() - 1))
}
impl TopKValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{allocation, hash_bytes, sampled};
        allocation::<Bucket>(self.buckets.capacity())
            .saturating_add(allocation::<HeapItem>(self.heap.capacity()))
            .saturating_add(hash_bytes::<Bytes, usize>(self.positions.capacity()))
            .saturating_add(sampled(self.heap.iter(), samples, |item| item.item.len()))
    }

    fn new(k: usize, width: usize, depth: usize, decay: f64, rng: u64) -> io::Result<Self> {
        let size = dimensions(width, depth)?;
        if k == 0 || k > MAX_CELLS || !decay.is_finite() || decay <= 0.0 || decay > 1.0 {
            return Err(bad_effect());
        }
        Ok(Self {
            k,
            width,
            depth,
            decay,
            rng,
            buckets: vec![Bucket::default(); size],
            heap: Vec::new(),
            positions: HashMap::new(),
        })
    }
    fn estimate(&self, item: &[u8]) -> u64 {
        let fingerprint = hash(item, 1919);
        (0..self.depth)
            .map(|row| {
                self.buckets[row * self.width + hash(item, row as u64) as usize % self.width]
            })
            .filter(|b| b.fingerprint == fingerprint)
            .map(|b| b.count)
            .max()
            .unwrap_or(0)
    }
    fn init_effect(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 1);
        e.u32(self.k as u32)?;
        e.u32(self.width as u32)?;
        e.u32(self.depth as u32)?;
        e.f64(self.decay)?;
        e.u64(self.rng)?;
        Ok(e.finish())
    }
    fn bucket_effect(&self, start: usize, end: usize) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 2);
        e.u32(start as u32)?;
        e.u32((end - start) as u32)?;
        for b in &self.buckets[start..end] {
            e.u64(b.fingerprint)?;
            e.u64(b.count)?;
        }
        Ok(e.finish())
    }
    fn heap_effect(&self, index: usize) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 4);
        e.u32(index as u32)?;
        e.bytes(&self.heap[index].item)?;
        e.u64(self.heap[index].count)?;
        Ok(e.finish())
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op != 1 {
            return Err(bad_effect());
        }
        let k = d.u32()? as usize;
        let width = d.u32()? as usize;
        let depth = d.u32()? as usize;
        let decay = d.f64()?;
        let rng = d.u64()?;
        d.finish()?;
        Self::new(k, width, depth, decay, rng)
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        match op {
            2 => {
                let start = d.u32()? as usize;
                let n = d.u32()? as usize;
                if start
                    .checked_add(n)
                    .is_none_or(|end| end > self.buckets.len())
                {
                    return Err(bad_effect());
                }
                let mut rows = Vec::new();
                for _ in 0..n {
                    let fingerprint = d.u64()?;
                    let count = d.u64()?;
                    if count > MAX_COUNT {
                        return Err(bad_effect());
                    }
                    rows.push(Bucket { fingerprint, count });
                }
                d.finish()?;
                self.buckets[start..start + n].copy_from_slice(&rows);
            }
            3 => {
                let rng = d.u64()?;
                let n = d.u32()? as usize;
                if n > self.buckets.len() {
                    return Err(bad_effect());
                }
                let mut buckets = Vec::new();
                for _ in 0..n {
                    let index = d.u32()? as usize;
                    let fingerprint = d.u64()?;
                    let count = d.u64()?;
                    if index >= self.buckets.len() || count > MAX_COUNT {
                        return Err(bad_effect());
                    }
                    buckets.push((index, Bucket { fingerprint, count }));
                }
                d.finish()?;
                for (index, bucket) in buckets {
                    self.buckets[index] = bucket;
                }
                self.rng = rng;
            }
            4 => {
                let index = d.u32()? as usize;
                let item = Bytes::copy_from_slice(d.bytes()?);
                let count = d.u64()?;
                if index > self.heap.len() || index >= self.k || count > MAX_COUNT {
                    return Err(bad_effect());
                }
                d.finish()?;
                if let Some(old) = self.heap.get(index)
                    && self.positions.get(&old.item) == Some(&index)
                {
                    self.positions.remove(&old.item);
                }
                self.positions.insert(item.clone(), index);
                if index == self.heap.len() {
                    self.heap.push(HeapItem { item, count });
                } else {
                    self.heap[index] = HeapItem { item, count };
                }
            }
            _ => return Err(bad_effect()),
        }
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot::TopK {
            value: self,
            cursor: 0,
            heap: 0,
            initialized: false,
        }
    }
    fn plan(&self, pairs: &[(Bytes, u64)]) -> Result<(Vec<Bytes>, Vec<Resp>), Resp> {
        let mut buckets = BTreeMap::new();
        let mut heap = HeapOverlay {
            base: &self.heap,
            positions: &self.positions,
            changes: BTreeMap::new(),
            membership: HashMap::new(),
            len: self.heap.len(),
        };
        let mut rng = self.rng;
        let mut replies = Vec::new();
        for (item, increment) in pairs {
            let fingerprint = hash(item, 1919);
            let mut estimate = 0;
            for row in 0..self.depth {
                let index = row * self.width + hash(item, row as u64) as usize % self.width;
                let mut bucket = buckets.get(&index).copied().unwrap_or(self.buckets[index]);
                if bucket.count == 0 || bucket.fingerprint == fingerprint {
                    bucket.fingerprint = fingerprint;
                    bucket.count = bucket
                        .count
                        .checked_add(*increment)
                        .filter(|&v| v <= MAX_COUNT)
                        .ok_or_else(|| error("TOPK: counter overflow"))?;
                } else {
                    let mut remaining = *increment;
                    while remaining > 0 && bucket.count > 0 {
                        let probability = self.decay.powf(bucket.count as f64);
                        if probability == 0.0 {
                            break;
                        }
                        let waiting = if probability == 1.0 {
                            1.0
                        } else {
                            (draw(&mut rng).ln() / (-probability).ln_1p()).floor() + 1.0
                        };
                        if waiting > remaining as f64 {
                            break;
                        }
                        remaining -= waiting as u64;
                        bucket.count -= 1;
                    }
                    if bucket.count == 0 {
                        bucket.fingerprint = fingerprint;
                        bucket.count = remaining + 1;
                    }
                }
                if bucket.fingerprint == fingerprint {
                    estimate = estimate.max(bucket.count);
                }
                buckets.insert(index, bucket);
            }
            let mut expelled = None;
            if let Some(index) = heap.position(item) {
                heap.set(
                    index,
                    HeapItem {
                        item: item.clone(),
                        count: estimate,
                    },
                );
                heap.fix(index);
            } else if heap.len < self.k {
                let index = heap.len;
                heap.len += 1;
                heap.set(
                    index,
                    HeapItem {
                        item: item.clone(),
                        count: estimate,
                    },
                );
                heap.fix(index);
            } else if estimate >= heap.get(0).count && estimate > 0 {
                let removed = heap.get(0).item.clone();
                heap.membership.insert(removed.clone(), None);
                expelled = Some(removed);
                heap.set(
                    0,
                    HeapItem {
                        item: item.clone(),
                        count: estimate,
                    },
                );
                heap.fix(0);
            }
            replies.push(Resp::Bulk(expelled));
        }
        let mut effects = Vec::new();
        let buckets: Vec<_> = buckets.into_iter().collect();
        for chunk in buckets.chunks(ROW) {
            let mut e = Encoder::new(1, 3);
            e.u64(rng).map_err(codec_error)?;
            e.u32(chunk.len() as u32).map_err(codec_error)?;
            for &(index, b) in chunk {
                e.u32(index as u32).map_err(codec_error)?;
                e.u64(b.fingerprint).map_err(codec_error)?;
                e.u64(b.count).map_err(codec_error)?;
            }
            effects.push(e.finish());
        }
        for (index, entry) in heap.changes {
            let mut e = Encoder::new(1, 4);
            e.u32(index as u32).map_err(codec_error)?;
            e.bytes(&entry.item).map_err(codec_error)?;
            e.u64(entry.count).map_err(codec_error)?;
            effects.push(e.finish());
        }
        Ok((effects, replies))
    }
}
// A sparse transaction overlay keeps updates atomic without cloning the table
// or the K retained strings on each ADD. Only touched heap paths are copied.
struct HeapOverlay<'a> {
    base: &'a [HeapItem],
    positions: &'a HashMap<Bytes, usize>,
    changes: BTreeMap<usize, HeapItem>,
    membership: HashMap<Bytes, Option<usize>>,
    len: usize,
}
impl HeapOverlay<'_> {
    fn get(&self, i: usize) -> &HeapItem {
        self.changes.get(&i).unwrap_or_else(|| &self.base[i])
    }
    fn position(&self, item: &Bytes) -> Option<usize> {
        self.membership
            .get(item)
            .copied()
            .unwrap_or_else(|| self.positions.get(item).copied())
    }
    fn set(&mut self, i: usize, item: HeapItem) {
        self.membership.insert(item.item.clone(), Some(i));
        self.changes.insert(i, item);
    }
    fn swap(&mut self, a: usize, b: usize) {
        let x = self.get(a).clone();
        let y = self.get(b).clone();
        self.set(a, y);
        self.set(b, x);
    }
    fn fix(&mut self, mut i: usize) {
        while i > 0 {
            let p = (i - 1) / 2;
            if self.get(p).count <= self.get(i).count {
                break;
            }
            self.swap(p, i);
            i = p;
        }
        loop {
            let l = i * 2 + 1;
            if l >= self.len {
                break;
            }
            let r = l + 1;
            let child = if r < self.len && self.get(r).count < self.get(l).count {
                r
            } else {
                l
            };
            if self.get(i).count <= self.get(child).count {
                break;
            }
            self.swap(i, child);
            i = child;
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct Centroid {
    mean: f64,
    weight: u64,
}
pub(super) struct TDigestValue {
    compression: usize,
    merged: Vec<Centroid>,
    pending: Vec<Centroid>,
    count: u64,
    min: f64,
    max: f64,
    compressions: u64,
    // Derived only: reads must not alter the journaled compression schedule.
    view: std::sync::OnceLock<Vec<Centroid>>,
}
fn capacity(compression: usize) -> io::Result<usize> {
    if compression == 0 || compression > MAX_COMPRESSION {
        return Err(bad_effect());
    }
    compression
        .checked_mul(6)
        .and_then(|v| v.checked_add(10))
        .ok_or_else(bad_effect)
}
fn blend(a: f64, b: f64, fraction: f64) -> f64 {
    if a == b {
        a
    } else if a.signum() == b.signum() {
        a + (b - a) * fraction
    } else {
        a * (1.0 - fraction) + b * fraction
    }
}
impl TDigestValue {
    pub(super) fn memory_footprint(&self, _samples: usize) -> usize {
        use super::memory::allocation;
        allocation::<Centroid>(self.merged.capacity())
            .saturating_add(allocation::<Centroid>(self.pending.capacity()))
            .saturating_add(
                self.view
                    .get()
                    .map_or(0, |view| allocation::<Centroid>(view.capacity())),
            )
    }

    fn new(compression: usize) -> io::Result<Self> {
        capacity(compression)?;
        Ok(Self {
            compression,
            merged: Vec::new(),
            pending: Vec::new(),
            count: 0,
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            compressions: 0,
            view: std::sync::OnceLock::new(),
        })
    }
    fn nodes(&self) -> usize {
        self.merged.len() + self.pending.len()
    }
    fn init_effect(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 1);
        e.u32(self.compression as u32)?;
        Ok(e.finish())
    }
    fn metadata(&self, e: &mut Encoder) -> io::Result<()> {
        e.u64(self.count)?;
        e.f64(self.min)?;
        e.f64(self.max)?;
        e.u64(self.compressions)?;
        Ok(())
    }
    fn append_effect(
        &self,
        nodes: &[Centroid],
        total: u64,
        min: f64,
        max: f64,
    ) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 2);
        e.u64(total)?;
        e.f64(min)?;
        e.f64(max)?;
        e.u32(nodes.len() as u32)?;
        for c in nodes {
            e.f64(c.mean)?;
            e.u64(c.weight)?;
        }
        Ok(e.finish())
    }
    fn state_effect(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 3);
        e.u32(self.compression as u32)?;
        self.metadata(&mut e)?;
        e.u32(self.merged.len() as u32)?;
        e.u32(self.pending.len() as u32)?;
        for c in self.merged.iter().chain(&self.pending) {
            e.f64(c.mean)?;
            e.u64(c.weight)?;
        }
        Ok(e.finish())
    }
    fn state_effects(&self) -> io::Result<Vec<Bytes>> {
        if self.nodes() <= ROW {
            return Ok(vec![self.state_effect()?]);
        }
        let mut e = Encoder::new(1, 6);
        e.u32(self.compression as u32)?;
        self.metadata(&mut e)?;
        let mut effects = vec![e.finish()];
        for pending in [false, true] {
            let len = if pending {
                self.pending.len()
            } else {
                self.merged.len()
            };
            for start in (0..len).step_by(ROW) {
                effects.push(self.snapshot_row(pending, start, (start + ROW).min(len))?);
            }
        }
        Ok(effects)
    }
    fn snapshot_row(&self, pending: bool, start: usize, end: usize) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 4);
        self.metadata(&mut e)?;
        e.u8(u8::from(pending))?;
        e.u32(start as u32)?;
        e.u32((end - start) as u32)?;
        let rows = if pending { &self.pending } else { &self.merged };
        for c in &rows[start..end] {
            e.f64(c.mean)?;
            e.u64(c.weight)?;
        }
        Ok(e.finish())
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op != 1 {
            return Err(bad_effect());
        }
        let compression = d.u32()? as usize;
        d.finish()?;
        Self::new(compression)
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op == 5 {
            d.finish()?;
            let _ = self.view.take();
            self.merged.clear();
            self.pending.clear();
            self.count = 0;
            self.min = f64::INFINITY;
            self.max = f64::NEG_INFINITY;
            self.compressions = 0;
            return Ok(());
        }
        if !matches!(op, 2 | 3 | 4 | 6) {
            return Err(bad_effect());
        }
        let compression = if op == 3 || op == 6 {
            d.u32()? as usize
        } else {
            self.compression
        };
        let limit = capacity(compression)?;
        let count = d.u64()?;
        let min = d.f64()?;
        let max = d.f64()?;
        let compressions = if op == 2 { self.compressions } else { d.u64()? };
        if count > MAX_COUNT
            || (count == 0 && (min != f64::INFINITY || max != f64::NEG_INFINITY))
            || (count > 0 && (!min.is_finite() || !max.is_finite() || min > max))
        {
            return Err(bad_effect());
        }
        if op == 6 {
            d.finish()?;
            let _ = self.view.take();
            self.merged.clear();
            self.pending.clear();
            self.compression = compression;
            self.count = count;
            self.min = min;
            self.max = max;
            self.compressions = compressions;
            return Ok(());
        }
        let (merged_n, pending_n, pending, start) = match op {
            2 => (0, d.u32()? as usize, true, self.pending.len()),
            3 => (d.u32()? as usize, d.u32()? as usize, false, 0),
            4 => {
                let p = d.u8()?;
                if p > 1 {
                    return Err(bad_effect());
                }
                let start = d.u32()? as usize;
                let n = d.u32()? as usize;
                (
                    if p == 0 { n } else { 0 },
                    if p == 1 { n } else { 0 },
                    p == 1,
                    start,
                )
            }
            _ => unreachable!(),
        };
        let n = merged_n
            .checked_add(pending_n)
            .filter(|&n| n <= limit)
            .ok_or_else(bad_effect)?;
        if op != 3 {
            let rows = if pending { &self.pending } else { &self.merged };
            if start != rows.len() || self.nodes().checked_add(n).is_none_or(|v| v > limit) {
                return Err(bad_effect());
            }
        }
        let mut nodes = Vec::new();
        let mut weight = 0u64;
        for _ in 0..n {
            let mean = d.f64()?;
            let w = d.u64()?;
            if !mean.is_finite() || w == 0 || mean < min || mean > max {
                return Err(bad_effect());
            }
            weight = weight
                .checked_add(w)
                .filter(|&v| v <= MAX_COUNT)
                .ok_or_else(bad_effect)?;
            nodes.push(Centroid { mean, weight: w });
        }
        d.finish()?;
        if op == 3 && weight != count {
            return Err(bad_effect());
        }
        if op == 2
            && (self.count.checked_add(weight) != Some(count) || min > self.min || max < self.max)
        {
            return Err(bad_effect());
        }
        if op == 4 {
            let existing = self
                .merged
                .iter()
                .chain(&self.pending)
                .try_fold(0u64, |sum, c| sum.checked_add(c.weight))
                .ok_or_else(bad_effect)?;
            if existing.checked_add(weight).is_none_or(|sum| sum > count) {
                return Err(bad_effect());
            }
            if self.count > 0
                && (self.count != count
                    || self.min != min
                    || self.max != max
                    || self.compressions != compressions)
            {
                return Err(bad_effect());
            }
        }
        if op == 3 {
            let pending_nodes = nodes.split_off(merged_n);
            if nodes.windows(2).any(|w| w[0].mean > w[1].mean) {
                return Err(bad_effect());
            }
            self.merged = nodes;
            self.pending = pending_nodes;
            self.compression = compression;
        } else if pending {
            self.pending.extend(nodes);
        } else {
            if self
                .merged
                .last()
                .zip(nodes.first())
                .is_some_and(|(a, b)| a.mean > b.mean)
                || nodes.windows(2).any(|w| w[0].mean > w[1].mean)
            {
                return Err(bad_effect());
            }
            self.merged.extend(nodes);
        }
        let _ = self.view.take();
        self.count = count;
        self.min = min;
        self.max = max;
        self.compressions = compressions;
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot::TDigest {
            value: self,
            cursor: 0,
            pending: false,
            initialized: false,
            metadata: false,
        }
    }
    fn compress_nodes(mut nodes: Vec<Centroid>, compression: usize, total: u64) -> Vec<Centroid> {
        if nodes.is_empty() {
            return nodes;
        }
        nodes.sort_unstable_by(|a, b| a.mean.total_cmp(&b.mean));
        let scale = |q: f64| {
            compression as f64
                * ((2.0 * q - 1.0).clamp(-1.0, 1.0).asin() + std::f64::consts::FRAC_PI_2)
                / std::f64::consts::PI
        };
        let mut output = Vec::with_capacity(compression.min(nodes.len()) + 2);
        let mut current = nodes[0];
        let mut before = 0u64;
        for next in nodes.into_iter().skip(1) {
            let proposed = current.weight + next.weight;
            let q0 = before as f64 / total as f64;
            let q2 = (before + proposed) as f64 / total as f64;
            if current.mean == next.mean || scale(q2) - scale(q0) <= 1.0 {
                current.mean = blend(
                    current.mean,
                    next.mean,
                    next.weight as f64 / proposed as f64,
                );
                current.weight = proposed;
            } else {
                before += current.weight;
                output.push(current);
                current = next;
            }
        }
        output.push(current);
        output
    }
    fn view(&self) -> &[Centroid] {
        self.view.get_or_init(|| {
            Self::compress_nodes(
                self.merged.iter().chain(&self.pending).copied().collect(),
                self.compression,
                self.count,
            )
        })
    }
    fn plan_add(&self, values: &[f64]) -> Result<Vec<Bytes>, Resp> {
        let total = self
            .count
            .checked_add(values.len() as u64)
            .filter(|&n| n <= MAX_COUNT)
            .ok_or_else(|| error("TDIGEST: observation count overflow"))?;
        let min = values.iter().copied().fold(self.min, f64::min);
        let max = values.iter().copied().fold(self.max, f64::max);
        let limit = capacity(self.compression).map_err(codec_error)?;
        if self.nodes() + values.len() <= limit {
            let mut effects = Vec::new();
            let mut count = self.count;
            for chunk in values.chunks(ROW) {
                let nodes: Vec<_> = chunk
                    .iter()
                    .map(|&mean| Centroid { mean, weight: 1 })
                    .collect();
                count += chunk.len() as u64;
                effects.push(
                    self.append_effect(&nodes, count, min, max)
                        .map_err(codec_error)?,
                );
            }
            return Ok(effects);
        }
        // Compression is the only update that copies the old bounded centroid
        // arrays. Large ADD batches are streamed through the same bounded buffer.
        let mut nodes: Vec<_> = self.merged.iter().chain(&self.pending).copied().collect();
        let mut compressions = self.compressions;
        for (offset, &mean) in values.iter().enumerate() {
            let weight = self.count + offset as u64;
            if nodes.len() >= limit {
                nodes = Self::compress_nodes(nodes, self.compression, weight);
                compressions = compressions
                    .checked_add(1)
                    .ok_or_else(|| error("TDIGEST: compression count overflow"))?;
            }
            nodes.push(Centroid { mean, weight: 1 });
        }
        nodes = Self::compress_nodes(nodes, self.compression, total);
        compressions = compressions
            .checked_add(1)
            .ok_or_else(|| error("TDIGEST: compression count overflow"))?;
        let value = Self {
            compression: self.compression,
            merged: nodes,
            pending: Vec::new(),
            count: total,
            min,
            max,
            compressions,
            view: std::sync::OnceLock::new(),
        };
        value.state_effects().map_err(codec_error)
    }
    fn quantile(&self, q: f64) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        let total = self.count as f64;
        let target = q * total;
        if q <= 0.0 || target < 1.0 {
            return self.min;
        }
        if q >= 1.0 || target > total - 1.0 {
            return self.max;
        }
        if self.min == self.max {
            return self.min;
        }
        let nodes = self.view();
        let first = nodes[0];
        let mut center = first.weight as f64 / 2.0;
        if nodes.len() == 1 {
            return first.mean;
        }
        // One sample is known to lie exactly at each extreme, even when the
        // outer centroid contains several samples.
        if target < center {
            return blend(
                self.min,
                first.mean,
                ((target - 1.0) / (center - 1.0)).clamp(0.0, 1.0),
            );
        }
        let mut previous = first;
        for &next in &nodes[1..] {
            let next_center = center + (previous.weight as f64 + next.weight as f64) / 2.0;
            if target < next_center {
                // A singleton's whole unit interval belongs to its exact mean;
                // only the remaining mass participates in interpolation.
                let left = if previous.weight == 1 { 0.5 } else { 0.0 };
                let right = if next.weight == 1 { 0.5 } else { 0.0 };
                if left > 0.0 && target - center < left {
                    return previous.mean;
                }
                if right > 0.0 && next_center - target <= right {
                    return next.mean;
                }
                return blend(
                    previous.mean,
                    next.mean,
                    ((target - center - left) / (next_center - center - left - right))
                        .clamp(0.0, 1.0),
                );
            }
            previous = next;
            center = next_center;
        }
        if target == center {
            return previous.mean;
        }
        blend(
            previous.mean,
            self.max,
            ((target - center) / (total - center - 1.0)).clamp(0.0, 1.0),
        )
    }
    fn cdf(&self, x: f64) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        if x < self.min {
            return 0.0;
        }
        if x > self.max {
            return 1.0;
        }
        if self.min == self.max {
            return 0.5;
        }
        let total = self.count as f64;
        let nodes = self.view();
        let first = nodes[0];
        let mut center = first.weight as f64 / 2.0;
        if nodes.len() == 1 {
            return 0.5;
        }
        if x == first.mean {
            return center / total;
        }
        if x < first.mean {
            if x == self.min {
                return 0.5 / total;
            }
            return (1.0 + (center - 1.0) * distance_fraction(self.min, first.mean, x)) / total;
        }
        let mut previous = first;
        for &next in &nodes[1..] {
            let next_center = center + (previous.weight as f64 + next.weight as f64) / 2.0;
            if x == next.mean {
                return next_center / total;
            }
            if x < next.mean {
                let left = if previous.weight == 1 { 0.5 } else { 0.0 };
                let right = if next.weight == 1 { 0.5 } else { 0.0 };
                return (center
                    + left
                    + (next_center - center - left - right)
                        * distance_fraction(previous.mean, next.mean, x))
                    / total;
            }
            previous = next;
            center = next_center;
        }
        if x == self.max {
            return 1.0 - 0.5 / total;
        }
        1.0 - (1.0 + (total - center - 1.0) * distance_fraction(self.max, previous.mean, x)) / total
    }
    fn trimmed_mean(&self, low: f64, high: f64) -> f64 {
        if self.count == 0 {
            return f64::NAN;
        }
        let nodes = self.view();
        let low = low * self.count as f64;
        let high = high * self.count as f64;
        let mut before = 0.0;
        let mut weight = 0.0;
        let mut mean = 0.0;
        for c in nodes {
            let end = before + c.weight as f64;
            let included = (end.min(high) - before.max(low)).max(0.0);
            if included > 0.0 {
                mean = blend(mean, c.mean, included / (weight + included));
                weight += included;
            }
            before = end;
        }
        mean
    }
}
fn distance_fraction(a: f64, b: f64, x: f64) -> f64 {
    if a == b {
        return 0.5;
    }
    let distance = b - a;
    if distance.is_finite() {
        ((x - a) / distance).clamp(0.0, 1.0)
    } else {
        ((x / 2.0 - a / 2.0) / (b / 2.0 - a / 2.0)).clamp(0.0, 1.0)
    }
}

pub(super) enum Snapshot<'a> {
    Cms {
        value: &'a CmsValue,
        cursor: usize,
        initialized: bool,
    },
    TopK {
        value: &'a TopKValue,
        cursor: usize,
        heap: usize,
        initialized: bool,
    },
    TDigest {
        value: &'a TDigestValue,
        cursor: usize,
        pending: bool,
        initialized: bool,
        metadata: bool,
    },
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Cms {
                value,
                cursor,
                initialized,
            } => {
                if !*initialized {
                    *initialized = true;
                    return Some(value.init_effect());
                }
                if *cursor == value.cells.len() {
                    return None;
                }
                let start = *cursor;
                *cursor = (start + ROW).min(value.cells.len());
                Some(value.row_effect(start, *cursor))
            }
            Self::TopK {
                value,
                cursor,
                heap,
                initialized,
            } => {
                if !*initialized {
                    *initialized = true;
                    return Some(value.init_effect());
                }
                if *cursor < value.buckets.len() {
                    let start = *cursor;
                    *cursor = (start + ROW).min(value.buckets.len());
                    return Some(value.bucket_effect(start, *cursor));
                }
                if *heap == value.heap.len() {
                    return None;
                }
                let index = *heap;
                *heap += 1;
                Some(value.heap_effect(index))
            }
            Self::TDigest {
                value,
                cursor,
                pending,
                initialized,
                metadata,
            } => {
                if !*initialized {
                    *initialized = true;
                    return Some(value.init_effect());
                }
                let nodes = if *pending {
                    &value.pending
                } else {
                    &value.merged
                };
                if *cursor < nodes.len() {
                    let start = *cursor;
                    *cursor = (start + ROW).min(nodes.len());
                    *metadata = true;
                    return Some(value.snapshot_row(*pending, start, *cursor));
                }
                if !*pending {
                    *pending = true;
                    *cursor = 0;
                    return self.next();
                }
                if !*metadata {
                    *metadata = true;
                    return Some(value.snapshot_row(true, 0, 0));
                }
                None
            }
        }
    }
}
fn codec_error(_: io::Error) -> Resp {
    error("ERR sketch size or numeric limit exceeded")
}

impl Engine {
    fn sketch(&mut self, key: &Bytes, kind: u8, now: i64) -> Result<&Data, Resp> {
        match self.value(key, now) {
            Some(Value::Typed(data)) if data.kind() == kind => Ok(data),
            Some(_) => Err(wrong_type()),
            None => Err(error(match kind {
                CMS_KIND => "CMS: key does not exist",
                TOPK_KIND => "TOPK: key does not exist",
                _ => "TDIGEST: key does not exist",
            })),
        }
    }
    fn sketch_create(&mut self, key: &Bytes, data: Data, effect: Bytes, now: i64) -> Resp {
        let kind = data.kind();
        let owned = self.own_key(key);
        self.write_entry(
            owned,
            Entry {
                value: Value::Typed(Box::new(data)),
                expires_at: None,
                access: crate::engine::AccessMetadata::default(),
            },
            now,
        );
        self.record_typed(key, kind, effect, now);
        Resp::Simple("OK")
    }
    fn sketch_effects(
        &mut self,
        key: &Bytes,
        kind: u8,
        effects: Vec<Bytes>,
        now: i64,
    ) -> Result<(), Resp> {
        for effect in effects {
            let Some(Entry {
                value: Value::Typed(data),
                ..
            }) = self.entries.get_mut(key)
            else {
                return Err(self.fail_io(bad_effect()));
            };
            if let Err(failure) = data.apply(kind, &effect) {
                return Err(self.fail_io(failure));
            }
            self.record_typed(key, kind, effect, now);
        }
        Ok(())
    }
    fn cms_init(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() != 4 && args.len() != 6 {
            return syntax_error();
        }
        let size = if args.len() == 6 {
            if !args[4].eq_ignore_ascii_case(b"CELL_SIZE") {
                return syntax_error();
            }
            match integer(&args[5]) {
                Some(v @ (1 | 2 | 4 | 8)) => v as u8,
                _ => return error("CMS: invalid cell size"),
            }
        } else {
            4
        };
        let (width, depth) = if args[0].eq_ignore_ascii_case(b"CMS.INITBYPROB") {
            let Some(epsilon) = finite(&args[2]).filter(|&v| v > 0.0 && v < 1.0) else {
                return error("CMS: invalid error");
            };
            let Some(probability) = finite(&args[3]).filter(|&v| v > 0.0 && v < 1.0) else {
                return error("CMS: invalid probability");
            };
            let width = (2.0 / epsilon).ceil();
            let depth = (probability.ln() / 0.5f64.ln()).ceil();
            if width > MAX_CELLS as f64 || depth > MAX_CELLS as f64 {
                return error("CMS: dimensions exceed memory limit");
            }
            (width as usize, depth as usize)
        } else {
            let Some(width) = positive(&args[2]) else {
                return error("CMS: invalid width");
            };
            let Some(depth) = positive(&args[3]) else {
                return error("CMS: invalid depth");
            };
            (width, depth)
        };
        if self.value(&args[1], now).is_some() {
            return error("CMS: key already exists");
        }
        let value = match CmsValue::new(width, depth, size) {
            Ok(value) => value,
            Err(_) => return error("CMS: dimensions exceed memory limit"),
        };
        let effect = match value.init_effect() {
            Ok(effect) => effect,
            Err(failure) => return codec_error(failure),
        };
        self.sketch_create(&args[1], Data::Cms(value), effect, now)
    }
    fn cms_incrby(&mut self, args: &[Bytes], now: i64) -> Resp {
        if !args.len().is_multiple_of(2) {
            return syntax_error();
        }
        let mut pairs = Vec::new();
        for pair in args[2..].as_chunks::<2>().0 {
            let Some(delta) = integer(&pair[1]).filter(|&v| v != i64::MIN) else {
                return integer_error();
            };
            pairs.push((pair[0].clone(), delta));
        }
        let limit = self.reply_limit;
        let value = match self.sketch(&args[1], CMS_KIND, now) {
            Ok(Data::Cms(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let (effects, replies) = match value.plan(&pairs) {
            Ok(plan) => plan,
            Err(reply) => return reply,
        };
        let reply = array(replies);
        if let Err(reply) = ReplyBudget::new(limit).response(&reply) {
            return reply;
        }
        if pairs.iter().any(|(_, delta)| *delta != 0)
            && let Err(reply) = self.sketch_effects(&args[1], CMS_KIND, effects, now)
        {
            return reply;
        }
        reply
    }
    fn cms_query(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let value = match self.sketch(&args[1], CMS_KIND, now) {
            Ok(Data::Cms(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        bounded_replies(
            args[2..]
                .iter()
                .map(|item| Resp::Integer(value.query(item) as i64)),
            limit,
        )
    }
    fn cms_info(&mut self, args: &[Bytes], now: i64) -> Resp {
        let value = match self.sketch(&args[1], CMS_KIND, now) {
            Ok(Data::Cms(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        info(vec![
            ("width", Resp::Integer(value.width as i64)),
            ("depth", Resp::Integer(value.depth as i64)),
            ("count", Resp::Integer(value.count as i64)),
            ("cell_size", Resp::Integer(value.cell_size as i64)),
        ])
    }
    fn cms_merge(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(n) = positive(&args[2]).filter(|&n| n <= args.len().saturating_sub(3)) else {
            return error("CMS: invalid numkeys");
        };
        let end = 3 + n;
        let mut weights = vec![1i64; n];
        if args.len() != end {
            if args.len() != end + 1 + n || !args[end].eq_ignore_ascii_case(b"WEIGHTS") {
                return error("CMS: wrong number of keys/weights");
            }
            for (weight, arg) in weights.iter_mut().zip(&args[end + 1..]) {
                let Some(value) = integer(arg) else {
                    return error("CMS: invalid weight value");
                };
                *weight = value;
            }
        }
        let destination = match self.sketch(&args[1], CMS_KIND, now) {
            Ok(Data::Cms(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let (width, depth, size, max) = (
            destination.width,
            destination.depth,
            destination.cell_size,
            destination.max(),
        );
        // Accumulators are separate from the destination: aliases and failed
        // signed merges always observe the original state.
        let mut sums = vec![0i128; width * depth];
        let mut total = 0i128;
        for (key, weight) in args[3..end].iter().zip(weights) {
            let source = match self.sketch(key, CMS_KIND, now) {
                Ok(Data::Cms(value)) => value,
                Ok(_) => unreachable!(),
                Err(reply) => return reply,
            };
            if source.width != width || source.depth != depth {
                return error("CMS: width/depth is not equal");
            }
            if source.cell_size != size {
                return error("CMS: cell size is not equal");
            }
            total = match total.checked_add(i128::from(source.count) * i128::from(weight)) {
                Some(v) => v,
                None => return error("CMS: MERGE overflow"),
            };
            for (sum, &cell) in sums.iter_mut().zip(&source.cells) {
                *sum = match sum.checked_add(i128::from(cell) * i128::from(weight)) {
                    Some(v) => v,
                    None => return error("CMS: MERGE overflow"),
                };
            }
        }
        let count = match checked_count(total, MAX_COUNT) {
            Ok(v) => v,
            Err(_) => return error("CMS: MERGE overflow"),
        };
        let cells = match sums
            .into_iter()
            .map(|v| checked_count(v, max))
            .collect::<io::Result<Vec<_>>>()
        {
            Ok(v) => v,
            Err(_) => return error("CMS: MERGE overflow"),
        };
        let value = CmsValue {
            width,
            depth,
            cell_size: size,
            count,
            cells,
        };
        let effects = match (0..value.cells.len())
            .step_by(ROW)
            .map(|start| value.row_effect(start, (start + ROW).min(value.cells.len())))
            .collect::<io::Result<Vec<_>>>()
        {
            Ok(effects) => effects,
            Err(failure) => return codec_error(failure),
        };
        match self.sketch_effects(&args[1], CMS_KIND, effects, now) {
            Ok(()) => Resp::Simple("OK"),
            Err(reply) => reply,
        }
    }
    fn topk_reserve(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() != 3 && args.len() != 6 {
            return syntax_error();
        }
        let Some(k) = positive(&args[2]) else {
            return error("TOPK: invalid k");
        };
        let (width, depth, decay) = if args.len() == 6 {
            let Some(w) = positive(&args[3]) else {
                return error("TOPK: invalid width");
            };
            let Some(d) = positive(&args[4]) else {
                return error("TOPK: invalid depth");
            };
            let Some(decay) = finite(&args[5]).filter(|&v| v > 0.0 && v <= 1.0) else {
                return error("TOPK: invalid decay");
            };
            (w, d, decay)
        } else {
            (8, 7, 0.9)
        };
        if self.value(&args[1], now).is_some() {
            return error("TOPK: key already exists");
        }
        let value = match TopKValue::new(k, width, depth, decay, hash(&args[1], now as u64)) {
            Ok(v) => v,
            Err(_) => return error("TOPK: dimensions exceed memory limit"),
        };
        let effect = match value.init_effect() {
            Ok(v) => v,
            Err(failure) => return codec_error(failure),
        };
        self.sketch_create(&args[1], Data::TopK(value), effect, now)
    }
    fn topk_update(&mut self, args: &[Bytes], now: i64) -> Resp {
        let add = args[0].eq_ignore_ascii_case(b"TOPK.ADD");
        if !add && !args.len().is_multiple_of(2) {
            return syntax_error();
        }
        let mut pairs = Vec::new();
        if add {
            pairs.extend(args[2..].iter().map(|item| (item.clone(), 1)));
        } else {
            for pair in args[2..].as_chunks::<2>().0 {
                let Some(increment) = integer(&pair[1]).filter(|&v| (0..=100000).contains(&v))
                else {
                    return error("TOPK: increment must be between 0 and 100000");
                };
                pairs.push((pair[0].clone(), increment as u64));
            }
        }
        let limit = self.reply_limit;
        let value = match self.sketch(&args[1], TOPK_KIND, now) {
            Ok(Data::TopK(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let (effect, replies) = match value.plan(&pairs) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        let reply = array(replies);
        if let Err(reply) = ReplyBudget::new(limit).response(&reply) {
            return reply;
        }
        match self.sketch_effects(&args[1], TOPK_KIND, effect, now) {
            Ok(()) => reply,
            Err(reply) => reply,
        }
    }
    fn topk_query(&mut self, args: &[Bytes], now: i64) -> Resp {
        let count = args[0].eq_ignore_ascii_case(b"TOPK.COUNT");
        let limit = self.reply_limit;
        let value = match self.sketch(&args[1], TOPK_KIND, now) {
            Ok(Data::TopK(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        bounded_replies(
            args[2..].iter().map(|item| {
                if count {
                    Resp::Integer(value.estimate(item) as i64)
                } else {
                    Resp::Boolean(value.positions.contains_key(item))
                }
            }),
            limit,
        )
    }
    fn topk_list(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() > 3 || (args.len() == 3 && !args[2].eq_ignore_ascii_case(b"WITHCOUNT")) {
            return syntax_error();
        }
        let limit = self.reply_limit;
        let value = match self.sketch(&args[1], TOPK_KIND, now) {
            Ok(Data::TopK(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let with_count = args.len() == 3;
        let mut budget = ReplyBudget::new(limit);
        if let Err(reply) = budget.charge(
            value.heap.len() * (if with_count { 2 } else { 1 }),
            value.heap.iter().map(|entry| entry.item.len()).sum(),
        ) {
            return reply;
        }
        let mut heap: Vec<_> = value.heap.iter().collect();
        heap.sort_unstable_by(|a, b| b.count.cmp(&a.count).then_with(|| a.item.cmp(&b.item)));
        let mut replies = Vec::with_capacity(heap.len() * (if with_count { 2 } else { 1 }));
        for entry in heap {
            replies.push(Resp::Bulk(Some(entry.item.clone())));
            if with_count {
                replies.push(Resp::Integer(entry.count as i64));
            }
        }
        array(replies)
    }
    fn topk_info(&mut self, args: &[Bytes], now: i64) -> Resp {
        let value = match self.sketch(&args[1], TOPK_KIND, now) {
            Ok(Data::TopK(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        info(vec![
            ("k", Resp::Integer(value.k as i64)),
            ("width", Resp::Integer(value.width as i64)),
            ("depth", Resp::Integer(value.depth as i64)),
            ("decay", Resp::Double(value.decay)),
        ])
    }
    fn td_create(&mut self, args: &[Bytes], now: i64) -> Resp {
        let compression = if args.len() == 2 {
            100
        } else if args.len() == 4 && args[2].eq_ignore_ascii_case(b"COMPRESSION") {
            let Some(v) = positive(&args[3]) else {
                return error("TDIGEST: invalid compression");
            };
            v
        } else {
            return syntax_error();
        };
        if self.value(&args[1], now).is_some() {
            return error("TDIGEST: key already exists");
        }
        let value = match TDigestValue::new(compression) {
            Ok(v) => v,
            Err(_) => return error("TDIGEST: compression exceeds memory limit"),
        };
        let effect = match value.init_effect() {
            Ok(v) => v,
            Err(failure) => return codec_error(failure),
        };
        self.sketch_create(&args[1], Data::TDigest(value), effect, now)
    }
    fn td_reset(&mut self, args: &[Bytes], now: i64) -> Resp {
        let value = match self.sketch(&args[1], TD_KIND, now) {
            Ok(Data::TDigest(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        if value.count == 0 && value.compressions == 0 {
            return Resp::Simple("OK");
        }
        let effect = Encoder::new(1, 5).finish();
        match self.sketch_effects(&args[1], TD_KIND, vec![effect], now) {
            Ok(()) => Resp::Simple("OK"),
            Err(reply) => reply,
        }
    }
    fn td_add(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut values = Vec::with_capacity(args.len() - 2);
        for arg in &args[2..] {
            let Some(value) = finite(arg) else {
                return error("TDIGEST: observations must be finite numbers");
            };
            values.push(value);
        }
        let value = match self.sketch(&args[1], TD_KIND, now) {
            Ok(Data::TDigest(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let effect = match value.plan_add(&values) {
            Ok(v) => v,
            Err(reply) => return reply,
        };
        match self.sketch_effects(&args[1], TD_KIND, effect, now) {
            Ok(()) => Resp::Simple("OK"),
            Err(reply) => reply,
        }
    }
    fn td_merge(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(n) = positive(&args[2]).filter(|&n| n <= args.len().saturating_sub(3)) else {
            return error("TDIGEST: invalid numkeys");
        };
        let end = 3 + n;
        let mut index = end;
        let mut compression = None;
        let mut overwrite = false;
        while index < args.len() {
            if args[index].eq_ignore_ascii_case(b"OVERRIDE") && !overwrite {
                overwrite = true;
                index += 1;
            } else if args[index].eq_ignore_ascii_case(b"COMPRESSION")
                && compression.is_none()
                && index + 1 < args.len()
            {
                let Some(c) = positive(&args[index + 1]).filter(|&v| capacity(v).is_ok()) else {
                    return error("TDIGEST: invalid compression");
                };
                compression = Some(c);
                index += 2;
            } else {
                return syntax_error();
            }
        }
        let mut present = false;
        let mut previous_compression = None;
        match self.value(&args[1], now) {
            Some(Value::Typed(data)) => {
                if let Data::TDigest(value) = &**data {
                    present = true;
                    if !overwrite {
                        previous_compression = Some(value.compression);
                    }
                } else {
                    return wrong_type();
                }
            }
            Some(_) => return wrong_type(),
            None => (),
        }
        let mut source_max = 1usize;
        // Validate every source before choosing the resulting compression.
        for key in &args[3..end] {
            let value = match self.sketch(key, TD_KIND, now) {
                Ok(Data::TDigest(value)) => value,
                Ok(_) => unreachable!(),
                Err(reply) => return reply,
            };
            source_max = source_max.max(value.compression);
        }
        let compression = compression.or(previous_compression).unwrap_or(source_max);
        let mut result = match TDigestValue::new(compression) {
            Ok(v) => v,
            Err(failure) => return codec_error(failure),
        };
        let limit = capacity(compression).expect("validated compression");
        let keys = args[3..end].iter().chain(if present && !overwrite {
            Some(&args[1])
        } else {
            None
        });
        for key in keys {
            let value = match self.sketch(key, TD_KIND, now) {
                Ok(Data::TDigest(value)) => value,
                Ok(_) => unreachable!(),
                Err(reply) => return reply,
            };
            if value.count == 0 {
                continue;
            }
            let Some(count) = result
                .count
                .checked_add(value.count)
                .filter(|&v| v <= MAX_COUNT)
            else {
                return error("TDIGEST: observation count overflow");
            };
            result.min = result.min.min(value.min);
            result.max = result.max.max(value.max);
            for &centroid in value.merged.iter().chain(&value.pending) {
                if result.merged.len() >= limit {
                    result.merged = TDigestValue::compress_nodes(
                        std::mem::take(&mut result.merged),
                        compression,
                        result.count,
                    );
                    result.compressions += 1;
                }
                result.merged.push(centroid);
                result.count += centroid.weight;
            }
            if result.count != count {
                return self.fail_io(bad_effect());
            }
        }
        if result.count > 0 {
            result.merged = TDigestValue::compress_nodes(
                std::mem::take(&mut result.merged),
                compression,
                result.count,
            );
            result.compressions += 1;
        }
        let effect = match result.state_effects() {
            Ok(v) => v,
            Err(failure) => return codec_error(failure),
        };
        if !present {
            let init = match result.init_effect() {
                Ok(v) => v,
                Err(failure) => return codec_error(failure),
            };
            let empty = TDigestValue::new(compression).expect("validated compression");
            self.sketch_create(&args[1], Data::TDigest(empty), init, now);
        }
        match self.sketch_effects(&args[1], TD_KIND, effect, now) {
            Ok(()) => Resp::Simple("OK"),
            Err(reply) => reply,
        }
    }
    fn td_query(&mut self, args: &[Bytes], now: i64) -> Resp {
        let name = &args[0];
        let limit = self.reply_limit;
        let byrank = name.eq_ignore_ascii_case(b"TDIGEST.BYRANK")
            || name.eq_ignore_ascii_case(b"TDIGEST.BYREVRANK");
        let reverse = name.eq_ignore_ascii_case(b"TDIGEST.REVRANK")
            || name.eq_ignore_ascii_case(b"TDIGEST.BYREVRANK");
        let quantile = name.eq_ignore_ascii_case(b"TDIGEST.QUANTILE");
        let rank = name.eq_ignore_ascii_case(b"TDIGEST.RANK")
            || name.eq_ignore_ascii_case(b"TDIGEST.REVRANK");
        let trimmed = name.eq_ignore_ascii_case(b"TDIGEST.TRIMMED_MEAN");
        let mut ranks = Vec::new();
        let mut values = Vec::new();
        if byrank {
            for arg in &args[2..] {
                let Some(v) = integer(arg).filter(|&v| v >= 0) else {
                    return error("TDIGEST: rank must be a nonnegative integer");
                };
                ranks.push(v as u64);
            }
        } else {
            for arg in &args[2..] {
                let Some(v) = number(arg) else {
                    return error("TDIGEST: invalid numeric value");
                };
                if (quantile || trimmed) && !(0.0..=1.0).contains(&v) {
                    return error("TDIGEST: quantile must be between 0 and 1");
                }
                values.push(v);
            }
            if trimmed && values[0] >= values[1] {
                return error("TDIGEST: low quantile must be smaller than high quantile");
            }
        }
        let value = match self.sketch(&args[1], TD_KIND, now) {
            Ok(Data::TDigest(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        if name.eq_ignore_ascii_case(b"TDIGEST.MIN") {
            return Resp::Double(if value.count == 0 {
                f64::NAN
            } else {
                value.min
            });
        }
        if name.eq_ignore_ascii_case(b"TDIGEST.MAX") {
            return Resp::Double(if value.count == 0 {
                f64::NAN
            } else {
                value.max
            });
        }
        if trimmed {
            return Resp::Double(value.trimmed_mean(values[0], values[1]));
        }
        if byrank {
            return bounded_replies(
                ranks.into_iter().map(|r| {
                    Resp::Double(if value.count == 0 {
                        f64::NAN
                    } else if r >= value.count {
                        if reverse {
                            f64::NEG_INFINITY
                        } else {
                            f64::INFINITY
                        }
                    } else if r == 0 {
                        if reverse { value.max } else { value.min }
                    } else if r == value.count - 1 {
                        if reverse { value.min } else { value.max }
                    } else {
                        value.quantile(if reverse {
                            (value.count - r - 1) as f64 / value.count as f64
                        } else {
                            r as f64 / value.count as f64
                        })
                    })
                }),
                limit,
            );
        }
        bounded_replies(
            values.into_iter().map(|v| {
                if quantile {
                    return Resp::Double(value.quantile(v));
                }
                if !rank {
                    return Resp::Double(value.cdf(v));
                }
                let result = if value.count == 0 {
                    -2
                } else if v < value.min {
                    if reverse { value.count as i64 } else { -1 }
                } else if v > value.max {
                    if reverse { -1 } else { value.count as i64 }
                } else {
                    let position = value.cdf(v) * value.count as f64;
                    if reverse {
                        (value.count as f64 - position.round()).round() as i64
                    } else {
                        let base = position.floor();
                        (base + if position - base > 0.5 { 1.0 } else { 0.0 }) as i64
                    }
                };
                Resp::Integer(result)
            }),
            limit,
        )
    }
    fn td_info(&mut self, args: &[Bytes], now: i64) -> Resp {
        let value = match self.sketch(&args[1], TD_KIND, now) {
            Ok(Data::TDigest(value)) => value,
            Ok(_) => unreachable!(),
            Err(reply) => return reply,
        };
        let merged: u64 = value.merged.iter().map(|c| c.weight).sum();
        let pending: u64 = value.pending.iter().map(|c| c.weight).sum();
        let cached = value.view.get().map_or(0, Vec::capacity);
        let memory = std::mem::size_of::<TDigestValue>()
            + (value.merged.capacity() + value.pending.capacity() + cached)
                * std::mem::size_of::<Centroid>();
        info(vec![
            ("Compression", Resp::Integer(value.compression as i64)),
            (
                "Capacity",
                Resp::Integer(capacity(value.compression).expect("valid compression") as i64),
            ),
            ("Merged nodes", Resp::Integer(value.merged.len() as i64)),
            ("Unmerged nodes", Resp::Integer(value.pending.len() as i64)),
            ("Merged weight", Resp::Integer(merged as i64)),
            ("Unmerged weight", Resp::Integer(pending as i64)),
            ("Observations", Resp::Integer(value.count as i64)),
            (
                "Total compressions",
                Resp::Integer(value.compressions as i64),
            ),
            ("Memory usage", Resp::Integer(memory as i64)),
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    fn apply_cms(value: &mut CmsValue, pairs: &[(Bytes, i64)]) -> Vec<Resp> {
        let (effects, replies) = value.plan(pairs).unwrap();
        for effect in effects {
            value.apply(&effect).unwrap();
        }
        replies
    }
    fn apply_topk(value: &mut TopKValue, pairs: &[(Bytes, u64)]) -> Vec<Resp> {
        let (effects, replies) = value.plan(pairs).unwrap();
        for effect in effects {
            value.apply(&effect).unwrap();
        }
        replies
    }
    fn add_digest(value: &mut TDigestValue, samples: &[f64]) {
        for effect in value.plan_add(samples).unwrap() {
            value.apply(&effect).unwrap();
        }
    }
    fn observations(engine: &mut Engine, key: &'static str) -> u64 {
        match engine
            .sketch(&Bytes::from_static(key.as_bytes()), TD_KIND, 1000)
            .unwrap()
        {
            Data::TDigest(value) => value.count,
            _ => unreachable!(),
        }
    }
    #[test]
    fn cms_probability_dimensions_signed_updates_and_batch_atomicity() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["CMS.INITBYPROB", "c", ".001", ".002"]),
            Resp::Simple("OK")
        );
        let Data::Cms(value) = engine
            .sketch(&Bytes::from_static(b"c"), CMS_KIND, 1000)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!((value.width, value.depth), (2000, 9));
        assert_eq!(
            run(
                &mut engine,
                &["CMS.INCRBY", "c", "a", "10", "b", "4", "a", "-2"]
            ),
            array(vec![Resp::Integer(10), Resp::Integer(4), Resp::Integer(8)])
        );
        assert!(matches!(
            run(&mut engine, &["CMS.INCRBY", "c", "a", "1", "b", "-100"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "c", "a", "b"]),
            array(vec![Resp::Integer(8), Resp::Integer(4)])
        );
        assert!(matches!(
            run(
                &mut engine,
                &["CMS.INCRBY", "c", "a", "1", "b", "nonnumeric"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "c", "a"]),
            array(vec![Resp::Integer(8)])
        );
        assert_eq!(
            run(
                &mut engine,
                &["CMS.INITBYDIM", "small", "64", "4", "CELL_SIZE", "1"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["CMS.INCRBY", "small", "x", "255"]),
            array(vec![Resp::Integer(255)])
        );
        assert!(matches!(
            run(&mut engine, &["CMS.INCRBY", "small", "y", "1", "x", "1"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "small", "x"]),
            array(vec![Resp::Integer(255)])
        );
    }
    #[test]
    fn cms_real_collisions_error_quality_binary_and_snapshot_replay() {
        let mut value = CmsValue::new(2048, 7, 4).unwrap();
        let pairs: Vec<_> = (0..5000u32)
            .map(|i| {
                (
                    Bytes::copy_from_slice(&i.to_le_bytes()),
                    i64::from(i % 10 + 1),
                )
            })
            .collect();
        apply_cms(&mut value, &pairs);
        let total = value.count;
        let mut error_sum = 0u64;
        for (item, count) in &pairs {
            let estimate = value.query(item);
            assert!(estimate >= *count as u64);
            assert!(estimate - (*count as u64) < total / 128);
            error_sum += estimate - *count as u64;
        }
        assert!(error_sum / (pairs.len() as u64) < total / (value.width as u64));
        assert_eq!(value.cells.len(), 2048 * 7);
        let mut effects = value.snapshot();
        let mut replay = CmsValue::restore(&effects.next().unwrap().unwrap()).unwrap();
        for effect in effects {
            replay.apply(&effect.unwrap()).unwrap();
        }
        assert_eq!(value.cells, replay.cells);
        assert_eq!(value.count, replay.count);
        let mut corrupt = Encoder::new(1, 3);
        corrupt.u64(1).unwrap();
        corrupt.u32(2).unwrap();
        corrupt.u32(0).unwrap();
        corrupt.u64(1).unwrap();
        assert!(replay.apply(&corrupt.finish()).is_err());
        assert_eq!(value.cells, replay.cells);
        assert_eq!(value.count, replay.count);
    }
    #[test]
    fn cms_merge_signed_weights_destination_alias_and_failed_merge() {
        let mut engine = Engine::new();
        for key in ["a", "b", "out"] {
            assert_eq!(
                run(&mut engine, &["CMS.INITBYDIM", key, "128", "5"]),
                Resp::Simple("OK")
            );
        }
        run(&mut engine, &["CMS.INCRBY", "a", "x", "10", "y", "4"]);
        run(&mut engine, &["CMS.INCRBY", "b", "x", "3", "y", "1"]);
        assert_eq!(
            run(
                &mut engine,
                &["CMS.MERGE", "out", "2", "a", "b", "WEIGHTS", "2", "-1"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "out", "x", "y"]),
            array(vec![Resp::Integer(17), Resp::Integer(7)])
        );
        assert!(matches!(
            run(
                &mut engine,
                &["CMS.MERGE", "out", "1", "a", "WEIGHTS", "-1"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "out", "x"]),
            array(vec![Resp::Integer(17)])
        );
        assert_eq!(
            run(&mut engine, &["CMS.MERGE", "out", "2", "out", "b"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["CMS.QUERY", "out", "x"]),
            array(vec![Resp::Integer(20)])
        );
        assert!(matches!(
            run(&mut engine, &["CMS.MERGE", "missing", "1", "a"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut engine,
                &["CMS.MERGE", "out", "2", "a", "b", "WEIGHTS", "1"]
            ),
            Resp::Error(_)
        ));
    }
    #[test]
    fn heavykeeper_finds_heavy_items_without_retaining_the_stream() {
        let mut value = TopKValue::new(3, 128, 7, 0.9, 12345).unwrap();
        let mut pairs = vec![
            (Bytes::from_static(b"first"), 10000),
            (Bytes::from_static(b"second"), 5000),
            (Bytes::from_static(b"third"), 2000),
        ];
        pairs.extend((0..5000u32).map(|i| (Bytes::copy_from_slice(&i.to_le_bytes()), 1)));
        apply_topk(&mut value, &pairs);
        assert_eq!(value.buckets.len(), 128 * 7);
        assert_eq!(value.heap.len(), 3);
        for item in [
            b"first".as_slice(),
            b"second".as_slice(),
            b"third".as_slice(),
        ] {
            assert!(value.heap.iter().any(|entry| &entry.item[..] == item));
        }
        assert!((9900..=10000).contains(&value.estimate(b"first")));
        assert!((4900..=5000).contains(&value.estimate(b"second")));
        for i in 1..value.heap.len() {
            assert!(value.heap[(i - 1) / 2].count <= value.heap[i].count);
        }
    }
    #[test]
    fn heavykeeper_collision_takeover_weighted_updates_and_deterministic_replay() {
        let mut value = TopKValue::new(1, 1, 1, 1.0, 99).unwrap();
        apply_topk(&mut value, &[(Bytes::from_static(b"a"), 1)]);
        let replies = apply_topk(&mut value, &[(Bytes::from_static(b"b"), 3)]);
        assert_eq!(replies, vec![Resp::Bulk(Some(Bytes::from_static(b"a")))]);
        assert_eq!(value.estimate(b"b"), 3);
        let mut effects = value.snapshot();
        let mut replay = TopKValue::restore(&effects.next().unwrap().unwrap()).unwrap();
        for effect in effects {
            replay.apply(&effect.unwrap()).unwrap();
        }
        let pairs: Vec<_> = (0..100u32)
            .map(|i| (Bytes::copy_from_slice(&i.to_le_bytes()), u64::from(i % 20)))
            .collect();
        let (left, replies) = value.plan(&pairs).unwrap();
        let (right, replayed_replies) = replay.plan(&pairs).unwrap();
        assert_eq!(left, right);
        assert_eq!(replies, replayed_replies);
        for effect in left {
            value.apply(&effect).unwrap();
            replay.apply(&effect).unwrap();
        }
        assert_eq!(value.rng, replay.rng);
        let before: Vec<_> = value.snapshot().map(Result::unwrap).collect();
        let (effect, _) = value.plan(&[(Bytes::from_static(b"after"), 100)]).unwrap();
        assert!(value.apply(&effect[0][..effect[0].len() - 1]).is_err());
        assert_eq!(
            before,
            value.snapshot().map(Result::unwrap).collect::<Vec<_>>()
        );
    }
    #[test]
    fn topk_commands_validation_membership_flat_counts_and_binary_members() {
        let mut engine = Engine::new();
        assert!(matches!(
            run(&mut engine, &["TOPK.RESERVE", "k", "2", "4"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["TOPK.RESERVE", "k", "2"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["TOPK.INCRBY", "k", "a", "10", "b", "5"]),
            array(vec![Resp::Bulk(None), Resp::Bulk(None)])
        );
        assert!(matches!(
            run(&mut engine, &["TOPK.INCRBY", "k", "a", "1", "b", "100001"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["TOPK.COUNT", "k", "a", "b"]),
            array(vec![Resp::Integer(10), Resp::Integer(5)])
        );
        assert_eq!(
            run(&mut engine, &["TOPK.QUERY", "k", "a", "absent"]),
            array(vec![Resp::Boolean(true), Resp::Boolean(false)])
        );
        assert_eq!(
            run(&mut engine, &["TOPK.LIST", "k", "WITHCOUNT"]),
            array(vec![
                bulk("a"),
                Resp::Integer(10),
                bulk("b"),
                Resp::Integer(5)
            ])
        );
        let mut value = TopKValue::new(2, 32, 4, 0.9, 44).unwrap();
        apply_topk(
            &mut value,
            &[(Bytes::from_static(b"\0\xff"), 100), (Bytes::new(), 10)],
        );
        assert_eq!(value.estimate(b"\0\xff"), 100);
        assert_eq!(value.estimate(b""), 10);
    }
    #[test]
    fn digest_singleton_mass_is_discrete_across_quantiles_cdf_and_both_rank_directions() {
        let mut engine = Engine::new();
        run(&mut engine, &["TDIGEST.CREATE", "t"]);
        run(&mut engine, &["TDIGEST.ADD", "t", "0", "100", "200"]);
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYRANK", "t", "0", "1", "2", "3"]),
            array(vec![
                Resp::Double(0.0),
                Resp::Double(100.0),
                Resp::Double(200.0),
                Resp::Double(f64::INFINITY)
            ])
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYREVRANK", "t", "0", "1", "2", "3"]),
            array(vec![
                Resp::Double(200.0),
                Resp::Double(100.0),
                Resp::Double(0.0),
                Resp::Double(f64::NEG_INFINITY)
            ])
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "TDIGEST.QUANTILE",
                    "t",
                    "0",
                    ".25",
                    ".3333333333333333",
                    ".5",
                    ".6666666666666666",
                    ".75",
                    "1"
                ]
            ),
            array(
                [0.0, 0.0, 100.0, 100.0, 200.0, 200.0, 200.0]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "TDIGEST.CDF",
                    "t",
                    "-inf",
                    "0",
                    "25",
                    "100",
                    "150",
                    "200",
                    "inf"
                ]
            ),
            array(
                [0.0, 1.0 / 6.0, 1.0 / 3.0, 0.5, 2.0 / 3.0, 5.0 / 6.0, 1.0]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "TDIGEST.RANK",
                    "t",
                    "-inf",
                    "0",
                    "25",
                    "100",
                    "150",
                    "200",
                    "inf"
                ]
            ),
            array(
                [-1, 0, 1, 1, 2, 2, 3]
                    .into_iter()
                    .map(Resp::Integer)
                    .collect()
            )
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "TDIGEST.REVRANK",
                    "t",
                    "-inf",
                    "0",
                    "25",
                    "100",
                    "150",
                    "200",
                    "inf"
                ]
            ),
            array(
                [3, 2, 2, 1, 1, 0, -1]
                    .into_iter()
                    .map(Resp::Integer)
                    .collect()
            )
        );
        // Exercise singleton/non-singleton interpolation in both directions,
        // including exact means and the boundary of a singleton's unit mass.
        run(&mut engine, &["TDIGEST.ADD", "t", "100"]);
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.QUANTILE", "t", ".25", ".375", ".5", ".625", ".75"]
            ),
            array(
                [0.0, 50.0, 100.0, 150.0, 200.0]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.CDF", "t", "0", "25", "100", "175", "200"]
            ),
            array(
                [0.125, 0.3125, 0.5, 0.6875, 0.875]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.RANK", "t", "0", "25", "100", "175", "200"]
            ),
            array([0, 1, 2, 3, 3].into_iter().map(Resp::Integer).collect())
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.REVRANK", "t", "0", "25", "100", "175", "200"]
            ),
            array([3, 3, 2, 1, 0].into_iter().map(Resp::Integer).collect())
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYRANK", "t", "0", "1", "2", "3"]),
            array(
                [0.0, 0.0, 100.0, 200.0]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYREVRANK", "t", "0", "1", "2", "3"]),
            array(
                [200.0, 100.0, 0.0, 0.0]
                    .into_iter()
                    .map(Resp::Double)
                    .collect()
            )
        );
        run(&mut engine, &["TDIGEST.RESET", "t"]);
        run(&mut engine, &["TDIGEST.ADD", "t", "7", "7", "7"]);
        assert_eq!(
            run(&mut engine, &["TDIGEST.CDF", "t", "6", "7", "8"]),
            array([0.0, 0.5, 1.0].into_iter().map(Resp::Double).collect())
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.QUANTILE", "t", "0", ".5", "1"]),
            array([7.0, 7.0, 7.0].into_iter().map(Resp::Double).collect())
        );
    }
    #[test]
    fn digest_readonly_queries_preserve_watch_journal_and_future_recovery_results() {
        fn state(engine: &mut Engine) -> Vec<Bytes> {
            engine
                .sketch(&Bytes::from_static(b"t"), TD_KIND, 1000)
                .unwrap()
                .snapshot()
                .map(Result::unwrap)
                .collect()
        }
        fn add_range(engine: &mut Engine, start: usize, end: usize) -> Resp {
            let mut args = vec![Bytes::from_static(b"TDIGEST.ADD"), Bytes::from_static(b"t")];
            args.extend((start..end).map(|n| Bytes::from(n.to_string())));
            engine.execute_at(&args, 1000)
        }
        let queries: &[&[&str]] = &[
            &["TDIGEST.MIN", "t"],
            &["TDIGEST.MAX", "t"],
            &[
                "TDIGEST.QUANTILE",
                "t",
                "0",
                ".001",
                ".25",
                ".5",
                ".99",
                "1",
            ],
            &["TDIGEST.CDF", "t", "-inf", "0", "25", "2500", "4999", "inf"],
            &[
                "TDIGEST.RANK",
                "t",
                "-inf",
                "0",
                "25",
                "2500",
                "4999",
                "inf",
            ],
            &[
                "TDIGEST.REVRANK",
                "t",
                "-inf",
                "0",
                "25",
                "2500",
                "4999",
                "inf",
            ],
            &["TDIGEST.BYRANK", "t", "0", "1", "100", "4999", "5000"],
            &["TDIGEST.BYREVRANK", "t", "0", "1", "100", "4999", "5000"],
            &["TDIGEST.TRIMMED_MEAN", "t", ".1", ".9"],
        ];
        let mut source = Engine::new();
        source.set_recording(true);
        let mut wal = Engine::new();
        run(&mut source, &["TDIGEST.CREATE", "t", "COMPRESSION", "2000"]);
        wal.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(add_range(&mut source, 0, 5000), Resp::Simple("OK"));
        wal.replay(source.take_batch().unwrap()).unwrap();
        let before = state(&mut source);
        let mut snapshot = Engine::new();
        for batch in source.snapshot_batches(1000) {
            snapshot.replay(batch.unwrap()).unwrap();
        }
        let watches = std::sync::Arc::new(crate::watch::Registry::default());
        source.set_watches(watches.clone());
        let token = watches.register(0, Bytes::from_static(b"t"), None);
        let expected: Vec<_> = queries.iter().map(|args| run(&mut source, args)).collect();
        for _ in 0..8 {
            for (args, expected) in queries.iter().zip(&expected) {
                assert_eq!(run(&mut source, args), *expected);
            }
        }
        assert!(!token.changed(1000));
        assert!(source.take_batch().is_none());
        assert_eq!(state(&mut source), before);
        assert_eq!(state(&mut wal), before);
        assert_eq!(state(&mut snapshot), before);
        // Force a canonical compression after only the live digest was queried.
        // Re-execution from a snapshot and resolved WAL replay must still agree.
        assert_eq!(add_range(&mut source, 5000, 13000), Resp::Simple("OK"));
        assert!(token.changed(1000));
        wal.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(add_range(&mut snapshot, 5000, 13000), Resp::Simple("OK"));
        assert_eq!(state(&mut source), state(&mut wal));
        assert_eq!(state(&mut source), state(&mut snapshot));
        for args in queries {
            let expected = run(&mut source, args);
            assert_eq!(run(&mut wal, args), expected);
            assert_eq!(run(&mut snapshot, args), expected);
        }
        assert_eq!(add_range(&mut source, 13000, 13010), Resp::Simple("OK"));
        wal.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(add_range(&mut snapshot, 13000, 13010), Resp::Simple("OK"));
        for args in queries {
            let expected = run(&mut source, args);
            assert_eq!(run(&mut wal, args), expected);
            assert_eq!(run(&mut snapshot, args), expected);
        }
        // A wide merge is journaled as metadata plus multiple centroid rows.
        // It must replace a warm reader's old result just like a small merge.
        run(
            &mut source,
            &["TDIGEST.CREATE", "wide", "COMPRESSION", "10000"],
        );
        let _ = source.take_batch();
        let mut args = vec![
            Bytes::from_static(b"TDIGEST.ADD"),
            Bytes::from_static(b"wide"),
        ];
        args.extend((0..5000).map(|n| Bytes::from(n.to_string())));
        assert_eq!(source.execute_at(&args, 1000), Resp::Simple("OK"));
        let _ = source.take_batch();
        assert_eq!(
            run(
                &mut source,
                &[
                    "TDIGEST.MERGE",
                    "t",
                    "1",
                    "wide",
                    "COMPRESSION",
                    "10000",
                    "OVERRIDE"
                ]
            ),
            Resp::Simple("OK")
        );
        wal.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(state(&mut source), state(&mut wal));
        for engine in [&mut source, &mut wal] {
            assert_eq!(
                run(engine, &["TDIGEST.BYRANK", "t", "2500"]),
                array(vec![Resp::Double(2500.0)])
            );
            assert_eq!(
                run(engine, &["TDIGEST.CDF", "t", "2500"]),
                array(vec![Resp::Double(2500.5 / 5000.0)])
            );
        }
        // Replacing an already queried digest through MERGE and then RESET
        // must not retain the preceding revision's derived results.
        run(&mut source, &["TDIGEST.CREATE", "other"]);
        let _ = source.take_batch();
        run(&mut source, &["TDIGEST.ADD", "other", "-200", "-100", "0"]);
        let _ = source.take_batch();
        assert_eq!(
            run(
                &mut source,
                &["TDIGEST.MERGE", "t", "1", "other", "OVERRIDE"]
            ),
            Resp::Simple("OK")
        );
        let _ = source.take_batch();
        assert_eq!(
            run(&mut source, &["TDIGEST.BYRANK", "t", "1"]),
            array(vec![Resp::Double(-100.0)])
        );
        assert_eq!(
            run(&mut source, &["TDIGEST.CDF", "t", "-175"]),
            array(vec![Resp::Double(1.0 / 3.0)])
        );
        run(&mut source, &["TDIGEST.RESET", "t"]);
        let _ = source.take_batch();
        assert!(
            matches!(run(&mut source,&["TDIGEST.TRIMMED_MEAN","t","0","1"]),Resp::Double(v) if v.is_nan())
        );
        run(&mut source, &["TDIGEST.ADD", "t", "40", "50", "60"]);
        let _ = source.take_batch();
        assert_eq!(
            run(&mut source, &["TDIGEST.BYRANK", "t", "1"]),
            array(vec![Resp::Double(50.0)])
        );
    }
    #[test]
    fn digest_quality_bounds_centroid_memory_and_exact_extremes() {
        let mut digest = TDigestValue::new(100).unwrap();
        let samples: Vec<_> = (0..100000u32).rev().map(f64::from).collect();
        add_digest(&mut digest, &samples);
        assert_eq!(digest.count, 100000);
        assert!(digest.nodes() <= capacity(100).unwrap());
        assert!(digest.nodes() < 1000);
        assert!(digest.compressions > 0);
        assert_eq!(digest.quantile(0.0), 0.0);
        assert_eq!(digest.quantile(1.0), 99999.0);
        for q in [0.001, 0.01, 0.1, 0.5, 0.9, 0.99, 0.999] {
            assert!((digest.quantile(q) - q * 99999.0).abs() < 1500.0);
            assert!((digest.cdf(q * 99999.0) - q).abs() < 0.02);
        }
        assert!((digest.trimmed_mean(0.1, 0.9) - 49999.5).abs() < 1000.0);
        let mut effects = digest.snapshot();
        let mut replay = TDigestValue::restore(&effects.next().unwrap().unwrap()).unwrap();
        for effect in effects {
            replay.apply(&effect.unwrap()).unwrap();
        }
        assert_eq!(
            digest.snapshot().map(Result::unwrap).collect::<Vec<_>>(),
            replay.snapshot().map(Result::unwrap).collect::<Vec<_>>()
        );
    }
    #[test]
    fn digest_empty_duplicates_rank_rounding_query_limits_and_reset() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["TDIGEST.CREATE", "t"]),
            Resp::Simple("OK")
        );
        assert!(matches!(run(&mut engine,&["TDIGEST.MIN","t"]),Resp::Double(v) if v.is_nan()));
        assert_eq!(
            run(&mut engine, &["TDIGEST.RANK", "t", "1"]),
            array(vec![Resp::Integer(-2)])
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.ADD", "t", "10", "10", "10", "10", "20", "20"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.RANK", "t", "10", "20", "-inf", "inf"]
            ),
            array(vec![
                Resp::Integer(2),
                Resp::Integer(5),
                Resp::Integer(-1),
                Resp::Integer(6)
            ])
        );
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.REVRANK", "t", "10", "20", "-inf", "inf"]
            ),
            array(vec![
                Resp::Integer(4),
                Resp::Integer(1),
                Resp::Integer(6),
                Resp::Integer(-1)
            ])
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYRANK", "t", "0", "5", "6"]),
            array(vec![
                Resp::Double(10.0),
                Resp::Double(20.0),
                Resp::Double(f64::INFINITY)
            ])
        );
        assert_eq!(
            run(&mut engine, &["TDIGEST.BYREVRANK", "t", "0", "5", "6"]),
            array(vec![
                Resp::Double(20.0),
                Resp::Double(10.0),
                Resp::Double(f64::NEG_INFINITY)
            ])
        );
        assert!(matches!(
            run(&mut engine, &["TDIGEST.ADD", "t", "50", "inf"]),
            Resp::Error(_)
        ));
        assert_eq!(observations(&mut engine, "t"), 6);
        assert!(matches!(
            run(&mut engine, &["TDIGEST.QUANTILE", "t", "1.01"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(&mut engine, &["TDIGEST.BYRANK", "t", "1.5"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(&mut engine, &["TDIGEST.TRIMMED_MEAN", "t", ".7", ".2"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, &["TDIGEST.RESET", "t"]),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "t"), 0);
        assert!(matches!(run(&mut engine,&["TDIGEST.MAX","t"]),Resp::Double(v) if v.is_nan()));
    }
    #[test]
    fn digest_merge_compression_override_aliases_and_error_atomicity() {
        let mut engine = Engine::new();
        for (key, c) in [("a", "20"), ("b", "200"), ("out", "50")] {
            run(&mut engine, &["TDIGEST.CREATE", key, "COMPRESSION", c]);
        }
        run(&mut engine, &["TDIGEST.ADD", "a", "1", "2"]);
        run(&mut engine, &["TDIGEST.ADD", "b", "3", "4", "5"]);
        run(&mut engine, &["TDIGEST.ADD", "out", "100"]);
        assert_eq!(
            run(&mut engine, &["TDIGEST.MERGE", "out", "2", "a", "b"]),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "out"), 6);
        let Data::TDigest(out) = engine
            .sketch(&Bytes::from_static(b"out"), TD_KIND, 1000)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(out.compression, 50);
        assert!(matches!(
            run(
                &mut engine,
                &["TDIGEST.MERGE", "out", "2", "a", "missing", "OVERRIDE"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(observations(&mut engine, "out"), 6);
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.MERGE", "out", "1", "out", "OVERRIDE"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "out"), 6);
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.MERGE", "out", "2", "a", "b", "OVERRIDE"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "out"), 5);
        let Data::TDigest(out) = engine
            .sketch(&Bytes::from_static(b"out"), TD_KIND, 1000)
            .unwrap()
        else {
            unreachable!()
        };
        assert_eq!(out.compression, 200);
        assert_eq!(
            run(
                &mut engine,
                &["TDIGEST.MERGE", "new", "1", "a", "COMPRESSION", "30"]
            ),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "new"), 2);
        assert_eq!(
            run(&mut engine, &["TDIGEST.MERGE", "new", "1", "new"]),
            Resp::Simple("OK")
        );
        assert_eq!(observations(&mut engine, "new"), 4);
    }
    #[test]
    fn digest_extreme_finite_means_weight_overflow_and_malformed_effect_atomicity() {
        let mut digest = TDigestValue::new(1).unwrap();
        add_digest(&mut digest, &[-f64::MAX, f64::MAX, -f64::MAX, f64::MAX]);
        assert!(digest.quantile(0.5).is_finite());
        assert!(digest.trimmed_mean(0.0, 1.0).is_finite());
        assert_eq!(digest.min, -f64::MAX);
        assert_eq!(digest.max, f64::MAX);
        let full = TDigestValue {
            compression: 100,
            merged: vec![Centroid {
                mean: 7.0,
                weight: MAX_COUNT,
            }],
            pending: Vec::new(),
            count: MAX_COUNT,
            min: 7.0,
            max: 7.0,
            compressions: 1,
            view: std::sync::OnceLock::new(),
        };
        let before = full.snapshot().map(Result::unwrap).collect::<Vec<_>>();
        assert!(full.plan_add(&[1.0]).is_err());
        let mut replay = TDigestValue::restore(&before[0]).unwrap();
        for effect in &before[1..] {
            replay.apply(effect).unwrap();
        }
        let effect = digest.state_effect().unwrap();
        assert!(replay.apply(&effect[..effect.len() - 1]).is_err());
        assert_eq!(
            before,
            replay.snapshot().map(Result::unwrap).collect::<Vec<_>>()
        );
        assert!(TDigestValue::new(0).is_err());
        assert!(TDigestValue::new(MAX_COMPRESSION + 1).is_err());
    }
    #[test]
    fn sketches_keep_ttl_and_reject_wrong_types() {
        let mut engine = Engine::new();
        run(&mut engine, &["CMS.INITBYDIM", "c", "32", "4"]);
        run(&mut engine, &["PEXPIRE", "c", "10000"]);
        run(&mut engine, &["CMS.INCRBY", "c", "a", "10"]);
        assert_eq!(run(&mut engine, &["PTTL", "c"]), Resp::Integer(10000));
        run(&mut engine, &["TOPK.RESERVE", "k", "2"]);
        run(&mut engine, &["PEXPIRE", "k", "10000"]);
        run(&mut engine, &["TOPK.ADD", "k", "a"]);
        assert_eq!(run(&mut engine, &["PTTL", "k"]), Resp::Integer(10000));
        run(&mut engine, &["TDIGEST.CREATE", "t"]);
        run(&mut engine, &["PEXPIRE", "t", "10000"]);
        run(&mut engine, &["TDIGEST.ADD", "t", "1"]);
        run(&mut engine, &["TDIGEST.RESET", "t"]);
        assert_eq!(run(&mut engine, &["PTTL", "t"]), Resp::Integer(10000));
        run(&mut engine, &["SET", "string", "value"]);
        for command in ["CMS.INFO", "TOPK.INFO", "TDIGEST.INFO"] {
            assert!(
                matches!(run(&mut engine,&[command,"string"]),Resp::Error(message) if message.starts_with("WRONGTYPE"))
            );
        }
    }
}

#[cfg(test)]
mod response_budget_tests {
    use super::*;
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    fn state(engine: &mut Engine, key: &'static [u8], kind: u8) -> Vec<Bytes> {
        engine
            .sketch(&Bytes::from_static(key), kind, 1000)
            .unwrap()
            .snapshot()
            .map(Result::unwrap)
            .collect()
    }
    #[test]
    fn failed_reply_budget_does_not_apply_counters_rng_or_heap_expulsion() {
        let mut engine = Engine::new();
        run(&mut engine, &["CMS.INITBYDIM", "c", "16", "4"]);
        run(&mut engine, &["TOPK.RESERVE", "k", "1"]);
        run(&mut engine, &["TOPK.INCRBY", "k", "a", "5"]);
        let cms = state(&mut engine, b"c", CMS_KIND);
        let topk = state(&mut engine, b"k", TOPK_KIND);
        engine.reply_limit = 1;
        assert!(matches!(
            run(&mut engine, &["CMS.INCRBY", "c", "a", "1", "b", "2"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(&mut engine, &["TOPK.INCRBY", "k", "b", "100"]),
            Resp::Error(_)
        ));
        assert_eq!(cms, state(&mut engine, b"c", CMS_KIND));
        assert_eq!(topk, state(&mut engine, b"k", TOPK_KIND));
    }
}
