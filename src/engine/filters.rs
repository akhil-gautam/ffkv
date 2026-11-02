//! Scalable bit Bloom filters and byte-fingerprint Cuckoo filters.
//!
//! SCANDUMP uses ffkv's versioned, checksummed effect stream (FFKF), NOT the
//! RedisBloom packed native headers. Hashing and bit/bucket layout follow the
//! published MurmurHash64A algorithms, but RedisBloom binary interchange has
//! not been verified and is deliberately rejected rather than advertised.
//! Layout references: https://github.com/RedisBloom/RedisBloom/blob/master/deps/bloom/bloom.c
//! and https://github.com/RedisBloom/RedisBloom/blob/master/src/cuckoo.h .
//! This is an original implementation, not a copy of the module wrappers.
//! In particular RedisBloom's Bloom header is packed 20+61*nfilters bytes and
//! its Cuckoo header is 38 bytes (src/sb.c, src/cf.h). FFKF instead wraps each
//! resolved effect with magic, kind and Murmur64 checksum, uses ordinal page
//! cursors and validates contiguous 64KiB pages plus final occupancy. The two
//! wire formats are intentionally different; only ffkv-to-ffkv roundtrip is
//! covered by the regressions below. Freeze the source while exporting.
use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{CommandSpec, Engine, Entry, Resp, Value, error, wrong_type};
use bytes::Bytes;
use std::io;

const VERSION: u8 = 1;
const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_FILTERS: usize = 33;
const MAX_BLOOM_FILTERS: usize = 1024;
const MAX_CURSOR: u64 = 1 + (MAX_BYTES / CHUNK + MAX_BLOOM_FILTERS) as u64;
const CHUNK: usize = 64 * 1024;
const MAX_CAPACITY: u64 = 1 << 30;
fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid probabilistic filter encoding",
    )
}
fn limit() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "filter capacity or allocation limit exceeded",
    )
}
fn hash(data: &[u8], seed: u64) -> u64 {
    // MurmurHash64A, explicitly little endian and independent of Rust's random hasher.
    const M: u64 = 0xc6a4a7935bd1e995;
    let mut h = seed ^ (data.len() as u64).wrapping_mul(M);
    let (chunks, tail) = data.as_chunks::<8>();
    for chunk in chunks {
        let mut k = u64::from_le_bytes(*chunk);
        k = k.wrapping_mul(M);
        k ^= k >> 47;
        k = k.wrapping_mul(M);
        h ^= k;
        h = h.wrapping_mul(M);
    }
    for (i, byte) in tail.iter().enumerate() {
        h ^= (*byte as u64) << (8 * i);
    }
    if !tail.is_empty() {
        h = h.wrapping_mul(M);
    }
    h ^= h >> 47;
    h = h.wrapping_mul(M);
    h ^ (h >> 47)
}
fn usize64(n: u64) -> io::Result<usize> {
    usize::try_from(n).map_err(|_| invalid())
}
fn encode(build: impl FnOnce(&mut Encoder) -> io::Result<()>, op: u8) -> io::Result<Bytes> {
    let mut e = Encoder::new(VERSION, op);
    build(&mut e)?;
    Ok(e.finish())
}
#[derive(Clone, Copy)]
struct BloomMeta {
    capacity: u64,
    rate: f64,
    hashes: u32,
    bytes: usize,
}
fn bloom_meta(capacity: u64, rate: f64) -> io::Result<BloomMeta> {
    if capacity == 0
        || capacity > MAX_CAPACITY
        || !rate.is_finite()
        || !(0.0..1.0).contains(&rate)
        || rate == 0.0
    {
        return Err(invalid());
    }
    let bpe = -rate.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2);
    let bits = ((capacity as f64 * bpe).floor()).max(1.0);
    let bytes = ((bits / 64.0).ceil() * 8.0) as usize;
    let hashes = (std::f64::consts::LN_2 * bpe).ceil() as u32;
    if bytes == 0 || bytes > MAX_BYTES || hashes == 0 || hashes > 2048 {
        return Err(limit());
    }
    Ok(BloomMeta {
        capacity,
        rate,
        hashes,
        bytes,
    })
}
fn put_bmeta(e: &mut Encoder, m: BloomMeta) -> io::Result<()> {
    e.u64(m.capacity)?;
    e.f64(m.rate)
}
fn get_bmeta(d: &mut Decoder<'_>) -> io::Result<BloomMeta> {
    bloom_meta(d.u64()?, d.f64()?)
}
struct BloomPart {
    meta: BloomMeta,
    count: u64,
    bits: Vec<u8>,
}
pub(super) struct BloomValue {
    expansion: u32,
    parts: Vec<BloomPart>,
    loading: u64,
}
impl BloomValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{allocation, sampled};
        allocation::<BloomPart>(self.parts.capacity()).saturating_add(sampled(
            self.parts.iter(),
            samples,
            |part| part.bits.capacity(),
        ))
    }

    fn new(capacity: u64, rate: f64, expansion: u32) -> io::Result<Self> {
        if expansion > 32768 {
            return Err(invalid());
        }
        let meta = bloom_meta(capacity, if expansion == 0 { rate } else { rate * 0.5 })?;
        Ok(Self {
            expansion,
            parts: vec![BloomPart {
                meta,
                count: 0,
                bits: vec![0; meta.bytes],
            }],
            loading: 0,
        })
    }
    fn contains(&self, item: &[u8]) -> bool {
        let a = hash(item, 0xc6a4a7935bd1e995);
        let b = hash(item, a);
        self.parts.iter().rev().any(|p| {
            (0..p.meta.hashes).all(|i| {
                let bit = a.wrapping_add((i as u64).wrapping_mul(b)) % (p.bits.len() as u64 * 8);
                p.bits[bit as usize / 8] & (1 << (bit % 8)) != 0
            })
        })
    }
    fn count(&self) -> u64 {
        self.parts.iter().map(|p| p.count).sum()
    }
    fn memory(&self) -> usize {
        self.parts.iter().map(|p| p.bits.len()).sum()
    }
    fn initial(&self) -> io::Result<Bytes> {
        encode(
            |e| {
                e.u32(self.expansion)?;
                e.u64(self.loading)?;
                e.u32(self.parts.len() as u32)?;
                for p in &self.parts {
                    put_bmeta(e, p.meta)?;
                    e.u64(p.count)?;
                }
                Ok(())
            },
            0,
        )
    }
    fn insert(&mut self, item: &[u8]) -> io::Result<Option<Bytes>> {
        if self.loading != 0 {
            return Err(invalid());
        }
        if self.contains(item) {
            return Ok(None);
        }
        let mut append = None;
        let last = self.parts.last().unwrap();
        if last.count == last.meta.capacity {
            if self.expansion == 0 || self.parts.len() >= MAX_BLOOM_FILTERS {
                return Err(limit());
            }
            let m = bloom_meta(
                last.meta
                    .capacity
                    .checked_mul(self.expansion as u64)
                    .ok_or_else(limit)?,
                last.meta.rate * 0.5,
            )?;
            if self
                .memory()
                .checked_add(m.bytes)
                .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(limit());
            }
            append = Some(m);
        }
        let index = self.parts.len() - usize::from(append.is_none());
        let meta = append.unwrap_or_else(|| self.parts[index].meta);
        let a = hash(item, 0xc6a4a7935bd1e995);
        let b = hash(item, a);
        let mut changes = Vec::with_capacity(meta.hashes as usize);
        for i in 0..meta.hashes {
            let bit = a.wrapping_add((i as u64).wrapping_mul(b)) % (meta.bytes as u64 * 8);
            changes.push((bit as usize / 8, 1u8 << (bit % 8)));
        }
        changes.sort_unstable_by_key(|c| c.0);
        let mut unique = 0;
        for i in 0..changes.len() {
            let (offset, mask) = changes[i];
            if unique > 0 && changes[unique - 1].0 == offset {
                changes[unique - 1].1 |= mask;
            } else {
                changes[unique] = (offset, mask);
                unique += 1;
            }
        }
        changes.truncate(unique);
        let mut cells = changes;
        if append.is_none() {
            for (offset, byte) in &mut cells {
                *byte |= self.parts[index].bits[*offset];
            }
            cells.retain(|(offset, byte)| *byte != self.parts[index].bits[*offset]);
        }
        let count = if append.is_some() {
            1
        } else {
            self.parts[index].count + 1
        };
        let effect = encode(
            |e| {
                e.u32(index as u32)?;
                e.u8(u8::from(append.is_some()))?;
                if let Some(m) = append {
                    put_bmeta(e, m)?;
                }
                e.u64(count)?;
                e.u32(cells.len() as u32)?;
                for (offset, byte) in &cells {
                    e.u64(*offset as u64)?;
                    e.u8(*byte)?;
                }
                Ok(())
            },
            1,
        )?;
        self.apply(&effect)?;
        Ok(Some(effect))
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, VERSION)?;
        if op == 4 {
            let (previous, next, done, inner) = read_load(&mut d)?;
            d.finish()?;
            if previous != 0 || next != 1 || done || inner.get(1) != Some(&0) {
                return Err(invalid());
            }
            let mut value = Self::restore(inner)?;
            if value.loading != 0 {
                return Err(invalid());
            }
            value.loading = next;
            return Ok(value);
        }
        if op != 0 {
            return Err(invalid());
        }
        let expansion = d.u32()?;
        let loading = d.u64()?;
        let n = d.u32()? as usize;
        if loading > MAX_CURSOR {
            return Err(invalid());
        }
        if expansion > 32768 || n == 0 || n > MAX_BLOOM_FILTERS || (expansion == 0 && n != 1) {
            return Err(invalid());
        }
        let mut metas = Vec::with_capacity(n);
        let mut total = 0usize;
        for _ in 0..n {
            let m = get_bmeta(&mut d)?;
            let count = d.u64()?;
            if count > m.capacity {
                return Err(invalid());
            }
            if let Some((previous, _)) = metas.last() {
                let previous: &BloomMeta = previous;
                if previous.capacity.checked_mul(expansion as u64) != Some(m.capacity)
                    || previous.rate * 0.5 != m.rate
                {
                    return Err(invalid());
                }
            }
            total = total
                .checked_add(m.bytes)
                .filter(|n| *n <= MAX_BYTES)
                .ok_or_else(limit)?;
            metas.push((m, count));
        }
        d.finish()?;
        let parts = metas
            .into_iter()
            .map(|(meta, count)| BloomPart {
                meta,
                count,
                bits: vec![0; meta.bytes],
            })
            .collect();
        Ok(Self {
            expansion,
            parts,
            loading,
        })
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, VERSION)?;
        if op == 4 {
            let (previous, next, done, inner) = read_load(&mut d)?;
            d.finish()?;
            if previous != self.loading || previous == 0 {
                return Err(invalid());
            }
            let (index, offset, bytes) =
                validate_load_span(self.snapshot(), inner, previous, done)?;
            if done {
                for (i, p) in self.parts.iter().enumerate() {
                    let mut ones: u64 = p.bits.iter().map(|b| b.count_ones() as u64).sum();
                    if i == index {
                        ones -= p.bits[offset..offset + bytes.len()]
                            .iter()
                            .map(|b| b.count_ones() as u64)
                            .sum::<u64>();
                        ones += bytes.iter().map(|b| b.count_ones() as u64).sum::<u64>();
                    }
                    if ones < p.count {
                        return Err(invalid());
                    }
                }
            }
            self.apply(inner)?;
            self.loading = if done { 0 } else { next };
            return Ok(());
        }
        if op == 2 {
            let (index, offset, bytes) = read_span(&mut d)?;
            d.finish()?;
            let p = self.parts.get_mut(index).ok_or_else(invalid)?;
            let end = offset
                .checked_add(bytes.len())
                .filter(|end| *end <= p.bits.len())
                .ok_or_else(invalid)?;
            p.bits[offset..end].copy_from_slice(bytes);
            return Ok(());
        }
        if op != 1 {
            return Err(invalid());
        }
        let index = d.u32()? as usize;
        let added = d.u8()?;
        let append = match added {
            0 => None,
            1 => Some(get_bmeta(&mut d)?),
            _ => return Err(invalid()),
        };
        let count = d.u64()?;
        let n = d.u32()? as usize;
        if n > 2048 {
            return Err(invalid());
        }
        let (meta, old_count) = if let Some(m) = append {
            let last = self.parts.last().ok_or_else(invalid)?;
            if index != self.parts.len()
                || index >= MAX_BLOOM_FILTERS
                || self.expansion == 0
                || last.count != last.meta.capacity
                || last.meta.capacity.checked_mul(self.expansion as u64) != Some(m.capacity)
                || last.meta.rate * 0.5 != m.rate
                || self
                    .memory()
                    .checked_add(m.bytes)
                    .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(invalid());
            }
            (m, 0)
        } else {
            let p = self.parts.get(index).ok_or_else(invalid)?;
            (p.meta, p.count)
        };
        if count != old_count + 1 || count > meta.capacity {
            return Err(invalid());
        }
        let mut cells = Vec::with_capacity(n);
        let mut changed = false;
        for _ in 0..n {
            let offset = usize64(d.u64()?)?;
            let byte = d.u8()?;
            if offset >= meta.bytes
                || cells.last().is_some_and(|(old, _)| *old >= offset)
                || (append.is_none()
                    && self.parts[index].bits[offset] & byte != self.parts[index].bits[offset])
            {
                return Err(invalid());
            }
            let old = if append.is_some() {
                0
            } else {
                self.parts[index].bits[offset]
            };
            changed |= byte != old;
            cells.push((offset, byte));
        }
        d.finish()?;
        if !changed {
            return Err(invalid());
        }
        if append.is_some() {
            self.parts.push(BloomPart {
                meta,
                count: 0,
                bits: vec![0; meta.bytes],
            });
        }
        self.parts[index].count = count;
        for (offset, byte) in cells {
            self.parts[index].bits[offset] = byte;
        }
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            value: SnapshotValue::Bloom(self),
            initial: true,
            part: 0,
            offset: 0,
        }
    }
}

struct CuckooPart {
    buckets: usize,
    slots: Vec<u8>,
}
pub(super) struct CuckooValue {
    bucket_size: u16,
    iterations: u16,
    expansion: u32,
    count: u64,
    deletes: u64,
    parts: Vec<CuckooPart>,
    loading: u64,
}
fn buckets(capacity: u64, bucket_size: u16) -> io::Result<usize> {
    if capacity > MAX_CAPACITY
        || capacity < 2 * bucket_size as u64
        || bucket_size == 0
        || bucket_size > 255
    {
        return Err(invalid());
    }
    let n = usize64(capacity / bucket_size as u64)?
        .checked_next_power_of_two()
        .ok_or_else(limit)?;
    if n.checked_mul(bucket_size as usize)
        .is_none_or(|n| n > MAX_BYTES)
    {
        return Err(limit());
    }
    Ok(n)
}
fn alternate(bucket: usize, fingerprint: u8, mask: usize) -> usize {
    (bucket ^ (fingerprint as usize).wrapping_mul(0x5bd1e995)) & mask
}
impl CuckooValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{allocation, sampled};
        allocation::<CuckooPart>(self.parts.capacity()).saturating_add(sampled(
            self.parts.iter(),
            samples,
            |part| part.slots.capacity(),
        ))
    }

    fn new(capacity: u64, bucket_size: u16, iterations: u16, expansion: u32) -> io::Result<Self> {
        if iterations == 0 || expansion > 32768 {
            return Err(invalid());
        }
        let expansion = if expansion == 0 {
            0
        } else {
            expansion.next_power_of_two()
        };
        let n = buckets(capacity, bucket_size)?;
        Ok(Self {
            bucket_size,
            iterations,
            expansion,
            count: 0,
            deletes: 0,
            parts: vec![CuckooPart {
                buckets: n,
                slots: vec![0; n * bucket_size as usize],
            }],
            loading: 0,
        })
    }
    fn memory(&self) -> usize {
        self.parts.iter().map(|p| p.slots.len()).sum()
    }
    fn fingerprint(item: &[u8]) -> (u64, u8) {
        let h = hash(item, 0);
        (h, (h % 255 + 1) as u8)
    }
    fn matches(&self, item: &[u8]) -> u64 {
        let (h, fp) = Self::fingerprint(item);
        let b = self.bucket_size as usize;
        self.parts
            .iter()
            .map(|p| {
                let first = h as usize & (p.buckets - 1);
                let second = alternate(first, fp, p.buckets - 1);
                let count = p.slots[first * b..(first + 1) * b]
                    .iter()
                    .filter(|v| **v == fp)
                    .count();
                (count
                    + if second == first {
                        0
                    } else {
                        p.slots[second * b..(second + 1) * b]
                            .iter()
                            .filter(|v| **v == fp)
                            .count()
                    }) as u64
            })
            .sum()
    }
    fn initial(&self) -> io::Result<Bytes> {
        encode(
            |e| {
                e.u32(self.bucket_size as u32)?;
                e.u32(self.iterations as u32)?;
                e.u32(self.expansion)?;
                e.u64(self.count)?;
                e.u64(self.deletes)?;
                e.u64(self.loading)?;
                e.u32(self.parts.len() as u32)?;
                for p in &self.parts {
                    e.u64(p.buckets as u64)?;
                }
                Ok(())
            },
            0,
        )
    }
    // Record every resolved swap. Failure reverses swaps in reverse order before
    // returning; neither growth failure nor a full fixed filter loses old members.
    fn place(
        part: &mut CuckooPart,
        b: usize,
        iterations: u16,
        bucket: usize,
        mut fp: u8,
        seed: u64,
    ) -> Option<Vec<(usize, u8)>> {
        let mut bucket = bucket & (part.buckets - 1);
        let second = alternate(bucket, fp, part.buckets - 1);
        for candidate in [bucket, second] {
            if let Some(slot) = (candidate * b..(candidate + 1) * b).find(|i| part.slots[*i] == 0) {
                part.slots[slot] = fp;
                return Some(vec![(slot, fp)]);
            }
        }
        let mut random = seed | 1;
        let mut undo = Vec::with_capacity(iterations as usize);
        let mut changes = Vec::with_capacity(iterations as usize + 1);
        for _ in 0..iterations {
            random ^= random << 13;
            random ^= random >> 7;
            random ^= random << 17;
            let slot = bucket * b + random as usize % b;
            let old = part.slots[slot];
            undo.push((slot, old));
            changes.push((slot, fp));
            part.slots[slot] = fp;
            fp = old;
            bucket = alternate(bucket, fp, part.buckets - 1);
            if let Some(slot) = (bucket * b..(bucket + 1) * b).find(|i| part.slots[*i] == 0) {
                part.slots[slot] = fp;
                changes.push((slot, fp));
                return Some(changes);
            }
        }
        for (slot, old) in undo.into_iter().rev() {
            part.slots[slot] = old;
        }
        None
    }
    fn patch(
        index: usize,
        append: Option<usize>,
        count: u64,
        deletes: u64,
        changes: &[(usize, u8)],
    ) -> io::Result<Bytes> {
        encode(
            |e| {
                e.u32(index as u32)?;
                e.u64(append.unwrap_or(0) as u64)?;
                e.u64(count)?;
                e.u64(deletes)?;
                e.u32(changes.len() as u32)?;
                for (slot, fp) in changes {
                    e.u64(*slot as u64)?;
                    e.u8(*fp)?;
                }
                Ok(())
            },
            1,
        )
    }
    fn insert(&mut self, item: &[u8], nx: bool) -> io::Result<Option<Bytes>> {
        if self.loading != 0 {
            return Err(invalid());
        }
        let (h, fp) = Self::fingerprint(item);
        if nx && self.matches(item) != 0 {
            return Ok(None);
        }
        for (index, part) in self.parts.iter_mut().enumerate().rev() {
            if let Some(changes) = Self::place(
                part,
                self.bucket_size as usize,
                self.iterations,
                h as usize,
                fp,
                h,
            ) {
                self.count += 1;
                return Ok(Some(Self::patch(
                    index,
                    None,
                    self.count,
                    self.deletes,
                    &changes,
                )?));
            }
        }
        if self.expansion == 0 || self.parts.len() >= MAX_FILTERS {
            return Err(limit());
        }
        let index = self.parts.len() - 1;
        let n = self.parts[index]
            .buckets
            .checked_mul(self.expansion as usize)
            .ok_or_else(limit)?;
        let bytes = n.checked_mul(self.bucket_size as usize).ok_or_else(limit)?;
        if self
            .memory()
            .checked_add(bytes)
            .is_none_or(|n| n > MAX_BYTES)
        {
            return Err(limit());
        }
        let mut part = CuckooPart {
            buckets: n,
            slots: vec![0; bytes],
        };
        let changes = Self::place(
            &mut part,
            self.bucket_size as usize,
            self.iterations,
            h as usize,
            fp,
            h,
        )
        .ok_or_else(limit)?;
        let effect = Self::patch(index + 1, Some(n), self.count + 1, self.deletes, &changes)?;
        self.parts.push(part);
        self.count += 1;
        Ok(Some(effect))
    }
    fn delete(&mut self, item: &[u8]) -> io::Result<Option<Bytes>> {
        if self.loading != 0 {
            return Err(invalid());
        }
        let (h, fp) = Self::fingerprint(item);
        let b = self.bucket_size as usize;
        for (index, p) in self.parts.iter_mut().enumerate().rev() {
            let first = h as usize & (p.buckets - 1);
            let second = alternate(first, fp, p.buckets - 1);
            for bucket in [first, second] {
                if let Some(slot) = (bucket * b..(bucket + 1) * b).find(|i| p.slots[*i] == fp) {
                    let count = self.count.checked_sub(1).ok_or_else(invalid)?;
                    let deletes = self.deletes.checked_add(1).ok_or_else(limit)?;
                    let effect = Self::patch(index, None, count, deletes, &[(slot, 0)])?;
                    p.slots[slot] = 0;
                    self.count = count;
                    self.deletes = deletes;
                    return Ok(Some(effect));
                }
            }
        }
        Ok(None)
    }
    fn compact_slot(&mut self, source: usize, slot: usize) -> io::Result<Option<(Bytes, Bytes)>> {
        if self.loading != 0 {
            return Err(invalid());
        }
        let b = self.bucket_size as usize;
        let fp = self.parts[source].slots[slot];
        if fp == 0 {
            return Ok(None);
        }
        let bucket = slot / b;
        for target in 0..source {
            if let Some(changes) = Self::place(
                &mut self.parts[target],
                b,
                self.iterations,
                bucket,
                fp,
                hash(&slot.to_le_bytes(), fp as u64),
            ) {
                let to = Self::patch(target, None, self.count + 1, self.deletes, &changes)?;
                let from = Self::patch(source, None, self.count, self.deletes, &[(slot, 0)])?;
                self.parts[source].slots[slot] = 0;
                return Ok(Some((to, from)));
            }
        }
        Ok(None)
    }
    fn compact_trim(&mut self) -> io::Result<Option<Bytes>> {
        let old = self.parts.len();
        while self.parts.len() > 1 && self.parts.last().unwrap().slots.iter().all(|s| *s == 0) {
            self.parts.pop();
        }
        if self.parts.len() != old {
            Ok(Some(encode(|e| e.u32(self.parts.len() as u32), 3)?))
        } else {
            Ok(None)
        }
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, VERSION)?;
        if op == 4 {
            let (previous, next, done, inner) = read_load(&mut d)?;
            d.finish()?;
            if previous != 0 || next != 1 || done || inner.get(1) != Some(&0) {
                return Err(invalid());
            }
            let mut value = Self::restore(inner)?;
            if value.loading != 0 {
                return Err(invalid());
            }
            value.loading = next;
            return Ok(value);
        }
        if op != 0 {
            return Err(invalid());
        }
        let bucket_size = d.u32()?;
        let iterations = d.u32()?;
        let expansion = d.u32()?;
        let count = d.u64()?;
        let deletes = d.u64()?;
        let loading = d.u64()?;
        let n = d.u32()? as usize;
        if loading > MAX_CURSOR {
            return Err(invalid());
        }
        if !(1..=255).contains(&bucket_size)
            || !(1..=65535).contains(&iterations)
            || expansion > 32768
            || (expansion != 0 && !expansion.is_power_of_two())
            || n == 0
            || n > MAX_FILTERS
            || (expansion == 0 && n != 1)
        {
            return Err(invalid());
        }
        let mut sizes: Vec<usize> = Vec::with_capacity(n);
        let mut total = 0usize;
        for _ in 0..n {
            let size = usize64(d.u64()?)?;
            if size < 2
                || !size.is_power_of_two()
                || sizes
                    .last()
                    .is_some_and(|previous| previous.checked_mul(expansion as usize) != Some(size))
            {
                return Err(invalid());
            }
            total = size
                .checked_mul(bucket_size as usize)
                .and_then(|bytes| total.checked_add(bytes))
                .filter(|n| *n <= MAX_BYTES)
                .ok_or_else(limit)?;
            sizes.push(size);
        }
        if count > total as u64 {
            return Err(invalid());
        }
        d.finish()?;
        let parts = sizes
            .into_iter()
            .map(|n| CuckooPart {
                buckets: n,
                slots: vec![0; n * bucket_size as usize],
            })
            .collect();
        Ok(Self {
            bucket_size: bucket_size as u16,
            iterations: iterations as u16,
            expansion,
            count,
            deletes,
            parts,
            loading,
        })
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let (op, mut d) = Decoder::new(effect, VERSION)?;
        if op == 4 {
            let (previous, next, done, inner) = read_load(&mut d)?;
            d.finish()?;
            if previous == 0 || previous != self.loading {
                return Err(invalid());
            }
            let (index, offset, bytes) =
                validate_load_span(self.snapshot(), inner, previous, done)?;
            if done {
                let mut occupied = self
                    .parts
                    .iter()
                    .flat_map(|p| &p.slots)
                    .filter(|fp| **fp != 0)
                    .count();
                occupied -= self.parts[index].slots[offset..offset + bytes.len()]
                    .iter()
                    .filter(|fp| **fp != 0)
                    .count();
                occupied += bytes.iter().filter(|fp| **fp != 0).count();
                if occupied as u64 != self.count {
                    return Err(invalid());
                }
            }
            self.apply(inner)?;
            self.loading = if done { 0 } else { next };
            return Ok(());
        }
        if op == 2 {
            let (index, offset, bytes) = read_span(&mut d)?;
            d.finish()?;
            let p = self.parts.get_mut(index).ok_or_else(invalid)?;
            let end = offset
                .checked_add(bytes.len())
                .filter(|end| *end <= p.slots.len())
                .ok_or_else(invalid)?;
            p.slots[offset..end].copy_from_slice(bytes);
            return Ok(());
        }
        if op == 3 {
            let n = d.u32()? as usize;
            d.finish()?;
            if n == 0
                || n >= self.parts.len()
                || self.parts[n..]
                    .iter()
                    .any(|p| p.slots.iter().any(|s| *s != 0))
            {
                return Err(invalid());
            }
            self.parts.truncate(n);
            return Ok(());
        }
        if op != 1 {
            return Err(invalid());
        }
        let index = d.u32()? as usize;
        let append = usize64(d.u64()?)?;
        let count = d.u64()?;
        let deletes = d.u64()?;
        let n = d.u32()? as usize;
        if n > self.iterations as usize + 1
            || deletes < self.deletes
            || deletes > self.deletes.saturating_add(1)
            || count.abs_diff(self.count) > 1
        {
            return Err(invalid());
        }
        let length = if append != 0 {
            if index != self.parts.len()
                || index >= MAX_FILTERS
                || self.expansion == 0
                || self
                    .parts
                    .last()
                    .unwrap()
                    .buckets
                    .checked_mul(self.expansion as usize)
                    != Some(append)
            {
                return Err(invalid());
            }
            let length = append
                .checked_mul(self.bucket_size as usize)
                .ok_or_else(limit)?;
            if self
                .memory()
                .checked_add(length)
                .is_none_or(|n| n > MAX_BYTES)
            {
                return Err(limit());
            }
            length
        } else {
            self.parts.get(index).ok_or_else(invalid)?.slots.len()
        };
        if count > (self.memory() + if append != 0 { length } else { 0 }) as u64 {
            return Err(invalid());
        }
        let mut changes = std::collections::HashMap::with_capacity(n);
        let mut delta = 0i64;
        let mut fingerprints = [0i32; 256];
        for _ in 0..n {
            let slot = usize64(d.u64()?)?;
            let fp = d.u8()?;
            if slot >= length {
                return Err(invalid());
            }
            let old = changes.insert(slot, fp).unwrap_or_else(|| {
                if append == 0 {
                    self.parts[index].slots[slot]
                } else {
                    0
                }
            });
            delta += i64::from(fp != 0) - i64::from(old != 0);
            if fp != 0 {
                fingerprints[fp as usize] += 1;
            }
            if old != 0 {
                fingerprints[old as usize] -= 1;
            }
        }
        d.finish()?;
        if self.count as i64 + delta != count as i64
            || n == 0
            || (deletes != self.deletes && delta != -1)
        {
            return Err(invalid());
        }
        if !matches!(delta, -1 | 1)
            || fingerprints.iter().filter(|change| **change != 0).count() != 1
            || !fingerprints.contains(&(delta as i32))
        {
            return Err(invalid());
        }
        if append != 0 {
            self.parts.push(CuckooPart {
                buckets: append,
                slots: vec![0; length],
            });
        }
        for (slot, fp) in changes {
            self.parts[index].slots[slot] = fp;
        }
        self.count = count;
        self.deletes = deletes;
        Ok(())
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            value: SnapshotValue::Cuckoo(self),
            initial: true,
            part: 0,
            offset: 0,
        }
    }
}

fn span(index: usize, offset: usize, bytes: &[u8]) -> io::Result<Bytes> {
    encode(
        |e| {
            e.u32(index as u32)?;
            e.u64(offset as u64)?;
            e.bytes(bytes)
        },
        2,
    )
}
fn read_span<'a>(d: &mut Decoder<'a>) -> io::Result<(usize, usize, &'a [u8])> {
    let index = d.u32()? as usize;
    let offset = usize64(d.u64()?)?;
    let bytes = d.bytes()?;
    if bytes.is_empty() || bytes.len() > CHUNK {
        return Err(invalid());
    }
    Ok((index, offset, bytes))
}
enum SnapshotValue<'a> {
    Bloom(&'a BloomValue),
    Cuckoo(&'a CuckooValue),
}
pub(super) struct Snapshot<'a> {
    value: SnapshotValue<'a>,
    initial: bool,
    part: usize,
    offset: usize,
}
impl Snapshot<'_> {
    fn parts(&self) -> usize {
        match self.value {
            SnapshotValue::Bloom(v) => v.parts.len(),
            SnapshotValue::Cuckoo(v) => v.parts.len(),
        }
    }
    fn bytes(&self, index: usize) -> &[u8] {
        match self.value {
            SnapshotValue::Bloom(v) => &v.parts[index].bits,
            SnapshotValue::Cuckoo(v) => &v.parts[index].slots,
        }
    }
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.initial {
            self.initial = false;
            return Some(match self.value {
                SnapshotValue::Bloom(v) => v.initial(),
                SnapshotValue::Cuckoo(v) => v.initial(),
            });
        }
        if self.part >= self.parts() {
            return None;
        }
        let end = (self.offset + CHUNK).min(self.bytes(self.part).len());
        let result = span(
            self.part,
            self.offset,
            &self.bytes(self.part)[self.offset..end],
        );
        if end == self.bytes(self.part).len() {
            self.part += 1;
            self.offset = 0;
        } else {
            self.offset = end;
        }
        Some(result)
    }
}
fn load_effect(previous: u64, next: u64, done: bool, inner: &[u8]) -> io::Result<Bytes> {
    encode(
        |e| {
            e.u64(previous)?;
            e.u64(next)?;
            e.u8(u8::from(done))?;
            e.bytes(inner)
        },
        4,
    )
}
fn read_load<'a>(d: &mut Decoder<'a>) -> io::Result<(u64, u64, bool, &'a [u8])> {
    let previous = d.u64()?;
    let next = d.u64()?;
    let done = d.u8()?;
    let inner = d.bytes()?;
    if done > 1 || previous.checked_add(1) != Some(next) || next > MAX_CURSOR {
        return Err(invalid());
    }
    Ok((previous, next, done == 1, inner))
}
fn validate_load_span<'a>(
    snapshot: Snapshot<'_>,
    inner: &'a [u8],
    previous: u64,
    done: bool,
) -> io::Result<(usize, usize, &'a [u8])> {
    let (op, mut d) = Decoder::new(inner, VERSION)?;
    if op != 2 {
        return Err(invalid());
    }
    let (index, offset, bytes) = read_span(&mut d)?;
    d.finish()?;
    let mut page = previous.checked_sub(1).ok_or_else(invalid)?;
    let mut part = 0;
    while part < snapshot.parts() {
        let length = snapshot.bytes(part).len();
        let pages = length.div_ceil(CHUNK) as u64;
        if page < pages {
            let expected_offset = page as usize * CHUNK;
            let expected_len = CHUNK.min(length - expected_offset);
            let last = part + 1 == snapshot.parts() && expected_offset + expected_len == length;
            if index != part
                || offset != expected_offset
                || bytes.len() != expected_len
                || done != last
            {
                return Err(invalid());
            }
            return Ok((index, offset, bytes));
        }
        page -= pages;
        part += 1;
    }
    Err(invalid())
}
fn dump(snapshot: Snapshot<'_>, cursor: u64, kind: u8) -> io::Result<Option<(u64, Bytes)>> {
    if cursor > MAX_CURSOR {
        return Err(invalid());
    }
    let mut snapshot = snapshot;
    // Only metadata is traversed: skip byte pages without encoding or copying them.
    let inner = if cursor == 0 {
        snapshot.next().transpose()?
    } else {
        snapshot.initial = false;
        let mut page = cursor - 1;
        while snapshot.part < snapshot.parts() {
            let pages = snapshot.bytes(snapshot.part).len().div_ceil(CHUNK) as u64;
            if page < pages {
                snapshot.offset = page as usize * CHUNK;
                break;
            }
            page -= pages;
            snapshot.part += 1;
        }
        snapshot.next().transpose()?
    };
    let Some(inner) = inner else {
        return Ok(None);
    };
    let done = !snapshot.initial && snapshot.part == snapshot.parts();
    let next = cursor + 1;
    let effect = load_effect(cursor, next, done, &inner)?;
    let mut bytes = Vec::with_capacity(effect.len() + 13);
    bytes.extend_from_slice(b"FFKF");
    bytes.push(kind);
    bytes.extend_from_slice(&hash(&effect, 0x46464b46).to_le_bytes());
    bytes.extend_from_slice(&effect);
    Ok(Some((next, Bytes::from(bytes))))
}
fn decode_dump(bytes: &[u8], kind: u8, iterator: u64) -> io::Result<&[u8]> {
    if bytes.len() < 15 || &bytes[..4] != b"FFKF" || bytes[4] != kind {
        return Err(invalid());
    }
    let checksum = u64::from_le_bytes(bytes[5..13].try_into().unwrap());
    let effect = &bytes[13..];
    if hash(effect, 0x46464b46) != checksum {
        return Err(invalid());
    }
    let (op, mut d) = Decoder::new(effect, VERSION)?;
    if op != 4 {
        return Err(invalid());
    }
    let (_, next, _, _) = read_load(&mut d)?;
    d.finish()?;
    if next != iterator {
        return Err(invalid());
    }
    Ok(effect)
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "bf.reserve",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.add",
        3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.madd",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.insert",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.exists",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.mexists",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.card",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.info",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.debug",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.scandump",
        3,
        &["readonly"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "bf.loadchunk",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.reserve",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.add",
        3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.addnx",
        3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.insert",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.insertnx",
        -4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.exists",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.mexists",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.count",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.del",
        3,
        &["write", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.info",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.debug",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.scandump",
        3,
        &["readonly"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.loadchunk",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::filter_command,
    ),
    CommandSpec::handler(
        "cf.compact",
        2,
        &["write"],
        (1, 1, 1),
        Engine::filter_command,
    ),
];
fn number(bytes: &[u8]) -> Result<u64, Resp> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| error("ERR invalid integer"))
}
fn rate(bytes: &[u8]) -> Result<f64, Resp> {
    let r = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|r| r.is_finite() && *r > 0.0 && *r < 1.0)
        .ok_or_else(|| error("ERR error rate must be between 0 and 1"))?;
    Ok(r.min(0.25))
}
fn same(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}
fn bulk(s: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(s.into()))
}
fn ok() -> Resp {
    Resp::Simple("OK")
}
fn full() -> Resp {
    error("ERR filter is full or allocation limit exceeded")
}
fn missing() -> Resp {
    error("ERR not found")
}
fn syntax() -> Resp {
    error("ERR syntax error")
}
fn typed_bloom(value: &Value) -> Option<&BloomValue> {
    if let Value::Typed(v) = value
        && let Data::Bloom(v) = &**v
    {
        return Some(v);
    }
    None
}
fn typed_cuckoo(value: &Value) -> Option<&CuckooValue> {
    if let Value::Typed(v) = value
        && let Data::Cuckoo(v) = &**v
    {
        return Some(v);
    }
    None
}
fn bloom_mut(value: &mut Value) -> Option<&mut BloomValue> {
    if let Value::Typed(v) = value
        && let Data::Bloom(v) = &mut **v
    {
        return Some(v);
    }
    None
}
fn cuckoo_mut(value: &mut Value) -> Option<&mut CuckooValue> {
    if let Value::Typed(v) = value
        && let Data::Cuckoo(v) = &mut **v
    {
        return Some(v);
    }
    None
}
struct Options {
    capacity: u64,
    rate: f64,
    expansion: u32,
    bucket: u16,
    iterations: u16,
    nocreate: bool,
    items: usize,
}
fn options(args: &[Bytes], bloom: bool, reserve: bool) -> Result<Options, Resp> {
    let mut o = Options {
        capacity: if bloom { 100 } else { 1024 },
        rate: 0.01,
        expansion: if bloom { 2 } else { 1 },
        bucket: 2,
        iterations: 20,
        nocreate: false,
        items: args.len(),
    };
    let mut i = 2;
    let mut explicit_expansion = false;
    let mut nonscaling = false;
    let mut creation = false;
    if reserve {
        if bloom {
            o.rate = rate(&args[2])?;
            o.capacity = number(&args[3])?;
            i = 4;
        } else {
            o.capacity = number(&args[2])?;
            i = 3;
        }
    }
    while i < args.len() {
        let name = &args[i];
        if !reserve && same(name, b"ITEMS") {
            o.items = i + 1;
            break;
        }
        if bloom && same(name, b"NONSCALING") {
            nonscaling = true;
            i += 1;
            continue;
        }
        if !reserve && same(name, b"NOCREATE") {
            o.nocreate = true;
            i += 1;
            continue;
        }
        let value = args.get(i + 1).ok_or_else(syntax)?;
        if !reserve && same(name, b"CAPACITY") {
            o.capacity = number(value)?;
            creation = true;
        } else if bloom && !reserve && same(name, b"ERROR") {
            o.rate = rate(value)?;
            creation = true;
        } else if same(name, b"EXPANSION") && (bloom || reserve) {
            let n = number(value)?;
            if n > 32768 {
                return Err(error("ERR expansion must be between 0 and 32768"));
            }
            o.expansion = n as u32;
            explicit_expansion = true;
        } else if !bloom && reserve && same(name, b"BUCKETSIZE") {
            let n = number(value)?;
            if !(1..=255).contains(&n) {
                return Err(error("ERR bucket size must be between 1 and 255"));
            }
            o.bucket = n as u16;
        } else if !bloom && reserve && same(name, b"MAXITERATIONS") {
            let n = number(value)?;
            if !(1..=65535).contains(&n) {
                return Err(error("ERR max iterations must be between 1 and 65535"));
            }
            o.iterations = n as u16;
        } else {
            return Err(syntax());
        }
        i += 2;
    }
    if nonscaling {
        if explicit_expansion && o.expansion != 0 {
            return Err(error("ERR NONSCALING conflicts with EXPANSION"));
        }
        o.expansion = 0;
    }
    if bloom && o.nocreate && creation {
        return Err(error("ERR NOCREATE conflicts with CAPACITY or ERROR"));
    }
    if !reserve && o.items >= args.len() {
        return Err(syntax());
    }
    if o.capacity == 0 {
        return Err(error("ERR capacity must be positive"));
    }
    Ok(o)
}
impl Engine {
    fn filter_command(&mut self, args: &[Bytes], now: i64) -> Resp {
        let bloom = same(&args[0][..2], b"bf");
        let command = &args[0][3..];
        if same(command, b"reserve") {
            return self.filter_reserve(args, now, bloom);
        }
        if same(command, b"add")
            || same(command, b"madd")
            || same(command, b"addnx")
            || same(command, b"insert")
            || same(command, b"insertnx")
        {
            return self.filter_insert(args, now, bloom);
        }
        if same(command, b"exists") || same(command, b"mexists") || same(command, b"count") {
            let multiple = same(command, b"mexists");
            let count = same(command, b"count");
            if multiple {
                let mut budget = super::ReplyBudget::new(self.reply_limit);
                if let Err(e) = budget.charge(args.len() - 2, 0) {
                    return e;
                }
            }
            let value = self.value(&args[1], now);
            let replies = args[2..].iter().map(|item| {
                let n = if bloom {
                    value
                        .and_then(typed_bloom)
                        .is_some_and(|v| v.contains(item)) as u64
                } else {
                    value.and_then(typed_cuckoo).map_or(0, |v| v.matches(item))
                };
                if count {
                    Resp::Integer(n as i64)
                } else {
                    Resp::Boolean(n != 0)
                }
            });
            return if multiple {
                Resp::Array(Some(replies.collect()))
            } else {
                replies.into_iter().next().unwrap()
            };
        }
        if same(command, b"card") {
            return match self.value(&args[1], now) {
                None => Resp::Integer(0),
                Some(v) => {
                    typed_bloom(v).map_or_else(wrong_type, |v| Resp::Integer(v.count() as i64))
                }
            };
        }
        if same(command, b"info") || same(command, b"debug") {
            return self.filter_info(args, now, bloom, same(command, b"debug"));
        }
        if same(command, b"scandump") {
            return self.filter_dump(args, now, bloom);
        }
        if same(command, b"loadchunk") {
            return self.filter_load(args, now, bloom);
        }
        if same(command, b"compact") {
            return self.filter_compact(&args[1], now);
        }
        if same(command, b"del") {
            self.discard_expired(&args[1], now);
            let result = match self.entries.get_mut(&args[1]) {
                None => return missing(),
                Some(entry) => match cuckoo_mut(&mut entry.value) {
                    None => return wrong_type(),
                    Some(v) => v.delete(&args[2]),
                },
            };
            return match result {
                Err(_) => error("ERR incomplete filter load"),
                Ok(None) => Resp::Boolean(false),
                Ok(Some(effect)) => {
                    self.record_typed(&args[1], 6, effect, now);
                    Resp::Boolean(true)
                }
            };
        }
        syntax()
    }
    fn filter_reserve(&mut self, args: &[Bytes], now: i64, bloom: bool) -> Resp {
        let o = match options(args, bloom, true) {
            Ok(o) => o,
            Err(e) => return e,
        };
        self.discard_expired(&args[1], now);
        if self.entries.contains_key(&args[1]) {
            return error("ERR item exists");
        }
        let (data, effect) = if bloom {
            let v = match BloomValue::new(o.capacity, o.rate, o.expansion) {
                Ok(v) => v,
                Err(_) => return full(),
            };
            let effect = v.initial().expect("bounded filter metadata");
            (Data::Bloom(v), effect)
        } else {
            let v = match CuckooValue::new(o.capacity, o.bucket, o.iterations, o.expansion) {
                Ok(v) => v,
                Err(_) => return full(),
            };
            let effect = v.initial().expect("bounded filter metadata");
            (Data::Cuckoo(v), effect)
        };
        self.write_entry(
            args[1].clone(),
            Entry {
                value: Value::Typed(Box::new(data)),
                expires_at: None,
                access: crate::engine::AccessMetadata::default(),
            },
            now,
        );
        self.record_typed(&args[1], if bloom { 5 } else { 6 }, effect, now);
        ok()
    }
    fn filter_insert(&mut self, args: &[Bytes], now: i64, bloom: bool) -> Resp {
        let command = &args[0][3..];
        let insert = same(command, b"insert") || same(command, b"insertnx");
        let multiple = insert || same(command, b"madd");
        let nx = same(command, b"addnx") || same(command, b"insertnx");
        let o = if insert {
            match options(args, bloom, false) {
                Ok(o) => o,
                Err(e) => return e,
            }
        } else {
            Options {
                capacity: if bloom { 100 } else { 1024 },
                rate: 0.01,
                expansion: if bloom { 2 } else { 1 },
                bucket: 2,
                iterations: 20,
                nocreate: false,
                items: 2,
            }
        };
        if multiple {
            let mut budget = super::ReplyBudget::new(self.reply_limit);
            if let Err(e) = budget.charge(args.len() - o.items, 128) {
                return e;
            }
        }
        self.discard_expired(&args[1], now);
        if let Some(entry) = self.entries.get(&args[1]) {
            if (bloom && typed_bloom(&entry.value).is_none())
                || (!bloom && typed_cuckoo(&entry.value).is_none())
            {
                return wrong_type();
            }
        } else {
            if o.nocreate {
                return missing();
            }
            let (data, effect) = if bloom {
                let v = match BloomValue::new(o.capacity, o.rate, o.expansion) {
                    Ok(v) => v,
                    Err(_) => return full(),
                };
                let e = v.initial().expect("bounded filter metadata");
                (Data::Bloom(v), e)
            } else {
                let v = match CuckooValue::new(o.capacity, o.bucket, o.iterations, o.expansion) {
                    Ok(v) => v,
                    Err(_) => return full(),
                };
                let e = v.initial().expect("bounded filter metadata");
                (Data::Cuckoo(v), e)
            };
            self.write_entry(
                args[1].clone(),
                Entry {
                    value: Value::Typed(Box::new(data)),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
            self.record_typed(&args[1], if bloom { 5 } else { 6 }, effect, now);
        }
        let mut replies = Vec::with_capacity(if multiple { args.len() - o.items } else { 1 });
        for item in &args[o.items..] {
            let entry = self.entries.get_mut(&args[1]).unwrap();
            let result = if bloom {
                bloom_mut(&mut entry.value).unwrap().insert(item)
            } else {
                cuckoo_mut(&mut entry.value).unwrap().insert(item, nx)
            };
            match result {
                Ok(effect) => {
                    let added = effect.is_some();
                    if let Some(effect) = effect {
                        self.record_typed(&args[1], if bloom { 5 } else { 6 }, effect, now);
                    }
                    replies.push(if !bloom && same(command, b"insertnx") {
                        Resp::Integer(added as i64)
                    } else {
                        Resp::Boolean(added)
                    });
                }
                Err(_) => {
                    replies.push(if bloom || !insert {
                        full()
                    } else if same(command, b"insert")
                        && self.command_version == crate::protocol::Version::Resp3
                    {
                        Resp::Boolean(false)
                    } else {
                        Resp::Integer(-1)
                    });
                    if bloom {
                        break;
                    }
                }
            }
        }
        if multiple {
            Resp::Array(Some(replies))
        } else {
            replies.pop().unwrap()
        }
    }
    fn filter_info(&mut self, args: &[Bytes], now: i64, bloom: bool, debug: bool) -> Resp {
        if args.len() > 3 {
            return syntax();
        }
        let value = match self.value(&args[1], now) {
            Some(v) => v,
            None => return missing(),
        };
        let fields: Vec<(&str, Resp)> = if bloom {
            let v = match typed_bloom(value) {
                Some(v) => v,
                None => return wrong_type(),
            };
            if debug {
                let mut rows = vec![bulk(format!("size:{}", v.count()))];
                for p in &v.parts {
                    rows.push(bulk(format!(
                        "bytes:{} bits:{} hashes:{} hashwidth:64 capacity:{} size:{} ratio:{}",
                        p.bits.len(),
                        p.bits.len() * 8,
                        p.meta.hashes,
                        p.meta.capacity,
                        p.count,
                        p.meta.rate
                    )));
                }
                return super::bounded_replies(rows, self.reply_limit);
            }
            vec![
                (
                    "Capacity",
                    Resp::Integer(v.parts.iter().map(|p| p.meta.capacity as i64).sum()),
                ),
                ("Size", Resp::Integer(v.memory() as i64)),
                ("Number of filters", Resp::Integer(v.parts.len() as i64)),
                ("Number of items inserted", Resp::Integer(v.count() as i64)),
                (
                    "Expansion rate",
                    if v.expansion == 0 {
                        Resp::Bulk(None)
                    } else {
                        Resp::Integer(v.expansion as i64)
                    },
                ),
            ]
        } else {
            let v = match typed_cuckoo(value) {
                Some(v) => v,
                None => return wrong_type(),
            };
            if debug {
                return bulk(format!(
                    "bktsize:{} buckets:{} items:{} deletes:{} filters:{} max_iterations:{} expansion:{}",
                    v.bucket_size,
                    v.parts[0].buckets,
                    v.count,
                    v.deletes,
                    v.parts.len(),
                    v.iterations,
                    v.expansion
                ));
            }
            vec![
                ("Size", Resp::Integer(v.memory() as i64)),
                (
                    "Number of buckets",
                    Resp::Integer(v.parts[0].buckets as i64),
                ),
                ("Number of filters", Resp::Integer(v.parts.len() as i64)),
                ("Number of items inserted", Resp::Integer(v.count as i64)),
                ("Number of items deleted", Resp::Integer(v.deletes as i64)),
                ("Bucket size", Resp::Integer(v.bucket_size as i64)),
                ("Expansion rate", Resp::Integer(v.expansion as i64)),
                ("Max iterations", Resp::Integer(v.iterations as i64)),
            ]
        };
        if let Some(field) = args.get(2) {
            let index = if same(field, b"CAPACITY") {
                Some(0)
            } else if same(field, b"SIZE") {
                Some(1)
            } else if same(field, b"FILTERS") {
                Some(2)
            } else if same(field, b"ITEMS") {
                Some(3)
            } else if same(field, b"EXPANSION") {
                Some(4)
            } else {
                None
            };
            if !bloom {
                return syntax();
            }
            let Some(index) = index else {
                return error("ERR invalid information value");
            };
            let (name, value) = fields.into_iter().nth(index).unwrap();
            return Resp::InfoField(Bytes::copy_from_slice(name.as_bytes()), Box::new(value));
        }
        Resp::Map(
            fields
                .into_iter()
                .map(|(name, value)| (bulk(Bytes::copy_from_slice(name.as_bytes())), value))
                .collect(),
        )
    }
    fn filter_dump(&mut self, args: &[Bytes], now: i64, bloom: bool) -> Resp {
        let cursor = match number(&args[2]) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let value = match self.value(&args[1], now) {
            Some(v) => v,
            None => return missing(),
        };
        let result = if bloom {
            let Some(v) = typed_bloom(value) else {
                return wrong_type();
            };
            if v.loading != 0 {
                return error("ERR incomplete filter load");
            }
            dump(v.snapshot(), cursor, 5)
        } else {
            let Some(v) = typed_cuckoo(value) else {
                return wrong_type();
            };
            if v.loading != 0 {
                return error("ERR incomplete filter load");
            }
            dump(v.snapshot(), cursor, 6)
        };
        let response = match result {
            Err(_) => return error("ERR invalid iterator"),
            Ok(None) => Resp::Array(Some(vec![
                Resp::Integer(0),
                if bloom {
                    bulk(Bytes::new())
                } else {
                    Resp::Bulk(None)
                },
            ])),
            Ok(Some((next, data))) => {
                Resp::Array(Some(vec![Resp::Integer(next as i64), bulk(data)]))
            }
        };
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        match budget.response(&response) {
            Ok(()) => response,
            Err(e) => e,
        }
    }
    fn filter_load(&mut self, args: &[Bytes], now: i64, bloom: bool) -> Resp {
        let iterator = match number(&args[2]) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let kind = if bloom { 5 } else { 6 };
        let effect = match decode_dump(&args[3], kind, iterator) {
            Ok(e) => e,
            Err(_) => return error("ERR invalid filter chunk"),
        };
        self.discard_expired(&args[1], now);
        if let Some(entry) = self.entries.get_mut(&args[1]) {
            let result = if bloom {
                match bloom_mut(&mut entry.value) {
                    Some(v) => v.apply(effect),
                    None => return wrong_type(),
                }
            } else {
                match cuckoo_mut(&mut entry.value) {
                    Some(v) => v.apply(effect),
                    None => return wrong_type(),
                }
            };
            if result.is_err() {
                return error("ERR invalid or out-of-order filter chunk");
            }
        } else {
            let data = if bloom {
                match BloomValue::restore(effect) {
                    Ok(v) => Data::Bloom(v),
                    Err(_) => return error("ERR invalid filter header"),
                }
            } else {
                match CuckooValue::restore(effect) {
                    Ok(v) => Data::Cuckoo(v),
                    Err(_) => return error("ERR invalid filter header"),
                }
            };
            self.write_entry(
                args[1].clone(),
                Entry {
                    value: Value::Typed(Box::new(data)),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
        }
        self.record_typed(&args[1], kind, Bytes::copy_from_slice(effect), now);
        ok()
    }
    fn filter_compact(&mut self, key: &Bytes, now: i64) -> Resp {
        self.discard_expired(key, now);
        let n = match self.entries.get(key) {
            None => return missing(),
            Some(entry) => match typed_cuckoo(&entry.value) {
                None => return wrong_type(),
                Some(v) => {
                    if v.loading != 0 {
                        return error("ERR incomplete filter load");
                    }
                    v.parts.len()
                }
            },
        };
        for source in (1..n).rev() {
            let slots = cuckoo_mut(&mut self.entries.get_mut(key).unwrap().value)
                .unwrap()
                .parts[source]
                .slots
                .len();
            for slot in 0..slots {
                let result = cuckoo_mut(&mut self.entries.get_mut(key).unwrap().value)
                    .unwrap()
                    .compact_slot(source, slot);
                match result {
                    Err(_) => return error("ERR filter compaction failed"),
                    Ok(None) => (),
                    Ok(Some((to, from))) => {
                        self.record_typed(key, 6, to, now);
                        self.record_typed(key, 6, from, now);
                    }
                }
            }
        }
        match cuckoo_mut(&mut self.entries.get_mut(key).unwrap().value)
            .unwrap()
            .compact_trim()
        {
            Err(_) => error("ERR filter compaction failed"),
            Ok(effect) => {
                if let Some(effect) = effect {
                    self.record_typed(key, 6, effect, now);
                }
                ok()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn item(n: u64) -> [u8; 8] {
        n.to_le_bytes()
    }
    fn replay_bloom(v: &BloomValue) -> BloomValue {
        let mut s = v.snapshot();
        let mut restored = BloomValue::restore(&s.next().unwrap().unwrap()).unwrap();
        for effect in s {
            restored.apply(&effect.unwrap()).unwrap();
        }
        restored
    }
    fn replay_cuckoo(v: &CuckooValue) -> CuckooValue {
        let mut s = v.snapshot();
        let mut restored = CuckooValue::restore(&s.next().unwrap().unwrap()).unwrap();
        for effect in s {
            restored.apply(&effect.unwrap()).unwrap();
        }
        restored
    }
    #[test]
    fn bloom_quality_scaling_no_false_negatives_and_incremental_replay() {
        let mut v = BloomValue::new(100, 0.01, 2).unwrap();
        let mut replay = BloomValue::restore(&v.initial().unwrap()).unwrap();
        for n in 0..5000 {
            if let Some(effect) = v.insert(&item(n)).unwrap() {
                assert!(effect.len() < 2048);
                replay.apply(&effect).unwrap();
            }
        }
        assert!(v.parts.len() > 1);
        for n in 0..5000 {
            assert!(v.contains(&item(n)));
            assert!(replay.contains(&item(n)));
        }
        let positives = (100_000..120_000).filter(|n| v.contains(&item(*n))).count();
        assert!(
            positives < 600,
            "Bloom false-positive rate: {positives}/20000"
        );
        assert!(
            v.memory() < 5000 * 8,
            "must store bits, not item identities"
        );
        let restored = replay_bloom(&v);
        assert_eq!(v.count(), restored.count());
        for n in 0..5000 {
            assert!(restored.contains(&item(n)));
        }
    }
    #[test]
    fn fixed_bloom_capacity_duplicate_and_failed_insert_are_nondestructive() {
        let mut v = BloomValue::new(20, 0.001, 0).unwrap();
        let mut accepted = Vec::new();
        for n in 0..1000 {
            match v.insert(&item(n)) {
                Ok(Some(_)) => accepted.push(n),
                Ok(None) => (),
                Err(_) => break,
            }
        }
        assert_eq!(v.count(), 20);
        let before = v.snapshot().collect::<io::Result<Vec<_>>>().unwrap();
        assert!(v.insert(&item(accepted[0])).unwrap().is_none());
        assert!(v.insert(&item(1_000_000)).is_err());
        assert_eq!(
            before,
            v.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
        );
        for n in accepted {
            assert!(v.contains(&item(n)));
        }
    }
    #[test]
    fn cuckoo_quality_real_fingerprints_counts_and_duplicate_deletion() {
        let mut v = CuckooValue::new(10000, 2, 100, 1).unwrap();
        let mut replay = CuckooValue::restore(&v.initial().unwrap()).unwrap();
        for n in 0..6000 {
            let effect = v.insert(&item(n), false).unwrap().unwrap();
            replay.apply(&effect).unwrap();
        }
        for n in 0..6000 {
            assert!(v.matches(&item(n)) > 0);
            assert!(replay.matches(&item(n)) > 0);
        }
        let positives = (100_000..120_000)
            .filter(|n| v.matches(&item(*n)) != 0)
            .count();
        assert!(
            positives < 800,
            "Cuckoo false-positive rate: {positives}/20000"
        );
        let mut duplicates = CuckooValue::new(16, 2, 20, 1).unwrap();
        for _ in 0..3 {
            duplicates.insert(b"same", false).unwrap();
        }
        assert_eq!(duplicates.matches(b"same"), 3);
        assert!(duplicates.insert(b"same", true).unwrap().is_none());
        for remaining in (0..3).rev() {
            duplicates.delete(b"same").unwrap().unwrap();
            assert_eq!(duplicates.matches(b"same"), remaining);
        }
        assert!(duplicates.delete(b"same").unwrap().is_none());
        assert_eq!(duplicates.deletes, 3);
    }
    #[test]
    fn cuckoo_reuses_earlier_parts_and_replays_the_selected_part() {
        let mut v = CuckooValue::new(4, 1, 20, 1).unwrap();
        let mut replay = CuckooValue::restore(&v.initial().unwrap()).unwrap();
        for item in [b"".as_slice(), b"", b"", b"", b"hello", b"hello"] {
            let effect = v.insert(item, false).unwrap().unwrap();
            replay.apply(&effect).unwrap();
            assert_eq!(
                v.snapshot().collect::<io::Result<Vec<_>>>().unwrap(),
                replay.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
            );
        }
        assert_eq!(v.parts.len(), 2);
        assert_eq!(v.memory(), 8);
        assert_eq!(v.count, 6);
        assert_eq!(v.matches(b""), 4);
        assert_eq!(v.matches(b"hello"), 2);
        assert!(v.insert(b"hello", true).unwrap().is_none());
        let restored = replay_cuckoo(&v);
        assert_eq!(restored.matches(b""), 4);
        assert_eq!(restored.matches(b"hello"), 2);
        assert_eq!(restored.count, 6);
        for remaining in (0..2).rev() {
            let effect = v.delete(b"hello").unwrap().unwrap();
            replay.apply(&effect).unwrap();
            assert_eq!(v.matches(b"hello"), remaining);
            assert_eq!(replay.matches(b"hello"), remaining);
            assert_eq!(v.matches(b""), 4);
        }
        assert_eq!(v.count, 4);
        assert_eq!(v.deletes, 2);
        assert_eq!(
            v.snapshot().collect::<io::Result<Vec<_>>>().unwrap(),
            replay.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
        );
    }
    #[test]
    fn failed_cuckoo_kickouts_rollback_and_growth_replays_resolved_cells() {
        let mut fixed = CuckooValue::new(4, 1, 64, 0).unwrap();
        let mut accepted = Vec::new();
        for n in 0..1000 {
            if fixed.count == fixed.memory() as u64 {
                break;
            }
            if fixed.insert(&item(n), false).is_ok() {
                accepted.push(n);
            }
        }
        assert_eq!(fixed.count, fixed.memory() as u64);
        assert!(!accepted.is_empty());
        let before = fixed.snapshot().collect::<io::Result<Vec<_>>>().unwrap();
        for n in 1000..1100 {
            let _ = fixed.insert(&item(n), false);
            for old in &accepted {
                assert!(fixed.matches(&item(*old)) > 0);
            }
        }
        assert_eq!(
            before,
            fixed.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
        );
        let mut growing = CuckooValue::new(4, 2, 128, 2).unwrap();
        let mut replay = CuckooValue::restore(&growing.initial().unwrap()).unwrap();
        for n in 0..3000 {
            let e = growing.insert(&item(n), false).unwrap().unwrap();
            replay.apply(&e).unwrap();
        }
        assert!(growing.parts.len() > 1);
        assert_eq!(
            growing.snapshot().collect::<io::Result<Vec<_>>>().unwrap(),
            replay.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
        );
        let restored = replay_cuckoo(&growing);
        for n in 0..3000 {
            assert!(growing.matches(&item(n)) > 0);
            assert!(restored.matches(&item(n)) > 0);
        }
    }
    #[test]
    fn compaction_relocates_fingerprints_and_releases_empty_tail() {
        let mut v = CuckooValue::new(4, 2, 100, 2).unwrap();
        for n in 0..30 {
            v.insert(&item(n), false).unwrap();
        }
        // Remove all but a few real fingerprint slots with resolved effects;
        // unlike deleting unknown user items, this cannot delete a collision.
        let mut kept = 0;
        let mut removed = 0;
        for p in v.parts.iter_mut().rev() {
            for fp in &mut p.slots {
                if *fp != 0 {
                    if kept < 2 {
                        kept += 1;
                    } else {
                        *fp = 0;
                        removed += 1;
                    }
                }
            }
        }
        v.count -= removed;
        let old_parts = v.parts.len();
        let mut replay = replay_cuckoo(&v);
        for source in (1..v.parts.len()).rev() {
            for slot in 0..v.parts[source].slots.len() {
                if let Some((to, from)) = v.compact_slot(source, slot).unwrap() {
                    replay.apply(&to).unwrap();
                    replay.apply(&from).unwrap();
                }
            }
        }
        if let Some(e) = v.compact_trim().unwrap() {
            replay.apply(&e).unwrap();
        }
        assert!(v.parts.len() < old_parts);
        assert_eq!(v.count, 2);
        assert_eq!(
            v.parts
                .iter()
                .flat_map(|p| &p.slots)
                .filter(|fp| **fp != 0)
                .count(),
            2
        );
        assert_eq!(
            v.snapshot().collect::<io::Result<Vec<_>>>().unwrap(),
            replay.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
        );
    }
    #[test]
    fn bounded_dump_load_roundtrip_rejects_corruption_and_out_of_order() {
        let mut v = BloomValue::new(100000, 0.001, 2).unwrap();
        for n in 0..1000 {
            v.insert(&item(n)).unwrap();
        }
        let mut cursor = 0;
        let mut restored: Option<BloomValue> = None;
        while let Some((next, data)) = dump(v.snapshot(), cursor, 5).unwrap() {
            assert!(data.len() <= CHUNK + 128);
            let effect = decode_dump(&data, 5, next).unwrap();
            let mut corrupt = data.to_vec();
            let last = corrupt.len() - 1;
            corrupt[last] ^= 1;
            assert!(decode_dump(&corrupt, 5, next).is_err());
            assert!(decode_dump(&data, 6, next).is_err());
            assert!(decode_dump(&data, 5, next + 1).is_err());
            if let Some(r) = &mut restored {
                r.apply(effect).unwrap();
                assert!(r.apply(effect).is_err());
            } else {
                restored = Some(BloomValue::restore(effect).unwrap());
            }
            cursor = next;
        }
        let r = restored.unwrap();
        assert_eq!(r.loading, 0);
        assert_eq!(r.count(), v.count());
        for n in 0..1000 {
            assert!(r.contains(&item(n)));
        }
        let mut cf = CuckooValue::new(4, 2, 50, 2).unwrap();
        for n in 0..300 {
            cf.insert(&item(n), false).unwrap();
        }
        let mut cursor = 0;
        let mut restored: Option<CuckooValue> = None;
        while let Some((next, data)) = dump(cf.snapshot(), cursor, 6).unwrap() {
            let effect = decode_dump(&data, 6, next).unwrap();
            if let Some(r) = &mut restored {
                r.apply(effect).unwrap();
            } else {
                restored = Some(CuckooValue::restore(effect).unwrap());
            }
            cursor = next;
        }
        let r = restored.unwrap();
        assert_eq!(r.loading, 0);
        assert_eq!(r.count, cf.count);
        for n in 0..300 {
            assert!(r.matches(&item(n)) > 0);
        }
    }
    #[test]
    fn invalid_allocations_and_truncated_effects_fail_before_changes() {
        assert!(BloomValue::new(u64::MAX, 0.01, 2).is_err());
        assert!(BloomValue::new(100, f64::NAN, 2).is_err());
        assert!(BloomValue::new(100, 0.0, 2).is_err());
        assert!(CuckooValue::new(u64::MAX, 2, 20, 1).is_err());
        assert!(CuckooValue::new(4, 0, 20, 1).is_err());
        assert!(CuckooValue::new(4, 2, 0, 1).is_err());
        let mut b = BloomValue::new(100, 0.01, 2).unwrap();
        let e = b.insert(b"member").unwrap().unwrap();
        for end in 0..e.len() {
            let mut target = BloomValue::new(100, 0.01, 2).unwrap();
            assert!(target.apply(&e[..end]).is_err());
            assert_eq!(target.count(), 0);
            assert!(!target.contains(b"member"));
        }
        let mut c = CuckooValue::new(16, 2, 20, 1).unwrap();
        let e = c.insert(b"member", false).unwrap().unwrap();
        for end in 0..e.len() {
            let mut target = CuckooValue::new(16, 2, 20, 1).unwrap();
            assert!(target.apply(&e[..end]).is_err());
            assert_eq!(target.count, 0);
            assert_eq!(target.matches(b"member"), 0);
        }
    }
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    #[test]
    fn insert_ignores_existing_filters_creation_budget_but_validates_options() {
        let mut engine = Engine::new();
        for (reserve, insert, exists) in [
            (
                vec!["BF.RESERVE", "bf", "0.01", "10"],
                "BF.INSERT",
                "BF.EXISTS",
            ),
            (vec!["CF.RESERVE", "cf", "10"], "CF.INSERT", "CF.EXISTS"),
        ] {
            let key = reserve[1];
            assert_eq!(run(&mut engine, &reserve), ok());
            assert_eq!(
                run(
                    &mut engine,
                    &[insert, key, "CAPACITY", "1073741824", "ITEMS", "x"]
                ),
                Resp::Array(Some(vec![Resp::Boolean(true)]))
            );
            assert_eq!(run(&mut engine, &[exists, key, "x"]), Resp::Boolean(true));
            for capacity in ["0", "not-an-integer"] {
                assert!(matches!(
                    run(
                        &mut engine,
                        &[insert, key, "CAPACITY", capacity, "ITEMS", "invalid"]
                    ),
                    Resp::Error(_)
                ));
                assert_eq!(
                    run(&mut engine, &[exists, key, "invalid"]),
                    Resp::Boolean(false)
                );
            }
            assert!(matches!(
                run(
                    &mut engine,
                    &[insert, "missing", "CAPACITY", "1073741824", "ITEMS", "x"]
                ),
                Resp::Error(_)
            ));
            assert_eq!(run(&mut engine, &["EXISTS", "missing"]), Resp::Integer(0));
            let oversized_reserve = if insert == "BF.INSERT" {
                vec!["BF.RESERVE", "missing", "0.01", "1073741824"]
            } else {
                vec!["CF.RESERVE", "missing", "1073741824"]
            };
            assert!(matches!(
                run(&mut engine, &oversized_reserve),
                Resp::Error(_)
            ));
            assert_eq!(run(&mut engine, &["EXISTS", "missing"]), Resp::Integer(0));
        }
        assert_eq!(run(&mut engine, &["BF.CARD", "bf"]), Resp::Integer(1));
        assert_eq!(run(&mut engine, &["CF.COUNT", "cf", "x"]), Resp::Integer(1));
        assert_eq!(
            run(
                &mut engine,
                &["CF.INSERTNX", "cf", "CAPACITY", "1073741824", "ITEMS", "x"]
            ),
            Resp::Array(Some(vec![Resp::Integer(0)]))
        );
    }
    #[test]
    fn command_options_wrong_types_no_create_and_expiry() {
        let mut engine = Engine::new();
        assert!(matches!(
            run(&mut engine, &["BF.INSERT", "b", "NOCREATE", "ITEMS", "x"]),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["BF.CARD", "b"]), Resp::Integer(0));
        assert_eq!(
            run(
                &mut engine,
                &["BF.RESERVE", "b", "0.01", "10", "NONSCALING"]
            ),
            ok()
        );
        assert!(matches!(
            run(
                &mut engine,
                &[
                    "BF.INSERT",
                    "other",
                    "NONSCALING",
                    "EXPANSION",
                    "2",
                    "ITEMS",
                    "x"
                ]
            ),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["BF.ADD", "b", "x"]), Resp::Boolean(true));
        assert_eq!(
            run(&mut engine, &["BF.ADD", "b", "x"]),
            Resp::Boolean(false)
        );
        assert_eq!(
            run(&mut engine, &["PEXPIRE", "b", "10000"]),
            Resp::Integer(1)
        );
        run(&mut engine, &["BF.ADD", "b", "y"]);
        assert_eq!(run(&mut engine, &["PTTL", "b"]), Resp::Integer(10000));
        run(&mut engine, &["SET", "string", "value"]);
        assert_eq!(
            run(&mut engine, &["BF.EXISTS", "string", "value"]),
            Resp::Boolean(false)
        );
        assert_eq!(
            run(&mut engine, &["CF.COUNT", "string", "value"]),
            Resp::Integer(0)
        );
        assert!(matches!(
            run(&mut engine, &["CF.ADD", "string", "value"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut engine,
                &["CF.RESERVE", "bad", "4", "BUCKETSIZE", "256"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["EXISTS", "bad"]), Resp::Integer(0));
    }
    #[test]
    fn full_insert_preserves_protocol_specific_failure_type() {
        for version in [
            crate::protocol::Version::Resp2,
            crate::protocol::Version::Resp3,
        ] {
            let mut engine = Engine::new();
            engine.set_command_version(version);
            assert_eq!(
                run(
                    &mut engine,
                    &["CF.RESERVE", "c", "2", "BUCKETSIZE", "1", "EXPANSION", "0"]
                ),
                ok()
            );
            let Resp::Array(Some(rows)) =
                run(&mut engine, &["CF.INSERT", "c", "ITEMS", "x", "x", "x"])
            else {
                panic!("insert results");
            };
            assert_eq!(
                rows.last(),
                Some(&if version == crate::protocol::Version::Resp3 {
                    Resp::Boolean(false)
                } else {
                    Resp::Integer(-1)
                })
            );
            assert_eq!(run(&mut engine, &["CF.COUNT", "c", "x"]), Resp::Integer(2));
        }
    }
}
