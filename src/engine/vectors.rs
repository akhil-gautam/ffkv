//! Independently implemented HNSW (Malkov/Yashunin, arXiv:1603.09320).
//! All normal searches traverse the graph; only VSIM TRUTH enumerates vectors.
//! Base-layer consecutive-UID edges protect connectivity during bounded pruning.
//! Command contracts: https://redis.io/docs/latest/commands/vadd/ and /vsim/.
//! Filter grammar: https://redis.io/docs/latest/develop/data-types/vector-sets/filtered-search/.
//! Public FILTER-EF 0 means unlimited; RAW/VINFO names follow current module
//! metadata (f32/int8/bin). Omitted insertion configuration inherits the set.
mod filter;
use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{CommandSpec, Engine, Resp, Value, array, bounded_replies, error};
use bytes::Bytes;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::io;

const KIND: u8 = 10;
const MAX_DIM: usize = 65536;
const MAX_LEVEL: usize = 32;
const MAX_EF: usize = 1_000_000;
const MAX_ATTRIBUTE: usize = 1024 * 1024;
const MAX_MEMBER: usize = 65536;
const MAX_MEMORY: usize = 1024 * 1024 * 1024;
const MAX_EFFECT: usize = 8 * 1024 * 1024;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler("vadd", -5, &["write", "denyoom"], (1, 1, 1), Engine::vadd),
    CommandSpec::handler("vrem", 3, &["write"], (1, 1, 1), Engine::vrem),
    CommandSpec::handler("vcard", 2, &["readonly", "fast"], (1, 1, 1), Engine::vcard),
    CommandSpec::handler("vdim", 2, &["readonly", "fast"], (1, 1, 1), Engine::vdim),
    CommandSpec::handler("vemb", -3, &["readonly", "fast"], (1, 1, 1), Engine::vemb).max(4),
    CommandSpec::handler("vinfo", 2, &["readonly", "fast"], (1, 1, 1), Engine::vinfo),
    CommandSpec::handler(
        "vgetattr",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::vgetattr,
    ),
    CommandSpec::handler(
        "vsetattr",
        4,
        &["write", "fast"],
        (1, 1, 1),
        Engine::vsetattr,
    ),
    CommandSpec::handler("vismember", 3, &["readonly"], (1, 1, 1), Engine::vismember),
    CommandSpec::handler(
        "vlinks",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::vlinks,
    )
    .max(4),
    CommandSpec::handler(
        "vrandmember",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::vrandmember,
    )
    .max(3),
    CommandSpec::handler("vrange", -4, &["readonly"], (1, 1, 1), Engine::vrange).max(5),
    CommandSpec::handler("vsim", -4, &["readonly"], (1, 1, 1), Engine::vsim),
];
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn err(message: &str) -> Resp {
    Resp::Error(format!("ERR {message}"))
}
fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
fn bounded_reply(reply: Resp, limit: usize) -> Resp {
    let mut budget = super::ReplyBudget::new(limit);
    match budget.response(&reply) {
        Ok(()) => reply,
        Err(e) => e,
    }
}
fn number(bytes: &[u8]) -> Option<f32> {
    std::str::from_utf8(bytes)
        .ok()?
        .parse::<f32>()
        .ok()
        .filter(|v| v.is_finite())
}
fn usize_arg(bytes: &[u8], min: usize, max: usize) -> Result<usize, Resp> {
    let value = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|n| (min..=max).contains(n));
    value.ok_or_else(|| err("invalid or out-of-range integer"))
}
fn keyword(arg: &Bytes, value: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(value)
}
fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn random(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9e3779b97f4a7c15);
    mix(*state)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Quant {
    Float,
    Q8,
    Bin,
}
impl Quant {
    fn id(self) -> u8 {
        match self {
            Self::Float => 0,
            Self::Q8 => 1,
            Self::Bin => 2,
        }
    }
    fn from_id(id: u8) -> io::Result<Self> {
        match id {
            0 => Ok(Self::Float),
            1 => Ok(Self::Q8),
            2 => Ok(Self::Bin),
            _ => Err(invalid("invalid quantization")),
        }
    }
    fn name(self) -> &'static str {
        match self {
            Self::Float => "f32",
            Self::Q8 => "int8",
            Self::Bin => "bin",
        }
    }
    fn bytes(self, dim: usize) -> usize {
        match self {
            Self::Float => dim * 4,
            Self::Q8 => dim,
            Self::Bin => dim.div_ceil(64) * 8,
        }
    }
}
#[derive(Debug)]
struct Embedding {
    data: Bytes,
    norm: f64,
    scale: f64,
}
impl Embedding {
    fn encode(vector: &[f32], quant: Quant) -> Self {
        let norm = vector
            .iter()
            .map(|v| (*v as f64).powi(2))
            .sum::<f64>()
            .sqrt();
        let divisor = if norm == 0.0 { 1.0 } else { norm };
        let range = vector
            .iter()
            .map(|v| (*v as f64 / divisor).abs())
            .fold(0.0, f64::max);
        let scale = if quant == Quant::Q8 {
            range / 127.0
        } else {
            1.0
        };
        let mut data = vec![0u8; quant.bytes(vector.len())];
        for (i, v) in vector.iter().enumerate() {
            let x = *v as f64 / divisor;
            match quant {
                Quant::Float => data[i * 4..i * 4 + 4].copy_from_slice(&(x as f32).to_le_bytes()),
                Quant::Q8 => {
                    data[i] = if scale == 0.0 {
                        0
                    } else {
                        (x / scale).round().clamp(-127.0, 127.0) as i8 as u8
                    }
                }
                Quant::Bin => {
                    if *v > 0.0 {
                        data[i / 8] |= 1 << (i % 8);
                    }
                }
            }
        }
        Self {
            data: Bytes::from(data),
            norm,
            scale,
        }
    }
    fn component(&self, i: usize, quant: Quant) -> f64 {
        match quant {
            Quant::Float => {
                f32::from_le_bytes(self.data[i * 4..i * 4 + 4].try_into().unwrap()) as f64
            }
            Quant::Q8 => self.data[i] as i8 as f64 * self.scale,
            Quant::Bin => {
                if self.data[i / 8] & (1 << (i % 8)) != 0 {
                    1.0
                } else {
                    -1.0
                }
            }
        }
    }
    fn distance(&self, other: &Self, quant: Quant, dim: usize) -> f64 {
        if quant == Quant::Bin {
            let count: u32 = self
                .data
                .iter()
                .zip(other.data.iter())
                .map(|(a, b)| (a ^ b).count_ones())
                .sum();
            2.0 * count as f64 / dim as f64
        } else {
            let dot: f64 = (0..dim)
                .map(|i| self.component(i, quant) * other.component(i, quant))
                .sum();
            (1.0 - dot).clamp(0.0, 2.0)
        }
    }
}
#[derive(Debug)]
struct Node {
    member: Bytes,
    embedding: Embedding,
    attributes: Option<Bytes>,
    links: Vec<Vec<u64>>,
}
impl Node {
    fn memory(&self) -> usize {
        128 + self.member.len()
            + self.embedding.data.len()
            + self.attributes.as_ref().map_or(0, Bytes::len)
            + self.links.iter().map(|l| 24 + 8 * l.len()).sum::<usize>()
    }
}
#[derive(Debug)]
pub(super) struct VectorValue {
    dim: usize,
    input_dim: usize,
    quant: Quant,
    m: usize,
    seed: u64,
    uid: u64,
    rng: u64,
    next_id: u64,
    revision: u64,
    entry: u64,
    nodes: BTreeMap<u64, Node>,
    members: BTreeMap<Bytes, u64>,
    slots: Vec<u64>,
    positions: HashMap<u64, usize>,
    levels: Vec<BTreeSet<u64>>,
    memory: usize,
    attributes_count: usize,
}
#[derive(Clone, Copy, Debug)]
struct Hit {
    distance: f64,
    id: u64,
}
impl PartialEq for Hit {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.distance.to_bits() == other.distance.to_bits()
    }
}
impl Eq for Hit {}
impl PartialOrd for Hit {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Hit {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then(self.id.cmp(&other.id))
    }
}
#[derive(Debug)]
struct Change {
    deleted: u64,
    node: Option<(u64, Node)>,
    patches: BTreeMap<(u64, usize), Vec<u64>>,
    entry: u64,
    rng: u64,
    next_id: u64,
    revision: u64,
}
impl VectorValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{allocation, hash_bytes, sampled, tree_bytes};
        tree_bytes::<(u64, Node)>(self.nodes.len())
            .saturating_add(tree_bytes::<(Bytes, u64)>(self.members.len()))
            .saturating_add(allocation::<u64>(self.slots.capacity()))
            .saturating_add(hash_bytes::<u64, usize>(self.positions.capacity()))
            .saturating_add(allocation::<BTreeSet<u64>>(self.levels.capacity()))
            .saturating_add(self.levels.iter().fold(0usize, |sum, level| {
                sum.saturating_add(tree_bytes::<u64>(level.len()))
            }))
            .saturating_add(sampled(self.nodes.values(), samples, |node| {
                node.member
                    .len()
                    .saturating_add(node.embedding.data.len())
                    .saturating_add(node.attributes.as_ref().map_or(0, Bytes::len))
                    .saturating_add(allocation::<Vec<u64>>(node.links.capacity()))
                    .saturating_add(node.links.iter().fold(0usize, |sum, links| {
                        sum.saturating_add(allocation::<u64>(links.capacity()))
                    }))
            }))
    }

    fn new(dim: usize, input_dim: usize, quant: Quant, m: usize, seed: u64) -> Self {
        Self {
            dim,
            input_dim,
            quant,
            m,
            seed,
            uid: seed & i64::MAX as u64,
            rng: seed,
            next_id: 1,
            revision: 0,
            entry: 0,
            nodes: BTreeMap::new(),
            members: BTreeMap::new(),
            slots: Vec::new(),
            positions: HashMap::new(),
            levels: (0..=MAX_LEVEL).map(|_| BTreeSet::new()).collect(),
            memory: 0,
            attributes_count: 0,
        }
    }
    fn prepare(&self, vector: &[f32]) -> Result<Embedding, Resp> {
        if vector.len() != self.input_dim {
            return Err(err("vector dimension mismatch"));
        }
        if self.dim == self.input_dim {
            return Ok(Embedding::encode(vector, self.quant));
        }
        // An implicit seeded Rademacher matrix is persisted losslessly by its seed.
        // Computing rows on demand avoids allocating input_dim*dim matrix entries.
        let factor = 1.0 / (self.dim as f64).sqrt();
        let mut projected = vec![0f32; self.dim];
        for (row, result) in projected.iter_mut().enumerate() {
            let sum: f64 = vector
                .iter()
                .enumerate()
                .map(|(column, v)| {
                    let sign = if mix(self.seed
                        ^ (row as u64).wrapping_mul(0x9e3779b97f4a7c15)
                        ^ (column as u64).wrapping_mul(0xd6e8feb86659fd93))
                        & 1
                        == 0
                    {
                        1.0
                    } else {
                        -1.0
                    };
                    *v as f64 * sign
                })
                .sum();
            *result = (sum * factor) as f32;
            if !result.is_finite() {
                return Err(err("projection overflow"));
            }
        }
        Ok(Embedding::encode(&projected, self.quant))
    }
    fn distance(&self, query: &Embedding, id: u64) -> f64 {
        query.distance(&self.nodes[&id].embedding, self.quant, self.dim)
    }
    fn greedy(&self, query: &Embedding, mut id: u64, layer: usize) -> u64 {
        let mut distance = self.distance(query, id);
        loop {
            let mut next = id;
            for neighbor in &self.nodes[&id].links[layer] {
                let candidate = self.distance(query, *neighbor);
                if candidate < distance {
                    distance = candidate;
                    next = *neighbor;
                }
            }
            if next == id {
                return id;
            }
            id = next;
        }
    }
    fn layer_search(
        &self,
        query: &Embedding,
        start: u64,
        layer: usize,
        ef: usize,
        exclude: u64,
    ) -> Vec<Hit> {
        let mut frontier = BinaryHeap::new();
        let mut best = BinaryHeap::new();
        let mut visited = HashSet::new();
        let first = Hit {
            distance: self.distance(query, start),
            id: start,
        };
        frontier.push(Reverse(first));
        visited.insert(start);
        if start != exclude {
            best.push(first);
        }
        while let Some(Reverse(hit)) = frontier.pop() {
            if best.len() >= ef
                && best
                    .peek()
                    .is_some_and(|worst: &Hit| hit.distance > worst.distance)
            {
                break;
            }
            for id in &self.nodes[&hit.id].links[layer] {
                if !visited.insert(*id) {
                    continue;
                }
                let candidate = Hit {
                    distance: self.distance(query, *id),
                    id: *id,
                };
                if best.len() < ef
                    || best.peek().is_some_and(|worst| candidate < *worst)
                    || *id == exclude
                {
                    frontier.push(Reverse(candidate));
                    if *id != exclude {
                        best.push(candidate);
                        if best.len() > ef {
                            best.pop();
                        }
                    }
                }
            }
        }
        let mut output = best.into_vec();
        output.sort_unstable();
        output
    }
    fn candidates(
        &self,
        query: &Embedding,
        level: usize,
        ef: usize,
        exclude: u64,
    ) -> Vec<Vec<u64>> {
        let mut result = vec![Vec::new(); level + 1];
        if self.entry == 0 {
            return result;
        }
        let mut entry = self.entry;
        let top = self.nodes[&entry].links.len() - 1;
        for layer in ((level + 1)..=top).rev() {
            entry = self.greedy(query, entry, layer);
        }
        for layer in (0..=level.min(top)).rev() {
            let found = self.layer_search(query, entry, layer, ef, exclude);
            if let Some(first) = found.first() {
                entry = first.id;
            }
            result[layer] = found.into_iter().map(|h| h.id).collect();
        }
        result
    }
    fn search(&self, query: &Embedding, options: &SearchOptions) -> Vec<Hit> {
        if self.entry == 0 {
            return Vec::new();
        }
        let matches = |id: u64| {
            options
                .filter
                .as_ref()
                .is_none_or(|e| e.matches(self.nodes[&id].attributes.as_deref()))
        };
        if options.truth {
            let mut best = BinaryHeap::new();
            for id in self.nodes.keys().copied().filter(|id| matches(*id)) {
                let hit = Hit {
                    distance: self.distance(query, id),
                    id,
                };
                if hit.distance / 2.0 <= options.epsilon
                    && (best.len() < options.count
                        || best.peek().is_some_and(|worst: &Hit| hit < *worst))
                {
                    best.push(hit);
                    if best.len() > options.count {
                        best.pop();
                    }
                }
            }
            let mut hits = best.into_vec();
            hits.sort_unstable();
            return hits;
        }
        let mut entry = self.entry;
        for layer in (1..self.nodes[&entry].links.len()).rev() {
            entry = self.greedy(query, entry, layer);
        }
        let ef = options.ef.max(options.count);
        let filtering = options.filter.is_some();
        let effort = if filtering {
            options
                .filter_ef
                .unwrap_or_else(|| options.count.saturating_mul(100))
        } else {
            usize::MAX
        };
        let limit = if effort == 0 { usize::MAX } else { effort };
        let mut frontier = BinaryHeap::new();
        let mut best: BinaryHeap<Hit> = BinaryHeap::new();
        let mut visited = HashSet::new();
        let mut inspected = 0;
        frontier.push(Reverse(Hit {
            distance: self.distance(query, entry),
            id: entry,
        }));
        visited.insert(entry);
        while let Some(Reverse(hit)) = frontier.pop() {
            if inspected >= limit {
                break;
            }
            if best.len() >= ef
                && best
                    .peek()
                    .is_some_and(|worst| hit.distance > worst.distance)
            {
                break;
            }
            inspected += 1;
            if matches(hit.id) {
                best.push(hit);
                if best.len() > ef {
                    best.pop();
                }
            }
            for id in &self.nodes[&hit.id].links[0] {
                if visited.insert(*id) {
                    let next = Hit {
                        distance: self.distance(query, *id),
                        id: *id,
                    };
                    if best.len() < ef || best.peek().is_some_and(|worst| next <= *worst) {
                        frontier.push(Reverse(next));
                    }
                }
            }
        }
        let mut hits: Vec<_> = best
            .into_iter()
            .filter(|hit| hit.distance / 2.0 <= options.epsilon)
            .collect();
        hits.sort_unstable();
        hits.truncate(options.count);
        hits
    }
    fn node_for<'a>(&'a self, change: &'a Change, id: u64) -> &'a Node {
        if let Some((new_id, node)) = &change.node
            && *new_id == id
        {
            return node;
        }
        &self.nodes[&id]
    }
    fn links_for(&self, change: &Change, id: u64, layer: usize) -> Vec<u64> {
        change
            .patches
            .get(&(id, layer))
            .cloned()
            .unwrap_or_else(|| self.node_for(change, id).links[layer].clone())
    }
    fn edge(&self, change: &mut Change, a: u64, b: u64, layer: usize, add: bool) {
        if a == b || a == change.deleted || b == change.deleted {
            return;
        }
        for (id, other) in [(a, b), (b, a)] {
            let mut links = self.links_for(change, id, layer);
            if add {
                if !links.contains(&other) {
                    links.push(other);
                }
            } else {
                links.retain(|n| *n != other);
            }
            links.sort_unstable();
            change.patches.insert((id, layer), links);
        }
    }
    fn protected(&self, change: &Change, a: u64, b: u64, layer: usize) -> bool {
        if layer != 0 {
            return false;
        }
        let (low, high) = if a < b { (a, b) } else { (b, a) };
        let between = self
            .nodes
            .range((
                std::ops::Bound::Excluded(low),
                std::ops::Bound::Excluded(high),
            ))
            .any(|(id, _)| *id != change.deleted);
        !between
            && change
                .node
                .as_ref()
                .is_none_or(|(id, _)| *id <= low || *id >= high)
    }
    fn prune(&self, change: &mut Change, id: u64, layer: usize) {
        let cap = if layer == 0 { 2 * self.m } else { self.m };
        loop {
            let links = self.links_for(change, id, layer);
            if links.len() <= cap {
                break;
            }
            let vector = &self.node_for(change, id).embedding;
            let remove = links
                .into_iter()
                .filter(|other| !self.protected(change, id, *other, layer))
                .max_by(|a, b| {
                    vector
                        .distance(&self.node_for(change, *a).embedding, self.quant, self.dim)
                        .total_cmp(&vector.distance(
                            &self.node_for(change, *b).embedding,
                            self.quant,
                            self.dim,
                        ))
                        .then(a.cmp(b))
                })
                .expect("M>=4 leaves room for protected chain");
            self.edge(change, id, remove, layer, false);
        }
    }
    fn select(
        &self,
        change: &Change,
        query: &Embedding,
        candidates: &[u64],
        limit: usize,
    ) -> Vec<u64> {
        let mut selected = Vec::new();
        let mut unused = Vec::new();
        for id in candidates
            .iter()
            .copied()
            .filter(|id| *id != change.deleted)
        {
            let distance =
                query.distance(&self.node_for(change, id).embedding, self.quant, self.dim);
            if selected.iter().all(|other| {
                self.node_for(change, id).embedding.distance(
                    &self.node_for(change, *other).embedding,
                    self.quant,
                    self.dim,
                ) >= distance
            }) {
                selected.push(id);
            } else {
                unused.push(id);
            }
            if selected.len() == limit {
                return selected;
            }
        }
        for id in unused {
            if selected.len() == limit {
                break;
            }
            selected.push(id);
        }
        selected
    }
    fn plan_remove(&self, id: u64) -> Change {
        let mut change = Change {
            deleted: id,
            node: None,
            patches: BTreeMap::new(),
            entry: self.entry,
            rng: self.rng,
            next_id: self.next_id,
            revision: self.revision + 1,
        };
        let node = &self.nodes[&id];
        for (layer, neighbors) in node.links.iter().enumerate() {
            for neighbor in neighbors {
                let mut links = self.nodes[neighbor].links[layer].clone();
                links.retain(|n| *n != id);
                change.patches.insert((*neighbor, layer), links);
            }
            // Repair the local hole using actual former neighbors, not a dataset scan.
            for neighbor in neighbors {
                let query = &self.nodes[neighbor].embedding;
                let mut candidates: Vec<_> = neighbors
                    .iter()
                    .copied()
                    .filter(|n| n != neighbor)
                    .collect();
                candidates.sort_unstable_by(|a, b| {
                    self.distance(query, *a)
                        .total_cmp(&self.distance(query, *b))
                        .then(a.cmp(b))
                });
                for other in self.select(&change, query, &candidates, self.m) {
                    self.edge(&mut change, *neighbor, other, layer, true);
                }
            }
        }
        let previous = self.nodes.range(..id).next_back().map(|(id, _)| *id);
        let next = self
            .nodes
            .range((std::ops::Bound::Excluded(id), std::ops::Bound::Unbounded))
            .next()
            .map(|(id, _)| *id);
        if let (Some(a), Some(b)) = (previous, next) {
            self.edge(&mut change, a, b, 0, true);
        }
        let touched: Vec<_> = change.patches.keys().copied().collect();
        for (id, layer) in touched {
            self.prune(&mut change, id, layer);
        }
        if self.entry == id {
            change.entry = self
                .levels
                .iter()
                .rev()
                .find_map(|ids| ids.iter().copied().find(|candidate| *candidate != id))
                .unwrap_or(0);
        }
        change
    }
    fn plan_insert(
        &self,
        member: Bytes,
        embedding: Embedding,
        attributes: Option<Option<Bytes>>,
        ef: usize,
        cas: bool,
    ) -> Result<Change, Resp> {
        if self.next_id == u64::MAX || self.revision == u64::MAX {
            return Err(err("vector identifier exhausted"));
        }
        let old = self.members.get(&member).copied().unwrap_or(0);
        let mut rng = self.rng;
        let mut level = 0;
        while level < MAX_LEVEL && random(&mut rng).is_multiple_of(self.m as u64) {
            level += 1;
        }
        let collect = || self.candidates(&embedding, level, ef, old);
        let candidates = if cas {
            std::thread::scope(|scope| scope.spawn(collect).join())
                .map_err(|_| err("vector background worker failed"))?
        } else {
            collect()
        };
        let mut change = if old == 0 {
            Change {
                deleted: 0,
                node: None,
                patches: BTreeMap::new(),
                entry: self.entry,
                rng,
                next_id: self.next_id + 1,
                revision: self.revision + 1,
            }
        } else {
            self.plan_remove(old)
        };
        let attrs = attributes.unwrap_or_else(|| {
            if old == 0 {
                None
            } else {
                self.nodes[&old].attributes.clone()
            }
        });
        let id = self.next_id;
        let node = Node {
            member,
            embedding,
            attributes: attrs,
            links: vec![Vec::new(); level + 1],
        };
        change.node = Some((id, node));
        change.rng = rng;
        change.next_id = id + 1;
        for layer in (0..=level).rev() {
            let query = &self.node_for(&change, id).embedding;
            for other in self.select(&change, query, &candidates[layer], self.m) {
                self.edge(&mut change, id, other, layer, true);
            }
        }
        if let Some(previous) = self.nodes.keys().rev().copied().find(|n| *n != old) {
            self.edge(&mut change, id, previous, 0, true);
        }
        let touched: Vec<_> = change.patches.keys().copied().collect();
        for (id, layer) in touched {
            self.prune(&mut change, id, layer);
        }
        if change.entry == 0 || level > self.node_for(&change, change.entry).links.len() - 1 {
            change.entry = id;
        }
        self.change_memory(&change)
            .map_err(|e| err(&e.to_string()))?;
        Ok(change)
    }
    fn init_effect(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 0);
        e.u32(self.dim as u32)?;
        e.u32(self.input_dim as u32)?;
        e.u8(self.quant.id())?;
        e.u32(self.m as u32)?;
        e.u64(self.seed)?;
        e.u64(self.uid)?;
        Ok(e.finish())
    }
    fn metadata(&self) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 5);
        e.u64(self.entry)?;
        e.u64(self.rng)?;
        e.u64(self.next_id)?;
        e.u64(self.revision)?;
        Ok(e.finish())
    }
    fn encode_node(e: &mut Encoder, id: u64, node: &Node) -> io::Result<()> {
        e.u64(id)?;
        e.bytes(&node.member)?;
        e.bytes(&node.embedding.data)?;
        e.f64(node.embedding.norm)?;
        e.f64(node.embedding.scale)?;
        e.u8(node.links.len() as u8)?;
        e.u8(u8::from(node.attributes.is_some()))?;
        if let Some(attrs) = &node.attributes {
            e.bytes(attrs)?;
        }
        Ok(())
    }
    fn decode_node(&self, d: &mut Decoder<'_>) -> io::Result<(u64, Node)> {
        let id = d.u64()?;
        let member = d.bytes()?;
        if id == 0 || member.len() > MAX_MEMBER {
            return Err(invalid("invalid vector node"));
        }
        let raw = d.bytes()?;
        if raw.len() != self.quant.bytes(self.dim) {
            return Err(invalid("invalid vector encoding"));
        }
        let norm = d.f64()?;
        let scale = d.f64()?;
        if !norm.is_finite() || norm < 0.0 || !scale.is_finite() || scale < 0.0 {
            return Err(invalid("invalid vector normalization"));
        }
        if self.quant == Quant::Float
            && raw
                .as_chunks::<4>()
                .0
                .iter()
                .any(|v| !f32::from_le_bytes(*v).is_finite())
        {
            return Err(invalid("nonfinite embedding"));
        }
        if self.quant == Quant::Bin
            && (self.dim..raw.len() * 8).any(|i| raw[i / 8] & (1 << (i % 8)) != 0)
        {
            return Err(invalid("invalid binary padding"));
        }
        let levels = d.u8()? as usize;
        if levels == 0 || levels > MAX_LEVEL + 1 {
            return Err(invalid("invalid vector layers"));
        }
        let attrs = match d.u8()? {
            0 => None,
            1 => {
                let b = d.bytes()?;
                if b.len() > MAX_ATTRIBUTE || b.is_empty() {
                    return Err(invalid("invalid attribute length"));
                }
                Some(Bytes::copy_from_slice(b))
            }
            _ => return Err(invalid("invalid attributes")),
        };
        Ok((
            id,
            Node {
                member: Bytes::copy_from_slice(member),
                embedding: Embedding {
                    data: Bytes::copy_from_slice(raw),
                    norm,
                    scale,
                },
                attributes: attrs,
                links: vec![Vec::new(); levels],
            },
        ))
    }
    fn encode_change(&self, change: &Change) -> io::Result<Bytes> {
        let mut e = Encoder::new(1, 1);
        e.u64(change.deleted)?;
        e.u8(u8::from(change.node.is_some()))?;
        if let Some((id, node)) = &change.node {
            Self::encode_node(&mut e, *id, node)?;
        }
        e.u32(change.patches.len() as u32)?;
        for ((id, layer), links) in &change.patches {
            e.u64(*id)?;
            e.u8(*layer as u8)?;
            e.u32(links.len() as u32)?;
            for other in links {
                e.u64(*other)?;
            }
        }
        e.u64(change.entry)?;
        e.u64(change.rng)?;
        e.u64(change.next_id)?;
        e.u64(change.revision)?;
        Ok(e.finish())
    }
    fn put_node(&mut self, id: u64, node: Node) {
        self.levels[node.links.len() - 1].insert(id);
        self.memory += node.memory();
        self.attributes_count += usize::from(node.attributes.is_some());
        self.members.insert(node.member.clone(), id);
        self.positions.insert(id, self.slots.len());
        self.slots.push(id);
        self.nodes.insert(id, node);
    }
    fn remove_node(&mut self, id: u64) {
        if let Some(node) = self.nodes.remove(&id) {
            self.levels[node.links.len() - 1].remove(&id);
            self.memory -= node.memory();
            self.attributes_count -= usize::from(node.attributes.is_some());
            self.members.remove(&node.member);
            let index = self.positions.remove(&id).unwrap();
            self.slots.swap_remove(index);
            if index < self.slots.len() {
                self.positions.insert(self.slots[index], index);
            }
        }
    }
    fn change_memory(&self, change: &Change) -> io::Result<usize> {
        let mut memory = self.memory;
        if change.deleted != 0 {
            memory = memory
                .checked_sub(self.nodes[&change.deleted].memory())
                .ok_or_else(|| invalid("vector memory accounting"))?;
        }
        if let Some((_, node)) = &change.node {
            memory = memory
                .checked_add(node.memory())
                .ok_or_else(|| invalid("vector memory overflow"))?;
        }
        for ((id, layer), links) in &change.patches {
            let old = self.node_for(change, *id).links[*layer].len() * 8;
            memory = memory
                .checked_sub(old)
                .and_then(|n| n.checked_add(links.len() * 8))
                .ok_or_else(|| invalid("vector graph memory overflow"))?;
        }
        if memory > MAX_MEMORY {
            return Err(invalid("vector memory limit exceeded"));
        }
        Ok(memory)
    }
    fn validate_backbone(&self, change: &Change) -> io::Result<()> {
        let mut targets: BTreeSet<_> = change
            .patches
            .keys()
            .filter(|(_, layer)| *layer == 0)
            .map(|(id, _)| *id)
            .collect();
        if let Some((id, _)) = &change.node {
            targets.insert(*id);
        }
        for id in targets {
            let mut previous = self
                .nodes
                .range(..id)
                .rev()
                .find(|(other, _)| **other != change.deleted)
                .map(|(id, _)| *id);
            let mut next = self
                .nodes
                .range((std::ops::Bound::Excluded(id), std::ops::Bound::Unbounded))
                .find(|(other, _)| **other != change.deleted)
                .map(|(id, _)| *id);
            if let Some((new, _)) = &change.node {
                if *new < id && previous.is_none_or(|p| p < *new) {
                    previous = Some(*new);
                }
                if *new > id && next.is_none_or(|n| n > *new) {
                    next = Some(*new);
                }
            }
            let links = change
                .patches
                .get(&(id, 0))
                .unwrap_or(&self.node_for(change, id).links[0]);
            if previous
                .into_iter()
                .chain(next)
                .any(|other| !links.contains(&other))
            {
                return Err(invalid("disconnected graph backbone"));
            }
        }
        Ok(())
    }
    fn apply_change(&mut self, change: Change) {
        if change.deleted != 0 {
            self.remove_node(change.deleted);
        }
        if let Some((id, node)) = change.node {
            self.put_node(id, node);
        }
        for ((id, layer), links) in change.patches {
            let node = self.nodes.get_mut(&id).unwrap();
            self.memory = self.memory - node.links[layer].len() * 8 + links.len() * 8;
            node.links[layer] = links;
        }
        self.entry = change.entry;
        self.rng = change.rng;
        self.next_id = change.next_id;
        self.revision = change.revision;
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(effect, 1)?;
        if op != 0 {
            return Err(invalid("vector initialization expected"));
        }
        let dim = d.u32()? as usize;
        let input = d.u32()? as usize;
        let quant = Quant::from_id(d.u8()?)?;
        let m = d.u32()? as usize;
        let seed = d.u64()?;
        let uid = d.u64()?;
        d.finish()?;
        if !(1..=MAX_DIM).contains(&dim)
            || !(dim..=MAX_DIM).contains(&input)
            || !(4..=4096).contains(&m)
        {
            return Err(invalid("invalid vector configuration"));
        }
        let mut value = Self::new(dim, input, quant, m, seed);
        value.uid = uid;
        Ok(value)
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        if effect.len() > MAX_EFFECT {
            return Err(invalid("vector effect too large"));
        }
        let (op, mut d) = Decoder::new(effect, 1)?;
        match op {
            1 => {
                let deleted = d.u64()?;
                if deleted != 0 && !self.nodes.contains_key(&deleted) {
                    return Err(invalid("unknown deleted node"));
                }
                let node = match d.u8()? {
                    0 => None,
                    1 => Some(self.decode_node(&mut d)?),
                    _ => return Err(invalid("invalid vector node flag")),
                };
                if let Some((id, node)) = &node
                    && (*id != self.next_id
                        || self.nodes.contains_key(id)
                        || self
                            .members
                            .get(&node.member)
                            .is_some_and(|old| *old != deleted))
                {
                    return Err(invalid("duplicate or out-of-sequence vector node"));
                }
                let count = d.u32()? as usize;
                if count > effect.len() / 13 {
                    return Err(invalid("invalid graph patch count"));
                }
                let mut patches = BTreeMap::new();
                for _ in 0..count {
                    let id = d.u64()?;
                    let layer = d.u8()? as usize;
                    let n = d.u32()? as usize;
                    let target = if node.as_ref().is_some_and(|(new, _)| *new == id) {
                        &node.as_ref().unwrap().1
                    } else {
                        self.nodes
                            .get(&id)
                            .ok_or_else(|| invalid("unknown graph target"))?
                    };
                    if id == deleted
                        || layer >= target.links.len()
                        || n > if layer == 0 { 2 * self.m } else { self.m }
                    {
                        return Err(invalid("invalid graph layer"));
                    }
                    let mut links = Vec::with_capacity(n);
                    let mut last = 0;
                    for _ in 0..n {
                        let other = d.u64()?;
                        let neighbor = if node.as_ref().is_some_and(|(new, _)| *new == other) {
                            &node.as_ref().unwrap().1
                        } else {
                            self.nodes
                                .get(&other)
                                .ok_or_else(|| invalid("unknown graph neighbor"))?
                        };
                        if other == id
                            || other == deleted
                            || other <= last
                            || layer >= neighbor.links.len()
                        {
                            return Err(invalid("invalid graph edge"));
                        }
                        last = other;
                        links.push(other);
                    }
                    if patches.insert((id, layer), links).is_some() {
                        return Err(invalid("duplicate graph patch"));
                    }
                }
                let entry = d.u64()?;
                let rng = d.u64()?;
                let next_id = d.u64()?;
                let revision = d.u64()?;
                d.finish()?;
                let change = Change {
                    deleted,
                    node,
                    patches,
                    entry,
                    rng,
                    next_id,
                    revision,
                };
                let size = self.nodes.len() - usize::from(deleted != 0)
                    + usize::from(change.node.is_some());
                if (size == 0) != (entry == 0)
                    || entry == deleted && deleted != 0
                    || entry != 0
                        && !self.nodes.contains_key(&entry)
                        && !change.node.as_ref().is_some_and(|(id, _)| *id == entry)
                    || self.next_id.checked_add(u64::from(change.node.is_some())) != Some(next_id)
                    || self.revision.checked_add(1) != Some(revision)
                {
                    return Err(invalid("invalid graph metadata"));
                }
                for ((id, layer), links) in &change.patches {
                    let original = &self.node_for(&change, *id).links[*layer];
                    for other in links
                        .iter()
                        .chain(original.iter())
                        .filter(|other| **other != deleted)
                    {
                        let reverse = change
                            .patches
                            .get(&(*other, *layer))
                            .unwrap_or(&self.node_for(&change, *other).links[*layer]);
                        if links.contains(other) != reverse.contains(id) {
                            return Err(invalid("asymmetric graph patch"));
                        }
                    }
                }
                if deleted != 0 {
                    for (layer, links) in self.nodes[&deleted].links.iter().enumerate() {
                        for other in links {
                            if !change
                                .patches
                                .get(&(*other, layer))
                                .is_some_and(|v| !v.contains(&deleted))
                            {
                                return Err(invalid("unrepaired deleted edge"));
                            }
                        }
                    }
                }
                let mut highest = self
                    .levels
                    .iter()
                    .enumerate()
                    .rev()
                    .find_map(|(level, ids)| ids.iter().any(|id| *id != deleted).then_some(level));
                self.validate_backbone(&change)?;
                if let Some((_, node)) = &change.node {
                    highest = Some(highest.unwrap_or(0).max(node.links.len() - 1));
                }
                if entry != 0 && highest != Some(self.node_for(&change, entry).links.len() - 1) {
                    return Err(invalid("entry is not on highest layer"));
                }
                self.change_memory(&change)?;
                self.apply_change(change);
                Ok(())
            }
            2 => {
                let id = d.u64()?;
                let raw = d.bytes()?;
                if raw.len() > MAX_ATTRIBUTE || !self.nodes.contains_key(&id) {
                    return Err(invalid("invalid vector attribute update"));
                }
                let attrs = (!raw.is_empty()).then(|| Bytes::copy_from_slice(raw));
                let revision = d.u64()?;
                d.finish()?;
                if self.revision.checked_add(1) != Some(revision)
                    || self.memory - self.nodes[&id].attributes.as_ref().map_or(0, Bytes::len)
                        + raw.len()
                        > MAX_MEMORY
                {
                    return Err(invalid("invalid vector revision or attribute memory"));
                }
                let node = self.nodes.get_mut(&id).unwrap();
                self.memory = self.memory - node.attributes.as_ref().map_or(0, Bytes::len)
                    + attrs.as_ref().map_or(0, Bytes::len);
                self.attributes_count = self.attributes_count
                    - usize::from(node.attributes.is_some())
                    + usize::from(attrs.is_some());
                node.attributes = attrs;
                self.revision = revision;
                Ok(())
            }
            3 => {
                let (id, node) = self.decode_node(&mut d)?;
                d.finish()?;
                if self.nodes.contains_key(&id)
                    || self.members.contains_key(&node.member)
                    || self
                        .memory
                        .checked_add(node.memory())
                        .is_none_or(|n| n > MAX_MEMORY)
                {
                    return Err(invalid("duplicate snapshot node or memory limit"));
                }
                self.put_node(id, node);
                Ok(())
            }
            4 => {
                let id = d.u64()?;
                let layer = d.u8()? as usize;
                let count = d.u32()? as usize;
                let node = self
                    .nodes
                    .get(&id)
                    .ok_or_else(|| invalid("missing snapshot node"))?;
                if layer >= node.links.len() || count > if layer == 0 { 2 * self.m } else { self.m }
                {
                    return Err(invalid("invalid snapshot links"));
                }
                let mut links = Vec::with_capacity(count);
                let mut last = 0;
                for _ in 0..count {
                    let other = d.u64()?;
                    if other <= last
                        || other == id
                        || !self
                            .nodes
                            .get(&other)
                            .is_some_and(|n| n.links.len() > layer)
                    {
                        return Err(invalid("invalid snapshot neighbor"));
                    }
                    last = other;
                    links.push(other);
                }
                d.finish()?;
                let memory = self.memory - node.links[layer].len() * 8 + links.len() * 8;
                if memory > MAX_MEMORY {
                    return Err(invalid("snapshot graph memory limit"));
                }
                let node = self.nodes.get_mut(&id).unwrap();
                self.memory = memory;
                node.links[layer] = links;
                Ok(())
            }
            5 => {
                let entry = d.u64()?;
                let rng = d.u64()?;
                let next_id = d.u64()?;
                let revision = d.u64()?;
                d.finish()?;
                if (entry == 0) != self.nodes.is_empty()
                    || entry != 0 && !self.nodes.contains_key(&entry)
                    || next_id == 0
                    || self
                        .nodes
                        .last_key_value()
                        .is_some_and(|(id, _)| next_id <= *id)
                {
                    return Err(invalid("invalid snapshot topology"));
                }
                if entry != 0
                    && self.levels.iter().rposition(|ids| !ids.is_empty())
                        != Some(self.nodes[&entry].links.len() - 1)
                {
                    return Err(invalid("invalid snapshot entry level"));
                }
                for (id, node) in &self.nodes {
                    for (layer, links) in node.links.iter().enumerate() {
                        for other in links {
                            if !self.nodes[other].links[layer].contains(id) {
                                return Err(invalid("asymmetric snapshot graph"));
                            }
                        }
                    }
                }
                for (a, b) in self.nodes.keys().zip(self.nodes.keys().skip(1)) {
                    if !self.nodes[a].links[0].contains(b) {
                        return Err(invalid("disconnected snapshot backbone"));
                    }
                }
                self.entry = entry;
                self.rng = rng;
                self.next_id = next_id;
                self.revision = revision;
                Ok(())
            }
            _ => Err(invalid("unknown vector effect")),
        }
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            value: self,
            phase: 0,
            nodes: self.nodes.iter(),
            links: self.nodes.iter(),
            current: None,
            layer: 0,
        }
    }
}
pub(super) struct Snapshot<'a> {
    value: &'a VectorValue,
    phase: u8,
    nodes: std::collections::btree_map::Iter<'a, u64, Node>,
    links: std::collections::btree_map::Iter<'a, u64, Node>,
    current: Option<(&'a u64, &'a Node)>,
    layer: usize,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.phase == 0 {
            self.phase = 1;
            return Some(self.value.init_effect());
        }
        if self.phase == 1 {
            if let Some((id, node)) = self.nodes.next() {
                return Some((|| {
                    let mut e = Encoder::new(1, 3);
                    VectorValue::encode_node(&mut e, *id, node)?;
                    Ok(e.finish())
                })());
            }
            self.phase = 2;
        }
        if self.phase == 2 {
            loop {
                if self.current.is_none() {
                    self.current = self.links.next();
                    self.layer = 0;
                }
                let Some((id, node)) = self.current else {
                    self.phase = 3;
                    break;
                };
                if self.layer == node.links.len() {
                    self.current = None;
                    continue;
                }
                let layer = self.layer;
                self.layer += 1;
                return Some((|| {
                    let mut e = Encoder::new(1, 4);
                    e.u64(*id)?;
                    e.u8(layer as u8)?;
                    e.u32(node.links[layer].len() as u32)?;
                    for other in &node.links[layer] {
                        e.u64(*other)?;
                    }
                    Ok(e.finish())
                })());
            }
        }
        if self.phase == 3 {
            self.phase = 4;
            return Some(self.value.metadata());
        }
        None
    }
}

struct SearchOptions {
    count: usize,
    ef: usize,
    epsilon: f64,
    filter: Option<filter::Expr>,
    filter_ef: Option<usize>,
    truth: bool,
    nothread: bool,
    scores: bool,
    attributes: bool,
}
impl Default for SearchOptions {
    fn default() -> Self {
        Self {
            count: 10,
            ef: 100,
            epsilon: 1.0,
            filter: None,
            filter_ef: None,
            truth: false,
            nothread: false,
            scores: false,
            attributes: false,
        }
    }
}
fn parse_vector(args: &[Bytes], position: &mut usize) -> Result<Vec<f32>, Resp> {
    let format = args
        .get(*position)
        .ok_or_else(|| err("missing vector format"))?;
    *position += 1;
    if keyword(format, b"FP32") {
        let blob = args
            .get(*position)
            .ok_or_else(|| err("missing FP32 vector"))?;
        *position += 1;
        if blob.is_empty() || blob.len() % 4 != 0 || blob.len() / 4 > MAX_DIM {
            return Err(err("invalid FP32 vector length"));
        }
        let vector: Vec<_> = blob
            .as_chunks::<4>()
            .0
            .iter()
            .map(|v| f32::from_le_bytes(*v))
            .collect();
        if vector.iter().any(|v| !v.is_finite()) {
            return Err(err("vector values must be finite"));
        }
        Ok(vector)
    } else if keyword(format, b"VALUES") {
        let count = usize_arg(
            args.get(*position)
                .ok_or_else(|| err("missing vector dimension"))?,
            1,
            MAX_DIM,
        )?;
        *position += 1;
        let end = position
            .checked_add(count)
            .filter(|end| *end <= args.len())
            .ok_or_else(|| err("not enough vector values"))?;
        let result = args[*position..end]
            .iter()
            .map(|v| number(v).ok_or_else(|| err("vector values must be finite floats")))
            .collect();
        *position = end;
        result
    } else {
        Err(err("expected FP32 or VALUES"))
    }
}

impl Engine {
    fn vector_value(&mut self, key: &Bytes, now: i64) -> Result<Option<&VectorValue>, Resp> {
        match self.value(key, now) {
            Some(Value::Typed(data)) => match data.as_ref() {
                Data::Vector(v) => Ok(Some(v)),
                _ => Err(error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value",
                )),
            },
            Some(_) => Err(error(
                "WRONGTYPE Operation against a key holding the wrong kind of value",
            )),
            None => Ok(None),
        }
    }
    fn vector_mut(&mut self, key: &Bytes) -> &mut VectorValue {
        let Some(entry) = self.entries.get_mut(key) else {
            unreachable!()
        };
        let Value::Typed(data) = &mut entry.value else {
            unreachable!()
        };
        let Data::Vector(value) = data.as_mut() else {
            unreachable!()
        };
        value
    }
    fn vadd(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut p = 2;
        let mut reduce = None;
        if args.get(p).is_some_and(|a| keyword(a, b"REDUCE")) {
            p += 1;
            reduce = Some(
                match args
                    .get(p)
                    .ok_or_else(|| err("missing REDUCE dimension"))
                    .and_then(|a| usize_arg(a, 1, MAX_DIM))
                {
                    Ok(n) => n,
                    Err(e) => return e,
                },
            );
            p += 1;
        }
        let vector = match parse_vector(args, &mut p) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let Some(member) = args.get(p) else {
            return err("missing element");
        };
        p += 1;
        if member.len() > MAX_MEMBER {
            return err("element exceeds vector-set limit");
        }
        let mut quant = None;
        let mut m = None;
        let mut ef = 200;
        let mut cas = false;
        let mut attrs = None;
        let mut seen = HashSet::new();
        while p < args.len() {
            let option = &args[p];
            p += 1;
            let tag = if keyword(option, b"CAS") {
                1
            } else if keyword(option, b"NOQUANT")
                || keyword(option, b"Q8")
                || keyword(option, b"BIN")
            {
                2
            } else if keyword(option, b"EF") {
                3
            } else if keyword(option, b"SETATTR") {
                4
            } else if keyword(option, b"M") {
                5
            } else {
                return err("syntax error");
            };
            if !seen.insert(tag) {
                return err("duplicate or conflicting VADD option");
            }
            match tag {
                1 => cas = true,
                2 => {
                    quant = Some(if keyword(option, b"NOQUANT") {
                        Quant::Float
                    } else if keyword(option, b"BIN") {
                        Quant::Bin
                    } else {
                        Quant::Q8
                    })
                }
                3 | 5 => {
                    let Some(value) = args.get(p) else {
                        return err("missing option value");
                    };
                    p += 1;
                    let parsed = if tag == 3 {
                        usize_arg(value, 1, MAX_EF)
                    } else {
                        usize_arg(value, 4, 4096)
                    };
                    match parsed {
                        Ok(n) => {
                            if tag == 3 {
                                ef = n
                            } else {
                                m = Some(n)
                            }
                        }
                        Err(e) => return e,
                    }
                }
                4 => {
                    let Some(value) = args.get(p) else {
                        return err("missing attributes");
                    };
                    p += 1;
                    if value.len() > MAX_ATTRIBUTE {
                        return err("attributes exceed vector-set limit");
                    }
                    attrs = Some((!value.is_empty()).then(|| Bytes::copy_from_slice(value)));
                }
                _ => unreachable!(),
            }
        }
        let key = &args[1];
        let present = match self.vector_value(key, now) {
            Ok(v) => v.is_some(),
            Err(e) => return e,
        };
        let mut created = None;
        if !present {
            let dim = reduce.unwrap_or(vector.len());
            if dim > vector.len() {
                return err("REDUCE dimension exceeds input dimension");
            }
            created = Some(VectorValue::new(
                dim,
                vector.len(),
                quant.unwrap_or(Quant::Q8),
                m.unwrap_or(16),
                rand::random::<u64>(),
            ));
        }
        let value = if let Some(value) = created.as_ref() {
            value
        } else {
            match self.vector_value(key, now) {
                Ok(Some(v)) => v,
                _ => unreachable!(),
            }
        };
        if vector.len() != value.input_dim
            || reduce.is_some_and(|dim| dim != value.dim)
            || quant.is_some_and(|q| q != value.quant)
            || m.is_some_and(|m| m != value.m)
        {
            return err("vector set configuration mismatch");
        }
        let added = !value.members.contains_key(member);
        let expected_revision = value.revision;
        let embedding = match value.prepare(&vector) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let change = match value.plan_insert(
            Bytes::copy_from_slice(member),
            embedding,
            attrs,
            ef,
            cas && present && added,
        ) {
            Ok(c) => c,
            Err(e) => return e,
        };
        let effect = match value.encode_change(&change) {
            Ok(e) => e,
            Err(e) => return err(&e.to_string()),
        };
        let init = if let Some(value) = created.as_ref() {
            match value.init_effect() {
                Ok(e) => Some(e),
                Err(e) => return err(&e.to_string()),
            }
        } else {
            None
        };
        if let Some(mut value) = created {
            value.apply_change(change);
            self.collection_entry(key, now, || Value::Typed(Box::new(Data::Vector(value))));
        } else {
            // CAS candidate collection happened off-thread. Publication is conditional
            // on exactly the topology revision observed by that collection.
            let value = self.vector_mut(key);
            if value.revision != expected_revision {
                return err("vector set changed during CAS");
            }
            value.apply_change(change);
        }
        if let Some(init) = init {
            self.record_typed(key, KIND, init, now);
        }
        self.record_typed(key, KIND, effect, now);
        Resp::Boolean(added)
    }
    fn vrem(&mut self, args: &[Bytes], now: i64) -> Resp {
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Boolean(false),
            Err(e) => return e,
        };
        let Some(id) = value.members.get(&args[2]).copied() else {
            return Resp::Boolean(false);
        };
        if value.revision == u64::MAX {
            return err("vector revision exhausted");
        }
        if value.nodes.len() == 1 {
            self.remove(&args[1]);
            self.record(now, || super::Mutation::Delete {
                key: args[1].clone(),
            });
            return Resp::Boolean(true);
        }
        let change = value.plan_remove(id);
        if let Err(e) = value.change_memory(&change) {
            return err(&e.to_string());
        }
        let effect = match value.encode_change(&change) {
            Ok(e) => e,
            Err(e) => return err(&e.to_string()),
        };
        self.vector_mut(&args[1]).apply_change(change);
        self.record_typed(&args[1], KIND, effect, now);
        Resp::Boolean(true)
    }
    fn vcard(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.vector_value(&args[1], now) {
            Ok(v) => Resp::Integer(v.map_or(0, |v| v.nodes.len()) as i64),
            Err(e) => e,
        }
    }
    fn vdim(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.vector_value(&args[1], now) {
            Ok(Some(v)) => Resp::Integer(v.dim as i64),
            Ok(None) => err("no such key"),
            Err(e) => e,
        }
    }
    fn vismember(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.vector_value(&args[1], now) {
            Ok(v) => Resp::Boolean(v.is_some_and(|v| v.members.contains_key(&args[2]))),
            Err(e) => e,
        }
    }
    fn vgetattr(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let reply = match self.vector_value(&args[1], now) {
            Ok(v) => Resp::Bulk(v.and_then(|v| {
                v.members
                    .get(&args[2])
                    .and_then(|id| v.nodes[id].attributes.clone())
            })),
            Err(e) => e,
        };
        bounded_reply(reply, limit)
    }
    fn vsetattr(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[3].len() > MAX_ATTRIBUTE {
            return err("attributes exceed vector-set limit");
        }
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Boolean(false),
            Err(e) => return e,
        };
        let Some(id) = value.members.get(&args[2]).copied() else {
            return Resp::Boolean(false);
        };
        let node = &value.nodes[&id];
        let attrs = (!args[3].is_empty()).then(|| Bytes::copy_from_slice(&args[3]));
        if node.attributes == attrs {
            return Resp::Boolean(true);
        }
        if value.revision == u64::MAX
            || value.memory - node.attributes.as_ref().map_or(0, Bytes::len) + args[3].len()
                > MAX_MEMORY
        {
            return err("vector memory or revision limit exceeded");
        }
        let revision = value.revision + 1;
        let effect = (|| -> io::Result<Bytes> {
            let mut e = Encoder::new(1, 2);
            e.u64(id)?;
            e.bytes(&args[3])?;
            e.u64(revision)?;
            Ok(e.finish())
        })();
        let effect = match effect {
            Ok(e) => e,
            Err(e) => return err(&e.to_string()),
        };
        if let Err(e) = self.vector_mut(&args[1]).apply(&effect) {
            return err(&e.to_string());
        }
        self.record_typed(&args[1], KIND, effect, now);
        Resp::Boolean(true)
    }
    fn vemb(&mut self, args: &[Bytes], now: i64) -> Resp {
        let raw = args.len() == 4;
        if raw && !keyword(&args[3], b"RAW") {
            return err("syntax error");
        }
        let limit = self.reply_limit;
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Bulk(None),
            Err(e) => return e,
        };
        let Some(id) = value.members.get(&args[2]) else {
            return Resp::Bulk(None);
        };
        let embedding = &value.nodes[id].embedding;
        if raw {
            let mut rows = vec![
                Resp::Simple(value.quant.name()),
                bulk(embedding.data.clone()),
                Resp::Double(embedding.norm),
            ];
            if value.quant == Quant::Q8 {
                rows.push(Resp::Double(embedding.scale * 127.0));
            }
            return bounded_replies(rows, limit);
        }
        bounded_replies(
            (0..value.dim).map(|i| {
                Resp::Double(
                    embedding.component(i, value.quant)
                        * if value.quant == Quant::Bin {
                            1.0
                        } else {
                            embedding.norm
                        },
                )
            }),
            limit,
        )
    }
    fn vinfo(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Array(None),
            Err(e) => return e,
        };
        let reply = Resp::Map(vec![
            (bulk("quant-type"), bulk(value.quant.name())),
            (bulk("hnsw-m"), Resp::Integer(value.m as i64)),
            (bulk("vector-dim"), Resp::Integer(value.dim as i64)),
            (
                bulk("projection-input-dim"),
                Resp::Integer(if value.dim == value.input_dim {
                    0
                } else {
                    value.input_dim as i64
                }),
            ),
            (bulk("size"), Resp::Integer(value.nodes.len() as i64)),
            (
                bulk("max-level"),
                Resp::Integer(if value.entry == 0 {
                    0
                } else {
                    value.nodes[&value.entry].links.len() as i64 - 1
                }),
            ),
            (
                bulk("attributes-count"),
                Resp::Integer(value.attributes_count as i64),
            ),
            (bulk("vset-uid"), Resp::Integer(value.uid as i64)),
            (
                bulk("hnsw-max-node-uid"),
                Resp::Integer((value.next_id - 1).min(i64::MAX as u64) as i64),
            ),
        ]);
        bounded_reply(reply, limit)
    }
    fn vlinks(&mut self, args: &[Bytes], now: i64) -> Resp {
        let scores = args.len() == 4;
        if scores && !keyword(&args[3], b"WITHSCORES") {
            return err("syntax error");
        }
        let limit = self.reply_limit;
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Bulk(None),
            Err(e) => return e,
        };
        let Some(id) = value.members.get(&args[2]) else {
            return Resp::Bulk(None);
        };
        let node = &value.nodes[id];
        bounded_replies(
            node.links.iter().rev().map(|links| {
                if scores {
                    if links.is_empty() {
                        return Resp::Map(Vec::new());
                    }
                    Resp::VectorResults(
                        links
                            .iter()
                            .map(|id| {
                                (
                                    value.nodes[id].member.clone(),
                                    Some(
                                        1.0 - node.embedding.distance(
                                            &value.nodes[id].embedding,
                                            value.quant,
                                            value.dim,
                                        ) / 2.0,
                                    ),
                                    None,
                                )
                            })
                            .collect(),
                    )
                } else {
                    array(
                        links
                            .iter()
                            .map(|id| bulk(value.nodes[id].member.clone()))
                            .collect(),
                    )
                }
            }),
            limit,
        )
    }
    fn vrandmember(&mut self, args: &[Bytes], now: i64) -> Resp {
        let count = match args.get(2) {
            Some(a) => match std::str::from_utf8(a)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) if n != i64::MIN => Some(n),
                _ => return err("invalid count"),
            },
            None => None,
        };
        let limit = self.reply_limit;
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return if count.is_some() {
                    array(Vec::new())
                } else {
                    Resp::Bulk(None)
                };
            }
            Err(e) => return e,
        };
        let n = value.slots.len();
        if n == 0 {
            return if count.is_some() {
                array(Vec::new())
            } else {
                Resp::Bulk(None)
            };
        }
        let mut rng = rand::random::<u64>();
        let Some(count) = count else {
            return bulk(
                value.nodes[&value.slots[(random(&mut rng) % n as u64) as usize]]
                    .member
                    .clone(),
            );
        };
        let output = if count >= 0 {
            (count as usize).min(n)
        } else {
            match usize::try_from(count.unsigned_abs()) {
                Ok(n) => n,
                Err(_) => return super::reply_limit_error(),
            }
        };
        if output > limit / std::mem::size_of::<Resp>() {
            return super::reply_limit_error();
        }
        if count < 0 {
            return bounded_replies(
                (0..output).map(|_| {
                    bulk(
                        value.nodes[&value.slots[(random(&mut rng) % n as u64) as usize]]
                            .member
                            .clone(),
                    )
                }),
                limit,
            );
        }
        if output == n {
            return bounded_replies(
                value.members.keys().map(|member| bulk(member.clone())),
                limit,
            );
        }
        // Floyd sampling chooses distinct indices in O(count), without copying the set.
        let mut chosen = HashSet::with_capacity(output);
        for j in n - output..n {
            let t = (random(&mut rng) % (j + 1) as u64) as usize;
            if !chosen.insert(t) {
                chosen.insert(j);
            }
        }
        bounded_replies(
            chosen
                .into_iter()
                .map(|i| bulk(value.nodes[&value.slots[i]].member.clone())),
            limit,
        )
    }
    fn vrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        use std::ops::Bound;
        fn bound(arg: &Bytes, start: bool) -> Result<Bound<Bytes>, Resp> {
            if (start && arg.as_ref() == b"-") || (!start && arg.as_ref() == b"+") {
                return Ok(Bound::Unbounded);
            }
            match arg.first() {
                Some(b'[') => Ok(Bound::Included(arg.slice(1..))),
                Some(b'(') => Ok(Bound::Excluded(arg.slice(1..))),
                _ => Err(err("invalid lexicographic range bound")),
            }
        }
        let start = match bound(&args[2], true) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let end = match bound(&args[3], false) {
            Ok(b) => b,
            Err(e) => return e,
        };
        let count = if let Some(a) = args.get(4) {
            match std::str::from_utf8(a)
                .ok()
                .and_then(|s| s.parse::<i64>().ok())
            {
                Some(n) if n >= 0 => n as usize,
                Some(_) => usize::MAX,
                None => return err("invalid count"),
            }
        } else {
            usize::MAX
        };
        let limit = self.reply_limit;
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return array(Vec::new()),
            Err(e) => return e,
        };
        let low = match &start {
            Bound::Included(b) | Bound::Excluded(b) => Some(b),
            _ => None,
        };
        let high = match &end {
            Bound::Included(b) | Bound::Excluded(b) => Some(b),
            _ => None,
        };
        if low.zip(high).is_some_and(|(a, b)| {
            a > b
                || a == b
                    && (matches!(start, Bound::Excluded(_)) || matches!(end, Bound::Excluded(_)))
        }) {
            return array(Vec::new());
        }
        bounded_replies(
            value
                .members
                .range((start, end))
                .take(count)
                .map(|(member, _)| bulk(member.clone())),
            limit,
        )
    }
    fn vsim(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut p = 2;
        let mut element = None;
        let vector = if args.get(p).is_some_and(|a| keyword(a, b"ELE")) {
            element = args.get(p + 1);
            if element.is_none() {
                return err("missing query element");
            }
            p += 2;
            None
        } else {
            match parse_vector(args, &mut p) {
                Ok(v) => Some(v),
                Err(e) => return e,
            }
        };
        let mut options = SearchOptions::default();
        let mut seen = HashSet::new();
        while p < args.len() {
            let arg = &args[p];
            p += 1;
            let tag = if keyword(arg, b"WITHSCORES") {
                1
            } else if keyword(arg, b"WITHATTRIBS") {
                2
            } else if keyword(arg, b"COUNT") {
                3
            } else if keyword(arg, b"EPSILON") {
                4
            } else if keyword(arg, b"EF") {
                5
            } else if keyword(arg, b"FILTER") {
                6
            } else if keyword(arg, b"FILTER-EF") {
                7
            } else if keyword(arg, b"TRUTH") {
                8
            } else if keyword(arg, b"NOTHREAD") {
                9
            } else {
                return err("syntax error");
            };
            if !seen.insert(tag) {
                return err("duplicate VSIM option");
            }
            match tag {
                1 => options.scores = true,
                2 => options.attributes = true,
                8 => options.truth = true,
                9 => options.nothread = true,
                _ => {
                    let Some(arg) = args.get(p) else {
                        return err("missing VSIM option value");
                    };
                    p += 1;
                    match tag {
                        3 => match usize_arg(arg, 1, MAX_EF) {
                            Ok(n) => options.count = n,
                            Err(e) => return e,
                        },
                        4 => match std::str::from_utf8(arg)
                            .ok()
                            .and_then(|s| s.parse::<f64>().ok())
                            .filter(|n| n.is_finite() && (0.0..=1.0).contains(n))
                        {
                            Some(n) => options.epsilon = n,
                            None => return err("EPSILON must be between zero and one"),
                        },
                        5 => match usize_arg(arg, 1, MAX_EF) {
                            Ok(n) => options.ef = n,
                            Err(e) => return e,
                        },
                        6 => match filter::compile(arg) {
                            Ok(e) => options.filter = Some(e),
                            Err(e) => return err(e),
                        },
                        7 => match usize_arg(arg, 0, MAX_EF) {
                            Ok(n) => options.filter_ef = Some(n),
                            Err(e) => return e,
                        },
                        _ => unreachable!(),
                    }
                }
            }
        }
        let limit = self.reply_limit;
        if options.count > limit / std::mem::size_of::<Resp>() {
            return super::reply_limit_error();
        }
        let value = match self.vector_value(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return if options.scores || options.attributes {
                    Resp::Map(Vec::new())
                } else {
                    array(Vec::new())
                };
            }
            Err(e) => return e,
        };
        let prepared;
        let query = if let Some(member) = element {
            let Some(id) = value.members.get(member) else {
                return err("query element does not exist");
            };
            &value.nodes[id].embedding
        } else {
            prepared = match value.prepare(vector.as_ref().unwrap()) {
                Ok(q) => q,
                Err(e) => return e,
            };
            &prepared
        };
        let hits = if options.nothread {
            value.search(query, &options)
        } else {
            match std::thread::scope(|scope| scope.spawn(|| value.search(query, &options)).join()) {
                Ok(h) => h,
                Err(_) => return err("vector search worker failed"),
            }
        };
        let reply = if hits.is_empty() {
            if options.scores || options.attributes {
                Resp::Map(Vec::new())
            } else {
                array(Vec::new())
            }
        } else {
            Resp::VectorResults(
                hits.into_iter()
                    .map(|hit| {
                        let node = &value.nodes[&hit.id];
                        (
                            node.member.clone(),
                            options.scores.then_some(1.0 - hit.distance / 2.0),
                            options.attributes.then(|| node.attributes.clone()),
                        )
                    })
                    .collect(),
            )
        };
        let mut budget = super::ReplyBudget::new(limit);
        match budget.response(&reply) {
            Ok(()) => reply,
            Err(e) => e,
        }
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
    fn insert(value: &mut VectorValue, name: &str, vector: &[f32], attrs: Option<&[u8]>) -> Bytes {
        let embedding = value.prepare(vector).unwrap();
        let change = value
            .plan_insert(
                Bytes::copy_from_slice(name.as_bytes()),
                embedding,
                attrs.map(|a| (!a.is_empty()).then(|| Bytes::copy_from_slice(a))),
                100,
                false,
            )
            .unwrap();
        let effect = value.encode_change(&change).unwrap();
        value.apply_change(change);
        effect
    }
    fn state(value: &VectorValue) -> Vec<Bytes> {
        value.snapshot().collect::<io::Result<Vec<_>>>().unwrap()
    }
    fn invariants(value: &VectorValue) {
        assert_eq!(value.nodes.len(), value.members.len());
        assert_eq!(value.nodes.len(), value.slots.len());
        assert_eq!(
            value.memory,
            value.nodes.values().map(Node::memory).sum::<usize>()
        );
        for (id, node) in &value.nodes {
            assert_eq!(value.members[&node.member], *id);
            assert_eq!(value.slots[value.positions[id]], *id);
            assert!(value.levels[node.links.len() - 1].contains(id));
            for (level, links) in node.links.iter().enumerate() {
                assert!(links.len() <= if level == 0 { 2 * value.m } else { value.m });
                let mut unique = HashSet::new();
                for neighbor in links {
                    assert!(unique.insert(*neighbor));
                    assert_ne!(id, neighbor);
                    assert!(value.nodes[neighbor].links.len() > level);
                    assert!(value.nodes[neighbor].links[level].contains(id));
                }
            }
        }
        for (a, b) in value.nodes.keys().zip(value.nodes.keys().skip(1)) {
            assert!(value.nodes[a].links[0].contains(b));
        }
        if value.nodes.is_empty() {
            assert_eq!(value.entry, 0);
            return;
        }
        assert_eq!(
            value.nodes[&value.entry].links.len() - 1,
            value
                .levels
                .iter()
                .rposition(|ids| !ids.is_empty())
                .unwrap()
        );
        let mut reachable = HashSet::new();
        let mut pending = vec![value.entry];
        while let Some(id) = pending.pop() {
            if reachable.insert(id) {
                pending.extend(value.nodes[&id].links[0].iter().copied());
            }
        }
        assert_eq!(reachable.len(), value.nodes.len());
    }
    #[test]
    fn hnsw_multilayer_recall_and_real_adjacency() {
        let mut value = VectorValue::new(12, 12, Quant::Float, 8, 42);
        let mut rng = 7;
        for i in 0..512 {
            let vector: Vec<_> = (0..12)
                .map(|_| (random(&mut rng) % 20001) as f32 / 10000.0 - 1.0)
                .collect();
            insert(&mut value, &format!("n{i:04}"), &vector, None);
        }
        invariants(&value);
        assert!(value.nodes.values().any(|n| n.links.len() > 1));
        assert!(
            value
                .nodes
                .values()
                .any(|n| n.links[0].len() < value.nodes.len() - 1)
        );
        let mut overlap = 0;
        for _ in 0..20 {
            let vector: Vec<_> = (0..12)
                .map(|_| (random(&mut rng) % 20001) as f32 / 10000.0 - 1.0)
                .collect();
            let query = value.prepare(&vector).unwrap();
            let options = SearchOptions {
                count: 10,
                ef: 100,
                ..Default::default()
            };
            let approximate = value.search(&query, &options);
            let exact = value.search(
                &query,
                &SearchOptions {
                    truth: true,
                    ..options
                },
            );
            let truth: HashSet<_> = exact.iter().map(|h| h.id).collect();
            overlap += approximate.iter().filter(|h| truth.contains(&h.id)).count();
        }
        assert!(overlap >= 170, "recall was {overlap}/200");
    }
    #[test]
    fn deletion_update_and_recovery_preserve_graph_and_resolved_randomness() {
        let mut value = VectorValue::new(6, 6, Quant::Q8, 4, 91);
        let mut recovered = VectorValue::restore(&value.init_effect().unwrap()).unwrap();
        for i in 0..100 {
            let v = [
                (i as f32).sin(),
                (i as f32).cos(),
                i as f32 / 100.0,
                0.2,
                -0.7,
                0.1,
            ];
            let effect = insert(
                &mut value,
                &format!("e{i:03}"),
                &v,
                Some(br#"{"year":2020}"#),
            );
            recovered.apply(&effect).unwrap();
        }
        for i in (0..100).step_by(3) {
            let id = value.members[format!("e{i:03}").as_bytes()];
            let change = value.plan_remove(id);
            let effect = value.encode_change(&change).unwrap();
            value.apply_change(change);
            recovered.apply(&effect).unwrap();
            invariants(&value);
        }
        let effect = insert(&mut value, "e001", &[1., 0., 0., 0., 0., 0.], None);
        recovered.apply(&effect).unwrap();
        assert!(
            value.nodes[&value.members[b"e001".as_slice()]]
                .attributes
                .is_some()
        );
        invariants(&value);
        invariants(&recovered);
        assert_eq!(state(&value), state(&recovered));
        let rows = state(&value);
        assert!(rows.iter().all(|r| r.len() < MAX_EFFECT));
        let mut snapshot = VectorValue::restore(&rows[0]).unwrap();
        for row in &rows[1..] {
            snapshot.apply(row).unwrap();
        }
        assert_eq!(state(&value), state(&snapshot));
        invariants(&snapshot);
        let a = insert(&mut value, "next", &[0., 1., 0., 0., 0., 0.], None);
        let b = insert(&mut snapshot, "next", &[0., 1., 0., 0., 0., 0.], None);
        assert_eq!(a, b);
    }
    #[test]
    fn filter_hybrid_effort_and_epsilon() {
        let mut value = VectorValue::new(2, 2, Quant::Float, 4, 17);
        for i in 0..80 {
            let angle = i as f32 * 0.03;
            insert(
                &mut value,
                &format!("p{i:03}"),
                &[angle.cos(), angle.sin()],
                Some(if i == 79 {
                    br#"{"kind":"rare","rating":9}"#
                } else {
                    br#"{"kind":"common","rating":1}"#
                }),
            );
        }
        let query = value.prepare(&[1., 0.]).unwrap();
        let options = SearchOptions {
            count: 1,
            ef: 10,
            filter: Some(filter::compile(br#".kind == "rare" and .rating >= 9"#).unwrap()),
            filter_ef: Some(0),
            ..Default::default()
        };
        let found = value.search(&query, &options);
        assert_eq!(found.len(), 1);
        assert_eq!(value.nodes[&found[0].id].member, b"p079".as_slice());
        let constrained = SearchOptions {
            filter_ef: Some(1),
            ..options
        };
        assert!(value.search(&query, &constrained).len() <= 1);
        let precise = SearchOptions {
            count: 80,
            ef: 100,
            epsilon: 0.01,
            ..Default::default()
        };
        assert!(
            value
                .search(&query, &precise)
                .iter()
                .all(|h| h.distance / 2.0 <= 0.01)
        );
        assert!(
            !filter::compile(b"not .missing")
                .unwrap()
                .matches(Some(b"{}"))
        );
    }
    #[test]
    fn binary_input_projection_quantization_and_dimension_validation() {
        let mut engine = Engine::new();
        let blob: Bytes = [3f32, 4f32, -2f32, 1f32]
            .iter()
            .flat_map(|f| f.to_le_bytes())
            .collect::<Vec<_>>()
            .into();
        let args = vec![
            Bytes::from_static(b"VADD"),
            Bytes::from_static(b"binary"),
            Bytes::from_static(b"REDUCE"),
            Bytes::from_static(b"2"),
            Bytes::from_static(b"FP32"),
            blob,
            Bytes::from_static(b"\0\xff"),
            Bytes::from_static(b"NOQUANT"),
        ];
        assert_eq!(engine.execute_at(&args, 1000), Resp::Boolean(true));
        assert_eq!(run(&mut engine, &["VDIM", "binary"]), Resp::Integer(2));
        assert!(matches!(
            run(&mut engine, &["VSIM", "binary", "VALUES", "2", "1", "2"]),
            Resp::Error(_)
        ));
        let value = engine
            .vector_value(&Bytes::from_static(b"binary"), 1000)
            .unwrap()
            .unwrap();
        let before = state(value);
        assert!(matches!(
            run(
                &mut engine,
                &["VADD", "binary", "VALUES", "4", "1", "2", "NaN", "4", "bad"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            state(
                engine
                    .vector_value(&Bytes::from_static(b"binary"), 1000)
                    .unwrap()
                    .unwrap()
            ),
            before
        );
        for quant in [Quant::Float, Quant::Q8, Quant::Bin] {
            let v = VectorValue::new(4, 4, quant, 4, 1);
            let embedding = v.prepare(&[3., -4., 0., 1.]).unwrap();
            assert_eq!(embedding.data.len(), quant.bytes(4));
            let opposite = v.prepare(&[-3., 4., 0., -1.]).unwrap();
            if quant != Quant::Bin {
                assert!(embedding.distance(&opposite, quant, 4) > 1.99);
            }
            if quant == Quant::Q8 {
                for (i, x) in [3., -4., 0., 1.].iter().enumerate() {
                    assert!((embedding.component(i, quant) * embedding.norm - x).abs() < 0.025);
                }
            }
            if quant == Quant::Bin {
                assert_eq!(embedding.data[0] & 15, 9);
                assert!(embedding.data[1..].iter().all(|b| *b == 0));
            }
        }
        let reduced = VectorValue::new(3, 8, Quant::Float, 4, 77);
        let full = [1., 2., 3., 4., 5., 6., 7., 8.];
        let a = reduced.prepare(&full).unwrap();
        let restored = VectorValue::restore(&reduced.init_effect().unwrap()).unwrap();
        let b = restored.prepare(&full).unwrap();
        assert_eq!(a.data, b.data);
        assert_eq!(a.norm, b.norm);
        assert_ne!(
            (0..3)
                .map(|i| a.component(i, Quant::Float) * a.norm)
                .collect::<Vec<_>>(),
            vec![1., 2., 3.]
        );
    }
    #[test]
    fn all_commands_attributes_ranges_random_and_ttl() {
        let mut engine = Engine::new();
        assert_eq!(
            run(
                &mut engine,
                &[
                    "VADD",
                    "v",
                    "VALUES",
                    "2",
                    "1",
                    "0",
                    "a",
                    "SETATTR",
                    r#"{"year":2020}"#
                ]
            ),
            Resp::Boolean(true)
        );
        assert_eq!(run(&mut engine, &["PEXPIRE", "v", "500"]), Resp::Integer(1));
        assert_eq!(
            run(
                &mut engine,
                &["VADD", "v", "VALUES", "2", "0", "1", "b", "CAS"]
            ),
            Resp::Boolean(true)
        );
        assert_eq!(run(&mut engine, &["PTTL", "v"]), Resp::Integer(500));
        assert_eq!(run(&mut engine, &["VCARD", "v"]), Resp::Integer(2));
        assert_eq!(
            run(&mut engine, &["VISMEMBER", "v", "a"]),
            Resp::Boolean(true)
        );
        assert_eq!(
            run(&mut engine, &["VRANGE", "v", "-", "+"]),
            array(vec![bulk("a"), bulk("b")])
        );
        assert_eq!(
            run(&mut engine, &["VRANGE", "v", "(a", "[b", "1"]),
            array(vec![bulk("b")])
        );
        assert_eq!(
            run(&mut engine, &["VSETATTR", "v", "a", "invalid-json"]),
            Resp::Boolean(true)
        );
        assert_eq!(
            run(&mut engine, &["VGETATTR", "v", "a"]),
            bulk("invalid-json")
        );
        assert_eq!(
            run(&mut engine, &["VSETATTR", "v", "a", ""]),
            Resp::Boolean(true)
        );
        assert_eq!(run(&mut engine, &["VGETATTR", "v", "a"]), Resp::Bulk(None));
        assert!(matches!(
            run(&mut engine, &["VEMB", "v", "a", "RAW"]),
            Resp::Array(Some(_))
        ));
        assert!(matches!(run(&mut engine, &["VINFO", "v"]), Resp::Map(_)));
        assert!(matches!(
            run(&mut engine, &["VLINKS", "v", "a", "WITHSCORES"]),
            Resp::Array(Some(_))
        ));
        let Resp::Array(Some(sample)) = run(&mut engine, &["VRANDMEMBER", "v", "10"]) else {
            panic!()
        };
        assert_eq!(sample.len(), 2);
        let Resp::Array(Some(sample)) = run(&mut engine, &["VRANDMEMBER", "v", "-7"]) else {
            panic!()
        };
        assert_eq!(sample.len(), 7);
        assert_eq!(run(&mut engine, &["VREM", "v", "a"]), Resp::Boolean(true));
        assert_eq!(run(&mut engine, &["VREM", "v", "b"]), Resp::Boolean(true));
        assert_eq!(run(&mut engine, &["TYPE", "v"]), Resp::Simple("none"));
    }
    #[test]
    fn malformed_effects_and_options_are_nonmutating() {
        let mut value = VectorValue::new(3, 3, Quant::Q8, 4, 3);
        insert(&mut value, "one", &[1., 2., 3.], None);
        let embedding = value.prepare(&[3., 2., 1.]).unwrap();
        let change = value
            .plan_insert(Bytes::from_static(b"two"), embedding, None, 10, false)
            .unwrap();
        let effect = value.encode_change(&change).unwrap();
        let before = state(&value);
        for length in 0..effect.len() {
            assert!(value.apply(&effect[..length]).is_err());
            assert_eq!(state(&value), before);
        }
        let mut trailing = effect.to_vec();
        trailing.push(0);
        assert!(value.apply(&trailing).is_err());
        assert_eq!(state(&value), before);
        let mut engine = Engine::new();
        run(
            &mut engine,
            &["VADD", "v", "VALUES", "3", "1", "2", "3", "a"],
        );
        let key = Bytes::from_static(b"v");
        let before = state(engine.vector_value(&key, 1000).unwrap().unwrap());
        for options in [
            vec!["VADD", "v", "VALUES", "3", "1", "2", "3", "b", "Q8", "BIN"],
            vec!["VADD", "v", "VALUES", "3", "1", "2", "3", "b", "M", "0"],
            vec!["VADD", "v", "VALUES", "3", "1", "2", "3", "b", "EF", "0"],
            vec!["VSIM", "v", "ELE", "a", "COUNT", "0"],
            vec!["VSIM", "v", "ELE", "a", "FILTER", ".x =="],
        ] {
            assert!(matches!(run(&mut engine, &options), Resp::Error(_)));
            assert_eq!(
                state(engine.vector_value(&key, 1000).unwrap().unwrap()),
                before
            );
        }
    }
}

#[cfg(test)]
mod boundary_tests {
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
    #[test]
    fn similarity_command_options_and_attribute_filtering() {
        let mut engine = Engine::new();
        for (member, x, y, attrs) in [
            ("parallel", "1", "0", r#"{"kind":"fruit","rating":8}"#),
            ("orthogonal", "0", "1", r#"{"kind":"other","rating":1}"#),
            ("opposite", "-1", "0", r#"{"kind":"fruit","rating":9}"#),
        ] {
            assert_eq!(
                run(
                    &mut engine,
                    &[
                        "VADD", "v", "VALUES", "2", x, y, member, "NOQUANT", "M", "4", "SETATTR",
                        attrs
                    ]
                ),
                Resp::Boolean(true)
            );
        }
        let reply = run(
            &mut engine,
            &[
                "VSIM",
                "v",
                "ELE",
                "parallel",
                "WITHSCORES",
                "WITHATTRIBS",
                "COUNT",
                "3",
                "EF",
                "40",
                "TRUTH",
                "NOTHREAD",
            ],
        );
        let Resp::VectorResults(rows) = reply else {
            panic!("expected vector results")
        };
        assert_eq!(
            rows.iter().map(|r| r.0.as_ref()).collect::<Vec<_>>(),
            vec![b"parallel".as_slice(), b"orthogonal", b"opposite"]
        );
        assert_eq!(
            rows.iter().map(|r| r.1.unwrap()).collect::<Vec<_>>(),
            vec![1.0, 0.5, 0.0]
        );
        assert!(rows.iter().all(|r| r.2.as_ref().unwrap().is_some()));
        let reply = run(
            &mut engine,
            &[
                "VSIM",
                "v",
                "VALUES",
                "2",
                "1",
                "0",
                "WITHATTRIBS",
                "FILTER",
                r#".kind == "fruit" and .rating > 8"#,
                "FILTER-EF",
                "0",
                "EF",
                "5",
                "COUNT",
                "2",
            ],
        );
        let Resp::VectorResults(rows) = reply else {
            panic!()
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"opposite".as_slice());
        assert!(rows[0].1.is_none());
        let reply = run(
            &mut engine,
            &[
                "VSIM",
                "v",
                "VALUES",
                "2",
                "1",
                "0",
                "WITHSCORES",
                "EPSILON",
                "0.1",
                "COUNT",
                "3",
                "NOTHREAD",
            ],
        );
        let Resp::VectorResults(rows) = reply else {
            panic!()
        };
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, b"parallel".as_slice());
    }
    #[test]
    fn little_endian_fp32_rejects_nonfinite_truncated_and_empty_blobs() {
        let mut engine = Engine::new();
        for blob in [
            Bytes::new(),
            Bytes::from_static(b"123"),
            Bytes::copy_from_slice(&f32::NAN.to_le_bytes()),
            Bytes::copy_from_slice(&f32::INFINITY.to_le_bytes()),
        ] {
            let args = vec![
                Bytes::from_static(b"VADD"),
                Bytes::from_static(b"bad"),
                Bytes::from_static(b"FP32"),
                blob,
                Bytes::from_static(b"member"),
            ];
            assert!(matches!(engine.execute_at(&args, 1000), Resp::Error(_)));
            assert_eq!(run(&mut engine, &["EXISTS", "bad"]), Resp::Integer(0));
        }
        for (key, option, kind, rawlen) in [
            ("f", "NOQUANT", "f32", 8),
            ("q", "Q8", "int8", 2),
            ("b", "BIN", "bin", 8),
        ] {
            assert_eq!(
                run(
                    &mut engine,
                    &["VADD", key, "VALUES", "2", "3", "-4", "member", option]
                ),
                Resp::Boolean(true)
            );
            let Resp::Array(Some(raw)) = run(&mut engine, &["VEMB", key, "member", "RAW"]) else {
                panic!()
            };
            assert_eq!(raw[0], Resp::Simple(kind));
            let Resp::Bulk(Some(data)) = &raw[1] else {
                panic!()
            };
            assert_eq!(data.len(), rawlen);
            assert_eq!(raw[2], Resp::Double(5.0));
            if option != "Q8" {
                assert!(matches!(
                    run(
                        &mut engine,
                        &["VADD", key, "VALUES", "2", "1", "2", "other", "Q8"]
                    ),
                    Resp::Error(_)
                ));
            }
        }
    }
    #[test]
    fn reply_bounds_do_not_change_vector_state() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "VADD",
                "v",
                "VALUES",
                "2",
                "1",
                "0",
                "a",
                "SETATTR",
                &"x".repeat(2048),
            ],
        );
        let key = Bytes::from_static(b"v");
        let revision = engine.vector_value(&key, 1000).unwrap().unwrap().revision;
        engine.set_reply_limit(128);
        assert!(matches!(
            run(&mut engine, &["VGETATTR", "v", "a"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut engine,
                &[
                    "VSIM",
                    "v",
                    "ELE",
                    "a",
                    "COUNT",
                    "1",
                    "WITHATTRIBS",
                    "NOTHREAD"
                ]
            ),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(&mut engine, &["VRANDMEMBER", "v", "-1000000"]),
            Resp::Error(_)
        ));
        assert_eq!(
            engine.vector_value(&key, 1000).unwrap().unwrap().revision,
            revision
        );
    }
}
