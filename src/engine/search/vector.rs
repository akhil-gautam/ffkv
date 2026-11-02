use super::model::{DocId, DocSet};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap};

/// Durable schema configuration; graph state is rebuilt from resolved documents.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorConfig {
    pub algorithm: String,
    pub data_type: String,
    pub dim: usize,
    pub distance_metric: String,
    pub initial_cap: usize,
    pub block_size: usize,
    pub m: usize,
    pub ef_construction: usize,
    pub ef_runtime: usize,
    pub epsilon: f64,
    pub compression: Option<String>,
    pub construction_window_size: usize,
    pub graph_max_degree: usize,
    pub search_window_size: usize,
    pub training_threshold: usize,
    pub reduce: Option<usize>,
}
impl VectorConfig {
    pub fn dims(&self) -> usize {
        self.dim
    }
    pub fn metric(&self) -> &str {
        &self.distance_metric
    }
    pub fn datatype(&self) -> &str {
        &self.data_type
    }
    pub fn element_size(&self) -> usize {
        match self.data_type.as_str() {
            "FLOAT64" => 8,
            "FLOAT32" => 4,
            "FLOAT16" | "BFLOAT16" => 2,
            _ => 1,
        }
    }
}
fn positive(value: &str, name: &str) -> Result<usize, String> {
    value
        .parse::<usize>()
        .ok()
        .filter(|v| *v > 0 && *v <= i32::MAX as usize)
        .ok_or_else(|| format!("{name} must be a positive integer"))
}
fn finite(value: &str, name: &str) -> Result<f64, String> {
    value
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
        .ok_or_else(|| format!("Invalid {name}"))
}
pub fn parse_config(algorithm: &str, args: &[Bytes]) -> Result<VectorConfig, String> {
    let algorithm = algorithm.to_ascii_uppercase();
    if !matches!(algorithm.as_str(), "FLAT" | "HNSW" | "SVS-VAMANA") {
        return Err("Unknown vector algorithm".into());
    }
    if !args.len().is_multiple_of(2) {
        return Err("Vector attributes must be name/value pairs".into());
    }
    let mut c = VectorConfig {
        algorithm,
        data_type: String::new(),
        dim: 0,
        distance_metric: String::new(),
        initial_cap: 0,
        block_size: 1024,
        m: 16,
        ef_construction: 200,
        ef_runtime: 10,
        epsilon: 0.01,
        compression: None,
        construction_window_size: 200,
        graph_max_degree: 32,
        search_window_size: 10,
        training_threshold: 10240,
        reduce: None,
    };
    let mut seen = BTreeSet::new();
    for pair in args.as_chunks::<2>().0 {
        let name = std::str::from_utf8(&pair[0])
            .map_err(|_| "Invalid vector attribute")?
            .to_ascii_uppercase();
        let value = std::str::from_utf8(&pair[1]).map_err(|_| "Invalid vector attribute value")?;
        if !seen.insert(name.clone()) {
            return Err(format!("Duplicate vector attribute {name}"));
        }
        match name.as_str() {
            "TYPE" => c.data_type = value.to_ascii_uppercase(),
            "DIM" => c.dim = positive(value, "DIM")?,
            "DISTANCE_METRIC" => c.distance_metric = value.to_ascii_uppercase(),
            "INITIAL_CAP" if c.algorithm != "SVS-VAMANA" => {
                c.initial_cap = value.parse().map_err(|_| "Invalid INITIAL_CAP")?;
                if c.initial_cap > i32::MAX as usize {
                    return Err("INITIAL_CAP exceeds capacity limit".into());
                }
            }
            // Historical Redis option: accepted and persisted, intentionally not used by current algorithms.
            "BLOCK_SIZE" if c.algorithm != "SVS-VAMANA" => {
                c.block_size = positive(value, "BLOCK_SIZE")?
            }
            "M" if c.algorithm == "HNSW" => {
                c.m = positive(value, "M")?;
                if c.m < 2 || c.m > 512 {
                    return Err("M must be between 2 and 512".into());
                }
            }
            "EF_CONSTRUCTION" if c.algorithm == "HNSW" => {
                c.ef_construction = positive(value, &name)?
            }
            "EF_RUNTIME" if c.algorithm == "HNSW" => c.ef_runtime = positive(value, &name)?,
            "EPSILON" if c.algorithm != "FLAT" => {
                c.epsilon = finite(value, &name)?;
                if c.epsilon <= 0.0 {
                    return Err("EPSILON must be positive".into());
                }
            }
            "COMPRESSION" if c.algorithm == "SVS-VAMANA" => {
                let v = value.to_ascii_uppercase();
                if !matches!(
                    v.as_str(),
                    "LVQ8" | "LVQ4" | "LVQ4X4" | "LVQ4X8" | "LEANVEC4X8" | "LEANVEC8X8"
                ) {
                    return Err("Invalid COMPRESSION".into());
                }
                c.compression = Some(v);
            }
            "CONSTRUCTION_WINDOW_SIZE" if c.algorithm == "SVS-VAMANA" => {
                c.construction_window_size = positive(value, &name)?
            }
            "GRAPH_MAX_DEGREE" if c.algorithm == "SVS-VAMANA" => {
                c.graph_max_degree = positive(value, &name)?;
                if c.graph_max_degree > 1024 {
                    return Err("GRAPH_MAX_DEGREE exceeds 1024".into());
                }
            }
            "SEARCH_WINDOW_SIZE" if c.algorithm == "SVS-VAMANA" => {
                c.search_window_size = positive(value, &name)?
            }
            "TRAINING_THRESHOLD" if c.algorithm == "SVS-VAMANA" => {
                c.training_threshold = positive(value, &name)?;
                if !(1024..102400).contains(&c.training_threshold) {
                    return Err(
                        "TRAINING_THRESHOLD must be at least 1024 and less than 102400".into(),
                    );
                }
            }
            "REDUCE" if c.algorithm == "SVS-VAMANA" => c.reduce = Some(positive(value, &name)?),
            _ => {
                return Err(format!(
                    "Invalid vector attribute {name} for {}",
                    c.algorithm
                ));
            }
        }
    }
    if c.dim == 0 || c.data_type.is_empty() || c.distance_metric.is_empty() {
        return Err("TYPE, DIM and DISTANCE_METRIC are required".into());
    }
    if !matches!(
        c.data_type.as_str(),
        "FLOAT32" | "FLOAT64" | "FLOAT16" | "BFLOAT16" | "INT8" | "UINT8"
    ) {
        return Err("Invalid vector TYPE".into());
    }
    if c.algorithm == "SVS-VAMANA" && !matches!(c.data_type.as_str(), "FLOAT16" | "FLOAT32") {
        return Err("SVS-VAMANA supports FLOAT16 and FLOAT32".into());
    }
    if !matches!(c.distance_metric.as_str(), "L2" | "IP" | "COSINE") {
        return Err("Invalid DISTANCE_METRIC".into());
    }
    if c.dim
        .checked_mul(c.element_size())
        .filter(|n| *n <= i32::MAX as usize)
        .is_none()
    {
        return Err("Vector dimensions exceed blob limit".into());
    }
    if seen.contains("TRAINING_THRESHOLD") && c.compression.is_none() {
        return Err("TRAINING_THRESHOLD requires COMPRESSION".into());
    }
    if let Some(r) = c.reduce
        && (r >= c.dim
            || !c
                .compression
                .as_deref()
                .is_some_and(|v| v.starts_with("LEANVEC")))
    {
        return Err("REDUCE requires LeanVec compression and must be less than DIM".into());
    }
    Ok(c)
}
fn half_to_f64(bits: u16) -> f64 {
    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exp = (bits >> 10) & 31;
    let mantissa = (bits & 1023) as f64;
    sign * match exp {
        0 => mantissa * 2f64.powi(-24),
        31 => {
            if mantissa == 0.0 {
                f64::INFINITY
            } else {
                f64::NAN
            }
        }
        _ => (1.0 + mantissa / 1024.0) * 2f64.powi(exp as i32 - 15),
    }
}
/// Round a JSON number to the declared storage representation (ties to even).
fn typed_value(c: &VectorConfig, v: f64) -> Result<f64, String> {
    if !v.is_finite() {
        return Err("Vector elements must be finite".into());
    }
    let n = match c.data_type.as_str() {
        "FLOAT32" => (v as f32) as f64,
        "FLOAT16" => {
            let a = v.abs();
            let step = if a < 2f64.powi(-14) {
                2f64.powi(-24)
            } else {
                2f64.powi(a.log2().floor() as i32 - 10)
            };
            let rounded = (v / step).round_ties_even() * step;
            if rounded.abs() > 65504.0 {
                return Err("Vector element outside FLOAT16 range".into());
            }
            rounded
        }
        "BFLOAT16" => {
            let bits = (v as f32).to_bits();
            let rounded = bits.wrapping_add(0x7fff + ((bits >> 16) & 1));
            f32::from_bits(rounded & 0xffff0000) as f64
        }
        "INT8" => {
            if v < -128.0 || v > 127.0 || v.fract() != 0.0 {
                return Err("Invalid INT8 vector element".into());
            }
            v
        }
        "UINT8" => {
            if v < 0.0 || v > 255.0 || v.fract() != 0.0 {
                return Err("Invalid UINT8 vector element".into());
            }
            v
        }
        _ => v,
    };
    if n.is_finite() {
        Ok(n)
    } else {
        Err("Vector element exceeds datatype range".into())
    }
}
pub fn validate_values(c: &VectorConfig, values: &[f64]) -> Result<(), String> {
    if values.len() != c.dim {
        return Err(format!(
            "SEARCH_VECTOR_LEN_BAD Invalid vector length. Expected {}, got {}",
            c.dim,
            values.len()
        ));
    }
    for &value in values {
        typed_value(c, value)?;
    }
    Ok(())
}
pub fn decode(c: &VectorConfig, bytes: &[u8]) -> Result<Vec<f64>, String> {
    let expected = c
        .dim
        .checked_mul(c.element_size())
        .ok_or("Vector blob length overflow")?;
    if bytes.len() != expected {
        return Err(format!(
            "SEARCH_VECTOR_BLOB_SIZE_MISMATCH Could not add vector with blob size {} (expected size {expected})",
            bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(c.dim);
    for b in bytes.chunks_exact(c.element_size()) {
        let v = match c.data_type.as_str() {
            "FLOAT64" => f64::from_le_bytes(b.try_into().unwrap()),
            "FLOAT32" => f32::from_le_bytes(b.try_into().unwrap()) as f64,
            "FLOAT16" => half_to_f64(u16::from_le_bytes(b.try_into().unwrap())),
            "BFLOAT16" => {
                f32::from_bits((u16::from_le_bytes(b.try_into().unwrap()) as u32) << 16) as f64
            }
            "INT8" => (b[0] as i8) as f64,
            "UINT8" => b[0] as f64,
            _ => return Err("Invalid vector datatype".into()),
        };
        if !v.is_finite() {
            return Err("Vector elements must be finite".into());
        }
        out.push(v);
    }
    Ok(out)
}
fn metric_distance(metric: &str, values: impl Iterator<Item = (f64, f64)>) -> f64 {
    let (mut dot, mut a, mut b, mut l2) = (0.0, 0.0, 0.0, 0.0);
    for (x, y) in values {
        if metric == "L2" {
            l2 += (x - y) * (x - y);
        } else {
            dot += x * y;
            if metric == "COSINE" {
                a += x * x;
                b += y * y;
            }
        }
    }
    match metric {
        "L2" => l2,
        "COSINE" => {
            if a == 0.0 || b == 0.0 {
                1.0
            } else {
                (1.0 - dot / (a.sqrt() * b.sqrt())).clamp(0.0, 2.0)
            }
        }
        _ => 1.0 - dot,
    }
}
#[derive(Clone, Debug)]
enum Values {
    Dense(Vec<f64>),
    Quantized { codes: Vec<u8>, min: f64, step: f64 },
}
impl Values {
    fn len(&self) -> usize {
        match self {
            Self::Dense(v) => v.len(),
            Self::Quantized { codes, .. } => codes.len(),
        }
    }
    fn at(&self, i: usize) -> f64 {
        match self {
            Self::Dense(v) => v[i],
            Self::Quantized { codes, min, step } => min + codes[i] as f64 * step,
        }
    }
    fn distance(&self, metric: &str, q: &[f64]) -> f64 {
        metric_distance(metric, q.iter().enumerate().map(|(i, v)| (*v, self.at(i))))
    }
    fn between(&self, metric: &str, other: &Self) -> f64 {
        metric_distance(metric, (0..self.len()).map(|i| (self.at(i), other.at(i))))
    }
    fn quantize(&mut self, min: f64, max: f64) {
        let step = (max - min) / 255.0;
        let codes = (0..self.len())
            .map(|i| {
                if step == 0.0 {
                    0
                } else {
                    ((self.at(i) - min) / step).round().clamp(0.0, 255.0) as u8
                }
            })
            .collect();
        *self = Self::Quantized { codes, min, step };
    }
}
#[derive(Clone, Debug)]
struct Node {
    doc: DocId,
    values: Values,
    links: Vec<Vec<u64>>,
}
#[derive(Clone, Copy, Debug)]
struct Candidate {
    distance: f64,
    id: u64,
}
impl PartialEq for Candidate {
    fn eq(&self, o: &Self) -> bool {
        self.distance.total_cmp(&o.distance).is_eq() && self.id == o.id
    }
}
impl Eq for Candidate {}
impl PartialOrd for Candidate {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for Candidate {
    fn cmp(&self, o: &Self) -> Ordering {
        self.distance
            .total_cmp(&o.distance)
            .then(self.id.cmp(&o.id))
    }
}
#[path = "vector_rollback.rs"]
mod rollback;
#[derive(Clone, Debug)]
pub struct VectorIndex {
    config: VectorConfig,
    nodes: HashMap<u64, Node>,
    documents: BTreeMap<DocId, Vec<u64>>,
    entry: Option<u64>,
    next: u64,
    quantizer: Option<(f64, f64)>,
    incoming: BTreeMap<u64, BTreeSet<(u64, usize)>>,
    levels: BTreeSet<(usize, u64)>,
    node_order: BTreeSet<u64>,
    undo: Option<rollback::VectorUndo>,
}
#[derive(Debug)]
struct Runtime {
    window: usize,
    epsilon: f64,
    policy: Option<String>,
    batch: Option<usize>,
    history: bool,
    capacity: usize,
}
impl VectorIndex {
    pub fn new(c: &VectorConfig) -> Self {
        let mut nodes = HashMap::new();
        // INITIAL_CAP is a capacity hint, not a commitment to allocate on OOM.
        let _ = nodes.try_reserve(c.initial_cap);
        Self {
            config: c.clone(),
            nodes,
            documents: BTreeMap::new(),
            entry: None,
            next: 0,
            quantizer: None,
            incoming: BTreeMap::new(),
            levels: BTreeSet::new(),
            node_order: BTreeSet::new(),
            undo: None,
        }
    }
    pub(super) fn gc_compact(&mut self) -> usize {
        let before = self.memory_bytes();
        self.nodes.shrink_to_fit();
        for node in self.nodes.values_mut() {
            if let Values::Dense(v) = &mut node.values {
                v.shrink_to_fit();
            }
            if let Values::Quantized { codes, .. } = &mut node.values {
                codes.shrink_to_fit();
            }
            for links in &mut node.links {
                links.shrink_to_fit();
            }
            node.links.shrink_to_fit();
        }
        for ids in self.documents.values_mut() {
            ids.shrink_to_fit();
        }
        before.saturating_sub(self.memory_bytes())
    }
    /// Owned capacities plus live tree entries; allocator bookkeeping is estimated.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        let tree_overhead = 3usize.saturating_mul(size_of::<usize>());
        let mut bytes = size_of::<Self>().saturating_add(
            self.nodes
                .capacity()
                .saturating_mul(size_of::<(u64, Node)>().saturating_add(1)),
        );
        bytes = bytes
            .saturating_add(self.config.algorithm.capacity())
            .saturating_add(self.config.data_type.capacity())
            .saturating_add(self.config.distance_metric.capacity());
        bytes = bytes.saturating_add(self.config.compression.as_ref().map_or(0, String::capacity));
        for node in self.nodes.values() {
            bytes = bytes.saturating_add(match &node.values {
                Values::Dense(v) => v.capacity().saturating_mul(size_of::<f64>()),
                Values::Quantized { codes, .. } => codes.capacity(),
            });
            bytes =
                bytes.saturating_add(node.links.capacity().saturating_mul(size_of::<Vec<u64>>()));
            for links in &node.links {
                bytes = bytes.saturating_add(links.capacity().saturating_mul(size_of::<u64>()));
            }
        }
        for ids in self.documents.values() {
            bytes = bytes
                .saturating_add(size_of::<(DocId, Vec<u64>)>())
                .saturating_add(tree_overhead)
                .saturating_add(ids.capacity().saturating_mul(size_of::<u64>()));
        }
        for incoming in self.incoming.values() {
            bytes = bytes
                .saturating_add(size_of::<(u64, BTreeSet<(u64, usize)>)>())
                .saturating_add(tree_overhead)
                .saturating_add(
                    incoming
                        .len()
                        .saturating_mul(size_of::<(u64, usize)>().saturating_add(tree_overhead)),
                );
        }
        bytes
            .saturating_add(
                self.levels
                    .len()
                    .saturating_mul(size_of::<(usize, u64)>().saturating_add(tree_overhead)),
            )
            .saturating_add(
                self.node_order
                    .len()
                    .saturating_mul(size_of::<u64>().saturating_add(tree_overhead)),
            )
    }
    fn runtime(
        &self,
        params: &BTreeMap<String, String>,
        range: bool,
        filtered: bool,
    ) -> Result<Runtime, String> {
        let c = &self.config;
        let mut r = Runtime {
            window: if c.algorithm == "HNSW" {
                c.ef_runtime
            } else {
                c.search_window_size
            },
            epsilon: c.epsilon,
            policy: None,
            batch: None,
            history: true,
            capacity: 0,
        };
        let mut seen = BTreeSet::new();
        for (key, v) in params {
            let key = key.trim_start_matches('$').to_ascii_uppercase();
            if !seen.insert(key.clone()) {
                return Err(format!("Duplicate vector parameter {key}"));
            }
            match key.as_str() {
                "EF_RUNTIME" if c.algorithm == "HNSW" && !range => r.window = positive(v, &key)?,
                "SEARCH_WINDOW_SIZE" if c.algorithm == "SVS-VAMANA" && !range => {
                    r.window = positive(v, &key)?
                }
                "EPSILON" if c.algorithm != "FLAT" && range => {
                    r.epsilon = finite(v, &key)?;
                    if r.epsilon <= 0.0 {
                        return Err("EPSILON must be positive".into());
                    }
                }
                "HYBRID_POLICY" if !range && filtered => {
                    let p = v.to_ascii_uppercase();
                    if !matches!(p.as_str(), "ADHOC_BF" | "BATCHES") {
                        return Err("Invalid HYBRID_POLICY".into());
                    }
                    r.policy = Some(p);
                }
                "BATCH_SIZE" if !range && filtered => r.batch = Some(positive(v, &key)?),
                "SHARD_K_RATIO" if !range => {
                    let ratio = finite(v, &key)?;
                    if !(0.1..=1.0).contains(&ratio)
                        || (ratio * 100.0 - (ratio * 100.0).round()).abs() > 1e-8
                    {
                        return Err(
                            "SHARD_K_RATIO must be 0.1 through 1.0 with at most two decimal places"
                                .into(),
                        );
                    } /* One standalone shard always needs all k results. */
                }
                "USE_SEARCH_HISTORY" if c.algorithm == "SVS-VAMANA" => {
                    r.history = match v.to_ascii_uppercase().as_str() {
                        "ON" | "AUTO" => true,
                        "OFF" => false,
                        _ => return Err("USE_SEARCH_HISTORY must be ON, OFF or AUTO".into()),
                    };
                }
                "SEARCH_BUFFER_CAPACITY"
                    if c.algorithm == "SVS-VAMANA"
                        && c.compression.as_deref().is_some_and(|v| v.contains('X'))
                        && !range =>
                {
                    r.capacity = positive(v, &key)?
                }
                _ => return Err(format!("Invalid vector query parameter {key}")),
            }
        }
        if r.batch.is_some() && r.policy.as_deref() == Some("ADHOC_BF") {
            return Err("BATCH_SIZE is incompatible with ADHOC_BF".into());
        }
        if r.policy.as_deref() == Some("ADHOC_BF") && seen.contains("EF_RUNTIME") {
            return Err("EF_RUNTIME is incompatible with ADHOC_BF".into());
        }
        if r.capacity != 0 && r.capacity < r.window {
            return Err("SEARCH_BUFFER_CAPACITY must be at least SEARCH_WINDOW_SIZE".into());
        }
        Ok(r)
    }
    fn validate(&self, q: &[f64]) -> Result<(), String> {
        if q.len() != self.config.dim {
            return Err(format!(
                "SEARCH_VECTOR_LEN_BAD Invalid vector length. Expected {}, got {}",
                self.config.dim,
                q.len()
            ));
        }
        if q.iter().any(|v| !v.is_finite()) {
            return Err("Vector elements must be finite".into());
        }
        Ok(())
    }
    pub fn insert(&mut self, id: DocId, vectors: &[Vec<f64>]) -> Result<(), String> {
        let mut resolved = Vec::with_capacity(vectors.len());
        for v in vectors {
            self.validate(v)?;
            resolved.push(
                v.iter()
                    .map(|v| typed_value(&self.config, *v))
                    .collect::<Result<Vec<_>, _>>()?,
            );
        }
        if self.next.checked_add(resolved.len() as u64).is_none() {
            return Err("Vector node identifier exhausted".into());
        }
        self.remove(id);
        if let Some((old_min, old_max)) = self.quantizer {
            let (mut min, mut max) = (old_min, old_max);
            for v in &resolved {
                for &x in v {
                    min = min.min(x);
                    max = max.max(x);
                }
            }
            if min < old_min || max > old_max {
                self.remember_quantization();
                for node in self.nodes.values_mut() {
                    node.values.quantize(min, max);
                }
                self.quantizer = Some((min, max));
            }
        }
        let mut ids = Vec::with_capacity(resolved.len());
        for v in resolved {
            let node_id = self.next;
            self.next += 1;
            let level = if self.config.algorithm == "HNSW" {
                self.level(node_id, id)
            } else {
                0
            };
            let mut values = Values::Dense(v);
            if let Some((min, max)) = self.quantizer {
                values.quantize(min, max);
            }
            self.remember_node(node_id);
            self.nodes.insert(
                node_id,
                Node {
                    doc: id,
                    values,
                    links: vec![Vec::new(); level + 1],
                },
            );
            self.node_order.insert(node_id);
            self.levels.insert((level, node_id));
            if self.config.algorithm != "FLAT" {
                self.connect(node_id);
            }
            ids.push(node_id);
        }
        self.remember_document(id);
        if !ids.is_empty() {
            self.documents.insert(id, ids);
        }
        // OSS uses global 8-bit scalar quantization for every named compression mode;
        // REDUCE and proprietary multi-level LVQ/LeanVec transforms do not apply.
        if self.config.compression.is_some()
            && self.quantizer.is_none()
            && self.nodes.len() >= self.config.training_threshold
        {
            let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
            for n in self.nodes.values() {
                for i in 0..n.values.len() {
                    let v = n.values.at(i);
                    min = min.min(v);
                    max = max.max(v);
                }
            }
            self.remember_quantization();
            for n in self.nodes.values_mut() {
                n.values.quantize(min, max);
            }
            self.quantizer = Some((min, max));
        }
        Ok(())
    }
    fn level(&self, node: u64, doc: DocId) -> usize {
        let mut x = node
            .wrapping_add(doc.rotate_left(17))
            .wrapping_add(0x9e3779b97f4a7c15);
        x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
        x ^= x >> 31;
        let mut level = 0;
        while level < 24 && x.is_multiple_of(self.config.m as u64) {
            level += 1;
            x /= self.config.m as u64;
        }
        level
    }
    fn node_distance(&self, a: u64, b: u64) -> f64 {
        self.nodes[&a]
            .values
            .between(&self.config.distance_metric, &self.nodes[&b].values)
    }
    fn greedy(&self, q: &[f64], mut at: u64, layer: usize) -> u64 {
        let mut best = self.nodes[&at]
            .values
            .distance(&self.config.distance_metric, q);
        loop {
            if super::runtime::cancelled() {
                return at;
            }
            let old = at;
            for n in self.nodes[&at].links.get(layer).into_iter().flatten() {
                let d = self.nodes[n]
                    .values
                    .distance(&self.config.distance_metric, q);
                if d < best {
                    best = d;
                    at = *n;
                }
            }
            if at == old {
                return at;
            }
        }
    }
    fn visit_neighbors(&self, id: u64, layer: usize, mut visit: impl FnMut(u64)) {
        if let Some(links) = self.nodes[&id].links.get(layer) {
            for &n in links {
                visit(n);
            }
        }
        // A navigable base-layer ring keeps incremental deletion from stranding components.
        if layer == 0 && self.nodes.len() > 1 {
            let prev = *self
                .node_order
                .range(..id)
                .next_back()
                .or_else(|| self.node_order.last())
                .unwrap();
            let next = *self
                .node_order
                .range((std::ops::Bound::Excluded(id), std::ops::Bound::Unbounded))
                .next()
                .or_else(|| self.node_order.first())
                .unwrap();
            let degree = if self.config.algorithm == "HNSW" {
                2 * self.config.m
            } else {
                self.config.graph_max_degree
            };
            if degree > 1 && prev != id {
                visit(prev);
            }
            if next != id && (next != prev || degree == 1) {
                visit(next);
            }
        }
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "Layer, beam bound, exclusions, radius, and history are independent HNSW traversal inputs."
    )]
    fn search_layer(
        &self,
        q: &[f64],
        entry: u64,
        layer: usize,
        ef: usize,
        exclude: Option<u64>,
        radius: Option<f64>,
        history: bool,
    ) -> Vec<Candidate> {
        let ef = ef.max(1).min(self.nodes.len().max(1));
        let start = Candidate {
            distance: self.nodes[&entry]
                .values
                .distance(&self.config.distance_metric, q),
            id: entry,
        };
        let mut pending = BinaryHeap::from([Reverse(start)]);
        let mut best = BinaryHeap::new();
        let mut visited = BTreeSet::from([entry]);
        let mut explored = Vec::new();
        let mut range_matches = Vec::new();
        if radius.is_some_and(|r| start.distance <= r) && Some(entry) != exclude {
            range_matches.push(start);
        }
        if Some(entry) != exclude {
            best.push(start);
        }
        while let Some(Reverse(c)) = pending.pop() {
            if super::runtime::cancelled() {
                break;
            }
            if best.len() >= ef
                && c.distance > best.peek().unwrap().distance
                && radius.is_none_or(|r| c.distance > r)
            {
                break;
            }
            if history && radius.is_none() {
                explored.push(c);
            }
            self.visit_neighbors(c.id, layer, |id| {
                if Some(id) == exclude || !visited.insert(id) {
                    return;
                }
                let n = Candidate {
                    distance: self.nodes[&id]
                        .values
                        .distance(&self.config.distance_metric, q),
                    id,
                };
                if radius.is_some_and(|r| n.distance <= r) {
                    range_matches.push(n);
                }
                if best.len() < ef
                    || n.distance <= best.peek().unwrap().distance
                    || radius.is_some_and(|r| n.distance <= r)
                {
                    pending.push(Reverse(n));
                    best.push(n);
                    if best.len() > ef {
                        best.pop();
                    }
                }
            });
        }
        if radius.is_some() {
            range_matches.sort();
            range_matches
        } else if history {
            explored.extend(best);
            explored.sort();
            explored.dedup_by_key(|c| c.id);
            explored
        } else {
            let mut v = best.into_vec();
            v.sort();
            v
        }
    }
    fn set_links(&mut self, id: u64, layer: usize, links: Vec<u64>) {
        if self.recording_rollback() {
            self.remember_node(id);
            let targets = self.nodes[&id].links[layer]
                .iter()
                .chain(&links)
                .copied()
                .collect::<Vec<_>>();
            for target in targets {
                self.remember_incoming(target);
            }
        }
        for &old in &self.nodes[&id].links[layer] {
            if let Some(incoming) = self.incoming.get_mut(&old) {
                incoming.remove(&(id, layer));
                if incoming.is_empty() {
                    self.incoming.remove(&old);
                }
            }
        }
        for &next in &links {
            self.incoming.entry(next).or_default().insert((id, layer));
        }
        self.nodes.get_mut(&id).unwrap().links[layer] = links;
    }
    fn connect(&mut self, id: u64) {
        let Some(mut entry) = self.entry else {
            self.entry = Some(id);
            return;
        };
        let level = self.nodes[&id].links.len() - 1;
        let top = self.nodes[&entry].links.len() - 1;
        let q = (0..self.config.dim)
            .map(|i| self.nodes[&id].values.at(i))
            .collect::<Vec<_>>();
        for layer in ((level + 1)..=top).rev() {
            entry = self.greedy(&q, entry, layer);
        }
        for layer in (0..=level.min(top)).rev() {
            let window = if self.config.algorithm == "HNSW" {
                self.config.ef_construction
            } else {
                self.config.construction_window_size
            };
            let found = self.search_layer(&q, entry, layer, window, Some(id), None, true);
            let candidates = found.iter().map(|c| c.id).collect();
            let selected = self.prune(id, candidates, layer);
            self.set_links(id, layer, selected.clone());
            for n in selected {
                let mut links = self.nodes[&n].links[layer].clone();
                links.push(id);
                let links = self.prune(n, links, layer);
                self.set_links(n, layer, links);
            }
            if let Some(n) = found.first() {
                entry = n.id;
            }
        }
        if level > top {
            self.entry = Some(id);
        }
    }
    fn prune(&self, id: u64, mut candidates: Vec<u64>, layer: usize) -> Vec<u64> {
        candidates.retain(|n| *n != id && self.nodes.contains_key(n));
        candidates.sort_unstable();
        candidates.dedup();
        candidates.sort_by(|a, b| {
            self.node_distance(id, *a)
                .total_cmp(&self.node_distance(id, *b))
                .then(a.cmp(b))
        });
        let degree = if self.config.algorithm == "HNSW" {
            self.config.m * if layer == 0 { 2 } else { 1 }
        } else {
            self.config.graph_max_degree
        };
        // Reserve base-layer edge slots for the incremental navigability ring.
        let max = degree.saturating_sub(if layer == 0 { degree.min(2) } else { 0 });
        let alpha = if self.config.algorithm == "SVS-VAMANA" {
            1.2
        } else {
            1.0
        };
        let mut selected = Vec::with_capacity(max.min(candidates.len()));
        for &n in &candidates {
            if selected.len() == max {
                break;
            }
            let d = self.node_distance(id, n);
            if selected
                .iter()
                .all(|s| alpha * self.node_distance(*s, n) > d)
            {
                selected.push(n);
            }
        }
        selected
    }
    pub fn remove(&mut self, id: DocId) {
        self.remember_document(id);
        let Some(ids) = self.documents.remove(&id) else {
            return;
        };
        for node in ids {
            if self.recording_rollback() {
                self.remember_node(node);
                self.remember_incoming(node);
                if let Some(n) = self.nodes.get(&node) {
                    let targets = n.links.iter().flatten().copied().collect::<Vec<_>>();
                    for target in targets {
                        self.remember_incoming(target);
                    }
                }
                if let Some(incoming) = self.incoming.get(&node) {
                    let sources = incoming
                        .iter()
                        .map(|(source, _)| *source)
                        .collect::<Vec<_>>();
                    for source in sources {
                        self.remember_node(source);
                    }
                }
            }
            if let Some(n) = self.nodes.remove(&node) {
                self.node_order.remove(&node);
                self.levels.remove(&(n.links.len() - 1, node));
                for (layer, links) in n.links.into_iter().enumerate() {
                    for target in links {
                        if let Some(incoming) = self.incoming.get_mut(&target) {
                            incoming.remove(&(node, layer));
                            if incoming.is_empty() {
                                self.incoming.remove(&target);
                            }
                        }
                    }
                }
                if let Some(incoming) = self.incoming.remove(&node) {
                    for (source, layer) in incoming {
                        if let Some(source) = self.nodes.get_mut(&source) {
                            source.links[layer].retain(|n| *n != node);
                        }
                    }
                }
            }
        }
        if self.entry.is_some_and(|e| !self.nodes.contains_key(&e)) {
            self.entry = self.levels.last().map(|(_, id)| *id);
        }
        if self.nodes.is_empty() {
            self.quantizer = None;
        }
    }
    fn results(
        &self,
        q: &[f64],
        candidates: impl Iterator<Item = u64>,
        filter: Option<&DocSet>,
        radius: Option<f64>,
    ) -> Vec<(DocId, f64)> {
        let mut docs = BTreeMap::<DocId, f64>::new();
        for id in candidates {
            if super::runtime::cancelled() {
                break;
            }
            let n = &self.nodes[&id];
            if filter.is_some_and(|f| !f.contains(&n.doc)) {
                continue;
            }
            let d = n.values.distance(&self.config.distance_metric, q);
            if radius.is_some_and(|r| d > r) {
                continue;
            }
            docs.entry(n.doc)
                .and_modify(|old| *old = old.min(d))
                .or_insert(d);
        }
        let mut out: Vec<_> = docs.into_iter().collect();
        super::runtime::sort(
            &mut out,
            |a, b| a.1.total_cmp(&b.1).then(a.0.cmp(&b.0)),
            super::runtime::current_deadline(),
        );
        out
    }
    pub fn query(
        &self,
        q: &[f64],
        k: usize,
        filter: Option<&DocSet>,
        params: &BTreeMap<String, String>,
    ) -> Result<Vec<(DocId, f64)>, String> {
        self.validate(q)?;
        let r = self.runtime(params, false, filter.is_some())?;
        if k == 0 || self.nodes.is_empty() {
            return Ok(Vec::new());
        }
        let adhoc = filter.is_some_and(|f| {
            r.policy.as_deref() == Some("ADHOC_BF")
                || (r.policy.is_none() && f.len().saturating_mul(8) < self.documents.len())
        });
        let mut out = if adhoc {
            self.results(
                q,
                filter
                    .unwrap()
                    .iter()
                    .filter_map(|id| self.documents.get(id))
                    .flatten()
                    .copied(),
                None,
                None,
            )
        } else if self.config.algorithm == "FLAT" {
            self.results(q, self.nodes.keys().copied(), filter, None)
        } else {
            let mut entry = self.entry.unwrap();
            for layer in (1..self.nodes[&entry].links.len()).rev() {
                entry = self.greedy(q, entry, layer);
            }
            let mut window = r.window.max(k).max(r.capacity).min(self.nodes.len());
            let mut results;
            loop {
                let found = self.search_layer(q, entry, 0, window, None, None, r.history);
                results = self.results(q, found.into_iter().map(|n| n.id), filter, None);
                if results.len() >= k || window >= self.nodes.len() || super::runtime::cancelled() {
                    break;
                }
                window = window
                    .saturating_add(r.batch.unwrap_or(window))
                    .min(self.nodes.len());
            }
            results
        };
        out.truncate(k);
        Ok(out)
    }
    pub fn range(
        &self,
        q: &[f64],
        radius: f64,
        filter: Option<&DocSet>,
        params: &BTreeMap<String, String>,
    ) -> Result<Vec<(DocId, f64)>, String> {
        self.validate(q)?;
        if !radius.is_finite() || radius < 0.0 {
            return Err("Vector radius must be finite and nonnegative".into());
        }
        let r = self.runtime(params, true, filter.is_some())?;
        if self.nodes.is_empty() {
            return Ok(Vec::new());
        }
        if self.config.algorithm == "FLAT" {
            return Ok(self.results(q, self.nodes.keys().copied(), filter, Some(radius)));
        }
        let mut entry = self.entry.unwrap();
        for layer in (1..self.nodes[&entry].links.len()).rev() {
            entry = self.greedy(q, entry, layer);
        }
        let found = self.search_layer(
            q,
            entry,
            0,
            r.window,
            None,
            Some(radius * (1.0 + r.epsilon)),
            r.history,
        );
        Ok(self.results(q, found.into_iter().map(|n| n.id), filter, Some(radius)))
    }
}
