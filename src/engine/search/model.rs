use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
mod prefix_bytes {
    use bytes::Bytes;
    use serde::{Deserialize, Deserializer, Serializer};
    pub fn serialize<S: Serializer>(values: &[Bytes], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(values.iter().map(Bytes::as_ref))
    }
    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Bytes>, D::Error> {
        Ok(Vec::<Vec<u8>>::deserialize(deserializer)?
            .into_iter()
            .map(Bytes::from)
            .collect())
    }
}

pub type DocId = u64;
pub type DocSet = BTreeSet<DocId>;
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FieldKind {
    Text,
    Tag,
    Numeric,
    Geo,
    GeoShape,
    Vector,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Field {
    pub source: String,
    pub name: String,
    pub kind: FieldKind,
    pub sortable: bool,
    pub unf: bool,
    pub noindex: bool,
    pub nostem: bool,
    pub weight: f64,
    pub phonetic: Option<String>,
    pub separator: Option<char>,
    pub case_sensitive: bool,
    pub index_empty: bool,
    pub index_missing: bool,
    pub suffix: bool,
    pub vector: Option<super::vector::VectorConfig>,
    pub spherical: bool,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Schema {
    pub name: String,
    pub json: bool,
    #[serde(with = "prefix_bytes")]
    pub prefixes: Vec<Bytes>,
    pub fields: Vec<Field>,
    pub filter: Option<String>,
    pub language: String,
    pub language_field: Option<String>,
    pub score: f64,
    pub score_field: Option<String>,
    pub payload_field: Option<String>,
    pub stopwords: BTreeSet<String>,
    pub nooffsets: bool,
    pub nohl: bool,
    pub nofields: bool,
    pub nofreqs: bool,
    pub maxtextfields: bool,
    pub indexall: bool,
    pub temporary_ms: Option<i64>,
    pub deadline: Option<i64>,
    pub wide_text_fields: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Token {
    pub term: String,
    pub position: u32,
    pub start: usize,
    pub end: usize,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum IndexedValue {
    Text {
        strings: Vec<String>,
        tokens: Vec<Token>,
    },
    Tag(Vec<String>),
    Numeric(Vec<f64>),
    Geo(Vec<[f64; 2]>),
    Geometry(String),
    Vector(Vec<Vec<f64>>),
    Empty,
    Missing,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Document {
    pub id: DocId,
    pub key: Vec<u8>,
    pub fields: BTreeMap<String, IndexedValue>,
    pub score: f64,
    pub payload: Option<Vec<u8>>,
    pub expires_at: Option<i64>,
    pub field_deadline: Option<i64>,
    pub length: usize,
    pub synonym_generation: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Number(pub f64);
impl PartialEq for Number {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}
impl Eq for Number {}
impl PartialOrd for Number {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Number {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Range bounds use numeric equality, so both signed zeros share one posting set.
        if self.0 == other.0 {
            std::cmp::Ordering::Equal
        } else {
            self.0.total_cmp(&other.0)
        }
    }
}
#[expect(
    clippy::large_enum_variant,
    reason = "Keep index state inline rather than allocating another box for each vector field."
)]
pub enum FieldIndex {
    Text(BTreeMap<String, BTreeMap<DocId, Vec<u32>>>),
    Tag(BTreeMap<String, DocSet>),
    Numeric(BTreeMap<Number, DocSet>),
    Geo(super::spatial::GeoIndex),
    Shape(super::spatial::ShapeIndex),
    Vector(super::vector::VectorIndex),
}
pub struct Index {
    pub schema: Schema,
    pub docs: BTreeMap<DocId, Document>,
    pub keys: BTreeMap<Vec<u8>, DocId>,
    pub fields: BTreeMap<String, FieldIndex>,
    pub missing: BTreeMap<String, DocSet>,
    pub suffixes: BTreeMap<String, super::text::SuffixIndex>,
    pub empty: BTreeMap<String, DocSet>,
    pub next_id: DocId,
    pub errors: u64,
    pub last_error: Option<String>,
    pub last_error_key: Option<Vec<u8>>,
    pub synonyms: BTreeMap<String, BTreeSet<String>>,
    pub synonym_generation: u64,
    pub total_length: usize,
    pub field_errors: BTreeMap<String, (u64, String, Vec<u8>)>,
    pub indexing_time_ms: f64,
    pub uses: u64,
    pub dialect_uses: [u64; 4],
}
#[derive(Clone, Debug)]
pub struct Hit {
    pub id: DocId,
    pub score: f64,
    pub vector_distance: Option<f64>,
    pub vector_distances: BTreeMap<String, f64>,
    pub matched_terms: Vec<String>,
}
pub struct QueryResult {
    pub hits: Vec<Hit>,
    pub explanation: String,
    pub visited: usize,
    pub timed_out: bool,
    pub score_explanations: BTreeMap<DocId, serde_json::Value>,
    pub deadline: Option<std::time::Instant>,
    pub timeout_capped: bool,
}
pub type Row = BTreeMap<String, serde_json::Value>;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub min_prefix: usize,
    pub max_expansions: usize,
    pub min_stem: usize,
    pub timeout_ms: u64,
    pub max_results: usize,
    pub max_aggregate: usize,
    pub default_dialect: u8,
    pub cursor_max_idle: u64,
    pub on_timeout_fail: bool,
    pub extras: BTreeMap<String, String>,
    pub workers: usize,
    pub on_timeout_strict: bool,
    pub foreground_timeout_ms: u64,
    pub gc: super::gc::Config,
    #[serde(skip)]
    pub pool: Option<std::sync::Arc<rayon::ThreadPool>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            min_prefix: 2,
            max_expansions: 200,
            min_stem: 4,
            timeout_ms: 500,
            max_results: 1_000_000,
            max_aggregate: 1_000_000,
            default_dialect: 1,
            cursor_max_idle: 300_000,
            on_timeout_fail: false,
            extras: BTreeMap::new(),
            workers: 0,
            on_timeout_strict: false,
            foreground_timeout_ms: 60_000,
            gc: super::gc::Config::default(),
            pool: None,
        }
    }
}
