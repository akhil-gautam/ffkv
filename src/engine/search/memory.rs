use super::{Catalog, model::*};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    mem::{size_of, size_of_val},
};

impl Catalog {
    /// Estimates resident storage reachable from the global catalog, counted once
    /// regardless of how many databases or shards hold its Arc. This is not an
    /// allocator usable-size measurement: capacities are used where exposed, and
    /// ordered-tree nodes are approximated by their live entries plus three words
    /// per entry (links, occupancy, and amortized spare slots). Allocator headers,
    /// size-class rounding, and retained empty tree nodes are not observable.
    /// serde_json hides object capacities and arbitrary-precision number String
    /// capacities, so those use live entry counts and numeric text lengths. Its
    /// preserve_order object entries include an estimated hash/index overhead.
    ///
    /// Inline values are charged by their enclosing allocation, then only their
    /// owned heap storage is visited. The suffix and record helpers deduplicate
    /// their shared Arc/Bytes allocations; pinned snapshots' current chunks are
    /// therefore charged once, and detached old chunks belong to the snapshot,
    /// not this catalog. The estimate does not include temporary traversal state.
    /// All arithmetic saturates. No documents, postings, or rows are cloned.
    pub(crate) fn memory_footprint(&self) -> usize {
        let state = self.lock();
        // The runtime installs one Arc<Catalog>; handles do not own more catalogs.
        let mut bytes = Estimate(size_of::<Self>());
        bytes.add(size_of::<usize>().saturating_mul(2));
        bytes.add(tree_map(&state.indexes));
        for (name, index) in &state.indexes {
            bytes.add(name.capacity());
            bytes.add(index_heap(index));
        }
        bytes.add(string_map(&state.aliases));
        bytes.add(string_sets(&state.dictionaries));
        bytes.add(string_map(&state.config.extras));
        bytes.add(tree_map(&state.cursors));
        for cursor in state.cursors.values() {
            bytes.add(cursor.index.capacity());
            bytes.capacity::<Row>(cursor.rows.capacity());
            for row in &cursor.rows {
                bytes.add(row_heap(row));
            }
        }
        bytes.add(tree_set(&state.deadlines));
        bytes.add(tree_set(&state.temp_deadlines));
        for (_, name) in &state.temp_deadlines {
            bytes.add(name.capacity());
        }
        bytes.add(tree_set(&state.cursor_deadlines));
        for (_, name, _) in &state.deadlines {
            bytes.add(name.capacity());
        }
        bytes.add(state.record_memory_footprint());
        bytes.0
    }
}

struct Estimate(usize);
impl Estimate {
    fn add(&mut self, bytes: usize) {
        self.0 = self.0.saturating_add(bytes);
    }
    fn capacity<T>(&mut self, capacity: usize) {
        self.add(capacity.saturating_mul(size_of::<T>()));
    }
}

fn tree_entries<T>(len: usize) -> usize {
    len.saturating_mul(size_of::<T>().saturating_add(size_of::<usize>().saturating_mul(3)))
}
fn tree_map<K, V>(map: &BTreeMap<K, V>) -> usize {
    tree_entries::<(K, V)>(map.len())
}
fn tree_set<T>(set: &BTreeSet<T>) -> usize {
    tree_entries::<T>(set.len())
}
fn optional_string(value: &Option<String>) -> usize {
    value.as_ref().map_or(0, String::capacity)
}
fn string_map(map: &BTreeMap<String, String>) -> usize {
    let mut bytes = Estimate(tree_map(map));
    for (key, value) in map {
        bytes.add(key.capacity());
        bytes.add(value.capacity());
    }
    bytes.0
}
fn string_set(set: &BTreeSet<String>) -> usize {
    let mut bytes = Estimate(tree_set(set));
    for value in set {
        bytes.add(value.capacity());
    }
    bytes.0
}
fn string_sets(map: &BTreeMap<String, BTreeSet<String>>) -> usize {
    let mut bytes = Estimate(tree_map(map));
    for (name, values) in map {
        bytes.add(name.capacity());
        bytes.add(string_set(values));
    }
    bytes.0
}
fn document_sets(map: &BTreeMap<String, DocSet>) -> usize {
    let mut bytes = Estimate(tree_map(map));
    for (name, ids) in map {
        bytes.add(name.capacity());
        bytes.add(tree_set(ids));
    }
    bytes.0
}

fn schema_heap(schema: &Schema) -> usize {
    let mut bytes = Estimate(schema.name.capacity());
    bytes.capacity::<bytes::Bytes>(schema.prefixes.capacity());
    for prefix in &schema.prefixes {
        bytes.add(prefix.len());
    }
    bytes.capacity::<Field>(schema.fields.capacity());
    for field in &schema.fields {
        bytes.add(field.source.capacity());
        bytes.add(field.name.capacity());
        bytes.add(optional_string(&field.phonetic));
        if let Some(config) = &field.vector {
            bytes.add(config.algorithm.capacity());
            bytes.add(config.data_type.capacity());
            bytes.add(config.distance_metric.capacity());
            bytes.add(optional_string(&config.compression));
        }
    }
    bytes.add(optional_string(&schema.filter));
    bytes.add(schema.language.capacity());
    bytes.add(optional_string(&schema.language_field));
    bytes.add(optional_string(&schema.score_field));
    bytes.add(optional_string(&schema.payload_field));
    bytes.add(string_set(&schema.stopwords));
    bytes.0
}

fn index_heap(index: &Index) -> usize {
    let mut bytes = Estimate(schema_heap(&index.schema));
    bytes.add(tree_map(&index.docs));
    for document in index.docs.values() {
        bytes.add(document.key.capacity());
        bytes.add(document.payload.as_ref().map_or(0, Vec::capacity));
        bytes.add(tree_map(&document.fields));
        for (name, value) in &document.fields {
            bytes.add(name.capacity());
            bytes.add(value_heap(value));
        }
    }
    bytes.add(tree_map(&index.keys));
    for key in index.keys.keys() {
        bytes.add(key.capacity());
    }
    bytes.add(tree_map(&index.fields));
    for (name, field) in &index.fields {
        bytes.add(name.capacity());
        match field {
            FieldIndex::Text(terms) => {
                bytes.add(tree_map(terms));
                for (term, postings) in terms {
                    bytes.add(term.capacity());
                    bytes.add(tree_map(postings));
                    for positions in postings.values() {
                        bytes.capacity::<u32>(positions.capacity());
                    }
                }
            }
            FieldIndex::Tag(terms) => bytes.add(document_sets(terms)),
            FieldIndex::Numeric(numbers) => {
                bytes.add(tree_map(numbers));
                for ids in numbers.values() {
                    bytes.add(tree_set(ids));
                }
            }
            // memory_bytes includes Self, already inside the FieldIndex entry.
            FieldIndex::Vector(vectors) => {
                bytes.add(vectors.memory_bytes().saturating_sub(size_of_val(vectors)))
            }
            FieldIndex::Geo(points) => {
                bytes.add(points.memory_bytes().saturating_sub(size_of_val(points)))
            }
            FieldIndex::Shape(shapes) => {
                bytes.add(shapes.memory_bytes().saturating_sub(size_of_val(shapes)))
            }
        }
    }
    bytes.add(document_sets(&index.missing));
    bytes.add(document_sets(&index.empty));
    bytes.add(tree_map(&index.suffixes));
    for (name, suffixes) in &index.suffixes {
        bytes.add(name.capacity());
        bytes.add(
            suffixes
                .memory_bytes()
                .saturating_sub(size_of_val(suffixes)),
        );
    }
    bytes.add(optional_string(&index.last_error));
    bytes.add(index.last_error_key.as_ref().map_or(0, Vec::capacity));
    bytes.add(string_sets(&index.synonyms));
    bytes.add(tree_map(&index.field_errors));
    for (name, (_, message, key)) in &index.field_errors {
        bytes.add(name.capacity());
        bytes.add(message.capacity());
        bytes.add(key.capacity());
    }
    bytes.0
}

fn value_heap(value: &IndexedValue) -> usize {
    let mut bytes = Estimate(0);
    match value {
        IndexedValue::Text { strings, tokens } => {
            bytes.capacity::<String>(strings.capacity());
            for string in strings {
                bytes.add(string.capacity());
            }
            bytes.capacity::<Token>(tokens.capacity());
            for token in tokens {
                bytes.add(token.term.capacity());
            }
        }
        IndexedValue::Tag(tags) => {
            bytes.capacity::<String>(tags.capacity());
            for tag in tags {
                bytes.add(tag.capacity());
            }
        }
        IndexedValue::Numeric(numbers) => bytes.capacity::<f64>(numbers.capacity()),
        IndexedValue::Geo(points) => bytes.capacity::<[f64; 2]>(points.capacity()),
        IndexedValue::Geometry(wkt) => bytes.add(wkt.capacity()),
        IndexedValue::Vector(vectors) => {
            bytes.capacity::<Vec<f64>>(vectors.capacity());
            for vector in vectors {
                bytes.capacity::<f64>(vector.capacity());
            }
        }
        IndexedValue::Empty | IndexedValue::Missing => {}
    }
    bytes.0
}

fn row_heap(row: &Row) -> usize {
    let mut bytes = Estimate(tree_map(row));
    for (name, value) in row {
        bytes.add(name.capacity());
        bytes.add(json_heap(value));
    }
    bytes.0
}
fn json_heap(value: &Value) -> usize {
    let mut bytes = Estimate(0);
    match value {
        Value::String(string) => bytes.add(string.capacity()),
        Value::Number(number) => bytes.add(number.as_str().len()),
        Value::Array(values) => {
            bytes.capacity::<Value>(values.capacity());
            for value in values {
                bytes.add(json_heap(value));
            }
        }
        Value::Object(object) => {
            // serde_json's preserve_order Map hides its IndexMap capacity.
            bytes.add(tree_entries::<(String, Value)>(object.len()));
            for (name, value) in object {
                bytes.add(name.capacity());
                bytes.add(json_heap(value));
            }
        }
        Value::Null | Value::Bool(_) => {}
    }
    bytes.0
}
