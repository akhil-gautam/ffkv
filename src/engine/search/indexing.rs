use super::model::*;
use std::collections::{BTreeMap, BTreeSet};
pub fn new_index(schema: Schema) -> Index {
    let mut index = Index {
        schema,
        docs: BTreeMap::new(),
        keys: BTreeMap::new(),
        fields: BTreeMap::new(),
        suffixes: BTreeMap::new(),
        missing: BTreeMap::new(),
        empty: BTreeMap::new(),
        next_id: 1,
        errors: 0,
        last_error: None,
        last_error_key: None,
        synonyms: BTreeMap::new(),
        synonym_generation: 0,
        total_length: 0,
        field_errors: BTreeMap::new(),
        indexing_time_ms: 0.,
        uses: 0,
        dialect_uses: [0; 4],
    };
    reset_fields(&mut index);
    index
}
pub fn reset_fields(index: &mut Index) {
    for f in &index.schema.fields {
        if index.fields.contains_key(&f.name) {
            continue;
        }
        let v = match f.kind {
            FieldKind::Text => FieldIndex::Text(BTreeMap::new()),
            FieldKind::Tag => FieldIndex::Tag(BTreeMap::new()),
            FieldKind::Numeric => FieldIndex::Numeric(BTreeMap::new()),
            FieldKind::Geo => FieldIndex::Geo(super::spatial::GeoIndex::new()),
            FieldKind::GeoShape => FieldIndex::Shape(super::spatial::ShapeIndex::new(f.spherical)),
            FieldKind::Vector => FieldIndex::Vector(super::vector::VectorIndex::new(
                f.vector.as_ref().expect("validated vector schema"),
            )),
        };
        if f.suffix {
            index.suffixes.entry(f.name.clone()).or_default();
        }
        index.fields.insert(f.name.clone(), v);
    }
}
fn remove_set<K: Ord>(map: &mut BTreeMap<K, DocSet>, key: &K, id: DocId) {
    if let Some(set) = map.get_mut(key) {
        set.remove(&id);
        if set.is_empty() {
            map.remove(key);
        }
    }
}
pub fn unindex(index: &mut Index, id: DocId, name: &str, old: &IndexedValue) {
    if let Some(set) = index.missing.get_mut(name) {
        set.remove(&id);
    }
    if let Some(set) = index.empty.get_mut(name) {
        set.remove(&id);
    }
    let Some(field) = index.fields.get_mut(name) else {
        return;
    };
    match (field, old) {
        (FieldIndex::Text(terms), IndexedValue::Text { tokens, .. }) => {
            for token in tokens {
                if let Some(postings) = terms.get_mut(&token.term) {
                    postings.remove(&id);
                    if postings.is_empty() {
                        terms.remove(&token.term);
                        if let Some(suffixes) = index.suffixes.get_mut(name) {
                            suffixes.remove(&token.term);
                        }
                    }
                }
            }
        }
        (FieldIndex::Tag(tags), IndexedValue::Tag(values)) => {
            for v in values {
                remove_set(tags, v, id);
                if !tags.contains_key(v)
                    && let Some(suffixes) = index.suffixes.get_mut(name)
                {
                    suffixes.remove(v);
                }
            }
        }
        (FieldIndex::Numeric(nums), IndexedValue::Numeric(values)) => {
            for &v in values {
                remove_set(nums, &Number(v), id)
            }
        }
        (FieldIndex::Geo(g), _) => g.remove(id),
        (FieldIndex::Shape(g), _) => g.remove(id),
        (FieldIndex::Vector(v), _) => v.remove(id),
        _ => {}
    }
}
pub fn insert(
    index: &mut Index,
    id: DocId,
    name: &str,
    value: &IndexedValue,
) -> Result<(), String> {
    let f = index
        .schema
        .fields
        .iter()
        .find(|f| f.name == name)
        .ok_or("Unknown indexed field")?;
    match value {
        IndexedValue::Missing => {
            if f.index_missing {
                index.missing.entry(name.into()).or_default().insert(id);
            }
            return Ok(());
        }
        IndexedValue::Empty => {
            if f.index_empty {
                index.empty.entry(name.into()).or_default().insert(id);
            }
            return Ok(());
        }
        _ => {}
    }
    if f.noindex {
        return Ok(());
    }
    let field = index
        .fields
        .get_mut(name)
        .ok_or("Missing index structure")?;
    match (field, value) {
        (FieldIndex::Text(terms), IndexedValue::Text { tokens, strings }) => {
            for token in tokens {
                if let Some(postings) = terms.get_mut(&token.term) {
                    postings.entry(id).or_default().push(token.position);
                } else {
                    if !token.term.starts_with(&['+', '$', '~'][..])
                        && let Some(suffixes) = index.suffixes.get_mut(name)
                    {
                        suffixes.insert(&token.term);
                    }
                    terms.insert(
                        token.term.clone(),
                        BTreeMap::from([(id, vec![token.position])]),
                    );
                }
            }
            if strings.iter().any(String::is_empty) && f.index_empty {
                index.empty.entry(name.into()).or_default().insert(id);
            }
        }
        (FieldIndex::Tag(tags), IndexedValue::Tag(values)) => {
            for v in values {
                if let Some(postings) = tags.get_mut(v) {
                    postings.insert(id);
                } else {
                    if let Some(suffixes) = index.suffixes.get_mut(name) {
                        suffixes.insert(v);
                    }
                    tags.insert(v.clone(), BTreeSet::from([id]));
                }
            }
            if values.iter().any(String::is_empty) && f.index_empty {
                index.empty.entry(name.into()).or_default().insert(id);
            }
        }
        (FieldIndex::Numeric(nums), IndexedValue::Numeric(values)) => {
            for &v in values {
                nums.entry(Number(v)).or_default().insert(id);
            }
        }
        (FieldIndex::Geo(g), IndexedValue::Geo(points)) => {
            g.insert(id, points);
        }
        (FieldIndex::Shape(g), IndexedValue::Geometry(wkt)) => g.insert(id, wkt)?,
        (FieldIndex::Vector(v), IndexedValue::Vector(values)) => v.insert(id, values)?,
        _ => return Err("Indexed value does not match schema".into()),
    }
    Ok(())
}
pub fn remove(index: &mut Index, id: DocId) {
    if let Some(doc) = index.docs.remove(&id) {
        index.keys.remove(&doc.key);
        for (name, old) in doc.fields {
            unindex(index, id, &name, &old)
        }
    }
}
// Graph undo restores exact original node IDs, links and quantization separately.
// Rebuilding vectors here could change approximate query results or touch new neighbors.
pub fn remove_for_rollback(index: &mut Index, id: DocId) {
    if let Some(doc) = index.docs.remove(&id) {
        index.keys.remove(&doc.key);
        for (name, old) in doc.fields {
            if matches!(index.fields.get(&name), Some(FieldIndex::Vector(_))) {
                if let Some(set) = index.missing.get_mut(&name) {
                    set.remove(&id);
                }
                if let Some(set) = index.empty.get_mut(&name) {
                    set.remove(&id);
                }
            } else {
                unindex(index, id, &name, &old)
            }
        }
    }
}
pub fn insert_for_rollback(
    index: &mut Index,
    id: DocId,
    name: &str,
    value: &IndexedValue,
) -> Result<(), String> {
    if matches!(index.fields.get(name), Some(FieldIndex::Vector(_)))
        && matches!(value, IndexedValue::Vector(_))
    {
        Ok(())
    } else {
        insert(index, id, name, value)
    }
}
pub fn synonyms(
    tokens: &mut Vec<Token>,
    groups: &BTreeMap<String, BTreeSet<String>>,
    phonetic: Option<&str>,
) {
    let mut extra = Vec::new();
    for token in tokens.iter().filter(|t| !t.term.starts_with('+')) {
        for (group, terms) in groups {
            if terms.contains(&token.term) {
                extra.push(Token {
                    term: format!("~{group}"),
                    ..token.clone()
                });
            }
        }
        if let Some(mode) = phonetic {
            for code in super::text::phonetic(&token.term, mode) {
                extra.push(Token {
                    term: format!("${code}"),
                    ..token.clone()
                });
            }
        }
    }
    tokens.extend(extra);
}
