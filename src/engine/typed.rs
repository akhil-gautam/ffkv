//! One durable seam for the extended datatype families.
pub(crate) mod codec;

use super::{Engine, Entry, Value, arrays, filters, json, sketches, streams, timeseries, vectors};
use bytes::Bytes;
use std::io;

pub(super) enum Data {
    Array(arrays::ArrayValue),
    Json(json::JsonValue),
    Stream(streams::StreamValue),
    TimeSeries(timeseries::TimeSeriesValue),
    Bloom(filters::BloomValue),
    Cuckoo(filters::CuckooValue),
    Cms(sketches::CmsValue),
    TopK(sketches::TopKValue),
    TDigest(sketches::TDigestValue),
    Vector(vectors::VectorValue),
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid datatype kind or effect",
    )
}

impl Data {
    /// Heap storage beyond the enclosing boxed Data allocation.
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        match self {
            Self::Array(value) => value.memory_footprint(samples),
            Self::Json(value) => value.memory_footprint(samples),
            Self::Stream(value) => value.memory_footprint(samples),
            Self::TimeSeries(value) => value.memory_footprint(samples),
            Self::Bloom(value) => value.memory_footprint(samples),
            Self::Cuckoo(value) => value.memory_footprint(samples),
            Self::Cms(value) => value.memory_footprint(samples),
            Self::TopK(value) => value.memory_footprint(samples),
            Self::TDigest(value) => value.memory_footprint(samples),
            Self::Vector(value) => value.memory_footprint(samples),
        }
    }

    pub(super) fn kind(&self) -> u8 {
        match self {
            Self::Array(_) => 1,
            Self::Json(_) => 2,
            Self::Stream(_) => 3,
            Self::TimeSeries(_) => 4,
            Self::Bloom(_) => 5,
            Self::Cuckoo(_) => 6,
            Self::Cms(_) => 7,
            Self::TopK(_) => 8,
            Self::TDigest(_) => 9,
            Self::Vector(_) => 10,
        }
    }
    pub(super) fn type_name(&self) -> &'static str {
        match self {
            Self::Array(_) => "array",
            Self::Json(_) => "ReJSON-RL",
            Self::Stream(_) => "stream",
            Self::TimeSeries(_) => "TSDB-TYPE",
            Self::Bloom(_) => "MBbloom--",
            Self::Cuckoo(_) => "MBbloomCF",
            Self::Cms(_) => "CMSk-TYPE",
            Self::TopK(_) => "TopK-TYPE",
            Self::TDigest(_) => "TDIS-TYPE",
            Self::Vector(_) => "vectorset",
        }
    }
    pub(super) fn restore(kind: u8, effect: &[u8]) -> io::Result<Self> {
        if effect.is_empty() || effect.len() > codec::MAX_EFFECT_BYTES {
            return Err(invalid());
        }
        Ok(match kind {
            1 => Self::Array(arrays::ArrayValue::restore(effect)?),
            2 => Self::Json(json::JsonValue::restore(effect)?),
            3 => Self::Stream(streams::StreamValue::restore(effect)?),
            4 => Self::TimeSeries(timeseries::TimeSeriesValue::restore(effect)?),
            5 => Self::Bloom(filters::BloomValue::restore(effect)?),
            6 => Self::Cuckoo(filters::CuckooValue::restore(effect)?),
            7 => Self::Cms(sketches::CmsValue::restore(effect)?),
            8 => Self::TopK(sketches::TopKValue::restore(effect)?),
            9 => Self::TDigest(sketches::TDigestValue::restore(effect)?),
            10 => Self::Vector(vectors::VectorValue::restore(effect)?),
            _ => return Err(invalid()),
        })
    }
    pub(super) fn apply(&mut self, kind: u8, effect: &[u8]) -> io::Result<()> {
        if kind != self.kind() || effect.is_empty() || effect.len() > codec::MAX_EFFECT_BYTES {
            return Err(invalid());
        }
        match self {
            Self::Array(value) => value.apply(effect),
            Self::Json(value) => value.apply(effect),
            Self::Stream(value) => value.apply(effect),
            Self::TimeSeries(value) => value.apply(effect),
            Self::Bloom(value) => value.apply(effect),
            Self::Cuckoo(value) => value.apply(effect),
            Self::Cms(value) => value.apply(effect),
            Self::TopK(value) => value.apply(effect),
            Self::TDigest(value) => value.apply(effect),
            Self::Vector(value) => value.apply(effect),
        }
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        match self {
            Self::Array(value) => Snapshot::Array(value.snapshot()),
            Self::Json(value) => Snapshot::Json(value.snapshot()),
            Self::Stream(value) => Snapshot::Stream(value.snapshot()),
            Self::TimeSeries(value) => Snapshot::TimeSeries(value.snapshot()),
            Self::Bloom(value) => Snapshot::Filter(value.snapshot()),
            Self::Cuckoo(value) => Snapshot::Filter(value.snapshot()),
            Self::Cms(value) => Snapshot::Sketch(value.snapshot()),
            Self::TopK(value) => Snapshot::Sketch(value.snapshot()),
            Self::TDigest(value) => Snapshot::Sketch(value.snapshot()),
            Self::Vector(value) => Snapshot::Vector(value.snapshot()),
        }
    }
}

pub(super) enum Snapshot<'a> {
    Array(arrays::Snapshot<'a>),
    Json(json::Snapshot<'a>),
    Stream(streams::Snapshot<'a>),
    TimeSeries(timeseries::Snapshot<'a>),
    Filter(filters::Snapshot<'a>),
    Sketch(sketches::Snapshot<'a>),
    Vector(vectors::Snapshot<'a>),
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        let result = match self {
            Self::Array(iter) => iter.next(),
            Self::Json(iter) => iter.next(),
            Self::Stream(iter) => iter.next(),
            Self::TimeSeries(iter) => iter.next(),
            Self::Filter(iter) => iter.next(),
            Self::Sketch(iter) => iter.next(),
            Self::Vector(iter) => iter.next(),
        };
        result.map(|effect| {
            effect.and_then(|effect| {
                if effect.is_empty() || effect.len() > codec::MAX_EFFECT_BYTES {
                    Err(invalid())
                } else {
                    Ok(effect)
                }
            })
        })
    }
}

impl Engine {
    pub(super) fn record_typed(&mut self, key: &Bytes, kind: u8, effect: Bytes, now: i64) {
        if !(1..=10).contains(&kind) || effect.is_empty() || effect.len() > codec::MAX_EFFECT_BYTES
        {
            self.failure = Some(invalid());
            return;
        }
        self.record(now, || crate::journal::Mutation::Typed {
            key: key.clone(),
            kind,
            effect,
        });
    }
    pub(super) fn replay_typed(
        &mut self,
        key: Bytes,
        kind: u8,
        effect: Bytes,
        now: i64,
    ) -> io::Result<()> {
        self.discard_expired(&key, now);
        if let Some(entry) = self.entries.get_mut(&key) {
            let Value::Typed(value) = &mut entry.value else {
                return Err(invalid());
            };
            value.apply(kind, &effect)
        } else {
            let value = Data::restore(kind, &effect)?;
            self.insert_entry_raw(
                key,
                Entry {
                    value: Value::Typed(Box::new(value)),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
            );
            Ok(())
        }
    }
}
