//! Allocator-agnostic resident estimates, not allocator/RSS telemetry.
//!
//! Inline headers are counted by their enclosing owner; family helpers return
//! only separately allocated storage. Bytes exposes length, not backing-buffer
//! capacity, so bodies use visible lengths. B-tree node occupancy and hash-table
//! control bytes are estimated independently of allocator size classes. Store
//! caches belong to the process, never to an individual key.

use super::{Engine, Entry, Resp, Value};
use bytes::Bytes;
use std::mem::size_of;

pub(super) const DEFAULT_SAMPLES: usize = 5;

#[derive(Clone, Copy, Debug)]
pub struct MemoryStatistics {
    pub allocated_bytes: u64,
    pub keys: u64,
}

pub(super) fn allocation<T>(capacity: usize) -> usize {
    capacity.saturating_mul(size_of::<T>())
}

pub(super) fn hash_bytes<K, V>(capacity: usize) -> usize {
    if capacity == 0 {
        return 0;
    }
    // HashMap reports usable capacity, not raw bucket count (7/8 load factor).
    let buckets = capacity.saturating_mul(8).saturating_add(6) / 7;
    allocation::<(K, V)>(buckets)
        .saturating_add(buckets)
        .saturating_add(16)
}

pub(super) fn tree_bytes<T>(len: usize) -> usize {
    if len == 0 {
        return 0;
    }
    // Rust B-trees hold up to eleven entries per node. Estimate typical
    // half-full nodes, including parent/edge pointers and unused entry slots.
    let nodes = len.saturating_add(5) / 6;
    nodes.saturating_mul(allocation::<T>(11).saturating_add(allocation::<usize>(14)))
}

pub(super) fn sampled<I, F>(items: I, samples: usize, mut bytes: F) -> usize
where
    I: ExactSizeIterator,
    F: FnMut(I::Item) -> usize,
{
    let len = items.len();
    let count = if samples == 0 { len } else { samples.min(len) };
    if count == 0 {
        return 0;
    }
    let total = items
        .take(count)
        .fold(0usize, |sum, item| sum.saturating_add(bytes(item)));
    // Quotient/remainder scaling avoids overflowing before division.
    (total / count)
        .saturating_mul(len)
        .saturating_add((total % count).saturating_mul(len) / count)
}

#[derive(Default)]
pub(super) struct Bodies {
    seen: [(usize, usize); 8],
    len: usize,
    overflow: std::collections::HashSet<(usize, usize)>,
}

impl Bodies {
    pub(super) fn allocation(&mut self, identity: (usize, usize), bytes: usize) -> usize {
        if bytes == 0
            || self.seen[..self.len].contains(&identity)
            || self.overflow.contains(&identity)
        {
            return 0;
        }
        if self.len < self.seen.len() {
            self.seen[self.len] = identity;
            self.len += 1;
        } else if self.overflow.len() < 262_144 - self.seen.len() {
            self.overflow.insert(identity);
        }
        bytes
    }

    pub(super) fn bytes(&mut self, value: &Bytes) -> usize {
        self.allocation((value.as_ptr() as usize, value.len()), value.len())
    }
}

fn value_allocations(value: &Value, samples: usize) -> usize {
    match value {
        Value::String(value) => value.len(),
        Value::List(values) => {
            let mut bodies = Bodies::default();
            allocation::<crate::storage::Payload>(values.capacity()).saturating_add(sampled(
                values.iter(),
                samples,
                |value| bodies.allocation(value.memory_identity(), value.memory_footprint()),
            ))
        }
        Value::Set(value) => value.memory_footprint(samples),
        Value::Hash(value) => value.memory_footprint(samples),
        Value::SortedSet(value) => value.memory_footprint(samples),
        Value::Queue(value) => {
            size_of::<crate::queue::Queue>().saturating_add(value.memory_footprint(samples))
        }
        Value::Typed(value) => {
            size_of::<super::typed::Data>().saturating_add(value.memory_footprint(samples))
        }
    }
}

fn share(bytes: usize, owners: usize) -> usize {
    if owners == 0 {
        0
    } else {
        bytes.div_ceil(owners)
    }
}

impl Engine {
    /// Per-slot estimates use resident capacities, never cold payload reads.
    /// Cluster has one logical database; selection by another client is irrelevant.
    pub(crate) fn fill_slot_memory(&self, now: i64, slots: &mut [u64]) {
        let entries = if self.active_database == 0 {
            &self.entries
        } else if let Some(database) = self.databases.get(&0) {
            &database.entries
        } else {
            return;
        };
        for (key, entry) in entries {
            if !Self::entry_live(entry, now) {
                continue;
            }
            let bytes = size_of::<(Bytes, Entry)>()
                .saturating_add(key.len())
                .saturating_add(value_allocations(&entry.value, DEFAULT_SAMPLES));
            let slot = usize::from(crate::cluster::key_slot(key));
            if let Some(total) = slots.get_mut(slot) {
                *total = total.saturating_add(bytes as u64);
            }
        }
    }

    pub fn memory_usage(
        &mut self,
        key: &Bytes,
        samples: usize,
        now: i64,
    ) -> Result<Option<usize>, Resp> {
        self.discard_expired(key, now);
        Ok(self.entries.get_key_value(key).map(|(key, entry)| {
            key.len()
                .saturating_add(value_allocations(&entry.value, samples))
                .saturating_add(share(
                    hash_bytes::<Bytes, Entry>(self.entries.capacity()),
                    self.entries.len(),
                ))
                .saturating_add(share(
                    tree_bytes::<(u64, Bytes)>(self.key_index.len()),
                    self.key_index.len(),
                ))
                .saturating_add(if entry.expires_at.is_some() {
                    share(
                        tree_bytes::<(i64, Bytes)>(self.expirations.len()),
                        self.expirations.len(),
                    )
                } else {
                    0
                })
                .saturating_add(if self.hash_deadline(key).is_some() {
                    share(
                        tree_bytes::<(i64, Bytes)>(self.hash_expirations.len()),
                        self.hash_expirations.len(),
                    )
                } else {
                    0
                })
                .saturating_add(if self.queue_deadline(key).is_some() {
                    share(
                        tree_bytes::<(i64, Bytes)>(self.queue_expirations.len()),
                        self.queue_expirations.len(),
                    )
                } else {
                    0
                })
        }))
    }

    /// All-database logical key count and resident owned storage estimate.
    /// Shared Store caches are deliberately excluded; callers report them once
    /// from Store statistics rather than multiplying them by database/shard.
    pub fn memory_statistics(&mut self, now: i64) -> Result<MemoryStatistics, Resp> {
        let active = self.active_database;
        // Only database identifiers are copied, never values or cold bodies.
        let databases: Vec<_> = std::iter::once(active)
            .chain(self.databases.keys().copied())
            .collect();
        let mut count = 0usize;
        let mut bytes = size_of::<Engine>().saturating_add(
            tree_bytes::<(u32, super::DatabaseState)>(self.databases.len()),
        );
        for database in databases {
            self.switch_database(database);
            self.expire_at(now, usize::MAX);
            self.expire_hashes(now, usize::MAX);
            count = count.saturating_add(self.entries.len());
            bytes = bytes
                .saturating_add(hash_bytes::<Bytes, Entry>(self.entries.capacity()))
                .saturating_add(hash_bytes::<Bytes, Entry>(self.held_entries.capacity()))
                .saturating_add(tree_bytes::<Bytes>(self.retained_hashes.len()))
                .saturating_add(tree_bytes::<(u64, Bytes)>(self.key_index.len()))
                .saturating_add(tree_bytes::<(i64, Bytes)>(self.expirations.len()))
                .saturating_add(tree_bytes::<(i64, Bytes)>(self.hash_expirations.len()))
                .saturating_add(tree_bytes::<(i64, Bytes)>(self.queue_expirations.len()));
            for (key, entry) in self.entries.iter().chain(&self.held_entries) {
                bytes = bytes
                    .saturating_add(key.len())
                    .saturating_add(value_allocations(&entry.value, 0));
            }
        }
        self.switch_database(active);
        if self.owns_global_metadata {
            bytes = bytes
                .saturating_add(self.function_libraries.len())
                .saturating_add(
                    self.native_position
                        .as_ref()
                        .map_or(0, |(history, _)| history.len()),
                );
            if let Some(auxiliary) = &self.rdb_auxiliary {
                let mut bodies = Bodies::default();
                bytes = bytes
                    .saturating_add(size_of::<std::collections::BTreeMap<Bytes, Bytes>>())
                    .saturating_add(2 * size_of::<usize>())
                    .saturating_add(tree_bytes::<(Bytes, Bytes)>(auxiliary.len()))
                    .saturating_add(sampled(auxiliary.iter(), 0, |(key, value)| {
                        bodies.bytes(key).saturating_add(bodies.bytes(value))
                    }));
            }
            if let Some(catalog) = &self.search {
                bytes = bytes.saturating_add(catalog.memory_footprint());
            }
        }
        Ok(MemoryStatistics {
            allocated_bytes: bytes as u64,
            keys: count as u64,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampling_is_bounded_and_zero_walks_every_item() {
        let values = [3usize, 3, 3, 3, 3, 3];
        let mut visited = 0;
        assert_eq!(
            sampled(values.iter(), 2, |value| {
                visited += 1;
                *value
            }),
            18
        );
        assert_eq!(visited, 2);
        assert_eq!(sampled(values.iter(), 0, |value| *value), 18);
        assert_eq!(allocation::<u64>(usize::MAX), usize::MAX);
    }

    #[test]
    fn expired_hash_fields_are_pruned_before_measurement() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"key");
        let body = "x".repeat(8192);
        command(
            &mut engine,
            &["HSET", "key", "large", &body, "retained", "small"],
        );
        command(
            &mut engine,
            &["HPEXPIRE", "key", "1", "FIELDS", "1", "large"],
        );
        let before = engine.memory_usage(&key, 0, 1000).unwrap().unwrap();
        let after = engine.memory_usage(&key, 0, 1001).unwrap().unwrap();
        assert!(after < before);
        assert_eq!(engine.memory_statistics(1001).unwrap().keys, 1);
    }

    #[test]
    fn statistics_include_inactive_databases_and_restore_selection() {
        let mut engine = Engine::new();
        command(&mut engine, &["SET", "first", "value"]);
        engine.switch_database(7);
        command(&mut engine, &["SET", "second", "value"]);
        command(&mut engine, &["PEXPIRE", "second", "1"]);
        engine.switch_database(0);
        assert_eq!(engine.memory_statistics(1000).unwrap().keys, 2);
        assert_eq!(engine.database(), 0);
        assert_eq!(engine.memory_statistics(1001).unwrap().keys, 1);
        assert_eq!(engine.database(), 0);
        assert!(
            engine
                .memory_usage(&Bytes::from_static(b"first"), 0, 1001)
                .unwrap()
                .is_some()
        );
        assert!(
            engine
                .memory_usage(&Bytes::from_static(b"second"), 0, 1001)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn json_shared_subtrees_count_one_owned_body() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"key");
        command(
            &mut engine,
            &["JSON.SET", "key", "$", r#"{"a":null,"b":null}"#],
        );
        let before = engine.memory_usage(&key, 0, 1000).unwrap().unwrap();
        let body = format!("\"{}\"", "x".repeat(8192));
        command(&mut engine, &["JSON.SET", "key", "$.*", &body]);
        let after = engine.memory_usage(&key, 0, 1000).unwrap().unwrap();
        assert!(after > before);
        assert!(after < before.saturating_add(2 * body.len()));
    }

    fn command(engine: &mut Engine, args: &[&str]) {
        let args: Vec<_> = args
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect();
        assert!(!matches!(engine.execute_at(&args, 1000), Resp::Error(_)));
    }

    #[test]
    fn owned_bodies_grow_shrink_and_expire() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"key");
        command(&mut engine, &["SET", "key", "small"]);
        let small = engine.memory_usage(&key, 0, 1000).unwrap().unwrap();
        let large = "x".repeat(8192);
        command(&mut engine, &["SET", "key", &large]);
        assert!(engine.memory_usage(&key, 0, 1000).unwrap().unwrap() > small);
        command(&mut engine, &["SET", "key", "small"]);
        assert_eq!(engine.memory_usage(&key, 0, 1000).unwrap(), Some(small));
        command(&mut engine, &["PEXPIRE", "key", "1"]);
        assert_eq!(engine.memory_usage(&key, 0, 1001).unwrap(), None);
        assert_eq!(engine.memory_statistics(1001).unwrap().keys, 0);
    }

    #[test]
    fn nested_collection_bodies_are_not_just_headers() {
        for (create, add) in [
            (
                vec!["HSET", "key", "field", "small"],
                vec!["HSET", "key", "field"],
            ),
            (vec!["SADD", "key", "small"], vec!["SADD", "key"]),
            (vec!["ZADD", "key", "1", "small"], vec!["ZADD", "key", "1"]),
            (vec!["RPUSH", "key", "small"], vec!["RPUSH", "key"]),
        ] {
            let mut engine = Engine::new();
            let key = Bytes::from_static(b"key");
            command(&mut engine, &create);
            let before = engine.memory_usage(&key, 0, 1000).unwrap().unwrap();
            let large = "x".repeat(8192);
            let mut args = add;
            args.push(&large);
            command(&mut engine, &args);
            assert!(engine.memory_usage(&key, 0, 1000).unwrap().unwrap() > before);
        }
    }
}
