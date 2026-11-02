//! Bounded logical export while the owning actor/shard is paused.
//! Dataset and global durable metadata are exported without copying whole values.

use super::{Engine, Entry, Value, hashes, sets, sorted_sets};
use crate::{
    journal::{Batch, Mutation},
    storage::Payload,
};
use bytes::Bytes;
use std::{
    collections::{btree_map, hash_map, vec_deque},
    io,
};

const MAX_ROWS: usize = 1024;
const TARGET_BYTES: usize = 8 * 1024 * 1024;

/// Borrows a frozen engine; callers must hold the cross-shard pause for its lifetime.
/// Bytes are reference-counted, and cold payloads are read only as their batch is built.
/// An indivisible row (e.g. an 8MiB job plus metadata) can exceed the byte target.
/// Any IO error terminates export; a partially built batch is never returned.
pub struct Snapshot<'a> {
    entries: std::iter::Chain<hash_map::Iter<'a, Bytes, Entry>, hash_map::Iter<'a, Bytes, Entry>>,
    current: Option<Current<'a>>,
    pending: Option<Row<'a>>,
    now_ms: i64,
    failed: bool,
}

/// A frozen multi-database view. Global records are owned by exactly one shard.
pub struct DatabaseSnapshot<'a> {
    data: Snapshot<'a>,
    database: u32,
    databases: btree_map::Iter<'a, u32, super::DatabaseState>,
    engine: &'a Engine,
    metadata_phase: u8,
    auxiliary: Option<btree_map::Iter<'a, Bytes, Bytes>>,
    search: Option<super::search::SearchSnapshot>,
    now_ms: i64,
    failed: bool,
}

impl Iterator for DatabaseSnapshot<'_> {
    type Item = io::Result<Batch>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        loop {
            if let Some(batch) = self.data.next() {
                return Some(match batch {
                    Ok(mut batch) => {
                        batch.mutations.insert(
                            0,
                            Mutation::SelectDatabase {
                                database: self.database,
                            },
                        );
                        Ok(batch)
                    }
                    Err(error) => {
                        self.failed = true;
                        Err(error)
                    }
                });
            }
            if let Some((database, state)) = self.databases.next() {
                self.database = *database;
                self.data = Snapshot {
                    entries: state.entries.iter().chain(state.held_entries.iter()),
                    current: None,
                    pending: None,
                    now_ms: self.now_ms,
                    failed: false,
                };
                continue;
            }
            if !self.engine.owns_global_metadata {
                return None;
            }
            let mutation = match self.metadata_phase {
                0 => {
                    self.metadata_phase = 1;
                    if self.engine.function_libraries.is_empty() {
                        continue;
                    }
                    Mutation::Functions {
                        snapshot: self.engine.function_libraries.clone(),
                    }
                }
                1 => {
                    self.metadata_phase = 2;
                    let Some((history, offset)) = &self.engine.native_position else {
                        continue;
                    };
                    Mutation::NativePosition {
                        history: history.clone(),
                        offset: *offset,
                    }
                }
                2 => {
                    let Some((key, value)) = self.auxiliary.as_mut().and_then(Iterator::next)
                    else {
                        self.metadata_phase = 3;
                        continue;
                    };
                    Mutation::RdbAuxiliary {
                        key: key.clone(),
                        value: value.clone(),
                    }
                }
                3 => {
                    let effect = self.search.as_mut().and_then(Iterator::next)?;
                    match effect {
                        Ok(effect) => Mutation::Search { effect },
                        Err(error) => {
                            self.failed = true;
                            return Some(Err(error));
                        }
                    }
                }
                _ => return None,
            };
            return Some(Ok(Batch {
                timestamp_ms: self.now_ms,
                mutations: vec![Mutation::SelectDatabase { database: 0 }, mutation],
            }));
        }
    }
}

impl Engine {
    pub fn snapshot_batches(&self, now_ms: i64) -> DatabaseSnapshot<'_> {
        // A follower may hide a deadline before receiving the source's resolved
        // deletion. Recovery must retain that payload at the acknowledged offset.
        let now_ms = if self.replica_mode { 0 } else { now_ms };
        DatabaseSnapshot {
            data: Snapshot {
                entries: self.entries.iter().chain(self.held_entries.iter()),
                current: None,
                pending: None,
                now_ms,
                failed: false,
            },
            database: self.active_database,
            databases: self.databases.iter(),
            engine: self,
            metadata_phase: 0,
            auxiliary: self.rdb_auxiliary.as_deref().map(|values| values.iter()),
            search: if self.owns_global_metadata && self.search.is_some() {
                Some(self.search_snapshot())
            } else {
                None
            },
            now_ms,
            failed: false,
        }
    }
}

enum Rows<'a> {
    String(Option<&'a Bytes>),
    List(vec_deque::Iter<'a, Payload>),
    Set(sets::Snapshot<'a>),
    Hash(hashes::Snapshot<'a>),
    SortedSet(sorted_sets::Snapshot<'a>),
    Queue(crate::queue::Snapshot<'a>),
    Typed(u8, super::typed::Snapshot<'a>),
}

struct Current<'a> {
    key: &'a Bytes,
    expires_at: Option<i64>,
    rows: Rows<'a>,
    emitted: bool,
}

impl<'a> Current<'a> {
    fn new(key: &'a Bytes, entry: &'a Entry, now_ms: i64) -> Self {
        let rows = match &entry.value {
            Value::String(value) => Rows::String(Some(value)),
            Value::List(values) => Rows::List(values.iter()),
            Value::Set(values) => Rows::Set(values.snapshot()),
            Value::Hash(values) => Rows::Hash(values.snapshot(now_ms)),
            Value::SortedSet(values) => Rows::SortedSet(values.snapshot()),
            Value::Queue(queue) => Rows::Queue(queue.snapshot_effects()),
            Value::Typed(value) => Rows::Typed(value.kind(), value.snapshot()),
        };
        Self {
            key,
            expires_at: entry.expires_at,
            rows,
            emitted: false,
        }
    }

    fn next(&mut self) -> io::Result<Option<Row<'a>>> {
        let key = self.key;
        let row = match &mut self.rows {
            Rows::String(value) => value.take().map(|value| {
                // SET carries its own TTL; collection TTLs follow their final row.
                Row::String(key, value, self.expires_at.take())
            }),
            Rows::List(values) => values.next().map(|value| Row::List(key, value)),
            Rows::Set(values) => values.next().map(|value| Row::Set(key, value)),
            Rows::Hash(values) => values
                .next()
                .map(|(field, value, deadline)| Row::Hash(key, field, value, deadline)),
            Rows::SortedSet(values) => values
                .next()
                .map(|(member, score)| Row::SortedSet(key, member, score)),
            Rows::Queue(values) => match values.next() {
                Some(effect) => Some(Row::Queue(key, effect?.encode()?)),
                None => None,
            },
            Rows::Typed(kind, values) => values
                .next()
                .transpose()?
                .map(|effect| Row::Typed(key, *kind, effect)),
        };
        self.emitted |= row.is_some();
        Ok(row)
    }
}

enum Row<'a> {
    String(&'a Bytes, &'a Bytes, Option<i64>),
    List(&'a Bytes, &'a Payload),
    Set(&'a Bytes, &'a Bytes),
    Hash(&'a Bytes, &'a Bytes, &'a Bytes, Option<i64>),
    SortedSet(&'a Bytes, &'a Bytes, f64),
    Queue(&'a Bytes, Bytes),
    Typed(&'a Bytes, u8, Bytes),
    Expire(&'a Bytes, i64),
}

impl Row<'_> {
    // Count keys and fixed metadata too, conservatively bounding logical WAL rows.
    fn bytes(&self) -> usize {
        match self {
            Self::String(key, value, _) => key.len().saturating_add(value.len()).saturating_add(16),
            Self::List(key, value) => key.len().saturating_add(value.len()).saturating_add(8),
            Self::Set(key, member) => key.len().saturating_add(member.len()).saturating_add(8),
            Self::Hash(key, field, value, _) => key
                .len()
                .saturating_add(field.len())
                .saturating_add(value.len())
                .saturating_add(24),
            Self::SortedSet(key, member, _) => {
                key.len().saturating_add(member.len()).saturating_add(16)
            }
            Self::Queue(key, effect) => key.len().saturating_add(effect.len()).saturating_add(8),
            Self::Typed(key, _, effect) => {
                key.len().saturating_add(effect.len()).saturating_add(10)
            }
            Self::Expire(key, _) => key.len().saturating_add(16),
        }
    }

    fn append(self, mutations: &mut Vec<Mutation>) -> io::Result<()> {
        match self {
            Self::String(key, value, expires_at) => mutations.push(Mutation::Set {
                key: key.clone(),
                value: value.clone(),
                expires_at,
            }),
            Self::List(key, payload) => {
                let value = payload.read()?;
                match mutations.last_mut() {
                    Some(Mutation::Push {
                        key: previous,
                        values,
                        right: true,
                    }) if previous == key => values.push(value),
                    _ => mutations.push(Mutation::Push {
                        key: key.clone(),
                        values: vec![value],
                        right: true,
                    }),
                }
            }
            Self::Set(key, member) => match mutations.last_mut() {
                Some(Mutation::SetAdd {
                    key: previous,
                    members,
                }) if previous == key => members.push(member.clone()),
                _ => mutations.push(Mutation::SetAdd {
                    key: key.clone(),
                    members: vec![member.clone()],
                }),
            },
            Self::Hash(key, field, value, expires_at) => {
                let field = (field.clone(), value.clone(), expires_at);
                match mutations.last_mut() {
                    Some(Mutation::HashSet {
                        key: previous,
                        fields,
                    }) if previous == key => fields.push(field),
                    _ => mutations.push(Mutation::HashSet {
                        key: key.clone(),
                        fields: vec![field],
                    }),
                }
            }
            Self::SortedSet(key, member, score) => match mutations.last_mut() {
                Some(Mutation::SortedSetAdd {
                    key: previous,
                    entries,
                }) if previous == key => entries.push((member.clone(), score)),
                _ => mutations.push(Mutation::SortedSetAdd {
                    key: key.clone(),
                    entries: vec![(member.clone(), score)],
                }),
            },
            Self::Queue(key, effect) => mutations.push(Mutation::Queue {
                key: key.clone(),
                effect,
            }),
            Self::Typed(key, kind, effect) => mutations.push(Mutation::Typed {
                key: key.clone(),
                kind,
                effect,
            }),
            Self::Expire(key, expires_at) => mutations.push(Mutation::Expire {
                key: key.clone(),
                expires_at: Some(expires_at),
            }),
        }
        Ok(())
    }
}

impl<'a> Snapshot<'a> {
    fn next_row(&mut self) -> io::Result<Option<Row<'a>>> {
        if let Some(row) = self.pending.take() {
            return Ok(Some(row));
        }
        loop {
            if let Some(current) = &mut self.current {
                if let Some(row) = current.next()? {
                    return Ok(Some(row));
                }
                // An all-expired hash creates no key, so it must not emit EXPIRE.
                let expiry = current.expires_at.filter(|_| current.emitted);
                let key = current.key;
                self.current = None;
                if let Some(deadline) = expiry {
                    return Ok(Some(Row::Expire(key, deadline)));
                }
            }
            let Some((key, entry)) = self.entries.next() else {
                return Ok(None);
            };
            if entry
                .expires_at
                .is_some_and(|deadline| deadline <= self.now_ms)
            {
                continue;
            }
            self.current = Some(Current::new(key, entry, self.now_ms));
        }
    }
}

impl Iterator for Snapshot<'_> {
    type Item = io::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }
        let mut mutations = Vec::new();
        let mut rows = 0;
        let mut bytes = 0usize;
        while rows < MAX_ROWS {
            let row = match self.next_row() {
                Ok(Some(row)) => row,
                Ok(None) => break,
                Err(error) => {
                    self.failed = true;
                    return Some(Err(error));
                }
            };
            let row_bytes = row.bytes();
            if rows != 0 && bytes.saturating_add(row_bytes) > TARGET_BYTES {
                self.pending = Some(row);
                break;
            }
            if let Err(error) = row.append(&mut mutations) {
                self.failed = true;
                return Some(Err(error));
            }
            rows += 1;
            bytes = bytes.saturating_add(row_bytes);
            if bytes >= TARGET_BYTES {
                break;
            }
        }
        (!mutations.is_empty()).then_some(Ok(Batch {
            timestamp_ms: self.now_ms,
            mutations,
        }))
    }
}

impl std::iter::FusedIterator for Snapshot<'_> {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        protocol::Resp,
        queue::{Operation, Output, Queue},
        storage::{Config, Store},
    };

    fn b(value: &str) -> Bytes {
        Bytes::copy_from_slice(value.as_bytes())
    }

    fn replay(engine: &mut Engine, mutations: Vec<Mutation>) {
        engine
            .replay(Batch {
                timestamp_ms: 1,
                mutations,
            })
            .unwrap();
    }

    fn restore(engine: &Engine, now: i64) -> Engine {
        let mut restored = Engine::new();
        for batch in engine.snapshot_batches(now) {
            restored.replay(batch.unwrap()).unwrap();
        }
        restored
    }

    fn command(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
        let args: Vec<_> = args.iter().map(|arg| b(arg)).collect();
        match engine.execute_at_time(&args, now).unwrap() {
            super::super::Execution::Reply(reply)
            | super::super::Execution::Pushed { reply, .. } => reply,
            _ => panic!("unexpected blocking snapshot test command"),
        }
    }

    #[test]
    fn replay_preserves_binary_values_order_scores_and_actual_field_ttls() {
        let mut source = Engine::new();
        let binary = Bytes::from_static(b"\0\xff\r\n");
        replay(
            &mut source,
            vec![
                Mutation::Set {
                    key: b("string"),
                    value: binary.clone(),
                    expires_at: Some(200),
                },
                Mutation::Push {
                    key: b("list"),
                    values: vec![binary.clone(), b("second"), b("third")],
                    right: true,
                },
                Mutation::SetAdd {
                    key: b("set"),
                    members: vec![binary.clone(), b("member")],
                },
                Mutation::HashSet {
                    key: b("hash"),
                    fields: vec![
                        (b("forever"), binary.clone(), None),
                        (b("future"), b("yes"), Some(150)),
                        (b("boundary"), b("no"), Some(100)),
                        (b("past"), b("no"), Some(99)),
                    ],
                },
                Mutation::SortedSetAdd {
                    key: b("zset"),
                    entries: vec![
                        (binary, -2.5),
                        (b("positive"), 1.25),
                        (b("infinite"), f64::INFINITY),
                    ],
                },
            ],
        );
        for key in ["list", "set", "hash", "zset"] {
            replay(
                &mut source,
                vec![Mutation::Expire {
                    key: b(key),
                    expires_at: Some(300),
                }],
            );
        }
        let batches: Vec<_> = source.snapshot_batches(100).map(Result::unwrap).collect();
        let fields: Vec<_> = batches
            .iter()
            .flat_map(|batch| &batch.mutations)
            .filter_map(|mutation| {
                if let Mutation::HashSet { fields, .. } = mutation {
                    Some(fields)
                } else {
                    None
                }
            })
            .flatten()
            .collect();
        assert_eq!(fields.len(), 2);
        assert!(
            fields
                .iter()
                .any(|(field, _, deadline)| field == &b("future") && *deadline == Some(150))
        );
        assert!(
            fields
                .iter()
                .any(|(field, _, deadline)| field == &b("forever") && deadline.is_none())
        );
        let mut recovered = Engine::new();
        for batch in batches {
            recovered.replay(batch).unwrap();
        }
        for args in [
            vec!["get", "string"],
            vec!["lrange", "list", "0", "-1"],
            vec!["smismember", "set", "member", "absent"],
            vec!["hmget", "hash", "forever", "future", "boundary", "past"],
            vec!["zrange", "zset", "0", "-1", "withscores"],
        ] {
            assert_eq!(
                command(&mut source, 100, &args),
                command(&mut recovered, 100, &args)
            );
        }
        for key in ["string", "list", "set", "hash", "zset"] {
            assert_eq!(
                command(&mut source, 100, &["pexpiretime", key]),
                command(&mut recovered, 100, &["pexpiretime", key])
            );
        }
        assert_eq!(
            command(&mut recovered, 150, &["hexists", "hash", "future"]),
            Resp::Integer(0)
        );
        assert_eq!(
            command(&mut recovered, 200, &["exists", "string"]),
            Resp::Integer(0)
        );
        assert_eq!(
            command(
                &mut recovered,
                300,
                &["exists", "list", "set", "hash", "zset"]
            ),
            Resp::Integer(0)
        );
    }

    #[test]
    fn exact_key_and_field_expiry_skips_missing_hash_expire_mutation() {
        let mut source = Engine::new();
        replay(
            &mut source,
            vec![
                Mutation::Set {
                    key: b("expired"),
                    value: b("body"),
                    expires_at: Some(100),
                },
                Mutation::HashSet {
                    key: b("all-expired"),
                    fields: vec![(b("field"), b("body"), Some(100))],
                },
                Mutation::Expire {
                    key: b("all-expired"),
                    expires_at: Some(200),
                },
            ],
        );
        assert!(restore(&source, 100).entries.is_empty());
        let mut before = restore(&source, 99);
        assert_eq!(
            command(&mut before, 99, &["exists", "expired", "all-expired"]),
            Resp::Integer(2)
        );
        assert_eq!(
            command(&mut before, 100, &["exists", "expired", "all-expired"]),
            Resp::Integer(0)
        );
    }

    #[test]
    fn large_collection_snapshots_restore_contents_and_expiration() {
        for kind in 0..4 {
            let mut source = Engine::new();
            let values: Vec<_> = (0..2500).map(|i| b(&format!("row-{i:04}"))).collect();
            let mutation = match kind {
                0 => Mutation::Push {
                    key: b("key"),
                    values,
                    right: true,
                },
                1 => Mutation::SetAdd {
                    key: b("key"),
                    members: values,
                },
                2 => Mutation::HashSet {
                    key: b("key"),
                    fields: values
                        .into_iter()
                        .map(|field| (field, b("value"), Some(200)))
                        .collect(),
                },
                _ => Mutation::SortedSetAdd {
                    key: b("key"),
                    entries: values
                        .into_iter()
                        .enumerate()
                        .map(|(i, member)| (member, i as f64))
                        .collect(),
                },
            };
            replay(
                &mut source,
                vec![
                    mutation,
                    Mutation::Expire {
                        key: b("key"),
                        expires_at: Some(300),
                    },
                ],
            );
            let mut recovered = restore(&source, 100);
            assert_eq!(
                command(&mut recovered, 100, &["PTTL", "key"]),
                Resp::Integer(200)
            );
            let read = match kind {
                0 => vec!["LRANGE", "key", "0", "-1"],
                1 => vec!["SMEMBERS", "key"],
                2 => vec!["HGETALL", "key"],
                _ => vec!["ZRANGE", "key", "0", "-1", "WITHSCORES"],
            };
            let actual = command(&mut recovered, 100, &read);
            let expected = command(&mut source, 100, &read);
            match (actual, expected) {
                (Resp::Map(actual), Resp::Map(expected)) => {
                    let fields = |rows: Vec<(Resp, Resp)>| {
                        rows.into_iter()
                            .map(|(key, value)| {
                                let (Resp::Bulk(Some(key)), Resp::Bulk(Some(value))) = (key, value)
                                else {
                                    panic!("hash field and value must be byte strings")
                                };
                                (key, value)
                            })
                            .collect::<std::collections::BTreeMap<_, _>>()
                    };
                    assert_eq!(fields(actual), fields(expected));
                }
                (Resp::Set(actual), Resp::Set(expected)) => {
                    let members = |rows: Vec<Resp>| {
                        rows.into_iter()
                            .map(|value| {
                                let Resp::Bulk(Some(value)) = value else {
                                    panic!("set member must be a byte string")
                                };
                                value
                            })
                            .collect::<std::collections::BTreeSet<_>>()
                    };
                    assert_eq!(members(actual), members(expected));
                }
                (actual, expected) => assert_eq!(actual, expected),
            }
            let size = match kind {
                0 => "llen",
                1 => "scard",
                2 => "hlen",
                _ => "zcard",
            };
            assert_eq!(
                command(&mut recovered, 100, &[size, "key"]),
                Resp::Integer(2500)
            );
            assert_eq!(
                command(&mut recovered, 300, &["EXISTS", "key"]),
                Resp::Integer(0)
            );
        }
    }

    #[test]
    fn large_binary_collection_and_single_value_roundtrip() {
        let mut source = Engine::new();
        let body = Bytes::from(vec![17; 3 * 1024 * 1024]);
        replay(
            &mut source,
            vec![Mutation::Push {
                key: b("key"),
                values: vec![body.clone(); 3],
                right: true,
            }],
        );
        let mut recovered = restore(&source, 100);
        assert_eq!(
            command(&mut recovered, 100, &["LRANGE", "key", "0", "-1"]),
            Resp::Array(Some(
                (0..3).map(|_| Resp::Bulk(Some(body.clone()))).collect()
            ))
        );
        let huge = Bytes::from(vec![19; TARGET_BYTES]);
        let mut source = Engine::new();
        replay(
            &mut source,
            vec![Mutation::Set {
                key: b("huge"),
                value: huge.clone(),
                expires_at: None,
            }],
        );
        assert_eq!(
            command(&mut restore(&source, 100), 100, &["strlen", "huge"]),
            Resp::Integer(TARGET_BYTES as i64)
        );
    }

    fn operation(engine: &mut Engine, now: i64, op: Operation) -> Output {
        engine
            .native_operation(&b("native"), op, now)
            .unwrap()
            .unwrap()
    }

    fn enqueue(
        engine: &mut Engine,
        now: i64,
        idem: Option<Bytes>,
        at_ms: Option<i64>,
        max_attempts: Option<u32>,
    ) -> Bytes {
        match operation(
            engine,
            now,
            Operation::Enqueue {
                payload: b("job-body"),
                idempotency: idem,
                at_ms,
                priority: 2,
                max_attempts,
            },
        ) {
            Output::Enqueued {
                job_id,
                created: true,
            } => job_id,
            other => panic!("unexpected enqueue {other:?}"),
        }
    }

    fn claim(engine: &mut Engine, now: i64) -> crate::queue::ClaimedJob {
        let Output::Claimed(mut jobs) = operation(
            engine,
            now,
            Operation::Claim {
                count: 1,
                visibility_ms: 1000,
            },
        ) else {
            panic!("claim");
        };
        jobs.pop().unwrap()
    }

    fn queue(engine: &Engine) -> &Queue {
        let Value::Queue(queue) = &engine.entries[&b("native")].value else {
            panic!("queue");
        };
        queue
    }

    #[test]
    fn native_snapshot_preserves_seal_epoch_receipts_generations_dead_and_terminal_idempotency() {
        let mut source = Engine::new();
        let terminal = enqueue(&mut source, 10, Some(b("terminal-token")), None, None);
        let completed = claim(&mut source, 11);
        operation(
            &mut source,
            12,
            Operation::Ack {
                receipt: completed.receipt,
            },
        );
        let dead = enqueue(&mut source, 13, None, None, Some(1));
        let first = claim(&mut source, 14);
        operation(
            &mut source,
            15,
            Operation::Nack {
                receipt: first.receipt.clone(),
                delay_ms: None,
                error: Some(b("failure")),
            },
        );
        operation(
            &mut source,
            16,
            Operation::Retry {
                job_id: dead.clone(),
                delay_ms: 0,
            },
        );
        let second = claim(&mut source, 17);
        operation(
            &mut source,
            18,
            Operation::Nack {
                receipt: second.receipt,
                delay_ms: None,
                error: Some(b("dead again")),
            },
        );
        enqueue(&mut source, 19, Some(b("running-token")), None, None);
        let running = claim(&mut source, 20);
        enqueue(&mut source, 21, None, Some(500), None);
        enqueue(&mut source, 22, None, None, None);
        replay(
            &mut source,
            vec![Mutation::Expire {
                key: b("native"),
                expires_at: Some(2000),
            }],
        );
        let mut recovered = restore(&source, 100);
        queue(&recovered).finish_recovery().unwrap();
        assert_eq!(queue(&recovered).stats(100).dead, 1);
        assert_eq!(queue(&recovered).stats(100).running, 1);
        assert_eq!(
            operation(
                &mut recovered,
                100,
                Operation::Enqueue {
                    payload: b("ignored"),
                    idempotency: Some(b("terminal-token")),
                    at_ms: None,
                    priority: 0,
                    max_attempts: None
                }
            ),
            Output::Enqueued {
                job_id: terminal,
                created: false
            }
        );
        let stale = queue(&recovered);
        assert_eq!(stale.stats(100).totals.claimed, 4);
        let error = recovered
            .native_operation(
                &b("native"),
                Operation::Ack {
                    receipt: first.receipt,
                },
                100,
            )
            .unwrap();
        assert!(error.is_err());
        assert_eq!(
            operation(
                &mut recovered,
                100,
                Operation::Ack {
                    receipt: running.receipt
                }
            ),
            Output::Ok
        );
        assert_eq!(
            operation(&mut recovered, 100, Operation::Get { job_id: dead }),
            operation(
                &mut source,
                100,
                Operation::Get {
                    job_id: first.job_id
                }
            )
        );
        assert_eq!(
            command(&mut recovered, 100, &["pexpiretime", "native"]),
            Resp::Integer(2000)
        );
        assert_eq!(
            command(&mut recovered, 2000, &["exists", "native"]),
            Resp::Integer(0)
        );
    }

    #[test]
    fn native_restore_preserves_large_queue_order_and_empty_queue_metadata() {
        let mut source = Engine::new();
        for _ in 0..1100 {
            enqueue(&mut source, 10, None, None, None);
        }
        let mut recovered = restore(&source, 100);
        queue(&recovered).finish_recovery().unwrap();
        assert_eq!(queue(&source).stats(100), queue(&recovered).stats(100));
        let original = claim(&mut source, 100);
        let restored = claim(&mut recovered, 100);
        assert_eq!(original.job_id, restored.job_id);
        assert_eq!(original.payload, restored.payload);
        assert_eq!(original.attempts, restored.attempts);
        assert_eq!(original.deadline_ms, restored.deadline_ms);

        let mut empty = Engine::new();
        let terminal = enqueue(&mut empty, 10, Some(b("terminal")), None, None);
        let job = claim(&mut empty, 11);
        operation(
            &mut empty,
            12,
            Operation::Ack {
                receipt: job.receipt,
            },
        );
        let mut recovered = restore(&empty, 100);
        assert_eq!(queue(&empty).stats(100), queue(&recovered).stats(100));
        assert_eq!(queue(&recovered).stats(100).jobs, 0);
        assert_eq!(
            operation(
                &mut recovered,
                100,
                Operation::Enqueue {
                    payload: b("ignored"),
                    idempotency: Some(b("terminal")),
                    at_ms: None,
                    priority: 0,
                    max_attempts: None,
                }
            ),
            Output::Enqueued {
                job_id: terminal,
                created: false
            }
        );
    }

    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!(
                "ffkv-snapshot-{}-{:032x}",
                std::process::id(),
                rand::random::<u128>()
            )))
        }
        fn store(&self) -> std::sync::Arc<Store> {
            Store::open(Config {
                directory: self.0.clone(),
                inline_threshold: 0,
                cache_bytes: 0,
                segment_bytes: 4096,
                disk_budget: 0,
            })
            .unwrap()
        }
        fn corrupt(&self) {
            for entry in std::fs::read_dir(&self.0).unwrap() {
                let entry = entry.unwrap();
                if entry.path().extension().is_some_and(|ext| ext == "seg") {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(entry.path())
                        .unwrap()
                        .set_len(0)
                        .unwrap();
                }
            }
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    #[test]
    fn hybrid_exports_logical_bytes_and_propagates_real_list_and_native_read_failures() {
        for native in [false, true] {
            let directory = Directory::new();
            let store = directory.store();
            let mut source = Engine::new();
            source.storage = Some(store.clone());
            if native {
                enqueue(&mut source, 10, None, None, None);
            } else {
                replay(
                    &mut source,
                    vec![Mutation::Push {
                        key: b("list"),
                        values: vec![b("job-body"); 3],
                        right: true,
                    }],
                );
                let Value::List(list) = &source.entries[&b("list")].value else {
                    panic!("list");
                };
                assert!(list.iter().all(Payload::is_disk));
            }
            let mut recovered = restore(&source, 100);
            if native {
                assert_eq!(queue(&source).stats(100), queue(&recovered).stats(100));
                assert_eq!(claim(&mut recovered, 100).payload, b("job-body"));
            } else {
                assert_eq!(
                    command(&mut recovered, 100, &["lrange", "list", "0", "-1"]),
                    Resp::Array(Some(
                        (0..3).map(|_| Resp::Bulk(Some(b("job-body")))).collect()
                    ))
                );
                let Value::List(list) = &recovered.entries[&b("list")].value else {
                    panic!("list");
                };
                assert!(list.iter().all(|payload| !payload.is_disk()));
            }
            directory.corrupt();
            let mut snapshot = source.snapshot_batches(100);
            let error = snapshot
                .next()
                .expect("failure must not look like EOF")
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(snapshot.next().is_none(), "failed snapshots are terminal");
        }
    }
}
