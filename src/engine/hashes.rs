use std::{
    collections::{BTreeSet, HashMap},
    io,
};

use bytes::Bytes;

use super::scan::{ScanIndex, parse_scan, scan_entries, stable_hash};
use super::{
    CommandSpec, Engine, Mutation, Resp, Value, array, error, integer, integer_error,
    invalid_journal, syntax_error, wrong_arity, wrong_type,
};

#[path = "hash_compat.rs"]
pub(super) mod compat;

struct HashField {
    value: Bytes,
    expires_at: Option<i64>,
    position: usize,
}

#[derive(Default)]
pub(super) struct HashValue {
    fields: HashMap<Bytes, HashField>,
    retained_fields: HashMap<Bytes, HashField>,
    expirations: BTreeSet<(i64, Bytes)>,
    index: ScanIndex,
    random_fields: Vec<Bytes>,
}

/// Includes real field deadlines; filtering uses the snapshot's frozen clock.
pub(super) struct Snapshot<'a> {
    fields: std::iter::Chain<
        std::collections::hash_map::Iter<'a, Bytes, HashField>,
        std::collections::hash_map::Iter<'a, Bytes, HashField>,
    >,
    now_ms: i64,
}

impl<'a> Iterator for Snapshot<'a> {
    type Item = (&'a Bytes, &'a Bytes, Option<i64>);

    fn next(&mut self) -> Option<Self::Item> {
        self.fields.find_map(|(field, value)| {
            (!value
                .expires_at
                .is_some_and(|deadline| deadline <= self.now_ms))
            .then_some((field, &value.value, value.expires_at))
        })
    }
}

impl HashValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{Bodies, allocation, hash_bytes, sampled, tree_bytes};
        let mut bodies = Bodies::default();
        hash_bytes::<Bytes, HashField>(self.fields.capacity())
            .saturating_add(hash_bytes::<Bytes, HashField>(
                self.retained_fields.capacity(),
            ))
            .saturating_add(tree_bytes::<(i64, Bytes)>(self.expirations.len()))
            .saturating_add(tree_bytes::<(u64, Bytes)>(self.index.len()))
            .saturating_add(allocation::<Bytes>(self.random_fields.capacity()))
            .saturating_add(sampled(self.fields.iter(), samples, |(field, value)| {
                bodies
                    .bytes(field)
                    .saturating_add(bodies.bytes(&value.value))
            }))
            .saturating_add(sampled(
                self.retained_fields.iter(),
                samples,
                |(field, value)| {
                    bodies
                        .bytes(field)
                        .saturating_add(bodies.bytes(&value.value))
                },
            ))
    }

    pub(super) fn snapshot(&self, now_ms: i64) -> Snapshot<'_> {
        Snapshot {
            fields: self.fields.iter().chain(self.retained_fields.iter()),
            now_ms,
        }
    }

    pub(super) fn search_field(&self, field: &[u8], now: i64) -> Option<&Bytes> {
        self.fields
            .get(field)
            .filter(|value| value.expires_at.is_none_or(|deadline| deadline > now))
            .map(|value| &value.value)
    }

    pub(super) fn next_expiry(&self) -> Option<i64> {
        self.expirations.first().map(|(deadline, _)| *deadline)
    }

    pub(super) fn has_retained_fields(&self) -> bool {
        !self.retained_fields.is_empty()
    }

    pub(super) fn restore_retained_fields(&mut self) {
        let mut retained = std::mem::take(&mut self.retained_fields);
        for (field, value) in retained.drain() {
            self.set(field, value.value, value.expires_at);
        }
        self.retained_fields = retained;
    }

    fn set(&mut self, field: Bytes, value: Bytes, expires_at: Option<i64>) -> bool {
        self.retained_fields.remove(&field);
        if let Some(previous) = self.fields.get_mut(&field) {
            if previous.expires_at != expires_at {
                if let Some(deadline) = previous.expires_at {
                    self.expirations.remove(&(deadline, field.clone()));
                }
                if let Some(deadline) = expires_at {
                    self.expirations.insert((deadline, field));
                }
            }
            previous.value = value;
            previous.expires_at = expires_at;
            false
        } else {
            self.index.insert((stable_hash(&field), field.clone()));
            if let Some(deadline) = expires_at {
                self.expirations.insert((deadline, field.clone()));
            }
            let position = self.random_fields.len();
            self.random_fields.push(field.clone());
            self.fields.insert(
                field,
                HashField {
                    value,
                    expires_at,
                    position,
                },
            );
            true
        }
    }

    fn detach(&mut self, field: &Bytes) -> Option<(Bytes, HashField)> {
        let (owned, previous) = self.fields.remove_entry(field)?;
        self.random_fields.swap_remove(previous.position);
        if let Some(moved) = self.random_fields.get(previous.position) {
            self.fields
                .get_mut(moved)
                .expect("indexed hash field")
                .position = previous.position;
        }
        self.index.remove(&(stable_hash(field), field.clone()));
        if let Some(deadline) = previous.expires_at {
            self.expirations.remove(&(deadline, field.clone()));
        }
        Some((owned, previous))
    }

    fn remove(&mut self, field: &Bytes) -> bool {
        self.detach(field).is_some()
    }

    fn set_expiry(&mut self, field: &Bytes, expires_at: Option<i64>) {
        let value = self.fields.get_mut(field).expect("existing hash field");
        if value.expires_at == expires_at {
            return;
        }
        if let Some(deadline) = value.expires_at {
            self.expirations.remove(&(deadline, field.clone()));
        }
        value.expires_at = expires_at;
        if let Some(deadline) = expires_at {
            self.expirations.insert((deadline, field.clone()));
        }
    }
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "hset",
        -4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hset,
    ),
    CommandSpec::handler(
        "hmset",
        -4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hset,
    ),
    CommandSpec::handler(
        "hsetnx",
        4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hsetnx,
    ),
    CommandSpec::handler("hget", 3, &["readonly", "fast"], (1, 1, 1), Engine::hget),
    CommandSpec::handler("hmget", -3, &["readonly", "fast"], (1, 1, 1), Engine::hmget),
    CommandSpec::handler(
        "hgetall",
        2,
        &["readonly"],
        (1, 1, 1),
        Engine::hash_contents,
    ),
    CommandSpec::handler("hdel", -3, &["write", "fast"], (1, 1, 1), Engine::hdel),
    CommandSpec::handler(
        "hexists",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hexists,
    ),
    CommandSpec::handler("hlen", 2, &["readonly", "fast"], (1, 1, 1), Engine::hlen),
    CommandSpec::handler("hkeys", 2, &["readonly"], (1, 1, 1), Engine::hash_contents),
    CommandSpec::handler("hvals", 2, &["readonly"], (1, 1, 1), Engine::hash_contents),
    CommandSpec::handler(
        "hstrlen",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hstrlen,
    ),
    CommandSpec::handler(
        "hincrby",
        4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hincrby,
    ),
    CommandSpec::handler("hscan", -3, &["readonly"], (1, 1, 1), Engine::hscan),
    CommandSpec::handler(
        "hsetex",
        -6,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hsetex,
    ),
    CommandSpec::handler(
        "hexpire",
        -6,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hash_expire,
    ),
    CommandSpec::handler(
        "hpexpire",
        -6,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hash_expire,
    ),
    CommandSpec::handler(
        "hexpireat",
        -6,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hash_expire,
    ),
    CommandSpec::handler(
        "hpexpireat",
        -6,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hash_expire,
    ),
    CommandSpec::handler(
        "httl",
        -5,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hash_ttl,
    ),
    CommandSpec::handler(
        "hpttl",
        -5,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hash_ttl,
    ),
    CommandSpec::handler(
        "hexpiretime",
        -5,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hash_ttl,
    ),
    CommandSpec::handler(
        "hpexpiretime",
        -5,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::hash_ttl,
    ),
    CommandSpec::handler(
        "hpersist",
        -5,
        &["write", "fast"],
        (1, 1, 1),
        Engine::hpersist,
    ),
];

// Redis 8 reserves two of its 48 field-expiration bits at the public API.
const MAX_FIELD_DEADLINE: i64 = (1_i64 << 46) - 1;

fn field_deadline(value: &Bytes, seconds: bool, relative: bool, now: i64) -> Result<i64, Resp> {
    let value = integer(value).ok_or_else(integer_error)?;
    if value < 0 {
        return Err(error("ERR invalid expire time, must be >= 0"));
    }
    let milliseconds = if seconds {
        value.checked_mul(1000)
    } else {
        Some(value)
    };
    milliseconds
        .and_then(|value| value.checked_add(if relative { now } else { 0 }))
        .filter(|deadline| *deadline <= MAX_FIELD_DEADLINE)
        .ok_or_else(|| error("ERR invalid expire time"))
}

fn field_arguments(args: &[Bytes], position: usize, pairs: bool) -> Result<&[Bytes], Resp> {
    if !args
        .get(position)
        .is_some_and(|value| value.eq_ignore_ascii_case(b"FIELDS"))
    {
        return Err(syntax_error());
    }
    let count = args
        .get(position + 1)
        .and_then(|value| integer(value))
        .filter(|count| *count > 0)
        .and_then(|count| usize::try_from(count).ok())
        .ok_or_else(integer_error)?;
    let fields = args.get(position + 2..).ok_or_else(syntax_error)?;
    if count.checked_mul(if pairs { 2 } else { 1 }) != Some(fields.len()) {
        return Err(syntax_error());
    }
    Ok(fields)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FieldCondition {
    Any,
    Nx,
    Xx,
    Gt,
    Lt,
}

impl FieldCondition {
    fn matches(self, old: Option<i64>, deadline: i64) -> bool {
        match self {
            Self::Any => true,
            Self::Nx => old.is_none(),
            Self::Xx => old.is_some(),
            Self::Gt => old.is_some_and(|old| deadline > old),
            Self::Lt => old.is_none_or(|old| deadline < old),
        }
    }
}

impl Engine {
    fn hash_value(&mut self, key: &Bytes, now: i64) -> Result<Option<&HashValue>, Resp> {
        match self.value(key, now) {
            None => Ok(None),
            Some(Value::Hash(hash)) => Ok(Some(hash)),
            Some(_) => Err(wrong_type()),
        }
    }

    fn finish_hash_mutation(&mut self, key: &Bytes, previous: Option<i64>) {
        let (empty, retained) = match self.entries.get(key).map(|entry| &entry.value) {
            Some(Value::Hash(hash)) => (hash.fields.is_empty(), hash.has_retained_fields()),
            _ => (false, false),
        };
        if !retained {
            self.retained_hashes.remove(key);
        }
        if empty {
            let retain = retained && self.holding_expiry(key);
            if retain {
                if let Some((key, entry)) = self.detach_entry(key) {
                    self.held_entries.insert(key, entry);
                }
            } else {
                self.remove(key);
            }
        }
        self.refresh_hash_deadline(key, previous);
    }

    // Indexing and waiter visibility need the same resolved changes even without
    // an ordinary AOF. Materialize only fields actually written.
    fn record_hash_fields<'a>(
        &mut self,
        key: &Bytes,
        fields: impl Iterator<Item = &'a Bytes>,
        now: i64,
    ) {
        if !self.capturing_changes() {
            return;
        }
        let Some(entry) = self.entries.get(key) else {
            return;
        };
        let Value::Hash(hash) = &entry.value else {
            return;
        };
        let fields: Vec<_> = fields
            .map(|field| {
                let value = hash.fields.get(field).expect("written hash field");
                (field.clone(), value.value.clone(), value.expires_at)
            })
            .collect();
        if !fields.is_empty() {
            self.record(now, || Mutation::HashSet {
                key: key.clone(),
                fields,
            });
        }
    }

    fn hset(&mut self, args: &[Bytes], now: i64) -> Resp {
        let hmset = args[0].eq_ignore_ascii_case(b"HMSET");
        if !args.len().is_multiple_of(2) {
            return wrong_arity(if hmset { "hmset" } else { "hset" });
        }
        if let Err(reply) = self.hash_value(&args[1], now) {
            return reply;
        }
        let previous = self.hash_deadline(&args[1]);
        let Value::Hash(hash) =
            self.collection_entry(&args[1], now, || Value::Hash(HashValue::default()))
        else {
            unreachable!("hash type validated");
        };
        let mut added = 0;
        for pair in args[2..].as_chunks::<2>().0.iter() {
            added += i64::from(hash.set(pair[0].clone(), pair[1].clone(), None));
        }
        self.finish_hash_mutation(&args[1], previous);
        self.record_hash_fields(
            &args[1],
            args[2..].as_chunks::<2>().0.iter().map(|pair| &pair[0]),
            now,
        );
        if hmset {
            Resp::Simple("OK")
        } else {
            Resp::Integer(added)
        }
    }

    fn hsetnx(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(Some(hash)) if hash.fields.contains_key(&args[2]) => return Resp::Integer(0),
            _ => {}
        }
        let previous = self.hash_deadline(&args[1]);
        let Value::Hash(hash) =
            self.collection_entry(&args[1], now, || Value::Hash(HashValue::default()))
        else {
            unreachable!("hash type validated");
        };
        hash.set(args[2].clone(), args[3].clone(), None);
        self.finish_hash_mutation(&args[1], previous);
        self.record_hash_fields(&args[1], std::iter::once(&args[2]), now);
        Resp::Integer(1)
    }

    fn hget(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => reply,
            Ok(hash) => Resp::Bulk(
                hash.and_then(|hash| hash.fields.get(&args[2]))
                    .map(|field| field.value.clone()),
            ),
        }
    }

    fn hmget(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        match self.hash_value(&args[1], now) {
            Err(reply) => reply,
            Ok(hash) => super::bounded_replies(
                args[2..].iter().map(|field| {
                    Resp::Bulk(
                        hash.and_then(|hash| hash.fields.get(field))
                            .map(|field| field.value.clone()),
                    )
                }),
                limit,
            ),
        }
    }

    fn hash_contents(&mut self, args: &[Bytes], now: i64) -> Resp {
        let keys = !args[0].eq_ignore_ascii_case(b"HVALS");
        let values = !args[0].eq_ignore_ascii_case(b"HKEYS");
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        let hash = match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(None) if keys && values => return Resp::Map(Vec::new()),
            Ok(None) => return array(Vec::new()),
            Ok(Some(hash)) => hash,
        };
        for (field, value) in &hash.fields {
            let rows = usize::from(keys) + usize::from(values);
            let bytes =
                if keys { field.len() } else { 0 } + if values { value.value.len() } else { 0 };
            if let Err(error) = budget.charge(rows, bytes) {
                return error;
            }
        }
        if keys && values {
            return Resp::Map(
                hash.fields
                    .iter()
                    .map(|(field, value)| {
                        (
                            Resp::Bulk(Some(field.clone())),
                            Resp::Bulk(Some(value.value.clone())),
                        )
                    })
                    .collect(),
            );
        }
        let mut result = Vec::with_capacity(hash.fields.len());
        for (field, value) in &hash.fields {
            if keys {
                result.push(Resp::Bulk(Some(field.clone())));
            }
            if values {
                result.push(Resp::Bulk(Some(value.value.clone())));
            }
        }
        array(result)
    }

    fn hdel(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(None) => return Resp::Integer(0),
            _ => {}
        }
        let previous = self.hash_deadline(&args[1]);
        let journal = self.capturing_changes();
        let mut removed = Vec::new();
        let mut count = 0;
        let Value::Hash(hash) = &mut self.entries.get_mut(&args[1]).expect("existing hash").value
        else {
            unreachable!();
        };
        for field in &args[2..] {
            if hash.remove(field) {
                count += 1;
                if journal {
                    removed.push(field.clone());
                }
            }
        }
        self.finish_hash_mutation(&args[1], previous);
        if !removed.is_empty() {
            self.record(now, || Mutation::HashDelete {
                key: args[1].clone(),
                fields: removed,
            });
        }
        Resp::Integer(count)
    }

    fn hexists(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => reply,
            Ok(hash) => Resp::Integer(i64::from(
                hash.is_some_and(|hash| hash.fields.contains_key(&args[2])),
            )),
        }
    }

    fn hlen(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => reply,
            Ok(hash) => Resp::Integer(hash.map_or(0, |hash| hash.fields.len() as i64)),
        }
    }

    fn hstrlen(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.hash_value(&args[1], now) {
            Err(reply) => reply,
            Ok(hash) => Resp::Integer(
                hash.and_then(|hash| hash.fields.get(&args[2]))
                    .map_or(0, |field| field.value.len() as i64),
            ),
        }
    }

    fn hincrby(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(increment) = integer(&args[3]) else {
            return integer_error();
        };
        let (old, deadline) = match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(hash) => match hash.and_then(|hash| hash.fields.get(&args[2])) {
                Some(field) => match integer(&field.value) {
                    Some(value) => (value, field.expires_at),
                    None => return error("ERR hash value is not an integer"),
                },
                None => (0, None),
            },
        };
        let Some(value) = old.checked_add(increment) else {
            return error("ERR increment or decrement would overflow");
        };
        let previous = self.hash_deadline(&args[1]);
        let Value::Hash(hash) =
            self.collection_entry(&args[1], now, || Value::Hash(HashValue::default()))
        else {
            unreachable!();
        };
        hash.set(args[2].clone(), Bytes::from(value.to_string()), deadline);
        self.finish_hash_mutation(&args[1], previous);
        self.record_hash_fields(&args[1], std::iter::once(&args[2]), now);
        Resp::Integer(value)
    }

    fn hscan(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        // NOVALUES is a flag, unlike MATCH/COUNT's option-value pairs.
        // Keep the common scan path allocation-free apart from its reply.
        let mut no_values = false;
        let mut filtered = Vec::new();
        let mut position = 3;
        while position < args.len() {
            if args[position].eq_ignore_ascii_case(b"NOVALUES") {
                if !no_values {
                    filtered.extend_from_slice(&args[..position]);
                }
                no_values = true;
                position += 1;
            } else {
                if no_values {
                    filtered.push(args[position].clone());
                    if let Some(value) = args.get(position + 1) {
                        filtered.push(value.clone());
                    }
                }
                position += 2;
            }
        }
        let options = match parse_scan(if no_values { &filtered } else { args }, 2, false) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        let hash = match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(None) => {
                return array(vec![
                    Resp::Bulk(Some(Bytes::from_static(b"0"))),
                    array(Vec::new()),
                ]);
            }
            Ok(Some(hash)) => hash,
        };
        let (cursor, fields) = match scan_entries(&hash.index, &options, limit) {
            Ok(page) => page,
            Err(error) => return error,
        };
        let mut budget = super::ReplyBudget::new(limit);
        for field in &fields {
            let value_bytes = if no_values {
                0
            } else {
                hash.fields[field].value.len()
            };
            if let Err(error) =
                budget.charge(if no_values { 1 } else { 2 }, field.len() + value_bytes)
            {
                return error;
            }
        }
        let mut result = Vec::with_capacity(fields.len() * if no_values { 1 } else { 2 });
        for field in fields {
            if no_values {
                result.push(Resp::Bulk(Some(field)));
            } else {
                let value = hash.fields.get(&field).expect("indexed hash field");
                result.push(Resp::Bulk(Some(field)));
                result.push(Resp::Bulk(Some(value.value.clone())));
            }
        }
        array(vec![
            Resp::Bulk(Some(Bytes::from(cursor.to_string()))),
            array(result),
        ])
    }

    fn hsetex(&mut self, args: &[Bytes], now: i64) -> Resp {
        if let Err(reply) = self.hash_value(&args[1], now) {
            return reply;
        }
        let compat::FieldOptions {
            fields,
            expiration,
            retain_or_persist: keep_ttl,
            condition,
        } = match compat::parse_field_options(args, now, true) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        let hash = match self.hash_value(&args[1], now) {
            Ok(hash) => hash,
            Err(reply) => return reply,
        };
        if let Some(none_exist) = condition
            && !fields.as_chunks::<2>().0.iter().all(|pair| {
                hash.is_some_and(|hash| hash.fields.contains_key(&pair[0])) != none_exist
            })
        {
            return Resp::Integer(0);
        }
        let previous = self.hash_deadline(&args[1]);
        if expiration.is_some_and(|deadline| deadline <= now) {
            // A successful already-due write leaves no fields behind. Journal
            // only the existing fields actually removed, never passive expiry.
            let journal = self.capturing_changes();
            let mut removed = Vec::new();
            if let Some(entry) = self.entries.get_mut(&args[1]) {
                let Value::Hash(hash) = &mut entry.value else {
                    unreachable!();
                };
                for pair in fields.as_chunks::<2>().0.iter() {
                    if hash.remove(&pair[0]) && journal {
                        removed.push(pair[0].clone());
                    }
                }
            }
            self.finish_hash_mutation(&args[1], previous);
            if !removed.is_empty() {
                self.record(now, || Mutation::HashDelete {
                    key: args[1].clone(),
                    fields: removed,
                });
            }
        } else {
            let Value::Hash(hash) =
                self.collection_entry(&args[1], now, || Value::Hash(HashValue::default()))
            else {
                unreachable!();
            };
            for pair in fields.as_chunks::<2>().0.iter() {
                let deadline = if keep_ttl {
                    hash.fields.get(&pair[0]).and_then(|field| field.expires_at)
                } else {
                    expiration
                };
                hash.set(pair[0].clone(), pair[1].clone(), deadline);
            }
            self.finish_hash_mutation(&args[1], previous);
            self.record_hash_fields(
                &args[1],
                fields.as_chunks::<2>().0.iter().map(|pair| &pair[0]),
                now,
            );
        }
        Resp::Integer(1)
    }

    fn hash_expire(&mut self, args: &[Bytes], now: i64) -> Resp {
        let exists = match self.hash_value(&args[1], now) {
            Ok(hash) => hash.is_some(),
            Err(reply) => return reply,
        };
        let seconds =
            args[0].eq_ignore_ascii_case(b"HEXPIRE") || args[0].eq_ignore_ascii_case(b"HEXPIREAT");
        let relative =
            args[0].eq_ignore_ascii_case(b"HEXPIRE") || args[0].eq_ignore_ascii_case(b"HPEXPIRE");
        let deadline = match field_deadline(&args[2], seconds, relative, now) {
            Ok(deadline) => deadline,
            Err(reply) => return reply,
        };
        let mut condition = FieldCondition::Any;
        let mut condition_flags = 0_u8;
        let mut fields = None;
        let mut position = 3;
        while let Some(option) = args.get(position) {
            if option.eq_ignore_ascii_case(b"FIELDS") {
                if fields.is_some() {
                    return error("ERR FIELDS keyword specified multiple times");
                }
                let count = args
                    .get(position + 1)
                    .and_then(|value| integer(value))
                    .filter(|count| (1..=i64::from(i32::MAX)).contains(count))
                    .and_then(|count| usize::try_from(count).ok());
                let Some(count) = count else {
                    return error("ERR Parameter `numFields` should be greater than 0");
                };
                let start = position + 2;
                let Some(end) = start.checked_add(count) else {
                    return syntax_error();
                };
                fields = match args.get(start..end) {
                    Some(fields) => Some(fields),
                    None => return error("ERR wrong number of arguments"),
                };
                position = end;
                continue;
            }
            let (flag, next) = if option.eq_ignore_ascii_case(b"NX") {
                (1, FieldCondition::Nx)
            } else if option.eq_ignore_ascii_case(b"XX") {
                (2, FieldCondition::Xx)
            } else if option.eq_ignore_ascii_case(b"GT") {
                (4, FieldCondition::Gt)
            } else if option.eq_ignore_ascii_case(b"LT") {
                (8, FieldCondition::Lt)
            } else {
                return Resp::Error(format!(
                    "ERR unknown argument: {}",
                    String::from_utf8_lossy(option)
                ));
            };
            condition_flags |= flag;
            condition = next;
            position += 1;
        }
        let Some(fields) = fields else {
            return error("ERR missing FIELDS argument");
        };
        if condition_flags.count_ones() > 1 {
            return error("ERR Multiple condition flags specified");
        }
        if let Err(error) = super::ReplyBudget::new(self.reply_limit).charge(fields.len(), 0) {
            return error;
        }
        if !exists {
            return array(fields.iter().map(|_| Resp::Integer(-2)).collect());
        }
        let previous = self.hash_deadline(&args[1]);
        let journal = self.capturing_changes();
        let mut changed = Vec::new();
        let mut result = Vec::with_capacity(fields.len());
        let Value::Hash(hash) = &mut self.entries.get_mut(&args[1]).expect("existing hash").value
        else {
            unreachable!();
        };
        for field in fields {
            let status = match hash.fields.get(field) {
                None => -2,
                Some(value) if !condition.matches(value.expires_at, deadline) => 0,
                Some(_) => {
                    if deadline <= now {
                        hash.remove(field);
                    } else {
                        hash.set_expiry(field, Some(deadline));
                    }
                    if journal {
                        changed.push(field.clone());
                    }
                    if deadline <= now { 2 } else { 1 }
                }
            };
            result.push(Resp::Integer(status));
        }
        self.finish_hash_mutation(&args[1], previous);
        if deadline <= now && !changed.is_empty() {
            self.record(now, || Mutation::HashDelete {
                key: args[1].clone(),
                fields: changed,
            });
        } else {
            self.record_hash_fields(&args[1], changed.iter(), now);
        }
        array(result)
    }

    fn hash_ttl(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let hash = match self.hash_value(&args[1], now) {
            Ok(hash) => hash,
            Err(reply) => return reply,
        };
        let fields = match field_arguments(args, 2, false) {
            Ok(fields) => fields,
            Err(reply) => return reply,
        };
        if let Err(error) = super::ReplyBudget::new(limit).charge(fields.len(), 0) {
            return error;
        }
        let relative =
            args[0].eq_ignore_ascii_case(b"HTTL") || args[0].eq_ignore_ascii_case(b"HPTTL");
        let seconds =
            args[0].eq_ignore_ascii_case(b"HTTL") || args[0].eq_ignore_ascii_case(b"HEXPIRETIME");
        array(
            fields
                .iter()
                .map(|field| {
                    Resp::Integer(match hash.and_then(|hash| hash.fields.get(field)) {
                        None => -2,
                        Some(HashField {
                            expires_at: None, ..
                        }) => -1,
                        Some(HashField {
                            expires_at: Some(deadline),
                            ..
                        }) => {
                            let value = *deadline - if relative { now } else { 0 };
                            if seconds { (value + 999) / 1000 } else { value }
                        }
                    })
                })
                .collect(),
        )
    }

    fn hpersist(&mut self, args: &[Bytes], now: i64) -> Resp {
        let exists = match self.hash_value(&args[1], now) {
            Ok(hash) => hash.is_some(),
            Err(reply) => return reply,
        };
        let fields = match field_arguments(args, 2, false) {
            Ok(fields) => fields,
            Err(reply) => return reply,
        };
        if let Err(error) = super::ReplyBudget::new(self.reply_limit).charge(fields.len(), 0) {
            return error;
        }
        if !exists {
            return array(fields.iter().map(|_| Resp::Integer(-2)).collect());
        }
        let previous = self.hash_deadline(&args[1]);
        let journal = self.capturing_changes();
        let mut changed = Vec::new();
        let mut result = Vec::with_capacity(fields.len());
        let Value::Hash(hash) = &mut self.entries.get_mut(&args[1]).expect("existing hash").value
        else {
            unreachable!();
        };
        for field in fields {
            let status = match hash.fields.get(field) {
                None => -2,
                Some(HashField {
                    expires_at: None, ..
                }) => -1,
                Some(_) => {
                    hash.set_expiry(field, None);
                    if journal {
                        changed.push(field.clone());
                    }
                    1
                }
            };
            result.push(Resp::Integer(status));
        }
        self.finish_hash_mutation(&args[1], previous);
        self.record_hash_fields(&args[1], changed.iter(), now);
        array(result)
    }

    pub(super) fn prune_hash_fields(&mut self, key: &Bytes, now: i64, limit: usize) -> usize {
        if self.replication_replay {
            return 0;
        }
        self.release_retained_hash(key, now);
        let previous = self.hash_deadline(key);
        let hold = self.holding_expiry(key);
        let record = self.capturing_changes() && !hold;
        let mut expired = Vec::new();
        let mut removed = 0;
        let owned = self.entries.get_key_value(key).map(|(key, _)| key.clone());
        if let Some(entry) = self.entries.get_mut(key)
            && let Value::Hash(hash) = &mut entry.value
        {
            while removed < limit && hash.next_expiry().is_some_and(|deadline| deadline <= now) {
                let (_, field) = hash
                    .expirations
                    .first()
                    .expect("due field expiration")
                    .clone();
                let (field, value) = hash.detach(&field).expect("indexed hash expiration");
                if hold {
                    hash.retained_fields.insert(field, value);
                } else if record {
                    expired.push(field);
                }
                removed += 1;
            }
        }
        if removed > 0 {
            if hold && let Some(owned) = owned {
                self.retained_hashes.insert(owned);
            }
            self.finish_hash_mutation(key, previous);
            if !expired.is_empty() {
                self.record(now, || Mutation::HashDelete {
                    key: key.clone(),
                    fields: expired,
                });
            }
        }
        removed
    }

    pub(super) fn release_retained_hash(&mut self, key: &Bytes, now: i64) {
        if self.replication_replay || self.holding_expiry(key) {
            return;
        }
        self.retained_hashes.remove(key);
        let fields = match self.entries.get_mut(key).map(|entry| &mut entry.value) {
            Some(Value::Hash(hash)) => hash
                .retained_fields
                .drain()
                .map(|(field, _)| field)
                .collect::<Vec<_>>(),
            _ => return,
        };
        if !fields.is_empty() {
            self.record_change(now, false, || Mutation::HashDelete {
                key: key.clone(),
                fields,
            });
        }
    }

    pub(super) fn release_retained_hashes(&mut self, now: i64, limit: usize) {
        if self.replica_mode || self.replication_replay {
            return;
        }
        let keys: Vec<_> = self
            .retained_hashes
            .iter()
            .filter(|key| !self.holding_expiry(key))
            .take(limit)
            .cloned()
            .collect();
        for key in keys {
            self.release_retained_hash(&key, now);
        }
    }

    pub(super) fn expire_hashes(&mut self, now: i64, limit: usize) {
        if self.replication_replay {
            return;
        }
        self.release_retained_hashes(now, limit);
        let mut remaining = limit;
        while remaining > 0 {
            let Some((deadline, key)) = self.hash_expirations.first() else {
                break;
            };
            if *deadline > now {
                break;
            }
            let key = key.clone();
            let removed = self.prune_hash_fields(&key, now, remaining);
            debug_assert!(removed > 0, "exact global hash expiration index");
            if removed == 0 {
                break;
            }
            remaining -= removed;
        }
    }

    pub(super) fn replay_hash_set(
        &mut self,
        key: Bytes,
        fields: Vec<(Bytes, Bytes, Option<i64>)>,
        now: i64,
    ) -> io::Result<()> {
        if fields.is_empty()
            || fields.iter().any(|(_, _, deadline)| {
                deadline.is_some_and(|value| !(0..=MAX_FIELD_DEADLINE).contains(&value))
            })
        {
            return Err(invalid_journal());
        }
        if self.hash_value(&key, now).is_err() {
            return Err(invalid_journal());
        }
        let previous = self.hash_deadline(&key);
        let Value::Hash(hash) =
            self.collection_entry(&key, now, || Value::Hash(HashValue::default()))
        else {
            unreachable!();
        };
        for (field, value, deadline) in fields {
            if deadline.is_some_and(|deadline| deadline <= now) {
                hash.remove(&field);
            } else {
                hash.set(field, value, deadline);
            }
        }
        self.finish_hash_mutation(&key, previous);
        Ok(())
    }

    pub(super) fn replay_hash_delete(&mut self, key: Bytes, fields: Vec<Bytes>) -> io::Result<()> {
        if fields.is_empty() {
            return Err(invalid_journal());
        }
        // The source has already resolved field expiration into this deletion.
        // Looking the hash up through hash_value would expire those same fields
        // again at the batch timestamp before we can apply the durable effect.
        if !matches!(
            self.entries.get(&key).map(|entry| &entry.value),
            Some(Value::Hash(_))
        ) {
            return Err(invalid_journal());
        }
        let previous = self.hash_deadline(&key);
        let Value::Hash(hash) = &mut self.entries.get_mut(&key).expect("existing hash").value
        else {
            unreachable!();
        };
        for field in fields {
            if !hash.remove(&field) {
                return Err(invalid_journal());
            }
        }
        self.finish_hash_mutation(&key, previous);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect::<Vec<_>>(),
            now,
        )
    }

    fn ints(values: &[i64]) -> Resp {
        array(values.iter().copied().map(Resp::Integer).collect())
    }

    fn value(value: &str) -> Resp {
        Resp::Bulk(Some(Bytes::copy_from_slice(value.as_bytes())))
    }

    fn persist(source: &mut Engine, recovered: &mut Engine, now: i64, args: &[&str]) -> Resp {
        let response = run(source, now, args);
        if let Some(batch) = source.take_batch() {
            recovered.replay(batch).unwrap();
        }
        response
    }

    #[test]
    fn binary_fields_duplicates_and_basic_hash_commands() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"hash\0\xff");
        let field = Bytes::from_static(b"\0\xff\r\n");
        let args = vec![
            Bytes::from_static(b"HSET"),
            key.clone(),
            field.clone(),
            Bytes::new(),
            field.clone(),
            Bytes::from_static(b"\xff\0"),
        ];
        assert_eq!(engine.execute_at(&args, 1000), Resp::Integer(1));
        assert_eq!(
            engine.execute_at(
                &[Bytes::from_static(b"HGET"), key.clone(), field.clone()],
                1000
            ),
            Resp::Bulk(Some(Bytes::from_static(b"\xff\0")))
        );
        assert_eq!(
            engine.execute_at(&[Bytes::from_static(b"HSTRLEN"), key.clone(), field], 1000),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HMSET", "h", "a", "1", "b", "2"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, 1000, &["HSETNX", "h", "a", "ignored"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HSETNX", "h", "c", "3"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HMGET", "h", "b", "missing", "a", "b"]),
            array(vec![value("2"), Resp::Bulk(None), value("1"), value("2")])
        );
        assert_eq!(
            run(&mut engine, 1000, &["HEXISTS", "h", "a"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, 1000, &["HLEN", "h"]), Resp::Integer(3));
        for (command, expected) in [
            ("HKEYS", vec![value("a"), value("b"), value("c")]),
            ("HVALS", vec![value("1"), value("2"), value("3")]),
        ] {
            let Resp::Array(Some(actual)) = run(&mut engine, 1000, &[command, "h"]) else {
                panic!("array reply");
            };
            assert_eq!(actual.len(), expected.len());
            assert!(expected.iter().all(|item| actual.contains(item)));
        }
        let Resp::Map(all) = run(&mut engine, 1000, &["HGETALL", "h"]) else {
            panic!("map reply");
        };
        assert_eq!(all.len(), 3);
        for (field, content) in [("a", "1"), ("b", "2"), ("c", "3")] {
            assert!(all.contains(&(value(field), value(content))));
        }
        assert_eq!(
            run(&mut engine, 1000, &["HDEL", "h", "a", "a", "missing"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HDEL", "h", "b", "c"]),
            Resp::Integer(2)
        );
        assert_eq!(run(&mut engine, 1000, &["TYPE", "h"]), Resp::Simple("none"));
        assert_eq!(
            run(&mut engine, 1000, &["HGETALL", "h"]),
            Resp::Map(Vec::new())
        );
    }

    #[test]
    fn hsetex_conditions_cover_the_entire_original_field_set() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["HSET", "h", "present", "old"]);
        assert_eq!(
            run(
                &mut engine,
                1000,
                &[
                    "HSETEX", "h", "FNX", "PX", "100", "FIELDS", "2", "new", "new", "present",
                    "changed"
                ]
            ),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HMGET", "h", "new", "present"]),
            array(vec![Resp::Bulk(None), value("old")])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &[
                    "HSETEX", "h", "FXX", "FIELDS", "2", "present", "changed", "new", "new"
                ]
            ),
            Resp::Integer(0)
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HSETEX", "missing", "FXX", "FIELDS", "1", "f", "v"]
            ),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, 1000, &["EXISTS", "missing"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &[
                    "HSETEX", "h", "PX", "100", "FNX", "FIELDS", "2", "new", "first", "new", "last"
                ]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, 1000, &["HGET", "h", "new"]), value("last"));
        assert_eq!(
            run(
                &mut engine,
                1050,
                &[
                    "HSETEX", "h", "FXX", "KEEPTTL", "FIELDS", "2", "new", "kept", "present", "new"
                ]
            ),
            Resp::Integer(1)
        );
        assert_eq!(
            run(
                &mut engine,
                1050,
                &["HPEXPIRETIME", "h", "FIELDS", "2", "new", "present"]
            ),
            ints(&[1100, -1])
        );
        assert_eq!(
            run(&mut engine, 1100, &["HMGET", "h", "new", "present"]),
            array(vec![Resp::Bulk(None), value("new")])
        );
        assert_eq!(
            run(
                &mut engine,
                1100,
                &[
                    "HSETEX", "h", "PXAT", "0", "FIELDS", "2", "present", "gone", "new", "gone"
                ]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, 1100, &["EXISTS", "h"]), Resp::Integer(0));
    }

    #[test]
    fn field_conditions_response_codes_and_duplicate_fields_are_sequential() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["HSET", "h", "a", "1", "b", "2"]);
        assert_eq!(
            run(
                &mut engine,
                1000,
                &[
                    "HPEXPIRE", "h", "100", "GT", "FIELDS", "3", "a", "b", "missing"
                ]
            ),
            ints(&[0, 0, -2])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "100", "LT", "FIELDS", "1", "a"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "100", "GT", "FIELDS", "1", "a"]
            ),
            ints(&[0])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "200", "GT", "FIELDS", "1", "a"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "100", "NX", "FIELDS", "3", "a", "b", "b"]
            ),
            ints(&[0, 1, 0])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPERSIST", "h", "FIELDS", "3", "a", "a", "missing"]
            ),
            ints(&[1, -1, -2])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "0", "XX", "FIELDS", "2", "a", "b"]
            ),
            ints(&[0, 2])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIREAT", "h", "0", "FIELDS", "2", "a", "a"]
            ),
            ints(&[2, -2])
        );
        assert_eq!(run(&mut engine, 1000, &["TYPE", "h"]), Resp::Simple("none"));
        assert_eq!(
            run(&mut engine, 1000, &["HPERSIST", "h", "FIELDS", "1", "a"]),
            ints(&[-2])
        );
    }

    #[test]
    fn ttl_rounding_deadline_bounds_and_all_expiration_units() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            1000,
            &["HSET", "h", "a", "1", "b", "2", "c", "3", "d", "4"],
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HEXPIRE", "h", "1", "FIELDS", "1", "a"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HEXPIREAT", "h", "3", "FIELDS", "1", "b"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIREAT", "h", "3001", "FIELDS", "1", "c"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRE", "h", "1", "FIELDS", "1", "d"]
            ),
            ints(&[1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HTTL", "h", "FIELDS", "4", "a", "b", "c", "d"]
            ),
            ints(&[1, 2, 3, 1])
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HEXPIRETIME", "h", "FIELDS", "4", "a", "b", "c", "d"]
            ),
            ints(&[2, 3, 4, 2])
        );
        assert_eq!(
            run(&mut engine, 1001, &["HPTTL", "h", "FIELDS", "1", "d"]),
            ints(&[-2])
        );
        let maximum = MAX_FIELD_DEADLINE.to_string();
        assert_eq!(
            run(
                &mut engine,
                1001,
                &["HPEXPIREAT", "h", &maximum, "FIELDS", "1", "a"]
            ),
            ints(&[1])
        );
        let too_large = (MAX_FIELD_DEADLINE + 1).to_string();
        for args in [
            vec!["HPEXPIREAT", "h", &too_large, "FIELDS", "1", "a"],
            vec!["HEXPIRE", "h", "9223372036854775807", "FIELDS", "1", "a"],
            vec!["HPEXPIRE", "h", "-1", "FIELDS", "1", "a"],
            vec!["HSETEX", "h", "PX", "-1", "FIELDS", "1", "a", "changed"],
        ] {
            assert!(matches!(run(&mut engine, 1001, &args), Resp::Error(_)));
        }
        assert_eq!(
            run(
                &mut engine,
                1001,
                &["HPEXPIRETIME", "h", "FIELDS", "1", "a"]
            ),
            ints(&[MAX_FIELD_DEADLINE])
        );
        assert_eq!(run(&mut engine, 1001, &["HGET", "h", "a"]), value("1"));
    }

    #[test]
    fn integer_overflow_and_syntax_errors_do_not_change_value_or_either_ttl() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            1000,
            &[
                "HSETEX",
                "h",
                "PX",
                "1000",
                "FIELDS",
                "1",
                "n",
                "9223372036854775807",
            ],
        );
        run(&mut engine, 1000, &["PEXPIRE", "h", "5000"]);
        for args in [
            vec!["HINCRBY", "h", "n", "1"],
            vec!["HINCRBY", "h", "n", "01"],
            vec!["HSET", "h", "n", "changed", "dangling"],
            vec![
                "HSETEX", "h", "EX", "1", "KEEPTTL", "FIELDS", "1", "n", "changed",
            ],
            vec!["HSETEX", "h", "FNX", "FXX", "FIELDS", "1", "n", "changed"],
            vec!["HSETEX", "h", "FIELDS", "2", "n", "changed"],
            vec!["HPEXPIRE", "h", "1", "XX", "LT", "FIELDS", "1", "n"],
            vec!["HPERSIST", "h", "FIELDS", "0"],
            vec!["HSCAN", "h", "0", "COUNT", "0"],
        ] {
            assert!(matches!(run(&mut engine, 1000, &args), Resp::Error(_)));
        }
        assert_eq!(
            run(&mut engine, 1000, &["HGET", "h", "n"]),
            value("9223372036854775807")
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRETIME", "h", "FIELDS", "1", "n"]
            ),
            ints(&[2000])
        );
        assert_eq!(run(&mut engine, 1000, &["PTTL", "h"]), Resp::Integer(5000));
        assert_eq!(
            run(&mut engine, 1000, &["HINCRBY", "h", "n", "-1"]),
            Resp::Integer(i64::MAX - 1)
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HPEXPIRETIME", "h", "FIELDS", "1", "n"]
            ),
            ints(&[2000])
        );
        run(
            &mut engine,
            1000,
            &[
                "HSETEX",
                "h",
                "KEEPTTL",
                "FIELDS",
                "1",
                "n",
                "-9223372036854775808",
            ],
        );
        assert!(matches!(
            run(&mut engine, 1000, &["HINCRBY", "h", "n", "-1"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut engine, 1000, &["HGET", "h", "n"]),
            value("-9223372036854775808")
        );
        assert_eq!(
            run(&mut engine, 1000, &["HSET", "h", "n", "new"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, 1000, &["HPTTL", "h", "FIELDS", "1", "n"]),
            ints(&[-1])
        );
        assert_eq!(run(&mut engine, 1000, &["PTTL", "h"]), Resp::Integer(5000));
        run(&mut engine, 1000, &["SET", "wrong", "string"]);
        assert!(matches!(
            run(&mut engine, 1000, &["HSET", "wrong", "f", "v"]),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, 1000, &["GET", "wrong"]), value("string"));
    }

    #[test]
    fn key_and_field_expiration_are_independent_and_last_field_removes_key() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            1000,
            &["HSETEX", "h", "PX", "100", "FIELDS", "1", "f", "v"],
        );
        run(&mut engine, 1000, &["PEXPIRE", "h", "5000"]);
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["HSETEX", "h", "KEEPTTL", "FIELDS", "1", "f", "new"]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, 1000, &["PTTL", "h"]), Resp::Integer(5000));
        assert_eq!(
            run(&mut engine, 1100, &["HGET", "h", "f"]),
            Resp::Bulk(None)
        );
        assert_eq!(run(&mut engine, 1100, &["EXISTS", "h"]), Resp::Integer(0));
        assert_eq!(
            run(&mut engine, 1100, &["SET", "h", "reused"]),
            Resp::Simple("OK")
        );
        assert_eq!(run(&mut engine, 1100, &["PTTL", "h"]), Resp::Integer(-1));
        run(
            &mut engine,
            1100,
            &["HSETEX", "other", "PX", "1000", "FIELDS", "1", "f", "v"],
        );
        run(&mut engine, 1100, &["PEXPIRE", "other", "1"]);
        assert_eq!(
            run(&mut engine, 1101, &["HPTTL", "other", "FIELDS", "1", "f"]),
            ints(&[-2])
        );
        assert!(
            !engine
                .hash_expirations
                .iter()
                .any(|(_, key)| key.as_ref() == b"other")
        );
    }

    #[test]
    fn active_expiration_has_a_field_quota_and_tracks_changed_minima() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            1000,
            &[
                "HSETEX", "h", "PX", "10", "FIELDS", "3", "a", "1", "b", "2", "c", "3",
            ],
        );
        run(
            &mut engine,
            1000,
            &["HSETEX", "other", "PX", "20", "FIELDS", "1", "d", "4"],
        );
        run(
            &mut engine,
            1000,
            &["HPEXPIRE", "h", "50", "FIELDS", "1", "a"],
        );
        run(&mut engine, 1000, &["HPERSIST", "h", "FIELDS", "1", "c"]);
        engine.expire_hashes(1020, 1);
        assert_eq!(engine.hash_deadline(&Bytes::from_static(b"h")), Some(1050));
        engine.expire_hashes(1020, 1);
        assert!(!engine.entries.contains_key(&Bytes::from_static(b"other")));
        engine.expire_hashes(1050, usize::MAX);
        assert_eq!(
            run(&mut engine, 1050, &["HGETALL", "h"]),
            Resp::Map(vec![(value("c"), value("3"))])
        );
        assert!(engine.hash_expirations.is_empty());
        run(
            &mut engine,
            1050,
            &["HPEXPIRE", "h", "1", "FIELDS", "1", "c"],
        );
        engine.expire_hashes(1051, usize::MAX);
        assert!(!engine.entries.contains_key(&Bytes::from_static(b"h")));
        assert!(!engine.key_index.iter().any(|(_, key)| key.as_ref() == b"h"));
    }

    #[test]
    fn hash_scan_visits_continuous_fields_during_mutation_and_filters_binary_names() {
        let mut engine = Engine::new();
        for i in 0..80 {
            run(&mut engine, 1000, &["HSET", "h", &format!("f:{i}"), "v"]);
        }
        let mut cursor = String::from("0");
        let mut found = BTreeSet::new();
        loop {
            let Resp::Array(Some(reply)) =
                run(&mut engine, 1000, &["HSCAN", "h", &cursor, "COUNT", "3"])
            else {
                panic!("scan reply");
            };
            let Resp::Bulk(Some(next)) = &reply[0] else {
                panic!("cursor");
            };
            let Resp::Array(Some(fields)) = &reply[1] else {
                panic!("fields");
            };
            for pair in fields.as_chunks::<2>().0.iter() {
                let Resp::Bulk(Some(field)) = &pair[0] else {
                    panic!("field");
                };
                assert_eq!(pair[1], value("v"));
                found.insert(field.clone());
            }
            for i in 40..80 {
                run(&mut engine, 1000, &["HDEL", "h", &format!("f:{i}")]);
            }
            run(
                &mut engine,
                1000,
                &["HSET", "h", &format!("new:{cursor}"), "v"],
            );
            cursor = String::from_utf8(next.to_vec()).unwrap();
            if cursor == "0" {
                break;
            }
        }
        for i in 0..40 {
            assert!(found.contains(&Bytes::from(format!("f:{i}"))));
        }
        let args = [
            Bytes::from_static(b"HSET"),
            Bytes::from_static(b"binary"),
            Bytes::from_static(b"\xff\0x"),
            Bytes::from_static(b"data"),
        ];
        engine.execute_at(&args, 1000);
        let args = [
            Bytes::from_static(b"HSCAN"),
            Bytes::from_static(b"binary"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"MATCH"),
            Bytes::from_static(b"\xff?*"),
        ];
        assert_eq!(
            engine.execute_at(&args, 1000),
            array(vec![
                value("0"),
                array(vec![
                    Resp::Bulk(Some(Bytes::from_static(b"\xff\0x"))),
                    value("data")
                ])
            ])
        );
        let args = [
            Bytes::from_static(b"HSCAN"),
            Bytes::from_static(b"binary"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"NOVALUES"),
            Bytes::from_static(b"MATCH"),
            Bytes::from_static(b"\xff?*"),
            Bytes::from_static(b"COUNT"),
            Bytes::from_static(b"1"),
        ];
        assert_eq!(
            engine.execute_at(&args, 1000),
            array(vec![
                value("0"),
                array(vec![Resp::Bulk(Some(Bytes::from_static(b"\xff\0x")))])
            ])
        );
    }

    #[test]
    fn replay_preserves_resolved_values_and_deadlines_without_resurrecting_expired_fields() {
        let mut source = Engine::new();
        source.set_recording(true);
        let mut recovered = Engine::new();
        persist(
            &mut source,
            &mut recovered,
            1000,
            &[
                "HSETEX", "h", "PX", "10", "FIELDS", "2", "dead", "gone", "keep", "1",
            ],
        );
        persist(
            &mut source,
            &mut recovered,
            1001,
            &["HPERSIST", "h", "FIELDS", "1", "keep"],
        );
        assert_eq!(
            persist(&mut source, &mut recovered, 1010, &["HGET", "h", "dead"]),
            Resp::Bulk(None)
        );
        persist(
            &mut source,
            &mut recovered,
            1011,
            &["HINCRBY", "h", "keep", "4"],
        );
        persist(
            &mut source,
            &mut recovered,
            1012,
            &[
                "HSETEX", "h", "FNX", "FIELDS", "2", "keep", "bad", "new", "bad",
            ],
        );
        assert!(source.take_batch().is_none());
        assert_eq!(
            run(&mut recovered, 1012, &["HGETALL", "h"]),
            Resp::Map(vec![(value("keep"), value("5"))])
        );
        persist(
            &mut source,
            &mut recovered,
            1013,
            &["HPEXPIRE", "h", "10", "FIELDS", "1", "keep"],
        );
        assert_eq!(
            run(
                &mut recovered,
                1013,
                &["HPEXPIRETIME", "h", "FIELDS", "1", "keep"]
            ),
            ints(&[1023])
        );
        assert_eq!(
            persist(&mut source, &mut recovered, 1023, &["TYPE", "h"]),
            Resp::Simple("none")
        );
        assert_eq!(
            run(&mut recovered, 1023, &["TYPE", "h"]),
            Resp::Simple("none")
        );
        persist(
            &mut source,
            &mut recovered,
            1024,
            &["HSET", "h", "a", "1", "b", "2"],
        );
        persist(
            &mut source,
            &mut recovered,
            1025,
            &["HDEL", "h", "a", "a", "missing"],
        );
        assert_eq!(
            run(&mut recovered, 1025, &["HGETALL", "h"]),
            Resp::Map(vec![(value("b"), value("2"))])
        );
        assert!(recovered.take_batch().is_none());
    }
}
