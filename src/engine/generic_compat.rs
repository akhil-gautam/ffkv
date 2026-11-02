//! Redis generic operations and portable per-key value transfer.
use super::{
    CommandSpec, Engine, Entry, Mutation, NO_KEYS, ONE_KEY, ReplyBudget, Resp, Value, array, error,
    integer, integer_error, syntax_error, wrong_type,
};
use crate::{
    journal::Batch,
    rdb::{self, Object},
};
use bytes::Bytes;
use rand::Rng;
use std::{collections::VecDeque, io};

#[path = "generic_rdb.rs"]
pub(crate) mod rdb_snapshot;

#[cfg(test)]
#[path = "generic_tests.rs"]
mod tests;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler("copy", -3, &["write"], (1, 2, 1), Engine::cmd_copy),
    CommandSpec::handler("dump", 2, &["readonly"], ONE_KEY, Engine::cmd_dump),
    CommandSpec::handler("restore", -4, &["write"], ONE_KEY, Engine::cmd_restore),
    CommandSpec::handler(
        "restore-asking",
        -4,
        &["write"],
        ONE_KEY,
        Engine::cmd_restore,
    ),
    CommandSpec::handler("move", 3, &["write"], ONE_KEY, Engine::cmd_move),
    CommandSpec::handler("object", 3, &["readonly"], (2, 2, 1), Engine::cmd_object),
    CommandSpec::handler(
        "randomkey",
        1,
        &["readonly"],
        NO_KEYS,
        Engine::cmd_randomkey,
    ),
    CommandSpec::handler("sort", -2, &["write"], ONE_KEY, Engine::cmd_sort),
    CommandSpec::handler("sort_ro", -2, &["readonly"], ONE_KEY, Engine::cmd_sort),
];

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid ffkv value transfer")
}
fn codec_error(e: io::Error) -> Resp {
    Resp::Error(format!(
        "ERR DUMP payload version or checksum are wrong: {e}"
    ))
}
fn put_string(out: &mut Vec<u8>, value: &[u8], limit: usize) -> io::Result<()> {
    if out.len().saturating_add(value.len()).saturating_add(9) > limit {
        return Err(invalid());
    }
    rdb::write_string(out, value)
}

/// A per-key logical snapshot, including collection TTL and field deadlines.
/// Only reference-counted bytes are copied; cold queue/list data is read once.
pub(super) fn entry_mutations(key: &Bytes, entry: &Entry, now: i64) -> io::Result<Vec<Mutation>> {
    let mut rows = Vec::new();
    match &entry.value {
        Value::String(value) => rows.push(Mutation::Set {
            key: key.clone(),
            value: value.clone(),
            expires_at: entry.expires_at,
        }),
        Value::List(values) => {
            for value in values {
                rows.push(Mutation::Push {
                    key: key.clone(),
                    values: vec![value.read()?],
                    right: true,
                });
            }
        }
        Value::Set(values) => {
            for member in values.snapshot() {
                rows.push(Mutation::SetAdd {
                    key: key.clone(),
                    members: vec![member.clone()],
                });
            }
        }
        Value::Hash(values) => {
            for (field, value, expires_at) in values.snapshot(now) {
                rows.push(Mutation::HashSet {
                    key: key.clone(),
                    fields: vec![(field.clone(), value.clone(), expires_at)],
                });
            }
        }
        Value::SortedSet(values) => {
            for (member, score) in values.snapshot() {
                rows.push(Mutation::SortedSetAdd {
                    key: key.clone(),
                    entries: vec![(member.clone(), score)],
                });
            }
        }
        Value::Queue(value) => {
            for effect in value.snapshot_effects() {
                rows.push(Mutation::Queue {
                    key: key.clone(),
                    effect: effect?.encode()?,
                });
            }
        }
        Value::Typed(value) => {
            for effect in value.snapshot() {
                rows.push(Mutation::Typed {
                    key: key.clone(),
                    kind: value.kind(),
                    effect: effect?,
                });
            }
        }
    }
    if !matches!(entry.value, Value::String(_))
        && !rows.is_empty()
        && let Some(deadline) = entry.expires_at
    {
        rows.push(Mutation::Expire {
            key: key.clone(),
            expires_at: Some(deadline),
        });
    }
    Ok(rows)
}

fn entry_object(entry: &Entry, now: i64, limit: usize) -> io::Result<Object> {
    let mut budget = ReplyBudget::new(limit);
    let mut charge = |bytes: usize| budget.charge(1, bytes).map_err(|_| invalid());
    Ok(match &entry.value {
        Value::String(value) => {
            charge(value.len())?;
            Object::String(value.clone())
        }
        Value::List(values) => {
            let mut rows = Vec::with_capacity(values.len().min(1024));
            for value in values {
                charge(value.len())?;
                rows.push(value.read()?);
            }
            Object::List(rows)
        }
        Value::Set(values) => {
            let mut rows = Vec::new();
            for value in values.snapshot() {
                charge(value.len())?;
                rows.push(value.clone());
            }
            Object::Set(rows)
        }
        Value::SortedSet(values) => {
            let mut rows = Vec::new();
            for (value, score) in values.snapshot() {
                charge(value.len() + 8)?;
                rows.push((value.clone(), score));
            }
            Object::SortedSet(rows)
        }
        Value::Hash(values) => {
            let mut rows = Vec::new();
            let mut timed = false;
            for (field, value, deadline) in values.snapshot(now) {
                charge(field.len() + value.len() + 8)?;
                timed |= deadline.is_some();
                rows.push((field.clone(), value.clone(), deadline));
            }
            if !timed {
                Object::Hash(rows.into_iter().map(|(f, v, _)| (f, v)).collect())
            } else {
                Object::HashWithExpiry(rows)
            }
        }
        Value::Queue(queue) => {
            let mut data = b"FFKVEX\x01\x02".to_vec();
            for effect in queue.snapshot_effects() {
                let effect = effect?.encode()?;
                put_string(&mut data, &effect, limit)?;
            }
            Object::Module {
                version: 1,
                payload: Bytes::from(data),
            }
        }
        Value::Typed(value) => {
            let mut data = b"FFKVEX\x01\x03".to_vec();
            data.push(value.kind());
            for effect in value.snapshot() {
                put_string(&mut data, &effect?, limit)?;
            }
            Object::Module {
                version: 1,
                payload: Bytes::from(data),
            }
        }
    })
}

fn object_mutations(
    object: Object,
    key: &Bytes,
    _now: i64,
    limit: usize,
) -> io::Result<Vec<Mutation>> {
    let mut rows = Vec::new();
    match object {
        Object::String(value) => rows.push(Mutation::Set {
            key: key.clone(),
            value,
            expires_at: None,
        }),
        Object::List(values) => {
            if values.is_empty() {
                return Err(invalid());
            }
            rows.push(Mutation::Push {
                key: key.clone(),
                values,
                right: true,
            });
        }
        Object::Set(members) => {
            if members.is_empty() {
                return Err(invalid());
            }
            rows.push(Mutation::SetAdd {
                key: key.clone(),
                members,
            });
        }
        Object::Hash(fields) => {
            if fields.is_empty() {
                return Err(invalid());
            }
            rows.push(Mutation::HashSet {
                key: key.clone(),
                fields: fields.into_iter().map(|(f, v)| (f, v, None)).collect(),
            });
        }
        Object::HashWithExpiry(fields) => {
            if fields.is_empty() {
                return Err(invalid());
            }
            rows.push(Mutation::HashSet {
                key: key.clone(),
                fields,
            });
        }
        Object::SortedSet(entries) => {
            if entries.is_empty() {
                return Err(invalid());
            }
            rows.push(Mutation::SortedSetAdd {
                key: key.clone(),
                entries,
            });
        }
        Object::Module { version, payload } => {
            if version != 1 || !payload.starts_with(b"FFKVEX\x01") || payload.len() < 8 {
                return Err(invalid());
            }
            let mut d = rdb::Decoder::new(&payload[8..], limit);
            match payload[7] {
                2 => {
                    while d.remaining() > 0 {
                        rows.push(Mutation::Queue {
                            key: key.clone(),
                            effect: d.string()?,
                        });
                    }
                }
                3 => {
                    let kind = d.byte()?;
                    while d.remaining() > 0 {
                        rows.push(Mutation::Typed {
                            key: key.clone(),
                            kind,
                            effect: d.string()?,
                        });
                    }
                }
                _ => return Err(invalid()),
            }
            if d.remaining() != 0 {
                return Err(invalid());
            }
        }
    }
    if rows.is_empty() {
        return Err(invalid());
    }
    Ok(rows)
}

impl Engine {
    pub fn dump_key(&mut self, key: &Bytes, now: i64) -> Result<Option<Bytes>, Resp> {
        self.discard_expired(key, now);
        let result = self
            .entries
            .get(key)
            .map(|entry| {
                entry_object(entry, now, self.reply_limit)
                    .and_then(|value| rdb::encode_dump(&value, self.reply_limit))
            })
            .transpose();
        result.map_err(|e| {
            if e.kind() == io::ErrorKind::InvalidData {
                codec_error(e)
            } else {
                self.fail_io(e)
            }
        })
    }
    pub fn export_key(&mut self, key: &Bytes, now: i64) -> Result<Option<Bytes>, Resp> {
        let Some(dump) = self.dump_key(key, now)? else {
            return Ok(None);
        };
        let expiry = self
            .entries
            .get(key)
            .and_then(|e| e.expires_at)
            .unwrap_or(-1);
        let mut payload = Vec::with_capacity(14 + dump.len());
        payload.extend_from_slice(b"FFKEY\x01");
        payload.extend_from_slice(&expiry.to_le_bytes());
        payload.extend_from_slice(&dump);
        Ok(Some(Bytes::from(payload)))
    }
    pub fn import_key(&mut self, key: &Bytes, payload: &[u8], now: i64) -> Resp {
        if payload.len() < 14 || !payload.starts_with(b"FFKEY\x01") {
            return codec_error(invalid());
        }
        let deadline = i64::from_le_bytes(payload[6..14].try_into().expect("checked envelope"));
        self.restore_key(
            key,
            &payload[14..],
            (deadline >= 0).then_some(deadline),
            true,
            now,
        )
    }
    /// Decode and stage completely before replacing an existing destination.
    pub fn restore_key(
        &mut self,
        key: &Bytes,
        data: &[u8],
        expires_at: Option<i64>,
        replace: bool,
        now: i64,
    ) -> Resp {
        self.discard_expired(key, now);
        if !replace && self.entries.contains_key(key) {
            return error("BUSYKEY Target key name already exists.");
        }
        let object = match rdb::decode_dump(data, self.reply_limit) {
            Ok(o) => o,
            Err(e) => return codec_error(e),
        };
        let key = self.own_key(key);
        let rows = match object_mutations(object, &key, now, self.reply_limit) {
            Ok(r) => r,
            Err(e) => return codec_error(e),
        };
        let mut staged = Engine::new();
        staged.storage = self.storage.clone();
        if let Err(e) = staged.replay(Batch {
            timestamp_ms: now,
            mutations: rows,
        }) {
            return if e.kind() == io::ErrorKind::InvalidData {
                codec_error(e)
            } else {
                self.fail_io(e)
            };
        }
        let Some(mut entry) = staged.remove(&key) else {
            self.resolve_held_write(&key, now);
            if self.remove(&key).is_some() {
                self.record(now, || Mutation::Delete { key: key.clone() });
            }
            return Resp::Simple("OK");
        };
        entry.expires_at = expires_at;
        entry.access = super::AccessMetadata::new(now);
        let rows = if expires_at.is_some_and(|d| d <= now) {
            Vec::new()
        } else {
            match entry_mutations(&key, &entry, now) {
                Ok(r) => r,
                Err(e) => return self.fail_io(e),
            }
        };
        self.resolve_held_write(&key, now);
        if self.remove(&key).is_some() {
            self.record(now, || Mutation::Delete { key: key.clone() });
        }
        if !rows.is_empty() {
            self.write_entry(key.clone(), entry, now);
            for row in rows {
                self.record(now, || row);
            }
        }
        Resp::Simple("OK")
    }
    pub fn prepare_migrate(
        &mut self,
        keys: &[Bytes],
        now: i64,
    ) -> Result<Vec<crate::migrate::MigrateItem>, Resp> {
        let mut items = Vec::new();
        let mut budget = ReplyBudget::new(self.reply_limit);
        for key in keys {
            if items
                .iter()
                .any(|item: &crate::migrate::MigrateItem| item.key == *key)
            {
                continue;
            }
            if let Some(dump) = self.dump_key(key, now)? {
                budget.charge(1, key.len() + dump.len())?;
                items.push(crate::migrate::MigrateItem {
                    key: key.clone(),
                    dump,
                    expires_at: self.entries.get(key).and_then(|e| e.expires_at),
                });
            }
        }
        Ok(items)
    }
    /// Parent holds the source-key write barrier from capture through acknowledgment.
    pub fn ack_migrate(
        &mut self,
        items: &[crate::migrate::MigrateItem],
        indices: &[usize],
        now: i64,
    ) -> Resp {
        for &index in indices {
            let Some(item) = items.get(index) else {
                return error("ERR invalid migration acknowledgment");
            };
            self.discard_expired(&item.key, now);
            // An unlocked caller must never remove a concurrent replacement.
            let same = match self.dump_key(&item.key, now) {
                Ok(Some(dump)) => {
                    dump == item.dump
                        && self.entries.get(&item.key).and_then(|e| e.expires_at) == item.expires_at
                }
                Ok(None) => false,
                Err(e) => return e,
            };
            if same && self.remove(&item.key).is_some() {
                self.record(now, || Mutation::Delete {
                    key: item.key.clone(),
                });
            }
        }
        Resp::Simple("OK")
    }
    fn cmd_dump(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.dump_key(&args[1], now) {
            Ok(value) => Resp::Bulk(value),
            Err(e) => e,
        }
    }
    fn cmd_restore(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(ttl) = integer(&args[2]).filter(|t| *t >= 0) else {
            return error("ERR Invalid TTL value, must be >= 0");
        };
        let (mut replace, mut absolute, mut idle, mut frequency) = (false, false, None, None);
        let mut i = 4;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"REPLACE") {
                replace = true;
                i += 1;
            } else if args[i].eq_ignore_ascii_case(b"ABSTTL") {
                absolute = true;
                i += 1;
            } else if args[i].eq_ignore_ascii_case(b"IDLETIME") && i + 1 < args.len() {
                let Some(v) = integer(&args[i + 1]).filter(|v| *v >= 0) else {
                    return integer_error();
                };
                if idle.is_some() || frequency.is_some() {
                    return syntax_error();
                }
                idle = Some(v);
                i += 2;
            } else if args[i].eq_ignore_ascii_case(b"FREQ") && i + 1 < args.len() {
                let Some(v) = integer(&args[i + 1]).filter(|v| (0..=255).contains(v)) else {
                    return integer_error();
                };
                if idle.is_some() || frequency.is_some() {
                    return syntax_error();
                }
                frequency = Some(v as u8);
                i += 2;
            } else {
                return syntax_error();
            }
        }
        let deadline = if ttl == 0 {
            None
        } else if absolute {
            Some(ttl)
        } else {
            match now.checked_add(ttl) {
                Some(d) => Some(d),
                None => return error("ERR Invalid TTL value, must be >= 0"),
            }
        };
        let result = self.restore_key(&args[1], &args[3], deadline, replace, now);
        if matches!(result, Resp::Simple("OK"))
            && let Some(entry) = self.entries.get_mut(&args[1])
        {
            if let Some(idle) = idle {
                entry.access.last_access_ms = now.saturating_sub(idle.saturating_mul(1000));
            }
            if let Some(frequency) = frequency {
                entry.access.frequency = frequency;
            }
        }
        result
    }
    fn cmd_randomkey(&mut self, _: &[Bytes], now: i64) -> Resp {
        self.expire_at(now, usize::MAX);
        self.expire_hashes(now, usize::MAX);
        let mut selected = None;
        let mut rng = rand::rng();
        for (index, key) in self.entries.keys().enumerate() {
            if rng.random_range(0..=index) == 0 {
                selected = Some(key.clone());
            }
        }
        Resp::Bulk(selected)
    }
    fn cmd_object(&mut self, args: &[Bytes], now: i64) -> Resp {
        if ![b"ENCODING".as_slice(), b"FREQ", b"IDLETIME", b"REFCOUNT"]
            .iter()
            .any(|kind| args[1].eq_ignore_ascii_case(kind))
        {
            return error("ERR Unknown subcommand or wrong number of arguments for OBJECT");
        }
        self.discard_expired(&args[2], now);
        let Some(entry) = self.entries.get(&args[2]) else {
            return Resp::Bulk(None);
        };
        if args[1].eq_ignore_ascii_case(b"FREQ") {
            return Resp::Integer(i64::from(entry.access.frequency));
        }
        if args[1].eq_ignore_ascii_case(b"IDLETIME") {
            return Resp::Integer(now.saturating_sub(entry.access.last_access_ms).max(0) / 1000);
        }
        if args[1].eq_ignore_ascii_case(b"REFCOUNT") {
            return Resp::Integer(1);
        } // Entry uniquely owns its Value; Bytes sharing is not Redis object sharing.
        let encoding = match &entry.value {
            Value::String(_) => "raw",
            Value::List(_) => "deque",
            Value::Set(_) => "hashtable",
            Value::Hash(_) => "hashtable",
            Value::SortedSet(_) => "btree",
            Value::Queue(_) => "ffkv-queue",
            Value::Typed(_) => "ffkv-module",
        };
        Resp::Bulk(Some(Bytes::from_static(encoding.as_bytes())))
    }
    fn cmd_copy(&mut self, args: &[Bytes], now: i64) -> Resp {
        let source_db = self.database();
        let (mut destination_db, mut replace) = (source_db, false);
        let mut i = 3;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"REPLACE") {
                replace = true;
                i += 1;
            } else if args[i].eq_ignore_ascii_case(b"DB") && i + 1 < args.len() {
                let Some(db) = integer(&args[i + 1])
                    .filter(|db| *db >= 0 && *db < i64::from(self.database_count()))
                else {
                    return error("ERR DB index is out of range");
                };
                destination_db = db as u32;
                i += 2;
            } else {
                return syntax_error();
            }
        }
        if source_db == destination_db && args[1] == args[2] {
            return error("ERR source and destination objects are the same");
        }
        self.discard_expired(&args[1], now);
        if !self.entries.contains_key(&args[1]) {
            return Resp::Integer(0);
        }
        self.switch_database(destination_db);
        self.discard_expired(&args[2], now);
        let occupied = self.entries.contains_key(&args[2]);
        self.switch_database(source_db);
        if occupied && !replace {
            return Resp::Integer(0);
        }
        let payload = match self.export_key(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => return Resp::Integer(0),
            Err(e) => return e,
        };
        self.switch_database(destination_db);
        let result = match self.import_key(&args[2], &payload, now) {
            Resp::Simple("OK") => Resp::Integer(1),
            other => other,
        };
        self.switch_database(source_db);
        result
    }
    fn cmd_move(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(db) =
            integer(&args[2]).filter(|db| *db >= 0 && *db < i64::from(self.database_count()))
        else {
            return error("ERR DB index is out of range");
        };
        let source = self.database();
        let db = db as u32;
        if db == source {
            return error("ERR source and destination objects are the same");
        }
        self.discard_expired(&args[1], now);
        if !self.entries.contains_key(&args[1]) {
            return Resp::Integer(0);
        }
        self.switch_database(db);
        self.discard_expired(&args[1], now);
        let occupied = self.entries.contains_key(&args[1]);
        self.switch_database(source);
        if occupied {
            return Resp::Integer(0);
        }
        let rows = match entry_mutations(
            &args[1],
            self.entries.get(&args[1]).expect("checked MOVE source"),
            now,
        ) {
            Ok(rows) => rows,
            Err(e) => return self.fail_io(e),
        };
        let entry = self.remove(&args[1]).expect("checked MOVE source");
        self.record(now, || Mutation::Delete {
            key: args[1].clone(),
        });
        self.switch_database(db);
        self.write_entry(args[1].clone(), entry, now);
        for row in rows {
            self.record(now, || row);
        }
        self.switch_database(source);
        Resp::Integer(1)
    }
}

#[derive(Default)]
struct SortOptions<'a> {
    by: Option<&'a Bytes>,
    gets: Vec<&'a Bytes>,
    offset: usize,
    count: Option<usize>,
    alpha: bool,
    descending: bool,
    store: Option<&'a Bytes>,
}
impl Engine {
    #[expect(
        clippy::too_many_arguments,
        reason = "SORT lookup shares its pattern, member, ACL proof, budget, and on-demand shard resolver."
    )]
    fn sort_lookup(
        &mut self,
        pattern: &Bytes,
        member: &Bytes,
        now: i64,
        command: &Bytes,
        access_keys: &mut Vec<Bytes>,
        budget: &mut ReplyBudget,
        lookup: &mut impl FnMut(&Bytes, Option<&[u8]>, i64) -> Result<Option<Bytes>, Resp>,
    ) -> Result<Option<Bytes>, Resp> {
        if pattern.as_ref() == b"#" {
            return Ok(Some(member.clone()));
        }
        let Some(star) = pattern.iter().position(|b| *b == b'*') else {
            return Ok(None);
        };
        let suffix = &pattern[star + 1..];
        let arrow = suffix.windows(2).position(|w| w == b"->");
        let (key_suffix, field) =
            arrow.map_or((suffix, None), |p| (&suffix[..p], Some(&suffix[p + 2..])));
        let length = star
            .saturating_add(member.len())
            .saturating_add(key_suffix.len());
        budget.charge(1, length)?;
        let mut key = Vec::with_capacity(length);
        key.extend_from_slice(&pattern[..star]);
        key.extend_from_slice(member);
        key.extend_from_slice(key_suffix);
        let key = Bytes::from(key);
        if let Some(user) = &self.command_user {
            let categories = if command.eq_ignore_ascii_case(b"SORT_RO") {
                ["read", "slow"]
            } else {
                ["write", "slow"]
            };
            user.authorize_access_logged(
                command,
                &categories,
                &[(&key, crate::auth::KeyAccess::Read)],
                false,
                &[],
                false,
                false,
                self.command_audit().as_ref(),
            )
            .map_err(|_| {
                error("NOPERM this user has no permissions to access a SORT lookup key")
            })?;
            access_keys.push(key.clone());
        }
        match self.sort_value_at(&key, field, now) {
            Some(value) => Ok(value),
            None => lookup(&key, field, now),
        }
    }
    fn cmd_sort(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.cmd_sort_using(args, now, &mut |_, _, _| Ok(None))
    }

    /// Execute SORT against its source/store view, resolving only the external
    /// BY keys and selected GET keys actually read by the command.
    pub(crate) fn execute_sort_with_lookup(
        &mut self,
        args: &[Bytes],
        now: i64,
        mut lookup: impl FnMut(&Bytes, Option<&[u8]>, i64) -> Result<Option<Bytes>, Resp>,
    ) -> io::Result<Resp> {
        debug_assert!(
            self.pending_batch.is_none(),
            "drain the previous journal batch"
        );
        self.prepare_replay_args(args);
        let reply = match Self::validate(args) {
            Ok(()) => self.cmd_sort_using(args, now, &mut lookup),
            Err(reply) => reply,
        };
        self.finish_search_changes(now);
        self.finish_access_metadata(args, now);
        match self.take_failure() {
            Some(error) => Err(error),
            None => Ok(reply),
        }
    }

    /// `None` means the key is outside this execution view. A present key of an
    /// unsupported type still resolves locally to a missing scalar.
    fn sort_value_at(
        &mut self,
        key: &Bytes,
        field: Option<&[u8]>,
        now: i64,
    ) -> Option<Option<Bytes>> {
        let value = match self.value(key, now) {
            None => return None,
            Some(Value::String(value)) if field.is_none() => Some(value.clone()),
            Some(Value::Hash(value)) if field.is_some() => value
                .snapshot(now)
                .find(|(name, _, _)| name.as_ref() == field.expect("checked field"))
                .map(|(_, value, _)| value.clone()),
            Some(_) => None,
        };
        self.touch_key_access(key, now);
        Some(value)
    }

    pub(crate) fn sort_lookup_value(
        &mut self,
        key: &Bytes,
        field: Option<&[u8]>,
        now: i64,
    ) -> io::Result<Result<Option<Bytes>, Resp>> {
        let value = self.sort_value_at(key, field, now).flatten();
        self.finish_search_changes(now);
        match self.take_failure() {
            Some(error) => Err(error),
            None => Ok(Ok(value)),
        }
    }

    fn cmd_sort_using(
        &mut self,
        args: &[Bytes],
        now: i64,
        lookup: &mut impl FnMut(&Bytes, Option<&[u8]>, i64) -> Result<Option<Bytes>, Resp>,
    ) -> Resp {
        let mut o = SortOptions::default();
        let mut i = 2;
        while i < args.len() {
            let option = &args[i];
            if option.eq_ignore_ascii_case(b"ALPHA") {
                o.alpha = true;
                i += 1;
            } else if option.eq_ignore_ascii_case(b"ASC") {
                o.descending = false;
                i += 1;
            } else if option.eq_ignore_ascii_case(b"DESC") {
                o.descending = true;
                i += 1;
            } else if option.eq_ignore_ascii_case(b"BY") && i + 1 < args.len() {
                o.by = Some(&args[i + 1]);
                i += 2;
            } else if option.eq_ignore_ascii_case(b"GET") && i + 1 < args.len() {
                o.gets.push(&args[i + 1]);
                i += 2;
            } else if option.eq_ignore_ascii_case(b"LIMIT") && i + 2 < args.len() {
                let (Some(offset), Some(count)) = (integer(&args[i + 1]), integer(&args[i + 2]))
                else {
                    return integer_error();
                };
                o.offset = offset.max(0) as usize;
                o.count = (count >= 0).then_some(count as usize);
                i += 3;
            } else if option.eq_ignore_ascii_case(b"STORE")
                && i + 1 < args.len()
                && !args[0].eq_ignore_ascii_case(b"SORT_RO")
            {
                o.store = Some(&args[i + 1]);
                i += 2;
            } else {
                return syntax_error();
            }
        }
        if let Some(destination) = o.store
            && let Some(user) = &self.command_user
            && user
                .authorize_access_logged(
                    b"sort",
                    &["write", "slow"],
                    &[(destination, crate::auth::KeyAccess::Write)],
                    false,
                    &[],
                    false,
                    false,
                    self.command_audit().as_ref(),
                )
                .is_err()
        {
            return error("NOPERM this user has no permissions to access the SORT destination");
        }
        let limit = self.reply_limit;
        let mut budget = ReplyBudget::new(limit);
        let mut members = Vec::new();
        let mut access_keys = if self.command_user.is_some() {
            vec![args[1].clone()]
        } else {
            Vec::new()
        };
        macro_rules! add {
            ($value:expr) => {{
                let value = $value;
                if let Err(e) = budget.charge(1, value.len()) {
                    return e;
                }
                members.push(value);
            }};
        }
        match self.value(&args[1], now) {
            None => {}
            Some(Value::List(values)) => {
                for v in values {
                    if let Err(e) = budget.charge(1, v.len()) {
                        return e;
                    }
                    let value = match v.read() {
                        Ok(v) => v,
                        Err(e) => return self.fail_io(e),
                    };
                    members.push(value);
                }
            }
            Some(Value::Set(values)) => {
                for v in values.snapshot() {
                    add!(v.clone());
                }
            }
            Some(Value::SortedSet(values)) => {
                let mut sorted = Vec::new();
                for (v, score) in values.snapshot() {
                    if let Err(e) = budget.charge(1, v.len() + 8) {
                        return e;
                    }
                    sorted.push((v.clone(), score));
                }
                sorted.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
                members.extend(sorted.into_iter().map(|(v, _)| v));
            }
            Some(_) => return wrong_type(),
        }
        let no_sort = o.by.is_some_and(|pattern| !pattern.contains(&b'*'));
        if !no_sort {
            let mut weighted = Vec::with_capacity(members.len());
            for member in members {
                let value = match o.by {
                    Some(pattern) => match self.sort_lookup(
                        pattern,
                        &member,
                        now,
                        &args[0],
                        &mut access_keys,
                        &mut budget,
                        lookup,
                    ) {
                        Ok(v) => v,
                        Err(e) => return e,
                    },
                    None => Some(member.clone()),
                };
                let score = if o.alpha {
                    0.0
                } else {
                    match value.as_ref() {
                        None => 0.0,
                        Some(v) => match std::str::from_utf8(v)
                            .ok()
                            .and_then(|v| v.parse::<f64>().ok())
                            .filter(|v| !v.is_nan())
                        {
                            Some(v) => {
                                if v == 0.0 {
                                    0.0
                                } else {
                                    v
                                }
                            }
                            None => {
                                return error(
                                    "ERR One or more scores can't be converted into double",
                                );
                            }
                        },
                    }
                };
                weighted.push((member, value, score));
            }
            weighted.sort_unstable_by(|a, b| {
                let order = if o.alpha {
                    a.1.cmp(&b.1)
                } else {
                    a.2.total_cmp(&b.2)
                }
                .then_with(|| a.0.cmp(&b.0));
                if o.descending { order.reverse() } else { order }
            });
            members = weighted.into_iter().map(|(m, _, _)| m).collect();
        }
        let mut result = Vec::new();
        let mut result_budget = ReplyBudget::new(limit);
        for member in members
            .into_iter()
            .skip(o.offset)
            .take(o.count.unwrap_or(usize::MAX))
        {
            if o.gets.is_empty() {
                if let Err(e) = result_budget.charge(1, member.len()) {
                    return e;
                }
                result.push(Some(member));
            } else {
                for pattern in &o.gets {
                    let value = match self.sort_lookup(
                        pattern,
                        &member,
                        now,
                        &args[0],
                        &mut access_keys,
                        &mut budget,
                        lookup,
                    ) {
                        Ok(v) => v,
                        Err(e) => return e,
                    };
                    if let Err(e) = result_budget.charge(1, value.as_ref().map_or(0, Bytes::len)) {
                        return e;
                    }
                    result.push(value);
                }
            }
        }
        if let Some(user) = &self.command_user {
            use crate::auth::KeyAccess;
            let mut accesses = access_keys
                .iter()
                .map(|key| (key, KeyAccess::Read))
                .collect::<Vec<_>>();
            if let Some(destination) = o.store {
                accesses.push((destination, KeyAccess::Write));
            }
            let categories = if args[0].eq_ignore_ascii_case(b"SORT_RO") {
                ["read", "slow"]
            } else {
                ["write", "slow"]
            };
            if user
                .authorize_access_logged(
                    &args[0],
                    &categories,
                    &accesses,
                    false,
                    &[],
                    false,
                    false,
                    self.command_audit().as_ref(),
                )
                .is_err()
            {
                return error("NOPERM this user has no permissions to access the SORT keys");
            }
        }
        if let Some(destination) = o.store {
            let values = result
                .into_iter()
                .map(|v| v.unwrap_or_default())
                .collect::<Vec<_>>();
            let length = values.len();
            let mut payloads = VecDeque::with_capacity(length);
            for value in &values {
                match self.payload(value.clone()) {
                    Ok(payload) => payloads.push_back(payload),
                    Err(e) => return self.fail_io(e),
                }
            }
            let mut staged = Engine::new();
            if length > 0 {
                staged.collection_entry(destination, now, || Value::List(payloads));
            }
            self.resolve_held_write(destination, now);
            if self.remove(destination).is_some() {
                self.record(now, || Mutation::Delete {
                    key: destination.clone(),
                });
            }
            if length > 0 {
                let entry = staged.remove(destination).expect("staged SORT list");
                let key = self.own_key(destination);
                self.write_entry(key.clone(), entry, now);
                self.record(now, || Mutation::Push {
                    key,
                    values,
                    right: true,
                });
            }
            Resp::Integer(length as i64)
        } else {
            array(result.into_iter().map(Resp::Bulk).collect())
        }
    }
}
