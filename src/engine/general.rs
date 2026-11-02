use std::io;

use crate::storage::Payload;
use bytes::Bytes;

use super::scan::{glob_matches, parse_scan, scan_entries};
use super::{
    ALL_KEYS, Command, CommandSpec, Engine, Execution, Mutation, NO_KEYS, ONE_KEY, Resp, Value,
    array, error, integer, integer_error, invalid_journal, syntax_error, wrong_type,
};

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler("scan", -2, &["readonly"], NO_KEYS, Engine::cmd_scan),
    CommandSpec::handler("keys", 2, &["readonly"], NO_KEYS, Engine::cmd_keys),
    CommandSpec::handler(
        "touch",
        -2,
        &["readonly", "fast"],
        ALL_KEYS,
        Engine::cmd_touch,
    ),
    CommandSpec::handler("unlink", -2, &["write"], ALL_KEYS, Engine::cmd_unlink),
    CommandSpec::handler("flushdb", -1, &["write"], NO_KEYS, Engine::cmd_flush).max(2),
    CommandSpec::handler("flushall", -1, &["write"], NO_KEYS, Engine::cmd_flush).max(2),
    CommandSpec::new(Command::Rename, "rename", 3, &["write"], (1, 2, 1)),
    CommandSpec::new(Command::Renamenx, "renamenx", 3, &["write"], (1, 2, 1)),
    CommandSpec::new(Command::Lpushx, "lpushx", -3, &["write"], ONE_KEY),
    CommandSpec::new(Command::Rpushx, "rpushx", -3, &["write"], ONE_KEY),
    CommandSpec::new(Command::Lmove, "lmove", 5, &["write"], (1, 2, 1)),
    CommandSpec::new(
        Command::Blmove,
        "blmove",
        6,
        &["write", "blocking"],
        (1, 2, 1),
    ),
    CommandSpec::handler("lindex", 3, &["readonly"], ONE_KEY, Engine::cmd_lindex),
    CommandSpec::handler("lset", 4, &["write"], ONE_KEY, Engine::cmd_lset),
    CommandSpec::handler("ltrim", 4, &["write"], ONE_KEY, Engine::cmd_ltrim),
    CommandSpec::handler("lrem", 4, &["write"], ONE_KEY, Engine::cmd_lrem),
];

pub(super) fn normalized_range(length: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
    let length = length as i128;
    let start = if start < 0 {
        length + i128::from(start)
    } else {
        i128::from(start)
    }
    .max(0);
    let stop = if stop < 0 {
        length + i128::from(stop)
    } else {
        i128::from(stop)
    }
    .min(length - 1);
    (start <= stop).then_some((start as usize, stop as usize))
}

fn normalized_index(length: usize, index: i64) -> Option<usize> {
    let index = if index < 0 {
        length as i128 + i128::from(index)
    } else {
        i128::from(index)
    };
    (index >= 0 && index < length as i128).then_some(index as usize)
}

fn kind(value: &Value) -> &'static [u8] {
    match value {
        Value::String(_) => b"string",
        Value::List(_) => b"list",
        Value::Set(_) => b"set",
        Value::Hash(_) => b"hash",
        Value::SortedSet(_) => b"zset",
        Value::Queue(_) => b"ffkv-queue",
        Value::Typed(value) => value.type_name().as_bytes(),
    }
}

fn remove_matching(
    list: &mut std::collections::VecDeque<Payload>,
    value: &Bytes,
    count: i64,
) -> io::Result<usize> {
    let limit = if count == 0 {
        usize::MAX
    } else {
        usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX)
    };
    let mut checksum = None;
    let mut matches = |candidate: &Payload| {
        let crc = if candidate.is_disk() {
            *checksum.get_or_insert_with(|| crc32c::crc32c(value))
        } else {
            0
        };
        candidate.matches(value, crc)
    };
    if count >= 0 || limit >= list.len() {
        let mut removed = 0;
        let mut failure = None;
        list.retain(|candidate| {
            if removed == limit || failure.is_some() {
                return true;
            }
            match matches(candidate) {
                Ok(true) => {
                    removed += 1;
                    false
                }
                Ok(false) => true,
                Err(error) => {
                    failure = Some(error);
                    true
                }
            }
        });
        return match failure {
            Some(error) => Err(error),
            None => Ok(removed),
        };
    }
    // Read from the requested end once, then retain by index. No cold body is
    // reread merely to count matches, and surviving payload order is unchanged.
    let mut indexes = Vec::new();
    for (index, candidate) in list.iter().enumerate().rev() {
        if matches(candidate)? {
            indexes.push(index);
            if indexes.len() == limit {
                break;
            }
        }
    }
    let removed = indexes.len();
    let mut next = indexes.pop();
    let mut index = 0;
    list.retain(|_| {
        let keep = next != Some(index);
        if !keep {
            next = indexes.pop();
        }
        index += 1;
        keep
    });
    Ok(removed)
}

impl Engine {
    fn cmd_scan(&mut self, args: &[Bytes], now: i64) -> Resp {
        let options = match parse_scan(args, 1, true) {
            Ok(options) => options,
            Err(error) => return error,
        };
        let (cursor, candidates) = match scan_entries(&self.key_index, &options, self.reply_limit) {
            Ok(page) => page,
            Err(error) => return error,
        };
        let values = candidates
            .into_iter()
            .filter_map(|key| {
                self.discard_expired(&key, now);
                let value = &self.entries.get(&key)?.value;
                options
                    .kind
                    .as_ref()
                    .is_none_or(|expected| expected.eq_ignore_ascii_case(kind(value)))
                    .then_some(Resp::Bulk(Some(key)))
            })
            .collect();
        array(vec![
            Resp::Bulk(Some(Bytes::from(cursor.to_string()))),
            array(values),
        ])
    }

    fn cmd_keys(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.expire_at(now, usize::MAX);
        self.expire_hashes(now, usize::MAX);
        super::bounded_replies(
            self.key_index
                .iter()
                .filter(|(_, key)| glob_matches(&args[1], key))
                .map(|(_, key)| Resp::Bulk(Some(key.clone()))),
            self.reply_limit,
        )
    }

    fn cmd_touch(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut present = 0;
        for key in &args[1..] {
            self.discard_expired(key, now);
            present += i64::from(self.entries.contains_key(key));
        }
        Resp::Integer(present)
    }

    fn cmd_unlink(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut removed = 0;
        for key in &args[1..] {
            self.discard_expired(key, now);
            if let Some(entry) = self.remove(key) {
                super::lazy_free::entry(entry);
                removed += 1;
                self.record(now, || Mutation::Delete { key: key.clone() });
            }
        }
        Resp::Integer(removed)
    }

    fn cmd_flush(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() > 2
            || args.get(1).is_some_and(|option| {
                !option.eq_ignore_ascii_case(b"SYNC") && !option.eq_ignore_ascii_case(b"ASYNC")
            })
        {
            return syntax_error();
        }
        let asynchronous = args
            .get(1)
            .is_some_and(|option| option.eq_ignore_ascii_case(b"ASYNC"));
        if args[0].eq_ignore_ascii_case(b"FLUSHALL") {
            let selected = self.active_database;
            let databases = self.database_ids().collect::<Vec<_>>();
            for database in databases {
                self.switch_database(database);
                self.flush_database(now, asynchronous);
            }
            self.switch_database(selected);
        } else {
            self.flush_database(now, asynchronous);
        }
        Resp::Simple("OK")
    }

    fn flush_database(&mut self, now: i64, asynchronous: bool) {
        if !self.entries.is_empty()
            || !self.held_entries.is_empty()
            || (self.active_database == 0
                && self
                    .search
                    .as_ref()
                    .is_some_and(|catalog| catalog.has_indexes()))
        {
            if self.ready_tracking {
                let database = self.active_database;
                self.ready_keys
                    .extend(
                        self.entries
                            .iter()
                            .filter_map(|(key, entry)| match &entry.value {
                                Value::Typed(value)
                                    if matches!(
                                        value.as_ref(),
                                        super::typed::Data::Stream(_)
                                            | super::typed::Data::TimeSeries(_)
                                    ) =>
                                {
                                    Some((database, key.clone()))
                                }
                                _ => None,
                            }),
                    );
            }
            if asynchronous {
                super::lazy_free::database(self.take_database());
            } else {
                self.clear_entries();
            }
            self.record(now, || Mutation::Clear);
        }
    }

    pub(super) fn rename(&mut self, args: &[Bytes], now: i64, nx: bool) -> Execution {
        let (from, to) = (&args[1], &args[2]);
        self.discard_expired(from, now);
        if !self.entries.contains_key(from) {
            return Execution::Reply(error("ERR no such key"));
        }
        if from == to {
            return Execution::Reply(if nx {
                Resp::Integer(0)
            } else {
                Resp::Simple("OK")
            });
        }
        self.discard_expired(to, now);
        if nx && self.entries.contains_key(to) {
            return Execution::Reply(Resp::Integer(0));
        }
        let entry = self.remove(from).expect("checked rename source");
        self.remove(to);
        self.write_entry(to.clone(), entry, now);
        self.record(now, || Mutation::Rename {
            from: from.clone(),
            to: to.clone(),
        });
        let reply = if nx {
            Resp::Integer(1)
        } else {
            Resp::Simple("OK")
        };
        if matches!(self.entries.get(to).map(|entry| &entry.value), Some(Value::List(list)) if !list.is_empty())
        {
            Execution::Pushed {
                reply,
                key: to.clone(),
            }
        } else {
            Execution::Reply(reply)
        }
    }

    pub(super) fn replay_rename(&mut self, from: Bytes, to: Bytes, now: i64) -> io::Result<()> {
        if from == to {
            return Err(invalid_journal());
        }
        self.discard_expired(&from, now);
        let entry = self.remove(&from).ok_or_else(invalid_journal)?;
        self.remove(&to);
        self.insert_entry_raw(to, entry);
        Ok(())
    }

    pub(super) fn list_move(&mut self, args: &[Bytes], blocking: bool, now: i64) -> Execution {
        let side = |value: &Bytes| {
            if value.eq_ignore_ascii_case(b"LEFT") {
                Some(false)
            } else if value.eq_ignore_ascii_case(b"RIGHT") {
                Some(true)
            } else {
                None
            }
        };
        let (Some(source_right), Some(destination_right)) = (side(&args[3]), side(&args[4])) else {
            return Execution::Reply(syntax_error());
        };
        let timeout = if blocking {
            match super::blocking_timeout(&args[5], now) {
                Ok(timeout) => timeout,
                Err(reply) => return Execution::Reply(reply),
            }
        } else {
            None
        };
        match self.try_move_at(&args[1], &args[2], source_right, destination_right, now) {
            Ok(Some(value)) => Execution::Pushed {
                reply: Resp::Bulk(Some(value)),
                key: args[2].clone(),
            },
            Ok(None) if blocking => Execution::Block(super::BlockingPop {
                keys: vec![args[1].clone()],
                right: source_right,
                timeout,
                destination: Some((args[2].clone(), destination_right)),
                kind: super::BlockingKind::ListPop,
            }),
            Ok(None) => Execution::Reply(Resp::Bulk(None)),
            Err(reply) => Execution::Reply(reply),
        }
    }

    pub(super) fn try_move_at(
        &mut self,
        source: &Bytes,
        destination: &Bytes,
        source_right: bool,
        destination_right: bool,
        now: i64,
    ) -> Result<Option<Bytes>, Resp> {
        self.discard_expired(source, now);
        let Some(entry) = self.entries.get(source) else {
            return Ok(None);
        };
        let Value::List(list) = &entry.value else {
            return Err(wrong_type());
        };
        if list.is_empty() {
            return Ok(None);
        }
        if source != destination {
            self.discard_expired(destination, now);
            if self
                .entries
                .get(destination)
                .is_some_and(|entry| !matches!(entry.value, Value::List(_)))
            {
                return Err(wrong_type());
            }
        }
        let entry = self.entries.get(source).expect("validated source list");
        let Value::List(list) = &entry.value else {
            unreachable!();
        };
        let restore_expiry = (source == destination && list.len() == 1)
            .then_some(entry.expires_at)
            .flatten();
        let value = match if source_right {
            list.back()
        } else {
            list.front()
        }
        .expect("nonempty source list")
        .read()
        {
            Ok(value) => value,
            Err(error) => return Err(self.fail_io(error)),
        };
        let entry = self.entries.get_mut(source).expect("validated source list");
        let Value::List(list) = &mut entry.value else {
            unreachable!();
        };
        let payload = if source_right {
            list.pop_back()
        } else {
            list.pop_front()
        }
        .expect("nonempty source list");
        if source == destination {
            if destination_right {
                list.push_back(payload);
            } else {
                list.push_front(payload);
            }
        } else {
            if list.is_empty() {
                self.remove(source);
            }
            let Value::List(list) = self.collection_entry(destination, now, || {
                Value::List(std::collections::VecDeque::new())
            }) else {
                unreachable!();
            };
            if destination_right {
                list.push_back(payload);
            } else {
                list.push_front(payload);
            }
        }
        self.record(now, || Mutation::Pop {
            key: source.clone(),
            count: 1,
            right: source_right,
        });
        self.record(now, || Mutation::Push {
            key: destination.clone(),
            values: vec![value.clone()],
            right: destination_right,
        });
        if let Some(expires_at) = restore_expiry {
            self.record(now, || Mutation::Expire {
                key: source.clone(),
                expires_at: Some(expires_at),
            });
        }
        Ok(Some(value))
    }

    fn cmd_lindex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(index) = integer(&args[2]) else {
            return integer_error();
        };
        match self.value(&args[1], now) {
            Some(Value::List(list)) => {
                match normalized_index(list.len(), index)
                    .map(|index| list[index].read())
                    .transpose()
                {
                    Ok(value) => Resp::Bulk(value),
                    Err(error) => self.fail_io(error),
                }
            }
            Some(_) => wrong_type(),
            None => Resp::Bulk(None),
        }
    }

    fn cmd_lset(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(index) = integer(&args[2]) else {
            return integer_error();
        };
        let key = &args[1];
        let storage = self.storage.clone();
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return error("ERR no such key");
        };
        let Value::List(list) = &mut entry.value else {
            return wrong_type();
        };
        let Some(index) = normalized_index(list.len(), index) else {
            return error("ERR index out of range");
        };
        list[index] = match &storage {
            Some(store) => match store.put(args[3].clone()) {
                Ok(value) => value,
                Err(error) => return self.fail_io(error),
            },
            None => Payload::Inline(args[3].clone()),
        };
        self.record(now, || Mutation::ListSet {
            key: key.clone(),
            index: index as u64,
            value: args[3].clone(),
        });
        Resp::Simple("OK")
    }

    fn cmd_ltrim(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (Some(start), Some(stop)) = (integer(&args[2]), integer(&args[3])) else {
            return integer_error();
        };
        let key = &args[1];
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return Resp::Simple("OK");
        };
        let Value::List(list) = &mut entry.value else {
            return wrong_type();
        };
        let Some((start, stop)) = normalized_range(list.len(), start, stop) else {
            self.remove(key);
            self.record(now, || Mutation::Delete { key: key.clone() });
            return Resp::Simple("OK");
        };
        let back = list.len() - stop - 1;
        list.truncate(stop + 1);
        list.drain(..start);
        if start > 0 {
            self.record(now, || Mutation::Pop {
                key: key.clone(),
                count: start as u64,
                right: false,
            });
        }
        if back > 0 {
            self.record(now, || Mutation::Pop {
                key: key.clone(),
                count: back as u64,
                right: true,
            });
        }
        Resp::Simple("OK")
    }

    fn cmd_lrem(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(count) = integer(&args[2]) else {
            return integer_error();
        };
        let key = &args[1];
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return Resp::Integer(0);
        };
        let Value::List(list) = &mut entry.value else {
            return wrong_type();
        };
        let removed = match remove_matching(list, &args[3], count) {
            Ok(removed) => removed,
            Err(error) => return self.fail_io(error),
        };
        if list.is_empty() {
            self.remove(key);
        }
        if removed > 0 {
            let resolved = if count < 0 {
                -(removed as i64)
            } else {
                removed as i64
            };
            self.record(now, || Mutation::ListRemove {
                key: key.clone(),
                value: args[3].clone(),
                count: resolved,
            });
        }
        Resp::Integer(removed as i64)
    }

    pub(super) fn replay_list_set(
        &mut self,
        key: Bytes,
        index: u64,
        value: Bytes,
        now: i64,
    ) -> io::Result<()> {
        self.discard_expired(&key, now);
        let index = usize::try_from(index).map_err(|_| invalid_journal())?;
        if !matches!(self.entries.get(&key).map(|entry| &entry.value), Some(Value::List(list)) if index < list.len())
        {
            return Err(invalid_journal());
        }
        let value = self.payload(value)?;
        let Some(super::Entry {
            value: Value::List(list),
            ..
        }) = self.entries.get_mut(&key)
        else {
            return Err(invalid_journal());
        };
        let Some(entry) = list.get_mut(index) else {
            return Err(invalid_journal());
        };
        *entry = value;
        Ok(())
    }

    pub(super) fn replay_list_remove(
        &mut self,
        key: Bytes,
        value: Bytes,
        count: i64,
        now: i64,
    ) -> io::Result<()> {
        if count == 0 {
            return Err(invalid_journal());
        }
        self.discard_expired(&key, now);
        let Some(super::Entry {
            value: Value::List(list),
            ..
        }) = self.entries.get_mut(&key)
        else {
            return Err(invalid_journal());
        };
        let removed = remove_matching(list, &value, count)?;
        if removed as u64 != count.unsigned_abs() {
            return Err(invalid_journal());
        }
        if list.is_empty() {
            self.remove(&key);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        let args: Vec<_> = args
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect();
        engine.execute_at(&args, 1000)
    }

    #[test]
    fn list_rewrites_keep_order_ttl_and_remove_empty_keys() {
        let mut engine = Engine::new();
        run(&mut engine, &["RPUSH", "q", "a", "b", "a", "c", "a"]);
        run(&mut engine, &["PEXPIRE", "q", "100"]);
        assert_eq!(
            run(&mut engine, &["LREM", "q", "-2", "a"]),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut engine, &["LINDEX", "q", "0"]),
            Resp::Bulk(Some(Bytes::from_static(b"a")))
        );
        assert_eq!(
            run(&mut engine, &["LINDEX", "q", "-1"]),
            Resp::Bulk(Some(Bytes::from_static(b"c")))
        );
        assert_eq!(
            run(&mut engine, &["LSET", "q", "-1", "last"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["LTRIM", "q", "1", "-1"]),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["LRANGE", "q", "0", "-1"]),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"b"))),
                Resp::Bulk(Some(Bytes::from_static(b"last"))),
            ])
        );
        assert_eq!(run(&mut engine, &["PTTL", "q"]), Resp::Integer(100));
        run(&mut engine, &["LTRIM", "q", "99", "100"]);
        assert_eq!(run(&mut engine, &["TYPE", "q"]), Resp::Simple("none"));
        assert_eq!(
            run(&mut engine, &["LPUSHX", "q", "absent"]),
            Resp::Integer(0)
        );
    }

    #[test]
    fn rename_moves_owned_collection_ttl_and_replay_preserves_list_edits() {
        let mut source = Engine::new();
        source.set_recording(true);
        let mut recovered = Engine::new();
        for args in [
            vec!["RPUSH", "from", "a", "b", "a", "d"],
            vec!["PEXPIRE", "from", "100"],
            vec!["RENAME", "from", "to"],
            vec!["LREM", "to", "1", "a"],
            vec!["LSET", "to", "0", "B"],
            vec!["LTRIM", "to", "0", "1"],
        ] {
            run(&mut source, &args);
            recovered.replay(source.take_batch().unwrap()).unwrap();
        }
        assert_eq!(run(&mut recovered, &["TYPE", "from"]), Resp::Simple("none"));
        assert_eq!(
            run(&mut recovered, &["LRANGE", "to", "0", "-1"]),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"B"))),
                Resp::Bulk(Some(Bytes::from_static(b"a"))),
            ])
        );
        assert_eq!(run(&mut recovered, &["PTTL", "to"]), Resp::Integer(100));
        assert_eq!(
            run(&mut recovered, &["RENAMENX", "to", "to"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut recovered, &["RENAME", "to", "to"]),
            Resp::Simple("OK")
        );
    }

    #[test]
    fn invalid_list_updates_do_not_change_existing_data() {
        let mut engine = Engine::new();
        run(&mut engine, &["RPUSH", "q", "a"]);
        for args in [
            vec!["LSET", "q", "1", "bad"],
            vec!["LREM", "q", "bad", "a"],
            vec!["LTRIM", "q", "bad", "0"],
        ] {
            assert!(matches!(run(&mut engine, &args), Resp::Error(_)));
            assert_eq!(
                run(&mut engine, &["LINDEX", "q", "0"]),
                Resp::Bulk(Some(Bytes::from_static(b"a")))
            );
        }
    }
}
