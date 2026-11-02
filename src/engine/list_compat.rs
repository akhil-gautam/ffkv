//! Redis list selection and batch movement. Resolve every cold read before publish.
use std::{collections::VecDeque, io};

use bytes::Bytes;

use super::{
    BlockingKind, BlockingPop, CommandSpec, Engine, Execution, Mutation, NO_KEYS, ONE_KEY,
    ReplyBudget, Resp, Value, array, blocking_timeout, error, integer, integer_error,
    invalid_journal, payload_replies, syntax_error, wrong_type,
};

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "linsert",
        5,
        &["write", "denyoom"],
        ONE_KEY,
        Engine::cmd_linsert,
    ),
    CommandSpec::handler("lpos", -3, &["readonly"], ONE_KEY, Engine::cmd_lpos),
    CommandSpec::execution(
        "lmpop",
        -4,
        &["write", "movablekeys"],
        NO_KEYS,
        Engine::cmd_lmpop,
    ),
    CommandSpec::execution(
        "blmpop",
        -5,
        &["write", "blocking", "movablekeys"],
        NO_KEYS,
        Engine::cmd_blmpop,
    ),
    CommandSpec::execution(
        "lmovem",
        -5,
        &["write", "denyoom"],
        (1, 2, 1),
        Engine::cmd_lmovem,
    ),
    CommandSpec::execution(
        "blmovem",
        -6,
        &["write", "denyoom", "blocking"],
        (1, 2, 1),
        Engine::cmd_blmovem,
    ),
    CommandSpec::execution(
        "rpoplpush",
        3,
        &["write", "denyoom"],
        (1, 2, 1),
        Engine::cmd_rpoplpush,
    ),
    CommandSpec::execution(
        "brpoplpush",
        4,
        &["write", "denyoom", "blocking"],
        (1, 2, 1),
        Engine::cmd_brpoplpush,
    ),
];

fn side(value: &[u8]) -> Result<bool, Resp> {
    if value.eq_ignore_ascii_case(b"LEFT") {
        Ok(false)
    } else if value.eq_ignore_ascii_case(b"RIGHT") {
        Ok(true)
    } else {
        Err(syntax_error())
    }
}

fn positive(value: &[u8]) -> Result<usize, Resp> {
    match integer(value) {
        Some(value) if value > 0 => Ok(usize::try_from(value).unwrap_or(usize::MAX)),
        _ => Err(error("ERR count must be greater than 0")),
    }
}

impl Engine {
    fn cmd_linsert(&mut self, args: &[Bytes], now: i64) -> Resp {
        let after = if args[2].eq_ignore_ascii_case(b"AFTER") {
            true
        } else if args[2].eq_ignore_ascii_case(b"BEFORE") {
            false
        } else {
            return syntax_error();
        };
        let key = &args[1];
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get(key) else {
            return Resp::Integer(0);
        };
        let Value::List(list) = &entry.value else {
            return wrong_type();
        };
        let mut checksum = None;
        let mut insertion = None;
        for (index, value) in list.iter().enumerate() {
            let checksum = if value.is_disk() {
                *checksum.get_or_insert_with(|| crc32c::crc32c(&args[3]))
            } else {
                0
            };
            match value.matches(&args[3], checksum) {
                Ok(true) => {
                    insertion = Some(index + usize::from(after));
                    break;
                }
                Ok(false) => {}
                Err(failure) => return self.fail_io(failure),
            }
        }
        let Some(index) = insertion else {
            return Resp::Integer(-1);
        };
        let value = Bytes::copy_from_slice(&args[4]);
        let payload = match self.payload(value.clone()) {
            Ok(value) => value,
            Err(failure) => return self.fail_io(failure),
        };
        let Some(entry) = self.entries.get_mut(key) else {
            unreachable!()
        };
        let Value::List(list) = &mut entry.value else {
            unreachable!()
        };
        list.insert(index, payload);
        let length = list.len();
        self.record(now, || Mutation::ListInsert {
            key: key.clone(),
            index: index as u64,
            value,
            pivot: args[3].clone(),
            before: !after,
        });
        Resp::Integer(length as i64)
    }

    pub(super) fn replay_list_insert(
        &mut self,
        key: Bytes,
        index: u64,
        value: Bytes,
        now: i64,
    ) -> io::Result<()> {
        self.discard_expired(&key, now);
        let index = usize::try_from(index).map_err(|_| invalid_journal())?;
        if !matches!(self.entries.get(&key).map(|entry| &entry.value), Some(Value::List(list)) if index <= list.len())
        {
            return Err(invalid_journal());
        }
        let payload = self.payload(value)?;
        let Some(entry) = self.entries.get_mut(&key) else {
            return Err(invalid_journal());
        };
        let Value::List(list) = &mut entry.value else {
            return Err(invalid_journal());
        };
        list.insert(index, payload);
        Ok(())
    }

    fn cmd_lpos(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (mut rank, mut count, mut maxlen) = (1i64, None, 0usize);
        let mut position = 3;
        while position < args.len() {
            let Some(value) = args.get(position + 1) else {
                return syntax_error();
            };
            let Some(value) = integer(value) else {
                return integer_error();
            };
            if args[position].eq_ignore_ascii_case(b"RANK") {
                if value == 0 || value == i64::MIN {
                    return error("ERR RANK can't be zero or out of range");
                }
                rank = value;
            } else if args[position].eq_ignore_ascii_case(b"COUNT") {
                if value < 0 {
                    return error("ERR COUNT can't be negative");
                }
                count = Some(usize::try_from(value).unwrap_or(usize::MAX));
            } else if args[position].eq_ignore_ascii_case(b"MAXLEN") {
                if value < 0 {
                    return error("ERR MAXLEN can't be negative");
                }
                maxlen = usize::try_from(value).unwrap_or(usize::MAX);
            } else {
                return syntax_error();
            }
            position += 2;
        }
        let mut budget = ReplyBudget::new(self.reply_limit);
        let list = match self.value(&args[1], now) {
            Some(Value::List(list)) => list,
            Some(_) => return wrong_type(),
            None => {
                return if count.is_some() {
                    array(Vec::new())
                } else {
                    Resp::Bulk(None)
                };
            }
        };
        let comparisons = if maxlen == 0 {
            list.len()
        } else {
            maxlen.min(list.len())
        };
        let mut checksum = None;
        let mut skip = rank.unsigned_abs() - 1;
        let mut found = Vec::new();
        for offset in 0..comparisons {
            let index = if rank < 0 {
                list.len() - offset - 1
            } else {
                offset
            };
            let checksum = if list[index].is_disk() {
                *checksum.get_or_insert_with(|| crc32c::crc32c(&args[2]))
            } else {
                0
            };
            match list[index].matches(&args[2], checksum) {
                Ok(false) => continue,
                Err(failure) => return self.fail_io(failure),
                Ok(true) => {}
            }
            if skip > 0 {
                skip -= 1;
                continue;
            }
            if count.is_none() {
                return Resp::Integer(index as i64);
            }
            if let Err(reply) = budget.charge(1, 0) {
                return reply;
            }
            found.push(Resp::Integer(index as i64));
            if count.is_some_and(|count| count != 0 && found.len() >= count) {
                break;
            }
        }
        if count.is_some() {
            array(found)
        } else {
            Resp::Bulk(None)
        }
    }

    fn cmd_lmpop(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_multipop_command(args, false, now)
    }

    fn cmd_blmpop(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_multipop_command(args, true, now)
    }

    fn list_multipop_command(&mut self, args: &[Bytes], blocking: bool, now: i64) -> Execution {
        let parsed = (|| {
            let timeout = if blocking {
                blocking_timeout(&args[1], now)?
            } else {
                None
            };
            let index = if blocking { 2 } else { 1 };
            let numkeys = positive(&args[index])?;
            let start = index + 1;
            let end = start
                .checked_add(numkeys)
                .filter(|end| *end < args.len())
                .ok_or_else(syntax_error)?;
            let right = side(&args[end])?;
            let count = if args.len() == end + 1 {
                1
            } else if args.len() == end + 3 && args[end + 1].eq_ignore_ascii_case(b"COUNT") {
                positive(&args[end + 2])?
            } else {
                return Err(syntax_error());
            };
            Ok((timeout, start, end, right, count))
        })();
        let (timeout, start, end, right, count) = match parsed {
            Ok(parsed) => parsed,
            Err(reply) => return Execution::Reply(reply),
        };
        let keys = &args[start..end];
        match self.try_list_multipop_at(keys, right, count, now) {
            Ok(Some(reply)) => Execution::Reply(reply),
            Ok(None) if blocking => Execution::Block(BlockingPop {
                keys: keys.iter().map(|key| Bytes::copy_from_slice(key)).collect(),
                right,
                timeout,
                destination: None,
                kind: BlockingKind::MultiPop { count },
            }),
            Ok(None) => Execution::Reply(Resp::Bulk(None)),
            Err(reply) => Execution::Reply(reply),
        }
    }

    pub(super) fn try_list_multipop_at(
        &mut self,
        keys: &[Bytes],
        right: bool,
        count: usize,
        now: i64,
    ) -> Result<Option<Resp>, Resp> {
        for key in keys {
            self.discard_expired(key, now);
            let Some(entry) = self.entries.get_mut(key) else {
                continue;
            };
            let Value::List(list) = &mut entry.value else {
                return Err(wrong_type());
            };
            if list.is_empty() {
                continue;
            }
            let count = count.min(list.len());
            let overhead = key
                .len()
                .saturating_add(std::mem::size_of::<Resp>().saturating_mul(3));
            let limit = self.reply_limit.saturating_sub(overhead);
            let replies = if right {
                payload_replies(list.iter().rev().take(count), limit)
            } else {
                payload_replies(list.iter().take(count), limit)
            };
            let replies = match replies {
                Ok(values) => values,
                Err(failure) => return Err(self.fail_payload_reply(failure)),
            };
            if right {
                list.truncate(list.len() - count);
            } else {
                list.drain(..count);
            }
            if list.is_empty() {
                self.remove(key);
            }
            self.record(now, || Mutation::Pop {
                key: key.clone(),
                count: count as u64,
                right,
            });
            return Ok(Some(array(vec![
                Resp::Bulk(Some(key.clone())),
                array(replies),
            ])));
        }
        Ok(None)
    }

    fn cmd_lmovem(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_move_many_command(args, false, now)
    }

    fn cmd_blmovem(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_move_many_command(args, true, now)
    }

    fn list_move_many_command(&mut self, args: &[Bytes], blocking: bool, now: i64) -> Execution {
        let parsed = (|| {
            let source_right = side(&args[3])?;
            let destination_right = side(&args[4])?;
            let timeout = if blocking {
                blocking_timeout(&args[5], now)?
            } else {
                None
            };
            let option = if blocking { 6 } else { 5 };
            let (count, exactly, bulk) = if args.len() == option {
                (1, false, false)
            } else {
                if args.len() != option + 3 {
                    return Err(syntax_error());
                }
                let exactly = if args[option].eq_ignore_ascii_case(b"EXACTLY") {
                    true
                } else if args[option].eq_ignore_ascii_case(b"COUNT") {
                    false
                } else {
                    return Err(syntax_error());
                };
                let count = positive(&args[option + 1])?;
                let bulk = if args[option + 2].eq_ignore_ascii_case(b"BULK") {
                    true
                } else if args[option + 2].eq_ignore_ascii_case(b"OBO") {
                    false
                } else {
                    return Err(syntax_error());
                };
                (count, exactly, bulk)
            };
            Ok((
                source_right,
                destination_right,
                timeout,
                count,
                exactly,
                bulk,
            ))
        })();
        let (source_right, destination_right, timeout, count, exactly, bulk) = match parsed {
            Ok(parsed) => parsed,
            Err(reply) => return Execution::Reply(reply),
        };
        match self.try_list_move_many_at(
            &args[1],
            &args[2],
            source_right,
            destination_right,
            count,
            exactly,
            bulk,
            now,
        ) {
            Ok(Some(reply)) => Execution::Pushed {
                reply,
                key: args[2].clone(),
            },
            Ok(None) if blocking => Execution::Block(BlockingPop {
                keys: vec![Bytes::copy_from_slice(&args[1])],
                right: source_right,
                timeout,
                destination: Some((Bytes::copy_from_slice(&args[2]), destination_right)),
                kind: BlockingKind::MoveMany {
                    count,
                    exactly,
                    bulk,
                },
            }),
            Ok(None) => Execution::Reply(if exactly {
                Resp::Bulk(None)
            } else {
                array(Vec::new())
            }),
            Err(reply) => Execution::Reply(reply),
        }
    }

    /// Select and read once, then move payload handles. Neither the WAL nor the
    /// reply can trigger a second cold read after a destructive pop.
    #[expect(
        clippy::too_many_arguments,
        reason = "Both directions, count/exactness, reply shape, and frozen time define one atomic list move."
    )]
    pub(super) fn try_list_move_many_at(
        &mut self,
        source: &Bytes,
        destination: &Bytes,
        source_right: bool,
        destination_right: bool,
        requested: usize,
        exactly: bool,
        bulk: bool,
        now: i64,
    ) -> Result<Option<Resp>, Resp> {
        self.discard_expired(source, now);
        let Some(entry) = self.entries.get(source) else {
            return Ok(None);
        };
        let Value::List(list) = &entry.value else {
            return Err(wrong_type());
        };
        if list.is_empty() || (exactly && list.len() < requested) {
            return Ok(None);
        }
        let count = requested.min(list.len());
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
        let entry = self.entries.get(source).expect("validated source");
        let Value::List(list) = &entry.value else {
            unreachable!()
        };
        let expiry = if source == destination && count == list.len() {
            entry.expires_at
        } else {
            None
        };
        let resolved = if source_right {
            payload_replies(list.iter().rev().take(count), self.reply_limit)
        } else {
            payload_replies(list.iter().take(count), self.reply_limit)
        };
        let mut resolved = match resolved {
            Ok(values) => values,
            Err(failure) => return Err(self.fail_payload_reply(failure)),
        };
        // Values are initially in pop order. Push order is reversed only for
        // BULK when both endpoints are the same side.
        let reverse_push = bulk && source_right == destination_right;
        if reverse_push {
            resolved.reverse();
        }
        let Some(entry) = self.entries.get_mut(source) else {
            unreachable!()
        };
        let Value::List(list) = &mut entry.value else {
            unreachable!()
        };
        let mut moved: Vec<_> = if source_right {
            list.drain(list.len() - count..).rev().collect()
        } else {
            list.drain(..count).collect()
        };
        if reverse_push {
            moved.reverse();
        }
        if source != destination && list.is_empty() {
            self.remove(source);
        }
        let Value::List(target) =
            self.collection_entry(destination, now, || Value::List(VecDeque::new()))
        else {
            unreachable!()
        };
        for payload in moved {
            if destination_right {
                target.push_back(payload);
            } else {
                target.push_front(payload);
            }
        }
        self.record(now, || Mutation::Pop {
            key: source.clone(),
            count: count as u64,
            right: source_right,
        });
        self.record(now, || Mutation::Push {
            key: destination.clone(),
            values: resolved
                .iter()
                .map(|reply| match reply {
                    Resp::Bulk(Some(value)) => value.clone(),
                    _ => unreachable!(),
                })
                .collect(),
            right: destination_right,
        });
        if let Some(expires_at) = expiry {
            self.record(now, || Mutation::Expire {
                key: source.clone(),
                expires_at: Some(expires_at),
            });
        }
        if !destination_right {
            resolved.reverse();
        }
        Ok(Some(array(resolved)))
    }

    fn cmd_rpoplpush(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_move_alias(args, false, now)
    }

    fn cmd_brpoplpush(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.list_move_alias(args, true, now)
    }

    fn list_move_alias(&mut self, args: &[Bytes], blocking: bool, now: i64) -> Execution {
        let timeout = if blocking {
            match blocking_timeout(&args[3], now) {
                Ok(timeout) => timeout,
                Err(reply) => return Execution::Reply(reply),
            }
        } else {
            None
        };
        match self.try_move_at(&args[1], &args[2], true, false, now) {
            Ok(Some(value)) => Execution::Pushed {
                reply: Resp::Bulk(Some(value)),
                key: args[2].clone(),
            },
            Ok(None) if blocking => Execution::Block(BlockingPop {
                keys: vec![Bytes::copy_from_slice(&args[1])],
                right: true,
                timeout,
                destination: Some((Bytes::copy_from_slice(&args[2]), false)),
                kind: BlockingKind::ListPop,
            }),
            Ok(None) => Execution::Reply(Resp::Bulk(None)),
            Err(reply) => Execution::Reply(reply),
        }
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

    fn strings(values: &[&str]) -> Resp {
        array(
            values
                .iter()
                .map(|value| Resp::Bulk(Some(Bytes::copy_from_slice(value.as_bytes()))))
                .collect(),
        )
    }

    #[test]
    fn batch_moves_select_before_push_and_reply_in_destination_order() {
        for (from, to, mode, moved, target) in [
            ("LEFT", "LEFT", "OBO", vec!["b", "a"], vec!["b", "a", "x"]),
            ("LEFT", "LEFT", "BULK", vec!["a", "b"], vec!["a", "b", "x"]),
            ("LEFT", "RIGHT", "OBO", vec!["a", "b"], vec!["x", "a", "b"]),
            ("LEFT", "RIGHT", "BULK", vec!["a", "b"], vec!["x", "a", "b"]),
            ("RIGHT", "LEFT", "OBO", vec!["c", "d"], vec!["c", "d", "x"]),
            ("RIGHT", "LEFT", "BULK", vec!["c", "d"], vec!["c", "d", "x"]),
            ("RIGHT", "RIGHT", "OBO", vec!["d", "c"], vec!["x", "d", "c"]),
            (
                "RIGHT",
                "RIGHT",
                "BULK",
                vec!["c", "d"],
                vec!["x", "c", "d"],
            ),
        ] {
            let mut engine = Engine::new();
            run(&mut engine, &["RPUSH", "s", "a", "b", "c", "d"]);
            run(&mut engine, &["RPUSH", "d", "x"]);
            assert_eq!(
                run(
                    &mut engine,
                    &["LMOVEM", "s", "d", from, to, "COUNT", "2", mode]
                ),
                strings(&moved)
            );
            assert_eq!(
                run(&mut engine, &["LRANGE", "d", "0", "-1"]),
                strings(&target)
            );
            let remaining = if from == "LEFT" {
                ["c", "d"]
            } else {
                ["a", "b"]
            };
            assert_eq!(
                run(&mut engine, &["LRANGE", "s", "0", "-1"]),
                strings(&remaining)
            );
        }
        for (from, to, mode, expected) in [
            ("LEFT", "LEFT", "OBO", ["b", "a", "c", "d"]),
            ("LEFT", "LEFT", "BULK", ["a", "b", "c", "d"]),
            ("RIGHT", "RIGHT", "OBO", ["a", "b", "d", "c"]),
            ("RIGHT", "RIGHT", "BULK", ["a", "b", "c", "d"]),
            ("LEFT", "RIGHT", "OBO", ["c", "d", "a", "b"]),
            ("RIGHT", "LEFT", "BULK", ["c", "d", "a", "b"]),
        ] {
            let mut engine = Engine::new();
            run(&mut engine, &["RPUSH", "q", "a", "b", "c", "d"]);
            run(
                &mut engine,
                &["LMOVEM", "q", "q", from, to, "EXACTLY", "2", mode],
            );
            assert_eq!(
                run(&mut engine, &["LRANGE", "q", "0", "-1"]),
                strings(&expected)
            );
        }
    }

    #[test]
    fn invalid_requests_and_insufficient_exact_batches_do_not_publish() {
        let mut engine = Engine::new();
        run(&mut engine, &["RPUSH", "s", "a", "b"]);
        run(&mut engine, &["SET", "wrong", "value"]);
        engine.set_recording(true);
        assert_eq!(
            run(
                &mut engine,
                &["LMOVEM", "s", "d", "LEFT", "RIGHT", "EXACTLY", "3", "BULK"]
            ),
            Resp::Bulk(None)
        );
        for args in [
            vec!["LMOVEM", "s", "d", "LEFT", "RIGHT", "COUNT", "2"],
            vec!["LMOVEM", "s", "d", "LEFT", "RIGHT", "COUNT", "0", "OBO"],
            vec!["LMOVEM", "s", "wrong", "LEFT", "RIGHT", "COUNT", "2", "OBO"],
            vec!["LINSERT", "s", "SIDEWAYS", "a", "bad"],
            vec!["LMPOP", "1", "s", "LEFT", "COUNT", "0"],
            vec!["LMPOP", "2", "s", "LEFT"],
        ] {
            assert!(matches!(run(&mut engine, &args), Resp::Error(_)));
            assert_eq!(
                run(&mut engine, &["LRANGE", "s", "0", "-1"]),
                strings(&["a", "b"])
            );
            assert!(engine.take_batch().is_none());
        }
        engine.set_reply_limit(1);
        assert!(matches!(
            run(
                &mut engine,
                &["LMOVEM", "s", "d", "LEFT", "RIGHT", "COUNT", "2", "BULK"]
            ),
            Resp::Error(_)
        ));
        assert!(engine.take_batch().is_none());
        engine.set_reply_limit(1024 * 1024);
        assert_eq!(
            run(&mut engine, &["LRANGE", "s", "0", "-1"]),
            strings(&["a", "b"])
        );
        assert_eq!(run(&mut engine, &["EXISTS", "d"]), Resp::Integer(0));
    }

    #[test]
    fn insertion_resolves_first_pivot_and_replay_retains_ttl() {
        let mut source = Engine::new();
        let mut recovered = Engine::new();
        source.set_recording(true);
        for args in [
            vec!["RPUSH", "q", "a", "p", "b", "p"],
            vec!["PEXPIRE", "q", "100"],
            vec!["LINSERT", "q", "BEFORE", "p", "before"],
            vec!["LINSERT", "q", "AFTER", "p", "after"],
            vec!["LMOVEM", "q", "q", "LEFT", "LEFT", "EXACTLY", "6", "OBO"],
        ] {
            assert!(!matches!(run(&mut source, &args), Resp::Error(_)));
            recovered.replay(source.take_batch().unwrap()).unwrap();
        }
        let expected = strings(&["p", "b", "after", "p", "before", "a"]);
        assert_eq!(run(&mut source, &["LRANGE", "q", "0", "-1"]), expected);
        assert_eq!(run(&mut recovered, &["LRANGE", "q", "0", "-1"]), expected);
        assert_eq!(run(&mut recovered, &["PTTL", "q"]), Resp::Integer(100));
        assert_eq!(
            run(&mut source, &["LINSERT", "q", "BEFORE", "missing", "bad"]),
            Resp::Integer(-1)
        );
        assert!(source.take_batch().is_none());
        assert!(
            recovered
                .replay_list_insert(
                    Bytes::from_static(b"q"),
                    7,
                    Bytes::from_static(b"bad"),
                    1000
                )
                .is_err()
        );
        assert!(
            recovered
                .replay_list_insert(
                    Bytes::from_static(b"missing"),
                    0,
                    Bytes::from_static(b"bad"),
                    1000
                )
                .is_err()
        );
    }

    #[test]
    fn positions_bound_comparisons_and_keep_forward_indexes_when_scanning_backwards() {
        let mut engine = Engine::new();
        run(&mut engine, &["RPUSH", "q", "a", "x", "b", "x", "x", "c"]);
        assert_eq!(
            run(
                &mut engine,
                &["LPOS", "q", "x", "RANK", "-2", "COUNT", "0", "MAXLEN", "5"]
            ),
            array(vec![Resp::Integer(3), Resp::Integer(1)])
        );
        assert_eq!(
            run(&mut engine, &["LPOS", "q", "x", "RANK", "2", "MAXLEN", "3"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(
                &mut engine,
                &["LPOS", "q", "x", "COUNT", "0", "MAXLEN", "0"]
            ),
            array(vec![Resp::Integer(1), Resp::Integer(3), Resp::Integer(4)])
        );
        assert_eq!(
            run(&mut engine, &["LPOS", "missing", "x", "COUNT", "0"]),
            array(Vec::new())
        );
        assert!(matches!(
            run(&mut engine, &["LPOS", "q", "x", "MAXLEN", "-1"]),
            Resp::Error(_)
        ));
    }

    #[test]
    fn blocking_descriptors_keep_exact_threshold_and_move_destination() {
        let mut engine = Engine::new();
        run(&mut engine, &["RPUSH", "s", "one"]);
        let args: Vec<_> = [
            "BLMOVEM", "s", "d", "LEFT", "RIGHT", "0", "EXACTLY", "2", "BULK",
        ]
        .iter()
        .map(|value| Bytes::copy_from_slice(value.as_bytes()))
        .collect();
        let Execution::Block(block) = engine.cmd_blmovem(&args, 1000) else {
            panic!("must block for full batch")
        };
        assert_eq!(block.keys, vec![Bytes::from_static(b"s")]);
        assert_eq!(block.destination, Some((Bytes::from_static(b"d"), true)));
        assert!(matches!(
            block.kind,
            BlockingKind::MoveMany {
                count: 2,
                exactly: true,
                bulk: true
            }
        ));
        assert!(
            engine
                .try_list_move_many_at(&args[1], &args[2], false, true, 2, true, true, 1000)
                .unwrap()
                .is_none()
        );
        run(&mut engine, &["RPUSH", "s", "two"]);
        assert_eq!(
            engine
                .try_list_move_many_at(&args[1], &args[2], false, true, 2, true, true, 1000)
                .unwrap(),
            Some(strings(&["one", "two"]))
        );
        assert_eq!(run(&mut engine, &["EXISTS", "s"]), Resp::Integer(0));
    }

    #[test]
    fn batch_moves_transfer_cold_handles_without_writes_or_post_publish_reads() {
        let path = std::env::temp_dir().join(format!(
            "ffkv-list-batch-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let store = super::super::Store::open(crate::storage::Config {
            directory: path.clone(),
            inline_threshold: 0,
            cache_bytes: 0,
            segment_bytes: 4096,
            disk_budget: 0,
        })
        .unwrap();
        let mut engine = Engine::new();
        engine.set_storage(store.clone());
        run(
            &mut engine,
            &["RPUSH", "s", "cold one", "cold two", "cold three"],
        );
        let before = store.stats();
        assert_eq!(
            run(
                &mut engine,
                &["LMOVEM", "s", "d", "LEFT", "LEFT", "EXACTLY", "2", "OBO"]
            ),
            strings(&["cold two", "cold one"])
        );
        let after = store.stats();
        assert_eq!(after.writes, before.writes);
        assert_eq!(after.reads - before.reads, 2);
        let Value::List(list) = &engine.entries[&Bytes::from_static(b"d")].value else {
            panic!("expected list")
        };
        assert!(list.iter().all(|value| value.is_disk()));
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "seg")
            {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(entry.path())
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }
        }
        assert!(matches!(
            run(
                &mut engine,
                &[
                    "LMOVEM", "d", "fresh", "LEFT", "RIGHT", "COUNT", "2", "BULK"
                ]
            ),
            Resp::Error(_)
        ));
        let Value::List(list) = &engine.entries[&Bytes::from_static(b"d")].value else {
            panic!("source must survive failed read")
        };
        assert_eq!(list.len(), 2);
        assert!(!engine.entries.contains_key(&Bytes::from_static(b"fresh")));
        drop(engine);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
