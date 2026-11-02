use std::collections::{HashMap, HashSet};
use std::io;

use bytes::Bytes;
use rand::{Rng, seq::index};

use super::scan::{ScanIndex, parse_scan, scan_entries, stable_hash};
use super::{
    CommandSpec, Engine, Entry, Mutation, Resp, Value, array, error, integer, integer_error,
    invalid_journal, wrong_type,
};

/// Dense positions make membership, uniform selection and swap-removal O(1).
/// SSCAN has its own immutable-hash ordering: dense positions are not cursors.
#[derive(Default)]
pub(super) struct SetValue {
    positions: HashMap<Bytes, usize>,
    members: Vec<Bytes>,
    scan_index: ScanIndex,
}

/// Read-only checkpoint traversal without exposing the dense storage.
pub(super) struct Snapshot<'a>(std::slice::Iter<'a, Bytes>);

impl<'a> Iterator for Snapshot<'a> {
    type Item = &'a Bytes;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl SetValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{allocation, hash_bytes, sampled, tree_bytes};
        hash_bytes::<Bytes, usize>(self.positions.capacity())
            .saturating_add(allocation::<Bytes>(self.members.capacity()))
            .saturating_add(tree_bytes::<(u64, Bytes)>(self.scan_index.len()))
            .saturating_add(sampled(self.members.iter(), samples, Bytes::len))
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = &Bytes> {
        self.members.iter()
    }

    pub(super) fn contains(&self, member: &Bytes) -> bool {
        self.positions.contains_key(member)
    }
    pub(super) fn len(&self) -> usize {
        self.members.len()
    }

    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot(self.members.iter())
    }

    fn insert(&mut self, member: Bytes) -> bool {
        let std::collections::hash_map::Entry::Vacant(position) =
            self.positions.entry(member.clone())
        else {
            return false;
        };
        position.insert(self.members.len());
        self.scan_index
            .insert((stable_hash(&member), member.clone()));
        self.members.push(member);
        true
    }

    fn remove(&mut self, member: &Bytes) -> Option<Bytes> {
        let position = self.positions.remove(member)?;
        Some(self.remove_dense(position))
    }

    fn remove_dense(&mut self, position: usize) -> Bytes {
        let member = self.members.swap_remove(position);
        if let Some(moved) = self.members.get(position) {
            *self
                .positions
                .get_mut(moved)
                .expect("dense member has a reverse position") = position;
        }
        self.scan_index
            .remove(&(stable_hash(&member), member.clone()));
        member
    }
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "sadd",
        -3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::set_add,
    ),
    CommandSpec::handler(
        "srem",
        -3,
        &["write", "fast"],
        (1, 1, 1),
        Engine::set_remove,
    ),
    CommandSpec::handler("smembers", 2, &["readonly"], (1, 1, 1), Engine::set_members),
    CommandSpec::handler(
        "scard",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::set_cardinality,
    ),
    CommandSpec::handler(
        "sismember",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::set_is_member,
    ),
    CommandSpec::handler(
        "smismember",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::set_is_members,
    ),
    CommandSpec::handler("spop", -2, &["write", "fast"], (1, 1, 1), Engine::set_pop).max(3),
    CommandSpec::handler(
        "srandmember",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::set_random,
    )
    .max(3),
    CommandSpec::handler("smove", 4, &["write", "fast"], (1, 2, 1), Engine::set_move),
    CommandSpec::handler("sunion", -2, &["readonly"], (1, -1, 1), Engine::set_union),
    CommandSpec::handler(
        "sinter",
        -2,
        &["readonly"],
        (1, -1, 1),
        Engine::set_intersection,
    ),
    CommandSpec::handler(
        "sdiff",
        -2,
        &["readonly"],
        (1, -1, 1),
        Engine::set_difference,
    ),
    CommandSpec::handler(
        "sunionstore",
        -3,
        &["write", "denyoom"],
        (1, -1, 1),
        Engine::set_union_store,
    ),
    CommandSpec::handler(
        "sinterstore",
        -3,
        &["write", "denyoom"],
        (1, -1, 1),
        Engine::set_intersection_store,
    ),
    CommandSpec::handler(
        "sdiffstore",
        -3,
        &["write", "denyoom"],
        (1, -1, 1),
        Engine::set_difference_store,
    ),
    CommandSpec::handler("sscan", -3, &["readonly"], (1, 1, 1), Engine::set_scan),
    CommandSpec::handler(
        "sdiffcard",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::set_difference_card,
    ),
    CommandSpec::handler(
        "sintercard",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::set_intersection_card,
    ),
    CommandSpec::handler(
        "sunioncard",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::set_union_card,
    ),
];

#[derive(Clone, Copy)]
enum Algebra {
    Union,
    Intersection,
    Difference,
}

impl Engine {
    fn set_add(&mut self, args: &[Bytes], now: i64) -> Resp {
        let journal = self.journal_enabled;
        let value = self.collection_entry(&args[1], now, || Value::Set(SetValue::default()));
        let Value::Set(set) = value else {
            return wrong_type();
        };
        let mut added = 0;
        let mut changes = journal.then(Vec::new);
        for member in &args[2..] {
            if set.insert(member.clone()) {
                added += 1;
                if let Some(changes) = &mut changes {
                    changes.push(member.clone());
                }
            }
        }
        if added > 0 {
            self.record(now, || Mutation::SetAdd {
                key: args[1].clone(),
                members: changes.expect("journal captures actual additions"),
            });
        }
        Resp::Integer(added)
    }

    fn set_remove(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.discard_expired(&args[1], now);
        let journal = self.journal_enabled;
        let Some(entry) = self.entries.get_mut(&args[1]) else {
            return Resp::Integer(0);
        };
        let Value::Set(set) = &mut entry.value else {
            return wrong_type();
        };
        let mut removed = 0;
        let mut changes = journal.then(Vec::new);
        for member in &args[2..] {
            if let Some(member) = set.remove(member) {
                removed += 1;
                if let Some(changes) = &mut changes {
                    changes.push(member);
                }
            }
        }
        if set.members.is_empty() {
            self.remove(&args[1]);
        }
        if removed > 0 {
            self.record(now, || Mutation::SetRemove {
                key: args[1].clone(),
                members: changes.expect("journal captures actual removals"),
            });
        }
        Resp::Integer(removed)
    }

    fn set_members(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        match self.value(&args[1], now) {
            Some(Value::Set(set)) => member_set(set.members.iter().cloned(), limit),
            None => Resp::Set(Vec::new()),
            _ => wrong_type(),
        }
    }

    fn set_cardinality(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.value(&args[1], now) {
            Some(Value::Set(set)) => Resp::Integer(set.members.len() as i64),
            None => Resp::Integer(0),
            _ => wrong_type(),
        }
    }

    fn set_is_member(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.value(&args[1], now) {
            Some(Value::Set(set)) => Resp::Integer(i64::from(set.positions.contains_key(&args[2]))),
            None => Resp::Integer(0),
            _ => wrong_type(),
        }
    }

    fn set_is_members(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let set = match self.value(&args[1], now) {
            Some(Value::Set(set)) => Some(set),
            None => None,
            _ => return wrong_type(),
        };
        super::bounded_replies(
            args[2..].iter().map(|member| {
                Resp::Integer(i64::from(
                    set.is_some_and(|set| set.positions.contains_key(member)),
                ))
            }),
            limit,
        )
    }

    fn set_pop(&mut self, args: &[Bytes], now: i64) -> Resp {
        let count = match args.get(2) {
            Some(count) => match integer(count) {
                Some(count) if count >= 0 => usize::try_from(count).unwrap_or(usize::MAX),
                _ => return error("ERR value is out of range, must be positive"),
            },
            None => 1,
        };
        self.discard_expired(&args[1], now);
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        let Some(entry) = self.entries.get_mut(&args[1]) else {
            return if args.len() == 2 {
                Resp::Bulk(None)
            } else {
                Resp::Set(Vec::new())
            };
        };
        let Value::Set(set) = &mut entry.value else {
            return wrong_type();
        };
        let count = count.min(set.members.len());
        if let Err(error) = budget.charge(count, 0) {
            return error;
        }
        let mut selected = Vec::with_capacity(count);
        let mut rng = rand::rng();
        // Select without mutation, so refusal cannot consume any member.
        for position in index::sample(&mut rng, set.members.len(), count) {
            let member = &set.members[position];
            if let Err(error) = budget.charge(0, member.len()) {
                return error;
            }
            selected.push(member.clone());
        }
        for member in &selected {
            set.remove(member);
        }
        if set.members.is_empty() {
            self.remove(&args[1]);
        }
        if !selected.is_empty() {
            self.record(now, || Mutation::SetRemove {
                key: args[1].clone(),
                members: selected.clone(),
            });
        }
        if args.len() == 2 {
            Resp::Bulk(selected.pop())
        } else {
            member_set(selected, self.reply_limit)
        }
    }

    fn set_random(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let count = match args.get(2) {
            Some(count) => match integer(count) {
                Some(i64::MIN) => return integer_error(),
                Some(count) => Some(count),
                None => return integer_error(),
            },
            None => None,
        };
        let set = match self.value(&args[1], now) {
            Some(Value::Set(set)) => set,
            None => {
                return if count.is_none() {
                    Resp::Bulk(None)
                } else {
                    array(Vec::new())
                };
            }
            _ => return wrong_type(),
        };
        let mut rng = rand::rng();
        match count {
            None => Resp::Bulk(Some(
                set.members[rng.random_range(0..set.members.len())].clone(),
            )),
            Some(count) if count < 0 => member_array(
                (0..count.unsigned_abs())
                    .map(|_| set.members[rng.random_range(0..set.members.len())].clone()),
                limit,
            ),
            Some(count) => {
                let count = usize::try_from(count)
                    .unwrap_or(usize::MAX)
                    .min(set.members.len());
                if let Err(error) = super::ReplyBudget::new(limit).charge(count, 0) {
                    return error;
                }
                if count == set.members.len() {
                    return member_array(set.members.iter().cloned(), limit);
                }
                // rand chooses a bounded, without-replacement algorithm, not rejection
                // against already-selected members when count approaches cardinality.
                member_array(
                    index::sample(&mut rng, set.members.len(), count)
                        .into_iter()
                        .map(|position| set.members[position].clone()),
                    limit,
                )
            }
        }
    }

    fn set_move(&mut self, args: &[Bytes], now: i64) -> Resp {
        let source = &args[1];
        let destination = &args[2];
        match self.value(source, now) {
            None => return Resp::Integer(0),
            Some(Value::Set(_)) => {}
            _ => return wrong_type(),
        }
        if !matches!(self.value(destination, now), None | Some(Value::Set(_))) {
            return wrong_type();
        }
        let Some(Entry {
            value: Value::Set(set),
            ..
        }) = self.entries.get_mut(source)
        else {
            unreachable!("validated source remains a set");
        };
        if source == destination {
            return Resp::Integer(i64::from(set.positions.contains_key(&args[3])));
        }
        let Some(member) = set.remove(&args[3]) else {
            return Resp::Integer(0);
        };
        if set.members.is_empty() {
            self.remove(source);
        }
        self.record(now, || Mutation::SetRemove {
            key: source.clone(),
            members: vec![member.clone()],
        });
        let Value::Set(set) =
            self.collection_entry(destination, now, || Value::Set(SetValue::default()))
        else {
            unreachable!("validated destination remains a set");
        };
        if set.insert(member) {
            self.record(now, || Mutation::SetAdd {
                key: destination.clone(),
                members: vec![args[3].clone()],
            });
        }
        Resp::Integer(1)
    }

    fn set_union(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Union, false)
    }
    fn set_intersection(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Intersection, false)
    }
    fn set_difference(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Difference, false)
    }
    fn set_union_store(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Union, true)
    }
    fn set_intersection_store(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Intersection, true)
    }
    fn set_difference_store(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra(args, now, Algebra::Difference, true)
    }
    fn set_difference_card(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra_card(args, now, Algebra::Difference)
    }

    fn set_intersection_card(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra_card(args, now, Algebra::Intersection)
    }

    fn set_union_card(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.set_algebra_card(args, now, Algebra::Union)
    }

    fn set_algebra_card(&mut self, args: &[Bytes], now: i64, operation: Algebra) -> Resp {
        let Some(numkeys) = integer(&args[1]) else {
            return integer_error();
        };
        if numkeys <= 0 {
            return error("ERR numkeys should be greater than 0");
        }
        let Ok(numkeys) = usize::try_from(numkeys) else {
            return error("ERR Number of keys can't be greater than number of args");
        };
        if numkeys > args.len() - 2 {
            return error("ERR Number of keys can't be greater than number of args");
        }
        let sources = &args[2..2 + numkeys];
        let mut limit = 0i64;
        let mut approximate = false;
        let mut seen_limit = false;
        let mut index = 2 + numkeys;
        while index < args.len() {
            if args[index].eq_ignore_ascii_case(b"APPROX")
                && matches!(operation, Algebra::Union)
                && !approximate
            {
                approximate = true;
                index += 1;
            } else if args[index].eq_ignore_ascii_case(b"LIMIT") && !seen_limit {
                let Some(value) = args.get(index + 1) else {
                    return super::syntax_error();
                };
                let Some(value) = integer(value) else {
                    return integer_error();
                };
                if value < 0 {
                    return error("ERR LIMIT can't be negative");
                }
                limit = value;
                seen_limit = true;
                index += 2;
            } else {
                return super::syntax_error();
            }
        }
        // Type-check all inputs before an early LIMIT or missing-set result.
        for key in sources {
            if !matches!(self.value(key, now), None | Some(Value::Set(_))) {
                return wrong_type();
            }
        }
        let sets: Vec<_> = sources
            .iter()
            .map(|key| match self.entries.get(key) {
                Some(Entry {
                    value: Value::Set(set),
                    ..
                }) => Some(set),
                None => None,
                _ => unreachable!("validated cardinality source"),
            })
            .collect();
        let cap = if limit == 0 { i64::MAX } else { limit };
        if approximate {
            return Resp::Integer(super::hyperloglog::approximate_cardinality(
                sets.iter().flatten().flat_map(|set| set.members.iter()),
                cap,
            ));
        }
        let mut cardinality = 0;
        match operation {
            Algebra::Union => {
                let mut unique = HashSet::new();
                for member in sets.iter().flatten().flat_map(|set| &set.members) {
                    if unique.insert(member) {
                        cardinality += 1;
                        if cardinality == cap {
                            break;
                        }
                    }
                }
            }
            Algebra::Intersection | Algebra::Difference => {
                let candidates = if matches!(operation, Algebra::Intersection) {
                    sets.iter()
                        .min_by_key(|set| set.map_or(0, |set| set.members.len()))
                        .copied()
                        .flatten()
                } else {
                    sets[0]
                };
                if let Some(candidates) = candidates {
                    for member in &candidates.members {
                        let present = match operation {
                            Algebra::Intersection => sets.iter().all(|set| {
                                set.is_some_and(|set| set.positions.contains_key(member))
                            }),
                            Algebra::Difference => !sets[1..].iter().any(|set| {
                                set.is_some_and(|set| set.positions.contains_key(member))
                            }),
                            Algebra::Union => unreachable!(),
                        };
                        if present {
                            cardinality += 1;
                            if cardinality == cap {
                                break;
                            }
                        }
                    }
                }
            }
        }
        Resp::Integer(cardinality)
    }

    fn set_algebra(&mut self, args: &[Bytes], now: i64, operation: Algebra, store: bool) -> Resp {
        let sources = &args[if store { 2 } else { 1 }..];
        // Validate every source, even after a missing/empty one determines a result.
        // Compute the complete result before replacing a possibly overlapping target.
        for key in sources {
            if !matches!(self.value(key, now), None | Some(Value::Set(_))) {
                return wrong_type();
            }
        }
        let sets: Vec<_> = sources
            .iter()
            .map(|key| match self.entries.get(key) {
                Some(Entry {
                    value: Value::Set(set),
                    ..
                }) => Some(set),
                None => None,
                _ => unreachable!("validated algebra source remains a set"),
            })
            .collect();
        if !store {
            return match operation {
                Algebra::Union => {
                    let mut budget = super::ReplyBudget::new(self.reply_limit);
                    let mut unique = HashSet::new();
                    for member in sets.iter().flatten().flat_map(|set| &set.members) {
                        if !unique.contains(member) {
                            // Include temporary deduplication storage in the row allowance.
                            if let Err(error) = budget.charge(2, member.len()) {
                                return error;
                            }
                            unique.insert(member);
                        }
                    }
                    member_set(unique.into_iter().cloned(), self.reply_limit)
                }
                Algebra::Intersection => match sets
                    .iter()
                    .min_by_key(|set| set.map_or(0, |set| set.members.len()))
                    .copied()
                    .flatten()
                {
                    None => Resp::Set(Vec::new()),
                    Some(smallest) => member_set(
                        smallest
                            .members
                            .iter()
                            .filter(|member| {
                                sets.iter().all(|set| {
                                    set.is_some_and(|set| set.positions.contains_key(*member))
                                })
                            })
                            .cloned(),
                        self.reply_limit,
                    ),
                },
                Algebra::Difference => match sets[0] {
                    None => Resp::Set(Vec::new()),
                    Some(first) => member_set(
                        first
                            .members
                            .iter()
                            .filter(|member| {
                                !sets[1..].iter().any(|set| {
                                    set.is_some_and(|set| set.positions.contains_key(*member))
                                })
                            })
                            .cloned(),
                        self.reply_limit,
                    ),
                },
            };
        }
        let members: Vec<Bytes> = match operation {
            Algebra::Union => sets
                .iter()
                .flatten()
                .flat_map(|set| &set.members)
                .collect::<HashSet<_>>()
                .into_iter()
                .cloned()
                .collect(),
            Algebra::Intersection => {
                match sets
                    .iter()
                    .min_by_key(|set| set.map_or(0, |set| set.members.len()))
                    .copied()
                    .flatten()
                {
                    None => Vec::new(),
                    Some(smallest) => smallest
                        .members
                        .iter()
                        .filter(|member| {
                            sets.iter().all(|set| {
                                set.is_some_and(|set| set.positions.contains_key(*member))
                            })
                        })
                        .cloned()
                        .collect(),
                }
            }
            Algebra::Difference => match sets[0] {
                None => Vec::new(),
                Some(first) => first
                    .members
                    .iter()
                    .filter(|member| {
                        !sets[1..]
                            .iter()
                            .any(|set| set.is_some_and(|set| set.positions.contains_key(*member)))
                    })
                    .cloned()
                    .collect(),
            },
        };
        let destination = &args[1];
        self.discard_expired(destination, now);
        self.resolve_held_write(destination, now);
        if self.remove(destination).is_some() {
            self.record(now, || Mutation::Delete {
                key: destination.clone(),
            });
        }
        let count = members.len();
        if count > 0 {
            self.record(now, || Mutation::SetAdd {
                key: destination.clone(),
                members: members.clone(),
            });
            let mut set = SetValue::default();
            for member in members {
                set.insert(member);
            }
            self.write_entry(
                destination.clone(),
                Entry {
                    value: Value::Set(set),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
        }
        Resp::Integer(count as i64)
    }

    fn set_scan(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let options = match parse_scan(args, 2, false) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        let (cursor, members) = match self.value(&args[1], now) {
            Some(Value::Set(set)) => match scan_entries(&set.scan_index, &options, limit) {
                Ok(page) => page,
                Err(error) => return error,
            },
            None => (0, Vec::new()),
            _ => return wrong_type(),
        };
        array(vec![
            Resp::Bulk(Some(Bytes::from(cursor.to_string()))),
            member_array(members, limit),
        ])
    }

    pub(super) fn replay_set_add(
        &mut self,
        key: Bytes,
        members: Vec<Bytes>,
        now: i64,
    ) -> io::Result<()> {
        if members.is_empty() {
            return Err(invalid_journal());
        }
        let Value::Set(set) = self.collection_entry(&key, now, || Value::Set(SetValue::default()))
        else {
            return Err(invalid_journal());
        };
        for member in members {
            if !set.insert(member) {
                return Err(invalid_journal());
            }
        }
        Ok(())
    }

    pub(super) fn replay_set_remove(
        &mut self,
        key: Bytes,
        members: Vec<Bytes>,
        now: i64,
    ) -> io::Result<()> {
        if members.is_empty() {
            return Err(invalid_journal());
        }
        self.discard_expired(&key, now);
        let Some(Entry {
            value: Value::Set(set),
            ..
        }) = self.entries.get_mut(&key)
        else {
            return Err(invalid_journal());
        };
        for member in members {
            if set.remove(&member).is_none() {
                return Err(invalid_journal());
            }
        }
        if set.members.is_empty() {
            self.remove(&key);
        }
        Ok(())
    }
}

fn member_array(members: impl IntoIterator<Item = Bytes>, limit: usize) -> Resp {
    super::bounded_replies(
        members.into_iter().map(|member| Resp::Bulk(Some(member))),
        limit,
    )
}

fn member_set(members: impl IntoIterator<Item = Bytes>, limit: usize) -> Resp {
    match member_array(members, limit) {
        Resp::Array(Some(values)) => Resp::Set(values),
        error => error,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn run(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
        let args: Vec<_> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        engine.execute_at(&args, now)
    }

    fn members(reply: Resp) -> BTreeSet<Bytes> {
        let Resp::Set(values) = reply else {
            panic!("expected set, got {reply:?}");
        };
        let count = values.len();
        let members = bulk_members(values);
        assert_eq!(members.len(), count, "set reply contains duplicate members");
        members
    }

    fn array_members(reply: Resp) -> BTreeSet<Bytes> {
        let Resp::Array(Some(values)) = reply else {
            panic!("expected array, got {reply:?}");
        };
        bulk_members(values)
    }

    fn bulk_members(values: Vec<Resp>) -> BTreeSet<Bytes> {
        values
            .into_iter()
            .map(|value| {
                let Resp::Bulk(Some(value)) = value else {
                    panic!("expected member");
                };
                value
            })
            .collect()
    }

    fn expected(values: &[&str]) -> BTreeSet<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect()
    }

    fn assert_indexes(engine: &Engine, key: &str) {
        let key = Bytes::copy_from_slice(key.as_bytes());
        let Some(Entry {
            value: Value::Set(set),
            ..
        }) = engine.entries.get(&key)
        else {
            panic!("expected nonempty set");
        };
        assert_eq!(set.members.len(), set.positions.len());
        assert_eq!(set.members.len(), set.scan_index.len());
        for (position, member) in set.members.iter().enumerate() {
            assert_eq!(set.positions.get(member), Some(&position));
            assert!(
                set.scan_index
                    .contains(&(stable_hash(member), member.clone()))
            );
        }
    }

    #[test]
    fn duplicate_add_remove_are_actual_changes_and_last_member_deletes_key() {
        let mut engine = Engine::new();
        engine.set_recording(true);
        assert_eq!(
            run(&mut engine, 1000, &["SADD", "s", "a", "a", "b", "c", "b"]),
            Resp::Integer(3)
        );
        let mut recovered = Engine::new();
        recovered.replay(engine.take_batch().unwrap()).unwrap();
        assert_eq!(
            members(run(&mut recovered, 1000, &["SMEMBERS", "s"])),
            expected(&["a", "b", "c"])
        );
        assert_eq!(
            run(&mut engine, 1001, &["SADD", "s", "c", "a"]),
            Resp::Integer(0)
        );
        assert!(engine.take_batch().is_none());
        assert_eq!(
            run(&mut engine, 1002, &["SREM", "s", "a", "a", "missing"]),
            Resp::Integer(1)
        );
        recovered.replay(engine.take_batch().unwrap()).unwrap();
        assert_eq!(
            members(run(&mut recovered, 1002, &["SMEMBERS", "s"])),
            expected(&["b", "c"])
        );
        assert_eq!(
            run(&mut engine, 1003, &["SMISMEMBER", "s", "a", "c", "c", "b"]),
            array(vec![
                Resp::Integer(0),
                Resp::Integer(1),
                Resp::Integer(1),
                Resp::Integer(1)
            ])
        );
        assert_eq!(
            members(run(&mut engine, 1003, &["SMEMBERS", "s"])),
            expected(&["b", "c"])
        );
        assert_eq!(
            run(&mut engine, 1004, &["SREM", "s", "c", "c", "b"]),
            Resp::Integer(2)
        );
        recovered.replay(engine.take_batch().unwrap()).unwrap();
        assert_eq!(
            run(&mut recovered, 1004, &["TYPE", "s"]),
            Resp::Simple("none")
        );
        assert_eq!(run(&mut engine, 1004, &["TYPE", "s"]), Resp::Simple("none"));
    }

    #[test]
    fn swap_removal_repairs_moved_member_and_scan_observability() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SADD", "s", "a", "b", "c", "d", "e"]);
        for (removed, remaining) in [
            ("b", vec!["a", "c", "d", "e"]),
            ("e", vec!["a", "c", "d"]),
            ("a", vec!["c", "d"]),
        ] {
            assert_eq!(
                run(&mut engine, 1000, &["SREM", "s", removed]),
                Resp::Integer(1)
            );
            assert_indexes(&engine, "s");
            assert_eq!(
                members(run(&mut engine, 1000, &["SMEMBERS", "s"])),
                expected(&remaining)
            );
            let Resp::Array(Some(mut reply)) =
                run(&mut engine, 1000, &["SSCAN", "s", "0", "COUNT", "100"])
            else {
                panic!("scan array");
            };
            assert_eq!(array_members(reply.pop().unwrap()), expected(&remaining));
            for member in remaining {
                assert_eq!(
                    run(&mut engine, 1000, &["SISMEMBER", "s", member]),
                    Resp::Integer(1)
                );
            }
        }
    }

    #[test]
    fn algebra_overlap_and_wrong_types_validate_before_destination_mutation() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SADD", "a", "1", "2", "3"]);
        run(&mut engine, 1000, &["SADD", "b", "2", "4"]);
        assert_eq!(
            members(run(&mut engine, 1000, &["SUNION", "a", "a", "b"])),
            expected(&["1", "2", "3", "4"])
        );
        assert_eq!(
            members(run(&mut engine, 1000, &["SINTER", "a", "a", "b"])),
            expected(&["2"])
        );
        assert!(members(run(&mut engine, 1000, &["SDIFF", "a", "a", "b"])).is_empty());
        assert_eq!(
            run(&mut engine, 1000, &["SDIFFSTORE", "a", "a", "b"]),
            Resp::Integer(2)
        );
        assert_eq!(
            members(run(&mut engine, 1000, &["SMEMBERS", "a"])),
            expected(&["1", "3"])
        );
        assert_eq!(
            run(&mut engine, 1000, &["SINTERSTORE", "b", "a", "b"]),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, 1000, &["TYPE", "b"]), Resp::Simple("none"));
        run(&mut engine, 1000, &["SET", "bad", "value"]);
        engine.set_recording(true);
        for command in ["SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"] {
            assert!(
                matches!(run(&mut engine, 1001, &[command, "a", "missing", "bad"]), Resp::Error(message) if message.starts_with("WRONGTYPE"))
            );
            assert!(engine.take_batch().is_none());
            assert_eq!(
                members(run(&mut engine, 1001, &["SMEMBERS", "a"])),
                expected(&["1", "3"])
            );
        }
        assert!(matches!(
            run(&mut engine, 1001, &["SMOVE", "a", "bad", "1"]),
            Resp::Error(_)
        ));
        assert!(engine.take_batch().is_none());
        assert_eq!(
            run(&mut engine, 1001, &["SISMEMBER", "a", "1"]),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut engine, 1001, &["SMOVE", "a", "a", "1"]),
            Resp::Integer(1)
        );
        assert!(engine.take_batch().is_none());
    }

    #[test]
    fn ttl_is_preserved_for_members_and_moves_but_reset_for_stores() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SADD", "a", "1", "2", "3"]);
        run(&mut engine, 1000, &["SADD", "b", "2"]);
        run(&mut engine, 1000, &["PEXPIRE", "a", "5000"]);
        run(&mut engine, 1000, &["PEXPIRE", "b", "6000"]);
        run(&mut engine, 1100, &["SADD", "a", "4"]);
        run(&mut engine, 1100, &["SREM", "a", "4"]);
        assert_eq!(
            run(&mut engine, 1100, &["SMOVE", "a", "b", "1"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, 1100, &["PTTL", "a"]), Resp::Integer(4900));
        assert_eq!(run(&mut engine, 1100, &["PTTL", "b"]), Resp::Integer(5900));
        assert_eq!(
            run(&mut engine, 1100, &["SUNIONSTORE", "b", "a", "b"]),
            Resp::Integer(3)
        );
        assert_eq!(run(&mut engine, 1100, &["PTTL", "b"]), Resp::Integer(-1));
        assert_eq!(run(&mut engine, 1100, &["PTTL", "a"]), Resp::Integer(4900));
    }

    #[test]
    fn random_counts_nulls_duplicates_and_pops_have_exact_observable_semantics() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, 1000, &["SMEMBERS", "missing"]),
            Resp::Set(Vec::new())
        );
        assert_eq!(
            run(&mut engine, 1000, &["SPOP", "missing"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut engine, 1000, &["SPOP", "missing", "2"]),
            Resp::Set(vec![])
        );
        assert_eq!(
            run(&mut engine, 1000, &["SRANDMEMBER", "missing"]),
            Resp::Bulk(None)
        );
        assert_eq!(
            run(&mut engine, 1000, &["SRANDMEMBER", "missing", "-2"]),
            array(vec![])
        );
        run(&mut engine, 1000, &["SADD", "s", "a", "b", "c", "d", "e"]);
        let all = expected(&["a", "b", "c", "d", "e"]);
        assert_eq!(
            run(&mut engine, 1000, &["SPOP", "s", "0"]),
            Resp::Set(vec![])
        );
        assert_eq!(
            run(&mut engine, 1000, &["SRANDMEMBER", "s", "0"]),
            array(vec![])
        );
        assert_eq!(
            array_members(run(&mut engine, 1000, &["SRANDMEMBER", "s", "99"])),
            all
        );
        let Resp::Array(Some(sample)) = run(&mut engine, 1000, &["SRANDMEMBER", "s", "-20"]) else {
            panic!("sample array");
        };
        assert_eq!(sample.len(), 20);
        assert!(array_members(array(sample)).is_subset(&all));
        assert!(matches!(
            run(&mut engine, 1000, &["SPOP", "s", "-1"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut engine,
                1000,
                &["SRANDMEMBER", "s", "-9223372036854775808"]
            ),
            Resp::Error(_)
        ));
        let popped = members(run(&mut engine, 1000, &["SPOP", "s", "3"]));
        assert_eq!(popped.len(), 3);
        assert!(popped.is_subset(&all));
        let remaining = members(run(&mut engine, 1000, &["SMEMBERS", "s"]));
        assert_eq!(remaining.len(), 2);
        assert!(remaining.is_disjoint(&popped));
        assert_eq!(
            remaining.union(&popped).cloned().collect::<BTreeSet<_>>(),
            all
        );
        assert_indexes(&engine, "s");
        assert_eq!(
            members(run(&mut engine, 1000, &["SPOP", "s", "99"])),
            remaining
        );
        assert_eq!(run(&mut engine, 1000, &["TYPE", "s"]), Resp::Simple("none"));
        run(&mut engine, 1000, &["SET", "bad", "value"]);
        for command in ["SPOP", "SRANDMEMBER"] {
            assert!(
                matches!(run(&mut engine, 1000, &[command, "bad", "0"]), Resp::Error(message) if message.starts_with("WRONGTYPE"))
            );
        }
    }

    #[test]
    fn scan_continuously_present_members_survive_swap_removal_and_insertion() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"scan");
        let stable: BTreeSet<_> = (0..80)
            .map(|index| Bytes::from(format!("stable:{index}")))
            .collect();
        let transient: Vec<_> = (0..80)
            .map(|index| Bytes::from(format!("transient:{index}")))
            .collect();
        let mut args = vec![Bytes::from_static(b"SADD"), key.clone()];
        args.extend(stable.iter().cloned());
        args.extend(transient.iter().cloned());
        engine.execute_at(&args, 1000);
        let mut cursor = Bytes::from_static(b"0");
        let mut found = BTreeSet::new();
        let mut iteration = 0;
        loop {
            let args = [
                Bytes::from_static(b"SSCAN"),
                key.clone(),
                cursor,
                Bytes::from_static(b"COUNT"),
                Bytes::from_static(b"3"),
            ];
            let Resp::Array(Some(mut values)) = engine.execute_at(&args, 1000) else {
                panic!("scan array");
            };
            found.extend(array_members(values.pop().unwrap()));
            let Resp::Bulk(Some(next)) = values.pop().unwrap() else {
                panic!("cursor bulk");
            };
            let mut removals = vec![Bytes::from_static(b"SREM"), key.clone()];
            removals.extend(transient.iter().cloned());
            engine.execute_at(&removals, 1000);
            engine.execute_at(
                &[
                    Bytes::from_static(b"SADD"),
                    key.clone(),
                    Bytes::from(format!("new:{iteration}")),
                ],
                1000,
            );
            assert_indexes(&engine, "scan");
            iteration += 1;
            assert!(iteration < 1000, "cursor must terminate");
            if next == b"0"[..] {
                break;
            }
            cursor = next;
        }
        assert!(stable.is_subset(&found));
    }

    #[test]
    fn journal_replay_preserves_selected_removals_moves_and_overlapping_store() {
        let mut source = Engine::new();
        source.set_recording(true);
        let mut recovered = Engine::new();
        recovered.set_recording(true);
        for args in [
            vec!["SADD", "a", "a", "b", "c", "d", "e"],
            vec!["SADD", "b", "existing"],
            vec!["SPOP", "a", "2"],
            vec!["SUNIONSTORE", "a", "a", "b"],
            vec!["SMOVE", "a", "b", "existing"],
            vec!["SDIFFSTORE", "b", "a", "b"],
        ] {
            run(&mut source, 1000, &args);
            let batch = source.take_batch().unwrap();
            assert!(!batch.mutations.is_empty());
            assert!(source.take_batch().is_none());
            recovered.replay(batch).unwrap();
            assert!(recovered.take_batch().is_none());
            for key in ["a", "b"] {
                assert_eq!(
                    members(run(&mut source, 1000, &["SMEMBERS", key])),
                    members(run(&mut recovered, 1000, &["SMEMBERS", key]))
                );
            }
        }
    }

    #[test]
    fn replay_rejects_empty_duplicate_missing_and_wrong_type_mutations() {
        let key = Bytes::from_static(b"s");
        let member = Bytes::from_static(b"member");
        let mut engine = Engine::new();
        assert!(engine.replay_set_add(key.clone(), vec![], 1000).is_err());
        assert!(engine.replay_set_remove(key.clone(), vec![], 1000).is_err());
        assert!(
            engine
                .replay_set_remove(key.clone(), vec![member.clone()], 1000)
                .is_err()
        );
        engine
            .replay_set_add(key.clone(), vec![member.clone()], 1000)
            .unwrap();
        assert!(
            engine
                .replay_set_add(key.clone(), vec![member.clone()], 1000)
                .is_err()
        );
        engine
            .replay_set_remove(key.clone(), vec![member.clone()], 1000)
            .unwrap();
        assert_eq!(run(&mut engine, 1000, &["TYPE", "s"]), Resp::Simple("none"));
        run(&mut engine, 1000, &["SET", "s", "wrong"]);
        assert!(
            engine
                .replay_set_add(key.clone(), vec![member.clone()], 1000)
                .is_err()
        );
        assert!(engine.replay_set_remove(key, vec![member], 1000).is_err());
        assert_eq!(
            run(&mut engine, 1000, &["GET", "s"]),
            Resp::Bulk(Some(Bytes::from_static(b"wrong")))
        );
    }
    #[test]
    fn counted_algebra_limits_aliases_and_validation_are_read_only() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SADD", "a", "a", "b", "c"]);
        run(&mut engine, 1000, &["SADD", "b", "b", "c", "d"]);
        for (command, expected) in [("SDIFFCARD", 1), ("SINTERCARD", 2), ("SUNIONCARD", 4)] {
            assert_eq!(
                run(&mut engine, 1000, &[command, "2", "a", "b"]),
                Resp::Integer(expected)
            );
            assert_eq!(
                run(&mut engine, 1000, &[command, "2", "a", "b", "LIMIT", "1"]),
                Resp::Integer(1)
            );
            assert_eq!(
                run(&mut engine, 1000, &[command, "2", "a", "b", "LIMIT", "0"]),
                Resp::Integer(expected)
            );
            for args in [
                vec![command, "0", "a"],
                vec![command, "9", "a"],
                vec![command, "1", "a", "LIMIT", "-1"],
                vec![command, "1", "a", "LIMIT"],
            ] {
                assert!(matches!(run(&mut engine, 1000, &args), Resp::Error(_)));
            }
        }
        assert_eq!(
            run(&mut engine, 1000, &["SDIFFCARD", "2", "a", "a"]),
            Resp::Integer(0)
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["SUNIONCARD", "3", "a", "a", "missing", "APPROX"]
            ),
            Resp::Integer(3)
        );
        run(&mut engine, 1000, &["SET", "bad", "wrong"]);
        for command in ["SDIFFCARD", "SINTERCARD", "SUNIONCARD"] {
            assert!(matches!(
                run(
                    &mut engine,
                    1000,
                    &[command, "3", "missing", "a", "bad", "LIMIT", "1"]
                ),
                Resp::Error(_)
            ));
        }
        assert_eq!(run(&mut engine, 1000, &["SCARD", "a"]), Resp::Integer(3));
    }

    #[test]
    fn approximate_union_uses_sketch_quality_and_count_limit() {
        let mut engine = Engine::new();
        for (key, start, end) in [("a", 0u64, 20_000), ("b", 10_000, 30_000)] {
            let mut args = vec![Bytes::from_static(b"SADD"), Bytes::from(key)];
            args.extend((start..end).map(|i| Bytes::copy_from_slice(&i.to_le_bytes())));
            assert_eq!(engine.execute_at(&args, 1000), Resp::Integer(20_000));
        }
        let Resp::Integer(count) = run(&mut engine, 1000, &["SUNIONCARD", "2", "a", "b", "APPROX"])
        else {
            panic!("cardinality");
        };
        assert!((count as f64 / 30_000.0 - 1.0).abs() < 0.03);
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["SUNIONCARD", "2", "a", "b", "APPROX", "LIMIT", "100"]
            ),
            Resp::Integer(100)
        );
    }
}
