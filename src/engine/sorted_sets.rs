use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::io;
use std::ops::Bound::{self, Excluded, Included, Unbounded};

use bytes::Bytes;

use super::scan::{ScanIndex, parse_scan, scan_entries, stable_hash};
use super::{
    CommandSpec, Engine, Mutation, Resp, Value, array, error, integer, integer_error,
    invalid_journal, syntax_error, wrong_type,
};

#[path = "zset_compat.rs"]
mod compat;

/// NaNs never enter either index; normalizing zero makes equality and ordering agree.
#[derive(Clone, Copy, Debug)]
struct Score(f64);

impl Score {
    fn new(value: f64) -> Option<Self> {
        (!value.is_nan()).then_some(Self(if value == 0.0 { 0.0 } else { value }))
    }
}

impl PartialEq for Score {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}
impl Eq for Score {}
impl PartialOrd for Score {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Score {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

#[derive(Default)]
pub(super) struct SortedSetValue {
    members: HashMap<Bytes, Score>,
    ordered: BTreeSet<(Score, Bytes)>,
    lex: BTreeSet<Bytes>,
    scan: ScanIndex,
}

/// Keep private score/index representations inside their owning module.
pub(super) struct Snapshot<'a>(std::collections::hash_map::Iter<'a, Bytes, Score>);

impl<'a> Iterator for Snapshot<'a> {
    type Item = (&'a Bytes, f64);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(member, score)| (member, score.0))
    }
}

impl SortedSetValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{hash_bytes, sampled, tree_bytes};
        hash_bytes::<Bytes, Score>(self.members.capacity())
            .saturating_add(tree_bytes::<(Score, Bytes)>(self.ordered.len()))
            .saturating_add(tree_bytes::<Bytes>(self.lex.len()))
            .saturating_add(tree_bytes::<(u64, Bytes)>(self.scan.len()))
            .saturating_add(sampled(self.members.keys(), samples, Bytes::len))
    }

    pub(super) fn geo_score(&self, member: &Bytes) -> Option<f64> {
        self.members.get(member).map(|score| score.0)
    }

    pub(super) fn geo_insert(&mut self, member: Bytes, score: f64) {
        self.insert(
            member,
            Score::new(score).expect("validated geospatial score"),
        );
    }

    pub(super) fn geo_range(&self, start: u64, end: u64) -> impl Iterator<Item = (&Bytes, f64)> {
        self.ordered
            .range((
                Included((Score(start as f64), Bytes::new())),
                Excluded((Score(end as f64), Bytes::new())),
            ))
            .map(|(score, member)| (member, score.0))
    }

    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot(self.members.iter())
    }

    fn insert(&mut self, member: Bytes, score: Score) {
        if let Some(previous) = self.members.insert(member.clone(), score) {
            self.ordered.remove(&(previous, member.clone()));
        } else {
            self.scan.insert((stable_hash(&member), member.clone()));
            self.lex.insert(member.clone());
        }
        self.ordered.insert((score, member));
    }

    fn remove(&mut self, member: &Bytes) -> Option<Bytes> {
        let (stored, score) = self.members.remove_entry(member)?;
        self.ordered.remove(&(score, stored.clone()));
        self.lex.remove(&stored);
        self.scan.remove(&(stable_hash(&stored), stored.clone()));
        Some(stored)
    }
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "zadd",
        -4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::zadd,
    ),
    CommandSpec::handler(
        "zincrby",
        4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::zincrby,
    ),
    CommandSpec::handler("zrem", -3, &["write", "fast"], (1, 1, 1), Engine::zrem),
    CommandSpec::handler("zcard", 2, &["readonly", "fast"], (1, 1, 1), Engine::zcard),
    CommandSpec::handler(
        "zscore",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::zscore,
    ),
    CommandSpec::handler(
        "zmscore",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::zmscore,
    ),
    CommandSpec::handler("zrank", -3, &["readonly", "fast"], (1, 1, 1), Engine::zrank).max(4),
    CommandSpec::handler(
        "zrevrank",
        -3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::zrank,
    )
    .max(4),
    CommandSpec::handler(
        "zcount",
        4,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::zcount,
    ),
    CommandSpec::handler("zrange", -4, &["readonly"], (1, 1, 1), Engine::zrange),
    CommandSpec::handler("zrevrange", -4, &["readonly"], (1, 1, 1), Engine::zrange),
    CommandSpec::handler(
        "zrangebyscore",
        -4,
        &["readonly"],
        (1, 1, 1),
        Engine::zrange,
    ),
    CommandSpec::handler(
        "zrevrangebyscore",
        -4,
        &["readonly"],
        (1, 1, 1),
        Engine::zrange,
    ),
    CommandSpec::handler(
        "zremrangebyrank",
        4,
        &["write"],
        (1, 1, 1),
        Engine::zremrange,
    ),
    CommandSpec::handler(
        "zremrangebyscore",
        4,
        &["write"],
        (1, 1, 1),
        Engine::zremrange,
    ),
    CommandSpec::handler("zscan", -3, &["readonly"], (1, 1, 1), Engine::zscan),
    CommandSpec::handler("zpopmin", -2, &["write", "fast"], (1, 1, 1), Engine::zpop).max(3),
    CommandSpec::handler("zpopmax", -2, &["write", "fast"], (1, 1, 1), Engine::zpop).max(3),
    CommandSpec::handler(
        "zdiff",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zinter",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zunion",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zdiffstore",
        -4,
        &["write", "denyoom", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zinterstore",
        -4,
        &["write", "denyoom", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zunionstore",
        -4,
        &["write", "denyoom", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zintercard",
        -3,
        &["readonly", "movablekeys"],
        (0, 0, 0),
        Engine::zalgebra,
    ),
    CommandSpec::handler(
        "zlexcount",
        4,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::zlexcount,
    ),
    CommandSpec::handler("zrangebylex", -4, &["readonly"], (1, 1, 1), Engine::zrange),
    CommandSpec::handler(
        "zrevrangebylex",
        -4,
        &["readonly"],
        (1, 1, 1),
        Engine::zrange,
    ),
    CommandSpec::handler("zremrangebylex", 4, &["write"], (1, 1, 1), Engine::zremlex),
    CommandSpec::handler(
        "zrangestore",
        -5,
        &["write", "denyoom"],
        (1, 2, 1),
        Engine::zrange_compat,
    ),
    CommandSpec::handler(
        "zrandmember",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::zrandmember,
    )
    .max(4),
    CommandSpec::execution(
        "zmpop",
        -4,
        &["write", "movablekeys"],
        (0, 0, 0),
        Engine::zmultipop,
    ),
    CommandSpec::execution(
        "bzmpop",
        -5,
        &["write", "blocking", "movablekeys"],
        (0, 0, 0),
        Engine::zmultipop,
    ),
    CommandSpec::execution(
        "bzpopmin",
        -3,
        &["write", "blocking", "fast"],
        (1, -2, 1),
        Engine::zblocking_pop,
    ),
    CommandSpec::execution(
        "bzpopmax",
        -3,
        &["write", "blocking", "fast"],
        (1, -2, 1),
        Engine::zblocking_pop,
    ),
];

pub(super) fn parse_float(raw: &[u8]) -> Option<f64> {
    if raw.is_empty() || raw.iter().any(u8::is_ascii_whitespace) {
        return None;
    }
    let value: f64 = std::str::from_utf8(raw).ok()?.parse().ok()?;
    let unsigned = if matches!(raw.first(), Some(b'+') | Some(b'-')) {
        &raw[1..]
    } else {
        raw
    };
    if value.is_infinite()
        && !unsigned.eq_ignore_ascii_case(b"inf")
        && !unsigned.eq_ignore_ascii_case(b"infinity")
    {
        return None;
    }
    // Redis rejects numeric overflow and underflow-to-zero, not explicit infinity.
    if value == 0.0
        && unsigned
            .iter()
            .take_while(|byte| **byte != b'e' && **byte != b'E')
            .any(|byte| matches!(byte, b'1'..=b'9'))
    {
        return None;
    }
    (!value.is_nan()).then_some(value)
}

fn parse_score(raw: &[u8]) -> Option<Score> {
    Score::new(parse_float(raw)?)
}

fn score_error() -> Resp {
    error("ERR value is not a valid float")
}

fn score_reply(score: Score) -> Resp {
    Resp::Double(score.0)
}

fn scan_score_reply(score: Score) -> Resp {
    let text = if score.0 == f64::INFINITY {
        "inf".to_owned()
    } else if score.0 == f64::NEG_INFINITY {
        "-inf".to_owned()
    } else {
        score.0.to_string()
    };
    Resp::Bulk(Some(Bytes::from(text)))
}

#[derive(Default)]
struct AddOptions {
    nx: bool,
    xx: bool,
    gt: bool,
    lt: bool,
    ch: bool,
    incr: bool,
}

#[derive(Clone, Copy)]
struct ScoreBound {
    score: Score,
    exclusive: bool,
}

impl ScoreBound {
    fn parse(raw: &[u8]) -> Option<Self> {
        let exclusive = raw.first() == Some(&b'(');
        Some(Self {
            score: parse_score(if exclusive { &raw[1..] } else { raw })?,
            exclusive,
        })
    }
}

type OrderedBound = Bound<(Score, Bytes)>;

/// A score boundary uses the smallest member as its tie-break. Advancing to the
/// next representable score implements inclusive upper/exclusive lower bounds
/// without inventing a largest binary member or copying any actual member.
fn next_score(score: Score) -> Option<Score> {
    let value = score.0;
    if value == f64::INFINITY {
        return None;
    }
    let bits = if value == 0.0 {
        1
    } else if value < 0.0 {
        value.to_bits() - 1
    } else {
        value.to_bits() + 1
    };
    Score::new(f64::from_bits(bits))
}

fn score_bounds(min: ScoreBound, max: ScoreBound) -> Option<(OrderedBound, OrderedBound)> {
    let lower = if min.exclusive {
        next_score(min.score)?
    } else {
        min.score
    };
    if lower > max.score || (lower == max.score && max.exclusive) {
        return None;
    }
    let upper = if max.exclusive {
        Excluded((max.score, Bytes::new()))
    } else if let Some(next) = next_score(max.score) {
        Excluded((next, Bytes::new()))
    } else {
        Unbounded
    };
    Some((Included((lower, Bytes::new())), upper))
}

fn rank_window(start: i64, stop: i64, length: usize) -> (usize, usize) {
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
    if start > stop {
        (0, 0)
    } else {
        (start as usize, (stop - start + 1) as usize)
    }
}

fn range_reply<'a>(
    entries: impl Iterator<Item = &'a (Score, Bytes)> + Clone,
    with_scores: bool,
    limit: usize,
) -> Resp {
    let mut budget = super::ReplyBudget::new(limit);
    for (_, member) in entries.clone() {
        if let Err(error) = budget.charge(
            if with_scores { 2 } else { 1 },
            member.len() + if with_scores { 32 } else { 0 },
        ) {
            return error;
        }
    }
    if with_scores {
        Resp::Scored(
            entries
                .map(|(score, member)| (member.clone(), score.0))
                .collect(),
        )
    } else {
        array(
            entries
                .map(|(_, member)| Resp::Bulk(Some(member.clone())))
                .collect(),
        )
    }
}

impl Engine {
    pub(super) fn sorted_set(
        &mut self,
        key: &Bytes,
        now: i64,
    ) -> Result<Option<&SortedSetValue>, Resp> {
        match self.value(key, now) {
            Some(Value::SortedSet(value)) => Ok(Some(value)),
            Some(_) => Err(wrong_type()),
            None => Ok(None),
        }
    }

    fn zadd(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut options = AddOptions::default();
        let mut position = 2;
        while let Some(arg) = args.get(position) {
            if arg.eq_ignore_ascii_case(b"NX") {
                options.nx = true;
            } else if arg.eq_ignore_ascii_case(b"XX") {
                options.xx = true;
            } else if arg.eq_ignore_ascii_case(b"GT") {
                options.gt = true;
            } else if arg.eq_ignore_ascii_case(b"LT") {
                options.lt = true;
            } else if arg.eq_ignore_ascii_case(b"CH") {
                options.ch = true;
            } else if arg.eq_ignore_ascii_case(b"INCR") {
                options.incr = true;
            } else {
                break;
            }
            position += 1;
        }
        if position == args.len() || !(args.len() - position).is_multiple_of(2) {
            return syntax_error();
        }
        if (options.nx && options.xx)
            || (options.gt && options.lt)
            || (options.nx && (options.gt || options.lt))
        {
            return syntax_error();
        }
        if options.incr && args.len() - position != 2 {
            return syntax_error();
        }
        let mut scores = Vec::with_capacity((args.len() - position) / 2);
        for pair in args[position..].as_chunks::<2>().0.iter() {
            let Some(score) = parse_score(&pair[0]) else {
                return score_error();
            };
            scores.push(score);
        }
        self.zadd_parsed(&args[1], &args[position..], &scores, options, now)
    }

    fn zincrby(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(score) = parse_score(&args[2]) else {
            return score_error();
        };
        self.zadd_parsed(
            &args[1],
            &args[2..],
            &[score],
            AddOptions {
                incr: true,
                ..AddOptions::default()
            },
            now,
        )
    }

    fn zadd_parsed(
        &mut self,
        key: &Bytes,
        pairs: &[Bytes],
        scores: &[Score],
        options: AddOptions,
        now: i64,
    ) -> Resp {
        let preview = if options.incr {
            Resp::Double(0.0)
        } else {
            Resp::Integer(0)
        };
        if let Err(reply) = super::ReplyBudget::new(self.reply_limit).response(&preview) {
            return reply;
        }
        let existing = match self.sorted_set(key, now) {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        // INCR is restricted to one pair; resolve its arithmetic before creating
        // the key or changing either index. Conditional no-ops precede arithmetic.
        let incremented = if options.incr {
            let old = existing
                .and_then(|value| value.members.get(&pairs[1]))
                .copied();
            if (options.nx && old.is_some()) || (options.xx && old.is_none()) {
                return Resp::Bulk(None);
            }
            let Some(score) = Score::new(old.map_or(0.0, |score| score.0) + scores[0].0) else {
                return error("ERR resulting score is not a number (NaN)");
            };
            Some(score)
        } else {
            None
        };
        let mut added = 0;
        let mut changed = 0;
        let mut result = None;
        let journaling = self.journal_enabled;
        let mut updates = Vec::new();
        for (pair, score) in pairs.as_chunks::<2>().0.iter().zip(scores) {
            let score = incremented.unwrap_or(*score);
            let member = &pair[1];
            let previous = match self.entries.get(key).map(|entry| &entry.value) {
                Some(Value::SortedSet(value)) => value.members.get_key_value(member),
                _ => None,
            };
            let old = previous.map(|(_, score)| *score);
            if (options.nx && old.is_some())
                || (options.xx && old.is_none())
                || old
                    .is_some_and(|old| (options.gt && score <= old) || (options.lt && score >= old))
            {
                continue;
            }
            result = Some(score);
            if old == Some(score) {
                continue;
            }
            let member = previous.map_or_else(
                || Bytes::copy_from_slice(member),
                |(stored, _)| stored.clone(),
            );
            let Value::SortedSet(value) =
                self.collection_entry(key, now, || Value::SortedSet(SortedSetValue::default()))
            else {
                unreachable!()
            };
            value.insert(member.clone(), score);
            added += i64::from(old.is_none());
            changed += 1;
            if journaling {
                updates.push((member, score.0));
            }
        }
        if !updates.is_empty() {
            self.record(now, || Mutation::SortedSetAdd {
                key: key.clone(),
                entries: updates,
            });
        }
        if options.incr {
            result.map_or(Resp::Bulk(None), score_reply)
        } else {
            Resp::Integer(if options.ch { changed } else { added })
        }
    }

    fn zcard(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.sorted_set(&args[1], now) {
            Ok(value) => Resp::Integer(value.map_or(0, |value| value.members.len()) as i64),
            Err(reply) => reply,
        }
    }

    fn zscore(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.sorted_set(&args[1], now) {
            Ok(value) => value
                .and_then(|value| value.members.get(&args[2]))
                .copied()
                .map_or(Resp::Bulk(None), score_reply),
            Err(reply) => reply,
        }
    }

    fn zmscore(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        match self.sorted_set(&args[1], now) {
            Ok(value) => super::bounded_replies(
                args[2..].iter().map(|member| {
                    value
                        .and_then(|value| value.members.get(member))
                        .copied()
                        .map_or(Resp::Bulk(None), score_reply)
                }),
                limit,
            ),
            Err(reply) => reply,
        }
    }

    fn zrank(&mut self, args: &[Bytes], now: i64) -> Resp {
        let with_score = args.len() == 4;
        if with_score && !args[3].eq_ignore_ascii_case(b"WITHSCORE") {
            return syntax_error();
        }
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(value)) => value,
            Ok(None) => {
                return if with_score {
                    Resp::Array(None)
                } else {
                    Resp::Bulk(None)
                };
            }
            Err(reply) => return reply,
        };
        let Some(score) = value.members.get(&args[2]) else {
            return if with_score {
                Resp::Array(None)
            } else {
                Resp::Bulk(None)
            };
        };
        let mut rank = value.ordered.range(..(*score, args[2].clone())).count();
        if args[0].eq_ignore_ascii_case(b"ZREVRANK") {
            rank = value.members.len() - 1 - rank;
        }
        let rank = Resp::Integer(rank as i64);
        if with_score {
            array(vec![rank, score_reply(*score)])
        } else {
            rank
        }
    }

    fn zcount(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (Some(min), Some(max)) = (ScoreBound::parse(&args[2]), ScoreBound::parse(&args[3]))
        else {
            return score_error();
        };
        match self.sorted_set(&args[1], now) {
            Ok(value) => Resp::Integer(
                value
                    .and_then(|value| {
                        score_bounds(min, max).map(|bounds| value.ordered.range(bounds).count())
                    })
                    .unwrap_or(0) as i64,
            ),
            Err(reply) => reply,
        }
    }

    fn zrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.zrange_compat(args, now)
    }

    fn zrem(&mut self, args: &[Bytes], now: i64) -> Resp {
        if let Err(reply) = super::ReplyBudget::new(self.reply_limit).response(&Resp::Integer(0)) {
            return reply;
        }
        if let Err(reply) = self.sorted_set(&args[1], now) {
            return reply;
        }
        let mut removed = Vec::new();
        let mut count = 0;
        if let Some(entry) = self.entries.get_mut(&args[1]) {
            let Value::SortedSet(value) = &mut entry.value else {
                unreachable!()
            };
            for member in &args[2..] {
                if let Some(member) = value.remove(member) {
                    count += 1;
                    if self.journal_enabled {
                        removed.push(member);
                    }
                }
            }
            if value.members.is_empty() {
                self.remove(&args[1]);
            }
        }
        if !removed.is_empty() {
            self.record(now, || Mutation::SortedSetRemove {
                key: args[1].clone(),
                members: removed,
            });
        }
        Resp::Integer(count)
    }

    fn zremove_selected(&mut self, key: &Bytes, members: Vec<Bytes>, now: i64) {
        if members.is_empty() {
            return;
        }
        let Some(entry) = self.entries.get_mut(key) else {
            unreachable!()
        };
        let Value::SortedSet(value) = &mut entry.value else {
            unreachable!()
        };
        for member in &members {
            let _ = value.remove(member);
        }
        if value.members.is_empty() {
            self.remove(key);
        }
        self.record(now, || Mutation::SortedSetRemove {
            key: key.clone(),
            members,
        });
    }

    fn zremrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        let by_score = args[0].eq_ignore_ascii_case(b"ZREMRANGEBYSCORE");
        let (bounds, ranks) = if by_score {
            let (Some(min), Some(max)) = (ScoreBound::parse(&args[2]), ScoreBound::parse(&args[3]))
            else {
                return score_error();
            };
            (score_bounds(min, max), None)
        } else {
            let (Some(start), Some(stop)) = (integer(&args[2]), integer(&args[3])) else {
                return integer_error();
            };
            (None, Some((start, stop)))
        };
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(value)) => value,
            Ok(None) => return Resp::Integer(0),
            Err(reply) => return reply,
        };
        let members: Vec<_> = if let Some((start, stop)) = ranks {
            let (offset, count) = rank_window(start, stop, value.members.len());
            value
                .ordered
                .iter()
                .skip(offset)
                .take(count)
                .map(|(_, member)| member.clone())
                .collect()
        } else if let Some(bounds) = bounds {
            value
                .ordered
                .range(bounds)
                .map(|(_, member)| member.clone())
                .collect()
        } else {
            Vec::new()
        };
        let count = members.len();
        if let Err(reply) =
            super::ReplyBudget::new(self.reply_limit).response(&Resp::Integer(count as i64))
        {
            return reply;
        }
        self.zremove_selected(&args[1], members, now);
        Resp::Integer(count as i64)
    }

    fn zpop(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let count = if let Some(raw) = args.get(2) {
            match integer(raw) {
                Some(count) if count >= 0 => count as usize,
                _ => return integer_error(),
            }
        } else {
            1
        };
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(value)) => value,
            Ok(None) => return Resp::Scored(Vec::new()),
            Err(reply) => return reply,
        };
        let reverse = args[0].eq_ignore_ascii_case(b"ZPOPMAX");
        let reply = if reverse {
            range_reply(value.ordered.iter().rev().take(count), true, limit)
        } else {
            range_reply(value.ordered.iter().take(count), true, limit)
        };
        if let Err(error) = super::ReplyBudget::new(limit).response(&reply) {
            return error;
        }
        let Resp::Scored(replies) = reply else {
            return reply;
        };
        let members = replies.iter().map(|(member, _)| member.clone()).collect();
        self.zremove_selected(&args[1], members, now);
        Resp::Scored(replies)
    }

    fn zscan(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let options = match parse_scan(args, 2, false) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(value)) => value,
            Ok(None) => {
                return array(vec![
                    Resp::Bulk(Some(Bytes::from_static(b"0"))),
                    array(Vec::new()),
                ]);
            }
            Err(reply) => return reply,
        };
        let (cursor, members) = match scan_entries(&value.scan, &options, limit) {
            Ok(page) => page,
            Err(error) => return error,
        };
        let mut budget = super::ReplyBudget::new(limit);
        for member in &members {
            if let Err(error) = budget.charge(2, member.len() + 32) {
                return error;
            }
        }
        let mut replies = Vec::with_capacity(members.len() * 2);
        for member in members {
            let score = value.members[&member];
            replies.push(Resp::Bulk(Some(member)));
            replies.push(scan_score_reply(score));
        }
        array(vec![
            Resp::Bulk(Some(Bytes::from(cursor.to_string()))),
            array(replies),
        ])
    }

    pub(super) fn replay_zadd(
        &mut self,
        key: Bytes,
        entries: Vec<(Bytes, f64)>,
        now: i64,
    ) -> io::Result<()> {
        if entries.is_empty()
            || entries
                .iter()
                .any(|(_, score)| Score::new(*score).is_none())
        {
            return Err(invalid_journal());
        }
        if self.sorted_set(&key, now).is_err() {
            return Err(invalid_journal());
        }
        let Value::SortedSet(value) =
            self.collection_entry(&key, now, || Value::SortedSet(SortedSetValue::default()))
        else {
            return Err(invalid_journal());
        };
        for (member, score) in entries {
            let score = Score::new(score).ok_or_else(invalid_journal)?;
            if value.members.get(&member) == Some(&score) {
                return Err(invalid_journal());
            }
            value.insert(member, score);
        }
        Ok(())
    }

    pub(super) fn replay_zremove(
        &mut self,
        key: Bytes,
        members: Vec<Bytes>,
        now: i64,
    ) -> io::Result<()> {
        if members.is_empty() {
            return Err(invalid_journal());
        }
        self.discard_expired(&key, now);
        let Some(entry) = self.entries.get_mut(&key) else {
            return Err(invalid_journal());
        };
        let Value::SortedSet(value) = &mut entry.value else {
            return Err(invalid_journal());
        };
        for member in members {
            if value.remove(&member).is_none() {
                return Err(invalid_journal());
            }
        }
        if value.members.is_empty() {
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
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        engine.execute_at(&args, 1000)
    }

    #[test]
    fn scores_are_doubles_in_scalar_multiscore_and_rank_replies() {
        let mut engine = Engine::new();
        run(&mut engine, &["ZADD", "z", "1.25", "a", "inf", "b"]);
        assert_eq!(run(&mut engine, &["ZSCORE", "z", "a"]), Resp::Double(1.25));
        assert_eq!(
            run(&mut engine, &["ZMSCORE", "z", "a", "missing", "b"]),
            array(vec![
                Resp::Double(1.25),
                Resp::Bulk(None),
                Resp::Double(f64::INFINITY),
            ])
        );
        assert_eq!(
            run(&mut engine, &["ZRANK", "z", "a", "WITHSCORE"]),
            array(vec![Resp::Integer(0), Resp::Double(1.25)])
        );
        assert_eq!(
            run(&mut engine, &["ZREVRANK", "z", "missing", "WITHSCORE"]),
            Resp::Array(None)
        );
        assert_eq!(
            run(&mut engine, &["ZADD", "z", "INCR", "0.5", "a"]),
            Resp::Double(1.75)
        );
    }

    #[test]
    fn ranges_and_pops_keep_member_score_pairs_including_empty_results() {
        let mut engine = Engine::new();
        for command in [
            vec!["ZRANGE", "missing", "0", "-1", "WITHSCORES"],
            vec!["ZPOPMIN", "missing"],
        ] {
            assert_eq!(run(&mut engine, &command), Resp::Scored(Vec::new()));
        }
        run(&mut engine, &["ZADD", "z", "-inf", "a", "2.5", "b"]);
        assert_eq!(
            run(&mut engine, &["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
            Resp::Scored(vec![
                (Bytes::from_static(b"a"), f64::NEG_INFINITY),
                (Bytes::from_static(b"b"), 2.5),
            ])
        );
        assert_eq!(
            run(&mut engine, &["ZRANGE", "z", "3", "2", "WITHSCORES"]),
            Resp::Scored(Vec::new())
        );
        assert_eq!(
            run(&mut engine, &["ZPOPMAX", "z", "1"]),
            Resp::Scored(vec![(Bytes::from_static(b"b"), 2.5)])
        );
        assert_eq!(
            run(&mut engine, &["ZRANGE", "z", "0", "-1"]),
            array(vec![Resp::Bulk(Some(Bytes::from_static(b"a")))])
        );
    }

    #[test]
    fn scan_scores_remain_bulk_strings_in_an_array() {
        let mut engine = Engine::new();
        run(&mut engine, &["ZADD", "z", "1.25", "a"]);
        assert_eq!(
            run(&mut engine, &["ZSCAN", "z", "0"]),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"0"))),
                array(vec![
                    Resp::Bulk(Some(Bytes::from_static(b"a"))),
                    Resp::Bulk(Some(Bytes::from_static(b"1.25"))),
                ]),
            ])
        );
    }
}
