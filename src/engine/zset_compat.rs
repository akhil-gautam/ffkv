//! Redis 8.10 sorted-set algebra, range and multi-pop options.
//! Behavioral references: redis.io/docs/latest/commands/{zrange,zunionstore,zmpop,zrandmember}/.
use super::super::{
    BlockingKind, BlockingPop, Execution, ReplyBudget, blocking_timeout, reply_limit_error,
};
use super::*;
use rand::{Rng, seq::IteratorRandom};

#[derive(Clone)]
enum LexBound {
    Negative,
    Positive,
    Member(Bytes, bool),
}
impl LexBound {
    fn parse(raw: &Bytes) -> Result<Self, Resp> {
        match raw.as_ref() {
            b"-" => Ok(Self::Negative),
            b"+" => Ok(Self::Positive),
            [b'[', rest @ ..] => Ok(Self::Member(Bytes::copy_from_slice(rest), false)),
            [b'(', rest @ ..] => Ok(Self::Member(Bytes::copy_from_slice(rest), true)),
            _ => Err(error("ERR min or max not valid string range item")),
        }
    }
}
fn lex_bounds(min: LexBound, max: LexBound) -> Option<(Bound<Bytes>, Bound<Bytes>)> {
    let lower = match min {
        LexBound::Negative => Unbounded,
        LexBound::Positive => return None,
        LexBound::Member(member, true) => Excluded(member),
        LexBound::Member(member, false) => Included(member),
    };
    let upper = match max {
        LexBound::Positive => Unbounded,
        LexBound::Negative => return None,
        LexBound::Member(member, true) => Excluded(member),
        LexBound::Member(member, false) => Included(member),
    };
    if let (Included(a) | Excluded(a), Included(b) | Excluded(b)) = (&lower, &upper)
        && (a > b || (a == b && (matches!(lower, Excluded(_)) || matches!(upper, Excluded(_)))))
    {
        return None;
    }
    Some((lower, upper))
}

enum RangeMode {
    Rank(i64, i64),
    Score(Option<(OrderedBound, OrderedBound)>),
    Lex(Option<(Bound<Bytes>, Bound<Bytes>)>),
}
struct RangeOptions {
    mode: RangeMode,
    reverse: bool,
    with_scores: bool,
    offset: usize,
    count: usize,
}
impl RangeOptions {
    fn parse(args: &[Bytes], store: bool) -> Result<Self, Resp> {
        let start = if store { 3 } else { 2 };
        let modern = store || args[0].eq_ignore_ascii_case(b"ZRANGE");
        let legacy_lex = args[0].eq_ignore_ascii_case(b"ZRANGEBYLEX")
            || args[0].eq_ignore_ascii_case(b"ZREVRANGEBYLEX");
        let mut mode = if legacy_lex {
            2
        } else if args[0].eq_ignore_ascii_case(b"ZRANGEBYSCORE")
            || args[0].eq_ignore_ascii_case(b"ZREVRANGEBYSCORE")
        {
            1
        } else {
            0
        };
        let mut reverse = args[0].eq_ignore_ascii_case(b"ZREVRANGE")
            || args[0].eq_ignore_ascii_case(b"ZREVRANGEBYSCORE")
            || args[0].eq_ignore_ascii_case(b"ZREVRANGEBYLEX");
        let mut with_scores = false;
        let mut limit = None;
        let mut position = start + 2;
        while position < args.len() {
            let option = &args[position];
            if modern && option.eq_ignore_ascii_case(b"BYSCORE") && mode != 2 {
                mode = 1;
            } else if modern && option.eq_ignore_ascii_case(b"BYLEX") && mode != 1 {
                mode = 2;
            } else if modern && option.eq_ignore_ascii_case(b"REV") {
                reverse = true;
            } else if !store && !legacy_lex && option.eq_ignore_ascii_case(b"WITHSCORES") {
                with_scores = true;
            } else if option.eq_ignore_ascii_case(b"LIMIT") && position + 2 < args.len() {
                let (Some(offset), Some(count)) =
                    (integer(&args[position + 1]), integer(&args[position + 2]))
                else {
                    return Err(integer_error());
                };
                limit = Some((offset, count));
                position += 2;
            } else {
                return Err(syntax_error());
            }
            position += 1;
        }
        if limit.is_some() && mode == 0 {
            return Err(syntax_error());
        }
        let (offset, count) = limit.unwrap_or((0, -1));
        let (offset, count) = if offset < 0 {
            (0, 0)
        } else {
            (
                offset as usize,
                if count < 0 {
                    usize::MAX
                } else {
                    count as usize
                },
            )
        };
        let (min, max) = if reverse && mode != 0 {
            (&args[start + 1], &args[start])
        } else {
            (&args[start], &args[start + 1])
        };
        let mode = match mode {
            0 => {
                let (Some(a), Some(b)) = (integer(min), integer(max)) else {
                    return Err(integer_error());
                };
                RangeMode::Rank(a, b)
            }
            1 => {
                let (Some(a), Some(b)) = (ScoreBound::parse(min), ScoreBound::parse(max)) else {
                    return Err(score_error());
                };
                RangeMode::Score(score_bounds(a, b))
            }
            _ => RangeMode::Lex(lex_bounds(LexBound::parse(min)?, LexBound::parse(max)?)),
        };
        Ok(Self {
            mode,
            reverse,
            with_scores,
            offset,
            count,
        })
    }
    fn select(
        &self,
        value: &SortedSetValue,
        budget: Option<usize>,
    ) -> Result<Vec<(Score, Bytes)>, Resp> {
        let mut result = Vec::new();
        let mut budget = budget.map(ReplyBudget::new);
        let mut push = |score: Score, member: &Bytes| -> Result<(), Resp> {
            if let Some(budget) = budget.as_mut() {
                budget.charge(
                    if self.with_scores { 2 } else { 1 },
                    member.len() + if self.with_scores { 32 } else { 0 },
                )?;
            }
            result.push((score, member.clone()));
            Ok(())
        };
        match &self.mode {
            RangeMode::Rank(start, stop) => {
                let (offset, count) = rank_window(*start, *stop, value.members.len());
                if self.reverse {
                    for (score, member) in value.ordered.iter().rev().skip(offset).take(count) {
                        push(*score, member)?;
                    }
                } else {
                    for (score, member) in value.ordered.iter().skip(offset).take(count) {
                        push(*score, member)?;
                    }
                }
            }
            RangeMode::Score(Some(bounds)) => {
                if self.reverse {
                    for (score, member) in value
                        .ordered
                        .range(bounds.clone())
                        .rev()
                        .skip(self.offset)
                        .take(self.count)
                    {
                        push(*score, member)?;
                    }
                } else {
                    for (score, member) in value
                        .ordered
                        .range(bounds.clone())
                        .skip(self.offset)
                        .take(self.count)
                    {
                        push(*score, member)?;
                    }
                }
            }
            RangeMode::Lex(Some(bounds)) => {
                if self.reverse {
                    for member in value
                        .lex
                        .range(bounds.clone())
                        .rev()
                        .skip(self.offset)
                        .take(self.count)
                    {
                        push(value.members[member], member)?;
                    }
                } else {
                    for member in value
                        .lex
                        .range(bounds.clone())
                        .skip(self.offset)
                        .take(self.count)
                    {
                        push(value.members[member], member)?;
                    }
                }
            }
            _ => {}
        }
        Ok(result)
    }
}
fn selected_reply(entries: Vec<(Score, Bytes)>, with_scores: bool) -> Resp {
    if with_scores {
        Resp::Scored(
            entries
                .into_iter()
                .map(|(score, member)| (member, score.0))
                .collect(),
        )
    } else {
        array(
            entries
                .into_iter()
                .map(|(_, member)| Resp::Bulk(Some(member)))
                .collect(),
        )
    }
}

#[derive(Clone, Copy)]
enum Aggregate {
    Sum,
    Min,
    Max,
    Count,
}
fn number(value: f64) -> f64 {
    if value.is_nan() { 0.0 } else { value }
}
fn weighted(score: f64, weight: f64, aggregate: Aggregate) -> f64 {
    number(if matches!(aggregate, Aggregate::Count) {
        weight
    } else {
        score * weight
    })
}
fn aggregate_score(old: f64, next: f64, aggregate: Aggregate) -> f64 {
    match aggregate {
        Aggregate::Sum | Aggregate::Count => number(old + next),
        Aggregate::Min => old.min(next),
        Aggregate::Max => old.max(next),
    }
}
enum Input<'a> {
    Empty,
    Sorted(&'a SortedSetValue),
    Set(&'a super::super::sets::SetValue),
}
impl Input<'_> {
    fn len(&self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Sorted(v) => v.members.len(),
            Self::Set(v) => v.len(),
        }
    }
    fn score(&self, member: &Bytes) -> Option<f64> {
        match self {
            Self::Empty => None,
            Self::Sorted(v) => v.members.get(member).map(|s| s.0),
            Self::Set(v) => v.contains(member).then_some(1.0),
        }
    }
    fn visit(&self, mut callback: impl FnMut(&Bytes, f64)) {
        match self {
            Self::Empty => {}
            Self::Sorted(v) => {
                for (m, s) in &v.members {
                    callback(m, s.0);
                }
            }
            Self::Set(v) => {
                for m in v.iter() {
                    callback(m, 1.0);
                }
            }
        }
    }
    fn visit_until(&self, mut callback: impl FnMut(&Bytes, f64) -> bool) {
        match self {
            Self::Empty => {}
            Self::Sorted(value) => {
                for (member, score) in &value.members {
                    if !callback(member, score.0) {
                        break;
                    }
                }
            }
            Self::Set(value) => {
                for member in value.iter() {
                    if !callback(member, 1.0) {
                        break;
                    }
                }
            }
        }
    }
}
fn key_count(args: &[Bytes], position: usize) -> Result<usize, Resp> {
    let Some(count) = args.get(position).and_then(|raw| integer(raw)) else {
        return Err(integer_error());
    };
    if count <= 0 {
        return Err(error("ERR at least 1 input key is needed"));
    }
    let count = usize::try_from(count).map_err(|_| integer_error())?;
    if count > args.len().saturating_sub(position + 1) {
        return Err(syntax_error());
    }
    Ok(count)
}

impl Engine {
    pub(super) fn zrange_compat(&mut self, args: &[Bytes], now: i64) -> Resp {
        let store = args[0].eq_ignore_ascii_case(b"ZRANGESTORE");
        let options = match RangeOptions::parse(args, store) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let source = &args[if store { 2 } else { 1 }];
        let limit = self.reply_limit;
        let entries = match self.sorted_set(source, now) {
            Ok(Some(value)) => match options.select(value, (!store).then_some(limit)) {
                Ok(v) => v,
                Err(e) => return e,
            },
            Ok(None) => Vec::new(),
            Err(e) => return e,
        };
        if store {
            self.zreplace(&args[1], entries, now)
        } else {
            selected_reply(entries, options.with_scores)
        }
    }
    fn zreplace(&mut self, destination: &Bytes, entries: Vec<(Score, Bytes)>, now: i64) -> Resp {
        let count = entries.len();
        if let Err(e) = ReplyBudget::new(self.reply_limit).response(&Resp::Integer(count as i64)) {
            return e;
        }
        self.discard_expired(destination, now);
        if self.remove(destination).is_some() {
            self.record(now, || Mutation::Delete {
                key: destination.clone(),
            });
        }
        if !entries.is_empty() {
            let journaling = self.journal_enabled;
            let mut updates = Vec::new();
            let Value::SortedSet(value) = self.collection_entry(destination, now, || {
                Value::SortedSet(SortedSetValue::default())
            }) else {
                unreachable!()
            };
            for (score, member) in entries {
                if journaling {
                    updates.push((member.clone(), score.0));
                }
                value.insert(member, score);
            }
            if !updates.is_empty() {
                self.record(now, || Mutation::SortedSetAdd {
                    key: destination.clone(),
                    entries: updates,
                });
            }
        }
        Resp::Integer(count as i64)
    }
    pub(super) fn zlexcount(&mut self, args: &[Bytes], now: i64) -> Resp {
        let bounds = match (LexBound::parse(&args[2]), LexBound::parse(&args[3])) {
            (Ok(a), Ok(b)) => lex_bounds(a, b),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        match self.sorted_set(&args[1], now) {
            Ok(value) => Resp::Integer(
                value
                    .and_then(|v| bounds.map(|b| v.lex.range(b).count()))
                    .unwrap_or(0) as i64,
            ),
            Err(e) => e,
        }
    }
    pub(super) fn zremlex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let bounds = match (LexBound::parse(&args[2]), LexBound::parse(&args[3])) {
            (Ok(a), Ok(b)) => lex_bounds(a, b),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        let members = match self.sorted_set(&args[1], now) {
            Ok(Some(v)) => bounds
                .map(|b| v.lex.range(b).cloned().collect::<Vec<_>>())
                .unwrap_or_default(),
            Ok(None) => Vec::new(),
            Err(e) => return e,
        };
        let count = members.len();
        if let Err(e) = ReplyBudget::new(self.reply_limit).response(&Resp::Integer(count as i64)) {
            return e;
        }
        self.zremove_selected(&args[1], members, now);
        Resp::Integer(count as i64)
    }
    pub(super) fn zalgebra(&mut self, args: &[Bytes], now: i64) -> Resp {
        let command = args[0].as_ref();
        let store = command.eq_ignore_ascii_case(b"ZDIFFSTORE")
            || command.eq_ignore_ascii_case(b"ZINTERSTORE")
            || command.eq_ignore_ascii_case(b"ZUNIONSTORE");
        let difference =
            command.eq_ignore_ascii_case(b"ZDIFF") || command.eq_ignore_ascii_case(b"ZDIFFSTORE");
        let union =
            command.eq_ignore_ascii_case(b"ZUNION") || command.eq_ignore_ascii_case(b"ZUNIONSTORE");
        let cardinality = command.eq_ignore_ascii_case(b"ZINTERCARD");
        let position = if store { 2 } else { 1 };
        let count = match key_count(args, position) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let keys = &args[position + 1..position + 1 + count];
        let mut weights = vec![1.0; count];
        let mut aggregate = Aggregate::Sum;
        let mut with_scores = false;
        let mut limit = usize::MAX;
        let mut cursor = position + count + 1;
        while cursor < args.len() {
            let option = &args[cursor];
            if !difference
                && !cardinality
                && option.eq_ignore_ascii_case(b"WEIGHTS")
                && args.len() - cursor > count
            {
                for (target, raw) in weights
                    .iter_mut()
                    .zip(&args[cursor + 1..cursor + 1 + count])
                {
                    let Some(v) = parse_score(raw) else {
                        return score_error();
                    };
                    *target = v.0;
                }
                cursor += count;
            } else if !difference
                && !cardinality
                && option.eq_ignore_ascii_case(b"AGGREGATE")
                && cursor + 1 < args.len()
            {
                aggregate = match args[cursor + 1].as_ref() {
                    v if v.eq_ignore_ascii_case(b"SUM") => Aggregate::Sum,
                    v if v.eq_ignore_ascii_case(b"MIN") => Aggregate::Min,
                    v if v.eq_ignore_ascii_case(b"MAX") => Aggregate::Max,
                    v if v.eq_ignore_ascii_case(b"COUNT") => Aggregate::Count,
                    _ => return syntax_error(),
                };
                cursor += 1;
            } else if !store && !cardinality && option.eq_ignore_ascii_case(b"WITHSCORES") {
                with_scores = true;
            } else if cardinality
                && option.eq_ignore_ascii_case(b"LIMIT")
                && cursor + 1 < args.len()
            {
                match integer(&args[cursor + 1]) {
                    Some(v) if v >= 0 => limit = if v == 0 { usize::MAX } else { v as usize },
                    _ => return error("ERR LIMIT can't be negative"),
                };
                cursor += 1;
            } else {
                return syntax_error();
            }
            cursor += 1;
        }
        for key in keys {
            self.discard_expired(key, now);
            match self.entries.get(key).map(|e| &e.value) {
                None | Some(Value::SortedSet(_)) => {}
                Some(Value::Set(_)) if !difference => {}
                _ => return wrong_type(),
            }
        }
        let mut inputs: Vec<_> = keys
            .iter()
            .zip(weights)
            .enumerate()
            .map(|(index, (key, weight))| {
                let input = match self.entries.get(key).map(|e| &e.value) {
                    Some(Value::SortedSet(v)) => Input::Sorted(v),
                    Some(Value::Set(v)) => Input::Set(v),
                    _ => Input::Empty,
                };
                (input, weight, index)
            })
            .collect();
        let mut result: HashMap<Bytes, f64> = HashMap::new();
        if union {
            for (input, weight, _) in &inputs {
                input.visit(|member, score| {
                    let next = weighted(score, *weight, aggregate);
                    result
                        .entry(member.clone())
                        .and_modify(|old| *old = aggregate_score(*old, next, aggregate))
                        .or_insert(next);
                });
            }
        } else if difference {
            inputs[0].0.visit(|member, score| {
                if inputs[1..]
                    .iter()
                    .all(|(input, _, _)| input.score(member).is_none())
                {
                    result.insert(member.clone(), score);
                }
            });
        } else {
            // Redis accumulates from the smallest source, retaining source order
            // for equal cardinalities. Keep each weight attached to its source.
            inputs.sort_unstable_by_key(|(input, _, index)| (input.len(), *index));
            let mut found = 0usize;
            inputs[0].0.visit_until(|member, initial| {
                let mut score = weighted(initial, inputs[0].1, aggregate);
                for (input, weight, _) in &inputs[1..] {
                    let Some(next) = input.score(member) else {
                        return true;
                    };
                    // Only the initial infinity-times-zero product is normalized.
                    // Later NaNs reset SUM but do not replace MIN/MAX's accumulator.
                    let next = if matches!(aggregate, Aggregate::Count) {
                        *weight
                    } else {
                        next * *weight
                    };
                    score = aggregate_score(score, next, aggregate);
                }
                found += 1;
                if !cardinality {
                    result.insert(member.clone(), score);
                }
                found < limit
            });
            if cardinality {
                return Resp::Integer(found as i64);
            }
        }
        let mut entries: Vec<_> = result
            .into_iter()
            .map(|(member, score)| (Score::new(score).expect("normalized aggregate"), member))
            .collect();
        entries.sort_unstable();
        if store {
            return self.zreplace(&args[1], entries, now);
        }
        range_reply(entries.iter(), with_scores, self.reply_limit)
    }
    pub(super) fn zrandmember(&mut self, args: &[Bytes], now: i64) -> Resp {
        let count = match args.get(2) {
            Some(raw) => match integer(raw) {
                Some(v) => Some(v),
                None => return integer_error(),
            },
            None => None,
        };
        let with_scores = args.len() == 4;
        if with_scores && !args[3].eq_ignore_ascii_case(b"WITHSCORES") {
            return syntax_error();
        }
        let limit = self.reply_limit;
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(v)) => v,
            Ok(None) => {
                return if count.is_none() {
                    Resp::Bulk(None)
                } else if with_scores {
                    Resp::Scored(Vec::new())
                } else {
                    array(Vec::new())
                };
            }
            Err(e) => return e,
        };
        let mut rng = rand::rng();
        let Some(count) = count else {
            return Resp::Bulk(value.members.keys().choose(&mut rng).cloned());
        };
        let requested = match usize::try_from(count.unsigned_abs()) {
            Ok(v) => v,
            Err(_) => return reply_limit_error(),
        };
        let requested = if count < 0 {
            requested
        } else {
            requested.min(value.members.len())
        };
        // Bound work before allocating or drawing a potentially enormous repeated sample.
        if requested > limit / std::mem::size_of::<Resp>() {
            return reply_limit_error();
        }
        let selected = if count < 0 {
            let mut result = Vec::new();
            // Draw random ranks, then walk the source once instead of copying it.
            let mut ranks: Vec<_> = (0..requested)
                .map(|position| (rng.random_range(0..value.members.len()), position))
                .collect();
            ranks.sort_unstable();
            let mut iter = value.members.iter().enumerate();
            let mut current = iter.next();
            for (rank, position) in ranks {
                while current.as_ref().is_some_and(|(i, _)| *i < rank) {
                    current = iter.next();
                }
                let (_, (member, score)) = current.as_ref().unwrap();
                result.push((position, (**score, (*member).clone())));
            }
            result.sort_unstable_by_key(|(position, _)| *position);
            result
                .into_iter()
                .map(|(_, entry)| entry)
                .collect::<Vec<_>>()
        } else {
            value
                .members
                .iter()
                .choose_multiple(&mut rng, requested)
                .into_iter()
                .map(|(m, s)| (*s, m.clone()))
                .collect()
        };
        range_reply(selected.iter(), with_scores, limit)
    }
    pub(crate) fn try_sorted_pop_at(
        &mut self,
        keys: &[Bytes],
        max: bool,
        count: usize,
        multi: bool,
        now: i64,
    ) -> Result<Option<Resp>, Resp> {
        if count == 0 {
            return Err(error("ERR count should be greater than 0"));
        }
        let limit = self.reply_limit;
        for key in keys {
            let value = match self.sorted_set(key, now) {
                Ok(Some(value)) if !value.members.is_empty() => value,
                Ok(_) => continue,
                Err(reply) => return Err(reply),
            };
            let reply = if max {
                range_reply(value.ordered.iter().rev().take(count), true, limit)
            } else {
                range_reply(value.ordered.iter().take(count), true, limit)
            };
            let Resp::Scored(entries) = reply else {
                return Err(reply);
            };
            let members = entries.iter().map(|(member, _)| member.clone()).collect();
            // Multi-pop is nested in both protocols, unlike RESP2 Scored.
            let reply = if multi {
                array(vec![
                    Resp::Bulk(Some(key.clone())),
                    array(
                        entries
                            .into_iter()
                            .map(|(member, score)| {
                                array(vec![Resp::Bulk(Some(member)), Resp::Double(score)])
                            })
                            .collect(),
                    ),
                ])
            } else {
                let (member, score) = entries.into_iter().next().unwrap();
                array(vec![
                    Resp::Bulk(Some(key.clone())),
                    Resp::Bulk(Some(member)),
                    Resp::Double(score),
                ])
            };
            ReplyBudget::new(limit).response(&reply)?;
            self.zremove_selected(key, members, now);
            return Ok(Some(reply));
        }
        Ok(None)
    }

    pub(super) fn zmultipop(&mut self, args: &[Bytes], now: i64) -> Execution {
        let blocking = args[0].eq_ignore_ascii_case(b"BZMPOP");
        let parsed = (|| {
            let timeout = if blocking {
                blocking_timeout(&args[1], now)?
            } else {
                None
            };
            let position = if blocking { 2 } else { 1 };
            let key_count = key_count(args, position)?;
            let end = position + key_count + 1;
            let max = match args.get(end) {
                Some(value) if value.eq_ignore_ascii_case(b"MIN") => false,
                Some(value) if value.eq_ignore_ascii_case(b"MAX") => true,
                _ => return Err(syntax_error()),
            };
            let count = if args.len() == end + 1 {
                1
            } else if args.len() == end + 3 && args[end + 1].eq_ignore_ascii_case(b"COUNT") {
                match integer(&args[end + 2]) {
                    Some(value) if value > 0 => value as usize,
                    _ => return Err(error("ERR count should be greater than 0")),
                }
            } else {
                return Err(syntax_error());
            };
            Ok((timeout, position + 1, end, max, count))
        })();
        let (timeout, start, end, max, count) = match parsed {
            Ok(parsed) => parsed,
            Err(reply) => return Execution::Reply(reply),
        };
        let keys = &args[start..end];
        match self.try_sorted_pop_at(keys, max, count, true, now) {
            Ok(Some(reply)) => Execution::Reply(reply),
            Ok(None) if blocking => Execution::Block(BlockingPop {
                keys: keys.iter().map(|key| Bytes::copy_from_slice(key)).collect(),
                right: max,
                timeout,
                destination: None,
                kind: BlockingKind::SortedPop { count, multi: true },
            }),
            Ok(None) => Execution::Reply(Resp::Array(None)),
            Err(reply) => Execution::Reply(reply),
        }
    }

    pub(super) fn zblocking_pop(&mut self, args: &[Bytes], now: i64) -> Execution {
        let timeout = match blocking_timeout(args.last().unwrap(), now) {
            Ok(timeout) => timeout,
            Err(reply) => return Execution::Reply(reply),
        };
        let keys = &args[1..args.len() - 1];
        let max = args[0].eq_ignore_ascii_case(b"BZPOPMAX");
        match self.try_sorted_pop_at(keys, max, 1, false, now) {
            Ok(Some(reply)) => Execution::Reply(reply),
            Ok(None) => Execution::Block(BlockingPop {
                keys: keys.iter().map(|key| Bytes::copy_from_slice(key)).collect(),
                right: max,
                timeout,
                destination: None,
                kind: BlockingKind::SortedPop {
                    count: 1,
                    multi: false,
                },
            }),
            Err(reply) => Execution::Reply(reply),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }

    #[test]
    fn intersection_normalizes_only_the_initial_product_in_cardinality_order() {
        for finite_score in ["5", "-5"] {
            for infinite_score in ["inf", "-inf"] {
                for infinite_is_smallest in [false, true] {
                    for reverse in [false, true] {
                        let mut engine = Engine::new();
                        run(&mut engine, &["ZADD", "finite", finite_score, "a"]);
                        run(&mut engine, &["ZADD", "infinite", infinite_score, "a"]);
                        let larger = if infinite_is_smallest {
                            "finite"
                        } else {
                            "infinite"
                        };
                        run(&mut engine, &["ZADD", larger, "1", "filler"]);
                        let (first, second, weights) = if reverse {
                            ("infinite", "finite", ["0", "1"])
                        } else {
                            ("finite", "infinite", ["1", "0"])
                        };
                        let finite: f64 = finite_score.parse().unwrap();
                        for (aggregate, expected) in [
                            ("SUM", if infinite_is_smallest { finite } else { 0.0 }),
                            (
                                "MIN",
                                if infinite_is_smallest {
                                    finite.min(0.0)
                                } else {
                                    finite
                                },
                            ),
                            (
                                "MAX",
                                if infinite_is_smallest {
                                    finite.max(0.0)
                                } else {
                                    finite
                                },
                            ),
                            ("COUNT", 1.0),
                        ] {
                            assert_eq!(
                                run(
                                    &mut engine,
                                    &[
                                        "ZINTER",
                                        "2",
                                        first,
                                        second,
                                        "WEIGHTS",
                                        weights[0],
                                        weights[1],
                                        "AGGREGATE",
                                        aggregate,
                                        "WITHSCORES",
                                    ],
                                ),
                                Resp::Scored(vec![(Bytes::from_static(b"a"), expected)]),
                                "{finite_score} {infinite_score} {infinite_is_smallest} {reverse} {aggregate}",
                            );
                            assert_eq!(
                                run(
                                    &mut engine,
                                    &[
                                        "ZINTERSTORE",
                                        "stored",
                                        "2",
                                        first,
                                        second,
                                        "WEIGHTS",
                                        weights[0],
                                        weights[1],
                                        "AGGREGATE",
                                        aggregate,
                                    ],
                                ),
                                Resp::Integer(1),
                            );
                            assert_eq!(
                                run(&mut engine, &["ZSCORE", "stored", "a"]),
                                Resp::Double(expected),
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn intersection_orders_all_contributions_by_cardinality() {
        let mut engine = Engine::new();
        run(&mut engine, &["ZADD", "small", "5", "a"]);
        run(&mut engine, &["ZADD", "medium", "inf", "a", "1", "filler"]);
        run(
            &mut engine,
            &["ZADD", "large", "7", "a", "1", "filler", "2", "other"],
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "ZINTER",
                    "3",
                    "small",
                    "large",
                    "medium",
                    "WEIGHTS",
                    "1",
                    "1",
                    "0",
                    "WITHSCORES",
                ],
            ),
            Resp::Scored(vec![(Bytes::from_static(b"a"), 7.0)]),
        );
    }

    #[test]
    fn intersection_preserves_equal_cardinality_source_order_and_duplicate_sources() {
        let mut engine = Engine::new();
        run(&mut engine, &["ZADD", "finite", "5", "a"]);
        run(&mut engine, &["ZADD", "infinite", "inf", "a"]);
        for (first, second, weights, expected) in [
            ("finite", "infinite", ["1", "0"], 0.0),
            ("infinite", "finite", ["0", "1"], 5.0),
        ] {
            assert_eq!(
                run(
                    &mut engine,
                    &[
                        "ZINTER",
                        "2",
                        first,
                        second,
                        "WEIGHTS",
                        weights[0],
                        weights[1],
                        "WITHSCORES",
                    ],
                ),
                Resp::Scored(vec![(Bytes::from_static(b"a"), expected)]),
            );
        }
        assert_eq!(
            run(
                &mut engine,
                &[
                    "ZINTERSTORE",
                    "infinite",
                    "2",
                    "infinite",
                    "infinite",
                    "WEIGHTS",
                    "1",
                    "0"
                ],
            ),
            Resp::Integer(1),
        );
        assert_eq!(
            run(&mut engine, &["ZSCORE", "infinite", "a"]),
            Resp::Double(0.0),
        );
    }
}
