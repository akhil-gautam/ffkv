//! Binary-safe FQ command parsing and semantic RESP replies.
use bytes::Bytes;

use super::{CommandSpec, Engine, Resp, array, bulk, error, integer, integer_error, syntax_error};
use crate::queue::{self, JobInfo, JobState, Operation, Output, PolicyUpdate};

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "fq.enqueue",
        -3,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::native_queue,
    ),
    CommandSpec::handler("fq.claim", -2, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.ack", 3, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.nack", -3, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.extend", 4, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.retry", -3, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.cancel", 3, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler("fq.config", -2, &["write"], (1, 1, 1), Engine::native_queue),
    CommandSpec::handler(
        "fq.stats",
        2,
        &["readonly"],
        (1, 1, 1),
        Engine::native_queue,
    ),
    CommandSpec::handler(
        "fq.jobs",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::native_queue,
    ),
    CommandSpec::handler("fq.get", 3, &["readonly"], (1, 1, 1), Engine::native_queue),
];

const DAY_MS: u64 = 86_400_000;

fn unsigned(value: &Bytes, maximum: u64) -> Result<u64, Resp> {
    integer(value)
        .and_then(|n| u64::try_from(n).ok())
        .filter(|n| *n <= maximum)
        .ok_or_else(integer_error)
}

fn positive(value: &Bytes, maximum: u64) -> Result<u64, Resp> {
    unsigned(value, maximum).and_then(|n| if n == 0 { Err(integer_error()) } else { Ok(n) })
}

// All options have one value; reject duplicates without allocating a set.
fn options<'a>(
    args: &'a [Bytes],
    names: &'a [&'a [u8]],
) -> Result<impl Iterator<Item = Result<(usize, &'a Bytes), Resp>> + 'a, Resp> {
    if !args.len().is_multiple_of(2) {
        return Err(syntax_error());
    }
    let mut seen = 0_u16;
    Ok(args.as_chunks::<2>().0.iter().map(move |pair| {
        let index = names
            .iter()
            .position(|name| pair[0].eq_ignore_ascii_case(name))
            .ok_or_else(syntax_error)?;
        let flag = 1 << index;
        if seen & flag != 0 {
            return Err(syntax_error());
        }
        seen |= flag;
        Ok((index, &pair[1]))
    }))
}

fn parse(args: &[Bytes], now: i64) -> Result<Operation, Resp> {
    let name = &args[0];
    if name.eq_ignore_ascii_case(b"FQ.ENQUEUE") {
        let mut operation = Operation::Enqueue {
            payload: args[2].clone(),
            idempotency: None,
            at_ms: None,
            priority: 0,
            max_attempts: None,
        };
        let Operation::Enqueue {
            payload,
            idempotency,
            at_ms,
            priority,
            max_attempts,
        } = &mut operation
        else {
            unreachable!()
        };
        if payload.len() > queue::MAX_PAYLOAD {
            return Err(error("ERR native queue payload too large"));
        }
        for option in options(
            &args[3..],
            &[b"IDEMPOTENCY", b"DELAY", b"AT", b"PRIORITY", b"MAXATTEMPTS"],
        )? {
            let (index, value) = option?;
            match index {
                0 => {
                    if value.len() > 1024 {
                        return Err(error("ERR native queue idempotency key too large"));
                    }
                    *idempotency = Some(value.clone());
                }
                1 | 2 => {
                    if at_ms.is_some() {
                        return Err(syntax_error());
                    }
                    let time = unsigned(value, i64::MAX as u64)? as i64;
                    *at_ms = Some(if index == 1 {
                        now.checked_add(time).ok_or_else(integer_error)?
                    } else {
                        time
                    });
                }
                3 => {
                    *priority = integer(value)
                        .and_then(|n| i32::try_from(n).ok())
                        .ok_or_else(integer_error)?
                }
                4 => *max_attempts = Some(positive(value, 1_000_000)? as u32),
                _ => unreachable!(),
            }
        }
        Ok(operation)
    } else if name.eq_ignore_ascii_case(b"FQ.CLAIM") {
        let mut count = 1;
        let mut visibility_ms = queue::DEFAULT_VISIBILITY_MS;
        for option in options(&args[2..], &[b"COUNT", b"VISIBILITY"])? {
            let (index, value) = option?;
            match index {
                0 => count = positive(value, queue::MAX_PAGE as u64)? as usize,
                1 => visibility_ms = positive(value, DAY_MS)?,
                _ => unreachable!(),
            }
        }
        now.checked_add(visibility_ms as i64)
            .ok_or_else(integer_error)?;
        Ok(Operation::Claim {
            count,
            visibility_ms,
        })
    } else if name.eq_ignore_ascii_case(b"FQ.ACK") {
        Ok(Operation::Ack {
            receipt: args[2].clone(),
        })
    } else if name.eq_ignore_ascii_case(b"FQ.NACK") {
        let mut delay_ms = None;
        let mut failure = None;
        for option in options(&args[3..], &[b"DELAY", b"ERROR"])? {
            let (index, value) = option?;
            match index {
                0 => {
                    let delay = unsigned(value, DAY_MS)?;
                    now.checked_add(delay as i64).ok_or_else(integer_error)?;
                    delay_ms = Some(delay);
                }
                1 => {
                    if value.len() > 4096 {
                        return Err(error("ERR native queue error too large"));
                    }
                    failure = Some(value.clone());
                }
                _ => unreachable!(),
            }
        }
        Ok(Operation::Nack {
            receipt: args[2].clone(),
            delay_ms,
            error: failure,
        })
    } else if name.eq_ignore_ascii_case(b"FQ.EXTEND") {
        let visibility_ms = positive(&args[3], DAY_MS)?;
        now.checked_add(visibility_ms as i64)
            .ok_or_else(integer_error)?;
        Ok(Operation::Extend {
            receipt: args[2].clone(),
            visibility_ms,
        })
    } else if name.eq_ignore_ascii_case(b"FQ.RETRY") {
        let mut delay_ms = 0;
        for option in options(&args[3..], &[b"DELAY"])? {
            let (_, value) = option?;
            delay_ms = unsigned(value, DAY_MS)?;
        }
        now.checked_add(delay_ms as i64).ok_or_else(integer_error)?;
        Ok(Operation::Retry {
            job_id: args[2].clone(),
            delay_ms,
        })
    } else if name.eq_ignore_ascii_case(b"FQ.CANCEL") {
        Ok(Operation::Cancel {
            job_id: args[2].clone(),
        })
    } else if name.eq_ignore_ascii_case(b"FQ.CONFIG") {
        let mut update = PolicyUpdate::default();
        for option in options(
            &args[2..],
            &[
                b"CONCURRENCY",
                b"MAXATTEMPTS",
                b"BACKOFF",
                b"MAXBACKOFF",
                b"IDEMPOTENCY",
            ],
        )? {
            let (index, value) = option?;
            match index {
                0 => update.concurrency = Some(unsigned(value, 1_000_000)? as u32),
                1 => update.max_attempts = Some(positive(value, 1_000_000)? as u32),
                2 => update.backoff_ms = Some(unsigned(value, DAY_MS)?),
                3 => update.max_backoff_ms = Some(unsigned(value, DAY_MS)?),
                4 => {
                    let retention = positive(value, 30 * DAY_MS)?;
                    now.checked_add(retention as i64)
                        .ok_or_else(integer_error)?;
                    update.idempotency_ms = Some(retention);
                }
                _ => unreachable!(),
            }
        }
        if let (Some(backoff), Some(maximum)) = (update.backoff_ms, update.max_backoff_ms)
            && backoff > maximum
        {
            return Err(error("ERR invalid native queue backoff"));
        }
        Ok(Operation::Config(update))
    } else if name.eq_ignore_ascii_case(b"FQ.STATS") {
        Ok(Operation::Stats)
    } else if name.eq_ignore_ascii_case(b"FQ.JOBS") {
        let cursor = if args[2].as_ref() == b"0" {
            None
        } else {
            if args[2].len() != 32
                || !args[2]
                    .iter()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
            {
                return Err(error("ERR invalid native queue cursor"));
            }
            Some(args[2].clone())
        };
        let mut count = 100;
        let mut state = None;
        for option in options(&args[3..], &[b"COUNT", b"STATE"])? {
            let (index, value) = option?;
            match index {
                0 => count = positive(value, queue::MAX_PAGE as u64)? as usize,
                1 => {
                    state = Some(if value.eq_ignore_ascii_case(b"ready") {
                        JobState::Ready
                    } else if value.eq_ignore_ascii_case(b"scheduled") {
                        JobState::Scheduled
                    } else if value.eq_ignore_ascii_case(b"running") {
                        JobState::Running
                    } else if value.eq_ignore_ascii_case(b"dead") {
                        JobState::Dead
                    } else {
                        return Err(syntax_error());
                    })
                }
                _ => unreachable!(),
            }
        }
        Ok(Operation::Jobs {
            cursor,
            count,
            state,
        })
    } else if name.eq_ignore_ascii_case(b"FQ.GET") {
        Ok(Operation::Get {
            job_id: args[2].clone(),
        })
    } else {
        Err(syntax_error())
    }
}

fn map<const N: usize>(fields: [(&'static str, Resp); N]) -> Resp {
    Resp::Map(
        fields
            .into_iter()
            .map(|(name, value)| (bulk(name), value))
            .collect(),
    )
}

// RESP integers are signed. Preserve exact large domain counters as decimal
// bulk strings rather than wrapping, saturating, or rounding through a double.
fn number(value: u64) -> Resp {
    match i64::try_from(value) {
        Ok(value) => Resp::Integer(value),
        Err(_) => Resp::Bulk(Some(Bytes::from(value.to_string()))),
    }
}

fn bytes(value: Bytes) -> Resp {
    Resp::Bulk(Some(value))
}

fn job_response(info: JobInfo, payload: Option<Bytes>) -> Resp {
    let state = match info.state {
        JobState::Ready => "ready",
        JobState::Scheduled => "scheduled",
        JobState::Running => "running",
        JobState::Dead => "dead",
    };
    let fields = [
        ("job_id", bytes(info.job_id)),
        ("state", bulk(state)),
        ("priority", Resp::Integer(i64::from(info.priority))),
        ("sequence", number(info.sequence)),
        ("enqueued_at_ms", Resp::Integer(info.enqueued_at_ms)),
        ("eligible_at_ms", Resp::Integer(info.eligible_at_ms)),
        ("attempts", number(u64::from(info.attempts))),
        ("max_attempts", number(u64::from(info.max_attempts))),
        ("generation", number(info.generation)),
        (
            "deadline_ms",
            info.deadline_ms.map_or(Resp::Bulk(None), Resp::Integer),
        ),
        ("error", Resp::Bulk(info.error)),
        ("payload_bytes", number(info.payload_bytes as u64)),
    ];
    let mut result = Vec::with_capacity(fields.len() + usize::from(payload.is_some()));
    result.extend(fields.into_iter().map(|(name, value)| (bulk(name), value)));
    if let Some(payload) = payload {
        result.push((bulk("payload"), bytes(payload)));
    }
    Resp::Map(result)
}

fn response(output: Output) -> Resp {
    match output {
        Output::Enqueued { job_id, created } => map([
            ("job_id", bytes(job_id)),
            ("created", Resp::Integer(i64::from(created))),
        ]),
        Output::Claimed(jobs) => array(
            jobs.into_iter()
                .map(|job| {
                    map([
                        ("job_id", bytes(job.job_id)),
                        ("receipt", bytes(job.receipt)),
                        ("payload", bytes(job.payload)),
                        ("attempts", number(u64::from(job.attempts))),
                        ("deadline_ms", Resp::Integer(job.deadline_ms)),
                    ])
                })
                .collect(),
        ),
        Output::Ok => Resp::Simple("OK"),
        Output::Extended { deadline_ms } => Resp::Integer(deadline_ms),
        Output::Config(policy) => map([
            ("concurrency", number(u64::from(policy.concurrency))),
            ("max_attempts", number(u64::from(policy.max_attempts))),
            ("backoff_ms", number(policy.backoff_ms)),
            ("max_backoff_ms", number(policy.max_backoff_ms)),
            ("idempotency_ms", number(policy.idempotency_ms)),
        ]),
        Output::Stats(stats) => {
            let totals = stats.totals;
            map([
                (
                    "totals",
                    map([
                        ("enqueued", number(totals.enqueued)),
                        ("claimed", number(totals.claimed)),
                        ("acked", number(totals.acked)),
                        ("failed", number(totals.failed)),
                        ("retried", number(totals.retried)),
                        ("expired_leases", number(totals.expired_leases)),
                        ("canceled", number(totals.canceled)),
                        ("queue_wait_ms", number(totals.queue_wait_ms)),
                        ("queue_wait_count", number(totals.queue_wait_count)),
                        ("run_ms", number(totals.run_ms)),
                        ("run_count", number(totals.run_count)),
                    ]),
                ),
                ("ready", number(stats.ready as u64)),
                ("scheduled", number(stats.scheduled as u64)),
                ("running", number(stats.running as u64)),
                ("dead", number(stats.dead as u64)),
                ("jobs", number(stats.jobs as u64)),
                (
                    "idempotency_records",
                    number(stats.idempotency_records as u64),
                ),
                (
                    "oldest_eligible_age_ms",
                    number(stats.oldest_eligible_age_ms),
                ),
            ])
        }
        Output::Jobs(page) => map([
            ("cursor", page.cursor.map_or_else(|| bulk("0"), bytes)),
            (
                "jobs",
                array(
                    page.jobs
                        .into_iter()
                        .map(|job| job_response(job, None))
                        .collect(),
                ),
            ),
        ]),
        Output::Job(None) => Resp::Bulk(None),
        Output::Job(Some(job)) => job_response(job.info, Some(job.payload)),
    }
}

impl Engine {
    fn native_queue(&mut self, args: &[Bytes], now: i64) -> Resp {
        let operation = match parse(args, now) {
            Ok(operation) => operation,
            Err(error) => return error,
        };
        match self.native_operation(&args[1], operation, now) {
            Ok(Ok(output)) => response(output),
            Ok(Err(error)) => error,
            Err(error) => self.fail_io(error),
        }
    }
}
