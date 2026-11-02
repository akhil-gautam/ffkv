//! Durable, protocol-independent native jobs. Effects contain logical bytes, never blob locations.
//! ACK/cancel discard the job; finite idempotency records outlive it. Dead jobs retain bodies
//! because explicit redrive is supported. A receipt is a capability scoped to one queue epoch.
use crate::storage::{Payload, Store};
use bytes::Bytes;
use rand::RngCore;
use std::{
    cmp::Reverse,
    collections::{BTreeMap, BTreeSet},
    fmt, io,
    ops::Bound::{Excluded, Unbounded},
    sync::Arc,
};

pub const MAX_PAYLOAD: usize = 8 * 1024 * 1024;
pub const MAX_EFFECT: usize = 16 * 1024 * 1024 - 1;
pub const MAX_PAGE: usize = 1000;
pub const DEFAULT_VISIBILITY_MS: u64 = 30_000;
const MAX_TIME: u64 = 86_400_000;
/// Aggregate logical body-byte limit for one CLAIM, checked before reads or lease grants.
pub const MAX_CLAIM_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;
const MAX_RETENTION: u64 = 30 * MAX_TIME;
const MAX_JOBS: usize = 1_000_000;
const MAX_IDEMPOTENCY: usize = 1_000_000;
const MAINTENANCE_BUDGET: usize = 1024;
const MAX_ERROR: usize = 4096;
const MAX_KEY: usize = 1024;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub concurrency: u32,
    pub max_attempts: u32,
    pub backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub idempotency_ms: u64,
}
impl Default for Policy {
    fn default() -> Self {
        Self {
            concurrency: 0,
            max_attempts: 25,
            backoff_ms: 1000,
            max_backoff_ms: 60_000,
            idempotency_ms: 300_000,
        }
    }
}
impl Policy {
    fn valid(&self) -> bool {
        self.concurrency <= MAX_JOBS as u32
            && (1..=1_000_000).contains(&self.max_attempts)
            && self.backoff_ms <= self.max_backoff_ms
            && self.max_backoff_ms <= MAX_TIME
            && (1..=MAX_RETENTION).contains(&self.idempotency_ms)
    }
}
#[derive(Clone, Debug, Default)]
pub struct PolicyUpdate {
    pub concurrency: Option<u32>,
    pub max_attempts: Option<u32>,
    pub backoff_ms: Option<u64>,
    pub max_backoff_ms: Option<u64>,
    pub idempotency_ms: Option<u64>,
}
impl PolicyUpdate {
    fn resolve(&self, current: &Policy) -> Policy {
        Policy {
            concurrency: self.concurrency.unwrap_or(current.concurrency),
            max_attempts: self.max_attempts.unwrap_or(current.max_attempts),
            backoff_ms: self.backoff_ms.unwrap_or(current.backoff_ms),
            max_backoff_ms: self.max_backoff_ms.unwrap_or(current.max_backoff_ms),
            idempotency_ms: self.idempotency_ms.unwrap_or(current.idempotency_ms),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JobState {
    Ready,
    Scheduled,
    Running,
    Dead,
}
#[derive(Clone, Debug)]
pub enum Operation {
    Enqueue {
        payload: Bytes,
        idempotency: Option<Bytes>,
        at_ms: Option<i64>,
        priority: i32,
        max_attempts: Option<u32>,
    },
    Claim {
        count: usize,
        visibility_ms: u64,
    },
    Ack {
        receipt: Bytes,
    },
    Nack {
        receipt: Bytes,
        delay_ms: Option<u64>,
        error: Option<Bytes>,
    },
    Extend {
        receipt: Bytes,
        visibility_ms: u64,
    },
    Retry {
        job_id: Bytes,
        delay_ms: u64,
    },
    Cancel {
        job_id: Bytes,
    },
    Config(PolicyUpdate),
    Stats,
    Jobs {
        cursor: Option<Bytes>,
        count: usize,
        state: Option<JobState>,
    },
    Get {
        job_id: Bytes,
    },
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum QueueError {
    InvalidOptions,
    NotFound,
    NotDead,
    StaleReceipt,
    Capacity,
    ReplyTooLarge,
    Exhausted,
    MaintenanceRequired,
}
impl fmt::Display for QueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InvalidOptions => "invalid native queue options",
            Self::NotFound => "job not found",
            Self::NotDead => "job is not dead",
            Self::StaleReceipt => "stale or expired receipt",
            Self::Capacity => "native queue capacity exceeded",
            Self::ReplyTooLarge => "response budget exceeded; use a smaller range/count",
            Self::Exhausted => "native queue counter exhausted",
            Self::MaintenanceRequired => "native queue due maintenance required; retry command",
        })
    }
}
impl std::error::Error for QueueError {}
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Counters {
    pub enqueued: u64,
    pub claimed: u64,
    pub acked: u64,
    pub failed: u64,
    pub retried: u64,
    pub expired_leases: u64,
    pub canceled: u64,
    pub queue_wait_ms: u64,
    pub queue_wait_count: u64,
    pub run_ms: u64,
    pub run_count: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stats {
    pub totals: Counters,
    pub ready: usize,
    pub scheduled: usize,
    pub running: usize,
    pub dead: usize,
    pub jobs: usize,
    pub idempotency_records: usize,
    pub oldest_eligible_age_ms: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobInfo {
    pub job_id: Bytes,
    pub state: JobState,
    pub priority: i32,
    pub sequence: u64,
    pub enqueued_at_ms: i64,
    pub eligible_at_ms: i64,
    pub attempts: u32,
    pub max_attempts: u32,
    pub generation: u64,
    pub deadline_ms: Option<i64>,
    pub error: Option<Bytes>,
    pub payload_bytes: usize,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobDetails {
    pub info: JobInfo,
    pub payload: Bytes,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClaimedJob {
    pub job_id: Bytes,
    pub receipt: Bytes,
    pub payload: Bytes,
    pub attempts: u32,
    pub deadline_ms: i64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobPage {
    pub cursor: Option<Bytes>,
    pub jobs: Vec<JobInfo>,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Output {
    Enqueued { job_id: Bytes, created: bool },
    Claimed(Vec<ClaimedJob>),
    Ok,
    Extended { deadline_ms: i64 },
    Config(Policy),
    Stats(Stats),
    Jobs(JobPage),
    Job(Option<JobDetails>),
}
#[derive(Debug)]
pub struct Outcome {
    pub result: Result<Output, QueueError>,
    pub effects: Vec<Effect>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Meta {
    id: Bytes,
    sequence: u64,
    priority: i32,
    enqueued: i64,
    eligible: i64,
    attempts: u32,
    max_attempts: u32,
    generation: u64,
    state: JobState,
    deadline: Option<i64>,
    started: Option<i64>,
    nonce: Option<[u8; 16]>,
    error: Option<Bytes>,
}
#[derive(Debug)]
struct Job {
    meta: Meta,
    payload: Payload,
}
#[derive(Clone, Debug, PartialEq, Eq)]
struct Idempotency {
    id: Bytes,
    expires: i64,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reason {
    Claim,
    Ack,
    Nack,
    Expire,
    Retry,
    Cancel,
    Promote,
    Extend,
}
/// Opaque resolved transition. The codec validates structure; `apply` also validates state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Effect(Event);
#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Init {
        epoch: [u8; 16],
        policy: Policy,
        sequence: u64,
        counters: Counters,
        snapshot: bool,
    },
    Configure(Policy),
    Enqueue {
        meta: Meta,
        payload: Bytes,
        idempotency: Option<(Bytes, Idempotency)>,
    },
    Transition {
        id: Bytes,
        generation: u64,
        at: i64,
        reason: Reason,
        next: Option<Meta>,
    },
    Forget {
        key: Bytes,
        record: Idempotency,
    },
    RestoreJob {
        meta: Meta,
        payload: Bytes,
    },
    RestoreIdempotency {
        key: Bytes,
        record: Idempotency,
    },
    Seal,
}

#[derive(Debug)]
pub struct Queue {
    epoch: [u8; 16],
    initialized: bool,
    restoring: bool,
    policy: Policy,
    sequence: u64,
    counters: Counters,
    jobs: BTreeMap<Bytes, Job>,
    ready: BTreeSet<(Reverse<i32>, u64, Bytes)>,
    ready_age: BTreeSet<(i64, u64, Bytes)>,
    scheduled: BTreeSet<(i64, Bytes)>,
    leases: BTreeSet<(i64, Bytes)>,
    dead: BTreeSet<Bytes>,
    idempotency: BTreeMap<Bytes, Idempotency>,
    expirations: BTreeSet<(i64, Bytes)>,
    sequences: BTreeSet<u64>,
}
impl Default for Queue {
    fn default() -> Self {
        Self::new()
    }
}
fn random_token() -> [u8; 16] {
    let mut token = [0; 16];
    rand::rng().fill_bytes(&mut token);
    token
}
fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn exhausted() -> io::Error {
    io::Error::other(QueueError::Exhausted)
}
fn add(value: &mut u64, amount: u64) -> io::Result<()> {
    *value = value.checked_add(amount).ok_or_else(exhausted)?;
    Ok(())
}
fn elapsed(now: i64, then: i64) -> u64 {
    now.saturating_sub(then).max(0) as u64
}
fn deadline(now: i64, delay: u64) -> Result<i64, QueueError> {
    now.checked_add(i64::try_from(delay).map_err(|_| QueueError::InvalidOptions)?)
        .ok_or(QueueError::Exhausted)
}
fn hex(bytes: &[u8]) -> Bytes {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(DIGITS[(b >> 4) as usize]);
        out.push(DIGITS[(b & 15) as usize]);
    }
    Bytes::from(out)
}
fn valid_id(id: &[u8]) -> bool {
    id.len() == 32
        && id
            .iter()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(b))
}
impl Queue {
    /// Estimated separately allocated resident bytes, excluding the Queue
    /// header, shared Store/cache, and cold disk payload bodies.
    pub(crate) fn memory_footprint(&self, samples: usize) -> usize {
        fn tree<T>(len: usize) -> usize {
            if len == 0 {
                return 0;
            }
            (len.saturating_add(5) / 6).saturating_mul(
                11usize
                    .saturating_mul(std::mem::size_of::<T>())
                    .saturating_add(14 * std::mem::size_of::<usize>()),
            )
        }
        fn sample<I: ExactSizeIterator>(
            values: I,
            samples: usize,
            mut size: impl FnMut(I::Item) -> usize,
        ) -> usize {
            let len = values.len();
            let count = if samples == 0 { len } else { samples.min(len) };
            if count == 0 {
                return 0;
            }
            let total = values
                .take(count)
                .fold(0usize, |sum, value| sum.saturating_add(size(value)));
            (total / count)
                .saturating_mul(len)
                .saturating_add((total % count).saturating_mul(len) / count)
        }
        let mut seen = std::collections::HashSet::new();
        tree::<(Bytes, Job)>(self.jobs.len())
            .saturating_add(tree::<(Reverse<i32>, u64, Bytes)>(self.ready.len()))
            .saturating_add(tree::<(i64, u64, Bytes)>(self.ready_age.len()))
            .saturating_add(tree::<(i64, Bytes)>(self.scheduled.len()))
            .saturating_add(tree::<(i64, Bytes)>(self.leases.len()))
            .saturating_add(tree::<Bytes>(self.dead.len()))
            .saturating_add(tree::<(Bytes, Idempotency)>(self.idempotency.len()))
            .saturating_add(tree::<(i64, Bytes)>(self.expirations.len()))
            .saturating_add(tree::<u64>(self.sequences.len()))
            .saturating_add(sample(self.jobs.values(), samples, |job| {
                let identity = job.payload.memory_identity();
                let payload = if seen.len() < 262_144 {
                    if seen.insert(identity) {
                        job.payload.memory_footprint()
                    } else {
                        0
                    }
                } else if seen.contains(&identity) {
                    0
                } else {
                    job.payload.memory_footprint()
                };
                job.meta
                    .id
                    .len()
                    .saturating_add(job.meta.error.as_ref().map_or(0, Bytes::len))
                    .saturating_add(payload)
            }))
            .saturating_add(sample(self.idempotency.iter(), samples, |(key, record)| {
                // record.id shares an existing job's identifier when that job
                // exists; retained idempotency records can outlive the job.
                key.len()
                    .saturating_add(if self.jobs.contains_key(&record.id) {
                        0
                    } else {
                        record.id.len()
                    })
            }))
    }

    pub fn new() -> Self {
        Self {
            epoch: random_token(),
            initialized: false,
            restoring: false,
            policy: Policy::default(),
            sequence: 0,
            counters: Counters::default(),
            jobs: BTreeMap::new(),
            ready: BTreeSet::new(),
            ready_age: BTreeSet::new(),
            scheduled: BTreeSet::new(),
            leases: BTreeSet::new(),
            dead: BTreeSet::new(),
            idempotency: BTreeMap::new(),
            expirations: BTreeSet::new(),
            sequences: BTreeSet::new(),
        }
    }
    #[cfg(test)]
    pub fn policy(&self) -> &Policy {
        &self.policy
    }
    pub fn finish_recovery(&self) -> io::Result<()> {
        if self.restoring || !self.initialized {
            Err(invalid("unfinished queue recovery"))
        } else {
            Ok(())
        }
    }
    pub fn stats(&self, now_ms: i64) -> Stats {
        Stats {
            totals: self.counters.clone(),
            ready: self.ready.len(),
            scheduled: self.scheduled.len(),
            running: self.leases.len(),
            dead: self.dead.len(),
            jobs: self.jobs.len(),
            idempotency_records: self.idempotency.len(),
            oldest_eligible_age_ms: self
                .ready_age
                .first()
                .map_or(0, |(at, _, _)| elapsed(now_ms, *at)),
        }
    }
    fn init_effect(&self, snapshot: bool) -> Effect {
        Effect(Event::Init {
            epoch: self.epoch,
            policy: self.policy.clone(),
            sequence: self.sequence,
            counters: self.counters.clone(),
            snapshot,
        })
    }
    fn emit(
        &mut self,
        effect: Effect,
        store: Option<&Arc<Store>>,
        effects: &mut Vec<Effect>,
    ) -> io::Result<()> {
        self.apply(effect.clone(), store)?;
        effects.push(effect);
        Ok(())
    }
    /// Execute an explicit operation. Source-owned replicas/imports pass false
    /// for `advance_time`: elapsed deadlines remain pending source effects.
    pub fn execute(
        &mut self,
        operation: Operation,
        now_ms: i64,
        advance_time: bool,
        store: Option<&Arc<Store>>,
        reply_limit: usize,
    ) -> io::Result<Outcome> {
        if self.restoring {
            return Err(invalid("unfinished queue snapshot"));
        }
        if let Err(error) = self.validate(&operation, now_ms) {
            return Ok(Outcome {
                result: Err(error),
                effects: Vec::new(),
            });
        }
        let mut effects = Vec::new();
        if !self.initialized {
            self.emit(self.init_effect(false), store, &mut effects)?;
        }
        // Held replicas/imports consume only resolved source transitions. Their
        // local clock may inform inspection, but cannot originate queue effects.
        if advance_time {
            effects.extend(self.advance(now_ms, MAINTENANCE_BUDGET)?);
        }
        // Resolve the referenced lease even when unrelated due work exhausted the budget.
        let target = match &operation {
            Operation::Ack { receipt }
            | Operation::Nack { receipt, .. }
            | Operation::Extend { receipt, .. }
                if receipt.len() == 115 =>
            {
                self.jobs
                    .get(&receipt[33..65])
                    .map(|job| job.meta.id.clone())
            }
            Operation::Cancel { job_id } => Some(job_id.clone()),
            _ => None,
        };
        if advance_time
            && let Some(id) = target
            && let Some(at) = self
                .jobs
                .get(&id)
                .filter(|j| j.meta.state == JobState::Running)
                .and_then(|j| j.meta.deadline)
                .filter(|at| *at <= now_ms)
        {
            let next = self.failed_next(
                &self.jobs[&id].meta,
                at,
                None,
                Some(Bytes::from_static(b"visibility timeout")),
            )?;
            self.emit(
                self.transition(id, at, Reason::Expire, Some(next)),
                store,
                &mut effects,
            )?;
        }
        let result = match self.perform(operation, now_ms, store, &mut effects, reply_limit) {
            Err(error)
                if error.get_ref().and_then(|e| e.downcast_ref::<QueueError>())
                    == Some(&QueueError::Exhausted) =>
            {
                Err(QueueError::Exhausted)
            }
            result => result?,
        };
        Ok(Outcome { result, effects })
    }
    fn validate(&self, operation: &Operation, now: i64) -> Result<(), QueueError> {
        let valid = match operation {
            Operation::Enqueue {
                payload,
                idempotency,
                at_ms,
                max_attempts,
                ..
            } => {
                payload.len() <= MAX_PAYLOAD
                    && idempotency.as_ref().is_none_or(|k| k.len() <= MAX_KEY)
                    && at_ms.is_none_or(|at| at >= 0)
                    && max_attempts.is_none_or(|n| (1..=1_000_000).contains(&n))
            }
            Operation::Claim {
                count,
                visibility_ms,
            } => (1..=MAX_PAGE).contains(count) && (1..=MAX_TIME).contains(visibility_ms),
            Operation::Extend { visibility_ms, .. } => (1..=MAX_TIME).contains(visibility_ms),
            Operation::Nack {
                delay_ms, error, ..
            } => {
                delay_ms.is_none_or(|n| n <= MAX_TIME)
                    && error.as_ref().is_none_or(|e| e.len() <= MAX_ERROR)
            }
            Operation::Retry { delay_ms, .. } => *delay_ms <= MAX_TIME,
            Operation::Config(update) => update.resolve(&self.policy).valid(),
            Operation::Jobs { count, cursor, .. } => {
                (1..=MAX_PAGE).contains(count) && cursor.as_ref().is_none_or(|id| valid_id(id))
            }
            _ => true,
        };
        if !valid || now < 0 {
            return Err(QueueError::InvalidOptions);
        }
        match operation {
            Operation::Claim { visibility_ms, .. } | Operation::Extend { visibility_ms, .. } => {
                deadline(now, *visibility_ms)?;
            }
            Operation::Retry { delay_ms, .. } => {
                deadline(now, *delay_ms)?;
            }
            Operation::Nack {
                delay_ms: Some(delay),
                ..
            } => {
                deadline(now, *delay)?;
            }
            Operation::Enqueue {
                idempotency: Some(_),
                ..
            } => {
                deadline(now, self.policy.idempotency_ms)?;
            }
            _ => {}
        }
        Ok(())
    }
    fn receipt(&self, meta: &Meta) -> Bytes {
        let mut value = Vec::with_capacity(116);
        value.extend_from_slice(&hex(&self.epoch));
        value.push(b':');
        value.extend_from_slice(&meta.id);
        value.push(b':');
        value.extend_from_slice(&hex(&meta.generation.to_be_bytes()));
        value.push(b':');
        value.extend_from_slice(&hex(&meta.nonce.expect("running nonce")));
        Bytes::from(value)
    }
    fn receipt_job(&self, receipt: &[u8], now: i64) -> Result<Bytes, QueueError> {
        if receipt.len() != 115 || receipt[32] != b':' || receipt[65] != b':' || receipt[82] != b':'
        {
            return Err(QueueError::StaleReceipt);
        }
        let Some(job) = self.jobs.get(&receipt[33..65]) else {
            return Err(QueueError::StaleReceipt);
        };
        if job.meta.state != JobState::Running
            || job.meta.started.is_none_or(|at| now < at)
            || job.meta.deadline.is_none_or(|at| now >= at)
        {
            return Err(QueueError::StaleReceipt);
        }
        // Compare the complete fixed-size capability, without a prefix-dependent early exit.
        let expected = self.receipt(&job.meta);
        let mismatch = expected
            .iter()
            .zip(receipt)
            .fold(0u8, |acc, (a, b)| acc | (a ^ b));
        if mismatch != 0 {
            return Err(QueueError::StaleReceipt);
        }
        Ok(job.meta.id.clone())
    }
    fn transition(&self, id: Bytes, at: i64, reason: Reason, next: Option<Meta>) -> Effect {
        Effect(Event::Transition {
            generation: self.jobs[&id].meta.generation,
            id,
            at,
            reason,
            next,
        })
    }
    fn failed_next(
        &self,
        old: &Meta,
        at: i64,
        delay: Option<u64>,
        error: Option<Bytes>,
    ) -> io::Result<Meta> {
        let mut next = old.clone();
        next.deadline = None;
        next.started = None;
        next.nonce = None;
        next.error = error;
        if old.attempts >= old.max_attempts {
            next.state = JobState::Dead;
        } else {
            let factor = 1u64
                .checked_shl(old.attempts.saturating_sub(1))
                .unwrap_or(u64::MAX);
            let backoff = self
                .policy
                .backoff_ms
                .saturating_mul(factor)
                .min(self.policy.max_backoff_ms);
            next.eligible = at
                .checked_add(delay.unwrap_or(backoff) as i64)
                .ok_or_else(exhausted)?;
            next.state = if next.eligible <= at {
                JobState::Ready
            } else {
                JobState::Scheduled
            };
        }
        Ok(next)
    }
    fn perform(
        &mut self,
        operation: Operation,
        now: i64,
        store: Option<&Arc<Store>>,
        effects: &mut Vec<Effect>,
        reply_limit: usize,
    ) -> io::Result<Result<Output, QueueError>> {
        macro_rules! domain {
            ($e:expr) => {
                match $e {
                    Ok(value) => value,
                    Err(error) => return Ok(Err(error)),
                }
            };
        }
        let output = match operation {
            Operation::Enqueue {
                payload,
                idempotency,
                at_ms,
                priority,
                max_attempts,
            } => {
                if let Some(key) = &idempotency {
                    if let Some(record) = self.idempotency.get(key).filter(|r| now < r.expires) {
                        return Ok(Ok(Output::Enqueued {
                            job_id: record.id.clone(),
                            created: false,
                        }));
                    }
                    if let Some(record) = self.idempotency.get(key).cloned() {
                        self.emit(
                            Effect(Event::Forget {
                                key: Bytes::copy_from_slice(key),
                                record,
                            }),
                            store,
                            effects,
                        )?;
                    }
                }
                if self.jobs.len() >= MAX_JOBS
                    || (idempotency.is_some() && self.idempotency.len() >= MAX_IDEMPOTENCY)
                {
                    return Ok(Err(QueueError::Capacity));
                }
                let Some(sequence) = self.sequence.checked_add(1) else {
                    return Ok(Err(QueueError::Exhausted));
                };
                // Monotone prefix prevents ID reuse without historical tombstones.
                let mut raw_id = random_token();
                raw_id[..8].copy_from_slice(&sequence.to_be_bytes());
                let id = hex(&raw_id);
                let eligible = at_ms.unwrap_or(now).max(now);
                let meta = Meta {
                    id: id.clone(),
                    sequence,
                    priority,
                    enqueued: now,
                    eligible,
                    attempts: 0,
                    max_attempts: max_attempts.unwrap_or(self.policy.max_attempts),
                    generation: 0,
                    state: if eligible > now {
                        JobState::Scheduled
                    } else {
                        JobState::Ready
                    },
                    deadline: None,
                    started: None,
                    nonce: None,
                    error: None,
                };
                let idem = idempotency.map(|key| {
                    (
                        Bytes::copy_from_slice(&key),
                        Idempotency {
                            id: id.clone(),
                            expires: now + self.policy.idempotency_ms as i64,
                        },
                    )
                });
                self.emit(
                    Effect(Event::Enqueue {
                        meta,
                        payload,
                        idempotency: idem,
                    }),
                    store,
                    effects,
                )?;
                Output::Enqueued {
                    job_id: id,
                    created: true,
                }
            }
            Operation::Claim {
                count,
                visibility_ms,
            } => {
                if self.next_deadline().is_some_and(|at| at <= now) {
                    return Ok(Err(QueueError::MaintenanceRequired));
                }
                let slots = if self.policy.concurrency == 0 {
                    count
                } else {
                    count.min((self.policy.concurrency as usize).saturating_sub(self.leases.len()))
                };
                // Materialize all replies before granting any lease: disk failure is fatal, not a missing job.
                let mut budget = crate::engine::ReplyBudget::new(reply_limit);
                if budget.charge(1, 0).is_err() {
                    return Ok(Err(QueueError::ReplyTooLarge));
                }
                for (_, _, id) in self.ready.iter().take(slots) {
                    if budget
                        .charge(11, self.jobs[id].payload.len() + 186)
                        .is_err()
                    {
                        return Ok(Err(QueueError::ReplyTooLarge));
                    }
                }
                let selected: Vec<_> = self
                    .ready
                    .iter()
                    .take(slots)
                    .map(|(_, _, id)| id.clone())
                    .collect();
                let body_bytes = selected.iter().try_fold(0usize, |sum, id| {
                    sum.checked_add(self.jobs[id].payload.len())
                });
                if body_bytes.is_none_or(|bytes| bytes > MAX_CLAIM_PAYLOAD_BYTES) {
                    return Ok(Err(QueueError::ReplyTooLarge));
                }
                let mut prepared = Vec::with_capacity(selected.len());
                for id in selected {
                    let job = &self.jobs[&id];
                    if job.meta.generation == u64::MAX || job.meta.attempts == u32::MAX {
                        return Ok(Err(QueueError::Exhausted));
                    }
                    let mut next = job.meta.clone();
                    next.state = JobState::Running;
                    next.generation += 1;
                    next.attempts += 1;
                    next.deadline = Some(now + visibility_ms as i64);
                    next.started = Some(now);
                    next.nonce = Some(random_token());
                    prepared.push((next, job.payload.read()?));
                }
                // Preflight the aggregate counters so exhaustion never produces a partial grant.
                let mut projected = self.counters.clone();
                add(&mut projected.claimed, prepared.len() as u64)?;
                add(&mut projected.queue_wait_count, prepared.len() as u64)?;
                for (meta, _) in &prepared {
                    add(&mut projected.queue_wait_ms, elapsed(now, meta.eligible))?;
                }
                let mut claimed = Vec::with_capacity(prepared.len());
                for (meta, payload) in prepared {
                    let row = ClaimedJob {
                        job_id: meta.id.clone(),
                        receipt: self.receipt(&meta),
                        payload,
                        attempts: meta.attempts,
                        deadline_ms: meta.deadline.unwrap(),
                    };
                    self.emit(
                        self.transition(meta.id.clone(), now, Reason::Claim, Some(meta)),
                        store,
                        effects,
                    )?;
                    claimed.push(row);
                }
                Output::Claimed(claimed)
            }
            Operation::Ack { receipt } => {
                let id = domain!(self.receipt_job(&receipt, now));
                self.emit(self.transition(id, now, Reason::Ack, None), store, effects)?;
                Output::Ok
            }
            Operation::Nack {
                receipt,
                delay_ms,
                error,
            } => {
                let id = domain!(self.receipt_job(&receipt, now));
                let next = self.failed_next(
                    &self.jobs[&id].meta,
                    now,
                    delay_ms,
                    error.map(|b| Bytes::copy_from_slice(&b)),
                )?;
                self.emit(
                    self.transition(id, now, Reason::Nack, Some(next)),
                    store,
                    effects,
                )?;
                Output::Ok
            }
            Operation::Extend {
                receipt,
                visibility_ms,
            } => {
                let id = domain!(self.receipt_job(&receipt, now));
                let mut next = self.jobs[&id].meta.clone();
                let at = now + visibility_ms as i64;
                next.deadline = Some(at);
                self.emit(
                    self.transition(id, now, Reason::Extend, Some(next)),
                    store,
                    effects,
                )?;
                Output::Extended { deadline_ms: at }
            }
            Operation::Retry { job_id, delay_ms } => {
                let Some(job) = self.jobs.get(&job_id) else {
                    return Ok(Err(QueueError::NotFound));
                };
                if job.meta.state != JobState::Dead {
                    return Ok(Err(QueueError::NotDead));
                }
                if now < job.meta.enqueued {
                    return Ok(Err(QueueError::InvalidOptions));
                }
                let mut next = job.meta.clone();
                next.attempts = 0;
                next.error = None;
                next.eligible = now + delay_ms as i64;
                next.state = if delay_ms == 0 {
                    JobState::Ready
                } else {
                    JobState::Scheduled
                };
                self.emit(
                    self.transition(job_id, now, Reason::Retry, Some(next)),
                    store,
                    effects,
                )?;
                Output::Ok
            }
            Operation::Cancel { job_id } => {
                if !self.jobs.contains_key(&job_id) {
                    return Ok(Err(QueueError::NotFound));
                }
                if self.jobs[&job_id].meta.enqueued > now {
                    return Ok(Err(QueueError::InvalidOptions));
                }
                self.emit(
                    self.transition(job_id, now, Reason::Cancel, None),
                    store,
                    effects,
                )?;
                Output::Ok
            }
            Operation::Config(update) => {
                let policy = update.resolve(&self.policy);
                if policy != self.policy {
                    self.emit(Effect(Event::Configure(policy.clone())), store, effects)?;
                }
                Output::Config(policy)
            }
            Operation::Stats => Output::Stats(self.stats(now)),
            Operation::Get { job_id } => Output::Job(match self.jobs.get(&job_id) {
                Some(job) => {
                    let mut budget = crate::engine::ReplyBudget::new(reply_limit);
                    if budget
                        .charge(
                            27,
                            job.payload.len() + job.meta.error.as_ref().map_or(0, Bytes::len) + 288,
                        )
                        .is_err()
                    {
                        return Ok(Err(QueueError::ReplyTooLarge));
                    }
                    Some(JobDetails {
                        info: job.info(),
                        payload: job.payload.read()?,
                    })
                }
                None => None,
            }),
            Operation::Jobs {
                cursor,
                count,
                state,
            } => {
                let mut budget = crate::engine::ReplyBudget::new(reply_limit);
                if budget.charge(5, 64).is_err() {
                    return Ok(Err(QueueError::ReplyTooLarge));
                }
                let lower = cursor.map_or(Unbounded, Excluded);
                let mut iter = self.jobs.range((lower, Unbounded));
                let mut rows = Vec::new();
                let mut last = None;
                for _ in 0..count {
                    let Some((id, job)) = iter.next() else {
                        break;
                    };
                    last = Some(id.clone());
                    if state.is_none_or(|s| s == job.meta.state) {
                        if budget
                            .charge(25, job.meta.error.as_ref().map_or(0, Bytes::len) + 288)
                            .is_err()
                        {
                            return Ok(Err(QueueError::ReplyTooLarge));
                        }
                        rows.push(job.info());
                    }
                }
                if iter.next().is_none() {
                    last = None;
                }
                Output::Jobs(JobPage {
                    cursor: last,
                    jobs: rows,
                })
            }
        };
        Ok(Ok(output))
    }
    pub fn next_deadline(&self) -> Option<i64> {
        [
            self.scheduled.first().map(|(t, _)| *t),
            self.leases.first().map(|(t, _)| *t),
            self.expirations.first().map(|(t, _)| *t),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    /// Processes at most `min(budget, 10000)` indexed due entries. No payload IO is needed.
    pub fn advance(&mut self, now_ms: i64, budget: usize) -> io::Result<Vec<Effect>> {
        if self.restoring {
            return Err(invalid("unfinished queue snapshot"));
        }
        let mut effects = Vec::new();
        if !self.initialized {
            return Ok(effects);
        }
        for _ in 0..budget.min(10_000) {
            let candidate = [
                self.scheduled.first().map(|(at, id)| (*at, 0, id.clone())),
                self.leases.first().map(|(at, id)| (*at, 1, id.clone())),
                self.expirations
                    .first()
                    .map(|(at, key)| (*at, 2, key.clone())),
            ]
            .into_iter()
            .flatten()
            .min();
            let Some((at, kind, id)) = candidate.filter(|(at, _, _)| *at <= now_ms) else {
                break;
            };
            let effect = match kind {
                0 => {
                    let mut next = self.jobs[&id].meta.clone();
                    next.state = JobState::Ready;
                    self.transition(id, at, Reason::Promote, Some(next))
                }
                1 => {
                    let next = self.failed_next(
                        &self.jobs[&id].meta,
                        at,
                        None,
                        Some(Bytes::from_static(b"visibility timeout")),
                    )?;
                    self.transition(id, at, Reason::Expire, Some(next))
                }
                _ => Effect(Event::Forget {
                    record: self.idempotency[&id].clone(),
                    key: id,
                }),
            };
            self.emit(effect, None, &mut effects)?;
        }
        Ok(effects)
    }
    fn index_insert(&mut self, meta: &Meta) {
        let id = meta.id.clone();
        match meta.state {
            JobState::Ready => {
                self.ready
                    .insert((Reverse(meta.priority), meta.sequence, id.clone()));
                self.ready_age.insert((meta.eligible, meta.sequence, id));
            }
            JobState::Scheduled => {
                self.scheduled.insert((meta.eligible, id));
            }
            JobState::Running => {
                self.leases.insert((meta.deadline.unwrap(), id));
            }
            JobState::Dead => {
                self.dead.insert(id);
            }
        }
    }
    fn index_remove(&mut self, meta: &Meta) {
        let id = meta.id.clone();
        match meta.state {
            JobState::Ready => {
                self.ready
                    .remove(&(Reverse(meta.priority), meta.sequence, id.clone()));
                self.ready_age.remove(&(meta.eligible, meta.sequence, id));
            }
            JobState::Scheduled => {
                self.scheduled.remove(&(meta.eligible, id));
            }
            JobState::Running => {
                self.leases.remove(&(meta.deadline.unwrap(), id));
            }
            JobState::Dead => {
                self.dead.remove(&id);
            }
        }
    }
    pub fn apply(&mut self, effect: Effect, store: Option<&Arc<Store>>) -> io::Result<()> {
        effect.validate()?;
        if let Event::Init {
            epoch,
            policy,
            sequence,
            counters,
            snapshot,
        } = effect.0
        {
            if self.initialized {
                return Err(invalid("duplicate queue initialization"));
            }
            if !snapshot
                && (sequence != 0 || counters != Counters::default() || policy != Policy::default())
            {
                return Err(invalid("invalid queue creation metadata"));
            }
            self.epoch = epoch;
            self.policy = policy;
            self.sequence = sequence;
            self.counters = counters;
            self.initialized = true;
            self.restoring = snapshot;
            return Ok(());
        }
        if !self.initialized {
            return Err(invalid("queue effect before initialization"));
        }
        match effect.0 {
            Event::RestoreJob { meta, payload } => {
                if !self.restoring || meta.sequence > self.sequence {
                    return Err(invalid("job outside queue snapshot"));
                }
                self.insert_job(meta, payload, store)?;
            }
            Event::RestoreIdempotency { key, record } => {
                if !self.restoring {
                    return Err(invalid("idempotency outside queue snapshot"));
                }
                self.insert_idempotency(key, record)?;
            }
            Event::Seal => {
                if !self.restoring {
                    return Err(invalid("unexpected snapshot seal"));
                }
                let remaining = self
                    .counters
                    .enqueued
                    .checked_sub(self.counters.acked)
                    .and_then(|n| n.checked_sub(self.counters.canceled));
                if remaining != Some(self.jobs.len() as u64)
                    || self.sequence != self.counters.enqueued
                    || self.counters.queue_wait_count != self.counters.claimed
                    || self.counters.run_count > self.counters.claimed
                    || self.counters.expired_leases > self.counters.failed
                {
                    return Err(invalid("inconsistent queue snapshot counters"));
                }
                self.restoring = false;
            }
            _ if self.restoring => return Err(invalid("mutation in unfinished queue snapshot")),
            Event::Configure(policy) => self.policy = policy,
            Event::Enqueue {
                meta,
                payload,
                idempotency,
            } => {
                if self.sequence.checked_add(1) != Some(meta.sequence)
                    || meta.attempts != 0
                    || meta.generation != 0
                    || !matches!(meta.state, JobState::Ready | JobState::Scheduled)
                    || meta.error.is_some()
                    || (meta.state == JobState::Ready) != (meta.eligible == meta.enqueued)
                {
                    return Err(invalid("invalid enqueue transition"));
                }
                if let Some((key, record)) = &idempotency
                    && (self.idempotency.contains_key(key)
                        || self.idempotency.len() >= MAX_IDEMPOTENCY
                        || record.id != meta.id
                        || record.expires <= meta.enqueued)
                {
                    return Err(invalid("invalid enqueue idempotency"));
                }
                let mut counters = self.counters.clone();
                add(&mut counters.enqueued, 1)?;
                let sequence = meta.sequence;
                self.insert_job(meta, payload, store)?;
                if let Some((key, record)) = idempotency {
                    self.insert_idempotency(key, record)?;
                }
                self.sequence = sequence;
                self.counters = counters;
            }
            Event::Transition {
                id,
                generation,
                at,
                reason,
                next,
            } => {
                let old = &self
                    .jobs
                    .get(&id)
                    .ok_or_else(|| invalid("transition missing job"))?
                    .meta;
                if old.generation != generation {
                    return Err(invalid("transition generation mismatch"));
                }
                validate_transition(old, next.as_ref(), at, reason)?;
                if reason == Reason::Claim
                    && self.policy.concurrency != 0
                    && self.leases.len() >= self.policy.concurrency as usize
                {
                    return Err(invalid("claim exceeds queue concurrency"));
                }
                let mut counters = self.counters.clone();
                match reason {
                    Reason::Claim => {
                        add(&mut counters.claimed, 1)?;
                        add(&mut counters.queue_wait_count, 1)?;
                        add(&mut counters.queue_wait_ms, elapsed(at, old.eligible))?;
                    }
                    Reason::Ack => add(&mut counters.acked, 1)?,
                    Reason::Nack | Reason::Expire => {
                        add(&mut counters.failed, 1)?;
                        if next.as_ref().unwrap().state != JobState::Dead {
                            add(&mut counters.retried, 1)?;
                        }
                        if reason == Reason::Expire {
                            add(&mut counters.expired_leases, 1)?;
                        }
                    }
                    Reason::Retry => add(&mut counters.retried, 1)?,
                    Reason::Cancel => add(&mut counters.canceled, 1)?,
                    _ => {}
                }
                if old.state == JobState::Running && !matches!(reason, Reason::Extend) {
                    add(&mut counters.run_count, 1)?;
                    add(&mut counters.run_ms, elapsed(at, old.started.unwrap()))?;
                }
                let old = old.clone();
                self.index_remove(&old);
                if let Some(meta) = next {
                    self.index_insert(&meta);
                    self.jobs.get_mut(&id).unwrap().meta = meta;
                } else {
                    self.jobs.remove(&id);
                    self.sequences.remove(&old.sequence);
                }
                self.counters = counters;
            }
            Event::Forget { key, record } => {
                if self.idempotency.get(&key) != Some(&record) {
                    return Err(invalid("idempotency expiry mismatch"));
                }
                self.idempotency.remove(&key);
                self.expirations.remove(&(record.expires, key));
            }
            Event::Init { .. } => unreachable!(),
        }
        Ok(())
    }
    fn insert_job(
        &mut self,
        meta: Meta,
        payload: Bytes,
        store: Option<&Arc<Store>>,
    ) -> io::Result<()> {
        if self.jobs.len() >= MAX_JOBS
            || self.jobs.contains_key(&meta.id)
            || self.sequences.contains(&meta.sequence)
        {
            return Err(invalid("duplicate or excessive queue job"));
        }
        let payload = match store {
            Some(store) => store.put(payload)?,
            None => Payload::Inline(payload),
        };
        self.index_insert(&meta);
        self.sequences.insert(meta.sequence);
        self.jobs.insert(meta.id.clone(), Job { meta, payload });
        Ok(())
    }
    fn insert_idempotency(&mut self, key: Bytes, record: Idempotency) -> io::Result<()> {
        if self.idempotency.len() >= MAX_IDEMPOTENCY || self.idempotency.contains_key(&key) {
            return Err(invalid("duplicate or excessive idempotency record"));
        }
        self.expirations.insert((record.expires, key.clone()));
        self.idempotency.insert(key, record);
        Ok(())
    }
    /// Stream one bounded logical record at a time; disk read errors remain fatal.
    pub fn snapshot_effects(&self) -> Snapshot<'_> {
        Snapshot {
            queue: self,
            phase: 0,
            jobs: self.jobs.values(),
            idempotency: self.idempotency.iter(),
        }
    }
}
impl Job {
    fn info(&self) -> JobInfo {
        let m = &self.meta;
        JobInfo {
            job_id: m.id.clone(),
            state: m.state,
            priority: m.priority,
            sequence: m.sequence,
            enqueued_at_ms: m.enqueued,
            eligible_at_ms: m.eligible,
            attempts: m.attempts,
            max_attempts: m.max_attempts,
            generation: m.generation,
            deadline_ms: m.deadline,
            error: m.error.clone(),
            payload_bytes: self.payload.len(),
        }
    }
}
pub struct Snapshot<'a> {
    queue: &'a Queue,
    phase: u8,
    jobs: std::collections::btree_map::Values<'a, Bytes, Job>,
    idempotency: std::collections::btree_map::Iter<'a, Bytes, Idempotency>,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Effect>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.queue.restoring {
            if self.phase == 0 {
                self.phase = 4;
                return Some(Err(invalid("snapshot of unfinished recovery")));
            }
            return None;
        }
        if self.phase == 0 {
            self.phase = 1;
            return Some(Ok(self.queue.init_effect(true)));
        }
        if self.phase == 1 {
            if let Some(job) = self.jobs.next() {
                return Some(job.payload.read().map(|payload| {
                    Effect(Event::RestoreJob {
                        meta: job.meta.clone(),
                        payload,
                    })
                }));
            }
            self.phase = 2;
        }
        if self.phase == 2 {
            if let Some((key, record)) = self.idempotency.next() {
                return Some(Ok(Effect(Event::RestoreIdempotency {
                    key: key.clone(),
                    record: record.clone(),
                })));
            }
            self.phase = 3;
            return Some(Ok(Effect(Event::Seal)));
        }
        None
    }
}

fn validate_meta(m: &Meta) -> io::Result<()> {
    if !valid_id(&m.id)
        || m.sequence == 0
        || m.enqueued < 0
        || m.eligible < m.enqueued
        || !(1..=1_000_000).contains(&m.max_attempts)
        || m.attempts > m.max_attempts
        || m.generation < m.attempts as u64
        || m.error.as_ref().is_some_and(|e| e.len() > MAX_ERROR)
    {
        return Err(invalid("invalid job metadata"));
    }
    match m.state {
        JobState::Running => {
            if m.attempts == 0
                || m.generation == 0
                || m.nonce.is_none()
                || m.started.is_none_or(|t| t < m.eligible)
                || m.deadline.is_none()
                || m.deadline <= m.started
            {
                return Err(invalid("invalid running metadata"));
            }
        }
        _ => {
            if m.nonce.is_some() || m.started.is_some() || m.deadline.is_some() {
                return Err(invalid("inactive job has lease"));
            }
            if m.state == JobState::Dead && m.attempts != m.max_attempts {
                return Err(invalid("premature dead job"));
            }
            if m.state != JobState::Dead && m.attempts >= m.max_attempts {
                return Err(invalid("exhausted eligible job"));
            }
        }
    }
    Ok(())
}
fn validate_transition(old: &Meta, next: Option<&Meta>, at: i64, reason: Reason) -> io::Result<()> {
    if at < old.enqueued {
        return Err(invalid("transition before enqueue"));
    }
    if let Some(n) = next
        && (n.id != old.id
            || n.sequence != old.sequence
            || n.priority != old.priority
            || n.enqueued != old.enqueued
            || n.max_attempts != old.max_attempts)
    {
        return Err(invalid("transition changed immutable job metadata"));
    }
    let live = old.state == JobState::Running
        && old.started.is_some_and(|t| at >= t)
        && old.deadline.is_some_and(|t| at < t);
    let valid = match reason {
        Reason::Ack => live && next.is_none(),
        Reason::Cancel => next.is_none() && (old.state != JobState::Running || live),
        Reason::Claim => next.is_some_and(|n| {
            old.state == JobState::Ready
                && at >= old.eligible
                && n.state == JobState::Running
                && old.generation.checked_add(1) == Some(n.generation)
                && old.attempts.checked_add(1) == Some(n.attempts)
                && n.started == Some(at)
                && n.deadline
                    .is_some_and(|d| d > at && d.saturating_sub(at) <= MAX_TIME as i64)
                && n.eligible == old.eligible
                && n.error == old.error
        }),
        Reason::Extend => next.is_some_and(|n| {
            let mut expected = old.clone();
            expected.deadline = n.deadline;
            live && *n == expected
                && n.deadline
                    .is_some_and(|d| d > at && d.saturating_sub(at) <= MAX_TIME as i64)
        }),
        Reason::Promote => next.is_some_and(|n| {
            let mut expected = old.clone();
            expected.state = JobState::Ready;
            old.state == JobState::Scheduled && at == old.eligible && *n == expected
        }),
        Reason::Retry => next.is_some_and(|n| {
            let mut expected = old.clone();
            expected.attempts = 0;
            expected.error = None;
            expected.eligible = n.eligible;
            expected.state = n.state;
            old.state == JobState::Dead
                && n.eligible >= at
                && n.eligible.saturating_sub(at) <= MAX_TIME as i64
                && n.state
                    == if n.eligible == at {
                        JobState::Ready
                    } else {
                        JobState::Scheduled
                    }
                && *n == expected
        }),
        Reason::Nack | Reason::Expire => next.is_some_and(|n| {
            let timing = if reason == Reason::Nack {
                live
            } else {
                old.state == JobState::Running && old.deadline == Some(at)
            };
            let mut expected = old.clone();
            expected.deadline = None;
            expected.started = None;
            expected.nonce = None;
            expected.error = n.error.clone();
            expected.state = n.state;
            expected.eligible = n.eligible;
            let target = if old.attempts >= old.max_attempts {
                n.state == JobState::Dead && n.eligible == old.eligible
            } else {
                n.eligible >= at
                    && n.eligible.saturating_sub(at) <= MAX_TIME as i64
                    && n.state
                        == if n.eligible == at {
                            JobState::Ready
                        } else {
                            JobState::Scheduled
                        }
            };
            timing && target && *n == expected
        }),
    };
    if !valid {
        return Err(invalid("impossible queue transition"));
    }
    Ok(())
}

// Versioned, explicit little-endian codec. Every variable allocation is length-bounded first.
struct Encoder(Vec<u8>);
impl Encoder {
    fn u8(&mut self, n: u8) {
        self.0.push(n);
    }
    fn u32(&mut self, n: u32) {
        self.0.extend_from_slice(&n.to_le_bytes());
    }
    fn u64(&mut self, n: u64) {
        self.0.extend_from_slice(&n.to_le_bytes());
    }
    fn i64(&mut self, n: i64) {
        self.0.extend_from_slice(&n.to_le_bytes());
    }
    fn bytes(&mut self, b: &[u8]) {
        self.u32(b.len() as u32);
        self.0.extend_from_slice(b);
    }
    fn optional_bytes(&mut self, b: Option<&Bytes>) {
        self.u8(u8::from(b.is_some()));
        if let Some(b) = b {
            self.bytes(b);
        }
    }
    fn optional_i64(&mut self, n: Option<i64>) {
        self.u8(u8::from(n.is_some()));
        if let Some(n) = n {
            self.i64(n);
        }
    }
    fn policy(&mut self, p: &Policy) {
        self.u32(p.concurrency);
        self.u32(p.max_attempts);
        self.u64(p.backoff_ms);
        self.u64(p.max_backoff_ms);
        self.u64(p.idempotency_ms);
    }
    fn counters(&mut self, c: &Counters) {
        for n in [
            c.enqueued,
            c.claimed,
            c.acked,
            c.failed,
            c.retried,
            c.expired_leases,
            c.canceled,
            c.queue_wait_ms,
            c.queue_wait_count,
            c.run_ms,
            c.run_count,
        ] {
            self.u64(n);
        }
    }
    fn record(&mut self, r: &Idempotency) {
        self.bytes(&r.id);
        self.i64(r.expires);
    }
    fn meta(&mut self, m: &Meta) {
        self.bytes(&m.id);
        self.u64(m.sequence);
        self.u32(m.priority as u32);
        self.i64(m.enqueued);
        self.i64(m.eligible);
        self.u32(m.attempts);
        self.u32(m.max_attempts);
        self.u64(m.generation);
        self.u8(match m.state {
            JobState::Ready => 0,
            JobState::Scheduled => 1,
            JobState::Running => 2,
            JobState::Dead => 3,
        });
        self.optional_i64(m.deadline);
        self.optional_i64(m.started);
        self.u8(u8::from(m.nonce.is_some()));
        if let Some(n) = m.nonce {
            self.0.extend_from_slice(&n);
        }
        self.optional_bytes(m.error.as_ref());
    }
}
struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Decoder<'a> {
    fn take(&mut self, n: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(n)
            .ok_or_else(|| invalid("effect length overflow"))?;
        let b = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("truncated queue effect"))?;
        self.position = end;
        Ok(b)
    }
    fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn flag(&mut self) -> io::Result<bool> {
        match self.u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("invalid queue effect boolean")),
        }
    }
    fn bytes(&mut self, max: usize) -> io::Result<Bytes> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(invalid("oversized queue effect field"));
        }
        Ok(Bytes::copy_from_slice(self.take(n)?))
    }
    fn optional_bytes(&mut self, max: usize) -> io::Result<Option<Bytes>> {
        if self.flag()? {
            Ok(Some(self.bytes(max)?))
        } else {
            Ok(None)
        }
    }
    fn optional_i64(&mut self) -> io::Result<Option<i64>> {
        if self.flag()? {
            Ok(Some(self.i64()?))
        } else {
            Ok(None)
        }
    }
    fn policy(&mut self) -> io::Result<Policy> {
        Ok(Policy {
            concurrency: self.u32()?,
            max_attempts: self.u32()?,
            backoff_ms: self.u64()?,
            max_backoff_ms: self.u64()?,
            idempotency_ms: self.u64()?,
        })
    }
    fn counters(&mut self) -> io::Result<Counters> {
        Ok(Counters {
            enqueued: self.u64()?,
            claimed: self.u64()?,
            acked: self.u64()?,
            failed: self.u64()?,
            retried: self.u64()?,
            expired_leases: self.u64()?,
            canceled: self.u64()?,
            queue_wait_ms: self.u64()?,
            queue_wait_count: self.u64()?,
            run_ms: self.u64()?,
            run_count: self.u64()?,
        })
    }
    fn record(&mut self) -> io::Result<Idempotency> {
        Ok(Idempotency {
            id: self.bytes(32)?,
            expires: self.i64()?,
        })
    }
    fn meta(&mut self) -> io::Result<Meta> {
        let id = self.bytes(32)?;
        let sequence = self.u64()?;
        let priority = self.u32()? as i32;
        let enqueued = self.i64()?;
        let eligible = self.i64()?;
        let attempts = self.u32()?;
        let max_attempts = self.u32()?;
        let generation = self.u64()?;
        let state = match self.u8()? {
            0 => JobState::Ready,
            1 => JobState::Scheduled,
            2 => JobState::Running,
            3 => JobState::Dead,
            _ => return Err(invalid("invalid job state")),
        };
        let deadline = self.optional_i64()?;
        let started = self.optional_i64()?;
        let nonce = if self.flag()? {
            Some(self.take(16)?.try_into().unwrap())
        } else {
            None
        };
        let error = self.optional_bytes(MAX_ERROR)?;
        Ok(Meta {
            id,
            sequence,
            priority,
            enqueued,
            eligible,
            attempts,
            max_attempts,
            generation,
            state,
            deadline,
            started,
            nonce,
            error,
        })
    }
}
impl Effect {
    fn validate(&self) -> io::Result<()> {
        match &self.0 {
            Event::Init { policy, .. } | Event::Configure(policy) => {
                if !policy.valid() {
                    return Err(invalid("invalid queue policy"));
                }
            }
            Event::Enqueue {
                meta,
                payload,
                idempotency,
            } => {
                validate_meta(meta)?;
                if payload.len() > MAX_PAYLOAD {
                    return Err(invalid("oversized job body"));
                }
                if let Some((key, record)) = idempotency {
                    validate_record(key, record)?;
                }
            }
            Event::RestoreJob { meta, payload } => {
                validate_meta(meta)?;
                if payload.len() > MAX_PAYLOAD {
                    return Err(invalid("oversized job body"));
                }
            }
            Event::Transition { id, next, at, .. } => {
                if !valid_id(id) || *at < 0 {
                    return Err(invalid("invalid transition identity"));
                }
                if let Some(meta) = next {
                    validate_meta(meta)?;
                }
            }
            Event::Forget { key, record } | Event::RestoreIdempotency { key, record } => {
                validate_record(key, record)?
            }
            Event::Seal => {}
        }
        Ok(())
    }
    pub fn encode(&self) -> io::Result<Bytes> {
        self.validate()?;
        let meta_len = |m: &Meta| {
            53 + m.id.len()
                + if m.deadline.is_some() { 8 } else { 0 }
                + if m.started.is_some() { 8 } else { 0 }
                + if m.nonce.is_some() { 16 } else { 0 }
                + m.error.as_ref().map_or(0, |b| 4 + b.len())
        };
        let size = 4 + match &self.0 {
            Event::Init { .. } => 16 + 32 + 8 + 88 + 1,
            Event::Configure(_) => 32,
            Event::Enqueue {
                meta,
                payload,
                idempotency,
            } => {
                meta_len(meta)
                    + 4
                    + payload.len()
                    + 1
                    + idempotency
                        .as_ref()
                        .map_or(0, |(k, r)| 4 + k.len() + 4 + r.id.len() + 8)
            }
            Event::Transition { id, next, .. } => {
                4 + id.len() + 8 + 8 + 1 + 1 + next.as_ref().map_or(0, meta_len)
            }
            Event::Forget { key, record } | Event::RestoreIdempotency { key, record } => {
                4 + key.len() + 4 + record.id.len() + 8
            }
            Event::RestoreJob { meta, payload } => meta_len(meta) + 4 + payload.len(),
            Event::Seal => 0,
        };
        if size > MAX_EFFECT {
            return Err(invalid("oversized queue effect"));
        }
        let mut e = Encoder(Vec::with_capacity(size));
        e.0.extend_from_slice(b"FQ\x01");
        match &self.0 {
            Event::Init {
                epoch,
                policy,
                sequence,
                counters,
                snapshot,
            } => {
                e.u8(0);
                e.0.extend_from_slice(epoch);
                e.policy(policy);
                e.u64(*sequence);
                e.counters(counters);
                e.u8(u8::from(*snapshot));
            }
            Event::Configure(p) => {
                e.u8(1);
                e.policy(p);
            }
            Event::Enqueue {
                meta,
                payload,
                idempotency,
            } => {
                e.u8(2);
                e.meta(meta);
                e.bytes(payload);
                e.u8(u8::from(idempotency.is_some()));
                if let Some((key, record)) = idempotency {
                    e.bytes(key);
                    e.record(record);
                }
            }
            Event::Transition {
                id,
                generation,
                at,
                reason,
                next,
            } => {
                e.u8(3);
                e.bytes(id);
                e.u64(*generation);
                e.i64(*at);
                e.u8(match reason {
                    Reason::Claim => 0,
                    Reason::Ack => 1,
                    Reason::Nack => 2,
                    Reason::Expire => 3,
                    Reason::Retry => 4,
                    Reason::Cancel => 5,
                    Reason::Promote => 6,
                    Reason::Extend => 7,
                });
                e.u8(u8::from(next.is_some()));
                if let Some(meta) = next {
                    e.meta(meta);
                }
            }
            Event::Forget { key, record } => {
                e.u8(4);
                e.bytes(key);
                e.record(record);
            }
            Event::RestoreJob { meta, payload } => {
                e.u8(5);
                e.meta(meta);
                e.bytes(payload);
            }
            Event::RestoreIdempotency { key, record } => {
                e.u8(6);
                e.bytes(key);
                e.record(record);
            }
            Event::Seal => e.u8(7),
        }
        if e.0.len() > MAX_EFFECT {
            return Err(invalid("oversized queue effect"));
        }
        Ok(Bytes::from(e.0))
    }
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() > MAX_EFFECT {
            return Err(invalid("oversized queue effect"));
        }
        let mut d = Decoder { bytes, position: 0 };
        if d.take(3)? != b"FQ\x01" {
            return Err(invalid("invalid queue effect version"));
        }
        let event = match d.u8()? {
            0 => Event::Init {
                epoch: d.take(16)?.try_into().unwrap(),
                policy: d.policy()?,
                sequence: d.u64()?,
                counters: d.counters()?,
                snapshot: d.flag()?,
            },
            1 => Event::Configure(d.policy()?),
            2 => {
                let meta = d.meta()?;
                let payload = d.bytes(MAX_PAYLOAD)?;
                let idempotency = if d.flag()? {
                    Some((d.bytes(MAX_KEY)?, d.record()?))
                } else {
                    None
                };
                Event::Enqueue {
                    meta,
                    payload,
                    idempotency,
                }
            }
            3 => {
                let id = d.bytes(32)?;
                let generation = d.u64()?;
                let at = d.i64()?;
                let reason = match d.u8()? {
                    0 => Reason::Claim,
                    1 => Reason::Ack,
                    2 => Reason::Nack,
                    3 => Reason::Expire,
                    4 => Reason::Retry,
                    5 => Reason::Cancel,
                    6 => Reason::Promote,
                    7 => Reason::Extend,
                    _ => return Err(invalid("invalid transition reason")),
                };
                let next = if d.flag()? { Some(d.meta()?) } else { None };
                Event::Transition {
                    id,
                    generation,
                    at,
                    reason,
                    next,
                }
            }
            4 => Event::Forget {
                key: d.bytes(MAX_KEY)?,
                record: d.record()?,
            },
            5 => Event::RestoreJob {
                meta: d.meta()?,
                payload: d.bytes(MAX_PAYLOAD)?,
            },
            6 => Event::RestoreIdempotency {
                key: d.bytes(MAX_KEY)?,
                record: d.record()?,
            },
            7 => Event::Seal,
            _ => return Err(invalid("invalid queue effect tag")),
        };
        if d.position != bytes.len() {
            return Err(invalid("trailing queue effect bytes"));
        }
        let effect = Effect(event);
        effect.validate()?;
        Ok(effect)
    }
}
fn validate_record(key: &Bytes, record: &Idempotency) -> io::Result<()> {
    if key.len() > MAX_KEY || !valid_id(&record.id) || record.expires < 0 {
        return Err(invalid("invalid idempotency record"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(q: &mut Queue, op: Operation, at: i64) -> Output {
        q.execute(op, at, true, None, usize::MAX)
            .unwrap()
            .result
            .unwrap()
    }
    fn enqueue(
        q: &mut Queue,
        at: i64,
        due: Option<i64>,
        priority: i32,
        max: Option<u32>,
        idem: Option<Bytes>,
    ) -> Bytes {
        match run(
            q,
            Operation::Enqueue {
                payload: Bytes::from_static(b"body"),
                idempotency: idem,
                at_ms: due,
                priority,
                max_attempts: max,
            },
            at,
        ) {
            Output::Enqueued {
                job_id,
                created: true,
            } => job_id,
            other => panic!("unexpected enqueue: {other:?}"),
        }
    }
    fn claim(q: &mut Queue, at: i64, count: usize, visibility_ms: u64) -> Vec<ClaimedJob> {
        match run(
            q,
            Operation::Claim {
                count,
                visibility_ms,
            },
            at,
        ) {
            Output::Claimed(rows) => rows,
            other => panic!("unexpected claim: {other:?}"),
        }
    }
    fn state(q: &mut Queue, id: &Bytes, at: i64) -> JobInfo {
        match run(q, Operation::Get { job_id: id.clone() }, at) {
            Output::Job(Some(job)) => job.info,
            other => panic!("missing job: {other:?}"),
        }
    }
    fn assert_error(q: &mut Queue, op: Operation, at: i64, error: QueueError) {
        assert_eq!(
            q.execute(op, at, true, None, usize::MAX).unwrap().result,
            Err(error)
        );
    }
    fn snapshots(q: &Queue) -> Vec<Bytes> {
        q.snapshot_effects()
            .map(|effect| effect.unwrap().encode().unwrap())
            .collect()
    }

    #[test]
    fn priority_fifo_and_exact_scheduled_boundary() {
        let mut q = Queue::new();
        let first = enqueue(&mut q, 0, Some(100), 4, None, None);
        let second = enqueue(&mut q, 1, None, 4, None, None);
        let high = enqueue(&mut q, 2, None, i32::MAX, None, None);
        let low = enqueue(&mut q, 3, None, i32::MIN, None, None);
        assert_eq!(q.next_deadline(), Some(100));
        assert!(q.advance(99, 10).unwrap().is_empty());
        let rows = claim(&mut q, 100, 4, 100);
        assert_eq!(
            rows.iter().map(|j| j.job_id.clone()).collect::<Vec<_>>(),
            vec![high, first, second, low]
        );
        assert_eq!(q.stats(100).totals.queue_wait_ms, 98 + 99 + 97);
        assert_eq!(q.stats(100).totals.queue_wait_count, 4);
    }

    #[test]
    fn bounded_maintenance_never_returns_false_empty_or_wrong_priority() {
        let mut q = Queue::new();
        let mut highest = Bytes::new();
        for n in 0..=MAINTENANCE_BUDGET {
            highest = enqueue(&mut q, 0, Some(10), n as i32, None, None);
        }
        let outcome = q
            .execute(
                Operation::Claim {
                    count: 1,
                    visibility_ms: 10,
                },
                10,
                true,
                None,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(outcome.result, Err(QueueError::MaintenanceRequired));
        assert_eq!(outcome.effects.len(), MAINTENANCE_BUDGET);
        assert_eq!(q.stats(10).running, 0);
        assert_eq!(claim(&mut q, 10, 1, 10)[0].job_id, highest);
    }

    #[test]
    fn nack_expiry_dead_redrive_and_generation_security() {
        let mut q = Queue::new();
        let id = enqueue(&mut q, 0, None, 0, Some(2), None);
        let one = claim(&mut q, 0, 1, 10).remove(0);
        run(
            &mut q,
            Operation::Nack {
                receipt: one.receipt.clone(),
                delay_ms: None,
                error: Some(Bytes::from_static(b"failed")),
            },
            5,
        );
        assert_eq!(state(&mut q, &id, 5).eligible_at_ms, 1005);
        assert!(claim(&mut q, 1004, 1, 10).is_empty());
        let two = claim(&mut q, 1005, 1, 10).remove(0);
        assert_eq!(two.attempts, 2);
        assert_error(
            &mut q,
            Operation::Ack {
                receipt: one.receipt.clone(),
            },
            1006,
            QueueError::StaleReceipt,
        );
        let expired = q
            .execute(
                Operation::Ack {
                    receipt: two.receipt.clone(),
                },
                1015,
                true,
                None,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(expired.result, Err(QueueError::StaleReceipt));
        assert!(!expired.effects.is_empty());
        assert_eq!(state(&mut q, &id, 1015).state, JobState::Dead);
        run(
            &mut q,
            Operation::Retry {
                job_id: id.clone(),
                delay_ms: 0,
            },
            1016,
        );
        assert_eq!(state(&mut q, &id, 1016).attempts, 0);
        assert_eq!(state(&mut q, &id, 1016).generation, 2);
        assert_eq!(state(&mut q, &id, 1016).error, None);
        let three = claim(&mut q, 1016, 1, 10).remove(0);
        assert_ne!(three.receipt, two.receipt);
        assert_eq!(state(&mut q, &id, 1016).generation, 3);
        assert_error(
            &mut q,
            Operation::Ack {
                receipt: two.receipt,
            },
            1017,
            QueueError::StaleReceipt,
        );
        run(
            &mut q,
            Operation::Ack {
                receipt: three.receipt.clone(),
            },
            1020,
        );
        assert_error(
            &mut q,
            Operation::Ack {
                receipt: three.receipt,
            },
            1021,
            QueueError::StaleReceipt,
        );
        let stats = q.stats(1021);
        assert_eq!(stats.jobs, 0);
        assert_eq!(stats.totals.claimed, 3);
        assert_eq!(stats.totals.failed, 2);
        assert_eq!(stats.totals.expired_leases, 1);
        assert_eq!(stats.totals.retried, 2);
        assert_eq!(stats.totals.run_ms, 19);
        assert_eq!(stats.totals.run_count, 3);
        assert_eq!(stats.totals.acked, 1);
    }

    #[test]
    fn visibility_extension_and_concurrency_slots() {
        let mut q = Queue::new();
        run(
            &mut q,
            Operation::Config(PolicyUpdate {
                concurrency: Some(1),
                backoff_ms: Some(0),
                ..PolicyUpdate::default()
            }),
            0,
        );
        let first = enqueue(&mut q, 0, None, 0, None, None);
        enqueue(&mut q, 0, None, 0, None, None);
        let row = claim(&mut q, 0, 2, 10).remove(0);
        assert!(claim(&mut q, 1, 2, 10).is_empty());
        assert_eq!(
            run(
                &mut q,
                Operation::Extend {
                    receipt: row.receipt.clone(),
                    visibility_ms: 20
                },
                5
            ),
            Output::Extended { deadline_ms: 25 }
        );
        assert_eq!(q.next_deadline(), Some(25));
        assert!(claim(&mut q, 10, 2, 10).is_empty());
        assert_error(
            &mut q,
            Operation::Extend {
                receipt: row.receipt,
                visibility_ms: 20,
            },
            25,
            QueueError::StaleReceipt,
        );
        let redelivery = claim(&mut q, 25, 2, 10);
        assert_eq!(redelivery.len(), 1);
        assert_eq!(redelivery[0].job_id, first);
        run(&mut q, Operation::Cancel { job_id: first }, 26);
        assert_eq!(claim(&mut q, 26, 2, 10).len(), 1);
        assert_eq!(q.stats(26).totals.canceled, 1);
    }

    #[test]
    fn idempotency_survives_ack_cancel_and_expires_exactly() {
        let mut q = Queue::new();
        run(
            &mut q,
            Operation::Config(PolicyUpdate {
                idempotency_ms: Some(100),
                ..PolicyUpdate::default()
            }),
            0,
        );
        let key = Bytes::from_static(b"\0key\xff");
        let original = enqueue(&mut q, 0, None, 0, None, Some(key.clone()));
        let row = claim(&mut q, 0, 1, 10).remove(0);
        run(
            &mut q,
            Operation::Ack {
                receipt: row.receipt,
            },
            1,
        );
        let op = || Operation::Enqueue {
            payload: Bytes::from_static(b"different"),
            idempotency: Some(key.clone()),
            at_ms: None,
            priority: 5,
            max_attempts: None,
        };
        assert_eq!(
            run(&mut q, op(), 99),
            Output::Enqueued {
                job_id: original.clone(),
                created: false
            }
        );
        assert_eq!(q.stats(99).jobs, 0);
        let replacement = match run(&mut q, op(), 100) {
            Output::Enqueued {
                job_id,
                created: true,
            } => job_id,
            other => panic!("{other:?}"),
        };
        assert_ne!(original, replacement);
        run(
            &mut q,
            Operation::Cancel {
                job_id: replacement.clone(),
            },
            101,
        );
        assert_eq!(
            run(&mut q, op(), 199),
            Output::Enqueued {
                job_id: replacement,
                created: false
            }
        );
        q.advance(200, 10).unwrap();
        assert_eq!(q.stats(200).idempotency_records, 0);
        assert_eq!(q.stats(200).jobs, 0);
        assert!(q.sequences.is_empty());
    }

    #[test]
    fn config_is_validated_before_user_mutation_and_existing_attempt_limit_is_fixed() {
        let mut q = Queue::new();
        let id = enqueue(&mut q, 0, None, 0, Some(1), None);
        assert_error(
            &mut q,
            Operation::Config(PolicyUpdate {
                backoff_ms: Some(100),
                max_backoff_ms: Some(1),
                ..PolicyUpdate::default()
            }),
            0,
            QueueError::InvalidOptions,
        );
        assert_eq!(q.policy(), &Policy::default());
        run(
            &mut q,
            Operation::Config(PolicyUpdate {
                max_attempts: Some(7),
                ..PolicyUpdate::default()
            }),
            0,
        );
        let row = claim(&mut q, 0, 1, 10).remove(0);
        run(
            &mut q,
            Operation::Nack {
                receipt: row.receipt,
                delay_ms: Some(0),
                error: None,
            },
            1,
        );
        assert_eq!(state(&mut q, &id, 1).state, JobState::Dead);
        assert_eq!(state(&mut q, &id, 1).max_attempts, 1);
        assert_error(
            &mut q,
            Operation::Claim {
                count: 0,
                visibility_ms: 1,
            },
            1,
            QueueError::InvalidOptions,
        );
        assert_error(
            &mut q,
            Operation::Claim {
                count: 1001,
                visibility_ms: 1,
            },
            1,
            QueueError::InvalidOptions,
        );
        assert_error(
            &mut q,
            Operation::Claim {
                count: 1,
                visibility_ms: 0,
            },
            1,
            QueueError::InvalidOptions,
        );
        assert_error(
            &mut q,
            Operation::Claim {
                count: 1,
                visibility_ms: MAX_TIME + 1,
            },
            1,
            QueueError::InvalidOptions,
        );
    }

    #[test]
    fn exponential_backoff_saturates_without_overflow() {
        let mut q = Queue::new();
        let id = enqueue(&mut q, 0, None, 0, Some(1_000_000), None);
        let mut m = q.jobs[&id].meta.clone();
        m.attempts = 999_999;
        assert_eq!(q.failed_next(&m, 100, None, None).unwrap().eligible, 60_100);
        q.policy.backoff_ms = 0;
        assert_eq!(q.failed_next(&m, 100, None, None).unwrap().eligible, 100);
    }

    #[test]
    fn inspection_pages_are_bounded_even_when_filter_does_not_match() {
        let mut q = Queue::new();
        for _ in 0..5 {
            enqueue(&mut q, 0, None, 0, None, None);
        }
        let first = match run(
            &mut q,
            Operation::Jobs {
                cursor: None,
                count: 2,
                state: Some(JobState::Dead),
            },
            0,
        ) {
            Output::Jobs(page) => page,
            other => panic!("{other:?}"),
        };
        assert!(first.jobs.is_empty());
        assert!(first.cursor.is_some());
        let second = match run(
            &mut q,
            Operation::Jobs {
                cursor: first.cursor,
                count: 2,
                state: None,
            },
            0,
        ) {
            Output::Jobs(page) => page,
            other => panic!("{other:?}"),
        };
        assert_eq!(second.jobs.len(), 2);
        assert!(second.cursor.is_some());
        let last = match run(
            &mut q,
            Operation::Jobs {
                cursor: second.cursor,
                count: 2,
                state: None,
            },
            0,
        ) {
            Output::Jobs(page) => page,
            other => panic!("{other:?}"),
        };
        assert_eq!(last.jobs.len(), 1);
        assert_eq!(last.cursor, None);
    }

    #[test]
    fn incremental_codec_and_snapshot_preserve_exact_receipts_and_state() {
        let mut q = Queue::new();
        let mut replay = Queue::new();
        let operations = vec![
            Operation::Config(PolicyUpdate {
                idempotency_ms: Some(90),
                ..PolicyUpdate::default()
            }),
            Operation::Enqueue {
                payload: Bytes::from_static(b"\0body\xff"),
                idempotency: Some(Bytes::from_static(b"unique")),
                at_ms: None,
                priority: 10,
                max_attempts: Some(2),
            },
            Operation::Enqueue {
                payload: Bytes::from_static(b"later"),
                idempotency: None,
                at_ms: Some(70),
                priority: 20,
                max_attempts: None,
            },
            Operation::Claim {
                count: 1,
                visibility_ms: 50,
            },
        ];
        let mut receipt = Bytes::new();
        for op in operations {
            let outcome = q.execute(op, 0, true, None, usize::MAX).unwrap();
            if let Ok(Output::Claimed(rows)) = &outcome.result {
                receipt = rows[0].receipt.clone();
            }
            for effect in outcome.effects {
                let bytes = effect.encode().unwrap();
                assert!(bytes.len() < 16 * 1024 * 1024);
                let decoded = Effect::decode(&bytes).unwrap();
                assert_eq!(decoded, effect);
                replay.apply(decoded, None).unwrap();
            }
            assert_eq!(snapshots(&q), snapshots(&replay));
        }
        let mut restored = Queue::new();
        for bytes in snapshots(&q) {
            restored
                .apply(Effect::decode(&bytes).unwrap(), None)
                .unwrap();
        }
        assert_eq!(snapshots(&q), snapshots(&restored));
        assert_eq!(
            run(
                &mut q,
                Operation::Ack {
                    receipt: receipt.clone()
                },
                10
            ),
            Output::Ok
        );
        assert_eq!(
            run(&mut restored, Operation::Ack { receipt }, 10),
            Output::Ok
        );
        assert_eq!(snapshots(&q), snapshots(&restored));
    }

    #[test]
    fn codec_rejects_truncation_trailing_data_bad_tags_and_impossible_replay() {
        let mut q = Queue::new();
        let id = enqueue(&mut q, 0, None, 0, None, None);
        for effect in q.snapshot_effects() {
            let effect = effect.unwrap();
            let bytes = effect.encode().unwrap();
            for end in 0..bytes.len() {
                assert!(Effect::decode(&bytes[..end]).is_err());
            }
            let mut extra = bytes.to_vec();
            extra.push(0);
            assert!(Effect::decode(&extra).is_err());
        }
        assert!(Effect::decode(b"FQ\x01\xff").is_err());
        assert!(Effect::decode(b"FQ\x02\x07").is_err());
        let old = q.jobs[&id].meta.clone();
        let impossible = q.transition(id.clone(), 0, Reason::Ack, None);
        assert!(q.apply(impossible, None).is_err());
        assert_eq!(q.jobs[&id].meta, old);
        let mut malformed = old.clone();
        malformed.sequence = 0;
        assert!(
            Effect(Event::RestoreJob {
                meta: malformed,
                payload: Bytes::new()
            })
            .encode()
            .is_err()
        );
        let mut d = Decoder {
            bytes: &u32::MAX.to_le_bytes(),
            position: 0,
        };
        assert!(d.bytes(MAX_PAYLOAD).is_err());
    }

    #[test]
    fn forged_and_recreated_queue_receipts_cannot_ack_a_live_job() {
        let mut q = Queue::new();
        enqueue(&mut q, 0, None, 0, None, None);
        let row = claim(&mut q, 0, 1, 10).remove(0);
        let mut forged = row.receipt.to_vec();
        forged[114] ^= 1;
        assert_error(
            &mut q,
            Operation::Ack {
                receipt: Bytes::from(forged),
            },
            1,
            QueueError::StaleReceipt,
        );
        let mut recreated = Queue::new();
        // Simulate even identical job metadata/nonce under a newly-created epoch.
        recreated.apply(recreated.init_effect(false), None).unwrap();
        let meta = q.jobs[&row.job_id].meta.clone();
        recreated
            .insert_job(meta, Bytes::from_static(b"body"), None)
            .unwrap();
        assert_error(
            &mut recreated,
            Operation::Ack {
                receipt: row.receipt,
            },
            1,
            QueueError::StaleReceipt,
        );
        assert_eq!(recreated.stats(1).running, 1);
    }

    #[test]
    fn sequence_generation_and_counter_overflow_never_wrap() {
        let mut q = Queue::new();
        let id = enqueue(&mut q, 0, None, 0, None, None);
        q.jobs.get_mut(&id).unwrap().meta.generation = u64::MAX;
        assert_error(
            &mut q,
            Operation::Claim {
                count: 1,
                visibility_ms: 1,
            },
            0,
            QueueError::Exhausted,
        );
        q.sequence = u64::MAX;
        assert_error(
            &mut q,
            Operation::Enqueue {
                payload: Bytes::new(),
                idempotency: None,
                at_ms: None,
                priority: 0,
                max_attempts: None,
            },
            0,
            QueueError::Exhausted,
        );
        q.jobs.get_mut(&id).unwrap().meta.generation = 0;
        let row = claim(&mut q, 0, 1, 10).remove(0);
        q.counters.acked = u64::MAX;
        assert_error(
            &mut q,
            Operation::Ack {
                receipt: row.receipt,
            },
            1,
            QueueError::Exhausted,
        );
        assert_eq!(q.stats(1).running, 1);
        assert_eq!(q.counters.acked, u64::MAX);
    }

    #[test]
    fn claim_body_budget_is_checked_atomically_and_preserves_due_effects() {
        let mut q = Queue::new();
        let body = Bytes::from(vec![7; MAX_PAYLOAD]);
        for _ in 0..5 {
            run(
                &mut q,
                Operation::Enqueue {
                    payload: body.clone(),
                    idempotency: None,
                    at_ms: Some(10),
                    priority: 0,
                    max_attempts: None,
                },
                0,
            );
        }
        let outcome = q
            .execute(
                Operation::Claim {
                    count: 5,
                    visibility_ms: 10,
                },
                10,
                true,
                None,
                usize::MAX,
            )
            .unwrap();
        assert_eq!(outcome.result, Err(QueueError::ReplyTooLarge));
        assert_eq!(outcome.effects.len(), 5);
        assert_eq!(q.stats(10).running, 0);
        assert_eq!(q.stats(10).ready, 5);
        assert_eq!(claim(&mut q, 10, 4, 10).len(), 4);
    }

    #[test]
    fn origin_reply_budget_refuses_claim_before_reading_or_granting_any_lease() {
        let mut q = Queue::new();
        enqueue(&mut q, 0, None, 0, None, None);
        let outcome = q
            .execute(
                Operation::Claim {
                    count: 1,
                    visibility_ms: 10,
                },
                0,
                true,
                None,
                1,
            )
            .unwrap();
        assert_eq!(outcome.result, Err(QueueError::ReplyTooLarge));
        assert!(outcome.effects.is_empty());
        assert_eq!(q.stats(0).running, 0);
        assert_eq!(q.stats(0).ready, 1);
        assert_eq!(claim(&mut q, 0, 1, 10).len(), 1);
    }

    fn directory() -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "ffkv-native-queue-{}",
            String::from_utf8(hex(&random_token()).to_vec()).unwrap()
        ))
    }
    #[test]
    fn storage_budget_failure_is_fatal_and_does_not_create_a_job() {
        let path = directory();
        let store = Store::open(crate::storage::Config {
            directory: path.clone(),
            inline_threshold: 0,
            cache_bytes: 0,
            segment_bytes: 4096,
            disk_budget: 129,
        })
        .unwrap();
        let mut q = Queue::new();
        let result = q.execute(
            Operation::Enqueue {
                payload: Bytes::from_static(b"body"),
                idempotency: None,
                at_ms: None,
                priority: 0,
                max_attempts: None,
            },
            0,
            true,
            Some(&store),
            usize::MAX,
        );
        assert!(result.is_err());
        assert_eq!(q.stats(0).jobs, 0);
        assert_eq!(q.stats(0).totals.enqueued, 0);
        drop(q);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
    #[test]
    fn disk_corruption_propagates_from_get_claim_and_snapshot() {
        let path = directory();
        let store = Store::open(crate::storage::Config {
            directory: path.clone(),
            inline_threshold: 0,
            cache_bytes: 0,
            segment_bytes: 4096,
            disk_budget: 0,
        })
        .unwrap();
        let mut q = Queue::new();
        let id = match q
            .execute(
                Operation::Enqueue {
                    payload: Bytes::from_static(b"cold body"),
                    idempotency: None,
                    at_ms: None,
                    priority: 0,
                    max_attempts: None,
                },
                0,
                true,
                Some(&store),
                usize::MAX,
            )
            .unwrap()
            .result
            .unwrap()
        {
            Output::Enqueued { job_id, .. } => job_id,
            other => panic!("{other:?}"),
        };
        assert!(q.jobs[&id].payload.is_disk());
        for entry in std::fs::read_dir(&path).unwrap() {
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
        assert!(
            q.execute(
                Operation::Get { job_id: id },
                1,
                true,
                Some(&store),
                usize::MAX
            )
            .is_err()
        );
        assert!(
            q.execute(
                Operation::Claim {
                    count: 1,
                    visibility_ms: 10
                },
                1,
                true,
                Some(&store),
                usize::MAX,
            )
            .is_err()
        );
        assert_eq!(q.stats(1).running, 0);
        assert!(q.snapshot_effects().any(|effect| effect.is_err()));
        drop(q);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }
}
