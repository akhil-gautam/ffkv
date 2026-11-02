//! Ordered streams, consumer groups, and resolved, incremental journal effects.
//! Auto-claim pages the PEL by ID, counting deleted references toward COUNT;
//! FIFO negative-acknowledgement priority is reserved for XREADGROUP CLAIM.
//! Valid claim attempts persist consumer seen-time even without delivery; CLAIM
//! reads expose idle/delivery metadata on both reclaimed and fresh entries.
use super::typed::{
    Data,
    codec::{Decoder, Encoder},
};
use super::{
    CommandSpec, Engine, Entry, Execution, NO_KEYS, ONE_KEY, Resp, Value, array, bounded_replies,
    error, integer, integer_error, syntax_error, wrong_type,
};
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    ops::Bound,
    time::Duration,
};

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler("xadd", -5, &["write", "denyoom"], ONE_KEY, Engine::cmd_xadd),
    CommandSpec::handler("xlen", 2, &["readonly", "fast"], ONE_KEY, Engine::cmd_xlen),
    CommandSpec::handler("xrange", -4, &["readonly"], ONE_KEY, Engine::cmd_xrange),
    CommandSpec::handler("xrevrange", -4, &["readonly"], ONE_KEY, Engine::cmd_xrange),
    CommandSpec::handler("xdel", -3, &["write"], ONE_KEY, Engine::cmd_xdel),
    CommandSpec::handler("xdelex", -5, &["write"], ONE_KEY, Engine::cmd_xdel),
    CommandSpec::handler("xackdel", -6, &["write"], ONE_KEY, Engine::cmd_xdel),
    CommandSpec::handler("xtrim", -4, &["write"], ONE_KEY, Engine::cmd_xtrim),
    CommandSpec::handler(
        "xread",
        -4,
        &["readonly", "blocking"],
        NO_KEYS,
        Engine::cmd_xread,
    ),
    CommandSpec::handler(
        "xreadgroup",
        -7,
        &["write", "blocking", "denyoom"],
        NO_KEYS,
        Engine::cmd_xread,
    ),
    CommandSpec::handler(
        "xgroup",
        -2,
        &["write", "denyoom"],
        NO_KEYS,
        Engine::cmd_xgroup,
    ),
    CommandSpec::handler("xack", -4, &["write", "fast"], ONE_KEY, Engine::cmd_xack),
    CommandSpec::handler("xpending", -3, &["readonly"], ONE_KEY, Engine::cmd_xpending),
    CommandSpec::handler("xclaim", -6, &["write"], ONE_KEY, Engine::cmd_xclaim),
    CommandSpec::handler(
        "xautoclaim",
        -6,
        &["write"],
        ONE_KEY,
        Engine::cmd_xautoclaim,
    ),
    CommandSpec::handler("xinfo", -2, &["readonly"], NO_KEYS, Engine::cmd_xinfo),
    CommandSpec::handler("xsetid", -3, &["write"], ONE_KEY, Engine::cmd_xsetid),
    CommandSpec::handler(
        "xcfgset",
        -2,
        &["write", "fast"],
        ONE_KEY,
        Engine::cmd_xcfgset,
    ),
    CommandSpec::handler("xnack", -7, &["write"], ONE_KEY, Engine::cmd_xnack),
    CommandSpec::handler(
        "xidmprecord",
        5,
        &["write"],
        ONE_KEY,
        Engine::cmd_xidmprecord,
    ),
];

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
struct Id(u64, u64);
impl Id {
    const MAX: Self = Self(u64::MAX, u64::MAX);
    fn parse(bytes: &[u8]) -> Result<Self, Resp> {
        let mut parts = bytes.split(|b| *b == b'-');
        let ms = unsigned(parts.next().unwrap_or_default()).ok_or_else(id_error)?;
        let seq = parts
            .next()
            .map(unsigned)
            .unwrap_or(Some(0))
            .ok_or_else(id_error)?;
        if parts.next().is_some() {
            return Err(id_error());
        }
        Ok(Self(ms, seq))
    }
    fn next(self) -> Option<Self> {
        self.1
            .checked_add(1)
            .map(|seq| Self(self.0, seq))
            .or_else(|| self.0.checked_add(1).map(|ms| Self(ms, 0)))
    }
    fn bytes(self) -> Bytes {
        Bytes::from(format!("{}-{}", self.0, self.1))
    }
    fn reply(self) -> Resp {
        Resp::Bulk(Some(self.bytes()))
    }
}
fn unsigned(b: &[u8]) -> Option<u64> {
    if b.is_empty() || b.iter().any(|c| !c.is_ascii_digit()) {
        return None;
    }
    b.iter().try_fold(0u64, |n, c| {
        n.checked_mul(10)?.checked_add(u64::from(c - b'0'))
    })
}
fn id_error() -> Resp {
    error("ERR Invalid stream ID specified as stream command argument")
}
fn number(b: &[u8]) -> Result<u64, Resp> {
    unsigned(b)
        .filter(|n| *n <= i64::MAX as u64)
        .ok_or_else(integer_error)
}
fn bulk(b: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(b.into()))
}
fn text(s: &'static str) -> Resp {
    bulk(Bytes::from_static(s.as_bytes()))
}
fn ok() -> Resp {
    Resp::Simple("OK")
}
fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid stream effect")
}
fn nogroup(key: &Bytes, group: &Bytes) -> Resp {
    Resp::Error(format!(
        "NOGROUP No such key '{}' or consumer group '{}'",
        String::from_utf8_lossy(key),
        String::from_utf8_lossy(group)
    ))
}

#[derive(Default)]
pub(super) struct StreamValue {
    entries: BTreeMap<Id, Vec<Bytes>>,
    last: Id,
    added: u64,
    max_deleted: Id,
    recorded_first: Id,
    groups: BTreeMap<Bytes, Group>,
    duration: u64,
    maxsize: u64,
    producers: BTreeMap<Bytes, BTreeMap<Bytes, Dedup>>,
    dedup_expiry: BTreeSet<(i128, Bytes, Bytes)>,
    iids_added: u64,
    duplicates: u64,
}
#[derive(Default)]
struct Group {
    // Runtime identity distinguishes a destroyed/recreated group for blocked readers.
    identity: u64,
    last: Id,
    read: Option<u64>,
    consumers: BTreeMap<Bytes, Consumer>,
    pending: BTreeMap<Id, Pending>,
    nack_seq: u64,
    idle: BTreeSet<(i64, Id)>,
    nacked: BTreeMap<u64, Id>,
    owners: BTreeMap<Bytes, BTreeSet<Id>>,
}
#[derive(Clone, Copy)]
struct Consumer {
    seen: i64,
    active: i64,
}
#[derive(Clone)]
struct Pending {
    consumer: Bytes,
    delivered: i64,
    deliveries: u64,
    nack: u64,
}
#[derive(Clone, Copy)]
struct Dedup {
    id: Id,
    at: i64,
}

impl Group {
    fn remove_pending(&mut self, id: Id) -> Option<Pending> {
        let p = self.pending.remove(&id)?;
        if p.nack > 0 {
            self.nacked.remove(&p.nack);
        } else {
            self.idle.remove(&(p.delivered, id));
            if let Some(ids) = self.owners.get_mut(&p.consumer) {
                ids.remove(&id);
                if ids.is_empty() {
                    self.owners.remove(&p.consumer);
                }
            }
        }
        Some(p)
    }
    fn set_pending(&mut self, id: Id, p: Pending) {
        self.remove_pending(id);
        self.nack_seq = self.nack_seq.max(p.nack);
        if p.nack > 0 {
            self.nacked.insert(p.nack, id);
        } else {
            self.idle.insert((p.delivered, id));
            self.owners
                .entry(p.consumer.clone())
                .or_default()
                .insert(id);
        }
        self.pending.insert(id, p);
    }
    fn consumer_count(&self, name: &Bytes) -> usize {
        self.owners.get(name).map_or(0, BTreeSet::len)
    }
}

static NEXT_GROUP_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
fn info_map(fields: Vec<Resp>) -> Resp {
    let mut fields = fields.into_iter();
    Resp::Map(std::iter::from_fn(|| Some((fields.next()?, fields.next()?))).collect())
}
fn bounded_response(reply: Resp, limit: usize) -> Resp {
    match super::ReplyBudget::new(limit).response(&reply) {
        Ok(()) => reply,
        Err(e) => e,
    }
}
fn charge_row(
    budget: &mut super::ReplyBudget,
    key: &Bytes,
    first: bool,
    id: Id,
    fields: Option<&Vec<Bytes>>,
    claimed: bool,
) -> Result<(), Resp> {
    if first {
        budget.charge(3, key.len())?;
    }
    budget.charge(
        3 + fields.map_or(0, Vec::len) + if claimed { 2 } else { 0 },
        id_length(id),
    )?;
    if let Some(fields) = fields {
        for f in fields {
            budget.charge(0, f.len())?;
        }
    }
    Ok(())
}

enum Effect {
    Init,
    Meta {
        last: Id,
        added: u64,
        deleted: Id,
        first: Id,
        iids: u64,
        duplicates: u64,
    },
    Add(Id, Vec<Bytes>),
    Delete(Id),
    Group(Bytes, Id, Option<u64>),
    DropGroup(Bytes),
    Consumer(Bytes, Bytes, Consumer),
    DropConsumer(Bytes, Bytes),
    Pending(Bytes, Id, Pending),
    Ack(Bytes, Id),
    Config(u64, u64),
    Dedup(Bytes, Bytes, Dedup),
    DropDedup(Bytes, Bytes),
    NackSequence(Bytes, u64),
}
fn put_id(e: &mut Encoder, id: Id) -> io::Result<()> {
    e.u64(id.0)?;
    e.u64(id.1)
}
fn get_id(d: &mut Decoder<'_>) -> io::Result<Id> {
    Ok(Id(d.u64()?, d.u64()?))
}
impl Effect {
    fn encode(&self) -> io::Result<Bytes> {
        let opcode = match self {
            Self::Init => 0,
            Self::Meta { .. } => 1,
            Self::Add(..) => 2,
            Self::Delete(..) => 3,
            Self::Group(..) => 4,
            Self::DropGroup(..) => 5,
            Self::Consumer(..) => 6,
            Self::DropConsumer(..) => 7,
            Self::Pending(..) => 8,
            Self::Ack(..) => 9,
            Self::Config(..) => 10,
            Self::Dedup(..) => 11,
            Self::DropDedup(..) => 12,
            Self::NackSequence(..) => 13,
        };
        let mut e = Encoder::new(1, opcode);
        match self {
            Self::Init => {}
            Self::Meta {
                last,
                added,
                deleted,
                first,
                iids,
                duplicates,
            } => {
                put_id(&mut e, *last)?;
                e.u64(*added)?;
                put_id(&mut e, *deleted)?;
                put_id(&mut e, *first)?;
                e.u64(*iids)?;
                e.u64(*duplicates)?;
            }
            Self::Add(id, fields) => {
                put_id(&mut e, *id)?;
                e.u32(u32::try_from(fields.len()).map_err(|_| invalid())?)?;
                for field in fields {
                    e.bytes(field)?;
                }
            }
            Self::Delete(id) => put_id(&mut e, *id)?,
            Self::Group(name, last, read) => {
                e.bytes(name)?;
                put_id(&mut e, *last)?;
                e.u8(u8::from(read.is_some()))?;
                if let Some(read) = read {
                    e.u64(*read)?;
                }
            }
            Self::DropGroup(name) => e.bytes(name)?,
            Self::Consumer(group, name, c) => {
                e.bytes(group)?;
                e.bytes(name)?;
                e.i64(c.seen)?;
                e.i64(c.active)?;
            }
            Self::DropConsumer(group, name) => {
                e.bytes(group)?;
                e.bytes(name)?;
            }
            Self::Pending(group, id, p) => {
                e.bytes(group)?;
                put_id(&mut e, *id)?;
                e.bytes(&p.consumer)?;
                e.i64(p.delivered)?;
                e.u64(p.deliveries)?;
                e.u64(p.nack)?;
            }
            Self::Ack(group, id) => {
                e.bytes(group)?;
                put_id(&mut e, *id)?;
            }
            Self::Config(duration, maxsize) => {
                e.u64(*duration)?;
                e.u64(*maxsize)?;
            }
            Self::Dedup(pid, iid, d) => {
                e.bytes(pid)?;
                e.bytes(iid)?;
                put_id(&mut e, d.id)?;
                e.i64(d.at)?;
            }
            Self::DropDedup(pid, iid) => {
                e.bytes(pid)?;
                e.bytes(iid)?;
            }
            Self::NackSequence(group, seq) => {
                e.bytes(group)?;
                e.u64(*seq)?;
            }
        }
        Ok(e.finish())
    }
    fn decode(bytes: &[u8]) -> io::Result<Self> {
        let (op, mut d) = Decoder::new(bytes, 1)?;
        let effect = match op {
            0 => Self::Init,
            1 => Self::Meta {
                last: get_id(&mut d)?,
                added: d.u64()?,
                deleted: get_id(&mut d)?,
                first: get_id(&mut d)?,
                iids: d.u64()?,
                duplicates: d.u64()?,
            },
            2 => {
                let id = get_id(&mut d)?;
                let count = d.u32()? as usize;
                if count == 0 || !count.is_multiple_of(2) || count > bytes.len() / 4 {
                    return Err(invalid());
                }
                let mut fields = Vec::with_capacity(count);
                for _ in 0..count {
                    fields.push(Bytes::copy_from_slice(d.bytes()?));
                }
                Self::Add(id, fields)
            }
            3 => Self::Delete(get_id(&mut d)?),
            4 => {
                let name = Bytes::copy_from_slice(d.bytes()?);
                let last = get_id(&mut d)?;
                let read = match d.u8()? {
                    0 => None,
                    1 => Some(d.u64()?),
                    _ => return Err(invalid()),
                };
                Self::Group(name, last, read)
            }
            5 => Self::DropGroup(Bytes::copy_from_slice(d.bytes()?)),
            6 => Self::Consumer(
                Bytes::copy_from_slice(d.bytes()?),
                Bytes::copy_from_slice(d.bytes()?),
                Consumer {
                    seen: d.i64()?,
                    active: d.i64()?,
                },
            ),
            7 => Self::DropConsumer(
                Bytes::copy_from_slice(d.bytes()?),
                Bytes::copy_from_slice(d.bytes()?),
            ),
            8 => Self::Pending(
                Bytes::copy_from_slice(d.bytes()?),
                get_id(&mut d)?,
                Pending {
                    consumer: Bytes::copy_from_slice(d.bytes()?),
                    delivered: d.i64()?,
                    deliveries: d.u64()?,
                    nack: d.u64()?,
                },
            ),
            9 => Self::Ack(Bytes::copy_from_slice(d.bytes()?), get_id(&mut d)?),
            10 => {
                let duration = d.u64()?;
                let size = d.u64()?;
                if !(1..=86400).contains(&duration) || !(1..=10000).contains(&size) {
                    return Err(invalid());
                }
                Self::Config(duration, size)
            }
            11 => Self::Dedup(
                Bytes::copy_from_slice(d.bytes()?),
                Bytes::copy_from_slice(d.bytes()?),
                Dedup {
                    id: get_id(&mut d)?,
                    at: d.i64()?,
                },
            ),
            12 => Self::DropDedup(
                Bytes::copy_from_slice(d.bytes()?),
                Bytes::copy_from_slice(d.bytes()?),
            ),
            13 => Self::NackSequence(Bytes::copy_from_slice(d.bytes()?), d.u64()?),
            _ => return Err(invalid()),
        };
        d.finish()?;
        Ok(effect)
    }
}
impl StreamValue {
    pub(super) fn memory_footprint(&self, samples: usize) -> usize {
        use super::memory::{Bodies, allocation, sampled, tree_bytes};
        let mut bodies = Bodies::default();
        tree_bytes::<(Id, Vec<Bytes>)>(self.entries.len())
            .saturating_add(sampled(self.entries.values(), samples, |fields| {
                allocation::<Bytes>(fields.capacity()).saturating_add(sampled(
                    fields.iter(),
                    samples,
                    |value| bodies.bytes(value),
                ))
            }))
            .saturating_add(tree_bytes::<(Bytes, Group)>(self.groups.len()))
            .saturating_add(sampled(self.groups.iter(), samples, |(name, group)| {
                bodies
                    .bytes(name)
                    .saturating_add(tree_bytes::<(Bytes, Consumer)>(group.consumers.len()))
                    .saturating_add(sampled(group.consumers.keys(), samples, |name| {
                        bodies.bytes(name)
                    }))
                    .saturating_add(tree_bytes::<(Id, Pending)>(group.pending.len()))
                    .saturating_add(sampled(group.pending.values(), samples, |pending| {
                        bodies.bytes(&pending.consumer)
                    }))
                    .saturating_add(tree_bytes::<(i64, Id)>(group.idle.len()))
                    .saturating_add(tree_bytes::<(u64, Id)>(group.nacked.len()))
                    .saturating_add(tree_bytes::<(Bytes, BTreeSet<Id>)>(group.owners.len()))
                    .saturating_add(sampled(group.owners.iter(), samples, |(name, ids)| {
                        bodies
                            .bytes(name)
                            .saturating_add(tree_bytes::<Id>(ids.len()))
                    }))
            }))
            .saturating_add(tree_bytes::<(Bytes, BTreeMap<Bytes, Dedup>)>(
                self.producers.len(),
            ))
            .saturating_add(sampled(self.producers.iter(), samples, |(name, ids)| {
                bodies
                    .bytes(name)
                    .saturating_add(tree_bytes::<(Bytes, Dedup)>(ids.len()))
                    .saturating_add(sampled(ids.keys(), samples, |name| bodies.bytes(name)))
            }))
            .saturating_add(tree_bytes::<(i128, Bytes, Bytes)>(self.dedup_expiry.len()))
            .saturating_add(sampled(
                self.dedup_expiry.iter(),
                samples,
                |(_, producer, iid)| bodies.bytes(producer).saturating_add(bodies.bytes(iid)),
            ))
    }

    fn new() -> Self {
        Self {
            duration: 100,
            maxsize: 100,
            ..Self::default()
        }
    }
    pub(super) fn restore(effect: &[u8]) -> io::Result<Self> {
        if !matches!(Effect::decode(effect)?, Effect::Init) {
            return Err(invalid());
        }
        Ok(Self::new())
    }
    pub(super) fn apply(&mut self, effect: &[u8]) -> io::Result<()> {
        let effect = Effect::decode(effect)?;
        // Validate cross-row references before modifying anything.
        match &effect {
            Effect::Init => return Err(invalid()),
            Effect::Add(id, _) if *id <= self.last || self.entries.contains_key(id) => {
                return Err(invalid());
            }
            Effect::Consumer(g, _, _)
            | Effect::DropConsumer(g, _)
            | Effect::Pending(g, _, _)
            | Effect::Ack(g, _)
            | Effect::NackSequence(g, _)
                if !self.groups.contains_key(g) =>
            {
                return Err(invalid());
            }
            Effect::Pending(g, _, p)
                if p.nack == 0 && !self.groups[g].consumers.contains_key(&p.consumer) =>
            {
                return Err(invalid());
            }
            Effect::Pending(_, _, p)
                if p.deliveries > i64::MAX as u64
                    || p.nack > 0 && (!p.consumer.is_empty() || p.delivered != 0) =>
            {
                return Err(invalid());
            }
            Effect::Pending(g, id, p)
                if p.nack > 0
                    && self.groups[g]
                        .nacked
                        .get(&p.nack)
                        .is_some_and(|existing| existing != id) =>
            {
                return Err(invalid());
            }
            Effect::NackSequence(g, seq) if *seq < self.groups[g].nack_seq => return Err(invalid()),
            Effect::Meta {
                last,
                added,
                deleted,
                first,
                ..
            } if *deleted > *last
                || *added < self.entries.len() as u64
                || self
                    .entries
                    .last_key_value()
                    .is_some_and(|(id, _)| id > last)
                || *first
                    != self
                        .entries
                        .first_key_value()
                        .map_or(Id::default(), |(id, _)| *id) =>
            {
                return Err(invalid());
            }
            _ => {}
        }
        self.resolve(effect);
        Ok(())
    }
    fn resolve(&mut self, effect: Effect) {
        match effect {
            Effect::Init => {}
            Effect::Meta {
                last,
                added,
                deleted,
                first,
                iids,
                duplicates,
            } => {
                self.last = last;
                self.added = added;
                self.max_deleted = deleted;
                self.recorded_first = first;
                self.iids_added = iids;
                self.duplicates = duplicates;
            }
            Effect::Add(id, fields) => {
                self.last = self.last.max(id);
                self.entries.insert(id, fields);
            }
            Effect::Delete(id) => {
                self.entries.remove(&id);
            }
            Effect::Group(name, last, read) => {
                let group = self.groups.entry(name).or_insert_with(|| Group {
                    identity: NEXT_GROUP_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                    ..Group::default()
                });
                group.last = last;
                group.read = read;
            }
            Effect::DropGroup(name) => {
                self.groups.remove(&name);
            }
            Effect::Consumer(g, n, c) => {
                self.groups
                    .get_mut(&g)
                    .expect("validated group")
                    .consumers
                    .insert(n, c);
            }
            Effect::DropConsumer(g, n) => {
                let group = self.groups.get_mut(&g).expect("validated group");
                group.consumers.remove(&n);
                if let Some(ids) = group.owners.get(&n) {
                    let ids: Vec<_> = ids.iter().copied().collect();
                    for id in ids {
                        group.remove_pending(id);
                    }
                }
            }
            Effect::Pending(g, id, p) => {
                self.groups
                    .get_mut(&g)
                    .expect("validated group")
                    .set_pending(id, p);
            }
            Effect::Ack(g, id) => {
                self.groups
                    .get_mut(&g)
                    .expect("validated group")
                    .remove_pending(id);
            }
            Effect::NackSequence(g, seq) => {
                self.groups.get_mut(&g).expect("validated group").nack_seq = seq;
            }
            Effect::Config(d, m) => {
                self.duration = d;
                self.maxsize = m;
                self.producers.clear();
                self.dedup_expiry.clear();
            }
            Effect::Dedup(pid, iid, d) => {
                if let Some(old) = self.producers.get(&pid).and_then(|p| p.get(&iid)) {
                    self.dedup_expiry.remove(&(
                        i128::from(old.at) + i128::from(self.duration) * 1000,
                        pid.clone(),
                        iid.clone(),
                    ));
                }
                self.dedup_expiry.insert((
                    i128::from(d.at) + i128::from(self.duration) * 1000,
                    pid.clone(),
                    iid.clone(),
                ));
                self.producers.entry(pid).or_default().insert(iid, d);
            }
            Effect::DropDedup(pid, iid) => {
                if let Some(p) = self.producers.get_mut(&pid) {
                    if let Some(old) = p.remove(&iid) {
                        self.dedup_expiry.remove(&(
                            i128::from(old.at) + i128::from(self.duration) * 1000,
                            pid.clone(),
                            iid,
                        ));
                    }
                    if p.is_empty() {
                        self.producers.remove(&pid);
                    }
                }
            }
        }
    }
    fn meta(&self) -> Effect {
        Effect::Meta {
            last: self.last,
            added: self.added,
            deleted: self.max_deleted,
            first: self.recorded_first,
            iids: self.iids_added,
            duplicates: self.duplicates,
        }
    }
    fn estimate_read(&self, id: Id) -> Option<u64> {
        if self.added == 0 {
            return Some(0);
        }
        if self.entries.is_empty() && id <= self.last || id == self.last {
            return Some(self.added);
        }
        if id > self.last || id != Id::default() && id < self.max_deleted {
            return None;
        }
        let first = *self.entries.first_key_value()?.0;
        if self.max_deleted < first {
            if id < first {
                return Some(self.added.saturating_sub(self.entries.len() as u64));
            }
            if id == first {
                return Some(
                    self.added
                        .saturating_sub(self.entries.len() as u64)
                        .saturating_add(1),
                );
            }
        }
        None
    }
    fn lag(&self, g: &Group) -> Option<u64> {
        let Some((&first, _)) = self.entries.first_key_value() else {
            return Some(0);
        };
        if g.last < first && self.max_deleted < first {
            return Some(self.entries.len() as u64);
        }
        g.read
            .filter(|_| self.max_deleted <= g.last)
            .or_else(|| self.estimate_read(g.last))
            .map(|r| self.added.saturating_sub(r))
    }
    fn acked(&self, id: Id, except: Option<&Bytes>) -> bool {
        self.groups
            .iter()
            .all(|(name, g)| g.last >= id && (except == Some(name) || !g.pending.contains_key(&id)))
    }
    pub(super) fn snapshot(&self) -> Snapshot<'_> {
        Snapshot {
            rows: Box::new(
                std::iter::once(Effect::Init)
                    .chain(std::iter::once(Effect::Config(self.duration, self.maxsize)))
                    .chain(
                        self.entries
                            .iter()
                            .map(|(id, f)| Effect::Add(*id, f.clone())),
                    )
                    .chain(self.groups.iter().flat_map(|(name, g)| {
                        std::iter::once(Effect::Group(name.clone(), g.last, g.read))
                            .chain(std::iter::once(Effect::NackSequence(
                                name.clone(),
                                g.nack_seq,
                            )))
                            .chain(
                                g.consumers.iter().map(move |(n, c)| {
                                    Effect::Consumer(name.clone(), n.clone(), *c)
                                }),
                            )
                            .chain(
                                g.pending.iter().map(move |(id, p)| {
                                    Effect::Pending(name.clone(), *id, p.clone())
                                }),
                            )
                    }))
                    .chain(self.producers.iter().flat_map(|(pid, p)| {
                        p.iter()
                            .map(move |(iid, d)| Effect::Dedup(pid.clone(), iid.clone(), *d))
                    }))
                    .chain(std::iter::once(self.meta())),
            ),
        }
    }
}
pub(super) struct Snapshot<'a> {
    rows: Box<dyn Iterator<Item = Effect> + Send + 'a>,
}
impl Iterator for Snapshot<'_> {
    type Item = io::Result<Bytes>;
    fn next(&mut self) -> Option<Self::Item> {
        self.rows.next().map(|e| e.encode())
    }
}

#[derive(Clone, Copy, PartialEq)]
enum RefPolicy {
    Keep,
    Del,
    Acked,
}
fn policy(arg: &[u8]) -> Option<RefPolicy> {
    if arg.eq_ignore_ascii_case(b"KEEPREF") {
        Some(RefPolicy::Keep)
    } else if arg.eq_ignore_ascii_case(b"DELREF") {
        Some(RefPolicy::Del)
    } else if arg.eq_ignore_ascii_case(b"ACKED") {
        Some(RefPolicy::Acked)
    } else {
        None
    }
}
struct Trim {
    maxlen: Option<u64>,
    minid: Option<Id>,
    approx: bool,
    limit: usize,
}
fn parse_trim(args: &[Bytes], i: &mut usize) -> Result<Trim, Resp> {
    let max = args
        .get(*i)
        .is_some_and(|a| a.eq_ignore_ascii_case(b"MAXLEN"));
    if !max
        && !args
            .get(*i)
            .is_some_and(|a| a.eq_ignore_ascii_case(b"MINID"))
    {
        return Err(syntax_error());
    }
    *i += 1;
    let mut approx = false;
    if let Some(a) = args.get(*i)
        && (a.as_ref() == b"~" || a.as_ref() == b"=")
    {
        approx = a.as_ref() == b"~";
        *i += 1;
    }
    let threshold = args.get(*i).ok_or_else(syntax_error)?;
    *i += 1;
    let (mut maxlen, mut minid) = (None, None);
    if max {
        maxlen = Some(number(threshold)?);
    } else {
        minid = Some(Id::parse(threshold)?);
    }
    let mut limit = if approx { 10000 } else { usize::MAX };
    if args
        .get(*i)
        .is_some_and(|a| a.eq_ignore_ascii_case(b"LIMIT"))
    {
        if !approx {
            return Err(error(
                "ERR syntax error, LIMIT cannot be used without the special ~ option",
            ));
        }
        *i += 1;
        let n = number(args.get(*i).ok_or_else(syntax_error)?)?;
        *i += 1;
        limit = if n == 0 {
            usize::MAX
        } else {
            usize::try_from(n).unwrap_or(usize::MAX)
        };
    }
    Ok(Trim {
        maxlen,
        minid,
        approx,
        limit,
    })
}
fn trim_ids(s: &StreamValue, t: &Trim, p: RefPolicy, additional: Option<Id>) -> Vec<Id> {
    let len = s.entries.len() + usize::from(additional.is_some());
    let excess = t
        .maxlen
        .map(|m| len.saturating_sub(usize::try_from(m).unwrap_or(usize::MAX)))
        .unwrap_or(usize::MAX);
    let target = if t.approx { excess / 100 * 100 } else { excess };
    let mut out = Vec::new();
    for (examined, id) in s.entries.keys().copied().chain(additional).enumerate() {
        if examined >= t.limit || out.len() >= target || t.minid.is_some_and(|min| id >= min) {
            break;
        }
        if p == RefPolicy::Acked && !s.acked(id, None) {
            continue;
        }
        out.push(id);
    }
    out
}
fn deletion_effects(s: &StreamValue, ids: &[Id], p: RefPolicy) -> Vec<Effect> {
    let mut effects = Vec::new();
    for &id in ids {
        effects.push(Effect::Delete(id));
        if p == RefPolicy::Del {
            for (name, g) in &s.groups {
                if g.pending.contains_key(&id) {
                    effects.push(Effect::Ack(name.clone(), id));
                }
            }
        }
    }
    effects
}
fn row(id: Id, fields: Option<&Vec<Bytes>>) -> Resp {
    array(vec![
        id.reply(),
        fields.map_or(Resp::Array(None), |f| {
            array(f.iter().cloned().map(bulk).collect())
        }),
    ])
}
fn range_bound(arg: &[u8], upper: bool) -> Result<Bound<Id>, Resp> {
    if arg == b"-" {
        return Ok(Bound::Included(Id::default()));
    }
    if arg == b"+" {
        return Ok(Bound::Included(Id::MAX));
    }
    let (arg, exclusive) = arg
        .strip_prefix(b"(")
        .map_or((arg, false), |rest| (rest, true));
    let mut id = Id::parse(arg)?;
    if upper && !arg.contains(&b'-') {
        id.1 = u64::MAX;
    }
    Ok(if exclusive {
        Bound::Excluded(id)
    } else {
        Bound::Included(id)
    })
}
fn valid_bounds(a: &Bound<Id>, b: &Bound<Id>) -> bool {
    match (a, b) {
        (Bound::Included(a), Bound::Included(b)) => a <= b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Included(b) | Bound::Excluded(b)) => a < b,
        _ => true,
    }
}

impl Engine {
    fn stream(&mut self, key: &Bytes, now: i64) -> Result<Option<&StreamValue>, Resp> {
        match self.value(key, now) {
            None => Ok(None),
            Some(Value::Typed(d)) => match d.as_ref() {
                Data::Stream(s) => Ok(Some(s)),
                _ => Err(wrong_type()),
            },
            Some(_) => Err(wrong_type()),
        }
    }
    fn stream_effects(&mut self, key: &Bytes, effects: Vec<Effect>, now: i64) -> Result<(), Resp> {
        if effects.is_empty() {
            return Ok(());
        }
        // Journal encoding is unnecessary for a memory-only mutation. WATCH
        // enables recording, while actor readiness is tracked independently.
        let encoded = if self.journal_enabled {
            Some(
                effects
                    .iter()
                    .map(Effect::encode)
                    .collect::<io::Result<Vec<_>>>()
                    .map_err(|e| self.fail_io(e))?,
            )
        } else {
            None
        };
        self.discard_expired(key, now);
        if !self.entries.contains_key(key) {
            let init = if self.journal_enabled {
                Some(Effect::Init.encode().map_err(|e| self.fail_io(e))?)
            } else {
                None
            };
            self.write_entry(
                key.clone(),
                Entry {
                    value: Value::Typed(Box::new(Data::Stream(StreamValue::new()))),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
            if let Some(init) = init {
                self.record_typed(key, 3, init, now);
            }
        }
        let Some(entry) = self.entries.get_mut(key) else {
            unreachable!()
        };
        let Value::Typed(d) = &mut entry.value else {
            return Err(wrong_type());
        };
        let Data::Stream(s) = d.as_mut() else {
            return Err(wrong_type());
        };
        for effect in effects {
            s.resolve(effect);
        }
        if let Some(encoded) = encoded {
            for effect in encoded {
                self.record_typed(key, 3, effect, now);
            }
        } else if self.ready_tracking {
            self.ready_keys.push((self.active_database, key.clone()));
        }
        Ok(())
    }
    fn cmd_xlen(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.stream(&args[1], now) {
            Ok(s) => Resp::Integer(s.map_or(0, |s| s.entries.len() as i64)),
            Err(e) => e,
        }
    }
    fn cmd_xrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        let reverse = args[0].eq_ignore_ascii_case(b"XREVRANGE");
        let (lo, hi) = if reverse {
            (&args[3], &args[2])
        } else {
            (&args[2], &args[3])
        };
        let (lo, hi) = match (range_bound(lo, false), range_bound(hi, true)) {
            (Ok(a), Ok(b)) => (a, b),
            (Err(e), _) | (_, Err(e)) => return e,
        };
        let count = if args.len() == 4 {
            usize::MAX
        } else if args.len() == 6 && args[4].eq_ignore_ascii_case(b"COUNT") {
            match number(&args[5]) {
                Ok(0) => return array(vec![]),
                Ok(n) => n as usize,
                Err(e) => return e,
            }
        } else {
            return syntax_error();
        };
        let limit = self.reply_limit;
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return array(vec![]),
            Err(e) => return e,
        };
        if !valid_bounds(&lo, &hi) {
            return array(vec![]);
        }
        if reverse {
            bounded_replies(
                s.entries
                    .range((lo, hi))
                    .rev()
                    .take(count)
                    .map(|(id, f)| row(*id, Some(f))),
                limit,
            )
        } else {
            bounded_replies(
                s.entries
                    .range((lo, hi))
                    .take(count)
                    .map(|(id, f)| row(*id, Some(f))),
                limit,
            )
        }
    }
    fn cmd_xadd(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (mut i, mut nomk, mut p, mut trim, mut dedup) = (2, false, None, None, None);
        while let Some(arg) = args.get(i) {
            if arg.eq_ignore_ascii_case(b"NOMKSTREAM") {
                nomk = true;
                i += 1;
            } else if let Some(policy) = policy(arg) {
                if p.replace(policy).is_some() {
                    return syntax_error();
                }
                i += 1;
            } else if arg.eq_ignore_ascii_case(b"MAXLEN") || arg.eq_ignore_ascii_case(b"MINID") {
                if trim.is_some() {
                    return syntax_error();
                }
                trim = match parse_trim(args, &mut i) {
                    Ok(t) => Some(t),
                    Err(e) => return e,
                };
            } else if arg.eq_ignore_ascii_case(b"IDMP") || arg.eq_ignore_ascii_case(b"IDMPAUTO") {
                if dedup.is_some() {
                    return syntax_error();
                }
                let auto = arg.eq_ignore_ascii_case(b"IDMPAUTO");
                let Some(pid) = args.get(i + 1) else {
                    return syntax_error();
                };
                let iid = if auto {
                    None
                } else {
                    let Some(iid) = args.get(i + 2) else {
                        return syntax_error();
                    };
                    Some(iid.clone())
                };
                dedup = Some((pid.clone(), iid));
                i += if auto { 2 } else { 3 };
            } else {
                break;
            }
        }
        let Some(selector) = args.get(i) else {
            return syntax_error();
        };
        i += 1;
        if i >= args.len() || !(args.len() - i).is_multiple_of(2) {
            return error("ERR wrong number of arguments for 'xadd' command");
        }
        if dedup.is_some() && selector.as_ref() != b"*" {
            return error("ERR IDMP options require an auto-generated ID");
        }
        let p = p.unwrap_or(RefPolicy::Keep);
        let empty = StreamValue::new();
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) if nomk => return Resp::Bulk(None),
            Ok(None) => &empty,
            Err(e) => return e,
        };
        let mut effects: Vec<_> = s
            .dedup_expiry
            .iter()
            .take_while(|(at, _, _)| *at <= i128::from(now))
            .map(|(_, pid, iid)| Effect::DropDedup(pid.clone(), iid.clone()))
            .collect();
        let dedup = dedup.map(|(pid, iid)| {
            (
                pid,
                iid.unwrap_or_else(|| {
                    let mut h = Sha256::new();
                    for field in &args[i..] {
                        h.update((field.len() as u64).to_le_bytes());
                        h.update(field);
                    }
                    Bytes::copy_from_slice(&h.finalize())
                }),
            )
        });
        if let Some((pid, iid)) = &dedup
            && let Some(d) = s
                .producers
                .get(pid)
                .and_then(|p| p.get(iid))
                .filter(|d| i128::from(now) - i128::from(d.at) < i128::from(s.duration) * 1000)
        {
            let mut meta = s.meta();
            if let Effect::Meta { duplicates, .. } = &mut meta {
                *duplicates = duplicates.saturating_add(1);
            }
            let original = d.id;
            effects.push(meta);
            return match self.stream_effects(&args[1], effects, now) {
                Ok(()) => original.reply(),
                Err(e) => e,
            };
        }
        let id = if selector.as_ref() == b"*" {
            let ms = (now.max(0) as u64).max(s.last.0);
            if ms == s.last.0 {
                match s.last.next() {
                    Some(id) => id,
                    None => return error("ERR The stream has exhausted the last possible ID"),
                }
            } else {
                Id(ms, 0)
            }
        } else if let Some(ms) = selector.strip_suffix(b"-*") {
            let ms = match unsigned(ms) {
                Some(v) => v,
                None => return id_error(),
            };
            if ms < s.last.0 {
                return error(
                    "ERR The ID specified in XADD is equal or smaller than the target stream top item",
                );
            }
            if ms == s.last.0 {
                match s.last.1.checked_add(1) {
                    Some(seq) => Id(ms, seq),
                    None => return error("ERR The stream has exhausted the last possible ID"),
                }
            } else {
                Id(ms, 0)
            }
        } else {
            match Id::parse(selector) {
                Ok(id) => id,
                Err(e) => return e,
            }
        };
        if id == Id::default() {
            return error("ERR The ID specified in XADD must be greater than 0-0");
        }
        if id <= s.last {
            return error(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item",
            );
        }
        let mut iid_count = s.iids_added;
        if let Some((pid, iid)) = dedup {
            if let Some(producer) = s.producers.get(&pid) {
                let mut retained: Vec<_> = producer
                    .iter()
                    .filter(|(_, d)| now.saturating_sub(d.at) < (s.duration * 1000) as i64)
                    .collect();
                retained.sort_unstable_by_key(|(_, d)| (d.at, d.id));
                let evict = retained
                    .len()
                    .saturating_add(1)
                    .saturating_sub(s.maxsize as usize);
                let oldest: BTreeSet<_> = retained
                    .iter()
                    .take(evict)
                    .map(|(iid, _)| (*iid).clone())
                    .collect();
                for (old, d) in producer {
                    if (old == &iid || oldest.contains(old))
                        && now.saturating_sub(d.at) < (s.duration * 1000) as i64
                    {
                        effects.push(Effect::DropDedup(pid.clone(), old.clone()));
                    }
                }
            }
            effects.push(Effect::Dedup(pid, iid, Dedup { id, at: now }));
            iid_count = iid_count.saturating_add(1);
        }
        let ids = trim
            .as_ref()
            .map(|t| trim_ids(s, t, p, Some(id)))
            .unwrap_or_default();
        effects.push(Effect::Add(id, args[i..].to_vec()));
        effects.extend(deletion_effects(s, &ids, p));
        let removed: BTreeSet<_> = ids.iter().copied().collect();
        let first = s
            .entries
            .keys()
            .copied()
            .chain(std::iter::once(id))
            .find(|id| !removed.contains(id))
            .unwrap_or_default();
        effects.push(Effect::Meta {
            last: id,
            added: s.added.saturating_add(1),
            deleted: s.max_deleted,
            first,
            iids: iid_count,
            duplicates: s.duplicates,
        });
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => id.reply(),
            Err(e) => e,
        }
    }
    fn cmd_xtrim(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut i = 2;
        let mut p = args.get(i).and_then(|a| policy(a));
        if p.is_some() {
            i += 1;
        }
        let t = match parse_trim(args, &mut i) {
            Ok(t) => t,
            Err(e) => return e,
        };
        if let Some(pp) = args.get(i).and_then(|a| policy(a)) {
            if p.replace(pp).is_some() {
                return syntax_error();
            }
            i += 1;
        }
        if i != args.len() {
            return syntax_error();
        }
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return Resp::Integer(0),
            Err(e) => return e,
        };
        let p = p.unwrap_or(RefPolicy::Keep);
        let ids = trim_ids(s, &t, p, None);
        let count = ids.len();
        let mut effects = deletion_effects(s, &ids, p);
        if count > 0 {
            let removed: BTreeSet<_> = ids.iter().copied().collect();
            let mut meta = s.meta();
            if let Effect::Meta { first, .. } = &mut meta {
                *first = s
                    .entries
                    .keys()
                    .find(|id| !removed.contains(id))
                    .copied()
                    .unwrap_or_default();
            }
            effects.push(meta);
        }
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => Resp::Integer(count as i64),
            Err(e) => e,
        }
    }
    fn cmd_xdel(&mut self, args: &[Bytes], now: i64) -> Resp {
        let extended = !args[0].eq_ignore_ascii_case(b"XDEL");
        let ack = args[0].eq_ignore_ascii_case(b"XACKDEL");
        let (mut i, mut p) = (if ack { 3 } else { 2 }, RefPolicy::Keep);
        if extended {
            if let Some(pp) = args.get(i).and_then(|a| policy(a)) {
                p = pp;
                i += 1;
            }
            if !args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"IDS")) {
                return syntax_error();
            }
            i += 1;
            let n = match args.get(i).map(|a| number(a)) {
                Some(Ok(n)) if n > 0 => n as usize,
                Some(Err(e)) => return e,
                _ => return syntax_error(),
            };
            i += 1;
            if n != args.len() - i {
                return syntax_error();
            }
        }
        let ids = match args[i..]
            .iter()
            .map(|a| Id::parse(a))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(ids) => ids,
            Err(e) => return e,
        };
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => {
                return if extended {
                    array(ids.iter().map(|_| Resp::Integer(-1)).collect())
                } else {
                    Resp::Integer(0)
                };
            }
            Err(e) => return e,
        };
        let g = if ack {
            match s.groups.get(&args[2]) {
                Some(g) => Some(g),
                None => return array(ids.iter().map(|_| Resp::Integer(-1)).collect()),
            }
        } else {
            None
        };
        let mut deleted = BTreeSet::new();
        let mut acknowledged = BTreeSet::new();
        let mut effects = Vec::new();
        let mut codes = Vec::new();
        for id in ids {
            let exists = s.entries.contains_key(&id) && !deleted.contains(&id);
            if let Some(g) = g {
                if !g.pending.contains_key(&id) || !acknowledged.insert(id) {
                    codes.push(Resp::Integer(-1));
                    continue;
                }
                effects.push(Effect::Ack(args[2].clone(), id));
            }
            if p == RefPolicy::Del {
                for (name, g) in &s.groups {
                    if g.pending.contains_key(&id) {
                        effects.push(Effect::Ack(name.clone(), id));
                    }
                }
            }
            if p == RefPolicy::Acked && !s.acked(id, if ack { Some(&args[2]) } else { None }) {
                codes.push(Resp::Integer(2));
                continue;
            }
            if !exists {
                codes.push(Resp::Integer(if ack { 1 } else { -1 }));
                continue;
            }
            deleted.insert(id);
            effects.push(Effect::Delete(id));
            codes.push(Resp::Integer(1));
        }
        if !deleted.is_empty() {
            let mut meta = s.meta();
            if let Effect::Meta {
                deleted: d, first, ..
            } = &mut meta
            {
                *d = (*d).max(*deleted.last().unwrap());
                *first = s
                    .entries
                    .keys()
                    .find(|id| !deleted.contains(id))
                    .copied()
                    .unwrap_or_default();
            }
            effects.push(meta);
        }
        let count = deleted.len();
        let reply = if extended {
            bounded_replies(codes, self.reply_limit)
        } else {
            Resp::Integer(count as i64)
        };
        if matches!(&reply, Resp::Error(_)) {
            return reply;
        }
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => reply,
            Err(e) => e,
        }
    }
}

/// Actor-owned blocked read. All relative cursors have already been resolved.
pub(crate) struct BlockingStream {
    pub args: Vec<Bytes>,
    pub keys: Vec<Bytes>,
    pub timeout: Option<Duration>,
    pub retry_after: Option<Duration>,
    pub timeseries: bool,
    pub timeseries_existed: bool,
    pub group_tokens: Vec<(Bytes, u64)>,
}
struct ReadOptions {
    group: Option<(Bytes, Bytes)>,
    count: usize,
    maxcount: usize,
    maxsize: usize,
    block: Option<Option<Duration>>,
    claim: Option<u64>,
    noack: bool,
    split: usize,
    keys: Vec<Bytes>,
    ids: Vec<Bytes>,
}
struct ReadStage {
    last: Id,
    read: Option<u64>,
    consumer: Consumer,
    pending: BTreeMap<Id, Option<Pending>>,
}
fn changed(stage: Option<&ReadStage>, id: &Id) -> bool {
    stage.is_some_and(|s| s.pending.contains_key(id))
}
impl ReadOptions {
    fn parse(args: &[Bytes]) -> Result<Self, Resp> {
        let grouped = args[0].eq_ignore_ascii_case(b"XREADGROUP");
        let mut i = 1;
        let group = if grouped {
            if args.len() < 7 || !args[1].eq_ignore_ascii_case(b"GROUP") {
                return Err(syntax_error());
            }
            i = 4;
            Some((args[2].clone(), args[3].clone()))
        } else {
            None
        };
        let mut o = Self {
            group,
            count: usize::MAX,
            maxcount: usize::MAX,
            maxsize: usize::MAX,
            block: None,
            claim: None,
            noack: false,
            split: 0,
            keys: vec![],
            ids: vec![],
        };
        loop {
            let a = args.get(i).ok_or_else(syntax_error)?;
            if a.eq_ignore_ascii_case(b"STREAMS") {
                i += 1;
                break;
            }
            if a.eq_ignore_ascii_case(b"NOACK") && grouped {
                o.noack = true;
                i += 1;
                continue;
            }
            if a.eq_ignore_ascii_case(b"COUNT") {
                let n =
                    integer(args.get(i + 1).ok_or_else(syntax_error)?).ok_or_else(integer_error)?;
                o.count = if n <= 0 { usize::MAX } else { n as usize };
                i += 2;
                continue;
            }
            let n = number(args.get(i + 1).ok_or_else(syntax_error)?)?;
            if a.eq_ignore_ascii_case(b"MAXCOUNT") {
                if n == 0 {
                    return Err(integer_error());
                }
                o.maxcount = usize::try_from(n).unwrap_or(usize::MAX);
            } else if a.eq_ignore_ascii_case(b"MAXSIZE") {
                if n == 0 {
                    return Err(integer_error());
                }
                o.maxsize = usize::try_from(n).unwrap_or(usize::MAX);
            } else if a.eq_ignore_ascii_case(b"BLOCK") {
                if n > i64::MAX as u64 {
                    return Err(integer_error());
                }
                o.block = Some(if n == 0 {
                    None
                } else {
                    Some(Duration::from_millis(n))
                });
            } else if a.eq_ignore_ascii_case(b"CLAIM") && grouped {
                o.claim = Some(n);
            } else {
                return Err(syntax_error());
            }
            i += 2;
        }
        if o.maxcount != usize::MAX && o.count != usize::MAX && o.maxcount < o.count {
            return Err(error("ERR MAXCOUNT must be greater than or equal to COUNT"));
        }
        let n = args.len() - i;
        if n == 0 || !n.is_multiple_of(2) {
            return Err(error(
                "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified.",
            ));
        }
        o.split = i + n / 2;
        o.keys = args[i..o.split].to_vec();
        o.ids = args[o.split..].to_vec();
        for id in &o.ids {
            if grouped {
                if id.as_ref() != b">" {
                    Id::parse(id)?;
                }
            } else if id.as_ref() != b"$" && id.as_ref() != b"+" {
                Id::parse(id)?;
            }
        }
        Ok(o)
    }
}
fn digits(n: u64) -> usize {
    if n == 0 { 1 } else { n.ilog10() as usize + 1 }
}
fn id_length(id: Id) -> usize {
    digits(id.0) + 1 + digits(id.1)
}
fn bulk_size(n: usize) -> usize {
    n.saturating_add(digits(n as u64)).saturating_add(5)
}
fn entry_size(id: Id, fields: Option<&Vec<Bytes>>) -> usize {
    4usize
        .saturating_add(bulk_size(id_length(id)))
        .saturating_add(fields.map_or(5, |f| {
            digits(f.len() as u64) + 3 + f.iter().map(|v| bulk_size(v.len())).sum::<usize>()
        }))
}
impl Engine {
    fn cmd_xread(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.stream_read(args, now) {
            Execution::Reply(r) => r,
            Execution::StreamBlock(_) => Resp::Array(None),
            _ => unreachable!(),
        }
    }
    pub(crate) fn stream_read(&mut self, args: &[Bytes], now: i64) -> Execution {
        match self.read_streams(args, now) {
            Ok(e) => e,
            Err(e) => Execution::Reply(e),
        }
    }
    pub(crate) fn resume_blocked_read(
        &mut self,
        read: &BlockingStream,
        timeout: bool,
    ) -> io::Result<Execution> {
        let now = Self::now();
        if !timeout && !read.group_tokens.is_empty() {
            for (key, token) in &read.group_tokens {
                let group = &read.args[2];
                match self.stream(key, now) {
                    Ok(Some(s)) if s.groups.get(group).is_some_and(|g| g.identity == *token) => {}
                    _ => {
                        return match self.failure.take() {
                            Some(e) => Err(e),
                            None => Ok(Execution::Reply(nogroup(key, group))),
                        };
                    }
                }
            }
        }
        let deleted = read.timeseries
            && read.timeseries_existed
            && !self.timeseries_key_exists(&read.keys[0], now);
        let execution = if deleted {
            Execution::Reply(array(vec![]))
        } else if read.timeseries {
            self.timeseries_read(&read.args, now, timeout)
        } else if timeout {
            Execution::Reply(Resp::Array(None))
        } else {
            self.stream_read(&read.args, now)
        };
        match self.failure.take() {
            Some(e) => Err(e),
            None => Ok(execution),
        }
    }
    fn read_streams(&mut self, args: &[Bytes], now: i64) -> Result<Execution, Resp> {
        let o = ReadOptions::parse(args)?;
        let mut frozen = args.to_vec();
        let mut group_tokens = Vec::new();
        let mut budget = super::ReplyBudget::new(self.reply_limit);
        budget.charge(1, 0)?;
        let resp2 = self.command_version() == crate::protocol::Version::Resp2;
        // Validate all types/groups and freeze all '$' cursors before delivery.
        for (index, key) in o.keys.iter().enumerate() {
            let s = self.stream(key, now)?;
            if let Some((group, _)) = &o.group {
                if s.is_none_or(|s| !s.groups.contains_key(group)) {
                    return Err(nogroup(key, group));
                }
                group_tokens.push((key.clone(), s.unwrap().groups[group].identity));
            }
            if o.ids[index].as_ref() == b"$" {
                frozen[o.split + index] = s.map_or(Id::default(), |s| s.last).bytes();
            }
        }
        let expiry_retry = if o.group.is_some() {
            o.keys
                .iter()
                .filter_map(|key| self.entries.get(key).and_then(|e| e.expires_at))
                .map(|at| at.saturating_sub(now).max(1) as u64)
                .min()
        } else {
            None
        };
        let mut replies = Vec::new();
        let mut changes: Vec<(Bytes, Vec<Effect>)> = Vec::new();
        let (mut total, mut bytes) = (0usize, 0usize);
        let mut retry: Option<u64> = None;
        let mut history = false;
        let mut seen = BTreeSet::new();
        let duplicates: BTreeSet<_> = if o.group.is_some() && o.keys.len() > 1 {
            o.keys
                .iter()
                .filter(|key| !seen.insert((*key).clone()))
                .cloned()
                .collect()
        } else {
            BTreeSet::new()
        };
        let mut staged = BTreeMap::<Bytes, ReadStage>::new();
        for (index, key) in o.keys.iter().enumerate() {
            if total >= o.maxcount || total > 0 && bytes >= o.maxsize {
                break;
            }
            let mut effects = Vec::new();
            let mut rows = Vec::new();
            let mut delivered = 0usize;
            let stream_header = bulk_size(key.len()) + if resp2 { 4 } else { 0 };
            let Some(s) = self.stream(key, now)? else {
                continue;
            };
            if let Some((group, consumer)) = &o.group {
                let g = &s.groups[group];
                let fresh = o.ids[index].as_ref() == b">";
                history |= !fresh;
                let prior = staged.get(key);
                let previous = prior
                    .map(|p| p.consumer)
                    .or_else(|| g.consumers.get(consumer).copied())
                    .unwrap_or(Consumer {
                        seen: now,
                        active: -1,
                    });
                let mut c = Consumer {
                    seen: now,
                    ..previous
                };
                let (mut last, mut read) = prior.map_or((g.last, g.read), |p| (p.last, p.read));
                let initial_last = last;
                if s.entries
                    .first_key_value()
                    .is_some_and(|(first, _)| last < *first && s.max_deleted < *first)
                {
                    read = s.estimate_read(last);
                }
                if fresh {
                    let mut claimed_ids = BTreeSet::new();
                    if let Some(minidle) = o.claim {
                        let candidates = g
                            .nacked
                            .values()
                            .filter(|id| !changed(prior, id))
                            .map(|id| (*id, &g.pending[id]))
                            .chain(
                                g.idle
                                    .iter()
                                    .filter(|(_, id)| !changed(prior, id))
                                    .map(|(_, id)| (*id, &g.pending[id])),
                            )
                            .chain(
                                prior
                                    .into_iter()
                                    .flat_map(|stage| stage.pending.iter())
                                    .filter_map(|(id, p)| p.as_ref().map(|p| (*id, p))),
                            );
                        for (id, p) in candidates {
                            let idle = now.saturating_sub(p.delivered).max(0) as u64;
                            if p.nack == 0 && idle < minidle {
                                let remaining = minidle - idle;
                                retry = Some(retry.map_or(remaining, |r| r.min(remaining)));
                                break;
                            }
                            let Some(fields) = s.entries.get(&id) else {
                                effects.push(Effect::Ack(group.clone(), id));
                                continue;
                            };
                            let size = entry_size(id, Some(fields))
                                .saturating_add(digits(idle) + digits(p.deliveries) + 6)
                                .saturating_add(if rows.is_empty() { stream_header } else { 0 });
                            if delivered >= o.count
                                || total >= o.maxcount
                                || (total > 0 && bytes >= o.maxsize)
                            {
                                break;
                            }
                            let idle_reply = Resp::Integer(idle.min(i64::MAX as u64) as i64);
                            charge_row(&mut budget, key, rows.is_empty(), id, Some(fields), true)?;
                            rows.push(array(vec![
                                id.reply(),
                                array(fields.iter().cloned().map(bulk).collect()),
                                idle_reply,
                                Resp::Integer(p.deliveries as i64),
                            ]));
                            claimed_ids.insert(id);
                            effects.push(Effect::Pending(
                                group.clone(),
                                id,
                                Pending {
                                    consumer: consumer.clone(),
                                    delivered: now,
                                    deliveries: p.deliveries.saturating_add(1).min(i64::MAX as u64),
                                    nack: 0,
                                },
                            ));
                            delivered += 1;
                            total += 1;
                            bytes = bytes.saturating_add(size);
                        }
                    }
                    for (&id, fields) in s.entries.range((Bound::Excluded(last), Bound::Unbounded))
                    {
                        if claimed_ids.contains(&id) {
                            last = id;
                            read = read.map(|n| n.saturating_add(1));
                            continue;
                        }
                        let size = entry_size(id, Some(fields))
                            .saturating_add(if o.claim.is_some() { 8 } else { 0 })
                            .saturating_add(if rows.is_empty() { stream_header } else { 0 });
                        if delivered >= o.count
                            || total >= o.maxcount
                            || (total > 0 && bytes >= o.maxsize)
                        {
                            break;
                        }
                        charge_row(
                            &mut budget,
                            key,
                            rows.is_empty(),
                            id,
                            Some(fields),
                            o.claim.is_some(),
                        )?;
                        rows.push(if o.claim.is_some() {
                            array(vec![
                                id.reply(),
                                array(fields.iter().cloned().map(bulk).collect()),
                                Resp::Integer(0),
                                Resp::Integer(0),
                            ])
                        } else {
                            row(id, Some(fields))
                        });
                        last = id;
                        read = read.map(|n| n.saturating_add(1));
                        if !o.noack {
                            effects.push(Effect::Pending(
                                group.clone(),
                                id,
                                Pending {
                                    consumer: consumer.clone(),
                                    delivered: now,
                                    deliveries: 1,
                                    nack: 0,
                                },
                            ));
                        }
                        delivered += 1;
                        total += 1;
                        bytes = bytes.saturating_add(size);
                    }
                    if last != initial_last {
                        if last == s.last {
                            read = Some(s.added);
                        } else if last >= s.max_deleted
                            && (read.is_none() || initial_last < s.max_deleted)
                        {
                            read = Some(
                                s.added.saturating_sub(
                                    s.entries
                                        .range((Bound::Excluded(last), Bound::Unbounded))
                                        .count() as u64,
                                ),
                            );
                        }
                        effects.push(Effect::Group(group.clone(), last, read));
                    }
                } else {
                    let from = Id::parse(&o.ids[index])?;
                    let mut base = g
                        .owners
                        .get(consumer)
                        .into_iter()
                        .flat_map(|ids| ids.range((Bound::Excluded(from), Bound::Unbounded)))
                        .filter(|id| !changed(prior, id))
                        .copied()
                        .peekable();
                    let mut extra = prior
                        .into_iter()
                        .flat_map(|stage| {
                            stage
                                .pending
                                .range((Bound::Excluded(from), Bound::Unbounded))
                        })
                        .filter(|(_, p)| p.is_some())
                        .map(|(id, _)| *id)
                        .peekable();
                    let ids = std::iter::from_fn(move || {
                        match (base.peek().copied(), extra.peek().copied()) {
                            (Some(a), Some(b)) if a < b => base.next(),
                            (Some(_), Some(_)) => extra.next(),
                            (Some(_), None) => base.next(),
                            (None, Some(_)) => extra.next(),
                            (None, None) => None,
                        }
                    });
                    for id in ids {
                        let p = prior
                            .and_then(|stage| stage.pending.get(&id))
                            .and_then(Option::as_ref)
                            .unwrap_or_else(|| &g.pending[&id]);
                        let fields = s.entries.get(&id);
                        let size = entry_size(id, fields).saturating_add(if rows.is_empty() {
                            stream_header
                        } else {
                            0
                        });
                        if delivered >= o.count
                            || total >= o.maxcount
                            || (total > 0 && bytes >= o.maxsize)
                        {
                            break;
                        }
                        charge_row(&mut budget, key, rows.is_empty(), id, fields, false)?;
                        rows.push(row(id, fields));
                        if fields.is_some() {
                            effects.push(Effect::Pending(
                                group.clone(),
                                id,
                                Pending {
                                    consumer: consumer.clone(),
                                    delivered: now,
                                    deliveries: p.deliveries.saturating_add(1).min(i64::MAX as u64),
                                    nack: 0,
                                },
                            ));
                        }
                        delivered += 1;
                        total += 1;
                        bytes = bytes.saturating_add(size);
                    }
                }
                if delivered > 0 {
                    c.active = now;
                }
                // Consumer must precede pending effects in the replay stream.
                effects.insert(0, Effect::Consumer(group.clone(), consumer.clone(), c));
                if duplicates.contains(key) {
                    let stage = staged.entry(key.clone()).or_insert_with(|| ReadStage {
                        last,
                        read,
                        consumer: c,
                        pending: BTreeMap::new(),
                    });
                    stage.last = last;
                    stage.read = read;
                    stage.consumer = c;
                    for effect in &effects {
                        match effect {
                            Effect::Pending(_, id, p) => {
                                stage.pending.insert(*id, Some(p.clone()));
                            }
                            Effect::Ack(_, id) => {
                                stage.pending.insert(*id, None);
                            }
                            _ => {}
                        }
                    }
                }
            } else {
                let selector = &frozen[o.split + index];
                if selector.as_ref() == b"+" {
                    if let Some((&id, fields)) = s.entries.last_key_value() {
                        let size = entry_size(id, Some(fields)).saturating_add(stream_header);
                        if total < o.maxcount && (total == 0 || bytes < o.maxsize) {
                            charge_row(&mut budget, key, true, id, Some(fields), false)?;
                            rows.push(row(id, Some(fields)));
                            total += 1;
                            bytes = bytes.saturating_add(size);
                        }
                    }
                } else {
                    let from = Id::parse(selector)?;
                    for (&id, fields) in s.entries.range((Bound::Excluded(from), Bound::Unbounded))
                    {
                        let size = entry_size(id, Some(fields))
                            .saturating_add(if rows.is_empty() { stream_header } else { 0 });
                        if rows.len() >= o.count
                            || total >= o.maxcount
                            || (total > 0 && bytes >= o.maxsize)
                        {
                            break;
                        }
                        charge_row(&mut budget, key, rows.is_empty(), id, Some(fields), false)?;
                        rows.push(row(id, Some(fields)));
                        total += 1;
                        bytes = bytes.saturating_add(size);
                    }
                }
            }
            if !rows.is_empty() || o.group.is_some() && o.ids[index].as_ref() != b">" {
                bytes = bytes.saturating_add(digits(rows.len() as u64) + 3);
                replies.push(array(vec![bulk(key.clone()), array(rows)]));
            }
            if !effects.is_empty() {
                changes.push((key.clone(), effects));
            }
        }
        let reply = if self.command_version() == crate::protocol::Version::Resp3 {
            Resp::Map(
                replies
                    .into_iter()
                    .map(|r| {
                        let Resp::Array(Some(mut pair)) = r else {
                            unreachable!()
                        };
                        let value = pair.pop().unwrap();
                        (pair.pop().unwrap(), value)
                    })
                    .collect(),
            )
        } else {
            array(replies)
        };
        let reply = bounded_response(reply, self.reply_limit);
        if matches!(&reply, Resp::Error(_)) {
            return Ok(Execution::Reply(reply));
        }
        for (key, effects) in changes {
            self.stream_effects(&key, effects, now)?;
        }
        if total > 0 || history {
            return Ok(Execution::Reply(reply));
        }
        if let Some(timeout) = o.block {
            let retry = retry.into_iter().chain(expiry_retry).min();
            return Ok(Execution::StreamBlock(BlockingStream {
                args: frozen,
                keys: o.keys,
                timeout,
                retry_after: retry.map(|ms| Duration::from_millis(ms.max(1))),
                timeseries: false,
                timeseries_existed: false,
                group_tokens,
            }));
        }
        Ok(Execution::Reply(Resp::Array(None)))
    }
    fn cmd_xgroup(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[1].eq_ignore_ascii_case(b"HELP") {
            return if args.len() == 2 {
                array(
                    [
                        "CREATE key group <id|$> [MKSTREAM] [ENTRIESREAD n]",
                        "DESTROY key group",
                        "CREATECONSUMER key group consumer",
                        "DELCONSUMER key group consumer",
                        "SETID key group <id|$> [ENTRIESREAD n]",
                    ]
                    .into_iter()
                    .map(text)
                    .collect(),
                )
            } else {
                syntax_error()
            };
        }
        if args.len() < 4 {
            return syntax_error();
        }
        let sub = &args[1];
        let key = &args[2];
        let name = &args[3];
        let create = sub.eq_ignore_ascii_case(b"CREATE");
        let set = sub.eq_ignore_ascii_case(b"SETID");
        let empty = StreamValue::new();
        let s = match self.stream(key, now) {
            Ok(Some(s)) => s,
            Ok(None) if create => &empty,
            Ok(None) => {
                return if sub.eq_ignore_ascii_case(b"DESTROY") {
                    Resp::Integer(0)
                } else {
                    nogroup(key, name)
                };
            }
            Err(e) => return e,
        };
        let (mut effects, reply) = (Vec::new(), ok());
        let reply = if create || set {
            if args.len() < 5 {
                return syntax_error();
            }
            let last = if args[4].as_ref() == b"$" {
                s.last
            } else {
                match Id::parse(&args[4]) {
                    Ok(id) => id,
                    Err(e) => return e,
                }
            };
            let (mut mk, mut read, mut supplied_read, mut i) = (false, None, false, 5);
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"MKSTREAM") && create {
                    mk = true;
                    i += 1;
                } else if args[i].eq_ignore_ascii_case(b"ENTRIESREAD") {
                    let Some(a) = args.get(i + 1) else {
                        return syntax_error();
                    };
                    supplied_read = true;
                    read = match integer(a) {
                        Some(-1) => None,
                        Some(n) if n >= 0 => Some(n as u64),
                        _ => return integer_error(),
                    };
                    i += 2;
                } else {
                    return syntax_error();
                }
            }
            if create {
                if s.groups.contains_key(name) {
                    return error("BUSYGROUP Consumer Group name already exists");
                }
                if std::ptr::eq(s, &empty) && !mk {
                    return error(
                        "ERR The XGROUP subcommand requires the key to exist. Note that for CREATE you may want to use the MKSTREAM option to create an empty stream automatically.",
                    );
                }
            } else if !s.groups.contains_key(name) {
                return nogroup(key, name);
            }
            if !supplied_read {
                read = s.estimate_read(last);
            }
            effects.push(Effect::Group(name.clone(), last, read));
            reply
        } else if sub.eq_ignore_ascii_case(b"DESTROY") {
            if args.len() != 4 {
                return syntax_error();
            }
            let existed = s.groups.contains_key(name);
            if existed {
                effects.push(Effect::DropGroup(name.clone()));
            }
            Resp::Integer(i64::from(existed))
        } else if sub.eq_ignore_ascii_case(b"CREATECONSUMER")
            || sub.eq_ignore_ascii_case(b"DELCONSUMER")
        {
            if args.len() != 5 {
                return syntax_error();
            }
            let Some(g) = s.groups.get(name) else {
                return nogroup(key, name);
            };
            let consumer = &args[4];
            if sub.eq_ignore_ascii_case(b"CREATECONSUMER") {
                let existed = g.consumers.contains_key(consumer);
                if !existed {
                    effects.push(Effect::Consumer(
                        name.clone(),
                        consumer.clone(),
                        Consumer {
                            seen: now,
                            active: -1,
                        },
                    ));
                }
                Resp::Integer(i64::from(!existed))
            } else {
                let count = g.consumer_count(consumer);
                if g.consumers.contains_key(consumer) {
                    effects.push(Effect::DropConsumer(name.clone(), consumer.clone()));
                }
                Resp::Integer(count as i64)
            }
        } else {
            return error("ERR unknown subcommand for XGROUP");
        };
        match self.stream_effects(key, effects, now) {
            Ok(()) => reply,
            Err(e) => e,
        }
    }
    fn cmd_xack(&mut self, args: &[Bytes], now: i64) -> Resp {
        let ids = match args[3..]
            .iter()
            .map(|a| Id::parse(a))
            .collect::<Result<BTreeSet<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => return e,
        };
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return Resp::Integer(0),
            Err(e) => return e,
        };
        let Some(g) = s.groups.get(&args[2]) else {
            return Resp::Integer(0);
        };
        let effects: Vec<_> = ids
            .into_iter()
            .filter(|id| g.pending.contains_key(id))
            .map(|id| Effect::Ack(args[2].clone(), id))
            .collect();
        let count = effects.len();
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => Resp::Integer(count as i64),
            Err(e) => e,
        }
    }
    fn cmd_xpending(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut i = 3;
        let mut idle = None;
        if args.get(i).is_some_and(|a| a.eq_ignore_ascii_case(b"IDLE")) {
            let Some(a) = args.get(i + 1) else {
                return syntax_error();
            };
            idle = match number(a) {
                Ok(n) => Some(n),
                Err(e) => return e,
            };
            i += 2;
        }
        let detail = if i == args.len() {
            None
        } else {
            if args.len() != i + 3 && args.len() != i + 4 {
                return syntax_error();
            }
            let bounds = match (
                range_bound(&args[i], false),
                range_bound(&args[i + 1], true),
            ) {
                (Ok(a), Ok(b)) => (a, b),
                (Err(e), _) | (_, Err(e)) => return e,
            };
            let count = match number(&args[i + 2]) {
                Ok(n) => n as usize,
                Err(e) => return e,
            };
            Some((bounds, count, args.get(i + 3)))
        };
        let limit = self.reply_limit;
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return nogroup(&args[1], &args[2]),
            Err(e) => return e,
        };
        let Some(g) = s.groups.get(&args[2]) else {
            return nogroup(&args[1], &args[2]);
        };
        if let Some(((lo, hi), count, consumer)) = detail {
            if !valid_bounds(&lo, &hi) {
                return array(vec![]);
            }
            return bounded_replies(
                g.pending
                    .range((lo, hi))
                    .filter(|(_, p)| {
                        consumer.is_none_or(|c| *c == p.consumer)
                            && idle.is_none_or(|n| {
                                p.nack > 0 || now.saturating_sub(p.delivered).max(0) as u64 >= n
                            })
                    })
                    .take(count)
                    .map(|(id, p)| {
                        array(vec![
                            id.reply(),
                            bulk(p.consumer.clone()),
                            Resp::Integer(if p.nack > 0 {
                                -1
                            } else {
                                now.saturating_sub(p.delivered).max(0)
                            }),
                            Resp::Integer(p.deliveries.min(i64::MAX as u64) as i64),
                        ])
                    }),
                limit,
            );
        }
        let consumers = if g.owners.is_empty() {
            Resp::Array(None)
        } else {
            array(
                g.owners
                    .iter()
                    .map(|(c, ids)| {
                        array(vec![
                            bulk(c.clone()),
                            bulk(Bytes::from(ids.len().to_string())),
                        ])
                    })
                    .collect(),
            )
        };
        bounded_replies(
            vec![
                Resp::Integer(g.pending.len() as i64),
                g.pending
                    .first_key_value()
                    .map_or(Resp::Bulk(None), |(id, _)| id.reply()),
                g.pending
                    .last_key_value()
                    .map_or(Resp::Bulk(None), |(id, _)| id.reply()),
                consumers,
            ],
            limit,
        )
    }
}

impl Engine {
    fn cmd_xclaim(&mut self, args: &[Bytes], now: i64) -> Resp {
        let minidle = match number(&args[4]) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let mut i = 5;
        let mut ids = Vec::new();
        while let Some(arg) = args.get(i) {
            match Id::parse(arg) {
                Ok(id) => {
                    ids.push(id);
                    i += 1;
                }
                Err(_) if !ids.is_empty() => break,
                Err(e) => return e,
            }
        }
        if ids.is_empty() {
            return syntax_error();
        }
        let (mut delivered, mut retries, mut force, mut justid, mut lastid) =
            (now, None, false, false, None);
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"FORCE") {
                force = true;
                i += 1;
                continue;
            }
            if args[i].eq_ignore_ascii_case(b"JUSTID") {
                justid = true;
                i += 1;
                continue;
            }
            let Some(v) = args.get(i + 1) else {
                return syntax_error();
            };
            if args[i].eq_ignore_ascii_case(b"IDLE") {
                delivered = match integer(v) {
                    Some(n) => now.saturating_sub(n.max(0)),
                    None => return integer_error(),
                };
            } else if args[i].eq_ignore_ascii_case(b"TIME") {
                delivered = match integer(v) {
                    Some(n) => n.clamp(0, now.max(0)),
                    None => return integer_error(),
                };
            } else if args[i].eq_ignore_ascii_case(b"RETRYCOUNT") {
                retries = match number(v) {
                    Ok(n) if n <= i64::MAX as u64 => Some(n),
                    _ => return integer_error(),
                };
            } else if args[i].eq_ignore_ascii_case(b"LASTID") {
                lastid = match Id::parse(v) {
                    Ok(id) => Some(id),
                    Err(e) => return e,
                };
            } else {
                return syntax_error();
            }
            i += 2;
        }
        let limit = self.reply_limit;
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return nogroup(&args[1], &args[2]),
            Err(e) => return e,
        };
        let Some(g) = s.groups.get(&args[2]) else {
            return nogroup(&args[1], &args[2]);
        };
        let mut effects = Vec::new();
        let mut rows = Vec::new();
        let mut selected = BTreeSet::new();
        for id in ids {
            if !selected.insert(id) {
                continue;
            }
            let pending = g.pending.get(&id);
            let Some(fields) = s.entries.get(&id) else {
                if pending.is_some() {
                    effects.push(Effect::Ack(args[2].clone(), id));
                }
                continue;
            };
            let p = if let Some(p) = pending {
                if p.nack == 0 && (now.saturating_sub(p.delivered).max(0) as u64) < minidle {
                    continue;
                }
                Pending {
                    consumer: args[3].clone(),
                    delivered,
                    deliveries: retries.unwrap_or_else(|| {
                        if justid {
                            p.deliveries
                        } else {
                            p.deliveries.saturating_add(1).min(i64::MAX as u64)
                        }
                    }),
                    nack: 0,
                }
            } else if force {
                Pending {
                    consumer: args[3].clone(),
                    delivered,
                    deliveries: retries.unwrap_or(if justid { 1 } else { 2 }),
                    nack: 0,
                }
            } else {
                continue;
            };
            rows.push(if justid {
                id.reply()
            } else {
                row(id, Some(fields))
            });
            effects.push(Effect::Pending(args[2].clone(), id, p));
        }
        let active = if rows.is_empty() {
            g.consumers.get(&args[3]).map_or(-1, |c| c.active)
        } else {
            now
        };
        effects.insert(
            0,
            Effect::Consumer(
                args[2].clone(),
                args[3].clone(),
                Consumer { seen: now, active },
            ),
        );
        if let Some(last) = lastid.filter(|id| *id > g.last) {
            effects.push(Effect::Group(args[2].clone(), last, None));
        }
        let reply = bounded_replies(rows, limit);
        if matches!(&reply, Resp::Error(_)) {
            return reply;
        }
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => reply,
            Err(e) => e,
        }
    }
    fn cmd_xautoclaim(&mut self, args: &[Bytes], now: i64) -> Resp {
        let minidle = match number(&args[4]) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let start = match Id::parse(&args[5]) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let (mut count, mut justid, mut i) = (100usize, false, 6);
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"JUSTID") {
                justid = true;
                i += 1;
            } else if args[i].eq_ignore_ascii_case(b"COUNT") {
                count = match args.get(i + 1).map(|a| number(a)) {
                    Some(Ok(n)) if n > 0 => usize::try_from(n).unwrap_or(usize::MAX),
                    _ => return integer_error(),
                };
                i += 2;
            } else {
                return syntax_error();
            }
        }
        let limit = self.reply_limit;
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return nogroup(&args[1], &args[2]),
            Err(e) => return e,
        };
        let Some(g) = s.groups.get(&args[2]) else {
            return nogroup(&args[1], &args[2]);
        };
        let (mut effects, mut rows, mut deleted) = (Vec::new(), Vec::new(), Vec::new());
        let mut next = Id::default();
        let mut pending = g.pending.range(start..).map(|(id, p)| (*id, p)).peekable();
        let mut scanned = 0usize;
        while let Some((id, p)) = pending.next() {
            scanned += 1;
            if !s.entries.contains_key(&id) {
                effects.push(Effect::Ack(args[2].clone(), id));
                deleted.push(id.reply());
            } else if p.nack > 0 || now.saturating_sub(p.delivered).max(0) as u64 >= minidle {
                rows.push(if justid {
                    id.reply()
                } else {
                    row(id, s.entries.get(&id))
                });
                effects.push(Effect::Pending(
                    args[2].clone(),
                    id,
                    Pending {
                        consumer: args[3].clone(),
                        delivered: now,
                        deliveries: if justid {
                            p.deliveries
                        } else {
                            p.deliveries.saturating_add(1).min(i64::MAX as u64)
                        },
                        nack: 0,
                    },
                ));
            }
            if rows.len().saturating_add(deleted.len()) >= count
                || scanned >= count.saturating_mul(10)
            {
                next = pending.peek().map_or(Id::default(), |(id, _)| *id);
                break;
            }
        }
        let active = if rows.is_empty() {
            g.consumers.get(&args[3]).map_or(-1, |c| c.active)
        } else {
            now
        };
        effects.insert(
            0,
            Effect::Consumer(
                args[2].clone(),
                args[3].clone(),
                Consumer { seen: now, active },
            ),
        );
        let reply = bounded_replies(vec![next.reply(), array(rows), array(deleted)], limit);
        if matches!(&reply, Resp::Error(_)) {
            return reply;
        }
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => reply,
            Err(e) => e,
        }
    }
    fn cmd_xsetid(&mut self, args: &[Bytes], now: i64) -> Resp {
        let last = match Id::parse(&args[2]) {
            Ok(id) => id,
            Err(e) => return e,
        };
        let (mut added, mut deleted, mut i) = (None, None, 3);
        while i < args.len() {
            let Some(v) = args.get(i + 1) else {
                return syntax_error();
            };
            if args[i].eq_ignore_ascii_case(b"ENTRIESADDED") {
                added = match number(v) {
                    Ok(n) => Some(n),
                    Err(e) => return e,
                };
            } else if args[i].eq_ignore_ascii_case(b"MAXDELETEDID") {
                deleted = match Id::parse(v) {
                    Ok(id) => Some(id),
                    Err(e) => return e,
                };
            } else {
                return syntax_error();
            }
            i += 2;
        }
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return error("ERR no such key"),
            Err(e) => return e,
        };
        if s.entries.last_key_value().is_some_and(|(id, _)| *id > last) {
            return error(
                "ERR The ID specified in XSETID is smaller than the target stream top item",
            );
        }
        if added.is_some_and(|n| n < s.entries.len() as u64) {
            return error(
                "ERR The entries_added specified in XSETID is smaller than the target stream length",
            );
        }
        if s.max_deleted > last || deleted.unwrap_or_default() > last {
            return error(
                "ERR The ID specified in XSETID is smaller than the provided max_deleted_entry_id",
            );
        }
        let mut meta = s.meta();
        if let Effect::Meta {
            last: l,
            added: a,
            deleted: d,
            ..
        } = &mut meta
        {
            *l = last;
            if let Some(n) = added {
                *a = n;
            }
            if let Some(id) = deleted.filter(|id| *id != Id::default()) {
                *d = id;
            }
        }
        match self.stream_effects(&args[1], vec![meta], now) {
            Ok(()) => ok(),
            Err(e) => e,
        }
    }
    fn cmd_xcfgset(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() < 4 || !args.len().is_multiple_of(2) {
            return syntax_error();
        }
        let (mut duration, mut size) = (None, None);
        for pair in args[2..].as_chunks::<2>().0 {
            if pair[0].eq_ignore_ascii_case(b"IDMP-DURATION") {
                if duration.is_some() {
                    return syntax_error();
                }
                duration = match unsigned(&pair[1]) {
                    Some(n) if (1..=86400).contains(&n) => Some(n),
                    _ => return error("ERR invalid duration"),
                };
            } else if pair[0].eq_ignore_ascii_case(b"IDMP-MAXSIZE") {
                if size.is_some() {
                    return syntax_error();
                }
                size = match unsigned(&pair[1]) {
                    Some(n) if (1..=10000).contains(&n) => Some(n),
                    _ => return error("ERR invalid maxsize"),
                };
            } else {
                return syntax_error();
            }
        }
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return error("ERR no such key"),
            Err(e) => return e,
        };
        let duration = duration.unwrap_or(s.duration);
        let size = size.unwrap_or(s.maxsize);
        if duration == s.duration && size == s.maxsize {
            return ok();
        }
        let effect = Effect::Config(duration, size);
        match self.stream_effects(&args[1], vec![effect], now) {
            Ok(()) => ok(),
            Err(e) => e,
        }
    }
    fn cmd_xidmprecord(&mut self, args: &[Bytes], now: i64) -> Resp {
        let id = match Id::parse(&args[4]) {
            Ok(id) => id,
            Err(e) => return e,
        };
        if args[2].is_empty() || args[3].is_empty() {
            return error("ERR producer and idempotent IDs must not be empty");
        }
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return error("ERR no such key"),
            Err(e) => return e,
        };
        if !s.entries.contains_key(&id) {
            return error("ERR stream entry does not exist");
        }
        if let Some(d) = s.producers.get(&args[2]).and_then(|p| p.get(&args[3])) {
            return if d.id == id {
                ok()
            } else {
                error("ERR conflicting idempotent ID")
            };
        }
        let mut effects = Vec::new();
        if let Some(p) = s.producers.get(&args[2]) {
            let mut rows: Vec<_> = p.iter().collect();
            rows.sort_unstable_by_key(|(_, d)| (d.at, d.id));
            let remove = rows
                .len()
                .saturating_add(1)
                .saturating_sub(s.maxsize as usize);
            for (iid, _) in rows.into_iter().take(remove) {
                effects.push(Effect::DropDedup(args[2].clone(), iid.clone()));
            }
        }
        effects.push(Effect::Dedup(
            args[2].clone(),
            args[3].clone(),
            Dedup { id, at: now },
        ));
        let mut meta = s.meta();
        if let Effect::Meta { iids, .. } = &mut meta {
            *iids = iids.saturating_add(1);
        }
        effects.push(meta);
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => ok(),
            Err(e) => e,
        }
    }
    fn cmd_xnack(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mode = &args[3];
        if !mode.eq_ignore_ascii_case(b"SILENT")
            && !mode.eq_ignore_ascii_case(b"FAIL")
            && !mode.eq_ignore_ascii_case(b"FATAL")
        {
            return syntax_error();
        }
        if !args[4].eq_ignore_ascii_case(b"IDS") {
            return syntax_error();
        }
        let count = match number(&args[5]) {
            Ok(n) if n > 0 && n <= (args.len() - 6) as u64 => n as usize,
            _ => return integer_error(),
        };
        let ids = match args[6..6 + count]
            .iter()
            .map(|a| Id::parse(a))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(v) => v,
            Err(e) => return e,
        };
        let (mut retries, mut force, mut i) = (None, false, 6 + count);
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"FORCE") {
                force = true;
                i += 1;
            } else if args[i].eq_ignore_ascii_case(b"RETRYCOUNT") {
                retries = match args.get(i + 1).and_then(|a| unsigned(a)) {
                    Some(n) if n <= i64::MAX as u64 => Some(n),
                    _ => return integer_error(),
                };
                i += 2;
            } else {
                return syntax_error();
            }
        }
        let s = match self.stream(&args[1], now) {
            Ok(Some(s)) => s,
            Ok(None) => return nogroup(&args[1], &args[2]),
            Err(e) => return e,
        };
        let Some(g) = s.groups.get(&args[2]) else {
            return nogroup(&args[1], &args[2]);
        };
        let (mut effects, mut n, mut seq, mut seen) = (Vec::new(), 0, g.nack_seq, BTreeSet::new());
        for id in ids {
            if !seen.insert(id) {
                continue;
            }
            let old = g.pending.get(&id);
            if old.is_none() && (!force || !s.entries.contains_key(&id)) {
                continue;
            }
            let base = old.map_or(0, |p| p.deliveries);
            let deliveries = retries.unwrap_or_else(|| {
                if mode.eq_ignore_ascii_case(b"FATAL") {
                    i64::MAX as u64
                } else if mode.eq_ignore_ascii_case(b"SILENT") {
                    base.saturating_sub(1)
                } else {
                    base
                }
            });
            let Some(next) = seq.checked_add(1) else {
                return error("ERR negative acknowledgement sequence exhausted");
            };
            seq = next;
            effects.push(Effect::Pending(
                args[2].clone(),
                id,
                Pending {
                    consumer: Bytes::new(),
                    delivered: 0,
                    deliveries,
                    nack: seq,
                },
            ));
            n += 1;
        }
        match self.stream_effects(&args[1], effects, now) {
            Ok(()) => Resp::Integer(n),
            Err(e) => e,
        }
    }
    fn cmd_xinfo(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[1].eq_ignore_ascii_case(b"HELP") {
            return if args.len() == 2 {
                array(
                    [
                        "STREAM key [FULL [COUNT count]]",
                        "GROUPS key",
                        "CONSUMERS key group",
                    ]
                    .into_iter()
                    .map(text)
                    .collect(),
                )
            } else {
                syntax_error()
            };
        }
        if args.len() < 3 {
            return syntax_error();
        }
        let sub = &args[1];
        let (mut full, mut count) = (false, 10usize);
        if sub.eq_ignore_ascii_case(b"STREAM") {
            if args.len() > 3 {
                if !args[3].eq_ignore_ascii_case(b"FULL") {
                    return syntax_error();
                }
                full = true;
                if args.len() > 4 {
                    if args.len() != 6 || !args[4].eq_ignore_ascii_case(b"COUNT") {
                        return syntax_error();
                    }
                    count = match number(&args[5]) {
                        Ok(0) => usize::MAX,
                        Ok(n) => n as usize,
                        Err(e) => return e,
                    };
                }
            }
        } else if sub.eq_ignore_ascii_case(b"GROUPS") {
            if args.len() != 3 {
                return syntax_error();
            }
        } else if sub.eq_ignore_ascii_case(b"CONSUMERS") {
            if args.len() != 4 {
                return syntax_error();
            }
        } else {
            return error("ERR unknown subcommand for XINFO");
        }
        let limit = self.reply_limit;
        let s = match self.stream(&args[2], now) {
            Ok(Some(s)) => s,
            Ok(None) => return error("ERR no such key"),
            Err(e) => return e,
        };
        if sub.eq_ignore_ascii_case(b"CONSUMERS") {
            let Some(g) = s.groups.get(&args[3]) else {
                return nogroup(&args[2], &args[3]);
            };
            return bounded_replies(
                g.consumers.iter().map(|(name, c)| {
                    info_map(vec![
                        text("name"),
                        bulk(name.clone()),
                        text("pending"),
                        Resp::Integer(g.consumer_count(name) as i64),
                        text("idle"),
                        Resp::Integer(now.saturating_sub(c.seen).max(0)),
                        text("inactive"),
                        Resp::Integer(if c.active < 0 {
                            -1
                        } else {
                            now.saturating_sub(c.active).max(0)
                        }),
                    ])
                }),
                limit,
            );
        }
        if sub.eq_ignore_ascii_case(b"GROUPS") {
            return bounded_replies(
                s.groups.iter().map(|(name, g)| group_info(s, name, g)),
                limit,
            );
        }
        // The packed radix allocation statistics are implementation-specific.
        // Report this implementation's ordered-index keys and payload nodes.
        let mut info = vec![
            text("length"),
            Resp::Integer(s.entries.len() as i64),
            text("radix-tree-keys"),
            Resp::Integer(s.entries.len() as i64),
            text("radix-tree-nodes"),
            Resp::Integer(s.entries.len() as i64),
            text("last-generated-id"),
            s.last.reply(),
            text("max-deleted-entry-id"),
            s.max_deleted.reply(),
            text("entries-added"),
            Resp::Integer(s.added.min(i64::MAX as u64) as i64),
            text("recorded-first-entry-id"),
            s.recorded_first.reply(),
        ];
        if full {
            info.push(text("entries"));
            info.push(array(
                s.entries
                    .iter()
                    .take(count)
                    .map(|(id, f)| row(*id, Some(f)))
                    .collect(),
            ));
            info.push(text("groups"));
            info.push(array(
                s.groups
                    .iter()
                    .map(|(name, g)| {
                        let mut info = group_info_fields(s, name, g);
                        info.extend([
                            text("pel-count"),
                            Resp::Integer(g.pending.len() as i64),
                            text("nacked-count"),
                            Resp::Integer(g.nacked.len() as i64),
                            text("pending"),
                            array(
                                g.pending
                                    .iter()
                                    .take(count)
                                    .map(|(id, p)| {
                                        array(vec![
                                            id.reply(),
                                            bulk(p.consumer.clone()),
                                            Resp::Integer(p.delivered),
                                            Resp::Integer(p.deliveries.min(i64::MAX as u64) as i64),
                                        ])
                                    })
                                    .collect(),
                            ),
                        ]);
                        let consumers = array(
                            g.consumers
                                .iter()
                                .map(|(n, c)| {
                                    info_map(vec![
                                        text("name"),
                                        bulk(n.clone()),
                                        text("seen-time"),
                                        Resp::Integer(c.seen),
                                        text("active-time"),
                                        Resp::Integer(c.active),
                                        text("pel-count"),
                                        Resp::Integer(g.consumer_count(n) as i64),
                                        text("pending"),
                                        array(
                                            g.owners
                                                .get(n)
                                                .into_iter()
                                                .flat_map(|ids| ids.iter())
                                                .take(count)
                                                .map(|id| {
                                                    let p = &g.pending[id];
                                                    array(vec![
                                                        id.reply(),
                                                        Resp::Integer(p.delivered),
                                                        Resp::Integer(
                                                            p.deliveries.min(i64::MAX as u64)
                                                                as i64,
                                                        ),
                                                    ])
                                                })
                                                .collect(),
                                        ),
                                    ])
                                })
                                .collect(),
                        );
                        // FULL uses the consumer array rather than GROUPS' consumer count.
                        if let Some(i) = info.iter().position(
                            |v| matches!(v,Resp::Bulk(Some(b))if b.as_ref()==b"consumers"),
                        ) {
                            info[i + 1] = consumers;
                        }
                        info_map(info)
                    })
                    .collect(),
            ));
        } else {
            info.extend([
                text("groups"),
                Resp::Integer(s.groups.len() as i64),
                text("first-entry"),
                s.entries
                    .first_key_value()
                    .map_or(Resp::Bulk(None), |(id, f)| row(*id, Some(f))),
                text("last-entry"),
                s.entries
                    .last_key_value()
                    .map_or(Resp::Bulk(None), |(id, f)| row(*id, Some(f))),
            ]);
        }
        let mut pids = 0;
        let mut iids = 0;
        for p in s.producers.values() {
            let count = p
                .values()
                .filter(|d| now.saturating_sub(d.at) < (s.duration * 1000) as i64)
                .count();
            if count > 0 {
                pids += 1;
                iids += count;
            }
        }
        info.extend([
            text("idmp-duration"),
            Resp::Integer(s.duration as i64),
            text("idmp-maxsize"),
            Resp::Integer(s.maxsize as i64),
            text("pids-tracked"),
            Resp::Integer(pids),
            text("iids-tracked"),
            Resp::Integer(iids as i64),
            text("iids-added"),
            Resp::Integer(s.iids_added.min(i64::MAX as u64) as i64),
            text("iids-duplicates"),
            Resp::Integer(s.duplicates.min(i64::MAX as u64) as i64),
        ]);
        bounded_response(info_map(info), limit)
    }
}
fn group_info(s: &StreamValue, name: &Bytes, g: &Group) -> Resp {
    info_map(group_info_fields(s, name, g))
}
fn group_info_fields(s: &StreamValue, name: &Bytes, g: &Group) -> Vec<Resp> {
    vec![
        text("name"),
        bulk(name.clone()),
        text("consumers"),
        Resp::Integer(g.consumers.len() as i64),
        text("pending"),
        Resp::Integer(g.pending.len() as i64),
        text("last-delivered-id"),
        g.last.reply(),
        text("entries-read"),
        g.read.map_or(Resp::Bulk(None), |n| {
            Resp::Integer(n.min(i64::MAX as u64) as i64)
        }),
        text("lag"),
        s.lag(g).map_or(Resp::Bulk(None), |n| {
            Resp::Integer(n.min(i64::MAX as u64) as i64)
        }),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestEngine {
        engine: Engine,
        journal: Vec<crate::journal::Batch>,
    }
    impl TestEngine {
        fn new() -> Self {
            Self {
                engine: Engine::new(),
                journal: Vec::new(),
            }
        }
    }
    impl std::ops::Deref for TestEngine {
        type Target = Engine;
        fn deref(&self) -> &Engine {
            &self.engine
        }
    }
    impl std::ops::DerefMut for TestEngine {
        fn deref_mut(&mut self) -> &mut Engine {
            &mut self.engine
        }
    }

    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }
    fn run(engine: &mut TestEngine, now: i64, values: &[&str]) -> Resp {
        let execution = engine.execute_at_time(&args(values), now).unwrap();
        if let Some(batch) = engine.take_batch() {
            engine.journal.push(batch);
        }
        match execution {
            Execution::Reply(reply) | Execution::Pushed { reply, .. } => reply,
            _ => panic!("unexpected blocked execution"),
        }
    }
    fn list(reply: Resp) -> Vec<Resp> {
        let Resp::Array(Some(rows)) = reply else {
            panic!("expected array, got {reply:?}")
        };
        rows
    }
    fn stream_rows(reply: Resp) -> Vec<Resp> {
        let pair = list(list(reply).into_iter().next().unwrap());
        list(pair.into_iter().nth(1).unwrap())
    }
    fn stream(engine: &mut TestEngine) -> &StreamValue {
        engine
            .stream(&Bytes::from_static(b"s"), 1000)
            .unwrap()
            .unwrap()
    }
    fn prepare(engine: &mut TestEngine) {
        assert_eq!(
            run(engine, 1000, &["XADD", "s", "1-0", "f", "a"]),
            Id(1, 0).reply()
        );
        assert_eq!(
            run(engine, 1000, &["XADD", "s", "2-0", "f", "b"]),
            Id(2, 0).reply()
        );
        assert_eq!(
            run(engine, 1000, &["XGROUP", "CREATE", "s", "g", "0"]),
            ok()
        );
    }

    #[test]
    fn ids_keep_all_128_bits_and_resolve_clock_rollback() {
        let mut e = TestEngine::new();
        assert_eq!(
            run(&mut e, 0, &["XADD", "s", "0-*", "f", "v"]),
            Id(0, 1).reply()
        );
        assert_eq!(
            run(&mut e, 1001, &["XADD", "s", "*", "f", "v"]),
            Id(1001, 0).reply()
        );
        assert_eq!(
            run(&mut e, 1000, &["XADD", "s", "*", "f", "v"]),
            Id(1001, 1).reply()
        );
        assert!(matches!(
            run(&mut e, 1000, &["XADD", "s", "1000-*", "f", "v"]),
            Resp::Error(_)
        ));
        assert_eq!(
            run(
                &mut e,
                0,
                &[
                    "XADD",
                    "top",
                    "18446744073709551615-18446744073709551614",
                    "f",
                    "v"
                ]
            ),
            Id(u64::MAX, u64::MAX - 1).reply()
        );
        assert_eq!(
            run(&mut e, 0, &["XADD", "top", "*", "f", "v"]),
            Id::MAX.reply()
        );
        assert!(matches!(
            run(&mut e, 0, &["XADD", "top", "*", "f", "v"]),
            Resp::Error(_)
        ));
        assert!(Id::parse(b"18446744073709551616-0").is_err());
        assert!(Id::parse(b"1-2-3").is_err());
    }

    #[test]
    fn ranges_preserve_duplicate_fields_binary_bytes_and_exclusive_bounds() {
        let mut e = TestEngine::new();
        let fields = vec![
            Bytes::from_static(b"field"),
            Bytes::from_static(b"\0\xff"),
            Bytes::from_static(b"field"),
            Bytes::from_static(b"again"),
        ];
        let mut command = args(&["XADD", "s", "10-1"]);
        command.extend(fields.iter().cloned());
        assert!(matches!(
            e.execute_at_time(&command, 1000).unwrap(),
            Execution::Reply(_)
        ));
        run(&mut e, 1000, &["XADD", "s", "10-2", "f", "b"]);
        run(&mut e, 1000, &["XADD", "s", "11-0", "f", "c"]);
        assert_eq!(
            run(&mut e, 1000, &["XRANGE", "s", "10", "(10-2"]),
            array(vec![row(Id(10, 1), Some(&fields))])
        );
        let rows = list(run(
            &mut e,
            1000,
            &["XREVRANGE", "s", "+", "(10-1", "COUNT", "2"],
        ));
        assert_eq!(
            list(rows.into_iter().next().unwrap()).remove(0),
            Id(11, 0).reply()
        );
        assert_eq!(
            run(&mut e, 1000, &["XRANGE", "s", "20", "10"]),
            array(vec![])
        );
    }

    #[test]
    fn exact_approximate_limit_and_minid_trims_keep_tombstones_distinct() {
        let mut e = TestEngine::new();
        for n in 1..=205 {
            run(&mut e, 1000, &["XADD", "s", &format!("{n}-0"), "f", "v"]);
        }
        assert_eq!(
            run(
                &mut e,
                1000,
                &["XTRIM", "s", "MAXLEN", "~", "100", "LIMIT", "100"]
            ),
            Resp::Integer(100)
        );
        assert_eq!(run(&mut e, 1000, &["XLEN", "s"]), Resp::Integer(105));
        assert_eq!(
            run(&mut e, 1000, &["XTRIM", "s", "MAXLEN", "=", "100"]),
            Resp::Integer(5)
        );
        assert_eq!(
            run(&mut e, 1000, &["XTRIM", "s", "MINID", "108"]),
            Resp::Integer(2)
        );
        assert_eq!(stream(&mut e).recorded_first, Id(108, 0));
        assert_eq!(stream(&mut e).max_deleted, Id::default());
        assert!(matches!(
            run(&mut e, 1000, &["XTRIM", "s", "MAXLEN", "100", "LIMIT", "1"]),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut e, 1000, &["XDEL", "s", "110-0"]), Resp::Integer(1));
        assert_eq!(stream(&mut e).max_deleted, Id(110, 0));
    }

    #[test]
    fn deleted_pending_history_is_null_and_autoclaim_removes_dangling_pel() {
        let mut e = TestEngine::new();
        prepare(&mut e);
        assert_eq!(
            stream_rows(run(
                &mut e,
                1000,
                &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"]
            ))
            .len(),
            2
        );
        run(&mut e, 1000, &["XDEL", "s", "1-0"]);
        let history = stream_rows(run(
            &mut e,
            1100,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "NOACK",
                "STREAMS",
                "s",
                "0",
            ],
        ));
        assert_eq!(history[0], row(Id(1, 0), None));
        assert_eq!(
            stream(&mut e).groups[b"g".as_slice()].pending[&Id(2, 0)].deliveries,
            2
        );
        let claimed = list(run(
            &mut e,
            1200,
            &["XAUTOCLAIM", "s", "g", "b", "0", "0", "COUNT", "10"],
        ));
        assert_eq!(claimed[2], array(vec![Id(1, 0).reply()]));
        assert!(
            !stream(&mut e).groups[b"g".as_slice()]
                .pending
                .contains_key(&Id(1, 0))
        );
        assert_eq!(
            stream(&mut e).groups[b"g".as_slice()].consumer_count(&Bytes::from_static(b"b")),
            1
        );
    }

    #[test]
    fn reference_policies_ackdel_and_noack_update_real_pending_state() {
        let mut e = TestEngine::new();
        prepare(&mut e);
        run(&mut e, 1000, &["XGROUP", "CREATE", "s", "other", "0"]);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        assert_eq!(
            run(
                &mut e,
                1000,
                &["XACKDEL", "s", "g", "ACKED", "IDS", "1", "1-0"]
            ),
            array(vec![Resp::Integer(2)])
        );
        assert!(
            !stream(&mut e).groups[b"g".as_slice()]
                .pending
                .contains_key(&Id(1, 0))
        );
        run(
            &mut e,
            1000,
            &[
                "XREADGROUP",
                "GROUP",
                "other",
                "b",
                "NOACK",
                "STREAMS",
                "s",
                ">",
            ],
        );
        assert_eq!(
            run(&mut e, 1000, &["XDELEX", "s", "ACKED", "IDS", "1", "1-0"]),
            array(vec![Resp::Integer(1)])
        );
        run(&mut e, 1000, &["XDEL", "s", "2-0"]);
        assert_eq!(
            run(&mut e, 1000, &["XDELEX", "s", "DELREF", "IDS", "1", "2-0"]),
            array(vec![Resp::Integer(-1)])
        );
        assert!(stream(&mut e).groups[b"g".as_slice()].pending.is_empty());
        assert!(
            stream(&mut e).groups[b"other".as_slice()]
                .pending
                .is_empty()
        );
        assert_eq!(
            run(
                &mut e,
                1000,
                &["XACKDEL", "s", "missing", "IDS", "1", "1-0"]
            ),
            array(vec![Resp::Integer(-1)])
        );
    }

    #[test]
    fn acked_trimming_skips_pending_entries_and_respects_thresholds_and_scan_limits() {
        let cases: &[(&[&str], Vec<u64>)] = &[
            (&["MAXLEN", "=", "0"], vec![1]),
            (
                &["MAXLEN", "=", "100"],
                std::iter::once(1).chain(7..=105).collect(),
            ),
            (
                &["MINID", "=", "4-0"],
                std::iter::once(1).chain(4..=105).collect(),
            ),
            (
                &["MAXLEN", "~", "0", "LIMIT", "0"],
                std::iter::once(1).chain(102..=105).collect(),
            ),
            (
                &["MAXLEN", "~", "0", "LIMIT", "3"],
                std::iter::once(1).chain(4..=105).collect(),
            ),
            (
                &["MINID", "~", "4-0", "LIMIT", "2"],
                std::iter::once(1).chain(3..=105).collect(),
            ),
            (&["MAXLEN", "~", "0", "LIMIT", "1"], (1..=105).collect()),
        ];
        for (options, retained) in cases {
            let mut e = TestEngine::new();
            e.set_recording(true);
            for n in 1..=105 {
                run(&mut e, 1000, &["XADD", "s", &format!("{n}-0"), "f", "v"]);
            }
            run(&mut e, 1000, &["XGROUP", "CREATE", "s", "g", "0"]);
            run(
                &mut e,
                1000,
                &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
            );
            for n in 2..=105 {
                assert_eq!(
                    run(&mut e, 1000, &["XACK", "s", "g", &format!("{n}-0")]),
                    Resp::Integer(1)
                );
            }
            let mut command = vec!["XTRIM", "s"];
            command.extend_from_slice(options);
            command.push("ACKED");
            assert_eq!(
                run(&mut e, 1000, &command),
                Resp::Integer((105 - retained.len()) as i64),
                "{options:?}"
            );
            let expected = array(
                retained
                    .iter()
                    .map(|n| row(Id(*n, 0), Some(&args(&["f", "v"]))))
                    .collect(),
            );
            assert_eq!(run(&mut e, 1000, &["XRANGE", "s", "-", "+"]), expected);
            assert_eq!(
                run(&mut e, 1000, &["XPENDING", "s", "g"]),
                array(vec![
                    Resp::Integer(1),
                    Id(1, 0).reply(),
                    Id(1, 0).reply(),
                    array(vec![array(vec![
                        bulk(Bytes::from_static(b"c")),
                        bulk(Bytes::from_static(b"1")),
                    ])]),
                ])
            );
            let mut recovered = replay_stream(&mut e);
            assert_eq!(
                run(&mut recovered, 1000, &["XRANGE", "s", "-", "+"]),
                expected
            );
        }
    }

    #[test]
    fn xadd_acked_trimming_skips_pending_entries_without_deleting_unread_addition() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        prepare(&mut e);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
        );
        run(&mut e, 1000, &["XACK", "s", "g", "2-0"]);
        assert_eq!(
            run(
                &mut e,
                1000,
                &["XADD", "s", "MAXLEN", "0", "ACKED", "3-0", "f", "c"],
            ),
            Id(3, 0).reply()
        );
        let expected = array(vec![
            row(Id(1, 0), Some(&args(&["f", "a"]))),
            row(Id(3, 0), Some(&args(&["f", "c"]))),
        ]);
        assert_eq!(run(&mut e, 1000, &["XRANGE", "s", "-", "+"]), expected);
        let mut recovered = replay_stream(&mut e);
        assert_eq!(
            run(&mut recovered, 1000, &["XRANGE", "s", "-", "+"]),
            expected
        );
        let pending = list(run(
            &mut recovered,
            1000,
            &["XPENDING", "s", "g", "-", "+", "10"],
        ));
        assert_eq!(pending.len(), 1);
        let pending = list(pending.into_iter().next().unwrap());
        assert_eq!(pending[0], Id(1, 0).reply());
        assert_eq!(pending[1], bulk(Bytes::from_static(b"c")));
        assert_eq!(pending[3], Resp::Integer(1));
    }

    #[test]
    fn duplicate_reference_policies_are_rejected_without_stream_or_journal_mutation() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        prepare(&mut e);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
        );
        let before = stream(&mut e)
            .snapshot()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let journal_len = e.journal.len();
        for first in ["KEEPREF", "DELREF", "ACKED"] {
            for second in ["KEEPREF", "DELREF", "ACKED"] {
                for options in [
                    vec![first, second, "MAXLEN", "0"],
                    vec![first, "MAXLEN", "0", second],
                    vec!["MAXLEN", "0", first, second],
                ] {
                    for verb in ["XADD", "XTRIM"] {
                        let mut command = vec![verb, "s"];
                        command.extend_from_slice(&options);
                        if verb == "XADD" {
                            command.extend_from_slice(&["3-0", "f", "new"]);
                        }
                        assert_eq!(run(&mut e, 1000, &command), syntax_error(), "{command:?}");
                        assert_eq!(e.journal.len(), journal_len, "{command:?}");
                        assert_eq!(
                            stream(&mut e)
                                .snapshot()
                                .collect::<io::Result<Vec<_>>>()
                                .unwrap(),
                            before,
                            "{command:?}"
                        );
                    }
                }
            }
        }
        let mut recovered = replay_stream(&mut e);
        assert_eq!(
            run(&mut recovered, 1000, &["XRANGE", "s", "-", "+"]),
            array(vec![
                row(Id(1, 0), Some(&args(&["f", "a"]))),
                row(Id(2, 0), Some(&args(&["f", "b"]))),
            ])
        );
        let pending = list(run(
            &mut recovered,
            1000,
            &["XPENDING", "s", "g", "-", "+", "10"],
        ));
        assert_eq!(pending.len(), 2);
        for (index, pending) in pending.into_iter().enumerate() {
            let pending = list(pending);
            assert_eq!(pending[0], Id(index as u64 + 1, 0).reply());
            assert_eq!(pending[1], bulk(Bytes::from_static(b"c")));
            assert_eq!(pending[3], Resp::Integer(1));
        }
    }

    #[test]
    fn force_claim_counts_initial_delivery_and_claim_unless_justid_or_retrycount() {
        for justid in [false, true] {
            for retrycount in [None, Some("0"), Some("7")] {
                let mut e = TestEngine::new();
                e.set_recording(true);
                prepare(&mut e);
                let mut command = vec!["XCLAIM", "s", "g", "c", "0", "1-0", "FORCE"];
                if justid {
                    command.push("JUSTID");
                }
                if let Some(count) = retrycount {
                    command.extend_from_slice(&["RETRYCOUNT", count]);
                }
                assert_eq!(
                    run(&mut e, 1100, &command),
                    array(vec![if justid {
                        Id(1, 0).reply()
                    } else {
                        row(Id(1, 0), Some(&args(&["f", "a"])))
                    }])
                );
                let expected_count = retrycount
                    .map(|n| n.parse::<i64>().unwrap())
                    .unwrap_or(if justid { 1 } else { 2 });
                let mut recovered = replay_stream(&mut e);
                for engine in [&mut e, &mut recovered] {
                    let pending = list(run(engine, 1200, &["XPENDING", "s", "g", "-", "+", "10"]));
                    assert_eq!(pending.len(), 1);
                    let pending = list(pending.into_iter().next().unwrap());
                    assert_eq!(pending[0], Id(1, 0).reply());
                    assert_eq!(pending[1], bulk(Bytes::from_static(b"c")));
                    assert_eq!(pending[3], Resp::Integer(expected_count));
                    assert_eq!(
                        run(engine, 1200, &["XCLAIM", "s", "g", "next", "0", "1-0"]),
                        array(vec![row(Id(1, 0), Some(&args(&["f", "a"])))])
                    );
                    let pending = list(run(engine, 1200, &["XPENDING", "s", "g", "-", "+", "10"]));
                    let pending = list(pending.into_iter().next().unwrap());
                    assert_eq!(pending[1], bulk(Bytes::from_static(b"next")));
                    assert_eq!(pending[3], Resp::Integer(expected_count + 1));
                }
            }
        }
    }

    #[test]
    fn claim_idle_retrycount_justid_and_nack_fifo_have_distinct_transitions() {
        let mut e = TestEngine::new();
        prepare(&mut e);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        assert_eq!(
            run(&mut e, 1050, &["XCLAIM", "s", "g", "b", "100", "1-0"]),
            array(vec![])
        );
        assert_eq!(
            run(
                &mut e,
                1100,
                &["XCLAIM", "s", "g", "b", "100", "1-0", "JUSTID"]
            ),
            array(vec![Id(1, 0).reply()])
        );
        assert_eq!(
            stream(&mut e).groups[b"g".as_slice()].pending[&Id(1, 0)].deliveries,
            1
        );
        assert_eq!(
            run(
                &mut e,
                1100,
                &["XNACK", "s", "g", "SILENT", "IDS", "2", "2-0", "1-0"]
            ),
            Resp::Integer(2)
        );
        let pending = list(run(&mut e, 1100, &["XPENDING", "s", "g", "-", "+", "10"]));
        assert_eq!(
            list(pending.into_iter().next().unwrap())[2],
            Resp::Integer(-1)
        );
        let rows = stream_rows(run(
            &mut e,
            1100,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "CLAIM",
                "999999999",
                "STREAMS",
                "s",
                ">",
            ],
        ));
        assert_eq!(list(rows.into_iter().next().unwrap())[0], Id(2, 0).reply());
        let g = &stream(&mut e).groups[b"g".as_slice()];
        assert!(g.nacked.is_empty());
        assert_eq!(g.consumer_count(&Bytes::from_static(b"a")), 0);
        assert_eq!(g.consumer_count(&Bytes::from_static(b"c")), 2);
    }

    fn replay_stream(source: &mut TestEngine) -> TestEngine {
        let mut recovered = TestEngine::new();
        for batch in source.journal.drain(..) {
            recovered.replay(batch).unwrap();
        }
        assert_eq!(
            stream(&mut recovered)
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap(),
            stream(source)
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap()
        );
        recovered
    }
    fn info_field<'a>(reply: &'a Resp, name: &str) -> &'a Resp {
        let Resp::Map(fields) = reply else {
            panic!("expected info map, got {reply:?}")
        };
        &fields
            .iter()
            .find(
                |(key, _)| matches!(key, Resp::Bulk(Some(key)) if key.as_ref() == name.as_bytes()),
            )
            .unwrap()
            .1
    }

    #[test]
    fn autoclaim_pages_nacked_and_owned_entries_in_id_order() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        prepare(&mut e);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        let identity = stream(&mut e).groups[b"g".as_slice()].identity;
        assert_eq!(
            run(
                &mut e,
                1100,
                &["XNACK", "s", "g", "FAIL", "IDS", "2", "2-0", "1-0"]
            ),
            Resp::Integer(2)
        );
        let first = list(run(
            &mut e,
            1200,
            &["XAUTOCLAIM", "s", "g", "b", "0", "0-0", "COUNT", "1"],
        ));
        assert_eq!(first[0], Id(2, 0).reply());
        assert_eq!(
            first[1],
            array(vec![row(Id(1, 0), Some(&args(&["f", "a"])))])
        );
        let second = list(run(
            &mut e,
            1300,
            &["XAUTOCLAIM", "s", "g", "b", "0", "2-0", "COUNT", "1"],
        ));
        assert_eq!(second[0], Id::default().reply());
        assert_eq!(
            second[1],
            array(vec![row(Id(2, 0), Some(&args(&["f", "b"])))])
        );
        let exhausted = list(run(
            &mut e,
            1400,
            &["XAUTOCLAIM", "s", "g", "b", "0", "2-1", "COUNT", "1"],
        ));
        assert_eq!(
            exhausted,
            vec![Id::default().reply(), array(vec![]), array(vec![])]
        );
        let g = &stream(&mut e).groups[b"g".as_slice()];
        assert_eq!(g.identity, identity);
        assert_eq!(g.last, Id(2, 0));
        assert_eq!(g.read, Some(2));
        assert_eq!(g.nack_seq, 2);
        assert_eq!(g.pending[&Id(1, 0)].delivered, 1200);
        assert_eq!(g.pending[&Id(2, 0)].delivered, 1300);
        assert_eq!(g.pending[&Id(1, 0)].deliveries, 2);
        assert_eq!(g.pending[&Id(2, 0)].deliveries, 2);
        let mut recovered = replay_stream(&mut e);
        assert_eq!(
            run(
                &mut recovered,
                1500,
                &["XPENDING", "s", "g", "-", "+", "10"]
            ),
            run(&mut e, 1500, &["XPENDING", "s", "g", "-", "+", "10"])
        );
    }

    #[test]
    fn autoclaim_deleted_entries_use_count_and_idle_entries_use_scan_limit() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        prepare(&mut e);
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        assert_eq!(
            run(&mut e, 1000, &["XDEL", "s", "1-0", "2-0"]),
            Resp::Integer(2)
        );
        assert_eq!(
            run(
                &mut e,
                1100,
                &["XAUTOCLAIM", "s", "g", "b", "0", "0-0", "COUNT", "1"]
            ),
            array(vec![
                Id(2, 0).reply(),
                array(vec![]),
                array(vec![Id(1, 0).reply()])
            ])
        );
        assert_eq!(
            run(
                &mut e,
                1200,
                &["XAUTOCLAIM", "s", "g", "b", "0", "2-0", "COUNT", "1"]
            ),
            array(vec![
                Id::default().reply(),
                array(vec![]),
                array(vec![Id(2, 0).reply()])
            ])
        );
        assert_eq!(
            list(run(&mut e, 1200, &["XPENDING", "s", "g"]))[0],
            Resp::Integer(0)
        );
        let mut recovered = replay_stream(&mut e);
        assert_eq!(
            run(&mut recovered, 1300, &["XINFO", "CONSUMERS", "s", "g"]),
            run(&mut e, 1300, &["XINFO", "CONSUMERS", "s", "g"])
        );

        let mut e = TestEngine::new();
        prepare(&mut e);
        for n in 3..=12 {
            run(&mut e, 1000, &["XADD", "s", &format!("{n}-0"), "f", "v"]);
        }
        run(
            &mut e,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        assert_eq!(
            run(
                &mut e,
                1050,
                &["XAUTOCLAIM", "s", "g", "b", "100", "0-0", "COUNT", "1"]
            ),
            array(vec![Id(11, 0).reply(), array(vec![]), array(vec![])])
        );
        assert_eq!(
            run(
                &mut e,
                1050,
                &["XAUTOCLAIM", "s", "g", "b", "100", "11-0", "COUNT", "1"]
            ),
            array(vec![Id::default().reply(), array(vec![]), array(vec![])])
        );
        assert_eq!(
            list(run(&mut e, 1050, &["XPENDING", "s", "g"]))[0],
            Resp::Integer(12)
        );
    }

    #[test]
    fn valid_empty_claim_attempts_refresh_seen_without_changing_active_or_pending() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        assert_eq!(
            run(
                &mut e,
                1000,
                &["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"]
            ),
            ok()
        );
        assert_eq!(
            run(&mut e, 1100, &["XCLAIM", "s", "g", "worker", "1000", "1-0"]),
            array(vec![])
        );
        let consumers = list(run(&mut e, 1150, &["XINFO", "CONSUMERS", "s", "g"]));
        assert_eq!(consumers.len(), 1);
        assert_eq!(info_field(&consumers[0], "name"), &text("worker"));
        assert_eq!(info_field(&consumers[0], "idle"), &Resp::Integer(50));
        assert_eq!(info_field(&consumers[0], "inactive"), &Resp::Integer(-1));
        assert_eq!(
            run(
                &mut e,
                1200,
                &["XAUTOCLAIM", "s", "g", "auto", "1000", "0-0"]
            ),
            array(vec![Id::default().reply(), array(vec![]), array(vec![])])
        );
        let consumers = list(run(&mut e, 1250, &["XINFO", "CONSUMERS", "s", "g"]));
        assert_eq!(consumers.len(), 2);
        assert_eq!(info_field(&consumers[0], "name"), &text("auto"));
        assert_eq!(info_field(&consumers[0], "idle"), &Resp::Integer(50));
        assert_eq!(info_field(&consumers[0], "inactive"), &Resp::Integer(-1));
        run(&mut e, 1300, &["XADD", "s", "1-0", "f", "v"]);
        run(
            &mut e,
            1300,
            &["XREADGROUP", "GROUP", "g", "worker", "STREAMS", "s", ">"],
        );
        assert_eq!(
            run(&mut e, 1350, &["XCLAIM", "s", "g", "worker", "1000", "1-0"]),
            array(vec![])
        );
        assert_eq!(
            run(
                &mut e,
                1400,
                &["XAUTOCLAIM", "s", "g", "worker", "1000", "0-0"]
            ),
            array(vec![Id::default().reply(), array(vec![]), array(vec![])])
        );
        let consumers = list(run(&mut e, 1450, &["XINFO", "CONSUMERS", "s", "g"]));
        assert_eq!(info_field(&consumers[1], "idle"), &Resp::Integer(50));
        assert_eq!(info_field(&consumers[1], "inactive"), &Resp::Integer(150));
        assert_eq!(
            run(&mut e, 1450, &["XPENDING", "s", "g", "-", "+", "10"]),
            array(vec![array(vec![
                Id(1, 0).reply(),
                text("worker"),
                Resp::Integer(150),
                Resp::Integer(1)
            ])])
        );
        let before = stream(&mut e)
            .snapshot()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert!(matches!(
            run(
                &mut e,
                1500,
                &["XCLAIM", "s", "g", "invalid", "0", "1-0", "UNKNOWN"]
            ),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut e,
                1500,
                &["XAUTOCLAIM", "s", "g", "invalid", "0", "0-0", "COUNT", "0"]
            ),
            Resp::Error(_)
        ));
        e.set_reply_limit(1);
        assert!(matches!(
            run(&mut e, 1500, &["XCLAIM", "s", "g", "limited", "0", "1-0"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(
                &mut e,
                1500,
                &["XAUTOCLAIM", "s", "g", "limited", "0", "0-0"]
            ),
            Resp::Error(_)
        ));
        e.set_reply_limit(1024 * 1024);
        assert_eq!(
            stream(&mut e)
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap(),
            before
        );
        let mut recovered = replay_stream(&mut e);
        assert_eq!(
            run(&mut recovered, 1450, &["XINFO", "CONSUMERS", "s", "g"]),
            run(&mut e, 1450, &["XINFO", "CONSUMERS", "s", "g"])
        );
    }

    #[test]
    fn readgroup_claim_fresh_rows_have_metadata_and_budget_is_atomic() {
        let mut e = TestEngine::new();
        e.set_recording(true);
        prepare(&mut e);
        let identity = stream(&mut e).groups[b"g".as_slice()].identity;
        let fresh = stream_rows(run(
            &mut e,
            1000,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "CLAIM",
                "1000",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                ">",
            ],
        ));
        assert_eq!(
            fresh,
            vec![array(vec![
                Id(1, 0).reply(),
                array(args(&["f", "a"]).into_iter().map(bulk).collect()),
                Resp::Integer(0),
                Resp::Integer(0)
            ])]
        );
        let fresh = stream_rows(run(
            &mut e,
            1100,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "CLAIM",
                "1000",
                "NOACK",
                "MAXSIZE",
                "1",
                "STREAMS",
                "s",
                ">",
            ],
        ));
        assert_eq!(
            fresh,
            vec![array(vec![
                Id(2, 0).reply(),
                array(args(&["f", "b"]).into_iter().map(bulk).collect()),
                Resp::Integer(0),
                Resp::Integer(0)
            ])]
        );
        assert_eq!(
            stream(&mut e).groups[b"g".as_slice()].consumer_count(&Bytes::from_static(b"a")),
            1
        );
        run(&mut e, 1200, &["XADD", "s", "3-0", "f", "c"]);
        let before = stream(&mut e)
            .snapshot()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        e.set_reply_limit(9 * std::mem::size_of::<Resp>() + 6);
        assert!(matches!(
            run(
                &mut e,
                1200,
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "b",
                    "CLAIM",
                    "1000",
                    "STREAMS",
                    "s",
                    ">"
                ]
            ),
            Resp::Error(_)
        ));
        e.set_reply_limit(1024 * 1024);
        assert_eq!(
            stream(&mut e)
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap(),
            before
        );
        assert_eq!(stream(&mut e).groups[b"g".as_slice()].identity, identity);
        let mut recovered = replay_stream(&mut e);
        let command = &[
            "XREADGROUP",
            "GROUP",
            "g",
            "b",
            "CLAIM",
            "1000",
            "STREAMS",
            "s",
            ">",
        ];
        assert_eq!(
            run(&mut recovered, 2000, command),
            run(&mut e, 2000, command)
        );

        let mut capped = TestEngine::new();
        prepare(&mut capped);
        let fresh = stream_rows(run(
            &mut capped,
            1000,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "CLAIM",
                "1000",
                "MAXSIZE",
                "43",
                "STREAMS",
                "s",
                ">",
            ],
        ));
        assert_eq!(fresh.len(), 1);
        assert_eq!(stream(&mut capped).groups[b"g".as_slice()].last, Id(1, 0));
    }

    #[test]
    fn cumulative_caps_do_not_deliver_excluded_messages_and_wrongtypes_are_atomic() {
        let mut e = TestEngine::new();
        prepare(&mut e);
        run(&mut e, 1000, &["XADD", "t", "1-0", "f", "c"]);
        run(&mut e, 1000, &["XGROUP", "CREATE", "t", "g", "0"]);
        let reply = run(
            &mut e,
            1000,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "MAXCOUNT",
                "1",
                "STREAMS",
                "s",
                "t",
                ">",
                ">",
            ],
        );
        assert_eq!(list(reply).len(), 1);
        assert_eq!(stream(&mut e).groups[b"g".as_slice()].pending.len(), 1);
        let t = e.stream(&Bytes::from_static(b"t"), 1000).unwrap().unwrap();
        assert!(t.groups[b"g".as_slice()].pending.is_empty());
        run(&mut e, 1000, &["SET", "bad", "v"]);
        assert!(matches!(
            run(
                &mut e,
                1000,
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "a",
                    "STREAMS",
                    "s",
                    "bad",
                    ">",
                    ">"
                ]
            ),
            Resp::Error(_)
        ));
        assert_eq!(stream(&mut e).groups[b"g".as_slice()].last, Id(1, 0));
        e.set_reply_limit(1);
        assert!(matches!(
            run(
                &mut e,
                1000,
                &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"]
            ),
            Resp::Error(_)
        ));
        e.set_reply_limit(1024 * 1024);
        assert_eq!(stream(&mut e).groups[b"g".as_slice()].last, Id(1, 0));
        assert_eq!(
            stream_rows(run(
                &mut e,
                1000,
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "a",
                    "MAXSIZE",
                    "1",
                    "STREAMS",
                    "s",
                    ">"
                ]
            ))
            .len(),
            1
        );
    }

    #[test]
    fn idempotency_is_content_based_bounded_expires_and_same_config_preserves_it() {
        let mut e = TestEngine::new();
        let first = run(&mut e, 1000, &["XADD", "s", "IDMPAUTO", "p", "*", "f", "v"]);
        assert_eq!(
            run(&mut e, 1001, &["XADD", "s", "IDMPAUTO", "p", "*", "f", "v"]),
            first
        );
        assert_eq!(run(&mut e, 1001, &["XLEN", "s"]), Resp::Integer(1));
        run(&mut e, 1001, &["XCFGSET", "s", "IDMP-DURATION", "100"]);
        assert_eq!(
            run(&mut e, 1002, &["XADD", "s", "IDMPAUTO", "p", "*", "f", "v"]),
            first
        );
        run(
            &mut e,
            1002,
            &["XCFGSET", "s", "IDMP-DURATION", "1", "IDMP-MAXSIZE", "1"],
        );
        let a = run(
            &mut e,
            1003,
            &["XADD", "s", "IDMP", "p", "a", "*", "f", "v"],
        );
        let b = run(
            &mut e,
            1004,
            &["XADD", "s", "IDMP", "p", "b", "*", "f", "v"],
        );
        assert_ne!(a, b);
        let anew = run(
            &mut e,
            1005,
            &["XADD", "s", "IDMP", "p", "a", "*", "f", "v"],
        );
        assert_ne!(a, anew);
        assert_ne!(
            run(
                &mut e,
                2005,
                &["XADD", "s", "IDMP", "p", "a", "*", "f", "v"]
            ),
            anew
        );
        assert_eq!(stream(&mut e).producers[b"p".as_slice()].len(), 1);
    }

    #[test]
    fn snapshots_and_incremental_replay_preserve_ack_claim_nack_and_counters() {
        let mut source = TestEngine::new();
        source.set_recording(true);
        prepare(&mut source);
        run(
            &mut source,
            1000,
            &["XREADGROUP", "GROUP", "g", "a", "STREAMS", "s", ">"],
        );
        run(&mut source, 1100, &["XACK", "s", "g", "1-0"]);
        run(
            &mut source,
            1200,
            &["XNACK", "s", "g", "FAIL", "IDS", "1", "2-0"],
        );
        run(
            &mut source,
            1200,
            &["XADD", "s", "IDMP", "producer", "iid", "*", "f", "v"],
        );
        let mut recovered = TestEngine::new();
        for batch in source.journal.drain(..) {
            recovered.replay(batch).unwrap();
        }
        let expected = stream(&mut source)
            .snapshot()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            stream(&mut recovered)
                .snapshot()
                .collect::<io::Result<Vec<_>>>()
                .unwrap(),
            expected
        );
        let mut snapshot = StreamValue::restore(&expected[0]).unwrap();
        for row in &expected[1..] {
            snapshot.apply(row).unwrap();
        }
        let group = &snapshot.groups[b"g".as_slice()];
        assert!(!group.pending.contains_key(&Id(1, 0)));
        assert!(group.pending[&Id(2, 0)].nack > 0);
        assert_eq!(group.nacked.len(), 1);
        assert_eq!(snapshot.added, 3);
        assert!(snapshot.producers[b"producer".as_slice()].contains_key(b"iid".as_slice()));
    }

    #[test]
    fn corrupt_effects_cannot_partially_modify_a_stream() {
        let init = Effect::Init.encode().unwrap();
        let mut s = StreamValue::restore(&init).unwrap();
        let valid = Effect::Add(
            Id(1, 0),
            vec![Bytes::from_static(b"f"), Bytes::from_static(b"v")],
        )
        .encode()
        .unwrap();
        for end in 0..valid.len() {
            assert!(s.apply(&valid[..end]).is_err());
            assert!(s.entries.is_empty());
        }
        let mut trailing = valid.to_vec();
        trailing.push(0);
        assert!(s.apply(&trailing).is_err());
        assert!(s.entries.is_empty());
        s.apply(&valid).unwrap();
        assert!(s.apply(&valid).is_err());
        assert_eq!(s.entries.len(), 1);
        let missing = Effect::Pending(
            Bytes::from_static(b"missing"),
            Id(1, 0),
            Pending {
                consumer: Bytes::from_static(b"c"),
                delivered: 0,
                deliveries: 1,
                nack: 0,
            },
        )
        .encode()
        .unwrap();
        assert!(s.apply(&missing).is_err());
        assert!(s.groups.is_empty());
        let impossible = Effect::Meta {
            last: Id::default(),
            added: 1,
            deleted: Id::default(),
            first: Id(1, 0),
            iids: 0,
            duplicates: 0,
        }
        .encode()
        .unwrap();
        assert!(s.apply(&impossible).is_err());
        assert_eq!(s.last, Id(1, 0));
    }
    #[test]
    fn duplicate_stream_keys_share_one_atomic_delivery_cursor() {
        let mut e = TestEngine::new();
        prepare(&mut e);
        let result = list(run(
            &mut e,
            1000,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "a",
                "COUNT",
                "1",
                "STREAMS",
                "s",
                "s",
                ">",
                ">",
            ],
        ));
        assert_eq!(result.len(), 2);
        let mut ids = Vec::new();
        for pair in result {
            let pair = list(pair);
            let rows = list(pair.into_iter().nth(1).unwrap());
            ids.push(list(rows.into_iter().next().unwrap()).remove(0));
        }
        assert_eq!(ids, vec![Id(1, 0).reply(), Id(2, 0).reply()]);
        let g = &stream(&mut e).groups[b"g".as_slice()];
        assert_eq!(g.last, Id(2, 0));
        assert_eq!(g.pending[&Id(1, 0)].deliveries, 1);
        assert_eq!(g.pending[&Id(2, 0)].deliveries, 1);
        run(
            &mut e,
            1000,
            &["XNACK", "s", "g", "FAIL", "IDS", "1", "1-0"],
        );
        run(&mut e, 1000, &["XACK", "s", "g", "1-0"]);
        let rows = stream(&mut e)
            .snapshot()
            .collect::<io::Result<Vec<_>>>()
            .unwrap();
        let mut restored = StreamValue::restore(&rows[0]).unwrap();
        for row in &rows[1..] {
            restored.apply(row).unwrap();
        }
        assert_eq!(restored.groups[b"g".as_slice()].nack_seq, 1);
    }
}
