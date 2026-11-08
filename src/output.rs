//! Connection output: one bounded encoding buffer shared by consecutive replies
//! and pushes, and a bounded ledger completing each command once its reply is
//! delivered.
//!
//! Replies to requests that are already buffered are encoded back to back and
//! leave together. A command is observed only after a flush succeeds past its
//! reply's last byte, so telemetry, slot statistics and latency still describe
//! delivered replies. The commands one flush delivers share that flush's
//! delivery time and are observed together: one telemetry lock and one metrics
//! publication per flush, each command still reported with its own execution
//! time, CPU, metadata and reply status. Dropping the output fails every
//! undelivered command, as a failed write does.
use std::{
    collections::VecDeque,
    future::Future,
    io::{self, Write},
    sync::atomic::Ordering,
    time::{Duration, Instant},
};

use bytes::{BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    cluster,
    connection::Connection,
    engine,
    metrics::{CommandLatencies, Metrics},
    protocol::{Resp, Version},
    pubsub,
    server_commands::{Observation, ServerRuntime},
    state::State,
};

/// Encoded bytes coalesced per connection before a socket write: many
/// medium replies to a pipeline leave together. The buffer starts at 128 bytes
/// and grows by powers of two only as far as the replies awaiting a write
/// need, so small replies never acquire the whole ceiling.
pub(crate) const OUTPUT_BUFFER_BYTES: usize = 128 * 1024;
/// Largest bulk frame copied into that buffer. A larger single payload is
/// written straight from its `Bytes` however much room the buffer has, so the
/// higher aggregate ceiling never makes large values pay a copy.
pub(crate) const BULK_COPY_BYTES: usize = 16 * 1024;
/// Upper bound of any frame header or fixed-size scalar (`$<u64>\r\n`, a RESP2 double).
const MAX_FIXED_FRAME: usize = 32;
/// Commands whose replies may await delivery together.
const LEDGER_COMMANDS: usize = 16;
/// Request bytes those commands may keep alive through their arguments: the
/// request framing cap, so an empty ledger admits any request.
const RETAINED_INPUT_BYTES: u64 = 8 * 1024 * 1024;
/// Emptied argument vectors of delivered commands kept for later requests.
const SPARE_ARGUMENT_VECTORS: usize = 16;
/// Arguments a kept vector may have room for. Vectors of larger requests are
/// freed once delivered, and larger requests never take a kept vector.
const SPARE_ARGUMENT_CAPACITY: usize = 32;

/// Encoded bytes awaiting the socket, and delivery progress as stream offsets.
/// Offsets count every byte the connection produced, including large payloads
/// written straight from their `Bytes`.
struct Wire {
    /// Never holds, nor grows its capacity past, `OUTPUT_BUFFER_BYTES`.
    buffer: BytesMut,
    /// `buffer[..head]` was already accepted by the stream.
    head: usize,
    /// Aggregates an encoding suspended to write a nested one, outermost
    /// first: one per suspended level with children left, each owning those
    /// children in its reply's own allocation. Reused across replies; a failed
    /// or cancelled encoding leaves its frames until the next one starts.
    parents: Vec<Pending>,
    /// Bytes the stream accepted.
    accepted: u64,
    /// `accepted` as of the last successful flush.
    flushed: u64,
}

impl Wire {
    fn unsent(&self) -> &[u8] {
        &self.buffer[self.head..]
    }

    /// Offset just past the last encoded byte.
    fn appended(&self) -> u64 {
        self.accepted + self.unsent().len() as u64
    }

    fn sent(&mut self, written: usize) {
        self.head += written;
        self.accepted += written as u64;
        if self.head == self.buffer.len() {
            self.buffer.clear();
            self.head = 0;
        }
    }

    /// Room left before the buffer reaches `OUTPUT_BUFFER_BYTES`.
    fn room(&self) -> usize {
        OUTPUT_BUFFER_BYTES.saturating_sub(self.buffer.len())
    }

    /// Grow to the next power of two within `OUTPUT_BUFFER_BYTES`; an exact
    /// allocation, so `Vec` doubling can never overshoot the limit.
    fn reserve(&mut self, additional: usize) {
        let buffer = &mut self.buffer;
        let needed = buffer.len() + additional;
        debug_assert!(needed <= OUTPUT_BUFFER_BYTES);
        if needed > buffer.capacity() {
            let mut grown =
                BytesMut::with_capacity(needed.next_power_of_two().min(OUTPUT_BUFFER_BYTES));
            grown.extend_from_slice(&buffer[..]);
            *buffer = grown;
        }
    }

    /// Write `child`'s children before the rest of the `open` aggregate's,
    /// which waits in `parents` only while it has children left.
    fn nest(&mut self, open: &mut Option<Pending>, child: Pending) {
        if child.finished() {
            return;
        }
        if let Some(parent) = open.replace(child)
            && !parent.finished()
        {
            self.parents.push(parent);
        }
    }
}

/// An aggregate whose header is encoded, owning its unwritten children where
/// its reply put them: opening one moves only its vector's header.
enum Pending {
    Items(std::vec::IntoIter<Resp>),
    /// Map entries, and the value still due for the key taken last.
    Pairs(std::vec::IntoIter<(Resp, Resp)>, Option<Resp>),
}

impl Pending {
    /// No child is left to write.
    fn finished(&self) -> bool {
        match self {
            Self::Items(items) => items.as_slice().is_empty(),
            Self::Pairs(entries, due) => due.is_none() && entries.as_slice().is_empty(),
        }
    }
}

impl Iterator for Pending {
    type Item = Resp;

    /// The next child in reply order: a map's keys each precede their value.
    fn next(&mut self) -> Option<Resp> {
        match self {
            Self::Items(items) => items.next(),
            Self::Pairs(entries, due) => {
                if let Some(value) = due.take() {
                    return Some(value);
                }
                let (key, value) = entries.next()?;
                *due = Some(value);
                Some(key)
            }
        }
    }
}

/// The connection's output. Methods taking the stream encode into or deliver
/// through it; nothing else writes to the socket while a connection is served.
pub(crate) struct Output<'a> {
    wire: Wire,
    /// Commands whose replies are encoded but not yet delivered, in order.
    ledger: VecDeque<Completion<'a>>,
    /// Request bytes of the `ledger` commands. Their arguments are slices of
    /// the connection's input buffer. While replies are pending, the
    /// connection reads only to complete a buffered partial request under
    /// 8 KiB that the ledger admits: at most one read of at most 8 KiB per
    /// parsed command. A read that outgrows the buffer while undelivered
    /// commands share it moves only that partial request, into an allocation
    /// of the larger of what it and the read need and the buffer's original
    /// 8 KiB, so under 16 KiB, never doubled. Each undelivered command keeps
    /// alive just the allocation its request was parsed from. A waiting
    /// command's own read-ahead can likewise replace the allocation, and its
    /// older one lives only until the next delivery.
    retained_input: u64,
    /// Emptied argument vectors for later requests: at most
    /// `SPARE_ARGUMENT_VECTORS`, each with room for at most
    /// `SPARE_ARGUMENT_CAPACITY` arguments.
    spare_arguments: Vec<Vec<Bytes>>,
    state: &'a State,
    address: &'a str,
}

impl<'a> Output<'a> {
    pub(crate) fn new(state: &'a State, address: &'a str) -> Self {
        Self {
            wire: Wire {
                buffer: BytesMut::with_capacity(128),
                head: 0,
                parents: Vec::new(),
                accepted: 0,
                flushed: 0,
            },
            ledger: VecDeque::with_capacity(LEDGER_COMMANDS),
            retained_input: 0,
            spare_arguments: Vec::new(),
            state,
            address,
        }
    }

    /// Time a request that started at `started`. Its delivery or failure is
    /// reported to this output's state metrics.
    pub(crate) fn timer(&self, started: Instant) -> RequestTimer<'a> {
        let state: &'a State = self.state;
        RequestTimer {
            metrics: Some(&state.metrics),
            started,
        }
    }

    /// An empty vector for a request of `count` arguments: one a delivered
    /// command left behind if the request is an ordinary one, else a new one.
    /// Its capacity is only a hint; the parser sizes it exactly.
    pub(crate) fn arguments(&mut self, count: usize) -> Vec<Bytes> {
        if (1..=SPARE_ARGUMENT_CAPACITY).contains(&count)
            && let Some(args) = self.spare_arguments.pop()
        {
            return args;
        }
        Vec::with_capacity(count)
    }

    /// Whether a command of `input_bytes` may join the undelivered ones;
    /// otherwise they settle first.
    pub(crate) fn admits(&self, input_bytes: u64) -> bool {
        self.ledger.is_empty()
            || (self.ledger.len() < LEDGER_COMMANDS
                && self.retained_input + input_bytes <= RETAINED_INPUT_BYTES)
    }

    /// Nothing is left to write or flush, so every command is complete.
    pub(crate) fn idle(&self) -> bool {
        self.wire.unsent().is_empty() && self.wire.flushed == self.wire.accepted
    }

    /// Encode a command's reply after everything before it.
    pub(crate) async fn reply<W: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut W,
        response: Resp,
        version: Version,
        mut done: Completion<'a>,
    ) -> io::Result<()> {
        let start = self.wire.appended();
        self.writer(stream).encode(response, version).await?;
        done.end = self.wire.appended();
        done.output_bytes = done.end - start;
        self.enqueue(done);
        Ok(())
    }

    /// Whether [`Self::reply_buffered`] takes `response`: a single frame that
    /// [`Self::reply`] would encode with room left for the next fixed-size
    /// frame, so without writing to the stream.
    pub(crate) fn buffers(&self, response: &Resp) -> bool {
        self.buffered_frame(response).is_some()
    }

    /// [`Self::reply`] of a response [`Self::buffers`] takes, without a
    /// future: the same bytes, buffer growth, offsets and completion.
    pub(crate) fn reply_buffered(
        &mut self,
        response: Resp,
        version: Version,
        mut done: Completion<'a>,
    ) {
        let frame = self
            .buffered_frame(&response)
            .expect("`buffers` took this response");
        let start = self.wire.appended();
        // Frames a failed or cancelled encoding left behind, as
        // `ReplyWriter::encode` drops them.
        self.wire.parents.clear();
        self.wire.reserve(frame);
        response.encode(&mut self.wire.buffer, version);
        debug_assert!(self.wire.room() >= MAX_FIXED_FRAME);
        done.end = self.wire.appended();
        done.output_bytes = done.end - start;
        self.enqueue(done);
    }

    /// The room `ReplyWriter::encode` reserves for `response` when it is one
    /// frame that, with `MAX_FIXED_FRAME` more, fits the room left; else
    /// `None`. Within that room every `spill` it passes finds enough room, so
    /// it writes nothing to the stream, and `Resp::encode` appends the bytes it
    /// would: lines with CR/LF rewritten, small bulk payloads copied, scalars.
    fn buffered_frame(&self, response: &Resp) -> Option<usize> {
        let room = self.wire.room();
        let frame = match response {
            Resp::Simple(value) => value.len() + 3,
            Resp::Error(value) => value.len() + 3,
            Resp::Status(value) => value.len() + 3,
            Resp::Bulk(Some(data)) => {
                // Copied, as `ReplyWriter::bulk` copies it, only within the
                // budget.
                let frame = data.len() + MAX_FIXED_FRAME;
                if frame > room.min(BULK_COPY_BYTES) {
                    return None;
                }
                frame
            }
            Resp::Integer(_)
            | Resp::Unsigned(_)
            | Resp::Boolean(_)
            | Resp::Double(_)
            | Resp::Bulk(None)
            | Resp::Array(None) => MAX_FIXED_FRAME,
            _ => return None,
        };
        (frame + MAX_FIXED_FRAME <= room).then_some(frame)
    }

    /// Complete a command without a reply once everything before it is delivered.
    pub(crate) fn record(&mut self, mut done: Completion<'a>) {
        done.end = self.wire.appended();
        self.enqueue(done);
        self.resolve();
    }

    /// Encode a push or out-of-band reply; it completes no command.
    pub(crate) async fn push<W: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut W,
        value: Resp,
        version: Version,
    ) -> io::Result<()> {
        self.writer(stream).encode(value, version).await
    }

    /// Encode a publication once its commit gate opens; a closed gate drops it.
    pub(crate) async fn publish<W: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut W,
        message: &mut pubsub::Message,
        version: Version,
    ) -> io::Result<()> {
        if !self.wait(stream, message.ready()).await? {
            return Ok(());
        }
        let marker = if version == Version::Resp3 { '>' } else { '*' };
        let mut writer = self.writer(stream);
        writer.header(marker, message.values.len()).await?;
        for value in std::mem::take(&mut message.values) {
            writer.encode(value, version).await?;
        }
        // Message owns the subscriber's event/byte charge. Keep it charged
        // through transport flush, including bytes buffered inside TLS.
        self.settle(stream).await
    }

    /// Deliver everything encoded so far, including bytes a TLS session still
    /// holds, and complete every command.
    pub(crate) async fn settle<W: AsyncWrite + Unpin>(&mut self, stream: &mut W) -> io::Result<()> {
        while !self.idle() {
            self.deliver(stream).await?;
        }
        debug_assert!(self.ledger.is_empty());
        Ok(())
    }

    /// Await `future` while delivering pending output. A future ready at once
    /// delivers nothing, so replies keep coalescing; a delivery step never
    /// delays the future.
    pub(crate) async fn wait<W: AsyncWrite + Unpin, T>(
        &mut self,
        stream: &mut W,
        future: impl Future<Output = T>,
    ) -> io::Result<T> {
        tokio::pin!(future);
        loop {
            let delivering = !self.idle();
            tokio::select! {
                biased;
                value = &mut future => return Ok(value),
                delivered = self.deliver(stream), if delivering => delivered?,
            }
        }
    }

    /// One cancel-safe delivery step: a single write of pending bytes and,
    /// once none are left, a flush completing the commands it delivered.
    pub(crate) async fn deliver<W: AsyncWrite + Unpin>(
        &mut self,
        stream: &mut W,
    ) -> io::Result<()> {
        if !self.wire.unsent().is_empty() {
            let written = stream.write(self.wire.unsent()).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            self.wire.sent(written);
            if !self.wire.unsent().is_empty() {
                return Ok(());
            }
        }
        stream.flush().await?;
        self.wire.flushed = self.wire.accepted;
        self.resolve();
        Ok(())
    }

    fn enqueue(&mut self, done: Completion<'a>) {
        debug_assert!(
            self.ledger.len() < LEDGER_COMMANDS,
            "admitted past the ledger"
        );
        self.retained_input += done.input_bytes;
        self.ledger.push_back(done);
    }

    /// Complete, in order, every command whose reply a flush delivered: all of
    /// them at the one delivery time sampled here. They are counted delivered
    /// first, so a panic in the observation bookkeeping that follows cannot
    /// turn a flushed reply into an undelivered failure.
    fn resolve(&mut self) {
        let flushed = self.wire.flushed;
        let delivered = self
            .ledger
            .iter()
            .take_while(|done| done.end <= flushed)
            .count();
        if delivered == 0 {
            return;
        }
        let at = Instant::now();
        let mut latencies = CommandLatencies::default();
        for done in self.ledger.range_mut(..delivered) {
            done.timer.delivered(at, done.failed, &mut latencies);
        }
        self.state.metrics.commands_complete(&latencies);
        let cluster = self.state.cluster.get().map(|cluster| &**cluster);
        let server = self.state.server.get().map(|server| &**server);
        if cluster.is_some() || server.is_some() {
            // Without slot accounting a builtin command resolves no keys, so
            // a flush of only those skips resolution.
            let keyless =
                cluster.is_none() && self.ledger.range(..delivered).all(|done| done.builtin);
            // Bounded by the ledger, so both stay on the stack.
            let mut keys = SmallVec::<[_; LEDGER_COMMANDS]>::new();
            if !keyless {
                for done in self.ledger.range(..delivered) {
                    keys.push(done.prepare(cluster, server));
                }
            }
            if let Some(server) = server {
                let observations: SmallVec<[_; LEDGER_COMMANDS]> = self
                    .ledger
                    .range(..delivered)
                    .enumerate()
                    .map(|(index, done)| {
                        let keys = keys.get(index).and_then(|keys| keys.as_deref());
                        done.observation(keys, self.address)
                    })
                    .collect();
                server.observe(&observations);
            }
        }
        // Each command releases its arguments here, after its observation;
        // a small vector is kept, emptied, for a later request. Taken in
        // place, so no completion moves out of the ledger.
        for done in self.ledger.range_mut(..delivered) {
            self.retained_input -= done.input_bytes;
            if (1..=SPARE_ARGUMENT_CAPACITY).contains(&done.args.capacity())
                && self.spare_arguments.len() < SPARE_ARGUMENT_VECTORS
            {
                done.args.clear();
                self.spare_arguments.push(std::mem::take(&mut done.args));
            }
        }
        self.ledger.drain(..delivered);
    }

    fn writer<'w, W>(&'w mut self, stream: &'w mut W) -> ReplyWriter<'w, W> {
        ReplyWriter {
            stream,
            wire: &mut self.wire,
        }
    }
}

/// Wall time of one request, from `started` to its reply's delivery. Dropped
/// undelivered, it reports a failure. Created by `Output::timer`.
pub(crate) struct RequestTimer<'a> {
    /// The output state's metrics, until delivery disarms the failure report.
    metrics: Option<&'a Metrics>,
    started: Instant,
}

impl RequestTimer<'_> {
    /// Count its reply, delivered `at`, into a flush's batch and disarm the
    /// failure report.
    fn delivered(&mut self, at: Instant, failed: bool, latencies: &mut CommandLatencies) {
        let armed = self.metrics.take().is_some();
        debug_assert!(armed, "a command delivered twice");
        latencies.record(at.saturating_duration_since(self.started), failed);
    }
}

impl Drop for RequestTimer<'_> {
    fn drop(&mut self) {
        if let Some(metrics) = self.metrics {
            metrics.command_complete(self.started.elapsed(), true);
        }
    }
}

/// A command from admission until it has run. The connection keeps its parsed
/// arguments while routing and execution borrow them, and hands them over at
/// completion for its observation; only owners outliving the request copy them.
pub(crate) struct CommandRecord<'a> {
    pub(crate) timer: RequestTimer<'a>,
    pub(crate) started: Instant,
    pub(crate) cpu_start: u64,
    pub(crate) input_bytes: u64,
    /// Its arguments name a builtin registration: `BoundCommand::exists`.
    pub(crate) builtin: bool,
}

impl<'a> CommandRecord<'a> {
    /// What its observation reports, read before its reply is written and so
    /// before any later command runs: execution time, CPU, database and name.
    pub(crate) fn complete(
        self,
        args: Vec<Bytes>,
        failed: bool,
        connection: &Connection,
        state: &State,
    ) -> Completion<'a> {
        Completion {
            elapsed: self.started.elapsed(),
            cpu_ns: connection
                .session
                .cpu_ns
                .load(Ordering::Relaxed)
                .saturating_sub(self.cpu_start),
            database: connection.session.database(),
            name: state.server.get().and_then(|_| connection.lease.name()),
            failed,
            builtin: self.builtin,
            input_bytes: self.input_bytes,
            output_bytes: 0,
            end: 0,
            timer: self.timer,
            args,
        }
    }
}

/// A command that has run, observed once its reply ending at `end` is delivered.
pub(crate) struct Completion<'a> {
    timer: RequestTimer<'a>,
    args: Vec<Bytes>,
    elapsed: Duration,
    cpu_ns: u64,
    database: u32,
    /// Read only for a server runtime, which alone reports it.
    name: Option<Bytes>,
    /// Its reply is an error.
    failed: bool,
    /// Its arguments name a builtin registration.
    builtin: bool,
    input_bytes: u64,
    output_bytes: u64,
    end: u64,
}

impl Completion<'_> {
    /// Its slot statistics, and the keys its observation reports, resolved
    /// before any telemetry lock: dynamic-module keys always, since extraction
    /// consults the extension registry, and builtin keys only when slot
    /// accounting needs them now. Otherwise `None` lets HOTKEYS derive builtin
    /// keys itself, only while a session is active at observation.
    fn prepare(
        &self,
        cluster: Option<&cluster::ClusterRuntime>,
        server: Option<&ServerRuntime>,
    ) -> Option<engine::AccessedKeys> {
        let sharded_channel = self.args.first().is_some_and(|name| {
            name.eq_ignore_ascii_case(b"SPUBLISH") || name.eq_ignore_ascii_case(b"SSUBSCRIBE")
        });
        let resolved = if self.builtin {
            (cluster.is_some() && !sharded_channel)
                .then(|| engine::BoundCommand::bind(&self.args).accessed_keys())
        } else {
            Some(
                server
                    .and_then(|server| server.extensions().prepare_sync(&self.args))
                    .and_then(Result::ok)
                    .map_or_else(engine::AccessedKeys::new, |invocation| {
                        invocation.keys().iter().cloned().collect()
                    }),
            )
        };
        if let Some(cluster) = cluster {
            let slot_key = if sharded_channel {
                self.args.get(1)
            } else {
                resolved.as_ref().and_then(|keys| keys.first())
            };
            if let Some(key) = slot_key {
                cluster.record(
                    cluster::key_slot(key),
                    self.cpu_ns / 1000,
                    self.input_bytes,
                    self.output_bytes,
                );
            }
        }
        resolved
    }

    fn observation<'o>(&'o self, keys: Option<&'o [Bytes]>, address: &'o str) -> Observation<'o> {
        Observation {
            args: &self.args,
            keys,
            database: self.database,
            client_address: address,
            client_name: self.name.as_deref().unwrap_or_default(),
            elapsed: self.elapsed,
            cpu_time: Duration::from_nanos(self.cpu_ns),
            input_bytes: self.input_bytes,
            output_bytes: self.output_bytes,
            failed: self.failed,
        }
    }
}

/// RESP encoder appending to the connection's buffer. Every append is bounded
/// by `room` or `MAX_FIXED_FRAME` and followed by `spill`, so the buffer never
/// holds, nor grows its capacity past, `OUTPUT_BUFFER_BYTES`, and always keeps
/// room for the next fixed-size frame. Bulk payloads are copied only when
/// their frame fits both that room and `BULK_COPY_BYTES`; others are written
/// straight from their `Bytes` beside the buffered prefix, never copied.
struct ReplyWriter<'w, W> {
    stream: &'w mut W,
    wire: &'w mut Wire,
}

impl<W: AsyncWrite + Unpin> ReplyWriter<'_, W> {
    async fn encode(&mut self, response: Resp, version: Version) -> io::Result<()> {
        let resp3 = version == Version::Resp3;
        // Frames a failed or cancelled encoding left behind.
        self.wire.parents.clear();
        // The aggregate whose children are being written; only those it
        // suspended wait in `parents`.
        let mut open: Option<Pending> = None;
        let mut value = response;
        loop {
            match value {
                Resp::Bulk(Some(data)) => self.bulk(data, version).await?,
                Resp::Simple(value) => self.line(b'+', value.as_bytes()).await?,
                Resp::Status(value) => self.line(b'+', &value).await?,
                Resp::Error(value) => self.line(b'-', value.as_bytes()).await?,
                Resp::BigNumber(value) if resp3 => {
                    self.extend(b"(");
                    self.payload(&value).await?;
                }
                Resp::Verbatim { format, value } if resp3 => {
                    self.reserve(MAX_FIXED_FRAME);
                    write!((&mut self.wire.buffer).writer(), "={}\r\n", value.len() + 4)?;
                    self.extend(&format);
                    self.extend(b":");
                    self.payload(&value).await?;
                }
                Resp::BigNumber(value) | Resp::Verbatim { value, .. } => {
                    self.bulk(value, version).await?;
                }
                Resp::Array(Some(values)) => {
                    self.header('*', values.len()).await?;
                    self.wire
                        .nest(&mut open, Pending::Items(values.into_iter()));
                }
                Resp::Set(values) => {
                    self.header(if resp3 { '~' } else { '*' }, values.len())
                        .await?;
                    self.wire
                        .nest(&mut open, Pending::Items(values.into_iter()));
                }
                Resp::Map(values) => {
                    let (marker, count) = if resp3 {
                        ('%', values.len())
                    } else {
                        ('*', values.len() * 2)
                    };
                    self.header(marker, count).await?;
                    self.wire
                        .nest(&mut open, Pending::Pairs(values.into_iter(), None));
                }
                Resp::Scored(values) => {
                    let count = if resp3 {
                        values.len()
                    } else {
                        values.len() * 2
                    };
                    self.header('*', count).await?;
                    for (member, score) in values {
                        if resp3 {
                            self.put(b"*2\r\n").await?;
                        }
                        self.bulk(member, version).await?;
                        self.frame(Resp::Double(score), version).await?;
                    }
                }
                Resp::InfoField(field, inner) => {
                    if resp3 {
                        self.put(b"%1\r\n").await?;
                        self.bulk(field, version).await?;
                    } else {
                        self.put(b"*1\r\n").await?;
                    }
                    // Its value precedes everything after the field.
                    value = *inner;
                    continue;
                }
                Resp::VectorResults(values) => {
                    let mapped = resp3
                        && values
                            .first()
                            .is_some_and(|entry| entry.1.is_some() || entry.2.is_some());
                    let count = if mapped {
                        values.len()
                    } else {
                        values
                            .iter()
                            .map(|(_, score, attr)| {
                                1 + usize::from(score.is_some()) + usize::from(attr.is_some())
                            })
                            .sum()
                    };
                    self.header(if mapped { '%' } else { '*' }, count).await?;
                    for (member, score, attr) in values {
                        self.bulk(member, version).await?;
                        if mapped && score.is_some() && attr.is_some() {
                            self.put(b"*2\r\n").await?;
                        }
                        if let Some(score) = score {
                            self.frame(Resp::Double(score), version).await?;
                        }
                        match attr {
                            Some(Some(attr)) => self.bulk(attr, version).await?,
                            Some(None) => self.frame(Resp::Bulk(None), version).await?,
                            None => {}
                        }
                    }
                }
                value @ (Resp::Integer(_)
                | Resp::Unsigned(_)
                | Resp::Boolean(_)
                | Resp::Double(_)
                | Resp::Bulk(None)
                | Resp::Array(None)) => self.frame(value, version).await?,
            }
            // Pre-order: the open aggregate's next child or, once it has none
            // left, that of the aggregate it suspended.
            value = loop {
                let Some(pending) = &mut open else {
                    return Ok(());
                };
                match pending.next() {
                    // Bulk runs skip the dispatch above; their frames are the same.
                    Some(Resp::Bulk(Some(data))) => self.bulk(data, version).await?,
                    Some(child) => break child,
                    None => open = self.wire.parents.pop(),
                }
            };
        }
    }

    /// Aggregate or push header.
    async fn header(&mut self, marker: char, count: usize) -> io::Result<()> {
        self.reserve(MAX_FIXED_FRAME);
        write!((&mut self.wire.buffer).writer(), "{marker}{count}\r\n")?;
        self.spill().await
    }

    async fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.extend(bytes);
        self.spill().await
    }

    /// A fixed-size frame: header-free scalar or null.
    async fn frame(&mut self, value: Resp, version: Version) -> io::Result<()> {
        self.reserve(MAX_FIXED_FRAME);
        value.encode(&mut self.wire.buffer, version);
        self.spill().await
    }

    /// Simple/status/error line with CR/LF rewritten to spaces exactly as
    /// `Resp::encode` does, copied through the buffer in bounded chunks.
    async fn line(&mut self, marker: u8, mut value: &[u8]) -> io::Result<()> {
        self.extend(&[marker]);
        while !value.is_empty() {
            // `spill` leaves room, so every chunk makes progress.
            let (chunk, rest) = value.split_at(value.len().min(self.room()));
            let start = self.wire.buffer.len();
            self.extend(chunk);
            for byte in &mut self.wire.buffer[start..] {
                if matches!(*byte, b'\r' | b'\n') {
                    *byte = b' ';
                }
            }
            value = rest;
            self.spill().await?;
        }
        self.put(b"\r\n").await
    }

    /// Copy a bulk frame within the copy budget; write a larger payload in place.
    async fn bulk(&mut self, data: Bytes, version: Version) -> io::Result<()> {
        if data.len() + MAX_FIXED_FRAME <= self.copy_room() {
            self.reserve(data.len() + MAX_FIXED_FRAME);
            Resp::Bulk(Some(data)).encode(&mut self.wire.buffer, version);
            return self.spill().await;
        }
        self.reserve(MAX_FIXED_FRAME);
        write!((&mut self.wire.buffer).writer(), "${}\r\n", data.len())?;
        self.write_through(&data).await
    }

    /// Complete a frame whose prefix is buffered with `data` and its CRLF.
    async fn payload(&mut self, data: &[u8]) -> io::Result<()> {
        if data.len() + 2 <= self.copy_room() {
            self.extend(data);
            return self.put(b"\r\n").await;
        }
        self.write_through(data).await
    }

    /// Write the unsent buffer and `data` with vectored writes, then buffer the CRLF.
    async fn write_through(&mut self, data: &[u8]) -> io::Result<()> {
        let mut tail = data;
        while !self.wire.unsent().is_empty() {
            let head = self.wire.unsent();
            let slices = [io::IoSlice::new(head), io::IoSlice::new(tail)];
            let written = self.stream.write_vectored(&slices).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            let from_head = written.min(head.len());
            tail = &tail[written - from_head..];
            self.wire.sent(from_head);
            self.wire.accepted += (written - from_head) as u64;
        }
        self.stream.write_all(tail).await?;
        self.wire.accepted += tail.len() as u64;
        self.extend(b"\r\n");
        Ok(())
    }

    fn extend(&mut self, bytes: &[u8]) {
        self.reserve(bytes.len());
        self.wire.buffer.extend_from_slice(bytes);
    }

    fn reserve(&mut self, additional: usize) {
        self.wire.reserve(additional);
    }

    fn room(&self) -> usize {
        self.wire.room()
    }

    /// Room a single payload may be copied into.
    fn copy_room(&self) -> usize {
        self.room().min(BULK_COPY_BYTES)
    }

    /// Keep room for the next fixed-size frame.
    async fn spill(&mut self) -> io::Result<()> {
        if self.room() < MAX_FIXED_FRAME {
            self.drain().await?;
        }
        Ok(())
    }

    /// Write every unsent byte; flushing is left to delivery.
    async fn drain(&mut self) -> io::Result<()> {
        while !self.wire.unsent().is_empty() {
            let written = self.stream.write(self.wire.unsent()).await?;
            if written == 0 {
                return Err(io::ErrorKind::WriteZero.into());
            }
            self.wire.sent(written);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll, Waker},
    };

    /// Peer transport accepting at most `limit` bytes per write and stalling
    /// periodically. Accepted bytes reach the peer only on flush, as with a TLS
    /// session; once `fail_at` bytes are accepted every write fails.
    struct Transport {
        limit: usize,
        vectored: bool,
        fail_at: usize,
        calls: usize,
        staged: Vec<u8>,
        delivered: Vec<u8>,
    }

    impl Transport {
        fn new(limit: usize, vectored: bool, fail_at: usize) -> Self {
            Self {
                limit,
                vectored,
                fail_at,
                calls: 0,
                staged: Vec::new(),
                delivered: Vec::new(),
            }
        }

        fn accept(
            &mut self,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            self.calls += 1;
            if self.calls.is_multiple_of(5) {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            let accepted = self.staged.len() + self.delivered.len();
            if accepted >= self.fail_at {
                return Poll::Ready(Err(io::ErrorKind::ConnectionReset.into()));
            }
            let mut budget = self.limit.min(self.fail_at - accepted);
            let start = self.staged.len();
            for buf in bufs {
                let take = buf.len().min(budget);
                self.staged.extend_from_slice(&buf[..take]);
                budget -= take;
            }
            Poll::Ready(Ok(self.staged.len() - start))
        }
    }

    impl AsyncWrite for Transport {
        fn poll_write(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.get_mut().accept(cx, &[io::IoSlice::new(buf)])
        }
        fn poll_write_vectored(
            self: Pin<&mut Self>,
            cx: &mut Context<'_>,
            bufs: &[io::IoSlice<'_>],
        ) -> Poll<io::Result<usize>> {
            let this = self.get_mut();
            if this.vectored {
                return this.accept(cx, bufs);
            }
            // Like tokio's default: only the first non-empty buffer.
            let buf = bufs
                .iter()
                .find(|buf| !buf.is_empty())
                .map_or(&[][..], |buf| &**buf);
            this.accept(cx, &[io::IoSlice::new(buf)])
        }
        fn is_write_vectored(&self) -> bool {
            self.vectored
        }
        fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
            let this = self.get_mut();
            this.delivered.append(&mut this.staged);
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    /// What a peer behind a TLS-like session holds: writes are accepted into
    /// `staged` at once but reach `delivered` only on a flush, which stalls
    /// until `open`.
    #[derive(Default)]
    struct Peer {
        open: bool,
        flush: Option<Waker>,
        staged: Vec<u8>,
        delivered: Vec<u8>,
    }

    /// Transport over a shared `Peer`, so a test can inspect and release the
    /// flush an output is awaiting.
    struct PeerStream(Arc<Mutex<Peer>>);

    impl AsyncWrite for PeerStream {
        fn poll_write(
            self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buf: &[u8],
        ) -> Poll<io::Result<usize>> {
            self.0.lock().unwrap().staged.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            let mut guard = self.0.lock().unwrap();
            let peer = &mut *guard;
            if !peer.open {
                peer.flush = Some(cx.waker().clone());
                return Poll::Pending;
            }
            peer.delivered.append(&mut peer.staged);
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            self.poll_flush(cx)
        }
    }

    fn bulk(len: usize, seed: u8) -> Resp {
        Resp::Bulk(Some((0..len).map(|i| seed.wrapping_add(i as u8)).collect()))
    }

    fn encoded(replies: impl IntoIterator<Item = Resp>, version: Version) -> BytesMut {
        let mut encoded = BytesMut::new();
        for reply in replies {
            reply.encode(&mut encoded, version);
        }
        encoded
    }

    /// ASCII text with CR and LF at offsets that drift across chunk edges.
    fn text(len: usize) -> String {
        (0..len)
            .map(|i| match i % 97 {
                0 => '\r',
                1 | 50 => '\n',
                _ => char::from(b'a' + (i % 26) as u8),
            })
            .collect()
    }

    /// Every reply shape, with payloads straddling the copy budget and the
    /// coalescing threshold.
    fn replies() -> Vec<Resp> {
        let edge = OUTPUT_BUFFER_BYTES - MAX_FIXED_FRAME;
        let copy_edge = BULK_COPY_BYTES - MAX_FIXED_FRAME;
        let mut replies = vec![
            Resp::Simple("OK"),
            Resp::Status(Bytes::from_static(b"multi\r\nline")),
            Resp::Error("ERR bad\r\ninput".into()),
            Resp::Integer(i64::MIN),
            Resp::Unsigned(u64::MAX),
            Resp::Boolean(true),
            Resp::Double(-1.5e-300),
            Resp::Double(f64::NEG_INFINITY),
            Resp::Bulk(None),
            Resp::Array(None),
            bulk(0, 0),
            bulk(256, 1),
            Resp::BigNumber(Bytes::from(vec![b'7'; 40])),
            Resp::BigNumber(Bytes::from(vec![b'7'; 2 * OUTPUT_BUFFER_BYTES])),
            Resp::Verbatim {
                format: *b"txt",
                value: Bytes::from_static(b"a\r\nb"),
            },
            Resp::Verbatim {
                format: *b"mkd",
                value: Bytes::from(vec![b'v'; 2 * OUTPUT_BUFFER_BYTES]),
            },
            Resp::Verbatim {
                format: *b"txt",
                value: Bytes::from(vec![b'w'; BULK_COPY_BYTES - 2]),
            },
            Resp::Verbatim {
                format: *b"txt",
                value: Bytes::from(vec![b'w'; BULK_COPY_BYTES - 1]),
            },
            Resp::InfoField(
                Bytes::from_static(b"field"),
                Box::new(bulk(OUTPUT_BUFFER_BYTES, 2)),
            ),
        ];
        for len in [
            copy_edge - 1,
            copy_edge,
            copy_edge + 1,
            BULK_COPY_BYTES,
            edge - 1,
            edge,
            edge + 1,
            OUTPUT_BUFFER_BYTES,
            3 * OUTPUT_BUFFER_BYTES + 3,
        ] {
            replies.push(bulk(len, 3));
        }
        // Many small members cross the threshold at varying offsets.
        replies.push(Resp::Array(Some(
            (0..2000).map(|i| bulk(i % 37, i as u8)).collect(),
        )));
        // Medium members, each copied, fill the buffer past its ceiling.
        replies.push(Resp::Array(Some(
            (0..20).map(|i| bulk(copy_edge - i * 7, i as u8)).collect(),
        )));
        replies.push(Resp::Array(Some(vec![
            bulk(10, 4),
            bulk(edge - 40, 5),
            Resp::Integer(-7),
            bulk(20_000, 6),
            Resp::Array(Some(vec![Resp::Bulk(None), Resp::Double(0.25)])),
            Resp::Map(vec![
                (bulk(3, 7), Resp::Set(vec![bulk(5, 8), bulk(40_000, 9)])),
                (Resp::Simple("count"), Resp::Integer(3)),
            ]),
            Resp::Array(Some(Vec::new())),
            Resp::Map(Vec::new()),
        ])));
        replies.push(Resp::Scored(
            (0..1500)
                .map(|i| (Bytes::from(vec![b'm'; i % 23]), i as f64 / 7.0))
                .collect(),
        ));
        replies.push(Resp::Scored(vec![
            (Bytes::from(vec![b'x'; 40_000]), 1.0),
            (Bytes::from_static(b"y"), f64::NAN),
        ]));
        replies.push(Resp::VectorResults(vec![
            (
                Bytes::from_static(b"a"),
                Some(0.5),
                Some(Some(Bytes::from_static(b"{}"))),
            ),
            (Bytes::from(vec![b'b'; 30_000]), Some(1.0), Some(None)),
            (
                Bytes::from_static(b"c"),
                Some(2.0),
                Some(Some(Bytes::from(vec![b'j'; 20_000]))),
            ),
        ]));
        replies.push(Resp::VectorResults(
            (0..900)
                .map(|i| (Bytes::from(vec![b'e'; i % 31]), None, None))
                .collect(),
        ));
        // A nearly full buffer, then oversized lines whose CR/LF must be rewritten.
        replies.push(Resp::Array(Some(vec![
            Resp::Array(Some(vec![
                bulk(edge - 20, 10),
                Resp::Status(Bytes::from(text(3 * OUTPUT_BUFFER_BYTES + 11))),
                Resp::Error(text(OUTPUT_BUFFER_BYTES + 1)),
                bulk(5, 11),
            ])),
            Resp::Simple("tail"),
        ])));
        // A bulk run, a nested array, then more bulks; empty aggregates
        // between siblings and closing an aggregate.
        replies.push(Resp::Array(Some(vec![
            bulk(3, 12),
            bulk(copy_edge + 1, 13),
            bulk(0, 14),
            Resp::Array(Some(vec![
                bulk(edge - 3, 15),
                Resp::Integer(5),
                bulk(6, 16),
            ])),
            bulk(4, 17),
            bulk(20_000, 18),
            Resp::Array(Some(Vec::new())),
            Resp::Set(Vec::new()),
            Resp::Map(Vec::new()),
            bulk(8, 19),
            Resp::Array(Some(vec![
                Resp::Array(Some(Vec::new())),
                Resp::Array(None),
                Resp::Set(Vec::new()),
            ])),
        ])));
        // Info fields whose values are aggregates, each followed by siblings.
        replies.push(Resp::Array(Some(vec![
            Resp::InfoField(
                Bytes::from_static(b"keys"),
                Box::new(Resp::Array(Some(vec![
                    bulk(copy_edge, 20),
                    Resp::Array(Some(vec![bulk(5, 21), Resp::Bulk(None)])),
                    bulk(copy_edge + 1, 22),
                ]))),
            ),
            bulk(11, 23),
            Resp::InfoField(Bytes::from_static(b"none"), Box::new(Resp::Map(Vec::new()))),
            Resp::InfoField(
                Bytes::from_static(b"outer"),
                Box::new(Resp::InfoField(
                    Bytes::from_static(b"inner"),
                    Box::new(Resp::Set(vec![bulk(13, 24)])),
                )),
            ),
            Resp::Simple("after"),
        ])));
        // Map keys and values that are aggregates, then more pairs.
        replies.push(Resp::Map(vec![
            (
                Resp::Array(Some(vec![bulk(2, 25), bulk(OUTPUT_BUFFER_BYTES + 5, 26)])),
                Resp::Map(vec![
                    (
                        bulk(1, 27),
                        Resp::Array(Some(vec![bulk(3, 28), Resp::Double(2.5)])),
                    ),
                    (Resp::Set(Vec::new()), bulk(copy_edge - 1, 29)),
                ]),
            ),
            (bulk(4, 30), bulk(5, 31)),
            (
                Resp::Integer(9),
                Resp::Set(vec![bulk(edge + 1, 32), bulk(6, 33)]),
            ),
            (bulk(7, 34), Resp::Array(Some(Vec::new()))),
        ]));
        // Five levels under an info field, each resuming after its nested child.
        replies.push(Resp::InfoField(
            Bytes::from_static(b"deep"),
            Box::new(Resp::Array(Some(vec![
                Resp::Array(Some(vec![
                    Resp::Set(vec![
                        Resp::Map(vec![
                            (
                                bulk(2, 35),
                                Resp::Array(Some(vec![bulk(edge + 1, 36), bulk(4, 37)])),
                            ),
                            (bulk(5, 38), bulk(6, 39)),
                        ]),
                        bulk(7, 40),
                    ]),
                    bulk(8, 41),
                ])),
                bulk(9, 42),
            ]))),
        ));
        // Many small entries, each a member and a nested array of varying
        // length, cross the threshold at varying offsets.
        replies.push(Resp::Array(Some(
            (0..3000)
                .map(|i| {
                    Resp::Array(Some(vec![
                        bulk(i % 37, i as u8),
                        Resp::Array(Some((0..i % 5).map(|j| bulk(j * 7, j as u8)).collect())),
                    ]))
                })
                .collect(),
        )));
        replies
    }

    /// Encode `reply` on its own and deliver it; return its encoded size.
    async fn deliver_one(
        out: &mut Output<'_>,
        transport: &mut Transport,
        reply: Resp,
        version: Version,
    ) -> io::Result<u64> {
        let start = out.wire.appended();
        out.push(transport, reply, version).await?;
        let size = out.wire.appended() - start;
        out.settle(transport).await?;
        Ok(size)
    }

    fn client(state: &Arc<State>) -> Connection {
        let lease = state.register(
            "127.0.0.1:1".parse().unwrap(),
            "127.0.0.1:2".parse().unwrap(),
        );
        Connection::new(lease, state.clone(), None)
    }

    /// A `GET k` request of `input_bytes` that has run, timed by `out`.
    fn command<'a>(
        out: &Output<'a>,
        connection: &Connection,
        state: &State,
        input_bytes: u64,
    ) -> Completion<'a> {
        request(out, connection, state, &["GET", "k"], input_bytes, false)
    }

    /// A request of `args` that has run, timed by `out`; its reply is an
    /// error if `failed`.
    fn request<'a>(
        out: &Output<'a>,
        connection: &Connection,
        state: &State,
        args: &[&str],
        input_bytes: u64,
        failed: bool,
    ) -> Completion<'a> {
        let args: Vec<Bytes> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        CommandRecord {
            timer: out.timer(Instant::now()),
            started: Instant::now(),
            cpu_start: 0,
            input_bytes,
            builtin: engine::BoundCommand::bind(&args).exists(),
        }
        .complete(args, failed, connection, state)
    }

    /// State whose server runtime logs every observed command as slow.
    fn observed_state() -> (Arc<State>, Arc<ServerRuntime>) {
        use crate::server_commands::{ConfigSnapshot, HookFuture, ServerAction, ServerHooks};
        struct NoHooks;
        impl ServerHooks for NoHooks {
            fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a> {
                panic!("no server action in output tests: {action:?}");
            }
        }
        let mut config = ConfigSnapshot::default();
        config
            .parameters
            .get_mut("slowlog-log-slower-than")
            .expect("a default parameter")
            .value = Bytes::from_static(b"0");
        let server = Arc::new(ServerRuntime::new(config, Arc::new(NoHooks)));
        let state = Arc::new(State::new(0, 1, false));
        assert!(state.server.set(server.clone()).is_ok());
        (state, server)
    }

    /// Observed commands, oldest first, as `SLOWLOG GET -1` reports them.
    async fn slowlog(server: &ServerRuntime) -> Vec<String> {
        let args = ["SLOWLOG", "GET", "-1"].map(|arg| Bytes::from_static(arg.as_bytes()));
        let context = crate::server_commands::CommandContext { database: 0 };
        let command = crate::engine::BoundCommand::bind(&args);
        let Some(Resp::Array(Some(entries))) = server.execute(&command, context).await else {
            panic!("a slowlog reply");
        };
        entries
            .iter()
            .rev()
            .map(|entry| match entry {
                Resp::Array(Some(fields)) => match &fields[3] {
                    Resp::Array(Some(args)) => args
                        .iter()
                        .map(|arg| match arg {
                            Resp::Bulk(Some(arg)) => String::from_utf8(arg.to_vec()).unwrap(),
                            other => panic!("slowlog argument {other:?}"),
                        })
                        .collect::<Vec<_>>()
                        .join(" "),
                    other => panic!("slowlog arguments {other:?}"),
                },
                other => panic!("slowlog entry {other:?}"),
            })
            .collect()
    }

    /// Expect the MONITOR lines of exactly `commands`, in order.
    fn expect_monitored(monitor: &mut tokio::sync::broadcast::Receiver<Bytes>, commands: &[&str]) {
        for command in commands {
            let line = monitor
                .try_recv()
                .expect("a MONITOR line per observed command");
            let quoted: String = command
                .split(' ')
                .map(|arg| format!(" \"{arg}\""))
                .collect();
            assert!(line.ends_with(quoted.as_bytes()), "{line:?} for {command}");
        }
        assert!(
            monitor.try_recv().is_err(),
            "only observed commands are monitored"
        );
    }

    #[tokio::test]
    async fn replies_keep_canonical_framing_across_short_and_vectored_writes() {
        let state = State::new(0, 1, false);
        for version in [Version::Resp2, Version::Resp3] {
            let expected: Vec<BytesMut> = replies()
                .into_iter()
                .map(|reply| encoded([reply], version))
                .collect();
            for (limit, vectored) in [(1, false), (7, true), (4096, false), (usize::MAX, true)] {
                let mut transport = Transport::new(limit, vectored, usize::MAX);
                // One output serves every reply, as on a connection.
                let mut out = Output::new(&state, "test");
                for (index, (reply, expected)) in replies().into_iter().zip(&expected).enumerate() {
                    let start = transport.delivered.len();
                    let written = deliver_one(&mut out, &mut transport, reply, version)
                        .await
                        .unwrap();
                    let context = format!("{version:?} limit {limit} reply {index}");
                    assert_eq!(written, expected.len() as u64, "{context}");
                    // Delivered (flushed) by settling, byte-for-byte canonical.
                    assert!(transport.delivered[start..] == expected[..], "{context}");
                    assert!(
                        out.wire.buffer.capacity() <= OUTPUT_BUFFER_BYTES,
                        "{context}"
                    );
                }
                // Replies queued back to back share the buffer and leave in order.
                let start = transport.delivered.len();
                for reply in replies() {
                    out.push(&mut transport, reply, version).await.unwrap();
                }
                out.settle(&mut transport).await.unwrap();
                let context = format!("{version:?} limit {limit} queued");
                assert!(
                    transport.delivered[start..] == expected.concat()[..],
                    "{context}"
                );
                assert!(
                    out.wire.buffer.capacity() <= OUTPUT_BUFFER_BYTES,
                    "{context}"
                );
            }
        }
    }

    /// The serve loop's choice for a command's reply: the buffer takes it
    /// without a future when it can, else the writer encodes it. Returns
    /// whether the buffer took it.
    async fn answer<'a>(
        out: &mut Output<'a>,
        transport: &mut Transport,
        reply: Resp,
        version: Version,
        done: Completion<'a>,
    ) -> io::Result<bool> {
        if out.buffers(&reply) {
            out.reply_buffered(reply, version, done);
            return Ok(true);
        }
        out.reply(transport, reply, version, done).await?;
        Ok(false)
    }

    #[tokio::test]
    async fn replies_the_buffer_takes_leave_framed_as_the_writer_frames_them() {
        let state = Arc::new(State::new(0, 1, false));
        let connection = client(&state);
        let tight = OUTPUT_BUFFER_BYTES - 2 * MAX_FIXED_FRAME;
        let full = OUTPUT_BUFFER_BYTES - MAX_FIXED_FRAME - 1;
        for version in [Version::Resp2, Version::Resp3] {
            // Unsent bytes leaving room on and around each bound a reply
            // the buffer takes must respect.
            for fill in [
                0,
                100,
                OUTPUT_BUFFER_BYTES - BULK_COPY_BYTES - 5,
                tight - 6,
                tight,
                tight + 1,
                full,
            ] {
                let mut taken = 0;
                for (index, reply) in replies().into_iter().enumerate() {
                    let context = format!("{version:?} fill {fill} reply {index}");
                    let mut expected = BytesMut::from(&vec![b'f'; fill][..]);
                    reply.encode(&mut expected, version);
                    let mut transport = Transport::new(4096, false, usize::MAX);
                    let mut out = Output::new(&state, "test");
                    out.wire.reserve(fill);
                    out.wire.buffer.extend_from_slice(&expected[..fill]);
                    let done = command(&out, &connection, &state, 20);
                    if answer(&mut out, &mut transport, reply, version, done)
                        .await
                        .unwrap()
                    {
                        taken += 1;
                        assert_eq!(transport.calls, 0, "{context}: it wrote");
                    }
                    let done = out.ledger.back().expect("an undelivered command");
                    assert_eq!(done.end, expected.len() as u64, "{context}");
                    assert_eq!(
                        done.output_bytes,
                        (expected.len() - fill) as u64,
                        "{context}"
                    );
                    assert!(
                        out.wire.buffer.capacity() <= OUTPUT_BUFFER_BYTES,
                        "{context}"
                    );
                    out.settle(&mut transport).await.unwrap();
                    assert!(transport.delivered[..] == expected[..], "{context}");
                }
                if fill == full {
                    assert_eq!(taken, 0, "{version:?}: no frame fits beside the next");
                } else {
                    assert!(taken > 0, "{version:?} fill {fill}");
                }
            }
        }
    }

    #[tokio::test]
    async fn transport_errors_surface_after_an_ordered_prefix() {
        let state = State::new(0, 1, false);
        // An aggregate map key and an info field's array hold large payloads,
        // so failures land while their enclosing aggregates wait.
        let map = || {
            Resp::Map(vec![
                (
                    Resp::Array(Some(vec![bulk(OUTPUT_BUFFER_BYTES + 7, 4), bulk(9, 5)])),
                    Resp::InfoField(
                        Bytes::from_static(b"field"),
                        Box::new(Resp::Array(Some(vec![
                            bulk(2 * OUTPUT_BUFFER_BYTES, 6),
                            Resp::Integer(3),
                        ]))),
                    ),
                ),
                (
                    bulk(4, 7),
                    Resp::Set(vec![
                        bulk(BULK_COPY_BYTES, 8),
                        Resp::Array(Some(Vec::new())),
                    ]),
                ),
            ])
        };
        let reply = || {
            Resp::Array(Some(vec![
                bulk(100, 1),
                bulk(3 * OUTPUT_BUFFER_BYTES, 2),
                Resp::Status(Bytes::from(text(2 * OUTPUT_BUFFER_BYTES + 5))),
                Resp::Array(Some((0..1000).map(|i| bulk(i % 29, 3)).collect())),
                map(),
            ]))
        };
        let expected = encoded([reply()], Version::Resp3);
        let map_at = expected.len() - encoded([map()], Version::Resp3).len();
        for fail_at in [
            0,
            1,
            50,
            OUTPUT_BUFFER_BYTES,
            OUTPUT_BUFFER_BYTES + 1,
            2 * OUTPUT_BUFFER_BYTES,
            // Among the small members, then inside the map key's payload and
            // the info field's.
            map_at - 100,
            map_at + OUTPUT_BUFFER_BYTES / 2,
            map_at + 2 * OUTPUT_BUFFER_BYTES,
            expected.len() - 1,
        ] {
            for vectored in [false, true] {
                let mut transport = Transport::new(4093, vectored, fail_at);
                let mut out = Output::new(&state, "test");
                let error = deliver_one(&mut out, &mut transport, reply(), Version::Resp3)
                    .await
                    .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset);
                transport.delivered.append(&mut transport.staged);
                assert!(
                    transport.delivered[..] == expected[..fail_at],
                    "fail at {fail_at}, vectored {vectored}"
                );
            }
        }
    }

    #[tokio::test]
    async fn commands_complete_only_once_a_flush_delivers_their_replies() {
        let state = Arc::new(State::new(0, 1, false));
        let connection = client(&state);
        let metrics = state.metrics.clone();
        let completed = || metrics.snapshot().commands_total;
        let mut out = Output::new(&state, "test");
        // Short writes and stalls: replies leave over many delivery steps.
        let mut transport = Transport::new(7, false, usize::MAX);
        out.reply(
            &mut transport,
            bulk(100, 1),
            Version::Resp2,
            command(&out, &connection, &state, 20),
        )
        .await
        .unwrap();
        out.record(command(&out, &connection, &state, 20));
        while !out.idle() {
            assert_eq!(completed(), 0, "completed before a flush delivered it");
            out.deliver(&mut transport).await.unwrap();
        }
        assert_eq!(
            completed(),
            2,
            "a command without a reply follows earlier replies"
        );
        // A payload written straight from its `Bytes` completes on flush too.
        out.reply(
            &mut transport,
            bulk(3 * OUTPUT_BUFFER_BYTES, 2),
            Version::Resp2,
            command(&out, &connection, &state, 20),
        )
        .await
        .unwrap();
        assert_eq!(completed(), 2);
        out.settle(&mut transport).await.unwrap();
        assert_eq!(completed(), 3);
        assert_eq!(metrics.snapshot().errors_total, 0);
        let expected = encoded(
            [bulk(100, 1), bulk(3 * OUTPUT_BUFFER_BYTES, 2)],
            Version::Resp2,
        );
        assert!(transport.delivered[..] == expected[..]);
    }

    #[tokio::test]
    async fn a_failed_write_fails_only_the_commands_it_left_undelivered() {
        let small = || bulk(10, 1);
        let large = || bulk(3 * OUTPUT_BUFFER_BYTES, 2);
        let expected = encoded([small(), small(), large()], Version::Resp2);
        let small_len = encoded([small()], Version::Resp2).len();
        // Inside the second small reply, inside the large payload, and at the
        // final CRLF, which only settling writes.
        for fail_at in [small_len + 3, 2 * small_len + 100, expected.len() - 1] {
            for vectored in [false, true] {
                let context = format!("fail at {fail_at}, vectored {vectored}");
                let state = Arc::new(State::new(0, 1, false));
                let connection = client(&state);
                let metrics = state.metrics.clone();
                let mut transport = Transport::new(4093, vectored, fail_at);
                let mut out = Output::new(&state, "test");
                out.reply(
                    &mut transport,
                    small(),
                    Version::Resp2,
                    command(&out, &connection, &state, 20),
                )
                .await
                .unwrap();
                out.settle(&mut transport).await.unwrap();
                let error = async {
                    for reply in [small(), large()] {
                        let done = command(&out, &connection, &state, 20);
                        out.reply(&mut transport, reply, Version::Resp2, done)
                            .await?;
                    }
                    out.settle(&mut transport).await
                }
                .await
                .unwrap_err();
                assert_eq!(error.kind(), io::ErrorKind::ConnectionReset, "{context}");
                drop(out);
                let snapshot = metrics.snapshot();
                assert_eq!(snapshot.commands_total, 3, "{context}");
                assert_eq!(
                    snapshot.errors_total, 2,
                    "{context}: only the flushed reply completes"
                );
                transport.delivered.append(&mut transport.staged);
                assert!(transport.delivered[..] == expected[..fail_at], "{context}");
            }
        }
    }

    #[tokio::test]
    async fn undelivered_commands_stay_within_their_count_and_request_bounds() {
        let state = Arc::new(State::new(0, 1, false));
        let connection = client(&state);
        let metrics = state.metrics.clone();
        let mut transport = Transport::new(usize::MAX, true, usize::MAX);
        let mut out = Output::new(&state, "test");
        assert!(
            out.admits(RETAINED_INPUT_BYTES),
            "an empty ledger admits any request"
        );
        for _ in 0..LEDGER_COMMANDS {
            assert!(out.admits(20));
            let done = command(&out, &connection, &state, 20);
            out.reply(&mut transport, Resp::Simple("OK"), Version::Resp2, done)
                .await
                .unwrap();
        }
        assert!(!out.admits(20), "a full ledger is delivered first");
        out.settle(&mut transport).await.unwrap();
        assert_eq!(metrics.snapshot().commands_total, LEDGER_COMMANDS as u64);
        let done = command(&out, &connection, &state, RETAINED_INPUT_BYTES - 20);
        out.reply(&mut transport, Resp::Simple("OK"), Version::Resp2, done)
            .await
            .unwrap();
        assert!(out.admits(20));
        assert!(
            !out.admits(21),
            "retained requests stay within the framing cap"
        );
        out.settle(&mut transport).await.unwrap();
        assert!(out.admits(RETAINED_INPUT_BYTES));
        assert_eq!(
            transport.calls, 2,
            "each burst of replies left in one write"
        );
        assert_eq!(metrics.snapshot().errors_total, 0);
    }

    #[tokio::test]
    async fn a_publication_awaiting_its_flush_still_counts_toward_the_subscriber_limit() {
        let state = Arc::new(State::new(0, 1, false));
        let channel = Bytes::from_static(b"news");
        let payload = Bytes::from_static(b"x");
        let frame: &[u8] = b"*3\r\n$7\r\nmessage\r\n$4\r\nnews\r\n$1\r\nx\r\n";
        // A subscriber that never reads sets the limit the stalled one shares.
        let mut idle = client(&state);
        let mut stalled = client(&state);
        for subscriber in [&mut idle, &mut stalled] {
            subscriber
                .subscription
                .command(&[Bytes::from_static(b"SUBSCRIBE"), channel.clone()])
                .unwrap();
            drop(subscriber.subscription.try_recv().expect("acknowledged"));
        }
        let idle_cancelled = idle.subscription.cancellation();
        let stalled_cancelled = stalled.subscription.cancellation();
        assert_eq!(state.pubsub.publish(&channel, &payload), Ok(2));

        let peer = Arc::new(Mutex::new(Peer::default()));
        let mut stream = PeerStream(peer.clone());
        let mut out = Output::new(&state, "test");
        // As `serve`: publish a burst of queued messages, dropping each once
        // `publish` returns, then settle before reading the client.
        let mut connection = std::pin::pin!(async {
            for _ in 0..16 {
                let Some(mut message) = stalled.subscription.try_recv() else {
                    break;
                };
                out.publish(&mut stream, &mut message, stalled.version)
                    .await?;
            }
            out.settle(&mut stream).await
        });
        assert!(futures_util::poll!(connection.as_mut()).is_pending());
        {
            let seen = peer.lock().unwrap();
            assert_eq!(
                seen.staged, frame,
                "the publication is encoded and accepted"
            );
            assert!(seen.delivered.is_empty(), "but its flush is stalled");
        }

        // The publisher keeps going while the stalled peer receives nothing.
        let mut accepted = 1;
        let receivers = loop {
            let receivers = state.pubsub.publish(&channel, &payload).unwrap();
            if receivers != 2 {
                break receivers;
            }
            accepted += 1;
            assert!(accepted <= 4096, "publications were never bounded");
        };
        assert_eq!(
            receivers, 0,
            "after {accepted} publications, a subscriber stalled on a flush \
             outlived the limit of one that never reads"
        );
        assert!(*idle_cancelled.borrow());
        assert!(*stalled_cancelled.borrow());

        // Releasing the flush delivers the held publication first.
        {
            let mut seen = peer.lock().unwrap();
            seen.open = true;
            seen.flush.take().expect("a stalled flush").wake();
        }
        tokio::time::timeout(Duration::from_secs(10), connection)
            .await
            .expect("the released connection settles")
            .unwrap();
        let seen = peer.lock().unwrap();
        assert!(seen.delivered.starts_with(frame));
        assert!(seen.staged.is_empty());
    }

    #[tokio::test]
    async fn one_delivery_observes_a_pipeline_in_order_with_each_reply_status() {
        let (state, server) = observed_state();
        let connection = client(&state);
        let metrics = state.metrics.clone();
        let mut monitor = server.monitor();
        let mut transport = Transport::new(usize::MAX, true, usize::MAX);
        let mut out = Output::new(&state, "test");
        let error = Resp::Error("ERR value is not an integer or out of range".into());
        // (request, reply or none, reply is an error)
        let pipeline: [(&[&str], Option<Resp>, bool); 4] = [
            (&["SET", "k", "v"], Some(Resp::Simple("OK")), false),
            (&["INCR", "k"], Some(error), true),
            (&["PING"], None, false),
            (&["GET", "k"], Some(bulk(1, b'v')), false),
        ];
        for (args, reply, failed) in pipeline {
            let done = request(&out, &connection, &state, args, 20, failed);
            match reply {
                Some(reply) => out
                    .reply(&mut transport, reply, Version::Resp2, done)
                    .await
                    .unwrap(),
                // Completes only after the replies before it.
                None => out.record(done),
            }
        }
        assert_eq!(metrics.snapshot().commands_total, 0);
        assert!(slowlog(&server).await.is_empty());
        expect_monitored(&mut monitor, &[]);

        out.settle(&mut transport).await.unwrap();
        let snapshot = metrics.snapshot();
        assert_eq!((snapshot.commands_total, snapshot.errors_total), (4, 1));
        let order = ["SET k v", "INCR k", "PING", "GET k"];
        assert_eq!(slowlog(&server).await, order);
        expect_monitored(&mut monitor, &order);
        drop(out);
        let snapshot = metrics.snapshot();
        assert_eq!(
            (snapshot.commands_total, snapshot.errors_total),
            (4, 1),
            "delivered commands are reported once"
        );
    }

    #[tokio::test]
    async fn requests_parsed_into_reused_argument_vectors_are_observed_with_their_own() {
        let (state, server) = observed_state();
        let connection = client(&state);
        let mut monitor = server.monitor();
        let mut transport = Transport::new(usize::MAX, true, usize::MAX);
        let mut out = Output::new(&state, "test");
        let fields: Vec<String> = (0..20).map(|i| format!("f{i} v{i}")).collect();
        let wide = format!("HSET h {}", fields.join(" "));
        // Each burst parses into the vectors earlier deliveries left behind:
        // longer, shorter and oversized requests alike.
        let bursts: [&[&str]; 3] = [
            &["SET k first", "MSET a 1 b 2 c 3", "GET k"],
            &[
                "GET a",
                "PING",
                wide.as_str(),
                "SET k second",
                "MSET a 1 b 2 c 3",
            ],
            &["DEL a b", "INCR k", wide.as_str(), "GET k"],
        ];
        for burst in bursts {
            let mut input = BytesMut::new();
            for command in burst {
                input.extend_from_slice(command.as_bytes());
                input.extend_from_slice(b"\r\n");
            }
            while let Some(args) =
                crate::protocol::parse(&mut input, |count| out.arguments(count)).unwrap()
            {
                let done = CommandRecord {
                    timer: out.timer(Instant::now()),
                    started: Instant::now(),
                    cpu_start: 0,
                    input_bytes: 20,
                    builtin: engine::BoundCommand::bind(&args).exists(),
                }
                .complete(args, false, &connection, &state);
                out.reply(&mut transport, Resp::Simple("OK"), Version::Resp2, done)
                    .await
                    .unwrap();
            }
            expect_monitored(&mut monitor, &[]);
            out.settle(&mut transport).await.unwrap();
            expect_monitored(&mut monitor, burst);
        }
    }

    #[tokio::test]
    async fn a_stalled_flush_observes_nothing_and_dropping_fails_the_rest_once() {
        let (state, server) = observed_state();
        let connection = client(&state);
        let metrics = state.metrics.clone();
        let totals = || {
            let snapshot = metrics.snapshot();
            (snapshot.commands_total, snapshot.errors_total)
        };
        let mut monitor = server.monitor();
        let peer = Arc::new(Mutex::new(Peer::default()));
        let mut stream = PeerStream(peer.clone());
        let mut out = Output::new(&state, "test");
        let error = || Resp::Error("ERR value is not an integer or out of range".into());

        let done = request(&out, &connection, &state, &["SET", "k", "v"], 20, false);
        out.reply(&mut stream, Resp::Simple("OK"), Version::Resp2, done)
            .await
            .unwrap();
        let done = request(&out, &connection, &state, &["INCR", "k"], 20, true);
        out.reply(&mut stream, error(), Version::Resp2, done)
            .await
            .unwrap();
        {
            let mut settling = std::pin::pin!(out.settle(&mut stream));
            assert!(futures_util::poll!(settling.as_mut()).is_pending());
            assert!(
                !peer.lock().unwrap().staged.is_empty(),
                "the session accepted the replies"
            );
            // Accepted but not flushed is not delivered.
            assert_eq!(totals(), (0, 0));
            assert!(slowlog(&server).await.is_empty());
            expect_monitored(&mut monitor, &[]);
            {
                let mut seen = peer.lock().unwrap();
                seen.open = true;
                seen.flush.take().expect("a stalled flush").wake();
            }
            settling.await.unwrap();
        }
        assert_eq!(totals(), (2, 1), "each delivered with its own reply status");
        assert_eq!(slowlog(&server).await, ["SET k v", "INCR k"]);
        expect_monitored(&mut monitor, &["SET k v", "INCR k"]);

        // Stalled again, the connection ends: every undelivered command fails
        // once, unobserved, whatever its reply.
        peer.lock().unwrap().open = false;
        let done = request(&out, &connection, &state, &["GET", "k"], 20, false);
        out.reply(&mut stream, bulk(1, b'v'), Version::Resp2, done)
            .await
            .unwrap();
        out.record(request(&out, &connection, &state, &["PING"], 20, false));
        let done = request(&out, &connection, &state, &["INCR", "k"], 20, true);
        out.reply(&mut stream, error(), Version::Resp2, done)
            .await
            .unwrap();
        {
            let mut settling = std::pin::pin!(out.settle(&mut stream));
            assert!(futures_util::poll!(settling.as_mut()).is_pending());
        }
        assert_eq!(totals(), (2, 1));
        drop(out);
        assert_eq!(totals(), (5, 4));
        assert_eq!(slowlog(&server).await, ["SET k v", "INCR k"]);
        expect_monitored(&mut monitor, &[]);
    }
}
