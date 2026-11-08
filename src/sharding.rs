//! Single-key commands own one shard. Each shard engine lives in a cell that one
//! holder leases at a time: its queue worker, a connection running a memory-only
//! command inline, or a pause holding every cell. A connection that finds the
//! cell held hands the command to the shard's worker, which runs those queued
//! commands under one hold of the cell. Coordinated commands pause behind a
//! write barrier and move only the affected entries, not their payloads.

use std::{
    collections::HashSet,
    io,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use rand::Rng;
use tokio::{
    sync::{
        Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, mpsc, oneshot,
        watch,
    },
    task::JoinSet,
};

use crate::{
    engine::{BoundCommand, Engine, Execution, GlobalCommand, Routing},
    journal::Batch,
    persistence::{Persistence, Staged},
    protocol::Resp,
    request::{CommandRequest, Origin, SingleKey},
    state::State,
    write_gate::Permit,
};

/// One shard's queue senders, which never change once assembled, so shards
/// may share cache lines. Inline leases never read them: what they read for
/// each command is the shard's `Pending`.
struct Shard {
    sender: mpsc::Sender<ShardRequest>,
    /// Commands connections found this shard's cell held for.
    contended: mpsc::Sender<Contended>,
}

/// Contended commands of one shard counted from before their hand-off until
/// its worker has answered or dropped the batch that took them, on a cache
/// line of its own: inline leases on every frontend read it for each command,
/// and only contended commands and their worker write it.
#[repr(align(128))]
#[derive(Default)]
struct Pending(AtomicUsize);

/// Contended commands counted in their shard's `Pending` until dropped,
/// cancelled or not, unless handed on with `queued`.
struct Counted<'a> {
    pending: &'a AtomicUsize,
    commands: usize,
}

impl<'a> Counted<'a> {
    /// Counts one more contended command.
    fn count(pending: &'a AtomicUsize) -> Self {
        pending.fetch_add(1, Ordering::Relaxed);
        Self {
            pending,
            commands: 1,
        }
    }

    /// `commands` already counted, which their worker received: uncounted
    /// once it answered or dropped them, however it leaves the batch.
    fn received(pending: &'a AtomicUsize, commands: usize) -> Self {
        Self { pending, commands }
    }

    /// The queue holds the command now, so its worker uncounts it.
    fn queued(self) {
        std::mem::forget(self);
    }
}

impl Drop for Counted<'_> {
    fn drop(&mut self) {
        self.pending.fetch_sub(self.commands, Ordering::Relaxed);
    }
}

/// One shard engine and its lease state. `engine` is `None` only while a
/// `Paused` holds it; `busy` spans one command or maintenance pass from its
/// first mutation until its commit settles. A holder that unwinds, fails or is
/// cancelled mid-command, or a pause dropped without `resume`, leaves the cell
/// busy or empty: poisoned, so no lease ever observes a half-applied command
/// or a placeholder engine.
struct ShardCell {
    engine: Option<Engine>,
    busy: bool,
    shared: Arc<Shared>,
}

impl ShardCell {
    fn usable(&self) -> bool {
        !self.busy && self.engine.is_some()
    }

    /// Marks a command started and lends the engine for it.
    fn begin(&mut self) -> io::Result<&mut Engine> {
        if self.busy {
            return Err(poisoned());
        }
        let engine = self.engine.as_mut().ok_or_else(poisoned)?;
        self.busy = true;
        Ok(engine)
    }

    /// Runs `command` on this cell's engine for the cell's holder, at most
    /// once; see `ShardLease::run`. A commit that cannot settle synchronously
    /// returns staged, the cell busy until its holder settles it. Meant to be
    /// inlined into both holders, so an inline lease's `run` stays one
    /// function that maps its outcome without an intermediate `Step` copy.
    #[inline]
    fn step(&mut self, command: &mut SingleKey<'_>, state: &State) -> io::Result<Step> {
        let Self {
            engine,
            busy,
            shared,
        } = self;
        shared.check()?;
        let (false, Some(engine)) = (*busy, engine.as_mut()) else {
            return Err(poisoned());
        };
        // Role changes and waiter registration happen only while a pause holds
        // every cell, so this holder reads both stably.
        if state.is_replica()
            || shared.waiters.load(Ordering::Acquire)
            || (!shared.persistence.commits_in_memory() && command.command.is_dataset_write())
        {
            return Ok(Step::Escalate);
        }
        if let Some(reply) = crate::pressure::reject_pressure(&state.metrics, &command.command) {
            return Ok(Step::Done(reply));
        }
        let persistence = &*shared.persistence;
        *busy = true;
        let executed = execute(
            engine,
            persistence,
            CommandInput::Bound(&mut command.command),
            command.origin,
            command.user.take(),
            command.session.take(),
            state.cpu_observed(),
        )
        .and_then(|execution| {
            // `reset` clears the session next, so the stage takes it.
            let staged = persistence.stage_detaching(engine);
            reset(engine, persistence);
            Ok((staged, single_reply(execution)?))
        });
        let (staged, reply) = match executed {
            Ok(executed) => executed,
            Err(error) => {
                shared.fatal(&error);
                return Err(error);
            }
        };
        match persistence.try_commit_now(staged) {
            Ok(Ok(())) => {
                *busy = false;
                Ok(Step::Done(reply))
            }
            Ok(Err(error)) => {
                shared.fatal(&error);
                Err(error)
            }
            Err(staged) => Ok(Step::Staged(staged, reply)),
        }
    }
}

/// How far `ShardCell::step` took one command.
enum Step {
    /// Not executed: the command returns untouched.
    Escalate,
    /// Refused, or executed and committed.
    Done(Resp),
    /// Executed; its holder settles the commit, then replies, and only then
    /// releases the cell.
    Staged(Staged, Resp),
}

/// Pool-wide state every lease reaches through its cell, so leasing costs no
/// shared reference count.
struct Shared {
    persistence: Arc<Persistence>,
    /// Conservative "blocked clients may exist". Written only through
    /// `Paused::publish_waiters` while every cell is held; inline commands read
    /// it under their own cell, so no push can miss a registered waiter.
    waiters: AtomicBool,
    /// Fast mirror of `status` leaving `Running`.
    stopped: AtomicBool,
    status: watch::Sender<Status>,
}

#[derive(Clone)]
enum Status {
    Running,
    /// `Pool::close` drained every command and lease.
    Closed,
    Fatal(io::ErrorKind, String),
}

impl Shared {
    fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    fn check(&self) -> io::Result<()> {
        if self.is_stopped() {
            return Err(stopped());
        }
        self.persistence.ensure_healthy()
    }

    /// Stops every lease and reports the first cause to the supervisor task.
    fn fatal(&self, error: &io::Error) {
        self.stopped.store(true, Ordering::Release);
        self.status.send_if_modified(|status| {
            if !matches!(status, Status::Running) {
                return false;
            }
            *status = Status::Fatal(error.kind(), error.to_string());
            true
        });
    }
}

/// How a stopped pool's tasks finish: a drained `close` is clean.
fn halted(status: &Status) -> io::Result<()> {
    match status {
        Status::Fatal(kind, cause) => Err(io::Error::new(*kind, cause.clone())),
        Status::Running | Status::Closed => Ok(()),
    }
}

/// The coordinator's hold on the pool. Connections may keep the pool itself
/// alive, so a coordinator leaving without `Pool::close` (an error or a panic)
/// must still stop every lease, pause, admission and worker at once. Commands
/// that already mutated keep settling their commits. Holds no pool or sender.
#[must_use = "dropping the lifetime stops the pool"]
pub struct CoordinatorLifetime {
    shared: Arc<Shared>,
}

impl Drop for CoordinatorLifetime {
    fn drop(&mut self) {
        // After `close` the status is `Closed`, which this leaves untouched.
        self.shared
            .fatal(&io::Error::other("shard coordinator stopped"));
    }
}

/// Pool-barrier read share. While any instance is alive, `Pool::pause` cannot
/// finish; only `Pool::admit` creates one.
#[must_use = "dropping an admission early lets a pause overtake the command"]
pub struct Admission {
    _guard: OwnedRwLockReadGuard<()>,
}

/// An admitted single-key command. The worker settles it end to end and
/// releases the admission and write permit only after replying.
struct ShardRequest {
    request: CommandRequest,
    admission: Admission,
}

/// A connection's command for a shard whose cell it found held, for that
/// shard's worker to run as `ShardLease::run` would. Unlike an admitted
/// `ShardRequest` it stays its client's until the worker claims it under the
/// cell: one whose `reply` closed by then is dropped unexecuted, as a dropped
/// lease wait runs nothing. A claimed one settles, commit included, whatever
/// becomes of its client. The worker always releases the write permit itself,
/// after answering or when it returns the command unexecuted: no answer
/// carries an admission guard, so one its client has yet to take holds off no
/// write pause.
struct Contended {
    args: Vec<Bytes>,
    origin: Origin,
    user: Option<Arc<crate::auth::User>>,
    session: Option<Arc<crate::session::ClientSession>>,
    write_permit: Option<Permit>,
    reply: oneshot::Sender<Served>,
}

/// What a shard worker did with a `Contended` command.
enum Served {
    /// Executed and committed.
    Done(Resp),
    /// Not executed: for the coordinator, with the command's identity but not
    /// its write permit.
    Escalate {
        user: Option<Arc<crate::auth::User>>,
        session: Option<Arc<crate::session::ClientSession>>,
    },
}

/// How `Pool::contend` ended.
pub enum Contention<'a> {
    /// Executed and committed.
    Done(Resp),
    /// Not executed and holding nothing of the shard: dispatch through the
    /// coordinator. Its worker released its write permit, so a write must be
    /// admitted again first.
    Escalate(SingleKey<'a>),
}

/// One shard worker's queues.
struct Queues {
    requests: mpsc::Receiver<ShardRequest>,
    contended: mpsc::Receiver<Contended>,
    /// The pool's `pending`, of which this worker uncounts `shard`'s.
    pending: Arc<[Pending]>,
    shard: usize,
}

/// Most contended commands a worker runs under one hold of its cell. It takes
/// only commands already queued before it asks for the cell, so a pause
/// queued behind that hold waits for at most these, and commands arriving
/// later queue behind the pause.
const CONTENDED_BATCH: usize = 32;

pub struct Pool {
    shards: Vec<Shard>,
    /// Each shard's contended commands, which inline leases give way to.
    pending: Arc<[Pending]>,
    cells: Vec<Arc<Mutex<ShardCell>>>,
    gate: Arc<RwLock<()>>,
    shared: Arc<Shared>,
    /// Hybrid engines may read values from disk; they never run inline, and
    /// each keeps a dedicated blocking worker thread.
    memory: bool,
    /// Whether `try_lease_spinning` spins, set once at creation: a holder can
    /// run while a contender spins only if the runtime starting the pool has
    /// several workers, and so `main` several frontends, and the host several
    /// CPUs.
    spin: bool,
}

/// Retries of a contended `Pool::try_lease_spinning`, each after twice the
/// `spin_loop` hints of the one before: 63 hints in all, a fixed bound needing
/// no clock. The hypothesis to measure: a cell held for one short inline
/// command on another thread often frees within it, sooner than the shard's
/// worker, woken across runtimes, would run the command.
const SPIN_RETRIES: u32 = 6;

pub struct Paused {
    pub engines: Vec<Engine>,
    /// Every cell in ascending order, each emptied into `engines`.
    cells: Vec<OwnedMutexGuard<ShardCell>>,
    _gate: OwnedRwLockWriteGuard<()>,
}

/// Exclusive hold on one shard engine outside its queue. Dropping a lease that
/// never ran a command is a clean release.
#[must_use = "a lease blocks its shard until dropped"]
pub struct ShardLease {
    cell: OwnedMutexGuard<ShardCell>,
}

pub enum Ran<'a> {
    /// Executed and committed.
    Done(Resp),
    /// Executed; a supervised task owning the lease, write permit and staged
    /// effects finishes the commit even if this receiver is dropped, then
    /// replies. `Err` or a closed channel means the engine stopped.
    Committing(oneshot::Receiver<io::Result<Resp>>),
    /// Not executed, holding nothing of the shard: dispatch through the
    /// coordinator.
    Escalate(SingleKey<'a>),
}

fn stopped() -> io::Error {
    io::Error::other("shard actor stopped")
}
fn poisoned() -> io::Error {
    io::Error::other("shard engine poisoned by an unsettled command")
}
fn invalid_reply() -> io::Error {
    io::Error::other("invalid internal shard response")
}

impl Pool {
    /// `state` answers the role and memory-pressure questions a worker asks
    /// under the cell for a contended command, as a lease's caller does.
    pub fn start(
        engine: Engine,
        count: usize,
        persistence: Arc<Persistence>,
        state: Arc<State>,
    ) -> (Arc<Self>, JoinSet<io::Result<()>>) {
        let (pool, queues) = Self::assemble(engine, count, 1024, persistence);
        let mut workers = JoinSet::new();
        let shards = pool.cells.iter().zip(queues).map(|(cell, queues)| {
            run_worker(
                cell.clone(),
                queues,
                pool.shared.persistence.clone(),
                state.clone(),
                pool.shared.status.subscribe(),
            )
        });
        let runtime = tokio::runtime::Handle::current();
        if pool.memory {
            // Memory workers never block on disk, so they share the runtime as
            // tasks instead of holding one OS thread per shard.
            let group: Vec<_> = shards.collect();
            let shared = pool.shared.clone();
            workers.spawn_blocking(move || runtime.block_on(own_workers(shared, group)));
        } else {
            for worker in shards {
                let runtime = runtime.clone();
                workers.spawn_blocking(move || runtime.block_on(worker));
            }
        }
        // Inline leases, deferred commits and grouped memory workers have no
        // task of their own whose failure the coordinator could join at once;
        // they report through this one.
        workers.spawn(supervise(pool.shared.status.subscribe()));
        (Arc::new(pool), workers)
    }

    /// Cells and bounded queues without workers; `start` attaches one each.
    fn assemble(
        mut engine: Engine,
        count: usize,
        capacity: usize,
        persistence: Arc<Persistence>,
    ) -> (Self, Vec<Queues>) {
        let memory = engine.storage().is_none();
        engine.set_watches(persistence.watches.clone());
        let shared = Arc::new(Shared {
            persistence,
            waiters: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            status: watch::Sender::new(Status::Running),
        });
        let mut shards = Vec::with_capacity(count);
        let mut cells = Vec::with_capacity(count);
        let mut queues = Vec::with_capacity(count);
        let pending: Arc<[Pending]> = std::iter::repeat_with(Pending::default)
            .take(count)
            .collect();
        for (shard, engine) in engine.partition(count).into_iter().enumerate() {
            let (sender, requests) = mpsc::channel(capacity);
            let (contended, queued) = mpsc::channel(capacity);
            shards.push(Shard { sender, contended });
            queues.push(Queues {
                requests,
                contended: queued,
                pending: pending.clone(),
                shard,
            });
            cells.push(Arc::new(Mutex::new(ShardCell {
                engine: Some(engine),
                busy: false,
                shared: shared.clone(),
            })));
        }
        // Holders run on other threads only with several workers and CPUs;
        // without a runtime nothing says so.
        let spin = tokio::runtime::Handle::try_current()
            .is_ok_and(|runtime| runtime.metrics().num_workers() > 1)
            && std::thread::available_parallelism().is_ok_and(|cpus| cpus.get() > 1);
        let pool = Self {
            shards,
            pending,
            cells,
            gate: Arc::new(RwLock::new(())),
            shared,
            memory,
            spin,
        };
        (pool, queues)
    }

    /// Taken by the coordinator right after `start`; see `CoordinatorLifetime`.
    pub fn lifetime(&self) -> CoordinatorLifetime {
        CoordinatorLifetime {
            shared: self.shared.clone(),
        }
    }

    pub async fn admit(&self) -> io::Result<Admission> {
        let guard = self.gate.clone().read_owned().await;
        self.shared.check()?;
        Ok(Admission { _guard: guard })
    }

    /// Hands an admitted command to its shard actor without awaiting; the actor
    /// replies on `request.reply`. Only a full queue defers the hand-off, into a
    /// task on `overflow` that keeps the request and admission until enqueued and
    /// fails if the actor stops. `Err` means the pool or the actor already
    /// stopped; the request was dropped unexecuted.
    pub fn submit(
        &self,
        shard: usize,
        request: CommandRequest,
        admission: Admission,
        overflow: &mut JoinSet<io::Result<()>>,
    ) -> io::Result<()> {
        if self.shared.is_stopped() {
            return Err(stopped());
        }
        let sender = &self.shards[shard].sender;
        match sender.try_send(ShardRequest { request, admission }) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(command)) => {
                let sender = sender.clone();
                overflow.spawn(async move { sender.send(command).await.map_err(|_| stopped()) });
                Ok(())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => Err(stopped()),
        }
    }

    /// The shard a connection may run `command` on itself: one local shard of a
    /// memory-backed, non-cluster engine and no coordinator interception. Only
    /// classification, fixed for a command once the server serves clients;
    /// whoever holds the cell, a lease or for `contend` the shard's worker,
    /// decides the rest under it.
    pub fn inline_shard(&self, command: &BoundCommand<'_>, state: &State) -> Option<usize> {
        if !self.memory || state.cluster.get().is_some() {
            return None;
        }
        let name = command.args().first()?;
        if [b"PUBLISH".as_slice(), b"SPUBLISH", b"PUBSUB", b"INFO"]
            .iter()
            .any(|intercepted| name.eq_ignore_ascii_case(intercepted))
        {
            return None;
        }
        match command.routing(self.cells.len()) {
            Routing::Local { shard, .. } => Some(shard),
            _ => None,
        }
    }

    /// Whether a holder of `command`'s cell would run it rather than return it
    /// untouched: not a dataset write whose commit already needs the WAL or an
    /// online backup. A connection about to wait or queue for the cell sends
    /// such a write to the coordinator at once instead; one that takes the
    /// cell without waiting leaves the refusal to its lease. Advisory: the
    /// check under the cell stays authoritative across cutovers.
    pub fn commits_inline(&self, command: &BoundCommand<'_>) -> bool {
        !command.is_dataset_write() || self.shared.persistence.commits_in_memory()
    }

    /// Whether contended commands are counted for `shard`, queued for its
    /// worker or in the batch it holds the cell for, so inline leases give way
    /// rather than take the cell ahead of them. One relaxed load of the
    /// shard's own `Pending` line. Only a hint: the cell lock's FIFO alone
    /// orders holders.
    fn waiting(&self, shard: usize) -> bool {
        self.pending[shard].0.load(Ordering::Relaxed) != 0
    }

    /// Never overtakes a queued worker or pause, the cell lock being FIFO, and
    /// gives way while contended commands wait for the shard's worker.
    pub fn try_lease(self: &Arc<Self>, shard: usize) -> Option<ShardLease> {
        if self.waiting(shard) {
            return None;
        }
        let cell = self.cells[shard].clone().try_lock_owned().ok()?;
        Some(ShardLease { cell })
    }

    /// As `try_lease`, but while the pool may spin and no client command
    /// waits for the shard, retries `SPIN_RETRIES` times with doubling
    /// `spin_loop` hints between, then gives up: a cell another thread holds
    /// for one short command may free within that bound. Never takes a cell
    /// handed to a queued worker, pause or lease, nor queues itself.
    pub fn try_lease_spinning(self: &Arc<Self>, shard: usize) -> Option<ShardLease> {
        if let Some(lease) = self.try_lease(shard) {
            return Some(lease);
        }
        if !self.spin {
            return None;
        }
        for retry in 0..SPIN_RETRIES {
            if self.waiting(shard) {
                return None;
            }
            for _ in 0..1_u32 << retry {
                std::hint::spin_loop();
            }
            if let Some(lease) = self.try_lease(shard) {
                return Some(lease);
            }
        }
        None
    }

    /// Hands `command` to its shard's worker instead of waiting for the cell,
    /// so no lease passes between connections' runtimes per waiting command:
    /// the worker runs it under the cell as `ShardLease::run` would, among
    /// the commands queued for the shard when it took the cell. Until the
    /// worker claims it, dropping the future drops it unexecuted; once
    /// claimed it settles, commit included, whatever becomes of the future.
    /// The worker owns the command's write permit from here on and releases
    /// it itself. A full queue holds the future back. Inline leases give way
    /// from its first poll until its worker answered or dropped it, or the
    /// future is dropped before the queue took it. `Err` means the engine
    /// stopped.
    pub fn contend<'a>(
        &self,
        shard: usize,
        mut command: SingleKey<'a>,
    ) -> impl Future<Output = io::Result<Contention<'a>>> + Send + use<'_, 'a> {
        let queue = &self.shards[shard].contended;
        let pending = &self.pending[shard].0;
        async move {
            let counted = Counted::count(pending);
            // The worker owns all it may run after this future is gone.
            let (reply, served) = oneshot::channel();
            let request = Contended {
                args: command.command.args().to_vec(),
                origin: command.origin,
                user: command.user.take(),
                session: command.session.take(),
                write_permit: command.write_permit.take(),
                reply,
            };
            queue.send(request).await.map_err(|_| stopped())?;
            counted.queued();
            Ok(match served.await.map_err(|_| stopped())? {
                Served::Done(reply) => Contention::Done(reply),
                Served::Escalate { user, session } => {
                    command.user = user;
                    command.session = session;
                    Contention::Escalate(command)
                }
            })
        }
    }

    /// Waits in FIFO order for the cell; dropping the future executes nothing.
    #[cfg(test)]
    pub fn lease(
        self: &Arc<Self>,
        shard: usize,
    ) -> impl Future<Output = io::Result<ShardLease>> + Send + use<'_> {
        let cell = self.cells[shard].clone();
        async move {
            let cell = cell.lock_owned().await;
            cell.shared.check()?;
            if !cell.usable() {
                return Err(poisoned());
            }
            Ok(ShardLease { cell })
        }
    }

    /// Drains admitted queue commands through the barrier, then takes every
    /// cell in ascending order. Leases, deferred commits and worker batches of
    /// contended commands that hold or already queued for a cell finish first;
    /// any later lease or batch waits behind this pause.
    pub async fn pause(&self) -> io::Result<Paused> {
        let gate = self.gate.clone().write_owned().await;
        self.shared.check()?;
        if self.shards.iter().any(|shard| shard.sender.is_closed()) {
            return Err(stopped());
        }
        let mut cells = Vec::with_capacity(self.cells.len());
        for cell in &self.cells {
            cells.push(cell.clone().lock_owned().await);
        }
        self.shared.check()?;
        if !cells.iter().all(|cell| cell.usable()) {
            return Err(poisoned());
        }
        let engines = cells
            .iter_mut()
            .map(|cell| cell.engine.take().expect("usable cell holds its engine"))
            .collect();
        Ok(Paused {
            engines,
            cells,
            _gate: gate,
        })
    }

    /// Shutdown: drain every admitted command, lease and deferred commit, then
    /// refuse leases and pauses and let workers and the supervisor finish.
    pub async fn close(&self) -> io::Result<()> {
        let paused = self.pause().await?;
        self.shared.stopped.store(true, Ordering::Release);
        self.shared.status.send_if_modified(|status| {
            if !matches!(status, Status::Running) {
                return false;
            }
            *status = Status::Closed;
            true
        });
        paused.resume()
    }
}

impl ShardLease {
    /// Runs `command` on this shard synchronously, at most once. Escalates
    /// before any mutation for replicas, possible blocked clients, and writes
    /// whose commit needs the WAL or an online backup. Fast replies commit on
    /// this thread; a contended or disk-bound commit moves to a supervised task.
    /// `Err` means the engine stopped; a failure after the first mutation
    /// poisons this shard and stops the server.
    pub fn run<'a>(mut self, mut command: SingleKey<'a>, state: &State) -> io::Result<Ran<'a>> {
        Ok(match self.cell.step(&mut command, state)? {
            Step::Escalate => Ran::Escalate(command),
            Step::Done(reply) => Ran::Done(reply),
            Step::Staged(staged, reply) => {
                let persistence = self.cell.shared.persistence.clone();
                let (sender, receiver) = oneshot::channel();
                tokio::spawn(self.commit(
                    persistence,
                    staged,
                    reply,
                    command.write_permit.take(),
                    sender,
                ));
                Ran::Committing(receiver)
            }
        })
    }

    /// Owns everything the command still holds, independent of the client.
    async fn commit(
        mut self,
        persistence: Arc<Persistence>,
        staged: Staged,
        reply: Resp,
        write_permit: Option<Permit>,
        sender: oneshot::Sender<io::Result<Resp>>,
    ) {
        match persistence.commit_staged(staged).await {
            Ok(()) => {
                self.cell.busy = false;
                let _ = sender.send(Ok(reply));
            }
            Err(error) => {
                self.cell.shared.fatal(&error);
                let _ = sender.send(Err(error));
            }
        }
        // Write pauses observe this command committed and answered.
        drop(write_permit);
    }
}

impl Drop for ShardLease {
    fn drop(&mut self) {
        if self.cell.busy {
            self.cell.shared.fatal(&io::Error::other(
                "shard lease released before its command settled",
            ));
        }
    }
}

async fn supervise(mut status: watch::Receiver<Status>) -> io::Result<()> {
    match status
        .wait_for(|status| !matches!(status, Status::Running))
        .await
    {
        Ok(status) => halted(&status),
        // Every cell and the pool are gone: nothing can fail any more.
        Err(_) => Ok(()),
    }
}

/// Runs a memory pool's workers as tasks on the shared runtime from one
/// blocking thread, which no `JoinSet` abort reaches once it runs: like the
/// dedicated worker threads this replaces, dropping the coordinator's set
/// never cancels an accepted command or its commit. Workers end only as
/// `run_worker` documents, so the group drains every one of them before it
/// returns, and only then releases `shared`. The first error or panic stops
/// the pool at once: the supervisor reports it and idle siblings halt while a
/// busy one is still settling. The group returns that first cause.
async fn own_workers<W>(shared: Arc<Shared>, workers: Vec<W>) -> io::Result<()>
where
    W: Future<Output = io::Result<()>> + Send + 'static,
{
    let mut group = JoinSet::new();
    for worker in workers {
        group.spawn(worker);
    }
    let mut first = Ok(());
    while let Some(joined) = group.join_next().await {
        let error = match joined {
            Ok(Ok(())) => continue,
            Ok(Err(error)) => error,
            Err(join) => io::Error::other(join),
        };
        shared.fatal(&error);
        if first.is_ok() {
            first = Err(error);
        }
    }
    first
}

impl Paused {
    pub fn select_database(&mut self, database: u32) {
        for engine in &mut self.engines {
            engine.switch_database(database);
        }
    }

    /// Gather precisely the requested logical-database keys by moving entries.
    pub fn take_database_keys(
        &mut self,
        keys: &[crate::engine::ReadyKey],
        journal: bool,
    ) -> Engine {
        let mut engine = Engine::new();
        engine.inherit_runtime(&self.engines[0]);
        engine.set_recording(journal);
        let selected = self.engines[0].database();
        for (database, key) in keys {
            let shard = Engine::shard_for(key, self.engines.len());
            self.engines[shard].switch_database(*database);
            engine.switch_database(*database);
            self.engines[shard].transfer_keys(std::slice::from_ref(key), &mut engine);
        }
        self.select_database(selected);
        engine.switch_database(selected);
        engine
    }

    pub fn reap_search(&mut self, now: i64, journal: bool) -> io::Result<Option<Batch>> {
        let keys = self.engines[0].search_due_keys(now);
        let selected = self.engines[0].database();
        if keys.is_empty() {
            // Cursor expiry and empty temporary indexes still need reaping.
            self.engines[0].switch_database(0);
            self.engines[0].set_recording(journal);
            self.engines[0].search_reap(now);
            self.engines[0].finish_pending_search();
            let batch = self.engines[0].take_batch();
            self.engines[0].switch_database(selected);
            return Ok(batch);
        }
        let keys: Vec<_> = keys.into_iter().map(|key| (0, key)).collect();
        let mut engine = self.take_database_keys(&keys, journal);
        engine.switch_database(0);
        engine.search_reap(now);
        engine.finish_pending_search();
        let batch = engine.take_batch();
        self.restore(engine);
        self.select_database(selected);
        Ok(batch)
    }

    pub fn take_keys(&mut self, keys: &[Bytes], journal: bool) -> Engine {
        let mut engine = Engine::new();
        engine.inherit_runtime(&self.engines[0]);
        if journal {
            engine.set_recording(true);
        }
        for key in keys {
            let shard = Engine::shard_for(key, self.engines.len());
            self.engines[shard].transfer_keys(std::slice::from_ref(key), &mut engine);
        }
        engine
    }

    /// Move the complete dataset into one coherent view without cloning values.
    /// Empty replacement shards retain their runtime and recording configuration
    /// so subsequent commands in the same transaction still capture mutations.
    pub fn take_all(&mut self, journal: bool) -> Engine {
        let mut engine = std::mem::replace(&mut self.engines[0], Engine::new());
        self.engines[0].inherit_runtime(&engine);
        self.engines[0].set_recording(journal);
        for source in self.engines.iter_mut().skip(1) {
            let previous = std::mem::replace(source, Engine::new());
            source.inherit_runtime(&previous);
            source.set_recording(journal);
            previous.distribute(std::slice::from_mut(&mut engine));
        }
        engine.set_recording(journal);
        engine
    }

    /// Resolve dynamic compaction endpoints while the write barrier is held.
    /// Label scans need every shard; ordinary writes move only related series.
    pub fn take_related(&mut self, command: &BoundCommand<'_>, journal: bool, now: i64) -> Engine {
        let args = command.args();
        let database = self.engines[0].database();
        let destination = if args[0].eq_ignore_ascii_case(b"MOVE") {
            args.get(2)
                .and_then(|value| std::str::from_utf8(value).ok()?.parse::<u32>().ok())
                .map(|db| (db, args[1].clone()))
        } else if args[0].eq_ignore_ascii_case(b"COPY") {
            args[3..]
                .windows(2)
                .find(|pair| pair[0].eq_ignore_ascii_case(b"DB"))
                .and_then(|pair| std::str::from_utf8(&pair[1]).ok()?.parse::<u32>().ok())
                .map(|db| (db, args[2].clone()))
        } else {
            None
        };
        if let Some(destination) = destination
            && destination.0 < self.engines[0].database_count()
        {
            let mut keys: Vec<_> = command
                .accessed_keys()
                .into_iter()
                .map(|key| (database, key))
                .collect();
            keys.push(destination);
            return self.take_database_keys(&keys, journal);
        }
        let mut keys = command.accessed_keys();
        if keys.is_empty() {
            return self.take_all(journal);
        }
        let mut seen = HashSet::with_capacity(keys.len());
        keys.retain(|key| seen.insert(key.clone()));
        let mut index = 0;
        while index < keys.len() {
            let shard = Engine::shard_for(&keys[index], self.engines.len());
            for key in self.engines[shard].related_keys(&keys[index], now) {
                if seen.insert(key.clone()) {
                    keys.push(key.clone());
                }
            }
            index += 1;
        }
        self.take_keys(&keys, journal)
    }

    pub fn restore(&mut self, engine: Engine) {
        engine.distribute(&mut self.engines);
    }

    /// Publishes whether blocked clients may exist. Inline commands escalate
    /// while it is true, so a registration publishes before `resume`; a stale
    /// `true` after removals only costs fallbacks until the next publication.
    pub fn publish_waiters(&mut self, present: bool) {
        if let Some(cell) = self.cells.first() {
            cell.shared.waiters.store(present, Ordering::Release);
        }
    }

    pub fn resume(mut self) -> io::Result<()> {
        if self.engines.len() != self.cells.len() {
            // Dropping `self` poisons the cells and stops the server.
            return Err(io::Error::other("paused shard engines changed count"));
        }
        for (cell, engine) in self.cells.iter_mut().zip(self.engines.drain(..)) {
            cell.engine = Some(engine);
        }
        // Every engine is back before the first cell is released.
        self.cells.clear();
        Ok(())
    }

    /// Sample the union of visible live keys, not one equally weighted shard.
    pub fn random_key(&self, now: i64, visible_slots: Option<&[bool]>) -> Resp {
        let visible = |key: &Bytes| {
            visible_slots.is_none_or(|slots| {
                slots
                    .get(crate::cluster::key_slot(key) as usize)
                    .copied()
                    .unwrap_or(false)
            })
        };
        let count = self
            .engines
            .iter()
            .flat_map(|engine| engine.live_keys(now))
            .filter(|key| visible(key))
            .count();
        if count == 0 {
            return Resp::Bulk(None);
        }
        let index = rand::rng().random_range(0..count);
        let key = self
            .engines
            .iter()
            .flat_map(|engine| engine.live_keys(now))
            .filter(|key| visible(key))
            .nth(index)
            .cloned();
        let reply = Resp::Bulk(key);
        let mut budget = crate::engine::ReplyBudget::new(self.engines[0].reply_limit());
        match budget.response(&reply) {
            Ok(()) => reply,
            Err(error) => error,
        }
    }

    /// Runs `bound` on every shard and aggregates the replies.
    pub fn collect_global(
        &mut self,
        command: GlobalCommand,
        bound: &mut BoundCommand<'_>,
        now: i64,
    ) -> io::Result<(Resp, Option<Batch>)> {
        if matches!(command, GlobalCommand::RandomKey) {
            return Ok((self.random_key(now, None), None));
        }
        let mut budget = crate::engine::ReplyBudget::new(self.engines[0].reply_limit());
        if matches!(command, GlobalCommand::Scan)
            && let Err(error) = budget.charge(3, 20)
        {
            return Ok((error, None));
        }
        let mut values = Vec::new();
        let mut total = 0_i64;
        let mut cursor = None;
        let mut flush_batch: Option<Batch> = None;
        let mut cleared = std::collections::BTreeSet::new();
        let mut flush_output_database = None;
        for engine in &mut self.engines {
            let execution = engine.execute_at_time(bound, now)?;
            let Execution::Reply(reply) = execution else {
                return Err(invalid_reply());
            };
            if let Some(batch) = engine.take_batch() {
                if !matches!(command, GlobalCommand::Flush) {
                    return Err(invalid_reply());
                }
                let mut database = 0;
                for mutation in batch.mutations {
                    if let crate::journal::Mutation::SelectDatabase { database: selected } =
                        mutation
                    {
                        database = selected;
                        continue;
                    }
                    if matches!(mutation, crate::journal::Mutation::Clear)
                        && !cleared.insert(database)
                    {
                        continue;
                    }
                    let output = flush_batch.get_or_insert_with(|| Batch {
                        timestamp_ms: now,
                        mutations: Vec::new(),
                    });
                    if flush_output_database != Some(database) {
                        output
                            .mutations
                            .push(crate::journal::Mutation::SelectDatabase { database });
                        flush_output_database = Some(database);
                    }
                    output.mutations.push(mutation);
                }
            }
            if matches!(reply, Resp::Error(_)) {
                return Ok((reply, None));
            }
            match command {
                GlobalCommand::RandomKey => {
                    unreachable!("RANDOMKEY is aggregated before shard execution")
                }
                GlobalCommand::Flush => {
                    if reply != Resp::Simple("OK") {
                        return Err(invalid_reply());
                    }
                }
                GlobalCommand::Dbsize => {
                    let Resp::Integer(count) = reply else {
                        return Err(invalid_reply());
                    };
                    total = total.checked_add(count).ok_or_else(invalid_reply)?;
                }
                GlobalCommand::Keys => {
                    let Resp::Array(Some(keys)) = reply else {
                        return Err(invalid_reply());
                    };
                    for key in &keys {
                        if let Err(error) = budget.response(key) {
                            return Ok((error, None));
                        }
                    }
                    values.extend(keys);
                }
                GlobalCommand::Scan => {
                    let Resp::Array(Some(mut fields)) = reply else {
                        return Err(invalid_reply());
                    };
                    if fields.len() != 2 {
                        return Err(invalid_reply());
                    }
                    let Resp::Array(Some(keys)) = fields.pop().unwrap() else {
                        return Err(invalid_reply());
                    };
                    let Resp::Bulk(Some(next)) = fields.pop().unwrap() else {
                        return Err(invalid_reply());
                    };
                    let next = std::str::from_utf8(&next)
                        .ok()
                        .and_then(|next| next.parse::<u64>().ok())
                        .ok_or_else(invalid_reply)?;
                    if next != 0 {
                        cursor = Some(cursor.map_or(next, |current: u64| current.min(next)));
                    }
                    for key in &keys {
                        if let Err(error) = budget.response(key) {
                            return Ok((error, None));
                        }
                    }
                    values.extend(keys);
                }
            }
        }
        let reply = match command {
            GlobalCommand::RandomKey => {
                unreachable!("RANDOMKEY is aggregated before shard execution")
            }
            GlobalCommand::Flush => Resp::Simple("OK"),
            GlobalCommand::Dbsize => Resp::Integer(total),
            GlobalCommand::Keys => Resp::Array(Some(values)),
            GlobalCommand::Scan => {
                // The next cursor cannot skip any unfinished shard bucket.
                if let Some(next) = cursor {
                    values.retain(|value| matches!(value, Resp::Bulk(Some(key)) if Engine::key_hash(key) < next));
                }
                Resp::Array(Some(vec![
                    Resp::Bulk(Some(Bytes::from(cursor.unwrap_or(0).to_string()))),
                    Resp::Array(Some(values)),
                ]))
            }
        };
        Ok((reply, flush_batch))
    }
}

impl Drop for Paused {
    fn drop(&mut self) {
        // Not resumed: the engines leave with this value. Their cells stay
        // empty, so nobody leases a placeholder, and the server stops.
        if let Some(cell) = self.cells.first() {
            cell.shared.fatal(&io::Error::other(
                "paused shards were dropped without resume",
            ));
        }
    }
}

/// Inline requests reuse their admission facts; owned queue requests bind on
/// the worker, inside the same CPU window that previously validated them.
enum CommandInput<'a, 'args> {
    Bound(&'a mut BoundCommand<'args>),
    Unbound(&'args [Bytes]),
}

impl CommandInput<'_, '_> {
    fn args(&self) -> &[Bytes] {
        match self {
            Self::Bound(command) => command.args(),
            Self::Unbound(args) => args,
        }
    }
}

/// Runs one single-key command under the requester's identity with CPU
/// accounting while `cpu_observed` (see `State::cpu_observed`), leaving its
/// effects captured for the caller to commit and then `reset`. The engine owns
/// the requester's session until then. Shared by queue workers and inline
/// leases.
fn execute(
    engine: &mut Engine,
    persistence: &Persistence,
    command: CommandInput<'_, '_>,
    origin: Origin,
    user: Option<Arc<crate::auth::User>>,
    session: Option<Arc<crate::session::ClientSession>>,
    cpu_observed: bool,
) -> io::Result<Execution> {
    engine.set_recording(persistence.recording_for(command.args()));
    engine.set_reply_limit(origin.reply_limit());
    engine.set_command_user(user);
    engine.set_command_version(origin.version());
    // Installing a session selects its database; sessionless commands run in
    // database 0 whatever the previous command selected.
    if session.is_none() {
        engine.switch_database(0);
    }
    engine.set_command_session(session);
    let cpu = crate::session::ClientSession::cpu_window(
        engine.command_session().map(|session| &**session),
        cpu_observed,
    );
    let mut rebound;
    let command = match command {
        CommandInput::Bound(command) => command,
        CommandInput::Unbound(args) => {
            rebound = BoundCommand::bind(args);
            &mut rebound
        }
    };
    let execution = engine.execute(command)?;
    let session = engine.command_session();
    if let Some(session) = session {
        session.charge_cpu(cpu);
    }
    persistence.note_reads(command, session.map(|session| &**session));
    Ok(execution)
}

/// Clears the command identity once its effects are staged or committed.
fn reset(engine: &mut Engine, persistence: &Persistence) {
    engine.set_command_session(None);
    engine.set_command_user(None);
    engine.set_command_version(crate::protocol::Version::Resp2);
    engine.set_recording(persistence.capturing());
}

fn single_reply(execution: Execution) -> io::Result<Resp> {
    match execution {
        Execution::Reply(reply) | Execution::Pushed { reply, .. } => Ok(reply),
        Execution::Block(_) | Execution::StreamBlock(_) => Err(io::Error::other(
            "blocking operation escaped coordinator routing",
        )),
    }
}

/// Settles one admitted single-key command: execute, commit, reply, release.
/// Any `Err` stops the actor and leaves its cell poisoned; the reply is then
/// dropped unsent and the client sees the engine stop, as for every other
/// queued command. A stopped pool drops the command unexecuted the same way.
async fn settle(
    cell: &mut ShardCell,
    persistence: &Persistence,
    state: &State,
    mut request: CommandRequest,
    admission: Admission,
) -> io::Result<()> {
    persistence.ensure_healthy()?;
    if cell.shared.is_stopped() {
        return Ok(());
    }
    let engine = cell.begin()?;
    let execution = execute(
        engine,
        persistence,
        CommandInput::Unbound(&request.args),
        request.origin,
        request.user.take(),
        request.session.take(),
        state.cpu_observed(),
    )?;
    persistence.commit(engine).await?;
    reset(engine, persistence);
    let reply = single_reply(execution)?;
    cell.busy = false;
    let _ = request.reply.send(reply);
    // No await from here: write pauses (FAILOVER, migrations) and pool pauses
    // must observe this command committed and answered.
    drop(request.write_permit);
    drop(admission);
    Ok(())
}

/// Runs a batch of contended commands in queue order under one hold of the
/// cell, each as `ShardLease::run` would, committing before it replies.
/// Claiming one is the cancellation boundary: a command whose client is gone
/// by then is dropped unexecuted. Each write permit is released here, never
/// handed back. Any `Err` stops the actor as in `settle`, and a stopped pool
/// drops the rest of the batch unexecuted.
async fn serve_contended(
    cell: &mut ShardCell,
    persistence: &Persistence,
    state: &State,
    batch: &mut Vec<Contended>,
) -> io::Result<()> {
    for contended in batch.drain(..) {
        if cell.shared.is_stopped() {
            return Ok(());
        }
        let Contended {
            args,
            origin,
            user,
            session,
            write_permit,
            reply,
        } = contended;
        if reply.is_closed() {
            continue;
        }
        let mut command = SingleKey {
            command: BoundCommand::bind(&args),
            origin,
            user,
            session,
            write_permit,
            // `Pool::inline_shard` places no command of a cluster engine.
            cluster_client: None,
        };
        match cell.step(&mut command, state)? {
            Step::Escalate => {
                let SingleKey {
                    user,
                    session,
                    write_permit,
                    ..
                } = command;
                // Untouched, it needs no admission until its client admits
                // it again for the coordinator.
                drop(write_permit);
                let _ = reply.send(Served::Escalate { user, session });
                continue;
            }
            Step::Done(answer) => {
                let _ = reply.send(Served::Done(answer));
            }
            Step::Staged(staged, answer) => {
                if let Err(error) = persistence.commit_staged(staged).await {
                    cell.shared.fatal(&error);
                    return Err(error);
                }
                cell.busy = false;
                let _ = reply.send(Served::Done(answer));
            }
        }
        // Released only once answered: write pauses observe this command
        // committed and answered.
        drop(command.write_permit);
    }
    Ok(())
}

async fn maintain(
    cell: &mut ShardCell,
    persistence: &Persistence,
    databases: &mut Vec<u32>,
) -> io::Result<()> {
    if cell.shared.is_stopped() {
        return Ok(());
    }
    let engine = cell.begin()?;
    engine.set_recording(persistence.capturing());
    let selected = engine.database();
    let now = Engine::now();
    databases.clear();
    databases.extend(engine.database_ids());
    for database in databases.iter().copied() {
        engine.switch_database(database);
        engine.expire_some();
        engine.maintain_native(now, 128)?;
    }
    engine.switch_database(selected);
    persistence.commit(engine).await?;
    engine.set_recording(persistence.capturing());
    cell.busy = false;
    Ok(())
}

/// What a worker takes up next.
enum Work {
    Request(Option<ShardRequest>),
    /// Contended commands just added to the batch; none once the pool and
    /// with it the only sender are gone.
    Contended(usize),
    Maintain,
}

/// Holds no pool or shard sender, and reaches the pool through `state` only
/// weakly: dropping the pool closes `requests`, and the worker settles
/// everything already admitted before returning. A stopped pool ends it at
/// once, even while connections still hold the pool: `close` has drained the
/// admitted queue, and after a fatal stop queued commands never run. Unclaimed
/// contended commands are dropped unexecuted either way; those it never
/// received stay counted, which only a stopped pool's leases could observe.
async fn run_worker(
    cell: Arc<Mutex<ShardCell>>,
    queues: Queues,
    persistence: Arc<Persistence>,
    state: Arc<State>,
    mut status: watch::Receiver<Status>,
) -> io::Result<()> {
    let Queues {
        mut requests,
        mut contended,
        pending: every,
        shard,
    } = queues;
    let pending = &every[shard].0;
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut databases = Vec::new();
    // Reused by every batch, so it grows to `CONTENDED_BATCH` at most once.
    let mut batch = Vec::new();
    let mut contending = true;
    loop {
        // A stop always wins; admitted commands, contended commands and
        // maintenance stay unbiased, so neither a continuously busy queue nor
        // a hot key can starve the others, expiry or native upkeep.
        let work = tokio::select! {
            biased;
            halt = status.wait_for(|status| !matches!(status, Status::Running)) => {
                return halt.map_or(Ok(()), |status| halted(&status));
            }
            work = async {
                tokio::select! {
                    request = requests.recv() => Work::Request(request),
                    // Takes only commands already queued, before the cell.
                    received = contended.recv_many(&mut batch, CONTENDED_BATCH),
                        if contending => Work::Contended(received),
                    _ = tick.tick() => Work::Maintain,
                }
            } => work,
        };
        match work {
            Work::Request(Some(ShardRequest { request, admission })) => {
                settle(
                    &mut *cell.lock().await,
                    &persistence,
                    &state,
                    request,
                    admission,
                )
                .await?;
            }
            Work::Request(None) => return Ok(()),
            Work::Contended(0) => contending = false,
            Work::Contended(received) => {
                // Inline leases give way until the whole batch is answered or
                // dropped, whatever its clients do with their answers.
                let taken = Counted::received(pending, received);
                let mut held = cell.lock().await;
                serve_contended(&mut held, &persistence, &state, &mut batch).await?;
                drop(held);
                drop(taken);
                // With the cell free, charge the batch to this task's
                // cooperative budget as one receive per command would; its
                // one receive already spent a unit.
                for _ in 1..received {
                    tokio::task::coop::consume_budget().await;
                }
            }
            Work::Maintain => {
                maintain(&mut *cell.lock().await, &persistence, &mut databases).await?;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn random_key_includes_later_shards_and_excludes_invisible_slots() {
        let key = (0..1000)
            .map(|index| Bytes::from(format!("visible:{index}")))
            .find(|key| Engine::shard_for(key, 2) == 1)
            .unwrap();
        let hidden = (0..1000)
            .map(|index| Bytes::from(format!("hidden:{index}")))
            .find(|candidate| crate::cluster::key_slot(candidate) != crate::cluster::key_slot(&key))
            .unwrap();
        let mut engine = Engine::new();
        for key in [&key, &hidden] {
            engine
                .execute(&mut crate::engine::BoundCommand::bind(&[
                    Bytes::from_static(b"SET"),
                    key.clone(),
                    Bytes::from_static(b"value"),
                ]))
                .unwrap();
        }
        let persistence = Arc::new(Persistence::new(None));
        let state = Arc::new(State::new(0, 2, false));
        let (pool, mut workers) = Pool::start(engine, 2, persistence, state);
        let paused = pool.pause().await.unwrap();
        let mut visible = vec![false; 16_384];
        visible[crate::cluster::key_slot(&key) as usize] = true;
        assert_eq!(
            paused.random_key(Engine::now(), Some(&visible)),
            Resp::Bulk(Some(key))
        );
        visible.fill(false);
        assert_eq!(
            paused.random_key(Engine::now(), Some(&visible)),
            Resp::Bulk(None)
        );
        paused.resume().unwrap();
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn global_keys_cannot_multiply_the_origin_budget_by_shard_count() {
        let limit = std::mem::size_of::<Resp>() + 1;
        let mut engines = Vec::new();
        for key in [b"a", b"b"] {
            let mut engine = Engine::new();
            engine
                .execute(&mut crate::engine::BoundCommand::bind(&[
                    Bytes::from_static(b"SET"),
                    Bytes::copy_from_slice(key),
                    Bytes::from_static(b"value"),
                ]))
                .unwrap();
            engine.set_reply_limit(limit);
            engines.push(engine);
        }
        let gate = Arc::new(RwLock::new(())).write_owned().await;
        let mut paused = Paused {
            engines,
            cells: Vec::new(),
            _gate: gate,
        };
        let args = [Bytes::from_static(b"KEYS"), Bytes::from_static(b"*")];
        let (reply, _) = paused
            .collect_global(
                GlobalCommand::Keys,
                &mut crate::engine::BoundCommand::bind(&args),
                Engine::now(),
            )
            .unwrap();
        assert!(matches!(reply, Resp::Error(_)));
        for engine in &mut paused.engines {
            engine.set_reply_limit(limit * 2);
        }
        let (reply, _) = paused
            .collect_global(
                GlobalCommand::Keys,
                &mut crate::engine::BoundCommand::bind(&args),
                Engine::now(),
            )
            .unwrap();
        assert!(matches!(reply, Resp::Array(Some(keys)) if keys.len() == 2));
    }

    fn command(args: &[&'static [u8]]) -> (CommandRequest, oneshot::Receiver<Resp>) {
        let (reply, response) = oneshot::channel();
        let args = args.iter().copied().map(Bytes::from_static).collect();
        (CommandRequest::internal(args, reply), response)
    }

    async fn submit(
        pool: &Pool,
        overflow: &mut JoinSet<io::Result<()>>,
        shard: usize,
        request: CommandRequest,
    ) -> io::Result<()> {
        let admission = pool.admit().await?;
        pool.submit(shard, request, admission, overflow)
    }

    fn get(engine: &mut Engine, key: &'static [u8]) -> Resp {
        match engine
            .execute(&mut crate::engine::BoundCommand::bind(&[
                Bytes::from_static(b"GET"),
                Bytes::from_static(key),
            ]))
            .unwrap()
        {
            Execution::Reply(reply) => reply,
            _ => panic!("GET always replies"),
        }
    }

    async fn join_all(tasks: &mut JoinSet<io::Result<()>>) {
        while let Some(result) = tasks.join_next().await {
            result.unwrap().unwrap();
        }
    }

    /// A memory engine of `count` shards with its workers running.
    fn started(count: u16) -> (Arc<Pool>, JoinSet<io::Result<()>>) {
        let persistence = Arc::new(Persistence::new(None));
        let state = Arc::new(State::new(0, count, false));
        Pool::start(Engine::new(), usize::from(count), persistence, state)
    }

    /// One shard whose queue the test drains by hand: a live actor would empty
    /// even a tiny queue before the test could observe it full.
    fn queued_pool(capacity: usize) -> (Arc<Pool>, Queues) {
        let (pool, mut queues) =
            Pool::assemble(Engine::new(), 1, capacity, Arc::new(Persistence::new(None)));
        (Arc::new(pool), queues.pop().unwrap())
    }

    /// Shard 0's worker for `queues`, for the test to drive.
    fn worker(
        pool: &Pool,
        queues: Queues,
        state: Arc<State>,
    ) -> impl Future<Output = io::Result<()>> + Send + use<> {
        let cell = pool.cells[0].clone();
        let persistence = pool.shared.persistence.clone();
        let status = pool.shared.status.subscribe();
        run_worker(cell, queues, persistence, state, status)
    }

    fn spawn_worker(pool: &Pool, queues: Queues) -> tokio::task::JoinHandle<io::Result<()>> {
        let runtime = tokio::runtime::Handle::current();
        let worker = worker(pool, queues, Arc::new(State::new(0, 1, false)));
        tokio::task::spawn_blocking(move || runtime.block_on(worker))
    }

    #[tokio::test]
    async fn pause_observes_an_admitted_write_whose_client_disconnected() {
        let (pool, mut workers) = started(1);
        let mut overflow = JoinSet::new();
        let (request, response) = command(&[b"SET", b"abandoned", b"value"]);
        drop(response);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"abandoned"),
            Resp::Bulk(Some(Bytes::from_static(b"value")))
        );
        paused.resume().unwrap();
        join_all(&mut overflow).await;
        drop(pool);
        join_all(&mut workers).await;
    }

    #[tokio::test]
    async fn write_pause_completes_only_after_the_admitted_write_replied() {
        let gate = crate::write_gate::WriteGate::default();
        let (pool, mut workers) = started(1);
        let mut overflow = JoinSet::new();
        let (mut request, mut response) = command(&[b"SET", b"gated", b"value"]);
        request.write_permit = Some(gate.admit().await);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        gate.pause().await.unwrap();
        assert_eq!(response.try_recv().unwrap(), Resp::Simple("OK"));
        gate.resume();
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"gated"),
            Resp::Bulk(Some(Bytes::from_static(b"value")))
        );
        paused.resume().unwrap();
        join_all(&mut overflow).await;
        drop(pool);
        join_all(&mut workers).await;
    }

    #[tokio::test]
    async fn dropping_the_pool_settles_every_queued_command_before_workers_exit() {
        let (pool, mut workers) = started(2);
        let mut overflow = JoinSet::new();
        let mut responses = Vec::new();
        for index in 0..64 {
            let key = Bytes::from(format!("drain:{index}"));
            let shard = Engine::shard_for(&key, 2);
            let (reply, response) = oneshot::channel();
            let request = CommandRequest::internal(vec![Bytes::from_static(b"INCR"), key], reply);
            submit(&pool, &mut overflow, shard, request).await.unwrap();
            responses.push(response);
        }
        join_all(&mut overflow).await;
        drop(pool);
        join_all(&mut workers).await;
        for mut response in responses {
            assert_eq!(response.try_recv().unwrap(), Resp::Integer(1));
        }
    }

    #[tokio::test]
    async fn actor_failure_unblocks_pause_and_rejects_later_commands() {
        let (pool, mut workers) = started(1);
        let mut overflow = JoinSet::new();
        // A blocking result escaping its coordinator route is fatal to the actor.
        let (request, response) = command(&[b"BLPOP", b"missing", b"0"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        assert!(response.await.is_err());
        // The failed command's cell stays poisoned for pauses and inline leases.
        assert!(pool.pause().await.is_err());
        assert!(pool.lease(0).await.is_err());
        assert!(workers.join_next().await.unwrap().unwrap().is_err());
        let (request, response) = command(&[b"SET", b"late", b"value"]);
        assert!(submit(&pool, &mut overflow, 0, request).await.is_err());
        assert!(response.await.is_err());
    }

    #[tokio::test]
    async fn full_queue_hand_off_holds_admission_until_the_actor_settles_it() {
        let (pool, queues) = queued_pool(1);
        let mut overflow = JoinSet::new();
        let (request, first) = command(&[b"SET", b"first", b"1"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        let (request, second) = command(&[b"SET", b"second", b"2"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pool.pause())
                .await
                .is_err()
        );
        let worker = spawn_worker(&pool, queues);
        assert_eq!(first.await.unwrap(), Resp::Simple("OK"));
        assert_eq!(second.await.unwrap(), Resp::Simple("OK"));
        join_all(&mut overflow).await;
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"second"),
            Resp::Bulk(Some(Bytes::from_static(b"2")))
        );
        paused.resume().unwrap();
        drop(pool);
        worker.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn full_queue_hand_off_fails_the_supervisor_when_the_actor_stops() {
        let (pool, queues) = queued_pool(1);
        let mut overflow = JoinSet::new();
        let (request, first) = command(&[b"SET", b"first", b"1"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        let (request, second) = command(&[b"SET", b"second", b"2"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        drop(queues);
        assert!(overflow.join_next().await.unwrap().unwrap().is_err());
        assert!(first.await.is_err());
        assert!(second.await.is_err());
        assert!(pool.pause().await.is_err());
    }

    struct Directory(std::path::PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::temp_dir()
                .join(format!("ffkv-inline-lease-{:032x}", rand::random::<u128>()));
            std::fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn journaled(directory: &Directory) -> Arc<Persistence> {
        let path = directory.0.join("wal");
        let (wal, _) = tokio::task::spawn_blocking(move || {
            crate::wal::Wal::open(&path, crate::wal::Durability::Strict, |_| Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
        Arc::new(Persistence::new(Some(wal)))
    }

    fn native(directory: &Directory) -> Arc<crate::native_replication::NativeReplication> {
        let state = Arc::new(State::new(0, 1, false));
        let config = crate::native_replication::Config {
            backlog_bytes: 4096,
            max_snapshot_bytes: 1024 * 1024,
            staging_directory: directory.0.join("native"),
            listening_port: 0,
            announce_ip: None,
            io_timeout: Duration::from_secs(1),
            reconnect_delay: Duration::from_millis(10),
            lazy_history: false,
            aof_enabled: false,
            allow_loopback: true,
            allow_private: true,
            upstream_template: None,
        };
        let backend = crate::native_runtime::Runtime::new(
            &state,
            crate::native_runtime::Config {
                native: config.clone(),
                cluster: None,
                fsync_gate: None,
            },
        );
        crate::native_replication::NativeReplication::new(config, backend).unwrap()
    }

    /// Cells and live queues without workers, so no maintenance tick races
    /// the test; the receivers keep the queues open.
    fn idle_pool(
        engine: Engine,
        count: usize,
        persistence: Arc<Persistence>,
    ) -> (Arc<Pool>, Vec<Queues>) {
        let (pool, queues) = Pool::assemble(engine, count, 8, persistence);
        (Arc::new(pool), queues)
    }

    /// One shard whose keys have all expired: reading one journals its
    /// deletion, and with a WAL that commit cannot settle synchronously.
    async fn expired(keys: &[&'static [u8]], directory: &Directory) -> (Arc<Pool>, Vec<Queues>) {
        let mut engine = Engine::new();
        for key in keys {
            engine
                .execute(&mut crate::engine::BoundCommand::bind(&[
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(key),
                    Bytes::from_static(b"value"),
                    Bytes::from_static(b"PX"),
                    Bytes::from_static(b"1"),
                ]))
                .unwrap();
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        idle_pool(engine, 1, journaled(directory).await)
    }

    fn owned(args: &[&'static [u8]]) -> Vec<Bytes> {
        args.iter().copied().map(Bytes::from_static).collect()
    }

    fn single(
        args: &[Bytes],
        session: Option<Arc<crate::session::ClientSession>>,
    ) -> SingleKey<'_> {
        SingleKey {
            command: BoundCommand::bind(args),
            origin: Origin::Client(crate::protocol::Version::Resp2),
            user: None,
            session,
            write_permit: None,
            cluster_client: None,
        }
    }

    fn inline<'a>(pool: &Arc<Pool>, state: &State, command: SingleKey<'a>) -> Ran<'a> {
        let shard = pool.inline_shard(&command.command, state).unwrap();
        pool.try_lease(shard).unwrap().run(command, state).unwrap()
    }

    fn done(ran: Ran<'_>) -> Resp {
        match ran {
            Ran::Done(reply) => reply,
            Ran::Committing(_) => panic!("a memory commit was deferred"),
            Ran::Escalate(_) => panic!("an eligible command escalated"),
        }
    }

    fn escalated(ran: Ran<'_>) -> SingleKey<'_> {
        match ran {
            Ran::Escalate(command) => command,
            _ => panic!("ran a command that must fall back to the coordinator"),
        }
    }

    /// Runs `command` inline once shard 0's worker releases its cell.
    async fn leased(pool: &Arc<Pool>, state: &State, command: SingleKey<'_>) -> Resp {
        done(pool.lease(0).await.unwrap().run(command, state).unwrap())
    }

    /// One client command queued to shard 0, awaited to its reply.
    async fn queued(
        pool: &Pool,
        overflow: &mut JoinSet<io::Result<()>>,
        args: &[Bytes],
        session: Option<Arc<crate::session::ClientSession>>,
    ) -> Resp {
        let (reply, response) = oneshot::channel();
        let request = CommandRequest::client(
            args.to_vec(),
            reply,
            None,
            crate::protocol::Version::Resp2,
            session,
        );
        submit(pool, overflow, 0, request).await.unwrap();
        response.await.unwrap()
    }

    #[tokio::test]
    async fn inline_write_publishes_its_session_offset_before_replying() {
        let directory = Directory::new();
        let native = native(&directory);
        let persistence = Arc::new(Persistence::new(None));
        persistence.set_native(native);
        let (pool, _queues) = idle_pool(Engine::new(), 2, persistence.clone());
        let state = State::new(0, 2, false);
        let session = Arc::new(crate::session::ClientSession::new(5));
        let set = owned(&[b"SET", b"offset", b"value"]);
        assert_eq!(
            done(inline(&pool, &state, single(&set, Some(session.clone())))),
            Resp::Simple("OK")
        );
        let offset = session.last_write_offset.load(Ordering::Acquire);
        assert!(offset > 0, "the native stream records memory-only writes");
        assert_eq!(offset, persistence.native_offset());
        let get = owned(&[b"GET", b"offset"]);
        assert_eq!(
            done(inline(&pool, &state, single(&get, Some(session.clone())))),
            Resp::Bulk(Some(Bytes::from_static(b"value")))
        );
        assert_eq!(session.last_write_offset.load(Ordering::Acquire), offset);
    }

    #[tokio::test]
    async fn inline_commands_escalate_untouched_before_any_mutation() {
        let (pool, _queues) = idle_pool(Engine::new(), 1, Arc::new(Persistence::new(None)));
        let state = State::new(0, 1, false);
        let gate = crate::write_gate::WriteGate::default();
        let mut paused = pool.pause().await.unwrap();
        paused.publish_waiters(true);
        paused.resume().unwrap();
        let set = owned(&[b"SET", b"key", b"one"]);
        let get = owned(&[b"GET", b"key"]);
        // A session in another database returns with the escalated command.
        let selected = Arc::new(crate::session::ClientSession::new(3));
        selected.set_database(2);
        let mut write = single(&set, Some(selected.clone()));
        write.write_permit = Some(gate.admit().await);
        let write = escalated(inline(&pool, &state, write));
        assert!(
            write.write_permit.is_some(),
            "the permit returns with the command"
        );
        let mut paused = pool.pause().await.unwrap();
        paused.publish_waiters(false);
        paused.resume().unwrap();
        state.set_replica(true);
        escalated(inline(&pool, &state, single(&get, None)));
        state.set_replica(false);
        assert_eq!(
            done(inline(&pool, &state, single(&get, None))),
            Resp::Bulk(None)
        );
        // The fallback request is intact: it runs exactly as submitted, in its
        // session's database.
        assert_eq!(done(inline(&pool, &state, write)), Resp::Simple("OK"));
        assert_eq!(
            done(inline(&pool, &state, single(&get, None))),
            Resp::Bulk(None)
        );
        assert_eq!(
            done(inline(&pool, &state, single(&get, Some(selected)))),
            Resp::Bulk(Some(Bytes::from_static(b"one")))
        );
        gate.pause().await.unwrap();
        gate.resume();

        // Journaled writes need the WAL: they stay on the coordinator route,
        // and a holder of the cell still refuses one that reaches it.
        let directory = Directory::new();
        let (pool, _queues) = idle_pool(Engine::new(), 1, journaled(&directory).await);
        let lease = pool.try_lease(0).unwrap();
        escalated(lease.run(single(&set, None), &state).unwrap());
        assert_eq!(
            done(inline(&pool, &state, single(&get, None))),
            Resp::Bulk(None)
        );
    }

    #[tokio::test]
    async fn every_route_runs_each_command_in_its_requesters_database() {
        let (pool, mut workers) = started(1);
        let state = State::new(0, 1, false);
        let mut overflow = JoinSet::new();
        let selected = Arc::new(crate::session::ClientSession::new(1));
        selected.set_database(3);
        let other = Arc::new(crate::session::ClientSession::new(2));
        let set = owned(&[b"SET", b"key", b"three"]);
        let get = owned(&[b"GET", b"key"]);
        let three = Resp::Bulk(Some(Bytes::from_static(b"three")));
        assert_eq!(
            queued(&pool, &mut overflow, &set, Some(selected.clone())).await,
            Resp::Simple("OK")
        );
        // Whatever the previous command selected, on either route, a session
        // runs in its own database and a sessionless command in database 0.
        assert_eq!(
            queued(&pool, &mut overflow, &get, None).await,
            Resp::Bulk(None)
        );
        assert_eq!(
            leased(&pool, &state, single(&get, Some(selected.clone()))).await,
            three
        );
        assert_eq!(
            leased(&pool, &state, single(&get, None)).await,
            Resp::Bulk(None)
        );
        assert_eq!(
            leased(&pool, &state, single(&get, Some(selected.clone()))).await,
            three
        );
        assert_eq!(
            queued(&pool, &mut overflow, &get, Some(other.clone())).await,
            Resp::Bulk(None)
        );
        assert_eq!(
            queued(&pool, &mut overflow, &get, Some(selected.clone())).await,
            three
        );
        assert_eq!(
            leased(&pool, &state, single(&get, Some(other.clone()))).await,
            Resp::Bulk(None)
        );
        // A session that selects another database runs there from its next
        // command on.
        other.set_database(3);
        selected.set_database(0);
        assert_eq!(
            queued(&pool, &mut overflow, &get, Some(other.clone())).await,
            three
        );
        assert_eq!(
            leased(&pool, &state, single(&get, Some(selected.clone()))).await,
            Resp::Bulk(None)
        );
        assert_eq!(
            leased(&pool, &state, single(&get, Some(other))).await,
            three
        );
        assert_eq!(
            queued(&pool, &mut overflow, &get, Some(selected)).await,
            Resp::Bulk(None)
        );
        join_all(&mut overflow).await;
        drop(pool);
        join_all(&mut workers).await;
    }

    #[tokio::test]
    async fn inline_cpu_is_charged_to_the_requesting_session_alone() {
        use crate::{
            server_commands::{
                CommandContext, ConfigSnapshot, HookFuture, ServerAction, ServerHooks,
                ServerRuntime, thread_cpu_time,
            },
            session::ClientSession,
        };
        struct NoHooks;
        impl ServerHooks for NoHooks {
            fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a> {
                panic!("HOTKEYS runs no server action: {action:?}");
            }
        }
        let (pool, _queues) = idle_pool(Engine::new(), 1, Arc::new(Persistence::new(None)));
        let state = State::new(0, 1, false);
        // Session CPU is measured while a HOTKEYS session may report it.
        let server = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        let start = owned(&[b"HOTKEYS", b"START", b"METRICS", b"1", b"CPU"]);
        assert_eq!(
            server
                .execute(&BoundCommand::bind(&start), CommandContext { database: 0 })
                .await,
            Some(Resp::Simple("OK"))
        );
        assert!(state.server.set(Arc::new(server)).is_ok());
        assert!(state.cpu_observed());
        let first = Arc::new(ClientSession::new(1));
        let second = Arc::new(ClientSession::new(2));
        second.set_database(1);
        let get = owned(&[b"GET", b"key"]);
        let spent = |session: &ClientSession| session.cpu_ns.load(Ordering::Relaxed);
        // Inline commands run on this thread, so every window charged to
        // `payer` lies within this thread's CPU time across the loop.
        let charge = |payer: &Arc<ClientSession>, idle: &ClientSession| {
            let (paid, untouched) = (spent(payer), spent(idle));
            let started = thread_cpu_time();
            let charged = (0..100_000).any(|_| {
                done(inline(&pool, &state, single(&get, Some(payer.clone()))));
                spent(payer) > paid
            });
            let elapsed = thread_cpu_time().saturating_sub(started);
            assert!(charged, "the requesting session pays for its commands");
            assert!(u128::from(spent(payer) - paid) <= elapsed.as_nanos());
            assert_eq!(spent(idle), untouched, "another session is never charged");
        };
        charge(&first, &second);
        charge(&second, &first);
        let paid = (spent(&first), spent(&second));
        for _ in 0..64 {
            done(inline(&pool, &state, single(&get, None)));
        }
        assert_eq!(
            (spent(&first), spent(&second)),
            paid,
            "a sessionless command charges no session"
        );
        charge(&first, &second);
    }

    #[tokio::test]
    async fn deferred_commit_owns_lease_permit_and_reply_beyond_the_client() {
        let directory = Directory::new();
        let (pool, _queues) = expired(&[b"gone", b"seen"], &directory).await;
        let persistence = pool.shared.persistence.clone();
        let state = State::new(0, 1, false);
        let gate = crate::write_gate::WriteGate::default();
        let revision = persistence.status().await.revision;

        // The client disconnects at once; the commit still settles.
        let gone = owned(&[b"GET", b"gone"]);
        let mut abandoned = single(&gone, None);
        abandoned.write_permit = Some(gate.admit().await);
        let Ran::Committing(response) = inline(&pool, &state, abandoned) else {
            panic!("a journaled deletion committed on the caller");
        };
        drop(response);
        let lease = pool.lease(0).await.unwrap();
        assert_eq!(persistence.status().await.revision, revision + 1);

        let session = Arc::new(crate::session::ClientSession::new(9));
        session.last_write_offset.store(u64::MAX, Ordering::Release);
        let seen = owned(&[b"GET", b"seen"]);
        let mut read = single(&seen, Some(session.clone()));
        read.write_permit = Some(gate.admit().await);
        let Ran::Committing(mut response) = lease.run(read, &state).unwrap() else {
            panic!("a journaled deletion committed on the caller");
        };
        // The continuation holds the write permit until it has replied.
        gate.pause().await.unwrap();
        assert_eq!(response.try_recv().unwrap().unwrap(), Resp::Bulk(None));
        assert_eq!(
            session.last_write_offset.load(Ordering::Acquire),
            persistence.native_offset()
        );
        assert_eq!(persistence.status().await.revision, revision + 2);
        gate.resume();
        pool.pause().await.unwrap().resume().unwrap();
    }

    /// `command` run through shard 0's worker, as a lease would run it.
    async fn contended<'a>(pool: &Pool, command: SingleKey<'a>) -> Ran<'a> {
        match pool.contend(0, command).await.unwrap() {
            Contention::Done(reply) => Ran::Done(reply),
            Contention::Escalate(command) => Ran::Escalate(command),
        }
    }

    #[tokio::test]
    async fn contended_commands_run_on_their_worker_or_return_untouched() {
        let state = Arc::new(State::new(0, 1, false));
        let persistence = Arc::new(Persistence::new(None));
        let (pool, mut workers) = Pool::start(Engine::new(), 1, persistence, state.clone());
        let gate = crate::write_gate::WriteGate::default();
        let set = owned(&[b"SET", b"key", b"one"]);
        let get = owned(&[b"GET", b"key"]);
        // Possible blocked clients: the worker returns the write unexecuted,
        // with its session; it released the write permit itself.
        let mut paused = pool.pause().await.unwrap();
        paused.publish_waiters(true);
        paused.resume().unwrap();
        let selected = Arc::new(crate::session::ClientSession::new(3));
        selected.set_database(2);
        let mut write = single(&set, Some(selected.clone()));
        write.write_permit = Some(gate.admit().await);
        let mut write = escalated(contended(&pool, write).await);
        assert!(write.write_permit.is_none(), "no answer carries a permit");
        assert!(Arc::ptr_eq(write.session.as_ref().unwrap(), &selected));
        let mut paused = pool.pause().await.unwrap();
        paused.publish_waiters(false);
        paused.resume().unwrap();
        // The worker reads the role under the cell, as a lease does.
        state.set_replica(true);
        escalated(contended(&pool, single(&get, None)).await);
        state.set_replica(false);
        assert_eq!(
            done(contended(&pool, single(&get, None)).await),
            Resp::Bulk(None)
        );
        // Admitted again, the returned command is intact: it runs exactly as
        // submitted, in its session's database, and releases its permit once
        // answered.
        write.write_permit = Some(gate.admit().await);
        assert_eq!(done(contended(&pool, write).await), Resp::Simple("OK"));
        gate.pause().await.unwrap();
        gate.resume();
        assert_eq!(
            done(contended(&pool, single(&get, None)).await),
            Resp::Bulk(None)
        );
        assert_eq!(
            done(contended(&pool, single(&get, Some(selected))).await),
            Resp::Bulk(Some(Bytes::from_static(b"one")))
        );
        drop(pool);
        join_all(&mut workers).await;
    }

    /// One poll of `future` in this task, which drives everything it awaits.
    async fn poll_once<F: Future>(mut future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
        std::future::poll_fn(|cx| std::task::Poll::Ready(future.as_mut().poll(cx))).await
    }

    /// A worker takes the contended commands queued before it asks for the
    /// cell and claims each under it: one whose client left by then never
    /// runs, and one queued once a pause waits for the cell runs after that
    /// pause. Time stands still, so no maintenance tick follows the first.
    #[tokio::test(start_paused = true)]
    async fn contended_commands_are_claimed_under_the_cell_after_earlier_pauses() {
        use std::task::Poll;
        let (pool, mut queues) = idle_pool(Engine::new(), 1, Arc::new(Persistence::new(None)));
        let state = Arc::new(State::new(0, 1, false));
        let worker = worker(&pool, queues.pop().unwrap(), state);
        tokio::pin!(worker);
        // Its first maintenance pass runs at once; then it idles.
        assert!(poll_once(worker.as_mut()).await.is_pending());
        let held = pool.try_lease(0).unwrap();
        let set_first = owned(&[b"SET", b"key", b"first"]);
        let set_abandoned = owned(&[b"SET", b"abandoned", b"value"]);
        let set_later = owned(&[b"SET", b"key", b"later"]);
        let first = pool.contend(0, single(&set_first, None));
        tokio::pin!(first);
        assert!(poll_once(first.as_mut()).await.is_pending());
        let mut abandoned = Box::pin(pool.contend(0, single(&set_abandoned, None)));
        assert!(poll_once(abandoned.as_mut()).await.is_pending());
        // The worker takes both, then waits for the cell.
        assert!(poll_once(worker.as_mut()).await.is_pending());
        // That client leaves before its command is claimed.
        drop(abandoned);
        // A pause asks for the cell after the worker, then a command arrives.
        let pause = pool.pause();
        tokio::pin!(pause);
        assert!(poll_once(pause.as_mut()).await.is_pending());
        let later = pool.contend(0, single(&set_later, None));
        tokio::pin!(later);
        assert!(poll_once(later.as_mut()).await.is_pending());
        // Released, the cell passes to the worker's batch, then to the pause;
        // the worker waits behind it for the later command.
        drop(held);
        assert!(poll_once(worker.as_mut()).await.is_pending());
        let Poll::Ready(Ok(Contention::Done(reply))) = poll_once(first.as_mut()).await else {
            panic!("the claimed command answers");
        };
        assert_eq!(reply, Resp::Simple("OK"));
        let Poll::Ready(Ok(mut paused)) = poll_once(pause.as_mut()).await else {
            panic!("the pause follows the batch queued before it");
        };
        assert_eq!(
            get(&mut paused.engines[0], b"key"),
            Resp::Bulk(Some(Bytes::from_static(b"first")))
        );
        assert_eq!(get(&mut paused.engines[0], b"abandoned"), Resp::Bulk(None));
        paused.resume().unwrap();
        assert!(poll_once(worker.as_mut()).await.is_pending());
        let Poll::Ready(Ok(Contention::Done(reply))) = poll_once(later.as_mut()).await else {
            panic!("the later command runs after the pause");
        };
        assert_eq!(reply, Resp::Simple("OK"));
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"key"),
            Resp::Bulk(Some(Bytes::from_static(b"later")))
        );
        assert_eq!(get(&mut paused.engines[0], b"abandoned"), Resp::Bulk(None));
        paused.resume().unwrap();
    }

    /// A command its worker returned unexecuted holds no write permit, and an
    /// answered command no longer holds inline leases back, while its client
    /// has yet to take the answer.
    #[tokio::test(start_paused = true)]
    async fn an_untaken_answer_holds_no_write_permit_nor_the_shard() {
        use std::task::Poll;
        let (pool, mut queues) = idle_pool(Engine::new(), 1, Arc::new(Persistence::new(None)));
        let state = Arc::new(State::new(0, 1, false));
        let worker = worker(&pool, queues.pop().unwrap(), state);
        tokio::pin!(worker);
        // Its first maintenance pass runs at once; then it idles.
        assert!(poll_once(worker.as_mut()).await.is_pending());
        // Possible blocked clients send writes to the coordinator.
        let mut paused = pool.pause().await.unwrap();
        paused.publish_waiters(true);
        paused.resume().unwrap();
        let gate = crate::write_gate::WriteGate::default();
        let set = owned(&[b"SET", b"key", b"value"]);
        let session = Arc::new(crate::session::ClientSession::new(7));
        let mut write = single(&set, Some(session.clone()));
        write.write_permit = Some(gate.admit().await);
        let queued = pool.contend(0, write);
        tokio::pin!(queued);
        assert!(poll_once(queued.as_mut()).await.is_pending());
        // Queued, it holds inline leases back.
        assert!(pool.try_lease(0).is_none());
        // Its worker answers; the client does not take the answer yet.
        assert!(poll_once(worker.as_mut()).await.is_pending());
        let lease = pool.try_lease(0);
        assert!(lease.is_some(), "an answer frees the shard");
        drop(lease);
        tokio::time::timeout(Duration::from_secs(10), gate.pause())
            .await
            .expect("an untaken answer holds no write permit")
            .unwrap();
        gate.resume();
        let Poll::Ready(Ok(Contention::Escalate(back))) = poll_once(queued.as_mut()).await else {
            panic!("a write escalates while blocked clients may exist");
        };
        assert!(back.write_permit.is_none());
        assert!(Arc::ptr_eq(back.session.as_ref().unwrap(), &session));
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(get(&mut paused.engines[0], b"key"), Resp::Bulk(None));
        paused.resume().unwrap();
    }

    /// A client that leaves while its contended command waits for room in a
    /// full queue runs nothing, and holds inline leases back only until the
    /// worker answered the command the queue did take.
    #[tokio::test(start_paused = true)]
    async fn a_client_leaving_a_full_contended_queue_never_holds_the_shard_back() {
        use std::task::Poll;
        let (pool, queues) = queued_pool(1);
        let set_first = owned(&[b"SET", b"first", b"1"]);
        let set_left = owned(&[b"SET", b"left", b"1"]);
        let first = pool.contend(0, single(&set_first, None));
        tokio::pin!(first);
        assert!(poll_once(first.as_mut()).await.is_pending());
        // The queue holds one command, so this one waits for room.
        let mut left = Box::pin(pool.contend(0, single(&set_left, None)));
        assert!(poll_once(left.as_mut()).await.is_pending());
        assert!(pool.try_lease(0).is_none());
        // Its client leaves before the queue took it.
        drop(left);
        assert!(
            pool.try_lease(0).is_none(),
            "the queued command still holds inline leases back"
        );
        let worker = worker(&pool, queues, Arc::new(State::new(0, 1, false)));
        tokio::pin!(worker);
        // One poll runs its first maintenance pass and answers the queued
        // command; then it idles.
        assert!(poll_once(worker.as_mut()).await.is_pending());
        let Poll::Ready(Ok(Contention::Done(reply))) = poll_once(first.as_mut()).await else {
            panic!("the queued command answers");
        };
        assert_eq!(reply, Resp::Simple("OK"));
        let lease = pool.try_lease(0);
        assert!(lease.is_some(), "no command is left to give way to");
        drop(lease);
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"first"),
            Resp::Bulk(Some(Bytes::from_static(b"1")))
        );
        assert_eq!(get(&mut paused.engines[0], b"left"), Resp::Bulk(None));
        paused.resume().unwrap();
    }

    /// A write its worker claimed settles, commit included, though its client
    /// leaves while the commit waits, and keeps its write permit until then.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_claimed_contended_write_commits_after_its_client_leaves() {
        let directory = Directory::new();
        let persistence = journaled(&directory).await;
        // As `--data-dir` without `--appendonly`: writes commit in memory.
        persistence
            .configure(Some(false), None, std::iter::empty::<io::Result<Batch>>())
            .await
            .unwrap();
        // A WATCH makes the write capture its effects, and shows it ran.
        let watched = persistence
            .watches
            .register(0, Bytes::from_static(b"key"), None);
        let state = Arc::new(State::new(0, 1, false));
        let (pool, mut workers) = Pool::start(Engine::new(), 1, persistence.clone(), state);
        let revision = persistence.status().await.revision;
        // An exclusive writer holds the commit gate until released, so the
        // claimed write's commit waits on its worker.
        let (held, holding) = oneshot::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let writer = {
            let persistence = persistence.clone();
            let runtime = tokio::runtime::Handle::current();
            tokio::task::spawn_blocking(move || {
                let mut held = Some(held);
                runtime.block_on(persistence.checkpoint(std::iter::from_fn(move || {
                    if let Some(held) = held.take() {
                        let _ = held.send(());
                        let _ = released.recv();
                    }
                    None::<io::Result<Batch>>
                })))
            })
        };
        holding.await.unwrap();
        let gate = crate::write_gate::WriteGate::default();
        let permit = gate.admit().await;
        let client = {
            let pool = pool.clone();
            tokio::spawn(async move {
                let set = owned(&[b"SET", b"key", b"value"]);
                let mut write = single(&set, None);
                write.write_permit = Some(permit);
                pool.contend(0, write).await.map(|_| ())
            })
        };
        tokio::time::timeout(Duration::from_secs(10), async {
            while !watched.changed(Engine::now()) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("the worker claims and runs the write");
        // Its client leaves while the commit waits.
        client.abort();
        assert!(client.await.unwrap_err().is_cancelled());
        release.send(()).unwrap();
        writer.await.unwrap().unwrap();
        // A write pause completes only once the write committed and answered.
        gate.pause().await.unwrap();
        assert_eq!(persistence.status().await.revision, revision + 1);
        gate.resume();
        let mut paused = pool.pause().await.unwrap();
        assert_eq!(
            get(&mut paused.engines[0], b"key"),
            Resp::Bulk(Some(Bytes::from_static(b"value")))
        );
        paused.resume().unwrap();
        drop(pool);
        join_all(&mut workers).await;
    }

    /// Several workers let leases spin wherever the host has several CPUs.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn pause_queues_fairly_between_inline_leases() {
        let (pool, _queues) = idle_pool(Engine::new(), 2, Arc::new(Persistence::new(None)));
        let held = pool.try_lease(1).unwrap();
        let pause = pool.pause();
        tokio::pin!(pause);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), pause.as_mut())
                .await
                .is_err()
        );
        // The pause owns shard 0 and is first in line for shard 1, even
        // against a spinning lease.
        assert!(pool.try_lease(0).is_none());
        assert!(pool.try_lease(1).is_none());
        assert!(pool.try_lease_spinning(1).is_none());
        let later = pool.lease(1);
        tokio::pin!(later);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), later.as_mut())
                .await
                .is_err()
        );
        drop(held);
        // Released, shard 1 is the pause's before the pause runs again.
        assert!(pool.try_lease_spinning(1).is_none());
        let paused = pause.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), later.as_mut())
                .await
                .is_err()
        );
        paused.resume().unwrap();
        drop(later.await.unwrap());
        assert!(pool.try_lease(0).is_some());
    }

    #[tokio::test]
    async fn dropped_pause_poisons_every_shard_and_stops_the_server() {
        let (pool, _queues) = idle_pool(Engine::new(), 2, Arc::new(Persistence::new(None)));
        let supervisor = tokio::spawn(supervise(pool.shared.status.subscribe()));
        let state = State::new(0, 2, false);
        drop(pool.pause().await.unwrap());
        assert!(supervisor.await.unwrap().is_err());
        let get = owned(&[b"GET", b"key"]);
        assert!(
            pool.try_lease(0)
                .unwrap()
                .run(single(&get, None), &state)
                .is_err()
        );
        assert!(pool.lease(1).await.is_err());
        assert!(pool.pause().await.is_err());
    }

    #[tokio::test]
    async fn panic_under_an_inline_lease_poisons_its_shard_and_stops_the_server() {
        let (pool, _queues) = idle_pool(Engine::new(), 1, Arc::new(Persistence::new(None)));
        let supervisor = tokio::spawn(supervise(pool.shared.status.subscribe()));
        let mut lease = pool.try_lease(0).unwrap();
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            // The engine panics after its command began mutating.
            let _ = lease.cell.begin().unwrap();
            panic!("engine invariant violated");
        }));
        assert!(unwound.is_err());
        assert!(supervisor.await.unwrap().is_err());
        assert!(pool.lease(0).await.is_err());
        assert!(pool.pause().await.is_err());
    }

    #[tokio::test]
    async fn close_drains_deferred_commits_then_refuses_leases() {
        let directory = Directory::new();
        let (pool, _queues) = expired(&[b"gone"], &directory).await;
        let supervisor = tokio::spawn(supervise(pool.shared.status.subscribe()));
        let state = State::new(0, 1, false);
        let gone = owned(&[b"GET", b"gone"]);
        let Ran::Committing(mut response) = inline(&pool, &state, single(&gone, None)) else {
            panic!("a journaled deletion committed on the caller");
        };
        pool.close().await.unwrap();
        assert_eq!(response.try_recv().unwrap().unwrap(), Resp::Bulk(None));
        assert!(
            pool.try_lease(0)
                .unwrap()
                .run(single(&gone, None), &state)
                .is_err()
        );
        assert!(pool.lease(0).await.is_err());
        assert!(pool.pause().await.is_err());
        supervisor.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn close_ends_workers_while_connections_still_hold_the_pool() {
        let (pool, mut workers) = started(2);
        let lifetime = pool.lifetime();
        let mut overflow = JoinSet::new();
        let (request, response) = command(&[b"SET", b"queued", b"value"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        pool.close().await.unwrap();
        assert_eq!(response.await.unwrap(), Resp::Simple("OK"));
        // The coordinator leaving after `close` is a clean shutdown.
        drop(lifetime);
        join_all(&mut workers).await;
        join_all(&mut overflow).await;
        drop(pool);
    }

    #[tokio::test]
    async fn coordinator_exit_refuses_new_work_but_settles_mutated_commits() {
        let directory = Directory::new();
        let (pool, mut queues) = expired(&[b"gone"], &directory).await;
        let supervisor = tokio::spawn(supervise(pool.shared.status.subscribe()));
        let lifetime = pool.lifetime();
        let state = State::new(0, 1, false);
        let persistence = pool.shared.persistence.clone();
        let revision = persistence.status().await.revision;
        let gone = owned(&[b"GET", b"gone"]);
        let Ran::Committing(response) = inline(&pool, &state, single(&gone, None)) else {
            panic!("a journaled deletion committed on the caller");
        };
        // The coordinator fails while a connection still holds `pool`.
        drop(lifetime);
        assert!(supervisor.await.unwrap().is_err());
        // The command that already mutated still commits and answers.
        assert_eq!(response.await.unwrap().unwrap(), Resp::Bulk(None));
        assert_eq!(persistence.status().await.revision, revision + 1);
        // Nothing new mutates, inline or through the checked queue.
        assert!(pool.lease(0).await.is_err());
        let set = owned(&[b"SET", b"late", b"value"]);
        assert!(
            pool.try_lease(0)
                .unwrap()
                .run(single(&set, None), &state)
                .is_err()
        );
        assert!(pool.admit().await.is_err());
        let (request, response) = command(&[b"SET", b"late", b"value"]);
        let admission = Admission {
            _guard: pool.gate.clone().read_owned().await,
        };
        assert!(
            pool.submit(0, request, admission, &mut JoinSet::new())
                .is_err()
        );
        assert!(response.await.is_err());
        assert!(queues[0].requests.try_recv().is_err());
        let mut cell = pool.cells[0].lock().await;
        assert_eq!(
            get(cell.engine.as_mut().unwrap(), b"late"),
            Resp::Bulk(None)
        );
        assert_eq!(persistence.status().await.revision, revision + 1);
    }

    #[tokio::test]
    async fn coordinator_exit_ends_workers_that_connections_keep_alive() {
        let (pool, mut workers) = started(2);
        let connection = pool.clone();
        drop(pool.lifetime());
        drop(pool);
        // Every worker and the supervisor report the coordinator's failure.
        while let Some(result) = workers.join_next().await {
            assert!(result.unwrap().is_err());
        }
        assert!(connection.lease(0).await.is_err());
    }

    #[tokio::test]
    async fn dropping_the_worker_set_never_cancels_an_accepted_command() {
        let (pool, workers) = started(1);
        let mut overflow = JoinSet::new();
        // A reply proves the workers already run.
        let (request, primed) = command(&[b"SET", b"primed", b"1"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        assert_eq!(primed.await.unwrap(), Resp::Simple("OK"));
        // The worker takes the next command, then waits behind this lease.
        let lease = pool.lease(0).await.unwrap();
        let (request, mut accepted) = command(&[b"INCR", b"accepted"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut accepted)
                .await
                .is_err()
        );
        // Dropping the set aborts what it can; the accepted command settles.
        drop(workers);
        drop(lease);
        assert_eq!(accepted.await.unwrap(), Resp::Integer(1));
        join_all(&mut overflow).await;
        let mut status = pool.shared.status.subscribe();
        pool.close().await.unwrap();
        drop(pool);
        // The group then ends and releases every shared reference.
        let released = tokio::time::timeout(Duration::from_secs(10), status.wait_for(|_| false));
        assert!(released.await.unwrap().is_err());
    }

    #[tokio::test]
    async fn a_failed_worker_stops_the_pool_while_a_sibling_is_busy() {
        let (pool, mut workers) = started(2);
        let mut overflow = JoinSet::new();
        // A reply proves the workers already run.
        let (request, primed) = command(&[b"SET", b"behind", b"1"]);
        submit(&pool, &mut overflow, 1, request).await.unwrap();
        assert_eq!(primed.await.unwrap(), Resp::Simple("OK"));
        // Shard 1's worker takes a command, then waits behind this lease.
        let lease = pool.lease(1).await.unwrap();
        let (request, mut behind) = command(&[b"INCR", b"behind"]);
        submit(&pool, &mut overflow, 1, request).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut behind)
                .await
                .is_err()
        );
        // A blocking result escaping its coordinator route is fatal to shard 0.
        let (request, failed) = command(&[b"BLPOP", b"missing", b"0"]);
        submit(&pool, &mut overflow, 0, request).await.unwrap();
        assert!(failed.await.is_err());
        // Reported at once, while the group still waits for shard 1.
        let reported = tokio::time::timeout(Duration::from_secs(10), workers.join_next())
            .await
            .unwrap();
        assert!(reported.unwrap().unwrap().is_err());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), workers.join_next())
                .await
                .is_err()
        );
        // After the fatal stop the accepted command never runs.
        drop(lease);
        assert!(behind.await.is_err());
        while let Some(result) = workers.join_next().await {
            assert!(result.unwrap().is_err());
        }
        join_all(&mut overflow).await;
    }
}
