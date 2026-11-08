//! One commit order shared by WAL, independent online backups, native replication,
//! cache invalidations and all shard actors. Replies follow successful commit.
use crate::{
    engine::Engine,
    journal::{Batch, Mutation},
    wal::{Durability, Wal},
    watch::Registry,
};
use bytes::Bytes;
use std::{
    io,
    marker::PhantomData,
    num::NonZeroUsize,
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};
use tokio::{
    sync::{OwnedRwLockWriteGuard, RwLock, RwLockReadGuard, RwLockWriteGuard},
    task::coop,
};

pub struct Persistence {
    /// The commit gate's global lock and guarded state. An exclusive writer
    /// (`Exclusive`) holds it for writing with every lane closed; that covers
    /// every WAL or backup await and every control change: durable and
    /// backup-captured commits, sync, checkpoint, AOF configuration and
    /// backups, so `enabled` changes only under it. Memory commits and
    /// ephemeral publications are CPU-only and take a share (`Shared`) of
    /// this lock or of their thread's lane, never awaiting while they hold it.
    gate: RwLock<Commit>,
    /// The gate's lanes, a power-of-two count fixed at construction: an
    /// inline memory commit locks only its own OS thread's lane.
    lanes: Box<[Arc<RwLock<Lane>>]>,
    /// Exclusive writers registered for the gate; see `try_share`.
    writers: Writers,
    /// Orders publications made under shares; see `in_publication_order`.
    /// A memory commit with nothing to order skips it; see `unordered_offset`.
    publication: Mutex<()>,
    enabled: AtomicBool,
    failed: AtomicBool,
    pub watches: Arc<Registry>,
    /// The revision, counted per lane: see `revision`.
    revisions: Box<[Revision]>,
    last_sync_ms: AtomicU64,
    checkpoint_bytes: AtomicU64,
    native: OnceLock<Arc<crate::native_replication::NativeReplication>>,
    backup: OnceLock<Arc<crate::server_runtime::OnlineBackup>>,
    clients: OnceLock<Arc<crate::client_registry::Registry>>,
    published_offset: AtomicU64,
    /// Native offset a real fsync covers; `None` when nothing is proven
    /// durable. Written by an exclusive writer or at setup.
    fsynced_offset: Mutex<Option<u64>>,
}

pub struct Status {
    pub bytes: u64,
    pub policy: &'static str,
    pub dirty: bool,
    pub last_sync_ms: Option<i64>,
    pub revision: u64,
}

/// One command's captured effects, detached from its engine so the commit can
/// settle on the executing thread or in a continuation that still owns the
/// shard lease. A non-empty stage must be settled exactly once.
#[must_use = "captured effects are lost unless committed"]
pub struct Staged {
    batch: Option<Batch>,
    session: Option<Arc<crate::session::ClientSession>>,
    /// `batch`'s native stream record, encoded before the commit gate unless
    /// lazy history looked inactive, and kept across a refused
    /// `try_commit_now`; only the gate decides its use.
    native: Option<io::Result<Bytes>>,
}

impl Staged {
    /// Replies follow this store, so a session's WAIT never misses its write.
    fn committed(self, offset: u64) {
        if let Some(session) = self.session {
            session.last_write_offset.store(offset, Ordering::Release);
        }
    }
}

/// Marks persistence failed if a check unwinds while this is alive, so a
/// memory commit outside `in_publication_order` still fails persistence
/// closed after a panic, as that order's poisoned mutex does.
struct FailClosedOnUnwind<'a>(&'a AtomicBool);
impl Drop for FailClosedOnUnwind<'_> {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0.store(true, Ordering::Release);
        }
    }
}

/// Bounds the lanes, and so the locks one writer's drain acquires.
const MAX_LANES: usize = 64;

/// Aligns each lane's lock to 128 bytes, an Apple silicon cache line and an
/// x86 adjacent-line prefetch pair, so no two lanes share one.
#[repr(align(128))]
struct Lane;

/// The registered-writer count, alone on its line: every inline memory
/// commit reads it twice, and only writers change it.
#[repr(align(128))]
struct Writers(AtomicUsize);

/// One lane's count of installed changes, alone on its line, so commits on
/// different threads never write a line another thread's commit reads.
#[repr(align(128))]
struct Revision(AtomicU64);

/// Every lane, closed by one exclusive writer.
type ClosedLanes = Box<[OwnedRwLockWriteGuard<Lane>]>;

/// The commit gate's guarded state.
struct Commit {
    wal: Option<Wal>,
    /// Every lane, kept closed by the last exclusive writer because commits
    /// could not settle in memory when it ended, so the next ordinary AOF or
    /// captured commit takes them instead of draining every lane again.
    /// Writers change it under the global write lock; under a global read,
    /// only the one reader that claims `lanes_parked` takes it, to reopen.
    parked: Mutex<Option<ClosedLanes>>,
    /// Set by the writer that parks lanes and cleared by whoever takes them,
    /// so a global reader locks `parked` only to reopen parked lanes.
    lanes_parked: AtomicBool,
}

/// Proof that no exclusive writer exists while it lives, so the guarded WAL
/// and `enabled` hold still: a read of this OS thread's lane, which every
/// `Exclusive` keeps closed, or a read of the global lock, which every
/// `Exclusive` holds for writing. Not `Send`: a lane read stays on its thread
/// in synchronous code and never crosses an await, so a writer's drain
/// waits only for CPU-only work.
struct Shared<'a> {
    proof: Proof<'a>,
    _thread: PhantomData<*const ()>,
}

enum Proof<'a> {
    Lane { _read: RwLockReadGuard<'a, Lane> },
    Global(RwLockReadGuard<'a, Commit>),
}

impl<'a> Shared<'a> {
    fn new(proof: Proof<'a>) -> Self {
        Self {
            proof,
            _thread: PhantomData,
        }
    }
    fn global(commit: RwLockReadGuard<'a, Commit>) -> Self {
        Self::new(Proof::Global(commit))
    }
    /// Reopens lanes the last writer parked, so later inline commits take
    /// lanes again. Only a global read can find them parked, as a lane read
    /// needs an open lane. Any share excludes every writer, so reopening is
    /// always safe, and every later writer closes all lanes again; callers
    /// reopen only once they saw `commits_in_memory` under this share, since
    /// reopened lanes cost the next writer a drain.
    #[inline]
    fn reopen_lanes(&self) {
        let Proof::Global(commit) = &self.proof else {
            return;
        };
        if commit.lanes_parked.load(Ordering::Acquire)
            && commit.lanes_parked.swap(false, Ordering::AcqRel)
        {
            let lanes = commit
                .parked
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take();
            drop(lanes);
        }
    }
}

/// One exclusive writer, registered from before it queues for the global
/// lock until it is gone. While any is registered, inline memory commits
/// take no share, so none overtakes a queued writer.
struct Registered<'a>(&'a AtomicUsize);

impl<'a> Registered<'a> {
    fn new(writers: &'a AtomicUsize) -> Self {
        // Every registration is a live writer, so the count cannot reach
        // `usize::MAX`. Were it to, wrapping to zero would admit inline
        // commits past queued writers.
        if writers
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                count.checked_add(1)
            })
            .is_err()
        {
            std::process::abort();
        }
        Self(writers)
    }
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The commit gate's write share: the global write lock and every lane,
/// closed, so no `Shared` exists. Only `Persistence::exclusive` makes one.
/// Fields drop in declaration order once `drop` has handed the lanes on: the
/// global lock, then the registration.
struct Exclusive<'a> {
    persistence: &'a Persistence,
    /// `Some` until `drop`.
    lanes: Option<ClosedLanes>,
    commit: RwLockWriteGuard<'a, Commit>,
    _registered: Registered<'a>,
}

impl Exclusive<'_> {
    fn wal(&mut self) -> &mut Option<Wal> {
        &mut self.commit.wal
    }
}

impl Drop for Exclusive<'_> {
    fn drop(&mut self) {
        // The global lock and the registration still exclude every other
        // writer here. Lanes reopen when commits may settle in memory, and
        // otherwise stay closed for the next writer. Backup capture toggles
        // outside the gate, so this choice is advisory only: a global reader
        // that sees memory commits resume reopens parked lanes, and a writer
        // closes open ones.
        let lanes = self.lanes.take();
        if !self.persistence.commits_in_memory() {
            let commit = &mut *self.commit;
            *commit
                .parked
                .get_mut()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = lanes;
            *commit.lanes_parked.get_mut() = true;
        }
    }
}

/// A publication's place in the one commit order.
enum Turn<'t, 'a> {
    /// Inside `in_publication_order`, for a holder that saw
    /// `commits_in_memory` under this share, so `enabled` is false.
    Shared { _share: &'t Shared<'a> },
    /// An exclusive writer's own tail, after any WAL append for its batch.
    Exclusive(&'t Exclusive<'a>),
}

/// Numbers each thread on its first inline commit. Mapping that number to a
/// lane is stable for the thread; collisions only share a lane.
fn thread_lane() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static LANE: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    LANE.with(|lane| *lane)
}

impl Persistence {
    pub fn new(wal: Option<Wal>) -> Self {
        // Twice the cores leaves room for committing threads besides the
        // workers before two threads share a lane.
        let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let lanes = cores.saturating_mul(2).min(MAX_LANES).next_power_of_two();
        Self {
            enabled: AtomicBool::new(wal.is_some()),
            gate: RwLock::new(Commit {
                wal,
                parked: Mutex::new(None),
                lanes_parked: AtomicBool::new(false),
            }),
            lanes: (0..lanes).map(|_| Arc::new(RwLock::new(Lane))).collect(),
            revisions: (0..lanes).map(|_| Revision(AtomicU64::new(0))).collect(),
            writers: Writers(AtomicUsize::new(0)),
            publication: Mutex::new(()),
            failed: AtomicBool::new(false),
            watches: Arc::new(Registry::default()),
            last_sync_ms: AtomicU64::new(0),
            checkpoint_bytes: AtomicU64::new(0),
            native: OnceLock::new(),
            backup: OnceLock::new(),
            clients: OnceLock::new(),
            published_offset: AtomicU64::new(0),
            fsynced_offset: Mutex::new(None),
        }
    }
    #[inline]
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
    #[inline]
    pub fn capturing(&self) -> bool {
        self.enabled()
            || self.native.get().is_some()
            || self.backup_active()
            || self.watches.active()
    }
    #[inline]
    fn backup_active(&self) -> bool {
        self.backup.get().is_some_and(|backup| backup.active())
    }
    /// Advisory: a commit could currently settle in memory (ordinary AOF off,
    /// no online backup capturing). Racy outside the commit gate; commits
    /// recheck it under a share.
    #[inline]
    pub fn commits_in_memory(&self) -> bool {
        !self.enabled() && !self.backup_active()
    }
    #[inline]
    pub fn recording_for(&self, _args: &[Bytes]) -> bool {
        self.capturing()
    }
    pub fn set_native(&self, native: Arc<crate::native_replication::NativeReplication>) {
        let status = native.status();
        self.published_offset
            .store(status.offset, Ordering::Release);
        self.set_fsynced(status.fsynced_offset);
        assert!(
            self.native.set(native).is_ok(),
            "native replication already configured"
        );
    }
    pub fn set_backup(&self, backup: Arc<crate::server_runtime::OnlineBackup>) {
        assert!(
            self.backup.set(backup).is_ok(),
            "online backup already configured"
        );
    }
    pub fn set_clients(&self, clients: Arc<crate::client_registry::Registry>) {
        assert!(
            self.clients.set(clients).is_ok(),
            "client registry already configured"
        );
    }
    pub fn native_offset(&self) -> u64 {
        self.published_offset.load(Ordering::Acquire)
    }
    pub fn last_fsynced_native_offset(&self) -> Option<u64> {
        *self
            .fsynced_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    fn set_fsynced(&self, offset: Option<u64>) {
        *self
            .fsynced_offset
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = offset;
    }
    #[inline]
    pub fn ensure_healthy(&self) -> io::Result<()> {
        // Exclusive WAL/control paths and unordered memory commits do not take
        // the publication mutex. They must still fail closed after a share
        // holder's publication unwinds.
        if self.failed.load(Ordering::Acquire) || self.publication.is_poisoned() {
            self.failed.store(true, Ordering::Release);
            Err(io::Error::other(
                "persistence is unavailable after a commit failure",
            ))
        } else {
            Ok(())
        }
    }

    pub async fn commit(&self, engine: &mut Engine) -> io::Result<()> {
        self.commit_staged(self.stage(engine)).await
    }

    /// Detach the executed command's effects and encode their native stream
    /// record outside the commit gate. Empty stages capture nothing.
    pub fn stage(&self, engine: &mut Engine) -> Staged {
        self.stage_with(engine, false)
    }

    /// [`Self::stage`] for a holder that clears the command identity right
    /// after: a captured batch takes the engine's session reference instead
    /// of another one.
    pub fn stage_detaching(&self, engine: &mut Engine) -> Staged {
        self.stage_with(engine, true)
    }

    #[inline]
    fn stage_with(&self, engine: &mut Engine, detach: bool) -> Staged {
        engine.finish_pending_search();
        let Some(batch) = engine.take_batch() else {
            return Staged {
                batch: None,
                session: None,
                native: None,
            };
        };
        Staged {
            native: self.prepare_native(&batch),
            session: if detach {
                engine.take_command_session()
            } else {
                engine.command_session().cloned()
            },
            batch: Some(batch),
        }
    }

    pub async fn commit_staged(&self, mut staged: Staged) -> io::Result<()> {
        let Some(batch) = &staged.batch else {
            return Ok(());
        };
        let origin = staged.session.as_ref().map(|session| session.client_id);
        let offset = self
            .commit_ordered(batch, staged.native.take(), true, origin)
            .await?;
        staged.committed(offset);
        Ok(())
    }

    /// Settle `staged` on the calling thread without awaiting or disk I/O.
    /// `Err` returns the stage with its prepared native record: nothing was
    /// committed or published, because a writer holds, awaits or has
    /// registered for the commit gate, ordinary AOF is on, or an online
    /// backup is capturing. `Ok(Err)` is a settled commit failure, exactly as
    /// from `commit_staged`.
    ///
    /// Order: `try_share` refuses while any writer is registered, which it is
    /// before it queues for the gate, so nothing bypasses a pending AOF or
    /// backup cutover, checkpoint, sync or durable commit; `enabled` changes
    /// only under an exclusive writer, which the share excludes. Concurrent
    /// memory commits publish in the one order of `in_publication_order`,
    /// briefly waiting there for other commits' CPU-only tails, unless
    /// `unordered_offset` finds nothing to order. Backup `active` is set
    /// before BASE pauses every shard; the caller holds this command's shard
    /// lease, so a batch skipped here is already in the engine that BASE
    /// snapshots, as for any memory commit in `commit_ordered`.
    pub fn try_commit_now(&self, mut staged: Staged) -> Result<io::Result<()>, Staged> {
        let Some(batch) = &staged.batch else {
            return Ok(Ok(()));
        };
        // Advisory, as refusing is always safe: ordinary AOF and capturing
        // backups then touch no lock.
        if !self.commits_in_memory() {
            return Err(staged);
        }
        let Some(shared) = self.try_share() else {
            return Err(staged);
        };
        if !self.commits_in_memory() {
            return Err(staged);
        }
        shared.reopen_lanes();
        let origin = staged.session.as_ref().map(|session| session.client_id);
        let committed = self.commit_in_memory(&shared, batch, staged.native.take(), true, origin);
        Ok(committed.map(|offset| staged.committed(offset)))
    }

    /// A share for an inline commit, without awaiting: a read of this OS
    /// thread's lane, or of the global lock while the last writer's lanes
    /// stay parked. `None` while any writer is registered, which it is from
    /// before it queues for the global lock until it is gone, so no share
    /// overtakes a queued writer. A writer registering after the first check
    /// still closes this thread's lane, so it waits for a share taken first;
    /// the second check releases a share taken after such a registration, so
    /// a drain never waits for later commits.
    #[inline]
    fn try_share(&self) -> Option<Shared<'_>> {
        if self.writers.0.load(Ordering::SeqCst) != 0 {
            return None;
        }
        let lane = &self.lanes[thread_lane() & (self.lanes.len() - 1)];
        let proof = match lane.try_read() {
            Ok(read) => Proof::Lane { _read: read },
            // With no writer registered, every lane is parked, or a
            // cancelled writer is releasing them.
            Err(_) => Proof::Global(self.gate.try_read().ok()?),
        };
        if self.writers.0.load(Ordering::SeqCst) != 0 {
            return None;
        }
        Some(Shared::new(proof))
    }

    /// The commit gate's write share. It registers first, so inline commits
    /// take no share once it may queue for the global lock, FIFO behind
    /// earlier shares and writers. It then takes every lane: those the last
    /// writer parked, or each one in turn, which waits only for inline
    /// commits' CPU-only shares. Cancelled at either await, it releases what
    /// it acquired, having changed nothing.
    async fn exclusive(&self) -> Exclusive<'_> {
        let registered = Registered::new(&self.writers.0);
        let mut commit = self.gate.write().await;
        let parked = commit
            .parked
            .get_mut()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        *commit.lanes_parked.get_mut() = false;
        let lanes = match parked {
            Some(lanes) => lanes,
            // The global lock spent this writer's one budget unit, as before
            // lanes existed; a forced yield here would hold that lock idle.
            None => {
                coop::unconstrained(async {
                    let mut lanes = Vec::with_capacity(self.lanes.len());
                    for lane in &self.lanes {
                        lanes.push(lane.clone().write_owned().await);
                    }
                    lanes.into_boxed_slice()
                })
                .await
            }
        };
        Exclusive {
            persistence: self,
            lanes: Some(lanes),
            commit,
            _registered: registered,
        }
    }

    pub async fn commit_batch(&self, batch: &Batch) -> io::Result<()> {
        self.commit_batch_offset(batch, None).await.map(|_| ())
    }
    pub async fn commit_batch_offset(&self, batch: &Batch, origin: Option<u64>) -> io::Result<u64> {
        let prepared = self.prepare_native(batch);
        self.commit_ordered(batch, prepared, true, origin).await
    }
    /// Pub/sub has no recoverable dataset effect, but its stream watermark must
    /// share commit order and become durable only after a real subsequent fsync.
    /// CPU-only in every AOF mode, so it publishes under a global read share
    /// like an async memory commit; writers, including every fsync, exclude it.
    pub async fn publish_ephemeral(&self, args: &[Bytes]) -> io::Result<Option<u64>> {
        let shared = Shared::global(self.gate.read().await);
        self.in_publication_order(&shared, || {
            self.ensure_healthy()?;
            let Some(native) = self.publishing_native() else {
                return Ok(None);
            };
            let offset = native.publish(crate::native_replication::encode_command(args)?, false)?;
            self.published_offset.store(offset, Ordering::Release);
            Ok(Some(offset))
        })
    }
    pub async fn commit_replicated_batch(&self, batch: &Batch) -> io::Result<()> {
        self.commit_ordered(batch, None, false, None)
            .await
            .map(|_| ())
    }
    /// A memory-eligible commit takes a global read share, reopens any lanes
    /// the last writer parked, and takes the same synchronous path as
    /// `try_commit_now`, so a contended memory commit waits only for writers
    /// queued ahead of it, never queuing as a writer itself. Any other commit
    /// takes the exclusive share, which excludes every share, and decides
    /// health, role, AOF and backup capture again under it. The prepared
    /// record is consumed only once the path is fixed.
    async fn commit_ordered(
        &self,
        batch: &Batch,
        prepared: Option<io::Result<Bytes>>,
        publish: bool,
        origin: Option<u64>,
    ) -> io::Result<u64> {
        if self.commits_in_memory() {
            let commit = self.gate.read().await;
            if self.commits_in_memory() {
                let shared = Shared::global(commit);
                shared.reopen_lanes();
                return self.commit_in_memory(&shared, batch, prepared, publish, origin);
            }
        }
        let mut exclusive = self.exclusive().await;
        let encoded = self.encode_native(batch, publish, prepared)?;
        if self.enabled() {
            let wal = exclusive
                .wal()
                .as_mut()
                .ok_or_else(|| io::Error::other("enabled journal has no WAL"))?;
            if let Err(error) = wal.append(batch).await {
                self.failed.store(true, Ordering::Release);
                return Err(error);
            }
            if !wal.is_dirty() {
                self.note_sync();
            }
        }
        if let Some(backup) = self.backup.get() {
            backup.record_batch(batch).await;
        }
        self.publish_committed(Turn::Exclusive(&exclusive), batch, encoded, publish, origin)
    }

    /// Pure step before the commit gate for a publishing commit: an immutable
    /// batch's native record does not depend on when or in which role it
    /// commits, so this touches no offset, role or health state. Keep errors
    /// too: the gate decides whether they apply, without encoding twice.
    /// Inactive lazy history skips the work; that read is only advisory, as
    /// the gate encodes a missing record once history is active.
    fn prepare_native(&self, batch: &Batch) -> Option<io::Result<Bytes>> {
        self.native
            .get()
            .filter(|native| native.history_active())
            .map(|_| crate::native_replication::encode_batch(batch))
    }

    /// The stream a publication goes to, decided at its commit gate: none on
    /// a replica or before lazy history activation. A batch commit still holds
    /// the shard lease, coordinator turn or pause it mutated under, which
    /// `NativeReplication::activate` relies on: "inactive" here means the
    /// first snapshot's pause waits for this commit, so its image has the
    /// effect. Ephemeral publications have no effect to lose.
    fn publishing_native(&self) -> Option<&Arc<crate::native_replication::NativeReplication>> {
        self.native
            .get()
            .filter(|native| !native.is_replica() && native.history_active())
    }

    /// Pre-effect step in commit order; an error here leaves no effect.
    /// Health, role and history activation are decided here, never at
    /// preparation: a prepared record is dropped unpublished on a replica, and
    /// a missing one (staged before activation) is encoded now, so an encoding
    /// error still surfaces only after the health check and only where the
    /// commit would publish.
    fn encode_native(
        &self,
        batch: &Batch,
        publish: bool,
        prepared: Option<io::Result<Bytes>>,
    ) -> io::Result<Option<Bytes>> {
        self.ensure_healthy()?;
        if !(publish && self.publishing_native().is_some()) {
            return Ok(None);
        }
        match prepared {
            Some(encoded) => encoded.map(Some),
            None => crate::native_replication::encode_batch(batch).map(Some),
        }
    }

    /// The memory commit: the caller holds a share and saw
    /// `commits_in_memory` under it, so no WAL append or backup capture
    /// applies. Encoding, publication and the session offset it returns all
    /// take one turn of the total publication order, unless the commit has
    /// nothing to order there (`unordered_offset`). A prepared record means
    /// history was already active when the batch was staged.
    fn commit_in_memory(
        &self,
        shared: &Shared<'_>,
        batch: &Batch,
        prepared: Option<io::Result<Bytes>>,
        publish: bool,
        origin: Option<u64>,
    ) -> io::Result<u64> {
        if publish && prepared.is_none() {
            let _fail_closed = FailClosedOnUnwind(&self.failed);
            if let Some(offset) = self.unordered_offset(shared)? {
                self.note_install()?;
                return Ok(offset);
            }
        }
        self.in_publication_order(shared, || {
            let encoded = self.encode_native(batch, publish, prepared)?;
            let turn = Turn::Shared { _share: shared };
            self.publish_committed(turn, batch, encoded, publish, origin)
        })
    }

    /// The session offset of a publishing memory commit whose ordered tail
    /// would only advance the revision, or `None` to take that tail. The tail
    /// orders native appends, published offsets and tracking invalidations;
    /// the engine already observed WATCH versions for a publishing commit.
    /// So this requires no counted tracking client, no WATCH token
    /// (conservatively), and no stream recording client writes: none is
    /// configured, or a primary's lazy history is still inactive. Health is
    /// checked first, as `encode_native` does, and `is_replica` before the
    /// history flag, so a poisoned history state fails the commit even while
    /// inactive; the caller holds `FailClosedOnUnwind`. Errors leave no effect.
    ///
    /// Offsets. The share excludes every writer, so beside it only a
    /// publication moves the published offset, and each one follows an
    /// "active" read: `publishing_native`, or a replica applying its upstream
    /// stream, whose `set_upstream` activated history first. The offset is
    /// read before the history flag: had it come from such a publication, the
    /// activation that publication read would happen-before this flag read,
    /// which would then read "active". So "inactive" means the offset predates
    /// activation: this commit is ordered before activation and every
    /// publication of the activated history, as if it had taken the mutex
    /// there and published nothing. Its revision is one atomic increment.
    ///
    /// Snapshot. Activation precedes the first snapshot's `Pool::pause`,
    /// which waits for the shard lease, coordinator turn or pause this commit
    /// still holds (`NativeReplication::activate`), so its effect is in that
    /// image and its offset at or before that snapshot's offset. Every command
    /// after the pause happens-after activation and publishes in order; one
    /// staged before it is encoded late by `encode_native`.
    ///
    /// Observers. Enabling tracking counts the client before its reply, and a
    /// tracked read or WATCH registration takes the engine this commit
    /// mutated under and still holds. A zero count here therefore means no
    /// tracked read preceded this mutation; a client counted later, BCAST
    /// included, is ordered after this commit and owed no notice for it, as
    /// when the ordered tail reads the count later.
    fn unordered_offset(&self, _shared: &Shared<'_>) -> io::Result<Option<u64>> {
        self.ensure_healthy()?;
        if self.watches.active()
            || self
                .clients
                .get()
                .is_some_and(|clients| clients.tracking_active())
        {
            return Ok(None);
        }
        let offset = self.native_offset();
        let streaming = self
            .native
            .get()
            .is_some_and(|native| native.is_replica() || native.history_active());
        Ok((!streaming).then_some(offset))
    }

    /// Runs a share holder's publication `tail` in the one total publication
    /// order. The share excludes every writer, whose own tails therefore need
    /// no mutex; this mutex orders share holders, so native appends,
    /// offsets and invalidations never interleave or regress. Revisions are
    /// atomic increments, so commits outside this order never regress them.
    ///
    /// Invariant: `tail` only appends to the in-memory native backlog, observes
    /// WATCH versions, queues tracking notices with non-blocking sends and
    /// bumps counters. It never awaits (this function is synchronous, so the
    /// guard cannot cross an await), does no file or network I/O and takes no
    /// lock that is held while acquiring this one, so a waiter only waits for
    /// another tail's bounded CPU work. A failure after a tail's first effect
    /// marks persistence failed before the guard drops (`publish_committed`),
    /// so no later publication enters past it; earlier errors leave no
    /// effect. A tail that panicked may have published partially, so poison
    /// fails persistence closed too. An unordered memory commit checks health
    /// on entry and publishes nothing, so one that passed it before a failure
    /// is ordered before that failure.
    fn in_publication_order<T>(
        &self,
        _shared: &Shared<'_>,
        tail: impl FnOnce() -> io::Result<T>,
    ) -> io::Result<T> {
        let Ok(_order) = self.publication.lock() else {
            self.failed.store(true, Ordering::Release);
            return Err(io::Error::other(
                "persistence is unavailable after a panicked publication",
            ));
        };
        tail()
    }

    /// Synchronous post-WAL tail shared by every batch commit, taking `turn`
    /// in the commit order. Any WAL append and backup capture for `batch`
    /// have already happened. Only an exclusive writer's guarded WAL can make
    /// the publication fsynced: a share holder saw `commits_in_memory` under
    /// its share, which keeps `enabled` false.
    fn publish_committed(
        &self,
        turn: Turn<'_, '_>,
        batch: &Batch,
        encoded: Option<Bytes>,
        publish: bool,
        origin: Option<u64>,
    ) -> io::Result<u64> {
        let mut offset = self.native_offset();
        if let (Some(native), Some(encoded)) = (self.native.get(), encoded)
            && !encoded.is_empty()
        {
            let fsynced = match turn {
                Turn::Shared { .. } => {
                    debug_assert!(!self.enabled(), "a share published under ordinary AOF");
                    false
                }
                Turn::Exclusive(exclusive) => {
                    self.enabled()
                        && exclusive
                            .commit
                            .wal
                            .as_ref()
                            .is_some_and(|wal| !wal.is_dirty())
                }
            };
            match native.publish(encoded, fsynced) {
                Ok(end) => {
                    offset = end;
                    self.published_offset.store(end, Ordering::Release);
                    if fsynced {
                        self.set_fsynced(Some(end));
                    }
                }
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error);
                }
            }
        }
        if !publish {
            if let Some(recovered_offset) =
                batch
                    .mutations
                    .iter()
                    .rev()
                    .find_map(|mutation| match mutation {
                        Mutation::NativePosition { offset, .. } => Some(*offset),
                        _ => None,
                    })
            {
                self.published_offset
                    .store(recovered_offset, Ordering::Release);
                offset = self.native_offset();
            }
            let mut database = 0;
            for mutation in &batch.mutations {
                if let Mutation::SelectDatabase { database: selected } = mutation {
                    database = *selected;
                } else {
                    self.watches.observe(database, mutation);
                }
            }
        }
        self.invalidate(batch, origin);
        self.note_install()?;
        Ok(offset)
    }

    /// The registry counts a tracking client before its CLIENT TRACKING reply.
    /// A tracked read records itself before releasing the engine (shard lease
    /// or pause) that this batch later mutated, so zero cannot hide a notice.
    fn invalidate(&self, batch: &Batch, origin: Option<u64>) {
        let Some(clients) = self
            .clients
            .get()
            .filter(|clients| clients.tracking_active())
        else {
            return;
        };
        let mut first: Option<&Bytes> = None;
        let mut multiple = Vec::new();
        for mutation in &batch.mutations {
            let keys: &[Bytes] = match mutation {
                Mutation::Clear | Mutation::SwapDatabases { .. } => {
                    clients.invalidate_all(None, origin);
                    return;
                }
                Mutation::Rename { from, to } => {
                    if let Some(key) = first.take() {
                        multiple.push(key.clone());
                    }
                    multiple.push(from.clone());
                    multiple.push(to.clone());
                    continue;
                }
                Mutation::Set { key, .. }
                | Mutation::Delete { key }
                | Mutation::Push { key, .. }
                | Mutation::Pop { key, .. }
                | Mutation::Expire { key, .. }
                | Mutation::SetAdd { key, .. }
                | Mutation::SetRemove { key, .. }
                | Mutation::HashSet { key, .. }
                | Mutation::HashDelete { key, .. }
                | Mutation::SortedSetAdd { key, .. }
                | Mutation::SortedSetRemove { key, .. }
                | Mutation::ListRemove { key, .. }
                | Mutation::ListSet { key, .. }
                | Mutation::ListInsert { key, .. }
                | Mutation::Queue { key, .. }
                | Mutation::Typed { key, .. } => std::slice::from_ref(key),
                Mutation::SelectDatabase { .. }
                | Mutation::Search { .. }
                | Mutation::Functions { .. }
                | Mutation::NativePosition { .. }
                | Mutation::RdbAuxiliary { .. }
                | Mutation::ResetGlobals
                | Mutation::ClearNativePosition => continue,
            };
            for key in keys {
                if !multiple.is_empty() {
                    multiple.push(key.clone());
                } else if let Some(previous) = first {
                    if previous != key {
                        multiple.push(previous.clone());
                        multiple.push(key.clone());
                        first = None;
                    }
                } else {
                    first = Some(key);
                }
            }
        }
        if let Some(key) = first {
            clients.invalidate(0, std::slice::from_ref(key), origin);
        } else if !multiple.is_empty() {
            multiple.sort_unstable();
            multiple.dedup();
            clients.invalidate(0, &multiple, origin);
        }
    }

    pub fn note_reads(
        &self,
        command: &crate::engine::BoundCommand<'_>,
        session: Option<&crate::session::ClientSession>,
    ) {
        let (Some(clients), Some(session)) = (
            self.clients
                .get()
                .filter(|clients| clients.tracking_active()),
            session,
        ) else {
            return;
        };
        if command.is_readonly() {
            command.any_accessed_key(|key| {
                clients.read(
                    session.client_id,
                    session.database(),
                    std::slice::from_ref(key),
                );
                false
            });
        }
    }

    pub async fn periodic_sync(&self) -> io::Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        let wal = exclusive.wal().as_mut().expect("persistent WAL");
        let dirty = wal.is_dirty();
        let offset = self.native_offset();
        let clean_offset_pending = !dirty
            && self
                .last_fsynced_native_offset()
                .is_none_or(|fsynced| offset > fsynced);
        let sync = if clean_offset_pending {
            wal.finish().await
        } else {
            wal.periodic_sync().await
        };
        if let Err(error) = sync {
            self.failed.store(true, Ordering::Release);
            return Err(error);
        }
        if clean_offset_pending || (dirty && !wal.is_dirty()) {
            self.note_sync();
            if let Some(native) = self.native.get()
                && !native.is_replica()
            {
                native.mark_fsynced(offset)?;
            }
            self.set_fsynced(Some(offset));
        }
        Ok(())
    }
    pub async fn finish(&self) -> io::Result<()> {
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        if let Some(wal) = exclusive.wal().as_mut() {
            if let Err(error) = wal.finish().await {
                self.failed.store(true, Ordering::Release);
                return Err(error);
            }
            self.note_sync();
            if self.enabled() {
                let offset = self.native_offset();
                if let Some(native) = self.native.get()
                    && !native.is_replica()
                {
                    native.mark_fsynced(offset)?;
                }
                self.set_fsynced(Some(offset));
            }
        }
        Ok(())
    }
    /// Counts one installed change on this thread's lane, so commits on
    /// different threads never contend for one line. A lane that would pass
    /// `u64::MAX` fails persistence closed, as the single count did.
    pub fn note_install(&self) -> io::Result<()> {
        self.revisions[thread_lane() & (self.revisions.len() - 1)]
            .0
            .try_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                revision.checked_add(1)
            })
            .map(|_| ())
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                io::Error::other("replication revision exhausted")
            })
    }
    /// Changes installed so far: the sum of the lane counts, saturating at
    /// `u64::MAX`. Each count only grows, so successive reads never regress,
    /// and every install that happened-before this read is included; with no
    /// install running beside it (a pause holds every shard) it is exact.
    fn revision(&self) -> u64 {
        self.revisions.iter().fold(0, |total: u64, lane| {
            total.saturating_add(lane.0.load(Ordering::Acquire))
        })
    }
    fn note_sync(&self) {
        self.last_sync_ms
            .store(Engine::now().max(0) as u64, Ordering::Release);
    }
    /// A global read share is a coherent snapshot: WAL size, policy,
    /// dirtiness, `enabled` and the last sync change only under an exclusive
    /// writer, and memory commits beside it advance only the revision, whose
    /// lane counts are each read atomically.
    pub async fn status(&self) -> Status {
        let commit = self.gate.read().await;
        let (bytes, policy, dirty) = if self.enabled() {
            match commit.wal.as_ref() {
                Some(wal) => (
                    wal.bytes(),
                    match wal.policy() {
                        Durability::Strict => "strict",
                        Durability::Everysec => "everysec",
                    },
                    wal.is_dirty(),
                ),
                None => (0, "none", false),
            }
        } else {
            (0, "none", false)
        };
        let last_sync = self.last_sync_ms.load(Ordering::Acquire);
        Status {
            bytes,
            policy,
            dirty,
            last_sync_ms: (last_sync != 0).then_some(last_sync as i64),
            revision: self.revision(),
        }
    }
    pub async fn publish(&self, metrics: &crate::metrics::Metrics) {
        let status = self.status().await;
        metrics.set_wal(
            status.bytes,
            status.policy,
            status.dirty,
            status.last_sync_ms,
        );
    }
    pub async fn checkpoint(
        &self,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<crate::wal::Checkpoint> {
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        let wal = exclusive.wal().as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint requires --data-dir",
            )
        })?;
        match wal.checkpoint(batches).await {
            Ok(result) => {
                self.note_sync();
                self.checkpoint_bytes
                    .store(result.new_bytes, Ordering::Release);
                Ok(result)
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }
    /// Install an authoritative image while the caller holds the pool pause.
    /// Both exports borrow the staged image; no full-image buffer is retained.
    pub async fn checkpoint_replacement<'a>(
        &self,
        image: &'a Engine,
        previous_databases: impl Iterator<Item = u32> + 'a,
    ) -> io::Result<()> {
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        if self.enabled() {
            let wal = exclusive
                .wal()
                .as_mut()
                .ok_or_else(|| io::Error::other("enabled journal has no WAL"))?;
            match wal.checkpoint(image.snapshot_batches(0)).await {
                Ok(result) => {
                    self.note_sync();
                    self.checkpoint_bytes
                        .store(result.new_bytes, Ordering::Release);
                }
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error);
                }
            }
        }
        if let Some(backup) = self.backup.get() {
            backup
                .record_replacement(replacement_batches(image, previous_databases))
                .await;
        }
        if let Some((_, offset)) = image.native_position() {
            self.published_offset.store(offset, Ordering::Release);
            self.set_fsynced(self.enabled().then_some(offset));
        }
        Ok(())
    }
    /// Applies one AOF configuration change. `None` keeps that setting as the
    /// exclusive writer finds it: ordinary AOF stays on or off, and the policy
    /// stays the WAL's, which keeps its configured value while AOF is off. So
    /// one call applies every setting of a change atomically, and settings
    /// changed while AOF was off apply when it is enabled again.
    pub async fn configure(
        &self,
        enabled: Option<bool>,
        policy: Option<Durability>,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<()> {
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        let wal = exclusive.wal().as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistence configuration requires --data-dir",
            )
        })?;
        let enabled = enabled.unwrap_or_else(|| self.enabled());
        let policy = policy.unwrap_or_else(|| wal.policy());
        if enabled && !self.enabled() {
            wal.checkpoint(batches).await?;
        }
        wal.finish().await?;
        wal.set_policy(policy);
        self.enabled.store(enabled, Ordering::Release);
        if let Some(native) = self.native.get() {
            native.set_aof_enabled(enabled);
        }
        self.note_sync();
        Ok(())
    }
    pub async fn needs_checkpoint(&self, threshold: u64) -> bool {
        self.enabled()
            && threshold != 0
            && self
                .status()
                .await
                .bytes
                .saturating_sub(self.checkpoint_bytes.load(Ordering::Acquire))
                >= threshold
    }
    pub async fn backup(
        &self,
        name: &str,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<(PathBuf, crate::backup::Backup)> {
        let mut exclusive = self.exclusive().await;
        self.ensure_healthy()?;
        let wal = exclusive.wal().as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "backup requires --data-dir")
        })?;
        let root = wal.directory().join("backups");
        std::fs::create_dir_all(&root)?;
        let path = root.join(name);
        let result = wal.backup(&path, batches).await?;
        Ok((std::fs::canonicalize(path)?, result))
    }
}

fn replacement_batches<'a>(
    image: &'a Engine,
    previous_databases: impl Iterator<Item = u32> + 'a,
) -> impl Iterator<Item = io::Result<Batch>> + 'a {
    let clears = previous_databases.map(|database| {
        Ok(Batch {
            timestamp_ms: 0,
            mutations: vec![Mutation::SelectDatabase { database }, Mutation::Clear],
        })
    });
    let reset = std::iter::once(Ok(Batch {
        timestamp_ms: 0,
        mutations: vec![Mutation::ResetGlobals],
    }));
    clears.chain(reset).chain(image.snapshot_batches(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::session::ClientSession;
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::current_dir().unwrap().join(format!(
                ".ffkv-native-durability-{:032x}",
                rand::random::<u128>()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn image(database: u32, key: &'static [u8], value: &'static [u8]) -> Engine {
        let mut engine = Engine::new();
        engine
            .replay(Batch {
                timestamp_ms: 0,
                mutations: vec![
                    Mutation::SelectDatabase { database },
                    Mutation::Set {
                        key: Bytes::from_static(key),
                        value: Bytes::from_static(value),
                        expires_at: None,
                    },
                ],
            })
            .unwrap();
        engine
    }
    fn get(engine: &mut Engine, database: u32, key: &'static [u8]) -> Option<Bytes> {
        engine.switch_database(database);
        match engine
            .execute(&mut crate::engine::BoundCommand::bind(&[
                Bytes::from_static(b"GET"),
                Bytes::from_static(key),
            ]))
            .unwrap()
        {
            crate::engine::Execution::Reply(crate::protocol::Resp::Bulk(value)) => value,
            _ => panic!("GET did not return a bulk value"),
        }
    }
    fn native(
        directory: &Directory,
        lazy_history: bool,
    ) -> Arc<crate::native_replication::NativeReplication> {
        let state = Arc::new(crate::state::State::new(0, 1, false));
        let config = crate::native_replication::Config {
            backlog_bytes: 4096,
            max_snapshot_bytes: 1024 * 1024,
            staging_directory: directory.0.join("native"),
            listening_port: 0,
            announce_ip: None,
            io_timeout: std::time::Duration::from_secs(1),
            reconnect_delay: std::time::Duration::from_millis(10),
            lazy_history,
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
    /// No native stream (`None`), an eager history, or lazy history that
    /// never activated. The eager one publishes every commit; the others
    /// leave each commit at the unpublished position.
    const STREAMS: [Option<bool>; 3] = [None, Some(false), Some(true)];
    fn configure_stream(
        persistence: &Persistence,
        directory: &Directory,
        lazy_history: Option<bool>,
    ) -> Option<Arc<crate::native_replication::NativeReplication>> {
        let stream = native(directory, lazy_history?);
        persistence.set_native(stream.clone());
        Some(stream)
    }
    fn recording(session: &Arc<ClientSession>) -> Engine {
        let mut engine = Engine::new();
        engine.set_recording(true);
        engine.set_command_session(Some(session.clone()));
        engine
    }
    fn set(engine: &mut Engine, value: &'static [u8]) {
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&[
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"key"),
                Bytes::from_static(value),
            ]))
            .unwrap();
    }
    fn watermark(session: &ClientSession) -> u64 {
        session.last_write_offset.load(Ordering::Acquire)
    }
    /// Read directly: `status()` would queue behind a pending writer.
    fn revision(persistence: &Persistence) -> u64 {
        persistence.revision()
    }
    fn settled(result: Result<io::Result<()>, Staged>) -> io::Result<()> {
        match result {
            Ok(result) => result,
            Err(_) => panic!("an eligible memory commit was refused"),
        }
    }
    fn refused(result: Result<io::Result<()>, Staged>) -> Staged {
        match result {
            Ok(_) => panic!("a commit settled on a path requiring the async commit"),
            Err(staged) => staged,
        }
    }
    /// `try_commit_now` on a fresh OS thread, through that thread's lane.
    fn try_commit_elsewhere(
        persistence: &Persistence,
        staged: Staged,
    ) -> Result<io::Result<()>, Staged> {
        std::thread::scope(|scope| {
            scope
                .spawn(move || persistence.try_commit_now(staged))
                .join()
                .unwrap()
        })
    }
    async fn wal_bytes(persistence: &Persistence) -> u64 {
        persistence.gate.read().await.wal.as_ref().unwrap().bytes()
    }
    fn native_end(after: u64, staged: &Staged) -> u64 {
        after
            + crate::native_replication::encode_batch(staged.batch.as_ref().unwrap())
                .unwrap()
                .len() as u64
    }
    #[tokio::test]
    async fn memory_and_async_commits_share_one_native_order_and_session_watermark() {
        let directory = Directory::new();
        let native = native(&directory, false);
        let persistence = Persistence::new(None);
        persistence.set_native(native.clone());
        assert!(persistence.commits_in_memory());
        let session = Arc::new(ClientSession::new(7));
        let mut engine = recording(&session);
        set(&mut engine, b"one");
        let staged = persistence.stage(&mut engine);
        let one = native_end(0, &staged);
        settled(persistence.try_commit_now(staged)).unwrap();
        assert_eq!(persistence.native_offset(), one);
        assert_eq!(native.status().offset, one);
        assert_eq!(watermark(&session), one);
        set(&mut engine, b"two");
        let staged = persistence.stage(&mut engine);
        let two = native_end(one, &staged);
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(persistence.native_offset(), two);
        assert_eq!(watermark(&session), two);
        set(&mut engine, b"three");
        let staged = persistence.stage(&mut engine);
        let three = native_end(two, &staged);
        settled(persistence.try_commit_now(staged)).unwrap();
        assert_eq!(persistence.native_offset(), three);
        assert_eq!(native.status().offset, three);
        assert_eq!(watermark(&session), three);
        assert_eq!(revision(&persistence), 3);
        assert_eq!(persistence.last_fsynced_native_offset(), Some(0));
    }
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_memory_commits_publish_one_order_matching_every_session_offset() {
        const SESSIONS: u64 = 4;
        const COMMITS: usize = 64;
        let directory = Directory::new();
        let native = native(&directory, false);
        let persistence = Arc::new(Persistence::new(None));
        persistence.set_native(native.clone());
        let running = Arc::new(AtomicBool::new(true));
        let observer = tokio::spawn({
            let (persistence, running) = (persistence.clone(), running.clone());
            async move {
                let mut seen = (0, 0);
                while running.load(Ordering::Acquire) {
                    let next = (persistence.native_offset(), revision(&persistence));
                    assert!(
                        next.0 >= seen.0 && next.1 >= seen.1,
                        "publication regressed"
                    );
                    seen = next;
                    tokio::task::yield_now().await;
                }
            }
        });
        let committers: Vec<_> = (0..SESSIONS)
            .map(|id| {
                let persistence = persistence.clone();
                tokio::spawn(async move {
                    let session = Arc::new(ClientSession::new(id));
                    let mut engine = recording(&session);
                    let mut published = Vec::with_capacity(COMMITS);
                    for sequence in 0..COMMITS {
                        // Unique record lengths expose an offset reported to the wrong commit.
                        let value = Bytes::from(vec![b'v'; 1 + id as usize * COMMITS + sequence]);
                        engine
                            .execute(&mut crate::engine::BoundCommand::bind(&[
                                Bytes::from_static(b"SET"),
                                Bytes::from_static(b"key"),
                                value,
                            ]))
                            .unwrap();
                        let staged = persistence.stage(&mut engine);
                        let length = native_end(0, &staged);
                        if sequence % 2 == 0 {
                            settled(persistence.try_commit_now(staged)).unwrap();
                        } else {
                            persistence.commit_staged(staged).await.unwrap();
                        }
                        published.push((watermark(&session), length));
                        tokio::task::yield_now().await;
                    }
                    published
                })
            })
            .collect();
        let mut published = Vec::new();
        for committer in committers {
            let session = committer.await.unwrap();
            assert!(
                session.windows(2).all(|pair| pair[0].0 < pair[1].0),
                "a session's offsets regressed"
            );
            published.extend(session);
        }
        running.store(false, Ordering::Release);
        observer.await.unwrap();
        published.sort_unstable();
        let mut end = 0;
        for (offset, length) in published {
            end += length;
            assert_eq!(offset, end, "each offset ends its own commit's record");
        }
        assert_eq!(native.status().offset, end);
        assert_eq!(persistence.native_offset(), end);
        assert_eq!(revision(&persistence), SESSIONS * COMMITS as u64);
    }
    #[tokio::test]
    async fn memory_commits_share_the_gate_but_cannot_bypass_a_queued_aof_cutover() {
        use futures_util::FutureExt;
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        let session = Arc::new(ClientSession::new(13));
        let mut engine = recording(&session);
        {
            // Stands in for an async memory commit in flight.
            let in_flight = persistence.gate.read().await;
            set(&mut engine, b"beside");
            persistence
                .commit_staged(persistence.stage(&mut engine))
                .now_or_never()
                .expect("an async memory commit queued behind another memory commit")
                .unwrap();
            assert_eq!(revision(&persistence), 1);
            set(&mut engine, b"durable");
            let durable = persistence.stage(&mut engine);
            let enable = persistence.configure(Some(true), None, std::iter::empty());
            let mut enable = std::pin::pin!(enable);
            assert!(enable.as_mut().now_or_never().is_none());
            // Once the cutover is queued, no later commit settles in memory.
            let durable = refused(persistence.try_commit_now(durable));
            let mut queued = std::pin::pin!(persistence.commit_staged(durable));
            assert!(queued.as_mut().now_or_never().is_none());
            assert_eq!(revision(&persistence), 1);
            drop(in_flight);
            enable.await.unwrap();
            assert!(!persistence.commits_in_memory());
            queued.await.unwrap();
            assert_eq!(revision(&persistence), 2);
        }
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"durable"))
        );
    }
    /// An inline memory commit in flight on this thread's lane holds off a
    /// queued AOF cutover, which must close every lane: meanwhile no thread
    /// settles a commit inline, and an async commit queues behind the
    /// cutover, so it becomes durable.
    #[tokio::test]
    async fn a_commit_in_flight_on_a_lane_holds_off_a_cutover_that_later_commits_follow() {
        use futures_util::FutureExt;
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        let session = Arc::new(ClientSession::new(17));
        let mut engine = recording(&session);
        {
            // Stands in for an inline memory commit in flight on this thread.
            let in_flight = persistence
                .try_share()
                .expect("an idle gate refused a share");
            set(&mut engine, b"beside");
            settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
            assert_eq!(revision(&persistence), 1);
            set(&mut engine, b"durable");
            let durable = persistence.stage(&mut engine);
            let enable = persistence.configure(Some(true), None, std::iter::empty());
            let mut enable = std::pin::pin!(enable);
            assert!(enable.as_mut().now_or_never().is_none());
            let durable = refused(persistence.try_commit_now(durable));
            let durable = refused(try_commit_elsewhere(&persistence, durable));
            let mut queued = std::pin::pin!(persistence.commit_staged(durable));
            assert!(queued.as_mut().now_or_never().is_none());
            assert_eq!(revision(&persistence), 1);
            drop(in_flight);
            enable.await.unwrap();
            assert!(!persistence.commits_in_memory());
            queued.await.unwrap();
            assert_eq!(revision(&persistence), 2);
        }
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"durable"))
        );
    }
    /// A writer cancelled while it closes the lanes behind a commit in flight
    /// releases everything having changed nothing: the commit queued behind
    /// it settles in memory, inline commits settle on every thread, nothing
    /// is journaled, and a later writer closes every lane.
    #[tokio::test]
    async fn a_writer_cancelled_mid_drain_releases_the_gate_unchanged() {
        use futures_util::FutureExt;
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        let disabled = wal_bytes(&persistence).await;
        let session = Arc::new(ClientSession::new(18));
        let mut engine = recording(&session);
        let in_flight = persistence
            .try_share()
            .expect("an idle gate refused a share");
        let enable = persistence.configure(Some(true), None, std::iter::empty());
        let mut enable = Box::pin(enable);
        assert!(enable.as_mut().now_or_never().is_none());
        set(&mut engine, b"queued");
        let queued = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        let mut queued = Box::pin(persistence.commit_staged(queued));
        assert!(queued.as_mut().now_or_never().is_none());
        drop(enable);
        queued
            .as_mut()
            .now_or_never()
            .expect("a cancelled writer kept the commit gate")
            .unwrap();
        drop(in_flight);
        assert!(persistence.commits_in_memory());
        set(&mut engine, b"inline");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        set(&mut engine, b"elsewhere");
        let staged = persistence.stage(&mut engine);
        settled(try_commit_elsewhere(&persistence, staged)).unwrap();
        assert_eq!(revision(&persistence), 3);
        assert_eq!(wal_bytes(&persistence).await, disabled);
        tokio::time::timeout(std::time::Duration::from_secs(10), persistence.finish())
            .await
            .expect("a cancelled writer left a lane closed")
            .unwrap();
        set(&mut engine, b"after");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(revision(&persistence), 4);
        assert_eq!(wal_bytes(&persistence).await, disabled);
    }
    #[tokio::test]
    async fn a_publication_that_panicked_fails_persistence_closed() {
        for lazy_history in STREAMS {
            let directory = Directory::new();
            let persistence = Persistence::new(None);
            let stream = configure_stream(&persistence, &directory, lazy_history);
            let session = Arc::new(ClientSession::new(14));
            let mut engine = recording(&session);
            // A tail that unwound may already have appended to the native stream.
            let torn = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _order = persistence.publication.lock().unwrap();
                panic!("publication unwound");
            }));
            assert!(torn.is_err());
            // Even a commit with nothing to publish must not settle past it.
            set(&mut engine, b"inline");
            assert!(settled(persistence.try_commit_now(persistence.stage(&mut engine))).is_err());
            assert!(persistence.ensure_healthy().is_err());
            set(&mut engine, b"async");
            assert!(
                persistence
                    .commit_staged(persistence.stage(&mut engine))
                    .await
                    .is_err()
            );
            let publish = [
                Bytes::from_static(b"PUBLISH"),
                Bytes::from_static(b"events"),
                Bytes::from_static(b"payload"),
            ];
            assert!(persistence.publish_ephemeral(&publish).await.is_err());
            assert_eq!(persistence.native_offset(), 0);
            assert!(stream.is_none_or(|stream| stream.status().offset == 0));
            assert_eq!(watermark(&session), 0);
            assert_eq!(revision(&persistence), 0);
        }
    }

    #[tokio::test]
    async fn a_poisoned_publication_cannot_be_bypassed_by_a_durable_commit() {
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        let before = persistence.status().await.bytes;
        let shared = Shared::global(persistence.gate.read().await);
        let torn = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            persistence.in_publication_order::<()>(&shared, || {
                panic!("publication unwound before the next durable write");
            })
        }));
        drop(shared);
        assert!(torn.is_err());

        let session = Arc::new(ClientSession::new(15));
        let mut engine = recording(&session);
        set(&mut engine, b"must-not-be-journaled");
        let result = persistence
            .commit_staged(persistence.stage(&mut engine))
            .await;
        assert!(result.is_err(), "a WAL writer bypassed publication poison");
        assert_eq!(persistence.status().await.bytes, before);
        assert_eq!(revision(&persistence), 0);
        assert_eq!(watermark(&session), 0);
    }
    #[tokio::test]
    async fn ordinary_aof_refuses_memory_commit_and_configure_switches_eligibility() {
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        assert!(!persistence.commits_in_memory());
        let session = Arc::new(ClientSession::new(9));
        let mut engine = recording(&session);
        let empty = wal_bytes(&persistence).await;
        set(&mut engine, b"durable");
        let staged = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        assert_eq!(wal_bytes(&persistence).await, empty);
        assert_eq!(revision(&persistence), 0);
        persistence.commit_staged(staged).await.unwrap();
        assert!(wal_bytes(&persistence).await > empty);
        assert_eq!(revision(&persistence), 1);
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        assert!(persistence.commits_in_memory());
        let disabled = wal_bytes(&persistence).await;
        set(&mut engine, b"memory");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(wal_bytes(&persistence).await, disabled);
        assert_eq!(revision(&persistence), 2);
        // The async path settles a memory-eligible commit in memory too.
        set(&mut engine, b"async");
        persistence
            .commit_staged(persistence.stage(&mut engine))
            .await
            .unwrap();
        assert_eq!(wal_bytes(&persistence).await, disabled);
        assert_eq!(revision(&persistence), 3);
        persistence
            .configure(Some(true), None, engine.snapshot_batches(Engine::now()))
            .await
            .unwrap();
        assert!(!persistence.commits_in_memory());
        set(&mut engine, b"again");
        let staged = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(revision(&persistence), 4);
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"again"))
        );
    }
    /// Ordinary AOF refuses inline commits on every thread across its
    /// writers; disabling it admits them on every thread unjournaled, and
    /// enabling it again refuses them everywhere and journals each commit.
    #[tokio::test]
    async fn aof_transitions_switch_inline_commits_on_every_thread() {
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        let session = Arc::new(ClientSession::new(20));
        let mut engine = recording(&session);
        set(&mut engine, b"first");
        let staged = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        persistence.commit_staged(staged).await.unwrap();
        persistence.periodic_sync().await.unwrap();
        set(&mut engine, b"second");
        let staged = persistence.stage(&mut engine);
        let staged = refused(try_commit_elsewhere(&persistence, staged));
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(revision(&persistence), 2);
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        let disabled = wal_bytes(&persistence).await;
        set(&mut engine, b"elsewhere");
        let staged = persistence.stage(&mut engine);
        settled(try_commit_elsewhere(&persistence, staged)).unwrap();
        set(&mut engine, b"inline");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(wal_bytes(&persistence).await, disabled);
        assert_eq!(revision(&persistence), 4);
        persistence
            .configure(Some(true), None, engine.snapshot_batches(Engine::now()))
            .await
            .unwrap();
        set(&mut engine, b"durable");
        let staged = persistence.stage(&mut engine);
        let staged = refused(try_commit_elsewhere(&persistence, staged));
        let staged = refused(persistence.try_commit_now(staged));
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(revision(&persistence), 5);
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"durable"))
        );
    }
    /// The AOF policy is the WAL's own setting and outlives ordinary AOF being
    /// off: enabling AOF again without naming a policy restores the last one
    /// configured, including one changed while AOF was off, and durable
    /// commits follow it.
    #[tokio::test]
    async fn aof_policy_survives_disabling_including_changes_while_disabled() {
        let directory = Directory::new();
        let (wal, _) = Wal::open(&directory.0, Durability::Strict, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        let session = Arc::new(ClientSession::new(21));
        let mut engine = recording(&session);
        persistence
            .configure(None, Some(Durability::Everysec), std::iter::empty())
            .await
            .unwrap();
        assert!(persistence.enabled());
        assert_eq!(persistence.status().await.policy, "everysec");
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        assert_eq!(persistence.status().await.policy, "none");
        persistence
            .configure(Some(true), None, engine.snapshot_batches(Engine::now()))
            .await
            .unwrap();
        assert_eq!(persistence.status().await.policy, "everysec");
        set(&mut engine, b"deferred");
        persistence
            .commit_staged(persistence.stage(&mut engine))
            .await
            .unwrap();
        assert!(persistence.status().await.dirty, "everysec synced inline");
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        persistence
            .configure(None, Some(Durability::Strict), std::iter::empty())
            .await
            .unwrap();
        assert!(!persistence.enabled());
        assert_eq!(persistence.status().await.policy, "none");
        persistence
            .configure(Some(true), None, engine.snapshot_batches(Engine::now()))
            .await
            .unwrap();
        assert_eq!(persistence.status().await.policy, "strict");
        set(&mut engine, b"synced");
        persistence
            .commit_staged(persistence.stage(&mut engine))
            .await
            .unwrap();
        assert!(
            !persistence.status().await.dirty,
            "strict left a commit unsynced"
        );
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"synced"))
        );
    }
    #[tokio::test]
    async fn the_commit_gate_not_staging_decides_native_publication_and_aof() {
        let journal = Directory::new();
        let stream = Directory::new();
        let (wal, _) = Wal::open(&journal.0, Durability::Everysec, |_| Ok(())).unwrap();
        let persistence = Persistence::new(Some(wal));
        persistence
            .configure(Some(false), None, std::iter::empty())
            .await
            .unwrap();
        let session = Arc::new(ClientSession::new(12));
        let mut engine = recording(&session);
        // Staged before any native stream existed.
        set(&mut engine, b"unstreamed");
        let unstreamed = persistence.stage(&mut engine);
        let first = native_end(0, &unstreamed);
        let native = native(&stream, false);
        persistence.set_native(native.clone());
        settled(persistence.try_commit_now(unstreamed)).unwrap();
        assert_eq!(persistence.native_offset(), first);
        assert_eq!(native.status().offset, first);
        assert_eq!(watermark(&session), first);
        // Staged while a memory commit was eligible; AOF turns on before commit.
        set(&mut engine, b"durable");
        let staged = persistence.stage(&mut engine);
        let second = native_end(first, &staged);
        persistence
            .configure(Some(true), None, std::iter::empty())
            .await
            .unwrap();
        let staged = refused(persistence.try_commit_now(staged));
        assert_eq!(native.status().offset, first);
        assert_eq!(revision(&persistence), 1);
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(persistence.native_offset(), second);
        assert_eq!(native.status().offset, second);
        assert_eq!(watermark(&session), second);
        assert_eq!(revision(&persistence), 2);
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&journal.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(
            get(&mut recovered, 0, b"key"),
            Some(Bytes::from_static(b"durable"))
        );
    }
    #[tokio::test]
    async fn capturing_online_backup_refuses_memory_commit_until_it_detaches() {
        let directory = Directory::new();
        let persistence = Persistence::new(None);
        let backup = crate::server_runtime::OnlineBackup::new(directory.0.join("backups"), 0);
        persistence.set_backup(backup.clone());
        assert!(persistence.commits_in_memory());
        let session = Arc::new(ClientSession::new(10));
        let mut engine = recording(&session);
        backup.begin().unwrap();
        assert!(!persistence.commits_in_memory());
        set(&mut engine, b"captured");
        let staged = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        assert_eq!(revision(&persistence), 0);
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(revision(&persistence), 1);
        backup.abort().await.unwrap();
        assert!(persistence.commits_in_memory());
        set(&mut engine, b"memory");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(revision(&persistence), 2);
    }
    /// Captured commits keep inline commits off every thread. Once the backup
    /// detaches outside the commit gate, even with a writer queued meanwhile
    /// and then cancelled, inline commits settle on every thread again, and
    /// a commit refused behind that writer settles once it is gone.
    #[tokio::test]
    async fn inline_commits_return_on_every_thread_once_a_backup_detaches() {
        use futures_util::FutureExt;
        let directory = Directory::new();
        let persistence = Persistence::new(None);
        let backup = crate::server_runtime::OnlineBackup::new(directory.0.join("backups"), 0);
        persistence.set_backup(backup.clone());
        let session = Arc::new(ClientSession::new(19));
        let mut engine = recording(&session);
        backup.begin().unwrap();
        set(&mut engine, b"captured");
        let staged = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
        persistence.commit_staged(staged).await.unwrap();
        set(&mut engine, b"captured-elsewhere");
        let staged = persistence.stage(&mut engine);
        let staged = refused(try_commit_elsewhere(&persistence, staged));
        persistence.commit_staged(staged).await.unwrap();
        assert_eq!(revision(&persistence), 2);
        set(&mut engine, b"behind");
        let behind = {
            // Stands in for an async commit in flight, keeping a writer queued.
            let in_flight = persistence.gate.read().await;
            let mut finish = Box::pin(persistence.finish());
            assert!(finish.as_mut().now_or_never().is_none());
            backup.abort().await.unwrap();
            assert!(persistence.commits_in_memory());
            // The queued writer still precedes every later commit.
            let behind = refused(persistence.try_commit_now(persistence.stage(&mut engine)));
            let behind = refused(try_commit_elsewhere(&persistence, behind));
            drop(finish);
            drop(in_flight);
            behind
        };
        assert_eq!(revision(&persistence), 2);
        settled(persistence.try_commit_now(behind)).unwrap();
        set(&mut engine, b"elsewhere");
        let staged = persistence.stage(&mut engine);
        settled(try_commit_elsewhere(&persistence, staged)).unwrap();
        set(&mut engine, b"inline");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(revision(&persistence), 5);
    }
    #[tokio::test]
    async fn memory_commit_failure_is_settled_fatal_and_poisons_later_commits() {
        for lazy_history in STREAMS {
            let directory = Directory::new();
            let persistence = Persistence::new(None);
            let stream = configure_stream(&persistence, &directory, lazy_history);
            // This thread's commits count on its lane, which is now exhausted.
            persistence.revisions[thread_lane() & (persistence.revisions.len() - 1)]
                .0
                .store(u64::MAX, Ordering::Release);
            let session = Arc::new(ClientSession::new(11));
            let mut engine = recording(&session);
            set(&mut engine, b"exhausted");
            assert!(settled(persistence.try_commit_now(persistence.stage(&mut engine))).is_err());
            assert!(persistence.ensure_healthy().is_err());
            assert_eq!(watermark(&session), 0, "a failed commit never acknowledges");
            let poisoned = persistence.native_offset();
            set(&mut engine, b"after");
            assert!(settled(persistence.try_commit_now(persistence.stage(&mut engine))).is_err());
            set(&mut engine, b"queued");
            assert!(
                persistence
                    .commit_staged(persistence.stage(&mut engine))
                    .await
                    .is_err()
            );
            assert_eq!(persistence.native_offset(), poisoned);
            assert!(stream.is_none_or(|stream| stream.status().offset == poisoned));
            assert_eq!(watermark(&session), 0);
            assert_eq!(revision(&persistence), u64::MAX);
            // As `commit(engine)` with nothing captured: no batch, no failure.
            settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        }
    }
    /// Lazy history that never activated gives a memory commit nothing to
    /// publish, yet observers enabled beside it see every commit: a WATCH
    /// token changes and a tracked read is invalidated, inline and async, with
    /// exact revisions and the unpublished session position throughout.
    #[tokio::test]
    async fn observers_enabled_before_history_activates_see_every_memory_commit() {
        use crate::protocol::{Resp, Version};
        let directory = Directory::new();
        let stream = native(&directory, true);
        let persistence = Persistence::new(None);
        persistence.set_native(stream.clone());
        let hub = Arc::new(crate::pubsub::Hub::default());
        let clients = Arc::new(crate::client_registry::Registry::new(hub.clone()));
        persistence.set_clients(clients.clone());
        let session = Arc::new(ClientSession::new(16));
        let mut engine = recording(&session);
        engine.set_watches(persistence.watches.clone());
        let key = Bytes::from_static(b"key");
        set(&mut engine, b"unobserved");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert_eq!(revision(&persistence), 1);

        let token = persistence.watches.register(0, key.clone(), None);
        set(&mut engine, b"watched");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert!(
            token.changed(Engine::now()),
            "a WATCH missed a memory commit"
        );
        assert_eq!(revision(&persistence), 2);
        drop(token);
        persistence.watches.reap();

        let mut pushes = hub.attach(1);
        let cache = clients.register(1, Bytes::new());
        cache.set_version(Version::Resp3);
        let tracking = |mode: &'static str| {
            let args = [
                Bytes::from_static(b"CLIENT"),
                Bytes::from_static(b"TRACKING"),
                Bytes::from_static(mode.as_bytes()),
            ];
            assert_eq!(clients.command(&cache, &args), Resp::Simple("OK"));
        };
        let invalidated = vec![
            Resp::Bulk(Some(Bytes::from_static(b"invalidate"))),
            Resp::Array(Some(vec![Resp::Bulk(Some(key.clone()))])),
        ];
        tracking("ON");
        clients.read(1, 0, std::slice::from_ref(&key));
        set(&mut engine, b"tracked");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        let notice = pushes
            .try_recv()
            .expect("an inline commit missed a tracked read");
        assert_eq!(notice.values, invalidated);
        assert_eq!(revision(&persistence), 3);
        clients.read(1, 0, std::slice::from_ref(&key));
        set(&mut engine, b"queued");
        persistence
            .commit_staged(persistence.stage(&mut engine))
            .await
            .unwrap();
        let notice = pushes
            .try_recv()
            .expect("an async commit missed a tracked read");
        assert_eq!(notice.values, invalidated);
        assert_eq!(revision(&persistence), 4);

        tracking("OFF");
        set(&mut engine, b"untracked");
        settled(persistence.try_commit_now(persistence.stage(&mut engine))).unwrap();
        assert!(pushes.try_recv().is_none());
        assert_eq!(revision(&persistence), 5);
        assert!(!stream.history_active());
        assert_eq!(stream.status().offset, 0);
        assert_eq!(persistence.native_offset(), 0);
        assert_eq!(watermark(&session), 0);
    }
    #[test]
    fn replacement_delta_removes_every_previous_database_and_global_metadata() {
        let mut restored = image(0, b"old-zero", b"zero");
        restored
            .replay(Batch {
                timestamp_ms: 0,
                mutations: vec![
                    Mutation::SelectDatabase { database: 31 },
                    Mutation::Set {
                        key: Bytes::from_static(b"old-seven"),
                        value: Bytes::from_static(b"seven"),
                        expires_at: None,
                    },
                    Mutation::Functions {
                        snapshot: Bytes::from_static(b"obsolete-library"),
                    },
                    Mutation::NativePosition {
                        history: Bytes::from("a".repeat(40)),
                        offset: 91,
                    },
                    Mutation::RdbAuxiliary {
                        key: Bytes::from_static(b"obsolete-aux"),
                        value: Bytes::from_static(b"value"),
                    },
                ],
            })
            .unwrap();
        let replacement = image(2, b"new", b"image");
        let previous = (0..restored.database_count())
            .chain(restored.database_ids())
            .collect::<std::collections::BTreeSet<_>>();
        for batch in replacement_batches(&replacement, previous.into_iter()) {
            restored.replay(batch.unwrap()).unwrap();
        }
        assert_eq!(get(&mut restored, 0, b"old-zero"), None);
        assert_eq!(get(&mut restored, 31, b"old-seven"), None);
        assert_eq!(
            get(&mut restored, 2, b"new"),
            Some(Bytes::from_static(b"image"))
        );
        assert!(restored.function_snapshot().is_empty());
        assert!(restored.native_position().is_none());
        assert!(restored.rdb_auxiliary().next().is_none());
    }
    #[tokio::test]
    async fn replacement_checkpoint_survives_recovery_without_old_database_keys() {
        let directory = Directory::new();
        let old = image(31, b"old", b"discard");
        let (mut wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        for batch in old.snapshot_batches(0) {
            wal.append(&batch.unwrap()).await.unwrap();
        }
        let persistence = Persistence::new(Some(wal));
        let mut replacement = image(2, b"new", b"durable");
        replacement.set_native_position(Bytes::from("b".repeat(40)), 712);
        persistence
            .checkpoint_replacement(&replacement, old.database_ids())
            .await
            .unwrap();
        assert_eq!(persistence.last_fsynced_native_offset(), Some(712));
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(get(&mut recovered, 31, b"old"), None);
        assert_eq!(
            get(&mut recovered, 2, b"new"),
            Some(Bytes::from_static(b"durable"))
        );
        assert_eq!(
            recovered.native_position(),
            Some((Bytes::from("b".repeat(40)), 712))
        );
    }
}
