//! Coordinator-backed administration and native BASE + INCR online backups.
//! BASE borrows paused engines; INCR is recorded in the persistence commit lock,
//! independently of ordinary AOF. Only a sealed, detached pair is restorable.
use crate::{
    engine::Engine,
    journal::Batch,
    persistence::Persistence,
    protocol::Resp,
    request::Request,
    server_commands::{
        BackupAction, CommandContext, ConfigKind, ConfigParameter, ConfigSnapshot, HookFuture,
        PersistenceStats, ServerAction, ServerHooks,
    },
    sharding::{Paused, Pool},
    state::State,
    wal::{Durability, Wal},
};
use bytes::Bytes;
use std::future::Future;
use std::{
    collections::BTreeMap,
    fs, io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{oneshot, watch};

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.into())
}
fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
fn array(values: Vec<Resp>) -> Resp {
    Resp::Array(Some(values))
}
fn ok() -> Resp {
    Resp::Simple("OK")
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Idle,
    Pending,
    Snapshotting,
    Incrementing,
    Sealed,
    Failed,
}
impl Phase {
    fn name(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Pending => "pending",
            Self::Snapshotting => "snapshotting",
            Self::Incrementing => "incrementing",
            Self::Sealed => "sealed",
            Self::Failed => "failed",
        }
    }
}
struct BackupStatus {
    phase: Phase,
    error: String,
    start: u64,
    end: u64,
    directory: Option<PathBuf>,
    base_ready: bool,
    busy: bool,
    generation: u64,
}
struct Capture {
    wal: Option<Wal>,
    generation: u64,
}
pub struct OnlineBackup {
    root: PathBuf,
    status: Mutex<BackupStatus>,
    capture: tokio::sync::Mutex<Capture>,
    active: AtomicBool,
    ttl: AtomicU64,
    timers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl OnlineBackup {
    /// The root may contain unrelated files. Each START exclusively creates its
    /// own child, and cleanup removes only the fixed names created in that child.
    pub fn new(root: PathBuf, sealed_ttl: u64) -> Arc<Self> {
        Arc::new(Self {
            root,
            status: Mutex::new(BackupStatus {
                phase: Phase::Idle,
                error: String::new(),
                start: 0,
                end: 0,
                directory: None,
                base_ready: false,
                busy: false,
                generation: 0,
            }),
            capture: tokio::sync::Mutex::new(Capture {
                wal: None,
                generation: 0,
            }),
            active: AtomicBool::new(false),
            ttl: AtomicU64::new(sealed_ttl),
            timers: Mutex::new(Vec::new()),
        })
    }
    pub fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
    pub fn set_sealed_ttl(&self, seconds: u64) {
        self.ttl.store(seconds, Ordering::Release);
    }
    pub fn in_progress(&self) -> bool {
        matches!(
            self.status.lock().unwrap().phase,
            Phase::Pending | Phase::Snapshotting | Phase::Incrementing
        )
    }
    fn fail(&self, generation: u64, error: impl ToString) {
        let mut status = self.status.lock().unwrap();
        if status.generation == generation {
            status.phase = Phase::Failed;
            status.error = error.to_string();
            status.end = now();
            self.active.store(false, Ordering::Release);
        }
    }
    fn begin(&self) -> io::Result<u64> {
        let mut s = self.status.lock().unwrap();
        if s.phase != Phase::Idle {
            return Err(invalid("backup already exists; CLEANUP is required"));
        }
        s.generation = s
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("backup generation exhausted"))?;
        s.phase = Phase::Pending;
        s.error.clear();
        s.start = now();
        s.end = 0;
        s.base_ready = false;
        s.busy = true;
        // Record mutations while BASE is pending, even when ordinary WAL is off.
        // Their effects belong to BASE; capture itself is armed only at its end.
        self.active.store(true, Ordering::Release);
        Ok(s.generation)
    }
    pub fn status(&self) -> Resp {
        let s = self.status.lock().unwrap();
        Resp::Map(vec![
            (bulk("state"), bulk(s.phase.name())),
            (bulk("error"), bulk(s.error.clone())),
            (bulk("start_time"), bulk(s.start.to_string())),
            (bulk("end_time"), bulk(s.end.to_string())),
        ])
    }
    pub fn list(&self) -> Resp {
        let s = self.status.lock().unwrap();
        let Some(dir) = &s.directory else {
            return array(vec![]);
        };
        let mut files = vec![];
        if s.base_ready {
            files.push(bulk(dir.join("base.ffkv").to_string_lossy().into_owned()));
        }
        if s.phase == Phase::Sealed {
            for name in ["incremental.ffkv", "manifest.ffkv"] {
                files.push(bulk(dir.join(name).to_string_lossy().into_owned()));
            }
        }
        array(files)
    }
    /// REQUIRED: called once per resolved batch inside Persistence's common
    /// commit lock, including replicated batches and disabled ordinary AOF.
    /// Backup failure is terminal for this backup, not for the live database.
    pub async fn record_batch(&self, batch: &Batch) {
        if !self.active() {
            return;
        }
        let mut capture = self.capture.lock().await;
        let generation = capture.generation;
        if let Some(wal) = capture.wal.as_mut()
            && let Err(error) = wal.append(batch).await
        {
            capture.wal = None;
            self.fail(generation, error);
        }
    }
    /// Mirror an installed full image without cloning it. The caller holds the
    /// Pool pause and persistence commit mutex and has already durably committed
    /// the replacement. Supply SelectDatabase+Clear for every previous DB,
    /// followed by the complete new image, including global metadata resets.
    /// An export failure invalidates this backup rather than sealing a prefix.
    pub async fn record_replacement(&self, batches: impl Iterator<Item = io::Result<Batch>>) {
        if !self.active() {
            return;
        }
        let mut capture = self.capture.lock().await;
        let generation = capture.generation;
        let Some(wal) = capture.wal.as_mut() else {
            return;
        };
        for batch in batches {
            let result = match batch {
                Ok(batch) => wal.append(&batch).await,
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                capture.wal = None;
                self.fail(generation, error);
                return;
            }
        }
    }
    async fn snapshot(self: &Arc<Self>, pool: &Arc<Pool>, generation: u64) -> io::Result<()> {
        {
            let mut s = self.status.lock().unwrap();
            if s.generation != generation || s.phase != Phase::Pending {
                s.busy = false;
                return Ok(());
            }
            s.phase = Phase::Snapshotting;
        }
        let result = self.produce_base(pool, generation).await;
        if let Err(error) = &result {
            self.fail(generation, error);
        }
        self.status.lock().unwrap().busy = false;
        result
    }
    async fn produce_base(&self, pool: &Arc<Pool>, generation: u64) -> io::Result<()> {
        fs::create_dir_all(&self.root)?;
        let root = fs::canonicalize(&self.root)?;
        let directory = root.join(format!(
            "ffkv-backup-{}-{}-{:032x}",
            std::process::id(),
            generation,
            rand::random::<u128>()
        ));
        fs::create_dir(&directory)?;
        {
            let mut s = self.status.lock().unwrap();
            s.directory = Some(directory.clone());
        }
        let paused = pool.pause().await?;
        let result = async {
            let (mut base, _) =
                Wal::open(&directory.join("base-work"), Durability::Everysec, |_| {
                    Ok(())
                })?;
            let timestamp = Engine::now();
            for engine in &paused.engines {
                for batch in engine.snapshot_batches(timestamp) {
                    base.append(&batch?).await?;
                }
            }
            base.finish().await?;
            drop(base);
            fs::rename(
                directory.join("base-work/wal.ffkv"),
                directory.join("base.ffkv"),
            )?;
            fs::remove_file(directory.join("base-work/LOCK"))?;
            fs::remove_dir(directory.join("base-work"))?;
            let (incremental, _) =
                Wal::open(&directory.join("incr-work"), Durability::Everysec, |_| {
                    Ok(())
                })?;
            let mut capture = self.capture.lock().await;
            let mut s = self.status.lock().unwrap();
            if s.generation != generation || s.phase != Phase::Snapshotting {
                return Err(invalid("backup was aborted"));
            }
            capture.wal = Some(incremental);
            capture.generation = generation;
            s.base_ready = true;
            s.phase = Phase::Incrementing;
            Ok(())
        }
        .await;
        let resumed = paused.resume();
        result.and(resumed)
    }
    pub async fn seal(self: &Arc<Self>) -> io::Result<()> {
        let mut capture = self.capture.lock().await;
        let (directory, generation) = {
            let s = self.status.lock().unwrap();
            if s.phase != Phase::Incrementing {
                return Err(invalid("backup is not incrementing"));
            }
            (s.directory.clone().unwrap(), s.generation)
        };
        // Detach before publication while holding the same lock as record_batch.
        self.active.store(false, Ordering::Release);
        let result = async {
            let mut wal = capture
                .wal
                .take()
                .ok_or_else(|| invalid("backup incremental WAL unavailable"))?;
            wal.finish().await?;
            drop(wal);
            fs::rename(
                directory.join("incr-work/wal.ffkv"),
                directory.join("incremental.ffkv"),
            )?;
            fs::remove_file(directory.join("incr-work/LOCK"))?;
            fs::remove_dir(directory.join("incr-work"))?;
            crate::backup::seal_incremental(&directory)?;
            Ok(())
        }
        .await;
        if let Err(error) = &result {
            self.fail(generation, error);
            return result;
        }
        {
            let mut s = self.status.lock().unwrap();
            s.phase = Phase::Sealed;
            s.end = now();
        }
        let ttl = self.ttl.load(Ordering::Acquire);
        if ttl != 0 {
            let this = self.clone();
            let mut timers = self.timers.lock().unwrap();
            for timer in timers.iter() {
                timer.abort();
            }
            timers.retain(|timer| !timer.is_finished());
            timers.push(tokio::spawn(async move {
                tokio::time::sleep(Duration::from_secs(ttl)).await;
                let _ = this.cleanup_generation(Some(generation)).await;
            }));
        }
        Ok(())
    }
    pub async fn abort(&self) -> io::Result<()> {
        let mut capture = self.capture.lock().await;
        let generation = {
            let s = self.status.lock().unwrap();
            if !matches!(
                s.phase,
                Phase::Pending | Phase::Snapshotting | Phase::Incrementing
            ) {
                return Err(invalid("no active backup to abort"));
            }
            s.generation
        };
        self.active.store(false, Ordering::Release);
        capture.wal = None;
        self.fail(generation, "backup aborted");
        Ok(())
    }
    pub async fn cleanup(&self) -> io::Result<()> {
        self.cleanup_generation(None).await
    }
    async fn cleanup_generation(&self, expected: Option<u64>) -> io::Result<()> {
        let mut capture = self.capture.lock().await;
        let mut s = self.status.lock().unwrap();
        if expected.is_some_and(|g| g != s.generation) {
            return Ok(());
        }
        if s.phase == Phase::Idle {
            return Ok(());
        }
        if !matches!(s.phase, Phase::Sealed | Phase::Failed) || s.busy {
            return Err(invalid(
                "backup is active; ABORT and wait for snapshot termination first",
            ));
        }
        capture.wal = None;
        if let Some(dir) = &s.directory {
            remove_owned(dir)?;
        }
        s.phase = Phase::Idle;
        s.error.clear();
        s.directory = None;
        s.base_ready = false;
        s.start = 0;
        s.end = 0;
        Ok(())
    }
}
fn remove_owned(directory: &Path) -> io::Result<()> {
    for name in [
        "base.ffkv",
        "incremental.ffkv",
        "manifest.ffkv",
        "base-work/wal.ffkv",
        "base-work/LOCK",
        "incr-work/wal.ffkv",
        "incr-work/LOCK",
    ] {
        match fs::remove_file(directory.join(name)) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
    }
    for name in ["base-work", "incr-work"] {
        match fs::remove_dir(directory.join(name)) {
            Ok(()) => (),
            Err(e) if e.kind() == io::ErrorKind::NotFound => (),
            Err(e) => return Err(e),
        }
    }
    fs::remove_dir(directory)
}

/// Controls contain their own replies. No engine reference crosses the channel.
pub enum Control {
    Action {
        action: ServerAction,
        reply: oneshot::Sender<Result<Resp, String>>,
    },
}

pub struct RuntimeState {
    pub backup: Arc<OnlineBackup>,
    pub databases: Arc<std::sync::atomic::AtomicU32>,
    pub max_connections: std::sync::atomic::AtomicU32,
    stats: Mutex<PersistenceStats>,
    save_busy: AtomicBool,
    save_scheduled: AtomicBool,
    save_done: tokio::sync::Notify,
    saved_revision: AtomicU64,
    metadata: std::sync::OnceLock<PathBuf>,
    pool: std::sync::OnceLock<Weak<Pool>>,
    state: std::sync::OnceLock<Weak<State>>,
    shutdown_pending: AtomicBool,
    shutdown_cancel: tokio::sync::Notify,
    actions: tokio::sync::Mutex<()>,
    jobs: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    stopping: AtomicBool,
    shutdown: watch::Sender<bool>,
}
impl RuntimeState {
    pub fn new(backup: Arc<OnlineBackup>, databases: u32, max_connections: u32) -> Arc<Self> {
        let (shutdown, _) = watch::channel(false);
        let stats = PersistenceStats {
            last_save_time: 0,
            ..Default::default()
        };
        Arc::new(Self {
            backup,
            databases: Arc::new(std::sync::atomic::AtomicU32::new(databases)),
            max_connections: std::sync::atomic::AtomicU32::new(max_connections),
            stats: Mutex::new(stats),
            save_busy: AtomicBool::new(false),
            save_scheduled: AtomicBool::new(false),
            save_done: tokio::sync::Notify::new(),
            saved_revision: AtomicU64::new(0),
            metadata: std::sync::OnceLock::new(),
            pool: std::sync::OnceLock::new(),
            state: std::sync::OnceLock::new(),
            shutdown_pending: AtomicBool::new(false),
            shutdown_cancel: tokio::sync::Notify::new(),
            actions: tokio::sync::Mutex::new(()),
            jobs: Mutex::new(Vec::new()),
            stopping: AtomicBool::new(false),
            shutdown,
        })
    }
    pub fn shutdown_notifications(&self) -> watch::Receiver<bool> {
        self.shutdown.subscribe()
    }
    pub fn attach_pool(&self, pool: &Arc<Pool>) {
        let _ = self.pool.set(Arc::downgrade(pool));
    }
    fn spawn_job(&self, future: impl Future<Output = ()> + Send + 'static) -> io::Result<()> {
        let mut jobs = self.jobs.lock().unwrap();
        if self.stopping.load(Ordering::Acquire) {
            return Err(invalid("Server is shutting down"));
        }
        jobs.retain(|job| !job.is_finished());
        jobs.push(tokio::spawn(future));
        Ok(())
    }
    /// Stop command producers first; keep the actor and shard pool alive until
    /// checkpoints have drained. Never abort an in-progress WAL checkpoint.
    pub async fn shutdown(&self) -> io::Result<()> {
        let actions = self.actions.lock().await;
        self.stopping.store(true, Ordering::Release);
        let jobs = std::mem::take(&mut *self.jobs.lock().unwrap());
        drop(actions);
        let mut failure = None;
        if self.backup.in_progress()
            && let Err(error) = self.backup.abort().await
        {
            failure = Some(error);
        }
        for job in jobs {
            if let Err(error) = job.await {
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
        let timers = std::mem::take(&mut *self.backup.timers.lock().unwrap());
        for timer in &timers {
            timer.abort();
        }
        for timer in timers {
            if let Err(error) = timer.await
                && !error.is_cancelled()
            {
                failure.get_or_insert_with(|| io::Error::other(error));
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
    fn abort_shutdown(&self) -> io::Result<Resp> {
        if !self.shutdown_pending.swap(false, Ordering::AcqRel) {
            return Err(invalid("No shutdown in progress"));
        }
        self.shutdown_cancel.notify_waiters();
        Ok(ok())
    }
    /// Call before accepting clients. SAVE timestamps survive process restarts
    /// through an independently fsynced native metadata file, never WAL mtime.
    pub fn load_metadata(&self, data_directory: Option<&Path>) -> io::Result<()> {
        if let Some(directory) = data_directory {
            let path = directory.join("server-save.ffkv");
            match fs::read(&path) {
                Ok(bytes) => {
                    let seconds = std::str::from_utf8(&bytes)
                        .ok()
                        .and_then(|s| s.trim().parse::<i64>().ok())
                        .filter(|v| *v >= 0)
                        .ok_or_else(|| invalid("invalid SAVE metadata"))?;
                    self.stats.lock().unwrap().last_save_time = seconds;
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => (),
                Err(e) => return Err(e),
            }
            self.metadata
                .set(path)
                .map_err(|_| invalid("SAVE metadata already initialized"))?;
        }
        Ok(())
    }
    pub fn publish(&self, state: &State) {
        if let Some(server) = state.server.get() {
            server.record_persistence(*self.stats.lock().unwrap());
        }
    }
}

pub struct Hooks {
    state: Weak<State>,
    runtime: Arc<RuntimeState>,
}
impl Hooks {
    pub fn new(state: Weak<State>, runtime: Arc<RuntimeState>) -> Arc<Self> {
        let _ = runtime.state.set(state.clone());
        Arc::new(Self { state, runtime })
    }
}
impl ServerHooks for Hooks {
    fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a> {
        Box::pin(async move {
            // These observations and sleeps must remain available during BASE/SAVE.
            match &action {
                ServerAction::Backup(BackupAction::Status) => {
                    return Ok(self.runtime.backup.status());
                }
                ServerAction::Backup(BackupAction::List) => return Ok(self.runtime.backup.list()),
                ServerAction::Backup(BackupAction::Abort) => {
                    return self
                        .runtime
                        .backup
                        .abort()
                        .await
                        .map(|_| ok())
                        .map_err(|e| e.to_string());
                }
                ServerAction::Backup(BackupAction::Cleanup) => {
                    return self
                        .runtime
                        .backup
                        .cleanup()
                        .await
                        .map(|_| ok())
                        .map_err(|e| e.to_string());
                }
                ServerAction::Shutdown { abort: true, .. } => {
                    return self.runtime.abort_shutdown().map_err(|e| e.to_string());
                }
                ServerAction::Debug(args)
                    if args
                        .first()
                        .is_some_and(|s| s.eq_ignore_ascii_case(b"SLEEP")) =>
                {
                    return debug_sleep(args).await.map_err(|e| e.to_string());
                }
                _ => (),
            }
            let state = self.state.upgrade().ok_or("server stopped")?;
            let tx = state
                .control_tx
                .get()
                .ok_or("server coordinator not initialized")?;
            let (reply, response) = oneshot::channel();
            tx.send(Request::Server(Control::Action { action, reply }))
                .await
                .map_err(|_| "server coordinator stopped")?;
            response
                .await
                .map_err(|_| "server coordinator stopped".to_string())?
        })
    }
}

pub struct Context<'a> {
    pub pool: &'a Arc<Pool>,
    pub persistence: &'a Arc<Persistence>,
    pub state: &'a Arc<State>,
    pub runtime: &'a Arc<RuntimeState>,
}
pub async fn handle(control: Control, context: &mut Context<'_>) {
    match control {
        Control::Action { action, reply } => {
            let result = run(action, context).await.map_err(|e| e.to_string());
            let _ = reply.send(result);
        }
    }
}

enum EnginePause<'a> {
    Borrowed(&'a mut Paused),
    Owned(Paused),
}
impl std::ops::Deref for EnginePause<'_> {
    type Target = Paused;
    fn deref(&self) -> &Paused {
        match self {
            Self::Borrowed(paused) => paused,
            Self::Owned(paused) => paused,
        }
    }
}
impl std::ops::DerefMut for EnginePause<'_> {
    fn deref_mut(&mut self) -> &mut Paused {
        match self {
            Self::Borrowed(paused) => paused,
            Self::Owned(paused) => paused,
        }
    }
}
impl EnginePause<'_> {
    fn resume(self) -> io::Result<()> {
        match self {
            Self::Borrowed(_) => Ok(()),
            Self::Owned(paused) => paused.resume(),
        }
    }
    async fn finish(mut self, persistence: &Persistence) -> io::Result<()> {
        let result = async {
            if let Self::Owned(paused) = &mut self {
                for engine in &mut paused.engines {
                    persistence.commit(engine).await?;
                }
            }
            Ok(())
        }
        .await;
        result.and(self.resume())
    }
}
async fn pause<'a>(
    pool: &Arc<Pool>,
    paused: Option<&'a mut Paused>,
) -> io::Result<EnginePause<'a>> {
    match paused {
        Some(paused) => Ok(EnginePause::Borrowed(paused)),
        None => pool.pause().await.map(EnginePause::Owned),
    }
}
struct ActionContext<'a> {
    pool: &'a Arc<Pool>,
    persistence: &'a Persistence,
    state: &'a State,
    runtime: &'a Arc<RuntimeState>,
    paused: Option<&'a mut Paused>,
}
struct PausedHooks<'a> {
    context: tokio::sync::Mutex<ActionContext<'a>>,
}
impl ServerHooks for PausedHooks<'_> {
    fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a> {
        Box::pin(async move {
            run_action(action, &mut *self.context.lock().await)
                .await
                .map_err(|e| e.to_string())
        })
    }
}
/// The caller authorizes commands and commits pending transaction mutations
/// before checkpoint/capture controls. No request is sent back to the actor.
pub async fn execute_paused(
    args: &[Bytes],
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
) -> io::Result<Option<Resp>> {
    let Some(server) = state.server.get() else {
        return Ok(None);
    };
    let runtime = state
        .admin_runtime
        .get()
        .ok_or_else(|| invalid("server runtime is not initialized"))?;
    let pool = runtime
        .pool
        .get()
        .and_then(Weak::upgrade)
        .ok_or_else(|| invalid("server engine pool is not initialized"))?;
    let database = paused.engines.first().map_or(0, Engine::database);
    let hooks = PausedHooks {
        context: tokio::sync::Mutex::new(ActionContext {
            pool: &pool,
            persistence,
            state,
            runtime,
            paused: Some(paused),
        }),
    };
    Ok(server
        .execute_with_hooks(args, CommandContext { database }, &hooks)
        .await)
}
pub fn requires_commit(args: &[Bytes]) -> bool {
    let Some(command) = args.first() else {
        return false;
    };
    if [b"SAVE".as_slice(), b"BGSAVE", b"BGREWRITEAOF", b"SHUTDOWN"]
        .iter()
        .any(|name| command.eq_ignore_ascii_case(name))
    {
        return true;
    }
    if command.eq_ignore_ascii_case(b"BACKUP") {
        return args.get(1).is_some_and(|sub| {
            sub.eq_ignore_ascii_case(b"START") || sub.eq_ignore_ascii_case(b"SEAL")
        });
    }
    command.eq_ignore_ascii_case(b"CONFIG")
        && args
            .get(1)
            .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SET"))
        && args[2..].as_chunks::<2>().0.iter().any(|pair| {
            pair[0].eq_ignore_ascii_case(b"appendonly")
                || pair[0].eq_ignore_ascii_case(b"appendfsync")
        })
}
async fn checkpoint(
    pool: &Arc<Pool>,
    persistence: &Persistence,
    runtime: &RuntimeState,
    save: bool,
) -> io::Result<u64> {
    let paused = pool.pause().await?;
    let result = checkpoint_paused(&paused, persistence, runtime, save).await;
    let resumed = paused.resume();
    result.and_then(|revision| resumed.map(|_| revision))
}
async fn checkpoint_paused(
    paused: &Paused,
    persistence: &Persistence,
    runtime: &RuntimeState,
    save: bool,
) -> io::Result<u64> {
    let timestamp = Engine::now();
    persistence
        .checkpoint(
            paused
                .engines
                .iter()
                .flat_map(|engine| engine.snapshot_batches(timestamp)),
        )
        .await?;
    if save {
        let timestamp = now().min(i64::MAX as u64) as i64;
        if let Some(path) = runtime.metadata.get() {
            persist_save_time(path, timestamp)?;
        }
        runtime.stats.lock().unwrap().last_save_time = timestamp;
    }
    Ok(persistence.status().await.revision)
}

async fn run(action: ServerAction, c: &mut Context<'_>) -> io::Result<Resp> {
    c.runtime.attach_pool(c.pool);
    run_action(
        action,
        &mut ActionContext {
            pool: c.pool,
            persistence: c.persistence,
            state: c.state,
            runtime: c.runtime,
            paused: None,
        },
    )
    .await
}
type BackgroundContext = (Arc<Pool>, Arc<Persistence>, Arc<RuntimeState>, Arc<State>);

fn background_context(c: &ActionContext<'_>) -> io::Result<BackgroundContext> {
    let state = c
        .runtime
        .state
        .get()
        .and_then(Weak::upgrade)
        .ok_or_else(|| invalid("server stopped"))?;
    let persistence = state
        .persistence
        .get()
        .cloned()
        .ok_or_else(|| invalid("persistence is not initialized"))?;
    Ok((c.pool.clone(), persistence, c.runtime.clone(), state))
}
async fn run_action(action: ServerAction, c: &mut ActionContext<'_>) -> io::Result<Resp> {
    let _action = c.runtime.actions.lock().await;
    if c.runtime.stopping.load(Ordering::Acquire) {
        return Err(invalid("Server is shutting down"));
    }
    match action {
        ServerAction::Save {
            background,
            schedule,
        } => {
            let background_resources = if background {
                Some(background_context(c)?)
            } else {
                None
            };
            if c.runtime.save_busy.swap(true, Ordering::AcqRel) {
                if !background || !schedule {
                    return Err(invalid("Background save already in progress"));
                }
                if c.runtime.save_scheduled.swap(true, Ordering::AcqRel) {
                    return Err(invalid("Background save already scheduled"));
                }
                let (pool, persistence, runtime, state) =
                    background_resources.expect("background schedule");
                let spawned = c.runtime.spawn_job(async move {
                    loop {
                        if runtime
                            .save_busy
                            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            break;
                        }
                        runtime.save_done.notified().await;
                    }
                    runtime.save_scheduled.store(false, Ordering::Release);
                    runtime.stats.lock().unwrap().save_in_progress = true;
                    runtime.publish(&state);
                    let result = checkpoint(&pool, &persistence, &runtime, true).await;
                    save_finished(
                        &runtime,
                        &state,
                        &persistence,
                        result.as_ref().ok().copied(),
                        false,
                    )
                    .await;
                });
                if let Err(error) = spawned {
                    c.runtime.save_scheduled.store(false, Ordering::Release);
                    return Err(error);
                }
                return Ok(Resp::Simple("Background saving scheduled"));
            }
            {
                let mut stats = c.runtime.stats.lock().unwrap();
                stats.save_in_progress = true;
            }
            c.runtime.publish(c.state);
            if background {
                let (pool, persistence, runtime, state) =
                    background_resources.expect("background save");
                let spawned = c.runtime.spawn_job(async move {
                    let result = checkpoint(&pool, &persistence, &runtime, true).await;
                    save_finished(
                        &runtime,
                        &state,
                        &persistence,
                        result.as_ref().ok().copied(),
                        false,
                    )
                    .await;
                });
                if let Err(error) = spawned {
                    save_finished(c.runtime, c.state, c.persistence, None, false).await;
                    return Err(error);
                }
                Ok(Resp::Simple("Background saving started"))
            } else {
                let result = async {
                    let paused = pause(c.pool, c.paused.as_deref_mut()).await?;
                    let result = checkpoint_paused(&paused, c.persistence, c.runtime, true).await;
                    let resumed = paused.finish(c.persistence).await;
                    result.and_then(|revision| resumed.map(|_| revision))
                }
                .await;
                save_finished(
                    c.runtime,
                    c.state,
                    c.persistence,
                    result.as_ref().ok().copied(),
                    false,
                )
                .await;
                result.map(|_| ok())
            }
        }
        ServerAction::RewriteAof => {
            if !c.persistence.enabled() {
                return Err(invalid("AOF is disabled"));
            }
            let (pool, persistence, runtime, state) = background_context(c)?;
            if c.runtime.save_busy.swap(true, Ordering::AcqRel) {
                return Err(invalid(
                    "Background persistence operation already in progress",
                ));
            }
            {
                c.runtime.stats.lock().unwrap().rewrite_in_progress = true;
            }
            c.runtime.publish(c.state);
            let spawned = c.runtime.spawn_job(async move {
                let result = checkpoint(&pool, &persistence, &runtime, false).await;
                save_finished(
                    &runtime,
                    &state,
                    &persistence,
                    result.as_ref().ok().copied(),
                    true,
                )
                .await;
            });
            if let Err(error) = spawned {
                save_finished(c.runtime, c.state, c.persistence, None, true).await;
                return Err(error);
            }
            Ok(Resp::Simple(
                "Background append only file rewriting started",
            ))
        }
        ServerAction::Backup(BackupAction::Seal) => {
            let paused = pause(c.pool, c.paused.as_deref_mut()).await?;
            let result = c.runtime.backup.seal().await;
            result.and(paused.finish(c.persistence).await)?;
            Ok(ok())
        }
        ServerAction::Backup(BackupAction::Status) => Ok(c.runtime.backup.status()),
        ServerAction::Backup(BackupAction::List) => Ok(c.runtime.backup.list()),
        ServerAction::Backup(BackupAction::Abort) => {
            c.runtime.backup.abort().await?;
            Ok(ok())
        }
        ServerAction::Backup(BackupAction::Cleanup) => {
            c.runtime.backup.cleanup().await?;
            Ok(ok())
        }
        ServerAction::Backup(BackupAction::Start) => {
            let generation = c.runtime.backup.begin()?;
            let backup = c.runtime.backup.clone();
            let pool = c.pool.clone();
            if let Err(error) = c.runtime.spawn_job(async move {
                let _ = backup.snapshot(&pool, generation).await;
            }) {
                c.runtime.backup.fail(generation, &error);
                c.runtime.backup.status.lock().unwrap().busy = false;
                return Err(error);
            }
            Ok(ok())
        }
        ServerAction::Configure(changes) => configure(c, changes).await,
        ServerAction::ResetStatistics => {
            c.state.reset_statistics();
            if let Some(server) = c.state.server.get() {
                server.reset_statistics();
            }
            Ok(ok())
        }
        ServerAction::Shutdown {
            save,
            now,
            force,
            abort,
        } => shutdown(c, save, now, force, abort).await,
        ServerAction::Role => role(c.state),
        ServerAction::Info(sections) => info(c, &sections).await,
        ServerAction::MemoryUsage {
            database,
            key,
            samples,
        } => {
            let mut paused = pause(c.pool, c.paused.as_deref_mut()).await?;
            let shard = Engine::shard_for(&key, paused.engines.len());
            let engine = &mut paused.engines[shard];
            let previous = engine.database();
            engine.switch_database(database);
            let usage = engine.memory_usage(&key, samples, Engine::now());
            engine.switch_database(previous);
            paused.finish(c.persistence).await?;
            Ok(match usage {
                Ok(value) => value.map_or(Resp::Bulk(None), |n| {
                    Resp::Integer(n.min(i64::MAX as usize) as i64)
                }),
                Err(reply) => reply,
            })
        }
        ServerAction::MemoryStatistics => {
            let mut paused = pause(c.pool, c.paused.as_deref_mut()).await?;
            let timestamp = Engine::now();
            let mut bytes = 0u64;
            let mut keys = 0u64;
            let mut failure = None;
            for engine in &mut paused.engines {
                match engine.memory_statistics(timestamp) {
                    Ok(statistics) => {
                        bytes = bytes.saturating_add(statistics.allocated_bytes);
                        keys = keys.saturating_add(statistics.keys)
                    }
                    Err(reply) => {
                        failure = Some(reply);
                        break;
                    }
                }
            }
            paused.finish(c.persistence).await?;
            if let Some(reply) = failure {
                return Ok(reply);
            }
            let mut fields = vec![
                (
                    bulk("total.allocated"),
                    Resp::Integer(bytes.min(i64::MAX as u64) as i64),
                ),
                (
                    bulk("keys.count"),
                    Resp::Integer(keys.min(i64::MAX as u64) as i64),
                ),
                (
                    bulk("ffkv.accounting"),
                    bulk("owned dataset capacity estimate; shared process caches excluded"),
                ),
            ];
            if let Some(rss) = c.state.metrics.rss_pressure().rss_bytes {
                fields.push((
                    bulk("rss.bytes"),
                    Resp::Integer(rss.min(i64::MAX as u64) as i64),
                ));
            }
            Ok(Resp::Map(fields))
        }
        ServerAction::Debug(args) => debug(c, &args).await,
    }
}
async fn shutdown(
    c: &mut ActionContext<'_>,
    save: Option<bool>,
    immediate: bool,
    force: bool,
    abort: bool,
) -> io::Result<Resp> {
    if abort {
        return c.runtime.abort_shutdown();
    }
    if c.runtime.shutdown_pending.swap(true, Ordering::AcqRel) {
        return Err(invalid("Shutdown already in progress"));
    }
    let result = async {
        // The same pause protects the catch-up watermark, optional image and
        // final flush. NOW bypasses only replica waiting, never persistence.
        let paused = pause(c.pool, c.paused.as_deref_mut()).await?;
        let result = async {
            let timeout = c
                .state
                .server
                .get()
                .map_or(10, |server| server.config_number("shutdown-timeout", 10))
                .max(0) as u64;
            let deadline = tokio::time::Instant::now() + Duration::from_secs(timeout);
            if !immediate && let Some(native) = c.state.native.get() {
                let watermark = native.status().offset;
                loop {
                    if !c.runtime.shutdown_pending.load(Ordering::Acquire) {
                        return Err(invalid("Shutdown aborted"));
                    }
                    let status = native.status();
                    if status
                        .replicas
                        .iter()
                        .all(|replica| replica.processed >= watermark)
                        || tokio::time::Instant::now() >= deadline
                    {
                        break;
                    }
                    tokio::select! {
                        _=tokio::time::sleep(Duration::from_millis(25))=>(),
                        _=c.runtime.shutdown_cancel.notified()=>(),
                    }
                }
            }
            if !c.runtime.shutdown_pending.load(Ordering::Acquire) {
                return Err(invalid("Shutdown aborted"));
            }
            if save.unwrap_or(c.persistence.enabled())
                && let Err(error) = checkpoint_paused(&paused, c.persistence, c.runtime, true).await
                && !force
            {
                return Err(error);
            }
            if let Err(error) = c.persistence.finish().await
                && !force
            {
                return Err(error);
            }
            if !c.runtime.shutdown_pending.load(Ordering::Acquire) {
                return Err(invalid("Shutdown aborted"));
            }
            c.runtime.shutdown.send_replace(true);
            Ok(ok())
        }
        .await;
        let resumed = paused.resume();
        result.and_then(|reply| resumed.map(|_| reply))
    }
    .await;
    c.runtime.shutdown_pending.store(false, Ordering::Release);
    result
}
async fn save_finished(
    runtime: &RuntimeState,
    state: &State,
    persistence: &Persistence,
    revision: Option<u64>,
    rewrite: bool,
) {
    let status = persistence.status().await;
    let success = revision.is_some();
    {
        let mut stats = runtime.stats.lock().unwrap();
        stats.wal_bytes = status.bytes;
        if rewrite {
            stats.rewrite_in_progress = false;
            stats.last_rewrite_ok = success;
        } else {
            stats.save_in_progress = false;
            stats.last_save_ok = success;
            if let Some(revision) = revision {
                runtime.saved_revision.store(revision, Ordering::Release);
                stats.changes_since_save = status.revision.saturating_sub(revision);
            }
        }
    }
    runtime.save_busy.store(false, Ordering::Release);
    runtime.save_done.notify_one();
    runtime.publish(state);
}

fn role(state: &State) -> io::Result<Resp> {
    let status = state
        .native
        .get()
        .ok_or_else(|| invalid("native replication is not initialized"))?
        .status();
    Ok(match status.role {
        crate::native_replication::Role::Primary => array(vec![
            bulk("master"),
            Resp::Integer(status.offset.min(i64::MAX as u64) as i64),
            array(
                status
                    .replicas
                    .into_iter()
                    .filter(|r| r.online)
                    .map(|r| {
                        array(vec![
                            bulk(r.host),
                            bulk(r.port.to_string()),
                            bulk(r.processed.to_string()),
                        ])
                    })
                    .collect(),
            ),
        ]),
        crate::native_replication::Role::Replica => {
            let (host, port) = status
                .upstream
                .ok_or_else(|| invalid("replica has no upstream"))?;
            array(vec![
                bulk("slave"),
                bulk(host),
                Resp::Integer(port as i64),
                bulk(if status.upstream_connected {
                    "connected"
                } else {
                    "connect"
                }),
                Resp::Integer(status.offset.min(i64::MAX as u64) as i64),
            ])
        }
    })
}

async fn info(c: &mut ActionContext<'_>, sections: &[String]) -> io::Result<Resp> {
    let wants = |name: &str| {
        sections.is_empty()
            || sections
                .iter()
                .any(|s| s == name || matches!(s.as_str(), "all" | "everything" | "default"))
    };
    // Legacy State::info contains a fabricated standalone replication section;
    // request only its observational sections and supply native status here.
    let filtered: Vec<Bytes> = ["server", "clients", "memory"]
        .into_iter()
        .filter(|s| wants(s))
        .map(|s| Bytes::from_static(s.as_bytes()))
        .collect();
    let mut text = if filtered.is_empty() {
        String::new()
    } else {
        match c.state.info(&filtered) {
            Resp::Bulk(Some(b)) => String::from_utf8_lossy(&b).into_owned(),
            _ => String::new(),
        }
    };
    use std::fmt::Write;
    if wants("memory") {
        let mut paused = pause(c.pool, c.paused.as_deref_mut()).await?;
        let timestamp = Engine::now();
        let mut bytes = 0u64;
        let mut keys = 0u64;
        let mut failure = None;
        for engine in &mut paused.engines {
            match engine.memory_statistics(timestamp) {
                Ok(statistics) => {
                    bytes = bytes.saturating_add(statistics.allocated_bytes);
                    keys = keys.saturating_add(statistics.keys)
                }
                Err(reply) => {
                    failure = Some(reply);
                    break;
                }
            }
        }
        paused.finish(c.persistence).await?;
        if let Some(reply) = failure {
            return Ok(reply);
        }
        write!(&mut text,"\r\n# Native_memory\r\nused_memory:{bytes}\r\nffkv_dataset_memory_bytes:{bytes}\r\nffkv_dataset_keys:{keys}\r\nffkv_memory_accounting:owned-dataset-capacity-estimate\r\n").unwrap();
    }
    if wants("replication") {
        let s = c
            .state
            .native
            .get()
            .ok_or_else(|| invalid("native replication is not initialized"))?
            .status();
        write!(&mut text,"\r\n# Replication\r\nrole:{}\r\nmaster_replid:{}\r\nmaster_replid2:{}\r\nmaster_repl_offset:{}\r\nsecond_repl_offset:{}\r\nslave_repl_offset:{}\r\nslave_fsynced_repl_offset:{}\r\nconnected_slaves:{}\r\nmaster_connect_denied:{}\r\nmaster_failover_state:{}\r\n",if s.role==crate::native_replication::Role::Primary{"master"}else{"slave"},s.history,s.secondary_history.as_deref().unwrap_or("0000000000000000000000000000000000000000"),s.offset,s.secondary_valid_through,s.offset,s.fsynced_offset,s.replicas.iter().filter(|r|r.online).count(),s.master_connect_denied,if s.failover_active{"in-progress"}else{"no-failover"}).unwrap();
        if let Some((host, port)) = s.upstream {
            write!(
                &mut text,
                "master_host:{host}\r\nmaster_port:{port}\r\nmaster_link_status:{}\r\n",
                if s.upstream_connected { "up" } else { "down" }
            )
            .unwrap();
        }
        for (index, r) in s.replicas.iter().enumerate() {
            write!(
                &mut text,
                "slave{index}:ip={},port={},state={},offset={},lag={}\r\n",
                r.host,
                r.port,
                if r.online { "online" } else { "offline" },
                r.processed,
                r.last_ack.elapsed().as_secs()
            )
            .unwrap();
        }
    }
    if wants("persistence") {
        let status = c.persistence.status().await;
        {
            let mut stats = c.runtime.stats.lock().unwrap();
            stats.wal_bytes = status.bytes;
            stats.changes_since_save = status
                .revision
                .saturating_sub(c.runtime.saved_revision.load(Ordering::Acquire));
        }
        c.runtime.publish(c.state);
        write!(&mut text,"\r\n# Native_persistence\r\nloading:{}\r\naof_enabled:{}\r\nffkv_wal_enabled:{}\r\nffkv_wal_policy:{}\r\nbackup_in_progress:{}\r\n",u8::from(!c.state.replica_ready()),u8::from(c.persistence.enabled()),u8::from(c.persistence.enabled()),status.policy,u8::from(c.runtime.backup.in_progress())).unwrap();
    }
    Ok(bulk(text))
}

async fn configure(c: &mut ActionContext<'_>, changes: Vec<(String, Bytes)>) -> io::Result<Resp> {
    let mut enabled = c.persistence.enabled();
    let status = c.persistence.status().await;
    let mut policy = if status.policy == "everysec" {
        Durability::Everysec
    } else {
        Durability::Strict
    };
    let mut persistence_change = false;
    let mut databases = None;
    let mut clients = None;
    let mut ttl = None;
    // No effects until every member of the transaction has been validated.
    for (name, value) in &changes {
        match name.as_str() {
            "appendonly" => {
                enabled = parse_bool(value)?;
                persistence_change = true;
            }
            "appendfsync" => {
                policy = parse_policy(value)?;
                persistence_change = true;
            }
            "databases" => {
                let n = parse_u64(value, 1, 65536)? as u32;
                if c.state.cluster.get().is_some() && n != 1 {
                    return Err(invalid("cluster mode requires one database"));
                }
                databases = Some(n);
            }
            "maxclients" => clients = Some(parse_u64(value, 1, u32::MAX as u64)? as u32),
            "backup-sealed-ttl" => ttl = Some(parse_u64(value, 0, u64::MAX)?),
            name if crate::server_commands::ServerRuntime::local_parameter(name) => (),
            _ => return Err(invalid(format!("configuration '{name}' is not mutable"))),
        }
    }
    let paused = pause(c.pool, c.paused.as_deref_mut()).await?;
    let result = async {
        if let Some(count) = databases
            && count < c.runtime.databases.load(Ordering::Acquire)
        {
            for engine in &paused.engines {
                if engine
                    .highest_nonempty_database(Engine::now())
                    .is_some_and(|db| db >= count)
                {
                    return Err(invalid("cannot remove a nonempty database"));
                }
            }
            // Existing SELECTed clients must not retain an out-of-range DB.
            if c.state.client_registry.highest_database() >= count {
                return Err(invalid(
                    "a client is using a database that would be removed",
                ));
            }
        }
        if persistence_change {
            let timestamp = Engine::now();
            c.persistence
                .configure(
                    enabled,
                    policy,
                    paused
                        .engines
                        .iter()
                        .flat_map(|engine| engine.snapshot_batches(timestamp)),
                )
                .await?;
            if let Some(native) = c.state.native.get() {
                native.set_aof_enabled(enabled);
            }
        }
        if let Some(count) = databases {
            c.runtime.databases.store(count, Ordering::Release);
        }
        if let Some(count) = clients {
            c.runtime.max_connections.store(count, Ordering::Release);
        }
        if let Some(seconds) = ttl {
            c.runtime.backup.set_sealed_ttl(seconds);
        }
        Ok(())
    }
    .await;
    result.and(paused.finish(c.persistence).await)?;
    if let Some(server) = c.state.server.get() {
        server.publish_configuration(changes);
    }
    Ok(ok())
}

async fn debug_sleep(args: &[Bytes]) -> io::Result<Resp> {
    if args.len() != 2 {
        return Err(invalid("wrong number of arguments for DEBUG SLEEP"));
    }
    let seconds = std::str::from_utf8(&args[1])
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|s| s.is_finite() && *s >= 0.0 && *s <= 86400.0)
        .ok_or_else(|| invalid("invalid DEBUG SLEEP duration"))?;
    tokio::time::sleep(Duration::from_secs_f64(seconds)).await;
    Ok(ok())
}
async fn debug(c: &mut ActionContext<'_>, args: &[Bytes]) -> io::Result<Resp> {
    let Some(command) = args.first() else {
        return Err(invalid("DEBUG requires a subcommand"));
    };
    if command.eq_ignore_ascii_case(b"HELP") && args.len() == 1 {
        return Ok(array(
            [
                "HELP",
                "SLEEP <seconds> -- sleep without pausing engines",
                "OBJECT <key> -- actual owned-memory diagnostics",
            ]
            .into_iter()
            .map(bulk)
            .collect(),
        ));
    }
    if command.eq_ignore_ascii_case(b"SLEEP") {
        return debug_sleep(args).await;
    }
    if command.eq_ignore_ascii_case(b"OBJECT") && args.len() == 2 {
        let mut paused = pause(c.pool, c.paused.as_deref_mut()).await?;
        let shard = Engine::shard_for(&args[1], paused.engines.len());
        let usage = paused.engines[shard].memory_usage(&args[1], 0, Engine::now());
        paused.finish(c.persistence).await?;
        let usage = match usage {
            Ok(value) => value.ok_or_else(|| invalid("no such key"))?,
            Err(reply) => return Ok(reply),
        };
        return Ok(Resp::Status(Bytes::from(format!(
            "memory_usage:{usage} accounting:owned-capacity-estimate sampling:full"
        ))));
    }
    Err(invalid("unsupported DEBUG subcommand; try DEBUG HELP"))
}
fn parse_bool(bytes: &[u8]) -> io::Result<bool> {
    if bytes.eq_ignore_ascii_case(b"yes") {
        Ok(true)
    } else if bytes.eq_ignore_ascii_case(b"no") {
        Ok(false)
    } else {
        Err(invalid("expected yes or no"))
    }
}
fn parse_u64(bytes: &[u8], min: u64, max: u64) -> io::Result<u64> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|n| *n >= min && *n <= max)
        .ok_or_else(|| invalid("configuration integer is out of range"))
}
fn parse_policy(bytes: &[u8]) -> io::Result<Durability> {
    if bytes.eq_ignore_ascii_case(b"always") {
        Ok(Durability::Strict)
    } else if bytes.eq_ignore_ascii_case(b"everysec") {
        Ok(Durability::Everysec)
    } else {
        Err(invalid("appendfsync supports always and everysec"))
    }
}

/// Startup-only values not represented by older Options versions. The parent
/// applies these before constructing Persistence, Pool, listeners or runtimes.
pub struct StartupValues {
    pub appendonly: Option<bool>,
    pub backup_directory: Option<PathBuf>,
    pub backup_sealed_ttl: u64,
    pub telemetry: BTreeMap<String, Bytes>,
}
pub fn load_startup(options: &mut crate::options::Options) -> io::Result<StartupValues> {
    let mut result = StartupValues {
        appendonly: None,
        backup_directory: None,
        backup_sealed_ttl: 0,
        telemetry: BTreeMap::new(),
    };
    let Some(path) = options.server_config_file.as_ref() else {
        return Ok(result);
    };
    let values = match ConfigSnapshot::load_values(path) {
        Ok(v) => v,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(result),
        Err(e) => return Err(e),
    };
    for (name, value) in values {
        let text =
            || std::str::from_utf8(&value).map_err(|_| invalid(format!("{name} is not UTF-8")));
        match name.as_str() {
            "appendonly" => {
                let enabled = parse_bool(&value)?;
                options.appendonly = enabled;
                result.appendonly = Some(enabled);
            }
            "appendfsync" => options.durability = Some(parse_policy(&value)?),
            "databases" => options.databases = parse_u64(&value, 1, 65536)? as u32,
            "maxclients" => options.max_connections = parse_u64(&value, 1, u32::MAX as u64)? as u32,
            "bind" => {
                options.bind = text()?
                    .parse()
                    .map_err(|_| invalid("invalid bind socket address"))?
            }
            "port" => {
                let port = parse_u64(&value, 1, u16::MAX as u64)? as u16;
                options.bind.set_port(port);
            }
            "dir" => {
                options.data_dir = if value.is_empty() {
                    None
                } else {
                    Some(PathBuf::from(text()?))
                }
            }
            "backupdirname" => result.backup_directory = Some(PathBuf::from(text()?)),
            "backup-sealed-ttl" => result.backup_sealed_ttl = parse_u64(&value, 0, u64::MAX)?,
            "cluster-enabled" => options.cluster_enabled = parse_bool(&value)?,
            "ffkv-shards" => options.shards = parse_u64(&value, 1, 256)? as u16,
            "latency-monitor-threshold" | "shutdown-timeout" => {
                parse_u64(&value, 0, u32::MAX as u64)?;
                result.telemetry.insert(name, value);
            }
            "latency-tracking" => {
                parse_bool(&value)?;
                result.telemetry.insert(name, value);
            }
            "slowlog-max-len" => {
                parse_u64(&value, 0, 100000)?;
                result.telemetry.insert(name, value);
            }
            "slowlog-log-slower-than" => {
                std::str::from_utf8(&value)
                    .ok()
                    .and_then(|s| s.parse::<i64>().ok())
                    .filter(|n| *n >= -1)
                    .ok_or_else(|| invalid("invalid slowlog threshold"))?;
                result.telemetry.insert(name, value);
            }
            _ => return Err(invalid(format!("unknown rewritten configuration '{name}'"))),
        }
    }
    Ok(result)
}
pub fn config_snapshot(
    options: &crate::options::Options,
    appendonly: bool,
    backup_directory: &Path,
    sealed_ttl: u64,
    startup: &StartupValues,
) -> ConfigSnapshot {
    let mut config = ConfigSnapshot {
        rewrite_path: options.server_config_file.clone(),
        ..Default::default()
    };
    let mut insert = |name: &str, value: String, kind, mutable| {
        config.parameters.insert(
            name.into(),
            ConfigParameter {
                value: Bytes::from(value),
                kind,
                mutable,
            },
        );
    };
    insert(
        "appendonly",
        if appendonly { "yes" } else { "no" }.into(),
        ConfigKind::Boolean,
        true,
    );
    insert(
        "appendfsync",
        if matches!(
            options.durability.unwrap_or(Durability::Strict),
            Durability::Strict
        ) {
            "always"
        } else {
            "everysec"
        }
        .into(),
        ConfigKind::Text,
        true,
    );
    insert(
        "databases",
        options.databases.to_string(),
        ConfigKind::Unsigned { min: 1, max: 65536 },
        true,
    );
    insert(
        "maxclients",
        options.max_connections.to_string(),
        ConfigKind::Unsigned {
            min: 1,
            max: u32::MAX as u64,
        },
        true,
    );
    insert(
        "backup-sealed-ttl",
        sealed_ttl.to_string(),
        ConfigKind::Unsigned {
            min: 0,
            max: u64::MAX,
        },
        true,
    );
    insert("bind", options.bind.to_string(), ConfigKind::Text, false);
    insert(
        "port",
        options.bind.port().to_string(),
        ConfigKind::Unsigned {
            min: 1,
            max: u16::MAX as u64,
        },
        false,
    );
    insert(
        "dir",
        options
            .data_dir
            .as_ref()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default(),
        ConfigKind::Text,
        false,
    );
    insert(
        "backupdirname",
        backup_directory.to_string_lossy().into_owned(),
        ConfigKind::Text,
        false,
    );
    insert(
        "cluster-enabled",
        if options.cluster_enabled { "yes" } else { "no" }.into(),
        ConfigKind::Boolean,
        false,
    );
    insert(
        "ffkv-shards",
        options.shards.to_string(),
        ConfigKind::Unsigned { min: 1, max: 256 },
        false,
    );
    for (name, value) in &startup.telemetry {
        if let Some(parameter) = config.parameters.get_mut(name) {
            parameter.value = value.clone();
        }
    }
    config
}

fn persist_save_time(path: &Path, timestamp: i64) -> io::Result<()> {
    use std::io::Write;
    let directory = path
        .parent()
        .ok_or_else(|| invalid("SAVE metadata has no directory"))?;
    let temporary = directory.join(format!(
        ".ffkv-save-{}-{:032x}",
        std::process::id(),
        rand::random::<u128>()
    ));
    let result = (|| {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        writeln!(file, "{timestamp}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        fs::File::open(directory)?.sync_all()
    })();
    if result.is_err() {
        let _ = fs::remove_file(temporary);
    }
    result
}
