//! Request-side administration. Persistence and engine work stays in ServerHooks;
//! bounded observations and diagnostic replies never require entering an engine.

pub mod extensions;
mod telemetry;

use crate::protocol::Resp;
use bytes::Bytes;
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};
pub use telemetry::{Observation, thread_cpu_time};
use tokio::sync::broadcast;

pub type HookFuture<'a> = Pin<Box<dyn Future<Output = Result<Resp, String>> + Send + 'a>>;
pub trait ServerHooks: Send + Sync {
    fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a>;
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BackupAction {
    Start,
    Status,
    List,
    Seal,
    Abort,
    Cleanup,
}
#[derive(Debug)]
pub enum ServerAction {
    Save {
        background: bool,
        schedule: bool,
    },
    RewriteAof,
    Backup(BackupAction),
    Configure(Vec<(String, Bytes)>),
    ResetStatistics,
    Shutdown {
        save: Option<bool>,
        now: bool,
        force: bool,
        abort: bool,
    },
    MemoryUsage {
        database: u32,
        key: Bytes,
        samples: usize,
    },
    MemoryStatistics,
    Debug(Vec<Bytes>),
    Role,
    Info(Vec<String>),
}

#[derive(Clone, Copy)]
pub enum ConfigKind {
    Text,
    Unsigned { min: u64, max: u64 },
    Signed { min: i64, max: i64 },
    Boolean,
}
#[derive(Clone)]
pub struct ConfigParameter {
    pub value: Bytes,
    pub kind: ConfigKind,
    pub mutable: bool,
}
#[derive(Clone)]
pub struct ConfigSnapshot {
    pub parameters: BTreeMap<String, ConfigParameter>,
    pub rewrite_path: Option<PathBuf>,
}
impl Default for ConfigSnapshot {
    fn default() -> Self {
        let mut parameters = BTreeMap::new();
        for (name, value, kind) in [
            (
                "latency-monitor-threshold",
                "0",
                ConfigKind::Unsigned {
                    min: 0,
                    max: u32::MAX as u64,
                },
            ),
            ("latency-tracking", "yes", ConfigKind::Boolean),
            (
                "slowlog-log-slower-than",
                "10000",
                ConfigKind::Signed {
                    min: -1,
                    max: i64::MAX,
                },
            ),
            (
                "slowlog-max-len",
                "128",
                ConfigKind::Unsigned {
                    min: 0,
                    max: 100000,
                },
            ),
            (
                "shutdown-timeout",
                "10",
                ConfigKind::Unsigned {
                    min: 0,
                    max: u32::MAX as u64,
                },
            ),
        ] {
            parameters.insert(
                name.into(),
                ConfigParameter {
                    value: Bytes::from_static(value.as_bytes()),
                    kind,
                    mutable: true,
                },
            );
        }
        Self {
            parameters,
            rewrite_path: None,
        }
    }
}
impl ConfigSnapshot {
    /// Rewrite files contain byte arrays, not lossy UTF-8 configuration values.
    pub fn load_values(path: &Path) -> io::Result<BTreeMap<String, Bytes>> {
        use std::io::Read;
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take(1024 * 1024 + 1)
            .read_to_end(&mut bytes)?;
        if bytes.len() > 1024 * 1024 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "configuration exceeds 1 MiB",
            ));
        }
        let values: BTreeMap<String, Vec<u8>> =
            serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        Ok(values
            .into_iter()
            .map(|(name, value)| (name, Bytes::from(value)))
            .collect())
    }
}

pub struct CommandContext {
    pub database: u32,
}
#[derive(Clone, Copy, Debug)]
pub struct PersistenceStats {
    pub last_save_time: i64,
    pub changes_since_save: u64,
    pub save_in_progress: bool,
    pub rewrite_in_progress: bool,
    pub last_save_ok: bool,
    pub last_rewrite_ok: bool,
    pub wal_bytes: u64,
}
impl Default for PersistenceStats {
    fn default() -> Self {
        Self {
            last_save_time: unix_ms() as i64 / 1000,
            changes_since_save: 0,
            save_in_progress: false,
            rewrite_in_progress: false,
            last_save_ok: true,
            last_rewrite_ok: true,
            wal_bytes: 0,
        }
    }
}

/// Commands this runtime intercepts, classified once per root registration by
/// the engine registry ([`crate::engine::BoundCommand::server_command`]).
/// Subcommand, arity and syntax errors stay with each arm's dispatch.
#[derive(Clone, Copy, Debug)]
pub(crate) enum ServerCommand {
    Time,
    LastSave,
    Role,
    Info,
    Save,
    BgSave,
    BgRewriteAof,
    Backup,
    Config,
    Debug,
    Memory,
    Latency,
    Slowlog,
    Hotkeys,
    Monitor,
    Shutdown,
    Lolwut,
    Module,
}
impl ServerCommand {
    /// Classifies static registrations and cold direct-runtime invocations.
    pub(crate) fn of(name: &[u8]) -> Option<Self> {
        Some(match name {
            name if name.eq_ignore_ascii_case(b"time") => Self::Time,
            name if name.eq_ignore_ascii_case(b"lastsave") => Self::LastSave,
            name if name.eq_ignore_ascii_case(b"role") => Self::Role,
            name if name.eq_ignore_ascii_case(b"info") => Self::Info,
            name if name.eq_ignore_ascii_case(b"save") => Self::Save,
            name if name.eq_ignore_ascii_case(b"bgsave") => Self::BgSave,
            name if name.eq_ignore_ascii_case(b"bgrewriteaof") => Self::BgRewriteAof,
            name if name.eq_ignore_ascii_case(b"backup") => Self::Backup,
            name if name.eq_ignore_ascii_case(b"config") => Self::Config,
            name if name.eq_ignore_ascii_case(b"debug") => Self::Debug,
            name if name.eq_ignore_ascii_case(b"memory") => Self::Memory,
            name if name.eq_ignore_ascii_case(b"latency") => Self::Latency,
            name if name.eq_ignore_ascii_case(b"slowlog") => Self::Slowlog,
            name if name.eq_ignore_ascii_case(b"hotkeys") => Self::Hotkeys,
            name if name.eq_ignore_ascii_case(b"monitor") => Self::Monitor,
            name if name.eq_ignore_ascii_case(b"shutdown") => Self::Shutdown,
            name if name.eq_ignore_ascii_case(b"lolwut") => Self::Lolwut,
            name if name.eq_ignore_ascii_case(b"module") => Self::Module,
            _ => return None,
        })
    }
}

/// Whether a HOTKEYS session may be tracking, on its own cache line: every
/// command's CPU window reads it, while the telemetry lock is written by every
/// delivery.
#[repr(align(128))]
#[derive(Default)]
struct CpuObserved(AtomicBool);

pub struct ServerRuntime {
    config: RwLock<ConfigSnapshot>,
    hooks: Arc<dyn ServerHooks>,
    telemetry: Mutex<telemetry::Telemetry>,
    /// Written only under `telemetry`. Set before any HOTKEYS command runs,
    /// so a starting session measures every CPU window that begins after its
    /// start time and process CPU snapshot; cleared once no session tracks.
    cpu_observed: CpuObserved,
    persistence: RwLock<PersistenceStats>,
    monitor: broadcast::Sender<Bytes>,
    extensions: Arc<extensions::Extensions>,
    boot: Instant,
}
impl ServerRuntime {
    pub fn new(config: ConfigSnapshot, hooks: Arc<dyn ServerHooks>) -> Self {
        let (monitor, _) = broadcast::channel(1024);
        Self {
            telemetry: Mutex::new(telemetry::Telemetry::new(&config)),
            cpu_observed: CpuObserved::default(),
            config: RwLock::new(config),
            hooks,
            persistence: RwLock::new(PersistenceStats::default()),
            monitor,
            extensions: Arc::new(extensions::Extensions::new()),
            boot: Instant::now(),
        }
    }
    pub fn extensions(&self) -> &extensions::Extensions {
        &self.extensions
    }
    pub fn shared_extensions(&self) -> Arc<extensions::Extensions> {
        self.extensions.clone()
    }
    pub fn monitor(&self) -> broadcast::Receiver<Bytes> {
        self.monitor.subscribe()
    }
    /// Whether a HOTKEYS session may report command CPU time. Commands
    /// measure their thread CPU windows only while this or another reporter
    /// (`State::cpu_observed`) may read them.
    pub fn cpu_observed(&self) -> bool {
        self.cpu_observed.0.load(Ordering::Relaxed)
    }
    /// Republishes `cpu_observed` from `telemetry`, held by the caller.
    fn publish_cpu_observed(&self, telemetry: &telemetry::Telemetry) {
        let active = telemetry.hotkeys_active();
        if self.cpu_observed.0.load(Ordering::Relaxed) != active {
            self.cpu_observed.0.store(active, Ordering::Relaxed);
        }
    }
    pub fn record_persistence(&self, stats: PersistenceStats) {
        *self.persistence.write().unwrap_or_else(|p| p.into_inner()) = stats;
    }
    pub(crate) fn reset_statistics(&self) {
        self.telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reset_statistics();
    }
    /// Observes the commands one flush delivered, in delivery order: their
    /// MONITOR lines first, outside the telemetry lock, then every observation
    /// under one telemetry lock, so they all adopt the same configuration
    /// publication. Keys a caller must resolve (dynamic modules) are resolved
    /// before this call; builtin HOTKEYS keys may still be derived inside.
    /// Takes no configuration lock: telemetry holds typed copies of the
    /// observation parameters, refreshed by each configuration publication.
    pub fn observe(&self, observations: &[Observation<'_>]) {
        if self.monitor.receiver_count() != 0 {
            for observation in observations {
                if crate::engine::BoundCommand::bind(observation.args).monitor_allowed() {
                    let _ = self.monitor.send(telemetry::monitor_line(observation));
                }
            }
        }
        let mut telemetry = self.telemetry.lock().unwrap_or_else(|p| p.into_inner());
        for observation in observations {
            telemetry.observe(observation);
        }
        // An observation may notice a session's duration ended.
        self.publish_cpu_observed(&telemetry);
    }
    pub fn config_snapshot(&self) -> ConfigSnapshot {
        self.config
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }
    pub(crate) fn local_parameter(name: &str) -> bool {
        telemetry::runtime_parameter(name)
    }
    /// Lock order is configuration, then telemetry: observations adopt the
    /// whole publication before it is released and CONFIG SET replies.
    pub(crate) fn publish_configuration(&self, changes: Vec<(String, Bytes)>) {
        let mut config = self.config.write().unwrap_or_else(|p| p.into_inner());
        for (name, value) in changes {
            config
                .parameters
                .get_mut(&name)
                .expect("validated configuration")
                .value = value;
        }
        self.telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .configure(&config);
    }
    pub(crate) fn config_number(&self, name: &str, default: i64) -> i64 {
        self.config
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .parameters
            .get(name)
            .and_then(|v| integer(&v.value))
            .unwrap_or(default)
    }
    async fn action(hooks: &dyn ServerHooks, action: ServerAction) -> Resp {
        match hooks.run(action).await {
            Ok(reply) => reply,
            Err(error) => Resp::Error(format!("ERR {error}")),
        }
    }
    /// Caller must authorize first, including all administrative commands. Dynamic
    /// extensions require prepare/authorize/execute rather than this trusted path.
    /// `None` when `command` is not a server command.
    pub(crate) async fn execute(
        &self,
        command: &crate::engine::BoundCommand<'_>,
        context: CommandContext,
    ) -> Option<Resp> {
        self.execute_with_hooks(command, context, self.hooks.as_ref())
            .await
    }
    pub(crate) async fn execute_with_hooks(
        &self,
        command: &crate::engine::BoundCommand<'_>,
        context: CommandContext,
        hooks: &dyn ServerHooks,
    ) -> Option<Resp> {
        let args = command.args();
        let kind = match command.server_command() {
            Some(kind) => kind,
            // Trusted direct callers can use controls absent from the wire
            // registry. Preserve their dispatch without registering new commands.
            None if command.root_name().is_none() => ServerCommand::of(args.first()?)?,
            None => return None,
        };
        let reply = match kind {
            ServerCommand::Module => return self.extensions.execute(args).await,
            ServerCommand::Time if args.len() == 1 => {
                let elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                array(vec![
                    bulk(elapsed.as_secs().to_string()),
                    bulk(elapsed.subsec_micros().to_string()),
                ])
            }
            ServerCommand::Time => arity("time"),
            ServerCommand::LastSave if args.len() == 1 => Resp::Integer(
                self.persistence
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_save_time,
            ),
            ServerCommand::LastSave => arity("lastsave"),
            ServerCommand::Role if args.len() == 1 => Self::action(hooks, ServerAction::Role).await,
            ServerCommand::Role => arity("role"),
            ServerCommand::Info => self.info(args, hooks).await,
            ServerCommand::Save if args.len() == 1 => {
                Self::action(
                    hooks,
                    ServerAction::Save {
                        background: false,
                        schedule: false,
                    },
                )
                .await
            }
            ServerCommand::Save => arity("save"),
            ServerCommand::BgSave
                if args.len() == 1
                    || args.len() == 2 && args[1].eq_ignore_ascii_case(b"SCHEDULE") =>
            {
                Self::action(
                    hooks,
                    ServerAction::Save {
                        background: true,
                        schedule: args.len() == 2,
                    },
                )
                .await
            }
            ServerCommand::BgSave => syntax(),
            ServerCommand::BgRewriteAof if args.len() == 1 => {
                Self::action(hooks, ServerAction::RewriteAof).await
            }
            ServerCommand::BgRewriteAof => arity("bgrewriteaof"),
            ServerCommand::Backup => self.backup(args, hooks).await,
            ServerCommand::Config => self.configure(args, hooks).await,
            ServerCommand::Debug => {
                Self::action(
                    hooks,
                    ServerAction::Debug(
                        args[1..]
                            .iter()
                            .map(|arg| Bytes::copy_from_slice(arg))
                            .collect(),
                    ),
                )
                .await
            }
            ServerCommand::Memory => self.memory(args, context.database, hooks).await,
            ServerCommand::Latency => self
                .telemetry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .latency_command(args),
            ServerCommand::Slowlog => self
                .telemetry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .slowlog_command(args),
            ServerCommand::Hotkeys => {
                let cluster = self
                    .config
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .parameters
                    .get("cluster-enabled")
                    .is_some_and(|p| p.value.eq_ignore_ascii_case(b"yes"));
                let mut telemetry = self.telemetry.lock().unwrap_or_else(|p| p.into_inner());
                // Raised before a session can start, so no CPU window that
                // begins after its start time and process CPU snapshot goes
                // unmeasured; settled to the outcome before the lock drops.
                self.cpu_observed.0.store(true, Ordering::Relaxed);
                let reply = telemetry.hotkeys_command(args, cluster);
                self.publish_cpu_observed(&telemetry);
                reply
            }
            ServerCommand::Monitor if args.len() == 1 => Resp::Simple("OK"),
            ServerCommand::Monitor => arity("monitor"),
            ServerCommand::Shutdown => self.shutdown(args, hooks).await,
            ServerCommand::Lolwut => {
                if !(args.len() == 1
                    || args.len() == 3
                        && args[1].eq_ignore_ascii_case(b"VERSION")
                        && integer(&args[2]).is_some())
                {
                    syntax()
                } else {
                    bulk(format!(
                        "   /\\_/\\\n  ( o.o )  ffkv\n   > ^ <\n\nffkv {} — Redis 8.10 command compatibility\n",
                        env!("CARGO_PKG_VERSION")
                    ))
                }
            }
        };
        Some(reply)
    }
    async fn info(&self, args: &[Bytes], hooks: &dyn ServerHooks) -> Resp {
        let sections: Vec<String> = args[1..]
            .iter()
            .map(|v| String::from_utf8_lossy(v).to_ascii_lowercase())
            .collect();
        let hook = Self::action(hooks, ServerAction::Info(sections.clone())).await;
        let mut output = match hook {
            Resp::Bulk(Some(bytes)) | Resp::Status(bytes) => bytes.to_vec(),
            Resp::Error(_) => return hook,
            _ => return Resp::Error("ERR INFO hook returned a non-text result".into()),
        };
        let wants = |section: &str| {
            sections.is_empty()
                || sections
                    .iter()
                    .any(|v| v == section || v == "all" || v == "everything" || v == "default")
        };
        if wants("persistence") {
            let p = self.persistence.read().unwrap_or_else(|p| p.into_inner());
            output.extend_from_slice(format!("\r\n# Persistence\r\nrdb_changes_since_last_save:{}\r\nrdb_bgsave_in_progress:{}\r\nrdb_last_save_time:{}\r\nrdb_last_bgsave_status:{}\r\naof_rewrite_in_progress:{}\r\naof_last_bgrewrite_status:{}\r\naof_current_size:{}\r\n", p.changes_since_save, u8::from(p.save_in_progress), p.last_save_time, if p.last_save_ok { "ok" } else { "err" }, u8::from(p.rewrite_in_progress), if p.last_rewrite_ok { "ok" } else { "err" }, p.wal_bytes).as_bytes());
        }
        if wants("stats") {
            let t = self.telemetry.lock().unwrap_or_else(|p| p.into_inner());
            output.extend_from_slice(format!("\r\n# Stats\r\ntotal_commands_processed:{}\r\ntotal_error_replies:{}\r\ntotal_net_input_bytes:{}\r\ntotal_net_output_bytes:{}\r\nuptime_in_seconds:{}\r\n", t.commands, t.errors, t.input_bytes, t.output_bytes, self.boot.elapsed().as_secs()).as_bytes());
        }
        bulk(Bytes::from(output))
    }
    async fn backup(&self, args: &[Bytes], hooks: &dyn ServerHooks) -> Resp {
        if args.len() != 2 {
            return arity("backup");
        }
        let action = match args[1].to_ascii_uppercase().as_slice() {
            b"START" => BackupAction::Start,
            b"STATUS" => BackupAction::Status,
            b"LIST" => BackupAction::List,
            b"SEAL" => BackupAction::Seal,
            b"ABORT" => BackupAction::Abort,
            b"CLEANUP" => BackupAction::Cleanup,
            _ => return Resp::Error("ERR unknown BACKUP subcommand".into()),
        };
        Self::action(hooks, ServerAction::Backup(action)).await
    }
    async fn configure(&self, args: &[Bytes], hooks: &dyn ServerHooks) -> Resp {
        let Some(sub) = args.get(1) else {
            return arity("config");
        };
        if sub.eq_ignore_ascii_case(b"GET") {
            if args.len() < 3 {
                return arity("config|get");
            }
            let config = self.config.read().unwrap_or_else(|p| p.into_inner());
            let mut patterns = Vec::new();
            for pattern in &args[2..] {
                match crate::auth::Glob::compile(pattern) {
                    Ok(glob) => patterns.push(glob),
                    Err(_) => return Resp::Map(Vec::new()),
                }
            }
            let mut work = 8 * 1024 * 1024;
            return Resp::Map(
                config
                    .parameters
                    .iter()
                    .filter(|(name, _)| {
                        patterns
                            .iter()
                            .any(|p| p.matches(name.as_bytes(), &mut work).unwrap_or(false))
                    })
                    .map(|(name, p)| (bulk(name.clone()), bulk(p.value.clone())))
                    .collect(),
            );
        }
        if sub.eq_ignore_ascii_case(b"SET") {
            if args.len() < 4 || !args.len().is_multiple_of(2) {
                return arity("config|set");
            }
            let mut changes = Vec::new();
            {
                let config = self.config.read().unwrap_or_else(|p| p.into_inner());
                for pair in args[2..].as_chunks::<2>().0 {
                    let name = String::from_utf8_lossy(&pair[0]).to_ascii_lowercase();
                    if changes.iter().any(|(existing, _)| existing == &name) {
                        return Resp::Error(format!("ERR duplicate parameter '{name}'"));
                    }
                    let Some(parameter) = config.parameters.get(&name) else {
                        return Resp::Error(format!(
                            "ERR Unknown option or number of arguments for CONFIG SET - '{name}'"
                        ));
                    };
                    if !parameter.mutable {
                        return Resp::Error(format!(
                            "ERR CONFIG SET failed (possibly related to argument '{name}') - can't set immutable config"
                        ));
                    }
                    let valid = match parameter.kind {
                        ConfigKind::Text => true,
                        ConfigKind::Unsigned { min, max } => {
                            unsigned(&pair[1]).is_some_and(|n| n >= min && n <= max)
                        }
                        ConfigKind::Signed { min, max } => {
                            integer(&pair[1]).is_some_and(|n| n >= min && n <= max)
                        }
                        ConfigKind::Boolean => {
                            pair[1].eq_ignore_ascii_case(b"yes")
                                || pair[1].eq_ignore_ascii_case(b"no")
                        }
                    };
                    if !valid {
                        return Resp::Error(format!(
                            "ERR Invalid argument '{}' for CONFIG SET '{name}'",
                            String::from_utf8_lossy(&pair[1])
                        ));
                    }
                    changes.push((name, Bytes::copy_from_slice(&pair[1])));
                }
            }
            // The actor applies and publishes every change together. In
            // particular, do not hold a parser lock while awaiting the actor:
            // an EXEC may already own it and execute CONFIG SET itself.
            return Self::action(hooks, ServerAction::Configure(changes)).await;
        }
        if sub.eq_ignore_ascii_case(b"RESETSTAT") {
            if args.len() != 2 {
                return arity("config|resetstat");
            }
            let reply = Self::action(hooks, ServerAction::ResetStatistics).await;
            return reply;
        }
        if sub.eq_ignore_ascii_case(b"REWRITE") {
            if args.len() != 2 {
                return arity("config|rewrite");
            }
            let snapshot = self.config_snapshot();
            let Some(path) = snapshot.rewrite_path else {
                return Resp::Error("ERR The server is running without a config file".into());
            };
            let values: BTreeMap<_, _> = snapshot
                .parameters
                .into_iter()
                .map(|(name, p)| (name, p.value.to_vec()))
                .collect();
            let result = tokio::task::spawn_blocking(move || -> io::Result<()> {
                use std::io::Write;
                let mut bytes = serde_json::to_vec(&values).map_err(io::Error::other)?;
                bytes.push(b'\n');
                let parent = path.parent().unwrap_or(Path::new("."));
                let temp = parent.join(format!(
                    ".ffkv-config-{}-{}.tmp",
                    std::process::id(),
                    rand::random::<u64>()
                ));
                let result = (|| {
                    let mut options = std::fs::OpenOptions::new();
                    options.write(true).create_new(true);
                    #[cfg(unix)]
                    {
                        use std::os::unix::fs::OpenOptionsExt;
                        options.mode(0o600);
                    }
                    let mut file = options.open(&temp)?;
                    file.write_all(&bytes)?;
                    file.sync_all()?;
                    std::fs::rename(&temp, &path)?;
                    std::fs::File::open(parent)?.sync_all()
                })();
                if result.is_err() {
                    let _ = std::fs::remove_file(&temp);
                }
                result
            })
            .await;
            return match result {
                Ok(Ok(())) => Resp::Simple("OK"),
                Ok(Err(e)) => Resp::Error(format!("ERR Rewriting config file: {e}")),
                Err(e) => Resp::Error(format!("ERR Rewriting config file: {e}")),
            };
        }
        Resp::Error("ERR unknown CONFIG subcommand".into())
    }
    async fn memory(&self, args: &[Bytes], database: u32, hooks: &dyn ServerHooks) -> Resp {
        let Some(sub) = args.get(1) else {
            return arity("memory");
        };
        if sub.eq_ignore_ascii_case(b"USAGE") {
            if args.len() != 3 && args.len() != 5 {
                return arity("memory|usage");
            }
            let samples = if args.len() == 5 {
                if !args[3].eq_ignore_ascii_case(b"SAMPLES") {
                    return syntax();
                }
                let Some(value) = unsigned(&args[4]).and_then(|n| usize::try_from(n).ok()) else {
                    return integer_error();
                };
                value
            } else {
                5
            };
            return Self::action(
                hooks,
                ServerAction::MemoryUsage {
                    database,
                    key: Bytes::copy_from_slice(&args[2]),
                    samples,
                },
            )
            .await;
        }
        if args.len() != 2 {
            return arity("memory");
        }
        if sub.eq_ignore_ascii_case(b"STATS") {
            return Self::action(hooks, ServerAction::MemoryStatistics).await;
        }
        if sub.eq_ignore_ascii_case(b"DOCTOR") {
            let stats = Self::action(hooks, ServerAction::MemoryStatistics).await;
            return match stats {
                Resp::Error(_) => stats,
                Resp::Map(fields) => {
                    let mut text =
                        String::from("ffkv memory diagnostics (Rust system allocator):\n");
                    for (key, value) in fields {
                        if let Resp::Bulk(Some(key)) = key
                            && let Resp::Integer(n) = value
                        {
                            text.push_str(&format!("{}: {n}\n", String::from_utf8_lossy(&key)));
                        }
                    }
                    text.push_str("Key/value accounting excludes allocator-private metadata and process code. Use process RSS alongside dataset bytes to assess overhead.\n");
                    bulk(text)
                }
                _ => Resp::Error("ERR memory statistics hook returned an invalid result".into()),
            };
        }
        if sub.eq_ignore_ascii_case(b"MALLOC-STATS") {
            return bulk(
                "Rust System allocator; jemalloc statistics are not available. MEMORY STATS reports ffkv dataset accounting, not allocator-private counters.",
            );
        }
        if sub.eq_ignore_ascii_case(b"PURGE") {
            return purge_allocator();
        }
        Resp::Error("ERR unknown MEMORY subcommand".into())
    }
    async fn shutdown(&self, args: &[Bytes], hooks: &dyn ServerHooks) -> Resp {
        let (mut save, mut now, mut force, mut abort) = (None, false, false, false);
        for arg in &args[1..] {
            if arg.eq_ignore_ascii_case(b"SAVE") && save.is_none() {
                save = Some(true);
            } else if arg.eq_ignore_ascii_case(b"NOSAVE") && save.is_none() {
                save = Some(false);
            } else if arg.eq_ignore_ascii_case(b"NOW") && !now {
                now = true;
            } else if arg.eq_ignore_ascii_case(b"FORCE") && !force {
                force = true;
            } else if arg.eq_ignore_ascii_case(b"ABORT") && !abort {
                abort = true;
            } else {
                return syntax();
            }
        }
        if abort && (save.is_some() || now || force) {
            return syntax();
        }
        Self::action(
            hooks,
            ServerAction::Shutdown {
                save,
                now,
                force,
                abort,
            },
        )
        .await
    }
}
fn purge_allocator() -> Resp {
    #[cfg(target_os = "linux")]
    {
        unsafe {
            libc::malloc_trim(0);
        }
        Resp::Simple("OK")
    }
    #[cfg(target_os = "macos")]
    {
        unsafe extern "C" {
            fn malloc_zone_pressure_relief(zone: *mut std::ffi::c_void, goal: usize) -> usize;
        }
        unsafe {
            malloc_zone_pressure_relief(std::ptr::null_mut(), 0);
        }
        Resp::Simple("OK")
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        Resp::Error("ERR memory purging is unavailable with this system allocator".into())
    }
}
pub(super) fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
pub(super) fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
pub(super) fn array(values: Vec<Resp>) -> Resp {
    Resp::Array(Some(values))
}
pub(super) fn arity(name: &str) -> Resp {
    Resp::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}
pub(super) fn syntax() -> Resp {
    Resp::Error("ERR syntax error".into())
}
pub(super) fn integer_error() -> Resp {
    Resp::Error("ERR value is not an integer or out of range".into())
}
pub(super) fn integer(bytes: &[u8]) -> Option<i64> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}
pub(super) fn unsigned(bytes: &[u8]) -> Option<u64> {
    if bytes.first() == Some(&b'-') {
        None
    } else {
        std::str::from_utf8(bytes).ok()?.parse().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    struct NoHooks;
    impl ServerHooks for NoHooks {
        fn run<'a>(&'a self, action: ServerAction) -> HookFuture<'a> {
            panic!("non-hook command dispatched a server action: {action:?}");
        }
    }

    async fn execute(runtime: &ServerRuntime, args: &[&[u8]]) -> Option<Resp> {
        let args: Vec<Bytes> = args.iter().map(|arg| Bytes::copy_from_slice(arg)).collect();
        let command = crate::engine::BoundCommand::bind(&args);
        runtime
            .execute(&command, CommandContext { database: 0 })
            .await
    }

    fn observe(runtime: &ServerRuntime, command: &[u8], elapsed: Duration) {
        let args = [Bytes::copy_from_slice(command)];
        runtime.observe(&[Observation {
            args: &args,
            keys: Some(&[]),
            database: 0,
            client_address: "127.0.0.1:1",
            client_name: b"",
            elapsed,
            cpu_time: Duration::ZERO,
            input_bytes: 0,
            output_bytes: 0,
            failed: false,
        }]);
    }

    /// Publishes validated changes as the actor does after CONFIG SET.
    fn configure(runtime: &ServerRuntime, changes: &[(&str, &str)]) {
        runtime.publish_configuration(
            changes
                .iter()
                .map(|&(name, value)| (name.to_owned(), Bytes::copy_from_slice(value.as_bytes())))
                .collect(),
        );
    }

    /// `LATENCY HISTOGRAM get` calls, or `None` without that histogram.
    async fn get_calls(runtime: &ServerRuntime) -> Option<i64> {
        let reply = execute(runtime, &[b"LATENCY", b"HISTOGRAM", b"get"]).await;
        let Some(Resp::Map(histograms)) = reply else {
            panic!("histogram reply {reply:?}");
        };
        match histograms.as_slice() {
            [] => None,
            [(name, Resp::Map(detail))] if *name == bulk("get") => match detail.first() {
                Some((_, Resp::Integer(calls))) => Some(*calls),
                other => panic!("histogram calls {other:?}"),
            },
            other => panic!("histograms {other:?}"),
        }
    }

    /// Worst latency the monitor recorded for the `command` event, if any.
    async fn worst_command_latency(runtime: &ServerRuntime) -> Option<i64> {
        let reply = execute(runtime, &[b"LATENCY", b"LATEST"]).await;
        let Some(Resp::Array(Some(events))) = reply else {
            panic!("latency reply {reply:?}");
        };
        match events.as_slice() {
            [] => None,
            [Resp::Array(Some(event))] => match event.as_slice() {
                [name, _, _, Resp::Integer(worst)] if *name == bulk("command") => Some(*worst),
                other => panic!("latency event {other:?}"),
            },
            other => panic!("latency events {other:?}"),
        }
    }

    /// Logged command names from `SLOWLOG GET -1`, newest first.
    async fn slowlog(runtime: &ServerRuntime) -> Vec<String> {
        let reply = execute(runtime, &[b"SLOWLOG", b"GET", b"-1"]).await;
        let Some(Resp::Array(Some(entries))) = reply else {
            panic!("slowlog reply {reply:?}");
        };
        entries
            .iter()
            .map(|entry| {
                if let Resp::Array(Some(fields)) = entry
                    && let Some(Resp::Array(Some(args))) = fields.get(3)
                    && let [Resp::Bulk(Some(command))] = args.as_slice()
                {
                    String::from_utf8(command.to_vec()).unwrap()
                } else {
                    panic!("slowlog entry {entry:?}");
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn builtin_names_match_ascii_case_insensitively() {
        let runtime = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        assert_eq!(
            execute(&runtime, &[b"bgrewriteaof", b"now"]).await,
            Some(arity("bgrewriteaof"))
        );
        assert_eq!(
            execute(&runtime, &[b"mOnItOr"]).await,
            Some(Resp::Simple("OK"))
        );
        assert_eq!(
            execute(&runtime, &[b"Time", b"extra"]).await,
            Some(arity("time"))
        );
    }

    #[tokio::test]
    async fn non_builtin_names_fall_through_without_running_hooks() {
        let runtime = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        let long = vec![b'S'; 4096];
        for name in [
            b"BGREWRITEAOFX".as_slice(),
            b"BGREWRITEAO",
            b"TIME\xff",
            b"\xffTIME",
            "tıme".as_bytes(),
            b"T\0IME",
            b"",
            b"GET",
            long.as_slice(),
        ] {
            assert_eq!(execute(&runtime, &[name]).await, None, "{name:?}");
        }
        assert_eq!(execute(&runtime, &[]).await, None);
    }

    #[tokio::test]
    async fn published_latency_settings_govern_later_observations_across_statistics_resets() {
        let runtime = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        let ms = Duration::from_millis;
        observe(&runtime, b"GET", ms(50));
        assert_eq!(get_calls(&runtime).await, Some(1));
        assert_eq!(worst_command_latency(&runtime).await, None);

        configure(
            &runtime,
            &[
                ("latency-tracking", "no"),
                ("latency-monitor-threshold", "7"),
            ],
        );
        // CONFIG RESETSTAT clears statistics, never the published settings.
        runtime.reset_statistics();
        observe(&runtime, b"GET", ms(6));
        assert_eq!(worst_command_latency(&runtime).await, None);
        observe(&runtime, b"GET", ms(7));
        assert_eq!(worst_command_latency(&runtime).await, Some(7));
        assert_eq!(get_calls(&runtime).await, None);

        configure(
            &runtime,
            &[
                ("latency-tracking", "yes"),
                ("latency-monitor-threshold", "0"),
            ],
        );
        observe(&runtime, b"GET", ms(50));
        assert_eq!(get_calls(&runtime).await, Some(1));
        assert_eq!(worst_command_latency(&runtime).await, Some(7));
    }

    #[tokio::test]
    async fn slowlog_threshold_is_inclusive_and_length_trims_at_the_next_observation() {
        let runtime = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        let us = Duration::from_micros;
        configure(
            &runtime,
            &[("slowlog-log-slower-than", "100"), ("slowlog-max-len", "3")],
        );
        for (command, elapsed) in [(b"A", 99), (b"B", 100), (b"C", 101), (b"D", 100)] {
            observe(&runtime, command, us(elapsed));
        }
        assert_eq!(slowlog(&runtime).await, ["D", "C", "B"]);

        configure(&runtime, &[("slowlog-max-len", "1")]);
        assert_eq!(slowlog(&runtime).await, ["D", "C", "B"]);
        observe(&runtime, b"E", us(0));
        assert_eq!(slowlog(&runtime).await, ["D"]);

        configure(
            &runtime,
            &[("slowlog-log-slower-than", "-1"), ("slowlog-max-len", "2")],
        );
        observe(&runtime, b"F", Duration::from_secs(1));
        assert_eq!(slowlog(&runtime).await, ["D"]);

        configure(
            &runtime,
            &[("slowlog-log-slower-than", "0"), ("slowlog-max-len", "0")],
        );
        observe(&runtime, b"G", us(0));
        assert!(slowlog(&runtime).await.is_empty());
    }

    #[tokio::test]
    async fn a_delivered_batch_reaches_monitor_and_the_slow_log_in_delivery_order() {
        let runtime = ServerRuntime::new(ConfigSnapshot::default(), Arc::new(NoHooks));
        configure(&runtime, &[("slowlog-log-slower-than", "0")]);
        let mut monitor = runtime.monitor();
        let commands: [&[&str]; 3] = [&["GET", "k"], &["SET", "k", "v"], &["PING"]];
        let commands = commands.map(|args| {
            args.iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect::<Vec<_>>()
        });
        let names: [&[u8]; 3] = [b"first", b"\xffsecond", b""];
        let observations: Vec<Observation<'_>> = commands
            .iter()
            .zip(names)
            .map(|(args, client_name)| Observation {
                args,
                keys: None,
                database: 0,
                client_address: "127.0.0.1:1",
                client_name,
                elapsed: Duration::ZERO,
                cpu_time: Duration::ZERO,
                input_bytes: 0,
                output_bytes: 0,
                failed: false,
            })
            .collect();
        runtime.observe(&observations);
        let lines: [&[u8]; 3] = [b" \"GET\" \"k\"", b" \"SET\" \"k\" \"v\"", b" \"PING\""];
        for expected in lines {
            let line = monitor.try_recv().expect("a MONITOR line per command");
            assert!(line.ends_with(expected), "{line:?}");
        }
        assert!(monitor.try_recv().is_err());

        let reply = execute(&runtime, &[b"SLOWLOG", b"GET", b"-1"]).await;
        let Some(Resp::Array(Some(entries))) = reply else {
            panic!("slowlog reply {reply:?}");
        };
        let logged: Vec<(Resp, Resp)> = entries
            .into_iter()
            .map(|entry| match entry {
                Resp::Array(Some(mut fields)) if fields.len() == 6 => {
                    let name = fields.pop().unwrap();
                    (fields.swap_remove(3), name)
                }
                other => panic!("slowlog entry {other:?}"),
            })
            .collect();
        let command = |args: &[&str]| array(args.iter().map(|arg| bulk(arg.to_string())).collect());
        assert_eq!(
            logged,
            [
                (command(&["PING"]), bulk("")),
                (command(&["SET", "k", "v"]), bulk("\u{FFFD}second")),
                (command(&["GET", "k"]), bulk("first")),
            ]
        );
    }
}
