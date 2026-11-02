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
    sync::{Arc, Mutex, RwLock},
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

pub struct ServerRuntime {
    config: RwLock<ConfigSnapshot>,
    hooks: Arc<dyn ServerHooks>,
    telemetry: Mutex<telemetry::Telemetry>,
    persistence: RwLock<PersistenceStats>,
    monitor: broadcast::Sender<Bytes>,
    extensions: Arc<extensions::Extensions>,
    boot: Instant,
}
impl ServerRuntime {
    pub fn new(config: ConfigSnapshot, hooks: Arc<dyn ServerHooks>) -> Self {
        let (monitor, _) = broadcast::channel(1024);
        Self {
            config: RwLock::new(config),
            hooks,
            telemetry: Mutex::new(telemetry::Telemetry::new()),
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
    pub fn record_persistence(&self, stats: PersistenceStats) {
        *self.persistence.write().unwrap_or_else(|p| p.into_inner()) = stats;
    }
    pub(crate) fn reset_statistics(&self) {
        self.telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .reset_statistics();
    }
    pub fn observe(&self, observation: Observation<'_>) {
        let config = self.config.read().unwrap_or_else(|p| p.into_inner());
        if self.monitor.receiver_count() != 0
            && crate::engine::Engine::monitor_allowed(observation.args)
        {
            let _ = self.monitor.send(telemetry::monitor_line(&observation));
        }
        self.telemetry
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .observe(observation, &config);
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
    pub(crate) fn publish_configuration(&self, changes: Vec<(String, Bytes)>) {
        let mut config = self.config.write().unwrap_or_else(|p| p.into_inner());
        for (name, value) in changes {
            config
                .parameters
                .get_mut(&name)
                .expect("validated configuration")
                .value = value;
        }
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
    pub async fn execute(&self, args: &[Bytes], context: CommandContext) -> Option<Resp> {
        self.execute_with_hooks(args, context, self.hooks.as_ref())
            .await
    }
    pub(crate) async fn execute_with_hooks(
        &self,
        args: &[Bytes],
        context: CommandContext,
        hooks: &dyn ServerHooks,
    ) -> Option<Resp> {
        let name = args.first()?;
        if name.eq_ignore_ascii_case(b"MODULE") {
            return self.extensions.execute(args).await;
        }
        let command = String::from_utf8_lossy(name).to_ascii_uppercase();
        let reply = match command.as_str() {
            "TIME" if args.len() == 1 => {
                let elapsed = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default();
                array(vec![
                    bulk(elapsed.as_secs().to_string()),
                    bulk(elapsed.subsec_micros().to_string()),
                ])
            }
            "TIME" => arity("time"),
            "LASTSAVE" if args.len() == 1 => Resp::Integer(
                self.persistence
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .last_save_time,
            ),
            "LASTSAVE" => arity("lastsave"),
            "ROLE" if args.len() == 1 => Self::action(hooks, ServerAction::Role).await,
            "ROLE" => arity("role"),
            "INFO" => self.info(args, hooks).await,
            "SAVE" if args.len() == 1 => {
                Self::action(
                    hooks,
                    ServerAction::Save {
                        background: false,
                        schedule: false,
                    },
                )
                .await
            }
            "SAVE" => arity("save"),
            "BGSAVE"
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
            "BGSAVE" => syntax(),
            "BGREWRITEAOF" if args.len() == 1 => {
                Self::action(hooks, ServerAction::RewriteAof).await
            }
            "BGREWRITEAOF" => arity("bgrewriteaof"),
            "BACKUP" => self.backup(args, hooks).await,
            "CONFIG" => self.configure(args, hooks).await,
            "DEBUG" => {
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
            "MEMORY" => self.memory(args, context.database, hooks).await,
            "LATENCY" => self
                .telemetry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .latency_command(args),
            "SLOWLOG" => self
                .telemetry
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .slowlog_command(args),
            "HOTKEYS" => {
                let cluster = self
                    .config
                    .read()
                    .unwrap_or_else(|p| p.into_inner())
                    .parameters
                    .get("cluster-enabled")
                    .is_some_and(|p| p.value.eq_ignore_ascii_case(b"yes"));
                self.telemetry
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .hotkeys_command(args, cluster)
            }
            "MONITOR" if args.len() == 1 => Resp::Simple("OK"),
            "MONITOR" => arity("monitor"),
            "SHUTDOWN" => self.shutdown(args, hooks).await,
            "LOLWUT" => {
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
            _ => return None,
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
