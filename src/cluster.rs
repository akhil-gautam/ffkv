//! Redis-compatible cluster administration and routing over an ffkv-owned peer bus.
//! The peer protocol is deliberately not represented as Redis cluster-bus interoperability.
mod bus;
mod hooks;
pub use hooks::ClusterCommandHooks;
mod commands;
mod migration;
mod snapshot;
mod source;
mod topology;

use crate::protocol::Resp;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    future::Future,
    io,
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{Mutex, Notify, RwLock};

pub const SLOT_COUNT: usize = 16_384;
pub type ClusterFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// The payload is a parent's native, resolved key snapshot, including absolute TTL.
/// It is transferred without interpreting or re-executing the originating request.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterKey {
    pub key: Vec<u8>,
    pub payload: Vec<u8>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterPage {
    pub keys: Vec<ClusterKey>,
    pub next: Option<Vec<u8>>,
}
pub struct ClusterChanges {
    pub through: u64,
    pub bytes: Vec<u8>,
}
#[derive(Clone, Debug, Default)]
pub struct ReplicationProgress {
    pub history: String,
    pub processed: u64,
    pub primary_offset: u64,
    pub upstream_connected: bool,
}

/// Each dataset operation must run against one coherent all-shard view. Imports,
/// deletion and clearing must journal resolved changes before acknowledging.
/// freeze_writes is a durable processed-offset barrier, not a boolean hint: it
/// waits for in-flight writes, then excludes further user writes until unfreeze.
pub trait ClusterHooks: Send + Sync {
    fn keys(&self, slot: u16, limit: usize) -> ClusterFuture<'_, Vec<Bytes>>;
    fn slot_key_counts(&self) -> ClusterFuture<'_, Vec<u64>>;
    fn exists<'a>(&'a self, keys: &'a [Bytes]) -> ClusterFuture<'a, Vec<bool>>;
    fn database_size(&self) -> ClusterFuture<'_, u64>;
    fn export_slots<'a>(
        &'a self,
        ranges: &'a [(u16, u16)],
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> ClusterFuture<'a, ClusterPage>;
    fn import_keys<'a>(&'a self, keys: &'a [ClusterKey]) -> ClusterFuture<'a, ()>;
    fn delete_slots<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()>;
    fn clear(&self) -> ClusterFuture<'_, ()>;
    fn freeze_writes(&self) -> ClusterFuture<'_, u64>;
    fn unfreeze_writes(&self) -> ClusterFuture<'_, ()>;
    fn follow<'a>(&'a self, host: &'a str, port: u16) -> ClusterFuture<'a, ()>;
    fn promote(&self, required_offset: Option<u64>) -> ClusterFuture<'_, ()>;
    fn replication_progress(&self) -> ClusterFuture<'_, ReplicationProgress>;
    fn publish_shard<'a>(&'a self, channel: &'a [u8], message: &'a [u8]) -> ClusterFuture<'a, ()>;
    fn publish_global<'a>(&'a self, channel: &'a [u8], message: &'a [u8]) -> ClusterFuture<'a, ()>;
    fn migration_changes<'a>(
        &'a self,
        history: &'a str,
        after: u64,
        max_bytes: usize,
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, ClusterChanges>;
    fn apply_migration_changes<'a>(
        &'a self,
        bytes: &'a [u8],
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, ()>;
    fn export_migration_globals<'a>(
        &'a self,
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, Vec<u8>>;
    fn import_migration_globals<'a>(
        &'a self,
        payload: &'a [u8],
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, ()>;
    fn finish_migration<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()>;
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub config_file: PathBuf,
    pub bind: String,
    pub announce_ip: String,
    pub port: u16,
    pub bus_port: u16,
    pub announce_bus_port: Option<u16>,
    pub node_timeout_ms: u64,
    pub require_full_coverage: bool,
    pub allow_reads_when_down: bool,
    pub bus_secret: String,
    pub cpu_stats: bool,
    pub memory_stats: bool,
    pub network_stats: bool,
}
impl ClusterConfig {
    pub fn new(config_file: PathBuf, port: u16, bus_port: u16) -> Self {
        Self {
            config_file,
            bind: "127.0.0.1".into(),
            announce_ip: "127.0.0.1".into(),
            port,
            bus_port,
            announce_bus_port: None,
            node_timeout_ms: 5_000,
            require_full_coverage: true,
            allow_reads_when_down: false,
            bus_secret: String::new(),
            cpu_stats: false,
            memory_stats: false,
            network_stats: false,
        }
    }
}

#[derive(Default, Debug)]
pub struct ClusterClient {
    pub readonly: bool,
    pub asking: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Node {
    id: String,
    shard_id: String,
    ip: String,
    port: u16,
    bus_port: u16,
    parent: Option<String>,
    epoch: u64,
    revision: u64,
    slots: Vec<(u16, u16)>,
    #[serde(default)]
    offset: u64,
}
#[derive(Clone, Serialize, Deserialize)]
struct Durable {
    version: u32,
    myself: String,
    current_epoch: u64,
    last_vote_epoch: u64,
    nodes: BTreeMap<String, Node>,
    importing: BTreeMap<u16, String>,
    migrating: BTreeMap<u16, String>,
    blacklist: BTreeMap<String, u64>,
    #[serde(default)]
    active_task: Option<migration::Task>,
}
struct LiveNode {
    last_seen: u64,
    first_seen: u64,
    ping_sent: u64,
    reports: HashMap<String, u64>,
    failed: bool,
}
#[derive(Default)]
struct SlotMetrics {
    cpu: AtomicU64,
    bytes_in: AtomicU64,
    bytes_out: AtomicU64,
    memory: AtomicU64,
}
struct LinkInfo {
    node: String,
    direction: &'static str,
    created: u64,
    allocated: usize,
    used: usize,
}
struct Peer {
    stream: Mutex<Option<tokio::net::TcpStream>>,
}
struct State {
    durable: Durable,
    owners: Vec<Option<String>>,
    live: HashMap<String, LiveNode>,
    tasks: Vec<migration::Task>,
    links: HashMap<u64, LinkInfo>,
    pending: Vec<(String, u16)>,
}

pub struct ClusterRuntime {
    config: ClusterConfig,
    hooks: Arc<dyn ClusterHooks>,
    state: RwLock<State>,
    peers: Mutex<HashMap<String, Arc<Peer>>>,
    admin: Mutex<()>,
    persist: Mutex<()>,
    metrics: Vec<SlotMetrics>,
    stopped: AtomicBool,
    wake: Notify,
    sent: AtomicU64,
    received: AtomicU64,
    next_link: AtomicU64,
    pause_owner: Mutex<Option<(String, u64)>>,
    replica_last_connected: AtomicU64,
    jobs: Mutex<tokio::task::JoinSet<()>>,
    stop_signal: tokio::sync::watch::Sender<bool>,
    health: AtomicBool,
}

impl ClusterRuntime {
    pub async fn open(
        config: ClusterConfig,
        hooks: Arc<dyn ClusterHooks>,
    ) -> io::Result<Arc<Self>> {
        if config.port == 0
            || config.bus_port == 0
            || config.announce_bus_port == Some(0)
            || config.node_timeout_ms == 0
            || config.node_timeout_ms > i64::MAX as u64 / 16
            || config.announce_ip.parse::<std::net::IpAddr>().is_err()
        {
            return Err(invalid("invalid cluster address, ports or timeout"));
        }
        let bind: std::net::IpAddr = config
            .bind
            .parse()
            .map_err(|_| invalid("cluster bind must be an IP address"))?;
        if !bind.is_loopback() && config.bus_secret.len() < 32 {
            return Err(invalid(
                "non-loopback cluster bus requires a secret of at least 32 bytes",
            ));
        }
        let durable = match tokio::fs::read(&config.config_file).await {
            Ok(bytes) => {
                if bytes.len() > 16 * 1024 * 1024 {
                    return Err(invalid("cluster config too large"));
                }
                let mut d: Durable =
                    serde_json::from_slice(&bytes).map_err(|e| invalid(&e.to_string()))?;
                topology::validate(&d)?;
                let me = d
                    .nodes
                    .get_mut(&d.myself)
                    .ok_or_else(|| invalid("missing myself"))?;
                me.ip = config.announce_ip.clone();
                me.port = config.port;
                me.bus_port = config.announce_bus_port.unwrap_or(config.bus_port);
                // Open legacy slots are config state, but network migration channels are not.
                d.blacklist.retain(|_, until| *until > clock());
                d
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let id = random_id();
                let node = Node {
                    id: id.clone(),
                    shard_id: random_id(),
                    ip: config.announce_ip.clone(),
                    port: config.port,
                    bus_port: config.announce_bus_port.unwrap_or(config.bus_port),
                    parent: None,
                    epoch: 0,
                    revision: 1,
                    slots: vec![],
                    offset: 0,
                };
                Durable {
                    version: 1,
                    myself: id.clone(),
                    current_epoch: 0,
                    last_vote_epoch: 0,
                    nodes: BTreeMap::from([(id, node)]),
                    importing: BTreeMap::new(),
                    migrating: BTreeMap::new(),
                    blacklist: BTreeMap::new(),
                    active_task: None,
                }
            }
            Err(e) => return Err(e),
        };
        let owners = topology::owners(&durable);
        let now = clock();
        let live = durable
            .nodes
            .keys()
            .map(|id| {
                (
                    id.clone(),
                    LiveNode {
                        last_seen: if id == &durable.myself { now } else { 0 },
                        first_seen: now,
                        ping_sent: 0,
                        reports: HashMap::new(),
                        failed: false,
                    },
                )
            })
            .collect();
        let tasks = durable
            .active_task
            .as_ref()
            .filter(|task| task.operation == "migrate" && task.source == durable.myself)
            .map(|task| {
                let mut recovered = task.clone();
                recovered.state = "failed".into();
                recovered.last_error = "source restarted; migration channels closed".into();
                recovered.end_time = now;
                recovered.running = false;
                vec![recovered]
            })
            .unwrap_or_default();
        let (stop_signal, _) = tokio::sync::watch::channel(false);
        let runtime = Arc::new(Self {
            config,
            hooks,
            state: RwLock::new(State {
                durable,
                owners,
                live,
                tasks,
                links: HashMap::new(),
                pending: vec![],
            }),
            peers: Mutex::new(HashMap::new()),
            admin: Mutex::new(()),
            persist: Mutex::new(()),
            metrics: (0..SLOT_COUNT).map(|_| SlotMetrics::default()).collect(),
            stopped: AtomicBool::new(false),
            wake: Notify::new(),
            sent: AtomicU64::new(0),
            received: AtomicU64::new(0),
            next_link: AtomicU64::new(1),
            pause_owner: Mutex::new(None),
            replica_last_connected: AtomicU64::new(0),
            jobs: Mutex::new(tokio::task::JoinSet::new()),
            stop_signal,
            health: AtomicBool::new(false),
        });
        runtime.save().await?;
        Ok(runtime)
    }

    /// Bind before returning, so startup cannot report a running cluster without a bus.
    pub async fn start(self: &Arc<Self>) -> io::Result<()> {
        self.cleanup_snapshots().await?;
        let listener =
            tokio::net::TcpListener::bind((self.config.bind.as_str(), self.config.bus_port))
                .await?;
        let runtime = self.clone();
        self.spawn_owned(async move {
            runtime.serve(listener).await;
        })
        .await?;
        let runtime = self.clone();
        self.spawn_owned(async move {
            runtime.gossip().await;
        })
        .await?;
        let runtime = self.clone();
        self.spawn_owned(async move {
            runtime.pause_watchdog().await;
        })
        .await?;
        let me = {
            let s = self.state.read().await;
            s.durable.nodes[&s.durable.myself].clone()
        };
        if let Some(parent) = me.parent {
            let p = self.state.read().await.durable.nodes.get(&parent).cloned();
            if let Some(p) = p {
                self.hooks.follow(&p.ip, p.port).await?;
            }
        } else {
            // Crash cleanup never exposes partially imported/unowned keys.
            let unowned = {
                let s = self.state.read().await;
                topology::ranges(
                    (0..SLOT_COUNT)
                        .filter(|slot| {
                            s.owners[*slot].as_deref() != Some(&s.durable.myself)
                                && !s.durable.importing.contains_key(&(*slot as u16))
                        })
                        .map(|slot| slot as u16),
                )
            };
            if !unowned.is_empty() {
                self.hooks.delete_slots(&unowned).await?;
            }
        }
        Ok(())
    }
    async fn spawn_owned<F>(&self, future: F) -> io::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut jobs = self.jobs.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cluster is shutting down",
            ));
        }
        while jobs.try_join_next().is_some() {}
        if jobs.len() >= 512 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "cluster background job limit reached",
            ));
        }
        jobs.spawn(future);
        Ok(())
    }
    /// Stop socket I/O, finish admitted durable hooks, and join every owned job.
    /// The parent must keep actors/persistence alive until this method returns.
    pub async fn shutdown(&self) -> io::Result<()> {
        self.stopped.store(true, Ordering::Release);
        self.stop_signal.send_replace(true);
        self.wake.notify_waiters();
        let mut jobs = {
            let mut owned = self.jobs.lock().await;
            std::mem::take(&mut *owned)
        };
        let mut joined = Ok(());
        while let Some(result) = jobs.join_next().await {
            if let Err(error) = result {
                joined = Err(io::Error::other(error.to_string()));
            }
        }
        self.peers.lock().await.clear();
        let unfreeze = self.hooks.unfreeze_writes().await;
        let cleanup = self.cleanup_snapshots().await;
        let saved = self.save().await;
        joined.and(unfreeze).and(cleanup).and(saved)
    }
    pub async fn command(
        self: &Arc<Self>,
        args: &[Bytes],
        client: &mut ClusterClient,
    ) -> Option<Resp> {
        let hooks = self.hooks.as_ref();
        self.command_with_hooks(args, client, &hooks).await
    }
    /// Execute Redis-permitted transactional controls against a caller-owned paused view.
    pub async fn command_with_hooks(
        self: &Arc<Self>,
        args: &[Bytes],
        client: &mut ClusterClient,
        hooks: &dyn ClusterCommandHooks,
    ) -> Option<Resp> {
        let cmd = args.first()?;
        if cmd.eq_ignore_ascii_case(b"ASKING")
            || cmd.eq_ignore_ascii_case(b"READONLY")
            || cmd.eq_ignore_ascii_case(b"READWRITE")
        {
            if args.len() != 1 {
                return Some(err("wrong number of arguments"));
            }
            if cmd.eq_ignore_ascii_case(b"ASKING") {
                client.asking = true;
            } else {
                client.readonly = cmd.eq_ignore_ascii_case(b"READONLY");
            }
            return Some(Resp::Simple("OK"));
        }
        if !cmd.eq_ignore_ascii_case(b"CLUSTER") {
            return None;
        }
        Some(match self.handle(args, hooks).await {
            Ok(reply) => reply,
            Err(error) => err(&error.to_string()),
        })
    }

    /// Call for every request, including commands without keys: ASKING is one-shot.
    /// Key extraction and transaction/script union validation belong to the caller.
    pub async fn admit(
        &self,
        keys: &[Bytes],
        write: bool,
        asking_command: bool,
        client: &mut ClusterClient,
    ) -> Result<Option<u16>, Resp> {
        self.admit_request(keys, write, asking_command, false, client)
            .await
    }
    pub async fn admit_migrate(
        &self,
        keys: &[Bytes],
        client: &mut ClusterClient,
    ) -> Result<Option<u16>, Resp> {
        self.admit_request(keys, true, false, true, client).await
    }
    async fn admit_request(
        &self,
        keys: &[Bytes],
        write: bool,
        asking_command: bool,
        migrating_command: bool,
        client: &mut ClusterClient,
    ) -> Result<Option<u16>, Resp> {
        let hooks = self.hooks.as_ref();
        self.admit_with_hooks(
            keys,
            write,
            asking_command,
            migrating_command,
            client,
            &hooks,
        )
        .await
    }
    pub async fn admit_with_hooks(
        &self,
        keys: &[Bytes],
        write: bool,
        asking_command: bool,
        migrating_command: bool,
        client: &mut ClusterClient,
        hooks: &dyn ClusterCommandHooks,
    ) -> Result<Option<u16>, Resp> {
        let asking = std::mem::take(&mut client.asking) || asking_command;
        let Some(first) = keys.first() else {
            return Ok(None);
        };
        let slot = key_slot(first);
        if keys.iter().any(|key| key_slot(key) != slot) {
            return Err(Resp::Error(
                "CROSSSLOT Keys in request don't hash to the same slot".into(),
            ));
        }
        let s = self.state.read().await;
        let me = &s.durable.nodes[&s.durable.myself];
        let Some(owner) = s.owners[slot as usize].as_ref() else {
            return Err(Resp::Error("CLUSTERDOWN Hash slot not served".into()));
        };
        if !self.healthy(&s) && (write || !self.config.allow_reads_when_down) {
            return Err(Resp::Error("CLUSTERDOWN The cluster is down".into()));
        }
        let importing = s.durable.importing.contains_key(&slot);
        if importing && asking && me.parent.is_none() {
            drop(s);
            if keys.len() > 1
                && keys.iter().any(|k| k != first)
                && hooks
                    .exists(keys)
                    .await
                    .map_err(io_resp)?
                    .iter()
                    .any(|v| !v)
            {
                return Err(Resp::Error(
                    "TRYAGAIN Multiple keys request during rehashing of slot".into(),
                ));
            }
            return Ok(Some(slot));
        }
        if me.id != *owner {
            if !write && client.readonly && me.parent.as_ref() == Some(owner) {
                return Ok(Some(slot));
            }
            return Err(redirect("MOVED", slot, &s.durable.nodes[owner]));
        }
        let destination = s
            .durable
            .migrating
            .get(&slot)
            .and_then(|id| s.durable.nodes.get(id))
            .cloned();
        drop(s);
        if migrating_command {
            return Ok(Some(slot));
        }
        if let Some(destination) = destination {
            let exists = hooks.exists(keys).await.map_err(io_resp)?;
            let present = exists.iter().filter(|v| **v).count();
            if present == 0 {
                return Err(redirect("ASK", slot, &destination));
            }
            if present != exists.len() {
                return Err(Resp::Error(
                    "TRYAGAIN Multiple keys request during rehashing of slot".into(),
                ));
            }
        }
        Ok(Some(slot))
    }
    /// Whether slot statistics account command CPU time; fixed at startup.
    pub fn cpu_stats(&self) -> bool {
        self.config.cpu_stats
    }
    pub fn record(&self, slot: u16, cpu_usec: u64, bytes_in: u64, bytes_out: u64) {
        let m = &self.metrics[slot as usize];
        if self.config.cpu_stats {
            m.cpu.fetch_add(cpu_usec, Ordering::Relaxed);
        }
        if self.config.network_stats {
            m.bytes_in.fetch_add(bytes_in, Ordering::Relaxed);
            m.bytes_out.fetch_add(bytes_out, Ordering::Relaxed);
        }
    }
    pub fn record_memory(&self, slot: u16, bytes: u64) {
        self.metrics[slot as usize]
            .memory
            .store(bytes, Ordering::Relaxed);
    }
    fn clear_slot_metrics(&self, ranges: &[(u16, u16)]) {
        for (start, end) in ranges {
            for slot in *start..=*end {
                let m = &self.metrics[slot as usize];
                m.cpu.store(0, Ordering::Relaxed);
                m.bytes_in.store(0, Ordering::Relaxed);
                m.bytes_out.store(0, Ordering::Relaxed);
                m.memory.store(0, Ordering::Relaxed);
            }
        }
    }
    pub async fn visible_slots(&self) -> Vec<bool> {
        let s = self.state.read().await;
        let me = &s.durable.nodes[&s.durable.myself];
        s.owners
            .iter()
            .enumerate()
            .map(|(slot, owner)| {
                owner.as_ref() == Some(&me.id)
                    || (me.parent.is_some() && owner.as_ref() == me.parent.as_ref())
                    || s.durable.importing.contains_key(&(slot as u16))
            })
            .collect()
    }
    async fn save(&self) -> io::Result<()> {
        self.refresh_health().await;
        let _serialize = self.persist.lock().await;
        let bytes = {
            let mut s = self.state.write().await;
            if s.durable.nodes[&s.durable.myself].parent.is_none() {
                s.durable.active_task = s.tasks.iter().find(|task| task.active()).cloned();
            }
            serde_json::to_vec(&s.durable).map_err(|e| invalid(&e.to_string()))?
        };
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(invalid("cluster config exceeds persistence limit"));
        }
        let path = self.config.config_file.clone();
        tokio::task::spawn_blocking(move || {
            use std::io::Write;
            let parent = path.parent().unwrap_or_else(|| std::path::Path::new("."));
            std::fs::create_dir_all(parent)?;
            let temporary = path.with_extension("cluster.tmp");
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(&temporary, &path)?;
            std::fs::File::open(parent)?.sync_all()
        })
        .await
        .map_err(|e| io::Error::other(e.to_string()))?
    }
    fn healthy(&self, _state: &State) -> bool {
        self.health.load(Ordering::Acquire)
    }
    async fn refresh_health(&self) {
        let s = self.state.read().await;
        self.health
            .store(self.compute_health(&s), Ordering::Release);
    }
    fn compute_health(&self, s: &State) -> bool {
        if self.config.require_full_coverage && s.owners.iter().any(Option::is_none) {
            return false;
        }
        let now = clock();
        let mut voters = 0;
        let mut reachable = 0;
        for node in s
            .durable
            .nodes
            .values()
            .filter(|node| topology::is_voter(s, node))
        {
            voters += 1;
            if node.id == s.durable.myself
                || s.live.get(&node.id).is_some_and(|live| {
                    now.saturating_sub(live.last_seen) < self.config.node_timeout_ms
                })
            {
                reachable += 1;
            }
        }
        if voters == 0 {
            return !self.config.require_full_coverage;
        }
        reachable > voters / 2
            && !s
                .owners
                .iter()
                .flatten()
                .any(|id| s.live.get(id).is_some_and(|live| live.failed))
    }
}

pub fn key_slot(key: &[u8]) -> u16 {
    let selected = if let Some(open) = key.iter().position(|b| *b == b'{') {
        match key[open + 1..].iter().position(|b| *b == b'}') {
            Some(close) if close > 0 => &key[open + 1..open + 1 + close],
            _ => key,
        }
    } else {
        key
    };
    let mut crc = 0u16;
    for byte in selected {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc & 16383
}
fn clock() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn random_id() -> String {
    let bytes: [u8; 20] = rand::random();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message.to_owned())
}
fn err(message: &str) -> Resp {
    Resp::Error(format!("ERR {message}"))
}
fn io_resp(error: io::Error) -> Resp {
    err(&error.to_string())
}
fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
fn array(value: Vec<Resp>) -> Resp {
    Resp::Array(Some(value))
}
fn redirect(kind: &str, slot: u16, node: &Node) -> Resp {
    let ipv6 = node.ip.contains(':');
    Resp::Error(format!(
        "{kind} {slot} {}{}{}:{}",
        if ipv6 { "[" } else { "" },
        node.ip,
        if ipv6 { "]" } else { "" },
        node.port
    ))
}
fn text(value: &Bytes) -> io::Result<&str> {
    std::str::from_utf8(value).map_err(|_| invalid("invalid argument encoding"))
}
fn number(value: &Bytes) -> io::Result<u64> {
    if value.is_empty() || value.len() > 19 || (value.len() > 1 && value[0] == b'0') {
        return Err(invalid("value is not an integer or out of range"));
    }
    let mut parsed = 0u64;
    for byte in value {
        if !byte.is_ascii_digit() {
            return Err(invalid("value is not an integer or out of range"));
        }
        parsed = parsed
            .checked_mul(10)
            .and_then(|n| n.checked_add(u64::from(*byte - b'0')))
            .ok_or_else(|| invalid("value is not an integer or out of range"))?;
    }
    if parsed > i64::MAX as u64 {
        return Err(invalid("value is not an integer or out of range"));
    }
    Ok(parsed)
}
fn slot_number(value: &Bytes) -> io::Result<u16> {
    let n = number(value)?;
    if n >= SLOT_COUNT as u64 {
        Err(invalid("Invalid or out of range slot"))
    } else {
        Ok(n as u16)
    }
}
