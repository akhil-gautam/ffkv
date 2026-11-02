//! Coordinator-owned native replication and cluster dataset adapter.
//!
//! Network tasks send typed controls instead of registering synthetic clients.
//! Native replication owns its reconfiguration lease across each backend future;
//! the coordinator additionally checks the generation at the paused commit barrier.
//! Resolved core effects use logical clock zero; opaque module effects retain the
//! source batch timestamp. Expiry holds preserve payloads until source deletion.
//! Stream SELECT is durable auxiliary metadata, independent of checkpoint DB scans.
use crate::engine::{SearchMigrationFilter, SearchMigrationImport};
use crate::{
    cluster::{self, ClusterFuture, ClusterHooks, ClusterKey, ClusterPage, ReplicationProgress},
    engine::{Engine, Execution},
    journal::{Batch, Mutation},
    native_replication::{
        self, Applied, Backend, BackendFuture, NativeReplication, Snapshot, SnapshotRequest,
        Upstream,
    },
    options::Options,
    persistence::Persistence,
    protocol::Resp,
    request::Request,
    scripting::ScriptRunner,
    sharding::{Paused, Pool},
    state::State,
};
use bytes::{BufMut, Bytes, BytesMut};
use std::os::unix::fs::OpenOptionsExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{self, BufReader, BufWriter, Write},
    path::PathBuf,
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::sync::oneshot;

const MAX_QUEUED_BYTES: usize = 64 * 1024 * 1024;
const MAX_QUEUED_COMMANDS: usize = 65_536;
const MAX_SNAPSHOT_LEASES: usize = 4;
const MAX_CLUSTER_PAGE: usize = 4096;
const NATIVE_DATABASE_AUX: &[u8] = b"ffkv.native-database.v1";

#[derive(Clone)]
pub struct Config {
    pub native: native_replication::Config,
    pub cluster: Option<cluster::ClusterConfig>,
    pub fsync_gate: Option<PathBuf>,
}
impl Config {
    pub fn from_options(options: &Options, aof_enabled: bool) -> io::Result<Self> {
        let directory = options
            .data_dir
            .clone()
            .unwrap_or_else(std::env::temp_dir)
            .join(format!(
                "native-staging-{}-{:016x}",
                std::process::id(),
                rand::random::<u64>()
            ));
        let password = options
            .native_replica_password_file
            .as_ref()
            .map(|path| {
                let mut value = fs::read_to_string(path)?;
                while value.ends_with(['\r', '\n']) {
                    value.pop();
                }
                if value.is_empty() {
                    return Err(invalid("empty native replication password"));
                }
                Ok(value)
            })
            .transpose()?;
        let tls = options
            .native_replica_ca_file
            .as_ref()
            .map(|path| {
                let data = fs::read(path)?;
                let mut roots = rustls::RootCertStore::empty();
                let certificates = rustls_pemfile::certs(&mut std::io::Cursor::new(data))
                    .collect::<Result<Vec<_>, _>>()?;
                if certificates.is_empty() {
                    return Err(invalid("empty native replication CA file"));
                }
                for certificate in certificates {
                    roots.add(certificate).map_err(io::Error::other)?;
                }
                let builder = rustls::ClientConfig::builder_with_provider(Arc::new(
                    rustls::crypto::ring::default_provider(),
                ))
                .with_safe_default_protocol_versions()
                .map_err(io::Error::other)?;
                Ok(Arc::new(
                    builder.with_root_certificates(roots).with_no_client_auth(),
                ))
            })
            .transpose()?;
        if options.native_replica_tls_name.is_some() && tls.is_none() {
            return Err(invalid("native TLS name requires a CA file"));
        }
        if options.native_replica_user.is_some() && password.is_none() {
            return Err(invalid("native replication username requires password"));
        }
        let template = Upstream {
            host: "localhost".into(),
            port: options.bind.port(),
            username: options.native_replica_user.clone(),
            password,
            tls,
            tls_server_name: options.native_replica_tls_name.clone(),
        };
        let native = native_replication::Config {
            backlog_bytes: options.replication_backlog_bytes,
            max_snapshot_bytes: options.replica_max_snapshot_bytes,
            staging_directory: directory,
            listening_port: options.bind.port(),
            announce_ip: Some(options.bind.ip().to_string()),
            io_timeout: Duration::from_millis(options.replica_timeout_ms),
            reconnect_delay: Duration::from_millis(options.replica_interval_ms),
            aof_enabled,
            allow_loopback: options.native_replication_allow_loopback,
            allow_private: options.cluster_enabled,
            upstream_template: Some(template),
        };
        let cluster = if options.cluster_enabled {
            let root = options
                .data_dir
                .as_ref()
                .ok_or_else(|| invalid("cluster requires a data directory"))?;
            let bus_port = if options.cluster_bus_port == 0 {
                options
                    .bind
                    .port()
                    .checked_add(10_000)
                    .ok_or_else(|| invalid("cluster bus port overflows"))?
            } else {
                options.cluster_bus_port
            };
            let mut config = cluster::ClusterConfig::new(
                options
                    .cluster_config_file
                    .clone()
                    .unwrap_or_else(|| root.join("nodes.json")),
                options.bind.port(),
                bus_port,
            );
            config.bind = options.bind.ip().to_string();
            config.announce_ip = options.bind.ip().to_string();
            config.node_timeout_ms = options.cluster_node_timeout_ms;
            config.require_full_coverage = options.cluster_require_full_coverage;
            config.allow_reads_when_down = options.cluster_allow_reads_when_down;
            config.bus_secret = options.cluster_bus_secret.clone().unwrap_or_default();
            config.announce_bus_port = options.cluster_announce_bus_port;
            config.cpu_stats = options.cluster_slot_stats_cpu;
            config.memory_stats = options.cluster_slot_stats_memory;
            config.network_stats = options.cluster_slot_stats_network;
            Some(config)
        } else {
            None
        };
        Ok(Self {
            native,
            cluster,
            fsync_gate: options.replication_fsync_gate.clone(),
        })
    }
}

pub struct Runtime {
    state: Weak<State>,
    config: Config,
    source_gate: crate::write_gate::WriteGate,
}
impl Runtime {
    pub fn new(state: &Arc<State>, config: Config) -> Arc<Self> {
        Arc::new(Self {
            state: Arc::downgrade(state),
            config,
            source_gate: crate::write_gate::WriteGate::default(),
        })
    }
    fn state(&self) -> io::Result<Arc<State>> {
        self.state.upgrade().ok_or_else(stopped)
    }
    async fn send<T>(
        &self,
        make: impl FnOnce(oneshot::Sender<io::Result<T>>) -> Control,
    ) -> io::Result<T> {
        let state = self.state()?;
        let sender = state.control_tx.get().ok_or_else(stopped)?;
        let (reply, receive) = oneshot::channel();
        sender
            .send(Request::Native(make(reply)))
            .await
            .map_err(|_| stopped())?;
        receive.await.map_err(|_| stopped())?
    }
    /// Start for a configured fsync fixture after State.native is populated;
    /// retain and abort this task at shutdown.
    /// Removing a fixture gate never manufactures durability: Sync performs fsync.
    pub fn start_fsync(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            loop {
                interval.tick().await;
                let Some(runtime) = weak.upgrade() else {
                    return;
                };
                let Ok(state) = runtime.state() else {
                    return;
                };
                let Some(native) = state.native.get() else {
                    continue;
                };
                let generation = native.generation();
                let Ok(_generation) = native.generation_guard(generation).await else {
                    continue;
                };
                if let Ok(offset) = runtime
                    .send(|reply| Control::Sync { generation, reply })
                    .await
                    && native.generation() == generation
                    && offset <= native.status().offset
                {
                    let _ = native.mark_fsynced(offset);
                }
            }
        })
    }
    pub fn native_config(&self) -> native_replication::Config {
        self.config.native.clone()
    }
    pub fn cluster_config(&self) -> Option<cluster::ClusterConfig> {
        self.config.cluster.clone()
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = fs::remove_dir(&self.config.native.staging_directory);
    }
}

pub enum Control {
    Snapshot {
        request: SnapshotRequest,
        directory: PathBuf,
        max_bytes: u64,
        reply: oneshot::Sender<io::Result<Snapshot>>,
    },
    Install {
        file: StagedFile,
        history: String,
        offset: u64,
        generation: u64,
        max_bytes: u64,
        reply: oneshot::Sender<io::Result<Applied>>,
    },
    Apply {
        command: Vec<Bytes>,
        end_offset: u64,
        generation: u64,
        reply: oneshot::Sender<io::Result<Applied>>,
    },
    Sync {
        generation: u64,
        reply: oneshot::Sender<io::Result<u64>>,
    },
    Keys {
        slot: u16,
        limit: usize,
        reply: oneshot::Sender<io::Result<Vec<Bytes>>>,
    },
    Counts {
        reply: oneshot::Sender<io::Result<Vec<u64>>>,
    },
    Exists {
        keys: Vec<Bytes>,
        reply: oneshot::Sender<io::Result<Vec<bool>>>,
    },
    Size {
        reply: oneshot::Sender<io::Result<u64>>,
    },
    Export {
        ranges: Vec<(u16, u16)>,
        after: Option<Vec<u8>>,
        limit: usize,
        reply: oneshot::Sender<io::Result<ClusterPage>>,
    },
    Import {
        keys: Vec<ClusterKey>,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Delete {
        ranges: Vec<(u16, u16)>,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Clear {
        reply: oneshot::Sender<io::Result<()>>,
    },
    Barrier {
        reply: oneshot::Sender<io::Result<u64>>,
    },
    ReplicationState {
        generation: u64,
        replica: bool,
        ready: bool,
        reply: oneshot::Sender<io::Result<()>>,
    },
    ExportGlobals {
        ranges: Vec<(u16, u16)>,
        reply: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    ImportGlobals {
        ranges: Vec<(u16, u16)>,
        payload: Bytes,
        reply: oneshot::Sender<io::Result<()>>,
    },
    Changes {
        history: String,
        after: u64,
        max_bytes: usize,
        ranges: Vec<(u16, u16)>,
        reply: oneshot::Sender<io::Result<cluster::ClusterChanges>>,
    },
    ApplyChanges {
        bytes: Bytes,
        ranges: Vec<(u16, u16)>,
        reply: oneshot::Sender<io::Result<()>>,
    },
    FinishMigration {
        ranges: Vec<(u16, u16)>,
        reply: oneshot::Sender<io::Result<()>>,
    },
}
impl Control {
    pub(crate) fn mutates_dataset(&self) -> bool {
        matches!(
            self,
            Self::Install { .. }
                | Self::Apply { .. }
                | Self::Import { .. }
                | Self::Delete { .. }
                | Self::Clear { .. }
                | Self::ImportGlobals { .. }
                | Self::ApplyChanges { .. }
                | Self::FinishMigration { .. }
        )
    }
}

type SourceFunctionImage = (String, Vec<(u16, u16)>, Bytes);

#[derive(Default)]
pub struct ReplayState {
    generation: Option<u64>,
    database: u32,
    transaction: Option<Vec<Vec<Bytes>>>,
    queued_bytes: usize,
    committed_offset: u64,
    fsynced_offset: u64,
    pub fsync_gate: Option<PathBuf>,
    source_search: Option<(Vec<(u16, u16)>, SearchMigrationFilter)>,
    destination_search: Option<(Vec<(u16, u16)>, SearchMigrationImport)>,
    source_functions: Option<SourceFunctionImage>,
}
impl ReplayState {
    pub fn new(fsync_gate: Option<PathBuf>) -> Self {
        Self {
            fsync_gate,
            ..Self::default()
        }
    }
    fn select_generation(&mut self, generation: u64) {
        if self.generation != Some(generation) {
            self.generation = Some(generation);
            self.database = 0;
            self.transaction = None;
            self.queued_bytes = 0;
            self.committed_offset = 0;
            self.fsynced_offset = 0;
        }
    }
    fn gate_closed(&self) -> io::Result<bool> {
        let Some(path) = &self.fsync_gate else {
            return Ok(false);
        };
        match fs::metadata(path) {
            Ok(_) => {
                let mut name = path.as_os_str().to_os_string();
                name.push(".entered");
                match OpenOptions::new()
                    .create_new(true)
                    .write(true)
                    .mode(0o600)
                    .open(PathBuf::from(name))
                {
                    Ok(file) => file.sync_all()?,
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
                Ok(true)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(error),
        }
    }
}
pub struct Context<'a> {
    pub pool: &'a Pool,
    pub persistence: &'a Persistence,
    pub scripts: &'a mut ScriptRunner,
    pub state: &'a State,
    pub replay: &'a mut ReplayState,
}
fn native(state: &State) -> io::Result<&Arc<NativeReplication>> {
    state.native.get().ok_or_else(stopped)
}
fn check_generation(state: &State, generation: u64) -> io::Result<()> {
    if native(state)?.generation() != generation {
        Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "stale native replication generation",
        ))
    } else {
        Ok(())
    }
}
fn stopped() -> io::Error {
    io::Error::new(
        io::ErrorKind::BrokenPipe,
        "native runtime coordinator stopped",
    )
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_owned())
}
fn reply_result(reply: Resp) -> io::Result<()> {
    if let Resp::Error(message) = reply {
        Err(invalid(&message))
    } else {
        Ok(())
    }
}

impl Backend for Runtime {
    fn snapshot(&self, request: SnapshotRequest) -> BackendFuture<'_, Snapshot> {
        Box::pin(async move {
            self.send(|reply| Control::Snapshot {
                request,
                directory: self.config.native.staging_directory.clone(),
                max_bytes: self.config.native.max_snapshot_bytes,
                reply,
            })
            .await
        })
    }
    fn install_snapshot(
        &self,
        file: PathBuf,
        history: String,
        offset: u64,
        generation: u64,
    ) -> BackendFuture<'_, Applied> {
        let file = StagedFile(file);
        Box::pin(async move {
            let _permit = self.source_gate.admit().await;
            self.send(|reply| Control::Install {
                file,
                history,
                offset,
                generation,
                max_bytes: self.config.native.max_snapshot_bytes,
                reply,
            })
            .await
        })
    }
    fn apply(
        &self,
        command: Vec<Bytes>,
        end_offset: u64,
        generation: u64,
    ) -> BackendFuture<'_, Applied> {
        Box::pin(async move {
            let _permit = self.source_gate.admit().await;
            self.send(|reply| Control::Apply {
                command,
                end_offset,
                generation,
                reply,
            })
            .await
        })
    }
    fn replication_state(
        &self,
        generation: u64,
        replica: bool,
        ready: bool,
    ) -> BackendFuture<'_, ()> {
        Box::pin(self.send(move |reply| Control::ReplicationState {
            generation,
            replica,
            ready,
            reply,
        }))
    }
    fn source_connection(
        &self,
        peer: std::net::SocketAddr,
        local: std::net::SocketAddr,
    ) -> io::Result<Box<dyn native_replication::SourceConnection>> {
        let state = self.state()?;
        let lease = state.register(peer, local);
        let subscription = state.pubsub.attach(lease.id);
        let control = state.client_registry.register(lease.id, lease.info());
        control.set_kind(crate::client_registry::ClientKind::Master);
        Ok(Box::new(SourceClient {
            lease,
            subscription,
            state,
        }))
    }
    fn drain_writes(&self) -> BackendFuture<'_, bool> {
        Box::pin(async move {
            let state = self.state()?;
            let owned = !state.write_gate.is_paused();
            state.write_gate.pause().await?;
            Ok(owned)
        })
    }
    fn unfreeze_migration(&self) -> io::Result<()> {
        self.state()?.write_gate.resume();
        self.source_gate.resume();
        Ok(())
    }
    fn pause_writes(&self, paused: bool) -> io::Result<()> {
        let state = self.state()?;
        if paused {
            if !state.write_gate.is_paused() {
                return Err(invalid(
                    "FAILOVER requires a drained write-admission barrier",
                ));
            }
        } else {
            state.write_gate.resume();
        }
        Ok(())
    }
}

struct SourceClient {
    lease: crate::state::ClientLease,
    subscription: crate::pubsub::Subscription,
    state: Arc<State>,
}
impl native_replication::SourceConnection for SourceClient {
    fn cancellation(&self) -> tokio::sync::watch::Receiver<bool> {
        self.subscription.cancellation()
    }
}
impl Drop for SourceClient {
    fn drop(&mut self) {
        self.state.client_registry.unregister(self.lease.id);
    }
}
impl ClusterHooks for Runtime {
    fn keys(&self, slot: u16, limit: usize) -> ClusterFuture<'_, Vec<Bytes>> {
        Box::pin(self.send(move |reply| Control::Keys { slot, limit, reply }))
    }
    fn slot_key_counts(&self) -> ClusterFuture<'_, Vec<u64>> {
        Box::pin(self.send(|reply| Control::Counts { reply }))
    }
    fn exists<'a>(&'a self, keys: &'a [Bytes]) -> ClusterFuture<'a, Vec<bool>> {
        Box::pin(self.send(|reply| Control::Exists {
            keys: keys.to_vec(),
            reply,
        }))
    }
    fn database_size(&self) -> ClusterFuture<'_, u64> {
        Box::pin(self.send(|reply| Control::Size { reply }))
    }
    fn export_slots<'a>(
        &'a self,
        ranges: &'a [(u16, u16)],
        after: Option<&'a [u8]>,
        limit: usize,
    ) -> ClusterFuture<'a, ClusterPage> {
        Box::pin(self.send(move |reply| Control::Export {
            ranges: ranges.to_vec(),
            after: after.map(<[u8]>::to_vec),
            limit,
            reply,
        }))
    }
    fn import_keys<'a>(&'a self, keys: &'a [ClusterKey]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::Import {
                keys: keys.to_vec(),
                reply,
            })
            .await
        })
    }
    fn delete_slots<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::Delete {
                ranges: ranges.to_vec(),
                reply,
            })
            .await
        })
    }
    fn clear(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::Clear { reply }).await
        })
    }
    fn freeze_writes(&self) -> ClusterFuture<'_, u64> {
        Box::pin(async move {
            let state = self.state()?;
            let native = native(&state)?;
            let _generation = native.generation_guard(native.generation()).await?;
            self.source_gate.pause().await?;
            if let Err(error) = state.write_gate.pause().await {
                self.source_gate.resume();
                return Err(error);
            }
            match self.send(|reply| Control::Barrier { reply }).await {
                Ok(offset) => Ok(offset),
                Err(error) => {
                    state.write_gate.resume();
                    self.source_gate.resume();
                    Err(error)
                }
            }
        })
    }
    fn unfreeze_writes(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move { Backend::unfreeze_migration(self) })
    }
    fn follow<'a>(&'a self, host: &'a str, port: u16) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let mut upstream = self
                .config
                .native
                .upstream_template
                .clone()
                .unwrap_or_else(|| Upstream::plain(host.into(), port));
            upstream.host = host.into();
            upstream.port = port;
            native(&state)?.follow(upstream).await
        })
    }
    fn promote(&self, required_offset: Option<u64>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let native = native(&state)?;
            native.promote_at(required_offset).await
        })
    }
    fn replication_progress(&self) -> ClusterFuture<'_, ReplicationProgress> {
        Box::pin(async move {
            let state = self.state()?;
            let status = native(&state)?.status();
            let primary_offset = if status.role == native_replication::Role::Primary {
                status.offset
            } else {
                0
            };
            Ok(ReplicationProgress {
                history: status.history,
                processed: status.offset,
                primary_offset,
                upstream_connected: status.upstream_connected,
            })
        })
    }
    fn publish_shard<'a>(&'a self, channel: &'a [u8], message: &'a [u8]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            self.state()?
                .pubsub
                .publish_shard(
                    &Bytes::copy_from_slice(channel),
                    &Bytes::copy_from_slice(message),
                )
                .map_err(invalid)?;
            Ok(())
        })
    }
    fn publish_global<'a>(&'a self, channel: &'a [u8], message: &'a [u8]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            self.state()?
                .pubsub
                .publish(
                    &Bytes::copy_from_slice(channel),
                    &Bytes::copy_from_slice(message),
                )
                .map_err(invalid)?;
            Ok(())
        })
    }
    fn export_migration_globals<'a>(
        &'a self,
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, Vec<u8>> {
        Box::pin(self.send(|reply| Control::ExportGlobals {
            ranges: ranges.to_vec(),
            reply,
        }))
    }
    fn import_migration_globals<'a>(
        &'a self,
        payload: &'a [u8],
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::ImportGlobals {
                payload: Bytes::copy_from_slice(payload),
                ranges: ranges.to_vec(),
                reply,
            })
            .await
        })
    }
    fn migration_changes<'a>(
        &'a self,
        history: &'a str,
        after: u64,
        max_bytes: usize,
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, cluster::ClusterChanges> {
        Box::pin(self.send(move |reply| Control::Changes {
            history: history.into(),
            after,
            max_bytes,
            ranges: ranges.to_vec(),
            reply,
        }))
    }
    fn apply_migration_changes<'a>(
        &'a self,
        bytes: &'a [u8],
        ranges: &'a [(u16, u16)],
    ) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::ApplyChanges {
                bytes: Bytes::copy_from_slice(bytes),
                ranges: ranges.to_vec(),
                reply,
            })
            .await
        })
    }
    fn finish_migration<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let state = self.state()?;
            let _permit = state.write_gate.admit().await;
            self.send(|reply| Control::FinishMigration {
                ranges: ranges.to_vec(),
                reply,
            })
            .await
        })
    }
}

/// Always return paused engines, including on malformed streams and failed I/O.
async fn paused_result<T>(paused: Paused, result: io::Result<T>) -> io::Result<T> {
    let resumed = paused.resume();
    match result {
        Ok(value) => {
            resumed?;
            Ok(value)
        }
        Err(error) => {
            let _ = resumed;
            Err(error)
        }
    }
}

pub async fn handle(control: Control, context: &mut Context<'_>) {
    match control {
        Control::Snapshot {
            request,
            directory,
            max_bytes,
            reply,
        } => {
            let result = snapshot(context, request, directory, max_bytes).await;
            let _ = reply.send(result);
        }
        Control::Install {
            file,
            history,
            offset,
            generation,
            max_bytes,
            reply,
        } => {
            let result = match check_generation(context.state, generation) {
                Err(error) => Err(error),
                Ok(()) => {
                    context.state.set_replica_ready(false);
                    install(context, file, history, offset, generation, max_bytes).await
                }
            };
            let _ = reply.send(result);
        }
        Control::Apply {
            command,
            end_offset,
            generation,
            reply,
        } => {
            let result = apply(context, command, end_offset, generation).await;
            if result.is_err()
                && context
                    .state
                    .native
                    .get()
                    .is_some_and(|native| native.generation() == generation)
            {
                context.state.set_replica_ready(false);
            }
            let _ = reply.send(result);
        }
        Control::Sync { generation, reply } => {
            let result = sync(context, generation).await;
            let _ = reply.send(result);
        }
        Control::Keys { slot, limit, reply } => {
            let result = keys(context, Some(slot), limit).await;
            let _ = reply.send(result);
        }
        Control::Counts { reply } => {
            let result = counts(context).await;
            let _ = reply.send(result);
        }
        Control::Exists { keys, reply } => {
            let result = exists(context, &keys).await;
            let _ = reply.send(result);
        }
        Control::Size { reply } => {
            let result = counts(context).await.map(|counts| counts.into_iter().sum());
            let _ = reply.send(result);
        }
        Control::Export {
            ranges,
            after,
            limit,
            reply,
        } => {
            let result = export(context, &ranges, after.as_deref(), limit).await;
            let _ = reply.send(result);
        }
        Control::Import { keys, reply } => {
            let result = import(context, &keys).await;
            let _ = reply.send(result);
        }
        Control::Delete { ranges, reply } => {
            let result = delete(context, Some(&ranges)).await;
            let _ = reply.send(result);
        }
        Control::Clear { reply } => {
            let result = delete(context, None).await;
            let _ = reply.send(result);
        }
        Control::Barrier { reply } => {
            let result = barrier(context).await;
            let _ = reply.send(result);
        }
        Control::ReplicationState {
            generation,
            replica,
            ready,
            reply,
        } => {
            let result = match check_generation(context.state, generation) {
                Ok(()) => set_role(context, generation, replica).await,
                Err(error) => Err(error),
            };
            let result = result.and_then(|()| check_generation(context.state, generation));
            if result.is_ok() {
                context.state.set_replica_ready(ready);
            }
            let _ = reply.send(result);
        }
        Control::ExportGlobals { ranges, reply } => {
            let result = export_globals(context, &ranges).await;
            let _ = reply.send(result);
        }
        Control::ImportGlobals {
            ranges,
            payload,
            reply,
        } => {
            let result = import_globals(context, &ranges, payload).await;
            let _ = reply.send(result);
        }
        Control::Changes {
            history,
            after,
            max_bytes,
            ranges,
            reply,
        } => {
            let result = changes(context, &history, after, max_bytes, &ranges).await;
            let _ = reply.send(result);
        }
        Control::ApplyChanges {
            bytes,
            ranges,
            reply,
        } => {
            let result = apply_changes(context, bytes, &ranges).await;
            let _ = reply.send(result);
        }
        Control::FinishMigration { ranges, reply } => {
            let result = finish_migration(context, &ranges).await;
            let _ = reply.send(result);
        }
    }
}

struct CheckedFile<W> {
    inner: W,
    checksum: u64,
    checksum_enabled: bool,
    length: u64,
    maximum: u64,
}
impl<W: Write> Write for CheckedFile<W> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        if data.len() as u64 > self.maximum.saturating_sub(self.length) {
            return Err(invalid("native snapshot exceeds configured limit"));
        }
        let written = self.inner.write(data)?;
        self.length += written as u64;
        if self.checksum_enabled {
            self.checksum = crate::rdb::crc64(self.checksum, &data[..written]);
        }
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

async fn snapshot(
    context: &mut Context<'_>,
    request: SnapshotRequest,
    directory: PathBuf,
    max_bytes: u64,
) -> io::Result<Snapshot> {
    fs::create_dir_all(&directory)?;
    let outstanding =
        fs::read_dir(&directory)?.try_fold(0usize, |count, entry| -> io::Result<usize> {
            Ok(count
                + usize::from(
                    entry?
                        .path()
                        .extension()
                        .is_some_and(|extension| extension == "rdb"),
                ))
        })?;
    if outstanding >= MAX_SNAPSHOT_LEASES {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "too many outstanding native snapshot leases",
        ));
    }
    let path = directory.join(format!("snapshot-{:032x}.rdb", rand::random::<u128>()));
    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(0o600)
        .open(&path)?;
    let mut pending = StagedFile(path.clone());
    let paused = context.pool.pause().await?;
    let result = (|| {
        let status = native(context.state)?.status();
        let now = Engine::now();
        let mut output = CheckedFile {
            inner: BufWriter::new(file),
            checksum: 0,
            checksum_enabled: !request.no_checksum,
            length: 0,
            maximum: max_bytes.saturating_sub(8),
        };
        output.write_all(b"REDIS0015")?;
        for (key, value) in paused.engines[0].rdb_auxiliary() {
            if key.as_ref() == NATIVE_DATABASE_AUX
                || key.as_ref() == b"repl-stream-db"
                || key.as_ref() == b"repl-id"
                || key.as_ref() == b"repl-offset"
            {
                continue;
            }
            write_auxiliary(&mut output, key, value)?;
        }
        write_auxiliary(&mut output, b"repl-id", status.history.as_bytes())?;
        write_auxiliary(
            &mut output,
            b"repl-offset",
            status.offset.to_string().as_bytes(),
        )?;
        if status.role == native_replication::Role::Replica {
            let database = recovered_database(&paused.engines[0])?.to_string();
            write_auxiliary(&mut output, b"repl-stream-db", database.as_bytes())?;
            write_auxiliary(&mut output, NATIVE_DATABASE_AUX, database.as_bytes())?;
        }
        if !request.exclude_functions {
            for source in context.scripts.function_sources() {
                output.write_all(&[245])?;
                crate::rdb::write_string(&mut output, &source)?;
            }
        }
        if !request.exclude_data {
            for effect in paused.engines[0].search_snapshot() {
                write_auxiliary(&mut output, b"ffkv.search.v1", &effect?)?;
            }
            for engine in &paused.engines {
                engine.write_replication_rdb_records(&mut output, now)?;
            }
        }
        // The codec emits uncompressed strings, so no_compress is naturally honored.
        output.write_all(&[255])?;
        let checksum = if request.no_checksum {
            0
        } else {
            output.checksum
        };
        output.inner.write_all(&checksum.to_le_bytes())?;
        output.inner.flush()?;
        output.inner.get_ref().sync_all()?;
        Ok(Snapshot {
            path: path.clone(),
            length: output.length + 8,
            history: status.history,
            offset: status.offset,
        })
    })();
    let result = paused_result(paused, result).await;
    if result.is_ok() {
        pending.0.clear();
    }
    result
}
fn write_auxiliary(output: &mut impl Write, key: &[u8], value: &[u8]) -> io::Result<()> {
    output.write_all(&[250])?;
    crate::rdb::write_string(output, key)?;
    crate::rdb::write_string(output, value)
}

pub struct StagedFile(PathBuf);
impl Drop for StagedFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

async fn install(
    context: &mut Context<'_>,
    file: StagedFile,
    history: String,
    offset: u64,
    generation: u64,
    max_bytes: u64,
) -> io::Result<Applied> {
    check_generation(context.state, generation)?;
    let snapshot_budget = usize::try_from(max_bytes)
        .map_err(|_| invalid("native snapshot budget overflows this platform"))?;
    let paused = context.pool.pause().await?;
    let mut staged = Engine::new();
    staged.inherit_runtime(&paused.engines[0]);
    let reply_limit = staged.reply_limit();
    staged.set_reply_limit(snapshot_budget);
    staged.clear_search();
    staged.set_replica_mode(true);
    staged.set_replication_replay(true);
    paused.resume()?;
    let metadata =
        staged.install_replica_rdb(&mut BufReader::new(File::open(&file.0)?), Engine::now())?;
    let stream_database = match metadata
        .auxiliary
        .iter()
        .rev()
        .find(|(key, _)| key.as_ref() == b"repl-stream-db")
    {
        Some((_, value)) if value.as_ref() == b"-1" => 0,
        Some((_, value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|database| *database < staged.database_count())
            .ok_or_else(|| invalid("invalid RDB replication stream database"))?,
        None => 0,
    };
    let mut libraries = ScriptRunner::new()?;
    for source in &metadata.function_sources {
        let command = [
            Bytes::from_static(b"FUNCTION"),
            Bytes::from_static(b"LOAD"),
            source.clone(),
        ];
        reply_result(
            libraries
                .control(&command)
                .ok_or_else(|| invalid("FUNCTION LOAD is unavailable"))?,
        )?;
    }
    let library_snapshot = libraries.function_snapshot();
    staged.set_function_snapshot(library_snapshot.clone());
    staged.set_native_position(Bytes::copy_from_slice(history.as_bytes()), offset);
    let mut auxiliary = metadata.auxiliary;
    auxiliary.retain(|(key, _)| key.as_ref() != NATIVE_DATABASE_AUX);
    auxiliary.push((
        Bytes::from_static(NATIVE_DATABASE_AUX),
        Bytes::from(stream_database.to_string()),
    ));
    staged.set_rdb_auxiliary(auxiliary);
    for effect in metadata.search_effects {
        staged.search_apply_effect(&effect)?;
    }
    staged.set_reply_limit(reply_limit);
    staged.set_global_metadata_owner(true);
    staged.set_replication_replay(false);
    let mut paused = context.pool.pause().await?;
    let result = async {
        check_generation(context.state, generation)?;
        let gate_closed = context.replay.gate_closed()?;
        let previous_libraries = context.scripts.function_snapshot();
        context
            .scripts
            .restore_function_snapshot(&library_snapshot)
            .map_err(|message| invalid(&message))?;
        // Durably publish the complete replacement before declaring the link ready.
        let previous_databases: BTreeSet<_> = (0..paused.engines[0].database_count())
            .chain(paused.engines.iter().flat_map(Engine::database_ids))
            .collect();
        if let Err(error) = context
            .persistence
            .checkpoint_replacement(&staged, previous_databases.into_iter())
            .await
        {
            let _ = context
                .scripts
                .restore_function_snapshot(&previous_libraries);
            return Err(error);
        }
        for engine in &mut paused.engines {
            engine.set_replica_mode(true);
            engine.set_replication_replay(false);
        }
        context.persistence.note_install()?;
        let old = paused.take_all(false);
        drop(old);
        for engine in &mut paused.engines {
            engine.clear_search();
        }
        paused.restore(staged);
        context.persistence.watches.invalidate_all();
        context.state.client_registry.invalidate_all(None, None);
        context.replay.select_generation(generation);
        context.replay.database = stream_database;
        context.replay.transaction = None;
        context.replay.queued_bytes = 0;
        context.replay.committed_offset = offset;
        let fsynced = if context.persistence.enabled() && !gate_closed {
            offset
        } else {
            0
        };
        context.replay.fsynced_offset = fsynced;
        native(context.state)?.commit_snapshot_position(history, offset, fsynced, generation)?;
        Ok(Applied {
            fsynced_offset: fsynced,
        })
    }
    .await;
    paused_result(paused, result).await
}

fn command_size(command: &[Bytes]) -> io::Result<usize> {
    command.iter().try_fold(0usize, |size, argument| {
        size.checked_add(argument.len())
            .and_then(|n| n.checked_add(std::mem::size_of::<Bytes>()))
            .ok_or_else(|| invalid("replication command size overflow"))
    })
}
fn select_database(command: &[Bytes]) -> io::Result<u32> {
    if command.len() != 2 {
        return Err(invalid("malformed replicated SELECT"));
    }
    std::str::from_utf8(&command[1])
        .ok()
        .and_then(|n| n.parse::<u32>().ok())
        .filter(|database| *database < 65_536)
        .ok_or_else(|| invalid("replicated database index out of range"))
}
fn recovered_database(engine: &Engine) -> io::Result<u32> {
    match engine
        .rdb_auxiliary()
        .find(|(key, _)| key.as_ref() == NATIVE_DATABASE_AUX)
    {
        Some((_, value)) => std::str::from_utf8(value)
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|database| *database < engine.database_count())
            .ok_or_else(|| invalid("invalid recovered replication database")),
        None => Ok(0),
    }
}
fn merge_batch(target: &mut Option<Batch>, batch: Batch) {
    if let Some(target) = target {
        target.mutations.extend(batch.mutations);
    } else {
        *target = Some(batch);
    }
}

struct PreparedCommand {
    command: Vec<Bytes>,
    replay: Option<Batch>,
}
struct PreparedReplay {
    commands: Vec<PreparedCommand>,
    keys: BTreeMap<u32, BTreeSet<Bytes>>,
    all_keys: bool,
}
fn prepare_replay(commands: Vec<Vec<Bytes>>, initial_database: u32) -> io::Result<PreparedReplay> {
    let mut prepared = PreparedReplay {
        commands: Vec::with_capacity(commands.len()),
        keys: BTreeMap::new(),
        all_keys: false,
    };
    let mut database = initial_database;
    for command in commands {
        let name = command
            .first()
            .ok_or_else(|| invalid("empty resolved replication command"))?;
        let replay = if name.eq_ignore_ascii_case(b"FFKV.REPLAY1") {
            if command.len() != 2 {
                return Err(invalid("malformed trusted replay envelope"));
            }
            let batch = crate::wal::decode_native_batch(command[1].clone())?;
            let mut effect_database = 0;
            for mutation in &batch.mutations {
                match mutation {
                    Mutation::SelectDatabase { database } if *database < 65_536 => {
                        effect_database = *database
                    }
                    Mutation::Queue { key, .. } | Mutation::Typed { key, .. } => {
                        prepared
                            .keys
                            .entry(effect_database)
                            .or_default()
                            .insert(key.clone());
                    }
                    Mutation::Search { .. } => {}
                    _ => {
                        return Err(invalid(
                            "trusted extension replay contains a non-extension mutation",
                        ));
                    }
                }
            }
            Some(batch)
        } else {
            if name.eq_ignore_ascii_case(b"SELECT") {
                database = select_database(&command)?;
            } else if name.eq_ignore_ascii_case(b"FLUSHDB")
                || name.eq_ignore_ascii_case(b"FLUSHALL")
                || name.eq_ignore_ascii_case(b"SWAPDB")
            {
                prepared.all_keys = true;
            } else {
                for key in Engine::accessed_keys(&command) {
                    prepared.keys.entry(database).or_default().insert(key);
                }
            }
            None
        };
        prepared.commands.push(PreparedCommand { command, replay });
    }
    Ok(prepared)
}
fn take_replay_view(paused: &mut Paused, replay: &PreparedReplay) -> Engine {
    if replay.all_keys {
        let mut engine = paused.take_all(true);
        engine.set_command_user(None);
        return engine;
    }
    let mut engine = Engine::new();
    engine.inherit_runtime(&paused.engines[0]);
    engine.set_recording(true);
    for (database, keys) in &replay.keys {
        engine.switch_database(*database);
        for source in &mut paused.engines {
            source.switch_database(*database);
        }
        for key in keys {
            let shard = Engine::shard_for(key, paused.engines.len());
            paused.engines[shard].transfer_keys(std::slice::from_ref(key), &mut engine);
        }
    }
    engine
}

async fn apply(
    context: &mut Context<'_>,
    command: Vec<Bytes>,
    end_offset: u64,
    generation: u64,
) -> io::Result<Applied> {
    check_generation(context.state, generation)?;
    if context.replay.generation != Some(generation) {
        let status = native(context.state)?.status();
        let paused = context.pool.pause().await?;
        let database = recovered_database(&paused.engines[0]);
        let database = paused_result(paused, database).await?;
        context.replay.select_generation(generation);
        context.replay.database = database;
        context.replay.committed_offset = status.offset;
        context.replay.fsynced_offset = status.fsynced_offset;
    }
    let name = command
        .first()
        .ok_or_else(|| invalid("empty replicated command"))?;
    if name.eq_ignore_ascii_case(b"MULTI") {
        if command.len() != 1 || context.replay.transaction.is_some() {
            return Err(invalid("malformed replicated MULTI"));
        }
        context.replay.transaction = Some(Vec::new());
        return Ok(Applied {
            fsynced_offset: context.replay.fsynced_offset,
        });
    }
    if name.eq_ignore_ascii_case(b"DISCARD") {
        if command.len() != 1 || context.replay.transaction.take().is_none() {
            return Err(invalid("malformed replicated DISCARD"));
        }
        context.replay.queued_bytes = 0;
        context.replay.committed_offset = end_offset;
        return finish_apply(context, end_offset).await;
    }
    if !name.eq_ignore_ascii_case(b"EXEC")
        && let Some(queue) = &mut context.replay.transaction
    {
        let size = command_size(&command)?;
        if queue.len() >= MAX_QUEUED_COMMANDS
            || size > MAX_QUEUED_BYTES.saturating_sub(context.replay.queued_bytes)
        {
            context.replay.transaction = None;
            context.replay.queued_bytes = 0;
            return Err(invalid("replicated transaction exceeds bounded queue"));
        }
        context.replay.queued_bytes += size;
        queue.push(command);
        return Ok(Applied {
            fsynced_offset: context.replay.fsynced_offset,
        });
    }
    let commands = if name.eq_ignore_ascii_case(b"EXEC") {
        if command.len() != 1 {
            return Err(invalid("malformed replicated EXEC"));
        }
        context.replay.queued_bytes = 0;
        context
            .replay
            .transaction
            .take()
            .ok_or_else(|| invalid("replicated EXEC without MULTI"))?
    } else {
        vec![command]
    };
    let prepared = prepare_replay(commands, context.replay.database)?;
    let mut paused = context.pool.pause().await?;
    let result = async {
        check_generation(context.state, generation)?;
        let history = Bytes::from(native(context.state)?.status().history);
        for engine in &mut paused.engines {
            engine.set_replica_mode(true);
            engine.set_replication_replay(false);
        }
        if prepared
            .keys
            .keys()
            .any(|database| *database >= paused.engines[0].database_count())
        {
            return Err(invalid(
                "replicated database exceeds configured database count",
            ));
        }
        let mut engine = take_replay_view(&mut paused, &prepared);
        engine.set_replica_mode(true);
        engine.set_replication_replay(true);
        engine.set_search_reindexing(false);
        if engine.function_snapshot().is_empty() {
            engine.set_function_snapshot(context.scripts.function_snapshot());
        }
        let mut database = context.replay.database;
        let mut combined: Option<Batch> = None;
        let mut publications = Vec::new();
        let execution = (|| {
            for PreparedCommand { command, replay } in prepared.commands {
                let name = command
                    .first()
                    .ok_or_else(|| invalid("empty replicated command"))?;
                if name.eq_ignore_ascii_case(b"SELECT") {
                    database = select_database(&command)?;
                    if database >= engine.database_count() {
                        return Err(invalid(
                            "replicated database exceeds configured database count",
                        ));
                    }
                    continue;
                }
                engine.switch_database(database);
                if name.eq_ignore_ascii_case(b"FFKV.REPLAY1") {
                    let batch = replay.expect("prepared trusted replay");
                    engine.replay(batch.clone())?;
                    if let Some(combined) = &mut combined {
                        combined.timestamp_ms = batch.timestamp_ms;
                    }
                    merge_batch(&mut combined, batch);
                } else if name.eq_ignore_ascii_case(b"PUBLISH")
                    || name.eq_ignore_ascii_case(b"SPUBLISH")
                {
                    if command.len() != 3 {
                        return Err(invalid("malformed replicated publication"));
                    }
                    publications.push((
                        name.eq_ignore_ascii_case(b"SPUBLISH"),
                        command[1].clone(),
                        command[2].clone(),
                    ));
                } else if name.eq_ignore_ascii_case(b"FUNCTION") {
                    let reply = context
                        .scripts
                        .control(&command)
                        .ok_or_else(|| invalid("unsupported replicated FUNCTION command"))?;
                    reply_result(reply)?;
                    let snapshot = context.scripts.function_snapshot();
                    engine.set_function_snapshot(snapshot.clone());
                    merge_batch(
                        &mut combined,
                        Batch {
                            timestamp_ms: 0,
                            mutations: vec![
                                Mutation::SelectDatabase { database },
                                Mutation::Functions { snapshot },
                            ],
                        },
                    );
                } else {
                    if Engine::is_control(&command) {
                        return Err(invalid(
                            "control command is forbidden in trusted replication data stream",
                        ));
                    }
                    match engine.execute_at_time(&command, 0)? {
                        Execution::Reply(reply) => reply_result(reply)?,
                        Execution::Pushed { reply, .. } => reply_result(reply)?,
                        Execution::Block(_) | Execution::StreamBlock(_) => {
                            return Err(invalid("blocking command in replication stream"));
                        }
                    }
                    if let Some(batch) = engine.take_batch() {
                        merge_batch(&mut combined, batch);
                    }
                }
            }
            Ok(())
        })();
        // Drain even an errored handler's captured effects before returning engines.
        if let Some(batch) = engine.take_batch() {
            merge_batch(&mut combined, batch);
        }
        if execution.is_ok() {
            engine.set_native_position(history.clone(), end_offset);
            if database != context.replay.database {
                let value = Bytes::from(database.to_string());
                let mut auxiliary: Vec<_> = engine
                    .rdb_auxiliary()
                    .filter(|(key, _)| key.as_ref() != NATIVE_DATABASE_AUX)
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect();
                auxiliary.push((Bytes::from_static(NATIVE_DATABASE_AUX), value.clone()));
                engine.set_rdb_auxiliary(auxiliary);
                merge_batch(
                    &mut combined,
                    Batch {
                        timestamp_ms: 0,
                        mutations: vec![
                            Mutation::SelectDatabase { database },
                            Mutation::RdbAuxiliary {
                                key: Bytes::from_static(NATIVE_DATABASE_AUX),
                                value,
                            },
                        ],
                    },
                );
            }
            merge_batch(
                &mut combined,
                Batch {
                    timestamp_ms: 0,
                    mutations: vec![
                        Mutation::SelectDatabase { database },
                        Mutation::NativePosition {
                            history,
                            offset: end_offset,
                        },
                    ],
                },
            );
        }
        let result = match execution {
            Ok(()) => {
                if let Some(batch) = &combined {
                    context.persistence.commit_replicated_batch(batch).await
                } else {
                    Ok(())
                }
            }
            Err(error) => Err(error),
        };
        if result.is_ok() {
            context.replay.database = database;
        }
        engine.set_search_reindexing(true);
        engine.set_replication_replay(false);
        paused.restore(engine);
        result?;
        context.replay.committed_offset = end_offset;
        let applied = finish_apply(context, end_offset).await?;
        for (sharded, channel, message) in publications {
            if sharded {
                context
                    .state
                    .pubsub
                    .publish_shard(&channel, &message)
                    .map_err(invalid)?;
            } else {
                context
                    .state
                    .pubsub
                    .publish(&channel, &message)
                    .map_err(invalid)?;
            }
        }
        Ok(applied)
    }
    .await;
    if result.is_err()
        && context
            .state
            .native
            .get()
            .is_some_and(|native| native.generation() == generation)
    {
        context.state.set_replica_ready(false);
    }
    paused_result(paused, result).await
}

async fn finish_apply(context: &mut Context<'_>, end_offset: u64) -> io::Result<Applied> {
    if context.persistence.enabled() && !context.replay.gate_closed()? {
        context.persistence.finish().await?;
        context.replay.fsynced_offset = end_offset;
    }
    Ok(Applied {
        fsynced_offset: context
            .replay
            .fsynced_offset
            .min(context.replay.committed_offset),
    })
}
async fn sync(context: &mut Context<'_>, generation: u64) -> io::Result<u64> {
    check_generation(context.state, generation)?;
    if context.replay.generation != Some(generation)
        || !context.persistence.enabled()
        || context.replay.gate_closed()?
    {
        return Ok(0);
    }
    if context.replay.fsynced_offset == context.replay.committed_offset {
        return Ok(context.replay.fsynced_offset);
    }
    context.persistence.finish().await?;
    context.replay.fsynced_offset = context.replay.committed_offset;
    Ok(context.replay.fsynced_offset)
}

async fn visible_slots(state: &State) -> Vec<bool> {
    match state.cluster.get() {
        Some(cluster) => cluster.visible_slots().await,
        None => vec![true; cluster::SLOT_COUNT],
    }
}
fn in_ranges(slot: u16, ranges: &[(u16, u16)]) -> bool {
    ranges
        .iter()
        .any(|(first, last)| *first <= slot && slot <= *last)
}
fn page_keys(
    paused: &Paused,
    visibility: &[bool],
    ranges: Option<&[(u16, u16)]>,
    after: Option<&[u8]>,
    limit: usize,
    now: i64,
) -> BTreeSet<Bytes> {
    let mut selected = BTreeSet::new();
    for engine in &paused.engines {
        for key in engine.live_keys(now) {
            let slot = cluster::key_slot(key);
            if !visibility[slot as usize]
                || ranges.is_some_and(|ranges| !in_ranges(slot, ranges))
                || after.is_some_and(|after| key.as_ref() <= after)
            {
                continue;
            }
            selected.insert(key.clone());
            if selected.len() > limit {
                selected.pop_last();
            }
        }
    }
    selected
}
async fn keys(
    context: &mut Context<'_>,
    slot: Option<u16>,
    limit: usize,
) -> io::Result<Vec<Bytes>> {
    if slot.is_some_and(|slot| usize::from(slot) >= cluster::SLOT_COUNT) {
        return Err(invalid("invalid cluster slot"));
    }
    if limit == 0 {
        return Ok(Vec::new());
    }
    let visibility = visible_slots(context.state).await;
    let paused = context.pool.pause().await?;
    let ranges = slot.map(|slot| [(slot, slot)]);
    let values = page_keys(
        &paused,
        &visibility,
        ranges.as_ref().map(|range| range.as_slice()),
        None,
        limit,
        Engine::now(),
    )
    .into_iter()
    .collect();
    paused_result(paused, Ok(values)).await
}
async fn counts(context: &mut Context<'_>) -> io::Result<Vec<u64>> {
    let visibility = visible_slots(context.state).await;
    let paused = context.pool.pause().await?;
    let result = counts_paused(&paused, &visibility, context.state);
    paused_result(paused, result).await
}
fn counts_paused(paused: &Paused, visibility: &[bool], state: &State) -> io::Result<Vec<u64>> {
    let mut counts = vec![0; cluster::SLOT_COUNT];
    let now = Engine::now();
    if let Some(cluster) = state.cluster.get() {
        for engine in &paused.engines {
            engine.fill_slot_memory(now, &mut counts);
        }
        for (slot, bytes) in counts.iter().enumerate() {
            cluster.record_memory(slot as u16, if visibility[slot] { *bytes } else { 0 });
        }
        counts.fill(0);
    }
    for engine in &paused.engines {
        for key in engine.live_keys(now) {
            let slot = usize::from(cluster::key_slot(key));
            if visibility[slot] {
                counts[slot] += 1;
            }
        }
    }
    Ok(counts)
}
async fn exists(context: &mut Context<'_>, keys: &[Bytes]) -> io::Result<Vec<bool>> {
    let paused = context.pool.pause().await?;
    let now = Engine::now();
    let values = keys
        .iter()
        .map(|key| {
            let engine = &paused.engines[Engine::shard_for(key, paused.engines.len())];
            engine.live_key_exists(key, now)
        })
        .collect();
    paused_result(paused, Ok(values)).await
}
async fn export(
    context: &mut Context<'_>,
    ranges: &[(u16, u16)],
    after: Option<&[u8]>,
    limit: usize,
) -> io::Result<ClusterPage> {
    let visibility = visible_slots(context.state).await;
    let mut paused = context.pool.pause().await?;
    let result = (|| {
        let now = Engine::now();
        let limit = limit.clamp(1, MAX_CLUSTER_PAGE);
        let mut selected = page_keys(&paused, &visibility, Some(ranges), after, limit + 1, now);
        let more = selected.len() > limit;
        if more {
            selected.pop_last();
        }
        let next = if more {
            selected.last().map(|key| key.to_vec())
        } else {
            None
        };
        let mut keys = Vec::with_capacity(selected.len());
        for key in selected {
            let index = Engine::shard_for(&key, paused.engines.len());
            let engine = &mut paused.engines[index];
            let payload = engine.export_key(&key, now).map_err(|reply| match reply {
                Resp::Error(message) => invalid(&message),
                _ => invalid("key export failed"),
            })?;
            if let Some(payload) = payload {
                keys.push(ClusterKey {
                    key: key.to_vec(),
                    payload: payload.to_vec(),
                });
            }
        }
        Ok(ClusterPage { keys, next })
    })();
    paused_result(paused, result).await
}
async fn import(context: &mut Context<'_>, keys: &[ClusterKey]) -> io::Result<()> {
    let key_names: Vec<_> = keys
        .iter()
        .map(|key| Bytes::copy_from_slice(&key.key))
        .collect();
    let mut paused = context.pool.pause().await?;
    for engine in &mut paused.engines {
        engine.switch_database(0);
    }
    let mut engine = paused.take_keys(&key_names, true);
    engine.set_search_reindexing(false);
    engine.set_replication_replay(true);
    let mut combined: Option<Batch> = None;
    let execution = (|| {
        let now = 0;
        for (item, key) in keys.iter().zip(&key_names) {
            let Some((_, importer)) = &mut context.replay.destination_search else {
                return Err(invalid("migration globals must be installed before keys"));
            };
            let effects = engine.search_reconcile_migration_key(importer, key)?;
            if !effects.is_empty() {
                merge_batch(
                    &mut combined,
                    Batch {
                        timestamp_ms: now,
                        mutations: std::iter::once(Mutation::SelectDatabase { database: 0 })
                            .chain(
                                effects
                                    .into_iter()
                                    .map(|effect| Mutation::Search { effect }),
                            )
                            .collect(),
                    },
                );
            }
            reply_result(engine.import_key(key, &item.payload, 0))?;
            if let Some(batch) = engine.take_batch() {
                merge_batch(&mut combined, batch);
            }
        }
        Ok(())
    })();
    if let Some(batch) = engine.take_batch() {
        merge_batch(&mut combined, batch);
    }
    let committed = if let Some(batch) = &combined {
        context.persistence.commit_batch(batch).await
    } else {
        Ok(())
    };
    engine.set_search_reindexing(true);
    engine.set_replication_replay(false);
    paused.restore(engine);
    paused_result(paused, committed.and(execution)).await
}
async fn delete(context: &mut Context<'_>, ranges: Option<&[(u16, u16)]>) -> io::Result<()> {
    let mut paused = context.pool.pause().await?;
    let result = delete_paused(&mut paused, context.persistence, ranges).await;
    paused_result(paused, result).await
}
async fn delete_paused(
    paused: &mut Paused,
    persistence: &Persistence,
    ranges: Option<&[(u16, u16)]>,
) -> io::Result<()> {
    let now = Engine::now();
    for engine in &mut paused.engines {
        engine.set_recording(true);
        if ranges.is_none() {
            match engine.execute_at_time(&[Bytes::from_static(b"FLUSHALL")], now)? {
                Execution::Reply(reply) => reply_result(reply)?,
                _ => return Err(invalid("invalid cluster clearing reply")),
            }
            persistence.commit(engine).await?;
            continue;
        }
        loop {
            let keys: Vec<_> = engine
                .live_keys(now)
                .filter(|key| {
                    in_ranges(
                        cluster::key_slot(key),
                        ranges.expect("slot deletion ranges"),
                    )
                })
                .take(MAX_CLUSTER_PAGE)
                .cloned()
                .collect();
            if keys.is_empty() {
                break;
            }
            let mut command = Vec::with_capacity(keys.len() + 1);
            command.push(Bytes::from_static(b"DEL"));
            command.extend(keys);
            match engine.execute_at_time(&command, now)? {
                Execution::Reply(reply) => reply_result(reply)?,
                _ => return Err(invalid("invalid cluster deletion reply")),
            }
            persistence.commit(engine).await?;
        }
    }
    Ok(())
}
async fn barrier(context: &mut Context<'_>) -> io::Result<u64> {
    let paused = context.pool.pause().await?;
    let result = async {
        if context.persistence.enabled() {
            context.persistence.finish().await?;
        }
        Ok(native(context.state)?.status().offset)
    }
    .await;
    paused_result(paused, result).await
}

const GLOBALS_MAGIC: &[u8; 8] = b"FFKVMG01";
fn append_global(output: &mut Vec<u8>, value: &[u8]) -> io::Result<()> {
    if output
        .len()
        .checked_add(8)
        .and_then(|n| n.checked_add(value.len()))
        .is_none_or(|n| n > MAX_QUEUED_BYTES)
    {
        return Err(invalid("migration global metadata exceeds 64 MiB"));
    }
    output.extend_from_slice(&(value.len() as u64).to_le_bytes());
    output.extend_from_slice(value);
    Ok(())
}
fn take_global(input: &Bytes, cursor: &mut usize) -> io::Result<Bytes> {
    let end = cursor
        .checked_add(8)
        .filter(|end| *end <= input.len())
        .ok_or_else(|| invalid("truncated migration metadata length"))?;
    let length = u64::from_le_bytes(input[*cursor..end].try_into().expect("checked length"));
    let length =
        usize::try_from(length).map_err(|_| invalid("migration metadata length overflows"))?;
    let start = end;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= input.len())
        .ok_or_else(|| invalid("truncated migration metadata record"))?;
    *cursor = end;
    Ok(input.slice(start..end))
}
async fn export_globals(context: &mut Context<'_>, ranges: &[(u16, u16)]) -> io::Result<Vec<u8>> {
    let paused = context.pool.pause().await?;
    let result = (|| {
        let (snapshot, filter) = paused.engines[0].export_search_migration(ranges)?;
        let mut output = GLOBALS_MAGIC.to_vec();
        append_global(
            &mut output,
            native(context.state)?.status().history.as_bytes(),
        )?;
        append_global(&mut output, &context.scripts.function_snapshot())?;
        for effect in snapshot {
            append_global(&mut output, &effect?)?;
        }
        context.replay.source_search = Some((ranges.to_vec(), filter));
        context.replay.destination_search = None;
        Ok(output)
    })();
    paused_result(paused, result).await
}
async fn import_globals(
    context: &mut Context<'_>,
    ranges: &[(u16, u16)],
    payload: Bytes,
) -> io::Result<()> {
    if payload.len() > MAX_QUEUED_BYTES || !payload.starts_with(GLOBALS_MAGIC) {
        return Err(invalid("invalid migration global metadata envelope"));
    }
    let mut cursor = GLOBALS_MAGIC.len();
    let history = take_global(&payload, &mut cursor)?;
    let history = std::str::from_utf8(&history)
        .map_err(|_| invalid("invalid migration source history"))?
        .to_owned();
    let functions = take_global(&payload, &mut cursor)?;
    let mut effects = Vec::new();
    while cursor < payload.len() {
        effects.push(take_global(&payload, &mut cursor)?);
    }
    let mut importer = SearchMigrationImport::new(ranges)?;
    let mut paused = context.pool.pause().await?;
    let mut engine = paused.take_keys(&[], false);
    let result = async {
        engine.validate_search_migration(&effects, ranges)?;
        let previous = context
            .replay
            .source_functions
            .as_ref()
            .filter(|(source, old_ranges, _)| source == &history && old_ranges.as_slice() == ranges)
            .map(|(_, _, bytes)| bytes.as_ref());
        context
            .scripts
            .merge_migration_functions(previous, &functions)
            .map_err(|message| invalid(&message))?;
        let library_snapshot = context.scripts.function_snapshot();
        engine.set_function_snapshot(library_snapshot.clone());
        let mut mutations = vec![
            Mutation::SelectDatabase { database: 0 },
            Mutation::Functions {
                snapshot: library_snapshot,
            },
        ];
        for effect in effects {
            for effect in engine.search_merge_migration_effect(&mut importer, &effect)? {
                mutations.push(Mutation::Search { effect });
            }
        }
        if !importer.is_ready() {
            return Err(invalid("truncated migration search metadata snapshot"));
        }
        for shard in &mut paused.engines {
            shard.set_migration_expiry_ranges(Some(ranges));
        }
        engine.set_migration_expiry_ranges(Some(ranges));
        context
            .persistence
            .commit_batch(&Batch {
                timestamp_ms: Engine::now(),
                mutations,
            })
            .await?;
        context.replay.destination_search = Some((ranges.to_vec(), importer));
        context.replay.source_search = None;
        context.replay.source_functions = Some((history, ranges.to_vec(), functions));
        Ok(())
    }
    .await;
    paused.restore(engine);
    paused_result(paused, result).await
}

fn encode_command(output: &mut BytesMut, command: Vec<Bytes>) -> io::Result<()> {
    let size = command_size(&command)?;
    if size > MAX_QUEUED_BYTES.saturating_sub(output.len()) {
        return Err(invalid("migration replay page exceeds bounded output"));
    }
    write!((&mut *output).writer(), "*{}\r\n", command.len())?;
    for value in command {
        write!((&mut *output).writer(), "${}\r\n", value.len())?;
        output.extend_from_slice(&value);
        output.extend_from_slice(b"\r\n");
    }
    Ok(())
}
async fn changes(
    context: &mut Context<'_>,
    history: &str,
    after: u64,
    max_bytes: usize,
    ranges: &[(u16, u16)],
) -> io::Result<cluster::ClusterChanges> {
    let (through, chunks) =
        native(context.state)?.changes_after_in_history(history, after, max_bytes)?;
    let Some((source_ranges, filter)) = &mut context.replay.source_search else {
        return Err(invalid("migration search snapshot session is missing"));
    };
    if source_ranges.as_slice() != ranges {
        return Err(invalid("migration search snapshot session ranges changed"));
    }
    let mut output = BytesMut::new();
    for chunk in chunks {
        for mut command in native_replication::decode_commands(chunk)? {
            if command
                .first()
                .is_some_and(|name| name.eq_ignore_ascii_case(b"FFKV.REPLAY1"))
            {
                if command.len() != 2 {
                    return Err(invalid("malformed migration trusted replay"));
                }
                let batch = crate::wal::decode_native_batch(command[1].clone())?;
                let mut filtered = Vec::with_capacity(batch.mutations.len());
                for mutation in batch.mutations {
                    match mutation {
                        Mutation::Search { effect } => {
                            if let Some(effect) =
                                Engine::search_filter_migration_effect(filter, &effect)?
                            {
                                filtered.push(Mutation::Search { effect });
                            }
                        }
                        other => filtered.push(other),
                    }
                }
                if filtered
                    .iter()
                    .all(|mutation| matches!(mutation, Mutation::SelectDatabase { .. }))
                {
                    continue;
                }
                command[1] = crate::wal::encode_native_batch(&Batch {
                    timestamp_ms: batch.timestamp_ms,
                    mutations: filtered,
                })?;
            }
            encode_command(&mut output, command)?;
        }
    }
    Ok(cluster::ClusterChanges {
        through,
        bytes: output.to_vec(),
    })
}

fn mutation_key(mutation: &Mutation) -> Option<&Bytes> {
    match mutation {
        Mutation::Queue { key, .. } | Mutation::Typed { key, .. } => Some(key),
        _ => None,
    }
}
fn mutation_slot(mutation: &Mutation) -> Option<u16> {
    mutation_key(mutation).map(|key| cluster::key_slot(key))
}
fn collect_migration_clear(
    engine: &mut Engine,
    ranges: &[(u16, u16)],
    combined: &mut Option<Batch>,
) -> io::Result<()> {
    loop {
        let now = 0;
        let keys: Vec<_> = engine
            .live_keys(now)
            .filter(|key| in_ranges(cluster::key_slot(key), ranges))
            .take(MAX_CLUSTER_PAGE)
            .cloned()
            .collect();
        if keys.is_empty() {
            return Ok(());
        }
        let mut command = vec![Bytes::from_static(b"DEL")];
        command.extend(keys);
        execute_migration_command(engine, &command, combined)?;
    }
}
fn execute_migration_command(
    engine: &mut Engine,
    command: &[Bytes],
    combined: &mut Option<Batch>,
) -> io::Result<()> {
    match engine.execute_at_time(command, 0)? {
        Execution::Reply(reply) | Execution::Pushed { reply, .. } => reply_result(reply)?,
        _ => return Err(invalid("blocking operation in migration replay")),
    }
    if let Some(batch) = engine.take_batch() {
        merge_batch(combined, batch);
    }
    Ok(())
}
fn reconcile_migration_functions(
    context: &mut Context<'_>,
    engine: &mut Engine,
    ranges: &[(u16, u16)],
    source: Bytes,
    combined: &mut Option<Batch>,
) -> io::Result<()> {
    let Some((_, old_ranges, previous)) = &context.replay.source_functions else {
        return Err(invalid("migration function ownership session is missing"));
    };
    if old_ranges.as_slice() != ranges {
        return Err(invalid("migration function ownership ranges changed"));
    }
    context
        .scripts
        .merge_migration_functions(Some(previous), &source)
        .map_err(|message| invalid(&message))?;
    let snapshot = context.scripts.function_snapshot();
    engine.set_function_snapshot(snapshot.clone());
    merge_batch(
        combined,
        Batch {
            timestamp_ms: 0,
            mutations: vec![
                Mutation::SelectDatabase { database: 0 },
                Mutation::Functions { snapshot },
            ],
        },
    );
    context
        .replay
        .source_functions
        .as_mut()
        .expect("checked session")
        .2 = source;
    Ok(())
}
async fn apply_changes(
    context: &mut Context<'_>,
    bytes: Bytes,
    ranges: &[(u16, u16)],
) -> io::Result<()> {
    let mut transaction = None;
    for command in native_replication::decode_commands(bytes)? {
        let name = &command[0];
        if name.eq_ignore_ascii_case(b"MULTI") {
            transaction = Some(vec![command]);
            continue;
        }
        if let Some(commands) = &mut transaction {
            let exec = name.eq_ignore_ascii_case(b"EXEC");
            commands.push(command);
            if exec {
                apply_changes_transaction(
                    context,
                    transaction.take().expect("active transaction"),
                    ranges,
                )
                .await?;
            }
        } else if !name.eq_ignore_ascii_case(b"PING") && !name.eq_ignore_ascii_case(b"REPLCONF") {
            apply_changes_transaction(context, vec![command], ranges).await?;
        }
    }
    Ok(())
}
async fn apply_changes_transaction(
    context: &mut Context<'_>,
    commands: Vec<Vec<Bytes>>,
    ranges: &[(u16, u16)],
) -> io::Result<()> {
    let mut prepared = prepare_replay(commands, 0)?;
    let mut paused = context.pool.pause().await?;
    for keys in prepared.keys.values_mut() {
        keys.retain(|key| in_ranges(cluster::key_slot(key), ranges));
    }
    if prepared.all_keys {
        prepared.all_keys = false;
        let keys = prepared.keys.entry(0).or_default();
        for engine in &paused.engines {
            for key in engine.live_keys(0) {
                if in_ranges(cluster::key_slot(key), ranges) {
                    keys.insert(key.clone());
                }
            }
        }
    }
    let mut engine = take_replay_view(&mut paused, &prepared);
    engine.set_search_reindexing(false);
    engine.set_replication_replay(true);
    let mut combined: Option<Batch> = None;
    let execution = (|| {
        for PreparedCommand { command, replay } in prepared.commands {
            let name = command
                .first()
                .ok_or_else(|| invalid("empty migration command"))?;
            if name.eq_ignore_ascii_case(b"MULTI")
                || name.eq_ignore_ascii_case(b"EXEC")
                || name.eq_ignore_ascii_case(b"PING")
            {
                continue;
            }
            if name.eq_ignore_ascii_case(b"REPLCONF")
                && command.len() == 3
                && command[1].eq_ignore_ascii_case(b"GETACK")
            {
                continue;
            }
            if name.eq_ignore_ascii_case(b"SELECT") {
                if select_database(&command)? != 0 {
                    return Err(invalid(
                        "cluster migration cannot select a nonzero database",
                    ));
                }
                continue;
            }
            if name.eq_ignore_ascii_case(b"FUNCTION") {
                if command.len() != 4 || !command[1].eq_ignore_ascii_case(b"RESTORE") {
                    return Err(invalid("unsupported migration function effect"));
                }
                reconcile_migration_functions(
                    context,
                    &mut engine,
                    ranges,
                    command[2].clone(),
                    &mut combined,
                )?;
                continue;
            }
            if name.eq_ignore_ascii_case(b"FFKV.REPLAY1") {
                let batch = replay.expect("prepared migration replay");
                if let Some(combined) = &mut combined {
                    combined.timestamp_ms = batch.timestamp_ms;
                }
                for mutation in batch.mutations {
                    match mutation {
                        Mutation::SelectDatabase { database: 0 } => {}
                        Mutation::SelectDatabase { .. } => {
                            return Err(invalid("cluster migration effect has a nonzero database"));
                        }
                        Mutation::Search { effect } => {
                            let Some((old_ranges, importer)) =
                                &mut context.replay.destination_search
                            else {
                                return Err(invalid("migration search import session is missing"));
                            };
                            if old_ranges.as_slice() != ranges {
                                return Err(invalid("migration search import ranges changed"));
                            }
                            let effects =
                                engine.search_merge_migration_effect(importer, &effect)?;
                            if !effects.is_empty() {
                                merge_batch(
                                    &mut combined,
                                    Batch {
                                        timestamp_ms: batch.timestamp_ms,
                                        mutations: std::iter::once(Mutation::SelectDatabase {
                                            database: 0,
                                        })
                                        .chain(
                                            effects
                                                .into_iter()
                                                .map(|effect| Mutation::Search { effect }),
                                        )
                                        .collect(),
                                    },
                                );
                            }
                        }
                        mutation @ (Mutation::Queue { .. } | Mutation::Typed { .. }) => {
                            if mutation_slot(&mutation).is_some_and(|slot| in_ranges(slot, ranges))
                            {
                                let batch = Batch {
                                    timestamp_ms: batch.timestamp_ms,
                                    mutations: vec![
                                        Mutation::SelectDatabase { database: 0 },
                                        mutation,
                                    ],
                                };
                                engine.replay(batch.clone())?;
                                merge_batch(&mut combined, batch);
                            }
                        }
                        _ => return Err(invalid("unexpected non-extension migration envelope")),
                    }
                }
                continue;
            }
            if name.eq_ignore_ascii_case(b"FLUSHDB") || name.eq_ignore_ascii_case(b"FLUSHALL") {
                collect_migration_clear(&mut engine, ranges, &mut combined)?;
                continue;
            }
            if name.eq_ignore_ascii_case(b"DEL") || name.eq_ignore_ascii_case(b"UNLINK") {
                let mut filtered = vec![command[0].clone()];
                filtered.extend(
                    command
                        .into_iter()
                        .skip(1)
                        .filter(|key| in_ranges(cluster::key_slot(key), ranges)),
                );
                if filtered.len() > 1 {
                    execute_migration_command(&mut engine, &filtered, &mut combined)?;
                }
                continue;
            }
            let keys = Engine::accessed_keys(&command);
            if keys.is_empty() {
                return Err(invalid(
                    "unexpected keyless command in migration data stream",
                ));
            }
            let selected = keys
                .iter()
                .filter(|key| in_ranges(cluster::key_slot(key), ranges))
                .count();
            if selected == 0 {
                continue;
            }
            if selected != keys.len() {
                return Err(invalid("cross-slot command in migration data stream"));
            }
            execute_migration_command(&mut engine, &command, &mut combined)?;
        }
        Ok(())
    })();
    if let Some(batch) = engine.take_batch() {
        merge_batch(&mut combined, batch);
    }
    // If a later frame fails, retain and durably record the already applied prefix.
    // Ownership cannot flip on an error; cluster cleanup removes imported slot keys.
    let committed = if let Some(batch) = &combined {
        context.persistence.commit_batch(batch).await
    } else {
        Ok(())
    };
    engine.set_search_reindexing(true);
    engine.set_replication_replay(false);
    paused.restore(engine);
    let result = committed.and(execution);
    paused_result(paused, result).await
}

async fn set_role(context: &mut Context<'_>, generation: u64, replica: bool) -> io::Result<()> {
    let mut paused = context.pool.pause().await?;
    let result = match check_generation(context.state, generation) {
        Ok(()) => set_role_paused(&mut paused, context.persistence, context.state, replica).await,
        Err(error) => Err(error),
    };
    paused_result(paused, result).await
}

async fn set_role_paused(
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
    replica: bool,
) -> io::Result<()> {
    // Invalidate the source position before accepting independent primary writes.
    if !replica
        && let Some((history, offset)) = paused.engines.iter().find_map(Engine::native_position)
    {
        let clear = Batch {
            timestamp_ms: 0,
            mutations: vec![Mutation::ClearNativePosition],
        };
        persistence.commit_replicated_batch(&clear).await?;
        if persistence.enabled() {
            persistence.finish().await?;
        }
        native(state)?.detach_recovered_primary_history(&history, offset);
        for engine in &mut paused.engines {
            engine.replay(clear.clone())?;
        }
    }
    for engine in &mut paused.engines {
        engine.set_replica_mode(replica);
        engine.set_replication_replay(false);
    }
    if state.is_replica() != replica {
        state.set_replica(replica);
    }
    Ok(())
}

fn committed_paused_position(
    paused: &Paused,
    persistence: &Persistence,
) -> Option<native_replication::CommittedPosition> {
    paused
        .engines
        .iter()
        .find_map(Engine::native_position)
        .map(|(history, offset)| native_replication::CommittedPosition {
            history,
            offset,
            fsynced_offset: persistence.last_fsynced_native_offset().min(offset),
        })
}

pub(crate) async fn follow_paused(
    host: &str,
    port: u16,
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
) -> io::Result<()> {
    let native = native(state)?;
    native.follow_paused(
        native.configured_upstream(host.to_owned(), port),
        committed_paused_position(paused, persistence),
    )?;
    set_role_paused(paused, persistence, state, true).await?;
    state.set_replica_ready(false);
    Ok(())
}

pub(crate) async fn promote_paused(
    required_offset: Option<u64>,
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
) -> io::Result<()> {
    native(state)?.promote_paused(
        required_offset,
        committed_paused_position(paused, persistence),
    )?;
    set_role_paused(paused, persistence, state, false).await?;
    state.set_replica_ready(true);
    Ok(())
}

/// EXEC already owns the actor and all shard engines. Never enqueue a native
/// role barrier back to that actor, nor await a lease held by a queued install.
pub(crate) async fn execute_paused(
    args: &[Bytes],
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
    _last_write_offset: u64,
) -> io::Result<Option<Resp>> {
    let native = native(state)?;
    let Some(reply) = native
        .configure_paused(
            args,
            state.cluster.get().is_some(),
            committed_paused_position(paused, persistence),
        )
        .await
    else {
        return Ok(None);
    };
    if !matches!(&reply, Resp::Error(_)) {
        let status = native.status();
        let replica = status.role == native_replication::Role::Replica;
        set_role_paused(paused, persistence, state, replica).await?;
        state.set_replica_ready(!replica || status.upstream_connected);
    }
    Ok(Some(reply))
}

async fn finish_migration(context: &mut Context<'_>, ranges: &[(u16, u16)]) -> io::Result<()> {
    let Some((active_ranges, _)) = &context.replay.destination_search else {
        return Ok(());
    };
    if active_ranges.as_slice() != ranges {
        return Err(io::Error::new(
            io::ErrorKind::Interrupted,
            "stale migration completion ranges",
        ));
    }
    let mut paused = context.pool.pause().await?;
    let result = async {
        for engine in &mut paused.engines {
            if let Some(batch) = engine.finish_migration_expiry(Engine::now())? {
                context.persistence.commit_batch(&batch).await?;
            }
        }
        context.replay.destination_search = None;
        context.replay.source_functions = None;
        Ok(())
    }
    .await;
    paused_result(paused, result).await
}

struct PausedClusterHooks<'a> {
    paused: tokio::sync::Mutex<&'a mut Paused>,
    persistence: &'a Persistence,
    state: &'a State,
}
impl cluster::ClusterCommandHooks for PausedClusterHooks<'_> {
    fn keys(&self, slot: u16, limit: usize) -> ClusterFuture<'_, Vec<Bytes>> {
        Box::pin(async move {
            if usize::from(slot) >= cluster::SLOT_COUNT {
                return Err(invalid("invalid cluster slot"));
            }
            if limit == 0 {
                return Ok(Vec::new());
            }
            let visibility = visible_slots(self.state).await;
            let paused = self.paused.lock().await;
            Ok(page_keys(
                &paused,
                &visibility,
                Some(&[(slot, slot)]),
                None,
                limit,
                Engine::now(),
            )
            .into_iter()
            .collect())
        })
    }
    fn slot_key_counts(&self) -> ClusterFuture<'_, Vec<u64>> {
        Box::pin(async move {
            let visibility = visible_slots(self.state).await;
            let paused = self.paused.lock().await;
            counts_paused(&paused, &visibility, self.state)
        })
    }
    fn exists<'a>(&'a self, keys: &'a [Bytes]) -> ClusterFuture<'a, Vec<bool>> {
        Box::pin(async move {
            let paused = self.paused.lock().await;
            let now = Engine::now();
            Ok(keys
                .iter()
                .map(|key| {
                    paused.engines[Engine::shard_for(key, paused.engines.len())]
                        .live_key_exists(key, now)
                })
                .collect())
        })
    }
    fn database_size(&self) -> ClusterFuture<'_, u64> {
        Box::pin(async move {
            let visibility = visible_slots(self.state).await;
            let paused = self.paused.lock().await;
            Ok(counts_paused(&paused, &visibility, self.state)?
                .into_iter()
                .sum())
        })
    }
    fn delete_slots<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let mut paused = self.paused.lock().await;
            delete_paused(&mut paused, self.persistence, Some(ranges)).await
        })
    }
    fn clear(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let mut paused = self.paused.lock().await;
            delete_paused(&mut paused, self.persistence, None).await
        })
    }
    fn unfreeze_writes(&self) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            // Cancel any older migration's external gates. The borrowed shard
            // barrier itself remains owned by EXEC until its atomic completion.
            native(self.state)?.unfreeze_migration()
        })
    }
    fn follow<'a>(&'a self, host: &'a str, port: u16) -> ClusterFuture<'a, ()> {
        Box::pin(async move {
            let mut paused = self.paused.lock().await;
            follow_paused(host, port, &mut paused, self.persistence, self.state).await
        })
    }
    fn promote(&self, required_offset: Option<u64>) -> ClusterFuture<'_, ()> {
        Box::pin(async move {
            let mut paused = self.paused.lock().await;
            promote_paused(required_offset, &mut paused, self.persistence, self.state).await
        })
    }
    fn replication_progress(&self) -> ClusterFuture<'_, ReplicationProgress> {
        Box::pin(async move {
            let status = native(self.state)?.status();
            let primary_offset = if status.role == native_replication::Role::Primary {
                status.offset
            } else {
                0
            };
            Ok(ReplicationProgress {
                history: status.history,
                processed: status.offset,
                primary_offset,
                upstream_connected: status.upstream_connected,
            })
        })
    }
}

pub(crate) async fn execute_cluster_paused(
    args: &[Bytes],
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
    _last_write_offset: u64,
) -> io::Result<Option<Resp>> {
    if !args
        .first()
        .is_some_and(|name| name.eq_ignore_ascii_case(b"CLUSTER"))
    {
        return Ok(None);
    }
    let Some(cluster) = state.cluster.get() else {
        return Ok(Some(Resp::Error(
            "ERR This instance has cluster support disabled".into(),
        )));
    };
    for engine in &mut paused.engines {
        engine.switch_database(0);
    }
    let hooks = PausedClusterHooks {
        paused: tokio::sync::Mutex::new(paused),
        persistence,
        state,
    };
    Ok(cluster
        .command_with_hooks(args, &mut cluster::ClusterClient::default(), &hooks)
        .await)
}

#[expect(
    clippy::too_many_arguments,
    reason = "Cluster admission must inspect the same paused image, client routing modes, and write/migration intent."
)]
pub(crate) async fn admit_cluster_paused(
    keys: &[Bytes],
    write: bool,
    asking_command: bool,
    migrating_command: bool,
    client: &mut cluster::ClusterClient,
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
) -> Result<(), Resp> {
    let Some(cluster) = state.cluster.get() else {
        return Ok(());
    };
    for engine in &mut paused.engines {
        engine.switch_database(0);
    }
    let hooks = PausedClusterHooks {
        paused: tokio::sync::Mutex::new(paused),
        persistence,
        state,
    };
    cluster
        .admit_with_hooks(
            keys,
            write,
            asking_command,
            migrating_command,
            client,
            &hooks,
        )
        .await
        .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn runtime() -> (Arc<State>, Arc<Runtime>) {
        let state = Arc::new(State::new(0, 1, false));
        let config = Config {
            native: native_replication::Config {
                backlog_bytes: 4096,
                max_snapshot_bytes: 1024 * 1024,
                staging_directory: std::env::current_dir().unwrap().join(format!(
                    ".ffkv-native-runtime-{:032x}",
                    rand::random::<u128>()
                )),
                listening_port: 0,
                announce_ip: None,
                io_timeout: Duration::from_secs(1),
                reconnect_delay: Duration::from_millis(10),
                aof_enabled: false,
                allow_loopback: true,
                allow_private: true,
                upstream_template: None,
            },
            cluster: None,
            fsync_gate: None,
        };
        let runtime = Runtime::new(&state, config);
        (state, runtime)
    }
    #[tokio::test]
    async fn native_history_chained_rdb_retains_elapsed_payload_for_source_persist() {
        fn command(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
            let args: Vec<_> = args
                .iter()
                .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                .collect();
            match engine.execute_at_time(&args, now).unwrap() {
                crate::engine::Execution::Reply(reply)
                | crate::engine::Execution::Pushed { reply, .. } => reply,
                _ => panic!("unexpected blocking command"),
            }
        }
        let (state, runtime) = runtime();
        let directory = runtime.config.native.staging_directory.clone();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        native
            .follow_paused(native.configured_upstream("127.0.0.1".into(), 1), None)
            .unwrap();
        assert!(state.native.set(native).is_ok());
        let now = Engine::now();
        let mut engine = Engine::new();
        assert!(matches!(
            command(
                &mut engine,
                now - 100,
                &["RPUSH", "elapsed-list", "retained"]
            ),
            Resp::Integer(1)
        ));
        assert!(matches!(
            command(&mut engine, now - 100, &["PEXPIRE", "elapsed-list", "10"]),
            Resp::Integer(1)
        ));
        assert!(matches!(
            command(
                &mut engine,
                now - 100,
                &["HSET", "elapsed-hash", "keep", "live", "field", "retained"]
            ),
            Resp::Integer(2)
        ));
        assert!(
            matches!(command(&mut engine, now - 100, &["HPEXPIRE", "elapsed-hash", "10", "FIELDS", "1", "field"]),
            Resp::Array(Some(values)) if matches!(values.as_slice(), [Resp::Integer(1)]))
        );
        engine.set_replica_mode(true);
        assert!(matches!(
            command(&mut engine, now, &["LLEN", "elapsed-list"]),
            Resp::Integer(0)
        ));
        assert!(matches!(
            command(&mut engine, now, &["HGET", "elapsed-hash", "field"]),
            Resp::Bulk(None)
        ));
        let mut ordinary = Vec::new();
        engine.write_rdb(&mut ordinary, now).unwrap();
        let mut user_export = Engine::new();
        user_export
            .install_rdb(&mut ordinary.as_slice(), now)
            .unwrap();
        assert!(matches!(
            command(&mut user_export, now, &["EXISTS", "elapsed-list"]),
            Resp::Integer(0)
        ));
        assert!(matches!(
            command(&mut user_export, now, &["HGET", "elapsed-hash", "field"]),
            Resp::Bulk(None)
        ));
        assert!(
            matches!(command(&mut user_export, now, &["HGET", "elapsed-hash", "keep"]),
            Resp::Bulk(Some(value)) if value.as_ref() == b"live")
        );
        let persistence = Arc::new(Persistence::new(None));
        let (pool, mut workers) = Pool::start(engine, 2, persistence.clone());
        let mut scripts = ScriptRunner::new().unwrap();
        let mut replay = ReplayState::default();
        let result = snapshot(
            &mut Context {
                pool: &pool,
                persistence: &persistence,
                scripts: &mut scripts,
                state: &state,
                replay: &mut replay,
            },
            SnapshotRequest::default(),
            directory.clone(),
            1024 * 1024,
        )
        .await
        .unwrap();
        let mut downstream = Engine::new();
        downstream.set_replica_mode(true);
        downstream.set_replication_replay(true);
        downstream
            .install_replica_rdb(&mut File::open(&result.path).unwrap(), now)
            .unwrap();
        assert!(
            matches!(
                command(&mut downstream, now, &["PERSIST", "elapsed-list"]),
                Resp::Integer(1)
            ),
            "chained native snapshot dropped source-owned payload before PERSIST"
        );
        assert!(
            matches!(command(&mut downstream, now, &["LRANGE", "elapsed-list", "0", "-1"]),
            Resp::Array(Some(values)) if matches!(values.as_slice(), [Resp::Bulk(Some(value))] if value.as_ref() == b"retained"))
        );
        assert!(
            matches!(command(&mut downstream, now, &["HPERSIST", "elapsed-hash", "FIELDS", "1", "field"]),
            Resp::Array(Some(values)) if matches!(values.as_slice(), [Resp::Integer(1)]))
        );
        assert!(
            matches!(command(&mut downstream, now, &["HGET", "elapsed-hash", "field"]),
            Resp::Bulk(Some(value)) if value.as_ref() == b"retained"),
            "chained native snapshot dropped retained hash field before HPERSIST"
        );
        drop(result);
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
        fs::remove_dir_all(directory).unwrap();
    }
    #[test]
    fn swap_replay_gathers_all_database_views_and_private_envelopes_stay_restricted() {
        let prepared = prepare_replay(
            vec![vec![
                Bytes::from_static(b"SWAPDB"),
                Bytes::from_static(b"2"),
                Bytes::from_static(b"7"),
            ]],
            0,
        )
        .unwrap();
        assert!(prepared.all_keys);
        for mutation in [
            Mutation::ResetGlobals,
            Mutation::ClearNativePosition,
            Mutation::SwapDatabases {
                first: 2,
                second: 7,
            },
        ] {
            let payload = crate::wal::encode_native_batch(&Batch {
                timestamp_ms: 0,
                mutations: vec![mutation],
            })
            .unwrap();
            assert!(
                prepare_replay(vec![vec![Bytes::from_static(b"FFKV.REPLAY1"), payload]], 0)
                    .is_err()
            );
        }
    }
    #[test]
    fn source_clients_are_real_master_connections_and_kill_cancels_the_link() {
        let (state, runtime) = runtime();
        let source = runtime
            .source_connection(
                "127.0.0.1:6379".parse().unwrap(),
                "127.0.0.1:49152".parse().unwrap(),
            )
            .unwrap();
        let cancelled = source.cancellation();
        assert!(!*cancelled.borrow());
        assert!(!state.client_registry.list(None, Some("master")).is_empty());
        assert!(state.client_registry.list(None, Some("replica")).is_empty());
        let normal = state.register(
            "127.0.0.1:49153".parse().unwrap(),
            "127.0.0.1:6380".parse().unwrap(),
        );
        let _subscription = state.pubsub.attach(normal.id);
        let control = state.client_registry.register(normal.id, normal.info());
        let reply = state.client_registry.command(
            &control,
            &[
                Bytes::from_static(b"CLIENT"),
                Bytes::from_static(b"KILL"),
                Bytes::from_static(b"TYPE"),
                Bytes::from_static(b"master"),
            ],
        );
        assert!(matches!(reply, Resp::Integer(1)));
        assert!(*cancelled.borrow());
        drop(source);
        assert!(state.client_registry.list(None, Some("master")).is_empty());
        assert_eq!(
            String::from_utf8_lossy(&state.client_info(None))
                .lines()
                .count(),
            1
        );
        state.client_registry.unregister(normal.id);
    }
    #[tokio::test]
    async fn internal_replication_bypasses_client_failover_pause() {
        let (state, runtime) = runtime();
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
        state.control_tx.set(sender).unwrap();
        state.write_gate.pause().await.unwrap();
        let apply = runtime.apply(vec![Bytes::from_static(b"PING")], 5, 0);
        let actor = async {
            let Some(Request::Native(Control::Apply { reply, .. })) = receiver.recv().await else {
                panic!("native apply was not queued");
            };
            reply.send(Ok(Applied { fsynced_offset: 0 })).unwrap();
        };
        let (applied, ()) =
            tokio::time::timeout(Duration::from_secs(1), async { tokio::join!(apply, actor) })
                .await
                .unwrap();
        assert_eq!(applied.unwrap().fsynced_offset, 0);
        state.write_gate.resume();
    }
    #[tokio::test]
    async fn recovered_offsets_seed_persistence_and_memory_finish_never_claims_fsync() {
        let (_state, runtime) = runtime();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        native.restore_position("a".repeat(40), 80, 40).unwrap();
        let persistence = Persistence::new(None);
        persistence.set_native(native.clone());
        assert_eq!(persistence.native_offset(), 80);
        assert_eq!(persistence.last_fsynced_native_offset(), 40);
        persistence
            .commit_batch(&Batch {
                timestamp_ms: 0,
                mutations: vec![Mutation::Set {
                    key: Bytes::from_static(b"key"),
                    value: Bytes::from_static(b"value"),
                    expires_at: None,
                }],
            })
            .await
            .unwrap();
        assert!(persistence.native_offset() > 80);
        persistence.finish().await.unwrap();
        assert_eq!(persistence.last_fsynced_native_offset(), 40);
        assert_eq!(native.status().fsynced_offset, 40);
    }
    #[tokio::test]
    async fn paused_role_configuration_fences_a_generation_without_waiting_for_its_actor_lease() {
        let (state, runtime) = runtime();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        assert!(state.native.set(native.clone()).is_ok());
        let lease = native.generation_guard(0).await.unwrap();
        let reply = tokio::time::timeout(
            Duration::from_secs(1),
            native.configure_paused(
                &[
                    Bytes::from_static(b"REPLICAOF"),
                    Bytes::from_static(b"127.0.0.1"),
                    Bytes::from_static(b"6379"),
                ],
                false,
                None,
            ),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(reply, Resp::Simple("OK")));
        assert!(native.is_replica());
        assert!(check_generation(&state, 0).is_err());
        drop(lease);
        let generation = native.generation();
        let lease = native.generation_guard(generation).await.unwrap();
        let history = native.status().history;
        native
            .promote_paused(
                Some(95),
                Some(native_replication::CommittedPosition {
                    history: Bytes::from(history.clone()),
                    offset: 100,
                    fsynced_offset: 90,
                }),
            )
            .unwrap();
        let promoted = native.status();
        assert_eq!(promoted.offset, 100);
        assert_eq!(promoted.fsynced_offset, 90);
        assert_eq!(promoted.secondary_history, Some(history));
        assert_eq!(promoted.secondary_valid_through, 101);
        assert!(check_generation(&state, generation).is_err());
        assert!(native.changes_after(80, 4096).is_err());
        drop(lease);
    }
    #[tokio::test]
    async fn killing_a_source_client_interrupts_actual_handshake_io_and_closes_the_socket() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let (state, runtime) = runtime();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        let port = Bytes::from(listener.local_addr().unwrap().port().to_string());
        let reply = native
            .configure_paused(
                &[
                    Bytes::from_static(b"REPLICAOF"),
                    Bytes::from_static(b"127.0.0.1"),
                    port,
                ],
                false,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(reply, Resp::Simple("OK")));
        let supervisor = native.start();
        let (mut upstream, _) = tokio::time::timeout(Duration::from_secs(1), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let expected = native_replication::encode(&[Bytes::from_static(b"PING")]);
        let mut ping = vec![0; expected.len()];
        tokio::time::timeout(Duration::from_secs(1), upstream.read_exact(&mut ping))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ping.as_slice(), expected.as_ref());
        let normal = state.register(
            "127.0.0.1:49153".parse().unwrap(),
            "127.0.0.1:6380".parse().unwrap(),
        );
        let _subscription = state.pubsub.attach(normal.id);
        let control = state.client_registry.register(normal.id, normal.info());
        let reply = state.client_registry.command(
            &control,
            &[
                Bytes::from_static(b"CLIENT"),
                Bytes::from_static(b"KILL"),
                Bytes::from_static(b"TYPE"),
                Bytes::from_static(b"master"),
            ],
        );
        assert!(matches!(reply, Resp::Integer(1)));
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), upstream.read(&mut ping))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        native.shutdown();
        tokio::time::timeout(Duration::from_secs(1), supervisor)
            .await
            .unwrap()
            .unwrap();
        assert!(state.client_registry.list(None, Some("master")).is_empty());
        state.client_registry.unregister(normal.id);
    }
    #[test]
    fn recovered_primary_detaches_old_source_history_but_replica_restart_keeps_it() {
        let (_state, runtime) = runtime();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        let history = "a".repeat(40);
        native.restore_position(history.clone(), 80, 80).unwrap();
        native.detach_recovered_primary_history(history.as_bytes(), 80);
        let primary = native.status();
        assert_ne!(primary.history, history);
        assert_eq!(primary.secondary_history, Some(history.clone()));
        assert_eq!(primary.secondary_valid_through, 81);
        assert_eq!(primary.offset, 80);
        let (_state, runtime) = self::runtime();
        let replica = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        replica.restore_position(history.clone(), 80, 80).unwrap();
        replica
            .follow_paused(Upstream::plain("127.0.0.1".into(), 6379), None)
            .unwrap();
        replica.detach_recovered_primary_history(history.as_bytes(), 80);
        assert_eq!(replica.status().history, history);
        assert_eq!(replica.status().offset, 80);
    }
    #[tokio::test]
    async fn ephemeral_publication_on_a_clean_wal_gets_a_real_periodic_fsync_watermark() {
        let (_state, runtime) = runtime();
        let directory = runtime.native_config().staging_directory.clone();
        let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
        let (wal, _) = crate::wal::Wal::open(
            &directory.join("wal"),
            crate::wal::Durability::Everysec,
            |_| Ok(()),
        )
        .unwrap();
        let persistence = Persistence::new(Some(wal));
        persistence.set_native(native.clone());
        let offset = persistence
            .publish_ephemeral(&[
                Bytes::from_static(b"PUBLISH"),
                Bytes::from_static(b"events"),
                Bytes::from_static(b"payload"),
            ])
            .await
            .unwrap()
            .unwrap();
        assert!(offset > 0);
        assert_eq!(persistence.native_offset(), offset);
        assert_eq!(persistence.last_fsynced_native_offset(), 0);
        assert!(!persistence.status().await.dirty);
        persistence.periodic_sync().await.unwrap();
        assert_eq!(persistence.last_fsynced_native_offset(), offset);
        assert_eq!(native.status().fsynced_offset, offset);
        drop(persistence);
        drop(native);
        std::fs::remove_dir_all(directory).unwrap();
    }
}

#[cfg(test)]
mod follow_barrier_tests {
    use super::*;
    #[tokio::test]
    async fn follow_drains_admitted_primary_writes_and_preserves_borrowed_migration_pause() {
        for borrowed_pause in [false, true] {
            let state = Arc::new(State::new(0, 1, false));
            let config = native_replication::Config {
                backlog_bytes: 4096,
                max_snapshot_bytes: 1024 * 1024,
                staging_directory: std::env::current_dir().unwrap().join(format!(
                    ".ffkv-native-follow-{:032x}",
                    rand::random::<u128>()
                )),
                listening_port: 0,
                announce_ip: None,
                io_timeout: Duration::from_secs(1),
                reconnect_delay: Duration::from_millis(10),
                aof_enabled: false,
                allow_loopback: true,
                allow_private: true,
                upstream_template: None,
            };
            let runtime = Runtime::new(
                &state,
                Config {
                    native: config,
                    cluster: None,
                    fsync_gate: None,
                },
            );
            let native = NativeReplication::new(runtime.native_config(), runtime).unwrap();
            let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
            state.control_tx.set(sender).unwrap();
            let permit = if borrowed_pause {
                state.write_gate.pause().await.unwrap();
                None
            } else {
                Some(state.write_gate.admit().await)
            };
            let follow = native.follow(Upstream::plain("127.0.0.1".into(), 6379));
            tokio::pin!(follow);
            let polled = std::future::poll_fn(|cx| {
                std::task::Poll::Ready(std::future::Future::poll(follow.as_mut(), cx))
            })
            .await;
            assert!(polled.is_pending());
            if !borrowed_pause {
                assert!(!native.is_replica());
            }
            drop(permit);
            let actor = async {
                let Some(Request::Native(Control::ReplicationState {
                    replica,
                    ready,
                    reply,
                    ..
                })) = receiver.recv().await
                else {
                    panic!("missing actor role barrier");
                };
                assert!(replica);
                assert!(!ready);
                reply.send(Ok(())).unwrap();
            };
            let (result, ()) = tokio::time::timeout(Duration::from_secs(1), async {
                tokio::join!(follow, actor)
            })
            .await
            .unwrap();
            result.unwrap();
            assert!(native.is_replica());
            assert_eq!(state.write_gate.is_paused(), borrowed_pause);
            state.write_gate.resume();
        }
    }
}
