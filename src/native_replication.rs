//! Native Redis replication. Offsets count the exact immutable RESP stream bytes.
//!
//! The actor adapter owns snapshot consistency and WAL durability: snapshot() must
//! capture its file and replication position in one commit barrier; apply() must
//! bypass client admission and must not call publish() a second time.
use crate::protocol::Resp;
use bytes::{Bytes, BytesMut};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io,
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{Notify, watch},
};

pub type BackendFuture<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;
pub trait Backend: Send + Sync {
    fn snapshot(&self, request: SnapshotRequest) -> BackendFuture<'_, Snapshot>;
    /// Commit the installed position with `commit_snapshot_position` under the
    /// image barrier, before returning or admitting another actor role command.
    fn install_snapshot(
        &self,
        file: PathBuf,
        history: String,
        offset: u64,
        generation: u64,
    ) -> BackendFuture<'_, Applied>;
    fn apply(
        &self,
        command: Vec<Bytes>,
        end_offset: u64,
        generation: u64,
    ) -> BackendFuture<'_, Applied>;
    /// Must share the write-admission gate, including commands already queued.
    fn pause_writes(&self, paused: bool) -> io::Result<()>;
    /// Acknowledges actor role, expiry mode and readiness before clients may
    /// observe the transition. Called without the native history mutex held.
    fn replication_state(
        &self,
        generation: u64,
        replica: bool,
        ready: bool,
    ) -> BackendFuture<'_, ()>;
    /// Only an established replication data link is exposed as TYPE master.
    fn source_connection(
        &self,
        peer: SocketAddr,
        local: SocketAddr,
    ) -> io::Result<Box<dyn SourceConnection>>;
    /// Returns whether this call acquired a new pause, rather than borrowing an
    /// existing migration/failover pause. Callers serialize role reconfiguration.
    fn drain_writes(&self) -> BackendFuture<'_, bool>;
    fn unfreeze_migration(&self) -> io::Result<()>;
}
pub trait SourceConnection: Send {
    fn cancellation(&self) -> watch::Receiver<bool>;
}
pub(crate) struct CommittedPosition {
    pub history: Bytes,
    pub offset: u64,
    pub fsynced_offset: u64,
}
#[derive(Clone, Copy, Debug)]
pub struct Applied {
    pub fsynced_offset: u64,
}
#[derive(Clone, Copy, Default)]
pub struct SnapshotRequest {
    pub exclude_data: bool,
    pub exclude_functions: bool,
    pub no_compress: bool,
    pub no_checksum: bool,
}
/// An owned temporary Redis RDB checkpoint, streamed without loading it into RAM.
/// Dropping the transport lease removes its owned file, including on failure.
pub struct Snapshot {
    pub path: PathBuf,
    pub length: u64,
    pub history: String,
    pub offset: u64,
}
impl Drop for Snapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
#[derive(Clone)]
pub struct Config {
    pub backlog_bytes: usize,
    pub max_snapshot_bytes: u64,
    pub staging_directory: PathBuf,
    pub listening_port: u16,
    pub announce_ip: Option<String>,
    pub io_timeout: Duration,
    pub reconnect_delay: Duration,
    pub aof_enabled: bool,
    pub allow_loopback: bool,
    pub allow_private: bool,
    pub upstream_template: Option<Upstream>,
}
#[derive(Clone)]
pub struct Upstream {
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub tls_server_name: Option<String>,
}
impl Upstream {
    pub fn plain(host: String, port: u16) -> Self {
        Self {
            host,
            port,
            username: None,
            password: None,
            tls: None,
            tls_server_name: None,
        }
    }
    fn validate(&self) -> io::Result<()> {
        if self.host.is_empty()
            || self.host.len() > 253
            || self.host.bytes().any(|b| b.is_ascii_control())
            || self.port == 0
        {
            return Err(invalid("invalid upstream endpoint"));
        }
        if self.username.is_some() && self.password.is_none() {
            return Err(invalid("upstream username requires password"));
        }
        if self.tls.is_none() && self.tls_server_name.is_some() {
            return Err(invalid("TLS server name requires TLS"));
        }
        if self.tls.is_some() {
            rustls::pki_types::ServerName::try_from(
                self.tls_server_name
                    .clone()
                    .unwrap_or_else(|| self.host.clone()),
            )
            .map_err(|_| invalid("invalid upstream TLS server name"))?;
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Primary,
    Replica,
}
#[derive(Clone, Debug)]
pub struct ReplicaStatus {
    pub id: u64,
    pub host: String,
    pub port: u16,
    pub processed: u64,
    pub fsynced: u64,
    pub online: bool,
    pub last_ack: Instant,
}
#[derive(Clone, Debug)]
pub struct Status {
    pub role: Role,
    pub history: String,
    pub secondary_history: Option<String>,
    pub secondary_valid_through: u64,
    pub offset: u64,
    pub fsynced_offset: u64,
    pub upstream: Option<(String, u16)>,
    pub upstream_connected: bool,
    pub replicas: Vec<ReplicaStatus>,
    pub failover_active: bool,
    pub master_connect_denied: u64,
}
#[derive(Default, Clone)]
pub struct ClientState {
    pub listening_port: u16,
    pub announce_ip: Option<String>,
    pub eof: bool,
    pub psync2: bool,
    pub rdb_only: bool,
    pub rdb_channel: bool,
    pub snapshot: SnapshotRequest,
}
pub enum CommandResult {
    Reply(Resp),
    NoReply,
    Downstream,
}
struct Chunk {
    first: u64,
    published_first: u64,
    bytes: Bytes,
}
struct State {
    history: String,
    secondary: Option<(String, u64)>,
    offset: u64,
    fsynced: u64,
    backlog: VecDeque<Chunk>,
    backlog_len: usize,
    replicas: BTreeMap<u64, ReplicaStatus>,
    next_replica: u64,
    upstream: Option<Upstream>,
    connected: bool,
    generation: u64,
    failover: Option<u64>,
    aof: bool,
    force_full_sync: bool,
    history_epoch: u64,
}
pub struct NativeReplication {
    config: Config,
    backend: Arc<dyn Backend>,
    state: Mutex<State>,
    changed: Notify,
    control: watch::Sender<u64>,
    stopped: watch::Sender<bool>,
    ack_requested: Notify,
    connect_denied: std::sync::atomic::AtomicU64,
    reconfigure: Arc<tokio::sync::Mutex<()>>,
}
impl NativeReplication {
    pub fn new(config: Config, backend: Arc<dyn Backend>) -> io::Result<Arc<Self>> {
        if config.backlog_bytes == 0
            || config.max_snapshot_bytes < 18
            || config.io_timeout.is_zero()
            || config.reconnect_delay.is_zero()
        {
            return Err(invalid("invalid native replication bounds"));
        }
        if let Some(upstream) = &config.upstream_template {
            upstream.validate()?;
        }
        std::fs::create_dir_all(&config.staging_directory)?;
        let (control, _) = watch::channel(0);
        let (stopped, _) = watch::channel(false);
        Ok(Arc::new(Self {
            state: Mutex::new(State {
                history: history_id(),
                secondary: None,
                offset: 0,
                fsynced: 0,
                backlog: VecDeque::new(),
                backlog_len: 0,
                replicas: BTreeMap::new(),
                next_replica: 1,
                upstream: None,
                connected: false,
                generation: 0,
                failover: None,
                aof: config.aof_enabled,
                force_full_sync: false,
                history_epoch: 0,
            }),
            config,
            backend,
            changed: Notify::new(),
            control,
            stopped,
            ack_requested: Notify::new(),
            connect_denied: std::sync::atomic::AtomicU64::new(0),
            reconfigure: Arc::new(tokio::sync::Mutex::new(())),
        }))
    }
    /// Starts one supervisor. Caller retains the task and awaits it on shutdown.
    pub fn start(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let this = self.clone();
        tokio::spawn(async move { this.supervise().await })
    }
    pub fn shutdown(&self) {
        self.stopped.send_replace(true);
        self.state.lock().unwrap().failover = None;
        self.changed.notify_waiters();
    }
    pub fn is_replica(&self) -> bool {
        self.state.lock().unwrap().upstream.is_some()
    }
    pub fn status(&self) -> Status {
        let s = self.state.lock().unwrap();
        Status {
            role: if s.upstream.is_some() {
                Role::Replica
            } else {
                Role::Primary
            },
            history: s.history.clone(),
            secondary_history: s.secondary.as_ref().map(|v| v.0.clone()),
            secondary_valid_through: s.secondary.as_ref().map_or(0, |v| v.1),
            offset: s.offset,
            fsynced_offset: s.fsynced,
            upstream: s.upstream.as_ref().map(|u| (u.host.clone(), u.port)),
            upstream_connected: s.connected,
            replicas: s.replicas.values().cloned().collect(),
            failover_active: s.failover.is_some(),
            master_connect_denied: self
                .connect_denied
                .load(std::sync::atomic::Ordering::Relaxed),
        }
    }
    fn history_epoch(&self) -> u64 {
        self.state.lock().unwrap().history_epoch
    }
    fn check_history_epoch(&self, epoch: u64) -> io::Result<()> {
        check_history_epoch(&self.state.lock().unwrap(), epoch)
    }
    /// Publish under the actor's durable image barrier. Epochs describe image
    /// continuity, not role generations: promotion alone must preserve CONTINUE.
    pub(crate) fn commit_snapshot_position(
        &self,
        history: String,
        offset: u64,
        fsynced: u64,
        generation: u64,
    ) -> io::Result<()> {
        if !valid_history(&history) || fsynced > offset {
            return Err(invalid("invalid installed replication position"));
        }
        let mut s = self.state.lock().unwrap();
        if s.generation != generation {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "stale snapshot installation generation",
            ));
        }
        invalidate_history(&mut s)?;
        s.history = history;
        s.secondary = None;
        s.offset = offset;
        s.fsynced = fsynced;
        s.force_full_sync = false;
        drop(s);
        self.changed.notify_waiters();
        Ok(())
    }
    /// Called once, in WAL commit order, with resolved effects (including DB
    /// selection, absolute expirations, and resolved random choices).
    pub fn publish(&self, bytes: Bytes, fsynced: bool) -> io::Result<u64> {
        let mut s = self.state.lock().unwrap();
        if s.upstream.is_some() {
            return Err(invalid("cannot publish client writes on a replica"));
        }
        let end = append(&mut s, bytes, self.config.backlog_bytes)?;
        if fsynced {
            s.fsynced = end;
        }
        drop(s);
        self.changed.notify_waiters();
        Ok(end)
    }
    pub fn mark_fsynced(&self, offset: u64) -> io::Result<()> {
        let mut s = self.state.lock().unwrap();
        if offset > s.offset {
            return Err(invalid("fsync offset exceeds processed offset"));
        }
        s.fsynced = s.fsynced.max(offset);
        drop(s);
        self.changed.notify_waiters();
        self.ack_requested.notify_one();
        Ok(())
    }
    pub fn set_aof_enabled(&self, enabled: bool) {
        self.state.lock().unwrap().aof = enabled;
        self.changed.notify_waiters();
    }
    pub async fn follow(&self, upstream: Upstream) -> io::Result<()> {
        upstream.validate()?;
        let _lease = self.reconfigure.clone().lock_owned().await;
        // Drain before changing the native role: otherwise an already-admitted
        // primary commit could skip publication after upstream becomes Some.
        let owned_pause = self.backend.drain_writes().await?;
        let (generation, cancelled_failover) = {
            let mut s = self.state.lock().unwrap();
            let cancelled_failover = s.failover.take().is_some();
            s.upstream = Some(upstream);
            s.connected = false;
            s.generation = s.generation.wrapping_add(1);
            (s.generation, cancelled_failover)
        };
        let transitioned = self.transition_state(generation, true, false).await;
        if transitioned.is_ok() {
            self.control.send_replace(generation);
            self.changed.notify_waiters();
        }
        if owned_pause || cancelled_failover {
            let s = self.state.lock().unwrap();
            if s.generation == generation || s.failover.is_none() {
                self.backend.pause_writes(false)?;
            }
        }
        transitioned?;
        Ok(())
    }
    pub async fn promote(&self) -> io::Result<()> {
        self.promote_at(None).await
    }
    /// Catch-up validation and promotion share the same generation lease and
    /// history lock, so a pending install cannot invalidate a sampled watermark.
    pub async fn promote_at(&self, required_offset: Option<u64>) -> io::Result<()> {
        let _lease = self.reconfigure.clone().lock_owned().await;
        self.promote_locked(required_offset).await
    }
    async fn promote_locked(&self, required_offset: Option<u64>) -> io::Result<()> {
        let generation = {
            let mut s = self.state.lock().unwrap();
            if s.force_full_sync {
                return Err(invalid(
                    "replica requires full synchronization before promotion",
                ));
            }
            if required_offset.is_some_and(|required| s.offset < required) {
                return Err(invalid(
                    "replica has not processed the required promotion offset",
                ));
            }
            if s.upstream.is_some() {
                s.secondary = Some((s.history.clone(), s.offset.saturating_add(1)));
                s.history = history_id();
                s.upstream = None;
                s.connected = false;
                s.generation = s.generation.wrapping_add(1);
            }
            s.failover = None;
            s.generation
        };
        self.transition_state(generation, false, true).await?;
        self.control.send_replace(generation);
        self.changed.notify_waiters();
        self.backend.pause_writes(false)
    }
    pub async fn handle_command(
        self: &Arc<Self>,
        args: &[Bytes],
        client: &mut ClientState,
        last_write_offset: u64,
        nonblocking: bool,
        cluster_enabled: bool,
        unblock: Option<watch::Receiver<Option<bool>>>,
    ) -> CommandResult {
        if args.is_empty() {
            return reply_error("ERR empty command");
        }
        let name = args[0].to_ascii_uppercase();
        match name.as_slice() {
            b"PSYNC" | b"SYNC" => CommandResult::Downstream,
            b"REPLCONF" => self.replconf(args, client),
            b"REPLICAOF" | b"SLAVEOF" => {
                if args.len() != 3 {
                    return arity();
                }
                if cluster_enabled {
                    return reply_error("ERR REPLICAOF not allowed in cluster mode");
                }
                if self.status().failover_active {
                    return reply_error("ERR failover is in progress");
                }
                if eq(&args[1], b"NO") && eq(&args[2], b"ONE") {
                    return result_ok(self.promote().await);
                }
                let Ok(host) = std::str::from_utf8(&args[1]) else {
                    return reply_error("ERR invalid master host");
                };
                let Ok(port) = number::<u16>(&args[2]) else {
                    return reply_error("ERR invalid master port");
                };
                if self
                    .status()
                    .upstream
                    .as_ref()
                    .is_some_and(|(h, p)| h == host && *p == port)
                {
                    return CommandResult::Reply(Resp::Simple(
                        "OK Already connected to specified master",
                    ));
                }
                result_ok(
                    self.follow(self.configured_upstream(host.into(), port))
                        .await,
                )
            }
            b"WAIT" => {
                if args.len() != 3 {
                    return arity();
                }
                let (Ok(count), Ok(timeout)) = (number::<u64>(&args[1]), number::<u64>(&args[2]))
                else {
                    return integer_error();
                };
                self.wait(
                    last_write_offset,
                    0,
                    count,
                    timeout,
                    false,
                    nonblocking,
                    unblock,
                )
                .await
            }
            b"WAITAOF" => {
                if args.len() != 4 {
                    return arity();
                }
                let (Ok(local), Ok(count), Ok(timeout)) = (
                    number::<u64>(&args[1]),
                    number::<u64>(&args[2]),
                    number::<u64>(&args[3]),
                ) else {
                    return integer_error();
                };
                if local > 1 {
                    return reply_error("ERR numlocal must be 0 or 1");
                }
                self.wait(
                    last_write_offset,
                    local,
                    count,
                    timeout,
                    true,
                    nonblocking,
                    unblock,
                )
                .await
            }
            b"FAILOVER" => self.failover(args, cluster_enabled, false, None).await,
            _ => reply_error("ERR unknown replication command"),
        }
    }
    fn replconf(&self, args: &[Bytes], client: &mut ClientState) -> CommandResult {
        if args.len().is_multiple_of(2) {
            return reply_error("ERR syntax error");
        }
        let mut next = client.clone();
        for pair in args[1..].as_chunks::<2>().0 {
            if eq(&pair[0], b"listening-port") {
                let Ok(port) = number::<u16>(&pair[1]) else {
                    return integer_error();
                };
                next.listening_port = port;
            } else if eq(&pair[0], b"ip-address") {
                let Ok(ip) = std::str::from_utf8(&pair[1]) else {
                    return reply_error("ERR invalid replica address");
                };
                if ip.len() > 253 || ip.bytes().any(|b| b.is_ascii_control()) {
                    return reply_error("ERR invalid replica address");
                }
                next.announce_ip = Some(ip.into());
            } else if eq(&pair[0], b"capa") {
                if eq(&pair[1], b"eof") {
                    next.eof = true;
                }
                if eq(&pair[1], b"psync2") {
                    next.psync2 = true;
                }
            } else if eq(&pair[0], b"ACK") {
                // An ordinary client never becomes an acknowledged replica.
                return CommandResult::NoReply;
            } else if eq(&pair[0], b"GETACK") {
                self.ack_requested.notify_one();
                return CommandResult::NoReply;
            } else if eq(&pair[0], b"rdb-only")
                || eq(&pair[0], b"rdb-channel")
                || eq(&pair[0], b"rdb-no-compress")
                || eq(&pair[0], b"rdb-no-checksum")
            {
                let Ok(value) = number::<u8>(&pair[1]) else {
                    return integer_error();
                };
                if value > 1 {
                    return reply_error("ERR argument must be 0 or 1");
                }
                if eq(&pair[0], b"rdb-only") {
                    next.rdb_only = value == 1;
                } else if eq(&pair[0], b"rdb-channel") {
                    next.rdb_channel = value == 1;
                } else if eq(&pair[0], b"rdb-no-compress") {
                    next.snapshot.no_compress = value == 1;
                } else {
                    next.snapshot.no_checksum = value == 1;
                }
            } else if eq(&pair[0], b"rdb-filter-only") {
                let Ok(filters) = std::str::from_utf8(&pair[1]) else {
                    return reply_error("ERR invalid RDB filter");
                };
                next.snapshot.exclude_data = true;
                next.snapshot.exclude_functions = true;
                for filter in filters.split_ascii_whitespace() {
                    if !filter.eq_ignore_ascii_case("functions") {
                        return reply_error("ERR unsupported RDB filter");
                    }
                    next.snapshot.exclude_functions = false;
                }
            } else if eq(&pair[0], b"main-ch-client-id") {
                if number::<u64>(&pair[1]).is_err() {
                    return integer_error();
                }
                // We do not advertise rdb-channel-repl, so no client can be in
                // WAIT_RDB_CHANNEL and there is no eligible pairing ID.
                return reply_error("ERR Unrecognized RDB client id");
            } else if eq(&pair[0], b"compression") {
                if number::<i64>(&pair[1]).is_err() {
                    return integer_error();
                }
                return reply_error("ERR Master has not enabled replication compression");
            } else {
                return reply_error("ERR Unrecognized REPLCONF option");
            }
        }
        *client = next;
        CommandResult::Reply(Resp::Simple("OK"))
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "WAIT and WAITAOF share independent watermark, acknowledgment targets, timeout, durability, and exact-client unblock state."
    )]
    async fn wait(
        &self,
        watermark: u64,
        local: u64,
        replicas: u64,
        timeout: u64,
        durable: bool,
        immediate: bool,
        mut unblock: Option<watch::Receiver<Option<bool>>>,
    ) -> CommandResult {
        let deadline = if timeout == 0 {
            None
        } else {
            Instant::now().checked_add(Duration::from_millis(timeout))
        };
        if replicas > i64::MAX as u64
            || timeout > i64::MAX as u64
            || (timeout != 0 && deadline.is_none())
        {
            return integer_error();
        }
        let mut requested_ack = false;
        loop {
            let notified = self.changed.notified();
            let (local_count, replica_count) = {
                let s = self.state.lock().unwrap();
                if s.upstream.is_some() {
                    return reply_error("ERR WAIT cannot be used with replica instances");
                }
                if durable && local == 1 && !s.aof {
                    return reply_error("ERR WAITAOF cannot be used when AOF is disabled");
                }
                (
                    u64::from(s.aof && s.fsynced >= watermark),
                    s.replicas
                        .values()
                        .filter(|r| {
                            r.online
                                && if durable {
                                    r.fsynced >= watermark
                                } else {
                                    r.processed >= watermark
                                }
                        })
                        .count() as u64,
                )
            };
            let cancelled = unblock.as_ref().and_then(|receiver| *receiver.borrow());
            if cancelled == Some(true) {
                return reply_error("UNBLOCKED client unblocked via CLIENT UNBLOCK");
            }
            if immediate
                || (replica_count >= replicas && (!durable || local_count >= local))
                || deadline.is_some_and(|d| Instant::now() >= d)
                || *self.stopped.borrow()
                || cancelled == Some(false)
            {
                return CommandResult::Reply(if durable {
                    Resp::Array(Some(vec![
                        Resp::Integer(local_count as i64),
                        Resp::Integer(replica_count as i64),
                    ]))
                } else {
                    Resp::Integer(replica_count as i64)
                });
            }
            if !requested_ack {
                if let Err(e) = self.request_ack() {
                    return reply_error(&format!("ERR {e}"));
                }
                requested_ack = true;
                continue;
            }
            tokio::select! {
                _ = notified => {},
                _ = async {
                    match deadline {
                        Some(d) => tokio::time::sleep_until(tokio::time::Instant::from_std(d)).await,
                        None => std::future::pending().await,
                    }
                } => {},
                _ = async {
                    match &mut unblock {
                        Some(receiver) => { let _ = receiver.changed().await; },
                        None => std::future::pending().await,
                    }
                } => {},
            }
        }
    }
    fn request_ack(&self) -> io::Result<()> {
        let mut s = self.state.lock().unwrap();
        // Chained replicas must forward the upstream byte history unchanged.
        if s.upstream.is_some() {
            return Ok(());
        }
        append(
            &mut s,
            encode(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"GETACK"),
                Bytes::from_static(b"*"),
            ]),
            self.config.backlog_bytes,
        )?;
        drop(s);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn unfreeze_migration(&self) -> io::Result<()> {
        self.backend.unfreeze_migration()
    }
    pub(crate) fn detach_recovered_primary_history(&self, history: &[u8], offset: u64) {
        let mut s = self.state.lock().unwrap();
        if s.upstream.is_none() && s.history.as_bytes() == history && s.offset == offset {
            s.secondary = Some((s.history.clone(), offset.saturating_add(1)));
            s.history = history_id();
        }
    }
    pub(crate) fn follow_paused(
        &self,
        upstream: Upstream,
        position: Option<CommittedPosition>,
    ) -> io::Result<()> {
        upstream.validate()?;
        let generation = {
            let mut s = self.state.lock().unwrap();
            reconcile_paused_position(&mut s, position)?;
            let cancelled = s.failover.take().is_some();
            s.upstream = Some(upstream);
            s.connected = false;
            s.generation = s.generation.wrapping_add(1);
            if cancelled {
                self.backend.pause_writes(false)?;
            }
            s.generation
        };
        self.control.send_replace(generation);
        self.changed.notify_waiters();
        Ok(())
    }
    pub(crate) fn promote_paused(
        &self,
        required_offset: Option<u64>,
        position: Option<CommittedPosition>,
    ) -> io::Result<()> {
        let generation = {
            let mut s = self.state.lock().unwrap();
            if s.force_full_sync {
                return Err(invalid(
                    "replica requires full synchronization before promotion",
                ));
            }
            let processed = if s.upstream.is_some() {
                position
                    .as_ref()
                    .map_or(s.offset, |position| position.offset)
            } else {
                s.offset
            };
            if required_offset.is_some_and(|required| processed < required) {
                return Err(invalid("replica has not reached the required offset"));
            }
            reconcile_paused_position(&mut s, position)?;
            if s.upstream.is_some() {
                s.secondary = Some((s.history.clone(), s.offset.saturating_add(1)));
                s.history = history_id();
                s.upstream = None;
                s.connected = false;
            }
            s.generation = s.generation.wrapping_add(1);
            s.failover = None;
            self.backend.pause_writes(false)?;
            s.generation
        };
        self.control.send_replace(generation);
        self.changed.notify_waiters();
        Ok(())
    }
    /// Actor-owned role changes cannot acquire the reconfiguration lease: its
    /// current owner may itself be waiting for this actor. Advancing generation
    /// fences those queued jobs before this actor updates its paused engines.
    pub(crate) async fn configure_paused(
        self: &Arc<Self>,
        args: &[Bytes],
        cluster: bool,
        position: Option<CommittedPosition>,
    ) -> Option<Resp> {
        let name = args.first()?;
        let result = if eq(name, b"FAILOVER") {
            self.failover(args, cluster, true, position).await
        } else if eq(name, b"REPLICAOF") || eq(name, b"SLAVEOF") {
            if args.len() != 3 {
                arity()
            } else if cluster {
                reply_error("ERR REPLICAOF not allowed in cluster mode")
            } else if self.status().failover_active {
                reply_error("ERR failover is in progress")
            } else if eq(&args[1], b"NO") && eq(&args[2], b"ONE") {
                result_ok(self.promote_paused(None, position))
            } else {
                let upstream = std::str::from_utf8(&args[1])
                    .map_err(|_| invalid("invalid master host"))
                    .and_then(|host| {
                        number::<u16>(&args[2])
                            .map(|port| self.configured_upstream(host.into(), port))
                            .map_err(|_| invalid("invalid master port"))
                    });
                match upstream.and_then(|upstream| {
                    upstream.validate()?;
                    Ok(upstream)
                }) {
                    Err(error) => result_ok(Err(error)),
                    Ok(upstream)
                        if self.status().upstream.as_ref().is_some_and(|(host, port)| {
                            host == &upstream.host && *port == upstream.port
                        }) =>
                    {
                        CommandResult::Reply(Resp::Simple(
                            "OK Already connected to specified master",
                        ))
                    }
                    Ok(upstream) => result_ok(self.follow_paused(upstream, position)),
                }
            }
        } else {
            return None;
        };
        match result {
            CommandResult::Reply(reply) => Some(reply),
            CommandResult::NoReply | CommandResult::Downstream => {
                unreachable!("role commands always reply")
            }
        }
    }

    async fn failover(
        self: &Arc<Self>,
        args: &[Bytes],
        cluster: bool,
        in_actor: bool,
        position: Option<CommittedPosition>,
    ) -> CommandResult {
        let _lease = if in_actor {
            None
        } else {
            Some(self.reconfigure.clone().lock_owned().await)
        };
        if cluster {
            return reply_error("ERR FAILOVER is not allowed in cluster mode");
        }
        let mut target = None;
        let mut force = false;
        let mut timeout = None;
        let mut abort = false;
        let mut i = 1;
        while i < args.len() {
            if eq(&args[i], b"TO") && i + 2 < args.len() && target.is_none() {
                let (Ok(host), Ok(port)) = (
                    std::str::from_utf8(&args[i + 1]),
                    number::<u16>(&args[i + 2]),
                ) else {
                    return integer_error();
                };
                target = Some((host.to_owned(), port));
                i += 3;
            } else if eq(&args[i], b"FORCE") && !force {
                force = true;
                i += 1;
            } else if eq(&args[i], b"TIMEOUT") && i + 1 < args.len() && timeout.is_none() {
                let Ok(ms) = number::<u64>(&args[i + 1]) else {
                    return integer_error();
                };
                if ms > i64::MAX as u64
                    || Instant::now()
                        .checked_add(Duration::from_millis(ms))
                        .is_none()
                {
                    return integer_error();
                }
                if ms == 0 {
                    return reply_error("ERR timeout must be positive");
                }
                timeout = Some(ms);
                i += 2;
            } else if eq(&args[i], b"ABORT") && !abort {
                abort = true;
                i += 1;
            } else {
                return reply_error("ERR syntax error");
            }
        }
        if abort {
            if args.len() != 2 {
                return reply_error("ERR ABORT cannot be combined with other options");
            }
            let generation = {
                let mut s = self.state.lock().unwrap();
                if s.failover.is_none() {
                    return reply_error("ERR No failover in progress");
                }
                if let Err(error) = reconcile_paused_position(&mut s, position) {
                    return result_ok(Err(error));
                }
                if s.upstream.is_some() {
                    s.upstream = None;
                    s.connected = false;
                }
                s.failover = None;
                s.generation = s.generation.wrapping_add(1);
                if let Err(error) = self.backend.pause_writes(false) {
                    return result_ok(Err(error));
                }
                s.generation
            };
            if !in_actor && let Err(error) = self.transition_state(generation, false, true).await {
                return reply_error(&format!("ERR {error}"));
            }
            self.control.send_replace(generation);
            self.changed.notify_waiters();
            return result_ok(Ok(()));
        }
        if force && (target.is_none() || timeout.is_none()) {
            return reply_error("ERR FORCE requires TO and TIMEOUT");
        }
        let (chosen, token) = {
            let mut s = self.state.lock().unwrap();
            if s.upstream.is_some() {
                return reply_error("ERR FAILOVER must be sent to a master");
            }
            if s.failover.is_some() {
                return reply_error("ERR failover is already in progress");
            }
            let chosen = s
                .replicas
                .values()
                .filter(|r| r.online && r.port != 0)
                .filter(|r| {
                    target
                        .as_ref()
                        .is_none_or(|(host, port)| r.host == *host && r.port == *port)
                })
                .max_by_key(|r| r.processed)
                .cloned();
            let Some(chosen) = chosen else {
                return reply_error("ERR No suitable replica for failover");
            };
            s.generation = s.generation.wrapping_add(1);
            let token = s.generation;
            s.failover = Some(token);
            (chosen, token)
        };
        // Never acquire the shared commit gate while holding history state:
        // a committing actor publishes under that gate and needs this mutex.
        if !in_actor && let Err(e) = self.backend.drain_writes().await {
            let mut s = self.state.lock().unwrap();
            if s.failover == Some(token) {
                s.failover = None;
            }
            if s.failover.is_none() {
                let _ = self.backend.pause_writes(false);
            }
            return reply_error(&format!("ERR {e}"));
        }
        let this = self.clone();
        tokio::spawn(async move {
            {
                let s = this.state.lock().unwrap();
                if s.failover != Some(token) {
                    if s.failover.is_none() {
                        let _ = this.backend.pause_writes(false);
                    }
                    return;
                }
            }
            if in_actor && let Err(error) = this.backend.drain_writes().await {
                let mut s = this.state.lock().unwrap();
                if s.failover == Some(token) {
                    s.failover = None;
                }
                if s.failover.is_none() {
                    let _ = this.backend.pause_writes(false);
                }
                drop(s);
                eprintln!("native failover write drain failed: {error}");
                return;
            }
            let watermark = {
                let s = this.state.lock().unwrap();
                if s.failover != Some(token) {
                    if s.failover.is_none() {
                        let _ = this.backend.pause_writes(false);
                    }
                    return;
                }
                s.offset
            };
            let deadline =
                timeout.and_then(|t| Instant::now().checked_add(Duration::from_millis(t)));
            loop {
                let notified = this.changed.notified();
                let (active, ready) = {
                    let s = this.state.lock().unwrap();
                    (
                        s.failover == Some(token),
                        s.replicas
                            .get(&chosen.id)
                            .is_some_and(|r| r.online && r.processed >= watermark),
                    )
                };
                if !active {
                    return;
                }
                let expired = deadline.is_some_and(|d| Instant::now() >= d);
                if ready || (expired && force) {
                    let _lease = this.reconfigure.clone().lock_owned().await;
                    if *this.stopped.borrow() {
                        return;
                    }
                    let generation = {
                        let mut s = this.state.lock().unwrap();
                        if s.failover != Some(token) {
                            return;
                        }
                        s.upstream =
                            Some(this.configured_upstream(chosen.host.clone(), chosen.port));
                        s.connected = false;
                        s.generation
                    };
                    if let Err(error) = this.transition_state(generation, true, false).await {
                        eprintln!("native replication role transition failed: {error}");
                        return;
                    }
                    this.control.send_replace(generation);
                    return;
                }
                if expired || *this.stopped.borrow() {
                    let mut s = this.state.lock().unwrap();
                    if s.failover == Some(token) {
                        s.failover = None;
                        let _ = this.backend.pause_writes(false);
                        this.changed.notify_waiters();
                    }
                    return;
                }
                // Do not wake our own waiter by appending another GETACK here:
                // the downstream heartbeat requests ACKs once per second.
                let sleep = deadline
                    .map(|d| d.saturating_duration_since(Instant::now()))
                    .unwrap_or(Duration::from_secs(1))
                    .min(Duration::from_secs(1));
                let _ = tokio::time::timeout(sleep, notified).await;
            }
        });
        CommandResult::Reply(Resp::Simple("OK"))
    }
}

fn append(s: &mut State, bytes: Bytes, max: usize) -> io::Result<u64> {
    if bytes.is_empty() {
        return Ok(s.offset);
    }
    let first = s
        .offset
        .checked_add(1)
        .ok_or_else(|| invalid("replication offset overflow"))?;
    s.offset = s
        .offset
        .checked_add(bytes.len() as u64)
        .ok_or_else(|| invalid("replication offset overflow"))?;
    s.backlog_len = s
        .backlog_len
        .checked_add(bytes.len())
        .ok_or_else(|| invalid("replication backlog overflow"))?;
    s.backlog.push_back(Chunk {
        first,
        published_first: first,
        bytes,
    });
    while s.backlog_len > max {
        let remove = s.backlog_len - max;
        let front = s.backlog.front_mut().unwrap();
        if front.bytes.len() <= remove {
            s.backlog_len -= front.bytes.len();
            s.backlog.pop_front();
        } else {
            front.bytes = Bytes::copy_from_slice(&front.bytes[remove..]);
            front.first += remove as u64;
            s.backlog_len -= remove;
        }
    }
    Ok(s.offset)
}
fn backlog(s: &State, next: u64) -> Option<Vec<Bytes>> {
    if next == s.offset.checked_add(1)? {
        return Some(Vec::new());
    }
    if next == 0 || next > s.offset || next < s.backlog.front()?.first {
        return None;
    }
    Some(
        s.backlog
            .iter()
            .filter_map(|c| {
                let end = c.first + c.bytes.len() as u64;
                if next >= end {
                    None
                } else {
                    Some(c.bytes.slice(next.saturating_sub(c.first) as usize..))
                }
            })
            .collect(),
    )
}
fn partial(s: &State, id: &str, next: u64) -> bool {
    (id == s.history
        || s.secondary
            .as_ref()
            .is_some_and(|(old, through)| id == old && next <= *through))
        && backlog(s, next).is_some()
}
fn history_id() -> String {
    use rand::RngCore;
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0u8; 20];
    rand::rng().fill_bytes(&mut bytes);
    let mut id = String::with_capacity(40);
    for b in bytes {
        id.push(HEX[(b >> 4) as usize] as char);
        id.push(HEX[(b & 15) as usize] as char);
    }
    id
}
fn valid_history(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}
fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn eq(a: &[u8], b: &[u8]) -> bool {
    a.eq_ignore_ascii_case(b)
}
fn number<T: std::str::FromStr>(b: &[u8]) -> Result<T, ()> {
    std::str::from_utf8(b)
        .map_err(|_| ())?
        .parse()
        .map_err(|_| ())
}
fn reply_error(message: &str) -> CommandResult {
    CommandResult::Reply(Resp::Error(message.into()))
}
fn integer_error() -> CommandResult {
    reply_error("ERR value is not an integer or out of range")
}
fn arity() -> CommandResult {
    reply_error("ERR wrong number of arguments")
}
fn result_ok(result: io::Result<()>) -> CommandResult {
    match result {
        Ok(()) => CommandResult::Reply(Resp::Simple("OK")),
        Err(e) => reply_error(&format!("ERR {e}")),
    }
}
/// Encodes trusted internal handshake/control commands.
pub fn encode(args: &[Bytes]) -> Bytes {
    let mut out = BytesMut::new();
    write_command(&mut out, args).expect("BytesMut writer is infallible");
    out.freeze()
}
/// Shared bounded native encoder for ephemeral commands ordered with commits.
pub fn encode_command(args: &[Bytes]) -> io::Result<Bytes> {
    let length = command_length(args)?;
    let mut out = BytesMut::with_capacity(length);
    write_command(&mut out, args)?;
    Ok(out.freeze())
}
fn command_length(args: &[Bytes]) -> io::Result<usize> {
    fn digits(n: usize) -> usize {
        if n == 0 { 1 } else { n.ilog10() as usize + 1 }
    }
    if args.is_empty() || args.len() > 1_048_576 {
        return Err(invalid("replication argument count exceeds bound"));
    }
    let mut length = digits(args.len()) + 3;
    for arg in args {
        length = length
            .checked_add(arg.len())
            .and_then(|n| n.checked_add(digits(arg.len()) + 5))
            .filter(|n| *n <= MAX_REPLICATION_COMMAND_BYTES)
            .ok_or_else(|| invalid("replication command exceeds bound"))?;
    }
    Ok(length)
}
fn write_command(out: &mut BytesMut, args: &[Bytes]) -> io::Result<()> {
    use bytes::BufMut;
    use std::io::Write;
    write!((&mut *out).writer(), "*{}\r\n", args.len())?;
    for arg in args {
        write!((&mut *out).writer(), "${}\r\n", arg.len())?;
        out.extend_from_slice(arg);
        out.extend_from_slice(b"\r\n");
    }
    Ok(())
}

impl NativeReplication {
    /// Must be called only after normal replies have drained. `input` contains
    /// any already-buffered transport bytes following the PSYNC/SYNC request.
    pub async fn serve_downstream<S>(
        self: &Arc<Self>,
        stream: S,
        input: BytesMut,
        args: &[Bytes],
        client: ClientState,
        peer_ip: IpAddr,
    ) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send,
    {
        let legacy = args.first().is_some_and(|a| eq(a, b"SYNC"));
        let mut stream = stream;
        if (legacy && args.len() != 1) || (!legacy && args.len() != 3 && args.len() != 4) {
            stream
                .write_all(b"-ERR wrong number of arguments\r\n")
                .await?;
            return Ok(());
        }
        let failover = !legacy && args.len() == 4 && eq(&args[3], b"FAILOVER");
        if !legacy && args.len() == 4 && !failover {
            stream.write_all(b"-ERR syntax error\r\n").await?;
            return Ok(());
        }
        if failover {
            let accepted = {
                let _lease = self.reconfigure.clone().lock_owned().await;
                let eligible = {
                    let s = self.state.lock().unwrap();
                    s.upstream.is_some()
                        && std::str::from_utf8(&args[1])
                            .ok()
                            .is_some_and(|id| id == s.history)
                };
                if eligible {
                    self.promote_locked(None).await?;
                }
                eligible
            };
            if !accepted {
                stream
                    .write_all(b"-ERR FAILOVER cannot be accepted for this history\r\n")
                    .await?;
                return Ok(());
            }
        }
        let status = self.status();
        if status.role == Role::Replica && !status.upstream_connected {
            stream
                .write_all(b"-NOMASTERLINK Can't SYNC while not connected with my master\r\n")
                .await?;
            return Ok(());
        }
        let requested = if legacy {
            None
        } else {
            match (std::str::from_utf8(&args[1]), number::<u64>(&args[2])) {
                (Ok(id), Ok(offset)) => Some((id.to_owned(), offset)),
                _ if args[1].as_ref() == b"?" && args[2].as_ref() == b"-1" => None,
                _ => {
                    // Redis accepts arbitrary IDs and the -1 full-sync sentinel.
                    if args[2].as_ref() != b"-1" {
                        stream
                            .write_all(b"-ERR invalid replication offset\r\n")
                            .await?;
                        return Ok(());
                    }
                    None
                }
            }
        };
        let accepted = {
            let s = self.state.lock().unwrap();
            requested
                .as_ref()
                .filter(|(id, next)| !client.rdb_only && partial(&s, id, *next))
                .map(|(_, next)| (*next, s.history.clone(), s.history_epoch))
        };
        let mut next;
        let epoch;
        if let Some((offset, id, accepted_epoch)) = accepted {
            epoch = accepted_epoch;
            let line = if client.psync2 {
                format!("+CONTINUE {id}\r\n")
            } else {
                "+CONTINUE\r\n".into()
            };
            stream.write_all(line.as_bytes()).await?;
            next = offset;
        } else {
            let snapshot = {
                let generation = self.generation();
                let _lease = self.generation_guard(generation).await?;
                epoch = self.history_epoch();
                let snapshot = self.backend.snapshot(client.snapshot).await?;
                self.check_history_epoch(epoch)?;
                snapshot
            };
            if !valid_history(&snapshot.history)
                || snapshot.length < 18
                || snapshot.length > self.config.max_snapshot_bytes
            {
                return Err(invalid("invalid replication snapshot metadata"));
            }
            // Check that the file and reported length agree before emitting a
            // header. Snapshot consistency itself is the actor barrier's job.
            let file = tokio::fs::File::open(&snapshot.path).await?;
            if file.metadata().await?.len() != snapshot.length {
                return Err(invalid("snapshot length changed"));
            }
            if !legacy {
                stream
                    .write_all(
                        format!("+FULLRESYNC {} {}\r\n", snapshot.history, snapshot.offset)
                            .as_bytes(),
                    )
                    .await?;
            }
            stream
                .write_all(format!("${}\r\n", snapshot.length).as_bytes())
                .await?;
            let sent = timed(
                self.config.io_timeout,
                tokio::io::copy(&mut file.take(snapshot.length), &mut stream),
            )
            .await?;
            if sent != snapshot.length {
                return Err(invalid("truncated replication snapshot"));
            }
            next = snapshot
                .offset
                .checked_add(1)
                .ok_or_else(|| invalid("replication offset overflow"))?;
        }
        stream.flush().await?;
        if client.rdb_only {
            return Ok(());
        }
        let replica_id = {
            let mut s = self.state.lock().unwrap();
            check_history_epoch(&s, epoch)?;
            let id = s.next_replica;
            s.next_replica = s
                .next_replica
                .checked_add(1)
                .ok_or_else(|| invalid("replica ID overflow"))?;
            s.replicas.insert(
                id,
                ReplicaStatus {
                    id,
                    host: client.announce_ip.unwrap_or_else(|| peer_ip.to_string()),
                    port: client.listening_port,
                    processed: 0,
                    fsynced: 0,
                    online: false,
                    last_ack: Instant::now(),
                },
            );
            id
        };
        let result = self
            .downstream_loop(&mut stream, input, replica_id, &mut next, epoch)
            .await;
        self.state.lock().unwrap().replicas.remove(&replica_id);
        self.changed.notify_waiters();
        result
    }

    async fn downstream_loop<S>(
        &self,
        stream: &mut S,
        mut input: BytesMut,
        replica: u64,
        next: &mut u64,
        epoch: u64,
    ) -> io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin,
    {
        let mut shutdown = self.stopped.subscribe();
        let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
        loop {
            let changed = self.changed.notified();
            // Cloned Bytes retain at most one bounded backlog, not a database
            // snapshot or an unbounded per-replica output queue.
            let pending = {
                let s = self.state.lock().unwrap();
                check_history_epoch(&s, epoch)?;
                backlog(&s, *next)
                    .ok_or_else(|| invalid("replica fell outside replication backlog"))?
            };
            for bytes in pending {
                timed(self.config.io_timeout, stream.write_all(&bytes)).await?;
                *next = next
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("replication offset overflow"))?;
            }
            while let Some((args, _)) = parse_frame(&mut input)? {
                self.downstream_ack(replica, &args, epoch)?;
            }
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = changed => {},
                _ = heartbeat.tick() => {
                    // Request an ACK also for idle streams so offline peers do
                    // not remain qualified merely because no writes occur.
                    let stale = self.state.lock().unwrap().replicas.get(&replica)
                        .is_some_and(|r| r.last_ack.elapsed() > self.config.io_timeout);
                    if stale { return Err(io::Error::new(io::ErrorKind::TimedOut, "replica ACK timeout")); }
                    self.request_ack()?;
                },
                result = stream.read_buf(&mut input) => {
                    if result? == 0 { return Ok(()); }
                    if input.len() > MAX_REPLICATION_COMMAND_BYTES { return Err(invalid("replication ACK exceeds input bound")); }
                }
            }
        }
    }
    fn downstream_ack(&self, replica: u64, args: &[Bytes], epoch: u64) -> io::Result<()> {
        if args.len() != 3 && args.len() != 5 {
            return Err(invalid("invalid replica acknowledgement"));
        }
        if !eq(&args[0], b"REPLCONF") || !eq(&args[1], b"ACK") {
            return Err(invalid("unexpected replica command"));
        }
        let processed = number::<u64>(&args[2]).map_err(|_| invalid("invalid ACK offset"))?;
        let fsynced = if args.len() == 5 {
            if !eq(&args[3], b"FACK") {
                return Err(invalid("invalid fsync acknowledgement"));
            }
            Some(number::<u64>(&args[4]).map_err(|_| invalid("invalid FACK offset"))?)
        } else {
            None
        };
        let mut s = self.state.lock().unwrap();
        check_history_epoch(&s, epoch)?;
        if processed > s.offset || fsynced.is_some_and(|f| f > processed) {
            return Err(invalid("replica acknowledged unprocessed bytes"));
        }
        if let Some(r) = s.replicas.get_mut(&replica) {
            r.processed = r.processed.max(processed);
            if let Some(f) = fsynced {
                r.fsynced = r.fsynced.max(f);
            }
            r.online = true;
            r.last_ack = Instant::now();
        }
        drop(s);
        self.changed.notify_waiters();
        Ok(())
    }

    async fn supervise(self: Arc<Self>) {
        let mut control = self.control.subscribe();
        let mut shutdown = self.stopped.subscribe();
        loop {
            let stopping = *shutdown.borrow();
            if stopping {
                self.drain().await;
                return;
            }
            let (upstream, generation, failover) = {
                let s = self.state.lock().unwrap();
                (s.upstream.clone(), s.generation, s.failover.is_some())
            };
            let Some(upstream) = upstream else {
                tokio::select! {
                    _ = control.changed() => {},
                    _ = shutdown.changed() => { self.drain().await; return; }
                }
                continue;
            };
            let result = tokio::select! {
                result = self.upstream_session(&upstream, generation, failover) => result,
                _ = control.changed() => continue,
                _ = shutdown.changed() => { self.drain().await; return; },
            };
            {
                let _lease = self.reconfigure.clone().lock_owned().await;
                let rollback = {
                    let mut s = self.state.lock().unwrap();
                    if s.generation == generation {
                        s.connected = false;
                        if failover && s.failover.is_some() && result.is_err() {
                            s.upstream = None;
                            s.failover = None;
                            true
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };
                if rollback {
                    if let Err(error) = self.transition_state(generation, false, true).await {
                        eprintln!("native replication role rollback failed: {error}");
                        return;
                    }
                    let _ = self.backend.pause_writes(false);
                }
            }
            self.changed.notify_waiters();
            tokio::select! {
                _ = tokio::time::sleep(self.config.reconnect_delay) => {},
                _ = control.changed() => {},
                _ = shutdown.changed() => { self.drain().await; return; },
            }
        }
    }

    async fn connect(
        &self,
        upstream: &Upstream,
    ) -> io::Result<(Box<dyn Transport>, SocketAddr, SocketAddr)> {
        upstream.validate()?;
        let addresses = timed(
            self.config.io_timeout,
            tokio::net::lookup_host((upstream.host.as_str(), upstream.port)),
        )
        .await?;
        let mut last = invalid("upstream has no permitted addresses");
        for address in addresses {
            if !permitted(
                address.ip(),
                self.config.allow_loopback,
                self.config.allow_private,
            ) {
                let _ = self.connect_denied.try_update(
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                    |count| Some(count.saturating_add(1)),
                );
                self.changed.notify_waiters();
                continue;
            }
            match timed(self.config.io_timeout, TcpStream::connect(address)).await {
                Ok(stream) => {
                    stream.set_nodelay(true)?;
                    let peer = stream.peer_addr()?;
                    let local = stream.local_addr()?;
                    if let Some(config) = &upstream.tls {
                        let name = rustls::pki_types::ServerName::try_from(
                            upstream
                                .tls_server_name
                                .clone()
                                .unwrap_or_else(|| upstream.host.clone()),
                        )
                        .map_err(|_| invalid("invalid upstream TLS name"))?;
                        let tls = timed(
                            self.config.io_timeout,
                            tokio_rustls::TlsConnector::from(config.clone()).connect(name, stream),
                        )
                        .await?;
                        return Ok((Box::new(tls), peer, local));
                    }
                    return Ok((Box::new(stream), peer, local));
                }
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    async fn upstream_session(
        self: &Arc<Self>,
        upstream: &Upstream,
        generation: u64,
        failover: bool,
    ) -> io::Result<()> {
        let (stream, peer, local) = self.connect(upstream).await?;
        let source = self.backend.source_connection(peer, local)?;
        let mut cancelled = source.cancellation();
        if *cancelled.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "source client killed",
            ));
        }
        tokio::select! {
            biased;
            _ = cancelled.changed() => Err(io::Error::new(io::ErrorKind::Interrupted, "source client killed")),
            result = self.upstream_link(stream, upstream, generation, failover) => result,
        }
    }

    async fn upstream_link(
        self: &Arc<Self>,
        stream: Box<dyn Transport>,
        upstream: &Upstream,
        generation: u64,
        failover: bool,
    ) -> io::Result<()> {
        let mut stream = BufReader::with_capacity(64 * 1024, stream);
        if let Some(password) = &upstream.password {
            let mut auth = vec![Bytes::from_static(b"AUTH")];
            if let Some(username) = &upstream.username {
                auth.push(Bytes::copy_from_slice(username.as_bytes()));
            }
            auth.push(Bytes::copy_from_slice(password.as_bytes()));
            self.exchange(&mut stream, &auth).await?;
        }
        self.exchange(&mut stream, &[Bytes::from_static(b"PING")])
            .await?;
        self.exchange(
            &mut stream,
            &[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"listening-port"),
                Bytes::from(self.config.listening_port.to_string()),
            ],
        )
        .await?;
        if let Some(ip) = &self.config.announce_ip {
            self.exchange(
                &mut stream,
                &[
                    Bytes::from_static(b"REPLCONF"),
                    Bytes::from_static(b"ip-address"),
                    Bytes::copy_from_slice(ip.as_bytes()),
                ],
            )
            .await?;
        }
        self.exchange(
            &mut stream,
            &[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"eof"),
                Bytes::from_static(b"capa"),
                Bytes::from_static(b"psync2"),
            ],
        )
        .await?;
        let (history, position) = {
            let s = self.state.lock().unwrap();
            if s.force_full_sync {
                ("?".to_owned(), "-1".to_owned())
            } else {
                (s.history.clone(), s.offset.saturating_add(1).to_string())
            }
        };
        let mut psync = vec![
            Bytes::from_static(b"PSYNC"),
            Bytes::from(history),
            Bytes::from(position),
        ];
        if failover {
            psync.push(Bytes::from_static(b"FAILOVER"));
        }
        timed(self.config.io_timeout, stream.write_all(&encode(&psync))).await?;
        let header = self.line(&mut stream).await?;
        if header.starts_with(b"+FULLRESYNC ") {
            let text =
                std::str::from_utf8(&header).map_err(|_| invalid("invalid full resync header"))?;
            let fields: Vec<_> = text.trim_end().split_ascii_whitespace().collect();
            if fields.len() != 3 || !valid_history(fields[1]) {
                return Err(invalid("invalid full resync history"));
            }
            let offset: u64 = fields[2]
                .parse()
                .map_err(|_| invalid("invalid full resync offset"))?;
            let stage = self.receive_snapshot(&mut stream).await?;
            let this = self.clone();
            let history = fields[1].to_owned();
            // The detached job retains both file and generation lease through
            // durable actor completion and publication of the native position.
            tokio::spawn(async move {
                let _lease = this.generation_guard(generation).await?;
                let applied = match this
                    .backend
                    .install_snapshot(stage.path.clone(), history.clone(), offset, generation)
                    .await
                {
                    Ok(applied) => applied,
                    Err(error) => return Err(this.require_full_sync(generation, error).await),
                };
                if applied.fsynced_offset > offset {
                    return Err(this
                        .require_full_sync(
                            generation,
                            invalid("snapshot fsync offset exceeds snapshot offset"),
                        )
                        .await);
                }
                let s = this.state.lock().unwrap();
                if s.generation != generation {
                    return Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "stale snapshot publication generation",
                    ));
                }
                if s.history != history || s.offset != offset || s.fsynced < applied.fsynced_offset
                {
                    return Err(invalid(
                        "snapshot backend did not commit its advertised position",
                    ));
                }
                drop(s);
                drop(stage);
                this.changed.notify_waiters();
                Ok(())
            })
            .await
            .map_err(|_| invalid("snapshot installer task failed"))??;
        } else if header.starts_with(b"+CONTINUE") {
            let _lease = self.generation_guard(generation).await?;
            let text =
                std::str::from_utf8(&header).map_err(|_| invalid("invalid continue header"))?;
            let fields: Vec<_> = text.trim_end().split_ascii_whitespace().collect();
            if fields.len() > 2 || fields.first() != Some(&"+CONTINUE") {
                return Err(invalid("invalid continue response"));
            }
            if let Some(id) = fields.get(1) {
                if !valid_history(id) {
                    return Err(invalid("invalid continuation history"));
                }
                let mut s = self.state.lock().unwrap();
                if s.history != *id {
                    s.secondary = Some((s.history.clone(), s.offset.saturating_add(1)));
                    s.history = (*id).to_owned();
                }
            }
        } else {
            return Err(invalid("upstream rejected PSYNC"));
        }
        {
            let _lease = self.generation_guard(generation).await?;
            {
                let mut s = self.state.lock().unwrap();
                s.connected = true;
                if failover {
                    s.failover = None;
                }
            }
            self.transition_state(generation, true, true).await?;
            if failover {
                self.backend.pause_writes(false)?;
            }
        }
        self.changed.notify_waiters();
        self.send_ack(&mut stream).await?;
        let mut input = BytesMut::with_capacity(64 * 1024);
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        let mut idle = tokio::time::Instant::now() + self.config.io_timeout;
        let mut transaction: Option<Vec<(Vec<Bytes>, Bytes)>> = None;
        let mut transaction_bytes = 0usize;
        loop {
            while !input.is_empty() {
                let Some((command, raw)) = parse_frame(&mut input)? else {
                    break;
                };
                let getack = command.len() == 3
                    && eq(&command[0], b"REPLCONF")
                    && eq(&command[1], b"GETACK");
                let multi = command.len() == 1 && eq(&command[0], b"MULTI");
                let exec = command.len() == 1 && eq(&command[0], b"EXEC");
                if multi {
                    if transaction.is_some() {
                        return Err(invalid("nested replication transaction"));
                    }
                    transaction = Some(Vec::new());
                    transaction_bytes = 0;
                }
                if let Some(frames) = transaction.as_mut() {
                    transaction_bytes = transaction_bytes
                        .checked_add(raw.len())
                        .filter(|n| *n <= MAX_REPLICATION_TRANSACTION_BYTES)
                        .ok_or_else(|| invalid("replication transaction exceeds bound"))?;
                    frames.push((command, raw));
                    if !exec {
                        continue;
                    }
                    self.apply_frames(transaction.take().unwrap(), generation)
                        .await?;
                } else {
                    if exec {
                        return Err(invalid("replication EXEC without MULTI"));
                    }
                    self.apply_frames(vec![(command, raw)], generation).await?;
                }
                if getack {
                    self.send_ack(&mut stream).await?;
                }
            }
            tokio::select! {
                result = stream.read_buf(&mut input) => {
                    if result? == 0 { return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "upstream disconnected")); }
                    idle = tokio::time::Instant::now() + self.config.io_timeout;
                    if input.len() > MAX_REPLICATION_COMMAND_BYTES { return Err(invalid("upstream command exceeds input bound")); }
                },
                _ = interval.tick() => self.send_ack(&mut stream).await?,
                _ = self.ack_requested.notified() => self.send_ack(&mut stream).await?,
                _ = tokio::time::sleep_until(idle) => return Err(io::Error::new(io::ErrorKind::TimedOut, "upstream idle timeout")),
            }
        }
    }
    async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        stream: &mut BufReader<S>,
        args: &[Bytes],
    ) -> io::Result<()> {
        timed(self.config.io_timeout, stream.write_all(&encode(args))).await?;
        let line = self.line(stream).await?;
        if line.first() != Some(&b'+') {
            return Err(invalid("upstream handshake rejected"));
        }
        Ok(())
    }
    async fn line<S: AsyncRead + Unpin>(&self, stream: &mut BufReader<S>) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        loop {
            let byte = timed(self.config.io_timeout, stream.read_u8()).await?;
            out.push(byte);
            if out.len() > 4096 {
                return Err(invalid("replication header exceeds bound"));
            }
            if byte == b'\n' {
                if !out.ends_with(b"\r\n") {
                    return Err(invalid("malformed replication header"));
                }
                return Ok(out);
            }
        }
    }
    async fn send_ack<S: AsyncWrite + Unpin>(&self, stream: &mut S) -> io::Result<()> {
        let (offset, fsynced, aof) = {
            let s = self.state.lock().unwrap();
            (s.offset, s.fsynced, s.aof)
        };
        let mut args = vec![
            Bytes::from_static(b"REPLCONF"),
            Bytes::from_static(b"ACK"),
            Bytes::from(offset.to_string()),
        ];
        if aof {
            args.push(Bytes::from_static(b"FACK"));
            args.push(Bytes::from(fsynced.to_string()));
        }
        timed(self.config.io_timeout, stream.write_all(&encode(&args))).await
    }
    async fn receive_snapshot<S: AsyncRead + Unpin>(
        &self,
        stream: &mut BufReader<S>,
    ) -> io::Result<Stage> {
        let header = loop {
            let line = self.line(stream).await?;
            if line.as_slice() != b"\r\n" {
                break line;
            }
        };
        let (stage, output) = Stage::new(&self.config.staging_directory)?;
        let mut output = tokio::fs::File::from_std(output);
        if header.starts_with(b"$EOF:") {
            let marker = &header[5..header.len() - 2];
            if marker.len() != 40 {
                return Err(invalid("invalid RDB EOF marker"));
            }
            let mut pending = Vec::with_capacity(64 * 1024 + 40);
            let mut total = 0u64;
            loop {
                // fill_buf/consume preserve every byte after the delimiter.
                let available = timed(self.config.io_timeout, stream.fill_buf()).await?;
                if available.is_empty() {
                    return Err(invalid("truncated diskless snapshot"));
                }
                let mut used = 0;
                let mut found = false;
                for byte in available {
                    pending.push(*byte);
                    used += 1;
                    if pending.ends_with(marker) {
                        pending.truncate(pending.len() - 40);
                        found = true;
                        break;
                    }
                }
                stream.consume(used);
                let flush = if found {
                    pending.len()
                } else {
                    pending.len().saturating_sub(39)
                };
                total = total
                    .checked_add(flush as u64)
                    .filter(|v| *v <= self.config.max_snapshot_bytes)
                    .ok_or_else(|| invalid("snapshot exceeds configured bound"))?;
                output.write_all(&pending[..flush]).await?;
                pending.drain(..flush);
                if found {
                    break;
                }
            }
        } else {
            if header.first() != Some(&b'$') {
                return Err(invalid("expected snapshot bulk header"));
            }
            let length = number::<u64>(&header[1..header.len() - 2])
                .map_err(|_| invalid("invalid snapshot length"))?;
            if length > self.config.max_snapshot_bytes {
                return Err(invalid("snapshot exceeds configured bound"));
            }
            let mut left = length;
            let mut chunk = [0u8; 64 * 1024];
            while left != 0 {
                let bound = left.min(chunk.len() as u64) as usize;
                let n = timed(self.config.io_timeout, stream.read(&mut chunk[..bound])).await?;
                if n == 0 {
                    return Err(invalid("truncated replication snapshot"));
                }
                output.write_all(&chunk[..n]).await?;
                left -= n as u64;
            }
        }
        output.sync_all().await?;
        Ok(stage)
    }
}
trait Transport: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Transport for T {}
struct Stage {
    path: PathBuf,
}
impl Stage {
    fn new(root: &std::path::Path) -> io::Result<(Self, std::fs::File)> {
        use std::os::unix::fs::OpenOptionsExt;
        let path = root.join(format!("native-repl-{}.rdb", history_id()));
        let file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(0o600)
            .open(&path)?;
        Ok((Self { path }, file))
    }
}
impl Drop for Stage {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}
async fn timed<T>(
    duration: Duration,
    future: impl Future<Output = io::Result<T>>,
) -> io::Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "native replication I/O timeout"))?
}
fn permitted(ip: IpAddr, loopback: bool, private: bool) -> bool {
    if ip.is_unspecified() || ip.is_multicast() {
        return false;
    }
    if ip.is_loopback() {
        return loopback;
    }
    match ip {
        IpAddr::V4(v) => {
            if v.is_broadcast() || v.is_link_local() {
                return false;
            }
            private || !(v.is_private() || v.octets()[0] == 0 || v.octets()[0] >= 240)
        }
        IpAddr::V6(v) => {
            if let Some(v4) = v.to_ipv4_mapped() {
                return permitted(IpAddr::V4(v4), loopback, private);
            }
            private || !(v.is_unique_local() || v.is_unicast_link_local())
        }
    }
}

const MAX_REPLICATION_COMMAND_BYTES: usize = 64 * 1024 * 1024;
type ParsedFrameSpans = (usize, Vec<(usize, usize)>);
/// Parses binary-safe RESP spans without consuming or copying input.
fn frame_spans(input: &[u8]) -> io::Result<Option<ParsedFrameSpans>> {
    fn length(input: &[u8], at: usize) -> io::Result<Option<(usize, usize)>> {
        let Some(end) = input[at..].windows(2).position(|w| w == b"\r\n") else {
            if input.len().saturating_sub(at) > 20 {
                return Err(invalid("replication length exceeds bound"));
            }
            return Ok(None);
        };
        let end = at + end;
        let value =
            number::<usize>(&input[at..end]).map_err(|_| invalid("invalid replication length"))?;
        Ok(Some((value, end + 2)))
    }
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        return Err(invalid("replication command must be a RESP array"));
    }
    let Some((count, mut at)) = length(input, 1)? else {
        return Ok(None);
    };
    if count == 0 || count > 1_048_576 {
        return Err(invalid("replication argument count exceeds bound"));
    }
    let mut spans = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if at >= input.len() {
            return Ok(None);
        }
        if input[at] != b'$' {
            return Err(invalid("replication argument must be a bulk string"));
        }
        let Some((len, start)) = length(input, at + 1)? else {
            return Ok(None);
        };
        let end = start
            .checked_add(len)
            .and_then(|v| v.checked_add(2))
            .filter(|v| *v <= MAX_REPLICATION_COMMAND_BYTES)
            .ok_or_else(|| invalid("replication command exceeds bound"))?;
        if input.len() < end {
            return Ok(None);
        }
        if &input[end - 2..end] != b"\r\n" {
            return Err(invalid("invalid replication bulk terminator"));
        }
        spans.push((start, end - 2));
        at = end;
    }
    Ok(Some((at, spans)))
}
fn parse_frame(input: &mut BytesMut) -> io::Result<Option<(Vec<Bytes>, Bytes)>> {
    let Some((length, spans)) = frame_spans(input)? else {
        return Ok(None);
    };
    let raw = input.split_to(length).freeze();
    let args = spans
        .into_iter()
        .map(|(start, end)| raw.slice(start..end))
        .collect();
    Ok(Some((args, raw)))
}

/// Decodes complete published stream chunks using the native framing limits.
/// Arguments share immutable input bytes; no whole-stream copying occurs.
/// Truncated frames and incomplete/nested MULTI transactions are rejected.
pub fn decode_commands(mut bytes: Bytes) -> io::Result<Vec<Vec<Bytes>>> {
    if bytes.len() > MAX_REPLICATION_TRANSACTION_BYTES {
        return Err(invalid("replication stream exceeds decode bound"));
    }
    let mut commands = Vec::new();
    let mut in_transaction = false;
    while !bytes.is_empty() {
        if commands.len() >= 1_048_576 {
            return Err(invalid("replication command count exceeds decode bound"));
        }
        let Some((length, spans)) = frame_spans(&bytes)? else {
            return Err(invalid("truncated replication stream"));
        };
        let raw = bytes.split_to(length);
        let args: Vec<Bytes> = spans
            .into_iter()
            .map(|(start, end)| raw.slice(start..end))
            .collect();
        if args.len() == 1 && eq(&args[0], b"MULTI") {
            if in_transaction {
                return Err(invalid("nested replication transaction"));
            }
            in_transaction = true;
        } else if args.len() == 1 && eq(&args[0], b"EXEC") {
            if !in_transaction {
                return Err(invalid("replication EXEC without MULTI"));
            }
            in_transaction = false;
        }
        commands.push(args);
    }
    if in_transaction {
        return Err(invalid("incomplete replication transaction"));
    }
    Ok(commands)
}

/// Canonical effects-only stream. Normal Redis types use native commands;
/// ffkv-specific effect records have an explicit versioned replication command.
/// The caller publishes this immutable buffer once after durable commit.
pub fn encode_batch(batch: &crate::journal::Batch) -> io::Result<Bytes> {
    use crate::journal::Mutation as M;
    if !batch.mutations.iter().any(|mutation| {
        !matches!(
            mutation,
            M::NativePosition { .. }
                | M::RdbAuxiliary { .. }
                | M::SelectDatabase { .. }
                | M::ResetGlobals
                | M::ClearNativePosition
        )
    }) {
        return Ok(Bytes::new());
    }
    let mut out = BytesMut::new();
    fn emit(out: &mut BytesMut, args: &[Bytes]) -> io::Result<()> {
        command_length(args)?;
        write_command(out, args)
    }
    fn literal(b: &'static [u8]) -> Bytes {
        Bytes::from_static(b)
    }
    fn decimal(n: impl std::fmt::Display) -> Bytes {
        Bytes::from(n.to_string())
    }
    emit(&mut out, &[literal(b"MULTI")])?;
    let mut selected_database = 0;
    for mutation in &batch.mutations {
        match mutation {
            M::SelectDatabase { database } => {
                selected_database = *database;
                emit(&mut out, &[literal(b"SELECT"), decimal(database)])?;
            }
            M::Set {
                key,
                value,
                expires_at,
            } => {
                emit(&mut out, &[literal(b"SET"), key.clone(), value.clone()])?;
                if let Some(at) = expires_at {
                    emit(&mut out, &[literal(b"PEXPIREAT"), key.clone(), decimal(at)])?;
                }
            }
            M::Delete { key } => emit(&mut out, &[literal(b"DEL"), key.clone()])?,
            M::Push { key, values, right } => {
                for v in values {
                    emit(
                        &mut out,
                        &[
                            literal(if *right { b"RPUSH" } else { b"LPUSH" }),
                            key.clone(),
                            v.clone(),
                        ],
                    )?;
                }
            }
            M::Pop { key, count, right } => emit(
                &mut out,
                &[
                    literal(if *right { b"RPOP" } else { b"LPOP" }),
                    key.clone(),
                    decimal(count),
                ],
            )?,
            M::Expire { key, expires_at } => match expires_at {
                Some(at) => emit(&mut out, &[literal(b"PEXPIREAT"), key.clone(), decimal(at)])?,
                None => emit(&mut out, &[literal(b"PERSIST"), key.clone()])?,
            },
            M::SetAdd { key, members } => {
                for m in members {
                    emit(&mut out, &[literal(b"SADD"), key.clone(), m.clone()])?;
                }
            }
            M::SetRemove { key, members } => {
                for m in members {
                    emit(&mut out, &[literal(b"SREM"), key.clone(), m.clone()])?;
                }
            }
            M::HashSet { key, fields } => {
                for (field, value, deadline) in fields {
                    emit(
                        &mut out,
                        &[literal(b"HSET"), key.clone(), field.clone(), value.clone()],
                    )?;
                    if let Some(at) = deadline {
                        emit(
                            &mut out,
                            &[
                                literal(b"HPEXPIREAT"),
                                key.clone(),
                                decimal(at),
                                literal(b"FIELDS"),
                                literal(b"1"),
                                field.clone(),
                            ],
                        )?;
                    }
                }
            }
            M::HashDelete { key, fields } => {
                for field in fields {
                    emit(&mut out, &[literal(b"HDEL"), key.clone(), field.clone()])?;
                }
            }
            M::SortedSetAdd { key, entries } => {
                for (member, score) in entries {
                    if score.is_nan() {
                        return Err(invalid("NaN in resolved sorted-set effect"));
                    }
                    let text = if *score == f64::INFINITY {
                        Bytes::from_static(b"+inf")
                    } else if *score == f64::NEG_INFINITY {
                        Bytes::from_static(b"-inf")
                    } else {
                        Bytes::from(ryu::Buffer::new().format(*score).to_owned())
                    };
                    emit(
                        &mut out,
                        &[literal(b"ZADD"), key.clone(), text, member.clone()],
                    )?;
                }
            }
            M::SortedSetRemove { key, members } => {
                for member in members {
                    emit(&mut out, &[literal(b"ZREM"), key.clone(), member.clone()])?;
                }
            }
            M::Clear => emit(&mut out, &[literal(b"FLUSHDB")])?,
            M::SwapDatabases { first, second } => emit(
                &mut out,
                &[literal(b"SWAPDB"), decimal(first), decimal(second)],
            )?,
            M::Rename { from, to } => {
                emit(&mut out, &[literal(b"RENAME"), from.clone(), to.clone()])?
            }
            M::ListRemove { key, value, count } => emit(
                &mut out,
                &[literal(b"LREM"), key.clone(), decimal(count), value.clone()],
            )?,
            M::ListSet { key, index, value } => emit(
                &mut out,
                &[literal(b"LSET"), key.clone(), decimal(index), value.clone()],
            )?,
            M::ListInsert {
                key,
                value,
                pivot,
                before,
                ..
            } => emit(
                &mut out,
                &[
                    literal(b"LINSERT"),
                    key.clone(),
                    literal(if *before { b"BEFORE" } else { b"AFTER" }),
                    pivot.clone(),
                    value.clone(),
                ],
            )?,
            // Local recovery and RDB auxiliary records have no logical effect
            // to relay and must never become invented database keys.
            M::NativePosition { .. }
            | M::RdbAuxiliary { .. }
            | M::ResetGlobals
            | M::ClearNativePosition => {}
            M::Search { .. } | M::Queue { .. } | M::Typed { .. } => {
                let resolved = crate::journal::Batch {
                    timestamp_ms: batch.timestamp_ms,
                    mutations: vec![
                        M::SelectDatabase {
                            database: selected_database,
                        },
                        mutation.clone(),
                    ],
                };
                emit(
                    &mut out,
                    &[
                        literal(b"FFKV.REPLAY1"),
                        crate::wal::encode_native_batch(&resolved)?,
                    ],
                )?;
            }
            M::Functions { snapshot } => emit(
                &mut out,
                &[
                    literal(b"FUNCTION"),
                    literal(b"RESTORE"),
                    snapshot.clone(),
                    literal(b"FLUSH"),
                ],
            )?,
        }
    }
    emit(&mut out, &[literal(b"EXEC")])?;
    Ok(out.freeze())
}

impl NativeReplication {
    /// Actor adapters compare this at execution time, under their commit gate.
    pub fn generation(&self) -> u64 {
        self.state.lock().unwrap().generation
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn state() -> State {
        State {
            history: "a".repeat(40),
            secondary: None,
            offset: 0,
            fsynced: 0,
            backlog: VecDeque::new(),
            backlog_len: 0,
            replicas: BTreeMap::new(),
            next_replica: 1,
            upstream: None,
            connected: false,
            generation: 0,
            failover: None,
            aof: true,
            force_full_sync: false,
            history_epoch: 0,
        }
    }
    async fn assert_history_replacement_fences(full_install: bool) {
        let state = Arc::new(crate::state::State::new(0, 1, false));
        let directory =
            std::env::temp_dir().join(format!("ffkv-history-{:032x}", rand::random::<u128>()));
        let config = Config {
            backlog_bytes: 4096,
            max_snapshot_bytes: 1024 * 1024,
            staging_directory: directory.clone(),
            listening_port: 0,
            announce_ip: None,
            io_timeout: Duration::from_secs(2),
            reconnect_delay: Duration::from_millis(10),
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
        let native = NativeReplication::new(config, backend).unwrap();
        native.restore_position("a".repeat(40), 3, 0).unwrap();
        native
            .follow_paused(native.configured_upstream("127.0.0.1".into(), 1), None)
            .unwrap();
        native.state.lock().unwrap().replicas.insert(
            1,
            ReplicaStatus {
                id: 1,
                host: "127.0.0.1".into(),
                port: 0,
                processed: 0,
                fsynced: 0,
                online: false,
                last_ack: Instant::now(),
            },
        );
        let (mut consumer, mut transport) = tokio::io::duplex(4096);
        let stream_native = native.clone();
        let stream = tokio::spawn(async move {
            let mut next = 4;
            let epoch = stream_native.history_epoch();
            stream_native
                .downstream_loop(&mut transport, BytesMut::new(), 1, &mut next, epoch)
                .await
        });
        consumer
            .write_all(&encode(&[
                Bytes::from_static(b"REPLCONF"),
                Bytes::from_static(b"ACK"),
                Bytes::from_static(b"3"),
            ]))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while !native
                .status()
                .replicas
                .iter()
                .any(|replica| replica.online)
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if full_install {
            native
                .commit_snapshot_position("a".repeat(40), 3, 0, native.generation())
                .unwrap();
            native.promote_paused(None, None).unwrap();
        } else {
            native
                .promote_paused(
                    None,
                    Some(CommittedPosition {
                        history: Bytes::from("b".repeat(40)),
                        offset: 3,
                        fsynced_offset: 0,
                    }),
                )
                .unwrap();
        }
        native
            .publish(
                encode(&[
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"new-history"),
                    Bytes::from_static(b"tail"),
                ]),
                false,
            )
            .unwrap();
        let mut byte = [0];
        let read = tokio::time::timeout(Duration::from_secs(1), consumer.read(&mut byte))
            .await
            .unwrap()
            .unwrap();
        stream.abort();
        let _ = stream.await;
        std::fs::remove_dir_all(directory).unwrap();
        assert_eq!(
            read, 0,
            "replacement history tail reached the obsolete downstream image"
        );
    }
    #[tokio::test]
    async fn native_history_replacement_closes_established_downstream_before_new_tail() {
        assert_history_replacement_fences(false).await;
    }
    #[tokio::test]
    async fn native_history_full_install_at_identical_position_closes_old_downstream() {
        assert_history_replacement_fences(true).await;
    }
    #[test]
    fn backlog_is_byte_addressed_and_exactly_bounded() {
        let mut s = state();
        append(&mut s, Bytes::from_static(b"abcdef"), 4).unwrap();
        assert_eq!(s.offset, 6);
        assert_eq!(s.backlog_len, 4);
        assert!(backlog(&s, 2).is_none());
        assert_eq!(backlog(&s, 3).unwrap(), vec![Bytes::from_static(b"cdef")]);
        append(&mut s, Bytes::from_static(b"gh"), 4).unwrap();
        let resumed = backlog(&s, 6).unwrap().concat();
        assert_eq!(resumed, b"fgh");
        assert!(backlog(&s, 9).unwrap().is_empty());
        assert!(backlog(&s, 10).is_none());
        assert!(read_changes(&s, 4, 16).is_err()); // trimmed publish prefix
        assert!(read_changes(&s, 6, 0).is_err());
        let (through, chunks) = read_changes(&s, 6, 1).unwrap();
        assert_eq!(through, 8); // include the whole first publish, over budget
        assert_eq!(chunks, vec![Bytes::from_static(b"gh")]);
    }
    #[test]
    fn secondary_history_cannot_cross_the_promotion_boundary() {
        let mut s = state();
        append(&mut s, Bytes::from_static(b"old"), 64).unwrap();
        s.secondary = Some((s.history.clone(), 4));
        s.history = "b".repeat(40);
        append(&mut s, Bytes::from_static(b"new"), 64).unwrap();
        assert!(partial(&s, &"a".repeat(40), 4));
        assert!(!partial(&s, &"a".repeat(40), 5));
        assert!(partial(&s, &"b".repeat(40), 5));
        assert!(!partial(&s, &"c".repeat(40), 4));
    }
    #[test]
    fn fragmented_binary_frames_keep_the_following_frame() {
        let first = encode(&[
            Bytes::from_static(b"SET"),
            Bytes::from_static(b"k"),
            Bytes::from_static(b"\0\r\n\xff"),
        ]);
        let second = encode(&[Bytes::from_static(b"PING")]);
        for split in 0..first.len() {
            let mut input = BytesMut::from(&first[..split]);
            assert!(parse_frame(&mut input).unwrap().is_none());
            input.extend_from_slice(&first[split..]);
            input.extend_from_slice(&second);
            let (args, raw) = parse_frame(&mut input).unwrap().unwrap();
            assert_eq!(raw, first);
            assert_eq!(args[2].as_ref(), b"\0\r\n\xff");
            let (args, raw) = parse_frame(&mut input).unwrap().unwrap();
            assert_eq!(raw, second);
            assert_eq!(args[0].as_ref(), b"PING");
            assert!(input.is_empty());
        }
    }
    #[test]
    fn network_policy_blocks_metadata_and_mapped_loopback() {
        assert!(!permitted("169.254.169.254".parse().unwrap(), true, true));
        assert!(!permitted("::ffff:127.0.0.1".parse().unwrap(), false, true));
        assert!(!permitted("10.0.0.1".parse().unwrap(), false, false));
        assert!(permitted("127.0.0.1".parse().unwrap(), true, false));
    }
    #[test]
    fn database_swap_uses_standard_commands_and_metadata_does_not_publish() {
        use crate::journal::{Batch, Mutation};
        let metadata = Batch {
            timestamp_ms: 0,
            mutations: vec![
                Mutation::SelectDatabase { database: 7 },
                Mutation::ResetGlobals,
                Mutation::ClearNativePosition,
                Mutation::NativePosition {
                    history: Bytes::from("a".repeat(40)),
                    offset: 77,
                },
            ],
        };
        assert!(encode_batch(&metadata).unwrap().is_empty());
        let batch = Batch {
            timestamp_ms: 0,
            mutations: vec![Mutation::SwapDatabases {
                first: 2,
                second: 9,
            }],
        };
        let commands = decode_commands(encode_batch(&batch).unwrap()).unwrap();
        assert_eq!(
            commands,
            vec![
                vec![Bytes::from_static(b"MULTI")],
                vec![
                    Bytes::from_static(b"SWAPDB"),
                    Bytes::from_static(b"2"),
                    Bytes::from_static(b"9")
                ],
                vec![Bytes::from_static(b"EXEC")],
            ]
        );
    }
}

impl NativeReplication {
    pub(crate) fn configured_upstream(&self, host: String, port: u16) -> Upstream {
        let mut upstream = self
            .config
            .upstream_template
            .clone()
            .unwrap_or_else(|| Upstream::plain(host.clone(), port));
        upstream.host = host;
        upstream.port = port;
        upstream
    }
}

const MAX_REPLICATION_TRANSACTION_BYTES: usize = 256 * 1024 * 1024;
impl NativeReplication {
    /// Held across actor WAL commit and native offset publication. Backend
    /// methods are already called under this lease and must not reacquire it.
    pub async fn generation_guard(
        &self,
        generation: u64,
    ) -> io::Result<tokio::sync::OwnedMutexGuard<()>> {
        let lease = self.reconfigure.clone().lock_owned().await;
        if self.generation() != generation || *self.stopped.borrow() {
            return Err(invalid("replication generation is no longer active"));
        }
        Ok(lease)
    }
    async fn apply_frames(
        self: &Arc<Self>,
        frames: Vec<(Vec<Bytes>, Bytes)>,
        generation: u64,
    ) -> io::Result<()> {
        let this = self.clone();
        tokio::spawn(async move {
            let _lease = this.generation_guard(generation).await?;
            let (old, mut fsynced) = {
                let s = this.state.lock().unwrap();
                (s.offset, s.fsynced)
            };
            let mut end = old;
            let raw = if frames.len() == 1 {
                frames[0].1.clone()
            } else {
                let length = frames
                    .iter()
                    .try_fold(0usize, |n, (_, b)| n.checked_add(b.len()))
                    .ok_or_else(|| invalid("replication transaction length overflow"))?;
                let mut bytes = BytesMut::with_capacity(length);
                for (_, b) in &frames {
                    bytes.extend_from_slice(b);
                }
                bytes.freeze()
            };
            for (command, bytes) in frames {
                end = end
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| invalid("replication offset overflow"))?;
                let control = (command.len() == 3
                    && eq(&command[0], b"REPLCONF")
                    && eq(&command[1], b"GETACK"))
                    || (command.len() == 1 && eq(&command[0], b"PING"));
                if !control {
                    let applied = match this.backend.apply(command, end, generation).await {
                        Ok(applied) => applied,
                        Err(error) => return Err(this.require_full_sync(generation, error).await),
                    };
                    if applied.fsynced_offset > end {
                        return Err(this
                            .require_full_sync(
                                generation,
                                invalid("applied fsync offset exceeds processed offset"),
                            )
                            .await);
                    }
                    fsynced = fsynced.max(applied.fsynced_offset);
                }
            }
            let published = {
                let mut s = this.state.lock().unwrap();
                if s.generation != generation {
                    Err(io::Error::new(
                        io::ErrorKind::Interrupted,
                        "stale stream publication generation",
                    ))
                } else if s.offset != old {
                    Err(invalid("replication history changed during apply"))
                } else {
                    append(&mut s, raw, this.config.backlog_bytes).map(|_| {
                        s.fsynced = fsynced;
                    })
                }
            };
            if let Err(error) = published {
                return Err(this.require_full_sync(generation, error).await);
            }
            this.changed.notify_waiters();
            Ok(())
        })
        .await
        .map_err(|_| invalid("replication apply task failed"))?
    }
    /// Startup only: restore the durable stream position before starting the
    /// supervisor, publishing changes, or admitting clients.
    pub fn restore_position(
        &self,
        history: String,
        offset: u64,
        fsynced_offset: u64,
    ) -> io::Result<()> {
        if !valid_history(&history) || fsynced_offset > offset {
            return Err(invalid("invalid recovered replication position"));
        }
        let mut s = self.state.lock().unwrap();
        if s.offset != 0 || s.generation != 0 || s.upstream.is_some() || !s.replicas.is_empty() {
            return Err(invalid(
                "replication position restore requires an unstarted runtime",
            ));
        }
        s.history = history;
        s.offset = offset;
        s.fsynced = fsynced_offset;
        Ok(())
    }
    #[cfg(test)]
    pub fn changes_after(&self, after: u64, max_bytes: usize) -> io::Result<(u64, Vec<Bytes>)> {
        read_changes(&self.state.lock().unwrap(), after, max_bytes)
    }
    pub fn changes_after_in_history(
        &self,
        expected_history: &str,
        after: u64,
        max_bytes: usize,
    ) -> io::Result<(u64, Vec<Bytes>)> {
        let s = self.state.lock().unwrap();
        if s.history != expected_history {
            return Err(invalid("replication history changed during transfer"));
        }
        read_changes(&s, after, max_bytes)
    }
}
fn read_changes(s: &State, after: u64, max_bytes: usize) -> io::Result<(u64, Vec<Bytes>)> {
    if max_bytes == 0 {
        return Err(invalid("change-stream budget must be positive"));
    }
    if after == s.offset {
        return Ok((after, Vec::new()));
    }
    let next = after
        .checked_add(1)
        .ok_or_else(|| invalid("change-stream offset overflow"))?;
    let start = s
        .backlog
        .iter()
        .position(|chunk| chunk.first == next && chunk.published_first == next)
        .ok_or_else(|| invalid("change-stream offset was evicted or is not a publish boundary"))?;
    let mut through = after;
    let mut length = 0usize;
    let mut result = Vec::new();
    for chunk in s.backlog.iter().skip(start) {
        if !result.is_empty() && length.saturating_add(chunk.bytes.len()) > max_bytes {
            break;
        }
        result.push(chunk.bytes.clone());
        length = length.saturating_add(chunk.bytes.len());
        through = chunk.first + chunk.bytes.len() as u64 - 1;
        if length >= max_bytes {
            break;
        }
    }
    Ok((through, result))
}

impl NativeReplication {
    async fn drain(&self) {
        let _lease = self.reconfigure.clone().lock_owned().await;
    }
}

#[cfg(test)]
mod decoder_tests {
    use super::*;
    #[test]
    fn decode_complete_transaction_and_reject_truncation() {
        let mut stream = BytesMut::new();
        for args in [
            vec![Bytes::from_static(b"MULTI")],
            vec![Bytes::from_static(b"SELECT"), Bytes::from_static(b"0")],
            vec![
                Bytes::from_static(b"SET"),
                Bytes::from_static(b"k"),
                Bytes::from_static(b"\0\xff"),
            ],
            vec![Bytes::from_static(b"EXEC")],
        ] {
            stream.extend_from_slice(&encode(&args));
        }
        let stream = stream.freeze();
        let commands = decode_commands(stream.clone()).unwrap();
        assert_eq!(commands.len(), 4);
        assert_eq!(commands[2][2].as_ref(), b"\0\xff");
        assert!(decode_commands(stream.slice(..stream.len() - 1)).is_err());
        assert!(decode_commands(encode(&[Bytes::from_static(b"MULTI")])).is_err());
    }
}

impl NativeReplication {
    /// An apply error may have changed in-memory data before failing. Do not
    /// replay its prefix through partial PSYNC; replace it from a full snapshot.
    async fn require_full_sync(&self, generation: u64, error: io::Error) -> io::Error {
        let release_handoff_pause = {
            let mut s = self.state.lock().unwrap();
            if s.generation != generation {
                return error;
            }
            s.force_full_sync = true;
            s.connected = false;
            s.failover.take().is_some()
        };
        self.changed.notify_waiters();
        if let Err(state_error) = self.transition_state(generation, true, false).await {
            return io::Error::other(format!(
                "{error}; cannot publish replication failure: {state_error}"
            ));
        }
        if release_handoff_pause && let Err(pause_error) = self.backend.pause_writes(false) {
            return io::Error::other(format!(
                "{error}; cannot release failover pause: {pause_error}"
            ));
        }
        error
    }
}

impl NativeReplication {
    async fn transition_state(
        &self,
        generation: u64,
        replica: bool,
        ready: bool,
    ) -> io::Result<()> {
        match self
            .backend
            .replication_state(generation, replica, ready)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                if self.generation() != generation {
                    return Err(error);
                }
                // An unacknowledged actor mode must never be followed by
                // network replication or successful readiness publication.
                self.shutdown();
                Err(error)
            }
        }
    }
}

fn check_history_epoch(state: &State, epoch: u64) -> io::Result<()> {
    if state.history_epoch != epoch {
        return Err(invalid(
            "replication image replaced during downstream session",
        ));
    }
    Ok(())
}

fn invalidate_history(state: &mut State) -> io::Result<()> {
    state.history_epoch = state
        .history_epoch
        .checked_add(1)
        .ok_or_else(|| invalid("replication image epoch exhausted"))?;
    state.backlog.clear();
    state.backlog_len = 0;
    // ACKs belong to the old image even when the replacement has the same offset.
    state.replicas.clear();
    Ok(())
}

/// The actor may receive a role command just after durable apply/install replied,
/// before its detached transport job publishes the stream position. Adopt the
/// committed engine image while changing generation, never await that old lease.
fn reconcile_paused_position(
    state: &mut State,
    position: Option<CommittedPosition>,
) -> io::Result<()> {
    let Some(position) = position.filter(|_| state.upstream.is_some()) else {
        return Ok(());
    };
    let history = std::str::from_utf8(&position.history)
        .map_err(|_| invalid("invalid committed replication history"))?;
    if !valid_history(history) {
        return Err(invalid("invalid committed replication history"));
    }
    if state.history != history || state.offset != position.offset {
        invalidate_history(state)?;
        state.history = history.to_owned();
        state.offset = position.offset;
        state.fsynced = position.fsynced_offset.min(position.offset);
        state.secondary = None;
    } else {
        state.fsynced = state
            .fsynced
            .max(position.fsynced_offset.min(position.offset));
    }
    Ok(())
}
