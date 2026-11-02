use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use bytes::Bytes;

use crate::protocol::{Resp, Version};

struct Metadata {
    name: Option<Bytes>,
    library: Option<Bytes>,
    library_version: Option<Bytes>,
    version: Version,
    user: Option<Bytes>,
}

struct Client {
    peer: SocketAddr,
    local: SocketAddr,
    connected: Instant,
    active_ms: AtomicU64,
    metadata: Mutex<Metadata>,
}

pub struct State {
    started: Instant,
    clients: Mutex<BTreeMap<u64, Arc<Client>>>,
    next_id: AtomicU64,
    processed: AtomicU64,
    port: u16,
    shards: u16,
    persistent: bool,
    pub metrics: Arc<crate::metrics::Metrics>,
    pub pubsub: Arc<crate::pubsub::Hub>,
    replica: AtomicBool,
    replica_ready: AtomicBool,
    instance_id: Bytes,
    pub client_registry: Arc<crate::client_registry::Registry>,
    pub native: OnceLock<Arc<crate::native_replication::NativeReplication>>,
    pub cluster: OnceLock<Arc<crate::cluster::ClusterRuntime>>,
    pub server: OnceLock<Arc<crate::server_commands::ServerRuntime>>,
    pub scripts: OnceLock<Arc<crate::scripting::ExecutionControl>>,
    pub admin_runtime: OnceLock<Arc<crate::server_runtime::RuntimeState>>,
    pub write_gate: Arc<crate::write_gate::WriteGate>,
    pub control_tx: OnceLock<tokio::sync::mpsc::Sender<crate::request::Request>>,
    pub persistence: OnceLock<Arc<crate::persistence::Persistence>>,
    actor_ready: AtomicBool,
    actor_ready_notify: tokio::sync::Notify,
}

pub struct ClientLease {
    pub id: u64,
    client: Arc<Client>,
    state: Arc<State>,
}

impl ClientLease {
    pub fn seen(&self) {
        self.state.processed.fetch_add(1, Ordering::Relaxed);
        self.client
            .active_ms
            .store(self.state.elapsed_ms(), Ordering::Relaxed);
    }

    pub fn name(&self) -> Option<Bytes> {
        self.client
            .metadata
            .lock()
            .expect("client metadata")
            .name
            .clone()
    }

    pub fn set_name(&self, name: Option<Bytes>) {
        self.client.metadata.lock().expect("client metadata").name = name;
    }

    pub fn set_user(&self, user: Option<Bytes>) {
        self.client.metadata.lock().expect("client metadata").user = user;
    }

    pub fn set_version(&self, version: Version) {
        self.client
            .metadata
            .lock()
            .expect("client metadata")
            .version = version;
    }

    pub fn set_library(&self, name: bool, value: Bytes) {
        let mut metadata = self.client.metadata.lock().expect("client metadata");
        if name {
            metadata.library = Some(value);
        } else {
            metadata.library_version = Some(value);
        }
    }

    pub fn info(&self) -> Bytes {
        self.state.client_info(Some(self.id))
    }
}

impl Drop for ClientLease {
    fn drop(&mut self) {
        self.state
            .clients
            .lock()
            .expect("client registry")
            .remove(&self.id);
        self.state.metrics.connection_close();
    }
}

impl State {
    pub fn new(port: u16, shards: u16, persistent: bool) -> Self {
        let pubsub = Arc::new(crate::pubsub::Hub::default());
        let client_registry = Arc::new(crate::client_registry::Registry::new(pubsub.clone()));
        Self {
            started: Instant::now(),
            clients: Mutex::new(BTreeMap::new()),
            next_id: AtomicU64::new(1),
            processed: AtomicU64::new(0),
            port,
            shards,
            persistent,
            metrics: Arc::new(crate::metrics::Metrics::new()),
            pubsub,
            client_registry,
            native: OnceLock::new(),
            cluster: OnceLock::new(),
            server: OnceLock::new(),
            scripts: OnceLock::new(),
            admin_runtime: OnceLock::new(),
            write_gate: Arc::new(crate::write_gate::WriteGate::default()),
            control_tx: OnceLock::new(),
            persistence: OnceLock::new(),
            actor_ready: AtomicBool::new(false),
            actor_ready_notify: tokio::sync::Notify::new(),
            replica: AtomicBool::new(false),
            replica_ready: AtomicBool::new(true),
            instance_id: Bytes::from(format!("{:032x}", rand::random::<u128>())),
        }
    }

    pub fn mark_actor_ready(&self) {
        self.actor_ready.store(true, Ordering::Release);
        self.actor_ready_notify.notify_waiters();
    }

    pub async fn wait_actor_ready(&self) {
        loop {
            let notified = self.actor_ready_notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.actor_ready.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }

    pub fn admission(&self) -> crate::pressure::Admission<'_> {
        crate::pressure::Admission {
            read_only: self.is_replica(),
            metrics: &self.metrics,
        }
    }

    pub fn role(&self) -> &'static str {
        if self.is_replica() { "slave" } else { "master" }
    }
    pub fn is_replica(&self) -> bool {
        self.replica.load(Ordering::Acquire)
    }
    pub fn replica_ready(&self) -> bool {
        self.replica_ready.load(Ordering::Acquire)
    }
    pub fn set_replica(&self, replica: bool) {
        self.replica.store(replica, Ordering::Release);
        self.replica_ready.store(!replica, Ordering::Release);
        self.metrics.set_replication(
            self.role(),
            if replica { "loading" } else { "standalone" },
            None,
        );
    }
    pub fn set_replica_ready(&self, ready: bool) {
        self.replica_ready.store(ready, Ordering::Release);
    }
    pub fn instance_id(&self) -> Bytes {
        self.instance_id.clone()
    }
    pub fn shards(&self) -> u16 {
        self.shards
    }
    pub fn set_blocked(&self, count: usize) {
        self.metrics.blocked_clients(count);
    }

    pub fn reset_statistics(&self) {
        self.processed.store(0, Ordering::Relaxed);
        self.metrics.reset_statistics();
    }

    fn elapsed_ms(&self) -> u64 {
        self.started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
    }

    pub fn register(self: &Arc<Self>, peer: SocketAddr, local: SocketAddr) -> ClientLease {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let client = Arc::new(Client {
            peer,
            local,
            connected: Instant::now(),
            active_ms: AtomicU64::new(self.elapsed_ms()),
            metadata: Mutex::new(Metadata {
                name: None,
                library: None,
                library_version: None,
                version: Version::Resp2,
                user: None,
            }),
        });
        self.clients
            .lock()
            .expect("client registry")
            .insert(id, client.clone());
        self.metrics.connection_open();
        ClientLease {
            id,
            client,
            state: self.clone(),
        }
    }

    pub fn client_info(&self, id: Option<u64>) -> Bytes {
        let clients = self.clients.lock().expect("client registry");
        let mut output = String::new();
        let now = self.elapsed_ms();
        use std::fmt::Write;
        for (&client_id, client) in clients
            .iter()
            .filter(|(client_id, _)| id.is_none_or(|id| id == **client_id))
        {
            let metadata = client.metadata.lock().expect("client metadata");
            let name = metadata
                .name
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            let library = metadata
                .library
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            let library_version = metadata
                .library_version
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_default();
            let resp = if metadata.version == Version::Resp3 {
                3
            } else {
                2
            };
            let idle = now.saturating_sub(client.active_ms.load(Ordering::Relaxed)) / 1000;
            let user = metadata
                .user
                .as_deref()
                .map(String::from_utf8_lossy)
                .unwrap_or_else(|| "unauthenticated".into());
            writeln!(&mut output, "id={client_id} addr={} laddr={} name={name} user={user} age={} idle={idle} db=0 resp={resp} lib-name={library} lib-ver={library_version}", client.peer, client.local, client.connected.elapsed().as_secs()).expect("string formatting");
        }
        Bytes::from(output)
    }

    pub fn info(&self, sections: &[Bytes]) -> Resp {
        use std::fmt::Write;
        let wanted = |name: &[u8]| {
            sections.is_empty()
                || sections.iter().any(|section| {
                    section.eq_ignore_ascii_case(name)
                        || section.eq_ignore_ascii_case(b"all")
                        || section.eq_ignore_ascii_case(b"default")
                })
        };
        let mut output = String::new();
        if wanted(b"server") {
            write!(&mut output, "# Server\r\nserver_name:ffkv\r\nffkv_version:{}\r\nredis_version:8.10.0\r\nffkv_compatibility_baseline:redis-8.10\r\nredis_mode:{}\r\nprocess_id:{}\r\ntcp_port:{}\r\nuptime_in_seconds:{}\r\nuptime_in_days:{}\r\nffkv_shards:{}\r\n", env!("CARGO_PKG_VERSION"), if self.cluster.get().is_some() { "cluster" } else { "standalone" }, std::process::id(), self.port, self.started.elapsed().as_secs(), self.started.elapsed().as_secs() / 86400, self.shards).expect("string formatting");
        }
        if wanted(b"clients") {
            write!(
                &mut output,
                "# Clients\r\nconnected_clients:{}\r\nblocked_clients:{}\r\n",
                self.clients.lock().expect("client registry").len(),
                self.client_registry.blocked_count()
            )
            .expect("string formatting");
        }
        if wanted(b"memory") {
            output.push_str("# Memory\r\nmaxmemory:0\r\nmaxmemory_policy:noeviction\r\n");
            let pressure = self.metrics.rss_pressure();
            write!(
                &mut output,
                "ffkv_max_rss_bytes_soft:{}\r\nffkv_rss_pressured:{}\r\nffkv_rss_sample_interval_ms:1000\r\n",
                pressure.max_rss_bytes_soft,
                u8::from(pressure.rss_pressured),
            )
            .expect("string formatting");
            if let Some(rss) = pressure.rss_bytes {
                write!(&mut output, "ffkv_process_rss_bytes:{rss}\r\n").expect("string formatting");
            }
        }
        if wanted(b"stats") {
            write!(
                &mut output,
                "# Stats\r\ntotal_commands_processed:{}\r\n",
                self.processed.load(Ordering::Relaxed)
            )
            .expect("string formatting");
        }
        if wanted(b"persistence") {
            write!(
                &mut output,
                "# Persistence\r\nloading:{}\r\nffkv_wal_enabled:{}\r\nffkv_wal_format:1\r\n",
                u8::from(self.is_replica() && !self.replica_ready()),
                u8::from(self.persistent)
            )
            .expect("string formatting");
        }
        if wanted(b"replication") {
            write!(
                &mut output,
                "# Replication\r\nrole:{}\r\nconnected_slaves:0\r\n",
                self.role()
            )
            .expect("string formatting");
        }
        Resp::Bulk(Some(Bytes::from(output)))
    }
}
