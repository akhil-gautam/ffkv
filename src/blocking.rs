//! Blocking list and stream readers live in the command actor, never in a storage lock.
//! Each key indexes consumers in registration order; completion removes every index.

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    future, io,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::{engine::ReadyKey, session::ClientSession};
use crate::{
    engine::{BlockingKind, BlockingPop, BlockingStream, BoundCommand, Engine, Execution, Routing},
    executor::Atomic,
    persistence::Persistence,
    protocol::Resp,
    request::{CommandRequest, Request},
    scripting::ScriptRunner,
    sharding::Pool,
    state::State,
    wal::Wal,
};

struct Waiter {
    keys: Vec<Bytes>,
    right: bool,
    destination: Option<(Bytes, bool)>,
    kind: BlockingKind,
    deadline: Option<Instant>,
    reply: oneshot::Sender<Resp>,
    stream: Option<BlockingStream>,
    retry: Option<Instant>,
    user: Option<Arc<crate::auth::User>>,
    origin: crate::request::Origin,
    weight: usize,
    session: Option<Arc<ClientSession>>,
    database: u32,
}

impl Waiter {
    fn null_reply(&self) -> Resp {
        if self.destination.is_some() {
            Resp::Bulk(None)
        } else {
            Resp::Array(None)
        }
    }
}

#[derive(Default)]
struct Waiters {
    next_id: u64,
    clients: HashMap<u64, Waiter>,
    keys: HashMap<ReadyKey, VecDeque<u64>>,
    deadlines: BTreeSet<(Instant, u64)>,
    retries: BTreeSet<(Instant, u64)>,
    retained_bytes: usize,
    registry: Option<Arc<crate::client_registry::Registry>>,
    replica: bool,
}

impl Waiters {
    fn register(
        &mut self,
        mut pop: BlockingPop,
        reply: oneshot::Sender<Resp>,
        now: Instant,
        session: Option<Arc<ClientSession>>,
    ) {
        if reply.is_closed() {
            return;
        }
        if self.clients.len() >= 65_536 {
            let _ = reply.send(Resp::Error("ERR too many blocked clients".into()));
            return;
        }
        let weight = pop
            .keys
            .iter()
            .fold(std::mem::size_of::<Waiter>(), |n, key| {
                n.saturating_add(key.len()).saturating_add(64)
            });
        if weight.saturating_add(self.retained_bytes) > 64 * 1024 * 1024 {
            let _ = reply.send(Resp::Error(
                "ERR blocked client memory limit reached".into(),
            ));
            return;
        }
        let deadline = match pop.timeout {
            Some(timeout) => match now.checked_add(timeout) {
                Some(deadline) => Some(deadline),
                None => {
                    let _ = reply.send(Resp::Error("ERR timeout is out of range".into()));
                    return;
                }
            },
            None => None,
        };
        let id = self.next_id;
        let Some(next_id) = id.checked_add(1) else {
            let _ = reply.send(Resp::Error(
                "ERR blocking request identifiers exhausted".into(),
            ));
            return;
        };
        self.next_id = next_id;
        // Redis re-executes the original ordered key scan on wakeup. Deduplicate
        // index membership without changing which key/error takes precedence.
        let mut seen = HashSet::with_capacity(pop.keys.len());
        pop.keys.retain(|key| seen.insert(key.clone()));
        let database = session.as_ref().map_or(0, |session| session.database());
        if let (Some(registry), Some(session)) = (&self.registry, &session) {
            registry.set_blocked(session.client_id, true);
        }
        for key in &pop.keys {
            self.keys
                .entry((database, key.clone()))
                .or_default()
                .push_back(id);
        }
        if let Some(deadline) = deadline {
            self.deadlines.insert((deadline, id));
        }
        self.clients.insert(
            id,
            Waiter {
                keys: pop.keys,
                right: pop.right,
                destination: pop.destination,
                kind: pop.kind,
                deadline,
                reply,
                stream: None,
                retry: None,
                user: None,
                origin: crate::request::Origin::Client(crate::protocol::Version::Resp2),
                weight,
                database,
                session,
            },
        );
        self.retained_bytes += weight;
    }

    fn register_stream(
        &mut self,
        stream: BlockingStream,
        reply: oneshot::Sender<Resp>,
        now: Instant,
        user: Option<Arc<crate::auth::User>>,
        origin: crate::request::Origin,
        session: Option<Arc<ClientSession>>,
    ) {
        let extra = stream.args.iter().fold(0usize, |n, arg| {
            n.saturating_add(arg.len()).saturating_add(32)
        });
        let key_weight = stream
            .keys
            .iter()
            .fold(std::mem::size_of::<Waiter>(), |n, key| {
                n.saturating_add(key.len()).saturating_add(64)
            });
        if self
            .retained_bytes
            .saturating_add(extra)
            .saturating_add(key_weight)
            > 64 * 1024 * 1024
        {
            let _ = reply.send(Resp::Error(
                "ERR blocked client memory limit reached".into(),
            ));
            return;
        }
        let id = self.next_id;
        let retry = stream.retry_after.and_then(|delay| now.checked_add(delay));
        self.register(
            BlockingPop {
                keys: stream.keys.clone(),
                right: false,
                timeout: stream.timeout,
                destination: None,
                kind: BlockingKind::ListPop,
            },
            reply,
            now,
            session,
        );
        if let Some(waiter) = self.clients.get_mut(&id) {
            waiter.stream = Some(stream);
            waiter.user = user;
            waiter.origin = origin;
            waiter.weight += extra;
            self.retained_bytes += extra;
            waiter.retry = retry;
            if let Some(at) = retry {
                self.retries.insert((at, id));
            }
        }
    }

    fn reschedule(&mut self, id: u64, stream: BlockingStream) {
        let waiter = self.clients.get_mut(&id).expect("registered stream read");
        if let Some(old) = waiter.retry {
            self.retries.remove(&(old, id));
        }
        waiter.retry = stream
            .retry_after
            .and_then(|delay| Instant::now().checked_add(delay));
        if let Some(at) = waiter.retry {
            self.retries.insert((at, id));
        }
        waiter.stream = Some(stream);
    }

    fn next_deadline(&self) -> Option<Instant> {
        self.deadlines
            .first()
            .map(|(at, _)| *at)
            .into_iter()
            .chain(self.retries.first().map(|(at, _)| *at))
            .min()
    }
    fn remove(&mut self, id: u64) -> Waiter {
        let waiter = self
            .clients
            .remove(&id)
            .expect("registered blocking client");
        self.retained_bytes -= waiter.weight;
        if let (Some(registry), Some(session)) = (&self.registry, &waiter.session) {
            registry.set_blocked(session.client_id, false);
        }
        if let Some(deadline) = waiter.deadline {
            self.deadlines.remove(&(deadline, id));
        }
        if let Some(retry) = waiter.retry {
            self.retries.remove(&(retry, id));
        }
        for key in &waiter.keys {
            let index = (waiter.database, key.clone());
            let queue = self.keys.get_mut(&index).expect("registered key index");
            if queue.front() == Some(&id) {
                queue.pop_front();
            } else {
                queue.retain(|candidate| *candidate != id);
            }
            if queue.is_empty() {
                self.keys.remove(&index);
            }
        }
        waiter
    }

    fn unblock(&mut self, client_id: u64, error: bool) -> Resp {
        let id = self.clients.iter().find_map(|(id, waiter)| {
            waiter
                .session
                .as_ref()
                .is_some_and(|session| session.client_id == client_id)
                .then_some(*id)
        });
        let Some(id) = id else {
            return Resp::Integer(0);
        };
        let waiter = self.remove(id);
        let response = if error {
            Resp::Error("UNBLOCKED client unblocked via CLIENT UNBLOCK".into())
        } else {
            waiter.null_reply()
        };
        let _ = waiter.reply.send(response);
        Resp::Integer(1)
    }

    fn expire(&mut self, now: Instant) {
        while let Some(&(deadline, id)) = self.deadlines.first() {
            if deadline > now {
                break;
            }
            let waiter = self.remove(id);
            let reply = waiter.null_reply();
            let _ = waiter.reply.send(reply);
        }
    }

    fn reap_closed(&mut self) {
        // At most one waiter per connection. Reclaim abandoned indexes even for
        // infinite waits on keys that are never pushed again.
        let closed: Vec<_> = self
            .clients
            .iter()
            .filter_map(|(&id, waiter)| waiter.reply.is_closed().then_some(id))
            .collect();
        for id in closed {
            self.remove(id);
        }
    }

    fn wake_keys(&self, mut keys: Vec<ReadyKey>) -> Vec<ReadyKey> {
        let mut seen: HashSet<ReadyKey> = keys.iter().cloned().collect();
        let mut index = 0;
        while index < keys.len() {
            if let Some(queue) = self.keys.get(&keys[index]) {
                for id in queue {
                    let waiter = self.clients.get(id).expect("indexed blocking client");
                    for key in waiter
                        .keys
                        .iter()
                        .chain(waiter.destination.iter().map(|(key, _)| key))
                    {
                        let key = (waiter.database, key.clone());
                        if seen.insert(key.clone()) {
                            keys.push(key);
                        }
                    }
                }
            }
            index += 1;
        }
        keys.sort_unstable();
        keys.dedup();
        keys
    }

    async fn wake(
        &mut self,
        engine: &mut Engine,
        key: &ReadyKey,
        persistence: &Persistence,
    ) -> io::Result<()> {
        let mut ready = VecDeque::from([key.clone()]);
        while let Some(key) = ready.pop_front() {
            // A stream read may stay blocked while later readers are ready.
            // Scan registration order without making list readiness gate streams.
            let ids: Vec<_> = self
                .keys
                .get(&key)
                .into_iter()
                .flat_map(|queue| queue.iter().copied())
                .collect();
            for id in ids {
                let Some(waiter) = self.clients.get(&id) else {
                    continue;
                };
                if waiter.reply.is_closed() {
                    self.remove(id);
                    continue;
                }
                if self.replica
                    && waiter
                        .stream
                        .as_ref()
                        .is_none_or(|stream| BoundCommand::bind(&stream.args).is_dataset_write())
                {
                    continue;
                }
                let timeout = waiter.deadline.is_some_and(|at| at <= Instant::now());
                engine.set_reply_limit(waiter.origin.reply_limit());
                engine.set_command_version(waiter.origin.version());
                engine.set_command_user(waiter.user.clone());
                engine.set_command_session(waiter.session.clone());
                engine.switch_database(waiter.database);
                engine.set_ready_tracking(true);
                if let Some(stream) = &waiter.stream {
                    let execution = engine.resume_blocked_read(stream, timeout)?;
                    persistence.commit(engine).await?;
                    let _ = engine.take_ready_keys();
                    match execution {
                        Execution::Reply(reply) => {
                            let waiter = self.remove(id);
                            let _ = waiter.reply.send(reply);
                        }
                        Execution::StreamBlock(stream) => self.reschedule(id, stream),
                        _ => return Err(io::Error::other("invalid blocked read execution")),
                    }
                    continue;
                }
                if timeout {
                    let waiter = self.remove(id);
                    let reply = waiter.null_reply();
                    let _ = waiter.reply.send(reply);
                    continue;
                }
                // Mutation notifications also include non-list writes. Only
                // consumable list data may trigger the original ordered scan,
                // and an earlier consumer may already have drained this key.
                if !matches!(waiter.kind, BlockingKind::SortedPop { .. })
                    && !engine.list_ready(&key.1)
                {
                    // Checking readiness can expire a key; commit that effect
                    // before another retry starts a journal batch.
                    persistence.commit(engine).await?;
                    let _ = engine.take_ready_keys();
                    continue;
                }
                let result = engine.retry_blocked(
                    &waiter.keys,
                    waiter.destination.as_ref(),
                    waiter.right,
                    waiter.kind,
                )?;
                let (result, pushed) = match result {
                    Ok(Some(reply)) => (
                        Some(reply),
                        waiter
                            .destination
                            .as_ref()
                            .map(|(key, _)| (waiter.database, key.clone())),
                    ),
                    Ok(None) => (None, None),
                    Err(reply) => (Some(reply), None),
                };
                persistence.commit(engine).await?;
                let _ = engine.take_ready_keys();
                if let Some(reply) = result {
                    let waiter = self.remove(id);
                    let _ = waiter.reply.send(reply);
                    if let Some(destination) = pushed {
                        ready.push_back(destination);
                    }
                }
            }
        }
        engine.set_command_user(None);
        engine.set_command_session(None);
        engine.set_command_version(crate::protocol::Version::Resp2);
        engine.set_ready_tracking(false);
        Ok(())
    }

    fn defer_retries(&mut self, now: Instant) {
        let next = now + Duration::from_millis(100);
        while let Some(&(at, id)) = self.retries.first() {
            if at > now {
                break;
            }
            self.retries.remove(&(at, id));
            if let Some(waiter) = self.clients.get_mut(&id) {
                waiter.retry = Some(next);
                self.retries.insert((next, id));
            }
        }
    }

    async fn wake_due_admitted(
        &mut self,
        pool: &Pool,
        persistence: &Persistence,
        state: &State,
    ) -> io::Result<()> {
        self.replica = state.is_replica();
        if let Some(_permit) = state.write_gate.try_admit() {
            self.wake_due(pool, persistence).await?;
        }
        self.defer_retries(Instant::now());
        Ok(())
    }

    async fn wake_all(&mut self, pool: &Pool, persistence: &Persistence) -> io::Result<()> {
        let ready: Vec<_> = self.keys.keys().cloned().collect();
        if ready.is_empty() {
            return Ok(());
        }
        let keys = self.wake_keys(ready);
        let mut paused = pool.pause().await?;
        let mut engine = paused.take_database_keys(&keys, persistence.capturing());
        for key in keys {
            self.wake(&mut engine, &key, persistence).await?;
        }
        paused.restore(engine);
        paused.resume()
    }

    async fn wake_due(&mut self, pool: &Pool, persistence: &Persistence) -> io::Result<()> {
        let now = Instant::now();
        let ids: BTreeSet<_> = self
            .deadlines
            .range(..=(now, u64::MAX))
            .chain(self.retries.range(..=(now, u64::MAX)))
            .filter_map(|(_, id)| {
                self.clients
                    .get(id)
                    .and_then(|w| w.stream.as_ref())
                    .map(|_| *id)
            })
            .collect();
        if ids.is_empty() {
            return Ok(());
        }
        let keys = self.wake_keys(
            ids.iter()
                .flat_map(|id| {
                    let waiter = &self.clients[id];
                    waiter
                        .keys
                        .iter()
                        .cloned()
                        .map(move |key| (waiter.database, key))
                })
                .collect(),
        );
        let mut paused = pool.pause().await?;
        let mut engine = paused.take_database_keys(&keys, persistence.capturing());
        let mut done = HashSet::new();
        for id in ids {
            if let Some(key) = self
                .clients
                .get(&id)
                .and_then(|w| w.keys.first().map(|key| (w.database, key.clone())))
                && done.insert(key.clone())
            {
                self.wake(&mut engine, &key, persistence).await?;
            }
        }
        paused.restore(engine);
        paused.resume()
    }

    async fn execute(
        &mut self,
        engine: &mut Engine,
        request: CommandRequest,
        persistence: &Persistence,
        cpu_observed: bool,
    ) -> io::Result<()> {
        // No reply or subsequent read can expose mutations until WAL commit.
        // A commit failure is fatal: the owner stops processing and closes clients.
        engine.set_reply_limit(request.origin.reply_limit());
        engine.set_command_version(request.origin.version());
        engine.set_command_user(request.user.clone());
        engine.switch_database(
            request
                .session
                .as_ref()
                .map_or(0, |session| session.database()),
        );
        engine.set_command_session(request.session.clone());
        engine.set_recording(persistence.recording_for(&request.args));
        let database = engine.database();
        engine.set_ready_tracking(!self.clients.is_empty());
        let cpu = ClientSession::cpu_window(request.session.as_deref(), cpu_observed);
        let mut command = BoundCommand::bind(&request.args);
        let execution = engine.execute(&mut command)?;
        if let Some(session) = &request.session {
            session.charge_cpu(cpu);
        }
        persistence.note_reads(&command, request.session.as_deref());
        persistence.commit(engine).await?;
        let ready = engine.take_ready_keys();
        match execution {
            Execution::Reply(reply) => {
                let _ = request.reply.send(reply);
            }
            Execution::Pushed { reply, key } => {
                let _ = request.reply.send(reply);
                self.wake(engine, &(database, key), persistence).await?;
            }
            Execution::Block(pop) => {
                let id = self.next_id;
                self.register(pop, request.reply, Instant::now(), request.session.clone());
                if let Some(waiter) = self.clients.get_mut(&id) {
                    waiter.origin = request.origin;
                    waiter.user = request.user;
                }
            }
            Execution::StreamBlock(stream) => self.register_stream(
                stream,
                request.reply,
                Instant::now(),
                request.user,
                request.origin,
                request.session,
            ),
        }
        for key in ready {
            self.wake(engine, &key, persistence).await?;
        }
        engine.set_command_user(None);
        engine.set_command_session(None);
        engine.set_command_version(crate::protocol::Version::Resp2);
        engine.set_ready_tracking(false);
        Ok(())
    }
}

async fn wait_until(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
        None => future::pending().await,
    }
}

pub struct Config {
    pub checkpoint_threshold: u64,
    pub aof_enabled: bool,
    pub durability: crate::wal::Durability,
    pub fsync_gate: Option<std::path::PathBuf>,
    pub databases: Arc<std::sync::atomic::AtomicU32>,
}

pub async fn run(
    mut requests: mpsc::Receiver<Request>,
    mut engine: Engine,
    wal: Option<Wal>,
    shard_count: usize,
    state: Arc<State>,
    config: Config,
) -> io::Result<()> {
    let has_wal = wal.is_some();
    let persistence = Arc::new(Persistence::new(wal));
    engine.set_database_limit(config.databases);
    if let Some(server) = state.server.get() {
        engine.set_extensions(server.shared_extensions());
    }
    if has_wal {
        persistence
            .configure(
                Some(config.aof_enabled),
                Some(config.durability),
                engine.snapshot_batches(Engine::now()),
            )
            .await?;
    }
    if let Some(native) = state.native.get() {
        if let Some((history, offset)) = engine.native_position() {
            let history = std::str::from_utf8(&history)
                .map_err(|_| io::Error::other("invalid recovered replication history"))?;
            native.restore_position(history.to_owned(), offset, offset)?;
        }
        persistence.set_native(native.clone());
        native.set_aof_enabled(config.aof_enabled && has_wal);
    }
    if let Some(runtime) = state.admin_runtime.get() {
        persistence.set_backup(runtime.backup.clone());
    }
    persistence.set_clients(state.client_registry.clone());
    state
        .persistence
        .set(persistence.clone())
        .map_err(|_| io::Error::other("persistence already initialized"))?;
    let mut scripts = ScriptRunner::new()?;
    let functions = engine.function_snapshot();
    if !functions.is_empty() {
        scripts
            .restore_function_snapshot(&functions)
            .map_err(io::Error::other)?;
    }
    state
        .scripts
        .set(scripts.execution_control())
        .map_err(|_| io::Error::other("scripts already initialized"))?;
    let mut replay = crate::native_runtime::ReplayState::new(config.fsync_gate);
    let storage = engine.storage().cloned();
    let status = persistence.status().await;
    state.metrics.set_server(
        shard_count as u16,
        if storage.is_some() {
            "hybrid"
        } else {
            "memory"
        },
        status.policy,
    );
    persistence.publish(&state.metrics).await;
    state.metrics.sample_process();
    let compaction = Arc::new(AtomicBool::new(false));
    let mut last_compaction = Instant::now();
    let (pool, mut workers) = Pool::start(engine, shard_count, persistence.clone(), state.clone());
    // Leaving this function without `close` (error or panic) stops the pool
    // even while connections still hold it.
    let _lifetime = pool.lifetime();
    if let Some(runtime) = state.admin_runtime.get() {
        runtime.attach_pool(&pool);
    }
    state
        .pool
        .set(Arc::downgrade(&pool))
        .map_err(|_| io::Error::other("shard pool already attached"))?;
    let mut pending = JoinSet::new();
    let mut waiters = Waiters {
        registry: Some(state.client_registry.clone()),
        ..Waiters::default()
    };
    let mut native_wake_pending = false;
    let mut slot_memory = state.cluster.get().map(|_| vec![0_u64; 16_384]);
    state.mark_actor_ready();
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut sync = tokio::time::interval(Duration::from_secs(1));
    sync.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let deadline = waiters.next_deadline();
        tokio::select! {
            request = requests.recv() => match request {
                Some(request) => {
                    waiters.wake_due_admitted(&pool, &persistence, &state).await?;
                    waiters.expire(Instant::now());
                    match request {
                    Request::Command(request) => {
                        let mut command = BoundCommand::bind(&request.args);
                        let routing = command.routing(shard_count);
                        let scripting = matches!(routing, Routing::Scripting);
                        if !scripting && state.is_replica() && command.is_dataset_write() {
                            let _ = request.reply.send(Resp::Error("READONLY this development replica accepts no dataset writes".into()));
                        } else if !scripting && state.is_replica() && !state.replica_ready() && !command.is_control() {
                            let _ = request.reply.send(Resp::Error("LOADING replica initial synchronization is not complete".into()));
                        } else if let Some(reply) = (!scripting).then(|| state.admission().reject_pressure(&command)).flatten() {
                            let _ = request.reply.send(reply);
                        } else {
                            match routing {
                        _ if request.args.first().is_some_and(|name| {
                            name.eq_ignore_ascii_case(b"PUBLISH") || name.eq_ignore_ascii_case(b"SPUBLISH") || name.eq_ignore_ascii_case(b"PUBSUB")
                        }) => {
                            let allowed = command.check().and_then(|()| {
                                request.user.as_deref().map_or(Ok(()), |user| {
                                    command.authorize(user, None)
                                })
                            });
                            let reply = match allowed {
                                Err(reply) => reply,
                                Ok(()) if request.args[0].eq_ignore_ascii_case(b"PUBLISH") || request.args[0].eq_ignore_ascii_case(b"SPUBLISH") => {
                                    match if request.args[0].eq_ignore_ascii_case(b"SPUBLISH") {
                                        state.pubsub.stage_shard(&request.args[1], &request.args[2])
                                    } else { state.pubsub.stage(&request.args[1], &request.args[2]) } {
                                        Ok(publication) => {
                                            if state.cluster.get().is_none()
                                                && let Some(offset) = persistence.publish_ephemeral(&request.args).await?
                                                    && let Some(session) = &request.session { session.last_write_offset.store(offset, Ordering::Release); }
                                            let count=publication.count();
                                            publication.deliver();
                                            Resp::Integer(count)
                                        },
                                        Err(reason) => Resp::Error(reason.into()),
                                    }
                                }
                                Ok(()) => state.pubsub.inspect(&request.args),
                            };
                            let _ = request.reply.send(reply);
                        }
                        _ if request.args.first().is_some_and(|name| name.eq_ignore_ascii_case(b"INFO")) => {
                            let _ = request.reply.send(state.info(&request.args[1..]));
                        }
                        Routing::Maintenance if request.args[0].eq_ignore_ascii_case(b"FFKV.COMPACT") => {
                            if let Some(store) = &storage {
                                start_compaction(&mut pending, compaction.clone(), store.clone(), persistence.clone(), Some(request.reply));
                            } else { let _ = request.reply.send(Resp::Error("ERR compaction requires --storage hybrid".into())); }
                        }
                        Routing::Local {shard, ready} => {
                            let keys = command.accessed_keys();
                            let database = request.session.as_ref().map_or(0, |session| session.database());
                            if state.cluster.get().is_some() || keys.iter().any(|key|waiters.keys.contains_key(&(database, key.clone()))) {
                                coordinated(&mut waiters, &pool, &persistence, keys.into_vec(), ready, request, Some(&state)).await?;
                            } else {
                                // Admission happens before the next registration can
                                // acquire the write barrier: no lost push/wait race.
                                let admission = pool.admit().await?;
                                if let Err(stopped) = pool.submit(shard, request, admission, &mut pending) {
                                    // A closed queue means that actor returned or unwound;
                                    // report its cause rather than the symptom.
                                    return Err(match workers.join_next().await {
                                        Some(Ok(Err(cause))) => cause,
                                        Some(Err(join)) => io::Error::other(join),
                                        _ => stopped,
                                    });
                                }
                            }
                        }
                        Routing::Coordinated {keys, ready} => {
                            coordinated(&mut waiters, &pool, &persistence, keys, ready, request, Some(&state)).await?;
                        }
                        Routing::All => {
                            atomic(&mut waiters, &pool, &persistence, &mut scripts, &state, Request::Command(request)).await?;
                        }
                        Routing::Global(_) => {
                            atomic(&mut waiters, &pool, &persistence, &mut scripts, &state, Request::Command(request)).await?;
                        }
                        Routing::Maintenance if request.args[0].eq_ignore_ascii_case(b"CLUSTER") || request.args[0].eq_ignore_ascii_case(b"MIGRATE") => {
                            atomic(&mut waiters,&pool,&persistence,&mut scripts,&state,Request::Command(request)).await?;
                        }
                        Routing::Maintenance => {
                            let mut paused = pool.pause().await?;
                            let reply = crate::admin::execute(&mut paused, &persistence, &state, &request.args).await?;
                            paused.resume()?;
                            let _ = request.reply.send(reply);
                        }
                        Routing::Scripting => {
                            atomic(&mut waiters, &pool, &persistence, &mut scripts, &state, Request::Command(request)).await?;
                        }
                    }
                        }
                    }
                    Request::Watch {mut keys, reply, session} => {
                        let mut paused = pool.pause().await?;
                        let database = session.as_ref().map_or(0, |session| session.database());
                        paused.select_database(database);
                        let now = Engine::now();
                        keys.sort_unstable(); keys.dedup();
                        let tokens = keys.into_iter().map(|key| {
                            let shard = Engine::shard_for(&key, shard_count);
                            let deadline = paused.engines[shard].watch_deadline(&key, now);
                            persistence.watches.register(database, key, deadline)
                        }).collect();
                        paused.resume()?;
                        let _ = reply.send(Ok(tokens));
                    }
                    Request::Transaction {commands,modules,watched,user,origin,session,write_permit,cluster_client,reply} => {
                        atomic(&mut waiters, &pool, &persistence, &mut scripts, &state, Request::Transaction {commands,modules,watched,user,origin,session,write_permit,cluster_client,reply}).await?;
                    }
                    request @ Request::Module { .. } => {
                        atomic(&mut waiters, &pool, &persistence, &mut scripts, &state, request).await?;
                    }
                    Request::InstallSnapshot {directory,reply} => {
                        let mut paused = pool.pause().await?;
                        let result = crate::admin::install_snapshot(&mut paused, &persistence, &state, &directory).await;
                        if result.is_ok() {
                            let functions = paused.engines[0].function_snapshot();
                            if functions.is_empty() {
                                let _ = scripts.control(&[Bytes::from_static(b"FUNCTION"), Bytes::from_static(b"FLUSH")]);
                            } else {
                                scripts.restore_function_snapshot(&functions).map_err(io::Error::other)?;
                            }
                        }
                        let fatal = if persistence.ensure_healthy().is_err() {
                            result.as_ref().err().map(|error| io::Error::new(error.kind(), error.to_string()))
                        } else { None };
                        paused.resume()?;
                        let _ = reply.send(result);
                        if let Some(error) = fatal { return Err(error); }
                    }
                    Request::Native(control) => {
                        native_wake_pending|=control.mutates_dataset();
                        crate::native_runtime::handle(control, &mut crate::native_runtime::Context {
                            pool: &pool, persistence: &persistence, scripts: &mut scripts,
                            state: &state, replay: &mut replay,
                        }).await;
                        persistence.ensure_healthy()?;
                    }
                    Request::Server(control) => {
                        let runtime = state.admin_runtime.get().ok_or_else(|| io::Error::other("server runtime unavailable"))?;
                        crate::server_runtime::handle(control, &mut crate::server_runtime::Context {
                            pool: &pool, persistence: &persistence,
                            state: &state, runtime,
                        }).await;
                        persistence.ensure_healthy()?;
                    }
                    Request::Unblock {client_id, error, reply} => {
                        let _ = reply.send(waiters.unblock(client_id, error));
                    }
                    Request::Shutdown => requests.close(),
                    }
                    state.set_blocked(waiters.clients.len());
                }
                None => break,
            },
            result = pending.join_next(), if !pending.is_empty() => {
                if let Some(result) = result { result.map_err(io::Error::other)??; }
            }
            result = workers.join_next(), if !workers.is_empty() => {
                if let Some(result) = result { result.map_err(io::Error::other)??; }
                return Err(io::Error::other("shard actor stopped while accepting commands"));
            }
            _ = wait_until(deadline) => {
                waiters.wake_due_admitted(&pool, &persistence, &state).await?;
                waiters.expire(Instant::now());
                state.set_blocked(waiters.clients.len());
            },
            _ = tick.tick() => {
                waiters.reap_closed();
                if native_wake_pending
                    && let Some(_permit)=state.write_gate.try_admit() {
                        waiters.replica=state.is_replica();
                        waiters.wake_all(&pool,&persistence).await?;
                        native_wake_pending=false;
                    }
                let mut paused = pool.pause().await?;
                // Lowers a stale waiter flag once blocked clients are gone.
                paused.publish_waiters(!waiters.clients.is_empty());
                if let Some(batch) = paused.reap_search(Engine::now(), persistence.capturing())? {
                    persistence.commit_batch(&batch).await?;
                }
                paused.resume()?;
                persistence.watches.reap();
                state.set_blocked(waiters.clients.len());
            },
            _ = sync.tick() => {
                if let Some(store) = &storage {
                    store.reap_retired()?;
                }
                persistence.periodic_sync().await?;
                persistence.publish(&state.metrics).await;
                state.metrics.sample_process();
                let checkpoint=persistence.needs_checkpoint(config.checkpoint_threshold).await;
                if checkpoint || slot_memory.is_some() {
                    let paused = pool.pause().await?;
                    let now = Engine::now();
                    if let (Some(slots),Some(cluster))=(&mut slot_memory,state.cluster.get()) {
                        slots.fill(0);
                        for engine in &paused.engines {engine.fill_slot_memory(now,slots);}
                        for (slot,bytes) in slots.iter().enumerate() {cluster.record_memory(slot as u16,*bytes);}
                    }
                    if checkpoint {
                        persistence.checkpoint(paused.engines.iter().flat_map(|engine| engine.snapshot_batches(now))).await?;
                    }
                    paused.resume()?;
                }
                if last_compaction.elapsed() >= Duration::from_secs(5) {
                    last_compaction = Instant::now();
                    if let Some(store) = &storage {
                        let stats = store.stats();
                        let live_bound = stats.live_payload_bytes.saturating_add(stats.live_values.saturating_mul(256));
                        if stats.physical_bytes > live_bound.saturating_mul(2).saturating_add(4 * 1024 * 1024) && !compaction.load(Ordering::Acquire) {
                            start_compaction(&mut pending, compaction.clone(), store.clone(), persistence.clone(), None);
                        }
                    }
                }
            },
        }
    }
    // Finish compaction and full-queue hand-offs, then drain every queued
    // command, inline lease and deferred commit before the final fsync, even
    // while a connection still holds the pool. Workers and the supervisor end.
    while let Some(result) = pending.join_next().await {
        result.map_err(io::Error::other)??;
    }
    pool.close().await?;
    drop(pool);
    while let Some(result) = workers.join_next().await {
        result.map_err(io::Error::other)??;
    }
    persistence.finish().await
}

fn start_compaction(
    pending: &mut JoinSet<io::Result<()>>,
    busy: Arc<AtomicBool>,
    store: Arc<crate::storage::Store>,
    persistence: Arc<Persistence>,
    reply: Option<oneshot::Sender<Resp>>,
) {
    if busy
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        if let Some(reply) = reply {
            let _ = reply.send(Resp::Error("BUSY compaction is already running".into()));
        }
        return;
    }
    pending.spawn(async move {
        let result = tokio::task::spawn_blocking(move || {
            // Keep the directory LOCK until filesystem work ends, including
            // coordinator failure/cancellation while this thread is running.
            let response = crate::admin::compact(&store);
            drop(persistence);
            response
        })
        .await;
        busy.store(false, Ordering::Release);
        let response = result.map_err(io::Error::other)?;
        if let Some(reply) = reply {
            let _ = reply.send(response);
        } else if let Resp::Error(error) = response {
            eprintln!("background compaction: {error}");
        }
        Ok(())
    });
}

async fn atomic(
    waiters: &mut Waiters,
    pool: &Pool,
    persistence: &Persistence,
    scripts: &mut ScriptRunner,
    state: &State,
    mut request: Request,
) -> io::Result<()> {
    let _write_permit = match &mut request {
        Request::Command(request) => request.write_permit.take(),
        Request::Transaction { write_permit, .. } => write_permit.take(),
        Request::Module { write_permit, .. } => write_permit.take(),
        _ => None,
    };
    let mut paused = pool.pause().await?;
    let now = Engine::now();
    let session = match &request {
        Request::Command(request) => request.session.clone(),
        Request::Transaction { session, .. } => session.clone(),
        Request::Module { session, .. } => session.clone(),
        _ => None,
    };
    paused.select_database(session.as_ref().map_or(0, |session| session.database()));
    let cluster_client = match &request {
        Request::Command(request) => request.cluster_client.as_ref(),
        Request::Transaction { cluster_client, .. } | Request::Module { cluster_client, .. } => {
            cluster_client.as_ref()
        }
        _ => None,
    };
    let script_cluster_client = crate::cluster::ClusterClient {
        readonly: cluster_client.is_some_and(|client| client.readonly),
        asking: cluster_client.is_some_and(|client| client.asking),
    };
    if state.cluster.get().is_some() {
        let writes = |command: &BoundCommand<'_>| {
            if matches!(command.routing(1), Routing::Scripting) {
                scripts
                    .invocation_context(command.args())
                    .map_or_else(|_| command.is_dataset_write(), |context| !context.readonly)
            } else {
                command.is_dataset_write()
            }
        };
        let (mut keys, write, migrating) = match &request {
            Request::Command(request) => {
                let command = BoundCommand::bind(&request.args);
                (
                    command.accessed_keys().into_vec(),
                    writes(&command),
                    request.args[0].eq_ignore_ascii_case(b"MIGRATE"),
                )
            }
            Request::Module { invocation, .. } => {
                (invocation.keys().to_vec(), invocation.is_write(), false)
            }
            Request::Transaction {
                commands, modules, ..
            } => {
                let mut keys = Vec::new();
                let mut write = false;
                for args in commands {
                    let command = BoundCommand::bind(args);
                    keys.extend(command.accessed_keys());
                    write |= writes(&command);
                }
                for (_, invocation) in modules {
                    keys.extend_from_slice(invocation.keys());
                    write |= invocation.is_write();
                }
                (keys, write, false)
            }
            _ => return Err(io::Error::other("invalid cluster admission request")),
        };
        keys.sort_unstable();
        keys.dedup();
        let mut fallback = crate::cluster::ClusterClient::default();
        let client = match &mut request {
            Request::Command(request) => request.cluster_client.as_mut().unwrap_or(&mut fallback),
            Request::Transaction { cluster_client, .. }
            | Request::Module { cluster_client, .. } => {
                cluster_client.as_mut().unwrap_or(&mut fallback)
            }
            _ => unreachable!(),
        };
        if let Err(response) = crate::native_runtime::admit_cluster_paused(
            &keys,
            write,
            false,
            migrating,
            client,
            &mut paused,
            persistence,
            state,
        )
        .await
        {
            paused.resume()?;
            match request {
                Request::Command(request) => {
                    let _ = request.reply.send(response);
                }
                Request::Transaction { reply, .. } | Request::Module { reply, .. } => {
                    let _ = reply.send(response);
                }
                _ => unreachable!(),
            }
            return Ok(());
        }
    }
    let visible_slots = if let Some(cluster) = state.cluster.get() {
        Some(cluster.visible_slots().await)
    } else {
        None
    };
    let (reply, batch, mut ready, publications, swapped_databases, channel) = match request {
        Request::Command(request) => {
            let mut executor = Atomic::new(
                &mut paused,
                persistence,
                now,
                request.origin,
                request.user,
                request.session,
                state.admission(),
                &state.pubsub,
            );
            executor.set_environment(state);
            executor.set_cluster_client(script_cluster_client);
            executor.set_visible_slots(visible_slots);
            let revision = scripts.function_revision();
            executor.track_ready(!waiters.clients.is_empty());
            let reply = match BoundCommand::bind(&request.args).routing(1) {
                Routing::Scripting => executor.script(&request.args, scripts)?,
                Routing::Maintenance => executor.server_command(&request.args, state).await?,
                _ => executor.data(&request.args)?,
            };
            executor.functions_changed(revision, scripts);
            let (batch, ready, publications, swapped) = executor.finish(persistence.capturing());
            (reply, batch, ready, publications, swapped, request.reply)
        }
        Request::Module {
            invocation,
            user,
            origin,
            session,
            write_permit: _,
            cluster_client: _,
            reply,
        } => {
            let mut executor = Atomic::new(
                &mut paused,
                persistence,
                now,
                origin,
                user,
                session,
                state.admission(),
                &state.pubsub,
            );
            executor.set_environment(state);
            executor.set_cluster_client(script_cluster_client);
            executor.set_visible_slots(visible_slots);
            executor.track_ready(!waiters.clients.is_empty());
            let response = executor.module(invocation)?;
            let (batch, ready, publications, swapped) = executor.finish(persistence.capturing());
            (response, batch, ready, publications, swapped, reply)
        }
        Request::Transaction {
            commands,
            modules,
            watched,
            user,
            origin,
            session,
            write_permit: _,
            cluster_client: _,
            reply,
        } => {
            if watched.iter().any(|token| token.changed(now)) {
                paused.resume()?;
                let _ = reply.send(Resp::Array(None));
                return Ok(());
            }
            let mut executor = Atomic::new(
                &mut paused,
                persistence,
                now,
                origin,
                user,
                session,
                state.admission(),
                &state.pubsub,
            );
            executor.set_environment(state);
            executor.set_cluster_client(script_cluster_client);
            executor.set_visible_slots(visible_slots);
            executor.track_ready(!waiters.clients.is_empty());
            let response = executor
                .transaction(&commands, modules, scripts, state)
                .await?;
            let (batch, ready, publications, swapped) = executor.finish(persistence.capturing());
            (response, batch, ready, publications, swapped, reply)
        }
        Request::Watch { .. }
        | Request::InstallSnapshot { .. }
        | Request::Native(_)
        | Request::Server(_)
        | Request::Unblock { .. }
        | Request::Shutdown => {
            return Err(io::Error::other("WATCH reached atomic data execution"));
        }
    };
    if !swapped_databases.is_empty() {
        ready.extend(
            waiters
                .keys
                .keys()
                .filter(|(database, _)| swapped_databases.contains(database))
                .cloned(),
        );
    }
    if let Some(batch) = batch {
        let offset = persistence
            .commit_batch_offset(&batch, session.as_ref().map(|session| session.client_id))
            .await?;
        if let Some(session) = &session {
            session.last_write_offset.store(offset, Ordering::Release);
        }
    }
    let mut forwarded = Vec::new();
    for (publication, args) in publications {
        if state.cluster.get().is_none()
            && let Some(offset) = persistence.publish_ephemeral(&args).await?
            && let Some(session) = &session
        {
            session.last_write_offset.store(offset, Ordering::Release);
        }
        publication.deliver();
        if state.cluster.get().is_some() {
            forwarded.push(args);
        }
    }
    // No consumer can observe an intermediate script/transaction list state.
    // All wake pops are themselves committed before delivering a value.
    let mut notified = HashSet::new();
    for key in ready {
        if !notified.insert(key.clone()) {
            continue;
        }
        let keys = waiters.wake_keys(vec![key.clone()]);
        let mut engine = paused.take_database_keys(&keys, persistence.capturing());
        waiters.wake(&mut engine, &key, persistence).await?;
        paused.restore(engine);
    }
    paused.resume()?;
    if let Some(cluster) = state.cluster.get() {
        for args in forwarded {
            if args[0].eq_ignore_ascii_case(b"SPUBLISH") {
                cluster.forward_shard(&args[1], &args[2]).await?;
            } else {
                cluster.forward_publish(&args[1], &args[2]).await?;
            }
        }
    }
    let _ = channel.send(reply);
    Ok(())
}

async fn coordinated(
    waiters: &mut Waiters,
    pool: &Pool,
    persistence: &Persistence,
    keys: Vec<Bytes>,
    ready: Option<Bytes>,
    mut request: CommandRequest,
    state: Option<&State>,
) -> io::Result<()> {
    let database = request
        .session
        .as_ref()
        .map_or(0, |session| session.database());
    let mut paused = pool.pause().await?;
    paused.select_database(database);
    if let Some(state) = state.filter(|state| state.cluster.get().is_some()) {
        let mut fallback = crate::cluster::ClusterClient::default();
        let client = request.cluster_client.as_mut().unwrap_or(&mut fallback);
        if let Err(response) = crate::native_runtime::admit_cluster_paused(
            &keys,
            BoundCommand::bind(&request.args).is_dataset_write(),
            false,
            request.args[0].eq_ignore_ascii_case(b"MIGRATE"),
            client,
            &mut paused,
            persistence,
            state,
        )
        .await
        {
            paused.resume()?;
            let _ = request.reply.send(response);
            return Ok(());
        }
    }
    let mut keys = keys;
    if let Some(key) = ready {
        keys.push(key);
    }
    let keys = waiters.wake_keys(keys.into_iter().map(|key| (database, key)).collect());
    let mut engine = paused.take_database_keys(&keys, persistence.capturing());
    let cpu_observed = state.is_none_or(State::cpu_observed);
    waiters
        .execute(&mut engine, request, persistence, cpu_observed)
        .await?;
    paused.restore(engine);
    // A new registration is visible to inline commands before any cell is
    // released; they then escalate to this coordinator's exact key check.
    paused.publish_waiters(!waiters.clients.is_empty());
    paused.resume()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn swapping_databases_wakes_existing_consumers_without_a_journal() {
        let mut engine = Engine::new();
        engine.switch_database(1);
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&[
                Bytes::from_static(b"RPUSH"),
                Bytes::from_static(b"jobs"),
                Bytes::from_static(b"moved"),
            ]))
            .unwrap();
        engine.switch_database(0);
        let persistence = Arc::new(Persistence::new(None));
        let server = Arc::new(State::new(0, 2, false));
        let (pool, mut workers) = Pool::start(engine, 2, persistence.clone(), server);
        let mut waiters = Waiters::default();
        let session = Arc::new(ClientSession::new(303));
        let (reply, blocked) = oneshot::channel();
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            vec![Bytes::from_static(b"jobs")],
            None,
            CommandRequest::client(
                vec![
                    Bytes::from_static(b"BLPOP"),
                    Bytes::from_static(b"jobs"),
                    Bytes::from_static(b"0"),
                ],
                reply,
                None,
                crate::protocol::Version::Resp2,
                Some(session.clone()),
            ),
            None,
        )
        .await
        .unwrap();
        let mut scripts = ScriptRunner::new().unwrap();
        let state = State::new(0, 2, false);
        let (reply, response) = oneshot::channel();
        atomic(
            &mut waiters,
            &pool,
            &persistence,
            &mut scripts,
            &state,
            Request::Command(CommandRequest::client(
                vec![
                    Bytes::from_static(b"SWAPDB"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"1"),
                ],
                reply,
                None,
                crate::protocol::Version::Resp2,
                Some(session),
            )),
        )
        .await
        .unwrap();
        assert_eq!(response.await.unwrap(), Resp::Simple("OK"));
        assert_eq!(
            blocked.await.unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"jobs"))),
                Resp::Bulk(Some(Bytes::from_static(b"moved"))),
            ]))
        );
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }

    async fn command(
        waiters: &mut Waiters,
        engine: &mut Engine,
        args: &[&str],
    ) -> oneshot::Receiver<Resp> {
        let (reply, response) = oneshot::channel();
        waiters
            .execute(
                engine,
                CommandRequest::client(
                    args.iter()
                        .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                        .collect(),
                    reply,
                    None,
                    crate::protocol::Version::Resp2,
                    None,
                ),
                &Persistence::new(None),
                true,
            )
            .await
            .unwrap();
        response
    }

    async fn session_command(
        waiters: &mut Waiters,
        engine: &mut Engine,
        session: Arc<ClientSession>,
        args: &[&str],
    ) -> oneshot::Receiver<Resp> {
        let (reply, response) = oneshot::channel();
        waiters
            .execute(
                engine,
                CommandRequest::client(
                    args.iter()
                        .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
                        .collect(),
                    reply,
                    None,
                    crate::protocol::Version::Resp2,
                    Some(session),
                ),
                &Persistence::new(None),
                true,
            )
            .await
            .unwrap();
        response
    }

    #[tokio::test]
    async fn blocked_clients_consume_only_their_selected_database() {
        let mut waiters = Waiters::default();
        let mut engine = Engine::new();
        let first = Arc::new(ClientSession::new(41));
        let second = Arc::new(ClientSession::new(42));
        second.set_database(1);
        let mut blocked = session_command(
            &mut waiters,
            &mut engine,
            first.clone(),
            &["BLPOP", "jobs", "0"],
        )
        .await;
        assert_eq!(
            session_command(
                &mut waiters,
                &mut engine,
                second.clone(),
                &["RPUSH", "jobs", "other-db"]
            )
            .await
            .await
            .unwrap(),
            Resp::Integer(1)
        );
        assert!(matches!(
            blocked.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(
            session_command(
                &mut waiters,
                &mut engine,
                first,
                &["RPUSH", "jobs", "own-db"]
            )
            .await
            .await
            .unwrap(),
            Resp::Integer(1)
        );
        assert_eq!(
            blocked.await.unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"jobs"))),
                Resp::Bulk(Some(Bytes::from_static(b"own-db"))),
            ]))
        );
        assert_eq!(
            session_command(&mut waiters, &mut engine, second, &["LPOP", "jobs"])
                .await
                .await
                .unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"other-db")))
        );
    }

    #[tokio::test]
    async fn explicit_unblock_targets_client_id_and_preserves_timeout_reply_shape() {
        let mut waiters = Waiters::default();
        let mut engine = Engine::new();
        let list = session_command(
            &mut waiters,
            &mut engine,
            Arc::new(ClientSession::new(100)),
            &["BLPOP", "jobs", "0"],
        )
        .await;
        let moved = session_command(
            &mut waiters,
            &mut engine,
            Arc::new(ClientSession::new(200)),
            &["BLMOVE", "source", "destination", "LEFT", "RIGHT", "0"],
        )
        .await;
        assert_eq!(waiters.unblock(999, false), Resp::Integer(0));
        assert_eq!(waiters.unblock(200, false), Resp::Integer(1));
        assert_eq!(moved.await.unwrap(), Resp::Bulk(None));
        assert_eq!(waiters.unblock(100, true), Resp::Integer(1));
        assert!(
            matches!(list.await.unwrap(),Resp::Error(message) if message.starts_with("UNBLOCKED "))
        );
        assert_eq!(waiters.unblock(100, true), Resp::Integer(0));
    }

    #[tokio::test]
    async fn blocking_moves_cascade_to_destination_waiters_in_fifo_order() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut first = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "queued", "primed", "RIGHT", "LEFT", "0"],
        )
        .await;
        let mut second = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "queued", "other", "LEFT", "RIGHT", "0"],
        )
        .await;
        let mut bridge = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "primed", "final", "LEFT", "RIGHT", "0"],
        )
        .await;
        let mut consumer = command(&mut waiters, &mut engine, &["BLPOP", "final", "0"]).await;
        command(
            &mut waiters,
            &mut engine,
            &["RPUSH", "queued", "a", "b", "c"],
        )
        .await;
        assert_eq!(
            first.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"c")))
        );
        assert_eq!(
            second.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"a")))
        );
        assert_eq!(
            bridge.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"c")))
        );
        assert_eq!(
            consumer.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"final"))),
                Resp::Bulk(Some(Bytes::from_static(b"c"))),
            ]))
        );
        let mut retained = command(&mut waiters, &mut engine, &["LPOP", "queued"]).await;
        assert_eq!(
            retained.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"b")))
        );
        assert!(waiters.clients.is_empty());
        assert!(waiters.keys.is_empty());
    }

    #[tokio::test]
    async fn blocked_move_destination_errors_leave_the_source_for_the_next_waiter() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut rejected = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "source", "wrong", "RIGHT", "LEFT", "0"],
        )
        .await;
        let mut accepted = command(&mut waiters, &mut engine, &["BLPOP", "source", "0"]).await;
        command(&mut waiters, &mut engine, &["SET", "wrong", "string"]).await;
        assert!(matches!(
            rejected.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        command(&mut waiters, &mut engine, &["RPUSH", "source", "retained"]).await;
        assert!(
            matches!(rejected.try_recv().unwrap(), Resp::Error(error) if error.starts_with("WRONGTYPE"))
        );
        assert_eq!(
            accepted.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"source"))),
                Resp::Bulk(Some(Bytes::from_static(b"retained"))),
            ]))
        );
    }

    #[tokio::test]
    async fn blocking_moves_timeout_as_bulk_nil_and_disconnections_do_not_move_values() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut timed = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "source", "destination", "RIGHT", "LEFT", "0.01"],
        )
        .await;
        let deadline = waiters.deadlines.first().unwrap().0;
        waiters.expire(deadline);
        assert_eq!(timed.try_recv().unwrap(), Resp::Bulk(None));
        let disconnected = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "source", "destination", "RIGHT", "LEFT", "0"],
        )
        .await;
        drop(disconnected);
        command(&mut waiters, &mut engine, &["RPUSH", "source", "job"]).await;
        let mut retained = command(&mut waiters, &mut engine, &["LPOP", "source"]).await;
        assert_eq!(
            retained.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"job")))
        );
        let mut destination = command(&mut waiters, &mut engine, &["LLEN", "destination"]).await;
        assert_eq!(destination.try_recv().unwrap(), Resp::Integer(0));
        assert!(waiters.clients.is_empty());
    }

    #[tokio::test]
    async fn same_key_blocked_rotations_wake_later_consumers_without_losing_the_value() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut rotation = command(
            &mut waiters,
            &mut engine,
            &["BLMOVE", "q", "q", "RIGHT", "LEFT", "0"],
        )
        .await;
        let mut consumer = command(&mut waiters, &mut engine, &["BRPOP", "q", "0"]).await;
        command(&mut waiters, &mut engine, &["RPUSH", "q", "job"]).await;
        assert_eq!(
            rotation.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"job")))
        );
        assert_eq!(
            consumer.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"q"))),
                Resp::Bulk(Some(Bytes::from_static(b"job"))),
            ]))
        );
        assert!(waiters.clients.is_empty());
    }

    #[tokio::test]
    async fn sharded_move_wakeups_include_transitive_destinations() {
        let path = std::env::temp_dir().join(format!(
            "ffkv-list-move-wal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let directory = path.clone();
        let (wal, _) = tokio::task::spawn_blocking(move || {
            Wal::open(&directory, crate::wal::Durability::Strict, |_| Ok(()))
        })
        .await
        .unwrap()
        .unwrap();
        let persistence = Arc::new(Persistence::new(Some(wal)));
        let server = Arc::new(State::new(0, 4, false));
        let (pool, mut workers) = Pool::start(Engine::new(), 4, persistence.clone(), server);
        let mut waiters = Waiters::default();
        let mut keys = Vec::new();
        for shard in 0..4 {
            keys.push(
                (0..)
                    .map(|index| Bytes::from(format!("list-move-{shard}-{index}")))
                    .find(|key| Engine::shard_for(key, 4) == shard)
                    .unwrap(),
            );
        }
        let mut replies = Vec::new();
        for index in 0..3 {
            let (reply, response) = oneshot::channel();
            let args = vec![
                Bytes::from_static(b"BLMOVE"),
                keys[index].clone(),
                keys[index + 1].clone(),
                Bytes::from_static(b"RIGHT"),
                Bytes::from_static(b"LEFT"),
                Bytes::from_static(b"0"),
            ];
            let accessed = BoundCommand::bind(&args).accessed_keys().into_vec();
            coordinated(
                &mut waiters,
                &pool,
                &persistence,
                accessed,
                Some(keys[index + 1].clone()),
                CommandRequest::client(args, reply, None, crate::protocol::Version::Resp2, None),
                None,
            )
            .await
            .unwrap();
            replies.push(response);
        }
        let (reply, mut consumer) = oneshot::channel();
        let args = vec![
            Bytes::from_static(b"BLPOP"),
            keys[3].clone(),
            Bytes::from_static(b"0"),
        ];
        let accessed = BoundCommand::bind(&args).accessed_keys().into_vec();
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            accessed,
            None,
            CommandRequest::client(args, reply, None, crate::protocol::Version::Resp2, None),
            None,
        )
        .await
        .unwrap();
        let (reply, mut push) = oneshot::channel();
        let args = vec![
            Bytes::from_static(b"RPUSH"),
            keys[0].clone(),
            Bytes::from_static(b"retained"),
            Bytes::from_static(b"job"),
        ];
        let accessed = BoundCommand::bind(&args).accessed_keys().into_vec();
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            accessed,
            Some(keys[0].clone()),
            CommandRequest::client(args, reply, None, crate::protocol::Version::Resp2, None),
            None,
        )
        .await
        .unwrap();
        assert_eq!(push.try_recv().unwrap(), Resp::Integer(2));
        for mut response in replies {
            assert_eq!(
                response.try_recv().unwrap(),
                Resp::Bulk(Some(Bytes::from_static(b"job")))
            );
        }
        assert_eq!(
            consumer.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(keys[3].clone())),
                Resp::Bulk(Some(Bytes::from_static(b"job"))),
            ]))
        );
        assert!(waiters.clients.is_empty());
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
        persistence.finish().await.unwrap();
        drop(persistence);
        let directory = path.clone();
        let recovery_keys = keys.clone();
        let (wal, recovered) = tokio::task::spawn_blocking(move || {
            let mut recovered = Engine::new();
            let mut job_seen = false;
            let mut job_consumed = false;
            let (wal, _) = Wal::open(&directory, crate::wal::Durability::Strict, |batch| {
                recovered.replay(batch)?;
                let mut jobs = 0;
                for (index, key) in recovery_keys.iter().enumerate() {
                    let args = vec![
                        Bytes::from_static(b"LRANGE"),
                        key.clone(),
                        Bytes::from_static(b"0"),
                        Bytes::from_static(b"-1"),
                    ];
                    let Execution::Reply(Resp::Array(Some(values))) =
                        recovered.execute(&mut crate::engine::BoundCommand::bind(&args))?
                    else {
                        panic!("recovered move source must remain a list");
                    };
                    let retained = Resp::Bulk(Some(Bytes::from_static(b"retained")));
                    if index == 0 {
                        assert_eq!(values.first(), Some(&retained));
                    }
                    for value in values {
                        if value == Resp::Bulk(Some(Bytes::from_static(b"job"))) {
                            jobs += 1;
                        } else {
                            assert_eq!(index, 0);
                            assert_eq!(value, retained);
                        }
                    }
                }
                // Every durable recovery boundary must expose at most one
                // job. Once consumed, it cannot reappear from a split move.
                assert!(jobs <= 1);
                if job_consumed {
                    assert_eq!(jobs, 0);
                }
                job_consumed |= job_seen && jobs == 0;
                job_seen |= jobs == 1;
                Ok(())
            })
            .unwrap();
            (wal, recovered)
        })
        .await
        .unwrap();
        let mut recovered = recovered;
        let args = vec![
            Bytes::from_static(b"LRANGE"),
            keys[0].clone(),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"-1"),
        ];
        assert!(
            matches!(recovered.execute(&mut crate::engine::BoundCommand::bind(&args)).unwrap(), Execution::Reply(Resp::Array(Some(values))) if values == vec![Resp::Bulk(Some(Bytes::from_static(b"retained")))])
        );
        for key in &keys[1..] {
            let args = vec![Bytes::from_static(b"EXISTS"), key.clone()];
            assert!(matches!(
                recovered
                    .execute(&mut crate::engine::BoundCommand::bind(&args))
                    .unwrap(),
                Execution::Reply(Resp::Integer(0))
            ));
        }
        drop(wal);
        drop(recovered);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[tokio::test]
    async fn consumers_receive_values_in_registration_order_and_remove_other_key_indexes() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut first = command(&mut waiters, &mut engine, &["BLPOP", "a", "b", "a", "0"]).await;
        let mut second = command(&mut waiters, &mut engine, &["BRPOP", "b", "0"]).await;
        assert!(matches!(
            first.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let mut push = command(
            &mut waiters,
            &mut engine,
            &["RPUSH", "b", "one", "two", "three"],
        )
        .await;
        assert_eq!(push.try_recv().unwrap(), Resp::Integer(3));
        assert_eq!(
            first.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"b"))),
                Resp::Bulk(Some(Bytes::from_static(b"one"))),
            ]))
        );
        assert_eq!(
            second.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"b"))),
                Resp::Bulk(Some(Bytes::from_static(b"three"))),
            ]))
        );
        let mut left = command(&mut waiters, &mut engine, &["LPOP", "b"]).await;
        assert_eq!(
            left.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"two")))
        );
        assert!(waiters.clients.is_empty());
        assert!(waiters.keys.is_empty());
        assert!(waiters.deadlines.is_empty());
    }

    #[tokio::test]
    async fn timed_out_and_disconnected_consumers_cannot_steal_later_pushes() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut timed = command(&mut waiters, &mut engine, &["BLPOP", "jobs", "0.01"]).await;
        let due = waiters.deadlines.first().unwrap().0;
        waiters.expire(due);
        assert_eq!(timed.try_recv().unwrap(), Resp::Array(None));
        let disconnected = command(&mut waiters, &mut engine, &["BRPOP", "jobs", "0"]).await;
        drop(disconnected);
        let mut push = command(&mut waiters, &mut engine, &["LPUSH", "jobs", "retained"]).await;
        assert_eq!(push.try_recv().unwrap(), Resp::Integer(1));
        let mut pop = command(&mut waiters, &mut engine, &["RPOP", "jobs"]).await;
        assert_eq!(
            pop.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"retained")))
        );
        assert!(waiters.keys.is_empty());
    }

    #[tokio::test]
    async fn awakened_consumer_rechecks_original_key_order_before_popping() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut consumer = command(&mut waiters, &mut engine, &["BLPOP", "z", "a", "z", "0"]).await;
        let mut set = command(&mut waiters, &mut engine, &["SET", "z", "string"]).await;
        assert_eq!(set.try_recv().unwrap(), Resp::Simple("OK"));
        assert!(matches!(
            consumer.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        command(&mut waiters, &mut engine, &["RPUSH", "a", "job"]).await;
        match consumer.try_recv().unwrap() {
            Resp::Error(error) => assert!(error.starts_with("WRONGTYPE ")),
            reply => panic!("expected WRONGTYPE, got {reply:?}"),
        }
        let mut retained = command(&mut waiters, &mut engine, &["LPOP", "a"]).await;
        assert_eq!(
            retained.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"job")))
        );
    }

    #[tokio::test]
    async fn draining_a_ready_key_does_not_spuriously_wake_later_consumers() {
        let mut engine = Engine::new();
        let mut waiters = Waiters::default();
        let mut first = command(&mut waiters, &mut engine, &["BLPOP", "b", "0"]).await;
        let mut later = command(&mut waiters, &mut engine, &["BLPOP", "a", "b", "0"]).await;
        command(&mut waiters, &mut engine, &["SET", "a", "string"]).await;
        command(&mut waiters, &mut engine, &["RPUSH", "b", "first"]).await;
        assert_eq!(
            first.try_recv().unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"b"))),
                Resp::Bulk(Some(Bytes::from_static(b"first"))),
            ]))
        );
        assert!(matches!(
            later.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        command(&mut waiters, &mut engine, &["RPUSH", "b", "second"]).await;
        match later.try_recv().unwrap() {
            Resp::Error(error) => assert!(error.starts_with("WRONGTYPE ")),
            reply => panic!("expected WRONGTYPE, got {reply:?}"),
        }
        let mut retained = command(&mut waiters, &mut engine, &["LPOP", "b"]).await;
        assert_eq!(
            retained.try_recv().unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"second")))
        );
    }

    fn rpush(value: &'static [u8]) -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"RPUSH"),
            Bytes::from_static(b"jobs"),
            Bytes::from_static(value),
        ]
    }

    fn inline(args: &[Bytes]) -> crate::request::SingleKey<'_> {
        crate::request::SingleKey {
            command: BoundCommand::bind(args),
            origin: crate::request::Origin::Client(crate::protocol::Version::Resp2),
            user: None,
            session: None,
            write_permit: None,
            cluster_client: None,
        }
    }

    fn blpop(reply: oneshot::Sender<Resp>) -> CommandRequest {
        CommandRequest::client(
            vec![
                Bytes::from_static(b"BLPOP"),
                Bytes::from_static(b"jobs"),
                Bytes::from_static(b"0"),
            ],
            reply,
            None,
            crate::protocol::Version::Resp2,
            None,
        )
    }

    fn popped(value: &'static [u8]) -> Resp {
        Resp::Array(Some(vec![
            Resp::Bulk(Some(Bytes::from_static(b"jobs"))),
            Resp::Bulk(Some(Bytes::from_static(value))),
        ]))
    }

    #[tokio::test]
    async fn inline_pushes_never_lose_a_wake_to_a_concurrent_blocking_registration() {
        use crate::sharding::Ran;
        let persistence = Arc::new(Persistence::new(None));
        let state = Arc::new(State::new(0, 2, false));
        let (pool, mut workers) = Pool::start(Engine::new(), 2, persistence.clone(), state.clone());
        let mut waiters = Waiters::default();
        let keys = vec![Bytes::from_static(b"jobs")];
        let shard = pool
            .inline_shard(&BoundCommand::bind(&rpush(b"probe")), &state)
            .unwrap();

        // A registering pause queues behind an inline lease: the push commits
        // first and the blocking pop consumes it instead of registering.
        let lease = pool.lease(shard).await.unwrap();
        let (reply, mut first) = oneshot::channel();
        {
            let registration = coordinated(
                &mut waiters,
                &pool,
                &persistence,
                keys.clone(),
                None,
                blpop(reply),
                None,
            );
            tokio::pin!(registration);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), registration.as_mut())
                    .await
                    .is_err()
            );
            assert!(matches!(
                lease.run(inline(&rpush(b"first")), &state).unwrap(),
                Ran::Done(Resp::Integer(1))
            ));
            registration.await.unwrap();
        }
        assert_eq!(first.try_recv().unwrap(), popped(b"first"));
        assert!(waiters.clients.is_empty());

        // A waiter registered by a pause makes later inline pushes fall back,
        // untouched, to the coordinator's exact key route, which wakes it.
        let (reply, mut second) = oneshot::channel();
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            keys.clone(),
            None,
            blpop(reply),
            None,
        )
        .await
        .unwrap();
        assert_eq!(waiters.clients.len(), 1);
        let second_push = rpush(b"second");
        let Ran::Escalate(command) = pool
            .lease(shard)
            .await
            .unwrap()
            .run(inline(&second_push), &state)
            .unwrap()
        else {
            panic!("an inline push ran while a client was blocked");
        };
        assert!(matches!(
            second.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert!(waiters.keys.contains_key(&(0, Bytes::from_static(b"jobs"))));
        let (reply, mut pushed) = oneshot::channel();
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            command.command.accessed_keys().into_vec(),
            Some(Bytes::from_static(b"jobs")),
            command.into_request(reply),
            None,
        )
        .await
        .unwrap();
        assert_eq!(pushed.try_recv().unwrap(), Resp::Integer(1));
        assert_eq!(second.try_recv().unwrap(), popped(b"second"));

        // That pause published that no client is blocked any more.
        assert!(matches!(
            pool.lease(shard)
                .await
                .unwrap()
                .run(inline(&rpush(b"third")), &state)
                .unwrap(),
            Ran::Done(Resp::Integer(1))
        ));
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }
}

#[cfg(test)]
mod stream_tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }
    async fn command(
        waiters: &mut Waiters,
        engine: &mut Engine,
        values: &[&str],
    ) -> oneshot::Receiver<Resp> {
        let (reply, rx) = oneshot::channel();
        waiters
            .execute(
                engine,
                CommandRequest::client(
                    args(values),
                    reply,
                    None,
                    crate::protocol::Version::Resp2,
                    None,
                ),
                &Persistence::new(None),
                true,
            )
            .await
            .unwrap();
        rx
    }
    fn first_id(reply: Resp) -> Bytes {
        let Resp::Array(Some(streams)) = reply else {
            panic!("stream reply")
        };
        let Resp::Array(Some(pair)) = streams.into_iter().next().unwrap() else {
            panic!("stream pair")
        };
        let Resp::Array(Some(rows)) = pair.into_iter().nth(1).unwrap() else {
            panic!("stream rows")
        };
        let Resp::Array(Some(row)) = rows.into_iter().next().unwrap() else {
            panic!("stream row")
        };
        let Resp::Bulk(Some(id)) = row.into_iter().next().unwrap() else {
            panic!("stream id")
        };
        id
    }

    #[tokio::test]
    async fn dollar_is_frozen_once_and_each_reader_observes_committed_append() {
        let (mut engine, mut waiters) = (Engine::new(), Waiters::default());
        command(&mut waiters, &mut engine, &["XADD", "s", "1-0", "f", "old"]).await;
        let mut a = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
        )
        .await;
        let mut b = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "$"],
        )
        .await;
        assert!(matches!(
            a.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(waiters.clients.len(), 2);
        command(&mut waiters, &mut engine, &["XADD", "s", "2-0", "f", "new"]).await;
        assert_eq!(first_id(a.try_recv().unwrap()), Bytes::from_static(b"2-0"));
        assert_eq!(first_id(b.try_recv().unwrap()), Bytes::from_static(b"2-0"));
        assert!(waiters.clients.is_empty());
        assert_eq!(waiters.retained_bytes, 0);
    }

    #[tokio::test]
    async fn timeout_disconnect_and_actor_memory_bound_remove_all_indexes() {
        let (mut engine, mut waiters) = (Engine::new(), Waiters::default());
        let timed = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "1", "STREAMS", "s", "0"],
        )
        .await;
        waiters.expire(Instant::now() + Duration::from_millis(2));
        assert_eq!(timed.await.unwrap(), Resp::Array(None));
        let abandoned = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "0", "STREAMS", "a", "b", "0", "0"],
        )
        .await;
        drop(abandoned);
        waiters.reap_closed();
        assert!(waiters.keys.is_empty());
        assert!(waiters.deadlines.is_empty());
        assert!(waiters.retries.is_empty());
        assert_eq!(waiters.retained_bytes, 0);
        waiters.retained_bytes = 64 * 1024 * 1024;
        let rejected = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "0"],
        )
        .await;
        assert!(matches!(rejected.await.unwrap(), Resp::Error(_)));
        assert!(waiters.clients.is_empty());
    }

    #[tokio::test]
    async fn deleted_or_recreated_group_unblocks_with_nogroup() {
        let (mut engine, mut waiters) = (Engine::new(), Waiters::default());
        command(
            &mut waiters,
            &mut engine,
            &["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"],
        )
        .await;
        let waiting = command(
            &mut waiters,
            &mut engine,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                ">",
            ],
        )
        .await;
        command(&mut waiters, &mut engine, &["DEL", "s"]).await;
        assert!(
            matches!(waiting.await.unwrap(),Resp::Error(reason)if reason.starts_with("NOGROUP "))
        );
        command(
            &mut waiters,
            &mut engine,
            &["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"],
        )
        .await;
        let waiting = command(
            &mut waiters,
            &mut engine,
            &[
                "XREADGROUP",
                "GROUP",
                "g",
                "c",
                "BLOCK",
                "0",
                "STREAMS",
                "s",
                ">",
            ],
        )
        .await;
        // Simulate one atomic command's committed final state: the same group
        // name exists again, but it must not inherit the old group's waiters.
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&args(&[
                "XGROUP", "DESTROY", "s", "g",
            ])))
            .unwrap();
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&args(&[
                "XGROUP", "CREATE", "s", "g", "0",
            ])))
            .unwrap();
        let persistence = Persistence::new(None);
        persistence.commit(&mut engine).await.unwrap();
        waiters
            .wake(&mut engine, &(0, Bytes::from_static(b"s")), &persistence)
            .await
            .unwrap();
        assert!(
            matches!(waiting.await.unwrap(),Resp::Error(reason)if reason.starts_with("NOGROUP "))
        );
    }

    #[tokio::test]
    async fn intermediate_append_removed_before_commit_never_reaches_a_reader() {
        let (mut engine, mut waiters) = (Engine::new(), Waiters::default());
        let mut waiting = command(
            &mut waiters,
            &mut engine,
            &["XREAD", "BLOCK", "0", "STREAMS", "s", "0"],
        )
        .await;
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&args(&[
                "XADD",
                "s",
                "1-0",
                "f",
                "intermediate",
            ])))
            .unwrap();
        engine
            .execute(&mut crate::engine::BoundCommand::bind(&args(&[
                "XDEL", "s", "1-0",
            ])))
            .unwrap();
        let persistence = Persistence::new(None);
        persistence.commit(&mut engine).await.unwrap();
        waiters
            .wake(&mut engine, &(0, Bytes::from_static(b"s")), &persistence)
            .await
            .unwrap();
        assert!(matches!(
            waiting.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        command(
            &mut waiters,
            &mut engine,
            &["XADD", "s", "2-0", "f", "final"],
        )
        .await;
        assert_eq!(first_id(waiting.await.unwrap()), Bytes::from_static(b"2-0"));
    }

    #[tokio::test]
    async fn multistream_read_acquires_every_shard_and_claim_aging_uses_real_deadline() {
        let persistence = Arc::new(Persistence::new(None));
        let mut engine = Engine::new();
        let key0 = Bytes::from_static(b"stream-a");
        let key1 = (0..1000)
            .map(|n| Bytes::from(format!("stream-{n}")))
            .find(|key| Engine::shard_for(key, 2) != Engine::shard_for(&key0, 2))
            .unwrap();
        for key in [&key0, &key1] {
            engine
                .execute(&mut crate::engine::BoundCommand::bind(&[
                    Bytes::from_static(b"XGROUP"),
                    Bytes::from_static(b"CREATE"),
                    key.clone(),
                    Bytes::from_static(b"g"),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"MKSTREAM"),
                ]))
                .unwrap();
        }
        let server = Arc::new(State::new(0, 2, false));
        let (pool, mut workers) = Pool::start(engine, 2, persistence.clone(), server);
        let mut waiters = Waiters::default();
        let (reply, mut read) = oneshot::channel();
        let read_args = vec![
            Bytes::from_static(b"XREAD"),
            Bytes::from_static(b"BLOCK"),
            Bytes::from_static(b"0"),
            Bytes::from_static(b"STREAMS"),
            key0.clone(),
            key1.clone(),
            Bytes::from_static(b"$"),
            Bytes::from_static(b"$"),
        ];
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            vec![key0.clone(), key1.clone()],
            None,
            CommandRequest::client(
                read_args,
                reply,
                None,
                crate::protocol::Version::Resp2,
                None,
            ),
            None,
        )
        .await
        .unwrap();
        let (reply, _) = oneshot::channel();
        let add = vec![
            Bytes::from_static(b"XADD"),
            key1.clone(),
            Bytes::from_static(b"1-0"),
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
        ];
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            vec![key1.clone()],
            Some(key1.clone()),
            CommandRequest::client(add, reply, None, crate::protocol::Version::Resp2, None),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            first_id(read.try_recv().unwrap()),
            Bytes::from_static(b"1-0")
        );
        let (reply, _) = oneshot::channel();
        let consume = vec![
            Bytes::from_static(b"XREADGROUP"),
            Bytes::from_static(b"GROUP"),
            Bytes::from_static(b"g"),
            Bytes::from_static(b"old"),
            Bytes::from_static(b"STREAMS"),
            key1.clone(),
            Bytes::from_static(b">"),
        ];
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            vec![key1.clone()],
            None,
            CommandRequest::client(consume, reply, None, crate::protocol::Version::Resp2, None),
            None,
        )
        .await
        .unwrap();
        let (reply, mut claimed) = oneshot::channel();
        let claim = vec![
            Bytes::from_static(b"XREADGROUP"),
            Bytes::from_static(b"GROUP"),
            Bytes::from_static(b"g"),
            Bytes::from_static(b"new"),
            Bytes::from_static(b"BLOCK"),
            Bytes::from_static(b"1000"),
            Bytes::from_static(b"CLAIM"),
            Bytes::from_static(b"30"),
            Bytes::from_static(b"STREAMS"),
            key1.clone(),
            Bytes::from_static(b">"),
        ];
        coordinated(
            &mut waiters,
            &pool,
            &persistence,
            vec![key1],
            None,
            CommandRequest::client(claim, reply, None, crate::protocol::Version::Resp2, None),
            None,
        )
        .await
        .unwrap();
        assert!(!waiters.retries.is_empty());
        tokio::time::sleep(Duration::from_millis(40)).await;
        waiters.wake_due(&pool, &persistence).await.unwrap();
        assert_eq!(
            first_id(claimed.try_recv().unwrap()),
            Bytes::from_static(b"1-0")
        );
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }
}
