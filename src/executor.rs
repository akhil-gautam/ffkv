//! Frozen, paused command execution. Resolved effects commit together; ordinary
//! script failures retain preceding writes, while DEBUG YES uses an undo journal.
use crate::{
    engine::{BoundCommand, Engine, Execution, ReadyKey, Routing},
    journal::{Batch, Mutation},
    persistence::Persistence,
    pressure::Admission,
    protocol::{Resp, Version},
    request::Origin,
    scripting::{InvocationContext, ScriptRunner},
    session::ClientSession,
    sharding::Paused,
    state::State,
};
use bytes::Bytes;
use std::{io, sync::Arc};

type AtomicEffects = (
    Option<Batch>,
    Vec<ReadyKey>,
    Vec<(crate::pubsub::Publication, Vec<Bytes>)>,
    std::collections::HashSet<u32>,
);

pub struct Atomic<'a> {
    paused: &'a mut Paused,
    persistence: &'a Persistence,
    now: i64,
    recording: bool,
    batch: Option<Batch>,
    ready: Vec<ReadyKey>,
    user: Option<Arc<crate::auth::User>>,
    session: Option<Arc<ClientSession>>,
    version: Version,
    admission: Admission<'a>,
    pubsub: &'a Arc<crate::pubsub::Hub>,
    publications: Vec<(crate::pubsub::Publication, Vec<Bytes>)>,
    invocation: Option<InvocationContext>,
    control: Option<Arc<crate::scripting::ExecutionControl>>,
    cluster: bool,
    cluster_client: crate::cluster::ClusterClient,
    stale: bool,
    script_slot: Option<u16>,
    visible_slots: Option<Vec<bool>>,
    undo_keys: std::collections::HashSet<ReadyKey>,
    undo_batches: Vec<Batch>,
    cpu_nested: bool,
    server: Option<Arc<crate::server_commands::ServerRuntime>>,
    in_transaction: bool,
    module_readonly: bool,
    module_depth: usize,
    state: Option<&'a State>,
    swapped_databases: std::collections::HashSet<u32>,
    debug_swaps: Vec<(u32, u32)>,
}
/// A CPU window charged to its session when dropped, measured only while
/// anything may report it (`State::cpu_observed`).
struct CpuCharge {
    session: Option<Arc<ClientSession>>,
    start: Option<std::time::Duration>,
}
impl CpuCharge {
    fn new(session: Option<Arc<ClientSession>>, observed: bool) -> Self {
        let start = ClientSession::cpu_window(session.as_deref(), observed);
        Self { session, start }
    }
}
impl Drop for CpuCharge {
    fn drop(&mut self) {
        if let Some(session) = &self.session {
            session.charge_cpu(self.start);
        }
    }
}

fn record_swaps(batch: &Batch, databases: &mut std::collections::HashSet<u32>) {
    for mutation in &batch.mutations {
        if let Mutation::SwapDatabases { first, second } = mutation {
            databases.insert(*first);
            databases.insert(*second);
        }
    }
}

fn absorb(target: &mut Option<Batch>, source: Option<Batch>) {
    if let Some(source) = source {
        if let Some(target) = target {
            assert_eq!(
                target.timestamp_ms, source.timestamp_ms,
                "atomic command clock"
            );
            target.mutations.extend(source.mutations);
        } else {
            *target = Some(source);
        }
    }
}

fn capture_undo_key(
    engine: &mut Engine,
    key: &Bytes,
    now: i64,
    undo_keys: &mut std::collections::HashSet<ReadyKey>,
    undo_batches: &mut Vec<Batch>,
) -> io::Result<()> {
    if !undo_keys.insert((engine.database(), key.clone())) {
        return Ok(());
    }
    let mut snapshot = Engine::new();
    snapshot.inherit_runtime(engine);
    snapshot.set_global_metadata_owner(false);
    engine.transfer_keys(std::slice::from_ref(key), &mut snapshot);
    let batches = snapshot
        .snapshot_batches(now)
        .collect::<io::Result<Vec<_>>>();
    snapshot.distribute(std::slice::from_mut(engine));
    undo_batches.extend(batches?);
    Ok(())
}
impl<'a> Atomic<'a> {
    #[expect(
        clippy::too_many_arguments,
        reason = "One invocation borrows the paused image, durability, identity, admission, and publication context."
    )]
    pub fn new(
        paused: &'a mut Paused,
        persistence: &'a Persistence,
        now: i64,
        origin: Origin,
        user: Option<Arc<crate::auth::User>>,
        session: Option<Arc<ClientSession>>,
        admission: Admission<'a>,
        pubsub: &'a Arc<crate::pubsub::Hub>,
    ) -> Self {
        let recording = persistence.capturing();
        let database = session.as_ref().map_or(0, |session| session.database());
        paused.select_database(database);
        for engine in &mut paused.engines {
            engine.set_recording(recording);
            engine.set_reply_limit(origin.reply_limit());
        }
        Self {
            paused,
            persistence,
            now,
            recording,
            batch: None,
            ready: Vec::new(),
            user,
            session,
            version: origin.version(),
            admission,
            pubsub,
            publications: Vec::new(),
            invocation: None,
            control: None,
            cluster: false,
            cluster_client: crate::cluster::ClusterClient::default(),
            stale: false,
            script_slot: None,
            visible_slots: None,
            undo_keys: std::collections::HashSet::new(),
            undo_batches: Vec::new(),
            cpu_nested: false,
            server: None,
            in_transaction: false,
            module_readonly: false,
            module_depth: 0,
            state: None,
            swapped_databases: std::collections::HashSet::new(),
            debug_swaps: Vec::new(),
        }
    }
    pub fn set_environment(&mut self, state: &'a State) {
        self.cluster = state.cluster.get().is_some();
        self.stale = state.is_replica() && !state.replica_ready();
        self.server = state.server.get().cloned();
        self.state = Some(state);
    }
    pub fn set_visible_slots(&mut self, slots: Option<Vec<bool>>) {
        self.visible_slots = slots;
    }
    pub fn set_cluster_client(&mut self, client: crate::cluster::ClusterClient) {
        self.cluster_client = client;
    }
    /// Starts charging this invocation's session, measured only while
    /// anything may report it; without an environment, always.
    fn cpu_charge(&self) -> CpuCharge {
        CpuCharge::new(
            self.session.clone(),
            self.state.is_none_or(|state| state.cpu_observed()),
        )
    }

    fn admit_script_keys(&mut self, keys: &[Bytes], write: bool) -> Result<(), Resp> {
        let Some(invocation) = self.invocation.filter(|_| self.cluster) else {
            return Ok(());
        };
        if keys.is_empty() {
            return Ok(());
        }
        let mut slot = self.script_slot;
        if !invocation.flags.allow_cross_slot_keys {
            for key in keys {
                let next = crate::slot::key_slot(key);
                if slot.is_some_and(|previous| previous != next) {
                    return Err(Resp::Error(
                        "ERR Script attempted to access keys that do not hash to the same slot"
                            .into(),
                    ));
                }
                slot = Some(next);
            }
        }
        let state = self.state.expect("cluster execution environment");
        // Preserve the enclosing request's routing modes for every Lua call.
        // ASKING is consumed by the outer request, not by its first redis.call.
        let mut client = crate::cluster::ClusterClient {
            readonly: self.cluster_client.readonly,
            asking: self.cluster_client.asking,
        };
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(crate::native_runtime::admit_cluster_paused(
                keys,
                write,
                false,
                false,
                &mut client,
                self.paused,
                self.persistence,
                state,
            ))
        })?;
        self.script_slot = slot;
        Ok(())
    }
    fn audit_context(&self) -> &'static str {
        if self.invocation.is_some() {
            "lua"
        } else if self.module_depth > 0 {
            "module"
        } else if self.in_transaction {
            "multi"
        } else {
            "toplevel"
        }
    }
    fn audit(&self) -> Option<crate::auth::Audit<'_>> {
        let session = self.session.as_ref()?;
        Some(crate::auth::Audit {
            acl: session.acl.as_deref()?,
            clients: &self.state?.client_registry,
            client_id: session.client_id,
            context: self.audit_context(),
        })
    }
    /// Counted-key errors win over NOPERM, as for every nested command.
    fn authorize(&self, command: &mut BoundCommand<'_>) -> Result<(), Resp> {
        self.user.as_ref().map_or(Ok(()), |user| {
            command.check()?;
            command.authorize(user, self.audit().as_ref())
        })
    }

    fn capture_undo(&mut self, engine: &mut Engine, command: &BoundCommand<'_>) -> io::Result<()> {
        if !self.invocation.is_some_and(|context| context.rollback) {
            return Ok(());
        }
        let args = command.args();
        if command.is_readonly() && command.accessed_keys().is_empty() {
            return Ok(());
        }
        let selected = engine.database();
        let mut snapshot = Engine::new();
        snapshot.inherit_runtime(engine);
        snapshot.set_global_metadata_owner(false);
        let mut databases: Vec<_> = engine.database_ids().collect();
        let destination = if args[0].eq_ignore_ascii_case(b"MOVE") {
            args.get(2)
        } else if args[0].eq_ignore_ascii_case(b"COPY") {
            args.get(3..)
                .unwrap_or_default()
                .windows(2)
                .find(|pair| pair[0].eq_ignore_ascii_case(b"DB"))
                .map(|pair| &pair[1])
        } else {
            None
        };
        let destination = destination
            .and_then(|value| std::str::from_utf8(value).ok()?.parse::<u32>().ok())
            .filter(|database| *database < engine.database_count());
        if let Some(database) = destination
            && !databases.contains(&database)
        {
            databases.push(database);
        }
        for database in databases {
            if args[0].eq_ignore_ascii_case(b"FLUSHDB") && database != selected {
                continue;
            }
            engine.switch_database(database);
            snapshot.switch_database(database);
            let mut keys: Vec<_> = engine.live_keys(0).cloned().collect();
            if database == selected {
                keys.extend(command.accessed_keys());
            }
            if destination == Some(database) {
                keys.push(
                    args[if args[0].eq_ignore_ascii_case(b"MOVE") {
                        1
                    } else {
                        2
                    }]
                    .clone(),
                );
            }
            keys.retain(|key| self.undo_keys.insert((database, key.clone())));
            engine.transfer_keys(&keys, &mut snapshot);
        }
        let batches = snapshot
            .snapshot_batches(self.now)
            .collect::<io::Result<Vec<_>>>();
        snapshot.distribute(std::slice::from_mut(engine));
        engine.switch_database(selected);
        self.undo_batches.extend(batches?);
        Ok(())
    }

    fn rollback_data(&mut self) -> io::Result<()> {
        let keys: Vec<_> = self.undo_keys.drain().collect();
        let discarded = self.paused.take_database_keys(&keys, false);
        drop(discarded);
        let mut restored = Engine::new();
        restored.inherit_runtime(&self.paused.engines[0]);
        restored.set_recording(false);
        for batch in self.undo_batches.drain(..) {
            restored.replay(batch)?;
        }
        self.paused.restore(restored);
        for (first, second) in self.debug_swaps.drain(..).rev() {
            for engine in &mut self.paused.engines {
                engine.replay(Batch {
                    timestamp_ms: self.now,
                    mutations: vec![Mutation::SwapDatabases { first, second }],
                })?;
            }
        }
        Ok(())
    }

    fn visible_reply(&self, command: crate::engine::GlobalCommand, mut reply: Resp) -> Resp {
        let Some(slots) = &self.visible_slots else {
            return reply;
        };
        let visible = |key: &Bytes| {
            slots
                .get(crate::slot::key_slot(key) as usize)
                .copied()
                .unwrap_or(false)
        };
        match command {
            crate::engine::GlobalCommand::Dbsize => Resp::Integer(
                self.paused
                    .engines
                    .iter()
                    .map(|engine| {
                        engine
                            .live_keys(self.now)
                            .filter(|key| visible(key))
                            .count() as i64
                    })
                    .sum(),
            ),
            crate::engine::GlobalCommand::Keys => {
                if let Resp::Array(Some(keys)) = &mut reply {
                    keys.retain(|value| matches!(value,Resp::Bulk(Some(key)) if visible(key)));
                }
                reply
            }
            crate::engine::GlobalCommand::Scan => {
                if let Resp::Array(Some(fields)) = &mut reply
                    && let Some(Resp::Array(Some(keys))) = fields.get_mut(1)
                {
                    keys.retain(|value| matches!(value,Resp::Bulk(Some(key)) if visible(key)));
                }
                reply
            }
            crate::engine::GlobalCommand::Flush | crate::engine::GlobalCommand::RandomKey => reply,
        }
    }
    pub fn track_ready(&mut self, enabled: bool) {
        for engine in &mut self.paused.engines {
            engine.set_ready_tracking(enabled);
        }
    }
    fn context(&self, engine: &mut Engine, command: &BoundCommand<'_>) {
        engine.set_command_user(self.user.clone());
        engine.set_command_version(self.version);
        engine.set_command_session(self.session.clone());
        engine.set_command_name(command);
        engine.set_command_acl_context(self.audit_context());
    }
    fn collect(&mut self, engine: &mut Engine) {
        engine.finish_pending_search();
        self.ready.extend(engine.take_ready_keys());
        absorb(&mut self.batch, engine.take_batch());
        engine.set_command_session(None);
        engine.set_command_user(None);
        engine.set_command_version(Version::Resp2);
    }
    pub fn data(&mut self, args: &[Bytes]) -> io::Result<Resp> {
        let _cpu = (self.invocation.is_none() && !self.cpu_nested).then(|| self.cpu_charge());
        if let Some(prepared) = self
            .server
            .as_ref()
            .and_then(|server| server.extensions().prepare_sync(args))
        {
            let invocation = match prepared {
                Ok(invocation) => invocation,
                Err(reply) => return Ok(reply),
            };
            let previous = std::mem::replace(&mut self.cpu_nested, true);
            let reply = self.module(invocation);
            self.cpu_nested = previous;
            return reply;
        }
        let mut command = BoundCommand::bind(args);
        if let Err(reply) = command.check() {
            return Ok(reply);
        }
        if self.stale
            && !self
                .invocation
                .is_some_and(|context| context.flags.allow_stale)
            && !command.is_control()
        {
            return Ok(Resp::Error(
                "LOADING replica initial synchronization is not complete".into(),
            ));
        }
        if self.invocation.is_some() && !command.script_allowed() {
            return Ok(Resp::Error(
                "ERR command is not allowed from a script".into(),
            ));
        }
        if self.in_transaction && !command.transaction_allowed() {
            return Ok(Resp::Error(
                "ERR Command not allowed inside a transaction".into(),
            ));
        }
        let dataset_write = command.is_dataset_write();
        if dataset_write && self.module_readonly {
            return Ok(Resp::Error(
                "ERR write command called from a read-only module command".into(),
            ));
        }
        let writes = dataset_write
            || [b"PUBLISH".as_slice(), b"SPUBLISH", b"PFCOUNT"]
                .iter()
                .any(|name| args[0].eq_ignore_ascii_case(name));
        if self.admission.read_only && dataset_write {
            return Ok(Resp::Error(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        if let Err(reply) = self.authorize(&mut command) {
            return Ok(reply);
        }
        if self
            .invocation
            .is_some_and(|invocation| invocation.readonly && writes)
        {
            return Ok(Resp::Error(
                "ERR Write commands are not allowed from read-only scripts.".into(),
            ));
        }
        if self.cluster
            && self
                .invocation
                .is_some_and(|context| !context.flags.allow_cross_slot_keys)
            && command.any_accessed_key(|_| false)
        {
            return Ok(Resp::Error(
                "ERR Script attempted to access keys that do not hash to the same slot".into(),
            ));
        }
        if self.cluster
            && self.invocation.is_some()
            && let Err(reply) = self.admit_script_keys(&command.accessed_keys(), dataset_write)
        {
            return Ok(reply);
        }
        if !self
            .invocation
            .is_some_and(|context| context.flags.allow_oom || context.readonly)
            && let Some(reply) = self.admission.reject_pressure(&command)
        {
            return Ok(reply);
        }
        if args[0].eq_ignore_ascii_case(b"TS.READ")
            && args[3..]
                .iter()
                .any(|arg| arg.eq_ignore_ascii_case(b"BLOCK"))
        {
            return Ok(Resp::Error(
                "ERR TS.READ with BLOCK is not allowed inside MULTI or Lua".into(),
            ));
        }
        if writes
            && self
                .control
                .as_ref()
                .is_some_and(|control| !control.mark_write())
        {
            return Ok(Resp::Error(
                "ERR Script killed by user with SCRIPT KILL".into(),
            ));
        }
        if args[0].eq_ignore_ascii_case(b"PUBLISH") || args[0].eq_ignore_ascii_case(b"SPUBLISH") {
            let publication = match if args[0].eq_ignore_ascii_case(b"SPUBLISH") {
                self.pubsub.stage_shard(&args[1], &args[2])
            } else {
                self.pubsub.stage(&args[1], &args[2])
            } {
                Ok(publication) => publication,
                Err(reason) => return Ok(Resp::Error(reason.into())),
            };
            let count = publication.count();
            self.publications.push((publication, args.to_vec()));
            return Ok(Resp::Integer(count));
        }
        if args[0].eq_ignore_ascii_case(b"PUBSUB") {
            return Ok(self.pubsub.inspect(args));
        }
        let database = self
            .session
            .as_ref()
            .map_or(0, |session| session.database());
        self.paused.select_database(database);
        if args[0].eq_ignore_ascii_case(b"MIGRATE") {
            return self.migrate(&command);
        }
        if args[0].eq_ignore_ascii_case(b"SWAPDB") {
            return self.swap_databases(&mut command);
        }
        if args[0].eq_ignore_ascii_case(b"SORT") || args[0].eq_ignore_ascii_case(b"SORT_RO") {
            return self.sort(&mut command);
        }
        let execution = match command.routing(self.paused.engines.len()) {
            Routing::Local { shard, .. } => {
                if self.invocation.is_some_and(|context| context.rollback) {
                    let mut engine = self
                        .paused
                        .take_keys(&command.accessed_keys(), self.recording);
                    self.context(&mut engine, &command);
                    self.capture_undo(&mut engine, &command)?;
                    let execution = engine.execute_at_time(&mut command, self.now)?;
                    self.collect(&mut engine);
                    self.paused.restore(engine);
                    execution
                } else {
                    let audit_context = self.audit_context();
                    let engine = &mut self.paused.engines[shard];
                    engine.set_command_user(self.user.clone());
                    engine.set_command_version(self.version);
                    engine.set_command_session(self.session.clone());
                    engine.set_command_name(&command);
                    engine.set_command_acl_context(audit_context);
                    let execution = engine.execute_at_time(&mut command, self.now)?;
                    engine.finish_pending_search();
                    self.ready.extend(engine.take_ready_keys());
                    absorb(&mut self.batch, engine.take_batch());
                    engine.set_command_session(None);
                    engine.set_command_user(None);
                    engine.set_command_version(Version::Resp2);
                    execution
                }
            }
            Routing::Coordinated { mut keys, .. } => {
                keys.sort_unstable();
                keys.dedup();
                let mut engine = self.paused.take_keys(&keys, self.recording);
                self.context(&mut engine, &command);
                self.capture_undo(&mut engine, &command)?;
                let execution = engine.execute_at_time(&mut command, self.now)?;
                self.collect(&mut engine);
                self.paused.restore(engine);
                execution
            }
            Routing::All => {
                let search = args[0]
                    .get(..3)
                    .is_some_and(|name| name.eq_ignore_ascii_case(b"FT."));
                let mut engine = if search {
                    absorb(
                        &mut self.batch,
                        self.paused.reap_search(self.now, self.recording)?,
                    );
                    self.paused.select_database(database);
                    let mut routing = Engine::new();
                    routing.inherit_runtime(&self.paused.engines[0]);
                    self.context(&mut routing, &command);
                    let plan = match routing.search_prepare(args, self.now) {
                        Ok(plan) => plan,
                        Err(reply) => return Ok(reply),
                    };
                    let mut engine = match plan.source_keys() {
                        Some(keys) => self.paused.take_keys(keys, self.recording),
                        None => self.paused.take_all(self.recording),
                    };
                    engine.search_set_plan(plan);
                    engine
                } else {
                    self.paused.take_related(&command, self.recording, self.now)
                };
                self.context(&mut engine, &command);
                self.capture_undo(&mut engine, &command)?;
                let execution = engine.execute_at_time(&mut command, self.now)?;
                self.collect(&mut engine);
                self.paused.restore(engine);
                execution
            }
            Routing::Global(global) => {
                if self.invocation.is_some_and(|context| context.rollback) && !command.is_readonly()
                {
                    let mut engine = self.paused.take_all(self.recording);
                    self.capture_undo(&mut engine, &command)?;
                    self.paused.restore(engine);
                    self.paused.select_database(database);
                }
                for engine in &mut self.paused.engines {
                    engine.set_command_user(self.user.clone());
                    engine.set_command_version(self.version);
                    engine.set_command_session(self.session.clone());
                }
                let (reply, batch) = if matches!(global, crate::engine::GlobalCommand::RandomKey) {
                    (
                        self.paused
                            .random_key(self.now, self.visible_slots.as_deref()),
                        None,
                    )
                } else {
                    self.paused.collect_global(global, &mut command, self.now)?
                };
                absorb(&mut self.batch, batch);
                for engine in &mut self.paused.engines {
                    self.ready.extend(engine.take_ready_keys());
                    engine.set_command_session(None);
                    engine.set_command_user(None);
                    engine.set_command_version(Version::Resp2);
                }
                self.persistence
                    .note_reads(&command, self.session.as_deref());
                return Ok(self.visible_reply(global, reply));
            }
            Routing::Maintenance => {
                let Some(state) = self.state else {
                    return Ok(Resp::Error(
                        "ERR command requires a server execution context".into(),
                    ));
                };
                return tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(self.server_command(args, state))
                });
            }
            Routing::Scripting => {
                return Ok(Resp::Error(
                    "ERR nested scripting command is not allowed".into(),
                ));
            }
        };
        self.persistence
            .note_reads(&command, self.session.as_deref());
        Ok(match execution {
            Execution::Reply(reply) => reply,
            Execution::Pushed { reply, key } => {
                self.ready.push((database, key));
                reply
            }
            Execution::Block(block) => block.null_reply(),
            Execution::StreamBlock(_) => Resp::Array(None),
        })
    }

    fn sort(&mut self, command: &mut BoundCommand<'_>) -> io::Result<Resp> {
        let args = command.args();
        let mut keys = command.accessed_keys();
        keys.sort_unstable();
        keys.dedup();
        let mut engine = self.paused.take_keys(&keys, self.recording);
        self.context(&mut engine, command);
        if let Err(error) = self.capture_undo(&mut engine, command) {
            self.collect(&mut engine);
            self.paused.restore(engine);
            return Err(error);
        }
        let user = self.user.clone();
        let session = self.session.clone();
        let version = self.version;
        let audit_context = self.audit_context();
        let recording = self.recording;
        let rollback = self.invocation.is_some_and(|context| context.rollback);
        let tracking = args[0].eq_ignore_ascii_case(b"SORT_RO");
        let clients = self.state.map(|state| &state.client_registry);
        let now = self.now;
        let paused = &mut *self.paused;
        let ready = &mut self.ready;
        let batch = &mut self.batch;
        let undo_keys = &mut self.undo_keys;
        let undo_batches = &mut self.undo_batches;
        let mut failure = None;
        // Lookups name the SORT itself while `command` stays borrowed by it.
        let named = command.clone();
        let result = engine.execute_sort_with_lookup(command, now, |key, field, at| {
            let shard = Engine::shard_for(key, paused.engines.len());
            let owner = &mut paused.engines[shard];
            owner.set_recording(recording);
            owner.set_command_user(user.clone());
            owner.set_command_version(version);
            owner.set_command_session(session.clone());
            owner.set_command_name(&named);
            owner.set_command_acl_context(audit_context);
            let value = if rollback {
                capture_undo_key(owner, key, at, undo_keys, undo_batches)
                    .and_then(|()| owner.sort_lookup_value(key, field, at))
            } else {
                owner.sort_lookup_value(key, field, at)
            };
            owner.finish_pending_search();
            ready.extend(owner.take_ready_keys());
            absorb(batch, owner.take_batch());
            owner.set_command_session(None);
            owner.set_command_user(None);
            owner.set_command_version(Version::Resp2);
            if tracking && let (Some(clients), Some(session)) = (clients, session.as_ref()) {
                clients.read(
                    session.client_id,
                    session.database(),
                    std::slice::from_ref(key),
                );
            }
            match value {
                Ok(value) => value,
                Err(error) => {
                    failure = Some(error);
                    Err(Resp::Error("IOERR SORT lookup failed".into()))
                }
            }
        });
        self.collect(&mut engine);
        self.paused.restore(engine);
        self.persistence
            .note_reads(command, self.session.as_deref());
        if let Some(error) = failure {
            return Err(error);
        }
        result
    }
    pub fn script(&mut self, args: &[Bytes], scripts: &mut ScriptRunner) -> io::Result<Resp> {
        let _cpu = self.cpu_charge();
        let mut command = BoundCommand::bind(args);
        if let Err(reply) = command.check() {
            return Ok(reply);
        }
        if let Err(reply) = self.authorize(&mut command) {
            return Ok(reply);
        }
        if args[0].eq_ignore_ascii_case(b"SCRIPT")
            && args
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"DEBUG"))
        {
            let Some(mode) = args
                .get(2)
                .and_then(|value| ScriptRunner::parse_debug_mode(value))
            else {
                return Ok(Resp::Error("ERR Use SCRIPT DEBUG YES/SYNC/NO".into()));
            };
            let Some(session) = &self.session else {
                return Ok(Resp::Error(
                    "ERR SCRIPT DEBUG requires a stateful client context".into(),
                ));
            };
            session.script_debug_mode.store(
                match mode {
                    crate::scripting::DebugMode::No => 0,
                    crate::scripting::DebugMode::Yes => 1,
                    crate::scripting::DebugMode::Sync => 2,
                },
                std::sync::atomic::Ordering::Release,
            );
            return Ok(Resp::Simple("OK"));
        }
        if args[0].eq_ignore_ascii_case(b"SCRIPT") || args[0].eq_ignore_ascii_case(b"FUNCTION") {
            if let Some(reply) = self.admission.reject(&command) {
                return Ok(reply);
            }
            if let Some(reply) = scripts.control(args) {
                return Ok(reply);
            }
        }
        scripts.set_debug_mode(
            match self.session.as_ref().map_or(0, |session| {
                session
                    .script_debug_mode
                    .load(std::sync::atomic::Ordering::Acquire)
            }) {
                1 => crate::scripting::DebugMode::Yes,
                2 => crate::scripting::DebugMode::Sync,
                _ => crate::scripting::DebugMode::No,
            },
        );
        let context = match scripts.invocation_context(args) {
            Ok(context) => context,
            Err(reply) => return Ok(reply),
        };
        if self.admission.read_only && !context.readonly {
            return Ok(Resp::Error(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        if self.stale && !context.flags.allow_stale {
            return Ok(Resp::Error(
                "MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'."
                    .into(),
            ));
        }
        if self.cluster && context.flags.no_cluster {
            return Ok(Resp::Error(
                "ERR Script with no-cluster flag is not allowed in cluster mode.".into(),
            ));
        }
        if !context.flags.allow_oom
            && !context.readonly
            && let Some(reply) = self.admission.reject_pressure(&command)
        {
            return Ok(reply);
        }
        let session = self
            .session
            .as_ref()
            .map_or_else(|| ClientSession::new(0), |session| session.script_child());
        let previous_session = self.session.replace(Arc::new(session));
        let previous_context = self.invocation.replace(context);
        self.control = Some(scripts.execution_control());
        self.script_slot = None;
        for key in command.accessed_keys() {
            let slot = crate::slot::key_slot(&key);
            if self.cluster && self.script_slot.is_some_and(|previous| previous != slot) {
                self.session = previous_session;
                self.invocation = previous_context;
                self.control = None;
                return Ok(Resp::Error(
                    "CROSSSLOT Keys in request don't hash to the same slot".into(),
                ));
            }
            self.script_slot = Some(slot);
        }
        let search_rollback = if context.rollback {
            for engine in &mut self.paused.engines {
                engine.set_watch_notifications(false);
            }
            Some(crate::engine::SearchRollback::begin(
                &mut self.paused.engines,
            )?)
        } else {
            None
        };
        let saved_batch = context.rollback.then(|| self.batch.take()).flatten();
        let saved_ready = self.ready.len();
        let saved_publications = self.publications.len();
        let mut calls = 0usize;
        let mut bytes = 0usize;
        scripts.execution_control().set_owner_client_scope(
            self.session.as_ref().map_or(0, |session| session.client_id),
            self.session
                .as_ref()
                .map(|session| session.disconnected.clone()),
        );
        let result = scripts.eval(args, |command, version, invocation| {
            calls += 1;
            let size = match crate::protocol::validate_command(command) {
                Ok(size) => size,
                Err(error) => return Ok(Resp::Error(format!("ERR {error}"))),
            };
            bytes = bytes.saturating_add(size);
            if calls > 10_000 || bytes > 16 * 1024 * 1024 {
                return Ok(Resp::Error(
                    "ERR script command/value budget exceeded".into(),
                ));
            }
            self.invocation = Some(invocation);
            let previous = std::mem::replace(&mut self.version, version);
            let result = self.data(command);
            self.version = previous;
            result
        });
        if context.rollback {
            self.rollback_data()?;
            self.batch = saved_batch;
            self.ready.truncate(saved_ready);
            self.publications.truncate(saved_publications);
        }
        if let Some(rollback) = search_rollback {
            for engine in &mut self.paused.engines {
                engine.finish_pending_search();
                let _ = engine.take_batch();
                let _ = engine.take_ready_keys();
            }
            rollback.rollback(&mut self.paused.engines)?;
            for engine in &mut self.paused.engines {
                engine.set_watch_notifications(true);
            }
        }
        self.session = previous_session;
        self.invocation = previous_context;
        self.control = None;
        result
    }
    pub fn module(
        &mut self,
        invocation: crate::server_commands::extensions::Invocation,
    ) -> io::Result<Resp> {
        if invocation.is_write() && self.module_readonly {
            return Ok(Resp::Error(
                "ERR write command called from a read-only module command".into(),
            ));
        }
        if self.invocation.is_some() && !invocation.script_allowed() {
            return Ok(Resp::Error(
                "ERR command is not allowed from a script".into(),
            ));
        }
        if self.in_transaction && !invocation.transaction_allowed() {
            return Ok(Resp::Error(
                "ERR Command not allowed inside a transaction".into(),
            ));
        }
        if let Some(user) = &self.user
            && user
                .authorize_logged(
                    invocation.name(),
                    invocation.categories(),
                    invocation.keys(),
                    invocation.global_keyspace(),
                    self.audit().as_ref(),
                )
                .is_err()
        {
            return Ok(Resp::Error(
                "NOPERM this user has no permissions to run this command or access its keys".into(),
            ));
        }
        if self.cluster
            && self.invocation.is_some()
            && let Err(reply) = self.admit_script_keys(invocation.keys(), invocation.is_write())
        {
            return Ok(reply);
        }
        if invocation.is_write() && self.invocation.is_some_and(|context| context.readonly) {
            return Ok(Resp::Error(
                "ERR Write commands are not allowed from read-only scripts.".into(),
            ));
        }
        if invocation.flags().contains(&"denyoom")
            && self.admission.metrics.rss_pressure().rss_pressured
            && !self
                .invocation
                .is_some_and(|context| context.flags.allow_oom || context.readonly)
        {
            return Ok(Resp::Error(
                "OOM command not allowed when used memory > 'maxmemory'.".into(),
            ));
        }
        if invocation.is_write()
            && self
                .control
                .as_ref()
                .is_some_and(|control| !control.mark_write())
        {
            return Ok(Resp::Error(
                "ERR Script killed by user with SCRIPT KILL".into(),
            ));
        }
        if invocation.is_write() && self.admission.read_only {
            return Ok(Resp::Error(
                "READONLY You can't write against a read only replica.".into(),
            ));
        }
        let _cpu = (!self.cpu_nested && self.invocation.is_none()).then(|| self.cpu_charge());
        let previous_cpu_nested = std::mem::replace(&mut self.cpu_nested, true);
        let previous_readonly = self.module_readonly;
        self.module_readonly |= !invocation.is_write();
        self.module_depth += 1;
        let mut failure = None;
        let reply = invocation.execute(|args| match self.data(args) {
            Ok(reply) => reply,
            Err(error) => {
                failure = Some(error);
                Resp::Error("IOERR module command failed".into())
            }
        });
        self.module_depth -= 1;
        self.cpu_nested = previous_cpu_nested;
        self.module_readonly = previous_readonly;
        if let Some(error) = failure {
            Err(error)
        } else {
            Ok(reply)
        }
    }

    fn swap_databases(&mut self, command: &mut BoundCommand<'_>) -> io::Result<Resp> {
        let args = command.args();
        let debug = self.invocation.is_some_and(|context| context.rollback);
        let audit_context = self.audit_context();
        for (index, engine) in self.paused.engines.iter_mut().enumerate() {
            engine.set_command_user(self.user.clone());
            engine.set_command_version(self.version);
            engine.set_command_session(self.session.clone());
            engine.set_command_name(command);
            engine.set_command_acl_context(audit_context);
            engine.set_recording(self.recording && index == 0);
            if index != 0 {
                engine.set_watch_notifications(false);
            }
            engine.set_search_reindexing(false);
            let execution = engine.execute_at_time(command, self.now);
            if execution.is_err() {
                engine.set_search_reindexing(true);
            }
            let execution = execution?;
            engine.finish_pending_search();
            self.ready.extend(engine.take_ready_keys());
            absorb(&mut self.batch, engine.take_batch());
            engine.set_search_reindexing(true);
            engine.set_command_session(None);
            engine.set_command_user(None);
            engine.set_command_version(Version::Resp2);
            engine.set_recording(self.recording);
            engine.set_watch_notifications(!debug);
            match execution {
                Execution::Reply(Resp::Error(error)) => return Ok(Resp::Error(error)),
                Execution::Reply(_) => {}
                _ => return Err(io::Error::other("invalid database swap execution")),
            }
        }
        let parse = |value: &Bytes| {
            std::str::from_utf8(value)
                .ok()
                .and_then(|value| value.parse::<u32>().ok())
                .expect("accepted SWAPDB database")
        };
        let (first, second) = (parse(&args[1]), parse(&args[2]));
        if first != second {
            if debug {
                let remap = |database: u32| {
                    if database == first {
                        second
                    } else if database == second {
                        first
                    } else {
                        database
                    }
                };
                let previous = std::mem::take(&mut self.undo_keys);
                self.undo_keys = previous
                    .into_iter()
                    .map(|(database, key)| (remap(database), key))
                    .collect();
                for batch in &mut self.undo_batches {
                    for mutation in &mut batch.mutations {
                        if let Mutation::SelectDatabase { database } = mutation {
                            *database = remap(*database);
                        }
                    }
                }
                self.debug_swaps.push((first, second));
            } else {
                self.swapped_databases.insert(first);
                self.swapped_databases.insert(second);
            }
            let effects = Engine::search_reconcile_swapped(
                &mut self.paused.engines,
                first,
                second,
                self.now,
            )?;
            if !effects.is_empty() {
                absorb(
                    &mut self.batch,
                    Some(Batch {
                        timestamp_ms: self.now,
                        mutations: effects
                            .into_iter()
                            .map(|effect| Mutation::Search { effect })
                            .collect(),
                    }),
                );
            }
        }
        Ok(Resp::Simple("OK"))
    }

    fn migrate(&mut self, command: &BoundCommand<'_>) -> io::Result<Resp> {
        let args = command.args();
        let options = match crate::migrate::parse(args) {
            Ok(options) => options,
            Err(reply) => return Ok(reply),
        };
        let mut engine = self.paused.take_keys(&options.keys, self.recording);
        self.context(&mut engine, command);
        self.capture_undo(&mut engine, command)?;
        let items = match engine.prepare_migrate(&options.keys, self.now) {
            Ok(items) => items,
            Err(reply) => {
                if let Some(error) = engine.take_failure() {
                    return Err(error);
                }
                self.collect(&mut engine);
                self.paused.restore(engine);
                return Ok(reply);
            }
        };
        let transfer = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(crate::migrate::transfer(&options, &items))
        });
        let mut reply = transfer.reply;
        if !options.copy {
            let acknowledged = engine.ack_migrate(&items, &transfer.acknowledged, self.now);
            if matches!(acknowledged, Resp::Error(_)) {
                reply = acknowledged;
            }
        }
        if let Some(error) = engine.take_failure() {
            return Err(error);
        }
        self.collect(&mut engine);
        self.paused.restore(engine);
        Ok(reply)
    }

    pub async fn server_command(&mut self, args: &[Bytes], state: &State) -> io::Result<Resp> {
        let mut command = BoundCommand::bind(args);
        if let Err(reply) = command.check() {
            return Ok(reply);
        }
        if self.in_transaction && !command.transaction_allowed() {
            return Ok(Resp::Error(
                "ERR Command not allowed inside a transaction".into(),
            ));
        }
        if args[0].eq_ignore_ascii_case(b"MIGRATE") {
            return self.data(args);
        }
        if let Err(reply) = self.authorize(&mut command) {
            return Ok(reply);
        }
        let cluster_command = args[0].eq_ignore_ascii_case(b"CLUSTER");
        if cluster_command || crate::server_runtime::requires_commit(args) {
            self.commit_prefix().await?;
        }
        self.paused.select_database(
            self.session
                .as_ref()
                .map_or(0, |session| session.database()),
        );
        let audit_context = self.audit_context();
        for engine in &mut self.paused.engines {
            engine.set_command_user(self.user.clone());
            engine.set_command_version(self.version);
            engine.set_command_session(self.session.clone());
            engine.set_command_name(&command);
            engine.set_command_acl_context(audit_context);
        }
        let result = if cluster_command {
            let offset = self.session.as_ref().map_or(0, |session| {
                session
                    .last_write_offset
                    .load(std::sync::atomic::Ordering::Acquire)
            });
            crate::native_runtime::execute_cluster_paused(
                args,
                self.paused,
                self.persistence,
                state,
                offset,
            )
            .await?
        } else {
            crate::server_runtime::execute_paused(&command, self.paused, self.persistence, state)
                .await?
        };
        for engine in &mut self.paused.engines {
            engine.finish_pending_search();
            self.ready.extend(engine.take_ready_keys());
            absorb(&mut self.batch, engine.take_batch());
            engine.set_command_session(None);
            engine.set_command_user(None);
            engine.set_command_version(Version::Resp2);
        }
        self.recording = self.persistence.capturing();
        for engine in &mut self.paused.engines {
            engine.set_recording(self.recording);
        }
        self.persistence
            .note_reads(&command, self.session.as_deref());
        self.admission.read_only = state.is_replica();
        self.stale = state.is_replica() && !state.replica_ready();
        if cluster_command {
            self.visible_slots = if let Some(cluster) = state.cluster.get() {
                Some(cluster.visible_slots().await)
            } else {
                None
            };
        }
        match result {
            Some(reply) => Ok(reply),
            None if args[0].eq_ignore_ascii_case(b"INFO") => Ok(state.info(&args[1..])),
            None => crate::admin::execute(self.paused, self.persistence, state, args).await,
        }
    }

    async fn commit_prefix(&mut self) -> io::Result<()> {
        if self.invocation.is_some_and(|context| context.rollback) {
            return Ok(());
        }
        if let Some(batch) = self.batch.take() {
            record_swaps(&batch, &mut self.swapped_databases);
            let offset = self
                .persistence
                .commit_batch_offset(
                    &batch,
                    self.session.as_ref().map(|session| session.client_id),
                )
                .await?;
            if let Some(session) = &self.session {
                session
                    .last_write_offset
                    .store(offset, std::sync::atomic::Ordering::Release);
            }
        }
        Ok(())
    }

    pub async fn transaction(
        &mut self,
        commands: &[Vec<Bytes>],
        modules: Vec<(usize, crate::server_commands::extensions::Invocation)>,
        scripts: &mut ScriptRunner,
        state: &State,
    ) -> io::Result<Resp> {
        self.in_transaction = true;
        let mut replies = Vec::with_capacity(commands.len());
        let mut modules = modules.into_iter().peekable();
        for (index, args) in commands.iter().enumerate() {
            let revision = scripts.function_revision();
            let reply = if modules.peek().is_some_and(|(command, _)| *command == index) {
                self.module(modules.next().expect("matched prepared module").1)?
            } else if args[0].eq_ignore_ascii_case(b"UNWATCH") {
                Resp::Simple("OK")
            } else if args[0].eq_ignore_ascii_case(b"INFO") {
                self.server_command(args, state).await?
            } else if args[0].eq_ignore_ascii_case(b"WAIT")
                || args[0].eq_ignore_ascii_case(b"WAITAOF")
            {
                let mut command = BoundCommand::bind(args);
                if let Err(reply) = command.check() {
                    reply
                } else if let Err(reply) = self.authorize(&mut command) {
                    reply
                } else if let Some(native) = state.native.get() {
                    let offset = self.session.as_ref().map_or(0, |session| {
                        session
                            .last_write_offset
                            .load(std::sync::atomic::Ordering::Acquire)
                    });
                    match native
                        .handle_command(
                            args,
                            &mut crate::native_replication::ClientState::default(),
                            offset,
                            true,
                            self.cluster,
                            None,
                        )
                        .await
                    {
                        crate::native_replication::CommandResult::Reply(reply) => reply,
                        _ => Resp::Error("ERR invalid replication transaction response".into()),
                    }
                } else {
                    Resp::Error("ERR native replication runtime is unavailable".into())
                }
            } else if [b"REPLICAOF".as_slice(), b"SLAVEOF", b"FAILOVER"]
                .iter()
                .any(|name| args[0].eq_ignore_ascii_case(name))
            {
                let mut command = BoundCommand::bind(args);
                if let Err(reply) = command.check() {
                    reply
                } else if let Err(reply) = self.authorize(&mut command) {
                    reply
                } else {
                    self.commit_prefix().await?;
                    self.paused.select_database(
                        self.session
                            .as_ref()
                            .map_or(0, |session| session.database()),
                    );
                    let offset = self.session.as_ref().map_or(0, |session| {
                        session
                            .last_write_offset
                            .load(std::sync::atomic::Ordering::Acquire)
                    });
                    let reply = crate::native_runtime::execute_paused(
                        args,
                        self.paused,
                        self.persistence,
                        state,
                        offset,
                    )
                    .await?
                    .ok_or_else(|| {
                        io::Error::other("native transaction command was not handled")
                    })?;
                    self.admission.read_only = state.is_replica();
                    self.stale = state.is_replica() && !state.replica_ready();
                    reply
                }
            } else {
                match BoundCommand::bind(args).routing(1) {
                    Routing::Scripting => self.script(args, scripts)?,
                    Routing::Maintenance => self.server_command(args, state).await?,
                    _ => self.data(args)?,
                }
            };
            self.functions_changed(revision, scripts);
            replies.push(reply);
        }
        self.in_transaction = false;
        Ok(Resp::Array(Some(replies)))
    }
    pub fn functions_changed(&mut self, revision: u64, scripts: &ScriptRunner) {
        if revision == scripts.function_revision() {
            return;
        }
        let snapshot = scripts.function_snapshot();
        for engine in &mut self.paused.engines {
            engine.set_function_snapshot(snapshot.clone());
        }
        absorb(
            &mut self.batch,
            Some(Batch {
                timestamp_ms: self.now,
                mutations: vec![Mutation::Functions { snapshot }],
            }),
        );
    }
    pub fn finish(mut self, persistent: bool) -> AtomicEffects {
        if let Some(batch) = &self.batch {
            record_swaps(batch, &mut self.swapped_databases);
        }
        for engine in &mut self.paused.engines {
            engine.set_recording(persistent);
            engine.set_ready_tracking(false);
            engine.set_command_session(None);
            engine.set_command_user(None);
            engine.set_command_version(Version::Resp2);
        }
        (
            self.batch,
            self.ready,
            self.publications,
            self.swapped_databases,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect()
    }

    #[tokio::test]
    async fn lua_hash_import_definitions_are_invocation_local_but_exec_uses_the_client_session() {
        let persistence = Arc::new(Persistence::new(None));
        let (pool, mut workers) = crate::sharding::Pool::start(
            Engine::new(),
            2,
            persistence.clone(),
            Arc::new(State::new(0, 2, false)),
        );
        let state = State::new(0, 2, false);
        let session = Arc::new(ClientSession::new(51));
        session.set_database(1);
        let mut scripts = ScriptRunner::new().unwrap();
        let mut paused = pool.pause().await.unwrap();
        {
            let mut executor = Atomic::new(
                &mut paused,
                &persistence,
                Engine::now(),
                Origin::Client(Version::Resp2),
                None,
                Some(session),
                state.admission(),
                &state.pubsub,
            );
            let transaction = vec![
                args(&["HIMPORT", "PREPARE", "client", "field"]),
                args(&["HIMPORT", "SET", "outside", "client", "saved"]),
            ];
            assert!(
                matches!(executor.transaction(&transaction,Vec::new(),&mut scripts,&state).await.unwrap(),Resp::Array(Some(replies)) if replies.iter().all(|reply|*reply==Resp::Simple("OK")))
            );
            let script = args(&[
                "EVAL",
                "redis.call('HIMPORT','PREPARE','local','field');redis.call('HIMPORT','SET','inside','local','value');return redis.call('HGET','inside','field')",
                "0",
            ]);
            assert_eq!(
                executor.script(&script, &mut scripts).unwrap(),
                Resp::Bulk(Some(Bytes::from_static(b"value")))
            );
            assert!(matches!(
                executor
                    .data(&args(&["HIMPORT", "SET", "outside", "local", "leaked"]))
                    .unwrap(),
                Resp::Error(_)
            ));
            let second = args(&[
                "EVAL",
                "return redis.call('HIMPORT','SET','inside','local','leaked')",
                "0",
            ]);
            assert!(matches!(
                executor.script(&second, &mut scripts).unwrap(),
                Resp::Error(_)
            ));
            assert_eq!(
                executor
                    .data(&args(&["HIMPORT", "SET", "outside", "client", "retained"]))
                    .unwrap(),
                Resp::Simple("OK")
            );
            assert_eq!(
                executor.data(&args(&["HGET", "inside", "field"])).unwrap(),
                Resp::Bulk(Some(Bytes::from_static(b"value")))
            );
            let selected = args(&[
                "EVAL",
                "redis.call('SELECT',0);redis.call('SET','script-db','zero');return redis.call('GET','script-db')",
                "0",
            ]);
            assert_eq!(
                executor.script(&selected, &mut scripts).unwrap(),
                Resp::Bulk(Some(Bytes::from_static(b"zero")))
            );
            assert_eq!(
                executor.data(&args(&["GET", "script-db"])).unwrap(),
                Resp::Bulk(None)
            );
            assert_eq!(
                executor.data(&args(&["SELECT", "0"])).unwrap(),
                Resp::Simple("OK")
            );
            assert_eq!(
                executor.data(&args(&["GET", "script-db"])).unwrap(),
                Resp::Bulk(Some(Bytes::from_static(b"zero")))
            );
            let _ = executor.finish(false);
        }
        paused.resume().unwrap();
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }
}
