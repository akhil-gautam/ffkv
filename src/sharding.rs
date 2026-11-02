//! Single-key commands own one shard. Coordinated commands pause shard actors
//! behind a write barrier and move only the affected entries, not their payloads.

use std::{collections::HashSet, io, sync::Arc, time::Duration};

use bytes::Bytes;
use rand::Rng;
use tokio::{
    sync::{OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock, mpsc, oneshot},
    task::JoinSet,
};

use crate::{
    engine::{Engine, Execution, GlobalCommand},
    journal::Batch,
    persistence::Persistence,
    protocol::Resp,
    request::CommandRequest,
};

struct Shard {
    sender: mpsc::Sender<ShardRequest>,
}

enum ShardRequest {
    Execute {
        args: Vec<Bytes>,
        reply_limit: usize,
        user: Option<Arc<crate::auth::User>>,
        version: crate::protocol::Version,
        session: Option<Arc<crate::session::ClientSession>>,
        reply: oneshot::Sender<io::Result<Execution>>,
    },
    Pause {
        reply: oneshot::Sender<Engine>,
        resume: oneshot::Receiver<Engine>,
    },
}

pub struct Pool {
    shards: Vec<Shard>,
    gate: Arc<RwLock<()>>,
    persistence: Arc<Persistence>,
}

pub struct Paused {
    pub engines: Vec<Engine>,
    resumes: Vec<oneshot::Sender<Engine>>,
    _guard: OwnedRwLockWriteGuard<()>,
}

fn stopped() -> io::Error {
    io::Error::other("shard actor stopped")
}
fn invalid_reply() -> io::Error {
    io::Error::other("invalid internal shard response")
}

impl Pool {
    pub fn start(
        mut engine: Engine,
        count: usize,
        persistence: Arc<Persistence>,
    ) -> (Arc<Self>, JoinSet<io::Result<()>>) {
        let mut workers = JoinSet::new();
        let mut shards = Vec::with_capacity(count);
        engine.set_watches(persistence.watches.clone());
        for engine in engine.partition(count) {
            let (sender, receiver) = mpsc::channel(1024);
            let persistence = persistence.clone();
            let runtime = tokio::runtime::Handle::current();
            workers.spawn_blocking(move || {
                runtime.block_on(run_worker(engine, receiver, persistence))
            });
            shards.push(Shard { sender });
        }
        (
            Arc::new(Self {
                shards,
                gate: Arc::new(RwLock::new(())),
                persistence,
            }),
            workers,
        )
    }

    pub async fn admit(&self) -> io::Result<OwnedRwLockReadGuard<()>> {
        let guard = self.gate.clone().read_owned().await;
        self.persistence.ensure_healthy()?;
        Ok(guard)
    }

    pub async fn execute(
        &self,
        shard: usize,
        request: CommandRequest,
        _guard: OwnedRwLockReadGuard<()>,
    ) -> io::Result<()> {
        let (reply, response) = oneshot::channel();
        self.shards[shard]
            .sender
            .send(ShardRequest::Execute {
                args: request.args,
                reply_limit: request.origin.reply_limit(),
                user: request.user,
                version: request.origin.version(),
                session: request.session,
                reply,
            })
            .await
            .map_err(|_| stopped())?;
        let execution = response.await.map_err(|_| stopped())??;
        let reply = match execution {
            Execution::Reply(reply) | Execution::Pushed { reply, .. } => reply,
            Execution::Block(_) | Execution::StreamBlock(_) => {
                return Err(io::Error::other(
                    "blocking operation escaped coordinator routing",
                ));
            }
        };
        let _ = request.reply.send(reply);
        Ok(())
    }

    pub async fn pause(&self) -> io::Result<Paused> {
        let guard = self.gate.clone().write_owned().await;
        self.persistence.ensure_healthy()?;
        let mut responses = Vec::with_capacity(self.shards.len());
        let mut resumes = Vec::with_capacity(self.shards.len());
        for shard in &self.shards {
            let (reply, response) = oneshot::channel();
            let (resume, receiver) = oneshot::channel();
            shard
                .sender
                .send(ShardRequest::Pause {
                    reply,
                    resume: receiver,
                })
                .await
                .map_err(|_| stopped())?;
            responses.push(response);
            resumes.push(resume);
        }
        let mut engines = Vec::with_capacity(responses.len());
        for response in responses {
            engines.push(response.await.map_err(|_| stopped())?);
        }
        Ok(Paused {
            engines,
            resumes,
            _guard: guard,
        })
    }
}

impl Paused {
    pub fn select_database(&mut self, database: u32) {
        for engine in &mut self.engines {
            engine.switch_database(database);
        }
    }

    /// Gather precisely the requested logical-database keys by moving entries.
    pub fn take_database_keys(
        &mut self,
        keys: &[crate::engine::ReadyKey],
        journal: bool,
    ) -> Engine {
        let mut engine = Engine::new();
        engine.inherit_runtime(&self.engines[0]);
        engine.set_recording(journal);
        let selected = self.engines[0].database();
        for (database, key) in keys {
            let shard = Engine::shard_for(key, self.engines.len());
            self.engines[shard].switch_database(*database);
            engine.switch_database(*database);
            self.engines[shard].transfer_keys(std::slice::from_ref(key), &mut engine);
        }
        self.select_database(selected);
        engine.switch_database(selected);
        engine
    }

    pub fn reap_search(&mut self, now: i64, journal: bool) -> io::Result<Option<Batch>> {
        let keys = self.engines[0].search_due_keys(now);
        let selected = self.engines[0].database();
        if keys.is_empty() {
            // Cursor expiry and empty temporary indexes still need reaping.
            self.engines[0].switch_database(0);
            self.engines[0].set_recording(journal);
            self.engines[0].search_reap(now);
            self.engines[0].finish_pending_search();
            let batch = self.engines[0].take_batch();
            self.engines[0].switch_database(selected);
            return Ok(batch);
        }
        let keys: Vec<_> = keys.into_iter().map(|key| (0, key)).collect();
        let mut engine = self.take_database_keys(&keys, journal);
        engine.switch_database(0);
        engine.search_reap(now);
        engine.finish_pending_search();
        let batch = engine.take_batch();
        self.restore(engine);
        self.select_database(selected);
        Ok(batch)
    }

    pub fn take_keys(&mut self, keys: &[Bytes], journal: bool) -> Engine {
        let mut engine = Engine::new();
        engine.inherit_runtime(&self.engines[0]);
        if journal {
            engine.set_recording(true);
        }
        for key in keys {
            let shard = Engine::shard_for(key, self.engines.len());
            self.engines[shard].transfer_keys(std::slice::from_ref(key), &mut engine);
        }
        engine
    }

    /// Move the complete dataset into one coherent view without cloning values.
    /// Empty replacement shards retain their runtime and recording configuration
    /// so subsequent commands in the same transaction still capture mutations.
    pub fn take_all(&mut self, journal: bool) -> Engine {
        let mut engine = std::mem::replace(&mut self.engines[0], Engine::new());
        self.engines[0].inherit_runtime(&engine);
        self.engines[0].set_recording(journal);
        for source in self.engines.iter_mut().skip(1) {
            let previous = std::mem::replace(source, Engine::new());
            source.inherit_runtime(&previous);
            source.set_recording(journal);
            previous.distribute(std::slice::from_mut(&mut engine));
        }
        engine.set_recording(journal);
        engine
    }

    /// Resolve dynamic compaction endpoints while the write barrier is held.
    /// Label scans need every shard; ordinary writes move only related series.
    pub fn take_related(&mut self, args: &[Bytes], journal: bool, now: i64) -> Engine {
        let database = self.engines[0].database();
        let destination = if args[0].eq_ignore_ascii_case(b"MOVE") {
            args.get(2)
                .and_then(|value| std::str::from_utf8(value).ok()?.parse::<u32>().ok())
                .map(|db| (db, args[1].clone()))
        } else if args[0].eq_ignore_ascii_case(b"COPY") {
            args[3..]
                .windows(2)
                .find(|pair| pair[0].eq_ignore_ascii_case(b"DB"))
                .and_then(|pair| std::str::from_utf8(&pair[1]).ok()?.parse::<u32>().ok())
                .map(|db| (db, args[2].clone()))
        } else {
            None
        };
        if let Some(destination) = destination
            && destination.0 < self.engines[0].database_count()
        {
            let mut keys: Vec<_> = Engine::accessed_keys(args)
                .into_iter()
                .map(|key| (database, key))
                .collect();
            keys.push(destination);
            return self.take_database_keys(&keys, journal);
        }
        let mut keys = Engine::accessed_keys(args);
        if keys.is_empty() {
            return self.take_all(journal);
        }
        let mut seen = HashSet::with_capacity(keys.len());
        keys.retain(|key| seen.insert(key.clone()));
        let mut index = 0;
        while index < keys.len() {
            let shard = Engine::shard_for(&keys[index], self.engines.len());
            for key in self.engines[shard].related_keys(&keys[index], now) {
                if seen.insert(key.clone()) {
                    keys.push(key.clone());
                }
            }
            index += 1;
        }
        self.take_keys(&keys, journal)
    }

    pub fn restore(&mut self, engine: Engine) {
        engine.distribute(&mut self.engines);
    }

    pub fn resume(self) -> io::Result<()> {
        for (resume, engine) in self.resumes.into_iter().zip(self.engines) {
            resume.send(engine).map_err(|_| stopped())?;
        }
        Ok(())
    }

    /// Sample the union of visible live keys, not one equally weighted shard.
    pub fn random_key(&self, now: i64, visible_slots: Option<&[bool]>) -> Resp {
        let visible = |key: &Bytes| {
            visible_slots.is_none_or(|slots| {
                slots
                    .get(crate::cluster::key_slot(key) as usize)
                    .copied()
                    .unwrap_or(false)
            })
        };
        let count = self
            .engines
            .iter()
            .flat_map(|engine| engine.live_keys(now))
            .filter(|key| visible(key))
            .count();
        if count == 0 {
            return Resp::Bulk(None);
        }
        let index = rand::rng().random_range(0..count);
        let key = self
            .engines
            .iter()
            .flat_map(|engine| engine.live_keys(now))
            .filter(|key| visible(key))
            .nth(index)
            .cloned();
        let reply = Resp::Bulk(key);
        let mut budget = crate::engine::ReplyBudget::new(self.engines[0].reply_limit());
        match budget.response(&reply) {
            Ok(()) => reply,
            Err(error) => error,
        }
    }

    pub fn collect_global(
        &mut self,
        command: GlobalCommand,
        args: &[Bytes],
        now: i64,
    ) -> io::Result<(Resp, Option<Batch>)> {
        if matches!(command, GlobalCommand::RandomKey) {
            return Ok((self.random_key(now, None), None));
        }
        let mut budget = crate::engine::ReplyBudget::new(self.engines[0].reply_limit());
        if matches!(command, GlobalCommand::Scan)
            && let Err(error) = budget.charge(3, 20)
        {
            return Ok((error, None));
        }
        let mut values = Vec::new();
        let mut total = 0_i64;
        let mut cursor = None;
        let mut flush_batch: Option<Batch> = None;
        let mut cleared = std::collections::BTreeSet::new();
        let mut flush_output_database = None;
        for engine in &mut self.engines {
            let execution = engine.execute_at_time(args, now)?;
            let Execution::Reply(reply) = execution else {
                return Err(invalid_reply());
            };
            if let Some(batch) = engine.take_batch() {
                if !matches!(command, GlobalCommand::Flush) {
                    return Err(invalid_reply());
                }
                let mut database = 0;
                for mutation in batch.mutations {
                    if let crate::journal::Mutation::SelectDatabase { database: selected } =
                        mutation
                    {
                        database = selected;
                        continue;
                    }
                    if matches!(mutation, crate::journal::Mutation::Clear)
                        && !cleared.insert(database)
                    {
                        continue;
                    }
                    let output = flush_batch.get_or_insert_with(|| Batch {
                        timestamp_ms: now,
                        mutations: Vec::new(),
                    });
                    if flush_output_database != Some(database) {
                        output
                            .mutations
                            .push(crate::journal::Mutation::SelectDatabase { database });
                        flush_output_database = Some(database);
                    }
                    output.mutations.push(mutation);
                }
            }
            if matches!(reply, Resp::Error(_)) {
                return Ok((reply, None));
            }
            match command {
                GlobalCommand::RandomKey => {
                    unreachable!("RANDOMKEY is aggregated before shard execution")
                }
                GlobalCommand::Flush => {
                    if reply != Resp::Simple("OK") {
                        return Err(invalid_reply());
                    }
                }
                GlobalCommand::Dbsize => {
                    let Resp::Integer(count) = reply else {
                        return Err(invalid_reply());
                    };
                    total = total.checked_add(count).ok_or_else(invalid_reply)?;
                }
                GlobalCommand::Keys => {
                    let Resp::Array(Some(keys)) = reply else {
                        return Err(invalid_reply());
                    };
                    for key in &keys {
                        if let Err(error) = budget.response(key) {
                            return Ok((error, None));
                        }
                    }
                    values.extend(keys);
                }
                GlobalCommand::Scan => {
                    let Resp::Array(Some(mut fields)) = reply else {
                        return Err(invalid_reply());
                    };
                    if fields.len() != 2 {
                        return Err(invalid_reply());
                    }
                    let Resp::Array(Some(keys)) = fields.pop().unwrap() else {
                        return Err(invalid_reply());
                    };
                    let Resp::Bulk(Some(next)) = fields.pop().unwrap() else {
                        return Err(invalid_reply());
                    };
                    let next = std::str::from_utf8(&next)
                        .ok()
                        .and_then(|next| next.parse::<u64>().ok())
                        .ok_or_else(invalid_reply)?;
                    if next != 0 {
                        cursor = Some(cursor.map_or(next, |current: u64| current.min(next)));
                    }
                    for key in &keys {
                        if let Err(error) = budget.response(key) {
                            return Ok((error, None));
                        }
                    }
                    values.extend(keys);
                }
            }
        }
        let reply = match command {
            GlobalCommand::RandomKey => {
                unreachable!("RANDOMKEY is aggregated before shard execution")
            }
            GlobalCommand::Flush => Resp::Simple("OK"),
            GlobalCommand::Dbsize => Resp::Integer(total),
            GlobalCommand::Keys => Resp::Array(Some(values)),
            GlobalCommand::Scan => {
                // The next cursor cannot skip any unfinished shard bucket.
                if let Some(next) = cursor {
                    values.retain(|value| matches!(value, Resp::Bulk(Some(key)) if Engine::key_hash(key) < next));
                }
                Resp::Array(Some(vec![
                    Resp::Bulk(Some(Bytes::from(cursor.unwrap_or(0).to_string()))),
                    Resp::Array(Some(values)),
                ]))
            }
        };
        Ok((reply, flush_batch))
    }
}

async fn run_worker(
    mut engine: Engine,
    mut requests: mpsc::Receiver<ShardRequest>,
    persistence: Arc<Persistence>,
) -> io::Result<()> {
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut databases = Vec::new();
    loop {
        tokio::select! {
            request = requests.recv() => match request {
                Some(ShardRequest::Execute {args, reply, reply_limit, user, version, session}) => {
                    persistence.ensure_healthy()?;
                    engine.set_recording(persistence.recording_for(&args));
                    engine.set_reply_limit(reply_limit);
                    engine.set_command_user(user);
                    engine.set_command_version(version);
                    engine.switch_database(session.as_ref().map_or(0, |session|session.database()));
                    engine.set_command_session(session.clone());
                    let cpu = crate::server_commands::thread_cpu_time();
                    let execution = match engine.execute(&args) {
                        Ok(execution) => execution,
                        Err(error) => {
                            let _ = reply.send(Err(io::Error::new(error.kind(), error.to_string())));
                            return Err(error);
                        }
                    };
                    if let Some(session) = &session {
                        session.cpu_ns.fetch_add(crate::server_commands::thread_cpu_time().saturating_sub(cpu).as_nanos().min(u64::MAX as u128) as u64, std::sync::atomic::Ordering::Relaxed);
                    }
                    persistence.note_reads(&args, session.as_deref());
                    if let Err(error) = persistence.commit(&mut engine).await {
                        let _ = reply.send(Err(io::Error::new(error.kind(), error.to_string())));
                        return Err(error);
                    }
                    engine.set_command_session(None);
                    engine.set_command_user(None);
                    engine.set_command_version(crate::protocol::Version::Resp2);
                    engine.set_recording(persistence.capturing());
                    let _ = reply.send(Ok(execution));
                }
                Some(ShardRequest::Pause {reply, resume}) => {
                    let paused = std::mem::replace(&mut engine, Engine::new());
                    reply.send(paused).map_err(|_| stopped())?;
                    engine = resume.await.map_err(|_| stopped())?;
                }
                None => return Ok(()),
            },
            _ = tick.tick() => {
                engine.set_recording(persistence.capturing());
                let selected = engine.database();
                let now = Engine::now();
                databases.clear();
                databases.extend(engine.database_ids());
                for database in databases.iter().copied() {
                    engine.switch_database(database);
                    engine.expire_some();
                    engine.maintain_native(now, 128)?;
                }
                engine.switch_database(selected);
                persistence.commit(&mut engine).await?;
                engine.set_recording(persistence.capturing());
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn random_key_includes_later_shards_and_excludes_invisible_slots() {
        let key = (0..1000)
            .map(|index| Bytes::from(format!("visible:{index}")))
            .find(|key| Engine::shard_for(key, 2) == 1)
            .unwrap();
        let hidden = (0..1000)
            .map(|index| Bytes::from(format!("hidden:{index}")))
            .find(|candidate| crate::cluster::key_slot(candidate) != crate::cluster::key_slot(&key))
            .unwrap();
        let mut engine = Engine::new();
        for key in [&key, &hidden] {
            engine
                .execute(&[
                    Bytes::from_static(b"SET"),
                    key.clone(),
                    Bytes::from_static(b"value"),
                ])
                .unwrap();
        }
        let persistence = Arc::new(Persistence::new(None));
        let (pool, mut workers) = Pool::start(engine, 2, persistence);
        let paused = pool.pause().await.unwrap();
        let mut visible = vec![false; 16_384];
        visible[crate::cluster::key_slot(&key) as usize] = true;
        assert_eq!(
            paused.random_key(Engine::now(), Some(&visible)),
            Resp::Bulk(Some(key))
        );
        visible.fill(false);
        assert_eq!(
            paused.random_key(Engine::now(), Some(&visible)),
            Resp::Bulk(None)
        );
        paused.resume().unwrap();
        drop(pool);
        while let Some(result) = workers.join_next().await {
            result.unwrap().unwrap();
        }
    }

    #[tokio::test]
    async fn global_keys_cannot_multiply_the_origin_budget_by_shard_count() {
        let limit = std::mem::size_of::<Resp>() + 1;
        let mut engines = Vec::new();
        for key in [b"a", b"b"] {
            let mut engine = Engine::new();
            engine
                .execute(&[
                    Bytes::from_static(b"SET"),
                    Bytes::copy_from_slice(key),
                    Bytes::from_static(b"value"),
                ])
                .unwrap();
            engine.set_reply_limit(limit);
            engines.push(engine);
        }
        let guard = Arc::new(RwLock::new(())).write_owned().await;
        let mut paused = Paused {
            engines,
            resumes: Vec::new(),
            _guard: guard,
        };
        let args = [Bytes::from_static(b"KEYS"), Bytes::from_static(b"*")];
        let (reply, _) = paused
            .collect_global(GlobalCommand::Keys, &args, Engine::now())
            .unwrap();
        assert!(matches!(reply, Resp::Error(_)));
        for engine in &mut paused.engines {
            engine.set_reply_limit(limit * 2);
        }
        let (reply, _) = paused
            .collect_global(GlobalCommand::Keys, &args, Engine::now())
            .unwrap();
        assert!(matches!(reply, Resp::Array(Some(keys)) if keys.len() == 2));
    }
}
