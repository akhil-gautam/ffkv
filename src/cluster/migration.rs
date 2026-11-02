use super::bus::{Reply, Request};
use super::*;
use tokio::time::{Duration, Instant, sleep};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) struct Task {
    pub id: String,
    pub ranges: Vec<(u16, u16)>,
    pub source: String,
    pub dest: String,
    pub operation: String,
    pub state: String,
    pub last_error: String,
    pub retries: u64,
    pub create_time: u64,
    pub start_time: u64,
    pub end_time: u64,
    pub paused_at: u64,
    #[serde(default)]
    pub stream_offset: u64,
    #[serde(default)]
    pub stream_history: String,
    #[serde(default)]
    pub activity: u64,
    #[serde(default)]
    pub snapshot_id: String,
    #[serde(default)]
    pub write_pause_ms: u64,
    #[serde(skip)]
    pub running: bool,
}
impl Task {
    pub fn active(&self) -> bool {
        !matches!(self.state.as_str(), "completed" | "canceled")
    }
    pub fn contains(&self, slot: u16) -> bool {
        self.ranges.iter().any(|(a, b)| slot >= *a && slot <= *b)
    }
    fn reply(&self) -> Resp {
        let slots = self
            .ranges
            .iter()
            .map(|(a, b)| {
                if a == b {
                    a.to_string()
                } else {
                    format!("{a}-{b}")
                }
            })
            .collect::<Vec<_>>()
            .join(" ");
        Resp::Map(vec![
            (bulk("id"), bulk(self.id.clone())),
            (bulk("slots"), bulk(slots)),
            (bulk("source"), bulk(self.source.clone())),
            (bulk("dest"), bulk(self.dest.clone())),
            (bulk("operation"), bulk(self.operation.clone())),
            (bulk("state"), bulk(self.state.clone())),
            (bulk("last_error"), bulk(self.last_error.clone())),
            (bulk("retries"), int(self.retries)),
            (bulk("create_time"), int(self.create_time)),
            (bulk("start_time"), int(self.start_time)),
            (bulk("end_time"), int(self.end_time)),
            (
                bulk("write_pause_ms"),
                int(
                    if self.operation == "migrate" && self.state == "completed" {
                        self.write_pause_ms
                    } else {
                        0
                    },
                ),
            ),
        ])
    }
}
impl ClusterRuntime {
    pub(super) async fn migration_command(
        self: &Arc<Self>,
        args: &[Bytes],
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<Resp> {
        if args.len() < 4 {
            return Err(invalid("wrong number of arguments"));
        }
        if args[2].eq_ignore_ascii_case(b"IMPORT") {
            let slots = topology::parse_ranges(&args[3..], true)?;
            let _admin = self.admin.lock().await;
            let task = {
                let mut s = self.state.write().await;
                let me = &s.durable.nodes[&s.durable.myself];
                if me.parent.is_some() {
                    return Err(invalid("destination must be a master"));
                }
                if !s.durable.importing.is_empty() || !s.durable.migrating.is_empty() {
                    return Err(invalid("destination has open legacy migration slots"));
                }
                if s.tasks.iter().any(|t| t.active() && t.state != "failed") {
                    return Err(invalid("another ASM task is already in progress"));
                }
                let source = s.owners[slots[0] as usize]
                    .clone()
                    .ok_or_else(|| invalid("slot range is unassigned"))?;
                if source == me.id {
                    return Err(invalid("this node is already the owner of the slot range"));
                }
                if slots
                    .iter()
                    .any(|slot| s.owners[*slot as usize].as_ref() != Some(&source))
                {
                    return Err(invalid("all slot ranges must have the same source master"));
                }
                if s.durable.nodes[&source].parent.is_some() {
                    return Err(invalid("source must be a master"));
                }
                for old in &mut s.tasks {
                    if old.active() {
                        old.state = "canceled".into();
                        old.end_time = clock();
                    }
                }
                let task = Task {
                    id: random_id(),
                    ranges: topology::ranges(slots),
                    source,
                    dest: s.durable.myself.clone(),
                    operation: "import".into(),
                    state: "none".into(),
                    last_error: String::new(),
                    retries: 0,
                    create_time: clock(),
                    start_time: 0,
                    end_time: 0,
                    paused_at: 0,
                    stream_offset: 0,
                    stream_history: String::new(),
                    activity: clock(),
                    snapshot_id: String::new(),
                    write_pause_ms: 0,
                    running: true,
                };
                archive_bound(&mut s.tasks);
                s.tasks.push(task.clone());
                task
            };
            self.save().await?;
            let runtime = self.clone();
            let id = task.id.clone();
            if let Err(error) = self
                .spawn_owned(async move {
                    runtime.run_migration(task).await;
                })
                .await
            {
                self.cancel_tasks_with_hooks(Some(&id), hooks).await?;
                return Err(error);
            }
            Ok(bulk(id))
        } else if args[2].eq_ignore_ascii_case(b"STATUS") || args[2].eq_ignore_ascii_case(b"CANCEL")
        {
            let id = if args.len() == 4 && args[3].eq_ignore_ascii_case(b"ALL") {
                None
            } else if args.len() == 5 && args[3].eq_ignore_ascii_case(b"ID") {
                Some(text(&args[4])?)
            } else {
                return Err(invalid("syntax error"));
            };
            if args[2].eq_ignore_ascii_case(b"CANCEL") {
                let _admin = self.admin.lock().await;
                Ok(Resp::Integer(
                    self.cancel_tasks_with_hooks(id, hooks).await? as i64,
                ))
            } else {
                let s = self.state.read().await;
                Ok(array(
                    s.tasks
                        .iter()
                        .filter(|t| id.is_none_or(|id| id == t.id))
                        .map(Task::reply)
                        .collect(),
                ))
            }
        } else {
            Err(invalid("Unknown CLUSTER MIGRATION subcommand"))
        }
    }
    pub(super) async fn cancel_tasks(&self, id: Option<&str>) -> io::Result<usize> {
        let hooks = self.hooks.as_ref();
        self.cancel_tasks_with_hooks(id, &hooks).await
    }
    pub(super) async fn cancel_tasks_with_hooks(
        &self,
        id: Option<&str>,
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<usize> {
        let (tasks, mine) = {
            let mut s = self.state.write().await;
            let mine = s.durable.myself.clone();
            let mut canceled = vec![];
            for task in &mut s.tasks {
                if task.active() && id.is_none_or(|id| id == task.id) {
                    task.state = "canceled".into();
                    task.last_error = "user request".into();
                    task.end_time = clock();
                    canceled.push(task.clone());
                }
            }
            (canceled, mine)
        };
        for task in &tasks {
            if task.source == mine {
                self.release_pause_with_hooks(&task.dest, hooks).await?;
                self.remove_snapshot(&task.snapshot_id).await?;
            } else {
                let owned = {
                    let s = self.state.read().await;
                    task.ranges
                        .iter()
                        .flat_map(|(a, b)| *a..=*b)
                        .all(|slot| s.owners[slot as usize].as_ref() == Some(&mine))
                };
                if !owned {
                    hooks.delete_slots(&task.ranges).await?;
                }
                // Source-side cancellation deliberately does not cancel importer intent.
                // Destination-side cancellation interrupts both channels and source pause.
                let source = self
                    .state
                    .read()
                    .await
                    .durable
                    .nodes
                    .get(&task.source)
                    .cloned();
                if let Some(source) = source {
                    let _ = self
                        .rpc(
                            &source,
                            Request::Cancel {
                                task: task.id.clone(),
                            },
                        )
                        .await;
                }
            }
        }
        if !tasks.is_empty() {
            self.save().await?;
        }
        Ok(tasks.len())
    }
    async fn update_task(&self, id: &str, state: &str) -> io::Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cluster is shutting down",
            ));
        }
        let mut s = self.state.write().await;
        let task = s
            .tasks
            .iter_mut()
            .find(|t| t.id == id && t.active())
            .ok_or_else(|| invalid("migration was canceled"))?;
        task.state = state.into();
        Ok(())
    }
    async fn run_migration(self: Arc<Self>, task: Task) {
        let source = self
            .state
            .read()
            .await
            .durable
            .nodes
            .get(&task.source)
            .cloned();
        let Some(source) = source else { return };
        let outcome = self.transfer(&task, &source).await;
        let owned = {
            let s = self.state.read().await;
            task.ranges
                .iter()
                .flat_map(|(a, b)| *a..=*b)
                .all(|slot| s.owners[slot as usize].as_ref() == Some(&s.durable.myself))
        };
        if outcome.is_err() && !owned {
            let _ = self
                .rpc(
                    &source,
                    Request::Cancel {
                        task: task.id.clone(),
                    },
                )
                .await;
        }
        let _ = self.rpc(&source, Request::Unfreeze).await;
        let cleanup = if owned {
            Ok(())
        } else {
            self.hooks.delete_slots(&task.ranges).await
        };
        let finish = self.hooks.finish_migration(&task.ranges).await;
        let completion = cleanup.and(finish);
        let mut s = self.state.write().await;
        if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
            t.running = false;
            if t.state != "canceled" {
                if let Err(error) = completion {
                    t.state = "failed".into();
                    t.last_error = error.to_string();
                    t.end_time = clock();
                } else {
                    match outcome {
                        Ok(()) => {
                            t.state = "completed".into();
                            t.end_time = clock();
                            t.last_error.clear();
                        }
                        Err(e) => {
                            if owned {
                                t.state = "completed".into();
                                t.end_time = clock();
                            } else {
                                t.state = "failed".into();
                                t.last_error = e.to_string();
                                t.end_time = clock();
                            }
                        }
                    }
                }
            }
        }
        drop(s);
        let _ = self.save().await;
    }
    async fn transfer(&self, task: &Task, source: &Node) -> io::Result<()> {
        self.update_task(&task.id, "connecting").await?;
        {
            let mut s = self.state.write().await;
            if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
                t.start_time = clock();
                t.end_time = 0;
            }
        }
        let mut offset = match self
            .rpc(
                source,
                Request::Freeze {
                    task: Some(task.clone()),
                },
            )
            .await?
        {
            Reply::Frozen(offset) => offset,
            _ => return Err(invalid("source refused migration barrier")),
        };
        // A coherent one-key-at-a-time disk snapshot releases the initial write
        // barrier before network transfer. Subsequent resolved native mutations
        // are replayed in order, with a final processed-offset handoff barrier.
        self.update_task(&task.id, "accumulate-buffer").await?;
        self.hooks.delete_slots(&task.ranges).await?;
        loop {
            self.update_task(&task.id, "accumulate-buffer").await?;
            let reply = self
                .rpc(
                    source,
                    Request::Globals {
                        task: task.id.clone(),
                    },
                )
                .await?;
            if matches!(reply, Reply::Pending) {
                sleep(Duration::from_millis(10)).await;
                continue;
            }
            let (key, payload, next) = self.receive_chunks(task, source, reply).await?;
            if !key.is_empty() || next.is_some() {
                return Err(invalid("invalid global migration metadata marker"));
            }
            self.hooks
                .import_migration_globals(&payload, &task.ranges)
                .await?;
            break;
        }
        let mut after = None;
        loop {
            self.update_task(&task.id, "streaming-buffer").await?;
            let reply = self
                .rpc(
                    source,
                    Request::Export {
                        task: task.id.clone(),
                        after: after.clone(),
                    },
                )
                .await?;
            if matches!(reply, Reply::Pending) {
                sleep(Duration::from_millis(10)).await;
                continue;
            }
            if matches!(reply, Reply::Empty) {
                break;
            }
            let (key, payload, next) = self.receive_chunks(task, source, reply).await?;
            if !task.contains(key_slot(&key)) {
                return Err(invalid("source sent key outside migration slots"));
            }
            self.update_task(&task.id, "streaming-buffer").await?;
            self.hooks
                .import_keys(&[ClusterKey { key, payload }])
                .await?;
            match next {
                Some(cursor) => {
                    if after.as_ref().is_some_and(|old: &Vec<u8>| cursor <= *old) {
                        return Err(invalid("snapshot cursor failed to advance"));
                    }
                    after = Some(cursor);
                }
                None => break,
            }
        }
        for _ in 0..64 {
            let through = self.replay_changes(task, source, offset).await?;
            if through == offset {
                break;
            }
            offset = through;
        }
        let boundary = match self
            .rpc(
                source,
                Request::HandoffFreeze {
                    task: task.id.clone(),
                },
            )
            .await?
        {
            Reply::Frozen(offset) => offset,
            _ => return Err(invalid("source refused final migration barrier")),
        };
        while offset < boundary {
            let through = self.replay_changes(task, source, offset).await?;
            if through <= offset {
                return Err(invalid("migration stream ended before handoff boundary"));
            }
            offset = through;
        }
        self.update_task(&task.id, "wait-stream-eof").await?;
        let _admin = self.admin.lock().await;
        let owner = {
            let mut s = self.state.write().await;
            if !s.tasks.iter().any(|t| t.id == task.id && t.active()) {
                return Err(invalid("migration canceled before takeover"));
            }
            if task
                .ranges
                .iter()
                .flat_map(|(a, b)| *a..=*b)
                .any(|slot| s.owners[slot as usize].as_ref() != Some(&source.id))
            {
                return Err(invalid("slot ownership changed during migration"));
            }
            let id = s.durable.myself.clone();
            let max = s.durable.nodes.values().map(|n| n.epoch).max().unwrap_or(0);
            s.durable.current_epoch = s
                .durable
                .current_epoch
                .max(max)
                .checked_add(1)
                .ok_or_else(|| invalid("cluster epoch exhausted"))?;
            let epoch = s.durable.current_epoch;
            let me = s
                .durable
                .nodes
                .get_mut(&id)
                .ok_or_else(|| invalid("missing self"))?;
            me.epoch = epoch;
            me.revision += 1;
            for (a, b) in &task.ranges {
                for slot in *a..=*b {
                    topology::set_owner(&mut s, slot, Some(&id));
                }
            }
            if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
                t.state = "takeover".into();
            }
            s.durable.nodes[&id].clone()
        };
        self.save().await?;
        self.rpc(
            source,
            Request::Takeover {
                task: task.id.clone(),
                owner,
                ranges: task.ranges.clone(),
            },
        )
        .await?;
        Ok(())
    }
    async fn receive_chunks(
        &self,
        task: &Task,
        source: &Node,
        mut reply: Reply,
    ) -> io::Result<(Vec<u8>, Vec<u8>, Option<Vec<u8>>)> {
        let mut payload = Vec::new();
        let mut key = Vec::new();
        let mut key_complete = false;
        loop {
            self.update_task(&task.id, "streaming-buffer").await?;
            match reply {
                Reply::Chunk {
                    key: fragment,
                    key_done,
                    data,
                    done,
                    next,
                } => {
                    if (key_complete && !fragment.is_empty())
                        || (!key_done && !data.is_empty())
                        || (key_complete && !key_done)
                    {
                        return Err(invalid("invalid migration key fragment order"));
                    }
                    if key
                        .len()
                        .checked_add(fragment.len())
                        .is_none_or(|length| length > 512 * 1024 * 1024)
                        || payload
                            .len()
                            .checked_add(data.len())
                            .is_none_or(|length| length > 512 * 1024 * 1024)
                    {
                        return Err(invalid("migration record exceeds supported value size"));
                    }
                    key.extend_from_slice(&fragment);
                    key_complete = key_done;
                    payload.extend_from_slice(&data);
                    if done {
                        if !key_complete {
                            return Err(invalid("truncated migration key"));
                        }
                        return Ok((key, payload, next));
                    }
                }
                _ => return Err(invalid("invalid migration stream chunk")),
            }
            reply = self
                .rpc(
                    source,
                    Request::ExportContinue {
                        task: task.id.clone(),
                    },
                )
                .await?;
        }
    }
    async fn replay_changes(&self, task: &Task, source: &Node, offset: u64) -> io::Result<u64> {
        let reply = self
            .rpc(
                source,
                Request::Changes {
                    task: task.id.clone(),
                    after: offset,
                },
            )
            .await?;
        let (key, bytes, next) = self.receive_chunks(task, source, reply).await?;
        if !key.is_empty() {
            return Err(invalid("invalid native stream marker"));
        }
        let through = u64::from_be_bytes(
            next.ok_or_else(|| invalid("missing native stream boundary"))?
                .as_slice()
                .try_into()
                .map_err(|_| invalid("invalid native stream boundary"))?,
        );
        if through < offset || (through == offset && !bytes.is_empty()) {
            return Err(invalid("nonadvancing native migration stream"));
        }
        if !bytes.is_empty() {
            self.hooks
                .apply_migration_changes(&bytes, &task.ranges)
                .await?;
        }
        Ok(through)
    }
    pub(super) async fn migration_tick(self: &Arc<Self>) {
        if self.stopped.load(Ordering::Acquire) {
            return;
        }
        let (retry, trim) = {
            let s = self.state.read().await;
            let retry = s
                .tasks
                .iter()
                .filter(|t| {
                    t.operation == "import"
                        && t.state == "failed"
                        && !t.running
                        && clock().saturating_sub(t.end_time) >= 1000
                })
                .cloned()
                .collect::<Vec<_>>();
            let trim = s
                .tasks
                .iter()
                .filter(|t| {
                    t.operation == "migrate"
                        && t.active()
                        && t.ranges
                            .iter()
                            .flat_map(|(a, b)| *a..=*b)
                            .all(|slot| s.owners[slot as usize].as_ref() == Some(&t.dest))
                })
                .cloned()
                .collect::<Vec<_>>();
            (retry, trim)
        };
        for task in trim {
            let _ = self.release_pause(&task.dest).await;
            if self.hooks.delete_slots(&task.ranges).await.is_ok() {
                self.clear_slot_metrics(&task.ranges);
                let mut s = self.state.write().await;
                if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
                    t.state = "completed".into();
                    t.end_time = clock();
                }
                drop(s);
                let _ = self.remove_snapshot(&task.snapshot_id).await;
            }
        }
        let abandoned = {
            let s = self.state.read().await;
            s.tasks
                .iter()
                .filter(|task| {
                    task.operation == "migrate"
                        && task.active()
                        && ((task.state == "failed"
                            && clock().saturating_sub(task.end_time)
                                > self.config.node_timeout_ms * 2)
                            || (task.activity > 0
                                && clock().saturating_sub(task.activity)
                                    > self.config.node_timeout_ms.max(1000) * 8))
                })
                .map(|task| task.id.clone())
                .collect::<Vec<_>>()
        };
        for id in abandoned {
            let _ = self.cancel_tasks(Some(&id)).await;
        }
        for task in retry {
            let mut s = self.state.write().await;
            let valid = task
                .ranges
                .iter()
                .flat_map(|(a, b)| *a..=*b)
                .all(|slot| s.owners[slot as usize].as_ref() == Some(&task.source));
            if !valid {
                if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
                    t.state = "canceled".into();
                    t.end_time = clock();
                    t.last_error = "slot ownership changed".into();
                }
                continue;
            }
            if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task.id) {
                t.running = true;
                t.retries += 1;
                t.state = "connecting".into();
            }
            drop(s);
            let id = task.id.clone();
            let runtime = self.clone();
            if let Err(error) = self
                .spawn_owned(async move {
                    runtime.run_migration(task).await;
                })
                .await
            {
                let mut s = self.state.write().await;
                if let Some(t) = s.tasks.iter_mut().find(|t| t.id == id) {
                    t.running = false;
                    t.state = "failed".into();
                    t.end_time = clock();
                    t.last_error = error.to_string();
                }
            }
        }
    }
    pub(super) async fn failover(&self, mode: &str, automatic: bool) -> io::Result<()> {
        let _admin = self.admin.lock().await;
        if self.stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cluster is shutting down",
            ));
        }
        let (me, primary, voters) = {
            let s = self.state.read().await;
            let me = s.durable.nodes[&s.durable.myself].clone();
            let parent = me
                .parent
                .as_ref()
                .ok_or_else(|| invalid("failover requires a replica"))?;
            let primary = s
                .durable
                .nodes
                .get(parent)
                .ok_or_else(|| invalid("unknown primary"))?
                .clone();
            let voters = s
                .durable
                .nodes
                .values()
                .filter(|node| topology::is_voter(&s, node))
                .cloned()
                .collect::<Vec<_>>();
            (me, primary, voters)
        };
        let progress = self.hooks.replication_progress().await?;
        if automatic {
            let s = self.state.read().await;
            let last_connected = self.replica_last_connected.load(Ordering::Relaxed);
            let fresh = last_connected > 0
                && clock().saturating_sub(last_connected)
                    < self.config.node_timeout_ms * 10 + 10_000;
            if !fresh {
                return Err(invalid("replica is too stale for election"));
            }
            if s.durable.nodes.values().any(|n| {
                n.parent.as_ref() == Some(&primary.id)
                    && n.id != me.id
                    && (n.offset > progress.processed
                        || (n.offset == progress.processed && n.id < me.id))
                    && s.live.get(&n.id).is_some_and(|l| {
                        clock().saturating_sub(l.last_seen) < self.config.node_timeout_ms
                    })
            }) {
                return Err(invalid(
                    "a fresher or higher-priority replica is eligible for election",
                ));
            }
        }
        let mut frozen = false;
        let mut required = None;
        let result = async {
            if mode.is_empty() {
                if !progress.upstream_connected {
                    return Err(invalid("master link is down"));
                }
                let offset = match self.rpc(&primary, Request::Freeze { task: None }).await? {
                    Reply::Frozen(offset) => offset,
                    _ => return Err(invalid("primary refused write barrier")),
                };
                frozen = true;
                required = Some(offset);
                let deadline =
                    Instant::now() + Duration::from_millis(self.config.node_timeout_ms * 2);
                loop {
                    if self.hooks.replication_progress().await?.processed >= offset {
                        break;
                    }
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "replica catch-up timed out",
                        ));
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }
            let epoch = {
                let mut s = self.state.write().await;
                s.durable.current_epoch = s
                    .durable
                    .current_epoch
                    .max(s.durable.nodes.values().map(|n| n.epoch).max().unwrap_or(0))
                    .checked_add(1)
                    .ok_or_else(|| invalid("cluster epoch exhausted"))?;
                s.durable.current_epoch
            };
            self.save().await?;
            if mode != "TAKEOVER" {
                let offset = self.hooks.replication_progress().await?.processed;
                let mut votes = 0;
                for voter in &voters {
                    if let Ok(Reply::Vote(true)) = self
                        .rpc(
                            voter,
                            Request::Vote {
                                epoch,
                                candidate: me.id.clone(),
                                parent: primary.id.clone(),
                                offset,
                                manual: !automatic,
                            },
                        )
                        .await
                    {
                        votes += 1;
                        if votes > voters.len() / 2 {
                            break;
                        }
                    }
                }
                if votes <= voters.len() / 2 {
                    return Err(invalid("failover election did not obtain a majority"));
                }
            }
            if mode.is_empty() {
                let offset = match self.rpc(&primary, Request::Freeze { task: None }).await? {
                    Reply::Frozen(offset) => offset,
                    _ => return Err(invalid("primary refused final write barrier")),
                };
                required = Some(offset);
                let deadline =
                    Instant::now() + Duration::from_millis(self.config.node_timeout_ms * 2);
                while self.hooks.replication_progress().await?.processed < offset {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(
                            io::ErrorKind::TimedOut,
                            "replica final catch-up timed out",
                        ));
                    }
                    sleep(Duration::from_millis(10)).await;
                }
            }
            self.hooks.promote(required).await?;
            let winner = {
                let mut s = self.state.write().await;
                let id = s.durable.myself.clone();
                let slots = s
                    .owners
                    .iter()
                    .enumerate()
                    .filter(|(_, owner)| owner.as_ref() == Some(&primary.id))
                    .map(|(slot, _)| slot as u16)
                    .collect::<Vec<_>>();
                let me = s
                    .durable
                    .nodes
                    .get_mut(&id)
                    .ok_or_else(|| invalid("missing self"))?;
                me.parent = None;
                me.epoch = epoch;
                me.revision += 1;
                for slot in slots {
                    topology::set_owner(&mut s, slot, Some(&id));
                }
                let old = s
                    .durable
                    .nodes
                    .get_mut(&primary.id)
                    .ok_or_else(|| invalid("missing old primary"))?;
                old.parent = Some(id.clone());
                old.slots.clear();
                old.revision += 1;
                s.durable.nodes[&id].clone()
            };
            self.save().await?;
            let _ = self.rpc(&primary, Request::Follow { target: winner }).await;
            Ok(())
        }
        .await;
        if frozen {
            let _ = self.rpc(&primary, Request::Unfreeze).await;
        }
        result
    }
}
fn archive_bound(tasks: &mut Vec<Task>) {
    while tasks.len() >= 32 {
        if let Some(index) = tasks.iter().position(|t| !t.active()) {
            tasks.remove(index);
        } else {
            break;
        }
    }
}
fn int(value: u64) -> Resp {
    Resp::Integer(value.min(i64::MAX as u64) as i64)
}
