use super::*;
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Duration, timeout},
};

const MAX_FRAME: usize = 16 * 1024 * 1024;
const CHUNK: usize = 128 * 1024;
#[derive(Serialize, Deserialize)]
struct Envelope {
    secret: String,
    sender: Node,
    request: Request,
}
#[derive(Serialize)]
struct EnvelopeRef<'a> {
    secret: &'a str,
    sender: &'a Node,
    request: &'a Request,
}
#[derive(Clone, Serialize, Deserialize)]
pub(super) enum Request {
    Gossip {
        meet: bool,
        epoch: u64,
        nodes: Vec<Node>,
        failed: Vec<String>,
        blacklist: BTreeMap<String, u64>,
    },
    Vote {
        epoch: u64,
        candidate: String,
        parent: String,
        offset: u64,
        manual: bool,
    },
    Freeze {
        task: Option<migration::Task>,
    },
    Unfreeze,
    Export {
        task: String,
        after: Option<Vec<u8>>,
    },
    ExportContinue {
        task: String,
    },
    Changes {
        task: String,
        after: u64,
    },
    HandoffFreeze {
        task: String,
    },
    Globals {
        task: String,
    },
    Takeover {
        task: String,
        owner: Node,
        ranges: Vec<(u16, u16)>,
    },
    Follow {
        target: Node,
    },
    Cancel {
        task: String,
    },
    Publish {
        sharded: bool,
        channel: Vec<u8>,
        message: Vec<u8>,
    },
    TaskMetadata {
        task: Option<migration::Task>,
    },
}
#[derive(Serialize, Deserialize)]
pub(super) enum Reply {
    Gossip {
        epoch: u64,
        nodes: Vec<Node>,
        failed: Vec<String>,
        blacklist: BTreeMap<String, u64>,
    },
    Ok,
    Error(String),
    Vote(bool),
    Frozen(u64),
    Pending,
    Chunk {
        key: Vec<u8>,
        key_done: bool,
        data: Vec<u8>,
        done: bool,
        next: Option<Vec<u8>>,
    },
    Empty,
}
struct Exporting {
    task: String,
    key: ClusterKey,
    next: Option<Vec<u8>>,
    key_offset: usize,
    offset: usize,
}

async fn write_frame<T: Serialize>(
    runtime: &ClusterRuntime,
    link: u64,
    stream: &mut TcpStream,
    value: &T,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(|e| invalid(&e.to_string()))?;
    if bytes.len() > MAX_FRAME {
        return Err(invalid("cluster frame exceeds limit"));
    }
    {
        let mut s = runtime.state.write().await;
        if let Some(link) = s.links.get_mut(&link) {
            link.allocated = bytes.capacity();
            link.used = bytes.len();
        }
    }
    let result = async {
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(&bytes).await?;
        stream.flush().await
    }
    .await;
    drop(bytes);
    {
        let mut s = runtime.state.write().await;
        if let Some(link) = s.links.get_mut(&link) {
            link.allocated = 0;
            link.used = 0;
        }
    }
    result
}
async fn read_frame<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> io::Result<T> {
    let size = stream.read_u32().await? as usize;
    if size == 0 || size > MAX_FRAME {
        return Err(invalid("invalid cluster frame size"));
    }
    let mut bytes = vec![0; size];
    stream.read_exact(&mut bytes).await?;
    serde_json::from_slice(&bytes).map_err(|e| invalid(&e.to_string()))
}
impl ClusterRuntime {
    pub(super) async fn touch_pause(&self, sender: &str) -> io::Result<()> {
        let mut pause = self.pause_owner.lock().await;
        match pause.as_mut() {
            Some((owner, deadline)) if owner == sender => {
                *deadline = clock() + self.config.node_timeout_ms.max(1000) * 8;
                Ok(())
            }
            _ => Err(invalid("source write barrier is no longer held")),
        }
    }
    pub(super) async fn release_pause(&self, sender: &str) -> io::Result<()> {
        let hooks = self.hooks.as_ref();
        self.release_pause_with_hooks(sender, &hooks).await
    }
    pub(super) async fn release_pause_with_hooks(
        &self,
        sender: &str,
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<()> {
        let mut pause = self.pause_owner.lock().await;
        if pause.as_ref().is_some_and(|(owner, _)| owner != sender) {
            return Err(invalid("write barrier belongs to another peer"));
        }
        if pause.is_some() {
            hooks.unfreeze_writes().await?;
            *pause = None;
            let mut s = self.state.write().await;
            for task in &mut s.tasks {
                if task.operation == "migrate" && task.dest == sender && task.paused_at > 0 {
                    task.write_pause_ms = clock().saturating_sub(task.paused_at);
                    task.paused_at = 0;
                }
            }
        }
        Ok(())
    }
    pub(super) async fn pause_watchdog(self: Arc<Self>) {
        let mut timer = tokio::time::interval(Duration::from_millis(100));
        while !self.stopped.load(Ordering::Acquire) {
            timer.tick().await;
            let expired = {
                let pause = self.pause_owner.lock().await;
                pause
                    .as_ref()
                    .filter(|(_, until)| clock() > *until)
                    .map(|(owner, _)| owner.clone())
            };
            if let Some(owner) = expired {
                let ids = {
                    let s = self.state.read().await;
                    s.tasks
                        .iter()
                        .filter(|t| t.active() && t.operation == "migrate" && t.dest == owner)
                        .map(|t| t.id.clone())
                        .collect::<Vec<_>>()
                };
                for id in ids {
                    let _ = self.cancel_tasks(Some(&id)).await;
                }
                let _ = self.release_pause(&owner).await;
            }
        }
    }
    pub(super) async fn rpc<R: std::borrow::Borrow<Request> + Send + Sync>(
        &self,
        node: &Node,
        request: R,
    ) -> io::Result<Reply> {
        let cleanup = matches!(request.borrow(), Request::Unfreeze | Request::Cancel { .. });
        let mut stop = self.stop_signal.subscribe();
        if self.stopped.load(Ordering::Acquire) && !cleanup {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cluster is shutting down",
            ));
        }
        {
            let s = self.state.read().await;
            if !s.durable.nodes.contains_key(&node.id)
                && !s.pending.iter().any(|(ip, port)| {
                    node.id.starts_with("meet:") && ip == &node.ip && *port == node.bus_port
                })
            {
                return Err(invalid("unknown cluster peer"));
            }
        }
        let peer = {
            let mut peers = self.peers.lock().await;
            peers
                .entry(node.id.clone())
                .or_insert_with(|| {
                    Arc::new(Peer {
                        stream: Mutex::new(None),
                    })
                })
                .clone()
        };
        let mut stream = if cleanup {
            peer.stream.lock().await
        } else {
            tokio::select! {guard=peer.stream.lock()=>guard,_=stop.changed()=>return Err(io::Error::new(io::ErrorKind::Interrupted,"cluster is shutting down"))}
        };
        {
            let mut s = self.state.write().await;
            let now = clock();
            let live = s.live.entry(node.id.clone()).or_insert(LiveNode {
                last_seen: 0,
                first_seen: now,
                ping_sent: 0,
                reports: HashMap::new(),
                failed: false,
            });
            if live.ping_sent == 0 {
                live.ping_sent = now;
            }
        }
        // Control traffic must not consume the transfer timeout: one stalled
        // primary otherwise delays voting until quorum failure reports expire.
        let deadline = Duration::from_millis(match request.borrow() {
            Request::Gossip { .. } | Request::Vote { .. } => {
                (self.config.node_timeout_ms / 4).clamp(100, 1000)
            }
            _ if cleanup => self.config.node_timeout_ms.clamp(100, 1000),
            _ => self.config.node_timeout_ms.max(1000) * 2,
        });
        let operation = timeout(deadline, async {
            if stream.is_none() {
                let connection = TcpStream::connect((node.ip.as_str(), node.bus_port)).await?;
                connection.set_nodelay(true)?;
                *stream = Some(connection);
                let mut s = self.state.write().await;
                s.links
                    .retain(|_, l| !(l.node == node.id && l.direction == "to"));
                let id = self.next_link.fetch_add(1, Ordering::Relaxed);
                s.links.insert(
                    id,
                    LinkInfo {
                        node: node.id.clone(),
                        direction: "to",
                        created: clock(),
                        allocated: 0,
                        used: 0,
                    },
                );
            }
            let bytes = {
                let s = self.state.read().await;
                serde_json::to_vec(&EnvelopeRef {
                    secret: &self.config.bus_secret,
                    sender: &s.durable.nodes[&s.durable.myself],
                    request: request.borrow(),
                })
                .map_err(|e| invalid(&e.to_string()))?
            };
            if bytes.len() > MAX_FRAME {
                return Err(invalid("cluster frame exceeds limit"));
            }
            {
                let mut s = self.state.write().await;
                for link in s
                    .links
                    .values_mut()
                    .filter(|link| link.direction == "to" && link.node == node.id)
                {
                    link.allocated = bytes.capacity();
                    link.used = bytes.len();
                }
            }
            let connection = stream
                .as_mut()
                .ok_or_else(|| io::Error::other("cluster connection disappeared"))?;
            connection.write_u32(bytes.len() as u32).await?;
            connection.write_all(&bytes).await?;
            connection.flush().await?;
            self.sent.fetch_add(1, Ordering::Relaxed);
            drop(bytes);
            {
                let mut s = self.state.write().await;
                for link in s
                    .links
                    .values_mut()
                    .filter(|link| link.direction == "to" && link.node == node.id)
                {
                    link.allocated = 0;
                    link.used = 0;
                }
            }
            let reply: Reply = read_frame(connection).await?;
            self.received.fetch_add(1, Ordering::Relaxed);
            {
                let mut s = self.state.write().await;
                let now = clock();
                if let Some(live) = s.live.get_mut(&node.id) {
                    live.last_seen = now;
                    live.ping_sent = 0;
                    live.failed = false;
                }
            }
            match reply {
                Reply::Error(message) => Err(io::Error::other(message)),
                other => Ok(other),
            }
        });
        let result = if cleanup {
            operation.await
        } else {
            tokio::select! {result=operation=>result,_=stop.changed()=>Ok(Err(io::Error::new(io::ErrorKind::Interrupted,"cluster is shutting down")))}
        };
        match result {
            Ok(Ok(reply)) => Ok(reply),
            failed => {
                *stream = None;
                self.state
                    .write()
                    .await
                    .links
                    .retain(|_, l| !(l.node == node.id && l.direction == "to"));
                match failed {
                    Ok(Err(e)) => Err(e),
                    Err(_) => Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "cluster peer timed out",
                    )),
                    _ => unreachable!(),
                }
            }
        }
    }
    pub(super) async fn serve(self: Arc<Self>, listener: TcpListener) {
        let permits = Arc::new(tokio::sync::Semaphore::new(256));
        loop {
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _=self.wake.notified()=>{if self.stopped.load(Ordering::Acquire) { break; }},
                accepted=listener.accept()=>{
                    let Ok((stream,_))=accepted else {continue};
                    let Ok(permit)=permits.clone().try_acquire_owned() else {continue};
                    let runtime=self.clone();let _=self.spawn_owned(async move {let _permit=permit;runtime.connection(stream).await;}).await;
                }
            }
        }
    }
    async fn connection(self: Arc<Self>, mut stream: TcpStream) {
        let link = self.next_link.fetch_add(1, Ordering::Relaxed);
        let mut exporting: Option<Exporting> = None;
        let mut identity = None;
        let mut stop = self.stop_signal.subscribe();
        loop {
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            let incoming = tokio::select! {result=timeout(Duration::from_millis(self.config.node_timeout_ms.max(1000)*4),read_frame::<Envelope>(&mut stream))=>result,_=stop.changed()=>break};
            let envelope = match incoming {
                Ok(Ok(value)) => value,
                _ => break,
            };
            if envelope
                .secret
                .as_bytes()
                .ct_eq(self.config.bus_secret.as_bytes())
                .unwrap_u8()
                != 1
            {
                break;
            }
            let sender = &envelope.sender;
            if !topology::valid_id(&sender.id)
                || !topology::valid_id(&sender.shard_id)
                || sender.port == 0
                || sender.bus_port == 0
                || sender.ip.parse::<std::net::IpAddr>().is_err()
                || sender.slots.len() > SLOT_COUNT
            {
                break;
            }
            if identity.as_ref().is_some_and(|id| id != &sender.id) {
                break;
            }
            identity = Some(sender.id.clone());
            {
                let mut s = self.state.write().await;
                if s.durable
                    .blacklist
                    .get(&sender.id)
                    .is_some_and(|until| *until > clock())
                {
                    break;
                }
                if !matches!(&envelope.request, Request::Gossip { meet: true, .. })
                    && !s.durable.nodes.contains_key(&sender.id)
                {
                    break;
                }
                s.links.entry(link).or_insert(LinkInfo {
                    node: sender.id.clone(),
                    direction: "from",
                    created: clock(),
                    allocated: 0,
                    used: 0,
                });
                let now = clock();
                let live = s.live.entry(sender.id.clone()).or_insert(LiveNode {
                    last_seen: now,
                    first_seen: now,
                    ping_sent: 0,
                    reports: HashMap::new(),
                    failed: false,
                });
                live.last_seen = now;
                live.failed = false;
            }
            self.received.fetch_add(1, Ordering::Relaxed);
            let reply = match self
                .receive(&envelope.sender, envelope.request, &mut exporting)
                .await
            {
                Ok(r) => r,
                Err(e) => Reply::Error(e.to_string()),
            };
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            let written = tokio::select! {result=timeout(Duration::from_millis(self.config.node_timeout_ms.max(1000)*2),write_frame(&self,link,&mut stream,&reply))=>matches!(result,Ok(Ok(()))),_=stop.changed()=>false};
            if !written {
                break;
            }
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        self.state.write().await.links.remove(&link);
    }
    async fn receive(
        self: &Arc<Self>,
        sender: &Node,
        request: Request,
        exporting: &mut Option<Exporting>,
    ) -> io::Result<Reply> {
        match request {
            Request::Gossip {
                meet,
                epoch,
                nodes,
                failed,
                blacklist,
            } => {
                self.merge(sender, epoch, nodes, failed, blacklist, meet)
                    .await?;
                let s = self.state.read().await;
                Ok(Reply::Gossip {
                    epoch: s.durable.current_epoch,
                    nodes: s.durable.nodes.values().cloned().collect(),
                    failed: self.suspects(&s),
                    blacklist: s.durable.blacklist.clone(),
                })
            }
            Request::Vote {
                epoch,
                candidate,
                parent,
                offset,
                manual,
            } => {
                let mut s = self.state.write().await;
                let mine = &s.durable.nodes[&s.durable.myself];
                let valid = topology::is_voter(&s, mine)
                    && candidate == sender.id
                    && sender.parent.as_deref() == Some(&parent)
                    && s.durable
                        .nodes
                        .get(&candidate)
                        .is_some_and(|n| n.parent.as_deref() == Some(&parent))
                    && epoch > s.durable.last_vote_epoch
                    && epoch > s.durable.current_epoch.saturating_sub(1)
                    && offset >= sender.offset
                    && (manual || s.live.get(&parent).is_some_and(|live| live.failed));
                if valid {
                    s.durable.current_epoch = s.durable.current_epoch.max(epoch);
                    s.durable.last_vote_epoch = epoch;
                }
                drop(s);
                if valid {
                    self.save().await?;
                }
                Ok(Reply::Vote(valid))
            }
            Request::Freeze { task } => {
                if let Some(task) = task {
                    return self.begin_export(sender, task).await;
                }
                let _admin = self.admin.lock().await;
                {
                    let s = self.state.read().await;
                    if sender.parent.as_deref() != Some(&s.durable.myself) {
                        return Err(invalid("only an attached replica can freeze a primary"));
                    }
                }
                let mut pause = self.pause_owner.lock().await;
                if pause.as_ref().is_some_and(|(owner, _)| owner != &sender.id) {
                    return Err(invalid("primary already paused for another peer"));
                }
                let offset = self.hooks.freeze_writes().await?;
                *pause = Some((
                    sender.id.clone(),
                    clock() + self.config.node_timeout_ms.max(1000) * 8,
                ));
                Ok(Reply::Frozen(offset))
            }
            Request::Unfreeze => {
                self.release_pause(&sender.id).await?;
                Ok(Reply::Ok)
            }
            Request::Globals { task } => {
                let source = self.source_task(sender, &task).await?;
                let Some(payload) = self.snapshot_globals(&source.snapshot_id).await? else {
                    return Ok(Reply::Pending);
                };
                *exporting = Some(Exporting {
                    task,
                    key: ClusterKey {
                        key: vec![],
                        payload,
                    },
                    next: None,
                    key_offset: 0,
                    offset: 0,
                });
                chunk(exporting)
            }
            Request::Export { task, after } => {
                let source = self.source_task(sender, &task).await?;
                let Some(page) = self
                    .snapshot_page(&source.snapshot_id, after.as_deref())
                    .await?
                else {
                    return Ok(Reply::Pending);
                };
                let Some(key) = page.keys.into_iter().next() else {
                    return Ok(Reply::Empty);
                };
                *exporting = Some(Exporting {
                    task,
                    key,
                    next: page.next,
                    key_offset: 0,
                    offset: 0,
                });
                chunk(exporting)
            }
            Request::ExportContinue { task } => {
                let source = self.source_task(sender, &task).await?;
                if source.state == "handoff" {
                    self.touch_pause(&sender.id).await?;
                }
                if exporting.as_ref().is_none_or(|e| e.task != task) {
                    return Err(invalid("no matching snapshot stream"));
                }
                chunk(exporting)
            }
            Request::HandoffFreeze { task } => self.handoff_freeze(sender, &task).await,
            Request::Changes { task, after } => {
                let source = self.source_task(sender, &task).await?;
                if source.state == "handoff" {
                    self.touch_pause(&sender.id).await?;
                }
                if source.state != "handoff" {
                    let mut s = self.state.write().await;
                    if let Some(t) = s.tasks.iter_mut().find(|t| t.id == task) {
                        t.state = "send-stream".into();
                    }
                }
                let changes = self
                    .hooks
                    .migration_changes(&source.stream_history, after, 256 * 1024, &source.ranges)
                    .await?;
                if changes.through < after {
                    return Err(invalid("invalid native migration stream boundary"));
                }
                *exporting = Some(Exporting {
                    task,
                    key: ClusterKey {
                        key: vec![],
                        payload: changes.bytes,
                    },
                    next: Some(changes.through.to_be_bytes().to_vec()),
                    key_offset: 0,
                    offset: 0,
                });
                chunk(exporting)
            }
            Request::Takeover {
                task,
                owner,
                ranges,
            } => {
                let _admin = self.admin.lock().await;
                let snapshot = self.source_task(sender, &task).await?.snapshot_id;
                {
                    let s = self.state.read().await;
                    if owner.id != sender.id
                        || owner.parent.is_some()
                        || !s.tasks.iter().any(|t| {
                            t.id == task && t.dest == sender.id && t.active() && t.ranges == ranges
                        })
                    {
                        return Err(invalid("invalid migration takeover"));
                    }
                }
                {
                    let mut s = self.state.write().await;
                    s.durable.current_epoch = s.durable.current_epoch.max(owner.epoch);
                    s.durable.nodes.insert(owner.id.clone(), owner.clone());
                    for (a, b) in &ranges {
                        for slot in *a..=*b {
                            topology::set_owner(&mut s, slot, Some(&owner.id));
                        }
                    }
                    for t in &mut s.tasks {
                        if t.id == task {
                            t.state = "stream-eof".into();
                        }
                    }
                }
                self.save().await?;
                self.release_pause(&sender.id).await?;
                self.hooks.delete_slots(&ranges).await?;
                self.clear_slot_metrics(&ranges);
                {
                    let mut s = self.state.write().await;
                    for t in &mut s.tasks {
                        if t.id == task {
                            t.state = "completed".into();
                            t.end_time = clock();
                        }
                    }
                }
                self.remove_snapshot(&snapshot).await?;
                self.save().await?;
                Ok(Reply::Ok)
            }
            Request::Follow { target } => {
                let allowed = {
                    let s = self.state.read().await;
                    let me = &s.durable.nodes[&s.durable.myself];
                    sender.id == target.id
                        && sender.shard_id == me.shard_id
                        && sender.epoch > me.epoch
                };
                if !allowed {
                    return Err(invalid("invalid failover winner"));
                }
                self.follow_node(&target).await?;
                {
                    let mut s = self.state.write().await;
                    let id = s.durable.myself.clone();
                    let me = s
                        .durable
                        .nodes
                        .get_mut(&id)
                        .ok_or_else(|| invalid("missing self"))?;
                    me.parent = Some(target.id.clone());
                    me.slots.clear();
                    me.revision += 1;
                    s.durable.nodes.insert(target.id.clone(), target);
                    s.owners = topology::owners(&s.durable);
                }
                self.save().await?;
                self.release_pause(&sender.id).await?;
                Ok(Reply::Ok)
            }
            Request::Cancel { task } => {
                self.cancel_tasks(Some(&task)).await?;
                Ok(Reply::Ok)
            }
            Request::Publish {
                sharded,
                channel,
                message,
            } => {
                if sharded {
                    let valid = {
                        let s = self.state.read().await;
                        s.durable.nodes[&s.durable.myself].shard_id == sender.shard_id
                    };
                    if !valid {
                        return Err(invalid("shard publication from another shard"));
                    }
                    self.hooks.publish_shard(&channel, &message).await?;
                } else {
                    self.hooks.publish_global(&channel, &message).await?;
                }
                Ok(Reply::Ok)
            }
            Request::TaskMetadata { task } => {
                {
                    let mut s = self.state.write().await;
                    let me = &s.durable.nodes[&s.durable.myself];
                    if me.parent.as_ref() != Some(&sender.id) {
                        return Err(invalid(
                            "migration metadata must come from the shard primary",
                        ));
                    }
                    if task.as_ref().is_some_and(|t| {
                        t.ranges.len() > SLOT_COUNT
                            || t.ranges
                                .iter()
                                .any(|(a, b)| a > b || *b as usize >= SLOT_COUNT)
                            || !topology::valid_id(&t.id)
                    }) {
                        return Err(invalid("invalid migration metadata"));
                    }
                    if s.durable.active_task == task {
                        return Ok(Reply::Ok);
                    }
                    s.durable.active_task = task;
                }
                self.save().await?;
                Ok(Reply::Ok)
            }
        }
    }
    pub(super) async fn merge(
        &self,
        sender: &Node,
        epoch: u64,
        nodes: Vec<Node>,
        failed: Vec<String>,
        blacklist: BTreeMap<String, u64>,
        allow_join: bool,
    ) -> io::Result<()> {
        if nodes.len() > 1024 || failed.len() > 1024 || blacklist.len() > 1024 {
            return Err(invalid("cluster gossip exceeds bound"));
        }
        let mut changed = false;
        let mut demote = None;
        {
            let mut s = self.state.write().await;
            let now = clock();
            let mine = s.durable.myself.clone();
            if !allow_join && !s.durable.nodes.contains_key(&sender.id) {
                return Err(invalid("peer was forgotten while gossip was in flight"));
            }
            let was = s.durable.current_epoch;
            s.durable.current_epoch = was.max(epoch);
            changed |= was != s.durable.current_epoch;
            s.durable.blacklist.retain(|_, until| *until > now);
            for (id, until) in blacklist {
                if id != mine
                    && s.durable.nodes[&mine].parent.as_ref() != Some(&id)
                    && topology::valid_id(&id)
                    && until > now
                    && until <= now + 120_000
                    && (s.durable.blacklist.contains_key(&id) || s.durable.blacklist.len() < 1024)
                {
                    s.durable.blacklist.insert(id.clone(), until);
                    s.durable.nodes.remove(&id);
                    s.durable.importing.retain(|_, peer| peer != &id);
                    s.durable.migrating.retain(|_, peer| peer != &id);
                    changed = true;
                }
            }
            for node in nodes.into_iter().chain(std::iter::once(sender.clone())) {
                if node.id == mine || s.durable.blacklist.contains_key(&node.id) {
                    continue;
                }
                if !topology::valid_id(&node.id)
                    || !topology::valid_id(&node.shard_id)
                    || node.port == 0
                    || node.bus_port == 0
                    || node.ip.parse::<std::net::IpAddr>().is_err()
                    || node.slots.len() > SLOT_COUNT
                    || node
                        .slots
                        .iter()
                        .any(|(a, b)| a > b || *b as usize >= SLOT_COUNT)
                    || (node.parent.is_some() && !node.slots.is_empty())
                {
                    return Err(invalid("malformed gossip node"));
                }
                if node.slots.windows(2).any(|pair| pair[0].1 >= pair[1].0)
                    || node
                        .parent
                        .as_ref()
                        .is_some_and(|id| id == &node.id || !topology::valid_id(id))
                {
                    return Err(invalid("malformed gossip topology"));
                }
                if s.durable.nodes.len() >= 1024 && !s.durable.nodes.contains_key(&node.id) {
                    continue;
                }
                if s.durable
                    .nodes
                    .get(&node.id)
                    .is_none_or(|old| (node.epoch, node.revision) > (old.epoch, old.revision))
                {
                    s.live.entry(node.id.clone()).or_insert(LiveNode {
                        last_seen: 0,
                        first_seen: now,
                        ping_sent: 0,
                        reports: HashMap::new(),
                        failed: false,
                    });
                    s.durable.nodes.insert(node.id.clone(), node);
                    changed = true;
                } else if let Some(old) = s.durable.nodes.get_mut(&node.id) {
                    old.offset = old.offset.max(node.offset);
                }
            }
            if let Some(live) = s.live.get_mut(&sender.id) {
                live.last_seen = now;
                live.failed = false;
            }
            if changed {
                s.owners = topology::owners(&s.durable);
            }
            let reporter = s
                .durable
                .nodes
                .get(&sender.id)
                .is_some_and(|node| topology::is_voter(&s, node));
            if reporter {
                for id in failed {
                    if let Some(l) = s.live.get_mut(&id) {
                        l.reports.insert(sender.id.clone(), now);
                    }
                }
            }
            let voters = s
                .durable
                .nodes
                .values()
                .filter(|node| topology::is_voter(&s, node))
                .count();
            let local_vote = usize::from(topology::is_voter(&s, &s.durable.nodes[&mine]));
            for (id, live) in &mut s.live {
                live.reports
                    .retain(|_, at| now.saturating_sub(*at) < self.config.node_timeout_ms * 2);
                if id != &mine
                    && now.saturating_sub(live.last_seen.max(live.first_seen))
                        > self.config.node_timeout_ms
                {
                    live.failed = live.reports.len() + local_vote > voters / 2;
                }
            }
            if changed {
                let me = &s.durable.nodes[&mine];
                if me.parent.is_none()
                    && let Some(winner) = s.durable.nodes.values().find(|n| {
                        n.id != mine
                            && n.shard_id == me.shard_id
                            && n.parent.is_none()
                            && n.epoch > me.epoch
                    })
                {
                    demote = Some(winner.clone());
                }
                // Nonzero primary epochs are unique, even for disjoint claims.
                let (primary, my_epoch) = {
                    let me = &s.durable.nodes[&mine];
                    (me.parent.is_none(), me.epoch)
                };
                if primary
                    && my_epoch != 0
                    && s.durable
                        .nodes
                        .values()
                        .any(|n| n.id > mine && n.parent.is_none() && n.epoch == my_epoch)
                {
                    s.durable.current_epoch = s
                        .durable
                        .current_epoch
                        .checked_add(1)
                        .ok_or_else(|| invalid("cluster epoch exhausted"))?;
                    let epoch = s.durable.current_epoch;
                    let me = s
                        .durable
                        .nodes
                        .get_mut(&mine)
                        .ok_or_else(|| invalid("missing self"))?;
                    me.epoch = epoch;
                    me.revision += 1;
                    s.owners = topology::owners(&s.durable);
                }
                topology::refresh_local_claims(&mut s)?;
            }
        }
        if changed {
            self.save().await?;
        }
        if let Some(winner) = demote {
            self.follow_node(&winner).await?;
            {
                let mut s = self.state.write().await;
                let id = s.durable.myself.clone();
                let me = s
                    .durable
                    .nodes
                    .get_mut(&id)
                    .ok_or_else(|| invalid("missing self"))?;
                me.parent = Some(winner.id);
                me.slots.clear();
                me.revision += 1;
                s.owners = topology::owners(&s.durable);
            }
            self.save().await?;
        }
        self.refresh_health().await;
        Ok(())
    }
    fn suspects(&self, s: &State) -> Vec<String> {
        s.live
            .iter()
            .filter(|(id, l)| {
                **id != s.durable.myself
                    && clock().saturating_sub(l.last_seen.max(l.first_seen))
                        > self.config.node_timeout_ms
            })
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub(super) async fn gossip(self: Arc<Self>) {
        let mut interval = tokio::time::interval(Duration::from_millis(
            (self.config.node_timeout_ms / 4).clamp(100, 1000),
        ));
        loop {
            tokio::select! {_=interval.tick()=>{},_=self.wake.notified()=>{if self.stopped.load(Ordering::Acquire) {break;}}}
            if self.stopped.load(Ordering::Acquire) {
                break;
            }
            if let Ok(progress) = self.hooks.replication_progress().await {
                let mut s = self.state.write().await;
                let id = s.durable.myself.clone();
                if let Some(me) = s.durable.nodes.get_mut(&id) {
                    if me.parent.is_some() && progress.upstream_connected {
                        self.replica_last_connected
                            .store(clock(), Ordering::Relaxed);
                    }
                    me.offset = if me.parent.is_some() {
                        progress.processed
                    } else {
                        progress.primary_offset
                    };
                }
            }
            let (nodes, epoch, failed, blacklist, pending) = {
                let s = self.state.read().await;
                (
                    s.durable.nodes.values().cloned().collect::<Vec<_>>(),
                    s.durable.current_epoch,
                    self.suspects(&s),
                    s.durable.blacklist.clone(),
                    s.pending.clone(),
                )
            };
            let me = { self.state.read().await.durable.myself.clone() };
            let handshake = if pending.is_empty() {
                None
            } else {
                Some(Arc::new(Request::Gossip {
                    meet: true,
                    epoch,
                    nodes: nodes.clone(),
                    failed: failed.clone(),
                    blacklist: blacklist.clone(),
                }))
            };
            let heartbeat = Arc::new(Request::Gossip {
                meet: false,
                epoch,
                nodes: nodes.clone(),
                failed,
                blacklist,
            });
            for group in nodes
                .iter()
                .filter(|n| n.id != me)
                .cloned()
                .collect::<Vec<_>>()
                .chunks(16)
            {
                let mut jobs = tokio::task::JoinSet::new();
                for node in group {
                    let runtime = self.clone();
                    let node = node.clone();
                    let request = heartbeat.clone();
                    jobs.spawn(async move {
                        if let Ok(Reply::Gossip {
                            epoch,
                            nodes,
                            failed,
                            blacklist,
                        }) = runtime.rpc(&node, request).await
                        {
                            let _ = runtime
                                .merge(&node, epoch, nodes, failed, blacklist, false)
                                .await;
                        }
                        let task = {
                            let s = runtime.state.read().await;
                            if node.parent.as_ref() == Some(&s.durable.myself) {
                                Some(s.tasks.iter().find(|task| task.active()).cloned())
                            } else {
                                None
                            }
                        };
                        if let Some(task) = task {
                            let _ = runtime.rpc(&node, Request::TaskMetadata { task }).await;
                        }
                    });
                }
                while jobs.join_next().await.is_some() {}
            }
            for (ip, port) in pending {
                let temporary = Node {
                    id: format!("meet:{ip}:{port}"),
                    shard_id: String::new(),
                    ip: ip.clone(),
                    port: 1,
                    bus_port: port,
                    parent: None,
                    epoch: 0,
                    revision: 0,
                    slots: vec![],
                    offset: 0,
                };
                let Some(request) = handshake.clone() else {
                    continue;
                };
                if let Ok(Reply::Gossip {
                    epoch,
                    nodes,
                    failed,
                    blacklist,
                }) = self.rpc(&temporary, request).await
                    && let Some(sender) = nodes
                        .iter()
                        .find(|n| n.ip == ip && n.bus_port == port)
                        .cloned()
                {
                    let pending = self
                        .state
                        .read()
                        .await
                        .pending
                        .contains(&(ip.clone(), port));
                    if pending {
                        let _ = self
                            .merge(&sender, epoch, nodes, failed, blacklist, true)
                            .await;
                    }
                    {
                        let mut s = self.state.write().await;
                        s.pending.retain(|p| p != &(ip.clone(), port));
                        s.live.remove(&temporary.id);
                        s.links.retain(|_, l| l.node != temporary.id);
                    }
                    self.peers.lock().await.remove(&temporary.id);
                }
            }
            self.refresh_health().await;
            self.migration_tick().await;
            let election = {
                let s = self.state.read().await;
                let mine = &s.durable.nodes[&s.durable.myself];
                mine.parent
                    .as_ref()
                    .is_some_and(|id| s.live.get(id).is_some_and(|l| l.failed))
            };
            if election {
                let _ = self.failover("FORCE", true).await;
            }
        }
    }
    pub async fn forward_shard(&self, channel: &[u8], message: &[u8]) -> io::Result<()> {
        self.forward_publication(true, channel, message).await
    }
    pub async fn forward_publish(&self, channel: &[u8], message: &[u8]) -> io::Result<()> {
        self.forward_publication(false, channel, message).await
    }
    async fn forward_publication(
        &self,
        sharded: bool,
        channel: &[u8],
        message: &[u8],
    ) -> io::Result<()> {
        let nodes = {
            let s = self.state.read().await;
            let mine = &s.durable.nodes[&s.durable.myself];
            s.durable
                .nodes
                .values()
                .filter(|n| n.id != mine.id && (!sharded || n.shard_id == mine.shard_id))
                .cloned()
                .collect::<Vec<_>>()
        };
        let request = Arc::new(Request::Publish {
            sharded,
            channel: channel.to_vec(),
            message: message.to_vec(),
        });
        let mut failure = None;
        for node in nodes {
            if let Err(error) = self.rpc(&node, request.clone()).await {
                failure.get_or_insert(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}
fn chunk(exporting: &mut Option<Exporting>) -> io::Result<Reply> {
    let e = exporting
        .as_mut()
        .ok_or_else(|| invalid("missing snapshot stream"))?;
    let key_end = e.key_offset.saturating_add(CHUNK).min(e.key.key.len());
    let key = e.key.key[e.key_offset..key_end].to_vec();
    e.key_offset = key_end;
    let key_done = key_end == e.key.key.len();
    let available = if key_done { CHUNK - key.len() } else { 0 };
    let end = e.offset.saturating_add(available).min(e.key.payload.len());
    let data = e.key.payload[e.offset..end].to_vec();
    e.offset = end;
    let done = key_done && end == e.key.payload.len();
    let reply = Reply::Chunk {
        key,
        key_done,
        data,
        done,
        next: if done { e.next.clone() } else { None },
    };
    if done {
        *exporting = None;
    }
    Ok(reply)
}
