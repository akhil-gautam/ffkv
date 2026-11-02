use super::*;

impl ClusterRuntime {
    pub(super) async fn handle(
        self: &Arc<Self>,
        args: &[Bytes],
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<Resp> {
        if args.len() < 2 {
            return Err(invalid("wrong number of arguments for 'cluster' command"));
        }
        let command = text(&args[1])?.to_ascii_uppercase();
        match command.as_str() {
            "MYID" | "MYSHARDID" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                let me = &s.durable.nodes[&s.durable.myself];
                Ok(bulk(if command == "MYID" {
                    me.id.clone()
                } else {
                    me.shard_id.clone()
                }))
            }
            "KEYSLOT" => {
                arity(args, 3)?;
                Ok(Resp::Integer(i64::from(key_slot(&args[2]))))
            }
            "INFO" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                let assigned = s.owners.iter().filter(|v| v.is_some()).count();
                let failed = s
                    .owners
                    .iter()
                    .flatten()
                    .filter(|id| s.live.get(*id).is_some_and(|l| l.failed))
                    .count();
                let pfail = s
                    .owners
                    .iter()
                    .flatten()
                    .filter(|id| {
                        s.live.get(*id).is_some_and(|l| {
                            !l.failed
                                && clock().saturating_sub(l.last_seen.max(l.first_seen))
                                    > self.config.node_timeout_ms
                        }) && **id != s.durable.myself
                    })
                    .count();
                let size = s
                    .durable
                    .nodes
                    .values()
                    .filter(|node| topology::is_voter(&s, node))
                    .count();
                let me = &s.durable.nodes[&s.durable.myself];
                Ok(verbatim(format!(
                    "cluster_state:{}\r\ncluster_slots_assigned:{assigned}\r\ncluster_slots_ok:{}\r\ncluster_slots_pfail:{pfail}\r\ncluster_slots_fail:{failed}\r\ncluster_known_nodes:{}\r\ncluster_size:{size}\r\ncluster_current_epoch:{}\r\ncluster_my_epoch:{}\r\ncluster_stats_messages_sent:{}\r\ncluster_stats_messages_received:{}\r\ntotal_cluster_links_buffer_limit_exceeded:0\r\n",
                    if self.healthy(&s) { "ok" } else { "fail" },
                    assigned - failed - pfail,
                    s.durable.nodes.len(),
                    s.durable.current_epoch,
                    me.epoch,
                    self.sent.load(Ordering::Relaxed),
                    self.received.load(Ordering::Relaxed)
                )))
            }
            "NODES" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                let mut lines = s
                    .durable
                    .nodes
                    .values()
                    .map(|n| topology::node_line(&s, n, self.config.node_timeout_ms))
                    .collect::<Vec<_>>()
                    .join("\n");
                lines.push('\n');
                Ok(verbatim(lines))
            }
            "SLOTS" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                Ok(array(
                    topology::slot_ranges(&s)
                        .into_iter()
                        .map(|(start, end, id)| {
                            let owner = &s.durable.nodes[&id];
                            let mut fields = vec![
                                Resp::Integer(i64::from(start)),
                                Resp::Integer(i64::from(end)),
                                descriptor(owner),
                            ];
                            for replica in s
                                .durable
                                .nodes
                                .values()
                                .filter(|n| n.parent.as_ref() == Some(&id))
                            {
                                fields.push(descriptor(replica));
                            }
                            array(fields)
                        })
                        .collect(),
                ))
            }
            "SHARDS" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                let mut shards = BTreeMap::<&str, Vec<&Node>>::new();
                for node in s.durable.nodes.values() {
                    shards.entry(&node.shard_id).or_default().push(node);
                }
                Ok(array(
                    shards
                        .into_values()
                        .map(|nodes| {
                            let slots = nodes
                                .iter()
                                .find(|n| n.parent.is_none())
                                .map(|n| {
                                    topology::slot_ranges(&s)
                                        .into_iter()
                                        .filter(|(_, _, id)| id == &n.id)
                                        .flat_map(|(a, b, _)| {
                                            [
                                                Resp::Integer(i64::from(a)),
                                                Resp::Integer(i64::from(b)),
                                            ]
                                        })
                                        .collect()
                                })
                                .unwrap_or_default();
                            Resp::Map(vec![
                                (bulk("slots"), array(slots)),
                                (
                                    bulk("nodes"),
                                    array(
                                        nodes
                                            .into_iter()
                                            .map(|n| {
                                                topology::node_map(
                                                    &s,
                                                    n,
                                                    self.config.node_timeout_ms,
                                                )
                                            })
                                            .collect(),
                                    ),
                                ),
                            ])
                        })
                        .collect(),
                ))
            }
            "LINKS" => {
                arity(args, 2)?;
                let s = self.state.read().await;
                Ok(array(
                    s.links
                        .values()
                        .map(|l| {
                            Resp::Map(vec![
                                (bulk("direction"), bulk(l.direction)),
                                (bulk("node"), bulk(l.node.clone())),
                                (
                                    bulk("create-time"),
                                    Resp::Integer(l.created.min(i64::MAX as u64) as i64),
                                ),
                                (bulk("events"), bulk(if l.used > 0 { "rw" } else { "r" })),
                                (
                                    bulk("send-buffer-allocated"),
                                    Resp::Integer(l.allocated as i64),
                                ),
                                (bulk("send-buffer-used"), Resp::Integer(l.used as i64)),
                            ])
                        })
                        .collect(),
                ))
            }
            "COUNTKEYSINSLOT" | "GETKEYSINSLOT" => {
                arity(args, if command == "COUNTKEYSINSLOT" { 3 } else { 4 })?;
                let slot = slot_number(&args[2])?;
                let visible = {
                    let s = self.state.read().await;
                    let me = &s.durable.nodes[&s.durable.myself];
                    let owner = s.owners[slot as usize].as_ref();
                    owner == Some(&me.id)
                        || (me.parent.is_some() && owner == me.parent.as_ref())
                        || s.durable.importing.contains_key(&slot)
                };
                if command == "COUNTKEYSINSLOT" {
                    if !visible {
                        return Ok(Resp::Integer(0));
                    }
                    let counts = self.counts(hooks).await?;
                    Ok(Resp::Integer(
                        counts[slot as usize].min(i64::MAX as u64) as i64
                    ))
                } else {
                    let count = usize::try_from(number(&args[3])?)
                        .map_err(|_| invalid("count out of range"))?;
                    if count == 0 || !visible {
                        return Ok(array(vec![]));
                    }
                    let keys = hooks.keys(slot, count).await?;
                    if keys.len() > count {
                        return Err(invalid("cluster key hook exceeded limit"));
                    }
                    Ok(array(keys.into_iter().map(bulk).collect()))
                }
            }
            "REPLICAS" | "SLAVES" => {
                arity(args, 3)?;
                let id = text(&args[2])?;
                let s = self.state.read().await;
                let target = s
                    .durable
                    .nodes
                    .get(id)
                    .ok_or_else(|| invalid("Unknown node"))?;
                if target.parent.is_some() {
                    return Err(invalid("The specified node is not a master"));
                }
                Ok(array(
                    s.durable
                        .nodes
                        .values()
                        .filter(|n| n.parent.as_deref() == Some(id))
                        .map(|n| bulk(topology::node_line(&s, n, self.config.node_timeout_ms)))
                        .collect(),
                ))
            }
            "COUNT-FAILURE-REPORTS" => {
                arity(args, 3)?;
                let id = text(&args[2])?;
                let s = self.state.read().await;
                if !s.durable.nodes.contains_key(id) {
                    return Err(invalid("Unknown node"));
                }
                let now = clock();
                let count = s.live.get(id).map_or(0, |l| {
                    l.reports
                        .values()
                        .filter(|at| now.saturating_sub(**at) < self.config.node_timeout_ms * 2)
                        .count()
                });
                Ok(Resp::Integer(count as i64))
            }
            "ADDSLOTS" | "ADDSLOTSRANGE" | "DELSLOTS" | "DELSLOTSRANGE" => {
                let slots = topology::parse_ranges(&args[2..], command.ends_with("RANGE"))?;
                let _admin = self.admin.lock().await;
                let add = command.starts_with("ADD");
                {
                    let s = self.state.read().await;
                    if s.durable.nodes[&s.durable.myself].parent.is_some() {
                        return Err(invalid("Please use a master"));
                    }
                    for slot in &slots {
                        if add && s.owners[*slot as usize].is_some() {
                            return Err(invalid(&format!("Slot {slot} is already busy")));
                        }
                        if !add && s.owners[*slot as usize].is_none() {
                            return Err(invalid(&format!("Slot {slot} is already unassigned")));
                        }
                    }
                }
                let tasks = {
                    let s = self.state.read().await;
                    s.tasks
                        .iter()
                        .filter(|t| t.active() && slots.iter().any(|slot| t.contains(*slot)))
                        .map(|t| t.id.clone())
                        .collect::<Vec<_>>()
                };
                for id in tasks {
                    self.cancel_tasks_with_hooks(Some(&id), hooks).await?;
                }
                {
                    let mut s = self.state.write().await;
                    let id = s.durable.myself.clone();
                    for slot in slots {
                        topology::set_owner(
                            &mut s,
                            slot,
                            if add { Some(id.as_str()) } else { None },
                        );
                        s.durable.importing.remove(&slot);
                        s.durable.migrating.remove(&slot);
                    }
                }
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "FLUSHSLOTS" => {
                arity(args, 2)?;
                let _admin = self.admin.lock().await;
                if hooks.database_size().await? != 0 {
                    return Err(invalid("DB must be empty to perform CLUSTER FLUSHSLOTS"));
                }
                {
                    let mut s = self.state.write().await;
                    let id = s.durable.myself.clone();
                    let slots = s.durable.nodes[&id]
                        .slots
                        .iter()
                        .flat_map(|(a, b)| *a..=*b)
                        .collect::<Vec<_>>();
                    for slot in slots {
                        topology::set_owner(&mut s, slot, None);
                    }
                }
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "MEET" => {
                if args.len() != 4 && args.len() != 5 {
                    return Err(invalid("wrong number of arguments"));
                }
                let ip = text(&args[2])?
                    .parse::<std::net::IpAddr>()
                    .map_err(|_| invalid("Invalid node address"))?
                    .to_string();
                let client_port = port(&args[3])?;
                let bus = if args.len() == 5 {
                    port(&args[4])?
                } else {
                    client_port
                        .checked_add(10_000)
                        .ok_or_else(|| invalid("Invalid cluster bus port"))?
                };
                let mut s = self.state.write().await;
                if !s.pending.contains(&(ip.clone(), bus))
                    && !s
                        .durable
                        .nodes
                        .values()
                        .any(|n| n.ip == ip && n.bus_port == bus)
                {
                    if s.pending.len() >= 1024 {
                        return Err(invalid("too many pending cluster peers"));
                    }
                    s.pending.push((ip, bus));
                }
                drop(s);
                self.wake.notify_one();
                Ok(Resp::Simple("OK"))
            }
            "SAVECONFIG" => {
                arity(args, 2)?;
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "SET-CONFIG-EPOCH" => {
                arity(args, 3)?;
                let epoch = number(&args[2])?;
                if epoch > i64::MAX as u64 {
                    return Err(invalid("Invalid config epoch"));
                }
                let _admin = self.admin.lock().await;
                {
                    let mut s = self.state.write().await;
                    let id = s.durable.myself.clone();
                    if s.durable.nodes.len() != 1 || s.durable.nodes[&id].epoch != 0 {
                        return Err(invalid(
                            "The user can assign a config epoch only when the node does not know any other node and its config epoch is zero",
                        ));
                    }
                    s.durable.current_epoch = s.durable.current_epoch.max(epoch);
                    let me = s
                        .durable
                        .nodes
                        .get_mut(&id)
                        .ok_or_else(|| invalid("missing self"))?;
                    me.epoch = epoch;
                    me.revision += 1;
                }
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "BUMPEPOCH" => {
                arity(args, 2)?;
                let _admin = self.admin.lock().await;
                let (bumped, epoch) = {
                    let mut s = self.state.write().await;
                    let id = s.durable.myself.clone();
                    let max = s.durable.nodes.values().map(|n| n.epoch).max().unwrap_or(0);
                    let old = s.durable.nodes[&id].epoch;
                    let bumped = old == 0 || old < max;
                    if bumped {
                        s.durable.current_epoch =
                            s.durable
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
                    }
                    (bumped, s.durable.nodes[&id].epoch)
                };
                self.save().await?;
                Ok(Resp::Status(Bytes::from(format!(
                    "{} {epoch}",
                    if bumped { "BUMPED" } else { "STILL" }
                ))))
            }
            "FORGET" => {
                arity(args, 3)?;
                let id = text(&args[2])?.to_owned();
                let _admin = self.admin.lock().await;
                {
                    let s = self.state.read().await;
                    if id == s.durable.myself {
                        return Err(invalid("I tried hard but I can't forget myself"));
                    }
                    if s.durable.nodes[&s.durable.myself].parent.as_ref() == Some(&id) {
                        return Err(invalid("Can't forget my master"));
                    }
                    if !s.durable.nodes.contains_key(&id)
                        && !s.durable.blacklist.get(&id).is_some_and(|t| *t > clock())
                    {
                        return Err(invalid("Unknown node"));
                    }
                }
                {
                    let mut s = self.state.write().await;
                    s.durable.blacklist.retain(|_, until| *until > clock());
                    if !s.durable.blacklist.contains_key(&id) && s.durable.blacklist.len() >= 1024 {
                        return Err(invalid("cluster forget blacklist is full"));
                    }
                }
                let tasks = {
                    let s = self.state.read().await;
                    s.tasks
                        .iter()
                        .filter(|t| t.active() && (t.source == id || t.dest == id))
                        .map(|t| t.id.clone())
                        .collect::<Vec<_>>()
                };
                for task in tasks {
                    self.cancel_tasks_with_hooks(Some(&task), hooks).await?;
                }
                {
                    let mut s = self.state.write().await;
                    s.durable.nodes.remove(&id);
                    s.live.remove(&id);
                    s.durable.importing.retain(|_, v| v != &id);
                    s.durable.migrating.retain(|_, v| v != &id);
                    s.durable.blacklist.insert(id.clone(), clock() + 60_000);
                    s.owners = topology::owners(&s.durable);
                    s.links.retain(|_, l| l.node != id);
                }
                self.peers.lock().await.remove(&id);
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "REPLICATE" => {
                arity(args, 3)?;
                let id = text(&args[2])?.to_owned();
                let _admin = self.admin.lock().await;
                let target = {
                    let s = self.state.read().await;
                    if id == s.durable.myself {
                        return Err(invalid("Can't replicate myself"));
                    }
                    let target = s
                        .durable
                        .nodes
                        .get(&id)
                        .ok_or_else(|| invalid("Unknown node"))?
                        .clone();
                    if target.parent.is_some() {
                        return Err(invalid("I can only replicate a master, not a replica"));
                    }
                    let me = &s.durable.nodes[&s.durable.myself];
                    if me.parent.is_none() && !me.slots.is_empty() {
                        return Err(invalid(
                            "To set a master the node must be empty and without assigned slots",
                        ));
                    }
                    target
                };
                let was_primary = {
                    let s = self.state.read().await;
                    s.durable.nodes[&s.durable.myself].parent.is_none()
                };
                if was_primary && hooks.database_size().await? != 0 {
                    return Err(invalid(
                        "To set a master the node must be empty and without assigned slots",
                    ));
                }
                self.follow_node_with_hooks(&target, hooks).await?;
                {
                    let mut s = self.state.write().await;
                    let mine = s.durable.myself.clone();
                    let me = s
                        .durable
                        .nodes
                        .get_mut(&mine)
                        .ok_or_else(|| invalid("missing self"))?;
                    me.parent = Some(id);
                    me.shard_id = target.shard_id;
                    me.slots.clear();
                    me.revision += 1;
                }
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "RESET" => {
                if args.len() > 3 {
                    return Err(invalid("wrong number of arguments"));
                }
                let mode = if args.len() == 3 {
                    text(&args[2])?.to_ascii_uppercase()
                } else {
                    "SOFT".into()
                };
                if mode != "HARD" && mode != "SOFT" {
                    return Err(invalid("syntax error"));
                }
                let _admin = self.admin.lock().await;
                let old = {
                    let s = self.state.read().await;
                    s.durable.nodes[&s.durable.myself].clone()
                };
                if old.parent.is_none() && hooks.database_size().await? != 0 {
                    return Err(invalid(
                        "CLUSTER RESET can't be called with master nodes containing keys",
                    ));
                }
                self.cancel_tasks_with_hooks(None, hooks).await?;
                if old.parent.is_some() {
                    hooks.promote(None).await?;
                    hooks.clear().await?;
                }
                self.clear_slot_metrics(&[(0, 16383)]);
                {
                    let mut s = self.state.write().await;
                    let hard = mode == "HARD";
                    let id = if hard { random_id() } else { old.id.clone() };
                    let mut me = old;
                    me.id = id.clone();
                    me.parent = None;
                    me.slots.clear();
                    me.revision += 1;
                    if hard {
                        me.shard_id = random_id();
                        me.epoch = 0;
                        s.durable.current_epoch = 0;
                        s.durable.last_vote_epoch = 0;
                    }
                    s.durable.myself = id.clone();
                    s.durable.nodes = BTreeMap::from([(id, me)]);
                    s.durable.importing.clear();
                    s.durable.migrating.clear();
                    s.durable.blacklist.clear();
                    s.pending.clear();
                    s.live.clear();
                    s.links.clear();
                    s.owners.fill(None);
                    s.tasks.clear();
                }
                self.peers.lock().await.clear();
                self.save().await?;
                Ok(Resp::Simple("OK"))
            }
            "SETSLOT" => self.set_slot(args, hooks).await,
            "SLOT-STATS" => self.slot_stats(args, hooks).await,
            "MIGRATION" => self.migration_command(args, hooks).await,
            "FAILOVER" => {
                if args.len() > 3 {
                    return Err(invalid("wrong number of arguments"));
                }
                let mode = if args.len() == 3 {
                    text(&args[2])?.to_ascii_uppercase()
                } else {
                    String::new()
                };
                if !matches!(mode.as_str(), "" | "FORCE" | "TAKEOVER") {
                    return Err(invalid("syntax error"));
                }
                {
                    let s = self.state.read().await;
                    if s.durable.nodes[&s.durable.myself].parent.is_none() {
                        return Err(invalid("You should send CLUSTER FAILOVER to a replica"));
                    }
                }
                if mode.is_empty() && !hooks.replication_progress().await?.upstream_connected {
                    return Err(invalid(
                        "Master is down or failed, please use CLUSTER FAILOVER FORCE",
                    ));
                }
                let runtime = self.clone();
                self.spawn_owned(async move {
                    let _ = runtime.failover(&mode, false).await;
                })
                .await?;
                Ok(Resp::Simple("OK"))
            }
            _ => Err(invalid("Unknown CLUSTER subcommand")),
        }
    }
    async fn counts(&self, hooks: &dyn ClusterCommandHooks) -> io::Result<Vec<u64>> {
        let counts = hooks.slot_key_counts().await?;
        if counts.len() != SLOT_COUNT {
            return Err(invalid("cluster slot counts hook returned invalid length"));
        }
        Ok(counts)
    }
    async fn set_slot(&self, args: &[Bytes], hooks: &dyn ClusterCommandHooks) -> io::Result<Resp> {
        if args.len() < 4 {
            return Err(invalid("wrong number of arguments"));
        }
        let slot = slot_number(&args[2])?;
        let action = text(&args[3])?.to_ascii_uppercase();
        arity(args, if action == "STABLE" { 4 } else { 5 })?;
        if !matches!(
            action.as_str(),
            "STABLE" | "IMPORTING" | "MIGRATING" | "NODE"
        ) {
            return Err(invalid("syntax error"));
        }
        let id = if args.len() == 5 {
            Some(text(&args[4])?.to_owned())
        } else {
            None
        };
        let _admin = self.admin.lock().await;
        {
            let s = self.state.read().await;
            let mine = &s.durable.myself;
            if s.durable.nodes[mine].parent.is_some() {
                return Err(invalid("Please use a master node"));
            }
            if s.tasks.iter().any(|t| t.active() && t.contains(slot)) {
                return Err(invalid("slot has an active atomic migration"));
            }
            if let Some(id) = &id {
                let target = s
                    .durable
                    .nodes
                    .get(id)
                    .ok_or_else(|| invalid("Unknown node"))?;
                if target.parent.is_some() {
                    return Err(invalid("Target node is not a master"));
                }
                if action == "MIGRATING"
                    && (id == mine || s.owners[slot as usize].as_ref() != Some(mine))
                {
                    return Err(invalid("I'm not the owner of this hash slot"));
                }
                if action == "IMPORTING"
                    && (id == mine || s.owners[slot as usize].as_ref() == Some(mine))
                {
                    return Err(invalid("I'm already the owner of this hash slot"));
                }
            }
        }
        if action == "NODE" {
            let s = self.state.read().await;
            let mine = &s.durable.myself;
            let relinquish =
                id.as_ref() != Some(mine) && s.owners[slot as usize].as_ref() == Some(mine);
            drop(s);
            if relinquish && self.counts(hooks).await?[slot as usize] > 0 {
                return Err(invalid(
                    "Can't assign hashslot while I still hold keys for it",
                ));
            }
        }
        {
            let mut s = self.state.write().await;
            match action.as_str() {
                "STABLE" => {
                    s.durable.importing.remove(&slot);
                    s.durable.migrating.remove(&slot);
                }
                "IMPORTING" => {
                    s.durable
                        .importing
                        .insert(slot, id.ok_or_else(|| invalid("missing source"))?);
                }
                "MIGRATING" => {
                    s.durable
                        .migrating
                        .insert(slot, id.ok_or_else(|| invalid("missing destination"))?);
                }
                "NODE" => {
                    let id = id.ok_or_else(|| invalid("missing node"))?;
                    if id == s.durable.myself && s.durable.importing.contains_key(&slot) {
                        let max = s.durable.nodes.values().map(|n| n.epoch).max().unwrap_or(0);
                        s.durable.current_epoch = s.durable.current_epoch.max(max) + 1;
                        let epoch = s.durable.current_epoch;
                        let me = s
                            .durable
                            .nodes
                            .get_mut(&id)
                            .ok_or_else(|| invalid("missing self"))?;
                        me.epoch = epoch;
                    }
                    topology::set_owner(&mut s, slot, Some(&id));
                    s.durable.importing.remove(&slot);
                    s.durable.migrating.remove(&slot);
                }
                _ => unreachable!(),
            }
        }
        self.save().await?;
        Ok(Resp::Simple("OK"))
    }
    async fn slot_stats(
        &self,
        args: &[Bytes],
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<Resp> {
        if args.len() < 4 {
            return Err(invalid("wrong number of arguments"));
        }
        let kind = text(&args[2])?.to_ascii_uppercase();
        let mut metric = "key-count".to_owned();
        let mut desc = true;
        let mut limit = SLOT_COUNT;
        let mut range = (0u16, 16383u16);
        if kind == "SLOTSRANGE" {
            arity(args, 5)?;
            range = (slot_number(&args[3])?, slot_number(&args[4])?);
            if range.0 > range.1 {
                return Err(invalid("start slot exceeds end slot"));
            }
        } else if kind == "ORDERBY" {
            metric = text(&args[3])?.to_ascii_lowercase();
            let mut seen_limit = false;
            let mut seen_direction = false;
            let mut i = 4;
            while i < args.len() {
                if args[i].eq_ignore_ascii_case(b"LIMIT") {
                    if seen_limit || i + 1 >= args.len() {
                        return Err(invalid("syntax error"));
                    }
                    limit = usize::try_from(number(&args[i + 1])?)
                        .map_err(|_| invalid("limit out of range"))?;
                    if limit == 0 || limit > SLOT_COUNT {
                        return Err(invalid("limit must be between 1 and 16384"));
                    }
                    seen_limit = true;
                    i += 2;
                } else if args[i].eq_ignore_ascii_case(b"ASC")
                    || args[i].eq_ignore_ascii_case(b"DESC")
                {
                    if seen_direction {
                        return Err(invalid("syntax error"));
                    }
                    desc = args[i].eq_ignore_ascii_case(b"DESC");
                    seen_direction = true;
                    i += 1;
                } else {
                    return Err(invalid("syntax error"));
                }
            }
        } else {
            return Err(invalid("syntax error"));
        }
        if !matches!(
            metric.as_str(),
            "key-count" | "cpu-usec" | "memory-bytes" | "network-bytes-in" | "network-bytes-out"
        ) {
            return Err(invalid("Unrecognized metric"));
        }
        if (metric == "cpu-usec" && !self.config.cpu_stats)
            || (metric == "memory-bytes" && !self.config.memory_stats)
            || (metric.starts_with("network-") && !self.config.network_stats)
        {
            return Err(invalid("Metric is disabled"));
        }
        let counts = self.counts(hooks).await?;
        let mut slots = {
            let s = self.state.read().await;
            let me = &s.durable.nodes[&s.durable.myself];
            let owner = me.parent.as_ref().unwrap_or(&me.id);
            (range.0..=range.1)
                .filter(|slot| s.owners[*slot as usize].as_ref() == Some(owner))
                .collect::<Vec<_>>()
        };
        let value = |slot: u16| {
            let m = &self.metrics[slot as usize];
            match metric.as_str() {
                "cpu-usec" => m.cpu.load(Ordering::Relaxed),
                "memory-bytes" => m.memory.load(Ordering::Relaxed),
                "network-bytes-in" => m.bytes_in.load(Ordering::Relaxed),
                "network-bytes-out" => m.bytes_out.load(Ordering::Relaxed),
                _ => counts[slot as usize],
            }
        };
        if kind == "ORDERBY" {
            slots.sort_by(|a, b| {
                let order = value(*a).cmp(&value(*b));
                let order = if desc { order.reverse() } else { order };
                order.then(a.cmp(b))
            });
        }
        slots.truncate(limit);
        Ok(array(
            slots
                .into_iter()
                .map(|slot| {
                    let m = &self.metrics[slot as usize];
                    let mut metrics = vec![(
                        bulk("key-count"),
                        Resp::Integer(counts[slot as usize].min(i64::MAX as u64) as i64),
                    )];
                    if self.config.cpu_stats {
                        metrics.push((bulk("cpu-usec"), integer(m.cpu.load(Ordering::Relaxed))));
                    }
                    if self.config.memory_stats {
                        metrics.push((
                            bulk("memory-bytes"),
                            integer(m.memory.load(Ordering::Relaxed)),
                        ));
                    }
                    if self.config.network_stats {
                        metrics.push((
                            bulk("network-bytes-in"),
                            integer(m.bytes_in.load(Ordering::Relaxed)),
                        ));
                        metrics.push((
                            bulk("network-bytes-out"),
                            integer(m.bytes_out.load(Ordering::Relaxed)),
                        ));
                    }
                    array(vec![Resp::Integer(i64::from(slot)), Resp::Map(metrics)])
                })
                .collect(),
        ))
    }
}
fn arity(args: &[Bytes], length: usize) -> io::Result<()> {
    if args.len() == length {
        Ok(())
    } else {
        Err(invalid("wrong number of arguments"))
    }
}
fn port(value: &Bytes) -> io::Result<u16> {
    let value = number(value)?;
    if value == 0 || value > 65535 {
        Err(invalid("Invalid port"))
    } else {
        Ok(value as u16)
    }
}
fn descriptor(node: &Node) -> Resp {
    array(vec![
        bulk(node.ip.clone()),
        Resp::Integer(i64::from(node.port)),
        bulk(node.id.clone()),
        Resp::Map(vec![]),
    ])
}
fn integer(value: u64) -> Resp {
    Resp::Integer(value.min(i64::MAX as u64) as i64)
}
fn verbatim(value: impl Into<Bytes>) -> Resp {
    Resp::Verbatim {
        format: *b"txt",
        value: value.into(),
    }
}
