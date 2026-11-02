use super::bus::Reply;
use super::*;

impl ClusterRuntime {
    pub(super) async fn follow_node(&self, node: &Node) -> io::Result<()> {
        let hooks = self.hooks.as_ref();
        self.follow_node_with_hooks(node, &hooks).await
    }
    pub(super) async fn follow_node_with_hooks(
        &self,
        node: &Node,
        hooks: &dyn ClusterCommandHooks,
    ) -> io::Result<()> {
        hooks.follow(&node.ip, node.port).await?;
        self.replica_last_connected.store(0, Ordering::Relaxed);
        Ok(())
    }
    pub(super) async fn begin_export(
        self: &Arc<Self>,
        sender: &Node,
        mut task: migration::Task,
    ) -> io::Result<Reply> {
        let _admin = self.admin.lock().await;
        {
            let mut s = self.state.write().await;
            if !topology::valid_id(&task.id)
                || task.source != s.durable.myself
                || task.dest != sender.id
                || task.ranges.is_empty()
                || task.ranges.len() > SLOT_COUNT
                || task
                    .ranges
                    .iter()
                    .any(|(a, b)| a > b || *b as usize >= SLOT_COUNT)
            {
                return Err(invalid("invalid migration source or ranges"));
            }
            if let Some(existing) = s
                .tasks
                .iter()
                .find(|t| t.id == task.id && t.active() && t.state != "failed")
            {
                return Ok(Reply::Frozen(existing.stream_offset));
            }
            if s.tasks.iter().any(|t| t.active() && t.state != "failed") {
                return Err(invalid("another ASM task is already in progress"));
            }
            if task
                .ranges
                .iter()
                .flat_map(|(a, b)| *a..=*b)
                .any(|slot| s.owners[slot as usize].as_ref() != Some(&s.durable.myself))
            {
                return Err(invalid("migration source no longer owns slots"));
            }
            s.tasks.retain(|t| t.id != task.id);
            for old in &mut s.tasks {
                if old.active() {
                    old.state = "canceled".into();
                    old.end_time = clock();
                }
            }
            while s.tasks.len() >= 32 {
                if let Some(index) = s.tasks.iter().position(|t| !t.active()) {
                    s.tasks.remove(index);
                } else {
                    break;
                }
            }
        }
        let mut pause = self.pause_owner.lock().await;
        if pause.as_ref().is_some_and(|(owner, _)| owner != &sender.id) {
            return Err(invalid("primary is paused for another peer"));
        }
        let offset = self.hooks.freeze_writes().await?;
        *pause = Some((
            sender.id.clone(),
            clock() + self.config.node_timeout_ms.max(1000) * 8,
        ));
        let progress = match self.hooks.replication_progress().await {
            Ok(progress) => progress,
            Err(e) => {
                self.hooks.unfreeze_writes().await?;
                *pause = None;
                return Err(e);
            }
        };
        task.operation = "migrate".into();
        task.state = "wait-bgsave-start".into();
        task.start_time = clock();
        task.paused_at = 0;
        task.write_pause_ms = 0;
        task.activity = clock();
        task.stream_offset = offset;
        task.stream_history = progress.history;
        task.snapshot_id = random_id();
        task.running = true;
        self.state.write().await.tasks.push(task.clone());
        drop(pause);
        if let Err(e) = self.save().await {
            let _ = self.release_pause(&sender.id).await;
            return Err(e);
        }
        let runtime = self.clone();
        if let Err(error) = self
            .spawn_owned(async move {
                runtime.spool_export(task).await;
            })
            .await
        {
            let _ = self.release_pause(&sender.id).await;
            return Err(error);
        }
        Ok(Reply::Frozen(offset))
    }
    pub(super) async fn source_task(&self, sender: &Node, id: &str) -> io::Result<migration::Task> {
        let mut s = self.state.write().await;
        if s.durable.nodes[&s.durable.myself].parent.is_some() {
            return Err(invalid("migration source was demoted"));
        }
        let task = s
            .tasks
            .iter_mut()
            .find(|t| t.id == id && t.active() && t.operation == "migrate" && t.dest == sender.id)
            .ok_or_else(|| invalid("unknown active migration"))?;
        if task.state == "failed" {
            return Err(io::Error::other(task.last_error.clone()));
        }
        task.activity = clock();
        Ok(task.clone())
    }
    pub(super) async fn handoff_freeze(&self, sender: &Node, id: &str) -> io::Result<Reply> {
        let _admin = self.admin.lock().await;
        let task = self.source_task(sender, id).await?;
        let mut pause = self.pause_owner.lock().await;
        if pause.as_ref().is_some_and(|(owner, _)| owner != &sender.id) {
            return Err(invalid("primary is paused for another peer"));
        }
        let offset = self.hooks.freeze_writes().await?;
        *pause = Some((
            sender.id.clone(),
            clock() + self.config.node_timeout_ms.max(1000) * 8,
        ));
        let progress = self.hooks.replication_progress().await?;
        if progress.history != task.stream_history {
            self.hooks.unfreeze_writes().await?;
            *pause = None;
            return Err(invalid("migration replication history changed"));
        }
        drop(pause);
        {
            let mut s = self.state.write().await;
            let task = s
                .tasks
                .iter_mut()
                .find(|t| t.id == id && t.active())
                .ok_or_else(|| invalid("migration canceled"))?;
            task.state = "handoff".into();
            task.paused_at = clock();
            task.activity = clock();
        }
        self.save().await?;
        Ok(Reply::Frozen(offset))
    }
}
