use super::*;
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt, BufWriter},
};

impl ClusterRuntime {
    fn spool_prefix(&self) -> String {
        format!(
            ".{}-ffkv-cluster-export-",
            self.config
                .config_file
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
        )
    }
    fn spool_path(&self, id: &str, suffix: &str) -> PathBuf {
        self.config
            .config_file
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(format!("{}{id}.{suffix}", self.spool_prefix()))
    }
    fn snapshot_path(&self, id: &str, partial: bool) -> PathBuf {
        self.spool_path(id, if partial { "part" } else { "snap" })
    }
    pub(super) async fn cleanup_snapshots(&self) -> io::Result<()> {
        let parent = self
            .config
            .config_file
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let prefix = self.spool_prefix();
        let mut entries = tokio::fs::read_dir(parent).await?;
        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let Some(name) = name.to_str() else { continue };
            if let Some(rest) = name.strip_prefix(&prefix)
                && let Some((id, suffix)) = rest.split_once('.')
                && topology::valid_id(id)
                && matches!(suffix, "part" | "snap" | "globals.part" | "globals")
            {
                tokio::fs::remove_file(entry.path()).await?;
            }
        }
        Ok(())
    }
    pub(super) async fn remove_snapshot(&self, id: &str) -> io::Result<()> {
        if id.is_empty() {
            return Ok(());
        }
        for suffix in ["part", "snap", "globals.part", "globals"] {
            match tokio::fs::remove_file(self.spool_path(id, suffix)).await {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
    pub(super) async fn spool_export(self: Arc<Self>, task: migration::Task) {
        let result = self.write_snapshot(&task).await;
        let current = {
            let mut s = self.state.write().await;
            if let Some(t) = s
                .tasks
                .iter_mut()
                .find(|t| t.id == task.id && t.snapshot_id == task.snapshot_id && t.active())
            {
                match &result {
                    Ok(()) => {
                        t.state = "send-bulk-and-stream".into();
                        t.activity = clock();
                    }
                    Err(e) => {
                        t.state = "failed".into();
                        t.last_error = e.to_string();
                        t.end_time = clock();
                    }
                }
                true
            } else {
                false
            }
        };
        if current {
            let _ = self.release_pause(&task.dest).await;
        }
        if result.is_err() || !current {
            let _ = self.remove_snapshot(&task.snapshot_id).await;
        }
        let _ = self.save().await;
    }
    async fn write_snapshot(&self, task: &migration::Task) -> io::Result<()> {
        if self.stopped.load(Ordering::Acquire) {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "cluster is shutting down",
            ));
        }
        self.remove_snapshot(&task.snapshot_id).await?;
        let globals = self.hooks.export_migration_globals(&task.ranges).await?;
        if globals.len() > 64 * 1024 * 1024 {
            return Err(invalid("migration global catalog exceeds limit"));
        }
        let mut metadata = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.spool_path(&task.snapshot_id, "globals.part"))
            .await?;
        metadata.write_all(&globals).await?;
        metadata.sync_all().await?;
        drop(metadata);
        drop(globals);
        tokio::fs::rename(
            self.spool_path(&task.snapshot_id, "globals.part"),
            self.spool_path(&task.snapshot_id, "globals"),
        )
        .await?;
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.snapshot_path(&task.snapshot_id, true))
            .await?;
        let mut file = BufWriter::with_capacity(256 * 1024, file);
        let mut after = None;
        loop {
            if self.stopped.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::Interrupted,
                    "cluster is shutting down",
                ));
            }
            {
                let mut s = self.state.write().await;
                let current = s
                    .tasks
                    .iter_mut()
                    .find(|t| t.id == task.id && t.snapshot_id == task.snapshot_id && t.active())
                    .ok_or_else(|| invalid("snapshot canceled"))?;
                current.activity = clock();
            }
            self.touch_pause(&task.dest).await?;
            let page = self
                .hooks
                .export_slots(&task.ranges, after.as_deref(), 1)
                .await?;
            if page.keys.len() > 1 {
                return Err(invalid("snapshot export exceeded page bound"));
            }
            for key in page.keys {
                if !task.contains(key_slot(&key.key))
                    || key.key.len() > 512 * 1024 * 1024
                    || key.payload.len() > 512 * 1024 * 1024
                {
                    return Err(invalid("invalid exported snapshot key"));
                }
                file.write_u32(key.key.len() as u32).await?;
                file.write_u64(key.payload.len() as u64).await?;
                file.write_all(&key.key).await?;
                file.write_all(&key.payload).await?;
            }
            match page.next {
                Some(cursor) => {
                    if after.as_ref().is_some_and(|old: &Vec<u8>| cursor <= *old) {
                        return Err(invalid("snapshot key cursor did not advance"));
                    }
                    after = Some(cursor)
                }
                None => break,
            }
        }
        file.flush().await?;
        file.get_ref().sync_all().await?;
        drop(file);
        {
            let s = self.state.read().await;
            if !s
                .tasks
                .iter()
                .any(|t| t.id == task.id && t.snapshot_id == task.snapshot_id && t.active())
            {
                return Err(invalid("snapshot canceled"));
            }
        }
        tokio::fs::rename(
            self.snapshot_path(&task.snapshot_id, true),
            self.snapshot_path(&task.snapshot_id, false),
        )
        .await
    }
    pub(super) async fn snapshot_globals(&self, id: &str) -> io::Result<Option<Vec<u8>>> {
        match File::open(self.spool_path(id, "globals")).await {
            Ok(mut file) => {
                let length = file.metadata().await?.len();
                if length > 64 * 1024 * 1024 {
                    return Err(invalid("migration global catalog exceeds limit"));
                }
                let mut bytes = Vec::with_capacity(length as usize);
                file.read_to_end(&mut bytes).await?;
                Ok(Some(bytes))
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }
    pub(super) async fn snapshot_page(
        &self,
        id: &str,
        after: Option<&[u8]>,
    ) -> io::Result<Option<ClusterPage>> {
        let mut file = match File::open(self.snapshot_path(id, false)).await {
            Ok(file) => file,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let length = file.metadata().await?.len();
        let position = match after {
            Some(bytes) => u64::from_be_bytes(
                bytes
                    .try_into()
                    .map_err(|_| invalid("invalid snapshot cursor"))?,
            ),
            None => 0,
        };
        if position > length {
            return Err(invalid("snapshot cursor beyond EOF"));
        }
        if position == length {
            return Ok(Some(ClusterPage {
                keys: vec![],
                next: None,
            }));
        }
        file.seek(std::io::SeekFrom::Start(position)).await?;
        let key_len = file.read_u32().await? as usize;
        let payload_len = file.read_u64().await?;
        if key_len > 512 * 1024 * 1024 || payload_len > 512 * 1024 * 1024 {
            return Err(invalid("invalid snapshot record length"));
        }
        let end = position
            .checked_add(12)
            .and_then(|p| p.checked_add(key_len as u64))
            .and_then(|p| p.checked_add(payload_len))
            .ok_or_else(|| invalid("snapshot record length overflow"))?;
        if end > length {
            return Err(invalid("truncated snapshot record"));
        }
        let mut key = vec![0; key_len];
        file.read_exact(&mut key).await?;
        let mut payload = vec![0; payload_len as usize];
        file.read_exact(&mut payload).await?;
        Ok(Some(ClusterPage {
            keys: vec![ClusterKey { key, payload }],
            next: if end < length {
                Some(end.to_be_bytes().to_vec())
            } else {
                None
            },
        }))
    }
}
