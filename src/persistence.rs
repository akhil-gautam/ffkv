//! One commit order shared by WAL, independent online backups, native replication,
//! cache invalidations and all shard actors. Replies follow successful commit.
use crate::{
    engine::Engine,
    journal::{Batch, Mutation},
    wal::{Durability, Wal},
    watch::Registry,
};
use bytes::Bytes;
use std::{
    io,
    path::PathBuf,
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};
use tokio::sync::Mutex;

pub struct Persistence {
    wal: Mutex<Option<Wal>>,
    enabled: AtomicBool,
    failed: AtomicBool,
    pub watches: Arc<Registry>,
    revision: AtomicU64,
    last_sync_ms: AtomicU64,
    checkpoint_bytes: AtomicU64,
    native: OnceLock<Arc<crate::native_replication::NativeReplication>>,
    backup: OnceLock<Arc<crate::server_runtime::OnlineBackup>>,
    clients: OnceLock<Arc<crate::client_registry::Registry>>,
    published_offset: AtomicU64,
    fsynced_offset: AtomicU64,
}

pub struct Status {
    pub bytes: u64,
    pub policy: &'static str,
    pub dirty: bool,
    pub last_sync_ms: Option<i64>,
    pub revision: u64,
}

impl Persistence {
    pub fn new(wal: Option<Wal>) -> Self {
        Self {
            enabled: AtomicBool::new(wal.is_some()),
            wal: Mutex::new(wal),
            failed: AtomicBool::new(false),
            watches: Arc::new(Registry::default()),
            revision: AtomicU64::new(0),
            last_sync_ms: AtomicU64::new(0),
            checkpoint_bytes: AtomicU64::new(0),
            native: OnceLock::new(),
            backup: OnceLock::new(),
            clients: OnceLock::new(),
            published_offset: AtomicU64::new(0),
            fsynced_offset: AtomicU64::new(0),
        }
    }
    pub fn enabled(&self) -> bool {
        self.enabled.load(Ordering::Acquire)
    }
    pub fn capturing(&self) -> bool {
        self.enabled()
            || self.native.get().is_some()
            || self.backup.get().is_some_and(|backup| backup.active())
            || self.watches.active()
    }
    pub fn recording_for(&self, _args: &[Bytes]) -> bool {
        self.capturing()
    }
    pub fn set_native(&self, native: Arc<crate::native_replication::NativeReplication>) {
        let status = native.status();
        self.published_offset
            .store(status.offset, Ordering::Release);
        self.fsynced_offset
            .store(status.fsynced_offset, Ordering::Release);
        assert!(
            self.native.set(native).is_ok(),
            "native replication already configured"
        );
    }
    pub fn set_backup(&self, backup: Arc<crate::server_runtime::OnlineBackup>) {
        assert!(
            self.backup.set(backup).is_ok(),
            "online backup already configured"
        );
    }
    pub fn set_clients(&self, clients: Arc<crate::client_registry::Registry>) {
        assert!(
            self.clients.set(clients).is_ok(),
            "client registry already configured"
        );
    }
    pub fn native_offset(&self) -> u64 {
        self.published_offset.load(Ordering::Acquire)
    }
    pub fn last_fsynced_native_offset(&self) -> u64 {
        self.fsynced_offset.load(Ordering::Acquire)
    }
    pub fn ensure_healthy(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            Err(io::Error::other(
                "persistence is unavailable after a commit failure",
            ))
        } else {
            Ok(())
        }
    }

    pub async fn commit(&self, engine: &mut Engine) -> io::Result<()> {
        engine.finish_pending_search();
        let session = engine.command_session().cloned();
        if let Some(batch) = engine.take_batch() {
            let offset = self
                .commit_batch_offset(&batch, session.as_ref().map(|session| session.client_id))
                .await?;
            if let Some(session) = session {
                session.last_write_offset.store(offset, Ordering::Release);
            }
        }
        Ok(())
    }

    pub async fn commit_batch(&self, batch: &Batch) -> io::Result<()> {
        self.commit_batch_offset(batch, None).await.map(|_| ())
    }
    pub async fn commit_batch_offset(&self, batch: &Batch, origin: Option<u64>) -> io::Result<u64> {
        self.commit_ordered(batch, true, origin).await
    }
    /// Pub/sub has no recoverable dataset effect, but its stream watermark must
    /// share commit order and become durable only after a real subsequent fsync.
    pub async fn publish_ephemeral(&self, args: &[Bytes]) -> io::Result<Option<u64>> {
        let _guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let Some(native) = self.native.get().filter(|native| !native.is_replica()) else {
            return Ok(None);
        };
        let offset = native.publish(crate::native_replication::encode_command(args)?, false)?;
        self.published_offset.store(offset, Ordering::Release);
        Ok(Some(offset))
    }
    pub async fn commit_replicated_batch(&self, batch: &Batch) -> io::Result<()> {
        self.commit_ordered(batch, false, None).await.map(|_| ())
    }
    async fn commit_ordered(
        &self,
        batch: &Batch,
        publish: bool,
        origin: Option<u64>,
    ) -> io::Result<u64> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let native = self.native.get();
        let encoded = if publish && native.is_some_and(|native| !native.is_replica()) {
            Some(crate::native_replication::encode_batch(batch)?)
        } else {
            None
        };
        if self.enabled() {
            let wal = guard
                .as_mut()
                .ok_or_else(|| io::Error::other("enabled journal has no WAL"))?;
            if let Err(error) = wal.append(batch).await {
                self.failed.store(true, Ordering::Release);
                return Err(error);
            }
            if !wal.is_dirty() {
                self.note_sync();
            }
        }
        if let Some(backup) = self.backup.get() {
            backup.record_batch(batch).await;
        }
        let mut offset = self.native_offset();
        if let (Some(native), Some(encoded)) = (native, encoded)
            && !encoded.is_empty()
        {
            let fsynced = self.enabled() && guard.as_ref().is_some_and(|wal| !wal.is_dirty());
            match native.publish(encoded, fsynced) {
                Ok(end) => {
                    offset = end;
                    self.published_offset.store(end, Ordering::Release);
                    if fsynced {
                        self.fsynced_offset.store(end, Ordering::Release);
                    }
                }
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error);
                }
            }
        }
        if !publish {
            if let Some(recovered_offset) =
                batch
                    .mutations
                    .iter()
                    .rev()
                    .find_map(|mutation| match mutation {
                        Mutation::NativePosition { offset, .. } => Some(*offset),
                        _ => None,
                    })
            {
                self.published_offset
                    .store(recovered_offset, Ordering::Release);
                offset = self.native_offset();
            }
            let mut database = 0;
            for mutation in &batch.mutations {
                if let Mutation::SelectDatabase { database: selected } = mutation {
                    database = *selected;
                } else {
                    self.watches.observe(database, mutation);
                }
            }
        }
        self.invalidate(batch, origin);
        self.note_install()?;
        Ok(offset)
    }

    fn invalidate(&self, batch: &Batch, origin: Option<u64>) {
        let Some(clients) = self.clients.get() else {
            return;
        };
        let mut first: Option<&Bytes> = None;
        let mut multiple = Vec::new();
        for mutation in &batch.mutations {
            let keys: &[Bytes] = match mutation {
                Mutation::Clear | Mutation::SwapDatabases { .. } => {
                    clients.invalidate_all(None, origin);
                    return;
                }
                Mutation::Rename { from, to } => {
                    if let Some(key) = first.take() {
                        multiple.push(key.clone());
                    }
                    multiple.push(from.clone());
                    multiple.push(to.clone());
                    continue;
                }
                Mutation::Set { key, .. }
                | Mutation::Delete { key }
                | Mutation::Push { key, .. }
                | Mutation::Pop { key, .. }
                | Mutation::Expire { key, .. }
                | Mutation::SetAdd { key, .. }
                | Mutation::SetRemove { key, .. }
                | Mutation::HashSet { key, .. }
                | Mutation::HashDelete { key, .. }
                | Mutation::SortedSetAdd { key, .. }
                | Mutation::SortedSetRemove { key, .. }
                | Mutation::ListRemove { key, .. }
                | Mutation::ListSet { key, .. }
                | Mutation::ListInsert { key, .. }
                | Mutation::Queue { key, .. }
                | Mutation::Typed { key, .. } => std::slice::from_ref(key),
                Mutation::SelectDatabase { .. }
                | Mutation::Search { .. }
                | Mutation::Functions { .. }
                | Mutation::NativePosition { .. }
                | Mutation::RdbAuxiliary { .. }
                | Mutation::ResetGlobals
                | Mutation::ClearNativePosition => continue,
            };
            for key in keys {
                if !multiple.is_empty() {
                    multiple.push(key.clone());
                } else if let Some(previous) = first {
                    if previous != key {
                        multiple.push(previous.clone());
                        multiple.push(key.clone());
                        first = None;
                    }
                } else {
                    first = Some(key);
                }
            }
        }
        if let Some(key) = first {
            clients.invalidate(0, std::slice::from_ref(key), origin);
        } else if !multiple.is_empty() {
            multiple.sort_unstable();
            multiple.dedup();
            clients.invalidate(0, &multiple, origin);
        }
    }

    pub fn note_reads(&self, args: &[Bytes], session: Option<&crate::session::ClientSession>) {
        let (Some(clients), Some(session)) = (self.clients.get(), session) else {
            return;
        };
        if Engine::is_readonly(args) {
            Engine::any_accessed_key(args, |key| {
                clients.read(
                    session.client_id,
                    session.database(),
                    std::slice::from_ref(key),
                );
                false
            });
        }
    }

    pub async fn periodic_sync(&self) -> io::Result<()> {
        if !self.enabled() {
            return Ok(());
        }
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let wal = guard.as_mut().expect("persistent WAL");
        let dirty = wal.is_dirty();
        let offset = self.native_offset();
        let clean_offset_pending = !dirty && offset > self.last_fsynced_native_offset();
        let sync = if clean_offset_pending {
            wal.finish().await
        } else {
            wal.periodic_sync().await
        };
        if let Err(error) = sync {
            self.failed.store(true, Ordering::Release);
            return Err(error);
        }
        if clean_offset_pending || (dirty && !wal.is_dirty()) {
            self.note_sync();
            if let Some(native) = self.native.get()
                && !native.is_replica()
            {
                native.mark_fsynced(offset)?;
            }
            self.fsynced_offset.store(offset, Ordering::Release);
        }
        Ok(())
    }
    pub async fn finish(&self) -> io::Result<()> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        if let Some(wal) = guard.as_mut() {
            if let Err(error) = wal.finish().await {
                self.failed.store(true, Ordering::Release);
                return Err(error);
            }
            self.note_sync();
            if self.enabled() {
                let offset = self.native_offset();
                if let Some(native) = self.native.get()
                    && !native.is_replica()
                {
                    native.mark_fsynced(offset)?;
                }
                self.fsynced_offset.store(offset, Ordering::Release);
            }
        }
        Ok(())
    }
    pub fn note_install(&self) -> io::Result<()> {
        self.revision
            .try_update(Ordering::AcqRel, Ordering::Acquire, |revision| {
                revision.checked_add(1)
            })
            .map(|_| ())
            .map_err(|_| {
                self.failed.store(true, Ordering::Release);
                io::Error::other("replication revision exhausted")
            })
    }
    fn note_sync(&self) {
        self.last_sync_ms
            .store(Engine::now().max(0) as u64, Ordering::Release);
    }
    pub async fn status(&self) -> Status {
        let guard = self.wal.lock().await;
        let (bytes, policy, dirty) = if self.enabled() {
            match guard.as_ref() {
                Some(wal) => (
                    wal.bytes(),
                    match wal.policy() {
                        Durability::Strict => "strict",
                        Durability::Everysec => "everysec",
                    },
                    wal.is_dirty(),
                ),
                None => (0, "none", false),
            }
        } else {
            (0, "none", false)
        };
        let last_sync = self.last_sync_ms.load(Ordering::Acquire);
        Status {
            bytes,
            policy,
            dirty,
            last_sync_ms: (last_sync != 0).then_some(last_sync as i64),
            revision: self.revision.load(Ordering::Acquire),
        }
    }
    pub async fn publish(&self, metrics: &crate::metrics::Metrics) {
        let status = self.status().await;
        metrics.set_wal(
            status.bytes,
            status.policy,
            status.dirty,
            status.last_sync_ms,
        );
    }
    pub async fn checkpoint(
        &self,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<crate::wal::Checkpoint> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let wal = guard.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint requires --data-dir",
            )
        })?;
        match wal.checkpoint(batches).await {
            Ok(result) => {
                self.note_sync();
                self.checkpoint_bytes
                    .store(result.new_bytes, Ordering::Release);
                Ok(result)
            }
            Err(error) => {
                self.failed.store(true, Ordering::Release);
                Err(error)
            }
        }
    }
    /// Install an authoritative image while the caller holds the pool pause.
    /// Both exports borrow the staged image; no full-image buffer is retained.
    pub async fn checkpoint_replacement<'a>(
        &self,
        image: &'a Engine,
        previous_databases: impl Iterator<Item = u32> + 'a,
    ) -> io::Result<()> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        if self.enabled() {
            let wal = guard
                .as_mut()
                .ok_or_else(|| io::Error::other("enabled journal has no WAL"))?;
            match wal.checkpoint(image.snapshot_batches(0)).await {
                Ok(result) => {
                    self.note_sync();
                    self.checkpoint_bytes
                        .store(result.new_bytes, Ordering::Release);
                }
                Err(error) => {
                    self.failed.store(true, Ordering::Release);
                    return Err(error);
                }
            }
        }
        if let Some(backup) = self.backup.get() {
            backup
                .record_replacement(replacement_batches(image, previous_databases))
                .await;
        }
        if let Some((_, offset)) = image.native_position() {
            self.published_offset.store(offset, Ordering::Release);
            self.fsynced_offset
                .store(if self.enabled() { offset } else { 0 }, Ordering::Release);
        }
        Ok(())
    }
    pub async fn configure(
        &self,
        enabled: bool,
        policy: Durability,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<()> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let wal = guard.as_mut().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "persistence configuration requires --data-dir",
            )
        })?;
        if enabled && !self.enabled() {
            wal.checkpoint(batches).await?;
        }
        wal.finish().await?;
        wal.set_policy(policy);
        self.enabled.store(enabled, Ordering::Release);
        if let Some(native) = self.native.get() {
            native.set_aof_enabled(enabled);
        }
        self.note_sync();
        Ok(())
    }
    pub async fn needs_checkpoint(&self, threshold: u64) -> bool {
        self.enabled()
            && threshold != 0
            && self
                .status()
                .await
                .bytes
                .saturating_sub(self.checkpoint_bytes.load(Ordering::Acquire))
                >= threshold
    }
    pub async fn backup(
        &self,
        name: &str,
        batches: impl Iterator<Item = io::Result<Batch>>,
    ) -> io::Result<(PathBuf, crate::backup::Backup)> {
        let mut guard = self.wal.lock().await;
        self.ensure_healthy()?;
        let wal = guard.as_mut().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "backup requires --data-dir")
        })?;
        let root = wal.directory().join("backups");
        std::fs::create_dir_all(&root)?;
        let path = root.join(name);
        let result = wal.backup(&path, batches).await?;
        Ok((std::fs::canonicalize(path)?, result))
    }
}

fn replacement_batches<'a>(
    image: &'a Engine,
    previous_databases: impl Iterator<Item = u32> + 'a,
) -> impl Iterator<Item = io::Result<Batch>> + 'a {
    let clears = previous_databases.map(|database| {
        Ok(Batch {
            timestamp_ms: 0,
            mutations: vec![Mutation::SelectDatabase { database }, Mutation::Clear],
        })
    });
    let reset = std::iter::once(Ok(Batch {
        timestamp_ms: 0,
        mutations: vec![Mutation::ResetGlobals],
    }));
    clears.chain(reset).chain(image.snapshot_batches(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    struct Directory(PathBuf);
    impl Directory {
        fn new() -> Self {
            let path = std::env::current_dir().unwrap().join(format!(
                ".ffkv-native-durability-{:032x}",
                rand::random::<u128>()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    impl Drop for Directory {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    fn image(database: u32, key: &'static [u8], value: &'static [u8]) -> Engine {
        let mut engine = Engine::new();
        engine
            .replay(Batch {
                timestamp_ms: 0,
                mutations: vec![
                    Mutation::SelectDatabase { database },
                    Mutation::Set {
                        key: Bytes::from_static(key),
                        value: Bytes::from_static(value),
                        expires_at: None,
                    },
                ],
            })
            .unwrap();
        engine
    }
    fn get(engine: &mut Engine, database: u32, key: &'static [u8]) -> Option<Bytes> {
        engine.switch_database(database);
        match engine
            .execute(&[Bytes::from_static(b"GET"), Bytes::from_static(key)])
            .unwrap()
        {
            crate::engine::Execution::Reply(crate::protocol::Resp::Bulk(value)) => value,
            _ => panic!("GET did not return a bulk value"),
        }
    }
    #[test]
    fn replacement_delta_removes_every_previous_database_and_global_metadata() {
        let mut restored = image(0, b"old-zero", b"zero");
        restored
            .replay(Batch {
                timestamp_ms: 0,
                mutations: vec![
                    Mutation::SelectDatabase { database: 31 },
                    Mutation::Set {
                        key: Bytes::from_static(b"old-seven"),
                        value: Bytes::from_static(b"seven"),
                        expires_at: None,
                    },
                    Mutation::Functions {
                        snapshot: Bytes::from_static(b"obsolete-library"),
                    },
                    Mutation::NativePosition {
                        history: Bytes::from("a".repeat(40)),
                        offset: 91,
                    },
                    Mutation::RdbAuxiliary {
                        key: Bytes::from_static(b"obsolete-aux"),
                        value: Bytes::from_static(b"value"),
                    },
                ],
            })
            .unwrap();
        let replacement = image(2, b"new", b"image");
        let previous = (0..restored.database_count())
            .chain(restored.database_ids())
            .collect::<std::collections::BTreeSet<_>>();
        for batch in replacement_batches(&replacement, previous.into_iter()) {
            restored.replay(batch.unwrap()).unwrap();
        }
        assert_eq!(get(&mut restored, 0, b"old-zero"), None);
        assert_eq!(get(&mut restored, 31, b"old-seven"), None);
        assert_eq!(
            get(&mut restored, 2, b"new"),
            Some(Bytes::from_static(b"image"))
        );
        assert!(restored.function_snapshot().is_empty());
        assert!(restored.native_position().is_none());
        assert!(restored.rdb_auxiliary().next().is_none());
    }
    #[tokio::test]
    async fn replacement_checkpoint_survives_recovery_without_old_database_keys() {
        let directory = Directory::new();
        let old = image(31, b"old", b"discard");
        let (mut wal, _) = Wal::open(&directory.0, Durability::Everysec, |_| Ok(())).unwrap();
        for batch in old.snapshot_batches(0) {
            wal.append(&batch.unwrap()).await.unwrap();
        }
        let persistence = Persistence::new(Some(wal));
        let mut replacement = image(2, b"new", b"durable");
        replacement.set_native_position(Bytes::from("b".repeat(40)), 712);
        persistence
            .checkpoint_replacement(&replacement, old.database_ids())
            .await
            .unwrap();
        assert_eq!(persistence.last_fsynced_native_offset(), 712);
        drop(persistence);
        let mut recovered = Engine::new();
        let (_wal, _) = Wal::open(&directory.0, Durability::Strict, |batch| {
            recovered.replay(batch)
        })
        .unwrap();
        assert_eq!(get(&mut recovered, 31, b"old"), None);
        assert_eq!(
            get(&mut recovered, 2, b"new"),
            Some(Bytes::from_static(b"durable"))
        );
        assert_eq!(
            recovered.native_position(),
            Some((Bytes::from("b".repeat(40)), 712))
        );
    }
}
