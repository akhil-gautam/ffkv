//! Followers and ASM imports hide expired payloads while source history owns them.
//! Authoritative recreation resolves DEL before partial effects; structural moves
//! and source replay preserve the retained image without inventing deletions.
use super::{Engine, Entry, Mutation, Value};
use bytes::Bytes;
use std::sync::Arc;

impl Engine {
    pub(crate) fn set_replica_mode(&mut self, replica: bool) {
        self.replica_mode = replica;
    }
    pub(crate) fn set_replication_replay(&mut self, replay: bool) {
        self.replication_replay = replay;
    }
    pub(crate) fn set_migration_expiry_ranges(&mut self, ranges: Option<&[(u16, u16)]>) {
        self.migration_expiry_ranges = ranges.map(Arc::from);
    }
    pub(super) fn holding_expiry(&self, key: &Bytes) -> bool {
        self.replica_mode
            || self.migration_expiry_ranges.as_ref().is_some_and(|ranges| {
                let slot = crate::cluster::key_slot(key);
                let index = ranges.partition_point(|(start, _)| *start <= slot);
                index > 0 && slot <= ranges[index - 1].1
            })
    }
    pub(super) fn entry_live(entry: &Entry, now: i64) -> bool {
        entry.expires_at.is_none_or(|deadline| deadline > now)
            && match &entry.value {
                Value::Hash(hash) => hash.snapshot(now).next().is_some(),
                _ => true,
            }
    }
    pub(super) fn restore_held_key(&mut self, key: &Bytes) {
        if let Some((owned, entry)) = self.held_entries.remove_entry(key)
            && !self.entries.contains_key(&owned)
        {
            self.insert_entry_raw(owned, entry);
        }
        let previous = self.hash_deadline(key);
        if let Some(super::Entry {
            value: Value::Hash(hash),
            ..
        }) = self.entries.get_mut(key)
        {
            hash.restore_retained_fields();
        }
        self.retained_hashes.remove(key);
        self.refresh_hash_deadline(key, previous);
    }
    /// Only replication replay restores held images before a command; every
    /// other command pays the inlined flag test, not a call.
    #[inline]
    pub(super) fn prepare_replay_args(&mut self, command: &super::BoundCommand<'_>) {
        if self.replication_replay {
            self.restore_replayed_args(command);
        }
    }
    fn restore_replayed_args(&mut self, command: &super::BoundCommand<'_>) {
        let args = command.args();
        for position in command.access_positions().into_iter().flatten() {
            self.restore_held_key(&args[position]);
        }
    }
    pub(super) fn prepare_replay_mutation(&mut self, mutation: &Mutation) {
        if !self.replication_replay {
            return;
        }
        match mutation {
            Mutation::Rename { from, to } => {
                self.restore_held_key(from);
                self.restore_held_key(to);
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
            | Mutation::Typed { key, .. } => self.restore_held_key(key),
            _ => {}
        }
    }
    /// A new authoritative value supersedes the retained source image. The DEL
    /// must precede partial creation effects, including clock-zero cluster imports.
    pub(super) fn resolve_held_write(&mut self, key: &Bytes, now: i64) {
        if !self.held_entries.is_empty() && self.held_entries.remove(key).is_some() {
            self.retained_hashes.remove(key);
            self.record(now, || Mutation::Delete { key: key.clone() });
        }
    }
    pub(super) fn release_unheld_key(&mut self, key: &Bytes, now: i64) {
        if !self.replication_replay
            && !self.holding_expiry(key)
            && self.held_entries.remove(key).is_some()
        {
            self.retained_hashes.remove(key);
            self.record(now, || Mutation::Delete { key: key.clone() });
        }
    }
    pub(super) fn expire_held(&mut self, now: i64, limit: usize) {
        if self.replica_mode || self.replication_replay {
            return;
        }
        // The iterator is not retained across removal; only due keys are detached.
        let keys: Vec<_> = self
            .held_entries
            .keys()
            .filter(|key| !self.holding_expiry(key))
            .take(limit)
            .cloned()
            .collect();
        for key in keys {
            self.held_entries.remove(&key);
            self.retained_hashes.remove(&key);
            self.record(now, || Mutation::Delete { key });
        }
    }
    pub(crate) fn finish_migration_expiry(
        &mut self,
        now: i64,
    ) -> std::io::Result<Option<crate::journal::Batch>> {
        debug_assert!(
            self.pending_batch.is_none(),
            "commit before finalizing expiry holds"
        );
        self.migration_expiry_ranges = None;
        self.replication_replay = false;
        let active = self.active_database;
        let databases: Vec<_> = self.databases.keys().copied().collect();
        for database in std::iter::once(active).chain(databases) {
            self.switch_database(database);
            self.expire_held(now, usize::MAX);
            self.expire_at(now, usize::MAX);
            self.expire_hashes(now, usize::MAX);
        }
        self.switch_database(active);
        self.finish_pending_search();
        if let Some(error) = self.failure.take() {
            return Err(error);
        }
        Ok(self.take_batch())
    }
}
