//! WATCH versions exist only while clients hold tokens, scoped to logical DBs.
use crate::journal::Mutation;
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

pub struct Token {
    pub database: u32,
    pub key: Bytes,
    deadline_ms: Option<i64>,
    version: Arc<AtomicU64>,
    expected: u64,
}
impl Token {
    pub fn changed(&self, now_ms: i64) -> bool {
        self.version.load(Ordering::Acquire) != self.expected
            || self.deadline_ms.is_some_and(|deadline| deadline <= now_ms)
    }
}
#[derive(Default)]
pub struct Registry {
    keys: Mutex<HashMap<u32, HashMap<Bytes, Weak<AtomicU64>>>>,
    active: AtomicBool,
}
impl Registry {
    pub fn active(&self) -> bool {
        self.active.load(Ordering::Acquire)
    }
    pub fn register(&self, database: u32, key: Bytes, deadline_ms: Option<i64>) -> Token {
        let mut databases = self.keys.lock().expect("watch registry");
        let entry = databases
            .entry(database)
            .or_default()
            .entry(key.clone())
            .or_default();
        let version = entry
            .upgrade()
            .unwrap_or_else(|| Arc::new(AtomicU64::new(0)));
        *entry = Arc::downgrade(&version);
        let expected = version.load(Ordering::Acquire);
        self.active.store(true, Ordering::Release);
        Token {
            database,
            key,
            deadline_ms,
            version,
            expected,
        }
    }
    pub fn observe(&self, database: u32, mutation: &Mutation) {
        if !self.active() {
            return;
        }
        let databases = self.keys.lock().expect("watch registry");
        if let Mutation::SwapDatabases { first, second } = mutation {
            for database in [first, second] {
                if let Some(keys) = databases.get(database) {
                    for version in keys.values().filter_map(Weak::upgrade) {
                        version.fetch_add(1, Ordering::AcqRel);
                    }
                }
            }
            return;
        }
        let Some(keys) = databases.get(&database) else {
            return;
        };
        let changed = |key: &Bytes| {
            if let Some(version) = keys.get(key).and_then(Weak::upgrade) {
                version.fetch_add(1, Ordering::AcqRel);
            }
        };
        match mutation {
            Mutation::Clear => {
                for version in keys.values().filter_map(Weak::upgrade) {
                    version.fetch_add(1, Ordering::AcqRel);
                }
            }
            Mutation::Rename { from, to } => {
                changed(from);
                changed(to);
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
            | Mutation::Typed { key, .. } => changed(key),
            Mutation::SelectDatabase { .. }
            | Mutation::Search { .. }
            | Mutation::Functions { .. }
            | Mutation::NativePosition { .. }
            | Mutation::RdbAuxiliary { .. }
            | Mutation::ResetGlobals
            | Mutation::ClearNativePosition
            | Mutation::SwapDatabases { .. } => {}
        }
    }
    pub fn invalidate_all(&self) {
        let databases = self.keys.lock().expect("watch registry");
        for keys in databases.values() {
            for version in keys.values().filter_map(Weak::upgrade) {
                version.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
    pub fn reap(&self) {
        let mut databases = self.keys.lock().expect("watch registry");
        databases.retain(|_, keys| {
            keys.retain(|_, version| version.strong_count() != 0);
            !keys.is_empty()
        });
        self.active.store(!databases.is_empty(), Ordering::Release);
    }
}
