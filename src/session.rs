//! Connection-local state shared by commands routed to different shard actors.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
};

use bytes::Bytes;

pub(crate) type HashFieldsets = Mutex<HashMap<Bytes, Arc<[Bytes]>>>;

#[derive(Default)]
pub(crate) struct ClientSession {
    pub hash_fieldsets: HashFieldsets,
    pub client_id: u64,
    pub acl: Option<Arc<crate::auth::Acl>>,
    pub clients: Option<Arc<crate::client_registry::Registry>>,
    /// Transport cancellation survives queuing before debugger ownership begins.
    pub disconnected: Arc<AtomicBool>,
    database: AtomicU32,
    pub no_touch: AtomicBool,
    pub last_write_offset: AtomicU64,
    pub cpu_ns: AtomicU64,
    pub script_debug_mode: AtomicU8,
}

impl ClientSession {
    pub fn new(client_id: u64) -> Self {
        Self {
            client_id,
            ..Self::default()
        }
    }

    pub fn script_child(&self) -> Self {
        Self {
            hash_fieldsets: Mutex::default(),
            client_id: self.client_id,
            acl: self.acl.clone(),
            clients: self.clients.clone(),
            disconnected: self.disconnected.clone(),
            database: AtomicU32::new(self.database()),
            no_touch: AtomicBool::new(self.no_touch.load(Ordering::Acquire)),
            last_write_offset: AtomicU64::new(0),
            cpu_ns: AtomicU64::new(0),
            script_debug_mode: AtomicU8::new(0),
        }
    }

    pub fn database(&self) -> u32 {
        self.database.load(Ordering::Acquire)
    }

    pub fn set_database(&self, database: u32) {
        self.database.store(database, Ordering::Release);
    }

    pub fn reset(&self) {
        self.hash_fieldsets
            .lock()
            .expect("session hash definitions")
            .clear();
        self.set_database(0);
        self.no_touch.store(false, Ordering::Release);
        self.script_debug_mode.store(0, Ordering::Release);
    }
}
