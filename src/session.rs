//! Connection-local state shared by commands routed to different shard actors.
use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;

use crate::server_commands::thread_cpu_time;

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

    /// Starts a CPU window to charge to `session` on this thread: a thread
    /// CPU reading when `observed` (see `State::cpu_observed`) and there is a
    /// session to charge, else nothing, so no clock is read.
    pub fn cpu_window(session: Option<&Self>, observed: bool) -> Option<Duration> {
        (observed && session.is_some()).then(thread_cpu_time)
    }

    /// Charges the CPU this thread spent since `start`, a [`Self::cpu_window`]
    /// reading taken on this thread; an unmeasured window charges nothing.
    pub fn charge_cpu(&self, start: Option<Duration>) {
        if let Some(start) = start {
            let spent = thread_cpu_time().saturating_sub(start);
            self.cpu_ns.fetch_add(
                spent.as_nanos().min(u128::from(u64::MAX)) as u64,
                Ordering::Relaxed,
            );
        }
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
