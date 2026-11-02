//! Separate write admission from shard ownership: migrations may pause user writes
//! while acquiring short dataset barriers for export/import and replication work.
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::{Mutex as AsyncMutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

pub struct WriteGate {
    gate: Arc<RwLock<()>>,
    pause_serial: AsyncMutex<()>,
    held: Mutex<Option<OwnedRwLockWriteGuard<()>>>,
    generation: AtomicU64,
}

impl Default for WriteGate {
    fn default() -> Self {
        Self {
            gate: Arc::new(RwLock::new(())),
            pause_serial: AsyncMutex::new(()),
            held: Mutex::new(None),
            generation: AtomicU64::new(0),
        }
    }
}

impl WriteGate {
    pub async fn admit(&self) -> OwnedRwLockReadGuard<()> {
        self.gate.clone().read_owned().await
    }

    pub fn try_admit(&self) -> Option<OwnedRwLockReadGuard<()>> {
        self.gate.clone().try_read_owned().ok()
    }

    pub async fn pause(&self) -> io::Result<()> {
        let _serial = self.pause_serial.lock().await;
        if self.is_paused() {
            return Ok(());
        }
        let generation = self.generation.load(Ordering::Acquire);
        let guard = self.gate.clone().write_owned().await;
        let mut held = self.held.lock().expect("write pause state");
        if self.generation.load(Ordering::Acquire) != generation {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "write pause cancelled",
            ));
        }
        *held = Some(guard);
        Ok(())
    }

    pub fn resume(&self) {
        let mut held = self.held.lock().expect("write pause state");
        self.generation.fetch_add(1, Ordering::AcqRel);
        held.take();
    }

    pub fn is_paused(&self) -> bool {
        self.held.lock().expect("write pause state").is_some()
    }
}
