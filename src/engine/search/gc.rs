//! Search's eager postings do not need a forked inverted-index collector. The
//! compatible scheduling controls govern reclamation of retired record slots and
//! retained vector allocation capacity; pinned persistence snapshots stay valid.
use super::super::Catalog;
use super::{CHUNK, Records, State};
use serde::{Deserialize, Serialize};
use std::{
    sync::{Arc, atomic::Ordering},
    time::{Duration, Instant},
};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    pub threshold: u64,
    pub interval: u64,
    pub retry: u64,
    pub sleep: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            threshold: 100,
            interval: 30,
            retry: 5,
            sleep: 0,
        }
    }
}
#[derive(Clone, Default)]
pub struct Runtime {
    pub next: i64,
    pub dirty: u64,
    pub cycles: u64,
    pub denied: u64,
    pub failures: u64,
    pub bytes_collected: u64,
    pub total_ms: f64,
    pub last_ms: f64,
}
fn after(now: i64, seconds: u64) -> i64 {
    now.saturating_add(seconds.saturating_mul(1000).min(i64::MAX as u64) as i64)
}
impl State {
    pub(in super::super) fn gc_due(&self, now: i64) -> bool {
        self.rollback.is_none() && self.gc.next <= now && self.gc.dirty > self.config.gc.threshold
    }
    pub(in super::super) fn gc_dirty(&mut self, e: &super::Effect) {
        use super::Effect;
        let dirty = match e {
            Effect::Header { index, id, .. } | Effect::Remove { index, id } => self
                .indexes
                .get(index)
                .is_some_and(|i| i.docs.contains_key(id)),
            Effect::Payload { index, id, .. } => self
                .indexes
                .get(index)
                .and_then(|i| i.docs.get(id))
                .is_some_and(|d| d.payload.is_some()),
            Effect::Field {
                index, id, name, ..
            } => self
                .indexes
                .get(index)
                .and_then(|i| i.docs.get(id))
                .is_some_and(|d| d.fields.contains_key(name)),
            Effect::Drop(index) => self.indexes.contains_key(index),
            _ => false,
        };
        if dirty {
            self.gc.dirty = self.gc.dirty.saturating_add(1);
        }
    }
}
impl Catalog {
    pub(in super::super) fn gc_running(&self) -> bool {
        self.gc_running.load(Ordering::Acquire)
    }
    pub(in super::super) fn collect(self: &Arc<Self>, now: i64) {
        if self.gc_running() {
            return;
        }
        let (generation, chunks, slots, config, dirty) = {
            let mut state = self.lock();
            if !state.gc_due(now) {
                return;
            }
            if self.gc_running.swap(true, Ordering::AcqRel) {
                return;
            }
            state.gc.next = after(now, state.config.gc.interval);
            (
                state.generation,
                state.records.chunks.clone(),
                state.records.slots.clone(),
                state.config.gc.clone(),
                state.gc.dirty,
            )
        };
        let weak = Arc::downgrade(self);
        let retry = config.retry;
        let work = move || {
            let started = Instant::now();
            let mut compact = Records::default();
            // Building a fresh slot directory releases holes and whole empty chunks. Bytes
            // are reference-counted; live document payloads are not serialized or copied.
            let mut failure = None;
            for (key, (group, slot)) in slots {
                match chunks
                    .get(group)
                    .and_then(|g| g.get(slot / CHUNK))
                    .and_then(|c| c.get(slot % CHUNK))
                    .and_then(Option::as_ref)
                {
                    Some(bytes) => compact.set(key, bytes.clone()),
                    None => {
                        failure = Some("invalid search record slot during garbage collection");
                        break;
                    }
                }
            }
            let old_slots = chunks
                .iter()
                .flat_map(|g| g.iter())
                .map(|c| c.capacity())
                .sum::<usize>();
            let new_slots = compact
                .chunks
                .iter()
                .flat_map(|g| g.iter())
                .map(|c| c.capacity())
                .sum::<usize>();
            let mut freed = old_slots
                .saturating_sub(new_slots)
                .saturating_mul(std::mem::size_of::<Option<bytes::Bytes>>());
            // The testing delay never holds the catalog lock or blocks command execution.
            if config.sleep > 0 {
                std::thread::sleep(Duration::from_secs(config.sleep));
            }
            if let Some(catalog) = weak.upgrade() {
                let mut state = catalog.lock();
                if state.rollback.is_none() {
                    if let Some(message) = failure {
                        state.gc.failures = state.gc.failures.saturating_add(1);
                        state.gc.next =
                            after(super::super::super::unix_milliseconds(), config.retry);
                        eprintln!("search garbage collection: {message}");
                    } else if state.generation == generation {
                        state.records = compact;
                        for index in state.indexes.values_mut() {
                            for field in index.fields.values_mut() {
                                if let super::super::model::FieldIndex::Vector(vector) = field {
                                    freed = freed.saturating_add(vector.gc_compact());
                                }
                            }
                        }
                        state.gc.dirty = state.gc.dirty.saturating_sub(dirty);
                        state.gc.cycles = state.gc.cycles.saturating_add(1);
                        state.gc.bytes_collected =
                            state.gc.bytes_collected.saturating_add(freed as u64);
                        state.gc.last_ms = started.elapsed().as_secs_f64() * 1000.;
                        state.gc.total_ms += state.gc.last_ms;
                    } else {
                        state.gc.denied = state.gc.denied.saturating_add(1);
                        state.gc.next =
                            after(super::super::super::unix_milliseconds(), config.retry);
                    }
                }
                catalog.gc_running.store(false, Ordering::Release);
            }
        };
        #[cfg(not(target_os = "wasi"))]
        let spawned = std::thread::Builder::new()
            .name("ffkv-search-gc".into())
            .spawn(work);
        // WASI has no threads. No catalog lock is held here, so collect inline.
        #[cfg(target_os = "wasi")]
        let spawned = {
            work();
            std::io::Result::Ok(())
        };
        if let Err(error) = spawned {
            let mut state = self.lock();
            state.gc.failures = state.gc.failures.saturating_add(1);
            state.gc.next = after(now, retry);
            self.gc_running.store(false, Ordering::Release);
            eprintln!("cannot start search garbage collection: {error}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::Effect;
    use super::*;
    use bytes::Bytes;
    fn wait(catalog: &Catalog) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while catalog.gc_running() {
            assert!(Instant::now() < deadline, "collector did not finish");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    #[test]
    fn scheduled_gc_reclaims_holes_preserves_pinned_snapshots_and_rejects_stale_work() {
        let catalog = Arc::new(Catalog::new());
        let args = [
            "FT.CREATE",
            "idx",
            "SKIPINITIALSCAN",
            "SCHEMA",
            "title",
            "TEXT",
        ]
        .map(|s| Bytes::copy_from_slice(s.as_bytes()));
        let (schema, _) = super::super::super::schema::create(&args, 0).unwrap();
        let pinned;
        let baseline;
        {
            let mut state = catalog.lock();
            state.effect(Effect::Create(schema)).unwrap();
            for id in 1..601 {
                state
                    .effect(Effect::Header {
                        index: "idx".into(),
                        id,
                        key: format!("doc:{id}").into_bytes(),
                        score: 1.,
                        expires_at: None,
                        field_deadline: None,
                        length: 0,
                        synonym_generation: 0,
                    })
                    .unwrap();
            }
            pinned = state.snapshot();
            for id in 1..591 {
                state
                    .effect(Effect::Remove {
                        index: "idx".into(),
                        id,
                    })
                    .unwrap();
            }
            baseline = state.snapshot().map(Result::unwrap).collect::<Vec<_>>();
            state.config.gc.threshold = 1;
            state.config.gc.interval = 1;
            state.config.gc.retry = 1;
            state.config.gc.sleep = 1;
        }
        catalog.collect(0);
        assert!(catalog.gc_running());
        assert!(
            catalog.state.try_lock().is_ok(),
            "collector sleep held catalog lock"
        );
        catalog
            .lock()
            .effect(Effect::Alias {
                alias: "view".into(),
                index: Some("idx".into()),
            })
            .unwrap();
        wait(&catalog);
        assert_eq!(catalog.lock().gc.denied, 1);
        {
            let mut state = catalog.lock();
            state.config.gc.sleep = 0;
            state.gc.next = 0;
        }
        catalog.collect(0);
        wait(&catalog);
        {
            let state = catalog.lock();
            assert_eq!(state.gc.cycles, 1);
            assert!(state.gc.bytes_collected > 0);
            assert_eq!(state.indexes["idx"].docs.len(), 10);
            assert_eq!(state.gc.dirty, 0);
        }
        let mut original = State::default();
        for effect in pinned {
            original.apply_bytes(effect.unwrap()).unwrap();
        }
        assert_eq!(original.indexes["idx"].docs.len(), 600);
        let mut restored = State::default();
        for effect in baseline {
            restored.apply_bytes(effect).unwrap();
        }
        assert_eq!(restored.indexes["idx"].docs.len(), 10);
    }
}
