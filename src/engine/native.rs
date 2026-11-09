//! Native jobs share Redis key ownership, TTL, mutation capture and snapshot lifecycle.
//! Replicas and source-owned ASM slots hold queue timers as well as key TTLs;
//! inspection never substitutes a local transition for a resolved source effect.

use super::{Engine, Entry, Value, invalid_journal, wrong_type};
use crate::{
    journal::Mutation,
    protocol::Resp,
    queue::{Effect, Operation, Output, Queue},
    storage::Store,
};
use bytes::Bytes;
use std::{io, sync::Arc};

fn creates(operation: &Operation) -> bool {
    match operation {
        Operation::Enqueue { .. } => true,
        Operation::Config(update) => {
            update.concurrency.is_some()
                || update.max_attempts.is_some()
                || update.backoff_ms.is_some()
                || update.max_backoff_ms.is_some()
                || update.idempotency_ms.is_some()
        }
        _ => false,
    }
}

impl Engine {
    pub(super) fn own_key(&self, key: &Bytes) -> Bytes {
        // A key must not retain an old value's entire network frame after
        // replacement, deletion of sibling arguments, or disk offloading.
        Bytes::copy_from_slice(key)
    }

    pub(super) fn queue_deadline(&self, key: &Bytes) -> Option<i64> {
        match self.entries.get(key).map(|entry| &entry.value) {
            Some(Value::Queue(queue)) => queue.next_deadline(),
            _ => None,
        }
    }

    fn refresh_queue_deadline(&mut self, key: &Bytes, previous: Option<i64>) {
        if let Some(previous) = previous {
            self.queue_expirations.remove(&(previous, key.clone()));
        }
        if let Some(deadline) = self.queue_deadline(key) {
            // Index metadata must use the stored owned key, not an ENQUEUE frame
            // that also holds an offloaded multi-megabyte body.
            let key = self
                .entries
                .get_key_value(key)
                .expect("native queue entry")
                .0
                .clone();
            self.queue_expirations.insert((deadline, key));
        }
    }

    fn record_queue_effects(
        &mut self,
        key: &Bytes,
        effects: Vec<Effect>,
        now: i64,
    ) -> io::Result<()> {
        if !self.journal_enabled {
            return Ok(());
        }
        for effect in effects {
            let effect = effect.encode()?;
            self.record(now, || Mutation::Queue {
                key: key.clone(),
                effect,
            });
        }
        Ok(())
    }

    pub(super) fn native_operation(
        &mut self,
        key: &Bytes,
        operation: Operation,
        now: i64,
    ) -> io::Result<Result<Output, Resp>> {
        self.discard_expired(key, now);
        if self
            .entries
            .get(key)
            .is_some_and(|entry| !matches!(entry.value, Value::Queue(_)))
        {
            return Ok(Err(wrong_type()));
        }
        let storage: Option<Arc<Store>> = self.storage.clone();
        let advance_time = !self.holding_expiry(key);
        let previous = self.queue_deadline(key);
        let outcome = if let Some(Entry {
            value: Value::Queue(queue),
            ..
        }) = self.entries.get_mut(key)
        {
            queue.execute(
                operation,
                now,
                advance_time,
                storage.as_ref(),
                self.reply_limit,
            )?
        } else {
            let create = creates(&operation);
            let mut queue = Queue::new();
            let outcome = queue.execute(
                operation,
                now,
                advance_time,
                storage.as_ref(),
                self.reply_limit,
            )?;
            if !create || outcome.effects.is_empty() {
                return Ok(outcome
                    .result
                    .map_err(|error| Resp::Error(format!("ERR {error}"))));
            }
            self.write_entry(
                self.own_key(key),
                Entry {
                    value: Value::Queue(Box::new(queue)),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
            outcome
        };
        self.refresh_queue_deadline(key, previous);
        self.record_queue_effects(key, outcome.effects, now)?;
        Ok(outcome
            .result
            .map_err(|error| Resp::Error(format!("ERR {error}"))))
    }

    pub(super) fn replay_queue(&mut self, key: Bytes, bytes: Bytes, now: i64) -> io::Result<()> {
        let effect = Effect::decode(&bytes)?;
        self.discard_expired(&key, now);
        let previous = self.queue_deadline(&key);
        let storage = self.storage.clone();
        let value = self.collection_entry(&key, now, || Value::Queue(Box::default()));
        let Value::Queue(queue) = value else {
            return Err(invalid_journal());
        };
        queue.apply(effect, storage.as_ref())?;
        self.refresh_queue_deadline(&key, previous);
        Ok(())
    }

    /// One visit per due queue per tick; hot backlogs cannot repeatedly occupy
    /// the head of the deadline index while other queues are waiting.
    pub fn maintain_native(&mut self, now: i64, mut budget: usize) -> io::Result<()> {
        if self.replica_mode || self.replication_replay {
            return Ok(());
        }
        let mut reschedule = Vec::new();
        while budget > 0 {
            let Some((deadline, _)) = self.queue_expirations.first() else {
                break;
            };
            if *deadline > now {
                break;
            }
            let (deadline, key) = self.queue_expirations.pop_first().expect("due queue");
            if self.holding_expiry(&key) {
                reschedule.push((deadline, key));
                continue;
            }
            self.discard_expired(&key, now);
            let Some(Entry {
                value: Value::Queue(queue),
                ..
            }) = self.entries.get_mut(&key)
            else {
                continue;
            };
            let allowance = budget.min(16);
            let effects = queue.advance(now, allowance)?;
            // A visit consumes at least one unit even if an index was already
            // obsolete, preventing a malformed empty transition spin.
            budget = budget.saturating_sub(effects.len().max(1));
            let next = queue.next_deadline();
            self.record_queue_effects(&key, effects, now)?;
            if let Some(next) = next {
                reschedule.push((next, key));
            }
        }
        self.queue_expirations.extend(reschedule);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queue::JobState;

    fn seeded_pair(migration: bool) -> (Engine, Engine, Bytes, Bytes) {
        let key = Bytes::from_static(b"source-owned-queue");
        let mut source = Engine::new();
        source.set_recording(true);
        let output = source
            .native_operation(
                &key,
                Operation::Enqueue {
                    payload: Bytes::from_static(b"retained-body"),
                    idempotency: None,
                    at_ms: Some(20),
                    priority: 0,
                    max_attempts: None,
                },
                10,
            )
            .unwrap()
            .unwrap();
        let Output::Enqueued { job_id, .. } = output else {
            panic!("enqueue reply");
        };
        let mut follower = Engine::new();
        follower.set_replica_mode(!migration);
        if migration {
            let slot = crate::slot::key_slot(&key);
            follower.set_migration_expiry_ranges(Some(&[(slot, slot)]));
        }
        follower.set_replication_replay(true);
        follower.replay(source.take_batch().unwrap()).unwrap();
        follower.set_replication_replay(false);
        follower.set_recording(true);
        (source, follower, key, job_id)
    }

    fn assert_scheduled(follower: &mut Engine, key: &Bytes, job_id: &Bytes) {
        let Output::Job(Some(job)) = follower
            .native_operation(
                key,
                Operation::Get {
                    job_id: job_id.clone(),
                },
                30,
            )
            .unwrap()
            .unwrap()
        else {
            panic!("held job disappeared");
        };
        assert_eq!(job.info.state, JobState::Scheduled);
        assert_eq!(job.payload, Bytes::from_static(b"retained-body"));
        let Output::Stats(stats) = follower
            .native_operation(key, Operation::Stats, 30)
            .unwrap()
            .unwrap()
        else {
            panic!("stats reply");
        };
        assert_eq!((stats.scheduled, stats.ready), (1, 0));
        assert!(
            follower.take_batch().is_none(),
            "inspection invented source history"
        );
    }

    fn replay_source_promotion(
        source: &mut Engine,
        follower: &mut Engine,
        key: &Bytes,
        job_id: Bytes,
    ) {
        source.maintain_native(30, 16).unwrap();
        let batch = source.take_batch().expect("source promotion");
        follower.set_replication_replay(true);
        follower
            .replay(batch.clone())
            .expect("source effect applies once");
        let Output::Job(Some(job)) = follower
            .native_operation(key, Operation::Get { job_id }, 30)
            .unwrap()
            .unwrap()
        else {
            panic!("promoted job disappeared");
        };
        assert_eq!(job.info.state, JobState::Ready);
        assert!(
            follower.replay(batch).is_err(),
            "duplicate source transition must remain invalid"
        );
    }

    #[test]
    fn native_history_replica_background_does_not_promote_source_job() {
        let (mut source, mut follower, key, id) = seeded_pair(false);
        follower.maintain_native(30, 16).unwrap();
        assert_scheduled(&mut follower, &key, &id);
        replay_source_promotion(&mut source, &mut follower, &key, id);
    }

    #[test]
    fn native_history_replica_readonly_get_and_stats_do_not_promote_source_job() {
        let (mut source, mut follower, key, id) = seeded_pair(false);
        assert_scheduled(&mut follower, &key, &id);
        replay_source_promotion(&mut source, &mut follower, &key, id);
    }

    #[test]
    fn native_history_migration_hold_preserves_source_job_until_replay() {
        let (mut source, mut follower, key, id) = seeded_pair(true);
        follower.maintain_native(30, 16).unwrap();
        assert_scheduled(&mut follower, &key, &id);
        replay_source_promotion(&mut source, &mut follower, &key, id);
    }
}
