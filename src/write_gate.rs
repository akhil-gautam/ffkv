//! Separate write admission from shard ownership: migrations may pause user writes
//! while acquiring short dataset barriers for export/import and replication work.
//!
//! Admission is striped across OS-thread lanes to reduce shared cache-line
//! contention; collisions, including those caused by thread churn, are safe.
//! A pause takes the FIFO `global` lock and then every lane;
//! admissions it turns away queue on `global` behind its writer.
use std::{
    io,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Poll, ready},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

/// Bounds the lanes, and so the locks one pause acquires.
const MAX_LANES: usize = 64;

/// Aligns each lock to 128 bytes, an Apple silicon cache line and an x86
/// adjacent-line prefetch pair, so no two locks share one.
#[repr(align(128))]
struct Line;

type Lock = Arc<RwLock<Line>>;

/// A write admission: while any is alive, `WriteGate::pause` cannot finish.
#[must_use = "dropping a write permit lets a write pause overtake the command"]
pub struct Permit {
    _guard: OwnedRwLockReadGuard<Line>,
}

/// A stored pause. Fields drop in declaration order: the lanes reopen before
/// `global` hands the queued admissions their turn.
struct Held {
    _lanes: Vec<OwnedRwLockWriteGuard<Line>>,
    _global: OwnedRwLockWriteGuard<Line>,
}

/// Clears `closing` when a pause attempt ends without storing its pause.
struct Closing<'a>(&'a AtomicBool);

impl Drop for Closing<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}

pub struct WriteGate {
    /// The fair queue for admissions a pause turned away. A pause takes it
    /// first, so they wait behind the pause in arrival order.
    global: Lock,
    /// A power-of-two count, fixed at construction: a pause always sees all.
    lanes: Box<[Lock]>,
    /// Set while a pause is pending or held. New admissions then skip the
    /// lanes and join the global queue; taking all locks excludes permits
    /// from a held pause.
    closing: AtomicBool,
    pause_serial: AsyncMutex<()>,
    held: Mutex<Option<Held>>,
    generation: AtomicU64,
}

impl Default for WriteGate {
    fn default() -> Self {
        // Twice the cores leaves room for admitting threads besides the
        // workers before two threads share a lane.
        let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        Self::with_lanes(cores.saturating_mul(2).min(MAX_LANES).next_power_of_two())
    }
}

impl WriteGate {
    fn with_lanes(count: usize) -> Self {
        debug_assert!(count.is_power_of_two());
        let lock = || Arc::new(RwLock::new(Line));
        Self {
            global: lock(),
            lanes: (0..count).map(|_| lock()).collect(),
            closing: AtomicBool::new(false),
            pause_serial: AsyncMutex::new(()),
            held: Mutex::new(None),
            generation: AtomicU64::new(0),
        }
    }

    pub async fn admit(&self) -> Permit {
        // Spend the one cooperative budget unit a lock acquisition costs, and
        // yield holding nothing once the budget is exhausted. A turned-away
        // admission returns the unit; its queued read lock charges its own.
        let admitted = std::future::poll_fn(|cx| {
            let unit = ready!(tokio::task::coop::poll_proceed(cx));
            let permit = self.try_admit();
            if permit.is_some() {
                unit.made_progress();
            }
            Poll::Ready(permit)
        })
        .await;
        match admitted {
            Some(permit) => permit,
            None => Permit {
                _guard: self.global.clone().read_owned().await,
            },
        }
    }

    pub fn try_admit(&self) -> Option<Permit> {
        self.try_admit_lane(thread_lane())
    }

    fn try_admit_lane(&self, lane: usize) -> Option<Permit> {
        if self.closing.load(Ordering::SeqCst) {
            return None;
        }
        let guard = self.lanes[lane & (self.lanes.len() - 1)]
            .clone()
            .try_read_owned()
            .ok()?;
        // A pause that began meanwhile may not have reached this lane yet;
        // releasing it keeps the pause from waiting on a later admission.
        if self.closing.load(Ordering::SeqCst) {
            return None;
        }
        Some(Permit { _guard: guard })
    }

    pub async fn pause(&self) -> io::Result<()> {
        let _serial = self.pause_serial.lock().await;
        if self.is_paused() {
            return Ok(());
        }
        let generation = self.generation.load(Ordering::Acquire);
        // Declared after `_serial`, so a failed or cancelled attempt releases
        // what it drained and clears the flag before the next pause may start.
        self.closing.store(true, Ordering::SeqCst);
        let closing = Closing(&self.closing);
        let global = self.global.clone().write_owned().await;
        let mut lanes = Vec::with_capacity(self.lanes.len());
        for lane in &self.lanes {
            lanes.push(lane.clone().write_owned().await);
        }
        let mut held = self.held.lock().expect("write pause state");
        if self.generation.load(Ordering::Acquire) != generation {
            return Err(io::Error::new(
                io::ErrorKind::Interrupted,
                "write pause cancelled",
            ));
        }
        *held = Some(Held {
            _lanes: lanes,
            _global: global,
        });
        // The stored pause keeps the flag; `resume` clears both together.
        std::mem::forget(closing);
        Ok(())
    }

    pub fn resume(&self) {
        let mut held = self.held.lock().expect("write pause state");
        self.generation.fetch_add(1, Ordering::AcqRel);
        // A pending pause instead fails its generation check and clears the
        // flag itself. Clearing under `held` keeps this from erasing the flag
        // of a pause that starts once this one is gone.
        if let Some(paused) = held.take() {
            drop(paused);
            self.closing.store(false, Ordering::SeqCst);
        }
    }

    pub fn is_paused(&self) -> bool {
        self.held.lock().expect("write pause state").is_some()
    }
}

/// Numbers each thread on its first admission or command completion. Mapping
/// that number to a lane is stable for the thread; collisions only share an
/// existing lock or counter line.
pub(crate) fn thread_lane() -> usize {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    thread_local! {
        static LANE: usize = NEXT.fetch_add(1, Ordering::Relaxed);
    }
    LANE.with(|lane| *lane)
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn,
        pin::pin,
        task::{Context, Poll},
    };

    use futures_util::FutureExt;
    use tokio::task::coop;

    use super::*;

    #[test]
    fn a_pause_drains_every_lane_and_queued_permit_and_admits_none_meanwhile() {
        let gate = WriteGate::with_lanes(4);
        // One permit queued on `global` behind a held pause, one on each lane.
        assert!(matches!(gate.pause().now_or_never(), Some(Ok(()))));
        let mut queued = pin!(gate.admit());
        assert!(queued.as_mut().now_or_never().is_none());
        gate.resume();
        let global = queued
            .now_or_never()
            .expect("resume admits the queued write");
        let lanes: Vec<_> = (0..4)
            .map(|lane| gate.try_admit_lane(lane).expect("an open lane admits"))
            .collect();

        let mut pause = pin!(gate.pause());
        assert!(pause.as_mut().now_or_never().is_none());
        assert!(!gate.is_paused());
        assert!((0..4).all(|lane| gate.try_admit_lane(lane).is_none()));
        let mut late = pin!(gate.admit());
        assert!(late.as_mut().now_or_never().is_none());

        drop(global);
        for permit in lanes {
            assert!(
                pause.as_mut().now_or_never().is_none(),
                "the pause waits for every admitted write"
            );
            drop(permit);
        }
        assert!(matches!(pause.now_or_never(), Some(Ok(()))));
        assert!(gate.is_paused());
        assert!((0..4).all(|lane| gate.try_admit_lane(lane).is_none()));
        assert!(late.as_mut().now_or_never().is_none());
        gate.resume();
        assert!(late.now_or_never().is_some());
    }

    #[test]
    fn a_cancelled_pause_reopens_what_it_drained() {
        let gate = WriteGate::with_lanes(4);
        let admitted = gate.try_admit_lane(2).expect("an open lane admits");
        let mut pause = Box::pin(gate.pause());
        // It holds `global` and lanes 0 and 1, waiting on lane 2.
        assert!(pause.as_mut().now_or_never().is_none());
        let mut queued = pin!(gate.admit());
        assert!(queued.as_mut().now_or_never().is_none());
        assert!(gate.try_admit_lane(0).is_none());

        drop(pause);
        let queued = queued
            .now_or_never()
            .expect("cancelling the pause admits the queued write");
        let reopened: Vec<_> = (0..4)
            .map(|lane| gate.try_admit_lane(lane).expect("every lane reopens"))
            .collect();
        assert!(!gate.is_paused());
        drop((admitted, queued, reopened));
        assert!(matches!(gate.pause().now_or_never(), Some(Ok(()))));
        gate.resume();
    }

    #[test]
    fn a_pause_resumed_while_pending_fails_and_then_reopens_admission() {
        let gate = WriteGate::with_lanes(2);
        let admitted = gate.try_admit_lane(1).expect("an open lane admits");
        let mut pause = pin!(gate.pause());
        assert!(pause.as_mut().now_or_never().is_none());
        gate.resume();
        // Until the pending pause settles, it still holds new admissions off.
        assert!(gate.try_admit_lane(0).is_none());
        let mut queued = pin!(gate.admit());
        assert!(queued.as_mut().now_or_never().is_none());

        drop(admitted);
        let error = pause
            .now_or_never()
            .expect("the drained pause settles")
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(!gate.is_paused());
        assert!(queued.now_or_never().is_some());
        assert!((0..2).all(|lane| gate.try_admit_lane(lane).is_some()));
    }

    #[test]
    fn pausing_a_paused_gate_borrows_the_held_pause() {
        let gate = WriteGate::with_lanes(2);
        assert!(matches!(gate.pause().now_or_never(), Some(Ok(()))));
        assert!(
            matches!(gate.pause().now_or_never(), Some(Ok(()))),
            "the second pause returns at once, owning nothing"
        );
        assert!(gate.is_paused());
        assert!(gate.try_admit().is_none());
        gate.resume();
        assert!(!gate.is_paused());
        assert!(gate.try_admit().is_some());
    }

    #[test]
    fn a_queued_admission_precedes_a_later_pause() {
        let gate = WriteGate::with_lanes(2);
        assert!(matches!(gate.pause().now_or_never(), Some(Ok(()))));
        let mut queued = pin!(gate.admit());
        assert!(queued.as_mut().now_or_never().is_none());
        gate.resume();
        let mut pause = pin!(gate.pause());
        assert!(pause.as_mut().now_or_never().is_none());
        let permit = queued
            .now_or_never()
            .expect("the queued admission is not overtaken");
        assert!(pause.as_mut().now_or_never().is_none());
        drop(permit);
        assert!(matches!(pause.now_or_never(), Some(Ok(()))));
        gate.resume();
    }

    /// Spends the rest of this poll's cooperative budget, counting its units.
    fn spend(cx: &mut Context<'_>) -> usize {
        let mut units = 0;
        while let Poll::Ready(unit) = coop::poll_proceed(cx) {
            unit.made_progress();
            units += 1;
        }
        units
    }

    #[tokio::test]
    async fn admission_spends_one_budget_unit_and_holds_nothing_while_yielding() {
        let gate = WriteGate::with_lanes(1);
        tokio::task::yield_now().await;
        let fresh = poll_fn(|cx| Poll::Ready(spend(cx))).await;

        // A lane admission spends one unit, as a read lock does.
        tokio::task::yield_now().await;
        let mut admit = pin!(gate.admit());
        let (permit, spare) = poll_fn(|cx| match admit.as_mut().poll(cx) {
            Poll::Ready(permit) => Poll::Ready((permit, spend(cx))),
            Poll::Pending => panic!("an open lane admits at once"),
        })
        .await;
        assert_eq!(spare + 1, fresh);
        drop(permit);

        // With the budget spent, admission yields before taking a lane.
        tokio::task::yield_now().await;
        let mut admit = pin!(gate.admit());
        poll_fn(|cx| {
            spend(cx);
            assert!(admit.as_mut().poll(cx).is_pending());
            let paused = coop::unconstrained(gate.pause()).now_or_never();
            assert!(
                matches!(paused, Some(Ok(()))),
                "the yielding admission holds no lane"
            );
            gate.resume();
            Poll::Ready(())
        })
        .await;
        drop(admit.await);

        // A turned-away admission spends nothing while it queues, then one
        // unit when its turn comes.
        gate.pause().await.unwrap();
        let mut queued = pin!(gate.admit());
        tokio::task::yield_now().await;
        let spare = poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_pending());
            Poll::Ready(spend(cx))
        })
        .await;
        assert_eq!(spare, fresh);
        gate.resume();
        tokio::task::yield_now().await;
        let spare = poll_fn(|cx| {
            assert!(queued.as_mut().poll(cx).is_ready());
            Poll::Ready(spend(cx))
        })
        .await;
        assert_eq!(spare + 1, fresh);
    }
}
