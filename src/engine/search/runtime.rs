use super::model::Config;
use std::{
    cmp::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};
impl Config {
    pub(super) fn set_workers(&mut self, workers: usize) -> Result<(), String> {
        if workers == self.workers {
            return Ok(());
        }
        if workers == 0 && self.on_timeout_strict {
            return Err("RETURN_STRICT requires WORKERS greater than zero".into());
        }
        let pool = if workers == 0 {
            None
        } else {
            Some(Arc::new(
                rayon::ThreadPoolBuilder::new()
                    .num_threads(workers)
                    .thread_name(|n| format!("ffkv-search-{n}"))
                    .build()
                    .map_err(|e| format!("Cannot create search workers: {e}"))?,
            ))
        };
        self.pool = pool;
        self.workers = workers;
        Ok(())
    }
    pub(super) fn run<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        match &self.pool {
            Some(pool) => pool.install(work),
            None => work(),
        }
    }
    pub(super) fn timeout_capped(&self, timeout: u64) -> bool {
        self.workers == 0
            && self.foreground_timeout_ms > 0
            && (timeout == 0 || timeout > self.foreground_timeout_ms)
    }
    pub(super) fn deadline(&self, started: Instant, timeout: u64) -> Option<Instant> {
        let timeout = if self.workers == 0 && self.foreground_timeout_ms > 0 {
            if timeout == 0 {
                self.foreground_timeout_ms
            } else {
                timeout.min(self.foreground_timeout_ms)
            }
        } else {
            timeout
        };
        (timeout > 0)
            .then(|| started.checked_add(Duration::from_millis(timeout)))
            .flatten()
    }
    pub(super) fn enforce_deadline(&self) -> bool {
        self.on_timeout_strict || self.on_timeout_fail
    }
}
pub(super) fn expired(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|d| Instant::now() >= d)
}
thread_local! {static DEADLINE:std::cell::Cell<Option<Instant>>=const{std::cell::Cell::new(None)};}
pub(super) struct DeadlineGuard(Option<Instant>);
impl DeadlineGuard {
    pub(super) fn enter(deadline: Option<Instant>) -> Self {
        let old = DEADLINE.with(|d| {
            let old = d.get();
            d.set(match (old, deadline) {
                (Some(a), Some(b)) => Some(a.min(b)),
                (a, b) => a.or(b),
            });
            old
        });
        Self(old)
    }
}
impl Drop for DeadlineGuard {
    fn drop(&mut self) {
        DEADLINE.with(|d| d.set(self.0));
    }
}
pub(super) fn current_deadline() -> Option<Instant> {
    DEADLINE.with(std::cell::Cell::get)
}
pub(super) fn cancelled() -> bool {
    expired(current_deadline())
}
/// In-place heapsort with cancellation points; unlike changing a comparator at a
/// deadline, this preserves the comparator contract. Incomplete ordering is never returned.
pub(super) fn sort<T>(
    values: &mut Vec<T>,
    mut compare: impl FnMut(&T, &T) -> Ordering,
    deadline: Option<Instant>,
) -> bool {
    if deadline.is_none() {
        values.sort_by(compare);
        return true;
    }
    fn sift<T>(
        values: &mut [T],
        mut root: usize,
        end: usize,
        compare: &mut impl FnMut(&T, &T) -> Ordering,
        deadline: Option<Instant>,
    ) -> bool {
        loop {
            if expired(deadline) {
                return false;
            }
            let Some(left) = root
                .checked_mul(2)
                .and_then(|n| n.checked_add(1))
                .filter(|n| *n < end)
            else {
                return true;
            };
            let right = left + 1;
            let child = if right < end && compare(&values[left], &values[right]).is_lt() {
                right
            } else {
                left
            };
            if !compare(&values[root], &values[child]).is_lt() {
                return true;
            }
            values.swap(root, child);
            root = child;
        }
    }
    let n = values.len();
    for root in (0..n / 2).rev() {
        if !sift(values, root, n, &mut compare, deadline) {
            values.clear();
            return false;
        }
    }
    for end in (1..n).rev() {
        values.swap(0, end);
        if !sift(values, 0, end, &mut compare, deadline) {
            values.clear();
            return false;
        }
    }
    if expired(deadline) {
        values.clear();
        false
    } else {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn configured_workers_execute_on_real_named_threads_and_resize() {
        let mut config = Config::default();
        let foreground = std::thread::current().id();
        assert_eq!(config.run(|| std::thread::current().id()), foreground);
        config.set_workers(2).unwrap();
        assert_eq!(config.pool.as_ref().unwrap().current_num_threads(), 2);
        let (worker, name) = config.run(|| {
            (
                std::thread::current().id(),
                std::thread::current().name().unwrap().to_owned(),
            )
        });
        assert_ne!(worker, foreground);
        assert!(name.starts_with("ffkv-search-"));
        config.set_workers(1).unwrap();
        assert_eq!(config.pool.as_ref().unwrap().current_num_threads(), 1);
        config.on_timeout_strict = true;
        assert!(config.set_workers(0).is_err());
        assert_eq!(config.workers, 1);
        config.on_timeout_strict = false;
        config.set_workers(0).unwrap();
        assert_eq!(config.run(|| std::thread::current().id()), foreground);
    }
    #[test]
    fn cancellable_ordering_is_sorted_or_empty_and_restores_nested_deadlines() {
        let mut values = vec![5, 2, 7, 1, 4, 3];
        assert!(sort(
            &mut values,
            Ord::cmp,
            Some(Instant::now() + Duration::from_secs(1))
        ));
        assert_eq!(values, vec![1, 2, 3, 4, 5, 7]);
        assert!(!sort(
            &mut values,
            Ord::cmp,
            Some(Instant::now() - Duration::from_secs(1))
        ));
        assert!(values.is_empty());
        assert!(!cancelled());
        {
            let _guard = DeadlineGuard::enter(Some(Instant::now() - Duration::from_secs(1)));
            assert!(cancelled());
            {
                let _nested = DeadlineGuard::enter(None);
                assert!(cancelled());
            }
            assert!(cancelled());
        }
        assert!(!cancelled());
    }
}
