//! Client runtimes. Each accepted client is served on one of several
//! single-worker runtimes, taken in turn at accept, so its socket IO, timers
//! and inline commands, and any supervised commit it starts, run on that
//! runtime; the main runtime keeps the coordinator and services. Unlike
//! `current_thread`, a single-worker multi-thread runtime still lets
//! `block_in_place` hand its worker to another thread.
//!
//! Building or dropping a `Runtime` may block, which an async thread forbids,
//! so both run on blocking threads. Runtimes cross every await inside
//! `Frontends`, whose drop without `shutdown` (an early return, an unwind, or
//! a construction or shutdown future dropped before its blocking job ran or
//! was consumed) falls back to stopping them in the background.
use std::io;

use tokio::runtime::{Builder, Handle, Runtime};

/// The client runtimes, served round-robin.
pub struct Frontends {
    /// Empty only once a blocking thread took them to join.
    runtimes: Vec<Runtime>,
    /// Serves the next accepted client.
    next: usize,
}

impl Frontends {
    /// Builds `count` runtimes, at least one, each with IO and time drivers.
    /// A failed build joins those already built on the same blocking thread.
    pub async fn new(count: usize) -> io::Result<Self> {
        tokio::task::spawn_blocking(move || {
            let mut frontends = Self {
                runtimes: Vec::with_capacity(count.max(1)),
                next: 0,
            };
            for index in 0..count.max(1) {
                match Builder::new_multi_thread()
                    .worker_threads(1)
                    // Wakes from other threads, such as a shard worker's
                    // answer to a contended command, wait in the global queue.
                    // With short polls the self-tuned interval reaches 127
                    // local ticks; every other tick runs those wakes promptly
                    // while local tasks still alternate with them.
                    .global_queue_interval(2)
                    .thread_name(format!("ffkv-front-{index}"))
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => frontends.runtimes.push(runtime),
                    Err(error) => {
                        drop(std::mem::take(&mut frontends.runtimes));
                        return Err(error);
                    }
                }
            }
            Ok(frontends)
        })
        .await
        .map_err(io::Error::other)?
    }

    /// The runtime for the next accepted client.
    pub fn next_handle(&mut self) -> &Handle {
        let runtime = &self.runtimes[self.next];
        self.next = (self.next + 1) % self.runtimes.len();
        runtime.handle()
    }

    /// Stops every runtime and returns once their remaining tasks are dropped
    /// and their threads have exited. Work a client started, such as a
    /// supervised commit or an administration job, runs on its runtime even
    /// after the client is gone, so call this only once the coordinator has
    /// drained and every service has stopped.
    pub async fn shutdown(self) -> io::Result<()> {
        tokio::task::spawn_blocking(move || {
            let mut frontends = self;
            // Joined here, on a blocking thread; the emptied owner's drop
            // then has nothing left to stop.
            drop(std::mem::take(&mut frontends.runtimes));
        })
        .await
        .map_err(io::Error::other)
    }
}

impl Drop for Frontends {
    fn drop(&mut self) {
        // Reached with runtimes only off the blocking paths above, likely on
        // an async thread, where a blocking drop panics: their tasks stop in
        // the background instead, without waiting for them.
        for runtime in self.runtimes.drain(..) {
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::oneshot::{self, error::TryRecvError};

    #[tokio::test]
    async fn shutdown_returns_once_every_frontend_task_is_dropped() {
        let mut frontends = Frontends::new(2).await.unwrap();
        let mut parked = Vec::new();
        for _ in 0..2 {
            let (held, receiver) = oneshot::channel::<()>();
            frontends.next_handle().spawn(async move {
                let _held = held;
                std::future::pending::<()>().await
            });
            parked.push(receiver);
        }
        for receiver in &mut parked {
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Empty)));
        }
        frontends.shutdown().await.unwrap();
        // Nothing a frontend ran is left once `shutdown` returns.
        for receiver in &mut parked {
            assert!(matches!(receiver.try_recv(), Err(TryRecvError::Closed)));
        }
    }

    #[tokio::test]
    async fn dropping_frontends_on_an_async_thread_stops_them_without_blocking() {
        let mut frontends = Frontends::new(1).await.unwrap();
        let parked = frontends.next_handle().spawn(std::future::pending::<()>());
        // A blocking drop here would panic; the fallback cancels the task.
        drop(frontends);
        let ended = tokio::time::timeout(Duration::from_secs(10), parked)
            .await
            .expect("the frontend task ends");
        assert!(ended.unwrap_err().is_cancelled());
    }
}
