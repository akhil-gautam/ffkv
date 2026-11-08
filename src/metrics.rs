//! Bounded, since-boot operational measurements. Latencies are bucket upper bounds.
use serde::Serialize;
use std::num::NonZeroUsize;
use std::sync::{
    Mutex, RwLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
const BUCKETS: [u64; 20] = [
    1,
    2,
    4,
    8,
    16,
    32,
    64,
    128,
    256,
    512,
    1024,
    2048,
    4096,
    8192,
    16384,
    32768,
    65536,
    131072,
    1048576,
    u64::MAX,
];
const NONE: u64 = u64::MAX;
/// Bounds the command counter lanes a snapshot sums.
const MAX_LANES: usize = 64;
#[derive(Clone, Serialize)]
pub struct Latency {
    pub samples: u64,
    pub p50_us: Option<u64>,
    pub p95_us: Option<u64>,
    pub p99_us: Option<u64>,
}
#[derive(Clone, Serialize)]
pub struct Wal {
    pub bytes: u64,
    pub policy: String,
    pub dirty: bool,
    pub last_sync_ms: Option<i64>,
}
#[derive(Clone, Serialize)]
pub struct Replication {
    pub role: String,
    pub status: String,
    pub lag: Option<u64>,
}
#[derive(Clone, Serialize)]
pub struct Server {
    pub shards: u16,
    pub storage_mode: String,
    pub durability: String,
}
#[derive(Clone, Serialize)]
pub struct RssPressure {
    pub max_rss_bytes_soft: u64,
    pub rss_bytes: Option<u64>,
    pub rss_pressured: bool,
}
#[derive(Clone, Serialize)]
pub struct MetricsSnapshot {
    pub observed_at_ms: u64,
    pub uptime_seconds: f64,
    pub commands_total: u64,
    pub errors_total: u64,
    pub connections: u64,
    pub blocked_clients: u64,
    pub rss_bytes: Option<u64>,
    pub max_rss_bytes_soft: u64,
    pub rss_pressured: bool,
    pub latency: Latency,
    pub wal: Wal,
    pub replication: Replication,
    pub server: Server,
}
/// Completed commands counted by the threads of one lane. Each completed
/// command lands in exactly one bucket of one lane, so the bucket sum over
/// every lane is also the command count.
///
/// The 128-byte alignment gives each lane and `RssState` cache lines of their
/// own (128 bytes is the line size on Apple silicon and harmless on 64-byte-line
/// hosts), so completions on one thread write no line another thread's
/// completions or admission read.
#[repr(align(128))]
struct CommandCounters {
    buckets: [AtomicU64; BUCKETS.len()],
    latency_sum: AtomicU64,
    errors: AtomicU64,
}
impl CommandCounters {
    fn new() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        }
    }
}
/// Completed-command latencies counted locally, then published to the shared
/// counters in one step, so a batch costs one atomic add per touched bucket
/// rather than per command.
#[derive(Default)]
pub struct CommandLatencies {
    buckets: [u64; BUCKETS.len()],
    latency_sum: u64,
    errors: u64,
}
impl CommandLatencies {
    pub fn record(&mut self, elapsed: Duration, error: bool) {
        let us = micros(elapsed);
        self.buckets[bucket(us)] += 1;
        // Wraps as the shared `fetch_add` does.
        self.latency_sum = self.latency_sum.wrapping_add(us);
        self.errors += u64::from(error);
    }
}
fn micros(elapsed: Duration) -> u64 {
    elapsed.as_micros().min(u64::MAX as u128) as u64
}
fn bucket(us: u64) -> usize {
    BUCKETS
        .partition_point(|bound| *bound < us)
        .min(BUCKETS.len() - 1)
}
/// Latest process-RSS sample (`NONE` while unknown) and the soft limit. Written
/// at startup and by process sampling; read by admission for growth commands.
#[repr(align(128))]
struct RssState {
    sample: AtomicU64,
    soft_limit: AtomicU64,
}
pub struct Metrics {
    boot: Instant,
    /// A power-of-two count of lanes, each thread adding to the lane
    /// `write_gate::thread_lane` numbers it into; readers sum every lane,
    /// wrapping as each lane's counters do.
    commands: Box<[CommandCounters]>,
    rss: RssState,
    connections: AtomicU64,
    blocked: AtomicU64,
    wal: RwLock<Wal>,
    replication: RwLock<Replication>,
    server: RwLock<Server>,
    process: Mutex<System>,
}
impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
impl Metrics {
    pub fn new() -> Self {
        // As the write gate: twice the cores leaves room for threads besides
        // the workers before two threads share a lane.
        let cores = std::thread::available_parallelism().map_or(1, NonZeroUsize::get);
        let lanes = cores.saturating_mul(2).min(MAX_LANES).next_power_of_two();
        Self {
            boot: Instant::now(),
            commands: (0..lanes).map(|_| CommandCounters::new()).collect(),
            rss: RssState {
                sample: AtomicU64::new(NONE),
                soft_limit: AtomicU64::new(0),
            },
            connections: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
            wal: RwLock::new(Wal {
                bytes: 0,
                policy: "unknown".into(),
                dirty: false,
                last_sync_ms: None,
            }),
            replication: RwLock::new(Replication {
                role: "unknown".into(),
                status: "unknown".into(),
                lag: None,
            }),
            server: RwLock::new(Server {
                shards: 0,
                storage_mode: "unknown".into(),
                durability: "unknown".into(),
            }),
            process: Mutex::new(System::new()),
        }
    }
    /// The calling thread's command counters.
    fn lane(&self) -> &CommandCounters {
        &self.commands[crate::write_gate::thread_lane() & (self.commands.len() - 1)]
    }
    fn bucket_counts(&self) -> [u64; BUCKETS.len()] {
        let mut counts = [0u64; BUCKETS.len()];
        for lane in &self.commands {
            for (count, bucket) in counts.iter_mut().zip(&lane.buckets) {
                *count = count.wrapping_add(bucket.load(Ordering::Relaxed));
            }
        }
        counts
    }
    fn errors(&self) -> u64 {
        self.commands.iter().fold(0, |total, lane| {
            total.wrapping_add(lane.errors.load(Ordering::Relaxed))
        })
    }
    fn latency_sum(&self) -> u64 {
        self.commands.iter().fold(0, |total, lane| {
            total.wrapping_add(lane.latency_sum.load(Ordering::Relaxed))
        })
    }
    pub fn reset_statistics(&self) {
        for commands in &self.commands {
            commands.errors.store(0, Ordering::Relaxed);
            commands.latency_sum.store(0, Ordering::Relaxed);
            for bucket in &commands.buckets {
                bucket.store(0, Ordering::Relaxed);
            }
        }
    }
    pub fn connection_open(&self) {
        self.connections.fetch_add(1, Ordering::Relaxed);
    }
    pub fn connection_close(&self) {
        let _ = self
            .connections
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                Some(n.saturating_sub(1))
            });
    }
    pub fn blocked_clients(&self, n: usize) {
        self.blocked.store(n as u64, Ordering::Relaxed);
    }
    /// One completed command; see `commands_complete` for several.
    pub fn command_complete(&self, elapsed: Duration, error: bool) {
        let us = micros(elapsed);
        let commands = self.lane();
        commands.buckets[bucket(us)].fetch_add(1, Ordering::Relaxed);
        commands.latency_sum.fetch_add(us, Ordering::Relaxed);
        if error {
            commands.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// Publishes a batch counted by `CommandLatencies`. Each command still lands
    /// in exactly one bucket, so the bucket sum stays the command count.
    pub fn commands_complete(&self, latencies: &CommandLatencies) {
        let commands = self.lane();
        for (bucket, &count) in commands.buckets.iter().zip(&latencies.buckets) {
            if count != 0 {
                bucket.fetch_add(count, Ordering::Relaxed);
            }
        }
        if latencies.latency_sum != 0 {
            commands
                .latency_sum
                .fetch_add(latencies.latency_sum, Ordering::Relaxed);
        }
        if latencies.errors != 0 {
            commands
                .errors
                .fetch_add(latencies.errors, Ordering::Relaxed);
        }
    }
    pub fn set_wal(&self, bytes: u64, policy: &str, dirty: bool, last_sync_ms: Option<i64>) {
        *self.wal.write().unwrap_or_else(|p| p.into_inner()) = Wal {
            bytes,
            policy: policy.into(),
            dirty,
            last_sync_ms,
        };
    }
    pub fn set_replication(&self, role: &str, status: &str, lag: Option<u64>) {
        *self.replication.write().unwrap_or_else(|p| p.into_inner()) = Replication {
            role: role.into(),
            status: status.into(),
            lag,
        };
    }
    pub fn set_server(&self, shards: u16, storage_mode: &str, durability: &str) {
        *self.server.write().unwrap_or_else(|p| p.into_inner()) = Server {
            shards,
            storage_mode: storage_mode.into(),
            durability: durability.into(),
        };
    }
    pub fn set_max_rss_bytes_soft(&self, bytes: u64) {
        self.rss.soft_limit.store(bytes, Ordering::Relaxed);
    }

    /// Admission uses only the latest sample; it neither measures nor reserves
    /// memory per request. Unknown RSS cannot establish pressure.
    pub fn rss_pressure(&self) -> RssPressure {
        let rss = self.rss.sample.load(Ordering::Relaxed);
        let limit = self.rss.soft_limit.load(Ordering::Relaxed);
        RssPressure {
            max_rss_bytes_soft: limit,
            rss_bytes: (rss != NONE).then_some(rss),
            rss_pressured: limit != 0 && rss != NONE && rss >= limit,
        }
    }

    pub fn sample_process(&self) {
        let mut system = self.process.lock().unwrap_or_else(|p| p.into_inner());
        let pid = Pid::from_u32(std::process::id());
        system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            true,
            ProcessRefreshKind::nothing().with_memory(),
        );
        self.rss.sample.store(
            system.process(pid).map_or(NONE, |p| p.memory()),
            Ordering::Relaxed,
        );
    }
    pub fn snapshot(&self) -> MetricsSnapshot {
        self.snapshot_with(&self.bucket_counts())
    }
    /// `counts` is one load of the latency buckets; it yields both the
    /// quantiles and `commands_total`, so they always agree.
    fn snapshot_with(&self, counts: &[u64; BUCKETS.len()]) -> MetricsSnapshot {
        let samples = counts.iter().sum();
        let percentile = |p: u64| {
            if samples == 0 {
                return None;
            }
            let target = ((samples as u128 * p as u128).div_ceil(100)) as u64;
            let mut n = 0;
            for (i, c) in counts.iter().enumerate() {
                n += c;
                if n >= target {
                    return Some(BUCKETS[i]);
                }
            }
            None
        };
        let pressure = self.rss_pressure();
        MetricsSnapshot {
            observed_at_ms: now_ms(),
            uptime_seconds: self.boot.elapsed().as_secs_f64(),
            commands_total: samples,
            errors_total: self.errors(),
            connections: self.connections.load(Ordering::Relaxed),
            blocked_clients: self.blocked.load(Ordering::Relaxed),
            rss_bytes: pressure.rss_bytes,
            max_rss_bytes_soft: pressure.max_rss_bytes_soft,
            rss_pressured: pressure.rss_pressured,
            latency: Latency {
                samples,
                p50_us: percentile(50),
                p95_us: percentile(95),
                p99_us: percentile(99),
            },
            wal: self.wal.read().unwrap_or_else(|p| p.into_inner()).clone(),
            replication: self
                .replication
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
            server: self
                .server
                .read()
                .unwrap_or_else(|p| p.into_inner())
                .clone(),
        }
    }
    pub fn prometheus(&self) -> String {
        use std::fmt::Write;
        let counts = self.bucket_counts();
        let s = self.snapshot_with(&counts);
        let mut out = String::new();
        for (name, kind, value) in [
            ("commands_total", "counter", s.commands_total as f64),
            ("errors_total", "counter", s.errors_total as f64),
            ("connections", "gauge", s.connections as f64),
            ("blocked_clients", "gauge", s.blocked_clients as f64),
            ("uptime_seconds", "gauge", s.uptime_seconds),
            ("wal_bytes", "gauge", s.wal.bytes as f64),
            ("max_rss_bytes_soft", "gauge", s.max_rss_bytes_soft as f64),
            ("rss_pressured", "gauge", u8::from(s.rss_pressured) as f64),
        ] {
            let _ = writeln!(out, "# TYPE ffkv_{name} {kind}\nffkv_{name} {value}");
        }
        if let Some(rss) = s.rss_bytes {
            let _ = writeln!(
                out,
                "# TYPE ffkv_process_resident_memory_bytes gauge\nffkv_process_resident_memory_bytes {rss}"
            );
        }
        let _ = writeln!(out, "# TYPE ffkv_command_duration_seconds histogram");
        let mut total = 0;
        for (b, c) in BUCKETS.iter().zip(counts) {
            total += c;
            let label = if *b == u64::MAX {
                "+Inf".into()
            } else {
                format!("{}", *b as f64 / 1e6)
            };
            let _ = writeln!(
                out,
                "ffkv_command_duration_seconds_bucket{{le=\"{label}\"}} {total}"
            );
        }
        let _ = writeln!(
            out,
            "ffkv_command_duration_seconds_count {total}\nffkv_command_duration_seconds_sum {}",
            self.latency_sum() as f64 / 1e6
        );
        out
    }
}
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rss_admission_tracks_sample_boundaries_without_treating_unknown_as_a_measurement() {
        let metrics = Metrics::new();
        metrics.set_max_rss_bytes_soft(100);
        assert_eq!(metrics.rss_pressure().rss_bytes, None);
        assert!(!metrics.rss_pressure().rss_pressured);
        for (rss, pressured) in [(99, false), (100, true), (101, true), (50, false)] {
            metrics.rss.sample.store(rss, Ordering::Relaxed);
            let sample = metrics.rss_pressure();
            assert_eq!(sample.rss_bytes, Some(rss));
            assert_eq!(sample.rss_pressured, pressured);
        }
        metrics.set_max_rss_bytes_soft(0);
        metrics.rss.sample.store(1_000, Ordering::Relaxed);
        assert!(!metrics.rss_pressure().rss_pressured);
    }

    #[test]
    fn pressure_blocks_growth_but_keeps_reclaimers_and_replica_precedence() {
        use crate::{engine::BoundCommand, pressure::Admission, protocol::Resp};
        use bytes::Bytes;

        let metrics = Metrics::new();
        metrics.set_max_rss_bytes_soft(1);
        metrics.rss.sample.store(100, Ordering::Relaxed);
        let policy = Admission {
            read_only: false,
            metrics: &metrics,
        };
        let set = [Bytes::from_static(b"SET")];
        let set = BoundCommand::bind(&set);
        assert!(
            matches!(policy.reject(&set), Some(Resp::Error(error)) if error.starts_with("OOM "))
        );
        for name in [b"GET".as_slice(), b"DEL", b"PING"] {
            let args = [Bytes::copy_from_slice(name)];
            assert!(policy.reject(&BoundCommand::bind(&args)).is_none());
        }
        let replica = Admission {
            read_only: true,
            metrics: &metrics,
        };
        assert!(
            matches!(replica.reject(&set), Some(Resp::Error(error)) if error.starts_with("READONLY "))
        );
        assert!(matches!(
            replica.reject(&BoundCommand::bind(&[Bytes::from_static(b"EVAL")])),
            Some(Resp::Error(error)) if error.starts_with("READONLY ")
        ));
    }

    #[test]
    fn command_totals_and_quantiles_follow_recorded_latencies_and_reset_to_empty() {
        let metrics = Metrics::new();
        metrics.connection_open();
        metrics.set_max_rss_bytes_soft(10);
        metrics.rss.sample.store(20, Ordering::Relaxed);
        let micros = Duration::from_micros;
        // (latency, completed commands, of which errors)
        for (latency, count, errors) in [
            (micros(2), 90, 1),
            (micros(64), 5, 0),
            (micros(4_096), 4, 1),
            (Duration::from_secs(2), 1, 1),
        ] {
            for n in 0..count {
                metrics.command_complete(latency, n < errors);
            }
        }
        let recorded = metrics.snapshot();
        assert_eq!(
            (
                recorded.commands_total,
                recorded.errors_total,
                recorded.latency.samples
            ),
            (100, 3, 100)
        );
        assert_eq!(
            (
                recorded.latency.p50_us,
                recorded.latency.p95_us,
                recorded.latency.p99_us
            ),
            (Some(2), Some(64), Some(4_096))
        );

        metrics.reset_statistics();
        let reset = metrics.snapshot();
        assert_eq!(
            (
                reset.commands_total,
                reset.errors_total,
                reset.latency.samples
            ),
            (0, 0, 0)
        );
        assert_eq!(
            (
                reset.latency.p50_us,
                reset.latency.p95_us,
                reset.latency.p99_us
            ),
            (None, None, None)
        );
        assert_eq!(reset.connections, 1, "reset clears statistics, not gauges");
        assert!(reset.rss_pressured, "reset keeps the RSS sample");

        metrics.command_complete(micros(100), false);
        let after = metrics.snapshot();
        assert_eq!((after.commands_total, after.errors_total), (1, 0));
        assert_eq!(
            (after.latency.p50_us, after.latency.p99_us),
            (Some(128), Some(128))
        );
    }

    #[test]
    fn batched_completions_publish_exact_buckets_sum_and_errors_like_single_ones() {
        let micros = Duration::from_micros;
        // (latency, error): bucket edges and both sides of the last finite bound.
        let completed = [
            (micros(0), false),
            (micros(1), true),
            (micros(2), false),
            (micros(3), false),
            (micros(1_048_576), true),
            (micros(1_048_577), false),
        ];
        let single = Metrics::new();
        for (latency, error) in completed {
            single.command_complete(latency, error);
        }
        // Two flushes, the second a mixed batch, accumulate into the same counters.
        let batched = Metrics::new();
        for flush in [&completed[..1], &completed[1..]] {
            let mut latencies = CommandLatencies::default();
            for &(latency, error) in flush {
                latencies.record(latency, error);
            }
            batched.commands_complete(&latencies);
        }
        let histogram = |metrics: &Metrics| -> Vec<String> {
            metrics
                .prometheus()
                .lines()
                .filter(|line| {
                    line.starts_with("ffkv_command_duration_seconds")
                        || line.starts_with("ffkv_commands_total")
                        || line.starts_with("ffkv_errors_total")
                })
                .map(str::to_owned)
                .collect()
        };
        let published = histogram(&batched);
        assert_eq!(published, histogram(&single));
        for line in [
            "ffkv_commands_total 6",
            "ffkv_errors_total 2",
            "ffkv_command_duration_seconds_bucket{le=\"0.000001\"} 2",
            "ffkv_command_duration_seconds_bucket{le=\"0.000002\"} 3",
            "ffkv_command_duration_seconds_bucket{le=\"0.000004\"} 4",
            "ffkv_command_duration_seconds_bucket{le=\"1.048576\"} 5",
            "ffkv_command_duration_seconds_bucket{le=\"+Inf\"} 6",
            "ffkv_command_duration_seconds_count 6",
            "ffkv_command_duration_seconds_sum 2.097159",
        ] {
            assert!(
                published.iter().any(|published| published == line),
                "{line} in {published:#?}"
            );
        }
        let snapshot = batched.snapshot();
        assert_eq!(
            (
                snapshot.commands_total,
                snapshot.errors_total,
                snapshot.latency.p50_us,
                snapshot.latency.p99_us
            ),
            (6, 2, Some(2), Some(u64::MAX))
        );
    }

    #[test]
    fn completions_on_many_threads_sum_exactly_and_reset_clears_every_thread() {
        let micros = Duration::from_micros;
        let metrics = Metrics::new();
        // Each thread publishes a flush of 100 commands, 10 failed, then one
        // more failed command alone.
        let complete = |threads: u64| {
            std::thread::scope(|scope| {
                for _ in 0..threads {
                    scope.spawn(|| {
                        let mut latencies = CommandLatencies::default();
                        for n in 0..100 {
                            latencies.record(micros(n % 8), n % 10 == 0);
                        }
                        metrics.commands_complete(&latencies);
                        metrics.command_complete(micros(4_096), true);
                    });
                }
            });
        };
        let totals = |metrics: &Metrics| {
            let snapshot = metrics.snapshot();
            (snapshot.commands_total, snapshot.errors_total)
        };
        let sum = |metrics: &Metrics| {
            let prefix = "ffkv_command_duration_seconds_sum ";
            let text = metrics.prometheus();
            text.lines()
                .find(|line| line.starts_with(prefix))
                .map(str::to_owned)
        };
        complete(16);
        assert_eq!(totals(&metrics), (16 * 101, 16 * 11));
        // 0..100 modulo 8 sums to 342 microseconds, then 4096 more.
        let expected = format!(
            "ffkv_command_duration_seconds_sum {}",
            (16 * (342 + 4_096)) as f64 / 1e6
        );
        assert_eq!(sum(&metrics), Some(expected));

        metrics.reset_statistics();
        assert_eq!(totals(&metrics), (0, 0));
        assert_eq!(metrics.snapshot().latency.p50_us, None);
        assert_eq!(
            sum(&metrics),
            Some("ffkv_command_duration_seconds_sum 0".to_owned())
        );
        complete(3);
        assert_eq!(totals(&metrics), (3 * 101, 3 * 11));
    }
}
