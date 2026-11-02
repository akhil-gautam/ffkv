//! Bounded, since-boot operational measurements. Latencies are bucket upper bounds.
use serde::Serialize;
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
pub struct Metrics {
    boot: Instant,
    commands: AtomicU64,
    errors: AtomicU64,
    connections: AtomicU64,
    blocked: AtomicU64,
    rss: AtomicU64,
    max_rss_bytes_soft: AtomicU64,
    buckets: [AtomicU64; 20],
    latency_sum: AtomicU64,
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
        Self {
            boot: Instant::now(),
            commands: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            connections: AtomicU64::new(0),
            blocked: AtomicU64::new(0),
            rss: AtomicU64::new(NONE),
            max_rss_bytes_soft: AtomicU64::new(0),
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            latency_sum: AtomicU64::new(0),
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
    pub fn reset_statistics(&self) {
        self.commands.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        self.latency_sum.store(0, Ordering::Relaxed);
        for bucket in &self.buckets {
            bucket.store(0, Ordering::Relaxed);
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
    pub fn command_complete(&self, elapsed: Duration, error: bool) {
        let us = elapsed.as_micros().min(u64::MAX as u128) as u64;
        let bucket = BUCKETS
            .partition_point(|bound| *bound < us)
            .min(BUCKETS.len() - 1);
        self.buckets[bucket].fetch_add(1, Ordering::Relaxed);
        self.latency_sum.fetch_add(us, Ordering::Relaxed);
        self.commands.fetch_add(1, Ordering::Relaxed);
        if error {
            self.errors.fetch_add(1, Ordering::Relaxed);
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
        self.max_rss_bytes_soft.store(bytes, Ordering::Relaxed);
    }

    /// Admission uses only the latest sample; it neither measures nor reserves
    /// memory per request. Unknown RSS cannot establish pressure.
    pub fn rss_pressure(&self) -> RssPressure {
        let rss = self.rss.load(Ordering::Relaxed);
        let limit = self.max_rss_bytes_soft.load(Ordering::Relaxed);
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
        self.rss.store(
            system.process(pid).map_or(NONE, |p| p.memory()),
            Ordering::Relaxed,
        );
    }
    pub fn snapshot(&self) -> MetricsSnapshot {
        let counts = self.buckets.each_ref().map(|b| b.load(Ordering::Relaxed));
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
            commands_total: self.commands.load(Ordering::Relaxed),
            errors_total: self.errors.load(Ordering::Relaxed),
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
        let s = self.snapshot();
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
        for (b, c) in BUCKETS.iter().zip(&self.buckets) {
            total += c.load(Ordering::Relaxed);
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
            self.latency_sum.load(Ordering::Relaxed) as f64 / 1e6
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
            metrics.rss.store(rss, Ordering::Relaxed);
            let sample = metrics.rss_pressure();
            assert_eq!(sample.rss_bytes, Some(rss));
            assert_eq!(sample.rss_pressured, pressured);
        }
        metrics.set_max_rss_bytes_soft(0);
        metrics.rss.store(1_000, Ordering::Relaxed);
        assert!(!metrics.rss_pressure().rss_pressured);
    }

    #[test]
    fn pressure_blocks_growth_but_keeps_reclaimers_and_replica_precedence() {
        use crate::{pressure::Admission, protocol::Resp};
        use bytes::Bytes;

        let metrics = Metrics::new();
        metrics.set_max_rss_bytes_soft(1);
        metrics.rss.store(100, Ordering::Relaxed);
        let policy = Admission {
            read_only: false,
            metrics: &metrics,
        };
        let set = [Bytes::from_static(b"SET")];
        assert!(
            matches!(policy.reject(&set), Some(Resp::Error(error)) if error.starts_with("OOM "))
        );
        for name in [b"GET".as_slice(), b"DEL", b"PING"] {
            assert!(policy.reject(&[Bytes::copy_from_slice(name)]).is_none());
        }
        let replica = Admission {
            read_only: true,
            metrics: &metrics,
        };
        assert!(
            matches!(replica.reject(&set), Some(Resp::Error(error)) if error.starts_with("READONLY "))
        );
        assert!(matches!(
            replica.reject(&[Bytes::from_static(b"EVAL")]),
            Some(Resp::Error(error)) if error.starts_with("READONLY ")
        ));
    }
}
