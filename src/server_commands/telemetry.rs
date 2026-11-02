use super::{
    ConfigSnapshot, arity, array, bulk, integer, integer_error, syntax, unix_ms, unsigned,
};
use crate::protocol::Resp;
use bytes::Bytes;
use rand::Rng;
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    time::{Duration, Instant},
};

pub struct Observation<'a> {
    pub args: &'a [Bytes],
    pub keys: &'a [Bytes],
    pub database: u32,
    pub client_address: &'a str,
    pub client_name: &'a str,
    pub elapsed: Duration,
    pub cpu_time: Duration,
    pub input_bytes: u64,
    pub output_bytes: u64,
    pub failed: bool,
}
/// Read on the executing thread before and after execution, not across an await
/// that can move a task to another thread. Wall-clock latency is separate.
pub fn thread_cpu_time() -> Duration {
    #[cfg(unix)]
    {
        let mut ts = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        if unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) } == 0 {
            return Duration::new(
                ts.tv_sec.max(0) as u64,
                ts.tv_nsec.clamp(0, 999999999) as u32,
            );
        }
    }
    Duration::ZERO
}
pub(super) fn runtime_parameter(name: &str) -> bool {
    matches!(
        name,
        "latency-monitor-threshold"
            | "latency-tracking"
            | "slowlog-log-slower-than"
            | "slowlog-max-len"
            | "shutdown-timeout"
    )
}
fn config_integer(config: &ConfigSnapshot, name: &str, default: i64) -> i64 {
    config
        .parameters
        .get(name)
        .and_then(|p| integer(&p.value))
        .unwrap_or(default)
}
struct LatencyEvent {
    samples: VecDeque<(u64, u64)>,
    maximum: u64,
}
struct Histogram {
    calls: u64,
    buckets: [u64; 31],
}
struct SlowEntry {
    id: u64,
    timestamp: u64,
    microseconds: u64,
    args: Vec<Bytes>,
    address: Bytes,
    name: Bytes,
}
#[derive(Default)]
struct HotValue {
    cpu: u64,
    net: u64,
}
struct Hotkeys {
    active: bool,
    cpu: bool,
    net: bool,
    count: usize,
    ratio: u64,
    started: Instant,
    started_ms: u64,
    stopped_ms: Option<u64>,
    duration: Option<Duration>,
    slots: HashSet<u16>,
    values: HashMap<Bytes, HotValue>,
    cpu_us: u64,
    net_bytes: u64,
    start_user_ms: u64,
    start_sys_ms: u64,
    selected_cpu_us: u64,
    selected_net_bytes: u64,
    sampled_cpu_us: u64,
    sampled_net_bytes: u64,
    end_process_cpu: Option<(u64, u64)>,
}
impl Hotkeys {
    fn expire(&mut self) {
        if self.active
            && self
                .duration
                .is_some_and(|duration| self.started.elapsed() >= duration)
        {
            self.stop();
            self.stopped_ms = self.duration.map(|duration| {
                self.started_ms
                    .saturating_add(duration.as_millis().min(u64::MAX as u128) as u64)
            });
        }
    }
    fn stop(&mut self) {
        self.active = false;
        self.stopped_ms = Some(unix_ms());
        self.end_process_cpu = Some(process_cpu_ms());
    }
    fn observe(&mut self, observation: &Observation<'_>) {
        self.expire();
        if !self.active {
            return;
        }
        let cpu = observation.cpu_time.as_micros().min(u64::MAX as u128) as u64;
        let net = observation
            .input_bytes
            .saturating_add(observation.output_bytes);
        self.cpu_us = self.cpu_us.saturating_add(cpu);
        self.net_bytes = self.net_bytes.saturating_add(net);
        if observation.keys.is_empty() {
            return;
        }
        let selected = observation
            .keys
            .iter()
            .enumerate()
            .filter(|(index, key)| {
                !observation.keys[..*index].contains(key)
                    && (self.slots.is_empty() || self.slots.contains(&slot(key)))
            })
            .count() as u64;
        let selected_cpu = cpu.saturating_mul(selected) / observation.keys.len() as u64;
        let selected_net = net.saturating_mul(selected) / observation.keys.len() as u64;
        self.selected_cpu_us = self.selected_cpu_us.saturating_add(selected_cpu);
        self.selected_net_bytes = self.selected_net_bytes.saturating_add(selected_net);
        if rand::rng().random_range(0..self.ratio) != 0 {
            return;
        }
        self.sampled_cpu_us = self.sampled_cpu_us.saturating_add(selected_cpu);
        self.sampled_net_bytes = self.sampled_net_bytes.saturating_add(selected_net);
        for (index, key) in observation.keys.iter().enumerate() {
            if observation.keys[..index].contains(key)
                || !self.slots.is_empty() && !self.slots.contains(&slot(key))
            {
                continue;
            }
            let per_cpu = cpu / observation.keys.len() as u64;
            let per_net = net / observation.keys.len() as u64;
            // Bounded sampled weighted counters retain candidates under a stream
            // of unique keys; values are estimates, not an exact global ranking.
            if !self.values.contains_key(key)
                && self.values.len() >= self.count.saturating_mul(16).max(64)
            {
                let victim = self
                    .values
                    .iter()
                    .min_by_key(|(_, value)| if self.cpu { value.cpu } else { value.net })
                    .map(|(key, _)| key.clone());
                if let Some(victim) = victim {
                    self.values.remove(&victim);
                }
            }
            if let Some(value) = self.values.get_mut(key) {
                value.cpu = value.cpu.saturating_add(per_cpu.saturating_mul(self.ratio));
                value.net = value.net.saturating_add(per_net.saturating_mul(self.ratio));
            } else {
                self.values.insert(
                    Bytes::copy_from_slice(key),
                    HotValue {
                        cpu: per_cpu.saturating_mul(self.ratio),
                        net: per_net.saturating_mul(self.ratio),
                    },
                );
            }
        }
    }
    fn reply(&mut self) -> Resp {
        self.expire();
        let mut fields = vec![
            (
                bulk("tracking-active"),
                Resp::Integer(i64::from(self.active)),
            ),
            (bulk("sample-ratio"), number(self.ratio)),
            (
                bulk("selected-slots"),
                if self.slots.is_empty() {
                    array(vec![array(vec![Resp::Integer(0), Resp::Integer(16383)])])
                } else {
                    let mut slots: Vec<_> = self.slots.iter().copied().collect();
                    slots.sort_unstable();
                    array(slots.into_iter().map(|n| Resp::Integer(n.into())).collect())
                },
            ),
            (bulk("all-commands-all-slots-us"), number(self.cpu_us)),
            (
                bulk("net-bytes-all-commands-all-slots"),
                number(self.net_bytes),
            ),
            (
                bulk("collection-start-time-unix-ms"),
                number(self.started_ms),
            ),
            (
                bulk("collection-duration-ms"),
                number(
                    self.stopped_ms
                        .unwrap_or_else(unix_ms)
                        .saturating_sub(self.started_ms),
                ),
            ),
        ];
        if self.cpu {
            let (user, sys) = self.end_process_cpu.unwrap_or_else(process_cpu_ms);
            fields.push((
                bulk("total-cpu-time-user-ms"),
                number(user.saturating_sub(self.start_user_ms)),
            ));
            fields.push((
                bulk("total-cpu-time-sys-ms"),
                number(sys.saturating_sub(self.start_sys_ms)),
            ));
            fields.push((bulk("by-cpu-time-us"), self.top(true)));
        }
        if self.net {
            fields.push((bulk("total-net-bytes"), number(self.net_bytes)));
            fields.push((bulk("by-net-bytes"), self.top(false)));
        }
        if !self.slots.is_empty() {
            fields.push((
                bulk("all-commands-selected-slots-us"),
                number(self.selected_cpu_us),
            ));
            fields.push((
                bulk("net-bytes-all-commands-selected-slots"),
                number(self.selected_net_bytes),
            ));
            if self.ratio != 1 {
                fields.push((
                    bulk("sampled-commands-selected-slots-us"),
                    number(self.sampled_cpu_us),
                ));
                fields.push((
                    bulk("net-bytes-sampled-commands-selected-slots"),
                    number(self.sampled_net_bytes),
                ));
            }
        }
        array(vec![Resp::Map(fields)])
    }
    fn top(&self, cpu: bool) -> Resp {
        let mut values: Vec<_> = self.values.iter().collect();
        values.sort_unstable_by(|(ka, a), (kb, b)| {
            let a = if cpu { a.cpu } else { a.net };
            let b = if cpu { b.cpu } else { b.net };
            b.cmp(&a).then_with(|| ka.cmp(kb))
        });
        let mut reply = Vec::with_capacity(self.count.min(values.len()) * 2);
        for (key, value) in values.into_iter().take(self.count) {
            reply.push(bulk(key.clone()));
            reply.push(number(if cpu { value.cpu } else { value.net }));
        }
        array(reply)
    }
}
fn process_cpu_ms() -> (u64, u64) {
    #[cfg(unix)]
    {
        let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
        if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } == 0 {
            let usage = unsafe { usage.assume_init() };
            let ms = |tv: libc::timeval| {
                (tv.tv_sec.max(0) as u64)
                    .saturating_mul(1000)
                    .saturating_add(tv.tv_usec.max(0) as u64 / 1000)
            };
            return (ms(usage.ru_utime), ms(usage.ru_stime));
        }
    }
    (0, 0)
}
pub(super) struct Telemetry {
    pub commands: u64,
    pub errors: u64,
    pub input_bytes: u64,
    pub output_bytes: u64,
    events: BTreeMap<String, LatencyEvent>,
    histograms: BTreeMap<String, Histogram>,
    slow: VecDeque<SlowEntry>,
    next_slow_id: u64,
    hotkeys: Option<Hotkeys>,
}
impl Telemetry {
    pub fn new() -> Self {
        Self {
            commands: 0,
            errors: 0,
            input_bytes: 0,
            output_bytes: 0,
            events: BTreeMap::new(),
            histograms: BTreeMap::new(),
            slow: VecDeque::new(),
            next_slow_id: 0,
            hotkeys: None,
        }
    }
    pub fn reset_statistics(&mut self) {
        self.commands = 0;
        self.errors = 0;
        self.input_bytes = 0;
        self.output_bytes = 0;
        self.histograms.clear();
    }
    pub fn latency(&mut self, event: &str, elapsed: Duration, threshold_ms: i64) {
        if threshold_ms <= 0 || elapsed.as_millis() < threshold_ms as u128 {
            return;
        }
        if !self.events.contains_key(event) && self.events.len() >= 128 {
            return;
        }
        let milliseconds = elapsed.as_millis().min(u64::MAX as u128) as u64;
        let second = unix_ms() / 1000;
        let event = self
            .events
            .entry(event.to_owned())
            .or_insert_with(|| LatencyEvent {
                samples: VecDeque::new(),
                maximum: 0,
            });
        event.maximum = event.maximum.max(milliseconds);
        if let Some((timestamp, value)) = event.samples.back_mut()
            && *timestamp == second
        {
            *value = (*value).max(milliseconds);
            return;
        }
        if event.samples.len() == 160 {
            event.samples.pop_front();
        }
        event.samples.push_back((second, milliseconds));
    }
    pub fn observe(&mut self, observation: Observation<'_>, config: &ConfigSnapshot) {
        self.commands = self.commands.saturating_add(1);
        self.errors = self.errors.saturating_add(u64::from(observation.failed));
        self.input_bytes = self.input_bytes.saturating_add(observation.input_bytes);
        self.output_bytes = self.output_bytes.saturating_add(observation.output_bytes);
        self.latency(
            "command",
            observation.elapsed,
            config_integer(config, "latency-monitor-threshold", 0),
        );
        if config
            .parameters
            .get("latency-tracking")
            .is_none_or(|p| p.value.eq_ignore_ascii_case(b"yes"))
            && let Some(name) = observation.args.first()
        {
            let mut command = String::from_utf8_lossy(name).to_ascii_lowercase();
            if matches!(
                command.as_str(),
                "config"
                    | "acl"
                    | "client"
                    | "latency"
                    | "slowlog"
                    | "memory"
                    | "hotkeys"
                    | "backup"
                    | "module"
                    | "command"
                    | "function"
                    | "script"
            ) && let Some(sub) = observation.args.get(1)
            {
                command.push('|');
                command.push_str(&String::from_utf8_lossy(sub).to_ascii_lowercase());
            }
            if self.histograms.contains_key(&command) || self.histograms.len() < 2048 {
                let histogram = self.histograms.entry(command).or_insert(Histogram {
                    calls: 0,
                    buckets: [0; 31],
                });
                histogram.calls = histogram.calls.saturating_add(1);
                let nanos = observation.elapsed.as_nanos().min(1_000_000_001) as u64;
                let bucket = if nanos <= 1 {
                    0
                } else {
                    (64 - (nanos - 1).leading_zeros() as usize).min(30)
                };
                histogram.buckets[bucket] = histogram.buckets[bucket].saturating_add(1);
            }
        }
        let max_len = config_integer(config, "slowlog-max-len", 128).clamp(0, 100000) as usize;
        let threshold = config_integer(config, "slowlog-log-slower-than", 10000);
        let micros = observation.elapsed.as_micros().min(u64::MAX as u128) as u64;
        if threshold >= 0 && micros >= threshold as u64 && max_len != 0 {
            let args = redacted(observation.args)
                .into_iter()
                .take(32)
                .map(|arg| {
                    if arg.len() <= 128 {
                        arg
                    } else {
                        let mut shortened = arg[..128].to_vec();
                        shortened.extend_from_slice(
                            format!("... ({} more bytes)", arg.len() - 128).as_bytes(),
                        );
                        Bytes::from(shortened)
                    }
                })
                .collect();
            self.slow.push_front(SlowEntry {
                id: self.next_slow_id,
                timestamp: unix_ms() / 1000,
                microseconds: micros,
                args,
                address: Bytes::copy_from_slice(observation.client_address.as_bytes()),
                name: Bytes::copy_from_slice(observation.client_name.as_bytes()),
            });
            self.next_slow_id = self.next_slow_id.saturating_add(1);
        }
        while self.slow.len() > max_len {
            self.slow.pop_back();
        }
        if let Some(hotkeys) = self.hotkeys.as_mut() {
            hotkeys.observe(&observation);
        }
    }
    pub fn latency_command(&mut self, args: &[Bytes]) -> Resp {
        let Some(sub) = args.get(1) else {
            return arity("latency");
        };
        if sub.eq_ignore_ascii_case(b"HISTORY") {
            if args.len() != 3 {
                return arity("latency|history");
            }
            return array(
                self.events
                    .get(String::from_utf8_lossy(&args[2]).as_ref())
                    .map(|event| {
                        event
                            .samples
                            .iter()
                            .map(|(time, value)| array(vec![number(*time), number(*value)]))
                            .collect()
                    })
                    .unwrap_or_default(),
            );
        }
        if sub.eq_ignore_ascii_case(b"LATEST") {
            if args.len() != 2 {
                return arity("latency|latest");
            }
            return array(
                self.events
                    .iter()
                    .filter_map(|(name, event)| {
                        event.samples.back().map(|(time, value)| {
                            array(vec![
                                bulk(name.clone()),
                                number(*time),
                                number(*value),
                                number(event.maximum),
                            ])
                        })
                    })
                    .collect(),
            );
        }
        if sub.eq_ignore_ascii_case(b"RESET") {
            let count = if args.len() == 2 {
                let count = self.events.len();
                self.events.clear();
                count
            } else {
                args[2..]
                    .iter()
                    .filter(|name| {
                        self.events
                            .remove(String::from_utf8_lossy(name).as_ref())
                            .is_some()
                    })
                    .count()
            };
            return Resp::Integer(count as i64);
        }
        if sub.eq_ignore_ascii_case(b"HISTOGRAM") {
            let wanted: HashSet<_> = args[2..]
                .iter()
                .map(|name| String::from_utf8_lossy(name).to_ascii_lowercase())
                .collect();
            return Resp::Map(
                self.histograms
                    .iter()
                    .filter(|(name, _)| wanted.is_empty() || wanted.contains(*name))
                    .map(|(name, histogram)| {
                        let mut total = 0;
                        let buckets =
                            histogram
                                .buckets
                                .iter()
                                .enumerate()
                                .filter_map(|(index, count)| {
                                    if *count == 0 {
                                        return None;
                                    }
                                    total += count;
                                    let upper_us =
                                        ((1u64 << index).saturating_add(999) / 1000).max(1);
                                    Some((upper_us, total))
                                });
                        // Multiple sub-microsecond buckets share the 1us boundary.
                        let mut merged = BTreeMap::new();
                        for (upper, count) in buckets {
                            merged.insert(upper, count);
                        }
                        (
                            bulk(name.clone()),
                            Resp::Map(vec![
                                (bulk("calls"), number(histogram.calls)),
                                (
                                    bulk("histogram_usec"),
                                    Resp::Map(
                                        merged
                                            .into_iter()
                                            .map(|(upper, count)| (number(upper), number(count)))
                                            .collect(),
                                    ),
                                ),
                            ]),
                        )
                    })
                    .collect(),
            );
        }
        if sub.eq_ignore_ascii_case(b"DOCTOR") {
            if args.len() != 2 {
                return arity("latency|doctor");
            }
            if self.events.is_empty() {
                return bulk(
                    "No latency spike was observed. Enable latency-monitor-threshold to record events.\n",
                );
            }
            let mut report = String::from("Latency analysis of observed events:\n");
            for (name, event) in &self.events {
                let sum: u128 = event.samples.iter().map(|(_, n)| *n as u128).sum();
                report.push_str(&format!(
                    "{name}: {} samples, average {} ms, worst {} ms.\n",
                    event.samples.len(),
                    sum / event.samples.len().max(1) as u128,
                    event.maximum
                ));
            }
            report.push_str("Compare command and aof-fsync events to distinguish execution work from persistence stalls.\n");
            return bulk(report);
        }
        if sub.eq_ignore_ascii_case(b"GRAPH") {
            if args.len() != 3 {
                return arity("latency|graph");
            }
            let name = String::from_utf8_lossy(&args[2]);
            let Some(event) = self.events.get(name.as_ref()) else {
                return Resp::Error(format!("ERR No samples available for event '{name}'"));
            };
            let max = event
                .samples
                .iter()
                .map(|(_, n)| *n)
                .max()
                .unwrap_or(1)
                .max(1);
            let min = event.samples.iter().map(|(_, n)| *n).min().unwrap_or(0);
            let mut graph = format!("{name} - high {max} ms, low {min} ms\n");
            for level in (1..=8).rev() {
                graph.push_str(&format!("{:>8} |", max.saturating_mul(level) / 8));
                for (_, value) in &event.samples {
                    graph.push(if value.saturating_mul(8) >= max.saturating_mul(level) {
                        '#'
                    } else {
                        ' '
                    });
                }
                graph.push('\n');
            }
            graph.push_str("         +");
            graph.push_str(&"-".repeat(event.samples.len()));
            graph.push('\n');
            return bulk(graph);
        }
        Resp::Error("ERR unknown LATENCY subcommand".into())
    }
    pub fn slowlog_command(&mut self, args: &[Bytes]) -> Resp {
        let Some(sub) = args.get(1) else {
            return arity("slowlog");
        };
        if sub.eq_ignore_ascii_case(b"LEN") {
            return if args.len() == 2 {
                Resp::Integer(self.slow.len() as i64)
            } else {
                arity("slowlog|len")
            };
        }
        if sub.eq_ignore_ascii_case(b"RESET") {
            if args.len() != 2 {
                return arity("slowlog|reset");
            }
            self.slow.clear();
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"GET") {
            if args.len() > 3 {
                return arity("slowlog|get");
            }
            let count = if let Some(count) = args.get(2) {
                match integer(count) {
                    Some(-1) => usize::MAX,
                    Some(n) if n >= 0 => n as usize,
                    _ => return integer_error(),
                }
            } else {
                10
            };
            return array(
                self.slow
                    .iter()
                    .take(count)
                    .map(|entry| {
                        array(vec![
                            number(entry.id),
                            number(entry.timestamp),
                            number(entry.microseconds),
                            array(entry.args.iter().cloned().map(bulk).collect()),
                            bulk(entry.address.clone()),
                            bulk(entry.name.clone()),
                        ])
                    })
                    .collect(),
            );
        }
        Resp::Error("ERR unknown SLOWLOG subcommand".into())
    }
    pub fn hotkeys_command(&mut self, args: &[Bytes], cluster: bool) -> Resp {
        let Some(sub) = args.get(1) else {
            return Resp::Error("ERR HOTKEYS requires GET, START, STOP or RESET".into());
        };
        if sub.eq_ignore_ascii_case(b"GET") {
            if args.len() != 2 {
                return arity("hotkeys|get");
            }
            return self
                .hotkeys
                .as_mut()
                .map(Hotkeys::reply)
                .unwrap_or(Resp::Bulk(None));
        }
        if sub.eq_ignore_ascii_case(b"STOP") {
            if args.len() != 2 {
                return arity("hotkeys|stop");
            }
            let Some(hotkeys) = self.hotkeys.as_mut() else {
                return Resp::Error("ERR No hotkeys tracking session".into());
            };
            hotkeys.expire();
            if !hotkeys.active {
                return Resp::Error("ERR Hotkeys tracking is not active".into());
            }
            hotkeys.stop();
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"RESET") {
            if args.len() != 2 {
                return arity("hotkeys|reset");
            }
            self.hotkeys = None;
            return Resp::Simple("OK");
        }
        if !sub.eq_ignore_ascii_case(b"START") {
            return Resp::Error("ERR unknown HOTKEYS subcommand".into());
        }
        if let Some(hotkeys) = self.hotkeys.as_mut() {
            hotkeys.expire();
            if hotkeys.active {
                return Resp::Error("ERR Hotkeys tracking is already active".into());
            }
        }
        if args.len() < 5 || !args[2].eq_ignore_ascii_case(b"METRICS") {
            return syntax();
        }
        let Some(metric_count) = unsigned(&args[3]).filter(|n| *n == 1 || *n == 2) else {
            return syntax();
        };
        let mut at = 4;
        let (mut cpu, mut net) = (false, false);
        for _ in 0..metric_count {
            let Some(metric) = args.get(at) else {
                return syntax();
            };
            if metric.eq_ignore_ascii_case(b"CPU") && !cpu {
                cpu = true;
            } else if metric.eq_ignore_ascii_case(b"NET") && !net {
                net = true;
            } else {
                return syntax();
            }
            at += 1;
        }
        let (mut count, mut ratio, mut duration) = (10usize, 1u64, None);
        let mut slots = HashSet::new();
        let mut options = HashSet::new();
        while at < args.len() {
            let option = args[at].to_ascii_uppercase();
            if !options.insert(option.clone()) {
                return syntax();
            }
            if option == b"SLOTS" {
                if !cluster {
                    return Resp::Error("ERR SLOTS is only supported in cluster mode".into());
                }
                let Some(count) = args
                    .get(at + 1)
                    .and_then(|v| unsigned(v))
                    .filter(|n| *n > 0 && *n <= 16384)
                else {
                    return integer_error();
                };
                let end = at + 2 + count as usize;
                let Some(values) = args.get(at + 2..end) else {
                    return syntax();
                };
                for value in values {
                    let Some(slot) = unsigned(value).filter(|n| *n < 16384) else {
                        return Resp::Error("ERR Invalid hash slot".into());
                    };
                    slots.insert(slot as u16);
                }
                at = end;
                continue;
            }
            let Some(value) = args.get(at + 1).and_then(|v| unsigned(v)) else {
                return integer_error();
            };
            match option.as_slice() {
                b"COUNT" if value > 0 && value <= 10000 => count = value as usize,
                b"SAMPLE" if value > 0 => ratio = value,
                b"DURATION" if value > 0 => duration = Some(Duration::from_secs(value)),
                _ => return syntax(),
            }
            at += 2;
        }
        let (user, sys) = process_cpu_ms();
        self.hotkeys = Some(Hotkeys {
            active: true,
            cpu,
            net,
            count,
            ratio,
            started: Instant::now(),
            started_ms: unix_ms(),
            stopped_ms: None,
            duration,
            slots,
            values: HashMap::new(),
            cpu_us: 0,
            net_bytes: 0,
            start_user_ms: user,
            start_sys_ms: sys,
            end_process_cpu: None,
            selected_cpu_us: 0,
            selected_net_bytes: 0,
            sampled_cpu_us: 0,
            sampled_net_bytes: 0,
        });
        Resp::Simple("OK")
    }
}
fn number(value: u64) -> Resp {
    if let Ok(value) = i64::try_from(value) {
        Resp::Integer(value)
    } else {
        Resp::Unsigned(value)
    }
}
fn slot(key: &[u8]) -> u16 {
    let tag = key
        .iter()
        .position(|b| *b == b'{')
        .and_then(|open| {
            key[open + 1..]
                .iter()
                .position(|b| *b == b'}')
                .filter(|n| *n != 0)
                .map(|len| &key[open + 1..open + 1 + len])
        })
        .unwrap_or(key);
    let mut crc = 0u16;
    for byte in tag {
        crc ^= (*byte as u16) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                crc << 1 ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc & 16383
}
/// Secrets never enter MONITOR/slowlog storage, including failed attempts.
fn redacted(args: &[Bytes]) -> Vec<Bytes> {
    let mut hide = false;
    let mut redact_next = false;
    args.iter()
        .enumerate()
        .map(|(index, arg)| {
            let secret = if index == 0 {
                false
            } else if args[0].eq_ignore_ascii_case(b"AUTH") {
                true
            } else if args[0].eq_ignore_ascii_case(b"ACL")
                && args
                    .get(1)
                    .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SETUSER"))
            {
                index >= 3
            } else if args[0].eq_ignore_ascii_case(b"CONFIG")
                && args
                    .get(1)
                    .is_some_and(|sub| sub.eq_ignore_ascii_case(b"SET"))
            {
                index >= 3 && index % 2 == 1
            } else if args[0].eq_ignore_ascii_case(b"HELLO") {
                if arg.eq_ignore_ascii_case(b"AUTH") {
                    hide = true;
                    false
                } else if hide {
                    if index >= 2 && args[index - 2].eq_ignore_ascii_case(b"AUTH") {
                        hide = false;
                    }
                    true
                } else {
                    false
                }
            } else if args[0].eq_ignore_ascii_case(b"MIGRATE") {
                let secret = redact_next;
                redact_next = arg.eq_ignore_ascii_case(b"AUTH")
                    || arg.eq_ignore_ascii_case(b"AUTH2")
                    || index > 0 && args[index - 1].eq_ignore_ascii_case(b"AUTH2");
                secret
            } else {
                false
            };
            if secret {
                Bytes::from_static(b"(redacted)")
            } else {
                Bytes::copy_from_slice(arg)
            }
        })
        .collect()
}
pub(super) fn monitor_line(observation: &Observation<'_>) -> Bytes {
    let now = unix_ms();
    let mut line = format!(
        "{}.{:06} [{} {}]",
        now / 1000,
        now % 1000 * 1000,
        observation.database,
        observation.client_address
    )
    .into_bytes();
    for arg in redacted(observation.args) {
        line.extend_from_slice(b" \"");
        for byte in arg {
            match byte {
                b'\\' => line.extend_from_slice(b"\\\\"),
                b'\"' => line.extend_from_slice(b"\\\""),
                b'\n' => line.extend_from_slice(b"\\n"),
                b'\r' => line.extend_from_slice(b"\\r"),
                b'\t' => line.extend_from_slice(b"\\t"),
                32..=126 => line.push(byte),
                _ => line.extend_from_slice(format!("\\x{byte:02x}").as_bytes()),
            }
        }
        line.push(b'\"');
    }
    Bytes::from(line)
}
