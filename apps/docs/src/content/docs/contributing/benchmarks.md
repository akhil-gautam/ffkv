---
title: Benchmarks and fault/soak measurements
description: Reproduce native Redis comparisons and list-backlog workloads, interpret RSS and latency evidence, and avoid unsupported performance or durability claims.
---

## Native Redis versus ffkv comparison

`benchmarks/redis_compare.py` uses the same compiled `redis-benchmark` executable against the selected engines, rather than timing Python request generation. The default remains ffkv and Redis; Dragonfly is opt-in. Install/provide `redis-server` and `redis-benchmark`, then run from the repository root:

```sh
cargo build --release --bin ffkv
python3 benchmarks/redis_compare.py --repetitions 3 --target-seconds 3 \
  --json redis-comparison.json
```

The default matrix runs 198 measured trials: nine memory-only workloads, three load profiles, two engines and three repetitions, plus 256-byte SET under everysec and strict durability. Profiles are one client/pipeline one/native thread one, 32 clients/pipeline one/four native threads, and 32 clients/pipeline 16/four native threads.

GET and SET use 10,000 populated keys with 256-byte or 4-KiB values; INCR uses 10,000 counters; HGET/HSET use 1,000 hashes with eight 256-byte fields; ZADD/ZRANGE use 1,000 sorted sets with 64 members. Writes vary payload suffixes or the existing member's score instead of benchmarking only identical-value no-ops. ZRANGE returns ten members. These are isolated operation workloads, not a mixed read/write application trace.

Every trial owns a fresh server and temporary directory. All preload replies are checked, sampled values/types/cardinalities are verified before and after measurement, and INCR additionally verifies the exact full-dataset counter delta. Read keys are populated: a missing-key GET is not counted as a payload-read benchmark. A fixed native seed is recorded, although multithreaded scheduling does not guarantee identical key interleaving.

Warmup and iterative calibration are excluded. Calibration chooses a bounded request count to approximate the requested measurement duration; actual duration and short-run flags remain in JSON. Engine order rotates between repetitions; use a repetition count divisible by the number of selected engines to balance positions. The default two-engine order still reverses on alternate repetitions. The report retains every failure, native stdout/stderr, exact commands, effective persistence settings, current RSS and server/client CPU observations. ffkv defaults to four shards and the memory backend; durable cases add its WAL. Automatic ffkv checkpoints and Redis AOF rewrites are disabled for the steady-state comparison. Checkpoint cost, hybrid storage, recovery and power-loss behavior are separate experiments.

**Latency semantics matter:** [Redis 8.2.1's native client](https://github.com/redis/redis/blob/8.2.1/src/redis-benchmark.c) measures send-to-first-socket-read-event time before parsing replies. It repeats that sample across pipeline replies and clamps its histogram at three seconds. These native p50/p95/p99 values are not pipeline completion times or individually measured reply latencies; never divide them by pipeline depth. Threaded throughput completion detection has a 250-ms timer, making short pilots unsuitable for capacity claims. The list harness below measures different latency semantics.

Redis AOF `always` and ffkv `strict` both wait for synchronization before acknowledging the relevant write, but their batching and commit implementations differ. Do not infer equal fsync counts or power-loss guarantees from matching policy names. Treat same-machine loopback results as workload-specific measurements, not production capacity or remote-network results.

Use `--help` for workload/profile/mode filters, request bounds and timeouts. A quick correctness smoke is deliberately not a performance result:

```sh
python3 benchmarks/redis_compare.py --repetitions 1 --target-seconds 0.25 \
  --profiles c32-p16-t4 --max-requests 8192 --json native-smoke.json
```

### Add a matched Dragonfly reference

Supply a real Dragonfly executable and select memory-only mode:

```sh
python3 benchmarks/redis_compare.py \
  --engines ffkv redis dragonfly --modes memory \
  --dragonfly-binary /path/to/dragonfly \
  --ffkv-shards 4 --dragonfly-threads 4 \
  --repetitions 3 --target-seconds 3 --json three-engine-comparison.json
```

The harness verifies the owned process identity, disables Dragonfly snapshot loading/saving and scheduled snapshots, and checks the requested proactor count against `INFO server`. Other Dragonfly tuning defaults remain unchanged. Record the binary version and complete invocation.

On Linux, add `--server-cpus 0-3 --client-cpus 4-7` to place every server and the native load generator on disjoint CPU sets. The CPUs must exist within the harness's allowed affinity. Server affinity and executable identity are verified after startup. Run all engines in the same environment; comparing native macOS ffkv with Dragonfly inside a Linux VM would confound the result.

Dragonfly's snapshots and in-memory replication journal are **not** equivalent to an append-only log synchronized on each write or once per second. Selecting Dragonfly with `strict` or `everysec` records those combinations as **unsupported**, without executing them or counting them as wins. Reports retain these rows. Exit status is `0` for an entirely passing run, `1` for failures, and `3` when unsupported combinations remain without a failure. See [Dragonfly's persistence flags](https://www.dragonflydb.io/docs/managing-dragonfly/flags) and the [versioned shutdown implementation](https://github.com/dragonflydb/dragonfly/blob/v2.0.2/src/server/server_family.cc).

### Latest integrated source: matched comparison

The current default release build (no PGO) completed **162/162 passing trials**
in the nine-workload, three-profile matrix with two repetitions per engine. All
engines used the same ARM64 Linux VM, four server CPUs and four separate client
CPUs: Redis/redis-benchmark **8.2.1**, Dragonfly **2.0.2** with four proactors,
and **64 ffkv logical shards**, memory mode only.

ffkv had higher medians than Dragonfly in **27/27** combinations, and every ffkv
trial was above every Dragonfly trial for the same case. Against Redis it was
higher in **18/27**; the nine single-client cases stayed 0.5–4% below Redis.
The narrowest Dragonfly margin was pipelined INCR, at **1.04×** median and
**1.03×** worst-case.

Memory-only medians with **32 clients, pipeline depth 16 and four client threads**:

| Workload | ffkv commands/s | Redis commands/s | Dragonfly commands/s |
| --- | ---: | ---: | ---: |
| GET, 256 B | 2,941,042 | 2,082,643 | 2,603,375 |
| SET, 256 B | 2,634,198 | 1,736,991 | 2,427,828 |
| GET, 4 KiB | 1,840,583 | 795,787 | 1,155,995 |
| SET, 4 KiB | 1,641,558 | 494,416 | 1,097,966 |
| INCR | 3,076,196 | 2,079,936 | 2,971,252 |
| HGET | 2,826,854 | 1,689,266 | 2,599,985 |
| HSET | 2,628,055 | 1,514,368 | 2,447,099 |
| ZADD, existing member/new score | 2,398,487 | 548,041 | 1,835,926 |
| ZRANGE, ten members | 1,786,067 | 1,094,335 | 1,485,170 |

The source changes behind this result, in order:

- Contended shards hand commands to the shard worker. Previously, frontend runtimes passed the shard lease back and forth.
- Per-client CPU time is read only while HOTKEYS or cluster CPU slot statistics can report it. The other per-command clocks are unchanged.
- The uncontended inline path does no extra work: one per-shard pending counter, reused routing, and no duplicated hashing.
- Cross-core shared counters and per-command locks are now per lane or per connection. Examples are the default ACL user reference, the processed-command total, metrics and persistence revisions. Readers still see exact totals.
- Single-frame replies are written synchronously, so no reply futures are built and copied per command.
- Integer formatting is table-based, and RESP length headers are parsed in a single pass.

A faster keyed hasher (foldhash) for keyspace and hash-field maps was measured
but **not integrated**. Its hash-flooding resistance is weaker than SipHash for
client-controlled keys.

**Cross-generator check:** the same binary completed **252/252 trials** with
redis-benchmark, memtier 2.5.2 and Dragonfly's native client, with two repetitions.
ffkv had higher medians than Dragonfly in **41/42** cases. The remaining loss is
memtier GET 4 KiB at pipeline 16 (**0.89×**). memtier refills one request per
reply, so ffkv's epoll path makes about 0.7 `sendto` and 0.7 `recvfrom` calls per
command. Dragonfly's io_uring loop makes about 0.07 `io_uring_enter` calls per
command. A zero-copy large-reply prototype gained about 2.5% in that case. It was
not integrated because it did not close the gap.

**io_uring prototype (rejected):** a per-frontend io_uring transport was then
built and measured. It used batched submission, a ring reaper registered with
Tokio and copying socket buffers. It cut system calls on the memtier case by
about 68%, yet throughput fell 4–10% there and 20–28% in redis-benchmark
GET/INCR screens at P1 and P16. On this VM Dragonfly was no faster with
io_uring than with `--force_epoll=true`. The prototype was removed. Evidence:
`io-uring-prototype-analysis.json` and
`io-uring-prototype-redis-benchmark-screen.json`.

**Correctness and controls for the same tree:**

- **Checks:** 713 Rust tests (macOS host), Clippy with `-D warnings` and rustfmt pass. The BDD suite passes: 28 features, 647 scenarios and 6,701 steps, with positive coverage of all 486 catalog entries. The live ACL freshness smoke passes all 1,401 checks.
- **Hot keys:** against the last shared-runtime build, in ABBA order with two samples each, single-key GET improved from 204k to 408k/s at P1 and from 246k to 825k/s at P16. Single-key INCR improved from 186k to 348k/s and from 210k to 588k/s.
- **Durable `everysec`:** writes were 6–13% faster than that build, and reads about 2×.

Evidence: `wave7b-hotkey-vs-stable.json`, `wave7b-everysec-vs-stable.json`,
`wave7b-bdd-evidence.json` and `wave7b-acl-smoke.json`.

**Isolation and limits:** `pmset` reported battery power with Low Power Mode
enabled throughout. Competing applications were **not** suspended for these runs.
Two repetitions per case do not establish statistical significance. No rows were
discarded. Evidence is in `benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`:
`wave7b-balanced.json`, `wave7b-analysis.json`, `wave7b-cross-generator.json` and
`wave7b-cross-generator-analysis.json`. The binary SHA-256 is
`b701a75a87ba5136cbf327986913ca83afa9708351154cb91bad2e4c477f190d`. The PGO and
source-experiment sections below are earlier history.

### Latest local PGO comparison with competing workloads suspended

The pinned experimental `fixed-key-pgo` binary completed **243/243 passing
trials** and **665,474,538 requested measured commands**. The matrix uses nine
workloads, three client/pipeline profiles and three repetitions per engine.
All engines used the same ARM64 Linux container, four server CPUs and four
separate client CPUs: Redis/redis-benchmark **8.2.1**, Dragonfly **2.0.2** with
four proactors, and **64 ffkv logical shards**.

ffkv had higher medians than Dragonfly in **12/27** combinations and Redis in
**6/27**. Against Dragonfly, these were all nine single-client cases and
pipelined GET 4 KiB, SET 4 KiB and ZRANGE. The other **15 combinations** remained
below Dragonfly. ZRANGE's **5.28%** median margin is not a significance claim.

Memory-only medians with **32 clients, pipeline depth 16 and four client threads**:

| Workload | ffkv commands/s | Redis commands/s | Dragonfly commands/s |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,469,275 | 1,559,840 | 1,740,595 |
| SET, 256 B | 1,463,150 | 1,731,681 | 2,310,938 |
| GET, 4 KiB | 1,307,475 | 779,815 | 1,040,477 |
| SET, 4 KiB | 1,095,434 | 549,810 | 939,529 |
| INCR | 1,468,930 | 2,087,708 | 2,773,618 |
| HGET | 1,727,332 | 1,733,387 | 2,079,936 |
| HSET | 1,387,667 | 1,468,930 | 2,189,821 |
| ZADD, existing member/new score | 1,307,475 | 545,574 | 1,836,142 |
| ZRANGE, ten members | 1,314,370 | 1,094,220 | 1,248,429 |

**Isolation and limits:** the run took place on November 4 from 13:46:02 to
14:16:41 +05:30. Power was AC before and after, with no recorded sleep/wake or
power-source change in that window. Low Power Mode remained enabled. With
explicit user authorization, 71 competing application/development/database
processes were confirmed suspended before measurement. At completion, 56 matching
identities remained stopped and were resumed; 15 Zed/Slack identities had exited
or been replaced. A fresh check found none of the recorded identities still
stopped. OS, networking, device management, the current agent and OrbStack were
left running. No Chrome video wake-lock events were recorded in this window.

No builds, tests, profiles, docs work or archive compression overlapped the run.
Four trials missed the five-second target; the shortest measured phase was
**3.501 seconds**. Noticeable repetition variation remains. Hardware frequency,
temperature and VM scheduling were not measured; suspension is not equivalent
to dedicated bare-metal isolation. No slow rows were discarded.

Raw trials and analysis are `fixed-key-pgo-isolated-balanced.json` and
`fixed-key-pgo-isolated-analysis.json` in
`benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`. The execution/restoration
record is `fixed-key-pgo-isolation-state.json`; archived tooling is indexed by
`fixed-key-pgo-isolation-tools.json`. The binary SHA-256 is
`15e9da05fba4ebcc058aa2e897d6ae83adfb912f2d2de88db4b25d372f5dc9d8`.
This is the same PGO binary as the earlier noisy run below, not a new source
optimization. PGO is experimental and is **not enabled by the default release
build**. These results do not establish across-the-board superiority.

### Cross-generator follow-up

The later client-sensitivity investigation completed **954/954 trials** and
**4,365,303,552 measured commands** using unmodified `redis-benchmark` 8.2.1,
memtier 2.5.2 and Dragonfly's 2.0.2 native client. It used the same frozen PGO
server, not a source optimization. The original 27-case result above remains
the latest full three-engine matrix.

For INCR with **32 connections, pipeline 16, eight client threads on four
client vCPUs and four separate server vCPUs**, the controlled AC confirmation
had these medians of three trials:

| Load generator | ffkv commands/s | Dragonfly commands/s |
| --- | ---: | ---: |
| redis-benchmark | 1,453,552 | 3,039,440 |
| memtier | 1,516,709 | 2,934,797 |
| dfly_bench | 1,550,079 | 3,142,670 |

Changing clients did not eliminate the INCR gap. GET 4 KiB behaved differently:
at the same eight client threads, giving memtier six rather than four client
vCPUs improved all three controlled repetitions. Client capacity must therefore
be checked per workload, not assumed from one operation.

The initial capacity sweep crossed from battery to AC; its absolute rates must
not be pooled with the separate AC confirmation. Low Power Mode remained on.
VM CPU affinity does not prove physical M4 P/E-core pinning, and neither CPU
frequency nor thermals were measured. Native timing, refill behavior, payload
patterns and some key spelling differ across generators; compare engines within
each generator, not latency percentiles across generators.

Evidence is in `cross-generator-analysis.json`, `cross-generator-provenance.json`
and `cross-generator-evidence.tar.gz` under the results directory above.
The archive retains raw trials, native output, harnesses and isolation records.

### Subsequent source A/B: request entry and integer storage

The first resumed source experiments used **unprofiled release binaries built
at the same manifest path with identical flags**, not the PGO binary above.
All **160 trials** passed their fixture checks, completing **1,259,557,312
measured commands**. These are source comparisons, not new Redis/Dragonfly wins.

The retained clock/pause change avoids the ready-only pause future for unpaused
requests and reuses the initial monotonic timestamp. Exhausted cooperative
budget still takes the yielding path; actual pause waits stay out of execution
timing. Thread-CPU and HOTKEYS accounting remain enabled.

With 32 connections and four native client threads, median changes against the
unchanged release control were:

| Workload | Pipeline | Throughput change | Server CPU/command change |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1 | +0.1% | −1.0% |
| GET, 256 B | 16 | +5.3% | −3.9% |
| GET, 4 KiB | 1 | −0.1% | −1.2% |
| GET, 4 KiB | 16 | +0.1% | −2.5% |
| INCR | 1 | −0.1% | +0.4% |
| INCR | 16 | +4.4% | −2.7% |

The 48-trial clock cohort used ABBA followed by BAAB, giving four samples per
binary and case. All four paired pipelined GET-256 and INCR comparisons favored
the candidate. This is not a significance claim: 23 trials missed the five-second
target, the shortest lasted 3.751 seconds, and the native 250-ms completion timer
still quantizes throughput.

The separate inline-integer storage candidate was **rejected and reverted**.
After the 72-trial screen, a longer 40-trial confirmation found essentially flat
INCR throughput (−0.2%), but HSCAN was slower in all four paired comparisons
(−5.3% median). Those measured phases lasted 10.502–14.913 seconds; 23 still
missed the requested 12 seconds. Favorable and unfavorable rows are retained.

These cohorts ran on battery with Low Power Mode enabled, four server vCPUs
and four separate client vCPUs. Selected competing applications were temporarily
suspended and identity-checked on restoration; no recorded identity remained
stopped. No builds, tests, profiles or archive compression overlapped measurement.
Controller editing/review remained active, and frequency and thermals were not
measured. Do not treat the absolute rates as comparable to the earlier AC/PGO run.

`resumed-wave1-analysis.json`, `resumed-wave1-provenance.json` and
`resumed-wave1-evidence.tar.gz` retain the decisions, all samples, frozen build
inputs, verification outputs, power observations and executable harnesses.
The archived analyzer reproduces the checked-in analysis byte-for-byte.

### Later source A/B: write admission and persistence

The next source cohorts completed **300 passing trials** and
**1,426,464,480 measured commands**: 144 for striped write admission and
synchronous request entry, followed by 108 persistence-lane screening trials
and 48 longer confirmation trials. These remain **unprofiled source A/B
comparisons**, not an updated Dragonfly/Redis matrix.

The admission and request-entry changes improved selected pipelined writes in
short, two-sample screens. They are provisionally retained, not evidence of a
combined across-the-board improvement. Synchronous entry's ZADD/P1 screen was
uneven and unfavorable; it remains part of subsequent combined verification.

Persistence read lanes avoid a shared reader-count cache line for eligible
memory-only requests. Exclusive persistence operations still close admission
and drain readers; AOF and backup paths retain their ordering and durability
requirements. The longer confirmation used ABBA followed by BAAB, with four
samples per binary/case, 32 connections, four client threads and four server
vCPUs:

| Workload | Durability | Pipeline | Throughput change | Server CPU/command change |
| --- | --- | ---: | ---: | ---: |
| GET, 256 B | Memory-only | 1 | +0.1% | +0.2% |
| ZRANGE, ten members | Memory-only | 1 | −1.4% | −0.5% |
| SET, 256 B | Memory-only | 16 | +6.4% | −4.9% |
| INCR | Memory-only | 16 | +5.2% | −5.2% |
| ZADD | Memory-only | 16 | +5.5% | −4.2% |
| SET, 256 B | Everysec | 16 | +0.9% | +0.7% |

All four paired comparisons favored the memory-only pipelined writes.
The larger initial ZRANGE regression did not reproduce at that magnitude,
but three of its four longer rate pairs remained below the control. The
everysec result includes an unusually weak first control sample; it does not
establish a durability-mode speedup. Single-server-CPU controls and all
unfavorable screen rows are retained in the analysis.

These runs used battery power and Low Power Mode. Actual confirmation durations
were 8.752–14.265 seconds; 12 of 48 missed the requested 12-second target.
Absolute throughput dropped substantially for **both** binaries late in the
write confirmation. The cause was not isolated: frequency and thermals were
not measured, and no samples were discarded to conceal the change.

The combined persistence candidate passed 690 Rust tests, 646 BDD scenarios
with positive wire evidence for all 486 catalog entries, and 12 live-client
verification cohorts. Five additional full stress cases passed on both control
and candidate: concurrent writers, WATCH/EXEC, pubsub/tracking, pause, AOF
transitions, backup/recovery and shutdown/crash boundaries. Crash checks retain
uncertain sent-but-unacknowledged operations instead of inventing guarantees.

`resumed-wave2-screen-analysis.json`, `resumed-wave3-screen-analysis.json` and
`resumed-wave3-confirmation-analysis.json` retain the individual samples.
`resumed-concurrency-provenance.json` and `resumed-concurrency-evidence.tar.gz`
include frozen source inputs, raw trial logs/counters, diagnostic evidence and
limitations. Generated benchmark WAL fixtures are excluded from the archive;
their deterministic setup and observed validation remain included.

### Frontend-poll locality diagnostic

A separate instrumented probe counted changes of **worker-thread identity
between polls of the same connection future**, not physical-core migration:

| Profile | GET, 256 B: changed-thread polls | INCR: changed-thread polls |
| --- | ---: | ---: |
| C1/P1/T1 | 0.13% | 0.13% |
| C32/P1/T4 | 56.48% | 56.74% |
| C32/P16/T4 | 75.48% | 75.80% |

All 18 full diagnostic trials passed fixture and counter-coverage checks.
Probe timing is **excluded from competitive throughput claims**. The results
motivated a separately frozen experiment that assigns connections to
single-worker frontend runtimes, without adding per-request dispatch or CPU
affinity. Its uninstrumented screen subsequently completed **144 passing trials**
and **328,167,622 measured commands**. With 32 connections, four client threads
and four server vCPUs, memory-only median throughput changed as follows:

| Workload | Pipeline 1 | Pipeline 16 |
| --- | ---: | ---: |
| GET, 256 B | +101.5% | −27.0% |
| SET, 256 B | +88.4% | −19.0% |
| GET, 4 KiB | +86.2% | −10.5% |
| SET, 4 KiB | +58.2% | −9.4% |
| INCR | +84.6% | −19.0% |
| HGET | +89.6% | −40.0% |
| HSET | +76.5% | −43.6% |
| ZADD | +42.6% | −42.4% |
| ZRANGE, ten members | +80.8% | −43.9% |

Both paired comparisons agree on the direction for every row: substantial
unpipelined gains, but pipelined regressions. Single-client rates were roughly
unchanged; the single-server-CPU memory controls also regressed. The candidate
is **not accepted as-is**. Removing task migrations is not sufficient evidence
of a better scheduler design.

`resumed-wave4-screen-analysis.json` retains every sample, CPU/command result,
single-CPU control and everysec control. These short screens used two samples
per binary/case, and do not establish statistical significance or change the
Dragonfly win count. One final-cohort preflight could not confirm suspension;
its cleanup stopped the owned lab. A first retry therefore could not start.
After restoring the lab, that previously unmeasured cohort completed; neither
failed attempt produced benchmark samples.

### Hot-key controls and runtime restoration

Further lease-spin, scheduler-interval and ACL-snapshot experiments completed
**120 passing trials and 509,692,640 measured commands**. Improving on the
regressed frontend build was not sufficient: the combined candidate was also
compared directly with the last stable shared-runtime build.

That control completed **56 passing trials and 193,223,232 measured commands**,
using 32 clients, four client threads, four server vCPUs and 64 shards. Each
case used ABBA order, with two samples per binary. The following changes are
against the stable build, not against Redis or Dragonfly:

| Workload | P1 throughput | P16 throughput | P1 CPU/command | P16 CPU/command |
| --- | ---: | ---: | ---: | ---: |
| Standard GET, 256 B | +104.60% | +13.87% | −21.09% | −11.06% |
| Standard INCR | +100.51% | +7.57% | −20.75% | −6.95% |
| Standard ZRANGE | +90.17% | −17.14% | −14.14% | +5.46% |
| Hot-key harness: distributed GET | +102.79% | +7.97% | −14.46% | −9.55% |
| Hot-key harness: single-key GET | −41.34% | −68.92% | +45.75% | +195.95% |
| Hot-key harness: distributed INCR | +98.32% | +7.70% | −14.23% | −7.02% |
| Hot-key harness: single-key INCR | −41.98% | −66.66% | +47.66% | +172.36% |

The hot-key harness keeps 10,000 resident keys but changes the native client's
random key range from 10,000 to one. GET decoys have a different payload length,
and INCR counters are reset and checked around every native phase. These are
separate diagnostics, **not replacements for or pooled with the standard rows**.
Every measured sample is retained; 22 of these 56 windows missed the five-second
target, with observed durations of 3.751–5.753 seconds. The Mac remained on
battery power with Low Power Mode enabled. Absolute rates across different
cohorts are not causal comparisons.

**Decision: restore the shared-runtime architecture.** The fixed frontend
runtimes and their follow-up scheduling candidates are not the repo default.
The retained clock, admission and persistence-lane work, the AOF-policy fix,
and immutable ACL snapshot invalidation remain. ACL changes still take effect
on subsequent commands; this does not cache authorization decisions or revive
deleted identities.

The restored source is byte-identical to a frozen build that passed 692 Rust
tests, Clippy, 647 BDD scenarios/6,701 steps with positive coverage of all 486
catalog entries, 1,401 live ACL checks, 12 live verification groups and all five
persistence stress cases. These checks establish correctness coverage, **not a
new throughput advantage**. This decision was later superseded: the actor
contention path fixed the hot-key regression, and the frontend runtimes are now
the default. See "Latest integrated source: matched comparison" above.

Evidence in `benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`:
`resumed-lease-acl-screen-analysis.json`, `resumed-stable-control-analysis.json`,
`resumed-lease-acl-provenance.json`, `resumed-lease-acl-evidence.tar.gz`,
`resumed-lease-acl-reproduction.json`, and
`resumed-stable-runtime-restoration.json`. The archive includes failed initial
compilation logs and all completed source comparisons; both analyses reproduce
byte-for-byte with its archived summarizer.

For subsequent runs, a separately frozen calibration revision extends native
calibration to at least two seconds when possible, retaining the completion
timer, request-count cap and every duration miss. Its identical-binary control
passed 12 trials/41,193,376 commands, with every measured window exceeding five
seconds. Earlier reports and their shorter calibration are unchanged.

### Earlier root/session comparison

The frozen `root-session` candidate completed **243/243 passing trials** and
**660,859,945 requested measured commands**. Each of the nine workloads ran with
C1/P1/T1, C32/P1/T4 and C32/P16/T4, with three repetitions per engine.
All engines used the same ARM64 Linux container and four server CPUs, with four
separate client CPUs: Redis/redis-benchmark **8.2.1**, Dragonfly **2.0.2** with
four proactors, and **64 ffkv logical shards**.

ffkv had higher median throughput than Dragonfly in **10/27** combinations and
Redis in **5/27**. Against Dragonfly, these were the nine single-client cases and
pipelined GET-4K; its **5.44%** median margin is not a significance claim.
The other **17 concurrent combinations** remained below Dragonfly.

Memory-only medians with **32 clients, pipeline depth 16 and four client threads**:

| Workload | ffkv commands/s | Redis commands/s | Dragonfly commands/s |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,552,586 | 2,079,520 | 2,607,092 |
| SET, 256 B | 1,307,750 | 1,833,473 | 2,444,134 |
| GET, 4 KiB | 1,219,432 | 779,153 | 1,156,522 |
| SET, 4 KiB | 981,899 | 497,557 | 1,099,744 |
| INCR | 1,314,647 | 2,079,520 | 2,971,677 |
| HGET | 1,554,633 | 1,644,906 | 2,599,660 |
| HSET | 1,224,248 | 1,468,584 | 2,456,444 |
| ZADD, existing member/new score | 1,156,008 | 520,688 | 1,835,710 |
| ZRANGE, ten members | 1,182,026 | 1,093,760 | 1,390,998 |

**Limitations:** four trials missed the five-second target; the shortest measured
phase was **3.501 seconds**. Battery charge was 71% before the run and 65% after.
The comparison ran on November 4 from 01:54:59 to 02:25:25 +05:30; lid-close sleep
at 02:27:43 interrupted later verification, not these measurements.
CPU-frequency and thermal stability were not measured. No builds, tests, docs
builds or profiles ran during the matrix. Timer quantization and host variation
limit small differences; cross-run changes are not causal estimates.

Raw trials, correctness checks, native latency fields, CPU/RSS observations and
analysis are `root-session-balanced.json`, `root-session-analysis.json` and
`root-session-power.json` in
`benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`.
The binary SHA-256 is
`7e7717712eb1b1d9a29e54b79e9bbc23c9002660b9c900880dbae3df75ad6e52`;
`root-session-provenance.json` records the source and verification.
This includes the collection/traversal changes described below, but not the
subsequent fixed-key routing or profile-guided optimization experiments.
The earlier owned-value run remains in `owned-values-balanced.json` and
`owned-values-analysis.json`: 243 passing trials, 672,501,511 measured commands,
and higher medians in 10/27 cases versus Dragonfly and 5/27 versus Redis.

### Earlier activity/dispatch comparison

The frozen `activity-dispatch` candidate completed **243/243 passing trials** and
**624,948,827 requested measured commands**, with three repetitions per
workload/profile/engine. It shares the ARM64 Linux container, four server CPUs,
four client CPUs, Redis/redis-benchmark **8.2.1**, Dragonfly **2.0.2** with four
proactors, and **64 ffkv logical shards** described below.

ffkv had higher medians than Dragonfly in **11/27** combinations and Redis in
**4/27**. Nine of the Dragonfly comparisons were single-client cases. Among the
concurrent cases, pipelined GET 4 KiB had a higher median; SET 4 KiB's **0.15%**
advantage is effectively a tie, not a reliable lead. The other **16 concurrent
combinations** remained below Dragonfly.

Memory-only medians with **32 clients, pipeline depth 16 and four native client
threads**:

| Workload | ffkv commands/s | Redis commands/s | Dragonfly commands/s |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,412,815 | 1,641,868 | 2,077,172 |
| SET, 256 B | 867,135 | 1,247,156 | 1,566,292 |
| GET, 4 KiB | 985,228 | 775,924 | 866,750 |
| SET, 4 KiB | 824,566 | 526,811 | 823,315 |
| INCR | 1,137,974 | 2,079,520 | 2,446,811 |
| HGET | 1,418,913 | 1,738,117 | 2,080,352 |
| HSET | 1,095,434 | 1,560,302 | 1,950,872 |
| ZADD, existing member/new score | 1,031,165 | 548,156 | 1,642,213 |
| ZRANGE, ten members | 986,565 | 1,094,220 | 1,188,039 |

**Limitations:** two trials missed the five-second target; the shortest derived
native duration was **3.751 seconds**. The power-event log shows AC throughout
the reported run interval, with the transition to battery **14.9 seconds after**
its end. This is not a hardware-frequency trace or an isolated bare-metal run.
Between-sample variation remains; separate historical runs are not controlled
before/after measurements. No builds, tests or profiles ran during this matrix.

This candidate reuses an activity timestamp, static registered command names
and typed administrative dispatch. It passed **667 Rust tests**, **646 BDD
scenarios**, seven real-server suites, memory/hybrid recovery, transport and
concurrent metrics checks. Its source archive, binary hashes and verification
logs are linked by `activity-dispatch-provenance.json` in
`benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`. The full comparison,
analysis and power chronology are `activity-dispatch-ac-balanced.json`,
`activity-dispatch-ac-analysis.json` and `activity-dispatch-ac-power.json`.
Later unmeasured source changes are not part of these results.

### Owned-value candidate: correctness, retention and ABBA diagnostic

The subsequent `owned-values` candidate passed **671 Rust tests**, **646 BDD
scenarios**, the seven live suites, memory/hybrid recovery and transport/access/
metrics checks. Additional live scenarios exercise binary overwrite pipelines,
integer boundaries, COPY/RESTORE, HLL cache writes, RESP3 scored ranges,
repeated ZADD members and self-source ZRANGESTORE, including WAL and snapshot
restarts. Its source and verification are recorded in
`owned-values-provenance.json`. Both the full comparison above and the
fixed-count before/after diagnostic below completed.

The same retention diagnostic produced these **single observations of retained
process RSS**, before purge, in decimal MB:

| Diagnostic | Activity/dispatch RSS, MB | Owned-values RSS, MB |
| --- | ---: | ---: |
| 20,000 non-pipelined SETs, 256 B | 31.1 | 30.1 |
| 512 one-byte strings interleaved with 65,500-byte ECHOs | 63.9 | 18.3 |
| 512 one-byte hash values interleaved with the same ECHOs | 63.7 | 63.9 |

Every stored value was read back byte-exactly. These are not repeated estimates
or a general RSS bound. The hash case deliberately shows that the new string
ownership rule does not apply to every datatype. New/shared strings and
replay/import now pay a placement copy; throughput must be measured rather than
inferred from memory retention.

An isolated counting-allocator probe also verified allocation-free repeated
same-size string overwrites and integer updates without outstanding views or
change capture, including after integer digit-width shrink. It preserves a held
old-value view across an overwrite. This is a module-level result, not a claim
that complete server commands allocate nothing. The probe source and results are
preserved in `owned-string-allocation-probe.tar.gz` and
`owned-string-allocation-analysis.json`.

The diagnostic ran **36 successful trials and 301,989,888 measured commands**:
9 workloads, C32/P16/T4, 8,388,608 commands per trial, with
activity/owned/owned/activity ordering. Medians below have only two samples per
candidate and workload.

| Workload | Activity/dispatch, req/s | Owned-values, req/s | Owned/activity |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,383,426 | 1,560,934 | 1.128 |
| SET, 256 B | 931,757 | 986,605 | 1.059 |
| GET, 4 KiB | 958,315 | 958,424 | 1.000 |
| SET, 4 KiB | 737,322 | 729,096 | 0.989 |
| INCR | 944,957 | 1,001,489 | 1.060 |
| HGET | 1,269,782 | 1,197,859 | 0.943 |
| HSET | 1,197,945 | 1,242,389 | 1.037 |
| ZADD | 1,118,034 | 1,197,859 | 1.071 |
| ZRANGE, 10 members | 1,137,457 | 1,197,859 | 1.053 |

GET-256's baseline endpoints varied from 1.525M to 1.242M req/s; HGET's from
1.198M to 1.342M. Their median changes are not established causal improvements
or regressions. Timer quantization also limits small comparisons. Both power
observations reported battery operation, from 99% to 97%; CPU-frequency stability
was not measured. SET-4K's median RSS fell from 203.3 MB to 65.7 MB in this
diagnostic, alongside its small throughput decrease. Raw trials, CPU/RSS
observations and analysis are retained as `owned-values-abba-*.json`.
This is not a Dragonfly comparison or a universal performance claim.

### Collection candidate: correctness and retention

The subsequent `owned-collections` candidate passed **675 Rust tests**, **646 BDD
scenarios**, all seven live suites, recovery, TLS/access, subscriber and concurrent
metrics checks. Additional live scenarios cover hash binary overwrite pipelines,
repeated HSETEX fields and TTL preservation, hash COPY/RESTORE, integer conditions,
large list/sorted-set arrays and nested Lua replies in both protocols, followed by
WAL and snapshot restarts for persistent memory and hybrid profiles.

It adds owned hash values/names, private iterator-based reply traversal and fewer
string-write lookups. Paired throughput measurements are below. Retention
diagnostics produced these single process-RSS observations, in decimal MB:

| Diagnostic | Owned-values RSS, MB | Owned-collections RSS, MB |
| --- | ---: | ---: |
| 20,000 non-pipelined SETs, 256 B | 30.1 | 29.7 |
| 20,000 non-pipelined HSETs, 256 B | 54.3 | 55.7 |
| 512 one-byte strings interleaved with 65,500-byte ECHOs | 18.3 | 18.8 |
| 512 one-byte hash values interleaved with the same ECHOs | 63.9 | 18.9 |
| 64 open connections after an 8,192-member flat reply each | 54.1 | 31.0 |

Every response/value was checked byte-exactly. These are not repeated memory
estimates or a general bound: the ordinary HSET case is slightly larger, while
the adversarial hash-frame retention and flat-reply stack cases are smaller.
The aggregate diagnostic verified 524,288 members across the open connections,
then nested replies and further requests on every connection. Its seeded-process
RSS differed between builds, so the totals do not isolate one allocation.

New/shared hash writes, replay and imports pay placement copies. Captured
multi-field writes also allocate/sort row positions to preserve repeated-field
journal semantics. Compiler layout measurements below distinguish encoder
state from the complete connection future.
Source, hashes and verification are recorded in `owned-collections-provenance.json`;
the raw diagnostics include `owned-collections-memory-candidate.json` and
`owned-collections-aggregate-retention.json`.

The fixed-count old/new/new/old diagnostic completed **36 successful trials**
and **301,989,888 measured commands**, with C32/P16/T4 and 8,388,608
commands per trial. Each median has only two samples:

| Workload | New/old median throughput |
| --- | ---: |
| GET, 256 B | 0.884 |
| SET, 256 B | 0.936 |
| GET, 4 KiB | 0.982 |
| SET, 4 KiB | 1.055 |
| INCR | 1.450 |
| HGET | 0.942 |
| HSET | 0.999 |
| ZADD | 0.951 |
| ZRANGE, 10 members | 1.053 |

This is **not an across-the-board speed improvement**. GET/HGET medians were
lower, while ZRANGE was higher. SET-4KiB varied sharply within the sequence:
the baseline endpoints were 860k and 172k req/s; the candidate samples were
839k and 251k. INCR also began with a much slower baseline sample. Their median
ratios must not be interpreted as causal improvements. The cause of this
variation has not been established. Battery observations were 81% before and
79% after; frequency/thermal stability was not measured. No builds, tests,
docs builds or profilers overlapped this run. Raw trials and CPU/RSS values
are preserved in `owned-collections-abba-*.json`.

The subsequent compiler diagnostic measured these future sizes, in bytes:

| Future | Owned-values | Owned-collections |
| --- | ---: | ---: |
| Reply encoder | 688 | 832 |
| Socket command | 15,760 | 15,760 |
| Complete connection | 17,248 | 17,248 |

The larger encoder did not enlarge the enclosing connection future. These
layouts do not establish the cause of throughput variation. The initial
combined diagnostic reused Cargo's baseline output for its second source;
that output was discarded as candidate evidence. A distinct diagnostic
metadata flag forced the candidate compilation, whose complete log is
preserved with `owned-collections-future-layout-analysis.json`.

Separate GET/INCR CPU profiles at C32/P1 and C32/P16 passed correctness checks.
Their timings are excluded from comparisons. Reviewed reactor-locality
alternatives were rejected: the prominent wake samples were kernel socket
wakeups, not Tokio unparks, while the proposed splits reduced queued execution
capacity or added threads. The runtime topology remains unchanged.

### Root/session candidate: verified behavior and measured limits

Relative to the collection candidate, `root-session` changes only `src/main.rs` and
`src/sharding.rs`: cached canonical root predicates replace repeated name
folding, and shard execution takes the request's existing session handle.
It passed **677 Rust tests**, formatting, clippy and the release build, followed
by **646 BDD scenarios**, all seven live suites and the existing recovery,
transport, access, subscriber and concurrent-metrics scenarios.

An additional live smoke runs mixed-case replication aliases, all four EVAL
variants, a scripted transaction, HELLO 2/3, invalid root spellings, interleaved
session databases and hash counters, RESET name/database/monitor transitions,
and CLIENT PAUSE/UNPAUSE. It passed on both the pinned baseline and candidate
in ephemeral memory, strict persistent memory and strict hybrid profiles.
Source and verification are recorded in `root-session-provenance.json`.
Its full matched comparison is the earlier root/session result above. It trails
Dragonfly in 17 concurrent cases; these changes did not establish an
across-the-board throughput improvement.

### Fixed-key candidate and compiler experiment

The subsequent `fixed-key` candidate derives single-key positions from the
compiled command specifications. It avoids general span construction in its
ordinary routing/key-extraction path while preserving counted-key validation,
optional positions, ACL checks and coordinated commands.

It passed **679 Rust tests**, formatting, clippy, the release build and the full
live/recovery/transport/metrics suite, including **646 BDD scenarios**.
Lid-close sleep interrupted the initial Rust run after 633 test passes; only the
unfinished server test, CLI/MCP tests and remaining checks were resumed.
`fixed-key-provenance.json` and `fixed-key-resumed-verification.json` preserve
that distinction. No throughput gain is claimed before measurement.

A separate profile-guided optimization experiment uses the same frozen source
and an explicit compiler target, without changing the ISA or Cargo release
profile. Its mixed training workload passed **25,949 command assertions** on
the verified unprofiled baseline, plus full read-back and durable restarts.
Training differs from the comparison in seed, payload sizes and client/pipeline
mix. Instrumented timings are not benchmark results. The optimized binary passed
the full runtime suite, including all 646 BDD scenarios; the same-source
unprofiled control also passed the mixed training workload and durable restarts.

The paired compiler comparison completed **36/36 trials** and **301,989,888
requested measured commands** at C32/P16/T4. Each workload used
control/PGO/PGO/control order, with 8,388,608 commands per trial:

| Workload | PGO / control median throughput |
| --- | ---: |
| GET 256 B | 1.105 |
| SET 256 B | 1.182 |
| GET 4 KiB | 1.091 |
| SET 4 KiB | 1.206 |
| INCR | 1.131 |
| HGET | 1.078 |
| HSET | 1.195 |
| ZADD | 1.072 |
| ZRANGE 10 members | 1.146 |

These are two samples per binary/workload, not significance claims. The 4 KiB
cases varied noticeably; all four endpoints are retained in
`fixed-key-pgo-abba-analysis.json`. No builds, tests, profiles or archive
compression overlapped measurement. The host remained on battery, without
measured CPU-frequency or thermal stability.

This isolates the compiler experiment, not the fixed-key source change, and
does **not** establish a Redis/Dragonfly win. The subsequent three-engine run
completed **243/243 correctness checks** and **589,201,517 requested measured
commands**, but is retained as **noisy exploratory evidence**, not a controlled
competitive improvement. All three engines varied sharply between repetitions;
15 trials missed the five-second target. Battery charge fell from 50% to 38%,
and the host power log records concurrent Chrome video activity. Its CPU cost
and any thermal/frequency effects were not measured; causality is not established.

`fixed-key-pgo-balanced.json`, `fixed-key-pgo-analysis.json` and
`fixed-key-pgo-power.json` retain every trial and the limitation. No slow rows
were discarded. The earlier `root-session` table remains labeled with its own
binary and measurement window; it must not be presented as the PGO result.
The later workload-suspended local rerun is reported at the top of this page,
separately from these noisy measurements.

`fixed-key-pgo-provenance.json`, `fixed-key-pgo-manifest.json`, the merged
profile and build logs preserve the compiler inputs and verification. The
merged profile contains 889 inputs, including the instrumented compatibility
and recovery suites: it is not a production workload trace. The optimized
build reported zero profile-mismatch warnings and 243 missing-function
warnings. PGO remains an experimental artifact; the default release build
does not enable it.

### Observed 2026-11-03 Linux comparison

The historical balanced run below measures the frozen `durable-position` candidate.
Evidence is preserved in `benchmarks/results/dragonfly-vs-ffkv-2026-11-03/`:
`durable-position-native.json`, `durable-position-analysis.json`, power observations,
the build-input manifest, and `durable-position-source.tar.gz`. The executable's
SHA-256 is `cfab8f62bb3969915f710cf12037c2205977a8b024c9edac8b00e5ae119704a7`.
This candidate includes compact entries, borrowed command arguments, lazy
ephemeral replication history and explicit optional durable positions. Subsequent
source changes are **not** included in these measurements.

All three servers ran inside the same ARM64 Linux container on the Apple M4
workstation, with server CPUs `0-3` and native-client CPUs `4-7`. This run used
Redis/redis-benchmark **8.2.1**, Dragonfly **2.0.2** with **four proactors**, and ffkv
with **64 logical shards**. Logical shards are not dedicated CPU cores. The run
rotated engine order over three repetitions and targeted five seconds per
measured trial.

**243/243 trials passed**, covering **624,365,083 measured commands**. ffkv had
higher median throughput than Dragonfly in **9/27** workload/profile combinations,
all single-client cases; it lost all **18 concurrent** combinations. It had higher
medians than Redis in **4/27** combinations. This is not an across-the-board
performance lead or a statistical significance claim.

Memory-only medians with **32 clients, pipeline depth 16 and four native client
threads**:

| Workload | ffkv commands/s | Redis commands/s | Dragonfly commands/s |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,294,269 | 2,079,520 | 2,456,444 |
| SET, 256 B | 1,040,176 | 1,731,681 | 2,311,451 |
| GET, 4 KiB | 913,314 | 778,997 | 1,040,685 |
| SET, 4 KiB | 551,766 | 435,138 | 991,829 |
| INCR | 821,161 | 1,835,279 | 2,189,821 |
| HGET | 1,247,904 | 1,833,473 | 2,600,310 |
| HSET | 975,256 | 1,467,380 | 2,085,244 |
| ZADD, existing member/new score | 966,542 | 517,825 | 1,836,142 |
| ZRANGE, ten members | 970,139 | 1,094,220 | 1,468,930 |

**Power is a material limitation:** the endpoint observations show battery power,
falling from 83% to 75%; they do not establish stable clocks throughout the run.
Six trials missed the duration target; the shortest lasted 3.501 seconds.
All remain included and flagged. This run used Linux **7.2.12**. The earlier
`observed-metadata` run used Linux 7.0.14 and reached 1% battery; its raw report,
analysis and source archive remain preserved. Neither that comparison nor the
older native-macOS table below is a controlled before/after measurement of code
alone. These are developer-workstation observations, not stable-power capacity.

The same frozen candidate passed 652 Rust tests, 646 BDD scenarios, seven
real-server smoke suites, strict memory/hybrid recovery soaks with 64 shards,
first-replica snapshot/stream checks and concurrent HTTP metrics scraping.
Those checks do not prove unmeasured production capacity, all-command performance,
absence of bugs or power-loss durability. Subsequent review reproduced a
CLIENT PAUSE renewal race outside the measured workloads; later corrections
must not be attributed to this frozen executable.

A later `flush-observation` candidate fixes the pause-renewal race, batches
delivery observations and avoids the publication mutex only for eligible
observer-free, inactive-history memory commits. It passed **660 Rust tests**,
the same **646 BDD scenarios** and real-server suites, including pipelined
HTTP metrics checks. Its source archive and manifest are preserved alongside
`flush-observation-probe.json` and `flush-observation-analysis.json`.

That later run is a **single-repetition diagnostic**, not a replacement for the
balanced table above: **81/81 trials passed**, with **125,632,434 measured
commands**, but ffkv still lost all **18 concurrent** comparisons to Dragonfly.
Four trials missed the three-second target; the shortest lasted **2.251 seconds**.
Battery observations fell from **68% to 66%**, both discharging. Keep those
limitations attached to the results; neither a short diagnostic nor successful
correctness checks establish an across-the-board performance advantage.

The newer `pipeline-io` candidate reuses emptied argument vectors, performs bounded
ready-only input continuation, and grows aggregate reply scratch on demand within
the documented [network bounds](/reference/limits/#network-execution-and-databases).
It passed **663 Rust tests**, **646 BDD scenarios**, the real-server suites and
plain/TLS 1.2/TLS 1.3 partial-upload and half-close checks. Its source archive,
manifest, compressed verification logs and diagnostic tools are preserved in the
same evidence directory.

Its matched single-repetition diagnostic (`pipeline-io-probe.json`) passed
**81/81 trials**, measuring **112,910,507 commands**. ffkv had higher throughput
than each reference in **8/27** cases, but still lost all **18 concurrent** cases
to Dragonfly. Three trials missed the three-second target; the shortest lasted
**1.474 seconds**. Battery observations fell from **38% to 35%**, discharging.
This does not replace the balanced table or establish a performance lead.

A separate fixed-count diagnostic alternated the previous and new ffkv binaries
in **old/new/new/old** order for each workload, using the same 32-client,
depth-16, four-thread profile and **4,194,304 commands per trial**. With only
two samples per candidate, the observed median throughput ratios were:

| Workload | `pipeline-io` / `flush-observation` |
| --- | ---: |
| GET, 256 B | 1.000× |
| GET, 4 KiB | 1.097× |
| SET, 4 KiB | 1.332× |

`pipeline-abba-analysis.json` links every underlying report; battery endpoints
were **33% and 32%**, discharging. These limited ratios support further testing,
not small-percent accuracy or an all-workload claim. Separate instrumented
flush/vector counters and owned-process CPU profiles diagnose work performed;
their throughput is excluded from the comparative results.

A server-only mimalloc prototype was evaluated and **not adopted**. Its
nine-workload fixed-count ABBA diagnostic completed **36 trials** and
**301,989,888 measured commands**, but throughput gains were inconsistent and
every measured workload used more resident memory. Each candidate had only
two observations per workload, with substantial variation; the host remained
on battery. `allocator-abba-analysis.json`, raw reports, power observations
and `allocator-prototype-source.tar.gz` preserve the rejected experiment.
Concurrent `MEMORY PURGE` checks preserved all live values and clean shutdown,
but reclamation remained best effort. ffkv still uses the system allocator.

The subsequent `bound-command` candidate reuses validated command metadata and
fuses hash mutation lookups. It passed **665 Rust tests**, the full BDD/live suites,
recovery, transport, access-control and concurrent metrics checks. Its source and
verification logs are preserved with `bound-command-provenance.json`.

Its later AC-powered, counterbalanced comparison passed **243/243 trials**, with
**429,158,739 requested measured commands**. The medians were higher than
Dragonfly in **9/27** cases and Redis in **7/27**, but individual samples varied by
multiples. AC endpoints were **39% and 81%**, charging; power-event logging began
partway through the run. **20 trials** missed the five-second target, with a
minimum derived native duration of **2.004 seconds**. AC alone did not establish
stable capacity. `bound-command-ac-balanced.json`, `bound-command-ac-analysis.json`
and `bound-command-ac-power.json` retain all samples and limitations. The earlier
`bound-command-probe.json` crossed from battery to AC and is not a controlled
before/after comparison.

The `direct-read` candidate adds direct adaptive socket reads and single-entry
GET/HGET access accounting. It passed **666 Rust tests**, the full BDD/live and
recovery suites, plain/TLS transport checks, and access/metrics checks. A
fixed-count ABBA comparison against `bound-command` ran **36 passing trials**,
each requesting **8,388,608 measured commands**, with the same 32-client,
depth-16, four-thread profile. Both endpoints were on AC at **99%**, finishing
charge. Median observations, with **two samples per candidate**:

| Workload | Throughput, direct / bound | Bound RSS, MB | Direct RSS, MB |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1.000× | 25.2 | 24.1 |
| SET, 256 B | 1.000× | 39.3 | 39.4 |
| GET, 4 KiB | 1.034× | 76.2 | 69.5 |
| SET, 4 KiB | 1.257× | 147.3 | 195.3 |
| INCR | 1.000× | 21.9 | 20.7 |
| HGET | 1.022× | 23.5 | 22.2 |
| HSET | 0.983× | 25.0 | 24.0 |
| ZADD | 0.971× | 40.7 | 39.8 |
| ZRANGE | 1.000× | 40.7 | 39.9 |

MB means 1,000,000 bytes. Near ties and small differences are not reliable
speedup/regression claims with this sample count and the native client's timer
quantization. The large-write throughput gain comes with higher retained RSS,
not a free improvement. Stored byte slices can keep whole network allocations
alive; logical dataset estimates do not measure that backing retention.
`direct-read-abba-analysis.json` and its raw reports preserve the tradeoff.

A rejected reserve-every-read prototype also exposed a small-value regression.
For 20,000 non-pipelined 256-byte SETs, its RSS increase was **97.4 MB**, versus
**13.9 MB** for the baseline. The corrected previous-read-size hint reduced that
increase to **13.7 MB** while preserving all 20,000 byte-exact readbacks. Hash
values, large interleaved ECHOs, multi-megabyte binary values and clean shutdown
were also exercised. These observations are not a general RSS bound.
`direct-read-memory-*.json`, both source archives and compressed verification
logs preserve the reproduction and correction.

Earlier diagnostic files are labeled by their own frozen candidate. For example,
`observer-state-probe.json` contains 81 successful trials and 49,006,962 measured
commands, but only one repetition per case; 30 trials missed its two-second
target. It still lost all concurrent comparisons to Dragonfly. That run used
Linux 7.2.12 after the owned lab restarted, rather than the earlier Linux 7.0.14,
and ran on battery. Do not use these short diagnostics or cross-kernel differences
as stable-capacity or causal speedup claims. The report, analysis, power observations
and source manifest remain available together.

### Observed 2026-11-02 comparison

Raw evidence and a machine-readable analysis are in `benchmarks/results/redis-vs-ffkv-2026-11-02/`: `native.json`, `client-sensitivity.json`, three `backlog-*.json` files, `native-smoke.json`, `environment.json` and `summary.json`. The summary records binary/runner SHA-256 hashes, reproducible commands, every profile's medians/ranges and limitations.

The run used ffkv 0.1.0 release, Redis and redis-benchmark 8.2.1, an Apple M4 with ten cores and 16 GiB RAM, macOS 27.0.1, loopback networking and four ffkv shards. **Battery power and Low Power Mode were enabled.** Existing applications remained running. These are developer-workstation observations, not peak hardware or isolated server capacity.

All 198 main trials, 24 client-thread sensitivity trials, 22 short smoke cases and 12 list-backlog cases passed their correctness checks. The native measured trials completed 124,637,682 commands. Warmup, calibration and smoke commands are excluded from that count.

Memory-only throughput below is the median of three trials with **32 clients, pipeline depth 16 and four native load-generator threads**. The data model is preloaded and bounded; reads are hits and writes change existing values/scores.

| Workload | Redis commands/s | ffkv commands/s | Redis / ffkv |
| --- | ---: | ---: | ---: |
| GET, 256 B | 1,371,602 | 98,271 | 13.96× |
| SET, 256 B | 1,142,741 | 60,476 | 18.90× |
| GET, 4 KiB | 628,736 | 94,375 | 6.66× |
| SET, 4 KiB | 471,859 | 58,982 | 8.00× |
| INCR | 1,369,181 | 65,514 | 20.90× |
| HGET | 1,142,741 | 98,271 | 11.63× |
| HSET | 943,341 | 58,959 | 16.00× |
| ZADD, existing member/new score | 377,091 | 65,536 | 5.75× |
| ZRANGE, ten members | 785,226 | 8,914 | 88.09× |

Redis had higher median throughput in every matched main profile. That does not mean every latency sample favored Redis, nor that the ratios apply to another workload. Pipelined ZRANGE was especially weak for ffkv; its native p99 median was 200.575 ms versus Redis's 0.775 ms. These use the first-read-event semantics described above.

Durable 256-byte SET with **32 clients and no pipelining**:

| Policy | Redis commands/s | ffkv commands/s | Redis native p99 (ms) | ffkv native p99 (ms) |
| --- | ---: | ---: | ---: | ---: |
| Everysec | 104,968 | 26,252 | 0.383 | 3.263 |
| Strict / AOF always | 4,434 | 297 | 10.367 | 157.183 |

Single-client strict-write throughput was similar: 317 Redis versus 308 ffkv commands/s. Concurrent strict writes diverged. The source shows ffkv serializing per-command WAL synchronization; this measurement alone does not establish a full causal performance profile. Strict results varied considerably: Redis's 32-client/non-pipelined throughput ranged from 4,369 to 7,198 commands/s across the three trials, so avoid fine-grained claims based on the median.

**Client sensitivity matters.** For 256-byte GET, switching from four to one native client thread increased ffkv's pipelined median from 98,271 to 124,090 commands/s and Redis's from 1,371,602 to 1,419,497. Eight threads reduced both. The non-pipelined four-thread Redis client consumed about 3.58 CPU cores; the one-thread alternative was faster. Therefore the default profile is a reproducible comparison, not either engine's optimized ceiling. Reproduce the supplemental check with:

```sh
python3 benchmarks/redis_compare.py --modes memory --workloads get-256 \
  --profiles c32-p1-t1 c32-p1-t8 c32-p16-t1 c32-p16-t8 \
  --repetitions 3 --target-seconds 3 --json client-sensitivity.json
```

All main measured trials lasted at least 2.006 seconds, but three missed the 80%-of-three-second duration target; they remain flagged and included. Reported latency aggregates are medians of trial percentiles, not pooled percentiles.

For 8,192 queued bodies, memory-backend backlog RSS medians were:

| Body size | Logical payload | Redis RSS (MiB) | ffkv RSS (MiB) |
| --- | ---: | ---: | ---: |
| 4 KiB | 32 MiB | 48.89 | 149.16 |
| 64 KiB | 512 MiB | 522.28 | 945.06 |

These are current resident sizes at the backlog phase, not peak RSS or evidence of a leak. The list harness uses a Python-threaded client, so its throughput is not treated as native capacity evidence. Hybrid storage, mixed application traffic, eviction, replication, clustering, TLS and beyond-RAM datasets were not measured in this comparison.

## List-backlog comparison

`benchmarks/compare.py` measures real LPUSH/LINDEX/LPOP list workloads against owned ffkv/Redis processes, with optional Dragonfly. It compares list remove-on-pop semantics, **not native lease/ACK semantics**. `benchmarks/runtime.py` supplies the standard-library RESP client, process ownership, histogram and current-RSS measurement. No performance numbers are generated by this documentation.

### Reproduce the list workload

Build a release server. Install/provide `redis-server` if including the owned Redis reference:

```sh
cargo build --release --bins
python3 benchmarks/compare.py --binary target/release/ffkv \
  --items 4096 --hot-operations 512 \
  --json benchmark.json --csv benchmark.csv
```

The harness owns disposable processes and temporary directories; it does not require a pre-existing ffkv server. If Redis is intentionally unavailable, explicitly omit it rather than treating absence as a result:

```sh
python3 benchmarks/compare.py --binary target/release/ffkv --skip-redis \
  --items 4096 --hot-operations 512 --json benchmark-ffkv.json
```

For three owned engines, add `--engines ffkv redis dragonfly --modes memory --dragonfly-binary /path/to/dragonfly`. The same identity, persistence and unsupported-mode rules apply as in the native comparison. Linux `--server-cpus` pins each owned server; `--client-cpus` pins the **Python harness process**, not a separate native client. Its threaded Python workload is useful for backlog correctness and memory observations, but is not an engine-capacity measurement.

Do not point external options at a production dataset. Even though external cases use a unique `ffkv:benchmark:*` namespace and are not reconfigured, they still create/read/drain real lists and consume resources.

## Workload and tunable controls

The default matrix includes memory/strict/everysec durability with 256 B, 4 KiB and 64 KiB bodies. ffkv hybrid cases require persistence, so volatile mode does not invent a hybrid-without-WAL case. Owned persistence settings are aligned/verified; this matters more than comparing unrelated defaults.

| Option | Current default / use |
| --- | --- |
| `--binary`, `--redis-binary` | target/release/ffkv, redis-server |
| `--skip-redis` | Explicitly omit Redis |
| `--engines`, `--dragonfly-binary` | ffkv redis; opt into dragonfly with its executable |
| `--ffkv-shards`, `--dragonfly-threads` | ffkv's own default / 4 Dragonfly proactors |
| `--server-cpus`, `--client-cpus` | Optional disjoint Linux CPU lists; no affinity override by default |
| `--modes` | memory strict everysec |
| `--payloads` | 256 4096 65536 bytes; values 1 byte–1 MiB |
| `--items` | 512 total queued bodies per case, **not per worker**; accepts suffixes such as 1m/5m/10m |
| `--concurrency` / `--jobs` | 4; 1–min(items, 128) |
| `--pipeline` | 8; 1–128, with combined payload/framing safety validation |
| `--hot-items` | 4 |
| `--hot-operations` | 256 per worker when duration is zero |
| `--duration` | 0; positive seconds select a timed hot phase per case |
| `--timeout`, `--maintenance-timeout` | 15 / 120 seconds |
| `--inline-threshold`, `--cache-bytes` | 1024 / 1048576 bytes for hybrid benchmark cases |
| `--segment-bytes`, `--disk-budget` | 8388608 / 0 bytes |
| `--json`, `--csv` | JSON defaults to stdout; CSV optional |

The benchmark cache/segment settings differ from server defaults. Record the complete invocation and report settings rather than assuming they match your deployment. Larger million-item or timed runs are available but were not automatically performed by publishing a small baseline.

## Interpret phases correctly

The harness records enqueue/backlog RSS, first reads, hot repeats, drain and maintenance. **Cold means the first application LINDEX pass over the backlog, not eviction of OS page cache.** Do not label this a physical-disk cold-cache test.

Latency is measured from batch send-start to each reply and includes pipeline queueing. It is not divided by pipeline depth to invent per-command latency. Percentiles are measured histogram bucket bounds with their method/semantics recorded. Throughput is completed operations divided by elapsed phase time; attempted requests, errors, unknown outcomes and retries are separate fields.

Current RSS is sampled from owned child processes at named phases, not max-RSS from an unrelated process or a logical allocation estimate. External endpoint RSS is unavailable. Compare backlog RSS together with payload bytes, cache size and throughput: lower RSS can come with worse small-body overhead, lower strict-write throughput or cold-read cost.

JSON/CSV include environment, versions, settings, command lines, per-phase counts/latency, RSS, cleanup errors and case/run failures. Inspect the report's `ok` and errors, not only a selected fast row. A missing engine, failed case or unknown command outcome must remain visible; do not discard failures to improve a headline.

## External Valkey/Dragonfly or Redis endpoints

`--external` accepts `redis=HOST:PORT`, `valkey=HOST:PORT` or `dragonfly=HOST:PORT`, with `--external-durability memory|strict|everysec` describing the intended policy. The harness does **not** reconfigure external persistence, and labels these results external/unverified. Verify their actual policy yourself and report the verification evidence. A `redis_version` signature does not uniquely identify every compatible engine.

This narrow benchmark client does not supply the protected native CLI's complete TLS/ACL interface. Do not weaken production security to accommodate it; use disposable owned/reference endpoints. Do not claim an external engine was tested unless the actual report contains a completed case for it.

## Fault/soak workflow

```sh
python3 tests/soak_smoke.py --binary target/release/ffkv \
  --duration 60 --json soak.json
```

This is a **finite** workload with owned data/processes. It exercises concurrent queue operations, SIGKILL/recovery cycles, checkpoint/compaction, stale receipts, idempotency and restoration into a real new process. It is not seven-day endurance, disk-loss simulation or proof of honest device caches. Fsync under real power loss is a different experiment from killing a process.

## Existing evidence and honest claims

Repository artifacts are `benchmarks/results/macos-arm64.json` and `benchmarks/results/soak-macos-arm64.json`; the README records their original environment and selected rows. They are historical single-workstation evidence, not checks rerun by this site. Read the full report before quoting any result and include engine/version, OS/architecture, concurrency/pipeline, payload/backlog, storage/durability, cache and latency semantics.

The published baseline demonstrates a memory/throughput tradeoff, including worse small-body overhead and strict-write throughput below Redis in that run. It does not establish a universal speed advantage, an 80% throughput target, performance-per-dollar, million-job/beyond-RAM capability, Valkey/Dragonfly results or production HA. Those require their own executed workloads and evidence. Keep [behavioral tests](/contributing/testing/) and performance measurements separate: a fast incorrect or unverified case is not a useful benchmark.

## MCP transport overhead

`benchmarks/mcp.py` compares persistent native RESP and persistent MCP stdio
sessions against the same disposable strict-durability ffkv server:

```sh
cargo build --release --bins
python3 benchmarks/mcp.py \
  --binary target/release/ffkv --mcp-binary target/release/ffkv-mcp \
  --rounds 100 --batch-sizes 1 16 128 --output mcp-benchmark.json
```

PING and 64-byte SET/GET workloads alternate transport order after warmup.
SET/GET uses two commands per workload item. Both paths decode complete replies;
MCP includes JSON request encoding while direct RESP requests are pre-encoded.
The direct socket drains replies concurrently with a reused writer thread, so
large bidirectional batches do not deadlock. Worker scheduling is included in
direct timings; no thread is created per measured round. Assertions run outside
the timed interval. Results include completed command counts, commands/second,
median and p95 round-trip time, and amortized command cost. The amortized figure
is **not individual-command latency**.

Batching amortizes JSON-RPC and process-boundary overhead. This local sequential
microbenchmark does not measure concurrent clients, remote networks, agent model
latency, or production durability hardware. See [MCP](/guides/mcp/) for access
semantics and limits; do not substitute speed measurements for behavioral tests.

The large-payload regression that previously stalled the direct reference path:

```sh
python3 benchmarks/mcp.py \
  --binary target/release/ffkv --mcp-binary target/release/ffkv-mcp \
  --rounds 1 --warmup 1 --batch-sizes 12 --value-bytes 1048576
```

This is a correctness regression, not the published small-value baseline.
Large MCP results require artifact paging; that retrieval time remains included.

### Observed MCP baseline

`benchmarks/results/mcp-macos-arm64.json` records the executed release-binary
baseline on macOS 27.0.1 arm64 with Python 3.12.10: 100 measured rounds per
case after ten excluded warmup rounds, one sequential client, persistent
connections, 64-byte values, and strict durability on the same owned server.
Each SET/GET item contributes two commands. The direct path uses the duplex writer
described above.

| Workload | Commands per round | MCP commands/s | Direct RESP commands/s | MCP median round (ms) | Direct median round (ms) |
| --- | ---: | ---: | ---: | ---: | ---: |
| PING | 1 | 2,050 | 4,451 | 0.265 | 0.154 |
| PING | 16 | 11,508 | 13,028 | 1.310 | 1.043 |
| PING | 128 | 15,320 | 19,753 | 6.922 | 5.986 |
| SET/GET | 2 | 301 | 318 | 5.786 | 5.196 |
| SET/GET | 32 | 403 | 397 | 74.213 | 73.240 |
| SET/GET | 256 | 534 | 548 | 469.901 | 470.471 |

The full report retains elapsed time, exact command counts and p95 values.
Strict writes synchronize before acknowledgement; pipelining did not turn this
workload into one shared fsync. The observed small write-throughput difference
does not imply zero MCP overhead, and PING throughput is not write throughput.
These are workstation observations, not latency or throughput guarantees.
