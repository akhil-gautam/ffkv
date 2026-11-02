---
title: Benchmarks and fault/soak measurements
description: Reproduce native Redis comparisons and list-backlog workloads, interpret RSS and latency evidence, and avoid unsupported performance or durability claims.
---

## Native Redis versus ffkv comparison

`benchmarks/redis_compare.py` uses the same compiled `redis-benchmark` executable against both engines, rather than timing Python request generation. Install/provide `redis-server` and `redis-benchmark`, then run from the repository root:

```sh
cargo build --release --bin ffkv
python3 benchmarks/redis_compare.py --repetitions 3 --target-seconds 3 \
  --json redis-comparison.json
```

The default matrix runs 198 measured trials: nine memory-only workloads, three load profiles, two engines and three repetitions, plus 256-byte SET under everysec and strict durability. Profiles are one client/pipeline one/native thread one, 32 clients/pipeline one/four native threads, and 32 clients/pipeline 16/four native threads.

GET and SET use 10,000 populated keys with 256-byte or 4-KiB values; INCR uses 10,000 counters; HGET/HSET use 1,000 hashes with eight 256-byte fields; ZADD/ZRANGE use 1,000 sorted sets with 64 members. Writes vary payload suffixes or the existing member's score instead of benchmarking only identical-value no-ops. ZRANGE returns ten members. These are isolated operation workloads, not a mixed read/write application trace.

Every trial owns a fresh server and temporary directory. All preload replies are checked, sampled values/types/cardinalities are verified before and after measurement, and INCR additionally verifies the exact full-dataset counter delta. Read keys are populated: a missing-key GET is not counted as a payload-read benchmark. A fixed native seed is recorded, although multithreaded scheduling does not guarantee identical key interleaving.

Warmup and iterative calibration are excluded. Calibration chooses a bounded request count to approximate the requested measurement duration; actual duration and short-run flags remain in JSON. Engine order reverses on alternate repetitions. The report retains every failure, native stdout/stderr, exact commands, effective persistence settings, current RSS and server/client CPU observations. ffkv uses four shards and the memory backend; durable cases add its WAL. Automatic ffkv checkpoints and Redis AOF rewrites are disabled for the steady-state comparison. Checkpoint cost, hybrid storage, recovery and power-loss behavior are separate experiments.

**Latency semantics matter:** [Redis 8.2.1's native client](https://github.com/redis/redis/blob/8.2.1/src/redis-benchmark.c) measures send-to-first-socket-read-event time before parsing replies. It repeats that sample across pipeline replies and clamps its histogram at three seconds. These native p50/p95/p99 values are not pipeline completion times or individually measured reply latencies; never divide them by pipeline depth. Threaded throughput completion detection has a 250-ms timer, making short pilots unsuitable for capacity claims. The list harness below measures different latency semantics.

Redis AOF `always` and ffkv `strict` both wait for synchronization before acknowledging the relevant write, but their batching and commit implementations differ. Do not infer equal fsync counts or power-loss guarantees from matching policy names. Treat same-machine loopback results as workload-specific measurements, not production capacity or remote-network results.

Use `--help` for workload/profile/mode filters, request bounds and timeouts. A quick correctness smoke is deliberately not a performance result:

```sh
python3 benchmarks/redis_compare.py --repetitions 1 --target-seconds 0.25 \
  --profiles c32-p16-t4 --max-requests 8192 --json native-smoke.json
```

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

`benchmarks/compare.py` measures real LPUSH/LINDEX/LPOP list workloads against owned ffkv/Redis processes. It compares list remove-on-pop semantics, **not native lease/ACK semantics**. `benchmarks/runtime.py` supplies the standard-library RESP client, process ownership, histogram and current-RSS measurement. No performance numbers are generated by this documentation.

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

Do not point external options at a production dataset. Even though external cases use a unique `ffkv:benchmark:*` namespace and are not reconfigured, they still create/read/drain real lists and consume resources.

## Workload and tunable controls

The default matrix includes memory/strict/everysec durability with 256 B, 4 KiB and 64 KiB bodies. ffkv hybrid cases require persistence, so volatile mode does not invent a hybrid-without-WAL case. Owned persistence settings are aligned/verified; this matters more than comparing unrelated defaults.

| Option | Current default / use |
| --- | --- |
| `--binary`, `--redis-binary` | target/release/ffkv, redis-server |
| `--skip-redis` | Explicitly omit Redis |
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
