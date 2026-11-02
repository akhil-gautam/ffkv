---
title: Memory and capacity planning
description: Distinguish RSS admission from eviction, payload-cache budgets and logical memory estimates.
---

ffkv has **no eviction policy and no hard dataset/RSS cap**. Memory-related settings govern different resources; none substitutes for OS/container limits and enough recovery headroom.

## The budgets are not interchangeable

| Control/measurement | Meaning | Does not bound |
| --- | --- | --- |
| `--cache-bytes` | Cached hybrid list/native-job body bytes | Metadata, RAM-resident types, process RSS |
| `--max-rss-bytes` | Soft admission breaker using sampled current process RSS | Peak RSS, recovery, already accepted work, maintenance |
| `--disk-budget` | Derived hybrid segments plus rebuild/compaction scratch | WAL, backups, replica staging, RAM |
| `MEMORY USAGE`/memory diagnostics | Owned-allocation estimates | Redis/jemalloc layout, exact RSS, module-private heap |
| Request/reply limits | Per-operation ingress/logical-response safety | Total resident dataset/process memory |
| Container memory limit | External cgroup/process boundary | Graceful eviction or guaranteed pre-OOM rejection |

Hybrid offloads large list/native-job bodies only. JSON documents, vectors, sketches, keys and indexes remain in RAM. Native modules can allocate private memory outside dataset estimates.

## Enable the RSS circuit breaker

On a fresh isolated directory:

```sh
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-memory --durability strict \
  --max-rss-bytes 536870912
```

Zero (the default) disables the breaker. With a nonzero limit, the one-second RSS sample at or above the threshold marks the server pressured. Growing commands, including EXEC entries and scripts, are rejected with OOM **before execution**. Reads, deletions, collection removals/pops, native ACK/CANCEL and maintenance remain usable. A replica's READONLY rejection takes precedence over pressure.

The breaker does not reserve bytes per accepted command. Concurrent operations, accepted requests, startup recovery and internal maintenance can overshoot. An unknown RSS sample does not establish pressure; do not mistake a missing sample for zero memory use. Place the soft threshold below the OS/container hard limit with room for the workload's largest transient operations; choose that margin from measurements, not a universal percentage.

## Observe before tuning

```sh
target/release/ffkv-cli -3 -p 6382 INFO memory
target/release/ffkv-cli -3 -p 6382 MEMORY STATS
target/release/ffkv-cli -3 -p 6382 FFKV.STATS
```

INFO, Prometheus, overview and storage limits expose the actual RSS sample, configured soft threshold and pressure state. Record these alongside key/job counts, live payload bytes, cache bytes, WAL/segment size and read/write rates. A cache miss is not proof of pressure, and allocator estimates need not equal OS RSS.

## Capacity and incident workflow

1. Measure backlog and hot-set behavior for your body-size distribution with both storage profiles and the intended durability.
2. Include RAM-resident indexes/types, connection state, blocking waiters, transaction/script buffers and response allocations in planning.
3. Reserve disk for current/replacement WAL, old/rebuilt hybrid generations, backups and replication staging. The segment budget is not a free-space monitor.
4. If pressured, reduce producers and inspect the sample. Reclaim intentionally using application-safe deletions/draining/ACKs; do not flush data just to clear an alarm.
5. Do not immediately retry a transport failure: a mutation may already have committed. Distinguish explicit pre-execution OOM from an unknown outcome.
6. Before raising limits, establish whether growth is legitimate retained data, an enlarged hot cache, temporary maintenance work or an application leak.

The [benchmark guide](/contributing/benchmarks/) describes reproducible RSS/backlog measurements. See [concrete bounds](/reference/limits/) for per-request, transaction, scripting and datatype limits.
