---
title: Hybrid RAM/SSD storage
description: Offload large queue bodies, size cache and disk headroom, and distinguish compaction from WAL checkpointing.
---

Hybrid mode offloads **large list elements and native-job bodies** into checksummed disk segments. It is not a general disk backend for the full dataset: keys, indexes, small bodies, strings, hashes, set/sorted-set member identities and newer typed structures remain in RAM.

## Start and inspect

Use a fresh, separate directory:

```sh
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-hybrid --durability strict --storage hybrid \
  --inline-threshold 1024 --cache-bytes 67108864 \
  --segment-bytes 67108864 --disk-budget 0
```

In another terminal:

```sh
target/release/ffkv-cli -3 -p 6382 FFKV.STATS
target/release/ffkv-cli -3 -p 6382 FFKV.COMPACT
```

`--storage hybrid` requires `--data-dir`. Payloads larger than `--inline-threshold` are eligible for offloading. `--cache-bytes` bounds cached body bytes, **not** process RSS, key metadata, indexes or the entire dataset. `--segment-bytes` controls segment rotation. Inspect live payload, physical segment, cache hit/miss and reclaimed-byte counters rather than assuming logical and physical bytes match.

## Read and compaction behavior

Dedicated blocking owner threads perform cold payload I/O instead of blocking Tokio network workers. Readers pin old segment files while using them; references remain valid across compaction. Idle maintenance also reclaims retired files. Cold reads still incur disk latency, so measure your real body-size distribution and hot set.

`FFKV.COMPACT` reclaims derived segment storage. A pre-cutover failure leaves old live locations usable. Live payload I/O failure is fail-stop, not a successful missing-value response. Compaction is different from `FFKV.CHECKPOINT`, which rewrites the logical WAL history. Both can require substantial temporary headroom and delay coordinated operations.

## Recovery and disk budgeting

The WAL/checkpoint stores logical body bytes, never physical segment pointers. On startup, after acquiring the database lock, ffkv rebuilds a fresh segment generation. Recognized obsolete generations are removed only after successful recovery; unknown files are not deleted. Plan for old and rebuilt payload data to coexist.

`--disk-budget` defaults to zero (unlimited). A nonzero budget covers **derived segments plus rebuild/compaction scratch**. It does not cap:

- the WAL or replacement checkpoints;
- immutable backups;
- replica staging and downloaded images;
- unrelated files or total filesystem usage;
- RAM or process RSS.

Keep filesystem free-space alerts independent of the derived-segment budget. Recovery can fail for insufficient rebuild headroom even if the old live dataset fit. Do not delete arbitrary segment files to free space; preserve originals and use a [verified backup restore](/operations/backup-restore/) when recovering damaged storage.

## Choosing a profile

Use `memory` when your dataset and bodies fit RAM and hot-read latency matters most. Use `hybrid` for list/native-job workloads whose body bytes dominate memory. A vector graph or large JSON document does not become SSD-resident merely because hybrid is enabled. Both profiles share logical durability and support the datatype families, TTLs, WATCH, checkpoints and backups.

Use the [benchmark harness](/contributing/benchmarks/) to measure cache-miss reads, enqueue/drain rates and current RSS under comparable durability. The existing single-workstation evidence is a memory/throughput tradeoff, not a universal performance claim.
