---
title: Top-K
description: Track approximate heavy hitters with HeavyKeeper counters and a bounded retained-member heap.
---

Top-K tracks candidate heavy hitters using **HeavyKeeper** counters plus a retained-member min-heap. It is a bounded approximation, not an exact sorted set of every item's count. Items can enter and leave the retained set as the workload changes.

## Retain frequent candidates

```sh
target/release/ffkv-cli -p 6382 TOPK.RESERVE docs:topk 2
target/release/ffkv-cli -p 6382 TOPK.INCRBY docs:topk alice 20 bob 1
target/release/ffkv-cli -p 6382 TOPK.ADD docs:topk carol
target/release/ffkv-cli -p 6382 TOPK.QUERY docs:topk alice bob carol
target/release/ffkv-cli -p 6382 TOPK.COUNT docs:topk alice bob carol
target/release/ffkv-cli -p 6382 TOPK.LIST docs:topk WITHCOUNT
target/release/ffkv-cli -3 --json -p 6382 TOPK.INFO docs:topk
```

RESERVE chooses K, the number of retained candidates. ADD increments by one; INCRBY uses item/increment pairs. Update replies identify evicted items where applicable, not a full current ranking. QUERY asks whether an item is currently retained. COUNT returns estimates, not the authoritative lifetime count of each original byte string.

Do not promise which low-frequency item is retained in this small example. Collisions, probabilistic decay, and tie/order effects are part of the model. LIST WITHCOUNT is useful for dashboards or candidate selection; confirm exact winners against authoritative data if a prize, bill, or irreversible action depends on it.

## Sizing and errors

Optional RESERVE parameters select width, depth, and decay. Wider/deeper counters trade RAM for approximation quality; decay controls collision-counter behavior. Valid decay is greater than zero and at most one. Width×depth is bounded to **8,388,608 cells**. Initialization requires a missing key.

INCRBY increments must be between **0 and 100,000** per item. Invalid sizes, counter overflow, incompatible key types, and request/reply limits are errors. Missing Top-K keys are errors for query/update operations rather than automatically initialized trackers. Inspect INFO to confirm the actual construction settings.

## Recovery is not resampling

With persistence enabled, ffkv records resolved counters/heap slots and random outcomes, so recovery does not randomly rerun the HeavyKeeper decisions. That makes recovery stable; it does not make the algorithm exact. Without a data directory, state is volatile. Hybrid mode keeps this family's structures in RAM.

For an exact leaderboard with known member identities use [sorted sets](/data-types/sorted-sets/). For approximate frequency queries without a retained candidate list use [count-min sketch](/data-types/count-min-sketch/). See [TOPK.RESERVE](/reference/extensions/#topkreserve), [TOPK.LIST](/reference/extensions/#topklist), and [persistence](/operations/persistence/).
