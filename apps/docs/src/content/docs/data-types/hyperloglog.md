---
title: HyperLogLog
description: Estimate distinct cardinality and unions without retaining every member.
---

HyperLogLog estimates the number of distinct binary elements using compact registers. It does **not** retain an enumerable set, answer membership, or provide an exact unique count. Use it for audience/visitor telemetry where estimation is acceptable, not billing or authorization.

## Count distinct observations

```sh
target/release/ffkv-cli -p 6382 PFADD docs:hll:day1 alice bob alice
target/release/ffkv-cli -p 6382 PFCOUNT docs:hll:day1
target/release/ffkv-cli -p 6382 PFADD docs:hll:day2 bob carol
target/release/ffkv-cli -p 6382 PFCOUNT docs:hll:day1 docs:hll:day2
target/release/ffkv-cli -p 6382 PFMERGE docs:hll:week docs:hll:day1 docs:hll:day2
target/release/ffkv-cli -p 6382 PFCOUNT docs:hll:week
```

PFADD reports whether registers changed, not the number of newly unique values. Repeated elements do not necessarily change registers; new elements may also leave them unchanged. Tiny examples may yield the intuitive count, but do not extrapolate exactness from that. Multi-key PFCOUNT estimates the union, not the sum of per-key estimates. PFMERGE combines register state into the destination and supports destination/source overlap.

## Encoding and cache

ffkv implements Redis-compatible **HYLL** sparse/dense registers. The backing value is a string; ordinary string mutation can corrupt it. Invalid headers/sparse runs are rejected by PF commands instead of being interpreted as an empty sketch. Diagnostic PFDEBUG/PFSELFTEST commands expose representation/estimator behavior, not proof that a production estimate equals a true count.

PFCOUNT may refresh a cached count. Register updates invalidate the cache; replay preserves the resolved sketch rather than rehashing a replayed user operation. Existing key expiration is retained by sketch merge/cache behavior covered in the repository's recovery scenarios.

## Accuracy, storage, and limits

Space is bounded by sketch representation rather than distinct-member count, but request/reply/value budgets still apply. There is no member deletion: to remove an observation's influence, build a new sketch from authoritative data or expire a time-window key. A union cannot reconstruct intersections or original identities exactly.

Sketch bytes remain RAM-resident in hybrid mode. All families support key TTL, WATCH, ACLs, checkpoints, and backups, but restart survival still requires a data directory and a suitable durability policy. A copied HYLL value is not a migration guarantee for unrelated ffkv persistence formats.

For exact uniqueness use [sets](/data-types/sets/); for membership prefiltering use [Bloom filters](/data-types/bloom/). See [PFADD](/reference/commands/pfadd/), [PFCOUNT](/reference/commands/pfcount/), [PFMERGE](/reference/commands/pfmerge/), and [compatibility](/concepts/compatibility/).
