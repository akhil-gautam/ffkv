---
title: Time series
description: Store timestamped samples with labels, retention, duplicate policies, and live aggregation rules.
---

A time series stores timestamp/value samples with optional labels and retention. Labels identify series for multi-series queries; they are not per-sample tags. Values/aggregates use floating-point semantics, not exact decimal accounting.

## Create and query samples

```sh
target/release/ffkv-cli -p 6382 TS.CREATE docs:ts:temperature RETENTION 60000 DUPLICATE_POLICY LAST LABELS room office
target/release/ffkv-cli -p 6382 TS.ADD docs:ts:temperature 1000 21.5
target/release/ffkv-cli -p 6382 TS.ADD docs:ts:temperature 2000 22
target/release/ffkv-cli -p 6382 TS.ADD docs:ts:temperature 2000 22.5
target/release/ffkv-cli -p 6382 TS.GET docs:ts:temperature
target/release/ffkv-cli -p 6382 TS.RANGE docs:ts:temperature - +
target/release/ffkv-cli -p 6382 TS.QUERYINDEX room=office
```

Timestamps are milliseconds. LAST replaces the duplicate timestamp's sample; select duplicate policy deliberately rather than assuming appends always create a new sample. Retention is a sample-history policy relative to series timestamps, not a wall-clock key TTL. A separate EXPIRE can remove the whole series.

TS.MADD writes timestamp/value triples across named series. RANGE/REVRANGE support filters and aggregation; MRANGE/MREVRANGE use label filters. INCRBY/DECRBY update sample counters under their documented timestamp rules. TS.DEL removes an inclusive timestamp span.

## Live compaction

On separate fresh series:

```sh
target/release/ffkv-cli -p 6382 TS.CREATE docs:ts:raw
target/release/ffkv-cli -p 6382 TS.CREATE docs:ts:average
target/release/ffkv-cli -p 6382 TS.CREATERULE docs:ts:raw docs:ts:average AGGREGATION avg 100
target/release/ffkv-cli -p 6382 TS.MADD docs:ts:raw 20 2 docs:ts:raw 80 4 docs:ts:raw 150 8
target/release/ffkv-cli -p 6382 TS.RANGE docs:ts:average - +
target/release/ffkv-cli -p 6382 TS.GET docs:ts:average LATEST
```

The closed bucket at zero averages to three. LATEST exposes the open bucket at 100 with value eight. Deleting a rule stops future output; it is not equivalent to deleting already generated destination samples. Writes coordinate only connected source/destination keys; label queries require a coherent whole-dataset view.

Deleting all source samples in the latest compaction bucket reopens the previous closed bucket. Its destination sample disappears from ordinary RANGE/GET and becomes visible through LATEST until a later sample closes it again. Deleting all source samples resets the compaction cursor, allowing earlier timestamps to be inserted into the empty source. The reopened accumulator and cursor recover through WAL replay and checkpoints.

## Blocking, errors, and storage

TS.READ BLOCK uses bounded actor waiters, not polling. It is rejected inside EXEC/Lua, unlike stream reads that become nonblocking. Invalid duplicate/retention/options and incompatible types fail; inspect TS.INFO for actual chunk/label/rule state. Compression choices do not remove request/reply/work limits or RAM requirements.

Compressed/uncompressed chunks remain RAM-resident in hybrid mode. With persistence, resolved sample/rule effects recover; without a data directory they are volatile. Offline time still affects a key's TTL. Avoid blindly replaying deltas after a lost reply.

See [TS.ADD](/reference/commands/ts-add/), [TS.RANGE](/reference/commands/ts-range/), [TS.CREATERULE](/reference/commands/ts-createrule/), [t-digest](/data-types/t-digest/), and [persistence](/operations/persistence/).
