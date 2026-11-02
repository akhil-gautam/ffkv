---
title: Count-min sketch
description: Estimate item frequencies with bounded counter rows and dimension-compatible weighted merges.
---

A count-min sketch hashes each item into several rows of counters and estimates frequency from those counters. It does not retain every item identity. With nonnegative updates, collisions can inflate estimates; a queried item with a positive estimate is not proof it was observed. This is not an exact hash of counters.

## Initialize and update

```sh
target/release/ffkv-cli -p 6382 CMS.INITBYDIM docs:cms 128 4
target/release/ffkv-cli -p 6382 CMS.INCRBY docs:cms alice 3 bob 1
target/release/ffkv-cli -p 6382 CMS.QUERY docs:cms alice bob carol
target/release/ffkv-cli -3 --json -p 6382 CMS.INFO docs:cms
```

INITBYDIM chooses width and depth explicitly. INITBYPROB instead derives dimensions from an error parameter and failure probability; neither parameter means that every answer has a fixed absolute error. INCRBY returns current estimates for updated items. Querying a missing sketch is an error, unlike querying an unseen item in an initialized sketch.

The implementation accepts signed updates subject to counter/total underflow and overflow checks. The usual nonnegative-stream overestimate interpretation does **not** automatically apply after arbitrary negative updates or weighted subtraction. Do not treat subtraction as exact removal of a specific item's history.

## Merge compatible sketches

```sh
target/release/ffkv-cli -p 6382 CMS.INITBYDIM docs:cms:other 128 4
target/release/ffkv-cli -p 6382 CMS.INCRBY docs:cms:other alice 2
target/release/ffkv-cli -p 6382 CMS.INITBYDIM docs:cms:merged 128 4
target/release/ffkv-cli -p 6382 CMS.MERGE docs:cms:merged 2 docs:cms docs:cms:other WEIGHTS 1 1
target/release/ffkv-cli -p 6382 CMS.QUERY docs:cms:merged alice
```

MERGE combines source counter rows using integer weights and replaces the destination state. Width, depth, and cell size must match; the number of weights must match source count. It is not valid to merge incompatible sketches and assume the system will resample them. Counter/total underflow or overflow rejects the result.

## Bounds and durability

Width×depth may contain at most **8,388,608 cells**. Optional CELL_SIZE is one of 1, 2, 4, or 8 bytes and affects counter capacity. Initialization requires a missing key; malformed sizes, incompatible types, and numeric limits produce errors. Larger tables trade RAM for reduced collisions, not exactness.

Counters remain RAM-resident in hybrid mode. With persistence enabled, resolved counter changes/merges recover; no data directory means no restart survival. A lost INCRBY reply may already have changed counters, so blind replay inflates frequency further.

Use [hashes](/data-types/hashes/) for exact named counters, [Top-K](/data-types/top-k/) for retained heavy hitters, or [CMS.INITBYPROB](/reference/extensions/#cmsinitbyprob), [CMS.MERGE](/reference/extensions/#cmsmerge), and [persistence](/operations/persistence/) for details.
