---
title: Sorted sets
description: Build score-ordered indexes, leaderboards, and schedules with unique members.
---

A sorted set associates each unique binary member with a numeric score. Members are ordered by score, with member bytes breaking score ties. Adding an existing member updates its score rather than creating a duplicate. Use it for leaderboards, ordered indexes, or application-managed schedules.

## Rank and score queries

```sh
target/release/ffkv-cli -p 6382 ZADD docs:zset:board 100 alice 200 bob 150 carol
target/release/ffkv-cli -p 6382 ZRANGE docs:zset:board 0 -1 WITHSCORES
target/release/ffkv-cli -p 6382 ZRANGE docs:zset:board 0 1 REV WITHSCORES
target/release/ffkv-cli -p 6382 ZINCRBY docs:zset:board 25 alice
target/release/ffkv-cli -p 6382 ZSCORE docs:zset:board alice
target/release/ffkv-cli -p 6382 ZRANK docs:zset:board bob
target/release/ffkv-cli -p 6382 ZRANGE docs:zset:board 100 200 BYSCORE WITHSCORES
```

Rank ranges are inclusive and support negative indexes. Score bounds support infinities and exclusive forms such as `'(100'` (quote shell metacharacters). ZRANGE supports BYSCORE, REV, LIMIT, and WITHSCORES. Lexicographic range semantics are useful when members share a score; they are not a replacement for arbitrary text search.

ZADD supports NX/XX, GT/LT, CH, and INCR subject to valid option combinations. Distinguish an added-member count from a changed-member count when CH is used. Scores are numeric, not exact arbitrary-precision currency; use integer-scaled values only within the supported floating-point precision range.

## Remove, sample, or combine

```sh
target/release/ffkv-cli -p 6382 ZSCAN docs:zset:board 0 COUNT 10
target/release/ffkv-cli -p 6382 ZPOPMIN docs:zset:board 1
target/release/ffkv-cli -p 6382 ZCARD docs:zset:board
```

ZPOPMIN/ZPOPMAX remove members immediately. They do not grant leases or protect external work from worker crashes. ZREM and rank/score-range removal are destructive too. Algebra and STORE forms combine indexes; inspect their aggregation/weight options in the command reference before using results as rankings.

ZSCAN has an opaque stable hash-order cursor, not sorted-rank pagination or a snapshot. Use ZRANGE for ordered reads and bounded pages rather than fetching an entire large collection.

## Storage and errors

Commands reject incompatible types and invalid numeric/range syntax. Hybrid mode retains member identities and sorted-set indexes in RAM. With persistence, resolved removals/score changes survive recovery; without it all data is volatile. A lost pop reply leaves an unknown removal outcome.

[Geospatial indexes](/data-types/geospatial/) reuse sorted sets but give scores an encoded coordinate meaning; arbitrary ZADD updates can destroy that meaning. See [ZADD](/reference/commands/zadd/), [ZRANGE](/reference/commands/zrange/), [native jobs](/guides/native-jobs/), and [limits](/reference/limits/).
