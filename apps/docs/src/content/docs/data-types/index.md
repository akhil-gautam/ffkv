---
title: Data types
description: Choose among ffkv's nineteen datatype families and distinguish exact collections from estimates.
---

ffkv's current taxonomy contains **19 families**, counting bitmaps and bitfields separately even though both operate on string bytes. Native `FQ.*` queues are an ffkv extension beyond that catalog. Search is a separate indexing capability, not an extra family.

## Choose a model

| Family | Use it for | Result model |
| --- | --- | --- |
| [Strings](/data-types/strings/) | Bytes, counters, expiring values | Exact bytes/integer arithmetic |
| [Lists](/data-types/lists/) | Ordered payloads and remove-on-pop queues | Exact order |
| [Sets](/data-types/sets/) | Membership and algebra | Exact unique members |
| [Hashes](/data-types/hashes/) | Field/value records and field TTL | Exact fields |
| [Sorted sets](/data-types/sorted-sets/) | Leaderboards, ordered schedules | Exact member/score index |
| [Bitmaps](/data-types/bitmaps/) | Dense boolean flags | Exact addressed bits |
| [Bitfields](/data-types/bitfields/) | Packed fixed-width integers | Exact within chosen overflow policy |
| [Geospatial](/data-types/geospatial/) | Point proximity and regions | Encoded coordinates and computed distance |
| [HyperLogLog](/data-types/hyperloglog/) | Unique cardinality with bounded space | Estimate, no member enumeration |
| [Streams](/data-types/streams/) | Append history and consumer groups | Ordered entries/pending ownership |
| [JSON](/data-types/json/) | Path-addressable document trees | Exact document operations |
| [Arrays](/data-types/arrays/) | Sparse unsigned-index byte strings | Exact slots, holes, and cursor |
| [Time series](/data-types/time-series/) | Timestamped samples and aggregation | Samples with floating-point aggregates |
| [Vector sets](/data-types/vector-sets/) | Embedding similarity | Approximate HNSW search; explicit exhaustive mode |
| [Bloom filters](/data-types/bloom/) | Compact membership prefilters | Possible false positives |
| [Cuckoo filters](/data-types/cuckoo/) | Fingerprint membership/counting/deletion | Probabilistic membership/counts |
| [Count-min sketch](/data-types/count-min-sketch/) | Frequency estimates | Collision-biased estimates |
| [Top-K](/data-types/top-k/) | Heavy hitters | Approximate ranked candidates |
| [t-digest](/data-types/t-digest/) | Quantiles/CDF over measurements | Approximate distribution |

Prefer an exact set/hash/counter when an incorrect answer changes authorization, money, or irreversible work. Probabilistic structures are useful for ranking, telemetry, and prefiltering with an authoritative fallback; they are not interchangeable with exact collections.

## Shared semantics

Examples use `target/release/ffkv-cli -p 6382` against the [quickstart](/getting-started/quickstart/) instance. Use a fresh dataset; commands mutate namespaced example keys. A command on an incompatible existing type generally returns WRONGTYPE; this is different from a missing key. Some families share physical representations: bitmaps/bitfields/HyperLogLog use strings, and geo uses sorted sets. Avoid unrelated commands that corrupt their encoded meaning.

All nineteen families participate in key TTLs, WATCH, ACLs, WAL recovery, checkpoints, and backups in both storage profiles. **No data directory means no restart durability.** Hybrid offloads only large list and native-job bodies: other families stay in RAM. Request/reply limits apply independently of family limits. See [persistence](/operations/persistence/), [limits](/reference/limits/), and [compatibility](/concepts/compatibility/).

A SCAN-family cursor is opaque and not a snapshot; COUNT is a hint. ARSCAN is a different index/value range API. Prefer bounded reads to whole-collection commands for large data.

For workflows rather than individual types, see [list queues](/guides/list-queues/), [native leased jobs](/guides/native-jobs/), [transactions](/guides/transactions/), and [search indexes](/guides/search/).
