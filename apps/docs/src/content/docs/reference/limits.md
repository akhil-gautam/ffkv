---
title: Limits and compatibility boundaries
description: Concrete protocol, response, queue, scripting, datatype and management bounds and what they do not guarantee.
---

These are implementation safety bounds, **not** recommended operating targets or a total process-memory budget. A command can hit request framing, logical reply or management JSON/base64 limits before a datatype's theoretical maximum. Catalog coverage is not exhaustive Redis option/binary parity. Check the running `COMMAND INFO` and each [command page](/reference/commands/) for command-specific syntax.

## Network, execution and databases

| Resource | Bound / behavior |
| --- | --- |
| Canonical RESP request | 8 MiB **including framing**, 1,024 arguments including command |
| Read-ahead pipeline data while awaiting reply | At most 8 MiB per TCP connection, then backpressure; queued commands are not executed early |
| Default connections | 1,024; configurable positive u32 maximum; excess connections close |
| Internal shard actors | 1–256; default 4 |
| Standalone logical databases | 1–65,536 configured; default 16 |
| Cluster logical databases | Database zero only |
| Cluster hash slots | 16,384, numbered 0–16,383 |
| Native client reply decoding | 64 MiB wire/logical allocation budgets, one million values, depth 64 |
| Aggregate engine/native reply budget | 64 MiB logical; management origin uses 8 MiB |
| Inline protocol | Space/tab separated with CRLF; no inline quoting/escaping; use RESP bulk strings for arbitrary bytes |
| RESP3 streamed/unknown-length frame syntax | Not supported by native client |

Malformed/oversized requests receive an error and the connection closes. Human client output escapes terminal controls; `--raw` deliberately emits bytes. Destructive count-pops/claims reject oversized logical results **before removing data/granting leases**, but final management delivery has an additional post-execution boundary. Long coordinated operations/checkpoints can still delay other clients.

## Transactions, Lua and functions

| Resource | Bound |
| --- | --- |
| MULTI queue | 1,024 commands, 16 MiB argument bytes |
| WATCH | 1,024 keys per transaction client |
| Lua instructions | 1,000,000 per invocation |
| Lua memory | 32 MiB |
| Lua nesting | 64 levels |
| Script / bridge value / reply size | 8 MiB |
| Lua bridge calls / cumulative argument bytes | 10,000 / 16 MiB |
| SHA script cache | 256 scripts / 16 MiB, volatile FIFO eviction; handle NOSCRIPT |
| Named function libraries | At most 256; encoded catalog must fit one 16 MiB WAL frame |

These are not a dataset/RSS cap. Runtime Lua/EXEC errors do not roll back earlier writes. Blocking list operations inside transactions/scripts do not wait; `TS.READ BLOCK` is rejected there. Native modules are a trusted API-v1 subset, **not sandboxed**; module-private memory is outside dataset estimates. Module datatypes/persistence callbacks and blocking/thread/timer/event/shared API facilities are not implemented.

## Native leased jobs and Pub/Sub

| Resource | Bound / default |
| --- | --- |
| FQ claim/page rows | At most 1,000 |
| Jobs and idempotency records | Up to one million each per queue |
| One CLAIM's logical bodies | 32 MiB; origin reply limits can be lower |
| Visibility/extension | Default claim visibility 30 seconds; maximum 24 hours |
| Idempotency retention | Default five minutes; maximum 30 days |
| Default job max attempts | 25 |
| Queue concurrency | Per queue; zero unlimited |
| Pub/Sub registrations | 1,024 channels/patterns and 8 MiB retained names per client |
| Pub/Sub pattern | 1,024 bytes |
| Subscriber output | 256 events and 16 MiB per client, including staged/in-flight data; overflow disconnects |

Native jobs are **at-least-once**, not exactly-once external side effects. Lists remove on pop with no ACK/redelivery. Pub/Sub is ephemeral; shared pattern-matching work is bounded and an excessive publication can be rejected before delivery.

## Datatype bounds

| Type / resource | Concrete bounds and caveats |
| --- | --- |
| String / bitmap stored value | 8 MiB; encoded request framing may reject input first |
| JSON | 4 MiB compact document, 262,144 nodes, depth 128, 16 KiB paths, 65,536 matches; expression/projection work also bounded |
| Sparse arrays | Indices through `2^64-2`; ARSEEK also accepts terminal `2^64-1`; dense range reads at most one million positions |
| Bloom / Cuckoo | 64 MiB backing arrays per key; bounded expansion/relocation |
| CMS / HeavyKeeper | At most 8,388,608 cells |
| t-digest | Compression at most 1,000,000 |
| Vector sets | 65,536 dimensions, 64 KiB member names, 1 MiB attributes, 1 GiB estimated per-set allocation; M/EF/search work separately bounded |
| Stream waiters | 65,536 clients and 64 MiB logical retained requests, further limited by configured connection cap |
| Stream approximate MAXLEN trimming | 100-entry granularity |

Sparse recent array walks have a separate work bound; ARSCAN is index/value output, not a SCAN cursor. Bloom/Cuckoo SCANDUMP/LOADCHUNK uses ffkv's versioned checksummed **FFKF** format, not RedisBloom's packed-memory binary dumps. Approximate sketches are not exact counters/sets. Vector TRUTH explicitly requests exhaustive search; ordinary searches use HNSW. Typed graphs/documents/sketches remain RAM-resident in hybrid mode.

## Management API and security policy

| Resource | Bound |
| --- | --- |
| Decoded command | 8 MiB canonical framing, 1,024 arguments |
| HTTP body | 12 MiB to accommodate base64/JSON overhead; not a larger decoded-command allowance |
| Final JSON response / logical engine origin budget | 8 MiB each, separate checks |
| Page request count | 1–200, default 100; SCAN COUNT is a hint and oversized pages are rejected, not silently truncated |
| Key/capability decode helpers | 4,096 decoded bytes, 8,192 encoded-input bytes |
| Scan match pattern | 1,024 bytes |
| List browse offset | At most 1,000,000,000 |
| URI target | 16,384 bytes |
| HTTP connections / active work / audited operations | 128 / 64 / 32 |
| API request rate | 100 requests in the implementation's one-second window |
| Ordinary command bus / outer HTTP deadline | 10 / 30 seconds |
| Websockets | 16; 32 KiB frames/messages; first auth within 2 seconds; session at most 300 seconds |
| Management tokens | At most 128; policy file at most 64 KiB; token input at most 16 KiB |
| Management DTO recursion | 32 levels |
| ACL file / line | 1 MiB / 64 KiB |
| ACL users / rules per user / total rules | 1,024 / 1,024 / 16,384 |
| ACL patterns per user / total / pattern bytes | 128 / 4,096 / 1,024 |
| ACL passwords per user / password bytes / username bytes | 32 / 1,024 / 128 |

These management bounds are fixed code limits, not CLI tuning knobs. Masking recognized JSON secrets is not DLP. A completed mutation can fail final response delivery; never blindly retry destructive operations after unknown/delivery failures. See [management API](/reference/management-api/) and [security](/operations/security/).

## Persistence, storage and replication

- WAL data frames are bounded to 16 MiB; large atomic mutations use grouped frames and commit only as a complete group.
- `FFKV.BACKUP` names are 1–64 ASCII alphanumeric/underscore/hyphen bytes; immutable existing destinations are refused. Restore requires a **new** destination.
- Automatic checkpoint threshold defaults to 256 MiB growth beyond the prior image; zero disables automatic checkpointing.
- Hybrid cache defaults to 64 MiB body bytes; it is not RSS. Derived-segment disk budget defaults to unlimited and excludes WAL/backups/replica staging.
- Soft RSS admission defaults off and samples once per second. No eviction or hard dataset/RSS cap exists; external limits and headroom remain required.
- Replication backlog defaults to 64 MiB, minimum 1 KiB. Snapshot transfer defaults to 1 GiB, configurable through 8,589,934,591 bytes. Full image replacement needs staging/install space.
- Cluster bus is custom plaintext shared-secret TCP, not Redis bus or RESP TLS. Non-loopback needs at least a 32-byte secret; full coverage defaults on and node timeout defaults to 15 seconds.

No hard per-process memory bound, power-loss/device-cache certification, zero-loss failover, complete Redis binary parity, seven-day soak or universal performance advantage is claimed. Consult [configuration](/reference/configuration/) for startup controls and [benchmarks](/contributing/benchmarks/) for the measured evidence scope.
