---
title: Compatibility and boundaries
description: Distinguish Redis command-catalog coverage from option, protocol, module, and operational parity.
---

ffkv implements all **19 datatype families** and the repository's **486-entry Redis 8.10 command catalog**. Those are concrete coverage targets, not a claim of complete Redis compatibility. The initial core baseline was Redis 7.2; newer command contracts follow the pinned 8.10 catalog and documented RedisJSON/RedisBloom APIs.

## Identify the running server

```sh
target/release/ffkv-cli -p 6382 HELLO 3
target/release/ffkv-cli -p 6382 INFO server
target/release/ffkv-cli -p 6382 COMMAND INFO GET SET
target/release/ffkv-cli -p 6382 COMMAND COUNT
```

HELLO identifies ffkv. INFO includes `server_name:ffkv`, `ffkv_version`, and `redis_version:7.2.0` for client baseline checks; this does not make ffkv Redis 7.2 or limit the implementation to that command set. COMMAND COUNT counts root commands, including ffkv extensions; 486 catalog entries also include subcommands. Do not compare these as the same count.

## What the coverage evidence means

Repository BDD scenarios launch actual ffkv processes and invoke the native CLI. The catalog gate requires a passing tagged scenario **and** an observed positive client invocation for each catalog entry. An inventory/tag check alone is not behavioral verification. A scoped feature run does not establish whole-catalog conformance.

Earlier core differential smokes used Redis 8.2.1; metadata/newer contracts are pinned to 8.10 sources/docs. No complete live Redis 8.10 or pinned 7.2 differential conformance run is claimed. Exact error prose, allocator statistics, every option combination, pathological input, or third-party behavior is not guaranteed identical. Unknown commands/options return errors, not success-shaped no-ops.

## Protocol and data model

RESP2 is the default; select RESP3 with `-3` or HELLO 3. Keys/values are bytes, not necessarily UTF-8. Requests cap at 8 MiB including framing and 1,024 arguments. Client replies have bounded logical/wire allocation, depth, and value counts. RESP3 streamed/unknown-length frame syntax is unsupported. Server inline syntax is simple space/tab separation, not shell/native-CLI quoting.

Standalone mode defaults to 16 logical databases, configurable with `--databases`; cluster mode exposes database zero. Scan cursors are opaque stable hash-order positions, COUNT is a hint, and iteration is not a snapshot. Array ARSCAN instead returns index/value pairs and is not a SCAN cursor API.

Strings/bitmaps are capped at 8 MiB per value. Extended families have additional [limits](/reference/limits/). Arrays use exact integer aggregates when operands are integers; real/mixed arithmetic uses binary64 rather than platform-specific C extended precision. All types remain subject to request/reply budgets.

## Persistence, modules, and distribution

- WAL, snapshots, and backups are ffkv formats. Bloom/Cuckoo SCANDUMP/LOADCHUNK uses versioned checksummed **FFKF**, not RedisBloom native-memory dumps. Do not use those dumps as a RedisBloom migration bridge.
- [Lua](/guides/lua-functions/) runs sandboxed Lua 5.1 with explicit instruction/memory/bridge limits. The volatile script cache is bounded to 256 scripts/16 MiB; applications must handle NOSCRIPT. Durable named function libraries are distinct from that cache.
- [Native modules](/guides/modules/) are trusted unsandboxed code using an explicit Modules API v1 subset. Module datatypes, persistence callbacks, blocking/thread/timer/event/shared APIs are not implemented. An arbitrary Redis module is not promised loadable.
- [Search](/guides/search/) is an indexing capability over source documents, not a twentieth datatype. Search workers use a real Rayon pool; generation-fenced Rust compaction is not a POSIX-fork GC implementation.
- [Hybrid storage](/operations/hybrid-storage/) offloads large list/native-job bodies only. MEMORY reports owned allocation estimates, not Redis/jemalloc accounting. There is no eviction or hard dataset/RSS cap.
- Native replication is asynchronous; ffkv clustering uses its own authenticated bus, not the Redis cluster bus. No production HA certification or exhaustive failover guarantee is implied by command coverage.
- ffkv requires an explicit native fsync acknowledgement for WAITAOF, including at offset zero. An owned Redis 8.2.1 reference run returned `[0, 1]` for preactivation writes followed by an AOF-disabled replica; ffkv deliberately returns `[0, 0]` in that case rather than interpreting an unknown fsync position as durable zero. This conservative difference follows the [documented fsync guarantee](https://redis.io/docs/latest/commands/waitaof/); it is not a claim of exact Redis edge-case parity.

## Client/application compatibility

Redis-protocol clients can connect to ffkv without installing a Redis server. The pinned repository integration uses Ruby 3.4.6, Sidekiq OSS 8.0.7, redis-client 0.26.2, and connection_pool 2.5.4; see the [Sidekiq guide](/guides/sidekiq/) for scope and configuration. A tested stack is evidence for that stack, not blanket certification for all versions/plugins.

Use a disposable dataset and your application's actual command/options, scripts, failure handling, and serialization before migration. List pops are remove-on-delivery, Pub/Sub is ephemeral, and runtime transaction/Lua errors do not roll back earlier writes. Those boundaries matter more than whether a command name appears in a catalog.
