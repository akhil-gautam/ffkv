---
title: Architecture
description: Understand ffkv actors, atomic coordination, resolved-effect persistence, and storage profiles.
---

ffkv is a Rust/Tokio datastore with RESP2/RESP3 listeners, exclusive shard ownership, an ordered durability gate, and optional authenticated management. It is an alpha implementation, not Redis running behind a different executable name.

## Request to reply

The protocol layer incrementally parses binary-safe requests. A connection maintains its selected database, authentication, transaction/watch state, subscriptions, and protocol version. Single-shard execution holds an exclusive lease on the owning shard. The default is four shards; `--shards` accepts 1 through 256.

Eligible memory-backed commands execute directly on the connection task, avoiding a coordinator/worker round trip. The direct path is disabled for cluster/replica interception and while blocking consumers are registered. Direct writes additionally require WAL recording and online backup to be inactive. A read that expires data can still require an asynchronous commit. That commit retains its shard lease and write permit independently of the connection until it settles.

Memory-shard worker loops share the Tokio runtime rather than dedicating an OS thread to each logical shard. One blocking lifecycle owner supervises the group and keeps accepted work alive until it drains. Hybrid commands retain dedicated blocking workers so cold payload I/O stays off async runtime threads.

Cross-shard commands fence queued admission, acquire every shard lease, and transfer only affected entries into an atomic view without copying collection payloads. Commands such as MSET, set stores, rename, and multi-key blocking pops retain command-level atomicity. A connection does not execute later pipelined commands before its preceding command finishes. Long coordinated commands and checkpoints can still delay other clients: sharding is not a latency-isolation guarantee.

Replies share one bounded encoder, not a second execution queue. A connection retains at most 16 pending command completions and 8 MiB of consumed request bytes. Its encoding buffer starts at 128 bytes and grows as needed, up to 128 KiB for aggregated replies; individual bulk payloads larger than 16 KiB still stream directly. A large collection reply can also grow that buffer, and the capacity is retained until the connection closes. Commands retain their own database, client name, execution time, CPU delta and byte span. The prefix completed by one successful transport flush, including TLS, shares one delivery timestamp; its metric counts are aggregated locally and published once, without reclassifying flushed replies if later observation bookkeeping fails.

Aggregate encoding consumes each array, set or map through an owned iterator in its original allocation. It does not move every child onto a second response stack. Only unfinished outer aggregates need parent frames, so retained traversal capacity follows nesting depth rather than the largest flat reply. Empty aggregates and exhausted parents need no frame. Nested map keys/values and InfoField replies preserve wire order; public reply types and logical response budgets are unchanged.

If parsing leaves a partial request under 8 KiB, pending completions have admission room, and a command was parsed since the last read, the connection may try one immediately-ready read of at most 8 KiB before flushing. It never waits for more input to form a batch: pending input, EOF and read errors settle earlier replies first. Control commands and blocking waits likewise deliver earlier replies.

Ordinary socket reads append directly into the input buffer. Their budget starts at 8 KiB, doubles after full-budget reads up to 64 KiB, and resets after a genuinely short read below 8 KiB. The previous read size guides reservation, so small non-pipelined stored values do not each force a fresh 8 KiB allocation. These are read budgets, not allocation caps: partial large requests and some non-string stored payloads can retain larger backing allocations, and existing buffers are not shrunk when the budget resets. Logical input/output limits are not a total heap or RSS bound.

Before waiting for a busy shard, a connection flushes older replies without holding a shard lease or write permit, then rechecks client pauses and write admission. The queued shard-acquisition path reads only: a slow subscriber cannot reserve a shard while blocked writing a push. Pub/Sub charges remain held through delivery rather than being released when a message merely reaches the encoder.

Subscribed-mode checks read a cached per-client flag rather than locking the subscriber registry on every command. Registration changes and removal update that flag under the registry lock; publication ordering and output-budget accounting are unchanged.

Each builtin invocation binds immutable arguments to its root/subcommand metadata and precomputed static classifications. Successful identity/arity admission and counted-key validation are cached separately; binding itself cannot reject a request. This preserves NOAUTH precedence and defers counted-key errors in MULTI until EXEC. The engine still requires full validation, while ACL, role, database, pause and pressure decisions remain live. Owned queue and nested execution boundaries bind their own arguments; queued binding stays inside the existing thread-CPU measurement window.

The root metadata also selects typed server-control dispatch. Ordinary data commands skip the administrative future that would return no reply; real controls retain the same hooks and wait/disconnect handling. Client activity records share the command's actual post-pause start timestamp, and known command names use static root-name bytes instead of allocating lowercase copies. Unknown names retain binary-safe normalization. Pre-pause metrics timing, post-pause execution timing and exact thread-CPU windows remain distinct and unchanged.

Socket-level builtin predicates reuse that canonical root name, including native replication aliases, cluster controls and connection resets. Transaction classification uses each queued command's own binding; EVAL-family debug setup is deferred until actor fallback. Script-busy checks and validation/error precedence are unchanged.

Inline and queued shard requests transfer their existing session handle into the engine for execution instead of cloning it. The installed session selects its database; sessionless commands explicitly select database zero. CPU attribution and read tracking use the retained session, while nonempty staged writes still capture the handle needed to publish their committed offset. Reset clears the execution context as before.

Key-position iterators use bounded, allocation-free spans. ACL metadata extraction is skipped only when the root selector itself grants every command, read/write key and channel; restricted policies and validation errors retain their original checks.

The registry derives fixed single-key positions from compiled key specifications at startup. Those commands route and extract their key directly when present, without constructing or traversing the general span iterator. Optional missing positions still mean no keys. Counted, dynamic and coordinated specifications retain their original validation and routing; restricted ACL selectors still check the extracted key. This is metadata-driven, not a list of benchmark command names.

HSET/HMSET reuse the mutable hash lookup to derive field-expiration changes and resolved journal rows. Existing fields keep their owned names and reuse unshared value storage; new names are copied before entering field, scan, random-selection or expiry indexes. Repeated input fields still produce rows in input order, each containing that field's final value. Captured multi-field value writes sort position indices to resolve those final duplicate rows; capture-off writes allocate no journal rows. Key TTLs, retained migration fields and the shared field-deadline index keep their existing semantics.

ZADD validates every score before mutation, stages small score batches inline, and reuses the outer sorted-set lookup across input pairs. Repeated members still apply in input order, preserving conditional updates, counts and resolved journal rows. Range reads build their final reply directly; rank-window preallocation is bounded by the reply budget. ZRANGESTORE owns its selection before changing the destination, including when source and destination are the same key.

Normal GET/HGET reads discard expired data, build the reply and update access metadata through one entry borrow. Wrong-type and missing-field reads still touch a surviving key exactly once. CLIENT NO-TOUCH stays live; replay keeps its existing wall-clock access accounting. Expiration-driven search-index changes still finish before the command returns.

Unconditional SET without GET or KEEPTTL skips an unused old-entry lookup; conditional forms retain it. Integer updates parse, check overflow and apply the owned-string write under one mutable entry borrow when the string is live and its key has no held image or retained marker. Missing keys and unusual held/retained or elapsed-replay states retain the general store lifecycle. Errors do not mutate values or journal a write.

One parsed argument vector stays owned by the connection until its command is observed. Direct shard execution borrows it; coordinator queues and transaction queues take independent copies only when they need ownership beyond the connection. After observation, a connection may keep up to 16 emptied vectors, each with capacity for at most 32 arguments; these retain no argument payloads. The parser requests a vector only after validating a complete frame. Disconnecting a client still cannot cancel accepted, already-mutated work.

Ordinary unpaused commands read a published pause scope without subscribing to the watch channel. Pause replacement and expiry update that scope under the channel's lock. A waiter reads the pause value and its deadline clock under the same read guard, so an extension cannot slip between the snapshot and the expiry decision. A fired timer causes a fresh check of the published pause.

Without cluster slot accounting, builtin key lists for HOTKEYS are derived only when the completed command is observed by an active tracking session. Exact CPU accounting remains enabled regardless of session state. Dynamic-extension keys are resolved before acquiring telemetry locks, preserving the extension registry's lock ordering.

Telemetry thresholds are parsed when configuration is published, not on every command completion. A delivered prefix is observed in order under one telemetry lock and one complete configuration snapshot; extension-key resolution, cluster accounting and MONITOR emission stay outside that lock. Control commands still settle their connection's earlier completions before reading or resetting statistics. Command totals and latency quantiles derive from one set of histogram bucket counts; a Prometheus scrape uses those same counts for its command total, histogram count and cumulative buckets.

Coordinator failure immediately stops new shard work, even if connections retain references to the pool. Already-mutated commands retain responsibility for settling their commits. Dropping an unsettled lease poisons the pool rather than exposing partially committed state.

The WAL has one ordered commit gate shared by actors. A resolved mutation batch is committed before publication to readers/waiters under the applicable durability policy. WAL failure is fail-stop, rather than allowing unjournaled changes to leak into later reads. [Transactions](/guides/transactions/) and [Lua](/guides/lua-functions/) coordinate a batch, but neither provides SQL-style rollback for runtime command errors.

The commit gate separates asynchronous WAL/configuration exclusion from CPU-only publication. Memory-only commits share its read side; WAL writes, fsync, checkpoints, mode changes and backups take the exclusive side. A memory commit on a primary with inactive native history and no WATCH or tracking observers advances its revision without taking the publication mutex. Active-history, replica, observer and durable paths retain ordered publication. The synchronous publication region performs no file or network I/O. Both memory paths preserve session watermarks and fail closed on publication poison, panic or revision exhaustion; the first snapshot's shard barrier includes effects committed before history activation.

Once native history is active, replication records are serialized from immutable captured effects before acquiring the ordered commit gate. A fresh standalone memory server without a data directory skips that serialization until replication starts, but still captures mutation effects for revisions, WATCH, tracking and backups. The gate rechecks activation and encodes a missing record if history started after staging. The first full snapshot waits for outstanding shard holders, so earlier writes are included in its image and later writes follow its offset. Data-directory and recovered instances retain eager history.

Native RESP framing reserves each validated command's encoded size, including space for the transaction footer, before copying its payloads. It borrows immutable mutation arguments instead of creating temporary owned argument vectors, and reuses the protocol reply encoder's length formatting. Binary values, numeric spelling and native stream offsets are unchanged.

## Memory and hybrid profiles

Without `--data-dir`, all state is ephemeral. A persistent memory profile keeps live structures in RAM while recording logical mutations to disk. Hybrid adds checksummed segment offloading for **large list elements and native-job bodies only**. Small bodies, strings, hashes, set identities, sorted-set indexes, JSON, sketches, vectors, and other typed structures remain RAM-resident.

Set, hash and sorted-set headers are boxed so every entry does not reserve inline space for the largest collection header. This adds one allocation and an indirection for those collections; memory accounting includes the boxed header as well as its contents. Their logical representation, iteration order, expiry rules and recovery format are unchanged.

Stored strings, including bitmap and HyperLogLog representations, and hash field values use the private `StringValue`/`StringBuffer` ownership boundary in `engine/strings.rs`. Incoming request and replay/import bytes are copied into independently owned, whole buffers. Overwrites reuse unique storage; outstanding replies, snapshots and journal views trigger copy-on-write instead. General buffers retain at most `2 × length + 64` bytes of reported capacity, while integer writes use exact digit capacity and stack formatting. This is not an allocator or process-RSS bound; other datatypes do not automatically inherit this ownership guarantee.

GET and hash readers still return cheap immutable views. Buffer edits preserve their owned allocation where possible; TTL-only GETEX does not copy the body. Hash field names and already-owned retained values move without body copies when restored. Recovery/import pays placement copies, including the existing COPY export/decode/replay path and HIMPORT field values; HINCRBYFLOAT also copies its formatted result into owned storage. TTL-only hash journal rows retain immutable value views, so a later write can require copy-on-write. Sparse HLL encoding uses bounded inline scratch before allocating its final stored bytes. Wire/WAL/snapshot formats are unchanged.

The hybrid cache budget counts cached body bytes, not metadata or process RSS. Dedicated blocking owner threads handle cold payload I/O. Readers pin generations across compaction; retired files are reclaimed after their readers finish. A data directory needs room for recovery rebuilds and compaction, not just current logical data. See [hybrid storage](/operations/hybrid-storage/) and [memory](/operations/memory/).

## Recovery records effects, not instructions

WAL/checkpoint data contains resolved logical payloads and absolute expiration deadlines, not physical segment pointers. Random selections, generated IDs, lease receipts, and script effects are resolved before journaling; replay does not rerun Lua or make a new random choice. Changing shard count on restart redistributes recovered keys without changing key identity.

Strict durability flushes and fsyncs each mutation batch before its reply. Every-second mode acknowledges before the next fsync and can lose recent acknowledged writes on an OS/storage crash. A lost reply always leaves command outcome uncertain. Local fsync is not replication, machine survival, or exactly-once external execution. Read [persistence](/operations/persistence/) and [backup/restore](/operations/backup-restore/) before relying on recovery.

A live WAL and its checkpoint/backup snapshots share one directory lease. The final logical owner explicitly unlocks it before closing the lock descriptor, so an unrelated pre-exec child cannot keep a closed database locked merely by inheriting that descriptor. Replacing or dropping an intermediate snapshot does not release the live database's lease.

## Blocking and expiration

Blocking list/stream/time-series requests register bounded waiters rather than polling the dataset. The initial scan and registration are atomic, avoiding a lost wakeup between checking and sleeping. Per-key ordering, monotonic timeout indexes, and disconnect cancellation reclaim waiters. A blocked connection's subsequent pipelined commands wait behind its pending reply.

Key expiration uses wall-clock deadlines, lazy access-time removal, and bounded background reclamation. Offline time counts. List operations preserve TTL except when the last element removes the key; hashes additionally support independent field deadlines. Exact semantics belong to each [datatype guide](/data-types/).

Reads skip expiry-index work only when all key-expiry, field-expiry and retained-expiry indexes are empty. Promotion and migration holds still defer removal; releasing a hold allows an expired value to be removed and journaled once.

## Separate management and distributed capabilities

The optional React management listener has separate bearer/OIDC authorization and auditing from RESP ACLs. Its command endpoint does not accept blocking reads, and its key browser supports fewer types than the command console. A browser write confirmation is not a substitute for server authorization.

Native RESP replication is asynchronous. ffkv cluster mode adds hash slots, routing, peer membership, and its own authenticated bus; it cannot join a Redis cluster bus. The development HTTP snapshot poller is a separate mechanism, not native replication. See [replication](/operations/replication/) and [clustering](/operations/clustering/). No production high-availability certification is claimed.

The native replication supervisor waits for the actor's role acknowledgement before opening its upstream connection. Reading the acknowledged generation before the role snapshot prevents a late notification for the same transition from cancelling the first handshake.

Primary-role checks use a cached no-upstream state instead of taking the replication-state mutex for every ordinary primary publication. Replica answers still consult guarded state, and poisoned replication state remains fail-stop.
