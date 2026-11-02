---
title: Architecture
description: Understand ffkv actors, atomic coordination, resolved-effect persistence, and storage profiles.
---

ffkv is a Rust/Tokio datastore with RESP2/RESP3 listeners, key-owning shard actors, an ordered durability gate, and optional authenticated management. It is an alpha implementation, not Redis running behind a different executable name.

## Request to reply

The protocol layer incrementally parses binary-safe requests. A connection maintains its selected database, authentication, transaction/watch state, subscriptions, and protocol version. An ordinary command runs on the actor owning its key. The default is four shards; `--shards` accepts 1 through 256.

Each actor owns its mutable state. A shared admission barrier permits ordinary actor work to progress independently. Cross-shard commands fence admission and transfer only affected entries into an atomic view without copying collection payloads. Commands such as MSET, set stores, rename, and multi-key blocking pops retain command-level atomicity. Long coordinated commands and checkpoints can still delay other clients: sharding is not a latency-isolation guarantee.

The WAL has one ordered commit gate shared by actors. A resolved mutation batch is committed before publication to readers/waiters under the applicable durability policy. WAL failure is fail-stop, rather than allowing unjournaled changes to leak into later reads. [Transactions](/guides/transactions/) and [Lua](/guides/lua-functions/) coordinate a batch, but neither provides SQL-style rollback for runtime command errors.

## Memory and hybrid profiles

Without `--data-dir`, all state is ephemeral. A persistent memory profile keeps live structures in RAM while recording logical mutations to disk. Hybrid adds checksummed segment offloading for **large list elements and native-job bodies only**. Small bodies, strings, hashes, set identities, sorted-set indexes, JSON, sketches, vectors, and other typed structures remain RAM-resident.

The hybrid cache budget counts cached body bytes, not metadata or process RSS. Dedicated blocking owner threads handle cold payload I/O. Readers pin generations across compaction; retired files are reclaimed after their readers finish. A data directory needs room for recovery rebuilds and compaction, not just current logical data. See [hybrid storage](/operations/hybrid-storage/) and [memory](/operations/memory/).

## Recovery records effects, not instructions

WAL/checkpoint data contains resolved logical payloads and absolute expiration deadlines, not physical segment pointers. Random selections, generated IDs, lease receipts, and script effects are resolved before journaling; replay does not rerun Lua or make a new random choice. Changing shard count on restart redistributes recovered keys without changing key identity.

Strict durability flushes and fsyncs each mutation batch before its reply. Every-second mode acknowledges before the next fsync and can lose recent acknowledged writes on an OS/storage crash. A lost reply always leaves command outcome uncertain. Local fsync is not replication, machine survival, or exactly-once external execution. Read [persistence](/operations/persistence/) and [backup/restore](/operations/backup-restore/) before relying on recovery.

## Blocking and expiration

Blocking list/stream/time-series requests register bounded waiters rather than polling the dataset. The initial scan and registration are atomic, avoiding a lost wakeup between checking and sleeping. Per-key ordering, monotonic timeout indexes, and disconnect cancellation reclaim waiters. A blocked connection's subsequent pipelined commands wait behind its pending reply.

Key expiration uses wall-clock deadlines, lazy access-time removal, and bounded background reclamation. Offline time counts. List operations preserve TTL except when the last element removes the key; hashes additionally support independent field deadlines. Exact semantics belong to each [datatype guide](/data-types/).

## Separate management and distributed capabilities

The optional React management listener has separate bearer/OIDC authorization and auditing from RESP ACLs. Its command endpoint does not accept blocking reads, and its key browser supports fewer types than the command console. A browser write confirmation is not a substitute for server authorization.

Native RESP replication is asynchronous. ffkv cluster mode adds hash slots, routing, peer membership, and its own authenticated bus; it cannot join a Redis cluster bus. The development HTTP snapshot poller is a separate mechanism, not native replication. See [replication](/operations/replication/) and [clustering](/operations/clustering/). No production high-availability certification is claimed.
