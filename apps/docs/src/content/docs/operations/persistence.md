---
title: Persistence and recovery
description: Choose WAL durability, understand crash recovery and locking, and manage checkpoints without risking your only copy.
---

Without `--data-dir`, ffkv is in-memory and loses its dataset when the process exits. Enabling a directory on a new process does not import data from an already running in-memory server. Both storage profiles can be durable; `memory` describes payload placement, not whether a WAL exists.

## Start a separate durable server

After [building the binaries](/getting-started/installation/), use a fresh directory and unused local port:

```sh
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-persistence --durability strict
```

From another terminal:

```sh
target/release/ffkv-cli -p 6382 SET durable:greeting hello
target/release/ffkv-cli -p 6382 RPUSH durable:jobs first second
target/release/ffkv-cli -p 6382 LPOP durable:jobs
target/release/ffkv-cli -p 6382 SET durable:temporary value EX 60
```

Stop with Ctrl-C, then repeat the server command using the **same directory**. The greeting survives and the list contains only `second`. TTLs are absolute wall-clock deadlines: downtime counts and expired data does not return.

## Acknowledgement policies

| Mode | Configuration | What a successful write reply means |
| --- | --- | --- |
| Strict | `--data-dir DIR --durability strict` | The mutation batch was appended, flushed and fsynced before reply. This is the default with a data directory. |
| Every second | `--data-dir DIR --durability everysec` | Appended/flushed before reply; fsync targets a one-second cadence. OS/storage failure can lose recent acknowledged writes; load and slow storage can extend the window. |
| Volatile | No `--data-dir` | No restart survival. |

These describe the normal append-only-enabled configuration. `--appendonly false` and runtime persistence configuration can disable AOF; do not assume a directory alone guarantees ongoing journaling after configuration changes. Inspect `CONFIG GET appendonly appendfsync` and `FFKV.STATS`. Persisted `CONFIG REWRITE` values can override startup flags; see [configuration](/reference/configuration/).

Ctrl-C and SIGTERM synchronize pending WAL data before exit. Any live WAL append/flush/fsync failure stops the server rather than allowing unjournaled changes to leak into later reads. A lost reply is an **unknown outcome**, not evidence that a mutation failed.

## Atomic records and recovery

`wal.ffkv` stores resolved logical effects, not commands to rerun with fresh random choices. Mutations from one command share an atomic batch. Large mutations use bounded BEGIN/data/COMMIT groups; recovery applies none of an unfinished group. Committed large groups replay in a bounded second pass rather than buffering the entire history.

At startup ffkv acquires the directory's `LOCK`, streams records, verifies CRC32C checksums and sequence numbers, and rebuilds values and expiration indexes. An incomplete final record or uncommitted tail group is discarded and synchronized before new appends. A complete record with a bad checksum, malformed data, unsupported version or invalid sequence fails startup **without truncating the WAL**. An empty/partial WAL header also fails closed.

- Never edit a live WAL or truncate a complete corrupt record.
- Never delete `LOCK` to bypass a running owner. A second process must not share the directory; a lock file's existence alone does not mean it is currently held.
- Preserve damaged files before investigation and restore a trusted backup into a **new** directory.
- Blocking-pop removals are logged before their replies; pending waiters are not restored. Pub/Sub subscriptions and traffic are ephemeral, not recovery data.

## Checkpoints

```sh
target/release/ffkv-cli -3 -p 6382 FFKV.STATS
target/release/ffkv-cli -3 -p 6382 FFKV.CHECKPOINT
```

A checkpoint pauses coordinated execution, streams a coherent logical image, syncs it and atomically replaces the WAL while retaining the lock. The response reports `old_bytes`, `new_bytes` and `data_frames`. File-local frame sequence numbers reset; they are **not replication offsets**. Automatic checkpointing defaults to 268,435,456 bytes (256 MiB) of WAL growth beyond the last image. `--wal-checkpoint-bytes 0` disables it.

Checkpointing bounds history growth but needs scratch space and can delay clients. It does not reclaim all derived hybrid blobs; [hybrid compaction](/operations/hybrid-storage/) is separate. Keep space for the current WAL, replacement image, segment rebuild/compaction, backups and replication staging.

## What durability does not promise

The crash suites cover process termination including SIGKILL, torn tails, directory locking and actual file-size-limit write failures. They do not certify power-loss behavior, honest hardware caches, disk survival or machine survival. `fsync` is local durability, not replication or HA. Use [backup/restore](/operations/backup-restore/) and, when needed, [replication](/operations/replication/) with an explicit recovery objective.
