---
title: Backup and restore
description: Create coherent immutable backups and restore trusted data into new directories with explicit cutover checks.
---

A copied, changing live WAL is not a backup. Use ffkv's coherent export, protect its plaintext logical data, and rehearse restoration before an incident. Backups do not automatically include your TLS keys, ACL policy, management tokens, audit history or deployment configuration; preserve those separately under appropriate secret controls.

## Named standalone backup

On a durable instance such as the one in [persistence](/operations/persistence/):

```sh
target/release/ffkv-cli -3 -p 6382 FFKV.CHECKPOINT
target/release/ffkv-cli -3 -p 6382 FFKV.BACKUP manual-1
```

A checkpoint is optional before a backup; the backup itself captures a coherent logical image. `FFKV.BACKUP` requires active persistence and writes an immutable directory at `data-dir/backups/manual-1`. Its response includes the path, source instance/revision, WAL byte count, data-frame count and SHA256. Names must contain **1–64 ASCII letters, digits, `_` or `-`**. Existing destinations are refused. Use unique names rather than overwriting the previous recovery point.

The standalone backup contains `wal.ffkv` and `manifest.ffkv` with length/frame-count/SHA256 metadata. Keep the complete directory. Encryption at rest and transport protection are your responsibility; checksums detect corruption, not authenticity from an untrusted producer.

## Online base-plus-incremental backup

The Redis-catalog `BACKUP` lifecycle is distinct from the named standalone extension:

```sh
target/release/ffkv-cli -3 -p 6382 BACKUP START
target/release/ffkv-cli -3 -p 6382 BACKUP STATUS
```

Wait until status reports `state: incrementing` with no error; source writes can continue. Then seal the recovery boundary:

```sh
target/release/ffkv-cli -3 -p 6382 BACKUP SEAL
target/release/ffkv-cli -3 -p 6382 BACKUP STATUS
target/release/ffkv-cli -3 -p 6382 BACKUP LIST
```

Only a successfully **sealed** backup is a restore source. It includes the base and writes through the seal boundary, not later writes. Its versioned manifest describes `base.ffkv` and `incremental.ffkv`; restore validates both and rebases file-local WAL sequences. Use the actual path returned by status/list rather than guessing a generated directory name. `BACKUP ABORT` stops an unsealed lifecycle; `BACKUP CLEANUP` removes the lifecycle's retained backup data. Do not clean up your only verified recovery point. See the [command reference](/reference/commands/) for each subcommand.

## Restore into a NEW directory

For the named example using `.ffkv-docs-persistence`:

```sh
target/release/ffkv restore \
  --source .ffkv-docs-persistence/backups/manual-1 \
  --data-dir .ffkv-docs-restored
```

Do **not** precreate `.ffkv-docs-restored`: restore refuses any existing destination, including an empty directory or symlink. It validates complete WAL groups, frame counts and manifests without repairing or modifying the source, synchronizes the restored files, and publishes only the new destination. Successful JSON reports `wal_bytes`, `data_frames` and the verified `sha256`. The same restore command accepts a trusted sealed online-backup directory.

After stopping the example source on 6382, start the restored instance:

```sh
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-restored --durability strict --storage hybrid
```

A backup's logical payloads allow recovery into either profile. Validate actual application values, collection order, queues, function libraries and TTLs before client cutover. Restored expiration deadlines remain absolute; already expired values are not resurrected. For a live source that must remain running, choose a different unused verification port instead.

## Incident runbook

1. **Fence writers and workers** on the damaged instance; avoid split-brain side effects.
2. Stop it and preserve its original directory and logs. Never delete `LOCK` to bypass a live owner, truncate complete corrupt records or overwrite the sole data copy.
3. Select a trusted complete backup and restore into a new directory with adequate space.
4. Start on an isolated endpoint with the intended ACL/TLS/runtime configuration. Verify state and application invariants, not just PING.
5. Establish the accepted data-loss boundary, then update clients/workers deliberately. Jobs are at-least-once; make external side effects idempotent.
6. Retain incident evidence and the old data until the restored service and a subsequent backup are verified.

Allow space for the source, backup, restore scratch, rebuilt hybrid segments and replication staging. Neither `--disk-budget` nor `--cache-bytes` caps total backup/recovery resource use. A publication/fsync error can be reported after a new destination has appeared; inspect and preserve it rather than automatically rerunning into that name.
