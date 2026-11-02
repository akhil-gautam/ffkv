---
title: Migrating data
description: Copy supported Redis data with absolute TTLs into an empty exclusive ffkv target, and recover safely from partial copies.
---

`scripts/migrate.py` is a standard-library Python client for a **quiesced**, one-database copy. It is not a live atomic migration, cluster resharder, full RDB importer or universal Redis module converter. Use [cluster migration](/operations/clustering/#atomic-slot-migration) for ffkv slot handoffs and [backup restore](/operations/backup-restore/) for ffkv logical backups.

## Supported data and prerequisites

The script copies strings, lists, sets, hashes and sorted sets, including binary/empty keys and values, list order, scores, absolute key TTLs and hash-field TTLs when supported. Unsupported types or oversized values cause an explicit failure, never a silent skip. Streams, JSON, probabilistic structures, vectors and native leased queues are **not** copied by this script.

Before running:

1. Stop **all source writers**, schedulers and queue consumers. `--source-quiesced` is your acknowledgement, not a server-enforced freeze.
2. Keep the source intact and available throughout validation/cutover.
3. Make the target empty and exclusively owned by this migration. The script checks `DBSIZE`; it does not lock out other clients or offer destructive replace mode.
4. Synchronize source/target clocks. Absolute expiration must not become accidental persistence.
5. Ensure the source supports `PEXPIRETIME`. If it supports `HPEXPIRETIME`, the target must support `HPEXPIREAT` for exact field expiry.
6. Use a separate target directory and port. The client uses database zero; there is no database-selection option or automatic all-database migration.

## Local workflow

Start a new durable target:

```sh
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-migration --durability strict
```

Assuming a deliberately quiesced source at `127.0.0.1:6379`, run in another terminal:

```sh
python3 scripts/migrate.py \
  --source 127.0.0.1:6379 --target 127.0.0.1:6382 \
  --source-quiesced
```

Do not use an existing production target just to try the command. The tool writes real data and checks the source's expirations while copying. It deduplicates repeated SCAN keys in a bounded-cache on-disk SQLite table rather than keeping the full keyspace in RAM.

## TLS and authentication

Each endpoint accepts `--source-username`/`--target-username`, corresponding `--*-password-file`, `--*-tls`, `--*-tls-ca` and `--*-tls-server-name`. A username requires a password file. CA selection enables verified TLS; a server-name override never disables verification. Endpoints are `HOST:PORT` or `[IPv6]:PORT`, **not credential URLs**.

For the protected target created by the console walkthrough, a local unprotected source can be copied with:

```sh
python3 scripts/migrate.py \
  --source 127.0.0.1:6379 --target 127.0.0.1:6382 --source-quiesced \
  --target-username admin --target-password-file .ffkv-docs-dev/admin-password \
  --target-tls-ca .ffkv-docs-dev/ca.crt --target-tls-server-name localhost
```

Use that only while the protected target is empty/exclusive; do not start a second server on the same port. Add source-side credential/TLS file flags if required by your actual source.

## Bounds and outcomes

`--page-size` defaults to 32 and accepts 1–128; it is a SCAN-family COUNT hint, not a guaranteed page size. `--timeout` defaults to 10 seconds per command and accepts values in `(0, 300]`. Encoded commands and reply/page memory are bounded to 8 MiB; commands allow 1,024 arguments. Reply decoding also caps nodes/nesting. Reduce page size for oversized pages, but an individual oversized value cannot be fixed by truncating it.

The tool prints one JSON summary with scanned/unique/copied/expired keys, duplicate scans, written items/commands, errors, elapsed time, `target_touched` and `partial_target`:

- Exit **0**, `status: complete`: the supported copy completed. Still validate application invariants before cutover.
- Exit **2**, `status: failed_before_copy`: no target mutation was recorded as attempted.
- Exit **2**, `status: failed_partial_target`: a target mutation was attempted; preserve the summary and treat the target as partial/possibly changed.

Lost mutation replies have an **unknown outcome**. There is no automatic write replay or safe resume/replace mode. Do not rerun into a partial target: preserve the source and start a fresh target directory/endpoint. The source may naturally expire data while quiesced; the script preserves deadlines and does not resurrect expired fields/keys.

## Cutover checklist

Compare representative binary data, list order, set/hash cardinalities, sorted scores, key/field expiration and application behavior while the source remains quiesced. Account for natural expiry rather than demanding an unchanged total key count. Back up the validated target, decide how to handle any failed partial attempt, then route clients and restart workers deliberately. Preserve the original source until rollback is no longer required.
