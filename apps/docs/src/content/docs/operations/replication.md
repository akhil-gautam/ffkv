---
title: Replication
description: Operate native RESP replication and writer-connection durability barriers without confusing them with development snapshot polling.
---

ffkv has **two different replication mechanisms**. Native RESP replication is used by `REPLICAOF`, `PSYNC`, `WAIT`, `WAITAOF`, standalone `FAILOVER` and ffkv clusters. Development HTTP replication periodically replaces a follower from a full management snapshot. Neither is a production-HA certification or an exactly-once worker guarantee.

## Native RESP replication

Start two isolated instances in separate terminals, using fresh directories:

```sh
# Source terminal
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-source --durability strict \
  --native-replication-allow-loopback
```

```sh
# Follower terminal
target/release/ffkv --bind 127.0.0.1:6384 \
  --data-dir .ffkv-docs-follower --durability strict \
  --native-replication-allow-loopback
```

Configure the follower from another terminal:

```sh
target/release/ffkv-cli -p 6384 REPLICAOF 127.0.0.1 6382
target/release/ffkv-cli -3 -p 6384 INFO replication
target/release/ffkv-cli -3 -p 6384 ROLE
```

Followership replaces stale local data; use a dedicated follower, never an unrelated existing dataset. `SLAVEOF` is the legacy spelling. Initial full synchronization transfers a bounded image, then live RESP effects. Reconnect can use partial synchronization when the requested history is still in the backlog; wrong/evicted history requires another full image. The default backlog is 64 MiB; `--replication-backlog-bytes` must be at least 1,024.

Upstream network access is deliberately constrained. Loopback needs the explicit `--native-replication-allow-loopback` opt-in. Cluster mode separately permits private upstream networking. On protected listeners configure the follower with `--native-replica-user`, `--native-replica-password-file`, `--native-replica-ca-file` and, when needed, `--native-replica-tls-name`. A username requires a password file and a TLS name requires a CA file. Use source ACL grants appropriate to replication; do not put upstream passwords into command arguments or URLs. RESP listener TLS and upstream verification are separate configurations.

## WAIT and WAITAOF belong to the writer connection

Open **one persistent CLI connection** to the source:

```sh
target/release/ffkv-cli -3 -p 6382
```

Enter these commands **inside that session**:

```text
SET demo:replicated written
WAIT 1 2000
WAITAOF 1 1 2000
```

`WAIT replicas timeout_ms` returns the number of connected replicas that processed this connection's write watermark. It is **not an fsync guarantee**. `WAITAOF numlocal numreplicas timeout_ms` returns `[local_count, replica_count]` for actual durable watermarks. The local threshold can be zero or one; requesting local durability with AOF disabled is an error. Both commands can time out with lower counts: inspect the returned counts, not merely whether the command returned successfully.

Running SET in one one-shot invocation and WAIT in a new invocation does **not** wait for the first connection's write. Keep barriers on the writing application's connection as well. A processed ACK can satisfy WAIT while a replica fsync is still pending and WAITAOF reports zero durable replicas. A barrier does not undo the original write on timeout and does not eliminate ambiguous delivery outcomes or every failover-loss scenario.

## State and handoff

Replication is asynchronous. Inspect `INFO replication` and `ROLE` on both nodes and validate actual data. Followers retain source-owned expired payloads and native-queue clocks until resolved source effects arrive. Full-image replacement fences downstream sessions, even at the same numeric offset. Live Pub/Sub publications can propagate, but subscriptions/messages are not durable backup data.

For a planned **standalone** handoff, stop application write admission and inspect a caught-up follower. On the current source:

```sh
target/release/ffkv-cli -p 6382 FAILOVER TO 127.0.0.1 6384 TIMEOUT 2000
```

The operation coordinates a write pause/catch-up and role swap; `OK` acknowledges initiation, so wait for both nodes' ROLE/INFO to show the intended roles before routing clients. `FAILOVER ABORT` can cancel a pending handoff. FORCE can promote without the ordinary catch-up guarantee and may lose data; it is not a routine repair. Cluster promotion uses **CLUSTER FAILOVER on a replica**, not this standalone command; see [clustering](/operations/clustering/).

For emergency manual promotion, fence the old writer **first**. `REPLICAOF NO ONE` detaches native followership, but does not fence the old source or guarantee that its acknowledged writes reached this replica. Reconcile queue receipts and application side effects before reopening workers. Reapply/verify intended roles after restart rather than assuming a previously issued standalone administrative command is your deployment configuration.

## Development HTTP full-snapshot polling

This mechanism is explicitly **not PSYNC, incremental streaming, election or automatic failover**. On the protected source from the [console walkthrough](/getting-started/console/), restart with the same flags plus `--replication-export`. This requires a durable master and exposes admin-only, audited `/api/replication/status` and `/api/replication/snapshot`.

Bootstrap a separate follower and start it on 6384:

```sh
python3 deploy/bootstrap.py "$PWD/.ffkv-docs-polling"
target/release/ffkv --bind 127.0.0.1:6384 \
  --data-dir .ffkv-docs-polling/data/database --durability strict --storage hybrid \
  --acl-file .ffkv-docs-polling/users.acl \
  --tls-cert .ffkv-docs-polling/tls.crt --tls-key .ffkv-docs-polling/tls.key \
  --replica-source https://localhost:6383 \
  --replica-token-file .ffkv-docs-dev/console-admin-token \
  --replica-ca-file .ffkv-docs-dev/ca.crt
```

The token and CA here belong to the **source management listener**; the follower's own TLS/ACL files protect its RESP listener. Polling defaults to 5,000 ms, timeout to 120,000 ms and maximum snapshot bytes to 1 GiB. Images are manifest/checksum/WAL-validated before atomic cutover. Source instance/revision identities skip unchanged data and are independent of checkpoint sequence resets. Temporary exports are cleaned on completion/disconnect and are not operator-retained backups.

On **every process start**, a polling follower returns LOADING until its first successful synchronization, even with a local copy. It rejects dataset writes, including management/EXEC/Lua paths, with READONLY. After synchronization it may serve explicitly stale reads during a source outage; status is disconnected and lag unknown. Known lag is committed-revision distance, not elapsed milliseconds. Keep source/follower clocks aligned for absolute TTLs and leases.

Named function libraries follow the image; ACLs, audit/client state, modules and script caches are local. Runtime configuration/module-registry ownership remains local. Allow disk space for source exports, download/install scratch and rebuilt segments. Interrupted `.ffkv-replica-*` staging is never a trusted recovery source; inspect/clean only owned staging while stopped, not live data.

To promote a polling follower: fence the source, stop the follower, restart its same data directory **without replica-source/token/CA flags**, verify state, then cut over clients. Snapshots can lag acknowledged source writes. This mechanism cannot provide zero-loss failover.
