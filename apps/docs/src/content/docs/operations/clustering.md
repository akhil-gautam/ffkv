---
title: ffkv clustering
description: Bootstrap slots and peers, attach replicas, migrate slots and understand custom-bus and failover safety limits.
---

Cluster mode provides 16,384 hash slots, MOVED/ASK routing, peer membership, real native replication, slot transfer and automatic/manual failover. It uses an **ffkv-owned bus**, not the Redis cluster bus. It cannot join a Redis cluster, and tested behavior is not production-HA certification. Standalone shard actors (`--shards`) are internal concurrency units, not cluster nodes.

## Prerequisites and bus security

Every node needs its **own** data directory, RESP address, bus address and durable topology. Enable `--cluster-enabled`; a data directory is mandatory. Topology defaults to `data-dir/nodes.json`, or set `--cluster-config-file`. Do not hand-edit live topology or copy one node's identity file to another.

The bus binds the RESP listener's IP and defaults to RESP port + 10,000; override with `--cluster-bus-port` and, for advertised ports, `--cluster-announce-bus-port`. Explicitly choose routable bind addresses in real deployments: `0.0.0.0` is not a useful advertised peer endpoint. This implementation has no separate CLI announce-IP flag.

All peers must share `--cluster-bus-secret`. Non-loopback bus startup requires at least 32 bytes. The bus sends length-framed JSON over TCP, including its authentication envelope: **RESP TLS does not encrypt it**. Use an isolated trusted private network and appropriate network controls/encrypted network transport; do not expose it to the Internet. The current flag takes the secret value directly, not a file, so account for process-argument exposure. Loopback fixtures may use the empty default only on trusted local hosts. RESP ACL/TLS and native upstream credentials are separate from the bus secret.

## Bootstrap three local primaries

Use three separate terminals with fresh directories and unused ports; do not reuse a running console example's directory:

```sh
# Terminal 1
target/release/ffkv --bind 127.0.0.1:6382 --data-dir .ffkv-docs-cluster-a \
  --durability strict --cluster-enabled --native-replication-allow-loopback
```

```sh
# Terminal 2
target/release/ffkv --bind 127.0.0.1:6384 --data-dir .ffkv-docs-cluster-b \
  --durability strict --cluster-enabled --native-replication-allow-loopback
```

```sh
# Terminal 3
target/release/ffkv --bind 127.0.0.1:6386 --data-dir .ffkv-docs-cluster-c \
  --durability strict --cluster-enabled --native-replication-allow-loopback
```

From a fourth terminal, introduce peers and assign disjoint inclusive ranges:

```sh
target/release/ffkv-cli -p 6382 CLUSTER MEET 127.0.0.1 6384 16384
target/release/ffkv-cli -p 6382 CLUSTER MEET 127.0.0.1 6386 16386
target/release/ffkv-cli -p 6382 CLUSTER ADDSLOTSRANGE 0 5460
target/release/ffkv-cli -p 6384 CLUSTER ADDSLOTSRANGE 5461 10922
target/release/ffkv-cli -p 6386 CLUSTER ADDSLOTSRANGE 10923 16383

target/release/ffkv-cli -p 6382 CLUSTER NODES
target/release/ffkv-cli -p 6382 CLUSTER LINKS
target/release/ffkv-cli -p 6382 CLUSTER INFO
target/release/ffkv-cli -3 -p 6382 CLUSTER SLOTS
```

Membership/ownership propagation is asynchronous. Wait until all nodes report three members, all 16,384 slots assigned and `cluster_state:ok` before application writes. `CLUSTER SHARDS` provides grouped primary/replica topology. `CLUSTER SAVECONFIG` explicitly saves topology; node and shard IDs survive restart. Repeat the same startup commands/directories to restart nodes, not the slot-assignment commands on already assigned slots.

This three-primary example has **no replicas** yet and cannot replace a failed primary. Cluster mode exposes database zero only.

## Routing and same-slot keys

```sh
target/release/ffkv-cli -p 6382 CLUSTER KEYSLOT 'foo{bar}'
target/release/ffkv-cli -p 6382 SET 'foo{bar}' value
```

The example slot is 5061, initially owned by the 6382 node. Slots use CRC16 XMODEM; the first nonempty hash tag groups related keys. Multi-key operations need compatible same-slot keys or fail with CROSSSLOT. A MOVED response identifies the current owner; ASK is temporary during legacy slot transfer and needs ASKING on the target connection. Do not assume the native CLI automatically follows redirects: connect to the reported owner. Connection-scoped ASKING/READONLY must share the subsequent command's connection.

## Add a replica

Start a fourth node in its own terminal:

```sh
target/release/ffkv --bind 127.0.0.1:6388 --data-dir .ffkv-docs-cluster-d \
  --durability strict --cluster-enabled --native-replication-allow-loopback
```

Then introduce it and attach it to the first primary, using the actual node ID:

```sh
target/release/ffkv-cli -p 6382 CLUSTER MEET 127.0.0.1 6388 16388
PRIMARY_ID=$(target/release/ffkv-cli --raw -p 6382 CLUSTER MYID)
# Wait until 6388's CLUSTER NODES includes PRIMARY_ID before attaching.
target/release/ffkv-cli -p 6388 CLUSTER REPLICATE "$PRIMARY_ID"
target/release/ffkv-cli -3 -p 6388 ROLE
target/release/ffkv-cli -p 6382 CLUSTER REPLICAS "$PRIMARY_ID"
```

Only attach an empty dedicated node; attachment starts real native replication. Configure TLS/password-file upstream flags on protected nodes. Add independent replicas for the other primaries if their slots also need a promotion candidate. Use [WAIT/WAITAOF on writer connections](/operations/replication/#wait-and-waitaof-belong-to-the-writer-connection) when the application needs explicit acknowledgement barriers.

## Atomic slot migration

For the example key in slot 5061, initiate transfer **on the destination** (6384):

```sh
MIGRATION_ID=$(target/release/ffkv-cli --raw -p 6384 CLUSTER MIGRATION IMPORT 5061 5061)
target/release/ffkv-cli -3 -p 6384 CLUSTER MIGRATION STATUS ID "$MIGRATION_ID"
```

Poll status until `completed`, inspecting `state`, `last_error`, source/destination IDs and timing. IMPORT accepts inclusive range pairs whose slots share one source primary. Destination must be a primary and cannot have open legacy importing/migrating slots or a conflicting active task. The transfer takes a real snapshot, streams concurrent effects, coordinates a final cutover and updates ownership; a write pause is possible and reported for completed source migration tasks.

After completion, verify ownership/data rather than assuming the task response alone is enough:

```sh
target/release/ffkv-cli -3 -p 6382 CLUSTER SLOTS
target/release/ffkv-cli -p 6384 GET 'foo{bar}'
```

`CLUSTER MIGRATION STATUS ALL` lists tasks. `CLUSTER MIGRATION CANCEL ID "$MIGRATION_ID"` or `CANCEL ALL` cancels active work. A source restart closes migration channels and can leave a failed task; inspect its error and ownership before retrying. Do not publish manual SETSLOT ownership changes in the middle of the atomic workflow.

Legacy `CLUSTER SETSLOT ... IMPORTING/MIGRATING/NODE/STABLE`, ASKING and MIGRATE are also available. Mixed-present multi-key requests during legacy migration can return TRYAGAIN without mutation. Slot metadata alone does not move data; avoid treating SETSLOT as a backup or a substitute for an actual transfer. Use the [command catalog](/reference/commands/) for exact variants.

## Failover and coverage policy

- Automatic elections use peer heartbeats, failure detection and quorum votes. Control deadlines are bounded separately from data transfers. The default node timeout is 15,000 ms.
- A planned `CLUSTER FAILOVER` is sent **to a replica** and observes the processed-offset catch-up barrier. It initiates asynchronously; wait for role/ownership convergence before client cutover.
- `CLUSTER FAILOVER FORCE` requests an election when the source is unavailable; it is not the same as bypassing quorum.
- `CLUSTER FAILOVER TAKEOVER` can claim slots without available voters. This is dangerous under partition: fence old writers first and accept explicit data-loss/split-brain responsibility.
- Full coverage is required by default. `--cluster-require-full-coverage false` and `--cluster-allow-reads-when-down true` change availability policy; they do not repair missing slots or provide stale-read consistency guarantees.

Do not test failure by killing unrelated processes. Preserve topology and data, verify actual replicas/catch-up, and rehearse isolated-node/partition/restart recovery in owned fixtures. Async replication, forced promotion, client retries and at-least-once jobs remain application-visible failure boundaries. The supplied Compose/Helm deployment is **single-node**, not a multi-node cluster installer.
