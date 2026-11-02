---
title: Pub/Sub
description: Publish live notifications with direct, pattern, and shard channels without confusing them with durable queues.
---

Pub/Sub delivers live binary-safe messages to currently connected subscribers. It has no offline mailbox, acknowledgement, replay log, or restart recovery. Use it for transient notifications; use [streams](/data-types/streams/) or [native jobs](/guides/native-jobs/) when missed messages must be recoverable.

## Subscribe and publish

In one terminal:

```sh
target/release/ffkv-cli -p 6382 SUBSCRIBE docs:events
```

In another:

```sh
target/release/ffkv-cli -p 6382 PUBLISH docs:events 'hello from ffkv'
```

The subscriber receives an acknowledgement then the message. The publication reply reports receivers, not completed application processing. Stop a direct subscription invocation with Ctrl-C. To change subscriptions while listening, use the interactive CLI and enter SUBSCRIBE/UNSUBSCRIBE on that persistent connection.

Pattern subscriptions match channel names:

```sh
target/release/ffkv-cli -p 6382 PSUBSCRIBE 'docs:events:*'
```

Publish from another terminal:

```sh
target/release/ffkv-cli -p 6382 PUBLISH docs:events:created '{"id":1}'
```

Quote patterns to prevent shell glob expansion. PSUBSCRIBE payloads identify the matching pattern as well as actual channel; applications must parse the correct response shape.

## RESP2, RESP3, and execution context

RESP2 subscribed connections accept only subscription changes, PING, and QUIT. RESP3 uses push frames and allows ordinary commands on the same connection, including pushes while a blocking list command waits. Select it with `-3` before the command; write clients must handle unsolicited pushes, not assume every frame answers the last request.

PUBLISH/SPUBLISH may execute inside MULTI or Lua. Publications are staged until the mutation batch commits. Earlier publications/effects survive a later runtime Lua error under the normal no-rollback model, but a failed WAL commit does not publish them. This ordering does **not** persist the messages: subscriptions and messages are never restored from WAL/snapshots.

## Shard channels and replication

SSUBSCRIBE/SUNSUBSCRIBE/SPUBLISH provide shard-channel delivery, separate from ordinary channels. PUBSUB SHARDCHANNELS/SHARDNUMSUB inspects that namespace. In ffkv cluster mode, real peer links forward global publications and deliver shard publications to the shard's replicas. Native replication propagates live publication traffic without persisting subscriptions. This is not a guarantee of replay after a network gap or failover.

## Authorization and backpressure

ACL channel grants use `&pattern`, allchannels, and resetchannels independently of `~key` permissions. A key grant does not authorize a similarly named channel. Pattern subscriptions require the exact granted pattern unless all channels are granted; global PUBSUB inspection requires allchannels. Subscription commands are unavailable in MULTI/Lua.

Each client may retain at most **1,024 channel/pattern registrations** and **8 MiB of names**; patterns cap at **1,024 bytes**. Output is bounded to **256 events / 16 MiB per client**, including staged/in-flight messages. Overflow disconnects a slow subscriber; it is not an unbounded server-side backlog. Publications exceeding shared pattern-matching work are rejected before delivery without disconnecting subscribers.

Handle disconnects as potentially missed notifications and rebuild authoritative state from durable data where needed. The CLI never automatically reconnects/replays writes. See [SUBSCRIBE](/reference/commands/subscribe/), [PUBLISH](/reference/commands/publish/), [security](/operations/security/), [replication](/operations/replication/), and [CLI](/getting-started/cli/).
