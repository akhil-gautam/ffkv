---
title: List queues
description: Run FIFO producers and blocking consumers while understanding remove-on-pop failure semantics.
---

A Redis-style list queue is simple: producers insert at one end and consumers remove at the other. It is not a leased-job system. This guide assumes the [quickstart](/getting-started/quickstart/) server on 127.0.0.1:6382.

## Run a FIFO consumer

In a consumer terminal:

```sh
target/release/ffkv-cli -p 6382 BRPOP docs:list-queue 0
```

In a producer terminal:

```sh
target/release/ffkv-cli -p 6382 LPUSH docs:list-queue '{"job":"print","id":1}' '{"job":"print","id":2}'
```

On a fresh queue, LPUSH reports length two before waiting consumers pop. The consumer receives the key and job `id:1`; LPUSH plus BRPOP gives FIFO order. RPUSH plus BLPOP is the symmetric choice. Using the same end for both gives stack-like behavior instead.

```sh
target/release/ffkv-cli -p 6382 LRANGE docs:list-queue 0 -1
target/release/ffkv-cli -p 6382 RPOP docs:list-queue
target/release/ffkv-cli -p 6382 TYPE docs:list-queue
target/release/ffkv-cli -p 6382 BLPOP docs:list-queue:empty 0.5
```

The remaining job is `id:2`; its pop removes the final element, so TYPE becomes `none`. The last command returns null after a finite wait.

## Waiter ordering and timeouts

BLPOP/BRPOP accept multiple keys in request order and a nonnegative floating-point timeout in **seconds**. Zero means indefinite waiting; positive values round upward to milliseconds. Waiters on the same key are served in registration order. The connection remains occupied: later pipelined commands do not run before the blocking command finishes. Use another connection for cancellation/control or independent work.

If watched keys change type while a consumer waits, a later wakeup rechecks the original ordered key scan and can produce WRONGTYPE before reaching the newly populated list. Merely turning a missing key into a string does not wake a consumer. List operations inside EXEC/Lua never block.

Disconnecting a pending consumer cancels its wait, but a lost delivery reply can mean the pop already happened. Do not reconnect and assume no item was removed.

## Failure and durability model

A successful pop **immediately removes the payload**. If the worker crashes before completing its external side effect, there is no automatic redelivery. Persistence preserves completed pops, not a backlog of unacknowledged deliveries. Unfulfilled waits are not restored after restart.

LMOVE/BLMOVE can atomically transfer to an application-managed processing list, but then the application must design acknowledgement, timeouts, duplicate handling, and recovery. Prefer [native leased jobs](/guides/native-jobs/) when those are requirements, or [streams](/data-types/streams/) for consumer groups and history. Sidekiq's normal BRPOP fetcher has this same remove-on-delivery boundary.

Pushes and partial pops retain key TTL; the last pop removes key and TTL. Recreating the key starts without expiry. Expiring a queue can discard all remaining work, so do not apply cache TTL habits to business-critical queues accidentally.

## Payload and operational bounds

Hybrid mode can offload large list bodies, but indexes/small values remain in RAM. Strict durability journals deliveries before reply; every-second mode has a weaker crash window. Oversized destructive count-pops are rejected before removal, not silently truncated. Use bounded LRANGE pages and [observability](/operations/observability/) rather than fetching an entire unbounded queue.

See [lists](/data-types/lists/), [BRPOP](/reference/commands/brpop/), [BLMOVE](/reference/commands/blmove/), [persistence](/operations/persistence/), and [Sidekiq](/guides/sidekiq/).
