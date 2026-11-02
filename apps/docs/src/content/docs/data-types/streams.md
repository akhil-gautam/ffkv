---
title: Streams
description: Append ordered events, read history, and manage durable consumer-group pending entries.
---

Streams hold ordered IDs and field/value sequences. Fields can repeat and retain their order; an entry is not simply a unique-key map. Use streams for event history and consumer groups when a remove-on-pop list is insufficient.

## Append and read history

```sh
target/release/ffkv-cli -p 6382 XADD docs:stream 1000-0 job print id 1
target/release/ffkv-cli -p 6382 XADD docs:stream 1001-0 job print id 2
target/release/ffkv-cli -p 6382 XRANGE docs:stream - + COUNT 10
target/release/ffkv-cli -p 6382 XREAD COUNT 10 STREAMS docs:stream 0-0
target/release/ffkv-cli -p 6382 XLEN docs:stream
```

Explicit IDs must obey stream ordering rules. In ordinary production writes, quote `'*'` to request a generated ID rather than letting the shell expand it. XREAD returns entries newer than the supplied ID; it does not remove them. BLOCK takes milliseconds, with zero meaning indefinite waiting. In EXEC/Lua, stream reads are nonblocking.

## Consumer groups and acknowledgement

On the fresh stream above:

```sh
target/release/ffkv-cli -p 6382 XGROUP CREATE docs:stream docs-workers 0
target/release/ffkv-cli -p 6382 XREADGROUP GROUP docs-workers alice COUNT 1 STREAMS docs:stream '>'
target/release/ffkv-cli -p 6382 XPENDING docs:stream docs-workers
target/release/ffkv-cli -p 6382 XACK docs:stream docs-workers 1000-0
target/release/ffkv-cli -p 6382 XRANGE docs:stream - +
```

`>` asks for new group deliveries. After successfully processing the first entry, XACK removes its pending ownership, **not the entry from history**. Pending entries and group progress survive recovery with persistence. XCLAIM/XAUTOCLAIM transfer stale pending work; XNACK releases owned work for reclaim. Define application retry/idempotency policy: acknowledgement does not make external side effects exactly once.

This example uses fixed IDs only so the acknowledgement is runnable. With generated IDs, capture and acknowledge the actual returned ID. Re-running CREATE against the existing group is an error; use a fresh key/group or inspect state rather than swallowing it.

## Retention and limits

XDEL/XTRIM are destructive history operations. Approximate MAXLEN trimming uses **100-entry granularity**, so `~` is not an exact size cap. Newer XACKDEL/XDELEX forms distinguish retained references and group acknowledgements; consult the reference before pruning data needed by consumers. Producer idempotency commands XCFGSET/XIDMPRECORD are separate from consumer acknowledgement.

Waiters cap at 65,536 clients and 64 MiB retained logical request data, also bounded by the connection cap. Reply/work limits apply to history reads. Physical index statistics describe ffkv, not Redis allocator layout. Stream structures remain RAM-resident in hybrid mode.

See [XREADGROUP](/reference/commands/xreadgroup/), [XAUTOCLAIM](/reference/commands/xautoclaim/), [native jobs](/guides/native-jobs/), [Pub/Sub](/guides/pubsub/), and [persistence](/operations/persistence/). Without a data directory, history and pending state are volatile.
