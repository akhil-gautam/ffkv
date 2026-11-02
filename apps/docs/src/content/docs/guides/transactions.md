---
title: Transactions and WATCH
description: Use one connection for MULTI/EXEC, optimistic WATCH, and atomic durable batches without assuming rollback.
---

MULTI/EXEC queues commands and executes their effects atomically relative to other clients. WATCH adds optimistic conflict detection. Neither mechanism supplies SQL-style rollback for runtime command failures.

## Keep transaction state on one connection

Start a persistent CLI session from the repository root:

```sh
target/release/ffkv-cli -p 6382
```

Enter these **inside that session**, not as separate shell invocations:

```text
SET docs:txn:balance 10
WATCH docs:txn:balance
GET docs:txn:balance
MULTI
SET docs:txn:balance 11
INCR docs:txn:updates
EXEC
```

Commands after MULTI are queued. EXEC pauses shard admission and publishes one atomic durable batch when persistence is enabled. WATCH checks the database/key identity for writes, deletion/recreation, and expiration. If another client changes the watched balance before EXEC, EXEC returns a null array and does not execute the queued work. In an application, reread state and recompute the entire optimistic operation rather than blindly replaying stale assignments.

Separate one-shot CLI calls cannot share a transaction or watch. A single piped CLI session also preserves connection state when interaction is unnecessary.

## Queue errors and runtime errors differ

In the same session, with fresh example keys:

```text
SET docs:txn:not-an-int text
MULTI
SET docs:txn:before retained
INCR docs:txn:not-an-int
SET docs:txn:after retained
EXEC
GET docs:txn:before
GET docs:txn:after
```

INCR fails at execution because its value is not an integer. The earlier write still commits, and the later write still runs. EXEC returns per-command results including the error; the native CLI treats nested EXEC errors as a nonzero command status. Unknown commands, invalid arity, and queue-time authorization/admission errors instead dirty the transaction and produce EXECABORT. Do not treat those outcomes as equivalent.

Malformed EVAL/EVALSHA/FCALL key counts are runtime errors: an otherwise well-formed invocation is queued, then its EXEC result contains the error while surrounding commands still run. This does not bypass authorization: command ACLs and extractable keys are checked when queueing, and full validation/authorization is mandatory at execution.

DISCARD abandons queued work. UNWATCH clears watched identities. Successful/aborted execution and disconnect end the relevant transaction state. SELECT changes the transaction's database context; WATCH identities include the database number, not just key bytes.

## Blocking, publication, and durability

List blocking operations inside EXEC never wait; stream reads are nonblocking, while TS.READ BLOCK is rejected. Outside list consumers wake only after the transaction's final state commits, not at an intermediate push that is later removed in the same batch.

PUBLISH/SPUBLISH can occur in EXEC, but publications become visible only after the batch commits. Pub/Sub remains ephemeral even when the transaction's dataset changes are durable. A failed WAL commit does not leak staged publications.

A lost EXEC reply is still an **unknown outcome**: atomicity does not tell a disconnected client whether the whole batch committed. Use application idempotency/state inspection rather than retrying increments, claims, or pops automatically.

## Limits and admission

Limits are **1,024 queued commands**, **16 MiB queued argument bytes**, and **1,024 watched keys**. Individual request/reply limits still apply. Registry command flags determine admission; NO_MULTI commands such as SAVE are rejected before execution. Large/long transactions pause other admission and can increase latency.

For reusable server-side logic see [Lua/functions](/guides/lua-functions/). For explicit acknowledgement/retry see [native jobs](/guides/native-jobs/). References: [MULTI](/reference/commands/multi/), [EXEC](/reference/commands/exec/), [WATCH](/reference/commands/watch/), and [persistence](/operations/persistence/).
