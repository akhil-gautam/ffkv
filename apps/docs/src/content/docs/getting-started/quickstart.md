---
title: Quickstart
description: Build ffkv, write data, consume a list queue, and recover a persistent dataset.
---

This walkthrough uses a fresh directory and port **6382**, leaving existing services alone. Run shell commands from the repository root. See [installation](/getting-started/installation/) for Rust and C-toolchain prerequisites.

## Build and start

```sh
cargo build --release --bins
target/release/ffkv --bind 127.0.0.1:6382 --data-dir .ffkv-docs-quickstart --durability strict
```

Keep the server running in this terminal. The directory must belong only to this instance. Strict mode writes, flushes, and fsyncs each mutation batch before acknowledging it. Omit `--data-dir` only if you deliberately want a memory-only dataset that disappears on shutdown.

## Store values and inspect them

In another terminal:

```sh
target/release/ffkv-cli -p 6382 PING
target/release/ffkv-cli -p 6382 SET docs:greeting 'hello ffkv'
target/release/ffkv-cli -p 6382 GET docs:greeting
target/release/ffkv-cli -p 6382 SET docs:counter 41
target/release/ffkv-cli -p 6382 INCR docs:counter
target/release/ffkv-cli -p 6382 HSET docs:profile name Ada plan builder
target/release/ffkv-cli -p 6382 HGETALL docs:profile
```

The first responses are `PONG`, `OK`, `hello ffkv`, `OK`, and `42`. Use `--json` before the command for machine-readable responses; `-3` selects RESP3.

```sh
target/release/ffkv-cli -3 --json -p 6382 HGETALL docs:profile
target/release/ffkv-cli -p 6382 SET docs:temporary value PX 60000
target/release/ffkv-cli -p 6382 PTTL docs:temporary
target/release/ffkv-cli -p 6382 SET docs:greeting ignored NX
target/release/ffkv-cli -p 6382 GET docs:greeting
```

The failed `NX` write returns null and leaves the greeting unchanged. `PTTL` returns remaining milliseconds, `-1` for a key without expiration, or `-2` for a missing key. Time passes while the server is stopped too.

## Pass work through a list

Start a waiting consumer in a third terminal:

```sh
target/release/ffkv-cli -p 6382 BRPOP docs:work 0
```

Then produce work from the client terminal:

```sh
target/release/ffkv-cli -p 6382 LPUSH docs:work '{"job":"print","id":1}' '{"job":"print","id":2}'
```

The waiting consumer receives the queue key and job `id:1`. `LPUSH` plus `BRPOP` gives FIFO order. Inspect the remaining item:

```sh
target/release/ffkv-cli -p 6382 LRANGE docs:work 0 -1
```

A pop removes an item immediately; a worker crash after delivery can lose it. Use [native leased jobs](/guides/native-jobs/) when acknowledgement and retry are required, or [streams](/data-types/streams/) for consumer-group history. Neither persistence nor a client reconnect converts a list pop into a lease.

## Recover the same instance

Stop **this walkthrough's server** with Ctrl-C. Start it again with the exact same directory:

```sh
target/release/ffkv --bind 127.0.0.1:6382 --data-dir .ffkv-docs-quickstart --durability strict
```

Then query:

```sh
target/release/ffkv-cli -p 6382 GET docs:greeting
target/release/ffkv-cli -p 6382 LRANGE docs:work 0 -1
```

The greeting survives and the already delivered job stays removed. An expired temporary key does not return just because the process restarted. Pending blocked connections themselves are not recovered.

## Continue safely

Explore the [19 datatype families](/data-types/), [transactions](/guides/transactions/), and [Lua/functions](/guides/lua-functions/). `COMMAND INFO` describes commands in the running engine; [compatibility](/concepts/compatibility/) explains what catalog coverage does and does not establish.

A connection failure after sending a write leaves its outcome unknown. The native CLI does not automatically reconnect or replay it. Inspect application state before retrying a destructive operation. The [CLI guide](/getting-started/cli/) covers statuses, binary data, and TLS credentials.
