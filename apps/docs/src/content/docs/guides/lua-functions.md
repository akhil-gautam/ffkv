---
title: Lua scripts and durable functions
description: Run bounded Lua 5.1, use persistent CLI sessions correctly, and distinguish volatile script cache from durable libraries.
---

ffkv runs real sandboxed **Lua 5.1**. Scripts receive binary-safe KEYS/ARGV and use redis.call/pcall, cjson encode/decode/null, and bit operations. Dataset effects are atomic relative to other clients, but a runtime error does **not** roll back earlier writes.

## Evaluate and cache a script

```sh
target/release/ffkv-cli -p 6382 EVAL 'return redis.call("INCR", KEYS[1])' 1 docs:lua:counter
target/release/ffkv-cli -p 6382 SCRIPT LOAD 'return redis.call("GET", KEYS[1])'
```

The integer after source/SHA is the number of key arguments; following non-key inputs become ARGV. Use explicit keys rather than hiding routing-sensitive identities in unrelated arguments. SCRIPT LOAD returns a SHA for EVALSHA; do not hardcode an unrelated example hash. Here is an executable shell workflow that captures the actual result:

```sh
sha=$(target/release/ffkv-cli -p 6382 --raw SCRIPT LOAD 'return redis.call("GET", KEYS[1])')
target/release/ffkv-cli -p 6382 EVALSHA "$sha" 1 docs:lua:counter
```

The SHA cache is **volatile**, capped at **256 scripts / 16 MiB**, with FIFO eviction for LOAD and EVAL entries. Handle NOSCRIPT by loading/evaluating appropriate source; do not assume restart or prior loading guarantees availability. SCRIPT FLUSH SYNC/ASYNC both clear this bounded cache inline. EVAL_RO/EVALSHA_RO reject writes, including attempts through protected calls.

Legacy scripts without a shebang can use EVAL_RO/EVALSHA_RO, with writes rejected at execution. A script starting with `#!lua` must explicitly declare `flags=no-writes` for those entry points; even a body that only returns a constant is rejected without that flag. Cached SHA execution enforces the same declaration.

In cluster mode, nested script/function commands check actual key ownership and migration state, including keys derived from ARGV when the declared key count is zero. `allow-cross-slot-keys` relaxes the script's inter-call slot constraint, not ownership checks or a nested command's own multi-key restrictions. It does not permit writes to a foreign owner's local shard. The enclosing request's ASKING/READONLY routing modes apply throughout that invocation.

## Load a durable named library

Unlike the SHA cache, named library **source is durable** with persistence. This complete example sends multiline source as the final FUNCTION LOAD argument:

```sh
printf '%s\n' '#!lua name=docs_reads' "redis.register_function{function_name='docs_get',callback=function(keys,args) return redis.call('GET',keys[1]) end,flags={'no-writes'},description='Read a documentation key'}" | target/release/ffkv-cli -p 6382 -x FUNCTION LOAD
target/release/ffkv-cli -p 6382 SET docs:lua:record present
target/release/ffkv-cli -p 6382 FCALL_RO docs_get 1 docs:lua:record
target/release/ffkv-cli -3 --json -p 6382 FUNCTION LIST LIBRARYNAME docs_reads WITHCODE
```

FCALL_RO returns `present`. Function registration uses a named library and globally named callback; a no-writes flag is enforced, not simply trusted. LOAD REPLACE updates an existing library deliberately; invalid replacement cannot discard the live library or steal another library's function ownership. LIST/DELETE/FLUSH/DUMP/RESTORE manage the catalog. At most **256 libraries** are allowed, and the encoded catalog must fit one **16 MiB WAL frame**. Oversized load/restore fails before replacing live libraries.

Restart the persistent server with the same directory and call `docs_get` again: library source recovers, while the SHA cache does not. No data directory means libraries are volatile too. DUMP/RESTORE is a binary container workflow: do not use raw CLI output as a delimiter-free file export, because raw scalars receive a newline.

## Persistent connection state and debugging

One-shot shell invocations open separate connections. If changing a connection-scoped setting, keep one session:

```sh
target/release/ffkv-cli -p 6382
```

Then enter:

```text
SCRIPT DEBUG NO
EVAL "return redis.call('GET', KEYS[1])" 1 docs:lua:record
```

For cooperative debugging, DEBUG YES discards dataset effects; SYNC retains them. The debugger supports stepping, console events, and eligible SCRIPT/FUNCTION KILL. Keep debug setup/evaluation on the same connection; another connection is used for cancellation/control. A function/script that has already written cannot be safely killed as if its effects were rolled back. Disconnect cancellation terminates continued loops, including after actor queuing.

## Sandbox, error model, and limits

Filesystem/process APIs, loaders, environment/metatable access, and recursive scripting are unavailable. Invocation globals are isolated. SELECT is invocation-local. Registry NOSCRIPT restrictions apply. Blocking list commands never wait, stream reads are nonblocking, and TS.READ BLOCK is rejected.

Per invocation bounds include **1 million instructions**, **32 MiB Lua memory**, **64 nesting levels**, **8 MiB scripts/bridge values/replies**, **10,000 bridge calls**, and **16 MiB cumulative bridge argument bytes**. These do not cap the dataset or process RSS. A script that writes and then errors retains earlier writes; redis.pcall catches errors but does not undo effects.

Resolved effects—not Lua source executions or new random outcomes—are replayed by WAL recovery. Lost replies remain ambiguous; do not blindly repeat side-effecting scripts. See [EVAL](/reference/commands/eval/), [FUNCTION LOAD](/reference/commands/function-load/), [FCALL](/reference/commands/fcall/), [transactions](/guides/transactions/), and [compatibility](/concepts/compatibility/).
