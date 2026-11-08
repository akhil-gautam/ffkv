---
title: Strings
description: Store binary values, atomic counters, and expiring cache entries with explicit replacement semantics.
---

A string is a binary-safe byte sequence, not necessarily text. Use it for serialized records, cache entries, exact integer counters, or the backing bytes of bitmaps/bitfields. Keys are binary-safe too.

## Write, read, and count

```sh
target/release/ffkv-cli -p 6382 SET docs:string:greeting 'hello ffkv'
target/release/ffkv-cli -p 6382 GET docs:string:greeting
target/release/ffkv-cli -p 6382 APPEND docs:string:greeting '!'
target/release/ffkv-cli -p 6382 STRLEN docs:string:greeting
target/release/ffkv-cli -p 6382 SET docs:string:counter 41
target/release/ffkv-cli -p 6382 INCR docs:string:counter
target/release/ffkv-cli -p 6382 MSET docs:string:first one docs:string:second two
target/release/ffkv-cli -p 6382 MGET docs:string:first docs:string:second
```

INCR returns 42 here. Integer arithmetic is atomic and rejects non-integers/out-of-range results without changing the value. GETRANGE/SETRANGE address byte ranges, not Unicode characters. MSET/MGET coordinate their keys atomically across shards; MGET returns null for missing or non-string keys.

## Conditions and expiry

```sh
target/release/ffkv-cli -p 6382 SET docs:string:cache fresh PX 60000 NX
target/release/ffkv-cli -p 6382 SET docs:string:cache refreshed XX KEEPTTL
target/release/ffkv-cli -p 6382 PTTL docs:string:cache
target/release/ffkv-cli -p 6382 GETDEL docs:string:cache
```

SET supports NX/XX, GET, and one expiry mode: EX, PX, EXAT, PXAT, or KEEPTTL. Ordinary SET/MSET clears an old TTL; arithmetic preserves it. A failed condition changes neither value nor TTL. SET with GET returns the previous value even if NX prevents a write. GETEX changes expiration while reading; GETDEL removes while returning the value.

SET may replace another type, but SET GET on a list fails WRONGTYPE without replacing it. GET/arithmetic reject incompatible types. An expiration deadline uses wall-clock time, including downtime; TTL/PTTL distinguish persistent (`-1`) and missing (`-2`) keys.

EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT accept `NX`, `XX`, `GT` and `LT`. `NX` conflicts with all other conditions; `GT` and `LT` conflict with each other; `XX` can combine with `GT` or `LT`.

## Limits, bytes, and durability

Stored strings/bitmap writes cap at **8 MiB**, and the 8 MiB request budget includes framing, so a single maximum-size value may be rejected earlier. Use `-x` for NUL-containing payloads:

```sh
printf 'a\000b' | target/release/ffkv-cli -p 6382 -x SET docs:string:binary
target/release/ffkv-cli -p 6382 --json GET docs:string:binary
```

String values remain RAM-resident in hybrid mode. They recover only when persistence is enabled; a lost reply to INCR is ambiguous and must not trigger blind replay. Read [CLI binary I/O](/getting-started/cli/), [persistence](/operations/persistence/), [SET reference](/reference/commands/set/), and [bitfields](/data-types/bitfields/).
