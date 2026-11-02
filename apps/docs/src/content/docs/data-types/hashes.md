---
title: Hashes
description: Store binary field/value records with atomic field updates and independent field expiration.
---

Hashes map binary field names to binary values under one key. They suit mutable records where individual attributes should be read or changed without replacing a whole serialized object. Field identity is exact; field order is not an application contract.

## Read and update a record

```sh
target/release/ffkv-cli -p 6382 HSET docs:hash:profile name Ada plan builder visits 0
target/release/ffkv-cli -p 6382 HGET docs:hash:profile name
target/release/ffkv-cli -p 6382 HMGET docs:hash:profile name missing
target/release/ffkv-cli -p 6382 HINCRBY docs:hash:profile visits 1
target/release/ffkv-cli -3 --json -p 6382 HGETALL docs:hash:profile
target/release/ffkv-cli -p 6382 HSCAN docs:hash:profile 0 COUNT 10
```

HSET reports fields added, not all fields overwritten. HSETNX changes only a missing field. HDEL removes named fields; HLEN counts live fields. HINCRBY requires an integer and rejects invalid/out-of-range arithmetic. HGETALL is a full response; HSCAN is preferable for large records. Its cursor is opaque and COUNT is a hint; NOVALUES can request names only.

## Field TTL is not key TTL

```sh
target/release/ffkv-cli -p 6382 HSETEX docs:hash:profile PX 60000 FIELDS 1 token temporary
target/release/ffkv-cli -p 6382 HPTTL docs:hash:profile FIELDS 1 token
target/release/ffkv-cli -p 6382 HEXPIRE docs:hash:profile 30 FIELDS 1 token
target/release/ffkv-cli -p 6382 HPERSIST docs:hash:profile FIELDS 1 token
```

Expiry operations return per-field arrays. Seconds-based field TTL inspection rounds upward. HEXPIRE/HPEXPIRE and absolute variants support one of NX/XX/GT/LT; HSETEX supports FNX/FXX and one expiration mode or KEEPTTL. FIELDS counts fields; HSETEX follows each field with its value.

Ordinary HSET clears an overwritten field's TTL. HINCRBY preserves it. Reads treat expired fields as absent; removing/expiring the final field removes the hash key. The key may separately have its own TTL, which removes the whole record regardless of later field deadlines. Offline time counts for both.

## Boundaries and related models

A hash operation on another datatype returns WRONGTYPE. Hash values remain RAM-resident in hybrid mode. Persistence records resolved updates and deadlines, but requires a data directory. A lost reply to an increment can mean it already committed; do not blindly retry.

Use [JSON](/data-types/json/) for nested document/path operations. [Search](/guides/search/) can index eligible hash fields, but an indexing failure does not undo HSET. See [HSET](/reference/commands/hset/), [HSETEX](/reference/commands/hsetex/), and [persistence](/operations/persistence/).
