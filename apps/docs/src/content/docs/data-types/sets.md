---
title: Sets
description: Maintain exact unique membership, random samples, and atomic set algebra.
---

A set holds unique binary members without a defined iteration order. Unlike a Bloom/Cuckoo filter, SISMEMBER answers exact membership and SCARD is the exact number of members. Choose sets when false positives are unacceptable and the member identities fit in RAM.

## Membership and sampling

```sh
target/release/ffkv-cli -p 6382 SADD docs:set:active alice bob alice
target/release/ffkv-cli -p 6382 SCARD docs:set:active
target/release/ffkv-cli -p 6382 SISMEMBER docs:set:active alice
target/release/ffkv-cli -p 6382 SMISMEMBER docs:set:active alice carol
target/release/ffkv-cli -p 6382 SMEMBERS docs:set:active
target/release/ffkv-cli -p 6382 SRANDMEMBER docs:set:active
target/release/ffkv-cli -p 6382 SPOP docs:set:active 1
```

The initial cardinality is two, not three. SRANDMEMBER samples without deleting; SPOP removes. Do not expect stable sample values or SMEMBERS order. SREM removes named members; SMOVE atomically transfers one member between sets.

## Algebra

```sh
target/release/ffkv-cli -p 6382 SADD docs:set:engineering alice bob
target/release/ffkv-cli -p 6382 SADD docs:set:oncall bob carol
target/release/ffkv-cli -p 6382 SINTER docs:set:engineering docs:set:oncall
target/release/ffkv-cli -p 6382 SUNIONSTORE docs:set:everyone docs:set:engineering docs:set:oncall
target/release/ffkv-cli -p 6382 SSCAN docs:set:everyone 0 COUNT 10
```

The intersection contains bob. SUNION/SINTER/SDIFF return results; STORE variants replace a destination. Source types are validated before replacement, including when a destination overlaps a source. Cross-key algebra is atomic across shards.

SSCAN returns an opaque cursor and a page. Continue with the returned cursor until zero; COUNT is a hint, not an exact page size. It is not a snapshot, so concurrent mutations can affect observations. Avoid whole-set responses for large collections.

## Errors and storage

Operations on incompatible types return WRONGTYPE. Memory and response limits still apply even if every individual member is small; oversized destructive operations are rejected before removal rather than silently truncating results. Member identities remain in RAM in hybrid mode.

With persistence enabled, SPOP journals the **actual selected members**. Recovery does not draw a new random sample. Without a data directory, no set survives restart. A disconnected SPOP caller cannot assume nothing was removed; inspect state before retrying.

See [SADD](/reference/commands/sadd/), [SINTERSTORE](/reference/commands/sinterstore/), [persistence](/operations/persistence/), and [Bloom filters](/data-types/bloom/) for the memory/accuracy tradeoff.
