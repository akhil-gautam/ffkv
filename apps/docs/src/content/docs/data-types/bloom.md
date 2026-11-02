---
title: Bloom filters
description: Use scalable probabilistic membership as a compact prefilter, with explicit capacity and interoperability limits.
---

Bloom filters encode membership in bit arrays. A negative result means the item was not inserted into the intact filter; a positive result means **possibly present**, because false positives are expected. They cannot enumerate members or delete an individual item. Use them to skip unnecessary authoritative lookups, not to enforce uniqueness or access control.

## Reserve and query

```sh
target/release/ffkv-cli -p 6382 BF.RESERVE docs:bloom 0.01 1000
target/release/ffkv-cli -p 6382 BF.ADD docs:bloom alice
target/release/ffkv-cli -p 6382 BF.MADD docs:bloom bob carol
target/release/ffkv-cli -3 --json -p 6382 BF.EXISTS docs:bloom alice
target/release/ffkv-cli -p 6382 BF.MEXISTS docs:bloom alice dave
target/release/ffkv-cli -3 --json -p 6382 BF.INFO docs:bloom
```

RESERVE takes error rate then initial capacity. Here 0.01 is a configured false-positive target, not a guarantee for every query/data distribution. ADD reports whether the filter changed, not authoritative proof of a newly unique member: a false positive can make an unseen item look already present.

BF.ADD can create a filter implicitly, but explicit reserve makes sizing visible. BF.INSERT supports batched items and creation controls; NOCREATE is useful when silently creating a missing filter would hide a deployment mistake. Non-scaling and expansion settings determine whether capacity can grow.

## Capacity and errors

Scalable filters add subfilters as necessary under bounded expansion. NONSCALING makes exhaustion an error rather than unbounded growth; it conflicts with a nonzero explicit expansion. Error rates must be finite and strictly between zero and one, capacity positive, and backing arrays fit a **64 MiB per-key** bound. Hash-count/expansion/work limits and request budgets also apply. Existing incompatible types return WRONGTYPE.

Do not interpret BF.INFO item counts as an exact distinct-member inventory. To expire a time window, expire/delete the entire filter or use rotating keys. If false positives would drop real work, check an exact store before making that decision; a Bloom filter is not an idempotency ledger.

## Durability and interchange

With a data directory, resolved filter mutations participate in WAL/checkpoints/backups; without it the filter is volatile. Hybrid mode does not move filter arrays to SSD. SCANDUMP/LOADCHUNK uses ffkv's versioned checksummed **FFKF** format with bounded pages, **not RedisBloom's packed-memory dump format**. Freeze writes during an export; use ffkv-to-ffkv roundtrips only, not RedisBloom binary migration.

See [BF.RESERVE](/reference/extensions/#bfreserve), [BF.INSERT](/reference/extensions/#bfinsert), [sets](/data-types/sets/), [Cuckoo filters](/data-types/cuckoo/), and [migration](/operations/migration/).
