---
title: Cuckoo filters
description: Manage compact fingerprint membership with counting, deletion, and bounded relocation.
---

Cuckoo filters store compact fingerprints in buckets, using relocation and optional expansion to place new entries. Membership and counts are **probabilistic**: two different values can share a fingerprint. Unlike Bloom filters, Cuckoo filters support deletion, but that makes application discipline particularly important.

## Add, count, and remove

```sh
target/release/ffkv-cli -p 6382 CF.RESERVE docs:cuckoo 128
target/release/ffkv-cli -p 6382 CF.ADD docs:cuckoo alice
target/release/ffkv-cli -p 6382 CF.ADD docs:cuckoo alice
target/release/ffkv-cli -p 6382 CF.EXISTS docs:cuckoo alice
target/release/ffkv-cli -p 6382 CF.COUNT docs:cuckoo alice
target/release/ffkv-cli -p 6382 CF.DEL docs:cuckoo alice
target/release/ffkv-cli -p 6382 CF.COUNT docs:cuckoo alice
target/release/ffkv-cli -3 --json -p 6382 CF.INFO docs:cuckoo
```

ADD permits duplicate fingerprints; DEL removes one matching occurrence. CF.COUNT counts matching fingerprints, **not an exact authoritative frequency** of the original bytes. EXISTS may return a false positive. ADDNX/INSERTNX avoid adding something already judged present, but a false positive can reject a genuinely new item.

Only delete values your application knows it successfully inserted and has not already removed. Deleting an absent item that collides can remove another item's fingerprint and compromise later membership answers. An exact ledger is needed if correctness depends on deletion identity.

## Sizing and failure

CF.RESERVE accepts capacity plus controls such as BUCKETSIZE, MAXITERATIONS, and EXPANSION. They trade fingerprint collision behavior, insertion work, and growth. Saturation can cause insertion failure even when an application expected remaining space; relocation is bounded rather than an unbounded retry loop. A nonexpanding filter cannot grow forever.

Backing arrays are capped at **64 MiB per key**. Bucket size is 1–255; relocation iterations are 1–65,535; expansion/capacity and request work are additionally bounded. Invalid values/options and incompatible key types return errors. Check the actual command result before updating an application's insertion ledger.

## Durability and export

Resolved bucket changes, including random relocation choices, are journaled when persistence is enabled. Recovery does not rerun relocation randomly. Without a data directory, nothing survives restart; hybrid mode still keeps this family in RAM.

CF.SCANDUMP/CF.LOADCHUNK uses ffkv's versioned checksummed **FFKF** stream, not RedisBloom native memory. Freeze the source during export and use only ffkv-to-ffkv roundtrips. Shared command names do not imply packed dump compatibility.

See [CF.RESERVE](/reference/extensions/#cfreserve), [CF.COUNT](/reference/extensions/#cfcount), [CF.DEL](/reference/extensions/#cfdel), [Bloom filters](/data-types/bloom/), and [exact sets](/data-types/sets/).
