---
title: Bitmaps
description: Address exact boolean flags in string bytes and combine them with bitmap operations.
---

A bitmap is a string interpreted as addressed bits. It has no separate storage type: the same bytes are visible through string commands. Bit offsets are zero-based, most-significant bit first within a byte. Use bitmaps when identifiers map naturally to a dense bounded integer domain; a single very large offset still allocates the intervening bytes.

## Set, count, and locate flags

```sh
target/release/ffkv-cli -p 6382 SETBIT docs:bitmap:day1 0 1
target/release/ffkv-cli -p 6382 SETBIT docs:bitmap:day1 9 1
target/release/ffkv-cli -p 6382 GETBIT docs:bitmap:day1 9
target/release/ffkv-cli -p 6382 BITCOUNT docs:bitmap:day1
target/release/ffkv-cli -p 6382 BITCOUNT docs:bitmap:day1 0 7 BIT
target/release/ffkv-cli -p 6382 BITPOS docs:bitmap:day1 1
```

SETBIT returns the previous bit. The total count here is two; the inclusive bit range 0–7 contains one. Missing bitmaps read as zero without creating a key. BITCOUNT ranges default to bytes; specify BIT to address bit units. BITPOS distinguishes searching for set bits from the implicit zero padding beyond stored bytes; explicit ranges constrain the search.

## Combine activity windows

```sh
target/release/ffkv-cli -p 6382 SETBIT docs:bitmap:day2 9 1
target/release/ffkv-cli -p 6382 SETBIT docs:bitmap:day2 10 1
target/release/ffkv-cli -p 6382 BITOP AND docs:bitmap:both docs:bitmap:day1 docs:bitmap:day2
target/release/ffkv-cli -p 6382 BITCOUNT docs:bitmap:both
target/release/ffkv-cli -p 6382 BITOP OR docs:bitmap:either docs:bitmap:day1 docs:bitmap:day2
```

The intersection count is one. BITOP stores a destination and returns its byte length, not the count of set bits. Sources are zero-padded as required by the operation; NOT is unary. The registry/reference also describes newer DIFF, DIFF1, ANDOR, and ONE operations. Destination/source aliases use original source data.

## Boundaries and durability

Stored strings/bitmaps cap at **8 MiB**; offsets and results must fit that bound. Invalid bits/offsets/options fail, and incompatible non-string keys return WRONGTYPE. String SET can overwrite a bitmap; GET can expose nonprinting bytes, so prefer the CLI's JSON or escaped human output.

These are exact bit operations, unlike probabilistic membership filters. The backing bytes remain in RAM even with hybrid storage. With a data directory, mutations and resulting bytes are durable under the selected policy; without it they vanish on restart. Lost write replies remain ambiguous.

See [SETBIT](/reference/commands/setbit/), [BITOP](/reference/commands/bitop/), [bitfields](/data-types/bitfields/), [Bloom filters](/data-types/bloom/), and [CLI byte handling](/getting-started/cli/).
