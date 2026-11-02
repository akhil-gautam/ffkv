---
title: Bitfields
description: Pack fixed-width integer fields into strings with explicit wrap, saturation, or fail-on-overflow behavior.
---

Bitfields interpret a string's bits as packed signed or unsigned integers. They are a distinct family in the nineteen-family taxonomy, even though they share string storage with bitmaps. Use them for bounded counters and compact numeric records, not arbitrary-precision arithmetic.

## Read and change packed fields

```sh
target/release/ffkv-cli -p 6382 BITFIELD docs:bitfield SET u4 6 15 GET u4 6
target/release/ffkv-cli -p 6382 BITFIELD docs:bitfield OVERFLOW FAIL INCRBY u4 6 1 GET u4 6
target/release/ffkv-cli -p 6382 BITFIELD docs:bitfield OVERFLOW SAT INCRBY u4 6 1
target/release/ffkv-cli -p 6382 BITFIELD docs:bitfield OVERFLOW WRAP INCRBY u4 6 1
target/release/ffkv-cli -p 6382 BITFIELD_RO docs:bitfield GET u4 6
```

The first reply is `[0,15]`: SET returns the old value, then GET returns the new one. FAIL yields a null increment result and leaves 15 unchanged; SAT stays at 15; WRAP returns zero. OVERFLOW affects subsequent increment subcommands in that BITFIELD request; choose the policy deliberately.

Offsets are absolute bit positions. A `#` offset multiplies by the field width; quote it for shell safety:

```sh
target/release/ffkv-cli -p 6382 BITFIELD docs:bitfield:counters OVERFLOW SAT INCRBY u16 '#0' 1 INCRBY u16 '#1' 2
target/release/ffkv-cli -p 6382 BITFIELD_RO docs:bitfield:counters GET u16 '#0' GET u16 '#1'
```

A single command can perform multiple ordered suboperations atomically. Signed encodings use `i`, unsigned use `u`; `u64` is rejected. Do not assume an arbitrary width is supported merely because its bytes would fit.

## Read-only and error semantics

BITFIELD_RO accepts GET, not SET or INCRBY. Reading a missing key returns zero fields without creating it. Reading beyond available bits uses zero bits. A key of an incompatible non-string type returns WRONGTYPE. Invalid encodings/offsets/options return errors; overflow under FAIL is a null result, not a transport failure.

Packed storage is exact within the chosen width and overflow policy. It is unrelated to estimated counts in [count-min sketches](/data-types/count-min-sketch/). Overlapping fields intentionally share bits, so define a layout once and keep every writer consistent.

## Storage and limits

The string/bitmap **8 MiB** stored-value cap bounds addressed fields, and request/reply budgets also apply. Backing bytes stay RAM-resident in hybrid mode. With persistence enabled, resolved field effects recover; no data directory means no restart survival. A lost increment reply must not be blindly replayed.

See [BITFIELD](/reference/commands/bitfield/), [BITFIELD_RO](/reference/commands/bitfield-ro/), [bitmaps](/data-types/bitmaps/), [strings](/data-types/strings/), and [persistence](/operations/persistence/).
