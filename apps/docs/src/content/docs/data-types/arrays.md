---
title: Arrays
description: Use sparse unsigned-index strings, independent insertion cursors, and bounded ring writes.
---

Arrays map unsigned indices to binary strings. Holes are first-class: the logical length, occupied count, and insertion cursor are different quantities. Unlike a list, deleting a slot does not shift later values.

## Sparse slots and holes

```sh
target/release/ffkv-cli -p 6382 ARSET docs:array 2 two three
target/release/ffkv-cli -p 6382 ARMSET docs:array 5 five 8 eight
target/release/ffkv-cli -p 6382 ARGET docs:array 4
target/release/ffkv-cli -p 6382 ARMGET docs:array 8 0 3 5
target/release/ffkv-cli -p 6382 ARGETRANGE docs:array 2 5
target/release/ffkv-cli -p 6382 ARLEN docs:array
target/release/ffkv-cli -p 6382 ARCOUNT docs:array
target/release/ffkv-cli -p 6382 ARSCAN docs:array 0 10
```

ARSET writes consecutive slots starting at its index; ARMSET assigns index/value pairs. Here length is nine and occupied count is four. ARGET at index four returns null, not an empty string. ARGETRANGE includes holes; reverse endpoints can read in reverse direction. ARSCAN returns **index/value pairs**, not a cursor-plus-page SCAN response.

ARDEL/ARDELRANGE create holes without moving survivors. A sparse high index does not imply proportional allocation for all empty slots, but a dense read across that span can still be huge.

## Cursor and ring operations

```sh
target/release/ffkv-cli -p 6382 ARINSERT docs:array:log first second
target/release/ffkv-cli -p 6382 ARSET docs:array:log 9 sparse
target/release/ffkv-cli -p 6382 ARNEXT docs:array:log
target/release/ffkv-cli -p 6382 ARSEEK docs:array:log 5
target/release/ffkv-cli -p 6382 ARINSERT docs:array:log at-five
target/release/ffkv-cli -p 6382 ARRING docs:array:ring 3 v0 v1 v2
target/release/ffkv-cli -p 6382 ARRING docs:array:ring 3 v3
```

ARINSERT uses/advances an independent cursor; assigning slot nine does not move the log cursor from two. ARSEEK changes that cursor. ARRING wraps within the supplied capacity, overwriting old slots; it is a bounded history, not a queue retaining every write. Recent-item operations preserve chronological semantics and holes as documented, including an exhausted cursor clamped to surviving tail state.

## Bounds and arithmetic

Indices extend through **2^64−2**; ARSEEK also accepts terminal **2^64−1**. Dense range reads cap at one million positions; sparse recent walks have separate work bounds. Invalid indices/options, incompatible types, and excessive work return errors. Integer aggregates preserve exact integer arithmetic; real/mixed aggregates use binary64, not platform-specific C extended precision.

Arrays stay RAM-resident in hybrid mode. Cursor, holes, values, and key deadlines participate in persistence when enabled; memory-only mode loses them on restart. See [ARSET](/reference/commands/arset/), [ARSCAN](/reference/commands/arscan/), [ARRING](/reference/commands/arring/), [lists](/data-types/lists/), and [limits](/reference/limits/).
