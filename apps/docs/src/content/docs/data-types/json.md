---
title: JSON
description: Store ordered document trees and use JSONPath reads, filters, and atomic mutations.
---

JSON keys contain parsed document trees rather than opaque JSON strings. Objects preserve ordering, and path operations address selected nodes. Use this family when applications need nested updates without a read/modify/rewrite round trip.

## Store and select a document

```sh
target/release/ffkv-cli -p 6382 JSON.SET docs:json '$' '{"name":"Ada","items":[{"price":2},{"price":9}],"active":true}'
target/release/ffkv-cli -p 6382 JSON.GET docs:json '$'
target/release/ffkv-cli -p 6382 JSON.GET docs:json '$.items[?(@.price > 5)]'
target/release/ffkv-cli -p 6382 JSON.TYPE docs:json '$.items'
target/release/ffkv-cli -p 6382 JSON.NUMINCRBY docs:json '$.items[*].price' 1
target/release/ffkv-cli -p 6382 JSON.TOGGLE docs:json '$.active'
```

Quote paths and JSON to protect `$`, brackets, spaces, and quotes from the shell. JSONPath (`$...`) can select multiple nodes, so response shape can contain multiple matches even when the application expects one. Legacy paths have different selection/response conventions; do not silently switch path styles in a client.

Read selectors preserve occurrence order and duplicates: selecting `$[1,0,1]` from `[10,20]` returns `[20,10,20]`, including through JSON.MGET. Mutation targets are distinct: the same selector increments or deletes each addressed node only once, rather than applying the update twice to index one.

JSON.SET NX/XX conditionally assigns according to path existence. New documents are created at the root; setting a nested path is not a general-purpose missing-parent constructor. JSON values must be valid JSON: a document string needs JSON quotes inside the shell argument.

## Structured mutation

```sh
target/release/ffkv-cli -p 6382 JSON.ARRAPPEND docs:json '$.items' '{"price":12}'
target/release/ffkv-cli -p 6382 JSON.ARRLEN docs:json '$.items'
target/release/ffkv-cli -p 6382 JSON.OBJKEYS docs:json '$'
target/release/ffkv-cli -p 6382 JSON.GET docs:json '$.items[*].price'
```

ARRINSERT/ARRPOP/ARRTRIM work on arrays; STRAPPEND/STRLEN on strings; numeric and boolean commands on matching types. JSON.MERGE applies merge-patch semantics, while DEL/FORGET removes selected nodes and CLEAR resets eligible values; these are not synonyms. JSON.MSET coordinates multi-document writes atomically. A path matching nothing is different from an invalid expression or wrong node/key type.

## Limits and durability

Compact documents cap at **4 MiB**, **262,144 nodes**, and **depth 128**. Paths cap at 16 KiB, matches at 65,536, and expression/projection work is bounded. Request/reply budgets may reject work earlier. These bounds are not a promise that every RedisJSON option or wire shape is identical.

Documents remain RAM-resident in hybrid mode. Key TTL, WATCH, ACL, WAL, checkpoints, and backups apply, but restart survival requires persistence. A lost mutation reply is ambiguous; prefer idempotent assignments over blind replay of numeric increments/appends.

[Search](/guides/search/) indexes source JSON documents separately; index failure does not necessarily mean the source write failed. See [JSON.SET](/reference/commands/json-set/), [JSON.GET](/reference/commands/json-get/), [hashes](/data-types/hashes/), and [compatibility](/concepts/compatibility/).
