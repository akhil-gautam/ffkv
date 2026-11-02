---
title: Search indexes
description: Index hash or JSON documents and query text, numeric, and tag fields without confusing indexes with source data.
---

Search is a separate indexing capability over source documents, **not a twentieth datatype**. Search catalog records are independent of document keys. Source writes maintain real indexes, and creating an index can scan eligible existing documents. Use CLI commands for the complete capability; the management key browser is not a full search UI.

## Index existing hashes

Use a fresh prefix/index name on the [quickstart](/getting-started/quickstart/) instance:

```sh
target/release/ffkv-cli -p 6382 HSET docs:search:1 title 'red shoes' price 10 tags sale,red
target/release/ffkv-cli -p 6382 HSET docs:search:2 title 'blue shoes' price 20 tags sale,blue
target/release/ffkv-cli -p 6382 FT.CREATE docs_products ON HASH PREFIX 1 docs:search: SCHEMA title TEXT price NUMERIC SORTABLE tags TAG
target/release/ffkv-cli -p 6382 FT.SEARCH docs_products '@title:shoes @price:[(10 +inf] @tags:{sale}' NOCONTENT DIALECT 2
target/release/ffkv-cli -p 6382 FT.SEARCH docs_products shoes NOCONTENT SORTBY price ASC LIMIT 0 10
target/release/ffkv-cli -3 --json -p 6382 FT.INFO docs_products
```

The typed intersection selects only document two because the lower numeric bound excludes ten. NOCONTENT omits source fields; it does not mean count-only. LIMIT gives offset/count; the reported total is distinct from the returned page length. Quote queries to protect spaces, parentheses, braces, and `*` from shell processing.

SCHEMA determines how fields are interpreted. A plain string containing numbers is not automatically a numeric index, and TAG matching is not the same as text tokenization. SORTABLE is an explicit schema property, not a promise that every arbitrary source field can be cheaply sorted.

Numeric indexes treat `-0` and `0` as equal, including inclusive/exclusive range bounds. Empty TAG queries such as `@tags:{""}` require `INDEXEMPTY` on that schema field; empty postings follow initial scans, field updates, deletion, and index recreation.

## Source mutation and indexing failure

```sh
target/release/ffkv-cli -p 6382 HSET docs:search:1 title 'new boots'
target/release/ffkv-cli -p 6382 FT.SEARCH docs_products shoes NOCONTENT
target/release/ffkv-cli -p 6382 HSET docs:search:1 price not-a-number
target/release/ffkv-cli -p 6382 HGET docs:search:1 price
target/release/ffkv-cli -3 --json -p 6382 FT.INFO docs_products
target/release/ffkv-cli -p 6382 HSET docs:search:1 price 12
```

Old postings are removed when an indexed field changes. An invalid numeric value can reject the document from the index **without rolling back HSET**; HGET still returns `not-a-number`, while INFO reports indexing failure and the document is not searchable. Repairing the value makes the document eligible again. Monitor indexing failures rather than interpreting a successful source write as proof of successful indexing.

ON JSON indexes use JSONPath schema selectors/aliases over [JSON documents](/data-types/json/), rather than pretending a JSON document is a hash. Vector search within a document index is also separate from the [vector-set](/data-types/vector-sets/) VADD/VSIM API; configure the intended representation explicitly.

## Aggregations, aliases, and lifecycle

```sh
target/release/ffkv-cli -p 6382 FT.AGGREGATE docs_products '*' GROUPBY 0 REDUCE COUNT 0 AS documents
target/release/ffkv-cli -p 6382 FT.ALIASADD docs_products_current docs_products
target/release/ffkv-cli -p 6382 FT.SEARCH docs_products_current boots NOCONTENT
target/release/ffkv-cli -p 6382 FT.ALIASDEL docs_products_current
```

Aggregation stages execute in order: LOAD/APPLY/FILTER/GROUPBY/REDUCE/SORTBY/LIMIT placement changes results. An approximate reducer such as COUNT_DISTINCTISH is not an exact distinct count. Cursor reads advance real pipeline state and require the actual returned cursor, not a fabricated numeric ID; free cursors you abandon and handle expiry/errors.

An unbounded SORTBY stage uses a default ten-row bound, including when a later stage groups its output. Set the intended sort bound explicitly rather than assuming an intermediate sort retains every match.

Aliases rebind metadata, not document keys. FT.ALTER validates schema changes and can rescan existing sources. FT.DROPINDEX without DD removes the index while preserving source documents; **DD explicitly deletes indexed source keys**. Treat it as destructive data administration, not ordinary index cleanup.

## Operational boundaries

Search metadata participates in snapshots/resolved-effect recovery with persistence; no data directory means no restart survival. Index structures are not automatically SSD-offloaded in hybrid mode. Search worker configuration uses a real Rayon pool; GC configuration schedules generation-fenced Rust compaction, not POSIX fork behavior. Memory statistics are ffkv allocation estimates, not Redis allocator parity.

Query/expression/cursor/work and reply budgets are bounded. Invalid syntax/options fail rather than silently weakening predicates. Broad queries, initial scans, and coordinated index changes can affect other workload latency. Catalog coverage is not exhaustive RediSearch option/ABI compatibility.

See [FT.CREATE](/reference/commands/ft-create/), [FT.SEARCH](/reference/commands/ft-search/), [FT.AGGREGATE](/reference/commands/ft-aggregate/), [FT.DROPINDEX](/reference/commands/ft-dropindex/), [limits](/reference/limits/), and [compatibility](/concepts/compatibility/).
