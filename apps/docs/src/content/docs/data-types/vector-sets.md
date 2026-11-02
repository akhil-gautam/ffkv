---
title: Vector sets
description: Store embeddings and JSON attributes for bounded HNSW nearest-neighbor search.
---

Vector sets associate unique member names with embeddings and optional JSON attributes. ffkv implements multilayer **HNSW** with quantization/projection options. Ordinary similarity search is approximate: it is not guaranteed to visit every vector or return the mathematically exact nearest neighbors.

## Insert and query embeddings

```sh
target/release/ffkv-cli -p 6382 VADD docs:vectors VALUES 3 1 0 0 east NOQUANT SETATTR '{"zone":"east"}'
target/release/ffkv-cli -p 6382 VADD docs:vectors VALUES 3 0 1 0 north NOQUANT SETATTR '{"zone":"north"}'
target/release/ffkv-cli -p 6382 VDIM docs:vectors
target/release/ffkv-cli -p 6382 VCARD docs:vectors
target/release/ffkv-cli -3 --json -p 6382 VSIM docs:vectors ELE east COUNT 1 WITHSCORES WITHATTRIBS
target/release/ffkv-cli -p 6382 VSIM docs:vectors VALUES 3 1 0 0 FILTER '.zone == "north"' COUNT 1 FILTER-EF 0
```

VALUES supplies a dimension count followed by coordinates; ELE queries using an existing member. This tiny example uses NOQUANT to keep the example easy to inspect. Real embeddings must use a consistent dimension and compatible construction options. Similarity scores are not confidence probabilities.

VSETATTR/VGETATTR change/read JSON attributes independently of embedding retrieval with VEMB. Filter syntax addresses those attributes, not arbitrary source keys. FILTER-EF controls filter-search effort; valid filters can still affect recall/work. Invalid filter expressions return an error rather than silently matching everything.

## Recall and inspection

```sh
target/release/ffkv-cli -p 6382 VSIM docs:vectors ELE east COUNT 2 WITHSCORES TRUTH
target/release/ffkv-cli -p 6382 VINFO docs:vectors
target/release/ffkv-cli -p 6382 VLINKS docs:vectors east
```

TRUTH explicitly requests exhaustive search and can be useful for comparing recall on a bounded dataset. It is a different cost model from ordinary HNSW traversal. Tune M/EF and quantization/projection for the actual data; do not infer benchmark quality or production recall from two orthogonal vectors. VLINKS/VINFO expose actual graph metadata, not Redis allocator sizes.

VRANGE enumerates members lexically; VRANDMEMBER samples identities; VREM deletes an embedding. These are distinct from similarity ranking. Replacing/deleting members changes the graph and subsequent approximate results.

## Limits and durability

Bounds include **65,536 dimensions**, **64 KiB members**, **1 MiB attributes**, bounded M/EF/search work, and **1 GiB estimated allocation per set**. Request/reply budgets may be lower. Dimension/type/option violations return errors. This is not a process RSS cap.

Graphs remain RAM-resident in hybrid mode. WAL/checkpoint recovery records resolved effects, including graph changes, when persistence is enabled; no data directory means no restart survival. [Search indexing](/guides/search/) is separate from vector-set storage. See [VADD](/reference/commands/vadd/), [VSIM](/reference/commands/vsim/), [limits](/reference/limits/), and [memory](/operations/memory/).
