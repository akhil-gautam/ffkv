---
title: t-digest
description: Summarize measurement distributions using weighted centroids, quantiles, CDFs, and merges.
---

t-digest compresses numeric observations into weighted centroids for approximate distribution queries. Use it for latency/size percentiles where retaining every sample is unnecessary. It does not preserve the original sample list, exact ordering, or an exact percentile result for arbitrary data.

## Add measurements and query a distribution

```sh
target/release/ffkv-cli -p 6382 TDIGEST.CREATE docs:tdigest COMPRESSION 100
target/release/ffkv-cli -p 6382 TDIGEST.ADD docs:tdigest 0 100 200
target/release/ffkv-cli -p 6382 TDIGEST.QUANTILE docs:tdigest 0.5 0.95
target/release/ffkv-cli -p 6382 TDIGEST.CDF docs:tdigest 100
target/release/ffkv-cli -p 6382 TDIGEST.MIN docs:tdigest
target/release/ffkv-cli -p 6382 TDIGEST.MAX docs:tdigest
target/release/ffkv-cli -3 --json -p 6382 TDIGEST.INFO docs:tdigest
```

QUANTILE maps fractions in [0,1] to estimated values; CDF maps values to estimated cumulative fractions. Rank/reverse-rank and by-rank forms answer related distribution questions, not indexed access to a stored array. TRIMMED_MEAN estimates a mean between two quantiles; its low fraction must be below its high fraction.

Observations must be finite numbers: NaN and infinities are rejected on ADD. Pick consistent measurement units before ingestion; merging milliseconds and seconds yields a meaningless distribution even if commands succeed.

## Merge and reset

```sh
target/release/ffkv-cli -p 6382 TDIGEST.CREATE docs:tdigest:other
target/release/ffkv-cli -p 6382 TDIGEST.ADD docs:tdigest:other 300 400
target/release/ffkv-cli -p 6382 TDIGEST.MERGE docs:tdigest:combined 2 docs:tdigest docs:tdigest:other COMPRESSION 100 OVERRIDE
target/release/ffkv-cli -p 6382 TDIGEST.QUANTILE docs:tdigest:combined 0.5
```

MERGE combines centroid summaries, not reconstructed original samples. OVERRIDE replaces destination history instead of combining with an existing destination; this distinction matters on repeated executions. Compression affects memory/accuracy tradeoffs and merge behavior, not a universal error guarantee. RESET clears observations while preserving a digest for reuse.

## Limits and durability

Compression defaults to 100 and caps at **1,000,000**, with bounded centroid storage and observation-count overflow checks. CREATE requires a missing key. Invalid quantiles/ranks, incompatible types, and excessive numeric/allocation work return errors. Empty-distribution query results should not be interpreted as actual zero-valued measurements.

Digests remain RAM-resident in hybrid mode. With persistence, resolved centroids/counts recover without rerunning an approximate merge; without a data directory, they disappear. A lost ADD reply is ambiguous and repeating it changes the sample weights.

Use [time series](/data-types/time-series/) when timestamps and retained samples matter. See [TDIGEST.CREATE](/reference/extensions/#tdigestcreate), [TDIGEST.QUANTILE](/reference/extensions/#tdigestquantile), [TDIGEST.MERGE](/reference/extensions/#tdigestmerge), and [limits](/reference/limits/).
