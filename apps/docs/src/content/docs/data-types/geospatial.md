---
title: Geospatial indexes
description: Index longitude/latitude points and query distances, radii, and boxes.
---

Geospatial indexes store named points as Morton-encoded scores in a sorted set. They support proximity queries, not routing, road-network distance, or arbitrary polygon geometry. Member identities are exact; decoded coordinates and computed spherical distances have encoding/floating-point precision limits.

## Add points and measure distance

```sh
target/release/ffkv-cli -p 6382 GEOADD docs:geo 13.361389 38.115556 Palermo 15.087269 37.502669 Catania
target/release/ffkv-cli -p 6382 GEOPOS docs:geo Palermo Catania
target/release/ffkv-cli -p 6382 GEOHASH docs:geo Palermo
target/release/ffkv-cli -p 6382 GEODIST docs:geo Palermo Catania km
```

Coordinates are **longitude then latitude**. Palermo–Catania is approximately 166.27 km in this model, not a travel-distance promise. Longitude must be between -180 and 180 degrees; latitude is bounded to approximately ±85.05112878. Invalid coordinates fail rather than inserting an unusable point. Missing members return null where applicable.

## Search a radius or box

```sh
target/release/ffkv-cli -p 6382 GEOSEARCH docs:geo FROMMEMBER Palermo BYRADIUS 200 km ASC WITHDIST
target/release/ffkv-cli -p 6382 GEOSEARCH docs:geo FROMLONLAT 15 37 BYBOX 400 400 km ASC WITHCOORD
target/release/ffkv-cli -p 6382 GEOSEARCHSTORE docs:geo:nearby docs:geo FROMMEMBER Palermo BYRADIUS 200 km
```

Choose either member or coordinate center, then a radius or width/height box. Supported units include m, km, ft, and mi. ASC/DESC orders by distance; COUNT bounds returned matches. GEOSEARCHSTORE replaces a destination sorted set. STOREDIST changes stored scores to distances rather than coordinate encodings: that destination is not then an ordinary geo index.

GEOADD's NX avoids moving existing members; XX updates only existing ones; inspect CH and valid option combinations in the reference. Remove a member using ZREM. Legacy GEORADIUS variants are available, but GEOSEARCH expresses new queries more clearly.

## Representation and operations

Because storage is a sorted set, TYPE reports that underlying family and ordinary sorted-set commands can affect it. Do not mix arbitrary ZADD scores into a geo key: the scores encode coordinates, not a leaderboard. A non-sorted-set key returns WRONGTYPE. Large result sets remain subject to reply/work budgets.

The index stays in RAM in hybrid mode. Key TTL and persistence apply like other families; no data directory means no restart durability. Recovery records resolved scores, so coordinates are not regenerated from a later search.

See [GEOADD](/reference/commands/geoadd/), [GEOSEARCH](/reference/commands/geosearch/), [sorted sets](/data-types/sorted-sets/), and [search indexing](/guides/search/) for broader document queries.
