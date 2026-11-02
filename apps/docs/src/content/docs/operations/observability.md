---
title: Observability
description: Read real server, storage and replication measurements without treating estimates or missing data as guarantees.
---

Use the native RESP diagnostics for immediate investigation, and the authenticated management plane for dashboard/API/Prometheus collection. The [console walkthrough](/getting-started/console/) provides TLS and a viewer token without exposing credentials in arguments.

## RESP diagnostics

On an unprotected isolated loopback server:

```sh
target/release/ffkv-cli -3 -p 6382 INFO
target/release/ffkv-cli -3 -p 6382 INFO replication
target/release/ffkv-cli -3 -p 6382 ROLE
target/release/ffkv-cli -3 -p 6382 FFKV.STATS
target/release/ffkv-cli -3 -p 6382 MEMORY STATS
target/release/ffkv-cli -3 -p 6382 SLOWLOG GET 20
target/release/ffkv-cli -3 -p 6382 LATENCY LATEST
```

Add the TLS/ACL options when using a protected listener. `INFO` identifies `server_name:ffkv` and its actual version. The `redis_version:7.2.0` compatibility field is a client baseline, not a claim that the server is Redis. `COMMAND COUNT` counts root commands including extensions; it is not the 486-entry catalog count, which also includes subcommands.

For native jobs, `FQ.STATS queue` provides actual ready/scheduled/running/dead counts and observed wait/run sums, not invented percentiles. For hybrid storage, compare live payload bytes with physical bytes, cache hits/misses, compactions and reclaimed bytes. Checkpoint frame sequences are file-local; do not use them as replication offsets.

## Authenticated Prometheus collection

The management `/metrics` endpoint needs the same bearer policy as `/api/*`; there is no unauthenticated metrics listener. Read the token from a private file. This example assumes the console setup:

```sh
python3 - <<'PY'
import ssl, urllib.request
from pathlib import Path
root = Path('.ffkv-docs-dev')
request = urllib.request.Request('https://localhost:6383/metrics', headers={
    'Authorization': 'Bearer ' + (root / 'console-viewer-token').read_text().strip()})
with urllib.request.urlopen(request,
        context=ssl.create_default_context(cafile=str(root / 'ca.crt')), timeout=10) as response:
    print(response.read().decode())
PY
```

Configure your collector's file-based bearer credentials and CA trust rather than a token-bearing URL. Current exported names from `src/metrics.rs` are:

| Metric | Type |
| --- | --- |
| `ffkv_commands_total`, `ffkv_errors_total` | Counters |
| `ffkv_connections`, `ffkv_blocked_clients` | Gauges |
| `ffkv_uptime_seconds`, `ffkv_wal_bytes` | Gauges |
| `ffkv_max_rss_bytes_soft`, `ffkv_rss_pressured` | Gauges |
| `ffkv_process_resident_memory_bytes` | Gauge, omitted when unavailable |
| `ffkv_command_duration_seconds_bucket`, `_count`, `_sum` | Histogram series |

Do not invent labels or assume all management fields also have a Prometheus series. Counters/histograms are since boot; account for restarts. Native job/storage detail is available through RESP and the management JSON surfaces.

## Console samples and interpretation

`/api/overview` exposes actual server, key-count, metrics and storage samples. `/api/storage` includes storage/WAL details and limits. `/api/live` sends real overview samples on a two-second tick after authentication; REST polling is the fallback. Browser history is observed session data, not synthetic or persistent server-side monitoring history.

Latency P50/P95/P99 values are approximate since-boot histogram **bucket bounds**, not exact order statistics. RSS is sampled current process memory, not a sum of key sizes. Unknown replica lag is unavailable, not zero. Development snapshot-poller lag is committed-revision distance, **not milliseconds**; inspect its source identity, connection state and first-sync status separately from native replication offsets.

## Useful alert categories

Define thresholds from your workload and recovery objective:

- rising command errors and transport failures, with explicit distinction between rejected commands and unknown outcomes;
- connection saturation, blocked-client growth and slow-consumer disconnections;
- RSS pressure or missing RSS measurements, approaching external memory limits;
- filesystem free space, WAL growth since checkpoint, derived-segment growth, backup/staging scratch;
- replica disconnection, lag/offset progress and source identity changes;
- failed or unknown management audit outcomes and inability to write the audit journal;
- backup age **and successful restore rehearsal**, not just the presence of a directory.

Do not use sensitive payload reveal as a monitoring mechanism. Audit records intentionally omit arguments/bodies/receipts. Use [troubleshooting](/operations/troubleshooting/) for safe evidence gathering and [limits](/reference/limits/) when an inspection page or response is too large.
