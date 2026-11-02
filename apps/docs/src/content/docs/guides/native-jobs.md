---
title: Native leased jobs
description: Enqueue, claim, acknowledge, retry, and inspect durable FQ queues with at-least-once semantics.
---

Native `FQ.*` queues are separate from Redis lists; TYPE reports `ffkv-queue`. Claims grant receipts and visibility leases rather than deleting work immediately. They provide **at-least-once execution**, not exactly-once external side effects. Make handlers idempotent and extend leases before expiry.

Start the persistent [quickstart](/getting-started/quickstart/) instance first. Without a data directory, queue/lease/idempotency state cannot survive restart.

## Configure and enqueue

```sh
target/release/ffkv-cli -3 --json -p 6382 FQ.CONFIG docs:native CONCURRENCY 4 MAXATTEMPTS 3 BACKOFF 1000 MAXBACKOFF 60000
target/release/ffkv-cli -3 --json -p 6382 FQ.ENQUEUE docs:native '{"job":"print","id":1}' IDEMPOTENCY print-1
target/release/ffkv-cli -3 --json -p 6382 FQ.STATS docs:native
target/release/ffkv-cli -3 --json -p 6382 FQ.JOBS docs:native 0 COUNT 20 STATE ready
```

The idempotency token deduplicates enqueue requests within its retention window. A duplicate returns the original job ID with `created: 0`; it does not replace the payload. Higher PRIORITY wins, with equal-priority eligible jobs in original enqueue order. DELAY uses milliseconds; AT uses an absolute epoch-millisecond time.

## A complete demo consumer

This consumer's actual work is printing the JSON document. It uses the returned receipt rather than a guessed job ID. Run from the repository root:

```sh
python3 - <<'PY'
import json
import subprocess

def kv(*args):
    return json.loads(subprocess.check_output(
        ["target/release/ffkv-cli", "-3", "--json", "-p", "6382", *map(str, args)],
        text=True))

jobs = kv("FQ.CLAIM", "docs:native", "COUNT", 1, "VISIBILITY", 60000)
for job in jobs:
    payload = json.loads(job["payload"])
    if payload["job"] != "print":
        raise ValueError("This demo only processes print jobs")
    print(json.dumps(payload), flush=True)
    print(kv("FQ.ACK", "docs:native", job["receipt"]))
if not jobs:
    print("No eligible job; CLAIM does not block")
PY
```

An application should invoke its real idempotent handler before ACK; printing is not an email/payment implementation. Receipts are capabilities: do not put them in logs. If a handler fails, NACK the actual receipt with optional DELAY/ERROR; if work takes longer, EXTEND that receipt with a new visibility duration. EXTEND returns the new absolute deadline, not OK. Capture actual returned job IDs for GET/RETRY/CANCEL rather than treating sample IDs as interchangeable.

## Lease and retry semantics

CLAIM increments attempts and returns body, job ID, receipt, and deadline. Default visibility is 30 seconds; visibility and extension are bounded to **24 hours**. At the exact deadline the old lease has expired. Expired, forged, old-generation, or old-epoch receipts cannot acknowledge, reject, or extend the current lease.

NACK/lease expiration schedules bounded exponential-backoff retries until max attempts, then retains a dead job. RETRY redrives a dead job, resets attempts, and preserves receipt non-reuse. CANCEL removes a job from ready/scheduled/running/dead. Concurrency is per queue; zero is unlimited. Default max attempts is 25. Changing queue max attempts affects future jobs, not existing ones.

Idempotency retention defaults to five minutes, caps at 30 days, and survives terminal ACK/cancel and restart. It deduplicates **enqueues**, not an external side effect that succeeded before a worker crashed. An expired token can allow another job with the same token.

## Inspection and limits

JOBS pages list metadata; GET/CLAIM fetch bodies. Pages/claims cap at 1,000 rows, queues at one million jobs/idempotency records, and a CLAIM at 32 MiB logical body bytes (the caller's reply limit may be lower). Queue stats are actual counts/observed wait/run sums, not synthetic percentiles. Empty queues retain policy/counters until DEL/TTL; a missing-queue policy read does not create a queue.

Strict persistence journals resolved transitions before reply. Lost claim/ACK replies remain ambiguous: do not blindly repeat destructive operations. Large bodies may be offloaded by [hybrid storage](/operations/hybrid-storage/), but queue metadata remains in RAM. See the [complete FQ command forms](/reference/extensions/), [list queues](/guides/list-queues/), and [backup/restore](/operations/backup-restore/).
