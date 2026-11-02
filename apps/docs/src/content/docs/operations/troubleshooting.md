---
title: Troubleshooting
description: Diagnose connection, recovery, memory, management and replication problems without destructive retries or unsafe repair.
---

Start with the exact error, timestamp, server/CLI version, listener and data-directory identity. Preserve logs and original storage before attempting repair. Never restart an unrelated server, delete a lock to bypass its owner, or retry an ambiguous non-idempotent command blindly.

## Connection and authentication

| Symptom | Check | Safe action |
| --- | --- | --- |
| Connection refused / wrong server | The example uses RESP 6382; the client defaults to 6380 and server defaults to 6379 | Pass `-p 6382`; check the startup bind line and process you own |
| TLS negotiation failure | RESP TLS flags, CA trust, certificate expiry and SAN | Use matching `--tls --cacert ... -h localhost` or `--sni`; replace invalid certs, never bypass verification |
| NOAUTH / WRONGPASS | Named ACL user/password vs console bearer token | Use `--user ... --askpass`; supply the RESP password, not a management token |
| Management 401 | Raw bearer token vs hashed policy entry; expired OIDC JWT | Read the private raw token file; validate issuer/audience/role/expiry |
| Management 403 | Viewer vs admin, denied command or origin mismatch | Use least-privilege correct role; check explicit origin and endpoint restrictions |
| Immediate connection closure under load | Configured connection cap, malformed request or slow subscriber output | Inspect counters/logs; reduce concurrency or frame size rather than reconnect-spamming |

Bootstrap refuses existing destinations by design. Select another new directory and update every path; do not remove live credentials/data to force a rerun. Development certificates expire after 30 days. Secret-file startup failures often indicate a symlink, nonregular file, unsafe permissions or wrong container owner; fix ownership/projection using the deployment workflow, not mode 0777.

## Persistence fails to start

- **Directory locked:** identify the owning process and stop it normally if appropriate. Reusing a data directory across two instances is unsupported. The presence of `LOCK` alone is not proof that a process still owns the lock.
- **Incomplete final record/group:** startup handles a torn tail automatically and synchronizes its repair.
- **Checksum/version/sequence/malformed complete record:** startup refuses without truncation. Preserve the entire directory and restore a trusted backup into a new directory. Do not classify complete corruption as an ordinary torn tail.
- **Empty/partial WAL header:** fails closed. Do not replace it with an empty header over your only evidence.
- **Disk full/budget/rebuild failure:** hybrid recovery needs old-plus-rebuilt segments; WAL/checkpoint/backup/staging space is outside the segment budget. Expand space or restore on adequately sized storage; never delete live blobs randomly.
- **Unexpected port/path/policy after restart:** inspect `server-config.json` or the explicit `--server-config-file`. Rewritten configuration is loaded after CLI parsing and can override flags; see [configuration](/reference/configuration/).

A live WAL/payload I/O failure is fail-stop to prevent unjournaled/missing data from leaking through. Fix the underlying storage issue before restarting. [Persistence](/operations/persistence/) and [backup/restore](/operations/backup-restore/) describe the exact trust boundary.

## Unknown command outcome

A disconnected client cannot infer whether a write committed. The native CLI does not reconnect/replay automatically. Management writes synchronize intent before dispatch, but a bus timeout, browser disconnect, failed outcome audit or result encoding/size failure can occur **after mutation**. `command completed; result delivery failed; do not retry automatically` means exactly that.

1. Stop automatic retries of pops, claims, increments and external side effects.
2. Inspect the audit outcome (for management), current key/queue state, job ID/receipt/idempotency state and application logs without revealing secrets.
3. Reconcile using your application's stable identifiers. An FQ idempotency key can prevent duplicate enqueue; it does not make every side effect exactly once.
4. Retry only when the operation's idempotence or observed state makes the action safe.

An explicit pre-execution OOM/syntax/authorization rejection is different from a lost reply. Do not hide that distinction in retry middleware.

## Memory or oversized responses

OOM under `--max-rss-bytes` is soft sampled admission, not eviction. Reduce producers and inspect RSS/pressure plus dataset/cache sizes. Reads/reclaimers remain available, but recovery/maintenance can overshoot. A 64 MiB native response budget or 8 MiB management budget is not an RSS cap. Use smaller SCAN-family pages instead of KEYS or a whole large collection. COUNT is a hint; pages are not silently truncated. Some single values remain too large for management delivery.

Management can commit a destructive command before its final JSON/base64 delivery exceeds the limit. A smaller page is sensible for a **read**, not justification for replaying the destructive command.

## Queues and blocking commands

- List pops remove immediately. A worker crash after BRPOP can lose work; use [native jobs](/guides/native-jobs/) when leases/receipts are required.
- A pipeline waits behind its connection's blocking command. Use separate producer/consumer connections; another client can still progress.
- FQ claim is nonblocking; no eligible jobs returns an empty result. Check ready/scheduled/running/dead counts, deadlines, concurrency and clocks.
- Expired/old-generation receipts cannot ACK the current lease. Do not extend or acknowledge using an old receipt after redelivery.
- Sidekiq lists are not native queues and cannot use native remediation.
- Blocking/session commands belong on RESP, not the management command endpoint.

## Replication and cluster problems

Distinguish native RESP replication from the development HTTP snapshot poller before interpreting status. Native loopback upstreams need explicit opt-in; protected upstream credentials/CA/TLS name are independent from local listener credentials. Inspect ROLE and INFO on both endpoints. WAIT/WAITAOF must use the writing connection; a new one-shot client has a different watermark.

Polling followers return LOADING on each process start until first sync. A source outage after sync can yield stale reads; unknown lag is not zero and known revision distance is not milliseconds. Check source export opt-in, admin bearer token, verified HTTPS, space and snapshot-size bounds.

For cluster failures, inspect `CLUSTER NODES`, `LINKS`, `INFO`, `SLOTS`/`SHARDS` and actual native follower progress. Bus ports, shared secret and routable announced addresses must match; RESP TLS does not secure the bus. CROSSSLOT requires same-slot keys, MOVED names the owner, ASK needs ASKING on the same target connection, and TRYAGAIN can occur during mixed-present migration. Do not bypass full-coverage policy or invoke TAKEOVER simply to suppress an error. Fence old writers before forced promotion. See [clustering](/operations/clustering/).

## Reporting a reproducible issue

Provide the commit/version, OS/architecture, sanitized flags, storage/durability mode, shard count, affected command **shape**, exact safe error text and a minimal reproduction on fresh directories. Include relevant bounded metrics and whether the write outcome is known. Never attach raw tokens, ACL passwords, receipts, payloads, unreviewed audit targets or backups containing private data. Keep historical verification counts separate from checks you actually ran on your checkout.
