---
title: Management API
description: Authenticated REST/websocket contracts, binary-safe request shapes, admin actions and ambiguous write outcomes.
---

The management listener is optional (`--management-bind`) and separate from RESP. Use the [console setup](/getting-started/console/) for a local server at `https://localhost:6383` with private token files and TLS trust. The implementation contracts live in `src/management/{mod,routes,dto,auth,audit,replication,transport}.rs`.

## Authentication and origin policy

Every `/api/*` route and `/metrics` requires a bearer identity. Send exactly one `Authorization: Bearer ...` header. Tokens come from the raw token files, **not** their SHA256 hashes in `management-tokens.json`. OIDC accepts verified JWTs under the configured issuer/audience/JWKS/role policy. RESP ACL passwords are a different identity system.

Static console assets do not require login. `/api/live` is the exception to header-based authentication: upgrade the websocket, then send a JSON text frame containing a `token` field within **two seconds**. Tokens do not belong in URLs, query parameters or cookies.

HTTP Origin is checked against Host/HTTP2 authority unless `--management-origin` specifies the exact expected origin. Cross-site browser requests are rejected; wildcard CORS and cookie authentication are not provided. Do not treat a reverse proxy's routing as a replacement for bearer authorization.

## Endpoint inventory

Viewer means any authenticated viewer/admin; admin-only operations still require the server's authorization even if browser controls are bypassed.

| Method and path | Role | Request / response |
| --- | --- | --- |
| `GET /api/session` | Viewer | Current principal name/role |
| `GET /api/overview` | Viewer | Actual server, metrics, storage, key count |
| `GET /api/keys` | Viewer | `cursor`, `count`, `match`; returns cursor and key/type/TTL rows |
| `GET /api/key` | Viewer; admin for reveal | Base64 `key`, optional cursor/count/reveal/state; type, TTL, placement and typed result |
| `GET /api/queues` | Viewer | Bounded native/Sidekiq queue discovery; cursor/count/match |
| `GET /api/queue` | Viewer; admin for reveal | Base64 `key`, cursor/count/state/reveal; queue kind, stats, typed result |
| `GET /api/commands` | Viewer | Actual running catalog, arity/flags, console availability and pinned client-stack metadata |
| `POST /api/command` | Viewer for safe reads; admin for writes/reveal | Base64 argument vector, optional reveal/confirmed; typed result and elapsed_us |
| `POST /api/queue/action` | Admin | Confirmed native ACK/NACK/EXTEND/RETRY/CANCEL |
| `GET /api/storage` | Viewer | Storage mode/stats, WAL and configured/fixed limits |
| `POST /api/storage/action` | Admin | Confirmed compact/checkpoint/backup |
| `GET /api/audit` | Admin | Numeric cursor/count; bounded intent/outcome journal page |
| `GET /api/live` | Authenticated first frame | Websocket actual overview samples |
| `GET /metrics` | Viewer | Prometheus text; see [observability](/operations/observability/) |
| `GET /api/replication/status` | Admin + export opt-in | Source instance/revision/role/format machine protocol |
| `GET /api/replication/snapshot` | Admin + export opt-in | Audited full snapshot stream for the development poller, not normal JSON |

The replication routes additionally require `--replication-export`; that requires a durable source, not a polling follower. Snapshot export is raw privileged data access and has separate staging/transfer bounds.

## Pagination and inspection

Most page requests default `count` to 100 and accept **1–200**. SCAN-family cursors are opaque and not snapshots; start at `0` and continue until the returned cursor indicates completion. Queue discovery combines internal cursors; return its token unchanged. Audit cursors are file offsets, not SCAN ranks. Do not mix these cursor types.

`/api/key` and `/api/queue` use **standard base64** in the `key` query parameter. URL-encode query values so `+`, `/` and `=` survive. These helpers accept at most 4,096 decoded bytes for keys/capabilities; the encoded input is also bounded. Key scanning `match` is a textual glob, at most 1,024 bytes. List inspection uses an offset cursor; sets/hashes/sorted sets use their SCAN-family cursor. List browsing offsets are bounded to one billion.

Rich key inspection supports string/list/set/hash/zset/native-queue; other types require an allowed command or the RESP CLI. A disappeared key reports type none/TTL -2. Placement is metadata such as `memory` or `mixed/metadata-resident`, not proof that an individual byte is currently cached. Native state filters are `ready`, `scheduled`, `running`, `dead`. Sidekiq inspection shows queued list data and bounded registry/heartbeat observations, not native leases.

## Binary-safe command requests

`POST /api/command` accepts this JSON shape:

```json
{"args_b64":["UElORw=="],"reveal":false,"confirmed":false}
```

That vector encodes `PING`; the endpoint is not a shell and does not perform shell expansion. Argument count is 1–1,024 and canonical decoded RESP framing must fit 8 MiB. Unknown JSON fields and malformed base64 are rejected. The current catalog determines syntax and readonly status. Non-readonly commands require admin and `confirmed: true`; reveal also requires admin and is audited.

Connection/session controls, subscriptions, replication stream commands, shutdown/config/ACL/debug controls, blocking reads and KEYS are among the denied commands. In particular, MULTI/WATCH/EXEC, BLPOP/BRPOP, XREAD/XREADGROUP, TS.READ and WAIT belong on a persistent RESP connection, not independent HTTP requests. Inspect `/api/commands` for the running console availability; catalog presence is not permission to call every command through HTTP.

### Read without exposing credentials in arguments

After the console setup, this requests overview and a PING result using the viewer token:

```sh
python3 - <<'PY'
import base64, json, ssl, urllib.request
from pathlib import Path
root = Path('.ffkv-docs-dev')
context = ssl.create_default_context(cafile=str(root / 'ca.crt'))
token = (root / 'console-viewer-token').read_text().strip()
for path, body in [('/api/overview', None), ('/api/command', {
        'args_b64': [base64.b64encode(b'PING').decode()], 'reveal': False})]:
    request = urllib.request.Request('https://localhost:6383' + path,
        data=None if body is None else json.dumps(body).encode(),
        headers={'Authorization': 'Bearer ' + token, 'Content-Type': 'application/json'})
    with urllib.request.urlopen(request, context=context, timeout=10) as response:
        print(json.dumps(json.load(response), indent=2))
PY
```

### Explicit confirmed write

This creates/replaces only `docs:api:example` on the isolated documentation instance. Do not point it at unrelated data:

```sh
python3 - <<'PY'
import base64, json, ssl, urllib.request
from pathlib import Path
root = Path('.ffkv-docs-dev')
body = {'args_b64': [base64.b64encode(v).decode() for v in
    (b'SET', b'docs:api:example', b'hello')], 'confirmed': True, 'reveal': False}
request = urllib.request.Request('https://localhost:6383/api/command',
    data=json.dumps(body).encode(), headers={
        'Authorization': 'Bearer ' + (root / 'console-admin-token').read_text().strip(),
        'Content-Type': 'application/json'})
with urllib.request.urlopen(request,
        context=ssl.create_default_context(cafile=str(root / 'ca.crt')), timeout=10) as response:
    print(json.dumps(json.load(response), indent=2))
PY
```

## Native queue and storage action shapes

`POST /api/queue/action` fields are `key_b64`, `action`, optional `receipt_b64`, `job_id_b64`, `visibility_ms`, `delay_ms`, and `confirmed`. ACK/NACK/EXTEND require the **actual current receipt**; RETRY/CANCEL require the actual job ID. EXTEND requires visibility_ms and NACK/RETRY may include delay_ms. The underlying queue engine enforces its own bounds after HTTP validation, including the 24-hour lease limit. Sidekiq/list keys are rejected: native queue required. Do not fabricate receipts, log them or replay an old lease's action after redelivery.

`POST /api/storage/action` uses `action: "compact"`, `"checkpoint"` or `"backup"`, with `confirmed: true`; backup additionally requires `name`. Use the backend-safe **1–64 ASCII alphanumeric/underscore/hyphen** name contract even though preliminary HTTP name parsing is broader. Storage actions map to `FFKV.COMPACT`, `FFKV.CHECKPOINT` and `FFKV.BACKUP`; see [backup/restore](/operations/backup-restore/).

## Result encoding and masking

Responses preserve RESP types rather than flattening all results into strings. Null, simple, integer, boolean, double, bulk, array, set, map, scored, verbatim and vector-result variants carry a `type` and associated fields. Maps are key/value pairs, not necessarily JavaScript string-keyed objects. Bulk byte views carry `base64`, optional `utf8`, `bytes` and `redacted`. Integers outside JavaScript's exact safe range become decimal strings; NaN/infinities become strings.

Bodies are hidden by default for **both** roles. Admin reveal is audited and recognized sensitive JSON fields remain masked in UTF-8/base64 unless the operator explicitly enabled sensitive reveal. This is not general DLP: unrecognized/non-JSON secrets may still appear. Command error text is sanitized; do not expect raw RESP error prose through the management DTO.

## Audit, timeouts and unknown write outcomes

Writes/reveals obtain a synchronized intent before dispatch and an outcome afterward. Audit rows include timestamp, principal/role, operation, optional target, outcome and request ID—not credentials, command arguments, payload bodies or receipts. Outcomes include success, executed/error, rejected, unknown, executed/result-too-large and executed/result-delivery-failed.

Audit failure before dispatch fails closed. After dispatch, disconnects/timeouts or outcome-audit failures can make the result unknown. Logical engine response checks and final JSON/base64 size checks are distinct: a mutation can commit before result delivery fails. An explicit `command completed; result delivery failed; do not retry automatically` response is **not a safe retry instruction**. Inspect application/audit state; never blindly repeat a pop, claim or other non-idempotent action.

Current fixed limits include 128 HTTP connections, 64 active work permits, 32 audited operations, 16 websockets, 100 API requests per one-second window, 12 MiB HTTP body cap, 8 MiB decoded command/final JSON limits and a 10-second ordinary command bus deadline. The outer HTTP handler deadline is 30 seconds. Websocket frames/messages cap at 32 KiB; a session lasts at most five minutes and reauthenticates before two-second updates. See [limits](/reference/limits/) for additional boundaries; they are not throughput promises.
