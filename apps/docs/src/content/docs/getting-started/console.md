---
title: Management console
description: Start an isolated TLS-protected ffkv console and understand its authentication and safety boundaries.
---

The React console is served by ffkv's optional management listener. It is separate from this static documentation site and from the RESP listener. You need built server/client binaries, Node 22/npm, Python 3 and OpenSSL. See [installation](/getting-started/installation/).

## Bootstrap a fresh local instance

Run from the repository root as your normal, nonroot user. Leave existing servers and data directories alone. These commands use RESP **6382**, management **6383**, and a **new** `.ffkv-docs-dev` directory:

```sh
npm --prefix apps/console ci
npm --prefix apps/console run build
python3 deploy/bootstrap.py "$PWD/.ffkv-docs-dev"

target/release/ffkv \
  --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-dev/data/database --durability strict --storage hybrid \
  --acl-file .ffkv-docs-dev/users.acl \
  --tls-cert .ffkv-docs-dev/tls.crt --tls-key .ffkv-docs-dev/tls.key \
  --management-bind 127.0.0.1:6383 \
  --management-token-file .ffkv-docs-dev/management-tokens.json \
  --management-audit-file .ffkv-docs-dev/data/management-audit.jsonl \
  --management-tls-cert .ffkv-docs-dev/tls.crt \
  --management-tls-key .ffkv-docs-dev/tls.key \
  --console-dir apps/console/dist
```

Bootstrap refuses **any existing destination**, even an empty directory. Choose a different new name and update every path if it already exists; do not delete a directory to make the example run. It creates private credential files, hashed ACL/token policies, and a self-signed development certificate valid for 30 days. It prints paths, not credentials. This is not production PKI.

Open `https://localhost:6383`. Verify/trust only the certificate you just generated. Use your editor to read `.ffkv-docs-dev/console-viewer-token` and paste its contents into the login field. Start with viewer access. Use `.ffkv-docs-dev/console-admin-token` only for administration; keep tokens out of screenshots, shared terminals and issue reports.

Browser tokens are memory-only: refresh ends the session; logout clears cached data. Static assets are public, but `/api/*` and `/metrics` require authentication. The websocket authenticates in its first frame, never a token-bearing URL.

## Verify the RESP side independently

In another terminal:

```sh
target/release/ffkv-cli --tls --cacert .ffkv-docs-dev/ca.crt \
  -h localhost -p 6382 --user admin --askpass PING
```

At the hidden prompt, supply the contents of `.ffkv-docs-dev/admin-password`; a successful request returns `PONG`. A console bearer token **does not** authenticate RESP, and an ACL password **does not** log in to the console. The `health` ACL user is restricted to PING:

```sh
python3 deploy/probe.py --host 127.0.0.1 --port 6382 --username health \
  --password-file .ffkv-docs-dev/health-password \
  --tls-ca .ffkv-docs-dev/ca.crt --tls-server-name localhost
```

## Use the console safely

- **Overview:** actual connection, request, latency, RSS, storage and replication samples. Browser charts are observed session history, not a monitoring database.
- **Keys:** bounded cursor browsing, binary-safe names, type and TTL. Rich inspectors cover strings, lists, sets, hashes, sorted sets and native queues. Use the CLI/command console for the newer datatype families.
- **Queues:** native leased-job states and Sidekiq's list/registry/schedule/retry/heartbeat keys. Sidekiq lists do not gain native receipts or lease remediation.
- **Command console:** an argument vector, **not a shell**. Read commands and safe PING/ECHO/INFO/COMMAND controls do not require write mode. Writes require admin, explicit write enablement and confirmation. Connection-scoped, blocking and several privileged commands are unavailable; use the native CLI for transactions, subscriptions and blocking consumers.
- **Storage:** explicit checkpoint, hybrid compaction and named backup actions. Back up before infrastructure changes.
- **Audit:** admin-only intent/outcome records. A record does not include credentials, command arguments, payload bodies or receipts.

Payloads are hidden for both roles. Admin reveal is audited and masks recognized sensitive JSON fields, including their base64 representation. This is **not general DLP**: unrecognized secrets and non-JSON data may be exposed on reveal. Do not enable `--management-allow-sensitive-reveal` unless unmasked sensitive JSON is intentionally required.

## A read-only API request

This reads the viewer token from disk rather than placing it in shell history or process arguments:

```sh
python3 - <<'PY'
import json, ssl, urllib.request
from pathlib import Path
root = Path('.ffkv-docs-dev')
request = urllib.request.Request('https://localhost:6383/api/overview', headers={
    'Authorization': 'Bearer ' + (root / 'console-viewer-token').read_text().strip()})
context = ssl.create_default_context(cafile=str(root / 'ca.crt'))
with urllib.request.urlopen(request, context=context, timeout=10) as response:
    print(json.dumps(json.load(response), indent=2))
PY
```

## Unknown outcomes and shutdown

A synchronized audit intent precedes a management write. Audit failure fails closed before dispatch, but a timeout, disconnect, failed outcome audit or oversized result **after execution** can leave a committed write without a delivered response. Do not automatically retry pops, claims or other non-idempotent operations. Inspect application state and the audit trail first; see [management API](/reference/management-api/) and [troubleshooting](/operations/troubleshooting/).

Stop this server with Ctrl-C in its own terminal. Keep the generated directory private. Restarting with the same directory recovers its durable data. Production deployments need your PKI, a secret manager, explicit proxy origin policy where applicable, and planned credential rotation; see [security](/operations/security/).
