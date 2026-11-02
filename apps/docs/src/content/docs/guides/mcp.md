---
title: MCP — agent access to ffkv
description: Connect MCP clients to every ffkv command surface, persistent sessions, management APIs, and opt-in process operations using the native Rust sidecar.
---

`ffkv-mcp` is a native Rust/Tokio **stdio MCP server**. It uses the same RESP/TLS client as `ffkv-cli`, not a shell invocation per command and not the restricted HTTP console. It runs outside the datastore process, so MCP protocol work does not enter the engine's execution path.

The official Rust MCP SDK handles legacy `initialize` negotiation and the `2026-07-28` discovery lifecycle. There is no MCP HTTP listener. Connections to ffkv itself can use RESP over TCP/TLS and management HTTP(S)/WebSocket.

## Build and connect

From the repository root:

```sh
cargo build --release --bins
# Start an instance separately, or connect to one you already operate.
target/release/ffkv --bind 127.0.0.1:6382
```

Have your MCP client launch this executable:

```sh
target/release/ffkv-mcp --host 127.0.0.1 --port 6382
```

It waits for MCP messages on stdin; it is not an interactive command prompt. Stdout is reserved for JSON-RPC; diagnostics go to stderr. Without overrides, the default target is `127.0.0.1:6379`, using RESP3. Starting the sidecar does not start or restart an existing ffkv server.

For clients using an `mcpServers` JSON configuration, replace the absolute checkout path:

```json
{
  "mcpServers": {
    "ffkv": {
      "command": "/absolute/path/to/ffkv/target/release/ffkv-mcp",
      "args": ["--host", "127.0.0.1", "--port", "6382"]
    }
  }
}
```

For a configured deployment, pass `--config /absolute/path/to/ffkv-mcp.json` instead. Use the client's own configuration format where it differs from the example above.

## Seven tools, complete surface access

The tool list stays small rather than sending hundreds of command schemas to the model. Read `ffkv://capabilities` for configured target names, enabled privileges, limits, and the access map. `ffkv://commands` returns the first live command page.

| Tool | What it exposes |
| --- | --- |
| `ffkv_execute` | Ordered, duplex command pipelines on a persistent connection; all ordinary-reply commands under the server's ACLs |
| `ffkv_session` | Open/list/close connections; unrestricted RESP send/receive; raw binary send/receive |
| `ffkv_commands` | The target's live COMMAND registry, including subcommands and loaded modules; prefix filtering and pagination |
| `ffkv_management` | Every existing management GET/POST route, query parameters, JSON bodies, metrics, audit, storage/queue operations, and snapshot downloads |
| `ffkv_management_live` | Authenticated `/api/live` WebSocket open/receive/list/close |
| `ffkv_process` | Explicitly enabled ffkv-only help/start/status/logs/stop/offline restore; all startup arguments |
| `ffkv_result` | Bounded, lossless paging and release of large serialized results |

There is no MCP allowlist of Redis command names. Strings, hashes, lists, sets, sorted sets, streams, bitmaps/bitfields, HyperLogLog, GEO, JSON, sparse arrays, time series, vectors, probabilistic types, search, native jobs, Lua, functions, modules, ACL, configuration, replication, and cluster commands use their real ffkv interfaces. The server remains the authority on syntax, permissions, availability, and behavior.

This is **feature access**, not a claim that every Redis option is compatible or that every option combination has an independent MCP test. Existing ffkv limits and [compatibility boundaries](/concepts/compatibility/) remain in force.

## Execute commands and batches

The examples below are the `arguments` object for the named tool, not shell commands. Call `ffkv_execute`:

```json
{
  "commands": [
    ["SET", "agent:counter", "41"],
    ["INCR", "agent:counter"],
    ["GET", "agent:counter"]
  ]
}
```

It returns `session`, `outcome`, ordered `replies`, and separate RESP3 `pushes`. The default connection is reused. Batch writes and reply reads run concurrently, so large bidirectional pipelines do not deadlock by filling both socket buffers. A batch is **not** a transaction: use MULTI/EXEC when required.

Strings are UTF-8 arguments. Use `{"base64":"AP8="}` for arbitrary bytes, including binary keys, embeddings, DUMP payloads, and native job receipts. Safe JSON integers are accepted; use decimal strings for integers outside ±9,007,199,254,740,991 and for floating-point arguments.

Results preserve RESP types rather than coercing maps or integers into lossy JSON:

```json
{
  "type": "integer",
  "value": "9007199254740993"
}
```

| RESP value | JSON representation |
| --- | --- |
| Simple/bulk/error/big number | `type`, `encoding: "utf8"` or `"base64"`, and `value` |
| Integer | `type: "integer"`, decimal-string `value` |
| Double | `type: "double"`, string `value`, including `nan`, `inf`, `-inf` |
| Null / boolean | `type: "null"` / `type: "boolean"` with boolean `value` |
| Array / set / push | `type` and ordered `items` |
| Map | `type: "map"`, `entries: [[keyFrame, valueFrame], ...]`; arbitrary and duplicate keys remain representable |
| Attribute | `type: "attribute"`, `entries`, and the wrapped `value` frame |
| Verbatim | `type: "verbatim"`, encoded `format`, `encoding`, and `value` |

Both MCP `structuredContent` and JSON text content are supplied. Server RESP errors remain typed replies, set `outcome: "error"`, and mark the MCP result `isError`. Other commands in a submitted pipeline can still succeed. Structural batch validation happens before any bytes are sent; server-side argument/type errors still happen at execution.

## Stateful sessions, transactions, and parallelism

Open an explicit connection with `ffkv_session`:

```json
{"action":"open","target":"local","protocol":3,"database":0}
```

Use the returned `session` string in subsequent calls; never invent a handle:

```json
{
  "session": "<returned-session>",
  "commands": [
    ["MULTI"],
    ["SET", "agent:transaction", "retained"],
    ["INCR", "agent:counter"],
    ["EXEC"]
  ]
}
```

The handle preserves WATCH, MULTI, selected database, authentication, and connection state. Independent handles can execute concurrently; overlapping execute calls on one handle serialize. The default session is persistent too: it is not a pool that silently resets SELECT or abandons a transaction.

Close when finished:

```json
{"action":"close","session":"<returned-session>"}
```

For cluster operations, configure each required node as a named target and open its connection explicitly. MOVED/ASK replies are preserved; the sidecar does not replay or redirect commands automatically. ASKING and READONLY remain connection-scoped.

## Blocking, subscriptions, debugging, and raw streams

Commands with unusual reply shapes use explicit send/receive. This includes subscriptions, MONITOR, CLIENT REPLY/TRACKING, SCRIPT DEBUG, protocol diagnostics, SHUTDOWN/QUIT, and native replication handshakes. They are refused by `ffkv_execute` **before its batch is sent**, but remain available through `ffkv_session`.

For a blocking queue consumer, open a dedicated session, then call:

```json
{"action":"send","session":"<consumer-session>","commands":[["BRPOP","agent:jobs","0"]]}
```

Send acknowledges bytes written, **not server acceptance or completion**. Another session can LPUSH a job. Retrieve the actual result:

```json
{"action":"receive","session":"<consumer-session>","max_frames":1,"wait_ms":5000}
```

Use the same pattern for SUBSCRIBE and receive its acknowledgement and subsequent push frames. There is no unbounded background event queue. Consumers must drain events; Pub/Sub remains ephemeral and server-side slow-consumer/disconnect rules still apply.

Once a session enters manual mode through send/receive, it stays **manual-only**. It cannot return to execute mode, because unread replies must not be mistaken for a later command's result. Close it or continue explicit send/receive. A send that times out or is cancelled before acquiring the manual-transition gate does not change the session's mode.

Open with `"mode":"raw"` for native replication or other byte streams. Raw send accepts framed `commands` for handshakes or `data_base64` for arbitrary bytes. Raw receive returns bounded `data_base64`, `bytes`, `timed_out`, and `eof`; it preserves negotiation read-ahead. It is raw protocol access, not an automatic replication client. Server replication/TLS/ACL prerequisites still apply.

### Cancellation and unknown outcomes

- Execute timeout/cancellation after dispatch closes that connection. Already received replies remain in the structured result where a response can be delivered; unobserved commands have **unknown outcomes**. Waiting for an execute slot is pre-dispatch and does not close the connection.
- A send waiting for the manual-transition gate or writer lock retains the connection on timeout/cancellation; another active operation continues. After writing starts, failures can leave unknown outcomes and close the connection. One deadline covers both waiting and writing.
- MCP cancellation is best-effort and a client may receive no final result. It does not roll back writes already committed.
- Receive timeout is nonfatal. RESP receive cancellation retains parser state and queued frames; a later receive continues. To abandon a blocked command, close its session or use authorized CLIENT UNBLOCK from another session.
- Management live-receive cancellation closes its handle.
- Neither MCP nor the shared wire client automatically retries or replays commands. Inspect state or use application idempotency after an ambiguous result.

## Management APIs and live events

Configure the target's `management_url` and token source. Then call `ffkv_management`:

```json
{"path":"/api/keys?count=20"}
```

```json
{"method":"POST","path":"/api/storage/action","body":{"action":"checkpoint","confirmed":true}}
```

The result includes HTTP `status`, `content_type`, and `body` with `encoding: json|text|base64`. **Inspect the HTTP status**: a returned 4xx/5xx is a real HTTP response, not a successful management action. The existing API's role checks, reveal settings, explicit confirmations, and audit remain unchanged. POST failures after dispatch can have unknown effects; local route, credential, body, and download preflight failures are known not to have dispatched and do not receive that warning.

Only relative `/api/...` or `/metrics` paths on the configured origin are accepted. Credentials cannot be overridden in tool arguments. Redirects, origin changes, traversal, and fragments are rejected. Ordinary HTTP clients are reused.

`ffkv_management_live` opens the real authenticated `/api/live` WebSocket. Receive returns bounded `frames`, `closed`, `timed_out`, and `limit_reached`, with transport errors reported explicitly. The initial overview is retained for retrieval. Active receives, including those queued behind another receive, are exempt from idle expiry. The idle timer restarts when a receive completes; actual idle handles still expire.

For raw snapshot export, enable the server's replication-export capability and configure an existing `file_root` directory. A download is explicit:

```json
{"path":"/api/replication/snapshot","download_path":"snapshot.ffkv","max_download_bytes":1073741824}
```

Downloads stream to a **new** file under that root and return byte count and SHA256. Relative paths and absolute paths beneath either the configured root spelling or its canonical path are accepted, including an operator-configured root alias such as macOS `/tmp`. Existing files and symlinks below the root are rejected—even when a child symlink points back inside the root. Cancellation/failure removes the partial file. The HTTP replication snapshot format is distinct from a sealed offline-restore backup; follow [persistence](/operations/persistence/) rather than treating every export as interchangeable.

## Operator configuration

An example `ffkv-mcp.json` for a TLS-protected server:

```json
{
  "default_target": "local",
  "targets": {
    "local": {
      "host": "127.0.0.1",
      "port": 6382,
      "protocol": 3,
      "database": 0,
      "username": "agent",
      "password_file": "/absolute/path/to/resp-password",
      "tls": {
        "ca_cert": "/absolute/path/to/ca.pem",
        "server_name": "ffkv.example.internal"
      },
      "management_url": "https://ffkv.example.internal:6383",
      "management_token_file": "/absolute/path/to/management-token",
      "management_ca_cert": "/absolute/path/to/ca.pem"
    }
  },
  "file_root": "/absolute/path/to/new-downloads",
  "process": { "enabled": false },
  "limits": { "max_sessions": 64, "inline_result_bytes": 65536 }
}
```

Omit `tls` for a loopback plaintext server. The RESP password file contains the password; the management token file contains the **raw bearer token**, not the server's token-policy JSON. Alternatives are `password_env` and `management_token_env`, naming environment variables. Do not set both file and environment sources for one credential. `FFKV_MCP_AUTH`, if present, supplies the default target's otherwise unconfigured RESP password.

TLS additionally supports `client_cert` and `client_key` together. Relative configuration paths are resolved from the sidecar's working directory; absolute paths avoid client-dependent working directories. Unknown configuration fields are rejected. Configured targets—not tool-supplied URLs—determine which endpoints the sidecar can access.

### Privileged process operations

Process control is disabled by default. Enable it with `--allow-process` or `process.enabled: true`; optionally set `--ffkv-binary` / `process.binary`. The default executable is the sibling `ffkv` binary. No arbitrary shell, executable, environment override, or working directory is accepted through tools.

- `help` runs the real server help; `args: ["restore"]` selects restore help.
- `start` accepts all ffkv startup arguments and returns an owned handle/PID. It acknowledges spawning, **not readiness**; inspect status and logs.
- `status`, `logs`, and `stop` operate only on owned handles, never arbitrary PIDs or existing servers.
- Logs have `since_sequence` / `next_sequence` byte cursors, bounded pages, explicit dropped/missed-byte counters, and lossless text/base64 entries.
- `restore` invokes `ffkv restore --source ... --data-dir ...`; the real restore implementation refuses an existing destination. Inspect `success` and `exit_code`; failed help/restore marks the MCP result as an error.

Process control is **privileged host administration**, not a sandbox. In particular, `file_root` confines management downloads, not paths inside server startup arguments or restore. Stopping an owned process terminates it; use an authorized SHUTDOWN command first if graceful server shutdown is required. MCP shutdown/EOF terminates and reaps owned children. Existing external servers are not owned or stopped.

## Large results and limits

A result larger than the inline threshold returns a `result_id`, `total_bytes`, and `read_tool: "ffkv_result"`. Call read with `offset` and `length`, concatenate the decoded `data_base64` chunks, then parse the reconstructed JSON. Follow `next_offset` until null, then release the handle. Reads are byte offsets and may split UTF-8 sequences; decode only after reassembly.

| Limit | Default |
| --- | --- |
| Open RESP sessions | 64; independently up to 64 management-live handles |
| Idle session lifetime | 15 minutes |
| Commands per execute/send batch | 1,024 |
| Framed command size / total batch size | 8 MiB / 16 MiB |
| Aggregate tagged reply budget | 64 MiB |
| Inline serialized result | 64 KiB; configurable up to 1 MiB |
| Retained serialized results | 128 MiB total; five-minute expiry |
| Result read chunk | 64 KiB default; 256 KiB maximum |
| Operation timeout | 10 seconds; default configured maximum five minutes |
| Snapshot download | Explicit cap; default and maximum 1 GiB |
| Owned child processes / retained logs per child | 8 / 64 KiB |
| Concurrent MCP tool calls / incoming JSON-RPC line | 128 / 64 MiB |

Limits fail explicitly rather than silently truncating data. Result reads do not extend expiry. If retention fills after a command executes, inspect state; do not rerun a write just to reproduce its lost result. `ffkv_result` pages themselves are not recursively paged. Process log overflow is explicitly counted rather than presented as complete history.

## Verify and measure

The smoke harness owns disposable servers, ports, credentials, data directories, and subprocesses:

```sh
python3 tests/mcp_smoke.py \
  --binary target/release/ffkv --mcp-binary target/release/ffkv-mcp

.ffkv-bdd-venv/bin/python tests/bdd/run.py \
  --server target/release/ffkv --client target/release/ffkv-cli \
  --feature tests/bdd/features/mcp.feature

python3 benchmarks/mcp.py \
  --binary target/release/ffkv --mcp-binary target/release/ffkv-mcp \
  --rounds 100 --batch-sizes 1 16 128
```

The benchmark compares persistent MCP stdio sessions against direct persistent RESP on the same isolated strict-durability server. It reports real command counts, throughput, and median/p95 round latency. JSON-RPC, JSON encoding, and agent-client overhead remain real costs; batching amortizes them. This is a local microbenchmark, not a network, concurrency, or production workload guarantee.

See [testing](/contributing/testing/), [benchmarks](/contributing/benchmarks/), [native CLI](/getting-started/cli/), and [management API](/reference/management-api/). Protocol references: [MCP specification](https://modelcontextprotocol.io/specification/2026-07-28) and the [official Rust SDK](https://github.com/modelcontextprotocol/rust-sdk).
