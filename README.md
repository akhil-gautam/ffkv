<p align="center">
  <img src="apps/docs/public/brand/ffkv-wordmark.svg" alt="ffkv" width="360">
</p>

# ffkv — freaking fast KV

A single-node Redis-compatible datastore built in usable, CLI-testable stages.
Memory-first and hybrid RAM/SSD profiles support background-job workloads, with
native leased jobs, a React management console and self-hosted deployment.

Brand assets: [wordmark](apps/docs/public/brand/ffkv-wordmark.svg) · [symbol](apps/docs/public/brand/ffkv-mark.svg).

## Documentation site

The Astro + Starlight handbook lives in [`apps/docs`](apps/docs). It includes
installation and operator guides, all 19 datatype families, a searchable generated
reference for the Redis 8.10 catalog, and ffkv's extension commands. It is separate
from the authenticated React management console.

With Node **22.23.4 or newer** and npm, run from the repository root:

```sh
npm --prefix apps/docs ci
npm --prefix apps/docs run dev
```

Open **http://localhost:4321**. This starts only the documentation site; it does not
start, stop or connect to a KV instance. The reference regenerates locally from
the pinned command metadata, current extension registrations and BDD source files.

If your installed Node is older, use an isolated runtime without changing it globally:

```sh
npx --yes --package node@22.23.4 -- npm --prefix apps/docs ci
npx --yes --package node@22.23.4 -- npm --prefix apps/docs run dev
```

Use the same isolated-runtime prefix for the check, build and preview commands below.

```sh
npm --prefix apps/docs run check
npm --prefix apps/docs run build
npm --prefix apps/docs run preview
```

The build includes a full internal-link, heading, command-route and search-index
audit. Static output is `apps/docs/dist/`. Set `DOCS_SITE` to the deployment origin
and optional `DOCS_BASE` to a project subpath before building; see the site's
**Deploy → Documentation site** guide for static-host and subpath instructions.

## MCP integration

The native `ffkv-mcp` stdio sidecar exposes command pipelines, persistent
RESP2/RESP3 sessions, raw streams, the management API and live events, and
explicitly enabled ffkv-only process/restore operations. It shares the native
client transport; commands do not spawn shells or reconnect per request.

```sh
cargo build --release --bins
# Configure your MCP client to launch this command against an existing instance:
target/release/ffkv-mcp --host 127.0.0.1 --port 6380
```

Seven tools cover the live command registry and the non-command surfaces.
Server ACLs still apply. Process control is disabled by default; existing
servers are never owned or stopped by the sidecar. See the
[MCP handbook](apps/docs/src/content/docs/guides/mcp.md) for client configuration,
binary-safe results, streaming sessions, permissions, limits and benchmarks.

## CLI-testable alpha — Steps 1–8

A Rust/Tokio TCP server with RESP2/RESP3, binary-safe keys and values, pipelining,
all 19 Redis datatype families, and the 486-entry Redis 8.10 command catalog.
Optional WAL persistence includes checksummed recovery and explicit fsync policy.
Independent key-owning actors execute single-shard commands; a coordinator keeps
cross-shard commands atomic.

Without `--data-dir`, data is lost when the process stops. TLS, mutable ACLs,
authenticated metrics/console, backups, native replication and ffkv clustering
are available. This is a tested alpha, not production certification or complete
Redis binary compatibility. Sidekiq support is the pinned integration below.

### Run

Prerequisites: macOS/Linux and Rust 1.99 or newer. The native `ffkv-cli` client
is included; neither the server nor the client requires a Redis installation.
Existing Redis-protocol clients remain compatible.

```sh
cargo run --release -- --bind 127.0.0.1:6380
```

Without `--bind`, the server defaults to `127.0.0.1:6379`. These examples use
**6380**, matching the native client's default. In another terminal, install the
client from this checkout:

```sh
cargo install --path . --locked --bin ffkv-cli --target-dir target
```

Then run:

```sh
ffkv-cli -2 -p 6380 PING                         # PONG
ffkv-cli -2 -p 6380 SET greeting 'hello ffkv'     # OK
ffkv-cli -2 -p 6380 GET greeting                 # hello ffkv
ffkv-cli -2 -p 6380 SET counter 41               # OK
ffkv-cli -2 -p 6380 INCR counter                 # 42
ffkv-cli -2 -p 6380 MSET first one second two    # OK
ffkv-cli -2 -p 6380 MGET first second            # one, two
ffkv-cli -2 -p 6380 SET short-lived value PX 10000
ffkv-cli -2 -p 6380 PTTL short-lived             # remaining milliseconds
ffkv-cli -2 -p 6380 SET greeting ignored NX      # nil: existing key unchanged
ffkv-cli -2 -p 6380 GET greeting                 # hello ffkv
ffkv-cli -2 -p 6380 COMMAND INFO get set         # implemented command metadata
ffkv-cli -2 -p 6380 COMMAND COUNT                # current implemented command count
```

Interactive shell: `ffkv-cli`. Stop the client with Ctrl-D or Ctrl-C; stop the
server separately with Ctrl-C. Use `ffkv-cli --help` for client options.
Use `cargo run -- --help` for listener and connection-limit options.

If a previous ffkv process is running, stop it with Ctrl-C and restart to load
the new code. Restarting without `--data-dir` discards in-memory data. Enabling
persistence does not import an already-running in-memory process's data.

### Native client

`cargo build --release --bins` builds both executables under `target/release`;
`cargo run` still selects the server. Add Cargo's `bin` directory to your PATH
after installation if it is not already present. The Dockerfile packages both
`ffkv` and `ffkv-cli`; its default entrypoint remains the server.

```sh
ffkv-cli PING
ffkv-cli -3 --json HELLO 3
printf 'SET "key with spaces" ""\nGET "key with spaces"\n' | ffkv-cli --json
printf 'binary\000payload\n' | ffkv-cli -x SET blob
ffkv-cli --raw GET blob
```

Place options before COMMAND; trailing arguments, including negative numbers,
are passed literally. No command opens an editable terminal session, or reads
one quoted command per line from piped stdin. Single/double quotes, empty
arguments and escapes such as `\n` and `\x00` are supported. Pasted terminal
commands retain their order. History is memory-only; AUTH, HELLO AUTH and other
credential commands are excluded. Ctrl-C closes the connection with status 130,
including while editing or waiting for a blocking reply.

`-x` appends stdin unchanged as the final argument, including NULs and trailing
newlines. Human output escapes terminal control bytes. `--raw` emits actual bytes,
with one newline after each scalar. `--json` emits ordinary JSON strings, arrays,
numbers and maps with unique UTF-8 string keys. Invalid UTF-8 uses
`{"type":"bytes","base64":"..."}`; binary/non-string/duplicate-key maps use
`{"type":"map","entries":[[key,value],...]}`. Sets/pushes have typed `values`,
attributes have `entries` and `value`, and errors have `type: "error"` and `value`.
Piped sessions emit one JSON value per reply or push.

Defaults are host `127.0.0.1`, port `6380`, RESP2 and database zero. Use `-h`, `-p`,
`-n` and `-2`/`-3` to override them. `-u` or `FFKV_URL` accepts `redis://` and
`rediss://` URIs, including percent-encoded credentials. Explicit host/port/db/user
flags override URI fields. Password precedence is `--askpass`, `--pass`,
`FFKVCLI_AUTH`, then the URI. `REDIS_URL` is intentionally not read implicitly.
Prefer the hidden password prompt or authentication environment variable over
password-bearing process arguments. On an ACL/TLS-enabled listener:

```sh
ffkv-cli --tls --cacert .ffkv-dev/ca.crt --sni localhost --user admin --askpass PING
```

TLS verifies certificate chains and server identity with bundled WebPKI roots or
`--cacert`; `--sni` overrides the verified name. Optional client certificates use
paired `--cert`/`--key`. There is no insecure verification bypass or plaintext
fallback. `--connect-timeout` bounds connection/negotiation setup; command waits
remain unbounded so `BRPOP ... 0` and subscriptions work normally.

Direct `SUBSCRIBE`/`PSUBSCRIBE` invocations print acknowledgements and messages
until Ctrl-C. Interactive sessions accept unsubscribe commands while receiving
messages. RESP2/RESP3 and successful HELLO/AUTH transitions stay synchronized.
All implemented server commands, including native `FQ.*`, use the same client.
Requests are bounded to 8 MiB/1,024 arguments; replies have 64 MiB wire/logical
allocation budgets and bounded nesting/value counts, not a process RSS cap.
RESP3 streamed/unknown-length frame syntax is not supported.

Exit statuses: `0` success, `1` server command or input-syntax errors,
`2` configuration/transport/protocol/input-I/O failures, `130` interrupted.
Nested EXEC errors also fail the command. A lost connection has an unknown
command outcome: the client never reconnects automatically or replays writes.


### Queue producer/consumer

With ffkv running on port 6380, start a consumer in another terminal:

```sh
ffkv-cli -2 -p 6380 BRPOP demo:jobs 0
```

It waits until a job arrives; `0` means no timeout. In a producer terminal:

```sh
ffkv-cli -2 -p 6380 LPUSH demo:jobs '{"job":"send-email","id":1}' '{"job":"send-email","id":2}'
```

On an initially empty queue, the producer returns `2` (length before waiting
consumers pop). The consumer receives `demo:jobs` and job `id:1`. Combining
`LPUSH` with `BRPOP` produces FIFO job order; `BLPOP` pops from the other end.

Inspect and consume the remaining job:

```sh
ffkv-cli -2 -p 6380 LRANGE demo:jobs 0 -1         # job id:2
ffkv-cli -2 -p 6380 RPOP demo:jobs               # consumes job id:2
ffkv-cli -2 -p 6380 TYPE demo:jobs               # none: last pop removes the key
ffkv-cli -2 -p 6380 BLPOP demo:empty 0.5          # nil after a finite wait
```

`BLPOP`/`BRPOP` accept multiple keys, checked in request order, and a nonnegative
floating-point timeout in seconds. Positive timeouts round up to milliseconds;
timeout returns a RESP2 null array. Consumers waiting on the same key are served
in registration order. A blocked connection does not stall other clients, but
its own pipelined commands wait behind the blocking pop.

List pushes and partial pops preserve TTL. The last pop removes both the key
and its TTL; recreating it starts without expiration. `LPOP`/`RPOP` return one
bulk value when count is omitted, or an array when count is present.

Changing a watched missing key into a string does not itself wake a consumer.
A later list push wakes it and rechecks the original ordered key scan, which
can return `WRONGTYPE` before reaching the newly populated list. This follows
[Redis 7.2's command reprocessing](https://github.com/redis/redis/blob/7.2.4/src/blocked.c).

**These are Redis list queues, not leased jobs.** A successful pop removes the
item immediately. A lost connection or worker crash after removal can lose that
job; there is no acknowledgement or automatic redelivery. Disconnected pending
consumers are cancelled and their waiter indexes are reclaimed. WAL recovery
preserves completed pops; native leased-job semantics belong to a later step.

### Persistence and crash recovery

Start a persistent instance (stop any existing process using this port first):

```sh
cargo run --release -- --bind 127.0.0.1:6380 --data-dir ./ffkv-data
```

The data directory defaults to **strict** durability. Set a value and a queue:

```sh
ffkv-cli -2 -p 6380 SET durable:greeting hello
ffkv-cli -2 -p 6380 RPUSH durable:jobs first second
ffkv-cli -2 -p 6380 LPOP durable:jobs             # first stays removed on restart
ffkv-cli -2 -p 6380 SET durable:temporary value EX 60
```

Stop with Ctrl-C and repeat the same server command using the **same directory**.
`GET durable:greeting` returns `hello`; `LRANGE durable:jobs 0 -1` contains only
`second`. TTL deadlines are absolute: time spent offline counts toward expiration.

| Policy | CLI | Acknowledgement and failure tradeoff |
| --- | --- | --- |
| Strict (default with a data directory) | `--durability strict` | Append, flush, and fsync each mutation batch before its reply. Higher write latency. |
| Every second | `--durability everysec` | Append and flush before reply; fsync targets a one-second cadence. Recent acknowledged writes can be lost on an OS/storage crash. Load or slow storage can extend that window. |
| In-memory | Omit `--data-dir` | No WAL and no restart survival. |

Ctrl-C and SIGTERM synchronize pending WAL data before exiting. A process crash
may leave an unacknowledged mutation present or absent; clients must treat a lost
reply as an unknown outcome. All changes in a command share an atomic WAL batch.
Large batches use bounded BEGIN/data/COMMIT groups; startup applies no part of an
unfinished group, even if some data frames are complete. This supports large set
stores/pops without whole-log buffering or partial recovery.
Blocking pop delivery is logged before its reply; unfulfilled waits are not
restored after restart. Any WAL append, flush, or fsync failure stops the server
instead of allowing unjournaled in-memory changes to leak through later reads.

The directory contains `wal.ffkv` and `LOCK`. A second ffkv process cannot open
it concurrently. Startup streams records, checks CRC32C and sequence numbers,
and reconstructs values and expiration indexes. An incomplete final record or
uncommitted group is discarded and synchronized before appending again.
Committed groups replay in a bounded second pass, not one giant memory buffer.
A complete record with a bad
checksum, malformed changes, unsupported version, or invalid sequence **fails
startup without truncating the WAL**. An empty/partial file header also fails
closed. Do not manually edit a live WAL.

The crash tests exercise acknowledged-data survival under **process termination**,
including SIGKILL, torn-tail recovery, and real file-size-limit write errors.
They do not prove behavior under power loss, dishonest device caches, lost disks,
or lost machines. `fsync` is local durability, not replication or high availability.
Checkpoints bound write-history growth; hybrid compaction reclaims derived blobs.
See Step 8 for automatic checkpoint thresholds, safe backup/restore, separate disk
budgets and the headroom required by recovery and compaction.

### Collections and coordination

```sh
ffkv-cli -2 -p 6380 SADD demo:active alice bob
ffkv-cli -2 -p 6380 SMEMBERS demo:active
ffkv-cli -2 -p 6380 SPOP demo:active 1
ffkv-cli -2 -p 6380 HSET demo:profile name Akhil plan builder
ffkv-cli -2 -p 6380 HGETALL demo:profile
ffkv-cli -2 -p 6380 HSETEX demo:profile PX 60000 FIELDS 1 token temporary
ffkv-cli -2 -p 6380 HPTTL demo:profile FIELDS 1 token
ffkv-cli -2 -p 6380 ZADD demo:leaderboard 100 alice 200 bob
ffkv-cli -2 -p 6380 ZRANGE demo:leaderboard 0 -1 WITHSCORES
ffkv-cli -2 -p 6380 SCAN 0 MATCH 'demo:*' COUNT 10
```

Set iteration order and random pop results are not fixed. `SPOP` persists the
actual removed members, so restart does not choose different random members.
Set algebra and its STORE forms validate source types before replacing a
destination, including overlapping source/destination keys.

Hash field deadlines are independent of the key TTL. Ordinary `HSET` clears an
overwritten field's TTL; `HINCRBY` preserves it. All hash reads treat expired
fields as missing, and the last expired/deleted field removes the hash key.
The command baseline now includes the published Redis 8.10 catalog. Coverage
and the remaining compatibility boundaries are documented below.

The default is four shard actors; select `--shards 1` through `--shards 256`.
Single-shard commands run on their owning actor. Cross-shard commands pause
actors behind a barrier and move only affected entries into an atomic view,
without copying their collection payloads. `MSET`/`MGET`, rename, set moves,
algebra stores, and multi-key blocking pops retain command-level atomicity.
The WAL has one ordered commit gate shared by all actors. Changing shard count
on restart redistributes recovered keys without changing their identity.

`SCAN`, `SSCAN`, `HSCAN`, and `ZSCAN` use stable hash-order cursors rather than
mutable rank offsets. COUNT is a hint; cursors are opaque, not page numbers or
snapshots. Use them for production-size browsing rather than `KEYS *`.

### All Redis datatype families

The [current Redis datatype catalog](https://redis.io/docs/latest/develop/data-types/)
contains 19 families. ffkv implements each family, including newer Redis/module
APIs rather than stopping at the original Redis 7.2 core baseline:

| Family | Representation and behavior |
| --- | --- |
| Strings | Binary-safe bytes, arithmetic, conditions and expiration |
| Bitfields | Packed signed/unsigned counters with overflow policies |
| Bitmaps | Bit addressing, counting, searching and boolean operations |
| Arrays | Sparse unsigned-index strings, cursor/ring writes and predicates |
| Geospatial indexes | Morton-encoded sorted sets, distance and radius/box queries |
| Hashes | Binary fields/values and independent field expiration |
| JSON | Ordered document trees, legacy paths, JSONPath filters/functions and atomic multi-key writes |
| Lists | Ordered binary payloads and blocking pops/moves |
| Bloom filters | Scalable probabilistic bit filters |
| Count-Min Sketch | Bounded counter rows and weighted merges |
| Cuckoo filters | Fingerprint buckets, relocation, counting and deletion |
| HyperLogLog | Redis-compatible HYLL registers and cardinality unions |
| t-digest | Weighted centroids, quantiles, CDFs and rank queries |
| Top-K | HeavyKeeper counters and a retained-member min-heap |
| Sets | Unique binary members, sampling and algebra |
| Sorted sets | Score/lexicographic indexes, ranges and algebra |
| Streams | Ordered IDs, consumer groups, pending entries, claims, NACKs and producer idempotency |
| Time series | Compressed/uncompressed chunks, labels, retention, aggregation and live compaction rules |
| Vector sets | Multilayer HNSW, quantization, projection, attributes and filtered nearest-neighbor search |

Family and catalog coverage is **not exhaustive option, wire-format or version
parity**. Redis 7.2 was the initial core baseline; current command contracts follow
the Redis 8.10 catalog and documented RedisJSON/RedisBloom APIs. `COMMAND LIST`
and `COMMAND INFO` describe the running engine. Recent array reads preserve holes
and clamp an exhausted cursor
to the surviving tail, including fixes absent from Redis 8.10. Integer array
aggregates preserve exact integer arithmetic; real/mixed arithmetic uses binary64,
not platform-specific C extended precision.

#### Try the new types independently

Keep an existing instance running; use a different port and a separate directory:

```sh
cargo build --release --bins
target/release/ffkv --bind 127.0.0.1:6382 --data-dir ./ffkv-types-data --durability strict
```

In another terminal, use the native client from that build:

```sh
target/release/ffkv-cli -p 6382 ARMSET demo:array 2 two 5 five
target/release/ffkv-cli -p 6382 ARSCAN demo:array 0 10
target/release/ffkv-cli -p 6382 GEOADD demo:places 13.361389 38.115556 Palermo
target/release/ffkv-cli -p 6382 GEOSEARCH demo:places FROMMEMBER Palermo BYRADIUS 1 km
target/release/ffkv-cli -p 6382 PFADD demo:visitors alice bob alice
target/release/ffkv-cli -p 6382 PFCOUNT demo:visitors
target/release/ffkv-cli -p 6382 JSON.SET demo:document '$' '{"items":[{"price":2},{"price":9}]}'
target/release/ffkv-cli -p 6382 JSON.GET demo:document '$.items[?(@.price > 5)]'
target/release/ffkv-cli -p 6382 XADD demo:events '*' job send-email
target/release/ffkv-cli -p 6382 XRANGE demo:events - +
target/release/ffkv-cli -p 6382 TS.ADD demo:temperature 1000 21.5 LABELS room office
target/release/ffkv-cli -p 6382 TS.RANGE demo:temperature - +
target/release/ffkv-cli -p 6382 BF.ADD demo:seen alice
target/release/ffkv-cli -3 -p 6382 BF.EXISTS demo:seen alice
target/release/ffkv-cli -p 6382 CF.RESERVE demo:members 128
target/release/ffkv-cli -p 6382 CF.ADD demo:members alice
target/release/ffkv-cli -p 6382 CF.COUNT demo:members alice
target/release/ffkv-cli -p 6382 CMS.INITBYDIM demo:counts 128 4
target/release/ffkv-cli -p 6382 CMS.INCRBY demo:counts alice 3
target/release/ffkv-cli -p 6382 CMS.QUERY demo:counts alice
target/release/ffkv-cli -p 6382 TOPK.RESERVE demo:popular 2
target/release/ffkv-cli -p 6382 TOPK.INCRBY demo:popular alice 20 bob 1
target/release/ffkv-cli -p 6382 TOPK.LIST demo:popular WITHCOUNT
target/release/ffkv-cli -p 6382 TDIGEST.CREATE demo:latency
target/release/ffkv-cli -p 6382 TDIGEST.ADD demo:latency 0 100 200
target/release/ffkv-cli -p 6382 TDIGEST.QUANTILE demo:latency 0.5
target/release/ffkv-cli -p 6382 VADD demo:vectors VALUES 3 1 0 0 east NOQUANT
target/release/ffkv-cli -3 -p 6382 VSIM demo:vectors ELE east COUNT 1 WITHSCORES
```

All families participate in key TTLs, WATCH, ACLs, WAL recovery, checkpoints and
backups in both storage profiles. Random choices and generated identifiers are
resolved before journaling. Time-series writes coordinate only their connected
source/destination keys; label queries need a coherent whole-dataset view.
`XREAD`/`XREADGROUP` and `TS.READ BLOCK` use bounded actor waiters, not data polling.
Stream reads in EXEC/Lua are nonblocking; `TS.READ BLOCK` is rejected there.

#### Extended datatype limits and interoperability

- JSON: 4 MiB compact documents, 262,144 nodes, depth 128, 16 KiB paths,
  65,536 matches and bounded expression/projection work.
- Arrays: indices through `2^64-2`; `ARSEEK` also accepts the terminal `2^64-1`.
  Dense range reads are capped at one million positions; sparse recent walks
  are bounded separately. `ARSCAN` returns index/value pairs, not a SCAN cursor.
- Bloom/Cuckoo: 64 MiB backing arrays per key; bounded expansion/relocation.
  `SCANDUMP`/`LOADCHUNK` round-trip ffkv's versioned, checksummed **FFKF** format,
  not RedisBloom's native packed-memory dumps. They are not a binary migration
  path from RedisBloom.
- CMS/HeavyKeeper: at most 8,388,608 cells; t-digest compression at most 1,000,000.
  These are probabilistic/approximate structures, not exact sets or counters.
- Vector sets: 65,536 dimensions, 64 KiB members, 1 MiB attributes, bounded
  M/EF/search work and a 1 GiB estimated per-set allocation limit. Ordinary
  searches traverse HNSW; `TRUTH` explicitly requests exhaustive search.
- Stream waiters: at most 65,536 clients and 64 MiB retained logical request
  data, further limited by the configured connection cap. Approximate MAXLEN
  trimming uses 100-entry granularity. Physical index statistics describe ffkv's
  actual representation, not Redis's allocator layout.
- New typed structures remain RAM-resident in the hybrid profile; hybrid does
  not imply that every document, graph or sketch is automatically SSD-offloaded.
  Their logical durability/recovery is shared with the memory profile.

The React command console uses the complete running catalog and supports the
new response shapes. Its key-value browser remains limited to the original
string/list/set/hash/sorted-set/native-queue inspectors; use the command console
or CLI for extended types. Blocking reads are CLI-only and are rejected by the
management command endpoint.





### Transactions, Lua, and Sidekiq

RESP2 remains the connection default. RESP3 can be selected with `HELLO 3`
or `ffkv-cli -3 -p 6380`; maps, sets, doubles, and nulls use their RESP3 types.
`HELLO` identifies the server as ffkv. `INFO` exposes `server_name:ffkv`,
`ffkv_version`, and the `redis_version:7.2.0` compatibility baseline used by clients.
It does not claim the server is Redis.

Use one interactive connection for transaction state:

```text
ffkv-cli -2 -p 6380
WATCH demo:balance
MULTI
SET demo:balance 42
INCR demo:transactions
EXEC
```

`EXEC` pauses shard admission and publishes one atomic durable batch. WATCH
detects writes, delete/recreate, and expiration; an invalid watch returns a null
array. Queue-time command/arity errors cause `EXECABORT`; runtime command errors
appear in the result array without rolling back earlier writes or skipping later
commands. Blocking pops inside EXEC never wait. Outside blocked consumers wake
only after the final transaction/script list state is committed.
Limits: 1,024 queued commands, 16 MiB queued argument bytes, 1,024 watched keys.
Transaction admission follows each command's registry flags; `NO_MULTI` commands
such as `SAVE` are rejected before execution. `SELECT` changes the transaction's
database context, and WATCH identities include the database number.

```sh
ffkv-cli -2 -p 6380 EVAL 'return redis.call("INCR", KEYS[1])' 1 demo:lua
ffkv-cli -2 -p 6380 SCRIPT LOAD 'return redis.call("GET", KEYS[1])'
# Pass the returned SHA to: EVALSHA <sha> 1 demo:lua
ffkv-cli -2 -p 6380 BITFIELD demo:metrics OVERFLOW SAT INCRBY u16 '#0' 1
```

Scripts use actual sandboxed Lua 5.1, binary KEYS/ARGV, redis.call/pcall,
cjson encode/decode/null, bit operations, and isolated invocation globals.
Writes preceding a Lua runtime error still commit, matching Redis's lack of
rollback; resolved effects, not scripts or random outcomes, are replayed.
Filesystem/process APIs, loaders, environment/metatable access and recursive
scripting remain unavailable. Registry `NOSCRIPT` restrictions are enforced.
`SELECT` is invocation-local; blocking list commands never wait inside Lua.
Limits: 1 million Lua instructions, 32 MiB Lua memory, 64 nesting levels,
8 MiB scripts/bridge values/replies, 10,000 bridge calls, 16 MiB cumulative
bridge argument bytes. These are not a process/dataset memory cap.
The SHA cache is volatile and bounded to 256 scripts/16 MiB, with FIFO eviction
for both LOAD and EVAL entries; callers must handle NOSCRIPT. This intentionally
differs from Redis 7.2's unbounded script cache. SCRIPT FLUSH accepts SYNC/ASYNC
but clears this bounded cache inline.

Named Lua libraries use `FUNCTION LOAD/LIST/DELETE/FLUSH/DUMP/RESTORE`, `FCALL`
and `FCALL_RO`. Library source is durable; the SHA cache remains volatile.
There are at most 256 libraries, and the encoded library catalog must fit one
16 MiB WAL frame. An oversized load/restore fails before replacing live libraries.
`SCRIPT DEBUG YES` discards dataset effects; `SYNC` retains them. The cooperative
debugger supports stepping, console events and eligible SCRIPT/FUNCTION KILL.
Disconnect cancellation survives actor queuing and terminates continued loops.
RESP3 Lua maps, sets, verbatim strings and arbitrary decimal big-number replies
preserve their wire types; RESP2 receives the corresponding compatible encoding.

Tested unmodified client stack:

| Component | Pinned version |
| --- | --- |
| Ruby | 3.4.6 |
| Sidekiq OSS | 8.0.7 |
| redis-client | 0.26.2 |
| connection_pool | 2.5.4 |
| Rack / rack-session | 3.2.6 / 2.1.2 |
| Puma | 8.0.0 |

```sh
BUNDLE_GEMFILE=tests/sidekiq/Gemfile bundle install
python3 tests/sidekiq_smoke.py --binary target/debug/ffkv
```

The real worker CLI, default metrics middleware, scheduler/retry Lua,
heartbeat renewal, process metadata, stats, and session-backed Sidekiq Web
pages are exercised. The fixture retains logs under target on failure.
connection_pool is pinned because 3.0.2 changes the TimedStack API used by this
Sidekiq release; no installed gem source is patched. The integration's normal
BRPOP fetcher removes jobs on delivery: it does **not** provide native leases or
crash-after-fetch redelivery.

An additional local application integration exercised Ruby 4.0.9, Rails 8.1.2.1,
Sidekiq 8.1.1, redis 5.4.1, redis-client 0.29.0, connection_pool 3.0.2,
sidekiq-cron 2.4.0 and sidekiq-unique-jobs 8.0.13 without modifying those gems.
The actual worker exercised `until_executing` duplicate rejection and
`until_and_while_executing` replacement conflicts, lock release/reacquisition,
cron registration/due-slot deduplication and execution after WAL recovery.
An actual subscriber received the uniqueness middleware's Lua-published changelog;
history and locking remained enabled. Rails cache serialization/counters,
session middleware across two requests, and rate-limit counters were also exercised.
This used the application's installed bundle; the self-contained repository fixture
above remains pinned to Sidekiq 8.0.7.

### Pub/Sub

```sh
ffkv-cli -2 -p 6380 SUBSCRIBE demo:events
```

In another terminal:

```sh
ffkv-cli -2 -p 6380 PUBLISH demo:events 'hello from ffkv'
```

Direct and pattern subscribers receive binary-safe messages. RESP2 subscribed
connections accept only subscription changes, PING and QUIT; RESP3 uses push frames
and permits ordinary commands, including pushes while a blocking list command waits.
`PUBLISH` and `SPUBLISH` work inside Lua and EXEC; those publications become visible
after the mutation batch commits. Runtime script errors retain earlier effects;
failed WAL commits do not publish. Subscriptions and messages are ephemeral,
never restored from WAL or snapshots, and provide no offline delivery. Native
replication propagates live publication traffic without persisting subscriptions.

Each client is limited to 1,024 channel/pattern registrations and 8 MiB of names;
patterns are at most 1,024 bytes. Output is bounded to 256 events and 16 MiB per
client, including staged and in-flight messages; overflow disconnects slow clients.
Retained publication bodies and acknowledgements do not pin oversized ingress
allocations. A publication exceeding the shared pattern-matching work budget is
rejected before delivery without disconnecting subscribers.

Channel ACL grants use `&pattern`, `allchannels` and `resetchannels`, independently
of `~key` permissions. Pattern subscriptions require the exact granted pattern
unless all channels are granted; global PUBSUB inspection requires `allchannels`.
For example, a jobs user's channel grant can be `&uniquejobs:changelog`.
Subscription commands are intentionally unavailable inside MULTI or Lua.

`SSUBSCRIBE`, `SUNSUBSCRIBE`, `SPUBLISH` and `PUBSUB SHARDCHANNELS/SHARDNUMSUB`
provide shard-channel delivery. In ffkv cluster mode, real peer links forward
global publications and deliver shard publications to the shard's replicas.


### Supported commands

Commands and option names are case-insensitive. Keys and values are byte strings,
not UTF-8 text. Standalone mode has 16 logical databases by default, configurable
with `--databases`; `SELECT`, `COPY ... DB`, `MOVE` and `SWAPDB` share that limit.
Cluster mode exposes database zero.

[`src/command_catalog.json`](src/command_catalog.json) pins the
[Redis 8.10 command catalog](https://redis.io/docs/latest/commands/redis-8-10-commands/).
[`tests/bdd/features`](tests/bdd/features) contains executable behavior scenarios;
the runner requires both a passing tagged scenario and a positive observed
native-client invocation for every catalog entry. Tags alone do not pass the gate.
`COMMAND COUNT` counts root commands, including ffkv extensions, whereas the
486-entry catalog also names subcommands. These are different counts.

Coverage includes the datatype APIs above, expanded strings/lists/sets/sorted sets,
key transfer/sorting, scripting/functions/debugging, client controls, ACL management,
Pub/Sub, configuration/persistence/backup diagnostics, native modules, replication
and cluster commands. `COMMAND LIST`, `COMMAND INFO`, `COMMAND DOCS`,
`COMMAND GETKEYS` and `COMMAND GETKEYSANDFLAGS` describe the actual registry,
including successfully loaded native modules.

The table is a compact core reference, not the exhaustive catalog:

| Group | Supported syntax |
| --- | --- |
| Connection | `PING [message]`, `ECHO message`, `QUIT`, `SELECT index`, `AUTH [username] password`, `HELLO [2|3] [AUTH username password] [SETNAME name]`, `ACL`, `CLIENT`, `INFO [section ...]` |
| Pub/Sub | `PUBLISH`, `SUBSCRIBE/UNSUBSCRIBE`, `PSUBSCRIBE/PUNSUBSCRIBE`, `SPUBLISH`, `SSUBSCRIBE/SUNSUBSCRIBE`, `PUBSUB` |
| Strings | `GET`, `SET [options]`, `MGET`, `MSET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `GETDEL`, `GETEX [EX/PX/EXAT/PXAT/PERSIST]`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `MSETNX` |
| Bitmaps | `GETBIT`, `SETBIT`, `BITCOUNT [start end [BYTE|BIT]]`, `BITPOS`, `BITOP AND/OR/XOR/NOT`, `BITFIELD [GET/SET/INCRBY/OVERFLOW WRAP/SAT/FAIL]`, `BITFIELD_RO` |
| Transactions / Lua | `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`, `EVAL/EVAL_RO`, `EVALSHA/EVALSHA_RO`, `SCRIPT`, `FUNCTION`, `FCALL/FCALL_RO` |
| Arithmetic | `INCR key`, `DECR key`, `INCRBY key integer`, `DECRBY key integer` |
| Lists | `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP key [count]`, `RPOP key [count]`, `LRANGE key start stop`, `LLEN`, `LINDEX`, `LSET`, `LTRIM`, `LREM`, `LMOVE source destination LEFT\|RIGHT LEFT\|RIGHT` |
| Blocking lists | `BLPOP key [key ...] timeout`, `BRPOP key [key ...] timeout`, `BLMOVE source destination LEFT\|RIGHT LEFT\|RIGHT timeout` |
| Keys | `DEL`, `UNLINK`, `EXISTS`, `TOUCH`, `TYPE`, `DBSIZE`, `RENAME/RENAMENX`, `SCAN`, `KEYS`, `COPY`, `MOVE`, `SWAPDB`, `SORT/SORT_RO`, `DUMP/RESTORE`, `MIGRATE`, `FLUSHDB/FLUSHALL [SYNC\|ASYNC]` |
| Expiration | `EXPIRE key seconds [options]`, `PEXPIRE key milliseconds [options]`, `EXPIREAT key unix-seconds [options]`, `PEXPIREAT key unix-milliseconds [options]` |
| TTL inspection | `TTL`, `PTTL`, `EXPIRETIME`, `PEXPIRETIME`, `PERSIST` |
| Introspection | `COMMAND`, `COMMAND INFO [name ...]`, `COMMAND COUNT`, `COMMAND GETKEYS command [args ...]` |
| Sets | `SADD`, `SREM`, `SMEMBERS`, `SCARD`, `SISMEMBER`, `SMISMEMBER`, `SPOP key [count]`, `SRANDMEMBER key [count]`, `SMOVE`, `SUNION`, `SINTER`, `SDIFF`, their STORE forms, `SSCAN` |
| Hashes | `HSET`, `HMSET`, `HSETNX`, `HGET`, `HMGET`, `HGETALL`, `HDEL`, `HEXISTS`, `HLEN`, `HKEYS`, `HVALS`, `HSTRLEN`, `HINCRBY`, `HSCAN` |
| Hash field TTL | `HSETEX`, `HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HEXPIRETIME`, `HPEXPIRETIME`, `HPERSIST` |
| Sorted sets | `ZADD [NX/XX/GT/LT/CH/INCR]`, `ZINCRBY`, `ZREM`, `ZCARD`, `ZSCORE`, `ZMSCORE`, `ZRANK`, `ZREVRANK`, `ZCOUNT`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZSCAN`, `ZPOPMIN`, `ZPOPMAX` |
| Native jobs | `FQ.ENQUEUE/CLAIM/ACK/NACK/EXTEND/RETRY/CANCEL/CONFIG/STATS/JOBS/GET` — complete forms below |
| Storage administration | `FFKV.STATS`, `FFKV.COMPACT`, `FFKV.CHECKPOINT`, `FFKV.BACKUP name` |

`SET` accepts `NX` or `XX`, `GET`, and one expiration mode:
`EX seconds`, `PX milliseconds`, `EXAT unix-seconds`, `PXAT unix-milliseconds`,
or `KEEPTTL`. Ordinary `SET`/`MSET` clear an existing TTL; arithmetic preserves it.
A failed condition does not change the value or TTL. With `GET`, SET returns the
previous value even when the NX condition prevents a write.
`SET` can replace a list, but `SET ... GET` on a list returns `WRONGTYPE` without
changing it. String arithmetic and `GET` reject lists; `MGET` returns null for
non-string keys. List commands reject string keys. Counts must be nonnegative
integers; `LRANGE` uses inclusive bounds and supports negative indexes.

Expiration commands accept `NX`, `XX`, `GT`, and `LT`. `NX` conflicts with all
other conditions; `GT` and `LT` conflict with each other; `XX` can combine with
`GT` or `LT`. TTL replies distinguish missing keys (`-2`) from persistent keys
(`-1`). Expiration uses wall-clock deadlines, with lazy removal on access and
bounded background reclamation.

Hash field expiry syntax uses `FIELDS n field ...`; `HSETEX` uses
`[FNX|FXX] [EX n|PX n|EXAT n|PXAT n|KEEPTTL] FIELDS n field value ...`.
Expiry updates accept one of NX/XX/GT/LT. TTL results are per-field arrays;
field-expiration seconds round upward. `HSCAN` supports MATCH, COUNT, NOVALUES.
Sorted-set rank ranges support negative indexes; score ranges support infinities
and exclusive bounds. `ZRANGE` supports BYSCORE, REV, LIMIT, WITHSCORES.
`UNLINK` and `FLUSHDB/FLUSHALL ASYNC` remove logical ownership immediately and
release retired payloads on dedicated background workers. `SORT` reads only
the external BY keys and selected GET keys it needs, across owning shards.

Unknown commands and unsupported options return errors, not success-shaped no-ops.
String/bitmap writes remain capped at 8 MiB per stored value; request framing can
reject a large input before command execution.

#### Native replication, cluster and compatibility boundaries

`REPLICAOF`/`SLAVEOF`, `SYNC`/`PSYNC`, `REPLCONF`, `ROLE`, `WAIT`, `WAITAOF`
and `FAILOVER` use the RESP connection, independently of the development HTTP
snapshot poller below. A loopback follower explicitly opts into the protected
upstream network policy:

```sh
# Two terminals, fresh directories, separate from an existing server on 6380:
target/debug/ffkv --bind 127.0.0.1:6382 --data-dir ./ffkv-source --durability strict --native-replication-allow-loopback
target/debug/ffkv --bind 127.0.0.1:6383 --data-dir ./ffkv-follower --durability strict --native-replication-allow-loopback

# Another terminal:
target/debug/ffkv-cli -p 6383 REPLICAOF 127.0.0.1 6382
target/debug/ffkv-cli -p 6383 INFO replication
target/debug/ffkv-cli -p 6383 ROLE
```

Replication is asynchronous. Use WAIT/WAITAOF on the writing connection when an
acknowledgment barrier is required. Fsync acknowledgments reflect actual durable
watermarks. Followers retain source-owned expired payloads and queue clocks until
resolved source effects arrive. Full-image replacement fences downstream sessions,
including replacements at the same numeric offset.

- Cluster commands require `--cluster-enabled`. ffkv implements hash slots,
  MOVED/ASK routing, peer membership, migration and quorum-based automatic/manual
  failover with its own authenticated bus. Heartbeats and votes use bounded control
  deadlines independently of data transfers. It does **not** join a Redis cluster bus.
- `MODULE LOAD/LOADEX/LIST/UNLOAD/HELP` host trusted native libraries through an
  explicit Redis Modules API v1 subset. String/reply/allocation, synchronous Call,
  basic key access, command metadata and immutable load-time string configuration
  are supported. Module datatypes, persistence callbacks, blocking/thread/timer/
  event/shared-API facilities are not implemented. A library requiring a missing
  API fails load; native code is not sandboxed, and its private heap is outside
  dataset memory estimates.
- Search worker configuration controls a real Rayon pool. GC controls schedule
  real generation-fenced Rust compaction, not a POSIX fork. MEMORY reports owned
  allocation estimates, not Redis/jemalloc accounting.
- Catalog coverage is not a claim that every Redis option combination, third-party
  module ABI, error message or pathological input is identical. The protocol and
  behavioral checks are explicit in the BDD features; no exhaustive upstream
  Redis conformance or production-HA certification is claimed.

### Verify

The BDD suite uses the real server and native `ffkv-cli`, not an in-memory test
double or an installed Redis server. It requires Python 3, a C compiler and
OpenSSL for owned native-module/TLS fixtures:

```sh
cargo build --bins
python3 -m venv .ffkv-bdd-venv
.ffkv-bdd-venv/bin/python -m pip install -r tests/bdd/requirements.txt
.ffkv-bdd-venv/bin/python tests/bdd/run.py --inventory
.ffkv-bdd-venv/bin/python tests/bdd/run.py --evidence-report .ffkv-bdd-evidence.json
.ffkv-bdd-venv/bin/python -m unittest discover -s tests/bdd -p 'test_*.py'
```

`--inventory` checks names only. A full run checks all command tags and observed
positive invocations. `--feature path` or `--tags expression` runs a scoped subset
and explicitly does not claim full-catalog verification. Nodes, credentials,
ports, data directories, fault gates and native libraries belong to each scenario;
an existing server on port 6380 is not used or restarted.

To verify optimized binaries, run `cargo build --release --bins` and pass
`--server target/release/ffkv --client target/release/ffkv-cli` to the same runner.
The optional evidence JSON is bounded to one positive `command/status/scenario`
record per catalog entry; it does not store command arguments or credentials.

Redis 8.10 catalog rollout verification:

| Gate | Observed result |
| --- | --- |
| Optimized server and native client | `cargo build --release --bins` passed |
| Catalog coverage | 486/486 positive observed invocations in passed scenarios |
| Full BDD run on optimized binaries | 26 features, 585 scenarios, 5,979 steps passed; zero failures/skips |
| Rust tests | 477 server + 16 native client tests passed |
| Strict Clippy | `cargo clippy --all-targets -- -D warnings` passed |
| Existing CLI smoke suites | All 17 passed, including real Sidekiq, TLS/ACL, replication, durability and all 19 data families |
| Evidence classifier tests | 11 passed |

```sh
cargo test
cargo build
cargo clippy --all-targets -- -D warnings
python3 tests/cli_smoke.py
python3 tests/queue_smoke.py
python3 tests/durability_smoke.py
python3 tests/sets_smoke.py
python3 tests/hashes_smoke.py
python3 tests/sorted_sets_smoke.py
python3 tests/shards_smoke.py
python3 tests/strings_smoke.py
python3 tests/transactions_smoke.py
python3 tests/scripting_smoke.py
python3 tests/native_queues_smoke.py
python3 tests/security_smoke.py
python3 tests/replication_smoke.py
python3 tests/sidekiq_smoke.py
python3 tests/pubsub_smoke.py
python3 tests/client_smoke.py
python3 tests/datatypes_smoke.py
```

The Python harnesses use the standard library; Sidekiq additionally requires the
pinned Ruby/Bundler dependencies above. Each launches an isolated ffkv process
on an OS-assigned port, exercises the actual socket endpoint, and stops it afterward.
String scenarios cover binary/empty keys and values, fragmented
frames, pipelining, TTL preservation/expiry, arithmetic errors without mutation,
concurrent atomic increments, and malformed-request isolation. Queue scenarios
cover ordering, count/range boundaries, cross-type operations, list TTLs, blocking
key priority, waiting-client FIFO, exact timeout frames, duplicate watched keys,
pipeline ordering, and disconnect cancellation. The FIFO socket setup uses
bounded scheduling allowances; actor regression tests check order deterministically.
The durability smoke owns temporary data directories and tests orderly/SIGKILL
restart, absolute TTLs, delivered and pending pops, directory locks, both fsync
policies, torn atomic records, corruption refusal, and real WAL write failure.
The everysec scenario exercises the timer but cannot independently observe a
storage device's fsync. POSIX file-limit injection reports a skip if unavailable.
Collection smokes include optional Redis reference runs and actual SIGKILL
recovery. The shard smoke exercises one and four actors, concurrent whole-tuple
multi-key updates, global scans, cross-key wakeups, and durable `4 → 1 → 4`
redistribution, including a 1,400-member STORE and exact 1,200-member SPOP recovery.

Optional differential run, using a **disposable** reference Redis:

```sh
# Separate terminal; no disk persistence enabled.
redis-server --bind 127.0.0.1 --port 16380 --save '' --appendonly no

# From this repository; writes ffkv:test:* keys to the reference.
python3 tests/cli_smoke.py --reference 127.0.0.1:16380
python3 tests/queue_smoke.py --reference 127.0.0.1:16380
python3 tests/strings_smoke.py --reference 127.0.0.1:16380
```

The string smoke adds 1,200 deterministic randomized operations compared against
Redis. The queue smoke repeats its scenarios on the reference and compares 1,600
seeded list/string operations, including return values, error categories, resulting
state, and nonmutation on error; it does not pin error-message prose.
Earlier core differential smokes used Redis **8.2.1**. Command metadata and newer
behavioral contracts are pinned to Redis 8.10 sources/documentation. A complete
live 8.10 or pinned 7.2 differential conformance run has not been performed;
the self-hosted BDD gate does not claim otherwise.

### Implementation and limits

- `src/protocol.rs`: incremental request parsing and RESP2/RESP3 response encoding.
- `src/engine.rs`: command registry, logical strings/lists, and an exact expiration index.
- `src/engine/`: collection, keyspace, and stable cursor implementations.
- `src/journal.rs`: resolved logical mutation batches with absolute deadlines.
- `src/wal.rs`: framed CRC32C WAL, locking, replay, and fsync policy.
- `src/blocking.rs`: command actor, per-key consumer ordering, deadlines, and cancellation cleanup.
- `src/sharding.rs`: independent actor ownership and coordinated transfer barrier.
- `src/persistence.rs`: shared ordered WAL gate and fail-stop state.
- `src/main.rs`: TCP connections and a bounded command channel feeding the actor.
- `src/connection.rs`, `src/request.rs`, `src/state.rs`: per-client state, handshakes, and actual client metadata.
- `src/executor.rs`, `src/watch.rs`: frozen atomic execution and bounded-lifetime WATCH versions.
- `src/scripting.rs`, `src/functions.rs`, `src/scripting_control.rs`: bounded Lua, durable libraries, cancellation and debugger ownership.
- `src/storage.rs`, `src/queue.rs`, `src/engine/native.rs`: checksummed offloaded bodies and the leased-job state machine.
- `src/backup.rs`, `src/engine/snapshot.rs`, `src/admin.rs`: coherent checkpoint/backup export and verified restore.
- `src/auth.rs`, `src/transport.rs`, `src/pressure.rs`: selector-safe mutable ACLs, denial auditing, TLS and sampled-RSS admission.
- `src/management/`, `src/metrics.rs`, `apps/console/`: authorized/audited management, actual metrics and React UI.
- `src/replication.rs`: authenticated development snapshot polling and read-only follower installation.
- `src/native_replication.rs`, `src/native_runtime.rs`, `src/cluster/`: RESP replication, actual durability watermarks, cluster routing and transfer.
- `src/server_commands/`, `src/server_runtime.rs`: configuration, diagnostics, online backup and trusted native module hosting.
- `tests/bdd/`: versioned CLI behavior scenarios, isolated multi-node fixtures and positive command-coverage evidence.
- `deploy/`, `scripts/migrate.py`, `benchmarks/`: self-hosting, quiesced migration and reproducible measurements.
- `tests/cli_smoke.py`: live-process string scenarios and optional differential checks.
- `tests/queue_smoke.py`: producer/consumer, list lifecycle, and differential scenarios.
- `tests/durability_smoke.py`: restart, crash, partial-write, corruption, and write-error scenarios.

Each shard actor owns its mutable state. A shared read/write admission barrier
allows ordinary actors to progress independently and fences coordinated commands.
Protocol code has no dependency on the storage implementation. Bulk responses
are streamed without building another whole-payload response buffer.
The actor registers blocking consumers atomically with the initial key scan,
so a push cannot be lost between checking and waiting. Timers use monotonic
deadlines; per-key queues and a deadline index are removed on completion.
Closed receivers are skipped before delivery and idle cancellation cleanup runs
every 100 ms. TCP connections buffer at most 8 MiB of read-ahead pipeline data
while awaiting a reply, then apply backpressure; they do not execute it early.

Each request is limited to **8 MiB including framing** and **1,024 arguments**.
Malformed/oversized frames receive an error and their connection closes.
Simple space/tab-separated inline commands are accepted with CRLF, but inline
quoting/escaping is deliberately unsupported; use RESP bulk strings for arbitrary
bytes. The default connection cap is 1,024; excess connections are closed.
Client aggregate replies have a 64 MiB logical allocation budget; management uses
8 MiB. Oversized destructive count-pops/claims are rejected before removing data
or granting leases. Scan pages are not silently truncated. Long coordinated
commands/checkpoints can still delay other clients. There is no eviction or hard
dataset/RSS cap; the optional sampled-RSS circuit breaker below is a soft limit.
Use OS/container limits and capacity planning. Non-loopback RESP startup requires
TLS plus ACLs; non-loopback management requires TLS and every management listener
requires its separate bearer/OIDC policy. Plain loopback mode is for trusted local
development only.

## Step 6 — Hybrid storage and native jobs

### Hybrid RAM/SSD mode

```sh
cargo run --release -- --bind 127.0.0.1:6380   --data-dir ./ffkv-data --durability strict --storage hybrid   --inline-threshold 1024 --cache-bytes 67108864

ffkv-cli -3 -p 6380 FFKV.STATS
ffkv-cli -3 -p 6380 FFKV.COMPACT
```

Hybrid mode requires persistence. List elements and native job bodies larger than
`--inline-threshold` are stored in checksummed disk segments. Small bodies,
strings, hashes, set/sorted-set member identities, keys and indexes remain in RAM.
This is queue-payload offloading, not an SSD backend for every datatype.
`--cache-bytes` bounds cached body bytes, not process RSS or metadata. Dedicated
blocking owner threads perform cold I/O; ordinary reads do not block Tokio's
network workers. References remain valid across concurrent compaction and readers
pin old files until finished. Idle maintenance also reclaims retired files.

The WAL/checkpoint contains logical payload bytes, never physical segment
pointers. Startup acquires the database lock, rebuilds a fresh segment generation,
and removes recognized obsolete generations only after successful recovery.
Unknown files are not deleted. This needs space for the old and rebuilt data.
`--segment-bytes` controls segment rotation; `--disk-budget` (zero: unlimited)
counts derived segments and compaction/rebuild scratch. It does **not** bound the
WAL, backups, replica staging, filesystem free space or RAM. Leave disk headroom.
Live payload/WAL I/O failures stop the server rather than returning missing data;
a failed pre-cutover compaction leaves the previous live locations usable.

### Native leased jobs

Redis lists retain Redis remove-on-pop behavior. Native `FQ.*` queues are a
separate datatype (`TYPE` returns `ffkv-queue`) with durable receipts and leases.
They provide **at-least-once** execution, not exactly-once external side effects.
Workers must make side effects idempotent and extend leases before they expire.

```sh
ffkv-cli -3 -p 6380 FQ.CONFIG demo:jobs CONCURRENCY 4 MAXATTEMPTS 3 BACKOFF 1000 MAXBACKOFF 60000
ffkv-cli -3 -p 6380 FQ.ENQUEUE demo:jobs '{"job":"send-email","id":1}' IDEMPOTENCY email-1
ffkv-cli -3 -p 6380 FQ.ENQUEUE demo:jobs '{"job":"later","id":2}' DELAY 10000 PRIORITY 10
ffkv-cli -3 -p 6380 FQ.STATS demo:jobs
ffkv-cli -3 -p 6380 FQ.JOBS demo:jobs 0 COUNT 20 STATE ready
```

A complete CLI consumer, including acknowledgement of the actual returned receipt:

```sh
python3 - <<'PY'
import json, subprocess

def kv(*args):
    return json.loads(subprocess.check_output(
        ["ffkv-cli", "-3", "--json", "-p", "6380", *map(str, args)], text=True))

jobs = kv("FQ.CLAIM", "demo:jobs", "COUNT", 1, "VISIBILITY", 60000)
for job in jobs:
    print("Processing", job["job_id"], job["payload"])
    # Perform the real, idempotent side effect before acknowledging.
    print(kv("FQ.ACK", "demo:jobs", job["receipt"]))
if not jobs:
    print("No eligible job; CLAIM does not block")
PY
```

The example prints the demo payload; it does not send email. A real worker uses
its application handler before ACK. The command forms are:

| Command | Arguments after queue key |
| --- | --- |
| `FQ.ENQUEUE` | `payload [IDEMPOTENCY bytes] [DELAY ms|AT epoch_ms] [PRIORITY i32] [MAXATTEMPTS n]` |
| `FQ.CLAIM` | `[COUNT n] [VISIBILITY ms]` |
| `FQ.ACK` | `receipt` |
| `FQ.NACK` | `receipt [DELAY ms] [ERROR bytes]` |
| `FQ.EXTEND` | `receipt visibility_ms` |
| `FQ.RETRY` | `job_id [DELAY ms]` |
| `FQ.CANCEL` | `job_id` |
| `FQ.CONFIG` | `[CONCURRENCY n] [MAXATTEMPTS n] [BACKOFF ms] [MAXBACKOFF ms] [IDEMPOTENCY ms]` |
| `FQ.STATS` | none |
| `FQ.JOBS` | `cursor [COUNT n] [STATE ready|scheduled|running|dead]` |
| `FQ.GET` | `job_id` |

Higher priority wins; equal-priority eligible jobs use original enqueue order.
Claim increments attempts and returns the body, job ID, receipt and deadline.
Default visibility is 30 seconds; visibility/extension is bounded to 24 hours.
`FQ.EXTEND` returns the new absolute deadline, not `OK`. Expired, forged, old-
generation and old-epoch receipts cannot ACK/NACK/extend the current lease.
At the exact deadline the old lease is expired. Expiration or NACK retries with
bounded exponential backoff until max attempts, then retains the job as dead.
Explicit RETRY redrives a dead job, resets attempts, and preserves receipt
non-reuse. CANCEL removes a ready, scheduled, running or dead job.

Concurrency is per queue; zero means unlimited. Default max attempts is 25.
Idempotency retention defaults to five minutes, is bounded to 30 days, and
survives terminal ACK/cancel and restart. A duplicate returns the original job ID
with `created: 0`; it does not replace its body. Configuration affects future
jobs' max attempts, not existing jobs. Empty queues retain their policy/counters
until DEL/TTL; reading a missing queue's policy does not create it.
Pages/claims cap at 1,000 rows, each queue caps jobs/idempotency records at one
million, and one CLAIM caps logical body bytes at 32 MiB (the origin reply limit
can be lower). JOBS lists metadata; GET/CLAIM fetch bodies explicitly.
Stats are actual state counts and observed wait/run sums, not invented percentiles.

## Step 7 — Authenticated management console

### Run a protected native instance

Requires Node 22/npm for the console build and Python 3/OpenSSL for development
credentials. Bootstrap refuses an existing destination and prints no credentials.
Its self-signed development certificate expires in 30 days; use your PKI outside
local development. The following commands enable TLS on **both** listeners:

```sh
npm --prefix apps/console ci
npm --prefix apps/console run build
python3 deploy/bootstrap.py "$PWD/.ffkv-dev"

target/release/ffkv --bind 127.0.0.1:6380   --data-dir .ffkv-dev/data/database --durability strict --storage hybrid   --acl-file .ffkv-dev/users.acl   --tls-cert .ffkv-dev/tls.crt --tls-key .ffkv-dev/tls.key   --management-bind 127.0.0.1:6381   --management-token-file .ffkv-dev/management-tokens.json   --management-audit-file .ffkv-dev/data/management-audit.jsonl   --management-tls-cert .ffkv-dev/tls.crt --management-tls-key .ffkv-dev/tls.key   --console-dir apps/console/dist
```

Open `https://localhost:6381`, trusting only your own development certificate.
Use the private `console-viewer-token` or `console-admin-token` file in the login
field. Tokens live only in browser memory; refresh ends the session, and logout
clears cached data. Use the password in `admin-password` when prompted by:

```sh
ffkv-cli --tls --cacert .ffkv-dev/ca.crt -h localhost -p 6380 --user admin --askpass PING
python3 deploy/probe.py --host 127.0.0.1 --port 6380 --username health   --password-file .ffkv-dev/health-password --tls-ca .ffkv-dev/ca.crt --tls-server-name localhost
```

The React console includes live overview metrics, cursor-based binary-safe key
inspection, native/Sidekiq queue views, command vectors, the actual command
catalog and pinned client versions, storage/checkpoint/backup actions, and an
admin-only audit trail. Mobile navigation and keyboard focus are supported.
Sidekiq is inspected through its list/registry/schedule/retry/heartbeat keys; the
console does not pretend Sidekiq jobs have native leases or grant native remediation
on Sidekiq lists. The command console is not a shell. Data reads and safe
PING/ECHO/INFO/COMMAND controls work without write mode; writes require admin
role, explicit write enablement and confirmation. Server authorization remains
mandatory even if a browser control is bypassed.

Payloads are hidden by default for **both** roles. An explicit audited admin
reveal can show a sanitized body while recognized sensitive JSON fields stay
masked, including its base64 representation. This is not a general DLP system:
unrecognized secrets/PII can be visible on reveal. Only an operator-enabled
`--management-allow-sensitive-reveal` allows unmasked sensitive JSON. Receipts are
capabilities; audit records never contain credentials, arguments, bodies or receipts.
Every management write has a synchronized intent before dispatch and an outcome;
audit failure fails closed. A timeout/lost reply can have an unknown outcome.
Post-execution delivery failure explicitly says not to retry automatically.
Logical engine limits and final JSON/base64 limits are distinct: a command can
commit and then fail result delivery. In particular, do not blindly repeat a
destructive pop or lease claim after a delivery error.

All `/api/*` and `/metrics` routes require a bearer token. Key/queue browsing uses
bounded SCAN-family pages, not KEYS. Responses preserve binary bytes as base64;
large integers and infinities are represented without JavaScript precision loss.
Management ingress is 8 MiB/1,024 arguments, pages cap at 200, logical engine
responses at 8 MiB, and bus calls at 10 seconds. HTTP connections, active work,
websockets, frame sizes and global request rate are bounded. Same-origin checks
cover HTTP/1 Host and HTTP/2 authority. Remote proxies must set the explicit
`--management-origin`; no wildcard CORS or cookie authentication is enabled.

A CLI API request without placing a bearer token in command-line arguments:

```sh
python3 - <<'PY'
import json, ssl, urllib.request
from pathlib import Path
root = Path('.ffkv-dev')
request = urllib.request.Request('https://localhost:6381/api/overview', headers={
    'Authorization': 'Bearer ' + (root / 'console-viewer-token').read_text().strip()})
with urllib.request.urlopen(request, context=ssl.create_default_context(cafile=str(root / 'ca.crt')), timeout=10) as response:
    print(json.dumps(json.load(response), indent=2))
PY
```

Endpoints: `/api/session`, `/api/overview`, `/api/keys`, `/api/key`, `/api/queues`,
`/api/queue`, `/api/commands`, `/api/storage`, `/api/audit`; confirmed admin POSTs
use `/api/command`, `/api/queue/action`, `/api/storage/action`. `/api/live` is an
authenticated websocket: its first JSON frame contains the token within two
seconds, not in the URL. It streams real samples; REST polling is the fallback.
RSS is sampled from the current process; latency percentiles are approximate
since-boot histogram bucket bounds. Browser history is observed session data,
not synthetic or persisted monitoring history. Lag is unavailable when unknown.

Optional OIDC validates real RS256/ES256 JWT signatures against bounded cached
JWKS, issuer, audience, subject, expiry/not-before and configured admin/viewer
roles. Configure `--management-oidc-issuer`, `--management-oidc-audience`,
`--management-oidc-jwks-url` and optional role flags. HTTPS is required; the explicit
loopback HTTP flag exists only for local issuer tests. A token-policy file is
still required; `{"tokens":[]}` permits OIDC-only access. No fallback bypass is
provided. ACL/token-policy files are loaded at startup; restart to rotate them.

## Step 8 — Operations and hardening

### Checkpoints, backups and offline restore

These examples use the unprotected loopback Step 6 listener; add the protected
connection options above when TLS/ACLs are enabled.

```sh
ffkv-cli -3 -p 6380 FFKV.CHECKPOINT
ffkv-cli -3 -p 6380 FFKV.BACKUP manual-1

target/release/ffkv restore --source ./ffkv-data/backups/manual-1 --data-dir ./ffkv-restored
target/release/ffkv --bind 127.0.0.1:6382 --data-dir ./ffkv-restored --durability strict --storage hybrid
```

CHECKPOINT pauses coordinated execution, streams a coherent logical image, syncs
it and atomically replaces the WAL while retaining the directory lock. Automatic
checkpointing defaults to 256 MiB of WAL growth beyond the previous image;
`--wal-checkpoint-bytes 0` disables it. File-local frame sequences reset; they are
not replication offsets. Compaction reclaims derived payload segments separately.

BACKUP writes an immutable directory under `data-dir/backups/name` containing
`wal.ffkv` and a length/frame-count/SHA256 manifest. Names allow 1–64 ASCII letters,
digits, `_` and `-`, never arbitrary paths; existing destinations are refused.
Restore validates every complete WAL group and manifest without repairing the
source, and publishes only into a **new** destination. Its JSON includes the
verified checksum. Backups contain plaintext logical data: protect their storage
and transport. Checksums detect corruption; they do not authenticate an untrusted
backup producer. Do not copy a changing live WAL as a substitute for BACKUP.

Recovery procedure: stop/fence the damaged instance, preserve its original files,
restore a trusted backup into a new directory, start on a different local port,
verify application keys/queues, then point clients at it. Never delete LOCK to
bypass a live owner, overwrite the sole copy, or truncate a complete corrupt
record. Torn final writes are repaired automatically; complete corruption is a
startup error. Restored TTLs remain absolute and expired data is not resurrected.
Test restore procedures before relying on backups.

### TLS, ACLs and memory pressure

Non-loopback RESP binding requires both TLS and an ACL file. Non-loopback
management requires TLS; management always requires its own token/OIDC policy.
RESP TLS supports 1.2/1.3 and never falls back to plaintext. Startup files and
runtime `ACL SETUSER` support named users, SHA256 password hashes, ordered
command/category rules, selectors and binary key patterns. AUTH/HELLO AUTH,
transaction identities and each Lua callback are checked. Failed reauthentication
preserves the prior identity, and repeated credential failures close the connection
after the last error is flushed. Password-only `AUTH password` requires a default
password; explicit `AUTH username password` follows that user's `nopass` rule.

`--max-rss-bytes N` enables a **soft**, one-second-sampled process-RSS admission
circuit breaker; zero disables it. Growing commands, including EXEC entries and
scripts, return OOM before executing when pressured. Reads, deletions, collection
removals/pops, native ACK/CANCEL and maintenance remain usable. It is neither an
exact dataset budget nor eviction, and cannot prevent overshoot from concurrent
work, already accepted requests, recovery or internal maintenance. Use OS/container
limits and capacity planning too. INFO, Prometheus, overview and storage limits
expose the actual sample, configured soft limit and pressured state. Replica
READONLY takes precedence over pressure errors.

### Docker Compose and Kubernetes

```sh
python3 deploy/bootstrap.py "$PWD/.ffkv-compose"
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml up --build -d --wait
```

Compose publishes verified TLS RESP on localhost:6379 and the console on
`https://localhost:8080`. Override `FFKV_RESP_PORT`, `FFKV_MANAGEMENT_PORT`,
`FFKV_IMAGE`, `FFKV_STORAGE`, `FFKV_DURABILITY`, `FFKV_MEMORY_LIMIT` or
`FFKV_MAX_RSS_BYTES` as needed. The native bootstrap owner runs the container as a
nonroot UID/GID with private bind-mounted files, read-only rootfs, dropped
capabilities, no-new-privileges, bounded tmpfs, persistent data and a real TLS AUTH
PING healthcheck. The bridge permits egress; published ports default to loopback.
Use production certificates/secret management for remote deployments.
`docker compose ... down` stops/removes containers, not the bind-mounted data.

The Helm chart is `deploy/helm/ffkv` (one StatefulSet, PVC, TLS services,
startup/readiness/liveness probes and restricted security contexts). This requires
kubectl, Helm 3, a cluster with persistent storage and an image available to its
runtime. For a local cluster sharing Docker images (such as OrbStack), use the
development files created above in a new namespace:

```sh
docker build -t ffkv:0.1.0 .
kubectl create namespace ffkv
kubectl --namespace ffkv create secret generic ffkv-config \
  --from-file=users.acl=.ffkv-compose/users.acl \
  --from-file=management-tokens.json=.ffkv-compose/management-tokens.json \
  --from-file=health-password=.ffkv-compose/health-password \
  --from-file=tls.crt=.ffkv-compose/tls.crt \
  --from-file=tls.key=.ffkv-compose/tls.key \
  --from-file=ca.crt=.ffkv-compose/ca.crt
helm lint deploy/helm/ffkv --set existingSecret=ffkv-config
helm upgrade --install ffkv deploy/helm/ffkv --namespace ffkv --set existingSecret=ffkv-config
```

For another runtime, load/publish the image and set `image.repository/tag`.
Development certificates cover localhost, not arbitrary cluster service names;
test through `kubectl --namespace ffkv port-forward service/ffkv-ffkv 6383:6379 8083:8080`
and use localhost with the generated CA. Replace certificates for service-DNS
clients. The six Secret members above are required; no credentials are embedded.

Set `image.repository/tag`, resource limits, `maxRssBytes`, storage class/size or
an existing claim in values. Secret projection is copied by an unprivileged init
container into private files compatible with the server's file checks. Certificate
SANs must include localhost for probes and actual service/client DNS names.
NetworkPolicy allows labelled `ffkv-client: "true"` pods and denies egress;
provide appropriate network policy for OIDC/replica sources if enabled. A rolling
restart is single-node downtime, not HA. Back up PVC data before infrastructure
changes; do not treat pod restart as backup.

### Quiesced migration

```sh
python3 scripts/migrate.py --source 127.0.0.1:6379 --target 127.0.0.1:6380 --source-quiesced
```

Stop source writers and make the target empty/exclusive first. The standard-library
migration client copies strings/lists/sets/hashes/sorted sets, binary/empty keys,
list order, scores and absolute key/hash-field TTLs using bounded cursor pages and
commands. It never silently skips unsupported types or oversized values. It uses
on-disk deduplication, not an unbounded key set. Optional source/target username,
password-file, TLS CA and server-name flags avoid secrets in arguments/URLs.
This is not a live atomic migration: failure exits 2 with copied counts and an
explicit untouched/partial-target status. Lost write replies have unknown outcomes.
Preserve the source and use a fresh target after a partial copy; no destructive
replace mode exists.

### Development snapshot replication

A durable master may opt in with `--replication-export` on its authenticated
management listener. Start it with the Step 7 flags plus that option. A separate
read-only follower uses verified HTTPS and an admin token file:

```sh
python3 deploy/bootstrap.py "$PWD/.ffkv-follower"
target/release/ffkv --bind 127.0.0.1:6382   --data-dir .ffkv-follower/data/database --durability strict --storage hybrid   --acl-file .ffkv-follower/users.acl   --tls-cert .ffkv-follower/tls.crt --tls-key .ffkv-follower/tls.key   --replica-source https://localhost:6381   --replica-token-file .ffkv-dev/console-admin-token --replica-ca-file .ffkv-dev/ca.crt
```

This is real authenticated, bounded **full-snapshot polling**, not Redis PSYNC,
incremental streaming, election or automatic failover. Source instance/revision
identities skip unchanged data and survive checkpoint sequence resets. Each image
is checksum/manifest/WAL-validated before an atomic follower cutover. Source
export is admin-only and audited; temporary exports are cleaned up on completion
or disconnect, not confused with operator backups.

The follower returns LOADING on reads until its first successful synchronization
on **each process start**, including when a local copy exists. It rejects dataset
writes, including management, EXEC and Lua, with READONLY. After synchronization
an outage may serve explicitly stale reads; status reports disconnected and lag
unknown. Lag is observed committed-revision distance, not milliseconds. Absolute
TTLs/leases require aligned clocks. ACLs, audit/client state, native modules and
script caches are local; named function libraries follow the source image.
Image installation retains local runtime configuration/module registry ownership
and replaces the full database/global image in online-backup deltas. Source/follower
disk headroom must cover snapshot scratch
and installation; the default transfer bound is 1 GiB. After a hard crash, preserve
or remove only owned `.ffkv-replica-*` staging directories while the follower is
stopped; interrupted staging is never a trusted recovery source.

For manual promotion, **fence the old master first**, stop the follower, restart
on the same data directory without all replica-source/token/CA flags, and verify
application/queue state before client cutover. Snapshots can lag acknowledged
master writes; this provides no zero-loss failover or exactly-once worker guarantee.
The exercised replication smoke covers HTTP loopback testing and verified TLS,
read-only enforcement, source outage/reboot, snapshot identity, export cleanup,
lease/idempotency preservation and fenced promotion.

### Reproducible measurements and fault/soak coverage

```sh
cargo build --release
python3 benchmarks/compare.py --binary target/release/ffkv --items 4096 --hot-operations 512 --json benchmark.json --csv benchmark.csv
python3 tests/soak_smoke.py --binary target/release/ffkv --duration 60 --json soak.json
```

The harness owns disposable ffkv/Redis processes, aligns memory/strict/everysec
policies and uses 256 B, 4 KiB and 64 KiB bodies, fixed concurrency/pipeline, cold
first reads, hot repeats, draining, checkpoint/compaction, and actual current RSS.
JSON includes settings, versions, commands, errors, retries, unknown outcomes and
measured latency histogram bounds. Cold means the first application read, **not**
OS-cache eviction. `--items`, `--duration` and external endpoints support larger or
longer workloads; never point the harness at a production dataset. External
Valkey/Dragonfly persistence must be verified separately and is labelled unverified.

Published evidence: `benchmarks/results/macos-arm64.json`. Shared development
workstation: macOS 27.0.1/arm64, 10 logical CPUs, Python 3.12.10, ffkv 0.1.0 release,
Redis 8.2.1; 4,096 queued bodies, four clients, pipeline eight, hybrid cache 1 MiB,
inline threshold 1 KiB. All 24 cases completed without reported command errors.
Selected **strict durability** results (single run, not a general speed claim):

| Engine/storage | Body | Backlog RSS MiB | Enqueue ops/s | Hot reads/s | Hot P99 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| ffkv memory | 256 B | 9.86 | 233 | 44,596 | 0.830 |
| ffkv hybrid | 256 B | 11.11 | 501 | 44,444 | 0.778 |
| Redis | 256 B | 8.59 | 2,448 | 120,547 | 0.585 |
| ffkv memory | 4 KiB | 34.31 | 290 | 46,519 | 0.787 |
| ffkv hybrid | 4 KiB | 12.36 | 343 | 42,846 | 0.851 |
| Redis | 4 KiB | 28.22 | 2,519 | 61,954 | 0.855 |
| ffkv memory | 64 KiB | 318.48 | 365 | 23,417 | 1.769 |
| ffkv hybrid | 64 KiB | 26.59 | 335 | 23,526 | 1.753 |
| Redis | 64 KiB | 268.62 | 715 | 25,253 | 1.737 |

This shows the memory/throughput tradeoff, including worse small-body overhead and
strict-write throughput below Redis. It does not establish the aspirational 80%
throughput target across workloads. Valkey/Dragonfly, million-job and beyond-RAM
runs were not performed for this baseline. No performance-per-dollar claim is made.
The finite concurrent soak exercised 15 active seconds in **each** storage mode,
four SIGKILL/recovery cycles per mode, concurrent checkpoint/compaction, stale
receipts, idempotency, and backup restoration into a real new process. It is not a
seven-day soak, simulated device power loss or production certification.
Machine-readable fault/soak evidence is in `benchmarks/results/soak-macos-arm64.json`.

Steps 1–8 and the Redis 8.10 command-catalog expansion are usable alpha workflows.
Native replication, sharded Pub/Sub and ffkv cluster workflows are exercised.
Complete Redis binary compatibility, production HA, managed hosting and the
aspirational long-duration/performance targets are not claimed by this release.
