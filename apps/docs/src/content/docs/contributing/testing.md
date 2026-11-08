---
title: Testing and behavior evidence
description: Run real server/native-client suites, distinguish full catalog completion from scoped runs, and keep owned fault fixtures isolated.
---

ffkv's BDD gate uses real server processes and the native `ffkv-cli`, not an in-memory substitute or an installed Redis instance. The pinned Redis 8.10 catalog has **486 entries including subcommands**. A full pass requires both a passed tagged scenario **and a positive observed canonical invocation** for every entry. Tags alone, an inventory pass or a narrowly scoped green feature do not establish catalog completion.

## Set up the BDD environment

You need Python 3, a C compiler and OpenSSL for owned native-module/TLS fixtures. Build both debug binaries before using the runner's defaults:

```sh
cargo build --bins
python3 -m venv .ffkv-bdd-venv
.ffkv-bdd-venv/bin/python -m pip install -r tests/bdd/requirements.txt
```

The requirements pin `behave==1.3.3`. Run from the repository root. Fixtures own their node processes, OS-assigned ports, fresh data directories, credentials, fault gates and native libraries; they do not use/restart a pre-existing server on 6380.

## Full catalog gate

```sh
.ffkv-bdd-venv/bin/python tests/bdd/run.py --inventory
.ffkv-bdd-venv/bin/python tests/bdd/run.py --evidence-report .ffkv-bdd-evidence.json
.ffkv-bdd-venv/bin/python -m unittest discover -s tests/bdd -p 'test_*.py'
```

`--inventory` audits scenario tag names only and exits nonzero for missing entries. The full runner executes all features, including real native replication and cluster fixtures, and checks:

1. Every catalog entry has a scenario tag.
2. A tagged scenario actually passed, with its steps passed.
3. The native-client evidence classifier observed a **positive** canonical wire invocation in a passed scenario.
4. There are no missing/unexecuted catalog entries or failed behavior runs.

An error-path call alone is not positive evidence. Only catalog containers HIMPORT, JSON.DEBUG and HOTKEYS can share successful child-invocation coverage; COMMAND and other standalone entries require their own invocation. The optional JSON stores at most one `command/status/scenario` record per catalog entry, not argv, secrets or payloads. Classifier unit tests test that evidence machinery, not just datastore behavior.

For optimized binaries:

```sh
cargo build --release --bins
.ffkv-bdd-venv/bin/python tests/bdd/run.py \
  --server target/release/ffkv --client target/release/ffkv-cli \
  --evidence-report .ffkv-bdd-release-evidence.json
```

Do not report historical scenario/test totals as a fresh run result. Record the current commit, binary profile, full invocation, failures/skips and evidence artifact from the actual run.

## Scoped feedback is not full verification

```sh
.ffkv-bdd-venv/bin/python tests/bdd/run.py \
  --feature tests/bdd/features/strings.feature
.ffkv-bdd-venv/bin/python tests/bdd/run.py --tags '@command_GET'
```

`--feature` can be repeated and `--tags` accepts a Behave expression. A scoped run explicitly reports that it is **not full-catalog verification**. Use it while developing, then run the complete gate for a catalog-completion claim. A missing global tag does not necessarily block an explicitly scoped run; that relaxation is not permission to label it complete.

## Data-structure and command regression coverage

The unit tests and live BDD features cover these previously reproduced failures:

| Area | Behavioral regressions | BDD features |
| --- | --- | --- |
| Search | Signed-zero numeric range equality without a server crash; empty TAG posting updates and removal | `search.feature` |
| Scripts and transactions | Declared/dynamic key ownership, including FCALL and migration; invalid script key counts inside EXEC without aborting surrounding writes; legacy versus shebang read-only admission | `cluster.feature`, `scripting.feature`, `transactions.feature`, `transactions_runtime.feature` |
| Streams | Duplicate/conflicting reference policies fail without mutation; ACKED trimming scans past pending entries; FORCE delivery counts distinguish JUSTID and RETRYCOUNT | `streams.feature` |
| GEO and sorted sets | Numeric underflow cannot erase an aliased destination; weighted intersections preserve source-order and infinity/NaN aggregation semantics | `geo.feature`, `sorted_sets.feature` |
| Time series | Deleting the latest compaction bucket reopens the previous bucket; cursor/accumulator state survives recovery | `timeseries.feature` |
| JSON | Read selectors preserve repeated matches and order; mutations apply once per selected node | `json.feature` |
| Vector filters | Null equality differs from numeric zero, false, empty strings, and other non-null values | `vectors.feature` |
| Bloom and Cuckoo filters | Existing-filter INSERT ignores creation allocation budgets; Cuckoo insertion reuses available earlier parts with correct replay and multiplicities | `probabilistic.feature` |

These regressions supplement the catalog gate; they are not proof of exhaustive Redis compatibility.

## Rust and CLI smoke suites

Useful repository checks are:

```sh
cargo test
cargo clippy --all-targets -- -D warnings
```

The existing smoke harnesses use isolated live sockets/processes. Build the intended binary first and consult each script's `--help` when selecting a nondefault binary. The current suite includes:

```sh
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
python3 tests/pubsub_smoke.py
python3 tests/client_smoke.py
python3 tests/datatypes_smoke.py
```

Durability covers orderly/SIGKILL restart, absolute TTLs, completed/pending pops, locks, both fsync policies, torn records/groups, corruption refusal and real WAL write failure. File-size-limit injection can report an explicit skip where unsupported. A timer exercise does not independently observe a storage device's fsync. Native replication BDD and the HTTP snapshot-poller smoke are different coverage; do not collapse their claims.

## MCP integration

Build all binaries before exercising the real stdio sidecar:

```sh
cargo build --release --bins
python3 tests/mcp_smoke.py \
  --binary target/release/ffkv --mcp-binary target/release/ffkv-mcp
```

The eleven groups cover negotiation/catalog discovery, data-type commands, native
jobs, stateful transactions, manual/raw streams, errors/cancellation/ACLs,
administration, authenticated management/live/downloads, lossless large-result
paging, opt-in process control/offline restore, and large-payload benchmark
pipelines. Fixtures own their servers, ports, credentials and temporary
directories; they do not use an existing development database.

`--group artifacts`, for example, runs only that smoke group. The same behavioral
groups are BDD scenarios in `tests/bdd/features/mcp.feature`, included in the full
gate. Scoped execution is feedback, not a substitute for the full gate. These
tests establish representative end-to-end access and failure semantics, not
exhaustive coverage of every command option through MCP.

Review regressions verify that queued-send timeouts preserve an active blocking
command without sending late bytes; active live receives survive idle TTL and
then expire when truly idle; pre-dispatch POST errors remain known-outcome; and
absolute root aliases work without allowing child symlinks or outside paths.
Rust regressions additionally exercise cancelled/dropped lock waiters and
post-dispatch POST cancellation. `--group benchmark` runs the real benchmark
with twelve 1 MiB SET/GET pairs and checks exact replies on both transports.

See the [MCP guide](/guides/mcp/) for protocol and permission boundaries, and
[benchmarks](/contributing/benchmarks/) for the separate performance harness.

## Sidekiq integration

Install the pinned unmodified Ruby dependencies, then run the owned fixture:

```sh
BUNDLE_GEMFILE=tests/sidekiq/Gemfile bundle install
python3 tests/sidekiq_smoke.py --binary target/debug/ffkv
```

The repository fixture pins Sidekiq 8.0.7, redis-client 0.26.2 and connection_pool 2.5.4. It exercises a real worker, scheduler/retry Lua, heartbeat/process/stats behavior and session-backed Web pages. Failure logs are retained under target. Sidekiq's normal BRPOP fetch remains remove-on-delivery, not native lease redelivery. The [application integration](/guides/sidekiq/#application-integration-on-a-newer-stack) on a newer stack is separate evidence, not a self-contained replacement for this fixture.

## Optional differential checks

Only use a **disposable** reference Redis, never an existing application server:

```sh
# Separate terminal: deliberately no disk persistence.
redis-server --bind 127.0.0.1 --port 16380 --save '' --appendonly no
```

Then run the reference-capable smokes:

```sh
python3 tests/cli_smoke.py --reference 127.0.0.1:16380
python3 tests/queue_smoke.py --reference 127.0.0.1:16380
python3 tests/strings_smoke.py --reference 127.0.0.1:16380
```

They write `ffkv:test:*` keys. Seeded comparisons check actual results/error categories/state rather than pinning every error-message string. Earlier differential evidence used Redis 8.2.1; it is not a complete live Redis 8.10 or pinned 7.2 conformance run. BDD catalog coverage does not change that boundary.

## Fault/soak and reporting discipline

`tests/soak_smoke.py` owns disposable processes, crash/restart cycles, checkpoint/compaction, lease/idempotency and backup restore. Its configurable finite duration is not a seven-day soak or simulated device power-loss test; see [benchmarks](/contributing/benchmarks/).

Keep failures observable: record exit code, exact scenario and sanitized logs without credentials/receipts/data. A lost write reply has unknown outcome and must not be papered over by an automatic retry. A skip is a skip, not a pass. Keep build, behavioral, evidence-classifier, differential and performance claims distinct.
