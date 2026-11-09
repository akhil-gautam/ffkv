---
title: Development guide
description: Build isolated native instances, find the owning source module and preserve ffkv's durability and compatibility contracts.
---

The repository contains a Rust/Tokio server, native Rust CLI, React management console, this static documentation site, Python behavior/fault harnesses, and deployment assets. Work on fresh owned ports/directories; an existing local datastore or application dataset is not a test fixture.

## Prerequisites and build

The supported development baseline is macOS/Linux with Rust **1.99 or newer**. Use Python 3 for harnesses, a C compiler for the native call shim/module fixtures, and OpenSSL for TLS fixtures. Lua 5.1 is vendored through `mlua`; no external Redis installation is required for the server or native client. Node 22/npm is needed for the React console. Ruby/Bundler is needed only for the pinned Sidekiq integration.

From the repository root:

```sh
cargo build --release --locked --bins
target/release/ffkv --bind 127.0.0.1:6382 \
  --data-dir .ffkv-docs-contributor --durability strict
```

Use another terminal:

```sh
target/release/ffkv-cli -p 6382 PING
target/release/ffkv-cli -3 -p 6382 COMMAND INFO get set
target/release/ffkv-cli -p 6382 SET docs:development hello
```

Stop only this server with Ctrl-C. Keep the directory if you need restart survival; omit data-dir only when intentionally testing volatile behavior. Never reuse the protected console bootstrap directory with a different unprotected command.

For quicker iterative compilation, `cargo build --bins` produces debug binaries. `Cargo.toml` uses edition 2024, defaults `cargo run` to ffkv, and configures thin LTO/one codegen unit for release. Root `build.rs` compiles `src/server_commands/extensions/call.c`; packaging must include it. The current Dockerfile omission of that build script is documented in [Docker deployment](/deployment/docker/), not a native build requirement workaround.

## Source ownership map

| Area | Start here |
| --- | --- |
| Startup/options and socket handling | `src/main.rs`, `src/options.rs`, `src/connection.rs`, `src/request.rs`, `src/session.rs`, `src/state.rs` |
| RESP request/response encoding | `src/protocol.rs` |
| Native CLI, input and wire decode | `src/bin/ffkv-cli.rs`, `src/cli/`, `src/client/` |
| Command registry, dispatch and metadata | `src/engine.rs`, `src/engine/catalog/`, `src/engine/registry.rs`, `src/command_catalog.json`, `src/command_metadata.json` |
| Type/keyspace implementation | `src/engine/` datatype modules; `src/engine/json/`, `vectors/`, `search/` for deeper implementations |
| Actor ownership and blocking waits | `src/sharding.rs`, `src/blocking.rs`, `src/write_gate.rs` |
| Atomic execution and WATCH | `src/executor.rs`, `src/watch.rs` |
| Resolved effects and durability | `src/journal.rs`, `src/wal.rs`, `src/persistence.rs` |
| Snapshot/checkpoint/backup | `src/engine/snapshot.rs`, `src/backup.rs`, `src/admin.rs`, `src/server_runtime.rs` |
| Hybrid bodies and native jobs | `src/storage.rs`, `src/queue.rs`, `src/engine/native.rs` |
| Lua/functions/debug cancellation | `src/scripting.rs`, `src/functions.rs`, `src/scripting_control.rs` |
| ACLs/TLS/pressure | `src/auth.rs`, `src/auth/namespace.rs`, `src/transport.rs`, `src/pressure.rs` |
| Pub/Sub and client controls | `src/pubsub.rs`, `src/client_registry.rs`, `src/server_commands.rs` |
| Native replication and integration | `src/native_replication.rs`, `src/native_runtime.rs`, `src/rdb.rs` |
| Custom cluster bus/topology/transfer | `src/cluster.rs`, `src/cluster/{bus,commands,topology,migration,snapshot,source,hooks}.rs` |
| Development HTTP snapshot polling | `src/replication.rs`, `src/management/replication.rs`—not native PSYNC |
| Diagnostics/config/module hosting | `src/server_runtime.rs`, `src/server_commands/telemetry.rs`, `src/server_commands/extensions/` |
| Management/auth/audit/DTO/transport | `src/management/`, `src/metrics.rs` |
| Browser console | `apps/console/` |
| Executable behavior and positive evidence | `tests/bdd/`, `tests/*_smoke.py`, `tests/sidekiq/` |
| Deploy/migrate/measure | `deploy/`, `scripts/migrate.py`, `benchmarks/` |

This is an ownership map, not a request to add another dispatch convention. Read the neighboring implementation/tests before changing a type or command.

## Contracts to preserve

- **Binary safety:** keys/values are bytes, not implicitly UTF-8. Cover empty/binary data and RESP2/RESP3 response shapes.
- **Atomic admission:** each actor owns mutable state. Coordinated operations fence actors and move affected entries without copying whole collection payloads. Preserve command-level atomicity and waiter registration/wakeup ordering.
- **Resolved durability:** journal actual chosen effects, generated IDs/random choices and absolute deadlines. A failed WAL commit must not leak writes or publications. Large atomic groups must recover all-or-nothing.
- **Failure semantics:** Lua/EXEC runtime errors retain earlier successful effects; do not invent rollback or swallow unsupported options with success-shaped responses.
- **Authorization:** command metadata, key extraction, selectors, transaction/Lua callbacks and management role checks must remain aligned. A browser control is not the security boundary.
- **Bounded work:** account for ingress/reply allocation, traversal, Lua, caches and retained waiter/publication data. A logical budget is not process RSS.
- **Distinct mechanisms:** do not conflate list remove-on-pop with native leases, internal shards with cluster nodes, native RESP replication with HTTP snapshot polling, or authentication with encrypted cluster transport.

## Console and docs work

The console's actual package scripts are `dev`, `build` and `preview`; build uses TypeScript project compilation then Vite. For an end-to-end protected served console, follow [the walkthrough](/getting-started/console/) rather than assuming an unconfigured Vite page can authenticate cross-origin to any server. The static docs have a separate [deployment workflow](/deployment/documentation/).

## Versions and releases

ffkv follows [Semantic Versioning 2.0.0](https://semver.org/). A version is `MAJOR.MINOR.PATCH`, with an optional pre-release suffix such as `0.2.0-rc.1`. The public API that versions describe is what users depend on: RESP command behavior and reply shapes, `ffkv`/`ffkv-cli` flags and configuration, the on-disk WAL, checkpoint and backup formats, the management HTTP API, metrics names, and the container image's environment variables and data layout.

- **Patch** (`0.1.0` → `0.1.1`): bug fixes that keep that API and read existing data.
- **Minor** (`0.1.x` → `0.2.0`): new behavior. While the major version is 0, a minor release may also break compatibility; every break is listed in the changelog.
- **Major** (from `1.0.0`): any incompatible change, such as an on-disk format a previous release can't read.

`Cargo.toml` holds the version. `scripts/version.py` keeps the copies that other tools need (`Cargo.lock`, the docs and console `package.json` files, and the Helm chart) in step, and the handbook reads it from `Cargo.toml` when it builds. `ffkv --version` reports the same value.

To cut a release:

1. Move the entries under **Unreleased** in `CHANGELOG.md` into a new `## [X.Y.Z] - YYYY-MM-DD` section and update the compare links at the bottom.
2. Set the version everywhere and confirm it:

   ```sh
   python3 scripts/version.py set X.Y.Z
   python3 scripts/version.py check
   ```

3. Commit both changes as `Release X.Y.Z`, then tag and push:

   ```sh
   git tag -a vX.Y.Z -m "ffkv X.Y.Z"
   git push origin main vX.Y.Z
   ```

The **Release** workflow refuses a tag that doesn't match every version field or has no changelog section. It runs the Rust tests, builds and smoke-tests the image, publishes `ghcr.io/akhil-gautam/ffkv:X.Y.Z` and `:X.Y` for amd64 and arm64, and creates the GitHub release from the changelog. Pre-release versions are marked as pre-releases. Pushes to `main` keep updating the `latest` image tag.

## Before proposing a change

Add focused regression coverage at the owning module and real protocol scenarios for user-visible behavior. Update command metadata/catalog evidence when a command contract changes, plus the relevant docs. Use [testing](/contributing/testing/) for the complete BDD gate versus scoped feedback, and [benchmarks](/contributing/benchmarks/) for measurement methodology. Report only checks actually run on your checkout, not historical counts. No license grant should be inferred from a repository without a LICENSE file.
