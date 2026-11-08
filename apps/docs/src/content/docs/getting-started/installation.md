---
title: Installation
description: Build the native ffkv server, CLI, and MCP sidecar from a checkout on macOS or Linux.
---

ffkv includes its own server and protocol client. You do **not** need Redis installed. An optional `redis-cli` is a client that can connect to ffkv; it does not supply or replace the ffkv server.

## Build prerequisites

Use macOS or Linux with Rust **1.99 or newer**, Cargo, and a working native C compiler/linker. The build compiles a small C bridge and vendored Lua 5.1; a separately installed Lua interpreter is unnecessary. On macOS, install Apple's Command Line Tools (`xcode-select --install`). On Linux, install your distribution's C build-toolchain package and Rust through your normal toolchain manager.

Python 3, OpenSSL, Ruby/Bundler, and Node are not required for the basic server/client build. They are needed for specific test fixtures, development credentials, Sidekiq integration, or the [management console](/getting-started/console/). This documentation site has its own frontend dependencies.

From the repository root:

```sh
cargo build --release --bins
```

This creates `target/release/ffkv`, `target/release/ffkv-cli`, and `target/release/ffkv-mcp`. All documentation examples use those paths, so no installation or PATH changes are necessary. To inspect supported options without starting a server:

```sh
target/release/ffkv --help
target/release/ffkv-cli --help
target/release/ffkv-mcp --help
```

Optionally, install the native client onto your PATH from the checkout:

```sh
cargo install --path . --locked --bin ffkv-cli --target-dir target
```

Add Cargo's `bin` directory to your PATH if it is not already present.

The repository's package is not published to crates.io. Build from the checkout you intend to run; restarting an old process is necessary to load a new executable.

## Choose a storage mode

For disposable experimentation, bind an isolated loopback listener:

```sh
target/release/ffkv --bind 127.0.0.1:6382
```

Without `--data-dir`, stopping the process loses the dataset. For restart survival, start a **new** instance with its own previously unused directory:

```sh
target/release/ffkv --bind 127.0.0.1:6382 --data-dir .ffkv-docs-data --durability strict
```

Run one of these commands, not both simultaneously. If 6382 is occupied, choose another unused port consistently; do not stop an unrelated service. Never share a data directory between running processes. Adding `--data-dir` does not import data from an existing memory-only process.

Strict durability synchronizes mutation batches before their replies. `--durability everysec` trades weaker crash durability for lower fsync frequency. [Persistence](/operations/persistence/) explains acknowledgements, recovery, and disk failure behavior. [Hybrid storage](/operations/hybrid-storage/) requires persistence and offloads only large list elements and native-job bodies, not every datatype.

## First connection and next steps

In a second terminal, still at the repository root:

```sh
target/release/ffkv-cli -p 6382 PING
```

The response is `PONG`. The server's default port is 6379 and the client's default is 6380; explicitly specifying 6382 avoids that mismatch and keeps these examples away from existing development services.

Continue with the [quickstart](/getting-started/quickstart/), [CLI guide](/getting-started/cli/), [MCP integration](/guides/mcp/), or [Docker deployment](/deployment/docker/). Loopback plaintext is for trusted local development. Read [security](/operations/security/) before exposing either listener to another machine.
