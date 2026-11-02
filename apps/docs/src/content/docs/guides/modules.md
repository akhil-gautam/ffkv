---
title: Native modules
description: Load trusted native libraries through ffkv's explicit Redis Modules API v1 subset.
---

Native modules execute inside the ffkv server process. They are **trusted, unsandboxed native code**, not Lua libraries and not isolated plugins. A module can crash the process, block execution, access files, or allocate memory outside dataset estimates. Only load audited libraries built for the host platform.

The supported Redis Modules API v1 surface is an explicit subset. Shared command names and an API version do not make an arbitrary Redis module compatible.

## Inspect lifecycle support

```sh
target/release/ffkv-cli -p 6382 MODULE HELP
target/release/ffkv-cli -3 --json -p 6382 MODULE LIST
```

Lifecycle forms are MODULE LOAD with library path/arguments, LOADEX with CONFIG name/value pairs and ARGS, LIST, and UNLOAD by registered module name. Paths are resolved on the **server filesystem**, not on a remote client's machine. Loaded commands appear in the running COMMAND metadata; they are not additional entries in the fixed Redis catalog.

## Run the repository's real ABI fixture

Use a disposable local instance from the [quickstart](/getting-started/quickstart/) and a native compiler plus Python 3. This compiles the existing public-ABI fixture, loads it, invokes a real native callback, and unloads it. It does not install a third-party module or change Rust source.

```sh
python3 - <<'PY'
from pathlib import Path
import subprocess
import sys

sys.path.insert(0, str(Path("tests/bdd/fixtures").resolve()))
from module_fixture import build_module

out = Path(".ffkv-docs-module")
out.mkdir(exist_ok=False)
library = build_module(out).resolve()

def kv(*args):
    subprocess.run(["target/release/ffkv-cli", "-p", "6382", *args], check=True)

kv("MODULE", "LOAD", str(library))
try:
    kv("MODULE", "LIST")
    kv("fixture.sum", "2", "3")
    kv("fixture.keyset", "docs:module:value", "stored")
    kv("GET", "docs:module:value")
    kv("COMMAND", "INFO", "fixture.sum")
finally:
    kv("MODULE", "UNLOAD", "fixture")
PY
```

The sum is five and the stored key reads `stored`. The build helper chooses `.dylib`/`-dynamiclib` on macOS or `.so`/`-shared` on Linux. The output directory must be new; choose a new directory for another run rather than overwriting a library a process might still have loaded. The fixture is a demonstration of supported APIs, not a production extension.

## Supported boundary

The implementation exports string/reply/allocation APIs, synchronous Call, basic key access, command metadata, and immutable load-time string configuration. Native commands run synchronously on the paused atomic dispatcher; supported Call/key effects go through normal ACL, journal, and replication paths. Readonly metadata is enforced against attempts to call dataset writes.

The following are **not implemented**: module-defined datatypes, persistence callbacks, blocking/thread/timer/event facilities, and shared APIs. A library requesting a missing API fails load rather than receiving a success-shaped stub. Failed/partial registration must not leave live commands. Review `src/server_commands/extensions/abi.rs` for the precise export table when developing a module.

The runtime allows at most **32 loaded modules**. Duplicate names/paths, incompatible libraries, missing OnLoad entrypoints, and invalid configuration fail. Unload can be refused by a module callback or while active invocations make removal unsafe; inspect the error instead of assuming unloading happened.

## Durability and operations

A module's private C globals/heap are not dataset state and have no persistence callback support. Do not assume MODULE LOAD registrations or private counters will reappear after restart. Supported mutations to ordinary ffkv keys follow the configured WAL policy; private native memory does not. The fixture's `fixture.counter`, for example, is process-local, unlike its `fixture.keyset` mutation.

Long native callbacks can stall coordinated execution. There is no sandbox memory budget protecting the server from a module's allocations. Restrict MODULE administration to trusted operators, preserve deployable library artifacts, and evaluate compatibility/failure behavior in an isolated instance before rollout.

See [MODULE LOAD](/reference/commands/module-load/), [MODULE LOADEX](/reference/commands/module-loadex/), [Lua/functions](/guides/lua-functions/), [security](/operations/security/), and [compatibility](/concepts/compatibility/).
