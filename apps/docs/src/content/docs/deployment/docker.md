---
title: Docker and Compose
description: Use the repository's hardened single-node Compose workflow, understand its bind-mounted secrets, and account for the current image-packaging caveat.
---

The existing `deploy/compose.yaml` runs one TLS/ACL-protected ffkv server with the React management console. It is not a cluster deployment. The Rust server/console image is separate from the [static documentation-site deployment](/deployment/documentation/).

:::caution[Current image-packaging caveat]
The repository Dockerfile's Rust stage copies `Cargo.toml`, `Cargo.lock` and `src/`, but **omits root `build.rs`**, which compiles the native module-call shim in `src/server_commands/extensions/call.c`. This is a source-packaging discrepancy: the documented build workflow below is the existing workflow, **not a claim that the current image was successfully built or run**. Review/correct that packaging in the server project before relying on a fresh image. A [native source installation](/getting-started/installation/) includes the build script and avoids this container packaging omission. No Docker daemon is needed for the documentation site.
:::

## Prerequisites

Use a nonroot host account, Python 3, OpenSSL, Docker with Compose support, and an image/build environment whose server packaging has been addressed. Choose a **new** configuration directory. Bootstrap refuses any existing destination and stores credentials in private files, never output or command arguments. Production deployments need your PKI and secret-management policy rather than its 30-day development certificate.

## Existing workflow

From the repository root:

```sh
python3 deploy/bootstrap.py "$PWD/.ffkv-compose"
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml up --build -d --wait
```

This publishes verified TLS RESP at **localhost:6379** and management/console at **https://localhost:8080**. These are Compose defaults, not the native walkthrough's 6382/6383. Open the console and use `.ffkv-compose/console-viewer-token` or, for administration, `.ffkv-compose/console-admin-token`. RESP uses `admin-password`, not the bearer token.

To avoid another local service's ports, supply overrides on first startup:

```sh
FFKV_RESP_PORT=6382 FFKV_MANAGEMENT_PORT=6383 \
  docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml up --build -d --wait
```

Do not start both variants concurrently. Keep the same overrides for subsequent lifecycle commands. The exact server command is in `deploy/compose.yaml`; it sets internal listeners 6379/8080, strict durability, hybrid storage, TLS/ACL/token policy, persistent audit journal and packaged console assets.

## Probe and connect

For the default published port:

```sh
python3 deploy/probe.py --host 127.0.0.1 --port 6379 --username health \
  --password-file .ffkv-compose/health-password \
  --tls-ca .ffkv-compose/ca.crt --tls-server-name localhost
```

For the explicit 6382 override, a locally built client can use:

```sh
target/release/ffkv-cli --tls --cacert .ffkv-compose/ca.crt \
  -h localhost -p 6382 --user admin --askpass PING
```

Supply the private admin password at the hidden prompt. Do not embed it in a URL or process arguments. The container image packages both `ffkv` and `ffkv-cli`, with `ffkv` as its default entrypoint; a plain `docker run IMAGE` is not the full Compose secret/management setup.

## Settings and resource boundaries

| Environment variable | Compose default / purpose |
| --- | --- |
| `FFKV_CONFIG_DIR`, `FFKV_UID`, `FFKV_GID` | Required; bootstrap writes absolute config directory and nonroot owner IDs to `compose.env` |
| `FFKV_IMAGE` | `ffkv:local` |
| `FFKV_PUBLISH_ADDRESS` | `127.0.0.1`; keep loopback unless remote access is intentionally secured |
| `FFKV_RESP_PORT`, `FFKV_MANAGEMENT_PORT` | `6379`, `8080` on host |
| `FFKV_STORAGE`, `FFKV_DURABILITY` | `hybrid`, `strict` |
| `FFKV_MEMORY_LIMIT` | `1g`, external container memory limit |
| `FFKV_MAX_RSS_BYTES` | `0`, disables optional sampled-RSS soft admission breaker |

The soft RSS breaker and container hard limit are different. Set measured headroom if enabling the breaker; it does not evict or bound recovery peaks. The current Compose file does not expose every server flag as an environment variable; do not assume arbitrary `FFKV_*` names are read by ffkv.

The service uses the bootstrap owner's nonroot UID/GID, a read-only root filesystem, all capabilities dropped, no-new-privileges, a 256 PID cap, bounded 16 MiB tmpfs and a real TLS AUTH PING healthcheck. The bridge permits egress. Published ports default to loopback.

`FFKV_CONFIG_DIR/data` is a persistent **bind mount** at `/var/lib/ffkv`; the database is its `data` subdirectory and the audit journal is beside it. ACL/token/TLS/health files are read-only bind mounts with host-path auto-creation disabled. Preserve ownership and private permissions; bootstrap's owner mapping matters on native hosts.

## Lifecycle and backups

```sh
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml ps
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml logs ffkv
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml down
```

`down` stops/removes containers, **not** bind-mounted data. Preserve `.ffkv-compose`, backups and the raw credentials separately. Do not delete that directory as routine cleanup. Use [coherent backups and NEW-directory restore](/operations/backup-restore/), not a changing live-WAL copy or image rebuild, as your recovery plan.

For remote deployment, replace certificates with real host SANs, control network exposure, set an explicit management origin behind a proxy and plan credential rotation/restarts. A restart is single-node downtime, not HA. See [security](/operations/security/) and [Kubernetes](/deployment/kubernetes/).
