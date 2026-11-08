---
title: Docker and Compose
description: Run the published multi-arch ffkv image, or the hardened Compose workflow with the management console.
---

Two container options run one single-node ffkv server; neither is a cluster deployment:

- **Published image** (`ghcr.io/akhil-gautam/ffkv`): one `docker run`, with TLS and ACL credentials generated on first start.
- **Compose workflow** (`deploy/compose.yaml`): adds the React management console, hybrid storage and externally managed secrets.

Both are separate from the [static documentation-site deployment](/deployment/documentation/).

## Published image

```sh
docker run -d --name ffkv -p 6379:6379 -v ffkv-data:/var/lib/ffkv ghcr.io/akhil-gautam/ffkv:latest
docker exec ffkv ffkv-cli PING          # PONG
docker exec ffkv ffkv-cli SET greeting hello
```

Images are built for `linux/amd64` and `linux/arm64`. They are tagged `latest` from `main`, `X.Y.Z` and `X.Y` from release tags, and `sha-<commit>` for every published build.

ffkv never serves RESP on a non-loopback address without TLS and an ACL. On first start, the entrypoint therefore creates the following in the volume under `/var/lib/ffkv/docker/`:

| File | Contents |
| --- | --- |
| `admin-password` | Random password for user `admin`, with all commands, keys and channels |
| `users.acl` | `default` disabled, `admin`, and a `health` user that may only `PING` |
| `ca.crt`, `tls.crt`, `tls.key` | Self-signed certificate for `localhost`, `ffkv`, `127.0.0.1` and `::1`, valid for 825 days |

These files persist with the volume, so a new container on the same volume keeps the same password and certificate. The certificate is regenerated when its names change or fewer than 30 days remain. Passwords are never logged. Read the admin password with `docker exec ffkv cat /var/lib/ffkv/docker/admin-password`.

Inside the container, `ffkv-cli` with no connection options uses these generated credentials automatically. To connect from the host or another machine, use any TLS-capable Redis client with the generated CA:

```sh
docker cp ffkv:/var/lib/ffkv/docker/ca.crt ./ffkv-ca.crt
REDISCLI_AUTH="$(docker exec ffkv cat /var/lib/ffkv/docker/admin-password)" \
  redis-cli -p 6379 --tls --cacert ./ffkv-ca.crt --user admin PING
```

Plaintext connections are refused.

| Setting | Purpose |
| --- | --- |
| `FFKV_PASSWORD` / `FFKV_PASSWORD_FILE` | Set the `admin` password instead of generating one; it is applied on every start |
| `FFKV_TLS_SANS` | Extra comma-separated DNS names or IPs for the certificate, e.g. `db.example.com,10.0.0.5`, when clients connect by a name other than `localhost` |
| `FFKV_DURABILITY` | `strict` (default) or `everysec` |
| `FFKV_PORT` | Listener port inside the container, default `6379` |
| Extra arguments | Appended to the server command, e.g. `docker run … ghcr.io/akhil-gautam/ffkv --storage hybrid --shards 16` |

To use your own PKI and ACL, pass `--bind`, `--acl-file`, `--tls-cert` and `--tls-key` yourself with the files mounted. Any of these flags turns off generation, and the arguments go to `ffkv` unchanged. A first argument that is not a flag runs that program instead, e.g. `ffkv-cli` or `sh`. The image runs as UID/GID 10001. A host bind mount for `/var/lib/ffkv` must be writable by that user; named volumes already are. The image also contains `ffkv-mcp` and the console assets in `/opt/ffkv/console`.

The self-signed certificate suits development and private networks. For production, provide certificates from your PKI and manage credentials with your secret store.

To build the image yourself from the repository root, run `docker build -t ffkv:local .`. The build cross-compiles, so `docker buildx build --platform linux/amd64,linux/arm64 .` does not need emulation.

## Compose workflow

### Prerequisites

Use a nonroot host account, Python 3, OpenSSL, and Docker with Compose support. Choose a **new** configuration directory. Bootstrap refuses any existing destination and stores credentials in private files, never output or command arguments. Production deployments need your PKI and secret-management policy rather than its 30-day development certificate.

### Start

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

### Probe and connect

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

Supply the private admin password at the hidden prompt. Do not embed it in a URL or process arguments. The Compose command passes its own `--bind`, `--acl-file` and TLS flags, so the image's entrypoint runs `ffkv` with exactly those arguments and generates nothing.

### Settings and resource boundaries

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

### Lifecycle and backups

```sh
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml ps
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml logs ffkv
docker compose --env-file .ffkv-compose/compose.env -f deploy/compose.yaml down
```

`down` stops/removes containers, **not** bind-mounted data. Preserve `.ffkv-compose`, backups and the raw credentials separately. Do not delete that directory as routine cleanup. Use [coherent backups and NEW-directory restore](/operations/backup-restore/), not a changing live-WAL copy or image rebuild, as your recovery plan.

For remote deployment, replace certificates with real host SANs, control network exposure, set an explicit management origin behind a proxy and plan credential rotation/restarts. A restart is single-node downtime, not HA. See [security](/operations/security/) and [Kubernetes](/deployment/kubernetes/).
