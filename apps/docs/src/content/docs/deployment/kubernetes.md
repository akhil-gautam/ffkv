---
title: Kubernetes and Helm
description: Deploy the existing single-node StatefulSet chart with an external Secret, persistent storage and verified TLS probes.
---

`deploy/helm/ffkv` installs **one StatefulSet replica**, a persistent volume claim, services, verified TLS AUTH probes, restricted security contexts and a NetworkPolicy. It does not provision a multi-node ffkv cluster or provide production HA. A rolling restart is downtime for this single node.

:::caution[Image prerequisite]
The current Dockerfile omits root `build.rs` from its Rust build stage, although that script builds the native module-call shim. The existing image-build commands below require that source-packaging issue to be addressed separately. They are **not recorded successful Docker/Kubernetes verification**. Use a correctly packaged image available to your cluster; [native installation](/getting-started/installation/) avoids this Dockerfile omission. The static [documentation deployment](/deployment/documentation/) does not depend on the server image.
:::

## Prerequisites and Secret

You need Helm 3, kubectl, a cluster with persistent storage, an accessible server image, and six externally provisioned Secret members:

- `users.acl`
- `management-tokens.json`
- `health-password`
- `tls.crt`
- `tls.key`
- `ca.crt`

The chart deliberately generates **no credentials**. The following is the repository's local-cluster workflow for a runtime that shares Docker images, such as a suitably configured local cluster. Run bootstrap as a nonroot user and choose a new `.ffkv-compose` directory (or reuse only the deliberately created development credentials from the [Compose guide](/deployment/docker/), without rerunning bootstrap):

```sh
python3 deploy/bootstrap.py "$PWD/.ffkv-compose"
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
helm upgrade --install ffkv deploy/helm/ffkv --namespace ffkv \
  --set existingSecret=ffkv-config
```

These commands create a namespace/Secret and install real resources; do not run them against an unrelated cluster. Inspect your context first as part of your own deployment procedure. Existing namespace/Secret creation is not automatically idempotent. For another runtime, publish/load a correctly built image and set `image.repository`/`image.tag` (and `imagePullSecrets` where needed).

## TLS and local access

The development certificate covers localhost (and the short `ffkv` name), **not arbitrary Kubernetes service DNS names**. Test through a local forward:

```sh
kubectl --namespace ffkv port-forward service/ffkv-ffkv 6383:6379 8083:8080
```

Open `https://localhost:8083` using the development CA and raw viewer/admin token file. RESP is forwarded to 6383 in this exact chart example; use the native client with matching TLS/CA/user options and the hidden password prompt, not a bearer token.

For in-cluster clients, replace certificates with your PKI and include actual service/client DNS SANs. **localhost must remain in the SANs for in-pod probes.** The chart's health username defaults to `health`, matching bootstrap's PING-only ACL user and `health-password` file. A probe validates TLS, AUTH and PING, not your entire application dataset or backup freshness.

## Configuration values that differ from native defaults

Consult `deploy/helm/ffkv/values.yaml` and templates together; chart values are not server CLI defaults.

| Values | Shipped setting |
| --- | --- |
| `image.repository`, `image.tag`, `image.pullPolicy` | `ffkv`, `0.1.0`, `IfNotPresent` |
| `existingSecret` | Required external Secret name |
| `durability`, `storage`, `shards`, `maxConnections` | `strict`, `hybrid`, `4`, `1024` |
| `inlineThreshold`, `cacheBytes`, `segmentBytes` | `4096`, `67108864`, `67108864` bytes |
| `diskBudget`, `maxRssBytes` | `8589934592`, `0` bytes |
| `persistence.size` | `20Gi`; set storageClass or existingClaim for your environment |
| `resources.requests` | CPU `250m`, memory `256Mi` |
| `resources.limits` | CPU `2`, memory `1Gi` |
| `service.respPort`, `service.managementPort` | `6379`, `8080` |
| `terminationGracePeriodSeconds` | `60` |
| `networkPolicy.enabled` | `true` |

The chart passes only its defined server arguments; it is not a generic flag/environment passthrough. Increasing StatefulSet replicas manually does not create an ffkv cluster. Plan resource/storage changes from measured capacity, including WAL/checkpoint, hybrid rebuild and backup headroom. The 8 GiB derived-segment budget does not cap total PVC use.

## Security and networking

The pod runs as UID/GID 10001, with RuntimeDefault seccomp, no service-account token mount, read-only rootfs, no privilege escalation and dropped capabilities. An unprivileged init container copies projected Secret files into private memory-backed runtime files compatible with the server's file checks; do not replace that with permissive credentials. Runtime tmpfs is bounded separately.

The default NetworkPolicy permits RESP/management ingress from matching `ffkv-client: "true"` pods in the same namespace. Set `clientNamespaceSelector` for intended cross-namespace access. It denies **all egress**. OIDC/JWKS access, external replica sources, DNS or additional cluster traffic need an appropriate explicit policy; toggling a server flag does not change networking. There is no public Ingress/LoadBalancer supplied by this chart.

## Operations and upgrade safety

Inspect the StatefulSet/pod status and startup/readiness/liveness probe events when a node does not become ready. Missing Secret keys, incorrect private-file ownership, invalid SANs, unavailable images and unwritable/undersized storage are configuration failures, not reasons to disable TLS checks.

Back up the dataset using [ffkv's coherent backup workflow](/operations/backup-restore/) before chart/image/storage changes. Keep restore material off the sole PVC and rehearse restoration into a new directory/volume. A pod restart, a PVC's existence and a successful liveness probe are not backups. Coordinate workers and preserve the old data before destructive infrastructure changes. Verify the cluster's reclaim policy before removing a release or claim; never assume uninstall is a data-retention strategy.
