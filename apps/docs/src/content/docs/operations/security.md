---
title: Security and authentication
description: Separate RESP ACLs from management identities, configure TLS and OIDC, and understand reveal and auditing boundaries.
---

ffkv is a tested alpha, not a security/production-HA certification. Keep listeners private, grant least privilege, protect backups and treat native module code as trusted process code. The [protected console walkthrough](/getting-started/console/) creates a safe local TLS/credential setup without embedding secrets in commands.

## Two listeners, two identity systems

| Surface | Authentication | Transport |
| --- | --- | --- |
| RESP | Named ACL users via `AUTH` or `HELLO AUTH` | `--tls-cert` and `--tls-key`; TLS required with ACLs on non-loopback binds |
| Management API, metrics, console session | Bearer tokens from a hashed token-policy file, or configured OIDC JWTs | Separate `--management-tls-cert`/`--management-tls-key`; TLS required on non-loopback management |
| Cluster bus | Shared `--cluster-bus-secret` | ffkv-specific TCP transport; not encrypted by RESP TLS |

A management token is not a RESP password. Management roles do not inherit a user's RESP ACL restrictions. A viewer can inspect allowed management metadata/read surfaces; an admin can reveal bodies and perform confirmed writes. Restrict access to the management plane accordingly. Every management listener requires `--management-token-file`, including OIDC-only configurations.

Plain loopback RESP is only for trusted local development. Non-loopback startup requires **both** TLS and an ACL file; there is no plaintext fallback. TLS 1.2/1.3 is supported. Verify server certificate chains and identity with the client's `--cacert`/`--sni`; never work around a name mismatch by disabling verification. The native client has no insecure verification bypass.

## RESP ACL policy

`deploy/bootstrap.py` writes `users.acl` with the default user disabled, a hashed-password admin, and a PING-only health user. Use `--user admin --askpass` with the private password file; avoid passwords in URLs, `--pass` arguments, shell history and shared environment dumps.

ACL rules support named users, SHA256 password hashes, ordered command/category rules, selectors and binary key patterns. Command grants (`+get`, `+@read`) and key patterns (`~jobs:*`) are independent of channel grants (`&jobs:*`, `allchannels`, `resetchannels`). Pattern subscriptions require an exact granted pattern unless all channels are granted; global PUBSUB inspection requires all-channel permission. The bootstrap admin's `~* +@all` does not itself grant all channels.

Authorization covers AUTH/HELLO transitions, transaction identities and every Lua callback. Failed reauthentication preserves the prior identity, and repeated credential failures eventually close the connection after the final error is flushed. Password-only `AUTH password` requires a default-user password; explicit `AUTH username password` follows that user's `nopass` rule. Use `ACL GETUSER`, `ACL DRYRUN` and bounded `ACL LOG` to inspect policy and denial behavior before assigning it to applications.

ACL files load at startup, while `ACL SETUSER` changes the live policy. Do not assume an in-memory change is durable merely because dataset WAL is enabled: explicitly manage/persist ACL policy using the ACL commands and configured policy file, and verify rotation on a fresh connection. Token-policy files load at startup; restart to rotate them. Preserve private file permissions rather than using a broad chmod to bypass startup checks.

## Management bearer tokens

The bootstrap creates raw `console-admin-token`/`console-viewer-token` files and a `management-tokens.json` policy containing names, roles and SHA256 hashes. Store the raw tokens privately; do not paste the hash from the policy into the login field. Policies allow at most 128 tokens with unique names/hashes. The policy must be a regular file, not a symlink, and must not be group/world writable. The audit journal must be a private regular file; bootstrap creates secrets with mode 0600 in a 0700 directory.

All API/metrics requests need one `Authorization: Bearer ...` header. There is no cookie auth, wildcard CORS or token query parameter. Browser origins are checked against the request Host/HTTP2 authority or the exact `--management-origin` value. A remote reverse proxy needs an explicit externally visible origin and verified TLS on the intended trust boundary.

## OIDC

Supply all three together:

- `--management-oidc-issuer`: exact expected issuer;
- `--management-oidc-audience`: expected audience;
- `--management-oidc-jwks-url`: signature-key endpoint.

The verifier validates real RS256/ES256 signatures using bounded cached JWKS, issuer, audience, subject, expiry/not-before and configured roles. Default role names are `ffkv-admin` and `ffkv-viewer`; override with `--management-oidc-admin-role`/`--management-oidc-viewer-role` when the issuer uses different names. HTTPS is required. `--management-oidc-allow-loopback-http` is only for owned local issuer tests, not a production bypass.

A token-policy file is still mandatory: `{"tokens":[]}` permits OIDC-only access when OIDC is configured. There is no fallback that accepts unverifiable JWTs. Allow network egress to the issuer/JWKS endpoint where your container/NetworkPolicy otherwise blocks it. Live connections revalidate JWT expiry and role before updates.

## Payload visibility and audit outcomes

Both roles start with payloads hidden. Explicit admin reveal is audited. Recognized sensitive JSON fields are masked in both UTF-8 and base64 representations; malformed/deep JSON that cannot be safely sanitized remains hidden. This is not general DLP: arbitrary non-JSON secrets and unrecognized PII can be visible on reveal. `--management-allow-sensitive-reveal` intentionally permits unmasked sensitive JSON; leave it off unless required.

Writes synchronize an audit intent **before dispatch** and record outcomes. Credentials, command arguments, bodies and lease receipts are never audit payloads. Audit failure fails closed, but after dispatch a timeout, outcome-audit failure or delivery-size failure may leave a completed mutation. An `executed/result-too-large`, `executed/result-delivery-failed` or `unknown` outcome must not trigger blind retries. Reconcile actual state first, particularly for pops/claims. Receipts are capabilities: disclose only to the worker/operator that needs them.

## Production checklist

- Replace 30-day development certificates with your PKI; include real service/client SANs.
- Keep RESP, management and cluster-bus exposure separate. The bus is plaintext shared-secret TCP; isolate it with private networking and network controls. A non-loopback bus requires a secret of at least 32 bytes; the CLI currently takes the value directly, so account for process-argument visibility.
- Restrict native `MODULE LOAD/LOADEX` to trusted operators and libraries. Native code is not sandboxed; missing API support causes load failure, not safe emulation.
- Protect plaintext backups, WAL and audit files at rest and in transit. Checksum validation does not authenticate a backup producer.
- Use OS/container memory limits as well as the optional [RSS admission breaker](/operations/memory/).
- Test credential/certificate rotation, restore and fencing procedures without touching production data.
