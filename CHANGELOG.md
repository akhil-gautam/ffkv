# Changelog

All notable changes to ffkv are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and ffkv uses
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). While the major
version is 0, a minor release may contain breaking changes; each one is listed
under **Changed** or **Removed**.

## [Unreleased]

## [0.1.0] - 2026-11-09

First tagged alpha release.

### Added

- Redis-compatible server speaking RESP2 and RESP3, covering all 486 entries of
  the Redis 8.10 command catalog across 19 data-type families.
- Durable storage: a checksummed write-ahead log with `strict` (fsync per write)
  and `everysec` modes, checkpoints, online backups and offline restore.
- Hybrid RAM/SSD storage that keeps large values on disk.
- Blocking list queues and native leased jobs with acknowledgements, Lua
  scripting, transactions, Pub/Sub, native replication and cluster mode.
- TLS, ACLs, Prometheus metrics and an authenticated React management console.
- `ffkv-cli` native client and `ffkv-mcp` sidecar for AI tools.
- Multi-arch container image (`linux/amd64`, `linux/arm64`) at
  `ghcr.io/akhil-gautam/ffkv` that generates TLS and an admin password on first
  start, plus Docker Compose and Helm deployment files.

[Unreleased]: https://github.com/akhil-gautam/ffkv/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/akhil-gautam/ffkv/releases/tag/v0.1.0
