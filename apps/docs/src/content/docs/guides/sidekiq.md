---
title: Sidekiq
description: Configure unmodified Sidekiq against ffkv and understand the tested stack and fetcher guarantees.
---

ffkv runs the repository's unmodified Sidekiq OSS integration over RESP, including scheduler/retry Lua, metrics middleware, heartbeat/process metadata, stats, and session-backed Web pages. This is evidence for a pinned client stack, not blanket certification of all Sidekiq releases, commercial fetchers, or plugins.

## Pinned repository stack

| Component | Version |
| --- | --- |
| Ruby | 3.4.6 |
| Sidekiq OSS | 8.0.7 |
| redis-client | 0.26.2 |
| connection_pool | 2.5.4 |
| Rack / rack-session | 3.2.6 / 2.1.2 |
| Puma | 8.0.0 |

The fixture pins connection_pool because 3.0.2 changes the TimedStack API used by this Sidekiq release. It does not patch installed gem source. The README separately records an application integration on newer Ruby/Rails/Sidekiq with cron/uniqueness plugins; that does not change the self-contained fixture's pinned versions.

## Point both client and worker at ffkv

The source pattern in `tests/sidekiq/jobs.rb` configures both paths with the same options:

```ruby
require "sidekiq"

redis_options = {
  url: ENV.fetch("FFKV_SIDEKIQ_URL"),
  network_timeout: 2,
  pool_timeout: 3
}
Sidekiq.configure_client { |config| config.redis = redis_options }
Sidekiq.configure_server { |config| config.redis = redis_options }
```

For a trusted plaintext loopback development server only:

```sh
export FFKV_SIDEKIQ_URL=redis://127.0.0.1:6382/0
```

A redis URI selects the protocol endpoint; it does not start Redis. For remote deployment, configure the application's redis-client TLS/ACL options using [security](/operations/security/), and obtain secrets through your secret manager/private files rather than embedding them in shell history or URLs.

## Run the actual fixture manually

Use a fresh endpoint because the seed command requires an empty database. Stop only your own previous documentation server if it owns 6382, then start:

```sh
target/release/ffkv --bind 127.0.0.1:6382 --data-dir .ffkv-docs-sidekiq --durability strict
```

In a second terminal at the repository root, using the pinned Ruby/Bundler environment:

```sh
export BUNDLE_GEMFILE="$PWD/tests/sidekiq/Gemfile"
export FFKV_SIDEKIQ_URL=redis://127.0.0.1:6382/0
export FFKV_SIDEKIQ_PREFIX=docs:sidekiq
export FFKV_SIDEKIQ_QUEUE=docs_sidekiq
bundle install
bundle exec ruby tests/sidekiq/client.rb seed
bundle exec sidekiq -r ./tests/sidekiq/jobs.rb -c 2 -q docs_sidekiq -t 5
```

The seed enqueues immediate, scheduled, intentional first-attempt retry, held-queue, and far-future jobs. The worker processes only `docs_sidekiq`; the held queue intentionally remains unconsumed. Inspect from another terminal after exporting the same variables:

```sh
bundle exec ruby tests/sidekiq/client.rb snapshot
target/release/ffkv-cli -p 6382 LRANGE queue:docs_sidekiq 0 -1
target/release/ffkv-cli -p 6382 ZCARD schedule
target/release/ffkv-cli -p 6382 ZCARD retry
```

The fixture handler writes observable results into a hash; it does not perform real business work. Stop the worker gracefully before the server. The automated isolated integration is documented under [testing](/contributing/testing/); it starts its own server and does not require your manual endpoint.

## Delivery guarantees and operations

The normal OSS **BRPOP fetcher removes a job on delivery**. A worker crash after fetch can lose the job before Sidekiq retries are registered. WAL persistence does not invent a native lease or crash-after-fetch redelivery. Native [FQ queues](/guides/native-jobs/) are a different model and are not a drop-in replacement for Sidekiq internals.

Scheduler/retry scripts have ffkv's [bounded Lua](/guides/lua-functions/) behavior, including a volatile script cache; clients must handle NOSCRIPT. Queue lists and schedule/retry sorted sets remain ordinary datastore keys. The management console inspects those structures but does not grant native-queue remediation or fake native receipts for Sidekiq jobs.

Plan capacity for metadata, heartbeat expiry, queues, and retained retries. Hybrid can offload large list bodies, not every sorted-set payload. See [list queues](/guides/list-queues/), [persistence](/operations/persistence/), [compatibility](/concepts/compatibility/), and [console](/getting-started/console/).
