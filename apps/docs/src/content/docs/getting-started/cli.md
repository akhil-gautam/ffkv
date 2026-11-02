---
title: Native CLI
description: Use ffkv-cli for one-shot commands, persistent sessions, binary input, TLS, and reliable automation.
---

`target/release/ffkv-cli` speaks RESP directly to ffkv. Build it with `cargo build --release --bins`. Examples assume the isolated server from the [quickstart](/getting-started/quickstart/) is listening on 127.0.0.1:6382.

## Commands and persistent sessions

Put client options **before** the command. Everything after COMMAND is passed literally, including negative numbers:

```sh
target/release/ffkv-cli -p 6382 -3 --json HELLO 3
target/release/ffkv-cli -p 6382 LRANGE docs:work 0 -1
target/release/ffkv-cli -p 6382
```

With no command, a terminal opens an editable session; piped stdin supplies one quoted command per line. These commands run on **one connection**, which matters for WATCH/MULTI, SELECT, authentication, and script debugging. Named function libraries are server-wide, not restricted to the loading connection:

```sh
printf 'SET "key with spaces" ""\nGET "key with spaces"\n' | target/release/ffkv-cli -p 6382 --json
```

The client's parser supports single/double quotes, empty arguments, and escapes including `\n` and `\x00`. This is not a shell: it does not execute pipes, substitutions, or external programs. The server's separate inline protocol does not implement this quoting syntax; use the native client/RESP for arbitrary bytes.

History exists only in memory. Credential commands including AUTH and HELLO AUTH are excluded. Ctrl-D exits an interactive session. Ctrl-C closes the connection with status 130, even while editing or waiting for a blocking reply; the interrupted connection is not reused.

## Binary-safe input and output

OS arguments cannot contain NUL. `-x` reads stdin unchanged and appends it as the **last** argument:

```sh
printf 'binary\000payload\n' | target/release/ffkv-cli -p 6382 -x SET docs:blob
target/release/ffkv-cli -p 6382 GET docs:blob
target/release/ffkv-cli -p 6382 --json GET docs:blob
target/release/ffkv-cli -p 6382 --raw GET docs:blob
```

Human output escapes terminal control bytes. Raw output emits actual bytes and adds one newline after each scalar, so it is not a byte-for-byte file export with no delimiter. `-x` retains trailing newlines in the stored value.

JSON output uses ordinary strings for valid UTF-8, numbers/arrays where appropriate, and objects for maps with unique UTF-8 string keys. Invalid UTF-8 becomes `{"type":"bytes","base64":"..."}`. Maps with binary, non-string, or duplicate keys use `{"type":"map","entries":[[key,value],...]}`. Sets/pushes have typed `values`; attributes have `entries` and `value`; errors have `type: "error"` and `value`. Piped sessions emit one JSON value per reply or push, not one combined JSON array.

## Connection settings and credentials

Defaults are 127.0.0.1, port 6380, database zero, RESP2. Specify `-p 6382` for these docs. `-h`, `-p`, `-n`, `--user`, and `-2`/`-3` select host, port, database, user, and protocol. `-u` or `FFKV_URL` accepts `redis://` or `rediss://` with percent-encoded credentials; explicit host/port/database/user flags override URI fields. `REDIS_URL` is deliberately not read implicitly.

Password precedence is `--askpass`, `--pass`, `FFKVCLI_AUTH`, then URI credentials. Prefer the hidden prompt. Avoid password-bearing URLs or `--pass` in shell history/process listings. Environment secrets can still be visible to privileged processes; manage their lifetime accordingly.

For the protected development instance created by the [console guide](/getting-started/console/):

```sh
target/release/ffkv-cli -p 6382 --tls --cacert .ffkv-docs-dev/ca.crt --sni localhost --user admin --askpass PING
```

Enter the password from the private `admin-password` file only at the prompt. TLS validates certificate chains and server identity using bundled WebPKI roots or `--cacert`. `--sni` changes the verified name; mutual TLS uses paired `--cert` and `--key`. There is no insecure verification bypass or plaintext fallback. See [security](/operations/security/).

`--connect-timeout` defaults to five seconds and bounds connection/negotiation setup, not command execution. Blocking pops and subscriptions can wait indefinitely. Direct SUBSCRIBE/PSUBSCRIBE prints acknowledgements/messages until Ctrl-C; interactive sessions can unsubscribe while receiving messages.

## Errors, limits, and retry policy

| Status | Meaning |
| --- | --- |
| 0 | Success |
| 1 | Server command error or input syntax error; nested EXEC errors count too |
| 2 | Configuration, transport, protocol, or input-I/O failure |
| 130 | Interrupted |

Requests are bounded to 8 MiB including framing and 1,024 arguments. Replies have 64 MiB wire/logical allocation budgets and bounded nesting/value counts; these are not process-RSS limits. RESP3 streamed/unknown-length frames are unsupported.

**A lost connection does not prove failure of a write.** The server may have committed before the reply was lost. The client never reconnects automatically or replays writes. Do not blindly retry INCR, pops, lease claims, or external-side-effect workflows. Use application idempotency and state inspection. [Transactions](/guides/transactions/) do not solve lost-reply ambiguity.
