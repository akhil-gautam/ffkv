//! Browser playground: ffkv's command engine as a WASI reactor module.
//!
//! The server modules below are the same source files the `ffkv` binary
//! compiles; only the session layer in `src/wasm/` is playground-specific.
//! Exports (all pointers and lengths are offsets/sizes in linear memory):
//!
//! - `ffkv_alloc(len) -> ptr`, `ffkv_free(ptr, len)`
//! - `ffkv_open() -> id`, `ffkv_close(id)`
//! - `ffkv_eval(id, ptr, len) -> (out_ptr << 32) | out_len`
//! - `ffkv_poll(id) -> (out_ptr << 32) | out_len`
//! - `ffkv_reset()`
//!
//! Results are UTF-8 JSON objects `{"output", "blocked", "prompt", "error"}`
//! that the caller releases with `ffkv_free`.
// Only part of each shared server module is reachable from the playground.
#![allow(dead_code)]

mod auth;
mod client_registry;
// Re-exports for server-only callers (search migration, rollback) go unused.
#[allow(unused_imports)]
mod engine;
mod journal;
mod protocol;
mod pubsub;
mod queue;
mod rdb;
mod server_commands;
mod session;
mod slot;
mod storage;
mod watch;

// `#[path]` modules resolve the same for rustc and rustfmt: relative to the
// declaring file's directory, with the target file's own `mod` children
// resolved as mod-rs (beside it).
#[path = "wasm/cli.rs"]
mod cli;
#[path = "wasm/client.rs"]
mod client;
#[path = "wasm/connection.rs"]
mod connection;
#[path = "wasm/mod.rs"]
mod wasm;
