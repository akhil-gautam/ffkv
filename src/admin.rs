//! Administrative operations run behind the same admission barrier as scripts.
//! Derived compaction is separately safe to run while shard owners keep working.

use crate::{
    engine::{Engine, GlobalCommand},
    journal::{Batch, Mutation},
    persistence::Persistence,
    protocol::Resp,
    sharding::Paused,
    state::State,
    storage::{Stats, Store},
    wal::{Durability, Wal},
};
use bytes::Bytes;
use std::{io, path::Path, sync::Arc};

fn bulk(value: &'static str) -> Resp {
    Resp::Bulk(Some(Bytes::from_static(value.as_bytes())))
}
pub fn number(value: u64) -> Resp {
    i64::try_from(value)
        .map(Resp::Integer)
        .unwrap_or_else(|_| Resp::Bulk(Some(Bytes::from(value.to_string()))))
}
fn map(fields: impl IntoIterator<Item = (&'static str, Resp)>) -> Resp {
    Resp::Map(
        fields
            .into_iter()
            .map(|(key, value)| (bulk(key), value))
            .collect(),
    )
}

fn sha256_text(hash: &[u8; 32]) -> Bytes {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = [0u8; 64];
    for (index, byte) in hash.iter().enumerate() {
        text[index * 2] = HEX[(byte >> 4) as usize];
        text[index * 2 + 1] = HEX[(byte & 15) as usize];
    }
    Bytes::copy_from_slice(&text)
}

pub fn storage_stats(stats: Stats) -> Resp {
    map([
        ("live_values", number(stats.live_values)),
        ("live_payload_bytes", number(stats.live_payload_bytes)),
        ("physical_bytes", number(stats.physical_bytes)),
        ("cached_payload_bytes", number(stats.cached_payload_bytes)),
        ("cache_hits", number(stats.cache_hits)),
        ("cache_misses", number(stats.cache_misses)),
        ("reads", number(stats.reads)),
        ("writes", number(stats.writes)),
        ("compactions", number(stats.compactions)),
        ("reclaimed_bytes", number(stats.reclaimed_bytes)),
    ])
}

pub fn compact(store: &Arc<Store>) -> Resp {
    match store.compact() {
        Ok(stats) => storage_stats(stats),
        Err(error) => Resp::Error(format!("ERR compaction failed: {error}")),
    }
}

pub async fn execute(
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
    args: &[Bytes],
) -> io::Result<Resp> {
    let now = Engine::now();
    if args[0].eq_ignore_ascii_case(b"FFKV.STATS") {
        let status = persistence.status().await;
        state.metrics.set_wal(
            status.bytes,
            status.policy,
            status.dirty,
            status.last_sync_ms,
        );
        let store = paused.engines.first().and_then(Engine::storage).cloned();
        let mode = if store.is_some() { "hybrid" } else { "memory" };
        let (count, _) = paused.collect_global(
            GlobalCommand::Dbsize,
            &mut crate::engine::BoundCommand::bind(&[Bytes::from_static(b"DBSIZE")]),
            now,
        )?;
        return Ok(map([
            (
                "server",
                map([
                    ("name", bulk("ffkv")),
                    ("version", bulk(env!("CARGO_PKG_VERSION"))),
                    ("redis_baseline", bulk("redis-7.2")),
                    ("shards", Resp::Integer(state.shards().into())),
                    ("storage_mode", bulk(mode)),
                    ("durability", bulk(status.policy)),
                    ("role", bulk(state.role())),
                    ("instance_id", Resp::Bulk(Some(state.instance_id()))),
                    ("revision", number(status.revision)),
                    (
                        "dataset_ready",
                        Resp::Integer(i64::from(state.replica_ready())),
                    ),
                ]),
            ),
            (
                "storage",
                store
                    .map(|store| storage_stats(store.stats()))
                    .unwrap_or(Resp::Bulk(None)),
            ),
            (
                "wal",
                map([
                    ("bytes", number(status.bytes)),
                    ("policy", bulk(status.policy)),
                    ("dirty", Resp::Integer(i64::from(status.dirty))),
                    (
                        "last_sync_ms",
                        status
                            .last_sync_ms
                            .map(Resp::Integer)
                            .unwrap_or(Resp::Bulk(None)),
                    ),
                ]),
            ),
            ("key_count", count),
        ]));
    }
    if args[0].eq_ignore_ascii_case(b"FFKV.CHECKPOINT") {
        if !persistence.enabled() {
            return Ok(Resp::Error("ERR checkpoint requires --data-dir".into()));
        }
        let batches = paused
            .engines
            .iter()
            .flat_map(|engine| engine.snapshot_batches(now));
        let result = persistence.checkpoint(batches).await?;
        persistence.publish(&state.metrics).await;
        return Ok(map([
            ("old_bytes", number(result.old_bytes)),
            ("new_bytes", number(result.new_bytes)),
            ("data_frames", number(result.data_frames)),
        ]));
    }
    if args[0].eq_ignore_ascii_case(b"FFKV.BACKUP") {
        if !persistence.enabled() {
            return Ok(Resp::Error("ERR backup requires --data-dir".into()));
        }
        let valid = !args[1].is_empty()
            && args[1].len() <= 64
            && args[1]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'-' || *b == b'_');
        if !valid {
            return Ok(Resp::Error(
                "ERR backup name must be a 1..64 byte alphanumeric/hyphen/underscore component"
                    .into(),
            ));
        }
        let name = std::str::from_utf8(&args[1]).expect("ASCII name");
        let batches = paused
            .engines
            .iter()
            .flat_map(|engine| engine.snapshot_batches(now));
        return Ok(match persistence.backup(name, batches).await {
            Ok((path, result)) => {
                let status = persistence.status().await;
                map([
                    ("name", Resp::Bulk(Some(args[1].clone()))),
                    (
                        "path",
                        Resp::Bulk(Some(Bytes::from(
                            path.into_os_string().into_encoded_bytes(),
                        ))),
                    ),
                    ("instance_id", Resp::Bulk(Some(state.instance_id()))),
                    ("revision", number(status.revision)),
                    ("wal_bytes", number(result.wal_bytes)),
                    ("data_frames", number(result.data_frames)),
                    ("sha256", Resp::Bulk(Some(sha256_text(&result.sha256)))),
                ])
            }
            Err(error) => Resp::Error(format!("ERR backup failed: {error}")),
        });
    }
    Ok(Resp::Error(
        "ERR administrative command requires its supported context".into(),
    ))
}

/// Validate and stage the entire source before touching the live WAL. Once
/// checkpoint commits, install every shard together and invalidate WATCH tokens.
pub async fn install_snapshot(
    paused: &mut Paused,
    persistence: &Persistence,
    state: &State,
    directory: &Path,
) -> io::Result<()> {
    if !state.is_replica() || !persistence.enabled() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "snapshot install requires a durable development replica",
        ));
    }
    let mut recovered = Engine::new();
    recovered.inherit_runtime(&paused.engines[0]);
    recovered.set_global_metadata_owner(true);
    recovered.replay(Batch {
        timestamp_ms: 0,
        mutations: vec![Mutation::ResetGlobals],
    })?;
    let (_source, recovery) = Wal::open(directory, Durability::Strict, |batch| {
        recovered.replay(batch)
    })?;
    if recovery.truncated_bytes != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "replica snapshot has an incomplete WAL tail",
        ));
    }
    recovered.finish_recovery()?;
    let previous_databases = (0..paused.engines[0].database_count())
        .chain(paused.engines.iter().flat_map(Engine::database_ids))
        .collect::<std::collections::BTreeSet<_>>();
    persistence
        .checkpoint_replacement(&recovered, previous_databases.into_iter())
        .await?;
    recovered.set_recording(true);
    let engines = recovered.partition(usize::from(state.shards()));
    paused.engines = engines;
    persistence.watches.invalidate_all();
    state.client_registry.invalidate_all(None, None);
    persistence.note_install()?;
    state.set_replica_ready(true);
    persistence.publish(&state.metrics).await;
    Ok(())
}
