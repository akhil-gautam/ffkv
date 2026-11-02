//! Soft sampled-process-RSS admission, not exact dataset accounting or eviction.
//! The coordinator samples once per second; concurrent admitted work can overshoot.

use bytes::Bytes;

use crate::{engine::Engine, metrics::Metrics, protocol::Resp};

#[derive(Clone, Copy)]
pub struct Admission<'a> {
    pub read_only: bool,
    pub metrics: &'a Metrics,
}

impl Admission<'_> {
    pub fn reject(&self, args: &[Bytes]) -> Option<Resp> {
        if self.read_only && Engine::is_dataset_write(args) {
            return Some(Resp::Error(
                "READONLY this development replica accepts no dataset writes".into(),
            ));
        }
        self.reject_pressure(args)
    }

    pub fn reject_pressure(&self, args: &[Bytes]) -> Option<Resp> {
        if self.metrics.rss_pressure().rss_pressured && grows_dataset(args) {
            return Some(Resp::Error(
                "OOM command rejected by soft process-RSS write circuit breaker (--max-rss-bytes); sampled every 1s, not a hard memory cap or eviction policy".into(),
            ));
        }
        None
    }
}

fn grows_dataset(args: &[Bytes]) -> bool {
    if !Engine::is_dataset_write(args) {
        return false;
    }
    // These commands remove existing data without inserting a destination value.
    // Moves and blocking moves are deliberately not reclaimers; scripts remain
    // conservatively write-classified even if their source only reads/deletes.
    ![
        b"DEL".as_slice(),
        b"UNLINK",
        b"FLUSHDB",
        b"FLUSHALL",
        b"LPOP",
        b"RPOP",
        b"LREM",
        b"BLPOP",
        b"BRPOP",
        b"SPOP",
        b"SREM",
        b"HDEL",
        b"ZREM",
        b"ZPOPMIN",
        b"ZPOPMAX",
        b"FQ.ACK",
        b"FQ.CANCEL",
    ]
    .iter()
    .any(|name| args[0].eq_ignore_ascii_case(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressure_preserves_reads_maintenance_and_reclamation_but_not_moves_or_scripts() {
        for command in [
            "PING",
            "GET",
            "INFO",
            "SCAN",
            "FFKV.COMPACT",
            "FFKV.CHECKPOINT",
            "DEL",
            "UNLINK",
            "FLUSHDB",
            "FLUSHALL",
            "LPOP",
            "RPOP",
            "LREM",
            "SPOP",
            "SREM",
            "HDEL",
            "ZREM",
            "ZPOPMIN",
            "ZPOPMAX",
            "FQ.ACK",
            "FQ.CANCEL",
            "BLPOP",
            "BRPOP",
        ] {
            assert!(
                !grows_dataset(&[Bytes::copy_from_slice(command.as_bytes())]),
                "{command}"
            );
        }
        for command in [
            "SET",
            "MSET",
            "APPEND",
            "INCR",
            "LPUSH",
            "SADD",
            "HSET",
            "ZADD",
            "RENAME",
            "SMOVE",
            "EVAL",
            "EVALSHA",
            "FQ.ENQUEUE",
        ] {
            assert!(
                grows_dataset(&[Bytes::copy_from_slice(command.as_bytes())]),
                "{command}"
            );
        }
    }
}
