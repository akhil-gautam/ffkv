use super::*;

/// The command-time dataset/native operations. A transaction implements these
/// directly over its already-paused engines, without sending back to the actor.
/// Snapshot transfer, stream replay and expiry-finalization belong to background
/// ClusterHooks and intentionally cannot be requested through this interface.
pub trait ClusterCommandHooks: Send + Sync {
    fn keys(&self, slot: u16, limit: usize) -> ClusterFuture<'_, Vec<Bytes>>;
    fn slot_key_counts(&self) -> ClusterFuture<'_, Vec<u64>>;
    fn exists<'a>(&'a self, keys: &'a [Bytes]) -> ClusterFuture<'a, Vec<bool>>;
    fn database_size(&self) -> ClusterFuture<'_, u64>;
    fn delete_slots<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()>;
    fn clear(&self) -> ClusterFuture<'_, ()>;
    fn unfreeze_writes(&self) -> ClusterFuture<'_, ()>;
    fn follow<'a>(&'a self, host: &'a str, port: u16) -> ClusterFuture<'a, ()>;
    fn promote(&self, required_offset: Option<u64>) -> ClusterFuture<'_, ()>;
    fn replication_progress(&self) -> ClusterFuture<'_, ReplicationProgress>;
}

// A borrowed reference adapts the owned service backend without requiring the
// transaction adapter to implement unrelated network-transfer operations.
impl<T: ClusterHooks + ?Sized> ClusterCommandHooks for &T {
    fn keys(&self, slot: u16, limit: usize) -> ClusterFuture<'_, Vec<Bytes>> {
        ClusterHooks::keys(*self, slot, limit)
    }
    fn slot_key_counts(&self) -> ClusterFuture<'_, Vec<u64>> {
        ClusterHooks::slot_key_counts(*self)
    }
    fn exists<'a>(&'a self, keys: &'a [Bytes]) -> ClusterFuture<'a, Vec<bool>> {
        ClusterHooks::exists(*self, keys)
    }
    fn database_size(&self) -> ClusterFuture<'_, u64> {
        ClusterHooks::database_size(*self)
    }
    fn delete_slots<'a>(&'a self, ranges: &'a [(u16, u16)]) -> ClusterFuture<'a, ()> {
        ClusterHooks::delete_slots(*self, ranges)
    }
    fn clear(&self) -> ClusterFuture<'_, ()> {
        ClusterHooks::clear(*self)
    }
    fn unfreeze_writes(&self) -> ClusterFuture<'_, ()> {
        ClusterHooks::unfreeze_writes(*self)
    }
    fn follow<'a>(&'a self, host: &'a str, port: u16) -> ClusterFuture<'a, ()> {
        ClusterHooks::follow(*self, host, port)
    }
    fn promote(&self, required_offset: Option<u64>) -> ClusterFuture<'_, ()> {
        ClusterHooks::promote(*self, required_offset)
    }
    fn replication_progress(&self) -> ClusterFuture<'_, ReplicationProgress> {
        ClusterHooks::replication_progress(*self)
    }
}
