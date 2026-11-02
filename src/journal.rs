//! Durable logical changes, independent of RESP and physical WAL framing.
//! Each batch is one atomic command (or one delivered blocking pop).

use bytes::Bytes;

#[derive(Clone, Debug)]
pub struct Batch {
    pub timestamp_ms: i64,
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug)]
pub enum Mutation {
    Set {
        key: Bytes,
        value: Bytes,
        expires_at: Option<i64>,
    },
    Delete {
        key: Bytes,
    },
    Push {
        key: Bytes,
        values: Vec<Bytes>,
        right: bool,
    },
    Pop {
        key: Bytes,
        count: u64,
        right: bool,
    },
    Expire {
        key: Bytes,
        expires_at: Option<i64>,
    },
    SetAdd {
        key: Bytes,
        members: Vec<Bytes>,
    },
    SetRemove {
        key: Bytes,
        members: Vec<Bytes>,
    },
    HashSet {
        key: Bytes,
        fields: Vec<(Bytes, Bytes, Option<i64>)>,
    },
    HashDelete {
        key: Bytes,
        fields: Vec<Bytes>,
    },
    SortedSetAdd {
        key: Bytes,
        entries: Vec<(Bytes, f64)>,
    },
    SortedSetRemove {
        key: Bytes,
        members: Vec<Bytes>,
    },
    Clear,
    Rename {
        from: Bytes,
        to: Bytes,
    },
    ListRemove {
        key: Bytes,
        value: Bytes,
        count: i64,
    },
    ListSet {
        key: Bytes,
        index: u64,
        value: Bytes,
    },
    ListInsert {
        key: Bytes,
        index: u64,
        value: Bytes,
        pivot: Bytes,
        before: bool,
    },
    SelectDatabase {
        database: u32,
    },
    Search {
        effect: Bytes,
    },
    Functions {
        snapshot: Bytes,
    },
    NativePosition {
        history: Bytes,
        offset: u64,
    },
    RdbAuxiliary {
        key: Bytes,
        value: Bytes,
    },
    ResetGlobals,
    SwapDatabases {
        first: u32,
        second: u32,
    },
    ClearNativePosition,
    Queue {
        key: Bytes,
        effect: Bytes,
    },
    Typed {
        key: Bytes,
        kind: u8,
        effect: Bytes,
    },
}
