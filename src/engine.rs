mod arrays;
mod authorization;
mod catalog;
mod command_flags;
mod databases;
mod key_specs;
mod registry;
mod replica_expiry;
use registry::{effective_spec, root_spec};
mod filters;
mod general;
mod generic_compat;
mod geospatial;
mod hashes;
mod hyperloglog;
mod json;
mod lazy_free;
mod list_compat;
mod memory;
mod native;
mod queues;
mod scan;
mod search;
mod sets;
mod sketches;
mod snapshot;
mod sorted_sets;
mod streams;
mod string_compat;
mod strings;
mod timeseries;
pub(crate) mod typed;
mod vectors;
pub(crate) use search::{
    SearchRollback,
    migration::{SearchMigrationFilter, SearchMigrationImport},
};
pub(crate) use streams::BlockingStream;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;

use crate::journal::{Batch, Mutation};
use crate::protocol::Resp;
use crate::storage::{Payload, Store};
use scan::{ScanIndex, stable_hash};

const ACTIVE_EXPIRATION_BATCH: usize = 128;

/// Account for logical body bytes as well as materialized response rows. Keeping
/// this separate from wire encoding also covers RESP3's compact semantic variants.
pub(crate) struct ReplyBudget {
    remaining: usize,
}

pub(crate) fn reply_limit_error() -> Resp {
    Resp::Error("ERR response budget exceeded; use a smaller range/count".into())
}

impl ReplyBudget {
    pub(crate) fn new(limit: usize) -> Self {
        Self { remaining: limit }
    }

    pub(crate) fn charge(&mut self, rows: usize, bytes: usize) -> Result<(), Resp> {
        let size = rows
            .checked_mul(std::mem::size_of::<Resp>())
            .and_then(|size| size.checked_add(bytes))
            .ok_or_else(reply_limit_error)?;
        self.remaining = self
            .remaining
            .checked_sub(size)
            .ok_or_else(reply_limit_error)?;
        Ok(())
    }

    pub(crate) fn response(&mut self, reply: &Resp) -> Result<(), Resp> {
        self.charge(1, 0)?;
        match reply {
            Resp::Bulk(Some(bytes))
            | Resp::Status(bytes)
            | Resp::BigNumber(bytes)
            | Resp::Verbatim { value: bytes, .. } => self.charge(0, bytes.len()),
            Resp::Simple(text) => self.charge(0, text.len()),
            Resp::Error(text) => self.charge(0, text.len()),
            Resp::InfoField(field, value) => {
                self.charge(1, field.len())?;
                self.response(value)
            }
            Resp::Array(Some(values)) | Resp::Set(values) => {
                for value in values {
                    self.response(value)?;
                }
                Ok(())
            }
            Resp::Map(values) => {
                for (key, value) in values {
                    self.response(key)?;
                    self.response(value)?;
                }
                Ok(())
            }
            Resp::Scored(values) => {
                for (member, _) in values {
                    self.charge(2, member.len() + 32)?;
                }
                Ok(())
            }
            Resp::VectorResults(values) => {
                for (member, score, attr) in values {
                    self.charge(
                        1 + usize::from(score.is_some()) + usize::from(attr.is_some()),
                        member.len().saturating_add(32).saturating_add(
                            attr.as_ref()
                                .and_then(|value| value.as_ref())
                                .map_or(0, Bytes::len),
                        ),
                    )?;
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }
}

pub(crate) fn bounded_replies(values: impl IntoIterator<Item = Resp>, limit: usize) -> Resp {
    let mut budget = ReplyBudget::new(limit);
    let mut replies = Vec::new();
    for value in values {
        if let Err(error) = budget.response(&value) {
            return error;
        }
        replies.push(value);
    }
    array(replies)
}

enum PayloadReplyError {
    Io(io::Error),
    Limit,
}

fn payload_replies<'a>(
    values: impl ExactSizeIterator<Item = &'a Payload> + Clone,
    limit: usize,
) -> Result<Vec<Resp>, PayloadReplyError> {
    let metadata = values
        .len()
        .checked_mul(std::mem::size_of::<Resp>())
        .ok_or(PayloadReplyError::Limit)?;
    let mut bytes = metadata;
    for value in values.clone() {
        bytes = bytes
            .checked_add(value.len())
            .filter(|bytes| *bytes <= limit)
            .ok_or(PayloadReplyError::Limit)?;
    }
    if bytes > limit {
        return Err(PayloadReplyError::Limit);
    }
    let mut replies = Vec::with_capacity(values.len());
    for value in values {
        replies.push(Resp::Bulk(Some(
            value.read().map_err(PayloadReplyError::Io)?,
        )));
    }
    Ok(replies)
}

enum Value {
    String(Bytes),
    List(VecDeque<Payload>),
    Set(sets::SetValue),
    Hash(hashes::HashValue),
    SortedSet(sorted_sets::SortedSetValue),
    Queue(Box<crate::queue::Queue>),
    Typed(Box<typed::Data>),
}

#[derive(Clone, Copy, Default)]
pub(super) struct AccessMetadata {
    last_access_ms: i64,
    frequency: u8,
}

impl AccessMetadata {
    pub(super) fn new(now: i64) -> Self {
        Self {
            last_access_ms: now,
            frequency: 5,
        }
    }
}

pub(crate) type ReadyKey = (u32, Bytes);

#[derive(Default)]
struct DatabaseState {
    entries: HashMap<Bytes, Entry>,
    held_entries: HashMap<Bytes, Entry>,
    retained_hashes: BTreeSet<Bytes>,
    expirations: BTreeSet<(i64, Bytes)>,
    key_index: ScanIndex,
    hash_expirations: BTreeSet<(i64, Bytes)>,
    queue_expirations: BTreeSet<(i64, Bytes)>,
}

struct Entry {
    value: Value,
    expires_at: Option<i64>,
    access: AccessMetadata,
}

pub enum Execution {
    Reply(Resp),
    Pushed { reply: Resp, key: Bytes },
    Block(BlockingPop),
    StreamBlock(BlockingStream),
}

#[derive(Clone, Copy)]
pub(crate) enum BlockingKind {
    ListPop,
    MultiPop {
        count: usize,
    },
    MoveMany {
        count: usize,
        exactly: bool,
        bulk: bool,
    },
    SortedPop {
        count: usize,
        multi: bool,
    },
}

pub struct BlockingPop {
    pub keys: Vec<Bytes>,
    pub right: bool,
    pub timeout: Option<Duration>,
    pub destination: Option<(Bytes, bool)>,
    pub(crate) kind: BlockingKind,
}

impl BlockingPop {
    pub fn null_reply(&self) -> Resp {
        if self.destination.is_some() {
            Resp::Bulk(None)
        } else {
            Resp::Array(None)
        }
    }
}

pub enum Routing {
    Local {
        shard: usize,
        ready: Option<Bytes>,
    },
    Coordinated {
        keys: Vec<Bytes>,
        ready: Option<Bytes>,
    },
    Global(GlobalCommand),
    All,
    Scripting,
    Maintenance,
}

#[derive(Clone, Copy)]
pub enum GlobalCommand {
    Scan,
    Keys,
    Dbsize,
    RandomKey,
    Flush,
}

/// The actor owns this module, so an entire command runs without interleaving.
/// Every expiring entry has exactly one matching expiration-index record.
pub struct Engine {
    entries: HashMap<Bytes, Entry>,
    held_entries: HashMap<Bytes, Entry>,
    retained_hashes: BTreeSet<Bytes>,
    expirations: BTreeSet<(i64, Bytes)>,
    key_index: ScanIndex,
    hash_expirations: BTreeSet<(i64, Bytes)>,
    journal_enabled: bool,
    pending_batch: Option<Batch>,
    watches: Option<Arc<crate::watch::Registry>>,
    watch_notifications: bool,
    storage: Option<Arc<Store>>,
    failure: Option<io::Error>,
    reply_limit: usize,
    queue_expirations: BTreeSet<(i64, Bytes)>,
    command_user: Option<Arc<crate::auth::User>>,
    command_version: crate::protocol::Version,
    command_session: Option<Arc<crate::session::ClientSession>>,
    command_acl_context: &'static str,
    extensions: Option<Arc<crate::server_commands::extensions::Extensions>>,
    ready_keys: Vec<ReadyKey>,
    ready_tracking: bool,
    active_database: u32,
    databases: BTreeMap<u32, DatabaseState>,
    journal_database: u32,
    search: Option<Arc<search::Catalog>>,
    function_libraries: Bytes,
    native_position: Option<(Bytes, u64)>,
    rdb_auxiliary: Option<Arc<BTreeMap<Bytes, Bytes>>>,
    owns_global_metadata: bool,
    command_name: &'static str,
    search_reindexing: bool,
    search_plan: Option<search::SearchPlan>,
    pending_search: Vec<Mutation>,
    search_processed: usize,
    database_limit: Option<Arc<std::sync::atomic::AtomicU32>>,
    replica_mode: bool,
    replication_replay: bool,
    migration_expiry_ranges: Option<Arc<[(u16, u16)]>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Command {
    Ping,
    Echo,
    Quit,
    Get,
    Set,
    Del,
    Exists,
    Mget,
    Mset,
    Lpush,
    Rpush,
    Lpop,
    Rpop,
    Lrange,
    Llen,
    Blpop,
    Brpop,
    Lmove,
    Blmove,
    Incr,
    Incrby,
    Decr,
    Decrby,
    Expire,
    Pexpire,
    Expireat,
    Pexpireat,
    Ttl,
    Pttl,
    Expiretime,
    Pexpiretime,
    Persist,
    Type,
    Dbsize,
    Select,
    Catalog,
    CatalogInfo,
    CatalogCount,
    CatalogGetkeys,
    Extension,
    Rename,
    Renamenx,
    Lpushx,
    Rpushx,
    Publish,
    Connection,
    Scripting,
    Maintenance,
    Execution,
}

type CommandHandler = fn(&mut Engine, &[Bytes], i64) -> Resp;
type ExecutionHandler = fn(&mut Engine, &[Bytes], i64) -> Execution;

#[derive(Clone, Copy)]
struct CommandSpec {
    command: Command,
    name: &'static str,
    // Redis arity includes the command name; negative means a minimum.
    arity: i64,
    max_arity: Option<usize>,
    pairs: bool,
    flags: &'static [&'static str],
    // Legacy COMMAND positions, including negative last-key positions.
    keys: (i64, i64, i64),
    handler: Option<CommandHandler>,
    execution_handler: Option<ExecutionHandler>,
}

impl CommandSpec {
    const fn new(
        command: Command,
        name: &'static str,
        arity: i64,
        flags: &'static [&'static str],
        keys: (i64, i64, i64),
    ) -> Self {
        Self {
            command,
            name,
            arity,
            max_arity: None,
            pairs: false,
            flags,
            keys,
            handler: None,
            execution_handler: None,
        }
    }

    const fn handler(
        name: &'static str,
        arity: i64,
        flags: &'static [&'static str],
        keys: (i64, i64, i64),
        handler: CommandHandler,
    ) -> Self {
        Self {
            command: Command::Extension,
            name,
            arity,
            max_arity: None,
            pairs: false,
            flags,
            keys,
            handler: Some(handler),
            execution_handler: None,
        }
    }

    const fn execution(
        name: &'static str,
        arity: i64,
        flags: &'static [&'static str],
        keys: (i64, i64, i64),
        handler: fn(&mut Engine, &[Bytes], i64) -> Execution,
    ) -> Self {
        Self {
            command: Command::Execution,
            name,
            arity,
            max_arity: None,
            pairs: false,
            flags,
            keys,
            handler: None,
            execution_handler: Some(handler),
        }
    }

    const fn max(mut self, max: usize) -> Self {
        self.max_arity = Some(max);
        self
    }

    const fn pairs(mut self) -> Self {
        self.pairs = true;
        self
    }

    fn accepts(&self, count: usize) -> bool {
        let arity_matches = if self.arity >= 0 {
            count == self.arity as usize
        } else {
            count >= (-self.arity) as usize
        };
        arity_matches
            && self.max_arity.is_none_or(|max| count <= max)
            && (!self.pairs || count % 2 == 1)
    }

    fn key_positions(&self, args: &[Bytes]) -> Result<Option<key_specs::KeyPositions>, Resp> {
        if let Some(result) = key_specs::compiled(self.name, args) {
            return result;
        }
        let count = args.len();
        let invalid = || error("ERR Invalid arguments specified for command");
        let mut prefix = None;
        let mut suffix = None;
        let (mut first, mut end, mut step) = if self.keys.0 > 0 && self.keys.2 > 0 {
            let last = if self.keys.1 < 0 {
                count as i64 + self.keys.1
            } else {
                self.keys.1
            };
            if last < self.keys.0 || last >= count as i64 {
                return Err(invalid());
            }
            (
                self.keys.0 as usize,
                last as usize + 1,
                self.keys.2 as usize,
            )
        } else {
            (0, 0, 1)
        };
        match self.name {
            "eval" | "evalsha" | "cms.merge" | "tdigest.merge" => {
                let keys = args
                    .get(2)
                    .and_then(|value| integer(value))
                    .filter(|count| *count >= 0)
                    .ok_or_else(invalid)? as u64;
                if keys > count.saturating_sub(3) as u64 {
                    return Err(invalid());
                }
                if matches!(self.name, "cms.merge" | "tdigest.merge") {
                    if keys == 0 {
                        return Err(invalid());
                    }
                    prefix = Some(1);
                }
                first = 3;
                end = first + keys as usize;
                step = 1;
            }
            "ts.nrange" | "ts.nrevrange" => {
                let keys = args
                    .get(1)
                    .and_then(|value| integer(value))
                    .filter(|count| *count > 0)
                    .ok_or_else(invalid)? as u64;
                if keys > count.saturating_sub(4) as u64 {
                    return Err(invalid());
                }
                first = 2;
                end = first + keys as usize;
                step = 1;
            }
            "xread" | "xreadgroup" => {
                let mut index = if self.name == "xreadgroup" { 4 } else { 1 };
                loop {
                    let option = args.get(index).ok_or_else(invalid)?;
                    if option.eq_ignore_ascii_case(b"STREAMS") {
                        break;
                    }
                    if [b"COUNT".as_slice(), b"BLOCK", b"MAXCOUNT", b"MAXSIZE"]
                        .iter()
                        .any(|name| option.eq_ignore_ascii_case(name))
                        || (self.name == "xreadgroup" && option.eq_ignore_ascii_case(b"CLAIM"))
                    {
                        index += 2;
                    } else if self.name == "xreadgroup" && option.eq_ignore_ascii_case(b"NOACK") {
                        index += 1;
                    } else {
                        return Err(invalid());
                    }
                }
                let remaining = count - index - 1;
                if remaining == 0 || !remaining.is_multiple_of(2) {
                    return Err(invalid());
                }
                first = index + 1;
                end = first + remaining / 2;
                step = 1;
            }
            "xgroup" | "xinfo" | "json.debug" => {
                let subcommand = args.get(1).ok_or_else(invalid)?;
                if subcommand.eq_ignore_ascii_case(b"HELP") {
                    first = 0;
                    end = 0;
                } else {
                    if count < 3 {
                        return Err(invalid());
                    }
                    first = 2;
                    end = 3;
                    step = 1;
                }
            }
            "json.mget" => {
                first = 1;
                end = count.checked_sub(1).ok_or_else(invalid)?;
                step = 1;
            }
            "json.mset" | "ts.madd" => {
                if count < 4 || !(count - 1).is_multiple_of(3) {
                    return Err(invalid());
                }
                first = 1;
                end = count;
                step = 3;
            }
            "georadius" | "georadiusbymember" => {
                first = 1;
                end = 2;
                step = 1;
                let mut index = if self.name == "georadius" { 6 } else { 5 };
                while index < count {
                    if args[index].eq_ignore_ascii_case(b"STORE")
                        || args[index].eq_ignore_ascii_case(b"STOREDIST")
                    {
                        if suffix.is_some() || index + 1 >= count {
                            return Err(invalid());
                        }
                        suffix = Some(index + 1);
                        index += 2;
                    } else if args[index].eq_ignore_ascii_case(b"COUNT") {
                        index += 2;
                    } else {
                        index += 1;
                    }
                }
            }
            "sort" | "sort_ro" => {
                let mut index = 2;
                while index < count {
                    let option = &args[index];
                    if option.eq_ignore_ascii_case(b"STORE") && self.name == "sort" {
                        if index + 1 >= count {
                            return Err(invalid());
                        }
                        suffix = Some(index + 1);
                        index += 2;
                    } else if option.eq_ignore_ascii_case(b"BY")
                        || option.eq_ignore_ascii_case(b"GET")
                    {
                        if index + 1 >= count {
                            return Err(invalid());
                        }
                        index += 2;
                    } else if option.eq_ignore_ascii_case(b"LIMIT") {
                        if index + 2 >= count {
                            return Err(invalid());
                        }
                        index += 3;
                    } else if [b"ASC".as_slice(), b"DESC", b"ALPHA"]
                        .iter()
                        .any(|name| option.eq_ignore_ascii_case(name))
                    {
                        index += 1;
                    } else {
                        return Err(invalid());
                    }
                }
            }
            "migrate" => {
                first = 3;
                end = 4;
                step = 1;
                let mut index = 6;
                while index < count {
                    let option = &args[index];
                    if option.eq_ignore_ascii_case(b"KEYS") {
                        if !args[3].is_empty() || index + 1 >= count {
                            return Err(invalid());
                        }
                        first = index + 1;
                        end = count;
                        break;
                    }
                    let width = if option.eq_ignore_ascii_case(b"AUTH2") {
                        3
                    } else if option.eq_ignore_ascii_case(b"AUTH") {
                        2
                    } else if option.eq_ignore_ascii_case(b"COPY")
                        || option.eq_ignore_ascii_case(b"REPLACE")
                    {
                        1
                    } else {
                        return Err(invalid());
                    };
                    if width > count - index {
                        return Err(invalid());
                    }
                    index += width;
                }
            }
            _ => {}
        }
        if first >= end && prefix.is_none() && suffix.is_none() {
            return Ok(None);
        }
        Ok(Some(key_specs::KeyPositions::new([
            prefix.map_or((0, 0, 1), |position| (position, position + 1, 1)),
            (first, end, step),
            suffix.map_or((0, 0, 1), |position| (position, position + 1, 1)),
        ])))
    }
}

const NO_KEYS: (i64, i64, i64) = (0, 0, 0);
const ONE_KEY: (i64, i64, i64) = (1, 1, 1);
const ALL_KEYS: (i64, i64, i64) = (1, -1, 1);

// This registry drives dispatch, common validation and COMMAND introspection.
static COMMANDS: &[CommandSpec] = &[
    CommandSpec::new(Command::Ping, "ping", -1, &["fast"], NO_KEYS).max(2),
    CommandSpec::new(Command::Echo, "echo", 2, &["fast"], NO_KEYS),
    CommandSpec::new(Command::Quit, "quit", 1, &["fast"], NO_KEYS),
    CommandSpec::new(Command::Get, "get", 2, &["readonly", "fast"], ONE_KEY),
    CommandSpec::new(Command::Set, "set", -3, &["write", "denyoom"], ONE_KEY),
    CommandSpec::new(Command::Del, "del", -2, &["write"], ALL_KEYS),
    CommandSpec::new(
        Command::Exists,
        "exists",
        -2,
        &["readonly", "fast"],
        ALL_KEYS,
    ),
    CommandSpec::new(Command::Mget, "mget", -2, &["readonly", "fast"], ALL_KEYS),
    CommandSpec::new(Command::Mset, "mset", -3, &["write", "denyoom"], (1, -1, 2)).pairs(),
    CommandSpec::new(
        Command::Lpush,
        "lpush",
        -3,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Rpush,
        "rpush",
        -3,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(Command::Lpop, "lpop", -2, &["write", "fast"], ONE_KEY).max(3),
    CommandSpec::new(Command::Rpop, "rpop", -2, &["write", "fast"], ONE_KEY).max(3),
    CommandSpec::new(Command::Lrange, "lrange", 4, &["readonly"], ONE_KEY),
    CommandSpec::new(Command::Llen, "llen", 2, &["readonly", "fast"], ONE_KEY),
    CommandSpec::new(
        Command::Blpop,
        "blpop",
        -3,
        &["write", "blocking"],
        (1, -2, 1),
    ),
    CommandSpec::new(
        Command::Brpop,
        "brpop",
        -3,
        &["write", "blocking"],
        (1, -2, 1),
    ),
    CommandSpec::new(
        Command::Incr,
        "incr",
        2,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Incrby,
        "incrby",
        3,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Decr,
        "decr",
        2,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Decrby,
        "decrby",
        3,
        &["write", "denyoom", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(Command::Expire, "expire", -3, &["write", "fast"], ONE_KEY),
    CommandSpec::new(Command::Pexpire, "pexpire", -3, &["write", "fast"], ONE_KEY),
    CommandSpec::new(
        Command::Expireat,
        "expireat",
        -3,
        &["write", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Pexpireat,
        "pexpireat",
        -3,
        &["write", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(Command::Ttl, "ttl", 2, &["readonly", "fast"], ONE_KEY),
    CommandSpec::new(Command::Pttl, "pttl", 2, &["readonly", "fast"], ONE_KEY),
    CommandSpec::new(
        Command::Expiretime,
        "expiretime",
        2,
        &["readonly", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(
        Command::Pexpiretime,
        "pexpiretime",
        2,
        &["readonly", "fast"],
        ONE_KEY,
    ),
    CommandSpec::new(Command::Persist, "persist", 2, &["write", "fast"], ONE_KEY),
    CommandSpec::new(Command::Type, "type", 2, &["readonly", "fast"], ONE_KEY),
    // Counting logically live keys can drain overdue expiration-index records.
    CommandSpec::new(Command::Dbsize, "dbsize", 1, &["readonly"], NO_KEYS),
    CommandSpec::new(Command::Select, "select", 2, &["fast"], NO_KEYS),
    CommandSpec::new(Command::Catalog, "command", -1, &[], NO_KEYS),
    CommandSpec::new(
        Command::Connection,
        "hello",
        -1,
        &["noscript", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Connection, "client", -2, &["noscript"], NO_KEYS),
    CommandSpec::new(Command::Connection, "info", -1, &["readonly"], NO_KEYS),
    CommandSpec::new(
        Command::Connection,
        "auth",
        -2,
        &["noscript", "fast"],
        NO_KEYS,
    )
    .max(3),
    CommandSpec::new(Command::Connection, "acl", -2, &["noscript"], NO_KEYS),
    CommandSpec::new(
        Command::Maintenance,
        "ffkv.stats",
        1,
        &["readonly", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "ffkv.compact",
        1,
        &["write", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "ffkv.checkpoint",
        1,
        &["write", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "ffkv.backup",
        2,
        &["write", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "multi",
        1,
        &["noscript", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Connection, "exec", 1, &["noscript"], NO_KEYS),
    CommandSpec::new(
        Command::Connection,
        "discard",
        1,
        &["noscript", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "watch",
        -2,
        &["noscript", "fast"],
        ALL_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "unwatch",
        1,
        &["noscript", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "eval",
        -3,
        &["noscript", "movablekeys"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "evalsha",
        -3,
        &["noscript", "movablekeys"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Scripting, "script", -2, &["noscript"], NO_KEYS),
    CommandSpec::new(
        Command::Publish,
        "publish",
        3,
        &["pubsub", "loading", "stale", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "subscribe",
        -2,
        &["pubsub", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "psubscribe",
        -2,
        &["pubsub", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "unsubscribe",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "punsubscribe",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub",
        -2,
        &["pubsub", "readonly", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
];

static COMMAND_SUBCOMMANDS: &[CommandSpec] = &[
    CommandSpec::new(Command::CatalogInfo, "command|info", -2, &[], NO_KEYS),
    CommandSpec::new(Command::CatalogCount, "command|count", 2, &[], NO_KEYS),
    CommandSpec::new(Command::CatalogGetkeys, "command|getkeys", -3, &[], NO_KEYS),
];

fn specifications() -> impl Iterator<Item = &'static CommandSpec> {
    COMMANDS
        .iter()
        .chain(sets::COMMANDS)
        .chain(generic_compat::COMMANDS)
        .chain(hashes::COMMANDS)
        .chain(hashes::compat::COMMANDS)
        .chain(list_compat::COMMANDS)
        .chain(sorted_sets::COMMANDS)
        .chain(general::COMMANDS)
        .chain(strings::COMMANDS)
        .chain(string_compat::COMMANDS)
        .chain(queues::COMMANDS)
        .chain(arrays::COMMANDS)
        .chain(filters::COMMANDS)
        .chain(geospatial::COMMANDS)
        .chain(hyperloglog::COMMANDS)
        .chain(json::COMMANDS)
        .chain(sketches::COMMANDS)
        .chain(streams::COMMANDS)
        .chain(timeseries::COMMANDS)
        .chain(vectors::COMMANDS)
        .chain(search::COMMANDS)
        .chain(catalog::COMMANDS)
}

fn subcommand_specifications() -> impl Iterator<Item = &'static CommandSpec> {
    COMMAND_SUBCOMMANDS
        .iter()
        .chain(hashes::compat::HIMPORT_SUBCOMMANDS)
        .chain(catalog::SUBCOMMANDS)
}

fn all_specifications() -> impl Iterator<Item = &'static CommandSpec> {
    specifications().chain(subcommand_specifications())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExpiryKind {
    Ex,
    Px,
    Exat,
    Pxat,
}

impl ExpiryKind {
    fn deadline(self, value: i64, now: i64) -> Option<i64> {
        let milliseconds = match self {
            Self::Ex | Self::Exat => value.checked_mul(1000)?,
            Self::Px | Self::Pxat => value,
        };
        match self {
            Self::Ex | Self::Px => now.checked_add(milliseconds),
            Self::Exat | Self::Pxat => Some(milliseconds),
        }
    }
}

impl Engine {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            held_entries: HashMap::new(),
            retained_hashes: BTreeSet::new(),
            expirations: BTreeSet::new(),
            storage: None,
            failure: None,
            reply_limit: 64 * 1024 * 1024,
            queue_expirations: BTreeSet::new(),
            key_index: ScanIndex::new(),
            hash_expirations: BTreeSet::new(),
            journal_enabled: false,
            pending_batch: None,
            watches: None,
            watch_notifications: true,
            command_user: None,
            command_version: crate::protocol::Version::Resp2,
            command_session: None,
            command_acl_context: "toplevel",
            extensions: None,
            ready_keys: Vec::new(),
            ready_tracking: false,
            active_database: 0,
            databases: BTreeMap::new(),
            journal_database: 0,
            search: None,
            function_libraries: Bytes::new(),
            native_position: None,
            rdb_auxiliary: None,
            owns_global_metadata: true,
            command_name: "",
            search_reindexing: true,
            search_plan: None,
            pending_search: Vec::new(),
            search_processed: 0,
            database_limit: None,
            replica_mode: false,
            replication_replay: false,
            migration_expiry_ranges: None,
        }
    }

    pub(crate) fn database(&self) -> u32 {
        self.active_database
    }

    pub(crate) fn database_ids(&self) -> impl Iterator<Item = u32> + '_ {
        std::iter::once(self.active_database).chain(self.databases.keys().copied())
    }

    pub(crate) fn live_keys(&self, now: i64) -> impl Iterator<Item = &Bytes> {
        self.entries
            .iter()
            .chain(self.held_entries.iter().filter(move |_| now == 0))
            .filter(move |(_, entry)| Self::entry_live(entry, now))
            .map(|(key, _)| key)
    }

    pub(crate) fn live_key_exists(&self, key: &Bytes, now: i64) -> bool {
        self.entries
            .get(key)
            .or_else(|| {
                if now == 0 {
                    self.held_entries.get(key)
                } else {
                    None
                }
            })
            .is_some_and(|entry| Self::entry_live(entry, now))
    }

    pub(crate) fn function_snapshot(&self) -> Bytes {
        self.function_libraries.clone()
    }
    pub(crate) fn set_function_snapshot(&mut self, snapshot: Bytes) {
        self.function_libraries = snapshot;
    }
    pub(crate) fn native_position(&self) -> Option<(Bytes, u64)> {
        self.native_position.clone()
    }
    pub(crate) fn set_native_position(&mut self, history: Bytes, offset: u64) {
        self.native_position = Some((history, offset));
    }
    pub(crate) fn set_global_metadata_owner(&mut self, owner: bool) {
        self.owns_global_metadata = owner;
    }
    pub(crate) fn set_search_reindexing(&mut self, enabled: bool) {
        self.search_reindexing = enabled;
    }
    pub(crate) fn database_count(&self) -> u32 {
        self.database_limit
            .as_ref()
            .map_or(16, |limit| limit.load(std::sync::atomic::Ordering::Acquire))
    }
    pub(crate) fn set_database_limit(&mut self, limit: Arc<std::sync::atomic::AtomicU32>) {
        self.database_limit = Some(limit);
    }
    pub(crate) fn command_session(&self) -> Option<&Arc<crate::session::ClientSession>> {
        self.command_session.as_ref()
    }

    pub(crate) fn set_extensions(
        &mut self,
        extensions: Arc<crate::server_commands::extensions::Extensions>,
    ) {
        self.extensions = Some(extensions);
    }

    pub(crate) fn take_failure(&mut self) -> Option<io::Error> {
        self.failure.take()
    }
    pub(crate) fn is_readonly(args: &[Bytes]) -> bool {
        effective_spec(args).is_some_and(|spec| spec.flags.contains(&"readonly"))
    }
    pub(crate) fn finish_pending_search(&mut self) {
        if self.pending_batch.is_none() && self.pending_search.is_empty() {
            return;
        }
        let now = self
            .pending_batch
            .as_ref()
            .map_or_else(unix_milliseconds, |batch| batch.timestamp_ms);
        self.finish_search_changes(now);
    }
    pub(super) fn capturing_changes(&self) -> bool {
        self.journal_enabled
            || self.ready_tracking
            || (self.search_reindexing
                && self
                    .search
                    .as_ref()
                    .is_some_and(|catalog| catalog.has_indexes()))
    }
    pub(super) fn command_prefixes_readable(&self, prefixes: &[Bytes]) -> bool {
        authorization::prefixes_readable(self, prefixes)
    }
    pub(crate) fn clear_search(&mut self) {
        self.search = None;
    }
    pub(crate) fn rdb_auxiliary(&self) -> impl Iterator<Item = (&Bytes, &Bytes)> {
        self.rdb_auxiliary
            .as_deref()
            .into_iter()
            .flat_map(|values| values.iter())
    }
    pub(crate) fn set_rdb_auxiliary(&mut self, values: Vec<(Bytes, Bytes)>) {
        self.rdb_auxiliary = Some(Arc::new(values.into_iter().collect()));
    }

    pub(crate) fn highest_nonempty_database(&self, now: i64) -> Option<u32> {
        std::iter::once((self.active_database, &self.entries))
            .chain(
                self.databases
                    .iter()
                    .map(|(database, state)| (*database, &state.entries)),
            )
            .filter(|(_, entries)| {
                entries
                    .values()
                    .any(|entry| entry.expires_at.is_none_or(|deadline| deadline > now))
            })
            .map(|(database, _)| database)
            .max()
    }

    fn take_database(&mut self) -> DatabaseState {
        DatabaseState {
            entries: std::mem::take(&mut self.entries),
            held_entries: std::mem::take(&mut self.held_entries),
            retained_hashes: std::mem::take(&mut self.retained_hashes),
            expirations: std::mem::take(&mut self.expirations),
            key_index: std::mem::take(&mut self.key_index),
            hash_expirations: std::mem::take(&mut self.hash_expirations),
            queue_expirations: std::mem::take(&mut self.queue_expirations),
        }
    }

    fn install_database(&mut self, state: DatabaseState) {
        self.entries = state.entries;
        self.held_entries = state.held_entries;
        self.retained_hashes = state.retained_hashes;
        self.expirations = state.expirations;
        self.key_index = state.key_index;
        self.hash_expirations = state.hash_expirations;
        self.queue_expirations = state.queue_expirations;
    }

    pub(crate) fn switch_database(&mut self, database: u32) {
        if database == self.active_database {
            return;
        }
        let target = self.databases.remove(&database).unwrap_or_default();
        let previous = self.take_database();
        if !previous.entries.is_empty() || !previous.held_entries.is_empty() {
            self.databases.insert(self.active_database, previous);
        }
        self.active_database = database;
        self.install_database(target);
    }

    pub fn key_hash(key: &Bytes) -> u64 {
        stable_hash(key)
    }

    pub fn shard_for(key: &Bytes, count: usize) -> usize {
        (stable_hash(key) % count as u64) as usize
    }
    pub(crate) fn set_command_user(&mut self, user: Option<Arc<crate::auth::User>>) {
        self.command_user = user;
    }

    pub(crate) fn set_command_session(
        &mut self,
        session: Option<Arc<crate::session::ClientSession>>,
    ) {
        if let Some(session) = &session {
            self.switch_database(session.database());
        }
        if session.is_none() {
            self.command_acl_context = "toplevel";
        }
        self.command_session = session;
    }
    pub(crate) fn set_command_acl_context(&mut self, context: &'static str) {
        self.command_acl_context = context;
    }

    pub(super) fn command_hash_fieldsets(&self) -> Option<&crate::session::HashFieldsets> {
        self.command_session
            .as_ref()
            .map(|session| &session.hash_fieldsets)
    }

    pub(crate) fn set_watch_notifications(&mut self, enabled: bool) {
        self.watch_notifications = enabled;
    }

    pub(crate) fn set_command_version(&mut self, version: crate::protocol::Version) {
        self.command_version = version;
    }

    pub(crate) fn set_command_name(&mut self, args: &[Bytes]) {
        self.command_name = effective_spec(args).map_or("", |spec| spec.name);
    }

    pub(super) fn command_version(&self) -> crate::protocol::Version {
        self.command_version
    }

    pub(super) fn command_key_readable(&self, key: &Bytes) -> bool {
        authorization::key_readable(self, key)
    }

    pub(crate) fn related_keys<'a>(
        &'a self,
        key: &Bytes,
        now: i64,
    ) -> impl Iterator<Item = &'a Bytes> + use<'a> {
        let series = self
            .entries
            .get(key)
            .filter(|entry| entry.expires_at.is_none_or(|deadline| deadline > now))
            .and_then(|entry| match &entry.value {
                Value::Typed(data) => match data.as_ref() {
                    typed::Data::TimeSeries(series) => Some(series),
                    _ => None,
                },
                _ => None,
            });
        series
            .into_iter()
            .flat_map(timeseries::TimeSeriesValue::related_keys)
    }

    pub fn now() -> i64 {
        unix_milliseconds()
    }
    pub(crate) fn requires_all_keys(name: &str) -> bool {
        matches!(
            name,
            "ts.alter"
                | "ts.add"
                | "ts.madd"
                | "ts.incrby"
                | "ts.decrby"
                | "ts.del"
                | "ts.createrule"
                | "ts.deleterule"
                | "ts.mget"
                | "ts.mrange"
                | "ts.mrevrange"
                | "ts.queryindex"
                | "ts.querylabels"
                | "ts.info"
        )
    }

    pub fn routing(args: &[Bytes], shard_count: usize) -> Routing {
        let local_error = || Routing::Local {
            shard: 0,
            ready: None,
        };
        let Some(spec) = effective_spec(args) else {
            return local_error();
        };
        let root = root_spec(args).expect("effective command has a root");
        if !spec.accepts(args.len()) {
            return local_error();
        }
        let latest_start = match spec.name {
            "ts.get" => Some(2),
            "ts.range" | "ts.revrange" => Some(4),
            "ts.nrange" | "ts.nrevrange" => args
                .get(1)
                .and_then(|value| integer(value))
                .and_then(|count| usize::try_from(count).ok())
                .and_then(|count| count.checked_add(4)),
            _ => None,
        };
        if latest_start
            .and_then(|start| args.get(start..))
            .is_some_and(|options| {
                options
                    .iter()
                    .any(|value| value.eq_ignore_ascii_case(b"LATEST"))
            })
        {
            return Routing::All;
        }
        if root.command == Command::Scripting {
            return Routing::Scripting;
        }
        if root.command == Command::Maintenance {
            return Routing::Maintenance;
        }
        let global = match spec.name {
            "scan" => Some(GlobalCommand::Scan),
            "keys" => Some(GlobalCommand::Keys),
            "dbsize" => Some(GlobalCommand::Dbsize),
            "randomkey" => Some(GlobalCommand::RandomKey),
            "flushdb" | "flushall" => Some(GlobalCommand::Flush),
            _ => None,
        };
        if let Some(global) = global {
            return Routing::Global(global);
        }
        if Self::requires_all_keys(spec.name)
            || spec.name.starts_with("ft.")
            || matches!(spec.name, "copy" | "move" | "swapdb" | "sort" | "sort_ro")
        {
            return Routing::All;
        }
        let Ok(Some(positions)) = spec.key_positions(args) else {
            return local_error();
        };
        let first = positions
            .clone()
            .next()
            .expect("nonempty key specification");
        let shard = Self::shard_for(&args[first], shard_count);
        let ready = match spec.command {
            Command::Lpush | Command::Rpush | Command::Lpushx | Command::Rpushx => {
                Some(args[1].clone())
            }
            Command::Rename | Command::Renamenx | Command::Lmove | Command::Blmove => {
                Some(args[2].clone())
            }
            _ if spec.name == "xadd" => Some(args[1].clone()),
            _ => None,
        };
        if !spec.flags.contains(&"blocking")
            && !matches!(
                spec.command,
                Command::Blpop | Command::Brpop | Command::Blmove
            )
            && !matches!(spec.name, "xread" | "xreadgroup" | "ts.read")
            && positions
                .clone()
                .all(|index| Self::shard_for(&args[index], shard_count) == shard)
        {
            Routing::Local { shard, ready }
        } else {
            Routing::Coordinated {
                keys: positions.map(|index| args[index].clone()).collect(),
                ready,
            }
        }
    }

    pub fn accessed_keys(args: &[Bytes]) -> Vec<Bytes> {
        effective_spec(args)
            .and_then(|spec| spec.key_positions(args).ok().flatten())
            .map(|positions| positions.map(|index| args[index].clone()).collect())
            .unwrap_or_default()
    }

    pub fn any_accessed_key(args: &[Bytes], mut predicate: impl FnMut(&Bytes) -> bool) -> bool {
        effective_spec(args).is_some_and(|spec| {
            Self::requires_all_keys(spec.name)
                || spec
                    .key_positions(args)
                    .ok()
                    .flatten()
                    .is_some_and(|mut positions| positions.any(|index| predicate(&args[index])))
        })
    }

    /// Validate command identity and arity without executing counted-key parsing.
    pub fn validate_admission(args: &[Bytes]) -> Result<(), Resp> {
        let Some(root) = root_spec(args) else {
            return Err(error(if args.is_empty() {
                "ERR empty command"
            } else {
                "ERR unknown command"
            }));
        };
        if !root.accepts(args.len()) {
            return Err(wrong_arity(root.name));
        }
        let spec = effective_spec(args).expect("registered root command");
        if spec.name == root.name
            && registry::is_container(root.name)
            && args.len() > 1
            && !(args.len() == 2 && args[1].eq_ignore_ascii_case(b"HELP"))
        {
            return Err(error("ERR unknown subcommand"));
        }
        if !spec.accepts(args.len()) {
            return Err(wrong_arity(spec.name));
        }
        Ok(())
    }

    pub fn validate(args: &[Bytes]) -> Result<(), Resp> {
        Self::validate_admission(args)?;
        let root = root_spec(args).expect("admitted command metadata");
        let spec = effective_spec(args).expect("admitted command metadata");
        spec.key_positions(args)?;
        if spec.name != root.name {
            root.key_positions(args)?;
        }
        Ok(())
    }

    pub fn script_allowed(args: &[Bytes]) -> bool {
        effective_spec(args).is_some_and(|spec| !spec.flags.contains(&"noscript"))
    }

    pub fn transaction_allowed(args: &[Bytes]) -> bool {
        effective_spec(args).is_some_and(|spec| !spec.flags.contains(&"no_multi"))
    }

    pub fn monitor_allowed(args: &[Bytes]) -> bool {
        effective_spec(args).is_some_and(|spec| {
            spec.name != "quit"
                && !spec.flags.contains(&"admin")
                && !spec.flags.contains(&"skip_monitor")
        })
    }

    pub fn is_control(args: &[Bytes]) -> bool {
        root_spec(args).is_some_and(|spec| {
            matches!(
                spec.command,
                Command::Ping
                    | Command::Echo
                    | Command::Quit
                    | Command::Select
                    | Command::Catalog
                    | Command::Connection
                    | Command::Publish
                    | Command::Maintenance
            ) || spec.name == "script"
        })
    }

    pub fn is_dataset_write(args: &[Bytes]) -> bool {
        effective_spec(args).is_some_and(|spec| {
            spec.command != Command::Maintenance
                && (spec.flags.contains(&"write")
                    || matches!(spec.name, "eval" | "evalsha" | "fcall"))
        })
    }

    pub fn is_pause_write(args: &[Bytes]) -> bool {
        Self::is_dataset_write(args)
            || effective_spec(args).is_some_and(|spec| spec.flags.contains(&"may_replicate"))
    }

    pub fn watch_deadline(&mut self, key: &Bytes, now: i64) -> Option<i64> {
        self.discard_expired(key, now);
        let entry = self.entries.get(key)?;
        let internal = self
            .hash_deadline(key)
            .into_iter()
            .chain(self.queue_deadline(key))
            .min();
        match (entry.expires_at, internal) {
            (Some(key), Some(field)) => Some(key.min(field)),
            (key, field) => key.or(field),
        }
    }

    pub fn set_watches(&mut self, watches: Arc<crate::watch::Registry>) {
        self.watches = Some(watches);
    }

    pub fn inherit_runtime(&mut self, source: &Self) {
        self.watches = source.watches.clone();
        self.watch_notifications = source.watch_notifications;
        self.storage = source.storage.clone();
        self.reply_limit = source.reply_limit;
        self.ready_tracking = source.ready_tracking;
        self.search.clone_from(&source.search);
        self.extensions.clone_from(&source.extensions);
        self.function_libraries
            .clone_from(&source.function_libraries);
        self.native_position.clone_from(&source.native_position);
        self.rdb_auxiliary.clone_from(&source.rdb_auxiliary);
        self.owns_global_metadata = false;
        self.database_limit.clone_from(&source.database_limit);
        self.replica_mode = source.replica_mode;
        self.replication_replay = source.replication_replay;
        self.migration_expiry_ranges
            .clone_from(&source.migration_expiry_ranges);
        self.search_reindexing = source.search_reindexing;
        self.switch_database(source.active_database);
    }

    pub fn set_storage(&mut self, storage: Arc<Store>) {
        assert!(self.entries.is_empty(), "configure storage before recovery");
        self.storage = Some(storage);
    }

    pub fn storage(&self) -> Option<&Arc<Store>> {
        self.storage.as_ref()
    }

    pub fn set_reply_limit(&mut self, limit: usize) {
        self.reply_limit = limit;
    }

    pub(crate) fn reply_limit(&self) -> usize {
        self.reply_limit
    }

    fn fail_payload_reply(&mut self, error: PayloadReplyError) -> Resp {
        match error {
            PayloadReplyError::Io(error) => self.fail_io(error),
            PayloadReplyError::Limit => Resp::Error(
                "ERR list reply exceeds memory budget; use a smaller range/count".into(),
            ),
        }
    }

    fn payload(&self, value: Bytes) -> io::Result<Payload> {
        match &self.storage {
            Some(store) => store.put(value),
            None => Ok(Payload::Inline(value)),
        }
    }

    fn fail_io(&mut self, error: io::Error) -> Resp {
        // The public execution boundary returns this error before any commit
        // or reply. No handler error-shaped sentinel is ever sent on the wire.
        if self.failure.is_none() {
            self.failure = Some(error);
        }
        Resp::Error("IOERR storage command failed".into())
    }

    pub fn partition(self, count: usize) -> Vec<Self> {
        let mut shards: Vec<_> = std::iter::repeat_with(Self::new).take(count).collect();
        for shard in &mut shards {
            shard.inherit_runtime(&self);
            shard.journal_enabled = self.journal_enabled;
        }
        self.distribute(&mut shards);
        shards
    }

    pub fn transfer_keys(&mut self, keys: &[Bytes], target: &mut Self) {
        for key in keys {
            if let Some((key, entry)) = self.detach_entry(key) {
                target.insert_entry_raw(key, entry);
            } else if let Some((key, entry)) = self.held_entries.remove_entry(key) {
                self.retained_hashes.remove(&key);
                target.held_entries.insert(key, entry);
            }
        }
    }

    pub fn distribute(mut self, targets: &mut [Self]) {
        assert!(
            self.pending_batch.is_none(),
            "commit before transferring engine state"
        );
        let active = self.active_database;
        let current = self.take_database();
        let databases = std::iter::once((active, current)).chain(self.databases);
        for target in targets.iter_mut() {
            if self.search.is_some() {
                target.search.clone_from(&self.search);
            }
            target
                .function_libraries
                .clone_from(&self.function_libraries);
            target.native_position.clone_from(&self.native_position);
            target.rdb_auxiliary.clone_from(&self.rdb_auxiliary);
            target.owns_global_metadata = false;
            target.database_limit.clone_from(&self.database_limit);
            target.replica_mode = self.replica_mode;
            target
                .migration_expiry_ranges
                .clone_from(&self.migration_expiry_ranges);
        }
        if let Some(first) = targets.first_mut() {
            first.owns_global_metadata = true;
        }
        for (database, state) in databases {
            for target in targets.iter_mut() {
                target.switch_database(database);
            }
            for (key, entry) in state.entries {
                let shard = Self::shard_for(&key, targets.len());
                targets[shard].insert_entry_raw(key, entry);
            }
            for (key, entry) in state.held_entries {
                let shard = Self::shard_for(&key, targets.len());
                targets[shard].held_entries.insert(key, entry);
            }
        }
        for target in targets {
            target.switch_database(active);
        }
    }

    pub fn set_recording(&mut self, enabled: bool) {
        debug_assert!(
            self.pending_batch.is_none(),
            "drain captured effects before changing recording mode"
        );
        self.journal_enabled = enabled;
    }

    /// Drain after each execution or successful scheduler pop, before replying.
    pub fn take_batch(&mut self) -> Option<Batch> {
        self.search_processed = 0;
        self.pending_batch.take()
    }

    /// Drain only after the corresponding logical effects have committed.
    pub(crate) fn take_ready_keys(&mut self) -> Vec<ReadyKey> {
        std::mem::take(&mut self.ready_keys)
    }
    pub(crate) fn set_ready_tracking(&mut self, enabled: bool) {
        self.ready_tracking = enabled;
    }

    /// Apply resolved changes at their original time, never recording them again.
    pub fn replay(&mut self, batch: Batch) -> io::Result<()> {
        let journal = self.journal_enabled;
        let ready = self.ready_tracking;
        let indexing = self.search_reindexing;
        self.journal_enabled = false;
        self.ready_tracking = false;
        self.search_reindexing = false;
        let result = self.replay_inner(batch);
        self.journal_enabled = journal;
        self.ready_tracking = ready;
        self.search_reindexing = indexing;
        result
    }

    fn replay_inner(&mut self, batch: Batch) -> io::Result<()> {
        if batch.mutations.is_empty() {
            return Err(invalid_journal());
        }
        self.switch_database(0);
        let now = batch.timestamp_ms;
        for mutation in batch.mutations {
            self.prepare_replay_mutation(&mutation);
            match mutation {
                Mutation::Set {
                    key,
                    value,
                    expires_at,
                } => {
                    if expires_at.is_some_and(|deadline| deadline <= now) {
                        return Err(invalid_journal());
                    }
                    self.store_value(&key, value, expires_at);
                }
                Mutation::Delete { key } => {
                    self.remove(&key);
                }
                Mutation::Push { key, values, right } => {
                    if values.is_empty() {
                        return Err(invalid_journal());
                    }
                    let values = values
                        .into_iter()
                        .map(|value| self.payload(value))
                        .collect::<io::Result<Vec<_>>>()?;
                    let entry = self.collection_entry(&key, now, || Value::List(VecDeque::new()));
                    let Value::List(list) = entry else {
                        return Err(invalid_journal());
                    };
                    list.reserve(values.len());
                    if right {
                        list.extend(values);
                    } else {
                        for value in values {
                            list.push_front(value);
                        }
                    }
                }
                Mutation::Pop { key, count, right } => {
                    let count = usize::try_from(count)
                        .ok()
                        .filter(|count| *count > 0)
                        .ok_or_else(invalid_journal)?;
                    self.discard_expired(&key, now);
                    let Some(Entry {
                        value: Value::List(list),
                        ..
                    }) = self.entries.get_mut(&key)
                    else {
                        return Err(invalid_journal());
                    };
                    if count > list.len() {
                        return Err(invalid_journal());
                    }
                    if right {
                        list.truncate(list.len() - count);
                    } else {
                        list.drain(..count);
                    }
                    if list.is_empty() {
                        self.remove(&key);
                    }
                }
                Mutation::Expire { key, expires_at } => {
                    self.discard_expired(&key, now);
                    let Some(entry) = self.entries.get_mut(&key) else {
                        return Err(invalid_journal());
                    };
                    if (expires_at.is_none() && entry.expires_at.is_none())
                        || expires_at.is_some_and(|deadline| deadline <= now)
                    {
                        return Err(invalid_journal());
                    }
                    if let Some(previous) = entry.expires_at {
                        self.expirations.remove(&(previous, key.clone()));
                    }
                    entry.expires_at = expires_at;
                    if let Some(deadline) = expires_at {
                        self.expirations.insert((deadline, key));
                    }
                }
                Mutation::SetAdd { key, members } => self.replay_set_add(key, members, now)?,
                Mutation::SetRemove { key, members } => {
                    self.replay_set_remove(key, members, now)?
                }
                Mutation::HashSet { key, fields } => self.replay_hash_set(key, fields, now)?,
                Mutation::HashDelete { key, fields } => self.replay_hash_delete(key, fields)?,
                Mutation::SortedSetAdd { key, entries } => self.replay_zadd(key, entries, now)?,
                Mutation::SortedSetRemove { key, members } => {
                    self.replay_zremove(key, members, now)?
                }
                Mutation::Clear => self.clear_entries(),
                Mutation::Rename { from, to } => self.replay_rename(from, to, now)?,
                Mutation::ListRemove { key, value, count } => {
                    self.replay_list_remove(key, value, count, now)?
                }
                Mutation::ListSet { key, index, value } => {
                    self.replay_list_set(key, index, value, now)?
                }
                Mutation::Queue { key, effect } => self.replay_queue(key, effect, now)?,
                Mutation::Typed { key, kind, effect } => {
                    self.replay_typed(key, kind, effect, now)?
                }
                Mutation::ListInsert {
                    key, index, value, ..
                } => self.replay_list_insert(key, index, value, now)?,
                Mutation::SelectDatabase { database } => self.switch_database(database),
                Mutation::Search { effect } => self.search_apply_effect(&effect)?,
                Mutation::Functions { snapshot } => self.set_function_snapshot(snapshot),
                Mutation::NativePosition { history, offset } => {
                    self.set_native_position(history, offset)
                }
                Mutation::RdbAuxiliary { key, value } => {
                    Arc::make_mut(
                        self.rdb_auxiliary
                            .get_or_insert_with(|| Arc::new(BTreeMap::new())),
                    )
                    .insert(key, value);
                }
                Mutation::ResetGlobals => {
                    self.search = None;
                    self.function_libraries = Bytes::new();
                    self.native_position = None;
                    self.rdb_auxiliary = None;
                }
                Mutation::ClearNativePosition => self.native_position = None,
                Mutation::SwapDatabases { first, second } => {
                    self.swap_database_states(first, second)?
                }
            }
        }
        Ok(())
    }

    pub fn finish_recovery(&mut self) -> io::Result<()> {
        let active = self.active_database;
        let databases: Vec<_> = self.databases.keys().copied().collect();
        let access_now = unix_milliseconds();
        let expire_now = if self.native_position.is_some() {
            0
        } else {
            access_now
        };
        for database in std::iter::once(active).chain(databases) {
            self.switch_database(database);
            for entry in self.entries.values_mut() {
                if entry.access.last_access_ms == 0 {
                    entry.access = AccessMetadata::new(access_now);
                }
                if let Value::Queue(queue) = &entry.value {
                    queue.finish_recovery()?;
                }
            }
            self.expire_at(expire_now, usize::MAX);
            self.expire_hashes(expire_now, usize::MAX);
        }
        self.switch_database(active);
        Ok(())
    }

    fn record(&mut self, now: i64, mutation: impl FnOnce() -> Mutation) {
        self.record_change(now, true, mutation);
    }

    fn record_change(&mut self, now: i64, notify: bool, mutation: impl FnOnce() -> Mutation) {
        if !self.capturing_changes() {
            return;
        }
        let mutation = mutation();
        if notify && self.ready_tracking {
            match &mutation {
                Mutation::Rename { from, to } => {
                    self.ready_keys.push((self.active_database, from.clone()));
                    self.ready_keys.push((self.active_database, to.clone()));
                }
                Mutation::Clear
                | Mutation::SelectDatabase { .. }
                | Mutation::Search { .. }
                | Mutation::Functions { .. }
                | Mutation::NativePosition { .. }
                | Mutation::RdbAuxiliary { .. } => {}
                Mutation::ResetGlobals
                | Mutation::ClearNativePosition
                | Mutation::SwapDatabases { .. } => {}
                Mutation::Set { key, .. }
                | Mutation::Delete { key }
                | Mutation::Push { key, .. }
                | Mutation::Pop { key, .. }
                | Mutation::Expire { key, .. }
                | Mutation::SetAdd { key, .. }
                | Mutation::SetRemove { key, .. }
                | Mutation::HashSet { key, .. }
                | Mutation::HashDelete { key, .. }
                | Mutation::SortedSetAdd { key, .. }
                | Mutation::SortedSetRemove { key, .. }
                | Mutation::ListRemove { key, .. }
                | Mutation::ListSet { key, .. }
                | Mutation::ListInsert { key, .. }
                | Mutation::Queue { key, .. }
                | Mutation::Typed { key, .. } => {
                    self.ready_keys.push((self.active_database, key.clone()))
                }
            }
        }
        if self.journal_enabled {
            // WATCH is inspected only behind the admission write barrier. These
            // versions cannot be observed before commit; commit failure stops
            // the server. Keeping intent here also represents silent Redis
            // effects (e.g. expansion by a failed BITFIELD write) correctly.
            if notify
                && self.watch_notifications
                && let Some(watches) = &self.watches
            {
                watches.observe(self.active_database, &mutation);
            }
            if self.pending_batch.is_none() {
                self.journal_database = self.active_database;
                self.pending_batch = Some(Batch {
                    timestamp_ms: now,
                    mutations: vec![Mutation::SelectDatabase {
                        database: self.active_database,
                    }],
                });
            }
            let batch = self
                .pending_batch
                .as_mut()
                .expect("initialized journal batch");
            if self.journal_database != self.active_database {
                batch.mutations.push(Mutation::SelectDatabase {
                    database: self.active_database,
                });
                self.journal_database = self.active_database;
            }
            debug_assert_eq!(batch.timestamp_ms, now);
            batch.mutations.push(mutation);
        } else if self.search_reindexing
            && self
                .search
                .as_ref()
                .is_some_and(|catalog| catalog.has_indexes())
        {
            self.pending_search.push(mutation);
        }
    }

    /// Refresh once after a command, when every source mutation is complete.
    /// Mutation payloads are borrowed from the journal, never cloned for indexing.
    pub(crate) fn finish_search_changes(&mut self, now: i64) {
        if self
            .pending_batch
            .as_ref()
            .is_some_and(|batch| self.search_processed == batch.mutations.len())
            && self.pending_search.is_empty()
        {
            return;
        }
        if !self.search_reindexing
            || self
                .search
                .as_ref()
                .is_none_or(|catalog| !catalog.has_indexes())
        {
            self.pending_search.clear();
            return;
        }
        let mut batch = self.pending_batch.take();
        let mutations = if let Some(batch) = &mut batch {
            std::mem::take(&mut batch.mutations)
        } else {
            std::mem::take(&mut self.pending_search)
        };
        let previous_database = self.active_database;
        let mut effects = Vec::new();
        for (index, mutation) in mutations.iter().enumerate() {
            if let Mutation::SelectDatabase { database } = mutation {
                self.switch_database(*database);
                continue;
            }
            if index < self.search_processed
                || matches!(
                    mutation,
                    Mutation::Search { .. }
                        | Mutation::Functions { .. }
                        | Mutation::NativePosition { .. }
                        | Mutation::RdbAuxiliary { .. }
                )
            {
                continue;
            }
            effects.extend(self.search_after_mutation(mutation, now));
        }
        self.switch_database(previous_database);
        if let Some(mut batch) = batch {
            batch.mutations = mutations;
            batch.mutations.extend(
                effects
                    .into_iter()
                    .map(|effect| Mutation::Search { effect }),
            );
            self.search_processed = batch.mutations.len();
            self.pending_batch = Some(batch);
        } else {
            self.pending_search = mutations;
            self.pending_search.clear();
        }
    }

    pub fn execute(&mut self, args: &[Bytes]) -> io::Result<Execution> {
        self.execute_at_time(args, unix_milliseconds())
    }

    pub fn execute_at_time(&mut self, args: &[Bytes], now: i64) -> io::Result<Execution> {
        debug_assert!(self.failure.is_none(), "execution failures are fatal");
        let execution = self.execution_at(args, now);
        self.finish_search_changes(now);
        match self.failure.take() {
            Some(error) => Err(error),
            None => Ok(execution),
        }
    }

    pub(crate) fn retry_blocked(
        &mut self,
        keys: &[Bytes],
        destination: Option<&(Bytes, bool)>,
        right: bool,
        kind: BlockingKind,
    ) -> io::Result<Result<Option<Resp>, Resp>> {
        debug_assert!(
            self.pending_batch.is_none(),
            "drain the previous journal batch"
        );
        let now = unix_milliseconds();
        let result = (|| match kind {
            BlockingKind::MultiPop { count } => self.try_list_multipop_at(keys, right, count, now),
            BlockingKind::SortedPop { count, multi } => {
                self.try_sorted_pop_at(keys, right, count, multi, now)
            }
            BlockingKind::MoveMany {
                count,
                exactly,
                bulk,
            } => {
                let (target, target_right) = destination.expect("move destination");
                self.try_list_move_many_at(
                    &keys[0],
                    target,
                    right,
                    *target_right,
                    count,
                    exactly,
                    bulk,
                    now,
                )
            }
            BlockingKind::ListPop => {
                for key in keys {
                    let value = if let Some((target, target_right)) = destination {
                        self.try_move_at(key, target, right, *target_right, now)?
                    } else {
                        self.try_pop_at(key, right, now)?
                    };
                    if let Some(value) = value {
                        return Ok(Some(if destination.is_some() {
                            Resp::Bulk(Some(value))
                        } else {
                            array(vec![Resp::Bulk(Some(key.clone())), Resp::Bulk(Some(value))])
                        }));
                    }
                }
                Ok(None)
            }
        })();
        match self.failure.take() {
            Some(error) => Err(error),
            None => Ok(result),
        }
    }

    pub fn list_ready(&mut self, key: &Bytes) -> bool {
        matches!(
            self.value(key, unix_milliseconds()),
            Some(Value::List(list)) if !list.is_empty()
        )
    }

    pub fn expire_some(&mut self) {
        self.expire_at(unix_milliseconds(), ACTIVE_EXPIRATION_BATCH);
        self.expire_hashes(unix_milliseconds(), ACTIVE_EXPIRATION_BATCH);
    }

    #[cfg(test)]
    fn execute_at(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.execution_at(args, now) {
            Execution::Reply(reply) | Execution::Pushed { reply, .. } => reply,
            Execution::Block(_) | Execution::StreamBlock(_) => {
                panic!("blocking commands require the execution interface")
            }
        }
    }

    fn execution_at(&mut self, args: &[Bytes], now: i64) -> Execution {
        self.prepare_replay_args(args);
        let execution = self.execution_inner(args, now);
        self.finish_search_changes(now);
        self.finish_access_metadata(args, now);
        execution
    }

    fn finish_access_metadata(&mut self, args: &[Bytes], now: i64) {
        if args.first().is_some_and(|name| {
            [b"OBJECT".as_slice(), b"RESTORE", b"RESTORE-ASKING"]
                .iter()
                .any(|skip| name.eq_ignore_ascii_case(skip))
        }) {
            return;
        }
        if let Some(spec) = effective_spec(args)
            && let Ok(Some(positions)) = spec.key_positions(args)
        {
            for position in positions {
                self.touch_key_access(&args[position], now);
            }
        }
    }

    fn touch_key_access(&mut self, key: &Bytes, now: i64) {
        if self
            .command_session
            .as_ref()
            .is_some_and(|session| session.no_touch.load(std::sync::atomic::Ordering::Acquire))
        {
            return;
        }
        let now = if self.replication_replay {
            unix_milliseconds()
        } else {
            now
        };
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.access.last_access_ms == 0 {
                entry.access = AccessMetadata::new(now);
            } else {
                entry.access.last_access_ms = now;
                entry.access.frequency = entry.access.frequency.saturating_add(1);
            }
        }
    }

    fn execution_inner(&mut self, args: &[Bytes], now: i64) -> Execution {
        debug_assert!(
            self.pending_batch.is_none(),
            "drain the previous journal batch"
        );
        if let Err(reply) = Self::validate(args) {
            return Execution::Reply(reply);
        }
        let spec = root_spec(args).expect("validated command root");
        self.set_command_name(args);
        if spec.name == "ts.read" {
            return self.timeseries_read(args, now, false);
        }
        if matches!(spec.name, "xread" | "xreadgroup") {
            return self.stream_read(args, now);
        }
        match spec.command {
            Command::Execution => {
                (spec
                    .execution_handler
                    .expect("registered execution handler"))(self, args, now)
            }
            Command::Blpop | Command::Brpop => {
                self.blocking_pop(args, spec.command == Command::Brpop, now)
            }
            Command::Lmove | Command::Blmove => {
                self.list_move(args, spec.command == Command::Blmove, now)
            }
            Command::Lpush | Command::Rpush | Command::Lpushx | Command::Rpushx => {
                let right = matches!(spec.command, Command::Rpush | Command::Rpushx);
                let reply = if matches!(spec.command, Command::Lpushx | Command::Rpushx) {
                    self.discard_expired(&args[1], now);
                    if self.entries.contains_key(&args[1]) {
                        self.push(args, right, now)
                    } else {
                        Resp::Integer(0)
                    }
                } else {
                    self.push(args, right, now)
                };
                if matches!(&reply, Resp::Integer(length) if *length > 0) {
                    Execution::Pushed {
                        reply,
                        key: args[1].clone(),
                    }
                } else {
                    Execution::Reply(reply)
                }
            }
            Command::Rename | Command::Renamenx => {
                self.rename(args, now, spec.command == Command::Renamenx)
            }
            _ => Execution::Reply(self.dispatch(args, spec, now)),
        }
    }

    fn dispatch(&mut self, args: &[Bytes], spec: &CommandSpec, now: i64) -> Resp {
        match spec.command {
            Command::Ping => match args.get(1) {
                Some(message) => Resp::Bulk(Some(message.clone())),
                None => Resp::Simple("PONG"),
            },
            Command::Echo => Resp::Bulk(Some(args[1].clone())),
            Command::Quit => Resp::Simple("OK"),
            Command::Get => match self.value(&args[1], now) {
                Some(Value::String(value)) => Resp::Bulk(Some(value.clone())),
                Some(_) => wrong_type(),
                None => Resp::Bulk(None),
            },
            Command::Set => self.set(args, now),
            Command::Del => {
                let mut count = 0;
                for key in &args[1..] {
                    self.discard_expired(key, now);
                    if self.remove(key).is_some() {
                        count += 1;
                        self.record(now, || Mutation::Delete { key: key.clone() });
                    }
                }
                Resp::Integer(count)
            }
            Command::Exists => {
                let mut count = 0;
                for key in &args[1..] {
                    self.discard_expired(key, now);
                    count += i64::from(self.entries.contains_key(key));
                }
                Resp::Integer(count)
            }
            Command::Mget => {
                let limit = self.reply_limit;
                bounded_replies(
                    args[1..].iter().map(|key| {
                        Resp::Bulk(match self.value(key, now) {
                            Some(Value::String(value)) => Some(value.clone()),
                            _ => None,
                        })
                    }),
                    limit,
                )
            }
            Command::Mset => {
                // The registry validates the entire key/value shape before any write.
                for pair in args[1..].as_chunks::<2>().0 {
                    self.store(&pair[0], pair[1].clone(), None, now);
                }
                Resp::Simple("OK")
            }
            Command::Lpop | Command::Rpop => self.pop(args, spec.command == Command::Rpop, now),
            Command::Lrange => self.list_range(args, now),
            Command::Llen => match self.value(&args[1], now) {
                Some(Value::List(list)) => Resp::Integer(list.len() as i64),
                Some(_) => wrong_type(),
                None => Resp::Integer(0),
            },
            Command::Incr | Command::Incrby | Command::Decr | Command::Decrby => {
                let delta = match spec.command {
                    Command::Incr => 1,
                    Command::Decr => -1,
                    Command::Incrby => match integer(&args[2]) {
                        Some(value) => value,
                        None => return integer_error(),
                    },
                    Command::Decrby => match integer(&args[2]) {
                        Some(i64::MIN) => return error("ERR decrement would overflow"),
                        Some(value) => -value,
                        None => return integer_error(),
                    },
                    _ => unreachable!(),
                };
                self.increment(&args[1], delta, now)
            }
            Command::Expire | Command::Pexpire | Command::Expireat | Command::Pexpireat => {
                let kind = match spec.command {
                    Command::Expire => ExpiryKind::Ex,
                    Command::Pexpire => ExpiryKind::Px,
                    Command::Expireat => ExpiryKind::Exat,
                    Command::Pexpireat => ExpiryKind::Pxat,
                    _ => unreachable!(),
                };
                self.expire(args, kind, now, spec.name)
            }
            Command::Ttl | Command::Pttl | Command::Expiretime | Command::Pexpiretime => {
                let key = &args[1];
                self.discard_expired(key, now);
                let ttl = match self.entries.get(key) {
                    None => -2,
                    Some(Entry {
                        expires_at: None, ..
                    }) => -1,
                    Some(Entry {
                        expires_at: Some(deadline),
                        ..
                    }) => {
                        match spec.command {
                            Command::Pexpiretime => *deadline,
                            Command::Expiretime => deadline / 1000,
                            Command::Pttl => deadline - now,
                            _ => {
                                let remaining = deadline - now;
                                // Redis rounds remaining TTL, not absolute time.
                                remaining / 1000 + i64::from(remaining % 1000 >= 500)
                            }
                        }
                    }
                };
                Resp::Integer(ttl)
            }
            Command::Persist => {
                let key = &args[1];
                self.discard_expired(key, now);
                let Some(entry) = self.entries.get_mut(key) else {
                    return Resp::Integer(0);
                };
                let Some(deadline) = entry.expires_at.take() else {
                    return Resp::Integer(0);
                };
                self.expirations.remove(&(deadline, key.clone()));
                self.record(now, || Mutation::Expire {
                    key: key.clone(),
                    expires_at: None,
                });
                Resp::Integer(1)
            }
            Command::Type => {
                self.discard_expired(&args[1], now);
                Resp::Simple(match self.entries.get(&args[1]).map(|entry| &entry.value) {
                    Some(Value::String(_)) => "string",
                    Some(Value::List(_)) => "list",
                    Some(Value::Set(_)) => "set",
                    Some(Value::Hash(_)) => "hash",
                    Some(Value::SortedSet(_)) => "zset",
                    Some(Value::Queue(_)) => "ffkv-queue",
                    Some(Value::Typed(value)) => value.type_name(),
                    None => "none",
                })
            }
            Command::Dbsize => {
                self.expire_at(now, usize::MAX);
                self.expire_hashes(now, usize::MAX);
                Resp::Integer(self.entries.len() as i64)
            }
            Command::Select => match integer(&args[1]) {
                Some(database) if database >= 0 && database < i64::from(self.database_count()) => {
                    let database = database as u32;
                    self.switch_database(database);
                    if let Some(session) = &self.command_session {
                        session.set_database(database);
                    }
                    Resp::Simple("OK")
                }
                Some(_) => error("ERR DB index is out of range"),
                None => integer_error(),
            },
            Command::Catalog => self.command(args),
            Command::Connection | Command::Publish | Command::Scripting | Command::Maintenance => {
                error("ERR command requires its execution context")
            }
            Command::Extension => {
                (spec.handler.expect("registered command handler"))(self, args, now)
            }
            Command::Lpush
            | Command::Rpush
            | Command::Blpop
            | Command::Brpop
            | Command::Lmove
            | Command::Blmove
            | Command::CatalogInfo
            | Command::CatalogGetkeys
            | Command::Rename
            | Command::Renamenx
            | Command::Lpushx
            | Command::Rpushx
            | Command::Execution
            | Command::CatalogCount => unreachable!(),
        }
    }

    fn command(&self, args: &[Bytes]) -> Resp {
        catalog::dispatch(self, args)
    }

    fn push(&mut self, args: &[Bytes], right: bool, now: i64) -> Resp {
        let key = &args[1];
        let storage = self.storage.clone();
        let value = self.collection_entry(key, now, || Value::List(VecDeque::new()));
        let Value::List(list) = value else {
            return wrong_type();
        };
        list.reserve(args.len() - 2);
        for value in &args[2..] {
            let value = match &storage {
                Some(store) => match store.put(value.clone()) {
                    Ok(value) => value,
                    Err(error) => return self.fail_io(error),
                },
                None => Payload::Inline(value.clone()),
            };
            if right {
                list.push_back(value);
            } else {
                list.push_front(value);
            }
        }
        let length = list.len() as i64;
        self.record(now, || Mutation::Push {
            key: key.clone(),
            values: args[2..].to_vec(),
            right,
        });
        Resp::Integer(length)
    }

    fn pop(&mut self, args: &[Bytes], right: bool, now: i64) -> Resp {
        let key = &args[1];
        let Some(count) = args.get(2) else {
            return match self.try_pop_at(key, right, now) {
                Ok(value) => Resp::Bulk(value),
                Err(reply) => reply,
            };
        };
        let count = match integer(count) {
            Some(value) if value >= 0 => usize::try_from(value).unwrap_or(usize::MAX),
            _ => return error("ERR value is out of range, must be positive"),
        };
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return Resp::Array(None);
        };
        let Value::List(list) = &mut entry.value else {
            return wrong_type();
        };
        let count = count.min(list.len());
        let values = if right {
            payload_replies(list.iter().rev().take(count), self.reply_limit)
        } else {
            payload_replies(list.iter().take(count), self.reply_limit)
        };
        let values = match values {
            Ok(values) => values,
            Err(error) => return self.fail_payload_reply(error),
        };
        if right {
            list.truncate(list.len() - count);
        } else {
            list.drain(..count);
        }
        if list.is_empty() {
            self.remove(key);
        }
        if count > 0 {
            self.record(now, || Mutation::Pop {
                key: key.clone(),
                count: count as u64,
                right,
            });
        }
        array(values)
    }

    fn try_pop_at(&mut self, key: &Bytes, right: bool, now: i64) -> Result<Option<Bytes>, Resp> {
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return Ok(None);
        };
        let Value::List(list) = &mut entry.value else {
            return Err(wrong_type());
        };
        let value = if right { list.back() } else { list.front() }
            .map(Payload::read)
            .transpose();
        let value = match value {
            Ok(value) => value,
            Err(error) => return Err(self.fail_io(error)),
        };
        if right {
            list.pop_back();
        } else {
            list.pop_front();
        }
        if list.is_empty() {
            self.remove(key);
        }
        if value.is_some() {
            self.record(now, || Mutation::Pop {
                key: key.clone(),
                count: 1,
                right,
            });
        }
        Ok(value)
    }

    fn list_range(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let (Some(start), Some(stop)) = (integer(&args[2]), integer(&args[3])) else {
            return integer_error();
        };
        let list = match self.value(&args[1], now) {
            Some(Value::List(list)) => list,
            Some(_) => return wrong_type(),
            None => return array(Vec::new()),
        };
        // Wider arithmetic keeps i64::MIN and i64::MAX bounds well-defined.
        let length = list.len() as i128;
        let start = if start < 0 {
            length + i128::from(start)
        } else {
            i128::from(start)
        };
        let stop = if stop < 0 {
            length + i128::from(stop)
        } else {
            i128::from(stop)
        };
        let start = start.max(0);
        let stop = stop.min(length - 1);
        if start > stop {
            return array(Vec::new());
        }
        match payload_replies(list.range(start as usize..=stop as usize), limit) {
            Ok(values) => array(values),
            Err(error) => self.fail_payload_reply(error),
        }
    }

    fn blocking_pop(&mut self, args: &[Bytes], right: bool, now: i64) -> Execution {
        let timeout = match blocking_timeout(&args[args.len() - 1], now) {
            Ok(timeout) => timeout,
            Err(reply) => return Execution::Reply(reply),
        };
        let keys = &args[1..args.len() - 1];
        // Key order is significant: an available earlier list wins even if a
        // later key has another type. Validate the timeout before any pop.
        for key in keys {
            match self.try_pop_at(key, right, now) {
                Ok(Some(value)) => {
                    return Execution::Reply(array(vec![
                        Resp::Bulk(Some(key.clone())),
                        Resp::Bulk(Some(value)),
                    ]));
                }
                Ok(None) => {}
                Err(reply) => return Execution::Reply(reply),
            }
        }
        Execution::Block(BlockingPop {
            keys: keys.to_vec(),
            right,
            timeout,
            destination: None,
            kind: BlockingKind::ListPop,
        })
    }

    fn set(&mut self, args: &[Bytes], now: i64) -> Resp {
        let options = match string_compat::SetOptions::parse(args, now) {
            Ok(options) => options,
            Err(reply) => return reply,
        };
        let key = &args[1];
        self.discard_expired(key, now);
        let existing = self.entries.get(key);
        let previous = if options.get {
            match existing.map(|entry| &entry.value) {
                Some(Value::String(value)) => Some(value.clone()),
                Some(_) => return wrong_type(),
                None => None,
            }
        } else {
            None
        };
        match options
            .condition
            .matches(existing.map(|entry| &entry.value))
        {
            Ok(true) => {}
            Ok(false) => return Resp::Bulk(previous),
            Err(reply) => return reply,
        }
        let deadline = if options.keep_ttl {
            existing.and_then(|entry| entry.expires_at)
        } else {
            options.deadline
        };
        self.store(key, args[2].clone(), deadline, now);
        if options.get {
            Resp::Bulk(previous)
        } else {
            Resp::Simple("OK")
        }
    }

    fn increment(&mut self, key: &Bytes, delta: i64, now: i64) -> Resp {
        self.discard_expired(key, now);
        let (old, deadline) = match self.entries.get(key) {
            Some(Entry {
                value: Value::String(value),
                expires_at,
                ..
            }) => match integer(value) {
                Some(value) => (value, *expires_at),
                None => return integer_error(),
            },
            Some(_) => return wrong_type(),
            None => (0, None),
        };
        let Some(value) = old.checked_add(delta) else {
            return error("ERR increment or decrement would overflow");
        };
        self.store(key, Bytes::from(value.to_string()), deadline, now);
        Resp::Integer(value)
    }

    fn expire(&mut self, args: &[Bytes], kind: ExpiryKind, now: i64, name: &'static str) -> Resp {
        let (mut nx, mut xx, mut gt, mut lt) = (false, false, false, false);
        for option in &args[3..] {
            if option.eq_ignore_ascii_case(b"NX") {
                nx = true;
            } else if option.eq_ignore_ascii_case(b"XX") {
                xx = true;
            } else if option.eq_ignore_ascii_case(b"GT") {
                gt = true;
            } else if option.eq_ignore_ascii_case(b"LT") {
                lt = true;
            } else {
                return error("ERR unsupported expiration option");
            }
        }
        if nx && (xx || gt || lt) {
            return error("ERR NX and XX, GT or LT options at the same time are not compatible");
        }
        if gt && lt {
            return error("ERR GT and LT options at the same time are not compatible");
        }
        let Some(value) = integer(&args[2]) else {
            return integer_error();
        };
        let Some(deadline) = kind.deadline(value, now) else {
            return invalid_expiry(name);
        };
        let key = &args[1];
        self.discard_expired(key, now);
        let Some(entry) = self.entries.get_mut(key) else {
            return Resp::Integer(0);
        };
        let previous = entry.expires_at;
        if (nx && previous.is_some())
            || (xx && previous.is_none())
            || (gt && previous.is_none_or(|previous| deadline <= previous))
            || (lt && previous.is_some_and(|previous| deadline >= previous))
        {
            return Resp::Integer(0);
        }
        if deadline <= now {
            self.remove(key);
            self.record(now, || Mutation::Delete { key: key.clone() });
        } else {
            if let Some(previous) = previous {
                self.expirations.remove(&(previous, key.clone()));
            }
            entry.expires_at = Some(deadline);
            self.expirations.insert((deadline, key.clone()));
            self.record(now, || Mutation::Expire {
                key: key.clone(),
                expires_at: Some(deadline),
            });
        }
        Resp::Integer(1)
    }

    fn value(&mut self, key: &Bytes, now: i64) -> Option<&Value> {
        self.discard_expired(key, now);
        self.entries.get(key).map(|entry| &entry.value)
    }

    fn store(&mut self, key: &Bytes, value: Bytes, deadline: Option<i64>, now: i64) {
        self.resolve_held_write(key, now);
        if deadline.is_some_and(|deadline| deadline <= now) {
            if self.remove(key).is_some() {
                self.record(now, || Mutation::Delete { key: key.clone() });
            }
            return;
        }
        self.store_value(key, value.clone(), deadline);
        self.record(now, || Mutation::Set {
            key: key.clone(),
            value: value.clone(),
            expires_at: deadline,
        });
    }

    fn store_value(&mut self, key: &Bytes, value: Bytes, deadline: Option<i64>) {
        self.held_entries.remove(key);
        self.retained_hashes.remove(key);
        if let Some(deadline) = self.hash_deadline(key) {
            self.hash_expirations.remove(&(deadline, key.clone()));
        }
        if let Some(deadline) = self.queue_deadline(key) {
            self.queue_expirations.remove(&(deadline, key.clone()));
        }
        if let Some(entry) = self.entries.get_mut(key) {
            if entry.expires_at != deadline {
                if let Some(previous) = entry.expires_at {
                    self.expirations.remove(&(previous, key.clone()));
                }
                if let Some(deadline) = deadline {
                    self.expirations.insert((deadline, key.clone()));
                }
            }
            entry.value = Value::String(value);
            entry.expires_at = deadline;
        } else {
            self.insert_entry_raw(
                self.own_key(key),
                Entry {
                    value: Value::String(value),
                    expires_at: deadline,
                    access: crate::engine::AccessMetadata::default(),
                },
            );
        }
    }

    fn remove(&mut self, key: &Bytes) -> Option<Entry> {
        self.detach_entry(key).map(|(_, entry)| entry)
    }

    fn detach_entry(&mut self, key: &Bytes) -> Option<(Bytes, Entry)> {
        let (owned_key, entry) = self.entries.remove_entry(key)?;
        self.retained_hashes.remove(key);
        self.key_index.remove(&(stable_hash(key), key.clone()));
        if let Value::Hash(hash) = &entry.value
            && let Some(deadline) = hash.next_expiry()
        {
            self.hash_expirations.remove(&(deadline, key.clone()));
        }
        if let Value::Queue(queue) = &entry.value
            && let Some(deadline) = queue.next_deadline()
        {
            self.queue_expirations
                .remove(&(deadline, owned_key.clone()));
        }
        if let Some(deadline) = entry.expires_at {
            self.expirations.remove(&(deadline, key.clone()));
        }
        Some((owned_key, entry))
    }

    fn discard_expired(&mut self, key: &Bytes, now: i64) {
        if self.replication_replay {
            return;
        }
        self.release_unheld_key(key, now);
        if self
            .entries
            .get(key)
            .and_then(|entry| entry.expires_at)
            .is_some_and(|deadline| deadline <= now)
        {
            if self.holding_expiry(key) {
                if let Some((key, entry)) = self.detach_entry(key) {
                    self.held_entries.insert(key, entry);
                }
            } else if self.remove(key).is_some() {
                self.record(now, || Mutation::Delete { key: key.clone() });
            }
            return;
        }
        self.prune_hash_fields(key, now, usize::MAX);
    }

    fn expire_at(&mut self, now: i64, limit: usize) {
        if self.replication_replay {
            return;
        }
        self.expire_held(now, limit);
        for _ in 0..limit {
            let Some((deadline, key)) = self.expirations.first() else {
                break;
            };
            if *deadline > now {
                break;
            }
            let key = key.clone();
            self.discard_expired(&key, now);
        }
    }

    fn collection_entry(
        &mut self,
        key: &Bytes,
        now: i64,
        create: impl FnOnce() -> Value,
    ) -> &mut Value {
        self.discard_expired(key, now);
        let owned = match self.entries.get_key_value(key) {
            Some((owned, _)) => owned.clone(),
            None => {
                if !self.replication_replay {
                    self.resolve_held_write(key, now);
                }
                self.own_key(key)
            }
        };
        match self.entries.entry(owned) {
            std::collections::hash_map::Entry::Occupied(entry) => &mut entry.into_mut().value,
            std::collections::hash_map::Entry::Vacant(entry) => {
                self.key_index
                    .insert((stable_hash(entry.key()), entry.key().clone()));
                &mut entry
                    .insert(Entry {
                        value: create(),
                        expires_at: None,
                        access: crate::engine::AccessMetadata::default(),
                    })
                    .value
            }
        }
    }

    fn hash_deadline(&self, key: &Bytes) -> Option<i64> {
        match self.entries.get(key).map(|entry| &entry.value) {
            Some(Value::Hash(hash)) => hash.next_expiry(),
            _ => None,
        }
    }

    fn refresh_hash_deadline(&mut self, key: &Bytes, previous: Option<i64>) {
        if let Some(deadline) = previous {
            self.hash_expirations.remove(&(deadline, key.clone()));
        }
        if let Some(deadline) = self.hash_deadline(key) {
            self.hash_expirations.insert((deadline, key.clone()));
        }
    }

    /// Install a validated semantic write, resolving held state before partial effects.
    fn write_entry(&mut self, key: Bytes, entry: Entry, now: i64) {
        self.resolve_held_write(&key, now);
        self.insert_entry_raw(key, entry);
    }

    /// Structural transfer and resolved replay only; never invent durable effects.
    fn insert_entry_raw(&mut self, key: Bytes, entry: Entry) {
        self.held_entries.remove(&key);
        self.retained_hashes.remove(&key);
        if matches!(&entry.value, Value::Hash(hash) if hash.has_retained_fields()) {
            self.retained_hashes.insert(key.clone());
        }
        if let Some(deadline) = entry.expires_at {
            self.expirations.insert((deadline, key.clone()));
        }
        if let Value::Hash(hash) = &entry.value
            && let Some(deadline) = hash.next_expiry()
        {
            self.hash_expirations.insert((deadline, key.clone()));
        }
        if let Value::Queue(queue) = &entry.value
            && let Some(deadline) = queue.next_deadline()
        {
            self.queue_expirations.insert((deadline, key.clone()));
        }
        self.key_index.insert((stable_hash(&key), key.clone()));
        self.entries.insert(key, entry);
    }

    fn clear_entries(&mut self) {
        drop(self.take_database());
    }
}

fn invalid_journal() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid journal mutation")
}

fn unix_milliseconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn blocking_timeout(value: &[u8], now: i64) -> Result<Option<Duration>, Resp> {
    let seconds = std::str::from_utf8(value)
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| !value.is_nan())
        .ok_or_else(|| error("ERR timeout is not a float or out of range"))?;
    if seconds < 0.0 {
        return Err(error("ERR timeout is negative"));
    }
    if !seconds.is_finite() {
        return Err(error("ERR timeout is out of range"));
    }
    if seconds == 0.0 {
        // Reject decimal underflow instead of treating a nonzero timeout as
        // the infinite-wait zero sentinel.
        let nonzero = value
            .iter()
            .take_while(|&&byte| !matches!(byte, b'e' | b'E'))
            .any(|&byte| matches!(byte, b'1'..=b'9'));
        if nonzero {
            return Err(error("ERR timeout is not a float or out of range"));
        }
        return Ok(None);
    }
    let milliseconds = (seconds * 1000.0).ceil().max(1.0);
    // The floating representation of i64::MAX rounds to 2^63, which is
    // already out of range. Check before Rust's saturating float cast.
    if milliseconds >= i64::MAX as f64 {
        return Err(error("ERR timeout is out of range"));
    }
    let milliseconds = milliseconds as i64;
    if now.checked_add(milliseconds).is_none() {
        return Err(error("ERR timeout is out of range"));
    }
    Ok(Some(Duration::from_millis(milliseconds as u64)))
}

// Match Redis's canonical signed-decimal representation, not Rust's more
// permissive parser: no '+', whitespace, leading zeroes, or negative zero.
fn integer(value: &[u8]) -> Option<i64> {
    if value == b"0" {
        return Some(0);
    }
    let (negative, digits) = match value.first()? {
        b'-' => (true, &value[1..]),
        _ => (false, value),
    };
    if !matches!(digits.first(), Some(b'1'..=b'9')) {
        return None;
    }
    // Accumulate negatively to allow i64::MIN without an unsigned conversion.
    let mut result = 0_i64;
    for digit in digits {
        if !digit.is_ascii_digit() {
            return None;
        }
        result = result
            .checked_mul(10)?
            .checked_sub(i64::from(digit - b'0'))?;
    }
    if negative {
        Some(result)
    } else {
        result.checked_neg()
    }
}

fn bulk(value: &'static str) -> Resp {
    Resp::Bulk(Some(Bytes::from_static(value.as_bytes())))
}

fn array(values: Vec<Resp>) -> Resp {
    Resp::Array(Some(values))
}

fn strings(values: &[&'static str]) -> Resp {
    array(values.iter().map(|value| bulk(value)).collect())
}

fn error(message: &'static str) -> Resp {
    Resp::Error(message.to_owned())
}

fn integer_error() -> Resp {
    error("ERR value is not an integer or out of range")
}

fn wrong_type() -> Resp {
    error("WRONGTYPE Operation against a key holding the wrong kind of value")
}

fn syntax_error() -> Resp {
    error("ERR syntax error")
}

fn wrong_arity(name: &str) -> Resp {
    Resp::Error(format!(
        "ERR wrong number of arguments for '{name}' command"
    ))
}

fn invalid_expiry(name: &'static str) -> Resp {
    Resp::Error(format!("ERR invalid expire time in '{name}' command"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn replay_pending(source: &mut Engine, recovered: &mut Engine) {
        let batch = source.take_batch().expect("expected a durable change");
        assert!(source.take_batch().is_none(), "one batch per execution");
        recovered.replay(batch).unwrap();
        assert!(recovered.take_batch().is_none(), "replay must not record");
    }

    fn persist(source: &mut Engine, recovered: &mut Engine, now: i64, args: &[&str]) -> Resp {
        let reply = run(source, now, args);
        replay_pending(source, recovered);
        reply
    }

    fn journaled_engine() -> Engine {
        let mut engine = Engine::new();
        engine.set_recording(true);
        engine
    }

    fn replay_held_expiry_batch(batch: Batch, recovered: &mut Engine, follower: &mut Engine) {
        let encoded = crate::wal::encode_native_batch(&batch).unwrap();
        recovered
            .replay(crate::wal::decode_native_batch(encoded).unwrap())
            .unwrap();
        let mut native = batch;
        native.timestamp_ms = 0;
        follower.set_replication_replay(true);
        follower.replay(native).unwrap();
        follower.set_replication_replay(false);
    }

    #[test]
    fn held_expiry_recreation_preserves_collections_through_wal_and_native_replay() {
        for database in [0, 3] {
            for (create, recreate, read) in [
                (
                    vec!["RPUSH", "k", "old"],
                    vec!["LPUSH", "k", "new"],
                    vec!["LRANGE", "k", "0", "-1"],
                ),
                (
                    vec!["HSET", "k", "old", "old"],
                    vec!["HSET", "k", "new", "new"],
                    vec!["HGETALL", "k"],
                ),
                (
                    vec!["SADD", "k", "old"],
                    vec!["SADD", "k", "new"],
                    vec!["SMEMBERS", "k"],
                ),
                (
                    vec!["ZADD", "k", "1", "old"],
                    vec!["ZADD", "k", "2", "new"],
                    vec!["ZRANGE", "k", "0", "-1"],
                ),
                (
                    vec!["BF.ADD", "k", "old"],
                    vec!["BF.ADD", "k", "new"],
                    vec!["BF.MEXISTS", "k", "old", "new"],
                ),
                (
                    vec!["JSON.SET", "k", "$", "{\"old\":1}"],
                    vec!["JSON.SET", "k", "$", "{\"new\":2}"],
                    vec!["JSON.GET", "k"],
                ),
                (
                    vec!["XADD", "k", "1-0", "f", "old"],
                    vec!["XADD", "k", "2-0", "f", "new"],
                    vec!["XRANGE", "k", "-", "+"],
                ),
                (
                    vec!["TS.ADD", "k", "1", "10"],
                    vec!["TS.ADD", "k", "2", "20"],
                    vec!["TS.RANGE", "k", "-", "+"],
                ),
            ] {
                let mut source = journaled_engine();
                let mut recovered = Engine::new();
                let mut follower = Engine::new();
                follower.set_replica_mode(true);
                source.switch_database(database);
                source.set_migration_expiry_ranges(Some(&[(0, 16383)]));
                run(&mut source, 1000, &create);
                replay_held_expiry_batch(
                    source.take_batch().unwrap(),
                    &mut recovered,
                    &mut follower,
                );
                run(&mut source, 1000, &["PEXPIRE", "k", "10"]);
                replay_held_expiry_batch(
                    source.take_batch().unwrap(),
                    &mut recovered,
                    &mut follower,
                );
                assert_integer(run(&mut source, 1010, &["EXISTS", "k"]), 0);
                assert!(
                    source.take_batch().is_none(),
                    "held expiry belongs to the source history"
                );
                run(&mut source, 1011, &recreate);
                replay_held_expiry_batch(
                    source.take_batch().unwrap(),
                    &mut recovered,
                    &mut follower,
                );
                if let Some(batch) = source.finish_migration_expiry(1012).unwrap() {
                    replay_held_expiry_batch(batch, &mut recovered, &mut follower);
                }
                let expected = run(&mut source, 1012, &read);
                assert_integer(run(&mut source, 1012, &["PTTL", "k"]), -1);
                for engine in [&mut recovered, &mut follower] {
                    engine.switch_database(database);
                    assert_eq!(
                        run(engine, 1012, &read),
                        expected,
                        "recreated collection diverged: {recreate:?}, DB {database}"
                    );
                    assert_integer(run(engine, 1012, &["PTTL", "k"]), -1);
                }
            }
        }
    }

    #[test]
    fn held_expiry_hash_rewrite_survives_release_and_raw_snapshot() {
        let mut source = journaled_engine();
        let mut recovered = Engine::new();
        let mut follower = Engine::new();
        follower.set_replica_mode(true);
        source.set_migration_expiry_ranges(Some(&[(0, 16383)]));
        for args in [
            vec!["HSET", "h", "keep", "sibling", "f", "old"],
            vec!["HPEXPIRE", "h", "10", "FIELDS", "1", "f"],
        ] {
            run(&mut source, 1000, &args);
            replay_held_expiry_batch(source.take_batch().unwrap(), &mut recovered, &mut follower);
        }
        assert_bulk(run(&mut source, 1010, &["HGET", "h", "f"]), None);
        assert!(source.take_batch().is_none());
        run(&mut source, 1011, &["HSET", "h", "f", "new"]);
        replay_held_expiry_batch(source.take_batch().unwrap(), &mut recovered, &mut follower);
        if let Some(batch) = source.finish_migration_expiry(1012).unwrap() {
            replay_held_expiry_batch(batch, &mut recovered, &mut follower);
        }
        for engine in [&mut source, &mut recovered, &mut follower] {
            assert_bulk(run(engine, 1012, &["HGET", "h", "f"]), Some(b"new"));
            assert_integer(run(engine, 1012, &["PTTL", "h"]), -1);
            assert_eq!(
                run(engine, 1012, &["HPTTL", "h", "FIELDS", "1", "f"]),
                array(vec![Resp::Integer(-1)]),
            );
        }
    }

    #[test]
    fn held_expiry_recreation_preserves_transfers_and_cluster_imports() {
        for operation in ["RESTORE", "IMPORT", "NATIVE_IMPORT", "COPY", "MOVE"] {
            let mut source = journaled_engine();
            let mut recovered = Engine::new();
            let mut follower = Engine::new();
            follower.set_replica_mode(true);
            source.set_migration_expiry_ranges(Some(&[(0, 16383)]));
            source.switch_database(3);
            for args in [vec!["RPUSH", "k", "old"], vec!["PEXPIRE", "k", "10"]] {
                run(&mut source, 1000, &args);
                replay_held_expiry_batch(
                    source.take_batch().unwrap(),
                    &mut recovered,
                    &mut follower,
                );
            }
            source.switch_database(1);
            let template = if operation == "MOVE" { "k" } else { "template" };
            for args in [
                vec!["RPUSH", template, "new"],
                vec!["PEXPIRE", template, "80"],
            ] {
                run(&mut source, 1000, &args);
                replay_held_expiry_batch(
                    source.take_batch().unwrap(),
                    &mut recovered,
                    &mut follower,
                );
            }
            let key = Bytes::from_static(b"k");
            let template_key = Bytes::copy_from_slice(template.as_bytes());
            let dump = source.dump_key(&template_key, 1000).unwrap().unwrap();
            let export = source.export_key(&template_key, 1000).unwrap().unwrap();
            source.switch_database(3);
            assert_integer(run(&mut source, 1010, &["EXISTS", "k"]), 0);
            assert!(source.take_batch().is_none());
            match operation {
                "RESTORE" => assert_status(
                    source.restore_key(&key, &dump, Some(1080), false, 1011),
                    "OK",
                ),
                "IMPORT" => assert_status(source.import_key(&key, &export, 1011), "OK"),
                "NATIVE_IMPORT" => {
                    source.set_replication_replay(true);
                    assert_status(source.import_key(&key, &export, 0), "OK");
                    source.set_replication_replay(false);
                }
                "COPY" | "MOVE" => {
                    source.switch_database(1);
                    let args = if operation == "COPY" {
                        vec!["COPY", "template", "k", "DB", "3"]
                    } else {
                        vec!["MOVE", "k", "3"]
                    };
                    assert_integer(run(&mut source, 1011, &args), 1);
                    assert_eq!(
                        source.database(),
                        1,
                        "transfers must restore their command database"
                    );
                }
                _ => unreachable!(),
            }
            replay_held_expiry_batch(source.take_batch().unwrap(), &mut recovered, &mut follower);
            if let Some(batch) = source.finish_migration_expiry(1012).unwrap() {
                replay_held_expiry_batch(batch, &mut recovered, &mut follower);
            }
            for engine in [&mut source, &mut recovered, &mut follower] {
                engine.switch_database(3);
                assert_bulk_array(run(engine, 1012, &["LRANGE", "k", "0", "-1"]), &[b"new"]);
                assert_integer(run(engine, 1012, &["PTTL", "k"]), 68);
            }
        }
    }

    #[test]
    fn held_expiry_structural_transfers_do_not_resolve_source_owned_deletion() {
        let mut source = journaled_engine();
        let mut recovered = Engine::new();
        source.set_migration_expiry_ranges(Some(&[(0, 16383)]));
        run(&mut source, 1000, &["RPUSH", "k", "old"]);
        replay_pending(&mut source, &mut recovered);
        run(&mut source, 1000, &["PEXPIRE", "k", "10"]);
        replay_pending(&mut source, &mut recovered);
        assert_integer(run(&mut source, 1010, &["EXISTS", "k"]), 0);
        let key = Bytes::from_static(b"k");
        let mut gathered = journaled_engine();
        gathered.inherit_runtime(&source);
        source.transfer_keys(std::slice::from_ref(&key), &mut gathered);
        assert!(source.take_batch().is_none());
        assert!(
            gathered.take_batch().is_none(),
            "gathering is not a semantic deletion"
        );
        let mut shards = gathered.partition(2);
        for shard in &mut shards {
            assert!(
                shard.take_batch().is_none(),
                "distribution is not a semantic deletion"
            );
        }
        let shard = &mut shards[Engine::shard_for(&key, 2)];
        shard.set_replication_replay(true);
        assert_integer(run(shard, 0, &["PERSIST", "k"]), 1);
        shard.set_replication_replay(false);
        replay_pending(shard, &mut recovered);
        assert_bulk_array(run(shard, 1012, &["LRANGE", "k", "0", "-1"]), &[b"old"]);
        assert_integer(run(shard, 1012, &["PTTL", "k"]), -1);
        assert_bulk_array(
            run(&mut recovered, 1012, &["LRANGE", "k", "0", "-1"]),
            &[b"old"],
        );
        assert_integer(run(&mut recovered, 1012, &["PTTL", "k"]), -1);
    }

    #[test]
    fn list_moves_support_every_side_and_preserve_both_list_deadlines() {
        for (from, to, moved, source, destination) in [
            (
                "LEFT",
                "LEFT",
                b"a".as_slice(),
                vec![b"b".as_slice()],
                vec![b"a".as_slice(), b"x", b"y"],
            ),
            (
                "LEFT",
                "RIGHT",
                b"a".as_slice(),
                vec![b"b".as_slice()],
                vec![b"x".as_slice(), b"y", b"a"],
            ),
            (
                "RIGHT",
                "LEFT",
                b"b".as_slice(),
                vec![b"a".as_slice()],
                vec![b"b".as_slice(), b"x", b"y"],
            ),
            (
                "RIGHT",
                "RIGHT",
                b"b".as_slice(),
                vec![b"a".as_slice()],
                vec![b"x".as_slice(), b"y", b"b"],
            ),
        ] {
            let mut engine = Engine::new();
            run(&mut engine, 1000, &["RPUSH", "source", "a", "b"]);
            run(&mut engine, 1000, &["RPUSH", "destination", "x", "y"]);
            run(&mut engine, 1000, &["PEXPIRE", "source", "100"]);
            run(&mut engine, 1000, &["PEXPIRE", "destination", "200"]);
            assert_bulk(
                run(
                    &mut engine,
                    1001,
                    &["LMOVE", "source", "destination", from, to],
                ),
                Some(moved),
            );
            assert_bulk_array(
                run(&mut engine, 1001, &["LRANGE", "source", "0", "-1"]),
                &source,
            );
            assert_bulk_array(
                run(&mut engine, 1001, &["LRANGE", "destination", "0", "-1"]),
                &destination,
            );
            assert_integer(run(&mut engine, 1001, &["PTTL", "source"]), 99);
            assert_integer(run(&mut engine, 1001, &["PTTL", "destination"]), 199);
            assert_bulk(
                run(
                    &mut engine,
                    1001,
                    &["BLMOVE", "source", "fresh", from, to, "0"],
                ),
                Some(source[0]),
            );
            assert_integer(run(&mut engine, 1001, &["EXISTS", "source"]), 0);
            assert_integer(run(&mut engine, 1001, &["PTTL", "fresh"]), -1);
        }
    }

    #[test]
    fn list_move_batches_replay_rotations_including_singleton_expiry() {
        for values in [vec!["one"], vec!["one", "two", "three"]] {
            for (from, to) in [
                ("LEFT", "LEFT"),
                ("LEFT", "RIGHT"),
                ("RIGHT", "LEFT"),
                ("RIGHT", "RIGHT"),
            ] {
                let mut source = journaled_engine();
                let mut recovered = journaled_engine();
                let mut push = vec!["RPUSH", "q"];
                push.extend(values.iter().copied());
                persist(&mut source, &mut recovered, 1000, &push);
                persist(&mut source, &mut recovered, 1000, &["PEXPIRE", "q", "100"]);
                let expected = if from == "LEFT" {
                    values[0]
                } else {
                    values[values.len() - 1]
                };
                assert_bulk(
                    persist(
                        &mut source,
                        &mut recovered,
                        1001,
                        &["LMOVE", "q", "q", from, to],
                    ),
                    Some(expected.as_bytes()),
                );
                assert_eq!(
                    run(&mut source, 1001, &["LRANGE", "q", "0", "-1"]),
                    run(&mut recovered, 1001, &["LRANGE", "q", "0", "-1"]),
                );
                assert_integer(run(&mut source, 1001, &["PTTL", "q"]), 99);
                assert_integer(run(&mut recovered, 1001, &["PTTL", "q"]), 99);
                assert_integer(run(&mut recovered, 1100, &["EXISTS", "q"]), 0);
            }
        }
        let mut source = journaled_engine();
        let mut recovered = journaled_engine();
        persist(
            &mut source,
            &mut recovered,
            1000,
            &["RPUSH", "queued", "job"],
        );
        assert_bulk(
            persist(
                &mut source,
                &mut recovered,
                1001,
                &["BLMOVE", "queued", "primed", "RIGHT", "LEFT", "0"],
            ),
            Some(b"job"),
        );
        assert_integer(run(&mut recovered, 1001, &["EXISTS", "queued"]), 0);
        assert_bulk_array(
            run(&mut recovered, 1001, &["LRANGE", "primed", "0", "-1"]),
            &[b"job"],
        );
    }

    #[test]
    fn list_moves_validate_before_mutation_and_only_block_on_the_source() {
        let mut engine = journaled_engine();
        run(&mut engine, 1000, &["RPUSH", "source", "job"]);
        engine.take_batch();
        run(&mut engine, 1000, &["SET", "wrong", "string"]);
        engine.take_batch();
        for args in [
            vec!["LMOVE", "source", "wrong", "RIGHT", "LEFT"],
            vec!["BLMOVE", "source", "wrong", "RIGHT", "LEFT", "0"],
            vec!["LMOVE", "source", "fresh", "invalid", "LEFT"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "invalid", "0"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "LEFT", "-1"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "LEFT", "NaN"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "LEFT", "inf"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "LEFT", "1e-9999"],
            vec!["LMOVE", "wrong", "source", "RIGHT", "LEFT"],
            vec!["LMOVE", "source", "fresh", "RIGHT"],
            vec!["BLMOVE", "source", "fresh", "RIGHT", "LEFT"],
        ] {
            assert_error(run(&mut engine, 1000, &args));
            assert!(engine.take_batch().is_none());
            assert_bulk_array(
                run(&mut engine, 1000, &["LRANGE", "source", "0", "-1"]),
                &[b"job"],
            );
        }
        assert_bulk(
            run(
                &mut engine,
                1000,
                &["LMOVE", "missing", "wrong", "RIGHT", "LEFT"],
            ),
            None,
        );
        let Execution::Block(block) = run_execution(
            &mut engine,
            1000,
            &["BLMOVE", "missing", "wrong", "right", "left", "0.001"],
        ) else {
            panic!("expected a blocking list move");
        };
        assert_eq!(block.keys, vec![Bytes::from_static(b"missing")]);
        assert_eq!(
            block.destination,
            Some((Bytes::from_static(b"wrong"), false))
        );
        assert_eq!(block.timeout, Some(Duration::from_millis(1)));
        assert_eq!(block.null_reply(), Resp::Bulk(None));
        assert!(block.right);
    }

    #[test]
    fn list_moves_keep_cold_payload_ownership_and_fail_before_removing_corrupt_bodies() {
        let path = std::env::temp_dir().join(format!(
            "ffkv-list-move-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
        ));
        let store = Store::open(crate::storage::Config {
            directory: path.clone(),
            inline_threshold: 0,
            cache_bytes: 0,
            segment_bytes: 4096,
            disk_budget: 0,
        })
        .unwrap();
        let mut engine = Engine::new();
        engine.set_storage(store.clone());
        run(&mut engine, 1000, &["RPUSH", "source", "cold body"]);
        let physical = store.stats().physical_bytes;
        assert_bulk(
            run(
                &mut engine,
                1000,
                &["LMOVE", "source", "destination", "RIGHT", "LEFT"],
            ),
            Some(b"cold body"),
        );
        assert_eq!(store.stats().physical_bytes, physical);
        let Value::List(list) = &engine.entries[&Bytes::from_static(b"destination")].value else {
            panic!("expected destination list");
        };
        assert!(list[0].is_disk());
        for entry in std::fs::read_dir(&path).unwrap() {
            let entry = entry.unwrap();
            if entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "seg")
            {
                std::fs::OpenOptions::new()
                    .write(true)
                    .open(entry.path())
                    .unwrap()
                    .set_len(0)
                    .unwrap();
            }
        }
        let args: Vec<_> = ["LMOVE", "destination", "fresh", "LEFT", "RIGHT"]
            .into_iter()
            .map(|arg| Bytes::from_static(arg.as_bytes()))
            .collect();
        assert!(engine.execute_at_time(&args, 1000).is_err());
        assert_integer(run(&mut engine, 1000, &["LLEN", "destination"]), 1);
        assert_integer(run(&mut engine, 1000, &["EXISTS", "fresh"]), 0);
        assert!(engine.take_batch().is_none());
        drop(engine);
        drop(store);
        std::fs::remove_dir_all(path).unwrap();
    }

    #[test]
    fn aggregate_budget_counts_repeated_bodies_and_refuses_pops_without_mutation() {
        let mut engine = Engine::new();
        let body = "b".repeat(700);
        run(&mut engine, 1000, &["SET", "s", &body]);
        run(&mut engine, 1000, &["HSET", "h", "f", &body]);
        run(&mut engine, 1000, &["SADD", "set", &body]);
        run(&mut engine, 1000, &["ZADD", "z", "1", &body]);
        let original_limit = engine.reply_limit;
        engine.set_reply_limit(1024);
        assert_error(run(&mut engine, 1000, &["MGET", "s", "s"]));
        assert_error(run(&mut engine, 1000, &["HMGET", "h", "f", "f"]));
        engine.set_reply_limit(original_limit);
        assert_eq!(
            into_array(run(&mut engine, 1000, &["HMGET", "h", "f", "f"])).len(),
            2
        );
        engine.set_reply_limit(128);
        assert_error(run(&mut engine, 1000, &["SMEMBERS", "set"]));
        assert_error(run(&mut engine, 1000, &["SPOP", "set", "1"]));
        assert_integer(run(&mut engine, 1000, &["SCARD", "set"]), 1);
        assert_integer(run(&mut engine, 1000, &["SISMEMBER", "set", &body]), 1);
        assert_error(run(&mut engine, 1000, &["ZPOPMIN", "z", "1"]));
        assert_integer(run(&mut engine, 1000, &["ZCARD", "z"]), 1);
        engine.set_reply_limit(original_limit);
        assert_eq!(
            run(&mut engine, 1000, &["ZPOPMIN", "z", "1"]),
            Resp::Scored(vec![(Bytes::from(body), 1.0)]),
        );
    }

    #[test]
    fn journal_replays_resolved_string_writes_and_absolute_ttls() {
        let mut source = journaled_engine();
        let mut recovered = Engine::new();
        assert_bulk(
            persist(
                &mut source,
                &mut recovered,
                1000,
                &["SET", "number", "10", "NX", "GET", "PX", "5000"],
            ),
            None,
        );
        assert_bulk(
            run(
                &mut source,
                1100,
                &["SET", "number", "ignored", "NX", "GET"],
            ),
            Some(b"10"),
        );
        assert!(source.take_batch().is_none());
        assert_bulk(
            run(&mut source, 1100, &["SET", "missing", "ignored", "XX"]),
            None,
        );
        assert!(source.take_batch().is_none());
        assert_bulk(
            persist(
                &mut source,
                &mut recovered,
                1200,
                &["SET", "number", "20", "XX", "GET", "KEEPTTL"],
            ),
            Some(b"10"),
        );
        for (args, expected) in [
            (vec!["INCR", "number"], 21),
            (vec!["INCRBY", "number", "4"], 25),
            (vec!["DECR", "number"], 24),
            (vec!["DECRBY", "number", "4"], 20),
        ] {
            assert_integer(persist(&mut source, &mut recovered, 1300, &args), expected);
        }
        persist(
            &mut source,
            &mut recovered,
            1400,
            &["SET", "reused", "old", "PX", "100"],
        );
        assert_bulk(
            persist(
                &mut source,
                &mut recovered,
                1500,
                &["SET", "reused", "new", "NX", "GET"],
            ),
            None,
        );
        assert_bulk(run(&mut recovered, 2000, &["GET", "reused"]), Some(b"new"));
        assert_integer(run(&mut recovered, 2000, &["PTTL", "reused"]), -1);
        assert_bulk(run(&mut recovered, 5999, &["GET", "number"]), Some(b"20"));
        assert_integer(run(&mut recovered, 5999, &["PTTL", "number"]), 1);
        assert_bulk(run(&mut recovered, 6000, &["GET", "number"]), None);
        assert_integer(run(&mut recovered, 6000, &["PTTL", "number"]), -2);
        assert!(recovered.take_batch().is_none());
    }

    #[test]
    fn journal_multi_key_commands_replay_as_single_complete_batches() {
        let mut source = journaled_engine();
        let mut recovered = journaled_engine();
        persist(&mut source, &mut recovered, 1000, &["RPUSH", "q", "old"]);
        persist(&mut source, &mut recovered, 1000, &["PEXPIRE", "q", "10"]);
        assert_error(run(&mut source, 1000, &["MSET", "q", "bad", "dangling"]));
        assert!(source.take_batch().is_none());
        assert_bulk_array(
            run(&mut source, 1000, &["LRANGE", "q", "0", "-1"]),
            &[b"old"],
        );
        assert_status(
            persist(
                &mut source,
                &mut recovered,
                1001,
                &["MSET", "q", "first", "other", "second", "q", "last"],
            ),
            "OK",
        );
        recovered.expire_at(2000, usize::MAX);
        assert_bulk_array(
            run(&mut recovered, 2000, &["MGET", "q", "other"]),
            &[b"last", b"second"],
        );
        assert_integer(run(&mut recovered, 2000, &["PTTL", "q"]), -1);
        assert!(recovered.expirations.is_empty());
        assert_integer(
            persist(
                &mut source,
                &mut recovered,
                2000,
                &["DEL", "q", "q", "absent", "other"],
            ),
            2,
        );
        assert_integer(run(&mut recovered, 2000, &["DBSIZE"]), 0);
        assert_integer(run(&mut source, 2000, &["DEL", "missing"]), 0);
        assert!(source.take_batch().is_none());
    }

    #[test]
    fn journal_replays_expiration_changes_persist_and_explicit_deletions() {
        let mut source = journaled_engine();
        let mut recovered = journaled_engine();
        persist(&mut source, &mut recovered, 1000, &["SET", "key", "value"]);
        for (now, args, ttl) in [
            (1000, vec!["EXPIRE", "key", "3", "NX"], 3000),
            (1100, vec!["PEXPIRE", "key", "4000", "XX", "GT"], 4000),
            (1200, vec!["EXPIREAT", "key", "6"], 4800),
            (1300, vec!["PEXPIREAT", "key", "7000"], 5700),
        ] {
            assert_integer(persist(&mut source, &mut recovered, now, &args), 1);
            assert_integer(run(&mut recovered, now, &["PTTL", "key"]), ttl);
            assert_eq!(recovered.expirations.len(), 1);
        }
        assert_integer(
            persist(&mut source, &mut recovered, 1400, &["PERSIST", "key"]),
            1,
        );
        assert_integer(run(&mut recovered, 1400, &["PTTL", "key"]), -1);
        assert!(recovered.expirations.is_empty());
        assert_integer(run(&mut source, 1400, &["PERSIST", "key"]), 0);
        assert!(source.take_batch().is_none());
        persist(
            &mut source,
            &mut recovered,
            1500,
            &["PEXPIRE", "key", "100"],
        );
        assert_integer(
            persist(&mut source, &mut recovered, 1501, &["PEXPIRE", "key", "0"]),
            1,
        );
        assert_bulk(run(&mut recovered, 1501, &["GET", "key"]), None);
        assert!(recovered.expirations.is_empty());
        persist(&mut source, &mut recovered, 1600, &["RPUSH", "q", "old"]);
        persist(&mut source, &mut recovered, 1600, &["PEXPIRE", "q", "100"]);
        assert_status(
            persist(
                &mut source,
                &mut recovered,
                1601,
                &["SET", "q", "gone", "PXAT", "1"],
            ),
            "OK",
        );
        assert_status(run(&mut recovered, 1601, &["TYPE", "q"]), "none");
        assert!(recovered.expirations.is_empty());
        assert_status(
            run(&mut source, 1601, &["SET", "absent", "gone", "PXAT", "1"]),
            "OK",
        );
        assert!(source.take_batch().is_none());
    }

    #[test]
    fn journal_replays_list_order_counts_and_delivered_blocking_pops() {
        let mut source = journaled_engine();
        let mut recovered = journaled_engine();
        persist(
            &mut source,
            &mut recovered,
            1000,
            &["RPUSH", "q", "a", "b", "c", "d", "e", "f"],
        );
        persist(&mut source, &mut recovered, 1000, &["LPUSH", "q", "x", "y"]);
        persist(&mut source, &mut recovered, 1000, &["PEXPIRE", "q", "100"]);
        assert_bulk_array(
            run(&mut recovered, 1000, &["LRANGE", "q", "0", "-1"]),
            &[b"y", b"x", b"a", b"b", b"c", b"d", b"e", b"f"],
        );
        assert_bulk_array(run(&mut source, 1001, &["LPOP", "q", "0"]), &[]);
        assert!(source.take_batch().is_none());
        assert_bulk_array(
            persist(&mut source, &mut recovered, 1001, &["LPOP", "q", "2"]),
            &[b"y", b"x"],
        );
        assert_bulk(
            persist(&mut source, &mut recovered, 1002, &["RPOP", "q"]),
            Some(b"f"),
        );
        assert_bulk_array(
            immediate_reply(run_execution(
                &mut source,
                1003,
                &["BLPOP", "missing", "q", "0"],
            )),
            &[b"q", b"a"],
        );
        replay_pending(&mut source, &mut recovered);
        assert_bulk_array(
            immediate_reply(run_execution(&mut source, 1004, &["BRPOP", "q", "0"])),
            &[b"q", b"e"],
        );
        replay_pending(&mut source, &mut recovered);
        let key = Bytes::from_static(b"q");
        assert_eq!(
            source.try_pop_at(&key, false, 1005).unwrap(),
            Some(Bytes::from_static(b"b")),
        );
        replay_pending(&mut source, &mut recovered);
        assert_bulk_array(
            run(&mut recovered, 1005, &["LRANGE", "q", "0", "-1"]),
            &[b"c", b"d"],
        );
        assert_integer(run(&mut recovered, 1005, &["PTTL", "q"]), 95);
        assert_bulk_array(
            persist(&mut source, &mut recovered, 1006, &["RPOP", "q", "99"]),
            &[b"d", b"c"],
        );
        assert_status(run(&mut recovered, 1006, &["TYPE", "q"]), "none");
        assert!(recovered.expirations.is_empty());
        assert!(matches!(
            run_execution(&mut source, 1006, &["BLPOP", "q", "0"]),
            Execution::Block(_)
        ));
        assert!(source.take_batch().is_none());
        assert_eq!(source.try_pop_at(&key, true, 1006).unwrap(), None);
        assert!(source.take_batch().is_none());
    }

    #[test]
    fn journal_replay_discards_expired_values_before_list_deltas() {
        let mut source = journaled_engine();
        let mut recovered = journaled_engine();
        persist(&mut source, &mut recovered, 1000, &["RPUSH", "q", "old"]);
        persist(&mut source, &mut recovered, 1000, &["PEXPIRE", "q", "1"]);
        persist(&mut source, &mut recovered, 1001, &["LPUSH", "q", "a", "b"]);
        persist(
            &mut source,
            &mut recovered,
            1001,
            &["SET", "changed-type", "old", "PX", "1"],
        );
        persist(
            &mut source,
            &mut recovered,
            1002,
            &["RPUSH", "changed-type", "new"],
        );
        recovered.expire_at(2000, usize::MAX);
        assert_bulk_array(
            run(&mut recovered, 2000, &["LRANGE", "q", "0", "-1"]),
            &[b"b", b"a"],
        );
        assert_bulk_array(
            run(&mut recovered, 2000, &["LRANGE", "changed-type", "0", "-1"]),
            &[b"new"],
        );
        assert_integer(run(&mut recovered, 2000, &["PTTL", "q"]), -1);
        assert_integer(run(&mut recovered, 2000, &["PTTL", "changed-type"]), -1);
        assert!(recovered.expirations.is_empty());
    }

    #[test]
    fn recovery_finishes_all_overdue_expiration_without_extending_ttls() {
        let now = unix_milliseconds();
        let historical = now - 100_000;
        let mut source = journaled_engine();
        let mut recovered = Engine::new();
        for index in 0..=ACTIVE_EXPIRATION_BATCH {
            let key = format!("expired-{index}");
            persist(
                &mut source,
                &mut recovered,
                historical,
                &["SET", &key, "old", "PX", "100"],
            );
        }
        persist(
            &mut source,
            &mut recovered,
            historical,
            &["RPUSH", "q", "old"],
        );
        persist(
            &mut source,
            &mut recovered,
            historical,
            &["PEXPIRE", "q", "100"],
        );
        persist(
            &mut source,
            &mut recovered,
            historical,
            &["SET", "live", "value", "PX", "200000"],
        );
        recovered.finish_recovery().unwrap();
        assert_eq!(recovered.entries.len(), 1);
        assert_bulk(run(&mut recovered, now, &["GET", "live"]), Some(b"value"));
        assert_integer(run(&mut recovered, now, &["PTTL", "live"]), 100_000);
        assert_status(run(&mut recovered, now, &["TYPE", "q"]), "none");
        assert!(recovered.take_batch().is_none());
    }

    #[test]
    fn journaling_is_opt_in_and_expiration_effects_recover_without_resurrection() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SET", "string", "7"]);
        run(&mut engine, 1000, &["RPUSH", "q", "a", "b"]);
        run(&mut engine, 1000, &["LPOP", "q"]);
        run(&mut engine, 1000, &["SET", "background", "old", "PX", "1"]);
        run(&mut engine, 1000, &["SET", "passive", "old", "PX", "2"]);
        assert!(engine.take_batch().is_none());
        engine.set_recording(true);
        for args in [
            vec!["GET", "string"],
            vec!["MGET", "string", "q", "missing"],
            vec!["EXISTS", "string", "q"],
            vec!["TYPE", "q"],
            vec!["LLEN", "q"],
            vec!["LRANGE", "q", "0", "-1"],
            vec!["PTTL", "q"],
            vec!["TTL", "string"],
            vec!["DBSIZE"],
            vec!["COMMAND", "COUNT"],
            vec!["SET", "string", "ignored", "NX"],
            vec!["SET", "missing", "ignored", "XX"],
            vec!["SET", "q", "ignored", "GET"],
            vec!["SET", "string", "ignored", "invalid"],
            vec!["LPUSH", "string", "ignored"],
            vec!["LPOP", "string"],
            vec!["LPOP", "q", "0"],
            vec!["LPOP", "q", "-1"],
            vec!["INCR", "q"],
            vec!["INCRBY", "string", "invalid"],
            vec!["DEL", "missing"],
            vec!["PERSIST", "string"],
            vec!["PEXPIRE", "missing", "1"],
            vec!["PEXPIRE", "string", "1", "XX"],
        ] {
            run(&mut engine, 1000, &args);
            assert!(engine.take_batch().is_none(), "{args:?} recorded a change");
        }
        assert_bulk(run(&mut engine, 1000, &["GET", "string"]), Some(b"7"));
        assert_bulk_array(run(&mut engine, 1000, &["LRANGE", "q", "0", "-1"]), &[b"b"]);
        let mut recovered = Engine::new();
        for batch in engine.snapshot_batches(1000) {
            recovered.replay(batch.unwrap()).unwrap();
        }
        engine.expire_at(1001, usize::MAX);
        replay_pending(&mut engine, &mut recovered);
        assert_bulk(run(&mut recovered, 1000, &["GET", "background"]), None);
        assert_bulk(run(&mut recovered, 1000, &["GET", "passive"]), Some(b"old"));
        assert_bulk(run(&mut engine, 1002, &["GET", "passive"]), None);
        replay_pending(&mut engine, &mut recovered);
        assert_bulk(run(&mut recovered, 1000, &["GET", "passive"]), None);
        assert_bulk(run(&mut recovered, 1002, &["GET", "string"]), Some(b"7"));
        assert_bulk_array(
            run(&mut recovered, 1002, &["LRANGE", "q", "0", "-1"]),
            &[b"b"],
        );
    }

    #[test]
    fn replay_rejects_invalid_logical_changes_and_accepts_absent_deletes() {
        let string = Bytes::from_static(b"string");
        let list = Bytes::from_static(b"q");
        let missing = Bytes::from_static(b"missing");
        let expired = Bytes::from_static(b"expired");
        let cases = [
            vec![],
            vec![Mutation::Set {
                key: missing.clone(),
                value: Bytes::from_static(b"value"),
                expires_at: Some(1002),
            }],
            vec![Mutation::Push {
                key: list.clone(),
                values: vec![],
                right: false,
            }],
            vec![Mutation::Push {
                key: string.clone(),
                values: vec![Bytes::from_static(b"value")],
                right: true,
            }],
            vec![Mutation::Pop {
                key: list.clone(),
                count: 0,
                right: false,
            }],
            vec![Mutation::Pop {
                key: list.clone(),
                count: 3,
                right: true,
            }],
            vec![Mutation::Pop {
                key: list.clone(),
                count: u64::MAX,
                right: false,
            }],
            vec![Mutation::Pop {
                key: missing.clone(),
                count: 1,
                right: false,
            }],
            vec![Mutation::Pop {
                key: string.clone(),
                count: 1,
                right: true,
            }],
            vec![Mutation::Pop {
                key: expired.clone(),
                count: 1,
                right: false,
            }],
            vec![Mutation::Expire {
                key: missing.clone(),
                expires_at: Some(2000),
            }],
            vec![Mutation::Expire {
                key: expired,
                expires_at: Some(2000),
            }],
            vec![Mutation::Expire {
                key: string.clone(),
                expires_at: Some(1002),
            }],
            vec![Mutation::Expire {
                key: string,
                expires_at: None,
            }],
        ];
        for mutations in cases {
            let mut engine = journaled_engine();
            engine
                .replay(Batch {
                    timestamp_ms: 1000,
                    mutations: vec![
                        Mutation::Set {
                            key: Bytes::from_static(b"string"),
                            value: Bytes::from_static(b"value"),
                            expires_at: None,
                        },
                        Mutation::Set {
                            key: Bytes::from_static(b"timed"),
                            value: Bytes::from_static(b"value"),
                            expires_at: Some(1100),
                        },
                        Mutation::Push {
                            key: Bytes::from_static(b"q"),
                            values: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
                            right: true,
                        },
                        Mutation::Push {
                            key: Bytes::from_static(b"expired"),
                            values: vec![Bytes::from_static(b"old")],
                            right: true,
                        },
                        Mutation::Expire {
                            key: Bytes::from_static(b"expired"),
                            expires_at: Some(1001),
                        },
                    ],
                })
                .unwrap();
            let error = engine
                .replay(Batch {
                    timestamp_ms: 1002,
                    mutations,
                })
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(engine.take_batch().is_none());
        }
        let mut engine = journaled_engine();
        engine
            .replay(Batch {
                timestamp_ms: 1000,
                mutations: vec![Mutation::Delete { key: missing }],
            })
            .unwrap();
        assert_integer(run(&mut engine, 1000, &["DBSIZE"]), 0);
        assert!(engine.take_batch().is_none());
    }

    fn run(engine: &mut Engine, now: i64, args: &[&str]) -> Resp {
        let args: Vec<_> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        engine.execute_at(&args, now)
    }

    fn run_execution(engine: &mut Engine, now: i64, args: &[&str]) -> Execution {
        let args: Vec<_> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        engine.execution_at(&args, now)
    }

    fn immediate_reply(execution: Execution) -> Resp {
        match execution {
            Execution::Reply(reply) => reply,
            _ => panic!("expected an immediate reply"),
        }
    }

    fn assert_integer(reply: Resp, expected: i64) {
        match reply {
            Resp::Integer(value) => assert_eq!(value, expected),
            _ => panic!("expected an integer reply"),
        }
    }

    fn assert_bulk(reply: Resp, expected: Option<&[u8]>) {
        match reply {
            Resp::Bulk(value) => assert_eq!(value.as_deref(), expected),
            _ => panic!("expected a bulk reply"),
        }
    }

    fn assert_status(reply: Resp, expected: &str) {
        match reply {
            Resp::Simple(value) => assert_eq!(value, expected),
            _ => panic!("expected a status reply"),
        }
    }

    fn assert_error(reply: Resp) {
        assert!(matches!(reply, Resp::Error(_)), "expected an error reply");
    }

    fn into_array(reply: Resp) -> Vec<Resp> {
        match reply {
            Resp::Array(Some(values)) => values,
            _ => panic!("expected an array reply"),
        }
    }

    fn assert_bulk_array(reply: Resp, expected: &[&[u8]]) {
        let values = into_array(reply);
        assert_eq!(values.len(), expected.len());
        for (value, expected) in values.into_iter().zip(expected) {
            assert_bulk(value, Some(expected));
        }
    }

    fn assert_wrong_type(reply: Resp) {
        match reply {
            Resp::Error(message) => assert!(message.starts_with("WRONGTYPE ")),
            _ => panic!("expected a WRONGTYPE reply"),
        }
    }

    #[test]
    fn list_push_order_and_pop_reply_shapes() {
        let mut engine = Engine::new();
        assert_integer(run(&mut engine, 1000, &["RPUSH", "q", "a", "b", "c"]), 3);
        assert_integer(run(&mut engine, 1000, &["LPUSH", "q", "x", "y"]), 5);
        assert_bulk_array(
            run(&mut engine, 1000, &["LRANGE", "q", "0", "-1"]),
            &[b"y", b"x", b"a", b"b", b"c"],
        );
        assert_status(run(&mut engine, 1000, &["TYPE", "q"]), "list");
        assert_bulk(run(&mut engine, 1000, &["LPOP", "q"]), Some(b"y"));
        assert_bulk_array(run(&mut engine, 1000, &["RPOP", "q", "2"]), &[b"c", b"b"]);
        assert_bulk_array(run(&mut engine, 1000, &["LPOP", "q", "0"]), &[]);
        assert_integer(run(&mut engine, 1000, &["LLEN", "q"]), 2);
        assert_bulk_array(
            run(&mut engine, 1000, &["RPOP", "q", "9223372036854775807"]),
            &[b"a", b"x"],
        );
        assert_integer(run(&mut engine, 1000, &["LLEN", "q"]), 0);
        assert_status(run(&mut engine, 1000, &["TYPE", "q"]), "none");
        assert_integer(run(&mut engine, 1000, &["DBSIZE"]), 0);
        assert_bulk(run(&mut engine, 1000, &["LPOP", "q"]), None);
        assert!(matches!(
            run(&mut engine, 1000, &["LPOP", "q", "0"]),
            Resp::Array(None)
        ));
        assert!(matches!(
            run(&mut engine, 1000, &["RPOP", "q", "1"]),
            Resp::Array(None)
        ));
        assert_bulk_array(run(&mut engine, 1000, &["LRANGE", "q", "0", "-1"]), &[]);
    }

    #[test]
    fn list_ranges_normalize_negative_bounds_without_overflow() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["RPUSH", "q", "a", "b", "c", "d"]);
        let cases: &[(&str, &str, &[&[u8]])] = &[
            ("0", "-1", &[b"a", b"b", b"c", b"d"]),
            ("-2", "-1", &[b"c", b"d"]),
            ("-100", "1", &[b"a", b"b"]),
            ("1", "-100", &[]),
            ("3", "1", &[]),
            ("4", "99", &[]),
            ("-2", "-2", &[b"c"]),
            ("-1", "9223372036854775807", &[b"d"]),
            (
                "-9223372036854775808",
                "9223372036854775807",
                &[b"a", b"b", b"c", b"d"],
            ),
            ("9223372036854775807", "9223372036854775807", &[]),
            ("-9223372036854775808", "-9223372036854775808", &[]),
        ];
        for (start, stop, expected) in cases {
            assert_bulk_array(
                run(&mut engine, 1000, &["LRANGE", "q", start, stop]),
                expected,
            );
        }
        assert_integer(run(&mut engine, 1000, &["LLEN", "q"]), 4);
    }

    #[test]
    fn list_count_and_range_validation_precedes_mutation_and_type_checks() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["RPUSH", "q", "a", "b"]);
        run(&mut engine, 1000, &["PEXPIRE", "q", "100"]);
        run(&mut engine, 1000, &["SET", "string", "value"]);
        for invalid in [
            "",
            "+1",
            "01",
            "-0",
            "1.0",
            " 1",
            "1 ",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            for command in ["LPOP", "RPOP"] {
                assert_error(run(&mut engine, 1000, &[command, "q", invalid]));
                assert_error(run(&mut engine, 1000, &[command, "missing", invalid]));
            }
            assert_error(run(&mut engine, 1000, &["LRANGE", "q", invalid, "-1"]));
            assert_error(run(&mut engine, 1000, &["LRANGE", "q", "0", invalid]));
            let reply = run(&mut engine, 1000, &["LRANGE", "string", invalid, "-1"]);
            assert!(matches!(reply, Resp::Error(message) if message.starts_with("ERR ")));
        }
        for command in ["LPOP", "RPOP"] {
            assert_error(run(&mut engine, 1000, &[command, "q", "-1"]));
            let reply = run(&mut engine, 1000, &[command, "string", "-1"]);
            assert!(matches!(reply, Resp::Error(message) if message.starts_with("ERR ")));
            assert_wrong_type(run(&mut engine, 1000, &[command, "string", "0"]));
            assert_error(run(&mut engine, 1000, &[command, "q", "1", "extra"]));
        }
        assert_error(run(&mut engine, 1000, &["LPUSH", "missing"]));
        assert_error(run(&mut engine, 1000, &["RPUSH", "q"]));
        assert_bulk_array(
            run(&mut engine, 1000, &["LRANGE", "q", "0", "-1"]),
            &[b"a", b"b"],
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "q"]), 100);
        assert_eq!(engine.expirations.len(), 1);
        assert_status(run(&mut engine, 1000, &["TYPE", "missing"]), "none");
    }

    #[test]
    fn strings_and_lists_obey_cross_type_and_conditional_set_precedence() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["RPUSH", "q", "a", "b"]);
        run(&mut engine, 1000, &["PEXPIRE", "q", "100"]);
        run(&mut engine, 1000, &["SET", "string", "7", "PX", "100"]);
        for args in [
            vec!["GET", "q"],
            vec!["INCR", "q"],
            vec!["DECR", "q"],
            vec!["INCRBY", "q", "2"],
            vec!["DECRBY", "q", "2"],
            vec!["SET", "q", "changed", "NX", "GET"],
            vec!["SET", "q", "changed", "GET", "NX"],
            vec!["SET", "q", "changed", "XX", "GET"],
            vec!["SET", "q", "changed", "GET"],
            vec!["LPUSH", "string", "item"],
            vec!["RPUSH", "string", "item"],
            vec!["LPOP", "string"],
            vec!["RPOP", "string", "1"],
            vec!["LLEN", "string"],
            vec!["LRANGE", "string", "0", "-1"],
        ] {
            assert_wrong_type(run(&mut engine, 1000, &args));
        }
        assert_bulk(run(&mut engine, 1000, &["SET", "q", "changed", "NX"]), None);
        assert_bulk_array(
            run(&mut engine, 1000, &["LRANGE", "q", "0", "-1"]),
            &[b"a", b"b"],
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "q"]), 100);
        assert_bulk(run(&mut engine, 1000, &["GET", "string"]), Some(b"7"));
        assert_integer(run(&mut engine, 1000, &["PTTL", "string"]), 100);
        let mut values =
            into_array(run(&mut engine, 1000, &["MGET", "q", "string", "missing"])).into_iter();
        assert_bulk(values.next().unwrap(), None);
        assert_bulk(values.next().unwrap(), Some(b"7"));
        assert_bulk(values.next().unwrap(), None);
        assert_status(run(&mut engine, 1000, &["SET", "q", "8", "KEEPTTL"]), "OK");
        assert_status(run(&mut engine, 1000, &["TYPE", "q"]), "string");
        assert_integer(run(&mut engine, 1000, &["PTTL", "q"]), 100);
        assert_status(run(&mut engine, 1000, &["SET", "q", "9"]), "OK");
        assert_integer(run(&mut engine, 1000, &["PTTL", "q"]), -1);
        run(&mut engine, 1000, &["DEL", "q"]);
        run(&mut engine, 1000, &["RPUSH", "q", "old"]);
        run(&mut engine, 1000, &["PEXPIRE", "q", "100"]);
        assert_status(
            run(&mut engine, 1000, &["MSET", "q", "new", "string", "new"]),
            "OK",
        );
        assert_bulk(run(&mut engine, 1000, &["GET", "q"]), Some(b"new"));
        assert_bulk(run(&mut engine, 1000, &["GET", "string"]), Some(b"new"));
        assert!(engine.expirations.is_empty());
    }

    #[test]
    fn list_mutations_preserve_ttl_and_final_pops_remove_expiration_records() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"q");
        run(&mut engine, 1000, &["RPUSH", "q", "a", "b", "c"]);
        run(&mut engine, 1000, &["PEXPIRE", "q", "100"]);
        run(&mut engine, 1001, &["RPUSH", "q", "d"]);
        assert_integer(run(&mut engine, 1001, &["PTTL", "q"]), 99);
        assert_bulk(run(&mut engine, 1002, &["LPOP", "q"]), Some(b"a"));
        assert_bulk_array(run(&mut engine, 1003, &["LPOP", "q", "2"]), &[b"b", b"c"]);
        assert_integer(run(&mut engine, 1003, &["PTTL", "q"]), 97);
        assert_eq!(engine.expirations.len(), 1);
        assert_eq!(
            engine.try_pop_at(&key, true, 1004).unwrap(),
            Some(Bytes::from_static(b"d"))
        );
        assert!(!engine.entries.contains_key(&key));
        assert!(engine.expirations.is_empty());
        assert_eq!(engine.try_pop_at(&key, false, 1004).unwrap(), None);
        assert_integer(run(&mut engine, 1004, &["PTTL", "q"]), -2);
        run(&mut engine, 1004, &["RPUSH", "q", "a", "b"]);
        run(&mut engine, 1004, &["PEXPIRE", "q", "100"]);
        assert_bulk_array(run(&mut engine, 1004, &["LPOP", "q", "99"]), &[b"a", b"b"]);
        assert!(!engine.entries.contains_key(&key));
        assert!(engine.expirations.is_empty());
        run(&mut engine, 1004, &["SET", "q", "string"]);
        assert_wrong_type(engine.try_pop_at(&key, false, 1004).unwrap_err());
    }

    #[test]
    fn expired_lists_reuse_keys_without_stale_ttl_or_wrong_type() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["RPUSH", "q", "old"]);
        run(&mut engine, 1000, &["PEXPIRE", "q", "1"]);
        assert_integer(run(&mut engine, 1001, &["LPUSH", "q", "new"]), 1);
        assert_integer(run(&mut engine, 1001, &["PTTL", "q"]), -1);
        assert!(engine.expirations.is_empty());
        engine.expire_at(2000, usize::MAX);
        assert_bulk_array(
            run(&mut engine, 2000, &["LRANGE", "q", "0", "-1"]),
            &[b"new"],
        );
        run(&mut engine, 2000, &["PEXPIRE", "q", "1"]);
        assert_bulk(
            run(&mut engine, 2001, &["SET", "q", "string", "NX", "GET"]),
            None,
        );
        assert_bulk(run(&mut engine, 2001, &["GET", "q"]), Some(b"string"));
        assert!(engine.expirations.is_empty());
        run(&mut engine, 2001, &["RPUSH", "active", "item"]);
        run(&mut engine, 2001, &["PEXPIRE", "active", "1"]);
        engine.expire_at(2002, 1);
        assert_status(run(&mut engine, 2002, &["TYPE", "active"]), "none");
        assert!(engine.expirations.is_empty());
    }

    #[test]
    fn list_keys_and_elements_are_binary_safe() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"\0\xff\r\n");
        let first = Bytes::from_static(b"\xff\0\xfe\r\n");
        let second = Bytes::from_static(b"");
        assert_integer(
            engine.execute_at(
                &[
                    Bytes::from_static(b"RPUSH"),
                    key.clone(),
                    first.clone(),
                    second.clone(),
                ],
                1000,
            ),
            2,
        );
        assert_bulk_array(
            engine.execute_at(
                &[
                    Bytes::from_static(b"LRANGE"),
                    key.clone(),
                    Bytes::from_static(b"0"),
                    Bytes::from_static(b"-1"),
                ],
                1000,
            ),
            &[first.as_ref(), second.as_ref()],
        );
        assert_eq!(engine.try_pop_at(&key, false, 1000).unwrap(), Some(first));
        assert_eq!(engine.try_pop_at(&key, true, 1000).unwrap(), Some(second));
    }

    #[test]
    fn initial_blocking_pops_preserve_key_priority_and_wrong_type_precedence() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SET", "bad", "string"]);
        run(&mut engine, 1000, &["RPUSH", "first", "a", "b"]);
        run(&mut engine, 1000, &["RPUSH", "second", "c", "d"]);
        assert_bulk_array(
            immediate_reply(run_execution(
                &mut engine,
                1000,
                &["BLPOP", "missing", "first", "bad", "0"],
            )),
            &[b"first", b"a"],
        );
        assert_wrong_type(immediate_reply(run_execution(
            &mut engine,
            1000,
            &["BRPOP", "missing", "bad", "first", "0"],
        )));
        assert_bulk_array(
            immediate_reply(run_execution(
                &mut engine,
                1000,
                &["BRPOP", "first", "second", "0"],
            )),
            &[b"first", b"b"],
        );
        assert_bulk_array(
            run(&mut engine, 1000, &["LRANGE", "second", "0", "-1"]),
            &[b"c", b"d"],
        );
        run(&mut engine, 1000, &["RPUSH", "expired", "old"]);
        run(&mut engine, 1000, &["PEXPIRE", "expired", "1"]);
        assert_bulk_array(
            immediate_reply(run_execution(
                &mut engine,
                1001,
                &["BRPOP", "expired", "second", "1"],
            )),
            &[b"second", b"d"],
        );
        let Execution::Block(request) = run_execution(
            &mut engine,
            1001,
            &["BLPOP", "missing", "other", "missing", "0"],
        ) else {
            panic!("expected a blocking request");
        };
        assert_eq!(
            request.keys,
            vec![
                Bytes::from_static(b"missing"),
                Bytes::from_static(b"other"),
                Bytes::from_static(b"missing"),
            ]
        );
        assert!(!request.right);
        assert_eq!(request.timeout, None);
    }

    #[test]
    fn blocking_timeout_validation_is_finite_and_precedes_any_pop() {
        let mut engine = Engine::new();
        let now = 1_800_000_000_000;
        run(&mut engine, now, &["RPUSH", "q", "a", "b"]);
        run(&mut engine, now, &["PEXPIRE", "q", "100"]);
        for invalid in [
            "",
            "NaN",
            "nan",
            "inf",
            "+inf",
            "-inf",
            "Infinity",
            "-1",
            "-0.001",
            "-1e-9999",
            "1e-9999",
            "1e9999",
            "1e308",
            "9223372036854775",
            "9223372036854776",
            " 1",
            "1 ",
            "1x",
            "1\0",
        ] {
            for command in ["BLPOP", "BRPOP"] {
                assert_error(immediate_reply(run_execution(
                    &mut engine,
                    now,
                    &[command, "q", invalid],
                )));
            }
            assert_bulk_array(
                run(&mut engine, now, &["LRANGE", "q", "0", "-1"]),
                &[b"a", b"b"],
            );
            assert_integer(run(&mut engine, now, &["PTTL", "q"]), 100);
            assert_eq!(engine.expirations.len(), 1);
        }
        for timeout in ["0", "-0", "+0", "0.0", "0e1000"] {
            let Execution::Block(request) =
                run_execution(&mut engine, now, &["BLPOP", "missing", timeout])
            else {
                panic!("expected an infinite blocking request");
            };
            assert_eq!(request.timeout, None);
        }
        for (timeout, milliseconds) in [
            ("0.0001", 1),
            ("1e-3", 1),
            ("+0.001", 1),
            ("1.25", 1250),
            ("2", 2000),
        ] {
            let Execution::Block(request) =
                run_execution(&mut engine, now, &["BRPOP", "missing", timeout])
            else {
                panic!("expected a finite blocking request");
            };
            assert!(request.right);
            assert_eq!(request.timeout, Some(Duration::from_millis(milliseconds)));
        }
        assert!(blocking_timeout(b"0.001", i64::MAX).is_err());
        assert!(blocking_timeout(b"0.0001", i64::MAX).is_err());
        assert_eq!(blocking_timeout(b"-0", i64::MAX).unwrap(), None);
    }

    #[test]
    fn set_conditions_get_and_ttl_preservation() {
        let mut engine = Engine::new();
        assert_bulk(
            run(
                &mut engine,
                1000,
                &["SET", "key", "one", "NX", "GET", "PX", "5000"],
            ),
            None,
        );
        assert_bulk(
            run(&mut engine, 1100, &["SET", "key", "two", "NX", "GET"]),
            Some(b"one"),
        );
        assert_bulk(run(&mut engine, 1100, &["GET", "key"]), Some(b"one"));
        assert_integer(run(&mut engine, 1100, &["PTTL", "key"]), 4900);
        assert_bulk(
            run(
                &mut engine,
                1100,
                &["SET", "key", "two", "XX", "GET", "KEEPTTL"],
            ),
            Some(b"one"),
        );
        assert_integer(run(&mut engine, 1200, &["PTTL", "key"]), 4800);
        assert_status(run(&mut engine, 1200, &["SET", "key", "three", "XX"]), "OK");
        assert_integer(run(&mut engine, 1200, &["PTTL", "key"]), -1);
        assert!(engine.expirations.is_empty());
        assert_bulk(
            run(&mut engine, 1200, &["SET", "absent", "value", "XX", "GET"]),
            None,
        );
        assert_bulk(run(&mut engine, 1200, &["GET", "absent"]), None);
        assert_status(
            run(&mut engine, 1200, &["SET", "key", "old", "PX", "1"]),
            "OK",
        );
        assert_bulk(
            run(&mut engine, 1201, &["SET", "key", "new", "NX", "GET"]),
            None,
        );
        assert_bulk(run(&mut engine, 1201, &["GET", "key"]), Some(b"new"));
    }

    #[test]
    fn set_validates_before_mutation_and_accepts_redis_repeated_options() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SET", "key", "original"]);
        for options in [
            vec!["NX", "XX"],
            vec!["XX", "NX"],
            vec!["PX", "20", "EX", "1"],
            vec!["KEEPTTL", "PX", "20"],
            vec!["PX", "20", "KEEPTTL"],
            vec!["PX"],
            vec!["PX", "0"],
            vec!["EX", "-1"],
            vec!["PX", "+1"],
            vec!["UNKNOWN"],
        ] {
            let mut args = vec!["SET", "key", "changed"];
            args.extend(options);
            assert_error(run(&mut engine, 1000, &args));
            assert_bulk(run(&mut engine, 1000, &["GET", "key"]), Some(b"original"));
        }
        assert_status(
            run(
                &mut engine,
                1000,
                &["SET", "key", "new", "XX", "XX", "PX", "invalid", "PX", "40"],
            ),
            "OK",
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 40);
        assert_bulk(
            run(
                &mut engine,
                1000,
                &["SET", "key", "last", "GET", "GET", "KEEPTTL", "KEEPTTL"],
            ),
            Some(b"new"),
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 40);
    }

    #[test]
    fn overwrites_keep_expiration_index_exact_and_never_expire_new_values() {
        let mut engine = Engine::new();
        for deadline in 2000..3000 {
            let deadline = deadline.to_string();
            assert_status(
                run(
                    &mut engine,
                    1000,
                    &["SET", "key", "value", "PXAT", &deadline],
                ),
                "OK",
            );
            assert_eq!(engine.expirations.len(), 1);
        }
        engine.expire_at(2000, ACTIVE_EXPIRATION_BATCH);
        assert_bulk(run(&mut engine, 2000, &["GET", "key"]), Some(b"value"));
        assert_integer(run(&mut engine, 2000, &["PEXPIRE", "key", "5000"]), 1);
        engine.expire_at(2999, ACTIVE_EXPIRATION_BATCH);
        assert_bulk(run(&mut engine, 2999, &["GET", "key"]), Some(b"value"));
        assert_eq!(engine.expirations.len(), 1);
        assert_status(run(&mut engine, 3000, &["MSET", "key", "persistent"]), "OK");
        assert!(engine.expirations.is_empty());
        engine.expire_at(10000, ACTIVE_EXPIRATION_BATCH);
        assert_bulk(
            run(&mut engine, 10000, &["GET", "key"]),
            Some(b"persistent"),
        );
        run(&mut engine, 10000, &["PEXPIRE", "key", "1"]);
        assert_integer(run(&mut engine, 10000, &["PERSIST", "key"]), 1);
        assert!(engine.expirations.is_empty());
        run(&mut engine, 10000, &["PEXPIRE", "key", "1"]);
        assert_integer(run(&mut engine, 10000, &["DEL", "key"]), 1);
        assert!(engine.expirations.is_empty());
    }

    #[test]
    fn active_expiration_is_bounded_and_does_not_scan_persistent_keys() {
        let mut engine = Engine::new();
        for index in 0..(ACTIVE_EXPIRATION_BATCH + 5) {
            let key = index.to_string();
            run(&mut engine, 1000, &["SET", &key, "value", "PX", "1"]);
        }
        run(&mut engine, 1000, &["SET", "persistent", "value"]);
        engine.expire_at(1001, ACTIVE_EXPIRATION_BATCH);
        assert_eq!(engine.expirations.len(), 5);
        assert_eq!(engine.entries.len(), 6);
        assert_integer(run(&mut engine, 1001, &["DBSIZE"]), 1);
        assert!(engine.expirations.is_empty());
        assert_bulk(
            run(&mut engine, 1001, &["GET", "persistent"]),
            Some(b"value"),
        );
    }

    #[test]
    fn integer_overflow_and_invalid_numbers_leave_value_and_ttl_unchanged() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            1000,
            &["SET", "key", "9223372036854775807", "PX", "100"],
        );
        assert_error(run(&mut engine, 1000, &["INCR", "key"]));
        assert_error(run(&mut engine, 1000, &["INCRBY", "key", "1"]));
        assert_bulk(
            run(&mut engine, 1000, &["GET", "key"]),
            Some(b"9223372036854775807"),
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 100);
        run(
            &mut engine,
            1000,
            &["SET", "key", "-9223372036854775808", "PX", "100"],
        );
        assert_error(run(&mut engine, 1000, &["DECR", "key"]));
        assert_error(run(
            &mut engine,
            1000,
            &["DECRBY", "key", "-9223372036854775808"],
        ));
        assert_bulk(
            run(&mut engine, 1000, &["GET", "key"]),
            Some(b"-9223372036854775808"),
        );
        assert_integer(
            run(&mut engine, 1000, &["INCRBY", "key", "9223372036854775807"]),
            -1,
        );
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 100);
        assert_eq!(engine.expirations.len(), 1);
        for invalid in [
            "",
            "+1",
            "01",
            "-0",
            " 1",
            "1 ",
            "1.0",
            "9223372036854775808",
            "-9223372036854775809",
        ] {
            run(&mut engine, 1000, &["SET", "bad", invalid]);
            assert_error(run(&mut engine, 1000, &["INCR", "bad"]));
            assert_bulk(
                run(&mut engine, 1000, &["GET", "bad"]),
                Some(invalid.as_bytes()),
            );
            assert_error(run(&mut engine, 1000, &["INCRBY", "absent", invalid]));
            assert_bulk(run(&mut engine, 1000, &["GET", "absent"]), None);
        }
        assert_integer(run(&mut engine, 1000, &["DECRBY", "absent", "-5"]), 5);
    }

    #[test]
    fn expiration_boundaries_units_and_ttl_rounding() {
        let mut engine = Engine::new();
        assert_integer(run(&mut engine, 1000, &["TTL", "missing"]), -2);
        run(&mut engine, 1000, &["SET", "key", "value", "PXAT", "1500"]);
        assert_integer(run(&mut engine, 1000, &["TTL", "key"]), 1);
        assert_integer(run(&mut engine, 1001, &["TTL", "key"]), 0);
        assert_integer(run(&mut engine, 1499, &["PTTL", "key"]), 1);
        assert_integer(run(&mut engine, 1500, &["PTTL", "key"]), -2);
        assert!(engine.expirations.is_empty());
        for args in [
            vec!["SET", "key", "value", "EX", "2"],
            vec!["SET", "key", "value", "EXAT", "3"],
            vec!["SET", "key", "value", "PX", "2000"],
            vec!["SET", "key", "value", "PXAT", "3000"],
        ] {
            assert_status(run(&mut engine, 1000, &args), "OK");
            assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 2000);
        }
        assert_status(
            run(&mut engine, 1000, &["SET", "key", "gone", "PXAT", "1000"]),
            "OK",
        );
        assert_bulk(run(&mut engine, 1000, &["GET", "key"]), None);
        run(&mut engine, 1000, &["SET", "key", "value"]);
        assert_integer(run(&mut engine, 1000, &["EXPIREAT", "key", "3"]), 1);
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 2000);
        assert_integer(run(&mut engine, 1000, &["EXPIRE", "key", "2"]), 1);
        assert_integer(run(&mut engine, 1000, &["PEXPIREAT", "key", "1000"]), 1);
        assert_integer(run(&mut engine, 1000, &["EXISTS", "key"]), 0);
        run(&mut engine, 1000, &["SET", "key", "value"]);
        assert_integer(
            run(
                &mut engine,
                1000,
                &["PEXPIRE", "key", "-9223372036854775808"],
            ),
            1,
        );
        assert_integer(run(&mut engine, 1000, &["PERSIST", "key"]), 0);
    }

    #[test]
    fn huge_expiration_arithmetic_is_checked_before_mutation() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SET", "key", "original"]);
        for args in [
            vec!["SET", "key", "new", "EX", "9223372036854775807"],
            vec!["SET", "key", "new", "PX", "9223372036854775807"],
            vec!["SET", "key", "new", "EXAT", "9223372036854775807"],
            vec!["EXPIRE", "key", "9223372036854775807"],
            vec!["EXPIRE", "key", "-9223372036854775808"],
            vec!["PEXPIRE", "key", "9223372036854775807"],
            vec!["EXPIREAT", "key", "9223372036854775807"],
        ] {
            assert_error(run(&mut engine, 1000, &args));
            assert_bulk(run(&mut engine, 1000, &["GET", "key"]), Some(b"original"));
        }
        assert_status(
            run(
                &mut engine,
                0,
                &["SET", "key", "last", "PXAT", "9223372036854775807"],
            ),
            "OK",
        );
        assert_integer(run(&mut engine, 0, &["PTTL", "key"]), i64::MAX);
        assert_integer(run(&mut engine, 0, &["TTL", "key"]), i64::MAX / 1000 + 1);
        assert_bulk(run(&mut engine, i64::MAX, &["GET", "key"]), None);
    }

    #[test]
    fn conditional_expiration_handles_infinite_ttl_and_combined_flags() {
        let mut engine = Engine::new();
        run(&mut engine, 1000, &["SET", "key", "value"]);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "100", "GT"]), 0);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "100", "XX"]), 0);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "100", "LT"]), 1);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "200", "NX"]), 0);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "100", "GT"]), 0);
        assert_integer(run(&mut engine, 1000, &["PEXPIRE", "key", "100", "LT"]), 0);
        assert_integer(
            run(&mut engine, 1000, &["PEXPIRE", "key", "200", "XX", "GT"]),
            1,
        );
        assert_integer(
            run(
                &mut engine,
                1000,
                &["PEXPIRE", "key", "150", "XX", "LT", "LT"],
            ),
            1,
        );
        assert_eq!(engine.expirations.len(), 1);
        for flags in [
            vec!["NX", "XX"],
            vec!["NX", "GT"],
            vec!["NX", "LT"],
            vec!["GT", "LT"],
            vec!["BAD"],
        ] {
            let mut args = vec!["PEXPIRE", "key", "1"];
            args.extend(flags);
            assert_error(run(&mut engine, 1000, &args));
            assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 150);
        }
        assert_integer(
            run(&mut engine, 1000, &["PEXPIRE", "key", "0", "XX", "LT"]),
            1,
        );
        assert!(engine.expirations.is_empty());
        assert_integer(
            run(&mut engine, 1000, &["PEXPIRE", "missing", "100", "NX"]),
            0,
        );
        run(&mut engine, 1000, &["SET", "key", "value"]);
        assert_integer(
            run(&mut engine, 1000, &["PEXPIRE", "key", "10", "NX", "NX"]),
            1,
        );
    }

    #[test]
    fn multi_key_commands_are_binary_safe_and_validate_before_writing() {
        let mut engine = Engine::new();
        let key = Bytes::from_static(b"\0\xff\r\n");
        let value = Bytes::from_static(b"\xff\0\xfe\r\n");
        assert_status(
            engine.execute_at(
                &[Bytes::from_static(b"SET"), key.clone(), value.clone()],
                1000,
            ),
            "OK",
        );
        assert_bulk(
            engine.execute_at(&[Bytes::from_static(b"GET"), key.clone()], 1000),
            Some(&value),
        );
        assert_integer(
            engine.execute_at(
                &[Bytes::from_static(b"EXISTS"), key.clone(), key.clone()],
                1000,
            ),
            2,
        );
        run(&mut engine, 1000, &["SET", "key", "old", "PX", "10"]);
        assert_error(run(&mut engine, 1000, &["MSET", "key", "new", "dangling"]));
        assert_bulk(run(&mut engine, 1000, &["GET", "key"]), Some(b"old"));
        assert_integer(run(&mut engine, 1000, &["PTTL", "key"]), 10);
        assert_status(
            run(
                &mut engine,
                1000,
                &["MSET", "key", "first", "key", "last", "other", "value"],
            ),
            "OK",
        );
        assert!(engine.expirations.is_empty());
        let replies = into_array(run(&mut engine, 1000, &["MGET", "key", "missing", "other"]));
        let mut replies = replies.into_iter();
        assert_bulk(replies.next().unwrap(), Some(b"last"));
        assert_bulk(replies.next().unwrap(), None);
        assert_bulk(replies.next().unwrap(), Some(b"value"));
        assert_integer(
            run(&mut engine, 1000, &["EXISTS", "key", "key", "missing"]),
            2,
        );
        assert_integer(run(&mut engine, 1000, &["DEL", "key", "key", "missing"]), 1);
        assert_integer(
            engine.execute_at(&[Bytes::from_static(b"DEL"), key.clone(), key], 1000),
            1,
        );
    }

    #[test]
    fn registry_drives_truthful_command_metadata_and_validation() {
        let mut engine = Engine::new();
        let mut info = into_array(run(
            &mut engine,
            1000,
            &[
                "COMMAND",
                "INFO",
                "MsEt",
                "SET",
                "not-implemented",
                "command|count",
            ],
        ))
        .into_iter();
        let mset = into_array(info.next().unwrap());
        assert_integer_ref(&mset[1], -3);
        assert_integer_ref(&mset[3], 1);
        assert_integer_ref(&mset[4], -1);
        assert_integer_ref(&mset[5], 2);
        let set = into_array(info.next().unwrap());
        assert_integer_ref(&set[1], -3);
        assert_integer_ref(&set[3], 1);
        assert_integer_ref(&set[4], 1);
        assert_integer_ref(&set[5], 1);
        assert_bulk(info.next().unwrap(), None);
        let count = into_array(info.next().unwrap());
        assert_integer_ref(&count[1], 2);
        assert_error(run(&mut engine, 1000, &["COMMAND", "COUNT", "extra"]));
        assert_eq!(
            run(&mut engine, 1000, &["COMMAND", "GETKEYS", "GET", "key"]),
            array(vec![bulk("key")]),
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["COMMAND", "GETKEYS", "MSET", "a", "1", "b", "2"]
            ),
            array(vec![bulk("a"), bulk("b")]),
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &["COMMAND", "GETKEYS", "FQ.ENQUEUE", "jobs", "body"]
            ),
            array(vec![bulk("jobs")]),
        );
        assert_eq!(
            run(
                &mut engine,
                1000,
                &[
                    "COMMAND", "GETKEYS", "EVAL", "return 1", "2", "a", "b", "arg"
                ]
            ),
            array(vec![bulk("a"), bulk("b")]),
        );
        assert_error(run(
            &mut engine,
            1000,
            &["COMMAND", "GETKEYS", "EVAL", "return 1", "2", "a"],
        ));
        assert_error(run(&mut engine, 1000, &["COMMAND", "GETKEYS", "GET"]));
        assert_error(run(&mut engine, 1000, &["COMMAND", "GETKEYS", "PING"]));
        assert_error(run(&mut engine, 1000, &["NOT-IMPLEMENTED"]));
        assert_error(run(&mut engine, 1000, &["GET"]));
        assert_error(run(&mut engine, 1000, &["PING", "one", "two"]));
        assert_error(run(&mut engine, 1000, &["MSET", "dangling"]));
        assert_error(engine.execute_at(&[], 1000));
    }

    fn assert_integer_ref(reply: &Resp, expected: i64) {
        match reply {
            Resp::Integer(value) => assert_eq!(*value, expected),
            _ => panic!("expected an integer reply"),
        }
    }

    #[test]
    fn connection_commands_and_type_are_explicit() {
        let mut engine = Engine::new();
        assert_status(run(&mut engine, 1000, &["pInG"]), "PONG");
        assert_bulk(
            run(&mut engine, 1000, &["PING", "message"]),
            Some(b"message"),
        );
        assert_bulk(
            run(&mut engine, 1000, &["ECHO", "message"]),
            Some(b"message"),
        );
        assert_status(run(&mut engine, 1000, &["QUIT"]), "OK");
        assert_error(run(&mut engine, 1000, &["QUIT", "extra"]));
        assert_status(run(&mut engine, 1000, &["SELECT", "0"]), "OK");
        assert_status(run(&mut engine, 1000, &["SELECT", "1"]), "OK");
        run(&mut engine, 1000, &["SET", "selected", "isolated"]);
        assert_status(run(&mut engine, 1000, &["SELECT", "0"]), "OK");
        assert_bulk(run(&mut engine, 1000, &["GET", "selected"]), None);
        assert_error(run(&mut engine, 1000, &["SELECT", "-1"]));
        assert_error(run(&mut engine, 1000, &["SELECT", "+0"]));
        assert_status(run(&mut engine, 1000, &["TYPE", "key"]), "none");
        run(&mut engine, 1000, &["SET", "key", "value", "PX", "1"]);
        assert_status(run(&mut engine, 1000, &["TYPE", "key"]), "string");
        assert_status(run(&mut engine, 1001, &["TYPE", "key"]), "none");
        assert_integer(run(&mut engine, 1001, &["DBSIZE"]), 0);
    }
    #[test]
    fn dynamic_key_extraction_ignores_payloads_paths_and_stream_ids() {
        let cases: &[(&[&str], &[&str])] = &[
            (
                &["CMS.MERGE", "out", "2", "a", "b", "WEIGHTS", "1", "2"],
                &["out", "a", "b"],
            ),
            (
                &["TDIGEST.MERGE", "out", "2", "a", "b", "OVERRIDE"],
                &["out", "a", "b"],
            ),
            (
                &["XREAD", "COUNT", "2", "STREAMS", "a", "STREAMS", "0", "0"],
                &["a", "STREAMS"],
            ),
            (
                &[
                    "XREADGROUP",
                    "GROUP",
                    "g",
                    "c",
                    "NOACK",
                    "STREAMS",
                    "a",
                    "b",
                    ">",
                    ">",
                ],
                &["a", "b"],
            ),
            (&["XGROUP", "CREATE", "a", "g", "0"], &["a"]),
            (&["XINFO", "STREAM", "a"], &["a"]),
            (&["JSON.DEBUG", "MEMORY", "a", "$"], &["a"]),
            (&["JSON.MGET", "a", "b", "$"], &["a", "b"]),
            (&["JSON.MSET", "a", "$", "1", "b", "$", "2"], &["a", "b"]),
            (&["TS.MADD", "a", "1", "2", "b", "3", "4"], &["a", "b"]),
            (&["TS.NRANGE", "2", "a", "b", "-", "+"], &["a", "b"]),
            (
                &["GEORADIUS", "src", "1", "2", "3", "km", "STORE", "dst"],
                &["src", "dst"],
            ),
            (
                &["EVAL", "return 1", "2", "a", "b", "argument"],
                &["a", "b"],
            ),
        ];
        for (command, expected) in cases {
            let args: Vec<_> = command
                .iter()
                .map(|value| Bytes::copy_from_slice(value.as_bytes()))
                .collect();
            assert_eq!(
                Engine::accessed_keys(&args),
                expected
                    .iter()
                    .map(|value| Bytes::copy_from_slice(value.as_bytes()))
                    .collect::<Vec<_>>()
            );
        }
        for command in [
            vec!["CMS.MERGE", "out", "999999999999999999999", "a"],
            vec!["XREAD", "STREAMS", "a", "b", "0"],
            vec!["JSON.MSET", "a", "$", "1", "dangling"],
            vec!["TS.MADD", "a", "1", "2", "dangling"],
        ] {
            let args: Vec<_> = command
                .iter()
                .map(|value| Bytes::copy_from_slice(value.as_bytes()))
                .collect();
            assert!(Engine::validate(&args).is_err());
        }
    }

    #[test]
    fn typed_snapshot_replay_watch_ttl_rename_and_corruption_share_native_lifecycle() {
        let mut source = journaled_engine();
        let watches = Arc::new(crate::watch::Registry::default());
        source.set_watches(watches.clone());
        source.set_ready_tracking(true);
        let key = Bytes::from_static(b"array");
        let token = watches.register(0, key.clone(), None);
        assert!(!matches!(
            run(&mut source, 1000, &["ARSET", "array", "7", "binary"]),
            Resp::Error(_)
        ));
        assert!(token.changed(1000));
        let first = source.take_batch().unwrap();
        assert!(
            first
                .mutations
                .iter()
                .any(|mutation| matches!(mutation, Mutation::Typed { kind: 1, .. }))
        );
        source.take_ready_keys();
        run(&mut source, 1000, &["PEXPIREAT", "array", "2000"]);
        source.take_batch();
        let mut recovered = Engine::new();
        for batch in source.snapshot_batches(1000) {
            recovered.replay(batch.unwrap()).unwrap();
        }
        assert_status(run(&mut recovered, 1000, &["TYPE", "array"]), "array");
        assert_bulk(
            run(&mut recovered, 1000, &["ARGET", "array", "7"]),
            Some(b"binary"),
        );
        assert_integer(run(&mut recovered, 1000, &["PTTL", "array"]), 1000);
        for (kind, effect) in [
            (2, Bytes::from_static(b"\x01\0")),
            (1, Bytes::from_static(b"\xff")),
        ] {
            assert!(
                recovered
                    .replay(Batch {
                        timestamp_ms: 1000,
                        mutations: vec![Mutation::Typed {
                            key: key.clone(),
                            kind,
                            effect
                        }]
                    })
                    .is_err()
            );
            assert_bulk(
                run(&mut recovered, 1000, &["ARGET", "array", "7"]),
                Some(b"binary"),
            );
        }
        assert_status(
            run(&mut recovered, 1000, &["RENAME", "array", "renamed"]),
            "OK",
        );
        assert_status(run(&mut recovered, 1000, &["TYPE", "array"]), "none");
        assert_bulk(
            run(&mut recovered, 1000, &["ARGET", "renamed", "7"]),
            Some(b"binary"),
        );
        assert_status(run(&mut recovered, 2000, &["TYPE", "renamed"]), "none");
        assert!(recovered.expirations.is_empty());
    }
}
