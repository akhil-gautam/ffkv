//! Versioned protocol metadata; no Redis implementation code is embedded.
mod descriptors;
use super::{Command, CommandSpec, Engine, NO_KEYS, Resp, array, error, syntax_error};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::Value;
use std::{collections::HashMap, sync::LazyLock};

fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::new(
        Command::Connection,
        "spublish",
        3,
        &["pubsub", "loading", "stale", "fast", "may_replicate"],
        (1, 1, 1),
    ),
    CommandSpec::new(
        Command::Connection,
        "ssubscribe",
        -2,
        &["pubsub", "noscript", "loading", "stale", "denyoom"],
        (1, -1, 1),
    ),
    CommandSpec::new(
        Command::Connection,
        "sunsubscribe",
        -1,
        &["pubsub", "noscript", "loading", "stale"],
        (1, -1, 1),
    ),
    CommandSpec::new(
        Command::Scripting,
        "evalsha_ro",
        -3,
        &[
            "noscript",
            "skip_monitor",
            "no_mandatory_keys",
            "stale",
            "script_runner",
            "readonly",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "eval_ro",
        -3,
        &[
            "noscript",
            "skip_monitor",
            "no_mandatory_keys",
            "stale",
            "script_runner",
            "readonly",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "fcall",
        -3,
        &[
            "noscript",
            "skip_monitor",
            "may_replicate",
            "no_mandatory_keys",
            "script_runner",
            "stale",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "fcall_ro",
        -3,
        &[
            "noscript",
            "skip_monitor",
            "no_mandatory_keys",
            "stale",
            "script_runner",
            "readonly",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "reset",
        1,
        &[
            "noscript",
            "loading",
            "stale",
            "fast",
            "no_auth",
            "allow_busy",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "bgrewriteaof",
        1,
        &["no_async_loading", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "bgsave",
        -1,
        &["no_async_loading", "admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "failover",
        -1,
        &["admin", "noscript", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Maintenance, "hotkeys", -2, &[], NO_KEYS),
    CommandSpec::new(
        Command::Maintenance,
        "lastsave",
        1,
        &["loading", "stale", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "lolwut",
        -1,
        &["readonly", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "monitor",
        1,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "psync",
        -3,
        &["no_async_loading", "admin", "no_multi", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "replconf",
        -1,
        &["admin", "noscript", "loading", "stale", "allow_busy"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "replicaof",
        3,
        &["no_async_loading", "admin", "noscript", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "role",
        1,
        &["noscript", "loading", "stale", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "save",
        1,
        &["no_async_loading", "admin", "noscript", "no_multi"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "shutdown",
        -1,
        &[
            "admin",
            "noscript",
            "loading",
            "stale",
            "no_multi",
            "allow_busy",
        ],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "slaveof",
        3,
        &["no_async_loading", "admin", "noscript", "stale"],
        NO_KEYS,
    ),
    CommandSpec::handler("swapdb", 3, &["write", "fast"], NO_KEYS, Engine::cmd_swapdb),
    CommandSpec::new(
        Command::Maintenance,
        "sync",
        1,
        &["no_async_loading", "admin", "no_multi", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "time",
        1,
        &["loading", "stale", "fast"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Connection, "asking", 1, &["fast"], NO_KEYS),
    CommandSpec::new(
        Command::Connection,
        "readonly",
        1,
        &["fast", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "readwrite",
        1,
        &["fast", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Maintenance, "wait", 3, &["blocking"], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "waitaof", 4, &["blocking"], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "cluster", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "config", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Scripting, "function", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "backup", 2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "latency", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "memory", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "module", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "slowlog", -2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "migrate", -6, &["write"], (3, 3, 1)),
];
pub(super) static SUBCOMMANDS: &[CommandSpec] = &[
    CommandSpec::new(
        Command::Extension,
        "xgroup|create",
        -5,
        &["write", "denyoom"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xgroup|createconsumer",
        5,
        &["write", "denyoom"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xgroup|delconsumer",
        5,
        &["write"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xgroup|destroy",
        4,
        &["write"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xgroup|setid",
        -5,
        &["write"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xinfo|consumers",
        4,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xinfo|groups",
        3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "xinfo|stream",
        -3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "json.debug|memory",
        -3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "ft.config|get",
        3,
        &["readonly"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Extension, "ft.config|set", 4, &["admin"], NO_KEYS),
    CommandSpec::new(
        Command::Extension,
        "ft.cursor|del",
        4,
        &["readonly"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Extension,
        "ft.cursor|read",
        -4,
        &["readonly"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub|channels",
        -2,
        &["pubsub", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub|numpat",
        2,
        &["pubsub", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub|numsub",
        -2,
        &["pubsub", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub|shardchannels",
        -2,
        &["pubsub", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "pubsub|shardnumsub",
        -2,
        &["pubsub", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|delete",
        3,
        &["noscript", "write"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|dump",
        2,
        &["noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|flush",
        -2,
        &["noscript", "write"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|kill",
        2,
        &["noscript", "allow_busy"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|list",
        -2,
        &["noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|load",
        -3,
        &["noscript", "write", "denyoom"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|restore",
        -3,
        &["noscript", "write", "denyoom"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "function|stats",
        2,
        &["noscript", "allow_busy"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "script|debug",
        3,
        &["noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "script|exists",
        -3,
        &["noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "script|flush",
        -2,
        &["noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "script|kill",
        2,
        &["noscript", "allow_busy"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Scripting,
        "script|load",
        3,
        &["noscript", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|caching",
        3,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|getname",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|getredir",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|id",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|info",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|kill",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|list",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|no-evict",
        3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|no-touch",
        3,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|pause",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|reply",
        3,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|setinfo",
        4,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|setname",
        3,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|tracking",
        -3,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|trackinginfo",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|unblock",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Connection,
        "client|unpause",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|cat",
        -2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|deluser",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|dryrun",
        -4,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|genpass",
        -2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|getuser",
        3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|list",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|load",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|log",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|save",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|setuser",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|users",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "acl|whoami",
        2,
        &["noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|abort",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|cleanup",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|list",
        2,
        &["admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|seal",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|start",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "backup|status",
        2,
        &["admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "command|docs",
        -2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "command|getkeysandflags",
        -3,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "command|list",
        -2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "config|get",
        -3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "config|resetstat",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "config|rewrite",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "config|set",
        -4,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "hotkeys|get",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "hotkeys|reset",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "hotkeys|start",
        -2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "hotkeys|stop",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|doctor",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|graph",
        3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|histogram",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|history",
        3,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|latest",
        2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "latency|reset",
        -2,
        &["admin", "noscript", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(Command::Maintenance, "memory|doctor", 2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "memory|malloc-stats", 2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "memory|purge", 2, &[], NO_KEYS),
    CommandSpec::new(Command::Maintenance, "memory|stats", 2, &[], NO_KEYS),
    CommandSpec::new(
        Command::Maintenance,
        "memory|usage",
        -3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Maintenance,
        "module|list",
        2,
        &["admin", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "module|load",
        -3,
        &["no_async_loading", "admin", "noscript", "protected"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "module|loadex",
        -3,
        &["no_async_loading", "admin", "noscript", "protected"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "module|unload",
        3,
        &["no_async_loading", "admin", "noscript", "protected"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "slowlog|get",
        -2,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "slowlog|len",
        2,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "slowlog|reset",
        2,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|addslots",
        -3,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|addslotsrange",
        -4,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|bumpepoch",
        2,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|count-failure-reports",
        3,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|countkeysinslot",
        3,
        &["stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|delslots",
        -3,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|delslotsrange",
        -4,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|failover",
        -2,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|flushslots",
        2,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|forget",
        3,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|getkeysinslot",
        4,
        &["stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|info",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|keyslot",
        3,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|links",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|meet",
        -4,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|migration",
        -4,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|myid",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|myshardid",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|nodes",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|replicas",
        3,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|replicate",
        3,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|reset",
        -2,
        &["admin", "stale", "noscript"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|saveconfig",
        2,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|set-config-epoch",
        3,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|setslot",
        -4,
        &["no_async_loading", "admin", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|shards",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|slaves",
        3,
        &["admin", "loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|slot-stats",
        -4,
        &["stale", "loading"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Maintenance,
        "cluster|slots",
        2,
        &["loading", "stale"],
        NO_KEYS,
    ),
    CommandSpec::new(
        Command::Extension,
        "object|encoding",
        3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "object|freq",
        3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "object|idletime",
        3,
        &["readonly"],
        (2, 2, 1),
    ),
    CommandSpec::new(
        Command::Extension,
        "object|refcount",
        3,
        &["readonly"],
        (2, 2, 1),
    ),
];
#[derive(Deserialize)]
pub(super) struct Info {
    pub name: String,
    pub group: String,
    pub syntax: String,
    pub description: String,
    pub since: String,
}
#[derive(Deserialize)]
struct Catalog {
    commands: Vec<Info>,
}
static DOCUMENTATION: LazyLock<HashMap<String, Info>> = LazyLock::new(|| {
    let catalog: Catalog = serde_json::from_str(include_str!("../command_catalog.json"))
        .expect("compiled command catalog is valid");
    catalog
        .commands
        .into_iter()
        .map(|mut info| {
            info.name = info.name.to_ascii_lowercase().replace(' ', "|");
            (info.name.clone(), info)
        })
        .collect()
});
#[derive(Deserialize)]
struct Metadata {
    commands: HashMap<String, Value>,
}
static METADATA: LazyLock<HashMap<String, Value>> = LazyLock::new(|| {
    serde_json::from_str::<Metadata>(include_str!("../command_metadata.json"))
        .expect("compiled protocol metadata is valid")
        .commands
});
pub(super) fn info(name: &str) -> Option<&'static Info> {
    DOCUMENTATION.get(name)
}
pub(super) fn metadata(name: &str) -> Option<&'static Value> {
    METADATA.get(name)
}
pub(super) fn categories(name: &str) -> &'static [&'static str] {
    match name {
        "acl|cat" => &["slow"],
        "acl|deluser" => &["admin", "slow"],
        "acl|dryrun" => &["admin", "slow"],
        "acl|genpass" => &["slow"],
        "acl|getuser" => &["admin", "slow"],
        "acl|list" => &["admin", "slow"],
        "acl|load" => &["admin", "slow"],
        "acl|log" => &["admin", "slow"],
        "acl|save" => &["admin", "slow"],
        "acl|setuser" => &["admin", "slow"],
        "acl|users" => &["admin", "slow"],
        "acl|whoami" => &["slow"],
        "acl" => &["slow"],
        "append" => &["fast", "string", "write"],
        "arcount" => &["array", "fast", "read"],
        "ardel" => &["array", "fast", "write"],
        "ardelrange" => &["array", "slow", "write"],
        "arget" => &["array", "fast", "read"],
        "argetrange" => &["array", "read", "slow"],
        "argrep" => &["array", "read", "slow"],
        "arinfo" => &["array", "read", "slow"],
        "arinsert" => &["array", "fast", "write"],
        "arlastitems" => &["array", "read", "slow"],
        "arlen" => &["array", "fast", "read"],
        "armget" => &["array", "fast", "read"],
        "armset" => &["array", "fast", "write"],
        "arnext" => &["array", "fast", "read"],
        "arop" => &["array", "read", "slow"],
        "arring" => &["array", "slow", "write"],
        "arscan" => &["array", "read", "slow"],
        "arseek" => &["array", "fast", "write"],
        "arset" => &["array", "fast", "write"],
        "asking" => &["connection", "fast"],
        "auth" => &["connection", "fast"],
        "backup|abort" => &["admin", "slow"],
        "backup|cleanup" => &["admin", "slow"],
        "backup|list" => &["admin", "slow"],
        "backup|seal" => &["admin", "slow"],
        "backup|start" => &["admin", "slow"],
        "backup|status" => &["admin", "slow"],
        "backup" => &["slow"],
        "bgrewriteaof" => &["admin", "slow"],
        "bgsave" => &["admin", "slow"],
        "bitcount" => &["bitmap", "read", "slow"],
        "bitfield" => &["bitmap", "slow", "write"],
        "bitfield_ro" => &["bitmap", "fast", "read"],
        "bitop" => &["bitmap", "slow", "write"],
        "bitpos" => &["bitmap", "read", "slow"],
        "blmove" => &["blocking", "list", "slow", "write"],
        "blmovem" => &["blocking", "list", "slow", "write"],
        "blmpop" => &["blocking", "list", "slow", "write"],
        "blpop" => &["blocking", "list", "slow", "write"],
        "brpop" => &["blocking", "list", "slow", "write"],
        "brpoplpush" => &["blocking", "list", "slow", "write"],
        "bzmpop" => &["blocking", "slow", "sortedset", "write"],
        "bzpopmax" => &["blocking", "fast", "sortedset", "write"],
        "bzpopmin" => &["blocking", "fast", "sortedset", "write"],
        "client|caching" => &["connection", "slow"],
        "client|getname" => &["connection", "slow"],
        "client|getredir" => &["connection", "slow"],
        "client|id" => &["connection", "slow"],
        "client|info" => &["connection", "slow"],
        "client|kill" => &["admin", "connection", "slow"],
        "client|list" => &["admin", "connection", "slow"],
        "client|no-evict" => &["admin", "connection", "slow"],
        "client|no-touch" => &["connection", "slow"],
        "client|pause" => &["admin", "connection", "slow"],
        "client|reply" => &["connection", "slow"],
        "client|setinfo" => &["connection", "slow"],
        "client|setname" => &["connection", "slow"],
        "client|tracking" => &["connection", "slow"],
        "client|trackinginfo" => &["connection", "slow"],
        "client|unblock" => &["admin", "connection", "slow"],
        "client|unpause" => &["admin", "connection", "slow"],
        "client" => &["slow"],
        "cluster|addslots" => &["admin", "slow"],
        "cluster|addslotsrange" => &["admin", "slow"],
        "cluster|bumpepoch" => &["admin", "slow"],
        "cluster|count-failure-reports" => &["admin", "slow"],
        "cluster|countkeysinslot" => &["slow"],
        "cluster|delslots" => &["admin", "slow"],
        "cluster|delslotsrange" => &["admin", "slow"],
        "cluster|failover" => &["admin", "slow"],
        "cluster|flushslots" => &["admin", "slow"],
        "cluster|forget" => &["admin", "slow"],
        "cluster|getkeysinslot" => &["slow"],
        "cluster|info" => &["slow"],
        "cluster|keyslot" => &["slow"],
        "cluster|links" => &["slow"],
        "cluster|meet" => &["admin", "slow"],
        "cluster|migration" => &["admin", "slow"],
        "cluster|myid" => &["slow"],
        "cluster|myshardid" => &["slow"],
        "cluster|nodes" => &["slow"],
        "cluster|replicas" => &["admin", "slow"],
        "cluster|replicate" => &["admin", "slow"],
        "cluster|reset" => &["admin", "slow"],
        "cluster|saveconfig" => &["admin", "slow"],
        "cluster|set-config-epoch" => &["admin", "slow"],
        "cluster|setslot" => &["admin", "slow"],
        "cluster|shards" => &["slow"],
        "cluster|slaves" => &["admin", "slow"],
        "cluster|slot-stats" => &["slow"],
        "cluster|slots" => &["slow"],
        "cluster" => &["slow"],
        "command|count" => &["connection", "slow"],
        "command|docs" => &["connection", "slow"],
        "command|getkeys" => &["connection", "slow"],
        "command|getkeysandflags" => &["connection", "slow"],
        "command|info" => &["connection", "slow"],
        "command|list" => &["connection", "slow"],
        "command" => &["connection", "slow"],
        "config|get" => &["admin", "slow"],
        "config|resetstat" => &["admin", "slow"],
        "config|rewrite" => &["admin", "slow"],
        "config|set" => &["admin", "slow"],
        "config" => &["slow"],
        "copy" => &["keyspace", "slow", "write"],
        "dbsize" => &["fast", "keyspace", "read"],
        "decr" => &["fast", "string", "write"],
        "decrby" => &["fast", "string", "write"],
        "del" => &["keyspace", "slow", "write"],
        "delex" => &["fast", "string", "write"],
        "digest" => &["fast", "read", "string"],
        "discard" => &["fast", "transaction"],
        "dump" => &["keyspace", "read", "slow"],
        "echo" => &["connection", "fast"],
        "eval" => &["scripting", "slow"],
        "eval_ro" => &["read", "scripting", "slow"],
        "evalsha" => &["scripting", "slow"],
        "evalsha_ro" => &["read", "scripting", "slow"],
        "exec" => &["slow", "transaction"],
        "exists" => &["fast", "keyspace", "read"],
        "expire" => &["fast", "keyspace", "write"],
        "expireat" => &["fast", "keyspace", "write"],
        "expiretime" => &["fast", "keyspace", "read"],
        "failover" => &["admin", "slow"],
        "fcall" => &["scripting", "slow"],
        "fcall_ro" => &["read", "scripting", "slow"],
        "flushall" => &["dangerous", "keyspace", "slow", "write"],
        "flushdb" => &["dangerous", "keyspace", "slow", "write"],
        "function|delete" => &["scripting", "slow", "write"],
        "function|dump" => &["scripting", "slow"],
        "function|flush" => &["scripting", "slow", "write"],
        "function|kill" => &["scripting", "slow"],
        "function|list" => &["scripting", "slow"],
        "function|load" => &["scripting", "slow", "write"],
        "function|restore" => &["scripting", "slow", "write"],
        "function|stats" => &["scripting", "slow"],
        "function" => &["slow"],
        "geoadd" => &["geo", "slow", "write"],
        "geodist" => &["geo", "read", "slow"],
        "geohash" => &["geo", "read", "slow"],
        "geopos" => &["geo", "read", "slow"],
        "georadius" => &["geo", "slow", "write"],
        "georadius_ro" => &["geo", "read", "slow"],
        "georadiusbymember" => &["geo", "slow", "write"],
        "georadiusbymember_ro" => &["geo", "read", "slow"],
        "geosearch" => &["geo", "read", "slow"],
        "geosearchstore" => &["geo", "slow", "write"],
        "get" => &["fast", "read", "string"],
        "getbit" => &["bitmap", "fast", "read"],
        "getdel" => &["fast", "string", "write"],
        "getex" => &["fast", "string", "write"],
        "getrange" => &["read", "slow", "string"],
        "getset" => &["fast", "string", "write"],
        "hdel" => &["fast", "hash", "write"],
        "hello" => &["connection", "fast"],
        "hexists" => &["fast", "hash", "read"],
        "hexpire" => &["fast", "hash", "write"],
        "hexpireat" => &["fast", "hash", "write"],
        "hexpiretime" => &["fast", "hash", "read"],
        "hget" => &["fast", "hash", "read"],
        "hgetall" => &["hash", "read", "slow"],
        "hgetdel" => &["fast", "hash", "write"],
        "hgetex" => &["fast", "hash", "write"],
        "himport|discard" => &["hash", "slow"],
        "himport|discardall" => &["hash", "slow"],
        "himport|prepare" => &["hash", "slow"],
        "himport|set" => &["hash", "slow", "write"],
        "himport" => &["hash", "slow"],
        "hincrby" => &["fast", "hash", "write"],
        "hincrbyfloat" => &["fast", "hash", "write"],
        "hkeys" => &["hash", "read", "slow"],
        "hlen" => &["fast", "hash", "read"],
        "hmget" => &["fast", "hash", "read"],
        "hmset" => &["fast", "hash", "write"],
        "hotkeys|get" => &["admin", "slow"],
        "hotkeys|reset" => &["admin", "slow"],
        "hotkeys|start" => &["admin", "slow"],
        "hotkeys|stop" => &["admin", "slow"],
        "hotkeys" => &["slow"],
        "hpersist" => &["fast", "hash", "write"],
        "hpexpire" => &["fast", "hash", "write"],
        "hpexpireat" => &["fast", "hash", "write"],
        "hpexpiretime" => &["fast", "hash", "read"],
        "hpttl" => &["fast", "hash", "read"],
        "hrandfield" => &["hash", "read", "slow"],
        "hscan" => &["hash", "read", "slow"],
        "hset" => &["fast", "hash", "write"],
        "hsetex" => &["fast", "hash", "write"],
        "hsetnx" => &["fast", "hash", "write"],
        "hstrlen" => &["fast", "hash", "read"],
        "httl" => &["fast", "hash", "read"],
        "hvals" => &["hash", "read", "slow"],
        "incr" => &["fast", "string", "write"],
        "incrby" => &["fast", "string", "write"],
        "incrbyfloat" => &["fast", "string", "write"],
        "increx" => &["fast", "string", "write"],
        "info" => &["dangerous", "slow"],
        "keys" => &["dangerous", "keyspace", "read", "slow"],
        "lastsave" => &["admin", "dangerous", "fast"],
        "latency|doctor" => &["admin", "slow"],
        "latency|graph" => &["admin", "slow"],
        "latency|histogram" => &["admin", "slow"],
        "latency|history" => &["admin", "slow"],
        "latency|latest" => &["admin", "slow"],
        "latency|reset" => &["admin", "slow"],
        "latency" => &["slow"],
        "lcs" => &["read", "slow", "string"],
        "lindex" => &["list", "read", "slow"],
        "linsert" => &["list", "slow", "write"],
        "llen" => &["fast", "list", "read"],
        "lmove" => &["list", "slow", "write"],
        "lmovem" => &["list", "slow", "write"],
        "lmpop" => &["list", "slow", "write"],
        "lolwut" => &["fast", "read"],
        "lpop" => &["fast", "list", "write"],
        "lpos" => &["list", "read", "slow"],
        "lpush" => &["fast", "list", "write"],
        "lpushx" => &["fast", "list", "write"],
        "lrange" => &["list", "read", "slow"],
        "lrem" => &["list", "slow", "write"],
        "lset" => &["list", "slow", "write"],
        "ltrim" => &["list", "slow", "write"],
        "memory|doctor" => &["slow"],
        "memory|malloc-stats" => &["slow"],
        "memory|purge" => &["slow"],
        "memory|stats" => &["slow"],
        "memory|usage" => &["read", "slow"],
        "memory" => &["slow"],
        "mget" => &["fast", "read", "string"],
        "migrate" => &["dangerous", "keyspace", "slow", "write"],
        "module|list" => &["admin", "slow"],
        "module|load" => &["admin", "slow"],
        "module|loadex" => &["admin", "slow"],
        "module|unload" => &["admin", "slow"],
        "module" => &["slow"],
        "monitor" => &["admin", "slow"],
        "move" => &["fast", "keyspace", "write"],
        "mset" => &["slow", "string", "write"],
        "msetex" => &["slow", "string", "write"],
        "msetnx" => &["slow", "string", "write"],
        "multi" => &["fast", "transaction"],
        "object|encoding" => &["keyspace", "read", "slow"],
        "object|freq" => &["keyspace", "read", "slow"],
        "object|idletime" => &["keyspace", "read", "slow"],
        "object|refcount" => &["keyspace", "read", "slow"],
        "object" => &["slow"],
        "persist" => &["fast", "keyspace", "write"],
        "pexpire" => &["fast", "keyspace", "write"],
        "pexpireat" => &["fast", "keyspace", "write"],
        "pexpiretime" => &["fast", "keyspace", "read"],
        "pfadd" => &["fast", "hyperloglog", "write"],
        "pfcount" => &["hyperloglog", "read", "slow"],
        "pfdebug" => &["admin", "hyperloglog", "slow", "write"],
        "pfmerge" => &["hyperloglog", "slow", "write"],
        "pfselftest" => &["admin", "hyperloglog", "slow"],
        "ping" => &["connection", "fast"],
        "psetex" => &["slow", "string", "write"],
        "psubscribe" => &["pubsub", "slow"],
        "psync" => &["admin", "slow"],
        "pttl" => &["fast", "keyspace", "read"],
        "publish" => &["fast", "pubsub"],
        "pubsub|channels" => &["pubsub", "slow"],
        "pubsub|numpat" => &["pubsub", "slow"],
        "pubsub|numsub" => &["pubsub", "slow"],
        "pubsub|shardchannels" => &["pubsub", "slow"],
        "pubsub|shardnumsub" => &["pubsub", "slow"],
        "pubsub" => &["slow"],
        "punsubscribe" => &["pubsub", "slow"],
        "quit" => &["connection", "fast"],
        "randomkey" => &["keyspace", "read", "slow"],
        "readonly" => &["connection", "fast"],
        "readwrite" => &["connection", "fast"],
        "rename" => &["keyspace", "slow", "write"],
        "renamenx" => &["fast", "keyspace", "write"],
        "replconf" => &["admin", "slow"],
        "replicaof" => &["admin", "slow"],
        "reset" => &["connection", "fast"],
        "restore-asking" => &["dangerous", "keyspace", "slow", "write"],
        "restore" => &["dangerous", "keyspace", "slow", "write"],
        "role" => &["admin", "dangerous", "fast"],
        "rpop" => &["fast", "list", "write"],
        "rpoplpush" => &["list", "slow", "write"],
        "rpush" => &["fast", "list", "write"],
        "rpushx" => &["fast", "list", "write"],
        "sadd" => &["fast", "set", "write"],
        "save" => &["admin", "slow"],
        "scan" => &["keyspace", "read", "slow"],
        "scard" => &["fast", "read", "set"],
        "script|debug" => &["scripting", "slow"],
        "script|exists" => &["scripting", "slow"],
        "script|flush" => &["scripting", "slow"],
        "script|kill" => &["scripting", "slow"],
        "script|load" => &["scripting", "slow"],
        "script" => &["slow"],
        "sdiff" => &["read", "set", "slow"],
        "sdiffcard" => &["read", "set", "slow"],
        "sdiffstore" => &["set", "slow", "write"],
        "select" => &["connection", "fast"],
        "set" => &["slow", "string", "write"],
        "setbit" => &["bitmap", "slow", "write"],
        "setex" => &["slow", "string", "write"],
        "setnx" => &["fast", "string", "write"],
        "setrange" => &["slow", "string", "write"],
        "shutdown" => &["admin", "slow"],
        "sinter" => &["read", "set", "slow"],
        "sintercard" => &["read", "set", "slow"],
        "sinterstore" => &["set", "slow", "write"],
        "sismember" => &["fast", "read", "set"],
        "slaveof" => &["admin", "slow"],
        "slowlog|get" => &["admin", "slow"],
        "slowlog|len" => &["admin", "slow"],
        "slowlog|reset" => &["admin", "slow"],
        "slowlog" => &["slow"],
        "smembers" => &["read", "set", "slow"],
        "smismember" => &["fast", "read", "set"],
        "smove" => &["fast", "set", "write"],
        "sort" => &["dangerous", "list", "set", "slow", "sortedset", "write"],
        "sort_ro" => &["dangerous", "list", "read", "set", "slow", "sortedset"],
        "spop" => &["fast", "set", "write"],
        "spublish" => &["fast", "pubsub"],
        "srandmember" => &["read", "set", "slow"],
        "srem" => &["fast", "set", "write"],
        "sscan" => &["read", "set", "slow"],
        "ssubscribe" => &["pubsub", "slow"],
        "strlen" => &["fast", "read", "string"],
        "subscribe" => &["pubsub", "slow"],
        "substr" => &["read", "slow", "string"],
        "sunion" => &["read", "set", "slow"],
        "sunioncard" => &["read", "set", "slow"],
        "sunionstore" => &["set", "slow", "write"],
        "sunsubscribe" => &["pubsub", "slow"],
        "swapdb" => &["dangerous", "fast", "keyspace", "write"],
        "sync" => &["admin", "slow"],
        "time" => &["fast"],
        "touch" => &["fast", "keyspace", "read"],
        "ttl" => &["fast", "keyspace", "read"],
        "type" => &["fast", "keyspace", "read"],
        "unlink" => &["fast", "keyspace", "write"],
        "unsubscribe" => &["pubsub", "slow"],
        "unwatch" => &["fast", "transaction"],
        "wait" => &["blocking", "connection", "slow"],
        "waitaof" => &["blocking", "connection", "slow"],
        "watch" => &["fast", "transaction"],
        "xack" => &["fast", "stream", "write"],
        "xackdel" => &["fast", "stream", "write"],
        "xadd" => &["fast", "stream", "write"],
        "xautoclaim" => &["fast", "stream", "write"],
        "xcfgset" => &["fast", "stream", "write"],
        "xclaim" => &["fast", "stream", "write"],
        "xdel" => &["fast", "stream", "write"],
        "xdelex" => &["fast", "stream", "write"],
        "xgroup|create" => &["slow", "stream", "write"],
        "xgroup|createconsumer" => &["slow", "stream", "write"],
        "xgroup|delconsumer" => &["slow", "stream", "write"],
        "xgroup|destroy" => &["slow", "stream", "write"],
        "xgroup|setid" => &["slow", "stream", "write"],
        "xgroup" => &["slow"],
        "xidmprecord" => &["fast", "stream", "write"],
        "xinfo|consumers" => &["read", "slow", "stream"],
        "xinfo|groups" => &["read", "slow", "stream"],
        "xinfo|stream" => &["read", "slow", "stream"],
        "xinfo" => &["slow"],
        "xlen" => &["fast", "read", "stream"],
        "xnack" => &["fast", "stream", "write"],
        "xpending" => &["read", "slow", "stream"],
        "xrange" => &["read", "slow", "stream"],
        "xread" => &["blocking", "read", "slow", "stream"],
        "xreadgroup" => &["blocking", "slow", "stream", "write"],
        "xrevrange" => &["read", "slow", "stream"],
        "xsetid" => &["fast", "stream", "write"],
        "xtrim" => &["slow", "stream", "write"],
        "zadd" => &["fast", "sortedset", "write"],
        "zcard" => &["fast", "read", "sortedset"],
        "zcount" => &["fast", "read", "sortedset"],
        "zdiff" => &["read", "slow", "sortedset"],
        "zdiffstore" => &["slow", "sortedset", "write"],
        "zincrby" => &["fast", "sortedset", "write"],
        "zinter" => &["read", "slow", "sortedset"],
        "zintercard" => &["read", "slow", "sortedset"],
        "zinterstore" => &["slow", "sortedset", "write"],
        "zlexcount" => &["fast", "read", "sortedset"],
        "zmpop" => &["slow", "sortedset", "write"],
        "zmscore" => &["fast", "read", "sortedset"],
        "zpopmax" => &["fast", "sortedset", "write"],
        "zpopmin" => &["fast", "sortedset", "write"],
        "zrandmember" => &["read", "slow", "sortedset"],
        "zrange" => &["read", "slow", "sortedset"],
        "zrangebylex" => &["read", "slow", "sortedset"],
        "zrangebyscore" => &["read", "slow", "sortedset"],
        "zrangestore" => &["slow", "sortedset", "write"],
        "zrank" => &["fast", "read", "sortedset"],
        "zrem" => &["fast", "sortedset", "write"],
        "zremrangebylex" => &["slow", "sortedset", "write"],
        "zremrangebyrank" => &["slow", "sortedset", "write"],
        "zremrangebyscore" => &["slow", "sortedset", "write"],
        "zrevrange" => &["read", "slow", "sortedset"],
        "zrevrangebylex" => &["read", "slow", "sortedset"],
        "zrevrangebyscore" => &["read", "slow", "sortedset"],
        "zrevrank" => &["fast", "read", "sortedset"],
        "zscan" => &["read", "slow", "sortedset"],
        "zscore" => &["fast", "read", "sortedset"],
        "zunion" => &["read", "slow", "sortedset"],
        "zunionstore" => &["slow", "sortedset", "write"],
        _ => &[],
    }
}
fn keyword(args: &[Bytes], word: &[u8], start: i64) -> usize {
    let start = if start < 0 {
        args.len().saturating_sub(start.unsigned_abs() as usize)
    } else {
        start as usize
    };
    args.iter()
        .enumerate()
        .skip(start)
        .find(|(_, arg)| arg.eq_ignore_ascii_case(word))
        .map_or(usize::MAX, |(index, _)| index + 1)
}
fn in_range(
    args: &[Bytes],
    position: usize,
    start: usize,
    last: i64,
    step: usize,
    limit: usize,
) -> bool {
    if start >= args.len() || position < start || step == 0 {
        return false;
    }
    let end = if last < 0 {
        args.len().saturating_sub(last.unsigned_abs() as usize)
    } else {
        start.saturating_add(last as usize)
    };
    if end < start {
        return false;
    }
    let end = (end + 1 - start)
        .checked_div(limit)
        .map_or(end, |count| start + count.saturating_sub(1));
    position <= end && (position - start).is_multiple_of(step)
}
fn in_count(
    args: &[Bytes],
    position: usize,
    start: usize,
    count_index: usize,
    first: usize,
    step: usize,
) -> bool {
    let Some(count) = args
        .get(start.saturating_add(count_index))
        .and_then(|value| super::integer(value))
        .and_then(|value| usize::try_from(value).ok())
    else {
        return false;
    };
    let first = start.saturating_add(first);
    position >= first
        && step > 0
        && (position - first).is_multiple_of(step)
        && (position - first) / step < count
}
pub(super) fn key_flags(name: &str, args: &[Bytes], position: usize) -> &'static [&'static str] {
    if name == "set" && position == 1 {
        return if args[3..].iter().any(|arg| arg.eq_ignore_ascii_case(b"GET")) {
            &["RW", "access", "update"]
        } else {
            &["OW", "update"]
        };
    }
    match name {
        "append" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "arcount" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "ardel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "ardelrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "arget" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "argetrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "argrep" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arinfo" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arinsert" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "arlastitems" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arlen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "armget" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "armset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "arnext" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arop" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arring" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "arscan" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "arseek" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "arset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "bitcount" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "bitfield" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update", "access", "variable_flags"];
            }
        }
        "bitfield_ro" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "bitop" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 3usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "bitpos" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "blmove" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "blmovem" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "blmpop" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RW", "access", "delete"];
            }
        }
        "blpop" => {
            if in_range(args, position, 1usize, -2, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "brpop" => {
            if in_range(args, position, 1usize, -2, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "brpoplpush" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "bzmpop" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RW", "access", "delete"];
            }
        }
        "bzpopmax" => {
            if in_range(args, position, 1usize, -2, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "bzpopmin" => {
            if in_range(args, position, 1usize, -2, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "copy" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "decr" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "decrby" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "del" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RM", "delete"];
            }
        }
        "delex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete", "variable_flags"];
            }
        }
        "digest" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "dump" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "eval" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RW", "access", "update"];
            }
        }
        "eval_ro" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "evalsha" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RW", "access", "update"];
            }
        }
        "evalsha_ro" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "exists" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO"];
            }
        }
        "expire" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "expireat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "expiretime" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "fcall" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RW", "access", "update"];
            }
        }
        "fcall_ro" => {
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "geoadd" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "geodist" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "geohash" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "geopos" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "georadius" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
            if in_range(args, position, keyword(args, b"STORE", 6), 0, 1, 0) {
                return &["OW", "update", "incomplete"];
            }
            if in_range(args, position, keyword(args, b"STOREDIST", 6), 0, 1, 0) {
                return &["OW", "update", "incomplete"];
            }
        }
        "georadius_ro" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "georadiusbymember" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
            if in_range(args, position, keyword(args, b"STORE", 5), 0, 1, 0) {
                return &["OW", "update", "incomplete"];
            }
            if in_range(args, position, keyword(args, b"STOREDIST", 5), 0, 1, 0) {
                return &["OW", "update", "incomplete"];
            }
        }
        "georadiusbymember_ro" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "geosearch" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "geosearchstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "get" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "getbit" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "getdel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "getex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "getrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "getset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "hdel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "hexists" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "hexpire" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hexpireat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hexpiretime" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hget" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hgetall" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hgetdel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "hgetex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "himport|set" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "hincrby" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "hincrbyfloat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "hkeys" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hlen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "hmget" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hmset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hpersist" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hpexpire" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hpexpireat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hpexpiretime" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hpttl" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hrandfield" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hscan" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hsetex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "hsetnx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "hstrlen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "httl" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "hvals" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "incr" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "incrby" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "incrbyfloat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "increx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "lcs" => {
            if in_range(args, position, 1usize, 1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "lindex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "linsert" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "llen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "lmove" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "lmovem" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "lmpop" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RW", "access", "delete"];
            }
        }
        "lpop" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "lpos" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "lpush" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "lpushx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "lrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "lrem" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "lset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "ltrim" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "memory|usage" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "mget" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "migrate" => {
            if in_range(args, position, 3usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, keyword(args, b"KEYS", -2), -1, 1, 0) {
                return &["RW", "access", "delete", "incomplete"];
            }
        }
        "move" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "mset" => {
            if in_range(args, position, 1usize, -1, 2, 0) {
                return &["OW", "update"];
            }
        }
        "msetex" => {
            if in_count(args, position, 1usize, 0, 1, 2) {
                return &["OW", "update"];
            }
        }
        "msetnx" => {
            if in_range(args, position, 1usize, -1, 2, 0) {
                return &["OW", "insert"];
            }
        }
        "object|encoding" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "object|freq" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "object|idletime" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "object|refcount" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "persist" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "pexpire" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "pexpireat" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "pexpiretime" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "pfadd" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "pfcount" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RW", "access"];
            }
        }
        "pfdebug" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "access"];
            }
        }
        "pfmerge" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "insert"];
            }
            if in_range(args, position, 2usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "psetex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "pttl" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "rename" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "renamenx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["OW", "insert"];
            }
        }
        "restore-asking" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "restore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "rpop" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "rpoplpush" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "rpush" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "rpushx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "sadd" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "scard" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "sdiff" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sdiffcard" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "sdiffstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 2usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "set" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update", "variable_flags"];
            }
        }
        "setbit" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "setex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
        }
        "setnx" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "insert"];
            }
        }
        "setrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "sinter" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sintercard" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "sinterstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 2usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sismember" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "smembers" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "smismember" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "smove" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "sort" => {
            return if position == 1 {
                &["RO", "access"]
            } else {
                &["OW", "update"]
            };
        }
        "sort_ro" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "spop" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "spublish" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["not_key"];
            }
        }
        "srandmember" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "srem" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "sscan" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "ssubscribe" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["not_key"];
            }
        }
        "strlen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "substr" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sunion" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sunioncard" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "sunionstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 2usize, -1, 1, 0) {
                return &["RO", "access"];
            }
        }
        "sunsubscribe" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["not_key"];
            }
        }
        "touch" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO"];
            }
        }
        "ttl" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "type" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "unlink" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RM", "delete"];
            }
        }
        "watch" => {
            if in_range(args, position, 1usize, -1, 1, 0) {
                return &["RO"];
            }
        }
        "xack" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xackdel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update", "delete"];
            }
        }
        "xadd" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xautoclaim" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "xcfgset" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xclaim" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xdel" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "xdelex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "xgroup|create" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "xgroup|createconsumer" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "insert"];
            }
        }
        "xgroup|delconsumer" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "xgroup|destroy" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "xgroup|setid" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xidmprecord" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xinfo|consumers" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xinfo|groups" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xinfo|stream" => {
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xlen" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "xnack" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xpending" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xread" => {
            if in_range(args, position, keyword(args, b"STREAMS", 1), -1, 1, 2) {
                return &["RO", "access", "incomplete"];
            }
        }
        "xreadgroup" => {
            if in_range(args, position, keyword(args, b"STREAMS", 4), -1, 1, 2) {
                return &["RO", "access", "incomplete"];
            }
        }
        "xrevrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "xsetid" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "xtrim" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "zadd" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "update"];
            }
        }
        "zcard" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO"];
            }
        }
        "zcount" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zdiff" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zdiffstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zincrby" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "update"];
            }
        }
        "zinter" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zintercard" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zinterstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zlexcount" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zmpop" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RW", "access", "delete"];
            }
        }
        "zmscore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zpopmax" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "zpopmin" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "access", "delete"];
            }
        }
        "zrandmember" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrangebylex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrangebyscore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrangestore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_range(args, position, 2usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrank" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrem" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "zremrangebylex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "zremrangebyrank" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "zremrangebyscore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RW", "delete"];
            }
        }
        "zrevrange" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrevrangebylex" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrevrangebyscore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zrevrank" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zscan" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zscore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["RO", "access"];
            }
        }
        "zunion" => {
            if in_count(args, position, 1usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        "zunionstore" => {
            if in_range(args, position, 1usize, 0, 1, 0) {
                return &["OW", "update"];
            }
            if in_count(args, position, 2usize, 0, 1, 1) {
                return &["RO", "access"];
            }
        }
        _ => {}
    }
    &["RW", "access", "update"]
}
pub(super) fn json_response(value: &Value) -> Resp {
    match value {
        Value::Null => Resp::Bulk(None),
        Value::Bool(value) => Resp::Boolean(*value),
        Value::String(value) => Resp::Bulk(Some(Bytes::copy_from_slice(value.as_bytes()))),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                Resp::Integer(value)
            } else if let Some(value) = value.as_u64() {
                Resp::Unsigned(value)
            } else {
                Resp::Double(value.as_f64().unwrap_or(f64::NAN))
            }
        }
        Value::Array(values) => array(values.iter().map(json_response).collect()),
        Value::Object(values) => Resp::Map(
            values
                .iter()
                .map(|(key, value)| {
                    (
                        Resp::Bulk(Some(Bytes::copy_from_slice(key.as_bytes()))),
                        json_response(value),
                    )
                })
                .collect(),
        ),
    }
}
pub(super) fn group(name: &str) -> Option<&'static str> {
    Some(match info(name)?.group.as_str() {
        "String commands" => "string",
        "Hash commands" => "hash",
        "List commands" => "list",
        "Set commands" => "set",
        "Sorted set commands" => "sortedset",
        "Stream commands" => "stream",
        "Bitmap commands" => "bitmap",
        "Array commands" => "array",
        "HyperLogLog commands" => "hyperloglog",
        "Geospatial commands" => "geo",
        "JSON commands" => "json",
        "Search commands" => "search",
        "Time series commands" => "timeseries",
        "Vector set commands" => "vector",
        "Pub/Sub commands" => "pubsub",
        "Transaction commands" => "transaction",
        "Scripting commands" => "scripting",
        "Connection commands" => "connection",
        "Server commands" => "server",
        "Cluster commands" => "cluster",
        "Generic commands" => "keyspace",
        _ => return None,
    })
}

fn fields(values: impl IntoIterator<Item = (&'static str, Resp)>) -> Resp {
    Resp::Map(
        values
            .into_iter()
            .map(|(name, value)| (bulk(name), value))
            .collect(),
    )
}

fn key_spec(value: &Value) -> Resp {
    let flags = value
        .get("flags")
        .and_then(Value::as_array)
        .map(|values| {
            array(
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|flag| {
                        bulk(if matches!(flag, "RO" | "RW" | "OW" | "RM") {
                            flag.to_owned()
                        } else {
                            flag.to_ascii_lowercase()
                        })
                    })
                    .collect(),
            )
        })
        .unwrap_or_else(|| array(Vec::new()));
    fn search(value: Option<&Value>, begin: bool) -> Resp {
        let Some(value) = value.and_then(Value::as_object) else {
            return fields([("type", bulk("unknown")), ("spec", fields([]))]);
        };
        let Some((kind, body)) = value.iter().next() else {
            return fields([("type", bulk("unknown")), ("spec", fields([]))]);
        };
        let body = if begin && kind == "index" {
            fields([(
                "index",
                body.get("pos")
                    .map(json_response)
                    .unwrap_or(Resp::Integer(0)),
            )])
        } else if !begin {
            let mut body = body.clone();
            if let Some(object) = body.as_object_mut()
                && let Some(step) = object.remove("step")
            {
                object.insert("keystep".into(), step);
            }
            json_response(&body)
        } else {
            json_response(body)
        };
        fields([("type", bulk(kind.clone())), ("spec", body)])
    }
    fields([
        ("flags", flags),
        ("begin_search", search(value.get("begin_search"), true)),
        ("find_keys", search(value.get("find_keys"), false)),
    ])
}

pub(super) fn describe(spec: &CommandSpec) -> Resp {
    let metadata = metadata(spec.name);
    let (first, last, step) = spec.keys;
    let key_specs = metadata
        .and_then(|value| value.get("key_specs"))
        .and_then(Value::as_array)
        .map(|values| values.iter().map(key_spec).collect())
        .unwrap_or_else(|| {
            if first == 0 {
                Vec::new()
            } else {
                vec![fields([
                    (
                        "flags",
                        super::strings(if spec.flags.contains(&"readonly") {
                            &["RO", "access"]
                        } else {
                            &["RW", "access", "update"]
                        }),
                    ),
                    (
                        "begin_search",
                        fields([
                            ("type", bulk("index")),
                            ("spec", fields([("index", Resp::Integer(first))])),
                        ]),
                    ),
                    (
                        "find_keys",
                        fields([
                            ("type", bulk("range")),
                            (
                                "spec",
                                fields([
                                    (
                                        "lastkey",
                                        Resp::Integer(if last < 0 { last } else { last - first }),
                                    ),
                                    ("keystep", Resp::Integer(step)),
                                    ("limit", Resp::Integer(0)),
                                ]),
                            ),
                        ]),
                    ),
                ])]
            }
        });
    let flags = metadata
        .and_then(|value| value.get("command_flags"))
        .and_then(Value::as_array)
        .map(|values| {
            array(
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .filter(|flag| !matches!(*flag, "SENTINEL" | "ONLY_SENTINEL"))
                    .map(|flag| bulk(flag.to_ascii_lowercase()))
                    .collect(),
            )
        })
        .unwrap_or_else(|| super::strings(spec.flags));
    let categories = categories(spec.name);
    let categories = if categories.is_empty() {
        super::authorization::module_category(spec.name)
            .into_iter()
            .map(|category| bulk(format!("@{category}")))
            .collect()
    } else {
        categories
            .iter()
            .map(|category| bulk(format!("@{category}")))
            .collect()
    };
    let tips = metadata
        .and_then(|value| value.get("command_tips"))
        .map(json_response)
        .unwrap_or_else(|| array(Vec::new()));
    let children = super::subcommand_specifications()
        .filter(|child| {
            child
                .name
                .strip_prefix(spec.name)
                .is_some_and(|suffix| suffix.starts_with('|'))
        })
        .map(describe)
        .collect();
    array(vec![
        bulk(spec.name),
        Resp::Integer(spec.arity),
        flags,
        Resp::Integer(first),
        Resp::Integer(last),
        Resp::Integer(step),
        array(categories),
        tips,
        array(key_specs),
        array(children),
    ])
}

fn documentation(spec: &CommandSpec) -> Resp {
    let mut values = Vec::new();
    if let Some(value) = metadata(spec.name) {
        for name in [
            "summary",
            "since",
            "group",
            "complexity",
            "history",
            "replaced_by",
            "deprecated_since",
            "arguments",
        ] {
            if let Some(value) = value.get(name) {
                values.push((bulk(name), json_response(value)));
            }
        }
        if let Some(flags) = value.get("doc_flags").and_then(Value::as_array) {
            values.push((
                bulk("doc_flags"),
                array(
                    flags
                        .iter()
                        .filter_map(Value::as_str)
                        .map(|flag| bulk(flag.to_ascii_lowercase()))
                        .collect(),
                ),
            ));
        }
    } else if let Some(info) = info(spec.name) {
        values.extend([
            (bulk("summary"), bulk(info.description.clone())),
            (bulk("since"), bulk(info.since.clone())),
            (bulk("group"), bulk(group(spec.name).unwrap_or("module"))),
        ]);
        let arguments = info
            .syntax
            .split_whitespace()
            .skip(if spec.name.contains('|') { 2 } else { 1 })
            .enumerate()
            .map(|(index, name)| {
                fields([
                    (
                        "name",
                        bulk(name.trim_matches(['[', ']', '<', '>']).to_owned()),
                    ),
                    (
                        "type",
                        bulk(if spec.keys.0 > 0 && index + 1 == spec.keys.0 as usize {
                            "key"
                        } else {
                            "string"
                        }),
                    ),
                ])
            })
            .collect();
        values.push((bulk("arguments"), array(arguments)));
    } else {
        values.extend([
            (bulk("summary"), bulk("ffkv extension command")),
            (bulk("group"), bulk("module")),
        ]);
    }
    let children: Vec<_> = super::subcommand_specifications()
        .filter(|child| {
            child
                .name
                .strip_prefix(spec.name)
                .is_some_and(|suffix| suffix.starts_with('|'))
        })
        .map(|child| (bulk(child.name), documentation(child)))
        .collect();
    if !children.is_empty() {
        values.push((bulk("subcommands"), Resp::Map(children)));
    }
    Resp::Map(values)
}

pub(super) fn dispatch(engine: &Engine, args: &[Bytes]) -> Resp {
    let dynamic = engine
        .extensions
        .as_ref()
        .map(|extensions| extensions.command_metadata_sync())
        .unwrap_or_default();
    if args.len() == 1 {
        return super::bounded_replies(
            descriptors::roots(&dynamic).map(|spec| spec.describe()),
            engine.reply_limit,
        );
    }
    if args[1].eq_ignore_ascii_case(b"COUNT") {
        return Resp::Integer((super::specifications().count() + dynamic.len()) as i64);
    }
    if args[1].eq_ignore_ascii_case(b"INFO") {
        return if args.len() == 2 {
            super::bounded_replies(
                descriptors::roots(&dynamic).map(|spec| spec.describe()),
                engine.reply_limit,
            )
        } else {
            super::bounded_replies(
                args[2..].iter().map(|name| {
                    descriptors::lookup(&dynamic, name)
                        .map_or(Resp::Bulk(None), |spec| spec.describe())
                }),
                engine.reply_limit,
            )
        };
    }
    if args[1].eq_ignore_ascii_case(b"GETKEYS") || args[1].eq_ignore_ascii_case(b"GETKEYSANDFLAGS")
    {
        let command = &args[2..];
        let with_flags = args[1].eq_ignore_ascii_case(b"GETKEYSANDFLAGS");
        if let Some(name) = command.first()
            && let Some(descriptors::Descriptor::Module(spec)) = descriptors::lookup(&dynamic, name)
        {
            return descriptors::module_keys(spec, command, with_flags, engine.reply_limit);
        }
        let mut bound = super::BoundCommand::bind(command);
        if let Err(reply) = bound.check() {
            return reply;
        }
        let spec = bound.spec().expect("checked command is registered");
        let Some(positions) = bound.access_positions() else {
            return if spec.flags.contains(&"no_mandatory_keys") {
                array(Vec::new())
            } else {
                error("ERR The command has no key arguments")
            };
        };
        return super::bounded_replies(
            positions.map(|position| {
                let key = Resp::Bulk(Some(command[position].clone()));
                if with_flags {
                    array(vec![
                        key,
                        super::strings(key_flags(spec.name, command, position)),
                    ])
                } else {
                    key
                }
            }),
            engine.reply_limit,
        );
    }
    if args[1].eq_ignore_ascii_case(b"DOCS") {
        let rows = if args.len() == 2 {
            descriptors::roots(&dynamic)
                .map(|spec| (bulk(spec.name().to_owned()), spec.documentation()))
                .collect()
        } else {
            args[2..]
                .iter()
                .filter_map(|name| descriptors::lookup(&dynamic, name))
                .map(|spec| (bulk(spec.name().to_owned()), spec.documentation()))
                .collect()
        };
        let reply = Resp::Map(rows);
        return match super::ReplyBudget::new(engine.reply_limit).response(&reply) {
            Ok(()) => reply,
            Err(error) => error,
        };
    }
    if args[1].eq_ignore_ascii_case(b"LIST") {
        let filter = if args.len() == 2 {
            None
        } else if args.len() == 5 && args[2].eq_ignore_ascii_case(b"FILTERBY") {
            Some((&args[3], &args[4]))
        } else {
            return syntax_error();
        };
        if filter.is_some_and(|(kind, _)| {
            ![b"PATTERN".as_slice(), b"ACLCAT", b"MODULE"]
                .iter()
                .any(|value| kind.eq_ignore_ascii_case(value))
        }) {
            return syntax_error();
        }
        return super::bounded_replies(
            descriptors::all(&dynamic)
                .filter(|spec| match filter {
                    None => true,
                    Some((kind, value)) if kind.eq_ignore_ascii_case(b"PATTERN") => {
                        super::scan::glob_matches(value, spec.name().as_bytes())
                    }
                    Some((kind, value)) if kind.eq_ignore_ascii_case(b"ACLCAT") => {
                        std::str::from_utf8(value).is_ok_and(|category| spec.category(category))
                    }
                    Some((_, value)) => spec.module(value),
                })
                .map(|spec| bulk(spec.name().to_owned())),
            engine.reply_limit,
        );
    }
    error("ERR unknown COMMAND subcommand")
}
