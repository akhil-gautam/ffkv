//! Compiled Redis 8.10 key specifications. No metadata DOM or heap work on ingress.
use super::{Resp, error, integer};
use bytes::Bytes;
use std::{iter::StepBy, ops::Range};

#[derive(Clone)]
pub(super) struct KeyPositions {
    ranges: [StepBy<Range<usize>>; 3],
    front: usize,
    back: usize,
    remaining: usize,
}
impl KeyPositions {
    pub(super) fn new(spans: [(usize, usize, usize); 3]) -> Self {
        let ranges = spans.map(|(first, end, step)| (first..end).step_by(step));
        let remaining = ranges.iter().map(ExactSizeIterator::len).sum();
        Self {
            ranges,
            front: 0,
            back: 3,
            remaining,
        }
    }
}
impl Iterator for KeyPositions {
    type Item = usize;
    fn next(&mut self) -> Option<usize> {
        while self.front < self.back {
            if let Some(position) = self.ranges[self.front].next() {
                self.remaining -= 1;
                return Some(position);
            }
            self.front += 1;
        }
        None
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}
impl DoubleEndedIterator for KeyPositions {
    fn next_back(&mut self) -> Option<usize> {
        while self.front < self.back {
            if let Some(position) = self.ranges[self.back - 1].next_back() {
                self.remaining -= 1;
                return Some(position);
            }
            self.back -= 1;
        }
        None
    }
}
impl ExactSizeIterator for KeyPositions {}
#[derive(Clone, Copy)]
enum Begin {
    Index(usize),
}
#[derive(Clone, Copy)]
enum Find {
    Range { last: isize, step: usize },
    Count { step: usize },
}
#[derive(Clone, Copy)]
struct Spec {
    begin: Begin,
    find: Find,
}

pub(super) fn compiled(name: &str, args: &[Bytes]) -> Option<Result<Option<KeyPositions>, Resp>> {
    if matches!(
        name,
        "eval" | "eval_ro" | "evalsha" | "evalsha_ro" | "fcall" | "fcall_ro"
    ) {
        let count = match args.get(2).and_then(|value| integer(value)) {
            Some(count) if count < 0 => {
                return Some(Err(error("ERR Number of keys can't be negative")));
            }
            Some(count) => count as u64,
            None => {
                return Some(Err(error("ERR value is not an integer or out of range")));
            }
        };
        if count > args.len().saturating_sub(3) as u64 {
            return Some(Err(error(
                "ERR Number of keys can't be greater than number of args",
            )));
        }
    }
    let specs: &[Spec] = match name {
        "acl" => &[],
        "acl|cat" => &[],
        "acl|deluser" => &[],
        "acl|dryrun" => &[],
        "acl|genpass" => &[],
        "acl|getuser" => &[],
        "acl|list" => &[],
        "acl|load" => &[],
        "acl|log" => &[],
        "acl|save" => &[],
        "acl|setuser" => &[],
        "acl|users" => &[],
        "acl|whoami" => &[],
        "append" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arcount" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "ardel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "ardelrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arget" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "argetrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "argrep" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arinfo" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arinsert" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arlastitems" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arlen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "armget" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "armset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arnext" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arring" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arscan" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arseek" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "arset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "asking" => &[],
        "auth" => &[],
        "backup" => &[],
        "backup|abort" => &[],
        "backup|cleanup" => &[],
        "backup|list" => &[],
        "backup|seal" => &[],
        "backup|start" => &[],
        "backup|status" => &[],
        "bgrewriteaof" => &[],
        "bgsave" => &[],
        "bitcount" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "bitfield" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "bitfield_ro" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "bitop" => &[
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(3),
                find: Find::Range { last: -1, step: 1 },
            },
        ],
        "bitpos" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "blmove" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "blmovem" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "blmpop" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "blpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -2, step: 1 },
        }],
        "brpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -2, step: 1 },
        }],
        "brpoplpush" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "bzmpop" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "bzpopmax" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -2, step: 1 },
        }],
        "bzpopmin" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -2, step: 1 },
        }],
        "client" => &[],
        "client|caching" => &[],
        "client|getname" => &[],
        "client|getredir" => &[],
        "client|id" => &[],
        "client|info" => &[],
        "client|kill" => &[],
        "client|list" => &[],
        "client|no-evict" => &[],
        "client|no-touch" => &[],
        "client|pause" => &[],
        "client|reply" => &[],
        "client|setinfo" => &[],
        "client|setname" => &[],
        "client|tracking" => &[],
        "client|trackinginfo" => &[],
        "client|unblock" => &[],
        "client|unpause" => &[],
        "cluster" => &[],
        "cluster|addslots" => &[],
        "cluster|addslotsrange" => &[],
        "cluster|bumpepoch" => &[],
        "cluster|count-failure-reports" => &[],
        "cluster|countkeysinslot" => &[],
        "cluster|delslots" => &[],
        "cluster|delslotsrange" => &[],
        "cluster|failover" => &[],
        "cluster|flushslots" => &[],
        "cluster|forget" => &[],
        "cluster|getkeysinslot" => &[],
        "cluster|info" => &[],
        "cluster|keyslot" => &[],
        "cluster|links" => &[],
        "cluster|meet" => &[],
        "cluster|migration" => &[],
        "cluster|myid" => &[],
        "cluster|myshardid" => &[],
        "cluster|nodes" => &[],
        "cluster|replicas" => &[],
        "cluster|replicate" => &[],
        "cluster|reset" => &[],
        "cluster|saveconfig" => &[],
        "cluster|set-config-epoch" => &[],
        "cluster|setslot" => &[],
        "cluster|shards" => &[],
        "cluster|slaves" => &[],
        "cluster|slot-stats" => &[],
        "cluster|slots" => &[],
        "command" => &[],
        "command|count" => &[],
        "command|docs" => &[],
        "command|getkeys" => &[],
        "command|getkeysandflags" => &[],
        "command|info" => &[],
        "command|list" => &[],
        "config" => &[],
        "config|get" => &[],
        "config|resetstat" => &[],
        "config|rewrite" => &[],
        "config|set" => &[],
        "copy" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "dbsize" => &[],
        "decr" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "decrby" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "del" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "delex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "digest" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "discard" => &[],
        "dump" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "echo" => &[],
        "eval" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "eval_ro" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "evalsha" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "evalsha_ro" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "exec" => &[],
        "exists" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "expire" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "expireat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "expiretime" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "failover" => &[],
        "fcall" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "fcall_ro" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Count { step: 1 },
        }],
        "flushall" => &[],
        "flushdb" => &[],
        "function" => &[],
        "function|delete" => &[],
        "function|dump" => &[],
        "function|flush" => &[],
        "function|kill" => &[],
        "function|list" => &[],
        "function|load" => &[],
        "function|restore" => &[],
        "function|stats" => &[],
        "geoadd" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "geodist" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "geohash" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "geopos" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "georadius_ro" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "georadiusbymember_ro" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "geosearch" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "geosearchstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "get" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "getbit" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "getdel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "getex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "getrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "getset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hdel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hello" => &[],
        "hexists" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hexpire" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hexpireat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hexpiretime" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hget" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hgetall" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hgetdel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hgetex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "himport" => &[],
        "himport|discard" => &[],
        "himport|discardall" => &[],
        "himport|prepare" => &[],
        "himport|set" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hincrby" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hincrbyfloat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hkeys" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hlen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hmget" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hmset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hotkeys" => &[],
        "hotkeys|get" => &[],
        "hotkeys|reset" => &[],
        "hotkeys|start" => &[],
        "hotkeys|stop" => &[],
        "hpersist" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hpexpire" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hpexpireat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hpexpiretime" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hpttl" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hrandfield" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hscan" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hsetex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hsetnx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hstrlen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "httl" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "hvals" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "incr" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "incrby" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "incrbyfloat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "increx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "info" => &[],
        "keys" => &[],
        "lastsave" => &[],
        "latency" => &[],
        "latency|doctor" => &[],
        "latency|graph" => &[],
        "latency|histogram" => &[],
        "latency|history" => &[],
        "latency|latest" => &[],
        "latency|reset" => &[],
        "lcs" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 1, step: 1 },
        }],
        "lindex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "linsert" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "llen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lmove" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "lmovem" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "lmpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "lolwut" => &[],
        "lpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lpos" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lpush" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lpushx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lrem" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "lset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "ltrim" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "memory" => &[],
        "memory|doctor" => &[],
        "memory|malloc-stats" => &[],
        "memory|purge" => &[],
        "memory|stats" => &[],
        "memory|usage" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "mget" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "module" => &[],
        "module|list" => &[],
        "module|load" => &[],
        "module|loadex" => &[],
        "module|unload" => &[],
        "monitor" => &[],
        "move" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "mset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 2 },
        }],
        "msetex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 2 },
        }],
        "msetnx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 2 },
        }],
        "multi" => &[],
        "object" => &[],
        "object|encoding" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "object|freq" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "object|idletime" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "object|refcount" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "persist" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pexpire" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pexpireat" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pexpiretime" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pfadd" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pfcount" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "pfdebug" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "pfmerge" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: -1, step: 1 },
            },
        ],
        "pfselftest" => &[],
        "ping" => &[],
        "psetex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "psubscribe" => &[],
        "psync" => &[],
        "pttl" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "publish" => &[],
        "pubsub" => &[],
        "pubsub|channels" => &[],
        "pubsub|numpat" => &[],
        "pubsub|numsub" => &[],
        "pubsub|shardchannels" => &[],
        "pubsub|shardnumsub" => &[],
        "punsubscribe" => &[],
        "quit" => &[],
        "randomkey" => &[],
        "readonly" => &[],
        "readwrite" => &[],
        "rename" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "renamenx" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "replconf" => &[],
        "replicaof" => &[],
        "reset" => &[],
        "restore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "restore-asking" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "role" => &[],
        "rpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "rpoplpush" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "rpush" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "rpushx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "sadd" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "save" => &[],
        "scan" => &[],
        "scard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "script" => &[],
        "script|debug" => &[],
        "script|exists" => &[],
        "script|flush" => &[],
        "script|kill" => &[],
        "script|load" => &[],
        "sdiff" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "sdiffcard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "sdiffstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: -1, step: 1 },
            },
        ],
        "select" => &[],
        "set" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "setbit" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "setex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "setnx" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "setrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "shutdown" => &[],
        "sinter" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "sintercard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "sinterstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: -1, step: 1 },
            },
        ],
        "sismember" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "slaveof" => &[],
        "slowlog" => &[],
        "slowlog|get" => &[],
        "slowlog|len" => &[],
        "slowlog|reset" => &[],
        "smembers" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "smismember" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "smove" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "spop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "spublish" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "srandmember" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "srem" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "sscan" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "ssubscribe" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "strlen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "subscribe" => &[],
        "substr" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "sunion" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "sunioncard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "sunionstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: -1, step: 1 },
            },
        ],
        "sunsubscribe" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "swapdb" => &[],
        "sync" => &[],
        "time" => &[],
        "touch" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "ttl" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "type" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "unlink" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "unsubscribe" => &[],
        "unwatch" => &[],
        "wait" => &[],
        "waitaof" => &[],
        "watch" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: -1, step: 1 },
        }],
        "xack" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xackdel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xadd" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xautoclaim" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xcfgset" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xclaim" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xdel" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xdelex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xgroup" => &[],
        "xgroup|create" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xgroup|createconsumer" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xgroup|delconsumer" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xgroup|destroy" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xgroup|setid" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xidmprecord" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xinfo" => &[],
        "xinfo|consumers" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xinfo|groups" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xinfo|stream" => &[Spec {
            begin: Begin::Index(2),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xlen" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xnack" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xpending" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xrevrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xsetid" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "xtrim" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zadd" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zcard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zcount" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zdiff" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "zdiffstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Count { step: 1 },
            },
        ],
        "zincrby" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zinter" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "zintercard" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "zinterstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Count { step: 1 },
            },
        ],
        "zlexcount" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zmpop" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "zmscore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zpopmax" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zpopmin" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrandmember" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrangebylex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrangebyscore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrangestore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Range { last: 0, step: 1 },
            },
        ],
        "zrank" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrem" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zremrangebylex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zremrangebyrank" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zremrangebyscore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrevrange" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrevrangebylex" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrevrangebyscore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zrevrank" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zscan" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zscore" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Range { last: 0, step: 1 },
        }],
        "zunion" => &[Spec {
            begin: Begin::Index(1),
            find: Find::Count { step: 1 },
        }],
        "zunionstore" => &[
            Spec {
                begin: Begin::Index(1),
                find: Find::Range { last: 0, step: 1 },
            },
            Spec {
                begin: Begin::Index(2),
                find: Find::Count { step: 1 },
            },
        ],
        _ => return None,
    };
    Some(resolve(specs, args))
}
fn resolve(specs: &[Spec], args: &[Bytes]) -> Result<Option<KeyPositions>, Resp> {
    let invalid = || error("ERR Invalid arguments specified for command");
    let mut spans = [(0, 0, 1); 3];
    for (span, spec) in spans.iter_mut().zip(specs) {
        let Begin::Index(begin) = spec.begin;
        if begin >= args.len() {
            // Optional trailing ranges (PFMERGE destkey without sources) are
            // absent, not malformed. Command arity guards mandatory ranges.
            if matches!(spec.find, Find::Range { .. }) {
                continue;
            }
            return Err(invalid());
        }
        *span = match spec.find {
            Find::Range { last, step } => {
                let last = if last < 0 {
                    args.len().checked_add_signed(last)
                } else {
                    begin.checked_add(last as usize)
                }
                .ok_or_else(invalid)?;
                if last < begin || last >= args.len() {
                    return Err(invalid());
                }
                (begin, last + 1, step)
            }
            Find::Count { step } => {
                let count = args
                    .get(begin)
                    .and_then(|value| integer(value))
                    .and_then(|n| usize::try_from(n).ok())
                    .ok_or_else(invalid)?;
                if count == 0 {
                    continue;
                }
                let first = begin.checked_add(1).ok_or_else(invalid)?;
                let last = (count - 1)
                    .checked_mul(step)
                    .and_then(|offset| first.checked_add(offset))
                    .ok_or_else(invalid)?;
                if last >= args.len() {
                    return Err(invalid());
                }
                (first, last + 1, step)
            }
        };
    }
    let positions = KeyPositions::new(spans);
    Ok((positions.len() != 0).then_some(positions))
}
