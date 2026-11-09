//! One playground client: the state a server connection keeps (protocol
//! version, selected database, MULTI queue, WATCH tokens, Pub/Sub
//! subscriptions, name), routed with the server's own connection rules.
//! Mirrors `Connection::route` without ACLs, cluster mode or server state.
use std::sync::Arc;

use bytes::Bytes;

use crate::{
    connection::rules::{
        Transaction, allowed_while_subscribed, auth_credentials, client_setinfo, hello_reply,
        invalid_client_name, is_subscription_command, no_password_configured, parse_hello,
        printable, subscribed_mode_error, subscribed_ping,
    },
    engine::{BoundCommand, Routing},
    protocol::{Resp, Version},
    pubsub::{Hub, Subscription},
    session::ClientSession,
    watch::Token,
};

const MAX_WATCHED_KEYS: usize = 1024;

/// Root commands that need a subsystem the playground does not include: Lua
/// scripting and functions, persistence and backups, replication, cluster,
/// server administration and telemetry, ACLs, MONITOR. Lowercase registered
/// names; every listed root answers [`not_available`].
pub(crate) const UNAVAILABLE: &[&str] = &[
    "acl",
    "asking",
    "backup",
    "bgrewriteaof",
    "bgsave",
    "cluster",
    "config",
    "eval",
    "eval_ro",
    "evalsha",
    "evalsha_ro",
    "failover",
    "fcall",
    "fcall_ro",
    "ffkv.backup",
    "ffkv.checkpoint",
    "ffkv.compact",
    "ffkv.stats",
    "function",
    "hotkeys",
    "info",
    "lastsave",
    "latency",
    "lolwut",
    "memory",
    "migrate",
    "module",
    "monitor",
    "psync",
    "readonly",
    "readwrite",
    "replconf",
    "replicaof",
    "role",
    "save",
    "script",
    "shutdown",
    "slaveof",
    "slowlog",
    "sync",
    "time",
    "wait",
    "waitaof",
];

/// CLIENT subcommands a playground session implements; the other CLIENT
/// subcommands answer [`not_available`].
pub(crate) const CLIENT_AVAILABLE: &[&str] = &["getname", "id", "setinfo", "setname", "unblock"];

const NOT_AVAILABLE_SUFFIX: &str = " is not available in the browser playground";

pub(crate) fn not_available(command: &str) -> Resp {
    Resp::Error(format!("ERR {command}{NOT_AVAILABLE_SUFFIX}"))
}

pub(crate) fn is_not_available(reply: &Resp) -> bool {
    matches!(reply, Resp::Error(message) if message.ends_with(NOT_AVAILABLE_SUFFIX))
}

/// The playground's answer for a registered command it cannot run. Server-only
/// routes are refused even if [`UNAVAILABLE`] missed one.
pub(crate) fn unavailable(command: &BoundCommand<'_>) -> Option<Resp> {
    let root = command.root_name()?;
    (UNAVAILABLE.contains(&root)
        || matches!(
            command.routing(1),
            Routing::Scripting | Routing::Maintenance
        ))
    .then(|| not_available(&root.to_ascii_uppercase()))
}

fn error(message: &'static str) -> Resp {
    Resp::Error(message.into())
}

/// What a routed command needs next (the server's `connection::Action`).
pub(crate) enum Action {
    Reply(Resp),
    /// Subscription changes answer through the subscription's queue.
    NoReply,
    /// A dataset, Pub/Sub or catalog command for the engine.
    Command,
    Watch(Vec<Bytes>),
    Unblock {
        id: u64,
        error: bool,
    },
    Transaction {
        commands: Vec<Vec<Bytes>>,
        watched: Vec<Token>,
    },
}

pub(crate) struct Session {
    pub id: u32,
    pub version: Version,
    pub client: Arc<ClientSession>,
    pub subscription: Subscription,
    transaction: Option<Transaction>,
    watched: Vec<Token>,
    name: Option<Bytes>,
    library_name: Option<Bytes>,
    library_version: Option<Bytes>,
}

impl Session {
    pub fn new(id: u32, hub: &Arc<Hub>) -> Self {
        Self {
            id,
            version: Version::Resp2,
            client: Arc::new(ClientSession::new(u64::from(id))),
            subscription: hub.attach(u64::from(id)),
            transaction: None,
            watched: Vec::new(),
            name: None,
            library_name: None,
            library_version: None,
        }
    }

    pub fn database(&self) -> u32 {
        self.client.database()
    }

    pub fn watched(&mut self, tokens: Vec<Token>) {
        for token in tokens {
            if !self
                .watched
                .iter()
                .any(|existing| existing.database == token.database && existing.key == token.key)
            {
                self.watched.push(token);
            }
        }
    }

    /// RESET, and the state a new connection starts with after QUIT.
    fn reset(&mut self) {
        self.transaction = None;
        self.watched.clear();
        self.subscription.clear();
        self.client.reset();
        self.version = Version::Resp2;
        self.name = None;
        self.library_name = None;
        self.library_version = None;
    }

    fn reject(&mut self, reply: Resp) -> Action {
        if let Some(transaction) = &mut self.transaction {
            transaction.dirty = true;
        }
        Action::Reply(reply)
    }

    fn queue(&mut self, args: &[Bytes]) -> Action {
        Action::Reply(
            self.transaction
                .as_mut()
                .expect("active transaction")
                .queue(args),
        )
    }

    /// `databases` is the engine's database count, which SELECT must stay below.
    pub fn route(&mut self, bound: &mut BoundCommand<'_>, databases: u32) -> Action {
        let args = bound.args();
        let Some(command) = args.first() else {
            return Action::Reply(error("ERR empty command"));
        };
        if command.eq_ignore_ascii_case(b"QUIT") && args.len() == 1 {
            // The server closes the connection after this reply; the
            // playground's terminal continues as a fresh connection.
            self.reset();
            return Action::Reply(Resp::Simple("OK"));
        }
        if self.version == Version::Resp2
            && self.subscription.active()
            && !allowed_while_subscribed(command)
        {
            return Action::Reply(subscribed_mode_error());
        }
        let reconfiguration = command.eq_ignore_ascii_case(b"HELLO")
            || command.eq_ignore_ascii_case(b"CLIENT")
            || command.eq_ignore_ascii_case(b"AUTH")
            || command.eq_ignore_ascii_case(b"ACL");
        if self.transaction.is_some() && reconfiguration {
            return self.reject(error(
                "ERR connection reconfiguration is not allowed inside MULTI",
            ));
        }
        if command.eq_ignore_ascii_case(b"AUTH") {
            // The playground has no users, like a server without ACLs.
            return Action::Reply(match auth_credentials(args) {
                Ok(_) => no_password_configured(),
                Err(reply) => reply,
            });
        }
        if command.eq_ignore_ascii_case(b"HELLO") {
            return Action::Reply(self.hello(args));
        }
        if command.eq_ignore_ascii_case(b"RESET") && args.len() == 1 {
            self.reset();
            return Action::Reply(Resp::Simple("RESET"));
        }
        if !bound.exists() {
            return self.reject(error("ERR unknown command"));
        }
        if let Some(reply) = unavailable(bound) {
            return self.reject(reply);
        }
        let validation = if self.transaction.is_some() {
            bound.admit()
        } else {
            bound.check()
        };
        if let Err(reply) = validation {
            return self.reject(reply);
        }
        if self.transaction.is_some() && !bound.transaction_allowed() {
            return self.reject(error("ERR Command not allowed inside a transaction"));
        }
        if is_subscription_command(command) {
            if self.transaction.is_some() {
                return self.reject(error(
                    "ERR subscription commands are not allowed inside MULTI",
                ));
            }
            return match self.subscription.command(args) {
                Ok(()) => Action::NoReply,
                Err(reply) => Action::Reply(reply),
            };
        }
        if command.eq_ignore_ascii_case(b"PING")
            && self.version == Version::Resp2
            && self.subscription.active()
        {
            return Action::Reply(subscribed_ping(args));
        }
        if command.eq_ignore_ascii_case(b"MULTI") && args.len() == 1 {
            if self.transaction.is_some() {
                return Action::Reply(error("ERR MULTI calls can not be nested"));
            }
            self.transaction = Some(Transaction::default());
            return Action::Reply(Resp::Simple("OK"));
        }
        if command.eq_ignore_ascii_case(b"EXEC") && args.len() == 1 {
            let Some(transaction) = self.transaction.take() else {
                return Action::Reply(error("ERR EXEC without MULTI"));
            };
            let watched = std::mem::take(&mut self.watched);
            if transaction.dirty {
                return Action::Reply(error(
                    "EXECABORT Transaction discarded because of previous errors.",
                ));
            }
            return Action::Transaction {
                commands: transaction.commands,
                watched,
            };
        }
        if command.eq_ignore_ascii_case(b"DISCARD") && args.len() == 1 {
            if self.transaction.take().is_none() {
                return Action::Reply(error("ERR DISCARD without MULTI"));
            }
            self.watched.clear();
            return Action::Reply(Resp::Simple("OK"));
        }
        if command.eq_ignore_ascii_case(b"WATCH") && args.len() >= 2 {
            if self.transaction.is_some() {
                return Action::Reply(error("ERR WATCH inside MULTI is not allowed"));
            }
            if args.len() - 1 + self.watched.len() > MAX_WATCHED_KEYS {
                return Action::Reply(error("ERR too many watched keys"));
            }
            return Action::Watch(args[1..].to_vec());
        }
        if command.eq_ignore_ascii_case(b"UNWATCH") && args.len() == 1 && self.transaction.is_none()
        {
            self.watched.clear();
            return Action::Reply(Resp::Simple("OK"));
        }
        if self.transaction.is_some() {
            return self.queue(args);
        }
        if command.eq_ignore_ascii_case(b"SELECT") && args.len() == 2 {
            let database = std::str::from_utf8(&args[1])
                .ok()
                .and_then(|value| value.parse::<u32>().ok());
            let Some(database) = database.filter(|db| *db < databases) else {
                return Action::Reply(error("ERR DB index is out of range"));
            };
            self.client.set_database(database);
            return Action::Reply(Resp::Simple("OK"));
        }
        if command.eq_ignore_ascii_case(b"CLIENT") {
            if args
                .get(1)
                .is_some_and(|sub| sub.eq_ignore_ascii_case(b"UNBLOCK"))
            {
                if !(args.len() == 3 || args.len() == 4) {
                    return Action::Reply(error(
                        "ERR wrong number of arguments for CLIENT UNBLOCK",
                    ));
                }
                let id = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|value| value.parse::<u64>().ok());
                let Some(id) = id else {
                    return Action::Reply(error("ERR Invalid client ID"));
                };
                let is_error = args
                    .get(3)
                    .is_some_and(|value| value.eq_ignore_ascii_case(b"ERROR"));
                if args.get(3).is_some_and(|value| {
                    !value.eq_ignore_ascii_case(b"ERROR") && !value.eq_ignore_ascii_case(b"TIMEOUT")
                }) {
                    return Action::Reply(error(
                        "ERR CLIENT UNBLOCK reason should be TIMEOUT or ERROR",
                    ));
                }
                return Action::Unblock {
                    id,
                    error: is_error,
                };
            }
            return Action::Reply(self.client(args));
        }
        Action::Command
    }

    fn hello(&mut self, args: &[Bytes]) -> Resp {
        // No users exist, so HELLO's AUTH option fails like AUTH does.
        let hello = match parse_hello(args, self.version, |_, _| {
            Err::<(), _>(no_password_configured())
        }) {
            Ok(hello) => hello,
            Err(reply) => return reply,
        };
        self.version = hello.version;
        if let Some(name) = hello.name {
            self.name = (!name.is_empty()).then_some(name);
        }
        hello_reply(self.version, u64::from(self.id), false, "master")
    }

    fn client(&mut self, args: &[Bytes]) -> Resp {
        let Some(subcommand) = args.get(1) else {
            return error("ERR wrong number of arguments for CLIENT");
        };
        if subcommand.eq_ignore_ascii_case(b"SETINFO") && args.len() == 4 {
            let (library, value) = match client_setinfo(args) {
                Ok(field) => field,
                Err(reply) => return reply,
            };
            if library {
                self.library_name = Some(value.clone());
            } else {
                self.library_version = Some(value.clone());
            }
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"SETNAME") && args.len() == 3 {
            if !printable(&args[2]) {
                return invalid_client_name();
            }
            self.name = (!args[2].is_empty()).then(|| args[2].clone());
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"GETNAME") && args.len() == 2 {
            Resp::Bulk(self.name.clone())
        } else if subcommand.eq_ignore_ascii_case(b"ID") && args.len() == 2 {
            Resp::Integer(i64::from(self.id))
        } else {
            not_available(&format!(
                "CLIENT {}",
                String::from_utf8_lossy(subcommand).to_ascii_uppercase()
            ))
        }
    }
}
