use std::{mem, sync::Arc};

use bytes::Bytes;

use crate::{
    auth::{Acl, User},
    engine::Engine,
    protocol::{Resp, Version},
    state::{ClientLease, State},
    watch::Token,
};

const MAX_QUEUED_COMMANDS: usize = 1024;
const MAX_QUEUED_BYTES: usize = 16 * 1024 * 1024;
const MAX_WATCHED_KEYS: usize = 1024;
const MAX_FAILED_AUTH: u8 = 16;

struct Transaction {
    commands: Vec<Vec<Bytes>>,
    bytes: usize,
    dirty: bool,
}

pub enum Action {
    Reply(Resp),
    NoReply,
    Close,
    Command(
        Vec<Bytes>,
        Option<crate::server_commands::extensions::Invocation>,
    ),
    Watch(Vec<Bytes>),
    Unblock {
        id: u64,
        error: bool,
    },
    Transaction {
        commands: Vec<Vec<Bytes>>,
        watched: Vec<Token>,
        user: Option<Arc<User>>,
    },
}

pub struct Connection {
    pub version: Version,
    pub lease: ClientLease,
    pub control: Arc<crate::client_registry::ClientControl>,
    pub session: Arc<crate::session::ClientSession>,
    pub native: crate::native_replication::ClientState,
    pub cluster: crate::cluster::ClusterClient,
    state: Arc<State>,
    transaction: Option<Transaction>,
    watched: Vec<Token>,
    acl: Option<Arc<Acl>>,
    user: Option<Arc<User>>,
    failed_auth: u8,
    pub subscription: crate::pubsub::Subscription,
}

fn error(message: &'static str) -> Resp {
    Resp::Error(message.into())
}
fn printable(value: &[u8]) -> bool {
    value.len() <= 256 && value.iter().all(|byte| (b'!'..=b'~').contains(byte))
}
fn bulk(value: &'static str) -> Resp {
    Resp::Bulk(Some(Bytes::from_static(value.as_bytes())))
}

impl Connection {
    pub fn new(lease: ClientLease, state: Arc<State>, acl: Option<Arc<Acl>>) -> Self {
        let user = acl.as_ref().and_then(|acl| acl.default_user());
        lease.set_user(
            user.as_ref()
                .map(|user| Bytes::copy_from_slice(user.name().as_bytes())),
        );
        let subscription = state.pubsub.attach(lease.id);
        let control = state.client_registry.register(lease.id, lease.info());
        let mut session = crate::session::ClientSession::new(lease.id);
        session.acl = acl.clone();
        session.clients = Some(state.client_registry.clone());
        let session = Arc::new(session);
        Self {
            version: Version::Resp2,
            lease,
            control,
            session,
            native: crate::native_replication::ClientState::default(),
            cluster: crate::cluster::ClusterClient::default(),
            state,
            transaction: None,
            watched: Vec::new(),
            acl,
            user,
            failed_auth: 0,
            subscription,
        }
    }

    pub fn acl(&self) -> Option<Arc<Acl>> {
        self.acl.clone()
    }

    pub fn user(&self) -> Option<Arc<User>> {
        self.user.clone()
    }
    pub fn audit(&self) -> Option<crate::auth::Audit<'_>> {
        Some(crate::auth::Audit {
            acl: self.acl.as_deref()?,
            clients: &self.state.client_registry,
            client_id: self.lease.id,
            context: if self.transaction.is_some() {
                "multi"
            } else {
                "toplevel"
            },
        })
    }

    pub fn authenticated(&self) -> bool {
        self.acl.is_none() || self.user.is_some()
    }

    pub fn take_reply_permission(&self) -> bool {
        self.control.should_reply()
    }

    pub fn close_after_reply(&self) -> bool {
        self.control.close_after_reply()
    }

    pub fn authentication_exhausted(&self) -> bool {
        self.failed_auth >= MAX_FAILED_AUTH
    }

    fn reject(&mut self, reply: Resp) -> Action {
        if let Some(transaction) = &mut self.transaction {
            transaction.dirty = true;
        }
        Action::Reply(reply)
    }

    fn queue(&mut self, args: Vec<Bytes>) -> Action {
        let transaction = self.transaction.as_mut().expect("active transaction");
        let bytes: usize = args.iter().map(Bytes::len).sum();
        if transaction.commands.len() == MAX_QUEUED_COMMANDS
            || transaction
                .bytes
                .checked_add(bytes)
                .is_none_or(|bytes| bytes > MAX_QUEUED_BYTES)
        {
            transaction.dirty = true;
            return Action::Reply(error("ERR transaction queue limit exceeded"));
        }
        transaction.bytes += bytes;
        transaction.commands.push(args);
        Action::Reply(Resp::Simple("QUEUED"))
    }

    fn credentials(&mut self, username: &[u8], password: &[u8]) -> Result<Arc<User>, Resp> {
        let Some(acl) = &self.acl else {
            return Err(error("ERR AUTH called without any password configured"));
        };
        let user = if username.len() <= 256 && password.len() <= 1024 {
            acl.authenticate(username, password)
        } else {
            None
        };
        match user {
            Some(user) => Ok(user),
            None => {
                self.failed_auth = self.failed_auth.saturating_add(1);
                Err(error(
                    "WRONGPASS invalid username-password pair or user is disabled.",
                ))
            }
        }
    }

    fn set_user(&mut self, user: Arc<User>) {
        self.lease
            .set_user(Some(Bytes::copy_from_slice(user.name().as_bytes())));
        self.control.set_field("user", user.name().to_owned());
        self.user = Some(user);
        self.state.client_registry.reset_tracking(self.lease.id);
        self.failed_auth = 0;
        // A new identity must not inherit another identity's watched key state.
        // Never retain another identity's channel registrations or queued messages.
        self.subscription.clear();
        self.watched.clear();
    }

    fn authenticate(&mut self, args: &[Bytes]) -> Resp {
        let (username, password) = match args {
            [_, password] => {
                if self
                    .acl
                    .as_ref()
                    .is_some_and(|acl| acl.default_is_passwordless())
                {
                    return error("ERR password-only AUTH requires a configured default password");
                }
                (b"default".as_slice(), password.as_ref())
            }
            [_, username, password] => (username.as_ref(), password.as_ref()),
            _ => return error("ERR wrong number of arguments for AUTH"),
        };
        match self.credentials(username, password) {
            Ok(user) => {
                self.set_user(user);
                Resp::Simple("OK")
            }
            Err(reply) => reply,
        }
    }
    pub fn watched(&mut self, watches: Vec<Token>) {
        for token in watches {
            if !self
                .watched
                .iter()
                .any(|existing| existing.database == token.database && existing.key == token.key)
            {
                self.watched.push(token);
            }
        }
    }

    pub fn route(&mut self, args: Vec<Bytes>) -> Action {
        self.lease.seen();
        self.control.seen(&args);
        if let (Some(acl), Some(user)) = (&self.acl, &self.user) {
            match acl.user(user.name()) {
                Some(fresh) if user.same_identity(&fresh) => self.user = Some(fresh),
                _ => {
                    self.state.pubsub.disconnect(self.lease.id);
                    return Action::NoReply;
                }
            }
        }
        if self.failed_auth >= MAX_FAILED_AUTH {
            return Action::Close;
        }
        let Some(command) = args.first() else {
            return Action::Reply(error("ERR empty command"));
        };
        if command.eq_ignore_ascii_case(b"QUIT") && args.len() == 1 {
            return Action::Close;
        }
        if self.version == Version::Resp2
            && self.subscription.active()
            && ![
                b"SUBSCRIBE".as_slice(),
                b"PSUBSCRIBE",
                b"UNSUBSCRIBE",
                b"PUNSUBSCRIBE",
                b"PING",
                b"SSUBSCRIBE",
                b"SUNSUBSCRIBE",
                b"RESET",
            ]
            .iter()
            .any(|name| command.eq_ignore_ascii_case(name))
        {
            return Action::Reply(error(
                "ERR Can't execute command in subscribed mode: only SUBSCRIBE / PSUBSCRIBE / UNSUBSCRIBE / PUNSUBSCRIBE / PING / QUIT are allowed",
            ));
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
            return Action::Reply(self.authenticate(&args));
        }
        if command.eq_ignore_ascii_case(b"HELLO") {
            return Action::Reply(self.hello(&args));
        }
        if command.eq_ignore_ascii_case(b"RESET") && args.len() == 1 {
            self.transaction = None;
            self.watched.clear();
            self.subscription.clear();
            self.state.client_registry.reset_tracking(self.lease.id);
            self.session.reset();
            self.control.reset();
            self.native = crate::native_replication::ClientState::default();
            self.cluster = crate::cluster::ClusterClient::default();
            self.version = Version::Resp2;
            self.lease.set_version(self.version);
            self.lease.set_name(None);
            self.lease.set_library(true, Bytes::new());
            self.lease.set_library(false, Bytes::new());
            self.user = self.acl.as_ref().and_then(|acl| acl.default_user());
            let username = self
                .user
                .as_ref()
                .map(|user| Bytes::copy_from_slice(user.name().as_bytes()));
            self.lease.set_user(username.clone());
            self.control.set_field(
                "user",
                username
                    .as_ref()
                    .map(|name| String::from_utf8_lossy(name).into_owned())
                    .unwrap_or_else(|| "default".into()),
            );
            return Action::Reply(Resp::Simple("RESET"));
        }
        if !self.authenticated() {
            return self.reject(error("NOAUTH Authentication required."));
        }
        if !Engine::command_exists(command) {
            let invocation = match self
                .state
                .server
                .get()
                .and_then(|server| server.extensions().prepare_sync(&args))
            {
                Some(Ok(invocation)) => invocation,
                Some(Err(reply)) => return self.reject(reply),
                None => return self.reject(error("ERR unknown command")),
            };
            if let Some(user) = self.user.as_ref()
                && let Err(reason) = user.authorize_logged(
                    invocation.name(),
                    invocation.categories(),
                    invocation.keys(),
                    invocation.global_keyspace(),
                    self.audit().as_ref(),
                )
            {
                return self.reject(Resp::Error(format!("NOPERM {reason}")));
            }
            if self.transaction.is_some() {
                if !invocation.transaction_allowed() {
                    return self.reject(error("ERR Command not allowed inside a transaction"));
                }
                return self.queue(args);
            }
            return Action::Command(args, Some(invocation));
        }
        let validation = if self.transaction.is_some() {
            Engine::validate_admission(&args)
        } else {
            Engine::validate(&args)
        };
        if let Err(reply) = validation {
            return self.reject(reply);
        }
        if let Some(user) = &self.user
            && let Err(reply) =
                Engine::authorize_admission_logged(&args, user, self.audit().as_ref())
        {
            return self.reject(reply);
        }
        if self.transaction.is_some() && !Engine::transaction_allowed(&args) {
            return self.reject(error("ERR Command not allowed inside a transaction"));
        }
        let subscription_command = [
            b"SUBSCRIBE".as_slice(),
            b"PSUBSCRIBE",
            b"UNSUBSCRIBE",
            b"PUNSUBSCRIBE",
            b"SSUBSCRIBE",
            b"SUNSUBSCRIBE",
        ]
        .iter()
        .any(|name| command.eq_ignore_ascii_case(name));
        if subscription_command {
            if self.transaction.is_some() {
                return self.reject(error(
                    "ERR subscription commands are not allowed inside MULTI",
                ));
            }
            if command.eq_ignore_ascii_case(b"SSUBSCRIBE") && self.state.cluster.get().is_some() {
                return Action::Command(args, None);
            }
            return match self.subscription.command(&args) {
                Ok(()) => Action::NoReply,
                Err(reply) => Action::Reply(reply),
            };
        }
        if command.eq_ignore_ascii_case(b"PING")
            && self.version == Version::Resp2
            && self.subscription.active()
        {
            return Action::Reply(Resp::Array(Some(vec![
                bulk("pong"),
                Resp::Bulk(Some(args.get(1).cloned().unwrap_or_default())),
            ])));
        }
        if command.eq_ignore_ascii_case(b"MULTI") && args.len() == 1 {
            if self.transaction.is_some() {
                return Action::Reply(error("ERR MULTI calls can not be nested"));
            }
            self.transaction = Some(Transaction {
                commands: Vec::new(),
                bytes: 0,
                dirty: false,
            });
            self.control.set_transaction(true);
            return Action::Reply(Resp::Simple("OK"));
        }
        if command.eq_ignore_ascii_case(b"EXEC") && args.len() == 1 {
            let Some(transaction) = self.transaction.take() else {
                return Action::Reply(error("ERR EXEC without MULTI"));
            };
            self.control.set_transaction(false);
            let watched = mem::take(&mut self.watched);
            if transaction.dirty {
                return Action::Reply(error(
                    "EXECABORT Transaction discarded because of previous errors.",
                ));
            }
            return Action::Transaction {
                commands: transaction.commands,
                watched,
                user: self.user.clone(),
            };
        }
        if command.eq_ignore_ascii_case(b"DISCARD") && args.len() == 1 {
            if self.transaction.take().is_none() {
                return Action::Reply(error("ERR DISCARD without MULTI"));
            }
            self.control.set_transaction(false);
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
            return Action::Watch(args.into_iter().skip(1).collect());
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
            let limit = self.state.admin_runtime.get().map_or(16, |runtime| {
                runtime.databases.load(std::sync::atomic::Ordering::Acquire)
            });
            let Some(database) = database.filter(|db| *db < limit) else {
                return Action::Reply(error("ERR DB index is out of range"));
            };
            if self.state.cluster.get().is_some() && database != 0 {
                return Action::Reply(error("ERR SELECT is not allowed in cluster mode"));
            }
            self.session.set_database(database);
            self.control.set_database(database);
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
            return Action::Reply(self.client(&args));
        }
        if command.eq_ignore_ascii_case(b"ACL") {
            return Action::Command(args, None);
        }
        Action::Command(args, None)
    }

    fn hello(&mut self, args: &[Bytes]) -> Resp {
        let mut version = self.version;
        let mut name = None;
        let mut user = None;
        if let Some(protocol) = args.get(1) {
            version = match protocol.as_ref() {
                b"2" => Version::Resp2,
                b"3" => Version::Resp3,
                _ => return error("NOPROTO unsupported protocol version"),
            };
        }
        let mut position = 2;
        while position < args.len() {
            if args[position].eq_ignore_ascii_case(b"SETNAME") && position + 1 < args.len() {
                let value = &args[position + 1];
                if !printable(value) {
                    return error(
                        "ERR Client names must be printable ASCII without spaces and at most 256 bytes",
                    );
                }
                name = Some(value.clone());
                position += 2;
            } else if args[position].eq_ignore_ascii_case(b"AUTH") && position + 2 < args.len() {
                if user.is_some() {
                    return error("ERR duplicate HELLO AUTH option");
                }
                match self.credentials(&args[position + 1], &args[position + 2]) {
                    Ok(authenticated) => user = Some(authenticated),
                    Err(reply) => return reply,
                }
                position += 3;
            } else {
                return error("ERR syntax error in HELLO option");
            }
        }
        if self.acl.is_some() && user.is_none() && self.user.is_none() {
            return error("NOAUTH Authentication required.");
        }
        // Validate the complete candidate before committing any connection state.
        if let Some(candidate) = user.as_ref().or(self.user.as_ref())
            && let Err(reply) = Engine::authorize_logged(args, candidate, self.audit().as_ref())
        {
            return reply;
        }
        if let Some(user) = user {
            self.set_user(user);
        }
        self.version = version;
        self.lease.set_version(version);
        self.control.set_version(version);
        if let Some(name) = name {
            self.control
                .set_field("name", String::from_utf8_lossy(&name).into_owned());
            self.lease.set_name((!name.is_empty()).then_some(name));
        }
        Resp::Map(vec![
            (bulk("server"), bulk("ffkv")),
            (bulk("version"), bulk(env!("CARGO_PKG_VERSION"))),
            (
                bulk("proto"),
                Resp::Integer(if version == Version::Resp3 { 3 } else { 2 }),
            ),
            (bulk("id"), Resp::Integer(self.lease.id as i64)),
            (
                bulk("mode"),
                bulk(if self.state.cluster.get().is_some() {
                    "cluster"
                } else {
                    "standalone"
                }),
            ),
            (bulk("role"), bulk(self.state.role())),
            (bulk("modules"), Resp::Array(Some(Vec::new()))),
        ])
    }

    fn client(&mut self, args: &[Bytes]) -> Resp {
        let Some(subcommand) = args.get(1) else {
            return error("ERR wrong number of arguments for CLIENT");
        };
        if subcommand.eq_ignore_ascii_case(b"SETINFO") && args.len() == 4 {
            if !printable(&args[3]) {
                return error(
                    "ERR client library metadata must be printable ASCII and at most 256 bytes",
                );
            }
            let library = if args[2].eq_ignore_ascii_case(b"LIB-NAME") {
                true
            } else if args[2].eq_ignore_ascii_case(b"LIB-VER") {
                false
            } else {
                return error("ERR unrecognized CLIENT SETINFO option");
            };
            self.lease.set_library(library, args[3].clone());
            self.control.set_field(
                if library { "lib-name" } else { "lib-ver" },
                String::from_utf8_lossy(&args[3]).into_owned(),
            );
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"SETNAME") && args.len() == 3 {
            if !printable(&args[2]) {
                return error(
                    "ERR Client names must be printable ASCII without spaces and at most 256 bytes",
                );
            }
            self.lease
                .set_name((!args[2].is_empty()).then(|| args[2].clone()));
            self.control
                .set_field("name", String::from_utf8_lossy(&args[2]).into_owned());
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"GETNAME") && args.len() == 2 {
            Resp::Bulk(self.lease.name())
        } else if subcommand.eq_ignore_ascii_case(b"ID") && args.len() == 2 {
            Resp::Integer(self.lease.id as i64)
        } else {
            let reply = self.state.client_registry.command(&self.control, args);
            if subcommand.eq_ignore_ascii_case(b"NO-TOUCH") && !matches!(&reply, Resp::Error(_)) {
                self.session.no_touch.store(
                    self.control.no_touch(),
                    std::sync::atomic::Ordering::Release,
                );
            }
            reply
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.state.client_registry.unregister(self.lease.id);
    }
}

#[cfg(test)]
mod frontend_tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect()
    }

    fn connection(state: Arc<State>) -> Connection {
        let lease = state.register(
            "127.0.0.1:40000".parse().unwrap(),
            "127.0.0.1:6379".parse().unwrap(),
        );
        Connection::new(lease, state, None)
    }

    #[test]
    fn select_uses_the_shared_database_limit_and_reset_clears_session_modes() {
        let state = Arc::new(State::new(6379, 1, false));
        let backup = crate::server_runtime::OnlineBackup::new(
            std::env::temp_dir().join("ffkv-unopened-frontend-test"),
            0,
        );
        let runtime = crate::server_runtime::RuntimeState::new(backup, 32, 100);
        assert!(state.admin_runtime.set(runtime.clone()).is_ok());
        let mut connection = connection(state);
        assert!(matches!(
            connection.route(args(&["SELECT", "23"])),
            Action::Reply(Resp::Simple("OK"))
        ));
        assert_eq!(connection.session.database(), 23);
        runtime
            .databases
            .store(24, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            connection.route(args(&["SELECT", "24"])),
            Action::Reply(Resp::Error(_))
        ));
        assert!(matches!(
            connection.route(args(&["CLIENT", "NO-TOUCH", "ON"])),
            Action::Reply(Resp::Simple("OK"))
        ));
        connection.cluster.readonly = true;
        connection.cluster.asking = true;
        connection.native.listening_port = 1234;
        assert!(matches!(
            connection.route(args(&["RESET"])),
            Action::Reply(Resp::Simple("RESET"))
        ));
        assert_eq!(connection.session.database(), 0);
        assert!(
            !connection
                .session
                .no_touch
                .load(std::sync::atomic::Ordering::Acquire)
        );
        assert!(!connection.cluster.readonly && !connection.cluster.asking);
        assert_eq!(connection.native.listening_port, 0);
    }

    #[test]
    fn identical_watched_key_names_in_different_databases_remain_distinct() {
        let state = Arc::new(State::new(6379, 1, false));
        let mut connection = connection(state);
        let registry = crate::watch::Registry::default();
        connection.watched(vec![
            registry.register(0, Bytes::from_static(b"key"), None),
            registry.register(1, Bytes::from_static(b"key"), None),
        ]);
        assert_eq!(connection.watched.len(), 2);
        connection.watched(vec![registry.register(1, Bytes::from_static(b"key"), None)]);
        assert_eq!(connection.watched.len(), 2);
    }
}
