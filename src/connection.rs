mod rules;

use std::{mem, sync::Arc, time::Instant};

use bytes::Bytes;

use crate::{
    auth::{Acl, User},
    engine::BoundCommand,
    protocol::{Resp, Version},
    state::{ClientLease, State},
    watch::Token,
};
use rules::{
    Transaction, allowed_while_subscribed, auth_credentials, client_setinfo, hello_reply,
    invalid_client_name, is_subscription_command, no_password_configured, parse_hello, printable,
    subscribed_mode_error, subscribed_ping,
};

const MAX_WATCHED_KEYS: usize = 1024;
const MAX_FAILED_AUTH: u8 = 16;

pub enum Action {
    Reply(Resp),
    NoReply,
    Close,
    /// Runs the routed arguments, which stay with the caller.
    Command(Option<crate::server_commands::extensions::Invocation>),
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
    /// Shard pool for inline single-shard execution. Shutdown closes the pool
    /// itself, so holding it never delays the actor's drain.
    pub pool: Option<Arc<crate::sharding::Pool>>,
    state: Arc<State>,
    transaction: Option<Transaction>,
    watched: Vec<Token>,
    acl: Option<Arc<Acl>>,
    /// The directory snapshot this connection holds: its identity, and the
    /// one whose supersession `route` checks.
    user: Option<Arc<User>>,
    /// This connection's own handle on `user`, passed with its commands.
    principal: Option<Arc<User>>,
    failed_auth: u8,
    pub subscription: crate::pubsub::Subscription,
}

fn error(message: &'static str) -> Resp {
    Resp::Error(message.into())
}
/// The name `CLIENT LIST` shows: a registered root's canonical lowercase name
/// without allocating, else the binary-safe ASCII-lowercase first argument, or
/// empty for an empty command.
fn command_name(bound: &BoundCommand<'_>) -> Bytes {
    match bound.root_name() {
        Some(name) => Bytes::from_static(name.as_bytes()),
        None => bound
            .args()
            .first()
            .map(|name| Bytes::from(name.to_ascii_lowercase()))
            .unwrap_or_default(),
    }
}

/// A connection's own handle on `user`: the same policy and identity in an
/// allocation of its own. Commands carry, and so reference-count, this handle
/// rather than the directory's snapshot, which every connection of that user
/// shares across threads.
fn principal(user: Option<&Arc<User>>) -> Option<Arc<User>> {
    user.map(|user| Arc::new(User::clone(user)))
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
        let principal = principal(user.as_ref());
        Self {
            version: Version::Resp2,
            lease,
            control,
            session,
            native: crate::native_replication::ClientState::default(),
            cluster: crate::cluster::ClusterClient::default(),
            pool: state.pool.get().and_then(std::sync::Weak::upgrade),
            state,
            transaction: None,
            watched: Vec::new(),
            acl,
            user,
            principal,
            failed_auth: 0,
            subscription,
        }
    }

    pub fn acl(&self) -> Option<Arc<Acl>> {
        self.acl.clone()
    }

    pub fn user(&self) -> Option<Arc<User>> {
        self.principal.clone()
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

    fn queue(&mut self, args: &[Bytes]) -> Action {
        Action::Reply(
            self.transaction
                .as_mut()
                .expect("active transaction")
                .queue(args),
        )
    }

    fn credentials(&mut self, username: &[u8], password: &[u8]) -> Result<Arc<User>, Resp> {
        let Some(acl) = &self.acl else {
            return Err(no_password_configured());
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
        self.principal = principal(Some(&user));
        self.user = Some(user);
        self.state.client_registry.reset_tracking(self.lease.id);
        self.failed_auth = 0;
        // A new identity must not inherit another identity's watched key state.
        // Never retain another identity's channel registrations or queued messages.
        self.subscription.clear();
        self.watched.clear();
    }

    fn authenticate(&mut self, args: &[Bytes]) -> Resp {
        let (username, password) = match auth_credentials(args) {
            Ok((None, password)) => {
                if self
                    .acl
                    .as_ref()
                    .is_some_and(|acl| acl.default_is_passwordless())
                {
                    return error("ERR password-only AUTH requires a configured default password");
                }
                (b"default".as_slice(), password)
            }
            Ok((Some(username), password)) => (username, password),
            Err(reply) => return reply,
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

    /// Raw-name checks keep their precedence ahead of registry validation,
    /// which `bound` remembers for the caller's later phases. `observed_at` is
    /// the request's post-pause start, both activity views' latest interaction.
    pub fn route(&mut self, bound: &mut BoundCommand<'_>, observed_at: Instant) -> Action {
        let args = bound.args();
        self.lease.seen(observed_at);
        self.control.seen(command_name(bound), observed_at);
        // The held snapshot stays authoritative until the directory supersedes
        // it; only then is its name looked up again. That lookup includes off
        // users: off blocks new authentication, not established identities.
        if let (Some(acl), Some(user)) = (&self.acl, &self.user)
            && user.superseded()
        {
            match acl.user(user.name()) {
                Some(fresh) if user.same_identity(&fresh) => {
                    self.principal = principal(Some(&fresh));
                    self.user = Some(fresh);
                }
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
            return Action::Reply(self.authenticate(args));
        }
        if command.eq_ignore_ascii_case(b"HELLO") {
            return Action::Reply(self.hello(bound));
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
            self.principal = principal(self.user.as_ref());
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
        if !bound.exists() {
            let invocation = match self
                .state
                .server
                .get()
                .and_then(|server| server.extensions().prepare_sync(args))
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
            return Action::Command(Some(invocation));
        }
        let validation = if self.transaction.is_some() {
            bound.admit()
        } else {
            bound.check()
        };
        if let Err(reply) = validation {
            return self.reject(reply);
        }
        if let Some(user) = &self.user
            && let Err(reply) = bound.authorize(user, self.audit().as_ref())
        {
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
            if command.eq_ignore_ascii_case(b"SSUBSCRIBE") && self.state.cluster.get().is_some() {
                return Action::Command(None);
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
                user: self.principal.clone(),
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
            return Action::Reply(self.client(args));
        }
        if command.eq_ignore_ascii_case(b"ACL") {
            return Action::Command(None);
        }
        Action::Command(None)
    }

    fn hello(&mut self, bound: &mut BoundCommand<'_>) -> Resp {
        let args = bound.args();
        let hello = match parse_hello(args, self.version, |username, password| {
            self.credentials(username, password)
        }) {
            Ok(hello) => hello,
            Err(reply) => return reply,
        };
        if self.acl.is_some() && hello.user.is_none() && self.user.is_none() {
            return error("NOAUTH Authentication required.");
        }
        // Validate the complete candidate before committing any connection state.
        if let Some(candidate) = hello.user.as_ref().or(self.user.as_ref())
            && let Err(reply) = bound
                .check()
                .and_then(|()| bound.authorize(candidate, self.audit().as_ref()))
        {
            return reply;
        }
        if let Some(user) = hello.user {
            self.set_user(user);
        }
        let version = hello.version;
        self.version = version;
        self.lease.set_version(version);
        self.control.set_version(version);
        if let Some(name) = hello.name {
            self.control
                .set_field("name", String::from_utf8_lossy(&name).into_owned());
            self.lease.set_name((!name.is_empty()).then_some(name));
        }
        hello_reply(
            version,
            self.lease.id,
            self.state.cluster.get().is_some(),
            self.state.role(),
        )
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
            self.lease.set_library(library, value.clone());
            self.control.set_field(
                if library { "lib-name" } else { "lib-ver" },
                String::from_utf8_lossy(value).into_owned(),
            );
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"SETNAME") && args.len() == 3 {
            if !printable(&args[2]) {
                return invalid_client_name();
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

    fn route(connection: &mut Connection, values: &[&str]) -> Action {
        let args = args(values);
        connection.route(&mut BoundCommand::bind(&args), Instant::now())
    }

    fn connection(state: Arc<State>) -> Connection {
        let lease = state.register(
            "127.0.0.1:40000".parse().unwrap(),
            "127.0.0.1:6379".parse().unwrap(),
        );
        Connection::new(lease, state, None)
    }

    /// Runs a space-separated ACL command as the default user.
    fn acl_command(acl: &Acl, command: &str) -> Resp {
        let words: Vec<&str> = command.split(' ').collect();
        acl.execute(&args(&words), "default", |_, _| Ok(()), |_| Vec::new())
    }

    fn established(state: &Arc<State>, acl: &Arc<Acl>, name: &str, port: u16) -> Connection {
        let lease = state.register(
            format!("127.0.0.1:{port}").parse().unwrap(),
            "127.0.0.1:6379".parse().unwrap(),
        );
        let mut connection = Connection::new(lease, state.clone(), Some(acl.clone()));
        assert!(matches!(
            route(&mut connection, &["AUTH", name, "secret"]),
            Action::Reply(Resp::Simple("OK"))
        ));
        assert!(matches!(
            route(&mut connection, &["GET", "key"]),
            Action::Command(None)
        ));
        connection
    }

    #[test]
    fn established_identities_end_when_deleted_recreated_or_reloaded_away() {
        let name = format!("ffkv-acl-route-{}", std::process::id());
        let path = std::env::temp_dir().join(name);
        let users = "user default on nopass +@all ~* &*\n\
                     user alice on >secret +@all ~*\n\
                     user bob on >secret +@all ~*\n";
        std::fs::write(&path, users).unwrap();
        let acl = Arc::new(Acl::load(&path).unwrap());
        let state = Arc::new(State::new(6379, 1, false));
        let mut alice = established(&state, &acl, "alice", 40001);
        let mut bob = established(&state, &acl, "bob", 40002);
        // A same-named replacement is a new identity, never a revival.
        assert_eq!(acl_command(&acl, "ACL DELUSER alice"), Resp::Integer(1));
        assert_eq!(
            acl_command(&acl, "ACL SETUSER alice on >secret +@all ~*"),
            Resp::Simple("OK")
        );
        assert!(matches!(
            route(&mut alice, &["GET", "key"]),
            Action::NoReply
        ));
        assert!(matches!(
            route(&mut bob, &["GET", "key"]),
            Action::Command(None)
        ));
        std::fs::write(&path, "user default on nopass +@all ~* &*\n").unwrap();
        let reloaded = acl_command(&acl, "ACL LOAD");
        std::fs::remove_file(&path).unwrap();
        assert_eq!(reloaded, Resp::Simple("OK"));
        assert!(matches!(route(&mut bob, &["GET", "key"]), Action::NoReply));
    }

    #[test]
    fn commands_carry_the_current_rules_of_the_identity_the_connection_holds() {
        let acl = Arc::new(Acl::new());
        let state = Arc::new(State::new(6379, 1, false));
        assert_eq!(
            acl_command(&acl, "ACL SETUSER alice on >secret +@all ~*"),
            Resp::Simple("OK")
        );
        let mut alice = established(&state, &acl, "alice", 40003);
        let key = Bytes::from_static(b"key");
        let allowed = |connection: &Connection| {
            connection
                .user()
                .expect("an authenticated identity")
                .authorize(b"get", &["read"], std::slice::from_ref(&key), false)
                .is_ok()
        };
        assert!(allowed(&alice));
        assert_eq!(
            acl_command(&acl, "ACL SETUSER alice resetkeys ~other:*"),
            Resp::Simple("OK")
        );
        // The next command adopts the replacement, and so do the commands it
        // hands on.
        assert!(matches!(
            route(&mut alice, &["GET", "key"]),
            Action::Reply(Resp::Error(_))
        ));
        assert!(!allowed(&alice));
        let current = acl.user("alice").unwrap();
        assert!(alice.user().unwrap().same_identity(&current));
        // RESET returns to the default user for both.
        assert!(matches!(
            route(&mut alice, &["RESET"]),
            Action::Reply(Resp::Simple("RESET"))
        ));
        assert!(allowed(&alice));
        assert_eq!(alice.user().unwrap().name(), "default");
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
            route(&mut connection, &["SELECT", "23"]),
            Action::Reply(Resp::Simple("OK"))
        ));
        assert_eq!(connection.session.database(), 23);
        runtime
            .databases
            .store(24, std::sync::atomic::Ordering::Release);
        assert!(matches!(
            route(&mut connection, &["SELECT", "24"]),
            Action::Reply(Resp::Error(_))
        ));
        assert!(matches!(
            route(&mut connection, &["CLIENT", "NO-TOUCH", "ON"]),
            Action::Reply(Resp::Simple("OK"))
        ));
        connection.cluster.readonly = true;
        connection.cluster.asking = true;
        connection.native.listening_port = 1234;
        assert!(matches!(
            route(&mut connection, &["RESET"]),
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
    fn client_list_shows_each_routed_command_by_its_lowercase_name() {
        let state = Arc::new(State::new(6379, 1, false));
        let mut connection = connection(state.clone());
        let mut listed = |args: Vec<Bytes>| {
            connection.route(&mut BoundCommand::bind(&args), Instant::now());
            String::from_utf8_lossy(&state.client_registry.list(None, None)).into_owned()
        };
        assert!(listed(args(&["GeT", "key"])).contains(" cmd=get "));
        assert!(listed(args(&["Config", "GET", "port"])).contains(" cmd=config "));
        assert!(listed(args(&["CONFIG|GET", "port"])).contains(" cmd=config|get "));
        let binary = vec![Bytes::from_static(b"No\xffSuch")];
        assert!(listed(binary).contains(" cmd=no\u{fffd}such "));
        assert!(listed(Vec::new()).contains(" cmd= "));
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
