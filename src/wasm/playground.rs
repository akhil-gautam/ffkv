//! The shared dataset and its sessions: one engine executes every command the
//! way a single-shard server's executor and blocking actor do, and each reply
//! is shown exactly as ffkv-cli's interactive mode prints it.
use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::Instant,
};

use bytes::{Bytes, BytesMut};
use futures_util::FutureExt;

use super::session::{Action, Session, is_not_available, unavailable};
use crate::{
    cli::{
        format::{Format, redacted_error, render},
        tokenize::{sensitive, tokenize},
    },
    engine::{BlockingKind, BlockingPop, BlockingStream, BoundCommand, Engine, Execution},
    protocol::{Resp, Version, encode_length, validate_command},
    pubsub::Hub,
    watch::{Registry, Token},
};

/// A command waiting for data, as the server's blocking actor holds it.
enum Wait {
    Pop(BlockingPop),
    Stream(BlockingStream),
}

struct Blocked {
    /// Registration order: earlier waiters are served first.
    order: u64,
    deadline: Option<Instant>,
    wait: Wait,
}

/// A session with the terminal-facing state around it.
struct Client {
    session: Session,
    blocked: Option<Blocked>,
    /// Lines entered while blocked run, in order, once it is served, as a
    /// pipelined connection's would.
    queued: VecDeque<Vec<Bytes>>,
    /// Text not yet returned by `eval` or `poll`.
    output: Vec<u8>,
    /// `output` includes an error.
    failed: bool,
}

enum Data {
    Reply(Resp),
    Wait(Wait, Option<std::time::Duration>),
}

pub(crate) struct Playground {
    engine: Engine,
    hub: Arc<Hub>,
    watches: Arc<Registry>,
    clients: BTreeMap<u32, Client>,
    next_id: u32,
    next_wait: u64,
}

fn new_engine(watches: &Arc<Registry>) -> Engine {
    let mut engine = Engine::new();
    engine.set_watches(watches.clone());
    // Capture every mutation, as the server's default append-only mode does:
    // WATCH versions and search maintenance observe them, then `finish`
    // discards the batch since there is no log to commit it to.
    engine.set_recording(true);
    engine
}

fn io_error(error: &std::io::Error) -> Resp {
    Resp::Error(format!("IOERR {error}"))
}

impl Playground {
    pub fn new() -> Self {
        let watches = Arc::new(Registry::default());
        Self {
            engine: new_engine(&watches),
            hub: Arc::new(Hub::default()),
            watches,
            clients: BTreeMap::new(),
            next_id: 1,
            next_wait: 0,
        }
    }

    pub fn open(&mut self) -> u32 {
        let id = self.next_id;
        self.next_id += 1;
        self.clients.insert(
            id,
            Client {
                session: Session::new(id, &self.hub),
                blocked: None,
                queued: VecDeque::new(),
                output: Vec::new(),
                failed: false,
            },
        );
        id
    }

    pub fn close(&mut self, id: u32) {
        self.clients.remove(&id);
    }

    /// Empties every database. Sessions, their subscriptions and any blocked
    /// commands stay, as after FLUSHALL; WATCHed keys count as modified.
    pub fn reset(&mut self) {
        self.engine = new_engine(&self.watches);
        self.watches.invalidate_all();
    }

    pub fn eval(&mut self, id: u32, line: &[u8]) -> String {
        let Some(mut client) = self.clients.remove(&id) else {
            return unknown_session(id);
        };
        match tokenize(line) {
            Err(error) => {
                client
                    .output
                    .extend_from_slice(format!("ffkv-cli: input: {error}\n").as_bytes());
                client.failed = true;
            }
            Ok(args) if args.is_empty() => {}
            Ok(args) => {
                if let Err(error) = validate_command(&args) {
                    client
                        .output
                        .extend_from_slice(format!("ffkv-cli: {error}\n").as_bytes());
                    client.failed = true;
                } else if client.blocked.is_some() {
                    client.queued.push_back(args);
                } else {
                    self.run(&mut client, &args);
                }
            }
        }
        self.clients.insert(id, client);
        self.wake();
        self.respond(id)
    }

    pub fn poll(&mut self, id: u32) -> String {
        if !self.clients.contains_key(&id) {
            return unknown_session(id);
        }
        self.watches.reap();
        self.wake();
        if let Some(client) = self.clients.get_mut(&id) {
            deliver_messages(client);
        }
        self.respond(id)
    }

    fn respond(&mut self, id: u32) -> String {
        let client = self.clients.get_mut(&id).expect("open session");
        let output = String::from_utf8_lossy(&std::mem::take(&mut client.output)).into_owned();
        let error = std::mem::take(&mut client.failed);
        let database = client.session.database();
        let prompt = if database == 0 {
            "ffkv> ".to_owned()
        } else {
            format!("ffkv[{database}]> ")
        };
        serde_json::json!({
            "output": output,
            "blocked": client.blocked.is_some(),
            "prompt": prompt,
            "error": error,
        })
        .to_string()
    }

    /// Runs one command line for `client`, which is not in `self.clients`.
    fn run(&mut self, client: &mut Client, args: &[Bytes]) {
        let mut bound = BoundCommand::bind(args);
        let action = client
            .session
            .route(&mut bound, self.engine.database_count());
        let reply = match action {
            Action::Reply(reply) => Some(reply),
            Action::NoReply => None,
            Action::Command => match self.data(&client.session, args, false) {
                Data::Reply(reply) => Some(reply),
                Data::Wait(wait, timeout) => self.block(client, wait, timeout),
            },
            Action::Watch(keys) => {
                let tokens = self.watch(&client.session, keys);
                client.session.watched(tokens);
                Some(Resp::Simple("OK"))
            }
            Action::Unblock { id, error } => Some(self.unblock(id, error)),
            Action::Transaction { commands, watched } => {
                Some(self.transaction(&client.session, &commands, &watched))
            }
        };
        if let Some(reply) = reply {
            // ffkv-cli redacts errors answering credential commands; the
            // playground's own refusal carries no credential.
            let redact = sensitive(args) && !is_not_available(&reply);
            show(client, &reply, redact);
        }
        deliver_messages(client);
    }

    /// The server executor's path for one dataset command (`Atomic::data`).
    fn data(&mut self, session: &Session, args: &[Bytes], in_transaction: bool) -> Data {
        let mut command = BoundCommand::bind(args);
        if let Err(reply) = command.check() {
            return Data::Reply(reply);
        }
        if in_transaction && !command.transaction_allowed() {
            return Data::Reply(Resp::Error(
                "ERR Command not allowed inside a transaction".into(),
            ));
        }
        if args[0].eq_ignore_ascii_case(b"PUBLISH") || args[0].eq_ignore_ascii_case(b"SPUBLISH") {
            let published = if args[0].eq_ignore_ascii_case(b"SPUBLISH") {
                self.hub.publish_shard(&args[1], &args[2])
            } else {
                self.hub.publish(&args[1], &args[2])
            };
            return Data::Reply(match published {
                Ok(count) => Resp::Integer(count),
                Err(reason) => Resp::Error(reason.into()),
            });
        }
        if args[0].eq_ignore_ascii_case(b"PUBSUB") {
            return Data::Reply(self.hub.inspect(args));
        }
        if let Some(reply) = unavailable(&command) {
            return Data::Reply(reply);
        }
        if args[0]
            .get(..3)
            .is_some_and(|name| name.eq_ignore_ascii_case(b"FT."))
        {
            // Search reclaims expired cursors and temporary indexes first.
            self.engine.switch_database(0);
            self.engine.search_reap(Engine::now());
            self.finish();
        }
        self.prepare(session);
        let execution = self.engine.execute(&mut command);
        self.finish();
        match execution {
            Err(error) => Data::Reply(io_error(&error)),
            Ok(Execution::Reply(reply) | Execution::Pushed { reply, .. }) => Data::Reply(reply),
            Ok(Execution::Block(pop)) if in_transaction => Data::Reply(pop.null_reply()),
            Ok(Execution::StreamBlock(_)) if in_transaction => Data::Reply(Resp::Array(None)),
            Ok(Execution::Block(pop)) => {
                let timeout = pop.timeout;
                Data::Wait(Wait::Pop(pop), timeout)
            }
            Ok(Execution::StreamBlock(read)) => {
                let timeout = read.timeout;
                Data::Wait(Wait::Stream(read), timeout)
            }
        }
    }

    /// Installs `session` as the command's client, selecting its database.
    fn prepare(&mut self, session: &Session) {
        self.engine.set_command_version(session.version);
        self.engine
            .set_command_session(Some(session.client.clone()));
    }

    /// Settles a command's effects; there is no journal to commit them to.
    fn finish(&mut self) {
        self.engine.finish_pending_search();
        let _ = self.engine.take_ready_keys();
        let _ = self.engine.take_batch();
        self.engine.set_command_session(None);
        self.engine.set_command_version(Version::Resp2);
    }

    fn block(
        &mut self,
        client: &mut Client,
        wait: Wait,
        timeout: Option<std::time::Duration>,
    ) -> Option<Resp> {
        let deadline = match timeout {
            Some(timeout) => match Instant::now().checked_add(timeout) {
                Some(deadline) => Some(deadline),
                None => return Some(Resp::Error("ERR timeout is out of range".into())),
            },
            None => None,
        };
        let wait = match wait {
            Wait::Pop(mut pop) => {
                // A wakeup re-runs the original ordered key scan; duplicate
                // keys never change which key or error takes precedence.
                let mut seen = std::collections::HashSet::with_capacity(pop.keys.len());
                pop.keys.retain(|key| seen.insert(key.clone()));
                Wait::Pop(pop)
            }
            stream => stream,
        };
        client.blocked = Some(Blocked {
            order: self.next_wait,
            deadline,
            wait,
        });
        self.next_wait += 1;
        None
    }

    fn watch(&mut self, session: &Session, mut keys: Vec<Bytes>) -> Vec<Token> {
        let database = session.database();
        self.prepare(session);
        let now = Engine::now();
        keys.sort_unstable();
        keys.dedup();
        let tokens = keys
            .into_iter()
            .map(|key| {
                let deadline = self.engine.watch_deadline(&key, now);
                self.watches.register(database, key, deadline)
            })
            .collect();
        self.finish();
        tokens
    }

    fn transaction(
        &mut self,
        session: &Session,
        commands: &[Vec<Bytes>],
        watched: &[Token],
    ) -> Resp {
        if watched.iter().any(|token| token.changed(Engine::now())) {
            return Resp::Array(None);
        }
        let replies = commands
            .iter()
            .map(|args| {
                if args[0].eq_ignore_ascii_case(b"UNWATCH") {
                    return Resp::Simple("OK");
                }
                match self.data(session, args, true) {
                    Data::Reply(reply) => reply,
                    Data::Wait(..) => unreachable!("transactions never block"),
                }
            })
            .collect();
        Resp::Array(Some(replies))
    }

    fn unblock(&mut self, id: u64, error: bool) -> Resp {
        let Some(id) = u32::try_from(id).ok().filter(|id| {
            self.clients
                .get(id)
                .is_some_and(|client| client.blocked.is_some())
        }) else {
            return Resp::Integer(0);
        };
        let mut client = self.clients.remove(&id).expect("blocked session");
        let blocked = client.blocked.take().expect("blocked session");
        let reply = if error {
            Resp::Error("UNBLOCKED client unblocked via CLIENT UNBLOCK".into())
        } else {
            match blocked.wait {
                Wait::Pop(pop) => pop.null_reply(),
                Wait::Stream(_) => Resp::Array(None),
            }
        };
        self.complete(&mut client, &reply);
        self.clients.insert(id, client);
        Resp::Integer(1)
    }

    /// Shows a served blocked command's reply, then runs the lines entered
    /// meanwhile until one blocks again.
    fn complete(&mut self, client: &mut Client, reply: &Resp) {
        show(client, reply, false);
        while client.blocked.is_none() {
            let Some(args) = client.queued.pop_front() else {
                break;
            };
            self.run(client, &args);
        }
    }

    /// Serves blocked sessions whose data arrived or whose timeout passed, in
    /// the order they blocked, as the server's blocking actor wakes them.
    fn wake(&mut self) {
        loop {
            let mut waiting: Vec<(u64, u32)> = self
                .clients
                .iter()
                .filter_map(|(id, client)| client.blocked.as_ref().map(|b| (b.order, *id)))
                .collect();
            waiting.sort_unstable();
            let mut served = false;
            for (_, id) in waiting {
                let mut client = self.clients.remove(&id).expect("blocked session");
                if let Some(reply) = self.retry(&mut client) {
                    client.blocked = None;
                    self.complete(&mut client, &reply);
                    served = true;
                }
                self.clients.insert(id, client);
            }
            // A served BLMOVE may have fed a later waiter.
            if !served {
                break;
            }
        }
    }

    fn retry(&mut self, client: &mut Client) -> Option<Resp> {
        let blocked = client.blocked.as_mut()?;
        let timeout = blocked.deadline.is_some_and(|at| at <= Instant::now());
        self.engine.set_command_version(client.session.version);
        self.engine
            .set_command_session(Some(client.session.client.clone()));
        let reply = match &mut blocked.wait {
            Wait::Stream(read) => match self.engine.resume_blocked_read(read, timeout) {
                Ok(Execution::Reply(reply)) => Some(reply),
                Ok(Execution::StreamBlock(next)) if !timeout => {
                    *read = next;
                    None
                }
                Ok(Execution::StreamBlock(_)) => Some(Resp::Array(None)),
                Ok(_) => Some(Resp::Error("ERR invalid blocked read execution".into())),
                Err(error) => Some(io_error(&error)),
            },
            Wait::Pop(pop) if timeout => Some(pop.null_reply()),
            Wait::Pop(pop) => {
                // Only consumable list data may trigger the ordered rescan.
                if !matches!(pop.kind, BlockingKind::SortedPop { .. })
                    && !pop.keys.iter().any(|key| self.engine.list_ready(key))
                {
                    None
                } else {
                    match self.engine.retry_blocked(
                        &pop.keys,
                        pop.destination.as_ref(),
                        pop.right,
                        pop.kind,
                    ) {
                        Ok(Ok(reply)) => reply,
                        Ok(Err(reply)) => Some(reply),
                        Err(error) => Some(io_error(&error)),
                    }
                }
            }
        };
        self.finish();
        reply
    }
}

fn unknown_session(id: u32) -> String {
    serde_json::json!({
        "output": format!("ffkv-wasm: session {id} is not open\n"),
        "blocked": false,
        "prompt": "ffkv> ",
        "error": true,
    })
    .to_string()
}

/// Appends `reply` as ffkv-cli prints it: encoded for the session's protocol,
/// decoded by the CLI's decoder and rendered in its human format.
fn show(client: &mut Client, reply: &Resp, redact: bool) {
    let mut wire = BytesMut::new();
    reply.encode(&mut wire, client.session.version);
    print(client, &wire, redact);
}

fn print(client: &mut Client, wire: &[u8], redact: bool) {
    match crate::client::decode(wire) {
        Ok(mut frame) => {
            let reply = !frame.is_push();
            if reply && frame.has_error() && redact {
                frame = redacted_error(&frame);
            }
            client.failed |= reply && frame.has_error();
            client.output.extend(render(&frame, Format::Human));
        }
        Err(error) => {
            client.output.extend_from_slice(
                format!(
                    "ffkv-cli: read reply (outcome unknown; not retried) failed ({:?})\n",
                    error.kind()
                )
                .as_bytes(),
            );
            client.failed = true;
        }
    }
}

/// Writes queued Pub/Sub messages and subscription acknowledgements as the
/// server does: pushes in RESP3, arrays in RESP2.
fn deliver_messages(client: &mut Client) {
    while let Some(mut message) = client.session.subscription.try_recv() {
        // A publication is delivered when published; a closed gate means it
        // was never committed.
        if message.ready().now_or_never() == Some(false) {
            continue;
        }
        let version = client.session.version;
        let values = std::mem::take(&mut message.values);
        let mut wire = BytesMut::new();
        encode_length(
            if version == Version::Resp3 {
                b'>'
            } else {
                b'*'
            },
            values.len(),
            &mut wire,
        );
        for value in &values {
            value.encode(&mut wire, version);
        }
        print(client, &wire, false);
    }
}
