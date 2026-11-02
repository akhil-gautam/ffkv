//! Live connection controls and bounded server-assisted client cache tracking.
use crate::{
    protocol::{Resp, Version},
    pubsub::Hub,
};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::watch;

const MAX_TRACKED_KEYS: usize = 65_536;
const MAX_TRACKED_BYTES: usize = 8 * 1024 * 1024;
const MAX_PREFIXES: usize = 1024;

#[derive(Clone, Default)]
struct Tracking {
    enabled: bool,
    redirect: u64,
    bcast: bool,
    optin: bool,
    optout: bool,
    noloop: bool,
    broken: bool,
    prefixes: BTreeSet<Bytes>,
    caching: Option<bool>,
    current_caching: Option<bool>,
}
#[derive(Clone, Copy, Default, PartialEq, Eq)]
pub enum ClientKind {
    #[default]
    Normal,
    Master,
    Replica,
}
struct Metadata {
    fields: BTreeMap<String, String>,
    version: Version,
    database: u32,
    command: Bytes,
    connected: Instant,
    active: Instant,
    blocked: bool,
    native_wait: Option<watch::Sender<Option<bool>>>,
    no_evict: bool,
    no_touch: bool,
    reply_off: bool,
    reply_skip: u8,
    suppress_reply: bool,
    close_after_reply: bool,
    commands: u64,
    in_transaction: bool,
    tracking: Tracking,
    kind: ClientKind,
}
pub struct ClientControl {
    id: u64,
    metadata: Mutex<Metadata>,
}
/// A socket's active replication wait. Dropping the response future also
/// unregisters it, so cancellation cannot leak into the next command.
pub struct NativeWait<'a> {
    client: &'a ClientControl,
    receiver: watch::Receiver<Option<bool>>,
}
impl NativeWait<'_> {
    pub fn receiver(&self) -> watch::Receiver<Option<bool>> {
        self.receiver.clone()
    }
}
impl Drop for NativeWait<'_> {
    fn drop(&mut self) {
        let mut metadata = self.client.metadata.lock().expect("client control");
        metadata.native_wait = None;
        metadata.blocked = false;
    }
}
impl ClientControl {
    pub fn set_database(&self, database: u32) {
        self.metadata.lock().expect("client control").database = database;
    }
    pub fn set_blocked(&self, blocked: bool) {
        self.metadata.lock().expect("client control").blocked = blocked;
    }
    pub fn native_wait(&self) -> NativeWait<'_> {
        let (sender, receiver) = watch::channel(None);
        let mut metadata = self.metadata.lock().expect("client control");
        metadata.native_wait = Some(sender);
        metadata.blocked = true;
        NativeWait {
            client: self,
            receiver,
        }
    }
    pub fn set_field(&self, key: &str, value: String) {
        self.metadata
            .lock()
            .expect("client control")
            .fields
            .insert(key.into(), value);
    }
    pub fn set_version(&self, version: Version) {
        self.metadata.lock().expect("client control").version = version;
    }
    pub fn set_kind(&self, kind: ClientKind) {
        self.metadata.lock().expect("client control").kind = kind;
    }
    pub fn no_touch(&self) -> bool {
        self.metadata.lock().expect("client control").no_touch
    }
    pub fn close_after_reply(&self) -> bool {
        self.metadata
            .lock()
            .expect("client control")
            .close_after_reply
    }
    pub fn set_transaction(&self, active: bool) {
        self.metadata.lock().expect("client control").in_transaction = active;
    }
    pub fn should_reply(&self) -> bool {
        let metadata = self.metadata.lock().expect("client control");
        !metadata.suppress_reply
    }
    pub fn seen(&self, args: &[Bytes]) {
        let mut metadata = self.metadata.lock().expect("client control");
        metadata.suppress_reply = metadata.reply_off || metadata.reply_skip > 0;
        metadata.reply_skip = metadata.reply_skip.saturating_sub(1);
        metadata.active = Instant::now();
        metadata.command = args
            .first()
            .map(|v| Bytes::from(v.to_ascii_lowercase()))
            .unwrap_or_default();
        metadata.commands = metadata.commands.saturating_add(1);
        if !metadata.in_transaction {
            metadata.tracking.current_caching = metadata.tracking.caching.take();
        }
    }
    pub fn reset(&self) {
        let mut m = self.metadata.lock().expect("client control");
        m.database = 0;
        m.version = Version::Resp2;
        m.tracking = Tracking::default();
        m.reply_off = false;
        m.reply_skip = 0;
        m.no_evict = false;
        m.no_touch = false;
        m.suppress_reply = false;
        m.in_transaction = false;
        m.fields.insert("name".into(), String::new());
        m.fields.insert("lib-name".into(), String::new());
        m.fields.insert("lib-ver".into(), String::new());
    }
}
struct Inner {
    clients: BTreeMap<u64, Arc<ClientControl>>,
    tracked: BTreeMap<Bytes, BTreeSet<u64>>,
    tracked_bytes: usize,
}
#[derive(Clone, Copy)]
struct Pause {
    until: Instant,
    writes_only: bool,
}
pub struct Registry {
    inner: Mutex<Inner>,
    hub: Arc<Hub>,
    pause: watch::Sender<Option<Pause>>,
}
impl Registry {
    pub fn new(hub: Arc<Hub>) -> Self {
        let (pause, _) = watch::channel(None);
        Self {
            inner: Mutex::new(Inner {
                clients: BTreeMap::new(),
                tracked: BTreeMap::new(),
                tracked_bytes: 0,
            }),
            hub,
            pause,
        }
    }
    pub fn register(&self, id: u64, initial_info: Bytes) -> Arc<ClientControl> {
        let fields = String::from_utf8_lossy(&initial_info)
            .split_whitespace()
            .filter_map(|field| {
                field
                    .split_once('=')
                    .map(|(k, v)| (k.to_owned(), v.to_owned()))
            })
            .collect();
        let now = Instant::now();
        let client = Arc::new(ClientControl {
            id,
            metadata: Mutex::new(Metadata {
                fields,
                version: Version::Resp2,
                database: 0,
                command: Bytes::new(),
                connected: now,
                active: now,
                kind: ClientKind::Normal,
                blocked: false,
                native_wait: None,
                no_evict: false,
                no_touch: false,
                reply_off: false,
                reply_skip: 0,
                suppress_reply: false,
                close_after_reply: false,
                commands: 0,
                in_transaction: false,
                tracking: Tracking::default(),
            }),
        });
        self.inner
            .lock()
            .expect("client registry")
            .clients
            .insert(id, client.clone());
        client
    }
    pub fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock().expect("client registry");
        inner.clients.remove(&id);
        Self::forget(&mut inner, id);
        for client in inner.clients.values() {
            let mut m = client.metadata.lock().expect("client control");
            if m.tracking.enabled && m.tracking.redirect == id && !m.tracking.broken {
                m.tracking.broken = true;
                if m.version == Version::Resp3 {
                    let _ = self.hub.push(
                        client.id,
                        vec![bulk(b"tracking-redir-broken"), Resp::Integer(id as i64)],
                    );
                }
            }
        }
    }
    fn forget(inner: &mut Inner, id: u64) {
        let mut released = 0;
        inner.tracked.retain(|key, clients| {
            if clients.remove(&id) {
                released += 8;
            }
            if clients.is_empty() {
                released += key.len();
                false
            } else {
                true
            }
        });
        inner.tracked_bytes -= released;
    }
    pub fn reset_tracking(&self, id: u64) {
        let mut inner = self.inner.lock().expect("client registry");
        Self::forget(&mut inner, id);
        if let Some(client) = inner.clients.get(&id) {
            client.metadata.lock().expect("client control").tracking = Tracking::default();
        }
    }
    pub fn control(&self, id: u64) -> Option<Arc<ClientControl>> {
        self.inner
            .lock()
            .expect("client registry")
            .clients
            .get(&id)
            .cloned()
    }
    pub fn blocked_count(&self) -> usize {
        self.inner
            .lock()
            .expect("client registry")
            .clients
            .values()
            .filter(|client| client.metadata.lock().expect("client control").blocked)
            .count()
    }
    pub fn set_blocked(&self, id: u64, blocked: bool) {
        if let Some(c) = self.inner.lock().expect("client registry").clients.get(&id) {
            c.set_blocked(blocked);
        }
    }
    pub fn unblock_native(&self, id: u64, error: bool) -> bool {
        let Some(client) = self.control(id) else {
            return false;
        };
        let mut metadata = client.metadata.lock().expect("client control");
        metadata
            .native_wait
            .take()
            .is_some_and(|sender| sender.send(Some(error)).is_ok())
    }
    pub fn highest_database(&self) -> u32 {
        self.inner
            .lock()
            .expect("client registry")
            .clients
            .values()
            .map(|client| client.metadata.lock().expect("client control").database)
            .max()
            .unwrap_or(0)
    }
    pub async fn wait_unpaused(&self, write: bool) {
        let mut pause = self.pause.subscribe();
        loop {
            let current = *pause.borrow_and_update();
            let Some(current) = current else {
                return;
            };
            if (current.writes_only && !write) || current.until <= Instant::now() {
                return;
            }
            tokio::select! { _ = tokio::time::sleep_until(current.until.into()) => return, changed = pause.changed() => { if changed.is_err() { return; } } }
        }
    }
    fn notify(&self, inner: &Inner, source: &Arc<ClientControl>, keys: Option<Vec<Bytes>>) {
        let (target, version) = {
            let m = source.metadata.lock().expect("client control");
            (
                if m.tracking.redirect == 0 {
                    source.id
                } else {
                    m.tracking.redirect
                },
                m.version,
            )
        };
        let Some(destination) = inner.clients.get(&target) else {
            let mut m = source.metadata.lock().expect("client control");
            if !m.tracking.broken {
                m.tracking.broken = true;
                if version == Version::Resp3 {
                    let _ = self.hub.push(
                        source.id,
                        vec![bulk(b"tracking-redir-broken"), Resp::Integer(target as i64)],
                    );
                }
            }
            return;
        };
        let version = destination.metadata.lock().expect("client control").version;
        let keys = match keys {
            Some(keys) => Resp::Array(Some(
                keys.into_iter().map(|k| Resp::Bulk(Some(k))).collect(),
            )),
            None => Resp::Array(None),
        };
        if version == Version::Resp3 {
            let _ = self.hub.push(target, vec![bulk(b"invalidate"), keys]);
        } else if self.hub.has_channel(target, b"__redis__:invalidate") {
            let _ = self.hub.push(
                target,
                vec![bulk(b"message"), bulk(b"__redis__:invalidate"), keys],
            );
        }
    }
    // Redis deliberately shares the tracking namespace across logical databases.
    pub fn read(&self, id: u64, _database: u32, keys: &[Bytes]) {
        let mut inner = self.inner.lock().expect("client registry");
        let Some(client) = inner.clients.get(&id).cloned() else {
            return;
        };
        {
            let m = client.metadata.lock().expect("client control");
            let t = &m.tracking;
            if !t.enabled
                || t.bcast
                || (t.optin && t.current_caching != Some(true))
                || (t.optout && t.current_caching == Some(false))
            {
                return;
            }
        }
        for key in keys {
            if inner
                .tracked
                .get(key)
                .is_some_and(|clients| clients.contains(&id))
            {
                continue;
            }
            if key.len().saturating_add(8) > MAX_TRACKED_BYTES {
                self.notify(&inner, &client, Some(vec![Bytes::copy_from_slice(key)]));
                continue;
            }
            while (!inner.tracked.contains_key(key) && inner.tracked.len() >= MAX_TRACKED_KEYS)
                || inner
                    .tracked_bytes
                    .saturating_add(if inner.tracked.contains_key(key) {
                        8
                    } else {
                        key.len() + 8
                    })
                    > MAX_TRACKED_BYTES
            {
                let Some((old, clients)) = inner.tracked.pop_first() else {
                    break;
                };
                inner.tracked_bytes -= old.len() + clients.len() * 8;
                for id in clients {
                    if let Some(c) = inner.clients.get(&id) {
                        self.notify(&inner, c, Some(vec![old.clone()]));
                    }
                }
            }
            inner.tracked_bytes += 8;
            if let Some(clients) = inner.tracked.get_mut(key) {
                clients.insert(id);
            } else {
                inner.tracked_bytes += key.len();
                inner
                    .tracked
                    .insert(Bytes::copy_from_slice(key), BTreeSet::from([id]));
            }
        }
    }
    pub fn invalidate(&self, _database: u32, keys: &[Bytes], origin: Option<u64>) {
        let mut inner = self.inner.lock().expect("client registry");
        let mut notices: BTreeMap<u64, BTreeSet<Bytes>> = BTreeMap::new();
        for key in keys {
            if let Some(clients) = inner.tracked.remove(key) {
                inner.tracked_bytes -= key.len() + clients.len() * 8;
                for id in clients {
                    notices.entry(id).or_default().insert(key.clone());
                }
            }
            for (&id, client) in &inner.clients {
                let m = client.metadata.lock().expect("client control");
                let t = &m.tracking;
                if t.enabled && t.bcast && t.prefixes.iter().any(|p| key.starts_with(p)) {
                    notices.entry(id).or_default().insert(key.clone());
                }
            }
        }
        for (id, keys) in notices {
            if let Some(client) = inner.clients.get(&id) {
                {
                    let m = client.metadata.lock().expect("client control");
                    if !m.tracking.enabled || (m.tracking.noloop && origin == Some(id)) {
                        continue;
                    }
                }
                self.notify(&inner, client, Some(keys.into_iter().collect()));
            }
        }
    }
    pub fn invalidate_all(&self, _database: Option<u32>, origin: Option<u64>) {
        let mut inner = self.inner.lock().expect("client registry");
        inner.tracked.clear();
        inner.tracked_bytes = 0;
        for (&id, client) in &inner.clients {
            {
                let m = client.metadata.lock().expect("client control");
                if !m.tracking.enabled || (m.tracking.noloop && origin == Some(id)) {
                    continue;
                }
            }
            self.notify(&inner, client, None);
        }
    }
    pub fn info(&self, id: u64) -> Bytes {
        self.list(Some(&[id]), None)
    }
    pub fn list(&self, ids: Option<&[u64]>, kind: Option<&str>) -> Bytes {
        use std::fmt::Write;
        let inner = self.inner.lock().expect("client registry");
        let mut result = String::new();
        for (&id, client) in &inner.clients {
            if ids.is_some_and(|ids| !ids.contains(&id)) {
                continue;
            }
            let m = client.metadata.lock().expect("client control");
            let (sub, psub, ssub) = self.hub.counts(id);
            let pubsub = sub + psub + ssub > 0;
            if kind.is_some_and(|kind| !matches_kind(kind, m.kind, pubsub)) {
                continue;
            }
            let mut flags = String::new();
            match m.kind {
                ClientKind::Master => flags.push('M'),
                ClientKind::Replica => flags.push('S'),
                ClientKind::Normal => {}
            }
            if pubsub {
                flags.push('P');
            }
            if m.blocked {
                flags.push('b');
            }
            if m.tracking.enabled {
                flags.push('t');
            }
            if m.tracking.bcast {
                flags.push('B');
            }
            if m.tracking.broken {
                flags.push('R');
            }
            if m.no_evict {
                flags.push('e');
            }
            if m.no_touch {
                flags.push('T');
            }
            if m.close_after_reply {
                flags.push('c');
            }
            if flags.is_empty() {
                flags.push('N');
            }
            let field = |key: &str| m.fields.get(key).map(String::as_str).unwrap_or("");
            let _ = writeln!(
                result,
                "id={id} addr={} laddr={} name={} age={} idle={} flags={flags} db={} sub={sub} psub={psub} ssub={ssub} cmd={} user={} resp={} lib-name={} lib-ver={} redir={} tot-cmds={}",
                field("addr"),
                field("laddr"),
                field("name"),
                m.connected.elapsed().as_secs(),
                m.active.elapsed().as_secs(),
                m.database,
                String::from_utf8_lossy(&m.command),
                field("user"),
                if m.version == Version::Resp3 { 3 } else { 2 },
                field("lib-name"),
                field("lib-ver"),
                if m.tracking.enabled {
                    m.tracking.redirect as i64
                } else {
                    -1
                },
                m.commands
            );
        }
        Bytes::from(result)
    }
    pub fn command(&self, client: &Arc<ClientControl>, args: &[Bytes]) -> Resp {
        let Some(sub) = args.get(1) else {
            return err("wrong number of arguments for CLIENT");
        };
        if sub.eq_ignore_ascii_case(b"INFO") && args.len() == 2 {
            return Resp::Bulk(Some(self.info(client.id)));
        }
        if sub.eq_ignore_ascii_case(b"LIST") {
            let mut ids = None;
            let mut kind = None;
            let mut pos = 2;
            while pos < args.len() {
                if args[pos].eq_ignore_ascii_case(b"TYPE") && pos + 1 < args.len() {
                    let mut value = String::from_utf8_lossy(&args[pos + 1]).to_ascii_lowercase();
                    if value == "slave" {
                        value = "replica".into();
                    }
                    if !matches!(value.as_str(), "normal" | "master" | "replica" | "pubsub") {
                        return err("Unknown client type");
                    }
                    kind = Some(value);
                    pos += 2;
                } else if args[pos].eq_ignore_ascii_case(b"ID") && pos + 1 < args.len() {
                    let parsed: Result<Vec<_>, _> =
                        args[pos + 1..].iter().map(|v| uint(v)).collect();
                    match parsed {
                        Ok(parsed) => ids = Some(parsed),
                        Err(()) => return err("Invalid client ID"),
                    }
                    break;
                } else {
                    return err("syntax error");
                }
            }
            return Resp::Bulk(Some(self.list(ids.as_deref(), kind.as_deref())));
        }
        if sub.eq_ignore_ascii_case(b"KILL") {
            return self.kill(client.id, args);
        }
        if sub.eq_ignore_ascii_case(b"PAUSE") && (args.len() == 3 || args.len() == 4) {
            let Ok(timeout) = uint(&args[2]) else {
                return err("timeout is not an integer or out of range");
            };
            let writes_only = args
                .get(3)
                .is_some_and(|v| v.eq_ignore_ascii_case(b"WRITE"));
            if args.get(3).is_some_and(|v| {
                !v.eq_ignore_ascii_case(b"ALL") && !v.eq_ignore_ascii_case(b"WRITE")
            }) {
                return err("CLIENT PAUSE mode must be WRITE or ALL");
            }
            let Some(until) = Instant::now().checked_add(Duration::from_millis(timeout)) else {
                return err("timeout is out of range");
            };
            self.pause.send_replace(Some(Pause { until, writes_only }));
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"UNPAUSE") && args.len() == 2 {
            self.pause.send_replace(None);
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"TRACKING") {
            return self.tracking(client, args);
        }
        let mut m = client.metadata.lock().expect("client control");
        if sub.eq_ignore_ascii_case(b"REPLY") && args.len() == 3 {
            if args[2].eq_ignore_ascii_case(b"ON") {
                m.reply_off = false;
                m.reply_skip = 0;
                m.suppress_reply = false;
            } else if args[2].eq_ignore_ascii_case(b"OFF") {
                m.reply_off = true;
                m.suppress_reply = true;
            } else if args[2].eq_ignore_ascii_case(b"SKIP") {
                m.suppress_reply = true;
                if !m.reply_off {
                    m.reply_skip = 1;
                }
            } else {
                return err("syntax error");
            }
            return Resp::Simple("OK");
        }
        if (sub.eq_ignore_ascii_case(b"NO-EVICT") || sub.eq_ignore_ascii_case(b"NO-TOUCH"))
            && args.len() == 3
        {
            let value = if args[2].eq_ignore_ascii_case(b"ON") {
                true
            } else if args[2].eq_ignore_ascii_case(b"OFF") {
                false
            } else {
                return err("syntax error");
            };
            if sub.eq_ignore_ascii_case(b"NO-EVICT") {
                m.no_evict = value;
            } else {
                m.no_touch = value;
            }
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"GETREDIR") && args.len() == 2 {
            return Resp::Integer(if m.tracking.enabled {
                m.tracking.redirect as i64
            } else {
                -1
            });
        }
        if sub.eq_ignore_ascii_case(b"CACHING") && args.len() == 3 {
            if !m.tracking.enabled || (!m.tracking.optin && !m.tracking.optout) {
                return err(
                    "CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
                );
            }
            let value = if args[2].eq_ignore_ascii_case(b"YES") {
                true
            } else if args[2].eq_ignore_ascii_case(b"NO") {
                false
            } else {
                return err("syntax error");
            };
            m.tracking.caching = Some(value);
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"TRACKINGINFO") && args.len() == 2 {
            let t = &m.tracking;
            let mut flags = vec![bulk(if t.enabled { b"on" } else { b"off" })];
            for (set, name) in [
                (t.bcast, b"bcast".as_slice()),
                (t.optin, b"optin"),
                (t.optout, b"optout"),
                (t.noloop, b"noloop"),
                (t.broken, b"broken_redirect"),
            ] {
                if set {
                    flags.push(bulk(name));
                }
            }
            if t.caching == Some(true) {
                flags.push(bulk(b"caching-yes"));
            } else if t.caching == Some(false) {
                flags.push(bulk(b"caching-no"));
            }
            return Resp::Map(vec![
                (bulk(b"flags"), Resp::Array(Some(flags))),
                (
                    bulk(b"redirect"),
                    Resp::Integer(if t.enabled { t.redirect as i64 } else { -1 }),
                ),
                (
                    bulk(b"prefixes"),
                    Resp::Array(Some(
                        t.prefixes
                            .iter()
                            .cloned()
                            .map(|p| Resp::Bulk(Some(p)))
                            .collect(),
                    )),
                ),
            ]);
        }
        err("unknown CLIENT subcommand or wrong number of arguments")
    }
    fn tracking(&self, client: &Arc<ClientControl>, args: &[Bytes]) -> Resp {
        if args.len() < 3 {
            return err("wrong number of arguments for CLIENT TRACKING");
        }
        let on = if args[2].eq_ignore_ascii_case(b"ON") {
            true
        } else if args[2].eq_ignore_ascii_case(b"OFF") {
            false
        } else {
            return err("syntax error");
        };
        let mut inner = self.inner.lock().expect("client registry");
        let old = client
            .metadata
            .lock()
            .expect("client control")
            .tracking
            .clone();
        let mut t = if on && old.enabled {
            old.clone()
        } else {
            Tracking::default()
        };
        t.enabled = on;
        let mut pos = 3;
        while pos < args.len() {
            let option = &args[pos];
            if option.eq_ignore_ascii_case(b"REDIRECT") && pos + 1 < args.len() {
                let Ok(id) = uint(&args[pos + 1]) else {
                    return err("Invalid client ID");
                };
                if id != 0 && !inner.clients.contains_key(&id) {
                    return err("The client ID you want redirect to does not exist");
                }
                t.redirect = id;
                t.broken = false;
                pos += 2;
            } else if option.eq_ignore_ascii_case(b"PREFIX") && pos + 1 < args.len() {
                if t.prefixes.len() >= MAX_PREFIXES
                    || t.prefixes
                        .iter()
                        .map(Bytes::len)
                        .sum::<usize>()
                        .saturating_add(args[pos + 1].len())
                        > MAX_TRACKED_BYTES
                {
                    return err("tracking prefix limit exceeded");
                }
                t.prefixes.insert(Bytes::copy_from_slice(&args[pos + 1]));
                pos += 2;
            } else {
                if option.eq_ignore_ascii_case(b"BCAST") {
                    t.bcast = true;
                } else if option.eq_ignore_ascii_case(b"OPTIN") {
                    t.optin = true;
                } else if option.eq_ignore_ascii_case(b"OPTOUT") {
                    t.optout = true;
                } else if option.eq_ignore_ascii_case(b"NOLOOP") {
                    t.noloop = true;
                } else {
                    return err("syntax error");
                }
                pos += 1;
            }
        }
        if t.optin && t.optout || t.bcast && (t.optin || t.optout) {
            return err("OPTIN and OPTOUT are not compatible with BCAST or each other");
        }
        if !t.bcast && !t.prefixes.is_empty() {
            return err("PREFIX option requires BCAST mode");
        }
        if old.enabled && old.bcast != t.bcast && on {
            return err("You can't switch BCAST mode on/off before disabling tracking");
        }
        let mut previous: Option<&Bytes> = None;
        for prefix in &t.prefixes {
            if previous.is_some_and(|p| prefix.starts_with(p)) {
                return err("Prefixes cannot overlap");
            }
            previous = Some(prefix);
        }
        if t.bcast && t.prefixes.is_empty() {
            t.prefixes.insert(Bytes::new());
        }
        if !on {
            t = Tracking::default();
        }
        if !on {
            Self::forget(&mut inner, client.id);
        }
        client.metadata.lock().expect("client control").tracking = t;
        Resp::Simple("OK")
    }
    fn kill(&self, own_id: u64, args: &[Bytes]) -> Resp {
        let legacy = args.len() == 3;
        let mut id = None;
        let mut addr = None;
        let mut laddr = None;
        let mut user = None;
        let mut kind = None;
        let mut skipme = !legacy;
        let mut maxage = None;
        if legacy {
            addr = Some(String::from_utf8_lossy(&args[2]).into_owned());
        } else {
            if args.len() < 4 || !args.len().is_multiple_of(2) {
                return err("syntax error");
            }
            for pair in args[2..].as_chunks::<2>().0 {
                let option = &pair[0];
                let value = &pair[1];
                if option.eq_ignore_ascii_case(b"ID") {
                    let Ok(parsed) = uint(value) else {
                        return err("Invalid client ID");
                    };
                    id = Some(parsed);
                } else if option.eq_ignore_ascii_case(b"ADDR") {
                    addr = Some(String::from_utf8_lossy(value).into_owned());
                } else if option.eq_ignore_ascii_case(b"LADDR") {
                    laddr = Some(String::from_utf8_lossy(value).into_owned());
                } else if option.eq_ignore_ascii_case(b"USER") {
                    user = Some(String::from_utf8_lossy(value).into_owned());
                } else if option.eq_ignore_ascii_case(b"TYPE") {
                    let mut v = String::from_utf8_lossy(value).to_ascii_lowercase();
                    if v == "slave" {
                        v = "replica".into();
                    }
                    if !matches!(v.as_str(), "normal" | "pubsub" | "master" | "replica") {
                        return err("Unknown client type");
                    }
                    kind = Some(v);
                } else if option.eq_ignore_ascii_case(b"MAXAGE") {
                    let Ok(parsed) = uint(value) else {
                        return err("Invalid maxage");
                    };
                    maxage = Some(parsed);
                } else if option.eq_ignore_ascii_case(b"SKIPME") {
                    if value.eq_ignore_ascii_case(b"YES") {
                        skipme = true;
                    } else if value.eq_ignore_ascii_case(b"NO") {
                        skipme = false;
                    } else {
                        return err("syntax error");
                    }
                } else {
                    return err("syntax error");
                }
            }
        }
        let inner = self.inner.lock().expect("client registry");
        let mut count = 0;
        for (&client_id, client) in &inner.clients {
            if skipme && client_id == own_id || id.is_some_and(|id| id != client_id) {
                continue;
            }
            let mut m = client.metadata.lock().expect("client control");
            if addr
                .as_ref()
                .is_some_and(|v| m.fields.get("addr") != Some(v))
                || laddr
                    .as_ref()
                    .is_some_and(|v| m.fields.get("laddr") != Some(v))
                || user
                    .as_ref()
                    .is_some_and(|v| m.fields.get("user") != Some(v))
                || maxage.is_some_and(|v| m.connected.elapsed().as_secs() < v)
            {
                continue;
            }
            let (s, p, ss) = self.hub.counts(client_id);
            let pubsub = s + p + ss > 0;
            if kind
                .as_deref()
                .is_some_and(|kind| !matches_kind(kind, m.kind, pubsub))
            {
                continue;
            }
            if client_id == own_id {
                m.close_after_reply = true;
            } else {
                self.hub.disconnect(client_id);
            }
            count += 1;
        }
        if legacy {
            if count > 0 {
                Resp::Simple("OK")
            } else {
                err("No such client")
            }
        } else {
            Resp::Integer(count)
        }
    }
}
fn matches_kind(filter: &str, kind: ClientKind, pubsub: bool) -> bool {
    match filter {
        "normal" => kind == ClientKind::Normal && !pubsub,
        "pubsub" => kind == ClientKind::Normal && pubsub,
        "master" => kind == ClientKind::Master,
        "replica" => kind == ClientKind::Replica,
        _ => false,
    }
}
fn bulk(value: &[u8]) -> Resp {
    Resp::Bulk(Some(Bytes::copy_from_slice(value)))
}
fn err(value: &str) -> Resp {
    Resp::Error(format!("ERR {value}"))
}
fn uint(value: &[u8]) -> Result<u64, ()> {
    let value = std::str::from_utf8(value).map_err(|_| ())?;
    if value.is_empty() || !value.bytes().all(|b| b.is_ascii_digit()) {
        return Err(());
    }
    value.parse().map_err(|_| ())
}
