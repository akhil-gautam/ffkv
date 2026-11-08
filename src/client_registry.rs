//! Live connection controls and bounded server-assisted client cache tracking.
use crate::{
    protocol::{Resp, Version},
    pubsub::Hub,
};
use bytes::Bytes;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU8, AtomicU32, AtomicUsize, Ordering},
    },
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
    command: Bytes,
    connected: Instant,
    active: Instant,
    blocked: bool,
    native_wait: Option<watch::Sender<Option<bool>>>,
    no_evict: bool,
    no_touch: bool,
    reply_off: bool,
    reply_skip: u8,
    commands: u64,
    in_transaction: bool,
    tracking: Tracking,
    kind: ClientKind,
}
/// One connection's control state. Fields its connection reads after every
/// command sit beside `metadata` as atomics, each stored only while holding
/// `metadata`, so a reader holding it sees them fixed as before, while the
/// connection reads them, or finds a value unchanged, without the lock.
pub struct ClientControl {
    id: u64,
    metadata: Mutex<Metadata>,
    database: AtomicU32,
    suppress_reply: AtomicBool,
    close_after_reply: AtomicBool,
    /// The owning registry's count of tracking-enabled clients.
    tracking_clients: Arc<AtomicUsize>,
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
    /// Stores nothing when `database` is already the one stored, as after
    /// most commands.
    pub fn set_database(&self, database: u32) {
        if self.database.load(Ordering::Relaxed) != database {
            let _metadata = self.metadata.lock().expect("client control");
            self.database.store(database, Ordering::Relaxed);
        }
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
        self.close_after_reply.load(Ordering::Relaxed)
    }
    pub fn set_transaction(&self, active: bool) {
        self.metadata.lock().expect("client control").in_transaction = active;
    }
    pub fn should_reply(&self) -> bool {
        !self.suppress_reply.load(Ordering::Relaxed)
    }
    /// Records one command, `command` being its lowercase name, observed at `at`.
    pub fn seen(&self, command: Bytes, at: Instant) {
        let mut metadata = self.metadata.lock().expect("client control");
        self.suppress_reply.store(
            metadata.reply_off || metadata.reply_skip > 0,
            Ordering::Relaxed,
        );
        metadata.reply_skip = metadata.reply_skip.saturating_sub(1);
        metadata.active = at;
        metadata.command = command;
        metadata.commands = metadata.commands.saturating_add(1);
        if !metadata.in_transaction {
            metadata.tracking.current_caching = metadata.tracking.caching.take();
        }
    }
    pub fn reset(&self) {
        let mut m = self.metadata.lock().expect("client control");
        self.database.store(0, Ordering::Relaxed);
        m.version = Version::Resp2;
        self.replace_tracking(&mut m, Tracking::default());
        m.reply_off = false;
        m.reply_skip = 0;
        m.no_evict = false;
        m.no_touch = false;
        self.suppress_reply.store(false, Ordering::Relaxed);
        m.in_transaction = false;
        m.fields.insert("name".into(), String::new());
        m.fields.insert("lib-name".into(), String::new());
        m.fields.insert("lib-ver".into(), String::new());
    }
    /// Replaces tracking state while the caller holds `metadata`'s lock. Enabling
    /// is counted before the enabled state becomes observable, and disabling is
    /// uncounted only after the disabled state is stored, so a zero
    /// `Registry::tracking_active` never hides an enabled client.
    fn replace_tracking(&self, metadata: &mut Metadata, tracking: Tracking) {
        let (was, now) = (metadata.tracking.enabled, tracking.enabled);
        if now && !was {
            self.tracking_clients.fetch_add(1, Ordering::SeqCst);
        }
        metadata.tracking = tracking;
        if was && !now {
            self.tracking_clients.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
struct Inner {
    clients: BTreeMap<u64, Arc<ClientControl>>,
    /// Superset of registered clients with enabled BCAST tracking, so writes do
    /// not inspect unrelated clients. Entries left by `ClientControl::reset` are
    /// filtered by metadata and dropped on the next registry-side transition.
    broadcasters: BTreeMap<u64, Arc<ClientControl>>,
    tracked: BTreeMap<Bytes, BTreeSet<u64>>,
    tracked_bytes: usize,
}
/// `Registry::pause_scope` values, ordered by the commands a CLIENT PAUSE
/// holds: none, writes only, or every command.
const UNPAUSED: u8 = 0;
const WRITES_PAUSED: u8 = 1;
const ALL_PAUSED: u8 = 2;
#[derive(Clone, Copy)]
struct Pause {
    /// On Tokio's clock: the monotonic clock, unless a test pauses it.
    until: tokio::time::Instant,
    writes_only: bool,
}
impl Pause {
    /// The scope `pause` holds at `now`; time only ever narrows it.
    fn scope(pause: Option<Self>, now: tokio::time::Instant) -> u8 {
        match pause {
            Some(pause) if pause.until > now && pause.writes_only => WRITES_PAUSED,
            Some(pause) if pause.until > now => ALL_PAUSED,
            _ => UNPAUSED,
        }
    }
}
pub struct Registry {
    inner: Mutex<Inner>,
    tracking_clients: Arc<AtomicUsize>,
    hub: Arc<Hub>,
    pause: watch::Sender<Option<Pause>>,
    /// Lock-free admission summary of `pause`, never narrower than the scope
    /// the published pause holds now. It is stored only inside `pause`'s write
    /// lock, as the exact scope of the value there at that instant: with each
    /// publication, and when an admission finds the pause expired. Between
    /// those critical sections the value is fixed and its scope only narrows,
    /// so the bound holds throughout. Its modification order is the lock's
    /// order, so a load that happens-after a publication reads that store or
    /// one from a later critical section, which saw the pause superseded or
    /// expired before the load.
    pause_scope: AtomicU8,
}
impl Registry {
    pub fn new(hub: Arc<Hub>) -> Self {
        let (pause, _) = watch::channel(None);
        Self {
            inner: Mutex::new(Inner {
                clients: BTreeMap::new(),
                broadcasters: BTreeMap::new(),
                tracked: BTreeMap::new(),
                tracked_bytes: 0,
            }),
            tracking_clients: Arc::new(AtomicUsize::new(0)),
            hub,
            pause,
            pause_scope: AtomicU8::new(UNPAUSED),
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
                commands: 0,
                in_transaction: false,
                tracking: Tracking::default(),
            }),
            database: AtomicU32::new(0),
            suppress_reply: AtomicBool::new(false),
            close_after_reply: AtomicBool::new(false),
            tracking_clients: self.tracking_clients.clone(),
        });
        let mut inner = self.inner.lock().expect("client registry");
        if let Some(previous) = inner.clients.insert(id, client.clone()) {
            Self::stop_tracking(&mut inner, id, Some(&*previous));
        }
        client
    }
    pub fn unregister(&self, id: u64) {
        let mut inner = self.inner.lock().expect("client registry");
        let removed = inner.clients.remove(&id);
        Self::stop_tracking(&mut inner, id, removed.as_deref());
        // Only enabled clients redirect, and enabling is counted under this lock.
        if !self.tracking_active() {
            return;
        }
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
    /// Drops `id`'s registry-side tracking state, then disables `client`.
    fn stop_tracking(inner: &mut Inner, id: u64, client: Option<&ClientControl>) {
        Self::forget(inner, id);
        inner.broadcasters.remove(&id);
        if let Some(client) = client {
            let mut m = client.metadata.lock().expect("client control");
            client.replace_tracking(&mut m, Tracking::default());
        }
    }
    /// Whether any client may have tracking enabled. Lock-free; a `false` never
    /// hides an enable that happens-before the call, because enabling counts
    /// before publishing its state and disabling uncounts after clearing it.
    pub fn tracking_active(&self) -> bool {
        self.tracking_clients.load(Ordering::SeqCst) != 0
    }
    pub fn reset_tracking(&self, id: u64) {
        let mut inner = self.inner.lock().expect("client registry");
        let client = inner.clients.get(&id).cloned();
        Self::stop_tracking(&mut inner, id, client.as_deref());
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
            .map(|client| client.database.load(Ordering::Relaxed))
            .max()
            .unwrap_or(0)
    }
    /// Whether a CLIENT PAUSE may hold this command, a write if `write`.
    /// `false` is `wait_unpaused`'s immediate admission, at the same
    /// `pause_scope` load, so a caller may skip waiting altogether.
    pub fn may_hold(&self, write: bool) -> bool {
        let holds = if write { WRITES_PAUSED } else { ALL_PAUSED };
        self.pause_scope.load(Ordering::SeqCst) >= holds
    }
    /// Waits until no CLIENT PAUSE holds this command, a write if `write`.
    ///
    /// Admission linearizes at the `pause_scope` load when that scope is too
    /// narrow to hold the command, which subscribes to nothing; otherwise at
    /// the channel read that admits it, which samples the clock under the same
    /// read lock, so no publication falls between the pause it sees and the
    /// instant it judges that pause at. Waiting loses no wakeup: each pass
    /// marks the version it read, `changed` requests notification before it
    /// checks for a later one, and an elapsed timer only re-reads, so a
    /// replacement or extension ready alongside an older deadline still holds.
    pub async fn wait_unpaused(&self, write: bool) {
        if !self.may_hold(write) {
            return;
        }
        let mut pause = self.pause.subscribe();
        loop {
            let (current, now) = {
                let current = pause.borrow_and_update();
                (*current, tokio::time::Instant::now())
            };
            #[cfg(test)]
            if let Some(hook) = tests::AFTER_PAUSE_SNAPSHOT.with(|slot| slot.borrow_mut().take()) {
                hook();
            }
            let Some(current) = current else {
                return;
            };
            if current.until <= now {
                self.narrow_pause_scope();
                return;
            }
            if current.writes_only && !write {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep_until(current.until) => {}
                changed = pause.changed() => {
                    if changed.is_err() {
                        return;
                    }
                }
            }
        }
    }
    /// Narrows `pause_scope` to what the published pause holds now, after an
    /// admission found a pause expired, so later admissions skip the channel
    /// again. Recomputed under the write lock, it never hides a publication.
    fn narrow_pause_scope(&self) {
        if self.pause_scope.load(Ordering::SeqCst) == UNPAUSED {
            return;
        }
        self.pause.send_if_modified(|pause| {
            let scope = Pause::scope(*pause, tokio::time::Instant::now());
            self.pause_scope.store(scope, Ordering::SeqCst);
            false
        });
    }
    /// Publishes a CLIENT PAUSE, or UNPAUSE as `None`, with its scope in the
    /// same critical section.
    fn publish_pause(&self, pause: Option<Pause>) {
        self.pause.send_modify(|current| {
            *current = pause;
            let scope = Pause::scope(pause, tokio::time::Instant::now());
            self.pause_scope.store(scope, Ordering::SeqCst);
        });
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
        if !self.tracking_active() {
            return;
        }
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
        if !self.tracking_active() {
            return;
        }
        let mut inner = self.inner.lock().expect("client registry");
        let mut notices: BTreeMap<u64, BTreeSet<Bytes>> = BTreeMap::new();
        for key in keys {
            if let Some(clients) = inner.tracked.remove(key) {
                inner.tracked_bytes -= key.len() + clients.len() * 8;
                for id in clients {
                    notices.entry(id).or_default().insert(key.clone());
                }
            }
        }
        for (&id, client) in &inner.broadcasters {
            let m = client.metadata.lock().expect("client control");
            let t = &m.tracking;
            if !t.enabled || !t.bcast {
                continue;
            }
            for key in keys {
                if t.prefixes.iter().any(|p| key.starts_with(p)) {
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
        // Enabling is counted under this lock; only enabled clients are notified.
        if !self.tracking_active() {
            return;
        }
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
            if client.close_after_reply.load(Ordering::Relaxed) {
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
                client.database.load(Ordering::Relaxed),
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
            let deadline = tokio::time::Instant::now().checked_add(Duration::from_millis(timeout));
            let Some(until) = deadline else {
                return err("timeout is out of range");
            };
            self.publish_pause(Some(Pause { until, writes_only }));
            return Resp::Simple("OK");
        }
        if sub.eq_ignore_ascii_case(b"UNPAUSE") && args.len() == 2 {
            self.publish_pause(None);
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
                client.suppress_reply.store(false, Ordering::Relaxed);
            } else if args[2].eq_ignore_ascii_case(b"OFF") {
                m.reply_off = true;
                client.suppress_reply.store(true, Ordering::Relaxed);
            } else if args[2].eq_ignore_ascii_case(b"SKIP") {
                client.suppress_reply.store(true, Ordering::Relaxed);
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
            Self::forget(&mut inner, client.id);
        }
        // Only the registered control is indexed, as only it can be notified.
        if inner
            .clients
            .get(&client.id)
            .is_some_and(|registered| Arc::ptr_eq(registered, client))
        {
            if t.enabled && t.bcast {
                inner.broadcasters.insert(client.id, client.clone());
            } else {
                inner.broadcasters.remove(&client.id);
            }
        }
        let mut m = client.metadata.lock().expect("client control");
        client.replace_tracking(&mut m, t);
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
            let m = client.metadata.lock().expect("client control");
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
                client.close_after_reply.store(true, Ordering::Relaxed);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::Subscription;
    use futures_util::FutureExt;
    use std::pin::pin;

    thread_local! {
        /// Runs once, on this thread, between a waiter's pause snapshot and
        /// its judgment of that snapshot, after the channel lock is released.
        pub(super) static AFTER_PAUSE_SNAPSHOT: std::cell::RefCell<Option<Box<dyn FnOnce()>>> =
            const { std::cell::RefCell::new(None) };
    }

    struct Client {
        control: Arc<ClientControl>,
        pushes: Subscription,
    }

    fn bytes(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value.as_bytes()))
            .collect()
    }

    fn connect(hub: &Arc<Hub>, registry: &Registry, id: u64, version: Version) -> Client {
        let pushes = hub.attach(id);
        let control = registry.register(id, Bytes::new());
        control.set_version(version);
        Client { control, pushes }
    }

    /// Runs `CLIENT <args>` the way a connection routes it.
    fn client(registry: &Registry, connection: &Client, args: &[&str]) -> Resp {
        let mut command = bytes(&["CLIENT"]);
        command.extend(bytes(args));
        connection
            .control
            .seen(Bytes::from_static(b"client"), Instant::now());
        registry.command(&connection.control, &command)
    }

    fn get(registry: &Registry, client: &Client, keys: &[&str]) {
        client
            .control
            .seen(Bytes::from_static(b"get"), Instant::now());
        registry.read(client.control.id, 0, &bytes(keys));
    }

    fn set(registry: &Registry, keys: &[&str], origin: Option<u64>) {
        registry.invalidate(0, &bytes(keys), origin);
    }

    fn drain(client: &mut Client) -> Vec<Vec<Resp>> {
        let mut pushes = Vec::new();
        while let Some(mut message) = client.pushes.try_recv() {
            pushes.push(std::mem::take(&mut message.values));
        }
        pushes
    }

    fn invalidated(keys: &[&str]) -> Vec<Resp> {
        vec![
            bulk(b"invalidate"),
            Resp::Array(Some(keys.iter().map(|key| bulk(key.as_bytes())).collect())),
        ]
    }

    fn error(message: &str) -> Resp {
        Resp::Error(format!("ERR {message}"))
    }

    #[test]
    fn on_off_and_reset_transitions_gate_invalidations() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let mut cache = connect(&hub, &registry, 1, Version::Resp3);
        let ok = Resp::Simple("OK");

        get(&registry, &cache, &["k"]);
        set(&registry, &["k"], Some(2));
        assert!(
            drain(&mut cache).is_empty(),
            "reads before ON are untracked"
        );

        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        get(&registry, &cache, &["k"]);
        set(&registry, &["k"], Some(2));
        assert_eq!(drain(&mut cache), vec![invalidated(&["k"])]);
        set(&registry, &["k"], Some(2));
        assert!(
            drain(&mut cache).is_empty(),
            "invalidation consumes the read"
        );

        get(&registry, &cache, &["k"]);
        assert_eq!(client(&registry, &cache, &["TRACKING", "OFF"]), ok);
        set(&registry, &["k"], Some(2));
        assert!(drain(&mut cache).is_empty());

        // Connection RESET clears registry state, then the control itself.
        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        get(&registry, &cache, &["k"]);
        registry.reset_tracking(1);
        cache.control.reset();
        cache.control.set_version(Version::Resp3);
        set(&registry, &["k"], Some(2));
        assert!(drain(&mut cache).is_empty());

        // The control's own reset disables tracking without the registry lock.
        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        get(&registry, &cache, &["k"]);
        cache.control.reset();
        cache.control.set_version(Version::Resp3);
        assert_eq!(client(&registry, &cache, &["GETREDIR"]), Resp::Integer(-1));
        set(&registry, &["k"], Some(2));
        assert!(drain(&mut cache).is_empty());

        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        get(&registry, &cache, &["k"]);
        set(&registry, &["k"], Some(2));
        assert_eq!(drain(&mut cache), vec![invalidated(&["k"])]);
    }

    #[test]
    fn broadcast_and_ordinary_modes_switch_only_through_off() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let mut cache = connect(&hub, &registry, 1, Version::Resp3);
        let ok = Resp::Simple("OK");

        assert_eq!(
            client(
                &registry,
                &cache,
                &["TRACKING", "ON", "BCAST", "PREFIX", "user:"]
            ),
            ok
        );
        set(&registry, &["order:1", "user:1"], Some(2));
        assert_eq!(drain(&mut cache), vec![invalidated(&["user:1"])]);
        // ON without BCAST keeps the enabled broadcast mode.
        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        set(&registry, &["user:2"], None);
        assert_eq!(drain(&mut cache), vec![invalidated(&["user:2"])]);

        assert_eq!(client(&registry, &cache, &["TRACKING", "OFF"]), ok);
        set(&registry, &["user:3"], None);
        assert!(drain(&mut cache).is_empty());

        assert_eq!(client(&registry, &cache, &["TRACKING", "ON"]), ok);
        set(&registry, &["user:4"], None);
        assert!(drain(&mut cache).is_empty(), "ordinary mode needs a read");
        assert_eq!(
            client(&registry, &cache, &["TRACKING", "ON", "BCAST"]),
            error("You can't switch BCAST mode on/off before disabling tracking")
        );
        set(&registry, &["user:5"], None);
        assert!(
            drain(&mut cache).is_empty(),
            "rejected switch keeps ordinary mode"
        );
        get(&registry, &cache, &["user:5"]);
        set(&registry, &["user:5"], None);
        assert_eq!(drain(&mut cache), vec![invalidated(&["user:5"])]);

        assert_eq!(client(&registry, &cache, &["TRACKING", "OFF"]), ok);
        assert_eq!(client(&registry, &cache, &["TRACKING", "ON", "BCAST"]), ok);
        set(&registry, &["b", "a"], None);
        assert_eq!(drain(&mut cache), vec![invalidated(&["a", "b"])]);
        registry.reset_tracking(1);
        set(&registry, &["a"], None);
        assert!(drain(&mut cache).is_empty());
    }

    #[test]
    fn redirect_delivers_resp2_messages_and_reports_disconnect() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let ok = Resp::Simple("OK");
        let mut target = connect(&hub, &registry, 1, Version::Resp2);
        target
            .pushes
            .command(&bytes(&["SUBSCRIBE", "__redis__:invalidate"]))
            .unwrap();
        drain(&mut target);
        let mut cache = connect(&hub, &registry, 2, Version::Resp3);
        let mut idle = connect(&hub, &registry, 3, Version::Resp3);

        assert_eq!(
            client(&registry, &cache, &["TRACKING", "ON", "REDIRECT", "1"]),
            ok
        );
        assert_eq!(client(&registry, &cache, &["GETREDIR"]), Resp::Integer(1));
        assert_eq!(
            client(&registry, &idle, &["TRACKING", "ON", "REDIRECT", "1"]),
            ok
        );
        assert_eq!(client(&registry, &idle, &["TRACKING", "OFF"]), ok);

        get(&registry, &cache, &["k"]);
        set(&registry, &["k"], Some(4));
        assert_eq!(
            drain(&mut target),
            vec![vec![
                bulk(b"message"),
                bulk(b"__redis__:invalidate"),
                Resp::Array(Some(vec![bulk(b"k")])),
            ]]
        );
        assert!(drain(&mut cache).is_empty());

        registry.unregister(1);
        assert_eq!(
            drain(&mut cache),
            vec![vec![bulk(b"tracking-redir-broken"), Resp::Integer(1)]]
        );
        assert!(
            drain(&mut idle).is_empty(),
            "disabled redirects are not broken"
        );
        let Resp::Map(info) = client(&registry, &cache, &["TRACKINGINFO"]) else {
            panic!("TRACKINGINFO is a map");
        };
        assert_eq!(
            info[0].1,
            Resp::Array(Some(vec![bulk(b"on"), bulk(b"broken_redirect")]))
        );
        get(&registry, &cache, &["k"]);
        set(&registry, &["k"], Some(4));
        assert!(drain(&mut cache).is_empty());
        assert!(drain(&mut target).is_empty());
    }

    #[test]
    fn multiple_clients_keep_optin_optout_noloop_and_flush_semantics() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let ok = Resp::Simple("OK");
        let mut noloop = connect(&hub, &registry, 1, Version::Resp3);
        let mut optin = connect(&hub, &registry, 2, Version::Resp3);
        let mut optout = connect(&hub, &registry, 3, Version::Resp3);
        let mut gone = connect(&hub, &registry, 4, Version::Resp3);
        let mut plain = connect(&hub, &registry, 5, Version::Resp3);
        assert_eq!(
            client(&registry, &noloop, &["TRACKING", "ON", "NOLOOP"]),
            ok
        );
        assert_eq!(client(&registry, &optin, &["TRACKING", "ON", "OPTIN"]), ok);
        assert_eq!(
            client(&registry, &optout, &["TRACKING", "ON", "OPTOUT"]),
            ok
        );
        assert_eq!(client(&registry, &gone, &["TRACKING", "ON"]), ok);
        for reader in [&noloop, &optin, &optout, &gone, &plain] {
            get(&registry, reader, &["k"]);
        }
        assert_eq!(client(&registry, &optin, &["CACHING", "YES"]), ok);
        get(&registry, &optin, &["j"]);
        get(&registry, &optin, &["k"]);
        assert_eq!(client(&registry, &optout, &["CACHING", "NO"]), ok);
        get(&registry, &optout, &["j"]);
        registry.unregister(4);

        set(&registry, &["j", "k"], Some(1));
        assert!(drain(&mut noloop).is_empty(), "NOLOOP skips its own write");
        assert_eq!(drain(&mut optin), vec![invalidated(&["j"])]);
        assert_eq!(drain(&mut optout), vec![invalidated(&["k"])]);
        assert!(drain(&mut gone).is_empty());
        assert!(drain(&mut plain).is_empty());

        get(&registry, &noloop, &["k"]);
        set(&registry, &["k"], Some(2));
        assert_eq!(drain(&mut noloop), vec![invalidated(&["k"])]);

        registry.invalidate_all(None, Some(1));
        let flushed = vec![vec![bulk(b"invalidate"), Resp::Array(None)]];
        assert!(drain(&mut noloop).is_empty());
        assert_eq!(drain(&mut optin), flushed);
        assert_eq!(drain(&mut optout), flushed);
        assert!(drain(&mut gone).is_empty());
        assert!(drain(&mut plain).is_empty());
    }

    #[test]
    fn rejected_tracking_options_change_nothing() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let ok = Resp::Simple("OK");
        let mut cache = connect(&hub, &registry, 1, Version::Resp3);
        let mut idle = connect(&hub, &registry, 2, Version::Resp3);
        assert_eq!(
            client(
                &registry,
                &cache,
                &["TRACKING", "ON", "BCAST", "PREFIX", "a:", "NOLOOP"]
            ),
            ok
        );
        let enabled = client(&registry, &cache, &["TRACKINGINFO"]);
        let disabled = client(&registry, &idle, &["TRACKINGINFO"]);
        let rejected_while_enabled: [(&[&str], &str); 6] = [
            (
                &["TRACKING", "ON", "PREFIX", "b:", "PREFIX", "b:c"],
                "Prefixes cannot overlap",
            ),
            (
                &["TRACKING", "ON", "OPTIN"],
                "OPTIN and OPTOUT are not compatible with BCAST or each other",
            ),
            (
                &["TRACKING", "ON", "REDIRECT", "99"],
                "The client ID you want redirect to does not exist",
            ),
            (&["TRACKING", "ON", "REDIRECT", "x"], "Invalid client ID"),
            (&["TRACKING", "OFF", "BOGUS"], "syntax error"),
            (&["TRACKING", "MAYBE"], "syntax error"),
        ];
        for (client_args, message) in rejected_while_enabled {
            assert_eq!(client(&registry, &cache, client_args), error(message));
        }
        let rejected_while_disabled: [(&[&str], &str); 3] = [
            (
                &["TRACKING", "ON", "OPTIN", "OPTOUT"],
                "OPTIN and OPTOUT are not compatible with BCAST or each other",
            ),
            (
                &["TRACKING", "ON", "PREFIX", "a:"],
                "PREFIX option requires BCAST mode",
            ),
            (
                &["TRACKING", "ON", "BCAST", "REDIRECT", "99"],
                "The client ID you want redirect to does not exist",
            ),
        ];
        for (client_args, message) in rejected_while_disabled {
            assert_eq!(client(&registry, &idle, client_args), error(message));
        }
        assert_eq!(client(&registry, &cache, &["TRACKINGINFO"]), enabled);
        assert_eq!(client(&registry, &idle, &["TRACKINGINFO"]), disabled);

        get(&registry, &idle, &["a:1"]);
        set(&registry, &["a:1", "b:1"], Some(2));
        assert_eq!(drain(&mut cache), vec![invalidated(&["a:1"])]);
        assert!(drain(&mut idle).is_empty());
        set(&registry, &["a:2"], Some(1));
        assert!(
            drain(&mut cache).is_empty(),
            "NOLOOP survives rejected options"
        );
    }

    #[test]
    fn per_command_reads_follow_select_reset_reply_and_kill() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let connection = connect(&hub, &registry, 1, Version::Resp2);
        let ok = Resp::Simple("OK");
        let listed = || {
            let list = registry.list(None, None);
            String::from_utf8(list.to_vec()).unwrap()
        };
        // The database each command leaves behind, stored only when changed.
        connection.control.set_database(0);
        assert!(listed().contains(" db=0 "));
        connection.control.set_database(5);
        assert!(listed().contains(" db=5 "));
        assert_eq!(registry.highest_database(), 5);
        connection.control.reset();
        assert!(listed().contains(" db=0 "));
        assert_eq!(registry.highest_database(), 0);
        connection.control.set_database(5);
        assert!(
            listed().contains(" db=5 "),
            "the database RESET cleared is stored again"
        );
        // Replies the connection sends after each later command.
        let replies = |commands: usize| -> Vec<bool> {
            (0..commands)
                .map(|_| {
                    connection
                        .control
                        .seen(Bytes::from_static(b"get"), Instant::now());
                    connection.control.should_reply()
                })
                .collect()
        };
        assert_eq!(replies(1), [true]);
        assert_eq!(client(&registry, &connection, &["REPLY", "SKIP"]), ok);
        assert!(!connection.control.should_reply(), "SKIP's own reply");
        assert_eq!(replies(2), [false, true]);
        assert_eq!(client(&registry, &connection, &["REPLY", "OFF"]), ok);
        assert!(!connection.control.should_reply());
        assert_eq!(replies(2), [false, false]);
        assert_eq!(client(&registry, &connection, &["REPLY", "ON"]), ok);
        assert!(connection.control.should_reply());
        assert_eq!(client(&registry, &connection, &["REPLY", "OFF"]), ok);
        connection.control.reset();
        assert!(connection.control.should_reply(), "RESET replies");
        assert_eq!(replies(1), [true]);
        // Killing itself closes the connection after this reply.
        assert!(!connection.control.close_after_reply());
        assert!(listed().contains(" flags=N "));
        assert_eq!(
            client(&registry, &connection, &["KILL", "ID", "1", "SKIPME", "NO"]),
            Resp::Integer(1)
        );
        assert!(connection.control.close_after_reply());
        assert!(listed().contains(" flags=c "));
    }

    #[tokio::test(start_paused = true)]
    async fn write_pauses_hold_only_writes_and_replacements_rescope_held_commands() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let admin = connect(&hub, &registry, 1, Version::Resp2);
        let ok = Resp::Simple("OK");
        let admitted = |write| registry.wait_unpaused(write).now_or_never().is_some();
        assert!(admitted(false) && admitted(true));

        assert_eq!(client(&registry, &admin, &["PAUSE", "60000", "WRITE"]), ok);
        assert!(admitted(false), "a WRITE pause admits reads");
        let mut write = pin!(registry.wait_unpaused(true));
        assert!(write.as_mut().now_or_never().is_none(), "and holds writes");

        // Widening to ALL holds reads too, and keeps holding the write.
        assert_eq!(client(&registry, &admin, &["PAUSE", "60000", "ALL"]), ok);
        let mut read = pin!(registry.wait_unpaused(false));
        assert!(read.as_mut().now_or_never().is_none());
        assert!(write.as_mut().now_or_never().is_none());

        // Narrowing back to WRITE releases the held read but not the write.
        assert_eq!(client(&registry, &admin, &["PAUSE", "60000", "WRITE"]), ok);
        assert_eq!(read.now_or_never(), Some(()));
        assert!(write.as_mut().now_or_never().is_none());
        assert!(admitted(false));
        assert!(!admitted(true));

        assert_eq!(client(&registry, &admin, &["UNPAUSE"]), ok);
        assert_eq!(write.now_or_never(), Some(()));
        assert!(admitted(false) && admitted(true));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_pauses_hold_until_their_latest_deadline_and_then_admit_everything() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let admin = connect(&hub, &registry, 1, Version::Resp2);
        let ok = Resp::Simple("OK");
        let admitted = |write| registry.wait_unpaused(write).now_or_never().is_some();
        let (timeout, tick) = (Duration::from_millis(100), Duration::from_millis(1));

        // A held command stays held until the deadline, then is released.
        let paused = tokio::time::Instant::now();
        assert_eq!(client(&registry, &admin, &["PAUSE", "100"]), ok);
        let mut read = pin!(registry.wait_unpaused(false));
        assert!(read.as_mut().now_or_never().is_none());
        tokio::time::advance(timeout - tick).await;
        assert!(read.as_mut().now_or_never().is_none());
        tokio::time::timeout(Duration::from_secs(10), read)
            .await
            .expect("the pause expires");
        assert!(paused.elapsed() >= timeout);
        assert!(admitted(false) && admitted(true));

        // Expiry with no command held admits both kinds at once afterward.
        assert_eq!(client(&registry, &admin, &["PAUSE", "50", "ALL"]), ok);
        assert!(!admitted(false));
        tokio::time::advance(Duration::from_millis(50)).await;
        assert!(admitted(false) && admitted(true));

        // A pause after an expired one holds again. A shorter replacement
        // releases the held write at its own deadline.
        assert_eq!(client(&registry, &admin, &["PAUSE", "60000", "WRITE"]), ok);
        assert!(admitted(false));
        let mut write = pin!(registry.wait_unpaused(true));
        assert!(write.as_mut().now_or_never().is_none());
        let replaced = tokio::time::Instant::now();
        assert_eq!(client(&registry, &admin, &["PAUSE", "100", "WRITE"]), ok);
        tokio::time::advance(timeout - tick).await;
        assert!(write.as_mut().now_or_never().is_none());
        tokio::time::timeout(Duration::from_secs(10), write)
            .await
            .expect("the replacement expires");
        assert!(replaced.elapsed() >= timeout);
        assert!(admitted(false) && admitted(true));
    }

    #[tokio::test(start_paused = true)]
    async fn an_extension_seen_only_after_the_old_deadline_still_holds_waiters() {
        let hub = Arc::new(Hub::default());
        let registry = Registry::new(hub.clone());
        let admin = connect(&hub, &registry, 1, Version::Resp2);
        let ok = Resp::Simple("OK");
        assert_eq!(client(&registry, &admin, &["PAUSE", "100", "ALL"]), ok);
        let mut waiters: Vec<_> = (0..16)
            .map(|_| Box::pin(registry.wait_unpaused(false)))
            .collect();
        for waiter in &mut waiters {
            assert!(waiter.as_mut().now_or_never().is_none());
        }
        // Unpolled while the clock passes the old deadline, every waiter's old
        // timer has fired before the extension is published, so both of its
        // branches are ready when it next runs. `select!` takes either at
        // random; neither may admit it under the extension.
        tokio::time::advance(Duration::from_millis(101)).await;
        assert_eq!(client(&registry, &admin, &["PAUSE", "60000", "ALL"]), ok);
        for waiter in &mut waiters {
            assert!(waiter.as_mut().now_or_never().is_none());
        }
        assert_eq!(client(&registry, &admin, &["UNPAUSE"]), ok);
        for waiter in waiters {
            assert_eq!(waiter.now_or_never(), Some(()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_extended_after_a_waiters_snapshot_holds_it_to_the_new_deadline() {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        let registry = Arc::new(Registry::new(Arc::new(Hub::default())));
        let start = tokio::time::Instant::now();
        registry.publish_pause(Some(Pause {
            until: start + Duration::from_secs(10),
            writes_only: true,
        }));
        let publisher = registry.clone();
        AFTER_PAUSE_SNAPSHOT.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move || {
                // Another publisher extends the unexpired pause the waiter just
                // read; then the clock passes the old deadline, not the new one.
                publisher.publish_pause(Some(Pause {
                    until: start + Duration::from_secs(20),
                    writes_only: true,
                }));
                let mut advance = Box::pin(tokio::time::advance(Duration::from_secs(10)));
                let _ = advance
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
            }));
        });
        let mut waiter = Box::pin(registry.wait_unpaused(true));
        let pending =
            std::future::poll_fn(|cx| Poll::Ready(waiter.as_mut().poll(cx).is_pending())).await;
        assert!(
            pending,
            "a continuously active pause extension was bypassed"
        );
        assert_eq!(registry.pause_scope.load(Ordering::SeqCst), WRITES_PAUSED);
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(waiter.as_mut().now_or_never().is_none());
        tokio::time::advance(Duration::from_secs(1)).await;
        waiter.await;
        assert_eq!(registry.pause_scope.load(Ordering::SeqCst), UNPAUSED);
    }
}
