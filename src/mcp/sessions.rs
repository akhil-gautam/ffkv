//! Persistent configured-target connections with separate explicit wire and
//! request/reply modes. No connection is ever retried or replayed.
use super::{ToolDefinition, config::Config};
use crate::client::{Client, ClientReader, ClientWriter, Frame, validate_command};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    io,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

pub struct Sessions {
    inner: Arc<Inner>,
}

struct Inner {
    config: Arc<Config>,
    sessions: Mutex<BTreeMap<String, Arc<Session>>>,
    default: Mutex<Option<String>>,
    permits: Arc<Semaphore>,
    shutdown: CancellationToken,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Mode {
    Resp,
    Raw,
}

impl Mode {
    fn name(self) -> &'static str {
        match self {
            Self::Resp => "resp",
            Self::Raw => "raw",
        }
    }
}

struct Session {
    id: String,
    target: String,
    mode: Mode,
    protocol: u8,
    database: u32,
    reader: Mutex<Option<Reader>>,
    writer: Mutex<Option<ClientWriter>>,
    gate: Mutex<()>,
    manual: AtomicBool,
    cancel: CancellationToken,
    activity: StdMutex<Activity>,
    _permit: OwnedSemaphorePermit,
}

struct Reader {
    wire: ClientReader,
    // Frames already consumed from the wire stay here until receive commits.
    // Cancellation and a caller's small max_bytes must not discard them.
    pending: VecDeque<(Value, usize)>,
}

struct Activity {
    last: Instant,
    active: usize,
}

struct Active<'a>(&'a Session);
impl Drop for Active<'_> {
    fn drop(&mut self) {
        let mut activity = self.0.activity.lock().unwrap_or_else(|e| e.into_inner());
        activity.active -= 1;
        activity.last = Instant::now();
    }
}

// Also handles a parent dropping an in-flight tool future instead of delivering
// its cancellation token. A per-session closer then drops both socket halves.
struct PoisonOnDrop<'a> {
    session: &'a Session,
    armed: bool,
}
impl Drop for PoisonOnDrop<'_> {
    fn drop(&mut self) {
        if self.armed {
            self.session.cancel.cancel();
        }
    }
}

impl Session {
    fn enter(&self) -> Result<Active<'_>, String> {
        let mut activity = self.activity.lock().unwrap_or_else(|e| e.into_inner());
        if self.cancel.is_cancelled() {
            return Err("session is closed; open a new session explicitly".into());
        }
        activity.active += 1;
        activity.last = Instant::now();
        Ok(Active(self))
    }

    async fn make_manual(&self) {
        if !self.manual.load(Ordering::Acquire) {
            // Transition only between execute batches. Never hold this gate
            // during manual I/O: a queued execute must not block the receive
            // needed to drain a concurrent send.
            let _gate = self.gate.lock().await;
            self.manual.store(true, Ordering::Release);
        }
    }

    async fn close(&self) {
        self.cancel.cancel();
        self.reader.lock().await.take();
        self.writer.lock().await.take();
    }

    fn description(&self) -> Value {
        json!({"session":self.id,"target":self.target,"mode":self.mode.name(),
            "protocol":self.protocol,"database":self.database,
            "manual":self.manual.load(Ordering::Acquire)})
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecuteArgs {
    commands: Vec<Vec<Value>>,
    session: Option<String>,
    timeout_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Open,
    List,
    Close,
    Send,
    Receive,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionArgs {
    action: Action,
    session: Option<String>,
    target: Option<String>,
    protocol: Option<u8>,
    database: Option<u32>,
    mode: Option<Mode>,
    commands: Option<Vec<Vec<Value>>>,
    data_base64: Option<String>,
    max_frames: Option<usize>,
    max_bytes: Option<usize>,
    wait_ms: Option<u64>,
}

pub fn definitions() -> Vec<ToolDefinition> {
    let argument = json!({"oneOf":[{"type":"string"},{"type":"integer","minimum":-9007199254740991_i64,"maximum":9007199254740991_u64},{"type":"object","properties":{"base64":{"type":"string"}},"required":["base64"],"additionalProperties":false}],"description":"UTF-8 string, binary {base64}, or exact safe JSON integer. Use decimal strings for all larger integers."});
    let commands = json!({"type":"array","minItems":1,"items":{"type":"array","minItems":1,"maxItems":1024,"items":argument},"description":"Argument vectors, not shell command strings. Entire batch is validated before sending. Operator command-count and byte caps apply; each framed command is at most 8 MiB."});
    vec![
        ToolDefinition {
            name: "ffkv_execute",
            description: "Run an ordered, genuinely duplex RESP pipeline on a persistent configured-target session. Every ffkv ordinary-reply command is accepted under the server ACL. Returns {session,outcome:ok|error,replies,pushes}; RESP errors are results, not transport errors. Reply frames are tagged: integer decimal string; simple/bulk/error/big_number {encoding:utf8|base64,value}; array/set/push {items}; map {entries:[[key,value]]}; attribute {entries,value}; double string including nan/inf/-inf; boolean value; null; verbatim {format:{encoding,value},encoding,value}. For example an integer is {type:integer,value:9223372036854775807}. Subscription/MONITOR/replication/CLIENT REPLY and other reply-shape commands require ffkv_session send/receive. Timeouts/cancellation close the session; effectful outcome is unknown and never retried.",
            schema: json!({"type":"object","properties":{"commands":commands.clone(),"session":{"type":"string"},"timeout_ms":{"type":"integer","minimum":1,"description":"Defaults to operator operation_timeout_ms, bounded by max_timeout_ms."}},"required":["commands"],"additionalProperties":false}),
            read_only: false,
        },
        ToolDefinition {
            name: "ffkv_session",
            description: "Open/list/close a configured-target connection, or explicitly send/receive unrestricted RESP commands or raw bytes, including replication streams. Open returns {session,target,mode,protocol,database,manual}; protocol/database describe initial negotiation. Send returns {session,outcome:written,bytes,commands?}: bytes written, NOT server acceptance. Any send/receive permanently makes that session manual-only, so execute cannot misassociate outstanding replies. RESP receive returns {session,mode:resp,frames,timed_out}; raw receive returns {session,mode:raw,data_base64,bytes,timed_out,eof}. Tagged frames use the ffkv_execute shape and include pushes unchanged. Receive timeout/cancellation retains parser state and queued frames; max_bytes bounds serialized frames (raw bytes in raw mode), oversized frames remain queued. Receive waits for the first frame then drains immediately available frames, bounded by max_frames and max_bytes. Close interrupts operations and drops only this owned connection. Idle TTL and operator session caps apply. No caller-supplied host or credentials.",
            schema: json!({"type":"object","properties":{
                "action":{"type":"string","enum":["open","list","close","send","receive"]},
                "session":{"type":"string","description":"Required for close/send/receive."},
                "target":{"type":"string","description":"Open only: operator-configured target name."},
                "protocol":{"type":"integer","enum":[2,3],"description":"Open only."},
                "database":{"type":"integer","minimum":0,"maximum":4294967295_u64,"description":"Open only."},
                "mode":{"type":"string","enum":["resp","raw"],"description":"Open only; defaults to resp. Raw preserves negotiation read-ahead."},
                "commands":commands,
                "data_base64":{"type":"string","description":"Send only, raw mode; mutually exclusive with commands."},
                "max_frames":{"type":"integer","minimum":1,"description":"Receive only; default 64, bounded by operator max_batch_commands."},
                "max_bytes":{"type":"integer","minimum":1,"description":"Receive only; defaults to operator max_response_bytes."},
                "wait_ms":{"type":"integer","minimum":0,"description":"Receive only; zero polls, default operation_timeout_ms, bounded by max_timeout_ms."}
            },"required":["action"],"additionalProperties":false}),
            read_only: false,
        },
    ]
}

impl Sessions {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        if config.limits.max_sessions == 0 || config.limits.session_idle_ms == 0 {
            return Err("session limits must be positive".into());
        }
        let inner = Arc::new(Inner {
            permits: Arc::new(Semaphore::new(config.limits.max_sessions)),
            config,
            sessions: Mutex::new(BTreeMap::new()),
            default: Mutex::new(None),
            shutdown: CancellationToken::new(),
        });
        let weak = Arc::downgrade(&inner);
        let shutdown = inner.shutdown.clone();
        let interval = Duration::from_millis(inner.config.limits.session_idle_ms.min(1000));
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    _ = tokio::time::sleep(interval) => (),
                }
                let Some(inner) = weak.upgrade() else { break };
                let ttl = Duration::from_millis(inner.config.limits.session_idle_ms);
                let expired = {
                    let mut sessions = inner.sessions.lock().await;
                    let ids: Vec<_> = sessions
                        .iter()
                        .filter_map(|(id, session)| {
                            let activity =
                                session.activity.lock().unwrap_or_else(|e| e.into_inner());
                            if session.cancel.is_cancelled()
                                || (activity.active == 0 && activity.last.elapsed() >= ttl)
                            {
                                session.cancel.cancel();
                                Some(id.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    ids.into_iter()
                        .filter_map(|id| sessions.remove(&id))
                        .collect::<Vec<_>>()
                };
                for session in expired {
                    session.close().await;
                }
            }
        });
        Ok(Self { inner })
    }

    pub async fn call(
        &self,
        name: &str,
        args: Value,
        cancel: CancellationToken,
    ) -> Result<Value, String> {
        if self.inner.shutdown.is_cancelled() {
            return Err("sessions are shutting down".into());
        }
        match name {
            "ffkv_execute" => {
                self.execute(
                    serde_json::from_value(args)
                        .map_err(|e| format!("invalid execute arguments: {e}"))?,
                    cancel,
                )
                .await
            }
            "ffkv_session" => {
                self.session(
                    serde_json::from_value(args)
                        .map_err(|e| format!("invalid session arguments: {e}"))?,
                    cancel,
                )
                .await
            }
            _ => Err("unknown session tool".into()),
        }
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let sessions = std::mem::take(&mut *self.inner.sessions.lock().await);
        for session in sessions.values() {
            session.cancel.cancel();
        }
        for session in sessions.into_values() {
            session.close().await;
        }
        self.inner.default.lock().await.take();
    }

    fn duration(&self, millis: Option<u64>, zero: bool) -> Result<Duration, String> {
        let millis = millis.unwrap_or(self.inner.config.limits.operation_timeout_ms);
        if (!zero && millis == 0) || millis > self.inner.config.limits.max_timeout_ms {
            return Err("timeout is outside configured limits".into());
        }
        Ok(Duration::from_millis(millis))
    }

    async fn open(
        &self,
        target: Option<&str>,
        mode: Mode,
        protocol: Option<u8>,
        database: Option<u32>,
        cancel: &CancellationToken,
    ) -> Result<Arc<Session>, String> {
        let (target_name, target) = self.inner.config.target(target)?;
        let mut config = target.client_config()?;
        if let Some(protocol) = protocol {
            config.protocol = protocol;
        }
        if let Some(database) = database {
            config.database = database;
        }
        if !matches!(config.protocol, 2 | 3) {
            return Err("protocol must be 2 or 3".into());
        }
        let protocol = config.protocol;
        let database = config.database;
        let permit = self
            .inner
            .permits
            .clone()
            .try_acquire_owned()
            .map_err(|_| "session cap reached; close an owned session first")?;
        let client = tokio::select! {
            biased;
            _ = self.inner.shutdown.cancelled() => return Err("sessions are shutting down".into()),
            _ = cancel.cancelled() => return Err("session open cancelled; no user command sent".into()),
            result = Client::connect(config) => result.map_err(|_| "connection initialization failed (configured target, TLS, authentication, or database); no user command sent")?,
        };
        let (reader, writer) = client.into_split();
        let session = Arc::new(Session {
            id: format!("s-{:032x}", rand::random::<u128>()),
            target: target_name.to_owned(),
            mode,
            protocol,
            database,
            reader: Mutex::new(Some(Reader {
                wire: reader,
                pending: VecDeque::new(),
            })),
            writer: Mutex::new(Some(writer)),
            gate: Mutex::new(()),
            manual: AtomicBool::new(mode == Mode::Raw),
            cancel: self.inner.shutdown.child_token(),
            activity: StdMutex::new(Activity {
                last: Instant::now(),
                active: 0,
            }),
            _permit: permit,
        });
        let weak = Arc::downgrade(&session);
        let closed = session.cancel.clone();
        tokio::spawn(async move {
            closed.cancelled().await;
            if let Some(session) = weak.upgrade() {
                session.close().await;
            }
        });
        let mut registry = self.inner.sessions.lock().await;
        if self.inner.shutdown.is_cancelled() || cancel.is_cancelled() {
            session.cancel.cancel();
            return Err("session open cancelled; no user command sent".into());
        }
        registry.insert(session.id.clone(), session.clone());
        Ok(session)
    }

    async fn lookup(&self, id: &str) -> Result<Arc<Session>, String> {
        let session = self
            .inner
            .sessions
            .lock()
            .await
            .get(id)
            .cloned()
            .ok_or("unknown or expired session")?;
        if session.cancel.is_cancelled() {
            return Err("session is closed; open a new session explicitly".into());
        }
        Ok(session)
    }

    async fn default_session(&self, cancel: &CancellationToken) -> Result<Arc<Session>, String> {
        let mut default = tokio::select! {
            _ = cancel.cancelled() => return Err("execute cancelled before sending commands".into()),
            _ = self.inner.shutdown.cancelled() => return Err("sessions are shutting down".into()),
            guard = self.inner.default.lock() => guard,
        };
        if let Some(id) = default.as_deref()
            && let Ok(session) = self.lookup(id).await
        {
            return Ok(session);
        }
        let session = self.open(None, Mode::Resp, None, None, cancel).await?;
        *default = Some(session.id.clone());
        Ok(session)
    }

    async fn remove(&self, session: &Session) {
        session.cancel.cancel();
        self.inner.sessions.lock().await.remove(&session.id);
        session.close().await;
    }

    async fn execute(&self, args: ExecuteArgs, cancel: CancellationToken) -> Result<Value, String> {
        let batch = prepare_batch(args.commands, &self.inner.config, true)?;
        let duration = self.duration(args.timeout_ms, false)?;
        let deadline = tokio::time::Instant::now() + duration;
        let get_session = async {
            match args.session.as_deref() {
                Some(id) => self.lookup(id).await,
                None => self.default_session(&cancel).await,
            }
        };
        let session = tokio::time::timeout_at(deadline, get_session)
            .await
            .map_err(|_| "execute timed out before sending commands")??;
        let _active = session.enter()?;
        let gate = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("execute cancelled before sending commands".into()),
            _ = session.cancel.cancelled() => return Err("session closed before sending commands".into()),
            _ = tokio::time::sleep_until(deadline) => return Err("execute timed out before sending commands".into()),
            gate = session.gate.lock() => gate,
        };
        if session.mode != Mode::Resp || session.manual.load(Ordering::Acquire) {
            return Err("execute requires a RESP session never used by send/receive; open another session or continue explicit send/receive".into());
        }
        let mut poison = PoisonOnDrop {
            session: &session,
            armed: true,
        };
        let mut replies = Vec::with_capacity(batch.commands.len());
        let mut pushes = Vec::new();
        let mut response_bytes =
            json_size(&json!({"session":session.id,"outcome":"error","replies":[],"pushes":[]}))
                .map_err(|_| "cannot measure response envelope")?;
        let mut has_error = false;
        let operation = async {
            let mut reader = session.reader.lock().await;
            let reader = reader
                .as_mut()
                .ok_or_else(|| io::Error::other("session reader is closed"))?;
            let mut writer = session.writer.lock().await;
            let writer = writer
                .as_mut()
                .ok_or_else(|| io::Error::other("session writer is closed"))?;
            let send = async {
                for command in &batch.commands {
                    writer.write_command(command).await?;
                }
                Ok::<_, io::Error>(())
            };
            let receive = async {
                while replies.len() < batch.commands.len() {
                    let frame = reader.wire.read_frame().await?;
                    let push = frame.is_push();
                    if !push {
                        has_error |= frame.has_error();
                    }
                    let value = frame_value(frame);
                    let bytes = json_size(&value)?;
                    let comma = usize::from(if push {
                        !pushes.is_empty()
                    } else {
                        !replies.is_empty()
                    });
                    response_bytes = response_bytes
                        .checked_add(bytes)
                        .and_then(|n| n.checked_add(comma))
                        .ok_or_else(|| io::Error::other("response size overflow"))?;
                    if response_bytes > self.inner.config.limits.max_response_bytes {
                        return Err(io::Error::other(
                            "response or push bytes exceed configured response cap",
                        ));
                    }
                    if push {
                        pushes.push(value);
                    } else {
                        replies.push(value);
                    }
                }
                Ok::<_, io::Error>(())
            };
            // Polling both halves concurrently is essential: the server can
            // fill its send buffer before it consumes the end of this batch.
            tokio::try_join!(send, receive)?;
            Ok::<_, io::Error>(())
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("cancelled".to_owned()),
            _ = session.cancel.cancelled() => Err("session closed".to_owned()),
            _ = tokio::time::sleep_until(deadline) => Err("timed out".to_owned()),
            result = operation => result.map_err(|error| format!("transport/protocol/response limit failure: {error}")),
        };
        drop(gate);
        if let Err(reason) = result {
            self.remove(&session).await;
            // Keep partial replies structured so the MCP result pager applies
            // the same byte budget to errors as it does to successful replies.
            let mut result = json!({
                "session":session.id,"outcome":"unknown","closed":true,
                "error":format!("execute {reason}; connection closed; unread replies/pushes unavailable; never automatically retry or replay")
            });
            result["replies"] = Value::Array(replies);
            result["pushes"] = Value::Array(pushes);
            return Ok(result);
        }
        poison.armed = false;
        let mut result = json!({"session":session.id,"outcome":if has_error {"error"} else {"ok"}});
        result["replies"] = Value::Array(replies);
        result["pushes"] = Value::Array(pushes);
        Ok(result)
    }

    async fn session(&self, args: SessionArgs, cancel: CancellationToken) -> Result<Value, String> {
        validate_action_fields(&args)?;
        match args.action {
            Action::Open => Ok(self
                .open(
                    args.target.as_deref(),
                    args.mode.unwrap_or(Mode::Resp),
                    args.protocol,
                    args.database,
                    &cancel,
                )
                .await?
                .description()),
            Action::List => {
                let sessions = self.inner.sessions.lock().await;
                Ok(
                    json!({"sessions":sessions.values().filter(|s| !s.cancel.is_cancelled()).map(|s| s.description()).collect::<Vec<_>>()}),
                )
            }
            Action::Close => {
                let session = self.lookup(args.session.as_deref().unwrap()).await?;
                self.remove(&session).await;
                Ok(json!({"session":session.id,"closed":true}))
            }
            Action::Send => self.send(args, cancel).await,
            Action::Receive => self.receive(args, cancel).await,
        }
    }

    async fn send(&self, args: SessionArgs, cancel: CancellationToken) -> Result<Value, String> {
        let deadline = tokio::time::Instant::now() + self.duration(None, false)?;
        let session = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("send cancelled before sending bytes".into()),
            _ = tokio::time::sleep_until(deadline) => return Err("send timed out before sending bytes".into()),
            result = self.lookup(args.session.as_deref().unwrap()) => result?,
        };
        let payload = match (args.commands, args.data_base64) {
            (Some(commands), None) => {
                Payload::Resp(prepare_batch(commands, &self.inner.config, false)?)
            }
            (None, Some(encoded)) if session.mode == Mode::Raw => {
                if encoded.len()
                    > (self.inner.config.limits.max_batch_bytes.saturating_add(2) / 3)
                        .saturating_mul(4)
                {
                    return Err("raw send exceeds configured byte cap".into());
                }
                let bytes = STANDARD
                    .decode(encoded)
                    .map_err(|_| "invalid base64 payload")?;
                if bytes.len() > self.inner.config.limits.max_batch_bytes {
                    return Err("raw send exceeds configured byte cap".into());
                }
                Payload::Raw(bytes)
            }
            _ => return Err("send requires commands, or data_base64 on a raw-mode session".into()),
        };
        let _active = session.enter()?;
        let acquire = async {
            session.make_manual().await;
            session.writer.lock().await
        };
        let mut guard = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("send cancelled before sending bytes".into()),
            _ = session.cancel.cancelled() => return Err("session closed before sending bytes".into()),
            _ = tokio::time::sleep_until(deadline) => return Err("send timed out before sending bytes".into()),
            guard = acquire => guard,
        };
        let writer = guard
            .as_mut()
            .ok_or("session writer is closed before sending bytes")?;
        let mut poison = PoisonOnDrop {
            session: &session,
            armed: false,
        };
        let operation = async {
            // Lock waits cannot poison another operation. Only a polled write
            // can leave a partial command or byte stream on the connection.
            poison.armed = true;
            match &payload {
                Payload::Resp(batch) => {
                    for command in &batch.commands {
                        writer.write_command(command).await?;
                    }
                }
                Payload::Raw(bytes) => writer.write_raw(bytes).await?,
            }
            Ok::<_, io::Error>(())
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("cancelled".to_owned()),
            _ = session.cancel.cancelled() => Err("session closed".to_owned()),
            _ = tokio::time::sleep_until(deadline) => Err("timed out".to_owned()),
            result = operation => result.map_err(|error| format!("transport failure: {error}")),
        };
        // Closing also locks the writer, so release it before destructive
        // cleanup of a dispatched write.
        drop(guard);
        if let Err(reason) = result {
            if !poison.armed {
                return Err(format!("send {reason} before sending bytes"));
            }
            self.remove(&session).await;
            return Err(format!(
                "send {reason}; session {} closed; command/byte-stream outcome unknown, no retry or replay",
                session.id
            ));
        }
        poison.armed = false;
        Ok(match payload {
            Payload::Resp(batch) => {
                json!({"session":session.id,"outcome":"written","bytes":batch.bytes,"commands":batch.commands.len()})
            }
            Payload::Raw(bytes) => {
                json!({"session":session.id,"outcome":"written","bytes":bytes.len()})
            }
        })
    }

    async fn receive(&self, args: SessionArgs, cancel: CancellationToken) -> Result<Value, String> {
        let session = self.lookup(args.session.as_deref().unwrap()).await?;
        let max_frames = args
            .max_frames
            .unwrap_or(64.min(self.inner.config.limits.max_batch_commands));
        let max_bytes = args
            .max_bytes
            .unwrap_or(self.inner.config.limits.max_response_bytes);
        if max_frames == 0
            || max_frames > self.inner.config.limits.max_batch_commands
            || max_bytes == 0
            || max_bytes > self.inner.config.limits.max_response_bytes
        {
            return Err("receive limits are outside configured caps".into());
        }
        let duration = self.duration(args.wait_ms, true)?;
        let deadline = tokio::time::Instant::now() + duration;
        let _active = session.enter()?;
        let acquire = async {
            session.make_manual().await;
            session.reader.lock().await
        };
        let mut guard = tokio::select! {
            biased;
            _ = cancel.cancelled() => return Err("receive cancelled; session/parser retained".into()),
            _ = session.cancel.cancelled() => return Err("session closed during receive".into()),
            result = tokio::time::timeout_at(deadline, acquire) => match result {
                Ok(guards) => guards,
                Err(_) => return Ok(empty_receive(&session, true)),
            },
        };
        let reader = guard.as_mut().ok_or("session reader is closed")?;
        if session.mode == Mode::Raw {
            let result = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err("receive cancelled; session/parser retained".into()),
                _ = session.cancel.cancelled() => return Err("session closed during receive".into()),
                result = tokio::time::timeout_at(deadline, reader.wire.read_raw(max_bytes)) => result,
            };
            return match result {
                Err(_) => Ok(empty_receive(&session, true)),
                Ok(Ok(bytes)) => {
                    let eof = bytes.is_empty();
                    let result = json!({"session":session.id,"mode":"raw","data_base64":STANDARD.encode(&bytes),"bytes":bytes.len(),"timed_out":false,"eof":eof});
                    if eof {
                        session.cancel.cancel();
                    }
                    Ok(result)
                }
                Ok(Err(_)) => {
                    session.cancel.cancel();
                    Err("raw receive transport failure; session closed, prior send outcomes unknown".into())
                }
            };
        }
        let mut count = 0;
        let mut bytes = 0usize;
        let mut timed_out = false;
        let mut terminal_error = None;
        while count < max_frames {
            if let Some((_, size)) = reader.pending.get(count) {
                if bytes.saturating_add(*size) > max_bytes {
                    if count == 0 {
                        return Err(format!(
                            "next frame needs {size} serialized bytes; increase max_bytes; frame retained"
                        ));
                    }
                    break;
                }
                bytes += size;
                count += 1;
                continue;
            }
            let until = if count == 0 {
                deadline
            } else {
                tokio::time::Instant::now()
            };
            let next = tokio::select! {
                biased;
                _ = cancel.cancelled() => return Err("receive cancelled; session/parser and queued frames retained".into()),
                _ = session.cancel.cancelled() => return Err("session closed during receive".into()),
                next = tokio::time::timeout_at(until, reader.wire.read_frame()) => next,
            };
            match next {
                Err(_) => {
                    timed_out = count == 0;
                    break;
                }
                Ok(Err(_)) => {
                    session.cancel.cancel();
                    let message = "RESP receive transport/protocol failure; session closed, prior send outcomes unknown; unread bytes unavailable";
                    if count == 0 {
                        return Err(message.into());
                    }
                    // A terminal transport event must not erase the complete
                    // frames read before it (notably QUIT's final reply).
                    terminal_error = Some(message);
                    break;
                }
                Ok(Ok(frame)) => {
                    let frame = frame_value(frame);
                    let size = json_size(&frame).map_err(|_| "cannot measure response frame")?;
                    if size > self.inner.config.limits.max_response_bytes {
                        session.cancel.cancel();
                        let message = "next RESP frame exceeds configured response cap and cannot be returned; session closed rather than silently truncating data";
                        if count == 0 {
                            return Err(message.into());
                        }
                        terminal_error = Some(message);
                        break;
                    }
                    reader.pending.push_back((frame, size));
                }
            }
        }
        let frames: Vec<_> = reader
            .pending
            .drain(..count)
            .map(|(frame, _)| frame)
            .collect();
        let mut result = json!({"session":session.id,"mode":"resp","timed_out":timed_out});
        result["frames"] = Value::Array(frames);
        if let Some(error) = terminal_error {
            result["closed"] = json!(true);
            result["transport_error"] = json!(error);
        }
        Ok(result)
    }
}

fn empty_receive(session: &Session, timed_out: bool) -> Value {
    match session.mode {
        Mode::Resp => json!({"session":session.id,"mode":"resp","frames":[],"timed_out":timed_out}),
        Mode::Raw => {
            json!({"session":session.id,"mode":"raw","data_base64":"","bytes":0,"timed_out":timed_out,"eof":false})
        }
    }
}

fn validate_action_fields(args: &SessionArgs) -> Result<(), String> {
    let open_fields = args.target.is_some()
        || args.protocol.is_some()
        || args.database.is_some()
        || args.mode.is_some();
    let send_fields = args.commands.is_some() || args.data_base64.is_some();
    let receive_fields =
        args.max_frames.is_some() || args.max_bytes.is_some() || args.wait_ms.is_some();
    let valid = match args.action {
        Action::Open => args.session.is_none() && !send_fields && !receive_fields,
        Action::List => args.session.is_none() && !open_fields && !send_fields && !receive_fields,
        Action::Close => args.session.is_some() && !open_fields && !send_fields && !receive_fields,
        Action::Send => {
            args.session.is_some()
                && !open_fields
                && !receive_fields
                && (args.commands.is_some() != args.data_base64.is_some())
        }
        Action::Receive => args.session.is_some() && !open_fields && !send_fields,
    };
    if valid {
        Ok(())
    } else {
        Err("fields do not match session action; open takes target/protocol/database/mode, send takes exactly one of commands/data_base64, receive takes limits/wait_ms, close/send/receive require session".into())
    }
}

struct Batch {
    commands: Vec<Vec<Bytes>>,
    bytes: usize,
}
enum Payload {
    Resp(Batch),
    Raw(Vec<u8>),
}

fn prepare_batch(
    commands: Vec<Vec<Value>>,
    config: &Config,
    execute: bool,
) -> Result<Batch, String> {
    if commands.is_empty() || commands.len() > config.limits.max_batch_commands {
        return Err("batch command count is outside configured limits".into());
    }
    let mut batch = Batch {
        commands: Vec::with_capacity(commands.len()),
        bytes: 0,
    };
    for command in commands {
        if command.is_empty() || command.len() > 1024 {
            return Err("each command requires 1..1024 arguments".into());
        }
        let mut args = Vec::with_capacity(command.len());
        let mut size = 1 + decimal_len(command.len()) + 2;
        for arg in command {
            let arg = match arg {
                Value::String(value) => Bytes::from(value),
                Value::Number(value) => {
                    let integer = value.as_i64().filter(|n| n.unsigned_abs() <= 9_007_199_254_740_991).ok_or("JSON numeric arguments must be exact integers within +/-9007199254740991; use decimal strings otherwise")?;
                    Bytes::from(integer.to_string())
                }
                Value::Object(mut value) if value.len() == 1 => {
                    let Some(Value::String(encoded)) = value.remove("base64") else {
                        return Err("binary argument must be exactly {base64:string}".into());
                    };
                    if encoded.len()
                        > (config.limits.max_batch_bytes.saturating_add(2) / 3).saturating_mul(4)
                    {
                        return Err("batch exceeds configured byte cap".into());
                    }
                    Bytes::from(
                        STANDARD
                            .decode(encoded)
                            .map_err(|_| "invalid base64 argument")?,
                    )
                }
                _ => {
                    return Err(
                        "arguments must be strings, exact safe integers, or {base64:string}".into(),
                    );
                }
            };
            size = size
                .checked_add(1 + decimal_len(arg.len()) + 2)
                .and_then(|n| n.checked_add(arg.len()))
                .and_then(|n| n.checked_add(2))
                .ok_or("command size overflow")?;
            if size > config.limits.max_batch_bytes {
                return Err("batch exceeds configured byte cap".into());
            }
            args.push(arg);
        }
        validate_command(&args).map_err(|e| e.to_string())?;
        if execute && changes_reply_shape(&args) {
            return Err("reply-shape/state-changing command requires ffkv_session send/receive (raw mode for replication bytes); batch not sent".into());
        }
        batch.bytes = batch
            .bytes
            .checked_add(size)
            .filter(|n| *n <= config.limits.max_batch_bytes)
            .ok_or("batch exceeds configured byte cap")?;
        batch.commands.push(args);
    }
    Ok(batch)
}

fn decimal_len(mut value: usize) -> usize {
    let mut digits = 1;
    while value >= 10 {
        value /= 10;
        digits += 1;
    }
    digits
}

fn changes_reply_shape(args: &[Bytes]) -> bool {
    let name = &args[0];
    [
        b"SUBSCRIBE".as_slice(),
        b"PSUBSCRIBE",
        b"SSUBSCRIBE",
        b"UNSUBSCRIBE",
        b"PUNSUBSCRIBE",
        b"SUNSUBSCRIBE",
        b"MONITOR",
        b"SYNC",
        b"PSYNC",
        b"REPLCONF",
        b"SHUTDOWN",
        b"QUIT",
    ]
    .iter()
    .any(|candidate| name.eq_ignore_ascii_case(candidate))
        || (name.eq_ignore_ascii_case(b"CLIENT")
            && args.get(1).is_some_and(|arg| {
                arg.eq_ignore_ascii_case(b"REPLY") || arg.eq_ignore_ascii_case(b"TRACKING")
            }))
        || (name.eq_ignore_ascii_case(b"DEBUG")
            && args
                .get(1)
                .is_some_and(|arg| arg.eq_ignore_ascii_case(b"PROTOCOL")))
        || (name.eq_ignore_ascii_case(b"SCRIPT")
            && args
                .get(1)
                .is_some_and(|arg| arg.eq_ignore_ascii_case(b"DEBUG")))
}

fn binary_value(bytes: &[u8]) -> Value {
    match std::str::from_utf8(bytes) {
        Ok(value) => json!({"encoding":"utf8","value":value}),
        Err(_) => json!({"encoding":"base64","value":STANDARD.encode(bytes)}),
    }
}

fn frame_value(frame: Frame) -> Value {
    frame_json(&frame)
}

/// Lossless JSON representation: no JSON-number integer precision loss, no map
/// key coercion, no duplicate-key loss, and no lossy UTF-8 replacement.
pub(super) fn frame_json(frame: &Frame) -> Value {
    match frame {
        Frame::Simple(bytes) => tagged_bytes("simple", bytes),
        Frame::Error(bytes) => tagged_bytes("error", bytes),
        Frame::Bulk(bytes) => tagged_bytes("bulk", bytes),
        Frame::BigNumber(bytes) => tagged_bytes("big_number", bytes),
        Frame::Integer(value) => json!({"type":"integer","value":value.to_string()}),
        Frame::Null => json!({"type":"null"}),
        Frame::Boolean(value) => json!({"type":"boolean","value":value}),
        Frame::Double(value) => {
            json!({"type":"double","value":if value.is_nan() {"nan".into()} else if *value == f64::INFINITY {"inf".into()} else if *value == f64::NEG_INFINITY {"-inf".into()} else {value.to_string()}})
        }
        Frame::Array(items) => frame_items("array", items),
        Frame::Set(items) => frame_items("set", items),
        Frame::Push(items) => frame_items("push", items),
        Frame::Map(entries) => {
            let mut result = json!({"type":"map"});
            result["entries"] = Value::Array(pair_values(entries));
            result
        }
        Frame::Attribute(entries, value) => {
            let mut result = json!({"type":"attribute"});
            result["entries"] = Value::Array(pair_values(entries));
            result["value"] = frame_json(value);
            result
        }
        Frame::Verbatim { format, data } => {
            let mut value = tagged_bytes("verbatim", data);
            value["format"] = binary_value(format);
            value
        }
    }
}

fn frame_items(kind: &'static str, items: &[Frame]) -> Value {
    let mut result = json!({"type":kind});
    result["items"] = Value::Array(items.iter().map(frame_json).collect());
    result
}

fn tagged_bytes(kind: &'static str, bytes: &[u8]) -> Value {
    let mut value = binary_value(bytes);
    value["type"] = Value::String(kind.into());
    value
}

fn pair_values(entries: &[(Frame, Frame)]) -> Vec<Value> {
    entries
        .iter()
        .map(|(key, value)| Value::Array(vec![frame_json(key), frame_json(value)]))
        .collect()
}

fn json_size(value: &Value) -> io::Result<usize> {
    struct Counter(usize);
    impl io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .ok_or_else(|| io::Error::other("JSON size overflow"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tagged_frames_preserve_binary_large_numbers_and_duplicate_keys() {
        let frame = Frame::Map(vec![
            (
                Frame::Bulk(Bytes::from_static(b"key")),
                Frame::Integer(i64::MAX),
            ),
            (
                Frame::Bulk(Bytes::from_static(b"key")),
                Frame::Bulk(Bytes::from_static(b"\0\xff\r\n")),
            ),
            (Frame::Integer(1), Frame::Double(f64::NEG_INFINITY)),
        ]);
        let value = frame_value(frame);
        assert_eq!(value["entries"].as_array().unwrap().len(), 3);
        assert_eq!(
            value["entries"][0][1],
            json!({"type":"integer","value":"9223372036854775807"})
        );
        assert_eq!(
            value["entries"][1][1],
            json!({"type":"bulk","encoding":"base64","value":"AP8NCg=="})
        );
        assert_eq!(
            value["entries"][2][1],
            json!({"type":"double","value":"-inf"})
        );
    }

    #[test]
    fn annotated_pushes_remain_separate_from_command_replies() {
        let frame = Frame::Attribute(vec![], Box::new(Frame::Push(vec![Frame::Integer(42)])));
        assert!(frame.is_push());
        let value = frame_value(frame);
        assert_eq!(value["type"], "attribute");
        assert_eq!(value["value"]["type"], "push");
        assert_eq!(value["value"]["items"][0]["value"], "42");
    }

    #[test]
    fn reply_shape_detection_is_case_insensitive_and_not_a_command_allowlist() {
        let args = |parts: &[&'static str]| {
            parts
                .iter()
                .map(|s| Bytes::from_static(s.as_bytes()))
                .collect::<Vec<_>>()
        };
        for command in [
            args(&["sUbScRiBe", "a"]),
            args(&["CLIENT", "rePLy", "OFF"]),
            args(&["CLIENT", "TRACKING", "ON"]),
            args(&["DEBUG", "PROTOCOL", "push"]),
            args(&["PSYNC", "?", "-1"]),
            args(&["QUIT"]),
        ] {
            assert!(changes_reply_shape(&command));
        }
        for command in [
            args(&["MULTI"]),
            args(&["WATCH", "a"]),
            args(&["HELLO", "3"]),
            args(&["FUTURE.MODULE.COMMAND", "arg"]),
            args(&["CLIENT", "GETNAME"]),
        ] {
            assert!(!changes_reply_shape(&command));
        }
    }

    #[test]
    fn session_actions_reject_irrelevant_or_ambiguous_fields() {
        for value in [
            json!({"action":"open","session":"x"}),
            json!({"action":"send","session":"x","commands":[["PING"]],"data_base64":""}),
            json!({"action":"receive","session":"x","target":"other"}),
            json!({"action":"close"}),
        ] {
            let args: SessionArgs = serde_json::from_value(value).unwrap();
            assert!(validate_action_fields(&args).is_err());
        }
        let args: SessionArgs =
            serde_json::from_value(json!({"action":"receive","session":"x","wait_ms":0})).unwrap();
        assert!(validate_action_fields(&args).is_ok());
    }

    #[test]
    fn json_budget_counts_escaped_bytes_without_allocating_serialized_copy() {
        let value = json!({"type":"bulk","encoding":"utf8","value":"\0\n\"\\"});
        assert_eq!(
            json_size(&value).unwrap(),
            serde_json::to_vec(&value).unwrap().len()
        );
    }

    #[test]
    fn batch_validation_covers_every_command_before_any_connection() {
        let config = Config::default();
        assert!(
            prepare_batch(
                vec![
                    vec![json!("SET"), json!("a"), json!("b")],
                    vec![json!("CLIENT"), json!("REPLY"), json!("OFF")]
                ],
                &config,
                true
            )
            .is_err()
        );
        assert!(
            prepare_batch(
                vec![vec![json!("PING")], vec![json!({"base64":"!"})]],
                &config,
                true
            )
            .is_err()
        );
        assert!(
            prepare_batch(
                vec![vec![json!("ECHO"), json!(9007199254740992_u64)]],
                &config,
                true
            )
            .is_err()
        );
        let batch = prepare_batch(
            vec![
                vec![json!("ECHO"), json!({"base64":"AP8="})],
                vec![json!("ECHO"), json!(-42)],
            ],
            &config,
            true,
        )
        .unwrap();
        assert_eq!(batch.commands[0][1].as_ref(), b"\0\xff");
        assert_eq!(batch.commands[1][1].as_ref(), b"-42");
        let mut limited = Config::default();
        limited.limits.max_batch_bytes = batch.bytes - 1;
        assert!(
            prepare_batch(
                vec![
                    vec![json!("ECHO"), json!({"base64":"AP8="})],
                    vec![json!("ECHO"), json!(-42)]
                ],
                &limited,
                true
            )
            .is_err()
        );
    }

    async fn open_pair(mode: &str) -> (Sessions, String, tokio::net::TcpStream) {
        open_pair_with_config(mode, Config::default()).await
    }

    async fn open_pair_with_config(
        mode: &str,
        mut config: Config,
    ) -> (Sessions, String, tokio::net::TcpStream) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let target = config.targets.get_mut(&config.default_target).unwrap();
        target.port = listener.local_addr().unwrap().port();
        target.protocol = 2;
        let sessions = Sessions::new(Arc::new(config)).unwrap();
        let opened = sessions
            .call(
                "ffkv_session",
                json!({"action":"open","mode":mode}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let id = opened["session"].as_str().unwrap().to_owned();
        let (peer, _) = listener.accept().await.unwrap();
        (sessions, id, peer)
    }

    #[derive(Clone, Copy)]
    enum QueuedSendEnd {
        Timeout,
        Cancel,
        Drop,
    }

    async fn interrupt_queued_send(sessions: &Sessions, id: &str, ending: QueuedSendEnd) {
        let cancel = CancellationToken::new();
        let send = sessions.call(
            "ffkv_session",
            json!({"action":"send","session":id,"commands":[["PING"]]}),
            cancel.clone(),
        );
        tokio::pin!(send);
        // The caller holds the exact lock this send needs, so a single poll
        // deterministically queues it before timeout, cancellation, or drop.
        assert!(futures_util::poll!(&mut send).is_pending());
        match ending {
            QueuedSendEnd::Timeout => (),
            QueuedSendEnd::Cancel => cancel.cancel(),
            QueuedSendEnd::Drop => return,
        }
        let error = tokio::time::timeout(Duration::from_secs(1), send)
            .await
            .expect("queued send must not wait for destructive session cleanup")
            .unwrap_err();
        let reason = match ending {
            QueuedSendEnd::Timeout => "timed out",
            QueuedSendEnd::Cancel => "cancelled",
            QueuedSendEnd::Drop => unreachable!(),
        };
        assert!(error.contains(reason), "{error}");
        assert!(error.contains("before sending bytes"), "{error}");
        assert!(!error.contains("unknown"), "{error}");
        assert!(!error.contains("closed"), "{error}");
    }

    async fn queued_send_preserves_execute(ending: QueuedSendEnd) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut config = Config::default();
        config.limits.operation_timeout_ms = 20;
        let (sessions, id, mut peer) = open_pair_with_config("resp", config).await;
        let session = sessions.lookup(&id).await.unwrap();
        *sessions.inner.default.lock().await = Some(id.clone());
        let execute = sessions.call(
            "ffkv_execute",
            json!({"commands":[["PING"]],"timeout_ms":5000}),
            CancellationToken::new(),
        );
        tokio::pin!(execute);
        let mut command = [0; 14];
        tokio::select! {
            result = &mut execute => panic!("peer supplied no response: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut command)) => {
                result.unwrap().unwrap();
            },
        }
        assert_eq!(&command, b"*1\r\n$4\r\nPING\r\n");

        interrupt_queued_send(&sessions, &id, ending).await;
        assert!(!session.cancel.is_cancelled());
        assert!(!session.manual.load(Ordering::Acquire));
        assert!(Arc::ptr_eq(&sessions.lookup(&id).await.unwrap(), &session));
        assert_eq!(
            sessions.inner.default.lock().await.as_deref(),
            Some(id.as_str())
        );
        peer.write_all(b"+PONG\r\n").await.unwrap();
        let result = tokio::time::timeout(Duration::from_secs(1), execute)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(result["session"], id);
        assert_eq!(result["outcome"], "ok");
        assert_eq!(
            result["replies"],
            json!([{"type":"simple","encoding":"utf8","value":"PONG"}])
        );
        sessions.shutdown().await;
        let mut extra = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut extra))
            .await
            .unwrap()
            .unwrap();
        assert!(
            extra.is_empty(),
            "queued send wrote unexpected bytes: {extra:?}"
        );
    }

    #[tokio::test]
    async fn send_gate_timeout_preserves_active_execute() {
        queued_send_preserves_execute(QueuedSendEnd::Timeout).await;
    }

    #[tokio::test]
    async fn send_gate_cancellation_preserves_active_execute() {
        queued_send_preserves_execute(QueuedSendEnd::Cancel).await;
    }

    #[tokio::test]
    async fn dropped_send_gate_wait_preserves_active_execute() {
        queued_send_preserves_execute(QueuedSendEnd::Drop).await;
    }

    async fn queued_send_preserves_writer(ending: QueuedSendEnd) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut config = Config::default();
        config.limits.operation_timeout_ms = 20;
        let (sessions, id, mut peer) = open_pair_with_config("resp", config).await;
        let session = sessions.lookup(&id).await.unwrap();
        session.make_manual().await;
        // Manual I/O must bypass an execute gate after the transition, even
        // while another operation owns the writer.
        let gate = session.gate.lock().await;
        let mut writer = session.writer.lock().await;
        interrupt_queued_send(&sessions, &id, ending).await;
        assert!(!session.cancel.is_cancelled());
        assert!(session.manual.load(Ordering::Acquire));
        assert!(Arc::ptr_eq(&sessions.lookup(&id).await.unwrap(), &session));
        writer
            .as_mut()
            .unwrap()
            .write_command(&[Bytes::from_static(b"PING")])
            .await
            .unwrap();
        drop(writer);
        let mut command = [0; 14];
        tokio::time::timeout(Duration::from_secs(1), peer.read_exact(&mut command))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&command, b"*1\r\n$4\r\nPING\r\n");
        peer.write_all(b"+PONG\r\n").await.unwrap();
        let received = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_frames":1,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            received["frames"],
            json!([{"type":"simple","encoding":"utf8","value":"PONG"}])
        );
        drop(gate);
        sessions.shutdown().await;
        let mut extra = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut extra))
            .await
            .unwrap()
            .unwrap();
        assert!(
            extra.is_empty(),
            "queued send wrote unexpected bytes: {extra:?}"
        );
    }

    #[tokio::test]
    async fn send_writer_timeout_preserves_original_writer() {
        queued_send_preserves_writer(QueuedSendEnd::Timeout).await;
    }

    #[tokio::test]
    async fn send_writer_cancellation_preserves_original_writer() {
        queued_send_preserves_writer(QueuedSendEnd::Cancel).await;
    }

    #[tokio::test]
    async fn dropped_send_writer_wait_preserves_original_writer() {
        queued_send_preserves_writer(QueuedSendEnd::Drop).await;
    }

    #[tokio::test]
    async fn receive_timeout_retains_partial_parser_and_oversized_frame() {
        use tokio::io::AsyncWriteExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        peer.write_all(b"$5\r\nab").await.unwrap();
        let first = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"wait_ms":10}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(first["timed_out"], true);
        peer.write_all(b"cde\r\n:42\r\n").await.unwrap();
        assert!(
            sessions
                .call(
                    "ffkv_session",
                    json!({"action":"receive","session":id,"max_bytes":1,"wait_ms":1000}),
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        let second = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_frames":1,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            second["frames"],
            json!([{"type":"bulk","encoding":"utf8","value":"abcde"}])
        );
        let third = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_frames":1,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(third["frames"], json!([{"type":"integer","value":"42"}]));
        assert!(
            sessions
                .call(
                    "ffkv_execute",
                    json!({"session":id,"commands":[["PING"]]}),
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn raw_session_supports_framed_handshake_and_binary_transfer() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (sessions, id, mut peer) = open_pair("raw").await;
        let sent = sessions
            .call(
                "ffkv_session",
                json!({"action":"send","session":id,"commands":[["PING"]]}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(sent["outcome"], "written");
        let mut command = [0; 14];
        peer.read_exact(&mut command).await.unwrap();
        assert_eq!(&command, b"*1\r\n$4\r\nPING\r\n");
        sessions
            .call(
                "ffkv_session",
                json!({"action":"send","session":id,"data_base64":"AP8="}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let mut raw = [0; 2];
        peer.read_exact(&mut raw).await.unwrap();
        assert_eq!(&raw, b"\0\xff");
        peer.write_all(b"\xff\0\r\n").await.unwrap();
        let received = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_bytes":2,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(received["data_base64"], "/wA=");
        let received = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_bytes":2,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(received["data_base64"], "DQo=");
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn execute_timeout_closes_connection_and_never_reuses_late_reply() {
        use tokio::io::AsyncReadExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        let result = sessions
            .call(
                "ffkv_execute",
                json!({"session":id,"commands":[["PING"]],"timeout_ms":10}),
                CancellationToken::new(),
            )
            .await;
        let result = result.unwrap();
        assert_eq!(result["outcome"], "unknown");
        assert_eq!(result["closed"], true);
        assert_eq!(result["replies"], json!([]));
        assert!(sessions.lookup(&id).await.is_err());
        let mut sent = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut sent))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sent, b"*1\r\n$4\r\nPING\r\n");
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn close_interrupts_receive_and_drops_both_halves() {
        use tokio::io::AsyncReadExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        let receive = sessions.call(
            "ffkv_session",
            json!({"action":"receive","session":id,"wait_ms":1000}),
            CancellationToken::new(),
        );
        let close = async {
            tokio::task::yield_now().await;
            sessions
                .call(
                    "ffkv_session",
                    json!({"action":"close","session":id}),
                    CancellationToken::new(),
                )
                .await
        };
        let (received, closed) = tokio::join!(receive, close);
        assert!(received.is_err());
        assert_eq!(closed.unwrap()["closed"], true);
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn receive_cancellation_keeps_partial_frame_for_next_call() {
        use tokio::io::AsyncWriteExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        peer.write_all(b"$5\r\nab").await.unwrap();
        let cancel = CancellationToken::new();
        let receive = sessions.call(
            "ffkv_session",
            json!({"action":"receive","session":id,"wait_ms":1000}),
            cancel.clone(),
        );
        let interrupt = async {
            tokio::time::sleep(Duration::from_millis(10)).await;
            cancel.cancel();
        };
        let (result, ()) = tokio::join!(receive, interrupt);
        assert!(result.is_err());
        assert!(sessions.lookup(&id).await.is_ok());
        peer.write_all(b"cde\r\n").await.unwrap();
        let result = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_frames":1,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            result["frames"],
            json!([{"type":"bulk","encoding":"utf8","value":"abcde"}])
        );
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn dropped_execute_future_closes_socket_without_parent_cleanup() {
        use tokio::io::AsyncReadExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        {
            let execute = sessions.call(
                "ffkv_execute",
                json!({"session":id,"commands":[["PING"]]}),
                CancellationToken::new(),
            );
            tokio::pin!(execute);
            tokio::select! {
                result = &mut execute => panic!("peer supplied no response: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(10)) => (),
            }
        }
        let mut sent = Vec::new();
        tokio::time::timeout(Duration::from_secs(1), peer.read_to_end(&mut sent))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(sent, b"*1\r\n$4\r\nPING\r\n");
        assert!(sessions.lookup(&id).await.is_err());
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn idle_expiry_releases_cap_and_owned_connection() {
        use tokio::io::AsyncReadExt;
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let mut config = Config::default();
        config.limits.max_sessions = 1;
        config.limits.session_idle_ms = 20;
        let target = config.targets.get_mut(&config.default_target).unwrap();
        target.port = listener.local_addr().unwrap().port();
        target.protocol = 2;
        let sessions = Sessions::new(Arc::new(config)).unwrap();
        let opened = sessions
            .call(
                "ffkv_session",
                json!({"action":"open"}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        let (mut peer, _) = listener.accept().await.unwrap();
        assert!(
            sessions
                .call(
                    "ffkv_session",
                    json!({"action":"open"}),
                    CancellationToken::new()
                )
                .await
                .is_err()
        );
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), peer.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        // A closed socket proves cleanup ran; yield lets its final Arc release
        // the cap permit before opening the replacement.
        tokio::task::yield_now().await;
        assert!(
            sessions
                .lookup(opened["session"].as_str().unwrap())
                .await
                .is_err()
        );
        sessions
            .call(
                "ffkv_session",
                json!({"action":"open"}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn manual_io_never_waits_on_an_execute_gate_after_transition() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let (sessions, id, mut peer) = open_pair("resp").await;
        let session = sessions.lookup(&id).await.unwrap();
        session.make_manual().await;
        let _gate = session.gate.lock().await;
        tokio::time::timeout(Duration::ZERO, session.make_manual())
            .await
            .unwrap();
        let sent = tokio::time::timeout(
            Duration::from_secs(1),
            sessions.call(
                "ffkv_session",
                json!({"action":"send","session":id,"commands":[["PING"]]}),
                CancellationToken::new(),
            ),
        )
        .await
        .expect("manual send must bypass the execute gate")
        .unwrap();
        assert_eq!(sent["outcome"], "written");
        let mut command = [0; 14];
        peer.read_exact(&mut command).await.unwrap();
        assert_eq!(&command, b"*1\r\n$4\r\nPING\r\n");
        peer.write_all(b"+PONG\r\n").await.unwrap();
        let received = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"max_frames":1,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            received["frames"],
            json!([{"type":"simple","encoding":"utf8","value":"PONG"}])
        );
        drop(_gate);
        sessions.shutdown().await;
    }

    #[tokio::test]
    async fn final_frames_are_returned_even_when_peer_closes_immediately() {
        use tokio::io::AsyncWriteExt;
        let (sessions, id, mut peer) = open_pair("resp").await;
        peer.write_all(b"+OK\r\n:42\r\n").await.unwrap();
        peer.shutdown().await.unwrap();
        let result = sessions
            .call(
                "ffkv_session",
                json!({"action":"receive","session":id,"wait_ms":1000}),
                CancellationToken::new(),
            )
            .await
            .unwrap();
        assert_eq!(
            result["frames"],
            json!([{"type":"simple","encoding":"utf8","value":"OK"},{"type":"integer","value":"42"}])
        );
        // EOF may be observed by this drain or the next receive, but complete
        // frames must be delivered in either case.
        sessions.shutdown().await;
    }
}
