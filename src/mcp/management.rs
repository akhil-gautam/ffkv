use super::{ToolDefinition, config::Config};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::CString,
    io::BufReader,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::{ffi::OsStrExt, fs::OpenOptionsExt},
    },
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex as StdMutex},
    time::{Duration, Instant},
};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
};
use tokio_tungstenite::{
    Connector, MaybeTlsStream, WebSocketStream,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use tokio_util::sync::CancellationToken;

const MAX_DOWNLOAD_BYTES: u64 = 1024 * 1024 * 1024;
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Streams = BTreeMap<String, Arc<LiveHandle>>;

pub struct Management {
    config: Arc<Config>,
    clients: BTreeMap<String, TargetClient>,
    streams: Arc<StdMutex<Streams>>,
    slots: Arc<Semaphore>,
    shutdown: CancellationToken,
}
struct TargetClient {
    origin: reqwest::Url,
    http: reqwest::Client,
    tls: Arc<rustls::ClientConfig>,
}
struct LiveHandle {
    target: String,
    state: Mutex<LiveState>,
    activity: StdMutex<LiveActivity>,
    cancel: CancellationToken,
    _permit: OwnedSemaphorePermit,
}
struct LiveActivity {
    touched: Instant,
    receives: usize,
}
struct LiveState {
    socket: Option<Socket>,
    pending: Option<Message>,
}

// A caller may drop the future rather than cancel its token. Never leave a
// partially consumed live receive available to a later, unrelated operation.
struct ReceiveGuard<'a> {
    streams: &'a StdMutex<Streams>,
    id: &'a str,
    handle: Arc<LiveHandle>,
    complete: bool,
}
impl<'a> ReceiveGuard<'a> {
    fn acquire(streams: &'a StdMutex<Streams>, id: &'a str) -> Result<Self, String> {
        // Admission and expiry share this lock, including receivers queued on state.
        let streams_guard = streams.lock().unwrap_or_else(|p| p.into_inner());
        let handle = streams_guard
            .get(id)
            .cloned()
            .ok_or("unknown or expired live stream")?;
        handle
            .activity
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .receives += 1;
        Ok(Self {
            streams,
            id,
            handle,
            complete: false,
        })
    }
}
impl Drop for ReceiveGuard<'_> {
    fn drop(&mut self) {
        let mut streams = self.streams.lock().unwrap_or_else(|p| p.into_inner());
        let mut activity = self
            .handle
            .activity
            .lock()
            .unwrap_or_else(|p| p.into_inner());
        activity.receives -= 1;
        activity.touched = Instant::now();
        if !self.complete {
            self.handle.cancel.cancel();
            streams.remove(self.id);
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct HttpArgs {
    target: Option<String>,
    method: Option<String>,
    path: String,
    body: Option<Value>,
    download_path: Option<String>,
    max_download_bytes: Option<u64>,
    timeout_ms: Option<u64>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LiveArgs {
    action: String,
    target: Option<String>,
    stream: Option<String>,
    max_frames: Option<usize>,
    wait_ms: Option<u64>,
}

pub fn definitions() -> Vec<ToolDefinition> {
    vec![
        ToolDefinition {
            name: "ffkv_management",
            description: "Call any existing management GET/POST route on an operator-configured origin, preserving query parameters and server authorization/confirmations. Redirects are never followed. Returns {target,status,content_type,body:{encoding:json|text|base64,value}} including HTTP errors. download_path streams a successful raw response to a NEW file under operator file_root and returns download:{path,bytes,sha256}; cancellation/failure removes partial files. Ordinary response bytes are bounded by configured max_response_bytes; downloads have an explicit independent byte cap. POST transport failures have unknown outcome and are never retried.",
            schema: json!({"type":"object","additionalProperties":false,"required":["path"],"properties":{
                "target":{"type":"string","description":"Configured target name; defaults to default_target."},
                "method":{"type":"string","enum":["GET","POST"],"default":"GET"},
                "path":{"type":"string","description":"Relative /api/... or /metrics URL, including arbitrary existing query parameters. No origin, traversal, backslash or fragment."},
                "body":{"description":"JSON request body, passed unchanged; includes null."},
                "download_path":{"type":"string","description":"New file, relative to file_root or absolute within it. Parent directories must exist; symlinks and overwrites are rejected."},
                "max_download_bytes":{"type":"integer","minimum":1,"maximum":MAX_DOWNLOAD_BYTES,"default":MAX_DOWNLOAD_BYTES,"description":"Only with download_path. Exceeding this cap fails explicitly and removes the partial file; never truncates."},
                "timeout_ms":{"type":"integer","minimum":1,"description":"Whole operation deadline; defaults to operation_timeout_ms and cannot exceed configured max_timeout_ms."}
            }}),
            read_only: false,
        },
        ToolDefinition {
            name: "ffkv_management_live",
            description: "Manage authenticated /api/live WebSocket handles. Open sends the server's {token} login and retains its first overview event. Receive returns {stream,frames,closed,timed_out,limit_reached}; text frames have encoding json/text and value, binary/ping/pong use base64. No background event queue or silent drops. Receive timeout preserves the connection; cancellation closes it. Handle count and idle TTL use configured max_sessions/session_idle_ms. Close and shutdown interrupt blocked receives.",
            schema: json!({"type":"object","additionalProperties":false,"required":["action"],"properties":{
                "action":{"type":"string","enum":["open","receive","close","list"]},
                "target":{"type":"string","description":"Configured target, only for open."},
                "stream":{"type":"string","description":"Opaque live handle, required for receive/close."},
                "max_frames":{"type":"integer","minimum":1,"description":"Receive frame cap, default 1; cannot exceed configured max_batch_commands. Aggregate raw frame bytes are capped by max_response_bytes; a next frame remains pending rather than being discarded."},
                "wait_ms":{"type":"integer","minimum":0,"description":"Receive deadline, defaults to operation_timeout_ms; at most max_timeout_ms. Zero drains already pending/readable frames without waiting."}
            }}),
            read_only: true,
        },
    ]
}

impl Management {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        let mut clients = BTreeMap::new();
        for (name, target) in &config.targets {
            let Some(url) = &target.management_url else {
                continue;
            };
            let origin =
                reqwest::Url::parse(url).map_err(|_| "invalid configured management URL")?;
            if !matches!(origin.scheme(), "http" | "https")
                || origin.host_str().is_none()
                || !origin.username().is_empty()
                || origin.password().is_some()
                || origin.query().is_some()
                || origin.fragment().is_some()
                || origin.path() != "/"
            {
                return Err("management_url must be an http(s) origin without credentials, path, query or fragment".into());
            }
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            let mut builder = reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy();
            if let Some(path) = &target.management_ca_cert {
                let pem = std::fs::read(path)
                    .map_err(|_| "cannot read configured management CA certificate")?;
                let certificates = rustls_pemfile::certs(&mut BufReader::new(pem.as_slice()))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|_| "invalid management CA certificate")?;
                if certificates.is_empty() {
                    return Err("management CA contains no certificates".into());
                }
                for certificate in certificates {
                    builder = builder.add_root_certificate(
                        reqwest::Certificate::from_der(certificate.as_ref())
                            .map_err(|_| "invalid management CA certificate")?,
                    );
                    roots
                        .add(certificate)
                        .map_err(|_| "invalid management CA certificate")?;
                }
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| "cannot initialize management TLS versions")?
            .with_root_certificates(roots)
            .with_no_client_auth();
            clients.insert(
                name.clone(),
                TargetClient {
                    origin,
                    http: builder
                        .build()
                        .map_err(|_| "cannot initialize management HTTP client")?,
                    tls: Arc::new(tls),
                },
            );
        }
        let streams = Arc::new(StdMutex::new(BTreeMap::<String, Arc<LiveHandle>>::new()));
        let shutdown = CancellationToken::new();
        let weak = Arc::downgrade(&streams);
        let stop = shutdown.clone();
        let ttl = Duration::from_millis(config.limits.session_idle_ms);
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = stop.cancelled() => break,
                    _ = tokio::time::sleep(ttl.min(Duration::from_secs(30))) => {}
                }
                let Some(streams) = weak.upgrade() else { break };
                expire(&streams, ttl);
            }
        });
        Ok(Self {
            slots: Arc::new(Semaphore::new(config.limits.max_sessions)),
            config,
            clients,
            streams,
            shutdown,
        })
    }

    pub async fn call(
        &self,
        name: &str,
        args: Value,
        cancel: CancellationToken,
    ) -> Result<Value, String> {
        if self.shutdown.is_cancelled() {
            return Err("management is shutting down".into());
        }
        match name {
            "ffkv_management" => {
                let body_present = args.get("body").is_some();
                let mut args: HttpArgs =
                    serde_json::from_value(args).map_err(|_| "invalid management arguments")?;
                if body_present && args.body.is_none() {
                    args.body = Some(Value::Null);
                }
                let timeout = self.timeout(args.timeout_ms, false)?;
                let effectful = args.method.as_deref() == Some("POST");
                let mut dispatched = false;
                let result = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => Err("management operation cancelled".into()),
                    _ = self.shutdown.cancelled() => Err("management is shutting down".into()),
                    result = tokio::time::timeout(timeout, self.http(args, &mut dispatched)) => result.unwrap_or_else(|_| Err("management operation timed out".into())),
                };
                result.map_err(|error: String| {
                    if effectful && dispatched {
                        format!("{error}; POST outcome may be unknown; do not automatically retry")
                    } else {
                        error
                    }
                })
            }
            "ffkv_management_live" => {
                let args: LiveArgs =
                    serde_json::from_value(args).map_err(|_| "invalid live arguments")?;
                self.live(args, cancel).await
            }
            _ => Err("unknown management tool".into()),
        }
    }

    fn timeout(&self, value: Option<u64>, zero: bool) -> Result<Duration, String> {
        let value = value.unwrap_or(self.config.limits.operation_timeout_ms);
        if (!zero && value == 0) || value > self.config.limits.max_timeout_ms {
            return Err("timeout is outside configured bounds".into());
        }
        Ok(Duration::from_millis(value))
    }

    async fn http(&self, args: HttpArgs, dispatched: &mut bool) -> Result<Value, String> {
        let (name, target) = self.config.target(args.target.as_deref())?;
        let client = self
            .clients
            .get(name)
            .ok_or("target has no management_url; configure a management origin first")?;
        let url = route_url(&client.origin, &args.path)?;
        let method = match args.method.as_deref().unwrap_or("GET") {
            "GET" => reqwest::Method::GET,
            "POST" => reqwest::Method::POST,
            _ => return Err("management method must be GET or POST".into()),
        };
        if args.max_download_bytes.is_some() && args.download_path.is_none() {
            return Err("max_download_bytes requires download_path".into());
        }
        let download_cap = args.max_download_bytes.unwrap_or(MAX_DOWNLOAD_BYTES);
        if download_cap == 0 || download_cap > MAX_DOWNLOAD_BYTES {
            return Err("max_download_bytes must be between 1 and 1073741824".into());
        }
        // Reserve the file before sending an effectful request. Drop always removes an incomplete download.
        let mut download = if let Some(path) = args.download_path.as_deref() {
            Some(Download::create(
                self.config
                    .file_root
                    .as_deref()
                    .ok_or("downloads require operator-configured file_root")?,
                self.config.file_root_alias.as_deref(),
                path,
            )?)
        } else {
            None
        };
        let mut request = client.http.request(method, url);
        if let Some(token) = target.management_token()? {
            request = request.bearer_auth(token);
        }
        if let Some(body) = args.body {
            let body =
                serde_json::to_vec(&body).map_err(|_| "cannot encode management JSON body")?;
            if body.len() > self.config.limits.max_batch_bytes {
                return Err("management request body exceeds max_batch_bytes".into());
            }
            request = request
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(body);
        }
        let request = request
            .build()
            .map_err(|_| "cannot build management HTTP request")?;
        // All local preflight has succeeded; failures from this point may follow a write.
        *dispatched = true;
        let mut response = client
            .http
            .execute(request)
            .await
            .map_err(|_| "management HTTP transport failed")?;
        let status = response.status().as_u16();
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        if let Some(download) = download.as_mut() {
            if !response.status().is_success() {
                return Err(format!(
                    "download failed with HTTP status {status}; no file retained"
                ));
            }
            if response
                .content_length()
                .is_some_and(|size| size > download_cap)
            {
                return Err("download exceeds max_download_bytes; no file retained".into());
            }
            let mut digest = Sha256::new();
            let mut count = 0u64;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "snapshot download transport failed; partial file removed")?
            {
                count = count
                    .checked_add(chunk.len() as u64)
                    .ok_or("download byte count overflow")?;
                if count > download_cap {
                    return Err("download exceeds max_download_bytes; partial file removed".into());
                }
                download
                    .file
                    .write_all(&chunk)
                    .await
                    .map_err(|_| "cannot write download; partial file removed")?;
                digest.update(&chunk);
            }
            download
                .file
                .flush()
                .await
                .map_err(|_| "cannot flush download; partial file removed")?;
            download
                .file
                .sync_all()
                .await
                .map_err(|_| "cannot sync download; partial file removed")?;
            download.complete = true;
            return Ok(
                json!({"target":name,"status":status,"content_type":content_type,"download":{
                    "path":download.path,"bytes":count,"sha256":format!("{:x}", digest.finalize())
                }}),
            );
        }
        let cap = self.config.limits.max_response_bytes;
        if response
            .content_length()
            .is_some_and(|size| size > cap as u64)
        {
            return Err("management response exceeds max_response_bytes; use download_path for raw snapshots".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| "management response transport failed")?
        {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err("management response exceeds max_response_bytes".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(
            json!({"target":name,"status":status,"content_type":content_type,"body":decode_body(&bytes)}),
        )
    }

    async fn live(&self, args: LiveArgs, cancel: CancellationToken) -> Result<Value, String> {
        expire(
            &self.streams,
            Duration::from_millis(self.config.limits.session_idle_ms),
        );
        if args.action != "open" && args.target.is_some() {
            return Err("target is only valid for live open".into());
        }
        if args.action != "receive" && (args.wait_ms.is_some() || args.max_frames.is_some()) {
            return Err("wait_ms and max_frames are only valid for live receive".into());
        }
        match args.action.as_str() {
            "open" => {
                if args.stream.is_some() {
                    return Err("open does not accept a stream handle".into());
                }
                let (name, target) = self.config.target(args.target.as_deref())?;
                let client = self
                    .clients
                    .get(name)
                    .ok_or("target has no management_url; configure a management origin first")?;
                let token = target.management_token()?.ok_or("live authentication requires a configured management token environment variable or file")?;
                let permit = self
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| "live stream limit reached; close a handle first")?;
                let connect = async {
                    let mut url = client
                        .origin
                        .join("/api/live")
                        .map_err(|_| "invalid live URL")?;
                    let scheme = if url.scheme() == "https" { "wss" } else { "ws" };
                    url.set_scheme(scheme)
                        .map_err(|_| "invalid live URL scheme")?;
                    let ws_config = WebSocketConfig::default()
                        .max_message_size(Some(self.config.limits.max_response_bytes))
                        .max_frame_size(Some(self.config.limits.max_response_bytes));
                    let (mut socket, _) = tokio_tungstenite::connect_async_tls_with_config(
                        url.as_str(),
                        Some(ws_config),
                        false,
                        Some(Connector::Rustls(client.tls.clone())),
                    )
                    .await
                    .map_err(|_| "live WebSocket connection failed (redirects are not followed)")?;
                    socket
                        .send(Message::Text(json!({"token":token}).to_string().into()))
                        .await
                        .map_err(|_| "live authentication write failed")?;
                    let first = socket
                        .next()
                        .await
                        .ok_or("live authentication rejected or connection closed")?
                        .map_err(|_| "live authentication or initial event failed")?;
                    // The actual server sends an overview immediately after successful login, not an acknowledgement.
                    if !matches!(&first, Message::Text(text) if serde_json::from_str::<Value>(text).ok()
                        .is_some_and(|value| value.get("server").is_some() && value.get("metrics").is_some()))
                    {
                        return Err(
                            "live authentication rejected or invalid initial overview".to_owned()
                        );
                    }
                    Ok::<_, String>((socket, first))
                };
                let (socket, first) = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Err("live open cancelled".into()),
                    _ = self.shutdown.cancelled() => return Err("management is shutting down".into()),
                    result = tokio::time::timeout(self.timeout(None, false)?, connect) => result.map_err(|_| "live open timed out")??,
                };
                let id = format!("live-{:032x}", rand::random::<u128>());
                self.streams
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        id.clone(),
                        Arc::new(LiveHandle {
                            target: name.to_owned(),
                            state: Mutex::new(LiveState {
                                socket: Some(socket),
                                pending: Some(first),
                            }),
                            activity: StdMutex::new(LiveActivity {
                                touched: Instant::now(),
                                receives: 0,
                            }),
                            cancel: self.shutdown.child_token(),
                            _permit: permit,
                        }),
                    );
                Ok(json!({"stream":id,"target":name}))
            }
            "list" => {
                if args.stream.is_some() {
                    return Err("list does not accept a stream handle".into());
                }
                let handles = self.streams.lock().unwrap_or_else(|p| p.into_inner());
                let streams: Vec<Value> = handles
                    .iter()
                    .map(|(id, handle)| json!({"stream":id,"target":handle.target}))
                    .collect();
                Ok(json!({"streams":streams}))
            }
            "close" => {
                let id = args.stream.as_deref().ok_or("close requires stream")?;
                let handle = self
                    .streams
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(id)
                    .ok_or("unknown or expired live stream")?;
                handle.cancel.cancel();
                let mut state = handle.state.lock().await;
                state.socket.take();
                state.pending.take();
                Ok(json!({"stream":id,"closed":true}))
            }
            "receive" => self.receive(args, cancel).await,
            _ => Err("live action must be open, receive, close or list".into()),
        }
    }

    async fn receive(&self, args: LiveArgs, cancel: CancellationToken) -> Result<Value, String> {
        let id = args.stream.as_deref().ok_or("receive requires stream")?;
        let count = args.max_frames.unwrap_or(1);
        if count == 0 || count > self.config.limits.max_batch_commands {
            return Err("max_frames is outside configured bounds".into());
        }
        let wait = self.timeout(args.wait_ms, true)?;
        let mut guard = ReceiveGuard::acquire(&self.streams, id)?;
        let handle = &guard.handle;
        let deadline = tokio::time::Instant::now() + wait;
        let receive = async {
            let mut state = match tokio::time::timeout_at(deadline, handle.state.lock()).await {
                Ok(state) => state,
                Err(_) => {
                    return Ok(
                        json!({"stream":id,"frames":[],"closed":false,"timed_out":true,"limit_reached":false}),
                    );
                }
            };
            let mut frames = Vec::new();
            let mut bytes = 0usize;
            let mut closed = false;
            let mut timed_out = false;
            let mut limit_reached = false;
            let mut error: Option<&str> = None;
            while frames.len() < count {
                let message = if let Some(message) = state.pending.take() {
                    message
                } else {
                    let Some(socket) = state.socket.as_mut() else {
                        closed = true;
                        break;
                    };
                    match tokio::time::timeout_at(deadline, socket.next()).await {
                        Err(_) => {
                            timed_out = true;
                            break;
                        }
                        Ok(None) => {
                            closed = true;
                            break;
                        }
                        Ok(Some(Err(_))) => {
                            error =
                                Some("live transport failed or frame exceeded max_response_bytes");
                            closed = true;
                            break;
                        }
                        Ok(Some(Ok(message))) => message,
                    }
                };
                let size = message.len();
                if size > self.config.limits.max_response_bytes.saturating_sub(bytes) {
                    state.pending = Some(message);
                    limit_reached = true;
                    break;
                }
                bytes += size;
                let frame = match message {
                    Message::Text(text) => match serde_json::from_str::<Value>(&text) {
                        Ok(value) => json!({"type":"text","encoding":"json","value":value}),
                        Err(_) => json!({"type":"text","encoding":"text","value":text.as_str()}),
                    },
                    Message::Binary(data) => {
                        json!({"type":"binary","encoding":"base64","value":STANDARD.encode(data)})
                    }
                    Message::Ping(data) => {
                        json!({"type":"ping","encoding":"base64","value":STANDARD.encode(data)})
                    }
                    Message::Pong(data) => {
                        json!({"type":"pong","encoding":"base64","value":STANDARD.encode(data)})
                    }
                    Message::Close(frame) => {
                        closed = true;
                        json!({"type":"close","code":frame.as_ref().map(|frame| u16::from(frame.code)),"reason":frame.as_ref().map(|frame| frame.reason.as_str())})
                    }
                    Message::Frame(_) => {
                        error = Some("unexpected raw WebSocket frame");
                        closed = true;
                        break;
                    }
                };
                frames.push(frame);
                if closed {
                    break;
                }
            }
            if closed {
                state.socket.take();
                state.pending.take();
            }
            Ok(
                json!({"stream":id,"frames":frames,"closed":closed,"timed_out":timed_out,"limit_reached":limit_reached,"error":error}),
            )
        };
        let result = tokio::select! {
            biased;
            _ = cancel.cancelled() => Err("live receive cancelled; stream closed".into()),
            _ = handle.cancel.cancelled() => Err("live stream closed or expired".into()),
            result = receive => result,
        };
        if result
            .as_ref()
            .map_or(true, |value| value["closed"] == true)
        {
            handle.cancel.cancel();
            self.streams
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(id);
            let mut state = handle.state.lock().await;
            state.socket.take();
            state.pending.take();
        }
        guard.complete = true;
        result
    }

    pub async fn shutdown(&self) {
        self.shutdown.cancel();
        let streams = std::mem::take(&mut *self.streams.lock().unwrap_or_else(|p| p.into_inner()));
        for (_, handle) in streams {
            handle.cancel.cancel();
            let mut state = handle.state.lock().await;
            state.socket.take();
            state.pending.take();
        }
    }
}
impl Drop for Management {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

fn expire(streams: &StdMutex<Streams>, ttl: Duration) {
    streams
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .retain(|_, handle| {
            let activity = handle.activity.lock().unwrap_or_else(|p| p.into_inner());
            if activity.receives == 0 && activity.touched.elapsed() >= ttl {
                handle.cancel.cancel();
                false
            } else {
                true
            }
        });
}

fn route_url(origin: &reqwest::Url, path: &str) -> Result<reqwest::Url, String> {
    if path.starts_with("//") || path.contains(['#', '\\']) || path.chars().any(char::is_control) {
        return Err(
            "management path must be a relative API path without fragment or backslash".into(),
        );
    }
    let route = path.split('?').next().unwrap_or(path);
    if !route.starts_with("/api/") && route != "/metrics" {
        return Err("management path must start /api/ or be /metrics".into());
    }
    for segment in route.split('/') {
        let decoded = percent_encoding::percent_decode_str(segment)
            .decode_utf8()
            .map_err(|_| "invalid management path encoding")?;
        if matches!(decoded.as_ref(), "." | "..")
            || decoded.contains(['/', '\\', '%'])
            || decoded.chars().any(char::is_control)
        {
            return Err("management path traversal or ambiguous encoding is forbidden".into());
        }
    }
    let url = origin
        .join(path)
        .map_err(|_| "invalid management relative URL")?;
    if url.origin() != origin.origin() {
        return Err("management requests cannot change origin".into());
    }
    Ok(url)
}
fn decode_body(bytes: &[u8]) -> Value {
    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        json!({"encoding":"json","value":value})
    } else if let Ok(text) = std::str::from_utf8(bytes) {
        json!({"encoding":"text","value":text})
    } else {
        json!({"encoding":"base64","value":STANDARD.encode(bytes)})
    }
}

// Directory-relative descriptors make creation and cleanup immune to symlink swaps.
// Never use a path-based remove on failure: a renamed parent must not redirect cleanup.
struct Download {
    file: tokio::fs::File,
    parent: std::fs::File,
    name: CString,
    path: PathBuf,
    complete: bool,
}
impl Download {
    fn create(root: &Path, alias: Option<&Path>, requested: &str) -> Result<Self, String> {
        let path = Path::new(requested);
        let relative = if path.is_absolute() {
            // Only the configured root may have a symlink spelling. Descendants
            // remain lexical and are opened one component at a time with NOFOLLOW.
            alias
                .and_then(|alias| path.strip_prefix(alias).ok())
                .or_else(|| path.strip_prefix(root).ok())
                .ok_or("download path must be within file_root")?
        } else {
            path
        };
        let components: Vec<_> = relative.components().collect();
        if components.is_empty()
            || components
                .iter()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err("download path must name a new file without traversal".into());
        }
        let mut parent = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)
            .map_err(|_| "cannot open configured file_root")?;
        for component in &components[..components.len() - 1] {
            let name = CString::new(component.as_os_str().as_bytes())
                .map_err(|_| "invalid download directory")?;
            let fd = unsafe {
                libc::openat(
                    parent.as_raw_fd(),
                    name.as_ptr(),
                    libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if fd < 0 {
                return Err(
                    "download parent must exist inside file_root and cannot be a symlink".into(),
                );
            }
            parent = unsafe { std::fs::File::from_raw_fd(fd) };
        }
        let name = CString::new(components.last().unwrap().as_os_str().as_bytes())
            .map_err(|_| "invalid download filename")?;
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                name.as_ptr(),
                libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                0o600 as libc::c_uint,
            )
        };
        if fd < 0 {
            return Err("cannot create download file; destination must be new and writable".into());
        }
        let file = unsafe { std::fs::File::from_raw_fd(fd) };
        Ok(Self {
            file: tokio::fs::File::from_std(file),
            parent,
            name,
            path: root.join(relative),
            complete: false,
        })
    }
}
impl Drop for Download {
    fn drop(&mut self) {
        if !self.complete {
            unsafe {
                libc::unlinkat(self.parent.as_raw_fd(), self.name.as_ptr(), 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;
    use std::os::unix::fs::symlink;
    use tokio::io::AsyncReadExt;

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new() -> Self {
            let path = std::env::temp_dir().join(format!(
                "ffkv-mcp-management-{:032x}",
                rand::random::<u128>()
            ));
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }

    fn configured_root(root: &Path) -> Config {
        Config::load(
            super::super::config::Options::try_parse_from([
                "ffkv-mcp",
                "--file-root",
                root.to_str().unwrap(),
            ])
            .unwrap(),
        )
        .unwrap()
    }

    fn download(config: &Config, path: &Path) -> Result<Download, String> {
        Download::create(
            config.file_root.as_deref().unwrap(),
            config.file_root_alias.as_deref(),
            path.to_str().unwrap(),
        )
    }

    #[test]
    fn downloads_accept_configured_root_alias_and_canonical_paths() {
        let directory = TestDirectory::new();
        let root = directory.0.join("root");
        let alias = directory.0.join("alias");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("child")).unwrap();
        symlink(&root, &alias).unwrap();
        for configured in [&root, &alias] {
            let config = configured_root(configured);
            let canonical = config.file_root.as_ref().unwrap();
            for requested in [
                configured.join("child/download"),
                canonical.join("child/download"),
                PathBuf::from("child/download"),
            ] {
                let file = download(&config, &requested).unwrap();
                assert_eq!(file.path, canonical.join("child/download"));
                assert!(file.path.exists());
                assert!(download(&config, &requested).is_err());
                drop(file);
                assert!(!root.join("child/download").exists());
            }
        }
    }

    #[test]
    fn downloads_reject_outside_paths_and_all_below_root_symlinks() {
        let directory = TestDirectory::new();
        let root = directory.0.join("root");
        let alias = directory.0.join("alias");
        let outside = directory.0.join("outside");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("child")).unwrap();
        std::fs::create_dir(&outside).unwrap();
        symlink(&root, &alias).unwrap();
        symlink(root.join("child"), root.join("inside-link")).unwrap();
        symlink(&outside, root.join("outside-link")).unwrap();
        symlink(root.join("child/new"), root.join("file-link")).unwrap();
        let config = configured_root(&alias);
        for requested in [
            outside.join("new"),
            directory.0.join("root-sibling/new"),
            alias.join("../outside/new"),
            alias.join("inside-link/new"),
            alias.join("outside-link/new"),
            alias.join("file-link"),
            root.canonicalize().unwrap().join("inside-link/new"),
            PathBuf::from("inside-link/new"),
            PathBuf::from("outside-link/new"),
            PathBuf::from("file-link"),
        ] {
            assert!(download(&config, &requested).is_err(), "{requested:?}");
        }
        assert!(!outside.join("new").exists());
        assert!(!root.join("child/new").exists());
    }

    async fn live_handle(management: &Management) -> (Arc<LiveHandle>, WebSocketStream<TcpStream>) {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let (client, server) = tokio::join!(
            TcpStream::connect(listener.local_addr().unwrap()),
            listener.accept()
        );
        let socket = WebSocketStream::from_raw_socket(
            MaybeTlsStream::Plain(client.unwrap()),
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let server = WebSocketStream::from_raw_socket(
            server.unwrap().0,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let handle = Arc::new(LiveHandle {
            target: "local".into(),
            state: Mutex::new(LiveState {
                socket: Some(socket),
                pending: Some(Message::Text("first".into())),
            }),
            activity: StdMutex::new(LiveActivity {
                touched: Instant::now(),
                receives: 0,
            }),
            cancel: CancellationToken::new(),
            _permit: management.slots.clone().try_acquire_owned().unwrap(),
        });
        management
            .streams
            .lock()
            .unwrap()
            .insert("test".into(), handle.clone());
        (handle, server)
    }

    fn receive_args() -> LiveArgs {
        serde_json::from_value(json!({
            "action":"receive", "stream":"test", "max_frames":1, "wait_ms":5000
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn active_and_queued_receives_prevent_expiry_until_idle() {
        let management = Management::new(Arc::new(Config::default())).unwrap();
        let (handle, mut server) = live_handle(&management).await;
        let state = handle.state.lock().await;
        let mut first = Box::pin(management.receive(receive_args(), CancellationToken::new()));
        let mut second = Box::pin(management.receive(receive_args(), CancellationToken::new()));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        assert_eq!(handle.activity.lock().unwrap().receives, 2);
        let ttl = Duration::from_secs(1);
        handle.activity.lock().unwrap().touched = Instant::now() - ttl * 2;
        expire(&management.streams, ttl);
        assert!(!handle.cancel.is_cancelled());
        assert!(state.pending.is_some());
        drop(state);
        assert_eq!(first.await.unwrap()["frames"][0]["value"], "first");
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        assert_eq!(handle.activity.lock().unwrap().receives, 1);
        handle.activity.lock().unwrap().touched = Instant::now() - ttl * 2;
        expire(&management.streams, ttl);
        assert!(!handle.cancel.is_cancelled());
        server.send(Message::Text("second".into())).await.unwrap();
        assert_eq!(second.await.unwrap()["frames"][0]["value"], "second");
        assert_eq!(handle.activity.lock().unwrap().receives, 0);
        expire(&management.streams, ttl);
        assert!(management.streams.lock().unwrap().contains_key("test"));
        handle.activity.lock().unwrap().touched = Instant::now() - ttl * 2;
        expire(&management.streams, ttl);
        assert!(handle.cancel.is_cancelled());
        assert!(management.streams.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn dropped_queued_receive_releases_activity_and_closes_stream() {
        let management = Management::new(Arc::new(Config::default())).unwrap();
        let (handle, _server) = live_handle(&management).await;
        let state = handle.state.lock().await;
        let mut first = Box::pin(management.receive(receive_args(), CancellationToken::new()));
        let mut second = Box::pin(management.receive(receive_args(), CancellationToken::new()));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        drop(second);
        assert_eq!(handle.activity.lock().unwrap().receives, 1);
        assert!(handle.cancel.is_cancelled());
        assert!(management.streams.lock().unwrap().is_empty());
        drop(state);
        assert!(first.await.is_err());
        assert_eq!(handle.activity.lock().unwrap().receives, 0);
    }

    #[tokio::test]
    async fn dropped_active_receive_releases_activity_and_closes_stream() {
        let management = Management::new(Arc::new(Config::default())).unwrap();
        let (handle, _server) = live_handle(&management).await;
        management
            .receive(receive_args(), CancellationToken::new())
            .await
            .unwrap();
        let mut receive = Box::pin(management.receive(receive_args(), CancellationToken::new()));
        assert!(futures_util::poll!(receive.as_mut()).is_pending());
        assert_eq!(handle.activity.lock().unwrap().receives, 1);
        drop(receive);
        assert_eq!(handle.activity.lock().unwrap().receives, 0);
        assert!(handle.cancel.is_cancelled());
        assert!(management.streams.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn post_preflight_errors_and_early_cancellation_have_known_outcomes() {
        let directory = TestDirectory::new();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let mut config = configured_root(&directory.0);
        config.targets.get_mut("local").unwrap().management_url =
            Some(format!("http://{}", listener.local_addr().unwrap()));
        config.limits.max_batch_bytes = 4;
        let management = Management::new(Arc::new(config)).unwrap();
        for args in [
            json!({"method":"POST","path":"/api/x","target":"absent"}),
            json!({"method":"POST","path":"/wrong"}),
            json!({"method":"POST","path":"/api/x","body":"oversized"}),
            json!({"method":"POST","path":"/api/x","max_download_bytes":0}),
            json!({"method":"POST","path":"/api/x","download_path":"../outside"}),
        ] {
            let error = management
                .call("ffkv_management", args, CancellationToken::new())
                .await
                .unwrap_err();
            assert!(!error.contains("outcome may be unknown"), "{error}");
        }
        let cancel = CancellationToken::new();
        cancel.cancel();
        let error = management
            .call(
                "ffkv_management",
                json!({"method":"POST","path":"/api/x"}),
                cancel,
            )
            .await
            .unwrap_err();
        assert!(!error.contains("outcome may be unknown"), "{error}");

        let secret = directory.0.join("secret");
        std::fs::write(&secret, "private-token\u{1}").unwrap();
        for token in [directory.0.join("missing-secret"), secret] {
            let mut config = Config::default();
            let target = config.targets.get_mut("local").unwrap();
            target.management_url = Some(format!("http://{}", listener.local_addr().unwrap()));
            target.management_token_file = Some(token);
            let management = Management::new(Arc::new(config)).unwrap();
            let error = management
                .call(
                    "ffkv_management",
                    json!({"method":"POST","path":"/api/x"}),
                    CancellationToken::new(),
                )
                .await
                .unwrap_err();
            assert!(!error.contains("outcome may be unknown"), "{error}");
            assert!(!error.contains("private-token"), "{error}");
        }
        assert_eq!(
            listener.accept().unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );
    }

    #[tokio::test]
    async fn post_cancellation_after_dispatch_warns_without_exposing_secrets() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let mut config = Config::default();
        config.targets.get_mut("local").unwrap().management_url =
            Some(format!("http://{}", listener.local_addr().unwrap()));
        let management = Management::new(Arc::new(config)).unwrap();
        let cancel = CancellationToken::new();
        let request = management.call(
            "ffkv_management",
            json!({"method":"POST","path":"/api/x?secret=private-value"}),
            cancel.clone(),
        );
        let server = async {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut byte = [0];
            socket.read_exact(&mut byte).await.unwrap();
            cancel.cancel();
            socket
        };
        let (result, _socket) = tokio::join!(request, server);
        let error = result.unwrap_err();
        assert!(error.contains("POST outcome may be unknown"), "{error}");
        assert!(!error.contains("private-value"), "{error}");
    }
}
