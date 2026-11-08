//! Isolated management authorization, bounded HTTP operations and durable operator audit.
mod audit;
mod auth;
mod dto;
mod replication;
mod routes;
mod transport;
use crate::{
    metrics::Metrics,
    protocol::Resp,
    request::{CommandRequest, Request as BusRequest},
    storage::Store,
};
use auth::{Auth, Principal, Role};
use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    http::{StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
#[derive(Clone)]
pub struct Config {
    pub token_file: PathBuf,
    pub audit_file: PathBuf,
    pub ui_dir: Option<PathBuf>,
    pub oidc: Option<OidcConfig>,
    pub tls: Option<TlsConfig>,
    pub allowed_origin: Option<String>,
    pub allow_sensitive_reveal: bool,
    pub replication_enabled: bool,
}
#[derive(Clone)]
pub struct TlsConfig {
    pub cert_file: PathBuf,
    pub key_file: PathBuf,
}
#[derive(Clone)]
pub struct OidcConfig {
    pub issuer: String,
    pub audience: String,
    pub jwks_url: String,
    pub admin_role: String,
    pub viewer_role: String,
    pub allow_loopback_http: bool,
}
#[derive(Clone)]
struct App {
    config: Config,
    auth: Arc<Auth>,
    audit: Arc<audit::Audit>,
    tx: mpsc::Sender<BusRequest>,
    metrics: Arc<Metrics>,
    storage: Option<Arc<Store>>,
    catalog: Arc<HashMap<String, Command>>,
    work: Arc<Semaphore>,
    mutations: Arc<Semaphore>,
    websockets: Arc<Semaphore>,
    rate: Arc<Mutex<Rate>>,
    ui: Option<PathBuf>,
}
#[derive(Clone)]
struct Command {
    name: String,
    arity: i64,
    flags: Vec<String>,
}
impl Command {
    fn readonly(&self) -> bool {
        self.flags.iter().any(|flag| flag == "readonly")
            // Redis omits "readonly" on safe non-dataset control commands.
            || ["PING", "ECHO", "INFO", "COMMAND"]
                .iter()
                .any(|name| self.name.eq_ignore_ascii_case(name))
    }
}
struct Rate {
    since: Instant,
    requests: u32,
}
#[derive(Debug)]
pub(super) struct ApiError {
    status: StatusCode,
    message: &'static str,
}
impl ApiError {
    fn bad(message: &'static str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message,
        }
    }
    fn unauthorized() -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            message: "authentication required",
        }
    }
    fn forbidden() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            message: "operation not permitted",
        }
    }
    fn unavailable(message: &'static str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            message,
        }
    }
}
#[derive(Clone)]
struct SafeError;
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let mut response = (self.status, axum::Json(json!({"error":self.message}))).into_response();
        response.extensions_mut().insert(SafeError);
        response.headers_mut().insert(
            header::CACHE_CONTROL,
            axum::http::HeaderValue::from_static("no-store"),
        );
        response
    }
}
type Result<T, E = ApiError> = std::result::Result<T, E>;
struct Execution {
    response: Resp,
    audit: Option<audit::Row>,
    write: bool,
}
impl App {
    async fn command(&self, args: Vec<Bytes>) -> Result<Resp> {
        validate(&args)?;
        let (reply, receive) = oneshot::channel();
        let request = BusRequest::Command(CommandRequest::internal(args, reply));
        tokio::time::timeout(Duration::from_secs(10), async {
            self.tx
                .send(request)
                .await
                .map_err(|_| ApiError::unavailable("server unavailable"))?;
            receive
                .await
                .map_err(|_| ApiError::unavailable("server unavailable"))
        })
        .await
        .map_err(|_| ApiError::unavailable("command timed out; outcome may be unknown"))?
    }
    async fn call(&self, args: &[&str]) -> Result<Resp> {
        self.command(
            args.iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect(),
        )
        .await
    }
    async fn authorized(
        &self,
        p: &Principal,
        operation: &str,
        target: Option<String>,
        args: Vec<Bytes>,
        reveal: bool,
        write: bool,
    ) -> Result<Execution> {
        validate(&args)?;
        if (write || reveal) && p.role != Role::Admin {
            return Err(ApiError::forbidden());
        }
        if !(write || reveal) {
            return self.command(args).await.map(|response| Execution {
                response,
                audit: None,
                write: false,
            });
        }
        let operation = if reveal && operation != "reveal" {
            format!("{operation}:reveal")
        } else {
            operation.to_owned()
        };
        let row = self.audit.row(p, &operation, target);
        let permit = self
            .mutations
            .clone()
            .try_acquire_owned()
            .map_err(|_| ApiError::unavailable("audited operation limit reached"))?;
        let app = self.clone();
        // Keep the journal lifecycle alive if the browser disconnects after dispatch.
        tokio::spawn(async move{
            let _permit=permit;
            app.audit.append(row.clone()).await?;
            let result=app.command(args).await;
            let mut outcome=row;
            outcome.timestamp_ms=crate::metrics::now_ms();
            outcome.outcome=match &result{Ok(Resp::Error(_)) if write=>"executed/error",Ok(Resp::Error(_))=>"rejected",Ok(_)=>"success",Err(_)=>"unknown"}.into();
            app.audit.append(outcome.clone()).await.map_err(|error|if write{ApiError::unavailable("command may have completed; outcome audit failed; do not retry automatically")}else{error})?;
            result.map(|response|Execution{response,audit:Some(outcome),write})
        }).await.map_err(|_|if write{ApiError::unavailable("command may have completed; audited operation interrupted; do not retry automatically")}else{ApiError::unavailable("audited operation interrupted")})?
    }
    async fn deliver(&self, execution: Execution, value: Result<Value>) -> Result<Response> {
        match value.and_then(json_response) {
            Ok(response) => Ok(response),
            Err(error) if execution.write => {
                if let Some(mut row) = execution.audit {
                    row.timestamp_ms = crate::metrics::now_ms();
                    row.outcome = if error.message.contains("limit") {
                        "executed/result-too-large"
                    } else {
                        "executed/result-delivery-failed"
                    }
                    .into();
                    self.audit.append(row).await.map_err(|_|ApiError::unavailable("command completed; result delivery and outcome audit failed; do not retry automatically"))?;
                }
                Err(ApiError {
                    status: StatusCode::BAD_GATEWAY,
                    message: "command completed; result delivery failed; do not retry automatically",
                })
            }
            Err(error) => Err(error),
        }
    }
}
fn validate(args: &[Bytes]) -> Result<()> {
    crate::protocol::validate_command(args).map_err(|_| ApiError::bad("invalid command vector"))?;
    crate::engine::BoundCommand::bind(args)
        .check()
        .map_err(|_| ApiError::bad("invalid command syntax"))?;
    Ok(())
}
fn admin(p: &Principal) -> Result<()> {
    if p.role == Role::Admin {
        Ok(())
    } else {
        Err(ApiError::forbidden())
    }
}
fn json_response(mut value: Value) -> Result<Response> {
    dto::js_safe(&mut value);
    let bytes = serde_json::to_vec(&value)
        .map_err(|_| ApiError::unavailable("response encoding failed"))?;
    if bytes.len() > dto::MAX_RESPONSE {
        return Err(ApiError::bad(
            "response exceeds management limit; use a smaller page",
        ));
    }
    Ok(([(header::CONTENT_TYPE, "application/json")], bytes).into_response())
}
fn bearer(headers: &axum::http::HeaderMap) -> Result<&str> {
    if headers.get_all(header::AUTHORIZATION).iter().count() != 1 {
        return Err(ApiError::unauthorized());
    }
    let value = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(ApiError::unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or_else(ApiError::unauthorized)?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return Err(ApiError::unauthorized());
    }
    Ok(token)
}
async fn guard(State(app): State<App>, mut request: Request, next: Next) -> Response {
    let path = request.uri().path();
    let api = path.starts_with("/api") || path == "/metrics";
    if request.uri().to_string().len() > 16384 {
        return ApiError::bad("request target too large").into_response();
    }
    let permit = match app.work.clone().try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            return ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                message: "management busy",
            }
            .into_response();
        }
    };
    if api {
        let allowed = {
            let mut rate = app.rate.lock().unwrap_or_else(|p| p.into_inner());
            if rate.since.elapsed() >= Duration::from_secs(1) {
                rate.since = Instant::now();
                rate.requests = 0;
            }
            rate.requests += 1;
            rate.requests <= 100
        };
        if !allowed {
            return ApiError {
                status: StatusCode::TOO_MANY_REQUESTS,
                message: "management rate exceeded",
            }
            .into_response();
        }
        if let Some(site) = request
            .headers()
            .get("sec-fetch-site")
            .and_then(|v| v.to_str().ok())
            && site == "cross-site"
        {
            return ApiError::forbidden().into_response();
        }
        if let Some(origin) = request.headers().get(header::ORIGIN) {
            let valid = origin
                .to_str()
                .ok()
                .is_some_and(|origin| origin_allowed(&app.config, &request, origin));
            if !valid {
                return ApiError::forbidden().into_response();
            }
        }
        if path != "/api/live" {
            let principal = match bearer(request.headers()) {
                Ok(token) => app.auth.authenticate(token).await,
                Err(error) => Err(error),
            };
            match principal {
                Ok(p) => {
                    request.extensions_mut().insert(p);
                }
                Err(error) => return error.into_response(),
            }
        }
    }
    let response = tokio::time::timeout(Duration::from_secs(30), next.run(request)).await;
    drop(permit);
    let mut response = match response {
        Ok(r) => r,
        Err(_) => ApiError::unavailable("management request timed out").into_response(),
    };
    if response.status().is_client_error() && response.extensions().get::<SafeError>().is_none() {
        response = ApiError {
            status: response.status(),
            message: "invalid management request",
        }
        .into_response();
    }
    for (name, value) in [
        ("x-content-type-options", "nosniff"),
        ("referrer-policy", "no-referrer"),
        ("x-frame-options", "DENY"),
        ("cache-control", "no-store"),
        (
            "content-security-policy",
            "default-src 'self'; script-src 'self'; style-src 'self'; style-src-attr 'unsafe-inline'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    ] {
        response.headers_mut().insert(
            axum::http::HeaderName::from_static(name),
            axum::http::HeaderValue::from_static(value),
        );
    }
    response
}
fn origin_allowed(config: &Config, request: &Request, origin: &str) -> bool {
    if let Some(expected) = &config.allowed_origin {
        return origin == expected;
    }
    let Ok(url) = reqwest::Url::parse(origin) else {
        return false;
    };
    // HTTP/2 carries :authority in the URI and may omit the HTTP/1 Host header.
    let host = request
        .uri()
        .authority()
        .map(|value| value.as_str())
        .or_else(|| {
            request
                .headers()
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
        });
    let scheme = if config.tls.is_some() {
        "https"
    } else {
        "http"
    };
    url.scheme() == scheme
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
        && url.username().is_empty()
        && url.password().is_none()
        && host.is_some_and(|host| origin == format!("{scheme}://{host}"))
}
async fn unknown_api() -> Response {
    (
        StatusCode::NOT_FOUND,
        axum::Json(json!({"error":"unknown management endpoint"})),
    )
        .into_response()
}
pub async fn serve(
    listener: tokio::net::TcpListener,
    config: Config,
    tx: mpsc::Sender<BusRequest>,
    metrics: Arc<Metrics>,
    storage: Option<Arc<Store>>,
    mut shutdown: watch::Receiver<bool>,
) -> io::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    if let Some(origin) = &config.allowed_origin {
        let url = reqwest::Url::parse(origin).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid management origin")
        })?;
        if !matches!(url.scheme(), "http" | "https")
            || url.origin().ascii_serialization() != *origin
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "management origin must be an exact HTTP origin",
            ));
        }
    }
    let auth = Arc::new(Auth::load(&config.token_file, config.oidc.clone())?);
    let audit = Arc::new(audit::Audit::open(&config.audit_file)?);
    let ui = config
        .ui_dir
        .as_ref()
        .map(std::fs::canonicalize)
        .transpose()?;
    let mut app = App {
        config: config.clone(),
        auth,
        audit,
        tx,
        metrics,
        storage,
        catalog: Arc::new(HashMap::new()),
        work: Arc::new(Semaphore::new(64)),
        mutations: Arc::new(Semaphore::new(32)),
        websockets: Arc::new(Semaphore::new(16)),
        rate: Arc::new(Mutex::new(Rate {
            since: Instant::now(),
            requests: 0,
        })),
        ui,
    };
    let catalog = app
        .call(&["COMMAND"])
        .await
        .map_err(|_| io::Error::other("management command catalog unavailable"))?;
    let mut commands = HashMap::new();
    if let Some(entries) = dto::list(&catalog) {
        for entry in entries {
            if let Some(parts) = dto::list(entry) {
                if parts.len() < 3 {
                    continue;
                }
                let Some(name) = dto::text(&parts[0]).and_then(|b| std::str::from_utf8(b).ok())
                else {
                    continue;
                };
                let Some(arity) = dto::int(&parts[1]) else {
                    continue;
                };
                let flags = dto::list(&parts[2])
                    .unwrap_or(&[])
                    .iter()
                    .filter_map(dto::text)
                    .filter_map(|b| std::str::from_utf8(b).ok())
                    .map(str::to_owned)
                    .collect();
                commands.insert(
                    name.to_ascii_uppercase(),
                    Command {
                        name: name.into(),
                        arity,
                        flags,
                    },
                );
            }
        }
    }
    if commands.is_empty() {
        return Err(io::Error::other("invalid management command catalog"));
    }
    app.catalog = Arc::new(commands);
    let router = Router::new()
        .route("/api/replication/status", get(replication::status))
        .route("/api/replication/snapshot", get(replication::snapshot))
        .route("/api/session", get(routes::session))
        .route("/api/overview", get(routes::overview))
        .route("/api/keys", get(routes::keys))
        .route("/api/key", get(routes::key))
        .route("/api/queues", get(routes::queues))
        .route("/api/queue", get(routes::queue))
        .route("/api/queue/action", post(routes::queue_action))
        .route("/api/commands", get(routes::commands))
        .route("/api/command", post(routes::console))
        .route("/api/storage", get(routes::storage))
        .route("/api/storage/action", post(routes::storage_action))
        .route("/api/audit", get(routes::audit_page))
        .route("/api/live", get(routes::live))
        .route("/metrics", get(routes::prometheus))
        .route("/api", get(unknown_api))
        .route("/api/{*path}", get(unknown_api).post(unknown_api))
        .fallback(routes::static_file)
        .layer(DefaultBodyLimit::max(12 * 1024 * 1024))
        .layer(middleware::from_fn_with_state(app.clone(), guard))
        .with_state(app);
    transport::run(listener, config.tls, router, async move {
        while !*shutdown.borrow() {
            if shutdown.changed().await.is_err() {
                break;
            }
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> Config {
        Config {
            token_file: PathBuf::new(),
            audit_file: PathBuf::new(),
            ui_dir: None,
            oidc: None,
            tls: Some(TlsConfig {
                cert_file: PathBuf::new(),
                key_file: PathBuf::new(),
            }),
            allowed_origin: None,
            allow_sensitive_reveal: false,
            replication_enabled: false,
        }
    }

    #[test]
    fn same_origin_http2_uses_authority_and_rejects_host_spoofing() {
        let mut request = Request::builder()
            .version(axum::http::Version::HTTP_2)
            .uri("https://localhost:6381/api/queue/action")
            .body(axum::body::Body::empty())
            .unwrap();
        let config = config();
        assert!(origin_allowed(&config, &request, "https://localhost:6381"));
        assert!(!origin_allowed(&config, &request, "https://localhost:6382"));
        assert!(!origin_allowed(&config, &request, "http://localhost:6381"));
        request
            .headers_mut()
            .insert(header::HOST, "attacker.example".parse().unwrap());
        assert!(!origin_allowed(
            &config,
            &request,
            "https://attacker.example"
        ));
    }

    #[test]
    fn http1_host_and_explicit_proxy_origin_remain_enforced() {
        let request = Request::builder()
            .uri("/api/command")
            .header(header::HOST, "localhost:6381")
            .body(axum::body::Body::empty())
            .unwrap();
        let mut config = config();
        assert!(origin_allowed(&config, &request, "https://localhost:6381"));
        assert!(!origin_allowed(
            &config,
            &request,
            "https://attacker.example"
        ));
        config.allowed_origin = Some("https://console.example".into());
        assert!(origin_allowed(&config, &request, "https://console.example"));
        assert!(!origin_allowed(&config, &request, "https://localhost:6381"));
    }

    #[test]
    fn safe_controls_do_not_grant_fast_dataset_writes() {
        for (name, flags, allowed) in [
            ("PING", vec!["fast"], true),
            ("COMMAND", vec![], true),
            ("GET", vec!["readonly", "fast"], true),
            ("INCR", vec!["write", "fast"], false),
            ("AUTH", vec!["fast"], false),
        ] {
            let command = Command {
                name: name.into(),
                arity: -1,
                flags: flags.into_iter().map(String::from).collect(),
            };
            assert_eq!(command.readonly(), allowed, "{name}");
        }
    }
}
