use super::*;
use axum::{
    Extension, Json,
    extract::{
        Query, WebSocketUpgrade,
        ws::{Message, WebSocket},
    },
    http::Uri,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use std::path::{Component, Path};
#[derive(Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Page {
    cursor: Option<String>,
    count: Option<usize>,
    #[serde(rename = "match")]
    pattern: Option<String>,
}
impl Page {
    fn count(&self) -> Result<usize> {
        let n = self.count.unwrap_or(100);
        if !(1..=200).contains(&n) {
            return Err(ApiError::bad("count must be between 1 and 200"));
        }
        Ok(n)
    }
    fn numeric_cursor(&self) -> Result<String> {
        let c = self.cursor.as_deref().unwrap_or("0");
        if c.len() > 20 || c.parse::<u64>().is_err() {
            return Err(ApiError::bad("invalid cursor"));
        }
        Ok(c.into())
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct KeyQuery {
    key: String,
    cursor: Option<String>,
    count: Option<usize>,
    reveal: Option<bool>,
    state: Option<String>,
}
fn decode(value: &str) -> Result<Bytes> {
    if value.len() > 8192 {
        return Err(ApiError::bad("key or capability exceeds limit"));
    }
    let value = STANDARD
        .decode(value)
        .map_err(|_| ApiError::bad("invalid base64"))?;
    if value.len() > 4096 {
        return Err(ApiError::bad("key or capability exceeds limit"));
    }
    Ok(value.into())
}
fn key_args(name: &'static str, key: &Bytes) -> Vec<Bytes> {
    vec![Bytes::from_static(name.as_bytes()), key.clone()]
}
fn ensure(resp: Resp) -> Result<Resp> {
    if matches!(resp, Resp::Error(_)) {
        Err(ApiError::bad("server rejected management operation"))
    } else {
        Ok(resp)
    }
}
fn kind(resp: &Resp) -> Result<String> {
    let value = dto::text(resp)
        .and_then(|v| std::str::from_utf8(v).ok())
        .ok_or_else(|| ApiError::unavailable("invalid server response"))?;
    Ok(value.into())
}
fn scan_page(resp: &Resp) -> Result<(String, &[Resp])> {
    let items = dto::list(resp)
        .filter(|v| v.len() == 2)
        .ok_or_else(|| ApiError::unavailable("invalid scan response"))?;
    let cursor = kind(&items[0])?;
    let values =
        dto::list(&items[1]).ok_or_else(|| ApiError::unavailable("invalid scan response"))?;
    if values.len() > 1000 {
        return Err(ApiError::bad("scan page exceeds management limit"));
    }
    Ok((cursor, values))
}
fn safe_meta(resp: &Resp, reveal: bool, sensitive: bool) -> Result<Value> {
    match resp {
        Resp::Map(pairs) => {
            let mut out = Vec::new();
            for (k, v) in pairs {
                let name = dto::text(k)
                    .and_then(|b| std::str::from_utf8(b).ok())
                    .unwrap_or("");
                let protected = ["payload", "error", "receipt", "token", "secret", "password"]
                    .iter()
                    .any(|word| name.to_ascii_lowercase().contains(word));
                out.push([
                    dto::response(k, true, true)?,
                    if protected {
                        dto::response(v, reveal, sensitive)?
                    } else {
                        safe_meta(v, reveal, sensitive)?
                    },
                ]);
            }
            Ok(json!({"type":"map","value":out}))
        }
        Resp::Array(Some(values)) | Resp::Set(values) => {
            let values = values
                .iter()
                .map(|v| safe_meta(v, reveal, sensitive))
                .collect::<Result<Vec<_>>>()?;
            Ok(json!({"type":if matches!(resp,Resp::Set(_)){"set"}else{"array"},"value":values}))
        }
        _ => dto::response(resp, true, true),
    }
}
pub(super) async fn session(Extension(p): Extension<Principal>) -> Result<Response> {
    json_response(
        serde_json::to_value(p).map_err(|_| ApiError::unavailable("session unavailable"))?,
    )
}
async fn overview_value(app: &App) -> Result<Value> {
    let snapshot = app.metrics.snapshot();
    let count = ensure(app.call(&["DBSIZE"]).await?)?;
    let stats = ensure(app.call(&["FFKV.STATS"]).await?)?;
    let server = dto::field(&stats, "server");
    let field = |name: &str| {
        server
            .and_then(|s| dto::field(s, name))
            .and_then(dto::text)
            .and_then(|v| std::str::from_utf8(v).ok())
            .map(str::to_owned)
    };
    Ok(
        json!({"server":{"name":"ffkv","version":env!("CARGO_PKG_VERSION"),"redis_baseline":field("redis_baseline").unwrap_or_else(||"7.2".into()),"shards":snapshot.server.shards,"storage_mode":snapshot.server.storage_mode,"durability":snapshot.server.durability,"role":snapshot.replication.role},"metrics":snapshot,"storage":storage_stats(app),"key_count":dto::int(&count).map(dto::integer)}),
    )
}
pub(super) async fn overview(State(app): State<App>) -> Result<Response> {
    json_response(overview_value(&app).await?)
}
fn storage_stats(app: &App) -> Value {
    match &app.storage {
        None => Value::Null,
        Some(store) => {
            let s = store.stats();
            json!({"live_values":s.live_values,"live_payload_bytes":s.live_payload_bytes,"physical_bytes":s.physical_bytes,"cached_payload_bytes":s.cached_payload_bytes,"cache_hits":s.cache_hits,"cache_misses":s.cache_misses,"reads":s.reads,"writes":s.writes,"compactions":s.compactions,"reclaimed_bytes":s.reclaimed_bytes})
        }
    }
}
pub(super) async fn keys(State(app): State<App>, Query(page): Query<Page>) -> Result<Response> {
    let count = page.count()?;
    let cursor = page.numeric_cursor()?;
    let pattern = page.pattern.unwrap_or_else(|| "*".into());
    if pattern.len() > 1024 {
        return Err(ApiError::bad("match pattern exceeds limit"));
    }
    let result = ensure(
        app.call(&[
            "SCAN",
            &cursor,
            "MATCH",
            &pattern,
            "COUNT",
            &count.to_string(),
        ])
        .await?,
    )?;
    let (cursor, values) = scan_page(&result)?;
    let mut rows = Vec::new();
    for value in values {
        let key = Bytes::copy_from_slice(
            dto::text(value).ok_or_else(|| ApiError::unavailable("invalid scan key"))?,
        );
        let ty = kind(&ensure(app.command(key_args("TYPE", &key)).await?)?)?;
        let ttl = ensure(app.command(key_args("PTTL", &key)).await?)?;
        rows.push(json!({"key":dto::bytes(&key,true,true),"type":ty,"ttl_ms":dto::int(&ttl).map(dto::integer)}));
    }
    json_response(json!({"cursor":cursor,"keys":rows}))
}
async fn inspect(app: &App, p: &Principal, q: KeyQuery, queue_only: bool) -> Result<Value> {
    let key = decode(&q.key)?;
    let count = Page {
        count: q.count,
        ..Page::default()
    }
    .count()?;
    let reveal = q.reveal.unwrap_or(false);
    if reveal {
        admin(p)?;
    }
    let ty = kind(&ensure(app.command(key_args("TYPE", &key)).await?)?)?;
    let ttl = ensure(app.command(key_args("PTTL", &key)).await?)?;
    let cursor = q.cursor.unwrap_or_else(|| "0".into());
    if cursor.len() > 128 || cursor.chars().any(char::is_control) {
        return Err(ApiError::bad("invalid cursor"));
    }
    let native = ty == "ffkv-queue";
    let mut empty_sidekiq = false;
    if queue_only && !native && ty == "none" && key.starts_with(b"queue:") {
        let membership = ensure(
            app.command(vec![
                Bytes::from_static(b"SISMEMBER"),
                Bytes::from_static(b"queues"),
                key.slice(6..),
            ])
            .await?,
        )?;
        empty_sidekiq = dto::int(&membership) == Some(1);
    }
    if queue_only && !native && (!(ty == "list" || empty_sidekiq) || !key.starts_with(b"queue:")) {
        return Err(ApiError::bad("not a native or Sidekiq queue"));
    }
    let browse_type = if empty_sidekiq { "list" } else { ty.as_str() };
    let state = q.state.unwrap_or_else(|| {
        if native {
            "ready".into()
        } else {
            "queued".into()
        }
    });
    let mut args;
    let mut result_cursor = None;
    if native {
        if !matches!(state.as_str(), "ready" | "scheduled" | "running" | "dead") {
            return Err(ApiError::bad("invalid native state"));
        }
        args = key_args("FQ.JOBS", &key);
        args.extend([
            Bytes::copy_from_slice(cursor.as_bytes()),
            Bytes::from_static(b"COUNT"),
            Bytes::from(count.to_string()),
            Bytes::from_static(b"STATE"),
            Bytes::copy_from_slice(state.as_bytes()),
        ]);
    } else {
        let offset = cursor
            .parse::<u64>()
            .map_err(|_| ApiError::bad("invalid cursor"))?;
        if browse_type == "list" && offset > 1_000_000_000 {
            return Err(ApiError::bad("cursor exceeds browsing limit"));
        }
        args = match browse_type {
            "string" => key_args("GET", &key),
            "list" => {
                let mut a = key_args("LRANGE", &key);
                a.extend([
                    Bytes::from(offset.to_string()),
                    Bytes::from((offset + count as u64 - 1).to_string()),
                ]);
                a
            }
            "set" | "hash" | "zset" => {
                let cmd = match browse_type {
                    "set" => "SSCAN",
                    "hash" => "HSCAN",
                    _ => "ZSCAN",
                };
                let mut a = key_args(cmd, &key);
                a.extend([
                    Bytes::from(cursor.clone()),
                    Bytes::from_static(b"COUNT"),
                    Bytes::from(count.to_string()),
                ]);
                a
            }
            "none" => {
                return Ok(
                    json!({"key":dto::bytes(&key,true,true),"type":"none","ttl_ms":-2,"placement":"absent","result":{"type":"null"},"cursor":null}),
                );
            }
            _ => return Err(ApiError::bad("key type is not inspectable")),
        };
    }
    let result = ensure(
        app.authorized(
            p,
            "reveal",
            Some(STANDARD.encode(&key)),
            args,
            reveal,
            false,
        )
        .await?
        .response,
    )?;
    if native {
        result_cursor = dto::field(&result, "cursor")
            .and_then(dto::text)
            .and_then(|b| std::str::from_utf8(b).ok())
            .filter(|s| *s != "0")
            .map(str::to_owned);
    } else if matches!(ty.as_str(), "set" | "hash" | "zset") {
        let (c, _) = scan_page(&result)?;
        if c != "0" {
            result_cursor = Some(c);
        }
    } else if ty == "list" && dto::list(&result).is_some_and(|v| v.len() == count) {
        let offset = cursor
            .parse::<u64>()
            .map_err(|_| ApiError::bad("invalid cursor"))?;
        result_cursor = Some((offset + count as u64).to_string());
    }
    let converted = if native {
        safe_meta(&result, reveal, app.config.allow_sensitive_reveal)?
    } else {
        dto::response(&result, reveal, app.config.allow_sensitive_reveal)?
    };
    if queue_only {
        let stats = if native {
            ensure(app.command(key_args("FQ.STATS", &key)).await?)?
        } else {
            sidekiq_stats(app, &key).await?
        };
        return Ok(
            json!({"key":dto::bytes(&key,true,true),"kind":if native{"native"}else{"sidekiq"},"stats":safe_meta(&stats,false,false)?,"result":converted,"cursor":result_cursor,"state":if native{state.as_str()}else{"queued"}}),
        );
    }
    let placement = if app.storage.is_some() && matches!(ty.as_str(), "list" | "ffkv-queue") {
        "mixed/metadata-resident"
    } else {
        "memory"
    };
    Ok(
        json!({"key":dto::bytes(&key,true,true),"type":ty,"ttl_ms":dto::int(&ttl).map(dto::integer),"placement":placement,"result":converted,"cursor":result_cursor}),
    )
}
pub(super) async fn key(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Query(q): Query<KeyQuery>,
) -> Result<Response> {
    json_response(inspect(&app, &p, q, false).await?)
}
pub(super) async fn queue(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Query(q): Query<KeyQuery>,
) -> Result<Response> {
    json_response(inspect(&app, &p, q, true).await?)
}

async fn sidekiq_stats(app: &App, key: &Bytes) -> Result<Resp> {
    let mut stats = vec![(
        Resp::Simple("queue_length"),
        ensure(app.command(key_args("LLEN", key)).await?)?,
    )];
    for (field, key) in [
        ("scheduled", "schedule"),
        ("retry", "retry"),
        ("dead", "dead"),
    ] {
        stats.push((
            Resp::Simple(field),
            ensure(app.call(&["ZCARD", key]).await?)?,
        ));
    }
    let processes = ensure(
        app.call(&["SSCAN", "processes", "0", "COUNT", "20"])
            .await?,
    )?;
    let (cursor, members) = scan_page(&processes)?;
    let mut heartbeats = Vec::new();
    for member in members.iter().take(20) {
        let identity = Bytes::copy_from_slice(
            dto::text(member)
                .ok_or_else(|| ApiError::unavailable("invalid Sidekiq process identity"))?,
        );
        let mut args = key_args("HMGET", &identity);
        args.extend([
            Bytes::from_static(b"beat"),
            Bytes::from_static(b"busy"),
            Bytes::from_static(b"quiet"),
            Bytes::from_static(b"rss"),
        ]);
        let response = ensure(app.command(args).await?)?;
        let values = dto::list(&response)
            .ok_or_else(|| ApiError::unavailable("invalid Sidekiq heartbeat response"))?;
        let mut fields = vec![(Resp::Simple("identity"), Resp::Bulk(Some(identity)))];
        for (name, value) in ["beat", "busy", "quiet", "rss"].iter().zip(values) {
            let value = match value {
                Resp::Bulk(value) => Resp::Bulk(value.clone()),
                _ => return Err(ApiError::unavailable("invalid Sidekiq heartbeat response")),
            };
            fields.push((Resp::Simple(name), value));
        }
        heartbeats.push(Resp::Map(fields));
    }
    stats.push((
        Resp::Simple("process_heartbeats"),
        Resp::Array(Some(heartbeats)),
    ));
    stats.push((
        Resp::Simple("process_scan_cursor"),
        Resp::Bulk(Some(Bytes::from(cursor))),
    ));
    Ok(Resp::Map(stats))
}
pub(super) async fn queues(State(app): State<App>, Query(page): Query<Page>) -> Result<Response> {
    let count = page.count()?;
    let raw = page.cursor.as_deref().unwrap_or("0");
    let initial = raw == "0";
    let parts = if initial {
        vec!["0", "0"]
    } else {
        raw.split(':').collect::<Vec<_>>()
    };
    if parts.len() != 2
        || parts
            .iter()
            .any(|s| s.len() > 20 || s.parse::<u64>().is_err())
    {
        return Err(ApiError::bad("invalid queue cursor"));
    }
    let mut next = ["0".to_string(), "0".to_string()];
    let mut queues = Vec::new();
    if initial || parts[0] != "0" {
        let result = ensure(
            app.call(&["SCAN", parts[0], "COUNT", &count.to_string()])
                .await?,
        )?;
        let (c, values) = scan_page(&result)?;
        next[0] = c;
        for value in values {
            let key = Bytes::copy_from_slice(
                dto::text(value).ok_or_else(|| ApiError::unavailable("invalid scan key"))?,
            );
            let ty = ensure(app.command(key_args("TYPE", &key)).await?)?;
            if dto::text(&ty) != Some(b"ffkv-queue".as_slice()) {
                continue;
            }
            let stats = ensure(app.command(key_args("FQ.STATS", &key)).await?)?;
            queues.push(json!({"key":dto::bytes(&key,true,true),"kind":"native","stats":safe_meta(&stats,false,false)?}));
        }
    }
    if initial || parts[1] != "0" {
        let result = ensure(
            app.call(&["SSCAN", "queues", parts[1], "COUNT", &count.to_string()])
                .await?,
        )?;
        let (c, values) = scan_page(&result)?;
        next[1] = c;
        for value in values {
            let name =
                dto::text(value).ok_or_else(|| ApiError::unavailable("invalid queue name"))?;
            let mut key = b"queue:".to_vec();
            key.extend_from_slice(name);
            let key = Bytes::from(key);
            let stats = ensure(app.command(key_args("LLEN", &key)).await?)?;
            queues.push(json!({"key":dto::bytes(&key,true,true),"kind":"sidekiq","stats":dto::response(&stats,true,true)?}));
        }
    }
    let cursor = if next.iter().all(|s| s == "0") {
        "0".into()
    } else {
        format!("{}:{}", next[0], next[1])
    };
    json_response(json!({"cursor":cursor,"queues":queues}))
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Console {
    args_b64: Vec<String>,
    reveal: Option<bool>,
    confirmed: Option<bool>,
}
fn denied(name: &str) -> bool {
    matches!(
        name,
        "AUTH"
            | "HELLO"
            | "QUIT"
            | "SELECT"
            | "CLIENT"
            | "MULTI"
            | "EXEC"
            | "DISCARD"
            | "WATCH"
            | "UNWATCH"
            | "SUBSCRIBE"
            | "PSUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "MONITOR"
            | "SYNC"
            | "PSYNC"
            | "REPLCONF"
            | "SHUTDOWN"
            | "CONFIG"
            | "DEBUG"
            | "ACL"
            | "BLPOP"
            | "BRPOP"
            | "BRPOPLPUSH"
            | "BLMOVE"
            | "BZPOPMIN"
            | "BZPOPMAX"
            | "XREAD"
            | "XREADGROUP"
            | "TS.READ"
            | "WAIT"
            | "KEYS"
    )
}
pub(super) async fn console(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Json(body): Json<Console>,
) -> Result<Response> {
    if body.args_b64.is_empty() || body.args_b64.len() > 1024 {
        return Err(ApiError::bad("invalid argument count"));
    }
    let mut total = 0usize;
    let mut args = Vec::with_capacity(body.args_b64.len());
    for s in body.args_b64 {
        total = total
            .checked_add(s.len())
            .ok_or_else(|| ApiError::bad("command too large"))?;
        if total > 12 * 1024 * 1024 {
            return Err(ApiError::bad("command too large"));
        }
        args.push(Bytes::from(
            STANDARD
                .decode(s)
                .map_err(|_| ApiError::bad("invalid argument base64"))?,
        ));
    }
    validate(&args)?;
    let name = std::str::from_utf8(&args[0])
        .map_err(|_| ApiError::bad("invalid command name"))?
        .to_ascii_uppercase();
    if denied(&name) {
        return Err(ApiError::forbidden());
    }
    let descriptor = app
        .catalog
        .get(&name)
        .ok_or_else(|| ApiError::bad("command not in server catalog"))?;
    let readonly = descriptor.readonly();
    if !readonly {
        admin(&p)?;
        if body.confirmed != Some(true) {
            return Err(ApiError::bad("explicit confirmation required"));
        }
    }
    let reveal = body.reveal.unwrap_or(false);
    let begin = Instant::now();
    let execution = app
        .authorized(&p, &name, None, args, reveal, !readonly)
        .await?;
    let result = if matches!(name.as_str(), "FQ.GET" | "FQ.JOBS" | "FQ.STATS") {
        safe_meta(
            &execution.response,
            reveal,
            app.config.allow_sensitive_reveal,
        )
    } else {
        dto::response(
            &execution.response,
            reveal,
            app.config.allow_sensitive_reveal,
        )
    };
    let value=result.map(|result|json!({"result":result,"elapsed_us":begin.elapsed().as_micros().min(u64::MAX as u128)as u64}));
    app.deliver(execution, value).await
}
fn compatibility_baseline(name: &str) -> &'static str {
    let starts = |prefix: &[u8]| {
        name.as_bytes()
            .get(..prefix.len())
            .is_some_and(|name| name.eq_ignore_ascii_case(prefix))
    };
    if starts(b"fq.") || starts(b"ffkv.") {
        "ffkv extension"
    } else if starts(b"ar") {
        "Redis 8.8+ sparse-array API"
    } else if starts(b"json.") {
        "RedisJSON API"
    } else if starts(b"ts.") {
        "RedisTimeSeries API, including Redis 8.10 commands"
    } else if [b"bf.".as_slice(), b"cf.", b"cms.", b"topk.", b"tdigest."]
        .iter()
        .any(|prefix| starts(prefix))
    {
        "RedisBloom datatype APIs"
    } else if starts(b"v") {
        "Redis 8.x vector-set API"
    } else if starts(b"x") {
        "Redis 7.2 Streams with Redis 8.x extensions"
    } else {
        "Redis 7.2 subset"
    }
}

pub(super) async fn commands(State(app): State<App>) -> Result<Response> {
    let mut commands=app.catalog.values().map(|c|json!({"name":c.name,"arity":c.arity,"flags":c.flags,"readonly":c.readonly(),"syntax":syntax(c),"baseline":compatibility_baseline(&c.name),"limits":if denied(&c.name.to_ascii_uppercase()){vec!["Unavailable in management console"]}else{vec!["8 MiB request", "1024 arguments", "8 MiB management response", "10 second bus deadline"]}})).collect::<Vec<_>>();
    commands.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
    json_response(
        json!({"commands":commands,"clients":[{"name":"Sidekiq","version":"8.0.7"},{"name":"redis-client","version":"0.26.2"},{"name":"Ruby","version":"3.4.6"},{"name":"connection_pool","version":"2.5.4"},{"name":"Rack","version":"3.2.6"},{"name":"rack-session","version":"2.1.2"},{"name":"Puma","version":"8.0.0"}]}),
    )
}
fn syntax(c: &Command) -> String {
    let name = c.name.to_ascii_uppercase();
    match name.as_str(){"GET"=>"GET key".into(),"SET"=>"SET key value [NX|XX] [EX seconds|PX milliseconds|EXAT seconds|PXAT milliseconds|KEEPTTL] [GET]".into(),"SCAN"=>"SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]".into(),"FQ.JOBS"=>"FQ.JOBS key cursor [COUNT count] [STATE ready|scheduled|running|dead]".into(),"FQ.GET"=>"FQ.GET key job_id".into(),"FQ.ACK"=>"FQ.ACK key receipt".into(),"FQ.NACK"=>"FQ.NACK key receipt [DELAY milliseconds] [ERROR bytes]".into(),"FQ.EXTEND"=>"FQ.EXTEND key receipt visibility_ms".into(),"FQ.RETRY"=>"FQ.RETRY key job_id [DELAY milliseconds]".into(),"FQ.CANCEL"=>"FQ.CANCEL key job_id".into(),_=>format!("{} ({} {} including command; validated by server)",name,c.arity.unsigned_abs(),if c.arity<0{"minimum arguments"}else{"arguments"})}
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueueAction {
    key_b64: String,
    action: String,
    receipt_b64: Option<String>,
    job_id_b64: Option<String>,
    visibility_ms: Option<u64>,
    delay_ms: Option<u64>,
    confirmed: Option<bool>,
}
pub(super) async fn queue_action(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Json(body): Json<QueueAction>,
) -> Result<Response> {
    admin(&p)?;
    if body.confirmed != Some(true) {
        return Err(ApiError::bad("explicit confirmation required"));
    }
    let key = decode(&body.key_b64)?;
    let ty = ensure(app.command(key_args("TYPE", &key)).await?)?;
    if dto::text(&ty) != Some(b"ffkv-queue".as_slice()) {
        return Err(ApiError::bad("native queue required"));
    }
    let name = match body.action.as_str() {
        "ack" => "FQ.ACK",
        "nack" => "FQ.NACK",
        "extend" => "FQ.EXTEND",
        "retry" => "FQ.RETRY",
        "cancel" => "FQ.CANCEL",
        _ => return Err(ApiError::bad("unknown queue action")),
    };
    let mut args = key_args(name, &key);
    let target = if matches!(body.action.as_str(), "ack" | "nack" | "extend") {
        body.receipt_b64.as_deref()
    } else {
        body.job_id_b64.as_deref()
    }
    .ok_or_else(|| ApiError::bad("receipt or job ID required"))?;
    args.push(decode(target)?);
    if body.action == "extend" {
        let n = body
            .visibility_ms
            .filter(|n| *n > 0 && *n <= 30 * 86400000)
            .ok_or_else(|| ApiError::bad("valid visibility_ms required"))?;
        args.push(Bytes::from(n.to_string()));
    }
    if matches!(body.action.as_str(), "nack" | "retry")
        && let Some(n) = body.delay_ms
    {
        if n > 30 * 86400000 {
            return Err(ApiError::bad("delay exceeds limit"));
        }
        args.extend([Bytes::from_static(b"DELAY"), Bytes::from(n.to_string())]);
    }
    let execution = app
        .authorized(&p, name, Some(STANDARD.encode(&key)), args, false, true)
        .await?;
    let value =
        dto::response(&execution.response, false, false).map(|result| json!({"result":result}));
    app.deliver(execution, value).await
}
pub(super) async fn storage(State(app): State<App>) -> Result<Response> {
    let wal = ensure(app.call(&["FFKV.STATS"]).await?)?;
    let pressure = app.metrics.rss_pressure();
    json_response(
        json!({"mode":if app.storage.is_some(){"hybrid"}else{"memory"},"stats":storage_stats(&app),"wal":safe_meta(&wal,false,false)?,"limits":{"command_bytes":8388608,"arguments":1024,"page_count":200,"response_bytes":8388608,"max_rss_bytes_soft":pressure.max_rss_bytes_soft,"rss_pressured":pressure.rss_pressured}}),
    )
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct StorageAction {
    action: String,
    name: Option<String>,
    confirmed: Option<bool>,
}
pub(super) async fn storage_action(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Json(body): Json<StorageAction>,
) -> Result<Response> {
    admin(&p)?;
    if body.confirmed != Some(true) {
        return Err(ApiError::bad("explicit confirmation required"));
    }
    let name = match body.action.as_str() {
        "compact" => "FFKV.COMPACT",
        "checkpoint" => "FFKV.CHECKPOINT",
        "backup" => "FFKV.BACKUP",
        _ => return Err(ApiError::bad("unknown storage action")),
    };
    let mut args = vec![Bytes::from_static(match name {
        "FFKV.COMPACT" => b"FFKV.COMPACT",
        "FFKV.CHECKPOINT" => b"FFKV.CHECKPOINT",
        _ => b"FFKV.BACKUP",
    })];
    if name == "FFKV.BACKUP" {
        let value = body
            .name
            .filter(|n| {
                !n.is_empty()
                    && n.len() <= 128
                    && n != "."
                    && n != ".."
                    && n.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
            })
            .ok_or_else(|| ApiError::bad("safe backup name required"))?;
        args.push(Bytes::from(value));
    }
    let execution = app.authorized(&p, name, None, args, false, true).await?;
    let value =
        dto::response(&execution.response, true, false).map(|result| json!({"result":result}));
    app.deliver(execution, value).await
}
pub(super) async fn audit_page(
    State(app): State<App>,
    Extension(p): Extension<Principal>,
    Query(page): Query<Page>,
) -> Result<Response> {
    admin(&p)?;
    let count = page.count()?;
    let offset = page
        .numeric_cursor()?
        .parse()
        .map_err(|_| ApiError::bad("invalid audit cursor"))?;
    json_response(app.audit.page(offset, count).await?)
}
pub(super) async fn prometheus(State(app): State<App>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        app.metrics.prometheus(),
    )
        .into_response()
}
pub(super) async fn live(State(app): State<App>, upgrade: WebSocketUpgrade) -> Result<Response> {
    let permit = app
        .websockets
        .clone()
        .try_acquire_owned()
        .map_err(|_| ApiError::unavailable("live connection limit reached"))?;
    Ok(upgrade
        .max_frame_size(32768)
        .max_message_size(32768)
        .on_upgrade(move |socket| live_socket(app, socket, permit))
        .into_response())
}
async fn live_socket(app: App, mut socket: WebSocket, _permit: tokio::sync::OwnedSemaphorePermit) {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Login {
        token: String,
    }
    let first = tokio::time::timeout(Duration::from_secs(2), socket.next()).await;
    let token = match first {
        Ok(Some(Ok(Message::Text(text)))) => {
            serde_json::from_str::<Login>(&text).ok().map(|v| v.token)
        }
        _ => None,
    };
    let Some(token) = token else {
        let _ = socket.close().await;
        return;
    };
    if app.auth.authenticate(&token).await.is_err() {
        let _ = socket.close().await;
        return;
    }
    let mut tick = tokio::time::interval(Duration::from_secs(2));
    let expires = tokio::time::sleep(Duration::from_secs(300));
    tokio::pin!(expires);
    let mut frames = 0u32;
    loop {
        tokio::select! {
            _=&mut expires=>break,
            _=tick.tick()=>{
                frames=0;
                // Recheck JWT expiry and current role before every update.
                if app.auth.authenticate(&token).await.is_err(){break;}
                let Ok(mut value)=overview_value(&app).await else{break;};
                dto::js_safe(&mut value);
                let Ok(text)=serde_json::to_string(&value)else{break;};
                if text.len()>dto::MAX_RESPONSE{break;}
                if !matches!(tokio::time::timeout(Duration::from_secs(2),socket.send(Message::Text(text.into()))).await,Ok(Ok(()))){break;}
            },
            frame=socket.next()=>{
                frames+=1;
                if frames>8{break;}
                match frame{Some(Ok(Message::Ping(_)))|Some(Ok(Message::Pong(_)))=>{},_=>break}
            }
        }
    }
    let _ = socket.close().await;
}
pub(super) async fn static_file(State(app): State<App>, uri: Uri) -> Response {
    let result = static_response(&app, uri.path()).await;
    match result {
        Ok(response) => response,
        Err(error) => error.into_response(),
    }
}
async fn static_response(app: &App, path: &str) -> Result<Response> {
    if path.starts_with("/api") || path == "/metrics" {
        return Err(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "not found",
        });
    }
    let root = app
        .ui
        .as_ref()
        .ok_or(ApiError {
            status: StatusCode::NOT_FOUND,
            message: "console assets not configured",
        })?
        .clone();
    if path.len() > 2048
        || path.contains('%')
        || path.contains('\\')
        || path.chars().any(char::is_control)
    {
        return Err(ApiError::bad("invalid asset path"));
    }
    let relative = path.trim_start_matches('/').to_owned();
    if Path::new(&relative)
        .components()
        .any(|c| !matches!(c, Component::Normal(_)))
    {
        return Err(ApiError::bad("invalid asset path"));
    }
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        use std::os::unix::fs::OpenOptionsExt;
        let root = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
            .open(root)
            .map_err(|_| ApiError::forbidden())?;
        let selected = if relative.is_empty() {
            "index.html"
        } else {
            relative.as_str()
        };
        let (mut file, selected) = match open_asset(&root, selected) {
            Ok(file) => (file, selected),
            Err(error)
                if error.kind() == io::ErrorKind::NotFound
                    && Path::new(selected).extension().is_none() =>
            {
                (
                    open_asset(&root, "index.html").map_err(|_| ApiError {
                        status: StatusCode::NOT_FOUND,
                        message: "asset not found",
                    })?,
                    "index.html",
                )
            }
            Err(_) => {
                return Err(ApiError {
                    status: StatusCode::NOT_FOUND,
                    message: "asset not found",
                });
            }
        };
        let meta = file.metadata().map_err(|_| ApiError::forbidden())?;
        if !meta.is_file() || meta.len() > 8 * 1024 * 1024 {
            return Err(ApiError::bad("asset exceeds limit"));
        }
        let mut body = Vec::new();
        file.by_ref()
            .take(8 * 1024 * 1024 + 1)
            .read_to_end(&mut body)
            .map_err(|_| ApiError::unavailable("asset read failed"))?;
        if body.len() > 8 * 1024 * 1024 {
            return Err(ApiError::bad("asset exceeds limit"));
        }
        let mime = mime_guess::from_path(selected)
            .first_or_octet_stream()
            .to_string();
        Ok(([(header::CONTENT_TYPE, mime)], body).into_response())
    })
    .await
    .map_err(|_| ApiError::unavailable("asset read failed"))?
}

// Resolve every component relative to an already-open directory. NOFOLLOW on
// every open prevents directory-symlink races, not merely lexical traversal.
fn open_asset(root: &std::fs::File, path: &str) -> io::Result<std::fs::File> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let mut directory = root.try_clone()?;
    let mut components = Path::new(path).components().peekable();
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid asset component",
            ));
        };
        use std::os::unix::ffi::OsStrExt;
        let name = std::ffi::CString::new(name.as_bytes())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid asset component"))?;
        let flags = libc::O_RDONLY
            | libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | if components.peek().is_some() {
                libc::O_DIRECTORY
            } else {
                0
            };
        // SAFETY: name is NUL terminated; directory owns a live FD; the new
        // descriptor is immediately assigned exactly one File owner.
        let fd = unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), flags) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        directory = unsafe { std::fs::File::from_raw_fd(fd) };
    }
    Ok(directory)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn permission_and_input_bounds_are_explicit() {
        let viewer = Principal {
            name: "viewer".into(),
            role: Role::Viewer,
            can_reveal: false,
        };
        let administrator = Principal {
            name: "admin".into(),
            role: Role::Admin,
            can_reveal: true,
        };
        assert!(admin(&viewer).is_err());
        assert!(admin(&administrator).is_ok());
        assert!(decode("not-base64!").is_err());
        assert!(decode(&STANDARD.encode(vec![0; 4097])).is_err());
        assert!(
            Page {
                count: Some(201),
                ..Page::default()
            }
            .count()
            .is_err()
        );
        assert!(
            Page {
                cursor: Some("-1".into()),
                ..Page::default()
            }
            .numeric_cursor()
            .is_err()
        );
        for command in ["AUTH", "MULTI", "SELECT", "CLIENT", "BLPOP", "KEYS"] {
            assert!(denied(command));
        }
    }
    #[test]
    fn native_metadata_never_exposes_default_payloads() {
        let response = Resp::Map(vec![
            (
                Resp::Simple("job_id"),
                Resp::Bulk(Some(Bytes::from_static(b"real-job-id"))),
            ),
            (
                Resp::Simple("payload"),
                Resp::Bulk(Some(Bytes::from_static(b"private payload"))),
            ),
            (
                Resp::Simple("error"),
                Resp::Bulk(Some(Bytes::from_static(b"private error"))),
            ),
        ]);
        let encoded = serde_json::to_string(&safe_meta(&response, false, false).unwrap()).unwrap();
        assert!(encoded.contains("real-job-id"));
        assert!(!encoded.contains("private payload"));
        assert!(!encoded.contains("private error"));
    }
    #[test]
    fn asset_walk_refuses_directory_and_file_symlinks() {
        let root = std::env::temp_dir().join(format!(
            "ffkv-assets-test-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        std::fs::create_dir(&root).unwrap();
        std::fs::write(root.join("index.html"), "console").unwrap();
        std::os::unix::fs::symlink("index.html", root.join("link.html")).unwrap();
        std::os::unix::fs::symlink(&root, root.join("linked-dir")).unwrap();
        let directory = std::fs::File::open(&root).unwrap();
        assert!(
            open_asset(&directory, "index.html")
                .unwrap()
                .metadata()
                .unwrap()
                .is_file()
        );
        assert!(open_asset(&directory, "link.html").is_err());
        assert!(open_asset(&directory, "linked-dir/index.html").is_err());
        assert!(open_asset(&directory, "../index.html").is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
