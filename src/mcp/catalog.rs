use super::{ToolDefinition, config::Config, sessions::frame_json};
use crate::client::{Client, Frame};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct Cached {
    created: Instant,
    commands: Vec<Value>,
}
pub struct Catalog {
    config: Arc<Config>,
    caches: Mutex<BTreeMap<String, Cached>>,
}
impl Catalog {
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            config,
            caches: Mutex::new(BTreeMap::new()),
        }
    }
    pub async fn call(&self, args: Value, cancel: CancellationToken) -> Result<Value, String> {
        let args: Arguments = serde_json::from_value(args)
            .map_err(|e| format!("invalid discovery arguments: {e}"))?;
        if !(1..=200).contains(&args.limit) {
            return Err("catalog limit must be between 1 and 200".into());
        }
        let offset = args
            .cursor
            .as_deref()
            .unwrap_or("0")
            .parse::<usize>()
            .map_err(|_| "invalid catalog cursor")?;
        let (name, target) = self.config.target(args.target.as_deref())?;
        let prefix = args.prefix.unwrap_or_default().to_ascii_lowercase();
        // A separate connection never enters or disrupts an agent's MULTI, WATCH,
        // selected database, reply mode, subscription or authentication state.
        let mut caches = tokio::select! {
            _ = cancel.cancelled() => return Err("catalog request cancelled".into()),
            caches = self.caches.lock() => caches,
        };
        if args.refresh
            || caches
                .get(name)
                .is_none_or(|c| c.created.elapsed() >= Duration::from_secs(60))
        {
            let query = async {
                let mut client = Client::connect(target.client_config()?)
                    .await
                    .map_err(|_| "catalog connection failed")?;
                client
                    .write_command(&[Bytes::from_static(b"COMMAND")])
                    .await
                    .map_err(|_| "catalog request failed")?;
                let frame = client
                    .read_frame()
                    .await
                    .map_err(|_| "catalog response failed")?;
                if frame.has_error() {
                    return Err("server rejected COMMAND discovery; check target ACLs".to_string());
                }
                let Frame::Array(entries) = strip(&frame) else {
                    return Err("unexpected COMMAND response".to_string());
                };
                let mut commands = Vec::new();
                for entry in entries {
                    flatten(entry, &mut commands)?;
                }
                commands.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
                Ok(commands)
            };
            let commands = tokio::select! {
                _ = cancel.cancelled() => return Err("catalog request cancelled".into()),
                result = tokio::time::timeout(Duration::from_millis(self.config.limits.operation_timeout_ms), query) => result.map_err(|_| "catalog request timed out")??,
            };
            caches.insert(
                name.to_owned(),
                Cached {
                    created: Instant::now(),
                    commands,
                },
            );
        }
        let cached = &caches[name];
        let matches = || {
            cached
                .commands
                .iter()
                .filter(|c| c["name"].as_str().is_some_and(|n| n.starts_with(&prefix)))
        };
        let total = matches().count();
        if offset > total {
            return Err("catalog cursor exceeds matching commands; restart discovery".into());
        }
        let commands: Vec<_> = matches().skip(offset).take(args.limit).cloned().collect();
        let next = offset + commands.len();
        Ok(json!({"target":name,"commands":commands,"total":total,
            "next_cursor":(next < total).then(|| next.to_string()),"cache_ttl_ms":60000}))
    }
}
fn strip(mut frame: &Frame) -> &Frame {
    while let Frame::Attribute(_, value) = frame {
        frame = value;
    }
    frame
}
fn text(frame: &Frame) -> Option<&str> {
    match strip(frame) {
        Frame::Simple(b) | Frame::Bulk(b) => std::str::from_utf8(b).ok(),
        _ => None,
    }
}
fn flatten(entry: &Frame, out: &mut Vec<Value>) -> Result<(), String> {
    let Frame::Array(parts) = strip(entry) else {
        return Err("invalid command metadata".into());
    };
    let name = parts.first().and_then(text).ok_or("missing command name")?;
    let arity = match parts.get(1).map(strip) {
        Some(Frame::Integer(n)) => *n,
        _ => return Err("invalid command arity".into()),
    };
    let flags = match parts.get(2).map(strip) {
        Some(Frame::Array(flags) | Frame::Set(flags)) => {
            flags.iter().filter_map(text).collect::<Vec<_>>()
        }
        _ => Vec::new(),
    };
    // Preserve every metadata field without nesting the same subcommand tree
    // repeatedly: children are independent paginated rows.
    let metadata = parts.iter().take(9).map(frame_json).collect::<Vec<_>>();
    out.push(json!({"name":name,"arity":arity,"flags":flags,"metadata":metadata}));
    if let Some(Frame::Array(children)) = parts.get(9).map(strip) {
        for child in children {
            flatten(child, out)?;
        }
    }
    Ok(())
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    target: Option<String>,
    prefix: Option<String>,
    cursor: Option<String>,
    #[serde(default = "page_size")]
    limit: usize,
    #[serde(default)]
    refresh: bool,
}
fn page_size() -> usize {
    50
}
pub fn definitions() -> Vec<ToolDefinition> {
    vec![ToolDefinition {
        name: "ffkv_commands",
        read_only: true,
        description: "Discover the target's actual COMMAND registry, including subcommands and dynamically loaded modules. Paginated, case-insensitive prefix filtering. Cached for 60 seconds; refresh explicitly after module changes. Does not reuse or alter command sessions.",
        schema: json!({"type":"object","additionalProperties":false,"properties":{
            "target":{"type":"string"},"prefix":{"type":"string"},"cursor":{"type":"string"},
            "limit":{"type":"integer","minimum":1,"maximum":200},"refresh":{"type":"boolean"}
        }}),
    }]
}
