//! MCP sidecar: protocol work stays outside ffkv's datastore hot path.
mod artifacts;
mod catalog;
pub mod config;
mod management;
mod process;
mod sessions;

use artifacts::Artifacts;
use catalog::Catalog;
use config::Config;
use management::Management;
use process::Processes;
use rmcp::{
    ErrorData, ServerHandler, ServiceExt,
    model::*,
    service::{RequestContext, RoleServer},
};
use serde_json::{Value, json};
use sessions::Sessions;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

pub struct ToolDefinition {
    pub name: &'static str,
    pub description: &'static str,
    pub schema: Value,
    pub read_only: bool,
}

struct State {
    config: Arc<Config>,
    sessions: Sessions,
    management: Management,
    processes: Processes,
    catalog: Catalog,
    artifacts: Artifacts,
    tools: BTreeMap<String, Tool>,
    work: Semaphore,
}
#[derive(Clone)]
struct Server(Arc<State>);

impl Server {
    fn new(config: Config) -> Result<Self, String> {
        let config = Arc::new(config);
        let mut tools = BTreeMap::new();
        for definition in sessions::definitions()
            .into_iter()
            .chain(management::definitions())
            .chain(process::definitions())
            .chain(catalog::definitions())
            .chain(artifacts::definitions())
        {
            let Value::Object(schema) = definition.schema else {
                return Err("tool schema must be an object".into());
            };
            let tool = Tool::new(definition.name, definition.description, schema).with_annotations(
                ToolAnnotations::new()
                    .read_only(definition.read_only)
                    .destructive(!definition.read_only)
                    .idempotent(false)
                    .open_world(true),
            );
            if tools.insert(definition.name.into(), tool).is_some() {
                return Err("duplicate MCP tool name".into());
            }
        }
        Ok(Self(Arc::new(State {
            sessions: Sessions::new(config.clone())?,
            management: Management::new(config.clone())?,
            processes: Processes::new(config.clone())?,
            catalog: Catalog::new(config.clone()),
            artifacts: Artifacts::new(config.clone()),
            config,
            tools,
            work: Semaphore::new(128),
        })))
    }

    fn result(&self, value: Value, page: bool) -> CallToolResult {
        let failed = value
            .get("outcome")
            .and_then(Value::as_str)
            .is_some_and(|v| matches!(v, "error" | "unknown"));
        // Serialize once for legacy MCP text content and size accounting.
        let bytes = serde_json::to_vec(&value).expect("JSON value serialization");
        if page && bytes.len() > self.0.config.limits.inline_result_bytes {
            return match self.0.artifacts.store(bytes) {
                Ok(value) => {
                    let mut result = CallToolResult::structured(value);
                    result.is_error = Some(failed);
                    result
                }
                Err(error) => {
                    CallToolResult::structured_error(json!({"error":error,"outcome":"unknown"}))
                }
            };
        }
        let mut result = CallToolResult::success(vec![ContentBlock::text(
            String::from_utf8(bytes).expect("JSON is UTF-8"),
        )]);
        result.structured_content = Some(value);
        result.is_error = Some(failed);
        result
    }

    fn capabilities(&self) -> Value {
        json!({
            "name":"ffkv-mcp","version":env!("CARGO_PKG_VERSION"),"transport":"stdio",
            "default_target":self.0.config.default_target,
            "targets":self.0.config.targets.iter().map(|(name,t)| json!({"name":name,"protocol":t.protocol,"management":t.management_url.is_some()})).collect::<Vec<_>>(),
            "permissions":{"resp":"target credentials and ffkv ACLs; no MCP command allowlist",
                "process_control":self.0.config.process.enabled,"downloads":self.0.config.file_root.is_some()},
            "features":[
                {"surface":"all registered commands, data types, native jobs, Lua, functions, loaded modules, admin and cluster commands","tools":["ffkv_execute","ffkv_session"],"discovery":"ffkv_commands"},
                {"surface":"transactions, WATCH, selected databases and connection modes","tools":["ffkv_session","ffkv_execute"]},
                {"surface":"blocking operations, Pub/Sub, monitor, debugging and RESP3 push","tools":["ffkv_session"],"mode":"send/receive; manual sessions never return to execute mode"},
                {"surface":"native replication and arbitrary wire transfers","tools":["ffkv_session"],"mode":"raw"},
                {"surface":"management HTTP, metrics, audit, storage, queue administration and snapshot export","tools":["ffkv_management"]},
                {"surface":"authenticated management live WebSocket","tools":["ffkv_management_live"]},
                {"surface":"all ffkv startup options and offline restore; only owned child processes","tools":["ffkv_process"],"requires":"explicit process enablement"},
                {"surface":"lossless large result retrieval","tools":["ffkv_result"]}
            ],
            "limits":{"max_sessions":self.0.config.limits.max_sessions,
                "max_batch_commands":self.0.config.limits.max_batch_commands,"max_batch_bytes":self.0.config.limits.max_batch_bytes,
                "max_response_bytes":self.0.config.limits.max_response_bytes,"inline_result_bytes":self.0.config.limits.inline_result_bytes,
                "max_results_bytes":self.0.config.limits.max_results_bytes,"result_ttl_ms":self.0.config.limits.result_ttl_ms,
                "session_idle_ms":self.0.config.limits.session_idle_ms,"max_inflight_tools":128,"mcp_line_bytes":67108864},
            "outcomes":"No retries or reconnect/replay. A lost/cancelled write may have committed. A send acknowledgment is not a server reply.",
            "coverage":"Feature access map, not a claim that every server option has been independently conformance-tested. See live MCP tests and documentation."
        })
    }

    async fn shutdown(&self) {
        tokio::join!(
            self.0.sessions.shutdown(),
            self.0.management.shutdown(),
            self.0.processes.shutdown()
        );
    }
}

impl ServerHandler for Server {
    fn get_info(&self) -> ServerConfig {
        ServerConfig::new(ServerCapabilities::builder().enable_tools().enable_resources().build())
            .with_server_info(Implementation::new("ffkv-mcp", env!("CARGO_PKG_VERSION")))
            .with_instructions("Read ffkv://capabilities, then use ffkv_commands for live command metadata. ffkv_execute pipelines ordinary replies on persistent sessions. Use ffkv_session send/receive for blocking, subscriptions, reply modes and raw transfers. Explicit session handles preserve state. Never replay an operation with unknown outcome. Administration uses configured credentials; process control and downloads require operator opt-in.")
    }

    fn get_tool(&self, name: &str) -> Option<Tool> {
        self.0.tools.get(name).cloned()
    }

    async fn list_tools(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        if request.is_some_and(|r| r.cursor.is_some()) {
            return Err(ErrorData::invalid_params(
                "tools list has no continuation cursor",
                None,
            ));
        }
        Ok(
            ListToolsResult::with_all_items(self.0.tools.values().cloned().collect())
                .with_ttl_ms(60_000)
                .with_cache_scope(CacheScope::Private),
        )
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        let name = request.name.as_ref();
        if !self.0.tools.contains_key(name) {
            return Err(ErrorData::invalid_params("unknown ffkv MCP tool", None));
        }
        let Ok(_permit) = self.0.work.try_acquire() else {
            return Ok(CallToolResult::structured_error(
                json!({"error":"MCP concurrent operation limit reached; nothing dispatched"}),
            )
            .into());
        };
        if context.ct.is_cancelled() {
            return Ok(CallToolResult::structured_error(
                json!({"error":"request cancelled before dispatch"}),
            )
            .into());
        }
        let args = Value::Object(request.arguments.unwrap_or_default());
        let result = match name {
            "ffkv_execute" | "ffkv_session" => self.0.sessions.call(name, args, context.ct).await,
            "ffkv_management" | "ffkv_management_live" => {
                self.0.management.call(name, args, context.ct).await
            }
            "ffkv_process" => self.0.processes.call(name, args, context.ct).await,
            "ffkv_commands" => self.0.catalog.call(args, context.ct).await,
            "ffkv_result" => self.0.artifacts.call(args),
            _ => unreachable!("only registered tools are dispatched"),
        };
        Ok(match result {
            Ok(value) => self.result(value, name != "ffkv_result"),
            Err(error) => CallToolResult::structured_error(json!({"error":error})),
        }
        .into())
    }

    async fn list_resources(
        &self,
        request: Option<PaginatedRequestParams>,
        _: RequestContext<RoleServer>,
    ) -> Result<ListResourcesResult, ErrorData> {
        if request.is_some_and(|r| r.cursor.is_some()) {
            return Err(ErrorData::invalid_params(
                "resources list has no continuation cursor",
                None,
            ));
        }
        Ok(ListResourcesResult::with_all_items(vec![
            Resource::new("ffkv://capabilities", "ffkv MCP feature access and limits")
                .with_mime_type("application/json"),
            Resource::new(
                "ffkv://commands",
                "First page of live default-target commands; continue with ffkv_commands",
            )
            .with_mime_type("application/json"),
        ])
        .with_ttl_ms(60_000)
        .with_cache_scope(CacheScope::Private))
    }

    async fn read_resource(
        &self,
        request: ReadResourceRequestParams,
        context: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        let value = match request.uri.as_str() {
            "ffkv://capabilities" => self.capabilities(),
            "ffkv://commands" => self
                .0
                .catalog
                .call(json!({}), context.ct)
                .await
                .map_err(|e| ErrorData::internal_error(e, None))?,
            _ => return Err(ErrorData::resource_not_found("unknown ffkv resource", None)),
        };
        Ok(
            ReadResourceResult::new(vec![ResourceContents::text(value.to_string(), request.uri)])
                .into(),
        )
    }
}

pub async fn run(config: Config) -> Result<(), String> {
    let server = Server::new(config)?;
    let stop = CancellationToken::new();
    let signal_stop = stop.clone();
    let signals = tokio::spawn(async move {
        #[cfg(unix)]
        {
            if let Ok(mut term) =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
            } else {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
        #[cfg(not(unix))]
        {
            let _ = tokio::signal::ctrl_c().await;
        }
        signal_stop.cancel();
    });
    let cleanup_server = server.clone();
    let cleaner_stop = stop.clone();
    let cleaner = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tokio::select! {
                _ = cleaner_stop.cancelled() => break,
                _ = tick.tick() => cleanup_server.0.artifacts.prune(),
            }
        }
    });
    let transport = rmcp::transport::async_rw::AsyncRwTransport::<RoleServer, _, _>::new(
        tokio::io::stdin(),
        tokio::io::stdout(),
    )
    .with_max_line_length(64 * 1024 * 1024);
    let result = match server.clone().serve_with_ct(transport, stop.clone()).await {
        Ok(service) => service
            .waiting()
            .await
            .map(|_| ())
            .map_err(|_| "MCP service task failed".to_string()),
        Err(_) => Err("MCP protocol initialization failed".into()),
    };
    stop.cancel();
    server.shutdown().await;
    signals.abort();
    let _ = cleaner.await;
    result
}
