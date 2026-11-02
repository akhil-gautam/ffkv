use super::{ToolDefinition, config::Config};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use bytes::Bytes;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
    process::{ExitStatus, Stdio},
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    process::{Child, Command},
    sync::watch,
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

pub struct Processes {
    config: Arc<Config>,
    binary: PathBuf,
    registry: Mutex<Registry>,
    shutdown: CancellationToken,
}

#[derive(Default)]
struct Registry {
    children: BTreeMap<String, Arc<OwnedProcess>>,
    next_id: u64,
}

struct OwnedProcess {
    id: String,
    pid: u32,
    stop: CancellationToken,
    done: watch::Receiver<bool>,
    exit: Mutex<Option<Result<ExitStatus, String>>>,
    logs: Mutex<LogBuffer>,
}

struct LogChunk {
    sequence: u64,
    stream: &'static str,
    bytes: Bytes,
}

struct LogBuffer {
    chunks: VecDeque<LogChunk>,
    capacity: usize,
    len: usize,
    next_sequence: u64,
    dropped_bytes: u64,
    capture_errors: Vec<&'static str>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Arguments {
    action: Action,
    process: Option<String>,
    args: Option<Vec<String>>,
    source: Option<String>,
    data_dir: Option<String>,
    timeout_ms: Option<u64>,
    since_sequence: Option<u64>,
    max_bytes: Option<usize>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Action {
    Help,
    Start,
    Status,
    Logs,
    Stop,
    Restore,
}

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(|error| error.into_inner())
}

pub fn definitions() -> Vec<ToolDefinition> {
    vec![ToolDefinition {
        name: "ffkv_process",
        description: "Privileged, opt-in control of the operator-configured ffkv executable only. No shell. Start accepts every ffkv CLI argument, returns an owned handle, and does not promise listener readiness. Help runs real --help (args:[\"restore\"] selects restore help). Restore runs offline into a NEW data directory; ffkv validates the backup and refuses existing destinations. Only owned handles can be stopped. Cancellation of help/restore kills and reaps that child; restore cancellation may leave filesystem effects and must not be blindly retried. EOF/shutdown kills and reaps all owned children. Logs are bounded, losslessly encoded and paged with byte-offset sequence cursors and explicit overflow counters. Process privileges are NOT confined by file_root. Exited handles can be evicted when capacity is needed.",
        schema: json!({
            "type": "object",
            "properties": {
                "action": {"type":"string", "enum":["help","start","status","logs","stop","restore"]},
                "process": {"type":"string", "description":"Owned opaque handle, required for logs/stop; omit for status list. Never a PID or port."},
                "args": {"type":"array", "items":{"type":"string"}, "description":"For start: ffkv argv without executable, passed verbatim without a shell; every startup option is available. For help: [] or [\"restore\"] only. No argv or credentials are echoed."},
                "source": {"type":"string", "description":"Required for restore: backup source directory."},
                "data_dir": {"type":"string", "description":"Required for restore: NEW destination directory. Existing directories, files and symlinks are refused by ffkv."},
                "timeout_ms": {"type":"integer", "minimum":1, "description":"Operation deadline, capped by the configured max_timeout_ms. Start acknowledges spawning, not readiness. A stop timeout does not undo the stop request."},
                "since_sequence": {"type":"integer", "minimum":0, "description":"Logs only: inclusive byte-offset cursor, initially 0; pass next_sequence to continue. missed_bytes counts overwritten bytes since this cursor."},
                "max_bytes": {"type":"integer", "minimum":1, "description":"Logs only: page byte limit, at most configured max_log_bytes; default is that capacity. Entries have sequence, stream, and either text or base64. dropped_bytes is cumulative overwritten data."}
            },
            "required":["action"],
            "additionalProperties":false
        }),
        read_only: false,
    }]
}

impl Processes {
    pub fn new(config: Arc<Config>) -> Result<Self, String> {
        let binary = match &config.process.binary {
            Some(path) => path.clone(),
            None => std::env::current_exe()
                .map_err(|_| "cannot locate the sibling ffkv executable".to_owned())?
                .with_file_name(if cfg!(windows) { "ffkv.exe" } else { "ffkv" }),
        };
        if config.process.enabled
            && (config.process.max_children == 0 || config.process.max_log_bytes == 0)
        {
            return Err("enabled process control requires positive child and log limits".into());
        }
        Ok(Self {
            config,
            binary,
            registry: Mutex::new(Registry::default()),
            shutdown: CancellationToken::new(),
        })
    }

    pub async fn call(
        &self,
        name: &str,
        args: Value,
        cancel: CancellationToken,
    ) -> Result<Value, String> {
        if name != "ffkv_process" {
            return Err("unknown process tool".into());
        }
        if !self.config.process.enabled {
            return Err("process control is disabled; the operator must enable --allow-process or process.enabled".into());
        }
        let args: Arguments = serde_json::from_value(args)
            .map_err(|_| "invalid ffkv_process arguments; consult the tool schema".to_owned())?;
        self.validate(&args)?;
        let timeout_ms = args
            .timeout_ms
            .unwrap_or(self.config.limits.operation_timeout_ms);
        if timeout_ms == 0 || timeout_ms > self.config.limits.max_timeout_ms {
            return Err("timeout_ms must be positive and no larger than max_timeout_ms".into());
        }
        if cancel.is_cancelled() || self.shutdown.is_cancelled() {
            return Err("process operation cancelled before execution".into());
        }
        let timeout = Duration::from_millis(timeout_ms);
        match args.action {
            Action::Start => {
                let child = self.spawn(args.args.as_deref().unwrap_or_default())?;
                // A cancelled call must never strand a newly spawned, unreturned handle.
                let guard = child.stop.clone().drop_guard();
                if cancel.is_cancelled() || self.shutdown.is_cancelled() {
                    child.stop.cancel();
                    child.finished().await;
                    return Err("process start cancelled; owned child stopped".into());
                }
                let result = child.status();
                guard.disarm();
                Ok(result)
            }
            Action::Help => {
                let mut argv = args.args.unwrap_or_default();
                argv.push("--help".into());
                self.run_to_exit(&argv, timeout, cancel, false).await
            }
            Action::Restore => {
                let argv = vec![
                    "restore".into(),
                    format!("--source={}", args.source.unwrap()),
                    format!("--data-dir={}", args.data_dir.unwrap()),
                ];
                // The real restore command atomically enforces new-directory semantics;
                // precreating the directory here would break that invariant.
                self.run_to_exit(&argv, timeout, cancel, true).await
            }
            Action::Status => {
                if let Some(id) = args.process {
                    Ok(self.lookup(&id)?.status())
                } else {
                    let children: Vec<_> =
                        lock(&self.registry).children.values().cloned().collect();
                    Ok(
                        json!({"processes":children.iter().map(|child| child.status()).collect::<Vec<_>>()}),
                    )
                }
            }
            Action::Logs => {
                let child = self.lookup(args.process.as_deref().unwrap())?;
                let mut result = lock(&child.logs).page(
                    args.since_sequence.unwrap_or(0),
                    args.max_bytes.unwrap_or(self.config.process.max_log_bytes),
                )?;
                result["process"] = json!(child.id);
                Ok(result)
            }
            Action::Stop => {
                let child = self.lookup(args.process.as_deref().unwrap())?;
                child.stop.cancel();
                tokio::select! {
                    biased;
                    _ = child.finished() => Ok(child.status()),
                    _ = cancel.cancelled() => Err("stop request cancelled; owned child termination continues in the background; query status".into()),
                    _ = tokio::time::sleep(timeout) => Err("stop deadline exceeded; owned child termination continues in the background; query status".into()),
                }
            }
        }
    }

    fn validate(&self, args: &Arguments) -> Result<(), String> {
        let valid = match args.action {
            Action::Help => {
                args.process.is_none()
                    && args.source.is_none()
                    && args.data_dir.is_none()
                    && args.args.as_ref().is_none_or(|argv| {
                        argv.is_empty() || (argv.len() == 1 && argv[0] == "restore")
                    })
            }
            Action::Start => {
                args.process.is_none() && args.source.is_none() && args.data_dir.is_none()
            }
            Action::Restore => {
                args.process.is_none()
                    && args.args.is_none()
                    && args.source.as_ref().is_some_and(|value| !value.is_empty())
                    && args
                        .data_dir
                        .as_ref()
                        .is_some_and(|value| !value.is_empty())
            }
            Action::Status => {
                args.args.is_none() && args.source.is_none() && args.data_dir.is_none()
            }
            Action::Logs | Action::Stop => {
                args.process.as_ref().is_some_and(|value| !value.is_empty())
                    && args.args.is_none()
                    && args.source.is_none()
                    && args.data_dir.is_none()
            }
        };
        if !valid {
            return Err("arguments do not match the requested process action".into());
        }
        if !matches!(args.action, Action::Logs)
            && (args.since_sequence.is_some() || args.max_bytes.is_some())
        {
            return Err("since_sequence and max_bytes are only accepted for logs".into());
        }
        if args
            .max_bytes
            .is_some_and(|limit| limit == 0 || limit > self.config.process.max_log_bytes)
        {
            return Err("max_bytes must be positive and no larger than max_log_bytes".into());
        }
        Ok(())
    }

    fn lookup(&self, id: &str) -> Result<Arc<OwnedProcess>, String> {
        lock(&self.registry)
            .children
            .get(id)
            .cloned()
            .ok_or_else(|| "unknown owned process handle".into())
    }

    fn spawn(&self, argv: &[String]) -> Result<Arc<OwnedProcess>, String> {
        let mut registry = lock(&self.registry);
        if self.shutdown.is_cancelled() {
            return Err("process controller is shutting down".into());
        }
        if registry.children.len() >= self.config.process.max_children {
            let expired = registry
                .children
                .iter()
                .find(|(_, child)| *child.done.borrow())
                .map(|(id, _)| id.clone());
            if let Some(id) = expired {
                registry.children.remove(&id);
            } else {
                return Err("owned process capacity reached; stop an owned child first".into());
            }
        }
        let mut command = Command::new(&self.binary);
        command
            .args(argv)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = command.spawn().map_err(|error| {
            format!(
                "cannot spawn the configured ffkv executable ({:?})",
                error.kind()
            )
        })?;
        let pid = child
            .id()
            .ok_or("spawned child has no process identifier")?;
        let stdout = child
            .stdout
            .take()
            .ok_or("spawned child stdout is unavailable")?;
        let stderr = child
            .stderr
            .take()
            .ok_or("spawned child stderr is unavailable")?;
        registry.next_id += 1;
        let id = format!("process-{}", registry.next_id);
        let (finished, done) = watch::channel(false);
        let entry = Arc::new(OwnedProcess {
            id: id.clone(),
            pid,
            stop: self.shutdown.child_token(),
            done,
            exit: Mutex::new(None),
            logs: Mutex::new(LogBuffer::new(self.config.process.max_log_bytes)),
        });
        registry.children.insert(id, entry.clone());
        let owner = entry.clone();
        tokio::spawn(async move {
            let stdout = tokio::spawn(capture(stdout, owner.clone(), "stdout"));
            let stderr = tokio::spawn(capture(stderr, owner.clone(), "stderr"));
            supervise(&mut child, &owner).await;
            // Do not let an inherited pipe held by another process block MCP teardown.
            let (out_complete, err_complete) =
                tokio::join!(finish_capture(stdout), finish_capture(stderr));
            if !out_complete || !err_complete {
                lock(&owner.logs)
                    .capture_errors
                    .push("pipe capture incomplete after child exit");
            }
            finished.send_replace(true);
        });
        Ok(entry)
    }

    async fn run_to_exit(
        &self,
        argv: &[String],
        timeout: Duration,
        cancel: CancellationToken,
        restore: bool,
    ) -> Result<Value, String> {
        let child = self.spawn(argv)?;
        // This also handles an MCP service dropping the request future outright.
        let guard = child.stop.clone().drop_guard();
        let interrupted = tokio::select! {
            biased;
            _ = cancel.cancelled() => Some("cancelled"),
            _ = self.shutdown.cancelled() => Some("interrupted by shutdown"),
            _ = child.finished() => None,
            _ = tokio::time::sleep(timeout) => Some("timed out"),
        };
        if let Some(reason) = interrupted {
            child.stop.cancel();
            child.finished().await;
            let effects = if restore {
                "; restore filesystem outcome is unknown: inspect the destination before retrying"
            } else {
                ""
            };
            return Err(format!(
                "process operation {reason}; owned child stopped{effects}"
            ));
        }
        guard.disarm();
        let mut result = child.status();
        result["outcome"] = json!(if result["success"] == true {
            "ok"
        } else {
            "error"
        });
        result["logs"] = lock(&child.logs).page(0, self.config.process.max_log_bytes)?;
        Ok(result)
    }

    pub async fn shutdown(&self) {
        // Lock ordering pairs this with spawn: no child can escape the shutdown snapshot.
        let children: Vec<_> = {
            let registry = lock(&self.registry);
            self.shutdown.cancel();
            registry.children.values().cloned().collect()
        };
        for child in children {
            child.finished().await;
        }
    }
}

impl Drop for Processes {
    fn drop(&mut self) {
        // The supervisors own the Child values and perform asynchronous kill + wait.
        self.shutdown.cancel();
    }
}

impl OwnedProcess {
    async fn finished(&self) {
        let mut done = self.done.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }

    fn status(&self) -> Value {
        let exit = lock(&self.exit);
        match exit.as_ref() {
            None => {
                json!({"process":self.id,"pid":self.pid,"state":"running","exit_code":null,"success":null})
            }
            Some(Ok(status)) => json!({
                "process":self.id,"pid":self.pid,"state":"exited",
                "exit_code":status.code(),"success":status.success(),
            }),
            Some(Err(error)) => json!({
                "process":self.id,"pid":self.pid,"state":"wait_error",
                "exit_code":null,"success":false,"outcome":"error","error":error
            }),
        }
    }
}

async fn supervise(child: &mut Child, owner: &OwnedProcess) {
    let result = tokio::select! {
        status = child.wait() => status,
        _ = owner.stop.cancelled() => {
            // Child is an owned OS handle; never signal a caller-supplied PID.
            let _ = child.start_kill();
            child.wait().await
        }
    };
    *lock(&owner.exit) = Some(result.map_err(|error| {
        format!(
            "failed to reap owned child ({:?}); termination outcome unknown",
            error.kind()
        )
    }));
}

async fn capture<R: AsyncRead + Unpin>(
    mut pipe: R,
    owner: Arc<OwnedProcess>,
    stream: &'static str,
) {
    let mut buffer = [0u8; 8192];
    loop {
        match pipe.read(&mut buffer).await {
            Ok(0) => break,
            Ok(count) => lock(&owner.logs).append(stream, &buffer[..count]),
            Err(_) => {
                lock(&owner.logs)
                    .capture_errors
                    .push(if stream == "stdout" {
                        "stdout capture failed; unread bytes are unknown"
                    } else {
                        "stderr capture failed; unread bytes are unknown"
                    });
                break;
            }
        }
    }
}

async fn finish_capture(mut task: JoinHandle<()>) -> bool {
    match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
        Ok(result) => result.is_ok(),
        Err(_) => {
            task.abort();
            let _ = task.await;
            false
        }
    }
}

impl LogBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            chunks: VecDeque::new(),
            capacity,
            len: 0,
            next_sequence: 0,
            dropped_bytes: 0,
            capture_errors: Vec::new(),
        }
    }

    fn append(&mut self, stream: &'static str, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        let sequence = self.next_sequence;
        self.next_sequence += bytes.len() as u64;
        let keep = bytes.len().min(self.capacity);
        let mut remove = self.len.saturating_add(keep).saturating_sub(self.capacity);
        self.dropped_bytes += (bytes.len() - keep + remove) as u64;
        while remove > 0 {
            let front = self
                .chunks
                .front_mut()
                .expect("log byte count matches chunks");
            let count = remove.min(front.bytes.len());
            if count == front.bytes.len() {
                self.chunks.pop_front();
            } else {
                front.bytes = front.bytes.slice(count..);
                front.sequence += count as u64;
            }
            remove -= count;
            self.len -= count;
        }
        if keep > 0 {
            self.chunks.push_back(LogChunk {
                sequence: sequence + (bytes.len() - keep) as u64,
                stream,
                bytes: Bytes::copy_from_slice(&bytes[bytes.len() - keep..]),
            });
            self.len += keep;
        }
    }

    fn page(&self, since: u64, max_bytes: usize) -> Result<Value, String> {
        if since > self.next_sequence {
            return Err("since_sequence is beyond the current log cursor".into());
        }
        let first_sequence = self
            .chunks
            .front()
            .map_or(self.next_sequence, |chunk| chunk.sequence);
        let mut next = since.max(first_sequence);
        let mut remaining = max_bytes;
        let mut entries = Vec::new();
        for chunk in &self.chunks {
            if remaining == 0 {
                break;
            }
            let end = chunk.sequence + chunk.bytes.len() as u64;
            if end <= next {
                continue;
            }
            let offset = next.saturating_sub(chunk.sequence) as usize;
            let count = remaining.min(chunk.bytes.len() - offset);
            let bytes = &chunk.bytes[offset..offset + count];
            let mut entry =
                json!({"sequence":chunk.sequence + offset as u64,"stream":chunk.stream});
            match std::str::from_utf8(bytes) {
                Ok(text) => entry["text"] = json!(text),
                Err(_) => entry["base64"] = json!(STANDARD.encode(bytes)),
            }
            entries.push(entry);
            next = chunk.sequence + (offset + count) as u64;
            remaining -= count;
        }
        Ok(json!({
            "entries":entries,
            "first_sequence":first_sequence,
            "next_sequence":next,
            "end_sequence":self.next_sequence,
            "has_more":next < self.next_sequence,
            "dropped_bytes":self.dropped_bytes,
            "missed_bytes":first_sequence.saturating_sub(since),
            "capture_errors":self.capture_errors
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_logs_account_for_overflow_and_resume_inside_chunks() {
        let mut logs = LogBuffer::new(6);
        logs.append("stdout", b"abcd");
        logs.append("stderr", b"EFGH");
        let page = logs.page(0, 3).unwrap();
        assert_eq!(page["first_sequence"], 2);
        assert_eq!(page["dropped_bytes"], 2);
        assert_eq!(page["missed_bytes"], 2);
        assert_eq!(page["next_sequence"], 5);
        assert_eq!(page["entries"][0]["text"], "cd");
        assert_eq!(page["entries"][1]["text"], "E");
        let page = logs.page(5, 3).unwrap();
        assert_eq!(page["entries"][0]["text"], "FGH");
        assert_eq!(page["missed_bytes"], 0);
        assert_eq!(page["has_more"], false);
    }

    #[test]
    fn oversized_chunks_keep_newest_bytes_and_report_every_drop() {
        let mut logs = LogBuffer::new(3);
        logs.append("stdout", b"ab");
        logs.append("stderr", b"123456");
        let page = logs.page(0, 3).unwrap();
        assert_eq!(page["entries"][0]["text"], "456");
        assert_eq!(page["entries"][0]["sequence"], 5);
        assert_eq!(page["dropped_bytes"], 5);
        assert_eq!(page["next_sequence"], 8);
        assert_eq!(logs.len, 3);
    }

    #[test]
    fn binary_and_split_utf8_logs_are_lossless() {
        let mut logs = LogBuffer::new(16);
        logs.append("stdout", &[0xff, 0, b'a']);
        let page = logs.page(0, 2).unwrap();
        assert_eq!(
            STANDARD
                .decode(page["entries"][0]["base64"].as_str().unwrap())
                .unwrap(),
            [0xff, 0]
        );
        assert_eq!(logs.page(2, 1).unwrap()["entries"][0]["text"], "a");
        assert!(logs.page(4, 1).is_err());
        assert_eq!(logs.page(3, 1).unwrap()["entries"], json!([]));
    }

    #[tokio::test]
    async fn process_control_requires_operator_opt_in() {
        let processes = Processes::new(Arc::new(Config::default())).unwrap();
        let result = processes
            .call(
                "ffkv_process",
                json!({"action":"help"}),
                CancellationToken::new(),
            )
            .await;
        assert!(result.is_err());
        processes.shutdown().await;
    }

    #[tokio::test]
    async fn caller_supplied_pids_and_executables_are_never_accepted() {
        let mut config = Config::default();
        config.process.enabled = true;
        let processes = Processes::new(Arc::new(config)).unwrap();
        for args in [
            json!({"action":"stop","process":std::process::id().to_string()}),
            json!({"action":"help","args":["some-other-executable"]}),
            json!({"action":"start","executable":"some-other-executable"}),
            json!({"action":"start","cwd":"/"}),
            json!({"action":"start","env":{"PATH":"/"}}),
            json!({"action":"restore","source":"backup","data_dir":"new","args":["--help"]}),
        ] {
            assert!(
                processes
                    .call("ffkv_process", args, CancellationToken::new())
                    .await
                    .is_err()
            );
        }
        assert!(lock(&processes.registry).children.is_empty());
        processes.shutdown().await;
    }
}
