//! Lock-independent cancellation and a bounded, cooperative line debugger.
use super::*;
use std::collections::BTreeSet;
use std::sync::{Condvar, Mutex};
use std::time::{Duration, Instant};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DebugMode {
    #[default]
    No,
    Yes,
    Sync,
}
#[derive(Clone, Copy, Debug, Default)]
pub struct InvocationContext {
    pub readonly: bool,
    pub flags: FunctionFlags,
    /// YES debugging executes against a disposable view; SYNC commits normally.
    pub rollback: bool,
}
struct DebugEvent {
    owner: u64,
    bytes: usize,
    terminal: bool,
    value: Resp,
}
#[derive(Default)]
struct Debugger {
    enabled: bool,
    paused: bool,
    step: bool,
    abort: bool,
    breakpoints: BTreeSet<usize>,
    events: VecDeque<DebugEvent>,
    event_bytes: usize,
    source: Bytes,
    line: usize,
    started: Option<Instant>,
    command: Vec<Bytes>,
    function: bool,
    libraries: usize,
    functions: usize,
    owner: u64,
    abandoned: bool,
    disconnected: Option<Arc<std::sync::atomic::AtomicBool>>,
}
#[derive(Default)]
pub struct ExecutionControl {
    // Kind in low two bits, write flag 4, kill request 8. One CAS orders
    // cancellation against the first mutation: neither can race past the other.
    state: AtomicUsize,
    debugger: Mutex<Debugger>,
    wake: Condvar,
}
pub(super) struct ExecutionGuard(Arc<ExecutionControl>);
impl ExecutionGuard {
    /// Linearize completion against KILL. A kill that wins this race must become
    /// an error reply; a later kill observes NOTBUSY instead of false success.
    pub(super) fn finish(&self) -> bool {
        self.0.state.swap(0, Ordering::AcqRel) & 8 != 0
    }
}
impl Drop for ExecutionGuard {
    fn drop(&mut self) {
        self.0.state.store(0, Ordering::Release);
        let mut debug = self.0.debugger.lock().unwrap();
        // Reserve one terminal event outside the log quota so flooding the
        // console cannot strand a client waiting for the end-of-session marker.
        if debug.enabled && !debug.abandoned {
            enqueue(
                &mut debug,
                Resp::Array(Some(vec![bulk("<endsession>")])),
                12,
                true,
                true,
            );
        }
        debug.enabled = false;
        debug.paused = false;
        debug.command.clear();
        debug.source = Bytes::new();
        debug.started = None;
        self.0.wake.notify_all();
    }
}
fn discard_events(debug: &mut Debugger, owner: u64, terminal: bool) {
    let mut removed = 0;
    debug.events.retain(|event| {
        let keep = event.owner != owner || (!terminal && event.terminal);
        if !keep {
            removed += event.bytes;
        }
        keep
    });
    debug.event_bytes = debug.event_bytes.saturating_sub(removed);
}

fn enqueue(debug: &mut Debugger, value: Resp, bytes: usize, terminal: bool, priority: bool) {
    if debug.abandoned {
        return;
    }
    const MAX_EVENTS: usize = 256;
    // Keep a terminal-event reserve. Priority stops/terminals may reclaim the
    // oldest retained console output, but a new invocation never clears it.
    let limit = if terminal { MAX_EVENTS } else { MAX_EVENTS - 1 };
    if priority {
        while debug.events.len() >= limit || debug.event_bytes.saturating_add(bytes) > MAX_VALUE {
            let index = debug
                .events
                .iter()
                .position(|event| !event.terminal)
                .unwrap_or(0);
            let Some(event) = debug.events.remove(index) else {
                break;
            };
            debug.event_bytes = debug.event_bytes.saturating_sub(event.bytes);
        }
    }
    if debug.events.len() >= limit || debug.event_bytes.saturating_add(bytes) > MAX_VALUE {
        return;
    }
    debug.event_bytes += bytes;
    debug.events.push_back(DebugEvent {
        owner: debug.owner,
        bytes,
        terminal,
        value,
    });
}

fn push(debug: &mut Debugger, lines: Vec<String>) {
    if debug
        .events
        .iter()
        .filter(|event| event.owner == debug.owner && !event.terminal)
        .count()
        >= 128
    {
        return;
    }
    let mut bytes = 0;
    let lines: Vec<Resp> = lines
        .into_iter()
        .take(128)
        .map(|line| {
            let end = line
                .char_indices()
                .nth(4096)
                .map_or(line.len(), |(at, _)| at);
            let line = Bytes::copy_from_slice(&line.as_bytes()[..end]);
            bytes += line.len();
            Resp::Bulk(Some(line))
        })
        .collect();
    enqueue(debug, Resp::Array(Some(lines)), bytes, false, false);
}
impl ExecutionControl {
    pub fn is_running(&self) -> bool {
        self.state.load(Ordering::Acquire) != 0
    }
    /// Short scripts retain atomic queued execution; only a debugger stop or
    /// Redis's default five-second busy threshold rejects concurrent commands.
    pub fn is_busy(&self) -> bool {
        if !self.is_running() {
            return false;
        }
        let debug = self.debugger.lock().unwrap();
        (debug.enabled && debug.paused)
            || debug
                .started
                .is_some_and(|started| started.elapsed() >= Duration::from_secs(5))
    }
    pub fn set_owner_client_scope(
        &self,
        owner: u64,
        disconnected: Option<Arc<std::sync::atomic::AtomicBool>>,
    ) {
        let mut debug = self.debugger.lock().unwrap();
        debug.owner = owner;
        debug.abandoned = disconnected
            .as_ref()
            .is_some_and(|token| token.load(Ordering::Acquire));
        debug.disconnected = disconnected;
    }
    pub fn debugger_paused_for(&self, owner: u64) -> bool {
        let debug = self.debugger.lock().unwrap();
        debug.owner == owner && debug.enabled && debug.paused && !debug.abandoned
    }
    pub fn mark_write(&self) -> bool {
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & 8 != 0 {
                return false;
            }
            if state == 0 {
                return true;
            }
            match self.state.compare_exchange_weak(
                state,
                state | 4,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(next) => state = next,
            }
        }
    }
    pub fn cancelled(&self) -> bool {
        self.state.load(Ordering::Acquire) & 8 != 0
    }
    pub fn control(&self, args: &[Bytes]) -> Option<Resp> {
        let function = args.first()?.eq_ignore_ascii_case(b"FUNCTION");
        if !function && !args[0].eq_ignore_ascii_case(b"SCRIPT") {
            return None;
        }
        if args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"STATS"))
            && function
        {
            if args.len() != 2 {
                return Some(error_reply(
                    "ERR wrong number of arguments for FUNCTION STATS",
                ));
            }
            let debug = self.debugger.lock().unwrap();
            let running =
                if self.is_running() && debug.function {
                    Resp::Map(vec![
                        (bulk("name"), Resp::Bulk(debug.command.get(1).cloned())),
                        (
                            bulk("command"),
                            Resp::Array(Some(
                                debug
                                    .command
                                    .iter()
                                    .cloned()
                                    .map(|v| Resp::Bulk(Some(v)))
                                    .collect(),
                            )),
                        ),
                        (
                            bulk("duration_ms"),
                            Resp::Integer(debug.started.map_or(0, |s| {
                                s.elapsed().as_millis().min(i64::MAX as u128) as i64
                            })),
                        ),
                    ])
                } else {
                    Resp::Bulk(None)
                };
            return Some(Resp::Map(vec![
                (bulk("running_script"), running),
                (
                    bulk("engines"),
                    Resp::Map(vec![(
                        bulk("LUA"),
                        Resp::Map(vec![
                            (
                                bulk("libraries_count"),
                                Resp::Integer(debug.libraries as i64),
                            ),
                            (
                                bulk("functions_count"),
                                Resp::Integer(debug.functions as i64),
                            ),
                        ]),
                    )]),
                ),
            ]));
        }
        if !args.get(1).is_some_and(|s| s.eq_ignore_ascii_case(b"KILL")) {
            return None;
        }
        if args.len() != 2 {
            return Some(error_reply("ERR wrong number of arguments for KILL"));
        }
        let expected = if function { 2 } else { 1 };
        let mut state = self.state.load(Ordering::Acquire);
        loop {
            if state & 3 != expected {
                return Some(error_reply("NOTBUSY No scripts in execution right now."));
            }
            if state & 4 != 0 {
                return Some(error_reply(
                    "UNKILLABLE Sorry the script already executed write commands against the dataset.",
                ));
            }
            match self.state.compare_exchange_weak(
                state,
                state | 8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.wake.notify_all();
                    return Some(Resp::Simple("OK"));
                }
                Err(next) => state = next,
            }
        }
    }
    pub(super) fn counts(&self, libraries: usize, functions: usize) {
        let mut debug = self.debugger.lock().unwrap();
        debug.libraries = libraries;
        debug.functions = functions;
    }
    pub(super) fn begin(
        self: &Arc<Self>,
        args: &[Bytes],
        source: &Bytes,
        function: bool,
        mode: DebugMode,
    ) -> Result<ExecutionGuard, Resp> {
        let mut debug = self.debugger.lock().unwrap();
        debug.abandoned |= debug
            .disconnected
            .as_ref()
            .is_some_and(|token| token.load(Ordering::Acquire));
        // The request can outlive its transport while queued. Reject before
        // publishing running state, not at the VM's first debugger line.
        if debug.abandoned && mode != DebugMode::No && !function {
            return Err(error_reply("ERR Lua debugger client disconnected"));
        }
        debug.command = args.to_vec();
        debug.source = source.clone();
        debug.started = Some(Instant::now());
        debug.function = function;
        debug.enabled = mode != DebugMode::No && !function;
        debug.step = true;
        debug.abort = false;
        debug.breakpoints.clear();
        debug.line = 0;
        self.state
            .store(if function { 2 } else { 1 }, Ordering::Release);
        Ok(ExecutionGuard(self.clone()))
    }
    pub fn take_debug_events_for(&self, owner: u64) -> Vec<Resp> {
        let mut debug = self.debugger.lock().unwrap();
        let mut result = Vec::new();
        let mut index = 0;
        while index < debug.events.len() {
            if debug.events[index].owner == owner {
                let event = debug.events.remove(index).expect("queued debugger event");
                debug.event_bytes = debug.event_bytes.saturating_sub(event.bytes);
                result.push(event.value);
            } else {
                index += 1;
            }
        }
        result
    }
    /// Dropping one transport must not abort another client's paused VM.
    pub fn abort_debug_for(&self, owner: u64) {
        let mut debug = self.debugger.lock().unwrap();
        discard_events(&mut debug, owner, true);
        if debug.owner == owner {
            debug.abandoned = true;
            if debug.enabled {
                debug.abort = true;
                debug.paused = false;
                // Instruction hooks must also cancel a debugger that has
                // continued into an infinite loop without another line stop.
                self.state.fetch_or(8, Ordering::AcqRel);
                self.wake.notify_all();
            }
        }
    }
    pub fn debug_command_for(&self, owner: u64, args: &[Bytes]) -> Option<Resp> {
        let mut debug = self.debugger.lock().unwrap();
        if debug.owner != owner || !debug.enabled || !debug.paused || debug.abandoned {
            return None;
        }
        let name = args.first()?.first()?.to_ascii_lowercase();
        match name {
            b's' | b'n' | b'c' => {
                debug.step = name != b'c';
                debug.paused = false;
                self.wake.notify_all();
            }
            b'a' => {
                debug.abort = true;
                debug.paused = false;
                self.wake.notify_all();
            }
            b'b' => {
                for value in &args[1..] {
                    let Some(line) = std::str::from_utf8(value)
                        .ok()
                        .and_then(|v| v.parse::<i64>().ok())
                    else {
                        return Some(error_reply("ERR invalid breakpoint"));
                    };
                    if line == 0 {
                        debug.breakpoints.clear();
                    } else if line < 0 {
                        debug
                            .breakpoints
                            .remove(&line.unsigned_abs().try_into().unwrap_or(usize::MAX));
                    } else {
                        debug.breakpoints.insert(line as usize);
                    }
                }
                let lines = debug
                    .breakpoints
                    .iter()
                    .map(|line| format!("Breakpoint at line {line}"))
                    .collect();
                push(&mut debug, lines);
            }
            b'l' | b'w' => {
                let current = args
                    .get(1)
                    .and_then(|v| std::str::from_utf8(v).ok()?.parse::<usize>().ok())
                    .filter(|&n| n > 0)
                    .unwrap_or(debug.line);
                let context = if name == b'w' {
                    usize::MAX
                } else {
                    args.get(2)
                        .and_then(|v| std::str::from_utf8(v).ok()?.parse().ok())
                        .unwrap_or(5)
                };
                let lines = String::from_utf8_lossy(&debug.source)
                    .lines()
                    .enumerate()
                    .filter(|(n, _)| (n + 1).abs_diff(current) <= context)
                    .take(128)
                    .map(|(n, text)| {
                        format!(
                            "{} {} {}",
                            if n + 1 == debug.line { "->" } else { "  " },
                            n + 1,
                            text.chars().take(4096).collect::<String>()
                        )
                    })
                    .collect();
                push(&mut debug, lines);
            }
            b'h' => push(
                &mut debug,
                vec!["[s]tep [n]ext [c]ontinue [b]reak line [l]ist [w]hole [a]bort".into()],
            ),
            _ => return Some(error_reply("ERR Unknown Lua debugger command")),
        }
        Some(Resp::Simple("OK"))
    }
    pub(super) fn line(&self, line: usize) -> mlua::Result<()> {
        let mut debug = self.debugger.lock().unwrap();
        if !debug.enabled {
            return Ok(());
        }
        if debug.abandoned {
            return Err(lua_error("Lua debugger client disconnected"));
        }
        debug.line = line;
        if debug.step || debug.breakpoints.contains(&line) {
            debug.paused = true;
            // If a client resumes without draining its console, retain the
            // newest stop rather than silently pausing without an observable stop.
            if debug
                .events
                .iter()
                .filter(|event| event.owner == debug.owner && !event.terminal)
                .count()
                >= 128
            {
                let owner = debug.owner;
                discard_events(&mut debug, owner, false);
            }
            let source: String = String::from_utf8_lossy(&debug.source)
                .lines()
                .nth(line.saturating_sub(1))
                .unwrap_or("")
                .chars()
                .take(4096)
                .collect();
            let values = vec![
                Resp::Bulk(Some(Bytes::from(format!("* Stopped at {line}")))),
                Resp::Bulk(Some(Bytes::from(format!("-> {line} {source}")))),
            ];
            let bytes = source.len() + 64;
            enqueue(&mut debug, Resp::Array(Some(values)), bytes, false, true);
            while debug.paused && !self.cancelled() {
                debug = self.wake.wait(debug).unwrap();
            }
        }
        if debug.abort || self.cancelled() {
            Err(lua_error(
                "Script killed by user with SCRIPT KILL or debugger abort",
            ))
        } else {
            Ok(())
        }
    }
    pub(super) fn breakpoint(&self) {
        self.debugger.lock().unwrap().step = true;
    }
    pub(super) fn log(&self, values: MultiValue) {
        let mut debug = self.debugger.lock().unwrap();
        if !debug.enabled {
            return;
        }
        let mut text = String::new();
        for value in values.into_iter().take(128) {
            if text.len() >= 4096 {
                break;
            }
            if !text.is_empty() {
                text.push_str(", ");
            }
            match value {
                Value::String(value) => {
                    let bytes = value.as_bytes();
                    text.push_str(&String::from_utf8_lossy(
                        &bytes[..bytes.len().min(4096_usize.saturating_sub(text.len()))],
                    ));
                }
                value => {
                    let _ = fmt::write(&mut text, format_args!("{value:?}"));
                }
            }
        }
        let line = debug.line;
        push(&mut debug, vec![format!("<debug> line {line}: {text}")]);
    }
}
fn bulk(text: &'static str) -> Resp {
    Resp::Bulk(Some(Bytes::from_static(text.as_bytes())))
}

#[cfg(test)]
mod owner_tests {
    use super::*;

    fn begin(control: &Arc<ExecutionControl>, owner: u64) -> ExecutionGuard {
        control.set_owner_client_scope(owner, None);
        let source = Bytes::from_static(b"return 1");
        control
            .begin(
                &[
                    Bytes::from_static(b"EVAL"),
                    source.clone(),
                    Bytes::from_static(b"0"),
                ],
                &source,
                false,
                DebugMode::Yes,
            )
            .unwrap()
    }

    #[test]
    fn completed_events_survive_the_next_owner_and_cannot_be_stolen() {
        let control = Arc::new(ExecutionControl::default());
        let first = begin(&control, 11);
        push(
            &mut control.debugger.lock().unwrap(),
            vec!["first client".into()],
        );
        drop(first);
        let second = begin(&control, 22);
        {
            let mut debug = control.debugger.lock().unwrap();
            debug.paused = true;
            push(&mut debug, vec!["second client".into()]);
        }
        let first_events = control.take_debug_events_for(11);
        assert_eq!(first_events.len(), 2);
        assert!(
            matches!(&first_events[1], Resp::Array(Some(values)) if matches!(&values[0], Resp::Bulk(Some(value)) if value.as_ref() == b"<endsession>"))
        );
        control.abort_debug_for(11);
        assert!(control.debugger_paused_for(22));
        assert_eq!(control.take_debug_events_for(22).len(), 1);
        drop(second);
        assert_eq!(control.take_debug_events_for(22).len(), 1);
    }

    #[test]
    fn disconnected_owner_leaves_neither_logs_nor_a_late_terminal() {
        let control = Arc::new(ExecutionControl::default());
        let guard = begin(&control, 7);
        push(&mut control.debugger.lock().unwrap(), vec!["unread".into()]);
        control.abort_debug_for(7);
        assert!(!control.debugger_paused_for(7));
        drop(guard);
        assert!(control.take_debug_events_for(7).is_empty());
        let next = begin(&control, 8);
        assert!(!control.debugger.lock().unwrap().abandoned);
        drop(next);
    }

    #[test]
    fn undrained_completed_sessions_remain_bounded() {
        let control = Arc::new(ExecutionControl::default());
        for owner in 1..=300 {
            drop(begin(&control, owner));
        }
        let debug = control.debugger.lock().unwrap();
        assert!(debug.events.len() <= 256);
        assert!(debug.event_bytes <= MAX_VALUE);
        assert!(
            debug
                .events
                .back()
                .is_some_and(|event| event.owner == 300 && event.terminal)
        );
    }
}

#[cfg(test)]
mod disconnect_tests {
    use super::*;
    #[test]
    fn disconnect_before_owner_or_begin_never_publishes_running_state() {
        for before_owner in [true, false] {
            let control = Arc::new(ExecutionControl::default());
            let disconnected = Arc::new(std::sync::atomic::AtomicBool::new(before_owner));
            control.set_owner_client_scope(9, Some(disconnected.clone()));
            disconnected.store(true, Ordering::Release);
            let source = Bytes::from_static(b"return 1");
            assert!(
                control
                    .begin(
                        &[
                            Bytes::from_static(b"EVAL"),
                            source.clone(),
                            Bytes::from_static(b"0")
                        ],
                        &source,
                        false,
                        DebugMode::Yes
                    )
                    .is_err()
            );
            assert!(!control.is_running());
        }
    }

    #[test]
    fn disconnect_after_continue_cancels_a_real_infinite_lua_invocation() {
        let mut runner = ScriptRunner::new().unwrap();
        runner.set_debug_mode(DebugMode::Yes);
        let control = runner.execution_control();
        control.set_owner_client_scope(9, None);
        let (entered, entering) = std::sync::mpsc::channel();
        let (finished, finishing) = std::sync::mpsc::channel();
        let worker = std::thread::spawn(move || {
            let args = [Bytes::from_static(b"EVAL"), Bytes::from_static(b"redis.call('GET','ready'); local s=string.rep('x',1048576); while true do s=string.gsub(s,'x','x') end"), Bytes::from_static(b"0")];
            let reply = runner
                .eval(&args, |_, _, _| {
                    entered.send(()).unwrap();
                    Ok(Resp::Bulk(None))
                })
                .unwrap();
            let healthy = runner
                .eval(
                    &[
                        Bytes::from_static(b"EVAL"),
                        Bytes::from_static(b"return 42"),
                        Bytes::from_static(b"0"),
                    ],
                    |_, _, _| unreachable!(),
                )
                .unwrap();
            finished.send((reply, healthy)).unwrap();
        });
        let deadline = Instant::now() + std::time::Duration::from_secs(5);
        while !control.debugger_paused_for(9) {
            assert!(Instant::now() < deadline, "debugger did not pause");
            std::thread::yield_now();
        }
        assert_eq!(
            control.debug_command_for(9, &[Bytes::from_static(b"continue")]),
            Some(Resp::Simple("OK"))
        );
        entering
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        control.abort_debug_for(9);
        let (reply, healthy) = finishing
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        worker.join().unwrap();
        assert!(matches!(reply, Resp::Error(_)));
        assert_eq!(healthy, Resp::Integer(42));
        assert!(!control.is_running());
        assert!(control.take_debug_events_for(9).is_empty());
    }

    #[test]
    fn disconnecting_a_nondebug_script_does_not_kill_its_execution() {
        let control = Arc::new(ExecutionControl::default());
        control.set_owner_client_scope(9, None);
        let source = Bytes::from_static(b"return 1");
        let guard = control
            .begin(
                &[
                    Bytes::from_static(b"EVAL"),
                    source.clone(),
                    Bytes::from_static(b"0"),
                ],
                &source,
                false,
                DebugMode::No,
            )
            .unwrap();
        control.abort_debug_for(9);
        assert!(control.line(1).is_ok());
        assert!(!control.cancelled());
        drop(guard);
    }
}
