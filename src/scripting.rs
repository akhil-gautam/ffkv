//! Bounded Lua 5.1 execution. The caller owns atomic command execution and durability.
//!
//! Libraries: Lua base (without loaders, environment/metatable introspection or GC
//! controls), table, string (without dump), math, cjson and LuaBitOp-compatible bit.
//! Eval receives new globals and library tables; named functions retain their
//! library-local helpers, while borrowed command bridges exist only during a call.
//! The caller supplies a frozen dataset view and commits resolved effects even on
//! Lua errors. Readonly/flags/rollback admission is explicit InvocationContext.
//! Limits: 1M instructions, 32 MiB Lua memory, 64 call/reply/JSON levels, 8 MiB
//! scripts/bridge values, 256 cached scripts/16 MiB and 256 libraries/16 MiB.
//! ExecutionControl provides cancellation and a bounded cooperative line debugger
//! independently of the runner lock. Library snapshots are Redis FUNCTION2 RDB
//! containers with shared version/checksum validation, unlike ephemeral scripts.

use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io::{self, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use bytes::Bytes;
use mlua::{
    ChunkMode, DebugEvent, HookTriggers, Lua, LuaOptions, MultiValue, StdLib, Table, Value, VmState,
};
use sha1::{Digest, Sha1};

use crate::protocol::Resp;

#[path = "scripting_control.rs"]
mod control;
#[path = "functions.rs"]
mod functions;
pub use control::{DebugMode, ExecutionControl, InvocationContext};
pub use functions::FunctionFlags;
use functions::script_metadata;

const MAX_SCRIPT: usize = 8 * 1024 * 1024;
const MAX_VALUE: usize = 8 * 1024 * 1024;
const MAX_MEMORY: usize = 32 * 1024 * 1024;
const MAX_CACHE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 256;
const MAX_INSTRUCTIONS: usize = 1_000_000;
const MAX_DEPTH: usize = 64;

pub struct ScriptRunner {
    lua: Lua,
    scripts: HashMap<String, Bytes>,
    order: VecDeque<String>,
    cache_bytes: usize,
    libraries: std::collections::BTreeMap<String, functions::Library>,
    function_revision: u64,
    execution: Arc<ExecutionControl>,
    debug_mode: DebugMode,
}

#[derive(Debug)]
struct CommandError(String);

impl fmt::Display for CommandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}
impl std::error::Error for CommandError {}

fn lua_error(message: impl Into<String>) -> mlua::Error {
    mlua::Error::RuntimeError(message.into())
}

fn clean_line(bytes: &[u8]) -> Vec<u8> {
    bytes
        .iter()
        .map(|&b| if b == b'\r' || b == b'\n' { b' ' } else { b })
        .collect()
}

fn error_reply(message: &str) -> Resp {
    let message = String::from_utf8_lossy(&clean_line(message.as_bytes())).into_owned();
    // Redis error tables may supply a code; unclassified user errors use ERR.
    let code = message.split(' ').next().unwrap_or("");
    let classified = !code.is_empty()
        && code
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'_');
    Resp::Error(if classified {
        message
    } else {
        format!("ERR {message}")
    })
}

fn runtime_reply(error: mlua::Error) -> Resp {
    let mut cause = &error;
    loop {
        if let Some(command) = cause.downcast_ref::<CommandError>() {
            return error_reply(&command.0);
        }
        match cause {
            mlua::Error::CallbackError { cause: next, .. }
            | mlua::Error::WithContext { cause: next, .. } => cause = next,
            _ => break,
        }
    }
    error_reply(&format!("ERR Error running script: {error}"))
}

fn digest(body: &[u8]) -> String {
    format!("{:x}", Sha1::digest(body))
}

impl ScriptRunner {
    pub fn new() -> io::Result<Self> {
        let lua = Lua::new_with(
            StdLib::TABLE | StdLib::STRING | StdLib::MATH,
            LuaOptions::default(),
        )
        .map_err(io::Error::other)?;
        lua.set_memory_limit(MAX_MEMORY).map_err(io::Error::other)?;
        // String method lookup uses Lua's shared string metatable, not the
        // invocation's copy. Remove dump there too, and never expose getmetatable.
        let string: Table = lua.globals().get("string").map_err(io::Error::other)?;
        string
            .raw_set("dump", Value::Nil)
            .map_err(io::Error::other)?;
        Ok(Self {
            lua,
            scripts: HashMap::new(),
            order: VecDeque::new(),
            cache_bytes: 0,
            libraries: std::collections::BTreeMap::new(),
            function_revision: 0,
            execution: Arc::new(ExecutionControl::default()),
            debug_mode: DebugMode::No,
        })
    }

    fn compile(&self, body: &[u8]) -> mlua::Result<()> {
        let (body, _) = script_metadata(body).map_err(lua_error)?;
        self.lua
            .load(body)
            .set_name("@user_script")
            .set_mode(ChunkMode::Text)
            .into_function()
            .map(|_| ())
    }

    fn remember(&mut self, sha: String, body: &[u8]) {
        if self.scripts.contains_key(&sha) {
            return;
        }
        while self.scripts.len() >= MAX_CACHE_ENTRIES
            || self.cache_bytes + body.len() > MAX_CACHE_BYTES
        {
            if let Some(old) = self.order.pop_front() {
                if let Some(body) = self.scripts.remove(&old) {
                    self.cache_bytes -= body.len();
                }
            } else {
                break;
            }
        }
        self.cache_bytes += body.len();
        // Never retain an EVAL/LOAD frame's shared allocation (which may also
        // contain large arguments unrelated to this script).
        self.scripts
            .insert(sha.clone(), Bytes::copy_from_slice(body));
        self.order.push_back(sha);
    }

    pub fn control(&mut self, args: &[Bytes]) -> Option<Resp> {
        if let Some(reply) = self.execution.control(args) {
            return Some(reply);
        }
        if args
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"FUNCTION"))
        {
            return Some(self.function_control(args));
        }
        if !args
            .first()
            .is_some_and(|name| name.eq_ignore_ascii_case(b"SCRIPT"))
        {
            return None;
        }
        let response = if args.get(1).is_some_and(|s| s.eq_ignore_ascii_case(b"LOAD")) {
            if args.len() != 3 {
                error_reply("ERR wrong number of arguments for 'script|load' command")
            } else if args[2].len() > MAX_SCRIPT {
                error_reply("ERR script exceeds 8 MiB limit")
            } else {
                let _ = self.lua.gc_collect();
                match self.compile(&args[2]) {
                    Err(error) => runtime_reply(error),
                    Ok(()) => {
                        let sha = digest(&args[2]);
                        self.remember(sha.clone(), &args[2]);
                        Resp::Bulk(Some(Bytes::from(sha)))
                    }
                }
            }
        } else if args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"EXISTS"))
        {
            if args.len() < 3 {
                error_reply("ERR wrong number of arguments for 'script|exists' command")
            } else {
                crate::engine::bounded_replies(
                    args[2..].iter().map(|sha| {
                        let exists = sha.len() == 40
                            && sha.iter().all(u8::is_ascii_hexdigit)
                            && self
                                .scripts
                                .contains_key(&String::from_utf8_lossy(sha).to_ascii_lowercase());
                        Resp::Integer(i64::from(exists))
                    }),
                    MAX_VALUE,
                )
            }
        } else if args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"FLUSH"))
        {
            if args.len() == 2
                || (args.len() == 3
                    && (args[2].eq_ignore_ascii_case(b"SYNC")
                        || args[2].eq_ignore_ascii_case(b"ASYNC")))
            {
                self.scripts.clear();
                self.order.clear();
                self.cache_bytes = 0;
                let _ = self.lua.gc_collect();
                Resp::Simple("OK")
            } else {
                error_reply("ERR SCRIPT FLUSH only supports SYNC|ASYNC option")
            }
        } else if args
            .get(1)
            .is_some_and(|s| s.eq_ignore_ascii_case(b"DEBUG"))
        {
            if args.len() != 3 {
                error_reply("ERR wrong number of arguments for SCRIPT DEBUG")
            } else if let Some(mode) = Self::parse_debug_mode(&args[2]) {
                self.debug_mode = mode;
                Resp::Simple("OK")
            } else {
                error_reply("ERR Use SCRIPT DEBUG YES/SYNC/NO")
            }
        } else {
            error_reply("ERR unknown SCRIPT subcommand or wrong number of arguments")
        };
        Some(response)
    }

    pub fn eval(
        &mut self,
        args: &[Bytes],
        mut call: impl FnMut(&[Bytes], crate::protocol::Version, InvocationContext) -> io::Result<Resp>,
    ) -> io::Result<Resp> {
        if args.len() < 3 {
            return Ok(error_reply(
                "ERR wrong number of arguments for EVAL/EVALSHA",
            ));
        }
        let is_eval =
            args[0].eq_ignore_ascii_case(b"EVAL") || args[0].eq_ignore_ascii_case(b"EVAL_RO");
        let is_function =
            args[0].eq_ignore_ascii_case(b"FCALL") || args[0].eq_ignore_ascii_case(b"FCALL_RO");
        let readonly = args[0].eq_ignore_ascii_case(b"EVAL_RO")
            || args[0].eq_ignore_ascii_case(b"EVALSHA_RO")
            || args[0].eq_ignore_ascii_case(b"FCALL_RO");
        if !is_eval
            && !is_function
            && !args[0].eq_ignore_ascii_case(b"EVALSHA")
            && !args[0].eq_ignore_ascii_case(b"EVALSHA_RO")
        {
            return Ok(error_reply("ERR expected EVAL, EVALSHA, or FCALL"));
        }
        let text = std::str::from_utf8(&args[2]).unwrap_or("");
        let digits = text.strip_prefix('-').unwrap_or(text);
        let canonical = !digits.is_empty()
            && digits.bytes().all(|b| b.is_ascii_digit())
            && (digits.len() == 1 || !digits.starts_with('0'));
        let nkeys = if canonical {
            text.parse::<i64>().ok()
        } else {
            None
        };
        let Some(nkeys) = nkeys else {
            return Ok(error_reply("ERR value is not an integer or out of range"));
        };
        if nkeys < 0 {
            return Ok(error_reply("ERR Number of keys can't be negative"));
        }
        if nkeys as u64 > (args.len() - 3) as u64 {
            return Ok(error_reply(
                "ERR Number of keys can't be greater than number of args",
            ));
        }
        let mut callback = None;
        let mut flags = FunctionFlags::default();
        let body = if is_function {
            let name = std::str::from_utf8(&args[1]).unwrap_or("");
            let Some((library, function)) = self.libraries.values().find_map(|library| {
                library
                    .functions
                    .iter()
                    .find(|(key, _)| key.eq_ignore_ascii_case(name))
                    .map(|(_, f)| (library, f))
            }) else {
                return Ok(error_reply("ERR Function not found"));
            };
            flags = function.flags;
            callback = Some(function.callback.clone());
            library.source.clone()
        } else if is_eval {
            if args[1].len() > MAX_SCRIPT {
                return Ok(error_reply("ERR script exceeds 8 MiB limit"));
            }
            args[1].clone()
        } else {
            let sha = String::from_utf8_lossy(&args[1]).to_ascii_lowercase();
            let Some(body) = self.scripts.get(&sha) else {
                return Ok(Resp::Error(
                    "NOSCRIPT No matching script. Please use EVAL.".into(),
                ));
            };
            body.clone()
        };
        let (script_body, script_flags) = if !is_function {
            match script_metadata(&body) {
                Ok(value) => value,
                Err(error) => return Ok(error_reply(&error)),
            }
        } else {
            (&body[..], flags)
        };
        flags = script_flags;
        if readonly && (is_function || body.starts_with(b"#!")) && !flags.no_writes {
            return Ok(error_reply(
                "ERR Can not execute a script with write flag using *_ro command.",
            ));
        }
        let mode = if is_function {
            DebugMode::No
        } else {
            std::mem::take(&mut self.debug_mode)
        };
        let invocation = InvocationContext {
            readonly: readonly || flags.no_writes,
            flags,
            rollback: mode == DebugMode::Yes,
        };
        let execution = self.execution.clone();
        let execution_guard = match execution.begin(args, &body, is_function, mode) {
            Ok(guard) => guard,
            Err(reply) => return Ok(reply),
        };
        let fatal = RefCell::new(None::<io::Error>);
        let response_version = Cell::new(2_u8);
        let bridge = RefCell::new(&mut call);
        let compiled = Cell::new(false);
        let instructions = Arc::new(AtomicUsize::new(0));
        let exhausted = Arc::new(AtomicBool::new(false));
        let instruction_count = instructions.clone();
        let limit_hit = exhausted.clone();
        let debug_line_offset = usize::from(!is_function && body.starts_with(b"#!"));
        let hooks = HookTriggers {
            on_calls: true,
            on_returns: false,
            every_line: mode != DebugMode::No,
            every_nth_instruction: Some(1000),
        };
        self.lua
            .set_hook(hooks, move |lua, debug| {
                if execution.cancelled() {
                    return Err(lua_error("Script killed by user with SCRIPT KILL"));
                }
                if debug.event() == DebugEvent::Line {
                    execution.line(debug.current_line().unwrap_or(0) + debug_line_offset)?;
                }
                let exceeded = match debug.event() {
                    DebugEvent::Count => {
                        instruction_count.fetch_add(1000, Ordering::Relaxed) + 1000
                            >= MAX_INSTRUCTIONS
                    }
                    // Count the actual stack: errors caught by pcall unwind without
                    // balanced return hooks, so an increment/decrement counter lies.
                    DebugEvent::Call => lua.inspect_stack(MAX_DEPTH, |_| ()).is_some(),
                    _ => false,
                };
                if exceeded {
                    limit_hit.store(true, Ordering::Relaxed);
                }
                if limit_hit.load(Ordering::Relaxed) {
                    Err(lua_error("script instruction or recursion limit exceeded"))
                } else {
                    Ok(VmState::Continue)
                }
            })
            .map_err(io::Error::other)?;
        let outcome = self.lua.scope(|scope| {
            let env = environment(&self.lua)?;
            let keys = self.lua.create_table_with_capacity(nkeys as usize, 0)?;
            for (index, key) in args[3..3 + nkeys as usize].iter().enumerate() {
                keys.raw_set(index + 1, self.lua.create_string(key)?)?;
            }
            let argv = self
                .lua
                .create_table_with_capacity(args.len() - 3 - nkeys as usize, 0)?;
            for (index, value) in args[3 + nkeys as usize..].iter().enumerate() {
                argv.raw_set(index + 1, self.lua.create_string(value)?)?;
            }
            if !is_function {
                env.raw_set("KEYS", keys.clone())?;
                env.raw_set("ARGV", argv.clone())?;
            }
            let redis = redis_library(&self.lua)?;
            for (name, protected) in [("call", false), ("pcall", true)] {
                let bridge = &bridge;
                let fatal = &fatal;
                let version = &response_version;
                let function = scope.create_function(move |lua, values: MultiValue| {
                    if fatal.borrow().is_some() {
                        return Err(lua_error("fatal command I/O failure"));
                    }
                    let result = (|| {
                        if values.is_empty() {
                            return Err(lua_error(
                                "Please specify at least one argument for redis.call()",
                            ));
                        }
                        let mut budget = crate::engine::ReplyBudget::new(MAX_VALUE);
                        budget
                            .charge(values.len(), 0)
                            .map_err(|_| lua_error("script command exceeds 8 MiB limit"))?;
                        let mut args = Vec::with_capacity(values.len());
                        for value in values {
                            if !matches!(
                                &value,
                                Value::String(_) | Value::Integer(_) | Value::Number(_)
                            ) {
                                return Err(lua_error(
                                    "Lua redis lib command arguments must be strings or integers",
                                ));
                            }
                            let string = lua
                                .coerce_string(value)?
                                .ok_or_else(|| lua_error("invalid command argument"))?;
                            budget
                                .charge(0, string.as_bytes().len())
                                .map_err(|_| lua_error("script command exceeds 8 MiB limit"))?;
                            args.push(Bytes::copy_from_slice(string.as_bytes().as_ref()));
                        }
                        let command_version = if version.get() == 3 {
                            crate::protocol::Version::Resp3
                        } else {
                            crate::protocol::Version::Resp2
                        };
                        let response =
                            match (bridge.borrow_mut())(&args, command_version, invocation) {
                                Ok(response) => response,
                                Err(error) => {
                                    *fatal.borrow_mut() = Some(error);
                                    return Err(lua_error("fatal command I/O failure"));
                                }
                            };
                        if let Resp::Error(error) = response {
                            return Err(mlua::Error::external(CommandError(error)));
                        }
                        resp_to_lua(lua, response, version.get(), 0)
                    })();
                    match result {
                        Err(error) if protected && fatal.borrow().is_none() => {
                            let message = error
                                .downcast_ref::<CommandError>()
                                .map(|e| e.0.clone())
                                .unwrap_or_else(|| format!("ERR {error}"));
                            let table = lua.create_table()?;
                            table.raw_set("err", message)?;
                            Ok(Value::Table(table))
                        }
                        other => other,
                    }
                })?;
                self.lua.set_named_registry_value(
                    if protected { "ffkv_pcall" } else { "ffkv_call" },
                    function.clone(),
                )?;
                redis.raw_set(name, function)?;
            }
            let setresp = scope.create_function(|_, version: i64| {
                if version != 2 && version != 3 {
                    return Err(lua_error("RESP version must be 2 or 3"));
                }
                response_version.set(version as u8);
                Ok(())
            })?;
            self.lua
                .set_named_registry_value("ffkv_setresp", setresp.clone())?;
            redis.raw_set("setresp", setresp)?;
            let control = self.execution.clone();
            redis.raw_set(
                "breakpoint",
                self.lua.create_function(move |_, ()| {
                    control.breakpoint();
                    Ok(())
                })?,
            )?;
            let control = self.execution.clone();
            redis.raw_set(
                "debug",
                self.lua.create_function(move |_, values: MultiValue| {
                    control.log(values);
                    Ok(())
                })?,
            )?;
            env.raw_set("redis", redis)?;
            let value: Value = if let Some(function) = callback {
                function.call((keys, argv))?
            } else {
                let function = self
                    .lua
                    .load(script_body)
                    .set_name("@user_script")
                    .set_mode(ChunkMode::Text)
                    .set_environment(env)
                    .into_function()?;
                compiled.set(true);
                function.call(())?
            };
            let mut budget = MAX_VALUE;
            lua_to_resp(value, 0, &mut budget)
        });
        self.lua.remove_hook();
        for name in ["ffkv_call", "ffkv_pcall", "ffkv_setresp"] {
            let _ = self.lua.unset_named_registry_value(name);
        }
        let cancelled = execution_guard.finish();
        // Scoped callbacks and invocation globals are gone before collecting or
        // releasing the caller's mutable borrow. No callback survives this call.
        let _ = self.lua.gc_collect();
        if is_eval && compiled.get() {
            self.remember(digest(&body), &body);
        }
        if let Some(error) = fatal.into_inner() {
            return Err(error);
        }
        if cancelled {
            return Ok(error_reply("ERR Script killed by user with SCRIPT KILL"));
        }
        if exhausted.load(Ordering::Relaxed) {
            return Ok(error_reply(
                "ERR script instruction or recursion limit exceeded",
            ));
        }
        Ok(match outcome {
            Ok(response) => response,
            Err(error) => runtime_reply(error),
        })
    }
}

fn environment(lua: &Lua) -> mlua::Result<Table> {
    let env = lua.create_table()?;
    let globals = lua.globals();
    for name in [
        "assert",
        "error",
        "ipairs",
        "next",
        "pairs",
        "pcall",
        "rawequal",
        "rawget",
        "rawset",
        "select",
        "setmetatable",
        "tonumber",
        "tostring",
        "type",
        "unpack",
        "xpcall",
        "_VERSION",
    ] {
        env.raw_set(name, globals.raw_get::<Value>(name)?)?;
    }
    for name in ["math", "string", "table"] {
        let source: Table = globals.raw_get(name)?;
        let copy = lua.create_table()?;
        for entry in source.pairs::<Value, Value>() {
            let (key, value) = entry?;
            copy.raw_set(key, value)?;
        }
        env.raw_set(name, copy)?;
    }
    // Redis starts each script's pseudo-random sequence from a fixed seed.
    let math: Table = env.raw_get("math")?;
    math.raw_get::<mlua::Function>("randomseed")?
        .call::<()>(0)?;
    env.raw_set("cjson", cjson_library(lua)?)?;
    env.raw_set("bit", bit_library(lua)?)?;
    env.raw_set("_G", env.clone())?;
    Ok(env)
}

fn redis_library(lua: &Lua) -> mlua::Result<Table> {
    let redis = lua.create_table()?;
    for (name, field) in [("status_reply", "ok"), ("error_reply", "err")] {
        redis.raw_set(
            name,
            lua.create_function(move |lua, text: mlua::String| {
                let table = lua.create_table()?;
                table.raw_set(
                    field,
                    lua.create_string(clean_line(text.as_bytes().as_ref()))?,
                )?;
                Ok(table)
            })?,
        )?;
    }
    redis.raw_set(
        "sha1hex",
        lua.create_function(|_, text: mlua::String| Ok(digest(text.as_bytes().as_ref())))?,
    )?;
    // Resolved commands, not EVAL source, are journaled by the caller.
    redis.raw_set("replicate_commands", lua.create_function(|_, ()| Ok(true))?)?;
    redis.raw_set(
        "set_repl",
        lua.create_function(|_, _: MultiValue| -> mlua::Result<()> {
            Err(lua_error("replication controls are not supported"))
        })?,
    )?;
    for (name, value) in [
        ("LOG_DEBUG", 0),
        ("LOG_VERBOSE", 1),
        ("LOG_NOTICE", 2),
        ("LOG_WARNING", 3),
    ] {
        redis.raw_set(name, value)?;
    }
    redis.raw_set(
        "log",
        lua.create_function(|lua, mut values: MultiValue| {
            let level = values
                .pop_front()
                .ok_or_else(|| lua_error("redis.log requires a level and message"))?;
            let level = lua
                .coerce_integer(level)?
                .ok_or_else(|| lua_error("invalid log level"))?;
            if !(0..=3).contains(&level) || values.is_empty() {
                return Err(lua_error("invalid redis.log arguments"));
            }
            let mut message = Vec::new();
            for value in values {
                if message.len() >= 4096 {
                    break;
                }
                let text = lua
                    .coerce_string(value)?
                    .ok_or_else(|| lua_error("redis.log arguments must be strings or numbers"))?;
                if !message.is_empty() {
                    message.push(b' ');
                }
                // Bound log amplification and neutralize terminal/control injection.
                for &byte in text
                    .as_bytes()
                    .iter()
                    .take(4096_usize.saturating_sub(message.len()))
                {
                    message.push(if byte.is_ascii_control() { b' ' } else { byte });
                }
            }
            let mut stderr = io::stderr().lock();
            writeln!(
                stderr,
                "[lua:{level}] {}",
                String::from_utf8_lossy(&message)
            )
            .map_err(mlua::Error::external)?;
            Ok(())
        })?,
    )?;
    Ok(redis)
}

fn number_string(lua: &Lua, number: f64) -> mlua::Result<Value> {
    let mut buffer = ryu::Buffer::new();
    let text = if number.is_nan() {
        "nan"
    } else if number == f64::INFINITY {
        "inf"
    } else if number == f64::NEG_INFINITY {
        "-inf"
    } else {
        let text = buffer.format_finite(number);
        text.strip_suffix(".0").unwrap_or(text)
    };
    Ok(Value::String(lua.create_string(text)?))
}

fn tagged(lua: &Lua, name: &str, value: Value) -> mlua::Result<Value> {
    let table = lua.create_table()?;
    table.raw_set(name, value)?;
    Ok(Value::Table(table))
}

fn resp_to_lua(lua: &Lua, response: Resp, version: u8, depth: usize) -> mlua::Result<Value> {
    if depth >= MAX_DEPTH {
        return Err(lua_error("script reply nesting limit exceeded"));
    }
    match response {
        Resp::Simple(text) => tagged(lua, "ok", Value::String(lua.create_string(text)?)),
        Resp::Status(text) => tagged(lua, "ok", Value::String(lua.create_string(text)?)),
        Resp::Verbatim { format, value } => {
            if version == 3 {
                let body = lua.create_table()?;
                body.raw_set("format", lua.create_string(format)?)?;
                body.raw_set("string", lua.create_string(value)?)?;
                tagged(lua, "verbatim_string", Value::Table(body))
            } else {
                Ok(Value::String(lua.create_string(value)?))
            }
        }
        Resp::Error(text) => tagged(lua, "err", Value::String(lua.create_string(text)?)),
        Resp::Integer(n) => Ok(Value::Integer(n)),
        Resp::Boolean(value) => Ok(if version == 3 {
            Value::Boolean(value)
        } else {
            Value::Integer(i64::from(value))
        }),
        Resp::Unsigned(value) if value <= i64::MAX as u64 => Ok(Value::Integer(value as i64)),
        Resp::Unsigned(value) => {
            let text = Value::String(lua.create_string(value.to_string())?);
            if version == 3 {
                tagged(lua, "big_number", text)
            } else {
                Ok(text)
            }
        }
        Resp::BigNumber(value) => {
            let text = Value::String(lua.create_string(value)?);
            if version == 3 {
                tagged(lua, "big_number", text)
            } else {
                Ok(text)
            }
        }
        Resp::InfoField(field, value) => {
            let table = lua.create_table()?;
            let value = resp_to_lua(lua, *value, version, depth + 1)?;
            if version == 3 {
                table.raw_set(lua.create_string(field)?, value)?;
                tagged(lua, "map", Value::Table(table))
            } else {
                table.raw_set(1, value)?;
                Ok(Value::Table(table))
            }
        }
        Resp::Bulk(Some(text)) => Ok(Value::String(lua.create_string(text)?)),
        Resp::Bulk(None) | Resp::Array(None) => Ok(if version == 3 {
            Value::Nil
        } else {
            Value::Boolean(false)
        }),
        Resp::Double(n) => {
            if version == 3 {
                tagged(lua, "double", Value::Number(n))
            } else {
                number_string(lua, n)
            }
        }
        Resp::Array(Some(values)) => {
            let table = lua.create_table_with_capacity(values.len(), 0)?;
            for (index, value) in values.into_iter().enumerate() {
                table.raw_set(index + 1, resp_to_lua(lua, value, version, depth + 1)?)?;
            }
            Ok(Value::Table(table))
        }
        Resp::Set(values) => {
            let table = lua.create_table()?;
            for (index, value) in values.into_iter().enumerate() {
                let value = resp_to_lua(lua, value, version, depth + 1)?;
                if version == 3 {
                    table.raw_set(value, true)?;
                } else {
                    table.raw_set(index + 1, value)?;
                }
            }
            if version == 3 {
                tagged(lua, "set", Value::Table(table))
            } else {
                Ok(Value::Table(table))
            }
        }
        Resp::Map(values) => {
            let table = lua.create_table()?;
            for (index, (key, value)) in values.into_iter().enumerate() {
                let key = resp_to_lua(lua, key, version, depth + 1)?;
                let value = resp_to_lua(lua, value, version, depth + 1)?;
                if version == 3 {
                    table.raw_set(key, value)?;
                } else {
                    table.raw_set(index * 2 + 1, key)?;
                    table.raw_set(index * 2 + 2, value)?;
                }
            }
            if version == 3 {
                tagged(lua, "map", Value::Table(table))
            } else {
                Ok(Value::Table(table))
            }
        }
        Resp::Scored(values) => {
            let table = lua.create_table()?;
            for (index, (member, score)) in values.into_iter().enumerate() {
                if version == 3 {
                    let pair = lua.create_table_with_capacity(2, 0)?;
                    pair.raw_set(1, lua.create_string(member)?)?;
                    pair.raw_set(2, tagged(lua, "double", Value::Number(score))?)?;
                    table.raw_set(index + 1, pair)?;
                } else {
                    table.raw_set(index * 2 + 1, lua.create_string(member)?)?;
                    table.raw_set(index * 2 + 2, number_string(lua, score)?)?;
                }
            }
            Ok(Value::Table(table))
        }
        Resp::VectorResults(values) => {
            let mapped = version == 3
                && values
                    .first()
                    .is_some_and(|entry| entry.1.is_some() || entry.2.is_some());
            let table = lua.create_table()?;
            let mut index = 1;
            for (member, score, attr) in values {
                if mapped {
                    let value = match (score, attr) {
                        (Some(score), Some(attr)) => {
                            let pair = lua.create_table_with_capacity(2, 0)?;
                            pair.raw_set(1, tagged(lua, "double", Value::Number(score))?)?;
                            pair.raw_set(
                                2,
                                resp_to_lua(lua, Resp::Bulk(attr), version, depth + 1)?,
                            )?;
                            Value::Table(pair)
                        }
                        (Some(score), None) => tagged(lua, "double", Value::Number(score))?,
                        (None, Some(attr)) => {
                            resp_to_lua(lua, Resp::Bulk(attr), version, depth + 1)?
                        }
                        (None, None) => Value::Nil,
                    };
                    table.raw_set(lua.create_string(member)?, value)?;
                } else {
                    table.raw_set(index, lua.create_string(member)?)?;
                    index += 1;
                    if let Some(score) = score {
                        table.raw_set(
                            index,
                            resp_to_lua(lua, Resp::Double(score), version, depth + 1)?,
                        )?;
                        index += 1;
                    }
                    if let Some(attr) = attr {
                        table.raw_set(
                            index,
                            resp_to_lua(lua, Resp::Bulk(attr), version, depth + 1)?,
                        )?;
                        index += 1;
                    }
                }
            }
            if mapped {
                tagged(lua, "map", Value::Table(table))
            } else {
                Ok(Value::Table(table))
            }
        }
    }
}

fn charge(budget: &mut usize, bytes: usize) -> mlua::Result<()> {
    *budget = budget
        .checked_sub(bytes)
        .ok_or_else(|| lua_error("script reply exceeds 8 MiB limit"))?;
    Ok(())
}

fn lua_to_resp(value: Value, depth: usize, budget: &mut usize) -> mlua::Result<Resp> {
    if depth >= MAX_DEPTH {
        return Err(lua_error("script reply nesting limit exceeded"));
    }
    charge(budget, std::mem::size_of::<Resp>())?;
    Ok(match value {
        Value::Nil | Value::Boolean(false) => Resp::Bulk(None),
        Value::Boolean(true) => Resp::Integer(1),
        Value::Integer(number) => Resp::Integer(number),
        Value::Number(number) => Resp::Integer(
            if !number.is_finite()
                || number >= 9223372036854775808.0
                || number < -9223372036854775808.0
            {
                i64::MIN
            } else {
                number.trunc() as i64
            },
        ),
        Value::String(text) => {
            charge(budget, text.as_bytes().len())?;
            Resp::Bulk(Some(Bytes::copy_from_slice(text.as_bytes().as_ref())))
        }
        Value::Table(table) => {
            if let Value::String(text) = table.raw_get::<Value>("err")? {
                charge(budget, text.as_bytes().len())?;
                error_reply(&String::from_utf8_lossy(text.as_bytes().as_ref()))
            } else if let Value::String(text) = table.raw_get::<Value>("ok")? {
                charge(budget, text.as_bytes().len())?;
                Resp::Status(Bytes::from(clean_line(text.as_bytes().as_ref())))
            } else if let Value::Number(number) = table.raw_get::<Value>("double")? {
                Resp::Double(number)
            } else if let Value::Integer(number) = table.raw_get::<Value>("double")? {
                Resp::Double(number as f64)
            } else if let Value::String(number) = table.raw_get::<Value>("big_number")? {
                let bytes = number.as_bytes();
                let digits = bytes.strip_prefix(b"-").unwrap_or(bytes.as_ref());
                if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
                    return Err(lua_error("invalid big number"));
                }
                charge(budget, bytes.len())?;
                Resp::BigNumber(Bytes::copy_from_slice(bytes.as_ref()))
            } else if let Value::Table(verbatim) = table.raw_get::<Value>("verbatim_string")? {
                let format: mlua::String = verbatim.raw_get("format")?;
                let value: mlua::String = verbatim.raw_get("string")?;
                let format: [u8; 3] =
                    format.as_bytes().as_ref().try_into().map_err(|_| {
                        lua_error("verbatim string format must contain three bytes")
                    })?;
                if !format.iter().all(u8::is_ascii_alphabetic) {
                    return Err(lua_error("invalid verbatim string format"));
                }
                charge(budget, value.as_bytes().len())?;
                Resp::Verbatim {
                    format,
                    value: Bytes::copy_from_slice(value.as_bytes().as_ref()),
                }
            } else if let Value::Table(map) = table.raw_get::<Value>("map")? {
                let mut entries = Vec::new();
                for pair in map.pairs::<Value, Value>() {
                    let (key, value) = pair?;
                    entries.push((
                        lua_to_resp(key, depth + 1, budget)?,
                        lua_to_resp(value, depth + 1, budget)?,
                    ));
                }
                Resp::Map(entries)
            } else if let Value::Table(set) = table.raw_get::<Value>("set")? {
                let mut entries = Vec::new();
                for pair in set.pairs::<Value, Value>() {
                    let (key, _) = pair?;
                    entries.push(lua_to_resp(key, depth + 1, budget)?);
                }
                Resp::Set(entries)
            } else {
                let mut entries = Vec::new();
                for index in 1_usize.. {
                    let value = table.raw_get::<Value>(index)?;
                    if matches!(value, Value::Nil) {
                        break;
                    }
                    entries.push(lua_to_resp(value, depth + 1, budget)?);
                }
                Resp::Array(Some(entries))
            }
        }
        _ => Resp::Bulk(None),
    })
}

fn cjson_library(lua: &Lua) -> mlua::Result<Table> {
    let library = lua.create_table()?;
    library.raw_set("null", Value::NULL)?;
    library.raw_set("_NAME", "cjson")?;
    library.raw_set("_VERSION", "2.1.0")?;
    library.raw_set(
        "encode",
        lua.create_function(|lua, value: Value| {
            let mut budget = MAX_VALUE;
            let json = lua_to_json(lua, value, 0, &mut budget)?;
            let bytes = serde_json::to_vec(&json).map_err(mlua::Error::external)?;
            if bytes.len() > MAX_VALUE {
                return Err(lua_error("JSON exceeds 8 MiB limit"));
            }
            lua.create_string(bytes)
        })?,
    )?;
    library.raw_set(
        "decode",
        lua.create_function(|lua, value: mlua::String| {
            if value.as_bytes().len() > MAX_VALUE {
                return Err(lua_error("JSON exceeds 8 MiB limit"));
            }
            let json: serde_json::Value =
                serde_json::from_slice(value.as_bytes().as_ref()).map_err(mlua::Error::external)?;
            json_to_lua(lua, json, 0)
        })?,
    )?;
    Ok(library)
}

fn lua_to_json(
    lua: &Lua,
    value: Value,
    depth: usize,
    budget: &mut usize,
) -> mlua::Result<serde_json::Value> {
    if depth >= MAX_DEPTH {
        return Err(lua_error("JSON nesting limit exceeded"));
    }
    charge(budget, 8)?;
    Ok(match value {
        Value::Nil => serde_json::Value::Null,
        Value::LightUserData(pointer) if pointer.0.is_null() => serde_json::Value::Null,
        Value::Boolean(value) => serde_json::Value::Bool(value),
        Value::Integer(value) => serde_json::Value::Number(value.into()),
        Value::Number(value) => serde_json::Value::Number(
            serde_json::Number::from_f64(value)
                .ok_or_else(|| lua_error("Cannot serialise number: must not be NaN or Infinity"))?,
        ),
        Value::String(value) => {
            charge(budget, value.as_bytes().len())?;
            serde_json::Value::String(
                std::str::from_utf8(value.as_bytes().as_ref())
                    .map_err(mlua::Error::external)?
                    .to_owned(),
            )
        }
        Value::Table(table) => {
            let mut length = 0_usize;
            let mut maximum = 0_usize;
            let mut array = true;
            for pair in table.clone().pairs::<Value, Value>() {
                let (key, _) = pair?;
                length += 1;
                match key {
                    Value::Integer(n) if n > 0 => maximum = maximum.max(n as usize),
                    Value::Number(n) if n > 0.0 && n.fract() == 0.0 && n <= MAX_VALUE as f64 => {
                        maximum = maximum.max(n as usize)
                    }
                    _ => array = false,
                }
            }
            if length != 0 && array && maximum == length {
                let mut values = Vec::with_capacity(length);
                for index in 1..=length {
                    values.push(lua_to_json(lua, table.raw_get(index)?, depth + 1, budget)?);
                }
                serde_json::Value::Array(values)
            } else {
                if array && maximum > length.saturating_mul(2) && maximum > 10 {
                    return Err(lua_error(
                        "Cannot serialise table: excessively sparse array",
                    ));
                }
                if array && length != 0 {
                    let mut values = Vec::with_capacity(maximum);
                    for index in 1..=maximum {
                        values.push(lua_to_json(lua, table.raw_get(index)?, depth + 1, budget)?);
                    }
                    serde_json::Value::Array(values)
                } else {
                    let mut values = serde_json::Map::new();
                    for pair in table.pairs::<Value, Value>() {
                        let (key, value) = pair?;
                        if !matches!(
                            &key,
                            Value::String(_) | Value::Integer(_) | Value::Number(_)
                        ) {
                            return Err(lua_error(
                                "Cannot serialise table: table key must be a number or string",
                            ));
                        }
                        let key = lua
                            .coerce_string(key)?
                            .ok_or_else(|| lua_error("invalid JSON object key"))?;
                        charge(budget, key.as_bytes().len())?;
                        let key = std::str::from_utf8(key.as_bytes().as_ref())
                            .map_err(mlua::Error::external)?
                            .to_owned();
                        values.insert(key, lua_to_json(lua, value, depth + 1, budget)?);
                    }
                    serde_json::Value::Object(values)
                }
            }
        }
        _ => return Err(lua_error("Cannot serialise value: unsupported type")),
    })
}

fn json_to_lua(lua: &Lua, value: serde_json::Value, depth: usize) -> mlua::Result<Value> {
    if depth >= MAX_DEPTH {
        return Err(lua_error("JSON nesting limit exceeded"));
    }
    Ok(match value {
        serde_json::Value::Null => Value::NULL,
        serde_json::Value::Bool(value) => Value::Boolean(value),
        serde_json::Value::Number(value) => Value::Number(
            value
                .as_f64()
                .ok_or_else(|| lua_error("invalid JSON number"))?,
        ),
        serde_json::Value::String(value) => Value::String(lua.create_string(value)?),
        serde_json::Value::Array(values) => {
            let table = lua.create_table_with_capacity(values.len(), 0)?;
            for (index, value) in values.into_iter().enumerate() {
                table.raw_set(index + 1, json_to_lua(lua, value, depth + 1)?)?;
            }
            Value::Table(table)
        }
        serde_json::Value::Object(values) => {
            let table = lua.create_table_with_capacity(0, values.len())?;
            for (key, value) in values {
                table.raw_set(key, json_to_lua(lua, value, depth + 1)?)?;
            }
            Value::Table(table)
        }
    })
}

fn bit_number(lua: &Lua, value: Value) -> mlua::Result<u32> {
    let number = lua
        .coerce_number(value)?
        .ok_or_else(|| lua_error("number expected"))?;
    if !number.is_finite() {
        return Ok(0);
    }
    Ok(number.round_ties_even().rem_euclid(4294967296.0) as u32)
}

fn bit_library(lua: &Lua) -> mlua::Result<Table> {
    let library = lua.create_table()?;
    library.raw_set(
        "tobit",
        lua.create_function(|lua, value: Value| Ok(bit_number(lua, value)? as i32))?,
    )?;
    library.raw_set(
        "bnot",
        lua.create_function(|lua, value: Value| Ok(!bit_number(lua, value)? as i32))?,
    )?;
    library.raw_set(
        "bswap",
        lua.create_function(|lua, value: Value| Ok(bit_number(lua, value)?.swap_bytes() as i32))?,
    )?;
    for operation in ["band", "bor", "bxor"] {
        library.raw_set(
            operation,
            lua.create_function(move |lua, values: MultiValue| {
                if values.is_empty() {
                    return Err(lua_error("at least one number expected"));
                }
                let mut result = if operation == "band" { u32::MAX } else { 0 };
                for value in values {
                    let number = bit_number(lua, value)?;
                    result = match operation {
                        "band" => result & number,
                        "bor" => result | number,
                        _ => result ^ number,
                    };
                }
                Ok(result as i32)
            })?,
        )?;
    }
    for operation in ["lshift", "rshift", "arshift", "rol", "ror"] {
        library.raw_set(
            operation,
            lua.create_function(move |lua, (number, shift): (Value, Value)| {
                let number = bit_number(lua, number)?;
                let shift = bit_number(lua, shift)? & 31;
                Ok(match operation {
                    "lshift" => number.wrapping_shl(shift),
                    "rshift" => number.wrapping_shr(shift),
                    "arshift" => ((number as i32) >> shift) as u32,
                    "rol" => number.rotate_left(shift),
                    _ => number.rotate_right(shift),
                } as i32)
            })?,
        )?;
    }
    library.raw_set(
        "tohex",
        lua.create_function(|lua, (number, digits): (Value, Option<i32>)| {
            let number = bit_number(lua, number)?;
            let digits = digits.unwrap_or(8).clamp(-8, 8);
            let text = if digits < 0 {
                format!("{number:08X}")
            } else {
                format!("{number:08x}")
            };
            Ok(text[8 - digits.unsigned_abs() as usize..].to_owned())
        })?,
    )?;
    Ok(library)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lua_return_budget_counts_container_rows_and_repeated_string_bodies() {
        let lua = Lua::new();
        let table = lua.create_table().unwrap();
        let body = lua.create_string("body").unwrap();
        table.raw_set(1, body.clone()).unwrap();
        table.raw_set(2, body).unwrap();
        let exact = 3 * std::mem::size_of::<Resp>() + 8;
        let mut insufficient = exact - 1;
        assert!(lua_to_resp(Value::Table(table.clone()), 0, &mut insufficient).is_err());
        let mut sufficient = exact;
        assert_eq!(
            lua_to_resp(Value::Table(table), 0, &mut sufficient).unwrap(),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(Bytes::from_static(b"body"))),
                Resp::Bulk(Some(Bytes::from_static(b"body"))),
            ])),
        );
        assert_eq!(sufficient, 0);
    }

    #[test]
    fn fatal_storage_io_survives_both_lua_protection_mechanisms() {
        // A real mutable keyspace: commands before failure take effect, but no
        // command after a failed write may execute even if Lua catches errors.
        let mut runner = ScriptRunner::new().unwrap();
        let mut values = HashMap::<Bytes, Bytes>::new();
        let source = b"redis.call('SET','before','kept'); pcall(function() redis.call('SET','fail','lost') end); redis.pcall('SET','after','forbidden'); return 'swallowed'";
        let args = [
            Bytes::from_static(b"EVAL"),
            Bytes::copy_from_slice(source),
            Bytes::from_static(b"0"),
        ];
        let outcome = runner.eval(&args, |command, _version, _context| {
            assert_eq!(command[0].as_ref(), b"SET");
            if command[1].as_ref() == b"fail" {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "storage write failed",
                ));
            }
            values.insert(command[1].clone(), command[2].clone());
            Ok(Resp::Simple("OK"))
        });
        assert_eq!(outcome.unwrap_err().kind(), io::ErrorKind::WriteZero);
        assert_eq!(values.len(), 1);
        assert_eq!(
            values.get(b"before".as_slice()),
            Some(&Bytes::from_static(b"kept"))
        );
        // Scoped callbacks have released the keyspace and the next invocation
        // can use a different storage borrow without retaining old closures.
        let read = [
            Bytes::from_static(b"EVAL"),
            Bytes::from_static(b"return redis.call('GET',KEYS[1])"),
            Bytes::from_static(b"1"),
            Bytes::from_static(b"before"),
        ];
        let result = runner
            .eval(&read, |command, _version, _context| {
                assert_eq!(command[0].as_ref(), b"GET");
                Ok(Resp::Bulk(values.get(&command[1]).cloned()))
            })
            .unwrap();
        assert_eq!(result, Resp::Bulk(Some(Bytes::from_static(b"kept"))));
    }

    fn argv(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|v| Bytes::copy_from_slice(v.as_bytes()))
            .collect()
    }

    #[test]
    fn library_callbacks_rebind_scoped_bridges_and_preserve_local_helpers() {
        let mut runner = ScriptRunner::new().unwrap();
        let source = "#!lua name=scope\nlocal call = redis.call; local function read(k) return call('GET', k) end; redis.register_function{function_name='read',callback=function(keys) return read(keys[1]) end,flags={'no-writes'},description='reader'}";
        assert_eq!(
            runner.control(&argv(&["FUNCTION", "LOAD", source])),
            Some(Resp::Bulk(Some(Bytes::from_static(b"scope"))))
        );
        for value in ["first", "second"] {
            let borrowed = Bytes::copy_from_slice(value.as_bytes());
            let response = runner
                .eval(
                    &argv(&["FCALL_RO", "read", "1", "k"]),
                    |command, _, context| {
                        assert!(context.readonly);
                        assert_eq!(command, argv(&["GET", "k"]));
                        Ok(Resp::Bulk(Some(borrowed.clone())))
                    },
                )
                .unwrap();
            assert_eq!(response, Resp::Bulk(Some(borrowed)));
        }
    }

    #[test]
    fn durable_function_limit_rejects_load_without_poisoning_existing_functions() {
        let mut runner = ScriptRunner::new().unwrap();
        for index in 0..16 {
            let name = format!("limit_library_{index}");
            let mut source = format!("#!lua name={name}\nredis.register_function('limit_function_{index}',function() return {index} end)\n--").into_bytes();
            source.resize(1024 * 1024, b' ');
            let reply = runner.control(&[
                Bytes::from_static(b"FUNCTION"),
                Bytes::from_static(b"LOAD"),
                Bytes::from(source),
            ]);
            if index < 15 {
                assert_eq!(reply, Some(Resp::Bulk(Some(Bytes::from(name)))));
            } else {
                assert!(matches!(reply, Some(Resp::Error(_))));
            }
        }
        assert_eq!(
            runner
                .eval(
                    &argv(&["FCALL", "limit_function_0", "0"]),
                    |_, _, _| unreachable!()
                )
                .unwrap(),
            Resp::Integer(0)
        );
        assert!(matches!(
            runner
                .eval(
                    &argv(&["FCALL", "limit_function_15", "0"]),
                    |_, _, _| unreachable!()
                )
                .unwrap(),
            Resp::Error(_)
        ));
    }

    #[test]
    fn empty_function_snapshot_roundtrip_removes_loaded_libraries() {
        let mut runner = ScriptRunner::new().unwrap();
        let Some(Resp::Bulk(Some(empty))) = runner.control(&argv(&["FUNCTION", "DUMP"])) else {
            panic!("FUNCTION DUMP must return its portable binary container");
        };
        runner.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=temporary\nredis.register_function('temporary', function() return 1 end)",
        ]));
        assert_eq!(
            runner.control(&[
                Bytes::from_static(b"FUNCTION"),
                Bytes::from_static(b"RESTORE"),
                empty,
                Bytes::from_static(b"FLUSH")
            ]),
            Some(Resp::Simple("OK")),
        );
        assert_eq!(
            runner.control(&argv(&["FUNCTION", "LIST"])),
            Some(Resp::Array(Some(Vec::new())))
        );
    }

    #[test]
    fn function_dump_restore_is_checked_and_collision_failure_is_atomic() {
        let mut runner = ScriptRunner::new().unwrap();
        runner.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=a\nredis.register_function('f',function() return 'a' end)",
        ]));
        let dump = runner.function_snapshot();
        let mut corrupted = dump.to_vec();
        corrupted[1] ^= 1;
        assert!(runner.restore_function_snapshot(&corrupted).is_err());
        runner.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=b\nredis.register_function('g',function() return 'b' end)",
        ]));
        assert!(runner.restore_libraries(&dump, b"APPEND").is_err());
        let reply = runner
            .eval(&argv(&["FCALL", "g", "0"]), |_, _, _| unreachable!())
            .unwrap();
        assert_eq!(reply, Resp::Bulk(Some(Bytes::from_static(b"b"))));
        runner.restore_function_snapshot(&dump).unwrap();
        assert!(matches!(
            runner
                .eval(&argv(&["FCALL", "g", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Error(_)
        ));
    }

    #[test]
    fn actual_function_cancellation_is_ordered_against_first_write() {
        for wrote in [false, true] {
            let mut runner = ScriptRunner::new().unwrap();
            let source = if wrote {
                "#!lua name=kill\nredis.register_function('wait',function() redis.call('SET','key','written'); redis.call('PING'); return 'finished' end)"
            } else {
                "#!lua name=kill\nredis.register_function('wait',function() redis.call('PING'); return 'finished' end)"
            };
            runner.control(&argv(&["FUNCTION", "LOAD", source]));
            let control = runner.execution_control();
            let worker_control = control.clone();
            let (arrived, arrival) = std::sync::mpsc::channel();
            let (release, released) = std::sync::mpsc::channel();
            let worker = std::thread::spawn(move || {
                let mut values = HashMap::<Bytes, Bytes>::new();
                let response = runner
                    .eval(&argv(&["FCALL", "wait", "0"]), |command, _, _| {
                        if command[0].eq_ignore_ascii_case(b"SET") {
                            assert!(worker_control.mark_write());
                            values.insert(command[1].clone(), command[2].clone());
                            return Ok(Resp::Simple("OK"));
                        }
                        arrived.send(()).unwrap();
                        released.recv().unwrap();
                        Ok(Resp::Simple("PONG"))
                    })
                    .unwrap();
                (response, values)
            });
            arrival.recv().unwrap();
            let response = control.control(&argv(&["FUNCTION", "KILL"])).unwrap();
            if wrote {
                assert!(
                    matches!(&response, Resp::Error(message) if message.starts_with("UNKILLABLE "))
                );
            } else {
                assert_eq!(response, Resp::Simple("OK"));
                assert!(!control.mark_write());
            }
            release.send(()).unwrap();
            let (response, values) = worker.join().unwrap();
            assert_eq!(
                values.get(b"key".as_slice()).map(Bytes::as_ref),
                wrote.then_some(b"written".as_slice())
            );
            if wrote {
                assert_eq!(response, Resp::Bulk(Some(Bytes::from_static(b"finished"))));
            } else {
                assert!(matches!(&response, Resp::Error(message) if message.contains("killed")));
            }
            assert!(
                matches!(control.control(&argv(&["FUNCTION","KILL"])), Some(Resp::Error(message)) if message.starts_with("NOTBUSY "))
            );
        }
    }

    #[test]
    fn readonly_admission_and_shebang_flags_are_available_before_execution() {
        let mut runner = ScriptRunner::new().unwrap();
        for (source, allowed) in [
            ("return 42", true),
            ("#!lua\nreturn 42", false),
            ("#!lua flags=no-writes\nreturn 42", true),
        ] {
            let Some(Resp::Bulk(Some(sha))) = runner.control(&argv(&["SCRIPT", "LOAD", source]))
            else {
                panic!("script metadata fixture did not compile");
            };
            for args in [
                argv(&["EVAL_RO", source, "0"]),
                vec![
                    Bytes::from_static(b"EVALSHA_RO"),
                    sha.clone(),
                    Bytes::from_static(b"0"),
                ],
            ] {
                assert_eq!(runner.invocation_context(&args).is_ok(), allowed);
                let response = runner
                    .eval(&args, |_, _, _| panic!("constant script called a command"))
                    .unwrap();
                if allowed {
                    assert_eq!(response, Resp::Integer(42));
                } else {
                    assert_eq!(
                        response,
                        error_reply(
                            "ERR Can not execute a script with write flag using *_ro command."
                        )
                    );
                }
            }
        }
        runner.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=write\nredis.register_function('write',function() return 1 end)",
        ]));
        assert!(
            runner
                .invocation_context(&argv(&["FCALL_RO", "write", "0"]))
                .is_err()
        );
        assert!(matches!(
            runner
                .eval(&argv(&["FCALL_RO", "write", "0"]), |_, _, _| panic!(
                    "write-capable function entered readonly invocation"
                ))
                .unwrap(),
            Resp::Error(_)
        ));
        let source = "#!lua flags=no-writes,allow-stale\nreturn redis.call('GET',KEYS[1])";
        let Some(Resp::Bulk(Some(sha))) = runner.control(&argv(&["SCRIPT", "LOAD", source])) else {
            panic!("valid shebang script did not compile");
        };
        let args = vec![
            Bytes::from_static(b"EVALSHA_RO"),
            sha,
            Bytes::from_static(b"1"),
            Bytes::from_static(b"key"),
        ];
        let context = runner.invocation_context(&args).unwrap();
        assert!(context.readonly && context.flags.no_writes && context.flags.allow_stale);
        assert_eq!(
            runner
                .eval(&args, |_, _, context| {
                    assert!(context.readonly && context.flags.allow_stale);
                    Ok(Resp::Bulk(Some(Bytes::from_static(b"value"))))
                })
                .unwrap(),
            Resp::Bulk(Some(Bytes::from_static(b"value")))
        );
    }

    #[test]
    fn cancellation_and_completion_have_one_atomic_winner() {
        let control = Arc::new(ExecutionControl::default());
        let args = argv(&["EVAL", "return 1", "0"]);
        let guard = control
            .begin(&args, &args[1], false, DebugMode::No)
            .unwrap();
        assert_eq!(
            control.control(&argv(&["SCRIPT", "KILL"])),
            Some(Resp::Simple("OK"))
        );
        assert!(guard.finish());
        drop(guard);
        let guard = control
            .begin(&args, &args[1], false, DebugMode::No)
            .unwrap();
        assert!(!guard.finish());
        assert!(
            matches!(control.control(&argv(&["SCRIPT","KILL"])), Some(Resp::Error(message)) if message.starts_with("NOTBUSY "))
        );
    }

    #[test]
    fn migration_catalog_reconciles_owned_libraries_without_resetting_retained_callbacks() {
        let mut source = ScriptRunner::new().unwrap();
        source.control(&argv(&["FUNCTION","LOAD","#!lua name=counter\nlocal n=0; redis.register_function('counter',function() n=n+1; return n end)"]));
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=old\nredis.register_function('old',function() return 2 end)",
        ]));
        let initial = source.function_snapshot();
        let mut destination = ScriptRunner::new().unwrap();
        destination.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=unrelated\nredis.register_function('unrelated',function() return 3 end)",
        ]));
        destination
            .merge_migration_functions(None, &initial)
            .unwrap();
        assert_eq!(
            destination
                .eval(&argv(&["FCALL", "counter", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Integer(1)
        );
        let initial_revision = destination.function_revision();
        destination
            .merge_migration_functions(None, &initial)
            .unwrap();
        assert_eq!(destination.function_revision(), initial_revision);
        assert_eq!(
            destination
                .eval(&argv(&["FCALL", "counter", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Integer(2)
        );
        source.control(&argv(&["FUNCTION", "DELETE", "old"]));
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=new\nredis.register_function('new',function() return 4 end)",
        ]));
        let updated = source.function_snapshot();
        destination
            .merge_migration_functions(Some(&initial), &updated)
            .unwrap();
        assert_eq!(
            destination
                .eval(&argv(&["FCALL", "counter", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Integer(3)
        );
        assert_eq!(
            destination
                .eval(
                    &argv(&["FCALL", "unrelated", "0"]),
                    |_, _, _| unreachable!()
                )
                .unwrap(),
            Resp::Integer(3)
        );
        assert_eq!(
            destination
                .eval(&argv(&["FCALL", "new", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Integer(4)
        );
        assert!(matches!(
            destination
                .eval(&argv(&["FCALL", "old", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Error(_)
        ));
        let revision = destination.function_revision();
        destination
            .merge_migration_functions(Some(&updated), &updated)
            .unwrap();
        assert_eq!(destination.function_revision(), revision);
        let empty = functions::encode(std::iter::empty::<&[u8]>());
        destination
            .merge_migration_functions(Some(&updated), &empty)
            .unwrap();
        assert_eq!(
            destination
                .eval(
                    &argv(&["FCALL", "unrelated", "0"]),
                    |_, _, _| unreachable!()
                )
                .unwrap(),
            Resp::Integer(3)
        );
        assert!(matches!(
            destination
                .eval(&argv(&["FCALL", "counter", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Error(_)
        ));
    }

    #[test]
    fn migration_collision_or_invalid_snapshot_never_changes_destination_catalog() {
        let mut source = ScriptRunner::new().unwrap();
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=owned\nredis.register_function('owned',function() return 1 end)",
        ]));
        let initial = source.function_snapshot();
        let mut destination = ScriptRunner::new().unwrap();
        destination
            .merge_migration_functions(None, &initial)
            .unwrap();
        destination.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=unrelated\nredis.register_function('unrelated',function() return 3 end)",
        ]));
        let before = destination.function_snapshot();
        let revision = destination.function_revision();
        destination
            .merge_migration_functions(None, &initial)
            .unwrap();
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "REPLACE",
            "#!lua name=owned\nredis.register_function('owned',function() return 99 end)",
        ]));
        assert!(
            destination
                .merge_migration_functions(None, &source.function_snapshot())
                .is_err()
        );
        source.restore_function_snapshot(&initial).unwrap();
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "#!lua name=unrelated\nredis.register_function('new_name',function() return 99 end)",
        ]));
        assert!(
            destination
                .merge_migration_functions(Some(&initial), &source.function_snapshot())
                .is_err()
        );
        source.control(&argv(&["FUNCTION", "DELETE", "unrelated"]));
        source.control(&argv(&[
            "FUNCTION",
            "LOAD",
            "REPLACE",
            "#!lua name=owned\nredis.register_function('unrelated',function() return 99 end)",
        ]));
        assert!(
            destination
                .merge_migration_functions(Some(&initial), &source.function_snapshot())
                .is_err()
        );
        assert!(
            destination
                .merge_migration_functions(Some(b"invalid"), &initial)
                .is_err()
        );
        let mut corrupted = initial.to_vec();
        corrupted[0] ^= 1;
        assert!(
            destination
                .merge_migration_functions(Some(&initial), &corrupted)
                .is_err()
        );
        assert_eq!(destination.function_revision(), revision);
        assert_eq!(destination.function_snapshot(), before);
        assert_eq!(
            destination
                .eval(&argv(&["FCALL", "owned", "0"]), |_, _, _| unreachable!())
                .unwrap(),
            Resp::Integer(1)
        );
        assert_eq!(
            destination
                .eval(
                    &argv(&["FCALL", "unrelated", "0"]),
                    |_, _, _| unreachable!()
                )
                .unwrap(),
            Resp::Integer(3)
        );
    }
}
