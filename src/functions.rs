//! Named Lua libraries and Redis-compatible FUNCTION DUMP containers.
use super::*;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, Default)]
pub struct FunctionFlags {
    pub no_writes: bool,
    pub allow_oom: bool,
    pub allow_stale: bool,
    pub no_cluster: bool,
    pub allow_cross_slot_keys: bool,
}
impl FunctionFlags {
    pub(super) fn parse(names: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let mut result = Self::default();
        for name in names {
            match name.as_str() {
                "no-writes" => result.no_writes = true,
                "allow-oom" => result.allow_oom = true,
                "allow-stale" => result.allow_stale = true,
                "no-cluster" => result.no_cluster = true,
                "allow-cross-slot-keys" => result.allow_cross_slot_keys = true,
                _ => return Err(format!("ERR Unknown flag given: {name}")),
            }
        }
        Ok(result)
    }
    fn reply(self) -> Resp {
        Resp::Array(Some(
            [
                ("no-writes", self.no_writes),
                ("allow-oom", self.allow_oom),
                ("allow-stale", self.allow_stale),
                ("no-cluster", self.no_cluster),
                ("allow-cross-slot-keys", self.allow_cross_slot_keys),
            ]
            .into_iter()
            .filter(|(_, yes)| *yes)
            .map(|(name, _)| bulk(name))
            .collect(),
        ))
    }
}

pub(super) struct RegisteredFunction {
    pub callback: mlua::Function,
    pub flags: FunctionFlags,
    description: Option<Bytes>,
}
pub(super) struct Library {
    pub name: String,
    pub source: Bytes,
    pub functions: BTreeMap<String, RegisteredFunction>,
}
fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}
fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
fn field(name: &'static str, value: Resp) -> (Resp, Resp) {
    (bulk(name), value)
}

fn library_header(source: &[u8]) -> mlua::Result<(String, usize)> {
    if source.len() > MAX_SCRIPT {
        return Err(lua_error("library exceeds 8 MiB limit"));
    }
    let newline = source
        .iter()
        .position(|&b| b == b'\n')
        .unwrap_or(source.len());
    let header = std::str::from_utf8(&source[..newline])
        .map_err(|_| lua_error("Invalid library metadata"))?;
    let mut words = header.split_ascii_whitespace();
    if !words
        .next()
        .is_some_and(|engine| engine.eq_ignore_ascii_case("#!lua"))
    {
        return Err(lua_error("Missing library metadata or unsupported engine"));
    }
    let mut name = None;
    for word in words {
        if let Some(value) = word.strip_prefix("name=") {
            if name.replace(value.to_owned()).is_some() {
                return Err(lua_error("Duplicate library name"));
            }
        } else {
            return Err(lua_error("Invalid metadata value"));
        }
    }
    let name = name.filter(|n| valid_name(n)).ok_or_else(|| {
        lua_error(
            "Library names can only contain letters, numbers, or underscores and must not be empty",
        )
    })?;
    Ok((name, newline))
}

pub(super) fn load(lua: &Lua, source: &[u8]) -> mlua::Result<Library> {
    let (name, newline) = library_header(source)?;
    lua.gc_collect()?;
    let registered = RefCell::new(BTreeMap::new());
    let instructions = Arc::new(AtomicUsize::new(0));
    let count = instructions.clone();
    lua.set_hook(
        HookTriggers {
            on_calls: true,
            every_nth_instruction: Some(1000),
            ..HookTriggers::default()
        },
        move |lua, debug| {
            if (debug.event() == DebugEvent::Count
                && count.fetch_add(1000, Ordering::Relaxed) >= MAX_INSTRUCTIONS)
                || lua.inspect_stack(MAX_DEPTH, |_| ()).is_some()
            {
                Err(lua_error("library instruction or recursion limit exceeded"))
            } else {
                Ok(VmState::Continue)
            }
        },
    )?;
    let result = lua.scope(|scope| {
        let env = environment(lua)?;
        let redis = redis_library(lua)?;
        // Permanent forwarding functions never retain an invocation's borrowed
        // bridge. Registry targets exist only inside the corresponding Lua scope.
        for (name, registry) in [("call", "ffkv_call"), ("pcall", "ffkv_pcall"), ("setresp", "ffkv_setresp")] {
            redis.raw_set(name, lua.create_function(move |lua, values: MultiValue| {
                let target = lua.named_registry_value::<mlua::Function>(registry)
                    .map_err(|_| lua_error("Redis API is unavailable during library loading"))?;
                target.call::<MultiValue>(values)
            })?)?;
        }
        let register = scope.create_function(|_, values: MultiValue| {
            let values = values.into_vec();
            let (name, callback, description, flags) = match values.as_slice() {
                [Value::String(name), Value::Function(callback)] =>
                    (name.to_str()?.to_owned(), callback.clone(), None, FunctionFlags::default()),
                [Value::Table(options)] => {
                    for pair in options.clone().pairs::<Value, Value>() {
                        let (key, _) = pair?;
                        let Value::String(key) = key else { return Err(lua_error("Invalid named argument")); };
                        if !["function_name", "callback", "description", "flags"].contains(&key.to_str()?.as_ref()) {
                            return Err(lua_error("Unknown named argument"));
                        }
                    }
                    let name: mlua::String = options.raw_get("function_name")?;
                    let callback: mlua::Function = options.raw_get("callback")?;
                    let description: Option<mlua::String> = options.raw_get("description")?;
                    let flag_table: Option<Table> = options.raw_get("flags")?;
                    let mut names = Vec::new();
                    if let Some(table) = flag_table {
                        let len = table.raw_len();
                        let mut count = 0;
                        for pair in table.clone().pairs::<usize, mlua::String>() {
                            let (index, _) = pair?;
                            if index == 0 || index > len { return Err(lua_error("flags must be an array of strings")); }
                            count += 1;
                        }
                        if count != len { return Err(lua_error("flags must be an array of strings")); }
                        for name in table.sequence_values::<mlua::String>() { names.push(name?.to_str()?.to_owned()); }
                    }
                    (name.to_str()?.to_owned(), callback, description.map(|v| Bytes::copy_from_slice(v.as_bytes().as_ref())), FunctionFlags::parse(names).map_err(lua_error)?)
                }
                _ => return Err(lua_error("redis.register_function expects a name and callback, or one named-argument table")),
            };
            if !valid_name(&name) { return Err(lua_error("Invalid function name")); }
            let mut functions = registered.borrow_mut();
            if functions.contains_key(&name) { return Err(lua_error("Function already exists in the library")); }
            if functions.len() >= 4096 { return Err(lua_error("too many functions in library")); }
            functions.insert(name, RegisteredFunction { callback, flags, description });
            Ok(())
        })?;
        redis.raw_set("register_function", register)?;
        env.raw_set("redis", redis.clone())?;
        let metatable = lua.create_table()?;
        metatable.raw_set("__newindex", lua.create_function(|_, (_, key, _): (Table, Value, Value)| -> mlua::Result<()> {
            Err(lua_error(format!("Script attempted to create global variable {key:?}")))
        })?)?;
        metatable.raw_set("__index", lua.create_function(|_, (_, key): (Table, Value)| -> mlua::Result<Value> {
            Err(lua_error(format!("Script attempted to access nonexistent global variable {key:?}")))
        })?)?;
        metatable.raw_set("__metatable", false)?;
        env.set_metatable(Some(metatable))?;
        let body = source.get(newline.saturating_add(1)..).unwrap_or_default();
        let result = lua.load(body).set_name(format!("@{name}")).set_mode(ChunkMode::Text).set_environment(env).exec();
        redis.raw_set("register_function", Value::Nil)?;
        result
    });
    lua.remove_hook();
    result?;
    if instructions.load(Ordering::Relaxed) >= MAX_INSTRUCTIONS {
        return Err(lua_error("library instruction limit exceeded"));
    }
    let functions = registered.into_inner();
    if functions.is_empty() {
        return Err(lua_error("No functions registered"));
    }
    Ok(Library {
        name,
        source: Bytes::copy_from_slice(source),
        functions,
    })
}

pub(super) fn list(libraries: &BTreeMap<String, Library>, args: &[Bytes]) -> Resp {
    let mut pattern = None;
    let mut with_code = false;
    let mut at = 2;
    while at < args.len() {
        if args[at].eq_ignore_ascii_case(b"LIBRARYNAME") && pattern.is_none() && at + 1 < args.len()
        {
            pattern = Some(&args[at + 1][..]);
            at += 2;
        } else if args[at].eq_ignore_ascii_case(b"WITHCODE") && !with_code {
            with_code = true;
            at += 1;
        } else {
            return error_reply("ERR syntax error");
        }
    }
    crate::engine::bounded_replies(
        libraries
            .values()
            .filter(|library| pattern.is_none_or(|p| glob(p, library.name.as_bytes())))
            .map(|library| {
                let functions = library
                    .functions
                    .iter()
                    .map(|(name, f)| {
                        Resp::Map(vec![
                            field("name", bulk(Bytes::copy_from_slice(name.as_bytes()))),
                            field("description", Resp::Bulk(f.description.clone())),
                            field("flags", f.flags.reply()),
                        ])
                    })
                    .collect();
                let mut fields = vec![
                    field(
                        "library_name",
                        bulk(Bytes::copy_from_slice(library.name.as_bytes())),
                    ),
                    field("engine", bulk("LUA")),
                    field("functions", Resp::Array(Some(functions))),
                ];
                if with_code {
                    fields.push(field("library_code", bulk(library.source.clone())));
                }
                Resp::Map(fields)
            }),
        MAX_VALUE,
    )
}

// Redis glob matching, including character classes and backslash escapes. The
// iterative star fallback bounds auxiliary space independently of input size.
fn glob(pattern: &[u8], text: &[u8]) -> bool {
    fn atom(p: &[u8], byte: u8) -> Option<(usize, bool)> {
        match *p.first()? {
            b'?' => Some((1, true)),
            b'\\' if p.len() > 1 => Some((2, p[1] == byte)),
            b'[' => {
                let mut i = 1;
                let negate = p.get(i) == Some(&b'^');
                if negate {
                    i += 1;
                }
                let mut matched = false;
                while i < p.len() && p[i] != b']' {
                    let first = if p[i] == b'\\' && i + 1 < p.len() {
                        i += 1;
                        p[i]
                    } else {
                        p[i]
                    };
                    i += 1;
                    if p.get(i) == Some(&b'-') && p.get(i + 1).is_some_and(|&b| b != b']') {
                        let end = p[i + 1];
                        matched |= first.min(end) <= byte && byte <= first.max(end);
                        i += 2;
                    } else {
                        matched |= first == byte;
                    }
                }
                if p.get(i) == Some(&b']') {
                    Some((i + 1, matched != negate))
                } else {
                    Some((1, byte == b'['))
                }
            }
            literal => Some((1, literal == byte)),
        }
    }
    let (mut p, mut t, mut star) = (0, 0, None);
    while t < text.len() {
        if pattern.get(p) == Some(&b'*') {
            p += 1;
            star = Some((p, t));
            continue;
        }
        if let Some((size, true)) = atom(&pattern[p..], text[t]) {
            p += size;
            t += 1;
            continue;
        }
        if let Some((after, previous)) = star {
            t = previous + 1;
            p = after;
            star = Some((after, t));
        } else {
            return false;
        }
    }
    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p == pattern.len()
}

pub(super) fn encode<'a>(sources: impl IntoIterator<Item = &'a [u8]>) -> Bytes {
    let mut out = Vec::new();
    for source in sources {
        out.push(245);
        // Vec's Write implementation is infallible apart from allocation failure.
        crate::rdb::write_string(&mut out, source).expect("writing function to Vec");
    }
    crate::rdb::append_footer(out)
}
pub(super) fn decode(payload: &[u8]) -> Result<Vec<Bytes>, String> {
    if payload.len() < 10 || payload.len() > MAX_CACHE_BYTES + 65536 {
        return Err("ERR Invalid function payload length".into());
    }
    let body = crate::rdb::verify_footer(payload).map_err(|e| format!("ERR {e}"))?;
    let mut input = crate::rdb::Decoder::new(body, MAX_CACHE_BYTES);
    let mut sources = Vec::new();
    let mut total = 0;
    while input.remaining() != 0 {
        if input.byte().map_err(|e| format!("ERR {e}"))? != 245 {
            return Err("ERR given type is not a function".into());
        }
        let source = input.string().map_err(|e| format!("ERR {e}"))?;
        if source.len() > MAX_SCRIPT {
            return Err("ERR library exceeds 8 MiB limit".into());
        }
        total += source.len();
        if total > MAX_CACHE_BYTES || sources.len() >= MAX_CACHE_ENTRIES {
            return Err("ERR functions exceed library limit".into());
        }
        sources.push(source);
    }
    Ok(sources)
}

impl ScriptRunner {
    pub fn execution_control(&self) -> Arc<ExecutionControl> {
        self.execution.clone()
    }
    pub fn set_debug_mode(&mut self, mode: DebugMode) {
        self.debug_mode = mode;
    }
    pub fn parse_debug_mode(value: &[u8]) -> Option<DebugMode> {
        if value.eq_ignore_ascii_case(b"YES") {
            Some(DebugMode::Yes)
        } else if value.eq_ignore_ascii_case(b"SYNC") {
            Some(DebugMode::Sync)
        } else if value.eq_ignore_ascii_case(b"NO") {
            Some(DebugMode::No)
        } else {
            None
        }
    }
    pub fn function_revision(&self) -> u64 {
        self.function_revision
    }
    pub fn function_sources(&self) -> Vec<Bytes> {
        self.libraries
            .values()
            .map(|library| library.source.clone())
            .collect()
    }
    pub fn function_snapshot(&self) -> Bytes {
        encode(
            self.libraries
                .values()
                .map(|library| library.source.as_ref()),
        )
    }
    pub fn restore_function_snapshot(&mut self, payload: &[u8]) -> Result<(), String> {
        self.restore_libraries(payload, b"FLUSH")
    }
    /// Reconcile one migration source without replacing unrelated destination
    /// libraries. `previous_source` is the last successfully installed source
    /// snapshot, not the destination's full catalog.
    /// Identical definitions may already exist after an earlier slot migration;
    /// reuse them, but reject incompatible newly introduced library names.
    pub fn merge_migration_functions(
        &mut self,
        previous_source: Option<&[u8]>,
        source: &[u8],
    ) -> Result<(), String> {
        let mut owned = std::collections::HashSet::new();
        if let Some(previous) = previous_source {
            for source in decode(previous)? {
                let (name, _) = library_header(&source).map_err(|e| format!("ERR {e}"))?;
                if !owned.insert(name) {
                    return Err("ERR Duplicate library in previous source payload".into());
                }
            }
        }
        let mut seen = std::collections::HashSet::new();
        let mut incoming = BTreeMap::new();
        for source in decode(source)? {
            let (name, _) = library_header(&source).map_err(|e| format!("ERR {e}"))?;
            if !seen.insert(name.clone()) {
                return Err("ERR Duplicate library in source payload".into());
            }
            if self
                .libraries
                .get(&name)
                .is_some_and(|library| library.source == source)
            {
                // Identical definitions, including those from a completed
                // migration, retain their callbacks and private Lua helpers.
                owned.remove(&name);
                continue;
            }
            let library = load(&self.lua, &source).map_err(|e| format!("ERR {e}"))?;
            incoming.insert(name, library);
        }
        validate_library_set(
            &incoming,
            self.libraries
                .iter()
                .filter(|(name, _)| !owned.contains(*name)),
        )?;
        if owned.iter().any(|name| self.libraries.contains_key(name)) || !incoming.is_empty() {
            self.libraries.retain(|name, _| !owned.contains(name));
            self.libraries.append(&mut incoming);
            self.changed_functions();
        }
        Ok(())
    }
    pub fn invocation_context(&self, args: &[Bytes]) -> Result<InvocationContext, Resp> {
        if args.len() < 3 {
            return Err(error_reply("ERR wrong number of arguments"));
        }
        let function =
            args[0].eq_ignore_ascii_case(b"FCALL") || args[0].eq_ignore_ascii_case(b"FCALL_RO");
        let mut declared_flags = function;
        let flags = if function {
            let name = std::str::from_utf8(&args[1]).unwrap_or("");
            self.libraries
                .values()
                .find_map(|library| {
                    library
                        .functions
                        .iter()
                        .find(|(n, _)| n.eq_ignore_ascii_case(name))
                        .map(|(_, f)| f.flags)
                })
                .ok_or_else(|| error_reply("ERR Function not found"))?
        } else {
            let body = if args[0].eq_ignore_ascii_case(b"EVALSHA")
                || args[0].eq_ignore_ascii_case(b"EVALSHA_RO")
            {
                self.scripts
                    .get(&String::from_utf8_lossy(&args[1]).to_ascii_lowercase())
                    .ok_or_else(|| error_reply("NOSCRIPT No matching script. Please use EVAL."))?
            } else {
                &args[1]
            };
            declared_flags = body.starts_with(b"#!");
            script_metadata(body).map_err(|e| error_reply(&e))?.1
        };
        let readonly = args[0].eq_ignore_ascii_case(b"EVAL_RO")
            || args[0].eq_ignore_ascii_case(b"EVALSHA_RO")
            || args[0].eq_ignore_ascii_case(b"FCALL_RO");
        if declared_flags && readonly && !flags.no_writes {
            return Err(error_reply(
                "ERR Can not execute a script with write flag using *_ro command.",
            ));
        }
        Ok(InvocationContext {
            readonly: readonly || flags.no_writes,
            flags,
            rollback: !function && self.debug_mode == DebugMode::Yes,
        })
    }
    fn changed_functions(&mut self) {
        self.function_revision = self.function_revision.wrapping_add(1);
        self.execution.counts(
            self.libraries.len(),
            self.libraries
                .values()
                .map(|library| library.functions.len())
                .sum(),
        );
        let _ = self.lua.gc_collect();
    }
    fn validate_libraries(
        &self,
        incoming: &BTreeMap<String, Library>,
        replace: bool,
        flush: bool,
    ) -> Result<(), String> {
        validate_library_set(
            incoming,
            self.libraries
                .iter()
                .filter(|(name, _)| !flush && !(replace && incoming.contains_key(*name))),
        )
    }
}

struct EncodedLength(usize);
impl std::io::Write for EncodedLength {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 += bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
impl EncodedLength {
    fn library(&mut self, source: &[u8]) {
        self.0 += 1; // FUNCTION library opcode.
        crate::rdb::write_string(self, source).expect("counting serialized library bytes");
    }
}

fn validate_library_set<'a>(
    incoming: &BTreeMap<String, Library>,
    retained: impl Iterator<Item = (&'a String, &'a Library)>,
) -> Result<(), String> {
    let mut names = std::collections::HashSet::new();
    let mut bytes = 0;
    let mut count = 0;
    let mut encoded = EncodedLength(10); // Version and checksum footer.
    for (name, library) in retained {
        if incoming.contains_key(name) {
            return Err(format!("ERR Library '{name}' already exists"));
        }
        bytes += library.source.len();
        count += 1;
        encoded.library(&library.source);
        for name in library.functions.keys() {
            names.insert(name.to_ascii_lowercase());
        }
    }
    for library in incoming.values() {
        bytes += library.source.len();
        count += 1;
        encoded.library(&library.source);
        for name in library.functions.keys() {
            if !names.insert(name.to_ascii_lowercase()) {
                return Err(format!("ERR Function {name} already exists"));
            }
        }
    }
    if bytes > MAX_CACHE_BYTES || count > MAX_CACHE_ENTRIES {
        return Err("ERR functions exceed library limit".into());
    }
    if encoded.0 > crate::wal::MAX_FUNCTION_SNAPSHOT_BYTES {
        return Err("ERR functions exceed durable library limit".into());
    }
    Ok(())
}

impl ScriptRunner {
    pub(super) fn restore_libraries(
        &mut self,
        payload: &[u8],
        policy: &[u8],
    ) -> Result<(), String> {
        let flush = policy.eq_ignore_ascii_case(b"FLUSH");
        let replace = policy.eq_ignore_ascii_case(b"REPLACE");
        if !flush && !replace && !policy.eq_ignore_ascii_case(b"APPEND") {
            return Err("ERR Wrong restore policy given".into());
        }
        let sources = decode(payload)?;
        let mut incoming = BTreeMap::new();
        for source in sources {
            let library = load(&self.lua, &source).map_err(|e| format!("ERR {e}"))?;
            let name = library.name.clone();
            if incoming.insert(name, library).is_some() {
                return Err("ERR Duplicate library in payload".into());
            }
        }
        self.validate_libraries(&incoming, replace, flush)?;
        if flush {
            self.libraries.clear();
        }
        self.libraries.append(&mut incoming);
        self.changed_functions();
        Ok(())
    }
    pub(super) fn function_control(&mut self, args: &[Bytes]) -> Resp {
        let Some(subcommand) = args.get(1) else {
            return error_reply("ERR wrong number of arguments for FUNCTION");
        };
        if subcommand.eq_ignore_ascii_case(b"LOAD") {
            let (source, replace) = match args {
                [_, _, source] => (source, false),
                [_, _, option, source] if option.eq_ignore_ascii_case(b"REPLACE") => (source, true),
                _ => return error_reply("ERR wrong number of arguments for FUNCTION LOAD"),
            };
            let library = match load(&self.lua, source) {
                Ok(library) => library,
                Err(e) => return runtime_reply(e),
            };
            let name = library.name.clone();
            let mut incoming = BTreeMap::new();
            incoming.insert(name.clone(), library);
            if let Err(error) = self.validate_libraries(&incoming, replace, false) {
                return error_reply(&error);
            }
            self.libraries.append(&mut incoming);
            self.changed_functions();
            bulk(name)
        } else if subcommand.eq_ignore_ascii_case(b"DELETE") {
            if args.len() != 3 {
                return error_reply("ERR wrong number of arguments for FUNCTION DELETE");
            }
            let name = std::str::from_utf8(&args[2]).unwrap_or("");
            if self.libraries.remove(name).is_none() {
                return error_reply("ERR Library not found");
            }
            self.changed_functions();
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"FLUSH") {
            if args.len() > 3
                || (args.len() == 3
                    && !args[2].eq_ignore_ascii_case(b"SYNC")
                    && !args[2].eq_ignore_ascii_case(b"ASYNC"))
            {
                return error_reply("ERR FUNCTION FLUSH only supports SYNC|ASYNC");
            }
            self.libraries.clear();
            self.changed_functions();
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"LIST") {
            list(&self.libraries, args)
        } else if subcommand.eq_ignore_ascii_case(b"DUMP") && args.len() == 2 {
            Resp::Bulk(Some(self.function_snapshot()))
        } else if subcommand.eq_ignore_ascii_case(b"RESTORE") && (3..=4).contains(&args.len()) {
            match self.restore_libraries(
                &args[2],
                args.get(3).map_or(b"APPEND".as_slice(), |v| v.as_ref()),
            ) {
                Ok(()) => Resp::Simple("OK"),
                Err(e) => error_reply(&e),
            }
        } else {
            error_reply("ERR unknown FUNCTION subcommand or wrong number of arguments")
        }
    }
}

pub(super) fn script_metadata(body: &[u8]) -> Result<(&[u8], FunctionFlags), String> {
    if !body.starts_with(b"#!") {
        return Ok((body, FunctionFlags::default()));
    }
    let newline = body.iter().position(|&b| b == b'\n').unwrap_or(body.len());
    let header =
        std::str::from_utf8(&body[..newline]).map_err(|_| "ERR Invalid script metadata")?;
    let mut words = header.split_ascii_whitespace();
    if words.next() != Some("#!lua") {
        return Err("ERR Unexpected engine in script shebang".into());
    }
    let mut names = Vec::new();
    for word in words {
        let value = word
            .strip_prefix("flags=")
            .ok_or("ERR Invalid script metadata")?;
        names.extend(
            value
                .split(',')
                .filter(|v| !v.is_empty())
                .map(str::to_owned),
        );
    }
    Ok((
        body.get(newline.saturating_add(1)..).unwrap_or_default(),
        FunctionFlags::parse(names)?,
    ))
}
