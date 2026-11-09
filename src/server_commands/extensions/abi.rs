//! Independently implemented subset of the public Redis Modules API v1.
//! No native datatypes, threads/blocked clients, timers, events, RDB/AOF module
//! callbacks, cluster messages, server-global config, or shared APIs are exported.
//! Call accepts c/s/b/l/v argument formats and !/E flags (all accepted engine
//! writes are always journaled). Unsupported formats fail with EINVAL.
//! Native heap allocation is system-backed and is not included in the engine's
//! keyspace memory limit/accounting. Config support is load-time immutable
//! string configuration only; CONFIG GET/SET/REWRITE integration is not exported.
use super::{CommandMetadata, Engine, Resp, err};
use bytes::Bytes;
use std::{
    cell::RefCell,
    collections::HashSet,
    ffi::{CStr, CString, c_char, c_int, c_void},
    ptr,
    sync::Arc,
};
pub type CommandFn = unsafe extern "C" fn(*mut Context, *mut *mut ModuleString, c_int) -> c_int;
pub struct Command {
    pub metadata: CommandMetadata,
    pub callback: CommandFn,
}
pub struct ModuleString {
    value: Bytes,
}
struct Frame {
    length: Option<usize>,
    values: Vec<Resp>,
}
type Dispatcher = dyn FnMut(&[Bytes]) -> Resp;
thread_local! { static LOADING_NAMES:RefCell<Vec<HashSet<String>>>=const { RefCell::new(Vec::new()) }; }
#[repr(C)]
pub struct Context {
    // RedisModule_Init obtains GetApi from the first machine word of ctx.
    get_api: unsafe extern "C" fn(*const c_char, *mut c_void) -> c_int,
    pub attributes: Option<(String, i64)>,
    #[expect(
        clippy::vec_box,
        reason = "Native command handles must keep stable addresses while registration grows this vector."
    )]
    pub commands: Vec<Box<Command>>,
    pub failure: Option<String>,
    pub config_loaded: bool,
    config: Vec<(Bytes, Bytes)>,
    configs: Vec<StringConfig>,
    busy: HashSet<String>,
    reserved: HashSet<String>,
    dispatch: Option<*mut Dispatcher>,
    frames: Vec<Frame>,
    replies: Vec<Resp>,
    auto: bool,
    strings: HashSet<*mut ModuleString>,
    calls: HashSet<*mut CallReply>,
    keys: HashSet<*mut ModuleKey>,
    loading: bool,
}
impl Context {
    pub fn lifecycle() -> Self {
        Self {
            get_api,
            attributes: None,
            commands: Vec::new(),
            failure: None,
            config_loaded: false,
            config: Vec::new(),
            configs: Vec::new(),
            busy: HashSet::new(),
            reserved: HashSet::new(),
            dispatch: None,
            frames: Vec::new(),
            replies: Vec::new(),
            auto: false,
            strings: HashSet::new(),
            calls: HashSet::new(),
            keys: HashSet::new(),
            loading: false,
        }
    }
    pub fn loading(
        config: &[(Bytes, Bytes)],
        busy: HashSet<String>,
        reserved: HashSet<String>,
    ) -> Self {
        LOADING_NAMES.with(|names| names.borrow_mut().push(busy.clone()));
        let mut ctx = Self::lifecycle();
        ctx.config = config.to_vec();
        ctx.busy = busy;
        ctx.reserved = reserved;
        ctx.loading = true;
        ctx
    }
    pub fn invocation(call: &mut dyn FnMut(&[Bytes]) -> Resp) -> Self {
        // The dispatcher is used only synchronously while this Context exists.
        let dispatch = unsafe {
            std::mem::transmute::<&mut dyn FnMut(&[Bytes]) -> Resp, *mut Dispatcher>(call)
        };
        let mut ctx = Self::lifecycle();
        ctx.dispatch = Some(dispatch);
        ctx
    }
    fn call(&mut self, args: &[Bytes]) -> Resp {
        match self.dispatch {
            Some(p) => unsafe { (&mut *p)(args) },
            None => err("engine access is only available during a module command"),
        }
    }
    fn push(&mut self, reply: Resp) {
        if let Some(frame) = self.frames.last_mut() {
            frame.values.push(reply);
        } else {
            self.replies.push(reply);
        }
        loop {
            let done = self
                .frames
                .last()
                .is_some_and(|f| f.length == Some(f.values.len()));
            if !done {
                break;
            }
            let frame = self.frames.pop().unwrap();
            let reply = Resp::Array(Some(frame.values));
            if let Some(parent) = self.frames.last_mut() {
                parent.values.push(reply);
            } else {
                self.replies.push(reply);
            }
        }
    }
    pub fn finish(mut self, status: c_int) -> Resp {
        if !self.frames.is_empty() || self.replies.len() > 1 {
            return err("module produced an incomplete or multiple reply");
        }
        self.replies.pop().unwrap_or_else(|| {
            if status == 0 {
                Resp::Bulk(None)
            } else {
                err("module command failed")
            }
        })
    }
}
impl Drop for Context {
    fn drop(&mut self) {
        unsafe {
            if self.loading {
                LOADING_NAMES.with(|names| {
                    names.borrow_mut().pop();
                });
            }
            for key in self.keys.drain() {
                drop(Box::from_raw(key));
            }
            for reply in self.calls.drain() {
                drop(Box::from_raw(reply));
            }
            for string in self.strings.drain() {
                drop(Arc::from_raw(string));
            }
        }
    }
}
pub fn argv(args: &[Bytes]) -> (Vec<Arc<ModuleString>>, Vec<*mut ModuleString>) {
    let values: Vec<_> = args
        .iter()
        .map(|v| Arc::new(ModuleString { value: v.clone() }))
        .collect();
    let pointers = values.iter().map(|v| Arc::as_ptr(v).cast_mut()).collect();
    (values, pointers)
}
unsafe fn text<'a>(p: *const c_char) -> &'a str {
    unsafe { CStr::from_ptr(p) }.to_str().unwrap_or("")
}
unsafe fn bytes<'a>(p: *const c_char, len: usize) -> &'a [u8] {
    if len == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(p.cast(), len) }
    }
}
unsafe extern "C" fn set_attributes(
    ctx: *mut Context,
    name: *const c_char,
    ver: c_int,
    api: c_int,
) {
    let ctx = unsafe { &mut *ctx };
    let name = unsafe { text(name) };
    if api != 1
        || ver < 0
        || name.is_empty()
        || name.bytes().any(|b| b.is_ascii_whitespace() || b == 0)
        || ctx.busy.contains(name)
        || ctx.attributes.is_some()
    {
        ctx.failure = Some("invalid, duplicate, or unsupported module attributes".into());
    } else {
        ctx.attributes = Some((name.to_owned(), ver as i64));
    }
}
unsafe extern "C" fn name_busy(name: *const c_char) -> c_int {
    let name = unsafe { text(name) };
    LOADING_NAMES.with(|names| {
        i32::from(
            names
                .borrow()
                .last()
                .is_some_and(|names| names.contains(name)),
        )
    })
}
unsafe extern "C" fn create_command(
    ctx: *mut Context,
    name: *const c_char,
    callback: Option<CommandFn>,
    flags: *const c_char,
    first: c_int,
    last: c_int,
    step: c_int,
) -> c_int {
    let ctx = unsafe { &mut *ctx };
    let name = unsafe { text(name) }.to_ascii_lowercase();
    let Some(callback) = callback else {
        return 1;
    };
    if name.is_empty()
        || name.bytes().any(|b| b.is_ascii_whitespace())
        || Engine::command_exists(name.as_bytes())
        || ctx.reserved.contains(&name)
        || ctx.commands.iter().any(|c| c.metadata.name == name)
        || super::reserved_command(&name)
        || first < 0
        || (first > 0 && (step <= 0 || last == 0))
    {
        return 1;
    }
    let mut supported = vec!["module"];
    let mut categories = vec!["module"];
    for flag in unsafe { text(flags) }.split_ascii_whitespace() {
        let known = match flag {
            "write" => "write",
            "readonly" => "readonly",
            "admin" => "admin",
            "deny-oom" => "denyoom",
            "fast" => "fast",
            "noscript" => "noscript",
            // Movable keys require the GetCommandKeys callback protocol, which
            // is deliberately not exported by this ABI subset.
            _ => return 1,
        };
        if !supported.contains(&known) {
            supported.push(known);
        }
    }
    if supported.contains(&"write") && supported.contains(&"readonly") {
        return 1;
    }
    if supported.contains(&"write") {
        categories.push("write");
    }
    if supported.contains(&"readonly") {
        categories.push("read");
    }
    if supported.contains(&"admin") {
        categories.extend(["admin", "dangerous"]);
    }
    categories.push(if supported.contains(&"fast") {
        "fast"
    } else {
        "slow"
    });
    ctx.commands.push(Box::new(Command {
        metadata: CommandMetadata {
            name,
            module_name: ctx
                .attributes
                .as_ref()
                .map_or_else(String::new, |(name, _)| name.clone()),
            arity: -1,
            flags: supported,
            categories,
            first_key: first as i64,
            last_key: last as i64,
            key_step: step as i64,
        },
        callback,
    }));
    0
}
unsafe extern "C" fn get_command(ctx: *mut Context, name: *const c_char) -> *mut Command {
    unsafe { &mut *ctx }
        .commands
        .iter_mut()
        .find(|c| c.metadata.name.eq_ignore_ascii_case(unsafe { text(name) }))
        .map_or(ptr::null_mut(), |c| &mut **c)
}
#[repr(C)]
struct InfoVersion {
    version: c_int,
    history: usize,
    keys: usize,
    args: usize,
}
#[repr(C)]
struct CommandInfo {
    version: *const InfoVersion,
    summary: *const c_char,
    complexity: *const c_char,
    since: *const c_char,
    history: *const c_void,
    tips: *const c_char,
    arity: c_int,
    keys: *const c_void,
    args: *const c_void,
}
unsafe extern "C" fn set_command_info(command: *mut Command, info: *const CommandInfo) -> c_int {
    if command.is_null() || info.is_null() {
        return 1;
    }
    let info = unsafe { &*info };
    if info.version.is_null()
        || unsafe { (*info.version).version } != 1
        || info.arity == 0
        || !info.keys.is_null()
        || !info.args.is_null()
    {
        return 1;
    }
    unsafe {
        (*command).metadata.arity = info.arity as i64;
    }
    0
}
unsafe extern "C" fn create_string(
    ctx: *mut Context,
    value: *const c_char,
    len: usize,
) -> *mut ModuleString {
    let p = Arc::into_raw(Arc::new(ModuleString {
        value: Bytes::copy_from_slice(unsafe { bytes(value, len) }),
    }))
    .cast_mut();
    if !ctx.is_null() && unsafe { (*ctx).auto } {
        unsafe {
            (*ctx).strings.insert(p);
        }
    }
    p
}
unsafe extern "C" fn create_string_from_string(
    ctx: *mut Context,
    value: *mut ModuleString,
) -> *mut ModuleString {
    unsafe { create_string(ctx, (*value).value.as_ptr().cast(), (*value).value.len()) }
}
unsafe extern "C" fn create_string_ll(ctx: *mut Context, value: i64) -> *mut ModuleString {
    let value = value.to_string();
    unsafe { create_string(ctx, value.as_ptr().cast(), value.len()) }
}
unsafe extern "C" fn free_string(ctx: *mut Context, value: *mut ModuleString) {
    if value.is_null() {
        return;
    }
    if !ctx.is_null() {
        unsafe {
            (*ctx).strings.remove(&value);
        }
    }
    unsafe {
        drop(Arc::from_raw(value));
    }
}
unsafe extern "C" fn retain_string(_ctx: *mut Context, value: *mut ModuleString) {
    unsafe {
        Arc::increment_strong_count(value);
    }
}
unsafe extern "C" fn string_ptr(value: *const ModuleString, len: *mut usize) -> *const c_char {
    if value.is_null() {
        if !len.is_null() {
            unsafe {
                *len = 0;
            }
        }
        return ptr::null();
    }
    let v = unsafe { &(*value).value };
    if !len.is_null() {
        unsafe {
            *len = v.len();
        }
    }
    v.as_ptr().cast()
}
unsafe extern "C" fn string_ll(value: *const ModuleString, out: *mut i64) -> c_int {
    let value = unsafe { &(*value).value };
    match std::str::from_utf8(value).ok().and_then(|v| v.parse().ok()) {
        Some(v) => {
            unsafe {
                *out = v;
            }
            0
        }
        None => 1,
    }
}
unsafe extern "C" fn string_double(value: *const ModuleString, out: *mut f64) -> c_int {
    match std::str::from_utf8(unsafe { &(*value).value })
        .ok()
        .and_then(|v| v.parse().ok())
    {
        Some(v) => {
            unsafe {
                *out = v;
            }
            0
        }
        None => 1,
    }
}
unsafe extern "C" fn string_compare(a: *const ModuleString, b: *const ModuleString) -> c_int {
    use std::cmp::Ordering;
    match unsafe { (*a).value.cmp(&(*b).value) } {
        Ordering::Less => -1,
        Ordering::Equal => 0,
        Ordering::Greater => 1,
    }
}
unsafe extern "C" fn auto_memory(ctx: *mut Context) {
    unsafe {
        (*ctx).auto = true;
    }
}
unsafe extern "C" fn wrong_arity(ctx: *mut Context) -> c_int {
    unsafe {
        (*ctx).push(err("wrong number of arguments"));
    }
    0
}
unsafe extern "C" fn reply_integer(ctx: *mut Context, n: i64) -> c_int {
    unsafe {
        (*ctx).push(Resp::Integer(n));
    }
    0
}
unsafe extern "C" fn reply_double(ctx: *mut Context, n: f64) -> c_int {
    unsafe {
        (*ctx).push(Resp::Double(n));
    }
    0
}
unsafe extern "C" fn reply_null(ctx: *mut Context) -> c_int {
    unsafe {
        (*ctx).push(Resp::Bulk(None));
    }
    0
}
unsafe extern "C" fn reply_buffer(ctx: *mut Context, p: *const c_char, len: usize) -> c_int {
    unsafe {
        (*ctx).push(Resp::Bulk(Some(Bytes::copy_from_slice(bytes(p, len)))));
    }
    0
}
unsafe extern "C" fn reply_string(ctx: *mut Context, p: *mut ModuleString) -> c_int {
    unsafe {
        (*ctx).push(Resp::Bulk(Some((*p).value.clone())));
    }
    0
}
unsafe extern "C" fn reply_error(ctx: *mut Context, p: *const c_char) -> c_int {
    let value = unsafe { CStr::from_ptr(p) }
        .to_string_lossy()
        .replace(['\r', '\n'], " ");
    unsafe {
        (*ctx).push(Resp::Error(value));
    }
    0
}
unsafe extern "C" fn reply_simple(ctx: *mut Context, p: *const c_char) -> c_int {
    let value = unsafe { CStr::from_ptr(p) }.to_bytes();
    if value.contains(&b'\r') || value.contains(&b'\n') {
        return 1;
    }
    unsafe {
        (*ctx).push(Resp::Status(Bytes::copy_from_slice(value)));
    }
    0
}
unsafe extern "C" fn reply_array(ctx: *mut Context, len: i64) -> c_int {
    let ctx = unsafe { &mut *ctx };
    if len < -1 {
        return 1;
    }
    if len == 0 {
        ctx.push(Resp::Array(Some(Vec::new())));
    } else {
        ctx.frames.push(Frame {
            length: if len < 0 { None } else { Some(len as usize) },
            values: Vec::new(),
        });
    }
    0
}
unsafe extern "C" fn array_length(ctx: *mut Context, len: i64) {
    let ctx = unsafe { &mut *ctx };
    if len < 0 {
        return;
    }
    if let Some(frame) = ctx.frames.iter_mut().rev().find(|f| f.length.is_none()) {
        frame.length = Some(len as usize);
    }
    while ctx
        .frames
        .last()
        .is_some_and(|f| f.length == Some(f.values.len()))
    {
        let f = ctx.frames.pop().unwrap();
        ctx.push(Resp::Array(Some(f.values)));
    }
}

// Native config callbacks are staged only for OnLoad; no registration is made
// visible until both OnLoad and LoadConfigs succeed. Runtime CONFIG mutation is
// deliberately not advertised by this supported subset.
type ConfigGet = unsafe extern "C" fn(*const c_char, *mut c_void) -> *mut ModuleString;
type ConfigSet = unsafe extern "C" fn(
    *const c_char,
    *mut ModuleString,
    *mut c_void,
    *mut *mut ModuleString,
) -> c_int;
type ConfigApply = unsafe extern "C" fn(*mut Context, *mut c_void, *mut *mut ModuleString) -> c_int;
struct StringConfig {
    name: CString,
    default: Bytes,
    set: ConfigSet,
    apply: Option<ConfigApply>,
    data: *mut c_void,
}
unsafe extern "C" fn register_string_config(
    ctx: *mut Context,
    name: *const c_char,
    default: *const c_char,
    flags: u32,
    _get: Option<ConfigGet>,
    set: Option<ConfigSet>,
    apply: Option<ConfigApply>,
    data: *mut c_void,
) -> c_int {
    let ctx = unsafe { &mut *ctx };
    let name = unsafe { CStr::from_ptr(name) }.to_owned();
    // Only immutable configuration is supported, never claim CONFIG SET works.
    if flags != 1
        || ctx.config_loaded
        || name.as_bytes().is_empty()
        || !name
            .as_bytes()
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
        || ctx.configs.iter().any(|c| c.name == name)
        || _get.is_none()
    {
        return 1;
    }
    let Some(set) = set else {
        return 1;
    };
    ctx.configs.push(StringConfig {
        name,
        default: Bytes::copy_from_slice(unsafe { CStr::from_ptr(default) }.to_bytes()),
        set,
        apply,
        data,
    });
    0
}
unsafe extern "C" fn load_configs(ctx: *mut Context) -> c_int {
    let ctx = unsafe { &mut *ctx };
    if ctx.config_loaded {
        return 1;
    }
    if ctx.config.iter().any(|(name, _)| {
        !ctx.configs
            .iter()
            .any(|c| c.name.as_bytes() == name.as_ref())
    }) {
        ctx.failure = Some("unknown module configuration".into());
        return 1;
    }
    // Copy callback descriptors, not module data, so callbacks may call APIs.
    let configs: Vec<_> = ctx
        .configs
        .iter()
        .map(|c| (c.name.clone(), c.default.clone(), c.set, c.apply, c.data))
        .collect();
    let mut applies = Vec::new();
    for (name, default, set, apply, data) in configs {
        let value = ctx
            .config
            .iter()
            .find(|(n, _)| n.as_ref() == name.as_bytes())
            .map_or(default, |(_, v)| v.clone());
        let value = Arc::new(ModuleString { value });
        let mut error = ptr::null_mut();
        let status = unsafe {
            set(
                name.as_ptr(),
                Arc::as_ptr(&value).cast_mut(),
                data,
                &mut error,
            )
        };
        if !error.is_null() {
            unsafe {
                free_string(ctx, error);
            }
        }
        if status != 0 {
            ctx.failure = Some("module configuration setter rejected value".into());
            return 1;
        }
        if let Some(apply) = apply
            && !applies.iter().any(|(f, p): &(ConfigApply, *mut c_void)| {
                *f as usize == apply as usize && *p == data
            })
        {
            applies.push((apply, data));
        }
    }
    for (apply, data) in applies {
        let mut error = ptr::null_mut();
        let status = unsafe { apply(ctx, data, &mut error) };
        if !error.is_null() {
            unsafe {
                free_string(ctx, error);
            }
        }
        if status != 0 {
            ctx.failure = Some("module configuration apply failed".into());
            return 1;
        }
    }
    ctx.config_loaded = true;
    0
}

// Call replies own a stable recursively allocated tree. Child pointers are
// borrowed until FreeCallReply frees the root, matching the public API.
pub struct CallReply {
    kind: c_int,
    value: Bytes,
    integer: i64,
    double: f64,
    children: Vec<CallReply>,
    owner: *mut Context,
    simple: bool,
    null_array: bool,
    format: Option<[u8; 3]>,
}
impl CallReply {
    fn from_resp(reply: Resp) -> Self {
        let mut r = Self {
            kind: 4,
            value: Bytes::new(),
            integer: 0,
            double: 0.0,
            children: Vec::new(),
            owner: ptr::null_mut(),
            simple: false,
            null_array: false,
            format: None,
        };
        match reply {
            Resp::Simple(v) => {
                r.kind = 0;
                r.simple = true;
                r.value = Bytes::from_static(v.as_bytes());
            }
            Resp::Status(v) => {
                r.kind = 0;
                r.simple = true;
                r.value = v;
            }
            Resp::Bulk(Some(v)) => {
                r.kind = 0;
                r.value = v;
            }
            Resp::Verbatim { format, value } => {
                r.kind = 10;
                r.format = Some(format);
                r.value = value;
            }
            Resp::Error(v) => {
                r.kind = 1;
                r.value = Bytes::from(v);
            }
            Resp::Integer(v) => {
                r.kind = 2;
                r.integer = v;
            }
            Resp::Unsigned(v) => {
                if let Ok(value) = i64::try_from(v) {
                    r.kind = 2;
                    r.integer = value;
                } else {
                    r.kind = 9;
                    r.value = Bytes::from(v.to_string());
                }
            }
            Resp::BigNumber(v) => {
                r.kind = 9;
                r.value = v;
            }
            Resp::Boolean(v) => {
                r.kind = 7;
                r.integer = i64::from(v);
            }
            Resp::Double(v) => {
                r.kind = 8;
                r.double = v;
            }
            Resp::Array(Some(v)) => {
                r.kind = 3;
                r.children = v.into_iter().map(Self::from_resp).collect();
            }
            Resp::Set(v) => {
                r.kind = 6;
                r.children = v.into_iter().map(Self::from_resp).collect();
            }
            Resp::Map(v) => {
                r.kind = 5;
                r.children = v
                    .into_iter()
                    .flat_map(|(k, v)| [Self::from_resp(k), Self::from_resp(v)])
                    .collect();
            }
            Resp::Scored(v) => {
                r.kind = 3;
                r.children = v
                    .into_iter()
                    .flat_map(|(k, v)| {
                        [
                            Self::from_resp(Resp::Bulk(Some(k))),
                            Self::from_resp(Resp::Bulk(Some(Bytes::from(v.to_string())))),
                        ]
                    })
                    .collect();
            }
            Resp::InfoField(k, v) => {
                return Self::from_resp(Resp::Map(vec![(Resp::Bulk(Some(k)), *v)]));
            }
            Resp::VectorResults(v) => {
                return Self::from_resp(Resp::Array(Some(
                    v.into_iter()
                        .map(|(key, score, payload)| {
                            let mut row = vec![Resp::Bulk(Some(key))];
                            if let Some(score) = score {
                                row.push(Resp::Double(score));
                            }
                            if let Some(payload) = payload {
                                row.push(Resp::Bulk(payload));
                            }
                            Resp::Array(Some(row))
                        })
                        .collect(),
                )));
            }
            Resp::Bulk(None) => {}
            Resp::Array(None) => {
                r.null_array = true;
            }
        }
        r
    }
    fn response(&self) -> Resp {
        match self.kind {
            0 if self.simple => Resp::Status(self.value.clone()),
            0 => Resp::Bulk(Some(self.value.clone())),
            1 => Resp::Error(String::from_utf8_lossy(&self.value).into_owned()),
            2 => Resp::Integer(self.integer),
            3 => Resp::Array(Some(self.children.iter().map(Self::response).collect())),
            5 => Resp::Map(
                self.children
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|c| (c[0].response(), c[1].response()))
                    .collect(),
            ),
            6 => Resp::Set(self.children.iter().map(Self::response).collect()),
            7 => Resp::Boolean(self.integer != 0),
            8 => Resp::Double(self.double),
            9 => Resp::BigNumber(self.value.clone()),
            10 => Resp::Verbatim {
                format: self.format.unwrap(),
                value: self.value.clone(),
            },
            _ if self.null_array => Resp::Array(None),
            _ => Resp::Bulk(None),
        }
    }
}
#[repr(C)]
pub struct CallArg {
    data: *const c_char,
    len: usize,
}
// Called by the native `call.c` shim; WASI never loads modules, so the
// playground neither links that shim nor exports these symbols.
#[cfg_attr(not(target_os = "wasi"), unsafe(no_mangle))]
pub unsafe extern "C" fn ffkv_module_callv(
    ctx: *mut Context,
    name: *const c_char,
    args: *const CallArg,
    count: usize,
) -> *mut CallReply {
    if ctx.is_null() || name.is_null() {
        return ptr::null_mut();
    }
    let mut argv = Vec::with_capacity(count + 1);
    argv.push(Bytes::copy_from_slice(
        unsafe { CStr::from_ptr(name) }.to_bytes(),
    ));
    if count != 0 {
        for arg in unsafe { std::slice::from_raw_parts(args, count) } {
            argv.push(Bytes::copy_from_slice(unsafe { bytes(arg.data, arg.len) }));
        }
    }
    let ctx = unsafe { &mut *ctx };
    let mut value = Box::new(CallReply::from_resp(ctx.call(&argv)));
    if ctx.auto {
        value.owner = ctx;
    }
    let reply = Box::into_raw(value);
    if ctx.auto {
        ctx.calls.insert(reply);
    }
    reply
}
unsafe extern "C" {
    fn ffkv_module_call(
        ctx: *mut c_void,
        name: *const c_char,
        format: *const c_char,
        ...
    ) -> *mut c_void;
}
unsafe extern "C" fn free_call(reply: *mut CallReply) {
    if !reply.is_null() {
        let reply = unsafe { Box::from_raw(reply) };
        if !reply.owner.is_null() {
            unsafe {
                (*reply.owner)
                    .calls
                    .remove(&(ptr::from_ref(&*reply).cast_mut()));
            }
        }
    }
}
unsafe extern "C" fn call_type(reply: *const CallReply) -> c_int {
    if reply.is_null() {
        -1
    } else {
        unsafe { (*reply).kind }
    }
}
unsafe extern "C" fn call_length(reply: *const CallReply) -> usize {
    let r = unsafe { &*reply };
    match r.kind {
        3 | 6 => r.children.len(),
        5 => r.children.len() / 2,
        0 | 1 => r.value.len(),
        _ => 0,
    }
}
unsafe extern "C" fn call_integer(reply: *const CallReply) -> i64 {
    unsafe { (*reply).integer }
}
unsafe extern "C" fn call_bool(reply: *const CallReply) -> c_int {
    unsafe { (*reply).integer as c_int }
}
unsafe extern "C" fn call_double(reply: *const CallReply) -> f64 {
    unsafe { (*reply).double }
}
unsafe extern "C" fn call_ptr(reply: *const CallReply, len: *mut usize) -> *const c_char {
    let r = unsafe { &*reply };
    if !len.is_null() {
        unsafe {
            *len = r.value.len();
        }
    }
    r.value.as_ptr().cast()
}
unsafe extern "C" fn call_element(reply: *mut CallReply, index: usize) -> *mut CallReply {
    if reply.is_null() {
        return ptr::null_mut();
    }
    let reply = unsafe { &mut *reply };
    if reply.kind != 3 {
        return ptr::null_mut();
    }
    reply.children.get_mut(index).map_or(ptr::null_mut(), |r| r)
}
unsafe extern "C" fn reply_call(ctx: *mut Context, reply: *const CallReply) -> c_int {
    if reply.is_null() {
        return 1;
    }
    unsafe {
        (*ctx).push((*reply).response());
    }
    0
}

// Key APIs use real engine commands, never a second native-only keyspace.
struct ModuleKey {
    ctx: *mut Context,
    name: Bytes,
    mode: c_int,
}
unsafe extern "C" fn open_key(
    ctx: *mut Context,
    name: *mut ModuleString,
    mode: c_int,
) -> *mut ModuleKey {
    if mode & !3 != 0 || mode & 3 == 0 || unsafe { (*ctx).dispatch.is_none() } {
        return ptr::null_mut();
    }
    if mode & 2 == 0 {
        let reply = unsafe { &mut *ctx }.call(&[Bytes::from_static(b"EXISTS"), unsafe {
            (*name).value.clone()
        }]);
        if !matches!(reply,Resp::Integer(n) if n>0) {
            return ptr::null_mut();
        }
    }
    let key = Box::into_raw(Box::new(ModuleKey {
        ctx,
        name: unsafe { (*name).value.clone() },
        mode,
    }));
    if unsafe { (*ctx).auto } {
        unsafe {
            (*ctx).keys.insert(key);
        }
    }
    key
}
unsafe extern "C" fn close_key(key: *mut ModuleKey) {
    if key.is_null() {
        return;
    }
    let key = unsafe { Box::from_raw(key) };
    unsafe {
        (*key.ctx).keys.remove(&(ptr::from_ref(&*key).cast_mut()));
    }
}
unsafe extern "C" fn key_type(key: *mut ModuleKey) -> c_int {
    if key.is_null() {
        return 0;
    }
    let key = unsafe { &mut *key };
    let reply = unsafe { &mut *key.ctx }.call(&[Bytes::from_static(b"TYPE"), key.name.clone()]);
    let name = match &reply {
        Resp::Simple(v) => v.as_bytes(),
        Resp::Status(v) | Resp::Bulk(Some(v)) => v.as_ref(),
        _ => return 0,
    };
    match name {
        b"none" => 0,
        b"string" => 1,
        b"list" => 2,
        b"hash" => 3,
        b"set" => 4,
        b"zset" => 5,
        b"stream" => 7,
        _ => 6,
    }
}
unsafe extern "C" fn value_length(key: *mut ModuleKey) -> usize {
    let key = unsafe { &mut *key };
    let command = match unsafe { key_type(key) } {
        1 => b"STRLEN".as_slice(),
        2 => b"LLEN",
        3 => b"HLEN",
        4 => b"SCARD",
        5 => b"ZCARD",
        7 => b"XLEN",
        _ => return 0,
    };
    match unsafe { &mut *key.ctx }.call(&[Bytes::copy_from_slice(command), key.name.clone()]) {
        Resp::Integer(n) => n.max(0) as usize,
        _ => 0,
    }
}
unsafe extern "C" fn string_set(key: *mut ModuleKey, value: *mut ModuleString) -> c_int {
    let key = unsafe { &mut *key };
    if key.mode & 2 == 0 {
        return 1;
    }
    match unsafe { &mut *key.ctx }.call(&[Bytes::from_static(b"SET"), key.name.clone(), unsafe {
        (*value).value.clone()
    }]) {
        Resp::Error(_) => 1,
        _ => 0,
    }
}
unsafe extern "C" fn delete_key(key: *mut ModuleKey) -> c_int {
    let key = unsafe { &mut *key };
    if key.mode & 2 == 0 {
        return 1;
    }
    match unsafe { &mut *key.ctx }.call(&[Bytes::from_static(b"DEL"), key.name.clone()]) {
        Resp::Error(_) => 1,
        _ => 0,
    }
}
unsafe extern "C" fn get_expire(key: *mut ModuleKey) -> i64 {
    let key = unsafe { &mut *key };
    match unsafe { &mut *key.ctx }.call(&[Bytes::from_static(b"PTTL"), key.name.clone()]) {
        Resp::Integer(n) => n.max(-1),
        _ => -1,
    }
}
unsafe extern "C" fn set_expire(key: *mut ModuleKey, ttl: i64) -> c_int {
    let key = unsafe { &mut *key };
    if key.mode & 2 == 0 || ttl < -1 {
        return 1;
    }
    let args = if ttl == -1 {
        vec![Bytes::from_static(b"PERSIST"), key.name.clone()]
    } else {
        vec![
            Bytes::from_static(b"PEXPIRE"),
            key.name.clone(),
            Bytes::from(ttl.to_string()),
        ]
    };
    match unsafe { &mut *key.ctx }.call(&args) {
        Resp::Error(_) => 1,
        _ => 0,
    }
}

unsafe extern "C" fn allocate(size: usize) -> *mut c_void {
    let p = unsafe { libc::malloc(size.max(1)) };
    if p.is_null() {
        std::process::abort();
    }
    p
}
unsafe extern "C" fn allocate_zeroed(count: usize, size: usize) -> *mut c_void {
    let Some(total) = count.checked_mul(size) else {
        std::process::abort();
    };
    let p = unsafe { libc::calloc(1, total.max(1)) };
    if p.is_null() {
        std::process::abort();
    }
    p
}
unsafe extern "C" fn reallocate(old: *mut c_void, size: usize) -> *mut c_void {
    let p = unsafe { libc::realloc(old, size.max(1)) };
    if p.is_null() {
        std::process::abort();
    }
    p
}

// This table is the compatibility contract: no generic success stubs are used.
#[cfg_attr(not(target_os = "wasi"), unsafe(export_name = "ffkv_module_get_api"))]
pub unsafe extern "C" fn get_api(name: *const c_char, target: *mut c_void) -> c_int {
    if name.is_null() || target.is_null() {
        return 1;
    }
    macro_rules! api {($($name:literal=>$func:expr),* $(,)?)=>{match unsafe {CStr::from_ptr(name)}.to_bytes() {$( $name=>{unsafe {*(target as *mut *const c_void)=$func as *const () as *const c_void;}0},)* _=>{unsafe {*(target as *mut *const c_void)=ptr::null();}1}}};}
    api! {
        b"RedisModule_GetApi"=>get_api,b"RedisModule_SetModuleAttribs"=>set_attributes,b"RedisModule_IsModuleNameBusy"=>name_busy,
        b"RedisModule_CreateCommand"=>create_command,b"RedisModule_GetCommand"=>get_command,b"RedisModule_SetCommandInfo"=>set_command_info,
        b"RedisModule_Alloc"=>allocate,b"RedisModule_Calloc"=>allocate_zeroed,b"RedisModule_Realloc"=>reallocate,b"RedisModule_Free"=>libc::free,
        b"RedisModule_CreateString"=>create_string,b"RedisModule_CreateStringFromString"=>create_string_from_string,b"RedisModule_CreateStringFromLongLong"=>create_string_ll,
        b"RedisModule_FreeString"=>free_string,b"RedisModule_RetainString"=>retain_string,b"RedisModule_StringPtrLen"=>string_ptr,
        b"RedisModule_StringToLongLong"=>string_ll,b"RedisModule_StringToDouble"=>string_double,b"RedisModule_StringCompare"=>string_compare,b"RedisModule_AutoMemory"=>auto_memory,
        b"RedisModule_WrongArity"=>wrong_arity,b"RedisModule_ReplyWithLongLong"=>reply_integer,b"RedisModule_ReplyWithDouble"=>reply_double,
        b"RedisModule_ReplyWithNull"=>reply_null,b"RedisModule_ReplyWithStringBuffer"=>reply_buffer,b"RedisModule_ReplyWithString"=>reply_string,
        b"RedisModule_ReplyWithError"=>reply_error,b"RedisModule_ReplyWithSimpleString"=>reply_simple,b"RedisModule_ReplyWithArray"=>reply_array,b"RedisModule_ReplySetArrayLength"=>array_length,
        b"RedisModule_RegisterStringConfig"=>register_string_config,b"RedisModule_LoadConfigs"=>load_configs,
        b"RedisModule_Call"=>ffkv_module_call,b"RedisModule_FreeCallReply"=>free_call,b"RedisModule_CallReplyType"=>call_type,b"RedisModule_CallReplyLength"=>call_length,
        b"RedisModule_CallReplyInteger"=>call_integer,b"RedisModule_CallReplyDouble"=>call_double,b"RedisModule_CallReplyBool"=>call_bool,b"RedisModule_CallReplyStringPtr"=>call_ptr,
        b"RedisModule_CallReplyArrayElement"=>call_element,b"RedisModule_ReplyWithCallReply"=>reply_call,
        b"RedisModule_OpenKey"=>open_key,b"RedisModule_CloseKey"=>close_key,b"RedisModule_KeyType"=>key_type,b"RedisModule_ValueLength"=>value_length,
        b"RedisModule_StringSet"=>string_set,b"RedisModule_DeleteKey"=>delete_key,b"RedisModule_GetExpire"=>get_expire,b"RedisModule_SetExpire"=>set_expire,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unknown_api_is_absent_not_a_success_stub() {
        let mut target = std::ptr::dangling::<c_void>();
        assert_eq!(
            unsafe {
                get_api(
                    c"RedisModule_UnsupportedFixtureAPI".as_ptr(),
                    ptr::from_mut(&mut target).cast(),
                )
            },
            1
        );
        assert!(target.is_null());
        assert_eq!(
            unsafe {
                get_api(
                    c"RedisModule_CreateCommand".as_ptr(),
                    ptr::from_mut(&mut target).cast(),
                )
            },
            0
        );
        assert!(!target.is_null());
    }
    #[test]
    fn auto_memory_and_retained_binary_strings_have_independent_lifetimes() {
        let mut ctx = Context::lifecycle();
        unsafe {
            auto_memory(&mut ctx);
        }
        let string = unsafe { create_string(&mut ctx, b"\0\xff".as_ptr().cast(), 2) };
        unsafe {
            retain_string(&mut ctx, string);
        }
        drop(ctx);
        let mut length = 0;
        let value = unsafe { string_ptr(string, &mut length) };
        assert_eq!(unsafe { bytes(value, length) }, b"\0\xff");
        unsafe {
            free_string(ptr::null_mut(), string);
        }
    }
    #[test]
    fn explicit_free_removes_call_reply_from_auto_memory() {
        let mut engine = |_: &[Bytes]| Resp::Simple("OK");
        let mut ctx = Context::invocation(&mut engine);
        unsafe {
            auto_memory(&mut ctx);
        }
        let reply = unsafe { ffkv_module_callv(&mut ctx, c"PING".as_ptr(), ptr::null(), 0) };
        assert_eq!(ctx.calls.len(), 1);
        assert_eq!(
            unsafe { (*reply).response() },
            Resp::Status(Bytes::from_static(b"OK"))
        );
        unsafe {
            free_call(reply);
        }
        assert!(ctx.calls.is_empty());
    }
    #[test]
    fn nested_and_postponed_arrays_finalize_in_public_reply_order() {
        let mut ctx = Context::lifecycle();
        unsafe {
            reply_array(&mut ctx, 2);
            reply_integer(&mut ctx, 7);
            reply_array(&mut ctx, -1);
            reply_buffer(&mut ctx, b"\0\xff".as_ptr().cast(), 2);
            array_length(&mut ctx, 1);
        }
        assert_eq!(
            ctx.finish(0),
            Resp::Array(Some(vec![
                Resp::Integer(7),
                Resp::Array(Some(vec![Resp::Bulk(Some(Bytes::from_static(b"\0\xff")))]))
            ]))
        );
    }
}
