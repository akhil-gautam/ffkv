//! Native Redis Modules API v1 host. Libraries are trusted code in this process,
//! not sandboxed. Only APIs explicitly exported by `abi::get_api` are supported;
//! absent optional APIs return REDISMODULE_ERR from GetApi. See that export table
//! for the precise compatibility surface (not the complete Redis binary API).
//! Commands run synchronously on the paused atomic dispatcher; all key access
//! and Call operations go through its normal ACL, journal and replication path.
mod abi;
use crate::{engine::Engine, protocol::Resp};
use abi::{Command, CommandFn, Context};
use bytes::Bytes;
use libloading::Library;
use std::{
    collections::{BTreeMap, HashSet},
    ffi::c_int,
    path::Path,
    sync::{
        Arc, RwLock,
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::sync::Mutex;

pub struct Extensions {
    modules: RwLock<BTreeMap<String, Arc<Module>>>,
    lifecycle: Mutex<()>,
}
struct Module {
    name: String,
    version: i64,
    path: Bytes,
    args: Vec<Bytes>,
    commands: Vec<Command>,
    active: AtomicUsize,
    unload: Option<unsafe extern "C" fn(*mut Context) -> c_int>,
    // Closed only after successful OnUnload and all invocation pins are gone.
    _library: std::mem::ManuallyDrop<Library>,
}
impl Drop for Module {
    fn drop(&mut self) {
        // A refused OnUnload must not become an implicit dlclose during State
        // destruction. Such handles intentionally live until process exit.
        if self.active.load(Ordering::Acquire) == usize::MAX {
            unsafe {
                std::mem::ManuallyDrop::drop(&mut self._library);
            }
        }
    }
}
#[derive(Clone, Debug)]
pub struct CommandMetadata {
    pub name: String,
    pub module_name: String,
    pub arity: i64,
    pub flags: Vec<&'static str>,
    pub categories: Vec<&'static str>,
    pub first_key: i64,
    pub last_key: i64,
    pub key_step: i64,
}
impl CommandMetadata {
    pub fn keys(&self, args: &[Bytes]) -> Vec<Bytes> {
        if self.first_key <= 0 || self.key_step <= 0 {
            return Vec::new();
        }
        let end = (if self.last_key < 0 {
            args.len() as i64 + self.last_key
        } else {
            self.last_key
        })
        .min(args.len() as i64 - 1);
        (self.first_key..=end)
            .step_by(self.key_step as usize)
            .filter_map(|i| args.get(i as usize).cloned())
            .collect()
    }
}
pub struct Invocation {
    module: Arc<Module>,
    command_index: usize,
    args: Vec<Bytes>,
    keys: Vec<Bytes>,
}
impl Invocation {
    pub fn name(&self) -> &[u8] {
        self.metadata().name.as_bytes()
    }
    pub fn categories(&self) -> &[&'static str] {
        &self.metadata().categories
    }
    pub fn flags(&self) -> &[&'static str] {
        &self.metadata().flags
    }
    pub fn keys(&self) -> &[Bytes] {
        &self.keys
    }
    pub fn is_write(&self) -> bool {
        self.flags().contains(&"write")
    }
    pub fn script_allowed(&self) -> bool {
        !self.flags().contains(&"noscript")
    }
    pub fn transaction_allowed(&self) -> bool {
        !self.flags().contains(&"no_multi")
    }
    pub fn global_keyspace(&self) -> bool {
        false
    }
    pub fn metadata(&self) -> &CommandMetadata {
        &self.module.commands[self.command_index].metadata
    }
    pub fn execute(self, mut call: impl FnMut(&[Bytes]) -> Resp) -> Resp {
        if self
            .module
            .active
            .try_update(Ordering::AcqRel, Ordering::Acquire, |n| {
                if n == usize::MAX {
                    None
                } else {
                    n.checked_add(1)
                }
            })
            .is_err()
        {
            return err("module is unloaded");
        }
        struct Active<'a>(&'a AtomicUsize);
        impl Drop for Active<'_> {
            fn drop(&mut self) {
                self.0.fetch_sub(1, Ordering::Release);
            }
        }
        let _active = Active(&self.module.active);
        let command = &self.module.commands[self.command_index];
        let readonly = !self.is_write();
        let mut guarded = |args: &[Bytes]| {
            if readonly && Engine::is_dataset_write(args) {
                return err("write command called from a readonly module command");
            }
            call(args)
        };
        let mut ctx = Context::invocation(&mut guarded);
        let (mut strings, mut argv) = abi::argv(&self.args);
        let status =
            unsafe { (command.callback)(&mut ctx, argv.as_mut_ptr(), strings.len() as c_int) };
        // Keep strings alive through native callback and finalize nested replies.
        strings.clear();
        ctx.finish(status)
    }
}
impl Default for Extensions {
    fn default() -> Self {
        Self::new()
    }
}
impl Extensions {
    pub fn new() -> Self {
        Self {
            modules: RwLock::new(BTreeMap::new()),
            lifecycle: Mutex::new(()),
        }
    }
    pub fn prepare_sync(&self, args: &[Bytes]) -> Option<Result<Invocation, Resp>> {
        let name = args.first()?;
        let modules = self.modules.read().unwrap_or_else(|e| e.into_inner());
        for module in modules.values() {
            if let Some((command_index, command)) = module
                .commands
                .iter()
                .enumerate()
                .find(|(_, c)| name.eq_ignore_ascii_case(c.metadata.name.as_bytes()))
            {
                let m = &command.metadata;
                if (m.arity > 0 && args.len() as i64 != m.arity)
                    || (m.arity < 0 && (args.len() as i64) < -m.arity)
                {
                    return Some(Err(err(format!(
                        "wrong number of arguments for '{}' command",
                        m.name
                    ))));
                }
                let keys = m.keys(args);
                return Some(Ok(Invocation {
                    module: module.clone(),
                    command_index,
                    args: args.to_vec(),
                    keys,
                }));
            }
        }
        None
    }
    pub fn command_metadata_sync(&self) -> Vec<CommandMetadata> {
        self.modules
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .flat_map(|m| m.commands.iter().map(|c| c.metadata.clone()))
            .collect()
    }
    pub async fn command_metadata(&self) -> Vec<CommandMetadata> {
        self.command_metadata_sync()
    }
    /// MODULE lifecycle only. Dynamic commands MUST use prepare and the actor.
    pub async fn execute(&self, args: &[Bytes]) -> Option<Resp> {
        if !args.first()?.eq_ignore_ascii_case(b"MODULE") {
            return None;
        }
        let Some(sub) = args.get(1) else {
            return Some(err("wrong number of arguments for MODULE"));
        };
        Some(if sub.eq_ignore_ascii_case(b"LIST") && args.len() == 2 {
            self.list().await
        } else if sub.eq_ignore_ascii_case(b"LOAD") && args.len() >= 3 {
            self.load(&args[2], &args[3..], &[]).await
        } else if sub.eq_ignore_ascii_case(b"LOADEX") && args.len() >= 3 {
            match loadex(&args[3..]) {
                Ok((config, argv)) => self.load(&args[2], argv, &config).await,
                Err(e) => e,
            }
        } else if sub.eq_ignore_ascii_case(b"UNLOAD") && args.len() == 3 {
            self.unload(&args[2]).await
        } else if sub.eq_ignore_ascii_case(b"HELP") && args.len() == 2 {
            Resp::Array(Some(vec![
                bulk("LIST"),
                bulk("LOAD <shared-library> [arg ...]"),
                bulk("LOADEX <shared-library> [CONFIG name value ...] [ARGS arg ...]"),
                bulk("UNLOAD <name>"),
            ]))
        } else {
            err("unknown MODULE subcommand or wrong number of arguments")
        })
    }
    async fn list(&self) -> Resp {
        Resp::Array(Some(
            self.modules
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .values()
                .map(|m| {
                    Resp::Map(vec![
                        (bulk("name"), bulk(m.name.clone())),
                        (bulk("ver"), Resp::Integer(m.version)),
                        (bulk("path"), bulk(m.path.clone())),
                        (
                            bulk("args"),
                            Resp::Array(Some(m.args.iter().cloned().map(bulk).collect())),
                        ),
                    ])
                })
                .collect(),
        ))
    }
    async fn load(&self, path: &Bytes, args: &[Bytes], config: &[(Bytes, Bytes)]) -> Resp {
        let _lifecycle = self.lifecycle.lock().await;
        let modules = self.modules.read().unwrap_or_else(|e| e.into_inner());
        if modules.len() >= 32 {
            return err("module limit reached");
        }
        #[cfg(unix)]
        let native_path = {
            use std::os::unix::ffi::OsStrExt;
            Path::new(std::ffi::OsStr::from_bytes(path))
        };
        #[cfg(not(unix))]
        let native_path = match std::str::from_utf8(path) {
            Ok(p) => Path::new(p),
            Err(_) => return err("invalid library path"),
        };
        let canonical = match std::fs::canonicalize(native_path) {
            Ok(p) => p,
            Err(e) => return err(format!("cannot resolve module library: {e}")),
        };
        if modules
            .values()
            .any(|m| m.path.as_ref() == canonical.as_os_str().as_encoded_bytes())
        {
            return err("module library already loaded");
        }
        let library = match unsafe { Library::new(&canonical) } {
            Ok(l) => l,
            Err(e) => return err(format!("cannot load module library: {e}")),
        };
        let onload: CommandFn = match unsafe { library.get::<CommandFn>(b"RedisModule_OnLoad\0") } {
            Ok(f) => *f,
            Err(e) => return err(format!("missing RedisModule_OnLoad: {e}")),
        };
        let unload = unsafe {
            library
                .get::<unsafe extern "C" fn(*mut Context) -> c_int>(b"RedisModule_OnUnload\0")
                .ok()
                .map(|f| *f)
        };
        let mut ctx = Context::loading(
            config,
            modules.keys().cloned().collect(),
            modules
                .values()
                .flat_map(|m| m.commands.iter().map(|c| c.metadata.name.clone()))
                .collect(),
        );
        let (_strings, mut argv) = abi::argv(args);
        let status = unsafe { onload(&mut ctx, argv.as_mut_ptr(), args.len() as c_int) };
        if status != 0 || ctx.failure.is_some() {
            return err(ctx
                .failure
                .take()
                .unwrap_or_else(|| "module initialization failed".into()));
        }
        if !ctx.config_loaded && !config.is_empty() {
            return err("module did not consume LOADEX configuration with LoadConfigs");
        }
        let Some((name, version)) = ctx.attributes.take() else {
            return err("module did not initialize API v1 attributes");
        };
        if modules.contains_key(&name) {
            return err("module name is already loaded");
        }
        let commands = std::mem::take(&mut ctx.commands)
            .into_iter()
            .map(|mut c| {
                c.metadata.module_name = name.clone();
                *c
            })
            .collect();
        drop(ctx);
        drop(modules);
        self.modules
            .write()
            .unwrap_or_else(|e| e.into_inner())
            .insert(
                name.clone(),
                Arc::new(Module {
                    name,
                    version,
                    path: Bytes::copy_from_slice(canonical.as_os_str().as_encoded_bytes()),
                    args: args.iter().map(|a| Bytes::copy_from_slice(a)).collect(),
                    commands,
                    active: AtomicUsize::new(0),
                    unload,
                    _library: std::mem::ManuallyDrop::new(library),
                }),
            );
        Resp::Simple("OK")
    }
    async fn unload(&self, name: &Bytes) -> Resp {
        let _lifecycle = self.lifecycle.lock().await;
        let Ok(name) = std::str::from_utf8(name) else {
            return err("module not found");
        };
        let mut modules = self.modules.write().unwrap_or_else(|e| e.into_inner());
        let Some(module) = modules.get(name) else {
            return err("module not found");
        };
        if Arc::strong_count(module) > 1 {
            return err("module has a prepared or active command");
        }
        if module
            .active
            .compare_exchange(0, usize::MAX, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return err("module has an active command");
        }
        if let Some(unload) = module.unload {
            let mut ctx = Context::lifecycle();
            if unsafe { unload(&mut ctx) } != 0 {
                module.active.store(0, Ordering::Release);
                return err("module OnUnload refused unloading");
            }
        }
        modules.remove(name);
        Resp::Simple("OK")
    }
    pub async fn shutdown(&self) {
        // Do not force dlclose modules that refuse OnUnload; OS reclaims process
        // memory on exit. Active/prepared invocations continue pinning libraries.
        let names: Vec<_> = self
            .modules
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .keys()
            .cloned()
            .collect();
        for name in names {
            let _ = self.unload(&Bytes::from(name)).await;
        }
    }
}
type ModuleLoadArguments<'a> = (Vec<(Bytes, Bytes)>, &'a [Bytes]);

fn loadex(args: &[Bytes]) -> Result<ModuleLoadArguments<'_>, Resp> {
    let mut config = Vec::new();
    let mut seen = HashSet::new();
    let mut i = 0;
    while i < args.len() {
        if args[i].eq_ignore_ascii_case(b"ARGS") {
            return Ok((config, &args[i + 1..]));
        }
        if !args[i].eq_ignore_ascii_case(b"CONFIG") || i + 2 >= args.len() {
            return Err(err("syntax error in MODULE LOADEX"));
        }
        if args[i + 1].is_empty() || !seen.insert(args[i + 1].clone()) {
            return Err(err("empty or duplicate module configuration"));
        }
        config.push((args[i + 1].clone(), args[i + 2].clone()));
        i += 3;
    }
    Ok((config, &args[args.len()..]))
}
fn err(message: impl std::fmt::Display) -> Resp {
    Resp::Error(format!("ERR {message}"))
}
fn bulk(value: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(value.into()))
}
fn reserved_command(name: &str) -> bool {
    [
        "module",
        "acl",
        "config",
        "client",
        "cluster",
        "script",
        "function",
        "auth",
        "hello",
        "monitor",
        "subscribe",
        "psubscribe",
        "ssubscribe",
        "publish",
        "spublish",
        "replconf",
        "sync",
        "psync",
    ]
    .contains(&name)
}

#[cfg(test)]
mod tests;
