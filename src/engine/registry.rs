//! Immutable compile-time command registrations indexed once at startup, and
//! [`BoundCommand`], the one view through which invocations consult them.
use super::{
    AccessedKeys, Command, CommandSpec, Engine, GlobalCommand, Handler, Resp, Routing, error,
    integer, key_specs, wrong_arity,
};
use crate::auth::{Audit, User};
use crate::server_commands::ServerCommand;
use bytes::Bytes;
use std::{
    cell::Cell,
    collections::HashMap,
    hash::{BuildHasherDefault, Hasher},
    sync::LazyLock,
};

const MAX_NAME: usize = 128;

/// Hasher for the static command dictionary only. Clients can query but never
/// insert, so every probe sequence is fixed at startup and hostile names cannot
/// grow collision chains. User-controlled key storage keeps its keyed hashers.
#[derive(Default)]
struct NameHasher(u64);
impl Hasher for NameHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.0 = xxhash_rust::xxh3::xxh3_64_with_seed(bytes, self.0);
    }
    fn write_usize(&mut self, length: usize) {
        self.0 ^= length as u64;
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

struct Registered {
    spec: CommandSpec,
    /// Some registration is named `<this>|<subcommand>`.
    container: bool,
    facts: Facts,
}

/// Argument-independent classifications of one registration used as the
/// effective command of an invocation, derived once from its registered flags
/// and, for subcommands, its root. Runtime decisions (ACL, pause, pressure,
/// role, database) stay with their callers.
#[derive(Clone, Copy)]
struct Facts {
    readonly: bool,
    dataset_write: bool,
    pause_write: bool,
    script_allowed: bool,
    transaction_allowed: bool,
    monitor_allowed: bool,
    /// Consulted only on root registrations.
    control: bool,
    /// Consulted only on root registrations.
    server: Option<ServerCommand>,
    requires_all_keys: bool,
    /// The single argument the compiled key specification always names when
    /// present; see [`key_specs::fixed_position`]. Such positions are never
    /// parsed or remembered: they follow from this and the argument count.
    fixed_key: Option<usize>,
    latest: Latest,
    route: Route,
}

/// Where a time-series read's options start, so `LATEST` fans out to all shards.
#[derive(Clone, Copy)]
enum Latest {
    Never,
    At(usize),
    /// `TS.NRANGE`/`TS.NREVRANGE`: after the counted keys that follow `args[1]`.
    AfterCount,
}

/// Static part of [`Routing`]; keyed commands still parse their key positions.
#[derive(Clone, Copy)]
enum Route {
    Scripting,
    Maintenance,
    Global(GlobalCommand),
    All,
    Keyed {
        /// Argument whose list or stream readiness the command may signal.
        ready: Option<usize>,
        /// Blocking reads coordinate even when every key maps to one shard.
        coordinated: bool,
    },
}

impl Facts {
    fn of(spec: &CommandSpec, root: &CommandSpec) -> Self {
        let flag = |name: &'static str| spec.flags.contains(&name);
        let dataset_write = spec.command != Command::Maintenance
            && (flag("write") || matches!(spec.name, "eval" | "evalsha" | "fcall"));
        let requires_all_keys = Engine::requires_all_keys(spec.name);
        let latest = match spec.name {
            "ts.get" => Latest::At(2),
            "ts.range" | "ts.revrange" => Latest::At(4),
            "ts.nrange" | "ts.nrevrange" => Latest::AfterCount,
            _ => Latest::Never,
        };
        let global = match spec.name {
            "scan" => Some(GlobalCommand::Scan),
            "keys" => Some(GlobalCommand::Keys),
            "dbsize" => Some(GlobalCommand::Dbsize),
            "randomkey" => Some(GlobalCommand::RandomKey),
            "flushdb" | "flushall" => Some(GlobalCommand::Flush),
            _ => None,
        };
        let route = if root.command == Command::Scripting {
            Route::Scripting
        } else if root.command == Command::Maintenance {
            Route::Maintenance
        } else if let Some(global) = global {
            Route::Global(global)
        } else if requires_all_keys
            || spec.name.starts_with("ft.")
            || matches!(spec.name, "copy" | "move" | "swapdb" | "sort" | "sort_ro")
        {
            Route::All
        } else {
            let ready = match spec.command {
                Command::Lpush | Command::Rpush | Command::Lpushx | Command::Rpushx => Some(1),
                Command::Rename | Command::Renamenx | Command::Lmove | Command::Blmove => Some(2),
                _ if spec.name == "xadd" => Some(1),
                _ => None,
            };
            let coordinated = flag("blocking")
                || matches!(
                    spec.command,
                    Command::Blpop | Command::Brpop | Command::Blmove
                )
                || matches!(spec.name, "xread" | "xreadgroup" | "ts.read");
            Route::Keyed { ready, coordinated }
        };
        Self {
            readonly: flag("readonly"),
            dataset_write,
            pause_write: dataset_write || flag("may_replicate"),
            script_allowed: !flag("noscript"),
            transaction_allowed: !flag("no_multi"),
            monitor_allowed: spec.name != "quit" && !flag("admin") && !flag("skip_monitor"),
            control: matches!(
                spec.command,
                Command::Ping
                    | Command::Echo
                    | Command::Quit
                    | Command::Select
                    | Command::Catalog
                    | Command::Connection
                    | Command::Publish
                    | Command::Maintenance
            ) || spec.name == "script",
            server: ServerCommand::of(spec.name.as_bytes()),
            requires_all_keys,
            fixed_key: key_specs::fixed_position(spec.name, spec.key_table),
            latest,
            route,
        }
    }
}

static REGISTRY: LazyLock<HashMap<&'static [u8], Registered, BuildHasherDefault<NameHasher>>> =
    LazyLock::new(|| {
        let mut commands = HashMap::with_capacity_and_hasher(
            super::all_specifications().count(),
            BuildHasherDefault::default(),
        );
        for spec in super::all_specifications() {
            assert!(
                spec.name.len() <= MAX_NAME,
                "static command name exceeds lookup bound"
            );
            // `Engine::read_entry` reads, and records the access of, `args[1]`.
            assert!(
                !matches!(spec.handler, Handler::Read(_))
                    || (spec.keys == (1, 1, 1) && spec.arity.abs() >= 2),
                "single-entry read must key exactly args[1]: {}",
                spec.name
            );
            let mut registered = *spec;
            if let Some(flags) = super::command_flags::lookup(spec.name) {
                registered.flags = flags;
            }
            registered.key_table = key_specs::KeyTable::Indexed(key_specs::table(spec.name));
            let entry = Registered {
                spec: registered,
                container: false,
                facts: Facts::of(&registered, &registered),
            };
            assert!(
                commands.insert(spec.name.as_bytes(), entry).is_none(),
                "duplicate command registration: {}",
                spec.name
            );
        }
        for spec in super::all_specifications() {
            if let Some((parent, _)) = spec.name.split_once('|')
                && let Some(root) = commands.get_mut(parent.as_bytes())
            {
                root.container = true;
                let root = root.spec;
                let entry = commands
                    .get_mut(spec.name.as_bytes())
                    .expect("registered subcommand");
                entry.facts = Facts::of(&entry.spec, &root);
            }
        }
        commands
    });

/// Recent hits per thread. Entries point into the immutable registry, so they
/// never go stale; misses always consult the dictionary and are never kept, so
/// unknown names (module commands included) resolve exactly as before.
const RECENT: usize = 4;

#[derive(Clone, Copy)]
struct Recent {
    entries: [Option<&'static Registered>; RECENT],
    next: usize,
}

thread_local! {
    static RECENT_HITS: Cell<Recent> = const {
        Cell::new(Recent {
            entries: [None; RECENT],
            next: 0,
        })
    };
}

/// Case-insensitive; registered names are lowercase. Byte equality with a
/// UTF-8 name implies the input was valid UTF-8, so no separate check.
fn registered(name: &[u8]) -> Option<&'static Registered> {
    if name.len() > MAX_NAME {
        return None;
    }
    // Canonical names are the lowercase form of every input that indexes them,
    // so ASCII-insensitive equality (length included) is the same predicate.
    let mut recent = RECENT_HITS.get();
    if let Some(entry) = recent
        .entries
        .into_iter()
        .flatten()
        .find(|entry| entry.spec.name.as_bytes().eq_ignore_ascii_case(name))
    {
        return Some(entry);
    }
    let mut normalized = [0u8; MAX_NAME];
    let normalized = &mut normalized[..name.len()];
    for (output, input) in normalized.iter_mut().zip(name) {
        *output = input.to_ascii_lowercase();
    }
    let entry = REGISTRY.get(&*normalized)?;
    recent.entries[recent.next] = Some(entry);
    recent.next = (recent.next + 1) % RECENT;
    RECENT_HITS.set(recent);
    Some(entry)
}

/// Name-only registration lookup (catalog descriptors, module name collisions,
/// the current command's ACL metadata); invocations bind a [`BoundCommand`].
pub(super) fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    registered(name).map(|entry| &entry.spec)
}

fn root(args: &[Bytes]) -> Option<&'static Registered> {
    let name = args.first()?;
    if name.contains(&b'|') {
        return None;
    }
    registered(name)
}

/// The registered `<root>|<subcommand>` named by `args[1]`, else `root` itself.
fn effective(root: &'static Registered, args: &[Bytes]) -> &'static Registered {
    let Some(subcommand) = args.get(1).filter(|_| root.container) else {
        return root;
    };
    let parent = root.spec.name;
    // Saturation only exceeds the bound, which falls back like any long name.
    let length = (parent.len() + 1).saturating_add(subcommand.len());
    if length > MAX_NAME {
        return root;
    }
    let mut name = [0u8; MAX_NAME];
    name[..parent.len()].copy_from_slice(parent.as_bytes());
    name[parent.len()] = b'|';
    name[parent.len() + 1..length].copy_from_slice(subcommand);
    registered(&name[..length]).unwrap_or(root)
}

#[derive(Clone, Copy)]
struct Resolved {
    root: &'static Registered,
    /// The registered subcommand, or `root` when there is none.
    spec: &'static Registered,
}

/// Validation reached so far; only successes are remembered.
#[derive(Clone)]
enum Phase {
    Resolved,
    /// Identity and arity validated.
    Admitted,
    /// Admitted and counted keys parsed: the effective command's key positions,
    /// `None` for a fixed single key, which [`Facts::fixed_key`] describes.
    Checked(Option<key_specs::KeyPositions>),
}

/// One invocation bound to its immutable arguments. Binding resolves the root
/// and effective registrations once; [`Self::admit`] and [`Self::check`] run
/// identity/arity and counted-key validation lazily, at each caller's original
/// point, and remember success so later phases and the engine reuse it. The
/// arguments are private and never re-pointed, so remembered facts and key
/// positions always describe exactly these arguments; a newly built vector
/// binds again.
#[derive(Clone)]
pub(crate) struct BoundCommand<'a> {
    args: &'a [Bytes],
    resolved: Option<Resolved>,
    phase: Phase,
}

impl<'a> BoundCommand<'a> {
    /// Pure: resolves registrations without validating anything.
    pub(crate) fn bind(args: &'a [Bytes]) -> Self {
        Self {
            args,
            resolved: root(args).map(|root| Resolved {
                root,
                spec: effective(root, args),
            }),
            phase: Phase::Resolved,
        }
    }

    pub(crate) fn args(&self) -> &'a [Bytes] {
        self.args
    }

    /// Canonical (lowercase) name of the registered root, e.g. `config` for
    /// `CONFIG GET`. `None` for unknown, `a|b` and dynamic module names.
    pub(crate) fn root_name(&self) -> Option<&'static str> {
        self.resolved.map(|resolved| resolved.root.spec.name)
    }

    /// The server-runtime command this invocation's root registration names.
    pub(crate) fn server_command(&self) -> Option<ServerCommand> {
        self.resolved
            .and_then(|resolved| resolved.root.facts.server)
    }

    /// Identity and arity checks in their error precedence.
    pub(crate) fn admit(&mut self) -> Result<(), Resp> {
        if !matches!(self.phase, Phase::Resolved) {
            return Ok(());
        }
        let args = self.args;
        let Some(Resolved { root: entry, spec }) = self.resolved else {
            return Err(error(if args.is_empty() {
                "ERR empty command"
            } else {
                "ERR unknown command"
            }));
        };
        let root = &entry.spec;
        if !root.accepts(args.len()) {
            return Err(wrong_arity(root.name));
        }
        if std::ptr::eq(spec, entry)
            && args.len() > 1
            && entry.container
            && !(args.len() == 2 && args[1].eq_ignore_ascii_case(b"HELP"))
        {
            return Err(error("ERR unknown subcommand"));
        }
        if !spec.spec.accepts(args.len()) {
            return Err(wrong_arity(spec.spec.name));
        }
        self.phase = Phase::Admitted;
        Ok(())
    }

    /// Full validation: admission followed by counted-key parsing of the
    /// effective command and then, for subcommands, of the root.
    pub(crate) fn check(&mut self) -> Result<(), Resp> {
        if matches!(self.phase, Phase::Checked(_)) {
            return Ok(());
        }
        self.admit()?;
        let Resolved { root, spec } = self.resolved.expect("admitted command is registered");
        // Fixed single-key specifications cannot fail, so only others parse.
        let keys = match spec.facts.fixed_key {
            Some(_) => None,
            None => spec.spec.key_positions(self.args)?,
        };
        if !std::ptr::eq(spec, root) && root.facts.fixed_key.is_none() {
            root.spec.key_positions(self.args)?;
        }
        self.phase = Phase::Checked(keys);
        Ok(())
    }

    /// A builtin registration, including `<root>|<subcommand>` names, which
    /// exist but never bind as roots and so fail admission as unknown.
    pub(crate) fn exists(&self) -> bool {
        self.resolved.is_some()
            || self
                .args
                .first()
                .is_some_and(|name| name.contains(&b'|') && registered(name).is_some())
    }

    /// The effective registered name, or empty when unregistered.
    pub(crate) fn name(&self) -> &'static str {
        self.spec().map_or("", |spec| spec.name)
    }

    fn facts(&self) -> Option<&'static Facts> {
        self.resolved.map(|resolved| &resolved.spec.facts)
    }

    pub(crate) fn is_readonly(&self) -> bool {
        self.facts().is_some_and(|facts| facts.readonly)
    }

    pub(crate) fn is_dataset_write(&self) -> bool {
        self.facts().is_some_and(|facts| facts.dataset_write)
    }

    pub(crate) fn is_pause_write(&self) -> bool {
        self.facts().is_some_and(|facts| facts.pause_write)
    }

    pub(crate) fn script_allowed(&self) -> bool {
        self.facts().is_some_and(|facts| facts.script_allowed)
    }

    pub(crate) fn transaction_allowed(&self) -> bool {
        self.facts().is_some_and(|facts| facts.transaction_allowed)
    }

    pub(crate) fn monitor_allowed(&self) -> bool {
        self.facts().is_some_and(|facts| facts.monitor_allowed)
    }

    /// Classified by the root registration.
    pub(crate) fn is_control(&self) -> bool {
        self.resolved
            .is_some_and(|resolved| resolved.root.facts.control)
    }

    pub(super) fn spec(&self) -> Option<&'static CommandSpec> {
        self.resolved.map(|resolved| &resolved.spec.spec)
    }

    pub(super) fn root_spec(&self) -> Option<&'static CommandSpec> {
        self.resolved.map(|resolved| &resolved.root.spec)
    }

    /// The effective command's fixed single-key argument, present or not.
    pub(super) fn fixed_key(&self) -> Option<usize> {
        self.facts().and_then(|facts| facts.fixed_key)
    }

    /// Key positions of the effective command: remembered once checked, else
    /// parsed on demand, even when admission would reject the command.
    pub(super) fn access_positions(&self) -> Option<key_specs::KeyPositions> {
        match self.fixed_key() {
            Some(position) => {
                (position < self.args.len()).then(|| key_specs::KeyPositions::single(position))
            }
            None => self.parsed_positions(),
        }
    }

    /// [`Self::access_positions`] for an effective command without a fixed key.
    fn parsed_positions(&self) -> Option<key_specs::KeyPositions> {
        match &self.phase {
            Phase::Checked(positions) => positions.clone(),
            Phase::Resolved | Phase::Admitted => self
                .spec()
                .and_then(|spec| spec.key_positions(self.args).ok().flatten()),
        }
    }

    pub(crate) fn accessed_keys(&self) -> AccessedKeys {
        match self.fixed_key() {
            Some(position) => self.args.get(position).cloned().into_iter().collect(),
            None => self
                .parsed_positions()
                .map(|positions| positions.map(|index| self.args[index].clone()).collect())
                .unwrap_or_default(),
        }
    }

    pub(crate) fn any_accessed_key(&self, mut predicate: impl FnMut(&Bytes) -> bool) -> bool {
        self.facts().is_some_and(|facts| {
            facts.requires_all_keys
                || match facts.fixed_key {
                    Some(position) => self.args.get(position).is_some_and(predicate),
                    None => self.parsed_positions().is_some_and(|mut positions| {
                        positions.any(|index| predicate(&self.args[index]))
                    }),
                }
        })
    }

    pub(crate) fn routing(&self, shard_count: usize) -> Routing {
        let local_error = || Routing::Local {
            shard: 0,
            ready: None,
        };
        let Some(Resolved { spec, .. }) = self.resolved else {
            return local_error();
        };
        let args = self.args;
        // Admission already proved the effective arity.
        if matches!(self.phase, Phase::Resolved) && !spec.spec.accepts(args.len()) {
            return local_error();
        }
        let latest_start = match spec.facts.latest {
            Latest::Never => None,
            Latest::At(start) => Some(start),
            Latest::AfterCount => args
                .get(1)
                .and_then(|value| integer(value))
                .and_then(|count| usize::try_from(count).ok())
                .and_then(|count| count.checked_add(4)),
        };
        if latest_start
            .and_then(|start| args.get(start..))
            .is_some_and(|options| {
                options
                    .iter()
                    .any(|value| value.eq_ignore_ascii_case(b"LATEST"))
            })
        {
            return Routing::All;
        }
        let (ready, coordinated) = match spec.facts.route {
            Route::Scripting => return Routing::Scripting,
            Route::Maintenance => return Routing::Maintenance,
            Route::Global(global) => return Routing::Global(global),
            Route::All => return Routing::All,
            Route::Keyed { ready, coordinated } => (ready, coordinated),
        };
        if let Some(position) = spec.facts.fixed_key {
            let Some(key) = args.get(position) else {
                return local_error();
            };
            let ready = ready.map(|index| args[index].clone());
            return if coordinated {
                Routing::Coordinated {
                    keys: vec![key.clone()],
                    ready,
                }
            } else {
                Routing::Local {
                    shard: Engine::shard_for(key, shard_count),
                    ready,
                }
            };
        }
        let Some(positions) = self.parsed_positions() else {
            return local_error();
        };
        let first = positions
            .clone()
            .next()
            .expect("nonempty key specification");
        let shard = Engine::shard_for(&args[first], shard_count);
        let ready = ready.map(|index| args[index].clone());
        if !coordinated
            && positions
                .clone()
                .skip(1)
                .all(|index| Engine::shard_for(&args[index], shard_count) == shard)
        {
            Routing::Local { shard, ready }
        } else {
            Routing::Coordinated {
                keys: positions.map(|index| args[index].clone()).collect(),
                ready,
            }
        }
    }

    /// ACL decision for the effective registration. Admits first; callers that
    /// require counted-key errors to win over NOPERM call [`Self::check`] first.
    pub(crate) fn authorize(&mut self, user: &User, audit: Option<&Audit<'_>>) -> Result<(), Resp> {
        self.admit()?;
        let spec = self.spec().expect("admitted command is registered");
        Engine::authorize_spec(spec, self.args, || self.access_positions(), user, audit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&[u8]]) -> Vec<Bytes> {
        values
            .iter()
            .map(|value| Bytes::copy_from_slice(value))
            .collect()
    }

    fn effective_name(values: &[&[u8]]) -> Option<&'static str> {
        BoundCommand::bind(&args(values))
            .spec()
            .map(|spec| spec.name)
    }

    #[test]
    fn lookups_fold_ascii_case_and_reject_foreign_names() {
        let get = lookup(b"get").expect("registered get");
        assert!(std::ptr::eq(lookup(b"GeT").unwrap(), get));
        assert!(std::ptr::eq(
            BoundCommand::bind(&args(&[b"GET", b"k"]))
                .root_spec()
                .unwrap(),
            get
        ));
        assert!(lookup(b"").is_none());
        assert!(lookup(b"no-such-command").is_none());
        assert!(lookup(b"g\xffet").is_none());
        assert!(lookup(b"G\xc3\x89T").is_none());
        assert!(lookup(&[b'a'; MAX_NAME + 1]).is_none());
        assert!(BoundCommand::bind(&[]).spec().is_none());
        let composite = args(&[b"config|get"]);
        assert!(BoundCommand::bind(&composite).root_spec().is_none());
        assert!(BoundCommand::bind(&composite).exists());
        assert!(effective_name(&[b"CONFIG|GET", b"x"]).is_none());
        assert!(!BoundCommand::bind(&args(&[b"no-such-command"])).exists());
    }

    #[test]
    fn subcommands_resolve_case_insensitively_and_fall_back_to_the_container() {
        for case in [&b"get"[..], b"GET", b"gEt"] {
            assert_eq!(
                effective_name(&[b"CoNfIg", case, b"maxmemory"]),
                Some("config|get")
            );
        }
        for unknown in [&b"nope"[..], b"get|x", b"\xff", &[b'g'; MAX_NAME]] {
            assert_eq!(effective_name(&[b"config", unknown]), Some("config"));
        }
        assert_eq!(effective_name(&[b"config"]), Some("config"));
        // A non-container never consults its first argument.
        assert_eq!(effective_name(&[b"get", b"config"]), Some("get"));
    }

    #[test]
    fn admission_checks_identity_then_root_arity_then_subcommand() {
        type Admission = Result<(&'static str, &'static str), Resp>;
        fn admit(values: &[&[u8]]) -> Admission {
            let args = args(values);
            let mut command = BoundCommand::bind(&args);
            command.admit().map(|()| {
                (
                    command.root_spec().unwrap().name,
                    command.spec().unwrap().name,
                )
            })
        }
        fn failure(message: &str) -> Admission {
            Err(Resp::Error(message.to_owned()))
        }
        fn arity(name: &str) -> Admission {
            failure(&format!(
                "ERR wrong number of arguments for '{name}' command"
            ))
        }
        assert_eq!(admit(&[]), failure("ERR empty command"));
        assert_eq!(admit(&[b"no-such-command"]), failure("ERR unknown command"));
        assert_eq!(
            admit(&[b"config|get", b"x"]),
            failure("ERR unknown command")
        );
        // Root arity wins over both subcommand resolution and its arity.
        assert_eq!(admit(&[b"CONFIG"]), arity("config"));
        assert_eq!(admit(&[b"BACKUP", b"START", b"extra"]), arity("backup"));
        assert_eq!(admit(&[b"backup", b"nope", b"extra"]), arity("backup"));
        assert_eq!(admit(&[b"config", b"HeLp"]), Ok(("config", "config")));
        let unknown = failure("ERR unknown subcommand");
        assert_eq!(admit(&[b"CONFIG", b"HELP", b"x"]), unknown);
        assert_eq!(admit(&[b"CONFIG", b"nope"]), unknown);
        assert_eq!(admit(&[b"config", &[b'g'; MAX_NAME][..]]), unknown);
        assert_eq!(admit(&[b"CONFIG", b"GET"]), arity("config|get"));
        assert_eq!(
            admit(&[b"config", b"GET", b"maxmemory"]),
            Ok(("config", "config|get"))
        );
        assert_eq!(admit(&[b"COMMAND"]), Ok(("command", "command")));
        // A non-container never treats its first argument as a subcommand.
        assert_eq!(admit(&[b"PING", b"config"]), Ok(("ping", "ping")));
        assert_eq!(admit(&[b"get", b"k", b"x"]), arity("get"));
    }

    #[test]
    fn remembered_hits_match_only_the_same_name_and_survive_eviction() {
        let names: [&[u8]; RECENT + 2] = [b"get", b"set", b"del", b"mget", b"mset", b"incr"];
        let expected: Vec<_> = names.iter().map(|name| lookup(name).unwrap()).collect();
        for _ in 0..3 {
            for (name, spec) in names.iter().zip(&expected) {
                assert!(std::ptr::eq(lookup(name).unwrap(), *spec));
                assert!(std::ptr::eq(
                    lookup(&name.to_ascii_uppercase()).unwrap(),
                    *spec
                ));
            }
        }
        lookup(b"get").unwrap();
        for near in [&b"ge"[..], b"gets", b"get\0", b"g\xc5t", b"GE\xd4"] {
            assert!(lookup(near).is_none(), "{near:?}");
        }
        let mut long = b"GET".to_vec();
        long.resize(MAX_NAME + 1, b' ');
        assert!(lookup(&long).is_none());
    }

    #[test]
    fn key_extraction_and_routing_follow_parsed_specifications_for_every_registration() {
        const SHARDS: usize = 7;
        for entry in REGISTRY.values() {
            let name = entry.spec.name;
            let prefix: Vec<&[u8]> = name.split('|').map(str::as_bytes).collect();
            for len in 1..prefix.len() + 6 {
                // Integers, so counted keys also parse; distinct, so keys identify positions.
                let values: Vec<Bytes> = (0..len)
                    .map(|index| match prefix.get(index) {
                        Some(part) => Bytes::copy_from_slice(part),
                        None => Bytes::from(index.to_string()),
                    })
                    .collect();
                let mut command = BoundCommand::bind(&values);
                if !command
                    .spec()
                    .is_some_and(|spec| std::ptr::eq(spec, &entry.spec))
                {
                    continue;
                }
                let positions: Vec<usize> = entry
                    .spec
                    .key_positions(&values)
                    .ok()
                    .flatten()
                    .map_or_else(Vec::new, |found| found.collect());
                let keys: Vec<Bytes> = positions.iter().map(|&at| values[at].clone()).collect();
                for checked in [false, true] {
                    if checked && command.check().is_err() {
                        break;
                    }
                    let context = format!("{name} {len} checked={checked}");
                    let found = command.access_positions();
                    assert_eq!(found.is_some(), !positions.is_empty(), "{context}");
                    let length = found.as_ref().map_or(0, ExactSizeIterator::len);
                    let forward: Vec<usize> = found.clone().into_iter().flatten().collect();
                    let mut backward: Vec<usize> = found.into_iter().flatten().rev().collect();
                    backward.reverse();
                    assert_eq!(length, positions.len(), "{context}");
                    assert_eq!(forward, positions, "{context}");
                    assert_eq!(backward, positions, "{context}");
                    assert_eq!(command.accessed_keys().into_vec(), keys, "{context}");
                    assert_eq!(
                        command.any_accessed_key(|key| keys.last() == Some(key)),
                        entry.facts.requires_all_keys || !keys.is_empty(),
                        "{context}"
                    );
                    match command.routing(SHARDS) {
                        Routing::Coordinated { keys: routed, .. } => {
                            assert_eq!(routed, keys, "{context}");
                        }
                        Routing::Local { shard, .. } if entry.spec.accepts(len) => assert_eq!(
                            shard,
                            keys.first().map_or(0, |key| Engine::shard_for(key, SHARDS)),
                            "{context}"
                        ),
                        _ => {}
                    }
                }
            }
        }
    }
}
