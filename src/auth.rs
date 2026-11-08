//! Mutable ACL directory with immutable, request-scoped user snapshots.
//! ACL files are parsed completely before replacement; password material is
//! hashed at the boundary and never included in errors or denial logs.
use crate::protocol::Resp;
use bytes::Bytes;
use rand::TryRngCore;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use subtle::{Choice, ConstantTimeEq};

mod namespace;

const MAX_FILE_BYTES: usize = 1024 * 1024;
const MAX_LINE_BYTES: usize = 64 * 1024;
const MAX_USERS: usize = 1024;
const MAX_RULES_PER_USER: usize = 1024;
const MAX_TOTAL_RULES: usize = 16 * 1024;
const MAX_PATTERNS_PER_USER: usize = 128;
const MAX_TOTAL_PATTERNS: usize = 4096;
const MAX_PATTERN_BYTES: usize = 1024;
const MAX_PASSWORD_BYTES: usize = 1024;
const MAX_PASSWORDS: usize = 32;
const MAX_USERNAME_BYTES: usize = 128;
const MAX_COMMAND_BYTES: usize = 128;
const MAX_GLOB_WORK: usize = 8 * 1024 * 1024;
const MAX_LOG_ENTRIES: usize = 128;

pub struct Acl {
    /// Published snapshots. Writers mark every replaced or removed snapshot
    /// superseded before releasing this lock.
    users: RwLock<HashMap<String, Arc<User>>>,
    path: Option<PathBuf>,
    file_lock: Mutex<()>,
    log: Mutex<DenialLog>,
}

/// One user snapshot. Its policy is immutable once published and shared by
/// every clone, so a connection's own handle on a snapshot costs one small
/// allocation, and counting references to that handle for each command
/// writes no line another connection uses. Mutation copies the policy first.
#[derive(Clone)]
pub struct User {
    policy: Arc<Policy>,
    superseded: Superseded,
}

/// A user's rules and identity, read through [`User`].
#[derive(Clone)]
pub struct Policy {
    name: String,
    identity: Arc<()>,
    enabled: bool,
    nopass: bool,
    password_hashes: Vec<[u8; 32]>,
    root: Selector,
    selectors: Vec<Selector>,
    namespace_cache: Arc<Mutex<namespace::Cache>>,
}

impl std::ops::Deref for User {
    type Target = Policy;

    fn deref(&self) -> &Policy {
        &self.policy
    }
}

impl std::ops::DerefMut for User {
    /// Copies a policy other clones share before changing it.
    fn deref_mut(&mut self) -> &mut Policy {
        Arc::make_mut(&mut self.policy)
    }
}

/// Set once, inside the directory writer's critical section and so before
/// ACL SETUSER, DELUSER or LOAD is acknowledged, when the entry publishing
/// this snapshot is replaced or removed. Never cleared: no path republishes a
/// removed snapshot.
#[derive(Default)]
struct Superseded(AtomicBool);

impl Clone for Superseded {
    /// A clone is a distinct snapshot that no directory has published, so it
    /// starts current whatever its source's state.
    fn clone(&self) -> Self {
        Self::default()
    }
}

#[derive(Clone, Default)]
struct Selector {
    command_rules: Vec<CommandRule>,
    key_patterns: Vec<KeyPattern>,
    channel_patterns: Vec<(Bytes, Glob)>,
}

#[derive(Clone)]
struct KeyPattern {
    source: Bytes,
    glob: Glob,
    access: KeyAccess,
}

/// Required access to a particular key, not merely the command's write flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyAccess {
    Read,
    Write,
    ReadWrite,
}

#[derive(Clone)]
struct CommandRule {
    allow: bool,
    target: Target,
}

#[derive(Clone)]
enum Target {
    All,
    Command(Vec<u8>),
    Category(String),
}

#[derive(Clone)]
pub(crate) struct Glob {
    atoms: Vec<Atom>,
    literal: bool,
    work_width: usize,
}

#[derive(Clone)]
enum Atom {
    Literal(u8),
    Any,
    Many,
    Class {
        negated: bool,
        ranges: Vec<(u8, u8)>,
    },
}

#[derive(Default)]
struct DenialLog {
    entries: VecDeque<Denial>,
    next_id: i64,
}

struct Denial {
    count: i64,
    reason: String,
    context: String,
    object: Bytes,
    username: String,
    client_info: String,
    id: i64,
    created: i64,
    updated: i64,
}
/// Request-local authorization audit. Client metadata is rendered only on a
/// denial, and command arguments never enter the log.
pub struct Audit<'a> {
    pub acl: &'a Acl,
    pub clients: &'a crate::client_registry::Registry,
    pub client_id: u64,
    pub context: &'static str,
}
impl Audit<'_> {
    fn record(&self, user: &User, reason: &str, object: &[u8]) {
        let info = self.clients.info(self.client_id);
        self.acl.record_denial(
            user.name(),
            reason,
            self.context,
            object,
            &String::from_utf8_lossy(&info),
        );
    }
}

impl Default for Acl {
    fn default() -> Self {
        Self::new()
    }
}

impl Acl {
    /// Redis's unconfigured default identity. Loading a file remains fail-closed:
    /// it does not implicitly insert this identity into that file's users.
    pub fn new() -> Self {
        let mut user = User::empty("default");
        user.enabled = true;
        user.nopass = true;
        user.root.apply(b"+@all").expect("constant rule");
        user.root.apply(b"~*").expect("constant rule");
        user.root.apply(b"&*").expect("constant rule");
        Self::from_users(HashMap::from([("default".into(), Arc::new(user))]))
    }

    fn from_users(users: HashMap<String, Arc<User>>) -> Self {
        Self {
            users: RwLock::new(users),
            path: None,
            file_lock: Mutex::new(()),
            log: Mutex::new(DenialLog::default()),
        }
    }

    pub fn load(path: &Path) -> io::Result<Self> {
        let mut acl = Self::parse_file(path)?;
        acl.path = Some(path.to_owned());
        Ok(acl)
    }

    /// Startup integration supplies the authoritative engine registry after
    /// loading a file. This also validates grants on disabled users/selectors.
    pub fn validate_commands(&self, commands: &[String]) -> io::Result<()> {
        let users = self.users.read().unwrap_or_else(|e| e.into_inner());
        if users.values().all(|user| user.valid_commands(commands)) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "ACL contains an unknown command or subcommand",
            ))
        }
    }

    fn parse_file(path: &Path) -> io::Result<Self> {
        let file = File::open(path)?;
        let mut contents = Vec::new();
        file.take((MAX_FILE_BYTES + 1) as u64)
            .read_to_end(&mut contents)?;
        let result = Self::parse(&contents);
        contents.fill(0);
        result
    }

    fn parse(contents: &[u8]) -> io::Result<Self> {
        if contents.len() > MAX_FILE_BYTES {
            return Err(invalid(1, "ACL file exceeds size limit"));
        }
        let mut users = HashMap::new();
        let mut total_rules = 0;
        let mut total_patterns = 0;
        let mut effective_rules = 0;
        let mut effective_patterns = 0;
        for (index, line) in contents.split(|byte| *byte == b'\n').enumerate() {
            let line_number = index + 1;
            if line.len() > MAX_LINE_BYTES {
                return Err(invalid(line_number, "line exceeds size limit"));
            }
            let tokens = split_acl_line(line).map_err(|reason| invalid(line_number, reason))?;
            if tokens.is_empty() || tokens[0].starts_with(b"#") {
                continue;
            }
            if tokens[0] != b"user" || tokens.len() < 3 {
                return Err(invalid(line_number, "expected user declaration and rules"));
            }
            let name = valid_username(&tokens[1]).map_err(|reason| invalid(line_number, reason))?;
            if users.contains_key(name) {
                return Err(invalid(line_number, "duplicate user declaration"));
            }
            if users.len() >= MAX_USERS {
                return Err(invalid(line_number, "user count exceeds limit"));
            }
            let mut user = User::empty(name);
            let rules: Vec<&[u8]> = tokens[2..].iter().map(Vec::as_slice).collect();
            total_rules += rules.len();
            total_patterns += rules.iter().filter(|rule| is_pattern_rule(rule)).count();
            if total_rules > MAX_TOTAL_RULES || total_patterns > MAX_TOTAL_PATTERNS {
                return Err(invalid(line_number, "total rule limit exceeded"));
            }
            user.apply_rules(&rules)
                .map_err(|reason| invalid(line_number, reason))?;
            let (rule_count, pattern_count) = user.counts();
            effective_rules += rule_count;
            effective_patterns += pattern_count;
            if effective_rules > MAX_TOTAL_RULES || effective_patterns > MAX_TOTAL_PATTERNS {
                return Err(invalid(line_number, "total rule limit exceeded"));
            }
            users.insert(name.to_owned(), Arc::new(user));
        }
        Ok(Self::from_users(users))
    }

    /// Fresh lookup deliberately includes off users: off disables subsequent
    /// authentication, not sessions already authenticated as that identity.
    pub fn user(&self, name: &str) -> Option<Arc<User>> {
        self.users
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .cloned()
    }

    pub fn authenticate(&self, username: &[u8], password: &[u8]) -> Option<Arc<User>> {
        let name = std::str::from_utf8(username).unwrap_or("");
        let result = self.user(name).filter(|user| {
            if !user.enabled || password.len() > MAX_PASSWORD_BYTES {
                return false;
            }
            let hash: [u8; 32] = Sha256::digest(password).into();
            let mut matched = Choice::from(u8::from(user.nopass));
            for candidate in &user.password_hashes {
                matched |= candidate.ct_eq(&hash);
            }
            bool::from(matched)
        });
        if result.is_none() {
            self.record_denial(name, "auth", "toplevel", b"AUTH", "");
        }
        result
    }

    pub fn default_user(&self) -> Option<Arc<User>> {
        self.user("default")
            .filter(|user| user.enabled && user.nopass)
    }

    pub fn default_is_passwordless(&self) -> bool {
        self.users
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .get("default")
            .is_some_and(|user| user.nopass)
    }

    /// `args` includes ACL. The catalog callback supplies all canonical command
    /// names for None, or members of a real engine category for Some(category).
    /// Subcommands use `parent|child`. DRYRUN never executes a command.
    pub fn execute(
        &self,
        args: &[Bytes],
        current_user: &str,
        authorize: impl Fn(&[Bytes], &User) -> Result<(), Resp>,
        commands: impl Fn(Option<&str>) -> Vec<String>,
    ) -> Resp {
        let Some(sub) = args.get(1) else {
            return acl_error("wrong number of arguments for ACL");
        };
        let sub = sub.to_ascii_uppercase();
        match sub.as_slice() {
            b"CAT" if (2..=3).contains(&args.len()) => {
                if args.len() == 2 {
                    return array(
                        CATEGORIES
                            .iter()
                            .map(|name| bulk(name.as_bytes()))
                            .collect(),
                    );
                }
                let category = String::from_utf8_lossy(&args[2]).to_ascii_lowercase();
                if !known_category(category.as_bytes()) {
                    return acl_error("Unknown category");
                }
                let mut names = commands(Some(&category));
                names.sort_unstable();
                names.dedup();
                array(
                    names
                        .into_iter()
                        .map(|name| bulk(name.into_bytes()))
                        .collect(),
                )
            }
            b"SETUSER" if args.len() >= 3 => {
                let name = match valid_username(&args[2]) {
                    Ok(name) => name,
                    Err(reason) => return acl_error(reason),
                };
                let rules: Vec<&[u8]> = args[3..].iter().map(Bytes::as_ref).collect();
                let catalog = commands(None);
                let mut users = self.users.write().unwrap_or_else(|e| e.into_inner());
                if !users.contains_key(name) && users.len() >= MAX_USERS {
                    return acl_error("user count exceeds limit");
                }
                let mut user = users
                    .get(name)
                    .map(|user| (**user).clone())
                    .unwrap_or_else(|| User::empty(name));
                if let Err(reason) = user.apply_rules(&rules) {
                    return acl_error(reason);
                }
                // The registry, not a stale hard-coded command list, validates
                // grants. First-argument grants are accepted only additively.
                if !user.valid_commands(&catalog) {
                    return acl_error("Unknown command or subcommand in ACL rule");
                }
                let (mut rules, mut patterns) = user.counts();
                for existing in users.values().filter(|user| user.name != name) {
                    let (r, p) = existing.counts();
                    rules += r;
                    patterns += p;
                }
                if rules > MAX_TOTAL_RULES || patterns > MAX_TOTAL_PATTERNS {
                    return acl_error("total rule limit exceeded");
                }
                if user.dump().len() > MAX_LINE_BYTES {
                    return acl_error("ACL user declaration exceeds size limit");
                }
                if let Some(previous) = users.insert(name.to_owned(), Arc::new(user)) {
                    previous.supersede();
                }
                Resp::Simple("OK")
            }
            b"DELUSER" if args.len() >= 3 => {
                // Validate the entire request before deleting anything.
                if args[2..].iter().any(|name| name.as_ref() == b"default") {
                    return acl_error("The 'default' user cannot be removed");
                }
                let mut users = self.users.write().unwrap_or_else(|e| e.into_inner());
                let mut removed = 0;
                for name in &args[2..] {
                    if let Ok(name) = std::str::from_utf8(name)
                        && let Some(previous) = users.remove(name)
                    {
                        previous.supersede();
                        removed += 1;
                    }
                }
                Resp::Integer(removed)
            }
            b"DRYRUN" if args.len() >= 4 => {
                let Some(user) = std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|name| self.user(name))
                else {
                    return acl_error("User not found");
                };
                match authorize(&args[3..], &user) {
                    Ok(()) => Resp::Simple("OK"),
                    Err(Resp::Error(message)) if message.starts_with("NOPERM ") => {
                        bulk(&message.as_bytes()[7..])
                    }
                    Err(reply) => reply,
                }
            }
            b"GENPASS" if (2..=3).contains(&args.len()) => {
                let bits = if args.len() == 2 {
                    Some(256)
                } else {
                    parse_count(&args[2])
                };
                let Some(bits @ 1..=1024) = bits else {
                    return acl_error("ACL GENPASS argument must be between 1 and 1024");
                };
                let digits = bits.div_ceil(4);
                let mut random = [0u8; 128];
                if rand::rngs::OsRng
                    .try_fill_bytes(&mut random[..digits.div_ceil(2)])
                    .is_err()
                {
                    return acl_error("secure random generator unavailable");
                }
                let mut encoded = hex(&random[..digits.div_ceil(2)]);
                encoded.truncate(digits);
                random.fill(0);
                bulk(encoded)
            }
            b"GETUSER" if args.len() == 3 => {
                match std::str::from_utf8(&args[2])
                    .ok()
                    .and_then(|name| self.user(name))
                {
                    Some(user) => user.reply(),
                    None => Resp::Bulk(None),
                }
            }
            b"LIST" if args.len() == 2 => {
                let users = self.users.read().unwrap_or_else(|e| e.into_inner());
                let mut names: Vec<_> = users.keys().collect();
                names.sort_unstable();
                array(
                    names
                        .into_iter()
                        .map(|name| bulk(users[name].dump()))
                        .collect(),
                )
            }
            b"USERS" if args.len() == 2 => {
                let users = self.users.read().unwrap_or_else(|e| e.into_inner());
                let mut names: Vec<_> = users.keys().collect();
                names.sort_unstable();
                array(
                    names
                        .into_iter()
                        .map(|name| bulk(name.as_bytes()))
                        .collect(),
                )
            }
            b"WHOAMI" if args.len() == 2 => bulk(current_user.as_bytes()),
            b"LOAD" if args.len() == 2 => {
                let _guard = self.file_lock.lock().unwrap_or_else(|e| e.into_inner());
                let Some(path) = &self.path else {
                    return acl_error("This Redis instance is not configured to use an ACL file");
                };
                match Self::parse_file(path) {
                    Ok(loaded) => {
                        let mut parsed =
                            loaded.users.into_inner().unwrap_or_else(|e| e.into_inner());
                        let catalog = commands(None);
                        if parsed.values().any(|user| !user.valid_commands(&catalog)) {
                            return acl_error(
                                "ACL file contains unknown commands; active rules were not changed",
                            );
                        }
                        let mut users = self.users.write().unwrap_or_else(|e| e.into_inner());
                        for (name, user) in &mut parsed {
                            if let Some(existing) = users.get(name) {
                                Arc::make_mut(user).identity = existing.identity.clone();
                            }
                        }
                        // Every loaded snapshot replaces its predecessor, even
                        // one with unchanged rules and identity.
                        for previous in std::mem::replace(&mut *users, parsed).values() {
                            previous.supersede();
                        }
                        Resp::Simple("OK")
                    }
                    Err(_) => {
                        acl_error("ACL file could not be loaded; active rules were not changed")
                    }
                }
            }
            b"SAVE" if args.len() == 2 => match self.save() {
                Ok(()) => Resp::Simple("OK"),
                Err(error) => acl_error(&format!("ACL SAVE failed: {error}")),
            },
            b"LOG" if (2..=3).contains(&args.len()) => {
                let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
                if args
                    .get(2)
                    .is_some_and(|arg| arg.eq_ignore_ascii_case(b"RESET"))
                {
                    log.entries.clear();
                    return Resp::Simple("OK");
                }
                let count = if args.len() == 2 {
                    Some(10)
                } else {
                    parse_count(&args[2])
                };
                let Some(count) = count else {
                    return acl_error("value is not an integer or out of range");
                };
                let now = unix_millis();
                array(
                    log.entries
                        .iter()
                        .take(count.min(MAX_LOG_ENTRIES))
                        .map(|entry| entry.reply(now))
                        .collect(),
                )
            }
            b"HELP" if args.len() == 2 => array(
                [
                    "CAT [category]",
                    "DELUSER username [username ...]",
                    "DRYRUN username command [arg ...]",
                    "GENPASS [bits]",
                    "GETUSER username",
                    "LIST",
                    "LOAD",
                    "LOG [count | RESET]",
                    "SAVE",
                    "SETUSER username [rule ...]",
                    "USERS",
                    "WHOAMI",
                ]
                .iter()
                .map(|line| bulk(line.as_bytes()))
                .collect(),
            ),
            _ => acl_error("Unknown ACL subcommand or wrong number of arguments"),
        }
    }

    fn save(&self) -> io::Result<()> {
        let _guard = self.file_lock.lock().unwrap_or_else(|e| e.into_inner());
        let path = self
            .path
            .as_ref()
            .ok_or_else(|| io::Error::other("no ACL file configured"))?;
        let users = self.users.read().unwrap_or_else(|e| e.into_inner());
        let mut names: Vec<_> = users.keys().collect();
        names.sort_unstable();
        let mut contents = Vec::new();
        for name in names {
            let line = users[name].dump();
            if line.len() > MAX_LINE_BYTES {
                return Err(io::Error::other("ACL user declaration exceeds size limit"));
            }
            contents.extend(line);
            contents.push(b'\n');
        }
        if contents.len() > MAX_FILE_BYTES {
            return Err(io::Error::other("ACL file exceeds size limit"));
        }
        let mut random = [0; 16];
        rand::rngs::OsRng
            .try_fill_bytes(&mut random)
            .map_err(|_| io::Error::other("secure random generator unavailable"))?;
        let parent = path
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or(Path::new("."));
        let temporary = parent.join(format!(
            ".ffkv-acl-{}.tmp",
            String::from_utf8(hex(&random)).expect("hex")
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        let mut file = options.open(&temporary)?;
        let result = (|| {
            file.write_all(&contents)?;
            file.sync_all()?;
            std::fs::rename(&temporary, path)?;
            File::open(parent)?.sync_all()
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        result
    }

    /// Logs only the denied resource, never command arguments or credentials.
    /// Repeated matching events within 60 seconds coalesce and become newest.
    pub fn record_denial(
        &self,
        username: &str,
        reason: &str,
        context: &str,
        object: &[u8],
        client_info: &str,
    ) {
        if !matches!(reason, "auth" | "command" | "key" | "channel") {
            return;
        }
        let now = unix_millis();
        let object = &object[..object.len().min(MAX_PATTERN_BYTES)];
        let username = truncate_utf8(username, MAX_USERNAME_BYTES);
        let context = match context {
            "multi" | "lua" | "module" => context,
            _ => "toplevel",
        };
        let mut log = self.log.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(index) = log.entries.iter().position(|entry| {
            entry.reason == reason
                && entry.context == context
                && entry.object.as_ref() == object
                && entry.username == username
                && now.saturating_sub(entry.updated) <= 60_000
        }) {
            let mut entry = log.entries.remove(index).expect("existing index");
            entry.count = entry.count.saturating_add(1);
            entry.updated = now;
            entry.client_info = truncate_utf8(client_info, 2048).to_owned();
            log.entries.push_front(entry);
        } else {
            let id = log.next_id;
            log.next_id = log.next_id.saturating_add(1);
            log.entries.push_front(Denial {
                count: 1,
                reason: reason.into(),
                context: context.into(),
                object: Bytes::copy_from_slice(object),
                username: username.into(),
                client_info: truncate_utf8(client_info, 2048).into(),
                id,
                created: now,
                updated: now,
            });
            log.entries.truncate(MAX_LOG_ENTRIES);
        }
    }
}

impl User {
    fn empty(name: &str) -> Self {
        Self {
            policy: Arc::new(Policy {
                name: name.into(),
                identity: Arc::new(()),
                enabled: false,
                nopass: false,
                password_hashes: Vec::new(),
                root: Selector::default(),
                selectors: Vec::new(),
                namespace_cache: Arc::default(),
            }),
            superseded: Superseded::default(),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Changes to an existing user preserve identity; delete/recreate does not.
    pub fn same_identity(&self, other: &User) -> bool {
        Arc::ptr_eq(&self.identity, &other.identity)
    }

    /// True once the directory no longer publishes this snapshot. Holders of
    /// a directory snapshot reuse it until then and must then look its name
    /// up again; that lookup takes the directory lock, so it observes the
    /// replacement published in the same writer critical section.
    pub fn superseded(&self) -> bool {
        self.superseded.0.load(Ordering::Acquire)
    }

    fn supersede(&self) {
        self.superseded.0.store(true, Ordering::Release);
    }

    fn valid_commands(&self, catalog: &[String]) -> bool {
        std::iter::once(&self.root)
            .chain(&self.selectors)
            .all(|selector| {
                selector.command_rules.iter().all(|rule| {
                    let Target::Command(command) = &rule.target else {
                        return true;
                    };
                    if catalog
                        .iter()
                        .any(|name| name.as_bytes().eq_ignore_ascii_case(command))
                    {
                        return true;
                    }
                    let base = command
                        .split(|byte| *byte == b'|')
                        .next()
                        .unwrap_or(command);
                    rule.allow
                        && command.contains(&b'|')
                        && catalog
                            .iter()
                            .any(|name| name.as_bytes().eq_ignore_ascii_case(base))
                        && !catalog.iter().any(|name| {
                            name.as_bytes().get(base.len()) == Some(&b'|')
                                && name.as_bytes()[..base.len()].eq_ignore_ascii_case(base)
                        })
                })
            })
    }

    fn counts(&self) -> (usize, usize) {
        // Count the serialized effective rules as well as the pattern state:
        // anything accepted here can be written and parsed again under limits.
        let metadata =
            1 + usize::from(self.nopass) + self.password_hashes.len() + self.selectors.len() * 2;
        std::iter::once(&self.root).chain(&self.selectors).fold(
            (metadata, 0),
            |(r, p), selector| {
                let patterns = selector.key_patterns.len() + selector.channel_patterns.len();
                let baseline = usize::from(
                    !selector
                        .command_rules
                        .first()
                        .is_some_and(|rule| matches!(rule.target, Target::All)),
                );
                (
                    r + baseline + selector.command_rules.len() + patterns + 1,
                    p + patterns,
                )
            },
        )
    }

    fn apply_rules(&mut self, rules: &[&[u8]]) -> Result<(), &'static str> {
        // SETUSER clones snapshots before mutation; proofs belong to the old
        // immutable policy and must never survive a rule change.
        self.namespace_cache = Arc::default();
        if rules.len() > MAX_RULES_PER_USER {
            return Err("rule count exceeds limit");
        }
        if rules
            .iter()
            .try_fold(0usize, |size, rule| size.checked_add(rule.len() + 1))
            .is_none_or(|size| size > MAX_LINE_BYTES)
        {
            return Err("ACL rules exceed size limit");
        }
        let mut selector: Option<Selector> = None;
        let mut password_rules = 0;
        let mut pattern_rules = 0;
        for raw in rules {
            // Parenthesized selectors can arrive as one argument or as several
            // argv words. Inside one argument whitespace separates rules.
            let grouped = selector.is_some() || raw.starts_with(b"(");
            if grouped {
                let mut token = *raw;
                let opened = token.starts_with(b"(");
                if opened {
                    if selector.is_some() {
                        return Err("nested selectors are not allowed");
                    }
                    selector = Some(Selector::default());
                    token = &token[1..];
                }
                let close = token.ends_with(b")");
                if close {
                    token = &token[..token.len() - 1];
                }
                // A complete selector supplied as one argv item contains a
                // configuration-style rule list. Separately supplied argv
                // items retain binary/whitespace pattern bytes verbatim.
                let words = if opened && close {
                    split_acl_line(token)?
                } else if token.is_empty() {
                    Vec::new()
                } else {
                    vec![token.to_vec()]
                };
                for rule in words {
                    pattern_rules += usize::from(is_pattern_rule(&rule));
                    selector.as_mut().ok_or("invalid selector")?.apply(&rule)?;
                }
                if close {
                    self.selectors
                        .push(selector.take().ok_or("invalid selector")?);
                }
            } else {
                let token = *raw;
                match token {
                    b"on" => self.enabled = true,
                    b"off" => self.enabled = false,
                    b"nopass" => {
                        self.nopass = true;
                        self.password_hashes.clear();
                    }
                    b"resetpass" => {
                        self.nopass = false;
                        self.password_hashes.clear();
                    }
                    b"clearselectors" => self.selectors.clear(),
                    b"reset" => {
                        let identity = self.identity.clone();
                        *self = User::empty(&self.name);
                        self.identity = identity;
                    }
                    _ if matches!(token.first(), Some(b'>' | b'<' | b'#' | b'!')) => {
                        password_rules += 1;
                        if password_rules > MAX_PASSWORDS {
                            return Err("password count exceeds limit");
                        }
                        let hash: [u8; 32] = if matches!(token[0], b'>' | b'<') {
                            if token.len() - 1 > MAX_PASSWORD_BYTES {
                                return Err("invalid password length");
                            }
                            Sha256::digest(&token[1..]).into()
                        } else {
                            decode_hash(&token[1..]).ok_or("invalid SHA256 password encoding")?
                        };
                        if matches!(token[0], b'>' | b'#') {
                            self.nopass = false;
                            if !self.password_hashes.contains(&hash) {
                                self.password_hashes.push(hash);
                            }
                        } else {
                            let Some(index) =
                                self.password_hashes.iter().position(|value| value == &hash)
                            else {
                                return Err("password to remove does not exist");
                            };
                            self.password_hashes.remove(index);
                        }
                    }
                    _ => {
                        pattern_rules += usize::from(is_pattern_rule(token));
                        self.root.apply(token)?;
                    }
                }
            }
            let (count, patterns) = self.counts();
            if count > MAX_RULES_PER_USER || self.selectors.len() > MAX_RULES_PER_USER {
                return Err("rule count exceeds limit");
            }
            if patterns > MAX_PATTERNS_PER_USER || pattern_rules > MAX_PATTERNS_PER_USER {
                return Err("pattern count exceeds limit");
            }
            if self.password_hashes.len() > MAX_PASSWORDS {
                return Err("password count exceeds limit");
            }
        }
        if selector.is_some() {
            return Err("unclosed selector");
        }
        Ok(())
    }

    /// Compatibility entry point. For commands with mixed source/destination
    /// keys use authorize_access with engine-provided per-key access modes.
    pub fn authorize(
        &self,
        command: &[u8],
        categories: &[&str],
        keys: &[Bytes],
        global_keyspace: bool,
    ) -> Result<(), String> {
        self.authorize_logged(command, categories, keys, global_keyspace, None)
    }

    pub fn authorize_logged(
        &self,
        command: &[u8],
        categories: &[&str],
        keys: &[Bytes],
        global_keyspace: bool,
        audit: Option<&Audit<'_>>,
    ) -> Result<(), String> {
        let mode = if categories.contains(&"write") {
            if categories.contains(&"read") {
                KeyAccess::ReadWrite
            } else {
                KeyAccess::Write
            }
        } else {
            KeyAccess::Read
        };
        self.check(
            command,
            categories,
            keys.iter().map(|key| (key, mode)),
            global_keyspace,
            &[],
            false,
            false,
            audit,
        )
    }

    /// One selector must grant the command AND every key AND every channel.
    /// Combining separate selector grants across these dimensions is forbidden.
    #[expect(
        clippy::too_many_arguments,
        reason = "One selector decision must cover the command, every key, and every channel together."
    )]
    pub fn authorize_access(
        &self,
        command: &[u8],
        categories: &[&str],
        keys: &[(&Bytes, KeyAccess)],
        global_keyspace: bool,
        channels: &[Bytes],
        pattern_channels: bool,
        all_channels: bool,
    ) -> Result<(), String> {
        self.authorize_access_logged(
            command,
            categories,
            keys,
            global_keyspace,
            channels,
            pattern_channels,
            all_channels,
            None,
        )
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "Audit identity accompanies the same atomic selector decision without materializing resource copies."
    )]
    pub fn authorize_access_logged(
        &self,
        command: &[u8],
        categories: &[&str],
        keys: &[(&Bytes, KeyAccess)],
        global_keyspace: bool,
        channels: &[Bytes],
        pattern_channels: bool,
        all_channels: bool,
        audit: Option<&Audit<'_>>,
    ) -> Result<(), String> {
        self.check(
            command,
            categories,
            keys.iter().copied(),
            global_keyspace,
            channels,
            pattern_channels,
            all_channels,
            audit,
        )
    }

    /// Authorize every namespace and concrete key within one selector.
    /// Empty prefixes cover all keys. Namespace proofs are bounded, cached
    /// per immutable policy, and fail closed on their complexity limit.
    pub fn authorize_prefixes_and_keys_logged(
        &self,
        command: &[u8],
        categories: &[&str],
        prefixes: &[Bytes],
        prefix_access: KeyAccess,
        keys: &[(&Bytes, KeyAccess)],
        audit: Option<&Audit<'_>>,
    ) -> Result<(), String> {
        namespace::authorize(
            self,
            command,
            categories,
            prefixes,
            prefix_access,
            keys,
            audit,
        )
    }

    /// The root selector's only command rule is `+@all` and it grants
    /// read-write `*` keys and `*` channels (extra patterns are moot). `check`
    /// consults the root first and, for such a selector, returns Ok before any
    /// glob work or audit, whatever the command, categories, keys, global
    /// flag or channels. Derived from this immutable snapshot on each call;
    /// like `check`, it does not consider the enabled flag.
    pub(crate) fn unrestricted_root(&self) -> bool {
        self.root.unrestricted()
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "The bounded selector proof borrows independent command, key, channel, and audit dimensions."
    )]
    fn check<'a>(
        &self,
        command: &[u8],
        categories: &[&str],
        keys: impl Iterator<Item = (&'a Bytes, KeyAccess)> + Clone,
        global: bool,
        channels: &[Bytes],
        patterns: bool,
        all_channels: bool,
        audit: Option<&Audit<'_>>,
    ) -> Result<(), String> {
        let mut work = MAX_GLOB_WORK;
        let mut command_granted = false;
        let mut keys_granted = false;
        let mut denied_key: Option<&Bytes> = None;
        let mut denied_channel: Option<&Bytes> = None;
        for selector in std::iter::once(&self.root).chain(&self.selectors) {
            if !selector.command_allowed(command, categories) {
                continue;
            }
            command_granted = true;
            if global && !selector.universal_keys(KeyAccess::ReadWrite) {
                continue;
            }
            let mut allowed = true;
            for (key, mode) in keys.clone() {
                match selector.key_allowed(key, mode, &mut work) {
                    Ok(true) => {}
                    Ok(false) => {
                        denied_key = Some(key);
                        allowed = false;
                        break;
                    }
                    Err(()) => {
                        if let Some(audit) = audit {
                            audit.record(self, "key", key);
                        }
                        return Err("key pattern work limit exceeded".into());
                    }
                }
            }
            if !allowed {
                continue;
            }
            keys_granted = true;
            match selector.channels_allowed(channels, patterns, all_channels, &mut work) {
                Ok(true) => return Ok(()),
                Ok(false) => {
                    if audit.is_some() {
                        denied_channel = channels.iter().find(|channel| {
                            !selector
                                .channels_allowed(
                                    std::slice::from_ref(*channel),
                                    patterns,
                                    false,
                                    &mut work,
                                )
                                .unwrap_or(false)
                        });
                    }
                }
                Err(()) => {
                    if let Some(audit) = audit {
                        audit.record(
                            self,
                            "channel",
                            channels.first().map_or(b"*".as_slice(), Bytes::as_ref),
                        );
                    }
                    return Err("channel pattern work limit exceeded".into());
                }
            }
        }
        if let Some(audit) = audit {
            let (reason, object) = if !command_granted {
                ("command", command)
            } else if !keys_granted {
                ("key", denied_key.map_or(b"*".as_slice(), Bytes::as_ref))
            } else {
                (
                    "channel",
                    denied_channel.map_or(b"*".as_slice(), Bytes::as_ref),
                )
            };
            audit.record(self, reason, object);
        }
        Err(if !command_granted {
            "command is not permitted"
        } else if !keys_granted {
            "key access is not permitted"
        } else {
            "channel access is not permitted"
        }
        .into())
    }

    fn reply(&self) -> Resp {
        let mut flags = vec![bulk(if self.enabled {
            b"on".as_slice()
        } else {
            b"off"
        })];
        if self.nopass {
            flags.push(bulk(b"nopass"));
        }
        if self.root.universal_keys(KeyAccess::ReadWrite) {
            flags.push(bulk(b"allkeys"));
        }
        if self.root.all_channels() {
            flags.push(bulk(b"allchannels"));
        }
        if self
            .root
            .command_rules
            .last()
            .is_some_and(|rule| rule.allow && matches!(rule.target, Target::All))
        {
            flags.push(bulk(b"allcommands"));
        }
        Resp::Map(vec![
            (bulk(b"flags"), Resp::Set(flags)),
            (
                bulk(b"passwords"),
                array(
                    self.password_hashes
                        .iter()
                        .map(|hash| bulk(hex(hash)))
                        .collect(),
                ),
            ),
            (bulk(b"commands"), bulk(self.root.command_dump())),
            (bulk(b"keys"), bulk(self.root.key_dump())),
            (bulk(b"channels"), bulk(self.root.channel_dump())),
            (
                bulk(b"selectors"),
                array(self.selectors.iter().map(Selector::reply).collect()),
            ),
        ])
    }

    fn dump(&self) -> Vec<u8> {
        let mut out = b"user ".to_vec();
        out.extend(quote_acl(self.name.as_bytes()));
        append_word(&mut out, if self.enabled { b"on" } else { b"off" });
        if self.nopass {
            append_word(&mut out, b"nopass");
        }
        for hash in &self.password_hashes {
            let mut rule = vec![b'#'];
            rule.extend(hex(hash));
            append_word(&mut out, &rule);
        }
        self.root.dump_into(&mut out);
        for selector in &self.selectors {
            append_word(&mut out, b"(");
            selector.dump_into(&mut out);
            append_word(&mut out, b")");
        }
        out
    }
}

impl Selector {
    fn apply(&mut self, token: &[u8]) -> Result<(), &'static str> {
        match token {
            b"allkeys" => return self.apply(b"~*"),
            b"allchannels" => return self.apply(b"&*"),
            b"allcommands" => return self.apply(b"+@all"),
            b"nocommands" => return self.apply(b"-@all"),
            b"resetkeys" => self.key_patterns.clear(),
            b"resetchannels" => self.channel_patterns.clear(),
            _ if token.starts_with(b"&") => {
                let source = &token[1..];
                let glob = Glob::compile(source)?;
                if source == b"*" {
                    self.channel_patterns.clear();
                }
                if !self
                    .channel_patterns
                    .iter()
                    .any(|(pattern, _)| pattern.as_ref() == source)
                {
                    self.channel_patterns
                        .push((Bytes::copy_from_slice(source), glob));
                }
            }
            _ if token.starts_with(b"~") || token.starts_with(b"%") => {
                let (access, source) = if let Some(source) = token.strip_prefix(b"~") {
                    (KeyAccess::ReadWrite, source)
                } else if let Some(source) = token.strip_prefix(b"%R~") {
                    (KeyAccess::Read, source)
                } else if let Some(source) = token.strip_prefix(b"%W~") {
                    (KeyAccess::Write, source)
                } else if let Some(source) = token.strip_prefix(b"%RW~") {
                    (KeyAccess::ReadWrite, source)
                } else {
                    return Err("invalid key access rule");
                };
                let glob = Glob::compile(source)?;
                if source == b"*" && access == KeyAccess::ReadWrite {
                    self.key_patterns.clear();
                }
                if let Some(pattern) = self
                    .key_patterns
                    .iter_mut()
                    .find(|pattern| pattern.source.as_ref() == source)
                {
                    if pattern.access != access {
                        pattern.access = KeyAccess::ReadWrite;
                    }
                } else {
                    self.key_patterns.push(KeyPattern {
                        source: Bytes::copy_from_slice(source),
                        glob,
                        access,
                    });
                }
            }
            _ if matches!(token.first(), Some(b'+' | b'-')) => {
                let allow = token[0] == b'+';
                let name = &token[1..];
                let target = if let Some(category) = name.strip_prefix(b"@") {
                    if category.eq_ignore_ascii_case(b"all") {
                        Target::All
                    } else if known_category(category) {
                        Target::Category(
                            String::from_utf8(category.to_ascii_lowercase())
                                .expect("ASCII category"),
                        )
                    } else {
                        return Err("unknown command category");
                    }
                } else {
                    let base = name.split(|byte| *byte == b'|').next().unwrap_or(name);
                    if base.is_empty()
                        || base.len() > MAX_COMMAND_BYTES
                        || name.len() > MAX_COMMAND_BYTES + MAX_PASSWORD_BYTES + 1
                        || !base
                            .iter()
                            .all(|byte| byte.is_ascii_alphanumeric() || b"_-.:".contains(byte))
                    {
                        return Err("invalid command name");
                    }
                    Target::Command(name.to_ascii_lowercase())
                };
                if matches!(target, Target::All) {
                    self.command_rules.clear();
                }
                self.command_rules.push(CommandRule { allow, target });
            }
            _ => return Err("unknown ACL rule or user-state rule inside selector"),
        }
        Ok(())
    }

    /// Grants every command, key access and channel by itself. A later rule,
    /// even a positive one, disqualifies the selector so the proof stays exact.
    fn unrestricted(&self) -> bool {
        matches!(
            self.command_rules.as_slice(),
            [CommandRule {
                allow: true,
                target: Target::All,
            }]
        ) && self.universal_keys(KeyAccess::ReadWrite)
            && self.all_channels()
    }

    fn command_allowed(&self, command: &[u8], categories: &[&str]) -> bool {
        let base = command
            .split(|byte| *byte == b'|')
            .next()
            .unwrap_or(command);
        let mut allowed = false;
        for rule in &self.command_rules {
            let matched = match &rule.target {
                Target::All => true,
                Target::Command(name) => {
                    name.eq_ignore_ascii_case(command)
                        || (base.len() != command.len() && name.eq_ignore_ascii_case(base))
                }
                Target::Category(name) => categories
                    .iter()
                    .any(|category| name.eq_ignore_ascii_case(category.trim_start_matches('@'))),
            };
            if matched {
                allowed = rule.allow;
            }
        }
        allowed
    }

    fn universal_keys(&self, mode: KeyAccess) -> bool {
        let mut read = false;
        let mut write = false;
        for pattern in &self.key_patterns {
            if pattern.source.as_ref() == b"*" {
                read |= pattern.access != KeyAccess::Write;
                write |= pattern.access != KeyAccess::Read;
            }
        }
        match mode {
            KeyAccess::Read => read,
            KeyAccess::Write => write,
            KeyAccess::ReadWrite => read && write,
        }
    }

    fn key_allowed(&self, key: &Bytes, mode: KeyAccess, work: &mut usize) -> Result<bool, ()> {
        if self.universal_keys(mode) {
            return Ok(true);
        }
        let mut read = false;
        let mut write = false;
        for pattern in &self.key_patterns {
            if pattern.glob.matches(key, work)? {
                read |= pattern.access != KeyAccess::Write;
                write |= pattern.access != KeyAccess::Read;
            }
            if match mode {
                KeyAccess::Read => read,
                KeyAccess::Write => write,
                KeyAccess::ReadWrite => read && write,
            } {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn all_channels(&self) -> bool {
        self.channel_patterns
            .iter()
            .any(|(source, _)| source.as_ref() == b"*")
    }

    fn channels_allowed(
        &self,
        channels: &[Bytes],
        patterns: bool,
        all: bool,
        work: &mut usize,
    ) -> Result<bool, ()> {
        if self.all_channels() {
            return Ok(true);
        }
        if all {
            return Ok(false);
        }
        for channel in channels {
            let mut matched = false;
            for (source, glob) in &self.channel_patterns {
                if if patterns {
                    source == channel
                } else {
                    glob.matches(channel, work)?
                } {
                    matched = true;
                    break;
                }
            }
            if !matched {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn command_dump(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if !self
            .command_rules
            .first()
            .is_some_and(|rule| matches!(rule.target, Target::All))
        {
            out.extend(b"-@all");
        }
        for rule in &self.command_rules {
            let mut token = vec![if rule.allow { b'+' } else { b'-' }];
            match &rule.target {
                Target::All => token.extend(b"@all"),
                Target::Command(name) => token.extend(name),
                Target::Category(name) => {
                    token.push(b'@');
                    token.extend(name.as_bytes());
                }
            }
            append_word(&mut out, &quote_acl(&token));
        }
        out
    }

    fn key_dump(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for pattern in &self.key_patterns {
            let mut token = match pattern.access {
                KeyAccess::Read => b"%R~".to_vec(),
                KeyAccess::Write => b"%W~".to_vec(),
                KeyAccess::ReadWrite => b"~".to_vec(),
            };
            token.extend(&pattern.source);
            append_word(&mut out, &quote_acl(&token));
        }
        out
    }

    fn channel_dump(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (source, _) in &self.channel_patterns {
            let mut token = vec![b'&'];
            token.extend(source);
            append_word(&mut out, &quote_acl(&token));
        }
        out
    }

    fn dump_into(&self, out: &mut Vec<u8>) {
        for pattern in &self.key_patterns {
            let mut token = match pattern.access {
                KeyAccess::Read => b"%R~".to_vec(),
                KeyAccess::Write => b"%W~".to_vec(),
                KeyAccess::ReadWrite => b"~".to_vec(),
            };
            token.extend(&pattern.source);
            append_word(out, &quote_acl(&token));
        }
        append_word(out, b"resetchannels");
        for (source, _) in &self.channel_patterns {
            let mut token = vec![b'&'];
            token.extend(source);
            append_word(out, &quote_acl(&token));
        }
        append_word(out, &self.command_dump());
    }

    fn reply(&self) -> Resp {
        Resp::Map(vec![
            (bulk(b"commands"), bulk(self.command_dump())),
            (bulk(b"keys"), bulk(self.key_dump())),
            (bulk(b"channels"), bulk(self.channel_dump())),
        ])
    }
}

impl Denial {
    fn reply(&self, now: i64) -> Resp {
        Resp::Map(vec![
            (bulk(b"count"), Resp::Integer(self.count)),
            (bulk(b"reason"), bulk(self.reason.as_bytes())),
            (bulk(b"context"), bulk(self.context.as_bytes())),
            (bulk(b"object"), bulk(self.object.clone())),
            (bulk(b"username"), bulk(self.username.as_bytes())),
            (
                bulk(b"age-seconds"),
                Resp::Double(now.saturating_sub(self.updated) as f64 / 1000.0),
            ),
            (bulk(b"client-info"), bulk(self.client_info.as_bytes())),
            (bulk(b"entry-id"), Resp::Integer(self.id)),
            (bulk(b"timestamp-created"), Resp::Integer(self.created)),
            (bulk(b"timestamp-last-updated"), Resp::Integer(self.updated)),
        ])
    }
}

fn valid_username(name: &[u8]) -> Result<&str, &'static str> {
    if name.is_empty() || name.len() > MAX_USERNAME_BYTES || name.iter().any(u8::is_ascii_control) {
        return Err("invalid username");
    }
    std::str::from_utf8(name).map_err(|_| "username must be UTF-8")
}
fn bulk(value: impl AsRef<[u8]>) -> Resp {
    Resp::Bulk(Some(Bytes::copy_from_slice(value.as_ref())))
}
fn array(values: Vec<Resp>) -> Resp {
    Resp::Array(Some(values))
}
fn acl_error(reason: &str) -> Resp {
    Resp::Error(format!("ERR {reason}"))
}
fn parse_count(value: &[u8]) -> Option<usize> {
    std::str::from_utf8(value).ok()?.parse().ok()
}
fn is_pattern_rule(rule: &[u8]) -> bool {
    matches!(rule.first(), Some(b'~' | b'%' | b'&'))
}
fn hex(value: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = Vec::with_capacity(value.len() * 2);
    for byte in value {
        out.push(DIGITS[(byte >> 4) as usize]);
        out.push(DIGITS[(byte & 15) as usize]);
    }
    out
}
fn append_word(out: &mut Vec<u8>, word: &[u8]) {
    if !out.is_empty() {
        out.push(b' ');
    }
    out.extend(word);
}
fn unix_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}
fn truncate_utf8(value: &str, maximum: usize) -> &str {
    let mut end = value.len().min(maximum);
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    &value[..end]
}

/// ACL LIST/SAVE use Redis configuration-style escaping, including binary key
/// patterns and whitespace. The complete line tokenizer is its inverse.
fn quote_acl(value: &[u8]) -> Vec<u8> {
    if !value.is_empty()
        && value
            .iter()
            .all(|b| !b.is_ascii_whitespace() && !b.is_ascii_control() && !b"\"\\'".contains(b))
    {
        return value.to_vec();
    }
    let mut out = vec![b'"'];
    for byte in value {
        match byte {
            b'"' | b'\\' => {
                out.push(b'\\');
                out.push(*byte);
            }
            0..=31 | 127 => {
                out.extend(b"\\x");
                out.extend(hex(&[*byte]));
            }
            _ => out.push(*byte),
        }
    }
    out.push(b'"');
    out
}

fn split_acl_line(line: &[u8]) -> Result<Vec<Vec<u8>>, &'static str> {
    let mut tokens = Vec::new();
    let mut position = 0;
    while position < line.len() {
        while position < line.len() && line[position].is_ascii_whitespace() {
            position += 1;
        }
        if position == line.len() {
            break;
        }
        if tokens.is_empty() && line[position] == b'#' {
            break;
        }
        let mut token = Vec::new();
        let quote = if matches!(line[position], b'"' | b'\'') {
            let q = line[position];
            position += 1;
            Some(q)
        } else {
            None
        };
        let mut closed = quote.is_none();
        while position < line.len() {
            let byte = line[position];
            if Some(byte) == quote {
                position += 1;
                closed = true;
                if position < line.len() && !line[position].is_ascii_whitespace() {
                    return Err("invalid quoted ACL token");
                }
                break;
            }
            if quote.is_none() && byte.is_ascii_whitespace() {
                break;
            }
            position += 1;
            if quote.is_some() && byte == b'\\' {
                let escaped = *line.get(position).ok_or("trailing quoted escape")?;
                position += 1;
                match escaped {
                    b'n' => token.push(b'\n'),
                    b'r' => token.push(b'\r'),
                    b't' => token.push(b'\t'),
                    b'x' => {
                        let pair = line
                            .get(position..position + 2)
                            .ok_or("invalid hexadecimal escape")?;
                        let text =
                            std::str::from_utf8(pair).map_err(|_| "invalid hexadecimal escape")?;
                        token.push(
                            u8::from_str_radix(text, 16)
                                .map_err(|_| "invalid hexadecimal escape")?,
                        );
                        position += 2;
                    }
                    other => token.push(other),
                }
            } else {
                token.push(byte);
            }
        }
        if !closed {
            return Err("unclosed ACL quotation");
        }
        tokens.push(token);
    }
    Ok(tokens)
}

fn invalid(line: usize, reason: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("ACL line {line}: {reason}"),
    )
}

fn decode_hash(encoded: &[u8]) -> Option<[u8; 32]> {
    fn digit(byte: u8) -> Option<u8> {
        match byte {
            b'0'..=b'9' => Some(byte - b'0'),
            b'a'..=b'f' => Some(byte - b'a' + 10),
            b'A'..=b'F' => Some(byte - b'A' + 10),
            _ => None,
        }
    }
    if encoded.len() != 64 {
        return None;
    }
    let mut hash = [0; 32];
    for (value, pair) in hash.iter_mut().zip(encoded.as_chunks::<2>().0.iter()) {
        *value = (digit(pair[0])? << 4) | digit(pair[1])?;
    }
    Some(hash)
}

const CATEGORIES: &[&str] = &[
    "keyspace",
    "read",
    "write",
    "set",
    "sortedset",
    "list",
    "hash",
    "string",
    "bitmap",
    "hyperloglog",
    "geo",
    "stream",
    "pubsub",
    "admin",
    "fast",
    "slow",
    "blocking",
    "dangerous",
    "connection",
    "transaction",
    "scripting",
    "array",
    "json",
    "timeseries",
    "bloom",
    "cuckoo",
    "cms",
    "topk",
    "tdigest",
    "search",
    "vector",
    "queue",
];

fn known_category(name: &[u8]) -> bool {
    CATEGORIES
        .iter()
        .any(|category| category.as_bytes().eq_ignore_ascii_case(name))
}

impl Glob {
    pub(crate) fn compile(pattern: &[u8]) -> Result<Self, &'static str> {
        if pattern.len() > MAX_PATTERN_BYTES {
            return Err("key pattern exceeds size limit");
        }
        let mut atoms = Vec::new();
        let mut position = 0;
        let mut literal = true;
        while position < pattern.len() {
            let byte = pattern[position];
            position += 1;
            let atom = match byte {
                b'*' => {
                    literal = false;
                    Atom::Many
                }
                b'?' => {
                    literal = false;
                    Atom::Any
                }
                b'\\' => {
                    let escaped = *pattern.get(position).ok_or("trailing key pattern escape")?;
                    position += 1;
                    Atom::Literal(escaped)
                }
                b'[' => {
                    literal = false;
                    let negated = pattern.get(position) == Some(&b'^');
                    position += usize::from(negated);
                    let mut units = Vec::new();
                    let mut closed = false;
                    while let Some(&value) = pattern.get(position) {
                        position += 1;
                        if value == b']' {
                            closed = true;
                            break;
                        }
                        let escaped = value == b'\\';
                        let value = if escaped {
                            let value =
                                *pattern.get(position).ok_or("trailing key pattern escape")?;
                            position += 1;
                            value
                        } else {
                            value
                        };
                        units.push((value, escaped));
                    }
                    if !closed || units.is_empty() {
                        return Err("invalid key pattern character class");
                    }
                    let mut ranges = Vec::new();
                    let mut index = 0;
                    while index < units.len() {
                        if index + 2 < units.len() && units[index + 1] == (b'-', false) {
                            let a = units[index].0;
                            let b = units[index + 2].0;
                            ranges.push((a.min(b), a.max(b)));
                            index += 3;
                        } else {
                            ranges.push((units[index].0, units[index].0));
                            index += 1;
                        }
                    }
                    Atom::Class { negated, ranges }
                }
                _ => Atom::Literal(byte),
            };
            atoms.push(atom);
        }
        let work_width = atoms
            .iter()
            .map(|atom| match atom {
                Atom::Class { ranges, .. } => ranges.len() + 1,
                _ => 1,
            })
            .sum::<usize>()
            + 1;
        Ok(Self {
            atoms,
            literal,
            work_width,
        })
    }

    // Thompson-style state propagation uses fixed bounded storage and O(P*K)
    // work, not recursive star backtracking. Charge class ranges as well as
    // atoms so large classes cannot evade the shared authorization budget.
    pub(crate) fn matches(&self, key: &[u8], work: &mut usize) -> Result<bool, ()> {
        charge(work, 1)?;
        if self.literal {
            if self.atoms.len() != key.len() {
                return Ok(false);
            }
            charge(work, key.len())?;
            return Ok(self
                .atoms
                .iter()
                .zip(key)
                .all(|(atom, byte)| matches!(atom, Atom::Literal(value) if value == byte)));
        }
        let cost = key
            .len()
            .checked_add(1)
            .and_then(|length| length.checked_mul(self.work_width))
            .ok_or(())?;
        charge(work, cost)?;
        let mut current = [false; MAX_PATTERN_BYTES + 1];
        let mut next = [false; MAX_PATTERN_BYTES + 1];
        current[0] = true;
        self.epsilon(&mut current);
        for byte in key {
            next[..=self.atoms.len()].fill(false);
            for (index, atom) in self.atoms.iter().enumerate() {
                if !current[index] {
                    continue;
                }
                match atom {
                    Atom::Many => next[index] = true,
                    Atom::Any => next[index + 1] = true,
                    Atom::Literal(value) if value == byte => next[index + 1] = true,
                    Atom::Class { negated, ranges } => {
                        let inside = ranges
                            .iter()
                            .any(|(start, end)| start <= byte && byte <= end);
                        if inside != *negated {
                            next[index + 1] = true;
                        }
                    }
                    _ => {}
                }
            }
            self.epsilon(&mut next);
            std::mem::swap(&mut current, &mut next);
        }
        Ok(current[self.atoms.len()])
    }

    fn epsilon(&self, states: &mut [bool; MAX_PATTERN_BYTES + 1]) {
        for (index, atom) in self.atoms.iter().enumerate() {
            if states[index] && matches!(atom, Atom::Many) {
                states[index + 1] = true;
            }
        }
    }
}

fn charge(work: &mut usize, amount: usize) -> Result<(), ()> {
    *work = work.checked_sub(amount).ok_or(())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn acl(contents: &[u8]) -> Acl {
        Acl::parse(contents).unwrap()
    }
    fn user(contents: &[u8]) -> Arc<User> {
        acl(contents).authenticate(b"alice", b"secret").unwrap()
    }
    fn key(value: &[u8]) -> Bytes {
        Bytes::copy_from_slice(value)
    }
    fn glob(pattern: &[u8], key: &[u8]) -> bool {
        let mut work = MAX_GLOB_WORK;
        Glob::compile(pattern)
            .unwrap()
            .matches(key, &mut work)
            .unwrap()
    }

    #[test]
    fn engine_channel_authorization_is_separate_and_pattern_safe() {
        let restricted = user(b"user alice on >secret +@pubsub &allowed:*");
        let check = |user: &User, values: &[&[u8]]| {
            let args = values.iter().map(|value| key(value)).collect::<Vec<_>>();
            let mut command = crate::engine::BoundCommand::bind(&args);
            command.check().and_then(|()| command.authorize(user, None))
        };
        assert!(check(&restricted, &[b"PUBLISH", b"allowed:x", b"body"]).is_ok());
        assert!(check(&restricted, &[b"PUBLISH", b"hidden:x", b"body"]).is_err());
        assert!(check(&restricted, &[b"SUBSCRIBE", b"allowed:x"]).is_ok());
        assert!(check(&restricted, &[b"PSUBSCRIBE", b"allowed:*"]).is_ok());
        assert!(check(&restricted, &[b"PSUBSCRIBE", b"*"]).is_err());
        assert!(check(&restricted, &[b"PUBSUB", b"NUMSUB", b"allowed:x"]).is_ok());
        assert!(check(&restricted, &[b"PUBSUB", b"NUMSUB", b"hidden:x"]).is_err());
        assert!(check(&restricted, &[b"PUBSUB", b"CHANNELS"]).is_err());
        let unrestricted = user(b"user alice on >secret +@pubsub allchannels");
        assert!(check(&unrestricted, &[b"PUBSUB", b"CHANNELS"]).is_ok());
        let keys_only = user(b"user alice on >secret allcommands allkeys");
        assert!(check(&keys_only, &[b"PUBLISH", b"allowed:x", b"body"]).is_err());
        let reset = user(b"user alice on >secret +@pubsub &* resetchannels &allowed:*");
        assert!(check(&reset, &[b"PUBLISH", b"hidden:x", b"body"]).is_err());
    }

    type Decision = (Result<(), Resp>, Vec<(String, Bytes)>);

    /// Engine authorization through a live audit: the decision and every
    /// denial it recorded as (reason, object).
    fn audited(user: &User, values: &[&[u8]], admission: bool) -> Decision {
        let log = Acl::new();
        let clients =
            crate::client_registry::Registry::new(Arc::new(crate::pubsub::Hub::default()));
        let audit = Audit {
            acl: &log,
            clients: &clients,
            client_id: 1,
            context: "multi",
        };
        let args: Vec<_> = values.iter().map(|value| key(value)).collect();
        let mut command = crate::engine::BoundCommand::bind(&args);
        // MULTI admission authorizes after identity and arity only; every
        // other caller fully validates first.
        let result = if admission {
            command.authorize(user, Some(&audit))
        } else {
            command
                .check()
                .and_then(|()| command.authorize(user, Some(&audit)))
        };
        let denials = log
            .log
            .lock()
            .unwrap()
            .entries
            .iter()
            .map(|entry| (entry.reason.clone(), entry.object.clone()))
            .collect();
        (result, denials)
    }
    fn granted() -> Decision {
        (Ok(()), Vec::new())
    }
    fn refused(reason: &str, object: &[u8]) -> Decision {
        (
            Err(Resp::Error(
                "NOPERM this user has no permissions to run this command or access its keys".into(),
            )),
            vec![(reason.into(), key(object))],
        )
    }

    #[test]
    fn validation_precedes_acl_decisions_and_admission_defers_counted_keys() {
        let everything = Acl::new().user("default").unwrap();
        let disabled = acl(b"user alice off >secret allcommands allkeys allchannels")
            .user("alice")
            .unwrap();
        let command_limited = user(b"user alice on >secret +get ~tenant:*");
        let keys_limited = user(b"user alice on >secret allcommands allchannels ~tenant:*");
        let policies = [&everything, &disabled, &command_limited, &keys_limited];
        let malformed: &[&[&[u8]]] = &[
            &[],
            &[b"NOPE"],
            &[b"GET"],
            &[b"CONFIG|GET", b"maxmemory"],
            &[b"CONFIG", b"NOPE"],
            &[b"CONFIG", b"GET"],
        ];
        for policy in policies {
            for values in malformed {
                let args: Vec<_> = values.iter().map(|value| key(value)).collect();
                let admission = crate::engine::BoundCommand::bind(&args).admit();
                assert!(admission.is_err());
                assert_eq!(audited(policy, values, true), (admission, Vec::new()));
                let full = crate::engine::BoundCommand::bind(&args).check();
                assert_eq!(audited(policy, values, false), (full, Vec::new()));
            }
        }
        // Invalid key counts fail full validation for everyone, yet MULTI
        // admission still decides command and channel access without keys.
        let counted: &[&[&[u8]]] = &[
            &[b"CMS.MERGE", b"out", b"999999999999999999999", b"a"],
            &[b"XREAD", b"STREAMS", b"a", b"b", b"0"],
        ];
        for values in counted {
            let args: Vec<_> = values.iter().map(|value| key(value)).collect();
            assert!(crate::engine::BoundCommand::bind(&args).admit().is_ok());
            for policy in policies {
                let full = crate::engine::BoundCommand::bind(&args).check();
                assert!(full.is_err());
                assert_eq!(audited(policy, values, false), (full, Vec::new()));
            }
            for policy in [&everything, &disabled, &keys_limited] {
                assert_eq!(audited(policy, values, true), granted());
            }
        }
        assert_eq!(
            audited(&command_limited, counted[0], true),
            refused("command", b"cms.merge")
        );
        assert_eq!(
            audited(&command_limited, counted[1], true),
            refused("command", b"xread")
        );
        let valid: &[&[&[u8]]] = &[
            &[b"GET", b"k"],
            &[b"SET", b"k", b"v"],
            &[b"COPY", b"a", b"b"],
            &[b"SCAN", b"0"],
            &[b"PUBSUB", b"CHANNELS"],
            &[b"PUBLISH", b"c", b"body"],
            &[b"CONFIG", b"GET", b"maxmemory"],
        ];
        for policy in [&everything, &disabled] {
            for values in valid {
                for admission in [false, true] {
                    assert_eq!(audited(policy, values, admission), granted());
                }
            }
        }
    }

    #[test]
    fn later_rules_and_split_directional_selectors_keep_denying() {
        let later_command = user(b"user alice on >secret +@all ~* &* -get");
        let later_category = user(b"user alice on >secret +@all ~* &* -@write");
        let earlier_negative = user(b"user alice on >secret -get +@all ~* &*");
        let later_positive = user(b"user alice on >secret +@all +get ~* &*");
        for admission in [false, true] {
            let check = |user: &User, values: &[&[u8]]| audited(user, values, admission);
            assert_eq!(
                check(&later_command, &[b"GET", b"k"]),
                refused("command", b"get")
            );
            assert_eq!(check(&later_command, &[b"SET", b"k", b"v"]), granted());
            assert_eq!(
                check(&later_category, &[b"SET", b"k", b"v"]),
                refused("command", b"set")
            );
            assert_eq!(check(&later_category, &[b"GET", b"k"]), granted());
            // +@all replaces every earlier rule.
            assert_eq!(check(&earlier_negative, &[b"GET", b"k"]), granted());
            assert_eq!(check(&later_positive, &[b"GET", b"k"]), granted());
        }
        // Read-only root plus write-only selector: each key is granted somewhere,
        // but no single selector grants both directions of COPY.
        let split = user(b"user alice on >secret +@all %R~* &* (+@all %W~* &*)");
        let check = |user: &User, values: &[&[u8]]| audited(user, values, false);
        assert_eq!(check(&split, &[b"GET", b"k"]), granted());
        assert_eq!(check(&split, &[b"DEL", b"k"]), granted());
        assert_eq!(check(&split, &[b"SET", b"k", b"v"]), granted());
        assert_eq!(check(&split, &[b"COPY", b"a", b"b"]), refused("key", b"a"));
        assert_eq!(check(&split, &[b"SCAN", b"0"]), refused("key", b"*"));
        let read_only = user(b"user alice on >secret +@all %R~* &*");
        assert_eq!(check(&read_only, &[b"GET", b"k"]), granted());
        assert_eq!(check(&read_only, &[b"DEL", b"k"]), refused("key", b"k"));
        // Both directions on `*` within one selector merge into read-write.
        let merged = user(b"user alice on >secret +@all %R~* %W~* &*");
        assert_eq!(check(&merged, &[b"COPY", b"a", b"b"]), granted());
        assert_eq!(check(&merged, &[b"SCAN", b"0"]), granted());
        let reset = user(b"user alice on >secret +@all ~* &* resetkeys ~tenant:*");
        assert_eq!(check(&reset, &[b"GET", b"other"]), refused("key", b"other"));
        assert_eq!(check(&reset, &[b"SCAN", b"0"]), refused("key", b"*"));
    }

    #[test]
    fn channel_restrictions_apply_despite_universal_commands_and_keys() {
        let limited = user(b"user alice on >secret +@all ~* &allowed:*");
        let reset = user(b"user alice on >secret +@all ~* &* resetchannels");
        let restored = user(b"user alice on >secret +@all ~* &* resetchannels &*");
        for admission in [false, true] {
            let check = |user: &User, values: &[&[u8]]| audited(user, values, admission);
            assert_eq!(
                check(&limited, &[b"PUBLISH", b"allowed:x", b"body"]),
                granted()
            );
            assert_eq!(
                check(&limited, &[b"PUBLISH", b"hidden:x", b"body"]),
                refused("channel", b"hidden:x")
            );
            assert_eq!(
                check(&limited, &[b"SUBSCRIBE", b"allowed:a", b"hidden:b"]),
                refused("channel", b"hidden:b")
            );
            assert_eq!(
                check(&limited, &[b"PUBSUB", b"CHANNELS"]),
                refused("channel", b"*")
            );
            assert_eq!(check(&limited, &[b"GET", b"k"]), granted());
            assert_eq!(
                check(&reset, &[b"PUBLISH", b"c", b"body"]),
                refused("channel", b"c")
            );
            assert_eq!(check(&restored, &[b"PUBSUB", b"CHANNELS"]), granted());
        }
    }

    #[test]
    fn root_work_limit_is_not_bypassed_by_a_later_unrestricted_selector() {
        let user = user(b"user alice on >secret +get ~tenant:* (+@all ~* &*)");
        let huge = vec![b'a'; MAX_GLOB_WORK];
        for admission in [false, true] {
            // The root grants GET, so its key match runs first and exhausts
            // the shared budget before the unrestricted selector is reached.
            assert_eq!(
                audited(&user, &[b"GET", huge.as_slice()], admission),
                refused("key", &huge[..MAX_PATTERN_BYTES])
            );
            assert_eq!(audited(&user, &[b"GET", b"other"], admission), granted());
            assert_eq!(
                audited(&user, &[b"SET", huge.as_slice(), b"v"], admission),
                granted()
            );
        }
    }

    #[test]
    fn defaults_deny_and_no_implicit_user() {
        assert!(acl(b"").default_user().is_none());
        let policy = acl(b"user alice on >secret\nuser default on >password");
        assert!(policy.default_user().is_none());
        assert!(policy.authenticate(b"missing", b"secret").is_none());
        assert!(policy.authenticate(b"alice", b"wrong").is_none());
        let user = policy.authenticate(b"alice", b"secret").unwrap();
        assert_eq!(user.name(), "alice");
        assert!(user.authorize(b"GET", &["read"], &[], false).is_err());
        let user = super::tests::user(b"user alice on >secret +get");
        assert!(
            user.authorize(b"GET", &["read"], &[key(b"a")], false)
                .is_err()
        );
    }

    #[test]
    fn disabled_users_and_passwordless_default() {
        let policy = acl(b"user default off nopass allcommands allkeys\nuser alice off >secret allcommands allkeys");
        assert!(policy.default_user().is_none());
        assert!(policy.authenticate(b"default", b"").is_none());
        assert!(policy.authenticate(b"alice", b"secret").is_none());
        let disabled = policy.user("alice").unwrap();
        // off prevents authentication but does not revoke existing sessions.
        assert!(disabled.authorize(b"GET", &["read"], &[], false).is_ok());
        let policy = acl(b"user default on nopass");
        assert!(policy.default_user().is_some());
        assert!(policy.authenticate(b"default", b"anything").is_some());
        assert!(acl(b"user default on").default_user().is_none());
    }

    #[test]
    fn hashes_multiple_credentials_and_ordered_nopass() {
        // SHA256("hello"), including uppercase hex acceptance.
        let policy = acl(b"user alice on >secret #2CF24DBA5FB0A30E26E83B2AC5B9E29E1B161E5C1FA7425E73043362938B9824");
        assert!(policy.authenticate(b"alice", b"secret").is_some());
        assert!(policy.authenticate(b"alice", b"hello").is_some());
        assert!(policy.authenticate(b"alice", b"hellp").is_none());
        assert!(policy.authenticate(b"alice", b"secreu").is_none());
        assert_eq!(policy.user("alice").unwrap().password_hashes.len(), 2);
        let policy = acl(b"user default on >secret nopass");
        assert!(policy.default_user().is_some());
        assert!(policy.user("default").unwrap().password_hashes.is_empty());
        let policy = acl(b"user default on nopass >secret");
        assert!(policy.default_user().is_none());
        assert!(policy.authenticate(b"default", b"secret").is_some());
        assert!(policy.authenticate(b"default", b"other").is_none());
    }

    #[test]
    fn command_and_category_rules_share_last_match_precedence() {
        let user =
            user(b"user alice on >secret allcommands -@write +set -get +@READ -@admin allkeys");
        assert!(
            user.authorize(b"SET", &["write"], &[key(b"x")], false)
                .is_ok()
        );
        assert!(
            user.authorize(b"DEL", &["write"], &[key(b"x")], false)
                .is_err()
        );
        assert!(
            user.authorize(b"GET", &["read"], &[key(b"x")], false)
                .is_ok()
        );
        assert!(
            user.authorize(b"CONFIG", &["admin", "read"], &[], false)
                .is_err()
        );
        for category in ["read", "write", "admin", "connection", "scripting", "queue"] {
            let text = format!("user alice on >secret +@{category} allkeys");
            let granted = super::tests::user(text.as_bytes());
            assert!(
                granted
                    .authorize(b"EXAMPLE", &[category], &[], false)
                    .is_ok()
            );
            assert!(granted.authorize(b"EXAMPLE", &[], &[], false).is_err());
        }
        let user = super::tests::user(b"user alice on >secret +@all -@all +ping");
        assert!(user.authorize(b"PING", &["connection"], &[], false).is_ok());
        assert!(user.authorize(b"GET", &["read"], &[], false).is_err());
        let user = super::tests::user(b"user alice on >secret +get nocommands");
        assert!(user.authorize(b"GET", &["read"], &[], false).is_err());
    }

    #[test]
    fn every_key_is_checked_and_global_commands_need_universal_grant() {
        let user = user(b"user alice on >secret allcommands ~tenant:*");
        assert!(
            user.authorize(
                b"MGET",
                &["read"],
                &[key(b"tenant:a"), key(b"tenant:b")],
                false
            )
            .is_ok()
        );
        assert!(
            user.authorize(
                b"MGET",
                &["read"],
                &[key(b"tenant:a"), key(b"private")],
                false
            )
            .is_err()
        );
        for command in [b"SCAN".as_slice(), b"KEYS", b"DBSIZE", b"FLUSHDB"] {
            assert!(user.authorize(command, &[], &[], true).is_err());
        }
        for rules in [b"allkeys".as_slice(), b"~*"] {
            let mut text = b"user alice on >secret allcommands ".to_vec();
            text.extend_from_slice(rules);
            assert!(
                super::tests::user(&text)
                    .authorize(b"SCAN", &[], &[], true)
                    .is_ok()
            );
        }
        let escaped = super::tests::user(b"user alice on >secret allcommands ~\\*");
        assert!(
            escaped
                .authorize(b"GET", &["read"], &[key(b"*")], false)
                .is_ok()
        );
        assert!(escaped.authorize(b"SCAN", &[], &[], true).is_err());
        let reset =
            super::tests::user(b"user alice on >secret allcommands allkeys ~* resetkeys ~tenant:*");
        assert!(
            reset
                .authorize(b"GET", &["read"], &[key(b"private")], false)
                .is_err()
        );
        assert!(reset.authorize(b"SCAN", &[], &[], true).is_err());
    }

    #[test]
    fn glob_is_binary_safe_and_handles_classes_ranges_and_escapes() {
        assert!(glob(b"*", b"\0\xff\n"));
        assert!(glob(b"a?c", b"a\0c"));
        assert!(!glob(b"a?c", b"ac"));
        assert!(glob(b"[a-z][^0-9]", b"m\xff"));
        assert!(!glob(b"[a-z][^0-9]", b"m4"));
        assert!(glob(b"[z-a]", b"m"));
        assert!(glob(b"[\x80-\xff]", b"\xfe"));
        assert!(glob(b"[a\\-z]", b"-"));
        assert!(!glob(b"[a\\-z]", b"m"));
        assert!(glob(b"[\\]]", b"]"));
        assert!(glob(b"\\*\\?\\[\\]\\\\", b"*?[]\\"));
        assert!(glob(b"a**b*c", b"axybzzc"));
        assert!(!glob(b"a*b", b"a\0c"));
        assert!(glob(b"", b""));
        assert!(!glob(b"", b"a"));
        let user = user(b"user alice on >secret +get ~binary:\xff?");
        assert!(
            user.authorize(b"GET", &["read"], &[key(b"binary:\xff\0")], false)
                .is_ok()
        );
    }

    #[test]
    fn malformed_input_is_rejected_without_secret_material() {
        for bad in [
            b"user alice".as_slice(),
            b"user alice maybe >secret",
            b"user alice on +",
            b"user alice on +@typo",
            b"user alice on #secret-hash",
            b"user alice on ~[",
            b"user alice on ~[]",
            b"user alice on ~trailing\\",
            b"user alice on secret-marker",
            b"user alice on\nuser alice off",
            b"user \xff on nopass",
        ] {
            let error = match Acl::parse(bad) {
                Ok(_) => panic!("accepted malformed ACL"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            let text = error.to_string();
            assert!(text.contains("line"));
            assert!(!text.contains("secret"));
            assert!(!text.contains("alice"));
        }
        let error = match Acl::parse(b"# comment\n\nuser bob on #sensitive-hash") {
            Ok(_) => panic!("accepted malformed hash"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("line 3"));
        assert!(!error.to_string().contains("sensitive-hash"));
    }

    #[test]
    fn all_resource_limits_fail_closed() {
        assert!(Acl::parse(&vec![b' '; MAX_FILE_BYTES + 1]).is_err());
        let line = format!("#{}", "a".repeat(MAX_LINE_BYTES));
        assert!(Acl::parse(line.as_bytes()).is_err());
        let users = (0..=MAX_USERS)
            .map(|i| format!("user u{i} on nopass\n"))
            .collect::<String>();
        assert!(Acl::parse(users.as_bytes()).is_err());
        let rules = format!("user alice on {}", "nopass ".repeat(MAX_RULES_PER_USER + 1));
        assert!(Acl::parse(rules.as_bytes()).is_err());
        let patterns = format!("user alice on {}", "~a ".repeat(MAX_PATTERNS_PER_USER + 1));
        assert!(Acl::parse(patterns.as_bytes()).is_err());
        let patterns = (0..=(MAX_TOTAL_PATTERNS / MAX_PATTERNS_PER_USER))
            .map(|i| format!("user u{i} on {}\n", "~a ".repeat(MAX_PATTERNS_PER_USER)))
            .collect::<String>();
        assert!(Acl::parse(patterns.as_bytes()).is_err());
        let rules = (0..=(MAX_TOTAL_RULES / MAX_RULES_PER_USER))
            .map(|i| format!("user u{i} on {}\n", "nopass ".repeat(MAX_RULES_PER_USER)))
            .collect::<String>();
        assert!(Acl::parse(rules.as_bytes()).is_err());
        let passwords = format!("user alice on {}", ">secret ".repeat(MAX_PASSWORDS + 1));
        assert!(Acl::parse(passwords.as_bytes()).is_err());
        let pattern = format!("user alice on ~{}", "a".repeat(MAX_PATTERN_BYTES + 1));
        assert!(Acl::parse(pattern.as_bytes()).is_err());
        let password = format!("user alice on >{}", "a".repeat(MAX_PASSWORD_BYTES + 1));
        assert!(Acl::parse(password.as_bytes()).is_err());
        let name = format!("user {} on", "a".repeat(MAX_USERNAME_BYTES + 1));
        assert!(Acl::parse(name.as_bytes()).is_err());
        let command = format!("user alice on +{}", "a".repeat(MAX_COMMAND_BYTES + 1));
        assert!(Acl::parse(command.as_bytes()).is_err());
    }

    #[test]
    fn pathological_globs_have_a_shared_finite_budget() {
        let pattern = Glob::compile(b"*a*a*a*a*a*a*a*a*a*a*b").unwrap();
        let mut budget = 100;
        assert!(pattern.matches(&vec![b'a'; 1024], &mut budget).is_err());
        let user = user(b"user alice on >secret +get ~*a*a*a*a*a*a*a*a*a*a*b");
        assert!(
            user.authorize(
                b"GET",
                &["read"],
                &[Bytes::from(vec![b'a'; MAX_GLOB_WORK])],
                false
            )
            .is_err()
        );
        let pattern = Glob::compile(b"?*").unwrap();
        let mut budget = 12;
        assert!(pattern.matches(b"ab", &mut budget).unwrap());
        assert!(pattern.matches(b"ab", &mut budget).is_err());
        let literal = Glob::compile(b"a").unwrap();
        let mut budget = 1;
        assert!(!literal.matches(b"different length", &mut budget).unwrap());
        assert!(literal.matches(b"different length", &mut budget).is_err());
    }

    #[test]
    fn load_reads_a_real_file_and_propagates_io_errors() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ffkv-acl-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = std::fs::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&path).unwrap();
        std::io::Write::write_all(&mut file, b"# comment\r\nuser default on nopass +ping\r\n")
            .unwrap();
        drop(file);
        let loaded = Acl::load(&path);
        std::fs::remove_file(&path).unwrap();
        let user = loaded.unwrap().default_user().unwrap();
        assert!(user.authorize(b"PING", &["connection"], &[], false).is_ok());
        assert_eq!(
            Acl::load(&path).err().unwrap().kind(),
            io::ErrorKind::NotFound
        );
    }
}
