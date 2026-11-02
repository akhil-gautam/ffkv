//! Command, directional key and channel permissions must be granted together by
//! one ACL selector. Protocol metadata supplies canonical core classifications.
use super::{CommandSpec, Engine, Resp, catalog, effective_spec, registry};
use crate::auth::{KeyAccess, User};
use bytes::Bytes;

fn denied() -> Resp {
    Resp::Error("NOPERM this user has no permissions to run this command or access its keys".into())
}

pub(super) struct Categories {
    values: [&'static str; 16],
    len: usize,
}
impl Categories {
    fn new() -> Self {
        Self {
            values: [""; 16],
            len: 0,
        }
    }
    fn push(&mut self, name: &'static str) {
        if !self.values[..self.len].contains(&name) {
            self.values[self.len] = name;
            self.len += 1;
        }
    }
    pub(super) fn as_slice(&self) -> &[&'static str] {
        &self.values[..self.len]
    }
}
pub(super) fn categories(spec: &CommandSpec) -> Categories {
    let mut result = Categories::new();
    let canonical = catalog::categories(spec.name);
    if !canonical.is_empty() {
        for category in canonical {
            result.push(category);
        }
        return result;
    }
    if spec.name.starts_with("ffkv.") {
        result.push("admin");
        result.push("dangerous");
        return result;
    }
    if spec.flags.contains(&"readonly") {
        result.push("read");
    }
    if spec.flags.contains(&"write") {
        result.push("write");
    }
    if spec.flags.contains(&"admin") {
        result.push("admin");
    }
    if spec.flags.contains(&"blocking") {
        result.push("blocking");
    }
    result.push(if spec.flags.contains(&"fast") {
        "fast"
    } else {
        "slow"
    });
    if let Some(category) = module_category(spec.name) {
        result.push(category);
    }
    result
}
fn access(spec: &CommandSpec, args: &[Bytes], position: usize) -> KeyAccess {
    if catalog::categories(spec.name).is_empty() {
        return if spec.flags.contains(&"readonly") {
            KeyAccess::Read
        } else {
            KeyAccess::ReadWrite
        };
    }
    let flags = catalog::key_flags(spec.name, args, position);
    let read = flags.contains(&"RO") || flags.contains(&"access");
    let write = flags
        .iter()
        .any(|flag| matches!(*flag, "OW" | "RM" | "update" | "insert" | "delete"));
    match (read, write) {
        (true, false) => KeyAccess::Read,
        (false, true) => KeyAccess::Write,
        _ => KeyAccess::ReadWrite,
    }
}

impl Engine {
    pub fn authorize(args: &[Bytes], user: &User) -> Result<(), Resp> {
        Self::authorize_logged(args, user, None)
    }
    pub fn authorize_logged(
        args: &[Bytes],
        user: &User,
        audit: Option<&crate::auth::Audit<'_>>,
    ) -> Result<(), Resp> {
        Self::validate(args)?;
        Self::authorize_admission_logged(args, user, audit)
    }

    /// Queue admission still checks command/channel ACLs when invalid key counts
    /// prevent extracting keys. Full validation is required before execution.
    pub fn authorize_admission_logged(
        args: &[Bytes],
        user: &User,
        audit: Option<&crate::auth::Audit<'_>>,
    ) -> Result<(), Resp> {
        Self::validate_admission(args)?;
        let spec = effective_spec(args).expect("validated command metadata");
        let categories = categories(spec);
        let global = spec.name.starts_with("ffkv.")
            || matches!(
                spec.name,
                "scan" | "keys" | "dbsize" | "flushdb" | "flushall" | "randomkey"
            )
            || (spec.name.starts_with("ts.")
                && Self::requires_all_keys(spec.name)
                && spec.name != "ts.querylabels");
        let (channels, pattern_channels, all_channels) = match spec.name {
            "publish" | "spublish" => (&args[1..2], false, false),
            "subscribe" | "ssubscribe" | "psubscribe" => {
                (&args[1..], spec.name == "psubscribe", false)
            }
            "pubsub|numsub" | "pubsub|shardnumsub" => (&args[2..], false, false),
            name if name.starts_with("pubsub|") => (&args[0..0], false, true),
            _ => (&args[0..0], false, false),
        };
        let check = |keys: &[(&Bytes, KeyAccess)]| {
            user.authorize_access_logged(
                spec.name.as_bytes(),
                categories.as_slice(),
                keys,
                global,
                channels,
                pattern_channels,
                all_channels,
                audit,
            )
            .map_err(|_| denied())
        };
        let Some(positions) = spec.key_positions(args).ok().flatten() else {
            return check(&[]);
        };
        let capacity = positions.size_hint().1.unwrap_or(usize::MAX);
        if capacity <= 8 {
            let empty = Bytes::new();
            let mut keys = [(&empty, KeyAccess::Read); 8];
            let mut count = 0;
            for position in positions {
                keys[count] = (&args[position], access(spec, args, position));
                count += 1;
            }
            check(&keys[..count])
        } else {
            let keys: Vec<_> = positions
                .map(|position| (&args[position], access(spec, args, position)))
                .collect();
            check(&keys)
        }
    }

    pub(super) fn command_audit(&self) -> Option<crate::auth::Audit<'_>> {
        let session = self.command_session.as_ref()?;
        Some(crate::auth::Audit {
            acl: session.acl.as_deref()?,
            clients: session.clients.as_deref()?,
            client_id: session.client_id,
            context: self.command_acl_context,
        })
    }

    pub fn command_exists(name: &[u8]) -> bool {
        registry::lookup(name).is_some()
    }
    pub fn command_names(category: Option<&str>) -> Vec<String> {
        super::all_specifications()
            .filter(|spec| {
                category.is_none_or(|category| {
                    categories(spec)
                        .as_slice()
                        .iter()
                        .any(|name| name.eq_ignore_ascii_case(category))
                })
            })
            .map(|spec| spec.name.to_owned())
            .collect()
    }
}

pub(super) fn key_readable(engine: &Engine, key: &Bytes) -> bool {
    let Some(user) = &engine.command_user else {
        return true;
    };
    let Some(spec) = registry::lookup(engine.command_name.as_bytes()) else {
        return false;
    };
    let categories = categories(spec);
    user.authorize_access(
        spec.name.as_bytes(),
        categories.as_slice(),
        &[(key, KeyAccess::Read)],
        false,
        &[],
        false,
        false,
    )
    .is_ok()
}
pub(super) fn prefixes_readable(engine: &Engine, prefixes: &[Bytes]) -> bool {
    let Some(user) = &engine.command_user else {
        return true;
    };
    let Some(spec) = registry::lookup(engine.command_name.as_bytes()) else {
        return false;
    };
    let categories = categories(spec);
    // Listing visible indexes is filtering, not a rejected authorization.
    let audit = (spec.name != "ft._list")
        .then(|| engine.command_audit())
        .flatten();
    user.authorize_prefixes_and_keys_logged(
        spec.name.as_bytes(),
        categories.as_slice(),
        prefixes,
        KeyAccess::Read,
        &[],
        audit.as_ref(),
    )
    .is_ok()
}

pub(super) fn module_category(name: &str) -> Option<&'static str> {
    if name.starts_with("fq.") {
        return Some("queue");
    }
    if name.starts_with("bf.") {
        return Some("bloom");
    }
    if name.starts_with("cf.") {
        return Some("cuckoo");
    }
    if name.starts_with("cms.") {
        return Some("cms");
    }
    if name.starts_with("topk.") {
        return Some("topk");
    }
    if name.starts_with("tdigest.") {
        return Some("tdigest");
    }
    match catalog::group(name) {
        Some("server" | "cluster" | "connection" | "transaction" | "scripting") => None,
        group => group,
    }
}
