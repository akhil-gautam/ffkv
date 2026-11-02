//! Immutable compile-time command registrations indexed once at startup.
use super::CommandSpec;
use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet},
    sync::LazyLock,
};

struct Registry {
    commands: HashMap<&'static str, CommandSpec>,
    containers: HashSet<&'static str>,
}
static REGISTRY: LazyLock<Registry> = LazyLock::new(|| {
    let mut commands = HashMap::with_capacity(super::all_specifications().count());
    let mut containers = HashSet::new();
    for spec in super::all_specifications() {
        assert!(
            spec.name.len() <= 128,
            "static command name exceeds lookup bound"
        );
        let mut registered = *spec;
        if let Some(flags) = super::command_flags::lookup(spec.name) {
            registered.flags = flags;
        }
        assert!(
            commands.insert(spec.name, registered).is_none(),
            "duplicate command registration: {}",
            spec.name
        );
        if let Some((parent, _)) = spec.name.split_once('|') {
            containers.insert(parent);
        }
    }
    Registry {
        commands,
        containers,
    }
});

pub(super) fn lookup(name: &[u8]) -> Option<&'static CommandSpec> {
    if name.len() > 128 {
        return None;
    }
    let mut normalized = [0u8; 128];
    for (output, input) in normalized.iter_mut().zip(name) {
        *output = input.to_ascii_lowercase();
    }
    let normalized = std::str::from_utf8(&normalized[..name.len()]).ok()?;
    REGISTRY.commands.get(normalized)
}

pub(super) fn root_spec(args: &[Bytes]) -> Option<&'static CommandSpec> {
    let name = args.first()?;
    if name.contains(&b'|') {
        return None;
    }
    lookup(name)
}

pub(super) fn effective_spec(args: &[Bytes]) -> Option<&'static CommandSpec> {
    let root = root_spec(args)?;
    let Some(subcommand) = args.get(1) else {
        return Some(root);
    };
    if !REGISTRY.containers.contains(root.name) {
        return Some(root);
    }
    let length = root
        .name
        .len()
        .checked_add(1)?
        .checked_add(subcommand.len())?;
    if length > 128 {
        return Some(root);
    }
    let mut name = [0u8; 128];
    name[..root.name.len()].copy_from_slice(root.name.as_bytes());
    name[root.name.len()] = b'|';
    name[root.name.len() + 1..length].copy_from_slice(subcommand);
    lookup(&name[..length]).or(Some(root))
}

pub(super) fn is_container(name: &str) -> bool {
    REGISTRY.containers.contains(name)
}
