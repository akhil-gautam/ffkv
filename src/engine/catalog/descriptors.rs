//! Native registrations share COMMAND's discovery surface without leaking libraries.
use super::{CommandSpec, Resp, array, bulk, fields};
use crate::server_commands::extensions::CommandMetadata;
use bytes::Bytes;

pub(super) enum Descriptor<'a> {
    Builtin(&'static CommandSpec),
    Module(&'a CommandMetadata),
}
impl Descriptor<'_> {
    pub(super) fn name(&self) -> &str {
        match self {
            Self::Builtin(spec) => spec.name,
            Self::Module(spec) => &spec.name,
        }
    }
    pub(super) fn describe(&self) -> Resp {
        match self {
            Self::Builtin(spec) => super::describe(spec),
            Self::Module(spec) => describe_module(spec),
        }
    }
    pub(super) fn documentation(&self) -> Resp {
        match self {
            Self::Builtin(spec) => super::documentation(spec),
            Self::Module(spec) => fields([
                ("group", bulk("module")),
                ("module", bulk(spec.module_name.clone())),
            ]),
        }
    }
    pub(super) fn category(&self, category: &str) -> bool {
        match self {
            Self::Builtin(spec) => super::super::authorization::categories(spec)
                .as_slice()
                .iter()
                .any(|value| value.eq_ignore_ascii_case(category)),
            Self::Module(spec) => spec
                .categories
                .iter()
                .any(|value| value.eq_ignore_ascii_case(category)),
        }
    }
    pub(super) fn module(&self, name: &[u8]) -> bool {
        matches!(self,Self::Module(spec) if name.eq_ignore_ascii_case(spec.module_name.as_bytes()))
    }
}
fn describe_module(spec: &CommandMetadata) -> Resp {
    let key_specs = if spec.first_key <= 0 {
        Vec::new()
    } else {
        vec![fields([
            ("flags", super::super::strings(key_flags(spec))),
            (
                "begin_search",
                fields([
                    ("type", bulk("index")),
                    ("spec", fields([("index", Resp::Integer(spec.first_key))])),
                ]),
            ),
            (
                "find_keys",
                fields([
                    ("type", bulk("range")),
                    (
                        "spec",
                        fields([
                            (
                                "lastkey",
                                Resp::Integer(if spec.last_key < 0 {
                                    spec.last_key
                                } else {
                                    spec.last_key - spec.first_key
                                }),
                            ),
                            ("keystep", Resp::Integer(spec.key_step)),
                            ("limit", Resp::Integer(0)),
                        ]),
                    ),
                ]),
            ),
        ])]
    };
    array(vec![
        bulk(spec.name.clone()),
        Resp::Integer(spec.arity),
        super::super::strings(&spec.flags),
        Resp::Integer(spec.first_key),
        Resp::Integer(spec.last_key),
        Resp::Integer(spec.key_step),
        array(
            spec.categories
                .iter()
                .map(|category| bulk(format!("@{category}")))
                .collect(),
        ),
        array(Vec::new()),
        array(key_specs),
        array(Vec::new()),
    ])
}
pub(super) fn key_flags(spec: &CommandMetadata) -> &'static [&'static str] {
    if spec.flags.contains(&"readonly") {
        &["RO", "access"]
    } else {
        &["RW", "access", "update"]
    }
}
pub(super) fn module_keys(
    spec: &CommandMetadata,
    args: &[Bytes],
    with_flags: bool,
    limit: usize,
) -> Resp {
    let count = args.len() as i64;
    if (spec.arity >= 0 && count != spec.arity) || (spec.arity < 0 && count < -spec.arity) {
        return super::super::wrong_arity(&spec.name);
    }
    if spec.first_key <= 0 || spec.key_step <= 0 {
        return if spec.flags.contains(&"no_mandatory_keys") {
            array(Vec::new())
        } else {
            super::error("ERR The command has no key arguments")
        };
    }
    let last = if spec.last_key < 0 {
        count + spec.last_key
    } else {
        spec.last_key
    };
    if last < spec.first_key || last >= count {
        return super::error("ERR Invalid arguments specified for command");
    }
    super::super::bounded_replies(
        (spec.first_key..=last)
            .step_by(spec.key_step as usize)
            .map(|position| {
                let key = Resp::Bulk(Some(args[position as usize].clone()));
                if with_flags {
                    array(vec![key, super::super::strings(key_flags(spec))])
                } else {
                    key
                }
            }),
        limit,
    )
}

pub(super) fn lookup<'a>(commands: &'a [CommandMetadata], name: &[u8]) -> Option<Descriptor<'a>> {
    super::super::registry::lookup(name)
        .map(Descriptor::Builtin)
        .or_else(|| {
            commands
                .iter()
                .find(|command| name.eq_ignore_ascii_case(command.name.as_bytes()))
                .map(Descriptor::Module)
        })
}

pub(super) fn roots(commands: &[CommandMetadata]) -> impl Iterator<Item = Descriptor<'_>> {
    super::super::specifications()
        .map(Descriptor::Builtin)
        .chain(commands.iter().map(Descriptor::Module))
}

pub(super) fn all(commands: &[CommandMetadata]) -> impl Iterator<Item = Descriptor<'_>> {
    super::super::all_specifications()
        .map(Descriptor::Builtin)
        .chain(commands.iter().map(Descriptor::Module))
}
