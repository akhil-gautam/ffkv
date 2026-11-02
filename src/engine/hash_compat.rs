use std::collections::HashSet;

use bytes::Bytes;
use rand::{Rng, seq::index};

use super::super::{
    CommandSpec, Engine, Entry, Mutation, Resp, Value, array, error, integer, integer_error,
    syntax_error, wrong_arity,
};
use super::{HashValue, field_arguments, field_deadline};

pub(in crate::engine) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "hgetdel",
        -5,
        &["write", "fast"],
        (1, 1, 1),
        Engine::hash_get_mutate,
    ),
    CommandSpec::handler(
        "hgetex",
        -5,
        &["write", "fast"],
        (1, 1, 1),
        Engine::hash_get_mutate,
    ),
    CommandSpec::handler(
        "hincrbyfloat",
        4,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::hash_increment_float,
    ),
    CommandSpec::handler(
        "hrandfield",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::hash_random_field,
    ),
    CommandSpec::handler("himport", -2, &[], (0, 0, 0), Engine::hash_import),
];

pub(in crate::engine) static HIMPORT_SUBCOMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "himport|prepare",
        -4,
        &["denyoom"],
        (0, 0, 0),
        Engine::hash_import,
    ),
    CommandSpec::handler(
        "himport|set",
        -5,
        &["write", "denyoom"],
        (2, 2, 1),
        Engine::hash_import,
    ),
    CommandSpec::handler("himport|discard", 3, &[], (0, 0, 0), Engine::hash_import),
    CommandSpec::handler("himport|discardall", 2, &[], (0, 0, 0), Engine::hash_import),
];

const MAX_IMPORT_FIELDSETS: usize = 65_536;
const MAX_IMPORT_BYTES: usize = 64 * 1024 * 1024;

fn fieldset_bytes(name: &Bytes, fields: &[Bytes]) -> Option<usize> {
    fields.iter().try_fold(
        name.len().checked_add(std::mem::size_of::<Bytes>())?,
        |size, field| {
            size.checked_add(field.len())?
                .checked_add(std::mem::size_of::<Bytes>())
        },
    )
}

pub(super) struct FieldOptions<'a> {
    pub(super) fields: &'a [Bytes],
    pub(super) expiration: Option<i64>,
    pub(super) retain_or_persist: bool,
    pub(super) condition: Option<bool>,
}

// FIELDS is a counted block, not necessarily the final option. Field names
// inside that block are binary data, including names identical to keywords.
pub(super) fn parse_field_options(
    args: &[Bytes],
    now: i64,
    setting: bool,
) -> Result<FieldOptions<'_>, Resp> {
    let expiration_error = || {
        error(if setting {
            "ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified"
        } else {
            "ERR Only one of EX, PX, EXAT, PXAT or PERSIST arguments can be specified"
        })
    };
    let mut fields = None;
    let mut expiration = None;
    let mut retain_or_persist = false;
    let mut condition = None;
    let mut position = 2;
    while let Some(option) = args.get(position) {
        if option.eq_ignore_ascii_case(b"FIELDS") {
            if fields.is_some() {
                return Err(error("ERR FIELDS keyword specified multiple times"));
            }
            let count = args
                .get(position + 1)
                .and_then(|value| integer(value))
                .filter(|count| (1..=i64::from(i32::MAX)).contains(count))
                .and_then(|count| usize::try_from(count).ok())
                .ok_or_else(|| error("ERR invalid number of fields"))?;
            let start = position + 2;
            let end = count
                .checked_mul(if setting { 2 } else { 1 })
                .and_then(|length| start.checked_add(length))
                .ok_or_else(syntax_error)?;
            fields = Some(
                args.get(start..end)
                    .ok_or_else(|| error("ERR wrong number of arguments"))?,
            );
            position = end;
            continue;
        }
        if setting && (option.eq_ignore_ascii_case(b"FNX") || option.eq_ignore_ascii_case(b"FXX")) {
            if condition.is_some() {
                return Err(error(
                    "ERR Only one of FXX or FNX arguments can be specified",
                ));
            }
            condition = Some(option.eq_ignore_ascii_case(b"FNX"));
        } else if (setting && option.eq_ignore_ascii_case(b"KEEPTTL"))
            || (!setting && option.eq_ignore_ascii_case(b"PERSIST"))
        {
            if retain_or_persist || expiration.is_some() {
                return Err(expiration_error());
            }
            retain_or_persist = true;
        } else {
            let seconds =
                option.eq_ignore_ascii_case(b"EX") || option.eq_ignore_ascii_case(b"EXAT");
            let relative = option.eq_ignore_ascii_case(b"EX") || option.eq_ignore_ascii_case(b"PX");
            if !(seconds
                || option.eq_ignore_ascii_case(b"PX")
                || option.eq_ignore_ascii_case(b"PXAT"))
            {
                return Err(Resp::Error(format!(
                    "ERR unknown argument: {}",
                    String::from_utf8_lossy(option)
                )));
            }
            if expiration.is_some() || retain_or_persist {
                return Err(expiration_error());
            }
            let value = args
                .get(position + 1)
                .ok_or_else(|| error("ERR missing expire time"))?;
            expiration = Some(field_deadline(value, seconds, relative, now)?);
            position += 1;
        }
        position += 1;
    }
    Ok(FieldOptions {
        fields: fields.ok_or_else(|| error("ERR missing FIELDS argument"))?,
        expiration,
        retain_or_persist,
        condition,
    })
}

impl Engine {
    fn hash_import(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(subcommand) = args.get(1) else {
            return wrong_arity("himport");
        };
        if subcommand.eq_ignore_ascii_case(b"SET") {
            if args.len() < 5 {
                return wrong_arity("himport|set");
            }
            // Redis 8.10 rejects non-hash targets before checking the session's
            // fieldset or value count, even though SET replaces an existing hash.
            if let Err(reply) = self.hash_value(&args[2], now) {
                return reply;
            }
            let fields = {
                let Some(fieldsets) = self.command_hash_fieldsets() else {
                    return error("ERR HIMPORT requires a stateful client context");
                };
                let fieldsets = fieldsets
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
                let Some(fields) = fieldsets.get(&args[3]) else {
                    return error("ERR no such fieldset");
                };
                if fields.len() != args.len() - 4 {
                    return error("ERR value count does not match fieldset field count");
                }
                fields.clone()
            };
            let mut hash = HashValue {
                fields: std::collections::HashMap::with_capacity(fields.len()),
                random_fields: Vec::with_capacity(fields.len()),
                ..HashValue::default()
            };
            for (field, value) in fields.iter().zip(&args[4..]) {
                hash.set(field.clone(), Bytes::copy_from_slice(value), None);
            }
            let existed = self.remove(&args[2]).is_some();
            let key = self.own_key(&args[2]);
            self.write_entry(
                key,
                Entry {
                    value: Value::Hash(hash),
                    expires_at: None,
                    access: crate::engine::AccessMetadata::default(),
                },
                now,
            );
            if existed {
                self.record(now, || Mutation::Delete {
                    key: args[2].clone(),
                });
            }
            self.record_hash_fields(&args[2], fields.iter(), now);
            return Resp::Simple("OK");
        }
        let Some(fieldsets) = self.command_hash_fieldsets() else {
            return error("ERR HIMPORT requires a stateful client context");
        };
        if subcommand.eq_ignore_ascii_case(b"PREPARE") {
            if args.len() < 4 {
                return wrong_arity("himport|prepare");
            }
            let Some(mut size) = fieldset_bytes(&args[2], &args[3..]) else {
                return error("ERR HIMPORT session fieldset memory limit exceeded");
            };
            if size > MAX_IMPORT_BYTES {
                return error("ERR HIMPORT session fieldset memory limit exceeded");
            }
            let mut unique = HashSet::with_capacity(args.len() - 3);
            for field in &args[3..] {
                if !unique.insert(field) {
                    return error("ERR duplicate field name in fieldset");
                }
            }
            let mut fieldsets = fieldsets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            if !fieldsets.contains_key(&args[2]) && fieldsets.len() >= MAX_IMPORT_FIELDSETS {
                return error("ERR HIMPORT session fieldset count limit exceeded");
            }
            for (name, fields) in fieldsets.iter().filter(|(name, _)| *name != &args[2]) {
                size = match fieldset_bytes(name, fields).and_then(|bytes| size.checked_add(bytes))
                {
                    Some(size) if size <= MAX_IMPORT_BYTES => size,
                    _ => return error("ERR HIMPORT session fieldset memory limit exceeded"),
                };
            }
            let fields: std::sync::Arc<[Bytes]> = args[3..]
                .iter()
                .map(|field| Bytes::copy_from_slice(field))
                .collect();
            fieldsets.insert(Bytes::copy_from_slice(&args[2]), fields);
            Resp::Simple("OK")
        } else if subcommand.eq_ignore_ascii_case(b"DISCARD") {
            if args.len() != 3 {
                return wrong_arity("himport|discard");
            }
            let removed = fieldsets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&args[2])
                .is_some();
            Resp::Integer(i64::from(removed))
        } else if subcommand.eq_ignore_ascii_case(b"DISCARDALL") {
            if args.len() != 2 {
                return wrong_arity("himport|discardall");
            }
            let mut fieldsets = fieldsets
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let count = fieldsets.len();
            fieldsets.clear();
            Resp::Integer(count as i64)
        } else {
            error("ERR unknown subcommand for 'himport'")
        }
    }

    fn hash_get_mutate(&mut self, args: &[Bytes], now: i64) -> Resp {
        let delete = args[0].eq_ignore_ascii_case(b"HGETDEL");
        if let Err(reply) = self.hash_value(&args[1], now) {
            return reply;
        }
        let (fields, expiration, persist) = if delete {
            match field_arguments(args, 2, false) {
                Ok(fields) => (fields, None, false),
                Err(reply) => return reply,
            }
        } else {
            match parse_field_options(args, now, false) {
                Ok(options) => (
                    options.fields,
                    options.expiration,
                    options.retain_or_persist,
                ),
                Err(reply) => return reply,
            }
        };
        let removing = delete || expiration.is_some_and(|deadline| deadline <= now);
        let mut budget = super::super::ReplyBudget::new(self.reply_limit);
        let hash = match self.hash_value(&args[1], now) {
            Ok(hash) => hash,
            Err(reply) => return reply,
        };
        // Budget the actual ordered reply before any field is changed. A repeated
        // field is absent after its first destructive access.
        let mut seen = HashSet::new();
        for field in fields {
            let value = if !removing || fields.len() == 1 || seen.insert(field) {
                hash.and_then(|hash| hash.fields.get(field))
            } else {
                None
            };
            if let Err(reply) = budget.charge(1, value.map_or(0, |value| value.value.len())) {
                return reply;
            }
        }
        if hash.is_none() {
            return array(fields.iter().map(|_| Resp::Bulk(None)).collect());
        }
        let previous = self.hash_deadline(&args[1]);
        let journal = self.journal_enabled;
        let mut changed = Vec::new();
        let mut result = Vec::with_capacity(fields.len());
        let Value::Hash(hash) = &mut self.entries.get_mut(&args[1]).expect("existing hash").value
        else {
            unreachable!();
        };
        for field in fields {
            let value = hash.fields.get(field).map(|value| value.value.clone());
            if value.is_some() {
                if removing {
                    hash.remove(field);
                    if journal {
                        changed.push(field.clone());
                    }
                } else if persist || expiration.is_some() {
                    let deadline = if persist { None } else { expiration };
                    if hash.fields.get(field).expect("existing field").expires_at != deadline {
                        hash.set_expiry(field, deadline);
                        if journal {
                            changed.push(field.clone());
                        }
                    }
                }
            }
            result.push(Resp::Bulk(value));
        }
        self.finish_hash_mutation(&args[1], previous);
        if removing {
            if !changed.is_empty() {
                self.record(now, || Mutation::HashDelete {
                    key: args[1].clone(),
                    fields: changed,
                });
            }
        } else {
            self.record_hash_fields(&args[1], changed.iter(), now);
        }
        array(result)
    }

    fn hash_increment_float(&mut self, args: &[Bytes], now: i64) -> Resp {
        use super::super::string_compat::{add_float, format_float, parse_float};
        let increment = match parse_float(&args[3]) {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let (old, deadline) = match self.hash_value(&args[1], now) {
            Err(reply) => return reply,
            Ok(hash) => match hash.and_then(|hash| hash.fields.get(&args[2])) {
                Some(field) => match parse_float(&field.value) {
                    Ok(value) => (value, field.expires_at),
                    Err(_) => return error("ERR hash value is not a float"),
                },
                None => (parse_float(b"0").expect("zero float"), None),
            },
        };
        let value = match add_float(old, increment) {
            Ok(value) => format_float(value),
            Err(reply) => return reply,
        };
        if let Err(reply) = super::super::ReplyBudget::new(self.reply_limit).charge(1, value.len())
        {
            return reply;
        }
        let previous = self.hash_deadline(&args[1]);
        let Value::Hash(hash) =
            self.collection_entry(&args[1], now, || Value::Hash(HashValue::default()))
        else {
            unreachable!();
        };
        let field = hash
            .fields
            .get_key_value(&args[2])
            .map(|(field, _)| field.clone())
            .unwrap_or_else(|| Bytes::copy_from_slice(&args[2]));
        hash.set(field, value.clone(), deadline);
        self.finish_hash_mutation(&args[1], previous);
        self.record_hash_fields(&args[1], std::iter::once(&args[2]), now);
        Resp::Bulk(Some(value))
    }

    fn hash_random_field(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() > 4 {
            return syntax_error();
        }
        let count = match args.get(2) {
            None => None,
            Some(raw) => match integer(raw) {
                Some(i64::MIN) | None => return integer_error(),
                Some(count) => Some(count),
            },
        };
        let with_values = match args.get(3) {
            None => false,
            Some(option) if option.eq_ignore_ascii_case(b"WITHVALUES") => true,
            Some(_) => return syntax_error(),
        };
        if with_values
            && count.is_some_and(|count| !(-(i64::MAX / 2)..=(i64::MAX / 2)).contains(&count))
        {
            return error("ERR value is out of range");
        }
        let resp3 = self.command_version() == crate::protocol::Version::Resp3;
        let mut budget = super::super::ReplyBudget::new(self.reply_limit);
        let hash = match self.hash_value(&args[1], now) {
            Ok(Some(hash)) => hash,
            Ok(None) => {
                return if count.is_none() {
                    Resp::Bulk(None)
                } else {
                    array(Vec::new())
                };
            }
            Err(reply) => return reply,
        };
        let mut rng = rand::rng();
        let Some(count) = count else {
            let field = &hash.random_fields[rng.random_range(0..hash.random_fields.len())];
            return match budget.charge(1, field.len()) {
                Ok(()) => Resp::Bulk(Some(field.clone())),
                Err(reply) => reply,
            };
        };
        let requested = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
        let length = if count < 0 {
            requested
        } else {
            requested.min(hash.fields.len())
        };
        if let Err(reply) = budget.charge(
            length.saturating_mul(if with_values {
                if resp3 { 3 } else { 2 }
            } else {
                1
            }),
            0,
        ) {
            return reply;
        }
        let mut result =
            Vec::with_capacity(length.saturating_mul(if with_values && !resp3 { 2 } else { 1 }));
        let mut append = |position: usize| -> Result<(), Resp> {
            let field = &hash.random_fields[position];
            let value = &hash.fields.get(field).expect("indexed hash field").value;
            budget.charge(
                0,
                field
                    .len()
                    .saturating_add(if with_values { value.len() } else { 0 }),
            )?;
            let field = Resp::Bulk(Some(field.clone()));
            if with_values && resp3 {
                result.push(array(vec![field, Resp::Bulk(Some(value.clone()))]));
            } else {
                result.push(field);
                if with_values {
                    result.push(Resp::Bulk(Some(value.clone())));
                }
            }
            Ok(())
        };
        if count < 0 {
            for _ in 0..length {
                if let Err(reply) = append(rng.random_range(0..hash.fields.len())) {
                    return reply;
                }
            }
        } else if length == 1 {
            if let Err(reply) = append(rng.random_range(0..hash.fields.len())) {
                return reply;
            }
        } else if length == hash.fields.len() {
            for position in 0..length {
                if let Err(reply) = append(position) {
                    return reply;
                }
            }
        } else {
            for position in index::sample(&mut rng, hash.fields.len(), length) {
                if let Err(reply) = append(position) {
                    return reply;
                }
            }
        }
        array(result)
    }
}
