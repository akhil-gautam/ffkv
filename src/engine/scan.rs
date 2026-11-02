use std::{
    collections::{BTreeSet, hash_map::DefaultHasher},
    hash::{Hash, Hasher},
};

use bytes::Bytes;

use super::{Resp, error, integer, syntax_error};

pub(super) type ScanIndex = BTreeSet<(u64, Bytes)>;

pub(super) fn stable_hash(value: &Bytes) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

pub(super) struct ScanOptions {
    pub cursor: u64,
    pub count: usize,
    pub pattern: Option<Bytes>,
    pub kind: Option<Bytes>,
}

pub(super) fn parse_scan(
    args: &[Bytes],
    cursor_index: usize,
    allow_type: bool,
) -> Result<ScanOptions, Resp> {
    let cursor = &args[cursor_index];
    if cursor.is_empty()
        || (cursor.len() > 1 && cursor[0] == b'0')
        || !cursor.iter().all(u8::is_ascii_digit)
    {
        return Err(error("ERR invalid cursor"));
    }
    let cursor = cursor
        .iter()
        .try_fold(0_u64, |value, byte| {
            value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))
        })
        .ok_or_else(|| error("ERR invalid cursor"))?;
    let mut options = ScanOptions {
        cursor,
        count: 10,
        pattern: None,
        kind: None,
    };
    let mut position = cursor_index + 1;
    while position < args.len() {
        let Some(value) = args.get(position + 1) else {
            return Err(syntax_error());
        };
        if args[position].eq_ignore_ascii_case(b"MATCH") {
            options.pattern = Some(value.clone());
        } else if args[position].eq_ignore_ascii_case(b"COUNT") {
            options.count = integer(value)
                .filter(|count| *count > 0)
                .and_then(|count| usize::try_from(count).ok())
                .ok_or_else(syntax_error)?;
        } else if allow_type && args[position].eq_ignore_ascii_case(b"TYPE") {
            options.kind = Some(value.clone());
        } else {
            return Err(syntax_error());
        }
        position += 2;
    }
    Ok(options)
}

// Cursor order is immutable hash order, not a mutable array/rank offset. Every
// entry present throughout a full iteration remains reachable despite mutation.
// Complete hash buckets together, so collisions cannot strand entries at a cursor.
pub(super) fn scan_entries(
    index: &ScanIndex,
    options: &ScanOptions,
    limit: usize,
) -> Result<(u64, Vec<Bytes>), Resp> {
    let mut budget = super::ReplyBudget::new(limit);
    let mut values = Vec::new();
    let mut previous_hash = None;
    for (visited, (hash, value)) in index.range((options.cursor, Bytes::new())..).enumerate() {
        if visited >= options.count && previous_hash != Some(*hash) {
            return Ok((*hash, values));
        }
        previous_hash = Some(*hash);
        if options
            .pattern
            .as_ref()
            .is_none_or(|pattern| glob_matches(pattern, value))
        {
            budget.charge(1, value.len())?;
            values.push(value.clone());
        }
    }
    Ok((0, values))
}

// Iterative glob matching avoids a recursive stack on untrusted patterns.
pub(super) fn glob_matches(pattern: &[u8], value: &[u8]) -> bool {
    let (mut p, mut v) = (0, 0);
    let mut star = None;
    let mut retry = 0;
    while v < value.len() {
        if pattern.get(p) == Some(&b'*') {
            while pattern.get(p) == Some(&b'*') {
                p += 1;
            }
            star = Some(p);
            retry = v;
            continue;
        }
        let matched = match pattern.get(p) {
            Some(b'?') => Some(p + 1),
            Some(b'[') => class_match(pattern, p + 1, value[v]),
            Some(b'\\') if p + 1 < pattern.len() => (pattern[p + 1] == value[v]).then_some(p + 2),
            Some(byte) if *byte == value[v] => Some(p + 1),
            _ => None,
        };
        if let Some(next) = matched {
            p = next;
            v += 1;
        } else if let Some(next) = star {
            retry += 1;
            v = retry;
            p = next;
        } else {
            return false;
        }
    }
    while pattern.get(p) == Some(&b'*') {
        p += 1;
    }
    p == pattern.len()
}

fn class_match(pattern: &[u8], mut p: usize, value: u8) -> Option<usize> {
    let inverted = pattern.get(p) == Some(&b'^');
    if inverted {
        p += 1;
    }
    let mut matched = false;
    while p < pattern.len() && pattern[p] != b']' {
        let start = if pattern[p] == b'\\' && p + 1 < pattern.len() {
            p += 1;
            pattern[p]
        } else {
            pattern[p]
        };
        p += 1;
        if pattern.get(p) == Some(&b'-') && p + 1 < pattern.len() && pattern[p + 1] != b']' {
            p += 1;
            let stop = if pattern[p] == b'\\' && p + 1 < pattern.len() {
                p += 1;
                pattern[p]
            } else {
                pattern[p]
            };
            p += 1;
            matched |= (start.min(stop)..=start.max(stop)).contains(&value);
        } else {
            matched |= value == start;
        }
    }
    (pattern.get(p) == Some(&b']') && matched != inverted).then_some(p + 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_collision_bucket_is_rejected_instead_of_skipping_members() {
        let index = [(7, Bytes::from_static(b"a")), (7, Bytes::from_static(b"b"))]
            .into_iter()
            .collect();
        let options = ScanOptions {
            cursor: 0,
            count: 1,
            pattern: None,
            kind: None,
        };
        let row = std::mem::size_of::<Resp>() + 1;
        assert!(scan_entries(&index, &options, row).is_err());
        let (cursor, members) = scan_entries(&index, &options, row * 2).unwrap();
        assert_eq!(cursor, 0);
        assert_eq!(
            members,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }

    #[test]
    fn glob_supports_binary_wildcards_ranges_and_escapes() {
        assert!(glob_matches(b"job:*:[a-c]?", b"job:\0\xff:b9"));
        assert!(glob_matches(b"a\\*b", b"a*b"));
        assert!(!glob_matches(b"a\\*b", b"axxb"));
        assert!(glob_matches(b"[^0-9]*", b"task"));
        assert!(!glob_matches(b"[^0-9]*", b"1task"));
        assert!(glob_matches(b"*a*b", b"xxaxxxab"));
        assert!(!glob_matches(b"*a*b", b"xxaxxxac"));
    }

    #[test]
    fn hash_cursor_keeps_stable_entries_reachable_across_mutations() {
        let entries: Vec<_> = (0..80)
            .map(|i| Bytes::from(format!("member:{i}")))
            .collect();
        let stable: Vec<_> = entries.iter().take(40).cloned().collect();
        let mut index: ScanIndex = entries
            .iter()
            .map(|value| (stable_hash(value), value.clone()))
            .collect();
        let mut options = ScanOptions {
            cursor: 0,
            count: 3,
            pattern: None,
            kind: None,
        };
        let mut found = BTreeSet::new();
        loop {
            let (cursor, values) = scan_entries(&index, &options, usize::MAX).unwrap();
            found.extend(values);
            // Remove transient entries and add others while all stable ones remain.
            for value in entries.iter().skip(40) {
                index.remove(&(stable_hash(value), value.clone()));
            }
            let extra = Bytes::from(format!("new:{cursor}"));
            index.insert((stable_hash(&extra), extra));
            if cursor == 0 {
                break;
            }
            options.cursor = cursor;
        }
        assert!(stable.iter().all(|value| found.contains(value)));
    }
}
