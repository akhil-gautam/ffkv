use bytes::Bytes;

use super::{
    CommandSpec, Engine, ExpiryKind, Mutation, Resp, Value, array, error, integer, integer_error,
    invalid_expiry, syntax_error, wrong_type,
};

pub(super) use stored::{StringBuffer, StringValue, StringWrite};

/// Stored string ownership. Private so that no engine code, including the
/// command handlers below, can place foreign memory into a stored string.
mod stored {
    use std::ops::{Deref, DerefMut};

    use bytes::{Bytes, BytesMut};

    /// Retention policy for a reusable block: doubling growth always fits, and
    /// a shrunken value releases an oversized block instead of monopolizing it.
    fn retention_limit(len: usize) -> usize {
        len.saturating_mul(2).saturating_add(64)
    }

    fn fits(capacity: usize, len: usize) -> bool {
        len <= capacity && capacity <= retention_limit(len)
    }

    /// A stored string. `bytes` is the static empty value or the whole
    /// (offset zero) view of a block this module allocated, within `fits`.
    /// Every other handle to that block is a view cloned from `bytes()`, so a
    /// unique handle's capacity is exactly the block the value owns. Nothing
    /// here adopts caller `Bytes`/`BytesMut`: network, WAL and dump slices
    /// are always copied rather than retained.
    #[derive(Default)]
    pub(in crate::engine) struct StringValue {
        bytes: Bytes,
    }

    const _: () = assert!(std::mem::size_of::<StringValue>() == std::mem::size_of::<Bytes>());

    /// Exclusive editable string storage, created only by this module.
    pub(in crate::engine) struct StringBuffer(BytesMut);

    pub(in crate::engine) enum StringWrite<'a> {
        /// Borrowed request/replay/import bytes, copied into owned storage.
        Copy(&'a Bytes),
        /// An integer in Redis spelling, formatted without a heap string.
        Integer(i64),
        /// A buffer edited after `take_buffer` or built by the engine.
        Buffer(StringBuffer),
        /// Rewrite only the TTL of the current string.
        Keep,
    }

    impl StringValue {
        /// `capture` returns the resolved `Mutation::Set` bytes, as `apply`.
        pub(in crate::engine) fn new(
            write: StringWrite<'_>,
            capture: bool,
        ) -> (Self, Option<Bytes>) {
            assert!(
                !matches!(write, StringWrite::Keep),
                "a TTL-only rewrite requires an existing string"
            );
            let mut value = Self::default();
            let resolved = value.apply(write, capture);
            (value, resolved)
        }

        pub(in crate::engine) fn bytes(&self) -> &Bytes {
            &self.bytes
        }

        pub(in crate::engine) fn into_bytes(self) -> Bytes {
            self.bytes
        }

        /// Moves the value out for editing, leaving static empty until the
        /// buffer is stored again. Outstanding views keep their bytes: a
        /// shared value is copied once, sized for `additional` growth.
        pub(in crate::engine) fn take_buffer(&mut self, additional: usize) -> StringBuffer {
            match std::mem::take(&mut self.bytes).try_into_mut() {
                Ok(mut buffer) => {
                    buffer.reserve(additional);
                    StringBuffer(buffer)
                }
                Err(shared) => {
                    let mut buffer = BytesMut::with_capacity(shared.len() + additional);
                    buffer.extend_from_slice(&shared);
                    StringBuffer(buffer)
                }
            }
        }

        /// Applies `write`; with `capture`, returns the bytes a resolved
        /// `Mutation::Set` carries. Copies journal the caller's immutable
        /// bytes and integers a separate copy, so the stored block stays
        /// unshared and reusable; buffers and TTL rewrites journal a view.
        pub(in crate::engine) fn apply(
            &mut self,
            write: StringWrite<'_>,
            capture: bool,
        ) -> Option<Bytes> {
            match write {
                StringWrite::Copy(value) => {
                    self.copy(value, retention_limit(value.len()));
                    capture.then(|| value.clone())
                }
                StringWrite::Integer(value) => {
                    self.format_integer(value);
                    capture.then(|| Bytes::copy_from_slice(&self.bytes))
                }
                StringWrite::Buffer(buffer) => {
                    self.finish(buffer);
                    capture.then(|| self.bytes.clone())
                }
                StringWrite::Keep => capture.then(|| self.bytes.clone()),
            }
        }

        fn copy(&mut self, source: &[u8], maximum_capacity: usize) {
            let mut buffer = match std::mem::take(&mut self.bytes).try_into_mut() {
                Ok(mut buffer)
                    if source.len() <= buffer.capacity()
                        && buffer.capacity() <= maximum_capacity =>
                {
                    buffer.clear();
                    buffer
                }
                // Release a shared or ill-fitting block before replacing it.
                rejected => {
                    drop(rejected);
                    BytesMut::with_capacity(source.len())
                }
            };
            buffer.extend_from_slice(source);
            self.bytes = buffer.freeze();
        }

        fn format_integer(&mut self, value: i64) {
            let spelling = crate::protocol::Decimal::signed(value);
            let digits = spelling.as_bytes();
            // Exact capacity avoids a fresh shared header on every subsequent
            // freeze after a digit-width shrink.
            self.copy(digits, digits.len());
        }

        fn finish(&mut self, StringBuffer(buffer): StringBuffer) {
            self.bytes = if fits(buffer.capacity(), buffer.len()) {
                buffer.freeze()
            } else {
                let mut exact = BytesMut::with_capacity(buffer.len());
                exact.extend_from_slice(&buffer);
                exact.freeze()
            };
        }
    }

    impl StringBuffer {
        pub(in crate::engine) fn zeroed(len: usize) -> Self {
            Self(BytesMut::zeroed(len))
        }

        pub(in crate::engine) fn with_capacity(capacity: usize) -> Self {
            Self(BytesMut::with_capacity(capacity))
        }

        pub(in crate::engine) fn extend_from_slice(&mut self, bytes: &[u8]) {
            self.0.extend_from_slice(bytes);
        }

        pub(in crate::engine) fn resize(&mut self, len: usize, value: u8) {
            self.0.resize(len, value);
        }
    }

    impl Deref for StringBuffer {
        type Target = [u8];

        fn deref(&self) -> &[u8] {
            &self.0
        }
    }

    impl DerefMut for StringBuffer {
        fn deref_mut(&mut self) -> &mut [u8] {
            &mut self.0
        }
    }
}

// Keep logical strings below the engine/WAL row limit, including sparse writes.
const MAX_STRING: usize = 8 * 1024 * 1024;
const MAX_BITS: usize = MAX_STRING * 8;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "setnx",
        3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::string_setnx,
    ),
    CommandSpec::handler(
        "setex",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::string_setex,
    ),
    CommandSpec::handler(
        "psetex",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::string_setex,
    ),
    CommandSpec::handler(
        "getset",
        3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::string_getset,
    ),
    CommandSpec::handler(
        "getdel",
        2,
        &["write", "fast"],
        (1, 1, 1),
        Engine::string_getdel,
    ),
    CommandSpec::handler(
        "getex",
        -2,
        &["write", "fast"],
        (1, 1, 1),
        Engine::string_getex,
    ),
    CommandSpec::handler(
        "append",
        3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::string_append,
    ),
    CommandSpec::handler(
        "strlen",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::string_length,
    ),
    CommandSpec::handler(
        "getrange",
        4,
        &["readonly"],
        (1, 1, 1),
        Engine::string_getrange,
    ),
    CommandSpec::handler(
        "setrange",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::string_setrange,
    ),
    CommandSpec::handler(
        "msetnx",
        -3,
        &["write", "denyoom"],
        (1, -1, 2),
        Engine::string_msetnx,
    )
    .pairs(),
    CommandSpec::handler(
        "getbit",
        3,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::bitmap_getbit,
    ),
    CommandSpec::handler(
        "setbit",
        4,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::bitmap_setbit,
    ),
    CommandSpec::handler(
        "bitcount",
        -2,
        &["readonly"],
        (1, 1, 1),
        Engine::bitmap_count,
    ),
    CommandSpec::handler(
        "bitpos",
        -3,
        &["readonly"],
        (1, 1, 1),
        Engine::bitmap_position,
    ),
    CommandSpec::handler(
        "bitop",
        -4,
        &["write", "denyoom"],
        (2, -1, 1),
        Engine::bitmap_operation,
    ),
    CommandSpec::handler(
        "bitfield",
        -2,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::bitmap_field,
    ),
    CommandSpec::handler(
        "bitfield_ro",
        -2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::bitmap_field,
    ),
];

fn size_error() -> Resp {
    error("ERR string exceeds maximum allowed size (8 MiB)")
}

fn bit_offset(value: &[u8], width: usize, indexed: bool) -> Result<usize, Resp> {
    let (number, multiplier) = if indexed && value.first() == Some(&b'#') {
        (&value[1..], width)
    } else {
        (value, 1)
    };
    integer(number)
        .and_then(|n| usize::try_from(n).ok())
        .and_then(|n| n.checked_mul(multiplier))
        .filter(|n| n.checked_add(width).is_some_and(|end| end <= MAX_BITS))
        .ok_or_else(|| {
            error("ERR bit offset is not an integer or out of range (maximum string size is 8 MiB)")
        })
}

// Normalize without negating user input (i64::MIN is valid range syntax).
fn range(start: i64, end: i64, length: usize) -> Option<(usize, usize)> {
    let length = length as i64;
    let start = if start < 0 {
        start.saturating_add(length).max(0)
    } else {
        start
    };
    let end = if end < 0 {
        end.saturating_add(length).max(0)
    } else {
        end
    };
    let end = end.min(length - 1);
    (start <= end && start < length).then_some((start as usize, end.max(0) as usize))
}

impl Engine {
    pub(super) fn string_get(value: Option<&Value>, _args: &[Bytes]) -> Resp {
        match value {
            Some(Value::String(value)) => Resp::Bulk(Some(value.bytes().clone())),
            Some(_) => wrong_type(),
            None => Resp::Bulk(None),
        }
    }

    fn string_setnx(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[2].len() > MAX_STRING {
            return size_error();
        }
        if self.value(&args[1], now).is_some() {
            return Resp::Integer(0);
        }
        self.store(&args[1], StringWrite::Copy(&args[2]), None, now);
        Resp::Integer(1)
    }

    fn string_setex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(ttl) = integer(&args[2]) else {
            return integer_error();
        };
        let (kind, name) = if args[0].eq_ignore_ascii_case(b"setex") {
            (ExpiryKind::Ex, "setex")
        } else {
            (ExpiryKind::Px, "psetex")
        };
        let Some(deadline) = kind.deadline(ttl, now).filter(|_| ttl > 0) else {
            return invalid_expiry(name);
        };
        if args[3].len() > MAX_STRING {
            return size_error();
        }
        self.store(&args[1], StringWrite::Copy(&args[3]), Some(deadline), now);
        Resp::Simple("OK")
    }

    fn string_getset(&mut self, args: &[Bytes], now: i64) -> Resp {
        let previous = match self.value(&args[1], now) {
            Some(Value::String(value)) => Some(value.bytes().clone()),
            Some(_) => return wrong_type(),
            None => None,
        };
        if args[2].len() > MAX_STRING {
            return size_error();
        }
        self.store(&args[1], StringWrite::Copy(&args[2]), None, now);
        Resp::Bulk(previous)
    }

    fn string_getdel(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.value(&args[1], now) {
            Some(Value::String(_)) => {}
            Some(_) => return wrong_type(),
            None => return Resp::Bulk(None),
        }
        let entry = self.remove(&args[1]).expect("validated string exists");
        let Value::String(value) = entry.value else {
            unreachable!()
        };
        self.record(now, || Mutation::Delete {
            key: args[1].clone(),
        });
        Resp::Bulk(Some(value.into_bytes()))
    }

    fn string_getex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut expiry = None;
        let mut persist = false;
        let mut index = 2;
        while index < args.len() {
            let token = &args[index];
            if token.eq_ignore_ascii_case(b"PERSIST") && expiry.is_none() {
                persist = true;
            } else {
                let kind = if token.eq_ignore_ascii_case(b"EX") {
                    ExpiryKind::Ex
                } else if token.eq_ignore_ascii_case(b"PX") {
                    ExpiryKind::Px
                } else if token.eq_ignore_ascii_case(b"EXAT") {
                    ExpiryKind::Exat
                } else if token.eq_ignore_ascii_case(b"PXAT") {
                    ExpiryKind::Pxat
                } else {
                    return syntax_error();
                };
                if persist || expiry.is_some_and(|(previous, _)| previous != kind) {
                    return syntax_error();
                }
                index += 1;
                let Some(value) = args.get(index) else {
                    return syntax_error();
                };
                expiry = Some((kind, value));
            }
            index += 1;
        }
        // Redis checks existence/type before parsing the final expiry integer.
        let value = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes().clone(),
            Some(_) => return wrong_type(),
            None => return Resp::Bulk(None),
        };
        let previous = self
            .entries
            .get(&args[1])
            .and_then(|entry| entry.expires_at);
        let deadline = if let Some((kind, raw)) = expiry {
            let Some(ttl) = integer(raw) else {
                return integer_error();
            };
            let Some(deadline) = kind.deadline(ttl, now).filter(|_| ttl > 0) else {
                return invalid_expiry("getex");
            };
            Some(deadline)
        } else if persist {
            None
        } else {
            previous
        };
        if deadline != previous {
            self.store(&args[1], StringWrite::Keep, deadline, now);
        }
        Resp::Bulk(Some(value))
    }

    // All callers validate before taking ownership and store the buffer back
    // on every path. Unique values keep their block; a value still viewed by a
    // reply, journal, snapshot or undo row is copied once, sized for growth.
    pub(super) fn string_buffer(
        &mut self,
        key: &Bytes,
        additional: usize,
    ) -> (StringBuffer, Option<i64>) {
        match self.entries.get_mut(key) {
            Some(entry) => {
                let Value::String(value) = &mut entry.value else {
                    unreachable!()
                };
                (value.take_buffer(additional), entry.expires_at)
            }
            None => (StringBuffer::with_capacity(additional), None),
        }
    }

    fn string_append(&mut self, args: &[Bytes], now: i64) -> Resp {
        let length = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes().len(),
            Some(_) => return wrong_type(),
            None => 0,
        };
        let Some(length) = length
            .checked_add(args[2].len())
            .filter(|n| *n <= MAX_STRING)
        else {
            return size_error();
        };
        let (mut value, deadline) = self.string_buffer(&args[1], args[2].len());
        value.extend_from_slice(&args[2]);
        self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        Resp::Integer(length as i64)
    }

    fn string_length(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.value(&args[1], now) {
            Some(Value::String(value)) => Resp::Integer(value.bytes().len() as i64),
            Some(_) => wrong_type(),
            None => Resp::Integer(0),
        }
    }

    pub(super) fn string_getrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (Some(start), Some(end)) = (integer(&args[2]), integer(&args[3])) else {
            return integer_error();
        };
        let value = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes(),
            Some(_) => return wrong_type(),
            None => return Resp::Bulk(Some(Bytes::new())),
        };
        if start < 0 && end < 0 && start > end {
            return Resp::Bulk(Some(Bytes::new()));
        }
        Resp::Bulk(Some(match range(start, end, value.len()) {
            Some((start, end)) => value.slice(start..=end),
            None => Bytes::new(),
        }))
    }

    fn string_setrange(&mut self, args: &[Bytes], now: i64) -> Resp {
        let Some(offset) = integer(&args[2]) else {
            return integer_error();
        };
        if offset < 0 {
            return error("ERR offset is out of range");
        }
        let length = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes().len(),
            Some(_) => return wrong_type(),
            None => 0,
        };
        if args[3].is_empty() {
            return Resp::Integer(length as i64);
        }
        let Some(end) = usize::try_from(offset)
            .ok()
            .and_then(|n| n.checked_add(args[3].len()))
            .filter(|n| *n <= MAX_STRING)
        else {
            return size_error();
        };
        let (mut value, deadline) = self.string_buffer(&args[1], length.max(end) - length);
        value.resize(length.max(end), 0);
        value[offset as usize..end].copy_from_slice(&args[3]);
        self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        Resp::Integer(length.max(end) as i64)
    }

    fn string_msetnx(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args[2..]
            .iter()
            .step_by(2)
            .any(|value| value.len() > MAX_STRING)
        {
            return size_error();
        }
        for pair in args[1..].as_chunks::<2>().0 {
            if self.value(&pair[0], now).is_some() {
                return Resp::Integer(0);
            }
        }
        for pair in args[1..].as_chunks::<2>().0 {
            self.store(&pair[0], StringWrite::Copy(&pair[1]), None, now);
        }
        Resp::Integer(1)
    }

    fn bitmap_getbit(&mut self, args: &[Bytes], now: i64) -> Resp {
        let offset = match bit_offset(&args[2], 1, false) {
            Ok(n) => n,
            Err(e) => return e,
        };
        match self.value(&args[1], now) {
            Some(Value::String(value)) => Resp::Integer(read_bit(value.bytes(), offset) as i64),
            Some(_) => wrong_type(),
            None => Resp::Integer(0),
        }
    }

    fn bitmap_setbit(&mut self, args: &[Bytes], now: i64) -> Resp {
        let offset = match bit_offset(&args[2], 1, false) {
            Ok(n) => n,
            Err(e) => return e,
        };
        let Some(bit) = integer(&args[3]).filter(|n| *n == 0 || *n == 1) else {
            return error("ERR bit is not an integer or out of range");
        };
        let (previous, length) = match self.value(&args[1], now) {
            Some(Value::String(value)) => (read_bit(value.bytes(), offset), value.bytes().len()),
            Some(_) => return wrong_type(),
            None => (0, 0),
        };
        let extent = offset / 8 + 1;
        let (mut value, deadline) = self.string_buffer(&args[1], extent.saturating_sub(length));
        value.resize(length.max(extent), 0);
        write_bit(&mut value, offset, bit as u8);
        self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        Resp::Integer(previous as i64)
    }

    fn bitmap_count(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (start, end, bits) = match parse_count_range(args) {
            Ok(r) => r,
            Err(e) => return e,
        };
        let value = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes(),
            Some(_) => return wrong_type(),
            None => return Resp::Integer(0),
        };
        if start < 0 && end < 0 && start > end {
            return Resp::Integer(0);
        }
        let Some((start, end)) = bitmap_range(start, end, value.len(), bits) else {
            return Resp::Integer(0);
        };
        let mut count = 0_i64;
        for index in start / 8..=end / 8 {
            count += (value[index] & byte_mask(index, start, end)).count_ones() as i64;
        }
        Resp::Integer(count)
    }

    fn bitmap_position(&mut self, args: &[Bytes], now: i64) -> Resp {
        if !(3..=6).contains(&args.len()) {
            return syntax_error();
        }
        let Some(bit) = integer(&args[2]) else {
            return integer_error();
        };
        if bit != 0 && bit != 1 {
            return error("ERR The bit argument must be 1 or 0.");
        }
        let start = if args.len() >= 4 {
            match integer(&args[3]) {
                Some(n) => n,
                None => return integer_error(),
            }
        } else {
            0
        };
        let end = if args.len() >= 5 {
            match integer(&args[4]) {
                Some(n) => n,
                None => return integer_error(),
            }
        } else {
            -1
        };
        let bits = if args.len() == 6 {
            match range_unit(&args[5]) {
                Ok(b) => b,
                Err(e) => return e,
            }
        } else {
            false
        };
        let value = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes(),
            Some(_) => return wrong_type(),
            None => return Resp::Integer(if bit == 0 { 0 } else { -1 }),
        };
        let Some((start, end)) = bitmap_range(start, end, value.len(), bits) else {
            return Resp::Integer(-1);
        };
        for index in start / 8..=end / 8 {
            let candidate = (if bit == 1 {
                value[index]
            } else {
                !value[index]
            }) & byte_mask(index, start, end);
            if candidate != 0 {
                return Resp::Integer((index * 8 + candidate.leading_zeros() as usize) as i64);
            }
        }
        Resp::Integer(if bit == 0 && args.len() < 5 {
            (value.len() * 8) as i64
        } else {
            -1
        })
    }

    fn bitmap_operation(&mut self, args: &[Bytes], now: i64) -> Resp {
        let operation = if args[1].eq_ignore_ascii_case(b"AND") {
            0
        } else if args[1].eq_ignore_ascii_case(b"OR") {
            1
        } else if args[1].eq_ignore_ascii_case(b"XOR") {
            2
        } else if args[1].eq_ignore_ascii_case(b"NOT") {
            3
        } else if args[1].eq_ignore_ascii_case(b"DIFF") {
            4
        } else if args[1].eq_ignore_ascii_case(b"DIFF1") {
            5
        } else if args[1].eq_ignore_ascii_case(b"ANDOR") {
            6
        } else if args[1].eq_ignore_ascii_case(b"ONE") {
            7
        } else {
            return syntax_error();
        };
        if operation == 3 && args.len() != 4 {
            return error("ERR BITOP NOT must be called with a single source key.");
        }
        let mut sources = Vec::with_capacity(args.len() - 3);
        let mut length = 0;
        for key in &args[3..] {
            match self.value(key, now) {
                Some(Value::String(value)) => {
                    length = length.max(value.bytes().len());
                    sources.push(value.bytes().clone());
                }
                Some(_) => return wrong_type(),
                None => sources.push(Bytes::new()),
            }
        }
        if length > MAX_STRING {
            return size_error();
        }
        if length == 0 {
            self.discard_expired(&args[2], now);
            if self.remove(&args[2]).is_some() {
                self.record(now, || Mutation::Delete {
                    key: args[2].clone(),
                });
            }
            return Resp::Integer(0);
        }
        let mut result = StringBuffer::zeroed(length);
        for (index, target) in result.iter_mut().enumerate() {
            *target = match operation {
                0 => sources
                    .iter()
                    .fold(255, |n, source| n & source.get(index).copied().unwrap_or(0)),
                1 => sources
                    .iter()
                    .fold(0, |n, source| n | source.get(index).copied().unwrap_or(0)),
                2 => sources
                    .iter()
                    .fold(0, |n, source| n ^ source.get(index).copied().unwrap_or(0)),
                3 => !sources[0][index],
                4..=6 => {
                    let first = sources[0].get(index).copied().unwrap_or(0);
                    let rest = sources[1..]
                        .iter()
                        .fold(0, |n, source| n | source.get(index).copied().unwrap_or(0));
                    match operation {
                        4 => first & !rest,
                        5 => !first & rest,
                        _ => first & rest,
                    }
                }
                _ => {
                    // Track bits seen at least once and at least twice. XOR
                    // alone is not ONE: three set inputs must produce zero.
                    let (mut seen, mut multiple) = (0u8, 0u8);
                    for source in &sources {
                        let byte = source.get(index).copied().unwrap_or(0);
                        multiple |= seen & byte;
                        seen |= byte;
                    }
                    seen & !multiple
                }
            };
        }
        self.store(&args[2], StringWrite::Buffer(result), None, now);
        Resp::Integer(length as i64)
    }

    fn bitmap_field(&mut self, args: &[Bytes], now: i64) -> Resp {
        let readonly = args[0].eq_ignore_ascii_case(b"BITFIELD_RO");
        let operations = match parse_fields(args, readonly) {
            Ok(ops) => ops,
            Err(e) => return e,
        };
        if let Err(error) = super::ReplyBudget::new(self.reply_limit).charge(operations.len(), 0) {
            return error;
        }
        let length = match self.value(&args[1], now) {
            Some(Value::String(value)) => value.bytes().len(),
            Some(_) => return wrong_type(),
            None => 0,
        };
        if operations
            .iter()
            .all(|op| matches!(op.kind, FieldKind::Get))
        {
            let value = match self.entries.get(&args[1]).map(|entry| &entry.value) {
                Some(Value::String(value)) => value.bytes().as_ref(),
                _ => &[],
            };
            return array(
                operations
                    .iter()
                    .map(|op| Resp::Integer(read_field(value, *op)))
                    .collect(),
            );
        }
        let extent = operations
            .iter()
            .filter(|op| !matches!(op.kind, FieldKind::Get))
            .map(|op| (op.offset + op.width).div_ceil(8))
            .max()
            .unwrap_or(0);
        let existed = self.entries.contains_key(&args[1]);
        let (mut value, deadline) = self.string_buffer(&args[1], extent.saturating_sub(length));
        let initial_len = value.len();
        let mut changed = false;
        let mut replies = Vec::with_capacity(operations.len());
        for op in operations {
            // Redis creates/extends a writable BITFIELD's string before
            // overflow is evaluated, even when OVERFLOW FAIL rejects the value.
            if !matches!(op.kind, FieldKind::Get) {
                value.resize(value.len().max((op.offset + op.width).div_ceil(8)), 0);
            }
            let previous = read_field(&value, op);
            let desired = match op.kind {
                FieldKind::Get => {
                    replies.push(Resp::Integer(previous));
                    continue;
                }
                FieldKind::Set(n) => n as i128,
                FieldKind::Increment(n) => previous as i128 + n as i128,
            };
            let Some(next) = overflow_value(desired, op) else {
                replies.push(Resp::Bulk(None));
                continue;
            };
            write_field(&mut value, op, next);
            changed = true;
            replies.push(Resp::Integer(if matches!(op.kind, FieldKind::Set(_)) {
                previous
            } else {
                next
            }));
        }
        if changed {
            self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        } else if value.len() != initial_len {
            let capture = self.capturing_changes();
            // Failed operations expand storage but Redis does not signal WATCH.
            if let Some(value) =
                self.store_value(&args[1], StringWrite::Buffer(value), deadline, capture)
            {
                self.record_change(now, false, || Mutation::Set {
                    key: args[1].clone(),
                    value,
                    expires_at: deadline,
                });
            }
        } else if existed {
            // No suboperation succeeded: restore ownership without journaling.
            self.store_value(&args[1], StringWrite::Buffer(value), deadline, false);
        }
        array(replies)
    }
}

fn range_unit(token: &[u8]) -> Result<bool, Resp> {
    if token.eq_ignore_ascii_case(b"BIT") {
        Ok(true)
    } else if token.eq_ignore_ascii_case(b"BYTE") {
        Ok(false)
    } else {
        Err(syntax_error())
    }
}

fn parse_count_range(args: &[Bytes]) -> Result<(i64, i64, bool), Resp> {
    if args.len() == 2 {
        return Ok((0, -1, false));
    }
    if args.len() != 4 && args.len() != 5 {
        return Err(syntax_error());
    }
    let start = integer(&args[2]).ok_or_else(integer_error)?;
    let end = integer(&args[3]).ok_or_else(integer_error)?;
    let bits = if args.len() == 5 {
        range_unit(&args[4])?
    } else {
        false
    };
    Ok((start, end, bits))
}

fn bitmap_range(start: i64, end: i64, length: usize, bits: bool) -> Option<(usize, usize)> {
    let (start, end) = range(start, end, if bits { length * 8 } else { length })?;
    Some(if bits {
        (start, end)
    } else {
        (start * 8, end * 8 + 7)
    })
}

fn byte_mask(index: usize, start: usize, end: usize) -> u8 {
    let left = if index == start / 8 {
        255_u8 >> (start % 8)
    } else {
        255
    };
    let right = if index == end / 8 {
        255_u8 << (7 - end % 8)
    } else {
        255
    };
    left & right
}

fn read_bit(value: &[u8], offset: usize) -> u8 {
    value
        .get(offset / 8)
        .map_or(0, |byte| (byte >> (7 - offset % 8)) & 1)
}

fn write_bit(value: &mut [u8], offset: usize, bit: u8) {
    let mask = 1 << (7 - offset % 8);
    value[offset / 8] = (value[offset / 8] & !mask) | (bit << (7 - offset % 8));
}

#[derive(Clone, Copy)]
enum Overflow {
    Wrap,
    Saturate,
    Fail,
}
#[derive(Clone, Copy)]
enum FieldKind {
    Get,
    Set(i64),
    Increment(i64),
}
#[derive(Clone, Copy)]
struct FieldOp {
    signed: bool,
    width: usize,
    offset: usize,
    overflow: Overflow,
    kind: FieldKind,
}

fn parse_fields(args: &[Bytes], readonly: bool) -> Result<Vec<FieldOp>, Resp> {
    let mut result = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut index = 2;
    while index < args.len() {
        let token = &args[index];
        if token.eq_ignore_ascii_case(b"OVERFLOW") && index + 1 < args.len() {
            overflow = if args[index + 1].eq_ignore_ascii_case(b"WRAP") {
                Overflow::Wrap
            } else if args[index + 1].eq_ignore_ascii_case(b"SAT") {
                Overflow::Saturate
            } else if args[index + 1].eq_ignore_ascii_case(b"FAIL") {
                Overflow::Fail
            } else {
                return Err(error("ERR Invalid OVERFLOW type specified"));
            };
            index += 2;
            continue;
        }
        let get = token.eq_ignore_ascii_case(b"GET");
        let set = token.eq_ignore_ascii_case(b"SET");
        let increment = token.eq_ignore_ascii_case(b"INCRBY");
        if readonly && (set || increment) {
            return Err(error("ERR BITFIELD_RO only supports the GET subcommand"));
        }
        if (!get && !set && !increment) || index + (if get { 2 } else { 3 }) >= args.len() {
            return Err(syntax_error());
        }
        let encoding = &args[index + 1];
        let signed = encoding.first() == Some(&b'i');
        let width =
            integer(encoding.get(1..).unwrap_or_default()).and_then(|n| usize::try_from(n).ok());
        let Some(width) = width.filter(|n| *n >= 1 && *n <= if signed { 64 } else { 63 }) else {
            return Err(error(
                "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
            ));
        };
        if !signed && encoding.first() != Some(&b'u') {
            return Err(error(
                "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is.",
            ));
        }
        let offset = bit_offset(&args[index + 2], width, true)?;
        let kind = if get {
            FieldKind::Get
        } else {
            let value = integer(&args[index + 3]).ok_or_else(integer_error)?;
            if set {
                FieldKind::Set(value)
            } else {
                FieldKind::Increment(value)
            }
        };
        result.push(FieldOp {
            signed,
            width,
            offset,
            overflow,
            kind,
        });
        index += if get { 3 } else { 4 };
    }
    Ok(result)
}

fn read_field(value: &[u8], op: FieldOp) -> i64 {
    let mut raw = 0_u64;
    for offset in op.offset..op.offset + op.width {
        raw = (raw << 1) | read_bit(value, offset) as u64;
    }
    if op.signed && op.width < 64 && raw & (1_u64 << (op.width - 1)) != 0 {
        (raw | (!0_u64 << op.width)) as i64
    } else {
        raw as i64
    }
}

fn write_field(value: &mut [u8], op: FieldOp, number: i64) {
    for bit in 0..op.width {
        write_bit(
            value,
            op.offset + bit,
            ((number as u64 >> (op.width - 1 - bit)) & 1) as u8,
        );
    }
}

fn overflow_value(value: i128, op: FieldOp) -> Option<i64> {
    let (minimum, maximum) = if op.signed {
        (-(1_i128 << (op.width - 1)), (1_i128 << (op.width - 1)) - 1)
    } else {
        (0, (1_i128 << op.width) - 1)
    };
    if value >= minimum && value <= maximum {
        return Some(value as i64);
    }
    match op.overflow {
        Overflow::Fail => None,
        Overflow::Saturate => Some(value.clamp(minimum, maximum) as i64),
        Overflow::Wrap => {
            let raw = value.rem_euclid(1_i128 << op.width);
            Some(if op.signed && raw > maximum {
                (raw - (1_i128 << op.width)) as i64
            } else {
                raw as i64
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, args: &[&str], now: i64) -> Resp {
        let args: Vec<_> = args
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect();
        engine.execute_at(&args, now)
    }

    fn fail(reply: Resp) {
        assert!(matches!(reply, Resp::Error(_)), "{reply:?}");
    }

    #[test]
    fn replacement_and_in_place_ttl_semantics() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["SETEX", "k", "3", "abc"], 1000),
            Resp::Simple("OK")
        );
        assert_eq!(
            run(&mut engine, &["GETEX", "k"], 1100),
            Resp::Bulk(Some(Bytes::from_static(b"abc")))
        );
        assert_eq!(
            run(&mut engine, &["APPEND", "k", "d"], 1100),
            Resp::Integer(4)
        );
        assert_eq!(
            run(&mut engine, &["SETRANGE", "k", "1", "Z"], 1100),
            Resp::Integer(4)
        );
        assert_eq!(run(&mut engine, &["PTTL", "k"], 1100), Resp::Integer(2900));
        assert_eq!(
            run(&mut engine, &["GETSET", "k", "new"], 1100),
            Resp::Bulk(Some(Bytes::from_static(b"aZcd")))
        );
        assert_eq!(run(&mut engine, &["PTTL", "k"], 1100), Resp::Integer(-1));
        assert_eq!(
            run(&mut engine, &["GETEX", "k", "PXAT", "1000"], 1100),
            Resp::Bulk(Some(Bytes::from_static(b"new")))
        );
        assert_eq!(run(&mut engine, &["EXISTS", "k"], 1100), Resp::Integer(0));
        assert_eq!(
            run(
                &mut engine,
                &["SETRANGE", "absent", "9223372036854775807", ""],
                1100
            ),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, &["EXISTS", "absent"], 1100),
            Resp::Integer(0)
        );
    }

    #[test]
    fn atomic_msetnx_and_whole_request_validation() {
        let mut engine = Engine::new();
        run(&mut engine, &["SET", "exists", "old"], 0);
        assert_eq!(
            run(&mut engine, &["MSETNX", "new", "x", "exists", "y"], 0),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, &["GET", "new"], 0), Resp::Bulk(None));
        assert_eq!(
            run(&mut engine, &["MSETNX", "new", "first", "new", "last"], 0),
            Resp::Integer(1)
        );
        assert_eq!(
            run(&mut engine, &["GET", "new"], 0),
            Resp::Bulk(Some(Bytes::from_static(b"last")))
        );
        fail(run(
            &mut engine,
            &[
                "BITFIELD", "new", "SET", "u8", "0", "1", "SET", "u64", "8", "1",
            ],
            0,
        ));
        assert_eq!(
            run(&mut engine, &["GET", "new"], 0),
            Resp::Bulk(Some(Bytes::from_static(b"last")))
        );
        run(&mut engine, &["LPUSH", "list", "x"], 0);
        fail(run(&mut engine, &["BITOP", "OR", "new", "new", "list"], 0));
        assert_eq!(
            run(&mut engine, &["GET", "new"], 0),
            Resp::Bulk(Some(Bytes::from_static(b"last")))
        );
    }

    #[test]
    fn bitfield_signed_unsigned_boundaries_and_individual_failure() {
        let mut engine = Engine::new();
        assert_eq!(
            run(
                &mut engine,
                &[
                    "BITFIELD",
                    "k",
                    "SET",
                    "i64",
                    "0",
                    "9223372036854775807",
                    "INCRBY",
                    "i64",
                    "0",
                    "1",
                    "GET",
                    "i64",
                    "0"
                ],
                0
            ),
            array(vec![
                Resp::Integer(0),
                Resp::Integer(i64::MIN),
                Resp::Integer(i64::MIN)
            ])
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "BITFIELD", "k", "OVERFLOW", "FAIL", "INCRBY", "i64", "0", "-1", "GET", "i64",
                    "0", "OVERFLOW", "SAT", "INCRBY", "i64", "0", "-1"
                ],
                0
            ),
            array(vec![
                Resp::Bulk(None),
                Resp::Integer(i64::MIN),
                Resp::Integer(i64::MIN)
            ])
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "BITFIELD", "metrics", "OVERFLOW", "SAT", "INCRBY", "u16", "#0", "65540",
                    "INCRBY", "u16", "#1", "2", "GET", "u16", "#0"
                ],
                0
            ),
            array(vec![
                Resp::Integer(65535),
                Resp::Integer(2),
                Resp::Integer(65535)
            ])
        );
        assert_eq!(
            run(&mut engine, &["GET", "metrics"], 0),
            Resp::Bulk(Some(Bytes::from_static(b"\xff\xff\x00\x02")))
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "BITFIELD", "missing", "OVERFLOW", "FAIL", "SET", "u1", "0", "2", "GET", "u1",
                    "0"
                ],
                0
            ),
            array(vec![Resp::Bulk(None), Resp::Integer(0)])
        );
        assert_eq!(
            run(&mut engine, &["EXISTS", "missing"], 0),
            Resp::Integer(1)
        );
        fail(run(
            &mut engine,
            &["BITFIELD_RO", "metrics", "SET", "u1", "0", "1"],
            0,
        ));
    }

    #[test]
    fn bit_byte_ranges_and_integer_extremes() {
        let mut engine = Engine::new();
        engine.store(
            &Bytes::from_static(b"k"),
            StringWrite::Copy(&Bytes::from_static(b"\xff\x00\x81")),
            None,
            0,
        );
        assert_eq!(run(&mut engine, &["BITCOUNT", "k"], 0), Resp::Integer(10));
        assert_eq!(
            run(&mut engine, &["BITCOUNT", "k", "1", "16", "BIT"], 0),
            Resp::Integer(8)
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "BITCOUNT",
                    "k",
                    "-9223372036854775808",
                    "9223372036854775807",
                    "BIT"
                ],
                0
            ),
            Resp::Integer(10)
        );
        assert_eq!(
            run(
                &mut engine,
                &["BITCOUNT", "k", "0", "-9223372036854775808"],
                0
            ),
            Resp::Integer(8)
        );
        assert_eq!(run(&mut engine, &["BITPOS", "k", "0"], 0), Resp::Integer(8));
        assert_eq!(
            run(&mut engine, &["BITPOS", "k", "1", "8", "23", "BIT"], 0),
            Resp::Integer(16)
        );
        assert_eq!(
            run(&mut engine, &["GETBIT", "absent", "7"], 0),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, &["BITPOS", "absent", "0", "900"], 0),
            Resp::Integer(0)
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GETRANGE",
                    "k",
                    "-9223372036854775808",
                    "9223372036854775807"
                ],
                0
            ),
            Resp::Bulk(Some(Bytes::from_static(b"\xff\x00\x81")))
        );
    }

    #[test]
    fn limits_fail_before_growth_and_preserve_existing_bytes() {
        let mut engine = Engine::new();
        run(&mut engine, &["SET", "k", "old"], 0);
        for command in [
            vec!["SETBIT", "k", "67108864", "1"],
            vec!["SETRANGE", "k", "8388608", "x"],
            vec!["BITFIELD", "k", "SET", "i64", "67108801", "1"],
            vec!["BITFIELD", "k", "GET", "u8", "#9223372036854775807"],
        ] {
            fail(run(&mut engine, &command, 0));
            assert_eq!(
                run(&mut engine, &["GET", "k"], 0),
                Resp::Bulk(Some(Bytes::from_static(b"old")))
            );
        }
        let args = [
            Bytes::from_static(b"APPEND"),
            Bytes::from_static(b"k"),
            Bytes::from(vec![0; MAX_STRING]),
        ];
        fail(engine.execute_at(&args, 0));
        assert_eq!(run(&mut engine, &["STRLEN", "k"], 0), Resp::Integer(3));
    }

    #[test]
    fn resolved_bitmap_writes_replay_values_and_absolute_ttl() {
        let mut source = Engine::new();
        source.set_recording(true);
        let mut recovered = Engine::new();
        assert_eq!(
            run(&mut source, &["MSETNX", "a", "one", "b", "two"], 100),
            Resp::Integer(1)
        );
        let batch = source.take_batch().unwrap();
        recovered.replay(batch).unwrap();
        assert_eq!(
            run(&mut recovered, &["MGET", "a", "b"], 100),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"one"))),
                Resp::Bulk(Some(Bytes::from_static(b"two"))),
            ])
        );
        assert!(source.take_batch().is_none());
        run(&mut source, &["PSETEX", "bitmap", "5000", ""], 100);
        recovered.replay(source.take_batch().unwrap()).unwrap();
        assert_eq!(
            run(
                &mut source,
                &[
                    "BITFIELD", "bitmap", "OVERFLOW", "SAT", "INCRBY", "u16", "#0", "65540",
                    "INCRBY", "u16", "#2", "7"
                ],
                200
            ),
            array(vec![Resp::Integer(65535), Resp::Integer(7)])
        );
        let batch = source.take_batch().unwrap();
        recovered.replay(batch).unwrap();
        assert_eq!(
            run(&mut recovered, &["GET", "bitmap"], 300),
            Resp::Bulk(Some(Bytes::from_static(b"\xff\xff\x00\x00\x00\x07")))
        );
        assert_eq!(
            run(&mut recovered, &["PTTL", "bitmap"], 300),
            Resp::Integer(4800)
        );
        assert_eq!(
            run(
                &mut source,
                &[
                    "BITFIELD", "bitmap", "OVERFLOW", "FAIL", "INCRBY", "u16", "#0", "1"
                ],
                300
            ),
            array(vec![Resp::Bulk(None)])
        );
        assert!(source.take_batch().is_none());
        assert_eq!(
            run(
                &mut source,
                &["BITFIELD_RO", "bitmap", "GET", "u16", "#0"],
                300
            ),
            array(vec![Resp::Integer(65535)])
        );
        assert!(source.take_batch().is_none());
        assert_eq!(
            run(&mut source, &["GETDEL", "bitmap"], 300),
            Resp::Bulk(Some(Bytes::from_static(b"\xff\xff\x00\x00\x00\x07")))
        );
        let batch = source.take_batch().unwrap();
        recovered.replay(batch).unwrap();
        assert_eq!(
            run(&mut recovered, &["EXISTS", "bitmap"], 300),
            Resp::Integer(0)
        );
    }

    #[test]
    fn expiration_validation_precedence_and_no_partial_changes() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["GETEX", "missing", "EX", "invalid"], 100),
            Resp::Bulk(None)
        );
        run(&mut engine, &["LPUSH", "list", "x"], 100);
        let reply = run(&mut engine, &["GETEX", "list", "EX", "invalid"], 100);
        assert!(matches!(reply, Resp::Error(message) if message.starts_with("WRONGTYPE")));
        assert_eq!(
            run(&mut engine, &["PSETEX", "k", "1000", "old"], 100),
            Resp::Simple("OK")
        );
        for options in [
            vec!["GETEX", "k", "EX", "0"],
            vec!["GETEX", "k", "EX", "9223372036854775807"],
            vec!["GETEX", "k", "EX", "1", "PX", "1"],
            vec!["SETEX", "k", "-1", "changed"],
        ] {
            fail(run(&mut engine, &options, 200));
            assert_eq!(
                run(&mut engine, &["GET", "k"], 200),
                Resp::Bulk(Some(Bytes::from_static(b"old")))
            );
            assert_eq!(run(&mut engine, &["PTTL", "k"], 200), Resp::Integer(900));
        }
        assert_eq!(
            run(&mut engine, &["GETEX", "k", "PX", "10", "PX", "20"], 200),
            Resp::Bulk(Some(Bytes::from_static(b"old")))
        );
        assert_eq!(run(&mut engine, &["PTTL", "k"], 200), Resp::Integer(20));
        assert_eq!(
            run(&mut engine, &["GETEX", "k", "PERSIST", "PERSIST"], 200),
            Resp::Bulk(Some(Bytes::from_static(b"old")))
        );
        assert_eq!(run(&mut engine, &["PTTL", "k"], 200), Resp::Integer(-1));
        assert_eq!(
            run(&mut engine, &["GETRANGE", "k", "-5", "-6"], 200),
            Resp::Bulk(Some(Bytes::new()))
        );
        assert_eq!(
            run(&mut engine, &["BITCOUNT", "k", "-5", "-6"], 200),
            Resp::Integer(0)
        );
        assert_eq!(
            run(&mut engine, &["BITPOS", "k", "1", "-5", "-6"], 200),
            Resp::Integer(1)
        );
    }
    #[test]
    fn bitmap_new_operators_zero_padding_aliases_and_bit_boundaries() {
        let mut engine = Engine::new();
        for (key, bytes) in [
            ("a", vec![0b1110_0001, 0x80]),
            ("b", vec![0b1011_0001]),
            ("c", vec![0b1001_0000]),
        ] {
            let args = [
                Bytes::from_static(b"SET"),
                Bytes::from(key),
                Bytes::from(bytes),
            ];
            assert_eq!(engine.execute_at(&args, 0), Resp::Simple("OK"));
        }
        for (operation, expected) in [
            ("AND", [0x80, 0]),
            ("OR", [0xf1, 0x80]),
            ("XOR", [0xc0, 0x80]),
            ("DIFF", [0x40, 0x80]),
            ("DIFF1", [0x10, 0]),
            ("ANDOR", [0xa1, 0]),
            ("ONE", [0x40, 0x80]),
        ] {
            assert_eq!(
                run(&mut engine, &["BITOP", operation, "out", "a", "b", "c"], 0),
                Resp::Integer(2)
            );
            assert_eq!(
                run(&mut engine, &["GET", "out"], 0),
                Resp::Bulk(Some(Bytes::copy_from_slice(&expected)))
            );
        }
        assert_eq!(
            run(&mut engine, &["BITOP", "DIFF", "a", "a", "b"], 0),
            Resp::Integer(2)
        );
        assert_eq!(run(&mut engine, &["GETBIT", "a", "7"], 0), Resp::Integer(0));
        assert_eq!(run(&mut engine, &["GETBIT", "a", "8"], 0), Resp::Integer(1));
        assert_eq!(
            run(&mut engine, &["BITOP", "ONE", "out", "b", "b", "b"], 0),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["BITCOUNT", "out"], 0), Resp::Integer(0));
        run(&mut engine, &["SET", "out", "preserve"], 0);
        run(&mut engine, &["LPUSH", "bad", "x"], 0);
        fail(run(
            &mut engine,
            &["BITOP", "ONE", "out", "missing", "bad"],
            0,
        ));
        assert_eq!(
            run(&mut engine, &["GET", "out"], 0),
            Resp::Bulk(Some(Bytes::from_static(b"preserve")))
        );
        assert_eq!(
            run(&mut engine, &["BITOP", "DIFF1", "out", "missing"], 0),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, &["EXISTS", "out"], 0), Resp::Integer(0));
    }

    fn bulk_view(reply: Resp) -> Bytes {
        match reply {
            Resp::Bulk(Some(value)) => value,
            reply => panic!("{reply:?}"),
        }
    }

    #[test]
    fn string_writes_never_change_outstanding_views() {
        let mut engine = Engine::new();
        run(&mut engine, &["SET", "k", "abcd"], 0);
        let writes: [(&[&str], &str); 7] = [
            (&["APPEND", "k", "ef"], "abcdef"),
            (&["SETRANGE", "k", "0", "XY"], "XYcdef"),
            (&["SETBIT", "k", "7", "1"], "YYcdef"),
            (&["SET", "k", "zzzzzz"], "zzzzzz"),
            (&["SET", "k", "123456"], "123456"),
            (&["INCR", "k"], "123457"),
            (&["BITFIELD", "k", "SET", "u8", "0", "65"], "A23457"),
        ];
        for (write, after) in writes {
            let view = bulk_view(run(&mut engine, &["GET", "k"], 0));
            let range = bulk_view(run(&mut engine, &["GETRANGE", "k", "1", "2"], 0));
            let (before, before_range) = (view.to_vec(), range.to_vec());
            run(&mut engine, write, 0);
            assert_eq!(&view[..], &before[..], "{write:?}");
            assert_eq!(&range[..], &before_range[..], "{write:?}");
            assert_eq!(
                bulk_view(run(&mut engine, &["GET", "k"], 0)),
                Bytes::copy_from_slice(after.as_bytes())
            );
        }
        let previous = bulk_view(run(&mut engine, &["SET", "k", "abc", "GET"], 0));
        let unchanged = bulk_view(run(&mut engine, &["GETEX", "k", "PX", "1000"], 0));
        run(&mut engine, &["APPEND", "k", "d"], 0);
        assert_eq!(previous, Bytes::from_static(b"A23457"));
        assert_eq!(unchanged, Bytes::from_static(b"abc"));
        assert_eq!(run(&mut engine, &["PTTL", "k"], 0), Resp::Integer(1000));

        // Undo/snapshot rows and COPY targets keep their own bytes.
        run(&mut engine, &["SET", "n", "41"], 0);
        let snapshot = engine
            .snapshot_batches(0)
            .collect::<std::io::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(run(&mut engine, &["COPY", "k", "c"], 0), Resp::Integer(1));
        run(&mut engine, &["APPEND", "k", "X"], 0);
        run(&mut engine, &["APPEND", "c", "Y"], 0);
        run(&mut engine, &["INCR", "n"], 0);
        assert_eq!(
            bulk_view(run(&mut engine, &["GET", "k"], 0)),
            Bytes::from_static(b"abcdX")
        );
        assert_eq!(
            bulk_view(run(&mut engine, &["GET", "c"], 0)),
            Bytes::from_static(b"abcdY")
        );
        let mut restored = Engine::new();
        for batch in snapshot {
            restored.replay(batch).unwrap();
        }
        assert_eq!(
            run(&mut restored, &["MGET", "k", "n", "c"], 0),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"abcd"))),
                Resp::Bulk(Some(Bytes::from_static(b"41"))),
                Resp::Bulk(None),
            ])
        );
    }

    #[test]
    fn retained_journal_rows_keep_each_resolved_string_value() {
        let mut source = Engine::new();
        source.set_recording(true);
        let mut batches = Vec::new();
        for command in [
            &["SET", "k", "aaaa"][..],
            &["SET", "k", "bbbb"],
            &["APPEND", "k", "cc"],
            &["APPEND", "k", "dd"],
            &["SETRANGE", "k", "0", "Z"],
            &["SET", "n", "9"],
            &["INCR", "n"],
            &["INCR", "n"],
            &["INCRBY", "n", "-2"],
            &["SET", "m", "-9223372036854775807"],
            &["DECR", "m"],
            &["DECR", "m"],
            &["GETEX", "k", "PX", "5000"],
            &["PFADD", "h", "a", "b", "c"],
            &["PFCOUNT", "h"],
        ] {
            run(&mut source, command, 100);
            // Retained batches model journal rows not yet persisted.
            batches.extend(source.take_batch());
        }
        let rows: Vec<_> = batches
            .iter()
            .flat_map(|batch| &batch.mutations)
            .filter_map(|mutation| match mutation {
                Mutation::Set {
                    key,
                    value,
                    expires_at,
                } => Some((key.clone(), value.clone(), *expires_at)),
                _ => None,
            })
            .collect();
        let (hll, strings): (Vec<_>, Vec<_>) = rows
            .into_iter()
            .partition(|(key, _, _)| key.as_ref() == b"h");
        let row = |key: &'static str, value: &'static str, ttl: Option<i64>| {
            (
                Bytes::from_static(key.as_bytes()),
                Bytes::from_static(value.as_bytes()),
                ttl,
            )
        };
        assert_eq!(
            strings,
            vec![
                row("k", "aaaa", None),
                row("k", "bbbb", None),
                row("k", "bbbbcc", None),
                row("k", "bbbbccdd", None),
                row("k", "Zbbbccdd", None),
                row("n", "9", None),
                row("n", "10", None),
                row("n", "11", None),
                row("n", "9", None),
                row("m", "-9223372036854775807", None),
                row("m", "-9223372036854775808", None),
                row("k", "Zbbbccdd", Some(5100)),
            ]
        );
        // The cache refresh rewrites a copy; PFADD's row keeps its stale flag.
        assert_eq!(hll.len(), 2);
        assert_ne!(hll[0].1[15] & 128, 0);
        assert_eq!(hll[1].1[15] & 128, 0);
        let mut recovered = Engine::new();
        for batch in batches {
            recovered.replay(batch).unwrap();
        }
        assert_eq!(
            run(&mut recovered, &["MGET", "k", "n", "m"], 100),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"Zbbbccdd"))),
                Resp::Bulk(Some(Bytes::from_static(b"9"))),
                Resp::Bulk(Some(Bytes::from_static(b"-9223372036854775808"))),
            ])
        );
        assert_eq!(
            run(&mut recovered, &["PTTL", "k"], 100),
            Resp::Integer(5000)
        );
        let count = run(&mut source, &["PFCOUNT", "h"], 100);
        assert!(source.take_batch().is_none());
        assert_eq!(run(&mut recovered, &["PFCOUNT", "h"], 100), count);
    }

    #[test]
    fn increments_store_and_journal_canonical_spellings_at_every_width() {
        let mut engine = Engine::new();
        engine.set_recording(true);
        let mut targets = vec![i64::MIN, i64::MAX, 0];
        let mut power = 1_i64;
        loop {
            targets.extend([power - 1, power, power + 1, -power, -(power + 1)]);
            match power.checked_mul(10) {
                Some(next) => power = next,
                None => break,
            }
        }
        for target in targets {
            // One step from a neighbour, so every width change and sign
            // change rewrites the stored value in place.
            let (start, command) = match target.checked_sub(1) {
                Some(previous) => (previous, "INCR"),
                None => (target + 1, "DECR"),
            };
            run(&mut engine, &["SET", "k", &start.to_string()], 0);
            engine.take_batch();
            assert_eq!(run(&mut engine, &[command, "k"], 0), Resp::Integer(target));
            let spelling = Bytes::from(target.to_string());
            let rows: Vec<_> = engine
                .take_batch()
                .into_iter()
                .flat_map(|batch| batch.mutations)
                .filter_map(|mutation| match mutation {
                    Mutation::Set { value, .. } => Some(value),
                    _ => None,
                })
                .collect();
            assert_eq!(rows, vec![spelling.clone()], "{target}");
            assert_eq!(
                run(&mut engine, &["GET", "k"], 0),
                Resp::Bulk(Some(spelling.clone()))
            );
            assert_eq!(
                run(&mut engine, &["STRLEN", "k"], 0),
                Resp::Integer(spelling.len() as i64)
            );
        }
        // Overflow fails without rewriting the stored spelling.
        run(&mut engine, &["SET", "k", &i64::MAX.to_string()], 0);
        engine.take_batch();
        fail(run(&mut engine, &["INCR", "k"], 0));
        assert_eq!(
            run(&mut engine, &["GET", "k"], 0),
            Resp::Bulk(Some(Bytes::from(i64::MAX.to_string())))
        );
    }
}
