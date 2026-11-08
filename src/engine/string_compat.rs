use super::{
    CommandSpec, Engine, ExpiryKind, Mutation, Resp, StringWrite, Value, array, error, integer,
    integer_error, invalid_expiry, syntax_error, wrong_type,
};
use bytes::Bytes;
use num_bigint::BigUint;
use rustc_apfloat::{Float, Round, Status};

const MAX_STRING: usize = 8 * 1024 * 1024;
// Both work and allocation are bounded before constructing the DP table.
const MAX_LCS_CELLS: usize = 4 * 1024 * 1024;
pub(super) type RedisFloat = rustc_apfloat::ieee::X87DoubleExtended;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "delex",
        -2,
        &["write", "fast"],
        (1, 1, 1),
        Engine::string_delex,
    ),
    CommandSpec::handler(
        "digest",
        2,
        &["readonly", "fast"],
        (1, 1, 1),
        Engine::string_digest,
    ),
    CommandSpec::handler(
        "incrbyfloat",
        3,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::string_incrbyfloat,
    ),
    CommandSpec::handler(
        "increx",
        -2,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::string_increx,
    ),
    CommandSpec::handler("lcs", -3, &["readonly"], (1, 2, 1), Engine::string_lcs),
    CommandSpec::handler(
        "msetex",
        -4,
        &["write", "denyoom", "movablekeys"],
        (0, 0, 0),
        Engine::string_msetex,
    ),
    CommandSpec::handler("substr", 4, &["readonly"], (1, 1, 1), Engine::string_substr),
];

fn float_error() -> Resp {
    error("ERR value is not a valid float")
}

// Validate the complete C numeric grammar before the software float parser.
// In particular, Rust's parser accepts neither Redis's hex floats nor its
// extended precision. The chosen primary numeric baseline is Linux x87
// long double (64 significant bits, 15 exponent bits), not Darwin binary64.
pub(super) fn parse_float(raw: &[u8]) -> Result<RedisFloat, Resp> {
    let text = std::str::from_utf8(raw).map_err(|_| float_error())?;
    if raw.is_empty()
        || raw.len() > MAX_STRING
        || raw.iter().any(|b| b.is_ascii_whitespace() || *b == 0)
    {
        return Err(float_error());
    }
    let unsigned = text.strip_prefix(['+', '-']).unwrap_or(text);
    let hex = unsigned.starts_with("0x") || unsigned.starts_with("0X");
    let body = if hex { &unsigned[2..] } else { unsigned };
    let mut digits = 0_i64;
    let mut fractional = 0_i64;
    let mut dot = false;
    let mut exponent_at = None;
    let mut significant = Vec::with_capacity(body.len().min(6001));
    let mut discarded = 0_i64;
    let mut sticky = false;
    for (i, byte) in body.bytes().enumerate() {
        if byte == b'.' && !dot {
            dot = true;
        } else if (hex && matches!(byte, b'p' | b'P')) || (!hex && matches!(byte, b'e' | b'E')) {
            exponent_at = Some(i);
            break;
        } else if if hex {
            byte.is_ascii_hexdigit()
        } else {
            byte.is_ascii_digit()
        } {
            digits += 1;
            if dot {
                fractional += 1;
            }
            if byte != b'0' || !significant.is_empty() {
                if significant.len() < 6000 {
                    significant.push(byte);
                } else {
                    discarded += 1;
                    sticky |= byte != b'0';
                }
            }
        } else {
            return Err(float_error());
        }
    }
    if digits == 0 {
        return Err(float_error());
    }
    let mut exponent = 0_i64;
    if let Some(i) = exponent_at {
        let raw_exp = &body[i + 1..];
        let exp = raw_exp.strip_prefix(['+', '-']).unwrap_or(raw_exp);
        if exp.is_empty() || !exp.bytes().all(|b| b.is_ascii_digit()) {
            return Err(float_error());
        }
        // Saturation bounds work even for megabytes of zero-padded exponents;
        // no arbitrary-size exponent or input-sized bignum is constructed.
        for b in exp.bytes() {
            exponent = (exponent * 10 + i64::from(b - b'0')).min(100_000_000);
        }
        if raw_exp.starts_with('-') {
            exponent = -exponent;
        }
    }
    if significant.is_empty() {
        return Ok(RedisFloat::from_bits(0));
    }
    // 6000 retained significant digits exceeds the entire x87 exponent range
    // plus precision. A final sticky digit preserves midpoint rounding.
    if sticky {
        significant.push(b'1');
        discarded -= 1;
    }
    let radix_shift = if hex { 4 } else { 1 };
    exponent += (discarded - fractional) * radix_shift;
    let magnitude = exponent + radix_shift * significant.len() as i64;
    if !(-20_000..=20_000).contains(&magnitude) || (!hex && !(-6000..=5000).contains(&magnitude)) {
        return Err(float_error());
    }
    let mut normalized = String::with_capacity(significant.len() + 32);
    if text.starts_with('-') {
        normalized.push('-');
    }
    if hex {
        normalized.push_str("0x");
    }
    normalized.push_str(std::str::from_utf8(&significant).expect("ASCII numeric digits"));
    normalized.push(if hex { 'p' } else { 'e' });
    normalized.push_str(&exponent.to_string());
    let parsed =
        RedisFloat::from_str_r(&normalized, Round::NearestTiesToEven).map_err(|_| float_error())?;
    if !parsed.value.is_finite()
        || parsed
            .status
            .intersects(Status::UNDERFLOW | Status::OVERFLOW)
    {
        return Err(float_error());
    }
    Ok(parsed.value)
}

pub(super) fn add_float(left: RedisFloat, right: RedisFloat) -> Result<RedisFloat, Resp> {
    let result = left.add_r(right, Round::NearestTiesToEven).value;
    if !result.is_finite() {
        return Err(error("ERR increment would produce NaN or Infinity"));
    }
    Ok(result)
}

// Redis's human long-double encoding is fixed-point with 17 fractional
// decimal places, trimmed, with negative zero normalized. Round the exact
// binary significand rather than formatting an already-rounded f64.
pub(super) fn format_float(value: RedisFloat) -> Bytes {
    let bits = value.to_bits();
    let exponent = ((bits >> 64) & 0x7fff) as i32;
    let shift = exponent.max(1) - 16383 - 63 + 17;
    let mut scaled = BigUint::from(bits as u64) * BigUint::from(5_u8).pow(17);
    if shift >= 0 {
        scaled <<= shift as usize;
    } else {
        let count = (-shift) as usize;
        let quotient = &scaled >> count;
        let remainder = &scaled - (&quotient << count);
        let half = BigUint::from(1_u8) << (count - 1);
        let up = remainder > half || (remainder == half && quotient.bit(0));
        scaled = quotient;
        if up {
            scaled += 1_u8;
        }
    }
    if scaled == BigUint::from(0_u8) {
        return Bytes::from_static(b"0");
    }
    let digits = scaled.to_str_radix(10);
    let mut out = String::with_capacity(digits.len() + 20);
    if value.is_negative() {
        out.push('-');
    }
    if digits.len() <= 17 {
        out.push_str("0.");
        for _ in digits.len()..17 {
            out.push('0');
        }
        out.push_str(&digits);
    } else {
        out.push_str(&digits[..digits.len() - 17]);
        out.push('.');
        out.push_str(&digits[digits.len() - 17..]);
    }
    while out.ends_with('0') {
        out.pop();
    }
    if out.ends_with('.') {
        out.pop();
    }
    Bytes::from(out)
}

#[derive(Clone, Copy)]
pub(super) enum SetCondition<'a> {
    Always,
    Nx,
    Xx,
    Eq(&'a Bytes),
    Ne(&'a Bytes),
    DigestEq(&'a Bytes),
    DigestNe(&'a Bytes),
}

fn digest_argument(raw: &[u8]) -> Result<u64, Resp> {
    if raw.len() != 16 || !raw.iter().all(u8::is_ascii_hexdigit) {
        return Err(error(
            "ERR digest must be exactly 16 hexadecimal characters",
        ));
    }
    u64::from_str_radix(std::str::from_utf8(raw).map_err(|_| syntax_error())?, 16)
        .map_err(|_| error("ERR invalid digest"))
}

impl<'a> SetCondition<'a> {
    fn parse(token: &[u8], arg: &'a Bytes) -> Result<Self, Resp> {
        if token.eq_ignore_ascii_case(b"IFEQ") {
            Ok(Self::Eq(arg))
        } else if token.eq_ignore_ascii_case(b"IFNE") {
            Ok(Self::Ne(arg))
        } else if token.eq_ignore_ascii_case(b"IFDEQ") {
            Ok(Self::DigestEq(arg))
        } else if token.eq_ignore_ascii_case(b"IFDNE") {
            Ok(Self::DigestNe(arg))
        } else {
            Err(syntax_error())
        }
    }
    pub(super) fn matches(self, existing: Option<&Value>) -> Result<bool, Resp> {
        match self {
            Self::Always => return Ok(true),
            Self::Nx => return Ok(existing.is_none()),
            Self::Xx => return Ok(existing.is_some()),
            _ => {}
        }
        let current = match existing {
            Some(Value::String(s)) => Some(s.bytes()),
            Some(_) => return Err(wrong_type()),
            None => None,
        };
        Ok(match self {
            Self::Eq(expected) => current == Some(expected),
            Self::Ne(expected) => current != Some(expected),
            Self::DigestEq(expected) => match current {
                Some(s) => xxhash_rust::xxh3::xxh3_64(s) == digest_argument(expected)?,
                None => false,
            },
            Self::DigestNe(expected) => match current {
                Some(s) => xxhash_rust::xxh3::xxh3_64(s) != digest_argument(expected)?,
                None => true,
            },
            _ => unreachable!(),
        })
    }
}

#[derive(Clone, Copy)]
enum Expiration {
    Unspecified,
    Keep,
    Deadline(i64),
    Persist,
}
fn expiry_kind(raw: &[u8]) -> Option<ExpiryKind> {
    if raw.eq_ignore_ascii_case(b"EX") {
        Some(ExpiryKind::Ex)
    } else if raw.eq_ignore_ascii_case(b"PX") {
        Some(ExpiryKind::Px)
    } else if raw.eq_ignore_ascii_case(b"EXAT") {
        Some(ExpiryKind::Exat)
    } else if raw.eq_ignore_ascii_case(b"PXAT") {
        Some(ExpiryKind::Pxat)
    } else {
        None
    }
}
fn deadline(kind: ExpiryKind, raw: &[u8], now: i64, command: &'static str) -> Result<i64, Resp> {
    let number = integer(raw).ok_or_else(integer_error)?;
    kind.deadline(number, now)
        .filter(|_| number > 0)
        .ok_or_else(|| invalid_expiry(command))
}

pub(super) struct SetOptions<'a> {
    pub(super) condition: SetCondition<'a>,
    pub(super) get: bool,
    pub(super) keep_ttl: bool,
    pub(super) deadline: Option<i64>,
}
impl<'a> SetOptions<'a> {
    pub(super) fn parse(args: &'a [Bytes], now: i64) -> Result<Self, Resp> {
        let mut result = Self {
            condition: SetCondition::Always,
            get: false,
            keep_ttl: false,
            deadline: None,
        };
        let mut condition_kind: Option<&[u8]> = None;
        let mut expiry = None;
        let mut i = 3;
        while i < args.len() {
            let token = &args[i];
            if token.eq_ignore_ascii_case(b"GET") {
                result.get = true;
            } else if token.eq_ignore_ascii_case(b"KEEPTTL") && expiry.is_none() {
                result.keep_ttl = true;
            } else if let Some(kind) = expiry_kind(token) {
                if result.keep_ttl || expiry.is_some_and(|(previous, _)| previous != kind) {
                    return Err(syntax_error());
                }
                i += 1;
                expiry = Some((kind, args.get(i).ok_or_else(syntax_error)?));
            } else {
                if condition_kind.is_some_and(|previous| !previous.eq_ignore_ascii_case(token)) {
                    return Err(syntax_error());
                }
                result.condition = if token.eq_ignore_ascii_case(b"NX") {
                    SetCondition::Nx
                } else if token.eq_ignore_ascii_case(b"XX") {
                    SetCondition::Xx
                } else {
                    i += 1;
                    SetCondition::parse(token, args.get(i).ok_or_else(syntax_error)?)?
                };
                condition_kind = Some(token);
            }
            i += 1;
        }
        if let Some((kind, raw)) = expiry {
            result.deadline = Some(deadline(kind, raw, now, "set")?);
        }
        Ok(result)
    }
}

#[derive(Clone, Copy)]
enum Existence {
    Any,
    Nx,
    Xx,
}
struct MsetOptions {
    count: usize,
    end: usize,
    condition: Existence,
    expiry: Expiration,
}
impl MsetOptions {
    fn parse(args: &[Bytes], now: i64) -> Result<Self, Resp> {
        let count = args
            .get(1)
            .and_then(|n| integer(n))
            .filter(|n| *n > 0 && *n <= i64::from(i32::MAX))
            .ok_or_else(|| error("ERR invalid numkeys value"))? as usize;
        let end = count
            .checked_mul(2)
            .and_then(|n| n.checked_add(2))
            .filter(|n| *n <= args.len())
            .ok_or_else(|| error("ERR wrong number of key-value pairs"))?;
        let mut result = Self {
            count,
            end,
            condition: Existence::Any,
            expiry: Expiration::Unspecified,
        };
        let mut expiry = None;
        let mut i = end;
        while i < args.len() {
            let raw = &args[i];
            if raw.eq_ignore_ascii_case(b"NX") {
                if matches!(result.condition, Existence::Xx) {
                    return Err(syntax_error());
                }
                result.condition = Existence::Nx;
            } else if raw.eq_ignore_ascii_case(b"XX") {
                if matches!(result.condition, Existence::Nx) {
                    return Err(syntax_error());
                }
                result.condition = Existence::Xx;
            } else if raw.eq_ignore_ascii_case(b"KEEPTTL") && expiry.is_none() {
                result.expiry = Expiration::Keep;
            } else if let Some(kind) = expiry_kind(raw) {
                if matches!(result.expiry, Expiration::Keep)
                    || expiry.is_some_and(|(old, _)| old != kind)
                {
                    return Err(syntax_error());
                }
                i += 1;
                expiry = Some((kind, args.get(i).ok_or_else(syntax_error)?));
            } else {
                return Err(syntax_error());
            }
            i += 1;
        }
        if let Some((kind, raw)) = expiry {
            result.expiry = Expiration::Deadline(deadline(kind, raw, now, "msetex")?);
        }
        if (0..result.count).any(|i| args[3 + 2 * i].len() > MAX_STRING) {
            return Err(error("ERR string exceeds maximum allowed size (8 MiB)"));
        }
        Ok(result)
    }
}

impl Engine {
    fn string_substr(&mut self, args: &[Bytes], now: i64) -> Resp {
        self.string_getrange(args, now)
    }
    fn string_digest(&mut self, args: &[Bytes], now: i64) -> Resp {
        match self.value(&args[1], now) {
            Some(Value::String(value)) => Resp::Bulk(Some(Bytes::from(format!(
                "{:016x}",
                xxhash_rust::xxh3::xxh3_64(value.bytes())
            )))),
            Some(_) => wrong_type(),
            None => Resp::Bulk(None),
        }
    }
    fn string_delex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let condition = match args.len() {
            2 => SetCondition::Always,
            4 => match SetCondition::parse(&args[2], &args[3]) {
                Ok(v) => v,
                Err(_) => return error("ERR Invalid condition"),
            },
            _ => return error("ERR wrong number of arguments for 'delex' command"),
        };
        let current = self.value(&args[1], now);
        if current.is_none() {
            return Resp::Integer(0);
        }
        if !matches!(condition, SetCondition::Always) && !matches!(current, Some(Value::String(_)))
        {
            return error("ERR DELEX condition requires a string value");
        }
        match condition.matches(current) {
            Ok(true) => {}
            Ok(false) => return Resp::Integer(0),
            Err(e) => return e,
        }
        self.remove(&args[1]);
        self.record(now, || Mutation::Delete {
            key: args[1].clone(),
        });
        Resp::Integer(1)
    }
    fn string_incrbyfloat(&mut self, args: &[Bytes], now: i64) -> Resp {
        let increment = match parse_float(&args[2]) {
            Ok(v) => v,
            Err(e) => return e,
        };
        self.discard_expired(&args[1], now);
        let old = match self.entries.get(&args[1]).map(|e| &e.value) {
            Some(Value::String(raw)) => match parse_float(raw.bytes()) {
                Ok(v) => v,
                Err(e) => return e,
            },
            Some(_) => return wrong_type(),
            None => RedisFloat::from_bits(0),
        };
        let sum = match add_float(old, increment) {
            Ok(v) => v,
            Err(e) => return e,
        };
        let value = format_float(sum);
        let ttl = self.entries.get(&args[1]).and_then(|e| e.expires_at);
        self.store(&args[1], StringWrite::Copy(&value), ttl, now);
        Resp::Bulk(Some(value))
    }
    fn string_msetex(&mut self, args: &[Bytes], now: i64) -> Resp {
        let options = match MsetOptions::parse(args, now) {
            Ok(v) => v,
            Err(e) => return e,
        };
        // Conditions observe the original state, including when keys repeat.
        for pair in args[2..options.end].as_chunks::<2>().0 {
            let exists = self.value(&pair[0], now).is_some();
            if matches!(options.condition, Existence::Nx) && exists
                || matches!(options.condition, Existence::Xx) && !exists
            {
                return Resp::Integer(0);
            }
        }
        for pair in args[2..options.end].as_chunks::<2>().0 {
            let expiry = match options.expiry {
                Expiration::Deadline(time) => Some(time),
                Expiration::Keep => self.entries.get(&pair[0]).and_then(|e| e.expires_at),
                _ => None,
            };
            self.store(&pair[0], StringWrite::Copy(&pair[1]), expiry, now);
        }
        Resp::Integer(1)
    }
}

#[derive(Clone, Copy)]
enum Increment {
    Integer(i64),
    Float(RedisFloat),
}
struct IncrementOptions {
    increment: Increment,
    lower: Option<Increment>,
    upper: Option<Increment>,
    saturate: bool,
    expiry: Expiration,
    enx: bool,
}
impl IncrementOptions {
    fn parse(args: &[Bytes], now: i64) -> Result<Self, Resp> {
        let (mut by, mut lower, mut upper) = (None, None, None);
        let mut saturate = false;
        let mut enx = false;
        let mut expiry = Expiration::Unspecified;
        let mut expiry_kind_seen = None;
        let mut i = 2;
        while i < args.len() {
            let token = &args[i];
            if token.eq_ignore_ascii_case(b"SATURATE") {
                if saturate {
                    return Err(syntax_error());
                }
                saturate = true;
            } else if token.eq_ignore_ascii_case(b"ENX") {
                if enx || matches!(expiry, Expiration::Persist) {
                    return Err(syntax_error());
                }
                enx = true;
            } else if token.eq_ignore_ascii_case(b"PERSIST") {
                if enx || !matches!(expiry, Expiration::Unspecified) {
                    return Err(syntax_error());
                }
                expiry = Expiration::Persist;
            } else if let Some(kind) = expiry_kind(token) {
                if matches!(expiry, Expiration::Persist) || expiry_kind_seen.is_some() {
                    return Err(syntax_error());
                }
                i += 1;
                expiry = Expiration::Deadline(deadline(
                    kind,
                    args.get(i).ok_or_else(syntax_error)?,
                    now,
                    "increx",
                )?);
                expiry_kind_seen = Some(kind);
            } else if token.eq_ignore_ascii_case(b"BYINT") || token.eq_ignore_ascii_case(b"BYFLOAT")
            {
                if by.is_some() {
                    return Err(syntax_error());
                }
                i += 1;
                by = Some((
                    token.eq_ignore_ascii_case(b"BYFLOAT"),
                    args.get(i).ok_or_else(syntax_error)?,
                ));
            } else if token.eq_ignore_ascii_case(b"LBOUND") {
                if lower.is_some() {
                    return Err(syntax_error());
                }
                i += 1;
                lower = Some(args.get(i).ok_or_else(syntax_error)?);
            } else if token.eq_ignore_ascii_case(b"UBOUND") {
                if upper.is_some() {
                    return Err(syntax_error());
                }
                i += 1;
                upper = Some(args.get(i).ok_or_else(syntax_error)?);
            } else {
                return Err(syntax_error());
            }
            i += 1;
        }
        if enx && !matches!(expiry, Expiration::Deadline(_)) {
            return Err(error("ERR ENX flag requires an expiration"));
        }
        let floating = by.is_some_and(|(floating, _)| floating);
        let parse = |raw: &Bytes| -> Result<Increment, Resp> {
            if floating {
                parse_float(raw).map(Increment::Float)
            } else {
                integer(raw)
                    .map(Increment::Integer)
                    .ok_or_else(integer_error)
            }
        };
        let increment = match by {
            Some((true, raw)) => Increment::Float(increx_float(raw, true)?),
            Some((false, raw)) => Increment::Integer(
                integer(raw).ok_or_else(|| error("ERR Increment is not an integer"))?,
            ),
            None => Increment::Integer(1),
        };
        let lower = lower.map(parse).transpose()?;
        let upper = upper.map(parse).transpose()?;
        let reversed = match (lower, upper) {
            (Some(Increment::Integer(a)), Some(Increment::Integer(b))) => a > b,
            (Some(Increment::Float(a)), Some(Increment::Float(b))) => a > b,
            _ => false,
        };
        if reversed {
            return Err(error("ERR LBOUND can't be greater than UBOUND"));
        }
        Ok(Self {
            increment,
            lower,
            upper,
            saturate,
            expiry,
            enx,
        })
    }
}

impl Engine {
    fn string_increx(&mut self, args: &[Bytes], now: i64) -> Resp {
        let options = match IncrementOptions::parse(args, now) {
            Ok(v) => v,
            Err(e) => return e,
        };
        self.discard_expired(&args[1], now);
        let entry = self.entries.get(&args[1]);
        let previous_ttl = entry.and_then(|e| e.expires_at);
        let raw = match entry.map(|e| &e.value) {
            Some(Value::String(s)) => Some(s.bytes()),
            Some(_) => return wrong_type(),
            None => None,
        };
        // Float text is reply and journal bytes too; integers format in place.
        let float;
        let (write, reply) = match options.increment {
            Increment::Integer(delta) => {
                let old = match raw {
                    Some(s) => match integer(s) {
                        Some(v) => v,
                        None => return integer_error(),
                    },
                    None => 0,
                };
                let low = match options.lower {
                    Some(Increment::Integer(v)) => v,
                    _ => i64::MIN,
                };
                let high = match options.upper {
                    Some(Increment::Integer(v)) => v,
                    _ => i64::MAX,
                };
                // Widen before addition and subtraction, including saturated delta.
                let candidate = i128::from(old) + i128::from(delta);
                if (candidate < i128::from(low) || candidate > i128::from(high))
                    && !options.saturate
                {
                    return array(vec![Resp::Integer(old), Resp::Integer(0)]);
                }
                let result = candidate.clamp(i128::from(low), i128::from(high)) as i64;
                let Ok(applied) = i64::try_from(i128::from(result) - i128::from(old)) else {
                    return error("ERR applied increment would overflow");
                };
                (
                    StringWrite::Integer(result),
                    array(vec![Resp::Integer(result), Resp::Integer(applied)]),
                )
            }
            Increment::Float(delta) => {
                let old = match raw {
                    Some(s) => match increx_float(s, false) {
                        Ok(v) => v,
                        Err(e) => return e,
                    },
                    None => RedisFloat::from_bits(0),
                };
                let low = match options.lower {
                    Some(Increment::Float(v)) => v,
                    _ => -RedisFloat::largest(),
                };
                let high = match options.upper {
                    Some(Increment::Float(v)) => v,
                    _ => RedisFloat::largest(),
                };
                let candidate = old.add_r(delta, Round::NearestTiesToEven).value;
                let out = !candidate.is_finite() || candidate < low || candidate > high;
                if out && !options.saturate {
                    return array(vec![
                        self.string_float_reply(old),
                        self.string_float_reply(RedisFloat::from_bits(0)),
                    ]);
                }
                let result = if candidate < low {
                    low
                } else if candidate > high {
                    high
                } else {
                    candidate
                };
                let applied = result.sub_r(old, Round::NearestTiesToEven).value;
                if !applied.is_finite() {
                    return error("ERR increment would produce NaN or Infinity");
                }
                float = format_float(result);
                (
                    StringWrite::Copy(&float),
                    array(vec![
                        self.string_float_reply(result),
                        self.string_float_reply(applied),
                    ]),
                )
            }
        };
        let expiry = if options.enx && previous_ttl.is_some() {
            previous_ttl
        } else {
            match options.expiry {
                Expiration::Deadline(v) => Some(v),
                Expiration::Persist => None,
                _ => previous_ttl,
            }
        };
        self.store(&args[1], write, expiry, now);
        reply
    }

    fn string_float_reply(&self, value: RedisFloat) -> Resp {
        if self.command_version() == crate::protocol::Version::Resp3 {
            let encoded = format_float(value);
            // RESP3 doubles necessarily have binary64 wire semantics.
            Resp::Double(
                std::str::from_utf8(&encoded)
                    .expect("numeric ASCII")
                    .parse()
                    .unwrap_or(if value.is_negative() {
                        f64::NEG_INFINITY
                    } else {
                        f64::INFINITY
                    }),
            )
        } else {
            Resp::Bulk(Some(format_float(value)))
        }
    }

    fn string_lcs(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (mut len, mut idx, mut with_length, mut min_match) = (false, false, false, 0_i64);
        let mut i = 3;
        while i < args.len() {
            let option = &args[i];
            if option.eq_ignore_ascii_case(b"LEN") {
                len = true;
            } else if option.eq_ignore_ascii_case(b"IDX") {
                idx = true;
            } else if option.eq_ignore_ascii_case(b"WITHMATCHLEN") {
                with_length = true;
            } else if option.eq_ignore_ascii_case(b"MINMATCHLEN") {
                i += 1;
                let Some(n) = args.get(i).and_then(|v| integer(v)) else {
                    return integer_error();
                };
                min_match = n.max(0);
            } else {
                return syntax_error();
            }
            i += 1;
        }
        if len && idx {
            return error("ERR If you want both the length and indexes, please just use IDX.");
        }
        let a = match self.value(&args[1], now) {
            Some(Value::String(s)) => s.bytes().clone(),
            Some(_) => return wrong_type(),
            None => Bytes::new(),
        };
        let b = match self.value(&args[2], now) {
            Some(Value::String(s)) => s.bytes().clone(),
            Some(_) => return wrong_type(),
            None => Bytes::new(),
        };
        let Some(cells) = a
            .len()
            .checked_add(1)
            .and_then(|n| b.len().checked_add(1).and_then(|m| n.checked_mul(m)))
            .filter(|n| *n <= MAX_LCS_CELLS)
        else {
            return error("ERR LCS exceeds maximum work or memory limit");
        };
        let width = b.len() + 1;
        if len {
            let mut row = vec![0_u32; width];
            for x in a.iter() {
                let mut diagonal = 0;
                for (j, y) in b.iter().enumerate() {
                    let above = row[j + 1];
                    row[j + 1] = if x == y {
                        diagonal + 1
                    } else {
                        row[j].max(above)
                    };
                    diagonal = above;
                }
            }
            return Resp::Integer(i64::from(row[b.len()]));
        }
        let mut table = vec![0_u32; cells];
        for i in 1..=a.len() {
            for j in 1..=b.len() {
                table[i * width + j] = if a[i - 1] == b[j - 1] {
                    table[(i - 1) * width + j - 1] + 1
                } else {
                    table[(i - 1) * width + j].max(table[i * width + j - 1])
                };
            }
        }
        let length = table[a.len() * width + b.len()] as usize;
        if length > self.reply_limit {
            return error("ERR reply exceeds configured limit");
        }
        let mut sequence = if idx { Vec::new() } else { vec![0; length] };
        let mut remaining = length;
        let mut matches = Vec::new();
        let (mut i, mut j) = (a.len(), b.len());
        let mut run: Option<(usize, usize, usize, usize)> = None;
        let flush = |run: &mut Option<(usize, usize, usize, usize)>, matches: &mut Vec<Resp>| {
            if let Some((a_start, a_end, b_start, b_end)) = run.take() {
                let n = a_end - a_start + 1;
                if n as i64 >= min_match {
                    let mut entry = vec![
                        array(vec![
                            Resp::Integer(a_start as i64),
                            Resp::Integer(a_end as i64),
                        ]),
                        array(vec![
                            Resp::Integer(b_start as i64),
                            Resp::Integer(b_end as i64),
                        ]),
                    ];
                    if with_length {
                        entry.push(Resp::Integer(n as i64));
                    }
                    matches.push(array(entry));
                }
            }
        };
        while i > 0 && j > 0 {
            if a[i - 1] == b[j - 1] {
                remaining -= 1;
                if idx {
                    match run.as_mut() {
                        Some((start, _, other, _)) if *start == i && *other == j => {
                            *start = i - 1;
                            *other = j - 1;
                        }
                        _ => {
                            flush(&mut run, &mut matches);
                            run = Some((i - 1, i - 1, j - 1, j - 1));
                        }
                    }
                } else {
                    sequence[remaining] = a[i - 1];
                }
                i -= 1;
                j -= 1;
            } else if table[(i - 1) * width + j] > table[i * width + j - 1] {
                i -= 1;
            } else {
                j -= 1;
            }
        }
        if !idx {
            return Resp::Bulk(Some(Bytes::from(sequence)));
        }
        flush(&mut run, &mut matches);
        let entries = vec![
            (
                Resp::Bulk(Some(Bytes::from_static(b"matches"))),
                array(matches),
            ),
            (
                Resp::Bulk(Some(Bytes::from_static(b"len"))),
                Resp::Integer(length as i64),
            ),
        ];
        if self.command_version() == crate::protocol::Version::Resp3 {
            Resp::Map(entries)
        } else {
            array(entries.into_iter().flat_map(|(k, v)| [k, v]).collect())
        }
    }
}

fn increx_float(raw: &[u8], increment: bool) -> Result<RedisFloat, Resp> {
    let magnitude = if matches!(raw.first(), Some(b'+') | Some(b'-')) {
        &raw[1..]
    } else {
        raw
    };
    if magnitude.eq_ignore_ascii_case(b"inf") || magnitude.eq_ignore_ascii_case(b"infinity") {
        return Err(error(if increment {
            "ERR BYFLOAT increment cannot be Infinity"
        } else {
            "ERR value cannot be Infinity"
        }));
    }
    parse_float(raw).map_err(|e| {
        if increment {
            error("ERR Increment is not a valid float")
        } else {
            e
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn args(values: &[&str]) -> Vec<Bytes> {
        values
            .iter()
            .map(|s| Bytes::copy_from_slice(s.as_bytes()))
            .collect()
    }
    fn run(engine: &mut Engine, values: &[&str], now: i64) -> Resp {
        engine.execute_at(&args(values), now)
    }
    fn bulk(value: &str) -> Resp {
        Resp::Bulk(Some(Bytes::copy_from_slice(value.as_bytes())))
    }

    #[test]
    fn xxh3_published_vector_and_case_insensitive_comparison() {
        let mut e = Engine::new();
        e.store(
            &Bytes::from_static(b"k"),
            StringWrite::Copy(&Bytes::from_static(b"Hello world")),
            None,
            1000,
        );
        assert_eq!(
            run(&mut e, &["DIGEST", "k"], 1000),
            bulk("b6acb9d84a38ff74")
        );
        assert_eq!(
            run(&mut e, &["DELEX", "k", "IFDEQ", "B6ACB9D84A38FF74"], 1000),
            Resp::Integer(1)
        );
    }

    #[test]
    fn digest_validation_is_after_missing_key_and_type_checks() {
        let raw = Bytes::from_static(b"bad");
        assert!(!SetCondition::DigestEq(&raw).matches(None).unwrap());
        assert!(SetCondition::DigestNe(&raw).matches(None).unwrap());
        assert!(
            SetCondition::DigestEq(&raw)
                .matches(Some(&Value::String(super::super::StringValue::default())))
                .is_err()
        );
        let mut e = Engine::new();
        assert_eq!(
            run(&mut e, &["DELEX", "missing", "IFDEQ", "bad"], 1),
            Resp::Integer(0)
        );
        run(&mut e, &["LPUSH", "list", "x"], 1);
        assert!(
            matches!(run(&mut e,&["SET","list","x","IFDNE","bad"],1),Resp::Error(s) if s.starts_with("WRONGTYPE"))
        );
        assert_eq!(run(&mut e, &["DELEX", "list"], 1), Resp::Integer(1));
    }

    #[test]
    fn extended_precision_is_not_silently_narrowed_to_binary64() {
        let one = parse_float(b"1").unwrap();
        assert_eq!(
            format_float(add_float(parse_float(b"9007199254740992").unwrap(), one).unwrap()),
            Bytes::from_static(b"9007199254740993")
        );
        assert_eq!(
            format_float(
                add_float(parse_float(b"0.1").unwrap(), parse_float(b"0.2").unwrap()).unwrap()
            ),
            Bytes::from_static(b"0.3")
        );
        assert_eq!(
            format_float(parse_float(b"-0.000").unwrap()),
            Bytes::from_static(b"0")
        );
        assert_eq!(
            format_float(parse_float(b"0x1.8p+2").unwrap()),
            Bytes::from_static(b"6")
        );
        assert!(parse_float(b"1e99999999999999999999999999999").is_err());
        for invalid in [
            b" 1".as_slice(),
            b"1 ",
            b"NaN",
            b"inf",
            b"1e",
            b"0x.p1",
            b"1\0",
        ] {
            assert!(parse_float(invalid).is_err());
        }
    }

    #[test]
    fn numeric_zero_padding_has_bounded_parser_state() {
        let mut raw = vec![b'0'; 128 * 1024];
        raw.extend_from_slice(b"1.000e+");
        raw.extend(std::iter::repeat_n(b'0', 128 * 1024));
        assert_eq!(
            format_float(parse_float(&raw).unwrap()),
            Bytes::from_static(b"1")
        );
        let mut zero = vec![b'0'; 128 * 1024];
        zero.extend_from_slice(b"e99999999999999999999999");
        assert!(parse_float(&zero).unwrap().is_zero());
    }

    #[test]
    fn increx_skips_overflow_without_touching_ttl_then_saturates() {
        let mut e = Engine::new();
        run(
            &mut e,
            &["SET", "k", "9223372036854775800", "PXAT", "9000"],
            1000,
        );
        assert_eq!(
            run(
                &mut e,
                &["INCREX", "k", "BYINT", "9223372036854775800", "PERSIST"],
                1000
            ),
            array(vec![Resp::Integer(9223372036854775800), Resp::Integer(0)])
        );
        assert_eq!(
            e.entries.get(b"k".as_slice()).unwrap().expires_at,
            Some(9000)
        );
        assert_eq!(
            run(
                &mut e,
                &["INCREX", "k", "BYINT", "9223372036854775800", "SATURATE"],
                1000
            ),
            array(vec![Resp::Integer(i64::MAX), Resp::Integer(7)])
        );
        assert!(matches!(
            run(
                &mut e,
                &["INCREX", "k", "UBOUND", "-9223372036854775808", "SATURATE"],
                1000
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            run(&mut e, &["GET", "k"], 1000),
            bulk("9223372036854775807")
        );
        assert_eq!(
            e.entries.get(b"k".as_slice()).unwrap().expires_at,
            Some(9000)
        );
        assert_eq!(
            run(
                &mut e,
                &["INCREX", "absent", "BYINT", "1", "UBOUND", "0"],
                1000
            ),
            array(vec![Resp::Integer(0), Resp::Integer(0)])
        );
        assert!(!e.entries.contains_key(b"absent".as_slice()));
    }

    #[test]
    fn increx_enx_and_past_deadline_use_the_same_clock() {
        let mut e = Engine::new();
        run(&mut e, &["SET", "k", "4", "PXAT", "9000"], 1000);
        assert_eq!(
            run(&mut e, &["INCREX", "k", "PXAT", "1", "ENX"], 1000),
            array(vec![Resp::Integer(5), Resp::Integer(1)])
        );
        assert_eq!(
            e.entries.get(b"k".as_slice()).unwrap().expires_at,
            Some(9000)
        );
        assert_eq!(
            run(&mut e, &["INCREX", "k", "PXAT", "1"], 1000),
            array(vec![Resp::Integer(6), Resp::Integer(1)])
        );
        assert!(!e.entries.contains_key(b"k".as_slice()));
    }

    #[test]
    fn increx_rejects_all_duplicate_flags_before_mutation() {
        for options in [
            vec!["BYINT", "1", "BYINT", "2"],
            vec!["BYFLOAT", "1", "BYFLOAT", "2"],
            vec!["SATURATE", "SATURATE"],
            vec!["LBOUND", "0", "LBOUND", "1"],
            vec!["UBOUND", "1", "UBOUND", "2"],
            vec!["EX", "1", "EX", "2"],
            vec!["PX", "1", "PX", "2"],
            vec!["EXAT", "1", "EXAT", "2"],
            vec!["PXAT", "1", "PXAT", "2"],
            vec!["PERSIST", "PERSIST"],
            vec!["EX", "1", "ENX", "ENX"],
        ] {
            let mut e = Engine::new();
            e.store(
                &Bytes::from_static(b"k"),
                StringWrite::Copy(&Bytes::from_static(b"5")),
                Some(9000),
                1000,
            );
            let mut command = vec!["INCREX", "k"];
            command.extend(options);
            assert!(matches!(run(&mut e, &command, 1000), Resp::Error(_)));
            assert_eq!(run(&mut e, &["GET", "k"], 1000), bulk("5"));
            assert_eq!(
                e.entries.get(b"k".as_slice()).unwrap().expires_at,
                Some(9000)
            );
        }
    }

    #[test]
    fn msetex_is_atomic_for_conditions_and_repeated_keys() {
        let mut e = Engine::new();
        run(&mut e, &["SET", "existing", "old", "PXAT", "9000"], 1000);
        assert_eq!(
            run(
                &mut e,
                &["MSETEX", "2", "new", "x", "existing", "changed", "NX"],
                1000
            ),
            Resp::Integer(0)
        );
        assert!(!e.entries.contains_key(b"new".as_slice()));
        assert_eq!(run(&mut e, &["GET", "existing"], 1000), bulk("old"));
        assert_eq!(
            run(
                &mut e,
                &[
                    "MSETEX", "3", "a", "first", "b", "other", "a", "last", "NX", "PX", "500"
                ],
                1000
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut e, &["GET", "a"], 1000), bulk("last"));
        assert_eq!(
            e.entries.get(b"a".as_slice()).unwrap().expires_at,
            Some(1500)
        );
        assert_eq!(
            e.entries.get(b"b".as_slice()).unwrap().expires_at,
            Some(1500)
        );
        assert_eq!(
            run(
                &mut e,
                &["MSETEX", "2", "a", "next", "a", "final", "XX", "KEEPTTL"],
                1100
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut e, &["GET", "a"], 1100), bulk("final"));
        assert_eq!(
            e.entries.get(b"a".as_slice()).unwrap().expires_at,
            Some(1500)
        );
        assert!(matches!(
            run(
                &mut e,
                &["MSETEX", "2", "a", "bad", "b", "bad", "EX", "0"],
                1100
            ),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut e, &["GET", "a"], 1100), bulk("final"));
    }

    #[test]
    fn lcs_returns_noncontiguous_subsequence_and_reverse_index_runs() {
        let mut e = Engine::new();
        run(&mut e, &["MSET", "a", "ohmytext", "b", "mynewtext"], 0);
        assert_eq!(run(&mut e, &["LCS", "a", "b"], 0), bulk("mytext"));
        assert_eq!(run(&mut e, &["LCS", "a", "b", "LEN"], 0), Resp::Integer(6));
        assert_eq!(
            run(
                &mut e,
                &["LCS", "a", "b", "IDX", "MINMATCHLEN", "4", "WITHMATCHLEN"],
                0
            ),
            array(vec![
                bulk("matches"),
                array(vec![array(vec![
                    array(vec![Resp::Integer(4), Resp::Integer(7)]),
                    array(vec![Resp::Integer(5), Resp::Integer(8)]),
                    Resp::Integer(4)
                ])]),
                bulk("len"),
                Resp::Integer(6),
            ])
        );
    }

    #[test]
    fn lcs_work_limit_errors_without_mutating_either_input() {
        let mut e = Engine::new();
        let value = Bytes::from(vec![b'a'; 4096]);
        let a = Bytes::from_static(b"a");
        let b = Bytes::from_static(b"b");
        e.store(&a, StringWrite::Copy(&value), Some(9000), 1000);
        e.store(&b, StringWrite::Copy(&value), None, 1000);
        assert!(matches!(
            run(&mut e, &["LCS", "a", "b"], 1000),
            Resp::Error(_)
        ));
        assert_eq!(e.entries.get(&a).unwrap().expires_at, Some(9000));
        assert!(
            matches!(&e.entries.get(&a).unwrap().value,Value::String(s) if s.bytes() == &value)
        );
        assert!(
            matches!(&e.entries.get(&b).unwrap().value,Value::String(s) if s.bytes() == &value)
        );
    }
}
