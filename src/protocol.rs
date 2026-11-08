//! Bounded RESP command parsing and versioned RESP2/RESP3 response encoding.
//!
//! Commands are limited to 8 MiB (including framing) and 1,024 arguments.
//! Inline commands require CRLF and support only space/tab-separated arguments:
//! quoting and escaping are deliberately unsupported; quote characters are rejected.

use bytes::{Bytes, BytesMut};

const MAX_COMMAND_BYTES: usize = 8 * 1024 * 1024;
const MAX_ARGUMENTS: usize = 1024;

/// Canonical RESP framing budget for internal ingress (Lua and management).
/// Network requests are validated incrementally by the parser.
pub fn validate_command(args: &[Bytes]) -> Result<usize, &'static str> {
    if args.is_empty() || args.len() > MAX_ARGUMENTS {
        return Err("command must contain 1..1024 arguments");
    }
    let mut total = args.len().ilog10() as usize + 4;
    for arg in args {
        let digits = if arg.is_empty() {
            1
        } else {
            arg.len().ilog10() as usize + 1
        };
        total = total
            .checked_add(arg.len())
            .and_then(|n| n.checked_add(digits + 5))
            .filter(|n| *n <= MAX_COMMAND_BYTES)
            .ok_or("command exceeds 8 MiB including framing")?;
    }
    Ok(total)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Version {
    Resp2,
    Resp3,
}

#[derive(Debug, PartialEq)]
pub enum Resp {
    Simple(&'static str),
    Status(Bytes),
    Verbatim { format: [u8; 3], value: Bytes },
    Error(String),
    Integer(i64),
    Boolean(bool),
    Unsigned(u64),
    BigNumber(Bytes),
    InfoField(Bytes, Box<Resp>),
    Bulk(Option<Bytes>),
    Array(Option<Vec<Resp>>),
    Map(Vec<(Resp, Resp)>),
    Set(Vec<Resp>),
    Double(f64),
    Scored(Vec<(Bytes, f64)>),
    VectorResults(Vec<(Bytes, Option<f64>, Option<Option<Bytes>>)>),
}

impl Resp {
    pub fn encode(&self, out: &mut BytesMut, version: Version) {
        match self {
            Self::Simple(value) => encode_line(b'+', value.as_bytes(), out),
            Self::Status(value) => encode_line(b'+', value, out),
            Self::BigNumber(value) => {
                if version == Version::Resp3 {
                    encode_line(b'(', value, out);
                } else {
                    encode_bulk(value, out);
                }
            }
            Self::Verbatim { format, value } => {
                if version == Version::Resp3 {
                    encode_length(b'=', value.len() + 4, out);
                    out.extend_from_slice(format);
                    out.extend_from_slice(b":");
                    out.extend_from_slice(value);
                    out.extend_from_slice(b"\r\n");
                } else {
                    encode_bulk(value, out);
                }
            }
            Self::Error(value) => encode_line(b'-', value.as_bytes(), out),
            Self::Boolean(value) => out.extend_from_slice(match (version, value) {
                (Version::Resp2, true) => b":1\r\n",
                (Version::Resp2, false) => b":0\r\n",
                (Version::Resp3, true) => b"#t\r\n",
                (Version::Resp3, false) => b"#f\r\n",
            }),
            Self::Unsigned(value) => {
                if *value <= i64::MAX as u64 {
                    encode_number(b':', false, *value, out);
                } else if version == Version::Resp3 {
                    encode_number(b'(', false, *value, out);
                } else {
                    let mut digits = [0_u8; 20];
                    let start = write_decimal(*value, &mut digits, 20);
                    encode_bulk(&digits[start..], out);
                }
            }
            Self::InfoField(field, value) => {
                encode_length(
                    if version == Version::Resp3 {
                        b'%'
                    } else {
                        b'*'
                    },
                    1,
                    out,
                );
                if version == Version::Resp3 {
                    encode_bulk(field, out);
                }
                value.encode(out, version);
            }
            Self::Integer(value) => encode_number(b':', *value < 0, value.unsigned_abs(), out),
            Self::Bulk(None) => out.extend_from_slice(match version {
                Version::Resp2 => b"$-1\r\n",
                Version::Resp3 => b"_\r\n",
            }),
            Self::Bulk(Some(value)) => encode_bulk(value, out),
            Self::Array(None) => out.extend_from_slice(match version {
                Version::Resp2 => b"*-1\r\n",
                Version::Resp3 => b"_\r\n",
            }),
            Self::Array(Some(values)) | Self::Set(values) => {
                let marker = if matches!(self, Self::Set(_)) && version == Version::Resp3 {
                    b'~'
                } else {
                    b'*'
                };
                encode_length(marker, values.len(), out);
                for value in values {
                    value.encode(out, version);
                }
            }
            Self::Map(entries) => {
                match version {
                    Version::Resp2 => encode_length(b'*', entries.len() * 2, out),
                    Version::Resp3 => encode_length(b'%', entries.len(), out),
                }
                for (key, value) in entries {
                    key.encode(out, version);
                    value.encode(out, version);
                }
            }
            Self::Double(value) => encode_double(*value, out, version),
            Self::Scored(entries) => {
                let length = match version {
                    Version::Resp2 => entries.len() * 2,
                    Version::Resp3 => entries.len(),
                };
                encode_length(b'*', length, out);
                for (member, score) in entries {
                    if version == Version::Resp3 {
                        encode_length(b'*', 2, out);
                    }
                    encode_bulk(member, out);
                    encode_double(*score, out, version);
                }
            }
            Self::VectorResults(entries) => {
                let mapped = version == Version::Resp3
                    && entries
                        .first()
                        .is_some_and(|entry| entry.1.is_some() || entry.2.is_some());
                let length = if mapped {
                    entries.len()
                } else {
                    entries
                        .iter()
                        .map(|(_, score, attr)| {
                            1 + usize::from(score.is_some()) + usize::from(attr.is_some())
                        })
                        .sum()
                };
                encode_length(if mapped { b'%' } else { b'*' }, length, out);
                for (member, score, attr) in entries {
                    encode_bulk(member, out);
                    if mapped && score.is_some() && attr.is_some() {
                        encode_length(b'*', 2, out);
                    }
                    if let Some(score) = score {
                        encode_double(*score, out, version);
                    }
                    if let Some(attr) = attr {
                        Self::Bulk(attr.clone()).encode(out, version);
                    }
                }
            }
        }
    }
}

fn encode_line(marker: u8, value: &[u8], out: &mut BytesMut) {
    out.extend_from_slice(&[marker]);
    // Statuses and errors may contain user input, never additional RESP frames.
    let mut start = 0;
    for (position, &byte) in value.iter().enumerate() {
        if matches!(byte, b'\r' | b'\n') {
            out.extend_from_slice(&value[start..position]);
            out.extend_from_slice(b" ");
            start = position + 1;
        }
    }
    out.extend_from_slice(&value[start..]);
    out.extend_from_slice(b"\r\n");
}

pub(crate) fn encode_length(marker: u8, length: usize, out: &mut BytesMut) {
    encode_number(marker, false, length as u64, out);
}

/// `marker`, a '-' when `negative`, `magnitude`'s digits and CRLF, appended
/// as one line.
fn encode_number(marker: u8, negative: bool, magnitude: u64, out: &mut BytesMut) {
    // Marker, sign, at most 20 digits and CRLF.
    let mut line = [0_u8; 24];
    line[22] = b'\r';
    line[23] = b'\n';
    let mut start = write_decimal(magnitude, &mut line, 22);
    if negative {
        start -= 1;
        line[start] = b'-';
    }
    start -= 1;
    line[start] = marker;
    out.extend_from_slice(&line[start..]);
}

fn encode_bulk(value: &[u8], out: &mut BytesMut) {
    encode_length(b'$', value.len(), out);
    out.extend_from_slice(value);
    out.extend_from_slice(b"\r\n");
}

fn encode_double(value: f64, out: &mut BytesMut, version: Version) {
    let mut buffer = ryu::Buffer::new();
    let text = if value.is_nan() {
        "nan"
    } else if value == f64::INFINITY {
        "inf"
    } else if value == f64::NEG_INFINITY {
        "-inf"
    } else {
        let text = buffer.format_finite(value);
        text.strip_suffix(".0").unwrap_or(text)
    };
    match version {
        Version::Resp2 => encode_bulk(text.as_bytes(), out),
        Version::Resp3 => {
            out.extend_from_slice(b",");
            out.extend_from_slice(text.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }
}

/// "00" through "99", two decimal digits per lookup.
const DIGIT_PAIRS: [u8; 200] = {
    let mut table = [0_u8; 200];
    let mut pair = 0;
    while pair < 100 {
        table[pair * 2] = b'0' + (pair / 10) as u8;
        table[pair * 2 + 1] = b'0' + (pair % 10) as u8;
        pair += 1;
    }
    table
};

/// Writes `value`'s decimal digits to end just before `buffer[end]`,
/// returning where they start. `u64::MAX` takes 20 digits.
fn write_decimal(mut value: u64, buffer: &mut [u8], mut end: usize) -> usize {
    while value >= 100 {
        let pair = (value % 100) as usize * 2;
        value /= 100;
        end -= 2;
        buffer[end..end + 2].copy_from_slice(&DIGIT_PAIRS[pair..pair + 2]);
    }
    if value >= 10 {
        let pair = value as usize * 2;
        end -= 2;
        buffer[end..end + 2].copy_from_slice(&DIGIT_PAIRS[pair..pair + 2]);
    } else {
        end -= 1;
        buffer[end] = b'0' + value as u8;
    }
    end
}

/// An integer's canonical decimal spelling, formatted without `core::fmt`.
pub(crate) struct Decimal {
    digits: [u8; 20],
    start: usize,
}

impl Decimal {
    /// Redis's signed spelling: a leading '-' only for negative values.
    pub(crate) fn signed(value: i64) -> Self {
        let mut digits = [0_u8; 20];
        // `i64::MIN`'s magnitude has 19 digits, leaving room for the sign.
        let mut start = write_decimal(value.unsigned_abs(), &mut digits, 20);
        if value < 0 {
            start -= 1;
            digits[start] = b'-';
        }
        Self { digits, start }
    }

    pub(crate) fn as_bytes(&self) -> &[u8] {
        &self.digits[self.start..]
    }
}

/// Parse exactly one command, leaving any pipelined commands buffered.
/// Incomplete input is never consumed. Protocol errors require connection closure.
/// Bulk argument bytes share the completed command's allocation.
///
/// `arguments` supplies the vector the arguments are collected into, given
/// their count. It is called only once a complete, valid command is buffered,
/// so incomplete or malformed input asks for nothing. Any vector serves: it is
/// cleared, then grown exactly to the count if it is too small.
pub fn parse(
    input: &mut BytesMut,
    arguments: impl FnOnce(usize) -> Vec<Bytes>,
) -> Result<Option<Vec<Bytes>>, String> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        return parse_inline(input, arguments);
    }

    let Some((count, mut cursor)) = read_length(input, 1, MAX_ARGUMENTS)? else {
        return Ok(None);
    };
    let arguments_start = cursor;
    for _ in 0..count {
        if cursor >= MAX_COMMAND_BYTES {
            return Err("command exceeds 8 MiB".into());
        }
        let Some(&marker) = input.get(cursor) else {
            return Ok(None);
        };
        if marker != b'$' {
            return Err("expected a bulk string argument".into());
        }
        let Some((length, start)) = read_length(input, cursor + 1, MAX_COMMAND_BYTES)? else {
            return Ok(None);
        };
        let end = start
            .checked_add(length)
            .and_then(|end| end.checked_add(2))
            .filter(|&end| end <= MAX_COMMAND_BYTES)
            .ok_or_else(|| "command exceeds 8 MiB".to_owned())?;
        if input.len() < end {
            return Ok(None);
        }
        if &input[end - 2..end] != b"\r\n" {
            return Err("bulk string must end with CRLF".into());
        }
        cursor = end;
    }

    // Only a complete, validated command gets split off. Re-reading its small
    // length headers avoids allocating offset metadata on incomplete requests.
    let frame = input.split_to(cursor).freeze();
    let mut args = argument_vector(arguments, count);
    cursor = arguments_start;
    for _ in 0..count {
        let Some((length, start)) = read_length(&frame, cursor + 1, MAX_COMMAND_BYTES)? else {
            return Err("incomplete validated command".into());
        };
        args.push(frame.slice(start..start + length));
        cursor = start + length + 2;
    }
    Ok(Some(args))
}

fn parse_inline(
    input: &mut BytesMut,
    arguments: impl FnOnce(usize) -> Vec<Bytes>,
) -> Result<Option<Vec<Bytes>>, String> {
    let Some((end, consumed)) = line_end(input, 0)? else {
        return Ok(None);
    };
    let line = &input[..end];
    if line.iter().any(|byte| matches!(byte, b'\'' | b'"' | b'\\')) {
        return Err("inline quoting and escaping are unsupported; use RESP bulk strings".into());
    }
    let count = line
        .split(|byte| matches!(byte, b' ' | b'\t'))
        .filter(|arg| !arg.is_empty())
        .count();
    if count > MAX_ARGUMENTS {
        return Err("command exceeds 1024 arguments".into());
    }
    let frame = input.split_to(consumed).freeze();
    let mut args = argument_vector(arguments, count);
    let mut cursor = 0;
    while cursor < end {
        if matches!(frame[cursor], b' ' | b'\t') {
            cursor += 1;
            continue;
        }
        let start = cursor;
        while cursor < end && !matches!(frame[cursor], b' ' | b'\t') {
            cursor += 1;
        }
        args.push(frame.slice(start..cursor));
    }
    Ok(Some(args))
}

/// An empty vector with room for `count` arguments from `arguments`. Growth is
/// exact, so a supplied vector never grows past what its command needs.
fn argument_vector(arguments: impl FnOnce(usize) -> Vec<Bytes>, count: usize) -> Vec<Bytes> {
    let mut args = arguments(count);
    args.clear();
    args.reserve_exact(count);
    args
}

/// Return the line's content end and the offset after its CRLF.
fn line_end(input: &[u8], start: usize) -> Result<Option<(usize, usize)>, String> {
    let mut cursor = start;
    while cursor < input.len() && cursor < MAX_COMMAND_BYTES {
        match input[cursor] {
            b'\r' => {
                if cursor + 1 >= MAX_COMMAND_BYTES {
                    return Err("command exceeds 8 MiB".into());
                }
                let Some(&next) = input.get(cursor + 1) else {
                    return Ok(None);
                };
                if next != b'\n' {
                    return Err("expected CRLF line ending".into());
                }
                return Ok(Some((cursor, cursor + 2)));
            }
            b'\n' => return Err("expected CRLF line ending".into()),
            _ => cursor += 1,
        }
    }
    if cursor >= MAX_COMMAND_BYTES {
        Err("command exceeds 8 MiB".into())
    } else {
        Ok(None)
    }
}

/// A RESP length header's value and the offset after its CRLF. Digits that
/// end in CRLF within the limits are decided in this one pass; any other
/// input, incomplete or invalid, is decided by `read_length_exactly`, so the
/// pass accepts exactly what that reference accepts, with the same result.
#[inline]
fn read_length(
    input: &[u8],
    start: usize,
    maximum: usize,
) -> Result<Option<(usize, usize)>, String> {
    let mut cursor = start;
    let mut length = 0_usize;
    while let Some(&byte) = input.get(cursor) {
        if byte.is_ascii_digit() {
            match length
                .checked_mul(10)
                .and_then(|value| value.checked_add((byte - b'0') as usize))
            {
                Some(value) if value <= maximum => length = value,
                _ => break,
            }
            cursor += 1;
        } else {
            // `line_end` scans only below `MAX_COMMAND_BYTES` and rejects a
            // CR whose LF would reach it.
            if byte == b'\r'
                && cursor > start
                && cursor + 1 < MAX_COMMAND_BYTES
                && input.get(cursor + 1) == Some(&b'\n')
            {
                return Ok(Some((length, cursor + 2)));
            }
            break;
        }
    }
    read_length_exactly(input, start, maximum)
}

#[cold]
fn read_length_exactly(
    input: &[u8],
    start: usize,
    maximum: usize,
) -> Result<Option<(usize, usize)>, String> {
    let Some((end, next)) = line_end(input, start)? else {
        return Ok(None);
    };
    if start == end {
        return Err("empty RESP length".into());
    }
    let mut length = 0_usize;
    for &byte in &input[start..end] {
        if !byte.is_ascii_digit() {
            return Err("RESP length must be an unsigned decimal integer".into());
        }
        length = length
            .checked_mul(10)
            .and_then(|value| value.checked_add((byte - b'0') as usize))
            .filter(|&value| value <= maximum)
            .ok_or_else(|| "RESP length exceeds command limits".to_owned())?;
    }
    Ok(Some((length, next)))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Arguments for input that must not complete a command.
    fn none(count: usize) -> Vec<Bytes> {
        panic!("{count} arguments requested before a complete, valid command")
    }

    #[test]
    fn fragmentation_at_every_split_preserves_incomplete_input() {
        let command = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n\0\r\n\xffxy\r\n";
        for split in 0..command.len() {
            let mut input = BytesMut::from(&command[..split]);
            let before = input.clone();
            assert_eq!(parse(&mut input, none).unwrap(), None, "split {split}");
            assert_eq!(input, before, "split {split}");
            input.extend_from_slice(&command[split..]);
            assert_eq!(
                parse(&mut input, Vec::with_capacity).unwrap().unwrap(),
                vec![
                    Bytes::from_static(b"SET"),
                    Bytes::from_static(b"k"),
                    Bytes::from_static(b"\0\r\n\xffxy")
                ],
            );
            assert!(input.is_empty());
        }
    }

    #[test]
    fn preserves_pipeline_and_binary_payloads() {
        let mut input = BytesMut::from(&b"*2\r\n$4\r\nECHO\r\n$4\r\n\r\n\0\xff\r\nPING\r\n"[..]);
        let args = parse(&mut input, Vec::with_capacity).unwrap().unwrap();
        assert_eq!(&args[1][..], b"\r\n\0\xff");
        assert_eq!(&input[..], b"PING\r\n");
        assert_eq!(
            parse(&mut input, Vec::with_capacity).unwrap().unwrap(),
            vec![Bytes::from_static(b"PING")]
        );
        assert!(input.is_empty());
        // Slices remain usable after the input buffer has been consumed/reused.
        input.extend_from_slice(b"other data");
        assert_eq!(&args[1][..], b"\r\n\0\xff");
    }

    #[test]
    fn rejects_malformed_and_oversized_lengths_without_consuming() {
        let cases: &[&[u8]] = &[
            b"*\r\n",
            b"*-1\r\n",
            b"*+1\r\n",
            b"*1x\r\n",
            b"*1025\r\n",
            b"*9999999999999999999999999999999999\r\n",
            b"*1\r\n$-1\r\n",
            b"*1\r\n$\r\n",
            b"*1\r\n$+0\r\n",
            b"*1\r\n$8388608\r\n",
            b"*1\r\n$999999999999999999999999\r\n",
            b"*1\r\n+OK\r\n",
            b"*1\n",
            b"*1\rx",
            b"*1\r\n$1\r\nxzz",
        ];
        for &case in cases {
            let mut input = BytesMut::from(case);
            let before = input.clone();
            assert!(parse(&mut input, none).is_err(), "{case:?}");
            assert_eq!(input, before);
        }
    }

    #[test]
    fn length_headers_fail_only_once_their_line_decides_the_error() {
        const EMPTY: &str = "empty RESP length";
        const DIGITS: &str = "RESP length must be an unsigned decimal integer";
        const LIMITS: &str = "RESP length exceeds command limits";
        const CRLF: &str = "expected CRLF line ending";
        let cases: &[(&[u8], &str)] = &[
            (b"*\r\n", EMPTY),
            (b"*1x\r\n", DIGITS),
            (b"*x1\r\n", DIGITS),
            (b"*1025\r\n", LIMITS),
            (b"*99999999999999999999999\r\n", LIMITS),
            (b"*1\r\n$8388609\r\n", LIMITS),
            (b"*1\r\n$1x\r\n", DIGITS),
            (b"*12\n", CRLF),
            (b"*1x\n", CRLF),
            (b"*1\r\r", CRLF),
            (b"*1\r\n$3\rx", CRLF),
        ];
        for &(case, message) in cases {
            // Each case ends at the byte that decides it: every shorter
            // prefix, however malformed already, waits for more input.
            for end in 0..case.len() {
                let mut input = BytesMut::from(&case[..end]);
                assert_eq!(parse(&mut input, none), Ok(None), "{case:?} at {end}");
                assert_eq!(&input[..], &case[..end]);
            }
            let mut input = BytesMut::from(case);
            let error = parse(&mut input, none).unwrap_err();
            assert_eq!(error, message, "{case:?}");
            assert_eq!(&input[..], case);
        }
    }

    #[test]
    fn bounds_whole_command_and_inline_lines() {
        let mut input = BytesMut::from(&b"*1\r\n$8388593\r\n"[..]);
        input.resize(MAX_COMMAND_BYTES, b'x');
        // The advertised payload plus its framing cannot fit the command limit.
        assert!(parse(&mut input, none).is_err());
        let mut input = BytesMut::from(&b"*1\r\n$8388592\r\n"[..]);
        input.resize(MAX_COMMAND_BYTES - 2, b'x');
        input.extend_from_slice(b"\r\nPING\r\n");
        assert_eq!(
            parse(&mut input, Vec::with_capacity).unwrap().unwrap()[0].len(),
            8_388_592
        );
        assert_eq!(&input[..], b"PING\r\n");
        let mut input = BytesMut::new();
        input.resize(MAX_COMMAND_BYTES, b'x');
        assert!(parse(&mut input, none).is_err());
        let mut input = BytesMut::from(&b"*1024\r\n"[..]);
        for _ in 0..1024 {
            input.extend_from_slice(b"$0\r\n\r\n");
        }
        assert_eq!(
            parse(&mut input, Vec::with_capacity)
                .unwrap()
                .unwrap()
                .len(),
            1024
        );
        let mut input = BytesMut::new();
        for _ in 0..1025 {
            input.extend_from_slice(b"x ");
        }
        input.extend_from_slice(b"\r\n");
        assert!(parse(&mut input, none).is_err());
    }

    #[test]
    fn inline_fragmentation_and_restricted_syntax() {
        for split in 0..b"PING\r\n".len() {
            let mut input = BytesMut::from(&b"PING\r\n"[..split]);
            assert_eq!(parse(&mut input, none).unwrap(), None);
            input.extend_from_slice(&b"PING\r\n"[split..]);
            assert_eq!(
                parse(&mut input, Vec::with_capacity).unwrap().unwrap(),
                vec![Bytes::from_static(b"PING")]
            );
        }
        let mut input = BytesMut::from(&b"  SET\tk  value \r\n"[..]);
        assert_eq!(
            parse(&mut input, Vec::with_capacity)
                .unwrap()
                .unwrap()
                .len(),
            3
        );
        for line in [
            "SET k 'value'\r\n",
            "SET k \"value\"\r\n",
            "SET k a\\ b\r\n",
        ] {
            assert!(parse(&mut BytesMut::from(line.as_bytes()), none).is_err());
        }
    }

    #[test]
    fn supplied_vectors_hold_only_the_parsed_command() {
        let stale = Bytes::from_static(b"stale");
        let cases: [(&[u8], &[&[u8]]); 4] = [
            (b"*2\r\n$3\r\nGET\r\n$0\r\n\r\n", &[b"GET", b""]),
            (
                b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\n\0\xff\r\n",
                &[b"SET", b"k", b"\0\xff"],
            ),
            (b"ECHO \0\xff\r\n", &[b"ECHO", b"\0\xff"]),
            (b"*0\r\n", &[]),
        ];
        // Reused vectors still holding arguments, one too small for most of
        // these commands, and an empty one.
        for supplied in [vec![stale.clone(); 3], vec![stale], Vec::new()] {
            for (request, expected) in cases {
                let mut input = BytesMut::from(request);
                let supplied = supplied.clone();
                let args = parse(&mut input, |count| {
                    assert_eq!(count, expected.len(), "{request:?}");
                    supplied
                })
                .unwrap()
                .unwrap();
                assert_eq!(args, expected, "{request:?}");
                assert!(input.is_empty());
            }
        }
    }

    #[test]
    fn encodes_binary_values_integer_extremes_and_both_null_kinds() {
        let response = Resp::Array(Some(vec![
            Resp::Integer(i64::MIN),
            Resp::Integer(i64::MAX),
            Resp::Integer(0),
            Resp::Bulk(Some(Bytes::from_static(b"\0\r\n\xff"))),
            Resp::Bulk(None),
            Resp::Array(None),
            Resp::Array(Some(vec![])),
        ]));
        for (version, expected) in [
            (Version::Resp2, &b"*7\r\n:-9223372036854775808\r\n:9223372036854775807\r\n:0\r\n$4\r\n\0\r\n\xff\r\n$-1\r\n*-1\r\n*0\r\n"[..]),
            (Version::Resp3, &b"*7\r\n:-9223372036854775808\r\n:9223372036854775807\r\n:0\r\n$4\r\n\0\r\n\xff\r\n_\r\n_\r\n*0\r\n"[..]),
        ] {
            let mut output = BytesMut::new();
            response.encode(&mut output, version);
            assert_eq!(&output[..], expected);
        }
    }

    #[test]
    fn encodes_integers_at_every_digit_width() {
        let mut magnitudes = vec![0_u64, i64::MAX as u64, i64::MAX as u64 + 1, u64::MAX];
        let mut power = 1_u64;
        loop {
            magnitudes.extend([power - 1, power, power + 1]);
            match power.checked_mul(10) {
                Some(next) => power = next,
                None => break,
            }
        }
        for magnitude in magnitudes {
            let digits = magnitude.to_string();
            let encoded = |response: Resp, version| {
                let mut output = BytesMut::new();
                response.encode(&mut output, version);
                String::from_utf8(output.to_vec()).unwrap()
            };
            let (resp2, resp3) = if magnitude <= i64::MAX as u64 {
                (format!(":{digits}\r\n"), format!(":{digits}\r\n"))
            } else {
                (
                    format!("${}\r\n{digits}\r\n", digits.len()),
                    format!("({digits}\r\n"),
                )
            };
            assert_eq!(encoded(Resp::Unsigned(magnitude), Version::Resp2), resp2);
            assert_eq!(encoded(Resp::Unsigned(magnitude), Version::Resp3), resp3);
            let Ok(value) = i64::try_from(magnitude) else {
                continue;
            };
            for value in [value, -value] {
                let spelling = value.to_string();
                assert_eq!(Decimal::signed(value).as_bytes(), spelling.as_bytes());
                for version in [Version::Resp2, Version::Resp3] {
                    assert_eq!(
                        encoded(Resp::Integer(value), version),
                        format!(":{spelling}\r\n")
                    );
                }
            }
        }
        assert_eq!(
            Decimal::signed(i64::MIN).as_bytes(),
            b"-9223372036854775808"
        );
        let mut output = BytesMut::new();
        encode_length(b'*', 1_000_000, &mut output);
        assert_eq!(&output[..], b"*1000000\r\n");
    }

    #[test]
    fn encodes_nested_maps_sets_and_scored_pairs_for_both_versions() {
        let response = Resp::Map(vec![
            (
                Resp::Bulk(Some(Bytes::from_static(b"members"))),
                Resp::Set(vec![
                    Resp::Bulk(Some(Bytes::from_static(b"\0\xff"))),
                    Resp::Map(vec![(Resp::Integer(1), Resp::Bulk(None))]),
                ]),
            ),
            (
                Resp::Simple("scores"),
                Resp::Scored(vec![
                    (Bytes::from_static(b"a\r\n"), 1.25),
                    (Bytes::new(), f64::NEG_INFINITY),
                ]),
            ),
        ]);
        for (version, expected) in [
            (Version::Resp2, &b"*4\r\n$7\r\nmembers\r\n*2\r\n$2\r\n\0\xff\r\n*2\r\n:1\r\n$-1\r\n+scores\r\n*4\r\n$3\r\na\r\n\r\n$4\r\n1.25\r\n$0\r\n\r\n$4\r\n-inf\r\n"[..]),
            (Version::Resp3, &b"%2\r\n$7\r\nmembers\r\n~2\r\n$2\r\n\0\xff\r\n%1\r\n:1\r\n_\r\n+scores\r\n*2\r\n*2\r\n$3\r\na\r\n\r\n,1.25\r\n*2\r\n$0\r\n\r\n,-inf\r\n"[..]),
        ] {
            let mut output = BytesMut::new();
            response.encode(&mut output, version);
            assert_eq!(&output[..], expected);
        }
    }

    #[test]
    fn encodes_empty_logical_aggregates() {
        let response = Resp::Array(Some(vec![
            Resp::Map(Vec::new()),
            Resp::Set(Vec::new()),
            Resp::Scored(Vec::new()),
        ]));
        for (version, expected) in [
            (Version::Resp2, &b"*3\r\n*0\r\n*0\r\n*0\r\n"[..]),
            (Version::Resp3, &b"*3\r\n%0\r\n~0\r\n*0\r\n"[..]),
        ] {
            let mut output = BytesMut::new();
            response.encode(&mut output, version);
            assert_eq!(&output[..], expected);
        }
    }

    #[test]
    fn encodes_finite_and_special_doubles_without_integer_suffixes() {
        for (value, text) in [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (-42.5, "-42.5"),
            (1.0e100, "1e100"),
            (1.0e-100, "1e-100"),
            (f64::INFINITY, "inf"),
            (f64::NEG_INFINITY, "-inf"),
            (f64::NAN, "nan"),
            (-f64::NAN, "nan"),
        ] {
            for version in [Version::Resp2, Version::Resp3] {
                let mut output = BytesMut::new();
                Resp::Double(value).encode(&mut output, version);
                let expected = match version {
                    Version::Resp2 => format!("${}\r\n{text}\r\n", text.len()),
                    Version::Resp3 => format!(",{text}\r\n"),
                };
                assert_eq!(&output[..], expected.as_bytes());
            }
        }
    }

    #[test]
    fn statuses_and_errors_cannot_inject_frames_but_preserve_other_bytes() {
        let response = Resp::Array(Some(vec![
            Resp::Simple("OK\r\n:1"),
            Resp::Status(Bytes::from_static(b"dynamic\r\n+injected\0\xff")),
            Resp::Error("ERR bad\r\n-input".into()),
        ]));
        for version in [Version::Resp2, Version::Resp3] {
            let mut output = BytesMut::new();
            response.encode(&mut output, version);
            assert_eq!(
                &output[..],
                b"*3\r\n+OK  :1\r\n+dynamic  +injected\0\xff\r\n-ERR bad  -input\r\n"
            );
        }
    }
    #[test]
    fn extended_semantic_replies_preserve_resp_versions() {
        let cases = [
            (
                Resp::Boolean(true),
                b":1\r\n".as_slice(),
                b"#t\r\n".as_slice(),
            ),
            (
                Resp::Boolean(false),
                b":0\r\n".as_slice(),
                b"#f\r\n".as_slice(),
            ),
            (
                Resp::Unsigned(u64::MAX),
                b"$20\r\n18446744073709551615\r\n".as_slice(),
                b"(18446744073709551615\r\n".as_slice(),
            ),
            (
                Resp::InfoField(Bytes::from_static(b"Capacity"), Box::new(Resp::Integer(9))),
                b"*1\r\n:9\r\n".as_slice(),
                b"%1\r\n$8\r\nCapacity\r\n:9\r\n".as_slice(),
            ),
            (
                Resp::VectorResults(vec![(Bytes::from_static(b"a"), Some(0.5), Some(None))]),
                b"*3\r\n$1\r\na\r\n$3\r\n0.5\r\n$-1\r\n".as_slice(),
                b"%1\r\n$1\r\na\r\n*2\r\n,0.5\r\n_\r\n".as_slice(),
            ),
            (
                Resp::VectorResults(vec![(Bytes::from_static(b"a"), None, Some(None))]),
                b"*2\r\n$1\r\na\r\n$-1\r\n".as_slice(),
                b"%1\r\n$1\r\na\r\n_\r\n".as_slice(),
            ),
        ];
        for (reply, resp2, resp3) in cases {
            for (version, expected) in [(Version::Resp2, resp2), (Version::Resp3, resp3)] {
                let mut bytes = BytesMut::new();
                reply.encode(&mut bytes, version);
                assert_eq!(&bytes[..], expected);
            }
        }
    }
}
