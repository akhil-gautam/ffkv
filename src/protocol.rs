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
                    out.extend_from_slice(b":");
                    encode_unsigned(*value, out);
                    out.extend_from_slice(b"\r\n");
                } else if version == Version::Resp3 {
                    out.extend_from_slice(b"(");
                    encode_unsigned(*value, out);
                    out.extend_from_slice(b"\r\n");
                } else {
                    let digits = if *value == 0 {
                        1
                    } else {
                        value.ilog10() as usize + 1
                    };
                    encode_length(b'$', digits, out);
                    encode_unsigned(*value, out);
                    out.extend_from_slice(b"\r\n");
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
            Self::Integer(value) => {
                out.extend_from_slice(b":");
                if *value < 0 {
                    out.extend_from_slice(b"-");
                }
                encode_unsigned(value.unsigned_abs(), out);
                out.extend_from_slice(b"\r\n");
            }
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

fn encode_length(marker: u8, length: usize, out: &mut BytesMut) {
    out.extend_from_slice(&[marker]);
    encode_unsigned(length as u64, out);
    out.extend_from_slice(b"\r\n");
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

fn encode_unsigned(mut value: u64, out: &mut BytesMut) {
    let mut digits = [0_u8; 20];
    let mut start = digits.len();
    loop {
        start -= 1;
        digits[start] = b'0' + (value % 10) as u8;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    out.extend_from_slice(&digits[start..]);
}

/// Parse exactly one command, leaving any pipelined commands buffered.
/// Incomplete input is never consumed. Protocol errors require connection closure.
/// Bulk argument bytes share the completed command's allocation.
pub fn parse(input: &mut BytesMut) -> Result<Option<Vec<Bytes>>, String> {
    if input.is_empty() {
        return Ok(None);
    }
    if input[0] != b'*' {
        return parse_inline(input);
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
    let mut args = Vec::with_capacity(count);
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

fn parse_inline(input: &mut BytesMut) -> Result<Option<Vec<Bytes>>, String> {
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
    let mut args = Vec::with_capacity(count);
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

fn read_length(
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

    #[test]
    fn fragmentation_at_every_split_preserves_incomplete_input() {
        let command = b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$6\r\n\0\r\n\xffxy\r\n";
        for split in 0..command.len() {
            let mut input = BytesMut::from(&command[..split]);
            let before = input.clone();
            assert_eq!(parse(&mut input).unwrap(), None, "split {split}");
            assert_eq!(input, before, "split {split}");
            input.extend_from_slice(&command[split..]);
            assert_eq!(
                parse(&mut input).unwrap().unwrap(),
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
        let args = parse(&mut input).unwrap().unwrap();
        assert_eq!(&args[1][..], b"\r\n\0\xff");
        assert_eq!(&input[..], b"PING\r\n");
        assert_eq!(
            parse(&mut input).unwrap().unwrap(),
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
            assert!(parse(&mut input).is_err(), "{case:?}");
            assert_eq!(input, before);
        }
    }

    #[test]
    fn bounds_whole_command_and_inline_lines() {
        let mut input = BytesMut::from(&b"*1\r\n$8388593\r\n"[..]);
        input.resize(MAX_COMMAND_BYTES, b'x');
        // The advertised payload plus its framing cannot fit the command limit.
        assert!(parse(&mut input).is_err());
        let mut input = BytesMut::from(&b"*1\r\n$8388592\r\n"[..]);
        input.resize(MAX_COMMAND_BYTES - 2, b'x');
        input.extend_from_slice(b"\r\nPING\r\n");
        assert_eq!(parse(&mut input).unwrap().unwrap()[0].len(), 8_388_592);
        assert_eq!(&input[..], b"PING\r\n");
        let mut input = BytesMut::new();
        input.resize(MAX_COMMAND_BYTES, b'x');
        assert!(parse(&mut input).is_err());
        let mut input = BytesMut::from(&b"*1024\r\n"[..]);
        for _ in 0..1024 {
            input.extend_from_slice(b"$0\r\n\r\n");
        }
        assert_eq!(parse(&mut input).unwrap().unwrap().len(), 1024);
        let mut input = BytesMut::new();
        for _ in 0..1025 {
            input.extend_from_slice(b"x ");
        }
        input.extend_from_slice(b"\r\n");
        assert!(parse(&mut input).is_err());
    }

    #[test]
    fn inline_fragmentation_and_restricted_syntax() {
        for split in 0..b"PING\r\n".len() {
            let mut input = BytesMut::from(&b"PING\r\n"[..split]);
            assert_eq!(parse(&mut input).unwrap(), None);
            input.extend_from_slice(&b"PING\r\n"[split..]);
            assert_eq!(
                parse(&mut input).unwrap().unwrap(),
                vec![Bytes::from_static(b"PING")]
            );
        }
        let mut input = BytesMut::from(&b"  SET\tk  value \r\n"[..]);
        assert_eq!(parse(&mut input).unwrap().unwrap().len(), 3);
        for line in [
            "SET k 'value'\r\n",
            "SET k \"value\"\r\n",
            "SET k a\\ b\r\n",
        ] {
            assert!(parse(&mut BytesMut::from(line.as_bytes())).is_err());
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
