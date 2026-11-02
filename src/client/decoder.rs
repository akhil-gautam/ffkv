use super::{Frame, invalid};
use bytes::{Buf, Bytes, BytesMut};
use std::{io, mem};

const MAX_BYTES: usize = 64 * 1024 * 1024;
const MAX_NODES: usize = 1_000_000;
const MAX_DEPTH: usize = 64;
// Includes spare Vec capacity and aggregate bookkeeping, not just payload bytes.
const NODE_COST: usize = mem::size_of::<Frame>() * 2 + mem::size_of::<Container>();

pub(super) struct Decoder {
    input: BytesMut,
    scanned: usize,
    body: Option<(u8, usize)>,
    stack: Vec<Container>,
    wire: usize,
    logical: usize,
    nodes: usize,
    outstanding: usize,
}

struct Container {
    kind: u8,
    remaining: usize,
    values: Vec<Frame>,
    pairs: Vec<(Frame, Frame)>,
    key: Option<Frame>,
}

impl Container {
    fn new(kind: u8, remaining: usize) -> Self {
        Self {
            kind,
            remaining,
            values: Vec::new(),
            pairs: Vec::new(),
            key: None,
        }
    }

    fn accept(&mut self, frame: Frame) -> Option<Frame> {
        self.remaining -= 1;
        if self.kind == b'|' && self.remaining == 0 {
            return Some(Frame::Attribute(
                mem::take(&mut self.pairs),
                Box::new(frame),
            ));
        }
        if matches!(self.kind, b'%' | b'|') {
            if let Some(key) = self.key.take() {
                self.pairs.push((key, frame));
            } else {
                self.key = Some(frame);
            }
        } else {
            self.values.push(frame);
        }
        if self.remaining == 0 {
            Some(self.finish())
        } else {
            None
        }
    }

    fn finish(&mut self) -> Frame {
        match self.kind {
            b'*' => Frame::Array(mem::take(&mut self.values)),
            b'~' => Frame::Set(mem::take(&mut self.values)),
            b'>' => Frame::Push(mem::take(&mut self.values)),
            b'%' => Frame::Map(mem::take(&mut self.pairs)),
            _ => unreachable!("attributes always include a following value"),
        }
    }
}

impl Decoder {
    pub(super) fn new() -> Self {
        Self {
            input: BytesMut::new(),
            scanned: 1,
            body: None,
            stack: Vec::new(),
            wire: 0,
            logical: 0,
            nodes: 0,
            outstanding: 1,
        }
    }

    pub(super) fn feed(&mut self, bytes: &[u8]) {
        self.input.extend_from_slice(bytes);
    }

    /// Preserve read-ahead when a negotiated connection enters raw mode.
    pub(super) fn take_raw(&mut self, max_bytes: usize) -> io::Result<Bytes> {
        if self.wire != 0 || self.body.is_some() || !self.stack.is_empty() || self.scanned != 1 {
            return Err(invalid(
                "cannot enter raw mode inside a partially decoded RESP frame",
            ));
        }
        let count = max_bytes.min(self.input.len());
        Ok(self.input.split_to(count).freeze())
    }

    /// Called only after decode reports incomplete, so every buffered byte
    /// belongs to this reply. Never allocate a read beyond its wire budget.
    pub(super) fn read_limit(&self) -> io::Result<usize> {
        let remaining = MAX_BYTES
            .checked_sub(self.wire)
            .and_then(|n| n.checked_sub(self.input.len()))
            .filter(|n| *n != 0)
            .ok_or_else(|| invalid("RESP reply exceeds 64 MiB wire limit"))?;
        Ok(remaining.min(8192))
    }

    /// Consumes every complete token exactly once. Incomplete line scan offsets,
    /// body lengths, and aggregate construction survive cancellation of a read.
    pub(super) fn decode(&mut self) -> io::Result<Option<Frame>> {
        loop {
            if let Some((marker, length)) = self.body {
                if self.input.len() < length + 2 {
                    return Ok(None);
                }
                if &self.input[length..length + 2] != b"\r\n" {
                    return Err(invalid("bulk data lacks its CRLF terminator"));
                }
                // Own the exact payload rather than retaining uncharged input capacity.
                let payload = Bytes::copy_from_slice(&self.input[..length]);
                self.input.advance(length + 2);
                self.charge_wire(length + 2)?;
                self.body = None;
                let frame = match marker {
                    b'$' => Frame::Bulk(payload),
                    b'!' => Frame::Error(payload),
                    b'=' => {
                        if payload.len() < 4 || payload[3] != b':' {
                            return Err(invalid("invalid verbatim string format prefix"));
                        }
                        let format = [payload[0], payload[1], payload[2]];
                        Frame::Verbatim {
                            format,
                            data: payload.slice(4..),
                        }
                    }
                    _ => unreachable!(),
                };
                if let Some(frame) = self.emit(frame) {
                    return Ok(Some(frame));
                }
                continue;
            }
            if self.input.is_empty() {
                return Ok(None);
            }
            let marker = self.input[0];
            if !matches!(
                marker,
                b'+' | b'-'
                    | b':'
                    | b'$'
                    | b'*'
                    | b'_'
                    | b'#'
                    | b','
                    | b'('
                    | b'!'
                    | b'='
                    | b'%'
                    | b'~'
                    | b'>'
                    | b'|'
            ) {
                return Err(invalid("unsupported RESP type marker"));
            }
            let mut end = None;
            let mut cursor = self.scanned;
            while cursor < self.input.len() {
                match self.input[cursor] {
                    b'\n' => return Err(invalid("RESP line contains a bare LF")),
                    b'\r' => {
                        if cursor + 1 == self.input.len() {
                            break;
                        }
                        if self.input[cursor + 1] != b'\n' {
                            return Err(invalid("RESP line contains a bare CR"));
                        }
                        end = Some(cursor);
                        break;
                    }
                    _ => cursor += 1,
                }
            }
            self.scanned = cursor;
            let Some(end) = end else {
                self.check_wire(self.input.len())?;
                return Ok(None);
            };
            self.charge_wire(end + 2)?;
            self.charge_node()?;
            let line = self.input.split_to(end + 2).freeze();
            let value = &line[1..end];
            self.scanned = 1;
            let frame = match marker {
                b'+' | b'-' | b'(' => {
                    if marker == b'(' && !decimal(value) {
                        return Err(invalid("invalid RESP big number"));
                    }
                    self.charge_logical(value.len())?;
                    let value = Bytes::copy_from_slice(value);
                    match marker {
                        b'+' => Frame::Simple(value),
                        b'-' => Frame::Error(value),
                        _ => Frame::BigNumber(value),
                    }
                }
                b':' => Frame::Integer(integer(value)?),
                b',' => Frame::Double(double(value)?),
                b'_' => {
                    if !value.is_empty() {
                        return Err(invalid("invalid RESP null"));
                    }
                    Frame::Null
                }
                b'#' => match value {
                    b"t" => Frame::Boolean(true),
                    b"f" => Frame::Boolean(false),
                    _ => return Err(invalid("invalid RESP boolean")),
                },
                b'$' | b'!' | b'=' => {
                    if value == b"-1" && marker == b'$' {
                        Frame::Null
                    } else {
                        let length = length(value)?;
                        if marker == b'=' && length < 4 {
                            return Err(invalid(
                                "verbatim string is shorter than its format prefix",
                            ));
                        }
                        self.check_wire(
                            length
                                .checked_add(2)
                                .ok_or_else(|| invalid("RESP bulk length overflow"))?,
                        )?;
                        self.charge_logical(length)?;
                        self.body = Some((marker, length));
                        continue;
                    }
                }
                b'*' | b'%' | b'~' | b'>' | b'|' => {
                    if marker == b'*' && value == b"-1" {
                        Frame::Null
                    } else {
                        let count = length(value)?;
                        let children = if matches!(marker, b'%' | b'|') {
                            count
                                .checked_mul(2)
                                .and_then(|n| n.checked_add(usize::from(marker == b'|')))
                        } else {
                            Some(count)
                        }
                        .ok_or_else(|| invalid("RESP container length overflow"))?;
                        // The current root/child also occupies a nesting level.
                        if self.stack.len() >= MAX_DEPTH {
                            return Err(invalid("RESP nesting exceeds 64 levels"));
                        }
                        self.reserve_nodes(children)?;
                        let mut container = Container::new(marker, children);
                        if children == 0 {
                            container.finish()
                        } else {
                            self.stack.push(container);
                            continue;
                        }
                    }
                }
                _ => unreachable!(),
            };
            if let Some(frame) = self.emit(frame) {
                return Ok(Some(frame));
            }
        }
    }

    fn emit(&mut self, mut frame: Frame) -> Option<Frame> {
        while let Some(container) = self.stack.last_mut() {
            frame = container.accept(frame)?;
            self.stack.pop();
        }
        self.wire = 0;
        self.logical = 0;
        self.nodes = 0;
        self.outstanding = 1;
        Some(frame)
    }

    fn check_wire(&self, additional: usize) -> io::Result<()> {
        if self
            .wire
            .checked_add(additional)
            .is_none_or(|n| n > MAX_BYTES)
        {
            Err(invalid("RESP reply exceeds 64 MiB wire limit"))
        } else {
            Ok(())
        }
    }

    fn charge_wire(&mut self, additional: usize) -> io::Result<()> {
        self.check_wire(additional)?;
        self.wire += additional;
        Ok(())
    }

    fn charge_logical(&mut self, additional: usize) -> io::Result<()> {
        let total = self.logical.checked_add(additional).and_then(|n| {
            self.outstanding
                .checked_mul(NODE_COST)
                .and_then(|pending| n.checked_add(pending))
        });
        if total.is_none_or(|n| n > MAX_BYTES) {
            return Err(invalid(
                "RESP reply exceeds 64 MiB logical allocation limit",
            ));
        }
        self.logical += additional;
        Ok(())
    }

    fn charge_node(&mut self) -> io::Result<()> {
        self.outstanding = self
            .outstanding
            .checked_sub(1)
            .ok_or_else(|| invalid("unexpected RESP value"))?;
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(invalid("RESP reply has too many values"));
        }
        self.charge_logical(NODE_COST)
    }

    fn reserve_nodes(&mut self, count: usize) -> io::Result<()> {
        self.outstanding = self
            .outstanding
            .checked_add(count)
            .ok_or_else(|| invalid("RESP value count overflow"))?;
        if self
            .nodes
            .checked_add(self.outstanding)
            .is_none_or(|n| n > MAX_NODES)
        {
            return Err(invalid("RESP reply has too many values"));
        }
        self.charge_logical(0)
    }
}

fn decimal(bytes: &[u8]) -> bool {
    let digits = match bytes.first() {
        Some(b'+' | b'-') => &bytes[1..],
        _ => bytes,
    };
    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

fn integer(bytes: &[u8]) -> io::Result<i64> {
    if !decimal(bytes) {
        return Err(invalid("invalid RESP integer"));
    }
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid("RESP integer is outside the signed 64-bit range"))
}

fn length(bytes: &[u8]) -> io::Result<usize> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return Err(invalid(
            "invalid RESP length (streamed frames are unsupported)",
        ));
    }
    let mut number = 0usize;
    for &digit in bytes {
        number = number
            .checked_mul(10)
            .and_then(|n| n.checked_add((digit - b'0') as usize))
            .ok_or_else(|| invalid("RESP length overflow"))?;
        if number > MAX_BYTES {
            return Err(invalid("RESP length exceeds reply limit"));
        }
    }
    Ok(number)
}

fn double(bytes: &[u8]) -> io::Result<f64> {
    match bytes {
        b"inf" => return Ok(f64::INFINITY),
        b"-inf" => return Ok(f64::NEG_INFINITY),
        b"nan" => return Ok(f64::NAN),
        _ => (),
    }
    let mut i = usize::from(matches!(bytes.first(), Some(b'+' | b'-')));
    let mut digits = 0;
    while bytes.get(i).is_some_and(u8::is_ascii_digit) {
        i += 1;
        digits += 1;
    }
    if bytes.get(i) == Some(&b'.') {
        i += 1;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
            digits += 1;
        }
    }
    if digits == 0 {
        return Err(invalid("invalid RESP double"));
    }
    if matches!(bytes.get(i), Some(b'e' | b'E')) {
        i += 1;
        if matches!(bytes.get(i), Some(b'+' | b'-')) {
            i += 1;
        }
        let start = i;
        while bytes.get(i).is_some_and(u8::is_ascii_digit) {
            i += 1;
        }
        if i == start {
            return Err(invalid("invalid RESP double exponent"));
        }
    }
    if i != bytes.len() {
        return Err(invalid("invalid RESP double"));
    }
    let number = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse::<f64>().ok())
        .ok_or_else(|| invalid("invalid RESP double"))?;
    if !number.is_finite() {
        return Err(invalid("RESP double overflow"));
    }
    Ok(number)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(bytes: &[u8]) -> io::Result<Option<Frame>> {
        let mut decoder = Decoder::new();
        decoder.feed(bytes);
        decoder.decode()
    }

    #[test]
    fn raw_handoff_preserves_negotiation_read_ahead_in_bounded_chunks() {
        let mut decoder = Decoder::new();
        decoder.feed(b"+OK\r\n\0\xffreplication\r\n");
        assert_eq!(
            decoder.decode().unwrap(),
            Some(Frame::Simple(Bytes::from_static(b"OK")))
        );
        assert_eq!(decoder.take_raw(2).unwrap(), Bytes::from_static(b"\0\xff"));
        assert_eq!(
            decoder.take_raw(4096).unwrap(),
            Bytes::from_static(b"replication\r\n")
        );
        assert!(decoder.take_raw(4096).unwrap().is_empty());
    }

    #[test]
    fn raw_handoff_rejects_partially_consumed_resp_state() {
        for prefix in [b"$5\r\nab".as_slice(), b"*2\r\n+OK\r\n", b"+part"] {
            let mut decoder = Decoder::new();
            decoder.feed(prefix);
            assert!(decoder.decode().unwrap().is_none());
            assert!(decoder.take_raw(4096).is_err());
        }
    }

    #[test]
    fn every_fragment_boundary_preserves_binary_and_pipeline() {
        let wire = b"*3\r\n$5\r\na\0\r\nb\r\n:-9223372036854775808\r\n*2\r\n+OK\r\n$-1\r\n:42\r\n";
        let expected = Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"a\0\r\nb")),
            Frame::Integer(i64::MIN),
            Frame::Array(vec![Frame::Simple(Bytes::from_static(b"OK")), Frame::Null]),
        ]);
        for boundary in 0..wire.len() - 5 {
            let mut decoder = Decoder::new();
            decoder.feed(&wire[..boundary]);
            assert!(decoder.decode().unwrap().is_none(), "boundary {boundary}");
            decoder.feed(&wire[boundary..]);
            assert_eq!(decoder.decode().unwrap(), Some(expected_for_binary()));
            assert_eq!(decoder.decode().unwrap(), Some(Frame::Integer(42)));
            assert!(decoder.decode().unwrap().is_none());
        }
        let mut decoder = Decoder::new();
        let mut replies = Vec::new();
        for &byte in wire {
            decoder.feed(&[byte]);
            if let Some(frame) = decoder.decode().unwrap() {
                replies.push(frame);
            }
        }
        assert_eq!(replies, vec![expected, Frame::Integer(42)]);
    }

    fn expected_for_binary() -> Frame {
        Frame::Array(vec![
            Frame::Bulk(Bytes::from_static(b"a\0\r\nb")),
            Frame::Integer(i64::MIN),
            Frame::Array(vec![Frame::Simple(Bytes::from_static(b"OK")), Frame::Null]),
        ])
    }

    #[test]
    fn resp3_types_and_attributed_pushes() {
        let wire = b"|1\r\n+source\r\n+test\r\n>2\r\n+message\r\n%4\r\n+set\r\n~2\r\n#t\r\n#f\r\n+double\r\n,1.25e2\r\n+big\r\n(123456789012345678901234567890\r\n+verbatim\r\n=7\r\ntxt:a\0b\r\n";
        let frame = decode(wire).unwrap().unwrap();
        assert!(frame.is_push());
        assert!(!frame.has_error());
        let Frame::Attribute(attributes, frame) = frame else {
            panic!("attribute expected")
        };
        assert_eq!(attributes.len(), 1);
        let Frame::Push(values) = *frame else {
            panic!("push expected")
        };
        let Frame::Map(pairs) = &values[1] else {
            panic!("map expected")
        };
        assert_eq!(
            pairs[0].1,
            Frame::Set(vec![Frame::Boolean(true), Frame::Boolean(false)])
        );
        assert_eq!(pairs[1].1, Frame::Double(125.0));
        assert_eq!(
            pairs[2].1,
            Frame::BigNumber(Bytes::from_static(b"123456789012345678901234567890"))
        );
        assert_eq!(
            pairs[3].1,
            Frame::Verbatim {
                format: *b"txt",
                data: Bytes::from_static(b"a\0b")
            }
        );
        let frame = decode(b"*2\r\n+OK\r\n!3\r\nbad\r\n").unwrap().unwrap();
        assert!(frame.has_error());
        assert!(!frame.is_push());
        assert_eq!(decode(b"_\r\n").unwrap(), Some(Frame::Null));
        assert_eq!(decode(b"*-1\r\n").unwrap(), Some(Frame::Null));
        assert_eq!(decode(b"%0\r\n").unwrap(), Some(Frame::Map(Vec::new())));
    }

    #[test]
    fn malformed_numbers_lengths_and_framing_are_rejected() {
        let invalid: &[&[u8]] = &[
            b":9223372036854775808\r\n",
            b":-9223372036854775809\r\n",
            b":\r\n",
            b": 1\r\n",
            b":1x\r\n",
            b"(+\r\n",
            b"(12.3\r\n",
            b",1e\r\n",
            b",NaN\r\n",
            b",1e999\r\n",
            b", 1\r\n",
            b"#yes\r\n",
            b"_x\r\n",
            b"$-2\r\n",
            b"!-1\r\n",
            b"%-1\r\n",
            b"$+1\r\n",
            b"$?\r\n",
            b"*9999999999999999999999999\r\n",
            b"$67108864\r\n",
            b"*1000001\r\n",
            b"%1000000\r\n",
            b"$1\r\naXX",
            b"+bad\n",
            b"+bad\rX",
            b"=3\r\nabc\r\n",
            b"=4\r\ntxtx\r\n",
            b"?\r\n",
        ];
        for wire in invalid {
            assert!(decode(wire).is_err(), "accepted {wire:?}");
        }
    }

    #[test]
    fn special_doubles_and_empty_attributes_keep_their_semantics() {
        assert_eq!(
            decode(b",inf\r\n").unwrap(),
            Some(Frame::Double(f64::INFINITY))
        );
        assert_eq!(
            decode(b",-inf\r\n").unwrap(),
            Some(Frame::Double(f64::NEG_INFINITY))
        );
        let Some(Frame::Double(value)) = decode(b",nan\r\n").unwrap() else {
            panic!("double expected")
        };
        assert!(value.is_nan());
        assert_eq!(
            decode(b"|0\r\n+OK\r\n").unwrap(),
            Some(Frame::Attribute(
                Vec::new(),
                Box::new(Frame::Simple(Bytes::from_static(b"OK"))),
            ))
        );
        let frame = decode(b"|0\r\n|0\r\n>1\r\n+event\r\n").unwrap().unwrap();
        assert!(frame.is_push());
        let frame = decode(b"%1\r\n+result\r\n*1\r\n-ERR failed\r\n")
            .unwrap()
            .unwrap();
        assert!(frame.has_error());
    }

    #[test]
    fn nesting_and_declared_container_memory_are_bounded() {
        let mut valid = b"*1\r\n".repeat(MAX_DEPTH);
        valid.extend_from_slice(b":0\r\n");
        assert!(decode(&valid).unwrap().is_some());
        let mut invalid = b"*1\r\n".repeat(MAX_DEPTH + 1);
        invalid.extend_from_slice(b":0\r\n");
        assert!(decode(&invalid).is_err());
        assert!(decode(b"*1000000\r\n").is_err());
        let mut decoder = Decoder::new();
        decoder.feed(b"*2\r\n$33554420\r\n");
        // Declared payloads consume the budget before reading/allocating a body.
        assert!(decoder.decode().unwrap().is_none());
        assert_eq!(decoder.body, Some((b'$', 33554420)));
        assert!(decoder.logical < MAX_BYTES);
        assert!(decoder.input.capacity() < 1024);
        let mut decoder = Decoder::new();
        decoder.wire = MAX_BYTES - 4;
        decoder.feed(b"+OK\r");
        assert!(decoder.decode().unwrap().is_none());
        assert!(decoder.read_limit().is_err());
    }

    #[test]
    fn interrupted_decode_retains_line_body_and_container_state() {
        let mut decoder = Decoder::new();
        decoder.feed(b"*2\r\n$5\r\nhe");
        assert!(decoder.decode().unwrap().is_none());
        assert_eq!(decoder.body, Some((b'$', 5)));
        assert_eq!(decoder.stack.len(), 1);
        for _ in 0..8 {
            assert!(decoder.decode().unwrap().is_none());
        }
        decoder.feed(b"llo\r\n+wor");
        assert!(decoder.decode().unwrap().is_none());
        let scanned = decoder.scanned;
        for _ in 0..8 {
            assert!(decoder.decode().unwrap().is_none());
            assert_eq!(decoder.scanned, scanned);
        }
        decoder.feed(b"ld\r\n");
        assert_eq!(
            decoder.decode().unwrap(),
            Some(Frame::Array(vec![
                Frame::Bulk(Bytes::from_static(b"hello")),
                Frame::Simple(Bytes::from_static(b"world")),
            ]))
        );
    }
}
