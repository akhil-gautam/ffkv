//! Independently implemented Redis RDB wire primitives and DUMP containers.
//!
//! Standard objects are written using portable, noncompact RDB encodings. Compact
//! formats are accepted on input. Extension objects use the explicit `ffkvdata_`
//! module identity; they are not standard Redis values without that module.
//! Format references: Redis 8.10 src/rdb.h, src/ziplist.c (format description),
//! and https://github.com/antirez/listpack/blob/master/listpack.md.

use bytes::Bytes;
use std::{
    collections::{HashMap, HashSet},
    io::{self, Read, Write},
    mem::size_of,
};

/// Redis 8.10's RDB version (src/rdb.h); older DUMP versions remain readable.
pub const VERSION: u16 = 15;
pub const MODULE_VERSION: u64 = 1;
pub const OPCODE_HASH_TEMPLATE: u8 = 242;
const MODULE_NAME: &[u8; 9] = b"ffkvdata_";

/// One key's DUMP payload as MIGRATE transfers it.
#[derive(Debug)]
pub struct MigrateItem {
    pub key: Bytes,
    pub dump: Bytes,
    /// Absolute Unix expiration in milliseconds; None means persistent.
    pub expires_at: Option<i64>,
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
fn limit() -> io::Error {
    invalid("RDB object exceeds configured byte limit")
}

const fn crc_table() -> [u64; 256] {
    let mut table = [0; 256];
    let mut i = 0;
    while i < 256 {
        let mut value = i as u64;
        let mut bit = 0;
        while bit < 8 {
            value = (value >> 1)
                ^ if value & 1 != 0 {
                    0x95ac9329ac4bc9b5
                } else {
                    0
                };
            bit += 1;
        }
        table[i] = value;
        i += 1;
    }
    table
}
const CRC_TABLE: [u64; 256] = crc_table();

/// Redis' reflected CRC64/Jones, with caller-supplied initial state and no xorout.
pub fn crc64(mut crc: u64, bytes: &[u8]) -> u64 {
    for &byte in bytes {
        crc = CRC_TABLE[((crc as u8) ^ byte) as usize] ^ (crc >> 8);
    }
    crc
}

pub fn write_length(out: &mut impl Write, length: u64) -> io::Result<()> {
    if length < 64 {
        out.write_all(&[length as u8])
    } else if length < 16384 {
        out.write_all(&[0x40 | (length >> 8) as u8, length as u8])
    } else if length <= u32::MAX as u64 {
        out.write_all(&[0x80])?;
        out.write_all(&(length as u32).to_be_bytes())
    } else {
        out.write_all(&[0x81])?;
        out.write_all(&length.to_be_bytes())
    }
}

pub fn write_string(out: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write_length(out, bytes.len() as u64)?;
    out.write_all(bytes)
}

/// A borrowing cursor with a cumulative allocation budget. Raw reads borrow;
/// strings and container slots consume the budget, including expanded integers.
pub struct Decoder<'a> {
    data: &'a [u8],
    position: usize,
    budget: usize,
    max_bytes: usize,
}
impl<'a> Decoder<'a> {
    pub fn new(data: &'a [u8], max_bytes: usize) -> Self {
        Self {
            data,
            position: 0,
            budget: max_bytes,
            max_bytes,
        }
    }
    pub fn remaining(&self) -> usize {
        self.data.len() - self.position
    }
    pub fn position(&self) -> usize {
        self.position
    }
    pub fn byte(&mut self) -> io::Result<u8> {
        Ok(self.raw(1)?[0])
    }
    /// Borrow a wire segment, useful for snapshot metadata and fixed-width values.
    pub fn raw(&mut self, length: usize) -> io::Result<&'a [u8]> {
        if length > self.max_bytes {
            return Err(limit());
        }
        let end = self.position.checked_add(length).ok_or_else(limit)?;
        let bytes = self
            .data
            .get(self.position..end)
            .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "truncated RDB value"))?;
        self.position = end;
        Ok(bytes)
    }
    pub fn u32_le(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.raw(4)?.try_into().unwrap()))
    }
    pub fn u64_le(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.raw(8)?.try_into().unwrap()))
    }
    fn charge(&mut self, bytes: usize) -> io::Result<()> {
        self.budget = self.budget.checked_sub(bytes).ok_or_else(limit)?;
        Ok(())
    }
    fn length_header(&mut self) -> io::Result<(u64, bool)> {
        let first = self.byte()?;
        match first >> 6 {
            0 => Ok((first as u64, false)),
            1 => Ok(((((first & 63) as u64) << 8) | self.byte()? as u64, false)),
            2 => match first {
                0x80 => Ok((
                    u32::from_be_bytes(self.raw(4)?.try_into().unwrap()) as u64,
                    false,
                )),
                0x81 => Ok((u64::from_be_bytes(self.raw(8)?.try_into().unwrap()), false)),
                _ => Err(invalid("invalid RDB length tag")),
            },
            _ => Ok(((first & 63) as u64, true)),
        }
    }
    pub fn length(&mut self) -> io::Result<u64> {
        let (value, encoded) = self.length_header()?;
        if encoded {
            Err(invalid("encoded value used as RDB length"))
        } else {
            Ok(value)
        }
    }
    fn usize_length(&mut self) -> io::Result<usize> {
        usize::try_from(self.length()?).map_err(|_| limit())
    }
    fn owned(&mut self, bytes: &[u8]) -> io::Result<Bytes> {
        self.charge(bytes.len())?;
        Ok(Bytes::copy_from_slice(bytes))
    }
    fn integer(&mut self, value: i64) -> io::Result<Bytes> {
        let text = value.to_string();
        self.charge(text.len())?;
        Ok(Bytes::from(text))
    }
    pub fn string(&mut self) -> io::Result<Bytes> {
        let (value, encoded) = self.length_header()?;
        if !encoded {
            let length = usize::try_from(value).map_err(|_| limit())?;
            self.charge(length)?;
            return Ok(Bytes::copy_from_slice(self.raw(length)?));
        }
        match value {
            0 => {
                let n = self.byte()? as i8;
                self.integer(n as i64)
            }
            1 => {
                let n = i16::from_le_bytes(self.raw(2)?.try_into().unwrap());
                self.integer(n as i64)
            }
            2 => {
                let n = i32::from_le_bytes(self.raw(4)?.try_into().unwrap());
                self.integer(n as i64)
            }
            3 => {
                let compressed = self.usize_length()?;
                let expanded = self.usize_length()?;
                self.charge(expanded)?;
                let input = self.raw(compressed)?;
                Ok(Bytes::from(lzf(input, expanded)?))
            }
            _ => Err(invalid("unknown RDB string encoding")),
        }
    }
    fn vector<T>(&mut self, count: usize) -> io::Result<Vec<T>> {
        self.charge(count.checked_mul(size_of::<T>()).ok_or_else(limit)?)?;
        let mut result = Vec::new();
        result.try_reserve_exact(count).map_err(|_| limit())?;
        Ok(result)
    }
    fn push<T>(&mut self, values: &mut Vec<T>, value: T) -> io::Result<()> {
        self.charge(size_of::<T>())?;
        values.try_reserve(1).map_err(|_| limit())?;
        values.push(value);
        Ok(())
    }
}

fn lzf(input: &[u8], length: usize) -> io::Result<Vec<u8>> {
    let mut source = Decoder::new(input, input.len());
    let mut out = Vec::new();
    out.try_reserve_exact(length).map_err(|_| limit())?;
    while source.remaining() != 0 {
        let control = source.byte()?;
        if control < 32 {
            let count = control as usize + 1;
            if count > length - out.len() {
                return Err(invalid("LZF literal exceeds output size"));
            }
            out.extend_from_slice(source.raw(count)?);
        } else {
            let mut count = (control >> 5) as usize;
            if count == 7 {
                count += source.byte()? as usize;
            }
            count += 2;
            let distance = (((control & 31) as usize) << 8) + source.byte()? as usize + 1;
            if distance > out.len() || count > length - out.len() {
                return Err(invalid("invalid LZF back reference"));
            }
            for _ in 0..count {
                out.push(out[out.len() - distance]);
            }
        }
    }
    if out.len() != length {
        return Err(invalid("LZF output size mismatch"));
    }
    Ok(out)
}

#[derive(Debug, Clone, PartialEq)]
pub enum Object {
    String(Bytes),
    List(Vec<Bytes>),
    Set(Vec<Bytes>),
    Hash(Vec<(Bytes, Bytes)>),
    /// Field deadlines are absolute Unix milliseconds, not relative TTLs.
    HashWithExpiry(Vec<(Bytes, Bytes, Option<i64>)>),
    SortedSet(Vec<(Bytes, f64)>),
    Module {
        version: u64,
        payload: Bytes,
    },
}

impl Object {
    pub fn type_byte(&self) -> u8 {
        match self {
            Self::String(_) => 0,
            Self::List(_) => 1,
            Self::Set(_) => 2,
            Self::Hash(_) => 4,
            Self::HashWithExpiry(_) => 24,
            Self::SortedSet(_) => 5,
            Self::Module { .. } => 7,
        }
    }
}

fn module_id() -> u64 {
    let alphabet = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut id = 0;
    for byte in MODULE_NAME {
        id = (id << 6) | alphabet.iter().position(|b| b == byte).unwrap() as u64;
    }
    (id << 10) | MODULE_VERSION
}

/// Write type byte and object body, without DUMP version/checksum footer.
pub fn write_object(out: &mut impl Write, object: &Object) -> io::Result<()> {
    out.write_all(&[object.type_byte()])?;
    write_object_body(out, object)
}

/// Write only the body, for RDB records ordered as type, key, body.
pub fn write_object_body(out: &mut impl Write, object: &Object) -> io::Result<()> {
    match object {
        Object::String(value) => write_string(out, value),
        Object::List(values) | Object::Set(values) => {
            write_length(out, values.len() as u64)?;
            for value in values {
                write_string(out, value)?;
            }
            Ok(())
        }
        Object::Hash(values) => {
            write_length(out, values.len() as u64)?;
            for (field, value) in values {
                write_string(out, field)?;
                write_string(out, value)?;
            }
            Ok(())
        }
        Object::HashWithExpiry(values) => {
            let mut minimum = None;
            for (_, _, deadline) in values {
                if let Some(deadline) = deadline {
                    if *deadline <= 0 || *deadline as u64 > HASH_EXPIRY_MAX {
                        return Err(invalid("invalid hash field expiration deadline"));
                    }
                    minimum = Some(minimum.map_or(*deadline, |old: i64| old.min(*deadline)));
                }
            }
            let minimum = minimum.unwrap_or(0) as u64;
            out.write_all(&minimum.to_le_bytes())?;
            write_length(out, values.len() as u64)?;
            for (field, value, deadline) in values {
                let relative = deadline.map_or(0, |deadline| deadline as u64 - minimum + 1);
                write_length(out, relative)?;
                write_string(out, field)?;
                write_string(out, value)?;
            }
            Ok(())
        }
        Object::SortedSet(values) => {
            if values.iter().any(|(_, score)| score.is_nan()) {
                return Err(invalid("NaN sorted-set score"));
            }
            write_length(out, values.len() as u64)?;
            for (member, score) in values {
                write_string(out, member)?;
                out.write_all(&score.to_le_bytes())?;
            }
            Ok(())
        }
        Object::Module { version, payload } => {
            if *version != MODULE_VERSION {
                return Err(invalid("unsupported ffkv module payload version"));
            }
            write_length(out, module_id())?;
            write_length(out, 2)?;
            write_length(out, *version)?;
            write_length(out, 5)?;
            write_string(out, payload)?;
            write_length(out, 0)
        }
    }
}

/// Read one ordinary RDB length without consuming bytes belonging to the next
/// record. Encoded-string tags are rejected here, just as by `Decoder::length`.
pub fn read_stream_length(input: &mut impl Read) -> io::Result<u64> {
    let mut header = [0u8; 9];
    input.read_exact(&mut header[..1])?;
    let size = match header[0] {
        0..=63 => 1,
        64..=127 => 2,
        0x80 => 5,
        0x81 => 9,
        _ => return Err(invalid("invalid or encoded RDB length")),
    };
    input.read_exact(&mut header[1..size])?;
    Decoder::new(&header[..size], size).length()
}

/// Read exactly one string from a snapshot reader, including integer/LZF forms.
pub fn read_stream_string(input: &mut impl Read, max_bytes: usize) -> io::Result<Bytes> {
    let mut scan = StreamScan {
        input,
        bytes: Vec::new(),
        limit: max_bytes,
    };
    scan.string()?;
    Decoder::new(&scan.bytes, max_bytes).string()
}

/// Read exactly one object body; the type byte was consumed by the caller.
/// Only this object's encoded bytes are buffered, never the whole snapshot.
pub fn read_stream_object(
    input: &mut impl Read,
    type_byte: u8,
    max_bytes: usize,
) -> io::Result<Object> {
    let mut scan = StreamScan {
        input,
        bytes: Vec::new(),
        limit: max_bytes,
    };
    scan.object(type_byte)?;
    let mut decoder = Decoder::new(&scan.bytes, max_bytes);
    let object = read_object(&mut decoder, type_byte)?;
    if decoder.remaining() != 0 {
        return Err(invalid("trailing streamed object bytes"));
    }
    Ok(object)
}

/// Per-snapshot template registry. Keep one context across SELECTDB records;
/// construct a new one for the next snapshot, never for each key.
pub struct SnapshotContext {
    templates: HashMap<u64, Vec<Bytes>>,
    remaining: usize,
}
impl SnapshotContext {
    /// Bounds retained template field bytes, vector slots and registry overhead.
    pub fn new(max_bytes: usize) -> Self {
        Self {
            templates: HashMap::new(),
            remaining: max_bytes,
        }
    }
}

/// Consume an opcode242 template body: id, count, then sorted field strings.
/// The caller has already consumed `OPCODE_HASH_TEMPLATE`.
pub fn read_stream_hash_template(
    input: &mut impl Read,
    context: &mut SnapshotContext,
) -> io::Result<()> {
    let mut scan = StreamScan {
        input,
        bytes: Vec::new(),
        limit: context.remaining,
    };
    let id = scan.length()?;
    if context.templates.contains_key(&id) {
        return Err(invalid("duplicate hash template identity"));
    }
    let count = scan.count()?;
    if count == 0 {
        return Err(invalid("empty hash template"));
    }
    if count.checked_mul(size_of::<Bytes>()).ok_or_else(limit)? > context.remaining {
        return Err(limit());
    }
    scan.strings(count)?;
    let mut decoder = Decoder::new(&scan.bytes, context.remaining);
    decoder.length()?;
    let fields = strings(&mut decoder)?;
    if fields.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(invalid("hash template fields are not strictly sorted"));
    }
    let mut cost = count
        .checked_mul(size_of::<Bytes>())
        .and_then(|n| n.checked_add(64))
        .ok_or_else(limit)?;
    for field in &fields {
        cost = cost.checked_add(field.len()).ok_or_else(limit)?;
    }
    let remaining = context.remaining.checked_sub(cost).ok_or_else(limit)?;
    context.templates.try_reserve(1).map_err(|_| limit())?;
    context.templates.insert(id, fields);
    context.remaining = remaining;
    Ok(())
}

/// Snapshot-aware object reader; handles reference types30/32 in addition to
/// every self-contained object. DUMP decoding deliberately has no such context.
pub fn read_stream_object_with_context(
    input: &mut impl Read,
    type_byte: u8,
    max_bytes: usize,
    context: &SnapshotContext,
) -> io::Result<Object> {
    if !matches!(type_byte, 30 | 32) {
        return read_stream_object(input, type_byte, max_bytes);
    }
    let mut scan = StreamScan {
        input,
        bytes: Vec::new(),
        limit: max_bytes,
    };
    if type_byte == 30 {
        scan.string()?;
    } else {
        let id = scan.length()?;
        let fields = context
            .templates
            .get(&id)
            .ok_or_else(|| invalid("unknown hash template identity"))?;
        scan.strings(fields.len())?;
    }
    let mut decoder = Decoder::new(&scan.bytes, max_bytes);
    let (id, values) = if type_byte == 30 {
        let blob = decoder.string()?;
        template_values(&mut decoder, &blob)?
    } else {
        let id = decoder.length()?;
        let fields = context
            .templates
            .get(&id)
            .ok_or_else(|| invalid("unknown hash template identity"))?;
        let mut values = decoder.vector(fields.len())?;
        for _ in 0..fields.len() {
            values.push(decoder.string()?);
        }
        (id, values)
    };
    let fields = context
        .templates
        .get(&id)
        .ok_or_else(|| invalid("unknown hash template identity"))?;
    if fields.len() != values.len() {
        return Err(invalid("template-hash count mismatch"));
    }
    let mut pairs = decoder.vector(fields.len())?;
    pairs.extend(fields.iter().cloned().zip(values));
    Ok(Object::Hash(pairs))
}

fn template_values(decoder: &mut Decoder<'_>, blob: &[u8]) -> io::Result<(u64, Vec<Bytes>)> {
    // Template identity must use an integer entry, not a numeric string.
    if !matches!(blob.get(6), Some(0..=127 | 192..=223 | 241..=244)) {
        return Err(invalid("template-listpack identity is not an integer"));
    }
    let mut entries = listpack(decoder, blob)?;
    if entries.is_empty() {
        return Err(invalid("missing template-hash identity"));
    }
    let identity = entries.remove(0);
    let id = std::str::from_utf8(&identity)
        .ok()
        .and_then(|text| text.parse::<u64>().ok())
        .ok_or_else(|| invalid("negative or invalid template-hash identity"))?;
    Ok((id, entries))
}

// Framing only: semantic validation remains centralized in read_object. Every
// read is exact, so a socket/file reader need not support buffering or seeking.
struct StreamScan<'a, R> {
    input: &'a mut R,
    bytes: Vec<u8>,
    limit: usize,
}
impl<R: Read> StreamScan<'_, R> {
    fn capture(&mut self, count: usize) -> io::Result<()> {
        let start = self.bytes.len();
        let end = start
            .checked_add(count)
            .filter(|end| *end <= self.limit)
            .ok_or_else(limit)?;
        self.bytes.try_reserve(count).map_err(|_| limit())?;
        self.bytes.resize(end, 0);
        self.input.read_exact(&mut self.bytes[start..])
    }
    fn byte(&mut self) -> io::Result<u8> {
        self.capture(1)?;
        Ok(*self.bytes.last().unwrap())
    }
    fn header(&mut self) -> io::Result<(u64, bool)> {
        let start = self.bytes.len();
        let first = self.byte()?;
        match first {
            0..=63 | 192..=255 => {}
            64..=127 => self.capture(1)?,
            0x80 => self.capture(4)?,
            0x81 => self.capture(8)?,
            _ => return Err(invalid("invalid RDB length tag")),
        }
        Decoder::new(&self.bytes[start..], 9).length_header()
    }
    fn length(&mut self) -> io::Result<u64> {
        let (value, encoded) = self.header()?;
        if encoded {
            return Err(invalid("encoded value used as RDB length"));
        }
        Ok(value)
    }
    fn count(&mut self) -> io::Result<usize> {
        let count = usize::try_from(self.length()?).map_err(|_| limit())?;
        if count > self.limit {
            return Err(limit());
        }
        Ok(count)
    }
    fn string(&mut self) -> io::Result<std::ops::Range<usize>> {
        let start = self.bytes.len();
        let (value, encoded) = self.header()?;
        if encoded {
            match value {
                0 => self.capture(1)?,
                1 => self.capture(2)?,
                2 => self.capture(4)?,
                3 => {
                    let compressed = self.count()?;
                    self.count()?; // bound expanded length before reading payload
                    self.capture(compressed)?;
                }
                _ => return Err(invalid("unknown RDB string encoding")),
            }
        } else {
            self.capture(usize::try_from(value).map_err(|_| limit())?)?;
        }
        Ok(start..self.bytes.len())
    }
    fn strings(&mut self, count: usize) -> io::Result<()> {
        for _ in 0..count {
            self.string()?;
        }
        Ok(())
    }
    fn object(&mut self, tag: u8) -> io::Result<()> {
        match tag {
            0 | 9..=13 | 16 | 17 | 20 => {
                self.string()?;
            }
            1 | 2 | 4 => {
                let count = self.count()?;
                let count = if tag == 4 {
                    count.checked_mul(2).ok_or_else(limit)?
                } else {
                    count
                };
                self.strings(count)?;
            }
            3 | 5 => {
                let count = self.count()?;
                for _ in 0..count {
                    self.string()?;
                    if tag == 5 {
                        self.capture(8)?;
                    } else {
                        let length = self.byte()?;
                        if length < 253 {
                            self.capture(length as usize)?;
                        }
                    }
                }
            }
            7 => {
                if self.length()? != module_id() {
                    return Err(invalid("foreign or unsupported RDB module"));
                }
                if self.length()? != 2 || self.length()? != MODULE_VERSION || self.length()? != 5 {
                    return Err(invalid("invalid ffkv module header"));
                }
                self.string()?;
                if self.length()? != 0 {
                    return Err(invalid("invalid ffkv module terminator"));
                }
            }
            14 | 18 => {
                let count = self.count()?;
                for _ in 0..count {
                    if tag == 18 && !matches!(self.length()?, 1 | 2) {
                        return Err(invalid("unknown quicklist container"));
                    }
                    self.string()?;
                }
            }
            22 | 24 => {
                if tag == 24 {
                    self.capture(8)?;
                }
                let count = self.count()?;
                for _ in 0..count {
                    self.length()?;
                    self.string()?;
                    self.string()?;
                }
            }
            23 | 25 => {
                if tag == 25 {
                    self.capture(8)?;
                }
                self.string()?;
            }
            29 | 31 => {
                let count = match self.length()? {
                    0 => {
                        let range = self.string()?;
                        let mut decoder = Decoder::new(&self.bytes[range], self.limit);
                        let blob = decoder.string()?;
                        listpack(&mut decoder, &blob)?.len()
                    }
                    1 => {
                        let count = self.count()?;
                        self.strings(count)?;
                        count
                    }
                    _ => return Err(invalid("unknown template-hash field format")),
                };
                if tag == 29 {
                    self.string()?;
                } else {
                    self.strings(count)?;
                }
            }
            _ => return Err(invalid("unsupported RDB object type")),
        }
        Ok(())
    }
}

/// Append the DUMP footer, NOT the different full-file RDB checksum trailer.
pub fn append_footer(mut body: Vec<u8>) -> Bytes {
    body.extend_from_slice(&VERSION.to_le_bytes());
    let checksum = crc64(0, &body);
    body.extend_from_slice(&checksum.to_le_bytes());
    Bytes::from(body)
}

pub fn verify_footer(data: &[u8]) -> io::Result<&[u8]> {
    if data.len() < 10 {
        return Err(invalid("truncated DUMP payload"));
    }
    let end = data.len() - 10;
    let version = u16::from_le_bytes(data[end..end + 2].try_into().unwrap());
    if version == 0 || version > VERSION {
        return Err(invalid("unsupported DUMP version"));
    }
    let checksum = u64::from_le_bytes(data[end + 2..].try_into().unwrap());
    if crc64(0, &data[..end + 2]) != checksum {
        return Err(invalid("DUMP checksum mismatch"));
    }
    Ok(&data[..end])
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(limit());
        }
        self.bytes.try_reserve(bytes.len()).map_err(|_| limit())?;
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode_dump(object: &Object, max_bytes: usize) -> io::Result<Bytes> {
    let mut writer = BoundedWriter {
        bytes: Vec::new(),
        limit: max_bytes.checked_sub(10).ok_or_else(limit)?,
    };
    write_object(&mut writer, object)?;
    Ok(append_footer(writer.bytes))
}

pub fn decode_dump(data: &[u8], max_bytes: usize) -> io::Result<Object> {
    if data.len() > max_bytes {
        return Err(limit());
    }
    let mut decoder = Decoder::new(verify_footer(data)?, max_bytes);
    let tag = decoder.byte()?;
    let object = read_object(&mut decoder, tag)?;
    if decoder.remaining() != 0 {
        return Err(invalid("trailing bytes in DUMP object"));
    }
    Ok(object)
}

fn strings(decoder: &mut Decoder<'_>) -> io::Result<Vec<Bytes>> {
    let count = decoder.usize_length()?;
    if count > decoder.remaining() {
        return Err(invalid("truncated RDB collection"));
    }
    let mut values = decoder.vector(count)?;
    for _ in 0..count {
        values.push(decoder.string()?);
    }
    Ok(values)
}

fn score(bytes: &[u8]) -> io::Result<f64> {
    let value: f64 = std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| invalid("invalid sorted-set score"))?;
    if value.is_nan() {
        return Err(invalid("NaN sorted-set score"));
    }
    Ok(value)
}

fn unique<'a>(
    decoder: &mut Decoder<'_>,
    values: impl Iterator<Item = &'a Bytes>,
    count: usize,
) -> io::Result<()> {
    // Account for temporary duplicate detection too; bound adversarial collections.
    let cost = count
        .checked_mul(size_of::<&[u8]>() + size_of::<usize>())
        .ok_or_else(limit)?;
    decoder.charge(cost)?;
    let mut seen = HashSet::new();
    seen.try_reserve(count).map_err(|_| limit())?;
    for value in values {
        if !seen.insert(value.as_ref()) {
            return Err(invalid("duplicate RDB collection member"));
        }
    }
    decoder.budget += cost;
    Ok(())
}

fn paired(decoder: &mut Decoder<'_>, values: Vec<Bytes>, sorted: bool) -> io::Result<Object> {
    if !values.len().is_multiple_of(2) {
        return Err(invalid("odd compact hash or sorted-set entry count"));
    }
    let count = values.len() / 2;
    if sorted {
        let mut pairs = decoder.vector(count)?;
        let mut iter = values.into_iter();
        while let Some(member) = iter.next() {
            pairs.push((member, score(&iter.next().unwrap())?));
        }
        Ok(Object::SortedSet(pairs))
    } else {
        let mut pairs = decoder.vector(count)?;
        let mut iter = values.into_iter();
        while let Some(field) = iter.next() {
            pairs.push((field, iter.next().unwrap()));
        }
        Ok(Object::Hash(pairs))
    }
}

// Redis ebuckets' maximum absolute millisecond deadline (48 bits).
const HASH_EXPIRY_MAX: u64 = 0x0000_ffff_ffff_ffff;

fn hash_expiry(value: u64) -> io::Result<Option<i64>> {
    if value > HASH_EXPIRY_MAX {
        return Err(invalid("invalid hash field expiration deadline"));
    }
    Ok(if value == 0 { None } else { Some(value as i64) })
}

fn hash_minimum(decoder: &mut Decoder<'_>) -> io::Result<u64> {
    let minimum = decoder.u64_le()?;
    if minimum > HASH_EXPIRY_MAX + 1 {
        return Err(invalid("invalid minimum hash expiration deadline"));
    }
    Ok(minimum)
}

/// Read one object body after its type byte has already been consumed.
/// Unsupported types (including stream/array values and template references)
/// fail explicitly rather than dropping metadata.
pub fn read_object(decoder: &mut Decoder<'_>, type_byte: u8) -> io::Result<Object> {
    let object = match type_byte {
        0 => Object::String(decoder.string()?),
        1 => Object::List(strings(decoder)?),
        2 => Object::Set(strings(decoder)?),
        3 | 5 => {
            let count = decoder.usize_length()?;
            if count > decoder.remaining() / 2 {
                return Err(invalid("truncated sorted set"));
            }
            let mut values = decoder.vector(count)?;
            for _ in 0..count {
                let member = decoder.string()?;
                let value = if type_byte == 5 {
                    f64::from_le_bytes(decoder.raw(8)?.try_into().unwrap())
                } else {
                    match decoder.byte()? {
                        253 => return Err(invalid("NaN sorted-set score")),
                        254 => f64::INFINITY,
                        255 => f64::NEG_INFINITY,
                        length => score(decoder.raw(length as usize)?)?,
                    }
                };
                if value.is_nan() {
                    return Err(invalid("NaN sorted-set score"));
                }
                values.push((member, value));
            }
            Object::SortedSet(values)
        }
        4 => {
            let count = decoder.usize_length()?;
            if count > decoder.remaining() / 2 {
                return Err(invalid("truncated hash"));
            }
            let mut values = decoder.vector(count)?;
            for _ in 0..count {
                values.push((decoder.string()?, decoder.string()?));
            }
            Object::Hash(values)
        }
        22 | 24 => {
            let minimum = if type_byte == 24 {
                hash_minimum(decoder)?
            } else {
                0
            };
            let count = decoder.usize_length()?;
            if count > decoder.remaining() / 3 {
                return Err(invalid("truncated expiring-field hash"));
            }
            let mut values = decoder.vector(count)?;
            for _ in 0..count {
                let ttl = decoder.length()?;
                let absolute = if type_byte == 24 && ttl != 0 {
                    minimum
                        .checked_add(ttl - 1)
                        .ok_or_else(|| invalid("hash field deadline overflow"))?
                } else {
                    ttl
                };
                let deadline = hash_expiry(absolute)?;
                values.push((decoder.string()?, decoder.string()?, deadline));
            }
            Object::HashWithExpiry(values)
        }
        23 | 25 => {
            if type_byte == 25 {
                hash_minimum(decoder)?;
            }
            let bytes = decoder.string()?;
            let entries = listpack(decoder, &bytes)?;
            if entries.len() % 3 != 0 {
                return Err(invalid("invalid expiring-field hash triplet count"));
            }
            let mut values = decoder.vector(entries.len() / 3)?;
            let mut entries = entries.into_iter();
            while let Some(field) = entries.next() {
                let value = entries.next().unwrap();
                let deadline = entries.next().unwrap();
                let deadline = std::str::from_utf8(&deadline)
                    .ok()
                    .and_then(|text| text.parse::<u64>().ok())
                    .ok_or_else(|| invalid("invalid compact hash field deadline"))?;
                values.push((field, value, hash_expiry(deadline)?));
            }
            Object::HashWithExpiry(values)
        }
        7 => {
            if decoder.length()? != module_id() {
                return Err(invalid("foreign or unsupported RDB module"));
            }
            if decoder.length()? != 2 {
                return Err(invalid("missing ffkv module version"));
            }
            let version = decoder.length()?;
            if version != MODULE_VERSION {
                return Err(invalid("unsupported ffkv module payload version"));
            }
            if decoder.length()? != 5 {
                return Err(invalid("missing ffkv module payload"));
            }
            let payload = decoder.string()?;
            if decoder.length()? != 0 {
                return Err(invalid("invalid ffkv module terminator"));
            }
            Object::Module { version, payload }
        }
        9 => {
            let bytes = decoder.string()?;
            Object::Hash(zipmap(decoder, &bytes)?)
        }
        10 | 12 | 13 | 16 | 17 | 20 => {
            let bytes = decoder.string()?;
            let values = if matches!(type_byte, 10 | 12 | 13) {
                ziplist(decoder, &bytes)?
            } else {
                listpack(decoder, &bytes)?
            };
            match type_byte {
                10 => Object::List(values),
                12 | 17 => paired(decoder, values, true)?,
                13 | 16 => paired(decoder, values, false)?,
                _ => Object::Set(values),
            }
        }
        11 => {
            let bytes = decoder.string()?;
            Object::Set(intset(decoder, &bytes)?)
        }
        14 | 18 => {
            let count = decoder.usize_length()?;
            if count > decoder.remaining() {
                return Err(invalid("truncated quicklist"));
            }
            let mut values = Vec::new();
            for _ in 0..count {
                let container = if type_byte == 18 {
                    decoder.length()?
                } else {
                    2
                };
                let bytes = decoder.string()?;
                match container {
                    1 => decoder.push(&mut values, bytes)?,
                    2 => {
                        let mut node = if type_byte == 14 {
                            ziplist(decoder, &bytes)?
                        } else {
                            listpack(decoder, &bytes)?
                        };
                        values.try_reserve(node.len()).map_err(|_| limit())?;
                        values.append(&mut node);
                    }
                    _ => return Err(invalid("unknown quicklist container")),
                }
            }
            Object::List(values)
        }
        29 | 31 => {
            let fields = match decoder.length()? {
                0 => {
                    let bytes = decoder.string()?;
                    listpack(decoder, &bytes)?
                }
                1 => strings(decoder)?,
                _ => return Err(invalid("unknown template-hash field format")),
            };
            let mut values = if type_byte == 29 {
                let bytes = decoder.string()?;
                template_values(decoder, &bytes)?.1
            } else {
                let mut entries = decoder.vector(fields.len())?;
                for _ in 0..fields.len() {
                    entries.push(decoder.string()?);
                }
                entries
            };
            if fields.len() != values.len() {
                return Err(invalid("template-hash count mismatch"));
            }
            let mut pairs = decoder.vector(fields.len())?;
            pairs.extend(fields.into_iter().zip(values.drain(..)));
            Object::Hash(pairs)
        }
        _ => return Err(invalid("unsupported RDB object type")),
    };
    match &object {
        Object::Set(values) => unique(decoder, values.iter(), values.len())?,
        Object::Hash(values) => unique(decoder, values.iter().map(|p| &p.0), values.len())?,
        Object::HashWithExpiry(values) => {
            unique(decoder, values.iter().map(|p| &p.0), values.len())?
        }
        Object::SortedSet(values) => unique(decoder, values.iter().map(|p| &p.0), values.len())?,
        _ => {}
    }
    Ok(object)
}

fn signed(bytes: &[u8]) -> i64 {
    let mut word = [0u8; 8];
    word[..bytes.len()].copy_from_slice(bytes);
    let shift = 64 - bytes.len() * 8;
    (u64::from_le_bytes(word) << shift) as i64 >> shift
}

fn listpack(owner: &mut Decoder<'_>, bytes: &[u8]) -> io::Result<Vec<Bytes>> {
    let mut input = Decoder::new(bytes, bytes.len());
    if bytes.len() < 7 || input.u32_le()? as usize != bytes.len() {
        return Err(invalid("invalid listpack size"));
    }
    let expected = u16::from_le_bytes(input.raw(2)?.try_into().unwrap());
    let mut values = Vec::new();
    loop {
        let start = input.position();
        let tag = input.byte()?;
        if tag == 255 {
            break;
        }
        let value = match tag {
            0..=127 => owner.integer(tag as i64)?,
            128..=191 => owner.owned(input.raw((tag & 63) as usize)?)?,
            192..=223 => {
                let n = (((tag & 31) as i64) << 8) | input.byte()? as i64;
                owner.integer(if n & 4096 != 0 { n - 8192 } else { n })?
            }
            224..=239 => {
                let length = (((tag & 15) as usize) << 8) | input.byte()? as usize;
                owner.owned(input.raw(length)?)?
            }
            240 => {
                let length = input.u32_le()? as usize;
                owner.owned(input.raw(length)?)?
            }
            241..=244 => {
                let width = match tag {
                    241 => 2,
                    242 => 3,
                    243 => 4,
                    _ => 8,
                };
                owner.integer(signed(input.raw(width)?))?
            }
            _ => return Err(invalid("invalid listpack entry encoding")),
        };
        // Back-length is the entry length excluding the back-length itself.
        // Redis intentionally uses a larger encoding at the upper boundaries.
        let length = input.position() - start;
        let width = if length <= 127 {
            1
        } else if length < 16383 {
            2
        } else if length < 2097151 {
            3
        } else if length < 268435455 {
            4
        } else {
            5
        };
        for index in 0..width {
            let shift = (width - 1 - index) * 7;
            let expected = ((length >> shift) & 127) as u8 | if index == 0 { 0 } else { 128 };
            if input.byte()? != expected {
                return Err(invalid("invalid listpack back-length"));
            }
        }
        owner.push(&mut values, value)?;
    }
    if input.remaining() != 0 || (expected != u16::MAX && expected as usize != values.len()) {
        return Err(invalid("listpack entry count or terminator mismatch"));
    }
    Ok(values)
}

fn ziplist(owner: &mut Decoder<'_>, bytes: &[u8]) -> io::Result<Vec<Bytes>> {
    let mut input = Decoder::new(bytes, bytes.len());
    if bytes.len() < 11 || input.u32_le()? as usize != bytes.len() {
        return Err(invalid("invalid ziplist size"));
    }
    let tail = input.u32_le()? as usize;
    let count = u16::from_le_bytes(input.raw(2)?.try_into().unwrap());
    let mut values = Vec::new();
    let mut previous = 0;
    let mut last = 10;
    loop {
        let start = input.position();
        let tag = input.byte()?;
        if tag == 255 {
            break;
        }
        let prev = if tag == 254 {
            input.u32_le()? as usize
        } else {
            tag as usize
        };
        if prev != previous {
            return Err(invalid("invalid ziplist previous-entry length"));
        }
        let encoding = input.byte()?;
        let value = match encoding {
            0..=63 => owner.owned(input.raw(encoding as usize)?)?,
            64..=127 => {
                let length = (((encoding & 63) as usize) << 8) | input.byte()? as usize;
                owner.owned(input.raw(length)?)?
            }
            128 => {
                let length = u32::from_be_bytes(input.raw(4)?.try_into().unwrap()) as usize;
                owner.owned(input.raw(length)?)?
            }
            192 | 208 | 224 | 240 | 254 => {
                let width = match encoding {
                    192 => 2,
                    208 => 4,
                    224 => 8,
                    240 => 3,
                    _ => 1,
                };
                owner.integer(signed(input.raw(width)?))?
            }
            241..=253 => owner.integer((encoding & 15) as i64 - 1)?,
            _ => return Err(invalid("invalid ziplist entry encoding")),
        };
        previous = input.position() - start;
        last = start;
        owner.push(&mut values, value)?;
    }
    if input.remaining() != 0
        || tail != last
        || (count != u16::MAX && count as usize != values.len())
    {
        return Err(invalid("ziplist count, tail or terminator mismatch"));
    }
    Ok(values)
}

fn intset(owner: &mut Decoder<'_>, bytes: &[u8]) -> io::Result<Vec<Bytes>> {
    let mut input = Decoder::new(bytes, bytes.len());
    let width = input.u32_le()? as usize;
    if !matches!(width, 2 | 4 | 8) {
        return Err(invalid("invalid intset width"));
    }
    let count = input.u32_le()? as usize;
    if count.checked_mul(width) != Some(input.remaining()) {
        return Err(invalid("invalid intset length"));
    }
    let mut values = owner.vector(count)?;
    let mut previous = None;
    for _ in 0..count {
        let n = signed(input.raw(width)?);
        if previous.is_some_and(|p| p >= n) {
            return Err(invalid("unsorted or duplicate intset member"));
        }
        previous = Some(n);
        values.push(owner.integer(n)?);
    }
    Ok(values)
}

fn zipmap_length(input: &mut Decoder<'_>) -> io::Result<usize> {
    match input.byte()? {
        0..=253 => Ok(input.data[input.position - 1] as usize),
        254 => Ok(input.u32_le()? as usize),
        _ => Err(invalid("unexpected zipmap terminator")),
    }
}
fn zipmap(owner: &mut Decoder<'_>, bytes: &[u8]) -> io::Result<Vec<(Bytes, Bytes)>> {
    let mut input = Decoder::new(bytes, bytes.len());
    let expected = input.byte()?;
    let mut values = Vec::new();
    while input.data.get(input.position) != Some(&255) {
        let length = zipmap_length(&mut input)?;
        let key = owner.owned(input.raw(length)?)?;
        let length = zipmap_length(&mut input)?;
        let free = input.byte()? as usize;
        let value = owner.owned(input.raw(length)?)?;
        input.raw(free)?;
        owner.push(&mut values, (key, value))?;
    }
    input.byte()?;
    if input.remaining() != 0 || (expected < 254 && expected as usize != values.len()) {
        return Err(invalid("zipmap entry count or terminator mismatch"));
    }
    Ok(values)
}

#[cfg(test)]
mod tests {
    use super::*;
    const LIMIT: usize = 1024 * 1024;
    fn b(value: &'static [u8]) -> Bytes {
        Bytes::from_static(value)
    }
    fn fixture(tag: u8, blob: &[u8]) -> Bytes {
        let mut body = vec![tag];
        write_string(&mut body, blob).unwrap();
        append_footer(body)
    }
    #[test]
    fn redis_checksum_known_vector_and_incremental_state() {
        assert_eq!(crc64(0, b"123456789"), 0xe9c6d914c4b8d9ca);
        assert_eq!(crc64(crc64(0, b"1234"), b"56789"), crc64(0, b"123456789"));
    }
    #[test]
    fn binary_objects_roundtrip_and_streaming_primitives() {
        let objects = [
            Object::String(b(b"\0\xff\r\n")),
            Object::List(vec![b(b""), b(b"\xff"), b(b"42")]),
            Object::Set(vec![b(b"one"), b(b"\0two")]),
            Object::Hash(vec![(b(b"\0f"), b(b"\xffv")), (b(b""), b(b""))]),
            Object::HashWithExpiry(vec![
                (b(b"volatile"), b(b"value"), Some(1_900_000_000_000)),
                (b(b"persistent"), b(b"value"), None),
            ]),
            Object::SortedSet(vec![
                (b(b"a"), -1.25),
                (b(b"b"), f64::INFINITY),
                (b(b"c"), f64::NEG_INFINITY),
            ]),
            Object::Module {
                version: 1,
                payload: b(b"\0extension\xff"),
            },
        ];
        for object in objects {
            let dump = encode_dump(&object, LIMIT).unwrap();
            assert_eq!(decode_dump(&dump, LIMIT).unwrap(), object);
            let mut decoder = Decoder::new(verify_footer(&dump).unwrap(), LIMIT);
            let tag = decoder.byte().unwrap();
            assert_eq!(read_object(&mut decoder, tag).unwrap(), object);
            assert_eq!(decoder.remaining(), 0);
        }
        for length in [
            0,
            63,
            64,
            16383,
            16384,
            u32::MAX as u64,
            u32::MAX as u64 + 1,
            u64::MAX,
        ] {
            let mut out = Vec::new();
            write_length(&mut out, length).unwrap();
            assert_eq!(Decoder::new(&out, LIMIT).length().unwrap(), length);
        }
    }
    #[test]
    fn redis_documented_ziplist_fixture_and_intset() {
        // Redis ziplist.c's published ["2", "5"] example, not our encoder.
        let zip = [15, 0, 0, 0, 12, 0, 0, 0, 2, 0, 0, 0xf3, 2, 0xf6, 255];
        assert_eq!(
            decode_dump(&fixture(10, &zip), LIMIT).unwrap(),
            Object::List(vec![b(b"2"), b(b"5")])
        );
        let ints = [2, 0, 0, 0, 3, 0, 0, 0, 0xff, 0xff, 0, 0, 0xff, 0x7f];
        assert_eq!(
            decode_dump(&fixture(11, &ints), LIMIT).unwrap(),
            Object::Set(vec![b(b"-1"), b(b"0"), b(b"32767")])
        );
    }
    #[test]
    fn externally_specified_listpack_and_quicklist_containers() {
        // Wire bytes follow the primary listpack specification: integer 1 and
        // tiny string "x", each with its backward-length trailer.
        let lp = [12, 0, 0, 0, 2, 0, 1, 1, 0x81, b'x', 2, 255];
        assert_eq!(
            decode_dump(&fixture(20, &lp), LIMIT).unwrap(),
            Object::Set(vec![b(b"1"), b(b"x")])
        );
        assert_eq!(
            decode_dump(&fixture(16, &lp), LIMIT).unwrap(),
            Object::Hash(vec![(b(b"1"), b(b"x"))])
        );
        let scores = [12, 0, 0, 0, 2, 0, 0x81, b'x', 2, 1, 1, 255];
        assert_eq!(
            decode_dump(&fixture(17, &scores), LIMIT).unwrap(),
            Object::SortedSet(vec![(b(b"x"), 1.0)])
        );
        let mut body = vec![18, 2, 1];
        write_string(&mut body, b"plain").unwrap();
        body.push(2);
        write_string(&mut body, &lp).unwrap();
        assert_eq!(
            decode_dump(&append_footer(body), LIMIT).unwrap(),
            Object::List(vec![b(b"plain"), b(b"1"), b(b"x")])
        );
    }
    #[test]
    fn integer_and_lzf_string_decoding() {
        for (wire, expected) in [
            (&[0xc0, 0xff][..], b"-1".as_slice()),
            (&[0xc1, 0, 0x80][..], b"-32768".as_slice()),
            (&[0xc2, 0, 0, 0, 0x80][..], b"-2147483648".as_slice()),
            // literal abc, then overlapping distance-three reference of six.
            (
                &[0xc3, 6, 9, 2, b'a', b'b', b'c', 0x80, 2][..],
                b"abcabcabc".as_slice(),
            ),
        ] {
            assert_eq!(
                Decoder::new(wire, LIMIT).string().unwrap().as_ref(),
                expected
            );
        }
        assert!(Decoder::new(&[0xc0, 0], LIMIT).length().is_err());
        assert!(
            Decoder::new(&[0xc3, 2, 3, 0x20, 0], LIMIT)
                .string()
                .is_err()
        );
    }
    #[test]
    fn rejects_corruption_trailing_bytes_versions_and_resource_exhaustion() {
        let valid = encode_dump(&Object::String(b(b"value")), LIMIT).unwrap();
        let mut corrupt = valid.to_vec();
        corrupt[2] ^= 1;
        assert!(decode_dump(&corrupt, LIMIT).is_err());
        for length in 0..valid.len() {
            assert!(decode_dump(&valid[..length], LIMIT).is_err());
        }
        assert!(decode_dump(&valid, valid.len() - 1).is_err());
        assert!(encode_dump(&Object::String(b(b"value")), valid.len() - 1).is_err());
        assert!(decode_dump(&append_footer(vec![0, 0, 99]), LIMIT).is_err());
        let mut newer = vec![0, 0];
        newer.extend_from_slice(&(VERSION + 1).to_le_bytes());
        newer.extend_from_slice(&crc64(0, &newer).to_le_bytes());
        assert!(decode_dump(&newer, LIMIT).is_err());
        let allocation_bomb = append_footer(vec![
            1, 0x81, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
        ]);
        assert!(decode_dump(&allocation_bomb, LIMIT).is_err());
        assert!(
            Decoder::new(&[0xc3, 1, 0x80, 0xff, 0xff, 0xff, 0xff, 0], 1024)
                .string()
                .is_err()
        );
        assert!(decode_dump(&append_footer(vec![2, 2, 1, b'a', 1, b'a']), LIMIT).is_err());
        let mut lp = [12, 0, 0, 0, 2, 0, 1, 1, 0x81, b'x', 2, 255];
        lp[10] = 3;
        assert!(decode_dump(&fixture(20, &lp), LIMIT).is_err());
        assert!(
            encode_dump(
                &Object::Module {
                    version: 2,
                    payload: b(b"x")
                },
                LIMIT
            )
            .is_err()
        );
        assert!(decode_dump(&append_footer(vec![7, 0, 0]), LIMIT).is_err());
    }
    #[test]
    fn published_redis_dump_fixture_includes_external_checksum() {
        // Verbatim binary example from https://redis.io/docs/latest/commands/dump/
        let dump = b"\x00\xc0\n\n\x00n\x9fWE\x0e\xaec\xbb";
        assert_eq!(decode_dump(dump, LIMIT).unwrap(), Object::String(b(b"10")));
    }

    #[test]
    fn streaming_readers_never_consume_the_next_record() {
        use std::io::{Cursor, Read};
        let objects = [
            Object::String(b(b"\0binary\xff")),
            Object::List(vec![b(b"x"), b(b"y")]),
            Object::Set(vec![b(b"x"), b(b"y")]),
            Object::Hash(vec![(b(b"f"), b(b"v"))]),
            Object::SortedSet(vec![(b(b"m"), -12.5)]),
            Object::Module {
                version: 1,
                payload: b(b"payload"),
            },
        ];
        for object in objects {
            let mut wire = vec![object.type_byte()];
            write_string(&mut wire, b"key").unwrap();
            write_object_body(&mut wire, &object).unwrap();
            wire.extend_from_slice(b"NEXT");
            let mut input = Cursor::new(wire);
            let mut tag = [0];
            input.read_exact(&mut tag).unwrap();
            assert_eq!(read_stream_string(&mut input, LIMIT).unwrap(), b(b"key"));
            assert_eq!(
                read_stream_object(&mut input, tag[0], LIMIT).unwrap(),
                object
            );
            let mut next = [0; 4];
            input.read_exact(&mut next).unwrap();
            assert_eq!(&next, b"NEXT");
        }
        let lp = [12, 0, 0, 0, 2, 0, 1, 1, 0x81, b'x', 2, 255];
        let mut wire = vec![1, 2]; // one quicklist2 packed node
        write_string(&mut wire, &lp).unwrap();
        wire.push(99);
        let mut input = Cursor::new(wire);
        assert_eq!(
            read_stream_object(&mut input, 18, LIMIT).unwrap(),
            Object::List(vec![b(b"1"), b(b"x")])
        );
        let mut next = [0];
        input.read_exact(&mut next).unwrap();
        assert_eq!(next, [99]);
        let mut input = Cursor::new([0x40, 64, 42]);
        assert_eq!(read_stream_length(&mut input).unwrap(), 64);
        input.read_exact(&mut next).unwrap();
        assert_eq!(next, [42]);
        assert!(read_stream_string(&mut Cursor::new([100]), 32).is_err());
        assert!(
            read_stream_object(
                &mut Cursor::new([0x81, 255, 255, 255, 255, 255, 255, 255, 255]),
                1,
                64
            )
            .is_err()
        );
    }

    #[test]
    fn compact_signed_integers_zipmap_and_template_hashes() {
        // Listpack signed-13, signed-16, signed-24, signed-32, signed-64.
        let lp = [
            35, 0, 0, 0, 5, 0, 0xdf, 0xff, 2, 0xf1, 0, 0x80, 3, 0xf2, 0, 0, 0x80, 4, 0xf3, 0, 0, 0,
            0x80, 5, 0xf4, 0, 0, 0, 0, 0, 0, 0, 0x80, 9, 255,
        ];
        assert_eq!(
            decode_dump(&fixture(20, &lp), LIMIT).unwrap(),
            Object::Set(vec![
                b(b"-1"),
                b(b"-32768"),
                b(b"-8388608"),
                b(b"-2147483648"),
                b(b"-9223372036854775808"),
            ])
        );
        let zm = [1, 1, b'f', 1, 0, b'v', 255];
        assert_eq!(
            decode_dump(&fixture(9, &zm), LIMIT).unwrap(),
            Object::Hash(vec![(b(b"f"), b(b"v"))])
        );
        let mut body = vec![31, 1, 2];
        write_string(&mut body, b"first").unwrap();
        write_string(&mut body, b"second").unwrap();
        write_string(&mut body, b"one").unwrap();
        write_string(&mut body, b"two").unwrap();
        let expected = Object::Hash(vec![(b(b"first"), b(b"one")), (b(b"second"), b(b"two"))]);
        let mut streamed = std::io::Cursor::new(&body[1..]);
        assert_eq!(
            read_stream_object(&mut streamed, 31, LIMIT).unwrap(),
            expected
        );
        assert_eq!(decode_dump(&append_footer(body), LIMIT).unwrap(), expected);
    }
    #[test]
    fn redis_expiring_hash_formats_preserve_absolute_deadlines() {
        // Metadata PRE_GA stores absolute length-encoded expiration values.
        // GA prepends LE min-expire and stores zero or (absolute-minimum+1).
        let expected = Object::HashWithExpiry(vec![
            (b(b"f"), b(b"v"), Some(1000)),
            (b(b"p"), b(b"q"), None),
        ]);
        for tag in [22, 24] {
            let mut body = vec![tag];
            if tag == 24 {
                body.extend_from_slice(&1000u64.to_le_bytes());
            }
            write_length(&mut body, 2).unwrap();
            write_length(&mut body, if tag == 24 { 1 } else { 1000 }).unwrap();
            write_string(&mut body, b"f").unwrap();
            write_string(&mut body, b"v").unwrap();
            write_length(&mut body, 0).unwrap();
            write_string(&mut body, b"p").unwrap();
            write_string(&mut body, b"q").unwrap();
            let mut cursor = std::io::Cursor::new(&body[1..]);
            assert_eq!(
                read_stream_object(&mut cursor, tag, LIMIT).unwrap(),
                expected
            );
            assert_eq!(cursor.position() as usize, body.len() - 1);
            assert_eq!(decode_dump(&append_footer(body), LIMIT).unwrap(), expected);
        }
        // Two field/value/absolute-expiry triplets, with integer deadline1000.
        let lp = [
            24, 0, 0, 0, 6, 0, 0x81, b'f', 2, 0x81, b'v', 2, 0xc3, 0xe8, 2, 0x81, b'p', 2, 0x81,
            b'q', 2, 0, 1, 255,
        ];
        for tag in [23, 25] {
            let mut body = vec![tag];
            if tag == 25 {
                body.extend_from_slice(&1000u64.to_le_bytes());
            }
            write_string(&mut body, &lp).unwrap();
            let mut cursor = std::io::Cursor::new(&body[1..]);
            assert_eq!(
                read_stream_object(&mut cursor, tag, LIMIT).unwrap(),
                expected
            );
            assert_eq!(cursor.position() as usize, body.len() - 1);
            assert_eq!(decode_dump(&append_footer(body), LIMIT).unwrap(), expected);
        }
        let dump = encode_dump(&expected, LIMIT).unwrap();
        assert_eq!(dump[0], 24);
        assert_eq!(decode_dump(&dump, LIMIT).unwrap(), expected);
        let invalid_deadline = Object::HashWithExpiry(vec![(b(b"f"), b(b"v"), Some(-1))]);
        assert!(encode_dump(&invalid_deadline, LIMIT).is_err());
        let mut overflow = vec![24];
        overflow.extend_from_slice(&HASH_EXPIRY_MAX.to_le_bytes());
        write_length(&mut overflow, 1).unwrap();
        write_length(&mut overflow, u64::MAX).unwrap();
        write_string(&mut overflow, b"f").unwrap();
        write_string(&mut overflow, b"v").unwrap();
        assert!(decode_dump(&append_footer(overflow), LIMIT).is_err());
    }
    #[test]
    fn snapshot_template_registry_resolves_both_reference_forms() {
        use std::io::{Cursor, Read};
        let mut registry_record = vec![7, 2];
        write_string(&mut registry_record, b"first").unwrap();
        write_string(&mut registry_record, b"second").unwrap();
        let mut context = SnapshotContext::new(LIMIT);
        let mut wire = registry_record.clone();
        wire.push(99);
        let mut input = Cursor::new(wire);
        read_stream_hash_template(&mut input, &mut context).unwrap();
        let mut next = [0];
        input.read_exact(&mut next).unwrap();
        assert_eq!(next, [99]);
        let expected = Object::Hash(vec![(b(b"first"), b(b"one")), (b(b"second"), b(b"two"))]);
        let mut array = vec![7];
        write_string(&mut array, b"one").unwrap();
        write_string(&mut array, b"two").unwrap();
        array.push(99);
        let mut input = Cursor::new(&array);
        assert_eq!(
            read_stream_object_with_context(&mut input, 32, LIMIT, &context).unwrap(),
            expected
        );
        input.read_exact(&mut next).unwrap();
        assert_eq!(next, [99]);
        let lp = [
            19, 0, 0, 0, 3, 0, 7, 1, 0x83, b'o', b'n', b'e', 4, 0x83, b't', b'w', b'o', 4, 255,
        ];
        let mut packed = Vec::new();
        write_string(&mut packed, &lp).unwrap();
        packed.push(99);
        let mut input = Cursor::new(&packed);
        assert_eq!(
            read_stream_object_with_context(&mut input, 30, LIMIT, &context).unwrap(),
            expected
        );
        input.read_exact(&mut next).unwrap();
        assert_eq!(next, [99]);
        // A reference encoding is not a valid standalone DUMP.
        let mut dump = vec![30];
        write_string(&mut dump, &lp).unwrap();
        assert!(decode_dump(&append_footer(dump), LIMIT).is_err());
        assert!(
            read_stream_hash_template(&mut Cursor::new(&registry_record), &mut context).is_err()
        );
        assert!(
            read_stream_hash_template(
                &mut Cursor::new(&registry_record),
                &mut SnapshotContext::new(32)
            )
            .is_err()
        );
        assert!(
            read_stream_object_with_context(&mut Cursor::new([8]), 32, LIMIT, &context).is_err()
        );
        let mut reversed = vec![8, 2];
        write_string(&mut reversed, b"second").unwrap();
        write_string(&mut reversed, b"first").unwrap();
        assert!(read_stream_hash_template(&mut Cursor::new(reversed), &mut context).is_err());
        let mut wrong_count = vec![7];
        write_string(&mut wrong_count, b"one").unwrap();
        assert!(
            read_stream_object_with_context(&mut Cursor::new(wrong_count), 32, LIMIT, &context)
                .is_err()
        );
    }
}
