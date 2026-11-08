//! Redis HYLL wire format; algorithm references: Redis 7.2's published format
//! and Otmar Ertl, arXiv:1702.01284, improved raw estimator (Algorithm 6).
//! Implemented independently from the format and mathematical definitions.
use super::{CommandSpec, Engine, Mutation, Resp, StringBuffer, StringWrite, Value, error};
use bytes::Bytes;

const REGISTERS: usize = 1 << 14;
const HEADER: usize = 16;
const DENSE_SIZE: usize = HEADER + REGISTERS * 6 / 8;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "pfadd",
        -2,
        &["write", "denyoom", "fast"],
        (1, 1, 1),
        Engine::pfadd,
    ),
    CommandSpec::handler("pfcount", -2, &["readonly"], (1, -1, 1), Engine::pfcount),
    CommandSpec::handler(
        "pfmerge",
        -2,
        &["write", "denyoom"],
        (1, -1, 1),
        Engine::pfmerge,
    ),
    CommandSpec::handler(
        "pfdebug",
        3,
        &["write", "denyoom", "admin"],
        (2, 2, 1),
        Engine::pfdebug,
    ),
    CommandSpec::handler("pfselftest", 1, &["admin"], (0, 0, 0), Engine::pfselftest),
];

fn invalid() -> Resp {
    error("INVALIDOBJ Corrupted HLL object detected")
}
fn hll_type() -> Resp {
    error("WRONGTYPE Key is not a valid HyperLogLog string value.")
}

fn register(value: &[u8], index: usize) -> u8 {
    let bit = index * 6;
    let byte = HEADER + bit / 8;
    let shift = bit % 8;
    let lo = value[byte] as u16;
    let hi = value.get(byte + 1).copied().unwrap_or(0) as u16;
    (((lo | hi << 8) >> shift) & 63) as u8
}
fn set_register(value: &mut [u8], index: usize, count: u8) {
    let bit = index * 6;
    let byte = HEADER + bit / 8;
    let shift = bit % 8;
    value[byte] = (value[byte] & !(63u16 << shift) as u8) | (count << shift);
    if shift > 2 {
        value[byte + 1] = (value[byte + 1] & !(63u8 >> (8 - shift))) | (count >> (8 - shift));
    }
}

// Walk sparse runs with checked bounds before exposing any registers to callers.
fn visit(value: &[u8], mut consume: impl FnMut(usize, u8)) -> Result<(), Resp> {
    if value.len() < HEADER || &value[..4] != b"HYLL" || value[4] > 1 {
        return Err(hll_type());
    }
    if value[4] == 0 {
        if value.len() != DENSE_SIZE {
            return Err(hll_type());
        }
        for i in 0..REGISTERS {
            let count = register(value, i);
            consume(i, count);
        }
        return Ok(());
    }
    let (mut position, mut index) = (HEADER, 0usize);
    while position < value.len() {
        let opcode = value[position];
        position += 1;
        let (count, length) = if opcode & 128 != 0 {
            (((opcode >> 2) & 31) + 1, (opcode as usize & 3) + 1)
        } else if opcode & 64 != 0 {
            let tail = *value.get(position).ok_or_else(invalid)?;
            position += 1;
            (0, (((opcode as usize & 63) << 8) | tail as usize) + 1)
        } else {
            (0, (opcode as usize & 63) + 1)
        };
        let end = index
            .checked_add(length)
            .filter(|end| *end <= REGISTERS)
            .ok_or_else(invalid)?;
        for i in index..end {
            consume(i, count);
        }
        index = end;
    }
    if index != REGISTERS {
        return Err(invalid());
    }
    Ok(())
}
fn validate(value: &[u8]) -> Result<(), Resp> {
    if value.len() < HEADER || &value[..4] != b"HYLL" || value[4] > 1 {
        return Err(hll_type());
    }
    if value[4] == 0 {
        if value.len() != DENSE_SIZE {
            return Err(hll_type());
        }
        Ok(())
    } else {
        visit(value, |_, _| {})
    }
}
fn empty() -> StringBuffer {
    let mut value = StringBuffer::zeroed(DENSE_SIZE);
    value[..4].copy_from_slice(b"HYLL");
    value
}
fn densify(value: &mut StringBuffer) {
    if value.is_empty() {
        *value = empty();
    } else if value[4] == 1 {
        let mut dense = empty();
        visit(value, |i, count| set_register(&mut dense, i, count)).expect("validated sparse HYLL");
        dense[8..16].copy_from_slice(&value[8..16]);
        *value = dense;
    }
}
fn sparsify(value: &mut StringBuffer) {
    // At most 3000 bytes plus one two-byte opcode; rejected encodings need no
    // allocation, and successful ones get exact-sized stored memory below.
    let mut sparse = smallvec::SmallVec::<[u8; 4096]>::new();
    sparse.extend_from_slice(&value[..HEADER]);
    sparse[4] = 1;
    let mut index = 0;
    while index < REGISTERS {
        let rank = register(value, index);
        if rank > 32 {
            return;
        }
        let mut end = index + 1;
        let maximum = if rank == 0 { REGISTERS } else { index + 4 };
        while end < REGISTERS && end < maximum && register(value, end) == rank {
            end += 1;
        }
        let run = end - index;
        if rank != 0 {
            sparse.extend_from_slice(&[128 | ((rank - 1) << 2) | (run - 1) as u8]);
        } else if run <= 64 {
            sparse.extend_from_slice(&[(run - 1) as u8]);
        } else {
            let encoded = run - 1;
            sparse.extend_from_slice(&[64 | (encoded >> 8) as u8, encoded as u8]);
        }
        if sparse.len() > 3000 {
            return;
        }
        index = end;
    }
    let mut encoded = StringBuffer::with_capacity(sparse.len());
    encoded.extend_from_slice(&sparse);
    *value = encoded;
}

// MurmurHash64A specifies wrapping 64-bit arithmetic and little-endian words.
fn hash(element: &[u8]) -> u64 {
    const M: u64 = 0xc6a4_a793_5bd1_e995;
    let mut state = 0xadc8_3b19u64 ^ (element.len() as u64).wrapping_mul(M);
    let (words, rest) = element.as_chunks::<8>();
    for word in words {
        let mut k = u64::from_le_bytes(*word).wrapping_mul(M);
        k ^= k >> 47;
        state ^= k.wrapping_mul(M);
        state = state.wrapping_mul(M);
    }
    if !rest.is_empty() {
        for (i, byte) in rest.iter().enumerate() {
            state ^= (*byte as u64) << (8 * i);
        }
        state = state.wrapping_mul(M);
    }
    state ^= state >> 47;
    state = state.wrapping_mul(M);
    state ^ (state >> 47)
}
fn sigma(mut x: f64) -> f64 {
    if x == 1.0 {
        return f64::INFINITY;
    }
    let (mut sum, mut weight) = (x, 1.0);
    loop {
        x *= x;
        let previous = sum;
        sum += x * weight;
        weight *= 2.0;
        if sum == previous {
            return sum;
        }
    }
}
fn tau(mut x: f64) -> f64 {
    if x == 0.0 || x == 1.0 {
        return 0.0;
    }
    let (mut sum, mut weight) = (1.0 - x, 1.0);
    loop {
        x = x.sqrt();
        weight *= 0.5;
        let previous = sum;
        sum -= (1.0 - x).powi(2) * weight;
        if sum == previous {
            return sum / 3.0;
        }
    }
}
fn estimate(histogram: &[u32; 64]) -> i64 {
    let m = REGISTERS as f64;
    let mut denominator = m * tau(1.0 - histogram[51] as f64 / m);
    for count in histogram[1..51].iter().rev() {
        denominator = (denominator + *count as f64) * 0.5;
    }
    denominator += m * sigma(histogram[0] as f64 / m);
    // Conversion saturates safely even for a user-created, saturated sketch.
    (m * m / (2.0 * std::f64::consts::LN_2 * denominator)).round() as i64
}
fn count(value: &[u8]) -> Result<i64, Resp> {
    let mut histogram = [0; 64];
    visit(value, |_, count| histogram[count as usize] += 1)?;
    Ok(estimate(&histogram))
}
/// The same register/hash/estimator path used by stored HYLL values, without
/// materializing a union of members for SUNIONCARD APPROX.
pub(super) fn approximate_cardinality<'a>(
    members: impl IntoIterator<Item = &'a Bytes>,
    limit: i64,
) -> i64 {
    let mut registers = [0u8; REGISTERS];
    let mut histogram = [0; 64];
    histogram[0] = REGISTERS as u32;
    for member in members {
        let h = hash(member);
        let index = h as usize & (REGISTERS - 1);
        let rank = ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
        let previous = registers[index];
        if previous < rank {
            histogram[previous as usize] -= 1;
            histogram[rank as usize] += 1;
            registers[index] = rank;
            if limit != i64::MAX && estimate(&histogram) >= limit {
                return limit;
            }
        }
    }
    estimate(&histogram).min(limit)
}

fn selftest() -> Result<(), &'static str> {
    // Seeded, bounded tests cover six-bit straddling and the final register,
    // without touching user keys or relying on process-global randomness.
    let mut packed = empty();
    let mut state = 0x7c65_192a_8de4_b03fu64;
    let mut expected = [0u8; REGISTERS];
    for round in 0..4 {
        for (index, expected) in expected.iter_mut().enumerate() {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            *expected = ((state >> round) & 63) as u8;
            set_register(&mut packed, index, *expected);
        }
        if expected
            .iter()
            .enumerate()
            .any(|(i, expected)| register(&packed, i) != *expected)
        {
            return Err("register packing");
        }
    }
    let mut dense = empty();
    let mut raw = [0u8; REGISTERS];
    for i in 0..100_000u64 {
        let h = hash(&i.wrapping_add(0xa398_ef27_14c0_b65d).to_le_bytes());
        let index = h as usize & (REGISTERS - 1);
        let rank = ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
        raw[index] = raw[index].max(rank);
        if register(&dense, index) < rank {
            set_register(&mut dense, index, rank);
        }
        let cardinality = i + 1;
        if [10, 100, 1_000, 10_000, 100_000].contains(&cardinality) {
            let mut histogram = [0; 64];
            for &rank in &raw {
                histogram[rank as usize] += 1;
            }
            let observed = count(&dense).map_err(|_| "dense decoding")?;
            if observed != estimate(&histogram)
                || (observed as f64 / cardinality as f64 - 1.0).abs() > 0.05
            {
                return Err("cardinality quality");
            }
        }
    }
    let mut sparse = StringBuffer::zeroed(HEADER);
    sparse[..4].copy_from_slice(b"HYLL");
    sparse[4] = 1;
    sparse.extend_from_slice(&[0x7f, 0xff]);
    if count(&sparse).map_err(|_| "sparse decoding")? != 0 {
        return Err("sparse cardinality");
    }
    densify(&mut sparse);
    if count(&sparse).map_err(|_| "sparse conversion")? != 0 {
        return Err("sparse conversion");
    }
    Ok(())
}

impl Engine {
    fn pfselftest(&mut self, _args: &[Bytes], _now: i64) -> Resp {
        match selftest() {
            Ok(()) => Resp::Simple("OK"),
            Err(stage) => Resp::Error(format!("ERR HyperLogLog self-test failed: {stage}")),
        }
    }

    fn pfdebug(&mut self, args: &[Bytes], now: i64) -> Resp {
        let reply_limit = self.reply_limit;
        let operation = &args[1];
        if ![b"ENCODING".as_slice(), b"TODENSE", b"GETREG", b"DECODE"]
            .iter()
            .any(|name| operation.eq_ignore_ascii_case(name))
        {
            return error("ERR Unknown PFDEBUG subcommand");
        }
        let value = match self.hll(&args[2], now) {
            Ok(Some(value)) => value,
            Ok(None) => return error("ERR The specified key does not exist"),
            Err(reply) => return reply,
        };
        let sparse = value[4] == 1;
        if operation.eq_ignore_ascii_case(b"ENCODING") {
            return Resp::Simple(if sparse { "sparse" } else { "dense" });
        }
        if operation.eq_ignore_ascii_case(b"DECODE") {
            if !sparse {
                return error("ERR HLL encoding is not sparse");
            }
            let mut text = String::new();
            let mut position = HEADER;
            while position < value.len() {
                let opcode = value[position];
                position += 1;
                let token = if opcode & 128 != 0 {
                    format!("v:{},{} ", ((opcode >> 2) & 31) + 1, (opcode & 3) + 1)
                } else if opcode & 64 != 0 {
                    let run = (((opcode as usize & 63) << 8) | value[position] as usize) + 1;
                    position += 1;
                    format!("Z:{run} ")
                } else {
                    format!("z:{} ", (opcode & 63) + 1)
                };
                if text.len().saturating_add(token.len()) > reply_limit {
                    return error("ERR reply exceeds configured limit");
                }
                text.push_str(&token);
            }
            text.pop(); // The diagnostic wire format has no trailing separator.
            return Resp::Bulk(Some(Bytes::from(text)));
        }
        if operation.eq_ignore_ascii_case(b"GETREG")
            && let Err(reply) = super::ReplyBudget::new(reply_limit).charge(REGISTERS, 0)
        {
            return reply;
        }
        // GETREG, like TODENSE, normalizes a sparse representation. Preserve
        // its cached cardinality and expiry; journal only an actual conversion.
        if sparse {
            let (mut value, deadline) = self.string_buffer(&args[2], 0);
            densify(&mut value);
            let capture = self.capturing_changes();
            if let Some(value) =
                self.store_value(&args[2], StringWrite::Buffer(value), deadline, capture)
            {
                self.record_change(now, false, || Mutation::Set {
                    key: args[2].clone(),
                    value,
                    expires_at: deadline,
                });
            }
        }
        if operation.eq_ignore_ascii_case(b"TODENSE") {
            return Resp::Integer(i64::from(sparse));
        }
        let Some(Value::String(value)) = self.value(&args[2], now) else {
            unreachable!("validated HyperLogLog");
        };
        let value = value.bytes();
        super::array(
            (0..REGISTERS)
                .map(|index| Resp::Integer(register(value, index) as i64))
                .collect(),
        )
    }

    fn hll(&mut self, key: &Bytes, now: i64) -> Result<Option<&Bytes>, Resp> {
        match self.value(key, now) {
            Some(Value::String(value)) => {
                let value = value.bytes();
                validate(value)?;
                Ok(Some(value))
            }
            Some(_) => Err(hll_type()),
            None => Ok(None),
        }
    }
    fn pfadd(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (present, was_sparse) = match self.hll(&args[1], now) {
            Ok(value) => (value.is_some(), value.is_none_or(|value| value[4] == 1)),
            Err(reply) => return reply,
        };
        // Do not take ownership or copy the dense buffer for a register no-op.
        if present {
            let Some(Value::String(value)) = self.value(&args[1], now) else {
                unreachable!()
            };
            let value = value.bytes();
            if value[4] == 0
                && !args[2..].iter().any(|element| {
                    let h = hash(element);
                    register(value, h as usize & (REGISTERS - 1))
                        < ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1
                })
            {
                return Resp::Integer(0);
            }
            if value[4] == 1 {
                let mut registers = [0u8; REGISTERS];
                visit(value, |i, count| registers[i] = count).expect("validated sparse HYLL");
                if !args[2..].iter().any(|element| {
                    let h = hash(element);
                    registers[h as usize & (REGISTERS - 1)]
                        < ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1
                }) {
                    return Resp::Integer(0);
                }
            }
            if args.len() == 2 {
                return Resp::Integer(0);
            }
        }
        let (mut value, deadline) = self.string_buffer(&args[1], 0);
        densify(&mut value);
        let mut changed = !present;
        for element in &args[2..] {
            let h = hash(element);
            let index = h as usize & (REGISTERS - 1);
            let rank = ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
            if register(&value, index) < rank {
                set_register(&mut value, index, rank);
                changed = true;
            }
        }
        if changed {
            value[15] |= 128;
            if was_sparse {
                sparsify(&mut value);
            }
            self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        } else {
            self.store_value(&args[1], StringWrite::Buffer(value), deadline, false);
        }
        Resp::Integer(i64::from(changed))
    }
    fn pfcount(&mut self, args: &[Bytes], now: i64) -> Resp {
        if args.len() == 2 {
            let result = match self.hll(&args[1], now) {
                Ok(None) => return Resp::Integer(0),
                Err(reply) => return reply,
                Ok(Some(value)) => {
                    if value[15] & 128 == 0 {
                        return Resp::Integer(
                            u64::from_le_bytes(value[8..16].try_into().unwrap()) as i64
                        );
                    }
                    match count(value) {
                        Ok(count) => count,
                        Err(reply) => return reply,
                    }
                }
            };
            let (mut value, deadline) = self.string_buffer(&args[1], 0);
            value[8..16].copy_from_slice(&(result as u64).to_le_bytes());
            let capture = self.capturing_changes();
            if let Some(value) =
                self.store_value(&args[1], StringWrite::Buffer(value), deadline, capture)
            {
                self.record_change(now, false, || Mutation::Set {
                    key: args[1].clone(),
                    value,
                    expires_at: deadline,
                });
            }
            return Resp::Integer(result);
        }
        let mut union = [0u8; REGISTERS];
        for key in &args[1..] {
            match self.hll(key, now) {
                Ok(Some(value)) => {
                    if let Err(reply) = visit(value, |i, count| union[i] = union[i].max(count)) {
                        return reply;
                    }
                }
                Ok(None) => {}
                Err(reply) => return reply,
            }
        }
        let mut histogram = [0; 64];
        for count in union {
            histogram[count as usize] += 1;
        }
        Resp::Integer(estimate(&histogram))
    }
    fn pfmerge(&mut self, args: &[Bytes], now: i64) -> Resp {
        let mut union = [0u8; REGISTERS];
        for key in &args[1..] {
            match self.hll(key, now) {
                Ok(Some(value)) => {
                    if let Err(reply) = visit(value, |i, count| union[i] = union[i].max(count)) {
                        return reply;
                    }
                }
                Ok(None) => {}
                Err(reply) => return reply,
            }
        }
        let (mut value, deadline) = self.string_buffer(&args[1], 0);
        densify(&mut value);
        for (i, count) in union.into_iter().enumerate() {
            set_register(&mut value, i, count);
        }
        value[15] |= 128;
        self.store(&args[1], StringWrite::Buffer(value), deadline, now);
        Resp::Simple("OK")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    #[test]
    fn dense_packing_roundtrips_every_register_without_overrun() {
        let mut value = empty();
        for i in 0..REGISTERS {
            set_register(&mut value, i, (i % 52) as u8);
        }
        for i in 0..REGISTERS {
            assert_eq!(register(&value, i), (i % 52) as u8);
        }
        assert_eq!(value.len(), 12304);
    }
    #[test]
    fn add_count_union_cache_and_string_interoperability() {
        let mut engine = Engine::new();
        assert_eq!(
            run(&mut engine, &["PFADD", "a", "foo", "bar", "zap"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["PFCOUNT", "a"]), Resp::Integer(3));
        assert_eq!(run(&mut engine, &["PFADD", "a", "foo"]), Resp::Integer(0));
        assert_eq!(
            run(&mut engine, &["PFADD", "b", "foo", "qux"]),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["PFCOUNT", "a", "b"]), Resp::Integer(4));
        assert_eq!(run(&mut engine, &["PFMERGE", "a", "b"]), Resp::Simple("OK"));
        assert_eq!(run(&mut engine, &["PFCOUNT", "a"]), Resp::Integer(4));
        assert_eq!(
            run(&mut engine, &["STRLEN", "a"]),
            Resp::Integer(DENSE_SIZE as i64)
        );
        run(&mut engine, &["SET", "bad", "HYLL"]);
        assert!(matches!(
            run(&mut engine, &["PFMERGE", "a", "bad"]),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["PFCOUNT", "a"]), Resp::Integer(4));
    }
    #[test]
    fn estimator_quality_across_cardinalities() {
        let mut dense = empty();
        for i in 0..100_000u64 {
            let h = hash(&i.to_le_bytes());
            let index = h as usize & (REGISTERS - 1);
            let rank = ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
            if rank > register(&dense, index) {
                set_register(&mut dense, index, rank);
            }
            let n = i + 1;
            if [100, 1000, 10_000, 100_000].contains(&n) {
                assert!((count(&dense).unwrap() as f64 / n as f64 - 1.0).abs() < 0.03);
            }
        }
    }
    #[test]
    fn sparse_import_and_malformed_runs_are_checked() {
        let mut value = bytes::BytesMut::zeroed(16);
        value[..4].copy_from_slice(b"HYLL");
        value[4] = 1;
        value.extend_from_slice(&[0x7f, 0xff]);
        assert_eq!(count(&value).unwrap(), 0);
        value.truncate(17);
        assert!(validate(&value).is_err());
        value.extend_from_slice(&[0xff, 0]);
        assert!(validate(&value).is_err());
    }
    #[test]
    fn overlapping_union_and_ttl_survive_cache_refresh_and_merge() {
        let mut engine = Engine::new();
        for (key, start, end) in [("a", 0u64, 20_000), ("b", 10_000, 30_000)] {
            let mut args = vec![
                Bytes::from_static(b"PFADD"),
                Bytes::copy_from_slice(key.as_bytes()),
            ];
            args.extend((start..end).map(|i| Bytes::copy_from_slice(&i.to_le_bytes())));
            assert_eq!(engine.execute_at(&args, 1000), Resp::Integer(1));
        }
        run(&mut engine, &["PEXPIRE", "a", "10000"]);
        assert!(matches!(
            run(&mut engine, &["PFCOUNT", "a"]),
            Resp::Integer(_)
        ));
        assert_eq!(run(&mut engine, &["PTTL", "a"]), Resp::Integer(10000));
        let Resp::Integer(union) = run(&mut engine, &["PFCOUNT", "a", "b"]) else {
            panic!("integer cardinality");
        };
        assert!((union as f64 / 30_000.0 - 1.0).abs() < 0.03);
        assert_eq!(run(&mut engine, &["PFMERGE", "a", "b"]), Resp::Simple("OK"));
        assert_eq!(run(&mut engine, &["PFCOUNT", "a"]), Resp::Integer(union));
        assert_eq!(run(&mut engine, &["PTTL", "a"]), Resp::Integer(10000));
        // A raw string operation remains observable to the HYLL validator.
        run(&mut engine, &["SETBIT", "a", "0", "1"]);
        assert!(matches!(
            run(&mut engine, &["PFCOUNT", "a"]),
            Resp::Error(_)
        ));
    }
    #[test]
    fn diagnostics_decode_convert_cache_and_expose_actual_registers() {
        let mut engine = Engine::new();
        assert_eq!(run(&mut engine, &["PFADD", "h"]), Resp::Integer(1));
        assert_eq!(
            run(&mut engine, &["PFDEBUG", "ENCODING", "h"]),
            Resp::Simple("sparse")
        );
        assert_eq!(
            run(&mut engine, &["PFDEBUG", "DECODE", "h"]),
            Resp::Bulk(Some(Bytes::from_static(b"Z:16384")))
        );
        run(&mut engine, &["PFADD", "h", "foo", "bar"]);
        run(&mut engine, &["PEXPIRE", "h", "10000"]);
        assert_eq!(run(&mut engine, &["PFCOUNT", "h"]), Resp::Integer(2));
        let Resp::Array(Some(registers)) = run(&mut engine, &["PFDEBUG", "GETREG", "h"]) else {
            panic!("register array");
        };
        assert_eq!(registers.len(), REGISTERS);
        for element in [b"foo", b"bar"] {
            let h = hash(element);
            let index = h as usize & (REGISTERS - 1);
            let rank = ((h >> 14) | (1 << 50)).trailing_zeros() as i64 + 1;
            assert_eq!(registers[index], Resp::Integer(rank));
        }
        assert_eq!(
            run(&mut engine, &["PFDEBUG", "TODENSE", "h"]),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, &["PFCOUNT", "h"]), Resp::Integer(2));
        assert_eq!(run(&mut engine, &["PTTL", "h"]), Resp::Integer(10000));
        assert!(matches!(
            run(&mut engine, &["PFDEBUG", "DECODE", "h"]),
            Resp::Error(_)
        ));
        assert!(matches!(
            run(&mut engine, &["PFDEBUG", "GETREG", "missing"]),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["EXISTS", "missing"]), Resp::Integer(0));
        assert_eq!(run(&mut engine, &["PFSELFTEST"]), Resp::Simple("OK"));
        assert_eq!(run(&mut engine, &["PFCOUNT", "h"]), Resp::Integer(2));
    }

    #[test]
    fn sparse_compression_roundtrips_and_does_not_revert_forced_dense() {
        let mut value = empty();
        for index in [0, 1, 3, 63, 64, 65, REGISTERS - 1] {
            set_register(&mut value, index, 17);
        }
        let original = value.to_vec();
        sparsify(&mut value);
        assert_eq!(value[4], 1);
        densify(&mut value);
        assert_eq!(&value[..], &original[..]);
        let mut engine = Engine::new();
        run(&mut engine, &["PFADD", "h"]);
        assert_eq!(
            run(&mut engine, &["PFDEBUG", "TODENSE", "h"]),
            Resp::Integer(1)
        );
        run(&mut engine, &["PFADD", "h", "new"]);
        assert_eq!(
            run(&mut engine, &["PFDEBUG", "ENCODING", "h"]),
            Resp::Simple("dense")
        );
    }
}
