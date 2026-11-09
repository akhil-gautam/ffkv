//! Redis Cluster hash slots: CRC16 (XMODEM) of a key's hash tag, modulo 16384.

pub fn key_slot(key: &[u8]) -> u16 {
    let selected = if let Some(open) = key.iter().position(|b| *b == b'{') {
        match key[open + 1..].iter().position(|b| *b == b'}') {
            Some(close) if close > 0 => &key[open + 1..open + 1 + close],
            _ => key,
        }
    } else {
        key
    };
    let mut crc = 0u16;
    for byte in selected {
        crc ^= u16::from(*byte) << 8;
        for _ in 0..8 {
            crc = if crc & 0x8000 != 0 {
                (crc << 1) ^ 0x1021
            } else {
                crc << 1
            };
        }
    }
    crc & 16383
}
