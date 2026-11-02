use super::node::{Node, V};
use super::{MAX_BYTES, MAX_DEPTH, MAX_NODES, Resp, invalid, limit};

// BSON's document reader is recursive. Validate framing, depth and node count
// before handing it any input, including deprecated code-with-scope values.
pub(super) fn parse(bytes: &[u8]) -> Result<V, Resp> {
    if bytes.len() > MAX_BYTES {
        return Err(limit());
    }
    let mut nodes = 0;
    document(bytes, 1, &mut nodes)?;
    let mut cursor = std::io::Cursor::new(bytes);
    let document = ::bson::Document::from_reader(&mut cursor).map_err(|_| invalid())?;
    let value = ::bson::from_document::<serde_json::Value>(document).map_err(|_| invalid())?;
    Node::from_value(value)
}
fn take<'a>(bytes: &'a [u8], position: &mut usize, count: usize) -> Result<&'a [u8], Resp> {
    let end = position
        .checked_add(count)
        .filter(|end| *end <= bytes.len())
        .ok_or_else(invalid)?;
    let value = &bytes[*position..end];
    *position = end;
    Ok(value)
}
fn length(bytes: &[u8], position: &mut usize) -> Result<usize, Resp> {
    let raw = take(bytes, position, 4)?;
    usize::try_from(i32::from_le_bytes(raw.try_into().unwrap())).map_err(|_| invalid())
}
fn cstring(bytes: &[u8], position: &mut usize) -> Result<(), Resp> {
    let rest = bytes.get(*position..).ok_or_else(invalid)?;
    let size = rest.iter().position(|b| *b == 0).ok_or_else(invalid)?;
    std::str::from_utf8(&rest[..size]).map_err(|_| invalid())?;
    take(bytes, position, size + 1)?;
    Ok(())
}
fn string(bytes: &[u8], position: &mut usize) -> Result<(), Resp> {
    let size = length(bytes, position)?;
    if size == 0 {
        return Err(invalid());
    }
    let value = take(bytes, position, size)?;
    if value.last() != Some(&0) {
        return Err(invalid());
    }
    std::str::from_utf8(&value[..size - 1]).map_err(|_| invalid())?;
    Ok(())
}
fn nested(bytes: &[u8], position: &mut usize, depth: usize, nodes: &mut usize) -> Result<(), Resp> {
    let mut end = *position;
    let size = length(bytes, &mut end)?;
    let value = take(bytes, position, size)?;
    document(value, depth, nodes)
}
fn document(bytes: &[u8], depth: usize, nodes: &mut usize) -> Result<(), Resp> {
    if depth > MAX_DEPTH {
        return Err(limit());
    }
    let mut position = 0;
    let size = length(bytes, &mut position)?;
    if size < 5 || size != bytes.len() || bytes.last() != Some(&0) {
        return Err(invalid());
    }
    *nodes += 1;
    if *nodes > MAX_NODES {
        return Err(limit());
    }
    while position < size - 1 {
        let kind = take(bytes, &mut position, 1)?[0];
        cstring(bytes, &mut position)?;
        *nodes += 1;
        if *nodes > MAX_NODES {
            return Err(limit());
        }
        match kind {
            0x01 => {
                let raw = take(bytes, &mut position, 8)?;
                if !f64::from_le_bytes(raw.try_into().unwrap()).is_finite() {
                    return Err(invalid());
                }
            }
            0x02 | 0x0d | 0x0e => string(bytes, &mut position)?,
            0x03 | 0x04 => nested(bytes, &mut position, depth + 1, nodes)?,
            0x05 => {
                let size = length(bytes, &mut position)?;
                take(bytes, &mut position, 1)?;
                take(bytes, &mut position, size)?;
            }
            0x06 | 0x0a | 0x7f | 0xff => {}
            0x07 => {
                take(bytes, &mut position, 12)?;
            }
            0x08 => {
                if take(bytes, &mut position, 1)?[0] > 1 {
                    return Err(invalid());
                }
            }
            0x09 | 0x11 | 0x12 => {
                take(bytes, &mut position, 8)?;
            }
            0x0b => {
                cstring(bytes, &mut position)?;
                cstring(bytes, &mut position)?;
            }
            0x0c => {
                string(bytes, &mut position)?;
                take(bytes, &mut position, 12)?;
            }
            0x0f => {
                let begin = position;
                let total = length(bytes, &mut position)?;
                let end = begin
                    .checked_add(total)
                    .filter(|end| *end < size)
                    .ok_or_else(invalid)?;
                string(bytes, &mut position)?;
                nested(bytes, &mut position, depth + 1, nodes)?;
                if position != end {
                    return Err(invalid());
                }
            }
            0x10 => {
                take(bytes, &mut position, 4)?;
            }
            0x13 => {
                take(bytes, &mut position, 16)?;
            }
            _ => return Err(invalid()),
        }
        if position > size - 1 {
            return Err(invalid());
        }
    }
    if position != size - 1 {
        return Err(invalid());
    }
    Ok(())
}
