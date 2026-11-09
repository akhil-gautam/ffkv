//! ffkv-cli's reply decoding, without its network transport.
#[path = "../client/decoder.rs"]
mod decoder;
#[path = "../client/frame.rs"]
mod frame;

pub use frame::Frame;
use frame::invalid;

/// Decodes one complete RESP reply exactly as ffkv-cli reads it.
pub(crate) fn decode(wire: &[u8]) -> std::io::Result<Frame> {
    let mut decoder = decoder::Decoder::new();
    decoder.feed(wire);
    decoder
        .decode()?
        .ok_or_else(|| invalid("incomplete reply frame"))
}
