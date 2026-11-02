//! Bounded, allocation-free decoding for resolved datatype effects.
use bytes::Bytes;
use std::io;

pub(crate) const MAX_EFFECT_BYTES: usize = 8 * 1024 * 1024;

fn invalid() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid datatype effect")
}

pub(crate) struct Encoder {
    bytes: Vec<u8>,
}
impl Encoder {
    pub(crate) fn new(version: u8, opcode: u8) -> Self {
        Self {
            bytes: vec![version, opcode],
        }
    }
    fn put(&mut self, bytes: &[u8]) -> io::Result<()> {
        if bytes.len() > MAX_EFFECT_BYTES.saturating_sub(self.bytes.len()) {
            return Err(invalid());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(())
    }
    pub(crate) fn u8(&mut self, value: u8) -> io::Result<()> {
        self.put(&[value])
    }
    pub(crate) fn u32(&mut self, value: u32) -> io::Result<()> {
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn u64(&mut self, value: u64) -> io::Result<()> {
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn i64(&mut self, value: i64) -> io::Result<()> {
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn f64(&mut self, value: f64) -> io::Result<()> {
        if value.is_nan() {
            return Err(invalid());
        }
        self.put(&value.to_le_bytes())
    }
    pub(crate) fn bytes(&mut self, value: &[u8]) -> io::Result<()> {
        let length = u32::try_from(value.len()).map_err(|_| invalid())?;
        if value
            .len()
            .checked_add(4)
            .is_none_or(|length| length > MAX_EFFECT_BYTES.saturating_sub(self.bytes.len()))
        {
            return Err(invalid());
        }
        self.u32(length)?;
        self.put(value)
    }
    pub(crate) fn finish(self) -> Bytes {
        Bytes::from(self.bytes)
    }
}

pub(crate) struct Decoder<'a> {
    bytes: &'a [u8],
    position: usize,
}
impl<'a> Decoder<'a> {
    pub(crate) fn new(bytes: &'a [u8], version: u8) -> io::Result<(u8, Self)> {
        if bytes.len() < 2 || bytes.len() > MAX_EFFECT_BYTES || bytes[0] != version {
            return Err(invalid());
        }
        Ok((bytes[1], Self { bytes, position: 2 }))
    }
    fn take(&mut self, count: usize) -> io::Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(count)
            .filter(|end| *end <= self.bytes.len())
            .ok_or_else(invalid)?;
        let result = &self.bytes[self.position..end];
        self.position = end;
        Ok(result)
    }
    pub(crate) fn u8(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }
    pub(crate) fn u32(&mut self) -> io::Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub(crate) fn u64(&mut self) -> io::Result<u64> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn i64(&mut self) -> io::Result<i64> {
        Ok(i64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub(crate) fn f64(&mut self) -> io::Result<f64> {
        let value = f64::from_le_bytes(self.take(8)?.try_into().unwrap());
        if value.is_nan() {
            return Err(invalid());
        }
        Ok(value)
    }
    pub(crate) fn bytes(&mut self) -> io::Result<&'a [u8]> {
        let count = self.u32()? as usize;
        self.take(count)
    }
    pub(crate) fn finish(self) -> io::Result<()> {
        if self.position == self.bytes.len() {
            Ok(())
        } else {
            Err(invalid())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_truncation_versions_trailing_data_and_nan() {
        let mut encoder = Encoder::new(1, 7);
        encoder.bytes(b"\0\xff").unwrap();
        encoder.i64(i64::MIN).unwrap();
        let bytes = encoder.finish();
        for end in 0..bytes.len() {
            assert!(
                Decoder::new(&bytes[..end], 1)
                    .and_then(|(_, mut d)| {
                        d.bytes()?;
                        d.i64()?;
                        d.finish()
                    })
                    .is_err()
            );
        }
        assert!(Decoder::new(&bytes, 2).is_err());
        let (_, mut decoder) = Decoder::new(&bytes, 1).unwrap();
        assert_eq!(decoder.bytes().unwrap(), b"\0\xff");
        assert_eq!(decoder.i64().unwrap(), i64::MIN);
        decoder.finish().unwrap();
        assert!(Encoder::new(1, 0).f64(f64::NAN).is_err());
        let mut nan = vec![1, 0];
        nan.extend_from_slice(&f64::NAN.to_le_bytes());
        assert!(Decoder::new(&nan, 1).unwrap().1.f64().is_err());
        assert!(Decoder::new(&[1, 0, 9], 1).unwrap().1.finish().is_err());
        assert!(
            Decoder::new(&[1, 0, 255, 255, 255, 255], 1)
                .unwrap()
                .1
                .bytes()
                .is_err()
        );
    }
    #[test]
    fn bounds_effect_before_growing_buffer() {
        let mut encoder = Encoder::new(1, 0);
        assert!(encoder.bytes(&vec![0; MAX_EFFECT_BYTES]).is_err());
        assert_eq!(encoder.finish().len(), 2);
    }
}
