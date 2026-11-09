use bytes::Bytes;
use std::io;

#[derive(Debug, PartialEq)]
pub enum Frame {
    Simple(Bytes),
    Error(Bytes),
    Integer(i64),
    Bulk(Bytes),
    Null,
    Array(Vec<Frame>),
    Map(Vec<(Frame, Frame)>),
    Set(Vec<Frame>),
    Double(f64),
    Boolean(bool),
    BigNumber(Bytes),
    Verbatim { format: [u8; 3], data: Bytes },
    Push(Vec<Frame>),
    Attribute(Vec<(Frame, Frame)>, Box<Frame>),
}

impl Frame {
    pub fn is_push(&self) -> bool {
        match self {
            Self::Push(_) => true,
            Self::Attribute(_, value) => value.is_push(),
            _ => false,
        }
    }

    pub fn has_error(&self) -> bool {
        match self {
            Self::Error(_) => true,
            Self::Array(values) | Self::Set(values) | Self::Push(values) => {
                values.iter().any(Self::has_error)
            }
            Self::Map(values) => values.iter().any(|(k, v)| k.has_error() || v.has_error()),
            Self::Attribute(values, value) => {
                value.has_error() || values.iter().any(|(k, v)| k.has_error() || v.has_error())
            }
            _ => false,
        }
    }
}

pub(super) fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
