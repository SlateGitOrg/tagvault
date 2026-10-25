//! Crate-wide error type.

use std::io;

/// Errors produced by tagvault.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Underlying filesystem or socket failure.
    #[error("i/o error: {0}")]
    Io(#[from] io::Error),
    /// On-disk data failed a checksum or structural check.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// A wire payload (protobuf, MQTT, batch) could not be decoded.
    #[error("decode error: {0}")]
    Decode(String),
    /// A caller supplied an invalid argument.
    #[error("invalid input: {0}")]
    InvalidInput(String),
    /// The requested tag does not exist.
    #[error("unknown tag: {0}")]
    UnknownTag(String),
    /// A configuration file could not be parsed.
    #[error("config error: {0}")]
    Config(String),
}

impl From<prost::DecodeError> for Error {
    fn from(e: prost::DecodeError) -> Self {
        Error::Decode(e.to_string())
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Error::Config(e.to_string())
    }
}

/// Convenience alias.
pub type Result<T> = std::result::Result<T, Error>;
