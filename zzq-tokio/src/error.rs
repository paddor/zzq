use std::io;

/// Engine errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("io: {0}")]
    Io(#[from] io::Error),
    #[error("invalid endpoint {0:?}: {1}")]
    InvalidEndpoint(String, &'static str),
    #[error("transport not available: {0}")]
    Unsupported(&'static str),
    #[error("address in use: {0}")]
    AddressInUse(String),
    #[error("connection refused: {0}")]
    Refused(String),
    #[error("context is shut down")]
    Closed,
    #[error("tls: {0}")]
    Tls(String),
}

/// Engine result.
pub type Result<T> = std::result::Result<T, Error>;
