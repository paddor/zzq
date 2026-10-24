use std::io;

/// A source error from a dependency, such as rustls or quinn.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Engine errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A socket or file operation failed.
    #[error("I/O error")]
    Io(#[from] io::Error),
    /// An endpoint URI does not parse.
    #[error("invalid endpoint {uri:?}: {why}")]
    InvalidEndpoint {
        /// The URI as given.
        uri: String,
        /// What is wrong with it.
        why: &'static str,
    },
    /// The transport is not built in, such as `wss` without the `tls`
    /// feature.
    #[error("transport not available: {0}")]
    Unsupported(&'static str),
    /// Another socket is bound to the endpoint.
    #[error("address in use: {endpoint}")]
    AddressInUse {
        /// The endpoint that failed to bind.
        endpoint: String,
        /// The bind error.
        #[source]
        source: io::Error,
    },
    /// Nothing accepts connections at the endpoint.
    #[error("connection refused: {endpoint}")]
    Refused {
        /// The endpoint that refused.
        endpoint: String,
        /// The transport's error, if it had one.
        #[source]
        source: Option<BoxError>,
    },
    /// The context's lanes have stopped.
    #[error("context is shut down")]
    Closed,
    /// TLS or QUIC setup or handshake failed.
    #[error("tls: {what}")]
    Tls {
        /// The step that failed.
        what: &'static str,
        /// The TLS or QUIC library's error, if it had one.
        #[source]
        source: Option<BoxError>,
    },
}

/// Engine result.
pub type Result<T> = std::result::Result<T, Error>;
