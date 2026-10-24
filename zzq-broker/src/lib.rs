//! MQTT 3.1.1 and 5.0 broker.
//!
//! Each IO lane of the [`zzq_tokio::Context`] owns the sessions of the client
//! IDs that hash to it (see [`lane_for`]) and a copy of the subscription
//! index. Publishes are matched on the publisher's lane; other lanes get
//! each publish once, in bounded batches, and deliver to their own sessions.
//! WebSocket and MQTT-SN clients reach the broker through in-memory MQTT
//! connections.

mod broker;
mod conn;
mod index;
mod lane;
mod msg;
mod options;
mod persist;
mod retain;
mod session;
mod slab;
mod sn;
mod sys;

pub use broker::{Broker, Stats, lane_for};
pub use options::{
    AuthDecision, Authenticator, Authorizer, BrokerOptions, ClientInfo, ConnectInfo,
};
pub use persist::{FilePersistence, Persistence, Retained};
pub use zzq_proto::{QoS, ReasonCode};
pub use zzq_tokio::transport::TransportOptions;
#[cfg(feature = "tls")]
pub use zzq_tokio::transport::tls::TlsOptions;
pub use zzq_tokio::{Config, Context, Endpoint};

/// Broker errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The transport failed: the endpoint, a listener or the context.
    #[error("transport failed")]
    Transport(#[from] zzq_tokio::Error),
    /// A topic name is not valid for a publish.
    #[error("invalid topic: {0:?}")]
    InvalidTopic(String),
    /// A message would be larger than MQTT can encode.
    #[error("message larger than MQTT allows")]
    TooLarge,
    /// The broker is shut down.
    #[error("broker is shut down")]
    Closed,
    /// The persistence backend failed to load retained messages.
    #[error("loading retained messages failed")]
    Persistence(#[source] std::io::Error),
}

/// Broker result.
pub type Result<T> = std::result::Result<T, Error>;
