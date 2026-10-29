//! MQTT 3.1.1 and 5.0 broker.
//!
//! Each IO lane of the [`zzq_tokio::Context`] owns the sessions of the client
//! IDs that hash to it and a copy of the subscription index. Publishes are
//! matched on the publisher's lane; other lanes get each publish once, in
//! bounded batches, and deliver to their own sessions.
//! WebSocket and MQTT-SN clients reach the broker through in-memory MQTT
//! connections.
//!
//! # Examples
//!
//! A retained message reaches a client that subscribes later:
//!
//! ```
//! use zzq_broker::{Broker, BrokerOptions, Context, QoS};
//! use zzq_client::{Client, ClientOptions};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = Context::new()?;
//! let broker = Broker::new(&ctx, BrokerOptions::default()).await?;
//! let endpoint = broker.bind("tcp://127.0.0.1:0").await?;
//! broker.publish_retained("status", "up", QoS::AtLeastOnce)?;
//!
//! let opts = ClientOptions::new("late");
//! let mut client = Client::connect(&ctx, &endpoint.to_string(), opts).await?;
//! client.subscribe("status", QoS::AtLeastOnce).await?;
//! let message = client.recv().await.expect("the retained message");
//! assert!(message.retain);
//! assert_eq!(message.payload, "up");
//! # Ok(())
//! # }
//! ```

mod broker;
mod budget;
mod cluster;
mod conn;
mod disk;
mod dump;
mod durable;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod index;
mod lane;
mod mailbox;
mod msg;
mod options;
mod retain;
mod session;
mod slab;
mod sn;
mod state;
mod sys;
#[cfg(test)]
mod tests;
mod time;

// The shared test support names the crate.
#[cfg(test)]
extern crate self as zzq_broker;

pub use broker::{Broker, Dropped, Stats, lane_for};
pub use options::{
    AuthDecision, Authenticator, Authorizer, BrokerOptions, ClientInfo, ConnectInfo, SnOptions,
};
#[doc(hidden)]
pub use options::{ClusterLinks, ClusterOptions, ClusterStart};
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
    /// [`BrokerOptions::data_dir`] could not be opened or read.
    #[error("data directory failed")]
    DataDir(#[source] std::io::Error),
    /// [`BrokerOptions::state_file`] could not be read or written.
    #[error("state file failed")]
    State(#[source] std::io::Error),
    /// A [`BrokerOptions`] field has a value the broker cannot work with.
    #[error("invalid option: {0}")]
    InvalidOption(&'static str),
}

/// Broker result.
pub type Result<T> = std::result::Result<T, Error>;
