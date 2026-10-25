//! MQTT 3.1.1 and 5.0 client.
//!
//! The connection runs on an IO lane of a [`zzq_tokio::Context`]. The
//! [`Client`] handle talks to it through channels and can be used from any
//! runtime. [`blocking::Client`] does the same for threads without one.
//!
//! # Examples
//!
//! A broker and a client in one context, over `inproc://`:
//!
//! ```
//! use zzq_broker::{Broker, BrokerOptions, Context};
//! use zzq_client::{Client, ClientOptions, QoS};
//!
//! # #[tokio::main(flavor = "current_thread")]
//! # async fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let ctx = Context::new()?;
//! let broker = Broker::new(&ctx, BrokerOptions::default()).await?;
//! broker.bind("inproc://docs").await?;
//!
//! let opts = ClientOptions::new("reader");
//! let mut client = Client::connect(&ctx, "inproc://docs", opts).await?;
//! client.subscribe("sensors/#", QoS::AtLeastOnce).await?;
//! client.publish("sensors/temp", "21.5", QoS::AtLeastOnce).await?;
//!
//! let message = client.recv().await.expect("a message");
//! assert_eq!(message.topic, "sensors/temp");
//! assert_eq!(message.payload, "21.5");
//! client.disconnect().await;
//! # Ok(())
//! # }
//! ```

#![forbid(unsafe_code)]

mod acks;
pub mod blocking;
mod check;
mod conn;
mod inbox;
mod submit;
mod topic;

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
pub use topic::Topic;
use zzq_proto::encode::PublishHeader;
pub use zzq_tokio::transport::TransportOptions;
#[cfg(feature = "tls")]
pub use zzq_tokio::transport::tls::TlsOptions;
pub use zzq_tokio::{Config, Context, Endpoint};

pub use zzq_proto::{QoS, ReasonCode, SubscriptionOptions, Version, reason};

/// Client errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The transport failed: the endpoint, the connection or TLS.
    #[error("transport failed")]
    Transport(#[from] zzq_tokio::Error),
    /// The broker refused the connection.
    #[error("connection refused: {0}")]
    Refused(ReasonCode),
    /// The broker rejected a request.
    #[error("rejected: {0}")]
    Rejected(ReasonCode),
    /// The broker broke the protocol.
    #[error("protocol error: {0}")]
    Protocol(&'static str),
    /// A topic name or filter is not valid.
    #[error("invalid topic {0:?}")]
    InvalidTopic(String),
    /// A string or binary field is longer than 65535 bytes.
    #[error("{0} longer than 65535 bytes")]
    TooLong(&'static str),
    /// A packet would be larger than MQTT can encode.
    #[error("packet larger than MQTT allows")]
    TooLarge,
    /// The connection or the client is closed.
    #[error("connection closed")]
    Closed,
    /// A blocking call ran inside an async runtime.
    #[error("blocking call inside an async runtime")]
    InAsync,
}

/// Client result.
pub type Result<T> = std::result::Result<T, Error>;

/// Will message sent by the broker if the connection is lost.
#[derive(Debug, Clone)]
pub struct LastWill {
    /// Will topic.
    pub topic: String,
    /// Will payload.
    pub payload: Bytes,
    /// QoS the will is published with.
    pub qos: QoS,
    /// The will is published as retained.
    pub retain: bool,
    /// v5 Will Delay Interval, seconds.
    pub delay: u32,
}

/// Connect settings.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// Client ID. Empty asks a v5 broker to assign one.
    pub client_id: String,
    /// Protocol version, v5 by default.
    pub version: Version,
    /// Start a new session instead of resuming one. A client that resumes
    /// keeps it on reconnects.
    pub clean_start: bool,
    /// Seconds. 0 disables keep alive.
    pub keep_alive: u16,
    /// v5 Session Expiry Interval, seconds.
    pub session_expiry: u32,
    /// User name.
    pub username: Option<String>,
    /// Password.
    pub password: Option<Bytes>,
    /// Will the broker publishes if the connection is lost.
    pub will: Option<LastWill>,
    /// Messages queued for [`Client::recv`] before the connection waits.
    /// `recv` takes the queue in batches, so up to twice this many can be
    /// in memory. QoS 1/2 messages are acked once taken: in groups while
    /// `recv` has more at hand, right away once it runs out. v5 brokers are
    /// asked to keep at most this many unacked.
    pub inbox: usize,
    /// Transport settings, such as trust anchors for `tls://`.
    pub transport: TransportOptions,
    /// Reconnect after a lost connection. `None` ends the client instead.
    pub reconnect: Option<Reconnect>,
    /// Time to connect and get CONNACK.
    pub connect_timeout: Duration,
}

/// Reconnect backoff: waits `min` after a loss, doubling up to `max` while
/// attempts fail.
#[derive(Debug, Clone, Copy)]
pub struct Reconnect {
    /// First wait after a loss.
    pub min: Duration,
    /// Longest wait between attempts.
    pub max: Duration,
}

impl Default for Reconnect {
    fn default() -> Self {
        Reconnect {
            min: Duration::from_millis(100),
            max: Duration::from_secs(30),
        }
    }
}

impl Default for ClientOptions {
    fn default() -> Self {
        ClientOptions {
            client_id: String::new(),
            version: Version::V5,
            clean_start: true,
            keep_alive: 60,
            session_expiry: 0,
            username: None,
            password: None,
            will: None,
            inbox: 4096,
            transport: TransportOptions::default(),
            reconnect: None,
            connect_timeout: Duration::from_secs(10),
        }
    }
}

impl ClientOptions {
    /// Options with a client ID.
    pub fn new(client_id: impl Into<String>) -> Self {
        ClientOptions {
            client_id: client_id.into(),
            ..Self::default()
        }
    }
}

/// A received message.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Message {
    /// Topic name, sharing the buffer the message was read into.
    pub topic: Topic,
    /// Payload.
    pub payload: Bytes,
    /// Delivery QoS.
    pub qos: QoS,
    /// The broker sent a retained message, or kept the flag for
    /// Retain As Published.
    pub retain: bool,
    /// v5 Subscription Identifiers of the matching subscriptions.
    pub subscription_ids: Vec<u32>,
    /// Packet ID to ack once the application takes the message.
    pub(crate) ack: Option<u16>,
}

/// CONNACK details.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Connected {
    /// The broker resumed a session.
    pub session_present: bool,
    /// Client ID assigned by a v5 broker, if the client sent none.
    pub assigned_client_id: Option<String>,
}

#[derive(Debug)]
pub(crate) enum Cmd {
    /// A queued QoS 1/2 PUBLISH, packet ID 0 until sent, and its ack slot.
    Publish { packet: Vec<u8>, slot: usize },
    Subscribe {
        filters: Vec<(String, SubscriptionOptions)>,
        sub_id: u32,
        done: oneshot::Sender<Result<Vec<ReasonCode>>>,
    },
    Unsubscribe {
        filters: Vec<String>,
        done: oneshot::Sender<Result<Vec<ReasonCode>>>,
    },
    Disconnect {
        reason: ReasonCode,
        done: oneshot::Sender<()>,
    },
}

/// A handle's way to its session task: requests go out, publish acks
/// come back.
#[derive(Debug)]
struct Link {
    submit: submit::Sender,
    acks: Arc<acks::Acks>,
    version: Version,
}

impl Link {
    /// Queues a publish, waiting while the queue is full. QoS 1/2 return
    /// the wait for the ack.
    async fn publish(
        &self,
        topic: &str,
        payload: &[u8],
        qos: QoS,
        retain: bool,
    ) -> Result<Option<acks::AckWait>> {
        check::publish(topic, payload.len())?;
        let h = PublishHeader {
            version: self.version,
            dup: false,
            qos,
            retain,
            topic,
            pkid: 0,
            properties: &[],
        };
        if qos == QoS::AtMostOnce {
            self.submit.publish(&h, payload, 0).await?;
            return Ok(None);
        }
        let mut wait = acks::AckWait::new(&self.acks);
        self.submit.publish(&h, payload, wait.slot()).await?;
        wait.queued();
        Ok(Some(wait))
    }

    fn send(&self, cmd: Cmd) -> Result<()> {
        self.submit.send(cmd)
    }
}

/// A starting session task.
struct Start {
    link: Link,
    inbox: inbox::Receiver,
    connack: oneshot::Receiver<Result<Connected>>,
}

fn start(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Start> {
    check::options(&opts)?;
    let endpoint = uri.parse::<Endpoint>()?;
    let (submit_tx, submit_rx) = submit::channel();
    let acks = Arc::new(acks::Acks::default());
    let (inbox_tx, inbox_rx) = inbox::channel(opts.inbox);
    let (connack_tx, connack_rx) = oneshot::channel();
    let ctx2 = ctx.clone();
    let transport = opts.transport.clone();
    let version = opts.version;
    let session = conn::Session::new(opts, submit_rx, acks.clone(), inbox_tx, connack_tx);
    ctx.next_lane()
        .spawn(move || conn::run_session(ctx2, endpoint, transport, session))?;
    Ok(Start {
        link: Link {
            submit: submit_tx,
            acks,
            version,
        },
        inbox: inbox_rx,
        connack: connack_rx,
    })
}

type Ack<T> = oneshot::Receiver<Result<T>>;

fn subscribe_cmd(
    filters: &[(&str, SubscriptionOptions)],
    sub_id: u32,
) -> Result<(Cmd, Ack<Vec<ReasonCode>>)> {
    check::filters(filters.iter().map(|(f, _)| *f))?;
    let (tx, rx) = oneshot::channel();
    let cmd = Cmd::Subscribe {
        filters: filters.iter().map(|(f, o)| ((*f).to_owned(), *o)).collect(),
        sub_id,
        done: tx,
    };
    Ok((cmd, rx))
}

fn unsubscribe_cmd(filters: &[&str]) -> Result<(Cmd, Ack<Vec<ReasonCode>>)> {
    check::filters(filters.iter().copied())?;
    let (tx, rx) = oneshot::channel();
    let cmd = Cmd::Unsubscribe {
        filters: filters.iter().map(|f| (*f).to_owned()).collect(),
        done: tx,
    };
    Ok((cmd, rx))
}

/// A connected client.
#[derive(Debug)]
pub struct Client {
    link: Link,
    inbox: inbox::Receiver,
    connected: Connected,
}

impl Client {
    /// Connects to `uri` and waits for CONNACK.
    ///
    /// # Errors
    ///
    /// [`Error::Transport`] if the URI is invalid or the connection or TLS
    /// fails, [`Error::Refused`] if the broker refuses it, and
    /// [`Error::InvalidTopic`] or [`Error::TooLong`] for options MQTT cannot
    /// carry.
    pub async fn connect(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Client> {
        let st = start(ctx, uri, opts)?;
        let connected = st.connack.await.map_err(|_| Error::Closed)??;
        Ok(Client {
            link: st.link,
            inbox: st.inbox,
            connected,
        })
    }

    /// CONNACK details.
    pub const fn connected(&self) -> &Connected {
        &self.connected
    }

    /// Publishes. QoS 0 returns once queued, waiting while about 1 MiB is
    /// queued already; QoS 1 and 2 wait for the ack.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTopic`] or [`Error::TooLarge`] for a message MQTT
    /// cannot carry, [`Error::Rejected`] if the broker refuses it, and
    /// [`Error::Closed`] once the client is gone.
    pub async fn publish(&self, topic: &str, payload: impl Into<Bytes>, qos: QoS) -> Result<()> {
        self.send_publish(topic, payload.into(), qos, false).await
    }

    /// Publishes a retained message: the broker keeps it for later
    /// subscribers. An empty payload clears the topic.
    ///
    /// # Errors
    ///
    /// As [`Client::publish`].
    pub async fn publish_retained(
        &self,
        topic: &str,
        payload: impl Into<Bytes>,
        qos: QoS,
    ) -> Result<()> {
        self.send_publish(topic, payload.into(), qos, true).await
    }

    async fn send_publish(
        &self,
        topic: &str,
        payload: Bytes,
        qos: QoS,
        retain: bool,
    ) -> Result<()> {
        match self.link.publish(topic, &payload, qos, retain).await? {
            Some(wait) => wait.await,
            None => Ok(()),
        }
    }

    /// Subscribes to one filter with a maximum QoS. Returns the granted
    /// QoS, or the reason the broker refused the filter.
    ///
    /// # Errors
    ///
    /// [`Error::InvalidTopic`] for an invalid filter, and [`Error::Closed`]
    /// once the client is gone.
    pub async fn subscribe(&self, filter: &str, qos: QoS) -> Result<ReasonCode> {
        let codes = self
            .subscribe_with(&[(filter, SubscriptionOptions::from(qos))], 0)
            .await?;
        Ok(codes
            .first()
            .copied()
            .unwrap_or(ReasonCode::UNSPECIFIED_ERROR))
    }

    /// Subscribes to several filters, optionally with a v5 Subscription
    /// Identifier. Returns one code per filter.
    ///
    /// # Errors
    ///
    /// As [`Client::subscribe`].
    pub async fn subscribe_with(
        &self,
        filters: &[(&str, SubscriptionOptions)],
        sub_id: u32,
    ) -> Result<Vec<ReasonCode>> {
        let (cmd, rx) = subscribe_cmd(filters, sub_id)?;
        self.link.send(cmd)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Unsubscribes. Returns one code per filter (empty for v3.1.1).
    ///
    /// # Errors
    ///
    /// As [`Client::subscribe`].
    pub async fn unsubscribe(&self, filters: &[&str]) -> Result<Vec<ReasonCode>> {
        let (cmd, rx) = unsubscribe_cmd(filters)?;
        self.link.send(cmd)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Next received message. `None` once the client is gone for good:
    /// disconnected, refused, or lost without [`ClientOptions::reconnect`].
    pub async fn recv(&mut self) -> Option<Message> {
        self.inbox.recv().await
    }

    /// Sends DISCONNECT and closes. [`ReasonCode::DISCONNECT_WITH_WILL`]
    /// asks a v5 broker for the will.
    pub async fn disconnect_with(self, reason: ReasonCode) {
        let (tx, rx) = oneshot::channel();
        if self.link.send(Cmd::Disconnect { reason, done: tx }).is_ok() {
            let _ = rx.await;
        }
    }

    /// Sends DISCONNECT and closes.
    pub async fn disconnect(self) {
        self.disconnect_with(ReasonCode::SUCCESS).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reconnect_backs_off_from_100_ms_to_30_s() {
        let r = Reconnect::default();
        assert_eq!(
            (r.min, r.max),
            (Duration::from_millis(100), Duration::from_secs(30))
        );
    }
}
