//! MQTT 3.1.1 and 5.0 client.
//!
//! The connection runs on an IO lane of a [`zzq_tokio::Context`]. The
//! [`Client`] handle talks to it through channels and can be used from any
//! runtime.

#![forbid(unsafe_code)]

pub mod blocking;
mod check;
mod conn;

use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use zzq_proto::{QoS, SubscriptionOptions, Version};
pub use zzq_tokio::transport::TransportOptions;
#[cfg(feature = "tls")]
pub use zzq_tokio::transport::tls::TlsOptions;
use zzq_tokio::{Context, Endpoint};

pub use zzq_proto::{ReasonCode, reason};

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
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    /// v5 Will Delay Interval, seconds.
    pub delay: u32,
}

/// Connect settings.
#[derive(Debug, Clone)]
pub struct ClientOptions {
    pub client_id: String,
    pub version: Version,
    pub clean_start: bool,
    /// Seconds. 0 disables keep alive.
    pub keep_alive: u16,
    /// v5 Session Expiry Interval, seconds.
    pub session_expiry: u32,
    pub username: Option<String>,
    pub password: Option<Bytes>,
    pub will: Option<LastWill>,
    /// Messages buffered for [`Client::recv`] before the connection waits.
    /// QoS 1/2 messages are acked when taken, and v5 brokers are asked to
    /// keep at most this many unacked.
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
    pub min: Duration,
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
    pub topic: String,
    pub payload: Bytes,
    pub qos: QoS,
    pub retain: bool,
    /// v5 Subscription Identifiers.
    pub subscription_ids: Vec<u32>,
    /// Packet ID to ack once the application takes the message.
    pub(crate) ack: Option<u16>,
}

/// CONNACK details.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Connected {
    pub session_present: bool,
    /// Client ID assigned by a v5 broker, if the client sent none.
    pub assigned_client_id: Option<String>,
}

#[derive(Debug)]
pub(crate) enum Cmd {
    Publish {
        topic: String,
        payload: Bytes,
        qos: QoS,
        retain: bool,
        done: Option<oneshot::Sender<Result<()>>>,
    },
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
    /// The application took a QoS 1/2 message: ack it.
    Ack { pkid: u16, qos: QoS },
}

/// Acks a QoS 1/2 message now that the application has it, so the
/// broker's window, and with it the inbox, stays bounded.
fn ack(cmd: &mpsc::UnboundedSender<Cmd>, m: &Message) {
    if let Some(pkid) = m.ack {
        let _ = cmd.send(Cmd::Ack { pkid, qos: m.qos });
    }
}

/// Channels to a starting session task.
struct Start {
    cmd: mpsc::UnboundedSender<Cmd>,
    inbox: mpsc::Receiver<Message>,
    connack: oneshot::Receiver<Result<Connected>>,
}

fn start(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Start> {
    check::options(&opts)?;
    let endpoint = uri.parse::<Endpoint>()?;
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let (inbox_tx, inbox_rx) = mpsc::channel(opts.inbox.max(1));
    let (ack_tx, ack_rx) = oneshot::channel();
    let ctx2 = ctx.clone();
    let transport = opts.transport.clone();
    let session = conn::Session::new(opts, cmd_rx, inbox_tx, ack_tx);
    ctx.next_lane()
        .spawn(move || conn::run_session(ctx2, endpoint, transport, session))?;
    Ok(Start {
        cmd: cmd_tx,
        inbox: inbox_rx,
        connack: ack_rx,
    })
}

type Ack<T> = oneshot::Receiver<Result<T>>;

fn publish_cmd(
    topic: &str,
    payload: Bytes,
    qos: QoS,
    retain: bool,
) -> Result<(Cmd, Option<Ack<()>>)> {
    check::publish(topic, payload.len())?;
    let (tx, rx) = if qos == QoS::AtMostOnce {
        (None, None)
    } else {
        let (tx, rx) = oneshot::channel();
        (Some(tx), Some(rx))
    };
    let cmd = Cmd::Publish {
        topic: topic.to_owned(),
        payload,
        qos,
        retain,
        done: tx,
    };
    Ok((cmd, rx))
}

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
    cmd: mpsc::UnboundedSender<Cmd>,
    inbox: mpsc::Receiver<Message>,
    connected: Connected,
}

impl Client {
    /// Connects to `uri` and waits for CONNACK.
    pub async fn connect(ctx: &Context, uri: &str, opts: ClientOptions) -> Result<Client> {
        let st = start(ctx, uri, opts)?;
        let connected = st.connack.await.map_err(|_| Error::Closed)??;
        Ok(Client {
            cmd: st.cmd,
            inbox: st.inbox,
            connected,
        })
    }

    /// CONNACK details.
    pub const fn connected(&self) -> &Connected {
        &self.connected
    }

    /// Publishes. QoS 0 returns once queued; QoS 1 and 2 wait for the ack.
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
        let (cmd, rx) = publish_cmd(topic, payload, qos, retain)?;
        self.send(cmd)?;
        match rx {
            Some(rx) => rx.await.map_err(|_| Error::Closed)?,
            None => Ok(()),
        }
    }

    /// Subscribes to one filter with a maximum QoS. Returns the granted code.
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
    pub async fn subscribe_with(
        &self,
        filters: &[(&str, SubscriptionOptions)],
        sub_id: u32,
    ) -> Result<Vec<ReasonCode>> {
        let (cmd, rx) = subscribe_cmd(filters, sub_id)?;
        self.send(cmd)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Unsubscribes. Returns one code per filter (empty for v3.1.1).
    pub async fn unsubscribe(&self, filters: &[&str]) -> Result<Vec<ReasonCode>> {
        let (cmd, rx) = unsubscribe_cmd(filters)?;
        self.send(cmd)?;
        rx.await.map_err(|_| Error::Closed)?
    }

    /// Next received message. `None` once the client is gone for good:
    /// disconnected, refused, or lost without [`ClientOptions::reconnect`].
    pub async fn recv(&mut self) -> Option<Message> {
        let m = self.inbox.recv().await?;
        ack(&self.cmd, &m);
        Some(m)
    }

    /// Sends DISCONNECT and closes. [`ReasonCode::DISCONNECT_WITH_WILL`]
    /// asks a v5 broker for the will.
    pub async fn disconnect_with(self, reason: ReasonCode) {
        let (tx, rx) = oneshot::channel();
        if self.send(Cmd::Disconnect { reason, done: tx }).is_ok() {
            let _ = rx.await;
        }
    }

    /// Sends DISCONNECT and closes.
    pub async fn disconnect(self) {
        self.disconnect_with(ReasonCode::SUCCESS).await;
    }

    fn send(&self, cmd: Cmd) -> Result<()> {
        self.cmd.send(cmd).map_err(|_| Error::Closed)
    }
}
