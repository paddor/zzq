use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use zzq_proto::{ReasonCode, Version};
use zzq_tokio::transport::TransportOptions;

/// What a client presented in CONNECT.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ConnectInfo<'a> {
    /// Client ID, empty if the client wants one assigned.
    pub client_id: &'a str,
    /// User name.
    pub username: Option<&'a str>,
    /// Password.
    pub password: Option<&'a [u8]>,
    /// Protocol version.
    pub version: Version,
    /// DER of the certificate the client presented over `tls://` or
    /// `quic://`, if the listener asks for one with `client_ca_pem`.
    pub peer_cert: Option<&'a [u8]>,
}

/// Result of [`Authenticator::authenticate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthDecision {
    /// Lets the client connect.
    Allow,
    /// Rejects with a v5 CONNACK reason code. v3.1.1 clients get the closest
    /// return code.
    Deny(ReasonCode),
}

/// Decides whether a client may connect.
pub trait Authenticator: Send + Sync + 'static {
    /// Decides on one CONNECT. Runs on a lane thread and must not block.
    fn authenticate(&self, info: &ConnectInfo<'_>) -> AuthDecision;
}

/// The client a publish or subscribe comes from. MQTT-SN QoS -1 publishes
/// come from a client with an empty ID, and are authenticated as one.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ClientInfo<'a> {
    /// Client ID.
    pub client_id: &'a str,
    /// User name from CONNECT.
    pub username: Option<&'a str>,
}

/// Decides what a connected client may publish and subscribe to.
pub trait Authorizer: Send + Sync + 'static {
    /// Whether the client may publish to `topic`. Asked when the topic of a
    /// client's publishes changes.
    fn may_publish(&self, client: &ClientInfo<'_>, topic: &str) -> bool;

    /// Whether the client may subscribe to `filter`. Shared subscriptions
    /// ask [`Authorizer::may_subscribe_shared`] instead.
    fn may_subscribe(&self, client: &ClientInfo<'_>, filter: &str) -> bool;

    /// Whether the client may join the share group `group` on `filter`,
    /// the filter after `$share/{group}/`. By default the rule of
    /// [`Authorizer::may_subscribe`] for `filter`.
    fn may_subscribe_shared(&self, client: &ClientInfo<'_>, group: &str, filter: &str) -> bool {
        let _ = group;
        self.may_subscribe(client, filter)
    }
}

/// Broker settings.
#[derive(Clone)]
pub struct BrokerOptions {
    /// Largest packet accepted from clients, and advertised to v5 clients.
    pub max_packet_size: usize,
    /// QoS 1/2 messages in flight per session, also capped by the client's
    /// Receive Maximum.
    pub max_inflight: u16,
    /// QoS 1/2 messages queued per session beyond the inflight window. The
    /// oldest is dropped and counted when full.
    pub max_queued: usize,
    /// Bytes of topics, properties and payloads of the QoS 1/2 messages
    /// queued per session beyond the inflight window. The oldest are
    /// dropped and counted when full; a message larger than this is
    /// dropped.
    pub max_queued_bytes: usize,
    /// QoS 1/2 messages queued for a connected session before publishers that
    /// feed it get their acks held back. Acks resume at half of this.
    pub qos_queue_high: usize,
    /// Bytes waiting to be written per connection before QoS 0 messages to it
    /// are dropped and counted.
    pub max_out_bytes: usize,
    /// QoS 2 messages from one client awaiting PUBREL, and the Receive
    /// Maximum advertised to v5 clients.
    pub receive_maximum: u16,
    /// Subscriptions per session. More get SUBACK Quota Exceeded. Each one is
    /// held by every lane.
    pub max_subscriptions: usize,
    /// Retained topics stored. Retained messages for new topics beyond it
    /// are delivered but not stored, and counted.
    pub max_retained: usize,
    /// Approximate bytes of broker state: sessions, their subscriptions and
    /// the QoS 1/2 messages they hold, and retained messages. A message that
    /// sessions of one lane share counts once, and so does a retained
    /// message sessions got on subscribe. Lanes check it 20 times a second.
    /// While it is reached, new sessions that would outlive their connection
    /// get CONNACK Quota Exceeded, messages that would wait in a session
    /// queue are dropped and counted, and retained messages for new topics
    /// are not stored.
    pub max_state_bytes: usize,
    /// Levels a topic name or filter may have. Deeper ones are rejected,
    /// which bounds the work and stack depth of matching.
    pub max_topic_levels: usize,
    /// Topic aliases a v5 client may use towards the broker.
    pub topic_alias_maximum: u16,
    /// Upper bound for the session expiry interval clients ask for, seconds.
    pub max_session_expiry: u32,
    /// Connected clients on all lanes. More get CONNACK Quota Exceeded; a
    /// client taking over its own session always gets in.
    pub max_connections: usize,
    /// Upper bound for client keep alive, seconds. Clients asking for more,
    /// or for none, get this; v5 clients learn it from Server Keep Alive.
    pub max_keep_alive: Option<u16>,
    /// Time a new connection has to send CONNECT.
    pub connect_timeout: Duration,
    /// Optional connect authentication. `None` allows everyone.
    pub authenticator: Option<Arc<dyn Authenticator>>,
    /// Optional publish and subscribe authorization. `None` allows all.
    pub authorizer: Option<Arc<dyn Authorizer>>,
    /// Durable storage for retained messages. `None` keeps them in memory
    /// only.
    pub persistence: Option<Arc<dyn crate::Persistence>>,
    /// File that keeps sessions over a restart: [`Broker::shutdown`]
    /// closes every connection and writes it, and [`Broker::new`] loads it
    /// and renames it to `<name>.loaded`. It holds the retained messages
    /// too, unless `persistence` keeps them. Sessions that end with their
    /// connection are left out. The file is compressed with Zstandard.
    ///
    /// [`Broker::shutdown`]: crate::Broker::shutdown
    /// [`Broker::new`]: crate::Broker::new
    pub state_file: Option<PathBuf>,
    /// Interval of the retained `$SYS/broker/...` status messages. `None`
    /// turns them off.
    pub sys_interval: Option<Duration>,
    /// Transport settings, such as certificates for `tls://`.
    pub transport: TransportOptions,
    /// The MQTT-SN gateway on `udp://` listeners.
    pub sn: SnOptions,
}

impl Default for BrokerOptions {
    fn default() -> Self {
        BrokerOptions {
            max_packet_size: 16 << 20,
            max_inflight: 1024,
            max_queued: 100_000,
            max_queued_bytes: usize::MAX,
            qos_queue_high: 1024,
            max_out_bytes: 64 << 20,
            receive_maximum: 65_535,
            max_subscriptions: usize::MAX,
            max_retained: usize::MAX,
            max_state_bytes: usize::MAX,
            max_topic_levels: 128,
            topic_alias_maximum: 1024,
            max_session_expiry: u32::MAX,
            max_connections: usize::MAX,
            max_keep_alive: None,
            connect_timeout: Duration::from_secs(10),
            authenticator: None,
            authorizer: None,
            persistence: None,
            state_file: None,
            sys_interval: Some(Duration::from_secs(10)),
            transport: TransportOptions::default(),
            sn: SnOptions::default(),
        }
    }
}

impl fmt::Debug for BrokerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerOptions")
            .field("max_packet_size", &self.max_packet_size)
            .field("max_inflight", &self.max_inflight)
            .field("max_queued", &self.max_queued)
            .field("max_queued_bytes", &self.max_queued_bytes)
            .field("qos_queue_high", &self.qos_queue_high)
            .field("max_out_bytes", &self.max_out_bytes)
            .field("receive_maximum", &self.receive_maximum)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("max_retained", &self.max_retained)
            .field("max_state_bytes", &self.max_state_bytes)
            .field("max_topic_levels", &self.max_topic_levels)
            .field("topic_alias_maximum", &self.topic_alias_maximum)
            .field("max_session_expiry", &self.max_session_expiry)
            .field("max_connections", &self.max_connections)
            .field("max_keep_alive", &self.max_keep_alive)
            .field("connect_timeout", &self.connect_timeout)
            .field("authenticator", &self.authenticator.is_some())
            .field("authorizer", &self.authorizer.is_some())
            .field("persistence", &self.persistence.is_some())
            .field("state_file", &self.state_file)
            .field("sys_interval", &self.sys_interval)
            .field("transport", &self.transport)
            .field("sn", &self.sn)
            .finish()
    }
}

/// MQTT-SN gateway settings.
#[derive(Debug, Clone)]
pub struct SnOptions {
    /// Gateway ID, sent in GWINFO.
    pub gateway_id: u8,
    /// Predefined topic IDs and their names.
    pub predefined: Vec<(u16, String)>,
    /// Retransmission interval for unacked messages to a client.
    pub retry: Duration,
    /// Retransmissions before the gateway gives up on a client.
    pub retries: u8,
    /// Datagrams held for a sleeping client; the oldest is dropped.
    pub sleep_buffer: usize,
    /// Topic IDs per client, registered by it or assigned by the gateway.
    /// More REGISTERs get Rejected: congestion, and messages on further
    /// topics are dropped.
    pub max_topics: usize,
}

impl Default for SnOptions {
    fn default() -> Self {
        SnOptions {
            gateway_id: 1,
            predefined: Vec::new(),
            retry: Duration::from_secs(10),
            retries: 5,
            sleep_buffer: 1000,
            max_topics: 4096,
        }
    }
}
