use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use zzq_proto::{ReasonCode, Version};
use zzq_tokio::transport::TransportOptions;

/// What a client presented in CONNECT.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ConnectInfo<'a> {
    pub client_id: &'a str,
    pub username: Option<&'a str>,
    pub password: Option<&'a [u8]>,
    pub version: Version,
    /// DER of the certificate the client presented over `tls://` or
    /// `quic://`, if the listener asks for one with `client_ca_pem`.
    pub peer_cert: Option<&'a [u8]>,
}

/// Result of [`Authenticator::authenticate`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AuthDecision {
    Allow,
    /// Rejects with a v5 CONNACK reason code. v3.1.1 clients get the closest
    /// return code.
    Deny(ReasonCode),
}

/// Decides whether a client may connect.
pub trait Authenticator: Send + Sync + 'static {
    fn authenticate(&self, info: &ConnectInfo<'_>) -> AuthDecision;
}

/// The client a publish or subscribe comes from. MQTT-SN QoS -1 publishes
/// come from a client with an empty ID, and are authenticated as one.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub struct ClientInfo<'a> {
    pub client_id: &'a str,
    pub username: Option<&'a str>,
}

/// Decides what a connected client may publish and subscribe to.
pub trait Authorizer: Send + Sync + 'static {
    /// Whether the client may publish to `topic`. Asked when the topic of a
    /// client's publishes changes.
    fn may_publish(&self, client: &ClientInfo<'_>, topic: &str) -> bool;

    /// Whether the client may subscribe to `filter`.
    fn may_subscribe(&self, client: &ClientInfo<'_>, filter: &str) -> bool;
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
    /// Interval of the retained `$SYS/broker/...` status messages. `None`
    /// turns them off.
    pub sys_interval: Option<Duration>,
    /// Transport settings, such as certificates for `tls://`.
    pub transport: TransportOptions,
    /// MQTT-SN gateway ID, sent in GWINFO.
    pub sn_gateway_id: u8,
    /// MQTT-SN predefined topic IDs and their names.
    pub sn_predefined: Vec<(u16, String)>,
    /// MQTT-SN retransmission interval for unacked messages to a client.
    pub sn_retry: Duration,
    /// MQTT-SN retransmissions before the gateway gives up on a client.
    pub sn_retries: u8,
    /// Datagrams held for a sleeping MQTT-SN client; the oldest is dropped.
    pub sn_sleep_buffer: usize,
    /// Topic IDs per MQTT-SN client, registered by it or assigned by the
    /// gateway. More REGISTERs get Rejected: congestion, and messages on
    /// further topics are dropped.
    pub sn_max_topics: usize,
}

impl Default for BrokerOptions {
    fn default() -> Self {
        BrokerOptions {
            max_packet_size: 16 << 20,
            max_inflight: 1024,
            max_queued: 100_000,
            qos_queue_high: 1024,
            max_out_bytes: 64 << 20,
            receive_maximum: 65_535,
            max_subscriptions: usize::MAX,
            max_retained: usize::MAX,
            max_topic_levels: 128,
            topic_alias_maximum: 1024,
            max_session_expiry: u32::MAX,
            max_connections: usize::MAX,
            max_keep_alive: None,
            connect_timeout: Duration::from_secs(10),
            authenticator: None,
            authorizer: None,
            persistence: None,
            sys_interval: Some(Duration::from_secs(10)),
            transport: TransportOptions::default(),
            sn_gateway_id: 1,
            sn_predefined: Vec::new(),
            sn_retry: Duration::from_secs(10),
            sn_retries: 5,
            sn_sleep_buffer: 1000,
            sn_max_topics: 4096,
        }
    }
}

impl fmt::Debug for BrokerOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrokerOptions")
            .field("max_packet_size", &self.max_packet_size)
            .field("max_inflight", &self.max_inflight)
            .field("max_queued", &self.max_queued)
            .field("qos_queue_high", &self.qos_queue_high)
            .field("max_out_bytes", &self.max_out_bytes)
            .field("receive_maximum", &self.receive_maximum)
            .field("max_subscriptions", &self.max_subscriptions)
            .field("max_retained", &self.max_retained)
            .field("max_topic_levels", &self.max_topic_levels)
            .field("topic_alias_maximum", &self.topic_alias_maximum)
            .field("max_session_expiry", &self.max_session_expiry)
            .field("max_connections", &self.max_connections)
            .field("max_keep_alive", &self.max_keep_alive)
            .field("connect_timeout", &self.connect_timeout)
            .field("authenticator", &self.authenticator.is_some())
            .field("authorizer", &self.authorizer.is_some())
            .field("persistence", &self.persistence.is_some())
            .field("sys_interval", &self.sys_interval)
            .field("transport", &self.transport)
            .field("sn_gateway_id", &self.sn_gateway_id)
            .field("sn_predefined", &self.sn_predefined)
            .field("sn_retry", &self.sn_retry)
            .field("sn_retries", &self.sn_retries)
            .field("sn_sleep_buffer", &self.sn_sleep_buffer)
            .field("sn_max_topics", &self.sn_max_topics)
            .finish()
    }
}
