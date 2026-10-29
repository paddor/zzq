//! Broker options: limits, storage, the authentication and authorization
//! hooks, and the MQTT-SN gateway.

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
    /// return code. A code below 0x80, which would accept, becomes Not
    /// authorized.
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
    /// Bytes waiting to be written per connection. Beyond it, QoS 0 messages
    /// to the connection are dropped and counted, and the connection is not
    /// read until its writer catches up.
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
    /// are not stored. With `data_dir`, the same holds while its device has
    /// less room than the state takes and two segments, so every journal
    /// can still write a snapshot and drop the segments before it.
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
    /// Directory for state that survives a crash: retained messages, and
    /// the sessions that outlive their connection with their
    /// subscriptions and QoS 1/2 messages. An ack waits until what it
    /// stands for is on disk: PUBACK or PUBREC for the message, retained or
    /// queued for such sessions, PUBCOMP for the end of a QoS 2 flow,
    /// SUBACK and UNSUBACK for the change. A connection the crash ended
    /// counts as lost at the next start, so its will goes out.
    /// `None` keeps it all in memory. Linux only, and the file system must
    /// take `O_DIRECT`, which tmpfs before Linux 6.6 does not. Not with
    /// `state_file`.
    pub data_dir: Option<PathBuf>,
    /// File that keeps sessions over a restart: [`Broker::shutdown`]
    /// closes every connection and writes it, and [`Broker::new`] loads it
    /// and renames it to `<name>.loaded`. It holds the retained messages
    /// too. Sessions that end with their connection are left out. The file
    /// is compressed with Zstandard. Not with `data_dir`, which keeps all
    /// of it.
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
    /// Runs the broker as one node of a three-node cluster. Engine API for
    /// the `zzq_broker` binary and the simulator, not semver.
    #[doc(hidden)]
    pub cluster: Option<ClusterOptions>,
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
            data_dir: None,
            state_file: None,
            sys_interval: Some(Duration::from_secs(10)),
            transport: TransportOptions::default(),
            sn: SnOptions::default(),
            cluster: None,
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
            .field("data_dir", &self.data_dir)
            .field("state_file", &self.state_file)
            .field("sys_interval", &self.sys_interval)
            .field("transport", &self.transport)
            .field("sn", &self.sn)
            .field("cluster", &self.cluster)
            .finish()
    }
}

/// A broker as one node of a cluster: the node's settings, how it starts,
/// and the links to the other two. Engine API, not semver.
///
/// With [`BrokerOptions::state_file`], [`Broker::shutdown`](crate::Broker::shutdown)
/// writes the node's exit dump there, and the next start runs from it
/// instead of as `start` says.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct ClusterOptions {
    /// The cluster and this node in it. The broker sets its
    /// `fingerprint` from what its state depends on: the lane count and
    /// `max_topic_levels`.
    pub node: zzq_cluster::node::NodeConfig,
    /// How the node starts without an exit dump.
    pub start: ClusterStart,
    /// Frames to and from the other two nodes.
    pub links: ClusterLinks,
}

/// How a node starts.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterStart {
    /// As one of three nodes of a new, empty cluster.
    Bootstrap,
    /// Without state, as after a crash: it recovers from the other two.
    Recover,
    /// As the leader of a new epoch, from the newest state it has: its
    /// exit dump, the dump it last started from, or none. For when no two
    /// nodes hold the state: what was acked after that dump is lost. The
    /// others join once they start without state.
    Force,
}

/// The links to the other nodes: frames go out on `outbound`, and come in
/// on the receiver the broker takes once.
#[doc(hidden)]
#[derive(Clone)]
pub struct ClusterLinks {
    /// Frames for another node. A full channel drops; the node sends again.
    pub outbound: tokio::sync::mpsc::Sender<zzq_cluster::node::Outgoing>,
    /// Frames from the other nodes.
    pub inbound: Arc<std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<[bytes::Bytes; 3]>>>>,
}

impl fmt::Debug for ClusterLinks {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClusterLinks").finish_non_exhaustive()
    }
}

/// MQTT-SN gateway settings. Start from [`SnOptions::default`] and set
/// fields, as later versions may add more.
#[derive(Debug, Clone)]
#[non_exhaustive]
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
    /// Keep alive, in seconds, for a client that asks for none or for more,
    /// and the longest a client may sleep. Over UDP nothing else tells the
    /// gateway that a client is gone.
    pub max_keep_alive: u16,
    /// Topic IDs per client, registered by it or assigned by the gateway.
    /// More REGISTERs get Rejected: congestion, and messages on further
    /// topics are dropped.
    pub max_topics: usize,
    /// Clients served at once. Anyone can send a CONNECT over UDP, so more
    /// get Rejected: congestion.
    pub max_clients: usize,
}

impl Default for SnOptions {
    fn default() -> Self {
        SnOptions {
            gateway_id: 1,
            predefined: Vec::new(),
            retry: Duration::from_secs(10),
            retries: 5,
            sleep_buffer: 1000,
            max_keep_alive: 3600,
            max_topics: 4096,
            max_clients: 10_000,
        }
    }
}
