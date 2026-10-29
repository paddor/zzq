//! One node of a cluster: the replication core with its ops in RAM, the
//! links to the two other nodes, and the events its application acts on.
//!
//! Sans-I/O. The caller moves [`Outgoing`] frames to their node and hands
//! received frames to [`Node::receive`], supplies a monotonic clock, and
//! acts on each [`Event`]: a follower applies committed ops and reports
//! them with [`Node::applied`]; a leader proposes ops with
//! [`Node::propose`] and acks clients once [`Event::Committed`] covers
//! them. Storage is RAM: an op is stored once it is in the log.

mod dump;
mod election;
mod history;
mod image;
mod link;
mod log;
mod normal;
mod recover;
mod voting;

pub use dump::DumpError;
pub use log::LogLimits;

use std::collections::VecDeque;
use std::time::Duration;

use bytes::Bytes;

use crate::driver::{Action, DriverError, ReplicaDriver, Timing};
use crate::wire::{ReplicaMessage, WireError, WireLimits};
use crate::{
    Configuration, JournalGeneration, NodeId, NormalReplica, OpNumber, PipelineLimits, Prefix,
    ReplicationError, RequestId, StartView, Status, decode_packet,
};

use history::Fetch;
use image::Images;
use link::{Encoder, Peer, Peers, Side};
use log::Log;
use recover::Recovering;
use voting::Driver;

/// Polls of the driver per [`Node::poll`]: each yields at most one action.
const DRIVER_ACTIONS: usize = 16;
/// Prepare batches sent to one follower per [`Node::poll`].
const SEND_BATCHES: usize = 4;

/// How a node runs.
#[derive(Debug, Clone, Copy)]
pub struct NodeConfig {
    /// The cluster's three nodes and identity, the same on every node.
    pub configuration: Configuration,
    /// This node, one of the configured three.
    pub local: NodeId,
    /// Heartbeat, timeouts and retries.
    pub timing: Timing,
    /// Ops in flight past the commit.
    pub pipeline: PipelineLimits,
    /// Ops kept for followers that fall behind; at least `pipeline` twice.
    pub log: LogLimits,
    /// Sizes of messages between nodes.
    pub wire: WireLimits,
    /// The application's shape, such as its lane count: links bind only
    /// between nodes with the same.
    pub fingerprint: u64,
}

/// Frames for one other node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    /// The node to send them to.
    pub to: NodeId,
    /// Header, metadata and payload.
    pub frames: [Bytes; 3],
}

/// What a node does in the cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// It leads: it serves clients and proposes ops. It stops once it
    /// hears from neither follower for half the primary timeout, before
    /// they could elect another.
    Leader,
    /// It follows the leader and applies what commits.
    Follower,
    /// No view is settled: it serves nothing.
    Electing,
}

/// A node at a glance, for metrics and tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeStatus {
    /// What it does.
    pub role: Role,
    /// The current view: settled, or proposed in an election.
    pub view: u64,
    /// Whether it recovers lost state and votes for nothing.
    pub recovering: bool,
    /// The last op it holds in its view; zero while not normal.
    pub accepted: OpNumber,
    /// The highest op known committed.
    pub committed: OpNumber,
    /// The last op the application reported applied.
    pub applied: OpNumber,
    /// The ops its log holds: after `log_base`, through `log_last`.
    pub log_base: OpNumber,
    /// The last op its log holds.
    pub log_last: OpNumber,
}

/// What the application acts on.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Event {
    /// Apply this committed op, the one after the last, then report it with
    /// [`Node::applied`]. A leader gets these only for ops of earlier views,
    /// before it leads.
    Apply {
        /// Its position.
        op: Prefix,
        /// Its body, as proposed.
        body: Bytes,
    },
    /// The leader's own ops through this one committed.
    Committed(OpNumber),
    /// The node's role changed.
    Role(Role),
    /// Drop all state: it holds ops the cluster's history does not. Ops
    /// come again from the first, or an [`Event::Image`] comes first.
    Reset,
    /// Cut an image of the state and give it to [`Node::offer_image`].
    CutImage,
    /// Take this image as the whole state. [`Event::Apply`] follows for
    /// the ops after `predecessor`; skip those the image holds.
    Image {
        /// The op after which replay starts.
        predecessor: OpNumber,
        /// The highest op the image holds.
        position: OpNumber,
        /// The state, as the donor's application encoded it.
        bytes: Bytes,
    },
}

/// Why a node rejected an input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NodeError {
    /// Frames from a node that is not one of the other two configured.
    #[error("unknown node")]
    UnknownNode,
    /// Frames from a node whose HELLO has not come.
    #[error("no session with this node")]
    NoSession,
    /// The other node runs an application of another shape.
    #[error("node with another application fingerprint")]
    Fingerprint,
    /// An image the log cannot anchor, or an empty one.
    #[error("image not anchored in the log")]
    Image,
    /// An exit dump that cannot be restored.
    #[error(transparent)]
    Dump(#[from] DumpError),
    /// Malformed or foreign frames.
    #[error(transparent)]
    Wire(#[from] WireError),
    /// The replication core rejected the input.
    #[error(transparent)]
    Driver(#[from] DriverError),
}

impl From<ReplicationError> for NodeError {
    fn from(e: ReplicationError) -> Self {
        NodeError::Driver(e.into())
    }
}

impl From<crate::ViewChangeError> for NodeError {
    fn from(e: crate::ViewChangeError) -> Self {
        NodeError::Driver(e.into())
    }
}

/// Why [`Node::propose`] did not take an op.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ProposeError {
    /// Only a leader proposes.
    #[error("not the leader")]
    NotLeader,
    /// Ops in flight reached the pipeline limits; retry after commits.
    #[error("pipeline full")]
    Full,
    /// The replication core rejected the op.
    #[error(transparent)]
    Driver(#[from] DriverError),
}

/// One node of a three-node cluster.
#[derive(Debug)]
pub struct Node {
    config: NodeConfig,
    encoder: Encoder,
    /// This process, for link sessions.
    incarnation: RequestId,
    peers: Peers,
    driver: Driver,
    /// Set while recovering lost state; the driver has no core then.
    recovering: Option<Box<Recovering>>,
    log: Log,
    role: Role,
    /// Leader: its first op this term. Ops before it came from earlier
    /// views and are applied as [`Event::Apply`].
    own_from: Option<OpNumber>,
    /// The last op handed to the application, or proposed by it.
    emitted: OpNumber,
    /// The last op the application reported applied, or proposed.
    app_applied: OpNumber,
    /// The last op the application holds.
    tip: Prefix,
    /// The highest commit known.
    committed: Prefix,
    /// Follower: an ack is due to the leader.
    ack_due: bool,
    /// History fetched for an election.
    fetch: Option<Fetch>,
    /// The selected history an installation builds, for the log once it
    /// completes.
    staged: Option<Log>,
    /// Backup: the current primary's `START_VIEW` to install.
    start: Option<(NodeId, StartView)>,
    /// Primary: the ops every backup of the view holds once installed.
    send_floor: OpNumber,
    /// Images given and taken.
    images: Images,
    lease: Lease,
    /// The time of the last input.
    now: Duration,
    out: VecDeque<Outgoing>,
    events: VecDeque<Event>,
}

impl Node {
    /// A node of a new, empty cluster at view zero. All three must start
    /// this way, and only once: a restart without its state must recover.
    ///
    /// # Errors
    ///
    /// [`NodeError::Driver`] for a node not in the configuration, zero
    /// limits or invalid timing.
    pub fn bootstrap(config: NodeConfig, now: Duration) -> Result<Node, NodeError> {
        let normal = NormalReplica::bootstrap(
            config.configuration,
            config.local,
            fresh_generation(),
            config.pipeline,
        )?;
        let driver = ReplicaDriver::from_normal(normal, now, config.timing)?;
        let others = config
            .configuration
            .voters()
            .iter()
            .copied()
            .filter(|&id| id != config.local);
        let mut peers = others.map(Peer::new);
        let (Some(a), Some(b)) = (peers.next(), peers.next()) else {
            return Err(ReplicationError::UnknownVoter.into());
        };
        let mut node = Node {
            config,
            encoder: Encoder {
                local: config.local,
                configuration: config.configuration,
                limits: config.wire,
            },
            incarnation: RequestId::new(),
            peers: Peers([a, b]),
            driver: Driver::new(driver),
            recovering: None,
            log: Log::new(Prefix::GENESIS, config.log),
            role: Role::Electing,
            own_from: None,
            emitted: OpNumber(0),
            app_applied: OpNumber(0),
            tip: Prefix::GENESIS,
            committed: Prefix::GENESIS,
            ack_due: false,
            fetch: None,
            staged: None,
            start: None,
            send_floor: OpNumber(0),
            out: VecDeque::new(),
            events: VecDeque::new(),
            images: Images::default(),
            lease: Lease {
                heard_at: now,
                // It may have acked a leader just before its process went.
                follow_until: now + config.timing.primary_timeout / 2 + config.timing.heartbeat,
                led: None,
            },
            now,
        };
        node.update_role();
        Ok(node)
    }

    /// This node.
    pub const fn id(&self) -> NodeId {
        self.config.local
    }

    /// What it does now.
    pub const fn role(&self) -> Role {
        self.role
    }

    /// The current view: settled, or proposed in an election.
    pub const fn view(&self) -> u64 {
        self.driver.scope().view
    }

    /// Its state at a glance.
    pub fn status(&self) -> NodeStatus {
        NodeStatus {
            role: self.role,
            view: self.view(),
            recovering: self.recovering.is_some(),
            accepted: self
                .driver
                .normal()
                .map_or(OpNumber(0), |n| n.snapshot().accepted.op),
            committed: self.committed.op,
            applied: self.app_applied,
            log_base: self.log.base().op,
            log_last: self.log.last().op,
        }
    }

    /// The highest op known committed.
    pub const fn committed(&self) -> Prefix {
        self.committed
    }

    /// The link to `peer` (re)connected: say HELLO, which binds the link
    /// once the peer says it too.
    ///
    /// # Errors
    ///
    /// [`NodeError::UnknownNode`] for a node not among the other two.
    pub fn connected(&mut self, peer: NodeId) -> Result<(), NodeError> {
        let i = self.peer_index(peer).ok_or(NodeError::UnknownNode)?;
        self.say_hello(i)
    }

    /// Takes frames from another node.
    ///
    /// # Errors
    ///
    /// Why the frames were dropped. Old views and duplicates are not
    /// errors; the caller may count errors and go on.
    pub fn receive(&mut self, frames: &[Bytes; 3], now: Duration) -> Result<(), NodeError> {
        let borrowed = [&frames[0][..], &frames[1][..], &frames[2][..]];
        let packet =
            decode_packet(&borrowed, self.config.wire.envelope).map_err(WireError::from)?;
        if let Some(hello) = link::read_hello(packet, &self.config.configuration)? {
            return self.on_hello(hello, now);
        }
        let from = packet.envelope.sender;
        let i = self.peer_index(from).ok_or(NodeError::UnknownNode)?;
        let Some(session) = self.peers[i].session else {
            self.hello_again(i, now)?;
            return Err(NodeError::NoSession);
        };
        let binding = self.encoder.binding(from, session)?;
        let message = match crate::wire::decode(&borrowed, binding, self.config.wire) {
            Ok(message) => message,
            Err(WireError::Peer) => {
                // Another session: one side missed the other's restart.
                self.hello_again(i, now)?;
                return Err(WireError::Peer.into());
            }
            Err(e) => return Err(e.into()),
        };
        // Only a peer that bound this process sends on the session.
        self.peers[i].knows_us = true;
        self.now = self.now.max(now);
        match message {
            ReplicaMessage::Control(control) if self.recovering.is_some() => {
                let start = match control {
                    crate::wire::Control::StartView(start) => Some(start),
                    _ => None,
                };
                self.recovery_control(from, control.scope(), start);
            }
            // Backing the leader's lease: no help to leave its view yet.
            // The messages come again.
            ReplicaMessage::Control(
                crate::wire::Control::ExitView(_)
                | crate::wire::Control::StartViewChange(_)
                | crate::wire::Control::DoViewChange(_),
            ) if self.role != Role::Leader && now < self.lease.follow_until => {}
            ReplicaMessage::Control(control) => self.on_control(from, control, now)?,
            ReplicaMessage::Prepare(batch) => self.on_prepare(from, batch, &frames[2], now)?,
            ReplicaMessage::FetchOps(request) => self.serve_fetch(from, request),
            ReplicaMessage::Ops(batch) => self.on_ops(&batch, &frames[2], now)?,
            ReplicaMessage::HistoryRetired(notice) => self.on_retired(from, notice),
            ReplicaMessage::Recovery(message) => self.on_recovery(from, &message, now),
            ReplicaMessage::Checkpoint(message) => self.on_checkpoint(from, message, now),
            ReplicaMessage::Flow(_) => {}
        }
        self.after_input();
        Ok(())
    }

    /// Advances timers: heartbeats, elections, resends. Call at
    /// [`Node::next_deadline`] and after inputs.
    ///
    /// # Errors
    ///
    /// [`NodeError::Driver`] once the core faulted; the node then serves
    /// nothing until it restarts.
    pub fn poll(&mut self, now: Duration) -> Result<(), NodeError> {
        self.now = self.now.max(now);
        self.check_isolation(now);
        for _ in 0..DRIVER_ACTIONS {
            match self.driver.poll(now)? {
                None => break,
                Some(Action::PersistPromise(ticket)) => {
                    // Stored in RAM at once.
                    self.driver.complete_promise(ticket)?;
                }
                Some(Action::Broadcast(message)) => {
                    for i in Side::BOTH {
                        self.send_control(i, message);
                    }
                }
                Some(Action::Send { to, message }) => {
                    if let Some(i) = self.peer_index(to) {
                        self.send_control(i, message);
                    }
                }
            }
        }
        if self.recovering.is_some() {
            self.poll_recovery(now);
            self.emit_commits();
            self.poll_fetch(now);
            self.retry_hellos(now)?;
            return Ok(());
        }
        self.after_input();
        self.elect(now)?;
        self.after_input();
        self.poll_fetch(now);
        self.poll_images(now);
        self.retry_hellos(now)?;
        self.send_prepares(now);
        Ok(())
    }

    /// When [`Node::poll`] next has work, if it knows.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.driver.next_deadline()
    }

    /// The next frames to send.
    pub fn next_outgoing(&mut self) -> Option<Outgoing> {
        self.out.pop_front()
    }

    /// The next event for the application.
    pub fn next_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn peer_index(&self, id: NodeId) -> Option<Side> {
        self.peers.side(id)
    }

    fn say_hello(&mut self, i: Side) -> Result<(), NodeError> {
        let peer = &self.peers[i];
        let out = self.encoder.hello(
            peer.id,
            link::Greeting {
                incarnation: self.incarnation,
                fingerprint: self.config.fingerprint,
                known: peer.incarnation,
            },
        )?;
        self.out.push_back(out);
        Ok(())
    }

    /// Says HELLO to peer `i` once per retransmit interval: it sent on a
    /// session this process does not know.
    fn hello_again(&mut self, i: Side, now: Duration) -> Result<(), NodeError> {
        if now >= self.peers[i].hello_at {
            self.peers[i].hello_at = now + self.config.timing.retransmit;
            self.say_hello(i)?;
        }
        Ok(())
    }

    /// Says HELLO again to peers that do not know this process yet.
    fn retry_hellos(&mut self, now: Duration) -> Result<(), NodeError> {
        for i in Side::BOTH {
            if !self.peers[i].knows_us && now >= self.peers[i].hello_at {
                self.peers[i].hello_at = now + self.config.timing.retransmit;
                self.say_hello(i)?;
            }
        }
        Ok(())
    }

    fn on_hello(&mut self, hello: link::Hello, now: Duration) -> Result<(), NodeError> {
        let i = self.peer_index(hello.from).ok_or(NodeError::UnknownNode)?;
        if hello.fingerprint != self.config.fingerprint {
            return Err(NodeError::Fingerprint);
        }
        let peer = &mut self.peers[i];
        let new = peer.incarnation != Some(hello.incarnation);
        if new {
            // A new process behind the link: bind a fresh session and start
            // its sends over.
            peer.incarnation = Some(hello.incarnation);
            peer.session = Some(link::session(self.incarnation, hello.incarnation));
            peer.knows_us = false;
            peer.sent = OpNumber(0);
            peer.acked = OpNumber(0);
            peer.progress_at = now;
        }
        let known = hello.known == Some(self.incarnation);
        if known {
            peer.knows_us = true;
        }
        // Tell it this process knows it, or who this process is.
        if new || !known {
            self.say_hello(i)?;
        }
        Ok(())
    }

    fn send_control(&mut self, i: Side, message: crate::wire::Control) {
        let peer = &self.peers[i];
        let Some(session) = peer.session else {
            return;
        };
        if let Ok(out) = self.encoder.control(peer.id, session, message) {
            self.out.push_back(out);
        }
    }

    /// Hands newly committed ops to the application and follows role
    /// changes.
    fn after_input(&mut self) {
        if let Some(n) = self.driver.normal() {
            let c = n.snapshot().committed;
            if c.op > self.committed.op {
                self.committed = c;
            }
        }
        self.update_role();
        self.emit_commits();
        self.sync_applied();
        if self.ack_due {
            self.ack_due = false;
            self.send_ack();
        }
    }

    fn update_role(&mut self) {
        let role = match self.driver.normal() {
            Some(n) => {
                let s = n.snapshot();
                let primary = self.config.configuration.primary(s.scope.view);
                if s.status != Status::Normal {
                    Role::Electing
                } else if primary != self.config.local {
                    Role::Follower
                } else if s.ready_for_appends
                    && self.now < self.lease.heard_at + self.config.timing.primary_timeout / 2
                {
                    Role::Leader
                } else {
                    Role::Electing
                }
            }
            None => Role::Electing,
        };
        if role == self.role {
            return;
        }
        if self.role == Role::Leader {
            // Its application holds every op it proposed.
            self.emitted = self.tip.op;
            self.app_applied = self.tip.op;
        }
        self.own_from = match role {
            Role::Leader => Some(OpNumber(self.log.last().op.0 + 1)),
            Role::Follower | Role::Electing => None,
        };
        self.role = role;
        self.events.push_back(Event::Role(role));
    }
}

/// The leader lease, from both sides.
#[derive(Debug)]
struct Lease {
    /// Leader: when a follower last acked in its view.
    heard_at: Duration,
    /// Follower: until when its last ack backs the leader's lease.
    follow_until: Duration,
    /// Primary: its view, and when it became its primary.
    led: Option<(u64, Duration)>,
}

/// A writer incarnation no other process uses.
fn fresh_generation() -> JournalGeneration {
    JournalGeneration(u128::from_be_bytes(*RequestId::new().as_bytes()))
}
