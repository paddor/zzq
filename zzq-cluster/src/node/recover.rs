//! Recovery of a node that lost its state, or fell behind the ops the
//! leader still holds: nonvoting, from both other nodes.
//!
//! The node asks both with `RECOVERY`; the primary of the highest view
//! names its history and, unless that starts at the first op, an image.
//! The node takes the image, fetches the ops after it, lets the
//! application replay them through the commit, and joins that view as a
//! backup once it has the view's `START_VIEW`.

use std::time::Duration;

use super::image::{ImageIn, Taken, control_frames};
use super::link::Side;
use super::log::{Entry, Log};
use super::voting::Driver;
use super::{Event, Node, NodeError, Role};
use crate::driver::ReplicaDriver;
use crate::recovery::{Recovery, RecoveryTicket};
use crate::wire::{CheckpointRequest, RecoveryRequest, RecoveryState, encode_recovery};
use crate::{NodeId, OpNumber, Prefix, PreparedOperation, RequestId, Scope, StartView};

/// A recovery in progress.
#[derive(Debug)]
pub(super) struct Recovering {
    core: Recovery,
    request: RecoveryRequest,
    asked_at: Option<Duration>,
    ticket: Option<RecoveryTicket>,
    image: Option<ImageIn>,
    /// The application holds the image, or none was needed.
    imaged: bool,
    /// The `START_VIEW` of the ticket's view, from its primary.
    start: Option<StartView>,
    /// When the transfer began: a stalled one starts over.
    since: Option<Duration>,
}

impl Node {
    /// A node that lost its state, as after a crash: it votes again once it
    /// recovered from both other nodes.
    ///
    /// # Errors
    ///
    /// As [`Node::bootstrap`].
    pub fn recover(config: super::NodeConfig, now: Duration) -> Result<Node, NodeError> {
        let mut node = Node::bootstrap(config, now)?;
        node.begin_recovery();
        node.events.clear();
        Ok(node)
    }

    /// Whether the node recovers and votes for nothing.
    pub const fn recovering(&self) -> bool {
        self.recovering.is_some()
    }

    /// Drops the core and the application's state and starts recovery.
    pub(super) fn begin_recovery(&mut self) {
        let scope = self.driver.scope();
        let generation = super::fresh_generation();
        let nonce = RequestId::new();
        let Ok(core) = Recovery::new(
            self.config.configuration,
            self.config.local,
            generation,
            nonce,
            self.config.pipeline,
        ) else {
            return;
        };
        self.driver.clear();
        if self.role != Role::Electing {
            self.role = Role::Electing;
            self.events.push_back(Event::Role(Role::Electing));
        }
        self.recovering = Some(Box::new(Recovering {
            core,
            request: RecoveryRequest {
                scope,
                request_id: RequestId::new(),
                nonce,
                nonvoting: true,
            },
            asked_at: None,
            ticket: None,
            image: None,
            imaged: false,
            start: None,
            since: None,
        }));
        self.images.rebuild = None;
        self.fetch = None;
        self.start = None;
        if self.tip != Prefix::GENESIS || self.app_applied.0 > 0 {
            self.events.push_back(Event::Reset);
        }
        self.emitted = OpNumber(u64::MAX);
        self.app_applied = OpNumber(0);
        self.tip = Prefix::GENESIS;
        self.own_from = None;
    }

    /// Takes a peer's answer, and once both answered, starts the transfer.
    pub(super) fn recovery_state(&mut self, from: NodeId, state: &RecoveryState) {
        let Some(r) = self.recovering.as_mut() else {
            return;
        };
        if r.ticket.is_some()
            || state.validate_response(r.request).is_err()
            || r.core.receive(from, state.response).is_err()
        {
            return;
        }
        let Ok(ticket) = r.core.begin_transfer() else {
            return;
        };
        r.ticket = Some(ticket);
        r.since = Some(self.now);
        let source = ticket.source();
        let base = ticket
            .checkpoint()
            .map_or(Prefix::GENESIS, |a| a.predecessor);
        if let Some(anchor) = ticket.checkpoint() {
            r.image = Some(ImageIn::new(
                source.voter,
                ticket.scope(),
                ticket.nonce(),
                source,
                anchor,
            ));
        } else {
            r.imaged = true;
            self.emitted = OpNumber(0);
        }
        self.log = Log::new(base, self.config.log);
        if base != source.accepted {
            self.fetch(source, base);
        }
    }

    /// Notes what peers say while recovering: a newer view supersedes the
    /// attempt; the primary's `START_VIEW` lets it join.
    pub(super) fn recovery_control(
        &mut self,
        from: NodeId,
        scope: Scope,
        start: Option<StartView>,
    ) {
        let Some(r) = self.recovering.as_mut() else {
            return;
        };
        let _ = r.core.observe_view(from, scope);
        if let Some(start) = start
            && r.ticket.is_some_and(|t| t.scope() == start.scope)
            && self.config.configuration.primary(start.scope.view) == from
        {
            r.start = Some(start);
        }
    }

    pub(super) fn recovery_chunk(
        &mut self,
        request: CheckpointRequest,
        bytes: &[u8],
        now: Duration,
    ) {
        let Some(r) = self.recovering.as_mut() else {
            return;
        };
        let (Some(image), Some(ticket)) = (r.image.as_mut(), r.ticket) else {
            return;
        };
        let anchor = image.anchor;
        match image.take(request, bytes) {
            Taken::Ignored | Taken::More => {}
            Taken::Corrupt => {
                self.begin_recovery();
                return;
            }
            Taken::Done(bytes) => {
                if r.core
                    .complete_checkpoint(ticket, anchor, anchor.position.op.0)
                    .is_err()
                {
                    self.begin_recovery();
                    return;
                }
                r.image = None;
                r.imaged = true;
                self.install_image(anchor, bytes);
            }
        }
        self.poll_recovery(now);
    }

    /// Asks, transfers and finishes, as far as answers allow.
    pub(super) fn poll_recovery(&mut self, now: Duration) {
        let retransmit = self.config.timing.retransmit;
        let Some(r) = self.recovering.as_mut() else {
            return;
        };
        // A newer view or a stall supersedes the attempt: the next one
        // asks with a fresh nonce.
        let stalled = r
            .since
            .is_some_and(|at| now >= at + self.config.timing.max_election_timeout * 4);
        if r.ticket.is_some_and(|t| r.core.view() > t.scope().view) || stalled {
            self.begin_recovery();
            return;
        }
        let Some(ticket) = r.ticket else {
            if r.asked_at.is_none_or(|at| now >= at + retransmit) {
                r.asked_at = Some(now);
                let request = r.request;
                for side in Side::BOTH {
                    self.ask_recovery(side, request);
                }
            }
            return;
        };
        if let Some(image) = r.image.as_mut() {
            let to = image_source(&ticket);
            if let Some(request) = image.next(now, retransmit) {
                self.ask_chunk(to, request);
            }
        }
        if self.take_fetched(&ticket).is_err() {
            self.begin_recovery();
            return;
        }
        self.finish_recovery(&ticket, now);
    }

    /// Checks and keeps the ops fetched so far.
    fn take_fetched(&mut self, ticket: &RecoveryTicket) -> Result<(), NodeError> {
        let (Some(r), Some(fetch)) = (self.recovering.as_mut(), self.fetch.as_mut()) else {
            return Ok(());
        };
        if !r.imaged || fetch.source != ticket.source() {
            return Ok(());
        }
        if fetch.retired {
            return Err(NodeError::Image);
        }
        let entries = fetch.drain();
        let limits = self.config.pipeline;
        for chunk in entries.chunks(limits.max_operations.max(1)) {
            let prepared: Vec<PreparedOperation> = chunk.iter().map(|e| e.prepared).collect();
            r.core
                .validate_chunk(*ticket, &prepared)
                .map_err(|_| NodeError::Image)?;
            for e in chunk {
                let trimmable = OpNumber(self.app_applied.0.min(ticket.committed().op.0));
                self.log.push(e.clone(), trimmable, trimmable);
            }
        }
        Ok(())
    }

    /// Joins the ticket's view as a backup once the history is whole, the
    /// application caught up and the view's `START_VIEW` is known.
    fn finish_recovery(&mut self, ticket: &RecoveryTicket, now: Duration) {
        let Some(r) = self.recovering.as_mut() else {
            return;
        };
        let accepted = ticket.source().accepted;
        let committed = ticket.committed();
        if !r.imaged
            || self.log.last() != accepted
            || self.app_applied < committed.op
            || (ticket.scope().view > 0 && r.start.is_none())
        {
            return;
        }
        let ring: Vec<PreparedOperation> = self
            .log
            .iter_after(committed.op)
            .map(|e: &Entry| e.prepared)
            .collect();
        let start = r.start;
        let Ok(normal) = r.core.complete_backup(*ticket, &ring, committed, start) else {
            self.begin_recovery();
            return;
        };
        let Ok(driver) = ReplicaDriver::from_normal(normal, now, self.config.timing) else {
            return;
        };
        self.driver = Driver::new(driver);
        self.recovering = None;
        self.fetch = None;
        self.committed = committed;
        self.ack_due = true;
    }

    fn ask_recovery(&mut self, side: Side, request: RecoveryRequest) {
        let to = self.peers[side].id;
        if let Some(out) = self.on_link(to, |me, session| {
            let mut metadata = vec![0; 256];
            let encoded =
                encode_recovery(me.local, session, request, &mut metadata, me.limits).ok()?;
            Some(control_frames(to, &encoded, metadata))
        }) {
            self.out.push_back(out);
        }
    }

    /// The view of the recovery's ticket, once it has one.
    pub(super) fn recovery_scope(&self) -> Option<Scope> {
        Some(self.recovering.as_ref()?.ticket?.scope())
    }

    /// The commit the application replays to while recovering.
    pub(super) fn recovery_target(&self) -> Option<Prefix> {
        let r = self.recovering.as_ref()?;
        let ticket = r.ticket?;
        r.imaged.then_some(ticket.committed())
    }
}

const fn image_source(ticket: &RecoveryTicket) -> NodeId {
    ticket.source().voter
}
