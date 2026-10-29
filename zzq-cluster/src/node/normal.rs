//! Normal operation: the leader proposes and sends prepares, followers
//! store and ack them, and commits reach the application.

use std::time::Duration;

use bytes::Bytes;

use super::link::Side;
use super::log::Entry;
use super::{Event, Node, NodeError, ProposeError, Role, SEND_BATCHES};
use crate::driver::DriverError;
use crate::operation::OperationKind;
use crate::wire::{Control, HistoryFence, HistoryRetired, Prepare, PrepareBatch};
use crate::{
    Admission, Commit, NodeId, OpNumber, Prefix, PreparedOperation, ReplicationError, RequestId,
};

impl Node {
    /// Leader: replicates `body` as the next op. [`Event::Committed`]
    /// covers it once a follower stored it too.
    ///
    /// # Errors
    ///
    /// [`ProposeError::NotLeader`] unless this node leads;
    /// [`ProposeError::Full`] while the ops in flight fill the pipeline.
    pub fn propose(&mut self, body: Bytes, now: Duration) -> Result<Prefix, ProposeError> {
        if self.role != Role::Leader {
            return Err(ProposeError::NotLeader);
        }
        let scope = self.driver.scope();
        let entry = Entry::propose(
            &self.config.configuration,
            scope.view,
            self.log.last(),
            OperationKind::Changes,
            body,
        );
        let prepared = entry.prepared;
        let ticket = match self
            .driver
            .prepare(self.config.local, scope, &[prepared], now)
        {
            Ok(Admission::Write { ticket, .. }) => ticket,
            Ok(Admission::Duplicate) => return Err(ProposeError::NotLeader),
            Err(DriverError::Replication(ReplicationError::Capacity)) => {
                return Err(ProposeError::Full);
            }
            Err(e) => return Err(e.into()),
        };
        self.store(entry);
        self.tip = prepared.prefix();
        self.driver.complete_durable_write(ticket, now)?;
        Ok(prepared.prefix())
    }

    /// Follower: the application applied the ops it was handed through
    /// `op`.
    ///
    /// # Errors
    ///
    /// [`NodeError::Driver`] for an op not handed out yet.
    pub fn applied(&mut self, op: OpNumber) -> Result<(), NodeError> {
        if op > self.emitted {
            return Err(ReplicationError::ApplyBeyondCommit.into());
        }
        self.app_applied = self.app_applied.max(op);
        self.sync_applied();
        Ok(())
    }

    /// Tells the core what the application applied, as far as it committed.
    /// Inside an installed tail the core takes only the tail's end; it
    /// catches up there.
    pub(super) fn sync_applied(&mut self) {
        let committed = self.committed.op;
        // While the application rebuilds from an image, the ops before it
        // are not needed.
        let through = if self.images.rebuild.is_some() {
            committed
        } else {
            self.app_applied.min(committed)
        };
        if let Some(prefix) = self.log.prefix_at(through)
            && self
                .driver
                .normal()
                .is_some_and(|n| n.snapshot().applied.op < through)
        {
            let _ = self.driver.apply_through(prefix);
        }
    }

    /// Keeps `entry`; ops the application applied may make room.
    fn store(&mut self, entry: Entry) {
        let applied = self
            .driver
            .normal()
            .map_or(OpNumber(0), |n| n.snapshot().applied.op);
        let kept = self.kept(self.now);
        self.log.push(entry, kept, applied);
    }

    /// The ops before the first one a live follower or a recovering node
    /// still needs from this log.
    pub(super) fn kept(&self, now: Duration) -> OpNumber {
        let window = self.config.timing.election_timeout * 4;
        let mut kept = OpNumber(u64::MAX);
        for side in Side::BOTH {
            let peer = &self.peers[side];
            if peer.acked_at.is_some_and(|at| now < at + window) {
                kept = kept.min(peer.acked);
            }
            if let Some((op, at)) = peer.pin
                && now < at + window
            {
                kept = kept.min(op);
            }
        }
        kept
    }

    pub(super) fn on_control(
        &mut self,
        from: NodeId,
        control: Control,
        now: Duration,
    ) -> Result<(), NodeError> {
        let received = self.driver.receive(from, control, now);
        match control {
            Control::PrepareOk { ack } if received.is_ok() => {
                self.heard_at = self.heard_at.max(now);
                self.note_ack(from, ack.durable.op, now);
            }
            // A heartbeat. The ack tells the leader this follower is there,
            // and where it stands when it cannot use the commit: past the
            // tail it installed, it needs the tail's commit first.
            // Only from the primary of this view: an old one gets no ack.
            Control::Commit(c) if c.scope.view == self.driver.scope().view => self.ack_due = true,
            _ => {}
        }
        match received {
            Ok(Some(start)) => self.start = Some((from, start)),
            Ok(None) => {}
            Err(DriverError::Replication(ReplicationError::HistoryGap))
                if matches!(control, Control::Commit(_)) => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    /// Follower: stores the ops of a prepare after the last held, then acks.
    pub(super) fn on_prepare(
        &mut self,
        from: NodeId,
        batch: PrepareBatch<'_>,
        payload: &Bytes,
        now: Duration,
    ) -> Result<(), NodeError> {
        let ops: Vec<PreparedOperation> = batch
            .verified_operations()
            .map(|v| PreparedOperation::from_verified(&v.canonical(), v.body_digest()))
            .collect();
        match self.driver.prepare(from, batch.scope(), &ops, now) {
            Ok(Admission::Write { ticket, first_new }) => {
                for v in batch.verified_operations().skip(first_new) {
                    let body = payload.slice_ref(v.canonical().body);
                    self.store(Entry::new(v.canonical(), body, v.body_digest()));
                }
                self.driver.complete_durable_write(ticket, now)?;
            }
            Ok(Admission::Duplicate)
            | Err(DriverError::Replication(
                ReplicationError::HistoryGap
                | ReplicationError::Capacity
                | ReplicationError::ActivationPending,
            )) => {}
            Err(e) => return Err(e.into()),
        }
        self.ack_due = true;
        let commit = Control::Commit(Commit {
            scope: batch.scope(),
            committed: batch.committed(),
        });
        match self.driver.receive(from, commit, now) {
            Ok(_) | Err(DriverError::Replication(ReplicationError::HistoryGap)) => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    /// Follower: sends the leader its stored prefix.
    pub(super) fn send_ack(&mut self) {
        let Some(normal) = self.driver.normal() else {
            return;
        };
        let Ok(ack) = normal.acknowledgment() else {
            return;
        };
        let primary = self.config.configuration.primary(ack.scope.view);
        if let Some(i) = self.peer_index(primary) {
            self.send_control(i, Control::PrepareOk { ack });
            // The ack extends the leader's lease: help elect no other until
            // it ran out, with a heartbeat for the ack's way there.
            let timing = self.config.timing;
            self.follow_until = self.now + timing.primary_timeout / 2 + timing.heartbeat;
        }
    }

    /// Leader: a follower's ack moved, so its resend timer restarts.
    /// The core counts only acks above its applied ops; a follower behind
    /// them is tracked here, from the ack itself.
    fn note_ack(&mut self, from: NodeId, op: OpNumber, now: Duration) {
        let acked = self.acked(from).max(op);
        if let Some(i) = self.peer_index(from) {
            let peer = &mut self.peers[i];
            if acked > peer.sent {
                peer.sent = acked;
            }
            if acked > peer.acked {
                peer.acked = acked;
                peer.progress_at = now;
            }
            peer.acked_at = Some(now);
        }
    }

    /// Leader: what `peer` acked in this view.
    fn acked(&self, peer: NodeId) -> OpNumber {
        self.driver
            .normal()
            .and_then(|n| n.snapshot().backups)
            .and_then(|b| b.into_iter().find(|(id, _)| *id == peer))
            .map_or(OpNumber(0), |(_, op)| op)
    }

    /// Hands what committed since the last call to the application. A
    /// leader's own ops are applied already.
    pub(super) fn emit_commits(&mut self) {
        let target = match (self.driver.normal(), self.driver.installation_ticket()) {
            (Some(n), _) => n.snapshot().committed,
            (None, Some(t)) => t.committed(),
            (None, None) => match self.recovery_target() {
                Some(target) => target,
                None => return,
            },
        };
        let own_from = self.own_from.unwrap_or(OpNumber(u64::MAX));
        while self.emitted < target.op && self.emitted.0 + 1 < own_from.0 {
            let op = OpNumber(self.emitted.0 + 1);
            // While installing, the history the installation builds.
            let history = self.staged.as_ref().unwrap_or(&self.log);
            let Some(entry) = history.get(op) else {
                // Gone from the log: the application rebuilds from an image.
                if op <= history.base().op && self.recovering.is_none() {
                    self.emitted = OpNumber(u64::MAX);
                    self.start_rebuild();
                }
                return;
            };
            self.events.push_back(Event::Apply {
                op: entry.prefix(),
                body: entry.body.clone(),
            });
            self.tip = entry.prefix();
            self.emitted = op;
        }
        if self.emitted < target.op && self.own_from.is_some() {
            self.emitted = target.op;
            self.app_applied = target.op;
            // Its own ops: the application already holds them.
            if self.driver.apply_through(target).is_ok() {
                self.events.push_back(Event::Committed(target.op));
            }
        }
    }

    /// Leader: sends each follower the ops it lacks, from its ack again
    /// once acks stop for the retransmit interval.
    pub(super) fn send_prepares(&mut self, now: Duration) {
        let Some(normal) = self.driver.normal() else {
            return;
        };
        let snapshot = normal.snapshot();
        // The primary sends, also while it activates the view it installed.
        if snapshot.status != crate::Status::Normal
            || self.config.configuration.primary(snapshot.scope.view) != self.config.local
        {
            return;
        }
        for side in Side::BOTH {
            self.send_to(side, &snapshot, now);
        }
    }

    /// Leader: sends one follower the ops after what it was sent, from its
    /// ack again once acks stall, or tells it they are gone.
    fn send_to(&mut self, side: Side, snapshot: &crate::ReplicaSnapshot, now: Duration) {
        let retransmit = self.config.timing.retransmit;
        let id = self.peers[side].id;
        let acked = self.acked(id).max(self.peers[side].acked);
        let Some(session) = self.peers[side].session else {
            return;
        };
        let floor = acked.max(self.send_floor);
        let peer = &mut self.peers[side];
        if peer.sent < acked {
            peer.sent = acked;
        }
        if acked < snapshot.accepted.op && peer.sent > floor && now >= peer.progress_at + retransmit
        {
            peer.sent = floor;
            peer.progress_at = now;
        }
        if peer.sent < self.log.base().op {
            // The ops it needs next are gone: it recovers.
            if peer.retired_at.is_none_or(|at| now >= at + retransmit) {
                peer.retired_at = Some(now);
                let notice = HistoryRetired {
                    scope: snapshot.scope,
                    fence: HistoryFence::Fetch(RequestId::new()),
                    before: self.log.base(),
                };
                if let Some(out) = self.notice_message(id, session, notice) {
                    self.out.push_back(out);
                }
            }
            return;
        }
        for _ in 0..SEND_BATCHES {
            let entries = self.log.after(
                self.peers[side].sent,
                self.config.wire.max_operations,
                self.config.wire.envelope.max_payload_bytes,
            );
            let Some(last) = entries.last().map(|e| e.prefix().op) else {
                break;
            };
            let wire: Vec<_> = entries.iter().map(Entry::wire).collect();
            let prepare = Prepare {
                scope: snapshot.scope,
                committed: snapshot.committed,
                operations: &wire,
            };
            let Ok(out) = self.encoder.prepare(id, session, prepare) else {
                break;
            };
            self.out.push_back(out);
            self.peers[side].sent = last;
        }
    }
}
