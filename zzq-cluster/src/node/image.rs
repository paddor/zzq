//! Images: committed state the application cuts for a node that has to
//! rebuild its own, moved in chunks.
//!
//! A node asks the primary with `RECOVERY`. The primary's
//! `RECOVERY_STATE` names the image in its checkpoint anchor; chunks follow
//! over `SNAPSHOT_BEGIN`/`SNAPSHOT_CHUNK`. A voter that only lost its
//! application state asks with `nonvoting` false and keeps voting.

use std::time::Duration;

use bytes::Bytes;

use super::link::Side;
use super::{Event, Node, NodeError, Outgoing};
use crate::integrity::hash;
use crate::recovery::{CheckpointAnchor, RecoveryLog, RecoveryResponse};
use crate::wire::{
    CheckpointMessage, CheckpointRequest, ControlEncoding, RecoveryMessage, RecoveryRequest,
    RecoveryState, encode_checkpoint, encode_recovery, encode_recovery_state,
};
use crate::{Digest, LogSource, NodeId, OpNumber, RequestId};

const STATE_CONTEXT: &str = "zzq cluster image v1";
/// Recovery answers kept, so a repeated request gets the same answer.
const ANSWERS: usize = 4;

/// The schema digest images carry: application images are opaque here.
fn schema() -> Digest {
    hash(STATE_CONTEXT, b"schema")
}

/// An image this node offers, once committed.
#[derive(Debug)]
pub(super) struct Donated {
    pub(super) anchor: CheckpointAnchor,
    pub(super) bytes: Bytes,
}

/// A node's side of moving an image: what it gives and what it takes.
#[derive(Debug, Default)]
pub(super) struct Images {
    /// Cut by the application, waiting for its position to commit.
    cut: Option<Donated>,
    /// Committed and offered.
    pub(super) offered: Option<Donated>,
    /// The application was asked for a cut and has not given one.
    cut_due: bool,
    /// Requests that wait for an image: who, and what they asked.
    waiting: Vec<(NodeId, RecoveryRequest)>,
    /// Answers given, by nonce, with the source they name.
    answers: Vec<(RequestId, RecoveryResponse)>,
    /// This node rebuilds its application state.
    pub(super) rebuild: Option<Rebuild>,
}

/// A rebuild of the application's state from the primary's image.
#[derive(Debug)]
pub(super) struct Rebuild {
    request: RecoveryRequest,
    asked_at: Option<Duration>,
    transfer: Option<ImageIn>,
    /// When it began: a stalled one starts over.
    since: Duration,
}

/// An image coming in, chunk by chunk.
#[derive(Debug)]
pub(super) struct ImageIn {
    from: NodeId,
    scope: crate::Scope,
    nonce: RequestId,
    source: LogSource,
    pub(super) anchor: CheckpointAnchor,
    bytes: Vec<u8>,
    sent: Option<(CheckpointRequest, Duration)>,
}

/// What a chunk did to an [`ImageIn`].
#[derive(Debug)]
pub(super) enum Taken {
    /// Not the chunk asked for.
    Ignored,
    /// More to come.
    More,
    /// All bytes, checked.
    Done(Bytes),
    /// All bytes, but not the ones named: start over.
    Corrupt,
}

impl ImageIn {
    pub(super) const fn new(
        from: NodeId,
        scope: crate::Scope,
        nonce: RequestId,
        source: LogSource,
        anchor: CheckpointAnchor,
    ) -> ImageIn {
        ImageIn {
            from,
            scope,
            nonce,
            source,
            anchor,
            bytes: Vec::new(),
            sent: None,
        }
    }

    /// The next chunk request, unless one is in flight and not overdue.
    pub(super) fn next(
        &mut self,
        now: Duration,
        retransmit: Duration,
    ) -> Option<CheckpointRequest> {
        if self.sent.is_some_and(|(_, at)| now < at + retransmit) {
            return None;
        }
        let request = CheckpointRequest {
            scope: self.scope,
            request_id: RequestId::new(),
            nonce: self.nonce,
            source: self.source,
            offset: self.bytes.len() as u64,
            max_bytes: self.anchor.chunk_bytes,
        };
        self.sent = Some((request, now));
        Some(request)
    }

    pub(super) fn take(&mut self, request: CheckpointRequest, bytes: &[u8]) -> Taken {
        if self.sent.is_none_or(|(sent, _)| sent != request)
            || self.bytes.len() as u64 + bytes.len() as u64 > self.anchor.state_bytes
        {
            return Taken::Ignored;
        }
        self.bytes.extend_from_slice(bytes);
        self.sent = None;
        if (self.bytes.len() as u64) < self.anchor.state_bytes {
            return Taken::More;
        }
        let bytes = Bytes::from(std::mem::take(&mut self.bytes));
        if hash(STATE_CONTEXT, &bytes) == self.anchor.state_digest {
            Taken::Done(bytes)
        } else {
            Taken::Corrupt
        }
    }
}

impl Node {
    /// Gives the image the application cut after [`Event::CutImage`]: its
    /// state through op `position`, where ops after `predecessor` may still
    /// need replay. The node offers it once `position` is committed.
    ///
    /// # Errors
    ///
    /// [`NodeError::Image`] if the log no longer holds `predecessor` or
    /// `position`, or the bytes are empty.
    pub fn offer_image(
        &mut self,
        predecessor: OpNumber,
        position: OpNumber,
        bytes: Bytes,
    ) -> Result<(), NodeError> {
        // Asked again on the next request, whatever comes of this one.
        self.images.cut_due = false;
        let (Some(predecessor), Some(position)) = (
            self.log.prefix_at(predecessor),
            self.log.prefix_at(position),
        ) else {
            return Err(NodeError::Image);
        };
        if bytes.is_empty() || predecessor.op > position.op || position.op.0 == 0 {
            return Err(NodeError::Image);
        }
        let chunk = u32::try_from(self.config.wire.envelope.max_payload_bytes).unwrap_or(u32::MAX);
        self.images.cut = Some(Donated {
            anchor: CheckpointAnchor {
                predecessor,
                position,
                schema: schema(),
                state_digest: hash(STATE_CONTEXT, &bytes),
                state_bytes: bytes.len() as u64,
                chunk_bytes: chunk,
            },
            bytes,
        });
        self.images.cut_due = false;
        Ok(())
    }

    /// Offers the cut image once committed, and answers who waited for it.
    pub(super) fn poll_images(&mut self, now: Duration) {
        if self
            .images
            .cut
            .as_ref()
            .is_some_and(|c| c.anchor.position.op <= self.committed.op)
        {
            self.images.offered = self.images.cut.take();
            for (from, request) in std::mem::take(&mut self.images.waiting) {
                self.on_recovery_request(from, request);
            }
        }
        self.poll_rebuild(now);
    }

    pub(super) fn on_recovery(&mut self, from: NodeId, message: &RecoveryMessage, now: Duration) {
        match message {
            RecoveryMessage::Request(request) => self.on_recovery_request(from, *request),
            RecoveryMessage::State(state) => self.on_recovery_state(from, state, now),
        }
    }

    /// Answers a `RECOVERY` as a normal node; the primary names an image,
    /// asking the application for one first if needed.
    fn on_recovery_request(&mut self, from: NodeId, request: RecoveryRequest) {
        let Some(response) = self.recovery_answer(from, request) else {
            return;
        };
        let state = RecoveryState {
            request_id: request.request_id,
            response,
        };
        if let Some(out) = self.on_link(from, |me, session| {
            let mut metadata = vec![0; 512];
            let encoded =
                encode_recovery_state(me.local, session, state, &mut metadata, me.limits).ok()?;
            Some(control_frames(from, &encoded, metadata))
        }) {
            self.out.push_back(out);
        }
    }

    /// The answer to `request`: the one given before in this view, or a new
    /// one. None until the image it names is cut and committed.
    fn recovery_answer(
        &mut self,
        from: NodeId,
        request: RecoveryRequest,
    ) -> Option<RecoveryResponse> {
        // An image whose replay ops this log no longer holds is of no use.
        if self
            .images
            .offered
            .as_ref()
            .is_some_and(|i| i.anchor.predecessor.op < self.log.base().op)
        {
            self.images.offered = None;
        }
        let normal = self.driver.normal()?;
        let view = normal.scope().view;
        if let Some((_, r)) = self
            .images
            .answers
            .iter()
            .find(|(n, r)| *n == request.nonce && r.scope.view == view)
        {
            return Some(*r);
        }
        let mut response = normal.recovery_response(request.nonce).ok()?;
        let full_log = request.nonvoting && self.log.base().op.0 == 0;
        if let Some(log) = response.primary.as_mut()
            && !full_log
        {
            let Some(anchor) = self.images.offered.as_ref().map(|i| i.anchor) else {
                self.wait_for_image(from, request);
                return None;
            };
            log.checkpoint = Some(anchor);
            if let Some(side) = self.peer_index(from) {
                self.peers[side].pin = Some((anchor.predecessor.op, self.now));
            }
        }
        if self.images.answers.len() == ANSWERS {
            self.images.answers.remove(0);
        }
        self.images.answers.push((request.nonce, response));
        Some(response)
    }

    /// Keeps `request` until an image is offered, asking the application for
    /// a cut once.
    fn wait_for_image(&mut self, from: NodeId, request: RecoveryRequest) {
        if !self
            .images
            .waiting
            .iter()
            .any(|(_, r)| r.nonce == request.nonce)
        {
            self.images.waiting.push((from, request));
        }
        if self.images.cut.is_none() && !self.images.cut_due {
            self.images.cut_due = true;
            self.events.push_back(Event::CutImage);
        }
    }

    /// Serves a chunk of the offered image named in an answer.
    pub(super) fn on_checkpoint(
        &mut self,
        from: NodeId,
        message: CheckpointMessage<'_>,
        now: Duration,
    ) {
        match message {
            CheckpointMessage::Request(request) => self.serve_chunk(from, request),
            CheckpointMessage::Chunk { request, bytes } => self.take_chunk(request, bytes, now),
        }
    }

    fn serve_chunk(&mut self, from: NodeId, request: CheckpointRequest) {
        let Some(image) = self.images.offered.as_ref() else {
            return;
        };
        let named = self.images.answers.iter().any(|(n, r)| {
            *n == request.nonce
                && r.primary.is_some_and(|log| {
                    log.checkpoint == Some(image.anchor)
                        && source(self.config.local, &log) == request.source
                })
        });
        let Ok(offset) = usize::try_from(request.offset) else {
            return;
        };
        if !named || offset >= image.bytes.len() {
            return;
        }
        let len = (request.max_bytes as usize)
            .min(image.anchor.chunk_bytes as usize)
            .min(image.bytes.len() - offset);
        let chunk = image.bytes.slice(offset..offset + len);
        if let Some(out) = self.on_link(from, |me, session| {
            let mut metadata = vec![0; 512];
            let encoded = encode_checkpoint(
                me.local,
                session,
                CheckpointMessage::Chunk {
                    request,
                    bytes: &chunk,
                },
                &mut metadata,
                me.limits,
            )
            .ok()?;
            metadata.truncate(encoded.metadata_bytes);
            Some(Outgoing {
                to: from,
                frames: [
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::from(metadata),
                    chunk.clone(),
                ],
            })
        }) {
            self.out.push_back(out);
        }
    }

    /// Starts rebuilding the application's state from an image: it holds
    /// ops the cluster's history lacks, and the log cannot replay from
    /// the first op.
    pub(super) fn start_rebuild(&mut self) {
        self.images.rebuild = Some(Rebuild {
            request: RecoveryRequest {
                scope: self.driver.scope(),
                request_id: RequestId::new(),
                nonce: RequestId::new(),
                nonvoting: false,
            },
            asked_at: None,
            transfer: None,
            since: self.now,
        });
    }

    /// Asks the primary for its image, then for its chunks, again after
    /// each retransmit interval without an answer.
    fn poll_rebuild(&mut self, now: Duration) {
        let retransmit = self.config.timing.retransmit;
        let view = self.driver.scope().view;
        let primary = self.config.configuration.primary(view);
        let Some(rebuild) = self.images.rebuild.as_mut() else {
            return;
        };
        if now >= rebuild.since + self.config.timing.max_election_timeout * 4 {
            self.start_rebuild();
            return;
        }
        if let Some(t) = rebuild.transfer.as_mut() {
            let to = t.from;
            if let Some(request) = t.next(now, retransmit) {
                self.ask_chunk(to, request);
            }
            return;
        }
        if primary == self.config.local || rebuild.asked_at.is_some_and(|at| now < at + retransmit)
        {
            return;
        }
        rebuild.asked_at = Some(now);
        rebuild.request.scope.view = view;
        let request = rebuild.request;
        if let Some(out) = self.on_link(primary, |me, session| {
            let mut metadata = vec![0; 256];
            let encoded =
                encode_recovery(me.local, session, request, &mut metadata, me.limits).ok()?;
            Some(control_frames(primary, &encoded, metadata))
        }) {
            self.out.push_back(out);
        }
    }

    fn on_recovery_state(&mut self, from: NodeId, state: &RecoveryState, now: Duration) {
        if self.recovering.is_some() {
            self.recovery_state(from, state);
            return;
        }
        let Some(rebuild) = self.images.rebuild.as_mut() else {
            return;
        };
        if rebuild.transfer.is_some() || state.validate_response(rebuild.request).is_err() {
            return;
        }
        let Some(log) = state.response.primary else {
            return;
        };
        let Some(anchor) = log.checkpoint else {
            return;
        };
        // Replay starts after the predecessor, from this node's log. A log
        // that does not reach it is behind the primary's: recover it too.
        if self.log.prefix_at(anchor.predecessor.op) != Some(anchor.predecessor) {
            self.images.rebuild = None;
            self.begin_recovery();
            return;
        }
        rebuild.transfer = Some(ImageIn::new(
            from,
            rebuild.request.scope,
            rebuild.request.nonce,
            source(from, &log),
            anchor,
        ));
        self.poll_rebuild(now);
    }

    fn take_chunk(&mut self, request: CheckpointRequest, bytes: &[u8], now: Duration) {
        if self.recovering.is_some() {
            self.recovery_chunk(request, bytes, now);
            return;
        }
        let Some(t) = self
            .images
            .rebuild
            .as_mut()
            .and_then(|r| r.transfer.as_mut())
        else {
            return;
        };
        let anchor = t.anchor;
        match t.take(request, bytes) {
            Taken::Ignored => {}
            Taken::More => self.poll_rebuild(now),
            Taken::Corrupt => self.start_rebuild(),
            Taken::Done(bytes) => {
                self.images.rebuild = None;
                self.install_image(anchor, bytes);
            }
        }
    }

    /// Hands a checked image to the application; replay follows from its
    /// predecessor.
    pub(super) fn install_image(&mut self, anchor: CheckpointAnchor, bytes: Bytes) {
        self.events.push_back(Event::Image {
            predecessor: anchor.predecessor.op,
            position: anchor.position.op,
            bytes,
        });
        self.emitted = anchor.predecessor.op;
        self.app_applied = anchor.predecessor.op;
        self.tip = anchor.position;
    }

    /// Asks `to` for an image chunk.
    pub(super) fn ask_chunk(&mut self, to: NodeId, request: CheckpointRequest) {
        if let Some(out) = self.on_link(to, |me, session| {
            let mut metadata = vec![0; 512];
            let encoded = encode_checkpoint(
                me.local,
                session,
                CheckpointMessage::Request(request),
                &mut metadata,
                me.limits,
            )
            .ok()?;
            Some(control_frames(to, &encoded, metadata))
        }) {
            self.out.push_back(out);
        }
    }

    /// Runs `encode` for `peer`'s bound session, if it has one.
    pub(super) fn on_link(
        &self,
        peer: NodeId,
        encode: impl FnOnce(&super::link::Encoder, crate::LinkSessionId) -> Option<Outgoing>,
    ) -> Option<Outgoing> {
        let side: Side = self.peer_index(peer)?;
        let session = self.peers[side].session?;
        encode(&self.encoder, session)
    }
}

/// The source a primary's recovery log names.
pub(super) const fn source(primary: NodeId, log: &RecoveryLog) -> LogSource {
    LogSource {
        voter: primary,
        generation: log.generation,
        accepted: log.accepted,
    }
}

pub(super) fn control_frames(
    to: NodeId,
    encoded: &ControlEncoding,
    mut metadata: Vec<u8>,
) -> Outgoing {
    metadata.truncate(encoded.metadata_bytes);
    Outgoing {
        to,
        frames: [
            Bytes::copy_from_slice(&encoded.header),
            Bytes::from(metadata),
            Bytes::new(),
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_schema_digest_is_fixed_and_nonzero() {
        assert_eq!(schema(), schema());
        assert_ne!(schema(), Digest::ZERO);
    }
}
