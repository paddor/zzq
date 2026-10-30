//! History moved between nodes outside normal prepares: a node fetches the
//! ops of another node's log that an election selected, and serves its own.

use std::time::Duration;

use bytes::Bytes;

use super::log::Entry;
use super::{Node, NodeError, Outgoing};
use crate::wire::{
    EncodeContext, FetchOps, HistoryFence, HistoryRetired, OpsBatch, encode_fetch,
    encode_history_retired, encode_ops,
};
use crate::{Digest, LogSource, NodeId, OpNumber, Prefix, RequestId};

/// Ops of another node's log, fetched after `from`.
#[derive(Debug)]
pub(super) struct Fetch {
    pub(super) source: LogSource,
    pub(super) from: Prefix,
    pub(super) entries: Vec<Entry>,
    /// The request in flight, and when it went out.
    request: Option<(FetchOps, Duration)>,
    /// The source no longer holds the ops after `from`.
    pub(super) retired: bool,
    /// The view requests name: at least the view of every op fetched.
    scope: crate::Scope,
    /// Requests sent without an answer.
    unanswered: u32,
    /// The source served ops after `from`: it holds this log's op there.
    pub(super) answered: bool,
}

impl Fetch {
    pub(super) const fn new(source: LogSource, from: Prefix, scope: crate::Scope) -> Fetch {
        Fetch {
            scope,
            source,
            from,
            entries: Vec::new(),
            request: None,
            retired: false,
            unanswered: 0,
            answered: false,
        }
    }

    /// The last op fetched, or `from`.
    pub(super) fn last(&self) -> Prefix {
        self.entries.last().map_or(self.from, Entry::prefix)
    }

    /// Fetched through the source's tail.
    pub(super) fn done(&self) -> bool {
        self.last() == self.source.accepted
    }

    /// Takes the ops fetched so far; fetching goes on after them.
    pub(super) fn drain(&mut self) -> Vec<Entry> {
        self.from = self.last();
        std::mem::take(&mut self.entries)
    }

    /// The source's digest at `op`, if fetched, or at `from` once the
    /// source answered from there.
    pub(super) fn digest_at(&self, op: OpNumber) -> Option<Digest> {
        if op == self.from.op {
            return self.answered.then_some(self.from.digest);
        }
        let i = op.0.checked_sub(self.from.op.0)?.checked_sub(1)?;
        let e = self.entries.get(usize::try_from(i).ok()?)?;
        Some(e.prefix().digest)
    }
}

impl Node {
    /// Fetches `source`'s history for an election, from the last op this
    /// log holds if the source has it, else from the commit.
    pub(super) fn fetch_election(&mut self, source: LogSource) {
        let last = self.log.last();
        let refused = self
            .fetch
            .as_ref()
            .is_some_and(|f| f.source == source && f.retired);
        let from = if last.op <= source.accepted.op && last.op > self.committed.op && !refused {
            last
        } else {
            self.committed
        };
        if refused {
            // Refused from the last op held: try from the commit.
            self.fetch = None;
        }
        self.fetch(source, from);
    }

    /// Starts fetching `source` after `from`, unless that runs already.
    pub(super) fn fetch(&mut self, source: LogSource, from: Prefix) {
        if self
            .fetch
            .as_ref()
            .is_some_and(|f| f.source == source && f.from.op <= from.op)
        {
            return;
        }
        // While recovering, the view the recovery learned; else the
        // election's.
        let scope = self.recovery_scope().unwrap_or_else(|| self.driver.scope());
        self.fetch = Some(Fetch::new(source, from, scope));
    }

    /// Sends the next fetch request, or the last again once it timed out.
    pub(super) fn poll_fetch(&mut self, now: Duration) {
        let Some(fetch) = self.fetch.as_mut() else {
            return;
        };
        if fetch.done() || fetch.retired {
            return;
        }
        if let Some((_, at)) = fetch.request
            && now < at + self.config.timing.retransmit
        {
            return;
        }
        if fetch.request.is_some() {
            fetch.unanswered += 1;
        }
        // The source may not have this log's last op: fall back to the commit.
        if fetch.unanswered >= 3 && fetch.entries.is_empty() && fetch.from.op > self.committed.op {
            *fetch = Fetch::new(fetch.source, self.committed, fetch.scope);
        }
        let request = FetchOps {
            scope: fetch.scope,
            request_id: RequestId::new(),
            source: fetch.source,
            predecessor: fetch.last(),
            max_operations: u32::try_from(self.config.wire.max_operations).unwrap_or(u32::MAX),
            max_body_bytes: u32::try_from(self.config.wire.envelope.max_payload_bytes)
                .unwrap_or(u32::MAX),
        };
        fetch.request = Some((request, now));
        let Some(i) = self.peer_index(request.source.voter) else {
            return;
        };
        let Some(session) = self.peers[i].session else {
            return;
        };
        let mut metadata = vec![0; 256];
        if let Ok(encoded) = encode_fetch(
            self.config.local,
            session,
            request,
            &mut metadata,
            self.config.wire,
        ) {
            metadata.truncate(encoded.metadata_bytes);
            self.out.push_back(Outgoing {
                to: request.source.voter,
                frames: [
                    Bytes::copy_from_slice(&encoded.header),
                    Bytes::from(metadata),
                    Bytes::new(),
                ],
            });
        }
    }

    /// Keeps the ops of a response to the request in flight, then asks for
    /// more at once.
    pub(super) fn on_ops(
        &mut self,
        batch: &OpsBatch<'_>,
        payload: &Bytes,
        now: Duration,
    ) -> Result<(), NodeError> {
        let Some(fetch) = self.fetch.as_mut() else {
            return Ok(());
        };
        let Some((request, _)) = fetch.request else {
            return Ok(());
        };
        batch.validate_response(request)?;
        for op in batch.operations() {
            let body = payload.slice_ref(op.canonical().body);
            fetch
                .entries
                .push(Entry::new(op.canonical(), body, op.body_digest()));
        }
        fetch.request = None;
        fetch.unanswered = 0;
        fetch.answered = true;
        self.driver.history_progress(now)?;
        self.poll_fetch(now);
        Ok(())
    }

    /// The source retired the ops a fetch asked for, or the primary the
    /// ops this follower needs next: it recovers.
    pub(super) fn on_retired(&mut self, from: NodeId, notice: HistoryRetired) {
        if let Some(normal) = self.driver.normal() {
            let s = normal.snapshot();
            if self.config.configuration.primary(s.scope.view) == from
                && notice.scope.view == s.scope.view
                && notice.before.op > s.accepted.op
            {
                self.begin_recovery();
                return;
            }
        }
        if let (Some(fetch), HistoryFence::Fetch(id)) = (self.fetch.as_mut(), notice.fence)
            && fetch.request.is_some_and(|(r, _)| r.request_id == id)
        {
            fetch.retired = true;
        }
    }

    /// Serves ops of this node's log to `from`, if it holds the source.
    pub(super) fn serve_fetch(&mut self, from: NodeId, request: FetchOps) {
        let Some(i) = self.peer_index(from) else {
            return;
        };
        self.peers[i].pin = Some((request.predecessor.op, self.now));
        let Some(session) = self.peers[i].session else {
            return;
        };
        if request.source.voter != self.config.local {
            return;
        }
        let out = if request.predecessor.op < self.log.base().op {
            // Gone, whatever the lineage: only a recovery helps.
            self.retired_message(from, session, request)
        } else if self.log.prefix_at(request.source.accepted.op) != Some(request.source.accepted) {
            None
        } else if self.log.prefix_at(request.predecessor.op) == Some(request.predecessor) {
            let count = (request.max_operations as usize).min(self.config.wire.max_operations);
            let bytes =
                (request.max_body_bytes as usize).min(self.config.wire.envelope.max_payload_bytes);
            let entries = self.log.after(request.predecessor.op, count, bytes);
            let end = entries
                .iter()
                .position(|e| e.prefix().op >= request.source.accepted.op)
                .map_or(entries.len(), |p| p + 1);
            let wire: Vec<_> = entries
                .get(..end)
                .unwrap_or_default()
                .iter()
                .map(Entry::wire)
                .collect();
            self.ops_message(from, session, request, &wire)
        } else {
            None
        };
        if let Some(out) = out {
            self.out.push_back(out);
        }
    }

    fn ops_message(
        &self,
        to: NodeId,
        session: crate::LinkSessionId,
        request: FetchOps,
        wire: &[crate::wire::Operation<'_>],
    ) -> Option<Outgoing> {
        let mut metadata = vec![0; self.config.wire.envelope.max_metadata_bytes];
        let mut payload = vec![0; wire.iter().map(|o| o.canonical().body.len()).sum()];
        let encoded = encode_ops(
            EncodeContext {
                sender: self.config.local,
                session,
                limits: self.config.wire,
            },
            request,
            wire,
            &mut metadata,
            &mut payload,
        )
        .ok()?;
        metadata.truncate(encoded.metadata_bytes);
        payload.truncate(encoded.payload_bytes);
        Some(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&encoded.header),
                Bytes::from(metadata),
                Bytes::from(payload),
            ],
        })
    }

    fn retired_message(
        &self,
        to: NodeId,
        session: crate::LinkSessionId,
        request: FetchOps,
    ) -> Option<Outgoing> {
        let notice = HistoryRetired {
            scope: request.scope,
            fence: HistoryFence::Fetch(request.request_id),
            before: self.log.base(),
        };
        self.notice_message(to, session, notice)
    }

    /// `HISTORY_RETIRED` for `to`.
    pub(super) fn notice_message(
        &self,
        to: NodeId,
        session: crate::LinkSessionId,
        notice: HistoryRetired,
    ) -> Option<Outgoing> {
        let mut metadata = vec![0; 256];
        let encoded = encode_history_retired(
            self.config.local,
            session,
            notice,
            &mut metadata,
            self.config.wire,
        )
        .ok()?;
        metadata.truncate(encoded.metadata_bytes);
        Some(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&encoded.header),
                Bytes::from(metadata),
                Bytes::new(),
            ],
        })
    }
}
