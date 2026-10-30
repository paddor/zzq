//! The ops a node holds in RAM: a contiguous run after a base prefix,
//! bounded by count and bytes.

use std::collections::VecDeque;

use bytes::Bytes;

use crate::operation::{CanonicalOperation, OperationKind, canonical_body_digest};
use crate::wire::Operation;
use crate::{Configuration, Digest, OpNumber, Prefix, PreparedOperation};

/// One op with its body.
#[derive(Debug, Clone)]
pub(crate) struct Entry {
    pub(crate) prepared: PreparedOperation,
    pub(crate) kind: OperationKind,
    pub(crate) body: Bytes,
    pub(crate) body_digest: Digest,
}

impl Entry {
    /// The entry for `canonical`, whose body digest is `body_digest`.
    pub(crate) fn new(
        canonical: CanonicalOperation<'_>,
        body: Bytes,
        body_digest: Digest,
    ) -> Entry {
        Entry {
            prepared: PreparedOperation::from_verified(&canonical, body_digest),
            kind: canonical.kind,
            body,
            body_digest,
        }
    }

    /// A new op after `previous`, hashing `body`.
    pub(crate) fn propose(
        configuration: &Configuration,
        view: u64,
        previous: Prefix,
        kind: OperationKind,
        body: Bytes,
    ) -> Entry {
        let scope = configuration.scope();
        let body_digest = canonical_body_digest(&body);
        let prepared = PreparedOperation::from_verified(
            &CanonicalOperation {
                group_id: scope.group_id,
                configuration_epoch: scope.configuration_epoch,
                original_view: view,
                op_number: previous.op.0 + 1,
                previous_digest: previous.digest,
                kind,
                body: &body,
            },
            body_digest,
        );
        Entry {
            prepared,
            kind,
            body,
            body_digest,
        }
    }

    pub(crate) const fn prefix(&self) -> Prefix {
        self.prepared.prefix()
    }

    /// The op as the wire sends it, borrowing the body.
    pub(crate) fn wire(&self) -> Operation<'_> {
        Operation::from_verified(
            CanonicalOperation {
                group_id: self.prepared.group_id,
                configuration_epoch: self.prepared.configuration_epoch,
                original_view: self.prepared.original_view,
                op_number: self.prepared.prefix.op.0,
                previous_digest: self.prepared.previous_digest,
                kind: self.kind,
                body: &self.body,
            },
            self.body_digest,
        )
    }
}

/// Count and byte caps of a node's log. Ops a follower still needs stay
/// past them, up to four times them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogLimits {
    /// Most ops held.
    pub max_operations: usize,
    /// Most body bytes held.
    pub max_body_bytes: usize,
}

impl Default for LogLimits {
    fn default() -> Self {
        LogLimits {
            max_operations: 1 << 16,
            max_body_bytes: 64 << 20,
        }
    }
}

/// Ops after `base`, oldest first.
#[derive(Debug, Clone)]
pub(crate) struct Log {
    base: Prefix,
    entries: VecDeque<Entry>,
    bytes: usize,
    limits: LogLimits,
}

impl Log {
    pub(crate) const fn new(base: Prefix, limits: LogLimits) -> Log {
        Log {
            base,
            entries: VecDeque::new(),
            bytes: 0,
            limits,
        }
    }

    /// The prefix before the first op held.
    pub(crate) const fn base(&self) -> Prefix {
        self.base
    }

    /// The last op held, or the base.
    pub(crate) fn last(&self) -> Prefix {
        self.entries.back().map_or(self.base, Entry::prefix)
    }

    /// Op `op`, if held.
    pub(crate) fn get(&self, op: OpNumber) -> Option<&Entry> {
        let index = op.0.checked_sub(self.base.op.0)?.checked_sub(1)?;
        self.entries.get(usize::try_from(index).ok()?)
    }

    /// The prefix at `op`: the base, or an op held.
    pub(crate) fn prefix_at(&self, op: OpNumber) -> Option<Prefix> {
        if op == self.base.op {
            return Some(self.base);
        }
        self.get(op).map(Entry::prefix)
    }

    /// Ops after `after`, at most `count` of them and `bytes` body bytes,
    /// but at least one if any. One run of the ring: call again after the
    /// last for more.
    pub(crate) fn after(&self, after: OpNumber, count: usize, bytes: usize) -> &[Entry] {
        let Some(start) = after
            .0
            .checked_sub(self.base.op.0)
            .and_then(|i| usize::try_from(i).ok())
        else {
            return &[];
        };
        let (front, back) = self.entries.as_slices();
        let run = match front.get(start..) {
            Some(run) if !run.is_empty() => run,
            _ => back
                .get(start - front.len().min(start)..)
                .unwrap_or_default(),
        };
        let mut total = 0;
        let mut n = 0;
        for e in run.iter().take(count) {
            total += e.body.len();
            if n > 0 && total > bytes {
                break;
            }
            n += 1;
        }
        run.get(..n).unwrap_or_default()
    }

    /// Every op after `after`, oldest first.
    pub(crate) fn iter_after(&self, after: OpNumber) -> impl Iterator<Item = &Entry> {
        let skip = after
            .0
            .saturating_sub(self.base.op.0)
            .try_into()
            .unwrap_or(usize::MAX);
        self.entries.iter().skip(skip)
    }

    /// Appends `entry`, the op after the last. Past the caps it drops the
    /// oldest ops through `kept`; past four times the caps, through
    /// `trimmable`. False if still past the caps.
    pub(crate) fn push(&mut self, entry: Entry, kept: OpNumber, trimmable: OpNumber) -> bool {
        debug_assert_eq!(entry.prepared.previous_digest, self.last().digest);
        self.bytes += entry.body.len();
        self.entries.push_back(entry);
        while self.over(4) && self.front_through(trimmable) {
            self.pop();
        }
        while self.over(1) && self.front_through(kept.min(trimmable)) {
            self.pop();
        }
        !self.over(1)
    }

    fn front_through(&self, op: OpNumber) -> bool {
        self.entries.front().is_some_and(|e| e.prefix().op <= op)
    }

    fn over(&self, times: usize) -> bool {
        self.entries.len() > self.limits.max_operations.saturating_mul(times)
            || self.bytes > self.limits.max_body_bytes.saturating_mul(times)
    }

    fn pop(&mut self) {
        if let Some(e) = self.entries.pop_front() {
            self.bytes -= e.body.len();
            self.base = e.prefix();
        }
    }

    /// Drops ops after `op`, which must be held or the base.
    pub(crate) fn truncate_after(&mut self, op: OpNumber) {
        while self.entries.back().is_some_and(|e| e.prefix().op > op) {
            if let Some(e) = self.entries.pop_back() {
                self.bytes -= e.body.len();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{GroupId, NodeId};

    fn configuration() -> Configuration {
        Configuration::new(
            GroupId::from_bytes([7; 16]),
            1,
            Digest::from_bytes([8; 32]),
            [1, 2, 3].map(|i| NodeId::from_bytes([i; 16])),
        )
        .unwrap()
    }

    fn fill(log: &mut Log, n: u8, trimmable: OpNumber) -> Vec<bool> {
        (0..n)
            .map(|i| {
                let e = Entry::propose(
                    &configuration(),
                    0,
                    log.last(),
                    OperationKind::Changes,
                    Bytes::from(vec![i; 10]),
                );
                log.push(e, trimmable, trimmable)
            })
            .collect()
    }

    #[test]
    fn ops_chain_and_are_found_by_number() {
        let mut log = Log::new(Prefix::GENESIS, LogLimits::default());
        fill(&mut log, 3, OpNumber(0));
        assert_eq!(log.last().op, OpNumber(3));
        let second = log.get(OpNumber(2)).unwrap();
        assert_eq!(
            second.prepared.previous_digest,
            log.prefix_at(OpNumber(1)).unwrap().digest
        );
        assert_eq!(log.prefix_at(OpNumber(0)), Some(Prefix::GENESIS));
        assert!(log.get(OpNumber(4)).is_none());
        assert!(log.get(OpNumber(0)).is_none());
    }

    #[test]
    fn a_full_log_drops_only_trimmable_ops() {
        let limits = LogLimits {
            max_operations: 2,
            max_body_bytes: 1000,
        };
        let mut log = Log::new(Prefix::GENESIS, limits);
        assert_eq!(fill(&mut log, 3, OpNumber(1)), [true, true, true]);
        assert_eq!(log.base().op, OpNumber(1));
        assert_eq!(
            fill(&mut log, 1, OpNumber(1)),
            [false],
            "op 2 not trimmable"
        );
        assert_eq!(log.base().op, OpNumber(1));
    }

    #[test]
    fn after_bounds_by_count_and_bytes_but_returns_one() {
        let mut log = Log::new(Prefix::GENESIS, LogLimits::default());
        fill(&mut log, 5, OpNumber(0));
        assert_eq!(log.after(OpNumber(1), 2, 1000).len(), 2);
        assert_eq!(log.after(OpNumber(1), 9, 25).len(), 2);
        assert_eq!(log.after(OpNumber(1), 9, 1).len(), 1);
        assert_eq!(log.after(OpNumber(5), 9, 1000).len(), 0);
        assert_eq!(log.after(OpNumber(0), 9, 1000)[0].prefix().op, OpNumber(1));
    }

    #[test]
    fn iter_after_crosses_the_ring_where_after_stops() {
        let limits = LogLimits {
            max_operations: 4,
            max_body_bytes: 1000,
        };
        let mut log = Log::new(Prefix::GENESIS, limits);
        fill(&mut log, 10, OpNumber(100));
        let all: Vec<u64> = log
            .iter_after(log.base().op)
            .map(|e| e.prefix().op.0)
            .collect();
        assert_eq!(all, [7, 8, 9, 10]);
        assert_eq!(log.iter_after(OpNumber(8)).count(), 2);
        assert_eq!(log.iter_after(OpNumber(10)).count(), 0);
    }

    #[test]
    fn truncate_keeps_the_chain_whole() {
        let mut log = Log::new(Prefix::GENESIS, LogLimits::default());
        fill(&mut log, 4, OpNumber(0));
        let two = log.prefix_at(OpNumber(2)).unwrap();
        log.truncate_after(OpNumber(2));
        assert_eq!(log.last(), two);
        fill(&mut log, 1, OpNumber(0));
        assert_eq!(log.last().op, OpNumber(3));
    }
}
