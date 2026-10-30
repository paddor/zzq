//! Elections past the core's messages: the new primary selects a history
//! and installs it, a backup installs what the primary's `START_VIEW`
//! names, and the application catches up before either serves.

use std::time::Duration;

use super::history::Fetch;
use super::log::Log;
use super::{Event, Node, NodeError, fresh_generation};
use crate::driver::DriverError;
use crate::{
    Digest, InstallTicket, LogSource, NodeId, OpNumber, Prefix, PreparedOperation,
    ReplicationError, StartView, ViewChangeError,
};

/// The history a node holds for lookups during an election.
struct Held<'a> {
    log: &'a Log,
    fetch: Option<&'a Fetch>,
    local: NodeId,
    committed: Prefix,
}

impl Held<'_> {
    /// The digest of `source`'s history at `prefix.op`, if this node holds
    /// it: in its own log when that holds the source's tail or the op is
    /// committed here, else in what it fetched from the source.
    fn lookup(&self, source: LogSource, prefix: Prefix) -> Option<Digest> {
        let Held {
            log,
            fetch,
            local,
            committed,
        } = *self;
        // A fetch from the source chained from an op of this log: the log
        // is in the source's history through it.
        let chained = fetch.is_some_and(|f| {
            f.source == source
                && f.answered
                && prefix.op <= f.from.op
                && log.prefix_at(f.from.op) == Some(f.from)
        });
        let own = source.voter == local
            || log.prefix_at(source.accepted.op) == Some(source.accepted)
            || prefix.op <= committed.op
            || chained;
        if own && let Some(p) = log.prefix_at(prefix.op) {
            return Some(p.digest);
        }
        // Committed here and trimmed: committed prefixes are the same on
        // every node, so the one named stands.
        if prefix.op <= committed.op && prefix.op < log.base().op {
            return Some(prefix.digest);
        }
        fetch
            .filter(|f| f.source == source)
            .and_then(|f| f.digest_at(prefix.op))
    }
}

/// Election errors that more messages or history can resolve.
const fn waiting(e: DriverError) -> bool {
    matches!(
        e,
        DriverError::ViewChange(
            ViewChangeError::PromiseRequired
                | ViewChangeError::PromisePending
                | ViewChangeError::StoragePending
                | ViewChangeError::ReportQuorumMissing
                | ViewChangeError::StartQuorumMissing
                | ViewChangeError::HistoryMissing
                | ViewChangeError::SelectionRequired
                | ViewChangeError::Replication(
                    ReplicationError::WrongRole | ReplicationError::ScopeMismatch
                ),
        ) | DriverError::Replication(ReplicationError::WrongRole | ReplicationError::NotNormal)
    )
}

impl Node {
    /// Moves an election on: select and install as primary, install as
    /// backup, or finish an installation once the application caught up.
    pub(super) fn elect(&mut self, now: Duration) -> Result<(), NodeError> {
        if let Some(ticket) = self.driver.installation_ticket() {
            return self.finish_install(&ticket, now);
        }
        if self.driver.normal().is_some() {
            return Ok(());
        }
        let view = self.driver.scope().view;
        let result = if self.config.configuration.primary(view) == self.config.local {
            self.select()
        } else if let Some((from, start)) = self.start.filter(|(_, s)| s.scope.view == view) {
            self.install_backup(from, start)
        } else {
            Ok(())
        };
        match result {
            Err(NodeError::Driver(e)) if waiting(e) => Ok(()),
            other => other,
        }
    }

    fn select(&mut self) -> Result<(), NodeError> {
        let held = Held {
            log: &self.log,
            fetch: self.fetch.as_ref(),
            local: self.config.local,
            committed: self.committed,
        };
        let selected = match self.driver.select(|s, p| held.lookup(s, p)) {
            Ok(selected) => selected,
            Err(DriverError::ViewChange(ViewChangeError::HistoryMissing)) => {
                self.fetch_reported();
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        };
        let source = selected.source();
        if !self.holds(source) {
            self.fetch_election(source);
            return Ok(());
        }
        let ticket = self.driver.begin_primary_install(fresh_generation())?;
        self.stage(&ticket)
    }

    /// Fetches the reported history this node lacks, the longest first.
    fn fetch_reported(&mut self) {
        let remote = self
            .driver
            .history_sources()
            .into_iter()
            .flatten()
            .filter(|s| s.voter != self.config.local && !self.holds(*s));
        let next = match &self.fetch {
            Some(f) if !f.done() && !f.retired => return,
            Some(f) => remote
                .filter(|s| *s != f.source)
                .max_by_key(|s| s.accepted.op),
            None => remote.max_by_key(|s| s.accepted.op),
        };
        if let Some(source) = next {
            self.fetch_election(source);
        }
    }

    /// Whether this node holds `source`'s history: its own log has the
    /// tail, or a finished fetch does.
    fn holds(&self, source: LogSource) -> bool {
        source.voter == self.config.local
            || self.log.prefix_at(source.accepted.op) == Some(source.accepted)
            || self
                .fetch
                .as_ref()
                .is_some_and(|f| f.source == source && f.done())
    }

    fn install_backup(&mut self, from: NodeId, start: StartView) -> Result<(), NodeError> {
        let source = LogSource {
            voter: from,
            generation: start.generation,
            accepted: start.accepted,
        };
        if !self.holds(source) {
            // The primary no longer holds the ops after this log's commit:
            // only a recovery from both others can bring it back.
            if self
                .fetch
                .as_ref()
                .is_some_and(|f| f.source == source && f.retired && f.from.op <= self.committed.op)
            {
                self.begin_recovery();
                return Ok(());
            }
            self.fetch_election(source);
            return Ok(());
        }
        let held = Held {
            log: &self.log,
            fetch: self.fetch.as_ref(),
            local: self.config.local,
            committed: self.committed,
        };
        let ticket =
            self.driver
                .begin_backup_install(from, start, fresh_generation(), |s, p| held.lookup(s, p))?;
        self.stage(&ticket)
    }

    /// Builds the selected history beside the log and checks it with the
    /// core, then lets the application catch up through its commit. The
    /// log takes it once the installation completes; an abandoned one
    /// leaves the log as the core's old promise describes it.
    fn stage(&mut self, ticket: &InstallTicket) -> Result<(), NodeError> {
        let accepted = ticket.accepted();
        let retained = ticket.retained();
        let mut log = self.log.clone();
        if log.prefix_at(accepted.op) == Some(accepted) {
            log.truncate_after(accepted.op);
        } else {
            let fetch = self
                .fetch
                .take()
                .filter(|f| f.source == ticket.source() && f.done())
                .ok_or(ViewChangeError::HistoryMissing)?;
            // The fetch chained from an op this log holds: the log is in the
            // source's history through it.
            let keep = if fetch.from.op >= retained.op
                && log.prefix_at(fetch.from.op) == Some(fetch.from)
            {
                fetch.from
            } else if fetch.digest_at(retained.op) == Some(retained.digest) {
                retained
            } else {
                return Err(ViewChangeError::ConflictingHistory.into());
            };
            log.truncate_after(keep.op);
            let trimmable = OpNumber(self.app_applied.0.min(retained.op.0));
            for e in fetch
                .entries
                .into_iter()
                .filter(|e| e.prefix().op > keep.op)
            {
                log.push(e, trimmable, trimmable);
            }
        }
        if self.tip.op > retained.op && log.prefix_at(self.tip.op) != Some(self.tip) {
            self.reset_app(log.base().op.0 == 0);
        }
        let limits = self.config.pipeline;
        let mut op = ticket.committed().op;
        while op < accepted.op {
            let chunk: Vec<PreparedOperation> = log
                .after(op, limits.max_operations, limits.max_body_bytes)
                .iter()
                .map(|e| e.prepared)
                .collect();
            let Some(last) = chunk.last() else {
                return Err(ReplicationError::HistoryGap.into());
            };
            op = last.prefix().op;
            self.driver.validate_install_suffix(&chunk)?;
        }
        self.staged = Some(log);
        Ok(())
    }

    /// The application holds ops the selected history does not: it starts
    /// over from the first op, or from an image.
    /// The application dropped its state, as an old leader must whose
    /// lanes held changes not committed: committed ops come again from the
    /// first, or after an image when the log no longer holds them. No
    /// [`Event::Reset`] follows; the application reset itself.
    pub fn reset_application(&mut self) {
        let replayable = self.log.base().op.0 == 0;
        self.reset_app(replayable);
        if self.events.back() == Some(&Event::Reset) {
            self.events.pop_back();
        }
        self.own_from = None;
    }

    fn reset_app(&mut self, replayable: bool) {
        self.events.push_back(Event::Reset);
        self.app_applied = OpNumber(0);
        self.tip = Prefix::GENESIS;
        if replayable {
            self.emitted = OpNumber(0);
        } else {
            // The log cannot replay from the first op: an image comes
            // first, and ops after it.
            self.emitted = OpNumber(u64::MAX);
            self.start_rebuild();
        }
    }

    fn finish_install(&mut self, ticket: &InstallTicket, now: Duration) -> Result<(), NodeError> {
        // A later view was asked for while this one installs: give it up,
        // so the next promise can go out.
        if self.driver.scope().view > ticket.scope().view {
            self.driver.complete_abandon(ticket, now)?;
            self.fetch = None;
            self.staged = None;
            return Ok(());
        }
        // A backup that rebuilds from the primary's image cannot wait for
        // it: the primary offers one only once a backup activated its view.
        let rebuilding = self.images.rebuild.is_some()
            && self.config.configuration.primary(ticket.scope().view) != self.config.local;
        if self.app_applied < ticket.committed().op && !rebuilding {
            return Ok(());
        }
        self.driver
            .complete_installation(ticket, ticket.committed(), now)?;
        if let Some(log) = self.staged.take() {
            self.log = log;
        }
        let accepted = ticket.accepted().op;
        self.send_floor = accepted;
        for peer in self.peers.iter_mut() {
            peer.sent = accepted;
            peer.acked = OpNumber(0);
        }
        self.start = None;
        self.fetch = None;
        self.ack_due = true;
        Ok(())
    }
}
