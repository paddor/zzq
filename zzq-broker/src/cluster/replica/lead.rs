//! Leadership: a follower's state goes to the lanes when the node leads;
//! a leader that loses its lease waits as long as it stays the leader of
//! its view, and drops its state once it no longer is.

use std::time::Duration;

use super::{App, Replica, appended, commits, journals};
use zzq_cluster::OpNumber;

/// How long a leader without its lease keeps its state and its clients
/// before the others surely lead without it.
const SUSPENDED_FOR: Duration = Duration::from_millis(1500);

impl Replica {
    /// The node leads: from a follower's state, or again after a lapse of
    /// its lease.
    pub(super) async fn lead(&mut self) {
        if self.suspended.take().is_some() {
            crate::cluster::open_listeners(&self.shared).await;
            return;
        }
        self.promote().await;
    }

    /// The node does not lead now. Still the leader of its view, it only
    /// missed acks: it stops taking clients and waits for them.
    pub(super) async fn step_down(&mut self) {
        if matches!(self.app, App::Leader) && self.node.leads_view() {
            self.suspended.get_or_insert(self.now());
            crate::cluster::close_listeners(&self.shared);
            return;
        }
        self.suspended = None;
        self.demote().await;
    }

    /// A suspended leader that no longer leads its view, or that waited
    /// too long, steps down for good.
    pub(super) async fn check_suspended(&mut self, now: Duration) {
        let Some(since) = self.suspended else {
            return;
        };
        if !self.node.leads_view() || now >= since + SUSPENDED_FOR {
            self.suspended = None;
            self.demote().await;
        }
    }

    /// The node's own ops through `through` committed.
    pub(super) fn commit_through(&mut self, through: OpNumber) {
        while self.proposed.front().is_some_and(|p| p.op <= through) {
            if let Some(p) = self.proposed.pop_front()
                && let Some(c) = commits(&self.shared, p.journal)
            {
                c.commit(p.last);
            }
        }
    }

    /// Hands the follower's state to the lanes, which serve it from now.
    async fn promote(&mut self) {
        let App::Follower(acc) = std::mem::replace(&mut self.app, App::Leader) else {
            return;
        };
        let (sessions, retained) = acc.finish();
        retained.install(&self.shared.retained);
        let _ = crate::dump::restore(&self.shared, sessions).await;
        if std::mem::take(&mut self.rewrite) {
            self.rewrite_journals().await;
        }
        crate::cluster::open_listeners(&self.shared).await;
    }

    /// Every lane and stripe journals its whole state, before any client
    /// comes: the followers of a forced epoch start from it.
    async fn rewrite_journals(&self) {
        let mut done = Vec::new();
        for lane in &self.shared.lanes {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if lane.control.push(crate::lane::Control::Rewrite(tx)).is_ok() {
                done.push(rx);
            }
        }
        for rx in done {
            let _ = rx.await;
        }
        self.shared.retained.rewrite();
    }

    /// The node no longer leads: what it proposed and did not commit
    /// fails, and the lanes drop their state, which may hold such changes.
    /// The committed state comes back as a follower's, from the ops again
    /// or an image.
    async fn demote(&mut self) {
        if matches!(self.app, App::Leader) {
            self.drop_state().await;
            self.node.reset_application();
        }
    }

    /// The node dropped the application's state: the ops come again. A
    /// leader's lanes drop theirs too.
    pub(super) async fn reset(&mut self) {
        if matches!(self.app, App::Leader) {
            self.suspended = None;
            self.drop_state().await;
        }
        self.app = App::Follower(self.empty());
    }

    /// What a leader proposed and did not commit fails, its lanes and
    /// stripes drop their state, and it holds a follower's, empty.
    async fn drop_state(&mut self) {
        self.fail_proposed();
        crate::cluster::close_listeners(&self.shared);
        let mut done = Vec::new();
        for lane in &self.shared.lanes {
            let (tx, rx) = tokio::sync::oneshot::channel();
            if lane.control.push(crate::lane::Control::Reset(tx)).is_ok() {
                done.push(rx);
            }
        }
        for rx in done {
            let _ = rx.await;
        }
        self.shared.retained.reset();
        // What the lanes handed over before they dropped their state goes
        // with it, and the numbers of what they discarded are passed.
        self.held.extend(self.shared.cluster_groups().take());
        self.fail_held();
        for j in journals(&self.shared) {
            if let Some(n) = appended(&self.shared, j) {
                let taken = self.taken.entry(j).or_insert(0);
                *taken = n.max(*taken);
            }
        }
        self.cut = None;
        self.app = App::Follower(self.empty());
    }
}
