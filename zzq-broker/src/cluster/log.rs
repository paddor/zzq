//! A journal replicated to the cluster: records collect into groups as for
//! the disk, each group goes to the replica as one op, and its records
//! count as written once the op committed.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use bytes::{BufMut, Bytes, BytesMut};
use zzq_tokio::signal::{DataSignal, StateSignal};

use crate::disk::Completion;

/// Bytes after which a group goes out on its own, so an op fits a prepare.
pub(crate) const GROUP_BYTES: usize = 1 << 20;
/// Groups the replica holds from all journals before journals keep theirs.
const QUEUED_GROUPS: usize = 4096;

/// The journal a group belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Journal {
    /// A lane's sessions.
    Lane(u16),
    /// A stripe of retained messages.
    Retained(u8),
}

impl Journal {
    const fn tag(self) -> [u8; 3] {
        match self {
            Journal::Lane(i) => {
                let [a, b] = i.to_be_bytes();
                [0, a, b]
            }
            Journal::Retained(i) => [1, 0, i],
        }
    }

    /// The journal a group's first three bytes name.
    pub(crate) const fn read(head: [u8; 3]) -> Option<Journal> {
        match head {
            [0, a, b] => Some(Journal::Lane(u16::from_be_bytes([a, b]))),
            [1, 0, i] => Some(Journal::Retained(i)),
            _ => None,
        }
    }
}

/// One op's worth of records: the journal, then each record as kind, body
/// length and body.
#[derive(Debug)]
pub(crate) struct Group {
    pub(crate) journal: Journal,
    /// The number of its last record in its journal.
    pub(crate) last: u64,
    pub(crate) body: Bytes,
}

/// The records of a group's body, after its journal.
pub(crate) fn records(body: &[u8]) -> Option<(Journal, Records<'_>)> {
    let (head, rest) = body.split_first_chunk::<3>()?;
    Some((Journal::read(*head)?, Records(rest)))
}

/// Appends a record of `kind` whose body is `parts` to a group body.
fn put_record(buf: &mut BytesMut, kind: u8, parts: &[&[u8]]) {
    let len: usize = parts.iter().map(|p| p.len()).sum();
    buf.put_u8(kind);
    buf.put_u32(u32::try_from(len).unwrap_or(u32::MAX));
    for p in parts {
        buf.put_slice(p);
    }
}

/// Builds a group body outside a journal: a journal's part of an image.
#[derive(Debug)]
pub(crate) struct GroupWriter {
    buf: BytesMut,
    records: u64,
}

impl GroupWriter {
    pub(crate) fn new(journal: Journal) -> GroupWriter {
        let mut buf = BytesMut::new();
        buf.put_slice(&journal.tag());
        GroupWriter { buf, records: 0 }
    }

    pub(crate) fn finish(self) -> Bytes {
        self.buf.freeze()
    }
}

impl crate::disk::Append for GroupWriter {
    fn append(&mut self, kind: u8, parts: &[&[u8]]) -> std::io::Result<u64> {
        put_record(&mut self.buf, kind, parts);
        self.records += 1;
        Ok(self.records)
    }
}

/// Records of a group body: kind and body each. Stops at a malformed one.
#[derive(Debug)]
pub(crate) struct Records<'a>(&'a [u8]);

impl<'a> Iterator for Records<'a> {
    type Item = Result<(u8, &'a [u8]), ()>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.0.is_empty() {
            return None;
        }
        let parsed = (|| {
            let (&kind, rest) = self.0.split_first()?;
            let (len, rest) = rest.split_first_chunk::<4>()?;
            let (body, rest) = rest.split_at_checked(u32::from_be_bytes(*len) as usize)?;
            Some((kind, body, rest))
        })();
        let Some((kind, body, rest)) = parsed else {
            self.0 = &[];
            return Some(Err(()));
        };
        self.0 = rest;
        Some(Ok((kind, body)))
    }
}

/// Groups from every journal for the replica, oldest first.
#[derive(Debug, Default)]
pub(crate) struct Groups {
    queue: Mutex<VecDeque<Group>>,
    /// Marked on the push that made the queue non-empty.
    pub(crate) ready: DataSignal,
    /// The node as the replica last saw it.
    pub(crate) status: Mutex<Option<zzq_cluster::node::NodeStatus>>,
    /// Endpoints for clients, bound only while the node leads.
    pub(crate) listeners: Mutex<Listeners>,
}

/// A node's client endpoints, and the accept tasks while it leads.
#[derive(Debug, Default)]
pub(crate) struct Listeners {
    pub(crate) endpoints: Vec<zzq_tokio::Endpoint>,
    pub(crate) bound: Vec<tokio::task::AbortHandle>,
    pub(crate) leading: bool,
    /// Counts changes of `leading`, so a bind that finishes after one
    /// closes its listener.
    pub(crate) epoch: u64,
}

impl Listeners {
    /// Keeps the accept task of a bind started in `epoch`, or ends it if
    /// the leadership changed since.
    pub(crate) fn own(&mut self, task: tokio::task::AbortHandle, epoch: u64) {
        if self.epoch == epoch {
            self.bound.push(task);
        } else {
            task.abort();
        }
    }
}

impl Groups {
    fn lock(&self) -> MutexGuard<'_, VecDeque<Group>> {
        self.queue.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Queues what fits of `groups`, from the front. Returns those that do
    /// not.
    fn push(&self, groups: &mut VecDeque<Group>) {
        let mut q = self.lock();
        let was_empty = q.is_empty();
        while q.len() < QUEUED_GROUPS {
            let Some(g) = groups.pop_front() else {
                break;
            };
            q.push_back(g);
        }
        let pushed = was_empty && !q.is_empty();
        drop(q);
        if pushed {
            self.ready.mark();
        }
    }

    /// Takes every group queued.
    pub(crate) fn take(&self) -> VecDeque<Group> {
        std::mem::take(&mut *self.lock())
    }
}

/// How far a journal's groups committed, for its completion task.
#[derive(Debug, Default)]
pub(crate) struct Commits {
    state: Mutex<CommitState>,
    changed: StateSignal,
}

#[derive(Debug, Default)]
struct CommitState {
    committed: u64,
    reported: u64,
    /// Record ranges whose op will never commit, oldest first.
    failed: VecDeque<(u64, u64)>,
    closed: bool,
}

impl Commits {
    fn lock(&self) -> MutexGuard<'_, CommitState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records up to `last` committed.
    pub(crate) fn commit(&self, last: u64) {
        {
            let mut st = self.lock();
            st.committed = st.committed.max(last);
        }
        self.changed.notify_changed();
    }

    /// Records `first..=last` will never commit, as their op was lost with
    /// the leadership.
    pub(crate) fn fail(&self, first: u64, last: u64) {
        self.lock().failed.push_back((first, last));
        self.changed.notify_changed();
    }

    /// No more commits come.
    pub(crate) fn close(&self) {
        self.lock().closed = true;
        self.changed.notify_changed();
    }

    /// The next completion, as the disk reports one.
    pub(crate) async fn next(&self) -> Completion {
        self.changed
            .until(|| {
                let mut st = self.lock();
                if let Some((first, last)) = st.failed.pop_front() {
                    st.reported = st.reported.max(last);
                    return Some(Completion::Failed(first, last));
                }
                if st.committed > st.reported {
                    st.reported = st.committed;
                    return Some(Completion::Written(st.committed));
                }
                st.closed.then_some(Completion::Closed)
            })
            .await
    }
}

/// One journal's side: the group it builds and those it holds back while
/// the replica's queue is full.
#[derive(Debug)]
pub(crate) struct ClusterLog {
    journal: Journal,
    groups: Arc<Groups>,
    pub(crate) commits: Arc<Commits>,
    /// The group being built, with its journal head.
    buf: BytesMut,
    /// Records in `buf` and in `held`.
    pending: usize,
    held: VecDeque<Group>,
    /// The number of the last record appended.
    appended: u64,
    /// The number of the last record handed to the replica.
    handed: u64,
}

impl ClusterLog {
    pub(crate) fn new(journal: Journal, groups: Arc<Groups>) -> ClusterLog {
        ClusterLog {
            journal,
            groups,
            commits: Arc::default(),
            buf: BytesMut::new(),
            pending: 0,
            held: VecDeque::new(),
            appended: 0,
            handed: 0,
        }
    }

    /// Appends a record of `kind` whose body is `parts`. Returns its number.
    pub(crate) fn append(&mut self, kind: u8, parts: &[&[u8]]) -> u64 {
        let len: usize = parts.iter().map(|p| p.len()).sum();
        if !self.buf.is_empty() && self.buf.len() + 5 + len > GROUP_BYTES {
            self.seal();
        }
        if self.buf.is_empty() {
            self.buf.put_slice(&self.journal.tag());
        }
        put_record(&mut self.buf, kind, parts);
        self.appended += 1;
        self.pending += 1;
        self.appended
    }

    /// Ends the group being built.
    fn seal(&mut self) {
        if self.buf.is_empty() {
            return;
        }
        self.held.push_back(Group {
            journal: self.journal,
            last: self.appended,
            body: self.buf.split().freeze(),
        });
    }

    /// Hands the records appended so far to the replica, as far as its
    /// queue has room.
    pub(crate) fn flush(&mut self) {
        self.seal();
        self.groups.push(&mut self.held);
        if self.held.is_empty() {
            self.pending = 0;
            self.handed = self.appended;
        }
    }

    /// Drops the records not handed to the replica, as their changes are
    /// dropped with the lane state of a leader that lost its leadership:
    /// whatever waits for them fails.
    pub(crate) fn discard(&mut self) {
        self.buf.clear();
        self.held.clear();
        self.pending = 0;
        if self.appended > self.handed {
            self.commits.fail(self.handed + 1, self.appended);
        }
        self.handed = self.appended;
    }

    /// Records appended that the replica does not hold yet.
    pub(crate) const fn backlog(&self) -> usize {
        self.pending
    }

    /// The number of the last record appended.
    pub(crate) const fn appended(&self) -> u64 {
        self.appended
    }
}

impl Drop for ClusterLog {
    /// The journal closed: its completion task stops.
    fn drop(&mut self) {
        self.commits.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_group_holds_its_journal_and_records_in_order() {
        let groups = Arc::new(Groups::default());
        let mut log = ClusterLog::new(Journal::Lane(3), groups.clone());
        assert_eq!(log.append(1, &[b"ab", b"c"]), 1);
        assert_eq!(log.append(2, &[]), 2);
        log.flush();
        let mut taken = groups.take();
        let g = taken.pop_front().unwrap();
        assert_eq!((g.journal, g.last), (Journal::Lane(3), 2));
        let (journal, recs) = records(&g.body).unwrap();
        assert_eq!(journal, Journal::Lane(3));
        let recs: Vec<_> = recs.map(Result::unwrap).collect();
        assert_eq!(recs, [(1, &b"abc"[..]), (2, &b""[..])]);
        assert_eq!(log.backlog(), 0);
    }

    #[test]
    fn a_big_group_splits_before_the_record_that_would_overflow_it() {
        let groups = Arc::new(Groups::default());
        let mut log = ClusterLog::new(Journal::Retained(7), groups.clone());
        let big = vec![0; GROUP_BYTES / 2];
        for _ in 0..3 {
            log.append(9, &[&big]);
        }
        log.flush();
        let lasts: Vec<u64> = groups.take().iter().map(|g| g.last).collect();
        assert_eq!(lasts, [1, 2, 3]);
    }

    #[test]
    fn malformed_records_end_the_walk_with_an_error() {
        let body = [0, 0, 1, 5, 0, 0, 0, 9, 1];
        let (_, mut recs) = records(&body).unwrap();
        assert_eq!(recs.next(), Some(Err(())));
        assert_eq!(recs.next(), None);
        assert!(records(&[2, 0]).is_none());
    }

    #[tokio::test]
    async fn commits_report_failures_then_progress_then_the_close() {
        let c = Commits::default();
        c.commit(4);
        c.fail(5, 6);
        assert_eq!(c.next().await, Completion::Failed(5, 6));
        c.commit(9);
        assert_eq!(c.next().await, Completion::Written(9));
        c.close();
        assert_eq!(c.next().await, Completion::Closed);
    }

    #[tokio::test]
    async fn a_closed_journal_ends_its_completions() {
        let log = ClusterLog::new(Journal::Lane(0), Arc::default());
        let commits = log.commits.clone();
        drop(log);
        assert_eq!(commits.next().await, Completion::Closed);
    }

    #[test]
    fn a_bind_that_outlived_the_leadership_closes_its_listener() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let set = tokio::task::LocalSet::new();
        set.block_on(&rt, async {
            let mut l = Listeners::default();
            let kept = tokio::task::spawn_local(std::future::pending::<()>());
            let late = tokio::task::spawn_local(std::future::pending::<()>());
            l.own(kept.abort_handle(), 0);
            l.epoch += 1;
            l.own(late.abort_handle(), 0);
            assert_eq!(l.bound.len(), 1);
            assert!(late.await.unwrap_err().is_cancelled());
        });
    }
}
