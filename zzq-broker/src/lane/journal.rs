//! A lane's journal: changes to the sessions that outlive their
//! connection, in the order the lane makes them.

use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll};

use bytes::Bytes;
use zzq_proto::{QoS, SubscriptionOptions};

use crate::cluster::{GroupWriter, Journal};
use crate::disk::{self, Append, Completion, Log};
use crate::durable::{Cursor, JournalId, Journaled, RETRY, Wait};
use crate::index::SubIds;
use crate::msg::Publication;
use crate::session::{Progress, Session};
use crate::slab::{Key, Slab};
use crate::state::Clock;
use crate::state::journal::{self as rec, CLEAR, CLEARED, IMAGE};

use super::route::Owned;
use super::{Lane, LaneRef};

/// A lane's journal. Only its lane writes it; it sits in `Shared` so the
/// broker handle can close it.
#[derive(Debug, Default)]
pub(crate) struct LaneLog {
    /// The lane it belongs to.
    lane: usize,
    state: Mutex<LogState>,
    pub(crate) cursor: Cursor,
    /// Writes that failed.
    pub(crate) write_errors: AtomicU64,
}

#[derive(Debug, Default)]
struct LogState {
    log: Option<Log>,
    /// A write failed: the journal may hold other sessions than the lane.
    /// The next change writes them all.
    stale: bool,
    /// The journal was closed: changes fail.
    closed: bool,
    /// Reused for record bodies.
    buf: Vec<u8>,
    /// Bytes the journal had appended at the last snapshot.
    snapshot_at: u64,
    /// The segment the last snapshot started in, and its last record:
    /// once that is written, the segments before go.
    trim: Option<(u64, u64)>,
}

impl LaneLog {
    pub(crate) fn new(lane: usize, log: Option<Log>) -> LaneLog {
        LaneLog {
            lane,
            state: Mutex::new(LogState {
                log,
                ..LogState::default()
            }),
            ..LaneLog::default()
        }
    }

    fn lock(&self) -> MutexGuard<'_, LogState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The commits of its cluster journal.
    pub(crate) fn commits(&self) -> Option<std::sync::Arc<crate::cluster::Commits>> {
        self.lock().log.as_ref().and_then(Log::commits)
    }

    /// Whether the lane journals its sessions.
    pub(crate) fn is_open(&self) -> bool {
        self.lock().log.is_some()
    }

    /// Whether its journal could not make a new segment, as on a full
    /// device.
    pub(crate) fn full(&self) -> bool {
        self.lock().log.as_ref().is_some_and(Log::full)
    }

    /// Sends what was appended, unless a write is in flight.
    fn flush(&self) {
        if let Some(log) = self.lock().log.as_mut()
            && log.flush().is_err()
        {
            self.write_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn poll_complete(&self, cx: &mut Context<'_>) -> Poll<Completion> {
        match self.lock().log.as_mut() {
            Some(log) => log.poll_complete(cx),
            None => Poll::Ready(Completion::Closed),
        }
    }

    /// The next completion of its writes, or of a cluster journal's commits.
    async fn next_completion(&self) -> Completion {
        let commits = self.lock().log.as_ref().and_then(Log::commits);
        match commits {
            Some(c) => c.next().await,
            None => std::future::poll_fn(|cx| self.poll_complete(cx)).await,
        }
    }

    /// Writes everything appended, blocking, then closes the journal,
    /// which lets go of its directory. Later changes fail.
    pub(crate) fn close(&self) {
        let log = self.take();
        if let Some(mut log) = log
            && log.sync().is_err()
        {
            self.write_errors.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// The number of the last record a cluster journal appended.
    pub(crate) fn cluster_appended(&self) -> Option<u64> {
        self.lock().log.as_ref()?.cluster_appended()
    }

    /// Drops the records a cluster journal did not hand over yet.
    pub(crate) fn discard(&self) {
        let mut st = self.lock();
        st.stale = false;
        if let Some(log) = st.log.as_mut() {
            log.discard();
        }
    }

    /// Closes the journal without waiting for what it has not written.
    pub(crate) fn release(&self) {
        drop(self.take());
    }

    /// Appends a record whose body `encode` writes, for lane `id`.
    /// `None` without a journal.
    fn append(
        &self,
        sessions: &Slab<Session>,
        kind: u8,
        encode: impl FnOnce(&mut Vec<u8>, Clock),
    ) -> Option<Journaled> {
        self.append_with(sessions, kind, encode, &[])
    }

    /// [`LaneLog::append`] with `tail` after what `encode` writes.
    fn append_with(
        &self,
        sessions: &Slab<Session>,
        kind: u8,
        encode: impl FnOnce(&mut Vec<u8>, Clock),
        tail: &[u8],
    ) -> Option<Journaled> {
        let mut st = self.lock();
        if st.closed {
            return Some(Journaled::Failed);
        }
        st.log.as_ref()?;
        if let Ok(n) = st.append(kind, sessions, encode, tail) {
            return Some(Journaled::At {
                wait: Wait {
                    journal: JournalId::Lane(self.lane),
                    n,
                },
                crowded: false,
            });
        }
        st.stale = true;
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        Some(Journaled::Failed)
    }

    fn take(&self) -> Option<Log> {
        let mut st = self.lock();
        let log = st.log.take();
        st.closed |= log.is_some();
        log
    }

    /// Makes the next write fail.
    #[cfg(test)]
    pub(crate) fn fail_next_write(&self) {
        if let Some(log) = self.lock().log.as_mut() {
            log.fail_next_write();
        }
    }

    /// Makes the next segment it asks for fail, as on a full device.
    #[cfg(test)]
    pub(crate) fn fail_next_segment(&self) {
        if let Some(log) = self.lock().log.as_mut() {
            log.fail_next_segment();
        }
    }

    /// Number of the last record appended, and groups written.
    #[cfg(test)]
    pub(crate) fn counts(&self) -> (u64, u64) {
        self.lock().log.as_ref().map_or((0, 0), Log::counts)
    }
}

impl LogState {
    /// Appends a record whose body is what `encode` writes, then `tail`,
    /// after all of `sessions` if the journal is stale.
    fn append(
        &mut self,
        kind: u8,
        sessions: &Slab<Session>,
        encode: impl FnOnce(&mut Vec<u8>, Clock),
        tail: &[u8],
    ) -> io::Result<u64> {
        if self.stale {
            self.write_all(sessions)?;
        }
        let LogState { log, buf, .. } = self;
        let log = log.as_mut().ok_or(io::ErrorKind::NotConnected)?;
        buf.clear();
        encode(buf, Clock::now());
        log.append(kind, &[buf, tail])
    }

    /// Journals a snapshot of every session the journal holds, so it
    /// holds what the lane holds. Returns the number of its last record.
    fn write_all(&mut self, sessions: &Slab<Session>) -> io::Result<u64> {
        let LogState { log, buf, .. } = self;
        let log = log.as_mut().ok_or(io::ErrorKind::NotConnected)?;
        let n = put_sessions(log, sessions, buf, Clock::now())?;
        self.stale = false;
        Ok(n)
    }

    /// Writes a snapshot from a new segment, if the journal grew enough
    /// past what it holds live, or with `force`, unless one waits for its
    /// trim: the segments before go once it is written. False if it waits
    /// for that segment.
    fn snapshot(&mut self, sessions: &Slab<Session>, live: u64, force: bool) -> io::Result<bool> {
        let Some(log) = self.log.as_mut().filter(|_| self.trim.is_none()) else {
            return Ok(true);
        };
        let since = log.written_bytes() - self.snapshot_at;
        if !force && !disk::snapshot_due(since, live) {
            return Ok(true);
        }
        let Some(segment) = log.roll()? else {
            return Ok(false);
        };
        let last = self.write_all(sessions)?;
        if let Some(log) = self.log.as_mut() {
            self.snapshot_at = log.written_bytes();
            log.flush()?;
        }
        self.trim = Some((segment, last));
        Ok(true)
    }

    /// The journal wrote up to record `n`: trims it behind a snapshot
    /// that is written now.
    fn written(&mut self, n: u64) -> io::Result<()> {
        let Some((segment, _)) = self.trim.filter(|t| t.1 <= n) else {
            return Ok(());
        };
        self.trim = None;
        self.log.as_mut().map_or(Ok(()), |log| log.trim(segment))
    }
}

impl Lane {
    fn log(&self) -> &LaneLog {
        &self.shared.lane_logs[usize::from(self.id)]
    }

    /// Whether the lane journals its sessions.
    pub(super) fn journals(&self) -> bool {
        self.log().is_open()
    }

    /// Journals record `kind` of session `key`: [`rec::SESSION`],
    /// [`rec::DOWN`], or one that names only the client. It goes out when
    /// the lane next flushes. `None` without a journal.
    pub(super) fn journal_session(&mut self, key: Key, kind: u8) -> Option<Journaled> {
        let id = usize::from(self.id);
        let s = self.sessions.get(key)?;
        let j = self.shared.lane_logs[id].append(&self.sessions, kind, |b, clock| match kind {
            rec::SESSION => rec::session(b, s, clock),
            rec::DOWN => rec::down(b, s, clock),
            _ => rec::client(b, &s.client_id),
        });
        self.appended(j);
        j
    }

    /// Journals the end of `s`, which left the lane's sessions.
    pub(super) fn journal_end(&mut self, s: &Session) {
        let id = usize::from(self.id);
        let j = self.shared.lane_logs[id].append(&self.sessions, rec::END, |b, _| {
            rec::client(b, &s.client_id);
        });
        self.appended(j);
    }

    /// Journals a subscription of session `key` to `filter` with `opts`,
    /// or its removal for `None`.
    pub(super) fn journal_sub(
        &mut self,
        key: Key,
        filter: &str,
        opts: Option<(SubscriptionOptions, u32)>,
    ) -> Option<Journaled> {
        let id = usize::from(self.id);
        let s = self.sessions.get(key).filter(|s| s.journaled)?;
        let log = &self.shared.lane_logs[id];
        let j = match opts {
            Some((o, sub_id)) => log.append(&self.sessions, rec::SUB, |b, _| {
                rec::sub(b, &s.client_id, filter, o, sub_id);
            }),
            None => log.append(&self.sessions, rec::UNSUB, |b, _| {
                rec::unsub(b, &s.client_id, filter);
            }),
        };
        self.appended(j);
        j
    }

    /// Journals a QoS 1/2 delivery of `p` to session `key` with grant
    /// `g`, after the message itself unless `owned` says it is there.
    /// Returns the session's number for it, 0 if it is not journaled.
    pub(super) fn journal_delivery(
        &mut self,
        key: Key,
        p: &Publication<'_>,
        owned: &mut Owned,
        g: (QoS, bool, SubIds),
    ) -> u64 {
        let id = usize::from(self.id);
        let Some(s) = self.sessions.get_mut(key).filter(|s| s.journaled) else {
            return 0;
        };
        s.flow.last_seq += 1;
        let seq = s.flow.last_seq;
        let log = &self.shared.lane_logs[id];
        if !owned.journaled {
            owned.journaled = true;
            let j = log.append_with(
                &self.sessions,
                rec::MSG,
                |b, c| rec::msg_head(b, p, c),
                p.payload,
            );
            self.needs.note(j);
            self.appended(j);
        }
        let log = &self.shared.lane_logs[id];
        let Some(s) = self.sessions.get(key) else {
            return 0;
        };
        let j = log.append(&self.sessions, rec::QUEUED, |b, _| {
            rec::queued(b, &s.client_id, seq, g);
        });
        self.needs.note(j);
        self.appended(j);
        seq
    }

    /// Journals what became of the journaled deliveries of session `key`
    /// since the last call. A released one holds its PUBREL until then.
    /// No ack waits for a DONE: it goes out with the next write, or with
    /// the next sweep.
    pub(super) fn journal_progress(&mut self, key: Key) {
        let id = usize::from(self.id);
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if s.flow.progress.is_empty() {
            return;
        }
        let mut progress = std::mem::take(&mut s.flow.progress);
        let log = &self.shared.lane_logs[id];
        let mut released = Vec::new();
        let mut last = None;
        if let Some(s) = self.sessions.get(key) {
            let cid = &s.client_id;
            for p in &progress {
                let (kind, seq) = match *p {
                    Progress::Done(seq) => (rec::DONE, seq),
                    Progress::Sent(seq, _) => (rec::SENT, seq),
                    Progress::Released(seq) => (rec::RELEASED, seq),
                };
                let j = log.append(&self.sessions, kind, |b, _| match *p {
                    Progress::Sent(_, pkid) => rec::sent(b, cid, seq, pkid),
                    _ => rec::delivery(b, cid, seq),
                });
                if kind != rec::DONE {
                    last = Some(j);
                }
                if kind == rec::RELEASED {
                    released.extend(j);
                }
            }
        }
        if let Some(j) = last {
            self.appended(j);
        }
        progress.clear();
        if let Some(s) = self.sessions.get_mut(key) {
            s.flow.progress = progress;
            if let Some(h) = s.inbound.hold.as_mut() {
                for j in released {
                    h.note(j);
                }
            }
        }
    }

    /// Journals inbound QoS 2 packet ID `pkid` of session `key` as waiting
    /// for PUBREL, or with `done`, as released.
    pub(super) fn journal_inbound(&mut self, key: Key, pkid: u16, done: bool) -> Option<Journaled> {
        let id = usize::from(self.id);
        let s = self.sessions.get(key).filter(|s| s.journaled)?;
        let kind = if done {
            rec::INBOUND_DONE
        } else {
            rec::INBOUND
        };
        let j = self.shared.lane_logs[id].append(&self.sessions, kind, |b, _| {
            rec::inbound(b, &s.client_id, pkid);
        });
        self.appended(j);
        j
    }

    /// Notes an append for the next flush and the pair cursors.
    fn appended(&mut self, j: Option<Journaled>) {
        match j {
            Some(Journaled::At { wait, .. }) => {
                self.journal_unflushed = true;
                self.pairs.appended = self.pairs.appended.max(wait.n);
            }
            Some(Journaled::Failed) => self.journal_unflushed = true,
            None => {}
        }
    }

    /// Sends the records appended since the last flush.
    pub(super) fn flush_journal(&mut self) {
        if std::mem::take(&mut self.journal_unflushed) {
            self.log().flush();
        }
    }

    /// Sends the records appended that no ack waited for, such as DONE.
    pub(super) fn sweep_journal(&self) {
        self.log().flush();
    }

    /// Writes a snapshot of the lane's sessions if the journal grew enough
    /// since the last, or with `force`, so the segments before it can go.
    /// False while it waits for a new segment.
    pub(crate) fn snapshot_journal(&mut self, force: bool) -> bool {
        let log = &self.shared.lane_logs[usize::from(self.id)];
        let live = self.budget.total.get() as u64;
        log.lock()
            .snapshot(&self.sessions, live, force)
            .unwrap_or_else(|_| {
                log.write_errors.fetch_add(1, Ordering::Relaxed);
                true
            })
    }

    /// A cluster leader's part of an image: the records so far go to the
    /// replica, and the lane's sessions after them become a group body.
    /// Returns the number of the last record the part holds.
    pub(crate) fn cut_image(&mut self) -> (u64, Bytes) {
        let log = &self.shared.lane_logs[usize::from(self.id)];
        let mut st = log.lock();
        let st = &mut *st;
        let last = match st.log.as_mut() {
            Some(Log::Cluster(c)) => {
                c.flush();
                c.appended()
            }
            _ => 0,
        };
        let mut part = GroupWriter::new(Journal::Lane(self.id));
        let _ = put_sessions(&mut part, &self.sessions, &mut st.buf, Clock::now());
        (last, part.finish())
    }

    /// Writes every journaled session again: after a failed write, or as
    /// the start of a forced cluster epoch's history.
    pub(super) fn rewrite_journal(&mut self) {
        let log = &self.shared.lane_logs[usize::from(self.id)];
        let mut st = log.lock();
        st.stale = true;
        let written = st.write_all(&self.sessions);
        let flushed = st.log.as_mut().map_or(Ok(()), Log::flush);
        if written.is_err() || flushed.is_err() {
            log.write_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Appends a snapshot of the journaled sessions to `out`. Returns the
/// number of its last record.
fn put_sessions(
    out: &mut impl Append,
    sessions: &Slab<Session>,
    buf: &mut Vec<u8>,
    clock: Clock,
) -> io::Result<u64> {
    out.append(CLEAR, &[])?;
    for key in sessions.keys() {
        if let Some(s) = sessions.get(key).filter(|s| s.journaled) {
            buf.clear();
            rec::image(buf, s, clock);
            out.append(IMAGE, &[buf])?;
        }
    }
    out.append(CLEARED, &[])
}

/// Moves the cursor of `lane`'s journal as its writes finish, until the
/// journal closes. The one task that takes its completions.
pub(crate) async fn complete(lane: LaneRef) {
    let (shared, id) = {
        let l = lane.borrow();
        (l.shared.clone(), usize::from(l.id))
    };
    let log = &shared.lane_logs[id];
    loop {
        match log.next_completion().await {
            Completion::Written(n) => {
                log.cursor.advance(n);
                if log.lock().written(n).is_err() {
                    log.write_errors.fetch_add(1, Ordering::Relaxed);
                }
                let mut l = lane.borrow_mut();
                l.pairs_written(0, n, false);
                // As soon as the journal grew enough, not at the next sweep.
                l.snapshot_journal(false);
            }
            Completion::Failed(first, last) => {
                let mut l = lane.borrow_mut();
                // A cluster journal fails with the leadership, and the lane
                // drops its state: nothing to write again.
                if !shared.clustered() {
                    log.write_errors.fetch_add(1, Ordering::Relaxed);
                    l.rewrite_journal();
                }
                log.cursor.fail(first, last);
                l.pairs_written(first, last, true);
            }
            Completion::Stalled => {
                log.write_errors.fetch_add(1, Ordering::Relaxed);
                // No new segment, as on a full device: a snapshot starts in
                // the spare kept back, and the trim behind it makes room.
                lane.borrow_mut().snapshot_journal(true);
                tokio::time::sleep(RETRY).await;
                log.flush();
            }
            // Whoever closed the journal closed the cursors.
            Completion::Closed => return,
        }
    }
}
