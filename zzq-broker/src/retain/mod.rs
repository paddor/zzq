//! Retained messages, in stripes by topic, each behind a lock of its own.
//!
//! With a data directory, each stripe journals its changes in order under
//! its lock. A change goes out with the stripe's next write, and the acks
//! of the publishes behind it wait for that write. A failed write makes
//! the stripe write all its messages again, so the journal holds what the
//! broker holds.

use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::task::Poll;
use std::time::Instant;

use crate::disk::{self, Completion, DataDir, Log, Record};
use crate::durable::{Cursor, JournalId, Journaled, RETRY, Wait};
use crate::msg::{Msg, expired, too_deep};
use crate::state::{self, Clock};

mod tree;

use tree::Tree;

/// Stripes of the store. Each topic has its stripe, and with it its
/// journal, for good: changing this needs a migration.
pub(crate) const STRIPES: usize = 8;
/// Bytes of a stripe's records waiting for the disk beyond which QoS 0
/// publishers of retained messages wait for it too.
const CROWDED: usize = 8 << 20;

/// Record kinds of the stripe journals.
const SET: u8 = 1;
const REMOVE: u8 = 2;
/// Starts a snapshot: the messages after it are all the stripe holds.
const CLEAR: u8 = 3;
/// A message of a snapshot.
const KEPT: u8 = 4;
/// Ends a snapshot, which takes the place of the messages before it.
const CLEARED: u8 = 5;

/// Retained messages of every stripe.
#[derive(Debug)]
pub(crate) struct Retained {
    stripes: Box<[Stripe]>,
    /// Topics stored; new ones beyond it are refused.
    max: usize,
    /// Topics stored in all stripes.
    len: AtomicUsize,
    /// Retained messages refused because `max` or the state budget was
    /// reached.
    rejected: AtomicU64,
    /// Journal writes that failed.
    write_errors: AtomicU64,
}

#[derive(Debug, Default)]
struct Stripe {
    state: Mutex<State>,
    /// How far the stripe's journal is written.
    cursor: Cursor,
}

#[derive(Debug, Default)]
struct State {
    tree: Tree,
    log: Option<Log>,
    /// The journal may hold other messages than the tree, since a write
    /// failed: the next change writes them all.
    stale: bool,
    /// The journal was closed: changes fail.
    closed: bool,
    /// Reused for the heads of records.
    head: Vec<u8>,
    /// Bytes the journal had appended at the last snapshot.
    snapshot_at: u64,
    /// The segment the last snapshot started in, and its last record:
    /// once that is written, the segments before go.
    trim: Option<(u64, u64)>,
}

/// The stripe of `topic`: an FNV-1a hash, stable across builds, since a
/// stripe's journal holds its topics.
fn stripe_of(topic: &str) -> usize {
    let hash = topic.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x0100_0000_01b3)
    });
    (hash % STRIPES as u64) as usize
}

impl Retained {
    /// A store in memory only.
    pub(crate) fn new(max: usize) -> Retained {
        Retained {
            stripes: (0..STRIPES).map(|_| Stripe::default()).collect(),
            max,
            len: AtomicUsize::new(0),
            rejected: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }

    /// A store with a journal per stripe in `dir`, holding the messages
    /// they kept, except those deeper than `max_levels`.
    pub(crate) fn open(dir: &DataDir, max: usize, max_levels: usize) -> io::Result<Retained> {
        let r = Retained::new(max);
        let replay = Replay::new(max_levels);
        for (i, stripe) in r.stripes.iter().enumerate() {
            let mut st = stripe.lock();
            let tree = &mut st.tree;
            let mut snapshot = None;
            let log = dir.journal(&format!("retained-{i}"), |rec| {
                replay.apply(tree, &mut snapshot, i, rec)
            })?;
            st.log = Some(log);
            r.len.fetch_add(st.tree.len(), Ordering::Relaxed);
        }
        Ok(r)
    }

    /// Stores a compact copy of `msg`, or clears the topic for an empty
    /// payload, and journals the change. While the state budget is
    /// `full`, only topics already stored change. `None` if nothing
    /// changed, or nothing went to a journal.
    pub(crate) fn set(&self, msg: &Msg, full: bool) -> Option<Journaled> {
        let topic = msg.topic();
        let i = stripe_of(topic);
        let mut st = self.stripes[i].lock();
        if msg.payload().is_empty() {
            if !st.tree.remove(topic) {
                return None;
            }
            self.len.fetch_sub(1, Ordering::Relaxed);
        } else {
            if st.tree.stored(topic).is_none() && (full || !self.reserve()) {
                self.rejected.fetch_add(1, Ordering::Relaxed);
                return None;
            }
            st.tree.insert(msg.compact());
        }
        // The broker writes `$SYS/` itself on every start.
        if topic.starts_with("$SYS/") {
            return None;
        }
        if st.closed {
            return Some(Journaled::Failed);
        }
        st.log.as_ref()?;
        if let Ok((n, backlog)) = st.journal(msg) {
            return Some(Journaled::At {
                wait: Wait {
                    journal: JournalId::Retained(i),
                    n,
                },
                crowded: backlog > CROWDED,
            });
        }
        st.stale = true;
        self.write_errors.fetch_add(1, Ordering::Relaxed);
        Some(Journaled::Failed)
    }

    /// Counts a new topic, unless `max` are stored.
    fn reserve(&self) -> bool {
        // `try_update`, the new name, needs Rust 1.99.
        #[allow(deprecated)]
        // Relaxed: a count; the stripe locks order the topics themselves.
        self.len
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n < self.max).then_some(n + 1)
            })
            .is_ok()
    }

    /// Retained messages matching `filter`.
    pub(crate) fn matching(&self, filter: &str) -> Vec<Msg> {
        let mut out = Vec::new();
        if filter.contains(['+', '#']) {
            for s in &self.stripes {
                s.lock().tree.matching(filter, &mut out);
            }
        } else {
            self.stripes[stripe_of(filter)]
                .lock()
                .tree
                .matching(filter, &mut out);
        }
        out
    }

    /// Topics stored.
    pub(crate) fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Retained messages refused because `max` or the state budget was
    /// reached.
    pub(crate) fn rejected(&self) -> u64 {
        self.rejected.load(Ordering::Relaxed)
    }

    /// Journal writes that failed.
    pub(crate) fn write_errors(&self) -> u64 {
        self.write_errors.load(Ordering::Relaxed)
    }

    /// Journals that could not make a new segment, as on a full device.
    pub(crate) fn full_journals(&self) -> u64 {
        let full = |s: &Stripe| s.lock().log.as_ref().is_some_and(Log::full);
        self.stripes.iter().filter(|s| full(s)).count() as u64
    }

    /// Every stored message, except the `$SYS/` ones the broker writes
    /// itself.
    pub(crate) fn messages(&self) -> Vec<Msg> {
        let mut out = Vec::new();
        for s in &self.stripes {
            out.extend(s.lock().tree.messages());
        }
        out
    }

    /// Bytes counted against `max_state_bytes`.
    pub(crate) fn bytes(&self) -> usize {
        self.stripes.iter().map(|s| s.lock().tree.bytes()).sum()
    }

    /// Drops messages whose Message Expiry Interval ran out, and released
    /// ones no session holds anymore. A replay skips the expired ones, so
    /// their journals keep them.
    pub(crate) fn sweep(&self, now: Instant) {
        for s in &self.stripes {
            let cleared = s.lock().tree.sweep(now);
            self.len.fetch_sub(cleared, Ordering::Relaxed);
        }
    }

    /// Writes a snapshot of each stripe whose journal grew enough since
    /// the last, or of all with `force`, so the segments before it can go.
    /// False while one waits for a new segment.
    pub(crate) fn snapshot(&self, force: bool) -> bool {
        let mut done = true;
        for s in &self.stripes {
            done &= self.snapshot_of(s, force);
        }
        done
    }

    /// Writes a snapshot of stripe `i`, as [`Retained::snapshot`] with
    /// `force` does for all, without making first segments for the others.
    #[cfg(test)]
    pub(crate) fn snapshot_stripe(&self, i: usize) -> bool {
        self.snapshot_of(&self.stripes[i], true)
    }

    fn snapshot_of(&self, s: &Stripe, force: bool) -> bool {
        s.lock().snapshot(force).unwrap_or_else(|_| {
            self.write_errors.fetch_add(1, Ordering::Relaxed);
            true
        })
    }

    /// The cursor of the journal of stripe `stripe`.
    pub(crate) fn cursor(&self, stripe: usize) -> &Cursor {
        &self.stripes[stripe].cursor
    }

    /// The cursor of the journal that holds `topic`.
    #[cfg(test)]
    pub(crate) fn cursor_of(&self, topic: &str) -> &Cursor {
        self.cursor(stripe_of(topic))
    }

    /// Moves the cursor of stripe `i` as its writes finish, until its
    /// journal stops. The one task that takes the stripe's completions.
    pub(crate) async fn complete(&self, i: usize) {
        let stripe = &self.stripes[i];
        loop {
            let done = std::future::poll_fn(|cx| match stripe.lock().log.as_mut() {
                Some(log) => log.poll_complete(cx),
                None => Poll::Ready(Completion::Closed),
            })
            .await;
            match done {
                Completion::Written(n) => {
                    stripe.cursor.advance(n);
                    let mut st = stripe.lock();
                    let trimmed = st.written(n);
                    // As soon as the journal grew enough, not at the next
                    // sweep.
                    if trimmed.is_err() || st.snapshot(false).is_err() {
                        self.write_errors.fetch_add(1, Ordering::Relaxed);
                    }
                }
                Completion::Failed(first, last) => {
                    self.write_errors.fetch_add(1, Ordering::Relaxed);
                    let mut st = stripe.lock();
                    st.stale = true;
                    if st.write_all().is_err() {
                        self.write_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(st);
                    stripe.cursor.fail(first, last);
                }
                Completion::Stalled => {
                    self.write_errors.fetch_add(1, Ordering::Relaxed);
                    // No new segment, as on a full device: a snapshot starts
                    // in the spare kept back, and the trim behind it makes
                    // room.
                    if stripe.lock().snapshot(true).is_err() {
                        self.write_errors.fetch_add(1, Ordering::Relaxed);
                    }
                    tokio::time::sleep(RETRY).await;
                    if let Some(log) = stripe.lock().log.as_mut() {
                        let _ = log.flush();
                    }
                }
                // Whoever closed the journal closed the cursor.
                Completion::Closed => return,
            }
        }
    }

    /// Writes every journal, blocking, then closes it, which lets go of
    /// its directory. Later changes fail.
    pub(crate) fn close(&self) {
        for mut log in self.take_logs() {
            if log.sync().is_err() {
                self.write_errors.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Closes every journal without waiting for what it has not written.
    pub(crate) fn release(&self) {
        drop(self.take_logs());
    }

    /// The journals, taken out, with their cursors closed: what they did
    /// not write by now they never will.
    fn take_logs(&self) -> Vec<Log> {
        let mut logs = Vec::new();
        for s in &self.stripes {
            let mut st = s.lock();
            if let Some(log) = st.log.take() {
                st.closed = true;
                logs.push(log);
            }
            drop(st);
            s.cursor.close();
        }
        logs
    }

    /// Makes the next write of stripe `i` fail.
    #[cfg(test)]
    pub(crate) fn fail_next_write(&self, i: usize) {
        if let Some(log) = self.stripes[i].lock().log.as_mut() {
            log.fail_next_write();
        }
    }

    /// Makes the next segment stripe `i` asks for fail, as on a full
    /// device.
    #[cfg(test)]
    pub(crate) fn fail_next_segment(&self, i: usize) {
        if let Some(log) = self.stripes[i].lock().log.as_mut() {
            log.fail_next_segment();
        }
    }

    /// The stripe of `topic`.
    #[cfg(test)]
    pub(crate) fn stripe_of(topic: &str) -> usize {
        stripe_of(topic)
    }
}

impl Stripe {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

impl State {
    /// Journals the change `msg` made, or the whole stripe while stale.
    /// Returns the number of the last record and the bytes waiting for the
    /// disk.
    fn journal(&mut self, msg: &Msg) -> io::Result<(u64, usize)> {
        if self.stale {
            return self.write_all();
        }
        let State { log, head, .. } = self;
        let Some(log) = log else {
            return Err(io::ErrorKind::NotConnected.into());
        };
        let n = if msg.payload().is_empty() {
            log.append(REMOVE, &[msg.topic().as_bytes()])?
        } else {
            head.clear();
            state::msg_head(head, msg, Clock::now());
            log.append(SET, &[head, msg.payload()])?
        };
        log.flush()?;
        Ok((n, log.backlog()))
    }

    /// Journals a snapshot of every message, so the journal holds what
    /// the tree holds.
    fn write_all(&mut self) -> io::Result<(u64, usize)> {
        let State {
            tree, log, head, ..
        } = self;
        let Some(log) = log else {
            return Err(io::ErrorKind::NotConnected.into());
        };
        let clock = Clock::now();
        log.append(CLEAR, &[])?;
        for m in tree.messages() {
            head.clear();
            state::msg_head(head, &m, clock);
            log.append(KEPT, &[head, m.payload()])?;
        }
        let n = log.append(CLEARED, &[])?;
        log.flush()?;
        self.stale = false;
        Ok((n, log.backlog()))
    }

    /// Writes a snapshot from a new segment, if the journal grew enough
    /// past what the tree holds, or with `force`, unless one waits for its
    /// trim: the segments before go once it is written. False if it waits
    /// for that segment.
    fn snapshot(&mut self, force: bool) -> io::Result<bool> {
        let Some(log) = self.log.as_mut().filter(|_| self.trim.is_none()) else {
            return Ok(true);
        };
        let since = log.written_bytes() - self.snapshot_at;
        if !force && !disk::snapshot_due(since, self.tree.bytes() as u64) {
            return Ok(true);
        }
        let Some(segment) = log.roll()? else {
            return Ok(false);
        };
        let (last, _) = self.write_all()?;
        if let Some(log) = self.log.as_ref() {
            self.snapshot_at = log.written_bytes();
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

/// Applies journal records to the trees of their stripes.
#[derive(Debug, Clone, Copy)]
struct Replay {
    clock: Clock,
    now: Instant,
    /// Deeper topics are left out.
    max_levels: usize,
}

impl Replay {
    fn new(max_levels: usize) -> Replay {
        Replay {
            clock: Clock::now(),
            now: Instant::now(),
            max_levels,
        }
    }

    /// Applies record `r` of the journal of stripe `stripe` to `tree`,
    /// or to `snapshot` while one is being read.
    fn apply(
        self,
        tree: &mut Tree,
        snapshot: &mut Option<Tree>,
        stripe: usize,
        r: Record<'_>,
    ) -> Result<(), &'static str> {
        match r.kind {
            CLEAR if r.body.is_empty() => *snapshot = Some(Tree::default()),
            CLEARED if r.body.is_empty() => {
                if let Some(s) = snapshot.take() {
                    *tree = s;
                }
            }
            KEPT => {
                let into = snapshot.as_mut().unwrap_or(tree);
                return self.apply(
                    into,
                    &mut None,
                    stripe,
                    Record {
                        kind: SET,
                        body: r.body,
                    },
                );
            }
            // Any other record after an unfinished snapshot: the snapshot
            // was cut off, and the stripe went on without it.
            _ if snapshot.take().is_some() => return self.apply(tree, snapshot, stripe, r),
            SET => {
                let m = state::decode_msg(r.body, self.clock)?;
                if stripe_of(m.topic()) != stripe {
                    return Err("retained message in the journal of another stripe");
                }
                if m.payload().is_empty() {
                    return Err("empty retained message");
                }
                // Deeper than clients may publish now: the tree walks topics
                // a level at a time.
                if too_deep(m.topic(), self.max_levels) || expired(m.expires_at, self.now) {
                    tree.remove(m.topic());
                } else {
                    tree.insert(m);
                }
            }
            REMOVE => {
                let topic = std::str::from_utf8(r.body).map_err(|_| "invalid UTF-8")?;
                tree.remove(topic);
            }
            _ => return Err("unknown retained record"),
        }
        Ok(())
    }
}

/// Replays `data` as one record of a stripe journal: a kind, then the
/// body. Returns the topics the stripe holds after it.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn replay_one(data: &[u8]) -> Result<usize, &'static str> {
    let Some((&kind, body)) = data.split_first() else {
        return Ok(0);
    };
    let mut tree = Tree::default();
    let replay = Replay::new(usize::MAX);
    let stripe = state::decode_msg(body, replay.clock).map_or(0, |m| stripe_of(m.topic()));
    replay.apply(&mut tree, &mut None, stripe, Record { kind, body })?;
    Ok(tree.len())
}

/// A record of each kind, kind byte first.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn record_seeds() -> Vec<Vec<u8>> {
    let mut m = Msg::new("a/b", b"v", &[1, 1], zzq_proto::QoS::AtLeastOnce, true);
    m.expires_at = Some(Instant::now() + std::time::Duration::from_secs(3600));
    let mut set = vec![SET];
    state::msg_head(&mut set, &m, Clock::now());
    set.extend_from_slice(m.payload());
    let mut kept = set.clone();
    kept[0] = KEPT;
    vec![
        set,
        [&[REMOVE][..], b"a/b"].concat(),
        vec![CLEAR],
        kept,
        vec![CLEARED],
    ]
}

#[cfg(test)]
mod tests;
