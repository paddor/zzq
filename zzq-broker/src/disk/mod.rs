//! Journals in the data directory, for state that survives a crash:
//! retained messages in `retained-<i>`, sessions in `lanes-<n>/lane-<j>`.

use std::fs;
use std::io;
use std::path::Path;

use crate::broker::lane_for;
use crate::state::journal::{self as rec, IMAGE, Sessions};
use crate::state::{Clock, SessionState};

#[cfg_attr(target_os = "linux", path = "linux.rs")]
#[cfg_attr(not(target_os = "linux"), path = "unsupported.rs")]
mod imp;

use imp::DiskLog;
pub(crate) use imp::{DataDir, Record};

use std::sync::Arc;
use std::task::{Context, Poll};

use crate::cluster::{ClusterLog, Commits};

/// Where a snapshot's records go: a journal, or a cluster image.
pub(crate) trait Append {
    /// Appends a record of `kind` whose body is `parts`. Returns its
    /// number.
    fn append(&mut self, kind: u8, parts: &[&[u8]]) -> io::Result<u64>;
}

impl Append for Log {
    fn append(&mut self, kind: u8, parts: &[&[u8]]) -> io::Result<u64> {
        Log::append(self, kind, parts)
    }
}

/// A journal: on disk, or replicated to the other nodes of a cluster.
#[derive(Debug)]
pub(crate) enum Log {
    Disk(Box<DiskLog>),
    Cluster(ClusterLog),
}

impl Log {
    pub(crate) fn append(&mut self, kind: u8, parts: &[&[u8]]) -> io::Result<u64> {
        match self {
            Log::Disk(d) => d.append(kind, parts),
            Log::Cluster(c) => Ok(c.append(kind, parts)),
        }
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        match self {
            Log::Disk(d) => d.flush(),
            Log::Cluster(c) => {
                c.flush();
                Ok(())
            }
        }
    }

    pub(crate) const fn backlog(&self) -> usize {
        match self {
            Log::Disk(d) => d.backlog(),
            Log::Cluster(c) => c.backlog(),
        }
    }

    /// Whether a new segment could not be made; never for a cluster.
    pub(crate) const fn full(&self) -> bool {
        match self {
            Log::Disk(d) => d.full(),
            Log::Cluster(_) => false,
        }
    }

    /// Bytes written: what makes a snapshot due. A cluster journal holds
    /// no history to trim, so it never takes one.
    pub(crate) const fn written_bytes(&self) -> u64 {
        match self {
            Log::Disk(d) => d.written_bytes(),
            Log::Cluster(_) => 0,
        }
    }

    pub(crate) fn roll(&mut self) -> io::Result<Option<u64>> {
        match self {
            Log::Disk(d) => d.roll(),
            Log::Cluster(_) => Ok(None),
        }
    }

    pub(crate) fn trim(&mut self, before: u64) -> io::Result<()> {
        match self {
            Log::Disk(d) => d.trim(before),
            Log::Cluster(_) => Ok(()),
        }
    }

    /// Writes everything appended and waits for it. A cluster journal
    /// hands it to the replica and does not wait.
    pub(crate) fn sync(&mut self) -> io::Result<u64> {
        match self {
            Log::Disk(d) => d.sync(),
            Log::Cluster(c) => {
                c.flush();
                Ok(c.appended())
            }
        }
    }

    /// The number of the last record a cluster journal appended.
    pub(crate) const fn cluster_appended(&self) -> Option<u64> {
        match self {
            Log::Disk(_) => None,
            Log::Cluster(c) => Some(c.appended()),
        }
    }

    /// Drops what a cluster journal holds back; nothing for the disk.
    pub(crate) fn discard(&mut self) {
        if let Log::Cluster(c) = self {
            c.discard();
        }
    }

    /// The commits of a cluster journal, which its completion task awaits
    /// instead of polling.
    pub(crate) fn commits(&self) -> Option<Arc<Commits>> {
        match self {
            Log::Disk(_) => None,
            Log::Cluster(c) => Some(c.commits.clone()),
        }
    }

    pub(crate) fn poll_complete(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        match self {
            Log::Disk(d) => d.poll_complete(cx),
            // Its completion task awaits `commits`.
            Log::Cluster(_) => Poll::Pending,
        }
    }

    #[cfg(test)]
    pub(crate) const fn fail_next_write(&mut self) {
        if let Log::Disk(d) = self {
            d.fail_next_write();
        }
    }

    #[cfg(test)]
    pub(crate) const fn fail_next_segment(&mut self) {
        if let Log::Disk(d) = self {
            d.fail_next_segment();
        }
    }

    #[cfg(test)]
    pub(crate) const fn counts(&self) -> (u64, u64) {
        match self {
            Log::Disk(d) => d.counts(),
            Log::Cluster(c) => (c.appended(), 0),
        }
    }
}

/// Journal bytes written since the last snapshot beyond which the next
/// one comes, once they are also twice what the journal holds live.
pub(crate) const SNAPSHOT_BYTES: u64 = 64 << 20;

/// Whether a journal with `since` bytes written since its last snapshot,
/// for `live` bytes of state, writes the next one.
pub(crate) const fn snapshot_due(since: u64, live: u64) -> bool {
    since >= SNAPSHOT_BYTES && since / 2 >= live
}

/// What finished in a journal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Completion {
    /// Records up to this one are written.
    Written(u64),
    /// Writing records `.0..=.1` failed. They may be on disk or not.
    Failed(u64, u64),
    /// A segment file could not be made, as on a full device. Writes wait
    /// for the next try; a snapshot starts in the spare kept back.
    Stalled,
    /// The disk threads stopped.
    Closed,
}

/// Opens the session journals of `n` lanes, `lanes-<n>/lane-<j>`, with
/// the sessions each holds. Sessions in the journals of another lane
/// count move to the lanes `lane_for` gives them first.
pub(crate) fn open_lanes(dir: &DataDir, n: usize) -> io::Result<Vec<(Log, Vec<SessionState>)>> {
    let root = dir.path();
    let want = format!("lanes-{n}");
    let mut others = Vec::new();
    if root.is_dir() {
        for entry in fs::read_dir(root)? {
            let name = entry?.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let Some(count) = name.strip_prefix("lanes-") else {
                continue;
            };
            if count.strip_suffix(".tmp").is_some() {
                // A move that did not finish.
                fs::remove_dir_all(root.join(name))?;
            } else if let Ok(m) = count.parse()
                && m != n
            {
                others.push(m);
            }
        }
    }
    if !root.join(&want).is_dir() {
        match others[..] {
            [] => {}
            [m] => move_sessions(dir, m, n)?,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "session journals of several lane counts",
                ));
            }
        }
    }
    // Left from a move that finished.
    for m in others {
        fs::remove_dir_all(root.join(format!("lanes-{m}")))?;
    }
    (0..n)
        .map(|j| read_lane(dir, &format!("{want}/lane-{j}"), j, n))
        .collect()
}

/// Opens journal `name` of lane `j` of `n`, with the sessions it holds.
fn read_lane(
    dir: &DataDir,
    name: &str,
    j: usize,
    n: usize,
) -> io::Result<(Log, Vec<SessionState>)> {
    let clock = Clock::now();
    let mut sessions = Sessions::default();
    let log = dir.journal(name, |r| {
        sessions.apply(r.kind, r.body, clock, |id| lane_for(id, n) == j)
    })?;
    Ok((log, sessions.finish(crate::time::now())))
}

/// Moves the sessions of `m` lane journals to `n`: writes them all to new
/// journals beside, then swaps the directories.
fn move_sessions(dir: &DataDir, m: usize, n: usize) -> io::Result<()> {
    let mut moved: Vec<Vec<SessionState>> = (0..n).map(|_| Vec::new()).collect();
    for j in 0..m {
        let (log, sessions) = read_lane(dir, &format!("lanes-{m}/lane-{j}"), j, m)?;
        drop(log);
        for st in sessions {
            moved[lane_for(&st.client_id, n)].push(st);
        }
    }
    let clock = Clock::now();
    let mut buf = Vec::new();
    for (j, sessions) in moved.into_iter().enumerate() {
        let mut log = dir.journal(&format!("lanes-{n}.tmp/lane-{j}"), |_| Ok(()))?;
        for st in sessions {
            buf.clear();
            rec::image_state(&mut buf, st, false, clock);
            log.append(IMAGE, &[&buf])?;
        }
        log.sync()?;
    }
    let root = dir.path();
    fs::rename(
        root.join(format!("lanes-{n}.tmp")),
        root.join(format!("lanes-{n}")),
    )?;
    sync_dir(root)
}

/// Makes a rename in `dir` durable.
fn sync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir)?.sync_all()
}
