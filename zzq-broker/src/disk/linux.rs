//! The data directory: journals written with Linux AIO.

use std::io;
use std::path::{Path, PathBuf};
use std::task::{Context, Poll};

pub(crate) use zzq_disk::Record;

use super::Completion;

/// Bytes per segment file, unless a packet needs more.
const SEGMENT: u64 = 64 << 20;
/// Room in a segment beyond a packet: framing and alignment.
const SEGMENT_SLACK: u64 = 1 << 20;

/// The disk threads of a data directory.
#[derive(Debug)]
pub(crate) struct DataDir {
    disk: zzq_disk::Disk,
    path: PathBuf,
}

impl DataDir {
    /// Starts the disk threads for `path`, with segments that take a
    /// packet of `max_packet` bytes.
    pub(crate) fn open(path: &Path, max_packet: usize) -> io::Result<DataDir> {
        let packet = u64::try_from(max_packet).unwrap_or(u64::MAX);
        let mut config = zzq_disk::Config::default();
        config.segment_bytes = SEGMENT.max(
            packet
                .saturating_add(SEGMENT_SLACK)
                .next_multiple_of(SEGMENT_SLACK),
        );
        Ok(DataDir {
            disk: zzq_disk::Disk::new(config).map_err(to_io)?,
            path: path.to_owned(),
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Opens journal `name`, handing the records it holds to `replay`.
    pub(crate) fn journal(
        &self,
        name: &str,
        mut replay: impl FnMut(Record<'_>) -> Result<(), &'static str>,
    ) -> io::Result<Log> {
        let journal = self
            .disk
            .open(self.path.join(name), |r| replay(r).map_err(Into::into))
            .map_err(to_io)?;
        Ok(Log(journal))
    }
}

/// A journal: records appended in order and written in groups.
#[derive(Debug)]
pub(crate) struct Log(zzq_disk::Journal);

impl Log {
    /// Appends a record whose body is `parts` joined, and returns its
    /// number.
    pub(crate) fn append(&mut self, kind: u8, parts: &[&[u8]]) -> io::Result<u64> {
        self.0.append(kind, parts).map_err(to_io)
    }

    /// Sends what was appended, unless a write is in flight.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.0.flush().map_err(to_io)
    }

    /// Bytes appended and not yet written.
    pub(crate) const fn backlog(&self) -> usize {
        self.0.backlog()
    }

    /// Bytes appended since the journal opened.
    pub(crate) const fn appended_bytes(&self) -> u64 {
        self.0.appended_bytes()
    }

    /// Starts the next write in a new segment, and returns its id; `None`
    /// until that segment is made.
    pub(crate) fn roll(&mut self) -> io::Result<Option<u64>> {
        self.0.roll().map_err(to_io)
    }

    /// Makes the next write fail.
    #[cfg(test)]
    pub(crate) const fn fail_next_write(&mut self) {
        self.0.fail_next_write();
    }

    /// Number of the last record appended, and groups written.
    #[cfg(test)]
    pub(crate) const fn counts(&self) -> (u64, u64) {
        (self.0.appended(), self.0.groups())
    }

    /// Removes the segments before `before`.
    pub(crate) fn trim(&mut self, before: u64) -> io::Result<()> {
        self.0.trim(before).map_err(to_io)
    }

    /// Writes everything appended, blocking.
    pub(crate) fn sync(&mut self) -> io::Result<u64> {
        self.0.sync().map_err(to_io)
    }

    /// The next thing that finished.
    pub(crate) fn poll_complete(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        self.0.poll_complete(cx).map(|r| match r {
            Ok(n) => Completion::Written(n),
            Err(zzq_disk::Error::Write { first, last, .. }) => Completion::Failed(first, last),
            Err(zzq_disk::Error::Closed) => Completion::Closed,
            Err(_) => Completion::Stalled,
        })
    }
}

fn to_io(e: zzq_disk::Error) -> io::Error {
    match e {
        zzq_disk::Error::Io(e) => e,
        e => io::Error::other(e),
    }
}
