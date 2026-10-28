//! A journal: one owner's records, written in groups.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};

use fanring::mpsc::{self, Receiver, Sender, TrySendError};

use crate::aio::AlignedBuf;
use crate::backend::{AioJob, Done, FileJob, QUEUE, Write, locked};
use crate::format::{self, GROUP_HEADER, PAGE, RECORD_HEADER, ScanError};
use crate::segment::{self, Segment};
use crate::{Config, Disk, Error, ReplayError};

/// A record read back when a journal opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Record<'a> {
    /// What the record is, as its writer numbered it.
    pub kind: u8,
    /// What it holds.
    pub body: &'a [u8],
}

/// Records of the journal's owner, written to its directory in groups.
///
/// Each record gets the next number, from 1. [`Journal::flush`] sends the
/// records appended so far as one write, unless a write is in flight: then
/// they wait behind it, and its completion sends them. Records appended
/// after the last flush go along with such a write, or wait for the next
/// flush. A completed write is on disk, and [`Journal::durable`] moves to
/// its last record.
pub struct Journal {
    id: u64,
    dir: Arc<Path>,
    config: Config,
    links: Links,
    tail: Tail,
    pending: Pending,
    in_flight: Option<InFlight>,
    appended: u64,
    /// The last record a flush asked for.
    flushed: u64,
    durable: u64,
    /// Bytes of the groups written, padding included.
    written_bytes: u64,
    stopped: bool,
    /// Held while open: one journal per directory.
    _lock: File,
}

/// The journal's ends of the channels to the disk threads.
struct Links {
    aio: Sender<AioJob>,
    files: Sender<FileJob>,
    /// Not sent on; registers a sender for each segment request.
    done_tx: Sender<Done>,
    done: Receiver<Done>,
}

/// Where the next group goes.
struct Tail {
    /// `None` until the first write: a journal never written holds no
    /// segment.
    active: Option<Segment>,
    offset: u64,
    /// The segment after `active`, made ahead of need.
    next: Option<Segment>,
    creating: bool,
    /// The last segment asked for could not be made, as on a full device.
    full: bool,
    /// Groups of the records after this one start a new segment.
    roll_after: Option<u64>,
    #[cfg(feature = "testing")]
    faults: Faults,
    /// Number of the next group.
    group: u64,
}

/// Failures a test asked for.
#[cfg(feature = "testing")]
#[derive(Default)]
struct Faults {
    /// The next write goes to a place the kernel refuses.
    write: bool,
    /// The next segment asked for fails as on a full device.
    segment: bool,
}

/// Records not yet written.
#[derive(Default)]
struct Pending {
    /// Takes new records.
    open: Group,
    /// Full groups, oldest first.
    queued: VecDeque<Group>,
    /// Buffers back from writes.
    free: Vec<AlignedBuf>,
    /// Framed bytes of the records appended but not yet written.
    bytes: usize,
}

#[derive(Default)]
struct Group {
    buf: AlignedBuf,
    records: u32,
    /// Number of the last record.
    last: u64,
    /// Framed bytes of the records.
    bytes: usize,
}

#[derive(Debug, Clone, Copy)]
struct InFlight {
    first: u64,
    last: u64,
    length: u64,
    bytes: usize,
}

impl std::fmt::Debug for Journal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Journal")
            .field("dir", &self.dir)
            .field("appended", &self.appended)
            .field("durable", &self.durable)
            .finish_non_exhaustive()
    }
}

impl Journal {
    /// Appends a record whose body is `parts` joined, and returns its
    /// number. It goes out with the next [`Journal::flush`].
    ///
    /// # Errors
    ///
    /// [`Error::TooLarge`] if it does not fit a segment, [`Error::Closed`]
    /// once the disk threads stopped, [`Error::Io`] if a buffer cannot grow.
    pub fn append(&mut self, kind: u8, parts: &[&[u8]]) -> Result<u64, Error> {
        if self.stopped {
            return Err(Error::Closed);
        }
        let len = parts.iter().map(|p| p.len()).sum::<usize>();
        let framed = RECORD_HEADER + len;
        if format::group_len(GROUP_HEADER + framed) as u64 > self.config.segment_bytes - PAGE as u64
        {
            return Err(Error::TooLarge { len });
        }
        let p = &mut self.pending;
        if p.open.buf.len() + framed > self.config.group_bytes {
            p.seal();
        }
        let open = &mut p.open;
        if open.buf.is_empty() {
            format::begin(&mut open.buf)?;
        }
        format::record(&mut open.buf, kind, parts)?;
        self.appended += 1;
        open.records += 1;
        open.last = self.appended;
        open.bytes += framed;
        p.bytes += framed;
        Ok(self.appended)
    }

    /// Sends the records appended so far, unless a write is in flight:
    /// then its completion sends them.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the disk threads stopped.
    pub fn flush(&mut self) -> Result<(), Error> {
        if self.stopped {
            return Err(Error::Closed);
        }
        self.flushed = self.appended;
        self.send()
    }

    /// Sends the oldest group waiting, unless a write is in flight or no
    /// flush asked for its records.
    fn send(&mut self) -> Result<(), Error> {
        if self.in_flight.is_some() || self.durable >= self.flushed {
            return Ok(());
        }
        let Some(mut group) = self.pending.take() else {
            return Ok(());
        };
        let length = format::group_len(group.buf.len()) as u64;
        let t = &mut self.tail;
        let first = group.last + 1 - u64::from(group.records);
        let rolls = t.roll_after.is_some_and(|n| first > n);
        let fits = |a: &&Segment| !rolls && t.offset + length <= a.capacity;
        let active = if let Some(a) = t.active.as_ref().filter(fits) {
            a
        } else {
            let Some(next) = t.next.take() else {
                self.pending.queued.push_front(group);
                return self.create_next();
            };
            t.offset = PAGE as u64;
            t.roll_after = None;
            t.active.insert(next)
        };
        format::seal(&mut group.buf, active.id, t.group, group.records)?;
        // One byte past its place, which the kernel refuses for direct IO,
        // when a test asked for a failure.
        #[cfg(feature = "testing")]
        let skew = u64::from(std::mem::take(&mut t.faults.write));
        #[cfg(not(feature = "testing"))]
        let skew = 0;
        let write = Write {
            journal: self.id,
            file: active.file.clone(),
            offset: t.offset + skew,
            buf: group.buf,
        };
        // Not again while full: the writes need it only once this one is.
        let half = t.offset + length > active.capacity / 2 && !t.full;
        send(&mut self.links.aio, AioJob::Write(write))?;
        self.in_flight = Some(InFlight {
            first,
            last: group.last,
            length,
            bytes: group.bytes,
        });
        if half {
            self.create_next()?;
        }
        Ok(())
    }

    /// Handles finished writes and new segments, and sends what waits
    /// behind them. Ready with [`Journal::durable`] once a write finished.
    ///
    /// # Errors
    ///
    /// [`Error::Write`] if a write failed, [`Error::Io`] if a new segment
    /// could not be made, [`Error::Closed`] once the disk threads stopped.
    pub fn poll_complete(&mut self, cx: &mut Context<'_>) -> Poll<Result<u64, Error>> {
        let mut wrote = false;
        loop {
            match self.links.done.poll_recv(cx) {
                Poll::Ready(Ok(done)) => match self.handle(done) {
                    Ok(w) => wrote |= w,
                    Err(e) => return Poll::Ready(Err(e)),
                },
                Poll::Ready(Err(_)) => return Poll::Ready(Err(Error::Closed)),
                Poll::Pending if wrote => return Poll::Ready(Ok(self.durable)),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    /// Blocks until the write in flight finishes, handling what comes
    /// before it as [`Journal::poll_complete`] does. Returns at once with
    /// nothing in flight.
    ///
    /// # Errors
    ///
    /// As [`Journal::poll_complete`].
    pub fn wait(&mut self) -> Result<u64, Error> {
        loop {
            if self.stopped {
                return Err(Error::Closed);
            }
            if self.in_flight.is_none() && !self.tail.creating {
                return Ok(self.durable);
            }
            let done = self.links.done.recv().map_err(|_| Error::Closed)?;
            if self.handle(done)? {
                return Ok(self.durable);
            }
        }
    }

    /// Sends everything appended and blocks until it is on disk, then notes
    /// that, as [`Journal::mark`] does.
    ///
    /// # Errors
    ///
    /// As [`Journal::poll_complete`].
    pub fn sync(&mut self) -> Result<u64, Error> {
        loop {
            self.flush()?;
            if self.durable >= self.appended {
                break;
            }
            self.wait()?;
        }
        let group = self.tail.group - 1;
        if group > 0 {
            segment::write_mark(&self.dir, group)?;
        }
        Ok(self.durable)
    }

    /// Starts a new segment for the records appended from here on, unless
    /// the current one holds none yet, and returns that segment's id. The
    /// records before stay in the segments before it. Once what the
    /// records from here on replace is durable, [`Journal::trim`] retires
    /// those. `None` while the next segment is still being made, or while
    /// the records before do not fit the current one, so writes need not
    /// wait: ask again later. On a device too full for another segment,
    /// the new one comes from the spare kept back for a roll, and the
    /// records from here on wait for it.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the disk threads stopped.
    pub fn roll(&mut self) -> Result<Option<u64>, Error> {
        let ahead = self.ahead();
        let t = &mut self.tail;
        let Some(a) = &t.active else {
            return Ok(Some(1));
        };
        let id = a.id + 1;
        if t.roll_after.is_some() {
            // Records from an earlier roll go there already.
            return Ok(Some(id));
        }
        if t.offset == PAGE as u64 && ahead == 0 {
            return Ok(Some(a.id));
        }
        let ready = t.offset + ahead <= a.capacity && (t.next.is_some() || t.full);
        if ready {
            t.roll_after = Some(self.appended);
            self.pending.seal();
        }
        // For these records, or for a later roll.
        self.create_next()?;
        Ok(ready.then_some(id))
    }

    /// Bytes the groups in flight and waiting take in a segment.
    fn ahead(&self) -> u64 {
        let p = &self.pending;
        let open = (p.open.records > 0).then_some(&p.open);
        let waiting = p
            .queued
            .iter()
            .chain(open)
            .map(|g| format::group_len(g.buf.len()) as u64)
            .sum::<u64>();
        self.in_flight.map_or(0, |w| w.length) + waiting
    }

    /// Retires the segments before `before`, oldest first, on the file
    /// thread, and notes the groups on disk so far. They stay as spares
    /// for the next segments, whose writes then land in blocks written
    /// before: those cost no metadata write.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the disk threads stopped.
    pub fn trim(&mut self, before: u64) -> Result<(), Error> {
        let dir = self.dir.clone();
        send(&mut self.links.files, FileJob::Trim { dir, before })?;
        self.mark()
    }

    /// Notes on the file thread that the groups written so far are on disk.
    /// A later open that finds fewer fails with [`Error::Corrupt`] instead
    /// of taking the damage for a torn write.
    ///
    /// # Errors
    ///
    /// [`Error::Closed`] once the disk threads stopped.
    pub fn mark(&mut self) -> Result<(), Error> {
        let group = self.tail.group - 1;
        if group == 0 {
            return Ok(());
        }
        let dir = self.dir.clone();
        send(&mut self.links.files, FileJob::Mark { dir, group })
    }

    /// Makes the next write fail, as a disk might.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub const fn fail_next_write(&mut self) {
        self.tail.faults.write = true;
    }

    /// Makes the next segment it asks for fail, as on a full device.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub const fn fail_next_segment(&mut self) {
        self.tail.faults.segment = true;
    }

    /// Groups written to the directory so far.
    #[cfg(feature = "testing")]
    #[doc(hidden)]
    pub const fn groups(&self) -> u64 {
        self.tail.group - 1
    }

    /// Bytes of the groups written since the journal opened, padding
    /// included: what it added to its segments.
    pub const fn written_bytes(&self) -> u64 {
        self.written_bytes
    }

    /// Number of the last record appended, 0 for none.
    pub const fn appended(&self) -> u64 {
        self.appended
    }

    /// Every record up to this number is on disk, or was reported failed
    /// with [`Error::Write`].
    pub const fn durable(&self) -> u64 {
        self.durable
    }

    /// Framed bytes of the records appended but not yet on disk. An owner
    /// that adds records for its peers stops reading them past a limit.
    pub const fn backlog(&self) -> usize {
        self.pending.bytes
    }

    /// Applies one completion. True for a finished write.
    fn handle(&mut self, done: Done) -> Result<bool, Error> {
        match done {
            Done::Written(mut buf, result) => {
                let w = self
                    .in_flight
                    .take()
                    .ok_or_else(|| io::Error::other("completion without a write"))?;
                buf.clear();
                self.pending.free.push(buf);
                self.pending.bytes -= w.bytes;
                self.durable = w.last;
                match result {
                    Ok(()) => {
                        self.written_bytes += w.length;
                        self.tail.offset += w.length;
                        self.tail.group += 1;
                        self.send()?;
                        Ok(true)
                    }
                    // The next group goes to the same place, with the same
                    // number. A send error shows again on the next call.
                    Err(source) => {
                        let _ = self.send();
                        Err(Error::Write {
                            first: w.first,
                            last: w.last,
                            source,
                        })
                    }
                }
            }
            Done::Created(result) => {
                self.tail.creating = false;
                self.tail.full = result.is_err();
                self.tail.next = Some(result?);
                self.send()?;
                Ok(false)
            }
            Done::Stopped => {
                self.stopped = true;
                match self.in_flight.take() {
                    Some(w) => {
                        self.durable = w.last;
                        Err(Error::Write {
                            first: w.first,
                            last: w.last,
                            source: io::Error::other("the disk threads stopped"),
                        })
                    }
                    None => Err(Error::Closed),
                }
            }
        }
    }

    /// Asks the file thread for the next segment, unless it is made or
    /// being made.
    fn create_next(&mut self) -> Result<(), Error> {
        let t = &mut self.tail;
        if t.creating || t.next.is_some() {
            return Ok(());
        }
        let reply = self
            .links
            .done_tx
            .try_register()
            .map_err(|_| Error::Closed)?;
        #[cfg(feature = "testing")]
        if std::mem::take(&mut t.faults.segment) {
            let mut reply = reply;
            let full = io::Error::from(io::ErrorKind::StorageFull);
            send(&mut reply, Done::Created(Err(full)))?;
            t.creating = true;
            return Ok(());
        }
        let job = FileJob::Create {
            dir: self.dir.clone(),
            id: t.active.as_ref().map_or(1, |a| a.id + 1),
            capacity: self.config.segment_bytes,
            zero: t.active.is_none(),
            last_spare: t.roll_after.is_some(),
            reply,
        };
        send(&mut self.links.files, job)?;
        t.creating = true;
        Ok(())
    }
}

impl Pending {
    /// Queues the open group, if it has records, so later ones start
    /// another.
    fn seal(&mut self) {
        if self.open.records > 0 {
            let fresh = Group {
                buf: self.free.pop().unwrap_or_default(),
                ..Group::default()
            };
            self.queued
                .push_back(std::mem::replace(&mut self.open, fresh));
        }
    }

    /// The oldest group with records, taken out.
    fn take(&mut self) -> Option<Group> {
        if let Some(g) = self.queued.pop_front() {
            return Some(g);
        }
        if self.open.records == 0 {
            return None;
        }
        let fresh = Group {
            buf: self.free.pop().unwrap_or_default(),
            ..Group::default()
        };
        Some(std::mem::replace(&mut self.open, fresh))
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        let _ = self.links.aio.try_send(AioJob::Close(self.id));
    }
}

fn send<T>(tx: &mut Sender<T>, job: T) -> Result<(), Error> {
    tx.try_send(job).map_err(|e| match e {
        TrySendError::Disconnected(_) => Error::Closed,
        TrySendError::Full(_) => Error::Io(io::Error::other("disk queue full")),
    })
}

/// What a scan of the directory found.
struct Found {
    /// The last segment with groups: id, capacity, and where its next
    /// group goes.
    active: Option<(u64, u64, u64)>,
    /// Segments after it, with no groups: id and capacity.
    empty: Vec<(u64, u64)>,
    /// The last group's number.
    last: Option<u64>,
}

impl Disk {
    /// Opens the journal in `dir`, making the directory if missing, and hands
    /// each record already written to `replay`, oldest first.
    ///
    /// # Errors
    ///
    /// [`Error::Io`] if the directory cannot be read or written, takes no
    /// `O_DIRECT`, or another journal holds it; [`Error::Corrupt`] for
    /// damage inside what was written; [`Error::Replay`] if `replay`
    /// refuses a record; [`Error::Closed`] once the disk threads stopped.
    pub fn open(
        &self,
        dir: impl AsRef<Path>,
        mut replay: impl FnMut(Record<'_>) -> Result<(), ReplayError>,
    ) -> Result<Journal, Error> {
        let dir: Arc<Path> = dir.as_ref().into();
        if !dir.is_dir() {
            fs::create_dir_all(&dir)?;
            // The new directory itself has to survive a crash.
            if let Some(parent) = dir.parent().filter(|p| !p.as_os_str().is_empty()) {
                segment::sync_dir(parent)?;
            }
        }
        let lock = segment::lock(&dir)?;
        let found = recover(&dir, &mut replay)?;
        let tail = Self::tail(&dir, found)?;
        let (done_tx, done) = mpsc::channel(QUEUE);
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let mut aio = locked(&self.aio)
            .try_register()
            .map_err(|_| Error::Closed)?;
        let files = locked(&self.files)
            .try_register()
            .map_err(|_| Error::Closed)?;
        let owner = done_tx.try_register().map_err(|_| Error::Closed)?;
        send(&mut aio, AioJob::Open(id, owner))?;
        Ok(Journal {
            id,
            dir,
            config: self.config,
            links: Links {
                aio,
                files,
                done_tx,
                done,
            },
            tail,
            pending: Pending::default(),
            in_flight: None,
            appended: 0,
            flushed: 0,
            durable: 0,
            written_bytes: 0,
            stopped: false,
            _lock: lock,
        })
    }

    /// Opens the segments writes continue in and removes further empty
    /// ones. A journal with none makes its first on the first write.
    fn tail(dir: &Path, found: Found) -> Result<Tail, Error> {
        let mut empty = found.empty.into_iter();
        let (active, offset) = match found.active {
            Some((id, capacity, end)) => (Some(segment::open(dir, id, capacity)?), end),
            None => match empty.next() {
                Some((id, capacity)) => (Some(segment::open(dir, id, capacity)?), PAGE as u64),
                None => (None, PAGE as u64),
            },
        };
        let next = empty
            .next()
            .map(|(id, capacity)| segment::open(dir, id, capacity))
            .transpose()?;
        for (id, _) in empty {
            segment::remove(dir, id)?;
        }
        Ok(Tail {
            active,
            offset,
            next,
            creating: false,
            full: false,
            roll_after: None,
            #[cfg(feature = "testing")]
            faults: Faults::default(),
            group: found.last.map_or(1, |g| g + 1),
        })
    }
}

/// Reads every segment of `dir` in order.
fn recover(
    dir: &Path,
    replay: &mut dyn FnMut(Record<'_>) -> Result<(), ReplayError>,
) -> Result<Found, Error> {
    let marked = segment::read_mark(dir).map_err(|_| Error::Corrupt {
        path: dir.join("DURABLE"),
        offset: 0,
        reason: "the durable mark is damaged",
    })?;
    let ids = segment::list(dir)?;
    let mut found = Found {
        active: None,
        empty: Vec::new(),
        last: None,
    };
    for (i, &id) in ids.iter().enumerate() {
        let path = segment::path(dir, id);
        let corrupt = |offset, reason| Error::Corrupt {
            path: path.clone(),
            offset,
            reason,
        };
        if i > 0 && ids[i - 1] + 1 != id {
            return Err(corrupt(0, "the segment before it is missing"));
        }
        let bytes = segment::read(dir, id)?;
        let first = found.last.map(|g| g + 1);
        let scanned = format::scan(&bytes, first, &mut |kind, body| {
            replay(Record { kind, body })
        })
        .map_err(|e| match e {
            ScanError::Corrupt { offset, reason } => corrupt(offset, reason),
            ScanError::Replay(e) => Error::Replay(e),
        })?;
        if scanned.id != id {
            return Err(corrupt(0, "segment header names another segment"));
        }
        match scanned.last {
            Some(_) if !found.empty.is_empty() => {
                return Err(corrupt(0, "a written segment follows an empty one"));
            }
            Some(last) => {
                found.last = Some(last);
                found.active = Some((id, scanned.capacity, scanned.end));
            }
            None => found.empty.push((id, scanned.capacity)),
        }
    }
    if found.last.unwrap_or(0) < marked {
        return Err(Error::Corrupt {
            path: dir.join("DURABLE"),
            offset: 0,
            reason: "the journal ends before groups it had on disk",
        });
    }
    Ok(found)
}

#[cfg(test)]
mod tests;
