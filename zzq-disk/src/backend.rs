//! The threads behind journals: one submits writes to the kernel and reaps
//! them, one makes segment files.

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::os::fd::OwnedFd;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Wake, Waker};
use std::thread::JoinHandle;

use fanring::mpsc::{self, Receiver, Sender};
use rustix::event::{EventfdFlags, PollFd, PollFlags, eventfd, poll};
use rustix::io::Errno;

use crate::Error;
use crate::aio::{AioContext, AlignedBuf};
use crate::format::PAGE;
use crate::segment::{self, Segment};

/// Jobs a sender may queue. Each journal has at most one write and one
/// segment request outstanding, so this never fills.
pub(crate) const QUEUE: usize = 4;

/// Settings of a [`Disk`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct Config {
    /// Bytes per segment file: a multiple of 4096 from 8192 to 1 GiB. A
    /// record with its framing must fit one, after the 4096-byte header.
    pub segment_bytes: u64,
    /// A write group takes records up to this many bytes; later ones go to
    /// the next. A single larger record makes a group of its own.
    pub group_bytes: usize,
    /// Writes the kernel holds at once across all journals, 1 to 1024.
    /// More wait their turn.
    pub depth: usize,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            segment_bytes: 64 << 20,
            group_bytes: 1 << 20,
            depth: 64,
        }
    }
}

impl Config {
    fn check(&self) -> Result<(), Error> {
        let s = self.segment_bytes;
        if s < 2 * PAGE as u64 || s > segment::MAX_BYTES || !s.is_multiple_of(PAGE as u64) {
            return Err(Error::Config("segment_bytes"));
        }
        if self.group_bytes == 0 || self.group_bytes as u64 > s - PAGE as u64 {
            return Err(Error::Config("group_bytes"));
        }
        if !(1..=1024).contains(&self.depth) {
            return Err(Error::Config("depth"));
        }
        Ok(())
    }
}

pub(crate) enum AioJob {
    /// Completions for journal `.0` go to `.1`.
    Open(u64, Sender<Done>),
    Write(Write),
    Close(u64),
    Stop,
}

pub(crate) struct Write {
    pub(crate) journal: u64,
    pub(crate) file: Arc<File>,
    pub(crate) offset: u64,
    pub(crate) buf: AlignedBuf,
}

pub(crate) enum FileJob {
    Create {
        dir: Arc<Path>,
        id: u64,
        capacity: u64,
        reply: Sender<Done>,
    },
    Stop,
}

/// What a journal hears back.
pub(crate) enum Done {
    /// The write in flight finished, and its buffer for reuse.
    Written(AlignedBuf, io::Result<()>),
    Created(io::Result<Segment>),
    /// The AIO thread stopped: nothing more completes.
    Stopped,
}

/// The threads that write journals. Dropping it lets writes in flight
/// finish, then stops them; journals then fail with [`Error::Closed`].
pub struct Disk {
    pub(crate) config: Config,
    pub(crate) aio: Mutex<Sender<AioJob>>,
    pub(crate) files: Mutex<Sender<FileJob>>,
    /// Ids for journals.
    pub(crate) next: AtomicU64,
    threads: Vec<JoinHandle<()>>,
}

impl std::fmt::Debug for Disk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Disk")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Disk {
    /// Starts the threads.
    ///
    /// # Errors
    ///
    /// [`Error::Config`] for a setting out of range, [`Error::Io`] if the
    /// kernel refuses an AIO context, such as past `fs.aio-max-nr`, or a
    /// thread does not start.
    pub fn new(config: Config) -> Result<Disk, Error> {
        config.check()?;
        let context = AioContext::new(config.depth)?;
        let signal = Arc::new(Signal {
            fd: eventfd(0, EventfdFlags::CLOEXEC | EventfdFlags::NONBLOCK)
                .map_err(io::Error::from)?,
            failed: AtomicBool::new(false),
        });
        let (aio, aio_rx) = mpsc::channel(QUEUE);
        let (files, files_rx) = mpsc::channel(QUEUE);
        let threads = vec![
            std::thread::Builder::new()
                .name("zzq-disk-aio".to_owned())
                .spawn(move || run_aio(context, &signal, aio_rx))?,
            std::thread::Builder::new()
                .name("zzq-disk-files".to_owned())
                .spawn(move || run_files(files_rx))?,
        ];
        Ok(Disk {
            config,
            aio: Mutex::new(aio),
            files: Mutex::new(files),
            next: AtomicU64::new(0),
            threads,
        })
    }

    /// The settings it runs with.
    pub const fn config(&self) -> &Config {
        &self.config
    }
}

pub(crate) fn locked<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Drop for Disk {
    fn drop(&mut self) {
        let _ = locked(&self.files).try_send(FileJob::Stop);
        let _ = locked(&self.aio).try_send(AioJob::Stop);
        for t in self.threads.drain(..) {
            let _ = t.join();
        }
    }
}

/// Wakes the AIO thread through an eventfd it polls beside the kernel's.
#[derive(Debug)]
struct Signal {
    fd: OwnedFd,
    /// A wake could not write the eventfd; the thread stops.
    failed: AtomicBool,
}

impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        loop {
            match rustix::io::write(&self.fd, &1_u64.to_ne_bytes()) {
                Err(Errno::INTR) => {}
                // AGAIN: the counter is readable already.
                Ok(_) | Err(Errno::AGAIN) => return,
                Err(_) => {
                    // Release: the thread acquires it after its next wake.
                    self.failed.store(true, Ordering::Release);
                    return;
                }
            }
        }
    }
}

impl Signal {
    fn clear(&self) -> io::Result<()> {
        if self.failed.load(Ordering::Acquire) {
            return Err(io::Error::other("AIO thread wake failed"));
        }
        loop {
            match rustix::io::read(&self.fd, &mut [0_u8; 8]) {
                Err(Errno::INTR) => {}
                Ok(_) | Err(Errno::AGAIN) => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
    }
}

/// The AIO thread. Whatever ends it, the journals hear [`Done::Stopped`].
fn run_aio(mut context: AioContext<u64>, signal: &Arc<Signal>, jobs: Receiver<AioJob>) {
    let mut owners = HashMap::new();
    let _ = aio_turns(&mut context, signal, jobs, &mut owners);
    // Waits for writes still in flight after a failure.
    drop(context);
    for (_, mut owner) in owners {
        let _ = owner.try_send(Done::Stopped);
    }
}

fn aio_turns(
    context: &mut AioContext<u64>,
    signal: &Arc<Signal>,
    jobs: Receiver<AioJob>,
    owners: &mut HashMap<u64, Sender<Done>>,
) -> io::Result<()> {
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut jobs = Some(jobs);
    let mut waiting = VecDeque::new();
    loop {
        // Clear first, then register and look. A wake before the register
        // is covered by the look after it.
        signal.clear()?;
        if let Some(rx) = &mut jobs
            && take_jobs(rx, &mut cx, owners, &mut waiting)
        {
            // Stop: later sends fail, accepted writes still go out.
            jobs = None;
        }
        context.reap(|(journal, buf, result)| {
            let result = result.and_then(|n| {
                if n == buf.len() {
                    Ok(())
                } else {
                    Err(io::Error::new(io::ErrorKind::WriteZero, "short write"))
                }
            });
            deliver(owners, journal, Done::Written(buf, result));
        })?;
        while !context.is_full()
            && let Some(w) = waiting.pop_front()
        {
            if let Err((journal, buf, e)) = context.submit_write(w.file, w.offset, w.buf, w.journal)
            {
                deliver(owners, journal, Done::Written(buf, Err(e)));
            }
        }
        if jobs.is_none() && context.in_flight() == 0 && waiting.is_empty() {
            return Ok(());
        }
        wait(signal, context)?;
    }
}

/// Takes every job queued. True once told to stop.
fn take_jobs(
    rx: &mut Receiver<AioJob>,
    cx: &mut Context<'_>,
    owners: &mut HashMap<u64, Sender<Done>>,
    waiting: &mut VecDeque<Write>,
) -> bool {
    loop {
        match rx.poll_recv(cx) {
            Poll::Ready(Ok(AioJob::Open(journal, owner))) => {
                owners.insert(journal, owner);
            }
            Poll::Ready(Ok(AioJob::Write(w))) => waiting.push_back(w),
            Poll::Ready(Ok(AioJob::Close(journal))) => {
                owners.remove(&journal);
            }
            Poll::Ready(Ok(AioJob::Stop) | Err(_)) => return true,
            Poll::Pending => return false,
        }
    }
}

/// Sends `done` to `journal`, unless it closed.
fn deliver(owners: &mut HashMap<u64, Sender<Done>>, journal: u64, done: Done) {
    if let Some(owner) = owners.get_mut(&journal) {
        // Full cannot happen: one write per journal is in flight.
        let _ = owner.try_send(done);
    }
}

/// Blocks until a wake or a kernel completion is ready.
fn wait(signal: &Signal, context: &AioContext<u64>) -> io::Result<()> {
    let mut fds = [
        PollFd::new(&signal.fd, PollFlags::IN),
        PollFd::new(context, PollFlags::IN),
    ];
    loop {
        match poll(&mut fds, None) {
            Err(Errno::INTR) => {}
            Ok(_) => break,
            Err(e) => return Err(e.into()),
        }
    }
    if fds.iter().any(|fd| {
        fd.revents()
            .intersects(PollFlags::ERR | PollFlags::HUP | PollFlags::NVAL)
    }) {
        return Err(io::Error::other("AIO thread readiness failed"));
    }
    Ok(())
}

/// The file thread: makes segments, which takes long enough to stall
/// completions if the AIO thread did it.
fn run_files(mut jobs: Receiver<FileJob>) {
    while let Ok(job) = jobs.recv() {
        match job {
            FileJob::Create {
                dir,
                id,
                capacity,
                mut reply,
            } => {
                let _ = reply.try_send(Done::Created(segment::create(&dir, id, capacity)));
            }
            FileJob::Stop => return,
        }
    }
}
