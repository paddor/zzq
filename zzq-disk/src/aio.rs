//! Linux kernel AIO (`io_submit`) for one thread.
//!
//! The owning thread submits page-aligned writes and reaps completions
//! without blocking; an eventfd tells it when to reap. Every buffer and job
//! stays owned by the context until the kernel reports the write finished.
//! Dropping the context waits for writes still in flight.
#![allow(unsafe_code)]

use std::fs::File;
use std::io;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::sync::Arc;

/// File offset, length and memory alignment of every submitted write.
pub(crate) const ALIGNMENT: usize = 4096;

const IOCB_CMD_PWRITE: u16 = 1;
const IOCB_FLAG_RESFD: u32 = 1;

/// Kernel `struct io_event`.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
struct IoEvent {
    data: u64,
    obj: u64,
    res: i64,
    res2: i64,
}

/// Heap bytes that start on an [`ALIGNMENT`] boundary, reused across
/// writes. Growing moves the bytes to a new aligned allocation; appending
/// within the reserved room never moves them.
#[derive(Debug, Default)]
pub(crate) struct AlignedBuf {
    bytes: Vec<u8>,
    start: usize,
}

impl AlignedBuf {
    pub(crate) const fn len(&self) -> usize {
        self.bytes.len() - self.start
    }

    pub(crate) const fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Makes room for `additional` more bytes.
    pub(crate) fn reserve(&mut self, additional: usize) -> io::Result<()> {
        let len = self.len();
        let need = len
            .checked_add(additional)
            .ok_or(io::ErrorKind::InvalidInput)?;
        if self.bytes.capacity() - self.start >= need {
            return Ok(());
        }
        let capacity = need
            .max(len.saturating_mul(2))
            .checked_add(ALIGNMENT)
            .ok_or(io::ErrorKind::InvalidInput)?;
        let mut bytes: Vec<u8> = Vec::new();
        bytes
            .try_reserve_exact(capacity)
            .map_err(io::Error::other)?;
        // Capacity covers the padding, so the start stays in the allocation.
        let start = (ALIGNMENT - bytes.as_ptr().addr() % ALIGNMENT) % ALIGNMENT;
        bytes.resize(start, 0);
        bytes.extend_from_slice(self.as_slice());
        self.bytes = bytes;
        self.start = start;
        Ok(())
    }

    pub(crate) fn extend(&mut self, data: &[u8]) -> io::Result<()> {
        self.reserve(data.len())?;
        self.bytes.extend_from_slice(data);
        Ok(())
    }

    /// Appends zeros up to the next multiple of [`ALIGNMENT`].
    pub(crate) fn pad(&mut self) -> io::Result<()> {
        let pad = self.len().next_multiple_of(ALIGNMENT) - self.len();
        self.reserve(pad)?;
        self.bytes.resize(self.bytes.len() + pad, 0);
        Ok(())
    }

    /// Empties the buffer and keeps its allocation.
    pub(crate) fn clear(&mut self) {
        self.bytes.truncate(self.start);
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[self.start..]
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes[self.start..]
    }
}

#[derive(Debug)]
struct Request<T> {
    buffer: AlignedBuf,
    file: Arc<File>,
    job: T,
}

/// A finished write: its job, its buffer, and the bytes written or why not.
pub(crate) type Completion<T> = (T, AlignedBuf, io::Result<usize>);

/// One kernel AIO context and the requests it owns.
#[derive(Debug)]
pub(crate) struct AioContext<T> {
    context: u64,
    event: OwnedFd,
    slots: Vec<Option<Request<T>>>,
    in_flight: usize,
    events: Vec<IoEvent>,
    /// `events.len()` as the syscall argument type.
    capacity: libc::c_long,
}

fn last_error() -> io::Error {
    io::Error::last_os_error()
}

impl<T> AioContext<T> {
    /// A context for at most `depth` writes in flight, 1 to 1024.
    pub(crate) fn new(depth: usize) -> io::Result<Self> {
        if !(1..=1024).contains(&depth) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AIO depth must be 1..=1024",
            ));
        }
        let requests = libc::c_long::try_from(depth)
            .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
        let mut context = 0_u64;
        // SAFETY: io_setup writes one aio_context_t into `context`.
        let result = unsafe {
            libc::syscall(
                libc::SYS_io_setup,
                requests,
                std::ptr::from_mut(&mut context),
            )
        };
        if result != 0 {
            return Err(last_error());
        }
        // SAFETY: eventfd takes no pointers.
        let raw = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
        if raw < 0 {
            let error = last_error();
            // SAFETY: the context exists and has no requests.
            unsafe { libc::syscall(libc::SYS_io_destroy, context) };
            return Err(error);
        }
        // SAFETY: `raw` is a fresh descriptor owned by nothing else.
        let event = unsafe { OwnedFd::from_raw_fd(raw) };
        Ok(Self {
            context,
            event,
            slots: (0..depth).map(|_| None).collect(),
            in_flight: 0,
            events: vec![IoEvent::default(); depth],
            capacity: requests,
        })
    }

    /// Whether every slot holds a write in flight.
    pub(crate) const fn is_full(&self) -> bool {
        self.in_flight == self.slots.len()
    }

    /// Requests whose buffers and jobs the kernel context still owns.
    pub(crate) const fn in_flight(&self) -> usize {
        self.in_flight
    }

    /// Submits one write of `buffer` at `offset` of `file`, which must be
    /// open with `O_DIRECT`. On failure the job and buffer come back,
    /// including for a full context (`WouldBlock`) and an unaligned offset,
    /// length or buffer.
    pub(crate) fn submit_write(
        &mut self,
        file: Arc<File>,
        offset: u64,
        buffer: AlignedBuf,
        job: T,
    ) -> Result<(), (T, AlignedBuf, io::Error)> {
        let offset = match check(offset, &buffer) {
            Ok(offset) => offset,
            Err(error) => return Err((job, buffer, error)),
        };
        let Some(slot) = self.slots.iter().position(Option::is_none) else {
            return Err((job, buffer, io::ErrorKind::WouldBlock.into()));
        };
        let request = Request { buffer, file, job };
        let bytes = request.buffer.as_slice();
        // SAFETY: all-zero is a valid iocb; the fields set below complete it.
        let mut iocb: libc::iocb = unsafe { std::mem::zeroed() };
        iocb.aio_data = slot as u64;
        iocb.aio_lio_opcode = IOCB_CMD_PWRITE;
        iocb.aio_fildes = request.file.as_raw_fd() as u32;
        iocb.aio_buf = bytes.as_ptr() as u64;
        iocb.aio_nbytes = bytes.len() as u64;
        iocb.aio_offset = offset;
        iocb.aio_flags = IOCB_FLAG_RESFD;
        iocb.aio_resfd = self.event.as_raw_fd() as u32;
        let mut list = [std::ptr::from_mut(&mut iocb)];
        // SAFETY: the iocb is read during the call. Its buffer and descriptor
        // stay alive in `request`, then in `slots` until io_getevents or
        // io_destroy reports it. Moving `AlignedBuf` moves only its `Vec`
        // header, so the heap bytes the kernel reads stay in place.
        let result =
            unsafe { libc::syscall(libc::SYS_io_submit, self.context, 1_i64, list.as_mut_ptr()) };
        if result == 1 {
            self.slots[slot] = Some(request);
            self.in_flight += 1;
            return Ok(());
        }
        let error = if result < 0 {
            let error = last_error();
            if error.raw_os_error() == Some(libc::EAGAIN) {
                io::ErrorKind::WouldBlock.into()
            } else {
                error
            }
        } else {
            io::Error::other("io_submit accepted no request")
        };
        Err((request.job, request.buffer, error))
    }

    /// Hands every finished write to `done` without blocking. Returns how
    /// many.
    pub(crate) fn reap(&mut self, done: impl FnMut(Completion<T>)) -> io::Result<usize> {
        self.drain_event()?;
        self.collect(
            0,
            Some(libc::timespec {
                tv_sec: 0,
                tv_nsec: 0,
            }),
            done,
        )
    }

    /// Waits for at least one write in flight to finish, then reaps.
    #[cfg(test)]
    pub(crate) fn wait(&mut self, done: impl FnMut(Completion<T>)) -> io::Result<usize> {
        if self.in_flight == 0 {
            return Ok(0);
        }
        self.drain_event()?;
        self.collect(1, None, done)
    }

    fn drain_event(&self) -> io::Result<()> {
        let mut counter = [0_u8; 8];
        loop {
            // SAFETY: reads at most 8 bytes into `counter`.
            let result = unsafe {
                libc::read(
                    self.event.as_raw_fd(),
                    counter.as_mut_ptr().cast(),
                    counter.len(),
                )
            };
            if result < 0 {
                let error = last_error();
                if error.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                if error.raw_os_error() != Some(libc::EAGAIN) {
                    return Err(error);
                }
            }
            return Ok(());
        }
    }

    fn collect(
        &mut self,
        minimum: libc::c_long,
        timeout: Option<libc::timespec>,
        mut done: impl FnMut(Completion<T>),
    ) -> io::Result<usize> {
        let mut timeout = timeout;
        let timeout = timeout
            .as_mut()
            .map_or(std::ptr::null_mut(), std::ptr::from_mut);
        let count = loop {
            // SAFETY: `events` holds `depth` entries; the kernel writes at most
            // that many. `timeout` is null or points to a live timespec.
            let result = unsafe {
                libc::syscall(
                    libc::SYS_io_getevents,
                    self.context,
                    minimum,
                    self.capacity,
                    self.events.as_mut_ptr(),
                    timeout,
                )
            };
            if result >= 0 {
                break result as usize;
            }
            let error = last_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        };
        for index in 0..count {
            let event = self.events[index];
            let request = usize::try_from(event.data)
                .ok()
                .and_then(|slot| self.slots.get_mut(slot))
                .and_then(Option::take)
                .ok_or_else(|| io::Error::other("completion for an unknown AIO request"))?;
            self.in_flight -= 1;
            let result = if event.res < 0 {
                Err(io::Error::from_raw_os_error(-event.res as i32))
            } else if event.res as usize > request.buffer.len() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "AIO completion exceeds submitted length",
                ))
            } else {
                Ok(event.res as usize)
            };
            done((request.job, request.buffer, result));
        }
        Ok(count)
    }
}

/// The kernel offset of a write of `buffer` at `offset` that direct I/O
/// accepts.
fn check(offset: u64, buffer: &AlignedBuf) -> io::Result<i64> {
    let bytes = buffer.as_slice();
    let aligned = !bytes.is_empty()
        && offset.is_multiple_of(ALIGNMENT as u64)
        && bytes.len().is_multiple_of(ALIGNMENT)
        && bytes.as_ptr().addr().is_multiple_of(ALIGNMENT);
    let in_range = offset
        .checked_add(bytes.len() as u64)
        .is_some_and(|end| i64::try_from(end).is_ok());
    match i64::try_from(offset) {
        Ok(offset) if aligned && in_range => Ok(offset),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unaligned AIO write",
        )),
    }
}

impl<T> AsFd for AioContext<T> {
    /// Readable after at least one write finished since the last reap.
    fn as_fd(&self) -> BorrowedFd<'_> {
        self.event.as_fd()
    }
}

impl<T> Drop for AioContext<T> {
    fn drop(&mut self) {
        // SAFETY: io_destroy waits for requests still in flight, so no buffer
        // in `slots` is freed while the kernel may still read it.
        let result = unsafe { libc::syscall(libc::SYS_io_destroy, self.context) };
        if result != 0 {
            // The kernel may still read these buffers; never free them.
            std::mem::forget(std::mem::take(&mut self.slots));
        }
    }
}

#[cfg(test)]
mod tests;
