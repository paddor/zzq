//! `ZZQ::Native::Wake`: how a Ruby fiber waits for a Rust future.
//!
//! A native call polls the future with a waker that writes one byte to a
//! pipe. If the future is pending, Ruby waits for the pipe to turn
//! readable, which suspends only the fiber under a fiber scheduler and
//! releases the GVL otherwise, then polls again.

use std::future::Future;
use std::os::fd::RawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll, Waker};

use rb_sys::VALUE;

use crate::rb::{self, DataType, RbResult, RubyErr};

static TYPE: DataType = DataType::new::<Wake>(c"ZZQ::Native::Wake");
static CLASS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();

/// A pipe one fiber waits on.
#[derive(Debug)]
pub struct Wake {
    pipe: Arc<Pipe>,
    waker: Waker,
}

#[derive(Debug)]
struct Pipe {
    read_fd: RawFd,
    write_fd: RawFd,
    /// The fiber is about to wait, so a wake must write.
    armed: AtomicBool,
}

impl std::task::Wake for Pipe {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        if self.armed.swap(false, Ordering::AcqRel) {
            let byte = 1u8;
            loop {
                let n = unsafe { libc::write(self.write_fd, (&raw const byte).cast(), 1) };
                // A full pipe holds a wake already.
                if n >= 0 || std::io::Error::last_os_error().raw_os_error() != Some(libc::EINTR) {
                    break;
                }
            }
        }
    }
}

impl Drop for Pipe {
    fn drop(&mut self) {
        unsafe {
            libc::close(self.read_fd);
            libc::close(self.write_fd);
        }
    }
}

impl Wake {
    fn new() -> std::io::Result<Wake> {
        let mut fds = [0; 2];
        if unsafe { pipe(&mut fds) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let pipe = Arc::new(Pipe {
            read_fd: fds[0],
            write_fd: fds[1],
            armed: AtomicBool::new(false),
        });
        let waker = Waker::from(pipe.clone());
        Ok(Wake { pipe, waker })
    }

    /// Polls `fut`. While it is pending, a wake writes to the pipe.
    pub fn poll<F: Future + ?Sized>(&self, fut: Pin<&mut F>) -> Poll<F::Output> {
        self.pipe.armed.store(true, Ordering::Release);
        let out = fut.poll(&mut Context::from_waker(&self.waker));
        if out.is_ready() {
            self.pipe.armed.store(false, Ordering::Release);
        }
        out
    }

    /// Reads the wakes written so far.
    fn clear(&self) {
        let mut buf = [0u8; 64];
        loop {
            let n = unsafe { libc::read(self.pipe.read_fd, buf.as_mut_ptr().cast(), buf.len()) };
            if n > 0 {
                continue;
            }
            if n < 0 && std::io::Error::last_os_error().raw_os_error() == Some(libc::EINTR) {
                continue;
            }
            break;
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "android"))]
unsafe fn pipe(fds: &mut [RawFd; 2]) -> i32 {
    unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_NONBLOCK | libc::O_CLOEXEC) }
}

#[cfg(all(unix, not(any(target_os = "linux", target_os = "android"))))]
unsafe fn pipe(fds: &mut [RawFd; 2]) -> i32 {
    let ret = unsafe { libc::pipe(fds.as_mut_ptr()) };
    if ret == 0 {
        for fd in *fds {
            unsafe {
                libc::fcntl(fd, libc::F_SETFL, libc::O_NONBLOCK);
                libc::fcntl(fd, libc::F_SETFD, libc::FD_CLOEXEC);
            }
        }
    }
    ret
}

/// The `Wake` inside `value`.
pub fn get(value: VALUE) -> RbResult<&'static Wake> {
    unsafe { TYPE.get(value) }
}

unsafe extern "C" fn wake_new(_class: VALUE) -> VALUE {
    rb::wrap(|| {
        let wake = Wake::new().map_err(|e| RubyErr::runtime(format!("pipe failed: {e}")))?;
        TYPE.wrap(*CLASS.get().expect("registered") as VALUE, wake)
    })
}

unsafe extern "C" fn wake_fd(this: VALUE) -> VALUE {
    rb::wrap(|| Ok(rb::u64_value(get(this)?.pipe.read_fd as u64)))
}

unsafe extern "C" fn wake_clear(this: VALUE) -> VALUE {
    rb::wrap(|| {
        get(this)?.clear();
        Ok(rb::qnil())
    })
}

pub fn register(native: VALUE) -> RbResult<()> {
    let class = rb::define_class_under(native, c"Wake")?;
    let _ = CLASS.set(class as usize);
    rb::define_singleton_method(class, c"new", wake_new as *const (), 0)?;
    rb::define_method(class, c"fd", wake_fd as *const (), 0)?;
    rb::define_method(class, c"clear", wake_clear as *const (), 0)?;
    Ok(())
}
