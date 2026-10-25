//! The one `zzq_tokio::Context` that clients and brokers of this process
//! share.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, PoisonError};

use zzq_client::{Config, Context};

use crate::error::Error;

static IO_THREADS: AtomicUsize = AtomicUsize::new(1);
static CONTEXT: Mutex<Option<(u32, Context)>> = Mutex::new(None);

/// The shared context, started on first use. A forked child gets its own:
/// the parent's lane threads do not exist in it.
pub fn get() -> Result<Context, Error> {
    let pid = std::process::id();
    let mut slot = CONTEXT.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some((owner, ctx)) = slot.as_ref()
        && *owner == pid
    {
        return Ok(ctx.clone());
    }
    // Dropping the parent's context would wait for threads this process
    // does not have.
    std::mem::forget(slot.take());
    let config = Config {
        io_threads: IO_THREADS.load(Ordering::Relaxed),
        ..Config::default()
    };
    let ctx = Context::with_config(&config).map_err(zzq_client::Error::from)?;
    *slot = Some((pid, ctx.clone()));
    Ok(ctx)
}

pub fn io_threads() -> usize {
    IO_THREADS.load(Ordering::Relaxed)
}

/// Sets the lane count, unless the context runs already.
pub fn set_io_threads(n: usize) -> bool {
    let slot = CONTEXT.lock().unwrap_or_else(PoisonError::into_inner);
    if slot
        .as_ref()
        .is_some_and(|(owner, _)| *owner == std::process::id())
    {
        return false;
    }
    IO_THREADS.store(n.max(1), Ordering::Relaxed);
    true
}
