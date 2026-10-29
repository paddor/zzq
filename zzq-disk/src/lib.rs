//! Durable append-only journals for zzq, written with Linux AIO.
//!
//! A [`Disk`] runs two threads: one submits journal writes to the kernel
//! and reaps them, one makes segment files. A [`Journal`] belongs to one
//! owner, such as a broker lane, and writes to a directory of its own.
//! Records collect into a write group; with no write in flight,
//! [`Journal::flush`] sends them at once, even one, and records appended
//! meanwhile wait behind it. Writes use `O_DIRECT | O_DSYNC`, so a
//! completed write is on disk and [`Journal::durable`] moves to its last
//! record. Trimmed segments stay as spares for later ones, so writes
//! land in blocks written before. One spare stays back for a roll, so a
//! snapshot can start a segment on a full device.
//!
//! The directory must take `O_DIRECT`. Linux only.
//!
//! ```no_run
//! use zzq_disk::{Config, Disk};
//!
//! let disk = Disk::new(Config::default())?;
//! let mut journal = disk.open("/var/lib/zzq/lane-0", |record| {
//!     println!("{} bytes of kind {}", record.body.len(), record.kind);
//!     Ok(())
//! })?;
//! let n = journal.append(1, &[b"hello"])?;
//! assert_eq!(journal.sync()?, n);
//! # Ok::<(), zzq_disk::Error>(())
//! ```
#![cfg(target_os = "linux")]

mod aio;
mod backend;
mod format;
#[cfg(feature = "fuzzing")]
#[doc(hidden)]
pub mod fuzzing;
mod journal;
mod segment;
#[cfg(test)]
mod testing;

use std::io;
use std::path::{Path, PathBuf};

pub use backend::{Config, Disk};
pub use journal::{Journal, Record};

/// Bytes an unprivileged writer may still take on the device that holds
/// `path`, such as a journal's directory.
///
/// # Errors
///
/// [`Error::Io`] if the device cannot tell, as for a missing path.
pub fn free_bytes(path: impl AsRef<Path>) -> Result<u64, Error> {
    let st = rustix::fs::statvfs(path.as_ref()).map_err(io::Error::from)?;
    Ok(st.f_bavail.saturating_mul(st.f_frsize))
}

/// Why replaying a record failed, from the callback of [`Disk::open`].
pub type ReplayError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of disks and journals.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A file or the kernel failed.
    #[error("journal I/O failed")]
    Io(#[from] io::Error),
    /// A segment holds bytes no journal wrote there, or misses some.
    #[error("{}: {reason}, at byte {offset}", path.display())]
    Corrupt {
        /// The segment file.
        path: PathBuf,
        /// Where in it.
        offset: u64,
        /// What is wrong.
        reason: &'static str,
    },
    /// The callback of [`Disk::open`] refused a record.
    #[error("replaying a record failed")]
    Replay(#[source] ReplayError),
    /// A record does not fit a segment.
    #[error("a record of {len} bytes does not fit a segment")]
    TooLarge {
        /// Its body's length.
        len: usize,
    },
    /// Writing records `first..=last` failed. They may be on disk or not;
    /// a restart may find them. Later records go on.
    #[error("writing records {first} to {last} failed")]
    Write {
        /// The first record of the write.
        first: u64,
        /// The last.
        last: u64,
        /// Why.
        #[source]
        source: io::Error,
    },
    /// The disk threads stopped.
    #[error("the disk threads stopped")]
    Closed,
    /// A [`Config`] field is out of range.
    #[error("config field {0} is out of range")]
    Config(&'static str),
}
