//! Segment files in a journal directory: `<id as 16 hex digits>.seg`, and
//! spares, segments trimmed and kept for later ones: `<id>.spare`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use rustix::fs::{FallocateFlags, FlockOperation};
use rustix::io::Errno;

use crate::format;

/// The largest segment a journal writes or reads.
pub(crate) const MAX_BYTES: u64 = 1 << 30;
const SUFFIX: &str = "seg";
const SPARE: &str = "spare";
/// The id of a spare allocated to stay back, older than any segment.
const RESERVE: u64 = 0;
/// A segment being made; a crash leaves it behind, and [`list`] removes it.
const PARTIAL: &str = "tmp";
/// Zeros written per call while a segment is made.
const ZEROS: usize = 1 << 20;

/// A segment open for direct writes.
#[derive(Debug, Clone)]
pub(crate) struct Segment {
    pub(crate) id: u64,
    pub(crate) capacity: u64,
    pub(crate) file: Arc<File>,
}

pub(crate) fn path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:016x}.{SUFFIX}"))
}

fn spare_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:016x}.{SPARE}"))
}

/// Segment ids in `dir`, oldest first. Removes segments a crash left half
/// made.
pub(crate) fn list(dir: &Path) -> io::Result<Vec<u64>> {
    ids(dir, SUFFIX)
}

/// Spare ids in `dir`, oldest first.
pub(crate) fn spares(dir: &Path) -> io::Result<Vec<u64>> {
    ids(dir, SPARE)
}

/// Ids of the files in `dir` named `<id>.<suffix>`, oldest first. Removes
/// segments a crash left half made.
fn ids(dir: &Path, suffix: &str) -> io::Result<Vec<u64>> {
    let mut ids = Vec::new();
    for entry in fs::read_dir(dir)? {
        let path = entry?.path();
        let (Some(stem), Some(ext)) = (
            path.file_stem().and_then(|s| s.to_str()),
            path.extension().and_then(|s| s.to_str()),
        ) else {
            continue;
        };
        let Some(id) = (stem.len() == 16)
            .then(|| u64::from_str_radix(stem, 16).ok())
            .flatten()
        else {
            continue;
        };
        match ext {
            PARTIAL => fs::remove_file(&path)?,
            _ if ext == suffix => ids.push(id),
            _ => {}
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

/// Makes segment `id` of `capacity` bytes, synced, then renamed into place.
///
/// It takes the newest spare when there is one, so direct writes land in
/// blocks written before. They cost no metadata write; blocks only
/// allocated do. Otherwise `zero` writes zeros through it, for a first
/// segment that nothing writes beside, and without `zero` the blocks are
/// only allocated: zeros would hold up the writes to the active segment.
///
/// One spare stays back, so a snapshot can always start a segment and
/// the trim behind it can make room on a full device: only `last_spare`,
/// for the records after a roll, takes it. Without one, it is allocated
/// beside the segment.
pub(crate) fn create(
    dir: &Path,
    id: u64,
    capacity: u64,
    zero: bool,
    last_spare: bool,
) -> io::Result<Segment> {
    let done = path(dir, id);
    let partial = done.with_extension(PARTIAL);
    let spares = fitting(dir, id, capacity)?;
    let keep = usize::from(!last_spare);
    let reserve = (spares.len() < keep).then(|| spare_path(dir, RESERVE));
    let from = spares
        .last()
        .filter(|_| spares.len() > keep)
        .map(|&old| spare_path(dir, old));
    let made = made(&partial, || {
        if let Some(reserve) = &reserve {
            fresh(reserve, capacity, false)?;
        }
        prepare(id, capacity, zero, from.as_deref(), &partial)
    });
    if let (Err(_), Some(reserve)) = (&made, &reserve) {
        let _ = fs::remove_file(reserve);
    }
    made?;
    fs::rename(&partial, &done)?;
    sync_dir(dir)?;
    open(dir, id, capacity)
}

/// Runs `make`, which fills `partial`. A failure removes it, which gives
/// back the space it took on a full device.
fn made(partial: &Path, make: impl FnOnce() -> io::Result<()>) -> io::Result<()> {
    make().inspect_err(|_| {
        let _ = fs::remove_file(partial);
    })
}

/// Segment `id` at `to`, from spare `from` or a new file, with its header,
/// synced.
fn prepare(id: u64, capacity: u64, zero: bool, from: Option<&Path>, to: &Path) -> io::Result<()> {
    let file = match from {
        Some(from) => {
            fs::rename(from, to)?;
            OpenOptions::new().write(true).open(to)?
        }
        None => fresh(to, capacity, zero)?,
    };
    file.write_all_at(&format::segment_header(id, capacity), 0)?;
    file.sync_all()
}

/// The spares segment `id` can take, oldest first. Removes the others:
/// one not older could hold groups that name `id`, and one of another
/// capacity does not fit.
fn fitting(dir: &Path, id: u64, capacity: u64) -> io::Result<Vec<u64>> {
    let mut fit = Vec::new();
    for old in spares(dir)? {
        let from = spare_path(dir, old);
        if old < id && fs::metadata(&from)?.len() == capacity {
            fit.push(old);
        } else {
            fs::remove_file(&from)?;
        }
    }
    Ok(fit)
}

/// A new file at `to` of `capacity` bytes, of zeros or allocated blocks.
/// The blocks come first, so a full device fails before any zeros.
fn fresh(to: &Path, capacity: u64, zero: bool) -> io::Result<File> {
    let mut file = File::create(to)?;
    let zero = match rustix::fs::fallocate(&file, FallocateFlags::empty(), 0, capacity) {
        Ok(()) => zero,
        Err(Errno::OPNOTSUPP) => true,
        Err(e) => return Err(e.into()),
    };
    if !zero {
        return Ok(file);
    }
    let zeros = vec![0; ZEROS];
    let mut left = capacity;
    while left > 0 {
        let n = left.min(ZEROS as u64) as usize;
        file.write_all(&zeros[..n])?;
        left -= n as u64;
    }
    Ok(file)
}

/// Opens segment `id` for direct writes that are durable once they finish.
pub(crate) fn open(dir: &Path, id: u64, capacity: u64) -> io::Result<Segment> {
    let file = OpenOptions::new()
        .write(true)
        .custom_flags(libc::O_DIRECT | libc::O_DSYNC)
        .open(path(dir, id))
        .map_err(|e| {
            if e.raw_os_error() == Some(libc::EINVAL) {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the journal directory does not support O_DIRECT",
                )
            } else {
                e
            }
        })?;
    Ok(Segment {
        id,
        capacity,
        file: Arc::new(file),
    })
}

/// The whole of segment `id`, if it is no larger than [`MAX_BYTES`].
pub(crate) fn read(dir: &Path, id: u64) -> io::Result<Vec<u8>> {
    let mut file = File::open(path(dir, id))?;
    let len = file.metadata()?.len();
    if len > MAX_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "segment larger than any journal writes",
        ));
    }
    let mut bytes = Vec::with_capacity(len as usize);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

pub(crate) fn remove(dir: &Path, id: u64) -> io::Result<()> {
    fs::remove_file(path(dir, id))?;
    sync_dir(dir)
}

/// Turns the segments before `before` into spares, oldest first, so the
/// rest stay in a row. They replace the spares left but the newest, which
/// stays back: about as many new segments come before the next trim.
pub(crate) fn trim(dir: &Path, before: u64) -> io::Result<()> {
    let trimmed: Vec<u64> = list(dir)?.into_iter().filter(|&id| id < before).collect();
    if trimmed.is_empty() {
        return Ok(());
    }
    let mut left = spares(dir)?;
    left.pop();
    for id in left {
        fs::remove_file(spare_path(dir, id))?;
    }
    for id in trimmed {
        fs::rename(path(dir, id), spare_path(dir, id))?;
    }
    sync_dir(dir)
}

/// The file that names the last group known to be on disk: the group
/// number and its XXH3, big endian.
const MARK: &str = "DURABLE";

/// Notes that the groups up to `group` are on disk.
pub(crate) fn write_mark(dir: &Path, group: u64) -> io::Result<()> {
    let done = dir.join(MARK);
    let partial = done.with_extension(PARTIAL);
    made(&partial, || {
        let mut file = File::create(&partial)?;
        let bytes = group.to_be_bytes();
        file.write_all(&bytes)?;
        file.write_all(&xxhash_rust::xxh3::xxh3_64(&bytes).to_be_bytes())?;
        file.sync_all()
    })?;
    fs::rename(&partial, &done)?;
    sync_dir(dir)
}

/// The group the mark names, 0 without one.
pub(crate) fn read_mark(dir: &Path) -> io::Result<u64> {
    let bytes = match fs::read(dir.join(MARK)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e),
    };
    let Some((group, sum)) = bytes.split_first_chunk::<8>() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "durable mark too short",
        ));
    };
    if sum != xxhash_rust::xxh3::xxh3_64(group).to_be_bytes() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "durable mark damaged",
        ));
    }
    Ok(u64::from_be_bytes(*group))
}

/// Makes the names in `dir` durable.
pub(crate) fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// A journal's directory, locked for as long as this lives. The journal
/// shares it with the writes and file work it starts.
#[derive(Debug)]
pub(crate) struct Dir {
    path: PathBuf,
    _lock: File,
}

/// Directories whose journal was dropped while work it started still
/// holds them. A journal opened there, on any disk of this process, waits
/// for that work.
#[derive(Debug)]
struct Closing {
    dirs: Mutex<Vec<PathBuf>>,
    done: Condvar,
}

static CLOSING: Closing = Closing {
    dirs: Mutex::new(Vec::new()),
    done: Condvar::new(),
};

impl Closing {
    fn dirs(&self) -> MutexGuard<'_, Vec<PathBuf>> {
        self.dirs.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Waits until no work of a dropped journal holds `path`.
    fn wait(&self, path: &Path) {
        let mut dirs = self.dirs();
        while dirs.iter().any(|d| d == path) {
            dirs = self.done.wait(dirs).unwrap_or_else(PoisonError::into_inner);
        }
    }
}

impl Dir {
    /// Takes the lock of `path`, a canonical path, once the work of a
    /// journal dropped there is done. Fails if another journal holds it.
    pub(crate) fn lock(path: &Path) -> io::Result<Dir> {
        CLOSING.wait(path);
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(path.join("LOCK"))?;
        rustix::fs::flock(&file, FlockOperation::NonBlockingLockExclusive).map_err(|e| {
            if e == rustix::io::Errno::WOULDBLOCK {
                io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "another journal uses this directory",
                )
            } else {
                e.into()
            }
        })?;
        Ok(Dir {
            path: path.to_owned(),
            _lock: file,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Notes that its journal was dropped: a journal opened here waits
    /// until the work it left lets go.
    pub(crate) fn closed(&self) {
        CLOSING.dirs().push(self.path.clone());
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        CLOSING.dirs().retain(|d| *d != self.path);
        CLOSING.done.notify_all();
    }
}
