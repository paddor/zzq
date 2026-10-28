//! Segment files in a journal directory: `<id as 16 hex digits>.seg`, and
//! spares, segments trimmed and kept for later ones: `<id>.spare`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rustix::fs::{FallocateFlags, FlockOperation};
use rustix::io::Errno;

use crate::format;

/// The largest segment a journal writes or reads.
pub(crate) const MAX_BYTES: u64 = 1 << 30;
const SUFFIX: &str = "seg";
const SPARE: &str = "spare";
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
/// It takes a spare when there is one, so direct writes land in blocks
/// written before. They cost no metadata write; blocks only allocated
/// do. Otherwise `zero` writes zeros through it, for a first segment
/// that nothing writes beside, and without `zero` the blocks are only
/// allocated: zeros would hold up the writes to the active segment.
pub(crate) fn create(dir: &Path, id: u64, capacity: u64, zero: bool) -> io::Result<Segment> {
    let done = path(dir, id);
    let partial = done.with_extension(PARTIAL);
    {
        let file = match reuse(dir, id, capacity, &partial)? {
            Some(file) => file,
            None => fresh(&partial, capacity, zero)?,
        };
        file.write_all_at(&format::segment_header(id, capacity), 0)?;
        file.sync_all()?;
    }
    fs::rename(&partial, &done)?;
    sync_dir(dir)?;
    open(dir, id, capacity)
}

/// The newest spare older than segment `id`, renamed to `to`. Removes the
/// spares it passes over: one not older could hold groups that name `id`,
/// and one of another capacity does not fit.
fn reuse(dir: &Path, id: u64, capacity: u64, to: &Path) -> io::Result<Option<File>> {
    let mut spares = spares(dir)?;
    while let Some(old) = spares.pop() {
        let from = spare_path(dir, old);
        if old < id && fs::metadata(&from)?.len() == capacity {
            fs::rename(&from, to)?;
            return OpenOptions::new().write(true).open(to).map(Some);
        }
        fs::remove_file(&from)?;
    }
    Ok(None)
}

/// A new file at `to` of `capacity` bytes, of zeros or allocated blocks.
fn fresh(to: &Path, capacity: u64, zero: bool) -> io::Result<File> {
    let mut file = File::create(to)?;
    if !zero {
        match rustix::fs::fallocate(&file, FallocateFlags::empty(), 0, capacity) {
            Ok(()) => return Ok(file),
            // Zeros instead.
            Err(Errno::OPNOTSUPP) => {}
            Err(e) => return Err(e.into()),
        }
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
/// rest stay in a row. They replace the spares left: about as many new
/// segments come before the next trim.
pub(crate) fn trim(dir: &Path, before: u64) -> io::Result<()> {
    let trimmed: Vec<u64> = list(dir)?.into_iter().filter(|&id| id < before).collect();
    if trimmed.is_empty() {
        return Ok(());
    }
    for id in spares(dir)? {
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
    {
        let mut file = File::create(&partial)?;
        let bytes = group.to_be_bytes();
        file.write_all(&bytes)?;
        file.write_all(&xxhash_rust::xxh3::xxh3_64(&bytes).to_be_bytes())?;
        file.sync_all()?;
    }
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

/// Takes `dir`'s lock for as long as the file lives. Fails if another
/// journal holds it.
pub(crate) fn lock(dir: &Path) -> io::Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(dir.join("LOCK"))?;
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
    Ok(file)
}
