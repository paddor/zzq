//! The state file: written at shutdown, loaded at start.
//!
//! Shutdown closes every connection as lost, so wills and session expiry
//! run as for any lost connection, and lanes take no new input. Deliveries
//! already on their way between lanes land before a marker each lane sends
//! last. Then the lanes write their sessions into one image, all at once,
//! while the broker writes the retained messages. The image goes to a
//! temporary file as Zstandard frames, each ending on a record so the
//! writers can interleave them. The file is synced, then renamed over the
//! state file.
//!
//! Start decompresses the image, hands each session to the lane its client
//! ID maps to, and renames the file to `<name>.loaded`, so a later crash
//! cannot bring back state that changed since.

use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use tokio::sync::oneshot;
use zrip::CompressContext;

use crate::broker::{LaneTx, Shared, lane_for};
use crate::lane::{Control, Dump, SyncAck};
use crate::msg::Msg;
use crate::session::Session;
use crate::state::{Clock, ImageWriter, Record, SessionState, read_image};
use crate::{Error, Result};

/// Image bytes per frame, or a little more, as a frame ends on a record.
/// Frames this size compress about as well as the whole image.
const FRAME: usize = 64 << 10;
/// Zstandard level: fast, and close to the ratio of higher ones.
const LEVEL: i32 = 1;

/// The temporary file, shared by the writers.
type Out = Arc<Mutex<BufWriter<File>>>;

/// One writer's records, compressed a frame at a time.
pub(crate) struct Part {
    image: ImageWriter<Vec<u8>>,
    /// Boxed, as its tables are large.
    zip: Box<CompressContext>,
    out: Out,
}

impl fmt::Debug for Part {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Part").finish_non_exhaustive()
    }
}

impl Part {
    fn new(out: &Out, image: ImageWriter<Vec<u8>>) -> io::Result<Part> {
        Ok(Part {
            image,
            zip: Box::new(CompressContext::new(LEVEL).map_err(io::Error::other)?),
            out: out.clone(),
        })
    }

    pub(crate) fn session(&mut self, s: &Session) -> io::Result<()> {
        self.image.session(s)?;
        self.spill(FRAME)
    }

    fn retained(&mut self, m: &Msg) -> io::Result<()> {
        self.image.retained(m)?;
        self.spill(FRAME)
    }

    /// Writes the records left as a last frame.
    pub(crate) fn flush(&mut self) -> io::Result<()> {
        self.spill(1)
    }

    /// Ends the image.
    fn finish(mut self) -> io::Result<()> {
        self.image.end()?;
        self.flush()
    }

    /// Compresses the records so far into a frame, once they are `min`
    /// bytes or more.
    fn spill(&mut self, min: usize) -> io::Result<()> {
        let raw = self.image.get_mut();
        if raw.len() < min {
            return Ok(());
        }
        let frame = self.zip.compress(raw).map_err(io::Error::other)?;
        self.out
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write_all(&frame)?;
        raw.clear();
        Ok(())
    }
}

/// Writes the state of every lane and the retained messages to `path`.
pub(crate) async fn save(shared: &Shared, path: &Path) -> Result<()> {
    quiesce(shared).await?;
    let tmp = with_suffix(path, ".tmp");
    let written = write(shared, &tmp).await;
    if written.is_err() {
        let _ = fs::remove_file(&tmp);
        return written;
    }
    fs::rename(&tmp, path).map_err(Error::State)?;
    sync_dir(path).map_err(Error::State)
}

/// Closes every connection, then waits for deliveries between lanes.
async fn quiesce(shared: &Shared) -> Result<()> {
    let mut closed = Vec::with_capacity(shared.lanes.len());
    for lane in &shared.lanes {
        let (tx, rx) = oneshot::channel();
        push(lane, Control::Quiesce(tx))?;
        closed.push(rx);
    }
    for rx in closed {
        rx.await.map_err(|_| Error::Closed)?;
    }
    let n = shared.lanes.len();
    let drained = Arc::new(SyncAck::default());
    drained.expect(n * (n - 1));
    for lane in &shared.lanes {
        push(lane, Control::Drain(drained.clone()))?;
    }
    drained.wait().await;
    Ok(())
}

async fn write(shared: &Shared, tmp: &Path) -> Result<()> {
    let file = File::create(tmp).map_err(Error::State)?;
    let out: Out = Arc::new(Mutex::new(BufWriter::new(file)));
    let clock = Clock::now();
    let header = ImageWriter::new(Vec::new(), clock).map_err(Error::State)?;
    let mut head = Part::new(&out, header).map_err(Error::State)?;
    // The header goes first, then frames from every writer as they fill.
    head.flush().map_err(Error::State)?;
    let mut lanes = Vec::with_capacity(shared.lanes.len());
    for lane in &shared.lanes {
        let part = Part::new(&out, ImageWriter::part(Vec::new(), clock)).map_err(Error::State)?;
        let (reply, rx) = oneshot::channel();
        push(lane, Control::Dump(Box::new(Dump { part, reply })))?;
        lanes.push(rx);
    }
    // A persistence backend keeps retained messages itself.
    let mut written = if shared.opts.persistence.is_none() {
        retained(shared, &mut head).map_err(Error::State)
    } else {
        Ok(())
    };
    for rx in lanes {
        let lane = rx.await.map_err(|_| Error::Closed);
        written = written.and(lane.and_then(|r| r.map_err(Error::State)));
    }
    written?;
    head.finish().map_err(Error::State)?;
    let mut file = out.lock().unwrap_or_else(PoisonError::into_inner);
    file.flush().map_err(Error::State)?;
    file.get_ref().sync_all().map_err(Error::State)
}

/// Writes the retained messages in topic order, so related ones share a
/// frame.
fn retained(shared: &Shared, head: &mut Part) -> io::Result<()> {
    let mut retained = shared
        .retained
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .messages();
    retained.sort_unstable_by(|a, b| a.topic().cmp(b.topic()));
    for m in &retained {
        head.retained(m)?;
    }
    Ok(())
}

/// Loads the state file, if there is one, into the lanes and the retained
/// store, then sets it aside.
pub(crate) async fn load(shared: &Shared, path: &Path) -> Result<()> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::State(e)),
    };
    let image = zrip::decompress(&data).map_err(damaged)?;
    drop(data);
    let records = read_image(&image, Clock::now()).map_err(damaged)?;
    drop(image);
    let n = shared.lanes.len();
    let mut sessions: Vec<Vec<SessionState>> = (0..n).map(|_| Vec::new()).collect();
    {
        let mut store = shared
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        for r in records {
            match r {
                Record::Session(st) => sessions[lane_for(&st.client_id, n)].push(*st),
                Record::Retained(m) => store.set(&m, false),
            }
        }
    }
    // Subscriptions are on every lane before the broker serves.
    let subscribed = Arc::new(SyncAck::default());
    let mut restored = Vec::with_capacity(n);
    for (lane, states) in shared.lanes.iter().zip(sessions) {
        let (tx, rx) = oneshot::channel();
        push(lane, Control::Restore(states, subscribed.clone(), tx))?;
        restored.push(rx);
    }
    for rx in restored {
        rx.await.map_err(|_| Error::Closed)?;
    }
    subscribed.wait().await;
    fs::rename(path, with_suffix(path, ".loaded")).map_err(Error::State)?;
    sync_dir(path).map_err(Error::State)
}

fn damaged(why: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> Error {
    Error::State(io::Error::new(io::ErrorKind::InvalidData, why))
}

fn push(lane: &LaneTx, c: Control) -> Result<()> {
    lane.control.push(c).map_err(|_| Error::Closed)
}

fn with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(suffix);
    PathBuf::from(name)
}

/// Makes a rename in the directory of `path` durable.
fn sync_dir(path: &Path) -> io::Result<()> {
    let dir = path
        .parent()
        .filter(|d| !d.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    File::open(dir)?.sync_all()
}
