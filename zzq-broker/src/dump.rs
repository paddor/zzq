//! The state file: written at shutdown, loaded at start.
//!
//! Shutdown closes every connection as lost, so wills and session expiry
//! run as for any lost connection, and lanes take no new input. Deliveries
//! already on their way between lanes land before a marker each lane sends
//! last. Then each lane in turn writes its sessions into one image, and the
//! retained messages follow. The image goes to a temporary file, synced,
//! then renamed over the state file.
//!
//! Start loads the image, hands each session to the lane its client ID maps
//! to, and renames the file to `<name>.loaded`, so a later crash cannot
//! bring back state that changed since.

use std::ffi::OsString;
use std::fs::{self, File};
use std::io::{self, BufWriter};
use std::path::{Path, PathBuf};
use std::sync::{Arc, PoisonError};

use tokio::sync::oneshot;

use crate::broker::{LaneTx, Shared, lane_for};
use crate::lane::{Control, Dump, SyncAck};
use crate::state::{Clock, ImageWriter, Record, SessionState, read_image};
use crate::{Error, Result};

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
    let mut writer = ImageWriter::new(BufWriter::new(file), Clock::now()).map_err(Error::State)?;
    for lane in &shared.lanes {
        let (reply, rx) = oneshot::channel();
        push(lane, Control::Dump(Box::new(Dump { writer, reply })))?;
        writer = rx.await.map_err(|_| Error::Closed)?.map_err(Error::State)?;
    }
    // A persistence backend keeps retained messages itself.
    if shared.opts.persistence.is_none() {
        let retained = shared
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .messages();
        for m in &retained {
            writer.retained(m).map_err(Error::State)?;
        }
    }
    let file = writer
        .finish()
        .and_then(|w| w.into_inner().map_err(io::IntoInnerError::into_error))
        .map_err(Error::State)?;
    file.sync_all().map_err(Error::State)
}

/// Loads the state file, if there is one, into the lanes and the retained
/// store, then sets it aside.
pub(crate) async fn load(shared: &Shared, path: &Path) -> Result<()> {
    let data = match fs::read(path) {
        Ok(data) => data,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(Error::State(e)),
    };
    let records = read_image(&data, Clock::now())
        .map_err(|why| Error::State(io::Error::new(io::ErrorKind::InvalidData, why)))?;
    drop(data);
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
