//! Opening a broker: its options checked, its journals or cluster opened
//! with what they hold, and its lanes brought up.

use std::sync::Arc;

use tokio::sync::oneshot;

use super::Shared;
use crate::disk::{self, DataDir};
use crate::lane::LaneLog;
use crate::options::BrokerOptions;
use crate::retain::{Retained, STRIPES};
use crate::state::handoff::LaneState;
use crate::{Error, Result};

/// Brings the installed lanes up: their journals' completions, the state
/// from the data directory, the cluster node, the state file and `$SYS`.
pub(super) async fn start(shared: &Arc<Shared>, sessions: Vec<LaneState>) -> Result<()> {
    if shared.journals() {
        complete_writes(shared).await?;
    }
    if shared.data.is_some() {
        crate::dump::restore(shared, sessions).await?;
        crate::dump::resume(shared).await?;
    }
    if let Some(cluster) = &shared.opts.cluster {
        crate::cluster::start(shared, cluster).await?;
    }
    if let Some(path) = shared
        .opts
        .state_file
        .as_ref()
        .filter(|_| !shared.clustered())
    {
        crate::dump::load(shared, path).await?;
    }
    if let Some(every) = shared.opts.sys_interval {
        let s = shared.clone();
        shared
            .ctx
            .lane(0)
            .call(move || {
                let task = tokio::task::spawn_local(crate::sys::run(s.clone(), every));
                s.own_task(task.abort_handle());
            })
            .await?;
    }
    Ok(())
}

/// What the data directory holds, or empty stores without one.
pub(crate) struct Opened {
    pub(crate) retained: Retained,
    pub(crate) data: Option<DataDir>,
    pub(crate) cluster: Option<Arc<crate::cluster::Groups>>,
    pub(crate) lane_logs: Box<[LaneLog]>,
    /// The sessions of each lane.
    pub(crate) sessions: Vec<LaneState>,
}

/// Opens the journals in `opts.data_dir`, if it is set, for `n` lanes,
/// with what they hold.
pub(super) async fn open_data(opts: &BrokerOptions, n: usize) -> Result<Opened> {
    if opts.cluster.is_some() {
        return Ok(crate::cluster::open(opts, n));
    }
    let Some(path) = opts.data_dir.clone() else {
        return Ok(Opened {
            retained: Retained::new(opts.max_retained),
            data: None,
            cluster: None,
            lane_logs: (0..n).map(|i| LaneLog::new(i, None)).collect(),
            sessions: Vec::new(),
        });
    };
    let (max_packet, max, levels) = (
        opts.max_packet_size,
        opts.max_retained,
        opts.max_topic_levels,
    );
    // Replaying the journals reads files.
    blocking(move || {
        let data = DataDir::open(&path, max_packet)?;
        let retained = Retained::open(&data, max, levels)?;
        let (logs, sessions) = disk::open_lanes(&data, n, &retained)?
            .into_iter()
            .enumerate()
            .map(|(i, (log, sessions))| (LaneLog::new(i, Some(log)), sessions))
            .unzip::<_, _, Vec<_>, _>();
        Ok(Opened {
            retained,
            data: Some(data),
            cluster: None,
            lane_logs: logs.into(),
            sessions,
        })
    })
    .await?
    .map_err(Error::DataDir)
}

/// Starts the task per retained stripe that takes its finished writes,
/// spread over the lanes.
pub(super) async fn complete_writes(shared: &Arc<Shared>) -> Result<()> {
    for i in 0..STRIPES {
        let s = shared.clone();
        shared
            .ctx
            .lane(i % shared.lanes.len())
            .call(move || {
                let task = tokio::task::spawn_local({
                    let s = s.clone();
                    async move { s.retained.complete(i).await }
                });
                s.own_task(task.abort_handle());
            })
            .await?;
    }
    Ok(())
}

/// Runs `f` on a thread of its own, for file work too long for a lane,
/// with or without a Tokio runtime around the caller.
pub(super) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> T + Send + 'static,
) -> Result<T> {
    let (tx, rx) = oneshot::channel();
    std::thread::Builder::new()
        .name("zzq-files".to_owned())
        .spawn(move || {
            let _ = tx.send(f());
        })
        .map_err(Error::DataDir)?;
    rx.await.map_err(|_| Error::Closed)
}

/// Refuses options the broker cannot work with.
pub(super) fn check(o: &BrokerOptions) -> Result<()> {
    if o.sys_interval.is_some_and(|d| d.is_zero()) {
        return Err(Error::InvalidOption("sys_interval"));
    }
    if o.sn.retry.is_zero() {
        return Err(Error::InvalidOption("sn.retry"));
    }
    if o.sn.max_keep_alive == 0 {
        return Err(Error::InvalidOption("sn.max_keep_alive"));
    }
    // A data directory keeps the sessions.
    if o.data_dir.is_some() && o.state_file.is_some() {
        return Err(Error::InvalidOption("state_file"));
    }
    // A cluster keeps its state in the RAM of two nodes, not on disk.
    if o.cluster.is_some() && o.data_dir.is_some() {
        return Err(Error::InvalidOption("cluster"));
    }
    // A v5 client must not get a Receive Maximum of 0.
    if o.receive_maximum == 0 {
        return Err(Error::InvalidOption("receive_maximum"));
    }
    Ok(())
}
