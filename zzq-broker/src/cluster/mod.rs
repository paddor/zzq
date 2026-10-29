//! A broker as one node of a three-node cluster: its journals replicate
//! through [`zzq_cluster::node::Node`] instead of going to disk. Only the
//! leader serves clients; a follower keeps the state the leader's ops
//! describe until it leads.

mod app;
mod dump;
#[cfg(feature = "fuzzing")]
pub(crate) mod fuzzing;
mod image;
#[cfg(feature = "cluster")]
mod links;
mod log;
mod replica;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

#[cfg(feature = "cluster")]
pub use links::omq_links;
pub(crate) use log::{ClusterLog, Commits, GroupWriter, Groups, Journal};

use crate::broker::{Opened, Shared};
use crate::disk::Log;
use crate::lane::LaneLog;
use crate::options::{BrokerOptions, ClusterOptions, ClusterStart};
use crate::retain::Retained;
use crate::{Error, Result};
use app::Accumulators;
use zzq_cluster::node::Node;
use zzq_tokio::Endpoint;

/// Journals that replicate to the cluster, for `n` lanes.
pub(crate) fn open(opts: &BrokerOptions, n: usize) -> Opened {
    let groups = Arc::new(Groups::default());
    let journal = |j| Log::Cluster(ClusterLog::new(j, groups.clone()));
    Opened {
        retained: Retained::clustered(opts.max_retained, |s| journal(Journal::Retained(s as u8))),
        data: None,
        lane_logs: (0..n)
            .map(|i| LaneLog::new(i, Some(journal(Journal::Lane(i as u16)))))
            .collect(),
        cluster: Some(groups.clone()),
        sessions: Vec::new(),
    }
}

/// Registers a client endpoint, bound now if the node leads.
pub(crate) async fn bind(shared: &Arc<Shared>, endpoint: Endpoint) -> Result<Endpoint> {
    let groups = shared.cluster_groups();
    let (leading, epoch) = {
        let mut l = lock(&groups.listeners);
        l.endpoints.push(endpoint.clone());
        (l.leading, l.epoch)
    };
    if leading {
        let (_, task) = crate::broker::listen(shared, endpoint.clone()).await?;
        lock(&groups.listeners).own(task, epoch);
    }
    Ok(endpoint)
}

/// Binds every client endpoint: the node leads.
pub(crate) async fn open_listeners(shared: &Arc<Shared>) {
    let groups = shared.cluster_groups();
    let (endpoints, epoch) = {
        let mut l = lock(&groups.listeners);
        l.leading = true;
        l.epoch += 1;
        (l.endpoints.clone(), l.epoch)
    };
    for endpoint in endpoints {
        if let Ok((_, task)) = crate::broker::listen(shared, endpoint).await {
            lock(&groups.listeners).own(task, epoch);
        }
    }
}

/// Closes every listener: the node no longer leads.
pub(crate) fn close_listeners(shared: &Shared) {
    let groups = shared.cluster_groups();
    let mut l = lock(&groups.listeners);
    l.leading = false;
    l.epoch += 1;
    for task in l.bound.drain(..) {
        task.abort();
    }
}

fn lock<T>(m: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// Where the replica answers with its exit dump, if it has one.
pub(crate) type ExitReply = oneshot::Sender<Option<Bytes>>;

/// Stops the node for good: its listeners and connections close, the
/// latter as lost, and the replica writes its exit dump to `path`.
pub(crate) async fn save(shared: &Arc<Shared>, path: Option<&Path>) -> Result<()> {
    close_listeners(shared);
    crate::dump::quiesce(shared).await?;
    let exit = lock(&shared.cluster_groups().exit).take();
    let (tx, rx) = oneshot::channel();
    let dumped = match exit {
        Some(exit) if exit.send(tx).await.is_ok() => rx.await.ok().flatten(),
        _ => None,
    };
    match (path, dumped) {
        (Some(path), Some(bytes)) => dump::write(path, &bytes).map_err(Error::State),
        _ => Ok(()),
    }
}

/// The node and its application state: from an unused exit dump in the
/// state file, which is set aside then, or else as `opts.start` says.
fn boot(shared: &Shared, opts: &ClusterOptions) -> Result<(Node, Accumulators)> {
    let now = Duration::ZERO;
    let (lanes, levels) = (shared.lanes.len(), shared.opts.max_topic_levels);
    let mut config = opts.node;
    config.fingerprint = fingerprint(lanes, levels);
    if let Some(path) = shared.opts.state_file.as_deref()
        && let Some(bytes) = dump::read(path).map_err(Error::State)?
    {
        let d = dump::decode(&bytes).map_err(crate::dump::damaged)?;
        let node = Node::restore(config, &d.node, now).map_err(crate::dump::damaged)?;
        let parts = image::decode(&d.image).map_err(crate::dump::damaged)?;
        let app = Accumulators::from_image(lanes, levels, &parts).map_err(crate::dump::damaged)?;
        dump::set_aside(path).map_err(Error::State)?;
        return Ok((node, app));
    }
    let node = match opts.start {
        ClusterStart::Bootstrap => Node::bootstrap(config, now),
        ClusterStart::Recover => Node::recover(config, now),
    }
    .map_err(|_| Error::InvalidOption("cluster"))?;
    Ok((node, Accumulators::new(lanes, levels)))
}

/// What the replicated state depends on besides the records: nodes that
/// differ in it would replay the same ops into different states, so
/// they do not link.
fn fingerprint(lanes: usize, max_levels: usize) -> u64 {
    let mut b = b"zzq-broker cluster state 1".to_vec();
    for n in [lanes, crate::retain::STRIPES, max_levels] {
        b.extend_from_slice(&(n as u64).to_be_bytes());
    }
    xxhash_rust::xxh3::xxh3_64(&b)
}

/// Starts the node and its replica task on the first lane.
pub(crate) async fn start(shared: &Arc<Shared>, opts: &ClusterOptions) -> Result<()> {
    let (mut node, app) = boot(shared, opts)?;
    for &peer in opts.node.configuration.voters() {
        if peer != opts.node.local {
            let _ = node.connected(peer);
        }
    }
    let inbound = opts
        .links
        .inbound
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .ok_or(Error::InvalidOption("cluster"))?;
    let outbound = opts.links.outbound.clone();
    let (exit, exits) = mpsc::channel(1);
    *lock(&shared.cluster_groups().exit) = Some(exit);
    let s = shared.clone();
    shared
        .ctx
        .lane(0)
        .call(move || {
            let links = replica::Links {
                outbound,
                inbound,
                exits,
            };
            let replica = replica::Replica::new(s.clone(), node, app, links);
            let task = tokio::task::spawn_local(replica.run());
            s.own_task(task.abort_handle());
        })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn nodes_with_other_lanes_or_topic_depth_have_another_fingerprint() {
        let f = super::fingerprint;
        assert_eq!(f(4, 8), f(4, 8));
        assert_ne!(f(4, 8), f(3, 8));
        assert_ne!(f(4, 8), f(4, 9));
    }
}
