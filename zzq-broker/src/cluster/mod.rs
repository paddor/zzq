//! A broker as one node of a three-node cluster: its journals replicate
//! through [`zzq_cluster::node::Node`] instead of going to disk. Only the
//! leader serves clients; a follower keeps the state the leader's ops
//! describe until it leads.

mod app;
mod image;
mod log;
mod replica;

use std::sync::Arc;

pub(crate) use log::{ClusterLog, Commits, GroupWriter, Groups, Journal};

use crate::broker::{Opened, Shared};
use crate::disk::Log;
use crate::lane::LaneLog;
use crate::options::{BrokerOptions, ClusterOptions, ClusterStart};
use crate::retain::Retained;
use crate::{Error, Result};
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

/// Starts the node and its replica task on the first lane.
pub(crate) async fn start(shared: &Arc<Shared>, opts: &ClusterOptions) -> Result<()> {
    let now = std::time::Duration::ZERO;
    let mut node = match opts.start {
        ClusterStart::Bootstrap => Node::bootstrap(opts.node, now),
        ClusterStart::Recover => Node::recover(opts.node, now),
    }
    .map_err(|_| Error::InvalidOption("cluster"))?;
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
    let s = shared.clone();
    shared
        .ctx
        .lane(0)
        .call(move || {
            let replica = replica::Replica::new(s.clone(), node, outbound, inbound);
            let task = tokio::task::spawn_local(replica.run());
            s.own_task(task.abort_handle());
        })
        .await?;
    Ok(())
}
