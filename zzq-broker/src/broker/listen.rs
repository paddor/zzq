//! Listeners and the connections they accept, which move to the lane
//! that owns their client ID.

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use zzq_tokio::Endpoint;
use zzq_tokio::transport::Listener;

use super::{Shared, lane_for, lane_ref};
use crate::conn::ConnDriver;
use crate::{Error, Result};

pub(super) async fn accept_loop(shared: Arc<Shared>, mut listener: Listener) {
    loop {
        let stream = match listener.accept().await {
            Ok(s) => s,
            Err(zzq_tokio::Error::Closed) => return,
            Err(_) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
                continue;
            }
        };
        tokio::task::spawn_local(admit(shared.clone(), stream));
    }
}

/// Binds `endpoint` on the next lane and returns the bound endpoint and
/// the handle of the task that accepts on it, which the broker owns.
pub(crate) async fn listen(
    shared: &Arc<Shared>,
    endpoint: Endpoint,
) -> Result<(Endpoint, AbortHandle)> {
    let (tx, rx) = oneshot::channel();
    let shared = shared.clone();
    let lane = shared.ctx.next_lane().clone();
    lane.spawn(move || async move {
        if matches!(endpoint, Endpoint::Udp { .. }) {
            bind_udp(shared, endpoint, tx).await;
            return;
        }
        let listener = match Listener::bind(&shared.ctx, &endpoint, &shared.opts.transport).await {
            Ok(l) => l,
            Err(e) => {
                let _ = tx.send(Err(e));
                return;
            }
        };
        let bound = listener.endpoint();
        let task = tokio::task::spawn_local(accept_loop(shared.clone(), listener));
        shared.own_task(task.abort_handle());
        let _ = tx.send(Ok((bound, task.abort_handle())));
    })?;
    Ok(rx.await.map_err(|_| Error::Closed)??)
}

/// Binds `udp://` and runs the MQTT-SN gateway on the current lane.
async fn bind_udp(
    shared: Arc<Shared>,
    endpoint: Endpoint,
    tx: oneshot::Sender<zzq_tokio::Result<(Endpoint, AbortHandle)>>,
) {
    let socket = match zzq_tokio::transport::udp::bind(&endpoint).await {
        Ok(s) => s,
        Err(e) => {
            let _ = tx.send(Err(e));
            return;
        }
    };
    let bound = match (&endpoint, socket.local_addr()) {
        (Endpoint::Udp { host, .. }, Ok(a)) => Endpoint::Udp {
            host: host.clone(),
            port: a.port(),
        },
        _ => endpoint.clone(),
    };
    let task = tokio::task::spawn_local(crate::sn::gateway(shared.clone(), socket));
    shared.own_task(task.abort_handle());
    let _ = tx.send(Ok((bound, task.abort_handle())));
}

/// Reads CONNECT on the accepting lane, then moves the connection to the
/// lane that owns its client ID.
pub(crate) async fn admit(shared: Arc<Shared>, stream: zzq_tokio::transport::Stream) {
    let max = shared.opts.max_packet_size;
    let mut buf = bytes::BytesMut::new();
    // Stops at a whole CONNECT, a decode error, or a first byte that is not
    // CONNECT, which the connection refuses without its body.
    let done = |b: &[u8]| {
        b.first().is_some_and(|&x| x >> 4 != 1)
            || !matches!(zzq_proto::decode(b, zzq_proto::Version::V5, max), Ok(None))
    };
    let read =
        tokio::time::timeout(shared.opts.connect_timeout, stream.prefetch(&mut buf, done)).await;
    let Ok(Ok(stream)) = read else {
        return;
    };
    let peer_cert = stream.peer_cert();
    let lanes = shared.ctx.lanes();
    let lane = match zzq_proto::decode(&buf, zzq_proto::Version::V5, max) {
        Ok(Some((zzq_proto::Packet::Connect(c), _))) if !c.client_id.is_empty() => {
            shared.ctx.lane(lane_for(c.client_id, lanes))
        }
        _ => shared.ctx.next_lane(),
    };
    let (id, index) = (shared.id, lane.index());
    let _ = lane.spawn(move || async move {
        if let Some(lane) = lane_ref(id, index) {
            let driver = ConnDriver {
                lane,
                initial: buf,
                peer_cert,
            };
            let _ = stream.drive(driver).await;
        }
    });
}
