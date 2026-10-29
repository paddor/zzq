//! Links between the nodes over OMQ: one PEER socket per node, bound at
//! its own endpoint, with its node ID as its identity. Of each pair, the
//! node listed later connects to the other, so a pair shares one
//! connection. The socket runs on a thread of its own.

use std::io;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use omq_tokio::{Endpoint, IdentitySocket, Message, Options, Socket, SocketType};
use tokio::sync::mpsc;
use zzq_cluster::NodeId;
use zzq_cluster::node::Outgoing;

use crate::options::ClusterLinks;
use crate::{Error, Result};

/// Frames queued each way. A full queue drops; the node sends again.
const QUEUE: usize = 4096;

/// Links for node `local` of `nodes`, which lists all three with their
/// endpoints, in the same order on every node.
///
/// # Errors
///
/// [`Error::InvalidOption`] if `local` is not in `nodes` or an endpoint
/// does not parse, and [`Error::ClusterLinks`] if the socket cannot bind
/// its endpoint or the thread does not start.
pub fn omq_links(local: NodeId, nodes: &[(NodeId, String)]) -> Result<ClusterLinks> {
    let at = nodes
        .iter()
        .position(|(id, _)| *id == local)
        .ok_or(Error::InvalidOption("cluster"))?;
    let parse = |s: &str| {
        s.parse::<Endpoint>()
            .map_err(|_| Error::InvalidOption("cluster"))
    };
    let bind = parse(&nodes[at].1)?;
    let connect = nodes[..at]
        .iter()
        .map(|(_, e)| parse(e))
        .collect::<Result<Vec<_>>>()?;
    let (outbound, out) = mpsc::channel(QUEUE);
    let (to_node, inbound) = mpsc::channel(QUEUE);
    let (ready, bound) = std::sync::mpsc::channel();
    std::thread::Builder::new()
        .name("zzq-cluster-links".to_owned())
        .spawn(move || {
            let rt = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    let _ = ready.send(Err(e));
                    return;
                }
            };
            let at = Addresses {
                local,
                bind,
                connect,
            };
            rt.block_on(run(at, out, to_node, ready));
        })
        .map_err(Error::ClusterLinks)?;
    bound
        .recv()
        .map_err(|_| Error::ClusterLinks(io::ErrorKind::BrokenPipe.into()))?
        .map_err(Error::ClusterLinks)?;
    Ok(ClusterLinks {
        outbound,
        inbound: Arc::new(Mutex::new(Some(inbound))),
    })
}

/// Binds and connects, says whether the bind worked, then moves frames
/// until the node is gone.
/// Where a node's socket is: its identity, its endpoint, and those it
/// connects to.
struct Addresses {
    local: NodeId,
    bind: Endpoint,
    connect: Vec<Endpoint>,
}

async fn run(
    at: Addresses,
    out: mpsc::Receiver<Outgoing>,
    to_node: mpsc::Sender<[Bytes; 3]>,
    ready: std::sync::mpsc::Sender<io::Result<()>>,
) {
    let identity = Bytes::copy_from_slice(at.local.as_bytes());
    let socket = Socket::new(SocketType::Peer, Options::default().identity(identity));
    let peer = match open(&socket, at.bind, at.connect).await {
        Ok(peer) => peer,
        Err(e) => {
            let _ = ready.send(Err(e));
            return;
        }
    };
    let _ = ready.send(Ok(()));
    tokio::select! {
        () = send(&peer, out) => {}
        () = recv(&peer, &to_node) => {}
    }
}

async fn open(
    socket: &Socket,
    bind: Endpoint,
    connect: Vec<Endpoint>,
) -> io::Result<IdentitySocket> {
    socket.bind(bind).await.map_err(io::Error::other)?;
    for e in connect {
        // Connecting retries in the background until the peer listens.
        socket.connect(e).await.map_err(io::Error::other)?;
    }
    socket.identity_routing().map_err(io::Error::other)
}

/// Sends each node's frames to it, until the node stops sending.
async fn send(peer: &IdentitySocket, mut out: mpsc::Receiver<Outgoing>) {
    while let Some(Outgoing { to, frames }) = out.recv().await {
        // A peer not connected or not keeping up drops: the node resends.
        let _ = peer.try_send_to(to.as_bytes(), Message::multipart(frames));
    }
}

/// Hands frames from the peers to the node, until it is gone.
async fn recv(peer: &IdentitySocket, to_node: &mpsc::Sender<[Bytes; 3]>) {
    loop {
        let Ok((_, mut m)) = peer.recv_from().await else {
            return;
        };
        let (Some(a), Some(b), Some(c)) = (m.pop_front(), m.pop_front(), m.pop_front()) else {
            continue;
        };
        match to_node.try_send([a, b, c]) {
            Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => {}
            Err(mpsc::error::TrySendError::Closed(_)) => return,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn id(n: u8) -> NodeId {
        NodeId::from_bytes([n; 16])
    }

    fn port() -> u16 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().port()
    }

    #[test]
    fn frames_reach_the_node_they_are_for_over_tcp() {
        let nodes: Vec<(NodeId, String)> = (1..=3)
            .map(|n| (id(n), format!("tcp://127.0.0.1:{}", port())))
            .collect();
        let links: Vec<ClusterLinks> = (1..=3).map(|n| omq_links(id(n), &nodes).unwrap()).collect();
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let mut third = links[2].inbound.lock().unwrap().take().unwrap();
            let frames = [
                Bytes::from_static(b"h"),
                Bytes::new(),
                Bytes::from_static(b"p"),
            ];
            // The link comes up in the background: send until one arrives.
            let got = loop {
                let out = Outgoing {
                    to: id(3),
                    frames: frames.clone(),
                };
                links[0].outbound.send(out).await.unwrap();
                if let Ok(got) =
                    tokio::time::timeout(Duration::from_millis(100), third.recv()).await
                {
                    break got.unwrap();
                }
            };
            assert_eq!(got, frames);
        });
    }

    #[test]
    fn a_node_not_listed_or_a_bad_endpoint_is_refused() {
        let nodes = vec![(id(1), "tcp://127.0.0.1:1".to_owned())];
        assert!(matches!(
            omq_links(id(2), &nodes),
            Err(Error::InvalidOption(_))
        ));
        let bad = vec![(id(1), "no endpoint".to_owned())];
        assert!(matches!(
            omq_links(id(1), &bad),
            Err(Error::InvalidOption(_))
        ));
    }
}
