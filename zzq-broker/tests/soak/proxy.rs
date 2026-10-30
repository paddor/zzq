use std::net::SocketAddr;
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};

/// A bounded TCP relay. Changing its gate closes all existing streams.
pub(super) struct Proxy {
    pub addr: SocketAddr,
    gate: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Proxy {
    pub(super) async fn new(targets: Vec<SocketAddr>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (gate, enabled) = watch::channel(true);
        let task = tokio::spawn(serve(listener, targets, enabled));
        Self { addr, gate, task }
    }

    pub(super) fn enable(&self, enabled: bool) {
        self.gate.send_replace(enabled);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve(listener: TcpListener, targets: Vec<SocketAddr>, enabled: watch::Receiver<bool>) {
    let mut streams = JoinSet::new();
    loop {
        tokio::select! {
            // Accept and join are cancel-safe; dropping JoinSet aborts all relays.
            accepted = listener.accept(), if streams.len() < 128 => {
                let (stream, _) = accepted.unwrap();
                if *enabled.borrow() {
                    streams.spawn(relay(stream, targets.clone(), enabled.clone()));
                }
            }
            result = streams.join_next(), if !streams.is_empty() => { result.unwrap().unwrap(); }
        }
    }
}

async fn relay(
    mut client: TcpStream,
    targets: Vec<SocketAddr>,
    mut enabled: watch::Receiver<bool>,
) {
    for addr in targets {
        if !*enabled.borrow_and_update() {
            return;
        }
        if let Ok(Ok(mut upstream)) =
            tokio::time::timeout(Duration::from_millis(200), TcpStream::connect(addr)).await
        {
            tokio::select! {
                // A gate change deliberately cancels the byte stream and drops both sockets.
                _ = tokio::io::copy_bidirectional(&mut client, &mut upstream) => {}
                _ = enabled.changed() => {}
            }
            return;
        }
    }
}
