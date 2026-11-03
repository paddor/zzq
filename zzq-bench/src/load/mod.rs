//! Throughput and latency scenarios.

use std::io::{self, ErrorKind};
use std::time::Duration;

use crate::wire::Session;

mod latency;
mod throughput;

pub use latency::{Latency, Until, latency};
pub use throughput::{Throughput, throughput};

/// Traffic shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scenario {
    /// `pubs` publishers, each with its own topic and subscriber.
    Pairs,
    /// One publisher, `subs` subscribers on one topic.
    Fanout,
    /// `pubs` publishers on distinct topics, one wildcard subscriber.
    Fanin,
}

/// Load settings.
#[derive(Debug, Clone)]
pub struct Load {
    /// Broker address, `host:port`.
    pub addr: String,
    /// Speak v5 instead of v3.1.1.
    pub v5: bool,
    /// Publish QoS, 0 to 2.
    pub qos: u8,
    /// Payload size in bytes.
    pub size: usize,
    /// Publisher connections.
    pub pubs: usize,
    /// Subscriber connections.
    pub subs: usize,
    /// Time to run before measuring.
    pub warmup: Duration,
    /// Time to measure.
    pub measure: Duration,
    /// QoS 1/2 messages in flight per publisher.
    pub window: usize,
    /// Lanes of the zzq broker under test. Client IDs are chosen so pair `i`
    /// lands on lane `i % lanes`. Other brokers ignore this.
    pub lanes: usize,
    /// Pairs: put each publisher on the lane after its subscriber's.
    pub cross: bool,
    /// Fan-out with `lanes`: this many subscribers share the publisher's
    /// lane, the rest go on the next one. Without it, they take turns.
    pub local: Option<usize>,
    /// Topics per publisher, cycled message by message. Above 1,
    /// subscribers use `/#` filters.
    pub topics: usize,
    /// What the broker has to keep.
    pub keep: Keep,
    /// The PIDs of the broker's processes. Their CPU and memory, with
    /// their descendants', go into the results.
    pub broker_pids: Vec<u32>,
}

impl Default for Load {
    fn default() -> Self {
        Load {
            addr: "127.0.0.1:1883".to_owned(),
            v5: true,
            qos: 0,
            size: 16,
            pubs: 1,
            subs: 1,
            warmup: Duration::from_secs(1),
            measure: Duration::from_secs(3),
            window: 100,
            lanes: 1,
            cross: false,
            local: None,
            topics: 1,
            keep: Keep::default(),
            broker_pids: Vec::new(),
        }
    }
}

/// What the broker has to keep: the state a data directory puts on disk.
#[derive(Debug, Clone, Copy, Default)]
pub struct Keep {
    /// Publish with the retain flag.
    pub retain: bool,
    /// Subscribers have sessions that outlive their connection.
    pub sessions: bool,
}

impl Keep {
    /// The session a subscriber has.
    const fn session(self) -> Session {
        if self.sessions {
            Session::Kept
        } else {
            Session::Clean
        }
    }
}

/// A client ID starting with `prefix` that a zzq broker with `lanes` lanes
/// places on lane `lane % lanes`.
fn client_id(prefix: &str, lane: usize, lanes: usize) -> String {
    if lanes <= 1 {
        return prefix.to_owned();
    }
    (0..1_000_000)
        .map(|n| format!("{prefix}-{n}"))
        .find(|id| zzq_broker::lane_for(id, lanes) == lane % lanes)
        .unwrap_or_else(|| prefix.to_owned())
}

fn timed_out(e: &io::Error) -> bool {
    matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut)
}

#[cfg(test)]
mod tests {
    use std::net::{TcpListener, TcpStream};

    use crate::wire::{Conn, Reader};

    /// A client connection named `id` and the server end of it.
    pub(super) fn pair(id: &str) -> (Conn, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let s = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (server, _) = listener.accept().unwrap();
        let c = Conn {
            id: id.to_owned(),
            s,
            rd: Reader::with_capacity(1 << 10),
            rmax: None,
            v5: true,
        };
        (c, server)
    }
}
