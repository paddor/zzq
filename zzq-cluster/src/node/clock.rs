//! Clock samples: each node sends its wall time to the two others once a
//! second, so each can tell whether its clock agrees with theirs.

use std::time::Duration;

use bytes::Bytes;

use super::link::{Encoder, Side};
use super::{Node, NodeError, Outgoing};
use crate::clock::{ClockConfig, ClockGuard, ClockStatus, SAMPLE_INTERVAL};
use crate::wire::WireError;
use crate::{Envelope, LinkSessionId, NodeId, Opcode, Packet};

/// `CLOCK_SAMPLE` metadata: wall time in Unix milliseconds.
const SAMPLE_BYTES: usize = 8;

/// The guard and when the next samples go out.
#[derive(Debug)]
pub(super) struct Sampling {
    guard: ClockGuard,
    origin_millis: u64,
    next: Duration,
}

impl Sampling {
    pub(super) const fn new(config: ClockConfig, peers: [NodeId; 2]) -> Sampling {
        Sampling {
            guard: ClockGuard::new(peers, config.tolerance_millis),
            origin_millis: config.origin_millis,
            next: Duration::ZERO,
        }
    }

    pub(super) const fn next(&self) -> Duration {
        self.next
    }

    /// This node's wall time at `now`.
    fn wall(&self, now: Duration) -> u64 {
        let elapsed = u64::try_from(now.as_millis()).unwrap_or(u64::MAX);
        self.origin_millis.saturating_add(elapsed)
    }
}

impl Encoder {
    /// A `CLOCK_SAMPLE` to `to` on `session`.
    fn sample(
        &self,
        to: NodeId,
        session: LinkSessionId,
        wall_millis: u64,
    ) -> Result<Outgoing, WireError> {
        let header = Envelope {
            opcode: Opcode::ClockSample,
            response: false,
            request_id: None,
            sender: self.local,
            session: Some(session),
        }
        .encode_header(SAMPLE_BYTES, 0, self.limits.envelope)?;
        Ok(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&header),
                Bytes::copy_from_slice(&wall_millis.to_be_bytes()),
                Bytes::new(),
            ],
        })
    }
}

/// The wall time a `CLOCK_SAMPLE` carries. The caller checks the session.
fn read_sample(packet: Packet<'_>) -> Result<u64, WireError> {
    if packet.envelope.response || packet.envelope.request_id.is_some() {
        return Err(WireError::Correlation);
    }
    if !packet.payload.is_empty() {
        return Err(WireError::Payload);
    }
    let wall = <[u8; SAMPLE_BYTES]>::try_from(packet.metadata).map_err(|_| WireError::Length)?;
    Ok(u64::from_be_bytes(wall))
}

impl Node {
    /// What the samples say about this node's clock.
    pub fn clock(&self) -> ClockStatus {
        let offset = |side| {
            let id = self.peers[side].id;
            let fresh = self
                .clock
                .guard
                .offsets(self.now)
                .find(|(peer, _)| *peer == id);
            (id, fresh.and_then(|(_, offset)| offset))
        };
        ClockStatus {
            offsets: [offset(Side::A), offset(Side::B)],
            majority: self.clock.guard.majority(self.now),
        }
    }

    /// Sends each peer bound to this process its wall time: once a
    /// second, and at once on a new session.
    pub(super) fn send_samples(&mut self, now: Duration) {
        let due = now >= self.clock.next;
        if due {
            self.clock.next = now + SAMPLE_INTERVAL;
        }
        let wall = self.clock.wall(now);
        for side in Side::BOTH {
            let peer = &mut self.peers[side];
            let Some(session) = peer.session.filter(|_| peer.knows_us) else {
                continue;
            };
            if !due && peer.sampled == Some(session) {
                continue;
            }
            peer.sampled = Some(session);
            if let Ok(out) = self.encoder.sample(peer.id, session, wall) {
                self.out.push_back(out);
            }
        }
    }

    /// Keeps the sample a peer sent on its session.
    pub(super) fn on_sample(
        &mut self,
        from: NodeId,
        packet: Packet<'_>,
        now: Duration,
    ) -> Result<(), NodeError> {
        let wall = read_sample(packet)?;
        let local = self.clock.wall(now);
        self.clock.guard.observe(from, wall, local, now);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::WireLimits;
    use crate::{Configuration, Digest, EnvelopeLimits, GroupId, decode_packet};

    fn encoder() -> Encoder {
        Encoder {
            local: NodeId::from_bytes([1; 16]),
            configuration: Configuration::new(
                GroupId::from_bytes([7; 16]),
                1,
                Digest::from_bytes([8; 32]),
                [1, 2, 3].map(|i| NodeId::from_bytes([i; 16])),
            )
            .unwrap(),
            limits: WireLimits::default(),
        }
    }

    #[test]
    fn a_sample_round_trips_in_network_order() {
        let session = LinkSessionId::from_bytes([5; 16]);
        let out = encoder()
            .sample(NodeId::from_bytes([2; 16]), session, 0x0102_0304_0506_0708)
            .unwrap();
        assert_eq!(&out.frames[1][..], [1, 2, 3, 4, 5, 6, 7, 8]);
        let frames: Vec<&[u8]> = out.frames.iter().map(|f| &f[..]).collect();
        let packet = decode_packet(&frames, EnvelopeLimits::default()).unwrap();
        assert_eq!(packet.envelope.opcode, Opcode::ClockSample);
        assert_eq!(packet.envelope.session, Some(session));
        assert_eq!(read_sample(packet).unwrap(), 0x0102_0304_0506_0708);
    }

    #[test]
    fn a_sample_of_another_shape_is_refused() {
        let session = LinkSessionId::from_bytes([5; 16]);
        let out = encoder()
            .sample(NodeId::from_bytes([2; 16]), session, 9)
            .unwrap();
        let frames: Vec<&[u8]> = out.frames.iter().map(|f| &f[..]).collect();
        let good = decode_packet(&frames, EnvelopeLimits::default()).unwrap();
        let short = Packet {
            metadata: &good.metadata[..7],
            ..good
        };
        assert!(matches!(read_sample(short), Err(WireError::Length)));
        let with_payload = Packet {
            payload: b"x",
            ..good
        };
        assert!(matches!(read_sample(with_payload), Err(WireError::Payload)));
        let response = Packet {
            envelope: Envelope {
                response: true,
                ..good.envelope
            },
            ..good
        };
        assert!(matches!(read_sample(response), Err(WireError::Correlation)));
    }

    #[test]
    fn wall_time_is_the_origin_plus_now_and_saturates() {
        let peers = [NodeId::from_bytes([2; 16]), NodeId::from_bytes([3; 16])];
        let config = |origin_millis| ClockConfig {
            origin_millis,
            tolerance_millis: 1000,
        };
        let s = Sampling::new(config(1_000), peers);
        assert_eq!(s.wall(Duration::from_millis(234)), 1_234);
        let s = Sampling::new(config(u64::MAX - 1), peers);
        assert_eq!(s.wall(Duration::from_secs(1)), u64::MAX);
    }
}
