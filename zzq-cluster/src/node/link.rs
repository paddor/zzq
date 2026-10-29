//! Links to the two other nodes: the HELLO that binds a session to both
//! processes, and encoding messages for a bound link.

use std::ops::{Index, IndexMut};
use std::time::Duration;

use bytes::Bytes;
use xxhash_rust::xxh3::xxh3_128;

use crate::wire::{
    ControlEncoding, EncodeContext, PeerBinding, Prepare, WireError, WireLimits, encode_control,
    encode_prepare,
};
use crate::{
    Configuration, Digest, ENVELOPE_BYTES, Envelope, LinkSessionId, NodeId, OpNumber, Opcode,
    Packet, RequestId,
};

use super::Outgoing;
use crate::wire::Control;

/// HELLO metadata: configuration digest, the application's fingerprint,
/// the receiver's incarnation as the sender knows it (zero: none), the
/// configuration epoch, and whether the sender's history is empty.
const HELLO_BYTES: usize = 32 + 8 + 16 + 8 + 1;

/// What a node knows of one other node.
#[derive(Debug)]
pub(super) struct Peer {
    pub(super) id: NodeId,
    /// The process behind the link, from its HELLO.
    pub(super) incarnation: Option<RequestId>,
    /// Bound once both incarnations are known.
    pub(super) session: Option<LinkSessionId>,
    /// It bound this process too: it named it in a HELLO or sent on the
    /// session.
    pub(super) knows_us: bool,
    /// When to say HELLO again while it does not know this process.
    pub(super) hello_at: Duration,
    /// Leader: the last op sent to it.
    pub(super) sent: OpNumber,
    /// Leader: the last op it acked.
    pub(super) acked: OpNumber,
    /// Leader: when its acks last moved or the sends last rewound.
    pub(super) progress_at: Duration,
    /// Leader: when it was last told its next ops are gone.
    pub(super) retired_at: Option<Duration>,
    /// Leader: when it last acked.
    pub(super) acked_at: Option<Duration>,
    /// The first op it fetches while it recovers, and when it last asked.
    pub(super) pin: Option<(OpNumber, Duration)>,
}

impl Peer {
    pub(super) const fn new(id: NodeId) -> Peer {
        Peer {
            id,
            incarnation: None,
            session: None,
            knows_us: false,
            hello_at: Duration::ZERO,
            sent: OpNumber(0),
            acked: OpNumber(0),
            progress_at: Duration::ZERO,
            retired_at: None,
            acked_at: None,
            pin: None,
        }
    }
}

/// One of the two other nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Side {
    A,
    B,
}

impl Side {
    pub(super) const BOTH: [Side; 2] = [Side::A, Side::B];
}

/// The two other nodes.
#[derive(Debug)]
pub(super) struct Peers(pub(super) [Peer; 2]);

impl Peers {
    /// The side of node `id`, if it is one of them.
    pub(super) fn side(&self, id: NodeId) -> Option<Side> {
        Side::BOTH.into_iter().find(|&s| self[s].id == id)
    }

    pub(super) fn iter_mut(&mut self) -> std::slice::IterMut<'_, Peer> {
        self.0.iter_mut()
    }
}

impl Index<Side> for Peers {
    type Output = Peer;

    fn index(&self, side: Side) -> &Peer {
        let [a, b] = &self.0;
        match side {
            Side::A => a,
            Side::B => b,
        }
    }
}

impl IndexMut<Side> for Peers {
    fn index_mut(&mut self, side: Side) -> &mut Peer {
        let [a, b] = &mut self.0;
        match side {
            Side::A => a,
            Side::B => b,
        }
    }
}

/// The session of the link between two processes: the same on both ends,
/// new when either restarts.
pub(super) fn session(a: RequestId, b: RequestId) -> LinkSessionId {
    let (low, high) = if a.as_bytes() <= b.as_bytes() {
        (a, b)
    } else {
        (b, a)
    };
    let mut both = [0; 32];
    both[..16].copy_from_slice(low.as_bytes());
    both[16..].copy_from_slice(high.as_bytes());
    let mut id = xxh3_128(&both).to_be_bytes();
    if id == [0; 16] {
        id[15] = 1;
    }
    LinkSessionId::from_bytes(id)
}

/// What a HELLO says.
#[derive(Debug, Clone, Copy)]
pub(super) struct Greeting {
    /// This process.
    pub(super) incarnation: RequestId,
    pub(super) fingerprint: u64,
    /// The receiver's process, as far as this one knows it.
    pub(super) known: Option<RequestId>,
    /// The sender's history is empty, at view zero.
    pub(super) fresh: bool,
}

impl Encoder {
    /// A HELLO to `to`.
    pub(super) fn hello(&self, to: NodeId, greeting: Greeting) -> Result<Outgoing, WireError> {
        let Greeting {
            incarnation,
            fingerprint,
            known,
            fresh,
        } = greeting;
        let (sender, configuration, limits) = (self.local, &self.configuration, self.limits);
        let header = Envelope {
            opcode: Opcode::Hello,
            response: false,
            request_id: Some(incarnation),
            sender,
            session: None,
        }
        .encode_header(HELLO_BYTES, 0, limits.envelope)?;
        let mut metadata = Vec::with_capacity(HELLO_BYTES);
        metadata.extend_from_slice(configuration.scope().configuration_digest.as_bytes());
        metadata.extend_from_slice(&fingerprint.to_be_bytes());
        metadata.extend_from_slice(known.map_or([0; 16], |k| *k.as_bytes()).as_slice());
        metadata.extend_from_slice(&configuration.scope().configuration_epoch.to_be_bytes());
        metadata.push(u8::from(fresh));
        Ok(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&header),
                Bytes::from(metadata),
                Bytes::new(),
            ],
        })
    }
}

/// A received HELLO.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Hello {
    pub(super) from: NodeId,
    pub(super) incarnation: RequestId,
    pub(super) fingerprint: u64,
    /// The receiver's incarnation as the sender knows it.
    pub(super) known: Option<RequestId>,
    /// The configuration the sender runs.
    pub(super) digest: Digest,
    pub(super) epoch: u64,
    /// The sender's history is empty, at view zero.
    pub(super) fresh: bool,
}

/// The HELLO in `packet`, if it is one.
pub(super) fn read_hello(packet: Packet<'_>) -> Result<Option<Hello>, WireError> {
    if packet.envelope.opcode != Opcode::Hello {
        return Ok(None);
    }
    let Some(incarnation) = packet.envelope.request_id else {
        return Err(WireError::Correlation);
    };
    let Ok(metadata) = <&[u8; HELLO_BYTES]>::try_from(packet.metadata) else {
        return Err(WireError::Length);
    };
    let (digest, rest) = metadata.split_at(32);
    let (fingerprint, rest) = rest.split_at(8);
    let (known, rest) = rest.split_at(16);
    let (epoch, fresh) = rest.split_at(8);
    let length = |_| WireError::Length;
    let fingerprint = u64::from_be_bytes(fingerprint.try_into().map_err(length)?);
    let known: [u8; 16] = known.try_into().map_err(length)?;
    let fresh = match fresh {
        [0] => false,
        [1] => true,
        _ => return Err(WireError::Length),
    };
    Ok(Some(Hello {
        from: packet.envelope.sender,
        incarnation,
        fingerprint,
        known: (known != [0; 16]).then(|| RequestId::from_bytes(known)),
        digest: Digest::from_bytes(digest.try_into().map_err(length)?),
        epoch: u64::from_be_bytes(epoch.try_into().map_err(length)?),
        fresh,
    }))
}

/// Encodes for one bound link.
#[derive(Debug, Clone, Copy)]
pub(super) struct Encoder {
    pub(super) local: NodeId,
    pub(super) configuration: Configuration,
    pub(super) limits: WireLimits,
}

impl Encoder {
    pub(super) fn binding(
        &self,
        peer: NodeId,
        session: LinkSessionId,
    ) -> Result<PeerBinding, WireError> {
        PeerBinding::new(self.configuration, peer, session)
    }

    pub(super) fn control(
        &self,
        to: NodeId,
        session: LinkSessionId,
        message: Control,
    ) -> Result<Outgoing, WireError> {
        let mut metadata = vec![0; 256];
        let ControlEncoding {
            header,
            metadata_bytes,
        } = encode_control(self.local, session, message, &mut metadata)?;
        metadata.truncate(metadata_bytes);
        Ok(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&header),
                Bytes::from(metadata),
                Bytes::new(),
            ],
        })
    }

    pub(super) fn prepare(
        &self,
        to: NodeId,
        session: LinkSessionId,
        prepare: Prepare<'_>,
    ) -> Result<Outgoing, WireError> {
        let mut metadata = vec![0; self.limits.envelope.max_metadata_bytes];
        let payload_bytes: usize = prepare
            .operations
            .iter()
            .map(|o| o.canonical().body.len())
            .sum();
        let mut payload = vec![0; payload_bytes];
        let encoded = encode_prepare(
            EncodeContext {
                sender: self.local,
                session,
                limits: self.limits,
            },
            prepare,
            &mut metadata,
            &mut payload,
        )?;
        metadata.truncate(encoded.metadata_bytes);
        payload.truncate(encoded.payload_bytes);
        debug_assert_eq!(encoded.header.len(), ENVELOPE_BYTES);
        Ok(Outgoing {
            to,
            frames: [
                Bytes::copy_from_slice(&encoded.header),
                Bytes::from(metadata),
                Bytes::from(payload),
            ],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Digest, EnvelopeLimits, GroupId, decode_packet};

    fn configuration() -> Configuration {
        Configuration::new(
            GroupId::from_bytes([7; 16]),
            1,
            Digest::from_bytes([8; 32]),
            [1, 2, 3].map(|i| NodeId::from_bytes([i; 16])),
        )
        .unwrap()
    }

    #[test]
    fn both_ends_derive_one_session_and_a_restart_changes_it() {
        let a = RequestId::from_bytes([1; 16]);
        let b = RequestId::from_bytes([2; 16]);
        assert_eq!(session(a, b), session(b, a));
        assert_ne!(session(a, b), session(a, RequestId::from_bytes([3; 16])));
    }

    #[test]
    fn hello_round_trips_and_names_its_configuration() {
        let c = configuration();
        let me = NodeId::from_bytes([1; 16]);
        let inc = RequestId::from_bytes([9; 16]);
        let encoder = Encoder {
            local: me,
            configuration: c,
            limits: WireLimits::default(),
        };
        let greeting = Greeting {
            incarnation: inc,
            fingerprint: 42,
            known: None,
            fresh: true,
        };
        let out = encoder
            .hello(NodeId::from_bytes([2; 16]), greeting)
            .unwrap();
        let frames: Vec<&[u8]> = out.frames.iter().map(|f| &f[..]).collect();
        let packet = decode_packet(&frames, EnvelopeLimits::default()).unwrap();
        let got = read_hello(packet).unwrap().unwrap();
        assert_eq!(
            (got.from, got.incarnation, got.fingerprint, got.known),
            (me, inc, 42, None)
        );
        let scope = c.scope();
        assert_eq!(
            (got.digest, got.epoch, got.fresh),
            (scope.configuration_digest, scope.configuration_epoch, true)
        );
    }
}
