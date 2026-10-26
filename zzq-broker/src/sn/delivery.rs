//! Datagrams to the client: retransmission until acked, and buffering
//! while it sleeps.

use std::time::{Duration, Instant};

use tokio::io::{AsyncWriteExt, DuplexStream};
use zzq_proto::encode;
use zzq_proto::sn::{self, SnQoS};

use super::Client;

const KIND_REGISTER: u8 = 0x0a;

const KIND_PUBLISH: u8 = 0x0c;

const KIND_PUBREL: u8 = 0x10;

/// A datagram waiting for an ack from the client.
pub(super) struct Retry {
    datagram: Vec<u8>,
    next: Instant,
    tries: u8,
}

impl Client {
    /// Sends a datagram, or buffers it while the client sleeps. QoS 1/2
    /// publishes, PUBREL and REGISTER are kept for retransmission.
    pub(super) async fn deliver(&mut self, datagram: Vec<u8>) {
        if let Some(key) = retry_key(&datagram) {
            let next = Instant::now() + self.shared.opts.sn_retry;
            self.retry.unacked.insert(
                key,
                Retry {
                    datagram: datagram.clone(),
                    next,
                    tries: 0,
                },
            );
        }
        if self.asleep.is_some() {
            if self.buffered.len() >= self.shared.opts.sn_sleep_buffer {
                self.buffered.pop_front();
            }
            self.buffered.push_back(datagram);
        } else {
            let _ = self.socket.send_to(&datagram, self.addr).await;
        }
    }

    /// Forgets retransmissions the client acknowledged.
    pub(super) fn acked(&mut self, packet: &sn::Packet<'_>) {
        let key = match *packet {
            sn::Packet::PubAck { msg_id, .. } | sn::Packet::PubRec { msg_id } => {
                (KIND_PUBLISH, msg_id)
            }
            sn::Packet::RegAck { msg_id, .. } => (KIND_REGISTER, msg_id),
            sn::Packet::PubComp { msg_id } => (KIND_PUBREL, msg_id),
            _ => return,
        };
        self.retry.unacked.remove(&key);
    }

    pub(super) fn sleep(&mut self, seconds: u16) {
        let duration = Duration::from_secs(u64::from(seconds.max(1)));
        self.asleep = Some((Instant::now() + duration.mul_f32(1.5), duration));
    }

    /// Handles a datagram from a sleeping client. Returns false if the
    /// client woke up for good.
    pub(super) async fn stays_asleep(&mut self, packet: &sn::Packet<'_>) -> bool {
        let sn::Packet::PingReq { client_id: Some(_) } = packet else {
            return false;
        };
        for d in std::mem::take(&mut self.buffered) {
            let _ = self.socket.send_to(&d, self.addr).await;
        }
        self.send(&sn::Packet::PingResp).await;
        if let Some((deadline, duration)) = &mut self.asleep {
            *deadline = Instant::now() + duration.mul_f32(1.5);
        }
        true
    }

    pub(super) async fn wake(&mut self) {
        self.asleep = None;
        for d in std::mem::take(&mut self.buffered) {
            let _ = self.socket.send_to(&d, self.addr).await;
        }
    }

    /// Retransmissions, and keep alive on behalf of a sleeping client.
    /// Returns false when the client is gone.
    pub(super) async fn on_tick(&mut self, pipe: &mut DuplexStream) -> bool {
        let now = Instant::now();
        if let Some((deadline, _)) = self.asleep {
            if now > deadline {
                return false;
            }
            let mut ping = Vec::new();
            encode::pingreq(&mut ping);
            return pipe.write_all(&ping).await.is_ok();
        }
        let retry = self.shared.opts.sn_retry;
        let max = self.shared.opts.sn_retries;
        let mut give_up = false;
        for r in self.retry.unacked.values_mut().filter(|r| r.next <= now) {
            if r.tries >= max {
                give_up = true;
                continue;
            }
            r.tries += 1;
            r.next = now + retry;
            set_dup(&mut r.datagram);
            let _ = self.socket.send_to(&r.datagram, self.addr).await;
        }
        !give_up
    }

    pub(super) fn msg_id(&mut self) -> u16 {
        self.retry.next_msg_id = self.retry.next_msg_id.wrapping_add(1).max(1);
        self.retry.next_msg_id
    }
}

/// Message type and ID of a datagram the client must ack.
fn retry_key(datagram: &[u8]) -> Option<(u8, u16)> {
    match sn::decode(datagram).ok()?.0 {
        sn::Packet::Publish { flags, msg_id, .. }
            if matches!(flags.qos, SnQoS::AtLeastOnce | SnQoS::ExactlyOnce) =>
        {
            Some((KIND_PUBLISH, msg_id))
        }
        sn::Packet::Register { msg_id, .. } => Some((KIND_REGISTER, msg_id)),
        sn::Packet::PubRel { msg_id } => Some((KIND_PUBREL, msg_id)),
        _ => None,
    }
}

/// Sets DUP on a PUBLISH datagram.
fn set_dup(datagram: &mut [u8]) {
    let at = if datagram.first() == Some(&0x01) {
        3
    } else {
        1
    };
    if datagram.get(at) == Some(&KIND_PUBLISH)
        && let Some(flags) = datagram.get_mut(at + 1)
    {
        *flags |= 0x80;
    }
}
