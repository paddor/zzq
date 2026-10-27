//! Routing a publish to the lane's sessions and to other lanes.

use std::rc::Rc;
use std::sync::PoisonError;

use bytes::Bytes;
use zzq_proto::encode;
use zzq_proto::packet::decode;
use zzq_proto::{Packet, QoS, Version};

use crate::budget::{self, Charge, LaneTotal};
use crate::index::{Entry, Matches, SubIds, SubRef};
use crate::msg::{Arena, Block, Msg, Publication};
use crate::session::{Kept, Outcome, QueueLimit, Queued, Session};
use crate::slab::Key;

use super::{Control, Hit, Lane, Ticket};

/// A delivery for a session on another lane.
#[derive(Debug)]
pub(crate) enum Remote {
    /// PUBLISH packets for the target lane to route to its own sessions,
    /// as offset and length into one shared buffer.
    Forward {
        chunk: Bytes,
        first: (u32, u32),
        more: Vec<(u32, u32)>,
        origin: Origin,
    },
    /// A delivery to one session, chosen for a shared subscription. With
    /// `pins`, the message slices the publisher's read buffer.
    Deliver { key: Key, item: Queued, pins: bool },
    /// The last batch the sending lane ships before a state dump.
    Drained(Ticket),
}

/// How forwarded packets were sent.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Origin {
    version: Version,
    /// The publisher, for No Local.
    from: Option<SubRef>,
    /// The chunk is the publisher's read buffer, which long-held messages
    /// must not pin, not a copy made for these packets.
    pins: bool,
}

/// The message of one publish once a session holds it, the charge for its
/// bytes once more than one session of the lane may hold it, and what its
/// sessions may queue beyond the limits.
#[derive(Debug, Default)]
pub(super) struct Owned {
    pub(super) msg: Option<Msg>,
    pub(super) share: Option<Rc<Charge>>,
    /// Beyond `max_queued` and `max_queued_bytes`: the batch of retained
    /// messages a SUBSCRIBE sends.
    pub(super) extra: Extra,
}

impl Owned {
    pub(super) fn of(msg: Msg) -> Owned {
        Owned {
            msg: Some(msg),
            ..Owned::default()
        }
    }
}

/// QoS, retain flag and Subscription Identifier of one delivery.
#[derive(Debug, Clone, Copy)]
pub(super) struct Grant {
    pub(super) qos: QoS,
    pub(super) retain: bool,
    pub(super) sub_id: SubIds,
    /// The last delivery of this publish: it may take the owned message
    /// instead of cloning it.
    pub(super) last: bool,
}

/// Messages and bytes a delivery may queue beyond the limits.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct Extra {
    pub(super) count: usize,
    pub(super) bytes: usize,
}

impl Lane {
    /// Whether batches for other lanes are waiting.
    pub(crate) fn has_remote(&self) -> bool {
        self.out.remote.iter().any(|b| !b.is_empty())
    }

    /// The next batch for another lane, and its lane. Its place gets a
    /// vector of the same size, so the next batch does not grow from empty.
    pub(crate) fn next_remote(&mut self) -> Option<(usize, Vec<Remote>)> {
        let (lane, batch) = self
            .out
            .remote
            .iter_mut()
            .enumerate()
            .find(|(_, b)| !b.is_empty())?;
        let len = batch.len();
        Some((lane, std::mem::replace(batch, Vec::with_capacity(len))))
    }

    /// Puts back a batch [`Lane::next_remote`] took, which could not leave
    /// yet. Nothing was collected for its lane since.
    pub(crate) fn unship(&mut self, lane: usize, batch: Vec<Remote>) {
        self.out.remote[lane] = batch;
    }

    /// Routes a message that is not from a client connection.
    pub(crate) fn publish_owned(&mut self, msg: &Msg) {
        if msg.retain {
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .set(msg, self.budget.full);
        }
        let p = Publication::of(msg);
        let mut owned = Owned::of(msg.clone());
        self.route(None, &p, &mut owned);
    }

    /// Matches for a publish from `key`, reusing the session's last lookup
    /// when the topic repeats and the index did not change.
    #[inline]
    pub(super) fn publisher_matches(&mut self, key: Key, topic: &str) -> Rc<Matches> {
        let generation = self.index.generation();
        if let Some(session) = self.sessions.get(key)
            && let Some((last, at, matches)) = &session.inbound.last_topic
            && *at == generation
            && **last == *topic
        {
            return matches.clone();
        }
        let (matches, cached) = self.index.lookup(topic);
        if cached && let Some(session) = self.sessions.get_mut(key) {
            // Reuse the topic buffer: a publisher changing topics should not
            // allocate per message.
            let mut last = session
                .inbound
                .last_topic
                .take()
                .map(|(t, _, _)| t)
                .unwrap_or_default();
            last.clear();
            last.push_str(topic);
            session.inbound.last_topic = Some((last, generation, matches.clone()));
        }
        matches
    }

    /// Delivers a publication to every matching subscription.
    #[inline]
    fn route(&mut self, from: Option<Key>, p: &Publication<'_>, owned: &mut Owned) {
        let (m, _) = self.index.lookup(p.topic);
        self.route_matches(from, &m, p, owned);
    }

    /// Delivers a publication to `m`, the subscriptions matching its topic.
    #[inline]
    pub(super) fn route_matches(
        &mut self,
        from: Option<Key>,
        m: &Matches,
        p: &Publication<'_>,
        owned: &mut Owned,
    ) {
        self.stats.publishes += 1;
        let mut remote_lanes = 0u64;
        let mut remote_hot = false;
        let n = m.direct.len();
        for (i, e) in m.direct.iter().enumerate() {
            if e.sub.lane == self.id {
                let last = i + 1 == n && m.groups.is_empty();
                if let Some(g) = Self::grant(from, e, p, last) {
                    self.deliver(e.sub.key, p, owned, g);
                }
            } else {
                remote_lanes |= 1 << (e.sub.lane & 63);
                if p.qos != QoS::AtMostOnce
                    && !self.congestion.remote.is_empty()
                    && self.congestion.remote.contains(&e.sub)
                {
                    self.congestion.hit.get_or_insert(Hit::Remote(e.sub));
                    remote_hot = true;
                }
            }
        }
        for (i, g) in m.groups.iter().enumerate() {
            let e = g.pick();
            if e.sub.lane == self.id {
                if let Some(g) = Self::grant(from, e, p, i + 1 == m.groups.len()) {
                    self.deliver(e.sub.key, p, owned, g);
                }
            } else {
                let msg = owned.msg.get_or_insert_with(|| p.to_msg()).clone();
                let pins = p.pins && p.chunk.is_some_and(|c| msg.pins(c));
                self.out.remote[usize::from(e.sub.lane)].push(Remote::Deliver {
                    key: e.sub.key,
                    pins,
                    item: Queued {
                        msg,
                        qos: p.qos.min(e.opts.qos),
                        retain: e.opts.retain_as_published && p.retain,
                        sub_id: e.sub_id,
                    },
                });
            }
        }
        if remote_lanes != 0 {
            let (chunk, at, version, pins) = forward_packet(p, remote_hot, &mut self.out.fwd);
            let origin = Origin {
                version,
                from: from.map(|k| self.sub_ref(k)),
                pins,
            };
            for (lane, batch) in self.out.remote.iter_mut().enumerate() {
                if remote_lanes & (1 << (lane & 63)) != 0 && lane != usize::from(self.id) {
                    push_forward(batch, &chunk, at, origin);
                }
            }
        }
    }

    /// Routes a publish forwarded by another lane to this lane's sessions.
    fn route_forwarded(&mut self, chunk: &Bytes, at: (u32, u32), origin: Origin) {
        let Origin {
            version,
            from,
            pins,
        } = origin;
        let Some(packet) = chunk.get(at.0 as usize..(at.0 + at.1) as usize) else {
            return;
        };
        let Ok(Some((Packet::Publish(p), n))) = decode(packet, version, usize::MAX) else {
            return;
        };
        let publication = Publication {
            topic: p.topic,
            payload: p.payload,
            props: p.properties.raw(),
            qos: p.qos,
            retain: p.retain,
            raw: (p.qos == QoS::AtMostOnce).then_some((&packet[..n], version)),
            packet: None,
            chunk: Some(chunk),
            pins,
            expires_at: None,
        };
        let from = from.filter(|f| f.lane == self.id).map(|f| f.key);
        let (m, _) = self.index.lookup(p.topic);
        let mut owned = Owned::default();
        let n = m.direct.len();
        for (i, e) in m.direct.iter().enumerate() {
            if e.sub.lane == self.id
                && let Some(g) = Self::grant(from, e, &publication, i + 1 == n)
            {
                self.deliver(e.sub.key, &publication, &mut owned, g);
            }
        }
    }

    /// What `e` gets of `p`, unless No Local skips its own publish.
    #[inline]
    fn grant(from: Option<Key>, e: &Entry, p: &Publication<'_>, last: bool) -> Option<Grant> {
        if e.opts.no_local && from == Some(e.sub.key) {
            return None;
        }
        Some(Grant {
            qos: p.qos.min(e.opts.qos),
            retain: e.opts.retain_as_published && p.retain,
            sub_id: e.sub_id,
            last,
        })
    }

    #[inline]
    pub(super) fn deliver(&mut self, key: Key, p: &Publication<'_>, owned: &mut Owned, g: Grant) {
        let Grant {
            qos,
            retain,
            sub_id,
            ..
        } = g;
        let o = &self.shared.opts;
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let outcome = if qos == QoS::AtMostOnce {
            if s.conn.is_none() {
                Outcome::Offline
            } else {
                s.deliver_qos0(p, retain, sub_id, o.max_out_bytes)
            }
        } else {
            let limit = QueueLimit {
                count: o.max_queued.saturating_add(owned.extra.count),
                bytes: o.max_queued_bytes.saturating_add(owned.extra.bytes),
                state_full: self.budget.full,
            };
            let kept = keep(s, p, owned, &self.budget.total, g);
            s.deliver_qos12(kept, limit)
        };
        if outcome == Outcome::Sent {
            // The session is at hand: no second lookup.
            Self::mark_in(&mut self.dirty, s, key);
        } else if matches!(outcome, Outcome::Queued | Outcome::QueueFull(_))
            && s.conn.is_some()
            && s.queued() >= self.shared.opts.qos_queue_high
        {
            let newly = !s.backpressure.congested;
            s.backpressure.congested = true;
            if newly {
                let sub = self.sub_ref(key);
                self.broadcast(&|| Control::Congested(sub, true));
            }
            self.congestion.hit.get_or_insert(Hit::Local(key));
        }
        self.count(outcome);
    }

    #[inline]
    fn count(&mut self, outcome: Outcome) {
        match outcome {
            Outcome::Sent => self.stats.deliveries += 1,
            Outcome::Queued => {}
            Outcome::QueueFull(n) => self.stats.dropped.queue_full += u64::from(n),
            Outcome::StateFull => self.stats.dropped.state_full += 1,
            Outcome::BufferFull => self.stats.dropped.buffer_full += 1,
            Outcome::Offline => self.stats.dropped.offline += 1,
            Outcome::TooLarge => self.stats.dropped.too_large += 1,
        }
    }

    /// Delivers a message routed on another lane.
    pub(crate) fn deliver_remote(&mut self, r: Remote) {
        match r {
            Remote::Forward {
                chunk,
                first,
                more,
                origin,
            } => {
                self.route_forwarded(&chunk, first, origin);
                for at in more {
                    self.route_forwarded(&chunk, at, origin);
                }
            }
            // After what came before it in the batch.
            Remote::Drained(ticket) => drop(ticket),
            Remote::Deliver { key, item, pins } => {
                let p = Publication {
                    pins,
                    ..Publication::of(&item.msg)
                };
                let mut owned = Owned::of(item.msg.clone());
                let g = Grant {
                    qos: item.qos,
                    retain: item.retain,
                    sub_id: item.sub_id,
                    last: true,
                };
                self.deliver(key, &p, &mut owned, g);
            }
        }
    }
}

/// The publication as one PUBLISH packet another lane can decode, as a
/// buffer, the packet's offset and length in it, and whether the buffer is
/// the publisher's read buffer. That is the publisher's own bytes, unless a
/// congested subscriber there (`hot`) will hold the message long: then a
/// copy in `arena`, so the copy is made here and not on that busy lane. A
/// packet that must change becomes a v5 encoding in `arena`.
fn forward_packet(
    p: &Publication<'_>,
    hot: bool,
    arena: &mut Arena,
) -> (Bytes, (u32, u32), Version, bool) {
    if let (Some((raw, version)), Some(chunk)) = (p.packet, p.chunk)
        && crate::msg::within(chunk, raw)
    {
        if !hot {
            let at = raw.as_ptr() as usize - chunk.as_ptr() as usize;
            return (chunk.clone(), (at as u32, raw.len() as u32), version, true);
        }
        let copy = arena.put(raw.len(), |b| b.extend_from_slice(raw));
        return (copy, (0, raw.len() as u32), version, false);
    }
    let h = encode::PublishHeader {
        version: Version::V5,
        dup: false,
        qos: p.qos,
        retain: p.retain,
        topic: p.topic,
        pkid: u16::from(p.qos != QoS::AtMostOnce),
        properties: p.props,
    };
    let len = encode::packet_len(h.remaining(p.payload.len()));
    let out = arena.put(len, |b| encode::publish(&h, p.payload, &mut Block(b)));
    let at = (0, out.len() as u32);
    (out, at, Version::V5, false)
}

/// What session `s` keeps of `p` at QoS 1/2: the message, copied if it would
/// pin a read buffer long, and a share of its bytes when more sessions of
/// the lane may get it.
fn keep(
    s: &mut Session,
    p: &Publication<'_>,
    owned: &mut Owned,
    total: &LaneTotal,
    g: Grant,
) -> Kept {
    let msg = owned.msg.get_or_insert_with(|| p.to_msg());
    // A slow or offline session may hold a message long; do not let it pin
    // read buffers.
    if s.holds_long() && p.pins && p.chunk.is_none_or(|c| msg.pins(c)) {
        *msg = s.copy(msg);
    }
    let share = if g.last && owned.share.is_none() {
        None
    } else {
        let share = owned
            .share
            .get_or_insert_with(|| Rc::new(Charge::new(total.clone(), budget::shared(msg))));
        Some(share.clone())
    };
    let msg = if g.last {
        owned.msg.take()
    } else {
        owned.msg.clone()
    };
    let item = Queued {
        msg: msg.unwrap_or_else(|| p.to_msg()),
        qos: g.qos,
        retain: g.retain,
        sub_id: g.sub_id,
    };
    Kept { item, share }
}

/// Appends a forwarded packet, sharing the last batch entry when it uses
/// the same buffer.
fn push_forward(batch: &mut Vec<Remote>, chunk: &Bytes, at: (u32, u32), origin: Origin) {
    if let Some(Remote::Forward {
        chunk: c,
        more,
        origin: o,
        ..
    }) = batch.last_mut()
        && c.as_ptr() == chunk.as_ptr()
        && c.len() == chunk.len()
        && *o == origin
    {
        more.push(at);
        return;
    }
    // Packets of one read are mostly alike, so size the rest from this one;
    // a copy made for one packet gets no vector.
    let more = (chunk.len() / (at.1 as usize).max(1)).saturating_sub(1);
    batch.push(Remote::Forward {
        chunk: chunk.clone(),
        first: at,
        more: Vec::with_capacity(more),
        origin,
    });
}
