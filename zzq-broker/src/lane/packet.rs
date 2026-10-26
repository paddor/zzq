//! Packets from a client connection.

use std::sync::PoisonError;

use bytes::Bytes;
use bytestring::ByteString;
use zzq_proto::encode;
use zzq_proto::packet::{Ack, Publish, decode};
use zzq_proto::property;
use zzq_proto::{Packet, PacketType, QoS, reason};

use crate::msg::Publication;
use crate::options::{Authorizer, ClientInfo};
use crate::session::Session;
use crate::slab::Key;

use super::route::Owned;
use super::{Close, Lane};

impl Lane {
    /// Handles every packet in `chunk`, which holds only whole packets.
    pub(crate) fn handle_chunk(&mut self, key: Key, chunk: &Bytes) -> Result<(), Close> {
        let Some(s) = self.sessions.get(key) else {
            return Err(Close::Kicked);
        };
        let version = s.version;
        let max = self.shared.opts.max_packet_size;
        let mut buf: &[u8] = chunk;
        while !buf.is_empty() {
            let (packet, n) = match decode(buf, version, max) {
                Ok(Some(p)) => p,
                Ok(None) => return Err(Close::Protocol(reason::MALFORMED_PACKET)),
                Err(e) => return Err(Close::Protocol(e.reason_code())),
            };
            let raw = &buf[..n];
            buf = &buf[n..];
            self.handle_packet(key, packet, raw, chunk)?;
        }
        Ok(())
    }

    #[inline]
    fn handle_packet(
        &mut self,
        key: Key,
        packet: Packet<'_>,
        raw: &[u8],
        chunk: &Bytes,
    ) -> Result<(), Close> {
        match packet {
            Packet::Publish(p) => self.on_publish(key, &p, raw, chunk),
            Packet::PubAck(a) => self.on_ack(key, |s| s.on_puback(a.pkid)),
            Packet::PubRec(a) => self.on_ack(key, |s| s.on_pubrec(a.pkid, a.reason)),
            Packet::PubRel(a) => {
                self.on_pubrel(key, &a);
                Ok(())
            }
            Packet::PubComp(a) => self.on_ack(key, |s| s.on_pubcomp(a.pkid)),
            Packet::Subscribe(s) => {
                self.on_subscribe(key, &s);
                Ok(())
            }
            Packet::Unsubscribe(u) => {
                self.on_unsubscribe(key, &u);
                Ok(())
            }
            Packet::PingReq => {
                if let Some(s) = self.sessions.get_mut(key) {
                    encode::pingresp(&mut s.out.buf);
                }
                self.mark(key);
                Ok(())
            }
            Packet::Disconnect(d) => {
                let s = self.sessions.get_mut(key).ok_or(Close::Kicked)?;
                if let Some(e) = d.properties.get_int(property::SESSION_EXPIRY_INTERVAL) {
                    if s.expiry == 0 && e != 0 {
                        return Err(Close::Protocol(reason::PROTOCOL_ERROR));
                    }
                    s.expiry = e.min(self.shared.opts.max_session_expiry);
                }
                Err(Close::Graceful {
                    will: d.reason == reason::DISCONNECT_WITH_WILL,
                })
            }
            _ => Err(Close::Protocol(reason::PROTOCOL_ERROR)),
        }
    }

    /// Applies an ack. `f` says whether the session has output, or the
    /// reason the ack breaks the protocol.
    fn on_ack(
        &mut self,
        key: Key,
        f: impl FnOnce(&mut Session) -> Result<bool, u8>,
    ) -> Result<(), Close> {
        let mut result = Ok(());
        if let Some(s) = self.sessions.get_mut(key) {
            match f(s) {
                Ok(true) => self.mark(key),
                Ok(false) => {}
                Err(code) => result = Err(Close::Protocol(code)),
            }
        }
        self.release_waiters(key);
        result
    }

    fn on_pubrel(&mut self, key: Key, a: &Ack<'_>) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let code = if s.inbound.awaiting_rel.remove(&a.pkid) {
            0
        } else {
            reason::PACKET_IDENTIFIER_NOT_FOUND
        };
        s.ack(PacketType::PubComp, a.pkid, code);
        self.mark(key);
    }

    #[inline]
    fn on_publish(
        &mut self,
        key: Key,
        p: &Publish<'_>,
        raw: &[u8],
        chunk: &Bytes,
    ) -> Result<(), Close> {
        let opts_alias_max = self.shared.opts.topic_alias_maximum;
        let receive_max = usize::from(self.shared.opts.receive_maximum);
        let s = self.sessions.get_mut(key).ok_or(Close::Kicked)?;
        let (aliased, filtered_props) = if s.version.is_v5() && !p.properties.is_empty() {
            // Subscription Identifiers are the broker's to set [MQTT-3.3.4-6].
            if p.properties
                .get_int(property::SUBSCRIPTION_IDENTIFIER)
                .is_some()
            {
                return Err(Close::Protocol(reason::PROTOCOL_ERROR));
            }
            resolve_alias(s, p, opts_alias_max)?
        } else {
            (None, None)
        };
        let topic = aliased.as_deref().unwrap_or(p.topic);
        if crate::msg::too_deep(topic, self.shared.opts.max_topic_levels) {
            return Err(Close::Protocol(reason::TOPIC_NAME_INVALID));
        }
        // `$SYS/` is the broker's own.
        let allowed = !topic.starts_with("$SYS/")
            && match self.shared.opts.authorizer.as_deref() {
                Some(acl) => may_publish(acl, s, topic),
                None => true,
            };
        // v3 has no reason codes: a denied publish gets a positive ack and is
        // dropped [MQTT-3.3.5-2].
        let code = if allowed || !s.version.is_v5() {
            0
        } else {
            reason::NOT_AUTHORIZED
        };
        if !admit_publish(s, p, allowed, code, receive_max)? {
            Self::mark_in(&mut self.dirty, s, key);
            return Ok(());
        }
        let packet = filtered_props.is_none().then_some((raw, s.version));
        let publication = Publication {
            topic,
            payload: p.payload,
            props: filtered_props.as_deref().unwrap_or(p.properties.raw()),
            qos: p.qos,
            retain: p.retain,
            raw: packet.filter(|_| p.qos == QoS::AtMostOnce),
            packet,
            chunk: Some(chunk),
            pins: true,
            expires_at: None,
        };
        let mut owned = Owned::default();
        if p.retain {
            let msg = publication.to_msg();
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .set(&msg, self.budget.full);
            owned.msg = Some(msg);
        }
        let m = self.publisher_matches(key, topic);
        self.congestion.hit = None;
        self.route_matches(Some(key), &m, &publication, &mut owned);
        self.ack_or_hold(key, p);
        Ok(())
    }
}

/// Applies a v5 Topic Alias. Returns the aliased topic, if the publish had
/// none, and the properties without the alias.
fn resolve_alias(
    s: &mut Session,
    p: &Publish<'_>,
    max: u16,
) -> Result<(Option<ByteString>, Option<Vec<u8>>), Close> {
    let Some(alias) = p.properties.get_int(property::TOPIC_ALIAS) else {
        return Ok((None, None));
    };
    let alias = alias as usize;
    if alias == 0 || alias > usize::from(max) {
        return Err(Close::Protocol(reason::TOPIC_ALIAS_INVALID));
    }
    let mut aliased = None;
    if p.topic.is_empty() {
        let topic = s.inbound.aliases.get(alias).cloned().flatten();
        aliased = Some(topic.ok_or(Close::Protocol(reason::PROTOCOL_ERROR))?);
    } else {
        if s.inbound.aliases.len() <= alias {
            s.inbound.aliases.resize(alias + 1, None);
        }
        s.inbound.aliases[alias] = Some(ByteString::from(p.topic));
    }
    let mut props = Vec::new();
    for prop in p
        .properties
        .iter()
        .filter(|x| x.id() != property::TOPIC_ALIAS)
    {
        prop.encode(&mut props);
    }
    Ok((aliased, Some(props)))
}

/// Asks the authorizer about a publish, reusing the answer for a repeated
/// topic.
#[inline]
fn may_publish(acl: &dyn Authorizer, s: &mut Session, topic: &str) -> bool {
    if let Some((last, allowed)) = &s.inbound.last_allowed
        && **last == *topic
    {
        return *allowed;
    }
    let client = ClientInfo {
        client_id: &s.client_id,
        username: s.username.as_deref(),
    };
    let allowed = acl.may_publish(&client, topic);
    s.inbound.last_allowed = Some((topic.into(), allowed));
    allowed
}

/// Checks an inbound PUBLISH before routing. Answers rejected publishes, with
/// `code`, and repeated QoS 2 packet IDs right away and returns false for
/// them. Routed publishes are acked after routing.
#[inline]
fn admit_publish(
    s: &mut Session,
    p: &Publish<'_>,
    allowed: bool,
    code: u8,
    receive_max: usize,
) -> Result<bool, Close> {
    match p.qos {
        QoS::AtMostOnce => Ok(allowed),
        QoS::AtLeastOnce if !allowed => {
            s.ack(PacketType::PubAck, p.pkid, code);
            Ok(false)
        }
        QoS::AtLeastOnce => Ok(true),
        QoS::ExactlyOnce if !allowed => {
            s.ack(PacketType::PubRec, p.pkid, code);
            Ok(false)
        }
        QoS::ExactlyOnce => {
            let fresh = s.inbound.awaiting_rel.insert(p.pkid);
            if fresh && s.inbound.awaiting_rel.len() > receive_max {
                return Err(Close::Protocol(reason::RECEIVE_MAXIMUM_EXCEEDED));
            }
            if !fresh {
                s.ack(PacketType::PubRec, p.pkid, 0);
            }
            Ok(fresh)
        }
    }
}
