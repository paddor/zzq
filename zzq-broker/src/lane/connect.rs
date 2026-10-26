//! Connections attaching to and leaving sessions, and wills.

use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use zzq_proto::encode;
use zzq_proto::property::{self, Property};
use zzq_proto::{PacketType, Version, reason};

use crate::conn::{ConnShared, OwnedConnect};
use crate::msg::Msg;
use crate::session::{DelayedWill, Session};
use crate::slab::Key;

use super::{Attached, Close, Lane};

impl Lane {
    /// A fresh client ID that `lane_for` maps to this lane, so the client
    /// finds its session here when it reconnects.
    pub(crate) fn assign_client_id(&mut self) -> String {
        let lanes = self.shared.lanes.len();
        loop {
            self.ids.client_no += 1;
            let id = format!("zzq-{}-{}-{}", self.shared.id, self.id, self.ids.client_no);
            if crate::broker::lane_for(&id, lanes) == usize::from(self.id) {
                return id;
            }
        }
    }

    /// Ends a session for good. A will still waiting for its delay goes
    /// out now.
    pub(super) fn remove_session(&mut self, key: Key) {
        if let Some(s) = self.sessions.get_mut(key) {
            s.unbind();
        }
        self.release_waiters(key);
        let Some(mut s) = self.sessions.remove(key) else {
            return;
        };
        self.stats.add_session(&s);
        self.stats.dropped.session_ended += s.held();
        let sub = self.sub_ref(key);
        for filter in s.subs.keys() {
            self.unsubscribe_entry(filter, sub, None);
        }
        self.stats.subscriptions = self.index.len() as u64;
        if self.by_client.get(&s.client_id) == Some(&key) {
            self.by_client.remove(&s.client_id);
        }
        if let Some(msg) = take_delayed_will(&mut self.wills, &mut s) {
            self.publish_will(msg);
        }
    }

    pub(super) fn kick(conn: &ConnShared, version: Version, code: u8) {
        let mut bye = Vec::new();
        if version.is_v5() {
            encode::reason_only(PacketType::Disconnect, version, code, &[], &mut bye);
        }
        conn.close(bye);
    }

    /// Whether `client_id` has a session on this lane.
    pub(crate) fn has_session(&self, client_id: &str) -> bool {
        self.by_client.contains_key(client_id)
    }

    /// Whether `client_id` has a live connection on this lane.
    pub(crate) fn is_connected(&self, client_id: &str) -> bool {
        self.by_client
            .get(client_id)
            .and_then(|k| self.sessions.get(*k))
            .is_some_and(|s| s.conn.is_some())
    }

    /// Binds a connection to its session, creating or resuming it, and
    /// queues CONNACK.
    pub(crate) fn attach(&mut self, c: &OwnedConnect, conn: Rc<ConnShared>) -> Attached {
        let max_expiry = self.shared.opts.max_session_expiry;
        let key = self.by_client.get(c.client_id.as_str()).copied();
        let present = key.is_some_and(|k| self.take_over(k, c));
        let key = key.filter(|_| present).unwrap_or_else(|| {
            let session = Session::new(
                c.client_id.as_str().into(),
                c.version,
                self.budget.total.clone(),
            );
            let k = self.sessions.insert(session);
            self.by_client.insert(c.client_id.as_str().into(), k);
            k
        });
        self.stats.connections += 1;
        let props = self.connack_props(c);
        let Some(s) = self.sessions.get_mut(key) else {
            return Attached { key };
        };
        s.version = c.version;
        s.username = c.username.as_deref().map(Box::from);
        s.inbound.last_allowed = None;
        s.inbound.held_acks = 0;
        s.bind(conn);
        s.expiry = c.session_expiry.min(max_expiry);
        s.expires_at = None;
        s.will.clone_from(&c.will);
        s.flow.window = c.receive_maximum.min(self.shared.opts.max_inflight).max(1);
        s.max_packet = c.max_packet_size;
        s.inbound.aliases.clear();
        connack_ok(c.version, present, &props, &mut s.out.buf);
        if present {
            s.resend();
        }
        self.mark(key);
        Attached { key }
    }

    /// Ends the old connection of session `k` for a new one. Returns whether
    /// the session goes on.
    fn take_over(&mut self, k: Key, c: &OwnedConnect) -> bool {
        let Some(s) = self.sessions.get_mut(k) else {
            return false;
        };
        // The old session ends if the new connection starts clean or the
        // old one kept no session.
        let ends = c.clean_start || s.expiry == 0;
        // The old connection ends without DISCONNECT, so its will goes
        // out now, unless it has a delay and the session goes on: the
        // new connection came first and cancels it. So does a will of
        // an earlier connection that waits for its delay.
        let old = s.conn.take();
        let delayed = take_delayed_will(&mut self.wills, s);
        let will = match s.will.take() {
            Some(w) if old.is_some() && (w.delay == 0 || ends) => Some(w.msg),
            _ => delayed.filter(|_| ends),
        };
        // Packets queued for the old connection must not reach the new
        // one; the inflight window is resent on attach.
        s.out.buf.clear();
        if let Some(old) = old {
            // Before its CONNACK, a connection just closes: CONNACK comes
            // before any other packet [MQTT-3.2.0-1].
            if old.connacked.get() {
                Self::kick(&old, s.version, reason::SESSION_TAKEN_OVER);
            } else {
                old.close(Vec::new());
            }
            self.stats.connections -= 1;
        }
        if let Some(will) = will {
            self.publish_will(will);
        }
        if ends {
            self.remove_session(k);
        }
        !ends
    }

    fn connack_props(&self, c: &OwnedConnect) -> Vec<u8> {
        let mut props = Vec::new();
        if !c.version.is_v5() {
            return props;
        }
        let o = &self.shared.opts;
        let mut list = vec![
            Property::U16(property::TOPIC_ALIAS_MAXIMUM, o.topic_alias_maximum),
            Property::U32(
                property::MAXIMUM_PACKET_SIZE,
                o.max_packet_size.min(u32::MAX as usize) as u32,
            ),
        ];
        if o.receive_maximum < u16::MAX {
            list.push(Property::U16(property::RECEIVE_MAXIMUM, o.receive_maximum));
        }
        if c.server_keep_alive {
            list.push(Property::U16(property::SERVER_KEEP_ALIVE, c.keep_alive));
        }
        if c.assigned {
            list.push(Property::Str(
                property::ASSIGNED_CLIENT_IDENTIFIER,
                &c.client_id,
            ));
        }
        if c.session_expiry > o.max_session_expiry {
            list.push(Property::U32(
                property::SESSION_EXPIRY_INTERVAL,
                o.max_session_expiry,
            ));
        }
        property::encode_all(&list, &mut props);
        props
    }

    /// Unbinds a closed connection from its session.
    pub(crate) fn detach(&mut self, key: Key, conn_id: u64, close: Close) {
        // Acks the connection can no longer get.
        self.held.retain(|h| h.conn != conn_id);
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        if !s.has_conn(conn_id) {
            return;
        }
        s.unbind();
        s.out.buf.clear();
        s.inbound.aliases.clear();
        self.stats.connections -= 1;
        self.shared.connections.fetch_sub(1, Ordering::AcqRel);
        let send_will = !matches!(close, Close::Graceful { will: false });
        let will = s.will.take().filter(|_| send_will);
        let expiry = s.expiry;
        if expiry != 0 && expiry != u32::MAX {
            s.expires_at = Some(Instant::now() + Duration::from_secs(u64::from(expiry)));
        }
        self.release_waiters(key);
        if expiry == 0 {
            self.remove_session(key);
        }
        if let Some(w) = will {
            if w.delay == 0 || expiry == 0 {
                self.publish_will(w.msg);
            } else {
                let delay = Duration::from_secs(u64::from(w.delay.min(expiry)));
                self.schedule_will(key, w.msg, delay);
            }
        }
    }

    /// Publishes `msg` after `delay`, unless a new connection or the end of
    /// the session takes it first. The sweep publishes due wills.
    pub(super) fn schedule_will(&mut self, key: Key, msg: Msg, delay: Duration) {
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        self.ids.will += 1;
        let (timer, due) = (self.ids.will, Instant::now() + delay);
        if let Some(old) = s.delayed_will.replace(DelayedWill { timer, due, msg }) {
            self.wills.remove(&(old.due, old.timer));
        }
        self.wills.insert((due, timer), key);
    }

    /// Publishes a will. Its Message Expiry Interval starts now.
    pub(super) fn publish_will(&mut self, mut msg: Msg) {
        msg.expires_at = crate::msg::expiry(msg.props());
        self.publish_owned(&msg);
    }
}

/// Queues a CONNACK accepting a connection.
fn connack_ok(version: Version, present: bool, props: &[u8], out: &mut Vec<u8>) {
    let ack = encode::ConnAckFields {
        session_present: present,
        code: reason::SUCCESS,
        properties: props,
    };
    encode::connack(&ack, version, out);
}

/// Takes the delayed will of `s` off its timer.
fn take_delayed_will(wills: &mut BTreeMap<(Instant, u64), Key>, s: &mut Session) -> Option<Msg> {
    let w = s.delayed_will.take()?;
    wills.remove(&(w.due, w.timer));
    Some(w.msg)
}
