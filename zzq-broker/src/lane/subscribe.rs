//! Subscription changes, their acks, and retained messages on subscribe.

use std::sync::{Arc, PoisonError};

use zzq_proto::encode::{self, SubAckFields};
use zzq_proto::packet::{Subscribe, Unsubscribe};
use zzq_proto::property;
use zzq_proto::topic::{SharedFilter, is_shared};
use zzq_proto::{PacketType, QoS, SubscriptionOptions, reason};

use crate::index::{Entry, SubIds, SubRef};
use crate::msg::{Msg, Publication};
use crate::options::{Authorizer, ClientInfo};
use crate::session::Session;
use crate::slab::Key;

use super::route::{Grant, Owned};
use super::{Control, Lane, SyncAck};

/// An ack packet held until other lanes applied a subscription change.
#[derive(Debug)]
pub(crate) struct Held {
    pub(crate) key: Key,
    pub(crate) sync: Arc<SyncAck>,
    pub(crate) packet: Vec<u8>,
    /// New subscriptions that get retained messages after the ack.
    pub(crate) retained: Vec<Wanted>,
}

/// A subscription that gets the retained messages it matches: filter,
/// granted QoS and Subscription Identifier.
type Wanted = (Box<str>, QoS, u32);

impl Lane {
    pub(super) fn subscribe_entry(
        &mut self,
        filter: &str,
        entry: &Entry,
        sync: Option<&Arc<SyncAck>>,
    ) {
        self.index.add(filter, entry.clone());
        let filter: Box<str> = filter.into();
        if let Some(s) = sync {
            s.expect(self.shared.lanes.len() - 1);
        }
        self.broadcast(&|| Control::SubAdd(filter.clone(), entry.clone(), sync.cloned()));
    }

    pub(super) fn unsubscribe_entry(
        &mut self,
        filter: &str,
        sub: SubRef,
        sync: Option<&Arc<SyncAck>>,
    ) -> bool {
        let removed = self.index.remove(filter, sub);
        let filter: Box<str> = filter.into();
        if let Some(s) = sync {
            s.expect(self.shared.lanes.len() - 1);
        }
        self.broadcast(&|| Control::SubRemove(filter.clone(), sub, sync.cloned()));
        removed
    }

    pub(super) fn entry_for(
        &self,
        key: Key,
        filter: &str,
        opts: SubscriptionOptions,
        sub_id: u32,
    ) -> Entry {
        Entry {
            sub: self.sub_ref(key),
            opts,
            sub_id: SubIds::one(sub_id),
            share: is_shared(filter).then(|| Arc::from(filter)),
            online: is_shared(filter)
                .then(|| self.sessions.get(key).map(|s| s.online.clone()))
                .flatten(),
        }
    }

    pub(super) fn on_subscribe(&mut self, key: Key, sub: &Subscribe<'_>) {
        let sub_id = sub
            .properties
            .get_int(property::SUBSCRIPTION_IDENTIFIER)
            .unwrap_or(0);
        let lanes = self.shared.lanes.len();
        let mut codes = Vec::new();
        let mut wanted = Vec::new();
        let sync = self.sync();
        for (filter, opts) in sub.filters() {
            let Some(s) = self.sessions.get_mut(key) else {
                return;
            };
            if let Some(code) = refused(&self.shared.opts, s, filter) {
                codes.push(code);
                continue;
            }
            let existed = s.subscribe(filter, opts, sub_id, lanes);
            let entry = self.entry_for(key, filter, opts, sub_id);
            self.subscribe_entry(filter, &entry, sync.as_ref());
            codes.push(opts.qos as u8);
            if retained_wanted(filter, opts, existed) {
                wanted.push((filter.into(), opts.qos, sub_id));
            }
        }
        self.stats.subscriptions = self.index.len() as u64;
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let a = SubAckFields {
            kind: PacketType::SubAck,
            pkid: sub.pkid,
            properties: &[],
            codes: &codes,
        };
        let version = s.version;
        self.ack_after_sync(key, sync, |out| encode::suback(&a, version, out), wanted);
    }

    /// Sends the retained messages `wanted` subscriptions match. Read only
    /// once every lane has the subscriptions: a publish on another lane then
    /// either updated the store already, or is routed to them live.
    fn send_retained(&mut self, key: Key, wanted: &[Wanted]) {
        if wanted.is_empty() {
            return;
        }
        let mut retained = Vec::new();
        {
            let store = self
                .shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            for (filter, qos, sub_id) in wanted {
                retained.extend(
                    store
                        .matching(filter)
                        .into_iter()
                        .map(|m| (m, *qos, *sub_id)),
                );
            }
        }
        self.deliver_retained(key, retained);
    }

    /// Sends retained messages for new subscriptions, with RETAIN set.
    fn deliver_retained(&mut self, key: Key, retained: Vec<(Msg, QoS, u32)>) {
        for (msg, max_qos, sub_id) in retained {
            let p = Publication {
                retain: true,
                ..Publication::of(&msg)
            };
            let g = Grant {
                qos: msg.qos.min(max_qos),
                retain: true,
                sub_id: SubIds::one(sub_id),
                last: true,
                on_subscribe: true,
            };
            let mut retained = msg.clone();
            retained.retain = true;
            let mut owned = Owned {
                msg: Some(retained),
                share: Some(self.budget.retained_share.clone()),
            };
            self.deliver(key, &p, &mut owned, g);
        }
    }

    /// An ack to collect other lanes, if there are any.
    fn sync(&self) -> Option<Arc<SyncAck>> {
        (self.shared.lanes.len() > 1).then(Arc::default)
    }

    /// Queues an ack packet now, or holds it until `sync` completes.
    fn ack_after_sync(
        &mut self,
        key: Key,
        sync: Option<Arc<SyncAck>>,
        encode: impl FnOnce(&mut Vec<u8>),
        retained: Vec<Wanted>,
    ) {
        if let Some(sync) = sync {
            let mut packet = Vec::new();
            encode(&mut packet);
            self.held.push(Held {
                key,
                sync,
                packet,
                retained,
            });
        } else {
            if let Some(s) = self.sessions.get_mut(key) {
                encode(&mut s.out);
            }
            self.mark(key);
            self.send_retained(key, &retained);
        }
    }

    /// Ack packets waiting for other lanes.
    pub(crate) fn take_held(&mut self) -> Vec<Held> {
        std::mem::take(&mut self.held)
    }

    /// Queues an ack packet that was held for other lanes.
    pub(crate) fn release(&mut self, h: &Held) {
        if let Some(s) = self.sessions.get_mut(h.key) {
            s.out.extend_from_slice(&h.packet);
        }
        self.mark(h.key);
        self.send_retained(h.key, &h.retained);
        self.flush();
    }

    pub(super) fn on_unsubscribe(&mut self, key: Key, u: &Unsubscribe<'_>) {
        let mut codes = Vec::new();
        let sub = self.sub_ref(key);
        let sync = self.sync();
        let lanes = self.shared.lanes.len();
        for filter in u.filters() {
            let existed = self
                .sessions
                .get_mut(key)
                .is_some_and(|s| s.unsubscribe(filter, lanes));
            if existed {
                self.unsubscribe_entry(filter, sub, sync.as_ref());
            }
            codes.push(if existed {
                0
            } else {
                reason::NO_SUBSCRIPTION_EXISTED
            });
        }
        self.stats.subscriptions = self.index.len() as u64;
        let Some(s) = self.sessions.get_mut(key) else {
            return;
        };
        let a = SubAckFields {
            kind: PacketType::UnsubAck,
            pkid: u.pkid,
            properties: &[],
            codes: &codes,
        };
        let version = s.version;
        self.ack_after_sync(
            key,
            sync,
            |out| encode::suback(&a, version, out),
            Vec::new(),
        );
    }
}

/// The SUBACK code refusing `filter` for `s`, if it is refused.
fn refused(o: &crate::options::BrokerOptions, s: &Session, filter: &str) -> Option<u8> {
    let code = if crate::msg::too_deep(filter, o.max_topic_levels) {
        reason::TOPIC_FILTER_INVALID
    } else if !may_subscribe(o.authorizer.as_deref(), s, filter) {
        reason::NOT_AUTHORIZED
    } else if s.subs.len() >= o.max_subscriptions && !s.subs.contains_key(filter) {
        reason::QUOTA_EXCEEDED
    } else {
        return None;
    };
    Some(if s.version.is_v5() {
        code
    } else {
        reason::UNSPECIFIED_ERROR
    })
}

fn may_subscribe(acl: Option<&dyn Authorizer>, s: &Session, filter: &str) -> bool {
    acl.is_none_or(|acl| {
        let client = ClientInfo {
            client_id: &s.client_id,
            username: s.username.as_deref(),
        };
        match SharedFilter::try_from(filter) {
            Ok(shared) => acl.may_subscribe_shared(&client, shared.group, shared.filter),
            Err(_) => acl.may_subscribe(&client, filter),
        }
    })
}

/// Whether a subscription gets the retained messages it matches, by
/// Retain Handling. Shared subscriptions never do.
fn retained_wanted(filter: &str, opts: SubscriptionOptions, existed: bool) -> bool {
    !is_shared(filter) && (opts.retain_handling == 0 || (opts.retain_handling == 1 && !existed))
}
