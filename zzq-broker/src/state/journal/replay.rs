//! Sessions rebuilt from the records of a lane journal.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;
use zzq_proto::topic::validate_filter;
use zzq_proto::{QoS, SubscriptionOptions, Version};

use crate::index::SubIds;
use crate::msg::Msg;
use crate::session::{Queued, Will};
use crate::state::codec::Decoder;
use crate::state::{Clock, SessionState};

use super::{
    CLEAR, CLEARED, DONE, DOWN, END, IMAGE, INBOUND, INBOUND_DONE, MSG, QUEUED, RELEASED, SENT,
    SESSION, SUB, UNSUB, WILL_SENT,
};

/// The sessions a lane journal leaves, record by record.
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    by_client: FxHashMap<Box<str>, Replayed>,
    /// The sessions of a snapshot still being read: they take the place
    /// of `by_client` at its end, and are dropped if it never comes.
    snapshot: Option<FxHashMap<Box<str>, Replayed>>,
    /// The message of the last [`MSG`] record, which [`QUEUED`] records
    /// after it deliver.
    msg: Option<Msg>,
}

#[derive(Debug)]
struct Replayed {
    /// Without subscriptions, inbound packet IDs or deliveries.
    st: SessionState,
    subs: BTreeMap<Box<str>, (SubscriptionOptions, u32)>,
    awaiting_rel: BTreeSet<u16>,
    /// Deliveries not done, by number.
    items: BTreeMap<u64, Item>,
    last_seq: u64,
    /// No connection end came after the last connection.
    connected: bool,
}

#[derive(Debug)]
struct Item {
    queued: Queued,
    /// The packet ID it went out under, if that matters.
    pkid: Option<u16>,
    released: bool,
}

impl Sessions {
    /// Applies a record. `owned` tells whether a client ID belongs to the
    /// journal's lane.
    pub(crate) fn apply(
        &mut self,
        kind: u8,
        body: &[u8],
        clock: Clock,
        owned: impl Fn(&str) -> bool,
    ) -> Result<(), &'static str> {
        let mut d = Decoder(body);
        match kind {
            CLEAR => self.snapshot = Some(FxHashMap::default()),
            CLEARED => {
                if let Some(snapshot) = self.snapshot.take() {
                    self.by_client = snapshot;
                }
            }
            IMAGE => {
                let r = image(&mut d, clock)?;
                if !owned(&r.st.client_id) {
                    return Err("session in the journal of another lane");
                }
                let into = self.snapshot.as_mut().unwrap_or(&mut self.by_client);
                into.insert(r.st.client_id.clone(), r);
            }
            // Any other record after an unfinished snapshot: the snapshot
            // was cut off, and the lane went on without it.
            _ if self.snapshot.take().is_some() => return self.apply(kind, body, clock, owned),
            MSG => self.msg = Some(d.msg(clock)?),
            SESSION => {
                let client_id = d.str()?;
                let st = session_head(&mut d, client_id, clock)?;
                if !owned(client_id) {
                    return Err("session in the journal of another lane");
                }
                // A new connection to a session goes on with what it held.
                match self.by_client.get_mut(client_id) {
                    Some(r) => r.attach(st),
                    None => {
                        self.by_client.insert(client_id.into(), Replayed::new(st));
                    }
                }
            }
            END => {
                self.by_client.remove(d.str()?);
            }
            _ => {
                let client_id = d.str()?;
                let r = self
                    .by_client
                    .get_mut(client_id)
                    .ok_or("record of an unknown session")?;
                r.update(kind, &mut d, clock, self.msg.as_ref())?;
            }
        }
        d.end()
    }

    /// The sessions, those still connected taken as lost `now`: their
    /// expiry starts and their will is due after its delay.
    pub(crate) fn finish(self, now: Instant) -> Vec<SessionState> {
        let mut out = Vec::with_capacity(self.by_client.len());
        for r in self.by_client.into_values() {
            out.push(r.finish(now));
        }
        out
    }
}

impl Replayed {
    const fn new(st: SessionState) -> Replayed {
        Replayed {
            st,
            subs: BTreeMap::new(),
            awaiting_rel: BTreeSet::new(),
            items: BTreeMap::new(),
            last_seq: 0,
            connected: true,
        }
    }

    /// A new connection with the header `st`.
    fn attach(&mut self, st: SessionState) {
        let next_pkid = self.st.next_pkid;
        self.st = SessionState { next_pkid, ..st };
        self.connected = true;
    }

    /// Applies the rest of record `kind`, which names this session. `msg`
    /// is the message of the last [`MSG`] record.
    fn update(
        &mut self,
        kind: u8,
        d: &mut Decoder<'_>,
        clock: Clock,
        msg: Option<&Msg>,
    ) -> Result<(), &'static str> {
        match kind {
            DOWN => self.down(d, clock)?,
            SUB => {
                let filter = d.str()?;
                validate_filter(filter).map_err(|_| "invalid filter")?;
                let opts = SubscriptionOptions::from_byte(d.u8()?, Version::V5)
                    .map_err(|_| "invalid subscription options")?;
                self.subs.insert(filter.into(), (opts, d.u32()?));
            }
            UNSUB => {
                self.subs.remove(d.str()?);
            }
            WILL_SENT => self.st.delayed_will = None,
            QUEUED => self.queued(d, msg.ok_or("delivery without a message")?)?,
            DONE => {
                self.items.remove(&d.u64()?);
            }
            SENT => {
                let seq = d.u64()?;
                let pkid = d.u16()?;
                if let Some(item) = self.items.get_mut(&seq) {
                    item.pkid = Some(pkid);
                    self.st.next_pkid = pkid;
                }
            }
            RELEASED => {
                if let Some(item) = self.items.get_mut(&d.u64()?) {
                    item.released = true;
                }
            }
            INBOUND => {
                self.awaiting_rel.insert(d.u16()?);
            }
            INBOUND_DONE => {
                self.awaiting_rel.remove(&d.u16()?);
            }
            _ => return Err("unknown session record"),
        }
        Ok(())
    }

    fn down(&mut self, d: &mut Decoder<'_>, clock: Clock) -> Result<(), &'static str> {
        self.st.expiry = d.u32()?;
        self.st.expires_at = clock.instant(d.u64()?);
        self.st.delayed_will = if d.flag()? {
            let due = clock
                .instant(d.u64()?)
                .ok_or("delayed will without a time")?;
            Some((due, d.msg(clock)?))
        } else {
            None
        };
        self.st.will = None;
        self.connected = false;
        Ok(())
    }

    fn queued(&mut self, d: &mut Decoder<'_>, msg: &Msg) -> Result<(), &'static str> {
        let seq = d.u64()?;
        let qos = d.qos()?;
        if qos == QoS::AtMostOnce {
            return Err("QoS 0 message held");
        }
        let retain = d.flag()?;
        let mut sub_id = SubIds::default();
        for _ in 0..4 {
            sub_id.add(d.u32()?);
        }
        let queued = Queued {
            msg: msg.clone(),
            qos,
            retain,
            sub_id,
        };
        let item = Item {
            queued,
            pkid: None,
            released: false,
        };
        self.items.insert(seq, item);
        self.last_seq = self.last_seq.max(seq);
        Ok(())
    }

    /// The session; still connected, it lost its connection `now`.
    fn finish(self, now: Instant) -> SessionState {
        let mut st = self.st;
        st.subs = self
            .subs
            .into_iter()
            .map(|(f, (o, id))| (f, o, id))
            .collect();
        st.awaiting_rel = self.awaiting_rel.into_iter().collect();
        let mut queue_seqs = Vec::new();
        for (seq, item) in self.items {
            match item.pkid {
                Some(pkid) => {
                    st.inflight.push((pkid, item.queued, item.released));
                    st.seqs.push(seq);
                }
                // Released under an ID the journal lost: the client has it.
                None if item.released => {}
                None => {
                    st.queue.push(item.queued);
                    queue_seqs.push(seq);
                }
            }
        }
        st.seqs.extend(queue_seqs);
        st.last_seq = self.last_seq;
        if self.connected {
            lost(&mut st, now);
        }
        st
    }
}

/// Decodes the rest of an [`IMAGE`] record.
fn image(d: &mut Decoder<'_>, clock: Clock) -> Result<Replayed, &'static str> {
    let connected = d.flag()?;
    let mut st = d.session(clock)?;
    let last_seq = d.u64()?;
    let n = d.count(8)?;
    if n != st.inflight.len() + st.queue.len() {
        return Err("delivery numbers do not match the deliveries");
    }
    let mut items = BTreeMap::new();
    for (pkid, queued, released) in std::mem::take(&mut st.inflight) {
        let item = Item {
            queued,
            pkid: Some(pkid),
            released,
        };
        items.insert(d.u64()?, item);
    }
    for queued in std::mem::take(&mut st.queue) {
        let item = Item {
            queued,
            pkid: None,
            released: false,
        };
        items.insert(d.u64()?, item);
    }
    let subs = std::mem::take(&mut st.subs)
        .into_iter()
        .map(|(f, o, id)| (f, (o, id)))
        .collect();
    let awaiting_rel = std::mem::take(&mut st.awaiting_rel).into_iter().collect();
    Ok(Replayed {
        st,
        subs,
        awaiting_rel,
        items,
        last_seq,
        connected,
    })
}

/// Session `st` lost its connection at `now`, as `Lane::detach` would
/// have it.
fn lost(st: &mut SessionState, now: Instant) {
    let expiry = st.expiry;
    if expiry != u32::MAX {
        st.expires_at = now.checked_add(Duration::from_secs(u64::from(expiry)));
    }
    if let Some(w) = st.will.take() {
        let delay = Duration::from_secs(u64::from(w.delay.min(expiry)));
        st.delayed_will = Some((now.checked_add(delay).unwrap_or(now), w.msg));
    }
}

/// Decodes the rest of a [`SESSION`] record into a session without
/// subscriptions or messages.
fn session_head(
    d: &mut Decoder<'_>,
    client_id: &str,
    clock: Clock,
) -> Result<SessionState, &'static str> {
    if client_id.is_empty() {
        return Err("empty client ID");
    }
    let username = if d.flag()? {
        Some(d.str()?.into())
    } else {
        None
    };
    let version = match d.u8()? {
        3 => Version::V31,
        4 => Version::V311,
        5 => Version::V5,
        _ => return Err("invalid protocol level"),
    };
    let expiry = d.u32()?;
    let will = if d.flag()? {
        let delay = d.u32()?;
        Some(Will {
            msg: d.msg(clock)?,
            delay,
        })
    } else {
        None
    };
    Ok(SessionState {
        client_id: client_id.into(),
        username,
        version,
        expiry,
        expires_at: None,
        will,
        delayed_will: None,
        next_pkid: 0,
        subs: Vec::new(),
        awaiting_rel: Vec::new(),
        inflight: Vec::new(),
        queue: Vec::new(),
        seqs: Vec::new(),
        last_seq: 0,
    })
}
