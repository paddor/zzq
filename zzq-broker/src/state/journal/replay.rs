//! Sessions rebuilt from the records of a lane journal.

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::time::{Duration, Instant};

use rustc_hash::FxHashMap;
use zzq_proto::topic::validate_filter;
use zzq_proto::{SubscriptionOptions, Version};

use crate::disk::Append;
use crate::msg::Msg;
use crate::session::Queued;
use crate::state::codec::Decoder;
use crate::state::handoff::{self, Handoffs, LaneState};
use crate::state::{Clock, Deliveries, SessionState, Snapshotted};

use super::{
    CLEAR, CLEARED, DONE, DOWN, END, HANDED, HANDOFF, HANDOFF_KEPT, IMAGE, INBOUND, INBOUND_DONE,
    MSG, PART, PART_BYTES, PART_END, QUEUED, RECEIVED, RELEASED, ROUTING, SENT, SESSION, SUB,
    TRANSACTION, UNSUB, WILL_SENT,
};

/// The sessions a lane journal leaves, record by record.
#[derive(Debug, Default)]
pub(crate) struct Sessions {
    /// The lane whose journal it reads, of `lanes`. With 0 lanes, of
    /// any lane: its sessions and handoffs are not checked.
    lane: usize,
    lanes: usize,
    state: Snapshotted<LaneReplay>,
    /// The message of the last [`MSG`] record, which [`QUEUED`] records
    /// after it deliver.
    msg: Option<Msg>,
    /// The [`PART`] records of a record that has not ended.
    pub(super) parts: Vec<u8>,
}

/// What a lane journal holds: its sessions and handoffs.
#[derive(Debug, Default)]
struct LaneReplay {
    by_client: FxHashMap<Box<str>, Replayed>,
    handoffs: Handoffs,
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
struct Item {
    queued: Queued,
    /// The packet ID it went out under, if that matters.
    pkid: Option<u16>,
    released: bool,
}

impl Sessions {
    /// The sessions of lane `lane` of `lanes`.
    pub(crate) fn new(lane: usize, lanes: usize) -> Sessions {
        Sessions {
            lane,
            lanes,
            ..Sessions::default()
        }
    }

    /// Whether `client_id` belongs to the journal's lane.
    fn owns(&self, client_id: &str) -> bool {
        self.lanes == 0 || crate::broker::lane_for(client_id, self.lanes) == self.lane
    }

    /// Applies a record.
    pub(crate) fn apply(
        &mut self,
        kind: u8,
        body: &[u8],
        clock: Clock,
    ) -> Result<(), &'static str> {
        match kind {
            PART => {
                let (&first, piece) = body.split_first().ok_or("empty record part")?;
                if first != 0 {
                    self.parts.clear();
                }
                self.parts.extend_from_slice(piece);
                Ok(())
            }
            PART_END => {
                let (&kind, piece) = body.split_first().ok_or("empty record end")?;
                let mut whole = std::mem::take(&mut self.parts);
                whole.extend_from_slice(piece);
                self.apply_whole(kind, &whole, clock)
            }
            _ => {
                // What a failed write left of a long record.
                self.parts.clear();
                self.apply_whole(kind, body, clock)
            }
        }
    }

    fn apply_whole(&mut self, kind: u8, body: &[u8], clock: Clock) -> Result<(), &'static str> {
        if kind != TRANSACTION {
            return self.apply_one(kind, body, clock);
        }
        let mut d = Decoder(body);
        while !d.0.is_empty() {
            let kind = d.u8()?;
            if matches!(
                kind,
                TRANSACTION
                    | CLEAR
                    | CLEARED
                    | IMAGE
                    | ROUTING
                    | HANDOFF
                    | HANDOFF_KEPT
                    | HANDED
                    | PART
                    | PART_END
            ) {
                return Err("invalid transaction record");
            }
            let len = d.u32()? as usize;
            self.apply_one(kind, d.take(len)?, clock)?;
        }
        Ok(())
    }

    /// Decodes a [`HANDOFF`] or [`HANDOFF_KEPT`] body, and checks it belongs
    /// in the journal of this lane.
    fn pending(&self, d: &mut Decoder<'_>, clock: Clock) -> Result<handoff::Pending, &'static str> {
        let p = handoff::read_pending(d, clock)?;
        if self.lanes != 0
            && p.targets
                .iter()
                .any(|t| crate::broker::lane_for(&t.client, self.lanes) != usize::from(t.lane))
        {
            return Err("handoff destination belongs to another lane");
        }
        if !self.owns(&p.publisher) {
            return Err("handoff in the journal of another lane");
        }
        Ok(p)
    }

    #[expect(clippy::too_many_lines, reason = "one arm per journal record")]
    fn apply_one(&mut self, kind: u8, body: &[u8], clock: Clock) -> Result<(), &'static str> {
        let mut d = Decoder(body);
        match kind {
            CLEAR => self.state.begin(),
            CLEARED => self.state.end(),
            IMAGE => {
                let r = image(&mut d, clock)?;
                if !self.owns(&r.st.client_id) {
                    return Err("session in the journal of another lane");
                }
                let into = self.state.target();
                into.handoffs.serial = into.handoffs.serial.max(r.st.birth);
                into.by_client.insert(r.st.client_id.clone(), r);
            }
            ROUTING => {
                handoff::read_meta(&mut d, &mut self.state.target().handoffs)?;
            }
            HANDOFF_KEPT => {
                let p = self.pending(&mut d, clock)?;
                let h = &mut self.state.target().handoffs;
                h.serial = h.serial.max(p.seq);
                h.pending.insert(p.seq, std::sync::Arc::new(p));
            }
            // Any other record after an unfinished snapshot: the snapshot
            // was cut off, and the lane went on without it.
            _ if self.state.cut() => return self.apply_one(kind, body, clock),
            HANDOFF => {
                let p = self.pending(&mut d, clock)?;
                if let Some(r) = self
                    .state
                    .live
                    .by_client
                    .get_mut(&p.publisher)
                    .filter(|r| r.st.birth == p.birth)
                {
                    r.awaiting_rel.insert(p.pkid);
                }
                self.state.live.handoffs.serial = self.state.live.handoffs.serial.max(p.seq);
                self.state
                    .live
                    .handoffs
                    .pending
                    .insert(p.seq, std::sync::Arc::new(p));
            }
            HANDED => {
                self.state.live.handoffs.pending.remove(&d.u64()?);
            }
            RECEIVED => {
                self.state.live.handoffs.receive(d.u16()?, d.u64()?);
            }
            MSG => self.msg = Some(d.msg(clock)?),
            SESSION => {
                let client_id = d.str()?;
                let st = session_head(&mut d, client_id, clock)?;
                self.state.live.handoffs.serial = self.state.live.handoffs.serial.max(st.birth);
                if !self.owns(client_id) {
                    return Err("session in the journal of another lane");
                }
                // A new connection to a session goes on with what it held.
                match self.state.live.by_client.get_mut(client_id) {
                    Some(r) => r.attach(st),
                    None => {
                        self.state
                            .live
                            .by_client
                            .insert(client_id.into(), Replayed::new(st));
                    }
                }
            }
            END => {
                self.state.live.by_client.remove(d.str()?);
            }
            _ => {
                let client_id = d.str()?;
                let r = self
                    .state
                    .live
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
    #[cfg(any(test, feature = "fuzzing"))]
    pub(crate) fn finish(self, now: Instant) -> Vec<SessionState> {
        self.into_lane(now).sessions
    }

    pub(crate) fn into_lane(self, now: Instant) -> LaneState {
        let mut out = Vec::with_capacity(self.state.live.by_client.len());
        for r in self.state.live.by_client.into_values() {
            out.push(r.finish(now));
        }
        LaneState {
            sessions: out,
            handoffs: self.state.live.handoffs,
        }
    }

    /// Appends records that rebuild these sessions as they stand, with
    /// their delivery numbers and connections, then the message for the
    /// deliveries that may follow.
    pub(crate) fn image(&self, out: &mut impl Append, clock: Clock) -> io::Result<()> {
        let mut buf = Vec::new();
        out.append(CLEAR, &[])?;
        for r in self.state.live.by_client.values() {
            buf.clear();
            super::image_state(&mut buf, r.clone().into_state(), r.connected, clock);
            super::append_record(out, IMAGE, &[&buf])?;
        }
        super::handoffs(out, &self.state.live.handoffs, &mut buf, clock)?;
        out.append(CLEARED, &[])?;
        if let Some(m) = &self.msg {
            buf.clear();
            crate::state::msg_head(&mut buf, m, clock);
            super::append_record(out, MSG, &[&buf, m.payload()])?;
        }
        // The rest of a long record may come in the ops after the image.
        for (i, piece) in self.parts.chunks(PART_BYTES).enumerate() {
            out.append(PART, &[&[u8::from(i == 0)], piece])?;
        }
        Ok(())
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
        self.st.delayed_will = d.delayed_will(clock)?;
        self.st.will = None;
        self.connected = false;
        Ok(())
    }

    fn queued(&mut self, d: &mut Decoder<'_>, msg: &Msg) -> Result<(), &'static str> {
        let seq = d.u64()?;
        let queued = Queued {
            grant: d.held_grant()?,
            msg: msg.clone(),
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
        let connected = self.connected;
        let mut st = self.into_state();
        if connected {
            lost(&mut st, now);
        }
        st
    }

    /// The session as it stands, its deliveries in order.
    fn into_state(self) -> SessionState {
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
                    st.deliveries
                        .inflight
                        .push((pkid, item.queued, item.released));
                    st.deliveries.seqs.push(seq);
                }
                // Released under an ID the journal lost: the client has it.
                None if item.released => {}
                None => {
                    st.deliveries.queue.push(item.queued);
                    queue_seqs.push(seq);
                }
            }
        }
        st.deliveries.seqs.extend(queue_seqs);
        st.deliveries.last_seq = self.last_seq;
        st
    }
}

/// Decodes the rest of an [`IMAGE`] record.
fn image(d: &mut Decoder<'_>, clock: Clock) -> Result<Replayed, &'static str> {
    let connected = d.flag()?;
    let mut st = d.session(clock)?;
    let last_seq = d.u64()?;
    let n = d.count(8)?;
    let Deliveries {
        inflight, queue, ..
    } = std::mem::take(&mut st.deliveries);
    if n != inflight.len() + queue.len() {
        return Err("delivery numbers do not match the deliveries");
    }
    let mut items = BTreeMap::new();
    for (pkid, queued, released) in inflight {
        let item = Item {
            queued,
            pkid: Some(pkid),
            released,
        };
        items.insert(d.u64()?, item);
    }
    for queued in queue {
        let item = Item {
            queued,
            pkid: None,
            released: false,
        };
        items.insert(d.u64()?, item);
    }
    st.birth = d.birth()?;
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
    let username = d.opt_str()?.map(Into::into);
    let version = d.version()?;
    let expiry = d.u32()?;
    let will = d.will(clock)?;
    let birth = d.birth()?;
    Ok(SessionState {
        birth,
        username,
        expiry,
        will,
        ..SessionState::new(client_id.into(), version)
    })
}
