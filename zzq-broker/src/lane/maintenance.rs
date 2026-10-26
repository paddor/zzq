//! Periodic sweeps, the state budget, and closing for a dump or shutdown.

use std::io;
use std::ops::Range;
use std::rc::Rc;
use std::sync::atomic::Ordering;
use std::sync::{Arc, PoisonError};
use std::time::{Duration, Instant};

use zzq_proto::{SubscriptionOptions, reason};

use crate::conn::ConnShared;
use crate::dump::Part;
use crate::session::Session;
use crate::slab::Key;
use crate::state::SessionState;

use super::{Close, Lane, Remote, SWEEP_PARTS, SyncAck};

impl Lane {
    /// Keep alive timeouts and session expiry, for the next of
    /// [`SWEEP_PARTS`] parts of the sessions.
    pub(crate) fn sweep(&mut self, now: Instant) {
        self.check_state();
        let part = self.sweeps % SWEEP_PARTS;
        self.sweeps = self.sweeps.wrapping_add(1);
        // The retained store is shared; one lane, once a round, is enough.
        if self.id == 0 && part == 0 {
            self.shared
                .retained
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .sweep(now);
        }
        for slot in sweep_part(part, self.sessions.slots()) {
            let Some(key) = self.sessions.key_at(slot) else {
                continue;
            };
            let Some(s) = self.sessions.get_mut(key) else {
                continue;
            };
            s.trim_buffers();
            if let Some(c) = &s.conn {
                let ka = c.keep_alive.get();
                if ka > 0
                    && now.duration_since(c.last_rx.get())
                        > Duration::from_millis(u64::from(ka) * 1500)
                {
                    Self::kick(c, s.version, reason::KEEP_ALIVE_TIMEOUT);
                }
            } else if s.expires_at.is_some_and(|t| t <= now) {
                self.remove_session(key);
            }
        }
    }

    /// Publishes this lane's state bytes, and checks `max_state_bytes`
    /// against those of all lanes and the retained store.
    fn check_state(&mut self) {
        let max = self.shared.opts.max_state_bytes;
        if max == usize::MAX {
            return;
        }
        self.shared.lanes[usize::from(self.id)]
            .state
            .store(self.state.get(), Ordering::Relaxed);
        let retained = self
            .shared
            .retained
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .bytes();
        let lanes: usize = self
            .shared
            .lanes
            .iter()
            .map(|l| l.state.load(Ordering::Relaxed))
            .sum();
        self.state_full = lanes + retained >= max;
    }

    /// Closes every connection as lost, so wills and session expiry run
    /// as usual, and refuses new ones.
    pub(super) fn quiesce(&mut self) {
        self.closing = true;
        let connected: Vec<(Key, Rc<ConnShared>)> = self
            .sessions
            .keys()
            .filter_map(|k| Some((k, self.sessions.get(k)?.conn.clone()?)))
            .collect();
        for (key, conn) in connected {
            if let Some(s) = self.sessions.get(key) {
                Self::kick(&conn, s.version, reason::SERVER_SHUTTING_DOWN);
            }
            self.detach(key, conn.id, Close::Kicked);
        }
    }

    /// Queues a marker for every other lane behind what waits for it.
    pub(super) fn drain(&mut self, sync: &Arc<SyncAck>) {
        for (lane, batch) in self.remote.iter_mut().enumerate() {
            if lane != usize::from(self.id) {
                batch.push(Remote::Drained(sync.clone()));
            }
        }
    }

    /// Writes the sessions that outlive their connection.
    pub(super) fn dump(&self, part: &mut Part) -> io::Result<()> {
        for key in self.sessions.keys() {
            if let Some(s) = self.sessions.get(key)
                && s.expiry != 0
            {
                part.session(s)?;
            }
        }
        part.flush()
    }

    /// Adds sessions from a state file, with their subscriptions on every
    /// lane and their delayed wills timed.
    pub(super) fn restore(&mut self, states: Vec<SessionState>, sync: &Arc<SyncAck>) {
        let lanes = self.shared.lanes.len();
        let now = Instant::now();
        for st in states {
            if self.by_client.contains_key(&st.client_id) {
                continue;
            }
            let (session, will) = Session::restore(st, self.state.clone(), lanes);
            let subs: Vec<(Box<str>, SubscriptionOptions, u32)> = session
                .subs
                .iter()
                .map(|(f, (opts, id))| (f.clone(), *opts, *id))
                .collect();
            let client_id = session.client_id.clone();
            let key = self.sessions.insert(session);
            self.by_client.insert(client_id, key);
            for (filter, opts, sub_id) in subs {
                let entry = self.entry_for(key, &filter, opts, sub_id);
                self.subscribe_entry(&filter, &entry, Some(sync));
            }
            if let Some((due, msg)) = will {
                self.schedule_will(key, msg, due.saturating_duration_since(now));
            }
        }
        self.stats.subscriptions = self.index.len() as u64;
    }

    pub(super) fn shutdown(&mut self) {
        self.shut_down = true;
        let keys: Vec<Key> = self.sessions.keys().collect();
        for key in keys {
            if let Some(s) = self.sessions.get_mut(key)
                && let Some(c) = s.conn.take()
            {
                Self::kick(&c, s.version, reason::SERVER_SHUTTING_DOWN);
            }
        }
    }
}

/// The session slots that sweep `part` of a round covers: its share of
/// `slots`, so each slot is swept once a round, however few there are.
fn sweep_part(part: u32, slots: u32) -> Range<u32> {
    let step = slots.div_ceil(SWEEP_PARTS);
    let from = part.saturating_mul(step).min(slots);
    from..from.saturating_add(step).min(slots)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sweep_parts_cover_each_slot_once_a_round() {
        for slots in [0, 1, 2, 19, 20, 21, 41, 1000, 20_001] {
            let mut swept = vec![0u8; slots as usize];
            for part in 0..SWEEP_PARTS {
                for slot in sweep_part(part, slots) {
                    swept[slot as usize] += 1;
                }
            }
            assert!(swept.iter().all(|&n| n == 1), "{slots} slots");
        }
    }
}
