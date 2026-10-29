//! A follower's state: what the leader's journals hold, rebuilt from the
//! ops it replicates with the replay a start from disk uses. No lane
//! serves it until the node leads.

use rustc_hash::FxHashMap;
use zzq_cluster::OpNumber;

use crate::broker::lane_for;
use crate::retain::{self, STRIPES};
use crate::state::journal::Sessions;
use crate::state::{Clock, SessionState};

use super::GroupWriter;
use super::image::Part;
use super::log::{Journal, records};

/// Sessions per lane and retained messages per stripe.
#[derive(Debug)]
pub(crate) struct Accumulators {
    lanes: Vec<Sessions>,
    retained: retain::Accumulator,
    /// The op through which an image gave a journal's state: replay skips
    /// its ops up to there.
    cuts: FxHashMap<Journal, OpNumber>,
}

impl Accumulators {
    pub(crate) fn new(lanes: usize, max_levels: usize) -> Accumulators {
        Accumulators {
            lanes: (0..lanes).map(|_| Sessions::default()).collect(),
            retained: retain::Accumulator::new(max_levels),
            cuts: FxHashMap::default(),
        }
    }

    /// Applies the records of op `op`, unless an image held them.
    ///
    /// # Errors
    ///
    /// When the op is malformed or does not fit the state.
    pub(crate) fn apply(&mut self, op: OpNumber, body: &[u8]) -> Result<(), &'static str> {
        let (journal, _) = records(body).ok_or("malformed op")?;
        if self.cuts.get(&journal).is_some_and(|&cut| op <= cut) {
            return Ok(());
        }
        self.apply_body(body)
    }

    fn apply_body(&mut self, body: &[u8]) -> Result<(), &'static str> {
        let (journal, recs) = records(body).ok_or("malformed op")?;
        let lanes = self.lanes.len();
        let clock = Clock::now();
        for rec in recs {
            let (kind, body) = rec.map_err(|()| "malformed record")?;
            match journal {
                Journal::Lane(j) => {
                    let j = usize::from(j);
                    let sessions = self.lanes.get_mut(j).ok_or("op of an unknown lane")?;
                    sessions.apply(kind, body, clock, |id| lane_for(id, lanes) == j)?;
                }
                Journal::Retained(s) => self.retained.apply(usize::from(s), kind, body)?,
            }
        }
        Ok(())
    }

    /// The state of an image: each part's journal through its op.
    ///
    /// # Errors
    ///
    /// When a part is malformed or does not fit the state.
    pub(crate) fn from_image(
        lanes: usize,
        max_levels: usize,
        parts: &[Part],
    ) -> Result<Accumulators, &'static str> {
        let mut acc = Accumulators::new(lanes, max_levels);
        for p in parts {
            let (journal, _) = records(&p.body).ok_or("malformed image part")?;
            acc.apply_body(&p.body)?;
            acc.cuts.insert(journal, p.op);
        }
        Ok(acc)
    }

    /// An image of the state, every part through op `applied`, or the op
    /// an image gave its journal, if later.
    pub(crate) fn image(&self, applied: OpNumber) -> Vec<Part> {
        let clock = Clock::now();
        let mut parts = Vec::with_capacity(self.lanes.len() + STRIPES);
        for (j, sessions) in self.lanes.iter().enumerate() {
            let Ok(j) = u16::try_from(j) else { break };
            let mut w = GroupWriter::new(Journal::Lane(j));
            if sessions.image(&mut w, clock).is_ok() {
                parts.push(self.part(applied, w));
            }
        }
        for s in 0..STRIPES {
            let Ok(stripe) = u8::try_from(s) else { break };
            let mut w = GroupWriter::new(Journal::Retained(stripe));
            if self.retained.image(s, &mut w).is_ok() {
                parts.push(self.part(applied, w));
            }
        }
        parts
    }

    fn part(&self, applied: OpNumber, w: GroupWriter) -> Part {
        let body = w.finish();
        let cut = records(&body).and_then(|(j, _)| self.cuts.get(&j).copied());
        Part {
            op: cut.map_or(applied, |c| c.max(applied)),
            body,
        }
    }

    /// The sessions of each lane, those still connected at the old leader
    /// taken as lost now, and the retained messages.
    pub(crate) fn finish(self) -> (Vec<Vec<SessionState>>, retain::Accumulator) {
        let now = crate::time::now();
        let sessions = self.lanes.into_iter().map(|s| s.finish(now)).collect();
        (sessions, self.retained)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;
    use crate::disk::Append;
    use crate::state::journal::record_seeds;

    /// An op of lane 0 with the seed records at `picks`.
    fn op(picks: &[usize]) -> Bytes {
        let seeds = record_seeds();
        let mut w = GroupWriter::new(Journal::Lane(0));
        for &i in picks {
            w.append(seeds[i][0], &[&seeds[i][1..]]).unwrap();
        }
        w.finish()
    }

    // SESSION, SUB, DOWN, MSG, QUEUED and SENT: a session gone offline
    // with a delivery in flight, its message still current.
    const LIVE: [usize; 6] = [0, 1, 4, 8, 9, 10];
    const DONE: usize = 12;

    /// The sessions, but for their instants, which each decode takes
    /// from its own clock.
    fn state(acc: Accumulators) -> String {
        let mut s = format!("{:?}", acc.finish().0);
        while let Some(a) = s.find("Instant {") {
            let b = s[a..].find('}').map_or(s.len(), |b| a + b + 1);
            s.replace_range(a..b, "Instant");
        }
        s
    }

    fn live() -> Accumulators {
        let mut acc = Accumulators::new(1, 8);
        acc.apply(OpNumber(1), &op(&LIVE)).unwrap();
        acc
    }

    #[test]
    fn an_image_rebuilds_the_sessions_with_their_delivery_numbers() {
        let mut rebuilt = Accumulators::from_image(1, 8, &live().image(OpNumber(1))).unwrap();
        let mut acc = live();
        // The next op names the delivery by the number the leader gave it.
        for a in [&mut acc, &mut rebuilt] {
            a.apply(OpNumber(2), &op(&[DONE])).unwrap();
        }
        assert_eq!(state(rebuilt), state(acc));
    }

    #[test]
    fn replay_skips_the_ops_of_a_journal_its_image_holds() {
        let parts = live().image(OpNumber(5));
        assert!(parts.iter().all(|p| p.op == OpNumber(5)));
        let fresh = || Accumulators::from_image(1, 8, &parts).unwrap();
        let mut skipped = fresh();
        skipped.apply(OpNumber(5), &op(&[DONE])).unwrap();
        assert_eq!(state(skipped), state(fresh()), "op 5 is in the image");
        let mut rebuilt = fresh();
        rebuilt.apply(OpNumber(6), &op(&[DONE])).unwrap();
        let mut done = live();
        done.apply(OpNumber(6), &op(&[DONE])).unwrap();
        assert_eq!(state(rebuilt), state(done));
        // An image of an image keeps each journal's later cut.
        let again = fresh().image(OpNumber(3));
        assert!(again.iter().all(|p| p.op == OpNumber(5)));
    }

    #[test]
    fn a_malformed_op_or_part_is_refused() {
        let mut acc = Accumulators::new(1, 8);
        assert!(acc.apply(OpNumber(1), &[9, 9]).is_err());
        let part = Part {
            op: OpNumber(1),
            body: Bytes::from_static(&[0, 0, 0, 1]),
        };
        assert!(Accumulators::from_image(1, 8, &[part]).is_err());
    }
}
