//! Atomic records for cross-lane handoffs.

use super::{JournalId, Journaled, Lane, LogState, Wait, io, rec};
use crate::state::handoff::Pending;

impl Lane {
    pub(in crate::lane) fn journal_handoff(&mut self, p: &Pending) -> Option<Journaled> {
        let j = self.log().append(
            (&self.sessions, &self.handoffs.state),
            rec::HANDOFF,
            |b, c| {
                rec::put_pending(b, p, c);
            },
        );
        self.appended(j);
        j
    }

    /// Journals that handoff `seq` finished. Nothing waits for that, and a
    /// crash that loses it only sends the handoff again: it goes out with the
    /// next write, or with the next sweep.
    pub(in crate::lane) fn journal_handed(&mut self, seq: u64) {
        self.log().append(
            (&self.sessions, &self.handoffs.state),
            rec::HANDED,
            |b, _| {
                b.extend_from_slice(&seq.to_be_bytes());
            },
        );
    }

    /// All QUEUED/SENT records and the receipt share one journal record.
    pub(in crate::lane) fn begin_handoff(&mut self) -> bool {
        #[cfg(test)]
        if self
            .log()
            .refuse_transactions
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return false;
        }
        let mut st = self.log().lock();
        if st.closed || st.log.is_none() {
            return false;
        }
        if st.stale
            && st
                .write_all((&self.sessions, &self.handoffs.state))
                .is_err()
        {
            return false;
        }
        st.transaction.clear();
        st.in_transaction = true;
        true
    }

    pub(in crate::lane) fn end_handoff(&mut self, from: u16, seq: u64) -> Journaled {
        let log = self.log();
        let mut st = log.lock();
        let state = (&self.sessions, &self.handoffs.state);
        let appended = st.append(
            rec::RECEIVED,
            state,
            |b, _| {
                b.extend_from_slice(&from.to_be_bytes());
                b.extend_from_slice(&seq.to_be_bytes());
            },
            &[],
        );
        let j = if appended.is_ok() {
            commit(&mut st, self.id)
        } else {
            st.in_transaction = false;
            st.stale = true;
            Journaled::Failed
        };
        drop(st);
        self.appended(Some(j));
        j
    }

    /// Ends a transaction that needs no receipt. `None` if it has no records.
    pub(in crate::lane) fn end_transaction(&mut self) -> Option<Journaled> {
        let mut st = self.log().lock();
        if st.transaction.is_empty() {
            st.in_transaction = false;
            return None;
        }
        let j = commit(&mut st, self.id);
        drop(st);
        self.appended(Some(j));
        Some(j)
    }
}

/// Appends the transaction as one record, which ends it.
fn commit(st: &mut LogState, lane: u16) -> Journaled {
    st.in_transaction = false;
    let LogState {
        log, transaction, ..
    } = st;
    let appended = log.as_mut().map_or_else(
        || Err(io::ErrorKind::NotConnected.into()),
        |log| rec::append_record(log, rec::TRANSACTION, &[transaction]),
    );
    if let Ok(n) = appended {
        st.last = n;
        Journaled::At {
            wait: Wait {
                journal: JournalId::Lane(usize::from(lane)),
                n,
            },
            crowded: false,
        }
    } else {
        st.stale = true;
        Journaled::Failed
    }
}
