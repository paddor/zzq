//! A retained update and its source receipt are one durable record.

use super::tree::Receipt;
use super::{
    Clock, JournalId, Journaled, Msg, ONCE, Ordering, REMOVE, Record, Replay, Retained, SET, State,
    Tree, Wait, io, state, stripe_of,
};

impl Retained {
    pub(crate) fn set_once(&self, from: u16, seq: u64, msg: &Msg, full: bool) -> Option<Journaled> {
        let i = stripe_of(msg.topic());
        let mut st = self.stripes[i].lock();
        if let Some(r) = st
            .tree
            .receipts
            .get(usize::from(from))
            .filter(|r| r.sequence >= seq)
        {
            return r.written;
        }
        let operation = if msg.payload().is_empty() {
            if st.tree.remove(msg.topic()) {
                self.len.fetch_sub(1, Ordering::Relaxed);
            }
            2
        } else if st.tree.stored(msg.topic()).is_none() && (full || !self.reserve()) {
            self.rejected.fetch_add(1, Ordering::Relaxed);
            0
        } else {
            st.tree.insert(msg.compact());
            1
        };
        let written = if let Ok(n) = st.journal_once(from, seq, operation, msg) {
            Some(Journaled::At {
                wait: Wait {
                    journal: JournalId::Retained(i),
                    n,
                },
                crowded: false,
            })
        } else {
            st.stale = true;
            self.write_errors.fetch_add(1, Ordering::Relaxed);
            Some(Journaled::Failed)
        };
        let n = st.tree.receipts.len().max(usize::from(from) + 1);
        st.tree.receipts.resize(n, Receipt::default());
        st.tree.receipts[usize::from(from)] = Receipt {
            sequence: seq,
            written,
        };
        written
    }

    /// The newest handoff of lane `from` any stripe has seen.
    pub(crate) fn receipt(&self, from: u16) -> u64 {
        self.stripes
            .iter()
            .filter_map(|s| Some(s.lock().tree.receipts.get(usize::from(from))?.sequence))
            .max()
            .unwrap_or(0)
    }

    /// Used during a lane-count migration, before completion tasks start.
    pub(crate) fn sync(&self) -> io::Result<()> {
        for stripe in &self.stripes {
            let mut st = stripe.lock();
            if let Some(log) = st.log.as_mut() {
                let n = log.sync()?;
                stripe.cursor.advance(n);
            }
        }
        Ok(())
    }
}

impl State {
    fn journal_once(&mut self, from: u16, seq: u64, operation: u8, msg: &Msg) -> io::Result<u64> {
        if self.closed {
            return Err(io::ErrorKind::NotConnected.into());
        }
        if self.stale {
            self.write_all()?;
        }
        let log = self.log.as_mut().ok_or(io::ErrorKind::NotConnected)?;
        self.head.clear();
        self.head.extend_from_slice(&from.to_be_bytes());
        self.head.extend_from_slice(&seq.to_be_bytes());
        self.head.push(operation);
        let tail = match operation {
            1 => {
                state::msg_head(&mut self.head, msg, Clock::now());
                msg.payload()
            }
            2 => msg.topic().as_bytes(),
            _ => &[],
        };
        let n = log.append(ONCE, &[&self.head, tail])?;
        log.flush()?;
        Ok(n)
    }
}

pub(super) fn read_receipts(tree: &mut Tree, body: &[u8]) -> Result<(), &'static str> {
    if !body.len().is_multiple_of(8) || body.len() / 8 > usize::from(u16::MAX) + 1 {
        return Err("invalid retained receipts");
    }
    tree.receipts.clear();
    for bytes in body.as_chunks::<8>().0 {
        tree.receipts.push(Receipt {
            sequence: u64::from_be_bytes(*bytes),
            written: None,
        });
    }
    Ok(())
}

pub(super) fn replay(
    r: Replay,
    tree: &mut Tree,
    stripe: usize,
    body: &[u8],
) -> Result<(), &'static str> {
    let (head, body) = body
        .split_first_chunk::<11>()
        .ok_or("short retained handoff")?;
    let from = usize::from(u16::from_be_bytes([head[0], head[1]]));
    let seq = u64::from_be_bytes([
        head[2], head[3], head[4], head[5], head[6], head[7], head[8], head[9],
    ]);
    if seq == 0 {
        return Err("invalid retained handoff sequence");
    }
    if tree
        .receipts
        .get(from)
        .is_some_and(|receipt| receipt.sequence >= seq)
    {
        return Ok(());
    }
    match head[10] {
        0 if body.is_empty() => {}
        1 => r.apply(tree, &mut None, stripe, Record { kind: SET, body })?,
        2 => r.apply(tree, &mut None, stripe, Record { kind: REMOVE, body })?,
        _ => return Err("invalid retained handoff operation"),
    }
    tree.receipts
        .resize(tree.receipts.len().max(from + 1), Receipt::default());
    tree.receipts[from] = Receipt {
        sequence: seq,
        written: None,
    };
    Ok(())
}
