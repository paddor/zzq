//! Images: the state of every journal, each part cut at its own op. A
//! receiver replays the ops after the lowest cut and skips those of a
//! journal its part holds.

use bytes::{BufMut, Bytes, BytesMut};
use zzq_cluster::OpNumber;

use super::log::{Journal, records};

/// One journal's part: its state through `op`, as a group body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Part {
    pub(crate) op: OpNumber,
    pub(crate) body: Bytes,
}

/// An image of `parts`: each its op, body length and body.
pub(crate) fn encode(parts: &[Part]) -> Bytes {
    let len = parts.iter().map(|p| 12 + p.body.len()).sum();
    let mut out = BytesMut::with_capacity(len);
    for p in parts {
        out.put_u64(p.op.0);
        out.put_u32(u32::try_from(p.body.len()).unwrap_or(u32::MAX));
        out.put_slice(&p.body);
    }
    out.freeze()
}

/// The parts of an image, each body a slice of `bytes`.
///
/// # Errors
///
/// When a part is cut short or does not name a journal.
pub(crate) fn decode(bytes: &Bytes) -> Result<Vec<Part>, &'static str> {
    let mut parts = Vec::new();
    let mut rest = &bytes[..];
    while !rest.is_empty() {
        let (op, r) = rest
            .split_first_chunk::<8>()
            .ok_or("image part cut short")?;
        let (len, r) = r.split_first_chunk::<4>().ok_or("image part cut short")?;
        let len = u32::from_be_bytes(*len) as usize;
        let body = r.get(..len).ok_or("image part cut short")?;
        if records(body).is_none() {
            return Err("image part of no journal");
        }
        parts.push(Part {
            op: OpNumber(u64::from_be_bytes(*op)),
            body: bytes.slice_ref(body),
        });
        rest = &r[len..];
    }
    Ok(parts)
}

/// A leader's image until the groups each journal sealed for it are
/// proposed: only then are the ops its parts hold known.
#[derive(Debug, Default)]
pub(crate) struct Cut {
    parts: Vec<Pending>,
}

#[derive(Debug)]
struct Pending {
    journal: Journal,
    /// The number of the last record the part holds.
    last: u64,
    body: Bytes,
    /// The op before the journal's first one after the part.
    op: Option<OpNumber>,
}

impl Cut {
    /// Adds `journal`'s part, which holds its records through `last`.
    pub(crate) fn add(&mut self, journal: Journal, last: u64, body: Bytes) {
        self.parts.push(Pending {
            journal,
            last,
            body,
            op: None,
        });
    }

    /// Op `op` was proposed with `journal`'s records through `last`.
    pub(crate) fn proposed(&mut self, journal: Journal, last: u64, op: OpNumber) {
        for p in &mut self.parts {
            if p.journal == journal && p.op.is_none() && last > p.last {
                p.op = Some(OpNumber(op.0.saturating_sub(1)));
            }
        }
    }

    /// The image and its lowest and highest cut, once every journal's
    /// records through its part are proposed: `taken` gives the last one
    /// proposed. A part no later op of its journal follows holds through
    /// `tip`, the last op proposed.
    pub(crate) fn finish(
        &self,
        taken: impl Fn(Journal) -> u64,
        tip: OpNumber,
    ) -> Option<(OpNumber, OpNumber, Bytes)> {
        if self.parts.iter().any(|p| taken(p.journal) < p.last) {
            return None;
        }
        let parts: Vec<Part> = self
            .parts
            .iter()
            .map(|p| Part {
                op: p.op.unwrap_or(tip),
                body: p.body.clone(),
            })
            .collect();
        let predecessor = parts.iter().map(|p| p.op).min()?;
        let position = parts.iter().map(|p| p.op).max()?;
        Some((predecessor, position, encode(&parts)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::GroupWriter;
    use crate::disk::Append;

    fn body(journal: Journal, kinds: &[u8]) -> Bytes {
        let mut w = GroupWriter::new(journal);
        for &k in kinds {
            w.append(k, &[b"x"]).unwrap();
        }
        w.finish()
    }

    #[test]
    fn an_image_decodes_to_the_parts_it_was_made_of() {
        let parts = vec![
            Part {
                op: OpNumber(7),
                body: body(Journal::Lane(1), &[1, 2]),
            },
            Part {
                op: OpNumber(9),
                body: body(Journal::Retained(3), &[]),
            },
        ];
        assert_eq!(decode(&encode(&parts)).unwrap(), parts);
        assert_eq!(decode(&Bytes::new()).unwrap(), []);
    }

    #[test]
    fn a_short_or_foreign_part_is_refused() {
        let bytes = encode(&[Part {
            op: OpNumber(1),
            body: body(Journal::Lane(0), &[1]),
        }]);
        for n in [1, 9, bytes.len() - 1] {
            assert!(decode(&bytes.slice(..n)).is_err(), "cut at {n}");
        }
        let foreign = encode(&[Part {
            op: OpNumber(1),
            body: Bytes::from_static(&[9, 9, 9]),
        }]);
        assert!(decode(&foreign).is_err());
    }

    #[test]
    fn a_cut_waits_for_its_groups_then_holds_each_journal_through_its_next_op() {
        let mut cut = Cut::default();
        cut.add(Journal::Lane(0), 5, body(Journal::Lane(0), &[]));
        cut.add(Journal::Lane(1), 2, body(Journal::Lane(1), &[]));
        let taken = |j| if j == Journal::Lane(0) { 4 } else { 2 };
        assert!(cut.finish(taken, OpNumber(10)).is_none(), "lane 0 waits");
        // Lane 0's sealed group, then its next one, which the part lacks.
        cut.proposed(Journal::Lane(0), 5, OpNumber(11));
        cut.proposed(Journal::Lane(0), 8, OpNumber(13));
        let (predecessor, position, bytes) = cut.finish(|_| 8, OpNumber(14)).unwrap();
        assert_eq!((predecessor, position), (OpNumber(12), OpNumber(14)));
        let ops: Vec<_> = decode(&bytes).unwrap().iter().map(|p| p.op).collect();
        assert_eq!(ops, [OpNumber(12), OpNumber(14)]);
    }
}
