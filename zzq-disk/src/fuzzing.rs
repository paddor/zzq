//! Entry points for the fuzz targets in `fuzz/afl`. Not part of the API.

use crate::format::{self, PAGE};

/// Reads `data` as a segment file. Then writes up to 16 records cut from
/// `data` into a segment, each group padded to a page, reads them back, damages one byte, and reads again: the
/// records read are the ones written, or the first groups of them, or the
/// scan fails.
///
/// # Panics
///
/// When it finds a bug.
pub fn journal_segment(data: &[u8]) {
    let _ = format::scan(data, None, &mut |_, _| Ok(()));
    let Some((&seed, rest)) = data.split_first() else {
        return;
    };
    let written: Vec<(u8, &[u8])> = rest
        .chunks(1 + usize::from(seed) % 17)
        .map(|c| (c[0], &c[1..]))
        .take(16)
        .collect();
    let Ok(mut bytes) = format::build(&written, 1 + usize::from(seed) % 5) else {
        return;
    };
    assert_eq!(read(&bytes).as_deref(), Some(&written[..]), "written");
    let at = PAGE + usize::from(seed) * 31 % (bytes.len() - PAGE);
    bytes[at] ^= 1 << (seed % 8);
    if let Some(got) = read(&bytes) {
        assert!(written.starts_with(&got), "damage changed records");
    }
}

fn read(bytes: &[u8]) -> Option<Vec<(u8, &[u8])>> {
    let mut got = Vec::new();
    format::scan(bytes, None, &mut |kind, body| {
        got.push((kind, body));
        Ok(())
    })
    .ok()?;
    Some(got)
}

/// Seeds for [`journal_segment`]: a segment of a few groups, and bytes the
/// target cuts into records.
#[must_use]
pub fn journal_segment_seeds() -> Vec<Vec<u8>> {
    let records: [(u8, &[u8]); 4] = [(1, b"a/b"), (2, b""), (3, &[0; 300]), (1, b"c")];
    let mut seeds: Vec<Vec<u8>> = format::build(&records, 2).into_iter().collect();
    seeds.push(b"\x05record one, record two, and a third".to_vec());
    seeds
}
