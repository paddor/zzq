use super::*;

type Read = Vec<(u8, Vec<u8>)>;

fn read(bytes: &[u8], first: Option<u64>) -> Result<(Scanned, Read), ScanError> {
    let mut got = Vec::new();
    let scanned = scan(bytes, first, &mut |kind, body| {
        got.push((kind, body.to_vec()));
        Ok(())
    })?;
    Ok((scanned, got))
}

fn records(n: usize) -> Vec<(u8, Vec<u8>)> {
    (0..n).map(|i| (i as u8, vec![i as u8; i * 700])).collect()
}

fn built(records: &[(u8, Vec<u8>)], per_group: usize) -> Vec<u8> {
    let borrowed: Vec<(u8, &[u8])> = records.iter().map(|(k, b)| (*k, &b[..])).collect();
    build(&borrowed, per_group).unwrap()
}

fn corrupt_at(result: Result<(Scanned, Read), ScanError>) -> u64 {
    match result {
        Err(ScanError::Corrupt { offset, .. }) => offset,
        other => panic!("expected corruption, got {other:?}"),
    }
}

/// Offsets of the groups in a segment that [`build`] made.
fn group_offsets(bytes: &[u8]) -> Vec<usize> {
    let mut offsets = Vec::new();
    let mut at = PAGE;
    while let Some(g) = group_at(bytes, at, 1, None) {
        offsets.push(at);
        at += g.length;
    }
    offsets
}

#[test]
fn groups_read_back_in_order() {
    let written = records(20);
    let bytes = built(&written, 3);
    let (scanned, got) = read(&bytes, None).unwrap();
    assert_eq!(got, written);
    assert_eq!((scanned.id, scanned.last), (1, Some(7)));
    assert_eq!(scanned.end, bytes.len() as u64 - PAGE as u64);
    assert_eq!(scanned.capacity, bytes.len() as u64);
    assert_eq!(read(&bytes, Some(1)).unwrap().1, written);
}

#[test]
fn an_empty_segment_has_no_groups() {
    let bytes = built(&[], 1);
    let (scanned, got) = read(&bytes, Some(9)).unwrap();
    assert_eq!(got, Read::new());
    assert_eq!((scanned.end, scanned.last), (PAGE as u64, None));
}

#[test]
fn a_torn_last_group_ends_the_log() {
    let written = records(9);
    let mut bytes = built(&written, 3);
    let last = *group_offsets(&bytes).last().unwrap();
    bytes[last + GROUP_HEADER + 1] ^= 1;
    let (scanned, got) = read(&bytes, None).unwrap();
    assert_eq!(got, written[..6]);
    assert_eq!((scanned.end, scanned.last), (last as u64, Some(2)));
    // A header that never landed reads as zeros.
    bytes[last..last + GROUP_HEADER].fill(0);
    assert_eq!(read(&bytes, None).unwrap().1, written[..6]);
}

#[test]
fn padding_is_not_covered() {
    let written = records(3);
    let mut bytes = built(&written, 3);
    let end = bytes.len() - PAGE;
    bytes[end - 1] = 0xff;
    assert_eq!(read(&bytes, None).unwrap().1, written);
}

#[test]
fn damage_before_a_valid_group_fails() {
    let written = records(9);
    let mut bytes = built(&written, 3);
    let groups = group_offsets(&bytes);
    bytes[groups[1] + GROUP_HEADER] ^= 1;
    assert_eq!(corrupt_at(read(&bytes, None)), groups[2] as u64);
}

#[test]
fn group_numbers_must_follow_on() {
    let bytes = built(&records(3), 1);
    assert_eq!(corrupt_at(read(&bytes, Some(2))), PAGE as u64);
    // Swapped groups: the second read is out of order.
    let mut swapped = bytes.clone();
    let groups = group_offsets(&bytes);
    let len = groups[1] - groups[0];
    assert_eq!(groups[2] - groups[1], len);
    swapped[groups[0]..groups[1]].copy_from_slice(&bytes[groups[1]..groups[2]]);
    swapped[groups[1]..groups[2]].copy_from_slice(&bytes[groups[0]..groups[1]]);
    assert_eq!(corrupt_at(read(&swapped, None)), groups[1] as u64);
}

#[test]
fn groups_of_another_segment_are_free_space() {
    let mut bytes = built(&records(4), 2);
    let groups = group_offsets(&bytes);
    // The file reused as segment 2: its groups from the earlier use name 1.
    let header = segment_header(2, bytes.len() as u64);
    bytes[..PAGE].copy_from_slice(&header);
    let (scanned, got) = read(&bytes, None).unwrap();
    assert_eq!(got, Read::new());
    assert_eq!(scanned.end, groups[0] as u64);
}

#[test]
fn headers_are_checked() {
    let bytes = built(&records(1), 1);
    for (at, flip) in [(0, 1), (9, 1), (17, 1), (20, 1), (33, 0x10), (63, 1)] {
        let mut damaged = bytes.clone();
        damaged[at] ^= flip;
        assert_eq!(corrupt_at(read(&damaged, None)), 0, "byte {at}");
    }
    assert_eq!(corrupt_at(read(&bytes[..100], None)), 0);
    assert_eq!(corrupt_at(read(&bytes[..bytes.len() - PAGE], None)), 0);
    let small = segment_header(1, PAGE as u64);
    assert_eq!(
        read_segment_header(&small),
        Err("segment capacity out of range")
    );
}

#[test]
fn records_must_fill_their_group() {
    let mut buf = AlignedBuf::default();
    begin(&mut buf).unwrap();
    record(&mut buf, 1, &[b"ab", b"cd"]).unwrap();
    // One record more than the group holds.
    seal(&mut buf, 1, 1, 2).unwrap();
    let mut bytes = segment_header(1, (3 * PAGE) as u64);
    bytes.extend_from_slice(buf.as_slice());
    bytes.resize(3 * PAGE, 0);
    assert_eq!(corrupt_at(read(&bytes, None)), PAGE as u64);
    // One record fewer.
    let mut buf = AlignedBuf::default();
    begin(&mut buf).unwrap();
    record(&mut buf, 1, &[b"ab"]).unwrap();
    record(&mut buf, 2, &[]).unwrap();
    seal(&mut buf, 1, 1, 1).unwrap();
    bytes[PAGE..2 * PAGE].copy_from_slice(buf.as_slice());
    assert_eq!(corrupt_at(read(&bytes, None)), PAGE as u64);
}

#[test]
fn replay_errors_stop_the_scan() {
    let bytes = built(&records(3), 1);
    let mut seen = 0;
    let result = scan(&bytes, None, &mut |_, _| {
        seen += 1;
        if seen == 2 { Err("no".into()) } else { Ok(()) }
    });
    assert!(matches!(result, Err(ScanError::Replay(e)) if e.to_string() == "no"));
    assert_eq!(seen, 2);
}
