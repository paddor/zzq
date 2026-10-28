use super::*;
use crate::testing::TempDir;

const fn small() -> Config {
    Config {
        segment_bytes: 64 << 10,
        group_bytes: 8 << 10,
        depth: 2,
    }
}

/// Opens `dir`, or `None` where it takes no `O_DIRECT`.
fn open(disk: &Disk, dir: &Path) -> Option<Journal> {
    match disk.open(dir, |_| Ok(())) {
        Ok(j) => Some(j),
        Err(Error::Io(e)) if e.kind() == io::ErrorKind::InvalidInput => {
            eprintln!("skipped: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

#[test]
fn one_empty_segment_stays_as_the_next_and_others_go() {
    let dir = TempDir::new("empty");
    let disk = Disk::new(small()).unwrap();
    let Some(mut j) = open(&disk, dir.path()) else {
        return;
    };
    j.append(1, &[b"x"]).unwrap();
    j.sync().unwrap();
    drop(j);
    // A crash after the next segments were made, before writes reached them.
    for id in 2..=4 {
        segment::create(dir.path(), id, small().segment_bytes).unwrap();
    }
    let mut j = open(&disk, dir.path()).unwrap();
    assert_eq!(segment::list(dir.path()).unwrap(), [1, 2]);
    assert_eq!(j.tail.next.as_ref().map(|s| s.id), Some(2));
    for _ in 0..20 {
        j.append(1, &[&[0; 4000]]).unwrap();
    }
    j.sync().unwrap();
    assert!(
        j.tail.active.as_ref().unwrap().id >= 2,
        "rolled into the kept segment"
    );
}

#[test]
fn a_journal_never_written_holds_no_segment() {
    let dir = TempDir::new("fresh");
    let disk = Disk::new(small()).unwrap();
    let Some(mut j) = open(&disk, dir.path()) else {
        return;
    };
    assert_eq!(j.sync().unwrap(), 0);
    drop(j);
    assert_eq!(segment::list(dir.path()).unwrap(), [0; 0]);
    let mut j = open(&disk, dir.path()).unwrap();
    j.append(1, &[b"x"]).unwrap();
    assert_eq!(j.sync().unwrap(), 1);
    assert_eq!(segment::list(dir.path()).unwrap(), [1]);
}

#[test]
fn an_empty_journal_starts_in_its_first_empty_segment() {
    let dir = TempDir::new("empty-first");
    let disk = Disk::new(small()).unwrap();
    let Some(j) = open(&disk, dir.path()) else {
        return;
    };
    drop(j);
    for id in 1..=2 {
        segment::create(dir.path(), id, small().segment_bytes).unwrap();
    }
    let j = open(&disk, dir.path()).unwrap();
    let active = j.tail.active.as_ref().map(|a| a.id);
    assert_eq!((active, j.tail.offset), (Some(1), PAGE as u64));
    assert_eq!(j.tail.next.as_ref().map(|s| s.id), Some(2));
    assert_eq!(j.tail.group, 1);
}

#[test]
fn a_failed_write_reports_its_records_and_writes_go_on() {
    let dir = TempDir::new("failed");
    let disk = Disk::new(small()).unwrap();
    let Some(mut j) = open(&disk, dir.path()) else {
        return;
    };
    j.append(1, &[b"one"]).unwrap();
    assert_eq!(j.sync().unwrap(), 1);
    // An unaligned offset is refused before the kernel sees the write.
    let offset = j.tail.offset;
    j.tail.offset += 1;
    j.append(1, &[b"two"]).unwrap();
    j.append(1, &[b"three"]).unwrap();
    j.flush().unwrap();
    match j.wait() {
        Err(Error::Write { first, last, .. }) => assert_eq!((first, last), (2, 3)),
        other => panic!("{other:?}"),
    }
    assert_eq!((j.durable(), j.backlog()), (3, 0));
    // The next group goes where the failed one would have, with its number.
    j.tail.offset = offset;
    j.append(1, &[b"four"]).unwrap();
    assert_eq!(j.sync().unwrap(), 4);
    drop(j);
    let mut got = Vec::new();
    disk.open(dir.path(), |r| {
        got.push(r.body.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(got, [b"one".to_vec(), b"four".to_vec()]);
}

#[test]
fn a_written_segment_after_an_empty_one_fails_the_open() {
    let dir = TempDir::new("gap");
    let disk = Disk::new(small()).unwrap();
    let Some(j) = open(&disk, dir.path()) else {
        return;
    };
    drop(j);
    segment::create(dir.path(), 1, small().segment_bytes).unwrap();
    let mut j = open(&disk, dir.path()).unwrap();
    // Writes skip segment 1, which a journal never does.
    j.tail.next = Some(segment::create(dir.path(), 2, small().segment_bytes).unwrap());
    j.tail.offset = j.tail.active.as_ref().unwrap().capacity;
    j.append(1, &[b"x"]).unwrap();
    j.sync().unwrap();
    assert_eq!(j.tail.active.as_ref().unwrap().id, 2);
    drop(j);
    match disk.open(dir.path(), |_| Ok(())) {
        Err(Error::Corrupt { reason, .. }) => {
            assert_eq!(reason, "a written segment follows an empty one");
        }
        other => panic!("{other:?}"),
    }
}
