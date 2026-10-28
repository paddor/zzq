use std::os::unix::fs::MetadataExt;

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
        segment::create(dir.path(), id, small().segment_bytes, false).unwrap();
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
        segment::create(dir.path(), id, small().segment_bytes, false).unwrap();
    }
    let j = open(&disk, dir.path()).unwrap();
    let active = j.tail.active.as_ref().map(|a| a.id);
    assert_eq!((active, j.tail.offset), (Some(1), PAGE as u64));
    assert_eq!(j.tail.next.as_ref().map(|s| s.id), Some(2));
    assert_eq!(j.tail.group, 1);
}

#[test]
fn a_roll_skips_the_empty_segment_a_write_goes_to() {
    let dir = TempDir::new("roll-in-flight");
    let disk = Disk::new(small()).unwrap();
    let Some(j) = open(&disk, dir.path()) else {
        return;
    };
    drop(j);
    for id in 1..=2 {
        segment::create(dir.path(), id, small().segment_bytes, false).unwrap();
    }
    let mut j = open(&disk, dir.path()).unwrap();
    j.append(1, &[b"before"]).unwrap();
    j.flush().unwrap();
    // Segment 1 is empty until that write finishes, and holds it then.
    assert_eq!(j.roll().unwrap(), Some(2));
    j.append(1, &[b"after"]).unwrap();
    j.sync().unwrap();
    drop(j);
    segment::trim(dir.path(), 2).unwrap();
    let mut got = Vec::new();
    disk.open(dir.path(), |r| {
        got.push(r.body.to_vec());
        Ok(())
    })
    .unwrap();
    assert_eq!(got, [b"after".to_vec()]);
}

#[test]
fn a_roll_waits_while_the_records_before_it_do_not_fit() {
    let dir = TempDir::new("roll-full");
    let disk = Disk::new(small()).unwrap();
    let Some(mut j) = open(&disk, dir.path()) else {
        return;
    };
    j.append(1, &[b"x"]).unwrap();
    j.sync().unwrap();
    assert_eq!(j.roll().unwrap(), None);
    j.wait().unwrap();
    // One page left in segment 1; the record waiting takes two.
    j.tail.offset = small().segment_bytes - PAGE as u64;
    j.append(1, &[&[0; 5000]]).unwrap();
    assert_eq!(j.roll().unwrap(), None);
    j.sync().unwrap();
    assert_eq!(j.tail.active.as_ref().map(|a| a.id), Some(2));
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
    segment::create(dir.path(), 1, small().segment_bytes, true).unwrap();
    let mut j = open(&disk, dir.path()).unwrap();
    // Writes skip segment 1, which a journal never does.
    j.tail.next = Some(segment::create(dir.path(), 2, small().segment_bytes, false).unwrap());
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

#[test]
fn a_new_segment_takes_the_newest_spare_that_fits() {
    let dir = TempDir::new("spares");
    let disk = Disk::new(small()).unwrap();
    let Some(j) = open(&disk, dir.path()) else {
        return;
    };
    drop(j);
    let d = dir.path();
    let cap = small().segment_bytes;
    let ino = |id: u64| fs::metadata(segment::path(d, id)).unwrap().ino();
    segment::create(d, 1, cap, true).unwrap();
    segment::create(d, 2, cap, false).unwrap();
    segment::create(d, 3, 2 * cap, false).unwrap();
    let two = ino(2);
    segment::trim(d, 4).unwrap();
    assert_eq!(segment::list(d).unwrap(), [0; 0]);
    assert_eq!(segment::spares(d).unwrap(), [1, 2, 3]);
    // Spare 3 is too large; 2 is the newest that fits.
    segment::create(d, 4, cap, false).unwrap();
    assert_eq!(ino(4), two);
    assert_eq!(segment::spares(d).unwrap(), [1]);
    // What a trim takes replaces the spares left.
    segment::trim(d, 5).unwrap();
    assert_eq!(segment::spares(d).unwrap(), [4]);
    // A trim that takes none keeps them.
    segment::trim(d, 5).unwrap();
    assert_eq!(segment::spares(d).unwrap(), [4]);
    // A spare not older than the new segment could hold groups that name
    // it.
    let spare = |id: u64| d.join(format!("{id:016x}.spare"));
    fs::rename(spare(4), spare(9)).unwrap();
    segment::create(d, 5, cap, false).unwrap();
    assert_eq!(segment::spares(d).unwrap(), [0; 0]);
    assert_eq!(segment::list(d).unwrap(), [5]);
}
