use std::os::unix::fs::MetadataExt;
use std::path::PathBuf;

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
        segment::create(dir.path(), id, small().segment_bytes, false, false).unwrap();
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
        segment::create(dir.path(), id, small().segment_bytes, false, false).unwrap();
    }
    let mut j = open(&disk, dir.path()).unwrap();
    let active = j.tail.active.as_ref().map(|a| a.id);
    assert_eq!((active, j.tail.offset), (Some(1), PAGE as u64));
    assert_eq!(j.tail.next.as_ref().map(|s| s.id), Some(2));
    assert_eq!(j.tail.group, 1);
    // Nothing comes before a roll, so it starts there.
    assert_eq!(j.roll().unwrap(), Some(1));
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
        segment::create(dir.path(), id, small().segment_bytes, false, false).unwrap();
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
    segment::create(dir.path(), 1, small().segment_bytes, true, false).unwrap();
    let mut j = open(&disk, dir.path()).unwrap();
    // Writes skip segment 1, which a journal never does.
    j.tail.next =
        Some(segment::create(dir.path(), 2, small().segment_bytes, false, false).unwrap());
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
    let spare = |id: u64| d.join(format!("{id:016x}.spare"));
    let ino = |path: PathBuf| fs::metadata(path).unwrap().ino();
    let spares = || segment::spares(d).unwrap();
    // The first segment comes with a spare kept back.
    segment::create(d, 1, cap, true, false).unwrap();
    assert_eq!(spares(), [0]);
    let reserve = ino(spare(0));
    for id in 2..=3 {
        segment::create(d, id, cap, false, false).unwrap();
    }
    let two = ino(segment::path(d, 2));
    // The spare kept back stays.
    segment::trim(d, 3).unwrap();
    assert_eq!(spares(), [0, 1, 2]);
    // The newest goes first, down to the spare kept back.
    segment::create(d, 4, cap, false, false).unwrap();
    assert_eq!(ino(segment::path(d, 4)), two);
    for id in 5..=6 {
        segment::create(d, id, cap, false, false).unwrap();
    }
    assert_eq!(spares(), [0]);
    // The segment after a roll takes it.
    segment::create(d, 7, cap, false, true).unwrap();
    assert_eq!(ino(segment::path(d, 7)), reserve);
    assert_eq!(spares(), [0; 0]);
    // What a trim takes replaces the spares left but the newest.
    segment::trim(d, 7).unwrap();
    assert_eq!(spares(), [3, 4, 5, 6]);
    // A trim that takes none keeps them.
    segment::trim(d, 7).unwrap();
    assert_eq!(spares(), [3, 4, 5, 6]);
    // A spare not older than the new segment could hold groups that name
    // it.
    fs::rename(spare(6), spare(9)).unwrap();
    segment::create(d, 8, cap, false, false).unwrap();
    assert_eq!(spares(), [3, 4]);
    // Spares of another size do not fit.
    segment::create(d, 9, 2 * cap, false, false).unwrap();
    assert_eq!(spares(), [0]);
    assert_eq!(fs::metadata(spare(0)).unwrap().len(), 2 * cap);
    assert_eq!(segment::list(d).unwrap(), [7, 8, 9]);
}

#[test]
fn a_roll_on_a_full_device_starts_in_the_spare_kept_back() {
    let dir = TempDir::new("full-roll");
    let disk = Disk::new(small()).unwrap();
    let Some(mut j) = open(&disk, dir.path()) else {
        return;
    };
    j.append(1, &[b"x"]).unwrap();
    j.sync().unwrap();
    let ino = |path: PathBuf| fs::metadata(path).unwrap().ino();
    let reserve = ino(dir.path().join(format!("{:016x}.spare", 0)));
    // As after a segment that could not be made: the roll does not wait
    // for one.
    (j.tail.failed, j.tail.full) = (true, true);
    assert_eq!(j.roll().unwrap(), Some(2));
    j.append(1, &[b"y"]).unwrap();
    j.sync().unwrap();
    assert_eq!(ino(segment::path(dir.path(), 2)), reserve);
    // The spare kept back shows no room on the device.
    assert!(j.full());
    // A segment made from free space does.
    j.create_next().unwrap();
    j.wait().unwrap();
    assert!(!j.full());
}
