//! Journals on disk: write, reopen, roll, damage, kill.

#![cfg(target_os = "linux")]
#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::{Duration, Instant};

use zzq_disk::{Config, Disk, Error, Journal};

/// A fresh directory in the target dir, which takes `O_DIRECT` where the
/// temp dir may be tmpfs. Removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> TempDir {
        static N: AtomicUsize = AtomicUsize::new(0);
        // Relaxed: only uniqueness matters.
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("zzq-disk-{}-{name}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        TempDir(path)
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn small() -> Config {
    let mut c = Config::default();
    c.segment_bytes = 64 << 10;
    c.group_bytes = 8 << 10;
    c.depth = 4;
    c
}

/// Record `n`'s body: its number, then up to 3 KiB that follow from it.
fn body(n: u64) -> Vec<u8> {
    let mut b = n.to_be_bytes().to_vec();
    b.extend((0..n * 37 % 3000).map(|i| (i ^ n) as u8));
    b
}

/// Opens `dir`, collecting the bodies of the records in it.
fn open(disk: &Disk, dir: &Path) -> (Journal, Vec<(u8, Vec<u8>)>) {
    let mut got = Vec::new();
    let j = disk
        .open(dir, |r| {
            got.push((r.kind, r.body.to_vec()));
            Ok(())
        })
        .unwrap();
    (j, got)
}

fn append(j: &mut Journal, from: u64, to: u64) {
    for n in from..=to {
        let before = j.appended();
        assert_eq!(j.append((n % 7) as u8, &[&body(n)]).unwrap(), before + 1);
    }
}

fn expected(to: u64) -> Vec<(u8, Vec<u8>)> {
    (1..=to).map(|n| ((n % 7) as u8, body(n))).collect()
}

fn segments(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter(|e| {
            e.as_ref()
                .unwrap()
                .path()
                .extension()
                .is_some_and(|x| x == "seg")
        })
        .count()
}

#[test]
fn records_read_back_after_reopen() {
    let dir = TempDir::new("reopen");
    let disk = Disk::new(small()).unwrap();
    let (mut j, got) = open(&disk, &dir.0);
    assert_eq!(got, []);
    append(&mut j, 1, 300);
    assert_eq!(j.sync().unwrap(), 300);
    assert_eq!(j.backlog(), 0);
    drop(j);
    let (mut j, got) = open(&disk, &dir.0);
    assert_eq!(got, expected(300));
    // Numbers start over; records go on after the old ones.
    assert_eq!(j.append(1, &[b"a", b"b"]).unwrap(), 1);
    j.sync().unwrap();
    drop(j);
    let (_, got) = open(&disk, &dir.0);
    assert_eq!(got.len(), 301);
    assert_eq!(got[300], (1, b"ab".to_vec()));
    assert!(segments(&dir.0) > 3, "rolled into new segments");
}

#[test]
fn records_wait_behind_the_write_in_flight() {
    let dir = TempDir::new("behind");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    j.append(1, &[b"first"]).unwrap();
    j.flush().unwrap();
    for _ in 0..50 {
        j.append(1, &[b"later"]).unwrap();
    }
    // In flight already: the later ones wait.
    j.flush().unwrap();
    assert_eq!(j.wait().unwrap(), 1);
    // That completion sent all the rest as one write.
    assert_eq!(j.wait().unwrap(), 51);
    assert_eq!(j.wait().unwrap(), 51);
}

#[test]
fn records_no_flush_asked_for_wait_for_one() {
    let dir = TempDir::new("unflushed");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    j.append(1, &[b"first"]).unwrap();
    j.flush().unwrap();
    j.append(1, &[b"lazy"]).unwrap();
    // That completion sent nothing more.
    assert_eq!(j.wait().unwrap(), 1);
    assert_eq!(j.wait().unwrap(), 1);
    assert!(j.backlog() > 0);
    j.flush().unwrap();
    j.append(1, &[b"wanted"]).unwrap();
    j.flush().unwrap();
    j.append(1, &[b"along"]).unwrap();
    assert_eq!(j.wait().unwrap(), 2);
    // The flushed record took the later one along in its write.
    assert_eq!(j.wait().unwrap(), 4);
    assert_eq!(j.wait().unwrap(), 4);
}

#[test]
fn completions_wake_an_async_owner() {
    let dir = TempDir::new("async");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    for round in 1..=20 {
        j.append(2, &[&body(round)]).unwrap();
        j.flush().unwrap();
        let durable = block_on(&mut j);
        assert_eq!(durable, round);
    }
}

/// Polls `poll_complete` until ready, parking between polls.
fn block_on(j: &mut Journal) -> u64 {
    struct Unpark(std::thread::Thread);
    impl Wake for Unpark {
        fn wake(self: Arc<Self>) {
            self.0.unpark();
        }
    }
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut cx = Context::from_waker(&waker);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(r) = j.poll_complete(&mut cx) {
            return r.unwrap();
        }
        assert!(Instant::now() < deadline, "no wake");
        std::thread::park_timeout(Duration::from_secs(1));
    }
}

#[test]
fn records_too_large_for_a_segment_are_refused() {
    let dir = TempDir::new("large");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    let big = vec![0; 60 << 10];
    assert!(matches!(j.append(1, &[&big]), Err(Error::TooLarge { len }) if len == big.len()));
    // The largest that fits: one group that fills a segment.
    let fits = vec![1; (60 << 10) - 64 - 5];
    j.append(1, &[&fits]).unwrap();
    j.append(1, &[&fits]).unwrap();
    assert_eq!(j.sync().unwrap(), 2);
    drop(j);
    let (_, got) = open(&disk, &dir.0);
    assert_eq!(got, [(1, fits.clone()), (1, fits)]);
}

#[test]
fn a_directory_takes_one_journal() {
    let dir = TempDir::new("lock");
    let disk = Disk::new(small()).unwrap();
    let (j, _) = open(&disk, &dir.0);
    let Err(Error::Io(e)) = disk.open(&dir.0, |_| Ok(())) else {
        panic!("opened twice");
    };
    assert_eq!(e.kind(), std::io::ErrorKind::WouldBlock);
    drop(j);
    open(&disk, &dir.0);
}

#[test]
fn damage_inside_the_log_fails_the_open() {
    let dir = TempDir::new("damage");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    // Several groups: damage to the last one reads as a torn write.
    append(&mut j, 1, 40);
    j.sync().unwrap();
    drop(j);
    let first = dir.0.join(format!("{:016x}.seg", 1));
    let mut bytes = std::fs::read(&first).unwrap();
    bytes[4096 + 100] ^= 1;
    std::fs::write(&first, &bytes).unwrap();
    match disk.open(&dir.0, |_| Ok(())) {
        Err(Error::Corrupt { path, offset, .. }) => {
            assert_eq!((path, offset > 4096), (first, true));
        }
        other => panic!("opened a damaged log: {other:?}"),
    }
}

#[test]
fn a_torn_last_write_ends_the_log() {
    let dir = TempDir::new("torn");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    append(&mut j, 1, 3);
    j.sync().unwrap();
    let mark = dir.0.join("DURABLE");
    let before = std::fs::read(&mark).unwrap();
    // One write of its own, at the end.
    j.append(9, &[&[7; 5000]]).unwrap();
    j.sync().unwrap();
    drop(j);
    // A crash during that write: the mark names the group before it.
    std::fs::write(&mark, &before).unwrap();
    tear_last_group(&dir.0);
    let (mut j, got) = open(&disk, &dir.0);
    assert_eq!(got, expected(3));
    // Writes go on where the torn one was.
    j.append(1, &[b"after"]).unwrap();
    j.sync().unwrap();
    drop(j);
    let (_, got) = open(&disk, &dir.0);
    assert_eq!(got.last().unwrap(), &(1, b"after".to_vec()));
    assert_eq!(got.len(), 4);
}

/// Flips a bit in the body of the last group of segment 1.
fn tear_last_group(dir: &Path) {
    let first = dir.join(format!("{:016x}.seg", 1));
    let mut bytes = std::fs::read(&first).unwrap();
    let last = (4096..bytes.len())
        .step_by(4096)
        .rfind(|&at| bytes[at..].starts_with(b"ZZQGROUP"))
        .unwrap();
    bytes[last + 64 + 4000] ^= 1;
    std::fs::write(&first, &bytes).unwrap();
}

#[test]
fn damage_to_a_group_the_mark_names_fails_the_open() {
    let dir = TempDir::new("marked");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    j.append(9, &[&[7; 5000]]).unwrap();
    j.sync().unwrap();
    drop(j);
    tear_last_group(&dir.0);
    match disk.open(&dir.0, |_| Ok(())) {
        Err(Error::Corrupt { reason, .. }) => {
            assert_eq!(reason, "the journal ends before groups it had on disk");
        }
        other => panic!("{other:?}"),
    }
    std::fs::write(dir.0.join("DURABLE"), b"garbage").unwrap();
    match disk.open(&dir.0, |_| Ok(())) {
        Err(Error::Corrupt { reason, .. }) => assert_eq!(reason, "the durable mark is damaged"),
        other => panic!("{other:?}"),
    }
}

#[test]
fn a_rolled_journal_trims_the_segments_before() {
    let dir = TempDir::new("trim");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    assert_eq!(
        j.roll().unwrap(),
        Some(1),
        "nothing written: the first segment"
    );
    append(&mut j, 1, 3);
    j.sync().unwrap();
    // The next segment is asked for, and there once a wait got it.
    assert_eq!(j.roll().unwrap(), None);
    j.wait().unwrap();
    let next = j.roll().unwrap().unwrap();
    assert_eq!(next, 2);
    assert_eq!(
        j.roll().unwrap(),
        Some(2),
        "the roll waits for the next group"
    );
    append(&mut j, 4, 6);
    j.sync().unwrap();
    assert_eq!(j.roll().unwrap(), None, "in segment 2, which holds groups");
    j.trim(next).unwrap();
    assert!(j.written_bytes() > 0);
    // The file thread trims in turn; a sync that comes after waits for it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while dir.0.join(format!("{:016x}.seg", 1)).exists() {
        assert!(std::time::Instant::now() < deadline, "segment 1 stays");
        std::thread::yield_now();
    }
    drop(j);
    let (_, got) = open(&disk, &dir.0);
    assert_eq!(got, expected(6)[3..]);
}

#[test]
fn trimmed_segments_become_the_next_ones() {
    let dir = TempDir::new("spare");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    let file = |name: String| dir.0.join(name);
    let ino = |name: String| std::fs::metadata(file(name)).unwrap().ino();
    append(&mut j, 1, 3);
    j.sync().unwrap();
    let first = ino(format!("{:016x}.seg", 1));
    assert_eq!(j.roll().unwrap(), None);
    j.wait().unwrap();
    let next = j.roll().unwrap().unwrap();
    append(&mut j, 4, 6);
    j.sync().unwrap();
    j.trim(next).unwrap();
    let deadline = Instant::now() + Duration::from_secs(10);
    while !file(format!("{:016x}.spare", 1)).exists() {
        assert!(Instant::now() < deadline, "segment 1 is no spare");
        std::thread::yield_now();
    }
    assert!(!file(format!("{:016x}.seg", 1)).exists());
    // Segment 3 is the old segment 1. Its groups name segment 1, so they
    // do not read back as segment 3's.
    append(&mut j, 7, 200);
    j.sync().unwrap();
    assert_eq!(ino(format!("{:016x}.seg", 3)), first);
    assert!(!file(format!("{:016x}.spare", 1)).exists());
    drop(j);
    let (_, got) = open(&disk, &dir.0);
    assert_eq!(got, expected(200)[3..]);
}

#[test]
fn written_bytes_count_whole_pages() {
    let dir = TempDir::new("written");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    j.append(1, &[b"x"]).unwrap();
    j.append(1, &[b"y"]).unwrap();
    j.sync().unwrap();
    assert_eq!(j.written_bytes(), 4096);
    j.append(1, &[&[0; 5000]]).unwrap();
    j.sync().unwrap();
    assert_eq!(j.written_bytes(), 3 * 4096);
}

#[test]
fn a_missing_segment_fails_the_open() {
    let dir = TempDir::new("missing");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    append(&mut j, 1, 200);
    j.sync().unwrap();
    drop(j);
    assert!(segments(&dir.0) >= 3);
    std::fs::remove_file(dir.0.join(format!("{:016x}.seg", 2))).unwrap();
    assert!(matches!(
        disk.open(&dir.0, |_| Ok(())),
        Err(Error::Corrupt { .. })
    ));
}

#[test]
fn replay_errors_fail_the_open() {
    let dir = TempDir::new("replay");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    append(&mut j, 1, 5);
    j.sync().unwrap();
    drop(j);
    let r = disk.open(&dir.0, |r| {
        if r.kind == 3 {
            Err("kind 3".into())
        } else {
            Ok(())
        }
    });
    let Err(Error::Replay(e)) = r else {
        panic!("replay error lost");
    };
    assert_eq!(e.to_string(), "kind 3");
}

#[test]
fn dropping_the_disk_closes_its_journals() {
    let dir = TempDir::new("closed");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    j.append(1, &[b"x"]).unwrap();
    j.sync().unwrap();
    drop(disk);
    let mut cx = Context::from_waker(Waker::noop());
    assert!(matches!(
        j.poll_complete(&mut cx),
        Poll::Ready(Err(Error::Closed))
    ));
    assert!(matches!(j.append(1, &[b"y"]), Err(Error::Closed)));
    assert!(matches!(j.flush(), Err(Error::Closed)));
    assert!(matches!(j.wait(), Err(Error::Closed)));
    assert_eq!(j.durable(), 1);
}

#[test]
fn segment_files_must_match_their_names_and_size() {
    let dir = TempDir::new("names");
    let disk = Disk::new(small()).unwrap();
    let (mut j, _) = open(&disk, &dir.0);
    append(&mut j, 1, 5);
    j.sync().unwrap();
    drop(j);
    // Files that are not segments stay.
    std::fs::write(dir.0.join("notes.txt"), b"x").unwrap();
    std::fs::write(dir.0.join("0001.seg"), b"x").unwrap();
    let (j, got) = open(&disk, &dir.0);
    assert_eq!(got, expected(5));
    drop(j);
    let seg = |id: u64| dir.0.join(format!("{id:016x}.seg"));
    std::fs::rename(seg(1), seg(2)).unwrap();
    assert!(matches!(
        disk.open(&dir.0, |_| Ok(())),
        Err(Error::Corrupt {
            reason: "segment header names another segment",
            ..
        })
    ));
    std::fs::rename(seg(2), seg(1)).unwrap();
    std::fs::File::options()
        .write(true)
        .open(seg(1))
        .unwrap()
        .set_len(2 << 30)
        .unwrap();
    match disk.open(&dir.0, |_| Ok(())) {
        Err(Error::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::InvalidData),
        other => panic!("{other:?}"),
    }
}

#[test]
fn configs_out_of_range_are_refused() {
    for (field, set) in [
        (
            "segment_bytes",
            (|c: &mut Config| c.segment_bytes = 4096) as fn(&mut Config),
        ),
        ("segment_bytes", |c| c.segment_bytes = (64 << 10) + 1),
        ("segment_bytes", |c| c.segment_bytes = 2 << 30),
        ("group_bytes", |c| c.group_bytes = 0),
        ("group_bytes", |c| c.group_bytes = 64 << 10),
        ("depth", |c| c.depth = 0),
        ("depth", |c| c.depth = 1025),
    ] {
        let mut c = small();
        set(&mut c);
        assert!(
            matches!(Disk::new(c), Err(Error::Config(f)) if f == field),
            "{field}"
        );
    }
}

/// Segments for a few rolls in 1000 records, without a sync per segment
/// dominating on a slow disk.
fn kill_config() -> Config {
    let mut c = small();
    c.segment_bytes = 256 << 10;
    c
}

/// The child of [`acked_records_survive_sigkill`]: writes until killed,
/// printing each durable number.
fn write_until_killed(dir: &Path) -> ! {
    let disk = Disk::new(kill_config()).unwrap();
    let (mut j, _) = open(&disk, dir);
    let mut out = std::io::stdout().lock();
    loop {
        let from = j.appended() + 1;
        append(&mut j, from, from + 49);
        j.flush().unwrap();
        while j.durable() < j.appended() {
            let durable = j.wait().unwrap();
            writeln!(out, "durable {durable}").unwrap();
            out.flush().unwrap();
        }
    }
}

#[test]
fn acked_records_survive_sigkill() {
    const CHILD: &str = "ZZQ_DISK_KILL_CHILD";
    if let Some(dir) = std::env::var_os(CHILD) {
        write_until_killed(Path::new(&dir));
    }
    let dir = TempDir::new("sigkill");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "acked_records_survive_sigkill", "--nocapture"])
        .env(CHILD, &dir.0)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut durable = 0;
    for line in BufReader::new(child.stdout.take().unwrap()).lines() {
        if let Some(n) = line.unwrap().strip_prefix("durable ") {
            durable = n.parse().unwrap();
            if durable >= 1000 {
                break;
            }
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(durable >= 1000, "the child stopped early");
    let disk = Disk::new(kill_config()).unwrap();
    let (_, got) = open(&disk, &dir.0);
    assert!(got.len() as u64 >= durable, "{} of {durable}", got.len());
    assert_eq!(got, expected(got.len() as u64));
}
