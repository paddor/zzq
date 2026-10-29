use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use zzq_proto::QoS;

use super::*;

fn msg(topic: &str) -> Msg {
    Msg::new(topic, b"x", &[], QoS::AtMostOnce, true)
}

fn clear(topic: &str) -> Msg {
    Msg::new(topic, &[], &[], QoS::AtMostOnce, true)
}

fn topics(r: &Retained, filter: &str) -> Vec<String> {
    let mut got: Vec<String> = r
        .matching(filter)
        .iter()
        .map(|m| m.topic().to_owned())
        .collect();
    got.sort();
    got
}

/// The tree agrees with matching every topic against the filter, across
/// stripes.
#[test]
fn matches_like_a_scan() {
    let topics_in = [
        "a", "a/b", "a/b/c", "a//c", "/a", "/", "b/b", "$SYS/x", "$SYS", "a/$b", "x/y/z/w",
    ];
    let filters = [
        "#", "+", "a/#", "a/+", "a/+/c", "+/b", "+/+", "/#", "+/#", "$SYS/#", "$SYS/+", "a/b/c/#",
        "a//+", "x/+/+/w", "a/$b", "+/$b", "a/b",
    ];
    let r = Retained::new(usize::MAX);
    for t in topics_in {
        r.set(&msg(t), false);
    }
    r.set(&msg("gone"), false);
    r.set(&clear("gone"), false);
    assert_eq!(r.len(), topics_in.len());
    for f in filters {
        let mut want: Vec<String> = topics_in
            .iter()
            .filter(|t| zzq_proto::topic::matches(f, t))
            .map(|t| (*t).to_owned())
            .collect();
        want.sort();
        assert_eq!(topics(&r, f), want, "filter {f}");
    }
}

#[test]
fn topics_spread_over_the_stripes() {
    let used: std::collections::BTreeSet<usize> =
        (0..100).map(|i| stripe_of(&format!("s/{i}"))).collect();
    assert_eq!(used.len(), STRIPES);
    // Stable across builds: journals hold the topics of their stripe.
    assert_eq!(stripe_of("a/b"), 1);
}

#[test]
fn bytes_follow_topics_and_a_full_budget_keeps_new_ones_out() {
    let r = Retained::new(usize::MAX);
    r.set(&msg("a/b/c"), false);
    r.set(&msg("a/b/d"), false);
    let two = r.bytes();
    let longer = Msg::new("a/b/c", b"longer", &[], QoS::AtMostOnce, true);
    r.set(&longer, false);
    assert_eq!(r.bytes(), two + 5);
    // A full budget refuses new topics, not updates.
    r.set(&msg("a/x"), true);
    r.set(&msg("a/b/c"), true);
    assert_eq!((r.len(), r.rejected(), r.bytes()), (2, 1, two));
    for topic in ["a/b/c", "a/b/d"] {
        r.set(&clear(topic), false);
    }
    assert_eq!((r.len(), r.bytes()), (0, 0));
}

#[test]
fn max_counts_topics_of_every_stripe() {
    let r = Retained::new(3);
    for i in 0..10 {
        r.set(&msg(&format!("m/{i}")), false);
    }
    assert_eq!((r.len(), r.rejected()), (3, 7));
    // Updates of stored topics still go in.
    let first = topics(&r, "m/#").remove(0);
    r.set(&Msg::new(&first, b"new", &[], QoS::AtMostOnce, true), false);
    assert_eq!((r.len(), r.rejected()), (3, 7));
}

#[test]
fn a_message_counts_until_no_session_holds_it() {
    let r = Retained::new(usize::MAX);
    r.set(&msg("a"), false);
    let one = r.bytes();
    let held = r.matching("a").pop().unwrap();
    // Replaced, the old message counts while a session holds it.
    r.set(&msg("a"), false);
    assert_eq!(r.bytes(), one + held.size());
    drop(held);
    r.sweep(crate::time::now());
    assert_eq!(r.bytes(), one);
    let held = r.matching("a").pop().unwrap();
    r.set(&clear("a"), false);
    r.sweep(crate::time::now());
    assert_eq!(r.bytes(), held.size());
    drop(held);
    r.sweep(crate::time::now());
    assert_eq!(r.bytes(), 0);
}

#[test]
fn sweep_drops_expired_but_not_replaced() {
    let r = Retained::new(usize::MAX);
    let now = crate::time::now();
    let soon = |topic: &str| {
        let mut m = msg(topic);
        m.expires_at = Some(now);
        m
    };
    r.set(&soon("e/gone"), false);
    r.set(&soon("e/kept"), false);
    r.set(&msg("e/kept"), false);
    r.set(&msg("e/plain"), false);
    assert_eq!(r.len(), 3);
    r.sweep(now + Duration::from_millis(1));
    assert_eq!(r.len(), 2);
    assert_eq!(topics(&r, "e/#"), ["e/kept", "e/plain"]);
}

#[test]
fn republished_topic_keeps_one_expiry_entry() {
    let mut tree = Tree::default();
    let now = crate::time::now();
    for i in 0..100 {
        let mut m = msg("status");
        m.expires_at = Some(now + Duration::from_secs(60 + i));
        tree.insert(m);
    }
    assert_eq!((tree.len(), tree.expiring()), (1, 1));
    tree.remove("status");
    assert_eq!((tree.len(), tree.expiring()), (0, 0));
}

#[test]
fn a_store_in_memory_journals_nothing() {
    let r = Retained::new(usize::MAX);
    assert_eq!(r.set(&msg("a"), false), None);
}

/// A data directory for the test `name`, or `None` where the temp
/// directory takes no `O_DIRECT`.
fn data_dir(name: &str) -> Option<(PathBuf, DataDir)> {
    let path = std::env::temp_dir().join(format!("zzq-retain-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&path);
    let data = DataDir::open(&path, 1 << 20).unwrap();
    match Retained::open(&data, usize::MAX, usize::MAX) {
        Ok(_) => Some((path, data)),
        Err(e) if e.kind() == io::ErrorKind::InvalidInput => {
            eprintln!("skipped: {e}");
            None
        }
        Err(e) => panic!("{e}"),
    }
}

/// Sets each message, writes the journals and closes them.
fn written(data: &DataDir, msgs: &[Msg]) -> Vec<Option<Journaled>> {
    let r = Retained::open(data, usize::MAX, usize::MAX).unwrap();
    let got = msgs.iter().map(|m| r.set(m, false)).collect();
    r.close();
    got
}

#[test]
fn journals_hold_what_the_store_held() {
    let Some((path, data)) = data_dir("reopen") else {
        return;
    };
    let mut expiring = Msg::new("e/1", b"soon", &[1, 1], QoS::AtLeastOnce, true);
    expiring.expires_at = Some(crate::time::now() + Duration::from_secs(600));
    let mut gone = msg("e/2");
    gone.expires_at = Some(crate::time::now() + Duration::from_millis(1));
    let got = written(
        &data,
        &[
            msg("k/1"),
            msg("k/2"),
            clear("k/2"),
            clear("never/stored"),
            msg("$SYS/x"),
            expiring,
            gone,
        ],
    );
    let journaled = got.iter().filter(|j| j.is_some()).count();
    assert_eq!(journaled, 5, "{got:?}");
    std::thread::sleep(Duration::from_millis(2));
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    assert_eq!(topics(&r, "#"), ["e/1", "k/1"]);
    assert_eq!(r.len(), 2);
    let m = r.matching("e/1").pop().unwrap();
    assert_eq!(
        (m.payload(), m.props(), m.qos),
        (&b"soon"[..], &[1, 1][..], QoS::AtLeastOnce)
    );
    let left = m.expires_at.unwrap() - crate::time::now();
    assert!(left > Duration::from_secs(300), "{left:?}");
    drop(r);
    // Topics deeper than allowed now stay out.
    let r = Retained::open(&data, usize::MAX, 1).unwrap();
    assert_eq!(r.len(), 0);
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn a_stale_stripe_writes_all_its_messages_after_a_clear() {
    let Some((path, data)) = data_dir("stale") else {
        return;
    };
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    // Topics of one stripe.
    let mine: Vec<String> = (0..)
        .map(|i| format!("s/{i}"))
        .filter(|t| stripe_of(t) == stripe_of("s/0"))
        .take(3)
        .collect();
    r.set(&msg(&mine[0]), false);
    r.set(&msg(&mine[1]), false);
    // As after a failed write: the journal may hold anything.
    r.stripes[stripe_of("s/0")].lock().stale = true;
    r.set(&clear(&mine[1]), false);
    let j = r.set(&msg(&mine[2]), false);
    assert!(matches!(j, Some(Journaled::At { .. })), "{j:?}");
    assert!(!r.stripes[stripe_of("s/0")].lock().stale);
    r.close();
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    let mut want = vec![mine[0].clone(), mine[2].clone()];
    want.sort();
    assert_eq!(topics(&r, "s/#"), want);
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn a_snapshot_takes_the_place_of_the_records_before_it() {
    let Some((path, data)) = data_dir("snapshot") else {
        return;
    };
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    for i in 0..50 {
        r.set(&msg(&format!("s/{}", i % 10)), false);
    }
    r.set(&clear("s/3"), false);
    let deadline = crate::time::now() + Duration::from_secs(10);
    while !r.snapshot(true) {
        assert!(crate::time::now() < deadline, "no segment for the snapshot");
        std::thread::sleep(Duration::from_millis(5));
    }
    r.set(&msg("s/after"), false);
    r.close();
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    assert_eq!(r.len(), 10);
    assert!(!topics(&r, "s/#").contains(&"s/3".to_owned()));
    drop(r);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn a_snapshot_cut_off_counts_for_nothing() {
    let seeds = record_seeds();
    let (set, clear_rec, kept, cleared) = (&seeds[0], &seeds[2], &seeds[3], &seeds[4]);
    let stripe = stripe_of("a/b");
    let replay = Replay::new(usize::MAX);
    let run = |records: &[&Vec<u8>]| {
        let mut tree = Tree::default();
        let mut snapshot = None;
        for r in records {
            let rec = Record {
                kind: r[0],
                body: &r[1..],
            };
            replay.apply(&mut tree, &mut snapshot, stripe, rec).unwrap();
        }
        tree.len()
    };
    // A finished snapshot without the topic clears it.
    assert_eq!(run(&[set, clear_rec, cleared]), 0);
    assert_eq!(run(&[set, clear_rec, kept, cleared]), 1);
    // One cut off leaves what came before, and what came after.
    assert_eq!(run(&[set, clear_rec]), 1);
    let remove = &seeds[1];
    assert_eq!(run(&[set, clear_rec, kept, remove]), 0);
    assert_eq!(run(&[clear_rec, kept, set]), 1);
}

#[tokio::test]
async fn the_cursor_follows_the_writes() {
    let Some((path, data)) = data_dir("cursor") else {
        return;
    };
    let r = Arc::new(Retained::open(&data, usize::MAX, usize::MAX).unwrap());
    let stripe = stripe_of("c/1");
    let completer = tokio::spawn({
        let r = r.clone();
        async move { r.complete(stripe).await }
    });
    let Some(Journaled::At { wait, crowded }) = r.set(&msg("c/1"), false) else {
        panic!("not journaled");
    };
    assert!(!crowded);
    let journal = JournalId::Retained(stripe);
    assert_eq!(wait, Wait { journal, n: 1 });
    let written = tokio::time::timeout(Duration::from_secs(10), r.cursor(stripe).wait(1)).await;
    assert_eq!(written.ok(), Some(true));
    r.close();
    // The completer sees the journal gone and closes the cursor.
    let ended = tokio::time::timeout(Duration::from_secs(10), completer).await;
    assert!(ended.is_ok());
    assert_eq!(r.cursor(stripe).check(2), Some(false));
    assert_eq!(r.set(&msg("c/2"), false), Some(Journaled::Failed));
    assert_eq!(r.set(&msg("$SYS/c"), false), None);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn a_stripe_with_much_waiting_for_the_disk_is_crowded() {
    let Some((path, data)) = data_dir("crowded") else {
        return;
    };
    let r = Retained::open(&data, usize::MAX, usize::MAX).unwrap();
    let big = Msg::new("big", &vec![7; CROWDED + 1], &[], QoS::AtMostOnce, true);
    let j = r.set(&big, false);
    assert!(
        matches!(j, Some(Journaled::At { crowded: true, .. })),
        "{j:?}"
    );
    r.close();
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn replay_refuses_records_no_stripe_wrote() {
    let seeds = record_seeds();
    for s in &seeds {
        assert!(replay_one(s).is_ok(), "{s:?}");
    }
    assert_eq!(replay_one(&seeds[0]), Ok(1));
    let mut tree = Tree::default();
    let rec = |kind, body| Record { kind, body };
    let replay = Replay::new(9);
    let other = (stripe_of("a/b") + 1) % STRIPES;
    let refused = [
        replay.apply(&mut tree, &mut None, other, rec(SET, &seeds[0][1..])),
        replay.apply(&mut tree, &mut None, 0, rec(9, b"")),
        replay.apply(&mut tree, &mut None, 0, rec(CLEAR, b"x")),
        replay.apply(&mut tree, &mut None, 0, rec(REMOVE, &[0xff])),
        replay.apply(&mut tree, &mut None, 0, rec(SET, b"short")),
    ];
    assert!(refused.iter().all(Result::is_err), "{refused:?}");
    let mut empty = vec![];
    state::msg_head(&mut empty, &clear("a/b"), replay.clock);
    let stripe = stripe_of("a/b");
    let r = replay.apply(&mut tree, &mut None, stripe, rec(SET, &empty));
    assert_eq!(r, Err("empty retained message"));
}
