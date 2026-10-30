//! QoS 2 handoffs: what waits for their journal writes, and what must not.

use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::time::Duration;

use zzq_proto::encode::ConnectFields;
use zzq_proto::{PacketType, QoS, Version, reason};

use super::support::{Got, Raw, publish_bytes};
use super::{Broker, BrokerOptions, Lane, WAIT, durable, id_on, on_disk, on_lane, setup};
use crate::durable::{JournalId, Wait};
use crate::msg::Msg;

fn ack(kind: PacketType, pkid: u16) -> Got {
    Got::Ack {
        kind,
        pkid,
        reason: 0,
    }
}

fn v311(client_id: &str) -> ConnectFields<'_> {
    ConnectFields {
        version: Version::V311,
        clean_start: false,
        keep_alive: 0,
        properties: &[],
        client_id,
        will: None,
        username: None,
        password: None,
    }
}

fn retained(topic: &str, payload: &[u8], qos: QoS, pkid: u16) -> Vec<u8> {
    let mut packet = publish_bytes(Version::V5, topic, payload, qos, pkid, &[]);
    packet[0] |= 1;
    packet
}

/// The broker lets go of its data directory first.
async fn clean_up(broker: Broker, dir: PathBuf) {
    broker.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

/// A record of lane 0's journal that never comes.
const NEVER: Wait = Wait {
    journal: JournalId::Lane(0),
    n: u64::MAX,
};

#[tokio::test(flavor = "multi_thread")]
async fn a_new_publish_with_the_id_of_a_finished_handoff_is_routed() {
    let (dir, opts) = on_disk("handoff-reuse");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "reuse-sub", Version::V5, 0).await;
    sub.subscribe("reuse/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("reuse-pub")).await;
    // Older handoffs on a slow lane kept this one from being reaped.
    on_lane(&ctx, &broker, 0, |l| {
        l.leave_handoff("reuse-pub", 5, Vec::new());
    })
    .await;
    p.pub_("reuse/t", b"new", QoS::ExactlyOnce, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    assert_eq!(sub.publish().await.payload, b"new");
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 5));
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_retried_publish_waits_for_the_newest_handoff_of_its_id() {
    let (dir, opts) = on_disk("handoff-newest");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("newest-pub")).await;
    // A finished flow, then the one this ID stands for now, whose record
    // is not written.
    on_lane(&ctx, &broker, 0, |l| {
        l.leave_handoff("newest-pub", 5, Vec::new());
        l.leave_handoff("newest-pub", 5, vec![NEVER]);
        let key = l.sessions.keys().next().unwrap();
        l.sessions
            .get_mut(key)
            .unwrap()
            .inbound
            .awaiting_rel
            .insert(5);
    })
    .await;
    let mut packet = publish_bytes(Version::V5, "newest/t", b"x", QoS::ExactlyOnce, 5, &[]);
    packet[0] |= 0x08;
    p.send(&packet).await;
    p.quiet(100).await;
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stalled_journal_stops_only_the_publish_that_waits_for_it() {
    let (dir, opts) = on_disk("handoff-stall");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "stall-sub", Version::V5, 0).await;
    sub.subscribe("stall/#", QoS::AtMostOnce.into()).await;
    let (mut held, _) = Raw::connect_with(&uri, &durable("stall-q2")).await;
    let mut other = Raw::connect(&uri, "stall-q0", Version::V5, 0).await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(true, Ordering::Relaxed);
    held.pub_("stall/q2", b"held", QoS::ExactlyOnce, 1).await;
    held.quiet(100).await;
    for payload in [&b"one"[..], b"two"] {
        other.pub_("stall/q0", payload, QoS::AtMostOnce, 0).await;
        assert_eq!(sub.publish().await.payload, payload);
    }
    tokio::time::timeout(WAIT, broker.stats())
        .await
        .expect("the lane's controls wait for the journal")
        .unwrap();
    log.paused.store(false, Ordering::Relaxed);
    assert_eq!(held.next().await, ack(PacketType::PubRec, 1));
    assert_eq!(sub.publish().await.payload, b"held");
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reset_does_not_stop_later_handoffs() {
    let (dir, opts) = on_disk("handoff-reset");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("reset-pub")).await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(true, Ordering::Relaxed);
    p.pub_("reset/t", b"old", QoS::ExactlyOnce, 1).await;
    p.quiet(100).await;
    on_lane(&ctx, &broker, 0, Lane::reset).await;
    log.paused.store(false, Ordering::Relaxed);
    let mut sub = Raw::connect(&uri, "reset-sub", Version::V5, 0).await;
    sub.subscribe("reset/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("reset-pub")).await;
    p.pub_("reset/t", b"new", QoS::ExactlyOnce, 2).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 2));
    assert_eq!(sub.publish().await.payload, b"new");
    clean_up(broker, dir).await;
}

/// A SUBSCRIBE either is matched, or finds the message in the store.
#[tokio::test(flavor = "multi_thread")]
async fn a_retained_qos2_publish_is_in_the_store_before_its_journal_write() {
    let (dir, opts) = on_disk("handoff-store");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("store-pub")).await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(true, Ordering::Relaxed);
    p.send(&retained("store/t", b"kept", QoS::ExactlyOnce, 1))
        .await;
    p.quiet(100).await;
    let mut sub = Raw::connect(&uri, "store-sub", Version::V5, 0).await;
    sub.subscribe("store/t", QoS::AtMostOnce.into()).await;
    assert_eq!(sub.publish().await.payload, b"kept");
    log.paused.store(false, Ordering::Relaxed);
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qos1_publish_does_not_pass_a_qos2_publish_of_the_same_read() {
    let (dir, opts) = on_disk("handoff-order");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "order-sub", Version::V5, 0).await;
    sub.subscribe("order/t", QoS::AtMostOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("order-pub")).await;
    let mut read = retained("order/t", b"old", QoS::ExactlyOnce, 1);
    read.extend(retained("order/t", b"new", QoS::AtLeastOnce, 2));
    p.send(&read).await;
    assert_eq!(sub.publish().await.payload, b"old");
    assert_eq!(sub.publish().await.payload, b"new");
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    assert_eq!(p.next().await, ack(PacketType::PubAck, 2));
    let mut late = Raw::connect(&uri, "order-late", Version::V5, 0).await;
    late.subscribe("order/t", QoS::AtMostOnce.into()).await;
    assert_eq!(late.publish().await.payload, b"new");
    clean_up(broker, dir).await;
}

/// PUBREC 0 would say the broker has the message.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_qos2_publish_of_a_v311_client_is_new_when_it_comes_again() {
    let (dir, opts) = on_disk("handoff-v311");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "v311-sub", Version::V5, 0).await;
    sub.subscribe("v311/t", QoS::ExactlyOnce.into()).await;
    broker.shared().retained.cursor_of("v311/t").fail(1, 1);
    let id = "v311-pub";
    let mut packet = publish_bytes(Version::V311, "v311/t", b"x", QoS::ExactlyOnce, 5, &[]);
    packet[0] |= 1;
    let (mut p, _) = Raw::connect_with(&uri, &v311(id)).await;
    p.send(&packet).await;
    p.closed().await;
    packet[0] |= 0x08;
    let (mut p, _) = Raw::connect_with(&uri, &v311(id)).await;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    assert_eq!(sub.publish().await.payload, b"x");
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 5));
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_full_state_budget_stops_growth_not_live_qos2_traffic() {
    let (dir, opts) = on_disk("handoff-budget");
    let opts = BrokerOptions {
        max_state_bytes: 1,
        ..opts()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let mut sub = Raw::connect(&uri, "budget-sub", Version::V5, 0).await;
    sub.subscribe("budget/t", QoS::ExactlyOnce.into()).await;
    on_lane(&ctx, &broker, 0, |l| l.budget.full = true).await;
    let mut v5 = Raw::connect(&uri, "budget-v5", Version::V5, 0).await;
    v5.pub_("budget/t", b"five", QoS::ExactlyOnce, 1).await;
    assert_eq!(v5.next().await, ack(PacketType::PubRec, 1));
    let delivered = sub.publish().await;
    assert_eq!(delivered.payload, b"five");
    sub.complete(&delivered).await;
    let clean = ConnectFields {
        clean_start: true,
        ..v311("budget-v3")
    };
    let (mut v3, _) = Raw::connect_with(&uri, &clean).await;
    let packet = publish_bytes(
        Version::V311,
        "budget/t",
        b"three",
        QoS::ExactlyOnce,
        2,
        &[],
    );
    v3.send(&packet).await;
    assert_eq!(v3.next().await, ack(PacketType::PubRec, 2));
    assert_eq!(sub.publish().await.payload, b"three");
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reconnect_without_expiry_goes_on_after_a_failed_output_barrier() {
    use crate::durable::Due;

    let (dir, opts) = on_disk("handoff-due");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let id = "due-sub";
    let (first, _) = Raw::connect_with(&uri, &durable(id)).await;
    drop(first);
    super::stats_until(&broker, |s| s.connections == 0).await;
    on_lane(&ctx, &broker, 0, |l| {
        let key = l.sessions.keys().next().unwrap();
        l.sessions.get_mut(key).unwrap().out.due = Due::Failed;
    })
    .await;
    let again = ConnectFields {
        properties: &[],
        ..durable(id)
    };
    let (mut c, code) = Raw::connect_with(&uri, &again).await;
    assert_eq!(code, reason::SUCCESS);
    c.subscribe("due/t", QoS::AtMostOnce.into()).await;
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_finished_handoff_behind_stuck_ones_is_reaped() {
    let (dir, opts) = on_disk("handoff-reap");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (_p, _) = Raw::connect_with(&uri, &durable("reap-pub")).await;
    let (stuck, finished) = on_lane(&ctx, &broker, 0, |l| {
        let stuck: Vec<u64> = (1..=64)
            .map(|pkid| l.leave_handoff("reap-pub", pkid, vec![NEVER]))
            .collect();
        (stuck, l.leave_handoff("reap-pub", 65, Vec::new()))
    })
    .await;
    super::lane_until(&ctx, &broker, 0, move |l| {
        l.sweep(crate::time::now());
        (!l.has_handoff(finished)).then_some(())
    })
    .await;
    let kept = on_lane(&ctx, &broker, 0, move |l| {
        stuck.iter().all(|&seq| l.has_handoff(seq))
    })
    .await;
    assert!(kept);
    clean_up(broker, dir).await;
}

/// The store's write can reach the disk before the source's does, and a
/// crash then. A new handoff must not take the number the store has seen.
#[tokio::test(flavor = "multi_thread")]
async fn a_retained_receipt_ahead_of_the_source_journal_does_not_swallow_a_handoff() {
    let (dir, opts) = on_disk("handoff-receipt");
    {
        let (_ctx, broker, _uri) = setup(1, opts()).await;
        let msg = Msg::new("receipt/t", b"old", &[], QoS::ExactlyOnce, true);
        broker.shared().retained.set_once(0, 100, &msg, false);
        broker.shutdown().await.unwrap();
    }
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("receipt-pub")).await;
    p.send(&retained("receipt/t", b"new", QoS::ExactlyOnce, 1))
        .await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    let mut sub = Raw::connect(&uri, "receipt-sub", Version::V5, 0).await;
    sub.subscribe("receipt/t", QoS::AtMostOnce.into()).await;
    assert_eq!(sub.publish().await.payload, b"new");
    clean_up(broker, dir).await;
}

/// Longer than a segment's room beyond a packet: the routing decision and
/// the destination's transaction both go over it.
#[tokio::test(flavor = "multi_thread")]
async fn a_handoff_longer_than_a_segment_survives_a_restart() {
    let (dir, opts) = on_disk("handoff-long");
    let max_packet_size = (64 << 20) - (1 << 20);
    let opts = move || BrokerOptions {
        max_packet_size,
        ..opts()
    };
    let ids: Vec<String> = (0..20)
        .map(|i| format!("{i:02}{}", "s".repeat(60_000)))
        .collect();
    let payload = vec![7u8; max_packet_size - 64];
    {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        for id in &ids {
            let (mut sub, code) = Raw::connect_with(&uri, &durable(id)).await;
            assert_eq!(code, reason::SUCCESS);
            sub.subscribe("long/t", QoS::ExactlyOnce.into()).await;
        }
        super::stats_until(&broker, |s| s.connections == 0).await;
        let (mut p, _) = Raw::connect_with(&uri, &durable("long-pub")).await;
        p.pub_("long/t", &payload, QoS::ExactlyOnce, 1).await;
        assert_eq!(next_slow(&mut p).await, ack(PacketType::PubRec, 1));
        p.ack(PacketType::PubRel, 1).await;
        assert_eq!(next_slow(&mut p).await, ack(PacketType::PubComp, 1));
        let stats = broker.stats().await.unwrap();
        assert_eq!(stats.write_errors, 0);
        broker.shutdown().await.unwrap();
    }
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable(&ids[7])).await;
    let Got::Publish(got) = next_slow(&mut sub).await else {
        panic!("expected PUBLISH");
    };
    assert_eq!(got.payload.len(), payload.len());
    clean_up(broker, dir).await;
}

/// Waits until the journal of lane 0 wrote what it has, then holds its writes.
async fn pause_lane_journal(broker: &Broker) {
    let log = &broker.shared().lane_logs[0];
    assert!(
        tokio::time::timeout(WAIT, log.cursor.wait(log.counts().0))
            .await
            .unwrap()
    );
    log.paused.store(true, Ordering::Relaxed);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qos2_publish_to_its_own_lane_takes_one_record_and_one_write_to_its_pubrec() {
    let (dir, opts) = on_disk("handoff-one-write");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("one-sub")).await;
    sub.subscribe("one/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("one-pub")).await;
    pause_lane_journal(&broker).await;
    let log = &broker.shared().lane_logs[0];
    let (records, writes) = log.counts();
    p.pub_("one/t", b"x", QoS::ExactlyOnce, 1).await;
    p.quiet(100).await;
    sub.quiet(0).await;
    assert_eq!(log.counts().0, records + 1, "one record");
    log.paused.store(false, Ordering::Relaxed);
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    assert_eq!(log.counts().1, writes + 1, "one write");
    assert_eq!(sub.publish().await.payload, b"x");
    clean_up(broker, dir).await;
}

/// PUBREC 0 says the broker has the message, which is only true once the
/// record of the first PUBLISH is written.
#[tokio::test(flavor = "multi_thread")]
async fn a_retry_of_a_publish_to_its_own_lane_waits_for_the_first_ones_record() {
    let (dir, opts) = on_disk("handoff-local-retry");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("retry-sub")).await;
    sub.subscribe("retry/t", QoS::ExactlyOnce.into()).await;
    let (mut first, _) = Raw::connect_with(&uri, &durable("retry-pub")).await;
    pause_lane_journal(&broker).await;
    first.pub_("retry/t", b"x", QoS::ExactlyOnce, 5).await;
    first.quiet(100).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("retry-pub")).await;
    let mut packet = publish_bytes(Version::V5, "retry/t", b"x", QoS::ExactlyOnce, 5, &[]);
    packet[0] |= 0x08;
    p.send(&packet).await;
    p.quiet(100).await;
    broker.shared().lane_logs[0]
        .paused
        .store(false, Ordering::Relaxed);
    assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 5));
    let got = sub.publish().await;
    assert_eq!(got.payload, b"x");
    sub.complete(&got).await;
    sub.quiet(100).await;
    clean_up(broker, dir).await;
}

/// The subscriber's state holds the message whatever became of the write, so
/// an error would make a second message of the client's next attempt.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_write_of_a_publish_to_its_own_lane_delivers_once_after_the_retry() {
    let (dir, opts) = on_disk("handoff-local-failed");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("failed-sub")).await;
    sub.subscribe("failed/t", QoS::ExactlyOnce.into()).await;
    let (mut first, _) = Raw::connect_with(&uri, &durable("failed-pub")).await;
    pause_lane_journal(&broker).await;
    broker.shared().fail_next_write(JournalId::Lane(0));
    broker.shared().lane_logs[0]
        .paused
        .store(false, Ordering::Relaxed);
    first.pub_("failed/t", b"x", QoS::ExactlyOnce, 5).await;
    assert_eq!(
        first.next().await,
        Got::Disconnect {
            reason: reason::UNSPECIFIED_ERROR
        }
    );
    sub.closed().await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("failed-pub")).await;
    let mut packet = publish_bytes(Version::V5, "failed/t", b"x", QoS::ExactlyOnce, 5, &[]);
    packet[0] |= 0x08;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 5));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 5));
    let again = ConnectFields {
        properties: &[],
        ..durable("failed-sub")
    };
    let (mut sub, _) = Raw::connect_with(&uri, &again).await;
    let got = sub.publish().await;
    assert_eq!(got.payload, b"x");
    sub.complete(&got).await;
    sub.quiet(100).await;
    clean_up(broker, dir).await;
}

/// The retained publish waits for its writes, and the plain one behind it
/// must too.
#[tokio::test(flavor = "multi_thread")]
async fn a_qos2_publish_does_not_pass_a_retained_one_of_the_same_read() {
    let (dir, opts) = on_disk("handoff-retained-order");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("order2-sub")).await;
    sub.subscribe("order2/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("order2-pub")).await;
    let mut read = retained("order2/t", b"first", QoS::ExactlyOnce, 1);
    read.extend(publish_bytes(
        Version::V5,
        "order2/t",
        b"second",
        QoS::ExactlyOnce,
        2,
        &[],
    ));
    p.send(&read).await;
    assert_eq!(sub.publish().await.payload, b"first");
    assert_eq!(sub.publish().await.payload, b"second");
    clean_up(broker, dir).await;
}

/// Its session is in RAM, so no write of its own holds it back, and a crash
/// would make the publisher's retry a second message for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscriber_in_ram_gets_a_qos2_message_only_after_the_write() {
    let (dir, opts) = on_disk("handoff-ram-sub");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "ram-sub", Version::V5, 0).await;
    sub.subscribe("ram/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("ram-pub")).await;
    pause_lane_journal(&broker).await;
    p.pub_("ram/t", b"x", QoS::ExactlyOnce, 1).await;
    sub.quiet(100).await;
    broker.shared().lane_logs[0]
        .paused
        .store(false, Ordering::Relaxed);
    assert_eq!(sub.publish().await.payload, b"x");
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    clean_up(broker, dir).await;
}

/// A long record takes many numbers of its journal, and what a session made
/// of its message was journaled before the last of them was known.
#[tokio::test(flavor = "multi_thread")]
async fn a_delivery_in_a_long_record_waits_for_its_last_part() {
    use crate::durable::Due;

    for lanes in [1, 2] {
        let (dir, opts) = on_disk(&format!("handoff-parts-{lanes}"));
        let (ctx, broker, uri) = setup(lanes, opts()).await;
        let to = lanes - 1;
        let subscriber = id_on("parts-sub", to, lanes);
        let publisher = id_on("parts-pub", 0, lanes);
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        sub.subscribe("parts/t", QoS::ExactlyOnce.into()).await;
        let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
        let log = &broker.shared().lane_logs[to];
        assert!(
            tokio::time::timeout(WAIT, log.cursor.wait(log.counts().0))
                .await
                .unwrap()
        );
        log.paused.store(true, Ordering::Relaxed);
        p.pub_("parts/t", &vec![7; 1 << 20], QoS::ExactlyOnce, 1)
            .await;
        let id = subscriber.clone();
        let due = super::lane_until(&ctx, &broker, to, move |l| {
            let key = l.by_client_key(&id)?;
            let due = l.sessions.get(key)?.out.due;
            (due != Due::Now).then_some(due)
        })
        .await;
        let wait = Wait {
            journal: JournalId::Lane(to),
            n: log.counts().0,
        };
        assert_eq!(due, Due::After(wait));
        log.paused.store(false, Ordering::Relaxed);
        assert_eq!(sub.publish().await.payload.len(), 1 << 20);
        clean_up(broker, dir).await;
    }
}

/// The next packet, after writes of many megabytes.
async fn next_slow(raw: &mut Raw) -> Got {
    raw.try_next_within(Duration::from_secs(120))
        .await
        .flatten()
        .expect("no packet")
}

/// Nothing waits for the record that a handoff finished, and a crash that
/// loses it only sends the handoff again. A write of its own would make the
/// publisher's next records wait for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_finished_handoff_starts_no_write_of_its_own() {
    let (dir, opts) = on_disk("handoff-handed");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (_p, _) = Raw::connect_with(&uri, &durable("handed-pub")).await;
    pause_lane_journal(&broker).await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(false, Ordering::Relaxed);
    let (before, after) = on_lane(&ctx, &broker, 0, |l| {
        let shared = l.shared.clone();
        let seq = l.leave_handoff("handed-pub", 1, Vec::new());
        let before = shared.lane_logs[0].counts();
        l.finish_handoff(seq);
        (before, shared.lane_logs[0].counts())
    })
    .await;
    assert_eq!(after, (before.0 + 1, before.1));
    // The sweep sends it.
    on_lane(&ctx, &broker, 0, |l| l.sweep(crate::time::now())).await;
    assert!(
        tokio::time::timeout(WAIT, log.cursor.wait(after.0))
            .await
            .unwrap()
    );
    clean_up(broker, dir).await;
}

/// A publisher on lane 0 and a kept subscriber of `topic` on lane 1.
async fn across_lanes(uri: &str, topic: &str, name: &str) -> (Raw, Raw) {
    let subscriber = id_on(&format!("{name}-sub"), 1, 2);
    let (mut sub, _) = Raw::connect_with(uri, &durable(&subscriber)).await;
    sub.subscribe(topic, QoS::ExactlyOnce.into()).await;
    let publisher = id_on(&format!("{name}-pub"), 0, 2);
    let (p, _) = Raw::connect_with(uri, &durable(&publisher)).await;
    (p, sub)
}

/// The routing decision on disk is enough: a crash resends it from there.
/// Only PUBCOMP frees the publisher's slot, so what it can leave unfinished
/// on other lanes stays bounded.
#[tokio::test(flavor = "multi_thread")]
async fn a_pubrec_needs_the_source_write_and_a_pubcomp_the_destination() {
    let (dir, opts) = on_disk("handoff-acks");
    let (ctx, broker, uri) = setup(2, opts()).await;
    let (mut p, mut sub) = across_lanes(&uri, "acks/t", "acks").await;
    let go = super::crash_cut::stop_destination(&ctx).await;
    p.pub_("acks/t", b"x", QoS::ExactlyOnce, 1).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    p.ack(PacketType::PubRel, 1).await;
    p.quiet(100).await;
    drop(go);
    assert_eq!(p.next().await, ack(PacketType::PubComp, 1));
    assert_eq!(sub.publish().await.payload, b"x");
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_later_publish_does_not_pass_a_handoff_on_its_way() {
    let (dir, opts) = on_disk("handoff-pass");
    let (ctx, broker, uri) = setup(2, opts()).await;
    let (mut p, mut sub) = across_lanes(&uri, "pass/t", "pass").await;
    let go = super::crash_cut::stop_destination(&ctx).await;
    p.pub_("pass/t", b"first", QoS::ExactlyOnce, 1).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    p.pub_("pass/t", b"second", QoS::AtLeastOnce, 2).await;
    p.quiet(100).await;
    drop(go);
    assert_eq!(sub.publish().await.payload, b"first");
    assert_eq!(sub.publish().await.payload, b"second");
    assert_eq!(p.next().await, ack(PacketType::PubAck, 2));
    clean_up(broker, dir).await;
}

/// The publisher has its PUBREC, so nobody else knows of the message but the
/// source journal. The destination takes it once it can.
#[tokio::test(flavor = "multi_thread")]
async fn a_destination_that_cannot_journal_a_handoff_takes_it_later() {
    let (dir, opts) = on_disk("handoff-later");
    let (ctx, broker, uri) = setup(2, opts()).await;
    let (mut p, mut sub) = across_lanes(&uri, "later/t", "later").await;
    let log = &broker.shared().lane_logs[1];
    log.refuse_transactions.store(true, Ordering::Relaxed);
    p.pub_("later/t", b"one", QoS::ExactlyOnce, 1).await;
    p.pub_("later/t", b"two", QoS::ExactlyOnce, 2).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    assert_eq!(p.next().await, ack(PacketType::PubRec, 2));
    p.ack(PacketType::PubRel, 1).await;
    p.quiet(100).await;
    sub.quiet(0).await;
    log.refuse_transactions.store(false, Ordering::Relaxed);
    super::on_lane(&ctx, &broker, 1, |l| l.sweep(crate::time::now())).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp, 1));
    assert_eq!(sub.publish().await.payload, b"one");
    assert_eq!(sub.publish().await.payload, b"two");
    clean_up(broker, dir).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_written_handoff_stays_until_a_failed_destination_has_it() {
    let (dir, opts) = on_disk("handoff-failed-dest");
    let (ctx, broker, uri) = setup(1, opts()).await;
    let (_p, _) = Raw::connect_with(&uri, &durable("kept-pub")).await;
    let journal = JournalId::Handoff { from: 0, to: 0 };
    broker.shared().cursor(journal).fail(1, 1);
    let waits = vec![Wait { journal, n: 1 }];
    let (kept, refused) = on_lane(&ctx, &broker, 0, move |l| {
        let refused = l.leave_handoff("kept-pub", 1, waits.clone());
        l.refuse_handoff(refused);
        (l.leave_handoff("kept-pub", 2, waits), refused)
    })
    .await;
    super::lane_until(&ctx, &broker, 0, move |l| {
        l.sweep(crate::time::now());
        (!l.has_handoff(refused)).then_some(())
    })
    .await;
    assert!(on_lane(&ctx, &broker, 0, move |l| l.has_handoff(kept)).await);
    clean_up(broker, dir).await;
}

/// Its lane has nobody to deliver to, and its record would be one more write
/// for the publisher's next records to wait behind.
#[tokio::test(flavor = "multi_thread")]
async fn a_handoff_to_another_lane_makes_one_record_on_its_own() {
    let (dir, opts) = on_disk("handoff-own-lane");
    let (ctx, broker, uri) = setup(2, opts()).await;
    let (mut p, _sub) = across_lanes(&uri, "own/t", "own").await;
    pause_lane_journal(&broker).await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(false, Ordering::Relaxed);
    let before = log.counts().0;
    let go = super::crash_cut::stop_destination(&ctx).await;
    p.pub_("own/t", b"x", QoS::ExactlyOnce, 1).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    p.quiet(100).await;
    assert_eq!(log.counts().0, before + 1);
    drop(go);
    clean_up(broker, dir).await;
}

/// Nothing of it reached the lane's state, so the publisher can be told, and
/// the flow ends: the same ID is a new publish.
#[tokio::test(flavor = "multi_thread")]
async fn a_publish_to_its_own_lane_without_a_journal_to_take_it_is_refused() {
    let (dir, opts) = on_disk("handoff-refused");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("refused-sub")).await;
    sub.subscribe("refused/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("refused-pub")).await;
    let log = &broker.shared().lane_logs[0];
    log.refuse_transactions.store(true, Ordering::Relaxed);
    p.pub_("refused/t", b"x", QoS::ExactlyOnce, 1).await;
    let refused = Got::Ack {
        kind: PacketType::PubRec,
        pkid: 1,
        reason: reason::UNSPECIFIED_ERROR,
    };
    assert_eq!(p.next().await, refused);
    sub.quiet(100).await;
    log.refuse_transactions.store(false, Ordering::Relaxed);
    p.pub_("refused/t", b"y", QoS::ExactlyOnce, 1).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    assert_eq!(sub.publish().await.payload, b"y");
    clean_up(broker, dir).await;
}

/// A session in RAM and no subscribers: there is nothing to keep.
#[tokio::test(flavor = "multi_thread")]
async fn a_publish_with_nothing_to_keep_needs_no_write_for_its_pubrec() {
    let (dir, opts) = on_disk("handoff-nothing");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let mut p = Raw::connect(&uri, "nothing-pub", Version::V5, 0).await;
    pause_lane_journal(&broker).await;
    p.pub_("nothing/t", b"x", QoS::ExactlyOnce, 1).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec, 1));
    broker.shared().lane_logs[0]
        .paused
        .store(false, Ordering::Relaxed);
    clean_up(broker, dir).await;
}

/// A retained publish goes through the lane's mailbox, so its PUBREC may leave
/// before the lane knows of the congestion: its PUBCOMP holds the publisher
/// back then.
#[tokio::test(flavor = "multi_thread")]
async fn an_ack_of_a_retained_publish_waits_for_a_congested_subscriber() {
    let (dir, opts) = on_disk("handoff-congested");
    let opts = BrokerOptions {
        max_inflight: 1,
        qos_queue_high: 2,
        ..opts()
    };
    let (_ctx, broker, uri) = setup(1, opts).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("congested-sub")).await;
    sub.subscribe("congested/t", QoS::AtLeastOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("congested-pub")).await;
    for pkid in 1..=2 {
        p.send(&retained("congested/t", b"x", QoS::ExactlyOnce, pkid))
            .await;
        assert_eq!(p.next().await, ack(PacketType::PubRec, pkid));
        p.ack(PacketType::PubRel, pkid).await;
        assert_eq!(p.next().await, ack(PacketType::PubComp, pkid));
    }
    p.send(&retained("congested/t", b"x", QoS::ExactlyOnce, 3))
        .await;
    let early = p
        .try_next_within(Duration::from_millis(200))
        .await
        .flatten();
    if let Some(got) = &early {
        assert_eq!(*got, ack(PacketType::PubRec, 3));
        p.ack(PacketType::PubRel, 3).await;
    }
    p.quiet(100).await;
    for _ in 0..3 {
        let got = sub.publish().await;
        sub.ack(PacketType::PubAck, got.pkid).await;
    }
    if early.is_none() {
        assert_eq!(p.next().await, ack(PacketType::PubRec, 3));
        p.ack(PacketType::PubRel, 3).await;
    }
    assert_eq!(p.next().await, ack(PacketType::PubComp, 3));
    clean_up(broker, dir).await;
}
