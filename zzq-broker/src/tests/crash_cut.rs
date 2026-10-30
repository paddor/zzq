//! Crash cuts between the independent lane journals.

use std::path::Path;

use zzq_proto::{PacketType, QoS, SubscriptionOptions, Version};

use super::support::{Got, Raw, publish_bytes};
use super::{Broker, Context, WAIT, durable, id_on, on_disk, setup};

fn copy_tree(source: &Path, target: &Path) {
    std::fs::create_dir_all(target).unwrap();
    for entry in std::fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_tree(&entry.path(), &to);
        } else {
            std::fs::copy(entry.path(), to).unwrap();
        }
    }
}

/// Dropping the sender also releases the blocked lane after an assertion.
pub(super) async fn stop_destination(ctx: &Context) -> std::sync::mpsc::Sender<()> {
    let (stopped, on_stop) = tokio::sync::oneshot::channel();
    let (go, on_go) = std::sync::mpsc::channel();
    let lanes = ctx.clone();
    tokio::spawn(async move {
        lanes
            .lane(1)
            .call(move || {
                let _ = stopped.send(());
                let _ = on_go.recv_timeout(std::time::Duration::from_secs(30));
            })
            .await
            .unwrap();
    });
    on_stop.await.unwrap();
    go
}

async fn source_written(broker: &Broker, before: u64) {
    let log = &broker.shared().lane_logs[0];
    let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
    tokio::time::timeout(WAIT, async {
        loop {
            tick.tick().await;
            let now = log.counts().0;
            if now > before && log.cursor.check(now) == Some(true) {
                break;
            }
        }
    })
    .await
    .expect("publisher's inbound record was not written");
}

fn ack(kind: PacketType) -> Got {
    Got::Ack {
        kind,
        pkid: 5,
        reason: 0,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qos2_delivery_waits_for_its_packet_id_to_reach_the_journal() {
    use std::sync::atomic::Ordering;

    let (dir, opts) = on_disk("qos2-output-barrier");
    let (_ctx, broker, uri) = setup(1, opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable("barrier-sub")).await;
    sub.subscribe("barrier/t", SubscriptionOptions::from(QoS::ExactlyOnce))
        .await;
    let log = &broker.shared().lane_logs[0];
    log.paused.store(true, Ordering::Relaxed);
    broker
        .publish("barrier/t", "once", QoS::ExactlyOnce)
        .unwrap();
    sub.quiet(100).await;
    log.paused.store(false, Ordering::Relaxed);
    assert_eq!(sub.publish().await.payload, b"once");
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_qos2_retry_recovers_a_delivery_missing_from_the_other_lane() {
    missing_destination("plain", false, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_keeps_an_unfinished_handoff() {
    missing_destination("snapshot", true, 2).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn changing_lane_count_finishes_an_unfinished_handoff() {
    missing_destination("migration", false, 3).await;
}

async fn missing_destination(name: &str, snapshot: bool, lanes_after: usize) {
    let (dir, opts) = on_disk(&format!("qos2-crash-source-{name}"));
    let (copy, copy_opts) = on_disk(&format!("qos2-crash-copy-{name}"));
    let publisher = id_on("crash-pub", 0, 2);
    let subscriber = id_on("crash-sub", 1, 2);
    let mut packet = publish_bytes(Version::V5, "crash/t", b"once", QoS::ExactlyOnce, 5, &[]);
    {
        let (ctx, broker, uri) = setup(2, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        sub.subscribe("crash/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
        let before = broker.shared().lane_logs[0].counts().0;
        let go = stop_destination(&ctx).await;
        p.send(&packet).await;
        source_written(&broker, before).await;
        if snapshot {
            super::lane_until(&ctx, &broker, 0, |l| l.snapshot_journal(true).then_some(())).await;
            source_written(&broker, before).await;
        }
        // A possible crash image: the handoff reached disk, and the destination
        // has not even handled its mailbox.
        copy_tree(&dir, &copy);
        drop(go);
        assert_eq!(p.next().await, ack(PacketType::PubRec));
    }
    let (_ctx, _broker, uri) = setup(lanes_after, copy_opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
    packet[0] |= 0x08;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp));
    let message = sub.publish().await;
    assert_eq!(message.payload, b"once");
    sub.complete(&message).await;
    sub.quiet(100).await;
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(copy).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_committed_destination_does_not_repeat_an_unfinished_source_handoff() {
    let (dir, opts) = on_disk("qos2-receipt-source");
    let (copy, copy_opts) = on_disk("qos2-receipt-copy");
    let publisher = id_on("receipt-pub", 0, 2);
    let subscriber = id_on("receipt-sub", 1, 2);
    let mut packet = publish_bytes(Version::V5, "receipt/t", b"once", QoS::ExactlyOnce, 5, &[]);
    {
        let (ctx, broker, uri) = setup(2, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        sub.subscribe("receipt/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
        let before = broker.shared().lane_logs[0].counts().0;
        let go = stop_destination(&ctx).await;
        p.send(&packet).await;
        source_written(&broker, before).await;
        copy_tree(&dir, &copy);
        drop(go);
        let message = sub.publish().await;
        sub.complete(&message).await;
        let id = subscriber.clone();
        super::lane_until(&ctx, &broker, 1, move |l| {
            let finished = l.sessions.keys().any(|key| {
                l.sessions
                    .get(key)
                    .is_some_and(|s| *s.client_id == id && s.inflight().len() == 0)
            });
            finished.then_some(())
        })
        .await;
        // A newer destination cut has completed its delivery. The source
        // cut still holds the recoverable routing decision.
        let log = &broker.shared().lane_logs[1];
        assert!(
            tokio::time::timeout(WAIT, log.cursor.wait(log.counts().0))
                .await
                .unwrap()
        );
        copy_tree(&dir.join("lanes-2/lane-1"), &copy.join("lanes-2/lane-1"));
    }
    let (_ctx, _broker, uri) = setup(2, copy_opts()).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
    packet[0] |= 0x08;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp));
    sub.quiet(100).await;
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(copy).unwrap();
}

/// The publisher has its PUBREC, so it only sends PUBREL again. The routing
/// decision on disk still reaches the destination.
#[tokio::test(flavor = "multi_thread")]
async fn a_pubrec_sent_before_the_destination_has_it_still_delivers_after_a_crash() {
    for lanes_after in [2, 1] {
        let name = format!("qos2-early-pubrec-{lanes_after}");
        let (dir, opts) = on_disk(&name);
        let (copy, copy_opts) = on_disk(&format!("{name}-copy"));
        let publisher = id_on("early-pub", 0, 2);
        let subscriber = id_on("early-sub", 1, 2);
        {
            let (ctx, _broker, uri) = setup(2, opts()).await;
            let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
            sub.subscribe("early/t", SubscriptionOptions::from(QoS::ExactlyOnce))
                .await;
            let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
            let go = stop_destination(&ctx).await;
            p.pub_("early/t", b"once", QoS::ExactlyOnce, 5).await;
            assert_eq!(p.next().await, ack(PacketType::PubRec));
            copy_tree(&dir, &copy);
            drop(go);
        }
        let (_ctx, _broker, uri) = setup(lanes_after, copy_opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
        p.ack(PacketType::PubRel, 5).await;
        assert_eq!(p.next().await, ack(PacketType::PubComp));
        let message = sub.publish().await;
        assert_eq!(message.payload, b"once");
        sub.complete(&message).await;
        sub.quiet(100).await;
        std::fs::remove_dir_all(dir).unwrap();
        std::fs::remove_dir_all(copy).unwrap();
    }
}

/// One record holds the publish and its delivery: a crash leaves both.
#[tokio::test(flavor = "multi_thread")]
async fn a_written_publish_to_its_own_lane_is_not_repeated_after_a_crash() {
    let (dir, opts) = on_disk("qos2-local-written");
    let (copy, copy_opts) = on_disk("qos2-local-written-copy");
    let mut packet = publish_bytes(Version::V5, "local/t", b"once", QoS::ExactlyOnce, 5, &[]);
    {
        let (_ctx, _broker, uri) = setup(1, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable("local-sub")).await;
        sub.subscribe("local/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        let (mut p, _) = Raw::connect_with(&uri, &durable("local-pub")).await;
        p.send(&packet).await;
        assert_eq!(p.next().await, ack(PacketType::PubRec));
        copy_tree(&dir, &copy);
    }
    let (_ctx, _broker, uri) = setup(1, copy_opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("local-pub")).await;
    packet[0] |= 0x08;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec));
    p.ack(PacketType::PubRel, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubComp));
    let (mut sub, _) = Raw::connect_with(&uri, &durable("local-sub")).await;
    let got = sub.publish().await;
    assert_eq!(got.payload, b"once");
    sub.complete(&got).await;
    sub.quiet(100).await;
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(copy).unwrap();
}

/// The crash took the record: the retry is the message's first arrival.
#[tokio::test(flavor = "multi_thread")]
async fn an_unwritten_publish_to_its_own_lane_is_new_after_a_crash() {
    use std::sync::atomic::Ordering;

    let (dir, opts) = on_disk("qos2-local-unwritten");
    let (copy, copy_opts) = on_disk("qos2-local-unwritten-copy");
    let mut packet = publish_bytes(Version::V5, "local2/t", b"once", QoS::ExactlyOnce, 5, &[]);
    {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable("local2-sub")).await;
        sub.subscribe("local2/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        let (mut p, _) = Raw::connect_with(&uri, &durable("local2-pub")).await;
        let log = &broker.shared().lane_logs[0];
        assert!(
            tokio::time::timeout(WAIT, log.cursor.wait(log.counts().0))
                .await
                .unwrap()
        );
        log.paused.store(true, Ordering::Relaxed);
        p.send(&packet).await;
        p.quiet(100).await;
        sub.quiet(0).await;
        copy_tree(&dir, &copy);
        log.paused.store(false, Ordering::Relaxed);
    }
    let (_ctx, _broker, uri) = setup(1, copy_opts()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("local2-pub")).await;
    packet[0] |= 0x08;
    p.send(&packet).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec));
    let (mut sub, _) = Raw::connect_with(&uri, &durable("local2-sub")).await;
    let got = sub.publish().await;
    assert_eq!(got.payload, b"once");
    sub.complete(&got).await;
    sub.quiet(100).await;
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(copy).unwrap();
}

/// The crash took the store's write, but the routing decision was on disk.
#[tokio::test(flavor = "multi_thread")]
async fn a_recovered_handoff_puts_its_retained_message_in_the_store() {
    let (dir, opts) = on_disk("qos2-recovered-store");
    let (copy, copy_opts) = on_disk("qos2-recovered-store-copy");
    let publisher = id_on("store-pub", 0, 2);
    let subscriber = id_on("store-sub", 1, 2);
    let mut packet = publish_bytes(Version::V5, "store/t", b"once", QoS::ExactlyOnce, 5, &[]);
    packet[0] |= 1;
    {
        let (ctx, broker, uri) = setup(2, opts()).await;
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        sub.subscribe("store/t", SubscriptionOptions::from(QoS::ExactlyOnce))
            .await;
        let (mut p, _) = Raw::connect_with(&uri, &durable(&publisher)).await;
        let before = broker.shared().lane_logs[0].counts().0;
        let go = stop_destination(&ctx).await;
        p.send(&packet).await;
        source_written(&broker, before).await;
        copy_tree(&dir, &copy);
        drop(go);
        assert_eq!(p.next().await, ack(PacketType::PubRec));
    }
    for entry in std::fs::read_dir(&copy).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("retained-") {
            std::fs::remove_dir_all(entry.path()).unwrap();
        }
    }
    let (_ctx, _broker, uri) = setup(2, copy_opts()).await;
    let mut late = Raw::connect(&uri, "store-late", Version::V5, 0).await;
    late.subscribe("store/t", QoS::AtMostOnce.into()).await;
    assert_eq!(late.publish().await.payload, b"once");
    std::fs::remove_dir_all(dir).unwrap();
    std::fs::remove_dir_all(copy).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_negative_pubrec_releases_the_packet_id_after_restart() {
    let (dir, opts) = on_disk("qos2-negative-pubrec");
    {
        let (_ctx, broker, uri) = setup(1, opts()).await;
        broker.shared().retained.cursor_of("nack/t").fail(1, 1);
        let (mut p, _) = Raw::connect_with(&uri, &durable("nack-publisher")).await;
        let mut packet = publish_bytes(Version::V5, "nack/t", b"failed", QoS::ExactlyOnce, 5, &[]);
        packet[0] |= 1;
        p.send(&packet).await;
        assert_eq!(
            p.next().await,
            Got::Ack {
                kind: PacketType::PubRec,
                pkid: 5,
                reason: zzq_proto::reason::UNSPECIFIED_ERROR
            }
        );
    }
    let (_ctx, _broker, uri) = setup(1, opts()).await;
    let mut sub = Raw::connect(&uri, "nack-sub", Version::V5, 0).await;
    sub.subscribe("new/t", QoS::ExactlyOnce.into()).await;
    let (mut p, _) = Raw::connect_with(&uri, &durable("nack-publisher")).await;
    p.pub_("new/t", b"new", QoS::ExactlyOnce, 5).await;
    assert_eq!(p.next().await, ack(PacketType::PubRec));
    assert_eq!(sub.publish().await.payload, b"new");
    std::fs::remove_dir_all(dir).unwrap();
}
