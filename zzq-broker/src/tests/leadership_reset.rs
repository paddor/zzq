//! References held by old connections must stay stale after leadership reset.

use zzq_proto::{QoS, SubscriptionOptions, Version};

use super::support::Raw;
use super::{BrokerOptions, Lane, on_lane, setup};

#[tokio::test(flavor = "multi_thread")]
async fn a_lane_reset_releases_its_connection_limit() {
    let opts = BrokerOptions {
        max_connections: 1,
        ..BrokerOptions::default()
    };
    let (ctx, broker, uri) = setup(1, opts).await;
    let _old = Raw::connect(&uri, "old", Version::V5, 0).await;
    on_lane(&ctx, &broker, 0, Lane::reset).await;
    let mut new = Raw::connect(&uri, "new", Version::V5, 0).await;
    new.subscribe("reset/t", SubscriptionOptions::from(QoS::AtMostOnce))
        .await;
    broker
        .publish("reset/t", "after reset", QoS::AtMostOnce)
        .unwrap();
    assert_eq!(new.publish().await.payload, b"after reset");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_late_detach_after_reset_cannot_remove_a_new_connection() {
    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let _old = Raw::connect(&uri, "old", Version::V5, 0).await;
    let stale = on_lane(&ctx, &broker, 0, |lane| {
        let key = lane.sessions.keys().next().unwrap();
        let id = lane.sessions.get(key).unwrap().conn.as_ref().unwrap().id;
        lane.reset();
        (key, id)
    })
    .await;
    let mut new = Raw::connect(&uri, "new", Version::V5, 0).await;
    new.subscribe("reset/t", SubscriptionOptions::from(QoS::AtMostOnce))
        .await;
    // The old task may finish after promotion and a new CONNECT.
    on_lane(&ctx, &broker, 0, move |lane| {
        lane.detach(stale.0, stale.1, crate::lane::Close::Lost);
    })
    .await;
    broker
        .publish("reset/t", "new connection", QoS::AtMostOnce)
        .unwrap();
    assert_eq!(new.publish().await.payload, b"new connection");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_forwarded_publish_from_before_reset_cannot_reach_new_sessions() {
    let (ctx, broker, uri) = setup(2, BrokerOptions::default()).await;
    let id = super::id_on("epoch-sub", 1, 2);
    let mut old = Raw::connect(&uri, &id, Version::V5, 0).await;
    old.subscribe("epoch/t", QoS::AtMostOnce.into()).await;
    let stale = on_lane(&ctx, &broker, 0, |l| {
        l.publish_owned(&crate::msg::Msg::new(
            "epoch/t",
            b"old",
            &[],
            QoS::AtMostOnce,
            false,
        ));
        l.next_remote().unwrap().1
    })
    .await;
    on_lane(&ctx, &broker, 0, Lane::reset).await;
    on_lane(&ctx, &broker, 1, Lane::reset).await;
    let mut new = Raw::connect(&uri, &id, Version::V5, 0).await;
    new.subscribe("epoch/t", QoS::AtMostOnce.into()).await;
    on_lane(&ctx, &broker, 1, move |l| {
        for message in stale {
            l.deliver_remote(message);
        }
        l.flush();
    })
    .await;
    new.quiet(100).await;
    broker.publish("epoch/t", "new", QoS::AtMostOnce).unwrap();
    assert_eq!(new.publish().await.payload, b"new");
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "multi_thread")]
async fn a_reset_pair_waits_for_its_new_delivery_to_be_journaled() {
    use zzq_proto::PacketType;

    use super::support::Got;
    use super::{durable, id_on, on_disk};

    let (dir, opts) = on_disk("reset-pairs");
    let (ctx, broker, uri) = setup(2, opts()).await;
    let publisher = id_on("pair-p", 0, 2);
    let subscriber = id_on("pair-s", 1, 2);
    let ack = Got::Ack {
        kind: PacketType::PubAck,
        pkid: 1,
        reason: 0,
    };
    {
        let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
        sub.subscribe("pair/t", SubscriptionOptions::from(QoS::AtLeastOnce))
            .await;
        let mut p = Raw::connect(&uri, &publisher, Version::V5, 0).await;
        p.pub_("pair/t", b"before", QoS::AtLeastOnce, 1).await;
        assert_eq!(p.next().await, ack);
        let message = sub.publish().await;
        sub.complete(&message).await;
    }
    // A batch dropped by reset leaves a gap in the absolute sequence.
    on_lane(&ctx, &broker, 0, |l| {
        l.publish_owned(&crate::msg::Msg::new(
            "pair/t",
            b"discarded",
            &[],
            QoS::AtLeastOnce,
            false,
        ));
        assert!(l.next_remote().is_some());
    })
    .await;
    on_lane(&ctx, &broker, 0, Lane::reset).await;
    on_lane(&ctx, &broker, 1, Lane::reset).await;
    let (mut sub, _) = Raw::connect_with(&uri, &durable(&subscriber)).await;
    sub.subscribe("pair/t", SubscriptionOptions::from(QoS::AtLeastOnce))
        .await;
    let mut p = Raw::connect(&uri, &publisher, Version::V5, 0).await;
    let go = super::crash_cut::stop_destination(&ctx).await;
    p.pub_("pair/t", b"after", QoS::AtLeastOnce, 1).await;
    p.quiet(100).await;
    drop(go);
    assert_eq!(p.next().await, ack);
    assert_eq!(sub.publish().await.payload, b"after");
    broker.shutdown().await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_control_from_before_reset_is_ignored() {
    use crate::index::{Entry, SubIds, SubRef};
    use crate::lane::Control;

    let (ctx, broker, uri) = setup(1, BrokerOptions::default()).await;
    let _old = Raw::connect(&uri, "old-control", Version::V5, 0).await;
    on_lane(&ctx, &broker, 0, |l| {
        let key = l.sessions.keys().next().unwrap();
        let entry = Entry {
            sub: SubRef {
                lane: 0,
                epoch: 0,
                key,
            },
            client: "old-control".into(),
            birth: l.sessions.get(key).unwrap().birth,
            opts: QoS::AtMostOnce.into(),
            sub_id: SubIds::default(),
            share: None,
            journaled: false,
        };
        l.reset();
        l.control(Control::SubAdd {
            filter: "stale/t".into(),
            entry,
            replace: false,
            ticket: None,
        });
        assert_eq!(l.stats.subscriptions, 0);
    })
    .await;
}
