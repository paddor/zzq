use std::time::Duration;

use zzq_broker::BrokerOptions;
use zzq_client::{Client, ClientOptions, LastWill, QoS, Version};

use super::support::{Run, Server, next, payload, within};

pub(super) async fn churn(name: &str, disk: bool) {
    let run = Run::new(name);
    let server = Server::new(&run, "tcp", disk, BrokerOptions::default()).await;
    let publisher = server.client("publisher", Version::V5, false).await;
    let mut cycles = 0u64;
    while run.running() || cycles < 2 {
        let version = if cycles.is_multiple_of(2) {
            Version::V311
        } else {
            Version::V5
        };
        let id = format!("churn-{}", cycles % 32);
        let sub = server.client(&id, version, true).await;
        within(sub.subscribe("churn/#", QoS::ExactlyOnce))
            .await
            .unwrap();
        within(sub.disconnect()).await;
        let expected = payload(cycles, 1024, run.seed);
        within(publisher.publish("churn/value", &expected, QoS::ExactlyOnce))
            .await
            .unwrap();
        let mut sub = server.client(&id, version, true).await;
        assert!(sub.connected().session_present);
        super::support::same(next(&mut sub).await.payload.as_ref(), &expected);
        within(sub.unsubscribe(&["churn/#"])).await.unwrap();
        let mut old = sub;
        let replacement = server.client(&id, version, false).await;
        assert!(
            within(old.recv()).await.is_none(),
            "takeover closes old connection"
        );
        within(replacement.disconnect()).await;
        retained(&server, &publisher, &expected).await;
        will(&server, cycles).await;
        canceled_waiter(&server, &publisher).await;
        if cycles.is_multiple_of(16) {
            expire(&server).await;
        }
        run.progress(1);
        cycles += 1;
    }
    within(publisher.disconnect()).await;
    server.close().await;
    run.finish(cycles);
}

async fn retained(server: &Server, publisher: &Client, expected: &[u8]) {
    within(publisher.publish_retained("retain/x", "old", QoS::AtLeastOnce))
        .await
        .unwrap();
    within(publisher.publish_retained("retain/x", expected, QoS::AtLeastOnce))
        .await
        .unwrap();
    let mut sub = server.client("retained", Version::V5, false).await;
    within(sub.subscribe("retain/#", QoS::AtLeastOnce))
        .await
        .unwrap();
    let got = next(&mut sub).await;
    assert!(got.retain);
    super::support::same(got.payload.as_ref(), expected);
    within(sub.disconnect()).await;
    within(publisher.publish_retained("retain/x", "", QoS::AtLeastOnce))
        .await
        .unwrap();
    let mut sub = server.client("retained", Version::V5, false).await;
    within(sub.subscribe("retain/#", QoS::AtLeastOnce))
        .await
        .unwrap();
    within(publisher.publish("retain/barrier", "done", QoS::AtLeastOnce))
        .await
        .unwrap();
    assert_eq!(next(&mut sub).await.topic, "retain/barrier");
    within(sub.disconnect()).await;
}

async fn will(server: &Server, cycle: u64) {
    let mut watcher = server.client("watcher", Version::V5, false).await;
    within(watcher.subscribe("will", QoS::AtLeastOnce))
        .await
        .unwrap();
    let client = within(Client::connect(
        &server.ctx,
        &server.uri,
        ClientOptions {
            will: Some(LastWill {
                topic: "will".into(),
                payload: cycle.to_le_bytes().to_vec().into(),
                qos: QoS::AtLeastOnce,
                retain: false,
                delay: 0,
            }),
            ..ClientOptions::new("departing")
        },
    ))
    .await
    .unwrap();
    drop(client);
    assert_eq!(
        next(&mut watcher).await.payload.as_ref(),
        cycle.to_le_bytes()
    );
    within(watcher.disconnect()).await;
}

async fn canceled_waiter(server: &Server, publisher: &Client) {
    let mut sub = server.client("canceled", Version::V5, false).await;
    within(sub.subscribe("canceled", QoS::ExactlyOnce))
        .await
        .unwrap();
    let ack = within(
        publisher
            .handle()
            .enqueue("canceled", "kept", QoS::ExactlyOnce, false),
    )
    .await
    .unwrap();
    drop(ack);
    assert_eq!(next(&mut sub).await.payload, "kept");
    within(sub.disconnect()).await;
}

async fn expire(server: &Server) {
    let c = within(Client::connect(
        &server.ctx,
        &server.uri,
        ClientOptions {
            clean_start: false,
            session_expiry: 1,
            ..ClientOptions::new("expiring")
        },
    ))
    .await
    .unwrap();
    within(c.subscribe("expiry", QoS::AtLeastOnce))
        .await
        .unwrap();
    within(c.disconnect()).await;
    within(async {
        let mut tick = tokio::time::interval(Duration::from_millis(50));
        loop {
            let stats = server.broker.stats().await.unwrap();
            if stats.subscriptions == 0 {
                break;
            }
            tick.tick().await;
        }
    })
    .await;
    let c = server.client("expiring", Version::V5, true).await;
    assert!(
        !c.connected().session_present,
        "expired session was removed"
    );
    within(c.disconnect()).await;
}

pub(super) async fn backpressure(name: &str, disk: bool) {
    let run = Run::new(name);
    let opts = BrokerOptions {
        qos_queue_high: 32,
        max_inflight: 8,
        max_out_bytes: 64 << 10,
        ..BrokerOptions::default()
    };
    let server = Server::new(&run, "tcp", disk, opts).await;
    let mut cycles = 0;
    while run.running() || cycles < 2 {
        pressure_cycle(&server, &run, cycles).await;
        run.progress(1);
        cycles += 1;
    }
    server.close().await;
    run.finish(cycles);
}

async fn pressure_cycle(server: &Server, run: &Run, cycle: u64) {
    let mut slow = within(Client::connect(
        &server.ctx,
        &server.uri,
        ClientOptions {
            inbox: 1,
            clean_start: true,
            session_expiry: 7200,
            ..ClientOptions::new("slow")
        },
    ))
    .await
    .unwrap();
    within(slow.subscribe("slow", QoS::AtLeastOnce))
        .await
        .unwrap();
    let publisher = server.client("pressure", Version::V5, false).await;
    let handle = publisher.handle();
    let seed = run.seed;
    let base = cycle * 144;
    let mut sending = tokio::spawn(async move {
        for seq in base..base + 128 {
            within(handle.publish("slow", payload(seq, 65536, seed), QoS::AtLeastOnce))
                .await
                .unwrap();
        }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(100), &mut sending)
            .await
            .is_err(),
        "slow subscriber did not apply backpressure"
    );
    independent(server).await;
    drain(&mut slow, base, base + 128, seed).await;
    within(sending).await.unwrap();
    within(slow.disconnect()).await;
    for seq in base + 128..base + 144 {
        within(publisher.publish("slow", payload(seq, 65536, seed), QoS::AtLeastOnce))
            .await
            .unwrap();
    }
    let mut slow = server.client("slow", Version::V5, true).await;
    drain(&mut slow, base + 128, base + 144, seed).await;
    within(slow.disconnect()).await;
    within(publisher.disconnect()).await;
}

async fn independent(server: &Server) {
    let mut fast = server.client("fast", Version::V5, false).await;
    within(fast.subscribe("fast", QoS::AtLeastOnce))
        .await
        .unwrap();
    let publisher = server.client("fast-pub", Version::V5, false).await;
    within(publisher.publish("fast", "alive", QoS::AtLeastOnce))
        .await
        .unwrap();
    assert_eq!(next(&mut fast).await.payload, "alive");
    within(fast.disconnect()).await;
    within(publisher.disconnect()).await;
}

async fn drain(sub: &mut Client, start: u64, end: u64, seed: u64) {
    within(async {
        let mut expected = start;
        while expected < end {
            let got = next(sub).await;
            let actual = super::support::sequence(&got.payload);
            assert!(
                got.payload.as_ref() == payload(actual, 65536, seed),
                "corrupt payload seq={actual}"
            );
            assert!(
                actual <= expected,
                "lost message: wanted={expected} got={actual}"
            );
            if actual == expected {
                expected += 1;
            }
        }
    })
    .await;
}
