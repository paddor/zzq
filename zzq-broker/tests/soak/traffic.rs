use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinSet;
use zzq_broker::BrokerOptions;
use zzq_client::{Client, QoS, Version};

use super::support::{Run, Server, next, payload, qos, within};

pub(super) async fn run(name: &str, scheme: &str, disk: bool) {
    let run = Arc::new(Run::new(name));
    let server = Server::new(&run, scheme, disk, BrokerOptions::default()).await;
    let mut workers = JoinSet::new();
    for n in 0..6 {
        let version = if n < 3 { Version::V311 } else { Version::V5 };
        let publisher = server.client(&format!("p{n}"), version, true).await;
        let a = server.client(&format!("a{n}"), version, true).await;
        let b = server.client(&format!("b{n}"), version, true).await;
        let topic = format!("load/{n}");
        within(a.subscribe(&topic, qos(n))).await.unwrap();
        within(b.subscribe(&topic, qos(n))).await.unwrap();
        workers.spawn(worker(run.clone(), publisher, [a, b], topic, qos(n)));
    }
    let mut cycles = 0;
    while let Some(result) = workers.join_next().await {
        cycles += result.unwrap();
    }
    server.close().await;
    Arc::try_unwrap(run).ok().unwrap().finish(cycles);
}

async fn worker(
    run: Arc<Run>,
    publisher: Client,
    mut subs: [Client; 2],
    topic: String,
    qos: QoS,
) -> u64 {
    let mut cycle = 0;
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    while run.running() || cycle < 2 {
        let size = [32, 1024, 65536][cycle as usize % 3];
        let count = if cycle % 6 < 2 { 4 } else { 32 };
        let data: Vec<_> = (0..count)
            .map(|i| payload(cycle * 32 + i, size, run.seed))
            .collect();
        let [a, b] = &mut subs;
        within(async {
            tokio::join!(
                async {
                    for p in &data {
                        publisher.publish(&topic, p, qos).await.unwrap();
                    }
                },
                receive(a, &topic, &data),
                receive(b, &topic, &data)
            );
        })
        .await;
        run.progress(count);
        if cycle % 6 < 2 {
            tick.tick().await;
        }
        cycle += 1;
    }
    within(publisher.disconnect()).await;
    for sub in subs {
        within(sub.disconnect()).await;
    }
    cycle
}

async fn receive(sub: &mut Client, topic: &str, data: &[Vec<u8>]) {
    for expected in data {
        let got = next(sub).await;
        assert_eq!(got.topic.as_str(), topic);
        super::support::same(got.payload.as_ref(), expected);
    }
}
