//! A broker and its clients on one thread under a paused clock, as a
//! simulation runs them.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::time::Duration;

use zzq_broker::{Broker, BrokerOptions, Context};
use zzq_client::{Client, ClientOptions};
use zzq_proto::{QoS, Version};

/// Runs `test` on a current-thread runtime with a paused clock, inside a
/// local task set.
fn simulated<F: Future<Output = ()>>(test: impl FnOnce() -> F) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .start_paused(true)
        .build()
        .unwrap();
    tokio::task::LocalSet::new().block_on(&rt, test());
}

fn opts(id: &str) -> ClientOptions {
    ClientOptions {
        version: Version::V5,
        session_expiry: 3600,
        ..ClientOptions::new(id)
    }
}

#[test]
fn a_broker_and_its_clients_run_on_one_thread_under_virtual_time() {
    simulated(|| async {
        let ctx = Context::local(2);
        let broker = Broker::new(&ctx, BrokerOptions::default()).await.unwrap();
        broker.bind("inproc://sim").await.unwrap();
        let mut sub = Client::connect(&ctx, "inproc://sim", opts("sub"))
            .await
            .unwrap();
        sub.subscribe("t/#", QoS::AtLeastOnce).await.unwrap();
        let publisher = Client::connect(&ctx, "inproc://sim", opts("pub"))
            .await
            .unwrap();
        publisher
            .publish("t/1", "hello", QoS::AtLeastOnce)
            .await
            .unwrap();
        assert_eq!(sub.recv().await.unwrap().payload, "hello");
        // An hour passes at once; the session outlives its connection for
        // that long and no longer.
        sub.disconnect().await;
        tokio::time::sleep(Duration::from_secs(3599)).await;
        assert_eq!(broker.stats().await.unwrap().sessions, 2);
        tokio::time::sleep(Duration::from_secs(120)).await;
        assert_eq!(broker.stats().await.unwrap().sessions, 1);
    });
}
