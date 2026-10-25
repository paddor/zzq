//! A handle publishes and subscribes while its client receives.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use std::time::Duration;

use tokio::time::timeout;
use zzq_broker::{Broker, BrokerOptions};
use zzq_client::{Client, ClientOptions, Context, QoS};

const WAIT: Duration = Duration::from_secs(5);

async fn setup(name: &str) -> (Context, Broker, String) {
    let ctx = Context::new().unwrap();
    let broker = Broker::new(&ctx, BrokerOptions::default()).await.unwrap();
    let uri = format!("inproc://{name}");
    broker.bind(&uri).await.unwrap();
    (ctx, broker, uri)
}

#[tokio::test]
async fn a_handle_publishes_while_the_client_waits_to_receive() {
    let (ctx, _broker, uri) = setup("handle-recv").await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("echo"))
        .await
        .unwrap();
    let h = c.handle();
    h.subscribe("t", QoS::AtLeastOnce).await.unwrap();
    let receiving = tokio::spawn(async move { c.recv().await });
    // The client waits before anything is published.
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.publish("t", "x", QoS::AtLeastOnce).await.unwrap();
    let m = timeout(WAIT, receiving).await.unwrap().unwrap().unwrap();
    assert_eq!(m.payload, "x");
}

#[tokio::test]
async fn a_handle_keeps_publishing_once_the_client_is_dropped() {
    let (ctx, _broker, uri) = setup("handle-alone").await;
    let mut reader = Client::connect(&ctx, &uri, ClientOptions::new("reader"))
        .await
        .unwrap();
    reader.subscribe("t", QoS::AtLeastOnce).await.unwrap();
    let writer = Client::connect(&ctx, &uri, ClientOptions::new("writer"))
        .await
        .unwrap();
    let h = writer.handle();
    drop(writer);
    h.publish("t", "still here", QoS::AtLeastOnce)
        .await
        .unwrap();
    let m = timeout(WAIT, reader.recv()).await.unwrap().unwrap();
    assert_eq!(m.payload, "still here");
}
