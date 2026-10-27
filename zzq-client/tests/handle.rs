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

#[tokio::test]
async fn a_handle_disconnects_a_receiving_client() {
    let (ctx, _broker, uri) = setup("handle-close").await;
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("closing"))
        .await
        .unwrap();
    let h = c.handle();
    let receiving = tokio::spawn(async move { c.recv().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    timeout(WAIT, h.disconnect()).await.unwrap();
    assert!(timeout(WAIT, receiving).await.unwrap().unwrap().is_none());
    assert!(matches!(
        h.publish("t", "x", QoS::AtMostOnce).await,
        Err(zzq_client::Error::Closed)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_client_that_stops_receiving_keeps_its_connection() {
    let (ctx, broker, uri) = setup("handle-full").await;
    let opts = ClientOptions {
        keep_alive: 1,
        inbox: 10,
        reconnect: None,
        ..ClientOptions::new("slow")
    };
    let mut c = Client::connect(&ctx, &uri, opts).await.unwrap();
    c.subscribe("t", QoS::AtMostOnce).await.unwrap();
    for _ in 0..100 {
        broker.publish("t", "x", QoS::AtMostOnce).unwrap();
    }
    // Longer than the broker waits for a ping, without taking a message.
    tokio::time::sleep(Duration::from_secs(3)).await;
    for _ in 0..100 {
        let m = timeout(WAIT, c.recv()).await.unwrap().unwrap();
        assert_eq!(m.payload, "x");
    }
    // Still the same connection: without reconnect, a lost one ends the
    // client.
    let published = timeout(WAIT, c.publish("u", "y", QoS::AtLeastOnce)).await;
    assert!(matches!(published, Ok(Ok(()))), "{published:?}");
}
