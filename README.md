# zzq

MQTT 3.1.1 and 5.0 broker and client in Rust, with an MQTT-SN 1.2 gateway.
Connections run on IO lanes: threads with their own Tokio runtime, so a
connection never moves between threads.

## Crates

| Crate | Purpose |
| --- | --- |
| `zzq-proto` | `no_std` codec for MQTT 3.1.1, 5.0 and MQTT-SN, topic rules and a topic trie |
| `zzq-tokio` | `Context`, IO lanes and transports |
| `zzq-broker` | `Broker`: sessions, retained messages, wills, shared subscriptions, MQTT-SN gateway, auth hooks |
| `zzq-client` | `Client`, async and blocking, with reconnect and session resume |
| `zzq-bench` | Load generator and local performance gates |

## Example

```rust
use zzq_broker::{Broker, BrokerOptions, Context};
use zzq_client::{Client, ClientOptions, QoS};

let ctx = Context::new()?;
let broker = Broker::new(&ctx, BrokerOptions::default()).await?;
broker.bind("tcp://0.0.0.0:1883").await?;

let mut client = Client::connect(&ctx, "tcp://localhost:1883", ClientOptions::new("reader")).await?;
client.subscribe("sensors/#", QoS::AtLeastOnce).await?;
client.publish("sensors/temp", "21.5", QoS::AtLeastOnce).await?;
let message = client.recv().await;
```

## Transports

`tcp://`, `tls://`, `ws://`, `wss://`, `quic://`, `ipc://`, `inproc://`,
and `udp://` for MQTT-SN. TLS and QUIC are behind the default `tls` and
`quic` features.

## Requirements

Rust 1.93 or newer.

## License

ISC
