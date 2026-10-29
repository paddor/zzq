# zzq

An MQTT 3.1.1 and 5.0 broker and client in Rust, with an MQTT-SN 1.2
gateway.

The broker listens and the client connects on `tcp://`, `tls://`,
`ws://`, `wss://`, `quic://`, `ipc://` and, within a process,
`inproc://`. MQTT-SN runs over `udp://`. TLS and QUIC are behind the
default `tls` and `quic` features.

## Why zzq

- **Correct under load.** A slow subscriber slows its publishers, no
  connection starves, and the broker avoids acking messages it would
  later drop.
- **Durable.** With a data directory, sessions and messages survive
  restarts and crashes. An ack goes out once its message is on disk.
  A cluster of three keeps it all in RAM on two nodes instead.
- **Fast.** A connection stays on its IO lane, and messages pass without
  locks, copies or allocations.
- **Flat memory.** Overload slows publishers instead of filling RAM.
- **Embeddable.** Run `zzq_broker`, or embed the `Broker` in an
  application. The `Client` is async or blocking.
- **Fuzzed.** AFL targets cover every input that reads bytes: packets,
  MQTT-SN, WebSocket, journals, state files, cluster ops and topics.

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

## Workspace

Six Cargo workspace crates. `zzq-tokio`, `zzq-client` and `zzq-bench`
forbid unsafe Rust. `zzq-proto` and `zzq-broker` each skip one UTF-8
check on bytes known to be valid, and `zzq-disk` keeps its Linux AIO calls
in one module.

| Crate | What it does |
| --- | --- |
| [`zzq-proto`](zzq-proto/) | Sans-I/O `no_std` codec for MQTT 3.1.1, 5.0 and MQTT-SN, topic rules and a topic trie |
| [`zzq-tokio`](zzq-tokio/) | `Context`, IO lanes and transports |
| [`zzq-disk`](zzq-disk/) | Durable append-only journals, written with Linux AIO |
| [`zzq-broker`](zzq-broker/) | `Broker`: sessions, retained messages, wills, shared subscriptions, MQTT-SN gateway, auth hooks |
| [`zzq-client`](zzq-client/) | `Client`, async and blocking, with reconnect and session resume |
| [`zzq-bench`](zzq-bench/) | Load generators, the benchmark suite, perf gates and charts |

Language bindings:

| Language | Binding |
| --- | --- |
| [Ruby](bindings/ruby/README.md) | The `zzq` gem: broker and client |

## Durability

By default the broker keeps everything in memory. Give it a data
directory (`zzq_broker --data-dir`, Linux only) and it also keeps retained
messages and persistent sessions on disk. An ack then goes out only once
its message is written, so a crash loses nothing that was acked. When the
disk runs low, the broker stops keeping new messages instead of failing.

## Clustering

Three `zzq_broker` nodes can run as one broker (`--cluster-node`,
`--cluster-peers`). The leader serves the clients and acks a message once
another node holds it in RAM. When the leader goes, another node takes
over with all that was acked, and the clients reconnect to it. Only the
leader listens, so a load balancer's TCP health check finds it. A clean
stop writes each node's state to its `--state-file`, so the whole cluster
can stop and come back.

## Further reading

- [doc/architecture.md](doc/architecture.md): the threads, the queues
  between them and the path of a message.
- [BENCHMARKS.md](BENCHMARKS.md): throughput, latency and memory next to
  twelve other brokers, with storage off and on.
- [COMPARISONS.md](COMPARISONS.md): how those brokers handle overload,
  what their storage keeps, and what a crash loses.

## Requirements

Rust 1.93 or newer.

## License

Licensed under either of [ISC](LICENSE-ISC) or
[Apache-2.0](LICENSE-APACHE), at your option.
