# Changelog

Notable changes to zzq. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.1.0] - 2026-10-26

### Added

- `zzq-proto`: `no_std` codec for MQTT 3.1.1, 5.0 and MQTT-SN 1.2, a typed
  `ReasonCode`, topic validation and matching, and a topic trie.
- `zzq-tokio`: `Context` with IO lanes, and the `tcp://`, `tls://` with
  mutual TLS, `ws://`, `wss://`, `quic://`, `ipc://`, `inproc://` and
  `udp://` transports.
- `zzq-broker`: sessions, QoS 0/1/2, retained messages with file
  persistence, wills with delay, shared subscriptions, topic aliases, an
  MQTT-SN gateway, authentication and authorization hooks, `$SYS` topics,
  connection and resource limits, and `Broker::shutdown`.
- `zzq-client`: async and blocking client with reconnect, session resume,
  Receive Maximum and input checks before encoding.
- `zzq-bench`: load generator, local performance gates, and `zzq_chart`,
  which draws broker and client comparison charts from the result cache in
  `~/.cache/zzq`.
- `zzq-tokio`: `Config::spin`, how long a lane keeps polling after work
  before it sleeps, 40 µs by default. `zzq_broker --spin-us` sets it.
- `zzq-bench`: `zzq_chart` shows broker and client CPU under each panel,
  and `zzq_chart fixed` draws latency, CPU and memory of brokers at fixed
  load with many connections.
- `zzq-client`, `zzq-broker`: `publish` and `publish_retained` take any
  `AsRef<[u8]>` payload, so a borrowed or reused buffer needs no
  allocation.
- `zzq-client`: `Client::handle` returns a cloneable `Handle` that publishes
  and subscribes from other tasks while the client receives, and
  `Handle::enqueue` queues a publish and returns its ack as a separate
  `Acked` future.
- `zzq-client`: `Handle::disconnect` closes the connection while the client
  waits in `recv`, which then returns `None`.
- Ruby binding: the `zzq` gem in `bindings/ruby`, with the client and the
  broker.

### Changed

- Dual-licensed under ISC or Apache-2.0, at your option.
