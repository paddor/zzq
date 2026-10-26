# Changelog

Notable changes to zzq. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `zzq-broker`: `Stats::dropped_session_ended` counts the QoS 1/2 messages
  a session still held, queued or unacked, when it ended.
  `$SYS/broker/messages/dropped` includes them.

### Changed

- `zzq-proto`: invalid topic filters fail with the new
  `DecodeError::InvalidFilter`, reason code "topic filter invalid", instead
  of `InvalidTopic`.

### Fixed

- `zzq-proto`: a property the packet type does not allow is malformed. A
  repeated property, an out-of-range value, a Response Topic with wildcards
  and Authentication Data without a method are protocol errors.
- `zzq-proto`: a 3.1.1 CONNECT with a password but no user name, and No
  Local on a shared subscription, are refused. An empty topic name without
  a topic alias is a protocol error.
- `zzq-broker`: a CONNECT with an Authentication Method is refused with
  "bad authentication method", and a v5 CONNECT that does not decode gets
  a CONNACK with the reason. A first packet other than CONNECT is refused
  from its first byte, without waiting for its body.
- `zzq-broker`: an ack of the wrong type for a delivery is a protocol
  error. A PUBACK for a QoS 2 delivery dropped the delivery uncounted.
- `zzq-broker`: a resumed session resends no more messages than the
  client's new Receive Maximum allows.
- `zzq-broker`: a connection taken over before its CONNACK closes without
  a DISCONNECT, which may only follow CONNACK.

## [0.1.1] - 2026-10-26

### Fixed

- `zzq-broker`: `Stats::subscriptions` no longer counts the subscriptions
  of sessions that ended.

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
