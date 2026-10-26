# Changelog

Notable changes to zzq. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `zzq-broker`: `Stats::dropped_session_ended` counts the QoS 1/2 messages
  a session still held, queued or unacked, when it ended.
  `$SYS/broker/messages/dropped` includes them.
- `zzq-broker`: `Authorizer::may_subscribe_shared` decides on shared
  subscriptions, given the share group. By default it applies
  `may_subscribe` to the filter after `$share/{group}/`.
- `zzq-broker`: `BrokerOptions::max_queued_bytes` limits the bytes queued
  per session, dropping the oldest messages like `max_queued`.
- `zzq-broker`: `BrokerOptions::max_state_bytes` bounds the memory of
  sessions, subscriptions, held QoS 1/2 messages and retained messages. A
  message that sessions of one lane share counts once. While it is reached,
  new sessions that outlive their connection are refused, messages that
  would queue are dropped and counted in `Stats::dropped_state_full`, and
  retained messages for new topics are not stored. `Stats::state_bytes` and
  `$SYS/broker/state/bytes` show the estimate, and
  `zzq_broker --max-state-mib` sets the limit.
- `zzq-broker`: `BrokerOptions::state_file` keeps sessions over a restart.
  `Broker::shutdown` closes every connection as lost, so wills go out, and
  writes the sessions with their subscriptions, queues, unacked messages
  and delayed wills, plus the retained messages unless `persistence` keeps
  them, compressed with Zstandard. `Broker::new` loads the file and
  renames it to `<name>.loaded`. `zzq_broker --state-file` writes it on
  SIGTERM or SIGINT.

### Changed

- `zzq-proto`: invalid topic filters fail with the new
  `DecodeError::InvalidFilter`, reason code "topic filter invalid", instead
  of `InvalidTopic`.
- `zzq-broker`: a share group gives each message to a member with a
  connection. It waits at an offline member only when no member is
  connected.
- `zzq-broker`: retained messages sent on subscribe no longer count
  against `max_queued`, which dropped them; `max_retained` bounds them.
- `zzq-broker`: `Broker::shutdown` returns a `Result`, for a state file it
  could not write.
- `zzq-broker`: `zzq_broker` shuts down on SIGTERM or SIGINT, telling v5
  clients "server shutting down", instead of dying with the signal.

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
- `zzq-broker`: `Authorizer::may_subscribe` got shared filters with their
  `$share/{group}/` prefix, so a rule on the topic filter did not apply.
- `zzq-broker`: with several IO lanes, a client that subscribed while
  another lane updated a retained message could keep the older value.
  Retained messages are now read once every lane has the subscription,
  and go out after SUBACK.

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
