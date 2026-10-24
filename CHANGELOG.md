# Changelog

Notable changes to zzq. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

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
