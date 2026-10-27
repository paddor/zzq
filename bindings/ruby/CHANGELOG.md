# Changelog

## [Unreleased]

## v0.5.0 — 2026-10-27

- Built on the zzq 0.3 crates, with their fixes. The broker stops reading
  a client that does not read its replies, and a client reads the acks of
  its own requests while its inbox is full.
- A forked child shared its parent's wake pipes, so one could take the
  other's wake and leave a call waiting for good. A child now makes its own.
- Every error the extension raised leaked its message, about 100 bytes.

## v0.4.0 — 2026-10-27

- `Broker.new` takes `state_file:`: `#close` writes the sessions there, and
  the next broker started with it resumes them.
- `Broker#close` during a call on another thread, such as `#stats`,
  returned at once and left the broker to stop without a shutdown. It now
  waits for the call and shuts down.
- A call whose waiting thread was killed kept running until garbage
  collection. Killing the thread now cancels it.

## v0.3.0 — 2026-10-27

- Built on the zzq 0.2 crates, with their fixes and faster QoS 1/2
  fan-out and fan-in.
- `Broker.new` takes `max_queued_bytes:` and `max_state_bytes:`.
- `Broker#stats` adds `dropped_session_ended`, `dropped_state_full` and
  `state_bytes`.
- `Broker#close` raises `ClosedError` if a lane stopped before it could
  close its connections.

## v0.2.0 — 2026-10-26

- zzq is now a binding to the zzq Rust crates, built as a native
  extension with rb-sys. It needs Rust 1.93 to install and Ruby 3.4.
- Calls no longer need an Async task: they wait under any fiber
  scheduler, and release the GVL in plain threads.
- `Client#receive` and `Client#each` take messages from one inbox per
  client. `#subscribe` returns the granted QoS; `Subscription`,
  `Message#ack!`, message properties and `#monitor` are gone.
- New: reconnect with session resume, will delay, `#unsubscribe`,
  `#disconnect(reason:)`, and `IO::TimeoutError` from `receive(timeout:)`.
- `session_expiry_interval:` is now `session_expiry:` and `recv_hwm:` is
  `inbox:`. `linger:`, `send_hwm:`, `receive_maximum:`, `max_packet_size:`
  and `topic_alias_maximum:` are gone from the client.
- `ZZQ::Broker` gets `#publish`, `#stats`, the limits of zzq-broker and
  an MQTT-SN gateway on `udp://`. `persistence:` takes the path of a log
  file instead of a store object. `#ingest`, `#<<` and `#session_count`
  are gone.
- `ws://`, `wss://`, `quic://` and `inproc://` are built in; the
  zzq-websocket gem is no longer needed.
- Dual-licensed under ISC or Apache-2.0, at your option.

## v0.1.1 — 2026-04-20

- `Engine#handle_accepted` and `Engine#handle_connected` now return the
  `ConnectionLifecycle` they just built (previously `nil`). This lets
  frame-oriented out-of-tree transports such as
  [`zzq-websocket`](https://github.com/paddor/zzq-websocket) attach
  cancel-supervisors to the per-connection barrier so teardown
  propagates into request/response fiber trees cleanly. Additive and
  backwards-compatible — the existing `Engine#bind` call-site still
  discards the return value.

## v0.1.0 — 2026-04-20

- First release. Async-native MQTT 3.1.1 + 5.0 client and broker in
  one gem, built on `async`, `io-stream`, and `protocol-mqtt`.
- Transports: `mqtt://` (plain TCP, port 1883), `mqtts://` (TLS,
  port 8883), `ipc://` (Unix-domain sockets, filesystem and Linux
  abstract namespace).
- `mqtts://` accepts any `OpenSSL::SSL::SSLContext` and drops the
  connection at 128 GiB of application data so reconnects force
  fresh keys before AEAD nonce counters approach their unsafe
  range.
- Retained-message persistence: in-memory (default) or stdlib
  `PStore` with `ultra_safe = true` for fsync-on-commit durability.
- Broker-to-broker bridging via `Client` + `Broker` composition;
  MQTT v5 `no_local` loop guard supported.
