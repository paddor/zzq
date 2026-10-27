# Changelog

Notable changes to zzq. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [0.2.0] - 2026-10-27

### Added

- `zzq-broker`: `Dropped::session_ended` counts the QoS 1/2 messages
  a session still held, queued or unacked, when it ended.
  `$SYS/broker/messages/dropped` includes them.
- `zzq-broker`: `Authorizer::may_subscribe_shared` decides on shared
  subscriptions, given the share group. By default it applies
  `may_subscribe` to the filter after `$share/{group}/`.
- `zzq-broker`: `BrokerOptions::max_queued_bytes` limits the bytes queued
  per session, dropping the oldest messages like `max_queued`.
- `zzq-broker`: `BrokerOptions::max_state_bytes` bounds the memory of
  sessions, subscriptions, held QoS 1/2 messages and retained messages. A
  message that sessions of one lane share counts once, and so does a
  retained message sessions got on subscribe. While it is reached, new
  sessions that outlive their connection are refused, messages that would
  queue are dropped and counted in `Dropped::state_full`, and retained
  messages for new topics are not stored. `Stats::state_bytes` and
  `$SYS/broker/state/bytes` show the estimate, and
  `zzq_broker --max-state-mib` sets the limit.
- `zzq-broker`: `BrokerOptions::state_file` keeps sessions over a restart.
  `Broker::shutdown` closes every connection as lost, so wills go out, and
  writes the sessions with their subscriptions, queues, unacked messages
  and delayed wills, plus the retained messages unless `persistence` keeps
  them, compressed with Zstandard. `Broker::new` loads the file and
  renames it to `<name>.loaded`. `zzq_broker --state-file` writes it on
  SIGTERM or SIGINT.
- `zzq-bench`: `zzq_ramprobe` measures the memory of an offline session, a
  queued and a fanned-out message and a retained message, and
  `zzq_perf_verify` gates state memory along with throughput and latency.

### Changed

- `zzq-proto`: invalid topic filters fail with the new
  `DecodeError::InvalidFilter`, reason code "topic filter invalid", instead
  of `InvalidTopic`.
- `zzq-broker`: a share group gives each message to a member with a
  connection. It waits at an offline member only when no member is
  connected.
- `zzq-broker`: retained messages sent on subscribe may queue beyond
  `max_queued` and `max_queued_bytes` by what that SUBSCRIBE sends, so the
  limits no longer drop them, and repeated SUBSCRIBEs cannot grow the queue
  without bound.
- `zzq-broker`: `Broker::shutdown` returns a `Result`, for a state file it
  could not write.
- `zzq-broker`: `zzq_broker` shuts down on SIGTERM or SIGINT, telling v5
  clients "server shutting down", instead of dying with the signal.
- `zzq-broker`: the `dropped_*` counters of `Stats` moved to
  `Stats::dropped`, a `Dropped` with `total()`, and the `sn_*` fields of
  `BrokerOptions` to `BrokerOptions::sn`, an `SnOptions`.
- `zzq-proto`: MQTT-SN topics are typed by message: PUBLISH takes a
  `Topic` (ID, predefined or short), SUBSCRIBE and UNSUBSCRIBE a `Filter`
  (name, predefined or short), and WILLTOPIC and WILLTOPICUPD carry
  `will: Option<(Flags, &str)>`. Forms that encoded to bytes decoding as
  another message no longer exist.
- `zzq-proto`: `SubscriptionOptions::retain_handling` is a `RetainHandling`,
  re-exported by `zzq-client`, instead of a `u8` whose 3 encoded the reserved
  value and whose 4 and above silently became 0.
- `zzq-broker`: a PUBACK, PUBREC or PUBCOMP finds its delivery by packet
  ID instead of scanning the inflight window, which made QoS 2 slow with many
  messages in flight. Fan-in at QoS 2 is about 40% faster.
- `zzq-broker`: sessions of a lane that get the same QoS 1/2 message share
  one handle to it, so a delivery takes no atomic refcount update. Fan-out
  of small messages is about 10% faster at QoS 1 and 20% at QoS 2.

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
- `zzq-broker`: a delayed will that a reconnect cancelled kept a timer task,
  holding the lane, until its delay ran out. The lane sweep now publishes
  due wills, without a task per will.
- `zzq-broker`: while closing connections for a state file, sessions no
  longer expire, so the file keeps them with their delayed wills instead of
  sending a will that the next start sends again. A connection that reached a
  lane after it shut down is refused with "server unavailable".
- `zzq-broker`: a connection closed while its peer had stopped reading kept
  its socket and up to `max_out_bytes` of output forever, past
  `max_connections`. Its writer now gets 2 s to finish, then stops.
- `zzq-broker`: a SUBSCRIBE or UNSUBSCRIBE whose change a stopping lane never
  applied waited for its ack forever, holding the lane.
- `zzq-broker`: a SUBACK or UNSUBACK held for other lanes could reach the
  connection that took the session over, and one connection could wait for
  another's held acks. A publisher that reconnected while its PUBACK was
  held for a congested subscriber got the ack twice. Held acks now go only
  to the connection they answer.
- `zzq-broker`: the acks held for a publisher while a subscriber was congested
  had no limit, so a v3.1.1 client flooding QoS 1 grew them without bound. A
  publisher now gets at most `receive_maximum` held acks: a v5 one past it
  breaks the protocol and is disconnected, a v3.1.1 one gets the rest at once.
- `zzq-broker`: a shared subscription member on another lane that held a
  message long, offline or behind many unacked ones, kept the publisher's
  whole read buffer for it. It now gets a copy, like members on the
  publisher's lane.
- `zzq-broker`: a lane handled all queued controls, such as broker publishes,
  before its connections ran again, and MQTT-SN QoS -1 publishes, which need
  no connection, queued without a limit. A lane now lets its connections
  run every 64 controls, and drops QoS -1 publishes while 4096 controls wait.
- `zzq-broker`: an unacked message too large for the Maximum Packet Size a
  client gave on reconnect kept its inflight slot for the life of the session.
  Queued messages dropped for that reason were not counted, and queued
  messages sent once the window had room were missing from
  `Stats::deliveries` and `$SYS/broker/messages/sent`.
- `zzq-broker`: with several IO lanes, messages that `Broker::publish`,
  `publish_retained` or MQTT-SN QoS -1 sent to one topic could reach
  subscribers out of order, and the retained store could keep an older one.
  They now go through the lane of their topic.
- `zzq-broker`: a zero `sys_interval` or `sn.retry` made the `$SYS` or
  MQTT-SN client task panic, and a zero `receive_maximum` refused every v5
  client. `Broker::new` now refuses them with the new
  `Error::InvalidOption`.
- `zzq-broker`: `AuthDecision::Deny` with a code below 0x80, such as
  `ReasonCode::SUCCESS`, sent a CONNACK that accepted the client and then
  closed. It now refuses with "not authorized".
- `zzq-broker`, `zzq-client`: a packet header alone made a connection
  allocate the whole packet it announced, up to `max_packet_size` in the
  broker and 256 MiB in the client. A read buffer now grows to at most
  twice what arrived.
- `zzq-broker`: with a disk slower than retained publishes,
  `FilePersistence` queued every change in memory. A topic now waits for the
  writer with its latest change only. Compaction writes `<log>.tmp`, so a
  log `zzq.log` and a state file `zzq` no longer share `zzq.tmp`.
- `zzq-broker`: retained messages and subscriptions loaded from the
  persistence or the state file skipped the `max_topic_levels` check, though
  the store and the index walk topics a level at a time. Deeper ones are now
  left out.
- `zzq-broker`: the match cache of a lane was bounded by topics only, so
  topics with many matching subscriptions could fill gigabytes outside the
  state budget, and idle publishers kept matches the index had replaced. The
  cache now holds at most 262144 entries, and the sweep drops stale matches.
- `zzq-broker`: every new subscription scanned all subscriptions to its
  filter on every lane, so k sessions subscribing to one filter took O(k²).
  Only a subscription that replaces one scans now.
- `zzq-broker`: a client whose packets the broker did not read, waiting for
  room to forward them or for other lanes, could be disconnected for keep
  alive. Keep alive now pauses during such waits.
- `zzq-broker`: an MQTT-SN CONNECT with a duration of 0, which anyone can
  send over UDP, made a client that never ended. Such clients, and those
  asking for more, now get `SnOptions::max_keep_alive`, 3600 s by default.
- `zzq-broker`: the MQTT-SN gateway held every broker publish for a topic
  whose REGISTER the client had not acked, without a limit, and released them
  under the refused topic ID when the client rejected the REGISTER. At most
  256 now wait per topic, and a rejected REGISTER drops them and forgets the
  ID.
- `zzq-client`: QoS 1/2 publishes beyond the broker's Receive Maximum
  waited in the session without a limit, so `Handle::enqueue` callers that
  did not await the acks were never held back. Publishing now waits once
  1 MiB of them waits.
- `zzq-client`: with a full inbox, the session waited for room and did
  nothing else, so an application that paused `recv` for longer than 1.5
  keep alive intervals lost its connection, and requests and acks waited
  too. It now stops reading only, keeps pinging, and catches up once the
  application takes messages.
- `zzq-client`: after a reconnect that the broker answered without the
  session, QoS 1/2 messages received before were still acked by packet ID
  on the new connection, where the ID may mean another message.
- `zzq-client`: a QoS 2 publish that got PUBREC before the connection
  dropped resent its PUBREL to a broker that lost the session, and failed
  with "packet identifier not found". It now completes.
- `zzq-client`: `disconnect` could wait forever on a broker that stopped
  reading, as could the session after its writer failed to flush. A
  DISCONNECT now goes through a full session and gets 2 s to be written, and
  a failed writer ends the connection.
- `zzq-tokio`: binding `ipc://` removed whatever was at the path when a
  connect to it failed, a regular file or a socket that refused for other
  reasons. Only a socket nobody listens on is replaced now.
- `zzq-tokio`: WebSocket control frames could be up to 16 MiB and
  fragmented, and pongs queued without a limit, so a peer that pinged and did
  not read grew memory without bound. Control frames over 125 bytes or
  without FIN now close the connection, and one pong waits at most. Once
  either side closes, the pump stops within 2 s instead of reading on.
- `zzq-tokio`: a lane whose runtime could not be built stopped silently,
  and `Context::with_config` returned a context whose calls then failed with
  `Error::Closed`. It now fails with `Error::Io`, as documented.
- `zzq-proto`, `zzq-client`: the encoder set DUP on QoS 0 publishes, and a
  3.1.1 client sent a password without a user name, both of which brokers
  refuse. DUP now goes with QoS 1 and 2 only, and such a client fails to
  connect with the new `Error::InvalidOption`.
- `zzq-proto`: variable byte integers longer than their value needs, a
  CONNACK with bytes after its fields, a SUBACK without a code and a 3.1.1
  UNSUBACK with a payload decoded; they are malformed or a protocol error now.
- `zzq-broker`: `zzq_broker --cert` without `--key`, or the other way
  around, was silently ignored, and so was `--client-ca` alone. Such
  arguments are now an error.

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
