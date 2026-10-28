# Comparisons

How the brokers in [BENCHMARKS.md](BENCHMARKS.md) compare:

- **Under load:** what they do with messages they cannot deliver, where
  the memory goes, and which defaults decide a benchmark. Measured on
  one machine with tuned configs: zzq on 2026-10-27, the rest on
  2026-10-26.
- **Durability and clustering:** from each release's source, docs and
  issues. Not tested here.

Source links point to the release that ran.

## Summary

| Broker | Fan-in loss, QoS 0 / 1 / 2 | Peak RSS | Stalls a publisher | Fixed load fails at |
| --- | --- | --- | --- | --- |
| zzq, 1 IO lane | 0 / 0 / 0% | 122 MiB | no | 60K / - / - |
| zzq, 2 IO lanes | 0 / 0 / 0% | 359 MiB | no | - / - / - |
| rmqtt | 0 / 59 / 76% | 3.6 GiB | no | - / 40K / - |
| EMQX | 99.7 / 75 / 61% | 1.6 GiB | no | 40K / 40K / 200K |
| Mosquitto | 0 / 12 / 0% | 57 MiB | yes | 40K / 40K / 150K |
| NanoMQ | 58 / 60 / 71% | 652 MiB | yes | 20K / 40K / 200K |
| rumqttd | 72 / 81 / 75% | 6.2 GiB | no | 10K / 60K / - |
| XMQ | 0 / 0 / 0% | 1.1 GiB | yes | - / - / - |
| FlashMQ | 99.5 / 0 / 66% | 10.1 GiB | yes | - / - / - |
| VerneMQ | 100 / 100 / 100% | 6.1 GiB | yes | 20K / 20K / 50K |
| FluxMQ | 87 / 0 / 0% | 7.3 GiB | yes | 10K / 10K / - |
| Sagüin | 90 / 90 / 83% | 460 MiB | yes | 60K / 20K / - |

- Fan-in: 16 publishers to one subscriber, the worst of 16 B to 4 KiB.
  Loss is what was not delivered within 2 s after the publishers
  stopped. At QoS 1 and 2, the publishers had their acks for all of it.
- Peak RSS: the largest broker RSS at the end of any throughput cell.
- Stalls a publisher: with 64 QoS 0 pairs, a publisher's 64 KiB write
  was still blocked 10 s after the run stopped. A pair is one publisher
  and one subscriber on a topic of their own.
- Fixed load: the first failed step for point to point, fan-in and
  fan-out with 20K connections. "-" passed every step (80K, 80K and
  200K/s).

## Patterns

- **Ack, then drop.** A PUBACK tells a publisher the broker took the
  message. Seven of ten brokers then dropped acked messages once a
  fan-in subscriber's queue filled: rmqtt, EMQX, Mosquitto, NanoMQ,
  rumqttd, FlashMQ (QoS 2) and Sagüin. Sagüin acked 151K msg/s at QoS 1
  while its subscriber got 1.7K/s. Mosquitto's man page calls these
  messages "silently dropped". FlashMQ's source says: "The QoS system is
  not between publisher and subscriber." The publisher never learns.
- **Ack, then starve.** VerneMQ acked 128K QoS 1 msg/s on fan-in and
  delivered none in the 5 s window. One QoS 1 pair: 59.7K/s acked, 20/s
  delivered. The backlog drains after the publishers stop.
- **Memory as the queue.** Without backpressure, overload goes to RAM. Peak
  RSS in the throughput suite: FlashMQ 10.1 GiB, FluxMQ 7.3 GiB, rumqttd
  6.2 GiB, VerneMQ 6.1 GiB and rmqtt 3.6 GiB. rmqtt loses nothing on QoS
  0 fan-in because its queue has no bound: 395K msg/s in, 122K/s out.
- **Kill the slow consumer.** On QoS 0 fan-in, EMQX closed the
  subscriber at every size (`mailbox_overflow`: 116K messages over a
  limit of 100K).
- **Stall the publisher.** At QoS 0 a broker's only backpressure is to
  read a socket less. With 64 pairs, seven of ten read less than 64 KiB
  from one publisher in 10 s, while the load fell. FlashMQ did it with
  one pair too. Such a client cannot send anything else, not even
  PINGREQ or DISCONNECT.
- **Allocate per connection.** Queue sizes that suit throughput break at
  20K connections, so three brokers needed their own config for the
  fixed-load test:
  - NanoMQ with a 100K queue reserves about 1 MB of virtual memory per
    connection: 16.9 GB at 16.4K connections. Near 19K, `malloc` failed
    and it died, with under 300 MB resident.
  - VerneMQ sizes each socket's buffer from the kernel's send buffer,
    2.5 MiB on loopback. It crashed at 6.1K connections with 16 GB of
    binaries.
  - FluxMQ allocates its send queue in full. With 1000 slots, 20K
    connections took 1 GiB.
- **Reset the quiet client.** FluxMQ resets connections that send about
  once a second. Its read deadline is 1 s, for a retry timer, and can
  expire mid-packet; the next read then starts inside the packet. At one
  message per second per publisher, 6.3K of 20K connections got a TCP
  reset, with no DISCONNECT and no log line.

## Defaults that drop or pace

Out of the box, a benchmark of these brokers measures these settings.
The runs in BENCHMARKS.md raise them.

| Broker | Setting | Default | Past it |
| --- | --- | --- | --- |
| rmqtt | `mqueue_rate_limit` | `1000,1s` | a subscriber gets 1000 msg/s, the rest is dropped: 99% loss on one pair |
| EMQX | `mqtt.max_mqueue_len` | 1000 | messages are dropped |
| EMQX | `force_shutdown.max_mailbox_size` | 1000 | the subscriber is disconnected |
| Mosquitto | `max_queued_messages` | 1000 | messages are dropped, the publisher is acked |
| Mosquitto | `set_tcp_nodelay` | false | Nagle delays small packets |
| NanoMQ | `max_mqueue_len` | 2048 | messages are dropped |
| VerneMQ | `max_online_messages` | 1000 | messages are dropped: 86% loss on one QoS 1 pair |
| FlashMQ | `max_qos_msg_pending_per_client` | 512 | QoS 1/2 messages are dropped |
| FlashMQ | `client_max_write_buffer_size` | 1 MiB | QoS 0 messages are dropped, not counted |
| Sagüin | `broker.qos2.max_inflight_per_client` | 20 | QoS 2 publishes get PUBREC 0x97 |
| FluxMQ, rumqttd, Sagüin, VerneMQ | connections | about 10K | new clients cannot connect |

## Brokers

### rmqtt 0.24.0

- QoS 1/2 fan-in: publishers acked at up to 224K msg/s, the subscriber
  got 12.5K/s, and 44-76% was lost.
- QoS 0 fan-in loses nothing, as the first hop to a subscriber has no
  bound. RSS reached 3.2 GiB in the 4 KiB cell.
- The shipped config paces each subscriber to 1000 msg/s: 99.2% loss on
  one QoS 0 pair, and a 1 ms round trip.
- With a fan-in backlog it ignores SIGTERM. It once held up the harness
  for 27 min until killed.
- Fixed load: fan-in fails at 40K/s with 29% delivered and a 5.4 s
  median.
- Reported: one `tokio::spawn` per offline message ran it out of memory
  in 63 s ([#495](https://github.com/rmqtt/rmqtt/issues/495)).

### EMQX 6.3.2

- QoS 0 fan-in: closes the subscriber at every size, 97-99.7% lost.
- QoS 1/2 fan-in: publishers acked at 37-71K msg/s, the subscriber got
  80-1800/s, and 51-75% was lost (`dropped_msg_due_to_mqueue_is_full`).
- One pair: 56K msg/s at QoS 0 and 18K at QoS 1, at under 200% CPU. The
  default config gives the same.
- Fixed load: 1.5 GiB for 20K connections. Point to point and fan-in
  fail at 40K/s (78% and 31% delivered, 1.3 s and 0.7 s medians),
  fan-out at 200K/s.
- Reported: CPU lockups and 15 s QoS 0 stalls in 6.3.0 and 6.3.2
  ([#19201](https://github.com/emqx/emqx/issues/19201),
  [#19629](https://github.com/emqx/emqx/issues/19629)).

### Mosquitto 2.1.2

- One thread: about 100K msg/s at QoS 0, whether 1 or 16 pairs.
- QoS 1 fan-in at 16 B: 12% lost once its 100K queue filled, with the
  publishers acked. A full queue drops the message and returns 2
  ([database.c](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.1.2/src/database.c#L629-L638)),
  and `subs__send` treats only 1 as a failure
  ([subs.c](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.1.2/src/subs.c#L101)).
- The default config (Nagle on, 20 in flight) doubles single-pair
  throughput over the tuned one: 216K against 102K msg/s at QoS 0.
- Fixed load: point to point and fan-in fail at 40K/s at 99% CPU (201 ms
  and 151 ms medians), fan-out at 150K/s.
- Reported: retained messages go through the same queue, so a QoS 1
  wildcard subscriber gets about 1000 of them and loses the rest
  ([#2887](https://github.com/eclipse-mosquitto/mosquitto/issues/2887)).

### NanoMQ 0.25.7

- 23K msg/s on one QoS 0 pair at 234% CPU, 83K on 16 pairs at 349%,
  most of it futex waits and context switches.
- Fan-in loses 30-58% at QoS 0 and 57-71% at QoS 1/2, with the
  publishers acked.
- Fixed load (default queue): point to point at 10K/s just passes, with
  98.8% delivered and a 1.06 s maximum, and fails at 20K/s.
- Reported: a session is found by a 32-bit hash of the client ID, so
  two clients can take over each other's session, with about 4% odds at
  18K clients ([#2334](https://github.com/nanomq/nanomq/issues/2334)).

### rumqttd 0.20.0

- No backpressure at QoS 0: one pair accepted 551K msg/s and delivered
  2.8K/s. The rest went to its in-memory log, 1.5 GiB.
- A topic's log is never freed, so a full suite ran it out of memory.
  Each cell here gets a fresh process.
- Fan-in loses up to 81% at 256 B and up, once the log (10 segments of
  100 MB) overflows, with the publishers acked. A reader that falls
  behind jumps to the head of the log and skips what it missed
  ([segments/mod.rs](https://github.com/bytebeamio/rumqtt/blob/rumqttd-0.20.0/rumqttd/src/segments/mod.rs#L186-L192)).
- It never used more than 2 of its 4 CPUs (109-195%), even in cells
  where it lost most messages. The other multi-threaded brokers peaked at
  377-400%. All routing runs on one thread
  ([routing.rs](https://github.com/bytebeamio/rumqtt/blob/rumqttd-0.20.0/rumqttd/src/router/routing.rs#L164-L165)).
- Fixed load: accepting 20K connections takes 36 s, or 68 s with the
  shipped `next_connection_delay_ms = 1`. Point to point then delivers 63
  of 10K msg/s.
- A new subscriber gets at most one in-flight window of retained
  messages; the rest are dropped
  ([routing.rs](https://github.com/bytebeamio/rumqtt/blob/rumqttd-0.20.0/rumqttd/src/router/routing.rs#L1469)).

### XMQ 1.0.0

- The strongest rival: 835K msg/s on one QoS 0 pair, 1.59M on 16 pairs,
  no fan-in loss at any QoS or size, and every fixed-load step passed.
- `queue_limits.max_size` is in the config but no server code reads it.
  Past the in-flight limit, a session's queue grows without bound
  ([MessageQueue.cpp](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/base/MessageQueue.cpp#L107-L111)).

### FlashMQ 1.27.3

- QoS 0 overload is dropped: fan-in accepted 1.1-2.3M msg/s and
  delivered 6-12K/s, 92-99.5% lost. Past the 1 MiB write buffer, a QoS 0
  publish is dropped without a log line or counter
  ([client.cpp](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/client.cpp#L411-L416)).
- QoS 2 fan-in lost 61-66% with the publishers acked; QoS 1 lost
  nothing. Past 512 messages or 64 KiB pending per client, QoS 1/2
  messages are dropped
  ([session.cpp](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/session.cpp#L259-L287),
  [comment](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/session.cpp#L344-L348)).
- RSS grew to 10.1 GiB in the throughput suite and stayed there.
- Fixed load: every step passed, with p99 under 5 ms point to point.

### VerneMQ 2.2.1

- QoS 1/2: acks 35-140K msg/s and delivers 0-220/s while publishers
  run. With a 100K queue, nothing was delivered in the window.
- Fan-in starves even at QoS 0: 91-100% not delivered.
- QoS 1 round trip p50 165-181 µs, against 66-69 µs at QoS 0: each QoS
  1/2 delivery goes through its LevelDB message store
  ([vmq_storage_engine_leveldb.erl](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_generic_msg_store/src/engines/vmq_storage_engine_leveldb.erl)).
- Without `listener.tcp.buffer_sizes`, each socket's Erlang buffer takes
  the kernel's send buffer size
  ([vmq_ranch.erl](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_server/src/vmq_ranch.erl#L65-L82)).
  With 128 KiB buffers it holds 20K connections.
- Fixed load: point to point and fan-in fail at 20K/s (83% and 38%
  delivered), fan-out at 50K/s.

### FluxMQ 1.0.0

- The connection resets above come from the read deadline
  ([lifecycle.go](https://github.com/absmach/fluxmq/blob/v1.0.0/mqtt/broker/lifecycle.go#L42-L48))
  and from reading the next packet from wherever the stream stands
  ([connection.go](https://github.com/absmach/fluxmq/blob/v1.0.0/mqtt/connection.go#L190)).
- In its default blocking mode, QoS 1/2 fan-in loses nothing. QoS 0
  fan-in drops 78-87%, and QoS 0 overload grew RSS to 7.3 GiB.
- Port 1883 speaks only MQTT 3.1.1; MQTT 5 is on 1884.
- Ignored SIGTERM for over 15 s after the fixed-load point-to-point
  scenario.

### Sagüin 0.1.0-rc.1

- Fan-in loses at every QoS: 77-90% at QoS 0, 79-90% at QoS 1 and
  70-83% at QoS 2, with the QoS 1/2 publishers acked.
- With 100 QoS 2 messages in flight per publisher, 16 pairs got 257K
  PUBREC 0x97 and lost 77.7%. Under MQTT 3.1.1 it closes the connection
  instead.
- Fixed load: 814-861 MiB for 20K connections. Point-to-point p99 is
  47 ms from the first step, and fan-in fails at 20K/s.

## Clients

- **rumqttc 0.25.1** acks a QoS 1/2 message when it reads it, before the
  application sees it
  ([state.rs](https://github.com/bytebeamio/rumqtt/blob/rumqttc-0.25.1/rumqttc/src/state.rs#L201-L215)).
  It flushes after every request: 200,001 `writev` calls for 200K
  publishes. `max_request_batch` is parsed but never used.
- **ntex-mqtt 9.3** acks when the application's handler returns, one
  handler future per message.
- **emqtt 1.14.6** publishes through one Erlang process per connection:
  8 calling processes were no faster than 1.
- **zzq-client** acks when the application takes the message, in
  batches.

## Durability

What a crash loses that a publisher already has its PUBACK for.

| Broker | Default | Storage option | PUBACK waits for the disk | A crash loses, with storage on |
| --- | --- | --- | --- | --- |
| zzq | RAM | state file at SIGTERM, or a data directory | yes | nothing it acked |
| rmqtt | RAM | plugins: sessions in sled, messages in Redis | no | queues of connected sessions |
| EMQX | RAM | durable sessions in RocksDB | no | writes in flight, 15 s of session state |
| Mosquitto | RAM | snapshot every 30 min, or a SQLite plugin | no | 30 min, or 5 s with SQLite |
| NanoMQ | RAM | SQLite, if built with it | no | writes in flight, every session |
| rumqttd | RAM | none | - | everything |
| XMQ | RAM | Redis | only Super-Durable with `max_queued_writes: 0` | 1000 messages |
| FlashMQ | RAM, or snapshots in the .deb config | snapshot every hour | no | an hour |
| VerneMQ | LevelDB | LevelDB | no | writes in flight, unsynced writes on power loss |
| FluxMQ | Badger, without fsync | durable queues | only for a queue in `fsync` mode | queued and in-flight MQTT messages, 1 s of queue writes |
| Sagüin | none until configured | memory or SQLite | for the commit, not the fsync | 150 ms on power loss with SQLite |

- **Ack, then write.** Of the brokers with storage, only XMQ in
  Super-Durable mode with `max_queued_writes: 0`, Sagüin, FluxMQ queues in
  `fsync` mode and zzq write before the PUBACK. XMQ's source says that
  mode caps the broker at about 25K msg/s
  ([MessageDelivery.cpp](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/server/MessageDelivery.cpp#L85-L107)).
- **zzq.** Sessions and queues live in RAM. `--state-file` writes them at
  SIGTERM or SIGINT and loads them at the next start, so a crash loses
  all of them. `--data-dir` keeps retained messages, sessions, their
  subscriptions and QoS 1/2 messages in journals written with `O_DSYNC`,
  and acks a publish or a subscription once its write is done.
- **rmqtt.** Both storage plugins are off in the shipped config
  ([rmqtt.toml](https://github.com/rmqtt/rmqtt/blob/0.24.0/rmqtt.toml#L78-L105)).
  Only disconnected sessions are stored
  ([docs](https://github.com/rmqtt/rmqtt/blob/0.24.0/docs/en_US/store-session.md#L7-L10)).
  A save waits in a bounded queue and is dropped when the queue is full
  ([lib.rs](https://github.com/rmqtt/rmqtt/blob/0.24.0/rmqtt-plugins/rmqtt-session-storage/src/lib.rs#L585-L609)).
  Retained messages stay in RAM by default.
- **EMQX.** "Session data is lost when the EMQX node hosting the session
  stops or restarts"
  ([docs](https://docs.emqx.com/en/emqx/latest/durability/durability_introduction.html)).
  Durable sessions are off by default. With them on, the PUBACK does not
  wait for the write
  ([emqx_broker.erl](https://github.com/emqx/emqx/blob/6.3.2/apps/emqx/src/emqx_broker.erl#L315-L321)),
  and session state is saved every 15 s.
- **Mosquitto.** Persistence is off by default
  ([conf.c](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.1.2/src/conf.c#L343)).
  The snapshot runs every 1800 s inside the single event loop
  ([loop.c](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.1.2/src/loop.c#L225-L238)).
  The SQLite plugin commits every 5 s, with `synchronous=NORMAL`
  ([plugin.c](https://github.com/eclipse-mosquitto/mosquitto/blob/v2.1.2/plugins/persist-sqlite/plugin.c#L58-L65)).
  With either, a `kill -9` can bring back sessions their clients already
  ended ([#3761](https://github.com/eclipse-mosquitto/mosquitto/issues/3761)).
- **NanoMQ.** The PUBACK goes out when the PUBLISH is read, before
  routing and before any write
  ([nmq_mqtt.c](https://github.com/nanomq/NanoNNG/blob/0.25.7/src/sp/protocol/mqtt/nmq_mqtt.c#L1161-L1190)).
  SQLite keeps outbound QoS 1/2 and retained messages, with
  `synchronous=NORMAL`, but no sessions
  ([mqtt_qos_db.c](https://github.com/nanomq/NanoNNG/blob/0.25.7/src/supplemental/mqtt/mqtt_qos_db.c#L145-L150)).
  When a persistent session disconnects, its send queue is freed
  ([nmq_mqtt.c](https://github.com/nanomq/NanoNNG/blob/0.25.7/src/sp/protocol/mqtt/nmq_mqtt.c#L917-L962)).
- **rumqttd.** Everything is in RAM. Persistence exists "in an internal
  fork" ([#967](https://github.com/bytebeamio/rumqtt/issues/967)).
- **XMQ.** Persistence is off by default
  ([README](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/README.md#L128-L129)).
  "Durable" is Redis with `appendfsync everysec`, "Super-Durable" with
  `appendfsync always`
  ([docs](https://xmq.sptk.net/xmq_persistence)). Both write up to 1000
  records behind the PUBACK by default. When those 1000 are waiting,
  every QoS 1/2 publisher pauses, durable session or not, until Redis
  catches up
  ([ClientSession.cpp](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/server/ClientSession/ClientSession.cpp#L971-L984)).
  With a bridge or cluster peers, the PUBACK does not wait for the store
  at all
  ([ClientSession.cpp](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/server/ClientSession/ClientSession.cpp#L1335-L1337)).
  While Redis is disconnected, records are skipped
  ([MessageDelivery.cpp](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/server/MessageDelivery.cpp#L293-L299)).
- **FlashMQ.** Without `storage_dir` nothing is saved. The .deb config
  sets it
  ([flashmq.conf](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/flashmq.conf#L4)),
  the Docker image does not. It saves sessions, queued messages, wills
  and retained messages every 3623 s
  ([settings.h](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/settings.h#L134))
  and at shutdown. A crash can bring back sessions their clients already
  ended ([#177](https://github.com/halfgaar/FlashMQ/issues/177)).
- **VerneMQ.** Every QoS 1/2 delivery is written to LevelDB, even to an
  online session
  ([vmq_queue.erl](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_server/src/vmq_queue.erl#L1362-L1399)).
  The PUBACK goes out before the write
  ([vmq_mqtt_fsm.erl](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_server/src/vmq_mqtt_fsm.erl#L1197-L1200)),
  and LevelDB does not sync by default.
- **FluxMQ.** The default store is Badger in `/tmp/fluxmq/data`, without
  fsync
  ([config.go](https://github.com/absmach/fluxmq/blob/v1.0.0/config/config.go#L1246-L1251)).
  It keeps sessions and subscriptions. Queued and in-flight messages are
  written only when a session is given up, such as at disconnect or
  shutdown, and write errors are ignored
  ([session.go](https://github.com/absmach/fluxmq/blob/v1.0.0/mqtt/broker/session.go#L911-L934)).
  On a single node, a CONNECT that resumes a stored session is refused
  without a log line
  ([#626](https://github.com/absmach/fluxmq/issues/626), fixed after
  1.0.0). Durable queues ack from the page cache and sync every second
  ([durability.go](https://github.com/absmach/fluxmq/blob/v1.0.0/queue/durability.go#L27-L33)).
- **Sagüin.** Nothing is stored until the config names a provider.
  Memory storage "survives a graceful shutdown snapshot, not a process
  crash or power loss"
  ([README](https://github.com/ifnesi/saguin/blob/v0.1.0-rc.1/README.md#L183-L184)).
  SQLite syncs every 150 ms, 10 ms at the least
  ([config.go](https://github.com/ifnesi/saguin/blob/v0.1.0-rc.1/internal/config/config.go#L872-L879)).
  Its own tests lost 52 and 138 ms of acked publishes in two power cuts
  ([RFC](https://github.com/ifnesi/saguin/blob/v0.1.0-rc.1/docs/rfcs/0004-storage.md#L572-L577)).

## Clustering

What the open release offers, and what happens when a node fails.

| Broker | Clustering | Shared between nodes | When a node fails |
| --- | --- | --- | --- |
| zzq | none | - | - |
| rmqtt | Raft or broadcast | client registry, subscriptions | its sessions and queues are lost |
| EMQX | one node without a commercial license | client registry, routes | its in-memory sessions are lost |
| Mosquitto | none, bridges | - | - |
| NanoMQ | none, bridges | - | - |
| rumqttd | none | - | - |
| XMQ | in the code, "not in this release" | all state, in one Redis | what its clients had not acked is lost |
| FlashMQ | none, bridges | - | - |
| VerneMQ | yes | subscriptions | the cluster refuses CONNECT and PUBLISH; its queues wait for it |
| FluxMQ | etcd | session ownership | its sessions are lost, and their client IDs cannot connect for 30 s |
| Sagüin | none | - | - |

- **rmqtt.** Neither mode replicates sessions, queues or in-flight
  messages. Broadcast mode sends every publish to every node
  ([shared.rs](https://github.com/rmqtt/rmqtt/blob/0.24.0/rmqtt-plugins/rmqtt-cluster-broadcast/src/shared.rs#L411-L431)).
  The Raft log lives in RAM
  ([storage.rs](https://github.com/rmqtt/rmqtt-raft/blob/0.7.2/src/storage.rs#L45-L48)),
  so a restarted node rejoins empty. After a majority goes down,
  restarted nodes exit with "Leader does not exist" and loop
  ([#330](https://github.com/rmqtt/rmqtt/issues/330)).
- **EMQX.** 5.8.0 removed core-replicant clustering from the open-source
  edition
  ([changelog](https://github.com/emqx/emqx/blob/v5.8.0/changes/ce/breaking-13526.en.md)).
  Since 5.9 the license allows "production use of a single node"
  ([LICENSE](https://github.com/emqx/emqx/blob/6.3.2/LICENSE#L12-L22)),
  and the built-in license refuses joins
  ([emqx_cluster.erl](https://github.com/emqx/emqx/blob/6.3.2/apps/emqx/src/emqx_cluster.erl#L126-L141)).
  Durable sessions replicate through Raft in a licensed cluster.
- **Mosquitto.** Clustering is in Cedalo's paid Pro edition:
  active-passive, at least three nodes
  ([docs](https://docs.cedalo.com/mosquitto/clustering/overview-clusters)).
  Open bridge issues: one QoS 2 message lost per reconnect with a full
  in-flight window
  ([#3275](https://github.com/eclipse-mosquitto/mosquitto/issues/3275)),
  and memory growth while no host answers at the bridge address
  ([#3296](https://github.com/eclipse-mosquitto/mosquitto/issues/3296)).
- **NanoMQ.** A maintainer on 2026-10-21: "it would rather be a mesh-like
  failover solution that just share msg between nodes, not standard
  clustering" ([#2044](https://github.com/nanomq/nanomq/issues/2044)).
  Busy bridges drop QoS 0 and evict the oldest QoS 1/2 messages
  ([bridge.c](https://github.com/nanomq/nanomq/blob/0.25.7/nanomq/bridge.c#L2133-L2160)).
  One drop path logs "Commercial ver assures you no msg lost!"
  ([mqtt_client.c](https://github.com/nanomq/NanoNNG/blob/0.25.7/src/mqtt/protocol/mqtt/mqtt_client.c#L1505-L1514)).
- **rumqttd.** "we do not support clustering at the moment"
  ([#368](https://github.com/bytebeamio/rumqtt/issues/368)). The `cluster`
  config is parsed and ignored
  ([broker.rs](https://github.com/bytebeamio/rumqtt/blob/rumqttd-0.20.0/rumqttd/src/server/broker.rs#L78-L95)).
- **XMQ.** The README says clustering "is being built and is not in this
  release"
  ([README](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/README.md#L134)),
  while its cluster doc plans "1.0: Release a complete, tested cluster".
  One shared Redis is the arbiter and the only store
  ([Cluster.md](https://github.com/alexparshin1/xmq_public/blob/version-1.0.0/doc/Cluster.md)):
  - Redis down for every node stops the whole cluster.
  - A node that cannot reach Redis for 10 s disconnects all its clients.
  - A Redis failover to another address is refused as a different store.
  - A node cut off from its peers but not from Redis keeps serving,
    while messages between it and the rest stop.
  - Retained changes are acked before the Redis write.
- **FlashMQ.** The man page says a bridge "is not a form of clustering"
  ([flashmq.conf.5](https://github.com/halfgaar/FlashMQ/blob/v1.27.3/man/flashmq.conf.5.dbk5#L1232)).
- **VerneMQ.** While any member is unreachable, the cluster refuses
  CONNECT, PUBLISH, SUBSCRIBE and UNSUBSCRIBE, and a QoS 1 PUBLISH gets no
  PUBACK, unless four `allow_*_during_netsplit` options are on
  ([vmq_server.schema](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_server/priv/vmq_server.schema#L13-L35)).
  In 2018 a maintainer wrote "I'll see that we set this to default
  settings" ([#866](https://github.com/vernemq/vernemq/issues/866)).
  Queues are not replicated: a dead node's offline messages wait for it
  ([#1577](https://github.com/vernemq/vernemq/issues/1577)), and a node
  that leaves loses them
  ([docs](https://github.com/vernemq/vmq-docs/blob/d2e184954670b3cf51a17b44ddd854019422b6c1/clustering/introduction.md#L89)).
  After a node dies, a client's first CONNECT to another node crashes
  ([vmq_reg.erl](https://github.com/vernemq/vernemq/blob/2.2.1/apps/vmq_server/src/vmq_reg.erl#L319-L339)),
  fixed on `main` after 2.2.1.
- **FluxMQ.** etcd holds session ownership. The session stays in the
  owner's RAM, with no replica. After the owner crashes, CONNECTs for its
  client IDs fail until its 30 s lease expires
  ([session.go](https://github.com/absmach/fluxmq/blob/v1.0.0/mqtt/broker/session.go#L134-L137),
  [etcd.go](https://github.com/absmach/fluxmq/blob/v1.0.0/cluster/etcd.go#L1092)).
- **Sagüin.** "One node and one process; no clustering, consensus, or
  automatic failover"
  ([README](https://github.com/ifnesi/saguin/blob/v0.1.0-rc.1/README.md#L172-L173)).

## zzq

- QoS 1/2: once a connected subscriber has `qos_queue_high` (1024)
  messages queued, the publishers that feed it get their acks held until
  it is down to half. Publishers slow to the subscriber's pace.
- QoS 0: past `max_out_bytes` waiting for one connection, QoS 0 messages
  to it are dropped and counted.
- An offline session queues up to `max_queued` messages and
  `max_queued_bytes`; past that the oldest are dropped and counted.
  `max_state_bytes` bounds all sessions, queues and retained messages.
- With `--data-dir`, the state stops growing the same way while the disk
  has less room than another copy of it, so every journal can still
  compact. A journal that finds the disk full writes on in the segment it
  keeps spare, and waits once that is full too, until there is room.
  `$SYS/broker/store/full` and a line on stderr say so.
