use std::time::Duration;

use tokio::net::UdpSocket;
use zzq_broker::{BrokerOptions, SnOptions};
use zzq_client::{QoS, Version};
use zzq_proto::sn::{self, Filter, Flags, Packet, SnQoS, Topic};

use super::support::{Run, Server, next, payload, within};

pub(super) async fn run(name: &str, disk: bool) {
    let run = Run::new(name);
    let mut sn = SnOptions::default();
    sn.retry = Duration::from_millis(200);
    let opts = BrokerOptions {
        sn,
        ..BrokerOptions::default()
    };
    let server = Server::new(&run, "tcp", disk, opts).await;
    let udp = within(server.broker.bind("udp://127.0.0.1:0"))
        .await
        .unwrap();
    let mut mqtt = server.client("mqtt", Version::V5, true).await;
    within(mqtt.subscribe("sn/up", QoS::AtLeastOnce))
        .await
        .unwrap();
    let mut cycles = 0;
    while run.running() || cycles < 2 {
        cycle(&udp.socket_addr_string().unwrap(), &mut mqtt, &run, cycles).await;
        cycles += 1;
        run.progress(2);
    }
    within(mqtt.disconnect()).await;
    server.close().await;
    run.finish(cycles);
}

const fn flags() -> Flags {
    Flags {
        qos: SnQoS::AtLeastOnce,
        dup: false,
        retain: false,
        will: false,
        clean_session: false,
    }
}

async fn send(s: &UdpSocket, packet: &Packet<'_>) {
    let mut out = Vec::new();
    sn::encode(packet, &mut out);
    within(s.send(&out)).await.unwrap();
}

async fn recv(s: &UdpSocket) -> Vec<u8> {
    let mut out = vec![0; 4096];
    let n = within(s.recv(&mut out)).await.unwrap();
    out.truncate(n);
    out
}

async fn connect(s: &UdpSocket) {
    send(
        s,
        &Packet::Connect {
            flags: Flags {
                clean_session: true,
                ..Flags::default()
            },
            duration: 30,
            client_id: "sn",
        },
    )
    .await;
    assert_eq!(
        sn::decode(&recv(s).await).unwrap().0,
        Packet::ConnAck { code: sn::ACCEPTED }
    );
}

async fn register(s: &UdpSocket) -> u16 {
    send(
        s,
        &Packet::Register {
            topic_id: 0,
            msg_id: 1,
            topic: "sn/up",
        },
    )
    .await;
    let bytes = recv(s).await;
    let Packet::RegAck {
        topic_id,
        msg_id: 1,
        code: sn::ACCEPTED,
    } = sn::decode(&bytes).unwrap().0
    else {
        panic!("REGACK: {bytes:?}");
    };
    topic_id
}

async fn subscribe(s: &UdpSocket) -> u16 {
    send(
        s,
        &Packet::Subscribe {
            flags: flags(),
            msg_id: 2,
            topic: Filter::Name("sn/down"),
        },
    )
    .await;
    let bytes = recv(s).await;
    let Packet::SubAck {
        topic_id,
        msg_id: 2,
        code: sn::ACCEPTED,
        ..
    } = sn::decode(&bytes).unwrap().0
    else {
        panic!("SUBACK: {bytes:?}");
    };
    topic_id
}

fn delivery(bytes: &[u8], topic: u16, expected: &[u8], dup: bool) -> u16 {
    let Packet::Publish {
        topic: Topic::Id(id),
        msg_id,
        data,
        flags,
    } = sn::decode(bytes).unwrap().0
    else {
        panic!("PUBLISH: {bytes:?}");
    };
    assert_eq!(id, topic);
    assert_eq!(data, expected);
    assert_eq!(flags.dup, dup);
    msg_id
}

async fn ack(s: &UdpSocket, topic_id: u16, msg_id: u16) {
    send(
        s,
        &Packet::PubAck {
            topic_id,
            msg_id,
            code: sn::ACCEPTED,
        },
    )
    .await;
}

async fn disconnected(s: &UdpSocket) {
    within(async {
        loop {
            let bytes = recv(s).await;
            match sn::decode(&bytes).unwrap().0 {
                Packet::Disconnect { duration: None } => break,
                // The gateway may have an upstream keepalive reply in flight
                // when an application datagram wakes the sleeping connection.
                Packet::PingResp => {}
                other => panic!("expected DISCONNECT: {other:?}"),
            }
        }
    })
    .await;
}

async fn sleep_cycle(s: &UdpSocket, mqtt: &zzq_client::Client, down: u16, data: &[u8]) {
    send(s, &Packet::Disconnect { duration: Some(30) }).await;
    disconnected(s).await;
    within(mqtt.publish("sn/down", data, QoS::AtLeastOnce))
        .await
        .unwrap();
    within(async {
        let mut poll = tokio::time::interval(Duration::from_millis(50));
        loop {
            tokio::select! {
                // Both receive and interval are cancel-safe. Each ping asks
                // for the messages that have reached the gateway by then.
                _ = poll.tick() => send(s, &Packet::PingReq { client_id: Some("sn") }).await,
                bytes = recv(s) => {
                    if sn::decode(&bytes).unwrap().0 == Packet::PingResp { continue; }
                    let id = delivery(&bytes, down, data, false);
                    ack(s, down, id).await;
                    break;
                }
            }
        }
    })
    .await;
}

async fn cycle(addr: &str, mqtt: &mut zzq_client::Client, run: &Run, cycles: u64) {
    let s = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    s.connect(addr).await.unwrap();
    connect(&s).await;
    let up = register(&s).await;
    let down = subscribe(&s).await;
    let data = payload(cycles, 1024, run.seed);
    send(
        &s,
        &Packet::Publish {
            flags: flags(),
            topic: Topic::Id(up),
            msg_id: 3,
            data: &data,
        },
    )
    .await;
    let response = recv(&s).await;
    assert_eq!(
        sn::decode(&response).unwrap().0,
        Packet::PubAck {
            topic_id: up,
            msg_id: 3,
            code: sn::ACCEPTED
        }
    );
    super::support::same(next(mqtt).await.payload.as_ref(), &data);
    within(mqtt.publish("sn/down", &data, QoS::AtLeastOnce))
        .await
        .unwrap();
    let first = delivery(&recv(&s).await, down, &data, false);
    let again = delivery(&recv(&s).await, down, &data, true);
    assert_eq!(first, again, "retry keeps packet ID");
    ack(&s, down, first).await;
    sleep_cycle(&s, mqtt, down, &data).await;
    send(&s, &Packet::Disconnect { duration: None }).await;
    disconnected(&s).await;
}
