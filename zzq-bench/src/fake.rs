//! A fake broker for one client connection, so a client bench measures the
//! client and not a broker.

use std::io::{self, ErrorKind, Write};
use std::net::{TcpListener, TcpStream};
use std::time::{Duration, Instant};

use crate::wire::{self, Head, PUBACK, PUBCOMP, PUBREC, PUBREL, Reader};

/// The topic a source publishes on.
pub const TOPIC: &str = "bench/t";
/// QoS 0 publishes per pre-encoded chunk.
const CHUNK: usize = 1024;

/// What the fake broker does with its client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    /// Acks and counts the client's publishes.
    Sink,
    /// Publishes to the client once it subscribes.
    Source,
}

/// Fake broker settings.
#[derive(Debug, Clone, Copy)]
pub struct Fake {
    /// Sink or source.
    pub role: Role,
    /// QoS a source publishes and subscribers are granted.
    pub qos: u8,
    /// Payload size of a source's publishes.
    pub size: usize,
    /// Messages to take or send.
    pub n: usize,
    /// QoS 1/2 messages a source keeps in flight.
    pub window: usize,
}

/// One client's session with the fake broker.
#[derive(Debug, Default)]
struct Session {
    v5: bool,
    subscribed: bool,
    /// Publishes taken (sink) or sent (source).
    count: usize,
    sent: usize,
    outstanding: usize,
    pkid: u16,
    /// QoS 0 publishes, [`CHUNK`] of them, built at CONNECT.
    chunk: Vec<u8>,
    /// When the first message moved.
    first: Option<Instant>,
    /// A source's measured time, once every message went out.
    done: Option<Duration>,
}

impl Session {
    fn elapsed(&self) -> Duration {
        self.done
            .unwrap_or_else(|| self.first.map_or(Duration::ZERO, |t| t.elapsed()))
    }
}

/// Serves one client from `listener`, v3.1.1 or v5 as its CONNECT says. A
/// sink measures from the first to the `n`-th publish it takes. A source
/// sends `n` publishes on [`TOPIC`] after SUBSCRIBE and measures until the
/// last write (QoS 0) or the last ack (QoS 1/2). The connection stays open
/// until the client closes it, as with a real broker.
///
/// # Errors
///
/// If accepting fails or the client's CONNECT is malformed.
pub fn serve(listener: &TcpListener, fake: &Fake) -> io::Result<Duration> {
    let (mut s, _) = listener.accept()?;
    s.set_nodelay(true)?;
    let mut rd = Reader::default();
    let mut out = Vec::new();
    let mut ses = Session::default();
    let payload = vec![b'x'; fake.size];
    loop {
        match rd.fill(&mut s) {
            Ok(0) | Err(_) => return Ok(ses.elapsed()),
            Ok(_) => {}
        }
        while let Some((h, body)) = rd.next_packet() {
            if !answer(&mut ses, (h, body), fake, &payload, &mut out)? {
                return Ok(ses.elapsed());
            }
        }
        if fake.role == Role::Sink && ses.count >= fake.n {
            let _ = s.write_all(&out);
            return Ok(ses.elapsed());
        }
        if fake.role == Role::Source && ses.subscribed {
            publish(&mut s, &mut ses, fake, &payload, &mut out);
        }
        if !out.is_empty() {
            if s.write_all(&out).is_err() {
                return Ok(ses.elapsed());
            }
            out.clear();
        }
    }
}

/// Answers one packet from the client. Returns false on DISCONNECT.
fn answer(
    ses: &mut Session,
    (h, body): (u8, &[u8]),
    fake: &Fake,
    payload: &[u8],
    out: &mut Vec<u8>,
) -> io::Result<bool> {
    match h >> 4 {
        1 => {
            let name = body
                .get(..2)
                .map(|b| usize::from(u16::from_be_bytes([b[0], b[1]])));
            let level = name.and_then(|n| body.get(2 + n));
            let Some(&level) = level else {
                return Err(io::Error::new(ErrorKind::InvalidData, "short CONNECT"));
            };
            ses.v5 = level == 5;
            out.extend_from_slice(if ses.v5 {
                &[0x20, 3, 0, 0, 0]
            } else {
                &[0x20, 2, 0, 0]
            });
            for _ in 0..CHUNK {
                let head = head(0, 0, ses.v5);
                wire::publish(&mut ses.chunk, head, payload);
            }
        }
        3 => match (h >> 1) & 3 {
            0 => ses.count += 1,
            1 => {
                wire::ack(out, PUBACK, &wire::publish_pkid(body));
                ses.count += 1;
            }
            _ => wire::ack(out, PUBREC, &wire::publish_pkid(body)),
        },
        4 | 7 => ses.outstanding = ses.outstanding.saturating_sub(1),
        5 => wire::ack(out, PUBREL, body),
        6 => {
            wire::ack(out, PUBCOMP, body);
            ses.count += 1;
        }
        8 => {
            let (a, b) = (body.first().copied(), body.get(1).copied());
            let id = [a.unwrap_or(0), b.unwrap_or(0)];
            if ses.v5 {
                out.extend_from_slice(&[0x90, 4, id[0], id[1], 0, fake.qos]);
            } else {
                out.extend_from_slice(&[0x90, 3, id[0], id[1], fake.qos]);
            }
            ses.subscribed = true;
        }
        12 => out.extend_from_slice(&[0xd0, 0]),
        14 => return Ok(false),
        _ => {}
    }
    if ses.count > 0 && ses.first.is_none() {
        ses.first = Some(Instant::now());
    }
    Ok(true)
}

const fn head(qos: u8, pkid: u16, v5: bool) -> Head<'static> {
    Head {
        topic: TOPIC,
        qos,
        pkid,
        v5,
        retain: false,
    }
}

/// A source's publishing step: all QoS 0 messages at once, or QoS 1/2 up to
/// the window.
fn publish(s: &mut TcpStream, ses: &mut Session, fake: &Fake, payload: &[u8], out: &mut Vec<u8>) {
    if fake.qos == 0 {
        if ses.sent >= fake.n {
            return;
        }
        if s.write_all(out).is_err() {
            return;
        }
        out.clear();
        let start = Instant::now();
        ses.first = Some(start);
        let one = ses.chunk.len() / CHUNK;
        while ses.sent < fake.n {
            let k = CHUNK.min(fake.n - ses.sent);
            if s.write_all(&ses.chunk[..one * k]).is_err() {
                break;
            }
            ses.sent += k;
        }
        ses.done = Some(start.elapsed());
        return;
    }
    let start = *ses.first.get_or_insert_with(Instant::now);
    if ses.sent == fake.n && ses.outstanding == 0 && ses.done.is_none() {
        ses.done = Some(start.elapsed());
    }
    while ses.sent < fake.n && ses.outstanding < fake.window {
        ses.pkid = ses.pkid % u16::MAX + 1;
        wire::publish(out, head(fake.qos, ses.pkid, ses.v5), payload);
        ses.sent += 1;
        ses.outstanding += 1;
    }
}
