//! Blocking raw MQTT client pieces.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use zzq_proto::encode::{self, ConnectFields, PublishHeader};
use zzq_proto::packet::fixed_header;
use zzq_proto::property::{self, Properties, Property};
use zzq_proto::{QoS, SubscriptionOptions, Version};

/// Keep alive the raw clients ask for: long enough that no run needs a
/// PINGREQ. Some brokers (rumqttd) close a connection that asks for 0.
pub const KEEP_ALIVE: u16 = 300;
/// First byte of PUBACK.
pub const PUBACK: u8 = 0x40;
/// First byte of PUBREC.
pub const PUBREC: u8 = 0x50;
/// First byte of PUBREL.
pub const PUBREL: u8 = 0x62;
/// First byte of PUBCOMP.
pub const PUBCOMP: u8 = 0x70;

/// Buffered packet reader over a blocking socket.
#[derive(Debug)]
pub struct Reader {
    buf: Vec<u8>,
    len: usize,
    pos: usize,
}

impl Default for Reader {
    fn default() -> Self {
        Reader::with_capacity(1 << 20)
    }
}

impl Reader {
    /// A reader that starts with `n` bytes of buffer.
    pub fn with_capacity(n: usize) -> Reader {
        Reader {
            buf: vec![0; n.max(1)],
            len: 0,
            pos: 0,
        }
    }

    /// Reads more bytes. `Ok(0)` means the peer closed.
    ///
    /// # Errors
    ///
    /// If the read fails.
    pub fn fill<R: Read>(&mut self, s: &mut R) -> io::Result<usize> {
        let k = s.read(self.space())?;
        self.filled(k);
        Ok(k)
    }

    /// Free space to read into, after moving unread bytes to the front.
    /// Grows when full.
    pub fn space(&mut self) -> &mut [u8] {
        if self.pos > 0 {
            self.buf.copy_within(self.pos..self.len, 0);
            self.len -= self.pos;
            self.pos = 0;
        }
        if self.len == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        &mut self.buf[self.len..]
    }

    /// Marks `k` bytes read into [`Reader::space`].
    pub const fn filled(&mut self, k: usize) {
        self.len += k;
    }

    /// Fixed header byte and body of the next whole packet.
    #[inline]
    pub fn next_packet(&mut self) -> Option<(u8, &[u8])> {
        let b = &self.buf[self.pos..self.len];
        let h = fixed_header(b).ok()??;
        if b.len() < h.packet_len() {
            return None;
        }
        let start = self.pos + h.header_len;
        self.pos = start + h.remaining;
        Some((h.byte1, &self.buf[start..self.pos]))
    }
}

/// Writes a short ack.
#[inline]
pub fn ack(out: &mut Vec<u8>, kind: u8, pkid: &[u8]) {
    out.extend_from_slice(&[kind, 2, pkid[0], pkid[1]]);
}

/// Packet ID of a QoS 1/2 PUBLISH body.
#[inline]
pub fn publish_pkid(body: &[u8]) -> [u8; 2] {
    let tl = usize::from(u16::from_be_bytes([body[0], body[1]]));
    [body[2 + tl], body[3 + tl]]
}

/// Payload of a PUBLISH body.
pub fn publish_payload(body: &[u8], qos: u8, v5: bool) -> &[u8] {
    let mut off = 2 + usize::from(u16::from_be_bytes([body[0], body[1]]));
    if qos > 0 {
        off += 2;
    }
    if v5 && let Ok(Some((len, n))) = zzq_proto::varint::decode(&body[off..]) {
        off += n + len;
    }
    &body[off.min(body.len())..]
}

/// PUBLISH header fields for [`publish`].
#[derive(Debug, Clone, Copy)]
pub struct Head<'a> {
    /// Topic name.
    pub topic: &'a str,
    /// QoS, 0 to 2.
    pub qos: u8,
    /// Packet ID, ignored for QoS 0.
    pub pkid: u16,
    /// Encode for v5.
    pub v5: bool,
    /// Retain flag.
    pub retain: bool,
}

/// Encodes a PUBLISH.
pub fn publish(out: &mut Vec<u8>, head: Head<'_>, payload: &[u8]) {
    let h = PublishHeader {
        version: version(head.v5),
        dup: false,
        qos: QoS::try_from(head.qos).unwrap_or_default(),
        retain: head.retain,
        topic: head.topic,
        pkid: head.pkid,
        properties: &[],
    };
    encode::publish(&h, payload, out);
}

const fn version(v5: bool) -> Version {
    if v5 { Version::V5 } else { Version::V311 }
}

fn closed(what: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        format!("closed before {what}"),
    )
}

fn invalid(what: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, what)
}

/// Whether a client's session outlives its connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Session {
    /// It ends with the connection.
    Clean,
    /// It stays for ever.
    Kept,
}

/// Encodes a CONNECT.
pub fn connect_packet(
    out: &mut Vec<u8>,
    client_id: &str,
    v5: bool,
    keep_alive: u16,
    session: Session,
) {
    let forever = [Property::U32(property::SESSION_EXPIRY_INTERVAL, u32::MAX)];
    let kept = session == Session::Kept;
    let c = ConnectFields {
        version: version(v5),
        clean_start: !kept,
        keep_alive,
        properties: if kept && v5 { &forever } else { &[] },
        client_id,
        will: None,
        username: None,
        password: None,
    };
    encode::connect(&c, out);
}

/// Encodes a SUBSCRIBE of one filter, packet ID 1.
pub fn subscribe_packet(out: &mut Vec<u8>, filter: &str, qos: u8, v5: bool) {
    let opts = SubscriptionOptions::from(QoS::try_from(qos).unwrap_or_default());
    encode::subscribe(version(v5), 1, &[], &[(filter, opts)], out);
}

/// A connected client.
#[derive(Debug)]
pub struct Conn {
    /// The client ID.
    pub id: String,
    /// The socket.
    pub s: TcpStream,
    /// The reader of the broker's packets.
    pub rd: Reader,
    /// The broker's Receive Maximum.
    pub rmax: Option<u16>,
    /// MQTT 5, not 3.1.1.
    pub v5: bool,
}

impl Conn {
    /// Connects as `id` and waits for CONNACK.
    ///
    /// # Errors
    ///
    /// If the connection fails, the broker closes it, or CONNACK is
    /// malformed or refuses.
    pub fn open(addr: &str, id: impl Into<String>, v5: bool, session: Session) -> io::Result<Conn> {
        let id = id.into();
        let mut s = TcpStream::connect(addr)?;
        s.set_nodelay(true)?;
        let mut out = Vec::new();
        connect_packet(&mut out, &id, v5, KEEP_ALIVE, session);
        s.write_all(&out)?;
        let mut rd = Reader::default();
        loop {
            if rd.fill(&mut s)? == 0 {
                return Err(closed("CONNACK"));
            }
            if let Some((h, body)) = rd.next_packet() {
                if h != 0x20 || body.len() < 2 || body[1] != 0 {
                    return Err(invalid(format!("{id}: CONNACK {h:#x} {body:x?}")));
                }
                let rmax = if v5 && body.len() > 2 {
                    receive_maximum(&body[2..])
                } else {
                    None
                };
                return Ok(Conn {
                    id,
                    s,
                    rd,
                    rmax,
                    v5,
                });
            }
        }
    }

    /// Subscribes to `filter` and waits for SUBACK.
    ///
    /// # Errors
    ///
    /// If the connection fails, the broker closes it, or SUBACK is
    /// malformed or refuses.
    pub fn subscribe(&mut self, filter: &str, qos: u8) -> io::Result<()> {
        let mut out = Vec::new();
        subscribe_packet(&mut out, filter, qos, self.v5);
        self.s.write_all(&out)?;
        loop {
            if self.rd.fill(&mut self.s)? == 0 {
                return Err(closed("SUBACK"));
            }
            if let Some((h, body)) = self.rd.next_packet() {
                let code = body.last().copied().unwrap_or(0x80);
                if h != 0x90 || code >= 0x80 {
                    return Err(invalid(format!("SUBACK {filter}: {h:#x} {code:#x}")));
                }
                return Ok(());
            }
        }
    }

    /// Sends DISCONNECT and waits for the broker to close the connection,
    /// so the session is offline when this returns.
    ///
    /// # Errors
    ///
    /// If the connection fails.
    pub fn disconnect(mut self) -> io::Result<()> {
        self.s.write_all(&[0xe0, 0])?;
        while self.rd.fill(&mut self.s)? > 0 {}
        Ok(())
    }
}

/// The Receive Maximum in CONNACK properties: the length, then the
/// properties.
fn receive_maximum(props: &[u8]) -> Option<u16> {
    let (len, n) = zzq_proto::varint::decode(props).ok().flatten()?;
    let p = Properties::try_from(props.get(n..n + len)?).ok()?;
    p.get_int(property::RECEIVE_MAXIMUM).map(|v| v as u16)
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::net::TcpListener;

    use super::*;

    #[test]
    fn a_connect_asks_for_a_keep_alive() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            let (mut s, _) = listener.accept().unwrap();
            let mut connect = [0; 12];
            s.read_exact(&mut connect).unwrap();
            connect
        });
        assert!(Conn::open(&addr, "c", false, Session::Clean).is_err());
        let connect = server.join().unwrap();
        assert_eq!(connect[..10], *b"\x10\x0d\x00\x04MQTT\x04\x02");
        assert_eq!(u16::from_be_bytes([connect[10], connect[11]]), 300);
    }
}
