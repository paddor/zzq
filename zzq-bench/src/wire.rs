//! Blocking raw MQTT client pieces.

use std::io::{self, Read, Write};
use std::net::TcpStream;

use zzq_proto::encode::{self, ConnectFields, PublishHeader};
use zzq_proto::packet::fixed_header;
use zzq_proto::property::{self, Properties};
use zzq_proto::{QoS, SubscriptionOptions, Version};

pub const PUBACK: u8 = 0x40;
pub const PUBREC: u8 = 0x50;
pub const PUBREL: u8 = 0x62;
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
        Reader {
            buf: vec![0; 1 << 20],
            len: 0,
            pos: 0,
        }
    }
}

impl Reader {
    /// Reads more bytes. `Ok(0)` means the peer closed.
    pub fn fill<R: Read>(&mut self, s: &mut R) -> io::Result<usize> {
        if self.pos > 0 {
            self.buf.copy_within(self.pos..self.len, 0);
            self.len -= self.pos;
            self.pos = 0;
        }
        if self.len == self.buf.len() {
            self.buf.resize(self.buf.len() * 2, 0);
        }
        let k = s.read(&mut self.buf[self.len..])?;
        self.len += k;
        Ok(k)
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
    pub topic: &'a str,
    pub qos: u8,
    pub pkid: u16,
    pub v5: bool,
}

/// Encodes a PUBLISH.
pub fn publish(out: &mut Vec<u8>, head: Head<'_>, payload: &[u8]) {
    let h = PublishHeader {
        version: version(head.v5),
        dup: false,
        qos: QoS::from_u8(head.qos).unwrap_or_default(),
        retain: false,
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

/// Connects and waits for CONNACK. Returns the v5 Receive Maximum.
pub fn connect(
    addr: &str,
    client_id: &str,
    v5: bool,
) -> io::Result<(TcpStream, Reader, Option<u16>)> {
    let mut s = TcpStream::connect(addr)?;
    s.set_nodelay(true)?;
    let mut out = Vec::new();
    let c = ConnectFields {
        version: version(v5),
        clean_start: true,
        keep_alive: 0,
        properties: &[],
        client_id,
        will: None,
        username: None,
        password: None,
    };
    encode::connect(&c, &mut out);
    s.write_all(&out)?;
    let mut rd = Reader::default();
    loop {
        if rd.fill(&mut s)? == 0 {
            return Err(closed("CONNACK"));
        }
        if let Some((h, body)) = rd.next_packet() {
            if h != 0x20 || body.len() < 2 || body[1] != 0 {
                return Err(invalid(format!("{client_id}: CONNACK {h:#x} {body:x?}")));
            }
            let rmax = if v5 && body.len() > 2 {
                zzq_proto::varint::decode(&body[2..])
                    .ok()
                    .flatten()
                    .and_then(|(len, n)| Properties::parse(body.get(2 + n..2 + n + len)?).ok())
                    .and_then(|p| p.get_int(property::RECEIVE_MAXIMUM))
                    .map(|v| v as u16)
            } else {
                None
            };
            return Ok((s, rd, rmax));
        }
    }
}

/// Subscribes and waits for SUBACK.
pub fn subscribe(
    s: &mut TcpStream,
    rd: &mut Reader,
    filter: &str,
    qos: u8,
    v5: bool,
) -> io::Result<()> {
    let mut out = Vec::new();
    let opts = SubscriptionOptions::qos(QoS::from_u8(qos).unwrap_or_default());
    encode::subscribe(version(v5), 1, &[], &[(filter, opts)], &mut out);
    s.write_all(&out)?;
    loop {
        if rd.fill(s)? == 0 {
            return Err(closed("SUBACK"));
        }
        if let Some((h, body)) = rd.next_packet() {
            let code = body.last().copied().unwrap_or(0x80);
            if h != 0x90 || code >= 0x80 {
                return Err(invalid(format!("SUBACK {filter}: {h:#x} {code:#x}")));
            }
            return Ok(());
        }
    }
}
