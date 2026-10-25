//! `ws://` and `wss://` MQTT over WebSocket (subprotocol `mqtt`).
//!
//! After the HTTP upgrade, a pump task on the current lane moves bytes
//! between binary frames and an in-memory stream, so the MQTT side sees a
//! plain byte stream that can move to any lane.

use std::hash::{BuildHasher, RandomState};
use std::io;
use std::sync::Arc;
use std::time::Instant;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};
use tokio::sync::mpsc;

use crate::endpoint::Endpoint;
use crate::transport::{Stream, tcp};
use crate::{Error, Result};

const GUID: &[u8] = b"258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// Largest HTTP upgrade request or response.
const MAX_HEADERS: usize = 8 << 10;
/// Largest frame payload accepted.
const MAX_FRAME: usize = 16 << 20;
/// Buffer of the in-memory stream, each way.
const PIPE_BYTES: usize = 256 << 10;
/// Subprotocols accepted, in preference order.
const SUBPROTOCOLS: [&str; 2] = ["mqtt", "mqttv3.1"];

const OP_CONTINUATION: u8 = 0x0;
const OP_TEXT: u8 = 0x1;
const OP_BINARY: u8 = 0x2;
const OP_CLOSE: u8 = 0x8;
const OP_PING: u8 = 0x9;
const OP_PONG: u8 = 0xa;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Role {
    Client,
    Server,
}

/// Bind settings shared by the connections of one WebSocket listener.
pub struct WsServer {
    path: String,
    #[cfg(feature = "tls")]
    tls: Option<tokio_rustls::TlsAcceptor>,
}

impl std::fmt::Debug for WsServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsServer")
            .field("path", &self.path)
            .finish_non_exhaustive()
    }
}

/// A bound `ws://` or `wss://` endpoint.
#[derive(Debug)]
pub struct WsListener {
    tcp: tcp::TcpListener,
    server: Arc<WsServer>,
    secure: bool,
}

impl WsListener {
    pub(crate) async fn bind(
        host: &str,
        port: u16,
        path: &str,
        tls: Option<&super::TransportOptions>,
    ) -> Result<WsListener> {
        #[cfg(feature = "tls")]
        let acceptor = match tls {
            Some(opts) => {
                let o = opts.tls.as_ref().ok_or(Error::Tls {
                    what: "wss:// needs TransportOptions::tls",
                    source: None,
                })?;
                Some(super::tls::acceptor(o)?)
            }
            None => None,
        };
        #[cfg(not(feature = "tls"))]
        if tls.is_some() {
            return Err(Error::Unsupported("wss"));
        }
        Ok(WsListener {
            tcp: tcp::TcpListener::bind(host, port).await?,
            secure: tls.is_some(),
            server: Arc::new(WsServer {
                path: path.to_owned(),
                #[cfg(feature = "tls")]
                tls: acceptor,
            }),
        })
    }

    /// Accepts TCP. The upgrade runs when the stream is first used.
    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        match self.tcp.accept().await? {
            Stream::Tcp(s) => Ok(Stream::WsAccept(s, self.server.clone())),
            other => Ok(other),
        }
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        let (host, port) = match self.tcp.endpoint() {
            Endpoint::Tcp { host, port } => (host, port),
            _ => (String::new(), 0),
        };
        let path = self.server.path.clone();
        if self.secure {
            Endpoint::Wss { host, port, path }
        } else {
            Endpoint::Ws { host, port, path }
        }
    }
}

/// Runs the server side of an accepted connection: TLS if configured, the
/// HTTP upgrade, then the pump.
pub(crate) async fn accept(s: std::net::TcpStream, server: &WsServer) -> Result<Stream> {
    let tcp = tcp::into_async(s)?;
    #[cfg(feature = "tls")]
    if let Some(acceptor) = &server.tls {
        let tls = acceptor.accept(tcp).await?;
        return upgrade_server(tls, &server.path).await;
    }
    upgrade_server(tcp, &server.path).await
}

/// Connects `ws://` (no TLS) or `wss://`.
pub(crate) async fn connect(
    host: &str,
    port: u16,
    path: &str,
    tls: Option<&super::TransportOptions>,
) -> Result<Stream> {
    match tls {
        #[cfg(feature = "tls")]
        Some(opts) => {
            let default = super::tls::TlsOptions::default();
            let tls = super::tls::connect_stream(host, port, opts.tls.as_ref().unwrap_or(&default))
                .await?;
            upgrade_client(tls, host, path).await
        }
        #[cfg(not(feature = "tls"))]
        Some(_) => Err(Error::Unsupported("wss")),
        None => {
            let tcp = tokio::net::TcpStream::connect((host, port)).await?;
            tcp.set_nodelay(true)?;
            upgrade_client(tcp, host, path).await
        }
    }
}

fn protocol_error(why: &str) -> Error {
    Error::Io(io::Error::new(
        io::ErrorKind::InvalidData,
        format!("websocket: {why}"),
    ))
}

/// Reads HTTP headers up to the blank line.
async fn read_head<S: AsyncRead + Unpin>(s: &mut S) -> Result<String> {
    let mut head = Vec::with_capacity(512);
    let mut byte = [0u8; 1];
    while !head.ends_with(b"\r\n\r\n") {
        if head.len() >= MAX_HEADERS {
            return Err(protocol_error("headers too large"));
        }
        if s.read(&mut byte).await? == 0 {
            return Err(Error::Closed);
        }
        head.push(byte[0]);
    }
    String::from_utf8(head).map_err(|_| protocol_error("headers not UTF-8"))
}

fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().skip(1).find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// The key and subprotocol of a valid upgrade request for `path`.
fn check_upgrade<'a>(head: &'a str, path: &str) -> Option<(&'a str, &'static str)> {
    let mut request = head.lines().next().unwrap_or_default().split_whitespace();
    let (method, target) = (request.next(), request.next());
    let upgrade = header(head, "upgrade").is_some_and(|v| v.eq_ignore_ascii_case("websocket"));
    let version_ok = header(head, "sec-websocket-version") == Some("13");
    let offered = header(head, "sec-websocket-protocol").unwrap_or_default();
    let protocol = SUBPROTOCOLS
        .into_iter()
        .find(|p| offered.split(',').any(|o| o.trim() == *p))?;
    let target_ok = target.is_some_and(|t| t.split('?').next() == Some(path));
    let ok = method == Some("GET") && target_ok && upgrade && version_ok;
    ok.then_some((header(head, "sec-websocket-key")?, protocol))
}

/// Reads a server-side upgrade request and the client frames after it
/// from `data`, as a lane would from a socket. For the fuzz targets.
#[cfg(feature = "fuzzing")]
pub(crate) async fn fuzz_server(mut data: &[u8]) {
    let Ok(head) = read_head(&mut data).await else {
        return;
    };
    if let Some((key, _)) = check_upgrade(&head, "/mqtt") {
        let _ = accept_key(key);
    }
    let mut payload = Vec::new();
    while let Ok(Some(_)) = read_frame(&mut data, true, &mut payload).await {}
}

async fn upgrade_server<S>(mut s: S, path: &str) -> Result<Stream>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let head = read_head(&mut s).await?;
    let Some((key, protocol)) = check_upgrade(&head, path) else {
        let _ = s
            .write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await;
        return Err(protocol_error("bad upgrade request"));
    };
    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {}\r\nSec-WebSocket-Protocol: {protocol}\r\n\r\n",
        accept_key(key)
    );
    s.write_all(response.as_bytes()).await?;
    s.flush().await?;
    Ok(Stream::Inproc(pump(s, Role::Server)))
}

async fn upgrade_client<S>(mut s: S, host: &str, path: &str) -> Result<Stream>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let key = base64(&random_u128().to_le_bytes());
    let request = format!(
        "GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Protocol: mqtt\r\n\r\n"
    );
    s.write_all(request.as_bytes()).await?;
    s.flush().await?;
    let head = read_head(&mut s).await?;
    let status_ok = head
        .lines()
        .next()
        .is_some_and(|l| l.split_whitespace().nth(1) == Some("101"));
    if !status_ok || header(&head, "sec-websocket-accept") != Some(accept_key(&key).as_str()) {
        return Err(protocol_error("upgrade refused"));
    }
    Ok(Stream::Inproc(pump(s, Role::Client)))
}

/// Starts the frame pump on the current lane and returns the byte stream.
fn pump<S>(ws: S, role: Role) -> DuplexStream
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
{
    let (local, remote) = tokio::io::duplex(PIPE_BYTES);
    let (ws_r, ws_w) = tokio::io::split(ws);
    let (pipe_r, pipe_w) = tokio::io::split(local);
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
    tokio::task::spawn_local(frames_in(ws_r, pipe_w, ctl_tx, role));
    tokio::task::spawn_local(frames_out(ws_w, pipe_r, ctl_rx, role));
    remote
}

/// WebSocket frames to bytes. Control frames go to the writer.
async fn frames_in<R, W>(
    mut ws: R,
    mut pipe: W,
    ctl: mpsc::UnboundedSender<(u8, Vec<u8>)>,
    role: Role,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut payload = Vec::new();
    while let Ok(Some(opcode)) = read_frame(&mut ws, role == Role::Server, &mut payload).await {
        match opcode {
            OP_BINARY | OP_CONTINUATION => {
                if pipe.write_all(&payload).await.is_err() {
                    break;
                }
            }
            OP_PING => {
                let _ = ctl.send((OP_PONG, std::mem::take(&mut payload)));
            }
            OP_PONG => {}
            _ => break,
        }
    }
    let _ = ctl.send((OP_CLOSE, Vec::new()));
    let _ = pipe.shutdown().await;
}

/// Bytes to binary frames, plus control frames from the reader.
async fn frames_out<W, R>(
    mut ws: W,
    mut pipe: R,
    mut ctl: mpsc::UnboundedReceiver<(u8, Vec<u8>)>,
    role: Role,
) where
    W: AsyncWrite + Unpin,
    R: AsyncRead + Unpin,
{
    let mut buf = vec![0u8; 64 << 10];
    let mut frame = Vec::with_capacity(buf.len() + 14);
    loop {
        tokio::select! {
            n = pipe.read(&mut buf) => {
                let n = match n {
                    Ok(0) | Err(_) => break,
                    Ok(n) => n,
                };
                frame.clear();
                write_frame(&mut frame, OP_BINARY, &buf[..n], role == Role::Client);
                // Flush: over TLS a write may only reach the TLS buffer.
                if ws.write_all(&frame).await.is_err() || ws.flush().await.is_err() {
                    return;
                }
            }
            c = ctl.recv() => {
                let Some((opcode, data)) = c else { break };
                frame.clear();
                write_frame(&mut frame, opcode, &data, role == Role::Client);
                let sent = ws.write_all(&frame).await.is_ok() && ws.flush().await.is_ok();
                if !sent || opcode == OP_CLOSE {
                    let _ = ws.shutdown().await;
                    return;
                }
            }
        }
    }
    frame.clear();
    write_frame(&mut frame, OP_CLOSE, &[], role == Role::Client);
    let _ = ws.write_all(&frame).await;
    let _ = ws.shutdown().await;
}

/// Reads one frame into `payload`, unmasked. `Ok(None)` at a clean end.
async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    want_masked: bool,
    payload: &mut Vec<u8>,
) -> io::Result<Option<u8>> {
    let mut h = [0u8; 2];
    match r.read_exact(&mut h).await {
        Ok(_) => {}
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let bad = |why: &str| io::Error::new(io::ErrorKind::InvalidData, why.to_owned());
    let opcode = h[0] & 0x0f;
    let masked = h[1] & 0x80 != 0;
    if h[0] & 0x70 != 0 || masked != want_masked || opcode == OP_TEXT {
        return Err(bad("unsupported frame"));
    }
    let len = match h[1] & 0x7f {
        126 => usize::from(r.read_u16().await?),
        127 => usize::try_from(r.read_u64().await?).unwrap_or(usize::MAX),
        n => usize::from(n),
    };
    if len > MAX_FRAME {
        return Err(bad("frame too large"));
    }
    let mut mask = [0u8; 4];
    if masked {
        r.read_exact(&mut mask).await?;
    }
    payload.resize(len, 0);
    r.read_exact(payload).await?;
    if masked {
        for (i, b) in payload.iter_mut().enumerate() {
            *b ^= mask[i & 3];
        }
    }
    Ok(Some(opcode))
}

fn write_frame(out: &mut Vec<u8>, opcode: u8, payload: &[u8], masked: bool) {
    out.push(0x80 | opcode);
    let mask_bit = if masked { 0x80 } else { 0 };
    let len = payload.len();
    if len < 126 {
        out.push(mask_bit | len as u8);
    } else if let Ok(short) = u16::try_from(len) {
        out.push(mask_bit | 0x7e);
        out.extend_from_slice(&short.to_be_bytes());
    } else {
        out.push(mask_bit | 0x7f);
        out.extend_from_slice(&(len as u64).to_be_bytes());
    }
    if masked {
        let mask = (random_u128() as u32).to_be_bytes();
        out.extend_from_slice(&mask);
        out.extend(payload.iter().enumerate().map(|(i, b)| b ^ mask[i & 3]));
    } else {
        out.extend_from_slice(payload);
    }
}

fn random_u128() -> u128 {
    let a = RandomState::new().hash_one(Instant::now());
    let b = RandomState::new().hash_one(a);
    (u128::from(a) << 64) | u128::from(b)
}

fn accept_key(key: &str) -> String {
    let mut input = Vec::with_capacity(key.len() + GUID.len());
    input.extend_from_slice(key.as_bytes());
    input.extend_from_slice(GUID);
    base64(&sha1(&input))
}

#[expect(
    clippy::many_single_char_names,
    reason = "SHA-1 names its working variables a to e"
)]
fn sha1(data: &[u8]) -> [u8; 20] {
    let mut h: [u32; 5] = [
        0x6745_2301,
        0xEFCD_AB89,
        0x98BA_DCFE,
        0x1032_5476,
        0xC3D2_E1F0,
    ];
    let bit_len = (data.len() as u64) * 8;
    let mut padded = data.to_vec();
    padded.push(0x80);
    while padded.len() % 64 != 56 {
        padded.push(0);
    }
    padded.extend_from_slice(&bit_len.to_be_bytes());
    for chunk in padded.as_chunks::<64>().0 {
        let mut w = [0u32; 80];
        for (i, word) in w[..16].iter_mut().enumerate() {
            let b = &chunk[i * 4..i * 4 + 4];
            *word = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let [mut a, mut b, mut c, mut d, mut e] = h;
        for (i, wi) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A82_7999_u32),
                20..=39 => (b ^ c ^ d, 0x6ED9_EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1B_BCDC),
                _ => (b ^ c ^ d, 0xCA62_C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(*wi);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        for (hv, v) in h.iter_mut().zip([a, b, c, d, e]) {
            *hv = hv.wrapping_add(v);
        }
    }
    let mut out = [0u8; 20];
    for (i, val) in h.iter().enumerate() {
        out[i * 4..i * 4 + 4].copy_from_slice(&val.to_be_bytes());
    }
    out
}

fn base64(data: &[u8]) -> String {
    const CHARS: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(char::from(CHARS[(n >> 18) as usize & 63]));
        out.push(char::from(CHARS[(n >> 12) as usize & 63]));
        out.push(if chunk.len() > 1 {
            char::from(CHARS[(n >> 6) as usize & 63])
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            char::from(CHARS[n as usize & 63])
        } else {
            '='
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accept_key_matches_rfc_example() {
        assert_eq!(
            accept_key("dGhlIHNhbXBsZSBub25jZQ=="),
            "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
        );
    }

    #[test]
    fn frame_header_grows_with_payload_length() {
        for len in [0, 125, 126, 65_535, 65_536] {
            let payload = vec![7u8; len];
            let mut out = Vec::new();
            write_frame(&mut out, OP_BINARY, &payload, false);
            let header = match len {
                n if n < 126 => 2,
                n if n <= 65_535 => 4,
                _ => 10,
            };
            assert_eq!(out.len(), header + len);
        }
    }
}
