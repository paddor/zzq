//! `quic://` MQTT over QUIC with quinn.
//!
//! One bidirectional stream per connection carries the MQTT byte stream, ALPN
//! `mqtt`. Certificates come from [`TlsOptions`]. A bind has one endpoint
//! per lane on a shared port. Quinn's tasks for a connection stay on the
//! lane that accepted or dialed it; the stream halves move to any lane.

use std::cell::Cell;
use std::fmt;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, ready};
use std::time::Duration;

use quinn::crypto::rustls::{QuicClientConfig, QuicServerConfig};
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::endpoint::Endpoint;
use crate::transport::Stream;
use crate::transport::tls::{self, TlsOptions};
use crate::{Error, Result};

const ALPN: &[u8] = b"mqtt";

/// QUIC keep-alive, below the idle timeout so live peers never time out.
/// MQTT keepalive still applies on top.
const KEEP_ALIVE: Duration = Duration::from_secs(15);
const IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Time a connection attempt has to finish its handshake and open the
/// MQTT stream.
const HANDSHAKE: Duration = Duration::from_secs(10);

/// Handshakes a lane runs at once. Past [`RETRY_ABOVE`], a client first
/// proves its address with a Retry, so spoofed sources cannot hold them;
/// past [`HANDSHAKES`], connections are refused.
const HANDSHAKES: usize = 1024;
const RETRY_ABOVE: usize = 128;

/// How long shutdown waits for the peer to acknowledge the last bytes.
const LINGER: Duration = Duration::from_secs(5);

/// Maps a quinn error to [`Error::Tls`], with `what` as the step.
fn failed<E: Into<crate::BoxError>>(what: &'static str) -> impl FnOnce(E) -> Error {
    move |e| Error::Tls {
        what,
        source: Some(e.into()),
    }
}

fn transport(server: bool) -> Arc<quinn::TransportConfig> {
    let mut t = quinn::TransportConfig::default();
    t.max_concurrent_bidi_streams(quinn::VarInt::from_u32(u32::from(server)))
        .max_concurrent_uni_streams(quinn::VarInt::from_u32(0))
        .datagram_receive_buffer_size(None)
        .keep_alive_interval(Some(KEEP_ALIVE));
    if let Ok(idle) = quinn::IdleTimeout::try_from(IDLE_TIMEOUT) {
        t.max_idle_timeout(Some(idle));
    }
    Arc::new(t)
}

fn alpn(o: &TlsOptions) -> Vec<Vec<u8>> {
    if o.alpn.is_empty() {
        vec![ALPN.to_vec()]
    } else {
        o.alpn.clone()
    }
}

fn server_config(o: &TlsOptions) -> Result<quinn::ServerConfig> {
    let mut tls = tls::server_config(o)?;
    tls.alpn_protocols = alpn(o);
    tls.max_early_data_size = 0;
    let crypto = QuicServerConfig::try_from(tls).map_err(failed("quic server config"))?;
    let mut config = quinn::ServerConfig::with_crypto(Arc::new(crypto));
    config.transport_config(transport(true)).migration(false);
    Ok(config)
}

fn client_config(o: &TlsOptions) -> Result<quinn::ClientConfig> {
    let mut tls = tls::client_config(o)?;
    tls.alpn_protocols = alpn(o);
    tls.enable_early_data = false;
    let crypto = QuicClientConfig::try_from(tls).map_err(failed("quic client config"))?;
    let mut config = quinn::ClientConfig::new(Arc::new(crypto));
    config.transport_config(transport(false));
    Ok(config)
}

async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>> {
    let addrs: Vec<_> = tokio::net::lookup_host((host, port)).await?.collect();
    if addrs.is_empty() {
        return Err(Error::Io(io::Error::other(format!("{host}: no address"))));
    }
    Ok(addrs)
}

/// An established QUIC connection and its MQTT stream.
pub struct QuicStream {
    recv: quinn::RecvStream,
    send: QuicWriter,
}

impl fmt::Debug for QuicStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicStream")
            .field("id", &self.send.conn.stable_id())
            .finish_non_exhaustive()
    }
}

impl QuicStream {
    pub(crate) const fn recv_mut(&mut self) -> &mut quinn::RecvStream {
        &mut self.recv
    }

    pub(crate) fn peer_cert(&self) -> Option<Vec<u8>> {
        let certs = self.send.conn.peer_identity()?;
        let certs = certs
            .downcast::<Vec<rustls::pki_types::CertificateDer<'static>>>()
            .ok()?;
        certs.first().map(|c| c.to_vec())
    }

    pub(crate) fn into_split(self) -> (quinn::RecvStream, QuicWriter) {
        (self.recv, self.send)
    }
}

type Linger = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

/// Send half. Shutdown sends FIN and waits until the peer acknowledges all
/// data, so closing the connection afterwards cannot drop the last packet.
pub struct QuicWriter {
    send: quinn::SendStream,
    conn: quinn::Connection,
    /// Keeps a client endpoint's socket alive.
    _endpoint: Option<quinn::Endpoint>,
    linger: Option<Linger>,
}

impl fmt::Debug for QuicWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicWriter")
            .field("id", &self.conn.stable_id())
            .finish_non_exhaustive()
    }
}

impl AsyncWrite for QuicWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        AsyncWrite::poll_write(Pin::new(&mut self.send), cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.linger.is_none() {
            let _ = self.send.finish();
            let stopped = self.send.stopped();
            self.linger = Some(Box::pin(async move {
                let _ = tokio::time::timeout(LINGER, stopped).await;
            }));
        }
        if let Some(linger) = self.linger.as_mut() {
            ready!(linger.as_mut().poll(cx));
        }
        Poll::Ready(Ok(()))
    }
}

/// A bound QUIC endpoint: one quinn endpoint per lane on `SO_REUSEPORT`
/// sockets, so the kernel spreads connections over the lanes. Each lane
/// runs the handshakes and the quinn tasks of its own connections.
pub struct QuicListener {
    lanes: Lanes,
    accepted: mpsc::UnboundedReceiver<Stream>,
    host: String,
    port: u16,
}

impl fmt::Debug for QuicListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QuicListener")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("lanes", &self.lanes.endpoints.len())
            .finish_non_exhaustive()
    }
}

/// The lanes' endpoints and accept tasks, stopped when dropped, also those
/// a bind that fails on a later lane started already.
#[derive(Default)]
struct Lanes {
    endpoints: Vec<quinn::Endpoint>,
    tasks: Vec<tokio::task::AbortHandle>,
}

impl Drop for Lanes {
    fn drop(&mut self) {
        for e in &self.endpoints {
            e.set_server_config(None);
        }
        for t in &self.tasks {
            t.abort();
        }
    }
}

impl QuicListener {
    pub(crate) async fn bind(
        ctx: &crate::Context,
        host: &str,
        port: u16,
        o: &TlsOptions,
    ) -> Result<QuicListener> {
        let config = server_config(o)?;
        let addr = resolve(host, port).await?[0];
        let in_use = |e: io::Error| match e.kind() {
            io::ErrorKind::AddrInUse => Error::AddressInUse {
                endpoint: format!("quic://{host}:{port}"),
                source: e,
            },
            _ => Error::Io(e),
        };
        // A plain socket first: SO_REUSEPORT would share a port that another
        // listener holds with it too, such as another QUIC listener.
        let bound = std::net::UdpSocket::bind(addr)
            .and_then(|probe| probe.local_addr())
            .map_err(in_use)?;
        let lanes = if cfg!(unix) { ctx.lanes() } else { 1 };
        let mut sockets = Vec::with_capacity(lanes);
        for _ in 0..lanes {
            sockets.push(reuse_socket(bound).map_err(in_use)?);
        }
        // Unbounded: each entry is a connection whose handshake took the
        // client round trips, and the listener's owner takes them as they
        // come.
        let (tx, accepted) = mpsc::unbounded_channel();
        let mut started = Lanes::default();
        for (i, socket) in sockets.into_iter().enumerate() {
            let (config, tx) = (config.clone(), tx.clone());
            let (endpoint, task) = ctx
                .lane(i)
                .call(move || -> io::Result<_> {
                    let runtime = Arc::new(quinn::TokioRuntime);
                    let ep = quinn::Endpoint::new(
                        quinn::EndpointConfig::default(),
                        Some(config),
                        socket,
                        runtime,
                    )?;
                    let task = tokio::task::spawn_local(accept_on_lane(ep.clone(), tx));
                    Ok((ep, task.abort_handle()))
                })
                .await??;
            started.endpoints.push(endpoint);
            started.tasks.push(task);
        }
        Ok(QuicListener {
            lanes: started,
            accepted,
            host: host.to_owned(),
            port: bound.port(),
        })
    }

    /// The next connection that finished its handshake on some lane.
    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        self.accepted.recv().await.ok_or(Error::Closed)
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        Endpoint::Quic {
            host: self.host.clone(),
            port: self.port,
        }
    }
}

/// What becomes of a connection attempt.
#[derive(Debug, PartialEq, Eq)]
enum Admit {
    Accept,
    Retry,
    Refuse,
}

/// What becomes of an attempt from a `validated` address while
/// `in_flight` handshakes run.
const fn admit(in_flight: usize, validated: bool) -> Admit {
    if in_flight >= HANDSHAKES {
        Admit::Refuse
    } else if in_flight >= RETRY_ABOVE && !validated {
        Admit::Retry
    } else {
        Admit::Accept
    }
}

/// Counts a handshake of a lane while it runs.
struct Handshaking(Rc<Cell<usize>>);

impl Handshaking {
    fn new(count: &Rc<Cell<usize>>) -> Handshaking {
        count.set(count.get() + 1);
        Handshaking(count.clone())
    }
}

impl Drop for Handshaking {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

/// Accepts on one lane's endpoint. Handshakes run as their own tasks, so a
/// slow client does not hold up the others.
async fn accept_on_lane(endpoint: quinn::Endpoint, tx: mpsc::UnboundedSender<Stream>) {
    let in_flight = Rc::new(Cell::new(0));
    while let Some(incoming) = endpoint.accept().await {
        match admit(in_flight.get(), incoming.remote_address_validated()) {
            Admit::Refuse => incoming.refuse(),
            // An attempt that cannot retry is refused when dropped.
            Admit::Retry => drop(incoming.retry()),
            Admit::Accept => {
                let (tx, counted) = (tx.clone(), Handshaking::new(&in_flight));
                tokio::task::spawn_local(async move {
                    let _counted = counted;
                    if let Ok(Ok(s)) = tokio::time::timeout(HANDSHAKE, handshake(incoming)).await {
                        let _ = tx.send(s);
                    }
                });
            }
        }
        // quinn's accept takes nothing from the task budget, so a burst
        // of attempts would not let the lane's other tasks run.
        tokio::task::consume_budget().await;
    }
}

/// A UDP socket that shares its port with the other lanes' sockets.
fn reuse_socket(addr: SocketAddr) -> io::Result<std::net::UdpSocket> {
    use socket2::{Domain, Protocol, Socket, Type};
    let s = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
    #[cfg(unix)]
    s.set_reuse_port(true)?;
    s.set_nonblocking(true)?;
    s.bind(&addr.into())?;
    Ok(s.into())
}

/// Completes a server handshake on the current lane and accepts the MQTT
/// stream.
async fn handshake(incoming: quinn::Incoming) -> Result<Stream> {
    let conn = incoming.await.map_err(failed("quic handshake"))?;
    let (send, recv) = conn.accept_bi().await.map_err(failed("quic stream"))?;
    Ok(Stream::Quic(Box::new(QuicStream {
        recv,
        send: QuicWriter {
            send,
            conn,
            _endpoint: None,
            linger: None,
        },
    })))
}

/// Dials `host:port` from a fresh client endpoint on the current lane.
pub(crate) async fn connect(host: &str, port: u16, o: &TlsOptions) -> Result<Stream> {
    let config = client_config(o)?;
    let name = o.server_name.as_deref().unwrap_or(host);
    let mut last = None;
    for addr in resolve(host, port).await? {
        let local: SocketAddr = if addr.is_ipv6() {
            "[::]:0".parse().map_err(failed("quic local address"))?
        } else {
            "0.0.0.0:0".parse().map_err(failed("quic local address"))?
        };
        let endpoint = quinn::Endpoint::client(local)?;
        let attempt = async {
            let conn = endpoint
                .connect_with(config.clone(), addr, name)
                .map_err(failed("quic connect"))?
                .await
                .map_err(|e| match e {
                    quinn::ConnectionError::ConnectionClosed(_)
                    | quinn::ConnectionError::TimedOut => Error::Refused {
                        endpoint: format!("quic://{host}:{port}"),
                        source: Some(e.into()),
                    },
                    other => failed("quic handshake")(other),
                })?;
            let (send, recv) = conn.open_bi().await.map_err(failed("quic stream"))?;
            Ok::<_, Error>((conn, send, recv))
        };
        match attempt.await {
            Ok((conn, send, recv)) => {
                return Ok(Stream::Quic(Box::new(QuicStream {
                    recv,
                    send: QuicWriter {
                        send,
                        conn,
                        _endpoint: Some(endpoint),
                        linger: None,
                    },
                })));
            }
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or(Error::Closed))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tls() -> TlsOptions {
        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
        TlsOptions {
            cert_chain_pem: Some(key.cert.pem().into_bytes()),
            key_pem: Some(key.signing_key.serialize_pem().into_bytes()),
            ..TlsOptions::default()
        }
    }

    #[test]
    fn unproven_sources_retry_and_too_many_handshakes_are_refused() {
        assert_eq!(admit(0, false), Admit::Accept);
        assert_eq!(admit(RETRY_ABOVE, true), Admit::Accept);
        assert_eq!(admit(RETRY_ABOVE, false), Admit::Retry);
        assert_eq!(admit(HANDSHAKES, true), Admit::Refuse);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_bind_that_fails_on_a_later_lane_lets_go_of_the_port() {
        let ctx = crate::Context::with_config(&crate::Config {
            io_threads: 2,
            ..crate::Config::default()
        })
        .unwrap();
        ctx.stop_lane(1);
        while ctx.lane(1).call(|| ()).await.is_ok() {
            tokio::task::yield_now().await;
        }
        let port = std::net::UdpSocket::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let bound = QuicListener::bind(&ctx, "127.0.0.1", port, &tls()).await;
        assert!(bound.is_err(), "lane 1 is stopped");
        // Lane 0 lets go once it ran the aborts.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
        while std::net::UdpSocket::bind(("127.0.0.1", port)).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "lane 0 holds the port"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }
}
