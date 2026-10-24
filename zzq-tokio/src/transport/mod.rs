//! Transports.
//!
//! Each transport lives in its own module and produces a [`Stream`]: a plain
//! socket that can move to any lane. A lane turns it into async read and
//! write halves with [`Stream::drive`], which keeps static dispatch.

pub mod inproc;
#[cfg(unix)]
pub mod ipc;
#[cfg(feature = "quic")]
pub mod quic;
pub mod tcp;
#[cfg(feature = "tls")]
pub mod tls;
pub mod udp;
pub mod ws;

use bytes::BytesMut;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};

use crate::{Context, Endpoint, Error, Result};

/// Per-transport settings for binding and connecting.
#[derive(Debug, Clone, Default)]
pub struct TransportOptions {
    /// Certificates and keys for `tls://`.
    #[cfg(feature = "tls")]
    pub tls: Option<tls::TlsOptions>,
}

/// An established connection, not yet bound to a lane.
pub enum Stream {
    Tcp(std::net::TcpStream),
    #[cfg(unix)]
    Ipc(std::os::unix::net::UnixStream),
    Inproc(tokio::io::DuplexStream),
    /// TLS after the handshake.
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::TlsStream<tcp::LaneTcp>>),
    /// Accepted TCP connection whose TLS handshake has not run yet.
    #[cfg(feature = "tls")]
    TlsAccept(std::net::TcpStream, tokio_rustls::TlsAcceptor),
    /// QUIC after the handshake.
    #[cfg(feature = "quic")]
    Quic(Box<quic::QuicStream>),
    /// Accepted TCP connection whose WebSocket upgrade has not run yet.
    WsAccept(std::net::TcpStream, std::sync::Arc<ws::WsServer>),
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self {
            Stream::Tcp(_) => "tcp",
            #[cfg(unix)]
            Stream::Ipc(_) => "ipc",
            Stream::Inproc(_) => "inproc",
            #[cfg(feature = "tls")]
            Stream::Tls(_) | Stream::TlsAccept(..) => "tls",
            #[cfg(feature = "quic")]
            Stream::Quic(_) => "quic",
            Stream::WsAccept(..) => "ws",
        };
        f.debug_tuple("Stream").field(&kind).finish()
    }
}

/// Consumes the read and write halves of a [`Stream`] on a lane.
pub trait Driver {
    type Output;

    /// Runs the connection.
    fn run<R, W>(self, reader: R, writer: W) -> impl Future<Output = Self::Output>
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static;
}

impl Stream {
    /// Reads into `buf` until `done` returns true, without binding the stream
    /// to the current lane, so it can still move to another one.
    pub async fn prefetch(
        self,
        buf: &mut BytesMut,
        mut done: impl FnMut(&[u8]) -> bool,
    ) -> Result<Stream> {
        match self {
            Stream::Tcp(s) => {
                let mut s = tcp::into_async(s)?;
                fill(&mut s, buf, &mut done).await?;
                Ok(Stream::Tcp(s.into_std()?))
            }
            #[cfg(unix)]
            Stream::Ipc(s) => {
                let mut s = ipc::into_async(s)?;
                fill(&mut s, buf, &mut done).await?;
                Ok(Stream::Ipc(s.into_std()?))
            }
            Stream::Inproc(mut s) => {
                fill(&mut s, buf, &mut done).await?;
                Ok(Stream::Inproc(s))
            }
            #[cfg(feature = "tls")]
            Stream::Tls(mut s) => {
                fill(&mut *s, buf, &mut done).await?;
                // Let the lane that drives the stream register it.
                s.get_mut().0.park()?;
                Ok(Stream::Tls(s))
            }
            #[cfg(feature = "tls")]
            Stream::TlsAccept(s, acceptor) => {
                Box::pin(async move {
                    tls::handshake(s, &acceptor)
                        .await?
                        .prefetch(buf, done)
                        .await
                })
                .await
            }
            #[cfg(feature = "quic")]
            Stream::Quic(mut s) => {
                fill(s.recv_mut(), buf, &mut done).await?;
                Ok(Stream::Quic(s))
            }
            Stream::WsAccept(s, server) => {
                Box::pin(async move { ws::accept(s, &server).await?.prefetch(buf, done).await })
                    .await
            }
        }
    }

    /// DER of the certificate the peer presented over TLS or QUIC.
    pub fn peer_cert(&self) -> Option<Vec<u8>> {
        match self {
            #[cfg(feature = "tls")]
            Stream::Tls(s) => s
                .get_ref()
                .1
                .peer_certificates()?
                .first()
                .map(|c| c.to_vec()),
            #[cfg(feature = "quic")]
            Stream::Quic(s) => s.peer_cert(),
            _ => None,
        }
    }

    /// Splits the stream on the current lane and runs `driver` on it.
    pub async fn drive<D: Driver>(self, driver: D) -> Result<D::Output> {
        match self {
            Stream::Tcp(s) => {
                let (r, w) = tcp::into_async(s)?.into_split();
                Ok(driver.run(r, w).await)
            }
            #[cfg(unix)]
            Stream::Ipc(s) => {
                let (r, w) = ipc::into_async(s)?.into_split();
                Ok(driver.run(r, w).await)
            }
            Stream::Inproc(s) => {
                let (r, w) = tokio::io::split(s);
                Ok(driver.run(r, w).await)
            }
            #[cfg(feature = "tls")]
            Stream::Tls(mut s) => {
                s.get_mut().0.unpark()?;
                let (r, w) = tokio::io::split(*s);
                Ok(driver.run(r, w).await)
            }
            #[cfg(feature = "tls")]
            Stream::TlsAccept(s, acceptor) => {
                Box::pin(async move { tls::handshake(s, &acceptor).await?.drive(driver).await })
                    .await
            }
            #[cfg(feature = "quic")]
            Stream::Quic(s) => {
                let (r, w) = s.into_split();
                Ok(driver.run(r, w).await)
            }
            Stream::WsAccept(s, server) => {
                Box::pin(async move { ws::accept(s, &server).await?.drive(driver).await }).await
            }
        }
    }
}

/// Reads into `buf` until `done` returns true.
async fn fill<S: AsyncRead + Unpin>(
    s: &mut S,
    buf: &mut BytesMut,
    done: &mut impl FnMut(&[u8]) -> bool,
) -> Result<()> {
    while !done(buf) {
        buf.reserve(1024);
        if s.read_buf(buf).await? == 0 {
            return Err(Error::Closed);
        }
    }
    Ok(())
}

/// A bound endpoint.
#[derive(Debug)]
pub enum Listener {
    Tcp(tcp::TcpListener),
    #[cfg(unix)]
    Ipc(ipc::IpcListener),
    Inproc(inproc::InprocListener),
    #[cfg(feature = "tls")]
    Tls(tls::TlsListener),
    #[cfg(feature = "quic")]
    Quic(quic::QuicListener),
    Ws(ws::WsListener),
}

impl Listener {
    /// Binds `endpoint`.
    pub async fn bind(
        ctx: &Context,
        endpoint: &Endpoint,
        opts: &TransportOptions,
    ) -> Result<Listener> {
        let _ = opts;
        match endpoint {
            #[cfg(feature = "tls")]
            Endpoint::Tls { host, port } => {
                let tls = opts
                    .tls
                    .as_ref()
                    .ok_or(Error::Tls("tls:// needs TransportOptions::tls".to_owned()))?;
                Ok(Listener::Tls(
                    tls::TlsListener::bind(host, *port, tls).await?,
                ))
            }
            #[cfg(feature = "quic")]
            Endpoint::Quic { host, port } => {
                let tls = opts
                    .tls
                    .as_ref()
                    .ok_or(Error::Tls("quic:// needs TransportOptions::tls".to_owned()))?;
                Ok(Listener::Quic(
                    quic::QuicListener::bind(ctx, host, *port, tls).await?,
                ))
            }
            Endpoint::Ws { host, port, path } => Ok(Listener::Ws(
                ws::WsListener::bind(host, *port, path, None).await?,
            )),
            Endpoint::Wss { host, port, path } => Ok(Listener::Ws(
                ws::WsListener::bind(host, *port, path, Some(opts)).await?,
            )),
            Endpoint::Tcp { host, port } => {
                Ok(Listener::Tcp(tcp::TcpListener::bind(host, *port).await?))
            }
            #[cfg(unix)]
            Endpoint::Ipc(path) => Ok(Listener::Ipc(ipc::IpcListener::bind(path)?)),
            Endpoint::Inproc(name) => {
                Ok(Listener::Inproc(inproc::InprocListener::bind(ctx, name)?))
            }
            other => Err(unsupported(other)),
        }
    }

    /// Waits for the next connection.
    pub async fn accept(&mut self) -> Result<Stream> {
        match self {
            Listener::Tcp(l) => l.accept().await,
            #[cfg(unix)]
            Listener::Ipc(l) => l.accept().await,
            Listener::Inproc(l) => l.accept().await,
            #[cfg(feature = "tls")]
            Listener::Tls(l) => l.accept().await,
            #[cfg(feature = "quic")]
            Listener::Quic(l) => l.accept().await,
            Listener::Ws(l) => l.accept().await,
        }
    }

    /// The bound endpoint, with the actual port for `tcp://...:0`.
    pub fn endpoint(&self) -> Endpoint {
        match self {
            Listener::Tcp(l) => l.endpoint(),
            #[cfg(unix)]
            Listener::Ipc(l) => l.endpoint(),
            Listener::Inproc(l) => l.endpoint(),
            #[cfg(feature = "tls")]
            Listener::Tls(l) => l.endpoint(),
            #[cfg(feature = "quic")]
            Listener::Quic(l) => l.endpoint(),
            Listener::Ws(l) => l.endpoint(),
        }
    }
}

/// Connects to `endpoint`.
pub async fn connect(
    ctx: &Context,
    endpoint: &Endpoint,
    opts: &TransportOptions,
) -> Result<Stream> {
    let _ = opts;
    match endpoint {
        #[cfg(feature = "tls")]
        Endpoint::Tls { host, port } => {
            let default = tls::TlsOptions::default();
            tls::connect(host, *port, opts.tls.as_ref().unwrap_or(&default)).await
        }
        #[cfg(feature = "quic")]
        Endpoint::Quic { host, port } => {
            let default = tls::TlsOptions::default();
            quic::connect(host, *port, opts.tls.as_ref().unwrap_or(&default)).await
        }
        Endpoint::Ws { host, port, path } => ws::connect(host, *port, path, None).await,
        Endpoint::Wss { host, port, path } => ws::connect(host, *port, path, Some(opts)).await,
        Endpoint::Tcp { host, port } => tcp::connect(host, *port).await,
        #[cfg(unix)]
        Endpoint::Ipc(path) => ipc::connect(path).await,
        Endpoint::Inproc(name) => inproc::connect(ctx, name).await,
        other => Err(unsupported(other)),
    }
}

fn unsupported(endpoint: &Endpoint) -> Error {
    Error::Unsupported(match endpoint {
        Endpoint::Tls { .. } => "tls",
        Endpoint::Ws { .. } => "ws",
        Endpoint::Wss { .. } => "wss",
        Endpoint::Quic { .. } => "quic",
        Endpoint::Udp { .. } => "udp",
        Endpoint::Tcp { .. } => "tcp",
        Endpoint::Ipc(_) => "ipc",
        Endpoint::Inproc(_) => "inproc",
    })
}
