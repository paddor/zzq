//! `tcp://`

use std::io;
#[cfg(feature = "tls")]
use std::pin::Pin;
#[cfg(feature = "tls")]
use std::task::{Context, Poll};

#[cfg(feature = "tls")]
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use crate::endpoint::Endpoint;
use crate::transport::Stream;
use crate::{Error, Result};

/// A bound TCP endpoint.
#[derive(Debug)]
pub struct TcpListener {
    inner: tokio::net::TcpListener,
    host: String,
    port: u16,
}

impl TcpListener {
    pub(crate) async fn bind(host: &str, port: u16) -> Result<TcpListener> {
        let addr = (host, port);
        let inner = tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| match e.kind() {
                io::ErrorKind::AddrInUse => Error::AddressInUse {
                    endpoint: format!("tcp://{host}:{port}"),
                    source: e,
                },
                _ => Error::Io(e),
            })?;
        let port = inner.local_addr()?.port();
        Ok(TcpListener {
            inner,
            host: host.to_owned(),
            port,
        })
    }

    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        let (s, _) = self.inner.accept().await?;
        s.set_nodelay(true)?;
        Ok(Stream::Tcp(s.into_std()?))
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        Endpoint::Tcp {
            host: self.host.clone(),
            port: self.port,
        }
    }
}

pub(crate) async fn connect(host: &str, port: u16) -> Result<Stream> {
    let s = tokio::net::TcpStream::connect((host, port))
        .await
        .map_err(|e| match e.kind() {
            io::ErrorKind::ConnectionRefused => Error::Refused {
                endpoint: format!("tcp://{host}:{port}"),
                source: Some(e.into()),
            },
            _ => Error::Io(e),
        })?;
    s.set_nodelay(true)?;
    Ok(Stream::Tcp(s.into_std()?))
}

/// Registers a moved socket with the current lane.
pub(crate) fn into_async(s: std::net::TcpStream) -> io::Result<tokio::net::TcpStream> {
    s.set_nonblocking(true)?;
    tokio::net::TcpStream::from_std(s)
}

/// A TCP stream that registers with the reactor of the lane polling it.
/// Parking deregisters it, so a stream wrapped in TLS can move to
/// another lane and be woken there.
#[cfg(feature = "tls")]
#[derive(Debug)]
pub enum LaneTcp {
    Live(tokio::net::TcpStream),
    Parked(Option<std::net::TcpStream>),
}

#[cfg(feature = "tls")]
impl LaneTcp {
    /// Deregisters from the current lane. The next poll registers again.
    pub(crate) fn park(&mut self) -> io::Result<()> {
        let old = std::mem::replace(self, LaneTcp::Parked(None));
        *self = match old {
            LaneTcp::Live(s) => LaneTcp::Parked(Some(s.into_std()?)),
            parked @ LaneTcp::Parked(_) => parked,
        };
        Ok(())
    }

    /// Registers with the current lane now, before the stream is polled.
    pub(crate) fn unpark(&mut self) -> io::Result<()> {
        self.live().map(|_| ())
    }

    fn live(&mut self) -> io::Result<&mut tokio::net::TcpStream> {
        if let LaneTcp::Parked(s) = self {
            let s = s
                .take()
                .ok_or_else(|| io::Error::other("tcp stream lost"))?;
            *self = LaneTcp::Live(into_async(s)?);
        }
        match self {
            LaneTcp::Live(s) => Ok(s),
            LaneTcp::Parked(_) => Err(io::Error::other("tcp stream lost")),
        }
    }
}

#[cfg(feature = "tls")]
impl AsyncRead for LaneTcp {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut().live() {
            Ok(s) => Pin::new(s).poll_read(cx, buf),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[cfg(feature = "tls")]
impl AsyncWrite for LaneTcp {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().live() {
            Ok(s) => Pin::new(s).poll_write(cx, buf),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut().live() {
            Ok(s) => Pin::new(s).poll_write_vectored(cx, bufs),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().live() {
            Ok(s) => Pin::new(s).poll_flush(cx),
            Err(e) => Poll::Ready(Err(e)),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut().live() {
            Ok(s) => Pin::new(s).poll_shutdown(cx),
            Err(e) => Poll::Ready(Err(e)),
        }
    }
}

#[cfg(all(test, feature = "tls"))]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    #[tokio::test(flavor = "current_thread")]
    async fn parked_stream_works_after_registering_again() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut peer = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (accepted, _) = listener.accept().await.unwrap();
        let mut stream = LaneTcp::Live(accepted);
        peer.write_all(b"hi").await.unwrap();
        let mut got = [0u8; 2];
        stream.read_exact(&mut got).await.unwrap();
        stream.park().unwrap();
        stream.unpark().unwrap();
        stream.write_all(b"yo").await.unwrap();
        peer.read_exact(&mut got).await.unwrap();
        assert_eq!(&got, b"yo");
    }
}
