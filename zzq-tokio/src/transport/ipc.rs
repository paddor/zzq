//! `ipc://` Unix domain sockets. A leading `@` selects the Linux abstract
//! namespace.

use std::io;
use std::os::unix::net::{UnixListener as StdListener, UnixStream as StdStream};
use std::path::PathBuf;

use crate::endpoint::Endpoint;
use crate::transport::Stream;
use crate::{Error, Result};

/// A bound Unix socket. Removes its socket file on drop.
#[derive(Debug)]
pub struct IpcListener {
    inner: tokio::net::UnixListener,
    name: String,
    file: Option<PathBuf>,
}

impl IpcListener {
    pub(crate) fn bind(name: &str) -> Result<IpcListener> {
        let in_use = |e: io::Error| match e.kind() {
            io::ErrorKind::AddrInUse => Error::AddressInUse {
                endpoint: format!("ipc://{name}"),
                source: e,
            },
            _ => Error::Io(e),
        };
        let (std, file) = if let Some(abs) = name.strip_prefix('@') {
            (bind_abstract(abs).map_err(in_use)?, None)
        } else {
            let path = PathBuf::from(name);
            if stale_socket(&path) {
                let _ = std::fs::remove_file(&path);
            }
            (StdListener::bind(&path).map_err(in_use)?, Some(path))
        };
        std.set_nonblocking(true)?;
        Ok(IpcListener {
            inner: tokio::net::UnixListener::from_std(std)?,
            name: name.to_owned(),
            file,
        })
    }

    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        let (s, _) = self.inner.accept().await?;
        Ok(Stream::Ipc(s.into_std()?))
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        Endpoint::Ipc(self.name.clone())
    }
}

/// Whether `path` is a socket file nobody listens on, as one left by a
/// listener that died. Anything else stays, and binding over it fails.
fn stale_socket(path: &std::path::Path) -> bool {
    use std::os::unix::fs::FileTypeExt;
    let socket = std::fs::symlink_metadata(path).is_ok_and(|m| m.file_type().is_socket());
    socket && StdStream::connect(path).is_err_and(|e| e.kind() == io::ErrorKind::ConnectionRefused)
}

impl Drop for IpcListener {
    fn drop(&mut self) {
        if let Some(path) = &self.file {
            let _ = std::fs::remove_file(path);
        }
    }
}

pub(crate) async fn connect(name: &str) -> Result<Stream> {
    let refused = |e: io::Error| match e.kind() {
        io::ErrorKind::ConnectionRefused | io::ErrorKind::NotFound => Error::Refused {
            endpoint: format!("ipc://{name}"),
            source: Some(e.into()),
        },
        _ => Error::Io(e),
    };
    if let Some(abs) = name.strip_prefix('@') {
        let s = connect_abstract(abs).map_err(refused)?;
        s.set_nonblocking(true)?;
        return Ok(Stream::Ipc(s));
    }
    let s = tokio::net::UnixStream::connect(name)
        .await
        .map_err(refused)?;
    Ok(Stream::Ipc(s.into_std()?))
}

pub(crate) fn into_async(s: StdStream) -> io::Result<tokio::net::UnixStream> {
    s.set_nonblocking(true)?;
    tokio::net::UnixStream::from_std(s)
}

#[cfg(target_os = "linux")]
fn bind_abstract(name: &str) -> io::Result<StdListener> {
    use std::os::linux::net::SocketAddrExt;
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    StdListener::bind_addr(&addr)
}

#[cfg(target_os = "linux")]
fn connect_abstract(name: &str) -> io::Result<StdStream> {
    use std::os::linux::net::SocketAddrExt;
    let addr = std::os::unix::net::SocketAddr::from_abstract_name(name)?;
    StdStream::connect_addr(&addr)
}

#[cfg(not(target_os = "linux"))]
fn bind_abstract(_: &str) -> io::Result<StdListener> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract sockets need Linux",
    ))
}

#[cfg(not(target_os = "linux"))]
fn connect_abstract(_: &str) -> io::Result<StdStream> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "abstract sockets need Linux",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn bind_leaves_a_file_that_is_no_socket() {
        let path = std::env::temp_dir().join(format!("zzq-ipc-file-{}", std::process::id()));
        std::fs::write(&path, b"data").unwrap();
        let bound = IpcListener::bind(path.to_str().unwrap());
        assert!(bound.is_err());
        assert_eq!(std::fs::read(&path).unwrap(), b"data");
        std::fs::remove_file(&path).unwrap();
    }
}
