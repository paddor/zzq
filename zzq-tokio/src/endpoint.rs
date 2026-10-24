//! Endpoint URIs.

use std::fmt;

use crate::{Error, Result};

/// Default MQTT port.
pub const TCP_PORT: u16 = 1883;
/// Default MQTT over TLS port.
pub const TLS_PORT: u16 = 8883;
/// Default MQTT over QUIC port (UDP).
pub const QUIC_PORT: u16 = 14567;
/// Default MQTT-SN port (UDP).
pub const UDP_PORT: u16 = 1884;

/// A parsed endpoint.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Endpoint {
    /// `tcp://host[:port]`
    Tcp { host: String, port: u16 },
    /// `tls://host[:port]`
    Tls { host: String, port: u16 },
    /// `ws://host[:port][/path]`
    Ws {
        host: String,
        port: u16,
        path: String,
    },
    /// `wss://host[:port][/path]`
    Wss {
        host: String,
        port: u16,
        path: String,
    },
    /// `quic://host[:port]`
    Quic { host: String, port: u16 },
    /// `udp://host[:port]`, MQTT-SN
    Udp { host: String, port: u16 },
    /// `ipc://path`, or `ipc://@name` for the Linux abstract namespace
    Ipc(String),
    /// `inproc://name`, within one context
    Inproc(String),
}

impl Endpoint {
    /// Parses an endpoint URI.
    pub fn parse(uri: &str) -> Result<Endpoint> {
        let bad = |why| Error::InvalidEndpoint(uri.to_owned(), why);
        let (scheme, rest) = uri.split_once("://").ok_or_else(|| bad("missing scheme"))?;
        if rest.is_empty() {
            return Err(bad("empty address"));
        }
        let host_port = |default: u16| -> Result<(String, u16, String)> {
            let (authority, path) = match rest.find('/') {
                Some(i) => (&rest[..i], rest[i..].to_owned()),
                None => (rest, String::new()),
            };
            let (host, port) =
                split_host_port(authority, default).ok_or_else(|| bad("bad host or port"))?;
            Ok((host, port, path))
        };
        let no_path = |path: &str| {
            if path.is_empty() {
                Ok(())
            } else {
                Err(bad("unexpected path"))
            }
        };
        Ok(match scheme {
            "tcp" => {
                let (host, port, path) = host_port(TCP_PORT)?;
                no_path(&path)?;
                Endpoint::Tcp { host, port }
            }
            "tls" => {
                let (host, port, path) = host_port(TLS_PORT)?;
                no_path(&path)?;
                Endpoint::Tls { host, port }
            }
            "ws" | "wss" => {
                let default = if scheme == "ws" { 80 } else { 443 };
                let (host, port, path) = host_port(default)?;
                let path = if path.is_empty() {
                    "/mqtt".to_owned()
                } else {
                    path
                };
                if scheme == "ws" {
                    Endpoint::Ws { host, port, path }
                } else {
                    Endpoint::Wss { host, port, path }
                }
            }
            "quic" => {
                let (host, port, path) = host_port(QUIC_PORT)?;
                no_path(&path)?;
                Endpoint::Quic { host, port }
            }
            "udp" => {
                let (host, port, path) = host_port(UDP_PORT)?;
                no_path(&path)?;
                Endpoint::Udp { host, port }
            }
            "ipc" => Endpoint::Ipc(rest.to_owned()),
            "inproc" => Endpoint::Inproc(rest.to_owned()),
            _ => return Err(bad("unknown scheme")),
        })
    }

    /// `host:port` for network endpoints.
    pub fn socket_addr_string(&self) -> Option<String> {
        match self {
            Endpoint::Tcp { host, port }
            | Endpoint::Tls { host, port }
            | Endpoint::Ws { host, port, .. }
            | Endpoint::Wss { host, port, .. }
            | Endpoint::Quic { host, port }
            | Endpoint::Udp { host, port } => Some(format_host_port(host, *port)),
            Endpoint::Ipc(_) | Endpoint::Inproc(_) => None,
        }
    }
}

fn split_host_port(authority: &str, default: u16) -> Option<(String, u16)> {
    if let Some(v6) = authority.strip_prefix('[') {
        let (host, rest) = v6.split_once(']')?;
        let port = match rest.strip_prefix(':') {
            Some(p) => p.parse().ok()?,
            None if rest.is_empty() => default,
            None => return None,
        };
        return Some((host.to_owned(), port));
    }
    match authority.rsplit_once(':') {
        Some((host, port)) if !host.is_empty() => Some((host.to_owned(), port.parse().ok()?)),
        None if !authority.is_empty() => Some((authority.to_owned(), default)),
        Some(_) | None => None,
    }
}

fn format_host_port(host: &str, port: u16) -> String {
    if host.contains(':') {
        format!("[{host}]:{port}")
    } else {
        format!("{host}:{port}")
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Endpoint::Tcp { host, port } => write!(f, "tcp://{}", format_host_port(host, *port)),
            Endpoint::Tls { host, port } => write!(f, "tls://{}", format_host_port(host, *port)),
            Endpoint::Ws { host, port, path } => {
                write!(f, "ws://{}{path}", format_host_port(host, *port))
            }
            Endpoint::Wss { host, port, path } => {
                write!(f, "wss://{}{path}", format_host_port(host, *port))
            }
            Endpoint::Quic { host, port } => write!(f, "quic://{}", format_host_port(host, *port)),
            Endpoint::Udp { host, port } => write!(f, "udp://{}", format_host_port(host, *port)),
            Endpoint::Ipc(p) => write!(f, "ipc://{p}"),
            Endpoint::Inproc(n) => write!(f, "inproc://{n}"),
        }
    }
}

impl std::str::FromStr for Endpoint {
    type Err = Error;

    fn from_str(s: &str) -> Result<Endpoint> {
        Endpoint::parse(s)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tcp(host: &str, port: u16) -> Endpoint {
        Endpoint::Tcp {
            host: host.to_owned(),
            port,
        }
    }

    #[test]
    fn parses_every_scheme_with_default_ports() {
        assert_eq!(
            Endpoint::parse("tcp://localhost").unwrap(),
            tcp("localhost", 1883)
        );
        assert_eq!(
            Endpoint::parse("tcp://0.0.0.0:1999").unwrap(),
            tcp("0.0.0.0", 1999)
        );
        assert_eq!(Endpoint::parse("tcp://[::1]:7").unwrap(), tcp("::1", 7));
        assert_eq!(Endpoint::parse("tcp://[::1]").unwrap(), tcp("::1", 1883));
        assert_eq!(
            Endpoint::parse("tls://h").unwrap(),
            Endpoint::Tls {
                host: "h".into(),
                port: 8883
            }
        );
        assert_eq!(
            Endpoint::parse("ws://h:9").unwrap(),
            Endpoint::Ws {
                host: "h".into(),
                port: 9,
                path: "/mqtt".into()
            }
        );
        assert_eq!(
            Endpoint::parse("ipc://@abc").unwrap(),
            Endpoint::Ipc("@abc".into())
        );
        assert_eq!(
            Endpoint::parse("inproc://x").unwrap(),
            Endpoint::Inproc("x".into())
        );
        assert_eq!(
            Endpoint::parse("tcp://[::1]:7").unwrap().to_string(),
            "tcp://[::1]:7"
        );
    }

    #[test]
    fn rejects_malformed_uris() {
        for bad in [
            "tcp:/x",
            "mqtt://x",
            "tcp://",
            "tcp://h:x",
            "tcp://:1",
            "tcp://h/p",
            "inproc://",
        ] {
            assert!(Endpoint::parse(bad).is_err(), "{bad}");
        }
    }
}
