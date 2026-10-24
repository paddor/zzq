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
#[non_exhaustive]
pub enum Endpoint {
    /// `tcp://host[:port]`
    Tcp {
        /// Host name or address.
        host: String,
        /// Port, 1883 by default.
        port: u16,
    },
    /// `tls://host[:port]`
    Tls {
        /// Host name or address.
        host: String,
        /// Port, 8883 by default.
        port: u16,
    },
    /// `ws://host[:port][/path]`
    Ws {
        /// Host name or address.
        host: String,
        /// Port, 80 by default.
        port: u16,
        /// Request path, `/mqtt` by default.
        path: String,
    },
    /// `wss://host[:port][/path]`
    Wss {
        /// Host name or address.
        host: String,
        /// Port, 443 by default.
        port: u16,
        /// Request path, `/mqtt` by default.
        path: String,
    },
    /// `quic://host[:port]`
    Quic {
        /// Host name or address.
        host: String,
        /// Port, 14567 by default.
        port: u16,
    },
    /// `udp://host[:port]`, MQTT-SN
    Udp {
        /// Host name or address.
        host: String,
        /// Port, 1884 by default.
        port: u16,
    },
    /// `ipc://path`, or `ipc://@name` for the Linux abstract namespace
    Ipc(String),
    /// `inproc://name`, within one context
    Inproc(String),
}

impl Endpoint {
    fn parse_uri(uri: &str) -> Result<Endpoint> {
        let bad = |why| Error::InvalidEndpoint {
            uri: uri.to_owned(),
            why,
        };
        let (scheme, rest) = uri.split_once("://").ok_or_else(|| bad("missing scheme"))?;
        if rest.is_empty() {
            return Err(bad("empty address"));
        }
        // Host, port and path of a network endpoint.
        let host_port = |default: u16| -> Result<(String, u16, String)> {
            let (authority, path) = match rest.find('/') {
                Some(i) => (&rest[..i], rest[i..].to_owned()),
                None => (rest, String::new()),
            };
            let (host, port) =
                split_host_port(authority, default).ok_or_else(|| bad("bad host or port"))?;
            Ok((host, port, path))
        };
        // Host and port of a network endpoint without a path.
        let addr = |default: u16| match host_port(default)? {
            (host, port, path) if path.is_empty() => Ok((host, port)),
            _ => Err(bad("unexpected path")),
        };
        Ok(match scheme {
            "tcp" => {
                let (host, port) = addr(TCP_PORT)?;
                Endpoint::Tcp { host, port }
            }
            "tls" => {
                let (host, port) = addr(TLS_PORT)?;
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
                let (host, port) = addr(QUIC_PORT)?;
                Endpoint::Quic { host, port }
            }
            "udp" => {
                let (host, port) = addr(UDP_PORT)?;
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

    /// Parses an endpoint URI such as `tcp://localhost:1883`.
    fn from_str(s: &str) -> Result<Endpoint> {
        Endpoint::parse_uri(s)
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
            "tcp://localhost".parse::<Endpoint>().unwrap(),
            tcp("localhost", 1883)
        );
        assert_eq!(
            "tcp://0.0.0.0:1999".parse::<Endpoint>().unwrap(),
            tcp("0.0.0.0", 1999)
        );
        assert_eq!("tcp://[::1]:7".parse::<Endpoint>().unwrap(), tcp("::1", 7));
        assert_eq!("tcp://[::1]".parse::<Endpoint>().unwrap(), tcp("::1", 1883));
        assert_eq!(
            "tls://h".parse::<Endpoint>().unwrap(),
            Endpoint::Tls {
                host: "h".into(),
                port: 8883
            }
        );
        assert_eq!(
            "ws://h:9".parse::<Endpoint>().unwrap(),
            Endpoint::Ws {
                host: "h".into(),
                port: 9,
                path: "/mqtt".into()
            }
        );
        assert_eq!(
            "ipc://@abc".parse::<Endpoint>().unwrap(),
            Endpoint::Ipc("@abc".into())
        );
        assert_eq!(
            "inproc://x".parse::<Endpoint>().unwrap(),
            Endpoint::Inproc("x".into())
        );
        assert_eq!(
            "tcp://[::1]:7".parse::<Endpoint>().unwrap().to_string(),
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
            assert!(bad.parse::<Endpoint>().is_err(), "{bad}");
        }
    }
}
