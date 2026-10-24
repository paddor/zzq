//! `udp://` datagram sockets, for MQTT-SN.

use std::io;

use crate::endpoint::Endpoint;
use crate::{Error, Result};

/// Largest MQTT-SN datagram accepted.
pub const MAX_DATAGRAM: usize = 65_535;

/// Binds a UDP socket on the current lane.
pub async fn bind(endpoint: &Endpoint) -> Result<tokio::net::UdpSocket> {
    let Endpoint::Udp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint {
            uri: endpoint.to_string(),
            why: "not udp://",
        });
    };
    tokio::net::UdpSocket::bind((host.as_str(), *port))
        .await
        .map_err(|e| match e.kind() {
            io::ErrorKind::AddrInUse => Error::AddressInUse {
                endpoint: endpoint.to_string(),
                source: e,
            },
            _ => Error::Io(e),
        })
}
