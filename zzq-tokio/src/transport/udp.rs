//! `udp://` datagram sockets, for MQTT-SN.

use std::io;

use crate::endpoint::Endpoint;
use crate::{Error, Result};

/// Largest MQTT-SN datagram accepted.
pub const MAX_DATAGRAM: usize = 65_535;

/// Binds a UDP socket on the current lane.
pub async fn bind(endpoint: &Endpoint) -> Result<tokio::net::UdpSocket> {
    let Endpoint::Udp { host, port } = endpoint else {
        return Err(Error::InvalidEndpoint(endpoint.to_string(), "not udp://"));
    };
    tokio::net::UdpSocket::bind((host.as_str(), *port))
        .await
        .map_err(|e| match e.kind() {
            io::ErrorKind::AddrInUse => Error::AddressInUse(endpoint.to_string()),
            _ => Error::Io(e),
        })
}
