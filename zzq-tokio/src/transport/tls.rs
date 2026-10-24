//! `tls://` TLS over TCP with rustls.
//!
//! The server handshake runs on the first lane that touches the stream. The
//! TCP socket then parks, so the TLS stream registers with the lane it moves
//! to.

use std::fmt;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::{CryptoProvider, ring};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{DigitallySignedStruct, SignatureScheme};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::endpoint::Endpoint;
use crate::transport::Stream;
use crate::transport::tcp::{self, LaneTcp};
use crate::{Error, Result};

/// Certificates and keys for `tls://`.
#[derive(Clone, Default)]
pub struct TlsOptions {
    /// PEM certificate chain presented to the peer: required to bind,
    /// optional client certificate when connecting.
    pub cert_chain_pem: Option<Vec<u8>>,
    /// PEM private key for `cert_chain_pem`.
    pub key_pem: Option<Vec<u8>>,
    /// PEM roots that client certificates must chain to. Setting it makes a
    /// bound endpoint require a client certificate.
    pub client_ca_pem: Option<Vec<u8>>,
    /// PEM trust anchors for verifying the server when connecting.
    pub trust_pem: Option<Vec<u8>>,
    /// Name to verify the server certificate against. Defaults to the host
    /// of the endpoint.
    pub server_name: Option<String>,
    /// Accept any server certificate when connecting. Handshake signatures
    /// are still checked, but anyone on the path can impersonate the
    /// server. For tests and local development only.
    pub danger_accept_invalid_certs: bool,
    /// ALPN protocols, in preference order.
    pub alpn: Vec<Vec<u8>>,
}

impl fmt::Debug for TlsOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsOptions")
            .field(
                "cert_chain_pem",
                &self.cert_chain_pem.as_ref().map(Vec::len),
            )
            .field("key_pem", &self.key_pem.as_ref().map(|_| "<redacted>"))
            .field("trust_pem", &self.trust_pem.as_ref().map(Vec::len))
            .field("client_ca_pem", &self.client_ca_pem.as_ref().map(Vec::len))
            .field("server_name", &self.server_name)
            .field(
                "danger_accept_invalid_certs",
                &self.danger_accept_invalid_certs,
            )
            .field("alpn", &self.alpn)
            .finish()
    }
}

fn provider() -> Arc<CryptoProvider> {
    Arc::new(ring::default_provider())
}

fn bad(what: &'static str) -> Error {
    Error::Tls { what, source: None }
}

/// Maps a rustls error to [`Error::Tls`], with `what` as the step.
fn failed<E: Into<crate::BoxError>>(what: &'static str) -> impl FnOnce(E) -> Error {
    move |e| Error::Tls {
        what,
        source: Some(e.into()),
    }
}

fn certs(pem: &[u8]) -> Result<Vec<CertificateDer<'static>>> {
    let certs = CertificateDer::pem_slice_iter(pem)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(failed("certificate PEM"))?;
    if certs.is_empty() {
        return Err(bad("no certificate in PEM"));
    }
    Ok(certs)
}

/// Builds the acceptor for a `tls://` bind.
pub(crate) fn acceptor(o: &TlsOptions) -> Result<TlsAcceptor> {
    Ok(TlsAcceptor::from(Arc::new(server_config(o)?)))
}

/// Server config from `o`: certificate chain, key, client verification and
/// ALPN.
pub(crate) fn server_config(o: &TlsOptions) -> Result<rustls::ServerConfig> {
    let (chain, key) =
        identity(o)?.ok_or_else(|| bad("tls bind needs cert_chain_pem and key_pem"))?;
    let builder = rustls::ServerConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(failed("protocol versions"))?;
    let builder = match o.client_ca_pem.as_deref() {
        Some(pem) => {
            let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
                Arc::new(roots(pem)?),
                provider(),
            )
            .build()
            .map_err(failed("client CA"))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let mut config = builder
        .with_single_cert(chain, key)
        .map_err(failed("certificate and key"))?;
    config.alpn_protocols.clone_from(&o.alpn);
    Ok(config)
}

/// Certificate chain and key, if both are set.
fn identity(
    o: &TlsOptions,
) -> Result<Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>> {
    let (Some(chain), Some(key)) = (o.cert_chain_pem.as_deref(), o.key_pem.as_deref()) else {
        return Ok(None);
    };
    let key = PrivateKeyDer::from_pem_slice(key).map_err(failed("key PEM"))?;
    Ok(Some((certs(chain)?, key)))
}

fn roots(pem: &[u8]) -> Result<rustls::RootCertStore> {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs(pem)? {
        roots.add(cert).map_err(failed("trust anchor"))?;
    }
    Ok(roots)
}

fn connector(o: &TlsOptions) -> Result<TlsConnector> {
    Ok(TlsConnector::from(Arc::new(client_config(o)?)))
}

/// Client config from `o`: trust anchors or no verification, the optional
/// client certificate, and ALPN.
pub(crate) fn client_config(o: &TlsOptions) -> Result<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder_with_provider(provider())
        .with_safe_default_protocol_versions()
        .map_err(failed("protocol versions"))?;
    let builder = if o.danger_accept_invalid_certs {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(AnyServer(provider())))
    } else {
        let pem = o
            .trust_pem
            .as_deref()
            .ok_or_else(|| bad("tls connect needs trust_pem"))?;
        builder.with_root_certificates(roots(pem)?)
    };
    let mut config = match identity(o)? {
        Some((chain, key)) => builder
            .with_client_auth_cert(chain, key)
            .map_err(failed("client certificate and key"))?,
        None => builder.with_no_client_auth(),
    };
    config.alpn_protocols.clone_from(&o.alpn);
    Ok(config)
}

/// A bound TLS endpoint.
pub struct TlsListener {
    tcp: tcp::TcpListener,
    acceptor: TlsAcceptor,
}

impl fmt::Debug for TlsListener {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsListener")
            .field("tcp", &self.tcp)
            .finish_non_exhaustive()
    }
}

impl TlsListener {
    pub(crate) async fn bind(host: &str, port: u16, o: &TlsOptions) -> Result<TlsListener> {
        let acceptor = acceptor(o)?;
        Ok(TlsListener {
            tcp: tcp::TcpListener::bind(host, port).await?,
            acceptor,
        })
    }

    /// Accepts a TCP connection. The handshake happens when the stream is
    /// first read, so a slow client does not hold up the accept loop.
    pub(crate) async fn accept(&mut self) -> Result<Stream> {
        match self.tcp.accept().await? {
            Stream::Tcp(s) => Ok(Stream::TlsAccept(s, self.acceptor.clone())),
            other => Ok(other),
        }
    }

    pub(crate) fn endpoint(&self) -> Endpoint {
        match self.tcp.endpoint() {
            Endpoint::Tcp { host, port } => Endpoint::Tls { host, port },
            other => other,
        }
    }
}

/// Completes a server handshake on the current lane.
pub(crate) async fn handshake(s: std::net::TcpStream, acceptor: &TlsAcceptor) -> Result<Stream> {
    let tls = acceptor.accept(LaneTcp::Live(tcp::into_async(s)?)).await?;
    Ok(Stream::Tls(Box::new(tls.into())))
}

pub(crate) async fn connect(host: &str, port: u16, o: &TlsOptions) -> Result<Stream> {
    let tcp = tokio::net::TcpStream::connect((host, port)).await?;
    tcp.set_nodelay(true)?;
    let tls = client_handshake(LaneTcp::Live(tcp), host, o).await?;
    Ok(Stream::Tls(Box::new(tls.into())))
}

/// TCP connect plus client handshake.
pub(crate) async fn connect_stream(
    host: &str,
    port: u16,
    o: &TlsOptions,
) -> Result<tokio_rustls::client::TlsStream<tokio::net::TcpStream>> {
    let tcp = tokio::net::TcpStream::connect((host, port)).await?;
    tcp.set_nodelay(true)?;
    client_handshake(tcp, host, o).await
}

/// Client handshake over `io`, a connection to `host`.
async fn client_handshake<IO>(
    io: IO,
    host: &str,
    o: &TlsOptions,
) -> Result<tokio_rustls::client::TlsStream<IO>>
where
    IO: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let connector = connector(o)?;
    let name = o.server_name.clone().unwrap_or_else(|| host.to_owned());
    let name = ServerName::try_from(name).map_err(failed("server name"))?;
    Ok(connector.connect(name, io).await?)
}

/// Accepts any server certificate but still checks handshake signatures.
#[derive(Debug)]
struct AnyServer(Arc<CryptoProvider>);

impl ServerCertVerifier for AnyServer {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> std::result::Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> std::result::Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.0.signature_verification_algorithms,
        )
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.0.signature_verification_algorithms.supported_schemes()
    }
}
