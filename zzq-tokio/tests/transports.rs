//! Transport edge cases: names and ports in use, refused connections,
//! failed handshakes and what the engine's types print.

#![expect(clippy::unwrap_used, reason = "tests fail loudly")]

use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use zzq_tokio::transport::{Driver, Listener, Stream, TransportOptions, connect};
use zzq_tokio::{Context, Endpoint, Error};

/// Drops the connection right away.
struct Hangup;

impl Driver for Hangup {
    type Output = ();

    async fn run<R, W>(self, _: R, _: W)
    where
        R: AsyncRead + Unpin + 'static,
        W: AsyncWrite + Unpin + 'static,
    {
    }
}

/// Drives `stream` on a lane of `ctx` and returns the result.
async fn drive(ctx: &Context, stream: Stream) -> Result<(), Error> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    ctx.lane(0)
        .spawn(move || async move {
            let _ = tx.send(stream.drive(Hangup).await);
        })
        .unwrap();
    rx.await.unwrap()
}

async fn bind(ctx: &Context, uri: &str, opts: &TransportOptions) -> Result<Listener, Error> {
    Listener::bind(ctx, &uri.parse().unwrap(), opts).await
}

#[tokio::test(flavor = "multi_thread")]
async fn inproc_names_are_taken_until_their_listener_drops() {
    let ctx = Context::new().unwrap();
    let opts = TransportOptions::default();
    let first = bind(&ctx, "inproc://busy", &opts).await.unwrap();
    let Err(e) = bind(&ctx, "inproc://busy", &opts).await else {
        panic!("second bind succeeded");
    };
    assert!(matches!(e, Error::AddressInUse { .. }), "{e:?}");
    drop(first);
    let gone: Endpoint = "inproc://busy".parse().unwrap();
    let Err(e) = connect(&ctx, &gone, &opts).await else {
        panic!("connect after the listener dropped succeeded");
    };
    assert!(matches!(e, Error::Refused { source: None, .. }), "{e:?}");
    let nobody: Endpoint = "inproc://nobody".parse().unwrap();
    assert!(matches!(
        connect(&ctx, &nobody, &opts).await,
        Err(Error::Refused { .. })
    ));
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn ipc_names_in_use_and_missing_paths() {
    let ctx = Context::new().unwrap();
    let opts = TransportOptions::default();
    let name = format!("ipc://@zzq-test-{}", std::process::id());
    let _first = bind(&ctx, &name, &opts).await.unwrap();
    let Err(e) = bind(&ctx, &name, &opts).await else {
        panic!("second bind succeeded");
    };
    assert!(matches!(e, Error::AddressInUse { .. }), "{e:?}");
    let missing: Endpoint = "ipc:///nonexistent/zzq.sock".parse().unwrap();
    let Err(e) = connect(&ctx, &missing, &opts).await else {
        panic!("connect to a missing path succeeded");
    };
    assert!(
        matches!(
            e,
            Error::Refused {
                source: Some(_),
                ..
            }
        ),
        "{e:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn udp_ports_in_use_and_udp_connects_are_refused() {
    let ctx = Context::new().unwrap();
    let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let port = held.local_addr().unwrap().port();
    let taken: Endpoint = format!("udp://127.0.0.1:{port}").parse().unwrap();
    let e = zzq_tokio::transport::udp::bind(&taken).await.unwrap_err();
    assert!(matches!(e, Error::AddressInUse { .. }), "{e:?}");
    // MQTT-SN clients reach the broker; the engine does not dial UDP.
    let e = connect(&ctx, &taken, &TransportOptions::default())
        .await
        .unwrap_err();
    assert!(matches!(e, Error::Unsupported(_)), "{e:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn websocket_upgrade_rejects_garbage_headers() {
    let ctx = Context::new().unwrap();
    let mut listener = bind(&ctx, "ws://127.0.0.1:0", &TransportOptions::default())
        .await
        .unwrap();
    let addr = listener.endpoint().socket_addr_string().unwrap();
    let mut raw = tokio::net::TcpStream::connect(addr).await.unwrap();
    raw.write_all(b"\xff\xfe\r\n\r\n").await.unwrap();
    let stream = listener.accept().await.unwrap();
    assert_eq!(format!("{stream:?}"), r#"Stream("ws")"#);
    let e = drive(&ctx, stream).await.unwrap_err();
    assert!(format!("{e:?}").contains("headers not UTF-8"), "{e:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn engine_types_print_without_secrets() {
    let ctx = Context::new().unwrap();
    let shown = format!("{ctx:?} {:?}", ctx.lane(0));
    assert!(
        shown.contains("Context") && shown.contains("LaneHandle"),
        "{shown}"
    );
    let listener = bind(&ctx, "tcp://127.0.0.1:0", &TransportOptions::default())
        .await
        .unwrap();
    assert!(format!("{listener:?}").contains("Tcp"));
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_and_quic_options_and_listeners_print_without_keys() {
    use zzq_tokio::transport::tls::TlsOptions;
    let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();
    let secret = key.signing_key.serialize_pem();
    let tls = TlsOptions {
        cert_chain_pem: Some(key.cert.pem().into_bytes()),
        key_pem: Some(secret.clone().into_bytes()),
        ..TlsOptions::default()
    };
    let shown = format!("{tls:?}");
    assert!(!shown.contains("PRIVATE KEY"), "{shown}");
    let ctx = Context::new().unwrap();
    let no_cert = TransportOptions {
        tls: Some(TlsOptions::default()),
    };
    let Err(e) = bind(&ctx, "tls://127.0.0.1:0", &no_cert).await else {
        panic!("tls bind without a certificate succeeded");
    };
    assert_eq!(
        e.to_string(),
        "tls: tls bind needs cert_chain_pem and key_pem"
    );
    let opts = TransportOptions { tls: Some(tls) };
    let tls_listener = bind(&ctx, "tls://127.0.0.1:0", &opts).await.unwrap();
    assert!(format!("{tls_listener:?}").contains("Tls"));
    #[cfg(feature = "quic")]
    {
        let quic = bind(&ctx, "quic://127.0.0.1:0", &opts).await.unwrap();
        assert!(format!("{quic:?}").contains("Quic"));
        // Not even another QUIC listener, which shares ports between lanes.
        let Err(e) = bind(&ctx, &quic.endpoint().to_string(), &opts).await else {
            panic!("two quic listeners on one port");
        };
        assert!(matches!(e, Error::AddressInUse { .. }), "{e:?}");
        // A plain UDP socket without SO_REUSEPORT blocks the port.
        let held = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = held.local_addr().unwrap().port();
        let Err(e) = bind(&ctx, &format!("quic://127.0.0.1:{port}"), &opts).await else {
            panic!("quic bind on a used port succeeded");
        };
        assert!(matches!(e, Error::AddressInUse { .. }), "{e:?}");
    }
}
