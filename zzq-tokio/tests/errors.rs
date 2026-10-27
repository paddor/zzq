//! Engine errors carry their endpoint and the error that caused them.

use std::error::Error as _;
use std::io;

use zzq_tokio::transport::{Listener, TransportOptions, connect};
use zzq_tokio::{Context, Endpoint, Error};

#[test]
fn invalid_endpoint_names_the_uri() {
    let e = "mqtt://x".parse::<Endpoint>().unwrap_err();
    assert!(matches!(
        e,
        Error::InvalidEndpoint {
            why: "unknown scheme",
            ..
        }
    ));
    assert_eq!(
        e.to_string(),
        r#"invalid endpoint "mqtt://x": unknown scheme"#
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn bind_and_connect_errors_keep_their_source() {
    let ctx = Context::new().unwrap();
    let opts = TransportOptions::default();
    let first = Listener::bind(&ctx, &"tcp://127.0.0.1:0".parse().unwrap(), &opts)
        .await
        .unwrap();
    let taken = first.endpoint();
    let Err(e) = Listener::bind(&ctx, &taken, &opts).await else {
        panic!("second bind succeeded");
    };
    assert_eq!(e.to_string(), format!("address in use: {taken}"));
    let source = e.source().unwrap().downcast_ref::<io::Error>().unwrap();
    assert_eq!(source.kind(), io::ErrorKind::AddrInUse);
    drop(first);
    let Err(e) = connect(&ctx, &taken, &opts).await else {
        panic!("connect to a closed port succeeded");
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
    assert_eq!(e.to_string(), format!("connection refused: {taken}"));
}

#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread")]
async fn tls_errors_name_the_failed_step() {
    let ctx = Context::new().unwrap();
    let opts = TransportOptions::default();
    let Err(e) = Listener::bind(&ctx, &"tls://127.0.0.1:0".parse().unwrap(), &opts).await else {
        panic!("tls bind without options succeeded");
    };
    assert_eq!(e.to_string(), "tls: tls:// needs TransportOptions::tls");
    let tls = zzq_tokio::transport::tls::TlsOptions {
        cert_chain_pem: Some(b"not a certificate".to_vec()),
        key_pem: Some(b"not a key".to_vec()),
        ..Default::default()
    };
    let opts = TransportOptions { tls: Some(tls) };
    let Err(e) = Listener::bind(&ctx, &"tls://127.0.0.1:0".parse().unwrap(), &opts).await else {
        panic!("tls bind with a bad key succeeded");
    };
    assert!(e.to_string().starts_with("tls: "), "{e}");
    assert!(e.source().is_some(), "{e:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_connect_is_refused_under_tls_and_websocket() {
    let ctx = Context::new().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut uris = vec![format!("ws://127.0.0.1:{port}/mqtt")];
    if cfg!(feature = "tls") {
        uris.push(format!("tls://127.0.0.1:{port}"));
        uris.push(format!("wss://127.0.0.1:{port}/mqtt"));
    }
    for uri in uris {
        let endpoint: Endpoint = uri.parse().unwrap();
        let Err(e) = connect(&ctx, &endpoint, &TransportOptions::default()).await else {
            panic!("{uri}: connect to a closed port succeeded");
        };
        assert!(
            matches!(
                e,
                Error::Refused {
                    source: Some(_),
                    ..
                }
            ),
            "{uri}: {e:?}"
        );
        assert_eq!(e.to_string(), format!("connection refused: {endpoint}"));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refused_websocket_upgrade_says_so() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let ctx = Context::new().unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let server = tokio::spawn(async move {
        let (mut s, _) = l.accept().await.unwrap();
        let mut request = [0u8; 1024];
        let _ = s.read(&mut request).await.unwrap();
        s.write_all(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        s
    });
    let endpoint: Endpoint = format!("ws://127.0.0.1:{port}/mqtt").parse().unwrap();
    let Err(e) = connect(&ctx, &endpoint, &TransportOptions::default()).await else {
        panic!("upgrade succeeded");
    };
    assert!(
        matches!(&e, Error::Io(io) if io.kind() == io::ErrorKind::InvalidData),
        "{e:?}"
    );
    let why = e.source().map(ToString::to_string).unwrap_or_default();
    assert!(why.contains("upgrade refused"), "{e:?}");
    drop(server.await.unwrap());
}
