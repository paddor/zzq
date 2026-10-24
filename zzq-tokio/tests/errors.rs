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
