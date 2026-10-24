//! The standalone broker binary.

use std::process::Command;

const EXE: &str = env!("CARGO_BIN_EXE_zzq_broker");

#[test]
fn bad_flags_print_usage() {
    let out = Command::new(EXE).args(["--nope", "x"]).output().unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).starts_with("usage: zzq_broker"));
}

#[test]
fn bind_errors_name_the_cause() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let uri = format!("tcp://{}", held.local_addr().unwrap());
    let out = Command::new(EXE).args(["--bind", &uri]).output().unwrap();
    assert!(!out.status.success());
    let err = String::from_utf8_lossy(&out.stderr);
    assert!(err.contains("transport failed: address in use"), "{err}");
}

#[cfg(feature = "tls")]
mod tls {
    use std::io::{BufRead, BufReader};
    use std::process::{Child, Command, Stdio};

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    use super::EXE;

    /// Kills the broker when the test ends, even on failure.
    struct Running(Child);

    impl Drop for Running {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[tokio::test(flavor = "multi_thread")]

    async fn serves_tcp_and_tls_with_retained_messages_on_disk() {
        let dir = std::env::temp_dir().join(format!("zzq-bin-{}", std::process::id()));

        std::fs::create_dir_all(&dir).unwrap();

        let key = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()]).unwrap();

        let (cert, pem_key) = (dir.join("cert.pem"), dir.join("key.pem"));

        std::fs::write(&cert, key.cert.pem()).unwrap();

        std::fs::write(&pem_key, key.signing_key.serialize_pem()).unwrap();

        let path = |p: &std::path::Path| p.to_str().unwrap().to_owned();

        let mut child = Command::new(EXE)
            .args(["--lanes", "2", "--busy-poll"])
            .args(["--bind", "tcp://127.0.0.1:0", "--bind", "tls://127.0.0.1:0"])
            .args(["--cert", &path(&cert), "--key", &path(&pem_key)])
            .args(["--client-ca", &path(&cert)])
            .args(["--retained-file", &path(&dir.join("retained.log"))])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();

        let stdout = child.stdout.take().unwrap();

        let _running = Running(child);

        let mut lines = BufReader::new(stdout).lines();

        let tcp = lines.next().unwrap().unwrap();

        let tls = lines.next().unwrap().unwrap();

        assert!(
            tcp.starts_with("tcp://") && tls.starts_with("tls://"),
            "{tcp} {tls}"
        );

        let ctx = Context::new().unwrap();

        let publ = Client::connect(&ctx, &tcp, ClientOptions::new("bin-pub"))
            .await
            .unwrap();

        publ.publish_retained("bin/status", "up", QoS::AtLeastOnce)
            .await
            .unwrap();

        let mut late = Client::connect(&ctx, &tcp, ClientOptions::new("bin-sub"))
            .await
            .unwrap();

        late.subscribe("bin/#", QoS::AtLeastOnce).await.unwrap();

        assert_eq!(late.recv().await.unwrap().payload, "up");

        // The TLS listener wants a client certificate.

        let anonymous = ClientOptions {
            transport: zzq_client::TransportOptions {
                tls: Some(zzq_client::TlsOptions {
                    danger_accept_invalid_certs: true,

                    ..Default::default()
                }),
            },

            ..ClientOptions::new("bin-tls")
        };

        assert!(Client::connect(&ctx, &tls, anonymous).await.is_err());

        let _ = std::fs::remove_dir_all(&dir);
    }
}
