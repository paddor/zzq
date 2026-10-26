//! The standalone broker binary.

use std::process::{Child, Command};

const EXE: &str = env!("CARGO_BIN_EXE_zzq_broker");

/// Kills the broker when the test ends, even on failure.
struct Running(Child);

impl Drop for Running {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

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

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_keeps_sessions_in_the_state_file() {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    let dir = std::env::temp_dir().join(format!("zzq-bin-state-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("state");
    let start = || {
        let mut child = Command::new(EXE)
            .args(["--lanes", "2", "--bind", "tcp://127.0.0.1:0"])
            .args(["--state-file", file.to_str().unwrap()])
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut uri = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut uri)
            .unwrap();
        (Running(child), uri.trim().to_owned())
    };
    // SIGTERM, then a clean exit.
    let stop = |mut b: Running| {
        let pid = b.0.id().to_string();
        assert!(
            Command::new("kill")
                .args(["-TERM", &pid])
                .status()
                .unwrap()
                .success()
        );
        assert!(b.0.wait().unwrap().success());
    };
    let durable = ClientOptions {
        clean_start: false,
        session_expiry: 600,
        ..ClientOptions::new("bin-durable")
    };
    let ctx = Context::new().unwrap();
    let (broker, uri) = start();
    let c = Client::connect(&ctx, &uri, durable.clone()).await.unwrap();
    c.subscribe("bin/q", QoS::AtLeastOnce).await.unwrap();
    c.disconnect().await;
    stop(broker);
    assert!(file.exists());
    let (broker, uri) = start();
    let c = Client::connect(&ctx, &uri, durable).await.unwrap();
    assert!(c.connected().session_present);
    stop(broker);
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(feature = "tls")]
mod tls {
    use std::io::{BufRead, BufReader};
    use std::process::{Command, Stdio};

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    use super::{EXE, Running};

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
