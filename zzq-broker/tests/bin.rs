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

/// Starts the broker on a free port with `args`, and returns it with the
/// endpoint it bound.
#[cfg(unix)]
#[expect(clippy::unwrap_used, reason = "tests fail loudly")]
fn spawn(args: &[&str]) -> (Running, String) {
    use std::io::{BufRead, BufReader};
    use std::process::Stdio;

    let mut child = Command::new(EXE)
        .args(["--bind", "tcp://127.0.0.1:0"])
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut uri = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut uri)
        .unwrap();
    (Running(child), uri.trim().to_owned())
}

/// A fresh data directory for the test `name`, on the target directory's
/// file system, which takes `O_DIRECT`.
#[cfg(unix)]
fn data_dir(name: &str) -> std::path::PathBuf {
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("zzq-bin-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    dir
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
    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    let dir = std::env::temp_dir().join(format!("zzq-bin-state-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let file = dir.join("state");
    let start = || spawn(&["--lanes", "2", "--state-file", file.to_str().unwrap()]);
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

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigterm_writes_every_retained_change_first() {
    use std::time::Duration;

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    const N: usize = 50_000;
    let dir = data_dir("retained");
    let start = || spawn(&["--lanes", "1", "--data-dir", dir.to_str().unwrap()]);
    let ctx = Context::new().unwrap();
    let (mut broker, uri) = start();
    let c = Client::connect(&ctx, &uri, ClientOptions::new("pub"))
        .await
        .unwrap();
    for i in 0..N {
        c.publish_retained(&format!("r/{i}"), "x", QoS::AtMostOnce)
            .await
            .unwrap();
    }
    // In order behind the others: once acked, all are stored.
    c.publish_retained("r/last", "x", QoS::AtLeastOnce)
        .await
        .unwrap();
    let pid = broker.0.id().to_string();
    assert!(
        Command::new("kill")
            .args(["-TERM", &pid])
            .status()
            .unwrap()
            .success()
    );
    assert!(broker.0.wait().unwrap().success());
    drop(c);
    let (_broker, uri) = start();
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("sub"))
        .await
        .unwrap();
    c.subscribe("r/#", QoS::AtMostOnce).await.unwrap();
    let mut got = 0;
    while tokio::time::timeout(Duration::from_secs(1), c.recv())
        .await
        .is_ok_and(|m| m.is_some())
    {
        got += 1;
    }
    assert_eq!(got, N + 1, "retained messages after the restart");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_keeps_acked_retained_messages() {
    use std::time::Duration;

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    let dir = data_dir("sigkill");
    let args = ["--data-dir", dir.to_str().unwrap()];
    // Big, so a write behind an early ack would still run when the kill
    // lands.
    let payload: Vec<u8> = (0..=u8::MAX).cycle().take(15 << 20).collect();
    let ctx = Context::new().unwrap();
    let (mut broker, uri) = spawn(&args);
    let c = Client::connect(&ctx, &uri, ClientOptions::new("pub"))
        .await
        .unwrap();
    c.publish_retained("k/big", &payload, QoS::AtLeastOnce)
        .await
        .unwrap();
    // No shutdown: the PUBACK alone has to mean the message is stored.
    broker.0.kill().unwrap();
    broker.0.wait().unwrap();
    drop(c);
    let (_broker, uri) = spawn(&args);
    let mut c = Client::connect(&ctx, &uri, ClientOptions::new("sub"))
        .await
        .unwrap();
    c.subscribe("k/#", QoS::AtMostOnce).await.unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), c.recv())
        .await
        .ok()
        .flatten()
        .expect("the acked retained message after the restart");
    assert!(got.payload == payload, "payload changed");
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_keeps_sessions_and_their_subscriptions() {
    use std::time::Duration;

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, LastWill, QoS};

    let dir = data_dir("sessions");
    let args = ["--lanes", "2", "--data-dir", dir.to_str().unwrap()];
    let durable = |id: &str| ClientOptions {
        clean_start: false,
        session_expiry: 600,
        ..ClientOptions::new(id)
    };
    let ctx = Context::new().unwrap();
    let (mut broker, uri) = spawn(&args);
    let c = Client::connect(&ctx, &uri, durable("sub")).await.unwrap();
    // No shutdown: the SUBACK alone has to mean the subscription is stored.
    c.subscribe("s/#", QoS::AtLeastOnce).await.unwrap();
    c.disconnect().await;
    let dying = ClientOptions {
        will: Some(LastWill {
            topic: "s/will".into(),
            payload: "gone".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            delay: 0,
        }),
        ..durable("dying")
    };
    let dying = Client::connect(&ctx, &uri, dying).await.unwrap();
    // CONNACK does not wait for the disk; a SUBACK after it does.
    dying.subscribe("x", QoS::AtMostOnce).await.unwrap();
    broker.0.kill().unwrap();
    broker.0.wait().unwrap();
    let (_broker, uri) = spawn(&args);
    let mut c = Client::connect(&ctx, &uri, durable("sub")).await.unwrap();
    assert!(c.connected().session_present);
    let next = async |c: &mut Client| {
        let got = tokio::time::timeout(Duration::from_secs(5), c.recv()).await;
        got.ok().flatten().expect("a message")
    };
    // A connection the crash ended sends its will after the restart.
    assert_eq!(next(&mut c).await.payload, "gone");
    let p = Client::connect(&ctx, &uri, ClientOptions::new("pub"))
        .await
        .unwrap();
    p.publish("s/1", "after", QoS::AtLeastOnce).await.unwrap();
    assert_eq!(next(&mut c).await.payload, "after");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Runs the broker with `lanes`, queues acked QoS 1 and 2 messages for an
/// offline persistent session, kills it, and finds them all after a
/// restart.
#[cfg(unix)]
#[expect(clippy::unwrap_used, clippy::expect_used, reason = "tests fail loudly")]
async fn acked_messages_survive_sigkill(lanes: &str) {
    use std::time::Duration;

    use zzq_broker::Context;
    use zzq_client::{Client, ClientOptions, QoS};

    let dir = data_dir(&format!("queue-{lanes}"));
    let args = ["--lanes", lanes, "--data-dir", dir.to_str().unwrap()];
    let durable = |id: &str| ClientOptions {
        clean_start: false,
        session_expiry: 600,
        ..ClientOptions::new(id)
    };
    let ctx = Context::new().unwrap();
    let (mut broker, uri) = spawn(&args);
    let mut subs = Vec::new();
    for id in ["q-sub-1", "q-sub-2", "q-sub-3"] {
        let c = Client::connect(&ctx, &uri, durable(id)).await.unwrap();
        c.subscribe("q/#", QoS::ExactlyOnce).await.unwrap();
        c.disconnect().await;
        subs.push(id);
    }
    let p = Client::connect(&ctx, &uri, ClientOptions::new("q-pub"))
        .await
        .unwrap();
    for i in 0..50 {
        let qos = if i % 2 == 0 {
            QoS::AtLeastOnce
        } else {
            QoS::ExactlyOnce
        };
        p.publish("q/1", i.to_string(), qos).await.unwrap();
    }
    // No shutdown: the acks alone have to mean the messages are stored.
    broker.0.kill().unwrap();
    broker.0.wait().unwrap();
    drop(p);
    let (_broker, uri) = spawn(&args);
    for id in subs {
        let mut c = Client::connect(&ctx, &uri, durable(id)).await.unwrap();
        assert!(c.connected().session_present);
        for i in 0..50 {
            let got = tokio::time::timeout(Duration::from_secs(5), c.recv()).await;
            let m = got.ok().flatten().expect("a queued message");
            assert_eq!(m.payload, i.to_string(), "{id}");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_keeps_acked_messages_on_one_lane() {
    acked_messages_survive_sigkill("1").await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread")]
async fn sigkill_keeps_acked_messages_across_lanes() {
    acked_messages_survive_sigkill("4").await;
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
        let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("zzq-bin-tls-{}", std::process::id()));

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
            .args(["--data-dir", &path(&dir.join("data"))])
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
