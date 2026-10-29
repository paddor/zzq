//! TBMQ and the stores it needs: PostgreSQL, Kafka and Valkey, all with
//! host networking.

use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use serde_json::Value;

use super::{Ctr, data, env_args, store};
use crate::paths::Paths;

/// TBMQ's REST API.
const API: &str = "http://127.0.0.1:8083/api";

/// Kafka as TBMQ's compose file runs it, on 127.0.0.1.
const KAFKA: [&str; 14] = [
    "KAFKA_NODE_ID=0",
    "KAFKA_PROCESS_ROLES=controller,broker",
    "KAFKA_CONTROLLER_QUORUM_VOTERS=0@127.0.0.1:9093",
    "KAFKA_LISTENERS=PLAINTEXT://127.0.0.1:9092,CONTROLLER://127.0.0.1:9093",
    "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:9092",
    "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
    "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
    "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
    "KAFKA_LOG_DIRS=/var/lib/kafka/data",
    "KAFKA_DEFAULT_REPLICATION_FACTOR=1",
    "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
    "KAFKA_AUTO_CREATE_TOPICS_ENABLE=false",
];

/// TBMQ's own settings for its stores, as its compose file has them.
const TBMQ: [&str; 6] = [
    "TB_SERVICE_ID=tbmq",
    "SPRING_DATASOURCE_URL=jdbc:postgresql://127.0.0.1:5432/thingsboard_mqtt_broker",
    "SPRING_DATASOURCE_USERNAME=postgres",
    "SPRING_DATASOURCE_PASSWORD=postgres",
    "TB_KAFKA_SERVERS=127.0.0.1:9092",
    "REDIS_HOST=127.0.0.1",
];

/// `-e` options for `vars`.
fn env(vars: &[&str]) -> Vec<String> {
    vars.iter()
        .flat_map(|v| ["-e".to_owned(), (*v).to_owned()])
        .collect()
}

/// PostgreSQL, Kafka and Valkey with empty stores, then TBMQ with the
/// settings in `conf`. With `durable`, Kafka syncs each message and Valkey
/// each write.
pub(super) fn ctrs(durable: bool, conf: &Path) -> Result<Vec<Ctr>, String> {
    let mut pg = env(&[
        "POSTGRES_DB=thingsboard_mqtt_broker",
        "POSTGRES_PASSWORD=postgres",
    ]);
    pg.extend(data(&store("tbmq-postgres")?, "/var/lib/postgresql/data"));
    let mut kafka = env(&KAFKA);
    let mut valkey_cmd = Vec::new();
    if durable {
        kafka.extend(env(&["KAFKA_LOG_FLUSH_INTERVAL_MESSAGES=1"]));
        valkey_cmd = [
            "valkey-server",
            "--appendonly",
            "yes",
            "--appendfsync",
            "always",
        ]
        .map(String::from)
        .to_vec();
    }
    kafka.extend(data(&store("tbmq-kafka")?, "/var/lib/kafka/data"));
    let mut tbmq = env(&TBMQ);
    tbmq.extend(env_args(conf).map_err(|e| format!("{}: {e}", conf.display()))?);
    Ok(vec![
        Ctr {
            name: "zzq-tbmq-postgres",
            image: "docker.io/library/postgres:17",
            args: pg,
            cmd: Vec::new(),
            port: 5432,
        },
        Ctr {
            name: "zzq-tbmq-kafka",
            image: "docker.io/apache/kafka:4.0.0",
            args: kafka,
            cmd: Vec::new(),
            port: 9092,
        },
        Ctr {
            name: "zzq-tbmq-valkey",
            image: "docker.io/valkey/valkey:8.0",
            args: data(&store("tbmq-valkey")?, "/data").to_vec(),
            cmd: valkey_cmd,
            port: 6379,
        },
        Ctr::broker("zzq-tbmq", "docker.io/thingsboard/tbmq:2.4.1", tbmq),
    ])
}

/// Lets clients in without credentials. TBMQ refuses them while its basic
/// auth provider, on after install, is enabled, and only its REST API
/// turns it off.
pub(super) fn open() -> Result<(), String> {
    let auth = format!("X-Authorization: Bearer {}", login()?);
    let providers = curl(&[
        "-H",
        &auth,
        &format!("{API}/mqtt/auth/providers?pageSize=20&page=0"),
    ])?;
    let id = basic_id(&providers).ok_or("TBMQ has no MQTT_BASIC provider")?;
    let disable = format!("{API}/mqtt/auth/provider/{id}/disable");
    curl(&["-X", "POST", "-H", &auth, &disable])?;
    Ok(())
}

/// Waits until a QoS 1 message gets through. TBMQ listens before its
/// Kafka consumers have their partitions, and what is sent before then
/// is never delivered.
pub(super) fn ready(p: &Paths) -> Result<(), String> {
    let deadline = Instant::now() + Duration::from_secs(300);
    loop {
        let probe = Command::new("timeout")
            .arg("30")
            .arg(p.tool("zzq_brokerbench"))
            .args(["rtt", "qos=1", "size=16", "iters=20"])
            .output();
        if probe.is_ok_and(|o| o.status.success() && delivered(&String::from_utf8_lossy(&o.stdout)))
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err("TBMQ delivers nothing after 300s".to_owned());
        }
        thread::sleep(Duration::from_secs(2));
    }
}

/// Whether a round-trip result lost none.
fn delivered(out: &str) -> bool {
    out.split_whitespace().skip_while(|&w| w != "lost").nth(1) == Some("0")
}

/// The ID of the basic auth provider in a list of providers.
fn basic_id(providers: &Value) -> Option<&str> {
    providers["data"]
        .as_array()?
        .iter()
        .find(|p| p["type"] == "MQTT_BASIC")?["id"]
        .as_str()
}

/// The system administrator's token, once the API answers.
fn login() -> Result<String, String> {
    let body = r#"{"username":"sysadmin@thingsboard.org","password":"sysadmin"}"#;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let reply = curl(&[
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "-d",
            body,
            &format!("{API}/auth/login"),
        ]);
        match reply.as_ref().map(|v| v["token"].as_str()) {
            Ok(Some(token)) => return Ok(token.to_owned()),
            _ if Instant::now() >= deadline => return Err(format!("TBMQ login: {reply:?}")),
            _ => thread::sleep(Duration::from_secs(1)),
        }
    }
}

/// A request with curl: its JSON reply, null for an empty one.
fn curl(args: &[&str]) -> Result<Value, String> {
    let out = Command::new("curl")
        .args(["-sSf", "--max-time", "30"])
        .args(args)
        .output()
        .map_err(|e| format!("curl: {e}"))?;
    if !out.status.success() {
        return Err(String::from_utf8_lossy(&out.stderr).trim().to_owned());
    }
    if out.stdout.iter().all(u8::is_ascii_whitespace) {
        return Ok(Value::Null);
    }
    serde_json::from_slice(&out.stdout).map_err(|e| format!("TBMQ reply: {e}"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn the_basic_provider_is_found_by_type() {
        let providers = json!({"data": [
            {"id": "a", "type": "JWT", "enabled": false},
            {"id": "b", "type": "MQTT_BASIC", "enabled": true},
        ]});
        assert_eq!(basic_id(&providers), Some("b"));
        assert_eq!(basic_id(&json!({"data": []})), None);
        assert_eq!(env(&["A=1"]), ["-e", "A=1"]);
        assert!(delivered("rtt v5  p50 9.0us  max 20.0us  lost 0  gen 1%"));
        assert!(!delivered("rtt v5  p50 9.0us  lost 3  gen 1%"));
        assert!(!delivered("error: closed"));
    }

    #[test]
    fn a_failed_request_says_why() {
        let e = curl(&["--max-time", "1", "http://127.0.0.1:9/"]).unwrap_err();
        assert_ne!(e, "");
    }
}
