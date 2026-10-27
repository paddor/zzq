//! Result lines of the load generators and client benches, as cache rows.

use serde_json::{Map, Value};

/// One cache row.
pub(crate) type Row = Map<String, Value>;

/// The words of a line, with `key=  value` joined to `key=value`.
fn words(line: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for w in line.split_whitespace() {
        match out.last_mut() {
            Some(last) if last.ends_with('=') => last.push_str(w),
            _ => out.push(w.to_owned()),
        }
    }
    out
}

/// The value of `key=value`.
fn kv<'a>(words: &'a [String], key: &str) -> Option<&'a str> {
    words
        .iter()
        .find_map(|w| w.strip_prefix(key)?.strip_prefix('='))
}

/// The word after `label`.
fn after<'a>(words: &'a [String], label: &str) -> Option<&'a str> {
    let i = words.iter().position(|w| w == label)?;
    words.get(i + 1).map(String::as_str)
}

/// The word before `label`.
fn before<'a>(words: &'a [String], label: &str) -> Option<&'a str> {
    let i = words.iter().position(|w| w == label)?;
    words.get(i.checked_sub(1)?).map(String::as_str)
}

/// The leading number of `12.5us`, `100%` or `0/8s`.
fn number(s: &str) -> Option<&str> {
    let end = s
        .find(|c: char| !(c.is_ascii_digit() || c == '.'))
        .unwrap_or(s.len());
    Some(&s[..end]).filter(|n| !n.is_empty())
}

fn int(s: Option<&str>) -> Option<Value> {
    number(s?)?.parse::<u64>().ok().map(Value::from)
}

fn float(s: Option<&str>) -> Option<Value> {
    number(s?)?.parse::<f64>().ok().map(Value::from)
}

/// `v5` as `5.0`, `v4` as `3.1.1`.
fn mqtt(v: &str) -> Option<Value> {
    match v {
        "v5" => Some("5.0".into()),
        "v4" => Some("3.1.1".into()),
        _ => None,
    }
}

/// Builds a row from `(key, value)` pairs. A missing required value fails
/// the row.
fn row(fields: Vec<(&str, Option<Value>)>) -> Option<Row> {
    fields
        .into_iter()
        .map(|(k, v)| Some((k.to_owned(), v?)))
        .collect()
}

/// Broker CPU, its memory and the load generator's CPU, if the line has
/// the broker's.
fn usage(w: &[String], row: &mut Row) -> Option<()> {
    if after(w, "cpu").is_none() {
        return Some(());
    }
    row.insert("cpu_pct".into(), int(after(w, "cpu"))?);
    row.insert("rss_mib".into(), int(after(w, "rss"))?);
    row.insert("gen_cpu_pct".into(), int(after(w, "gen"))?);
    Some(())
}

/// A `zzq_brokerbench` result: throughput or round trips.
pub(crate) fn broker_row(line: &str) -> Option<Row> {
    let w = words(line);
    let mut r = match w.first()?.as_str() {
        s @ ("pairs" | "fanout" | "fanin") => row(vec![
            ("kind", Some("throughput".into())),
            ("scenario", Some(s.into())),
            ("mqtt", mqtt(w.get(1)?)),
            ("qos", int(kv(&w, "qos"))),
            ("msg_size", int(kv(&w, "size"))),
            ("pubs", int(kv(&w, "pubs"))),
            ("subs", int(kv(&w, "subs"))),
            ("pub_msgs_s", float(after(&w, "pub"))),
            ("msgs_s", float(after(&w, "delivered"))),
            ("mbps", float(before(&w, "MB/s"))),
            ("loss_pct", float(after(&w, "loss"))),
            ("closed", int(after(&w, "closed"))),
            ("nacks", int(after(&w, "nacks"))),
        ])?,
        "rtt" => row(vec![
            ("kind", Some("rtt".into())),
            ("mqtt", mqtt(w.get(1)?)),
            ("qos", int(kv(&w, "qos"))),
            ("msg_size", int(kv(&w, "size"))),
            ("iterations", int(kv(&w, "iters"))),
            ("p50_us", float(after(&w, "p50"))),
            ("p90_us", float(after(&w, "p90"))),
            ("p99_us", float(after(&w, "p99"))),
            ("p999_us", float(after(&w, "p99.9"))),
            ("max_us", float(after(&w, "max"))),
            ("lost", int(after(&w, "lost"))),
        ])?,
        _ => return None,
    };
    usage(&w, &mut r)?;
    Some(r)
}

/// A `zzq_loadbench` step.
pub(crate) fn fixed_row(line: &str) -> Option<Row> {
    let w = words(line);
    if w.first()? != "fixed" {
        return None;
    }
    let mut r = row(vec![
        ("kind", Some("fixed".into())),
        ("scenario", w.get(1).map(|s| Value::from(s.as_str()))),
        ("mqtt", mqtt(w.get(2)?)),
        ("qos", int(kv(&w, "qos"))),
        ("msg_size", int(kv(&w, "size"))),
        ("pubs", int(kv(&w, "pubs"))),
        ("subs", int(kv(&w, "subs"))),
        ("offered_s", int(after(&w, "offered"))),
        ("msgs_s", float(after(&w, "delivered"))),
        ("loss_pct", float(after(&w, "loss"))),
        ("closed", int(after(&w, "closed"))),
        ("gen_cpu_pct", int(after(&w, "gen"))),
        ("p50_us", float(after(&w, "p50"))),
        ("p90_us", float(after(&w, "p90"))),
        ("p99_us", float(after(&w, "p99"))),
        ("p999_us", float(after(&w, "p99.9"))),
        ("max_us", float(after(&w, "max"))),
        ("samples", int(after(&w, "samples"))),
    ])?;
    if after(&w, "cpu").is_some() {
        r.insert("cpu_pct".into(), int(after(&w, "cpu"))?);
        r.insert("rss_mib".into(), int(after(&w, "rss"))?);
    }
    if let Some(steal) = int(after(&w, "steal")) {
        r.insert("steal_s".into(), steal);
    }
    let failed = match w.last()?.as_str() {
        "ok" => false,
        "failed" => true,
        _ => return None,
    };
    r.insert("failed".into(), failed.into());
    Some(r)
}

/// A client bench result: the fake sink's line measures a publishing
/// client, a `*-sub` line a subscribing one. The fake source's line is not
/// a result.
pub(crate) fn client_row(line: &str) -> Option<Row> {
    let w = words(line);
    let name = w.first()?;
    let kind = if name == "fake-sink" {
        "pub"
    } else if name.ends_with("-sub") && name != "fake-sub" {
        "sub"
    } else {
        return None;
    };
    row(vec![
        ("kind", Some(kind.into())),
        ("qos", int(kv(&w, "qos"))),
        ("msg_size", int(kv(&w, "size"))),
        ("n", int(kv(&w, "n"))),
        ("msgs_s", float(before(&w, "msg/s"))),
        ("mbps", float(before(&w, "MB/s"))),
    ])
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn a_throughput_line_becomes_a_row() {
        let line = " pairs v5 qos=1 size=   16 pubs=16 subs=16  pub   2474581/s  delivered   2474581/s    39.6 MB/s  loss   0.0%  closed 0 nacks 3  cpu 100% rss 6M gen 77%  per-sec [2.46M 2.47M]";
        let r = broker_row(line).unwrap();
        assert_eq!(
            Value::Object(r),
            json!({"kind": "throughput", "scenario": "pairs", "mqtt": "5.0", "qos": 1,
                   "msg_size": 16, "pubs": 16, "subs": 16, "pub_msgs_s": 2_474_581.0,
                   "msgs_s": 2_474_581.0, "mbps": 39.6, "loss_pct": 0.0, "closed": 0,
                   "nacks": 3, "cpu_pct": 100, "rss_mib": 6, "gen_cpu_pct": 77})
        );
        let bare = line.replace("  cpu 100% rss 6M", "");
        assert!(!broker_row(&bare).unwrap().contains_key("gen_cpu_pct"));
        assert_eq!(broker_row(&line.replace(" nacks 3", "")), None);
    }

    #[test]
    fn a_round_trip_line_becomes_a_row() {
        let line = "   rtt v4 qos=0 size=  256 iters=20000  p50 21.5us  p90 22.6us  p99 27.4us  p99.9 36.0us  max 155.8us  lost 0  cpu 96% rss 4M gen 64%";
        assert_eq!(
            Value::Object(broker_row(line).unwrap()),
            json!({"kind": "rtt", "mqtt": "3.1.1", "qos": 0, "msg_size": 256,
                   "iterations": 20000, "p50_us": 21.5, "p90_us": 22.6, "p99_us": 27.4,
                   "p999_us": 36.0, "max_us": 155.8, "lost": 0, "cpu_pct": 96,
                   "rss_mib": 4, "gen_cpu_pct": 64})
        );
        assert_eq!(broker_row("connected 20000 in 4.1s"), None);
    }

    #[test]
    fn a_fixed_load_step_becomes_a_row() {
        let line = "fixed p2p v5 qos=1 size=16 pubs=10000 subs=10000  offered 10000/s  delivered 9990/s  loss 0.1%  closed 2  cpu 45% rss 105M gen 25%  p50 71.7us  p90 122.9us  p99 2097.2us  p99.9 4718.6us  max 9699.3us  samples 80010  steal 1/8s  failed";
        assert_eq!(
            Value::Object(fixed_row(line).unwrap()),
            json!({"kind": "fixed", "scenario": "p2p", "mqtt": "5.0", "qos": 1,
                   "msg_size": 16, "pubs": 10000, "subs": 10000, "offered_s": 10000,
                   "msgs_s": 9990.0, "loss_pct": 0.1, "closed": 2, "cpu_pct": 45,
                   "rss_mib": 105, "gen_cpu_pct": 25, "p50_us": 71.7, "p90_us": 122.9,
                   "p99_us": 2097.2, "p999_us": 4718.6, "max_us": 9699.3,
                   "samples": 80010, "steal_s": 1, "failed": true})
        );
        let plain = line
            .replace("cpu 45% rss 105M ", "")
            .replace("steal 1/8s  failed", "ok");
        let r = fixed_row(&plain).unwrap();
        assert!(!r.contains_key("cpu_pct") && !r.contains_key("steal_s"));
        assert_eq!(r["failed"], false);
        assert_eq!(fixed_row(&line.replace("failed", "maybe")), None);
        assert_eq!(fixed_row("pairs v5"), None);
    }

    #[test]
    fn client_lines_become_rows_by_who_measured() {
        let sink = "fake-sink qos=1 size=   16 n=  500000      349714 msg/s       5.6 MB/s";
        assert_eq!(
            Value::Object(client_row(sink).unwrap()),
            json!({"kind": "pub", "qos": 1, "msg_size": 16, "n": 500_000,
                   "msgs_s": 349_714.0, "mbps": 5.6})
        );
        let sub = " zzq-sub qos=0 size= 1024 n= 3000000     3500445 msg/s    3584.5 MB/s";
        assert_eq!(client_row(sub).unwrap()["kind"], "sub");
        assert_eq!(client_row(&sink.replace("sink", "source")), None);
        assert_eq!(client_row(""), None);
    }
}
