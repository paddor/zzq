use std::collections::HashMap;
use std::fs::File;
use std::io::Write;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

pub(super) type Pids = Arc<Mutex<Vec<u32>>>;

pub(super) struct Monitor {
    pub pids: Pids,
    progress: Arc<AtomicU64>,
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Monitor {
    pub(super) fn new(dir: &Path) -> Self {
        let pids = Arc::new(Mutex::new(vec![std::process::id()]));
        let progress = Arc::new(AtomicU64::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let file = File::create(dir.join("resources.jsonl")).unwrap();
        let dir = dir.to_owned();
        let args = (pids.clone(), progress.clone(), stop.clone());
        let thread = std::thread::spawn(move || sample(file, &dir, &args.0, &args.1, &args.2));
        Self {
            pids,
            progress,
            stop,
            thread: Some(thread),
        }
    }

    pub(super) fn progress(&self, messages: u64) {
        self.progress.fetch_add(messages.max(1), Relaxed);
    }

    pub(super) fn finish(mut self) {
        self.stop.store(true, Relaxed);
        self.thread.take().unwrap().join().unwrap();
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.stop.store(true, Relaxed);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn metrics(pid: u32) -> Option<(u64, usize, usize)> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss = status.lines().find_map(|line| {
        line.strip_prefix("VmRSS:")
            .and_then(|s| s.split_whitespace().next()?.parse::<u64>().ok())
    })? * 1024;
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count();
    let threads = std::fs::read_dir(format!("/proc/{pid}/task")).ok()?.count();
    Some((rss, fds, threads))
}

fn activity(pid: u32) -> serde_json::Value {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let mut fields = stat
        .rsplit_once(')')
        .map_or("", |(_, rest)| rest)
        .split_whitespace();
    let user = fields.nth(11).and_then(|n| n.parse::<u64>().ok());
    let system = fields.next().and_then(|n| n.parse::<u64>().ok());
    let io = std::fs::read_to_string(format!("/proc/{pid}/io")).unwrap_or_default();
    let bytes = |name| {
        io.lines()
            .find_map(|line| line.strip_prefix(name)?.trim().parse::<u64>().ok())
    };
    serde_json::json!({"cpu_user_ticks":user,"cpu_system_ticks":system,"read_bytes":bytes("read_bytes:"),"write_bytes":bytes("write_bytes:")})
}

fn disk_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|entry| {
            let Ok(meta) = entry.metadata() else { return 0 };
            if meta.is_dir() {
                disk_bytes(&entry.path())
            } else {
                meta.blocks() * 512
            }
        })
        .sum()
}

fn sample(mut file: File, dir: &Path, pids: &Pids, progress: &AtomicU64, stop: &AtomicBool) {
    let start = Instant::now();
    let mut last = (0, Instant::now());
    let mut baselines = HashMap::new();
    while !stop.load(Relaxed) {
        let done = progress.load(Relaxed);
        if done != last.0 {
            last = (done, Instant::now());
        }
        if last.1.elapsed() > Duration::from_secs(90) {
            fail("no workload progress for 90s");
        }
        let elapsed = start.elapsed().as_secs();
        if elapsed.is_multiple_of(30) {
            writeln!(
                file,
                "{}",
                serde_json::json!({"seconds":elapsed,"disk_bytes":disk_bytes(dir)})
            )
            .unwrap();
        }
        let owned = pids.lock().unwrap().clone();
        baselines.retain(|pid, _| owned.contains(pid));
        for pid in owned {
            if let Some((rss, fds, threads)) = metrics(pid) {
                let row = serde_json::json!({"seconds":elapsed,"pid":pid,"rss":rss,"fds":fds,"threads":threads,"progress":done,"activity":activity(pid)});
                writeln!(file, "{row}").unwrap();
                if rss > 768 << 20 || fds > 2048 || threads > 128 {
                    fail(&format!("resource ceiling: {row}"));
                }
                let baseline = baselines.entry(pid).or_insert((elapsed, rss, fds, threads));
                if elapsed - baseline.0 < 60 {
                    baseline.1 = rss;
                    baseline.2 = fds;
                    baseline.3 = threads;
                }
                if elapsed - baseline.0 > 180
                    && (rss > baseline.1 + (128 << 20)
                        || fds > baseline.2 + 64
                        || threads > baseline.3 + 8)
                {
                    fail(&format!(
                        "resources grew after warmup: baseline={baseline:?} current={row}"
                    ));
                }
                if elapsed.is_multiple_of(30) {
                    eprintln!("resources {row}");
                }
            }
        }
        file.flush().unwrap();
        std::thread::sleep(Duration::from_secs(1));
    }
}

fn fail(reason: &str) -> ! {
    eprintln!("SOAK WATCHDOG: {reason}");
    // This also catches a wedged runtime. Child-process watchdogs reap brokers.
    std::process::exit(101)
}
