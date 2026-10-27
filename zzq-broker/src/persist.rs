//! State that survives a broker restart: retained messages.
//!
//! [`FilePersistence`] keeps them in an append-only log with a versioned
//! header. A background thread writes, so lanes never wait for the disk,
//! and a topic changed again before it wrote waits only with its latest
//! change.
//!
//! Log format, big endian:
//!
//! ```text
//! file   = "ZZQRET" version:u16 record*
//! record = len:u32 crc32(body):u32 body
//! body   = 1:u8 topic qos:u8 expires_ms:u64 props:bytes32 payload:bytes32
//!        | 2:u8 topic
//! topic  = len:u16 utf8
//! ```
//!
//! `expires_ms` is Unix time in milliseconds, 0 for none. Loading stops at
//! the first torn or corrupt record and rewrites the log from what it read.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, mpsc};
use std::thread::JoinHandle;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use zzq_proto::QoS;

const MAGIC: &[u8; 6] = b"ZZQRET";
const VERSION: u16 = 1;
const SET: u8 = 1;
const REMOVE: u8 = 2;
/// The log is rewritten once it is this much larger than its live records.
const COMPACT_SLACK: u64 = 1 << 20;

/// A retained message as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retained {
    /// Topic name.
    pub topic: String,
    /// Payload, never empty: an empty one clears the topic.
    pub payload: Bytes,
    /// QoS of the message.
    pub qos: QoS,
    /// Raw v5 properties.
    pub properties: Bytes,
    /// When the Message Expiry Interval runs out.
    pub expires_at: Option<SystemTime>,
}

/// Storage for broker state that survives a restart. Calls come from lane
/// threads and must not block.
pub trait Persistence: Send + Sync + 'static {
    /// Retained messages stored earlier. Read once when the broker starts.
    ///
    /// # Errors
    ///
    /// If the store cannot be read. [`crate::Broker::new`] then fails with
    /// [`crate::Error::Persistence`].
    fn load_retained(&self) -> io::Result<Vec<Retained>>;

    /// Stores or replaces the retained message of `msg.topic`.
    fn set_retained(&self, msg: &Retained);

    /// Removes the retained message of `topic`.
    fn remove_retained(&self, topic: &str);
}

/// Changes waiting for the writer: the latest of each topic, so a slow
/// disk holds back at most one change per retained topic.
#[derive(Default)]
struct Pending {
    /// Topic to its new record body, or `None` to remove it.
    changes: HashMap<String, Option<Vec<u8>>>,
    /// Answered once the changes before them are written.
    flushes: Vec<mpsc::Sender<()>>,
    closed: bool,
}

/// The pending changes and the writer's wake.
#[derive(Default)]
struct Queue {
    pending: Mutex<Pending>,
    ready: Condvar,
}

impl Queue {
    fn lock(&self) -> MutexGuard<'_, Pending> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Replaces what is pending for `topic`.
    fn change(&self, topic: String, body: Option<Vec<u8>>) {
        let mut p = self.lock();
        if !p.closed {
            p.changes.insert(topic, body);
            self.ready.notify_one();
        }
    }

    /// Everything pending, once there is something. `None` once closed
    /// and written.
    fn take(&self) -> Option<Pending> {
        let mut p = self.lock();
        while p.changes.is_empty() && p.flushes.is_empty() && !p.closed {
            p = self.ready.wait(p).unwrap_or_else(PoisonError::into_inner);
        }
        if p.closed && p.changes.is_empty() && p.flushes.is_empty() {
            return None;
        }
        Some(Pending {
            changes: std::mem::take(&mut p.changes),
            flushes: std::mem::take(&mut p.flushes),
            closed: false,
        })
    }

    fn close(&self) {
        self.lock().closed = true;
        self.ready.notify_one();
    }
}

/// Retained messages in a log file.
pub struct FilePersistence {
    path: PathBuf,
    loaded: Mutex<Vec<Retained>>,
    queue: Arc<Queue>,
    thread: Mutex<Option<JoinHandle<()>>>,
    errors: Arc<AtomicU64>,
}

impl std::fmt::Debug for FilePersistence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FilePersistence")
            .field("path", &self.path)
            .field("write_errors", &self.write_errors())
            .finish_non_exhaustive()
    }
}

impl FilePersistence {
    /// Opens or creates the log at `path`. Writes reach the OS in batches
    /// and the disk when the OS flushes them.
    ///
    /// # Errors
    ///
    /// If the log cannot be read, compacted or created, or the writer thread
    /// cannot start.
    pub fn open(path: impl AsRef<Path>) -> io::Result<FilePersistence> {
        Self::open_with(path.as_ref(), false)
    }

    /// Like [`FilePersistence::open`], but every batch of writes is synced
    /// to disk before the next one.
    ///
    /// # Errors
    ///
    /// As [`FilePersistence::open`].
    pub fn open_synced(path: impl AsRef<Path>) -> io::Result<FilePersistence> {
        Self::open_with(path.as_ref(), true)
    }

    fn open_with(path: &Path, sync: bool) -> io::Result<FilePersistence> {
        let path = path.to_owned();
        let live = read_log(&path)?;
        let file = compact(&path, &live)?;
        let loaded = live.values().filter_map(|b| decode_set(b)).collect();
        let errors = Arc::new(AtomicU64::new(0));
        let queue = Arc::new(Queue::default());
        let writer = Writer::new(path.clone(), file, live, sync, errors.clone());
        let thread = std::thread::Builder::new()
            .name("zzq-persist".to_owned())
            .spawn({
                let queue = queue.clone();
                move || writer.run(&queue)
            })?;
        Ok(FilePersistence {
            path,
            loaded: Mutex::new(loaded),
            queue,
            thread: Mutex::new(Some(thread)),
            errors,
        })
    }

    /// Waits until every change so far is written.
    pub fn flush(&self) {
        let (ack, done) = mpsc::channel();
        {
            let mut p = self.queue.lock();
            if p.closed {
                return;
            }
            p.flushes.push(ack);
        }
        self.queue.ready.notify_one();
        let _ = done.recv();
    }

    /// Writes that failed. The broker keeps running without them.
    pub fn write_errors(&self) -> u64 {
        // Relaxed: a counter, no data rides on it.
        self.errors.load(Ordering::Relaxed)
    }
}

impl Drop for FilePersistence {
    fn drop(&mut self) {
        self.queue.close();
        if let Some(t) = self
            .thread
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = t.join();
        }
    }
}

impl Persistence for FilePersistence {
    fn load_retained(&self) -> io::Result<Vec<Retained>> {
        Ok(std::mem::take(
            &mut *self.loaded.lock().unwrap_or_else(PoisonError::into_inner),
        ))
    }

    fn set_retained(&self, msg: &Retained) {
        self.queue.change(msg.topic.clone(), Some(encode_set(msg)));
    }

    fn remove_retained(&self, topic: &str) {
        self.queue.change(topic.to_owned(), None);
    }
}

struct Writer {
    path: PathBuf,
    file: BufWriter<File>,
    /// Record bodies by topic, for compaction.
    live: HashMap<String, Vec<u8>>,
    log_len: u64,
    /// The length of a log of only `live`.
    live_len: u64,
    sync: bool,
    errors: Arc<AtomicU64>,
    /// A write or compaction failed: the log may end in a torn record, or
    /// be an old file renamed away. Set until a compaction succeeds.
    needs_compact: bool,
}

impl Writer {
    fn new(
        path: PathBuf,
        file: File,
        live: HashMap<String, Vec<u8>>,
        sync: bool,
        errors: Arc<AtomicU64>,
    ) -> Writer {
        Writer {
            path,
            file: BufWriter::new(file),
            log_len: log_len(&live),
            live_len: log_len(&live),
            live,
            sync,
            errors,
            needs_compact: false,
        }
    }

    fn run(mut self, queue: &Queue) {
        while let Some(batch) = queue.take() {
            self.write(batch.changes);
            for a in batch.flushes {
                let _ = a.send(());
            }
        }
    }

    /// Writes a batch of changes and syncs them as configured.
    fn write(&mut self, changes: HashMap<String, Option<Vec<u8>>>) {
        let mut failed = false;
        for (topic, body) in changes {
            let result = self.apply(topic, body);
            failed |= result.is_err();
            self.count(result.is_ok());
        }
        let r = self.file.flush().and_then(|()| {
            if self.sync {
                self.file.get_ref().sync_data()
            } else {
                Ok(())
            }
        });
        failed |= r.is_err();
        self.count(r.is_ok());
        // After a failed write the log may end in a torn record that
        // would hide everything after it: rewrite it from memory, until
        // that works.
        self.needs_compact |= failed;
        if self.needs_compact || self.log_len > 2 * self.live_len + COMPACT_SLACK {
            match compact(&self.path, &self.live) {
                Ok(f) => {
                    self.file = BufWriter::new(f);
                    self.log_len = self.live_len;
                    self.needs_compact = false;
                }
                Err(_) => self.count(false),
            }
        }
    }

    /// Appends a change of `topic` and applies it to the live records.
    fn apply(&mut self, topic: String, body: Option<Vec<u8>>) -> io::Result<()> {
        let (written, old) = if let Some(body) = body {
            let r = self.append(&body);
            self.live_len += record_len(&body);
            (r, self.live.insert(topic, body))
        } else {
            (
                self.append(&encode_remove(&topic)),
                self.live.remove(&topic),
            )
        };
        if let Some(old) = old {
            self.live_len -= record_len(&old);
        }
        written
    }

    fn append(&mut self, body: &[u8]) -> io::Result<()> {
        write_record(&mut self.file, body)?;
        self.log_len += record_len(body);
        Ok(())
    }

    fn count(&self, ok: bool) {
        if !ok {
            // Relaxed: see `write_errors`.
            self.errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

const fn record_len(body: &[u8]) -> u64 {
    8 + body.len() as u64
}

fn log_len(live: &HashMap<String, Vec<u8>>) -> u64 {
    let header = (MAGIC.len() + 2) as u64;
    header + live.values().map(|b| record_len(b)).sum::<u64>()
}

fn write_record(w: &mut impl Write, body: &[u8]) -> io::Result<()> {
    let len = u32::try_from(body.len()).map_err(|_| io::Error::other("record too large"))?;
    w.write_all(&len.to_be_bytes())?;
    w.write_all(&crc32(body).to_be_bytes())?;
    w.write_all(body)
}

/// Writes `live` to a fresh log next to `path`, renames it over `path` and
/// returns it open for appending.
fn compact(path: &Path, live: &HashMap<String, Vec<u8>>) -> io::Result<File> {
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(".tmp");
    let tmp = PathBuf::from(tmp);
    {
        let mut w = BufWriter::new(File::create(&tmp)?);
        w.write_all(MAGIC)?;
        w.write_all(&VERSION.to_be_bytes())?;
        for body in live.values() {
            write_record(&mut w, body)?;
        }
        w.flush()?;
        w.get_ref().sync_data()?;
    }
    fs::rename(&tmp, path)?;
    // Make the rename itself durable.
    let dir = path.parent().filter(|d| !d.as_os_str().is_empty());
    File::open(dir.unwrap_or(Path::new(".")))?.sync_all()?;
    OpenOptions::new().append(true).open(path)
}

/// Live record bodies by topic. A missing file is empty.
fn read_log(path: &Path) -> io::Result<HashMap<String, Vec<u8>>> {
    let mut data = Vec::new();
    match File::open(path) {
        Ok(mut f) => {
            f.read_to_end(&mut data)?;
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(HashMap::new()),
        Err(e) => return Err(e),
    }
    parse_log(&data).map_err(|why| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{why}: {}", path.display()),
        )
    })
}

/// The live records of a log, by topic. Stops at the first torn or
/// corrupt record.
pub(crate) fn parse_log(data: &[u8]) -> Result<HashMap<String, Vec<u8>>, &'static str> {
    let rest = data
        .strip_prefix(MAGIC.as_slice())
        .ok_or("not a zzq retained log")?;
    let (version, mut rest) = rest.split_at_checked(2).ok_or("short header")?;
    if u16::from_be_bytes([version[0], version[1]]) != VERSION {
        return Err("unsupported retained log version");
    }
    let mut live = HashMap::new();
    while let Some((body, next)) = next_record(rest) {
        rest = next;
        match body.first() {
            Some(&SET) => {
                if let Some(m) = decode_set(body) {
                    live.insert(m.topic, body.to_vec());
                }
            }
            Some(&REMOVE) => {
                if let Some(topic) = Fields(&body[1..]).topic() {
                    live.remove(topic);
                }
            }
            _ => break,
        }
    }
    Ok(live)
}

/// The next record body if it is whole and intact.
fn next_record(buf: &[u8]) -> Option<(&[u8], &[u8])> {
    let (head, rest) = buf.split_at_checked(8)?;
    let len = u32::from_be_bytes([head[0], head[1], head[2], head[3]]) as usize;
    let crc = u32::from_be_bytes([head[4], head[5], head[6], head[7]]);
    let (body, rest) = rest.split_at_checked(len)?;
    (crc32(body) == crc).then_some((body, rest))
}

fn encode_set(m: &Retained) -> Vec<u8> {
    let mut b = Vec::with_capacity(24 + m.topic.len() + m.properties.len() + m.payload.len());
    b.push(SET);
    put_topic(&mut b, &m.topic);
    b.push(m.qos as u8);
    let ms = m
        .expires_at
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| {
            u64::try_from(d.as_millis()).unwrap_or(u64::MAX).max(1)
        });
    b.extend_from_slice(&ms.to_be_bytes());
    put_bytes(&mut b, &m.properties);
    put_bytes(&mut b, &m.payload);
    b
}

fn encode_remove(topic: &str) -> Vec<u8> {
    let mut b = vec![REMOVE];
    put_topic(&mut b, topic);
    b
}

fn put_topic(b: &mut Vec<u8>, topic: &str) {
    let len = u16::try_from(topic.len()).unwrap_or(u16::MAX);
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(&topic.as_bytes()[..usize::from(len)]);
}

fn put_bytes(b: &mut Vec<u8>, data: &[u8]) {
    let len = u32::try_from(data.len()).unwrap_or(u32::MAX);
    b.extend_from_slice(&len.to_be_bytes());
    b.extend_from_slice(data);
}

fn decode_set(body: &[u8]) -> Option<Retained> {
    let mut f = Fields(body.strip_prefix(&[SET])?);
    let topic = f.topic()?.to_owned();
    let qos = QoS::try_from(f.take(1)?[0]).ok()?;
    let ms = u64::from_be_bytes(f.take(8)?.try_into().ok()?);
    let properties = Bytes::copy_from_slice(f.bytes()?);
    let payload = Bytes::copy_from_slice(f.bytes()?);
    let expires_at = (ms != 0).then(|| UNIX_EPOCH + Duration::from_millis(ms));
    Some(Retained {
        topic,
        payload,
        qos,
        properties,
        expires_at,
    })
}

struct Fields<'a>(&'a [u8]);

impl<'a> Fields<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(n)?;
        self.0 = rest;
        Some(head)
    }

    fn topic(&mut self) -> Option<&'a str> {
        let len = self.take(2)?;
        let len = usize::from(u16::from_be_bytes([len[0], len[1]]));
        std::str::from_utf8(self.take(len)?).ok()
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.take(4)?;
        let len = u32::from_be_bytes([len[0], len[1], len[2], len[3]]) as usize;
        self.take(len)
    }
}

/// CRC-32 (IEEE), bitwise. Records are written off the lanes.
fn crc32(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in data {
        c ^= u32::from(b);
        for _ in 0..8 {
            c = if c & 1 == 0 {
                c >> 1
            } else {
                (c >> 1) ^ 0xEDB8_8320
            };
        }
    }
    !c
}

#[cfg(test)]
mod tests {
    use super::*;

    fn msg(topic: &str, payload: &'static str) -> Retained {
        Retained {
            topic: topic.to_owned(),
            payload: Bytes::from_static(payload.as_bytes()),
            qos: QoS::AtLeastOnce,
            properties: Bytes::from_static(&[0x01, 0x01]),
            expires_at: Some(UNIX_EPOCH + Duration::from_secs(4_000_000_000)),
        }
    }

    fn sorted(mut v: Vec<Retained>) -> Vec<Retained> {
        v.sort_by(|a, b| a.topic.cmp(&b.topic));
        v
    }

    #[test]
    fn a_topic_waits_for_the_writer_with_its_latest_change_only() {
        let q = Queue::default();
        for i in 0..1000u32 {
            q.change("a".to_owned(), Some(i.to_be_bytes().to_vec()));
        }
        q.change("b".to_owned(), None);
        let batch = q.take().unwrap();
        assert_eq!(batch.changes.len(), 2);
        assert_eq!(batch.changes["a"], Some(999u32.to_be_bytes().to_vec()));
        q.close();
        assert!(q.take().is_none(), "closed and written");
    }

    #[test]
    fn short_or_foreign_logs_are_refused() {
        let dir = std::env::temp_dir().join(format!("zzq-persist-bad-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("retained.log");
        for (bytes, why) in [
            (b"ZZQRET\x00".to_vec(), "short header"),
            (b"other".to_vec(), "not a zzq retained log"),
        ] {
            fs::write(&path, bytes).unwrap();
            let e = FilePersistence::open(&path).unwrap_err();
            assert_eq!(e.kind(), io::ErrorKind::InvalidData);
            assert!(e.to_string().starts_with(why), "{e}");
        }
        let p = FilePersistence::open(dir.join("fresh.log")).unwrap();
        assert!(format!("{p:?}").starts_with("FilePersistence"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn survives_reopen_and_torn_tail() {
        let dir = std::env::temp_dir().join(format!("zzq-persist-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("retained.log");
        let _ = fs::remove_file(&path);
        {
            let p = FilePersistence::open(&path).unwrap();
            assert_eq!(p.load_retained().unwrap(), []);
            p.set_retained(&msg("a", "1"));
            p.set_retained(&msg("b", "2"));
            p.set_retained(&msg("a", "3"));
            p.remove_retained("b");
            p.set_retained(&msg("c", "4"));
            p.flush();
        }
        // A torn record at the end is dropped.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[0, 0, 0, 40, 1, 2, 3]).unwrap();
        drop(f);
        let p = FilePersistence::open_synced(&path).unwrap();
        assert_eq!(
            sorted(p.load_retained().unwrap()),
            vec![msg("a", "3"), msg("c", "4")]
        );
        drop(p);
        let p = FilePersistence::open(&path).unwrap();
        assert_eq!(p.load_retained().unwrap().len(), 2);
        drop(p);
        fs::write(&path, b"garbage").unwrap();
        assert!(FilePersistence::open(&path).is_err());
        let _ = fs::remove_dir_all(&dir);
    }

    /// A compaction that failed after a failed write is tried again with the
    /// next batch, so the changes after a torn record are not lost.
    #[cfg(target_os = "linux")]
    #[test]
    fn a_failed_compaction_is_tried_again() {
        let dir = std::env::temp_dir().join(format!("zzq-persist-retry-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("retained.log");
        // A log that ends in a torn record, as a write to a full disk leaves.
        let mut torn = MAGIC.to_vec();
        torn.extend_from_slice(&VERSION.to_be_bytes());
        torn.extend_from_slice(&[0, 0, 0, 40, 1, 2, 3]);
        fs::write(&path, torn).unwrap();
        // Compaction fails while something is in the way of its new file.
        let blocker = dir.join("retained.log.tmp");
        fs::create_dir(&blocker).unwrap();
        let full = OpenOptions::new().write(true).open("/dev/full").unwrap();
        let mut w = Writer::new(path.clone(), full, HashMap::new(), false, Arc::default());
        w.write(HashMap::from([(
            "a".to_owned(),
            Some(encode_set(&msg("a", "1"))),
        )]));
        // The disk has room again: appends land behind the torn record.
        fs::remove_dir(&blocker).unwrap();
        w.file = BufWriter::new(OpenOptions::new().append(true).open(&path).unwrap());
        w.write(HashMap::from([(
            "b".to_owned(),
            Some(encode_set(&msg("b", "2"))),
        )]));
        let live = read_log(&path).unwrap();
        let mut topics: Vec<_> = live.keys().cloned().collect();
        topics.sort();
        assert_eq!(topics, ["a", "b"]);
        let _ = fs::remove_dir_all(&dir);
    }
}
