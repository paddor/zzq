//! A node's exit dump: its part of the replicated history, then an image
//! of the state its application held through the op that part names.
//!
//! ```text
//! dump = "ZZQCDUMP" node_len:u32 node image xxh3_64(all before):u64
//! ```

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use bytes::{BufMut, Bytes, BytesMut};
use xxhash_rust::xxh3::xxh3_64;

use crate::dump::{sync_dir, with_suffix};

const MAGIC: &[u8; 8] = b"ZZQCDUMP";

/// The two parts of a dump, as slices of the bytes read.
#[derive(Debug)]
pub(crate) struct Dump {
    /// What `Node::export` gave.
    pub(crate) node: Bytes,
    /// The application's image, every part through the node's op.
    pub(crate) image: Bytes,
}

pub(crate) fn encode(node: &[u8], image: &[u8]) -> Bytes {
    let mut b = BytesMut::with_capacity(MAGIC.len() + 12 + node.len() + image.len());
    b.put_slice(MAGIC);
    b.put_u32(u32::try_from(node.len()).unwrap_or(u32::MAX));
    b.put_slice(node);
    b.put_slice(image);
    let check = xxh3_64(&b);
    b.put_u64(check);
    b.freeze()
}

/// # Errors
///
/// When the bytes are cut short, damaged or no dump of a node.
pub(crate) fn decode(bytes: &Bytes) -> Result<Dump, &'static str> {
    let (body, check) = bytes.split_last_chunk::<8>().ok_or("dump cut short")?;
    if xxh3_64(body) != u64::from_be_bytes(*check) {
        return Err("dump damaged");
    }
    let rest = body.strip_prefix(MAGIC).ok_or("not a node's dump")?;
    let (len, rest) = rest.split_first_chunk::<4>().ok_or("dump cut short")?;
    let len = u32::from_be_bytes(*len) as usize;
    let node = rest.get(..len).ok_or("dump cut short")?;
    Ok(Dump {
        node: bytes.slice_ref(node),
        image: bytes.slice_ref(&rest[len..]),
    })
}

/// Writes `bytes` to `path` whole: a synced temporary file, renamed.
pub(crate) fn write(path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = with_suffix(path, ".tmp");
    let written = File::create(&tmp).and_then(|mut f| {
        f.write_all(bytes)?;
        f.sync_all()
    });
    if let Err(e) = written {
        let _ = fs::remove_file(&tmp);
        return Err(e);
    }
    fs::rename(&tmp, path)?;
    sync_dir(path)
}

/// The dump at `path`, if a node left one there.
pub(crate) fn read(path: &Path) -> io::Result<Option<Bytes>> {
    match fs::read(path) {
        Ok(b) => Ok(Some(Bytes::from(b))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Sets the dump at `path` aside once the node runs from it: a crash
/// after that leaves no dump, and the node recovers from the others.
pub(crate) fn set_aside(path: &Path) -> io::Result<()> {
    fs::rename(path, with_suffix(path, ".loaded"))?;
    sync_dir(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dump_decodes_to_its_parts() {
        let bytes = encode(b"node", b"image");
        let d = decode(&bytes).unwrap();
        assert_eq!((&d.node[..], &d.image[..]), (&b"node"[..], &b"image"[..]));
    }

    #[test]
    fn a_damaged_short_or_foreign_dump_is_refused() {
        let bytes = encode(b"node", b"image");
        for n in [0, 7, 12, bytes.len() - 1] {
            assert!(decode(&bytes.slice(..n)).is_err(), "cut at {n}");
        }
        let mut flipped = bytes.to_vec();
        flipped[9] ^= 1;
        assert_eq!(decode(&Bytes::from(flipped)).unwrap_err(), "dump damaged");
        let mut foreign = b"ZZQXDUMP".to_vec();
        foreign.extend_from_slice(&[0; 4]);
        let check = xxh3_64(&foreign);
        foreign.extend_from_slice(&check.to_be_bytes());
        assert!(decode(&Bytes::from(foreign)).is_err());
        // A length past the end.
        let mut long = MAGIC.to_vec();
        long.extend_from_slice(&9u32.to_be_bytes());
        let check = xxh3_64(&long);
        long.extend_from_slice(&check.to_be_bytes());
        assert_eq!(decode(&Bytes::from(long)).unwrap_err(), "dump cut short");
    }

    #[test]
    fn a_written_dump_reads_back_until_it_is_set_aside() {
        let dir = std::env::temp_dir().join(format!("zzq-cluster-dump-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("node.dump");
        assert_eq!(read(&path).unwrap(), None);
        write(&path, b"dumped").unwrap();
        assert_eq!(read(&path).unwrap().as_deref(), Some(&b"dumped"[..]));
        set_aside(&path).unwrap();
        assert_eq!(read(&path).unwrap(), None);
        assert!(dir.join("node.dump.loaded").exists());
        fs::remove_dir_all(&dir).unwrap();
    }
}
