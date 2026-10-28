//! Byte layout of a segment, big endian.
//!
//! ```text
//! segment = header group* free
//! header  = "ZZQSEGMT" digest:u64 version:u16 0:u16 0:u32 id:u64
//!           capacity:u64 0:u64[3], zeros to 4096 bytes
//! group   = "ZZQGROUP" digest:u64 segment:u64 number:u64 length:u32
//!           records:u32 payload:u32 0:u32 0:u64[2] record* zeros
//! record  = len:u32 kind:u8 body
//! ```
//!
//! A digest is XXH3-64 of the bytes from the end of the digest to the end
//! of the header, and for a group on through its records. A group starts
//! on a 4096-byte boundary; `length`, padding included, is a multiple of
//! 4096, and `payload` counts the record bytes. Group numbers rise by one
//! through a journal, across segments. Free space holds zeros, or groups
//! from an earlier use of the file, which name another segment.

use std::io;

use xxhash_rust::xxh3::xxh3_64;

use crate::ReplayError;
use crate::aio::{ALIGNMENT, AlignedBuf};

/// Alignment of segments, groups and their lengths.
pub(crate) const PAGE: usize = ALIGNMENT;
pub(crate) const GROUP_HEADER: usize = 64;
pub(crate) const RECORD_HEADER: usize = 5;
const SEGMENT_MAGIC: &[u8; 8] = b"ZZQSEGMT";
const GROUP_MAGIC: &[u8; 8] = b"ZZQGROUP";
const VERSION: u16 = 1;

/// The bytes of a segment's first page.
pub(crate) fn segment_header(id: u64, capacity: u64) -> Vec<u8> {
    let mut page = vec![0; PAGE];
    page[..8].copy_from_slice(SEGMENT_MAGIC);
    page[16..18].copy_from_slice(&VERSION.to_be_bytes());
    page[24..32].copy_from_slice(&id.to_be_bytes());
    page[32..40].copy_from_slice(&capacity.to_be_bytes());
    let digest = xxh3_64(&page[16..64]);
    page[8..16].copy_from_slice(&digest.to_be_bytes());
    page
}

/// Id and capacity from a segment's first page.
pub(crate) fn read_segment_header(bytes: &[u8]) -> Result<(u64, u64), &'static str> {
    let page = bytes.get(..PAGE).ok_or("segment shorter than its header")?;
    if page[..8] != *SEGMENT_MAGIC {
        return Err("not a segment");
    }
    if be64(page, 8) != xxh3_64(&page[16..64]) {
        return Err("segment header damaged");
    }
    if u16::from_be_bytes([page[16], page[17]]) != VERSION
        || page[18..24] != [0; 6]
        || page[40..64] != [0; 24]
    {
        return Err("unknown segment version");
    }
    let capacity = be64(page, 32);
    if capacity < 2 * PAGE as u64 || !capacity.is_multiple_of(PAGE as u64) {
        return Err("segment capacity out of range");
    }
    Ok((be64(page, 24), capacity))
}

/// Bytes a group of `bytes` of header and records takes, padded.
pub(crate) const fn group_len(bytes: usize) -> usize {
    bytes.next_multiple_of(PAGE)
}

/// Starts a group in an empty `buf`: room for its header.
pub(crate) fn begin(buf: &mut AlignedBuf) -> io::Result<()> {
    buf.extend(&[0; GROUP_HEADER])
}

/// Appends a record of `kind` whose body is `parts` joined.
pub(crate) fn record(buf: &mut AlignedBuf, kind: u8, parts: &[&[u8]]) -> io::Result<()> {
    let len = parts.iter().map(|p| p.len()).sum::<usize>();
    let len = u32::try_from(len).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
    buf.reserve(RECORD_HEADER + len as usize)?;
    buf.extend(&len.to_be_bytes())?;
    buf.extend(&[kind])?;
    for part in parts {
        buf.extend(part)?;
    }
    Ok(())
}

/// Pads a group that [`begin`] started and fills in its header.
pub(crate) fn seal(
    buf: &mut AlignedBuf,
    segment: u64,
    number: u64,
    records: u32,
) -> io::Result<()> {
    let end = buf.len();
    let invalid = || io::Error::from(io::ErrorKind::InvalidInput);
    let payload =
        u32::try_from(end.checked_sub(GROUP_HEADER).ok_or_else(invalid)?).map_err(|_| invalid())?;
    buf.pad()?;
    let length = u32::try_from(buf.len()).map_err(|_| invalid())?;
    let bytes = buf.as_mut_slice();
    bytes[..8].copy_from_slice(GROUP_MAGIC);
    bytes[16..24].copy_from_slice(&segment.to_be_bytes());
    bytes[24..32].copy_from_slice(&number.to_be_bytes());
    bytes[32..36].copy_from_slice(&length.to_be_bytes());
    bytes[36..40].copy_from_slice(&records.to_be_bytes());
    bytes[40..44].copy_from_slice(&payload.to_be_bytes());
    let digest = xxh3_64(&bytes[16..end]);
    bytes[8..16].copy_from_slice(&digest.to_be_bytes());
    Ok(())
}

/// What a scan of one segment found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Scanned {
    /// The segment's id, from its header.
    pub(crate) id: u64,
    pub(crate) capacity: u64,
    /// Where the next group goes: the end of the last valid one.
    pub(crate) end: u64,
    /// The number of the last valid group, if any.
    pub(crate) last: Option<u64>,
}

/// Why a scan stopped short.
#[derive(Debug)]
pub(crate) enum ScanError {
    Corrupt { offset: u64, reason: &'static str },
    Replay(ReplayError),
}

/// Reads the groups of a whole segment file and hands each record to
/// `each`, oldest first. The first group must have number `first`, if
/// given, and each later one the next number. The log ends at the first
/// group that does not validate: a write that never finished. A valid
/// group of this segment there or further on, such as one out of order,
/// means damage inside what was written, and fails the scan.
pub(crate) fn scan<'a>(
    bytes: &'a [u8],
    first: Option<u64>,
    each: &mut dyn FnMut(u8, &'a [u8]) -> Result<(), ReplayError>,
) -> Result<Scanned, ScanError> {
    let corrupt = |offset: usize, reason| ScanError::Corrupt {
        offset: offset as u64,
        reason,
    };
    let (id, capacity) = read_segment_header(bytes).map_err(|r| corrupt(0, r))?;
    if capacity != bytes.len() as u64 {
        return Err(corrupt(0, "segment length differs from its header"));
    }
    let (mut offset, mut expect, mut last) = (PAGE, first, None);
    while let Some(group) = group_at(bytes, offset, id, expect) {
        records(group.payload, group.records, each).map_err(|e| match e {
            Some(e) => ScanError::Replay(e),
            None => corrupt(offset, "records do not fill their group"),
        })?;
        last = Some(group.number);
        expect = group.number.checked_add(1);
        offset += group.length;
    }
    let mut probe = offset;
    while probe < bytes.len() {
        if group_at(bytes, probe, id, None).is_some() {
            return Err(corrupt(probe, "a valid group follows a damaged one"));
        }
        probe += PAGE;
    }
    Ok(Scanned {
        id,
        capacity,
        end: offset as u64,
        last,
    })
}

struct Group<'a> {
    number: u64,
    length: usize,
    records: u32,
    payload: &'a [u8],
}

/// The group at `offset` of segment `segment`, if one validates there.
fn group_at(bytes: &[u8], offset: usize, segment: u64, expect: Option<u64>) -> Option<Group<'_>> {
    let rest = bytes.get(offset..)?;
    let head = rest.get(..GROUP_HEADER)?;
    if head[..8] != *GROUP_MAGIC || be64(head, 16) != segment {
        return None;
    }
    let number = be64(head, 24);
    if expect.is_some_and(|e| e != number) {
        return None;
    }
    let length = be32(head, 32) as usize;
    let records = be32(head, 36);
    let payload = be32(head, 40) as usize;
    if length < PAGE
        || !length.is_multiple_of(PAGE)
        || payload > length - GROUP_HEADER
        || head[44..64] != [0; 20]
    {
        return None;
    }
    let group = rest.get(..length)?;
    let end = GROUP_HEADER + payload;
    (xxh3_64(&group[16..end]) == be64(head, 8)).then(|| Group {
        number,
        length,
        records,
        payload: &group[GROUP_HEADER..end],
    })
}

/// Hands the `count` records of `payload` to `each`. `Err(None)` if they do
/// not fill it exactly.
fn records<'a>(
    mut payload: &'a [u8],
    count: u32,
    each: &mut dyn FnMut(u8, &'a [u8]) -> Result<(), ReplayError>,
) -> Result<(), Option<ReplayError>> {
    for _ in 0..count {
        let (head, rest) = payload.split_at_checked(RECORD_HEADER).ok_or(None)?;
        let (body, rest) = rest.split_at_checked(be32(head, 0) as usize).ok_or(None)?;
        each(head[4], body).map_err(Some)?;
        payload = rest;
    }
    if payload.is_empty() {
        Ok(())
    } else {
        Err(None)
    }
}

fn be32(bytes: &[u8], at: usize) -> u32 {
    let mut b = [0; 4];
    b.copy_from_slice(&bytes[at..at + 4]);
    u32::from_be_bytes(b)
}

fn be64(bytes: &[u8], at: usize) -> u64 {
    let mut b = [0; 8];
    b.copy_from_slice(&bytes[at..at + 8]);
    u64::from_be_bytes(b)
}

/// A segment holding `records`, `per_group` to a group, with one free page
/// after them.
#[cfg(any(test, feature = "fuzzing"))]
pub(crate) fn build(records: &[(u8, &[u8])], per_group: usize) -> io::Result<Vec<u8>> {
    let mut groups = Vec::new();
    for (number, chunk) in (1..).zip(records.chunks(per_group.max(1))) {
        let mut buf = AlignedBuf::default();
        begin(&mut buf)?;
        for (kind, body) in chunk {
            record(&mut buf, *kind, &[body])?;
        }
        let count = u32::try_from(chunk.len()).map_err(|_| io::ErrorKind::InvalidInput)?;
        seal(&mut buf, 1, number, count)?;
        groups.extend_from_slice(buf.as_slice());
    }
    let capacity = (2 * PAGE + groups.len()) as u64;
    let mut bytes = segment_header(1, capacity);
    bytes.extend_from_slice(&groups);
    bytes.resize(capacity as usize, 0);
    Ok(bytes)
}

#[cfg(test)]
mod tests;
