//! The data directory needs Linux.

use std::io;
use std::path::Path;
use std::task::{Context, Poll};

use super::{Completion, Log};

/// A journal record.
#[derive(Debug)]
pub(crate) struct Record<'a> {
    pub(crate) kind: u8,
    pub(crate) body: &'a [u8],
}

/// No data directory.
#[derive(Debug)]
pub(crate) enum DataDir {}

impl DataDir {
    pub(crate) fn open(_path: &Path, _max_packet: usize) -> io::Result<DataDir> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "a data directory needs Linux",
        ))
    }

    pub(crate) fn path(&self) -> &Path {
        match *self {}
    }

    pub(crate) const fn room(&self) -> u64 {
        match *self {}
    }

    pub(crate) fn journal(
        &self,
        _name: &str,
        _replay: impl FnMut(Record<'_>) -> Result<(), &'static str>,
    ) -> io::Result<Log> {
        match *self {}
    }
}

/// No journal on disk.
#[derive(Debug)]
pub(crate) enum DiskLog {}

impl DiskLog {
    pub(crate) fn append(&mut self, _kind: u8, _parts: &[&[u8]]) -> io::Result<u64> {
        match *self {}
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        match *self {}
    }

    pub(crate) const fn backlog(&self) -> usize {
        match *self {}
    }

    pub(crate) const fn written_bytes(&self) -> u64 {
        match *self {}
    }

    pub(crate) const fn full(&self) -> bool {
        match *self {}
    }

    pub(crate) fn roll(&mut self) -> io::Result<Option<u64>> {
        match *self {}
    }

    pub(crate) fn trim(&mut self, _before: u64) -> io::Result<()> {
        match *self {}
    }

    pub(crate) fn sync(&mut self) -> io::Result<u64> {
        match *self {}
    }

    pub(crate) fn poll_complete(&mut self, _cx: &mut Context<'_>) -> Poll<Completion> {
        match *self {}
    }
}
