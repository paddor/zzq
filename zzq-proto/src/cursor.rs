use crate::property::Rules;
use crate::{DecodeError, Properties, varint};

/// Reads fields from a packet body that is already complete.
#[derive(Debug)]
pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
}

impl<'a> Cursor<'a> {
    #[inline]
    pub(crate) const fn new(buf: &'a [u8]) -> Self {
        Cursor { buf }
    }

    #[inline]
    pub(crate) const fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    #[inline]
    pub(crate) const fn remaining(&self) -> usize {
        self.buf.len()
    }

    #[inline]
    pub(crate) fn rest(&mut self) -> &'a [u8] {
        core::mem::take(&mut self.buf)
    }

    #[inline]
    pub(crate) const fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < n {
            return Err(DecodeError::Malformed);
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    #[inline]
    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.bytes(1)?[0])
    }

    #[inline]
    pub(crate) fn u16(&mut self) -> Result<u16, DecodeError> {
        let b = self.bytes(2)?;
        Ok(u16::from_be_bytes([b[0], b[1]]))
    }

    #[inline]
    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        let b = self.bytes(4)?;
        Ok(u32::from_be_bytes([b[0], b[1], b[2], b[3]]))
    }

    #[inline]
    pub(crate) fn varint(&mut self) -> Result<usize, DecodeError> {
        let (v, n) = varint::decode(self.buf)?.ok_or(DecodeError::Malformed)?;
        self.buf = &self.buf[n..];
        Ok(v)
    }

    /// Two-byte length-prefixed binary data.
    #[inline]
    pub(crate) fn binary(&mut self) -> Result<&'a [u8], DecodeError> {
        let n = self.u16()?;
        self.bytes(usize::from(n))
    }

    /// Two-byte length-prefixed UTF-8 string without U+0000.
    #[inline]
    pub(crate) fn string(&mut self) -> Result<&'a str, DecodeError> {
        let b = self.binary()?;
        utf8(b)
    }

    /// v5 property block: varint length, then the properties, checked
    /// against the packet's `rules`.
    #[inline]
    pub(crate) fn properties(&mut self, rules: Rules) -> Result<Properties<'a>, DecodeError> {
        let n = self.varint()?;
        if n == 0 {
            return Ok(Properties::EMPTY);
        }
        Properties::parse(self.bytes(n)?, rules)
    }
}

/// Validates an MQTT UTF-8 string.
#[inline]
pub(crate) fn utf8(b: &[u8]) -> Result<&str, DecodeError> {
    let s = core::str::from_utf8(b).map_err(|_| DecodeError::InvalidUtf8)?;
    if b.contains(&0) {
        return Err(DecodeError::InvalidUtf8);
    }
    Ok(s)
}
