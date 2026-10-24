//! MQTT variable byte integers.

use crate::DecodeError;

/// Largest value a variable byte integer can hold.
pub const MAX: usize = 268_435_455;

/// Decodes a variable byte integer from the start of `buf`.
///
/// Returns `Ok(None)` if `buf` ends before the integer does, and
/// `(value, length)` otherwise.
#[inline]
pub fn decode(buf: &[u8]) -> Result<Option<(usize, usize)>, DecodeError> {
    let mut value = 0usize;
    for i in 0..4 {
        let Some(&b) = buf.get(i) else {
            return Ok(None);
        };
        value |= usize::from(b & 0x7f) << (7 * i);
        if b & 0x80 == 0 {
            return Ok(Some((value, i + 1)));
        }
    }
    Err(DecodeError::Malformed)
}

/// Number of bytes `value` takes on the wire.
#[inline]
pub const fn len(value: usize) -> usize {
    if value < 128 {
        1
    } else if value < 16_384 {
        2
    } else if value < 2_097_152 {
        3
    } else {
        4
    }
}

/// Encodes `value` into `out` and returns the number of bytes written.
#[inline]
pub const fn encode(mut value: usize, out: &mut [u8; 4]) -> usize {
    let mut i = 0;
    loop {
        let mut b = (value & 0x7f) as u8;
        value >>= 7;
        if value > 0 {
            b |= 0x80;
        }
        out[i] = b;
        i += 1;
        if value == 0 || i == 4 {
            return i;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_length_class_round_trips() {
        for v in [0, 1, 127, 128, 16_383, 16_384, 2_097_151, 2_097_152, MAX] {
            let mut out = [0; 4];
            let n = encode(v, &mut out);
            assert_eq!(n, len(v));
            assert_eq!(decode(&out[..n]), Ok(Some((v, n))));
        }
    }

    #[test]
    fn short_input_is_incomplete_and_a_fifth_byte_malformed() {
        assert_eq!(decode(&[0x80]), Ok(None));
        assert_eq!(
            decode(&[0x80, 0x80, 0x80, 0x80]),
            Err(DecodeError::Malformed)
        );
    }
}
