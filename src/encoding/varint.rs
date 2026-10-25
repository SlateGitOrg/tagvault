//! LEB128 varints, zigzag, and a bounds-checked little-endian byte cursor.

use crate::error::{Error, Result};

/// Append an unsigned LEB128 varint.
pub fn put_uvarint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Zigzag-map a signed integer so small magnitudes encode small.
pub fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

/// Inverse of [`zigzag`].
pub fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Append a signed (zigzag) varint.
pub fn put_ivarint(out: &mut Vec<u8>, v: i64) {
    put_uvarint(out, zigzag(v));
}

/// Append a length-prefixed UTF-8 string.
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_uvarint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

/// Cursor over a byte slice; every read is bounds-checked and returns [`Error::Corrupt`] on underflow.
#[derive(Debug, Clone)]
pub struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    /// New cursor at offset 0.
    pub fn new(buf: &'a [u8]) -> Self {
        Cursor { buf, pos: 0 }
    }

    /// Bytes not yet consumed.
    pub fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    /// Current offset.
    pub fn position(&self) -> usize {
        self.pos
    }

    /// Take the next `n` bytes.
    pub fn bytes(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.remaining() < n {
            return Err(Error::Corrupt(format!("need {n} bytes, have {}", self.remaining())));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    /// Read one byte.
    pub fn u8(&mut self) -> Result<u8> {
        Ok(self.bytes(1)?[0])
    }

    /// Read a little-endian u32.
    pub fn u32(&mut self) -> Result<u32> {
        let b = self.bytes(4)?;
        Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
    }

    /// Read a little-endian u64.
    pub fn u64(&mut self) -> Result<u64> {
        let b = self.bytes(8)?;
        let mut a = [0u8; 8];
        a.copy_from_slice(b);
        Ok(u64::from_le_bytes(a))
    }

    /// Read a little-endian i64.
    pub fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    /// Read a little-endian f64.
    pub fn f64(&mut self) -> Result<f64> {
        Ok(f64::from_bits(self.u64()?))
    }

    /// Read an unsigned LEB128 varint (max 10 bytes).
    pub fn uvarint(&mut self) -> Result<u64> {
        let mut result: u64 = 0;
        for i in 0..10 {
            let b = self.u8()?;
            result |= ((b & 0x7f) as u64) << (7 * i);
            if b & 0x80 == 0 {
                return Ok(result);
            }
        }
        Err(Error::Corrupt("varint too long".into()))
    }

    /// Read a zigzag varint.
    pub fn ivarint(&mut self) -> Result<i64> {
        Ok(unzigzag(self.uvarint()?))
    }

    /// Read a length-prefixed UTF-8 string (bounded by the remaining input).
    pub fn string(&mut self) -> Result<String> {
        let len = self.uvarint()? as usize;
        let b = self.bytes(len)?;
        String::from_utf8(b.to_vec()).map_err(|_| Error::Corrupt("invalid utf-8".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_roundtrip() {
        let mut buf = Vec::new();
        let vals = [0u64, 1, 127, 128, 300, u32::MAX as u64, u64::MAX];
        for v in vals {
            put_uvarint(&mut buf, v);
        }
        for v in [-1i64, 0, 63, -64, i64::MIN, i64::MAX] {
            put_ivarint(&mut buf, v);
        }
        put_str(&mut buf, "Tank01/Level");
        let mut c = Cursor::new(&buf);
        for v in vals {
            assert_eq!(c.uvarint().unwrap(), v);
        }
        for v in [-1i64, 0, 63, -64, i64::MIN, i64::MAX] {
            assert_eq!(c.ivarint().unwrap(), v);
        }
        assert_eq!(c.string().unwrap(), "Tank01/Level");
        assert_eq!(c.remaining(), 0);
        assert!(c.u8().is_err());
    }

    #[test]
    fn malformed_inputs_error() {
        assert!(Cursor::new(&[0xff; 11]).uvarint().is_err());
        assert!(Cursor::new(&[5, b'a']).string().is_err());
        assert!(Cursor::new(&[2, 0xff, 0xfe]).string().is_err());
        assert!(Cursor::new(&[1, 2, 3]).u32().is_err());
    }
}
