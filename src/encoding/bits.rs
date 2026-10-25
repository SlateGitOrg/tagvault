//! MSB-first bit stream writer and reader.

use crate::error::{Error, Result};

/// Appends bits most-significant-first into a byte vector.
#[derive(Debug, Default, Clone)]
pub struct BitWriter {
    buf: Vec<u8>,
    /// Number of bits already used in the last byte (0 means the last byte is full or absent).
    used: u32,
}

impl BitWriter {
    /// New empty writer.
    pub fn new() -> Self {
        Self::default()
    }

    /// Write a single bit.
    pub fn write_bit(&mut self, bit: bool) {
        if self.used == 0 {
            self.buf.push(0);
        }
        if bit {
            let last = self.buf.len() - 1;
            self.buf[last] |= 1 << (7 - self.used);
        }
        self.used = (self.used + 1) % 8;
    }

    /// Write the low `n` bits of `value` (n ≤ 64), most significant first.
    pub fn write_bits(&mut self, value: u64, n: u32) {
        debug_assert!(n <= 64);
        let mut remaining = n;
        while remaining > 0 {
            if self.used == 0 {
                self.buf.push(0);
            }
            let free = 8 - self.used;
            let take = free.min(remaining);
            let shift = remaining - take;
            let chunk = ((value >> shift) & ((1u64 << take) - 1)) as u8;
            let last = self.buf.len() - 1;
            self.buf[last] |= chunk << (free - take);
            self.used = (self.used + take) % 8;
            remaining -= take;
        }
    }

    /// Total number of bits written.
    pub fn bit_len(&self) -> usize {
        if self.used == 0 {
            self.buf.len() * 8
        } else {
            (self.buf.len() - 1) * 8 + self.used as usize
        }
    }

    /// Finish and return the padded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.buf
    }
}

/// Reads bits most-significant-first from a byte slice.
#[derive(Debug, Clone)]
pub struct BitReader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> BitReader<'a> {
    /// Reader positioned at the first bit of `buf`.
    pub fn new(buf: &'a [u8]) -> Self {
        BitReader { buf, pos: 0 }
    }

    /// Read one bit.
    pub fn read_bit(&mut self) -> Result<bool> {
        let byte = self.buf.get(self.pos / 8).ok_or_else(|| Error::Corrupt("bit stream exhausted".into()))?;
        let bit = (byte >> (7 - (self.pos % 8))) & 1 == 1;
        self.pos += 1;
        Ok(bit)
    }

    /// Read `n` bits (n ≤ 64) as an unsigned integer.
    pub fn read_bits(&mut self, n: u32) -> Result<u64> {
        debug_assert!(n <= 64);
        if self.pos + n as usize > self.buf.len() * 8 {
            return Err(Error::Corrupt("bit stream exhausted".into()));
        }
        let mut out: u64 = 0;
        let mut remaining = n;
        while remaining > 0 {
            let byte = self.buf[self.pos / 8];
            let offset = (self.pos % 8) as u32;
            let avail = 8 - offset;
            let take = avail.min(remaining);
            let chunk = (byte >> (avail - take)) & ((1u16 << take) - 1) as u8;
            out = (out << take) | chunk as u64;
            self.pos += take as usize;
            remaining -= take;
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_mixed_widths() {
        let mut w = BitWriter::new();
        let items: Vec<(u64, u32)> = vec![(1, 1), (0b101, 3), (0xdead_beef, 32), (u64::MAX, 64), (0, 7), (42, 9)];
        for &(v, n) in &items {
            w.write_bits(v, n);
        }
        w.write_bit(true);
        let total: u32 = items.iter().map(|&(_, n)| n).sum::<u32>() + 1;
        assert_eq!(w.bit_len(), total as usize);
        let bytes = w.into_bytes();
        let mut r = BitReader::new(&bytes);
        for &(v, n) in &items {
            let mask = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
            assert_eq!(r.read_bits(n).unwrap(), v & mask);
        }
        assert!(r.read_bit().unwrap());
    }

    #[test]
    fn exhausted_stream_errors() {
        let mut r = BitReader::new(&[0xff]);
        assert_eq!(r.read_bits(8).unwrap(), 0xff);
        assert!(r.read_bit().is_err());
        assert!(BitReader::new(&[0]).read_bits(9).is_err());
    }
}
