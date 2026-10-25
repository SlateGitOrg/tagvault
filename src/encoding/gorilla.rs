//! Gorilla-style column codecs.
//!
//! **Timestamps** — the first timestamp is stored raw; every later timestamp is stored as the
//! *delta of deltas* (dod) divided by a per-chunk `unit` (the GCD of all deltas, so a 1 s scan
//! rate in milliseconds costs nothing extra). Control prefixes select a bucket:
//!
//! | prefix  | payload             | dod range (in units) |
//! |---------|---------------------|----------------------|
//! | `0`     | –                   | 0                    |
//! | `10`    | 7 bits              | [-63, 64]            |
//! | `110`   | 9 bits              | [-255, 256]          |
//! | `1110`  | 12 bits             | [-2047, 2048]        |
//! | `11110` | 32 bits             | i32 range            |
//! | `11111` | 64 bits             | anything             |
//!
//! **Values** — each `f64` is XOR-ed with its predecessor. A zero XOR costs one bit; otherwise
//! the meaningful bits are written either inside the previous leading/trailing-zero window
//! (`10`) or with a fresh 5-bit leading-zero count and 6-bit length (`11`).

use super::bits::{BitReader, BitWriter};
use super::varint::{put_ivarint, put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::types::Timestamp;

fn gcd(mut a: u64, mut b: u64) -> u64 {
    while b != 0 {
        let t = a % b;
        a = b;
        b = t;
    }
    a
}

/// Encode a non-decreasing sequence of timestamps.
///
/// Output layout: `uvarint(count) ivarint(first) uvarint(unit) <bit stream>`.
pub fn encode_timestamps(ts: &[Timestamp]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, ts.len() as u64);
    if ts.is_empty() {
        return out;
    }
    put_ivarint(&mut out, ts[0]);
    let unit = ts
        .windows(2)
        .map(|w| w[1].wrapping_sub(w[0]).unsigned_abs())
        .fold(0u64, gcd)
        .max(1);
    put_uvarint(&mut out, unit);
    let mut w = BitWriter::new();
    let mut prev_delta: i64 = 0;
    for pair in ts.windows(2) {
        let delta = pair[1].wrapping_sub(pair[0]) / unit as i64;
        let dod = delta.wrapping_sub(prev_delta);
        prev_delta = delta;
        match dod {
            0 => w.write_bit(false),
            -63..=64 => {
                w.write_bits(0b10, 2);
                w.write_bits((dod + 63) as u64, 7);
            }
            -255..=256 => {
                w.write_bits(0b110, 3);
                w.write_bits((dod + 255) as u64, 9);
            }
            -2047..=2048 => {
                w.write_bits(0b1110, 4);
                w.write_bits((dod + 2047) as u64, 12);
            }
            _ if i32::try_from(dod).is_ok() => {
                w.write_bits(0b11110, 5);
                w.write_bits(dod as i32 as u32 as u64, 32);
            }
            _ => {
                w.write_bits(0b11111, 5);
                w.write_bits(dod as u64, 64);
            }
        }
    }
    out.extend_from_slice(&w.into_bytes());
    out
}

/// Decode the output of [`encode_timestamps`].
pub fn decode_timestamps(buf: &[u8]) -> Result<Vec<Timestamp>> {
    let mut c = Cursor::new(buf);
    let count = c.uvarint()? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    // Each encoded timestamp costs at least one bit, which bounds a hostile count.
    if count > buf.len() * 8 + 1 {
        return Err(Error::Corrupt("timestamp count exceeds payload".into()));
    }
    let first = c.ivarint()?;
    let unit = c.uvarint()?;
    if unit == 0 || unit > i64::MAX as u64 {
        return Err(Error::Corrupt("invalid timestamp unit".into()));
    }
    let rest = c.bytes(c.remaining())?;
    let mut r = BitReader::new(rest);
    let mut out = Vec::with_capacity(count);
    out.push(first);
    let mut prev = first;
    let mut prev_delta: i64 = 0;
    for _ in 1..count {
        let dod: i64 = if !r.read_bit()? {
            0
        } else if !r.read_bit()? {
            r.read_bits(7)? as i64 - 63
        } else if !r.read_bit()? {
            r.read_bits(9)? as i64 - 255
        } else if !r.read_bit()? {
            r.read_bits(12)? as i64 - 2047
        } else if !r.read_bit()? {
            r.read_bits(32)? as u32 as i32 as i64
        } else {
            r.read_bits(64)? as i64
        };
        let delta = prev_delta.wrapping_add(dod);
        prev_delta = delta;
        prev = prev.wrapping_add(delta.wrapping_mul(unit as i64));
        out.push(prev);
    }
    Ok(out)
}

/// Encode a sequence of `f64` values with XOR compression.
///
/// Output layout: `uvarint(count) <bit stream>`.
pub fn encode_values(vals: &[f64]) -> Vec<u8> {
    let mut out = Vec::new();
    put_uvarint(&mut out, vals.len() as u64);
    if vals.is_empty() {
        return out;
    }
    let mut w = BitWriter::new();
    let mut prev = vals[0].to_bits();
    w.write_bits(prev, 64);
    // Window of the previous non-zero XOR: (leading zeros, meaningful length).
    let mut window: Option<(u32, u32)> = None;
    for v in &vals[1..] {
        let bits = v.to_bits();
        let xor = bits ^ prev;
        prev = bits;
        if xor == 0 {
            w.write_bit(false);
            continue;
        }
        w.write_bit(true);
        let leading = xor.leading_zeros().min(31);
        let trailing = xor.trailing_zeros();
        match window {
            Some((pl, plen)) if leading >= pl && trailing >= 64 - pl - plen => {
                w.write_bit(false);
                let shift = 64 - pl - plen;
                w.write_bits(xor >> shift, plen);
            }
            _ => {
                let len = 64 - leading - trailing;
                w.write_bit(true);
                w.write_bits(leading as u64, 5);
                // A 64-bit meaningful length is stored as 0.
                w.write_bits((len % 64) as u64, 6);
                w.write_bits(xor >> trailing, len);
                window = Some((leading, len));
            }
        }
    }
    out.extend_from_slice(&w.into_bytes());
    out
}

/// Decode the output of [`encode_values`].
pub fn decode_values(buf: &[u8]) -> Result<Vec<f64>> {
    let mut c = Cursor::new(buf);
    let count = c.uvarint()? as usize;
    if count == 0 {
        return Ok(Vec::new());
    }
    if count > buf.len() * 8 {
        return Err(Error::Corrupt("value count exceeds payload".into()));
    }
    let rest = c.bytes(c.remaining())?;
    let mut r = BitReader::new(rest);
    let mut prev = r.read_bits(64)?;
    let mut out = Vec::with_capacity(count);
    out.push(f64::from_bits(prev));
    let mut window: Option<(u32, u32)> = None;
    for _ in 1..count {
        if !r.read_bit()? {
            out.push(f64::from_bits(prev));
            continue;
        }
        let xor = if !r.read_bit()? {
            let (pl, plen) =
                window.ok_or_else(|| Error::Corrupt("xor window reused before set".into()))?;
            r.read_bits(plen)? << (64 - pl - plen)
        } else {
            let leading = r.read_bits(5)? as u32;
            let mut len = r.read_bits(6)? as u32;
            if len == 0 {
                len = 64;
            }
            if leading + len > 64 {
                return Err(Error::Corrupt("xor block exceeds 64 bits".into()));
            }
            window = Some((leading, len));
            r.read_bits(len)? << (64 - leading - len)
        };
        prev ^= xor;
        out.push(f64::from_bits(prev));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timestamps_roundtrip_regular_and_irregular() {
        let regular: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let enc = encode_timestamps(&regular);
        assert_eq!(decode_timestamps(&enc).unwrap(), regular);
        // 999 zero dods → ~1 bit each.
        assert!(
            enc.len() < 150,
            "regular series should be tiny, got {}",
            enc.len()
        );

        let irregular = vec![
            -5,
            0,
            3,
            3,
            1_000_000,
            1_000_007,
            9_000_000_000,
            i64::MAX / 2,
        ];
        assert_eq!(
            decode_timestamps(&encode_timestamps(&irregular)).unwrap(),
            irregular
        );
        assert_eq!(
            decode_timestamps(&encode_timestamps(&[])).unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            decode_timestamps(&encode_timestamps(&[7])).unwrap(),
            vec![7]
        );
    }

    #[test]
    fn timestamp_buckets_cover_boundaries() {
        let mut ts = vec![0i64, 1000];
        let mut delta = 1000i64;
        for dod in [
            64,
            -63,
            256,
            -255,
            2048,
            -2047,
            100_000,
            -100_000,
            1i64 << 40,
        ] {
            delta += dod;
            let last = *ts.last().unwrap();
            ts.push(last + delta);
        }
        assert_eq!(decode_timestamps(&encode_timestamps(&ts)).unwrap(), ts);
    }

    #[test]
    fn values_roundtrip_including_special_floats() {
        let vals = vec![
            12.5,
            12.5,
            12.75,
            -3.0,
            0.0,
            -0.0,
            f64::NAN,
            f64::INFINITY,
            1e-300,
            1e300,
            12.5,
        ];
        let dec = decode_values(&encode_values(&vals)).unwrap();
        assert_eq!(dec.len(), vals.len());
        for (a, b) in vals.iter().zip(&dec) {
            assert_eq!(a.to_bits(), b.to_bits());
        }
        assert!(decode_values(&encode_values(&[])).unwrap().is_empty());
    }

    #[test]
    fn constant_values_compress_to_bits() {
        let vals = vec![42.0; 1000];
        let enc = encode_values(&vals);
        assert!(enc.len() < 140, "got {}", enc.len());
    }

    #[test]
    fn truncated_payloads_error() {
        let vals: Vec<f64> = (0..50).map(|i| i as f64 * 1.37).collect();
        let enc = encode_values(&vals);
        assert!(decode_values(&enc[..enc.len() / 2]).is_err());
        let ts: Vec<i64> = (0..50).map(|i| i * i * 17).collect();
        let enc = encode_timestamps(&ts);
        assert!(decode_timestamps(&enc[..enc.len() / 2]).is_err());
        assert!(decode_timestamps(&[0xff, 0xff, 0x01]).is_err());
    }
}
