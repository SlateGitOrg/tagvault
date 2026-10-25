//! Binary wire format for a batch of samples, used by the spool and by `POST /api/v1/ingest`.
//!
//! ```text
//! "TVB1"
//! uvarint ntags  { str name, u8 kind }*
//! uvarint npts   { uvarint tag_index, ivarint ts, f64 value (LE), u8 quality }*
//! ```

use crate::encoding::varint::{put_ivarint, put_str, put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::types::{Point, Quality, Sample, ValueKind};
use std::collections::HashMap;

const MAGIC: &[u8; 4] = b"TVB1";

/// Encode samples (tag names are interned once per batch).
pub fn encode(samples: &[Sample]) -> Vec<u8> {
    let mut names: Vec<(&str, ValueKind)> = Vec::new();
    let mut index: HashMap<&str, usize> = HashMap::new();
    for s in samples {
        index.entry(&s.tag).or_insert_with(|| {
            names.push((&s.tag, s.kind));
            names.len() - 1
        });
    }
    let mut out = Vec::with_capacity(16 + samples.len() * 20);
    out.extend_from_slice(MAGIC);
    put_uvarint(&mut out, names.len() as u64);
    for (n, k) in &names {
        put_str(&mut out, n);
        out.push(k.code());
    }
    put_uvarint(&mut out, samples.len() as u64);
    for s in samples {
        put_uvarint(&mut out, index[s.tag.as_str()] as u64);
        put_ivarint(&mut out, s.point.ts);
        out.extend_from_slice(&s.point.value.to_le_bytes());
        out.push(s.point.quality.code());
    }
    out
}

/// Decode a batch; every length is bounds-checked against the input.
pub fn decode(buf: &[u8]) -> Result<Vec<Sample>> {
    let mut c = Cursor::new(buf);
    if c.bytes(4)? != MAGIC {
        return Err(Error::Decode("not a tagvault batch".into()));
    }
    let ntags = c.uvarint()? as usize;
    let mut names = Vec::with_capacity(ntags.min(c.remaining()));
    for _ in 0..ntags {
        let name = c.string()?;
        let kind = ValueKind::from_code(c.u8()?).ok_or_else(|| Error::Decode("bad value kind".into()))?;
        names.push((name, kind));
    }
    let npts = c.uvarint()? as usize;
    let mut out = Vec::with_capacity(npts.min(c.remaining() / 11));
    for _ in 0..npts {
        let idx = c.uvarint()? as usize;
        let (name, kind) = names.get(idx).ok_or_else(|| Error::Decode("tag index out of range".into()))?;
        let ts = c.ivarint()?;
        let value = c.f64()?;
        let quality = Quality::from_code(c.u8()?).ok_or_else(|| Error::Decode("bad quality".into()))?;
        out.push(Sample { tag: name.clone(), kind: *kind, point: Point { ts, value, quality } });
    }
    if c.remaining() != 0 {
        return Err(Error::Decode("trailing bytes after batch".into()));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_and_rejects_garbage() {
        let samples = vec![
            Sample { tag: "a/b".into(), kind: ValueKind::Analog, point: Point::good(-5, 1.25) },
            Sample { tag: "c".into(), kind: ValueKind::Discrete, point: Point::with_quality(7, 0.0, Quality::Stale) },
            Sample { tag: "a/b".into(), kind: ValueKind::Analog, point: Point::good(9, 2.5) },
        ];
        let enc = encode(&samples);
        assert_eq!(decode(&enc).unwrap(), samples);
        assert!(decode(&enc[..enc.len() - 1]).is_err());
        assert!(decode(b"XXXX").is_err());
        let mut extra = enc.clone();
        extra.push(0);
        assert!(decode(&extra).is_err());
        assert!(decode(&encode(&[])).unwrap().is_empty());
        // Header claiming a huge count must not allocate it.
        let mut huge = MAGIC.to_vec();
        huge.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x0f]);
        assert!(decode(&huge).is_err());
    }
}
