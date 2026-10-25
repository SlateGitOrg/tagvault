//! Length- and CRC-framed records: `[len: u32 LE][crc32(payload): u32 LE][payload]`.
//!
//! A log is valid up to the first frame whose header is short, whose length is implausible, whose
//! payload is short, or whose checksum does not match. Everything from that point on is treated
//! as a torn write from a crash and truncated by the owner.

use std::ops::Range;

/// Largest payload accepted (guards against interpreting garbage as a huge length).
pub const MAX_FRAME: usize = 64 << 20;

/// Header bytes per frame.
pub const HEADER: usize = 8;

/// Append a framed payload to `out`.
pub fn encode(payload: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    out.extend_from_slice(payload);
}

/// Outcome of scanning a framed log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scan {
    /// Payload byte ranges of the valid frames, in order.
    pub frames: Vec<Range<usize>>,
    /// Length of the valid prefix.
    pub valid_len: usize,
}

impl Scan {
    /// Bytes after the valid prefix (a torn tail if non-zero).
    pub fn torn_bytes(&self, total: usize) -> usize {
        total - self.valid_len
    }
}

/// Scan `buf` from the start and return the valid frames.
pub fn scan(buf: &[u8]) -> Scan {
    let mut frames = Vec::new();
    let mut pos = 0usize;
    while buf.len() - pos >= HEADER {
        let len = u32::from_le_bytes(buf[pos..pos + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(buf[pos + 4..pos + 8].try_into().expect("4 bytes"));
        let start = pos + HEADER;
        if len > MAX_FRAME || buf.len() - start < len {
            break;
        }
        let payload = &buf[start..start + len];
        if crc32fast::hash(payload) != crc {
            break;
        }
        frames.push(start..start + len);
        pos = start + len;
    }
    Scan { frames, valid_len: pos }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scan_stops_at_first_damaged_frame() {
        let mut log = Vec::new();
        for p in [&b"alpha"[..], b"", b"gamma-gamma"] {
            encode(p, &mut log);
        }
        let full = scan(&log);
        assert_eq!(full.frames.len(), 3);
        assert_eq!(full.valid_len, log.len());
        assert_eq!(&log[full.frames[2].clone()], b"gamma-gamma");

        // Torn tail: every strict prefix loses exactly the incomplete frame.
        for cut in 0..log.len() {
            let s = scan(&log[..cut]);
            assert!(s.valid_len <= cut);
            assert!(s.frames.len() < 3);
        }
        // Bit flip in the second frame's payload of a three-frame log with non-empty middle.
        let mut log2 = Vec::new();
        for p in [&b"one"[..], b"two", b"three"] {
            encode(p, &mut log2);
        }
        log2[HEADER + 3 + HEADER] ^= 0x01;
        let s = scan(&log2);
        assert_eq!(s.frames.len(), 1);
        assert_eq!(s.torn_bytes(log2.len()), log2.len() - (HEADER + 3));
        // Implausible length.
        let mut bad = vec![0xff, 0xff, 0xff, 0x7f, 0, 0, 0, 0];
        bad.extend_from_slice(&[0; 16]);
        assert_eq!(scan(&bad).valid_len, 0);
    }
}
