//! Immutable segment files of columnar, Gorilla-encoded chunks.
//!
//! ```text
//! "TVSEG001"                                  8-byte magic
//! chunk*                                      one per tag present in the flush
//!   uvarint(len) timestamps (delta-of-delta)  see encoding::gorilla
//!   uvarint(len) values     (XOR)
//!   uvarint(runs) (u8 quality, uvarint run)*  run-length encoded quality column
//! index                                       56 bytes per chunk (see ChunkMeta)
//! u32 index_len, u32 crc32(index)             footer
//! "TVSEGEND"                                  8-byte end magic
//! ```
//!
//! The footer index carries each chunk's offset, length, CRC, point count, time bounds and
//! good-value min/max, so queries can locate and prune chunks without decoding them. A segment
//! is written to a temporary name, fsync-ed and renamed, so it is either complete or absent.

use super::sync_dir;
use crate::encoding::gorilla;
use crate::encoding::varint::{put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::types::{Point, Quality, TagId, Timestamp};
use std::fs::{self, File};
use std::io::Write;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"TVSEG001";
const END_MAGIC: &[u8; 8] = b"TVSEGEND";
const INDEX_ENTRY: usize = 56;

/// Location and summary of one chunk inside a segment.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkMeta {
    /// Owning tag.
    pub tag: TagId,
    /// Byte offset in the file.
    pub offset: u64,
    /// Encoded length.
    pub len: u32,
    /// CRC32 of the encoded chunk.
    pub crc: u32,
    /// Number of points.
    pub count: u32,
    /// First timestamp.
    pub t_min: Timestamp,
    /// Last timestamp.
    pub t_max: Timestamp,
    /// Minimum good, finite value (NaN if none).
    pub v_min: f64,
    /// Maximum good, finite value (NaN if none).
    pub v_max: f64,
}

/// Encode one chunk's three columns.
pub fn encode_chunk(points: &[Point]) -> Vec<u8> {
    let ts: Vec<Timestamp> = points.iter().map(|p| p.ts).collect();
    let vals: Vec<f64> = points.iter().map(|p| p.value).collect();
    let ts_col = gorilla::encode_timestamps(&ts);
    let val_col = gorilla::encode_values(&vals);
    let mut runs: Vec<(Quality, u64)> = Vec::new();
    for p in points {
        match runs.last_mut() {
            Some((q, n)) if *q == p.quality => *n += 1,
            _ => runs.push((p.quality, 1)),
        }
    }
    let mut out = Vec::with_capacity(ts_col.len() + val_col.len() + 16);
    put_uvarint(&mut out, ts_col.len() as u64);
    out.extend_from_slice(&ts_col);
    put_uvarint(&mut out, val_col.len() as u64);
    out.extend_from_slice(&val_col);
    put_uvarint(&mut out, runs.len() as u64);
    for (q, n) in runs {
        out.push(q.code());
        put_uvarint(&mut out, n);
    }
    out
}

/// Decode a chunk produced by [`encode_chunk`].
pub fn decode_chunk(buf: &[u8]) -> Result<Vec<Point>> {
    let mut c = Cursor::new(buf);
    let ts_len = c.uvarint()? as usize;
    let ts = gorilla::decode_timestamps(c.bytes(ts_len)?)?;
    let val_len = c.uvarint()? as usize;
    let vals = gorilla::decode_values(c.bytes(val_len)?)?;
    if ts.len() != vals.len() {
        return Err(Error::Corrupt("chunk column lengths differ".into()));
    }
    let nruns = c.uvarint()? as usize;
    let mut qualities = Vec::with_capacity(ts.len());
    for _ in 0..nruns {
        let q = Quality::from_code(c.u8()?).ok_or_else(|| Error::Corrupt("bad quality code".into()))?;
        let n = c.uvarint()? as usize;
        if qualities.len() + n > ts.len() {
            return Err(Error::Corrupt("quality runs exceed chunk length".into()));
        }
        qualities.extend(std::iter::repeat_n(q, n));
    }
    if qualities.len() != ts.len() {
        return Err(Error::Corrupt("quality runs shorter than chunk".into()));
    }
    Ok(ts.into_iter().zip(vals).zip(qualities).map(|((ts, value), quality)| Point { ts, value, quality }).collect())
}

fn bounds(points: &[Point]) -> (f64, f64) {
    points
        .iter()
        .filter(|p| p.quality.is_good() && p.value.is_finite())
        .fold((f64::NAN, f64::NAN), |(lo, hi), p| (lo.min(p.value), hi.max(p.value)))
}

/// File name for segment `id`.
pub fn file_name(id: u64) -> String {
    format!("seg-{id:010}.tvs")
}

/// Parse a segment id out of a file name produced by [`file_name`].
pub fn parse_file_name(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?.strip_suffix(".tvs")?.parse().ok()
}

/// An open, immutable segment.
#[derive(Debug)]
pub struct Segment {
    id: u64,
    path: PathBuf,
    file: File,
    size: u64,
    chunks: Vec<ChunkMeta>,
}

impl Segment {
    /// Write a new segment atomically. `chunks` must each be non-empty and time-ordered.
    pub fn write(dir: &Path, id: u64, chunks: &[(TagId, &[Point])]) -> Result<Segment> {
        let mut body = Vec::new();
        body.extend_from_slice(MAGIC);
        let mut metas = Vec::with_capacity(chunks.len());
        for (tag, pts) in chunks {
            let (Some(first), Some(last)) = (pts.first(), pts.last()) else {
                return Err(Error::InvalidInput("empty chunk".into()));
            };
            let enc = encode_chunk(pts);
            let (v_min, v_max) = bounds(pts);
            metas.push(ChunkMeta {
                tag: *tag,
                offset: body.len() as u64,
                len: enc.len() as u32,
                crc: crc32fast::hash(&enc),
                count: pts.len() as u32,
                t_min: first.ts,
                t_max: last.ts,
                v_min,
                v_max,
            });
            body.extend_from_slice(&enc);
        }
        let mut index = Vec::with_capacity(metas.len() * INDEX_ENTRY);
        for m in &metas {
            index.extend_from_slice(&m.tag.to_le_bytes());
            index.extend_from_slice(&m.offset.to_le_bytes());
            index.extend_from_slice(&m.len.to_le_bytes());
            index.extend_from_slice(&m.crc.to_le_bytes());
            index.extend_from_slice(&m.count.to_le_bytes());
            index.extend_from_slice(&m.t_min.to_le_bytes());
            index.extend_from_slice(&m.t_max.to_le_bytes());
            index.extend_from_slice(&m.v_min.to_le_bytes());
            index.extend_from_slice(&m.v_max.to_le_bytes());
        }
        body.extend_from_slice(&index);
        body.extend_from_slice(&(index.len() as u32).to_le_bytes());
        body.extend_from_slice(&crc32fast::hash(&index).to_le_bytes());
        body.extend_from_slice(END_MAGIC);

        let path = dir.join(file_name(id));
        let tmp = path.with_extension("tvs.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&body)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, &path)?;
        sync_dir(dir)?;
        let file = File::open(&path)?;
        Ok(Segment { id, path, file, size: body.len() as u64, chunks: metas })
    }

    /// Open and validate a segment's footer and index.
    pub fn open(path: &Path) -> Result<Segment> {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
        let id = parse_file_name(name).ok_or_else(|| Error::Corrupt(format!("bad segment name {name}")))?;
        let file = File::open(path)?;
        let size = file.metadata()?.len();
        let corrupt = |why: &str| Error::Corrupt(format!("segment {}: {why}", path.display()));
        if size < (MAGIC.len() + 16) as u64 {
            return Err(corrupt("too short"));
        }
        let mut head = [0u8; 8];
        file.read_exact_at(&mut head, 0)?;
        let mut tail = [0u8; 16];
        file.read_exact_at(&mut tail, size - 16)?;
        if &head != MAGIC || &tail[8..] != END_MAGIC {
            return Err(corrupt("bad magic"));
        }
        let index_len = u32::from_le_bytes(tail[0..4].try_into().expect("4")) as u64;
        let index_crc = u32::from_le_bytes(tail[4..8].try_into().expect("4"));
        if index_len % INDEX_ENTRY as u64 != 0 || index_len + 24 > size {
            return Err(corrupt("bad index length"));
        }
        let mut index = vec![0u8; index_len as usize];
        file.read_exact_at(&mut index, size - 16 - index_len)?;
        if crc32fast::hash(&index) != index_crc {
            return Err(corrupt("index checksum mismatch"));
        }
        let data_end = size - 16 - index_len;
        let mut c = Cursor::new(&index);
        let mut chunks = Vec::with_capacity(index.len() / INDEX_ENTRY);
        while c.remaining() > 0 {
            let m = ChunkMeta {
                tag: c.u32()?,
                offset: c.u64()?,
                len: c.u32()?,
                crc: c.u32()?,
                count: c.u32()?,
                t_min: c.i64()?,
                t_max: c.i64()?,
                v_min: c.f64()?,
                v_max: c.f64()?,
            };
            if m.offset < MAGIC.len() as u64 || m.offset + m.len as u64 > data_end {
                return Err(corrupt("chunk outside data region"));
            }
            chunks.push(m);
        }
        Ok(Segment { id, path: path.to_path_buf(), file, size, chunks })
    }

    /// Read and decode one chunk, verifying its checksum.
    pub fn read_chunk(&self, meta: &ChunkMeta) -> Result<Vec<Point>> {
        let mut buf = vec![0u8; meta.len as usize];
        self.file.read_exact_at(&mut buf, meta.offset)?;
        if crc32fast::hash(&buf) != meta.crc {
            return Err(Error::Corrupt(format!("segment {}: chunk checksum mismatch", self.path.display())));
        }
        let pts = decode_chunk(&buf)?;
        if pts.len() != meta.count as usize {
            return Err(Error::Corrupt("chunk count mismatch".into()));
        }
        Ok(pts)
    }

    /// Segment id.
    pub fn id(&self) -> u64 {
        self.id
    }

    /// File size in bytes.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Chunk index.
    pub fn chunks(&self) -> &[ChunkMeta] {
        &self.chunks
    }

    /// File path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pts(n: i64, base: f64) -> Vec<Point> {
        (0..n)
            .map(|i| {
                let q = if i == 3 { Quality::Bad } else { Quality::Good };
                Point::with_quality(1_000 + i * 1000, base + i as f64 * 0.5, q)
            })
            .collect()
    }

    #[test]
    fn write_open_read_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let a = pts(100, 10.0);
        let b = pts(5, -3.0);
        let seg = Segment::write(dir.path(), 7, &[(1, &a), (9, &b)]).unwrap();
        assert_eq!(seg.id(), 7);
        let reopened = Segment::open(seg.path()).unwrap();
        assert_eq!(reopened.chunks(), seg.chunks());
        assert_eq!(reopened.size(), fs::metadata(seg.path()).unwrap().len());
        let m = reopened.chunks()[0];
        assert_eq!((m.tag, m.count, m.t_min, m.t_max), (1, 100, 1_000, 100_000));
        assert_eq!((m.v_min, m.v_max), (10.0, 59.5));
        assert_eq!(reopened.read_chunk(&m).unwrap(), a);
        assert_eq!(reopened.read_chunk(&reopened.chunks()[1]).unwrap(), b);
    }

    #[test]
    fn detects_corruption() {
        let dir = tempfile::tempdir().unwrap();
        let a = pts(50, 1.0);
        let seg = Segment::write(dir.path(), 1, &[(0, &a)]).unwrap();
        let path = seg.path().to_path_buf();
        let meta = seg.chunks()[0];
        drop(seg);
        let mut bytes = fs::read(&path).unwrap();
        bytes[meta.offset as usize + 3] ^= 0xff;
        fs::write(&path, &bytes).unwrap();
        let s = Segment::open(&path).unwrap();
        assert!(s.read_chunk(&meta).is_err(), "chunk crc must catch payload damage");

        let n = bytes.len();
        bytes[n - 20] ^= 0xff; // inside the index
        fs::write(&path, &bytes).unwrap();
        assert!(Segment::open(&path).is_err());
        fs::write(&path, &bytes[..n - 3]).unwrap();
        assert!(Segment::open(&path).is_err());
        fs::write(&path, b"short").unwrap();
        assert!(Segment::open(&path).is_err());
        assert!(Segment::write(dir.path(), 2, &[(0, &[])]).is_err());
    }

    #[test]
    fn chunk_decoder_rejects_inconsistent_columns() {
        let enc = encode_chunk(&pts(10, 0.0));
        assert!(decode_chunk(&enc[..enc.len() - 1]).is_err());
        let mut bad = enc.clone();
        let last = bad.len() - 1;
        bad[last] = 50; // last quality run longer than the chunk
        assert!(decode_chunk(&bad).is_err());
        assert_eq!(parse_file_name(&file_name(42)), Some(42));
        assert_eq!(parse_file_name("seg-x.tvs"), None);
    }
}
