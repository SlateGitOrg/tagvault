//! Write-ahead log.
//!
//! Every accepted raw point is logged (and, with [`SyncMode::Always`], fsync-ed) *before* it is
//! applied to the in-memory compressor state. The log always begins with a [`Checkpoint`] that
//! captures the full compressor state at the moment the previous memtable was flushed to a
//! segment, so recovery = load checkpoint + replay the remaining records through the same
//! deterministic code path. A recovered archive is therefore identical to one that never
//! crashed (see `tests/crash_recovery.rs`).
//!
//! Rotation is atomic: the new log (containing only the new checkpoint) is written to a
//! temporary file, fsync-ed, renamed over the old log, and the directory is fsync-ed.

use super::frame;
use super::sync_dir;
use crate::compress::sdt::Sdt;
use crate::encoding::varint::{put_str, put_uvarint, Cursor};
use crate::error::{Error, Result};
use crate::types::{Point, Quality, TagId, Timestamp, ValueKind};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// When the WAL is fsync-ed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SyncMode {
    /// fsync on every commit: an acknowledged write survives power loss.
    #[default]
    Always,
    /// Leave flushing to the OS: survives a process crash, not a power loss. For benchmarks.
    Os,
}

/// Per-tag state captured in a checkpoint.
#[derive(Debug, Clone, PartialEq)]
pub struct TagCheckpoint {
    /// Tag id.
    pub id: TagId,
    /// Tag name.
    pub name: String,
    /// Compressor state.
    pub sdt: Sdt,
    /// Last accepted timestamp.
    pub last_ts: Option<Timestamp>,
}

/// Snapshot of the archive's mutable state at a flush boundary.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Checkpoint {
    /// Highest segment id whose contents are covered by this checkpoint (0 = none).
    pub last_segment: u64,
    /// Every tag known at the checkpoint.
    pub tags: Vec<TagCheckpoint>,
}

/// One logical WAL record.
#[derive(Debug, Clone, PartialEq)]
pub enum WalRecord {
    /// Always the first record of a log.
    Checkpoint(Checkpoint),
    /// A new tag.
    TagDef {
        /// Assigned id.
        id: TagId,
        /// Name.
        name: String,
        /// Interpolation kind.
        kind: ValueKind,
    },
    /// A batch of accepted raw points.
    Points(Vec<(TagId, Point)>),
}

const REC_CHECKPOINT: u8 = 1;
const REC_TAGDEF: u8 = 2;
const REC_POINTS: u8 = 3;

impl WalRecord {
    /// Serialize the record body (without framing).
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            WalRecord::Checkpoint(cp) => {
                out.push(REC_CHECKPOINT);
                put_uvarint(out, cp.last_segment);
                put_uvarint(out, cp.tags.len() as u64);
                for t in &cp.tags {
                    put_uvarint(out, t.id as u64);
                    put_str(out, &t.name);
                    t.sdt.encode(out);
                    match t.last_ts {
                        Some(ts) => {
                            out.push(1);
                            out.extend_from_slice(&ts.to_le_bytes());
                        }
                        None => out.push(0),
                    }
                }
            }
            WalRecord::TagDef { id, name, kind } => {
                out.push(REC_TAGDEF);
                put_uvarint(out, *id as u64);
                put_str(out, name);
                out.push(kind.code());
            }
            WalRecord::Points(pts) => {
                out.push(REC_POINTS);
                put_uvarint(out, pts.len() as u64);
                for (id, p) in pts {
                    put_uvarint(out, *id as u64);
                    out.extend_from_slice(&p.ts.to_le_bytes());
                    out.extend_from_slice(&p.value.to_le_bytes());
                    out.push(p.quality.code());
                }
            }
        }
    }

    /// Parse a record body.
    pub fn decode(buf: &[u8]) -> Result<WalRecord> {
        let mut c = Cursor::new(buf);
        let rec = match c.u8()? {
            REC_CHECKPOINT => {
                let last_segment = c.uvarint()?;
                let n = c.uvarint()? as usize;
                let mut tags = Vec::with_capacity(n.min(c.remaining()));
                for _ in 0..n {
                    let id = tag_id(c.uvarint()?)?;
                    let name = c.string()?;
                    let sdt = Sdt::decode(&mut c)?;
                    let last_ts = if c.u8()? == 1 { Some(c.i64()?) } else { None };
                    tags.push(TagCheckpoint {
                        id,
                        name,
                        sdt,
                        last_ts,
                    });
                }
                WalRecord::Checkpoint(Checkpoint { last_segment, tags })
            }
            REC_TAGDEF => {
                let id = tag_id(c.uvarint()?)?;
                let name = c.string()?;
                let kind = ValueKind::from_code(c.u8()?)
                    .ok_or_else(|| Error::Corrupt("bad value kind".into()))?;
                WalRecord::TagDef { id, name, kind }
            }
            REC_POINTS => {
                let n = c.uvarint()? as usize;
                let mut pts = Vec::with_capacity(n.min(c.remaining()));
                for _ in 0..n {
                    let id = tag_id(c.uvarint()?)?;
                    let ts = c.i64()?;
                    let value = c.f64()?;
                    let quality = Quality::from_code(c.u8()?)
                        .ok_or_else(|| Error::Corrupt("bad quality".into()))?;
                    pts.push((id, Point { ts, value, quality }));
                }
                WalRecord::Points(pts)
            }
            t => return Err(Error::Corrupt(format!("unknown WAL record type {t}"))),
        };
        if c.remaining() != 0 {
            return Err(Error::Corrupt("trailing bytes in WAL record".into()));
        }
        Ok(rec)
    }
}

fn tag_id(v: u64) -> Result<TagId> {
    TagId::try_from(v).map_err(|_| Error::Corrupt("tag id out of range".into()))
}

/// What recovery found in the log.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WalRecovery {
    /// Valid records read (including the checkpoint).
    pub records: usize,
    /// Bytes of torn tail that were truncated.
    pub truncated_bytes: usize,
}

/// An open write-ahead log.
#[derive(Debug)]
pub struct Wal {
    path: PathBuf,
    file: File,
    pending: Vec<u8>,
    len: u64,
    sync: SyncMode,
}

impl Wal {
    /// Atomically replace (or create) the log at `path` with one containing only `checkpoint`.
    pub fn create(path: &Path, checkpoint: &Checkpoint, sync: SyncMode) -> Result<Wal> {
        let mut body = Vec::new();
        WalRecord::Checkpoint(checkpoint.clone()).encode(&mut body);
        let mut framed = Vec::with_capacity(body.len() + frame::HEADER);
        frame::encode(&body, &mut framed);
        let tmp = path.with_extension("tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&framed)?;
            f.sync_all()?;
        }
        fs::rename(&tmp, path)?;
        if let Some(dir) = path.parent() {
            sync_dir(dir)?;
        }
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Wal {
            path: path.to_path_buf(),
            file,
            pending: Vec::new(),
            len: framed.len() as u64,
            sync,
        })
    }

    /// Open an existing log, truncating any torn tail, and return its records.
    pub fn open(path: &Path, sync: SyncMode) -> Result<(Wal, Vec<WalRecord>, WalRecovery)> {
        let buf = fs::read(path)?;
        let scan = frame::scan(&buf);
        let mut records = Vec::with_capacity(scan.frames.len());
        for r in &scan.frames {
            records.push(WalRecord::decode(&buf[r.clone()])?);
        }
        if !matches!(records.first(), Some(WalRecord::Checkpoint(_))) {
            return Err(Error::Corrupt(format!(
                "{} does not start with a checkpoint",
                path.display()
            )));
        }
        let truncated = scan.torn_bytes(buf.len());
        let file = OpenOptions::new().write(true).open(path)?;
        if truncated > 0 {
            log::warn!(
                "WAL {}: truncating {truncated} byte torn tail",
                path.display()
            );
            file.set_len(scan.valid_len as u64)?;
            file.sync_all()?;
        }
        drop(file);
        let file = OpenOptions::new().append(true).open(path)?;
        let rec = WalRecovery {
            records: records.len(),
            truncated_bytes: truncated,
        };
        Ok((
            Wal {
                path: path.to_path_buf(),
                file,
                pending: Vec::new(),
                len: scan.valid_len as u64,
                sync,
            },
            records,
            rec,
        ))
    }

    /// Buffer a record; it is not durable until [`Wal::commit`].
    pub fn append(&mut self, rec: &WalRecord) {
        let mut body = Vec::new();
        rec.encode(&mut body);
        frame::encode(&body, &mut self.pending);
    }

    /// Write buffered records and (per [`SyncMode`]) fsync.
    pub fn commit(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }
        self.file.write_all(&self.pending)?;
        if self.sync == SyncMode::Always {
            self.file.sync_data()?;
        }
        self.len += self.pending.len() as u64;
        self.pending.clear();
        Ok(())
    }

    /// Committed size in bytes.
    pub fn len(&self) -> u64 {
        self.len
    }

    /// True when the log holds no committed bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Path of the log file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compress::sdt::SdtConfig;

    fn sample_records() -> Vec<WalRecord> {
        let mut sdt = Sdt::new(SdtConfig::default(), ValueKind::Analog);
        let mut sink = Vec::new();
        sdt.offer(Point::good(1, 2.0), &mut sink);
        vec![
            WalRecord::Checkpoint(Checkpoint {
                last_segment: 4,
                tags: vec![TagCheckpoint {
                    id: 0,
                    name: "a/b".into(),
                    sdt,
                    last_ts: Some(1),
                }],
            }),
            WalRecord::TagDef {
                id: 1,
                name: "c".into(),
                kind: ValueKind::Discrete,
            },
            WalRecord::Points(vec![
                (0, Point::good(5, 1.5)),
                (1, Point::with_quality(6, f64::NAN, Quality::Bad)),
            ]),
        ]
    }

    #[test]
    fn records_roundtrip() {
        for r in sample_records() {
            let mut b = Vec::new();
            r.encode(&mut b);
            let d = WalRecord::decode(&b).unwrap();
            match (&r, &d) {
                (WalRecord::Points(a), WalRecord::Points(b)) => {
                    assert_eq!(a.len(), b.len());
                    assert_eq!(a[1].1.value.to_bits(), b[1].1.value.to_bits());
                }
                _ => assert_eq!(r, d),
            }
            b.push(0);
            assert!(WalRecord::decode(&b).is_err());
        }
        assert!(WalRecord::decode(&[9]).is_err());
    }

    #[test]
    fn create_append_reopen_and_truncate_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let recs = sample_records();
        let WalRecord::Checkpoint(cp) = &recs[0] else {
            unreachable!()
        };
        let mut wal = Wal::create(&path, cp, SyncMode::Always).unwrap();
        wal.append(&recs[1]);
        wal.append(&recs[2]);
        wal.commit().unwrap();
        wal.commit().unwrap();
        let full = wal.len();
        assert_eq!(fs::metadata(&path).unwrap().len(), full);
        assert_eq!(wal.path(), path.as_path());
        assert!(!wal.is_empty());
        drop(wal);

        // Simulate a torn write.
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        f.write_all(&[42, 0, 0, 0, 1, 2]).unwrap();
        drop(f);
        let (wal, got, rec) = Wal::open(&path, SyncMode::Os).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(
            rec,
            WalRecovery {
                records: 3,
                truncated_bytes: 6
            }
        );
        assert_eq!(wal.len(), full);
        assert_eq!(fs::metadata(&path).unwrap().len(), full);
    }

    #[test]
    fn log_without_checkpoint_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.log");
        let mut body = Vec::new();
        WalRecord::TagDef {
            id: 0,
            name: "x".into(),
            kind: ValueKind::Analog,
        }
        .encode(&mut body);
        let mut framed = Vec::new();
        frame::encode(&body, &mut framed);
        fs::write(&path, framed).unwrap();
        assert!(matches!(
            Wal::open(&path, SyncMode::Os),
            Err(Error::Corrupt(_))
        ));
    }
}
