//! Disk-backed FIFO spool for store-and-forward.
//!
//! Records are framed (`storage::frame`) into append-only segment files
//! `spool-<id>.q`. A separate `cursor` file records how far the consumer has acknowledged; it is
//! replaced atomically (write temp → fsync → rename). Fully acknowledged segments are deleted.
//!
//! Delivery is **at-least-once**: a crash between delivering a record and persisting the
//! acknowledgement redelivers it. The archive rejects points whose timestamp is not newer than
//! the tag's last one, so redelivery is harmless.
//!
//! When the spool reaches `max_bytes`, [`Overflow`] decides: drop the oldest segment (keep the
//! freshest data) or reject new records (keep the oldest). Both are counted.

use crate::error::{Error, Result};
use crate::storage::frame;
use crate::storage::SyncMode;
use serde::Serialize;
use std::collections::VecDeque;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Behaviour when the spool is full.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Overflow {
    /// Delete the oldest segment to make room.
    #[default]
    DropOldest,
    /// Refuse the new record.
    RejectNew,
}

/// Spool tuning.
#[derive(Debug, Clone, Copy)]
pub struct SpoolOptions {
    /// Rotate to a new segment file beyond this size.
    pub segment_bytes: u64,
    /// Capacity across all segments.
    pub max_bytes: u64,
    /// Full-spool policy.
    pub overflow: Overflow,
    /// fsync behaviour for pushes and cursor updates.
    pub sync: SyncMode,
}

impl Default for SpoolOptions {
    fn default() -> Self {
        SpoolOptions {
            segment_bytes: 1 << 20,
            max_bytes: 256 << 20,
            overflow: Overflow::DropOldest,
            sync: SyncMode::Always,
        }
    }
}

/// Position just after a record: acknowledging it acknowledges everything before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Position {
    /// Segment id.
    pub segment: u64,
    /// Byte offset just past the record.
    pub offset: u64,
}

/// A spooled record.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    /// Where the record ends.
    pub pos: Position,
    /// Record payload.
    pub payload: Vec<u8>,
}

/// Spool counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct SpoolStats {
    /// Records waiting for acknowledgement.
    pub pending_records: u64,
    /// Bytes on disk (including acknowledged bytes in the head segment).
    pub disk_bytes: u64,
    /// Records ever pushed (since open).
    pub pushed: u64,
    /// Records acknowledged (since open).
    pub acked: u64,
    /// Records lost to [`Overflow::DropOldest`] (since open).
    pub dropped: u64,
    /// Records refused by [`Overflow::RejectNew`] (since open).
    pub rejected: u64,
    /// Torn bytes truncated during open.
    pub truncated_bytes: u64,
}

#[derive(Debug, Clone, Copy)]
struct SegInfo {
    id: u64,
    len: u64,
    records: u64,
}

/// The spool.
#[derive(Debug)]
pub struct Spool {
    dir: PathBuf,
    opts: SpoolOptions,
    segments: VecDeque<SegInfo>,
    tail: File,
    cursor: Position,
    stats: SpoolStats,
    _lock: File,
}

fn seg_name(id: u64) -> String {
    format!("spool-{id:010}.q")
}

fn parse_seg_name(n: &str) -> Option<u64> {
    n.strip_prefix("spool-")?.strip_suffix(".q")?.parse().ok()
}

impl Spool {
    /// Open or create a spool directory.
    pub fn open(dir: &Path, opts: SpoolOptions) -> Result<Spool> {
        if opts.segment_bytes == 0 || opts.max_bytes < opts.segment_bytes {
            return Err(Error::InvalidInput("spool max_bytes must be >= segment_bytes > 0".into()));
        }
        fs::create_dir_all(dir)?;
        let lock = crate::storage::lock_dir(dir)?;
        let mut ids: Vec<u64> = Vec::new();
        for e in fs::read_dir(dir)? {
            let name = e?.file_name().to_string_lossy().into_owned();
            if let Some(id) = parse_seg_name(&name) {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        let mut stats = SpoolStats::default();
        let mut segments = VecDeque::new();
        for id in ids {
            let path = dir.join(seg_name(id));
            let buf = fs::read(&path)?;
            let scan = frame::scan(&buf);
            let torn = scan.torn_bytes(buf.len());
            if torn > 0 {
                log::warn!("spool {}: truncating {torn} torn bytes", path.display());
                let f = OpenOptions::new().write(true).open(&path)?;
                f.set_len(scan.valid_len as u64)?;
                f.sync_all()?;
                stats.truncated_bytes += torn as u64;
            }
            segments.push_back(SegInfo { id, len: scan.valid_len as u64, records: scan.frames.len() as u64 });
        }
        if segments.is_empty() {
            File::create(dir.join(seg_name(1)))?;
            segments.push_back(SegInfo { id: 1, len: 0, records: 0 });
        }
        let first = segments[0];
        let mut cursor = read_cursor(dir)?.unwrap_or(Position { segment: first.id, offset: 0 });
        if cursor.segment < first.id {
            cursor = Position { segment: first.id, offset: 0 };
        }
        let tail_id = segments.back().expect("non-empty").id;
        let tail = OpenOptions::new().append(true).open(dir.join(seg_name(tail_id)))?;
        let mut spool = Spool { dir: dir.to_path_buf(), opts, segments, tail, cursor, stats, _lock: lock };
        spool.stats.pending_records = spool.count_pending()?;
        Ok(spool)
    }

    fn count_pending(&self) -> Result<u64> {
        let mut n = 0;
        for s in &self.segments {
            if s.id > self.cursor.segment {
                n += s.records;
            } else if s.id == self.cursor.segment {
                let buf = fs::read(self.dir.join(seg_name(s.id)))?;
                let start = (self.cursor.offset as usize).min(buf.len());
                n += frame::scan(&buf[start..]).frames.len() as u64;
            }
        }
        Ok(n)
    }

    fn disk_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.len).sum()
    }

    /// Append a record. Returns `false` if it was refused because the spool is full.
    pub fn push(&mut self, payload: &[u8]) -> Result<bool> {
        let mut framed = Vec::with_capacity(payload.len() + frame::HEADER);
        frame::encode(payload, &mut framed);
        let need = framed.len() as u64;
        if need > self.opts.max_bytes {
            return Err(Error::InvalidInput("record larger than spool capacity".into()));
        }
        while self.disk_bytes() + need > self.opts.max_bytes {
            match self.opts.overflow {
                Overflow::RejectNew => {
                    self.stats.rejected += 1;
                    return Ok(false);
                }
                Overflow::DropOldest => {
                    if self.segments.len() == 1 {
                        self.rotate()?;
                    }
                    self.drop_oldest()?;
                }
            }
        }
        let tail = *self.segments.back().expect("non-empty");
        if tail.len > 0 && tail.len + need > self.opts.segment_bytes {
            self.rotate()?;
        }
        self.tail.write_all(&framed)?;
        if self.opts.sync == SyncMode::Always {
            self.tail.sync_data()?;
        }
        let tail = self.segments.back_mut().expect("non-empty");
        tail.len += need;
        tail.records += 1;
        self.stats.pushed += 1;
        self.stats.pending_records += 1;
        Ok(true)
    }

    fn rotate(&mut self) -> Result<()> {
        let id = self.segments.back().expect("non-empty").id + 1;
        let path = self.dir.join(seg_name(id));
        self.tail = OpenOptions::new().create(true).append(true).open(&path)?;
        if self.opts.sync == SyncMode::Always {
            crate::storage::sync_dir(&self.dir)?;
        }
        self.segments.push_back(SegInfo { id, len: 0, records: 0 });
        Ok(())
    }

    fn drop_oldest(&mut self) -> Result<()> {
        let head = self.segments.pop_front().expect("at least two segments");
        let unacked = if self.cursor.segment > head.id {
            0
        } else if self.cursor.segment == head.id {
            let buf = fs::read(self.dir.join(seg_name(head.id)))?;
            let start = (self.cursor.offset as usize).min(buf.len());
            frame::scan(&buf[start..]).frames.len() as u64
        } else {
            head.records
        };
        fs::remove_file(self.dir.join(seg_name(head.id)))?;
        self.stats.dropped += unacked;
        self.stats.pending_records -= unacked;
        if unacked > 0 {
            log::warn!("spool full: dropped {unacked} unforwarded record(s)");
        }
        let next = self.segments.front().expect("non-empty").id;
        if self.cursor.segment < next {
            self.cursor = Position { segment: next, offset: 0 };
            self.write_cursor()?;
        }
        Ok(())
    }

    /// Up to `max` unacknowledged records, oldest first.
    pub fn peek(&self, max: usize) -> Result<Vec<Entry>> {
        let mut out = Vec::new();
        for s in self.segments.iter().filter(|s| s.id >= self.cursor.segment) {
            if out.len() >= max {
                break;
            }
            let start = if s.id == self.cursor.segment { self.cursor.offset } else { 0 };
            if start >= s.len {
                continue;
            }
            let buf = fs::read(self.dir.join(seg_name(s.id)))?;
            let end = (s.len as usize).min(buf.len());
            let region = &buf[start as usize..end];
            for r in frame::scan(region).frames {
                if out.len() >= max {
                    break;
                }
                out.push(Entry {
                    pos: Position { segment: s.id, offset: start + r.end as u64 },
                    payload: region[r].to_vec(),
                });
            }
        }
        Ok(out)
    }

    /// Acknowledge every record up to and including the one ending at `pos`.
    pub fn ack(&mut self, pos: Position, records: u64) -> Result<()> {
        if pos < self.cursor {
            return Ok(());
        }
        self.cursor = pos;
        // Delete fully consumed non-tail segments.
        while self.segments.len() > 1 {
            let head = self.segments[0];
            let consumed =
                head.id < self.cursor.segment || (head.id == self.cursor.segment && self.cursor.offset >= head.len);
            if !consumed {
                break;
            }
            fs::remove_file(self.dir.join(seg_name(head.id)))?;
            self.segments.pop_front();
            let next = self.segments[0].id;
            if self.cursor.segment < next {
                self.cursor = Position { segment: next, offset: 0 };
            }
        }
        self.write_cursor()?;
        self.stats.acked += records;
        self.stats.pending_records = self.stats.pending_records.saturating_sub(records);
        Ok(())
    }

    fn write_cursor(&self) -> Result<()> {
        let mut body = Vec::with_capacity(20);
        body.extend_from_slice(&self.cursor.segment.to_le_bytes());
        body.extend_from_slice(&self.cursor.offset.to_le_bytes());
        body.extend_from_slice(&crc32fast::hash(&body).to_le_bytes());
        let tmp = self.dir.join("cursor.tmp");
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&body)?;
            if self.opts.sync == SyncMode::Always {
                f.sync_all()?;
            }
        }
        fs::rename(&tmp, self.dir.join("cursor"))?;
        if self.opts.sync == SyncMode::Always {
            crate::storage::sync_dir(&self.dir)?;
        }
        Ok(())
    }

    /// Counters.
    pub fn stats(&self) -> SpoolStats {
        SpoolStats { disk_bytes: self.disk_bytes(), ..self.stats }
    }

    /// True when nothing is waiting.
    pub fn is_empty(&self) -> bool {
        self.stats.pending_records == 0
    }
}

fn read_cursor(dir: &Path) -> Result<Option<Position>> {
    let path = dir.join("cursor");
    let buf = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    if buf.len() != 20 || crc32fast::hash(&buf[..16]).to_le_bytes() != buf[16..20] {
        return Err(Error::Corrupt(format!("spool cursor {} is damaged", path.display())));
    }
    let segment = u64::from_le_bytes(buf[0..8].try_into().expect("8"));
    let offset = u64::from_le_bytes(buf[8..16].try_into().expect("8"));
    Ok(Some(Position { segment, offset }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(seg: u64, max: u64, overflow: Overflow) -> SpoolOptions {
        SpoolOptions { segment_bytes: seg, max_bytes: max, overflow, sync: SyncMode::Os }
    }

    #[test]
    fn fifo_across_segments_and_restart() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), opts(64, 1 << 20, Overflow::DropOldest)).unwrap();
        for i in 0..20u8 {
            assert!(s.push(&[i; 10]).unwrap());
        }
        assert_eq!(s.stats().pending_records, 20);
        let first = s.peek(7).unwrap();
        assert_eq!(first.len(), 7);
        assert_eq!(first[0].payload, vec![0u8; 10]);
        s.ack(first[6].pos, 7).unwrap();
        drop(s);

        let mut s = Spool::open(dir.path(), opts(64, 1 << 20, Overflow::DropOldest)).unwrap();
        assert_eq!(s.stats().pending_records, 13);
        let rest = s.peek(100).unwrap();
        let firsts: Vec<u8> = rest.iter().map(|e| e.payload[0]).collect();
        assert_eq!(firsts, (7..20).collect::<Vec<u8>>());
        s.ack(rest.last().unwrap().pos, 13).unwrap();
        assert!(s.is_empty());
        assert!(s.peek(10).unwrap().is_empty());
        // Consumed segments were deleted (only the tail remains).
        let files = fs::read_dir(dir.path())
            .unwrap()
            .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().ends_with(".q"))
            .count();
        assert_eq!(files, 1);
        // Acking an older position is a no-op.
        s.ack(Position { segment: 0, offset: 0 }, 0).unwrap();
    }

    #[test]
    fn overflow_policies() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), opts(40, 80, Overflow::RejectNew)).unwrap();
        let mut accepted = 0;
        for i in 0..10u8 {
            if s.push(&[i; 10]).unwrap() {
                accepted += 1;
            }
        }
        assert!(accepted < 10);
        assert_eq!(s.stats().rejected, 10 - accepted);
        assert_eq!(s.peek(100).unwrap()[0].payload[0], 0, "oldest kept");

        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), opts(40, 80, Overflow::DropOldest)).unwrap();
        for i in 0..10u8 {
            assert!(s.push(&[i; 10]).unwrap());
        }
        let st = s.stats();
        assert!(st.dropped > 0);
        assert_eq!(st.pending_records + st.dropped, 10);
        let pending = s.peek(100).unwrap();
        assert_eq!(pending.last().unwrap().payload[0], 9, "newest kept");
        assert!(st.disk_bytes <= 80);
        assert!(s.push(&[0; 100]).is_err());
    }

    #[test]
    fn torn_tail_truncated_and_bad_cursor_detected() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = Spool::open(dir.path(), opts(1 << 20, 1 << 20, Overflow::DropOldest)).unwrap();
        s.push(b"one").unwrap();
        s.push(b"two").unwrap();
        drop(s);
        let seg = dir.path().join(seg_name(1));
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[9, 0, 0, 0, 1]).unwrap();
        drop(f);
        let s = Spool::open(dir.path(), opts(1 << 20, 1 << 20, Overflow::DropOldest)).unwrap();
        assert_eq!(s.stats().truncated_bytes, 5);
        assert_eq!(s.peek(10).unwrap().len(), 2);
        drop(s);
        fs::write(dir.path().join("cursor"), b"garbage").unwrap();
        assert!(Spool::open(dir.path(), opts(1 << 20, 1 << 20, Overflow::DropOldest)).is_err());
        assert!(Spool::open(dir.path(), opts(0, 1, Overflow::DropOldest)).is_err());
    }
}
