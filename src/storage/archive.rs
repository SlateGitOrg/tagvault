//! The archive engine: WAL → per-tag SDT → memtable → segments.
//!
//! Write path for a batch of samples:
//!
//! 1. Resolve or create each tag (a `TagDef` record is logged for new tags).
//! 2. Reject points whose timestamp is not after the tag's last accepted timestamp. This also
//!    makes redelivery from the store-and-forward spool idempotent.
//! 3. Log the accepted raw points as one WAL record and commit (fsync).
//! 4. Feed each point to the tag's swinging-door compressor; archived points go to the memtable.
//! 5. When the memtable or WAL passes its threshold, flush: write a segment, then rotate the WAL
//!    to a new checkpoint holding every compressor's state.
//!
//! Recovery loads the checkpoint, discards segments newer than it (a flush that crashed before
//! its checkpoint landed — their contents are still in the WAL), and replays the WAL tail through
//! the same code path.

use super::segment::{self, ChunkMeta, Segment};
use super::wal::{Checkpoint, SyncMode, TagCheckpoint, Wal, WalRecord};
use crate::compress::sdt::Sdt;
use crate::config::Policy;
use crate::error::{Error, Result};
use crate::types::{Point, Sample, TagId, Timestamp, ValueKind};
use serde::Serialize;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const WAL_FILE: &str = "wal.log";
const SEGMENT_DIR: &str = "segments";

/// Archive tuning knobs.
#[derive(Debug, Clone)]
pub struct ArchiveOptions {
    /// WAL durability.
    pub sync: SyncMode,
    /// Flush when this many archived points are buffered in memory.
    pub flush_points: usize,
    /// Flush (to rotate the WAL) when the WAL grows past this many bytes.
    pub wal_max_bytes: u64,
    /// Compression policy per tag.
    pub policy: Policy,
}

impl Default for ArchiveOptions {
    fn default() -> Self {
        ArchiveOptions { sync: SyncMode::Always, flush_points: 50_000, wal_max_bytes: 32 << 20, policy: Policy::default() }
    }
}

/// Public description of a tag.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TagInfo {
    /// Tag id.
    pub id: TagId,
    /// Tag name.
    pub name: String,
    /// Interpolation behaviour.
    pub kind: ValueKind,
    /// Compression deviation in force.
    pub comp_dev: f64,
    /// Archived points (segments + memtable, excluding the snapshot).
    pub archived_points: u64,
    /// Latest received point (the snapshot), if any.
    pub snapshot: Option<Point>,
}

/// Outcome of one [`Archive::write`] call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
pub struct WriteReport {
    /// Points accepted and logged.
    pub accepted: u64,
    /// Points rejected because their timestamp was not after the tag's last timestamp.
    pub rejected_out_of_order: u64,
    /// Points that became archived (passed compression) during this call.
    pub archived: u64,
}

/// What [`Archive::open`] had to do to reach a consistent state.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RecoveryReport {
    /// WAL records replayed after the checkpoint.
    pub replayed_records: usize,
    /// Raw points replayed.
    pub replayed_points: usize,
    /// Torn WAL tail bytes truncated.
    pub truncated_bytes: usize,
    /// Segments removed because their flush never reached a checkpoint.
    pub orphan_segments_removed: usize,
    /// Temporary files removed.
    pub temp_files_removed: usize,
}

/// Size and volume counters.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct ArchiveStats {
    /// Number of tags.
    pub tags: usize,
    /// Number of segments.
    pub segments: usize,
    /// Bytes in segment files.
    pub segment_bytes: u64,
    /// Bytes in the WAL.
    pub wal_bytes: u64,
    /// Archived points in segments.
    pub segment_points: u64,
    /// Archived points still in memory.
    pub memtable_points: u64,
    /// Points accepted since open.
    pub accepted_since_open: u64,
    /// Out-of-order points rejected since open.
    pub rejected_since_open: u64,
    /// Flushes since open.
    pub flushes_since_open: u64,
}

/// Archived data for one tag over a requested range, ready for the query engine.
#[derive(Debug, Clone, PartialEq)]
pub struct Series {
    /// Interpolation behaviour.
    pub kind: ValueKind,
    /// Time-ordered points covering the range: every stored point inside it, plus the nearest
    /// point before and after (when they exist) for boundary interpolation. The snapshot is
    /// included when it falls in or after the range.
    pub points: Vec<Point>,
}

#[derive(Debug)]
struct TagState {
    id: TagId,
    name: String,
    sdt: Sdt,
    last_ts: Option<Timestamp>,
    mem: Vec<Point>,
    /// (segment index, chunk index) in time order.
    chunks: Vec<(usize, usize)>,
}

/// An open archive directory.
#[derive(Debug)]
pub struct Archive {
    dir: PathBuf,
    opts: ArchiveOptions,
    wal: Wal,
    tags: Vec<TagState>,
    by_name: HashMap<String, TagId>,
    segments: Vec<Segment>,
    last_segment: u64,
    mem_points: usize,
    stats: ArchiveStats,
    poisoned: bool,
    recovery: RecoveryReport,
}

impl Archive {
    /// Open (or create) an archive in `dir`, recovering from any crash.
    pub fn open(dir: &Path, opts: ArchiveOptions) -> Result<Archive> {
        opts.policy.validate()?;
        let seg_dir = dir.join(SEGMENT_DIR);
        fs::create_dir_all(&seg_dir)?;
        let wal_path = dir.join(WAL_FILE);
        let mut recovery = RecoveryReport::default();
        let (wal, records) = if wal_path.exists() {
            let (wal, records, rec) = Wal::open(&wal_path, opts.sync)?;
            recovery.truncated_bytes = rec.truncated_bytes;
            (wal, records)
        } else {
            let cp = Checkpoint::default();
            (Wal::create(&wal_path, &cp, opts.sync)?, vec![WalRecord::Checkpoint(cp)])
        };
        let mut records = records.into_iter();
        let Some(WalRecord::Checkpoint(cp)) = records.next() else {
            return Err(Error::Corrupt("WAL must start with a checkpoint".into()));
        };

        // Segments: keep those covered by the checkpoint, drop orphans and temp files.
        let mut ids = Vec::new();
        for entry in fs::read_dir(&seg_dir)? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".tmp") {
                fs::remove_file(entry.path())?;
                recovery.temp_files_removed += 1;
            } else if let Some(id) = segment::parse_file_name(&name) {
                if id > cp.last_segment {
                    log::warn!("removing orphan segment {name} (flush did not reach a checkpoint)");
                    fs::remove_file(entry.path())?;
                    recovery.orphan_segments_removed += 1;
                } else {
                    ids.push(id);
                }
            }
        }
        ids.sort_unstable();
        let mut segments = Vec::with_capacity(ids.len());
        for id in ids {
            segments.push(Segment::open(&seg_dir.join(segment::file_name(id)))?);
        }

        let mut archive = Archive {
            dir: dir.to_path_buf(),
            opts,
            wal,
            tags: Vec::new(),
            by_name: HashMap::new(),
            segments,
            last_segment: cp.last_segment,
            mem_points: 0,
            stats: ArchiveStats::default(),
            poisoned: false,
            recovery: RecoveryReport::default(),
        };
        for t in cp.tags {
            archive.install_tag(t.id, t.name, t.sdt, t.last_ts)?;
        }
        for (si, seg) in archive.segments.iter().enumerate() {
            for (ci, meta) in seg.chunks().iter().enumerate() {
                let tag = archive
                    .tags
                    .get_mut(meta.tag as usize)
                    .ok_or_else(|| Error::Corrupt(format!("segment {} references unknown tag {}", seg.id(), meta.tag)))?;
                tag.chunks.push((si, ci));
            }
        }
        for rec in records {
            recovery.replayed_records += 1;
            match rec {
                WalRecord::Checkpoint(_) => return Err(Error::Corrupt("checkpoint in WAL body".into())),
                WalRecord::TagDef { id, name, kind } => {
                    let sdt = Sdt::new(archive.opts.policy.resolve(&name).compression, kind);
                    archive.install_tag(id, name, sdt, None)?;
                }
                WalRecord::Points(pts) => {
                    recovery.replayed_points += pts.len();
                    archive.apply(&pts)?;
                }
            }
        }
        archive.recovery = recovery;
        archive.stats.accepted_since_open = 0;
        if archive.recovery != RecoveryReport::default() {
            log::info!("archive {} recovered: {:?}", dir.display(), archive.recovery);
        }
        Ok(archive)
    }

    fn install_tag(&mut self, id: TagId, name: String, mut sdt: Sdt, last_ts: Option<Timestamp>) -> Result<()> {
        if id as usize != self.tags.len() || self.by_name.contains_key(&name) {
            return Err(Error::Corrupt(format!("tag {id} ({name}) out of sequence")));
        }
        // The policy file is the source of truth for settings; door state carries over.
        sdt.set_config(self.opts.policy.resolve(&name).compression);
        self.by_name.insert(name.clone(), id);
        self.tags.push(TagState { id, name, sdt, last_ts, mem: Vec::new(), chunks: Vec::new() });
        Ok(())
    }

    /// Feed logged points to the compressors (shared by the write path and recovery).
    fn apply(&mut self, pts: &[(TagId, Point)]) -> Result<u64> {
        let mut archived = 0u64;
        let mut out = Vec::new();
        for (id, p) in pts {
            let tag = self.tags.get_mut(*id as usize).ok_or_else(|| Error::Corrupt(format!("unknown tag id {id}")))?;
            tag.last_ts = Some(p.ts);
            out.clear();
            tag.sdt.offer(*p, &mut out);
            archived += out.len() as u64;
            self.mem_points += out.len();
            tag.mem.extend_from_slice(&out);
        }
        Ok(archived)
    }

    /// Durably accept a batch of samples.
    pub fn write(&mut self, samples: &[Sample]) -> Result<WriteReport> {
        if self.poisoned {
            return Err(Error::InvalidInput("archive is read-only after a failed WAL write; reopen it".into()));
        }
        let mut report = WriteReport::default();
        let mut accepted: Vec<(TagId, Point)> = Vec::with_capacity(samples.len());
        let mut new_tags = 0usize;
        // Last timestamp per tag including points accepted earlier in this batch.
        let mut batch_last: HashMap<TagId, Timestamp> = HashMap::new();
        for s in samples {
            let id = match self.by_name.get(&s.tag) {
                Some(&id) => id,
                None => {
                    let id = TagId::try_from(self.tags.len()).map_err(|_| Error::InvalidInput("too many tags".into()))?;
                    self.wal.append(&WalRecord::TagDef { id, name: s.tag.clone(), kind: s.kind });
                    let sdt = Sdt::new(self.opts.policy.resolve(&s.tag).compression, s.kind);
                    self.install_tag(id, s.tag.clone(), sdt, None)?;
                    new_tags += 1;
                    id
                }
            };
            let last = batch_last.get(&id).copied().or(self.tags[id as usize].last_ts);
            if last.is_some_and(|l| s.point.ts <= l) {
                report.rejected_out_of_order += 1;
                continue;
            }
            batch_last.insert(id, s.point.ts);
            accepted.push((id, s.point));
        }
        if !accepted.is_empty() {
            self.wal.append(&WalRecord::Points(accepted.clone()));
        }
        if let Err(e) = self.wal.commit() {
            self.poisoned = true;
            return Err(e);
        }
        if new_tags > 0 {
            log::debug!("created {new_tags} tag(s)");
        }
        report.accepted = accepted.len() as u64;
        report.archived = self.apply(&accepted)?;
        self.stats.accepted_since_open += report.accepted;
        self.stats.rejected_since_open += report.rejected_out_of_order;
        if self.mem_points >= self.opts.flush_points || self.wal.len() >= self.opts.wal_max_bytes {
            self.flush()?;
        }
        Ok(report)
    }

    /// Write buffered archived points to a new segment and rotate the WAL to a fresh checkpoint.
    pub fn flush(&mut self) -> Result<()> {
        let seg_dir = self.dir.join(SEGMENT_DIR);
        let chunks: Vec<(TagId, &[Point])> =
            self.tags.iter().filter(|t| !t.mem.is_empty()).map(|t| (t.id, t.mem.as_slice())).collect();
        let mut new_segment = None;
        if !chunks.is_empty() {
            let id = self.last_segment + 1;
            new_segment = Some(Segment::write(&seg_dir, id, &chunks)?);
        }
        let last_segment = new_segment.as_ref().map_or(self.last_segment, |s| s.id());
        let cp = Checkpoint {
            last_segment,
            tags: self
                .tags
                .iter()
                .map(|t| TagCheckpoint { id: t.id, name: t.name.clone(), sdt: t.sdt.clone(), last_ts: t.last_ts })
                .collect(),
        };
        self.wal = Wal::create(&self.dir.join(WAL_FILE), &cp, self.opts.sync)?;
        if let Some(seg) = new_segment {
            let si = self.segments.len();
            for (ci, meta) in seg.chunks().iter().enumerate() {
                self.tags[meta.tag as usize].chunks.push((si, ci));
            }
            self.segments.push(seg);
        }
        self.last_segment = last_segment;
        for t in &mut self.tags {
            t.mem.clear();
        }
        self.mem_points = 0;
        self.stats.flushes_since_open += 1;
        Ok(())
    }

    /// Archive every snapshot (held) point and flush. Use before a final export; normal shutdown
    /// does not need it because snapshots are part of the checkpoint.
    pub fn flush_snapshots(&mut self) -> Result<()> {
        for t in &mut self.tags {
            if let Some(p) = t.sdt.flush() {
                t.mem.push(p);
                self.mem_points += 1;
            }
        }
        self.flush()
    }

    /// Flush and close.
    pub fn close(mut self) -> Result<()> {
        self.flush()
    }

    /// What recovery did when this archive was opened.
    pub fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Look up a tag id by name.
    pub fn tag_id(&self, name: &str) -> Option<TagId> {
        self.by_name.get(name).copied()
    }

    fn tag(&self, name: &str) -> Result<&TagState> {
        let id = self.tag_id(name).ok_or_else(|| Error::UnknownTag(name.to_string()))?;
        Ok(&self.tags[id as usize])
    }

    fn chunk_meta(&self, r: (usize, usize)) -> &ChunkMeta {
        &self.segments[r.0].chunks()[r.1]
    }

    /// Describe every tag, sorted by name.
    pub fn tags(&self) -> Vec<TagInfo> {
        let mut v: Vec<TagInfo> = self
            .tags
            .iter()
            .map(|t| TagInfo {
                id: t.id,
                name: t.name.clone(),
                kind: t.sdt.kind(),
                comp_dev: t.sdt.config().dev,
                archived_points: t.chunks.iter().map(|&r| self.chunk_meta(r).count as u64).sum::<u64>() + t.mem.len() as u64,
                snapshot: t.sdt.snapshot(),
            })
            .collect();
        v.sort_by(|a, b| a.name.cmp(&b.name));
        v
    }

    /// Archived points for `tag` covering `[start, end]` (see [`Series`]).
    pub fn series(&self, tag: &str, start: Timestamp, end: Timestamp) -> Result<Series> {
        if end < start {
            return Err(Error::InvalidInput(format!("end {end} is before start {start}")));
        }
        let t = self.tag(tag)?;
        let metas: Vec<&ChunkMeta> = t.chunks.iter().map(|&r| self.chunk_meta(r)).collect();
        // Chunks of one tag are time-ordered and non-overlapping.
        let first_overlap = metas.partition_point(|m| m.t_max < start);
        let after_end = metas.partition_point(|m| m.t_min <= end);
        let lo = first_overlap.saturating_sub(1);
        let hi = (after_end + 1).min(metas.len());
        let mut pts = Vec::new();
        for i in lo..hi {
            let (si, ci) = t.chunks[i];
            pts.extend(self.segments[si].read_chunk(&self.segments[si].chunks()[ci])?);
        }
        if after_end == metas.len() {
            pts.extend_from_slice(&t.mem);
            pts.extend(t.sdt.snapshot());
        }
        let a = pts.partition_point(|p| p.ts < start).saturating_sub(1);
        let b = (pts.partition_point(|p| p.ts <= end) + 1).min(pts.len());
        let points = if a < b { pts[a..b].to_vec() } else { Vec::new() };
        Ok(Series { kind: t.sdt.kind(), points })
    }

    /// Counters and sizes.
    pub fn stats(&self) -> ArchiveStats {
        ArchiveStats {
            tags: self.tags.len(),
            segments: self.segments.len(),
            segment_bytes: self.segments.iter().map(|s| s.size()).sum(),
            wal_bytes: self.wal.len(),
            segment_points: self.segments.iter().flat_map(|s| s.chunks()).map(|c| c.count as u64).sum(),
            memtable_points: self.mem_points as u64,
            ..self.stats.clone()
        }
    }

    /// Re-read every chunk and verify checksums and ordering. Returns the number of points checked.
    pub fn verify(&self) -> Result<u64> {
        let mut n = 0u64;
        for t in &self.tags {
            let mut prev: Option<Timestamp> = None;
            for &(si, ci) in &t.chunks {
                for p in self.segments[si].read_chunk(&self.segments[si].chunks()[ci])? {
                    if prev.is_some_and(|q| p.ts <= q) {
                        return Err(Error::Corrupt(format!("tag {} has non-increasing timestamps", t.name)));
                    }
                    prev = Some(p.ts);
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Directory of this archive.
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}
