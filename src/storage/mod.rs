//! Durable storage: a crash-safe write-ahead log plus immutable columnar chunk segments.
//!
//! * [`frame`] — `[len][crc32][payload]` record framing with torn-tail detection, shared by the
//!   WAL and the store-and-forward spool.
//! * [`wal`] — write-ahead log of raw points, tag definitions and checkpoints.
//! * [`segment`] — immutable segment files holding Gorilla-encoded per-tag chunks and a
//!   checksummed footer index with per-chunk time/value bounds.
//! * [`archive`] — the engine that ties them together with per-tag SDT compression.

pub mod archive;
pub mod frame;
pub mod segment;
pub mod wal;

pub use archive::{Archive, ArchiveOptions, ArchiveStats, RecoveryReport, Series, TagInfo, WriteReport};
pub use wal::SyncMode;

use std::fs::File;
use std::path::Path;

/// fsync a directory so a rename or create inside it is durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}
