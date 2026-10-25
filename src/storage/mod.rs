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

/// An exclusive advisory lock on `<dir>/LOCK` (BSD `flock`), released when dropped.
///
/// `flock` locks belong to the open file description, not to the descriptor. While any thread
/// of the process forks or `posix_spawn`s a child, the child briefly holds a duplicate of every
/// descriptor, `O_CLOEXEC` ones included, until its `exec` closes them. If the lock were released
/// only by closing the file, a close that lands inside that window would leave the lock held by
/// the child's duplicate, and an immediate reopen of the directory would be refused as "in use".
/// Dropping a `DirLock` therefore unlocks explicitly (`LOCK_UN` acts on the shared description)
/// before closing. A crashed process still releases the lock when the kernel closes its files.
#[derive(Debug)]
pub(crate) struct DirLock {
    file: File,
}

impl Drop for DirLock {
    fn drop(&mut self) {
        if let Err(e) = self.file.unlock() {
            log::warn!("unlocking directory lock failed: {e}");
        }
    }
}

/// Take an exclusive advisory lock on `<dir>/LOCK`, held for as long as the returned guard lives.
pub(crate) fn lock_dir(dir: &Path) -> crate::error::Result<DirLock> {
    let file = std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("LOCK"))?;
    match file.try_lock() {
        Ok(()) => Ok(DirLock { file }),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(crate::error::Error::InvalidInput(format!("{} is in use by another tagvault process", dir.display())))
        }
        Err(std::fs::TryLockError::Error(e)) => Err(e.into()),
    }
}

/// fsync a directory so a rename or create inside it is durable.
pub(crate) fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(test)]
mod tests {
    use super::lock_dir;
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn a_held_lock_refuses_a_second_locker() {
        let dir = tempfile::tempdir().unwrap();
        let held = lock_dir(dir.path()).unwrap();
        let err = lock_dir(dir.path()).unwrap_err();
        assert!(err.to_string().contains("in use"), "{err}");
        drop(held);
        lock_dir(dir.path()).unwrap();
    }

    /// Regression: a lock dropped while another thread was spawning a child process stayed held
    /// by the child's transient copy of the descriptor, so an immediate re-lock failed with
    /// "in use by another tagvault process" (seen as a flaky crash-recovery test under load).
    #[test]
    fn lock_is_released_on_drop_while_another_thread_spawns_processes() {
        let dir = tempfile::tempdir().unwrap();
        let stop = Arc::new(AtomicBool::new(false));
        let spawned = Arc::new(AtomicUsize::new(0));
        let spawner = {
            let (stop, spawned) = (stop.clone(), spawned.clone());
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    Command::new("true").status().expect("spawn `true`");
                    spawned.fetch_add(1, Ordering::Relaxed);
                }
            })
        };
        let t0 = std::time::Instant::now();
        let (mut cycles, mut refused) = (0u64, 0u64);
        while spawned.load(Ordering::Relaxed) < 200 && t0.elapsed() < std::time::Duration::from_secs(60) {
            match lock_dir(dir.path()) {
                Ok(lock) => drop(lock),
                Err(_) => refused += 1,
            }
            cycles += 1;
        }
        stop.store(true, Ordering::Relaxed);
        spawner.join().unwrap();
        assert_eq!(
            refused,
            0,
            "{refused} of {cycles} re-locks refused during {} spawns",
            spawned.load(Ordering::Relaxed)
        );
    }
}
