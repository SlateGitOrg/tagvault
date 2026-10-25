//! Crash-recovery tests for the write-ahead log.
//!
//! 1. A property test tears the WAL at every possible byte inside the last commit and checks the
//!    recovered archive equals a reference archive that never crashed and received exactly the
//!    fully-committed batches.
//! 2. A process-kill test re-executes this test binary as a child that writes continuously
//!    (fsync on, frequent flushes), SIGKILLs it at varying moments, and checks that recovery
//!    yields exactly what an uninterrupted compressor produces for the recovered prefix.

mod common;

use proptest::prelude::*;
use std::fs::{self, OpenOptions};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};
use tagvault::compress::sdt::Sdt;
use tagvault::config::Policy;
use tagvault::storage::{Archive, ArchiveOptions, SyncMode};
use tagvault::{Point, Sample, ValueKind};

const TAGS: [&str; 3] = ["U1/level", "U1/temp", "U1/flow"];
const COMP_DEV: f64 = 0.2;

/// Deterministic raw signal for tag `k` at index `i`.
fn raw(k: usize, i: i64) -> Point {
    let x = i as f64;
    let v = match k {
        0 => 50.0 + 20.0 * (x / 300.0).sin() + ((i * 7919) % 13) as f64 * 0.02,
        1 => {
            80.0 + if (i / 500) % 2 == 0 { 5.0 } else { -5.0 } + ((i * 104_729) % 11) as f64 * 0.03
        }
        _ => (x / 50.0).cos() * 3.0 + ((i * 31) % 5) as f64 * 0.05,
    };
    Point::good(1_700_000_000_000 + i * 1000, v)
}

fn batch(start: i64, len: i64) -> Vec<Sample> {
    (start..start + len)
        .flat_map(|i| {
            TAGS.iter().enumerate().map(move |(k, t)| Sample {
                tag: t.to_string(),
                kind: ValueKind::Analog,
                point: raw(k, i),
            })
        })
        .collect()
}

fn opts(flush_points: usize, sync: SyncMode) -> ArchiveOptions {
    ArchiveOptions {
        sync,
        flush_points,
        policy: Policy::uniform(0.0, COMP_DEV),
        ..Default::default()
    }
}

/// `(ts, value bits)` — bit-exact point identity.
type Bits = (i64, u64);

/// Everything observable about an archive: per tag, all stored points and the snapshot.
fn fingerprint(a: &Archive) -> Vec<(String, Vec<Bits>, Option<Bits>)> {
    a.tags()
        .into_iter()
        .map(|t| {
            let pts = a
                .series(&t.name, i64::MIN / 4, i64::MAX / 4)
                .unwrap()
                .points;
            let pts = pts.into_iter().map(|p| (p.ts, p.value.to_bits())).collect();
            (t.name, pts, t.snapshot.map(|p| (p.ts, p.value.to_bits())))
        })
        .collect()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 48, ..ProptestConfig::default() })]

    #[test]
    fn torn_last_commit_recovers_to_committed_prefix(
        batches in 2usize..12,
        batch_len in 5i64..60,
        flush_points in 20usize..200,
        tear_frac in 0.0f64..1.0,
    ) {
        let dir = common::tempdir();
        let wal = dir.path().join("wal.log");
        let mut a = Archive::open(dir.path(), opts(flush_points, SyncMode::Os)).unwrap();
        for b in 0..batches - 1 {
            a.write(&batch(b as i64 * batch_len, batch_len)).unwrap();
        }
        let before_last = fs::metadata(&wal).unwrap().len();
        let flushes = a.stats().flushes_since_open;
        a.write(&batch((batches - 1) as i64 * batch_len, batch_len)).unwrap();
        let after_last = fs::metadata(&wal).unwrap().len();
        let flushed_during_last = a.stats().flushes_since_open != flushes;
        drop(a); // crash: no close, memtable lost

        // Tear the last commit somewhere inside its bytes (only meaningful if it did not rotate).
        let full_batches = if !flushed_during_last && after_last > before_last {
            let cut = before_last + ((after_last - before_last - 1) as f64 * tear_frac) as u64;
            OpenOptions::new().write(true).open(&wal).unwrap().set_len(cut).unwrap();
            batches - 1
        } else {
            batches
        };
        let recovered = Archive::open(dir.path(), opts(flush_points, SyncMode::Os)).unwrap();
        recovered.verify().unwrap();

        let ref_dir = common::tempdir();
        let mut reference = Archive::open(ref_dir.path(), opts(flush_points, SyncMode::Os)).unwrap();
        for b in 0..full_batches {
            reference.write(&batch(b as i64 * batch_len, batch_len)).unwrap();
        }
        prop_assert_eq!(fingerprint(&recovered), fingerprint(&reference));
    }
}

/// Child-process body: write forever until killed. A no-op unless the env var is set.
#[test]
fn crash_child_writer() {
    let Ok(dir) = std::env::var("TAGVAULT_CRASH_CHILD_DIR") else {
        return;
    };
    let mut a = Archive::open(Path::new(&dir), opts(400, SyncMode::Always)).unwrap();
    let mut i = 0i64;
    loop {
        a.write(&batch(i, 25)).unwrap();
        i += 25;
    }
}

/// Run the reference compressor over tag `k` up to and including timestamp `last_ts`; return
/// what a query sees (archived points followed by the open segment) and the snapshot.
fn reference_for(k: usize, last_ts: i64) -> (Vec<Point>, Option<Point>) {
    let cfg = Policy::uniform(0.0, COMP_DEV).resolve(TAGS[k]).compression;
    let mut sdt = Sdt::new(cfg, ValueKind::Analog);
    let mut out = Vec::new();
    let mut i = 0;
    loop {
        let p = raw(k, i);
        if p.ts > last_ts {
            break;
        }
        sdt.offer(p, &mut out);
        i += 1;
    }
    out.extend(sdt.pending());
    (out, sdt.snapshot())
}

#[test]
fn sigkill_during_writes_recovers_a_consistent_prefix() {
    let exe = std::env::current_exe().unwrap();
    for (round, delay_ms) in [60u64, 150, 260, 410, 650].into_iter().enumerate() {
        let dir = common::tempdir();
        let mut child = Command::new(&exe)
            .args([
                "--exact",
                "crash_child_writer",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("TAGVAULT_CRASH_CHILD_DIR", dir.path())
            .spawn()
            .expect("spawn child");
        // Wait until the child has demonstrably written something, then let it run a while.
        let wal = dir.path().join("wal.log");
        let t0 = Instant::now();
        while fs::metadata(&wal).map(|m| m.len() < 512).unwrap_or(true)
            && t0.elapsed() < Duration::from_secs(20)
        {
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(delay_ms));
        child.kill().expect("SIGKILL child");
        child.wait().unwrap();

        let a = Archive::open(dir.path(), opts(400, SyncMode::Always)).unwrap();
        a.verify().unwrap();
        let tags = a.tags();
        assert_eq!(tags.len(), TAGS.len(), "round {round}: all tags recovered");
        for info in tags {
            let k = TAGS.iter().position(|t| *t == info.name).unwrap();
            let stored = a
                .series(&info.name, i64::MIN / 4, i64::MAX / 4)
                .unwrap()
                .points;
            let last_ts = stored.last().unwrap().ts;
            let (expected, expected_snapshot) = reference_for(k, last_ts);
            assert_eq!(
                stored, expected,
                "round {round} tag {}: archive differs from uninterrupted run",
                info.name
            );
            assert_eq!(info.snapshot, expected_snapshot);
        }
        eprintln!(
            "round {round}: recovered {:?}, {} segments",
            a.recovery(),
            a.stats().segments
        );
    }
}
