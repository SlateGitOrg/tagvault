//! Archive engine integration tests: write/flush/reopen, chunk-boundary reads, idempotent
//! redelivery, and orphan cleanup.

mod common;

use std::fs;
use tagvault::config::Policy;
use tagvault::storage::{Archive, ArchiveOptions, SyncMode};
use tagvault::{query, Point, Quality, Sample, ValueKind};

fn opts(flush_points: usize) -> ArchiveOptions {
    ArchiveOptions {
        sync: SyncMode::Os,
        flush_points,
        policy: Policy::uniform(0.0, 0.05),
        ..Default::default()
    }
}

fn wave(tag: &str, i: i64) -> Sample {
    let v = (i as f64 / 40.0).sin() * 10.0 + (i % 7) as f64 * 0.01;
    Sample {
        tag: tag.into(),
        kind: ValueKind::Analog,
        point: Point::good(i * 1000, v),
    }
}

fn write_all(a: &mut Archive, range: std::ops::Range<i64>) {
    for chunk in range.collect::<Vec<_>>().chunks(100) {
        let batch: Vec<Sample> = chunk
            .iter()
            .flat_map(|&i| [wave("A/flow", i), wave("A/temp", i * 2)])
            .collect();
        a.write(&batch).unwrap();
    }
}

#[test]
fn write_flush_reopen_preserves_every_point() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(300)).unwrap();
    write_all(&mut a, 0..5000);
    let before = a.series("A/flow", 0, 10_000_000).unwrap();
    let tags_before = a.tags();
    let stats = a.stats();
    assert!(
        stats.segments >= 2,
        "small flush threshold must produce several segments"
    );
    assert_eq!(stats.accepted_since_open, 10_000);
    a.close().unwrap();

    let b = Archive::open(dir.path(), opts(300)).unwrap();
    assert_eq!(b.series("A/flow", 0, 10_000_000).unwrap(), before);
    assert_eq!(b.tags(), tags_before);
    assert_eq!(
        b.recovery().replayed_points,
        0,
        "clean close leaves nothing to replay"
    );
    assert!(b.verify().unwrap() > 0);
    // Compression actually happened.
    let flow = b.tags().into_iter().find(|t| t.name == "A/flow").unwrap();
    assert!(
        flow.archived_points < 2500,
        "archived {}",
        flow.archived_points
    );
}

#[test]
fn series_includes_boundary_points_across_chunks() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(50)).unwrap();
    write_all(&mut a, 0..3000);
    let all = a
        .series("A/flow", i64::MIN / 2, i64::MAX / 2)
        .unwrap()
        .points;
    // A window strictly inside the data must carry one point before and one after it.
    let (start, end) = (1_234_567, 1_500_000);
    let s = a.series("A/flow", start, end).unwrap();
    assert!(s.points.first().unwrap().ts < start);
    assert!(s.points.last().unwrap().ts > end);
    let inside: Vec<_> = all
        .iter()
        .filter(|p| p.ts >= start && p.ts <= end)
        .copied()
        .collect();
    assert_eq!(&s.points[1..s.points.len() - 1], inside.as_slice());
    // Interpolation from the archive agrees with interpolation over the full series.
    let full = tagvault::storage::Series {
        kind: ValueKind::Analog,
        points: all.clone(),
    };
    for t in (start..=end).step_by(7_919) {
        assert_eq!(query::value_at(&s, t), query::value_at(&full, t));
    }
    // A window before all data has no "before" point; after all data ends at the snapshot.
    let early = a.series("A/flow", -10_000, -1).unwrap();
    assert_eq!(early.points.len(), 1);
    let late = a.series("A/flow", 10_000_000, 20_000_000).unwrap();
    assert_eq!(late.points.len(), 1);
    assert!(a.series("nope", 0, 1).is_err());
    assert!(a.series("A/flow", 5, 1).is_err());
}

#[test]
fn redelivered_and_out_of_order_points_are_rejected() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(1000)).unwrap();
    let batch: Vec<Sample> = (0..10).map(|i| wave("T", i)).collect();
    let r = a.write(&batch).unwrap();
    assert_eq!((r.accepted, r.rejected_out_of_order), (10, 0));
    // Redelivery of the same batch (e.g. the spool resends after a crash) is a no-op.
    let r = a.write(&batch).unwrap();
    assert_eq!((r.accepted, r.rejected_out_of_order), (0, 10));
    // Within one batch, a timestamp going backwards is rejected too.
    let mixed = vec![wave("T", 20), wave("T", 15), wave("T", 21)];
    let r = a.write(&mixed).unwrap();
    assert_eq!((r.accepted, r.rejected_out_of_order), (2, 1));
    assert_eq!(a.stats().rejected_since_open, 11);
}

#[test]
fn quality_and_discrete_tags_roundtrip() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(1000)).unwrap();
    let pump = |i: i64, v: f64| Sample {
        tag: "P/run".into(),
        kind: ValueKind::Discrete,
        point: Point::good(i, v),
    };
    let level = |i: i64, v: f64, q: Quality| Sample {
        tag: "P/lvl".into(),
        kind: ValueKind::Analog,
        point: Point::with_quality(i, v, q),
    };
    a.write(&[
        pump(0, 0.0),
        pump(1, 0.0),
        pump(2, 1.0),
        pump(3, 1.0),
        pump(4, 0.0),
    ])
    .unwrap();
    a.write(&[
        level(0, 1.0, Quality::Good),
        level(5, f64::NAN, Quality::Stale),
        level(9, 2.0, Quality::Good),
    ])
    .unwrap();
    a.close().unwrap();
    let a = Archive::open(dir.path(), opts(1000)).unwrap();
    let run = a.series("P/run", 0, 10).unwrap();
    assert_eq!(run.kind, ValueKind::Discrete);
    assert_eq!(
        run.points.iter().map(|p| p.ts).collect::<Vec<_>>(),
        vec![0, 2, 4]
    );
    let lvl = a.series("P/lvl", 0, 10).unwrap();
    assert_eq!(lvl.points[1].quality, Quality::Stale);
    assert_eq!(query::value_at(&lvl, 7), None);
}

#[test]
fn orphan_segments_and_temp_files_are_removed_on_open() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(100)).unwrap();
    write_all(&mut a, 0..1000);
    a.close().unwrap();
    let seg_dir = dir.path().join("segments");
    let mut segs: Vec<_> = fs::read_dir(&seg_dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .collect();
    segs.sort();
    let last = segs.last().unwrap().clone();
    // A segment written by a flush whose checkpoint never landed, plus a half-written temp file.
    fs::copy(&last, seg_dir.join("seg-9999999999.tvs")).unwrap();
    fs::write(seg_dir.join("seg-0000000077.tvs.tmp"), b"partial").unwrap();
    let a = Archive::open(dir.path(), opts(100)).unwrap();
    assert_eq!(a.recovery().orphan_segments_removed, 1);
    assert_eq!(a.recovery().temp_files_removed, 1);
    assert_eq!(fs::read_dir(&seg_dir).unwrap().count(), segs.len());
    a.verify().unwrap();
}

#[test]
fn flush_snapshots_archives_held_points_and_policy_reapplies_on_open() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(1000)).unwrap();
    a.write(&(0..10).map(|i| wave("S", i)).collect::<Vec<_>>())
        .unwrap();
    assert!(a.tags()[0].snapshot.is_some());
    a.flush_snapshots().unwrap();
    assert!(a.tags()[0].snapshot.is_none());
    let n = a.tags()[0].archived_points;
    drop(a);
    let wider = ArchiveOptions {
        policy: Policy::uniform(0.0, 3.0),
        ..opts(1000)
    };
    let a = Archive::open(dir.path(), wider).unwrap();
    assert_eq!(a.tags()[0].comp_dev, 3.0);
    assert_eq!(a.tags()[0].archived_points, n);
    assert_eq!(a.dir(), dir.path());
}

#[test]
fn corrupt_segment_inside_checkpoint_is_an_error() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(100)).unwrap();
    write_all(&mut a, 0..500);
    a.close().unwrap();
    let seg = fs::read_dir(dir.path().join("segments"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let bytes = fs::read(&seg).unwrap();
    fs::write(&seg, &bytes[..bytes.len() - 1]).unwrap();
    assert!(Archive::open(dir.path(), opts(100)).is_err());
}

#[test]
fn second_open_of_the_same_directory_is_refused() {
    let dir = common::tempdir();
    let a = Archive::open(dir.path(), opts(100)).unwrap();
    let err = Archive::open(dir.path(), opts(100)).unwrap_err();
    assert!(err.to_string().contains("in use"), "{err}");
    drop(a);
    Archive::open(dir.path(), opts(100)).unwrap();
}

#[test]
fn policy_changes_apply_on_open_but_read_only_opens_keep_settings() {
    let dir = common::tempdir();
    let mut a = Archive::open(dir.path(), opts(1000)).unwrap();
    write_all(&mut a, 0..200);
    drop(a); // crash: the WAL holds TagDefs with the settings they were created with
    let ro = ArchiveOptions {
        reconfigure_existing: false,
        policy: Policy::uniform(0.0, 9.0),
        ..opts(1000)
    };
    let a = Archive::open(dir.path(), ro).unwrap();
    assert!(
        a.tags().iter().all(|t| t.comp_dev == 0.05),
        "read-only open keeps logged settings"
    );
    assert_eq!(a.stats().flushes_since_open, 0);
    drop(a);
    let changed = ArchiveOptions {
        policy: Policy::uniform(0.0, 9.0),
        ..opts(1000)
    };
    let a = Archive::open(dir.path(), changed).unwrap();
    assert!(a.tags().iter().all(|t| t.comp_dev == 9.0));
    assert_eq!(
        a.stats().flushes_since_open,
        1,
        "a settings change is checkpointed immediately"
    );
    drop(a);
    let a = Archive::open(
        dir.path(),
        ArchiveOptions {
            reconfigure_existing: false,
            ..opts(1000)
        },
    )
    .unwrap();
    assert!(a.tags().iter().all(|t| t.comp_dev == 9.0));
}
