//! Property tests for the core guarantees.

mod common;

use proptest::prelude::*;
use std::collections::VecDeque;
use tagvault::compress::deadband::{ExceptionConfig, ExceptionFilter};
use tagvault::compress::sdt::{compress_series, SdtConfig, SdtMode};
use tagvault::encoding::gorilla;
use tagvault::forward::batch;
use tagvault::forward::spool::{Overflow, Spool, SpoolOptions};
use tagvault::query;
use tagvault::storage::segment::{decode_chunk, encode_chunk};
use tagvault::storage::{Series, SyncMode};
use tagvault::{Point, Quality, Sample, ValueKind};

/// Random process-like signals: irregular sampling, drift, noise, occasional steps, optional
/// bad-quality points.
fn signal(with_bad: bool) -> impl Strategy<Value = Vec<Point>> {
    prop::collection::vec((1i64..5_000, -1.0f64..1.0, 0u8..100), 2..400).prop_map(move |steps| {
        let mut ts = 1_700_000_000_000i64;
        let mut v = 0.0f64;
        let mut out = Vec::with_capacity(steps.len());
        for (dt, r, kind) in steps {
            ts += dt;
            match kind {
                0..=4 => v += r * 50.0, // step change
                5..=60 => v += r * 0.5, // drift
                _ => v += r * 0.05,     // noise
            }
            let q = if with_bad && kind == 99 { Quality::Bad } else { Quality::Good };
            out.push(Point::with_quality(ts, if q.is_good() { v } else { f64::NAN }, q));
        }
        out
    })
}

fn lin(points: Vec<Point>) -> Series {
    Series { kind: ValueKind::Analog, points }
}

fn tol(dev: f64, pts: &[Point]) -> f64 {
    let scale = pts.iter().filter(|p| p.value.is_finite()).map(|p| p.value.abs()).fold(1.0, f64::max);
    dev + 1e-9 * scale
}

proptest! {
    #![proptest_config(common::proptest_config(512))]

    /// Strict and lookback SDT: linear reconstruction is within `dev` of every received good point.
    #[test]
    fn sdt_reconstruction_error_never_exceeds_deviation(
        pts in signal(true),
        dev in 0.0f64..5.0,
        max_ms in prop::option::of(1_000i64..200_000),
        lookback in any::<bool>(),
    ) {
        let mode = if lookback { SdtMode::Lookback } else { SdtMode::Strict };
        let arch = compress_series(SdtConfig { dev, max_ms, mode }, ValueKind::Analog, &pts);
        let s = lin(arch.clone());
        prop_assert!(arch.len() <= pts.len());
        for p in pts.iter().filter(|p| p.quality.is_good()) {
            let v = query::value_at(&s, p.ts);
            prop_assert!(v.is_some(), "undefined at {}", p.ts);
            let err = (v.unwrap() - p.value).abs();
            prop_assert!(err <= tol(dev, &pts), "{mode:?}: error {err} > dev {dev} at {}", p.ts);
        }
        // Archived values are original measurements, never synthesized.
        for a in &arch {
            prop_assert!(pts.iter().any(|p| p.ts == a.ts && p.value.to_bits() == a.value.to_bits()));
        }
    }

    /// Exception filter: hold reconstruction within `dev`, linear within `2·dev`.
    #[test]
    fn exception_deadband_bounds(pts in signal(false), dev in 0.0f64..3.0) {
        let mut f = ExceptionFilter::new(ExceptionConfig { dev, max_ms: None }, ValueKind::Analog);
        let mut rep = Vec::new();
        for &p in &pts {
            f.offer(p, &mut rep);
        }
        rep.extend(f.flush());
        let held = Series { kind: ValueKind::Discrete, points: rep.clone() };
        let linear = lin(rep);
        for p in &pts {
            let h = query::value_at(&held, p.ts).unwrap();
            prop_assert!((h - p.value).abs() <= tol(dev, &pts));
            let l = query::value_at(&linear, p.ts).unwrap();
            prop_assert!((l - p.value).abs() <= tol(2.0 * dev, &pts));
        }
    }

    /// Exception at the collector then SDT at the archive: within `2·exc + comp`.
    #[test]
    fn combined_pipeline_bound(pts in signal(false), exc in 0.0f64..1.0, comp in 0.0f64..2.0, lookback in any::<bool>()) {
        let mode = if lookback { SdtMode::Lookback } else { SdtMode::Strict };
        let mut f = ExceptionFilter::new(ExceptionConfig { dev: exc, max_ms: Some(60_000) }, ValueKind::Analog);
        let mut rep = Vec::new();
        for &p in &pts {
            f.offer(p, &mut rep);
        }
        rep.extend(f.flush());
        let arch = compress_series(SdtConfig { dev: comp, max_ms: Some(600_000), mode }, ValueKind::Analog, &rep);
        let s = lin(arch);
        for p in &pts {
            let v = query::value_at(&s, p.ts).unwrap();
            prop_assert!((v - p.value).abs() <= tol(2.0 * exc + comp, &pts));
        }
    }

    #[test]
    fn gorilla_roundtrips_any_values(vals in prop::collection::vec(any::<u64>(), 0..300)) {
        let f: Vec<f64> = vals.iter().map(|b| f64::from_bits(*b)).collect();
        let d = gorilla::decode_values(&gorilla::encode_values(&f)).unwrap();
        prop_assert_eq!(d.iter().map(|x| x.to_bits()).collect::<Vec<_>>(), vals);
    }

    #[test]
    fn gorilla_roundtrips_timestamps(start in -1i64 << 50..1i64 << 50, deltas in prop::collection::vec(0i64..1i64 << 40, 0..300)) {
        let mut ts = vec![start];
        for d in deltas {
            let next = *ts.last().unwrap() + d;
            ts.push(next);
        }
        prop_assert_eq!(gorilla::decode_timestamps(&gorilla::encode_timestamps(&ts)).unwrap(), ts);
    }

    #[test]
    fn decoders_never_panic_on_garbage(bytes in prop::collection::vec(any::<u8>(), 0..200)) {
        let _ = gorilla::decode_values(&bytes);
        let _ = gorilla::decode_timestamps(&bytes);
        let _ = decode_chunk(&bytes);
        let _ = batch::decode(&bytes);
    }

    #[test]
    fn chunk_and_batch_roundtrip(pts in signal(true)) {
        let dec = decode_chunk(&encode_chunk(&pts)).unwrap();
        prop_assert_eq!(dec.len(), pts.len());
        for (a, b) in pts.iter().zip(&dec) {
            prop_assert_eq!((a.ts, a.value.to_bits(), a.quality), (b.ts, b.value.to_bits(), b.quality));
        }
        let samples: Vec<Sample> = pts.iter().map(|p| Sample { tag: "t".into(), kind: ValueKind::Analog, point: *p }).collect();
        let back = batch::decode(&batch::encode(&samples)).unwrap();
        prop_assert_eq!(back.len(), samples.len());
    }

    /// The exact time-weighted average equals a fine Riemann sum of the reconstruction.
    #[test]
    fn time_weighted_average_matches_numeric_integration(pts in signal(false)) {
        let s = lin(pts.clone());
        let (a, b) = (pts[0].ts, pts.last().unwrap().ts);
        prop_assume!(b - a >= 100);
        let agg = query::aggregate(&s, a, b, b - a).unwrap();
        let avg = agg[0].avg.unwrap();
        let n = 20_000i64;
        let step = (b - a) as f64 / n as f64;
        let mut sum = 0.0;
        for k in 0..n {
            let t = a as f64 + (k as f64 + 0.5) * step;
            let lo = query::value_at(&s, t.floor() as i64).unwrap();
            let hi = query::value_at(&s, (t.floor() as i64 + 1).min(b)).unwrap();
            sum += lo + (hi - lo) * (t - t.floor());
        }
        let numeric = sum / n as f64;
        let span = pts.iter().map(|p| p.value).fold(f64::MIN, f64::max) - pts.iter().map(|p| p.value).fold(f64::MAX, f64::min);
        prop_assert!((avg - numeric).abs() <= 1e-3 * (1.0 + span), "exact {avg} vs numeric {numeric}");
        prop_assert!(agg[0].min.unwrap() <= avg && avg <= agg[0].max.unwrap());
    }
}

#[derive(Debug, Clone)]
enum Op {
    Push(u8),
    Deliver(usize),
    Reopen,
}

proptest! {
    #![proptest_config(common::proptest_config(64))]

    /// The spool behaves like a FIFO queue across restarts (no overflow).
    #[test]
    fn spool_is_a_durable_fifo(ops in prop::collection::vec(prop_oneof![
        4 => any::<u8>().prop_map(Op::Push),
        2 => (1usize..6).prop_map(Op::Deliver),
        1 => Just(Op::Reopen),
    ], 1..80)) {
        let dir = tempfile::tempdir().unwrap();
        let opts = SpoolOptions { segment_bytes: 64, max_bytes: 1 << 30, overflow: Overflow::DropOldest, sync: SyncMode::Os };
        let mut spool = Spool::open(dir.path(), opts).unwrap();
        let mut model: VecDeque<Vec<u8>> = VecDeque::new();
        for op in ops {
            match op {
                Op::Push(b) => {
                    let rec = vec![b; (b % 17) as usize + 1];
                    prop_assert!(spool.push(&rec).unwrap());
                    model.push_back(rec);
                }
                Op::Deliver(n) => {
                    let got = spool.peek(n).unwrap();
                    let want: Vec<Vec<u8>> = model.iter().take(n).cloned().collect();
                    prop_assert_eq!(got.iter().map(|e| e.payload.clone()).collect::<Vec<_>>(), want);
                    if let Some(last) = got.last() {
                        spool.ack(last.pos, got.len() as u64).unwrap();
                        for _ in 0..got.len() {
                            model.pop_front();
                        }
                    }
                }
                Op::Reopen => {
                    drop(spool);
                    spool = Spool::open(dir.path(), opts).unwrap();
                }
            }
            prop_assert_eq!(spool.stats().pending_records, model.len() as u64);
        }
    }
}
