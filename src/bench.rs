//! Compression and query benchmarks on generated process signals, with a tuning/held-out split.
//!
//! * **Tuning split** (seeds `1..=tune_seeds`): for each signal class, pick the largest
//!   compression deviation (as a fraction of span, from a fixed grid) whose RMSE stays within
//!   `rmse_target` of span. Exception deviation is half the compression deviation (the usual
//!   PI rule of thumb).
//! * **Held-out split** (seeds `101..`): apply the tuned settings and report point reduction,
//!   bytes per point, worst-case error relative to the guaranteed bound, and RMSE — plus the
//!   same data under classic SDT, exception-only, and lossless Gorilla for comparison.
//! * **Query benchmark**: load the held-out series into an archive and time raw, interpolated
//!   and aggregate queries; compare archive time-weighted averages with those of the raw data.

use crate::compress::deadband::{ExceptionConfig, ExceptionFilter};
use crate::compress::sdt::{compress_series, SdtConfig, SdtMode};
use crate::config::{Policy, Rule, TagSettings};
use crate::error::Result;
use crate::query;
use crate::sim::signals::{series, SignalClass};
use crate::storage::segment::encode_chunk;
use crate::storage::{Archive, ArchiveOptions, Series, SyncMode};
use crate::types::{Point, Sample, ValueKind};
use serde::Serialize;
use std::path::Path;
use std::time::Instant;

/// Benchmark size.
#[derive(Debug, Clone, Copy)]
pub struct BenchConfig {
    /// Seeds in the tuning split.
    pub tune_seeds: u64,
    /// Seeds in the held-out split.
    pub heldout_seeds: u64,
    /// Samples per series (1 s scan).
    pub points: usize,
    /// RMSE target as a fraction of span for tuning.
    pub rmse_target: f64,
}

impl BenchConfig {
    /// Full benchmark: 8 tuning + 16 held-out series per class, 6 h at 1 s each.
    pub fn full() -> Self {
        BenchConfig { tune_seeds: 8, heldout_seeds: 16, points: 6 * 3600, rmse_target: 0.001 }
    }

    /// Small configuration for tests.
    pub fn quick() -> Self {
        BenchConfig { tune_seeds: 2, heldout_seeds: 3, points: 3000, rmse_target: 0.001 }
    }
}

/// Grid of compression deviations, as fractions of span.
pub const DEV_GRID: [f64; 6] = [0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01];

const START: i64 = crate::sim::capture::DEFAULT_START;
const RAW_BYTES_PER_POINT: f64 = 16.0;

fn heldout_seed(i: u64) -> u64 {
    100 + i + 1
}

/// Exception then SDT, returning (archived points, exception-reported count).
fn reduce(pts: &[Point], exc: f64, comp: f64, mode: SdtMode) -> (Vec<Point>, usize) {
    let mut f = ExceptionFilter::new(ExceptionConfig { dev: exc, max_ms: None }, ValueKind::Analog);
    let mut rep = Vec::new();
    for &p in pts {
        f.offer(p, &mut rep);
    }
    rep.extend(f.flush());
    let n = rep.len();
    (compress_series(SdtConfig { dev: comp, max_ms: None, mode }, ValueKind::Analog, &rep), n)
}

fn errors(raw: &[Point], arch: &[Point]) -> (f64, f64) {
    let s = Series { kind: ValueKind::Analog, points: arch.to_vec() };
    let mut max: f64 = 0.0;
    let mut sq = 0.0;
    for p in raw {
        let e = (query::value_at(&s, p.ts).unwrap_or(f64::NAN) - p.value).abs();
        max = max.max(e);
        sq += e * e;
    }
    (max, (sq / raw.len() as f64).sqrt())
}

/// Tuned setting for one class.
#[derive(Debug, Clone, Serialize)]
pub struct Tuned {
    /// Signal class.
    pub class: SignalClass,
    /// Chosen compression deviation as a fraction of span.
    pub comp_frac: f64,
    /// Absolute compression deviation.
    pub comp_dev: f64,
    /// Absolute exception deviation (half of `comp_dev`).
    pub exc_dev: f64,
    /// Mean RMSE / span on the tuning split at the chosen setting.
    pub tune_rmse_frac: f64,
}

/// One point of the deviation/compression trade-off curve (tuning split).
#[derive(Debug, Clone, Serialize)]
pub struct TradeoffPoint {
    /// Signal class.
    pub class: SignalClass,
    /// Compression deviation as a fraction of span.
    pub comp_frac: f64,
    /// raw / archived points, lookback mode.
    pub lookback_ratio: f64,
    /// raw / archived points, strict mode.
    pub strict_ratio: f64,
    /// Mean RMSE / span, lookback mode.
    pub lookback_rmse_frac: f64,
}

/// Held-out results for one class at its tuned setting.
#[derive(Debug, Clone, Serialize)]
pub struct HeldOut {
    /// Signal class.
    pub class: SignalClass,
    /// Raw samples evaluated.
    pub raw_points: u64,
    /// raw / points after exception filtering alone.
    pub exception_only_ratio: f64,
    /// Archived points, lookback SDT (the default).
    pub lookback_points: u64,
    /// Archived points, strict SDT.
    pub strict_points: u64,
    /// Archived points, classic SDT.
    pub classic_points: u64,
    /// raw / archived, lookback.
    pub point_ratio: f64,
    /// Encoded chunk bytes per archived point (lookback).
    pub bytes_per_archived_point: f64,
    /// 16-byte (i64 + f64) raw samples / encoded archive bytes (lookback).
    pub byte_ratio: f64,
    /// Lossless Gorilla on the raw series: 16-byte raw / encoded bytes.
    pub lossless_gorilla_ratio: f64,
    /// Worst error / guaranteed bound (2·exc + comp), lookback; must be ≤ 1.
    pub lookback_max_err_over_bound: f64,
    /// Worst error / bound, strict; must be ≤ 1.
    pub strict_max_err_over_bound: f64,
    /// Worst error / bound, classic (may exceed 1).
    pub classic_max_err_over_bound: f64,
    /// Held-out series where classic SDT exceeded the bound.
    pub classic_bound_violations: u64,
    /// RMSE / span (lookback).
    pub rmse_frac: f64,
}

fn span(class: SignalClass) -> f64 {
    let (lo, hi) = class.span();
    hi - lo
}

/// Trade-off curve and per-class settings chosen on the tuning split (lookback mode).
pub fn tune(cfg: &BenchConfig) -> (Vec<Tuned>, Vec<TradeoffPoint>) {
    let mut curve = Vec::new();
    let tuned = SignalClass::ALL
        .iter()
        .map(|&class| {
            let sp = span(class);
            let sets: Vec<Vec<Point>> =
                (1..=cfg.tune_seeds).map(|s| series(class, s, START, cfg.points, 1000)).collect();
            let n = sets.len() as f64;
            let raw_total: usize = sets.iter().map(|r| r.len()).sum();
            let mut best = (DEV_GRID[0], f64::NAN);
            for &frac in &DEV_GRID {
                let comp = frac * sp;
                let (mut rmse, mut lookback_pts, mut strict_pts) = (0.0, 0usize, 0usize);
                for raw in &sets {
                    let (arch, _) = reduce(raw, comp / 2.0, comp, SdtMode::Lookback);
                    rmse += errors(raw, &arch).1 / sp / n;
                    lookback_pts += arch.len();
                    strict_pts += reduce(raw, comp / 2.0, comp, SdtMode::Strict).0.len();
                }
                curve.push(TradeoffPoint {
                    class,
                    comp_frac: frac,
                    lookback_ratio: raw_total as f64 / lookback_pts as f64,
                    strict_ratio: raw_total as f64 / strict_pts as f64,
                    lookback_rmse_frac: rmse,
                });
                if rmse <= cfg.rmse_target || best.1.is_nan() {
                    best = (frac, rmse);
                }
            }
            let comp = best.0 * sp;
            Tuned { class, comp_frac: best.0, comp_dev: comp, exc_dev: comp / 2.0, tune_rmse_frac: best.1 }
        })
        .collect();
    (tuned, curve)
}

/// Evaluate tuned settings on the held-out split.
pub fn evaluate(cfg: &BenchConfig, tuned: &[Tuned]) -> Vec<HeldOut> {
    tuned
        .iter()
        .map(|t| {
            let sp = span(t.class);
            let bound = 2.0 * t.exc_dev + t.comp_dev;
            let (mut raw_pts, mut reported, mut lookback, mut strict, mut classic) = (0u64, 0u64, 0u64, 0u64, 0u64);
            let (mut f_err, mut s_err, mut c_err, mut violations) = (0.0f64, 0.0f64, 0.0f64, 0u64);
            let (mut bytes, mut lossless, mut sq) = (0usize, 0usize, 0.0);
            for i in 0..cfg.heldout_seeds {
                let raw = series(t.class, heldout_seed(i), START, cfg.points, 1000);
                raw_pts += raw.len() as u64;
                let (arch, rep) = reduce(&raw, t.exc_dev, t.comp_dev, SdtMode::Lookback);
                reported += rep as u64;
                lookback += arch.len() as u64;
                let (max, rmse) = errors(&raw, &arch);
                f_err = f_err.max(max / bound);
                sq += rmse * rmse;
                bytes += encode_chunk(&arch).len();
                lossless += encode_chunk(&raw).len();
                let (st, _) = reduce(&raw, t.exc_dev, t.comp_dev, SdtMode::Strict);
                strict += st.len() as u64;
                s_err = s_err.max(errors(&raw, &st).0 / bound);
                let (cl, _) = reduce(&raw, t.exc_dev, t.comp_dev, SdtMode::Classic);
                classic += cl.len() as u64;
                let ce = errors(&raw, &cl).0 / bound;
                c_err = c_err.max(ce);
                violations += u64::from(ce > 1.0 + 1e-12);
            }
            HeldOut {
                class: t.class,
                raw_points: raw_pts,
                exception_only_ratio: raw_pts as f64 / reported as f64,
                lookback_points: lookback,
                strict_points: strict,
                classic_points: classic,
                point_ratio: raw_pts as f64 / lookback as f64,
                bytes_per_archived_point: bytes as f64 / lookback as f64,
                byte_ratio: raw_pts as f64 * RAW_BYTES_PER_POINT / bytes as f64,
                lossless_gorilla_ratio: raw_pts as f64 * RAW_BYTES_PER_POINT / lossless as f64,
                lookback_max_err_over_bound: f_err,
                strict_max_err_over_bound: s_err,
                classic_max_err_over_bound: c_err,
                classic_bound_violations: violations,
                rmse_frac: (sq / cfg.heldout_seeds as f64).sqrt() / sp,
            }
        })
        .collect()
}

/// Query benchmark results.
#[derive(Debug, Clone, Serialize)]
pub struct QueryBench {
    /// Tags loaded.
    pub tags: usize,
    /// Raw points generated.
    pub raw_points: u64,
    /// Archived points stored.
    pub archived_points: u64,
    /// Bytes in segment files.
    pub segment_bytes: u64,
    /// Ingest throughput (exception-filtered points/s through `Archive::write`, WAL without fsync).
    pub ingest_points_per_s: f64,
    /// Median fsync'd batch commit latency (ms), 1 000-point batches.
    pub fsync_commit_p50_ms: f64,
    /// Median latency (µs) of a full-range raw query per tag.
    pub raw_p50_us: f64,
    /// Median latency (µs) of a 1-minute interpolated query over the full range.
    pub interpolated_p50_us: f64,
    /// Median latency (µs) of 15-minute aggregates over the full range.
    pub aggregate_p50_us: f64,
    /// 95th-percentile latency (µs) of the aggregate query.
    pub aggregate_p95_us: f64,
    /// Worst |TWA(archive) − TWA(raw)| / span over all 15-minute intervals.
    pub twa_max_err_frac: f64,
    /// Same worst error expressed relative to the guaranteed bound.
    pub twa_max_err_over_bound: f64,
}

/// Everything the benchmark produces.
#[derive(Debug, Clone, Serialize)]
pub struct BenchReport {
    /// Points per series.
    pub points_per_series: usize,
    /// Tuning seeds per class.
    pub tune_seeds: u64,
    /// Held-out seeds per class.
    pub heldout_seeds: u64,
    /// Tuning RMSE target as a fraction of span.
    pub rmse_target: f64,
    /// Tuned settings.
    pub tuned: Vec<Tuned>,
    /// Deviation/compression trade-off on the tuning split.
    pub tradeoff: Vec<TradeoffPoint>,
    /// Held-out compression results.
    pub heldout: Vec<HeldOut>,
    /// Query benchmark.
    pub query: QueryBench,
}

fn percentile(v: &mut [f64], q: f64) -> f64 {
    v.sort_by(|a, b| a.total_cmp(b));
    v[((v.len() - 1) as f64 * q).round() as usize]
}

/// Policy matching the tuned settings, keyed by the tag names used in [`query_bench`].
pub fn tuned_policy(tuned: &[Tuned]) -> Policy {
    Policy {
        defaults: TagSettings::default(),
        rules: tuned
            .iter()
            .map(|t| Rule {
                pattern: format!("bench/{}/*", t.class.name()),
                settings: TagSettings {
                    exc_dev: Some(t.exc_dev),
                    comp_dev: Some(t.comp_dev),
                    exc_max_ms: Some(0),
                    comp_max_ms: Some(0),
                    comp_mode: Some(SdtMode::Lookback),
                },
            })
            .collect(),
    }
}

/// Load held-out series into an archive in `dir` and time queries.
pub fn query_bench(cfg: &BenchConfig, tuned: &[Tuned], dir: &Path) -> Result<QueryBench> {
    let policy = tuned_policy(tuned);
    let opts =
        ArchiveOptions { sync: SyncMode::Os, flush_points: 200_000, policy: policy.clone(), ..Default::default() };
    let mut archive = Archive::open(&dir.join("archive"), opts)?;
    let mut tags = Vec::new();
    let mut raw_by_tag = Vec::new();
    for t in tuned {
        for i in 0..cfg.heldout_seeds {
            let name = format!("bench/{}/s{}", t.class.name(), heldout_seed(i));
            let raw = series(t.class, heldout_seed(i), START, cfg.points, 1000);
            // Exception filtering happens at the collector; apply it here too.
            let mut f = ExceptionFilter::new(policy.resolve(&name).exception, ValueKind::Analog);
            let mut rep = Vec::new();
            for &p in &raw {
                f.offer(p, &mut rep);
            }
            rep.extend(f.flush());
            tags.push((name, t.class, rep));
            raw_by_tag.push(raw);
        }
    }
    let raw_points: u64 = raw_by_tag.iter().map(|r| r.len() as u64).sum();
    let reported: u64 = tags.iter().map(|t| t.2.len() as u64).sum();
    // Interleave tags in 1 000-sample batches like a collector would.
    let t0 = Instant::now();
    let mut idx = vec![0usize; tags.len()];
    loop {
        let mut batch = Vec::with_capacity(1000);
        for (k, (name, _, rep)) in tags.iter().enumerate() {
            let end = (idx[k] + 1000 / tags.len().max(1) + 1).min(rep.len());
            batch.extend(rep[idx[k]..end].iter().map(|p| Sample {
                tag: name.clone(),
                kind: ValueKind::Analog,
                point: *p,
            }));
            idx[k] = end;
        }
        if batch.is_empty() {
            break;
        }
        archive.write(&batch)?;
    }
    archive.flush_snapshots()?;
    let ingest_s = t0.elapsed().as_secs_f64();
    let stats = archive.stats();

    // fsync'd commit latency on a separate small archive.
    let mut fs_archive = Archive::open(
        &dir.join("fsync"),
        ArchiveOptions { sync: SyncMode::Always, policy: policy.clone(), ..Default::default() },
    )?;
    let mut commit = Vec::new();
    let raw0 = &raw_by_tag[0];
    for (b, chunk) in raw0.chunks(1000).take(20).enumerate() {
        let batch: Vec<Sample> =
            chunk.iter().map(|p| Sample { tag: format!("fsync/{b}"), kind: ValueKind::Analog, point: *p }).collect();
        let t = Instant::now();
        fs_archive.write(&batch)?;
        commit.push(t.elapsed().as_secs_f64() * 1e3);
    }

    let (start, end) = (START, START + cfg.points as i64 * 1000);
    let mut raw_lat = Vec::new();
    let mut interp_lat = Vec::new();
    let mut agg_lat = Vec::new();
    let mut twa_err: f64 = 0.0;
    let mut twa_err_bound: f64 = 0.0;
    for ((name, class, _), raw) in tags.iter().zip(&raw_by_tag) {
        let t = Instant::now();
        let s = archive.series(name, start, end)?;
        let n = query::raw(&s, start, end, usize::MAX).len();
        raw_lat.push(t.elapsed().as_secs_f64() * 1e6);
        debug_assert!(n > 0);

        let t = Instant::now();
        let s = archive.series(name, start, end)?;
        query::interpolated(&s, start, end, 60_000)?;
        interp_lat.push(t.elapsed().as_secs_f64() * 1e6);

        let t = Instant::now();
        let s = archive.series(name, start, end)?;
        let rows = query::aggregate(&s, start, end, 900_000)?;
        agg_lat.push(t.elapsed().as_secs_f64() * 1e6);

        let truth = query::aggregate(&Series { kind: ValueKind::Analog, points: raw.clone() }, start, end, 900_000)?;
        let tn = tuned.iter().find(|x| x.class == *class).expect("tuned class");
        let bound = 2.0 * tn.exc_dev + tn.comp_dev;
        for (a, b) in rows.iter().zip(&truth) {
            if let (Some(x), Some(y)) = (a.avg, b.avg) {
                twa_err = twa_err.max((x - y).abs() / span(*class));
                twa_err_bound = twa_err_bound.max((x - y).abs() / bound);
            }
        }
    }
    Ok(QueryBench {
        tags: tags.len(),
        raw_points,
        archived_points: stats.segment_points + stats.memtable_points,
        segment_bytes: stats.segment_bytes,
        ingest_points_per_s: reported as f64 / ingest_s,
        fsync_commit_p50_ms: percentile(&mut commit, 0.5),
        raw_p50_us: percentile(&mut raw_lat, 0.5),
        interpolated_p50_us: percentile(&mut interp_lat, 0.5),
        aggregate_p50_us: percentile(&mut agg_lat, 0.5),
        aggregate_p95_us: percentile(&mut agg_lat, 0.95),
        twa_max_err_frac: twa_err,
        twa_max_err_over_bound: twa_err_bound,
    })
}

/// Run everything; scratch data goes under `dir`.
pub fn run(cfg: &BenchConfig, dir: &Path) -> Result<BenchReport> {
    let (tuned, tradeoff) = tune(cfg);
    let heldout = evaluate(cfg, &tuned);
    let query = query_bench(cfg, &tuned, dir)?;
    Ok(BenchReport {
        points_per_series: cfg.points,
        tune_seeds: cfg.tune_seeds,
        heldout_seeds: cfg.heldout_seeds,
        rmse_target: cfg.rmse_target,
        tuned,
        tradeoff,
        heldout,
        query,
    })
}

/// Render the report as Markdown tables.
pub fn to_markdown(r: &BenchReport) -> String {
    let mut s = String::new();
    s.push_str(&format!(
        "Signals: {} tuning + {} held-out series per class, {} samples each (1 s scan). \
         Exception deviation = comp/2, so the guaranteed bound is 2·exc + comp = 2·comp.\n\n",
        r.tune_seeds, r.heldout_seeds, r.points_per_series
    ));
    s.push_str(
        "**Trade-off on the tuning split** (raw points / archived points; RMSE in % of span, lookback mode)\n\n",
    );
    s.push_str("| class |");
    for f in DEV_GRID {
        s.push_str(&format!(" {:.2}% |", f * 100.0));
    }
    s.push_str("\n|---|");
    s.push_str(&"---|".repeat(DEV_GRID.len()));
    s.push('\n');
    for class in SignalClass::ALL {
        s.push_str(&format!("| {} |", class.name()));
        for p in r.tradeoff.iter().filter(|p| p.class == class) {
            s.push_str(&format!(
                " {:.1}x / {:.1}x strict, RMSE {:.3} |",
                p.lookback_ratio,
                p.strict_ratio,
                p.lookback_rmse_frac * 100.0
            ));
        }
        s.push('\n');
    }
    s.push_str("\n**Held-out results at the tuned deviation** (largest grid value with tuning RMSE ≤ ");
    s.push_str(&format!("{:.2}% of span)\n\n", r.rmse_target * 100.0));
    s.push_str("| class | comp dev (% span) | exception only | lookback SDT | strict SDT | classic SDT | bytes/archived pt | byte ratio (lookback) | lossless Gorilla | max err / bound (lookback, strict, classic) | classic violations | RMSE (% span) |\n");
    s.push_str("|---|---|---|---|---|---|---|---|---|---|---|---|\n");
    for (t, h) in r.tuned.iter().zip(&r.heldout) {
        let ratio = |n: u64| h.raw_points as f64 / n as f64;
        s.push_str(&format!(
            "| {} | {:.2} | {:.2}x | {:.1}x | {:.1}x | {:.1}x | {:.2} | {:.1}x | {:.2}x | {:.3}, {:.3}, {:.3} | {}/{} | {:.4} |\n",
            t.class.name(),
            t.comp_frac * 100.0,
            h.exception_only_ratio,
            ratio(h.lookback_points),
            ratio(h.strict_points),
            ratio(h.classic_points),
            h.bytes_per_archived_point,
            h.byte_ratio,
            h.lossless_gorilla_ratio,
            h.lookback_max_err_over_bound,
            h.strict_max_err_over_bound,
            h.classic_max_err_over_bound,
            h.classic_bound_violations,
            r.heldout_seeds,
            h.rmse_frac * 100.0,
        ));
    }
    let q = &r.query;
    s.push_str(&format!(
        "\n**Query benchmark**: {} held-out tags, {} raw points → {} archived points in {} segment bytes.\n\n",
        q.tags, q.raw_points, q.archived_points, q.segment_bytes
    ));
    s.push_str("| metric | value |\n|---|---|\n");
    s.push_str(&format!(
        "| ingest (exception-filtered points/s into `Archive::write`, WAL without fsync) | {:.0} |\n",
        q.ingest_points_per_s
    ));
    s.push_str(&format!("| fsync'd 1 000-point batch commit, p50 | {:.2} ms |\n", q.fsync_commit_p50_ms));
    s.push_str(&format!("| raw query, full 6 h range, p50 | {:.0} µs |\n", q.raw_p50_us));
    s.push_str(&format!("| interpolated 1-min grid over 6 h, p50 | {:.0} µs |\n", q.interpolated_p50_us));
    s.push_str(&format!(
        "| 15-min TWA/min/max/count over 6 h, p50 / p95 | {:.0} / {:.0} µs |\n",
        q.aggregate_p50_us, q.aggregate_p95_us
    ));
    s.push_str(&format!(
        "| worst 15-min TWA error, archive vs raw | {:.5} % of span ({:.3} of bound) |\n",
        q.twa_max_err_frac * 100.0,
        q.twa_max_err_over_bound
    ));
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quick_bench_produces_sane_numbers() {
        let dir = tempfile::tempdir().unwrap();
        let r = run(&BenchConfig::quick(), dir.path()).unwrap();
        assert_eq!(r.tuned.len(), 4);
        for h in &r.heldout {
            assert!(h.lookback_max_err_over_bound <= 1.0 + 1e-9, "{h:?}");
            assert!(h.strict_max_err_over_bound <= 1.0 + 1e-9, "{h:?}");
            assert!(h.point_ratio > 1.0);
            assert!(h.lookback_points <= h.strict_points);
            assert!(h.byte_ratio > h.point_ratio, "Gorilla encoding should add to point reduction");
        }
        assert_eq!(r.tradeoff.len(), 4 * DEV_GRID.len());
        assert!(r.query.twa_max_err_over_bound <= 1.0 + 1e-9);
        assert!(r.query.archived_points < r.query.raw_points);
        let md = to_markdown(&r);
        assert!(md.contains("| tank_level |"));
        assert_eq!(tuned_policy(&r.tuned).rules.len(), 4);
    }
}
