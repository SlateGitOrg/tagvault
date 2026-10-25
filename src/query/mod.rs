//! Query engine over a [`Series`] of archived points.
//!
//! **Signal model.** Stored points `p0 … pn` define the reconstructed signal:
//!
//! * between a good point `p_i` and the next point `p_{i+1}`: linear interpolation for analog
//!   tags when `p_{i+1}` is also good, otherwise the value of `p_i` is held (step);
//! * from a Bad/Stale point until the next point: undefined;
//! * after the last point (the snapshot): undefined — tagvault never extrapolates.
//!
//! This is exactly the reconstruction the swinging-door bound is stated for, so interpolated
//! values are within `comp_dev` of every point the archive received.
//!
//! **Aggregates** per interval `[a, b)`: time-weighted average (exact integral of the
//! reconstruction divided by the covered time), min and max of the reconstruction, the count of
//! stored good points, and the covered fraction of the interval.
//!
//! Complexity: `interpolated` is O(n + k) for n points and k output rows (a single merge pass);
//! `aggregate` is O(n + k) as well, each segment being visited once per interval it overlaps.

use crate::error::{Error, Result};
use crate::storage::Series;
use crate::types::{Point, Timestamp, ValueKind};
use serde::Serialize;

/// Upper bound on rows produced by one interpolated/aggregate query.
pub const MAX_ROWS: u64 = 100_000;

/// One interpolated sample.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct InterpRow {
    /// Sample time.
    pub ts: Timestamp,
    /// Reconstructed value, or `None` where the signal is undefined.
    pub value: Option<f64>,
}

/// Aggregates for one interval.
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct AggRow {
    /// Interval start (inclusive).
    pub start: Timestamp,
    /// Interval end (exclusive).
    pub end: Timestamp,
    /// Time-weighted average over the covered part of the interval.
    pub avg: Option<f64>,
    /// Minimum of the reconstructed signal.
    pub min: Option<f64>,
    /// Maximum of the reconstructed signal.
    pub max: Option<f64>,
    /// Stored good points inside the interval.
    pub count: u64,
    /// Fraction of the interval where the signal is defined (0–1).
    pub coverage: f64,
}

fn defined(p: &Point) -> bool {
    p.quality.is_good() && p.value.is_finite()
}

/// Value of segment `[a, b)` at time `t` (requires `defined(a)`).
fn seg_value(kind: ValueKind, a: &Point, b: &Point, t: Timestamp) -> f64 {
    if kind == ValueKind::Discrete || !defined(b) || t <= a.ts {
        return a.value;
    }
    a.value + (b.value - a.value) * ((t - a.ts) as f64 / (b.ts - a.ts) as f64)
}

/// Stored points with `start <= ts <= end`, at most `limit`.
pub fn raw(series: &Series, start: Timestamp, end: Timestamp, limit: usize) -> Vec<Point> {
    series
        .points
        .iter()
        .filter(|p| p.ts >= start && p.ts <= end)
        .take(limit)
        .copied()
        .collect()
}

/// Reconstructed value at `t`.
pub fn value_at(series: &Series, t: Timestamp) -> Option<f64> {
    let pts = &series.points;
    let i = pts.partition_point(|p| p.ts <= t).checked_sub(1)?;
    let a = &pts[i];
    if !defined(a) {
        return None;
    }
    if a.ts == t {
        return Some(a.value);
    }
    pts.get(i + 1).map(|b| seg_value(series.kind, a, b, t))
}

fn check_grid(start: Timestamp, end: Timestamp, step: i64) -> Result<u64> {
    if step <= 0 {
        return Err(Error::InvalidInput("step/interval must be positive".into()));
    }
    if end < start {
        return Err(Error::InvalidInput("end must not be before start".into()));
    }
    let rows = ((end - start) / step) as u64 + 1;
    if rows > MAX_ROWS {
        return Err(Error::InvalidInput(format!(
            "query would return {rows} rows (max {MAX_ROWS})"
        )));
    }
    Ok(rows)
}

/// Values at `start, start+step, … <= end`.
pub fn interpolated(
    series: &Series,
    start: Timestamp,
    end: Timestamp,
    step: i64,
) -> Result<Vec<InterpRow>> {
    let rows = check_grid(start, end, step)?;
    let pts = &series.points;
    let mut out = Vec::with_capacity(rows as usize);
    let mut i = 0usize; // number of points with ts <= t
    let mut t = start;
    while t <= end {
        while i < pts.len() && pts[i].ts <= t {
            i += 1;
        }
        let value = match i.checked_sub(1) {
            None => None,
            Some(j) => {
                let a = &pts[j];
                if !defined(a) {
                    None
                } else if a.ts == t {
                    Some(a.value)
                } else {
                    pts.get(j + 1).map(|b| seg_value(series.kind, a, b, t))
                }
            }
        };
        out.push(InterpRow { ts: t, value });
        t += step;
    }
    Ok(out)
}

/// Aggregates over consecutive intervals `[start + k·interval, start + (k+1)·interval)` covering
/// `[start, end)`.
pub fn aggregate(
    series: &Series,
    start: Timestamp,
    end: Timestamp,
    interval: i64,
) -> Result<Vec<AggRow>> {
    if end <= start {
        return Err(Error::InvalidInput("end must be after start".into()));
    }
    check_grid(start, end - 1, interval)?;
    let pts = &series.points;
    let mut out = Vec::new();
    let mut a = start;
    // First point that could start a segment overlapping the current interval.
    let mut seg = 0usize;
    while a < end {
        let b = (a + interval).min(end);
        while seg + 1 < pts.len() && pts[seg + 1].ts <= a {
            seg += 1;
        }
        let mut area = 0.0;
        let mut covered = 0i64;
        let mut lo = f64::INFINITY;
        let mut hi = f64::NEG_INFINITY;
        let mut count = 0u64;
        let mut j = seg;
        while j < pts.len() && pts[j].ts < b {
            let p = &pts[j];
            if p.ts >= a && defined(p) {
                count += 1;
                lo = lo.min(p.value);
                hi = hi.max(p.value);
            }
            if let (true, Some(q)) = (defined(p), pts.get(j + 1)) {
                let x0 = p.ts.max(a);
                let x1 = q.ts.min(b);
                if x1 > x0 {
                    let v0 = seg_value(series.kind, p, q, x0);
                    // The value just before x1 (a step segment never takes q's value inside it).
                    let v1 = if series.kind == ValueKind::Discrete || !defined(q) {
                        p.value
                    } else {
                        seg_value(series.kind, p, q, x1)
                    };
                    area += (v0 + v1) / 2.0 * (x1 - x0) as f64;
                    covered += x1 - x0;
                    lo = lo.min(v0.min(v1));
                    hi = hi.max(v0.max(v1));
                }
            }
            j += 1;
        }
        let has = lo <= hi;
        out.push(AggRow {
            start: a,
            end: b,
            avg: (covered > 0)
                .then(|| area / covered as f64)
                .or(if has && count > 0 { Some(lo) } else { None }),
            min: has.then_some(lo),
            max: has.then_some(hi),
            count,
            coverage: covered as f64 / (b - a) as f64,
        });
        a = b;
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Quality;

    fn series(kind: ValueKind, pts: &[(i64, f64)]) -> Series {
        Series {
            kind,
            points: pts.iter().map(|&(t, v)| Point::good(t, v)).collect(),
        }
    }

    #[test]
    fn interpolation_linear_step_and_edges() {
        let s = series(ValueKind::Analog, &[(0, 0.0), (10, 10.0), (20, 0.0)]);
        assert_eq!(value_at(&s, 5), Some(5.0));
        assert_eq!(value_at(&s, 15), Some(5.0));
        assert_eq!(value_at(&s, 20), Some(0.0));
        assert_eq!(value_at(&s, 21), None, "no extrapolation past the snapshot");
        assert_eq!(value_at(&s, -1), None);
        let d = series(ValueKind::Discrete, &[(0, 0.0), (10, 1.0)]);
        assert_eq!(value_at(&d, 9), Some(0.0));
        let rows = interpolated(&s, -5, 25, 5).unwrap();
        let vals: Vec<Option<f64>> = rows.iter().map(|r| r.value).collect();
        assert_eq!(
            vals,
            vec![
                None,
                Some(0.0),
                Some(5.0),
                Some(10.0),
                Some(5.0),
                Some(0.0),
                None
            ]
        );
        assert!(interpolated(&s, 0, 10, 0).is_err());
        assert!(interpolated(&s, 10, 0, 1).is_err());
        assert!(interpolated(&s, 0, i64::MAX / 2, 1).is_err());
    }

    #[test]
    fn bad_points_break_the_signal() {
        let s = Series {
            kind: ValueKind::Analog,
            points: vec![
                Point::good(0, 1.0),
                Point::with_quality(10, f64::NAN, Quality::Bad),
                Point::good(20, 3.0),
                Point::good(30, 3.0),
            ],
        };
        assert_eq!(value_at(&s, 5), Some(1.0), "held up to a bad point");
        assert_eq!(value_at(&s, 15), None);
        assert_eq!(value_at(&s, 25), Some(3.0));
        let agg = aggregate(&s, 0, 30, 30).unwrap();
        assert_eq!(agg[0].coverage, 20.0 / 30.0);
        assert_eq!(agg[0].avg, Some(2.0));
        assert_eq!(agg[0].count, 2);
    }

    #[test]
    fn time_weighted_average_of_a_ramp() {
        let s = series(ValueKind::Analog, &[(0, 0.0), (100, 100.0), (200, 100.0)]);
        let agg = aggregate(&s, 0, 200, 50).unwrap();
        let avgs: Vec<f64> = agg.iter().map(|r| r.avg.unwrap()).collect();
        assert_eq!(avgs, vec![25.0, 75.0, 100.0, 100.0]);
        assert_eq!(agg[0].min, Some(0.0));
        assert_eq!(agg[0].max, Some(50.0));
        assert_eq!(agg[1].count, 0);
        assert_eq!(agg[2].count, 1);
        assert!(agg.iter().all(|r| r.coverage == 1.0));
        // Uneven final interval is clipped to `end`.
        let agg = aggregate(&s, 0, 120, 100).unwrap();
        assert_eq!((agg[1].start, agg[1].end), (100, 120));
        assert!(aggregate(&s, 10, 10, 5).is_err());
    }

    #[test]
    fn discrete_average_is_duty_cycle_and_isolated_point_counts() {
        let d = series(
            ValueKind::Discrete,
            &[(0, 0.0), (30, 1.0), (40, 0.0), (100, 0.0)],
        );
        let agg = aggregate(&d, 0, 100, 100).unwrap();
        assert!((agg[0].avg.unwrap() - 0.1).abs() < 1e-12);
        assert_eq!((agg[0].min, agg[0].max), (Some(0.0), Some(1.0)));
        // A single point (no following segment) still yields min/max/count but zero coverage.
        let one = series(ValueKind::Analog, &[(50, 7.0)]);
        let agg = aggregate(&one, 0, 100, 100).unwrap();
        assert_eq!(
            (agg[0].avg, agg[0].min, agg[0].count, agg[0].coverage),
            (Some(7.0), Some(7.0), 1, 0.0)
        );
        let empty = series(ValueKind::Analog, &[]);
        let agg = aggregate(&empty, 0, 10, 5).unwrap();
        assert!(agg.iter().all(|r| r.avg.is_none() && r.min.is_none()));
    }

    #[test]
    fn raw_filters_and_limits() {
        let s = series(
            ValueKind::Analog,
            &[(0, 0.0), (10, 1.0), (20, 2.0), (30, 3.0)],
        );
        assert_eq!(raw(&s, 5, 30, 10).len(), 3);
        assert_eq!(raw(&s, 0, 30, 2).len(), 2);
    }
}
