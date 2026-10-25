//! Exception (deadband) filter, applied at the collector before data is buffered and forwarded.
//!
//! A point is *reported* when it differs from the last reported value by more than `dev`, when
//! `max_ms` has elapsed since the last report, or when quality changes. When a point is reported
//! because of a deviation, the previously received (unreported) point is reported first, so the
//! archive sees where the excursion started — the same rule OSIsoft PI interfaces use.
//!
//! **Guarantee.** For every received point `x`, let `r` be the last reported point at or before
//! `x.ts`. Then `|x.value - r.value| <= dev` (sample-and-hold reconstruction). With *linear*
//! reconstruction between consecutive reported points the bound is `2·dev` — see
//! `docs/ARCHITECTURE.md` for the proof sketch; both bounds are property-tested.

use super::exceeds;
use crate::types::{Point, ValueKind};

/// Exception settings for one tag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ExceptionConfig {
    /// Absolute deviation in engineering units. `0` reports every change.
    pub dev: f64,
    /// Force a report after this many milliseconds without one (`None` = never).
    pub max_ms: Option<i64>,
}

impl Default for ExceptionConfig {
    fn default() -> Self {
        ExceptionConfig { dev: 0.0, max_ms: Some(600_000) }
    }
}

/// Streaming exception filter for one tag.
#[derive(Debug, Clone)]
pub struct ExceptionFilter {
    cfg: ExceptionConfig,
    kind: ValueKind,
    last_reported: Option<Point>,
    /// Most recent received point that has not been reported.
    pending: Option<Point>,
}

impl ExceptionFilter {
    /// New filter. Discrete tags ignore `dev` and report every change of value.
    pub fn new(cfg: ExceptionConfig, kind: ValueKind) -> Self {
        ExceptionFilter { cfg, kind, last_reported: None, pending: None }
    }

    /// Feed one point; reported points are appended to `out` in time order.
    pub fn offer(&mut self, p: Point, out: &mut Vec<Point>) {
        let Some(last) = self.last_reported else {
            self.report(p, out);
            return;
        };
        let dev = match self.kind {
            ValueKind::Analog => self.cfg.dev,
            ValueKind::Discrete => 0.0,
        };
        let quality_change = p.quality != last.quality;
        let timed_out = self.cfg.max_ms.is_some_and(|m| p.ts - last.ts >= m);
        let deviates = p.quality.is_good() && exceeds(p.value, last.value, dev);
        if quality_change || deviates {
            if let Some(prev) = self.pending.take() {
                out.push(prev);
            }
            self.report(p, out);
        } else if timed_out {
            self.pending = None;
            self.report(p, out);
        } else {
            self.pending = Some(p);
        }
    }

    fn report(&mut self, p: Point, out: &mut Vec<Point>) {
        self.pending = None;
        self.last_reported = Some(p);
        out.push(p);
    }

    /// Report the held (unreported) point, e.g. on shutdown, so the archive receives the latest
    /// known value.
    pub fn flush(&mut self) -> Option<Point> {
        let p = self.pending.take()?;
        self.last_reported = Some(p);
        Some(p)
    }

    /// The configuration in force.
    pub fn config(&self) -> ExceptionConfig {
        self.cfg
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Quality;

    fn run(cfg: ExceptionConfig, kind: ValueKind, pts: &[Point]) -> Vec<Point> {
        let mut f = ExceptionFilter::new(cfg, kind);
        let mut out = Vec::new();
        for &p in pts {
            f.offer(p, &mut out);
        }
        out.extend(f.flush());
        out
    }

    #[test]
    fn reports_previous_point_on_excursion() {
        let pts: Vec<Point> = [10.0, 10.2, 10.4, 12.0, 12.1]
            .iter()
            .enumerate()
            .map(|(i, &v)| Point::good(i as i64 * 1000, v))
            .collect();
        let cfg = ExceptionConfig { dev: 0.5, max_ms: None };
        let out = run(cfg, ValueKind::Analog, &pts);
        let ts: Vec<i64> = out.iter().map(|p| p.ts).collect();
        // first, the point before the jump, the jump, then the flushed tail.
        assert_eq!(ts, vec![0, 2000, 3000, 4000]);
    }

    #[test]
    fn max_time_forces_report() {
        let pts: Vec<Point> = (0..10).map(|i| Point::good(i * 1000, 5.0)).collect();
        let cfg = ExceptionConfig { dev: 1.0, max_ms: Some(3000) };
        let mut f = ExceptionFilter::new(cfg, ValueKind::Analog);
        let mut out = Vec::new();
        for p in pts {
            f.offer(p, &mut out);
        }
        let ts: Vec<i64> = out.iter().map(|p| p.ts).collect();
        assert_eq!(ts, vec![0, 3000, 6000, 9000]);
        assert_eq!(f.config().max_ms, Some(3000));
    }

    #[test]
    fn quality_change_and_discrete_changes_always_report() {
        let pts = vec![
            Point::good(0, 1.0),
            Point::good(1000, 1.0),
            Point::with_quality(2000, 1.0, Quality::Stale),
            Point::good(3000, 1.0),
            Point::good(4000, 0.0),
        ];
        let out = run(ExceptionConfig { dev: 5.0, max_ms: None }, ValueKind::Discrete, &pts);
        let ts: Vec<i64> = out.iter().map(|p| p.ts).collect();
        assert_eq!(ts, vec![0, 1000, 2000, 3000, 4000]);
    }

    #[test]
    fn flush_on_empty_is_none() {
        let mut f = ExceptionFilter::new(ExceptionConfig::default(), ValueKind::Analog);
        assert!(f.flush().is_none());
    }
}
