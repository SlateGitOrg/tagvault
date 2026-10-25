//! Swinging-door trending (SDT) compression.
//!
//! The archive keeps an *anchor* `A` (the last archived point), a *held* point `H` (the latest
//! received point, the "snapshot"), and a slope window `W = [lo, hi]`. For any point `Q` after
//! `A`, the set of slopes from `A` whose line passes within `dev` of `Q` is
//!
//! ```text
//! win(Q) = [ (Q.v - dev - A.v) / (Q.t - A.t),  (Q.v + dev - A.v) / (Q.t - A.t) ]
//! ```
//!
//! Geometrically these are the two "doors" hinged at `A ± dev`; each new point can only swing
//! them closed.
//!
//! **Strict mode (default).** `W` is the intersection of `win(Q)` over the *intermediate* points
//! strictly between `A` and `H`. A new point `P` replaces `H` only if the straight line `A → P`
//! has a slope inside `W ∩ win(H)`; otherwise `H` is archived and becomes the new anchor. The
//! invariant `slope(A → H) ∈ W` therefore holds at every step, which means the archived segment
//! `A → H` passes within `dev` of every received point — the reconstruction error bound holds
//! *by construction*, and archived values are original measurements (never synthesized).
//!
//! **Classic mode** is the textbook test: keep `H` while the doors have not crossed
//! (`W ∩ win(P) ≠ ∅`). It compresses slightly more, but archiving the *measured* value of `H`
//! can violate the bound at intermediate points (see `classic_mode_can_exceed_deviation`); it is
//! kept only so benchmarks can quantify the trade.
//!
//! Discrete tags bypass the doors: a point is archived when its value or quality changes, which
//! is exact under step reconstruction.

use super::exceeds;
use crate::error::{Error, Result};
use crate::types::{Point, Quality, ValueKind};

/// Which door test to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SdtMode {
    /// Bounded error at every received point (default).
    #[default]
    Strict,
    /// Textbook doors-crossed test; for comparison only.
    Classic,
}

/// Compression settings for one tag.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SdtConfig {
    /// Maximum reconstruction error in engineering units.
    pub dev: f64,
    /// Archive the held point when the anchor is older than this (`None` = never).
    pub max_ms: Option<i64>,
    /// Door test.
    pub mode: SdtMode,
}

impl Default for SdtConfig {
    fn default() -> Self {
        SdtConfig { dev: 0.0, max_ms: Some(3_600_000), mode: SdtMode::Strict }
    }
}

/// Streaming SDT compressor for one tag. Serializable so the WAL checkpoint can persist it.
#[derive(Debug, Clone, PartialEq)]
pub struct Sdt {
    cfg: SdtConfig,
    kind: ValueKind,
    anchor: Option<Point>,
    held: Option<Point>,
    lo: f64,
    hi: f64,
}

fn window(anchor: Point, q: Point, dev: f64) -> (f64, f64) {
    let dt = (q.ts - anchor.ts) as f64;
    ((q.value - dev - anchor.value) / dt, (q.value + dev - anchor.value) / dt)
}

impl Sdt {
    /// New compressor.
    pub fn new(cfg: SdtConfig, kind: ValueKind) -> Self {
        Sdt { cfg, kind, anchor: None, held: None, lo: f64::NEG_INFINITY, hi: f64::INFINITY }
    }

    /// Settings in force.
    pub fn config(&self) -> SdtConfig {
        self.cfg
    }

    /// Replace the settings (takes effect for subsequent points).
    pub fn set_config(&mut self, cfg: SdtConfig) {
        self.cfg = cfg;
    }

    /// Value kind of the tag.
    pub fn kind(&self) -> ValueKind {
        self.kind
    }

    /// The held, not-yet-archived latest point (PI calls this the snapshot).
    pub fn snapshot(&self) -> Option<Point> {
        self.held
    }

    /// The last archived point.
    pub fn anchor(&self) -> Option<Point> {
        self.anchor
    }

    fn reset_window(&mut self) {
        self.lo = f64::NEG_INFINITY;
        self.hi = f64::INFINITY;
    }

    fn archive(&mut self, p: Point, out: &mut Vec<Point>) {
        out.push(p);
        self.anchor = Some(p);
        self.reset_window();
    }

    /// Archive the held point (if any).
    fn archive_held(&mut self, out: &mut Vec<Point>) {
        if let Some(h) = self.held.take() {
            self.archive(h, out);
        }
    }

    /// Feed one point (timestamps must be strictly increasing; the archive enforces this).
    /// Points that become archived are appended to `out` in time order.
    pub fn offer(&mut self, p: Point, out: &mut Vec<Point>) {
        let Some(anchor) = self.anchor else {
            self.archive(p, out);
            return;
        };
        // Non-good data: archive only quality transitions; a run of bad values is one point.
        if !p.quality.is_good() {
            self.archive_held(out);
            let last = self.anchor.unwrap_or(anchor);
            if last.quality != p.quality {
                self.archive(p, out);
            }
            return;
        }
        if !anchor.quality.is_good() || !p.value.is_finite() {
            self.archive_held(out);
            self.archive(p, out);
            return;
        }
        if self.kind == ValueKind::Discrete {
            let changed = exceeds(p.value, anchor.value, 0.0);
            let timed_out = self.cfg.max_ms.is_some_and(|m| p.ts - anchor.ts > m);
            if changed || timed_out {
                // Under step reconstruction the held point carries no extra information.
                self.held = None;
                self.archive(p, out);
            } else {
                self.held = Some(p);
            }
            return;
        }
        let Some(held) = self.held else {
            self.held = Some(p);
            return;
        };
        if self.cfg.max_ms.is_some_and(|m| p.ts - anchor.ts > m) || !held.value.is_finite() {
            self.archive_held(out);
            self.held = Some(p);
            return;
        }
        let dev = self.cfg.dev;
        let (hl, hh) = window(anchor, held, dev);
        let accept = match self.cfg.mode {
            SdtMode::Strict => {
                let lo = self.lo.max(hl);
                let hi = self.hi.min(hh);
                let s = (p.value - anchor.value) / (p.ts - anchor.ts) as f64;
                if lo <= s && s <= hi {
                    self.lo = lo;
                    self.hi = hi;
                    true
                } else {
                    false
                }
            }
            SdtMode::Classic => {
                // In classic mode the window already includes win(H); add win(P).
                let (pl, ph) = window(anchor, p, dev);
                let lo = self.lo.max(hl).max(pl);
                let hi = self.hi.min(hh).min(ph);
                if lo <= hi {
                    self.lo = lo;
                    self.hi = hi;
                    true
                } else {
                    false
                }
            }
        };
        if accept {
            self.held = Some(p);
        } else {
            self.archive_held(out);
            self.held = Some(p);
        }
    }

    /// Archive the held point, e.g. at shutdown or before an export.
    pub fn flush(&mut self) -> Option<Point> {
        let mut out = Vec::with_capacity(1);
        self.archive_held(&mut out);
        out.pop()
    }

    /// Serialize the full state (config + doors) for a WAL checkpoint.
    pub fn encode(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.cfg.dev.to_le_bytes());
        out.extend_from_slice(&self.cfg.max_ms.unwrap_or(-1).to_le_bytes());
        out.push(match self.cfg.mode {
            SdtMode::Strict => 0,
            SdtMode::Classic => 1,
        });
        out.push(self.kind.code());
        for p in [self.anchor, self.held] {
            match p {
                None => out.push(0),
                Some(p) => {
                    out.push(1);
                    out.extend_from_slice(&p.ts.to_le_bytes());
                    out.extend_from_slice(&p.value.to_le_bytes());
                    out.push(p.quality.code());
                }
            }
        }
        out.extend_from_slice(&self.lo.to_le_bytes());
        out.extend_from_slice(&self.hi.to_le_bytes());
    }

    /// Inverse of [`Sdt::encode`].
    pub fn decode(c: &mut crate::encoding::varint::Cursor<'_>) -> Result<Sdt> {
        let dev = c.f64()?;
        let max = c.i64()?;
        let mode = match c.u8()? {
            0 => SdtMode::Strict,
            1 => SdtMode::Classic,
            m => return Err(Error::Corrupt(format!("bad sdt mode {m}"))),
        };
        let kind = ValueKind::from_code(c.u8()?).ok_or_else(|| Error::Corrupt("bad value kind".into()))?;
        let mut pts = [None, None];
        for slot in pts.iter_mut() {
            if c.u8()? == 1 {
                let ts = c.i64()?;
                let value = c.f64()?;
                let quality = Quality::from_code(c.u8()?).ok_or_else(|| Error::Corrupt("bad quality".into()))?;
                *slot = Some(Point { ts, value, quality });
            }
        }
        let lo = c.f64()?;
        let hi = c.f64()?;
        Ok(Sdt {
            cfg: SdtConfig { dev, max_ms: (max >= 0).then_some(max), mode },
            kind,
            anchor: pts[0],
            held: pts[1],
            lo,
            hi,
        })
    }
}

/// Compress a whole series in one call (used by benchmarks and tests).
pub fn compress_series(cfg: SdtConfig, kind: ValueKind, pts: &[Point]) -> Vec<Point> {
    let mut sdt = Sdt::new(cfg, kind);
    let mut out = Vec::new();
    for &p in pts {
        sdt.offer(p, &mut out);
    }
    out.extend(sdt.flush());
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encoding::varint::Cursor;

    fn strict(dev: f64) -> SdtConfig {
        SdtConfig { dev, max_ms: None, mode: SdtMode::Strict }
    }

    fn lerp(a: Point, b: Point, t: i64) -> f64 {
        a.value + (b.value - a.value) * (t - a.ts) as f64 / (b.ts - a.ts) as f64
    }

    fn max_error(orig: &[Point], arch: &[Point]) -> f64 {
        let mut worst: f64 = 0.0;
        for q in orig {
            let i = arch.partition_point(|a| a.ts <= q.ts);
            let v = if arch[i - 1].ts == q.ts { arch[i - 1].value } else { lerp(arch[i - 1], arch[i], q.ts) };
            worst = worst.max((v - q.value).abs());
        }
        worst
    }

    #[test]
    fn straight_line_compresses_to_endpoints() {
        let pts: Vec<Point> = (0..100).map(|i| Point::good(i * 1000, 2.0 * i as f64)).collect();
        let arch = compress_series(strict(0.01), ValueKind::Analog, &pts);
        assert_eq!(arch.len(), 2);
        assert_eq!(arch[0].ts, 0);
        assert_eq!(arch[1].ts, 99_000);
    }

    #[test]
    fn classic_mode_can_exceed_deviation() {
        // A=(0,0), Q=(1,0), H=(10,10.5): the doors (dev=1) have not crossed, so classic SDT
        // keeps H; but the line A→H passes 1.05 away from Q.
        let pts = vec![Point::good(0, 0.0), Point::good(1, 0.0), Point::good(10, 10.5), Point::good(11, 30.0)];
        let classic = compress_series(SdtConfig { dev: 1.0, max_ms: None, mode: SdtMode::Classic }, ValueKind::Analog, &pts);
        assert!(max_error(&pts, &classic) > 1.0);
        let strict_arch = compress_series(strict(1.0), ValueKind::Analog, &pts);
        assert!(max_error(&pts, &strict_arch) <= 1.0);
        assert!(strict_arch.len() > classic.len());
    }

    #[test]
    fn noisy_ramp_respects_bound_and_compresses() {
        let mut seed = 7u64;
        let pts: Vec<Point> = (0..2000)
            .map(|i| {
                seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                let noise = ((seed >> 33) as f64 / (1u64 << 31) as f64 - 0.5) * 0.2;
                Point::good(i * 1000, (i as f64 * 0.01).sin() * 10.0 + noise)
            })
            .collect();
        let arch = compress_series(strict(0.25), ValueKind::Analog, &pts);
        assert!(max_error(&pts, &arch) <= 0.25 + 1e-9);
        assert!(arch.len() * 5 < pts.len(), "ratio too low: {}", arch.len());
    }

    #[test]
    fn bad_quality_runs_collapse_and_resume() {
        let pts = vec![
            Point::good(0, 1.0),
            Point::good(1000, 1.1),
            Point::with_quality(2000, f64::NAN, Quality::Bad),
            Point::with_quality(3000, f64::NAN, Quality::Bad),
            Point::with_quality(4000, 0.0, Quality::Stale),
            Point::good(5000, 2.0),
            Point::good(6000, 2.0),
        ];
        let arch = compress_series(strict(0.5), ValueKind::Analog, &pts);
        let ts: Vec<i64> = arch.iter().map(|p| p.ts).collect();
        assert_eq!(ts, vec![0, 1000, 2000, 4000, 5000, 6000]);
    }

    #[test]
    fn non_finite_good_values_are_archived_verbatim() {
        let pts = vec![Point::good(0, 1.0), Point::good(1000, f64::INFINITY), Point::good(2000, 1.0), Point::good(3000, 1.0)];
        let arch = compress_series(strict(0.5), ValueKind::Analog, &pts);
        assert!(arch.iter().any(|p| p.value.is_infinite()));
        assert_eq!(arch.last().unwrap().ts, 3000);
    }

    #[test]
    fn discrete_archives_changes_only() {
        let vals = [0.0, 0.0, 1.0, 1.0, 1.0, 0.0];
        let pts: Vec<Point> = vals.iter().enumerate().map(|(i, &v)| Point::good(i as i64, v)).collect();
        let arch = compress_series(strict(10.0), ValueKind::Discrete, &pts);
        let ts: Vec<i64> = arch.iter().map(|p| p.ts).collect();
        // 0, change at 2, change at 5 (the flush has nothing left to add).
        assert_eq!(ts, vec![0, 2, 5]);
    }

    #[test]
    fn max_time_forces_archive() {
        let pts: Vec<Point> = (0..20).map(|i| Point::good(i * 1000, 3.0)).collect();
        let arch = compress_series(SdtConfig { dev: 1.0, max_ms: Some(5000), mode: SdtMode::Strict }, ValueKind::Analog, &pts);
        for w in arch.windows(2) {
            assert!(w[1].ts - w[0].ts <= 6000);
        }
        assert!(arch.len() >= 4);
    }

    #[test]
    fn state_roundtrips_and_continues_identically() {
        let pts: Vec<Point> = (0..500).map(|i| Point::good(i * 1000, ((i * i) % 97) as f64 / 10.0)).collect();
        let cfg = SdtConfig { dev: 0.3, max_ms: Some(60_000), mode: SdtMode::Strict };
        let mut a = Sdt::new(cfg, ValueKind::Analog);
        let mut out_a = Vec::new();
        for &p in &pts[..250] {
            a.offer(p, &mut out_a);
        }
        let mut buf = Vec::new();
        a.encode(&mut buf);
        let mut b = Sdt::decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(a, b);
        let mut out_b = out_a.clone();
        for &p in &pts[250..] {
            a.offer(p, &mut out_a);
            b.offer(p, &mut out_b);
        }
        assert_eq!(out_a, out_b);
        assert_eq!(a.snapshot(), b.snapshot());
        assert!(Sdt::decode(&mut Cursor::new(&buf[..10])).is_err());
    }

    #[test]
    fn accessors_and_config_updates() {
        let mut s = Sdt::new(strict(1.0), ValueKind::Analog);
        assert!(s.anchor().is_none());
        assert_eq!(s.kind(), ValueKind::Analog);
        s.set_config(strict(2.0));
        assert_eq!(s.config().dev, 2.0);
        let mut out = Vec::new();
        s.offer(Point::good(0, 1.0), &mut out);
        s.offer(Point::good(1, 1.5), &mut out);
        assert_eq!(s.anchor().unwrap().ts, 0);
        assert_eq!(s.snapshot().unwrap().ts, 1);
        assert_eq!(s.flush().unwrap().ts, 1);
        assert!(s.flush().is_none());
    }
}
