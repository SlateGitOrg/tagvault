//! Benign process-signal models: tank level with a hysteresis pump, first-order loops with
//! setpoint steps (temperature, flow with trips), and a mean-reverting pressure. All are driven
//! by a seeded [`Rng`], so every series is reproducible.

use super::rng::Rng;
use crate::types::{Point, Timestamp};
use serde::Serialize;

/// Classes of generated signal (used to pick models and to tune per-class deviations).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, PartialOrd, Ord)]
pub enum SignalClass {
    /// Tank level in %, pump cycling between 30 % and 80 %.
    TankLevel,
    /// Reactor temperature in °C, slow first-order response to setpoint steps.
    Temperature,
    /// Flow in m³/h, fast response, noisy, occasional pump trips to zero.
    Flow,
    /// Header pressure in bar, mean-reverting random walk.
    Pressure,
}

impl SignalClass {
    /// All classes.
    pub const ALL: [SignalClass; 4] = [SignalClass::TankLevel, SignalClass::Temperature, SignalClass::Flow, SignalClass::Pressure];

    /// Engineering range `(low, high)`.
    pub fn span(self) -> (f64, f64) {
        match self {
            SignalClass::TankLevel => (0.0, 100.0),
            SignalClass::Temperature => (0.0, 150.0),
            SignalClass::Flow => (0.0, 80.0),
            SignalClass::Pressure => (0.0, 10.0),
        }
    }

    /// Lower-case name.
    pub fn name(self) -> &'static str {
        match self {
            SignalClass::TankLevel => "tank_level",
            SignalClass::Temperature => "temperature",
            SignalClass::Flow => "flow",
            SignalClass::Pressure => "pressure",
        }
    }
}

/// Buffer tank: pump fills at a fixed rate, demand drains at a slowly wandering rate.
#[derive(Debug, Clone)]
pub struct Tank {
    rng: Rng,
    level: f64,
    pump_on: bool,
    demand: f64,
    capacity_m3: f64,
    pump_rate: f64,
}

/// One tank sample.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TankReading {
    /// Measured level, %.
    pub level: f64,
    /// Measured inlet flow, m³/h.
    pub inflow: f64,
    /// Pump state.
    pub pump_on: bool,
}

impl Tank {
    /// New tank with a seeded initial state.
    pub fn new(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let level = rng.range(35.0, 75.0);
        let pump_on = rng.chance(0.5);
        let demand = rng.range(15.0, 28.0);
        Tank { rng, level, pump_on, demand, capacity_m3: 20.0, pump_rate: 42.0 }
    }

    /// Advance `dt_s` seconds.
    pub fn step(&mut self, dt_s: f64) -> TankReading {
        self.demand = (self.demand + self.rng.normal() * 0.02 * dt_s.sqrt()).clamp(12.0, 30.0);
        let inflow = if self.pump_on { self.pump_rate } else { 0.0 };
        self.level += (inflow - self.demand) / 3600.0 * dt_s / self.capacity_m3 * 100.0;
        self.level = self.level.clamp(0.0, 100.0);
        if self.level < 30.0 {
            self.pump_on = true;
        } else if self.level > 80.0 {
            self.pump_on = false;
        }
        TankReading {
            level: self.level + self.rng.normal() * 0.05,
            inflow: if self.pump_on { (inflow + self.rng.normal() * 0.3).max(0.0) } else { 0.0 },
            pump_on: self.pump_on,
        }
    }
}

/// First-order lag tracking a setpoint that steps at random intervals.
#[derive(Debug, Clone)]
pub struct FirstOrder {
    rng: Rng,
    pv: f64,
    sp: f64,
    tau_s: f64,
    noise: f64,
    sp_range: (f64, f64),
    hold_s: (f64, f64),
    until_step: f64,
    trip_rate_per_s: f64,
    trip_left: f64,
}

impl FirstOrder {
    /// Temperature loop: τ = 240 s, setpoint 60–90 °C every 15–45 min, σ = 0.08 °C.
    pub fn temperature(seed: u64) -> Self {
        Self::build(seed, 240.0, 0.08, (60.0, 90.0), (900.0, 2700.0), 0.0)
    }

    /// Flow loop: τ = 8 s, setpoint 20–60 m³/h every 5–20 min, σ = 0.4, pump trips ~1 per 2 h.
    pub fn flow(seed: u64) -> Self {
        Self::build(seed, 8.0, 0.4, (20.0, 60.0), (300.0, 1200.0), 1.0 / 7200.0)
    }

    fn build(seed: u64, tau_s: f64, noise: f64, sp_range: (f64, f64), hold_s: (f64, f64), trip: f64) -> Self {
        let mut rng = Rng::new(seed);
        let sp = rng.range(sp_range.0, sp_range.1);
        let until_step = rng.range(hold_s.0, hold_s.1);
        FirstOrder { rng, pv: sp, sp, tau_s, noise, sp_range, hold_s, until_step, trip_rate_per_s: trip, trip_left: 0.0 }
    }

    /// Advance `dt_s` seconds and return the measurement.
    pub fn step(&mut self, dt_s: f64) -> f64 {
        self.until_step -= dt_s;
        if self.until_step <= 0.0 {
            self.sp = self.rng.range(self.sp_range.0, self.sp_range.1);
            self.until_step = self.rng.range(self.hold_s.0, self.hold_s.1);
        }
        if self.trip_left > 0.0 {
            self.trip_left -= dt_s;
        } else if self.trip_rate_per_s > 0.0 && self.rng.chance(self.trip_rate_per_s * dt_s) {
            self.trip_left = self.rng.range(60.0, 180.0);
        }
        let target = if self.trip_left > 0.0 { 0.0 } else { self.sp };
        let a = 1.0 - (-dt_s / self.tau_s).exp();
        self.pv += (target - self.pv) * a;
        let meas = self.pv + self.rng.normal() * self.noise;
        if self.trip_left > 0.0 {
            meas.max(0.0)
        } else {
            meas
        }
    }
}

/// Ornstein–Uhlenbeck process with measurement noise (header pressure).
#[derive(Debug, Clone)]
pub struct MeanReverting {
    rng: Rng,
    x: f64,
    mu: f64,
    theta: f64,
    sigma: f64,
    noise: f64,
}

impl MeanReverting {
    /// Pressure around 5 bar.
    pub fn pressure(seed: u64) -> Self {
        let mut rng = Rng::new(seed);
        let x = 5.0 + rng.normal() * 0.2;
        MeanReverting { rng, x, mu: 5.0, theta: 0.005, sigma: 0.01, noise: 0.005 }
    }

    /// Advance `dt_s` seconds and return the measurement.
    pub fn step(&mut self, dt_s: f64) -> f64 {
        self.x += self.theta * (self.mu - self.x) * dt_s + self.sigma * dt_s.sqrt() * self.rng.normal();
        self.x + self.rng.normal() * self.noise
    }
}

/// A single-output signal of any class.
#[derive(Debug, Clone)]
pub enum Signal {
    /// Tank level.
    Tank(Tank),
    /// Temperature or flow loop.
    Loop(FirstOrder),
    /// Pressure.
    Ou(MeanReverting),
}

impl Signal {
    /// Generator for `class` with `seed`.
    pub fn new(class: SignalClass, seed: u64) -> Signal {
        match class {
            SignalClass::TankLevel => Signal::Tank(Tank::new(seed)),
            SignalClass::Temperature => Signal::Loop(FirstOrder::temperature(seed)),
            SignalClass::Flow => Signal::Loop(FirstOrder::flow(seed)),
            SignalClass::Pressure => Signal::Ou(MeanReverting::pressure(seed)),
        }
    }

    /// Next measurement after `dt_s` seconds.
    pub fn step(&mut self, dt_s: f64) -> f64 {
        match self {
            Signal::Tank(t) => t.step(dt_s).level,
            Signal::Loop(l) => l.step(dt_s),
            Signal::Ou(o) => o.step(dt_s),
        }
    }
}

/// Generate `n` samples of `class` every `dt_ms` starting at `start`.
pub fn series(class: SignalClass, seed: u64, start: Timestamp, n: usize, dt_ms: i64) -> Vec<Point> {
    let mut s = Signal::new(class, seed);
    let dt_s = dt_ms as f64 / 1000.0;
    (0..n).map(|i| Point::good(start + i as i64 * dt_ms, s.step(dt_s))).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tank_cycles_between_hysteresis_limits() {
        let mut t = Tank::new(3);
        let mut switches = 0;
        let mut last = None;
        let mut lo = f64::MAX;
        let mut hi = f64::MIN;
        for _ in 0..6 * 3600 {
            let r = t.step(1.0);
            if last.is_some_and(|l| l != r.pump_on) {
                switches += 1;
            }
            last = Some(r.pump_on);
            lo = lo.min(r.level);
            hi = hi.max(r.level);
        }
        assert!(switches >= 4, "pump should cycle, got {switches}");
        assert!(lo > 28.0 && hi < 82.0, "level range {lo}..{hi}");
    }

    #[test]
    fn signals_are_deterministic_and_in_range() {
        for class in SignalClass::ALL {
            let a = series(class, 11, 0, 5000, 1000);
            assert_eq!(a, series(class, 11, 0, 5000, 1000));
            assert_ne!(a, series(class, 12, 0, 5000, 1000));
            let (lo, hi) = class.span();
            assert!(a.iter().all(|p| p.value >= lo - 1.0 && p.value <= hi + 1.0), "{class:?} out of span");
            assert!(!class.name().is_empty());
        }
    }

    #[test]
    fn flow_steps_and_trips_occur() {
        let mut f = FirstOrder::flow(5);
        let vals: Vec<f64> = (0..20_000).map(|_| f.step(1.0)).collect();
        assert!(vals.iter().any(|v| *v < 2.0), "expected at least one trip");
        let distinct_levels = vals.windows(600).step_by(600).filter(|w| (w[0] - w[599]).abs() > 5.0).count();
        assert!(distinct_levels > 0);
    }
}
