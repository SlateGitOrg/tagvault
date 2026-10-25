//! Per-tag data reduction, split the way PI-style historians split it:
//!
//! * [`deadband`] — *exception* filtering at the collector (cheap, reduces link traffic).
//! * [`sdt`] — *swinging-door trending* compression at the archive (bounded linear
//!   reconstruction error).
//!
//! Both filters are pure, streaming, O(1) per point and O(1) state per tag.

pub mod deadband;
pub mod sdt;

/// True when `a` and `b` differ by more than `dev`. Non-finite values compare by bit pattern so a
/// NaN never silently suppresses a change.
pub(crate) fn exceeds(a: f64, b: f64, dev: f64) -> bool {
    if a.is_finite() && b.is_finite() {
        (a - b).abs() > dev
    } else {
        a.to_bits() != b.to_bits()
    }
}

#[cfg(test)]
mod tests {
    use super::exceeds;

    #[test]
    fn exceeds_handles_non_finite() {
        assert!(exceeds(1.0, 2.5, 1.0));
        assert!(!exceeds(1.0, 2.0, 1.0));
        assert!(exceeds(f64::NAN, 1.0, 100.0));
        assert!(!exceeds(f64::NAN, f64::NAN, 0.0));
        assert!(exceeds(f64::INFINITY, f64::NEG_INFINITY, 0.0));
    }
}
