//! Time parsing/formatting for the CLI and HTTP API (UTC only, no external crates).
//!
//! Accepted time forms: integer milliseconds since epoch; RFC 3339 UTC
//! (`2026-01-05T06:30:00Z`, optional fractional seconds); `*` for now; and PI-style relative
//! times `*-2h`, `*+30m`. Durations: integer milliseconds or `<n>ms|s|m|h|d`.

use crate::error::{Error, Result};
use crate::types::Timestamp;

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

/// Format as RFC 3339 UTC with milliseconds.
pub fn format_time(ts: Timestamp) -> String {
    let days = ts.div_euclid(86_400_000);
    let ms = ts.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{:03}Z",
        ms / 3_600_000,
        ms / 60_000 % 60,
        ms / 1000 % 60,
        ms % 1000
    )
}

/// Parse a duration (`500`, `250ms`, `10s`, `5m`, `2h`, `1d`).
pub fn parse_duration(s: &str) -> Result<i64> {
    let s = s.trim();
    let bad = || Error::InvalidInput(format!("bad duration {s:?}"));
    let split = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    let (num, unit) = s.split_at(split);
    let n: i64 = num.parse().map_err(|_| bad())?;
    let mult = match unit {
        "" | "ms" => 1,
        "s" => 1000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return Err(bad()),
    };
    n.checked_mul(mult).ok_or_else(bad)
}

/// Parse a time; `now` resolves `*` forms.
pub fn parse_time(s: &str, now: Timestamp) -> Result<Timestamp> {
    let s = s.trim();
    let bad = || Error::InvalidInput(format!("bad time {s:?} (use ms, RFC 3339 UTC, or *-1h)"));
    if let Some(rest) = s.strip_prefix('*') {
        if rest.is_empty() {
            return Ok(now);
        }
        let (sub, dur) = match rest.split_at(1) {
            ("-", d) => (true, d),
            ("+", d) => (false, d),
            _ => return Err(bad()),
        };
        let dur = parse_duration(dur)?;
        let t = if sub { now.checked_sub(dur) } else { now.checked_add(dur) };
        return t.ok_or_else(bad);
    }
    if let Ok(ms) = s.parse::<i64>() {
        return Ok(ms);
    }
    // YYYY-MM-DDTHH:MM:SS[.fff]Z
    let b = s.as_bytes();
    if b.len() < 20
        || b[4] != b'-'
        || b[7] != b'-'
        || (b[10] != b'T' && b[10] != b' ')
        || b[13] != b':'
        || b[16] != b':'
        || !s.ends_with('Z')
    {
        return Err(bad());
    }
    let num = |r: std::ops::Range<usize>| s.get(r).and_then(|x| x.parse::<i64>().ok()).ok_or_else(bad);
    let (y, mo, d, h, mi, se) = (num(0..4)?, num(5..7)?, num(8..10)?, num(11..13)?, num(14..16)?, num(17..19)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) || h > 23 || mi > 59 || se > 60 {
        return Err(bad());
    }
    let frac = &s[19..s.len() - 1];
    let ms = if frac.is_empty() {
        0
    } else {
        let digits = frac.strip_prefix('.').ok_or_else(bad)?;
        if digits.is_empty() || digits.len() > 9 || !digits.bytes().all(|c| c.is_ascii_digit()) {
            return Err(bad());
        }
        format!("{digits:0<3}")[..3].parse::<i64>().map_err(|_| bad())?
    };
    Ok(days_from_civil(y, mo, d) * 86_400_000 + h * 3_600_000 + mi * 60_000 + se * 1000 + ms)
}

/// Current wall-clock time in ms since epoch.
pub fn now_ms() -> Timestamp {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc3339_roundtrip_and_known_values() {
        assert_eq!(parse_time("1970-01-01T00:00:00Z", 0).unwrap(), 0);
        assert_eq!(parse_time("2026-01-05T00:00:00Z", 0).unwrap(), 1_767_571_200_000);
        assert_eq!(parse_time("2024-02-29T12:34:56.789Z", 0).unwrap(), 1_709_210_096_789);
        assert_eq!(parse_time("2024-02-29T12:34:56.7Z", 0).unwrap(), 1_709_210_096_700);
        for ts in [0i64, 1_767_571_200_000, 1_709_210_096_789, -86_400_001, 4_102_444_800_000] {
            assert_eq!(parse_time(&format_time(ts), 0).unwrap(), ts);
        }
        assert_eq!(format_time(1_767_571_200_000), "2026-01-05T00:00:00.000Z");
    }

    #[test]
    fn relative_numeric_and_errors() {
        assert_eq!(parse_time("*", 5000).unwrap(), 5000);
        assert_eq!(parse_time("*-2s", 5000).unwrap(), 3000);
        assert_eq!(parse_time("*+1m", 0).unwrap(), 60_000);
        assert_eq!(parse_time("12345", 0).unwrap(), 12345);
        for bad in ["*x1h", "2026-13-01T00:00:00Z", "2026-01-01 00:00:00", "2026-01-01T00:00:00.Z", "yesterday", "*-1y"]
        {
            assert!(parse_time(bad, 0).is_err(), "{bad}");
        }
        assert_eq!(parse_duration("250ms").unwrap(), 250);
        assert_eq!(parse_duration("1d").unwrap(), 86_400_000);
        assert_eq!(parse_duration("15").unwrap(), 15);
        assert!(parse_duration("h").is_err());
        assert!(parse_duration("99999999999999999d").is_err());
        // Relative times that leave the i64 range are errors, not wrapped values.
        assert!(parse_time("*+9223372036854775807", 1).is_err());
        assert!(parse_time("*-9223372036854775807", -2).is_err());
        assert_eq!(parse_time("*-9223372036854775807", 0).unwrap(), -i64::MAX);
        assert!(now_ms() > 1_700_000_000_000);
    }
}
