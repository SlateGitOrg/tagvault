//! Core value types shared by every layer.

use serde::{Deserialize, Serialize};

/// Milliseconds since the Unix epoch (Sparkplug B uses the same unit).
pub type Timestamp = i64;

/// Dense numeric identifier assigned to a tag by the archive.
pub type TagId = u32;

/// Data quality attached to every stored value.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Quality {
    /// Value is valid.
    #[default]
    Good,
    /// Value is invalid (e.g. a Sparkplug `is_null` metric); the signal is undefined from here.
    Bad,
    /// The source went offline (NDEATH/DDEATH); the last value is no longer current.
    Stale,
}

impl Quality {
    /// Compact wire code.
    pub fn code(self) -> u8 {
        match self {
            Quality::Good => 0,
            Quality::Bad => 1,
            Quality::Stale => 2,
        }
    }

    /// Inverse of [`Quality::code`].
    pub fn from_code(c: u8) -> Option<Quality> {
        match c {
            0 => Some(Quality::Good),
            1 => Some(Quality::Bad),
            2 => Some(Quality::Stale),
            _ => None,
        }
    }

    /// True for [`Quality::Good`].
    pub fn is_good(self) -> bool {
        self == Quality::Good
    }
}

/// How a tag's value behaves between stored points.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ValueKind {
    /// Continuous process value: linear interpolation between points.
    #[default]
    Analog,
    /// Discrete/boolean/state value: held (stepped) until the next point.
    Discrete,
}

impl ValueKind {
    /// Compact wire code.
    pub fn code(self) -> u8 {
        match self {
            ValueKind::Analog => 0,
            ValueKind::Discrete => 1,
        }
    }

    /// Inverse of [`ValueKind::code`].
    pub fn from_code(c: u8) -> Option<ValueKind> {
        match c {
            0 => Some(ValueKind::Analog),
            1 => Some(ValueKind::Discrete),
            _ => None,
        }
    }
}

/// One timestamped value.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point {
    /// Milliseconds since the Unix epoch.
    pub ts: Timestamp,
    /// Numeric value (booleans are 0/1).
    pub value: f64,
    /// Quality flag.
    pub quality: Quality,
}

impl Point {
    /// A good-quality point.
    pub fn good(ts: Timestamp, value: f64) -> Self {
        Point {
            ts,
            value,
            quality: Quality::Good,
        }
    }

    /// A point with explicit quality.
    pub fn with_quality(ts: Timestamp, value: f64, quality: Quality) -> Self {
        Point { ts, value, quality }
    }
}

/// A point addressed to a named tag, as it travels from collector to archive.
#[derive(Debug, Clone, PartialEq)]
pub struct Sample {
    /// Fully-qualified tag name, e.g. `Plant1/Line1/Tank01/Level`.
    pub tag: String,
    /// Interpolation behaviour of the tag.
    pub kind: ValueKind,
    /// The value itself.
    pub point: Point,
}
