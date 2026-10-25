//! # tagvault
//!
//! An industrial time-series historian for plant telemetry.
//!
//! Data flows through the crate in the same stages a PI/AVEVA-style historian uses:
//!
//! 1. [`sparkplug`] — decode MQTT Sparkplug B (v3.0) payloads: births, data, deaths, metric
//!    aliases, sequence numbers, reorder window and rebirth requests.
//! 2. [`compress::deadband`] — exception filtering at the collector.
//! 3. [`forward`] — disk-backed store-and-forward spool for intermittent links.
//! 4. [`compress::sdt`] — swinging-door compression at the archive with a bounded error.
//! 5. [`storage`] — crash-safe write-ahead log plus columnar Gorilla-encoded chunk segments.
//! 6. [`query`] — raw, interpolated and per-interval aggregate queries.
//!
//! [`sim`] provides a benign process-signal generator and a simulated Sparkplug edge node used by
//! the demo, tests and benchmarks; [`net`] holds the minimal MQTT client and HTTP API.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod compress;
pub mod encoding;
pub mod error;
pub mod sparkplug;
pub mod types;

pub use error::{Error, Result};
pub use types::{Point, Quality, Sample, TagId, Timestamp, ValueKind};
