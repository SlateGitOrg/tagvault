//! Eclipse Sparkplug B (specification v3.0) support.
//!
//! * [`proto`] — the `sparkplug_b.proto` message definitions (hand-written prost derives, so no
//!   `protoc` is needed at build time).
//! * [`topic`] — `spBv1.0/<group>/<type>/<edge>[/<device>]` topic parsing and validation.
//! * [`datatype`] — the Sparkplug datatype codes and conversion of metric values to numbers.
//! * [`host`] — the Host Application state machine: births, deaths, alias resolution, sequence
//!   checking with a reorder window, and `Node Control/Rebirth` requests.

pub mod datatype;
pub mod host;
pub mod proto;
pub mod topic;

pub use host::{Host, HostConfig, HostEvent, HostOutput, HostStats, Outbound};
pub use topic::{MessageType, Topic};
