//! Benign simulation for demos, tests and benchmarks: process signals, a Sparkplug edge node,
//! and a scripted plant scenario recorded to a capture file.

pub mod capture;
pub mod edge;
pub mod rng;
pub mod signals;

pub use capture::{CaptureFrame, Scenario};
pub use edge::{EdgeNode, SimDevice, SpMessage};
pub use signals::SignalClass;
