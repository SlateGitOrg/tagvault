//! Byte- and bit-level encodings used by chunks, the WAL and the forwarding wire format.
//!
//! * [`bits`]   — MSB-first bit writer/reader.
//! * [`gorilla`] — delta-of-delta timestamps and XOR floating-point values
//!   (Pelkonen et al., "Gorilla: A Fast, Scalable, In-Memory Time Series Database", VLDB 2015).
//! * [`varint`] — LEB128 + zigzag integers and a bounds-checked byte cursor.

pub mod bits;
pub mod gorilla;
pub mod varint;
