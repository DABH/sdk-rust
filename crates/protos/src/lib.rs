#![warn(missing_docs)]

//! Compiled protobuf definitions for the Temporal Rust SDK.

pub mod protos;

pub use protos::*;

/// The descriptor set used to generate the SDK's bundled protobuf messages.
#[cfg(feature = "descriptors")]
pub const FILE_DESCRIPTOR_SET: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/descriptors.bin"));
