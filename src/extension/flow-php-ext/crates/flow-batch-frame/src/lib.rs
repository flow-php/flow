//! Flow's batch layout in Rust; the pure-PHP classes in flow-php/etl are the reference.

// Floe is little-endian by construction; a big-endian host could never match its bytes.
#[cfg(target_endian = "big")]
compile_error!("flow-batch-frame only supports little-endian targets");

pub mod builder;
pub mod column;
pub mod error;
pub mod frame;
pub mod key;
pub mod kind;
pub mod layout;

pub use error::Error;
