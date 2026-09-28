//! Flow's batch layout in Rust: one column's buffers in the canonical form of `Flow\ETL\Column\Column::encode()` and
//! back (`column`), and the Floe BATCH frame body around them (`body`). The pure-PHP classes in flow-php/etl are the
//! reference; this crate holds no PHP types and no message text.
//!
//! arrow-ext will hold a copy of this crate: keep both copies identical.

// Floe is little-endian by construction; a big-endian host could never match its bytes.
#[cfg(target_endian = "big")]
compile_error!("flow-batch-frame only supports little-endian targets");

pub mod body;
pub mod column;
pub mod error;
pub mod format;
pub mod kind;
pub mod layout;

pub use error::Error;
