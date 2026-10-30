//! Flow's batch layout in Rust: one column's buffers in the canonical form of `Flow\ETL\Values\Column\Column::encode()` and
//! back (`column`), the builders that append rows into that form (`builder`), the kind and value shape of every Flow
//! type (`kind`) and the keys columns are held by (`key`). The pure-PHP classes in flow-php/etl are the reference; this
//! crate holds no PHP types and no message text.

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
