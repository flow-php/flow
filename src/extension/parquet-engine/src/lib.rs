//! Parquet over Flow filesystem streams: one reader, one cast table and one writer, surfaced as the
//! `flow-php/parquet` lib's values (`library`).

#[cfg(all(feature = "arrow-names", feature = "flow-php-names"))]
compile_error!("flow-parquet-engine: enable exactly one of the features arrow-names, flow-php-names");
#[cfg(not(any(feature = "arrow-names", feature = "flow-php-names")))]
compile_error!("flow-parquet-engine: enable exactly one of the features arrow-names, flow-php-names");

pub mod canonical;
pub mod cells;
pub mod chunks;
pub mod engine;
pub mod error;
pub mod exception;
pub mod filter;
pub mod footer;
pub mod interfaces;
pub mod lib_calls;
#[allow(
    non_snake_case,
    reason = "PHP takes a parameter name from its Rust identifier: the contracts' are camelCase"
)]
pub mod library;
pub mod options;
pub mod php;
pub mod read;
pub mod render;
pub mod sink;
pub mod source;
pub mod thrift;
pub mod values;
pub mod write;
