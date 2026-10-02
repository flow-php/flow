//! Parquet in arrow: one reader, one cast table and one writer over Flow filesystem streams, surfaced as the
//! `flow-php/parquet` lib's values (`library`) and as Arrow C Data struct batches (`batch`).

pub mod batch;
pub mod canonical;
pub mod cells;
pub mod error;
pub mod fetch;
pub mod footer;
pub mod library;
pub mod options;
pub mod read;
pub mod sink;
pub mod source;
pub mod write;
