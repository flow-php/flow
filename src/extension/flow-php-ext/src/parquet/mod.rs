//! Parquet inside flow_php: one reader, one cast table and one writer over Flow filesystem streams, with no Flow types
//! below `etl`, the `Flow\ETL\Adapter\Parquet` surface.

pub mod canonical;
pub mod cells;
pub mod error;
pub mod etl;
pub mod fetch;
pub mod footer;
pub mod library;
pub mod options;
pub mod read;
pub mod sink;
pub mod source;
pub mod write;
