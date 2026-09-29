//! Parquet inside flow_php: one reader, one cast table and one writer over Flow filesystem streams, with no Flow types
//! below `etl`, the `Flow\ETL\Adapter\Parquet` surface.

pub mod canonical;
pub mod error;
pub mod etl;
pub mod options;
pub mod read;
pub mod sink;
pub mod source;
pub mod write;
