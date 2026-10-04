//! Parquet in flow_php: the `Flow\ETL\Adapter\Parquet` surface over arrow-ext's Parquet batches.

#[allow(
    non_snake_case,
    reason = "PHP takes a parameter name from its Rust identifier: the contracts' are camelCase"
)]
pub mod etl;
