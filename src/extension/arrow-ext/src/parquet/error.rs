//! What the Parquet core refuses, as data: `render.rs` turns each variant into the PHP exception and its message.

use arrow_schema::ArrowError;
use parquet::errors::ParquetError;

#[derive(Debug)]
pub enum Error {
    Parquet(ParquetError),
    Arrow(ArrowError),
    MissingColumn(String),
    /// `parquet`: the arrow type (or codec) the core has no cast for.
    Unsupported { column: String, parquet: String },
    Overflow { column: String, row: usize },
    InvalidUtf8 { column: String, row: usize },
    /// A fixed-length target the value's byte length does not match.
    Length { column: String, row: usize, expected: usize },
    /// MAP keys of a type a PHP array does not key by (int or string), named by the PHP type they would read as.
    MapKey { column: String, key: &'static str },
    /// A value the target type does not accept: what it expects and what it got.
    Value { column: String, row: usize, expected: &'static str, got: String },
    /// Bytes that are not a Parquet file: no footer where it must be.
    NotParquet(String),
    Options(String),
    Stream(String),
}

impl Error {
    /// The same refusal named after the root column it happened in.
    pub fn in_column(self, name: &str) -> Self {
        match self {
            Error::Unsupported { parquet, .. } => Error::Unsupported { column: name.to_string(), parquet },
            Error::Overflow { row, .. } => Error::Overflow { column: name.to_string(), row },
            Error::InvalidUtf8 { row, .. } => Error::InvalidUtf8 { column: name.to_string(), row },
            Error::Length { row, expected, .. } => Error::Length { column: name.to_string(), row, expected },
            Error::MapKey { key, .. } => Error::MapKey { column: name.to_string(), key },
            Error::Value { row, expected, got, .. } => Error::Value { column: name.to_string(), row, expected, got },
            error => error,
        }
    }

    /// The same refusal at the parent row `parent` maps a child row to.
    pub fn at_row(self, parent: impl Fn(usize) -> usize) -> Self {
        match self {
            Error::Overflow { column, row } => Error::Overflow { column, row: parent(row) },
            Error::InvalidUtf8 { column, row } => Error::InvalidUtf8 { column, row: parent(row) },
            Error::Length { column, row, expected } => Error::Length { column, row: parent(row), expected },
            Error::Value { column, row, expected, got } => Error::Value { column, row: parent(row), expected, got },
            error => error,
        }
    }
}

impl From<ParquetError> for Error {
    fn from(error: ParquetError) -> Self {
        Error::Parquet(error)
    }
}

impl From<ArrowError> for Error {
    fn from(error: ArrowError) -> Self {
        Error::Arrow(error)
    }
}

/// The parent row of child row `child` under list/map `offsets`.
pub fn parent_row<O: Copy + Into<i64>>(offsets: &[O], child: usize) -> usize {
    offsets.partition_point(|offset| (*offset).into() <= child as i64).saturating_sub(1)
}
