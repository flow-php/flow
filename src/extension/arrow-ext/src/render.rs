//! Parquet core refusals rendered as the `Flow\Parquet` exceptions, for the readers and the writer.

use ext_php_rs::exception::PhpException;

use crate::parquet::error::Error;
use crate::parquet::source::PhpStream;
use crate::php::{find_class, transparent_exception};

const RUNTIME: &str = "Flow\\Parquet\\Exception\\RuntimeException";
const VALIDATION: &str = "Flow\\Parquet\\Exception\\ValidationException";
const INVALID_ARGUMENT: &str = "Flow\\Parquet\\Exception\\InvalidArgumentException";

pub fn exception(class: &str, message: String) -> PhpException {
    match find_class(class) {
        Ok(ce) => PhpException::new(message, 0, ce),
        Err(e) => e,
    }
}

/// Which Parquet surface refused: the same core refusal names what to do about it per side.
pub enum Side {
    Read,
    Write,
}

/// A Parquet core refusal; an exception the stream threw while the core called it surfaces as itself.
pub fn parquet_exception(error: Error, stream: &PhpStream, side: Side) -> PhpException {
    if let Some(mut thrown) = stream.thrown() {
        return transparent_exception(&mut thrown);
    }

    let failure = |message: String| exception(RUNTIME, message);

    match (error, side) {
        (Error::Unsupported { column, parquet }, Side::Read) => exception(
            RUNTIME,
            format!(
                "Parquet column \"{column}\" ({parquet}) is not supported by the arrow Parquet reader; read the file \
                 with \\Flow\\Parquet\\Reader::php()"
            ),
        ),
        (Error::Unsupported { column, parquet }, Side::Write) => exception(
            RUNTIME,
            format!(
                "Parquet column \"{column}\" ({parquet}) is not supported by the arrow Parquet writer; write it with \
                 \\Flow\\Parquet\\Writer::php()"
            ),
        ),
        (Error::Overflow { column, row }, _) => exception(
            RUNTIME,
            format!("Parquet column \"{column}\" row {row} holds a value out of the range arrow stores it in"),
        ),
        (Error::InvalidUtf8 { column, row }, _) => exception(
            RUNTIME,
            format!(
                "Parquet column \"{column}\" row {row} holds a string that is not valid UTF-8; Parquet STRING columns \
                 require UTF-8"
            ),
        ),
        (Error::Length { column, row, expected }, _) => exception(
            RUNTIME,
            format!("Parquet column \"{column}\" row {row} holds a value that is not {expected} bytes long"),
        ),
        (Error::Value { column, row, expected, got }, _) => {
            exception(VALIDATION, format!("Column \"{column}\" row {row}: expected {expected}, got {got}"))
        }
        (Error::MapKey { column, key }, _) => exception(
            INVALID_ARGUMENT,
            format!("Map key of Parquet column \"{column}\" must be int or string, got {key}"),
        ),
        (Error::MissingColumn(name), _) => exception(INVALID_ARGUMENT, format!("Parquet file has no column \"{name}\"")),
        (Error::Options(message), _) => exception(INVALID_ARGUMENT, message),
        (Error::NotParquet(message), _) => {
            exception(INVALID_ARGUMENT, format!("Given file is not valid Parquet file: {message}"))
        }
        (Error::Stream(message), _) => failure(format!("arrow Parquet stream {message}")),
        (Error::Parquet(error), Side::Read) => failure(format!("arrow failed to read Parquet: {error}")),
        (Error::Arrow(error), Side::Read) => failure(format!("arrow failed to read Parquet: {error}")),
        (Error::Parquet(error), Side::Write) => failure(format!("arrow failed to write Parquet: {error}")),
        (Error::Arrow(error), Side::Write) => failure(format!("arrow failed to write Parquet: {error}")),
    }
}
