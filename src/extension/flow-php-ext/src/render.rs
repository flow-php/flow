//! flow-batch-frame refusals rendered as the exceptions `PhpBackend::decode()` throws for the same buffers: the
//! `Flow\ETL\Exception\InvalidArgumentException` (or `OffsetOverflow`) with its exact message, a nested type named by
//! its PHP `toString()`. Parquet core refusals rendered for the reader and the writer of `Flow\ETL\Adapter\Parquet`.

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use flow_batch_frame::error::{Of, Role, Values};
use flow_batch_frame::kind::Kind;
use flow_batch_frame::layout::node_count;
use flow_batch_frame::Error;

use crate::ctx::{call_method, expect_object, find_class, read_property, transparent_exception};
use crate::exception::ext_exception;
use crate::parquet::error::Error as ParquetError;
use crate::parquet::source::PhpStream;

const INVALID_ARGUMENT: &str = "Flow\\ETL\\Exception\\InvalidArgumentException";
const OFFSET_OVERFLOW: &str = "Flow\\ETL\\Exception\\OffsetOverflow";
const RUNTIME: &str = "Flow\\ETL\\Exception\\RuntimeException";

pub fn exception(class: &str, message: String) -> PhpException {
    match find_class(class) {
        Ok(ce) => PhpException::new(message, 0, ce),
        Err(e) => e,
    }
}

pub fn invalid_argument(message: String) -> PhpException {
    exception(INVALID_ARGUMENT, message)
}

pub fn offset_overflow(last: u64) -> PhpException {
    exception(OFFSET_OVERFLOW, format!("Offsets exceed the i32 range of a batch buffer: the last offset is {last}"))
}

fn of(of: Of) -> &'static str {
    match of {
        Of::List => "List",
        Of::Map => "Map",
        Of::Utf8 => "Utf8",
    }
}

fn values(values: Values) -> &'static str {
    match values {
        Values::Int64 => "Int64",
        Values::Int32 => "Int32",
        Values::Float64 => "Float64",
        Values::Boolean => "Boolean",
        Values::FixedBinary16 => "FixedBinary16",
    }
}

/// A `column::decode` refusal of the column typed `type_zv`, whose kind is `kind`.
pub fn decode_exception(error: Error, type_zv: &Zval, kind: &Kind) -> PhpException {
    let message = match error {
        Error::OffsetOverflow { last } => return offset_overflow(last),
        Error::BuffersExhausted => "Column buffers exhausted: the column needs more buffers than given".to_string(),
        Error::OffsetsLength { of: layout, bytes, expected, rows } => format!(
            "{} offsets buffer of {bytes} bytes, expected {expected} for {rows} rows",
            of(layout)
        ),
        Error::MapEntriesValidity { bytes } => format!("Map entries validity must be omitted, got {bytes} bytes"),
        Error::ValidityTooShort { bytes, rows } => format!("Validity bitmap of {bytes} bytes is too short for {rows} rows"),
        Error::NullKindNullCount { rows, null_count } => {
            format!("Null column of {rows} rows carries a null count of {null_count}")
        }
        Error::NullCountMismatch { declared, derived, rows } => format!(
            "Column null count {declared} disagrees with its validity bitmap ({derived} nulls in {rows} rows)"
        ),
        Error::NestedNulls { node, role, nulls } => {
            let name = match type_name_at(type_zv, kind, node) {
                Ok(name) => name,
                Err(e) => return e,
            };

            format!(
                "{} holds {nulls} nulls in a non-nullable {name}",
                match role {
                    Role::ListElement => "List child",
                    Role::MapKey => "Map key child",
                    Role::MapValue => "Map value child",
                }
            )
        }
        Error::BufferCount { node, buffers, expected } => {
            let name = match type_name_at(type_zv, kind, node) {
                Ok(name) => name,
                Err(e) => return e,
            };

            format!("Column of type {name} holds {buffers} buffers, its layout needs {expected}")
        }
        Error::OffsetsStart { of: layout, first } => format!("{} offsets start at {first}, not 0", of(layout)),
        Error::OffsetsNotMonotonic { of: layout, index, previous, current } => format!(
            "{} offsets are not monotonic: offset {} is {previous}, offset {index} is {current}",
            of(layout),
            index - 1
        ),
        Error::ValuesLength { values: layout, bytes, expected, rows } => format!(
            "{} values buffer of {bytes} bytes, expected {expected} for {rows} rows",
            values(layout)
        ),
        Error::Utf8DataLength { bytes, last_offset } => {
            format!("Utf8 data buffer of {bytes} bytes, the last offset is {last_offset}")
        }
        error => format!("flow_php refused the column buffers: {error}"),
    };

    invalid_argument(message)
}

/// `toString()` of the type at pre-order `node` of `type_zv`'s tree (the map entries node counts but has no type).
fn type_name_at(type_zv: &Zval, kind: &Kind, node: usize) -> Result<String, PhpException> {
    let name = call_method(&type_at(type_zv.shallow_clone(), kind, node)?, "toString", &mut [])?;

    Ok(String::from_utf8_lossy(
        name.zend_str()
            .ok_or_else(|| ext_exception("flow_php expected Type::toString() to return a string"))?
            .as_bytes(),
    )
    .into_owned())
}

fn type_at(type_zv: Zval, kind: &Kind, node: usize) -> Result<Zval, PhpException> {
    if node == 0 {
        return Ok(type_zv);
    }

    let optional = find_class("Flow\\Types\\Type\\Logical\\OptionalType")?;
    let base = if expect_object(&type_zv, "a Type")?.instance_of(optional) {
        call_method(&type_zv, "base", &mut [])?
    } else {
        type_zv
    };

    match kind {
        Kind::List(element) => type_at(call_method(&base, "element", &mut [])?, &element.kind, node - 1),
        Kind::Map(key, value) => {
            let key_nodes = node_count(&key.kind);

            if node - 2 < key_nodes {
                type_at(call_method(&base, "key", &mut [])?, &key.kind, node - 2)
            } else {
                type_at(call_method(&base, "value", &mut [])?, &value.kind, node - 2 - key_nodes)
            }
        }
        Kind::Struct(fields) => {
            let elements = call_method(&base, "elements", &mut [])?;
            let elements = elements
                .array()
                .ok_or_else(|| ext_exception("flow_php expected StructureType::elements() to return an array"))?;
            let mut first = 1;

            for (field, element) in fields.iter().zip(elements.values()) {
                let nodes = node_count(&field.kind);

                if node < first + nodes {
                    let element_type = read_property(expect_object(element, "a StructureElement")?, "type")?;

                    return type_at(element_type, &field.kind, node - first);
                }

                first += nodes;
            }

            Err(ext_exception("flow_php could not find a structure node"))
        }
        _ => Err(ext_exception("flow_php could not find a nested node")),
    }
}

/// Which Parquet surface refused: the same core refusal names what to do about it per side.
pub enum Side {
    Read,
    Write,
}

/// Which API the refusal surfaces through: `Flow\ETL\Adapter\Parquet` or `Flow\Parquet`.
#[derive(Clone, Copy)]
pub enum Surface {
    Etl,
    Lib,
}

const LIB_RUNTIME: &str = "Flow\\Parquet\\Exception\\RuntimeException";
const LIB_VALIDATION: &str = "Flow\\Parquet\\Exception\\ValidationException";
const LIB_INVALID_ARGUMENT: &str = "Flow\\Parquet\\Exception\\InvalidArgumentException";

/// A Parquet core refusal; an exception the stream threw while the core called it surfaces as itself.
pub fn parquet_exception(error: ParquetError, stream: &PhpStream, side: Side, surface: Surface) -> PhpException {
    if let Some(mut thrown) = stream.thrown() {
        return transparent_exception(&mut thrown);
    }

    let (runtime, validation, arguments) = match surface {
        Surface::Etl => (RUNTIME, RUNTIME, INVALID_ARGUMENT),
        Surface::Lib => (LIB_RUNTIME, LIB_VALIDATION, LIB_INVALID_ARGUMENT),
    };
    let failure = |message: String| match surface {
        Surface::Etl => ext_exception(message),
        Surface::Lib => exception(LIB_RUNTIME, message),
    };

    match (error, side) {
        (ParquetError::Unsupported { column, parquet }, Side::Read) => exception(
            runtime,
            format!(
                "Parquet column \"{column}\" ({parquet}) is not supported by the flow_php Parquet reader; {}",
                match surface {
                    Surface::Etl => {
                        "read the file with from_parquet($path, engine: new \\Flow\\Parquet\\Engine\\PhpParquetEngine())"
                    }
                    Surface::Lib => "read the file with \\Flow\\Parquet\\Reader::php()",
                }
            ),
        ),
        (ParquetError::Unsupported { column, parquet }, Side::Write) => exception(
            runtime,
            format!(
                "Parquet column \"{column}\" ({parquet}) is not supported by the flow_php Parquet writer; {}",
                match surface {
                    Surface::Etl => {
                        "write the file with to_parquet($path, engine: new \\Flow\\Parquet\\Engine\\PhpParquetEngine())"
                    }
                    Surface::Lib => "write it with \\Flow\\Parquet\\Writer::php()",
                }
            ),
        ),
        (ParquetError::Overflow { column, row }, _) => exception(
            runtime,
            format!("Parquet column \"{column}\" row {row} holds a value out of the range flow_php stores it in"),
        ),
        (ParquetError::InvalidUtf8 { column, row }, _) => exception(
            runtime,
            format!(
                "Parquet column \"{column}\" row {row} holds a string that is not valid UTF-8; Parquet STRING columns \
                 require UTF-8"
            ),
        ),
        (ParquetError::Length { column, row, expected }, _) => exception(
            runtime,
            format!("Parquet column \"{column}\" row {row} holds a value that is not {expected} bytes long"),
        ),
        (ParquetError::Value { column, row, expected, got }, _) => {
            exception(validation, format!("Column \"{column}\" row {row}: expected {expected}, got {got}"))
        }
        (ParquetError::MapKey { column, key }, _) => exception(
            arguments,
            format!("Map key of Parquet column \"{column}\" must be int or string, got {key}"),
        ),
        (ParquetError::MissingColumn(name), _) => exception(arguments, format!("Parquet file has no column \"{name}\"")),
        (ParquetError::Options(message), _) => exception(arguments, message),
        (ParquetError::NotParquet(message), _) => match surface {
            Surface::Etl => failure(format!("flow_php failed to read Parquet: {message}")),
            Surface::Lib => exception(arguments, format!("Given file is not valid Parquet file: {message}")),
        },
        (ParquetError::Stream(message), _) => failure(format!("flow_php Parquet stream {message}")),
        (ParquetError::Parquet(error), Side::Read) => failure(format!("flow_php failed to read Parquet: {error}")),
        (ParquetError::Arrow(error), Side::Read) => failure(format!("flow_php failed to read Parquet: {error}")),
        (ParquetError::Parquet(error), Side::Write) => failure(format!("flow_php failed to write Parquet: {error}")),
        (ParquetError::Arrow(error), Side::Write) => failure(format!("flow_php failed to write Parquet: {error}")),
    }
}
