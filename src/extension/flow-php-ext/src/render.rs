//! flow-batch-frame refusals rendered as the exceptions `PhpBackend::decode()` throws for the same buffers: the
//! `Flow\ETL\Foundation\Exception\InvalidArgumentException` (or `OffsetOverflow`) with its exact message, a nested type named by
//! its PHP `toString()`.

use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use flow_batch_frame::error::{Of, Role, Values};
use flow_batch_frame::frame::Failure;
use flow_batch_frame::kind::Kind;
use flow_batch_frame::layout::node_count;
use flow_batch_frame::Error;

use crate::backend::Definitions;
use crate::ctx::{
    call_method, construct, expect_object, find_class, read_property, transparent_exception, zval_long, zval_str,
};
use crate::exception::ext_exception;

const INVALID_ARGUMENT: &str = "Flow\\ETL\\Foundation\\Exception\\InvalidArgumentException";
const OFFSET_OVERFLOW: &str = "Flow\\ETL\\Foundation\\Exception\\OffsetOverflow";
const FLOE_EXCEPTION: &str = "Flow\\ETL\\Values\\Column\\Frame\\FloeException";
const FRAME_TOO_LARGE: &str = "Flow\\ETL\\Values\\Column\\Frame\\FrameTooLarge";
pub const RUNTIME: &str = "Flow\\ETL\\Foundation\\Exception\\RuntimeException";

pub fn exception(class: &str, message: String) -> PhpException {
    match find_class(class) {
        Ok(ce) => PhpException::new(message, 0, ce),
        Err(e) => e,
    }
}

pub fn invalid_argument(message: String) -> PhpException {
    exception(INVALID_ARGUMENT, message)
}

pub fn runtime(message: String) -> PhpException {
    exception(RUNTIME, message)
}

pub fn offset_overflow(last: u64) -> PhpException {
    exception(OFFSET_OVERFLOW, offset_overflow_message(last))
}

fn offset_overflow_message(last: u64) -> String {
    format!("Offsets exceed the i32 range of a batch buffer: the last offset is {last}")
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
    match decode_refusal(error, type_zv, kind) {
        Ok((class, message)) => exception(class, message),
        Err(e) => e,
    }
}

/// The exception class and message `PhpBackend::decode()` throws for `error`.
fn decode_refusal(error: Error, type_zv: &Zval, kind: &Kind) -> Result<(&'static str, String), PhpException> {
    let message = match error {
        Error::OffsetOverflow { last } => return Ok((OFFSET_OVERFLOW, offset_overflow_message(last))),
        Error::BuffersExhausted => "Column buffers exhausted: the column needs more buffers than given".to_string(),
        Error::OffsetsLength {
            of: layout,
            bytes,
            expected,
            rows,
        } => {
            format!(
                "{} offsets buffer of {bytes} bytes, expected {expected} for {rows} rows",
                of(layout)
            )
        }
        Error::MapEntriesValidity { bytes } => format!("Map entries validity must be omitted, got {bytes} bytes"),
        Error::ValidityTooShort { bytes, rows } => {
            format!("Validity bitmap of {bytes} bytes is too short for {rows} rows")
        }
        Error::NullKindNullCount { rows, null_count } => {
            format!("Null column of {rows} rows carries a null count of {null_count}")
        }
        Error::NullCountMismatch {
            declared,
            derived,
            rows,
        } => {
            format!("Column null count {declared} disagrees with its validity bitmap ({derived} nulls in {rows} rows)")
        }
        Error::NestedNulls { node, role, nulls } => {
            let name = type_name_at(type_zv, kind, node)?;

            format!(
                "{} holds {nulls} nulls in a non-nullable {name}",
                match role {
                    Role::ListElement => "List child",
                    Role::MapKey => "Map key child",
                    Role::MapValue => "Map value child",
                }
            )
        }
        Error::BufferCount {
            node,
            buffers,
            expected,
        } => {
            let name = type_name_at(type_zv, kind, node)?;

            format!("Column of type {name} holds {buffers} buffers, its layout needs {expected}")
        }
        Error::OffsetsStart { of: layout, first } => format!("{} offsets start at {first}, not 0", of(layout)),
        Error::OffsetsNotMonotonic {
            of: layout,
            index,
            previous,
            current,
        } => format!(
            "{} offsets are not monotonic: offset {} is {previous}, offset {index} is {current}",
            of(layout),
            index - 1
        ),
        Error::ValuesLength {
            values: layout,
            bytes,
            expected,
            rows,
        } => {
            format!(
                "{} values buffer of {bytes} bytes, expected {expected} for {rows} rows",
                values(layout)
            )
        }
        Error::Utf8DataLength { bytes, last_offset } => {
            format!("Utf8 data buffer of {bytes} bytes, the last offset is {last_offset}")
        }
        error => format!("flow_php refused the column buffers: {error}"),
    };

    Ok((INVALID_ARGUMENT, message))
}

/// A `frame::decode` refusal as the `FloeException` `PhpBackend::decodeFrame()` throws for the same body; a malformed
/// column carries its own refusal as `previous`, a codec's exception surfaces as itself.
pub fn frame_exception(failure: Failure<PhpException>, definitions: &Definitions) -> PhpException {
    let error = match failure {
        Failure::Codec(e) => return e,
        Failure::Frame(error) => error,
    };
    let name = |column: usize| String::from_utf8_lossy(&definitions.columns[column].key).into_owned();
    let message = match error {
        Error::DirectoryTruncated => "Floe BATCH frame is truncated, its directory is incomplete".to_string(),
        Error::FrameNodeCount { frame, schema } => {
            format!("Floe BATCH frame holds {frame} nodes, the schema describes {schema}")
        }
        Error::FrameBufferCount { frame, schema } => {
            format!("Floe BATCH frame holds {frame} buffers, the schema describes {schema}")
        }
        Error::ColumnRows {
            column,
            rows,
            frame_rows,
        } => format!(
            "Floe BATCH frame column \"{}\" holds {rows} rows, the frame {frame_rows}",
            name(column)
        ),
        Error::ColumnNulls { column, nulls, rows } => {
            format!(
                "Floe BATCH frame column \"{}\" has {nulls} nulls in {rows} rows",
                name(column)
            )
        }
        Error::BufferOutsideArea { buffer, area } => {
            format!("Floe BATCH frame buffer {buffer} lies outside the {area}-byte buffer area")
        }
        Error::BufferShorterThanPrefix { buffer } => {
            format!("Floe BATCH frame buffer {buffer} is shorter than its 8-byte length prefix")
        }
        Error::DecodedLength {
            buffer,
            decoded,
            prefix,
        } => format!("Floe BATCH frame buffer {buffer} decodes to {decoded} bytes, its prefix declares {prefix}"),
        Error::NodesDisagree { column } => format!(
            "Floe BATCH frame column \"{}\" declares nodes that disagree with its buffers",
            name(column)
        ),
        Error::MalformedColumn { column, error } => {
            let plan = &definitions.columns[column].plan;
            let (class, message) = match decode_refusal(*error, &plan.type_zv, &plan.kind) {
                Ok(refusal) => refusal,
                Err(e) => return e,
            };
            let previous = match find_class(class)
                .and_then(|ce| construct(ce, &mut [zval_str(message.as_bytes()), zval_long(0)]))
                .and_then(|object| {
                    object
                        .into_zval(false)
                        .map_err(|e| ext_exception(format!("flow_php failed to build the previous exception: {e:?}")))
                }) {
                Ok(previous) => previous,
                Err(e) => return e,
            };

            return floe_exception(
                format!("Floe BATCH frame column \"{}\" is malformed: {message}", name(column)),
                previous,
            );
        }
        error => return ext_exception(format!("flow_php refused the frame: {error}")),
    };

    exception(FLOE_EXCEPTION, message)
}

/// A `frame::encode` refusal as the exception `PhpBackend::encodeFrame()` throws for the same rows: a column's
/// `OffsetOverflow`, the body's `FrameTooLarge`; a codec's exception surfaces as itself.
pub fn encode_exception(failure: Failure<PhpException>) -> PhpException {
    match failure {
        Failure::Codec(e) => e,
        Failure::Frame(Error::OffsetOverflow { last }) => offset_overflow(last),
        Failure::Frame(Error::FrameTooLarge { bytes }) => exception(
            FRAME_TOO_LARGE,
            format!("Floe BATCH frame body of {bytes} bytes exceeds the 4 GiB frame length"),
        ),
        Failure::Frame(error) => ext_exception(format!("flow_php failed to encode a frame: {error}")),
    }
}

/// `new FloeException($message, 0, $previous)`, thrown as itself so `getPrevious()` is what PHP's walk attaches.
pub fn floe_exception(message: String, previous: Zval) -> PhpException {
    let built = find_class(FLOE_EXCEPTION)
        .and_then(|ce| construct(ce, &mut [zval_str(message.as_bytes()), zval_long(0), previous]));

    match built {
        Ok(mut exception) => transparent_exception(&mut exception),
        Err(e) => e,
    }
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
