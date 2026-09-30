//! One renderer per column and batch: the plain value of `TextValues::of()` as scalar text or as JSON, read from the
//! arrow array of the column's kind.

use std::collections::hash_map::Entry;
use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, DurationMicrosecondArray, FixedSizeBinaryArray, Float64Array, Int64Array,
    ListArray, MapArray, TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use ext_php_rs::exception::PhpException;
use flow_batch_frame::kind::Kind;

use crate::ctx::array_key_index;
use crate::plan::ValueNode;
use crate::text::date::{self, Format, Zone};
use crate::text::json::{self, Flags};
use crate::text::{float, int, Error};
use crate::values::push_uuid_text;

/// The writer's datetime and date formats; `None` holds a letter the native lane does not render.
pub struct Formats {
    pub date_time: Option<Format>,
    pub date: Option<Format>,
}

impl Formats {
    pub fn parse(date_time: &[u8], date: &[u8]) -> Self {
        Self {
            date_time: Format::parse(date_time),
            date: Format::parse(date),
        }
    }
}

/// The flags of a nested CSV cell: `json_encode($plain, JSON_PRESERVE_ZERO_FRACTION | JSON_THROW_ON_ERROR)`.
const CELL: Flags = Flags {
    unescaped_slashes: false,
    unescaped_unicode: false,
    preserve_zero_fraction: true,
};

enum MapKeys<'a> {
    Int(&'a Int64Array),
    Bytes(&'a BinaryArray),
}

enum Values<'a> {
    Null,
    Int(&'a Int64Array),
    Duration(&'a DurationMicrosecondArray),
    Float(&'a Float64Array),
    Bool(&'a BooleanArray),
    Bytes(&'a BinaryArray),
    Uuid(&'a FixedSizeBinaryArray),
    Instant {
        array: &'a TimestampMicrosecondArray,
        format: &'a Format,
        zone: Zone,
    },
    Days {
        array: &'a Date32Array,
        format: &'a Format,
    },
    List {
        list: &'a ListArray,
        element: Box<Renderer<'a>>,
    },
    Map {
        map: &'a MapArray,
        keys: MapKeys<'a>,
        values: Box<Renderer<'a>>,
    },
    /// (name, optional, renderer): an optional element that is null is absent.
    Struct(Vec<(&'a [u8], bool, Renderer<'a>)>),
}

pub struct Renderer<'a> {
    nulls: Option<&'a NullBuffer>,
    values: Values<'a>,
}

/// Whether the native lane renders a column of `node`. Not rendered: markup; json unless it is the column itself and
/// the format writes its stored text; a datetime or date whose format did not parse; a container holding any of those.
pub fn renders(node: &ValueNode, formats: &Formats, json_column_is_text: bool) -> bool {
    match node {
        ValueNode::Json => json_column_is_text,
        _ => renders_nested(node, formats),
    }
}

fn renders_nested(node: &ValueNode, formats: &Formats) -> bool {
    match node {
        ValueNode::Plain | ValueNode::Null | ValueNode::Time | ValueNode::Uuid | ValueNode::Enum(_) | ValueNode::TimeZone => true,
        ValueNode::DateTime(_) => formats.date_time.is_some(),
        ValueNode::Date => formats.date.is_some(),
        ValueNode::Json | ValueNode::Markup(_) => false,
        ValueNode::List(element) | ValueNode::Map(element) => renders_nested(element, formats),
        ValueNode::Struct(elements) => elements.iter().all(|element| renders_nested(element, formats)),
    }
}

impl<'a> Renderer<'a> {
    /// `None` for a column `renders()` refuses. A named zone still has to be resolved: [`Renderer::resolve`].
    pub fn new(node: &ValueNode, kind: &'a Kind, array: &'a dyn Array, formats: &'a Formats) -> Option<Self> {
        let values = match (node, kind) {
            (_, Kind::Null) | (ValueNode::Null, _) => Values::Null,
            (ValueNode::DateTime(zone), Kind::Timestamp) => Values::Instant {
                array: array.as_primitive::<TimestampMicrosecondType>(),
                format: formats.date_time.as_ref()?,
                zone: Zone::of(zone),
            },
            (ValueNode::Date, Kind::Int32) => Values::Days {
                array: array.as_primitive::<Date32Type>(),
                format: formats.date.as_ref()?,
            },
            (ValueNode::Time, Kind::Duration) => Values::Duration(array.as_primitive::<DurationMicrosecondType>()),
            (ValueNode::Uuid, Kind::Uuid) => Values::Uuid(array.as_fixed_size_binary()),
            (ValueNode::Plain, Kind::Int64) => Values::Int(array.as_primitive::<Int64Type>()),
            (ValueNode::Plain, Kind::Float64) => Values::Float(array.as_primitive::<Float64Type>()),
            (ValueNode::Plain, Kind::Boolean) => Values::Bool(array.as_boolean()),
            (ValueNode::Plain | ValueNode::Enum(_) | ValueNode::TimeZone | ValueNode::Json, Kind::Bytes) => {
                Values::Bytes(array.as_binary::<i32>())
            }
            (ValueNode::List(element_node), Kind::List(element)) => {
                let list = array.as_list::<i32>();

                Values::List {
                    list,
                    element: Box::new(Renderer::new(element_node, &element.kind, list.values().as_ref(), formats)?),
                }
            }
            (ValueNode::Map(value_node), Kind::Map(key, value)) => {
                let map = array.as_map();

                Values::Map {
                    map,
                    keys: match key.kind {
                        Kind::Bytes => MapKeys::Bytes(map.keys().as_binary::<i32>()),
                        Kind::Int64 => MapKeys::Int(map.keys().as_primitive::<Int64Type>()),
                        _ => return None,
                    },
                    values: Box::new(Renderer::new(value_node, &value.kind, map.values().as_ref(), formats)?),
                }
            }
            (ValueNode::Struct(nodes), Kind::Struct(fields)) => Values::Struct(
                nodes
                    .iter()
                    .zip(fields)
                    .zip(array.as_struct().columns())
                    .map(|((node, field), child)| {
                        Some((field.name.as_bytes(), field.optional, Renderer::new(node, &field.kind, child.as_ref(), formats)?))
                    })
                    .collect::<Option<_>>()?,
            ),
            _ => return None,
        };

        Some(Self {
            nulls: array.nulls(),
            values,
        })
    }

    /// Every named zone reads its transitions over the instants it renders, once per batch.
    pub fn resolve(&mut self) -> Result<(), PhpException> {
        match &mut self.values {
            Values::Instant { array, zone, .. } => {
                if let (Some(earliest), Some(latest)) = (array.iter().flatten().min(), array.iter().flatten().max()) {
                    zone.resolve(earliest.div_euclid(1_000_000), latest.div_euclid(1_000_000))?;
                }

                Ok(())
            }
            Values::List { element, .. } => element.resolve(),
            Values::Map { values, .. } => values.resolve(),
            Values::Struct(fields) => fields.iter_mut().try_for_each(|(_, _, field)| field.resolve()),
            _ => Ok(()),
        }
    }

    pub fn is_null(&self, i: usize) -> bool {
        matches!(self.values, Values::Null) || self.nulls.is_some_and(|nulls| nulls.is_null(i))
    }

    /// A field whose text cannot hold a CSV separator, enclosure or escape character that is no digit, letter or
    /// one of `_+./:-`: a number, a boolean, a uuid.
    pub fn is_plain(&self) -> bool {
        matches!(
            self.values,
            Values::Null | Values::Int(_) | Values::Duration(_) | Values::Float(_) | Values::Bool(_) | Values::Uuid(_)
        )
    }

    /// The stored bytes of a non-null string value, when the column is one.
    pub fn bytes(&self, i: usize) -> Option<&'a [u8]> {
        match &self.values {
            Values::Bytes(array) => Some(array.value(i)),
            _ => None,
        }
    }

    /// The scalar text of a non-null value: a CSV field before quoting. A nested value is its JSON text.
    pub fn text(&self, out: &mut Vec<u8>, i: usize) -> Result<(), Error> {
        match &self.values {
            Values::Null => {}
            Values::Int(array) => int(out, array.value(i)),
            Values::Duration(array) => int(out, array.value(i)),
            Values::Float(array) => float::text(out, array.value(i)),
            Values::Bool(array) => out.extend_from_slice(if array.value(i) { b"true" } else { b"false" }),
            Values::Bytes(array) => out.extend_from_slice(array.value(i)),
            Values::Uuid(array) => push_uuid_text(out, array.value(i)),
            Values::Instant { array, format, zone } => date::write(out, format, array.value(i), zone),
            Values::Days { array, format } => date::write(out, format, i64::from(array.value(i)) * 86_400_000_000, &Zone::Utc),
            Values::List { .. } | Values::Map { .. } | Values::Struct(_) => {
                let mut non_finite = false;

                self.json(out, i, &CELL, &mut non_finite)?;

                if non_finite {
                    return Err(Error::NonFinite);
                }
            }
        }

        Ok(())
    }

    /// The JSON text of row `i`, null included. As `json_encode()`, a NAN or an infinity writes `0`, raises
    /// `non_finite` and goes on; text that is not UTF-8 stops with `Error::Utf8`.
    pub fn json(&self, out: &mut Vec<u8>, i: usize, flags: &Flags, non_finite: &mut bool) -> Result<(), Error> {
        if self.is_null(i) {
            out.extend_from_slice(b"null");

            return Ok(());
        }

        match &self.values {
            Values::Null => unreachable!("is_null() above"),
            Values::Int(array) => int(out, array.value(i)),
            Values::Duration(array) => int(out, array.value(i)),
            Values::Float(array) => {
                if float::json(out, array.value(i), flags.preserve_zero_fraction).is_err() {
                    *non_finite = true;
                    out.push(b'0');
                }
            }
            Values::Bool(array) => out.extend_from_slice(if array.value(i) { b"true" } else { b"false" }),
            Values::Bytes(array) => json::string(out, array.value(i), flags)?,
            Values::Uuid(array) => {
                out.push(b'"');
                push_uuid_text(out, array.value(i));
                out.push(b'"');
            }
            Values::Instant { .. } | Values::Days { .. } => {
                let start = out.len();

                out.push(b'"');
                self.text(out, i)?;

                // a format of digits, dashes and colons needs no escaping; anything else is escaped as a string
                if out[start + 1..].iter().all(|byte| matches!(byte, b' ' | b'+'..=b'.' | b'0'..=b':' | b'A'..=b'Z' | b'_' | b'a'..=b'z')) {
                    out.push(b'"');
                } else {
                    let text = out.split_off(start + 1);

                    out.truncate(start);
                    json::string(out, &text, flags)?;
                }
            }
            Values::List { list, element } => {
                let offsets = list.value_offsets();

                out.push(b'[');

                for j in offsets[i] as usize..offsets[i + 1] as usize {
                    if j > offsets[i] as usize {
                        out.push(b',');
                    }

                    element.json(out, j, flags, non_finite)?;
                }

                out.push(b']');
            }
            Values::Map { map, keys, values } => {
                let offsets = map.value_offsets();
                out.push(b'{');

                for (written, (j, last)) in entries(keys, offsets[i] as usize, offsets[i + 1] as usize).into_iter().enumerate() {
                    if written > 0 {
                        out.push(b',');
                    }

                    match keys {
                        MapKeys::Bytes(keys) => json::string(out, keys.value(j), flags)?,
                        MapKeys::Int(keys) => {
                            out.push(b'"');
                            int(out, keys.value(j));
                            out.push(b'"');
                        }
                    }

                    out.push(b':');
                    values.json(out, last, flags, non_finite)?;
                }

                out.push(b'}');
            }
            Values::Struct(fields) => {
                let mut written = 0;

                out.push(b'{');

                for (name, optional, field) in fields {
                    if *optional && field.is_null(i) {
                        continue;
                    }

                    if written > 0 {
                        out.push(b',');
                    }

                    json::string(out, name, flags)?;
                    out.push(b':');
                    field.json(out, i, flags, non_finite)?;
                    written += 1;
                }

                out.push(b'}');
            }
        }

        Ok(())
    }
}

/// A map key as the PHP array key it becomes: `"1"` is the key `1`.
#[derive(PartialEq, Eq, Hash)]
enum ArrayKey<'a> {
    Int(i64),
    Bytes(&'a [u8]),
}

fn array_key<'a>(keys: &MapKeys<'a>, j: usize) -> ArrayKey<'a> {
    match keys {
        MapKeys::Int(keys) => ArrayKey::Int(keys.value(j)),
        MapKeys::Bytes(keys) => {
            let key: &'a [u8] = keys.value(j);

            array_key_index(key).map_or(ArrayKey::Bytes(key), ArrayKey::Int)
        }
    }
}

/// The entries `start..end` of one map as a PHP array holds them, (key index, value index) each: a repeated key keeps
/// its first position and its last value.
fn entries(keys: &MapKeys<'_>, start: usize, end: usize) -> Vec<(usize, usize)> {
    let mut entries: Vec<(usize, usize)> = Vec::with_capacity(end - start);
    let mut positions: HashMap<ArrayKey<'_>, usize> = HashMap::with_capacity(end - start);

    for j in start..end {
        match positions.entry(array_key(keys, j)) {
            Entry::Occupied(position) => entries[*position.get()].1 = j,
            Entry::Vacant(position) => {
                position.insert(entries.len());
                entries.push((j, j));
            }
        }
    }

    entries
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{BinaryBuilder, Int64Builder, MapBuilder};
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
    use arrow_array::types::Int64Type;
    use arrow_schema::{DataType, Field as ArrowField};
    use flow_batch_frame::kind::{Field, Kind};

    use super::{Formats, Renderer};
    use crate::plan::ValueNode;
    use crate::text::json::Flags;

    fn json(node: &ValueNode, kind: &Kind, array: &dyn Array, row: usize) -> String {
        let formats = Formats::parse(b"Y-m-d\\TH:i:sP", b"Y-m-d");
        let mut out = Vec::new();
        let mut non_finite = false;

        Renderer::new(node, kind, array, &formats)
            .unwrap()
            .json(&mut out, row, &Flags::default(), &mut non_finite)
            .unwrap();

        String::from_utf8(out).unwrap()
    }

    fn field(name: &str, kind: Kind, optional: bool) -> Box<Field> {
        Box::new(Field { name: name.to_owned(), kind, optional })
    }

    fn string_map(rows: &[&[(&[u8], i64)]]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, BinaryBuilder::new(), Int64Builder::new());

        for row in rows {
            for (key, value) in *row {
                builder.keys().append_value(key);
                builder.values().append_value(*value);
            }

            builder.append(true).unwrap();
        }

        Arc::new(builder.finish())
    }

    #[test]
    fn a_map_is_always_an_object() {
        let node = ValueNode::Map(Box::new(ValueNode::Plain));
        let kind = Kind::Map(field("key", Kind::Bytes, false), field("value", Kind::Int64, false));
        let map = string_map(&[&[(b"0", 1), (b"1", 2)], &[], &[(b"a", 5)]]);

        assert_eq!(json(&node, &kind, map.as_ref(), 0), r#"{"0":1,"1":2}"#);
        assert_eq!(json(&node, &kind, map.as_ref(), 1), "{}");
        assert_eq!(json(&node, &kind, map.as_ref(), 2), r#"{"a":5}"#);
    }

    #[test]
    fn a_repeated_map_key_keeps_its_first_position_and_its_last_value() {
        let node = ValueNode::Map(Box::new(ValueNode::Plain));
        let kind = Kind::Map(field("key", Kind::Bytes, false), field("value", Kind::Int64, false));
        let map = string_map(&[&[(b"a", 1), (b"b", 2), (b"a", 3), (b"c", 4), (b"b", 5)]]);

        assert_eq!(json(&node, &kind, map.as_ref(), 0), r#"{"a":3,"b":5,"c":4}"#);
    }

    #[test]
    fn a_map_keyed_by_integers_is_an_object() {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), Int64Builder::new());

        for (key, value) in [(0, 10), (1, 11)] {
            builder.keys().append_value(key);
            builder.values().append_value(value);
        }

        builder.append(true).unwrap();

        let node = ValueNode::Map(Box::new(ValueNode::Plain));
        let kind = Kind::Map(field("key", Kind::Int64, false), field("value", Kind::Int64, false));

        assert_eq!(json(&node, &kind, &builder.finish(), 0), r#"{"0":10,"1":11}"#);
    }

    #[test]
    fn a_list_is_always_a_list_and_a_null_is_null() {
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>([Some(vec![Some(1), None, Some(3)]), Some(vec![]), None]);
        let node = ValueNode::List(Box::new(ValueNode::Plain));
        let kind = Kind::List(field("element", Kind::Int64, true));

        assert_eq!(json(&node, &kind, &list, 0), "[1,null,3]");
        assert_eq!(json(&node, &kind, &list, 1), "[]");
        assert_eq!(json(&node, &kind, &list, 2), "null");
    }

    #[test]
    fn a_structure_is_an_object_and_a_null_optional_element_is_absent() {
        let structure = StructArray::from(vec![
            (
                Arc::new(ArrowField::new("a", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(1), None])) as ArrayRef,
            ),
            (
                Arc::new(ArrowField::new("b", DataType::Int64, true)),
                Arc::new(Int64Array::from(vec![Some(2), None])) as ArrayRef,
            ),
        ]);
        let node = ValueNode::Struct(vec![ValueNode::Plain, ValueNode::Plain]);
        let kind = Kind::Struct(vec![*field("a", Kind::Int64, false), *field("b", Kind::Int64, true)]);

        assert_eq!(json(&node, &kind, &structure, 0), r#"{"a":1,"b":2}"#);
        assert_eq!(json(&node, &kind, &structure, 1), r#"{"a":null}"#);
    }
}
