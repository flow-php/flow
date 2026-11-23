//! The canonical, type-tagged bytes of values and rows, byte for byte those of PHP's `CanonicalEncoding`.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::Array;
use arrow_schema::DataType;
use flow_batch_frame::error::Part;
use flow_batch_frame::kind::TypeJson;
use flow_batch_frame::Error;
use md5::Md5;
use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_128;

pub const NULL: u8 = 0x00;
pub const BOOLEAN: u8 = 0x01;
pub const INTEGER: u8 = 0x02;
pub const FLOAT: u8 = 0x03;
pub const STRING: u8 = 0x04;
pub const JSON: u8 = 0x05;
pub const ENUM: u8 = 0x06;
pub const XML: u8 = 0x07;
pub const HTML: u8 = 0x08;
pub const TIMEZONE: u8 = 0x09;
pub const UUID: u8 = 0x0A;
pub const DATE: u8 = 0x0B;
pub const DATETIME: u8 = 0x0C;
pub const TIME: u8 = 0x0D;
const LIST: u8 = 0x0E;
const MAP: u8 = 0x0F;
const STRUCTURE: u8 = 0x10;
pub const XML_ELEMENT: u8 = 0x11;
pub const HTML_ELEMENT: u8 = 0x12;

const NAN_BITS: u64 = 0x7FF8_0000_0000_0000;

/// Arrow `LargeBinary` cannot tell string, json, enum, xml, html and timezone apart, so the tag comes from the type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Encoding {
    Scalar(u8),
    List(Box<Encoding>),
    Map(Box<Encoding>, Box<Encoding>),
    /// (child index in the Arrow struct, its encoding), sorted by the element name bytes.
    Struct(Vec<(usize, Encoding)>),
}

/// The encoding of a column of `parsed`, by the type JSON name; an optional type is encoded as its base.
pub fn encoding_of(parsed: &TypeJson) -> Result<Encoding, Error> {
    let missing = |part: Part| Error::MissingPart {
        type_name: parsed.type_.clone(),
        part,
    };

    Ok(match parsed.type_.as_str() {
        "null" => Encoding::Scalar(NULL),
        "boolean" => Encoding::Scalar(BOOLEAN),
        "integer" | "positive_integer" => Encoding::Scalar(INTEGER),
        "float" => Encoding::Scalar(FLOAT),
        "string" | "non_empty_string" | "numeric-string" | "class_string" => Encoding::Scalar(STRING),
        "json" => Encoding::Scalar(JSON),
        "enum" => Encoding::Scalar(ENUM),
        "xml" => Encoding::Scalar(XML),
        "html" => Encoding::Scalar(HTML),
        "timezone" => Encoding::Scalar(TIMEZONE),
        "uuid" => Encoding::Scalar(UUID),
        "date" => Encoding::Scalar(DATE),
        "datetime" => Encoding::Scalar(DATETIME),
        "time" => Encoding::Scalar(TIME),
        "xml_element" => Encoding::Scalar(XML_ELEMENT),
        "html_element" => Encoding::Scalar(HTML_ELEMENT),
        "list" => Encoding::List(Box::new(encoding_of(
            parsed.element().ok_or_else(|| missing(Part::Element))?,
        )?)),
        "map" => Encoding::Map(
            Box::new(encoding_of(parsed.key().ok_or_else(|| missing(Part::Key))?)?),
            Box::new(encoding_of(parsed.value().ok_or_else(|| missing(Part::Value))?)?),
        ),
        "structure_v2" => {
            let mut elements = parsed.fields().iter().enumerate().collect::<Vec<_>>();

            elements.sort_by(|(_, left), (_, right)| left.name.as_bytes().cmp(right.name.as_bytes()));

            Encoding::Struct(
                elements
                    .into_iter()
                    .map(|(child, element)| Ok((child, encoding_of(&element.type_)?)))
                    .collect::<Result<_, Error>>()?,
            )
        }
        "optional" => encoding_of(parsed.base().ok_or_else(|| missing(Part::Base))?)?,
        other => {
            return Err(Error::UnknownType {
                type_name: other.to_owned(),
            })
        }
    })
}

/// `entries` is the reused pool of map entry buffers.
pub fn append_value(out: &mut Vec<u8>, entries: &mut Vec<Vec<u8>>, encoding: &Encoding, array: &dyn Array, row: usize) {
    write(out, entries, 0, encoding, array, row);
}

/// The caller passes the pairs sorted by name bytes.
pub fn append_pair(
    out: &mut Vec<u8>,
    entries: &mut Vec<Vec<u8>>,
    name: &[u8],
    encoding: &Encoding,
    array: &dyn Array,
    row: usize,
) {
    if is_null(array, row) {
        return;
    }

    append_name(out, name);
    write(out, entries, 0, encoding, array, row);
}

pub fn append_name(out: &mut Vec<u8>, name: &[u8]) {
    out.extend_from_slice(&(name.len() as u64).to_le_bytes());
    out.extend_from_slice(name);
}

/// [low, high] of `xxh3_128(encoded)`.
pub fn row_lanes(encoded: &[u8]) -> [u64; 2] {
    let hash = xxh3_128(encoded);

    [hash as u64, (hash >> 64) as u64]
}

/// [low, high] of the first 128 bits of `md5(encoded)`.
pub fn md5_row_lanes(encoded: &[u8]) -> [u64; 2] {
    first_128_bits(&Md5::digest(encoded))
}

/// [low, high] of the first 128 bits of `sha256(encoded)`.
pub fn sha256_row_lanes(encoded: &[u8]) -> [u64; 2] {
    first_128_bits(&Sha256::digest(encoded))
}

fn first_128_bits(digest: &[u8]) -> [u64; 2] {
    let mut first = [0_u8; 16];
    first.copy_from_slice(&digest[..16]);
    let hash = u128::from_be_bytes(first);

    [hash as u64, (hash >> 64) as u64]
}

fn write(
    out: &mut Vec<u8>,
    entries: &mut Vec<Vec<u8>>,
    base: usize,
    encoding: &Encoding,
    array: &dyn Array,
    row: usize,
) {
    if is_null(array, row) {
        return out.push(NULL);
    }

    match encoding {
        Encoding::Scalar(tag) => write_scalar(out, *tag, array, row),
        Encoding::List(element) => {
            let list = array.as_list::<i32>();
            let values = list.value(row);

            out.push(LIST);
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());

            for index in 0..values.len() {
                write(out, entries, base, element, values.as_ref(), index);
            }
        }
        Encoding::Map(key, value) => {
            let map = array.as_map();
            let start = map.value_offsets()[row] as usize;
            let count = map.value_offsets()[row + 1] as usize - start;

            while entries.len() < base + count {
                entries.push(Vec::new());
            }

            for index in 0..count {
                let mut entry = std::mem::take(&mut entries[base + index]);

                entry.clear();
                write(
                    &mut entry,
                    entries,
                    base + count,
                    key,
                    map.keys().as_ref(),
                    start + index,
                );
                write(
                    &mut entry,
                    entries,
                    base + count,
                    value,
                    map.values().as_ref(),
                    start + index,
                );
                entries[base + index] = entry;
            }

            entries[base..base + count].sort_unstable();
            out.push(MAP);
            out.extend_from_slice(&(count as u64).to_le_bytes());

            for entry in &entries[base..base + count] {
                out.extend_from_slice(entry);
            }
        }
        Encoding::Struct(elements) => {
            let structure = array.as_struct();

            out.push(STRUCTURE);
            let count_at = out.len();
            out.extend_from_slice(&0_u64.to_le_bytes());
            let mut present = 0_u64;

            for (child, element) in elements {
                let column = structure.column(*child);

                if is_null(column.as_ref(), row) {
                    continue;
                }

                append_name(out, structure.fields()[*child].name().as_bytes());
                write(out, entries, base, element, column.as_ref(), row);
                present += 1;
            }

            out[count_at..count_at + 8].copy_from_slice(&present.to_le_bytes());
        }
    }
}

// a NullArray has no validity buffer, so is_null() is false for its rows
fn is_null(array: &dyn Array, row: usize) -> bool {
    array.data_type() == &DataType::Null || array.is_null(row)
}

fn write_scalar(out: &mut Vec<u8>, tag: u8, array: &dyn Array, row: usize) {
    out.push(tag);

    match tag {
        NULL => {}
        BOOLEAN => out.push(u8::from(array.as_boolean().value(row))),
        INTEGER => out.extend_from_slice(&array.as_primitive::<Int64Type>().value(row).to_le_bytes()),
        FLOAT => {
            let value = array.as_primitive::<Float64Type>().value(row);
            let bits = if value.is_nan() {
                NAN_BITS
            } else if value == 0.0 {
                0.0_f64.to_bits()
            } else {
                value.to_bits()
            };

            out.extend_from_slice(&bits.to_le_bytes());
        }
        UUID => out.extend_from_slice(array.as_fixed_size_binary().value(row)),
        DATE => out.extend_from_slice(&array.as_primitive::<Date32Type>().value(row).to_le_bytes()),
        DATETIME => out.extend_from_slice(
            &array
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_le_bytes(),
        ),
        TIME => out.extend_from_slice(&array.as_primitive::<DurationMicrosecondType>().value(row).to_le_bytes()),
        XML_ELEMENT | HTML_ELEMENT => append_text(out, element_markup(array.as_binary::<i64>().value(row))),
        _ => append_text(out, array.as_binary::<i64>().value(row)),
    }
}

fn append_text(out: &mut Vec<u8>, text: &[u8]) {
    out.extend_from_slice(&(text.len() as u64).to_le_bytes());
    out.extend_from_slice(text);
}

/// `ElementPosition::markupOf()`: `\x01 path \0 length \0 markup document`; bare markup is its own markup.
fn element_markup(physical: &[u8]) -> &[u8] {
    positioned_markup(physical).unwrap_or(physical)
}

fn positioned_markup(physical: &[u8]) -> Option<&[u8]> {
    let rest = physical.strip_prefix(b"\x01")?;
    let path_end = rest.iter().position(|&byte| byte == 0)?;
    let rest = &rest[path_end + 1..];
    let length_end = rest.iter().position(|&byte| byte == 0)?;
    let length: usize = std::str::from_utf8(&rest[..length_end]).ok()?.parse().ok()?;

    rest.get(length_end + 1..length_end + 1 + length)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{Int64Builder, LargeBinaryBuilder, MapBuilder};
    use arrow_array::{
        ArrayRef, BooleanArray, Date32Array, DurationMicrosecondArray, FixedSizeBinaryArray, Float64Array, Int64Array,
        LargeBinaryArray, ListArray, NullArray, StructArray, TimestampMicrosecondArray,
    };
    use arrow_buffer::NullBuffer;
    use arrow_schema::{Field, Fields};
    use flow_batch_frame::kind::parse_type;
    use std::sync::Arc;

    fn encoded(type_json: &str, array: &ArrayRef) -> Vec<String> {
        let encoding = encoding_of(&parse_type(type_json.as_bytes()).unwrap()).unwrap();
        let mut entries = Vec::new();

        (0..array.len())
            .map(|row| {
                let mut out = Vec::new();
                append_value(&mut out, &mut entries, &encoding, array.as_ref(), row);
                hex(&out)
            })
            .collect()
    }

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    fn binary(values: &[&[u8]]) -> ArrayRef {
        Arc::new(LargeBinaryArray::from_vec(values.to_vec()))
    }

    fn strings(values: &[Option<&str>]) -> ArrayRef {
        Arc::new(LargeBinaryArray::from_opt_vec(
            values.iter().map(|value| value.map(str::as_bytes)).collect(),
        ))
    }

    fn ints(values: &[Option<i64>]) -> ArrayRef {
        Arc::new(Int64Array::from(values.to_vec()))
    }

    fn structure(fields: Vec<(&str, ArrayRef)>, nulls: Option<NullBuffer>) -> ArrayRef {
        let (fields, columns): (Vec<_>, Vec<_>) = fields
            .into_iter()
            .map(|(name, column)| (Field::new(name, column.data_type().clone(), true), column))
            .unzip();

        Arc::new(StructArray::try_new(Fields::from(fields), columns, nulls).unwrap())
    }

    fn string_map(entries: &[&[(&str, i64)]]) -> ArrayRef {
        let mut builder = MapBuilder::new(None, LargeBinaryBuilder::new(), Int64Builder::new());

        for entry in entries {
            for (key, value) in *entry {
                builder.keys().append_value(key.as_bytes());
                builder.values().append_value(*value);
            }
            builder.append(true).unwrap();
        }

        Arc::new(builder.finish())
    }

    fn positioned(path: &str, markup: &str, document: &str) -> Vec<u8> {
        [
            b"\x01".as_slice(),
            path.as_bytes(),
            b"\0",
            markup.len().to_string().as_bytes(),
            b"\0",
            markup.as_bytes(),
            document.as_bytes(),
        ]
        .concat()
    }

    const INTEGER_JSON: &str = r#"{"type":"integer"}"#;

    #[test]
    fn null_in_the_columns_form_is_tag_00() {
        assert_eq!(
            encoded(r#"{"type":"null"}"#, &(Arc::new(NullArray::new(1)) as ArrayRef)),
            ["00"]
        );
    }

    #[test]
    fn boolean_is_tag_01() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), Some(false), None]));

        assert_eq!(
            encoded(r#"{"type":"optional","base":{"type":"boolean"}}"#, &array),
            ["0101", "0100", "00"]
        );
    }

    #[test]
    fn integer_is_tag_02() {
        assert_eq!(
            encoded(INTEGER_JSON, &ints(&[Some(1), Some(-1)])),
            ["020100000000000000", "02ffffffffffffffff"]
        );
    }

    #[test]
    fn float_is_tag_03() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![1.0, 1.5]));

        assert_eq!(
            encoded(r#"{"type":"float"}"#, &array),
            ["03000000000000f03f", "03000000000000f83f"]
        );
    }

    #[test]
    fn string_is_tag_04() {
        assert_eq!(
            encoded(r#"{"type":"string"}"#, &strings(&[Some("1"), Some("")])),
            ["04010000000000000031", "040000000000000000"]
        );
    }

    #[test]
    fn json_is_tag_05() {
        assert_eq!(
            encoded(r#"{"type":"json"}"#, &strings(&[Some(r#"{"a":1}"#)])),
            ["0507000000000000007b2261223a317d"]
        );
    }

    #[test]
    fn enum_is_tag_06() {
        assert_eq!(
            encoded(r#"{"type":"enum","class":"E"}"#, &strings(&[Some("one")])),
            ["0603000000000000006f6e65"]
        );
    }

    #[test]
    fn xml_is_tag_07() {
        assert_eq!(
            encoded(
                r#"{"type":"xml"}"#,
                &strings(&[Some("<?xml version=\"1.0\"?>\n<a/>\n")])
            ),
            ["071b000000000000003c3f786d6c2076657273696f6e3d22312e30223f3e0a3c612f3e0a"]
        );
    }

    #[test]
    fn html_is_tag_08() {
        assert_eq!(
            encoded(
                r#"{"type":"html"}"#,
                &strings(&[Some("<!DOCTYPE html><html><head></head><body><p>a</p></body></html>")])
            ),
            ["083e000000000000003c21444f43545950452068746d6c3e3c68746d6c3e3c686561643e3c2f686561643e3c626f64793e3c703e613c2f703e3c2f626f64793e3c2f68746d6c3e"]
        );
    }

    #[test]
    fn timezone_is_tag_09() {
        assert_eq!(
            encoded(r#"{"type":"timezone"}"#, &strings(&[Some("UTC")])),
            ["090300000000000000555443"]
        );
    }

    #[test]
    fn uuid_is_tag_0a() {
        let mut bytes = [0_u8; 16];
        bytes[15] = 1;
        let array: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter([bytes].into_iter()).unwrap());

        assert_eq!(
            encoded(r#"{"type":"uuid"}"#, &array),
            ["0a00000000000000000000000000000001"]
        );
    }

    #[test]
    fn date_is_tag_0b() {
        let array: ArrayRef = Arc::new(Date32Array::from(vec![1]));

        assert_eq!(encoded(r#"{"type":"date"}"#, &array), ["0b01000000"]);
    }

    #[test]
    fn datetime_is_tag_0c() {
        let array: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![1_000_002]).with_timezone("UTC"));

        assert_eq!(
            encoded(r#"{"type":"datetime","zone":"UTC"}"#, &array),
            ["0c42420f0000000000"]
        );
    }

    #[test]
    fn time_is_tag_0d() {
        let array: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![1_000_000, -1_000_000]));

        assert_eq!(
            encoded(r#"{"type":"time"}"#, &array),
            ["0d40420f0000000000", "0dc0bdf0ffffffffff"]
        );
    }

    #[test]
    fn list_with_a_null_element_is_tag_0e() {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![
            Some(1),
            None,
        ])]));

        assert_eq!(
            encoded(
                r#"{"type":"list","element":{"type":"optional","base":{"type":"integer"}}}"#,
                &array
            ),
            ["0e020000000000000002010000000000000000"]
        );
    }

    const STRING_INTEGER_MAP: &str = r#"{"type":"map","key":{"type":"string"},"value":{"type":"integer"}}"#;

    #[test]
    fn map_is_tag_0f_with_entries_sorted_by_bytes() {
        assert_eq!(
            encoded(
                STRING_INTEGER_MAP,
                &string_map(&[&[("b", 2), ("a", 1)], &[("a", 1), ("b", 2)]])
            ),
            [
                "0f02000000000000000401000000000000006102010000000000000004010000000000000062020200000000000000",
                "0f02000000000000000401000000000000006102010000000000000004010000000000000062020200000000000000"
            ]
        );
    }

    #[test]
    fn map_string_key_one_takes_tag_04() {
        assert_eq!(
            encoded(STRING_INTEGER_MAP, &string_map(&[&[("1", 1)]])),
            ["0f010000000000000004010000000000000031020100000000000000"]
        );
    }

    #[test]
    fn map_with_integer_keys_takes_tag_02_for_the_keys() {
        let mut builder = MapBuilder::new(None, Int64Builder::new(), LargeBinaryBuilder::new());
        builder.keys().append_value(1);
        builder.values().append_value(b"a");
        builder.append(true).unwrap();
        let array: ArrayRef = Arc::new(builder.finish());

        assert_eq!(
            encoded(
                r#"{"type":"map","key":{"type":"integer"},"value":{"type":"string"}}"#,
                &array
            ),
            ["0f010000000000000002010000000000000004010000000000000061"]
        );
    }

    #[test]
    fn nested_structure_is_tag_10_with_elements_by_name() {
        let inner = structure(
            vec![("x", strings(&[Some("y"), Some("y")]))],
            Some(NullBuffer::from(vec![true, false])),
        );
        let array = structure(vec![("b", ints(&[Some(1), Some(1)])), ("a", inner)], None);

        assert_eq!(
            encoded(
                r#"{"type":"structure_v2","fields":[
                    {"name":"b","type":{"type":"integer"}},
                    {"name":"a","type":{"type":"optional","base":{"type":"structure_v2","fields":[{"name":"x","type":{"type":"string"}}]}}}
                ]}"#,
                &array
            ),
            [
                "10020000000000000001000000000000006110010000000000000001000000000000007804010000000000000079010000000000000062020100000000000000",
                "100100000000000000010000000000000062020100000000000000"
            ]
        );
    }

    #[test]
    fn null_structure_element_equals_missing_optional_element() {
        let array = structure(vec![("a", ints(&[Some(1)])), ("b", strings(&[None]))], None);

        assert_eq!(
            encoded(
                r#"{"type":"structure_v2","fields":[
                    {"name":"a","type":{"type":"integer"}},
                    {"name":"b","type":{"type":"optional","base":{"type":"string"}},"optional":true}
                ]}"#,
                &array
            ),
            ["100100000000000000010000000000000061020100000000000000"]
        );
    }

    #[test]
    fn structure_element_order_does_not_count() {
        let left = structure(vec![("a", ints(&[Some(1)])), ("b", strings(&[Some("x")]))], None);
        let right = structure(vec![("b", strings(&[Some("x")])), ("a", ints(&[Some(1)]))], None);

        assert_eq!(
            encoded(
                r#"{"type":"structure_v2","fields":[{"name":"a","type":{"type":"integer"}},{"name":"b","type":{"type":"string"}}]}"#,
                &left
            ),
            encoded(
                r#"{"type":"structure_v2","fields":[{"name":"b","type":{"type":"string"}},{"name":"a","type":{"type":"integer"}}]}"#,
                &right
            )
        );
    }

    #[test]
    fn renamed_nested_element_differs() {
        let left = structure(vec![("s", structure(vec![("a", ints(&[Some(1)]))], None))], None);
        let right = structure(vec![("s", structure(vec![("b", ints(&[Some(1)]))], None))], None);

        assert_ne!(
            encoded(
                r#"{"type":"structure_v2","fields":[{"name":"s","type":{"type":"structure_v2","fields":[{"name":"a","type":{"type":"integer"}}]}}]}"#,
                &left
            ),
            encoded(
                r#"{"type":"structure_v2","fields":[{"name":"s","type":{"type":"structure_v2","fields":[{"name":"b","type":{"type":"integer"}}]}}]}"#,
                &right
            )
        );
    }

    #[test]
    fn xml_element_is_tag_11_with_its_markup_wherever_it_lives() {
        let array = binary(&[
            &positioned(
                "0",
                "<a x=\"1\"></a>",
                "<?xml version=\"1.0\"?>\n<div><a x=\"1\"/></div>\n",
            ),
            &positioned(
                "1",
                "<a x=\"1\"></a>",
                "<?xml version=\"1.0\"?>\n<section><b/><a x=\"1\"/></section>\n",
            ),
            &positioned("0", "<a></a>", "<?xml version=\"1.0\"?>\n<div><a/></div>\n"),
            b"<a></a>",
        ]);

        assert_eq!(
            encoded(r#"{"type":"xml_element"}"#, &array),
            [
                "110d000000000000003c6120783d2231223e3c2f613e",
                "110d000000000000003c6120783d2231223e3c2f613e",
                "1107000000000000003c613e3c2f613e",
                "1107000000000000003c613e3c2f613e"
            ]
        );
    }

    #[test]
    fn xml_element_differs_from_xml_document_with_the_same_markup() {
        let array = strings(&[Some("<a></a>")]);

        assert_ne!(
            encoded(r#"{"type":"xml"}"#, &array),
            encoded(r#"{"type":"xml_element"}"#, &array)
        );
    }

    #[test]
    fn html_element_is_tag_12_with_its_markup_wherever_it_lives() {
        let array = binary(&[
            &positioned("0", "<p>a</p>", "<div><p>a</p></div>"),
            &positioned("1", "<p>a</p>", "<section><b></b><p>a</p></section>"),
            b"<p>a</p>",
        ]);

        assert_eq!(
            encoded(r#"{"type":"html_element"}"#, &array),
            [
                "1208000000000000003c703e613c2f703e",
                "1208000000000000003c703e613c2f703e",
                "1208000000000000003c703e613c2f703e"
            ]
        );
    }

    #[test]
    fn same_text_of_different_types_differs() {
        let mut all = vec![
            encoded(INTEGER_JSON, &ints(&[Some(1)])),
            encoded(
                r#"{"type":"float"}"#,
                &(Arc::new(Float64Array::from(vec![1.0])) as ArrayRef),
            ),
            encoded(r#"{"type":"string"}"#, &strings(&[Some("1")])),
            encoded(r#"{"type":"json"}"#, &strings(&[Some("[1]")])),
            encoded(r#"{"type":"enum","class":"E"}"#, &strings(&[Some("one")])),
        ];
        all.sort();
        all.dedup();

        assert_eq!(all.len(), 5);
    }

    #[test]
    fn null_differs_from_empty_string() {
        assert_eq!(
            encoded(
                r#"{"type":"optional","base":{"type":"string"}}"#,
                &strings(&[None, Some("")])
            ),
            ["00", "040000000000000000"]
        );
    }

    #[test]
    fn negative_zero_equals_zero_and_every_nan_is_one_value() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 0.0, f64::NAN, -f64::NAN]));

        assert_eq!(
            encoded(r#"{"type":"float"}"#, &array),
            [
                "030000000000000000",
                "030000000000000000",
                "03000000000000f87f",
                "03000000000000f87f"
            ]
        );
    }

    #[test]
    fn float_nan_differs_from_string_nan() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![f64::NAN]));

        assert_ne!(
            encoded(r#"{"type":"float"}"#, &array),
            encoded(r#"{"type":"string"}"#, &strings(&[Some("NaN")]))
        );
    }

    #[test]
    fn the_same_instant_is_one_value_whatever_the_zone_of_the_type() {
        let array: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![1_767_225_600_000_000]).with_timezone("UTC"));

        assert_eq!(
            encoded(r#"{"type":"datetime","zone":"Europe/Warsaw"}"#, &array),
            encoded(r#"{"type":"datetime","zone":"UTC"}"#, &array)
        );
        assert_eq!(
            encoded(r#"{"type":"datetime","zone":"UTC"}"#, &array),
            ["0c0040204648470600"]
        );
    }

    #[test]
    fn date_differs_from_datetime_at_midnight() {
        let date: ArrayRef = Arc::new(Date32Array::from(vec![1]));
        let datetime: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![86_400_000_000]).with_timezone("UTC"));

        assert_eq!(encoded(r#"{"type":"date"}"#, &date), ["0b01000000"]);
        assert_eq!(
            encoded(r#"{"type":"datetime","zone":"UTC"}"#, &datetime),
            ["0c0060d71d14000000"]
        );
    }

    #[test]
    fn list_order_counts() {
        let array: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(2), Some(1)]),
        ]));
        let encoded = encoded(r#"{"type":"list","element":{"type":"integer"}}"#, &array);

        assert_ne!(encoded[0], encoded[1]);
    }

    #[test]
    fn json_whitespace_counts() {
        let encoded = encoded(
            r#"{"type":"json"}"#,
            &strings(&[Some(r#"{"a":1}"#), Some(r#"{"a": 1}"#)]),
        );

        assert_ne!(encoded[0], encoded[1]);
    }

    #[test]
    fn columns_form_concatenates_the_columns_in_list_order() {
        let a = ints(&[Some(1)]);
        let b: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let integer = Encoding::Scalar(INTEGER);
        let boolean = Encoding::Scalar(BOOLEAN);
        let mut entries = Vec::new();
        let mut forward = Vec::new();
        let mut backward = Vec::new();

        append_value(&mut forward, &mut entries, &integer, a.as_ref(), 0);
        append_value(&mut forward, &mut entries, &boolean, b.as_ref(), 0);
        append_value(&mut backward, &mut entries, &boolean, b.as_ref(), 0);
        append_value(&mut backward, &mut entries, &integer, a.as_ref(), 0);

        assert_eq!(hex(&forward), "0201000000000000000101");
        assert_eq!(hex(&backward), "0101020100000000000000");
    }

    #[test]
    fn row_form_writes_name_parts_and_leaves_out_null_pairs() {
        let a = ints(&[Some(1)]);
        let b: ArrayRef = Arc::new(BooleanArray::from(vec![true]));
        let missing = ints(&[None]);
        let null: ArrayRef = Arc::new(NullArray::new(1));
        let mut entries = Vec::new();
        let mut row = Vec::new();

        append_pair(&mut row, &mut entries, b"a", &Encoding::Scalar(INTEGER), a.as_ref(), 0);
        append_pair(&mut row, &mut entries, b"b", &Encoding::Scalar(BOOLEAN), b.as_ref(), 0);
        append_pair(
            &mut row,
            &mut entries,
            b"c",
            &Encoding::Scalar(INTEGER),
            missing.as_ref(),
            0,
        );
        append_pair(&mut row, &mut entries, b"d", &Encoding::Scalar(NULL), null.as_ref(), 0);

        assert_eq!(hex(&row), "0100000000000000610201000000000000000100000000000000620101");
    }

    #[test]
    fn row_lanes_are_the_low_and_high_half_of_xxh3_128() {
        assert_eq!(
            row_lanes(b"abc"),
            [8_696_274_497_037_089_104_u64, 481_984_899_792_396_677_u64]
        );
    }

    #[test]
    fn md5_and_sha256_row_lanes_are_the_first_128_bits_of_the_digest() {
        assert_eq!(md5_row_lanes(b"abc"), [0xd6963f7d28e17f72_u64, 0x900150983cd24fb0_u64]);
        assert_eq!(
            sha256_row_lanes(b"abc"),
            [0x414140de5dae2223_u64, 0xba7816bf8f01cfea_u64]
        );
    }

    #[test]
    fn nested_maps_share_the_entry_pool_without_mixing_entries() {
        let inner = string_map(&[&[("b", 2), ("a", 1)]]);
        let mut entries = vec![b"stale".to_vec(); 3];
        let encoding = encoding_of(&parse_type(STRING_INTEGER_MAP.as_bytes()).unwrap()).unwrap();
        let mut out = Vec::new();

        append_value(&mut out, &mut entries, &encoding, inner.as_ref(), 0);

        assert_eq!(
            hex(&out),
            "0f02000000000000000401000000000000006102010000000000000004010000000000000062020200000000000000"
        );
    }

    #[test]
    fn an_unknown_type_is_refused() {
        assert!(matches!(
            encoding_of(&parse_type(br#"{"type":"mixed"}"#).unwrap()),
            Err(Error::UnknownType { .. })
        ));
    }
}
