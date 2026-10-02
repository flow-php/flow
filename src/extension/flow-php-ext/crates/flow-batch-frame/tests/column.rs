mod common;

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int64Builder, MapBuilder, MapFieldNames};
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, FixedSizeBinaryArray, Float64Array, Int64Array, ListArray,
    NullArray, StructArray, TimestampMicrosecondArray,
};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use common::{hex, unhex};
use flow_batch_frame::column::{decode, encode};
use flow_batch_frame::error::{Error, Of, Role, Values};
use flow_batch_frame::kind::{data_type, kind, Kind};

fn kind_of(type_json: &str) -> Kind {
    kind(type_json.as_bytes()).expect("a valid type JSON")
}

fn encoded_hex(data: &ArrayData, kind: &Kind) -> Vec<String> {
    encode(data, kind)
        .expect("an encodable column")
        .iter()
        .map(|buffer| hex(buffer))
        .collect()
}

fn decoded(kind: &Kind, buffers: &[&str], len: u32, null_count: u32) -> Result<ArrayData, Error> {
    let buffers: Vec<Vec<u8>> = buffers.iter().map(|buffer| unhex(buffer)).collect();
    let buffers: Vec<&[u8]> = buffers.iter().map(Vec::as_slice).collect();

    decode(kind, &buffers, len, null_count)
}

fn raw(kind: &Kind, buffers: &[&[u8]], len: u32, null_count: u32) -> Error {
    decode(kind, buffers, len, null_count).expect_err("a refused column")
}

fn structure(fields: Vec<(&str, ArrayRef)>, nulls: Option<Vec<bool>>) -> ArrayData {
    let arrow_fields = fields
        .iter()
        .map(|(name, array)| Arc::new(arrow_schema::Field::new(*name, array.data_type().clone(), true)))
        .collect::<Vec<_>>();

    StructArray::try_new(
        arrow_fields.into(),
        fields.into_iter().map(|(_, array)| array).collect(),
        nulls.map(NullBuffer::from),
    )
    .expect("a valid struct")
    .into_data()
}

/// ColumnEncodeTest.php:40-105, and each case decodes back to itself.
fn cases() -> Vec<(&'static str, Kind, ArrayData, Vec<&'static str>)> {
    let list = kind_of("{\"type\":\"list\",\"element\":{\"type\":\"integer\"}}");
    let list_rows = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(vec![Some(1)]),
        Some(vec![Some(2), Some(3)]),
        None,
    ]);
    let mut map = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        BinaryBuilder::new(),
        Int64Builder::new(),
    );
    map.keys().append_value(b"a");
    map.values().append_value(1);
    map.append(true).expect("a map row");

    vec![
        (
            "integer",
            kind_of("{\"type\":\"integer\"}"),
            Int64Array::from(vec![1, 2]).into_data(),
            vec!["", "01000000000000000200000000000000"],
        ),
        (
            "nullable string",
            kind_of("{\"type\":\"string\"}"),
            BinaryArray::from(vec![Some(b"ab".as_ref()), None]).into_data(),
            vec!["01", "000000000200000002000000", "6162"],
        ),
        (
            "float",
            kind_of("{\"type\":\"float\"}"),
            Float64Array::from(vec![1.5]).into_data(),
            vec!["", "000000000000f83f"],
        ),
        (
            "date before the epoch",
            kind_of("{\"type\":\"date\"}"),
            Date32Array::from(vec![-3]).into_data(),
            vec!["", "fdffffff"],
        ),
        (
            "boolean",
            kind_of("{\"type\":\"boolean\"}"),
            BooleanArray::from(vec![true, false, true]).into_data(),
            vec!["", "05"],
        ),
        (
            "nullable boolean with a null slot at 0",
            kind_of("{\"type\":\"boolean\"}"),
            BooleanArray::from(vec![Some(true), None, Some(true)]).into_data(),
            vec!["05", "05"],
        ),
        (
            "uuid",
            kind_of("{\"type\":\"uuid\"}"),
            FixedSizeBinaryArray::try_from_iter(vec![unhex("6c2f1d4e8b3a4c5d9e6f0a1b2c3d4e5f")].into_iter())
                .expect("16 bytes")
                .into_data(),
            vec!["", "6c2f1d4e8b3a4c5d9e6f0a1b2c3d4e5f"],
        ),
        (
            "list with a null slot",
            list.clone(),
            ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
                Some(vec![Some(1)]),
                None,
                Some(vec![Some(2), Some(3)]),
            ])
            .into_data(),
            vec!["05", "00000000010000000100000003000000", "", "010000000000000002000000000000000300000000000000"],
        ),
        (
            "map",
            kind_of("{\"type\":\"map\",\"key\":{\"type\":\"string\"},\"value\":{\"type\":\"integer\"}}"),
            map.finish().into_data(),
            vec!["", "0000000001000000", "", "", "0000000001000000", "61", "", "0100000000000000"],
        ),
        (
            "structure: the child bit is 0 under a null parent",
            kind_of("{\"type\":\"structure_v2\",\"fields\":[{\"name\":\"x\",\"type\":{\"type\":\"integer\"}}]}"),
            structure(vec![("x", Arc::new(Int64Array::from(vec![1, 7])))], Some(vec![true, false])),
            vec!["01", "01", "01000000000000000000000000000000"],
        ),
        (
            "a null column has no buffers",
            kind_of("{\"type\":\"null\"}"),
            NullArray::new(3).into_data(),
            vec![],
        ),
        (
            "a null element of a structure has no buffers",
            kind_of(
                "{\"type\":\"structure_v2\",\"fields\":[{\"name\":\"x\",\"type\":{\"type\":\"integer\"}},{\"name\":\"n\",\"type\":{\"type\":\"null\"}}]}",
            ),
            structure(
                vec![("x", Arc::new(Int64Array::from(vec![1]))), ("n", Arc::new(NullArray::new(1)))],
                None,
            ),
            vec!["", "", "0100000000000000"],
        ),
        (
            "an optional null element of a structure has no buffers",
            kind_of(
                "{\"type\":\"structure_v2\",\"fields\":[{\"name\":\"x\",\"type\":{\"type\":\"integer\"}},{\"name\":\"n\",\"type\":{\"type\":\"optional\",\"base\":{\"type\":\"null\"}}}]}",
            ),
            structure(
                vec![("x", Arc::new(Int64Array::from(vec![1]))), ("n", Arc::new(NullArray::new(1)))],
                None,
            ),
            vec!["", "", "0100000000000000"],
        ),
        (
            "a slice re-bases offsets and re-packs validity from bit 0",
            list,
            list_rows.slice(1, 2).into_data(),
            vec!["01", "000000000200000002000000", "", "02000000000000000300000000000000"],
        ),
    ]
}

#[test]
fn encodes_the_column_encode_test_cases() {
    for (name, kind, data, expected) in cases() {
        assert_eq!(expected, encoded_hex(&data, &kind), "{name}");
    }
}

#[test]
fn decodes_the_column_encode_test_cases_back() {
    for (name, kind, data, expected) in cases() {
        let null_count = match kind {
            Kind::Null => data.len() as u32,
            _ => data.nulls().map_or(0, |nulls| nulls.null_count() as u32),
        };
        let decoded = decoded(&kind, &expected, data.len() as u32, null_count).expect(name);

        assert_eq!(&data_type(&kind), decoded.data_type(), "{name}");
        assert_eq!(expected, encoded_hex(&decoded, &kind), "{name}");
    }
}

#[test]
fn a_selection_under_a_null_list_slot_is_compacted() {
    let kind = kind_of("{\"type\":\"list\",\"element\":{\"type\":\"string\"}}");
    let strings = BinaryArray::from(vec![b"a".as_ref(), b"bc", b"d"]);
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 1, 2, 3].into());
    let field = Arc::new(arrow_schema::Field::new("item", arrow_schema::DataType::Binary, true));
    let list = ListArray::new(
        field,
        offsets,
        Arc::new(strings),
        Some(NullBuffer::from(vec![true, false, true])),
    );

    assert_eq!(
        vec![
            "05",
            "00000000010000000100000002000000",
            "",
            "000000000100000002000000",
            "6164"
        ],
        encoded_hex(&list.into_data(), &kind),
    );
}

#[test]
fn bytes_that_are_not_utf8_round_trip() {
    let kind = kind_of("{\"type\":\"string\"}");
    let data = BinaryArray::from(vec![b"a\xFFb".as_ref()]).into_data();
    let buffers = encoded_hex(&data, &kind);
    let buffers: Vec<&str> = buffers.iter().map(String::as_str).collect();

    assert_eq!(vec!["", "0000000003000000", "61ff62"], buffers);
    assert_eq!(
        b"a\xFFb",
        decoded(&kind, &buffers, 1, 0).expect("a binary column").buffers()[1].as_slice(),
    );
}

#[test]
fn encode_refuses_a_foreign_data_type() {
    let kind = kind_of("{\"type\":\"datetime\",\"zone\":\"UTC\"}");

    assert!(matches!(
        encode(&TimestampMicrosecondArray::from(vec![1]).into_data(), &kind),
        Err(Error::DataTypeMismatch { .. }),
    ));
}

/// PhpBackendTest.php:454-556.
#[test]
fn decode_refuses_corrupt_buffers() {
    let int = kind_of("{\"type\":\"integer\"}");
    let string = kind_of("{\"type\":\"string\"}");
    let map = kind_of("{\"type\":\"map\",\"key\":{\"type\":\"string\"},\"value\":{\"type\":\"integer\"}}");
    let null = kind_of("{\"type\":\"null\"}");

    assert!(matches!(raw(&int, &[b""], 1, 0), Error::BuffersExhausted));
    assert!(matches!(
        raw(&int, &[b"", b"\x01\x00"], 1, 0),
        Error::ValuesLength {
            values: Values::Int64,
            bytes: 2,
            expected: 8,
            rows: 1
        },
    ));
    assert!(matches!(
        raw(&string, &[b"", b"\x01\x00\x00\x00\x02\x00\x00\x00", b"ab"], 1, 0),
        Error::OffsetsStart { of: Of::Utf8, first: 1 },
    ));
    assert!(matches!(
        raw(
            &string,
            &[b"", b"\x00\x00\x00\x00\x02\x00\x00\x00\x01\x00\x00\x00", b"ab"],
            2,
            0
        ),
        Error::OffsetsNotMonotonic {
            of: Of::Utf8,
            index: 2,
            previous: 2,
            current: 1
        },
    ));
    assert!(matches!(
        raw(&string, &[b"", b"\x00\x00\x00\x00\x03\x00\x00\x00", b"ab"], 1, 0),
        Error::Utf8DataLength {
            bytes: 2,
            last_offset: 3
        },
    ));
    assert!(matches!(
        raw(
            &map,
            &[
                b"",
                b"\x00\x00\x00\x00\x00\x00\x00\x00",
                b"\x01",
                b"",
                b"\x00\x00\x00\x00",
                b"",
                b"",
                b""
            ],
            1,
            0
        ),
        Error::MapEntriesValidity { bytes: 1 },
    ));
    assert!(matches!(
        raw(&int, &[b"\x01", &[0; 16]], 2, 0),
        Error::NullCountMismatch {
            declared: 0,
            derived: 1,
            rows: 2
        },
    ));
    assert!(matches!(
        raw(&int, &[b"", b""], 9, 0),
        Error::ValuesLength {
            values: Values::Int64,
            bytes: 0,
            expected: 72,
            rows: 9
        },
    ));
    assert!(matches!(
        raw(&null, &[], 2, 1),
        Error::NullKindNullCount { rows: 2, null_count: 1 }
    ));
    assert!(matches!(
        raw(&int, &[b"", b"\x01\x00\x00\x00\x00\x00\x00\x00", b""], 1, 0),
        Error::LeftoverBuffers { count: 1 },
    ));
    assert!(matches!(
        raw(&int, &[b"\x01", &[0; 72]], 9, 0),
        Error::ValidityTooShort { bytes: 1, rows: 9 },
    ));
    assert!(matches!(
        raw(&string, &[b"", b"\x00\x00", b""], 1, 0),
        Error::OffsetsLength {
            of: Of::Utf8,
            bytes: 2,
            expected: 8,
            rows: 1
        },
    ));
}

/// ColumnDecoderTest.php: a list element or map value holds nulls only when optional or of the null kind, a map key
/// never, a structure child always may.
#[test]
fn decode_refuses_nested_nulls_where_the_type_forbids_them() {
    let one_null_child = [b"".as_ref(), b"\x00\x00\x00\x00\x01\x00\x00\x00", b"\x00", &[0; 8]];

    assert!(matches!(
        raw(
            &kind_of("{\"type\":\"list\",\"element\":{\"type\":\"integer\"}}"),
            &one_null_child,
            1,
            0
        ),
        Error::NestedNulls {
            node: 1,
            role: Role::ListElement,
            nulls: 1
        },
    ));
    assert!(decode(
        &kind_of("{\"type\":\"list\",\"element\":{\"type\":\"optional\",\"base\":{\"type\":\"integer\"}}}"),
        &one_null_child,
        1,
        0,
    )
    .is_ok());
    assert!(decode(
        &kind_of("{\"type\":\"list\",\"element\":{\"type\":\"null\"}}"),
        &[b"".as_ref(), b"\x00\x00\x00\x00\x02\x00\x00\x00"],
        1,
        0,
    )
    .is_ok());

    let entries = |key_validity: &'static [u8], value_validity: &'static [u8]| -> [&'static [u8]; 7] {
        [
            b"",
            b"\x00\x00\x00\x00\x01\x00\x00\x00",
            b"",
            key_validity,
            &[0; 8],
            value_validity,
            &[0; 8],
        ]
    };
    let map = kind_of("{\"type\":\"map\",\"key\":{\"type\":\"integer\"},\"value\":{\"type\":\"integer\"}}");
    let optional_values =
        kind_of("{\"type\":\"map\",\"key\":{\"type\":\"integer\"},\"value\":{\"type\":\"optional\",\"base\":{\"type\":\"integer\"}}}");

    assert!(matches!(
        raw(&map, &entries(b"\x00", b""), 1, 0),
        Error::NestedNulls {
            node: 2,
            role: Role::MapKey,
            nulls: 1
        },
    ));
    assert!(matches!(
        raw(&map, &entries(b"", b"\x00"), 1, 0),
        Error::NestedNulls {
            node: 3,
            role: Role::MapValue,
            nulls: 1
        },
    ));
    assert!(decode(&optional_values, &entries(b"", b"\x00"), 1, 0).is_ok());

    let structure =
        kind_of("{\"type\":\"structure_v2\",\"fields\":[{\"name\":\"x\",\"type\":{\"type\":\"integer\"}}]}");

    assert!(decode(&structure, &[b"".as_ref(), b"\x00", &[0; 8]], 1, 0).is_ok());
}
