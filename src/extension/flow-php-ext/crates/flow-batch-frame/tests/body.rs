mod common;

use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, Int64Builder, MapBuilder, MapFieldNames};
use arrow_array::types::Int64Type;
use arrow_array::{Array, BinaryArray, Int64Array, ListArray, NullArray, StructArray};
use arrow_buffer::NullBuffer;
use arrow_data::ArrayData;
use common::{hex, unhex};
use flow_batch_frame::body::{decode_body, encode_body};
use flow_batch_frame::error::Error;
use flow_batch_frame::kind::{kind, Kind};

fn kind_of(type_json: &str) -> Kind {
    kind(type_json.as_bytes()).expect("a valid type JSON")
}

/// FrameContext::directory(): the counts, then every node and extent word.
fn directory(body: &[u8]) -> Vec<u32> {
    let word =
        |i: usize| u32::from_le_bytes(body[i * 4..i * 4 + 4].try_into().expect("4-byte slice"));

    (0..3 + 2 * (word(1) + word(2)) as usize)
        .map(word)
        .collect()
}

fn round_trips(body: &[u8], kinds: &[Kind]) {
    let (row_count, arrays) = decode_body(body, kinds).expect("a decodable body");
    let columns: Vec<(Kind, ArrayData)> = kinds.iter().cloned().zip(arrays).collect();

    assert_eq!(
        body,
        encode_body(&columns, row_count)
            .expect("an encodable batch")
            .as_slice()
    );
}

fn worked_example() -> Vec<(Kind, ArrayData)> {
    vec![
        (
            kind_of(r#"{"type":"integer"}"#),
            Int64Array::from(vec![1, 2]).into_data(),
        ),
        (
            kind_of(r#"{"type":"string"}"#),
            BinaryArray::from(vec![Some(b"ab".as_ref()), None]).into_data(),
        ),
    ]
}

/// FrameEncoderTest.php:29-35.
#[test]
fn the_worked_example() {
    let body = encode_body(&worked_example(), 2).expect("the worked example");

    assert_eq!(152, body.len());
    assert_eq!(
        "020000000200000005000000020000000000000002000000010000000000000000000000000000001800000018000000090000002800000014000000400000000a00000000000000ffffffffffffffff01000000000000000200000000000000ffffffffffffffff0100000000000000ffffffffffffffff00000000020000000200000000000000ffffffffffffffff6162000000000000",
        hex(&body),
    );
    round_trips(
        &body,
        &[worked_example()[0].0.clone(), worked_example()[1].0.clone()],
    );
}

#[test]
fn a_list_with_a_null_slot() {
    let kind = kind_of(r#"{"type":"list","element":{"type":"integer"}}"#);
    let list =
        ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1), Some(2)]), None]);
    let body = encode_body(&[(kind.clone(), list.into_data())], 2).expect("a list batch");

    assert_eq!(
        vec![2, 2, 4, 2, 1, 2, 0, 0, 9, 16, 20, 0, 0, 40, 24],
        directory(&body)
    );
    round_trips(&body, &[kind]);
}

#[test]
fn a_map_entries_node_and_extent() {
    let kind = kind_of(r#"{"type":"map","key":{"type":"string"},"value":{"type":"integer"}}"#);
    let mut map = MapBuilder::new(
        Some(MapFieldNames {
            entry: "entries".into(),
            key: "key".into(),
            value: "value".into(),
        }),
        BinaryBuilder::new(),
        Int64Builder::new(),
    );

    for row in [vec![("a", 1)], vec![("b", 2), ("c", 3)]] {
        for (key, value) in row {
            map.keys().append_value(key.as_bytes());
            map.values().append_value(value);
        }

        map.append(true).expect("a map row");
    }

    let body = encode_body(&[(kind.clone(), map.finish().into_data())], 2).expect("a map batch");

    assert_eq!(
        vec![
            2, 4, 8, 2, 0, 3, 0, 3, 0, 3, 0, 0, 0, 0, 20, 0, 0, 0, 0, 24, 24, 48, 11, 0, 0, 64, 32
        ],
        directory(&body),
    );
    round_trips(&body, &[kind]);
}

#[test]
fn a_structure_with_a_null_parent() {
    let kind =
        kind_of(r#"{"type":"structure_v2","fields":[{"name":"a","type":{"type":"integer"}}]}"#);
    let structure = StructArray::try_new(
        vec![Arc::new(arrow_schema::Field::new(
            "a",
            arrow_schema::DataType::Int64,
            true,
        ))]
        .into(),
        vec![Arc::new(Int64Array::from(vec![1, 5]))],
        Some(NullBuffer::from(vec![true, false])),
    )
    .expect("a valid struct");
    let body = encode_body(&[(kind.clone(), structure.into_data())], 2).expect("a structure batch");

    assert_eq!(
        vec![2, 2, 3, 2, 1, 2, 1, 0, 9, 16, 9, 32, 24],
        directory(&body)
    );
    round_trips(&body, &[kind]);
}

#[test]
fn a_null_column_has_a_full_null_node_and_no_extent() {
    let kind = kind_of(r#"{"type":"null"}"#);
    let body =
        encode_body(&[(kind.clone(), NullArray::new(2).into_data())], 2).expect("a null batch");

    assert_eq!(
        "020000000100000000000000020000000200000000000000",
        hex(&body)
    );
    round_trips(&body, &[kind]);
}

#[test]
fn a_batch_without_columns_keeps_its_row_count() {
    let body = encode_body(&[], 3).expect("a zero-column batch");

    assert_eq!("03000000000000000000000000000000", hex(&body));
    round_trips(&body, &[]);
}

#[test]
fn encode_refuses_a_column_of_another_length() {
    assert!(matches!(
        encode_body(&worked_example(), 3),
        Err(Error::ColumnRows {
            column: 0,
            rows: 2,
            frame_rows: 3
        }),
    ));
}

#[test]
fn decode_refuses_a_malformed_body() {
    let kinds = [
        kind_of(r#"{"type":"integer"}"#),
        kind_of(r#"{"type":"string"}"#),
    ];
    let body = encode_body(&worked_example(), 2).expect("the worked example");
    let with = |at: usize, bytes: &[u8]| {
        let mut corrupt = body.clone();
        corrupt[at..at + bytes.len()].copy_from_slice(bytes);
        corrupt
    };

    assert!(matches!(
        decode_body(&body[..11], &kinds),
        Err(Error::DirectoryTruncated)
    ));
    assert!(matches!(
        decode_body(&body[..40], &kinds),
        Err(Error::DirectoryTruncated)
    ));
    assert!(matches!(
        decode_body(&body, &kinds[..1]),
        Err(Error::FrameNodeCount {
            frame: 2,
            schema: 1
        }),
    ));
    assert!(matches!(
        decode_body(&with(12, &unhex("03000000")), &kinds),
        Err(Error::ColumnRows {
            column: 0,
            rows: 3,
            frame_rows: 2
        }),
    ));
    assert!(matches!(
        decode_body(&with(24, &unhex("03000000")), &kinds),
        Err(Error::ColumnNulls {
            column: 1,
            nulls: 3,
            rows: 2
        }),
    ));
    assert!(matches!(
        decode_body(&with(72, &unhex("0000000000000000")), &kinds),
        Err(Error::CompressedBuffer {
            buffer: 1,
            prefix: 0
        }),
    ));
    assert!(matches!(
        decode_body(&with(36, &unhex("ff000000")), &kinds),
        Err(Error::BufferOutsideArea {
            buffer: 1,
            area: 80
        }),
    ));
    assert!(matches!(
        decode_body(&with(24, &unhex("00000000")), &kinds),
        Err(Error::MalformedColumn { column: 1, .. }),
    ));
}
