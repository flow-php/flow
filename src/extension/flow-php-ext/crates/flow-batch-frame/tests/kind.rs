use arrow_data::ArrayData;
use flow_batch_frame::column::{decode, encode};
use flow_batch_frame::error::{Error, Part};
use flow_batch_frame::kind::{data_type, kind, kind_of, parse_schema, parse_type, Field, Kind};

/// The footer schema of FloeGoldenTest's `v2/all-entry-types.floe`.
const GOLDEN_SCHEMA: &str = r#"[{"ref":"int","type":{"type":"integer"},"nullable":false,"metadata":[]},{"ref":"int_min","type":{"type":"integer"},"nullable":false,"metadata":[]},{"ref":"int_max","type":{"type":"integer"},"nullable":false,"metadata":[]},{"ref":"float","type":{"type":"float"},"nullable":false,"metadata":[]},{"ref":"bool","type":{"type":"boolean"},"nullable":false,"metadata":[]},{"ref":"string","type":{"type":"string"},"nullable":false,"metadata":[]},{"ref":"string_binary","type":{"type":"string"},"nullable":false,"metadata":[]},{"ref":"string_unicode","type":{"type":"string"},"nullable":false,"metadata":[]},{"ref":"string_null","type":{"type":"string"},"nullable":true,"metadata":[]},{"ref":"string_from_null","type":{"type":"null"},"nullable":true,"metadata":[]},{"ref":"datetime_immutable","type":{"type":"datetime","zone":"Europe\/Warsaw"},"nullable":false,"metadata":[]},{"ref":"datetime_mutable","type":{"type":"datetime","zone":"America\/New_York"},"nullable":false,"metadata":[]},{"ref":"datetime_before_epoch","type":{"type":"datetime","zone":"UTC"},"nullable":false,"metadata":[]},{"ref":"date","type":{"type":"date"},"nullable":false,"metadata":[]},{"ref":"time","type":{"type":"time"},"nullable":false,"metadata":[]},{"ref":"uuid","type":{"type":"uuid"},"nullable":false,"metadata":[]},{"ref":"json","type":{"type":"json"},"nullable":false,"metadata":[]},{"ref":"json_object","type":{"type":"json"},"nullable":false,"metadata":[]},{"ref":"enum_backed","type":{"type":"enum","class":"Flow\\ETL\\Tests\\Fixtures\\Enum\\BackedStringEnum"},"nullable":false,"metadata":[]},{"ref":"enum_basic","type":{"type":"enum","class":"Flow\\ETL\\Tests\\Fixtures\\Enum\\BasicEnum"},"nullable":false,"metadata":[]},{"ref":"list","type":{"type":"list","element":{"type":"integer"}},"nullable":false,"metadata":[]},{"ref":"map","type":{"type":"map","key":{"type":"string"},"value":{"type":"integer"}},"nullable":false,"metadata":[]},{"ref":"structure","type":{"type":"structure_v2","fields":[{"name":"street","type":{"type":"string"},"optional":false},{"name":"nested","type":{"type":"structure_v2","fields":[{"name":"count","type":{"type":"integer"},"optional":false},{"name":"tags","type":{"type":"list","element":{"type":"string"}},"optional":false}]},"optional":false}]},"nullable":false,"metadata":[]},{"ref":"structure_interleaved","type":{"type":"structure_v2","fields":[{"name":"z","type":{"type":"integer"},"optional":false},{"name":"a","type":{"type":"string"},"optional":true},{"name":"b","type":{"type":"string"},"optional":false}]},"nullable":false,"metadata":[]},{"ref":"xml","type":{"type":"xml"},"nullable":false,"metadata":[]},{"ref":"xml_element","type":{"type":"xml_element"},"nullable":false,"metadata":[]}]"#;

fn field(name: &str, kind: Kind, optional: bool) -> Field {
    Field {
        name: name.to_owned(),
        kind,
        optional,
    }
}

#[test]
fn parses_a_golden_footer_schema() {
    let definitions = parse_schema(GOLDEN_SCHEMA.as_bytes()).expect("the golden schema");

    assert_eq!(26, definitions.len());
    assert_eq!("string_null", definitions[8].name);
    assert!(definitions[8].nullable);
    assert_eq!(Some(b"Europe/Warsaw".as_ref()), definitions[10].type_.zone());
    assert_eq!(
        Some(br"Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum".as_ref()),
        definitions[18].type_.class()
    );
    assert_eq!(
        Kind::Struct(vec![
            field("z", Kind::Int64, false),
            field("a", Kind::Bytes, true),
            field("b", Kind::Bytes, false),
        ]),
        kind_of(&definitions[23].type_).expect("a structure"),
    );
}

#[test]
fn refuses_schema_and_type_json_it_cannot_read() {
    assert!(matches!(parse_schema(b"[\xFF]"), Err(Error::SchemaJsonUtf8)));
    assert!(matches!(parse_schema(b"[{"), Err(Error::SchemaJson(_))));
    assert!(matches!(parse_type(b"\xFF"), Err(Error::TypeJsonUtf8)));
    assert!(matches!(parse_type(b"{}"), Err(Error::TypeJson(_))));
}

#[test]
fn refuses_a_type_without_a_column_kind() {
    assert!(matches!(
        kind(br#"{"type":"mixed"}"#),
        Err(Error::UnknownType { type_name }) if type_name == "mixed",
    ));
    assert!(matches!(
        kind(br#"{"type":"list"}"#),
        Err(Error::MissingPart { type_name, part: Part::Element }) if type_name == "list",
    ));
    assert!(matches!(
        kind(br#"{"type":"map","key":{"type":"string"}}"#),
        Err(Error::MissingPart { part: Part::Value, .. }),
    ));
    assert!(matches!(
        kind(br#"{"type":"optional"}"#),
        Err(Error::MissingPart { part: Part::Base, .. })
    ));
}

/// Every Definition's `Schema::normalize()` type JSON.
fn definitions() -> Vec<(&'static str, Kind)> {
    vec![
        (r#"{"type":"integer"}"#, Kind::Int64),
        (r#"{"type":"float"}"#, Kind::Float64),
        (r#"{"type":"boolean"}"#, Kind::Boolean),
        (r#"{"type":"string"}"#, Kind::Bytes),
        (r#"{"type":"datetime","zone":"UTC"}"#, Kind::Timestamp),
        (r#"{"type":"date"}"#, Kind::Int32),
        (r#"{"type":"time"}"#, Kind::Duration),
        (r#"{"type":"uuid"}"#, Kind::Uuid),
        (r#"{"type":"json"}"#, Kind::Bytes),
        (r#"{"type":"enum","class":"E"}"#, Kind::Bytes),
        (r#"{"type":"timezone"}"#, Kind::Bytes),
        (r#"{"type":"xml"}"#, Kind::Bytes),
        (r#"{"type":"xml_element"}"#, Kind::Bytes),
        (r#"{"type":"html"}"#, Kind::Bytes),
        (r#"{"type":"html_element"}"#, Kind::Bytes),
        (
            r#"{"type":"list","element":{"type":"optional","base":{"type":"integer"}}}"#,
            Kind::List(Box::new(field("item", Kind::Int64, true))),
        ),
        (
            r#"{"type":"map","key":{"type":"string"},"value":{"type":"optional","base":{"type":"integer"}}}"#,
            Kind::Map(
                Box::new(field("key", Kind::Bytes, false)),
                Box::new(field("value", Kind::Int64, true)),
            ),
        ),
        (
            r#"{"type":"structure_v2","fields":[{"name":"a","type":{"type":"integer"},"optional":false},{"name":"b","type":{"type":"optional","base":{"type":"string"}},"optional":false},{"name":"c","type":{"type":"string"},"optional":true}]}"#,
            Kind::Struct(vec![
                field("a", Kind::Int64, false),
                field("b", Kind::Bytes, false),
                field("c", Kind::Bytes, true),
            ]),
        ),
        (r#"{"type":"null"}"#, Kind::Null),
    ]
}

#[test]
fn every_definition_has_its_kind() {
    for (type_json, expected) in definitions() {
        assert_eq!(expected, kind(type_json.as_bytes()).expect(type_json), "{type_json}");
    }
}

#[test]
fn decode_builds_the_data_type_of_its_kind() {
    for (type_json, kind) in definitions() {
        let empty = encode(&ArrayData::new_empty(&data_type(&kind)), &kind).expect(type_json);
        let buffers: Vec<&[u8]> = empty.iter().map(Vec::as_slice).collect();

        assert_eq!(
            &data_type(&kind),
            decode(&kind, &buffers, 0, 0).expect(type_json).data_type(),
            "{type_json}",
        );
    }
}
