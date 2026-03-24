//! Standalone binary to generate test fixtures.
//! Run with: cargo run --example generate_fixtures

use std::fs::File;
use std::sync::Arc;

fn generate_simple() {
    use parquet::basic::Type as PhysicalType;
    use parquet::basic::{ConvertedType, Repetition};
    use parquet::column::writer::ColumnWriter;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::schema::types::Type;

    let id_field = Arc::new(
        Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .unwrap(),
    );

    let name_field = Arc::new(
        Type::primitive_type_builder("name", PhysicalType::BYTE_ARRAY)
            .with_repetition(Repetition::REQUIRED)
            .with_converted_type(ConvertedType::UTF8)
            .build()
            .unwrap(),
    );

    let schema = Arc::new(
        Type::group_type_builder("schema")
            .with_fields(vec![id_field, name_field])
            .build()
            .unwrap(),
    );

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(parquet::basic::Compression::UNCOMPRESSED)
            .build(),
    );

    let file = File::create("tests/fixtures/simple.parquet").unwrap();
    let mut writer = SerializedFileWriter::new(file, schema, props).unwrap();

    let mut row_group_writer = writer.next_row_group().unwrap();

    // Write id column
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        if let ColumnWriter::Int64ColumnWriter(ref mut w) = col_writer.untyped() {
            w.write_batch(&[1, 2, 3], None, None).unwrap();
        }
        col_writer.close().unwrap();
    }

    // Write name column
    if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
        if let ColumnWriter::ByteArrayColumnWriter(ref mut w) = col_writer.untyped() {
            let values: Vec<parquet::data_type::ByteArray> =
                vec!["Alice".into(), "Bob".into(), "Charlie".into()];
            w.write_batch(&values, None, None).unwrap();
        }
        col_writer.close().unwrap();
    }

    row_group_writer.close().unwrap();
    writer.close().unwrap();

    println!("Generated tests/fixtures/simple.parquet");
}

fn generate_all_flat_types() {
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("col_bool", DataType::Boolean, true),
        Field::new("col_int8", DataType::Int8, true),
        Field::new("col_int16", DataType::Int16, true),
        Field::new("col_int32", DataType::Int32, true),
        Field::new("col_int64", DataType::Int64, true),
        Field::new("col_uint8", DataType::UInt8, true),
        Field::new("col_uint16", DataType::UInt16, true),
        Field::new("col_uint32", DataType::UInt32, true),
        Field::new("col_uint64", DataType::UInt64, true),
        Field::new("col_float", DataType::Float32, true),
        Field::new("col_double", DataType::Float64, true),
        Field::new("col_string", DataType::Utf8, true),
        Field::new("col_binary", DataType::Binary, true),
        Field::new("col_date", DataType::Date32, true),
        Field::new(
            "col_timestamp",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("col_decimal", DataType::Decimal128(10, 2), true),
    ]));

    let col_bool = BooleanArray::from(vec![Some(true), Some(false), None]);
    let col_int8 = Int8Array::from(vec![Some(1i8), Some(-128i8), None]);
    let col_int16 = Int16Array::from(vec![Some(256i16), Some(-32768i16), None]);
    let col_int32 = Int32Array::from(vec![Some(100000i32), Some(-100000i32), None]);
    let col_int64 = Int64Array::from(vec![Some(1000000000i64), Some(-1000000000i64), None]);
    let col_uint8 = UInt8Array::from(vec![Some(0u8), Some(255u8), None]);
    let col_uint16 = UInt16Array::from(vec![Some(0u16), Some(65535u16), None]);
    let col_uint32 = UInt32Array::from(vec![Some(0u32), Some(4294967295u32), None]);
    let col_uint64 = UInt64Array::from(vec![Some(0u64), Some(u64::MAX), None]);
    let col_float = Float32Array::from(vec![Some(1.5f32), Some(-2.5f32), None]);
    let col_double = Float64Array::from(vec![Some(3.14159f64), Some(-2.71828f64), None]);
    let col_string = StringArray::from(vec![Some("hello"), Some("world"), None]);

    let col_binary: BinaryArray = vec![
        Some(b"\x00\x01\x02" as &[u8]),
        Some(b"\xff\xfe" as &[u8]),
        None,
    ]
    .into_iter()
    .collect();

    // 19000 days since epoch
    let col_date = Date32Array::from(vec![Some(19000i32), Some(0i32), None]);
    let col_timestamp =
        TimestampMicrosecondArray::from(vec![Some(1700000000000000i64), Some(0i64), None]);

    // Decimal128: value is unscaled integer. 123.45 with scale=2 → 12345, -999.99 → -99999
    let col_decimal = Decimal128Array::from(vec![Some(12345i128), Some(-99999i128), None])
        .with_precision_and_scale(10, 2)
        .unwrap();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(col_bool),
            Arc::new(col_int8),
            Arc::new(col_int16),
            Arc::new(col_int32),
            Arc::new(col_int64),
            Arc::new(col_uint8),
            Arc::new(col_uint16),
            Arc::new(col_uint32),
            Arc::new(col_uint64),
            Arc::new(col_float),
            Arc::new(col_double),
            Arc::new(col_string),
            Arc::new(col_binary),
            Arc::new(col_date),
            Arc::new(col_timestamp),
            Arc::new(col_decimal),
        ],
    )
    .unwrap();

    let file = File::create("tests/fixtures/all_flat_types.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    println!("Generated tests/fixtures/all_flat_types.parquet");
}

fn generate_nested() {
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            true,
        ),
        Field::new(
            "metadata",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            true,
        ),
        Field::new(
            "props",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int64, true),
                    ])),
                    false,
                )),
                false,
            ),
            true,
        ),
    ]));

    // id column
    let id_array = Int64Array::from(vec![1, 2, 3]);

    // tags column: Row 0: ["a","b","c"], Row 1: [] (empty), Row 2: null
    let mut tags_builder = ListBuilder::new(StringBuilder::new());
    tags_builder.values().append_value("a");
    tags_builder.values().append_value("b");
    tags_builder.values().append_value("c");
    tags_builder.append(true);
    tags_builder.append(true); // empty list
    tags_builder.append(false); // null
    let tags_array = tags_builder.finish();

    // metadata column: Row 0: {key:"x", value:10}, Row 1: null, Row 2: {key:"y", value:20}
    let meta_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, false),
        Field::new("value", DataType::Int64, false),
    ]);
    let mut meta_builder = StructBuilder::new(
        meta_fields,
        vec![
            Box::new(StringBuilder::new()),
            Box::new(Int64Builder::new()),
        ],
    );
    // Row 0: {key:"x", value:10}
    meta_builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("x");
    meta_builder
        .field_builder::<Int64Builder>(1)
        .unwrap()
        .append_value(10);
    meta_builder.append(true);
    // Row 1: null
    meta_builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_null();
    meta_builder
        .field_builder::<Int64Builder>(1)
        .unwrap()
        .append_null();
    meta_builder.append(false);
    // Row 2: {key:"y", value:20}
    meta_builder
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("y");
    meta_builder
        .field_builder::<Int64Builder>(1)
        .unwrap()
        .append_value(20);
    meta_builder.append(true);
    let metadata_array = meta_builder.finish();

    // props column: Row 0: {"p1":100, "p2":200}, Row 1: {"p3":300}, Row 2: null
    let mut props_builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    props_builder.keys().append_value("p1");
    props_builder.values().append_value(100);
    props_builder.keys().append_value("p2");
    props_builder.values().append_value(200);
    props_builder.append(true).unwrap();
    props_builder.keys().append_value("p3");
    props_builder.values().append_value(300);
    props_builder.append(true).unwrap();
    props_builder.append(false).unwrap(); // null
    let props_array = props_builder.finish();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(id_array),
            Arc::new(tags_array),
            Arc::new(metadata_array),
            Arc::new(props_array),
        ],
    )
    .unwrap();

    let file = File::create("tests/fixtures/nested.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    println!("Generated tests/fixtures/nested.parquet");
}

fn generate_deeply_nested() {
    use arrow_array::builder::*;
    use arrow_array::*;
    use arrow_schema::{DataType, Field, Fields, Schema};
    use parquet::arrow::ArrowWriter;

    let scores_field = Field::new(
        "scores",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    );
    let struct_fields = Fields::from(vec![
        Field::new("name", DataType::Utf8, false),
        scores_field,
    ]);
    let groups_field = Field::new(
        "groups",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(struct_fields.clone()),
            true,
        ))),
        true,
    );

    let schema = Arc::new(Schema::new(vec![groups_field]));

    // Build using nested builders
    let scores_builder = ListBuilder::new(Int64Builder::new());
    let struct_builder = StructBuilder::new(
        struct_fields,
        vec![
            Box::new(StringBuilder::new()),
            Box::new(scores_builder),
        ],
    );
    let mut groups_builder = ListBuilder::new(struct_builder);

    // Row 0: [{name:"alpha", scores:[10,20]}, {name:"beta", scores:[30]}]
    groups_builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("alpha");
    let scores_b = groups_builder
        .values()
        .field_builder::<ListBuilder<Int64Builder>>(1)
        .unwrap();
    scores_b.values().append_value(10);
    scores_b.values().append_value(20);
    scores_b.append(true);
    groups_builder.values().append(true);

    groups_builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("beta");
    let scores_b = groups_builder
        .values()
        .field_builder::<ListBuilder<Int64Builder>>(1)
        .unwrap();
    scores_b.values().append_value(30);
    scores_b.append(true);
    groups_builder.values().append(true);
    groups_builder.append(true);

    // Row 1: [{name:"gamma", scores:[]}]
    groups_builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("gamma");
    let scores_b = groups_builder
        .values()
        .field_builder::<ListBuilder<Int64Builder>>(1)
        .unwrap();
    scores_b.append(true); // empty list
    groups_builder.values().append(true);
    groups_builder.append(true);

    // Row 2: null
    groups_builder.append(false);

    let groups_array = groups_builder.finish();

    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(groups_array)]).unwrap();

    let file = File::create("tests/fixtures/deeply_nested.parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    println!("Generated tests/fixtures/deeply_nested.parquet");
}

fn main() {
    std::fs::create_dir_all("tests/fixtures").unwrap();
    generate_simple();
    generate_all_flat_types();
    generate_nested();
    generate_deeply_nested();
}
