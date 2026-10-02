//! Ported verbatim from arrow-ext (`src/parquet/type_converter.rs` `parse_time_unit`, `php_schema_entry_to_field`,
//! `php_schema_to_arrow`; `src/parquet/writer.rs` `parse_compression`, `parse_encoding`, `apply_writer_options` and
//! the writer constructor's properties): the arrays of `Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()` and
//! `OptionsConverter::toExtension()`.

use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{
    extension::{Json, Uuid},
    DataType, Field, Fields, Schema, TimeUnit,
};
use ext_php_rs::types::{ArrayKey, ZendHashTable};
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

use crate::parquet::error::Error;

pub fn schema(ht: &ZendHashTable) -> Result<Schema, Error> {
    php_schema_to_arrow(ht).map_err(Error::Options)
}

pub fn properties(compression: &str, options: &ZendHashTable) -> Result<WriterProperties, Error> {
    if options.is_empty() {
        let codec = parse_compression(compression, None, None, None).map_err(Error::Options)?;

        return Ok(WriterProperties::builder().set_compression(codec).build());
    }

    Ok(apply_writer_options(WriterProperties::builder(), options, compression)
        .map_err(Error::Options)?
        .build())
}

fn parse_time_unit(entry_ht: &ZendHashTable, name: &str) -> Result<TimeUnit, String> {
    match entry_ht.get("unit").and_then(|z| z.str()) {
        None | Some("MICROS") => Ok(TimeUnit::Microsecond),
        Some("MILLIS") => Ok(TimeUnit::Millisecond),
        Some("NANOS") => Ok(TimeUnit::Nanosecond),
        Some(s) => Err(format!(
            "Column '{}': unsupported unit '{}', expected MILLIS, MICROS or NANOS",
            name, s
        )),
    }
}

fn php_schema_entry_to_field(entry_ht: &ZendHashTable) -> Result<Field, String> {
    let name = entry_ht
        .get("name")
        .and_then(|z| z.str())
        .ok_or_else(|| "Schema entry must have a 'name' string key".to_string())?
        .to_string();

    let type_str = entry_ht
        .get("type")
        .and_then(|z| z.str())
        .ok_or_else(|| "Schema entry must have a 'type' string key".to_string())?
        .to_string();

    let nullable = entry_ht
        .get("optional")
        .and_then(|z| z.bool())
        .unwrap_or(true);

    let data_type = match type_str.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "INT8" => DataType::Int8,
        "INT16" => DataType::Int16,
        "INT32" => DataType::Int32,
        "INT64" => DataType::Int64,
        "UINT8" => DataType::UInt8,
        "UINT16" => DataType::UInt16,
        "UINT32" => DataType::UInt32,
        "UINT64" => DataType::UInt64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => {
            let unit = parse_time_unit(entry_ht, &name)?;
            let utc = entry_ht.get("utc").and_then(|z| z.bool()).unwrap_or(true);
            DataType::Timestamp(unit, if utc { Some("UTC".into()) } else { None })
        }
        "TIME" => match parse_time_unit(entry_ht, &name)? {
            TimeUnit::Millisecond => DataType::Time32(TimeUnit::Millisecond),
            unit => DataType::Time64(unit),
        },
        "DECIMAL" => {
            let precision = entry_ht
                .get("precision")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': DECIMAL type requires 'precision' integer key",
                        name
                    )
                })? as u8;
            let scale = entry_ht
                .get("scale")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': DECIMAL type requires 'scale' integer key",
                        name
                    )
                })? as i8;
            DataType::Decimal128(precision, scale)
        }
        "UUID" => {
            let mut field = Field::new(&name, DataType::FixedSizeBinary(16), nullable);
            field.try_with_extension_type(Uuid).map_err(|e| {
                format!(
                    "Column '{}': failed to set UUID extension type: {}",
                    name, e
                )
            })?;
            return Ok(field);
        }
        "JSON" => {
            let mut field = Field::new(&name, DataType::Utf8, nullable);
            field
                .try_with_extension_type(Json::default())
                .map_err(|e| {
                    format!(
                        "Column '{}': failed to set JSON extension type: {}",
                        name, e
                    )
                })?;
            return Ok(field);
        }
        "FIXED_SIZE_BINARY" => {
            let length = entry_ht
                .get("length")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': FIXED_SIZE_BINARY type requires 'length' integer key",
                        name
                    )
                })? as i32;
            DataType::FixedSizeBinary(length)
        }
        "LIST" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| format!("Column '{}': LIST type requires 'children' array", name))?;
            let child_zv = children.values().next().ok_or_else(|| {
                format!("Column '{}': LIST type requires exactly one child", name)
            })?;
            let child_ht = child_zv
                .array()
                .ok_or_else(|| format!("Column '{}': LIST child must be an array", name))?;
            let child_field = php_schema_entry_to_field(child_ht)?;
            DataType::List(Arc::new(child_field))
        }
        "STRUCT" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| {
                    format!("Column '{}': STRUCT type requires 'children' array", name)
                })?;
            let fields: Result<Vec<Field>, String> = children
                .values()
                .map(|child_zv| {
                    let child_ht = child_zv.array().ok_or_else(|| {
                        format!("Column '{}': STRUCT child must be an array", name)
                    })?;
                    php_schema_entry_to_field(child_ht)
                })
                .collect();
            DataType::Struct(Fields::from(fields?))
        }
        "MAP" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| format!("Column '{}': MAP type requires 'children' array", name))?;
            let mut child_iter = children.values();
            let key_zv = child_iter.next().ok_or_else(|| {
                format!(
                    "Column '{}': MAP type requires key and value children",
                    name
                )
            })?;
            let val_zv = child_iter.next().ok_or_else(|| {
                format!(
                    "Column '{}': MAP type requires key and value children",
                    name
                )
            })?;
            let key_field =
                php_schema_entry_to_field(key_zv.array().ok_or_else(|| {
                    format!("Column '{}': MAP key child must be an array", name)
                })?)?;
            let val_field =
                php_schema_entry_to_field(val_zv.array().ok_or_else(|| {
                    format!("Column '{}': MAP value child must be an array", name)
                })?)?;
            let entries_struct = DataType::Struct(Fields::from(vec![key_field, val_field]));
            DataType::Map(
                Arc::new(Field::new("entries", entries_struct, false)),
                false,
            )
        }
        other => {
            return Err(format!("Column '{}': Unsupported type '{}'", name, other));
        }
    };

    Ok(Field::new(&name, data_type, nullable))
}

fn php_schema_to_arrow(schema: &ZendHashTable) -> Result<Schema, String> {
    if schema.is_empty() {
        return Err("Schema must have at least one column".into());
    }

    let mut fields = Vec::with_capacity(schema.len());

    for (_key, zv) in schema.iter() {
        let entry_ht = zv
            .array()
            .ok_or_else(|| "Schema entry must be an array".to_string())?;
        fields.push(php_schema_entry_to_field(entry_ht)?);
    }

    Ok(Schema::new(fields))
}

fn parse_compression(
    s: &str,
    gzip_level: Option<u32>,
    brotli_level: Option<u32>,
    zstd_level: Option<i32>,
) -> Result<Compression, String> {
    match s.to_uppercase().as_str() {
        "UNCOMPRESSED" => Ok(Compression::UNCOMPRESSED),
        "SNAPPY" => Ok(Compression::SNAPPY),
        "GZIP" => {
            let level = match gzip_level {
                Some(l) => GzipLevel::try_new(l)
                    .map_err(|e| format!("Invalid GZIP compression level: {}", e))?,
                None => Default::default(),
            };
            Ok(Compression::GZIP(level))
        }
        "ZSTD" => {
            let level = match zstd_level {
                Some(l) => ZstdLevel::try_new(l)
                    .map_err(|e| format!("Invalid ZSTD compression level: {}", e))?,
                None => Default::default(),
            };
            Ok(Compression::ZSTD(level))
        }
        "LZ4" | "LZ4_RAW" => Ok(Compression::LZ4_RAW),
        "BROTLI" => {
            let level = match brotli_level {
                Some(l) => BrotliLevel::try_new(l)
                    .map_err(|e| format!("Invalid BROTLI compression level: {}", e))?,
                None => Default::default(),
            };
            Ok(Compression::BROTLI(level))
        }
        other => Err(format!(
            "Unsupported compression codec '{}'. Supported: UNCOMPRESSED, SNAPPY, GZIP, ZSTD, LZ4, LZ4_RAW, BROTLI",
            other
        )),
    }
}

fn parse_encoding(s: &str) -> Result<Encoding, String> {
    Encoding::from_str(&s.to_uppercase())
        .map_err(|e| format!("Unsupported encoding '{}': {}", s, e))
}

fn apply_writer_options(
    mut builder: parquet::file::properties::WriterPropertiesBuilder,
    options: &ZendHashTable,
    compression_str: &str,
) -> Result<parquet::file::properties::WriterPropertiesBuilder, String> {
    let gzip_level = options
        .get("GZIP_COMPRESSION_LEVEL")
        .and_then(|z| z.long())
        .map(|v| v as u32);

    let brotli_level = options
        .get("BROTLI_COMPRESSION_LEVEL")
        .and_then(|z| z.long())
        .map(|v| v as u32);

    let zstd_level = options
        .get("ZSTD_COMPRESSION_LEVEL")
        .and_then(|z| z.long())
        .map(|v| v as i32);

    let codec = parse_compression(compression_str, gzip_level, brotli_level, zstd_level)?;
    builder = builder.set_compression(codec);

    if let Some(val) = options.get("ROW_GROUP_SIZE_BYTES").and_then(|z| z.long()) {
        builder = builder.set_max_row_group_bytes(Some(val as usize));
    }

    if let Some(val) = options.get("PAGE_SIZE_BYTES").and_then(|z| z.long()) {
        builder = builder.set_data_page_size_limit(val as usize);
    }

    if let Some(val) = options.get("DICTIONARY_PAGE_SIZE").and_then(|z| z.long()) {
        builder = builder.set_dictionary_page_size_limit(val as usize);
    }

    if let Some(val) = options.get("WRITER_VERSION").and_then(|z| z.long()) {
        let version = match val {
            1 => WriterVersion::PARQUET_1_0,
            2 => WriterVersion::PARQUET_2_0,
            other => {
                return Err(format!(
                    "Invalid WRITER_VERSION '{}'. Must be 1 or 2",
                    other
                ))
            }
        };
        builder = builder.set_writer_version(version);
    }

    if let Some(col_comp_zval) = options.get("COLUMNS_COMPRESSIONS") {
        if let Some(col_comp_ht) = col_comp_zval.array() {
            for (key, val) in col_comp_ht.iter() {
                let col_path_str = match key {
                    ArrayKey::String(ref s) => s.clone(),
                    ArrayKey::Str(s) => s.to_string(),
                    _ => continue,
                };
                if let Some(codec_str) = val.str() {
                    let parts: Vec<String> =
                        col_path_str.split('.').map(|s| s.to_string()).collect();
                    let col_path = ColumnPath::new(parts);
                    let col_codec =
                        parse_compression(codec_str, gzip_level, brotli_level, zstd_level)?;
                    builder = builder.set_column_compression(col_path, col_codec);
                }
            }
        }
    }

    if let Some(col_enc_zval) = options.get("COLUMNS_ENCODINGS") {
        if let Some(col_enc_ht) = col_enc_zval.array() {
            for (key, val) in col_enc_ht.iter() {
                let col_path_str = match key {
                    ArrayKey::String(ref s) => s.clone(),
                    ArrayKey::Str(s) => s.to_string(),
                    _ => continue,
                };
                if let Some(enc_str) = val.str() {
                    let parts: Vec<String> =
                        col_path_str.split('.').map(|s| s.to_string()).collect();
                    let col_path = ColumnPath::new(parts);
                    let encoding = parse_encoding(enc_str)?;
                    builder = builder.set_column_encoding(col_path, encoding);
                }
            }
        }
    }

    Ok(builder)
}

