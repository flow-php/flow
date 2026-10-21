//! The arrays of `Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()` and `OptionsConverter::toExtension()`.

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use arrow_schema::{
    extension::{Json, Uuid},
    DataType, Field, Fields, Schema, TimeUnit,
};
use ext_php_rs::ffi::zend_ulong;
use ext_php_rs::types::{ZendHashTable, ZendStr};
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, Type as PhysicalType, ZstdLevel};
use parquet::file::properties::{EnabledStatistics, WriterProperties, WriterVersion, DEFAULT_WRITE_BATCH_SIZE};
use parquet::schema::types::{ColumnPath, SchemaDescriptor};

use crate::parquet::error::Error;
use crate::php::ht_for_each;

/// Field metadata key of a Utf8 field written with the ENUM logical type, which no arrow data type carries.
pub const ENUM_METADATA_KEY: &str = "flow.parquet.enum";

pub fn schema(ht: &ZendHashTable) -> Result<Schema, Error> {
    php_schema_to_arrow(ht).map_err(Error::Options)
}

/// `leaves`: the parquet leaves the written schema converts to, which `COLUMNS_ENCODINGS` entries are checked against.
pub fn properties(
    compression: &str,
    options: &ZendHashTable,
    leaves: &SchemaDescriptor,
) -> Result<WriterProperties, Error> {
    if options.is_empty() {
        let codec = parse_compression(compression, None, None, None).map_err(Error::Options)?;

        return Ok(WriterProperties::builder().set_compression(codec).build());
    }

    Ok(apply_writer_options(WriterProperties::builder(), options, compression, leaves)
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

    let nullable = entry_ht.get("optional").and_then(|z| z.bool()).unwrap_or(true);

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
        "ENUM" => {
            return Ok(Field::new(&name, DataType::Utf8, nullable)
                .with_metadata(HashMap::from([(ENUM_METADATA_KEY.to_string(), String::new())])));
        }
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
                .ok_or_else(|| format!("Column '{}': DECIMAL type requires 'precision' integer key", name))?
                as u8;
            let scale = entry_ht
                .get("scale")
                .and_then(|z| z.long())
                .ok_or_else(|| format!("Column '{}': DECIMAL type requires 'scale' integer key", name))?
                as i8;
            DataType::Decimal128(precision, scale)
        }
        "UUID" => {
            let mut field = Field::new(&name, DataType::FixedSizeBinary(16), nullable);
            field
                .try_with_extension_type(Uuid)
                .map_err(|e| format!("Column '{}': failed to set UUID extension type: {}", name, e))?;
            return Ok(field);
        }
        "JSON" => {
            let mut field = Field::new(&name, DataType::Utf8, nullable);
            field
                .try_with_extension_type(Json::default())
                .map_err(|e| format!("Column '{}': failed to set JSON extension type: {}", name, e))?;
            return Ok(field);
        }
        "FIXED_SIZE_BINARY" => {
            let length = entry_ht.get("length").and_then(|z| z.long()).ok_or_else(|| {
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
            let child_zv = children
                .values()
                .next()
                .ok_or_else(|| format!("Column '{}': LIST type requires exactly one child", name))?;
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
                .ok_or_else(|| format!("Column '{}': STRUCT type requires 'children' array", name))?;
            let fields: Result<Vec<Field>, String> = children
                .values()
                .map(|child_zv| {
                    let child_ht = child_zv
                        .array()
                        .ok_or_else(|| format!("Column '{}': STRUCT child must be an array", name))?;
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
            let key_zv = child_iter
                .next()
                .ok_or_else(|| format!("Column '{}': MAP type requires key and value children", name))?;
            let val_zv = child_iter
                .next()
                .ok_or_else(|| format!("Column '{}': MAP type requires key and value children", name))?;
            let key_field = php_schema_entry_to_field(
                key_zv
                    .array()
                    .ok_or_else(|| format!("Column '{}': MAP key child must be an array", name))?,
            )?;
            let val_field = php_schema_entry_to_field(
                val_zv
                    .array()
                    .ok_or_else(|| format!("Column '{}': MAP value child must be an array", name))?,
            )?;
            let entries_struct = DataType::Struct(Fields::from(vec![key_field, val_field]));
            // parquet-format LogicalTypes.md names the repeated group of a MAP `key_value`
            DataType::Map(Arc::new(Field::new("key_value", entries_struct, false)), false)
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
        let entry_ht = zv.array().ok_or_else(|| "Schema entry must be an array".to_string())?;
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
                Some(l) => GzipLevel::try_new(l).map_err(|e| format!("Invalid GZIP compression level: {}", e))?,
                None => Default::default(),
            };
            Ok(Compression::GZIP(level))
        }
        "ZSTD" => {
            let level = match zstd_level {
                Some(l) => ZstdLevel::try_new(l).map_err(|e| format!("Invalid ZSTD compression level: {}", e))?,
                None => Default::default(),
            };
            Ok(Compression::ZSTD(level))
        }
        "LZ4" | "LZ4_RAW" => Ok(Compression::LZ4_RAW),
        "BROTLI" => {
            let level = match brotli_level {
                Some(l) => BrotliLevel::try_new(l).map_err(|e| format!("Invalid BROTLI compression level: {}", e))?,
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
    Encoding::from_str(&s.to_uppercase()).map_err(|e| format!("Unsupported encoding '{}': {}", s, e))
}

fn apply_writer_options(
    mut builder: parquet::file::properties::WriterPropertiesBuilder,
    options: &ZendHashTable,
    compression_str: &str,
    leaves: &SchemaDescriptor,
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
        if val < 1 {
            return Err(format!("ROW_GROUP_SIZE_BYTES must be greater than 0, {val} given"));
        }

        builder = builder.set_max_row_group_bytes(Some(val as usize));
    }

    if let Some(val) = options.get("PAGE_SIZE_BYTES").and_then(|z| z.long()) {
        builder = builder.set_data_page_size_limit(val as usize);
    }

    if let Some(val) = options.get("PAGE_MAXIMUM_ROWS_COUNT").and_then(|z| z.long()) {
        if val < 1 {
            return Err(format!("PAGE_MAXIMUM_ROWS_COUNT must be greater than 0, {val} given"));
        }

        // arrow-rs checks the page row limit once per write batch: a smaller batch closes pages where PHP does
        builder = builder
            .set_data_page_row_count_limit(val as usize)
            .set_write_batch_size((val as usize).min(DEFAULT_WRITE_BATCH_SIZE));
    }

    if let Some(val) = options.get("STATISTICS_TRUNCATE_LENGTH").and_then(|z| z.long()) {
        if val < 1 {
            return Err(format!(
                "STATISTICS_TRUNCATE_LENGTH must be greater than 0, {val} given"
            ));
        }

        builder = builder.set_statistics_truncate_length(Some(val as usize));
    }

    if let Some(val) = options.get("COLUMN_INDEX_TRUNCATE_LENGTH").and_then(|z| z.long()) {
        if val < 1 {
            return Err(format!(
                "COLUMN_INDEX_TRUNCATE_LENGTH must be greater than 0, {val} given"
            ));
        }

        builder = builder.set_column_index_truncate_length(Some(val as usize));
    }

    if let Some(level) = options.get("STATISTICS_LEVEL").and_then(|z| z.str()) {
        builder = builder.set_statistics_enabled(match level {
            "NONE" => EnabledStatistics::None,
            "CHUNK" => EnabledStatistics::Chunk,
            "PAGE" => EnabledStatistics::Page,
            other => {
                return Err(format!(
                    "Invalid STATISTICS_LEVEL '{other}'. Must be NONE, CHUNK or PAGE"
                ))
            }
        });
    }

    if let Some(enabled) = options.get("PAGE_HEADER_STATISTICS").and_then(|z| z.bool()) {
        builder = builder.set_write_page_header_statistics(enabled);
    }

    if let Some(enabled) = options.get("DICTIONARY_ENABLED").and_then(|z| z.bool()) {
        builder = builder.set_dictionary_enabled(enabled);
    }

    if let Some(val) = options.get("DICTIONARY_PAGE_SIZE").and_then(|z| z.long()) {
        builder = builder.set_dictionary_page_size_limit(val as usize);
    }

    if let Some(val) = options.get("WRITER_VERSION").and_then(|z| z.long()) {
        let version = match val {
            1 => WriterVersion::PARQUET_1_0,
            2 => WriterVersion::PARQUET_2_0,
            other => return Err(format!("Invalid WRITER_VERSION '{}'. Must be 1 or 2", other)),
        };
        builder = builder.set_writer_version(version);
    }

    if let Some(columns) = options.get("COLUMNS_COMPRESSIONS").and_then(|z| z.array()) {
        let mut compressions = Vec::new();

        ht_for_each(columns, |key, index, value| {
            let path = column_key("COLUMNS_COMPRESSIONS", key, index)?;
            let codec = value
                .str()
                .ok_or_else(|| format!("COLUMNS_COMPRESSIONS['{path}'] must be a string"))?;
            compressions.push((
                column_path(&path),
                parse_compression(codec, gzip_level, brotli_level, zstd_level)?,
            ));

            Ok::<(), String>(())
        })?;

        for (path, codec) in compressions {
            builder = builder.set_column_compression(path, codec);
        }
    }

    if let Some(columns) = options.get("COLUMNS_ENCODINGS").and_then(|z| z.array()) {
        let mut encodings = Vec::new();

        ht_for_each(columns, |key, index, value| {
            let path = column_key("COLUMNS_ENCODINGS", key, index)?;
            let encoding = value
                .str()
                .ok_or_else(|| format!("COLUMNS_ENCODINGS['{path}'] must be a string"))?;
            let encoding = parse_encoding(encoding)?;
            let writer_version = options.get("WRITER_VERSION").and_then(|z| z.long()).unwrap_or(1);

            if let Some(column) = leaves.columns().iter().find(|column| column.path().string() == path) {
                if let Some(requirement) = encoding_requirement(encoding, column.physical_type(), writer_version) {
                    return Err(format!(
                        "{encoding:?} encoding {requirement}. Column '{path}' has type: {:?}",
                        column.physical_type()
                    ));
                }
            }

            encodings.push((column_path(&path), encoding));

            Ok::<(), String>(())
        })?;

        for (path, encoding) in encodings {
            // arrow-rs panics on a dictionary encoding set as the column encoding, and uses any other one only once the
            // dictionary falls back: turning the dictionary off makes it the column's encoding, as the PHP writer writes it
            builder = match encoding {
                Encoding::PLAIN_DICTIONARY | Encoding::RLE_DICTIONARY => {
                    builder.set_column_dictionary_enabled(path, true)
                }
                encoding => builder
                    .set_column_encoding(path.clone(), encoding)
                    .set_column_dictionary_enabled(path, false),
            };
        }
    }

    if let Some(columns) = options.get("COLUMNS_BLOOM_FILTERS").and_then(|z| z.array()) {
        let mut filters = Vec::new();

        ht_for_each(columns, |key, index, value| {
            let path = column_key("COLUMNS_BLOOM_FILTERS", key, index)?;
            let filter = value
                .array()
                .ok_or_else(|| format!("COLUMNS_BLOOM_FILTERS['{path}'] must be an array"))?;
            let fpp = filter
                .get("fpp")
                .and_then(|z| z.double())
                .ok_or_else(|| format!("COLUMNS_BLOOM_FILTERS['{path}'] has no float fpp"))?;

            if !(fpp > 0.0 && fpp < 1.0) {
                return Err(format!(
                    "COLUMNS_BLOOM_FILTERS['{path}'] fpp must be greater than 0 and less than 1, {fpp} given"
                ));
            }

            let ndv = filter
                .get("ndv")
                .and_then(|z| z.long())
                .map(|ndv| {
                    u64::try_from(ndv).map_err(|_| format!("COLUMNS_BLOOM_FILTERS['{path}'] ndv must not be negative"))
                })
                .transpose()?;

            filters.push((column_path(&path), fpp, ndv));

            Ok::<(), String>(())
        })?;

        for (path, fpp, ndv) in filters {
            builder = builder
                .set_column_bloom_filter_enabled(path.clone(), true)
                .set_column_bloom_filter_fpp(path.clone(), fpp);

            if let Some(ndv) = ndv {
                builder = builder.set_column_bloom_filter_max_ndv(path, ndv);
            }
        }
    }

    Ok(builder)
}

/// A `COLUMNS_*` option's key as a column path; PHP keys a digit-only path by its integer.
fn column_key(option: &str, key: Option<&ZendStr>, index: zend_ulong) -> Result<String, String> {
    match key {
        Some(key) => key
            .as_str()
            .map(str::to_string)
            .map_err(|_| format!("{option} has a key that is not a column path")),
        None => Ok(index.to_string()),
    }
}

/// The PHP writer's `ColumnEncodingPolicy` rules: what an encoding requires of the column, when the column lacks it.
fn encoding_requirement(encoding: Encoding, physical: PhysicalType, writer_version: i64) -> Option<&'static str> {
    match encoding {
        Encoding::RLE if physical != PhysicalType::BOOLEAN => Some("is only supported for BOOLEAN columns"),
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY if physical == PhysicalType::BOOLEAN => {
            Some("is not supported for BOOLEAN columns")
        }
        Encoding::RLE_DICTIONARY | Encoding::PLAIN_DICTIONARY
            if physical == PhysicalType::FIXED_LEN_BYTE_ARRAY && writer_version != 2 =>
        {
            Some("is not supported for FIXED_LEN_BYTE_ARRAY columns under WRITER_VERSION 1")
        }
        Encoding::DELTA_BINARY_PACKED if !matches!(physical, PhysicalType::INT32 | PhysicalType::INT64) => {
            Some("is only supported for INT32 and INT64 columns")
        }
        Encoding::DELTA_LENGTH_BYTE_ARRAY if physical != PhysicalType::BYTE_ARRAY => {
            Some("is only supported for BYTE_ARRAY columns")
        }
        Encoding::DELTA_BYTE_ARRAY
            if !matches!(physical, PhysicalType::BYTE_ARRAY | PhysicalType::FIXED_LEN_BYTE_ARRAY) =>
        {
            Some("is only supported for BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY columns")
        }
        Encoding::BYTE_STREAM_SPLIT
            if !matches!(
                physical,
                PhysicalType::FLOAT
                    | PhysicalType::DOUBLE
                    | PhysicalType::INT32
                    | PhysicalType::INT64
                    | PhysicalType::FIXED_LEN_BYTE_ARRAY
            ) =>
        {
            Some("is only supported for FLOAT, DOUBLE, INT32, INT64 and FIXED_LEN_BYTE_ARRAY columns")
        }
        _ => None,
    }
}

fn column_path(path: &str) -> ColumnPath {
    ColumnPath::new(path.split('.').map(str::to_string).collect())
}
