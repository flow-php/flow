use std::str::FromStr;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ArrayKey, ZendHashTable, ZendObject};
use parquet::arrow::ArrowWriter;
use parquet::basic::{BrotliLevel, Compression, Encoding, GzipLevel, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterVersion};
use parquet::schema::types::ColumnPath;

use crate::parquet::exception::parquet_exception;
use crate::parquet::type_converter;
use crate::stream::php_destination_stream::PhpDestinationStream;

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\Writer")]
pub struct Writer {
    writer: Option<ArrowWriter<PhpDestinationStream>>,
    schema: Option<Arc<ArrowSchema>>,
}

impl Default for Writer {
    fn default() -> Self {
        Self {
            writer: None,
            schema: None,
        }
    }
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

impl Drop for Writer {
    fn drop(&mut self) {
        if let Some(writer) = self.writer.take() {
            let _ = writer.close();
        }
    }
}

#[php_impl]
impl Writer {
    pub fn __construct(
        stream: &mut ZendObject,
        schema: &ZendHashTable,
        compression: Option<String>,
        options: Option<&ZendHashTable>,
    ) -> PhpResult<Self> {
        let dest = PhpDestinationStream::new(stream).map_err(|e| parquet_exception(e))?;

        let arrow_schema =
            type_converter::php_schema_to_arrow(schema).map_err(|e| parquet_exception(e))?;

        let compression_str = compression.as_deref().unwrap_or("SNAPPY");

        let props = if let Some(opts) = options {
            if opts.len() > 0 {
                let builder = WriterProperties::builder();
                apply_writer_options(builder, opts, compression_str)
                    .map_err(|e| parquet_exception(e))?
                    .build()
            } else {
                let codec = parse_compression(compression_str, None, None, None)
                    .map_err(|e| parquet_exception(e))?;
                WriterProperties::builder()
                    .set_compression(codec)
                    .build()
            }
        } else {
            let codec = parse_compression(compression_str, None, None, None)
                .map_err(|e| parquet_exception(e))?;
            WriterProperties::builder()
                .set_compression(codec)
                .build()
        };

        let schema_arc = Arc::new(arrow_schema);

        let writer =
            ArrowWriter::try_new(dest, schema_arc.clone(), Some(props)).map_err(|e| {
                parquet_exception(format!("Failed to create Parquet writer: {}", e))
            })?;

        Ok(Self {
            writer: Some(writer),
            schema: Some(schema_arc),
        })
    }

    #[allow(non_snake_case)]
    pub fn writeBatch(&mut self, batch: &ZendHashTable) -> PhpResult<()> {
        let schema = self
            .schema
            .clone()
            .ok_or_else(|| parquet_exception("Writer is closed"))?;

        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| parquet_exception("Writer is closed"))?;

        let mut columns = Vec::with_capacity(schema.fields().len());

        for field in schema.fields() {
            let col_zval = batch.get(field.name().as_str()).ok_or_else(|| {
                parquet_exception(format!("Batch is missing column '{}'", field.name()))
            })?;

            let php_ht = col_zval.array().ok_or_else(|| {
                parquet_exception(format!("Column '{}' must be an array", field.name()))
            })?;

            let values: Vec<&ext_php_rs::types::Zval> = php_ht.values().collect();

            let array = type_converter::php_array_to_arrow(&values, field.data_type(), field.name())
                .map_err(|e| parquet_exception(e))?;

            columns.push(array);
        }

        let record_batch = RecordBatch::try_new(schema, columns).map_err(|e| {
            parquet_exception(format!("Failed to create record batch: {}", e))
        })?;

        writer.write(&record_batch).map_err(|e| {
            parquet_exception(format!("Failed to write batch: {}", e))
        })?;

        Ok(())
    }

    pub fn close(&mut self) -> PhpResult<()> {
        let writer = self
            .writer
            .take()
            .ok_or_else(|| parquet_exception("Writer is already closed"))?;

        self.schema = None;

        writer.close().map_err(|e| {
            parquet_exception(format!("Failed to close Parquet writer: {}", e))
        })?;

        Ok(())
    }
}
