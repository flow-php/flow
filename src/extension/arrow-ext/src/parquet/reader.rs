use arrow_schema::DataType;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, ZendObject};
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReader,
    ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::ProjectionMask;

use crate::parquet::exception::parquet_exception;
use crate::parquet::type_converter;
use crate::stream::php_source_stream::PhpSourceStream;

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\Reader")]
#[derive(Default)]
pub struct Reader {
    stream: Option<PhpSourceStream>,
    reader_metadata: Option<ArrowReaderMetadata>,
    num_row_groups: usize,
    current_row_group: usize,
    batch_size: Option<usize>,
    active_batch_reader: Option<ParquetRecordBatchReader>,
    active_columns: Vec<String>,
}

impl Reader {
    fn ensure_open(&self) -> PhpResult<(&PhpSourceStream, &ArrowReaderMetadata)> {
        match (&self.stream, &self.reader_metadata) {
            (Some(s), Some(m)) => Ok((s, m)),
            _ => Err(parquet_exception("Reader is closed")),
        }
    }

    fn batch_to_php(
        batch: &arrow_array::RecordBatch,
        columns: &[String],
    ) -> PhpResult<ZBox<ZendHashTable>> {
        let mut result = ZendHashTable::new();
        let schema = batch.schema();

        for col_name in columns {
            let column = batch.column_by_name(col_name).ok_or_else(|| {
                parquet_exception(format!("Column '{}' not found in batch", col_name))
            })?;
            let field = schema.field_with_name(col_name).ok();

            let values = type_converter::arrow_array_to_php_values(column.as_ref(), field)?;

            let mut php_array = ZendHashTable::with_capacity(values.len() as u32);

            for zv in values {
                php_array
                    .push(zv)
                    .map_err(|_| parquet_exception("Failed to build column array"))?;
            }

            result
                .insert(col_name.as_str(), php_array)
                .map_err(|_| parquet_exception("Failed to build result array"))?;
        }

        Ok(result)
    }

    fn consume_all_batches(
        reader: ParquetRecordBatchReader,
        columns: &[String],
    ) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        let mut column_data: Vec<Vec<ext_php_rs::types::Zval>> =
            columns.iter().map(|_| Vec::new()).collect();

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| parquet_exception(format!("Failed to read batch: {}", e)))?;
            let schema = batch.schema();

            for (idx, col_name) in columns.iter().enumerate() {
                let column = batch.column_by_name(col_name).ok_or_else(|| {
                    parquet_exception(format!("Column '{}' not found in batch", col_name))
                })?;
                let field = schema.field_with_name(col_name).ok();

                let mut values = type_converter::arrow_array_to_php_values(column.as_ref(), field)?;
                column_data[idx].append(&mut values);
            }
        }

        let mut result = ZendHashTable::new();

        for (idx, col_name) in columns.iter().enumerate() {
            let values = std::mem::take(&mut column_data[idx]);
            let mut php_array = ZendHashTable::with_capacity(values.len() as u32);

            for zv in values {
                php_array
                    .push(zv)
                    .map_err(|_| parquet_exception("Failed to build column array"))?;
            }

            result
                .insert(col_name.as_str(), php_array)
                .map_err(|_| parquet_exception("Failed to build result array"))?;
        }

        Ok(Some(result))
    }
}

fn field_to_php_schema(field: &arrow_schema::Field) -> PhpResult<ZBox<ZendHashTable>> {
    let mut entry = ZendHashTable::new();
    entry
        .insert("name", field.name().as_str())
        .map_err(|_| parquet_exception("Failed to build schema entry"))?;

    let type_str = match field.data_type() {
        DataType::Boolean => "BOOLEAN",
        DataType::Int8 => "INT8",
        DataType::Int16 => "INT16",
        DataType::Int32 => "INT32",
        DataType::Int64 => "INT64",
        DataType::UInt8 => "UINT8",
        DataType::UInt16 => "UINT16",
        DataType::UInt32 => "UINT32",
        DataType::UInt64 => "UINT64",
        DataType::Float32 => "FLOAT",
        DataType::Float64 => "DOUBLE",
        DataType::Utf8 | DataType::LargeUtf8 => "STRING",
        DataType::Binary | DataType::LargeBinary => "BINARY",
        DataType::Date32 | DataType::Date64 => "DATE",
        DataType::Timestamp(_, _) => "TIMESTAMP",
        DataType::Time32(_) | DataType::Time64(_) => "TIME",
        DataType::Decimal128(_, _) => "DECIMAL",
        DataType::FixedSizeBinary(_) => "FIXED_SIZE_BINARY",
        DataType::List(_) | DataType::LargeList(_) => "LIST",
        DataType::Struct(_) => "STRUCT",
        DataType::Map(_, _) => "MAP",
        other => {
            return Err(parquet_exception(format!(
                "Unsupported Arrow type in schema: {:?}",
                other
            )));
        }
    };

    entry
        .insert("type", type_str)
        .map_err(|_| parquet_exception("Failed to build schema entry"))?;
    entry
        .insert("optional", field.is_nullable())
        .map_err(|_| parquet_exception("Failed to build schema entry"))?;

    match field.data_type() {
        DataType::Decimal128(precision, scale) => {
            entry
                .insert("precision", *precision as i64)
                .map_err(|_| parquet_exception("Failed to build schema entry"))?;
            entry
                .insert("scale", *scale as i64)
                .map_err(|_| parquet_exception("Failed to build schema entry"))?;
        }
        DataType::FixedSizeBinary(n) => {
            entry
                .insert("length", *n as i64)
                .map_err(|_| parquet_exception("Failed to build schema entry"))?;
        }
        DataType::List(child_field) | DataType::LargeList(child_field) => {
            let mut children = ZendHashTable::new();
            let child_schema = field_to_php_schema(child_field)?;
            children
                .push(child_schema)
                .map_err(|_| parquet_exception("Failed to build schema children"))?;
            entry
                .insert("children", children)
                .map_err(|_| parquet_exception("Failed to build schema entry"))?;
        }
        DataType::Struct(fields) => {
            let mut children = ZendHashTable::new();
            for f in fields {
                let child_schema = field_to_php_schema(f)?;
                children
                    .push(child_schema)
                    .map_err(|_| parquet_exception("Failed to build schema children"))?;
            }
            entry
                .insert("children", children)
                .map_err(|_| parquet_exception("Failed to build schema entry"))?;
        }
        DataType::Map(struct_field, _) => {
            if let DataType::Struct(fields) = struct_field.data_type() {
                let mut children = ZendHashTable::new();
                for f in fields {
                    let child_schema = field_to_php_schema(f)?;
                    children
                        .push(child_schema)
                        .map_err(|_| parquet_exception("Failed to build schema children"))?;
                }
                entry
                    .insert("children", children)
                    .map_err(|_| parquet_exception("Failed to build schema entry"))?;
            }
        }
        _ => {}
    }

    Ok(entry)
}

#[php_impl]
impl Reader {
    pub fn __construct(
        source: &mut ZendObject,
        options: Option<&ZendHashTable>,
    ) -> PhpResult<Self> {
        let stream = PhpSourceStream::new(source).map_err(parquet_exception)?;

        let reader_metadata = ArrowReaderMetadata::load(&stream, ArrowReaderOptions::default())
            .map_err(|e| parquet_exception(format!("Failed to open Parquet file: {}", e)))?;

        let num_row_groups = reader_metadata.metadata().num_row_groups();

        let batch_size = options
            .and_then(|opts| opts.get("BATCH_SIZE"))
            .and_then(|z| z.long())
            .map(|v| v as usize);

        Ok(Self {
            stream: Some(stream),
            reader_metadata: Some(reader_metadata),
            num_row_groups,
            current_row_group: 0,
            batch_size,
            active_batch_reader: None,
            active_columns: Vec::new(),
        })
    }

    pub fn schema(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let (_, reader_metadata) = self.ensure_open()?;
        let arrow_schema = reader_metadata.schema();

        let mut result = ZendHashTable::new();

        for field in arrow_schema.fields() {
            let entry = field_to_php_schema(field)?;
            result
                .push(entry)
                .map_err(|_| parquet_exception("Failed to build schema array"))?;
        }

        Ok(result)
    }

    pub fn metadata(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let (_, reader_metadata) = self.ensure_open()?;
        let metadata = reader_metadata.metadata();

        let mut result = ZendHashTable::new();
        result
            .insert("rows", metadata.file_metadata().num_rows())
            .map_err(|_| parquet_exception("Failed to build metadata"))?;
        result
            .insert("row_groups", metadata.num_row_groups() as i64)
            .map_err(|_| parquet_exception("Failed to build metadata"))?;

        match metadata.file_metadata().created_by() {
            Some(created_by) => {
                result
                    .insert("created_by", created_by)
                    .map_err(|_| parquet_exception("Failed to build metadata"))?;
            }
            None => {
                result
                    .insert("created_by", ())
                    .map_err(|_| parquet_exception("Failed to build metadata"))?;
            }
        }

        let mut kv_metadata = ZendHashTable::new();

        if let Some(kv_pairs) = metadata.file_metadata().key_value_metadata() {
            for kv in kv_pairs {
                if let Some(ref value) = kv.value {
                    kv_metadata
                        .insert(kv.key.as_str(), value.as_str())
                        .map_err(|_| parquet_exception("Failed to build metadata"))?;
                }
            }
        }

        result
            .insert("metadata", kv_metadata)
            .map_err(|_| parquet_exception("Failed to build metadata"))?;

        Ok(result)
    }

    #[allow(non_snake_case)]
    pub fn readRowGroup(
        &mut self,
        columns: Option<Vec<String>>,
    ) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        loop {
            if let Some(ref mut batch_reader) = self.active_batch_reader {
                match batch_reader.next() {
                    Some(Ok(batch)) => {
                        return Ok(Some(Self::batch_to_php(&batch, &self.active_columns)?));
                    }
                    Some(Err(e)) => {
                        self.active_batch_reader = None;
                        return Err(parquet_exception(format!("Failed to read batch: {}", e)));
                    }
                    None => {
                        self.active_batch_reader = None;
                        self.current_row_group += 1;
                        continue;
                    }
                }
            }

            if self.current_row_group >= self.num_row_groups {
                return Ok(None);
            }

            let (stream, reader_metadata) = self.ensure_open()?;
            let arrow_schema = reader_metadata.schema().clone();
            let parquet_metadata = reader_metadata.metadata().clone();
            let cloned_stream = stream.clone();
            let cloned_metadata = reader_metadata.clone();

            let selected_columns: Vec<String> = if let Some(ref cols) = columns {
                for col_name in cols {
                    if arrow_schema.field_with_name(col_name).is_err() {
                        return Err(parquet_exception(format!(
                            "Column '{}' not found in schema",
                            col_name
                        )));
                    }
                }
                cols.clone()
            } else {
                arrow_schema
                    .fields()
                    .iter()
                    .map(|f| f.name().clone())
                    .collect()
            };

            let parquet_schema = parquet_metadata.file_metadata().schema_descr();

            let mut builder =
                ParquetRecordBatchReaderBuilder::new_with_metadata(cloned_stream, cloned_metadata);
            builder = builder.with_row_groups(vec![self.current_row_group]);

            if columns.is_some() {
                let indices: Result<Vec<usize>, _> = selected_columns
                    .iter()
                    .map(|name| {
                        arrow_schema
                            .fields()
                            .iter()
                            .position(|f| f.name() == name)
                            .ok_or_else(|| {
                                parquet_exception(format!("Column '{}' not found in schema", name))
                            })
                    })
                    .collect();
                let indices = indices?;
                let mask = ProjectionMask::roots(parquet_schema, indices);
                builder = builder.with_projection(mask);
            }

            if let Some(bs) = self.batch_size {
                builder = builder.with_batch_size(bs);
            }

            let reader = builder
                .build()
                .map_err(|e| parquet_exception(format!("Failed to build batch reader: {}", e)))?;

            self.active_columns = selected_columns;

            if self.batch_size.is_some() {
                self.active_batch_reader = Some(reader);
                continue;
            } else {
                let result = Self::consume_all_batches(reader, &self.active_columns)?;
                self.current_row_group += 1;
                self.active_columns = Vec::new();
                return Ok(result);
            }
        }
    }

    pub fn close(&mut self) {
        self.active_batch_reader = None;
        self.stream = None;
        self.reader_metadata = None;
        self.active_columns = Vec::new();
    }
}
