//! Canonical arrays to Parquet: each batch cast to the writer schema's types (`to_parquet`) and written by arrow-rs.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{Schema, SchemaRef};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use crate::parquet::canonical::to_parquet;
use crate::parquet::error::Error;
use crate::parquet::sink::PhpSink;

pub struct ParquetWriter {
    writer: ArrowWriter<PhpSink>,
    schema: SchemaRef,
}

impl ParquetWriter {
    pub fn open(sink: PhpSink, schema: Schema, props: WriterProperties) -> Result<Self, Error> {
        let schema = Arc::new(schema);

        Ok(Self {
            writer: ArrowWriter::try_new(sink, Arc::clone(&schema), Some(props))?,
            schema,
        })
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// `columns`: canonical, one per writer schema field, in its order.
    pub fn write(&mut self, rows: usize, columns: &[ArrayRef]) -> Result<(), Error> {
        let arrays = columns
            .iter()
            .zip(self.schema.fields())
            .map(|(column, field)| to_parquet(column, field.data_type()).map_err(|e| e.in_column(field.name())))
            .collect::<Result<Vec<_>, _>>()?;

        self.writer.write(&RecordBatch::try_new_with_options(
            Arc::clone(&self.schema),
            arrays,
            &RecordBatchOptions::new().with_row_count(Some(rows)),
        )?)?;

        Ok(())
    }

    pub fn close(self) -> Result<(), Error> {
        self.writer.close()?;

        Ok(())
    }
}
