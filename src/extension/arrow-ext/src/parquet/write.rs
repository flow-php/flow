//! Canonical arrays to Parquet: each batch cast to the writer schema's types (`to_parquet`) and written by arrow-rs.

use std::sync::Arc;

use arrow_array::{ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{DataType, Field, Fields, Schema, SchemaRef};
use parquet::arrow::arrow_writer::ArrowWriterOptions;
use parquet::arrow::{ArrowSchemaConverter, ArrowWriter};
use parquet::basic::{ConvertedType, LogicalType};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::{BasicTypeInfo, SchemaDescriptor, Type};

use crate::parquet::canonical::to_parquet;
use crate::parquet::error::Error;
use crate::parquet::options::ENUM_METADATA_KEY;
use crate::parquet::sink::PhpSink;

pub struct ParquetWriter {
    writer: ArrowWriter<PhpSink>,
    schema: SchemaRef,
}

impl ParquetWriter {
    /// Fields marked with `ENUM_METADATA_KEY` are written with the ENUM logical type; the marker itself is not.
    pub fn open(sink: PhpSink, schema: Schema, props: WriterProperties) -> Result<Self, Error> {
        let converted = ArrowSchemaConverter::new()
            .with_coerce_types(props.coerce_types())
            .convert(&schema)?;
        let Type::GroupType { basic_info, fields } = converted.root_schema() else {
            unreachable!("the parquet schema root is a group");
        };
        let fields = fields
            .iter()
            .zip(schema.fields())
            .map(|(parquet_type, field)| annotate_enums(parquet_type, field).map(Arc::new))
            .collect::<Result<Vec<_>, _>>()?;
        let root = Type::group_type_builder(basic_info.name())
            .with_fields(fields)
            .build()?;
        let schema = Arc::new(Schema::new_with_metadata(
            schema.fields().iter().map(|field| unmarked(field)).collect::<Vec<_>>(),
            schema.metadata().clone(),
        ));
        let writer = ArrowWriter::try_new_with_options(
            sink,
            Arc::clone(&schema),
            ArrowWriterOptions::new()
                .with_properties(props)
                .with_skip_arrow_metadata(true)
                .with_parquet_schema(SchemaDescriptor::new(Arc::new(root))),
        )?;

        Ok(Self { writer, schema })
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

/// `parquet_type`: the type arrow-rs converts `field` to.
fn annotate_enums(parquet_type: &Type, field: &Field) -> Result<Type, ParquetError> {
    let Type::GroupType { basic_info, fields } = parquet_type else {
        if !field.metadata().contains_key(ENUM_METADATA_KEY) {
            return Ok(parquet_type.clone());
        }

        let basic_info = parquet_type.get_basic_info();

        return Type::primitive_type_builder(basic_info.name(), parquet_type.get_physical_type())
            .with_repetition(basic_info.repetition())
            .with_logical_type(Some(LogicalType::Enum))
            .with_converted_type(ConvertedType::ENUM)
            .with_id(basic_info.has_id().then(|| basic_info.id()))
            .build();
    };
    let fields = match field.data_type() {
        DataType::Struct(children) => fields
            .iter()
            .zip(children)
            .map(|(child_type, child)| annotate_enums(child_type, child))
            .collect::<Result<Vec<_>, _>>()?,
        DataType::Map(entries, _) => fields
            .iter()
            .map(|entries_type| annotate_enums(entries_type, entries))
            .collect::<Result<Vec<_>, _>>()?,
        // arrow-rs writes a list in three levels: the repeated group holds the element as a struct holds its child
        DataType::List(element) => fields
            .iter()
            .map(|repeated| {
                annotate_enums(
                    repeated,
                    &Field::new(
                        repeated.name(),
                        DataType::Struct(Fields::from(vec![Arc::clone(element)])),
                        false,
                    ),
                )
            })
            .collect::<Result<Vec<_>, _>>()?,
        _ => return Ok(parquet_type.clone()),
    };

    group(basic_info, fields)
}

fn group(basic_info: &BasicTypeInfo, fields: Vec<Type>) -> Result<Type, ParquetError> {
    let builder = Type::group_type_builder(basic_info.name())
        .with_fields(fields.into_iter().map(Arc::new).collect())
        .with_logical_type(basic_info.logical_type_ref().cloned())
        .with_converted_type(basic_info.converted_type())
        .with_id(basic_info.has_id().then(|| basic_info.id()));

    if basic_info.has_repetition() {
        builder.with_repetition(basic_info.repetition()).build()
    } else {
        builder.build()
    }
}

/// `field` without `ENUM_METADATA_KEY`, at any depth, so the embedded arrow schema stays as written before the marker.
fn unmarked(field: &Field) -> Field {
    let data_type = match field.data_type() {
        DataType::List(element) => DataType::List(Arc::new(unmarked(element))),
        DataType::Map(entries, sorted) => DataType::Map(Arc::new(unmarked(entries)), *sorted),
        DataType::Struct(children) => DataType::Struct(children.iter().map(|child| unmarked(child)).collect()),
        other => other.clone(),
    };
    let mut metadata = field.metadata().clone();
    metadata.remove(ENUM_METADATA_KEY);

    field.clone().with_data_type(data_type).with_metadata(metadata)
}
