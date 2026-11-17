//! `Flow\ETL\Adapter\Parquet\{RustParquetOpenSource, RustParquetFileWriter}`: `Rows` of `RustColumn`s straight from
//! the batches of the `flow_php` Parquet engine, and any conformed `Rows` straight to its writer, in process.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_select::concat::concat;
use ext_php_rs::convert::{FromZvalMut, IntoZval};
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::kind::data_type;
use flow_parquet_engine::library::{
    batch_size, read_plan, RustParquetFileReader as EngineFileReader, RustParquetFileWriter as EngineFileWriter,
};
use flow_parquet_engine::read::ParquetReader;
use flow_parquet_engine::render::{parquet_exception, Side};
use flow_parquet_engine::source::PhpStream;

use crate::backend::{adopt_batch, native_column, Adoption, Definitions};
use crate::batch_columns::HeldBatch;
use crate::column::RustColumn;
use crate::ctx::{
    call_method, column_key, construct, expect_object, find_class, ht_for_each, ht_insert, read_property, zval_long,
    zval_str,
};
use crate::exception::ext_exception;
use crate::interfaces::{file_writer_ce, parquet_open_source_ce};
use crate::iterator::RustIterator;
use crate::plan::TypePlan;
use crate::render::invalid_argument;

const SKIPPED_ROWS: &str = "Flow\\ETL\\IO\\Extractor\\File\\SkippedRows";

type Imported = (usize, Vec<ArrayRef>);

/// The reader of the last `batches()`: emptied by the next one and by `close()`.
type SharedReader = Rc<RefCell<Option<ParquetReader>>>;

// PhpParquetOpenSource fills every batch across row groups, so a group's remainder is carried into the next batch
fn next_full_batch(
    reader: &SharedReader,
    stream: &PhpStream,
    columns: &[(Vec<u8>, Rc<TypePlan>)],
    batch_size: usize,
    carried: &mut Vec<Imported>,
) -> PhpResult<Option<HeldBatch>> {
    loop {
        if carried.iter().map(|(count, _)| count).sum::<usize>() >= batch_size {
            let (full, rest) = split(joined(std::mem::take(carried))?, batch_size);
            carried.extend(rest);

            return held(full, columns).map(Some);
        }

        let batch = reader.borrow_mut().as_mut().and_then(Iterator::next);

        match batch {
            None => {
                return if carried.is_empty() {
                    Ok(None)
                } else {
                    held(joined(std::mem::take(carried))?, columns).map(Some)
                };
            }
            Some(batch) => carried.push(batch.map_err(|error| parquet_exception(error, stream, Side::Read))?),
        }
    }
}

fn split((count, arrays): Imported, at: usize) -> (Imported, Option<Imported>) {
    let head = arrays.iter().map(|array| array.slice(0, at)).collect();
    let rest = (count > at).then(|| {
        (
            count - at,
            arrays.iter().map(|array| array.slice(at, count - at)).collect(),
        )
    });

    ((at, head), rest)
}

// pieces are concatenated once per batch: joining each one onto the carried head copies it again per row group
fn joined(mut pieces: Vec<Imported>) -> PhpResult<Imported> {
    if pieces.len() == 1 {
        return Ok(pieces.remove(0));
    }

    let count = pieces.iter().map(|(count, _)| count).sum();
    let arrays = (0..pieces[0].1.len())
        .map(|column| {
            concat(
                &pieces
                    .iter()
                    .map(|(_, arrays)| arrays[column].as_ref())
                    .collect::<Vec<_>>(),
            )
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ext_exception(format!("flow_php failed to carry rows across row groups: {e}")))?;

    Ok((count, arrays))
}

fn held((count, arrays): Imported, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<HeldBatch> {
    let mut held = ZendHashTable::with_capacity(columns.len() as u32);

    for ((name, plan), array) in columns.iter().zip(arrays) {
        let mut native = Zval::new();
        RustColumn::new(array, Rc::clone(plan)).set_zval(&mut native, false)?;
        ht_insert(&mut held, name, native);
    }

    Ok(HeldBatch { columns: held, count })
}

fn skipped_rows(reader: &SharedReader) -> PhpResult<Zval> {
    let count = reader.borrow().as_ref().map_or(0, ParquetReader::skipped);

    Ok(construct(
        find_class(SKIPPED_ROWS)?,
        &mut [zval_long(i64::try_from(count).unwrap_or(i64::MAX))],
    )?
    .into_zval(false)?)
}

/// Refuses a column whose stored type is not the one its schema type stores, before any batch.
fn stored_types(reader: &ParquetReader, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<()> {
    for (((name, plan), canonical), field) in columns.iter().zip(reader.types()).zip(reader.fields()) {
        if *canonical == data_type(&plan.kind) {
            continue;
        }

        let type_name = call_method(&plan.type_zv, "toString", &mut [])?;

        return Err(invalid_argument(format!(
            "Parquet column \"{}\" ({}) is read as {}, its schema type {} stores {}",
            String::from_utf8_lossy(name),
            field.data_type(),
            canonical,
            String::from_utf8_lossy(type_name.zend_str().map_or(&b""[..], |name| name.as_bytes())),
            data_type(&plan.kind),
        )));
    }

    Ok(())
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\Parquet\\IO\\Extractor\\RustParquetOpenSource",
    flags = ClassFlags::Final,
    implements(ce = parquet_open_source_ce, stub = "Flow\\ETL\\Adapter\\Parquet\\IO\\Extractor\\ParquetOpenSource")
)]
pub struct RustParquetOpenSource {
    file: Zval,
    batches: RefCell<Option<SharedReader>>,
}

#[php_impl]
impl RustParquetOpenSource {
    /// `$file`: the `flow_php` engine's `FlowPhpParquetFileReader`, which owns the stream.
    pub fn __construct(file: &Zval) -> PhpResult<Self> {
        if file.extract::<&EngineFileReader>().is_none() {
            return Err(ext_exception(
                "flow_php expected a Flow\\ETL\\Adapter\\Parquet\\IO\\Engine\\FlowPhpParquetFileReader",
            ));
        }

        Ok(Self {
            file: file.shallow_clone(),
            batches: RefCell::new(None),
        })
    }

    /// Batches of at most `$batchSize` rows of `$schema`'s columns, the rows `$plan` selects, every column adopted by
    /// `$backend`; the file's schema is checked before any batch.
    pub fn batches(&self, schema: &Zval, batchSize: i64, plan: &Zval, backend: &Zval) -> PhpResult<RustIterator> {
        let definitions = Definitions::of(schema)?;
        let columns = definitions.plans();
        let names = columns
            .iter()
            .map(|(name, _)| String::from_utf8_lossy(name).into_owned())
            .collect::<Vec<_>>();

        self.close()?;

        let file = self
            .file
            .extract::<&EngineFileReader>()
            .ok_or_else(|| ext_exception("flow_php expected its FlowPhpParquetFileReader"))?;
        let batch_size = batch_size(batchSize)?;
        let (stream, meta) = file.read()?;
        let stream = Arc::clone(stream);
        let read = read_plan(meta, plan, &stream)?;
        let reader = ParquetReader::open(Arc::clone(&stream), meta, &names, read, batch_size)
            .map_err(|error| parquet_exception(error, &stream, Side::Read))?;
        stored_types(&reader, &columns)?;

        let reader: SharedReader = Rc::new(RefCell::new(Some(reader)));
        *self.batches.borrow_mut() = Some(Rc::clone(&reader));

        let adoption = Adoption::new(definitions, schema, backend);
        let mut reports_skipped = read_property(expect_object(plan, "a ReadPlan")?, "offset")?
            .long()
            .unwrap_or(0)
            > 0;
        let mut first_rows: Option<Zval> = None;
        let mut carried: Vec<Imported> = Vec::new();

        Ok(RustIterator::new(Box::new(move || {
            if let Some(rows) = first_rows.take() {
                return Ok(Some(rows));
            }

            let rows = next_full_batch(&reader, &stream, &columns, batch_size, &mut carried)?
                .map(|batch| adopt_batch(&adoption, batch))
                .transpose()?;

            if !std::mem::take(&mut reports_skipped) {
                return Ok(rows);
            }

            // the reader's count is final once its first batch, or its end, is reached
            first_rows = rows;

            Ok(Some(skipped_rows(&reader)?))
        })))
    }

    pub fn close(&self) -> PhpResult<()> {
        if let Some(reader) = self.batches.borrow_mut().take() {
            reader.borrow_mut().take();
        }

        Ok(())
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\Parquet\\IO\\Loader\\RustParquetFileWriter",
    flags = ClassFlags::Final,
    implements(ce = file_writer_ce, stub = "Flow\\ETL\\IO\\Loader\\File\\FileWriter")
)]
pub struct RustParquetFileWriter {
    writer: Zval,
    conformance: Zval,
    backend: Zval,
}

#[php_impl]
impl RustParquetFileWriter {
    /// `$writer` owns the stream; `$conformance` and `$backend` come from the run.
    pub fn __construct(writer: &Zval, conformance: &Zval, backend: &Zval) -> PhpResult<Self> {
        if writer.extract::<&EngineFileWriter>().is_none() {
            return Err(ext_exception(
                "flow_php expected a Flow\\ETL\\Adapter\\Parquet\\IO\\Engine\\FlowPhpParquetFileWriter",
            ));
        }

        Ok(Self {
            writer: writer.shallow_clone(),
            conformance: conformance.shallow_clone(),
            backend: backend.shallow_clone(),
        })
    }

    /// Every column of `$rows` conformed to the run's Parquet schema - a `RustColumn` as it is, any other column
    /// adopted into one - as one batch; the writer takes its columns by name and writes a column the batch lacks as
    /// nulls.
    pub fn write(&mut self, rows: &Zval) -> PhpResult<()> {
        let rows = call_method(
            &self.conformance,
            "conform",
            &mut [rows.shallow_clone(), self.backend.shallow_clone()],
        )?;
        let rows = &rows;
        let count = call_method(rows, "count", &mut [])?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))?
            as usize;
        let columns = call_method(rows, "columns", &mut [])?;
        let columns = columns
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Rows::columns() to return an array"))?;
        let schema = call_method(rows, "schema", &mut [])?;
        let mut arrays = Vec::with_capacity(columns.len());

        ht_for_each(columns, |name, index, column| {
            let name = column_key(name, index);
            let data = match column.extract::<&RustColumn>() {
                Some(native) => Arc::clone(native.data()),
                None => native_column(&call_method(&schema, "get", &mut [zval_str(&name)])?, column)?.1,
            };

            arrays.push((String::from_utf8_lossy(&name).into_owned(), data));

            Ok(())
        })?;

        <&mut EngineFileWriter>::from_zval_mut(&mut self.writer)
            .ok_or_else(|| ext_exception("flow_php expected its FlowPhpParquetFileWriter"))?
            .write_arrow_columns(count, &arrays)
    }

    /// The writer's buffered rows, the footer, then the stream closed.
    pub fn close(&mut self) -> PhpResult<()> {
        call_method(&self.writer, "close", &mut [])?;

        Ok(())
    }
}
