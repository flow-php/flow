//! `Flow\ETL\Adapter\Parquet\{RustParquetOpenSource, RustParquetFileWriter}`: `Rows` of `RustColumn`s straight from
//! arrow-ext's Parquet batches, and any conformed `Rows` straight to its writer, through the Arrow C Data Interface.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_select::concat::concat;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_arrow_c::Expected;

use crate::arrow_c;
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

const FILE_READER: &str = "Flow\\Parquet\\Engine\\RustParquetFileReader";
const ARROW_FILE_WRITER: &str = "Flow\\Parquet\\Engine\\RustParquetFileWriter";
const BATCH_READER: &str = "Flow\\Arrow\\Parquet\\RustBatchReader";
const SKIPPED_ROWS: &str = "Flow\\ETL\\IO\\Extractor\\File\\SkippedRows";

type Imported = (usize, Vec<ArrayRef>);

// PhpParquetOpenSource fills every batch across row groups, so a group's remainder is carried into the next batch
fn next_full_batch(
    batches: &Zval,
    expected: &Expected,
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

        let batch = call_method(batches, "next", &mut [])?;

        if batch.is_null() {
            return if carried.is_empty() {
                Ok(None)
            } else {
                held(joined(std::mem::take(carried))?, columns).map(Some)
            };
        }

        carried.push(arrow_c::import(&batch, expected, columns)?);
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

fn skipped_rows(batches: &Zval) -> PhpResult<Zval> {
    let count = call_method(batches, "skipped", &mut [])?
        .long()
        .ok_or_else(|| ext_exception("flow_php expected RustBatchReader::skipped() to return an int"))?;

    Ok(construct(find_class(SKIPPED_ROWS)?, &mut [zval_long(count)])?.into_zval(false)?)
}

/// `$object` is arrow-ext's `$class`: the only object whose Arrow state flow_php can read.
fn expect_arrow(object: &Zval, class: &str) -> PhpResult<()> {
    if object
        .object()
        .is_some_and(|object| find_class(class).is_ok_and(|ce| object.instance_of(ce)))
    {
        return Ok(());
    }

    Err(ext_exception(format!("flow_php expected a {class}")))
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\Parquet\\IO\\Extractor\\RustParquetOpenSource",
    flags = ClassFlags::Final,
    implements(ce = parquet_open_source_ce, stub = "Flow\\ETL\\Adapter\\Parquet\\IO\\Extractor\\ParquetOpenSource")
)]
pub struct RustParquetOpenSource {
    file: Zval,
    /// The `RustBatchReader` of the last `batches()`, closed by the next one and by `close()`.
    batches: RefCell<Option<Zval>>,
}

#[php_impl]
impl RustParquetOpenSource {
    /// `$file`: arrow-ext's `Flow\Parquet\Engine\RustParquetFileReader`, which owns the stream.
    pub fn __construct(file: &Zval) -> PhpResult<Self> {
        expect_arrow(file, FILE_READER)?;

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
        let mut names = ZendHashTable::new();

        for (name, _) in &columns {
            names.push(zval_str(name))?;
        }

        let mut names_zv = Zval::new();
        names_zv.set_hashtable(names);

        self.close()?;

        let batches = construct(
            find_class(BATCH_READER)?,
            &mut [
                self.file.shallow_clone(),
                names_zv,
                zval_long(batchSize),
                plan.shallow_clone(),
            ],
        )?
        .into_zval(false)?;
        *self.batches.borrow_mut() = Some(batches.shallow_clone());

        let expected = arrow_c::check_schema(&call_method(&batches, "schema", &mut [])?, &columns)?;
        let adoption = Adoption::new(definitions, schema, backend);
        let mut reports_skipped = read_property(expect_object(plan, "a ReadPlan")?, "offset")?
            .long()
            .unwrap_or(0)
            > 0;
        let mut first_rows: Option<Zval> = None;
        let mut carried: Vec<Imported> = Vec::new();
        // RustBatchReader's constructor refused a batch size below 1
        let batch_size = batchSize as usize;

        Ok(RustIterator::new(Box::new(move || {
            if let Some(rows) = first_rows.take() {
                return Ok(Some(rows));
            }

            let rows = next_full_batch(&batches, &expected, &columns, batch_size, &mut carried)?
                .map(|batch| adopt_batch(&adoption, batch))
                .transpose()?;

            if !std::mem::take(&mut reports_skipped) {
                return Ok(rows);
            }

            // the reader's count is final once its first batch, or its end, is reached
            first_rows = rows;

            Ok(Some(skipped_rows(&batches)?))
        })))
    }

    pub fn close(&self) -> PhpResult<()> {
        // the borrow ends before close() runs PHP
        let batches = self.batches.borrow_mut().take();

        if let Some(batches) = batches {
            call_method(&batches, "close", &mut [])?;
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
        expect_arrow(writer, ARROW_FILE_WRITER)?;

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

        let mut batch = Zval::new();
        arrow_c::export(count, arrays)?.set_zval(&mut batch, false)?;
        call_method(&self.writer, "writeArrowBatch", &mut [batch])?;

        Ok(())
    }

    /// The writer's buffered rows, the footer, then the stream closed.
    pub fn close(&mut self) -> PhpResult<()> {
        call_method(&self.writer, "close", &mut [])?;

        Ok(())
    }
}
