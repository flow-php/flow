//! `Flow\ETL\Adapter\Parquet\{RustParquetOpenSource, RustParquetOpenSink}`: `Rows` of `RustColumn`s straight from
//! arrow-ext's Parquet batches, and any `Rows` straight to its writer, through the Arrow C Data Interface.

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::arrow_c;
use crate::backend::{adopt_batch, native_column};
use crate::batch_columns::HeldBatch;
use crate::column::RustColumn;
use crate::ctx::{
    call_method, column_key, construct, find_class, ht_for_each, ht_insert, null_zval, zval_long, zval_str,
};
use crate::exception::ext_exception;
use crate::interfaces::{parquet_open_sink_ce, parquet_open_source_ce};
use crate::iterator::RustIterator;
use crate::plan::{type_plan, TypePlan};

const FILE_READER: &str = "Flow\\Parquet\\Engine\\RustParquetFileReader";
const FILE_WRITER: &str = "Flow\\Parquet\\Engine\\RustParquetFileWriter";
const BATCH_READER: &str = "Flow\\Arrow\\Parquet\\RustBatchReader";

/// `Schema::definitions()` as (name, definition): an int key is a numeric column name.
fn definitions(schema: &Zval) -> PhpResult<Vec<(Vec<u8>, Zval)>> {
    let definitions = call_method(schema, "definitions", &mut [])?;
    let definitions = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
    let mut named = Vec::with_capacity(definitions.len());

    ht_for_each(definitions, |name, index, definition| {
        named.push((column_key(name, index), definition.shallow_clone()));

        Ok(())
    })?;

    Ok(named)
}

/// The next Arrow C batch of `RustBatchReader` as `RustColumn`s under `columns`' plans.
fn next_batch(batches: &Zval, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<Option<HeldBatch>> {
    let batch = call_method(batches, "next", &mut [])?;

    if batch.is_null() {
        return Ok(None);
    }

    let (count, arrays) = arrow_c::import(&batch, columns)?;
    let mut held = ZendHashTable::with_capacity(columns.len() as u32);

    for ((name, plan), array) in columns.iter().zip(arrays) {
        let mut native = Zval::new();
        RustColumn::new(array, Rc::clone(plan)).set_zval(&mut native, false)?;
        ht_insert(&mut held, name, native);
    }

    Ok(Some(HeldBatch { columns: held, count }))
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
    name = "Flow\\ETL\\Adapter\\Parquet\\RustParquetOpenSource",
    flags = ClassFlags::Final,
    implements(ce = parquet_open_source_ce, stub = "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSource")
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

    /// Batches of at most `$batchSize` rows of `$schema`'s columns, every column adopted by `$backend`; the file's
    /// schema is checked before any batch.
    pub fn batches(
        &self,
        schema: &Zval,
        batchSize: i64,
        offset: Option<i64>,
        limit: Option<i64>,
        backend: &Zval,
    ) -> PhpResult<RustIterator> {
        let mut columns = Vec::new();
        let mut names = ZendHashTable::new();

        for (name, definition) in definitions(schema)? {
            columns.push((name.clone(), type_plan(&call_method(&definition, "type", &mut [])?)?));
            names.push(zval_str(&name))?;
        }

        let mut names_zv = Zval::new();
        names_zv.set_hashtable(names);
        let optional = |value: Option<i64>| value.map_or_else(null_zval, zval_long);

        self.close()?;

        let batches = construct(
            find_class(BATCH_READER)?,
            &mut [
                self.file.shallow_clone(),
                names_zv,
                zval_long(batchSize),
                optional(offset),
                optional(limit),
            ],
        )?
        .into_zval(false)?;
        *self.batches.borrow_mut() = Some(batches.shallow_clone());

        arrow_c::check_schema(&call_method(&batches, "schema", &mut [])?, &columns)?;

        let (schema, backend) = (schema.shallow_clone(), backend.shallow_clone());

        Ok(RustIterator::new(Box::new(move || {
            next_batch(&batches, &columns)?
                .map(|batch| adopt_batch(&schema, &backend, batch))
                .transpose()
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
    name = "Flow\\ETL\\Adapter\\Parquet\\RustParquetOpenSink",
    flags = ClassFlags::Final,
    implements(ce = parquet_open_sink_ce, stub = "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSink")
)]
pub struct RustParquetOpenSink {
    writer: Zval,
}

#[php_impl]
impl RustParquetOpenSink {
    /// `$writer`: arrow-ext's `Flow\Parquet\Engine\RustParquetFileWriter`, which owns the stream.
    pub fn __construct(writer: &Zval) -> PhpResult<Self> {
        expect_arrow(writer, FILE_WRITER)?;

        Ok(Self {
            writer: writer.shallow_clone(),
        })
    }

    /// Every column of `$rows` - a `RustColumn` as it is, any other column adopted into one - as one batch; the
    /// writer takes its columns by name and writes a column the batch lacks as nulls.
    pub fn write(&mut self, rows: &Zval) -> PhpResult<()> {
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
