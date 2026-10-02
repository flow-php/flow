//! `Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter}`: `Rows` of `NativeColumn`s straight from
//! arrow-ext's Parquet batches, and any `Rows` straight to its writer, through the Arrow C Data Interface.

use std::rc::Rc;
use std::sync::Arc;

use ext_php_rs::convert::IntoZval;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::arrow_c;
use crate::backend::DefaultBackend;
use crate::column::NativeColumn;
use crate::ctx::{call_method, call_static, ht_for_each, ht_insert, null_zval, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::plan::{type_plan, TypePlan};

/// `Schema::definitions()` as (name, definition): an int key is a numeric column name.
fn definitions(schema: &Zval) -> PhpResult<Vec<(Vec<u8>, Zval)>> {
    let definitions = call_method(schema, "definitions", &mut [])?;
    let definitions = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
    let mut named = Vec::with_capacity(definitions.len());

    ht_for_each(definitions, |name, index, definition| {
        named.push((
            name.map_or_else(|| index.to_string().into_bytes(), |name| name.as_bytes().to_vec()),
            definition.shallow_clone(),
        ));

        Ok(())
    })?;

    Ok(named)
}

#[php_class]
#[php(name = "Flow\\ETL\\Adapter\\Parquet\\NativeParquetReader", flags = ClassFlags::Final)]
pub struct NativeParquetReader {
    batches: Zval,
    schema: Zval,
    columns: Vec<(Vec<u8>, Rc<TypePlan>)>,
}

#[php_impl]
impl NativeParquetReader {
    /// `$batches`: a `Flow\Arrow\Parquet\BatchReader` over `$schema`'s columns; its schema is checked here, before
    /// any batch.
    pub fn __construct(batches: &Zval, schema: &Zval) -> PhpResult<Self> {
        let mut columns = Vec::new();

        for (name, definition) in definitions(schema)? {
            columns.push((name, type_plan(&call_method(&definition, "type", &mut [])?)?));
        }

        arrow_c::check_schema(&call_method(batches, "schema", &mut [])?, &columns)?;

        Ok(Self {
            batches: batches.shallow_clone(),
            schema: schema.shallow_clone(),
            columns,
        })
    }

    /// The next batch as `Rows::fromColumns($schema, $columns, $count)`, null after the last.
    pub fn next(&mut self) -> PhpResult<Zval> {
        let batch = call_method(&self.batches, "next", &mut [])?;

        if batch.is_null() {
            return Ok(null_zval());
        }

        let (count, arrays) = arrow_c::import(&batch, &self.columns)?;
        let mut columns = ZendHashTable::with_capacity(self.columns.len() as u32);

        for ((name, plan), array) in self.columns.iter().zip(arrays) {
            let mut native = Zval::new();
            NativeColumn::new(array, Rc::clone(plan)).set_zval(&mut native, false)?;
            ht_insert(&mut columns, name, native);
        }

        let mut columns_zv = Zval::new();
        columns_zv.set_hashtable(columns);

        call_static(
            "Flow\\ETL\\Rows",
            "fromColumns",
            &mut [self.schema.shallow_clone(), columns_zv, zval_long(count as i64)],
        )
    }

    pub fn close(&mut self) -> PhpResult<()> {
        call_method(&self.batches, "close", &mut [])?;

        Ok(())
    }
}

#[php_class]
#[php(name = "Flow\\ETL\\Adapter\\Parquet\\NativeParquetWriter", flags = ClassFlags::Final)]
pub struct NativeParquetWriter {
    writer: Zval,
}

#[php_impl]
impl NativeParquetWriter {
    /// `$writer`: a `Flow\Arrow\Parquet\RowsWriter`, which owns the stream.
    pub fn __construct(writer: &Zval) -> Self {
        Self {
            writer: writer.shallow_clone(),
        }
    }

    /// Every column of `$rows` - a `NativeColumn` as it is, any other column adopted into one - as one batch; the
    /// writer takes its columns by name and writes a column the batch lacks as nulls.
    pub fn write(&mut self, rows: &Zval) -> PhpResult<()> {
        let count = call_method(rows, "count", &mut [])?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))? as usize;
        let columns = call_method(rows, "columns", &mut [])?;
        let columns = columns
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Rows::columns() to return an array"))?;
        let schema = call_method(rows, "schema", &mut [])?;
        let mut arrays = Vec::with_capacity(columns.len());

        ht_for_each(columns, |name, index, column| {
            let name = name.map_or_else(|| index.to_string().into_bytes(), |name| name.as_bytes().to_vec());
            let adopted;
            let native = match column.extract::<&NativeColumn>() {
                Some(native) => native,
                None => {
                    let definition = call_method(&schema, "get", &mut [zval_str(&name)])?;
                    adopted = DefaultBackend.adopt(&definition, column)?.0;
                    adopted
                        .extract::<&NativeColumn>()
                        .ok_or_else(|| ext_exception("flow_php expected DefaultBackend::adopt() to build a NativeColumn"))?
                }
            };

            arrays.push((String::from_utf8_lossy(&name).into_owned(), Arc::clone(native.data())));

            Ok(())
        })?;

        let mut batch = Zval::new();
        arrow_c::export(count, arrays)?.set_zval(&mut batch, false)?;
        call_method(&self.writer, "writeBatch", &mut [batch])?;

        Ok(())
    }

    /// The writer's buffered rows, the footer, then the stream closed.
    pub fn close(&mut self) -> PhpResult<()> {
        call_method(&self.writer, "close", &mut [])?;

        Ok(())
    }
}
