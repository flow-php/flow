//! `Flow\ETL\Adapter\Parquet\{NativeParquetReader, NativeParquetWriter}`: `Rows` of `NativeColumn`s straight from the
//! core's canonical arrays, and any `Rows` straight to them.

use std::rc::Rc;
use std::sync::Arc;

use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::kind::data_type;

use crate::backend::DefaultBackend;
use crate::column::NativeColumn;
use crate::ctx::{call_method, call_static, ht_for_each, ht_get, ht_insert, null_zval, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::parquet::error::Error;
use crate::parquet::library::NativeParquetFile;
use crate::parquet::options;
use crate::parquet::read::{ParquetReader, ReadPlan};
use crate::parquet::sink::PhpSink;
use crate::parquet::source::PhpStream;
use crate::parquet::write::ParquetWriter;
use crate::plan::{type_plan, TypePlan};
use crate::render::{invalid_argument, parquet_exception, Side, Surface};

fn rows(value: Option<i64>, name: &str) -> PhpResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| invalid_argument(format!("flow_php Parquet {name} must be greater or equal to 0")))
        })
        .transpose()
}

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
    reader: Option<ParquetReader>,
    stream: Arc<PhpStream>,
    schema: Zval,
    columns: Vec<(Vec<u8>, Rc<TypePlan>)>,
}

#[php_impl]
impl NativeParquetReader {
    /// `$file`'s footer plans the read; no footer is read here.
    pub fn __construct(
        file: &NativeParquetFile,
        schema: &Zval,
        batch_size: i64,
        offset: Option<i64>,
        limit: Option<i64>,
    ) -> PhpResult<Self> {
        let batch_size = usize::try_from(batch_size)
            .ok()
            .filter(|size| *size > 0)
            .ok_or_else(|| invalid_argument("flow_php Parquet batch size must be greater than 0".to_string()))?;
        let (offset, limit) = (rows(offset, "offset")?.unwrap_or(0), rows(limit, "limit")?);
        let (stream, size, meta) = file.read()?;
        let stream = Arc::clone(stream);
        let refused = |error| parquet_exception(error, &stream, Side::Read, Surface::Etl);
        let plan = ReadPlan::new(meta, offset, limit);
        let mut columns = Vec::new();
        let mut names = Vec::new();

        for (name, definition) in definitions(schema)? {
            names.push(String::from_utf8_lossy(&name).into_owned());
            columns.push((name, type_plan(&call_method(&definition, "type", &mut [])?)?));
        }

        let reader = ParquetReader::open(Arc::clone(&stream), size, meta, &names, plan, batch_size).map_err(refused)?;

        for ((field, canonical), (name, plan)) in reader.fields().iter().zip(reader.types()).zip(&columns) {
            let stored = data_type(&plan.kind);

            if canonical != &stored {
                let type_name = call_method(&plan.type_zv, "toString", &mut [])?;

                return Err(invalid_argument(format!(
                    "Parquet column \"{}\" ({}) is read as {canonical}, its schema type {} stores {stored}",
                    String::from_utf8_lossy(name),
                    field.data_type(),
                    String::from_utf8_lossy(type_name.zend_str().map_or(&b""[..], |name| name.as_bytes())),
                )));
            }
        }

        Ok(Self {
            reader: Some(reader),
            stream,
            schema: schema.shallow_clone(),
            columns,
        })
    }

    /// The next batch as `Rows::fromColumns($schema, $columns, $count)`, null after the last.
    pub fn next(&mut self) -> PhpResult<Zval> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(null_zval());
        };

        let (count, arrays) = match reader.next() {
            None => {
                self.reader = None;

                return Ok(null_zval());
            }
            Some(batch) => batch.map_err(|error| parquet_exception(error, &self.stream, Side::Read, Surface::Etl))?,
        };

        let mut columns = ZendHashTable::with_capacity(self.columns.len() as u32);

        for ((name, plan), array) in self.columns.iter().zip(arrays) {
            let mut native = Zval::new();
            ext_php_rs::convert::IntoZval::set_zval(NativeColumn::new(array, Rc::clone(plan)), &mut native, false)?;
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

    pub fn close(&mut self) {
        self.reader = None;
    }
}

#[php_class]
#[php(name = "Flow\\ETL\\Adapter\\Parquet\\NativeParquetWriter", flags = ClassFlags::Final)]
pub struct NativeParquetWriter {
    writer: Option<ParquetWriter>,
    stream: Arc<PhpStream>,
}

impl Drop for NativeParquetWriter {
    fn drop(&mut self) {
        self.stream.detach();
    }
}

#[php_impl]
impl NativeParquetWriter {
    /// `$schema`: `Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()`, `$options`: `OptionsConverter::toExtension()`.
    pub fn __construct(stream: &Zval, schema: &ZendHashTable, compression: String, options: &ZendHashTable) -> PhpResult<Self> {
        let stream = Arc::new(PhpStream::new(stream)?);
        let refused = |error| parquet_exception(error, &stream, Side::Write, Surface::Etl);
        let writer = ParquetWriter::open(
            PhpSink::new(Arc::clone(&stream)),
            options::schema(schema).map_err(refused)?,
            options::properties(&compression, options).map_err(refused)?,
        )
        .map_err(refused)?;

        Ok(Self {
            writer: Some(writer),
            stream,
        })
    }

    /// The writer schema's columns by name: a `NativeColumn` as it is, any other column adopted into one, a column
    /// the rows lack as nulls.
    pub fn write(&mut self, rows: &Zval) -> PhpResult<()> {
        let writer = self.writer.as_mut().ok_or_else(|| ext_exception("flow_php Parquet writer is closed"))?;
        let count = call_method(rows, "count", &mut [])?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))? as usize;
        let columns = call_method(rows, "columns", &mut [])?;
        let columns = columns
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Rows::columns() to return an array"))?;
        let schema = call_method(rows, "schema", &mut [])?;
        let mut arrays = Vec::with_capacity(writer.schema().fields().len());

        for field in writer.schema().fields() {
            let Some(column) = ht_get(columns, field.name().as_bytes()) else {
                arrays.push(arrow_array::new_null_array(field.data_type(), count));

                continue;
            };

            let adopted;
            let native = match column.extract::<&NativeColumn>() {
                Some(native) => native,
                None => {
                    let definition = call_method(&schema, "get", &mut [zval_str(field.name().as_bytes())])?;
                    adopted = DefaultBackend.adopt(&definition, column)?.0;
                    adopted
                        .extract::<&NativeColumn>()
                        .ok_or_else(|| ext_exception("flow_php expected DefaultBackend::adopt() to build a NativeColumn"))?
                }
            };

            arrays.push(Arc::clone(native.data()));
        }

        writer
            .write(count, &arrays)
            .map_err(|error| parquet_exception(error, &self.stream, Side::Write, Surface::Etl))
    }

    /// The footer, then the stream closed - as the lib's Parquet writers close theirs.
    pub fn close(&mut self) -> PhpResult<()> {
        self.writer
            .take()
            .ok_or_else(|| ext_exception("flow_php Parquet writer is already closed"))?
            .close()
            .map_err(|error| parquet_exception(error, &self.stream, Side::Write, Surface::Etl))?;

        match self.stream.call("close", &mut []) {
            Some(_) => Ok(()),
            None => Err(parquet_exception(
                Error::Stream("close() threw".to_string()),
                &self.stream,
                Side::Write,
                Surface::Etl,
            )),
        }
    }
}
