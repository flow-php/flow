//! `Flow\Arrow\Parquet\{ParquetFile, ColumnsReader, RowsWriter}`: the `flow-php/parquet` lib's values over the core -
//! one footer read per file, columns as lists of the values `PhpParquetEngine` returns, rows written as it accepts
//! them, and struct batches written as another extension exports them.

use std::sync::Arc;

use arrow_array::{new_null_array, Array, ArrayRef};
use ext_php_rs::boxed::ZBox;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::Length;

use crate::parquet::batch;
use crate::parquet::cells::{Checked, Classes, ReadCell, Refusal, Scalar, Times, Validated, WriteColumn};
use crate::parquet::error::Error;
use crate::parquet::footer::Footer;
use crate::parquet::options;
use crate::parquet::read::{ParquetReader, ReadPlan};
use crate::parquet::sink::PhpSink;
use crate::parquet::source::{PhpSource, PhpStream};
use crate::parquet::write::ParquetWriter;
use crate::php::{ht_for_each, ht_get, ht_insert};
use crate::render::{exception, parquet_exception, Side};
use crate::thrift;

const FILE_META_DATA: &str = "Flow\\Parquet\\ThriftModel\\FileMetaData";
/// `FileMetaData::$schema`'s field id.
const SCHEMA: i16 = 2;
const RUNTIME: &str = "Flow\\Parquet\\Exception\\RuntimeException";
pub const INVALID_ARGUMENT: &str = "Flow\\Parquet\\Exception\\InvalidArgumentException";

fn closed() -> PhpException {
    exception(RUNTIME, "Parquet file is closed".to_string())
}

pub fn rows(value: Option<i64>, name: &str) -> PhpResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| exception(INVALID_ARGUMENT, format!("arrow Parquet {name} must be greater or equal to 0")))
        })
        .transpose()
}

pub fn batch_size(value: i64) -> PhpResult<usize> {
    usize::try_from(value)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| exception(INVALID_ARGUMENT, "arrow Parquet batch size must be greater than 0".to_string()))
}

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\ParquetFile", flags = ClassFlags::Final)]
pub struct ParquetFile {
    stream: Arc<PhpStream>,
    source: PhpSource,
    footer: Footer,
    schema: Option<Zval>,
    thrift: Option<Zval>,
    closed: bool,
}

impl ParquetFile {
    fn open(&self) -> PhpResult<()> {
        if self.closed {
            return Err(closed());
        }

        Ok(())
    }

    /// The file's stream, size and footer, for a reader of it: the lib's `ColumnsReader` or a `BatchReader`.
    pub fn read(&self) -> PhpResult<(&Arc<PhpStream>, u64, &Arc<ParquetMetaData>)> {
        self.open()?;

        Ok((&self.stream, self.source.len(), self.footer.meta()))
    }
}

#[php_impl]
impl ParquetFile {
    /// Reads and decodes the footer; owns the stream from here on.
    pub fn __construct(stream: &Zval) -> PhpResult<Self> {
        let stream = Arc::new(PhpStream::new(stream)?);
        let refused = |error| parquet_exception(error, &stream, Side::Read);
        let source = PhpSource::new(Arc::clone(&stream)).map_err(refused)?;
        let footer = Footer::read(&source).map_err(refused)?;

        Ok(Self {
            stream,
            source,
            footer,
            schema: None,
            thrift: None,
            closed: false,
        })
    }

    /// `FileMetaData::$schema`: the list of `SchemaElement`s, decoded on the first call.
    pub fn schema(&mut self) -> PhpResult<Zval> {
        self.open()?;

        if let Some(schema) = &self.schema {
            return Ok(schema.shallow_clone());
        }

        let schema = thrift::decode(self.footer.bytes(), FILE_META_DATA, Some(SCHEMA))?;
        self.schema = Some(schema.shallow_clone());

        Ok(schema)
    }

    #[php(name = "rowsNumber")]
    pub fn rows_number(&self) -> PhpResult<i64> {
        self.open()?;

        Ok(self.footer.meta().file_metadata().num_rows())
    }

    /// Σ row group `total_byte_size` (uncompressed).
    #[php(name = "totalByteSize")]
    pub fn total_byte_size(&self) -> PhpResult<i64> {
        self.open()?;

        Ok(self.footer.meta().row_groups().iter().map(|group| group.total_byte_size()).sum())
    }

    /// The whole footer as `FileMetaData`, decoded on the first call.
    pub fn thrift(&mut self) -> PhpResult<Zval> {
        self.open()?;

        if let Some(thrift) = &self.thrift {
            return Ok(thrift.shallow_clone());
        }

        let thrift = thrift::decode(self.footer.bytes(), FILE_META_DATA, None)?;
        self.thrift = Some(thrift.shallow_clone());

        Ok(thrift)
    }

    /// Closes the stream; any later call throws.
    pub fn close(&mut self) -> PhpResult<()> {
        self.open()?;
        self.closed = true;
        self.schema = None;
        self.thrift = None;

        match self.stream.call("close", &mut []) {
            Some(_) => Ok(()),
            None => Err(parquet_exception(
                Error::Stream("close() threw".to_string()),
                &self.stream,
                Side::Read,
            )),
        }
    }
}

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\ColumnsReader", flags = ClassFlags::Final)]
pub struct ColumnsReader {
    reader: Option<ParquetReader>,
    stream: Arc<PhpStream>,
    columns: Vec<(Vec<u8>, ReadCell)>,
    times: Option<Times>,
}

#[php_impl]
impl ColumnsReader {
    /// `$columns`: root names or struct paths; `$batchSize` rows at most per `next()`.
    pub fn __construct(
        file: &ParquetFile,
        columns: &ZendHashTable,
        batch_size: i64,
        offset: Option<i64>,
        limit: Option<i64>,
    ) -> PhpResult<Self> {
        let batch_size = self::batch_size(batch_size)?;
        let (offset, limit) = (rows(offset, "offset")?.unwrap_or(0), rows(limit, "limit")?);
        let (stream, size, meta) = file.read()?;
        let stream = Arc::clone(stream);
        let refused = |error| parquet_exception(error, &stream, Side::Read);
        let mut names = Vec::with_capacity(columns.len());

        ht_for_each(columns, |_, _, name| {
            names.push(
                name.str()
                    .ok_or_else(|| exception(INVALID_ARGUMENT, "arrow Parquet column names must be strings".to_string()))?
                    .to_string(),
            );

            Ok(())
        })?;

        let reader =
            ParquetReader::open(Arc::clone(&stream), size, meta, &names, ReadPlan::new(meta, offset, limit), batch_size)
                .map_err(refused)?;
        let cells = reader
            .fields()
            .iter()
            .zip(reader.types())
            .map(|(field, canonical)| ReadCell::new(canonical, field).map_err(|e| e.in_column(field.name())))
            .collect::<Result<Vec<_>, Error>>()
            .map_err(refused)?;
        let times = if cells.iter().any(ReadCell::needs_times) { Some(Times::new()?) } else { None };

        Ok(Self {
            reader: Some(reader),
            stream,
            columns: names.into_iter().map(String::into_bytes).zip(cells).collect(),
            times,
        })
    }

    /// At most `$batchSize` rows as lists keyed by column, null after the last.
    pub fn next(&mut self) -> PhpResult<Option<ZBox<ZendHashTable>>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };

        let arrays: Vec<ArrayRef> = match reader.next() {
            None => {
                self.reader = None;

                return Ok(None);
            }
            Some(batch) => {
                batch
                    .map_err(|error| parquet_exception(error, &self.stream, Side::Read))?
                    .1
            }
        };

        let mut chunk = ZendHashTable::with_capacity(self.columns.len() as u32);

        for ((name, cell), array) in self.columns.iter().zip(&arrays) {
            ht_insert(&mut chunk, name, cell.values(array, self.times.as_ref())?);
        }

        Ok(Some(chunk))
    }

    pub fn close(&mut self) {
        self.reader = None;
    }
}

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\RowsWriter", flags = ClassFlags::Final)]
pub struct RowsWriter {
    writer: Option<ParquetWriter>,
    stream: Arc<PhpStream>,
    columns: Vec<WriteColumn>,
    classes: Classes,
    buffered: usize,
    /// Every column holds one value per row: rows are checked into `Scalar`s.
    scalar: bool,
    batch_size: usize,
}

impl Drop for RowsWriter {
    fn drop(&mut self) {
        self.stream.detach();
    }
}

impl RowsWriter {
    fn refused(&self, error: Error) -> PhpException {
        parquet_exception(error, &self.stream, Side::Write)
    }

    fn open(&self) -> PhpResult<()> {
        match self.writer {
            Some(_) => Ok(()),
            None => Err(exception(RUNTIME, "Writer is not open".to_string())),
        }
    }

    /// The refusal of `column` at buffered row `row`.
    fn refusal(&self, refusal: Refusal, column: String, row: usize) -> PhpException {
        match refusal {
            Refusal::Value { expected, got } => self.refused(Error::Value {
                column,
                row,
                expected,
                got,
            }),
            Refusal::Overflow => self.refused(Error::Overflow { column, row }),
            Refusal::Php(exception) => exception,
        }
    }

    /// The buffered rows through `ParquetWriter::write`.
    fn flush(&mut self) -> PhpResult<()> {
        if self.buffered == 0 {
            return Ok(());
        }

        let arrays = self.columns.iter_mut().map(WriteColumn::take).collect::<Vec<_>>();
        let rows = std::mem::take(&mut self.buffered);
        let writer = self.writer.as_mut().ok_or_else(|| exception(RUNTIME, "Writer is not open".to_string()))?;

        writer
            .write(rows, &arrays)
            .map_err(|error| parquet_exception(error, &self.stream, Side::Write))
    }

    /// One row whose every column was checked (`value` of column index): appended, unless a column's offsets would
    /// pass i32, which refuses the row whole before anything is appended.
    fn append_checked<'a, 'b, V: Checked<'a> + 'b>(&mut self, value: impl Fn(usize) -> &'b V) -> PhpResult<()> {
        let overflow = self
            .columns
            .iter_mut()
            .enumerate()
            .position(|(index, column)| !value(index).fits(&column.cell, &mut column.builder));

        if let Some(index) = overflow {
            return Err(self.refused(Error::Overflow {
                column: self.columns[index].name.clone(),
                row: self.buffered,
            }));
        }

        for (index, column) in self.columns.iter_mut().enumerate() {
            value(index).append(&column.cell, &mut column.builder);
        }

        self.buffered += 1;

        Ok(())
    }

    /// A row is checked whole into `checked` first; a refused row appends nothing.
    fn append<'a, V: Checked<'a>>(&mut self, row: &'a Zval, checked: &mut Vec<V>) -> PhpResult<()> {
        self.open()?;

        let row = row
            .array()
            .ok_or_else(|| exception(INVALID_ARGUMENT, "arrow Parquet writer rows must be arrays".to_string()))?;
        checked.clear();

        for column in &self.columns {
            let value = match ht_get(row, column.name.as_bytes()) {
                Some(value) => V::check(&column.cell, value, column.nullable, &self.classes),
                None => V::missing(column.nullable),
            };

            match value {
                Ok(value) => checked.push(value),
                Err(refusal) => return Err(self.refusal(refusal, column.name.clone(), self.buffered)),
            }
        }

        self.append_checked(|index| &checked[index])?;

        if self.buffered >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }

    fn append_rows<'a, V: Checked<'a>>(&mut self, rows: &'a ZendHashTable) -> PhpResult<()> {
        let mut checked = Vec::with_capacity(self.columns.len());

        for row in rows.values() {
            self.append::<V>(row, &mut checked)?;
        }

        Ok(())
    }

    /// `rows` values of every column from `first` on, column by column; a refused cell lowers the row limit of the
    /// columns after it, so the refusal raised and the rows kept are those of `append()` row by row.
    fn append_columns<'a, V: Checked<'a>>(&mut self, lists: &[Option<Vec<&'a Zval>>], first: usize, rows: usize) -> PhpResult<()> {
        let mut limit = rows;
        let mut refused = None;
        let mut checked: Vec<Vec<V>> = Vec::with_capacity(self.columns.len());

        for (index, column) in self.columns.iter().enumerate() {
            let mut values = Vec::with_capacity(limit);
            let mut stop = None;

            for row in 0..limit {
                let value = match &lists[index] {
                    Some(values) => V::check(&column.cell, values[first + row], column.nullable, &self.classes),
                    None => V::missing(column.nullable),
                };

                match value {
                    Ok(value) => values.push(value),
                    Err(refusal) => {
                        stop = Some((row, refusal));

                        break;
                    }
                }
            }

            if let Some((row, refusal)) = stop {
                limit = row;
                refused = Some((column.name.clone(), refusal));
            }

            checked.push(values);
        }

        (0..limit).try_for_each(|row| self.append_checked(|index| &checked[index][row]))?;

        if let Some((name, refusal)) = refused {
            return Err(self.refusal(refusal, name, self.buffered));
        }

        if self.buffered >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }
}

#[php_impl]
impl RowsWriter {
    /// `$schema` / `$options`: `Flow\Parquet\Engine\Arrow\SchemaConverter` / `OptionsConverter::toExtension()`;
    /// `$batchSize` rows per `ParquetWriter::write`.
    pub fn __construct(
        stream: &Zval,
        schema: &ZendHashTable,
        compression: String,
        options: &ZendHashTable,
        batch_size: i64,
    ) -> PhpResult<Self> {
        let batch_size = self::batch_size(batch_size)?;
        let stream = Arc::new(PhpStream::new(stream)?);
        let refused = |error| parquet_exception(error, &stream, Side::Write);
        let schema = options::schema(schema).map_err(refused)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| WriteColumn::new(field, batch_size))
            .collect::<Result<Vec<_>, Error>>()
            .map_err(refused)?;
        let scalar = columns.iter().all(|column| column.cell.is_scalar());
        let writer = ParquetWriter::open(
            PhpSink::new(Arc::clone(&stream)),
            schema,
            options::properties(&compression, options).map_err(refused)?,
        )
        .map_err(refused)?;

        Ok(Self {
            writer: Some(writer),
            stream,
            columns,
            classes: Classes::new()?,
            buffered: 0,
            scalar,
            batch_size,
        })
    }

    /// A writer column the row lacks is null.
    #[php(name = "writeRow")]
    pub fn write_row(&mut self, row: &Zval) -> PhpResult<()> {
        if self.scalar {
            self.append::<Scalar>(row, &mut Vec::with_capacity(self.columns.len()))
        } else {
            self.append::<Validated>(row, &mut Vec::with_capacity(self.columns.len()))
        }
    }

    #[php(name = "writeRows")]
    pub fn write_rows(&mut self, rows: &ZendHashTable) -> PhpResult<()> {
        if self.scalar {
            self.append_rows::<Scalar>(rows)
        } else {
            self.append_rows::<Validated>(rows)
        }
    }

    /// Every list of one length; a writer column `$columns` lacks is nulls, a key the writer lacks is ignored.
    #[php(name = "writeColumns")]
    pub fn write_columns(&mut self, columns: &ZendHashTable) -> PhpResult<()> {
        if self.writer.is_none() {
            return Err(exception(RUNTIME, "Writer is not open".to_string()));
        }

        let mut lengths = Vec::with_capacity(columns.len());

        for (key, values) in columns.iter() {
            let values = values.array().ok_or_else(|| {
                exception(INVALID_ARGUMENT, "arrow Parquet writer columns must be arrays".to_string())
            })?;

            lengths.push((key.to_string(), values.len()));
        }

        let count = lengths.first().map_or(0, |(_, length)| *length);

        if lengths.iter().any(|(_, length)| *length != count) {
            let described = lengths.iter().map(|(name, length)| format!("\"{name}\": {length}")).collect::<Vec<_>>();

            return Err(exception(
                INVALID_ARGUMENT,
                format!("writeColumns() takes lists of one length, got {}", described.join(", ")),
            ));
        }

        let lists = self
            .columns
            .iter()
            .map(|column| {
                ht_get(columns, column.name.as_bytes())
                    .and_then(Zval::array)
                    .map(|values| values.values().collect::<Vec<_>>())
            })
            .collect::<Vec<_>>();
        let mut first = 0;

        // in the row door's flush steps, so a refusal names the row index append() names
        while first < count {
            let rows = (count - first).min(self.batch_size - self.buffered);

            if self.scalar {
                self.append_columns::<Scalar>(&lists, first, rows)?;
            } else {
                self.append_columns::<Validated>(&lists, first, rows)?;
            }
            first += rows;
        }

        Ok(())
    }

    /// An Arrow C Data struct batch another extension exported, moved out of `$batch`: its children by writer column
    /// name, a writer column the batch lacks as nulls, a child the writer lacks ignored. Buffered rows go first.
    #[php(name = "writeBatch")]
    pub fn write_batch(&mut self, batch: &Zval) -> PhpResult<()> {
        self.open()?;
        self.flush()?;

        let batch = batch::import(batch)?;
        let rows = batch.len();
        let writer = self.writer.as_mut().ok_or_else(|| exception(RUNTIME, "Writer is not open".to_string()))?;
        let arrays = writer
            .schema()
            .fields()
            .iter()
            .map(|field| match batch.column_by_name(field.name()) {
                Some(column) => Arc::clone(column),
                None => new_null_array(field.data_type(), rows),
            })
            .collect::<Vec<ArrayRef>>();

        writer
            .write(rows, &arrays)
            .map_err(|error| parquet_exception(error, &self.stream, Side::Write))
    }

    /// Buffered rows, the footer, then the stream closed.
    pub fn close(&mut self) -> PhpResult<()> {
        let flushed = self.flush();
        let writer = self.writer.take().ok_or_else(|| exception(RUNTIME, "Writer is not open".to_string()))?;
        flushed?;
        writer.close().map_err(|error| self.refused(error))?;

        match self.stream.call("close", &mut []) {
            Some(_) => Ok(()),
            None => Err(self.refused(Error::Stream("close() threw".to_string()))),
        }
    }
}
