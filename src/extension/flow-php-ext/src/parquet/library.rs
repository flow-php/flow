//! `Flow\Parquet\Engine\Native\{NativeParquetFile, NativeParquetColumnsReader, NativeParquetRowsWriter}`: the
//! `flow-php/parquet` lib's values over the core - one footer read per file, columns as lists of the values
//! `PhpParquetEngine` returns, rows written as it accepts them.

use std::sync::Arc;

use arrow_array::ArrayRef;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::Length;

use crate::ctx::{ht_for_each, ht_get, ht_insert};
use crate::parquet::cells::{Classes, ReadCell, Refusal, Times, WriteColumn};
use crate::parquet::error::Error;
use crate::parquet::footer::Footer;
use crate::parquet::options;
use crate::parquet::read::{ParquetReader, ReadPlan};
use crate::parquet::sink::PhpSink;
use crate::parquet::source::{PhpSource, PhpStream};
use crate::parquet::write::ParquetWriter;
use crate::render::{exception, parquet_exception, Side, Surface};
use crate::thrift;

const FILE_META_DATA: &str = "Flow\\Parquet\\ThriftModel\\FileMetaData";
/// `FileMetaData::$schema`'s field id.
const SCHEMA: i16 = 2;
const RUNTIME: &str = "Flow\\Parquet\\Exception\\RuntimeException";
const INVALID_ARGUMENT: &str = "Flow\\Parquet\\Exception\\InvalidArgumentException";

fn closed() -> PhpException {
    exception(RUNTIME, "Parquet file is closed".to_string())
}

fn rows(value: Option<i64>, name: &str) -> PhpResult<Option<u64>> {
    value
        .map(|value| {
            u64::try_from(value)
                .map_err(|_| exception(INVALID_ARGUMENT, format!("flow_php Parquet {name} must be greater or equal to 0")))
        })
        .transpose()
}

fn batch_size(value: i64) -> PhpResult<usize> {
    usize::try_from(value)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| exception(INVALID_ARGUMENT, "flow_php Parquet batch size must be greater than 0".to_string()))
}

#[php_class]
#[php(name = "Flow\\Parquet\\Engine\\Native\\NativeParquetFile", flags = ClassFlags::Final)]
pub struct NativeParquetFile {
    stream: Arc<PhpStream>,
    source: PhpSource,
    footer: Footer,
    schema: Option<Zval>,
    thrift: Option<Zval>,
    closed: bool,
}

impl NativeParquetFile {
    fn open(&self) -> PhpResult<()> {
        if self.closed {
            return Err(closed());
        }

        Ok(())
    }

    /// The file's stream, size and footer, for a reader of it: the ETL's `NativeParquetReader` or the lib's.
    pub fn read(&self) -> PhpResult<(&Arc<PhpStream>, u64, &Arc<ParquetMetaData>)> {
        self.open()?;

        Ok((&self.stream, self.source.len(), self.footer.meta()))
    }
}

#[php_impl]
impl NativeParquetFile {
    /// Reads and decodes the footer; owns the stream from here on.
    pub fn __construct(stream: &Zval) -> PhpResult<Self> {
        let stream = Arc::new(PhpStream::new(stream)?);
        let refused = |error| parquet_exception(error, &stream, Side::Read, Surface::Lib);
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
                Surface::Lib,
            )),
        }
    }
}

#[php_class]
#[php(name = "Flow\\Parquet\\Engine\\Native\\NativeParquetColumnsReader", flags = ClassFlags::Final)]
pub struct NativeParquetColumnsReader {
    reader: Option<ParquetReader>,
    stream: Arc<PhpStream>,
    columns: Vec<(Vec<u8>, ReadCell)>,
    times: Option<Times>,
}

#[php_impl]
impl NativeParquetColumnsReader {
    /// `$columns`: root names or struct paths; `$batchSize` rows at most per `next()`.
    pub fn __construct(
        file: &NativeParquetFile,
        columns: &ZendHashTable,
        batch_size: i64,
        offset: Option<i64>,
        limit: Option<i64>,
    ) -> PhpResult<Self> {
        let batch_size = self::batch_size(batch_size)?;
        let (offset, limit) = (rows(offset, "offset")?.unwrap_or(0), rows(limit, "limit")?);
        let (stream, size, meta) = file.read()?;
        let stream = Arc::clone(stream);
        let refused = |error| parquet_exception(error, &stream, Side::Read, Surface::Lib);
        let mut names = Vec::with_capacity(columns.len());

        ht_for_each(columns, |_, _, name| {
            names.push(
                name.str()
                    .ok_or_else(|| exception(INVALID_ARGUMENT, "flow_php Parquet column names must be strings".to_string()))?
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
                    .map_err(|error| parquet_exception(error, &self.stream, Side::Read, Surface::Lib))?
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
#[php(name = "Flow\\Parquet\\Engine\\Native\\NativeParquetRowsWriter", flags = ClassFlags::Final)]
pub struct NativeParquetRowsWriter {
    writer: Option<ParquetWriter>,
    stream: Arc<PhpStream>,
    columns: Vec<WriteColumn>,
    classes: Classes,
    buffered: usize,
    batch_size: usize,
}

impl Drop for NativeParquetRowsWriter {
    fn drop(&mut self) {
        self.stream.detach();
    }
}

impl NativeParquetRowsWriter {
    fn refused(&self, error: Error) -> PhpException {
        parquet_exception(error, &self.stream, Side::Write, Surface::Lib)
    }

    /// The buffered rows through `ParquetWriter::write`.
    fn flush(&mut self) -> PhpResult<()> {
        if self.buffered == 0 {
            return Ok(());
        }

        let arrays = self
            .columns
            .iter_mut()
            .map(WriteColumn::take)
            .collect::<Result<Vec<_>, Error>>()
            .map_err(|error| self.refused(error))?;
        let rows = std::mem::take(&mut self.buffered);
        let writer = self.writer.as_mut().ok_or_else(|| exception(RUNTIME, "Writer is not open".to_string()))?;

        writer
            .write(rows, &arrays)
            .map_err(|error| parquet_exception(error, &self.stream, Side::Write, Surface::Lib))
    }

    fn append(&mut self, row: &Zval) -> PhpResult<()> {
        if self.writer.is_none() {
            return Err(exception(RUNTIME, "Writer is not open".to_string()));
        }

        let row = row
            .array()
            .ok_or_else(|| exception(INVALID_ARGUMENT, "flow_php Parquet writer rows must be arrays".to_string()))?;
        let null = Zval::new();

        for index in 0..self.columns.len() {
            let column = &mut self.columns[index];
            let value = ht_get(row, column.name.as_bytes()).unwrap_or(&null);

            if let Err(refusal) = column.cell.append(&mut column.builder, value, column.nullable, &self.classes) {
                let (name, row) = (column.name.clone(), self.buffered);

                // the row is refused whole: the columns before this one drop what it appended to them
                for column in &mut self.columns[..=index] {
                    column.builder.truncate(row);
                }

                return Err(match refusal {
                    Refusal::Value { expected, got } => self.refused(Error::Value {
                        column: name,
                        row,
                        expected,
                        got,
                    }),
                    Refusal::Overflow => self.refused(Error::Overflow { column: name, row }),
                    Refusal::Php(exception) => exception,
                });
            }
        }

        self.buffered += 1;

        if self.buffered >= self.batch_size {
            self.flush()?;
        }

        Ok(())
    }
}

#[php_impl]
impl NativeParquetRowsWriter {
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
        let refused = |error| parquet_exception(error, &stream, Side::Write, Surface::Lib);
        let schema = options::schema(schema).map_err(refused)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| WriteColumn::new(field))
            .collect::<Result<Vec<_>, Error>>()
            .map_err(refused)?;
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
            batch_size,
        })
    }

    /// A writer column the row lacks is null.
    #[php(name = "writeRow")]
    pub fn write_row(&mut self, row: &Zval) -> PhpResult<()> {
        self.append(row)
    }

    #[php(name = "writeRows")]
    pub fn write_rows(&mut self, rows: &ZendHashTable) -> PhpResult<()> {
        for row in rows.values() {
            self.append(row)?;
        }

        Ok(())
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
