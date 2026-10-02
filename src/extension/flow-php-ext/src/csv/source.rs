//! `Flow\ETL\Adapter\CSV\RustCSVOpenSource`: one CSV file read through the native tokenizer - the stream fed in chunks
//! (a `NativeLocalSourceStream` in `CHUNK`s, any other stream in `$charactersReadInLine`), batches as native columns
//! adopted by the configured backend, records as PHP arrays, the type sniff folded natively.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::batch_columns::{batch_size_of, HeldBatch};
use crate::csv::columns::{next_columns, CsvColumns};
use crate::csv::fold::{string_values, Fold};
use crate::csv::CsvReader;
use crate::ctx::{call_function, call_method, call_static, construct, find_class, ht_insert, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::interfaces::csv_open_source_ce;
use crate::iterator::RustIterator;
use crate::source::{self, reading, Fed, CHUNK};

const CLASS: &str = "RustCSVOpenSource";
const RECORDS_BATCH: usize = 1000;
const NATIVE_LOCAL_SOURCE_STREAM: &str = "Flow\\Filesystem\\Stream\\NativeLocalSourceStream";
const STRING_TYPE_NARROWER: &str = "Flow\\Types\\Type\\Native\\String\\StringTypeNarrower";
const SCHEMA_INFERRER: &str = "Flow\\ETL\\Schema\\Inference\\SchemaInferrer";
/// The candidates the native fold narrows to, in `StringTypeNarrower`'s order; HTML and XML are never folded natively.
const FOLDED: [&str; 8] = [
    "Flow\\Types\\DSL\\type_json",
    "Flow\\Types\\DSL\\type_uuid",
    "Flow\\Types\\DSL\\type_float",
    "Flow\\Types\\DSL\\type_integer",
    "Flow\\Types\\DSL\\type_datetime",
    "Flow\\Types\\DSL\\type_date",
    "Flow\\Types\\DSL\\type_boolean",
    "Flow\\Types\\DSL\\type_time_zone",
];

/// A contract method's `array` return.
pub struct ArrayValue(Zval);

impl IntoZval for ArrayValue {
    const TYPE: DataType = DataType::Array;
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

/// `CSVOpenSource::sniff()`'s return, typed `Flow\ETL\Schema\Inference\ColumnTypes`.
pub struct ColumnTypesValue(Zval);

impl IntoZval for ColumnTypesValue {
    const TYPE: DataType = DataType::object("Flow\\ETL\\Schema\\Inference\\ColumnTypes");
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

struct Reading {
    reader: CsvReader,
    columns: CsvColumns,
}

struct Shared {
    stream: Zval,
    chunk_size: i64,
    reading: RefCell<Reading>,
    produced_rows: Cell<u64>,
    produced_bytes: Cell<u64>,
}

impl Shared {
    /// `f` over the reader, no PHP stream call inside; `producedBytes()` follows what it consumed.
    fn with<R>(&self, f: impl FnOnce(&mut Reading) -> PhpResult<R>) -> PhpResult<R> {
        let mut reading = reading(&self.reading, CLASS)?;
        let result = f(&mut reading);
        self.produced_bytes.set(reading.reader.consumed_bytes());

        result
    }

    fn produced(&self, rows: u64) {
        self.produced_rows.set(self.produced_rows.get() + rows);
    }

    /// `$stream->iterate($chunkSize)`, the generator one consuming method feeds from.
    fn chunks(&self) -> PhpResult<Zval> {
        call_method(&self.stream, "iterate", &mut [zval_long(self.chunk_size)])
    }

    /// The next chunk of `chunks` fed to the reader; false once the generator is exhausted. The generator runs with no
    /// borrow held: the stream may call back into the source.
    fn feed(&self, chunks: &Zval) -> PhpResult<bool> {
        if !call_method(chunks, "valid", &mut [])?.bool().unwrap_or(false) {
            return Ok(false);
        }

        let chunk = call_method(chunks, "current", &mut [])?;
        let bytes = chunk
            .zend_str()
            .ok_or_else(|| ext_exception("flow_php expected SourceStream::iterate() to yield strings"))?
            .as_bytes();
        self.with(|reading| {
            reading.reader.feed(bytes);

            Ok(())
        })?;
        call_method(chunks, "next", &mut [])?;

        Ok(true)
    }

    fn finish(&self) -> PhpResult<()> {
        self.with(|reading| {
            reading.reader.finish();

            Ok(())
        })
    }
}

/// One `batches()` call over the shared reader.
struct CsvFeed {
    shared: Rc<Shared>,
    chunks: Zval,
    schema: Zval,
    batch_size: usize,
}

impl Fed for CsvFeed {
    fn next_batch(&self) -> PhpResult<Option<HeldBatch>> {
        let batch = self
            .shared
            .with(|reading| next_columns(&mut reading.reader, &mut reading.columns, &self.schema, self.batch_size))?;

        if let Some(batch) = &batch {
            self.shared.produced(batch.count as u64);
        }

        Ok(batch)
    }

    fn feed(&self) -> PhpResult<bool> {
        self.shared.feed(&self.chunks)
    }

    fn finish(&self) -> PhpResult<()> {
        self.shared.finish()
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\CSV\\RustCSVOpenSource",
    flags = ClassFlags::Final,
    implements(ce = csv_open_source_ce, stub = "Flow\\ETL\\Adapter\\CSV\\CSVOpenSource")
)]
pub struct RustCSVOpenSource {
    shared: Rc<Shared>,
}

impl RustCSVOpenSource {
    fn records_iterator(&self) -> PhpResult<RustIterator> {
        let shared = Rc::clone(&self.shared);
        let chunks = shared.chunks()?;
        let mut pending: VecDeque<Zval> = VecDeque::new();
        let mut finished = false;

        Ok(RustIterator::new(Box::new(move || loop {
            if let Some(record) = pending.pop_front() {
                return Ok(Some(record));
            }

            let batch = shared.with(|reading| reading.reader.next(RECORDS_BATCH))?;

            if !batch.is_empty() {
                shared.produced(batch.len() as u64);
                pending.extend(batch.values().map(Zval::shallow_clone));

                continue;
            }

            if finished {
                return Ok(None);
            }

            if !shared.feed(&chunks)? {
                shared.finish()?;
                finished = true;
            }
        })))
    }
}

#[php_impl]
impl RustCSVOpenSource {
    #[allow(clippy::too_many_arguments)]
    pub fn __construct(
        stream: &Zval,
        separator: BinarySlice<u8>,
        enclosure: BinarySlice<u8>,
        escape: BinarySlice<u8>,
        withHeader: bool,
        emptyToNull: bool,
        removeBOM: bool,
        charactersReadInLine: Option<i64>,
    ) -> PhpResult<Self> {
        // a local file ignores the read length, as NativeLocalSourceStream::readLines() does on the PHP path; for
        // S3/Azure it is the range-request size - and from_csv() defaults it to 10 MB, which would be the chunk
        let local = match (
            stream.object(),
            ext_php_rs::zend::ClassEntry::try_find(NATIVE_LOCAL_SOURCE_STREAM),
        ) {
            (Some(object), Some(ce)) => object.instance_of(ce),
            _ => false,
        };

        Ok(Self {
            shared: Rc::new(Shared {
                stream: stream.shallow_clone(),
                chunk_size: if local {
                    CHUNK
                } else {
                    charactersReadInLine.unwrap_or(CHUNK)
                },
                reading: RefCell::new(Reading {
                    reader: CsvReader::new(&separator, &enclosure, &escape, withHeader, emptyToNull, removeBOM)?,
                    columns: CsvColumns::default(),
                }),
                produced_rows: Cell::new(0),
                produced_bytes: Cell::new(0),
            }),
        })
    }

    pub fn close(&self) -> PhpResult<()> {
        call_method(&self.shared.stream, "close", &mut [])?;

        Ok(())
    }

    #[php(name = "producedBytes")]
    pub fn produced_bytes(&self) -> i64 {
        self.shared.produced_bytes.get() as i64
    }

    #[php(name = "producedRows")]
    pub fn produced_rows(&self) -> i64 {
        self.shared.produced_rows.get() as i64
    }

    pub fn columns(&self) -> PhpResult<ArrayValue> {
        let chunks = self.shared.chunks()?;

        while self.shared.feed(&chunks)? {
            if self.shared.with(|reading| Ok(!reading.reader.headers().is_empty()))? {
                return self.headers();
            }
        }

        self.shared.finish()?;

        self.headers()
    }

    /// Exactly `$batchSize` rows keyed and ordered by `$schema` (the last may be shorter), every column adopted by
    /// `$backend`; refusals are `RowsBuilder::appendRows()`'s, row indexes relative to the batch.
    pub fn batches(&self, schema: &Zval, batchSize: i64, backend: &Zval) -> PhpResult<RustIterator> {
        let fed = CsvFeed {
            shared: Rc::clone(&self.shared),
            chunks: self.shared.chunks()?,
            schema: schema.shallow_clone(),
            batch_size: batch_size_of(batchSize, "CSV")?,
        };

        Ok(source::batches(fed, schema, backend))
    }

    pub fn headers(&self) -> PhpResult<ArrayValue> {
        self.shared.with(|reading| headers(&mut reading.reader)).map(ArrayValue)
    }

    pub fn records(&self) -> PhpResult<RustIterator> {
        self.records_iterator()
    }

    /// `SchemaInferrer::sniff()` over `records()`; a `StringTypeNarrower` without its HTML and XML rungs folds natively.
    pub fn sniff(
        &self,
        names: &ZendHashTable,
        rowBudget: i64,
        inference: &Zval,
        typer: &Zval,
    ) -> PhpResult<ColumnTypesValue> {
        if rowBudget < -1 {
            return Err(ext_exception("flow_php CSV fold limit must be -1 or at least 0"));
        }

        if !folds_natively(typer)? {
            let mut records = Zval::new();
            self.records_iterator()?.set_zval(&mut records, false)?;
            let mut names_zv = Zval::new();
            names_zv.set_hashtable(names.to_owned());
            let inferrer = construct(
                find_class(SCHEMA_INFERRER)?,
                &mut [inference.shallow_clone(), typer.shallow_clone()],
            )?
            .into_zval(false)?;

            return call_method(&inferrer, "sniff", &mut [names_zv, records, zval_long(rowBudget)])
                .map(ColumnTypesValue);
        }

        let mut candidates = Vec::new();

        for function in FOLDED {
            let candidate = call_function(function, &mut [])?;

            if call_method(typer, "emitsType", &mut [candidate.shallow_clone()])?
                .bool()
                .unwrap_or(false)
            {
                candidates.push(
                    call_method(&candidate, "toString", &mut [])?
                        .zend_str()
                        .ok_or_else(|| ext_exception("flow_php expected Type::toString() to return a string"))?
                        .as_bytes()
                        .to_vec(),
                );
            }
        }

        let mut fold = Fold::new(string_values(names), &candidates)?;
        let remaining = |fold: &Fold| match rowBudget {
            -1 => None,
            budget => Some((budget as u64).saturating_sub(fold.rows())),
        };
        let chunks = self.shared.chunks()?;

        while self.shared.feed(&chunks)? {
            let limit = remaining(&fold);
            let folded = self.shared.with(|reading| reading.reader.fold(&mut fold, limit))?;
            self.shared.produced(folded);

            if rowBudget != -1 && fold.rows() >= rowBudget as u64 {
                break;
            }
        }

        if rowBudget == -1 || fold.rows() < rowBudget as u64 {
            self.shared.finish()?;
            let limit = remaining(&fold);
            let folded = self.shared.with(|reading| reading.reader.fold(&mut fold, limit))?;
            self.shared.produced(folded);
        }

        let mut types = ZendHashTable::new();

        for (name, code) in fold.types() {
            ht_insert(
                &mut types,
                name,
                call_static(
                    "Flow\\Types\\Type\\TypeFactory",
                    "fromString",
                    &mut [zval_str(code.as_bytes())],
                )?,
            );
        }

        let mut types_zv = Zval::new();
        types_zv.set_hashtable(types);

        call_static(
            "Flow\\ETL\\Schema\\Inference\\ColumnTypes",
            "fromColumnTypes",
            &mut [types_zv, zval_long(fold.rows() as i64), typer.shallow_clone()],
        )
        .map(ColumnTypesValue)
    }
}

/// The Rust fold is `StringTypeNarrower` without its HTML and XML rungs - a DOMDocument per cell is not worth porting.
fn folds_natively(typer: &Zval) -> PhpResult<bool> {
    let narrower = match (
        typer.object(),
        ext_php_rs::zend::ClassEntry::try_find(STRING_TYPE_NARROWER),
    ) {
        (Some(object), Some(ce)) => object.instance_of(ce),
        _ => false,
    };

    if !narrower {
        return Ok(false);
    }

    for markup in ["Flow\\Types\\DSL\\type_html", "Flow\\Types\\DSL\\type_xml"] {
        if call_method(typer, "emitsType", &mut [call_function(markup, &mut [])?])?
            .bool()
            .unwrap_or(false)
        {
            return Ok(false);
        }
    }

    Ok(true)
}

fn headers(reader: &mut CsvReader) -> PhpResult<Zval> {
    let headers = reader.headers();
    let mut list = ZendHashTable::with_capacity(headers.len() as u32);

    for header in headers {
        list.push(zval_str(header))
            .map_err(|e| ext_exception(format!("flow_php failed to collect a CSV header: {e:?}")))?;
    }

    let mut zv = Zval::new();
    zv.set_hashtable(list);

    Ok(zv)
}
