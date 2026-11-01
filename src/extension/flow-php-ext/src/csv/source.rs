//! `Flow\ETL\Adapter\CSV\IO\Extractor\RustCSVOpenSource`: one CSV file read through the native tokenizer - a local file (or a
//! window of one) read by Rust in `NATIVE_CHUNK` preads, any other stream fed in `$charactersReadInLine` chunks through
//! its PHP `iterate()` - batches as native columns adopted by the configured backend, records as PHP arrays, the type
//! sniff folded natively.

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::FileExt;
use std::rc::Rc;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::batch_columns::batch_size_of;
use crate::csv::columns::{next_columns, CsvColumns};
use crate::csv::fold::{string_values, Fold};
use crate::csv::CsvReader;
use crate::ctx::{call_function, call_method, call_static, ht_insert, null_zval, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::interfaces::csv_open_source_ce;
use crate::iterator::RustIterator;
use crate::render::{invalid_argument, runtime};
use crate::source::{self, reading, window_start, Fed, Fetched, CHUNK, RANGE_SOURCE_STREAM};

const CLASS: &str = "RustCSVOpenSource";
const RECORDS_BATCH: usize = 1000;
const NATIVE_LOCAL_SOURCE_STREAM: &str = "Flow\\Filesystem\\Stream\\NativeLocalSourceStream";
/// One pread of a local file, about 512 per GiB; `CHUNK` stays the PHP arm's.
const NATIVE_CHUNK: usize = 2 << 20;
const STRING_TYPE_NARROWER: &str = "Flow\\Types\\Type\\Native\\String\\StringTypeNarrower";
const COLUMN_TYPES: &str = "Flow\\ETL\\IO\\Inference\\ColumnTypes";
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

/// `CSVOpenSource::sniff()`'s return, typed `Flow\ETL\IO\Inference\ColumnTypes`.
pub struct ColumnTypesValue(Zval);

impl IntoZval for ColumnTypesValue {
    const TYPE: DataType = DataType::object("Flow\\ETL\\IO\\Inference\\ColumnTypes");
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

/// Where the bytes come from.
enum Feed {
    /// A `NativeLocalSourceStream`, or a `RangeSourceStream` whose `stream()` is one: the file reopened by its path and
    /// read `[at, end)` by `pread`, the position shared by every consumer as the PHP stream's would be.
    Native {
        file: File,
        uri: String,
        at: Cell<u64>,
        end: u64,
    },
    /// Everything else (remote, traced, block-cached, a window over any of those): `$stream->iterate($chunkSize)`.
    Php { chunk_size: i64 },
}

/// `Feed::Native` when `stream` is a local file or a window of one, else `Feed::Php` reading `chunk_size` at a time.
fn feed_of(stream: &Zval, chunk_size: i64) -> PhpResult<Feed> {
    let instance_of = |zv: &Zval, class: &str| {
        matches!(
            (zv.object(), ext_php_rs::zend::ClassEntry::try_find(class)),
            (Some(object), Some(ce)) if object.instance_of(ce)
        )
    };
    let (local, window) = if instance_of(stream, NATIVE_LOCAL_SOURCE_STREAM) {
        (stream.shallow_clone(), None)
    } else if instance_of(stream, RANGE_SOURCE_STREAM)
        && instance_of(&call_method(stream, "stream", &mut [])?, NATIVE_LOCAL_SOURCE_STREAM)
    {
        let bound = |method: &str| -> PhpResult<u64> {
            let bound = call_method(stream, method, &mut [])?;

            u64::try_from(bound.long().unwrap_or(-1)).map_err(|_| {
                ext_exception(format!(
                    "flow_php expected RangeSourceStream::{method}() to be an int >= 0"
                ))
            })
        };

        (
            call_method(stream, "stream", &mut [])?,
            Some((bound("start")?, bound("end")?)),
        )
    } else {
        return Ok(Feed::Php { chunk_size });
    };

    let path = call_method(&local, "path", &mut [])?;
    let uri = call_method(&path, "uri", &mut [])?;
    let uri = String::from_utf8_lossy(uri.zend_str().map_or(&b""[..], |uri| uri.as_bytes())).into_owned();
    let path = call_method(&path, "path", &mut [])?;
    let path = path
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected Path::path() to return a string"))?;
    let file = File::open(OsStr::from_bytes(path.as_bytes()))
        .map_err(|error| runtime(format!("Cannot open file: {uri} ({error})")))?;
    let size = file
        .metadata()
        .map_err(|error| runtime(format!("Cannot get file size: {uri} ({error})")))?
        .len();
    let (at, end) = window.unwrap_or((0, size));

    Ok(Feed::Native {
        file,
        uri,
        at: Cell::new(at),
        end,
    })
}

struct Shared {
    stream: Zval,
    feed: Feed,
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
        self.produced_rows.set(reading.reader.consumed_rows());

        result
    }

    /// `$stream->iterate($chunkSize)`; `None` on the native arm, which reads the file itself.
    fn chunks(&self) -> PhpResult<Option<Zval>> {
        match &self.feed {
            Feed::Native { .. } => Ok(None),
            Feed::Php { chunk_size } => call_method(&self.stream, "iterate", &mut [zval_long(*chunk_size)]).map(Some),
        }
    }

    /// The next chunk fed to the reader; the generator runs with no borrow held, since the stream may call back.
    fn feed(&self, chunks: &Option<Zval>) -> PhpResult<bool> {
        match &self.feed {
            Feed::Native { file, uri, at, end } => {
                let want = NATIVE_CHUNK.min(end.saturating_sub(at.get()) as usize);

                if want == 0 {
                    return Ok(false);
                }

                let got = self.with(|reading| {
                    reading
                        .reader
                        .fill_with(want, |buffer| file.read_at(buffer, at.get()))
                        .map_err(|error| runtime(format!("Cannot read file: {uri} ({error})")))
                })?;

                if got == 0 {
                    return Ok(false);
                }

                at.set(at.get() + got as u64);

                Ok(true)
            }
            Feed::Php { .. } => {
                let chunks = chunks.as_ref().expect("the PHP arm feeds from its generator");

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
        }
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
    chunks: Option<Zval>,
    schema: Zval,
    batch_size: usize,
    uri: Zval,
    inference: Option<Zval>,
}

impl Fed for CsvFeed {
    fn next_batch(&self) -> PhpResult<Option<Fetched>> {
        self.shared.with(|reading| {
            next_columns(
                &mut reading.reader,
                &mut reading.columns,
                &self.schema,
                self.batch_size,
                &self.uri,
                self.inference.as_ref(),
            )
        })
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
    name = "Flow\\ETL\\Adapter\\CSV\\IO\\Extractor\\RustCSVOpenSource",
    flags = ClassFlags::Final,
    implements(ce = csv_open_source_ce, stub = "Flow\\ETL\\Adapter\\CSV\\IO\\Extractor\\CSVOpenSource")
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
        headers: Option<&ZendHashTable>,
        missingColumnsAsNull: Option<bool>,
    ) -> PhpResult<Self> {
        // a local file is read by Rust and ignores the read length, as NativeLocalSourceStream::readLines() does on
        // the PHP path; for S3/Azure it is the range-request size - and from_csv() defaults it to 10 MB
        Ok(Self {
            shared: Rc::new(Shared {
                stream: stream.shallow_clone(),
                feed: feed_of(stream, charactersReadInLine.unwrap_or(CHUNK))?,
                reading: RefCell::new(Reading {
                    reader: CsvReader::new(
                        &separator,
                        &enclosure,
                        &escape,
                        withHeader,
                        emptyToNull,
                        removeBOM,
                        headers.map(string_values),
                        missingColumnsAsNull.unwrap_or(false),
                        window_start(stream)?,
                    )?,
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

    /// `PhpCSVOpenSource::batches()`: `$batchSize` records keyed and ordered by `$schema` at a time, every column
    /// adopted by `$backend`, corrupted records handed out as they are read.
    pub fn batches(
        &self,
        schema: &Zval,
        batchSize: i64,
        backend: &Zval,
        inference: Option<&Zval>,
    ) -> PhpResult<RustIterator> {
        let fed = CsvFeed {
            shared: Rc::clone(&self.shared),
            chunks: self.shared.chunks()?,
            schema: schema.shallow_clone(),
            batch_size: batch_size_of(batchSize, "CSV")?,
            uri: call_method(&call_method(&self.shared.stream, "path", &mut [])?, "uri", &mut [])?,
            inference: source::inference_of(inference)?,
        };

        source::batches(fed, schema, backend)
    }

    pub fn headers(&self) -> PhpResult<ArrayValue> {
        self.shared.with(|reading| headers(&mut reading.reader)).map(ArrayValue)
    }

    pub fn records(&self) -> PhpResult<RustIterator> {
        self.records_iterator()
    }

    /// `ColumnTypes::sniff()` over `records()`; a `StringTypeNarrower` without its HTML and XML rungs folds natively.
    pub fn sniff(&self, names: &ZendHashTable, rowBudget: Option<i64>, typer: &Zval) -> PhpResult<ColumnTypesValue> {
        if rowBudget.is_some_and(|budget| budget < 0) {
            return Err(invalid_argument(
                "Row budget must be 0 or more. Use null for every row.".to_string(),
            ));
        }

        if !folds_natively(typer)? {
            let mut records = Zval::new();
            self.records_iterator()?.set_zval(&mut records, false)?;
            let mut names_zv = Zval::new();
            names_zv.set_hashtable(names.to_owned());

            return call_static(
                COLUMN_TYPES,
                "sniff",
                &mut [
                    names_zv,
                    records,
                    rowBudget.map_or_else(null_zval, zval_long),
                    typer.shallow_clone(),
                ],
            )
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
        let remaining = |fold: &Fold| rowBudget.map(|budget| (budget as u64).saturating_sub(fold.rows()));
        let chunks = self.shared.chunks()?;

        while self.shared.feed(&chunks)? {
            let limit = remaining(&fold);
            self.shared.with(|reading| reading.reader.fold(&mut fold, limit))?;

            if rowBudget.is_some_and(|budget| fold.rows() >= budget as u64) {
                break;
            }
        }

        if rowBudget.is_none_or(|budget| fold.rows() < budget as u64) {
            self.shared.finish()?;
            let limit = remaining(&fold);
            self.shared.with(|reading| reading.reader.fold(&mut fold, limit))?;
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
            "Flow\\ETL\\IO\\Inference\\ColumnTypes",
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
