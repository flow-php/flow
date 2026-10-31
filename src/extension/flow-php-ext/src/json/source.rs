//! `Flow\ETL\Adapter\JSON\IO\Extractor\RustJsonOpenSource`: JSON lines or a top-level JSON array read from the stream in `CHUNK`s
//! into native columns, every batch adopted by the configured backend.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::batch_columns::batch_size_of;
use crate::ctx::{call_method, ht_for_each, zval_long};
use crate::exception::ext_exception;
use crate::interfaces::json_open_source_ce;
use crate::iterator::RustIterator;
use crate::json::read::JsonReader;
use crate::source::{self, reading, window_start, Fed, Fetched, CHUNK};

const CLASS: &str = "RustJsonOpenSource";

struct Shared {
    stream: Zval,
    reader: RefCell<JsonReader>,
    /// The chunk the opener already read at `offset`; empty once fed.
    head: RefCell<Vec<u8>>,
    offset: Cell<i64>,
}

/// One `batches()` call over the shared reader.
struct JsonFeed {
    shared: Rc<Shared>,
    schema: Zval,
    batch_size: usize,
    sample_size: Option<i64>,
    /// The names a record may hold under an inferred schema.
    known: Vec<Vec<u8>>,
}

impl Fed for JsonFeed {
    fn next_batch(&self) -> PhpResult<Option<Fetched>> {
        reading(&self.shared.reader, CLASS)?.next_columns(&self.schema, self.batch_size, self.sample_size, &self.known)
    }

    /// `SourceStream::read()` runs with no borrow held: the stream may call back into the source.
    fn feed(&self) -> PhpResult<bool> {
        let head = std::mem::take(&mut *self.shared.head.borrow_mut());
        let chunk = if head.is_empty() {
            let read = call_method(
                &self.shared.stream,
                "read",
                &mut [zval_long(CHUNK), zval_long(self.shared.offset.get())],
            )?;

            read.zend_str()
                .ok_or_else(|| ext_exception("flow_php expected SourceStream::read() to return a string"))?
                .as_bytes()
                .to_vec()
        } else {
            head
        };

        if chunk.is_empty() {
            return Ok(false);
        }

        reading(&self.shared.reader, CLASS)?.feed(&chunk);
        self.shared.offset.set(self.shared.offset.get() + chunk.len() as i64);

        Ok(true)
    }

    fn finish(&self) -> PhpResult<()> {
        reading(&self.shared.reader, CLASS)?.finish();

        Ok(())
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\JSON\\IO\\Extractor\\RustJsonOpenSource",
    flags = ClassFlags::Final,
    implements(ce = json_open_source_ce, stub = "Flow\\ETL\\Adapter\\JSON\\IO\\Extractor\\JsonOpenSource")
)]
pub struct RustJsonOpenSource {
    shared: Rc<Shared>,
}

#[php_impl]
impl RustJsonOpenSource {
    /// `$head`: the chunk the opener already read at `$offset` ('' for none); the bytes before it are JSON whitespace
    /// after an optional BOM, which the reader would skip. `$uri` names the file in refusals.
    pub fn __construct(
        stream: &Zval,
        lines: bool,
        uri: BinarySlice<u8>,
        head: BinarySlice<u8>,
        offset: i64,
    ) -> PhpResult<Self> {
        Ok(Self {
            shared: Rc::new(Shared {
                stream: stream.shallow_clone(),
                reader: RefCell::new(JsonReader::new(lines, &uri, window_start(stream)?)),
                head: RefCell::new(head.to_vec()),
                offset: Cell::new(offset),
            }),
        })
    }

    /// `PhpJsonOpenSource::batches()`: `$batchSize` records keyed and ordered by `$schema` at a time, every column
    /// adopted by `$backend`, a batch's corrupted records handed out before its rows.
    pub fn batches(
        &self,
        schema: &Zval,
        batchSize: i64,
        backend: &Zval,
        sampleSize: Option<i64>,
        known: Option<&Zval>,
    ) -> PhpResult<RustIterator> {
        let mut names = Vec::new();

        if sampleSize.is_some() {
            let definitions = call_method(known.unwrap_or(schema), "definitions", &mut [])?;
            let definitions = definitions
                .array()
                .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;

            ht_for_each(definitions, |name, index, _| {
                names.push(name.map_or_else(|| index.to_string().into_bytes(), |name| name.as_bytes().to_vec()));

                Ok(())
            })?;
        }

        let fed = JsonFeed {
            shared: Rc::clone(&self.shared),
            schema: schema.shallow_clone(),
            batch_size: batch_size_of(batchSize, "JSON")?,
            sample_size: sampleSize,
            known: names,
        };

        source::batches(fed, schema, backend)
    }

    pub fn close(&self) -> PhpResult<()> {
        call_method(&self.shared.stream, "close", &mut [])?;

        Ok(())
    }
}
