//! `Flow\ETL\Adapter\JSON\RustJsonOpenSource`: JSON lines or a top-level JSON array read from the stream in `CHUNK`s
//! into native columns, every batch adopted by the configured backend.

use std::cell::{Cell, RefCell};
use std::rc::Rc;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::batch_columns::{batch_size_of, HeldBatch};
use crate::ctx::{call_method, zval_long};
use crate::exception::ext_exception;
use crate::interfaces::json_open_source_ce;
use crate::iterator::RustIterator;
use crate::json::read::JsonReader;
use crate::source::{self, reading, Fed, CHUNK};

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
}

impl Fed for JsonFeed {
    fn next_batch(&self) -> PhpResult<Option<HeldBatch>> {
        reading(&self.shared.reader, CLASS)?.next_columns(&self.schema, self.batch_size)
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
    name = "Flow\\ETL\\Adapter\\JSON\\RustJsonOpenSource",
    flags = ClassFlags::Final,
    implements(ce = json_open_source_ce, stub = "Flow\\ETL\\Adapter\\JSON\\JsonOpenSource")
)]
pub struct RustJsonOpenSource {
    shared: Rc<Shared>,
}

#[php_impl]
impl RustJsonOpenSource {
    /// `$head`: the chunk the opener already read at `$offset` ('' for none); the bytes before it are JSON whitespace
    /// after an optional BOM, which the reader would skip. `$uri` names the file in refusals.
    pub fn __construct(stream: &Zval, lines: bool, uri: BinarySlice<u8>, head: BinarySlice<u8>, offset: i64) -> Self {
        Self {
            shared: Rc::new(Shared {
                stream: stream.shallow_clone(),
                reader: RefCell::new(JsonReader::new(lines, &uri)),
                head: RefCell::new(head.to_vec()),
                offset: Cell::new(offset),
            }),
        }
    }

    /// Exactly `$batchSize` rows keyed and ordered by `$schema` (the last may be shorter), every column adopted by
    /// `$backend`; refusals are `RowsBuilder::appendRows()`'s, row indexes relative to the batch.
    pub fn batches(&self, schema: &Zval, batchSize: i64, backend: &Zval) -> PhpResult<RustIterator> {
        let fed = JsonFeed {
            shared: Rc::clone(&self.shared),
            schema: schema.shallow_clone(),
            batch_size: batch_size_of(batchSize, "JSON")?,
        };

        Ok(source::batches(fed, schema, backend))
    }

    pub fn close(&self) -> PhpResult<()> {
        call_method(&self.shared.stream, "close", &mut [])?;

        Ok(())
    }
}
