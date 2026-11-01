//! What the Rust open sources share: the chunk size they read streams in and the batches loop over a reader the
//! stream is fed into.

use std::cell::{RefCell, RefMut};

use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::backend::{adopt_batch, Adoption, Definitions};
use crate::batch_columns::HeldBatch;
use crate::ctx::call_method;
use crate::exception::ext_exception;
use crate::iterator::RustIterator;
use crate::render::runtime;

/// Chunks are PHP strings, so their size sets peak memory; 32 KB matches the PHP path (`JsonFileBatches::CHUNK`).
pub const CHUNK: i64 = 1 << 15;

/// What a reader hands out next.
pub enum Fetched {
    Batch(HeldBatch),
    /// A `CorruptedRecordException`.
    Corrupted(Zval),
}

pub const RANGE_SOURCE_STREAM: &str = "Flow\\Filesystem\\Stream\\RangeSourceStream";

/// The byte of its file `stream` begins at: a `RangeSourceStream`'s start, else 0.
pub fn window_start(stream: &Zval) -> PhpResult<u64> {
    let is_window = matches!(
        (stream.object(), ext_php_rs::zend::ClassEntry::try_find(RANGE_SOURCE_STREAM)),
        (Some(object), Some(ce)) if object.instance_of(ce)
    );

    if !is_window {
        return Ok(0);
    }

    u64::try_from(call_method(stream, "start", &mut [])?.long().unwrap_or(-1))
        .map_err(|_| ext_exception("flow_php expected RangeSourceStream::start() to be an int >= 0"))
}

/// A reader a stream is fed into, one consuming call at a time.
pub trait Fed {
    /// The next full batch of what was fed (the remainder once finished) or corrupted record, none while more input
    /// is needed.
    fn next_batch(&self) -> PhpResult<Option<Fetched>>;

    /// The next chunk of the stream fed in; false at its end.
    fn feed(&self) -> PhpResult<bool>;

    fn finish(&self) -> PhpResult<()>;
}

/// The batches of `fed`, each adopted by `backend` into `Rows` of `schema`, and its corrupted records as they are.
/// The `?SchemaInference $inference` argument of `batches()`: PHP's `null` arrives as a null zval, not `None`.
pub fn inference_of(inference: Option<&Zval>) -> PhpResult<Option<Zval>> {
    match inference {
        Some(zv) if zv.is_null() => Ok(None),
        Some(zv) if zv.object().is_some() => Ok(Some(zv.shallow_clone())),
        Some(_) => Err(ext_exception(
            "flow_php expected the inference to be a SchemaInference or null",
        )),
        None => Ok(None),
    }
}

pub fn batches(fed: impl Fed + 'static, schema: &Zval, backend: &Zval) -> PhpResult<RustIterator> {
    let adoption = Adoption::new(Definitions::of(schema)?, schema, backend);
    let mut finished = false;

    Ok(RustIterator::new(Box::new(move || loop {
        match fed.next_batch()? {
            Some(Fetched::Batch(batch)) => return adopt_batch(&adoption, batch).map(Some),
            Some(Fetched::Corrupted(record)) => return Ok(Some(record)),
            None => {}
        }

        if finished {
            return Ok(None);
        }

        if !fed.feed()? {
            fed.finish()?;
            finished = true;
        }
    })))
}

/// The reader, borrowed only while Rust code runs on it: a stream, a backend or an inferrer that calls back into the
/// source mid-read is refused, never a panic.
pub fn reading<'a, T>(reader: &'a RefCell<T>, class: &str) -> PhpResult<RefMut<'a, T>> {
    reader.try_borrow_mut().map_err(|_| {
        runtime(format!(
            "{class} is already reading: a read cannot start from inside another"
        ))
    })
}
