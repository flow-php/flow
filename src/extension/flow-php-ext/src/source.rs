//! What the Rust open sources share: the chunk size they read streams in and the batches loop over a reader the
//! stream is fed into.

use std::cell::{RefCell, RefMut};

use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::backend::adopt_batch;
use crate::batch_columns::HeldBatch;
use crate::iterator::RustIterator;
use crate::render::runtime;

/// Each chunk is held as a PHP string, so it sets the read's peak memory: 32 KB matched the PHP path's own peak on wide
/// and narrow files. `AdaptiveJsonOpenSource::CHUNK` is its PHP copy.
pub const CHUNK: i64 = 1 << 15;

/// A reader a stream is fed into, one consuming call at a time.
pub trait Fed {
    /// The next full batch of what was fed (the remainder once finished), none while more input is needed.
    fn next_batch(&self) -> PhpResult<Option<HeldBatch>>;

    /// The next chunk of the stream fed in; false at its end.
    fn feed(&self) -> PhpResult<bool>;

    fn finish(&self) -> PhpResult<()>;
}

/// The batches of `fed`, each adopted by `backend` into `Rows` of `schema`.
pub fn batches(fed: impl Fed + 'static, schema: &Zval, backend: &Zval) -> RustIterator {
    let (schema, backend) = (schema.shallow_clone(), backend.shallow_clone());
    let mut finished = false;

    RustIterator::new(Box::new(move || loop {
        if let Some(batch) = fed.next_batch()? {
            return adopt_batch(&schema, &backend, batch).map(Some);
        }

        if finished {
            return Ok(None);
        }

        if !fed.feed()? {
            fed.finish()?;
            finished = true;
        }
    }))
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
