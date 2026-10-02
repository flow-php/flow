//! `Flow\ETL\RustIterator`: the `\Iterator` the Rust open sources return from `batches()` / `records()` - keys 0..n,
//! each value pulled from a Rust producer on demand. It behaves as the Generator it replaced: it streams once
//! (`rewind()` after `next()` throws) and ends for good when the producer throws.

use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use ext_php_rs::zend::ce;

use crate::render::runtime;

pub type Producer = Box<dyn FnMut() -> PhpResult<Option<Zval>>>;

#[php_class]
#[php(name = "Flow\\ETL\\RustIterator", flags = ClassFlags::Final, implements(ce = ce::iterator, stub = "\\Iterator"))]
pub struct RustIterator {
    producer: Producer,
    current: Option<Zval>,
    /// The index of `current`.
    key: i64,
    started: bool,
    /// `next()` moved past the first value: `rewind()` throws from here on.
    advanced: bool,
    /// The producer returned its last value or threw: it is never called again.
    done: bool,
}

impl RustIterator {
    pub fn new(producer: Producer) -> Self {
        Self {
            producer,
            current: None,
            key: 0,
            started: false,
            advanced: false,
            done: false,
        }
    }

    /// A Generator starts on its first `rewind()`, `valid()`, `current()`, `key()` or `next()`.
    fn start(&mut self) -> PhpResult<()> {
        if self.started {
            return Ok(());
        }

        self.started = true;
        self.pull()
    }

    fn pull(&mut self) -> PhpResult<()> {
        self.current = None;

        if self.done {
            return Ok(());
        }

        match (self.producer)() {
            Ok(Some(value)) => {
                self.current = Some(value);

                Ok(())
            }
            Ok(None) => {
                self.done = true;

                Ok(())
            }
            Err(error) => {
                self.done = true;

                Err(error)
            }
        }
    }
}

#[php_impl]
impl RustIterator {
    pub fn rewind(&mut self) -> PhpResult<()> {
        if self.advanced {
            return Err(runtime("RustIterator cannot rewind".to_string()));
        }

        self.start()
    }

    pub fn valid(&mut self) -> PhpResult<bool> {
        self.start()?;

        Ok(self.current.is_some())
    }

    pub fn current(&mut self) -> PhpResult<Zval> {
        self.start()?;

        Ok(self.current.as_ref().map_or_else(Zval::new, Zval::shallow_clone))
    }

    /// Null once the iterator ended, as a Generator's.
    pub fn key(&mut self) -> PhpResult<Option<i64>> {
        self.start()?;

        Ok(self.current.is_some().then_some(self.key))
    }

    pub fn next(&mut self) -> PhpResult<()> {
        self.start()?;

        if self.done {
            return Ok(());
        }

        self.advanced = true;
        self.key += 1;
        self.pull()
    }
}
