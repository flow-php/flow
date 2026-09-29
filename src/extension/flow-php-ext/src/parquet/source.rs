//! Parquet bytes from a `Flow\Filesystem\SourceStream` (`read(int $length, int $offset): string`, `size(): ?int`), and
//! the PHP stream object both the source and the sink call into.

use std::cell::{Cell, RefCell};
use std::io::{self, Read};
use std::sync::Arc;

use bytes::{Buf, Bytes};
use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendObject, Zval};
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};

use crate::ctx::{call_handle_catching, ce_method_ref, expect_object, zval_long};
use crate::parquet::error::Error;

/// What `get_read()` pulls from the stream per `read()` call.
pub const WINDOW: usize = 64 * 1024;

/// A PHP stream object. An exception its method throws is kept here, for the surface to rethrow as itself once the
/// Parquet call it broke returns.
pub struct PhpStream {
    stream: Zval,
    thrown: RefCell<Option<ZBox<ZendObject>>>,
    detached: Cell<bool>,
}

// SAFETY: the stream belongs to the request thread that built it; the parquet crate requires Send + Sync of its
// readers and writers but calls them only from that thread, synchronously.
unsafe impl Send for PhpStream {}
unsafe impl Sync for PhpStream {}

impl PhpStream {
    pub fn new(stream: &Zval) -> Result<Self, PhpException> {
        expect_object(stream, "a stream")?;

        Ok(Self {
            stream: stream.shallow_clone(),
            thrown: RefCell::new(None),
            detached: Cell::new(false),
        })
    }

    /// `$stream->$method(...$args)`; `None` when it threw, or once the stream is detached.
    pub fn call(&self, method: &str, args: &mut [Zval]) -> Option<Zval> {
        if self.detached.get() {
            return None;
        }

        let object = self.stream.object()?;
        let function = ce_method_ref(unsafe { object.ce.as_ref() }?, method).ok()?;

        match call_handle_catching(function, Some(object), args) {
            Ok(value) => Some(value),
            Err(exception) => {
                self.thrown.replace(Some(exception));

                None
            }
        }
    }

    /// No PHP call from here on: what an abandoned Parquet writer flushes while it is dropped - possibly while a PHP
    /// exception unwinds, which a call would take - never reaches the stream.
    pub fn detach(&self) {
        self.detached.set(true);
    }

    /// The exception a call threw, taken.
    pub fn thrown(&self) -> Option<ZBox<ZendObject>> {
        self.thrown.take()
    }
}

/// Bytes at an offset of a source: the one call `WindowedRead` and `get_bytes()` make.
pub trait ReadAt {
    fn read_at(&self, offset: u64, length: usize) -> Result<Bytes, ParquetError>;
}

impl ReadAt for PhpStream {
    fn read_at(&self, offset: u64, length: usize) -> Result<Bytes, ParquetError> {
        let read = self
            .call("read", &mut [zval_long(length as i64), zval_long(offset as i64)])
            .ok_or_else(|| ParquetError::External("read() threw".into()))?;

        Ok(Bytes::copy_from_slice(
            read.zend_str()
                .ok_or_else(|| ParquetError::External("read() must return a string".into()))?
                .as_bytes(),
        ))
    }
}

pub struct PhpSource {
    stream: Arc<PhpStream>,
    size: u64,
}

impl PhpSource {
    pub fn new(stream: Arc<PhpStream>) -> Result<Self, Error> {
        let size = stream
            .call("size", &mut [])
            .ok_or_else(|| Error::Stream("size() threw".to_string()))?
            .long()
            .ok_or_else(|| Error::Stream("size() must return the stream size".to_string()))?;

        Ok(Self {
            stream,
            size: u64::try_from(size).map_err(|_| Error::Stream(format!("size() returned {size}")))?,
        })
    }

    /// The footer, without the page index.
    pub fn metadata(&self) -> Result<parquet::file::metadata::ParquetMetaData, Error> {
        Ok(parquet::file::metadata::ParquetMetaDataReader::new().parse_and_finish(self)?)
    }
}

impl Length for PhpSource {
    fn len(&self) -> u64 {
        self.size
    }
}

impl ChunkReader for PhpSource {
    type T = WindowedRead<PhpStream>;

    fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
        Ok(WindowedRead::new(Arc::clone(&self.stream), start, self.size))
    }

    fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
        exact(self.stream.as_ref(), start, length)
    }
}

/// Exactly `length` bytes at `start`, in one read.
pub fn exact<R: ReadAt>(source: &R, start: u64, length: usize) -> Result<Bytes, ParquetError> {
    if length == 0 {
        return Ok(Bytes::new());
    }

    let bytes = source.read_at(start, length)?;

    if bytes.len() != length {
        return Err(ParquetError::EOF(format!("expected {length} bytes at offset {start}, read {}", bytes.len())));
    }

    Ok(bytes)
}

/// A `Read` from `position` to `end` that pulls `WINDOW` bytes at a time, only when the previous window is used up.
pub struct WindowedRead<R> {
    source: Arc<R>,
    position: u64,
    end: u64,
    window: Bytes,
}

impl<R: ReadAt> WindowedRead<R> {
    pub fn new(source: Arc<R>, position: u64, end: u64) -> Self {
        Self {
            source,
            position,
            end,
            window: Bytes::new(),
        }
    }
}

impl<R: ReadAt> Read for WindowedRead<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.window.is_empty() {
            if self.position >= self.end {
                return Ok(0);
            }

            let length = (self.end - self.position).min(WINDOW as u64) as usize;
            self.window = self.source.read_at(self.position, length).map_err(io::Error::other)?;
            self.position += self.window.len() as u64;
        }

        let read = buf.len().min(self.window.len());
        buf[..read].copy_from_slice(&self.window[..read]);
        self.window.advance(read);

        Ok(read)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bytes::Bytes;
    use parquet::errors::ParquetError;

    use super::{exact, ReadAt, WindowedRead, WINDOW};

    struct Counting {
        data: Vec<u8>,
        reads: AtomicUsize,
        bytes: AtomicUsize,
    }

    impl Counting {
        fn new(size: usize) -> Self {
            Self {
                data: (0..size).map(|i| i as u8).collect(),
                reads: AtomicUsize::new(0),
                bytes: AtomicUsize::new(0),
            }
        }
    }

    impl ReadAt for Counting {
        fn read_at(&self, offset: u64, length: usize) -> Result<Bytes, ParquetError> {
            let start = (offset as usize).min(self.data.len());
            let end = (start + length).min(self.data.len());
            self.reads.fetch_add(1, Ordering::Relaxed);
            self.bytes.fetch_add(end - start, Ordering::Relaxed);

            Ok(Bytes::copy_from_slice(&self.data[start..end]))
        }
    }

    #[test]
    fn windowed_read_pulls_one_window_for_a_small_read() {
        let source = Arc::new(Counting::new(10 * WINDOW));
        let mut read = WindowedRead::new(Arc::clone(&source), 100, source.data.len() as u64);
        let mut header = [0u8; 32];

        read.read_exact(&mut header).unwrap();

        assert_eq!(header.to_vec(), source.data[100..132].to_vec());
        assert_eq!((source.reads.load(Ordering::Relaxed), source.bytes.load(Ordering::Relaxed)), (1, WINDOW));
    }

    #[test]
    fn windowed_read_pulls_the_next_window_only_when_the_first_is_used_up() {
        let source = Arc::new(Counting::new(10 * WINDOW));
        let mut read = WindowedRead::new(Arc::clone(&source), 0, source.data.len() as u64);
        let mut buffer = vec![0u8; WINDOW + 1];

        read.read_exact(&mut buffer[..WINDOW]).unwrap();
        assert_eq!((source.reads.load(Ordering::Relaxed), source.bytes.load(Ordering::Relaxed)), (1, WINDOW));

        read.read_exact(&mut buffer[WINDOW..]).unwrap();
        assert_eq!(buffer, source.data[..WINDOW + 1].to_vec());
        assert_eq!((source.reads.load(Ordering::Relaxed), source.bytes.load(Ordering::Relaxed)), (2, 2 * WINDOW));
    }

    #[test]
    fn windowed_read_never_reads_past_the_end() {
        let source = Arc::new(Counting::new(3 * WINDOW));
        let mut read = WindowedRead::new(Arc::clone(&source), 3 * WINDOW as u64 - 10, 3 * WINDOW as u64);
        let mut rest = Vec::new();

        read.read_to_end(&mut rest).unwrap();

        assert_eq!(rest, source.data[3 * WINDOW - 10..].to_vec());
        assert_eq!((source.reads.load(Ordering::Relaxed), source.bytes.load(Ordering::Relaxed)), (1, 10));
    }

    #[test]
    fn exact_reads_the_length_asked_in_one_read() {
        let source = Counting::new(10 * WINDOW);

        assert_eq!(exact(&source, 5, 3 * WINDOW).unwrap().to_vec(), source.data[5..5 + 3 * WINDOW].to_vec());
        assert_eq!((source.reads.load(Ordering::Relaxed), source.bytes.load(Ordering::Relaxed)), (1, 3 * WINDOW));
    }

    #[test]
    fn exact_refuses_a_short_read() {
        let source = Counting::new(100);

        assert!(matches!(exact(&source, 90, 20), Err(ParquetError::EOF(_))));
    }

    #[test]
    fn exact_reads_nothing_for_zero_bytes() {
        let source = Counting::new(100);

        assert!(exact(&source, 0, 0).unwrap().is_empty());
        assert_eq!(source.reads.load(Ordering::Relaxed), 0);
    }
}
