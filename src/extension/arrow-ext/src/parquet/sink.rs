//! Parquet bytes to a `Flow\Filesystem\DestinationStream` (`append(string $data)`).

use std::io::{self, Write};
use std::sync::Arc;

use crate::parquet::source::PhpStream;
use crate::php::zval_str;

pub struct PhpSink {
    stream: Arc<PhpStream>,
}

impl PhpSink {
    pub fn new(stream: Arc<PhpStream>) -> Self {
        Self { stream }
    }
}

impl Write for PhpSink {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.stream
            .call("append", &mut [zval_str(buf)])
            .ok_or_else(|| io::Error::other("append() threw"))?;

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
