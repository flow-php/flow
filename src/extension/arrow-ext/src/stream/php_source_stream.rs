use bytes::Bytes;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZvalDyn;
use ext_php_rs::types::ZendObject;
use ext_php_rs::zend::ClassEntry;
use parquet::errors::ParquetError;
use parquet::file::reader::{ChunkReader, Length};
use std::io::Cursor;
use std::sync::Arc;

struct PhpSourceStreamInner {
    obj: ZBox<ZendObject>,
    len: u64,
}

// SAFETY: PHP runs single-threaded (NTS). PhpSourceStream is only used
// on the PHP request thread. The parquet crate requires Send+Sync for
// ChunkReader but never sends across threads in synchronous usage.
unsafe impl Send for PhpSourceStreamInner {}
unsafe impl Sync for PhpSourceStreamInner {}

#[derive(Clone)]
pub struct PhpSourceStream {
    inner: Arc<PhpSourceStreamInner>,
}

impl PhpSourceStream {
    pub fn new(obj: &mut ZendObject) -> Result<Self, String> {
        let ce = ClassEntry::try_find("Flow\\Arrow\\RandomAccessFile")
            .ok_or_else(|| "Interface Flow\\Arrow\\RandomAccessFile not loaded. Did you require the file?".to_string())?;
        if !obj.instance_of(ce) {
            return Err("Source must implement Flow\\Arrow\\RandomAccessFile".to_string());
        }

        let ptr: *mut ZendObject = obj;
        unsafe { (*ptr).gc.refcount += 1 };
        let owned = unsafe { ZBox::from_raw(ptr) };

        let zval = owned.try_call_method("size", vec![])
            .map_err(|e| format!("Failed to call size(): {:?}", e))?;

        if zval.is_null() {
            return Err("size() returned null; file size is required for Parquet reading".into());
        }

        let size = zval.long()
            .ok_or_else(|| "size() must return an integer".to_string())?;

        Ok(Self {
            inner: Arc::new(PhpSourceStreamInner {
                obj: owned,
                len: size as u64,
            }),
        })
    }
}

impl Length for PhpSourceStream {
    fn len(&self) -> u64 {
        self.inner.len
    }
}

impl ChunkReader for PhpSourceStream {
    type T = Cursor<Vec<u8>>;

    fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
        let length = self.inner.len.saturating_sub(start) as usize;
        let bytes = self.get_bytes(start, length)?;
        Ok(Cursor::new(bytes.to_vec()))
    }

    fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<Bytes> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let length_arg = length as i64;
        let offset_arg = start as i64;

        let result = self.inner.obj
            .try_call_method(
                "read",
                vec![
                    &length_arg as &dyn IntoZvalDyn,
                    &offset_arg as &dyn IntoZvalDyn,
                ],
            )
            .map_err(|e| ParquetError::General(format!("Failed to call read(): {:?}", e)))?;

        let zend_str = result
            .zend_str()
            .ok_or_else(|| ParquetError::General("read() must return a string".into()))?;

        Ok(Bytes::copy_from_slice(zend_str.as_bytes()))
    }
}
