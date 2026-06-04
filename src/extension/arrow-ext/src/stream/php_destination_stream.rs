use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZvalDyn;
use ext_php_rs::types::{ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;
use std::io::{self, Write};

pub struct PhpDestinationStream {
    obj: ZBox<ZendObject>,
}

// SAFETY: PhpDestinationStream wraps a PHP ZendObject that belongs to the
// current request thread. In NTS mode PHP is single-threaded; in ZTS mode each
// thread owns its own request context. The parquet crate requires Send+Sync for
// ArrowWriter<W: Write> but never sends across threads in synchronous usage.
unsafe impl Send for PhpDestinationStream {}
unsafe impl Sync for PhpDestinationStream {}

impl PhpDestinationStream {
    pub fn new(obj: &mut ZendObject) -> Result<Self, String> {
        let ce = ClassEntry::try_find("Flow\\Arrow\\OutputStream").ok_or_else(|| {
            "Interface Flow\\Arrow\\OutputStream not loaded. Did you require the file?".to_string()
        })?;
        if !obj.instance_of(ce) {
            return Err("Destination must implement Flow\\Arrow\\OutputStream".to_string());
        }

        let ptr: *mut ZendObject = obj;
        unsafe { (*ptr).gc.refcount += 1 };
        Ok(Self {
            obj: unsafe { ZBox::from_raw(ptr) },
        })
    }
}

impl Write for PhpDestinationStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut data = Zval::new();
        data.set_binary(buf.to_vec());

        let _result = self
            .obj
            .try_call_method("append", vec![&data as &dyn IntoZvalDyn])
            .map_err(|e| io::Error::other(format!("Failed to call append(): {:?}", e)))?;

        // OutputStream::append() is all-or-nothing: it returns self on success
        // or throws on failure (caught above). No partial write path exists.
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
