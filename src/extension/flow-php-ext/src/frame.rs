//! The PHP side of `RustBackend::decodeFrame()` and `encodeFrame()`: the frame body's zend_string as the owner of the
//! Arrow buffers that point into it, and a PHP `Flow\ETL\Values\Column\Frame\Codec` as the crate's `Codec`.

use std::sync::Arc;

use arrow_buffer::alloc::Allocation;
use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::{ext_php_rs_zend_string_release, zend_string, GC_IMMUTABLE};
use ext_php_rs::flags::DataType;
use ext_php_rs::types::Zval;
use flow_batch_frame::frame::Codec;

use crate::ctx::{call_method, zval_str};
use crate::exception::ext_exception;

/// Held while buffers point into it; the pointer comes from the zval, never a `&ZendStr` (writing through that is UB).
pub struct FrameBody(*mut zend_string);

impl FrameBody {
    /// None for an interned or immutable string: those are not refcounted, so such a body is copied instead.
    pub fn retain(string: *mut zend_string) -> Option<Arc<dyn Allocation>> {
        // SAFETY: `string` is the live zend_string of a string zval the engine handed to this call; `type_info` is the
        // only view of the `u` union.
        unsafe {
            if (*string).gc.u.type_info & GC_IMMUTABLE != 0 {
                return None;
            }

            (*string).gc.refcount += 1;
        }

        Some(Arc::new(FrameBody(string)))
    }
}

impl Drop for FrameBody {
    fn drop(&mut self) {
        // SAFETY: the refcount taken in `retain` is released exactly once, here.
        unsafe { ext_php_rs_zend_string_release(self.0) };
    }
}

/// The string zval's zend_string and its bytes; None for another type.
pub fn string_of(zv: &Zval) -> Option<(*mut zend_string, &[u8])> {
    let zv = zv.dereference();

    if zv.get_type() != DataType::String {
        return None;
    }

    // SAFETY: a string zval's `value.str_` is its zend_string; the slice covers `len` bytes of its payload and borrows
    // the payload only, so the refcount may change underneath it.
    unsafe {
        let string = zv.value.str_;

        Some((
            string,
            std::slice::from_raw_parts((*string).val.as_ptr().cast::<u8>(), (*string).len),
        ))
    }
}

// `Allocation` wants `Send + Sync`. The engine is single-threaded and arrow-rs never moves a buffer across threads
// here; arrow's own `FFI_ArrowArray` owner carries the same impls.
unsafe impl Send for FrameBody {}
unsafe impl Sync for FrameBody {}

/// `$codec->decode($bytes)` and `$codec->encode($bytes)`.
pub struct PhpCodec<'z>(pub &'z Zval);

impl Codec for PhpCodec<'_> {
    type Error = PhpException;

    fn decode(&mut self, stored: &[u8], _length: u64) -> Result<Vec<u8>, PhpException> {
        let decoded = call_method(self.0, "decode", &mut [zval_str(stored)])?;

        Ok(decoded
            .zend_str()
            .ok_or_else(|| ext_exception("flow_php expected Codec::decode() to return a string"))?
            .as_bytes()
            .to_vec())
    }

    fn encode(&mut self, raw: &[u8]) -> Result<Vec<u8>, PhpException> {
        let encoded = call_method(self.0, "encode", &mut [zval_str(raw)])?;

        Ok(encoded
            .zend_str()
            .ok_or_else(|| ext_exception("flow_php expected Codec::encode() to return a string"))?
            .as_bytes()
            .to_vec())
    }
}
