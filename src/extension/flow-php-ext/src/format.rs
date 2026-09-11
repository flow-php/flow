//! Floe frame-body wire constants and a bounds-checked byte reader. The canonical
//! definitions live in the pure-PHP `Flow\Floe\Format`; mirror any change in the same PR.

use ext_php_rs::exception::PhpException;

use crate::exception::ext_exception;

// Floe is little-endian by construction; a big-endian host could never match its bytes.
#[cfg(target_endian = "big")]
compile_error!("flow_php only supports little-endian targets");

pub const VALUE_NULL: u8 = 0x00;
pub const VALUE_PRESENT: u8 = 0x01;
pub const VALUE_NULL_WITH_META: u8 = 0x02;
pub const VALUE_ABSENT: u8 = 0x03;
pub const VALUE_PRESENT_WITH_META: u8 = 0x04;

pub struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    pub fn is_eof(&self) -> bool {
        self.pos >= self.data.len()
    }

    fn take(&mut self, len: usize, context: &str) -> Result<&'a [u8], PhpException> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.data.len());

        match end {
            Some(end) => {
                let slice = &self.data[self.pos..end];
                self.pos = end;
                Ok(slice)
            }
            None => Err(ext_exception(format!(
                "flow_php frame body is truncated, {context} is incomplete"
            ))),
        }
    }

    pub fn u8(&mut self, context: &str) -> Result<u8, PhpException> {
        Ok(self.take(1, context)?[0])
    }

    pub fn u32(&mut self, context: &str) -> Result<u32, PhpException> {
        let bytes = self.take(4, context)?;

        Ok(u32::from_le_bytes(bytes.try_into().expect("4-byte slice")))
    }

    pub fn i64(&mut self, context: &str) -> Result<i64, PhpException> {
        let bytes = self.take(8, context)?;

        Ok(i64::from_le_bytes(bytes.try_into().expect("8-byte slice")))
    }

    pub fn f64(&mut self, context: &str) -> Result<f64, PhpException> {
        let bytes = self.take(8, context)?;

        Ok(f64::from_le_bytes(bytes.try_into().expect("8-byte slice")))
    }

    pub fn bytes(&mut self, len: usize, context: &str) -> Result<&'a [u8], PhpException> {
        self.take(len, context)
    }
}

pub fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
