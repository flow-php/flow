//! Floe wire constants and a bounds-checked little-endian reader. The canonical definitions live in the pure-PHP
//! `Flow\Floe\Format`; mirror any change there.

use crate::error::Error;

pub const MAGIC: &[u8; 4] = b"FLOE";
pub const VERSION: u8 = 0x02;

pub const FRAME_BATCH: u8 = 0x05;
pub const FRAME_FOOTER: u8 = 0x06;

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

    fn take(&mut self, len: usize) -> Result<&'a [u8], Error> {
        let end = self
            .pos
            .checked_add(len)
            .filter(|end| *end <= self.data.len())
            .ok_or(Error::DirectoryTruncated)?;
        let slice = &self.data[self.pos..end];
        self.pos = end;

        Ok(slice)
    }

    pub fn u8(&mut self) -> Result<u8, Error> {
        Ok(self.take(1)?[0])
    }

    pub fn u32(&mut self) -> Result<u32, Error> {
        let bytes = self.take(4)?;

        Ok(u32::from_le_bytes(bytes.try_into().expect("4-byte slice")))
    }

    pub fn i64(&mut self) -> Result<i64, Error> {
        let bytes = self.take(8)?;

        Ok(i64::from_le_bytes(bytes.try_into().expect("8-byte slice")))
    }

    pub fn f64(&mut self) -> Result<f64, Error> {
        let bytes = self.take(8)?;

        Ok(f64::from_le_bytes(bytes.try_into().expect("8-byte slice")))
    }

    pub fn bytes(&mut self, len: usize) -> Result<&'a [u8], Error> {
        self.take(len)
    }
}

pub fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
