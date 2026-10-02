//! A Parquet file's footer, read and decoded once: the readers plan and read from its `ParquetMetaData`, the lib
//! surface decodes its thrift bytes into PHP objects.

use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::reader::ChunkReader;

use crate::parquet::error::Error;

/// What the first read takes from the end of the file: the whole footer of every file it is shorter than.
const TAIL: u64 = 64 * 1024;

const MAGIC: &[u8; 4] = b"PAR1";

pub struct Footer {
    /// The thrift `FileMetaData`.
    bytes: Bytes,
    meta: Arc<ParquetMetaData>,
}

impl Footer {
    /// One read of the last min(size, 64 KiB) bytes, a second only when the footer is longer.
    pub fn read<R: ChunkReader>(source: &R) -> Result<Self, Error> {
        let size = source.len();

        if size < 8 {
            return Err(corrupt(format!(
                "Invalid Parquet file. Size is {size} bytes, smaller than the footer"
            )));
        }

        let read = size.min(TAIL);
        let tail = source.get_bytes(size - read, read as usize)?;
        let trailer = &tail[tail.len() - 8..];

        if &trailer[4..] != MAGIC {
            return Err(corrupt("Invalid Parquet file. Corrupt footer".to_string()));
        }

        let length = u64::from(u32::from_le_bytes([trailer[0], trailer[1], trailer[2], trailer[3]]));

        if length + 8 > size {
            return Err(corrupt(format!(
                "Invalid Parquet file. Reported metadata length of {length} + 8 byte footer, but file is only {size} \
                 bytes"
            )));
        }

        let end = tail.len() - 8;
        let bytes = if length + 8 <= read {
            tail.slice(end - length as usize..end)
        } else {
            let mut bytes = BytesMut::with_capacity(length as usize);
            bytes.extend_from_slice(&source.get_bytes(size - 8 - length, (length + 8 - read) as usize)?);
            bytes.extend_from_slice(&tail[..end]);
            bytes.freeze()
        };
        let meta = ParquetMetaDataReader::decode_metadata(&bytes)?;

        Ok(Self {
            bytes,
            meta: Arc::new(meta),
        })
    }

    pub fn meta(&self) -> &Arc<ParquetMetaData> {
        &self.meta
    }

    pub fn bytes(&self) -> &Bytes {
        &self.bytes
    }
}

fn corrupt(message: String) -> Error {
    Error::NotParquet(message)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::errors::ParquetError;
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use parquet::file::reader::{ChunkReader, Length};

    use super::Footer;
    use crate::parquet::error::Error;
    use crate::parquet::source::fake::Counting;
    use crate::parquet::source::{exact, WindowedRead};

    struct Source(Arc<Counting>);

    impl Length for Source {
        fn len(&self) -> u64 {
            self.0.data.len() as u64
        }
    }

    impl ChunkReader for Source {
        type T = WindowedRead<Counting>;

        fn get_read(&self, start: u64) -> Result<Self::T, ParquetError> {
            Ok(WindowedRead::new(Arc::clone(&self.0), start, self.len()))
        }

        fn get_bytes(&self, start: u64, length: usize) -> Result<Bytes, ParquetError> {
            exact(self.0.as_ref(), start, length)
        }
    }

    /// A file whose footer carries `padding` bytes of key/value metadata.
    fn file(padding: usize) -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![KeyValue::new("padding".to_string(), "x".repeat(padding))]))
            .build();
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut file, Arc::clone(&schema), Some(props)).unwrap();
        writer
            .write(&RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap())
            .unwrap();
        writer.close().unwrap();

        file
    }

    #[test]
    fn a_footer_inside_the_tail_is_read_once() {
        let counting = Arc::new(Counting::new(file(10)));
        let footer = Footer::read(&Source(Arc::clone(&counting))).unwrap();

        assert_eq!(counting.counted().0, 1);
        assert_eq!(footer.meta().file_metadata().num_rows(), 3);

        let data = &counting.data;
        let length = u32::from_le_bytes(data[data.len() - 8..data.len() - 4].try_into().unwrap()) as usize;
        assert_eq!(footer.bytes().as_ref(), &data[data.len() - 8 - length..data.len() - 8]);
    }

    #[test]
    fn a_footer_longer_than_the_tail_takes_a_second_read() {
        let counting = Arc::new(Counting::new(file(100 * 1024)));
        let footer = Footer::read(&Source(Arc::clone(&counting))).unwrap();

        assert_eq!(counting.counted().0, 2);
        assert_eq!(footer.meta().file_metadata().num_rows(), 3);
        assert_eq!(
            footer.meta().file_metadata().key_value_metadata().unwrap()[0]
                .value
                .as_ref()
                .map(String::len),
            Some(100 * 1024)
        );
    }

    #[test]
    fn a_file_without_the_magic_is_refused() {
        let mut data = file(10);
        let end = data.len();
        data[end - 1] = b'X';

        assert!(matches!(
            Footer::read(&Source(Arc::new(Counting::new(data)))),
            Err(Error::NotParquet(message)) if message.contains("Corrupt footer")
        ));
        assert!(matches!(
            Footer::read(&Source(Arc::new(Counting::new(vec![1, 2, 3])))),
            Err(Error::NotParquet(_))
        ));
    }
}
