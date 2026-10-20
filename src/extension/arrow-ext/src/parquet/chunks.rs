//! The column chunks of the leaves a filter reads and their column orders, sliced from the footer's thrift bytes
//! as the file holds them: the PHP planner decodes the bytes the PHP engine decodes, so both engines prune alike.

use std::ops::Range;

use crate::parquet::error::Error;
use crate::thrift::{elements as list, fields, Field, Malformed};

const FILE_ROW_GROUPS: i16 = 4;
const FILE_COLUMN_ORDERS: i16 = 7;
const FILE_ENCRYPTION_ALGORITHM: i16 = 8;
const ROW_GROUP_COLUMNS: i16 = 1;
const CHUNK_CRYPTO_METADATA: i16 = 8;
const CHUNK_ENCRYPTED_COLUMN_METADATA: i16 = 9;

/// Byte ranges into the footer, one entry per leaf asked for, in the order asked.
#[derive(Debug, PartialEq, Eq)]
pub struct ChunkSpans {
    /// Each leaf's `ColumnChunk` in every row group, in file order.
    pub chunks: Vec<Vec<Range<usize>>>,
    /// Each leaf's `ColumnOrder`; `None` when the file has no `column_orders`.
    pub orders: Option<Vec<Range<usize>>>,
}

/// `leaves`: indices into the schema's leaves.
pub fn chunk_spans(footer: &[u8], leaves: &[usize]) -> Result<ChunkSpans, Error> {
    let mut chunks = vec![Vec::new(); leaves.len()];
    let mut orders = None;

    for field in fields(footer) {
        let field = field.map_err(malformed)?;

        match field.id {
            FILE_ROW_GROUPS => {
                for group in elements(footer, &field, 0)? {
                    let group = group?;
                    let columns = row_group_columns(footer, &group)?.collect::<Result<Vec<_>, _>>()?;

                    for (position, leaf) in leaves.iter().enumerate() {
                        chunks[position].push(
                            columns
                                .get(*leaf)
                                .cloned()
                                .ok_or_else(|| corrupt("a row group without the leaf"))?,
                        );
                    }
                }
            }
            FILE_COLUMN_ORDERS => {
                let columns = elements(footer, &field, 0)?.collect::<Result<Vec<_>, _>>()?;

                orders = Some(
                    leaves
                        .iter()
                        .map(|leaf| {
                            columns
                                .get(*leaf)
                                .cloned()
                                .ok_or_else(|| corrupt("column orders without the leaf"))
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            _ => {}
        }
    }

    Ok(ChunkSpans { chunks, orders })
}

/// Refuses a footer that holds an encryption algorithm or an encrypted column chunk: arrow-rs decodes a plaintext
/// footer with encrypted columns without a word.
pub fn refuse_encrypted(footer: &[u8]) -> Result<(), Error> {
    for field in fields(footer) {
        let field = field.map_err(malformed)?;

        match field.id {
            FILE_ROW_GROUPS => {
                for group in elements(footer, &field, 0)? {
                    for chunk in row_group_columns(footer, &group?)? {
                        for field in fields(&footer[chunk?]) {
                            let field = field.map_err(malformed)?;

                            if field.id == CHUNK_CRYPTO_METADATA || field.id == CHUNK_ENCRYPTED_COLUMN_METADATA {
                                return Err(Error::Encrypted);
                            }
                        }
                    }
                }
            }
            FILE_ENCRYPTION_ALGORITHM => return Err(Error::Encrypted),
            _ => {}
        }
    }

    Ok(())
}

/// The `ColumnChunk`s of the row group at `group` in `footer`, as ranges into `footer`.
fn row_group_columns<'a>(
    footer: &'a [u8],
    group: &Range<usize>,
) -> Result<impl Iterator<Item = Result<Range<usize>, Error>> + 'a, Error> {
    let mut columns = None;

    for field in fields(&footer[group.clone()]) {
        let field = field.map_err(malformed)?;

        if field.id == ROW_GROUP_COLUMNS {
            columns = Some(field);
            break;
        }
    }

    elements(
        footer,
        &columns.ok_or_else(|| corrupt("a row group without columns"))?,
        group.start,
    )
}

/// The elements of the list `field` holds, `field` read from the struct at `offset` in `footer`, as ranges into
/// `footer`.
fn elements<'a>(
    footer: &'a [u8],
    field: &Field,
    offset: usize,
) -> Result<impl Iterator<Item = Result<Range<usize>, Error>> + 'a, Error> {
    if !field.is_list() {
        return Err(corrupt("another value where a list belongs"));
    }

    let start = offset + field.value.start;

    Ok(list(&footer[start..offset + field.value.end])
        .map_err(malformed)?
        .map(move |element| {
            element
                .map(|element| element.start + start..element.end + start)
                .map_err(malformed)
        }))
}

fn corrupt(what: &str) -> Error {
    Error::NotParquet(format!("Invalid Parquet file. The footer holds {what}"))
}

fn malformed(malformed: Malformed) -> Error {
    Error::NotParquet(format!(
        "Invalid Parquet file. The footer is not thrift: {}",
        malformed.0
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    use super::{chunk_spans, refuse_encrypted};
    use crate::parquet::error::Error;
    use crate::parquet::footer::Footer;
    use crate::thrift::{elements, fields};

    /// `id` Int64 and `name` Utf8 in two row groups of 2 rows.
    fn two_groups() -> Footer {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let mut file = Vec::new();
        let mut writer = ArrowWriter::try_new(
            &mut file,
            Arc::clone(&schema),
            Some(WriterProperties::builder().set_max_row_group_row_count(Some(2)).build()),
        )
        .unwrap();
        writer
            .write(
                &RecordBatch::try_new(
                    schema,
                    vec![
                        Arc::new(Int64Array::from(vec![1, 2, 3, 4])) as ArrayRef,
                        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                    ],
                )
                .unwrap(),
            )
            .unwrap();
        writer.close().unwrap();

        Footer::read(&Bytes::from(file)).unwrap()
    }

    /// `bytes` with `insert` put at `at` and the `remove` bytes after it dropped.
    fn spliced(bytes: &[u8], at: usize, remove: usize, insert: &[u8]) -> Vec<u8> {
        let mut spliced = bytes[..at].to_vec();
        spliced.extend_from_slice(insert);
        spliced.extend_from_slice(&bytes[at + remove..]);

        spliced
    }

    /// The footer with `field` (of compact wire type `wire`, then `value`) appended to the first chunk of `id`.
    fn with_chunk_field(bytes: &[u8], field: u8, wire: u8, value: &[u8]) -> Vec<u8> {
        let chunk = chunk_spans(bytes, &[0]).unwrap().chunks[0][0].clone();
        let last = fields(&bytes[chunk.clone()]).last().unwrap().unwrap().id as u8;
        let mut insert = vec![((field - last) << 4) | wire];
        insert.extend_from_slice(value);

        spliced(bytes, chunk.end - 1, 0, &insert)
    }

    fn assert_corrupt(result: Result<super::ChunkSpans, Error>, message: &str) {
        match result {
            Err(Error::NotParquet(actual)) => assert!(actual.contains(message), "{actual}"),
            other => panic!("expected NotParquet containing {message:?}, got {other:?}"),
        }
    }

    /// `ColumnChunk.meta_data.path_in_schema` of the chunk `span` holds.
    fn path(footer: &[u8], span: &std::ops::Range<usize>) -> Vec<u8> {
        let chunk = &footer[span.clone()];
        let meta = fields(chunk)
            .map(Result::unwrap)
            .find(|field| field.id == 3)
            .unwrap()
            .value;
        let path = fields(&chunk[meta.clone()])
            .map(Result::unwrap)
            .find(|field| field.id == 3)
            .unwrap()
            .value;
        let elements = elements(&chunk[meta.start + path.start..meta.start + path.end])
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        chunk[meta.start + path.start + elements[0].start + 1..meta.start + path.start + elements[0].end].to_vec()
    }

    #[test]
    fn the_spans_are_each_asked_leafs_chunk_per_row_group_and_its_column_order() {
        let footer = two_groups();
        let bytes = footer.bytes();
        let spans = chunk_spans(bytes, &[1, 0]).unwrap();

        assert_eq!(spans.chunks.len(), 2);
        assert_eq!(spans.chunks[0].len(), 2);
        assert_eq!(spans.chunks[1].len(), 2);
        assert_eq!(path(bytes, &spans.chunks[0][0]), b"name");
        assert_eq!(path(bytes, &spans.chunks[0][1]), b"name");
        assert_eq!(path(bytes, &spans.chunks[1][1]), b"id");

        let orders = spans.orders.unwrap();

        // a ColumnOrder union holding TYPE_ORDER (field 1, an empty struct)
        assert_eq!(&bytes[orders[0].clone()], b"\x1c\x00\x00");
        assert_eq!(&bytes[orders[1].clone()], b"\x1c\x00\x00");
    }

    #[test]
    fn a_file_without_column_orders_has_no_orders() {
        let footer = two_groups();
        let bytes = footer.bytes();
        let orders = fields(bytes)
            .map(Result::unwrap)
            .find(|field| field.id == 7)
            .unwrap()
            .value;

        // column_orders is the last field: its header is one byte, delta 1 and wire type LIST
        assert_eq!(bytes[orders.start - 1], 0x19);

        let without = spliced(bytes, orders.start - 1, orders.len() + 1, &[]);
        let spans = chunk_spans(&without, &[1]).unwrap();

        assert_eq!(spans.orders, None);
        assert_eq!(path(&without, &spans.chunks[0][1]), b"name");
    }

    #[test]
    fn a_row_group_without_the_asked_leaf_is_corrupt() {
        assert_corrupt(chunk_spans(two_groups().bytes(), &[2]), "a row group without the leaf");
    }

    #[test]
    fn a_truncated_footer_is_not_thrift() {
        let footer = two_groups();
        let bytes = footer.bytes();

        assert_corrupt(chunk_spans(&bytes[..bytes.len() / 2], &[0]), "The footer is not thrift");
    }

    #[test]
    fn a_list_field_of_another_wire_type_is_corrupt() {
        // row_groups (field 4) as the i32 1: header delta 4, wire type I32, zigzag 2, then the struct's stop
        assert_corrupt(chunk_spans(b"\x45\x02\x00", &[0]), "another value where a list belongs");
    }

    #[test]
    fn an_encrypted_column_chunk_is_refused() {
        let footer = two_groups();
        let bytes = footer.bytes();

        // crypto_metadata (8): ENCRYPTION_WITH_FOOTER_KEY, an empty struct in a union
        assert!(matches!(
            refuse_encrypted(&with_chunk_field(bytes, 8, 12, b"\x1c\x00\x00")),
            Err(Error::Encrypted)
        ));
        // encrypted_column_metadata (9): one byte of binary
        assert!(matches!(
            refuse_encrypted(&with_chunk_field(bytes, 9, 8, b"\x01\xaa")),
            Err(Error::Encrypted)
        ));
        assert!(refuse_encrypted(bytes).is_ok());
    }

    #[test]
    fn an_encryption_algorithm_is_refused() {
        let footer = two_groups();
        let bytes = footer.bytes();

        // encryption_algorithm (8) after column_orders (7): AES_GCM_V1, an empty struct in a union
        assert!(matches!(
            refuse_encrypted(&spliced(bytes, bytes.len() - 1, 0, b"\x1c\x1c\x00\x00")),
            Err(Error::Encrypted)
        ));
    }
}
