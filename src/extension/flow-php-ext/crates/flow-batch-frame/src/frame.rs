//! A Floe BATCH frame body (`PhpBackend::encodeFrame()`) from one array per schema column, and back the way
//! `PhpBackend::decodeFrame()` reads it: the directory (`rows, nodes, buffers` then `(length, nulls)` per node and
//! `(offset, stored)` per buffer, padded to 8 bytes), then the buffer area where every stored buffer is an 8-byte length
//! prefix (-1 = raw) and its bytes, padded to 8. A raw buffer's bytes are referenced in place when an owner keeps the
//! body alive.

use std::convert::Infallible;
use std::sync::Arc;

use arrow_buffer::alloc::Allocation;
use arrow_data::ArrayData;

use crate::column::{decode_sources, Source};
use crate::error::Error;
use crate::kind::Kind;
use crate::layout::{buffer_count, node_count, nodes, nodes_at, null_count_of};

/// Floe's per-buffer codec: `stored` is the buffer as written, `length` the byte count its prefix declares.
pub trait Codec {
    type Error;

    fn decode(&mut self, stored: &[u8], length: u64) -> Result<Vec<u8>, Self::Error>;

    /// The buffer as the frame stores it; the frame keeps the raw bytes (prefix -1) unless this is shorter.
    fn encode(&mut self, raw: &[u8]) -> Result<Vec<u8>, Self::Error>;
}

/// The identity codec: a prefixed buffer under it is a lie the length check catches, as `NoopCodec` does in PHP.
pub struct Noop;

impl Codec for Noop {
    type Error = Infallible;

    fn decode(&mut self, stored: &[u8], _length: u64) -> Result<Vec<u8>, Infallible> {
        Ok(stored.to_vec())
    }

    fn encode(&mut self, raw: &[u8]) -> Result<Vec<u8>, Infallible> {
        Ok(raw.to_vec())
    }
}

#[derive(Debug)]
pub enum Failure<E> {
    /// The directory, a buffer extent, or one column's buffers (`Error::MalformedColumn`).
    Frame(Error),
    Codec(E),
}

pub struct Frame {
    pub rows: u32,
    pub columns: Vec<ArrayData>,
}

const COUNTS: usize = 12;
const PREFIX: usize = 8;

/// One non-empty buffer as the frame stores it: its length prefix (-1 = raw) and its bytes.
struct Stored {
    prefix: i64,
    bytes: Vec<u8>,
}

/// The mirror of `decode`: `columns` in schema order, each with its kind; one allocation of the exact frame size.
/// Nodes come from `layout::nodes(kind, len, null_count, &buffers)` over `column::encode(data, kind)`, as
/// `PhpBackend::encodeFrame()` computes them from `BufferLayout::nodes`.
pub fn encode<C: Codec>(
    columns: &[(&ArrayData, &Kind)],
    rows: u32,
    codec: &mut C,
) -> Result<Vec<u8>, Failure<C::Error>> {
    let mut node_words = Vec::new();
    let mut stored = Vec::new();

    for (data, kind) in columns {
        let buffers = crate::column::encode(data, kind).map_err(Failure::Frame)?;
        let nulls = match kind {
            Kind::Null => rows,
            _ => null_count_of(&buffers[0], rows).map_err(Failure::Frame)?,
        };

        if node_count(kind) == 1 {
            node_words.push((rows, nulls));
        } else {
            let views = buffers.iter().map(Vec::as_slice).collect::<Vec<&[u8]>>();
            node_words.extend(nodes(kind, rows, nulls, &views).map_err(Failure::Frame)?);
        }

        for raw in buffers {
            if raw.is_empty() {
                stored.push(None);

                continue;
            }

            let encoded = codec.encode(&raw).map_err(Failure::Codec)?;

            stored.push(Some(if encoded.len() < raw.len() {
                Stored {
                    prefix: raw.len() as i64,
                    bytes: encoded,
                }
            } else {
                Stored { prefix: -1, bytes: raw }
            }));
        }
    }

    let directory = padded(COUNTS + 8 * (node_words.len() + stored.len()));
    let total = frame_length(
        directory,
        stored.iter().flatten().map(|buffer| PREFIX + buffer.bytes.len()),
    )
    .map_err(Failure::Frame)?;
    let mut body = Vec::with_capacity(total);
    body.extend_from_slice(&rows.to_le_bytes());
    body.extend_from_slice(&(node_words.len() as u32).to_le_bytes());
    body.extend_from_slice(&(stored.len() as u32).to_le_bytes());

    for (length, nulls) in &node_words {
        body.extend_from_slice(&length.to_le_bytes());
        body.extend_from_slice(&nulls.to_le_bytes());
    }

    let mut offset = 0;

    for buffer in &stored {
        let (at, length) = match buffer {
            Some(buffer) => (offset, PREFIX + buffer.bytes.len()),
            None => (0, 0),
        };
        body.extend_from_slice(&(at as u32).to_le_bytes());
        body.extend_from_slice(&(length as u32).to_le_bytes());
        offset += padded(length);
    }

    body.resize(directory, 0);

    for buffer in stored.into_iter().flatten() {
        body.extend_from_slice(&buffer.prefix.to_le_bytes());
        body.extend_from_slice(&buffer.bytes);
        body.resize(padded(body.len()), 0);
    }

    Ok(body)
}

fn padded(length: usize) -> usize {
    length.div_ceil(8) * 8
}

/// The body's length - the padded directory and every stored buffer padded to 8 - refused past the frame's u32 length
/// before anything is allocated for it.
fn frame_length(directory: usize, stored: impl Iterator<Item = usize>) -> Result<usize, Error> {
    let bytes = stored.fold(directory as u64, |sum, length| sum + padded(length) as u64);

    if bytes > u64::from(u32::MAX) {
        return Err(Error::FrameTooLarge { bytes });
    }

    Ok(bytes as usize)
}

/// `kinds` are the schema's columns in order. With an `owner`, every raw buffer whose payload is 8-byte aligned becomes
/// a `Buffer` into `body` kept alive by `owner`; without one, or for a misaligned or codec-decoded buffer, the bytes are
/// copied as `column::decode` copies them.
pub fn decode<C: Codec>(
    body: &[u8],
    owner: Option<&Arc<dyn Allocation>>,
    kinds: &[&Kind],
    codec: &mut C,
) -> Result<Frame, Failure<C::Error>> {
    if body.len() < COUNTS {
        return Err(Failure::Frame(Error::DirectoryTruncated));
    }

    let word = |at: usize| u32::from_le_bytes(body[at..at + 4].try_into().expect("4-byte slice"));
    let rows = word(0);
    let nodes = word(4) as usize;
    let buffers = word(8) as usize;
    let expected_nodes = kinds.iter().map(|kind| node_count(kind)).sum::<usize>();
    let expected_buffers = kinds.iter().map(|kind| buffer_count(kind)).sum::<usize>();

    if nodes != expected_nodes {
        return Err(Failure::Frame(Error::FrameNodeCount {
            frame: nodes as u64,
            schema: expected_nodes as u64,
        }));
    }

    if buffers != expected_buffers {
        return Err(Failure::Frame(Error::FrameBufferCount {
            frame: buffers as u64,
            schema: expected_buffers as u64,
        }));
    }

    let area_start = (COUNTS + 8 * (nodes + buffers)).div_ceil(8) * 8;

    if body.len() < area_start {
        return Err(Failure::Frame(Error::DirectoryTruncated));
    }

    let node_word = |node: usize, second: bool| word(COUNTS + 8 * node + if second { 4 } else { 0 });
    let extent_word = |extent: usize| word(COUNTS + 4 * extent);
    let area = &body[area_start..];
    let mut node = 0;
    let mut extent = 2 * nodes;
    let mut buffer = 0;
    let mut columns = Vec::with_capacity(kinds.len());

    for (column, kind) in kinds.iter().enumerate() {
        let length = node_word(node, false);
        let nulls = node_word(node, true);

        if length != rows {
            return Err(Failure::Frame(Error::ColumnRows {
                column,
                rows: length as u64,
                frame_rows: rows as u64,
            }));
        }

        if nulls > length {
            return Err(Failure::Frame(Error::ColumnNulls {
                column,
                nulls: nulls as u64,
                rows: length as u64,
            }));
        }

        let mut sources = Vec::with_capacity(buffer_count(kind));

        for _ in 0..buffer_count(kind) {
            let index = buffer;
            let offset = extent_word(extent) as usize;
            let stored = extent_word(extent + 1) as usize;
            extent += 2;
            buffer += 1;

            if stored == 0 {
                sources.push(Source::Copy(&[]));

                continue;
            }

            if offset + stored > area.len() {
                return Err(Failure::Frame(Error::BufferOutsideArea {
                    buffer: index,
                    area: area.len() as u64,
                }));
            }

            if stored < PREFIX {
                return Err(Failure::Frame(Error::BufferShorterThanPrefix { buffer: index }));
            }

            let prefix = i64::from_le_bytes(area[offset..offset + PREFIX].try_into().expect("8-byte slice"));
            let bytes = &area[offset + PREFIX..offset + stored];

            if prefix == -1 {
                sources.push(match owner {
                    Some(owner) => Source::Shared(bytes, owner),
                    None => Source::Copy(bytes),
                });

                continue;
            }

            let decoded = codec.decode(bytes, prefix as u64).map_err(Failure::Codec)?;

            if prefix < 0 || decoded.len() as u64 != prefix as u64 {
                return Err(Failure::Frame(Error::DecodedLength {
                    buffer: index,
                    decoded: decoded.len() as u64,
                    prefix,
                }));
            }

            sources.push(Source::Owned(decoded));
        }

        let malformed = |error: Error| {
            Failure::Frame(Error::MalformedColumn {
                column,
                error: Box::new(error),
            })
        };

        if node_count(kind) == 1 {
            node += 1;
        } else {
            let derived = nodes_at(
                kind,
                0,
                length,
                nulls,
                &sources.iter().map(Source::bytes).collect::<Vec<_>>(),
            )
            .map_err(malformed)?;

            for (derived_length, derived_nulls) in derived {
                if node_word(node, false) != derived_length || node_word(node, true) != derived_nulls {
                    return Err(Failure::Frame(Error::NodesDisagree { column }));
                }

                node += 1;
            }
        }

        columns.push(decode_sources(kind, sources, length, nulls).map_err(malformed)?);
    }

    Ok(Frame { rows, columns })
}

#[cfg(test)]
mod tests {
    use std::convert::Infallible;
    use std::sync::Arc;

    use arrow_array::{make_array, Array, Int64Array, LargeBinaryArray, ListArray};
    use arrow_buffer::alloc::Allocation;
    use arrow_buffer::Buffer;
    use arrow_data::ArrayData;

    use super::{decode, encode, frame_length, Codec, Failure, Noop};
    use crate::error::Error;
    use crate::kind::{data_type, Field, Kind};

    /// One column's declared nodes and its stored buffers: `(prefix, bytes)` per buffer, `None` for an omitted one.
    struct Column {
        nodes: Vec<(u32, u32)>,
        buffers: Vec<Option<(i64, Vec<u8>)>>,
    }

    fn raw(bytes: &[u8]) -> Option<(i64, Vec<u8>)> {
        Some((-1, bytes.to_vec()))
    }

    fn padded(bytes: &mut Vec<u8>) {
        while !bytes.len().is_multiple_of(8) {
            bytes.push(0);
        }
    }

    /// `PhpBackend::encodeFrame()`: counts, node words, extent words, padding, then every stored buffer padded to 8.
    fn body(rows: u32, columns: &[Column]) -> Vec<u8> {
        let mut nodes = Vec::new();
        let mut extents = Vec::new();
        let mut area = Vec::new();

        for column in columns {
            for (length, nulls) in &column.nodes {
                nodes.extend_from_slice(&length.to_le_bytes());
                nodes.extend_from_slice(&nulls.to_le_bytes());
            }

            for buffer in &column.buffers {
                let Some((prefix, bytes)) = buffer else {
                    extents.extend_from_slice(&[0u8; 8]);

                    continue;
                };
                let mut stored = prefix.to_le_bytes().to_vec();
                stored.extend_from_slice(bytes);
                extents.extend_from_slice(&(area.len() as u32).to_le_bytes());
                extents.extend_from_slice(&(stored.len() as u32).to_le_bytes());
                area.extend_from_slice(&stored);
                padded(&mut area);
            }
        }

        let mut body = rows.to_le_bytes().to_vec();
        body.extend_from_slice(&((nodes.len() / 8) as u32).to_le_bytes());
        body.extend_from_slice(&((extents.len() / 8) as u32).to_le_bytes());
        body.extend_from_slice(&nodes);
        body.extend_from_slice(&extents);
        padded(&mut body);
        body.extend_from_slice(&area);

        body
    }

    fn i64s(values: &[i64]) -> Vec<u8> {
        values.iter().flat_map(|value| value.to_le_bytes()).collect()
    }

    fn u32s(values: &[u32]) -> Vec<u8> {
        values.iter().flat_map(|value| value.to_le_bytes()).collect()
    }

    fn int_column(values: &[i64]) -> Column {
        Column {
            nodes: vec![(values.len() as u32, 0)],
            buffers: vec![None, raw(&i64s(values))],
        }
    }

    /// The body behind an `Arc`, the `Allocation` view of that `Arc` (a second strong count) and the body's address range.
    fn owner(body: &[u8]) -> (Arc<Vec<u8>>, Arc<dyn Allocation>, std::ops::Range<usize>) {
        let owned = Arc::new(body.to_vec());
        let range = owned.as_ptr() as usize..owned.as_ptr() as usize + owned.len();
        let shared: Arc<dyn Allocation> = Arc::<Vec<u8>>::clone(&owned);

        (owned, shared, range)
    }

    fn inside(range: &std::ops::Range<usize>, data: &arrow_data::ArrayData, index: usize) -> bool {
        range.contains(&(data.buffers()[index].as_ptr() as usize))
    }

    #[test]
    fn decodes_a_flat_frame_without_copying() {
        let int = Kind::Int64;
        let bytes = Kind::Bytes;
        let frame = body(
            3,
            &[
                int_column(&[1, 2, 3]),
                Column {
                    nodes: vec![(3, 1)],
                    buffers: vec![raw(&[0b101]), raw(&u32s(&[0, 2, 2, 5])), raw(b"abcde")],
                },
            ],
        );
        let (body, shared, range) = owner(&frame);

        let decoded = decode(&body, Some(&shared), &[&int, &bytes], &mut Noop).expect("a frame");

        assert_eq!(decoded.rows, 3);
        assert_eq!(
            make_array(decoded.columns[0].clone())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2, 3]
        );
        assert!(inside(&range, &decoded.columns[0], 0), "int values point into the body");
        assert!(
            inside(&range, &decoded.columns[1], 1),
            "string data points into the body"
        );
        assert!(!inside(&range, &decoded.columns[1], 0), "widened offsets are a copy");
        assert!(range.contains(&(decoded.columns[1].nulls().unwrap().buffer().as_ptr() as usize)));
        // the body, its Allocation view, then int values, string validity, string data
        assert_eq!(Arc::strong_count(&body), 2 + 3);

        let strings = make_array(decoded.columns[1].clone());
        let strings = strings.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
        assert_eq!(strings.value(0), b"ab");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), b"cde");
    }

    #[test]
    fn copies_a_misaligned_body() {
        let int = Kind::Int64;
        let mut shifted = vec![0u8];
        shifted.extend(body(2, &[int_column(&[7, 8])]));
        let (body, shared, range) = owner(&shifted);

        let decoded = decode(&body[1..], Some(&shared), &[&int], &mut Noop).expect("a frame");

        assert!(!inside(&range, &decoded.columns[0], 0));
        assert_eq!(Arc::strong_count(&body), 2);
        assert_eq!(
            make_array(decoded.columns[0].clone())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[7, 8]
        );
    }

    #[test]
    fn copies_without_an_owner() {
        let int = Kind::Int64;
        let frame = body(2, &[int_column(&[7, 8])]);
        let range = frame.as_ptr() as usize..frame.as_ptr() as usize + frame.len();

        let decoded = decode(&frame, None, &[&int], &mut Noop).expect("a frame");

        assert!(!inside(&range, &decoded.columns[0], 0));
    }

    #[test]
    fn list_offsets_are_shared_and_nodes_checked() {
        let list = Kind::List(Box::new(Field {
            name: "item".into(),
            kind: Kind::Int64,
            optional: false,
        }));
        let frame = body(
            2,
            &[Column {
                nodes: vec![(2, 0), (3, 0)],
                buffers: vec![None, raw(&u32s(&[0, 2, 3])), None, raw(&i64s(&[1, 2, 3]))],
            }],
        );
        let (body, shared, range) = owner(&frame);

        let decoded = decode(&body, Some(&shared), &[&list], &mut Noop).expect("a frame");

        assert!(
            inside(&range, &decoded.columns[0], 0),
            "list offsets point into the body"
        );
        let list = make_array(decoded.columns[0].clone());
        assert_eq!(list.as_any().downcast_ref::<ListArray>().unwrap().value_length(1), 1);

        let mut lying = frame.clone();
        lying[12 + 8..12 + 12].copy_from_slice(&5u32.to_le_bytes());

        assert!(matches!(
            decode(
                &lying,
                None,
                &[&Kind::List(Box::new(Field {
                    name: "item".into(),
                    kind: Kind::Int64,
                    optional: false
                }))],
                &mut Noop
            ),
            Err(Failure::Frame(Error::NodesDisagree { column: 0 }))
        ));
    }

    struct Reversing;

    impl Codec for Reversing {
        type Error = Infallible;

        fn decode(&mut self, stored: &[u8], _length: u64) -> Result<Vec<u8>, Infallible> {
            Ok(stored.iter().rev().copied().collect())
        }

        fn encode(&mut self, raw: &[u8]) -> Result<Vec<u8>, Infallible> {
            Ok(raw.iter().rev().copied().collect())
        }
    }

    #[test]
    fn a_prefixed_buffer_goes_through_the_codec() {
        let int = Kind::Int64;
        let mut reversed = i64s(&[1, 2]);
        reversed.reverse();
        let frame = body(
            2,
            &[Column {
                nodes: vec![(2, 0)],
                buffers: vec![None, Some((16, reversed.clone()))],
            }],
        );
        let (held, shared, range) = owner(&frame);

        let decoded = decode(&held, Some(&shared), &[&int], &mut Reversing).expect("a frame");

        assert!(!inside(&range, &decoded.columns[0], 0), "a codec's output is copied");
        assert_eq!(
            make_array(decoded.columns[0].clone())
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[1, 2]
        );

        let lying = body(
            2,
            &[Column {
                nodes: vec![(2, 0)],
                buffers: vec![None, Some((15, reversed))],
            }],
        );

        assert!(matches!(
            decode(&lying, None, &[&int], &mut Reversing),
            Err(Failure::Frame(Error::DecodedLength {
                buffer: 1,
                decoded: 16,
                prefix: 15
            }))
        ));
    }

    #[test]
    fn refuses() {
        let int = Kind::Int64;
        let frame = body(2, &[int_column(&[1, 2])]);
        let patched = |at: usize, value: u32| {
            let mut body = frame.clone();
            body[at..at + 4].copy_from_slice(&value.to_le_bytes());
            body
        };
        let refusal = |body: &[u8]| match decode(body, None, &[&int], &mut Noop) {
            Err(Failure::Frame(error)) => error,
            _ => panic!("a refusal"),
        };

        assert!(matches!(refusal(&frame[..11]), Error::DirectoryTruncated));
        assert!(matches!(refusal(&frame[..20]), Error::DirectoryTruncated));
        assert!(matches!(
            refusal(&patched(4, 3)),
            Error::FrameNodeCount { frame: 3, schema: 1 }
        ));
        assert!(matches!(
            refusal(&patched(8, 4)),
            Error::FrameBufferCount { frame: 4, schema: 2 }
        ));
        assert!(matches!(
            refusal(&patched(12, 3)),
            Error::ColumnRows {
                column: 0,
                rows: 3,
                frame_rows: 2
            }
        ));
        assert!(matches!(
            refusal(&patched(16, 3)),
            Error::ColumnNulls {
                column: 0,
                nulls: 3,
                rows: 2
            }
        ));
        // the values extent: offset word at 28, stored word at 32
        assert!(matches!(
            refusal(&patched(32, 200)),
            Error::BufferOutsideArea { buffer: 1, area: 24 }
        ));
        assert!(matches!(
            refusal(&patched(32, 4)),
            Error::BufferShorterThanPrefix { buffer: 1 }
        ));
        assert!(matches!(
            refusal(&patched(32, 10)),
            Error::MalformedColumn { column: 0, error } if matches!(*error, Error::ValuesLength { .. })
        ));
    }

    #[test]
    fn drops_the_owner_with_the_last_buffer() {
        let int = Kind::Int64;
        let frame = body(2, &[int_column(&[1, 2])]);
        let (body, shared, _) = owner(&frame);

        let decoded = decode(&body, Some(&shared), &[&int], &mut Noop).expect("a frame");
        assert_eq!(Arc::strong_count(&body), 3);

        drop(decoded);
        assert_eq!(Arc::strong_count(&body), 2);
    }

    /// Drops trailing zero bytes; decodes by padding back to the prefix's length. Records every buffer it encodes.
    struct TrailingZeros(Vec<usize>);

    impl Codec for TrailingZeros {
        type Error = Infallible;

        fn decode(&mut self, stored: &[u8], length: u64) -> Result<Vec<u8>, Infallible> {
            let mut decoded = stored.to_vec();
            decoded.resize(length as usize, 0);

            Ok(decoded)
        }

        fn encode(&mut self, raw: &[u8]) -> Result<Vec<u8>, Infallible> {
            self.0.push(raw.len());
            let kept = raw.iter().rposition(|byte| *byte != 0).map_or(0, |last| last + 1);

            Ok(raw[..kept].to_vec())
        }
    }

    fn list_of_int64() -> Kind {
        Kind::List(Box::new(Field {
            name: "item".into(),
            kind: Kind::Int64,
            optional: false,
        }))
    }

    /// `[[1, 2], [3]]` as the List kind's data type.
    fn list_data() -> ArrayData {
        ArrayData::builder(data_type(&list_of_int64()))
            .len(2)
            .add_buffer(Buffer::from_slice_ref([0i32, 2, 3]))
            .add_child_data(Int64Array::from(vec![1, 2, 3]).into_data())
            .build()
            .expect("a list")
    }

    #[test]
    fn encodes_what_decode_reads_back() {
        let int = Kind::Int64;
        let bytes = Kind::Bytes;
        let ints = Int64Array::from(vec![1, 2, 3]).into_data();
        let strings = LargeBinaryArray::from(vec![Some(&b"ab"[..]), None, Some(&b"cde"[..])]).into_data();

        let flat = encode(&[(&ints, &int), (&strings, &bytes)], 3, &mut Noop).expect("a frame");

        assert_eq!(
            flat,
            body(
                3,
                &[
                    int_column(&[1, 2, 3]),
                    Column {
                        nodes: vec![(3, 1)],
                        buffers: vec![raw(&[0b101]), raw(&u32s(&[0, 2, 2, 5])), raw(b"abcde")],
                    },
                ],
            )
        );
        let decoded = decode(&flat, None, &[&int, &bytes], &mut Noop).expect("a frame");
        assert_eq!(decoded.columns, vec![ints, strings]);

        let list = list_of_int64();
        let lists = list_data();
        let nested = encode(&[(&lists, &list)], 2, &mut Noop).expect("a frame");

        assert_eq!(
            nested,
            body(
                2,
                &[Column {
                    nodes: vec![(2, 0), (3, 0)],
                    buffers: vec![None, raw(&u32s(&[0, 2, 3])), None, raw(&i64s(&[1, 2, 3]))],
                }],
            )
        );
        assert_eq!(
            decode(&nested, None, &[&list], &mut Noop).expect("a frame").columns,
            vec![lists]
        );
    }

    #[test]
    fn a_shorter_codec_output_is_prefixed_with_the_raw_length() {
        let int = Kind::Int64;
        let ints = Int64Array::from(vec![1, 2]).into_data();
        let mut stripped = i64s(&[1, 2]);
        stripped.truncate(9);

        let frame = encode(&[(&ints, &int)], 2, &mut TrailingZeros(Vec::new())).expect("a frame");

        assert_eq!(
            frame,
            body(
                2,
                &[Column {
                    nodes: vec![(2, 0)],
                    buffers: vec![None, Some((16, stripped))],
                }],
            )
        );
        assert_eq!(
            decode(&frame, None, &[&int], &mut TrailingZeros(Vec::new()))
                .expect("a frame")
                .columns,
            vec![ints]
        );
    }

    #[test]
    fn an_empty_buffer_is_a_zero_extent() {
        let int = Kind::Int64;
        let ints = Int64Array::from(vec![7]).into_data();
        let mut codec = TrailingZeros(Vec::new());

        let frame = encode(&[(&ints, &int)], 1, &mut codec).expect("a frame");

        // counts, one node, then the validity extent
        assert_eq!(&frame[20..28], &[0u8; 8]);
        assert_eq!(codec.0, vec![8], "the codec never sees an empty buffer");
    }

    #[test]
    fn refuses_a_frame_past_u32() {
        assert_eq!(frame_length(16, [8usize].into_iter()).expect("a length"), 24);
        assert!(matches!(
            frame_length(16, [u32::MAX as usize].into_iter()),
            Err(Error::FrameTooLarge { bytes }) if bytes == 16 + u64::from(u32::MAX) + 1
        ));
    }
}
