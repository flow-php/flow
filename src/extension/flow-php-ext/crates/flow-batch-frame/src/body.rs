//! The Floe BATCH frame body (`FrameEncoder` / `FrameDecoder`), Noop codec only: every stored buffer is prefixed with
//! the i64 -1.
//!
//! ```text
//! V rowCount, V nodeCount, V bufferCount, nodeCount × (V length, V nullCount), bufferCount × (V offset, V stored),
//! zero padding to 8, buffer area: per non-empty buffer the i64 prefix and its bytes, each zero padded to 8
//! ```

use arrow_data::ArrayData;

use crate::column;
use crate::error::Error;
use crate::format::{write_u32, Reader};
use crate::kind::Kind;
use crate::layout::{buffer_count, node_count, nodes, null_count_of};

const RAW: i64 = -1;

pub fn encode_body(columns: &[(Kind, ArrayData)], row_count: u32) -> Result<Vec<u8>, Error> {
    let mut node_words = Vec::new();
    let mut extents = Vec::new();
    let mut area: Vec<u8> = Vec::new();
    let mut node_total: u32 = 0;
    let mut buffer_total: u32 = 0;

    for (index, (kind, data)) in columns.iter().enumerate() {
        if data.len() as u64 != row_count as u64 {
            return Err(Error::ColumnRows {
                column: index,
                rows: data.len() as u64,
                frame_rows: row_count as u64,
            });
        }

        let buffers = column::encode(data, kind)?;
        let buffers: Vec<&[u8]> = buffers.iter().map(Vec::as_slice).collect();
        let null_count = match kind {
            Kind::Null => row_count,
            _ => null_count_of(buffers[0], row_count)?,
        };
        let column_nodes = if node_count(kind) == 1 {
            vec![(row_count, null_count)]
        } else {
            nodes(kind, row_count, null_count, &buffers)?
        };

        for (length, nulls) in column_nodes {
            write_u32(&mut node_words, length);
            write_u32(&mut node_words, nulls);
            node_total += 1;
        }

        for buffer in buffers {
            buffer_total += 1;

            if buffer.is_empty() {
                write_u32(&mut extents, 0);
                write_u32(&mut extents, 0);

                continue;
            }

            write_u32(&mut extents, area.len() as u32);
            write_u32(&mut extents, (8 + buffer.len()) as u32);
            area.extend_from_slice(&RAW.to_le_bytes());
            area.extend_from_slice(buffer);
            pad8(&mut area);
        }
    }

    let mut body = Vec::with_capacity(12 + node_words.len() + extents.len() + 8 + area.len());
    write_u32(&mut body, row_count);
    write_u32(&mut body, node_total);
    write_u32(&mut body, buffer_total);
    body.extend_from_slice(&node_words);
    body.extend_from_slice(&extents);
    pad8(&mut body);
    body.extend_from_slice(&area);

    if body.len() as u64 > u32::MAX as u64 {
        return Err(Error::FrameTooLarge {
            bytes: body.len() as u64,
        });
    }

    Ok(body)
}

/// `FrameDecoder::decode()`'s checks in its order, columns by index. Returns the row count and one array per kind.
pub fn decode_body(body: &[u8], kinds: &[Kind]) -> Result<(u32, Vec<ArrayData>), Error> {
    let mut directory = Reader::new(body);
    let row_count = directory.u32()?;
    let node_total = directory.u32()?;
    let buffer_total = directory.u32()?;
    let expected_nodes: usize = kinds.iter().map(node_count).sum();
    let expected_buffers: usize = kinds.iter().map(buffer_count).sum();

    if node_total as u64 != expected_nodes as u64 {
        return Err(Error::FrameNodeCount {
            frame: node_total as u64,
            schema: expected_nodes as u64,
        });
    }

    if buffer_total as u64 != expected_buffers as u64 {
        return Err(Error::FrameBufferCount {
            frame: buffer_total as u64,
            schema: expected_buffers as u64,
        });
    }

    let words_len = 2 * (expected_nodes + expected_buffers);
    let area_start = (12 + 4 * words_len).div_ceil(8) * 8;

    if body.len() < area_start {
        return Err(Error::DirectoryTruncated);
    }

    let words: Vec<u32> = (0..words_len)
        .map(|_| directory.u32())
        .collect::<Result<_, _>>()?;
    let area = &body[area_start..];
    let mut node = 0;
    let mut extent = 2 * expected_nodes;
    let mut buffer = 0;
    let mut columns = Vec::with_capacity(kinds.len());

    for (index, kind) in kinds.iter().enumerate() {
        let length = words[2 * node];
        let null_count = words[2 * node + 1];

        if length != row_count {
            return Err(Error::ColumnRows {
                column: index,
                rows: length as u64,
                frame_rows: row_count as u64,
            });
        }

        if null_count > length {
            return Err(Error::ColumnNulls {
                column: index,
                nulls: null_count as u64,
                rows: length as u64,
            });
        }

        let mut buffers: Vec<&[u8]> = Vec::with_capacity(buffer_count(kind));

        for _ in 0..buffer_count(kind) {
            let offset = words[extent] as usize;
            let stored = words[extent + 1] as usize;

            if stored != 0 {
                if offset as u64 + stored as u64 > area.len() as u64 {
                    return Err(Error::BufferOutsideArea {
                        buffer,
                        area: area.len() as u64,
                    });
                }

                if stored < 8 {
                    return Err(Error::BufferShorterThanPrefix { buffer });
                }

                let prefix =
                    i64::from_le_bytes(area[offset..offset + 8].try_into().expect("8-byte slice"));

                if prefix != RAW {
                    return Err(Error::CompressedBuffer { buffer, prefix });
                }

                buffers.push(&area[offset + 8..offset + stored]);
            } else {
                buffers.push(&[]);
            }

            buffer += 1;
            extent += 2;
        }

        let malformed = |error: Error| Error::MalformedColumn {
            column: index,
            error: Box::new(error),
        };

        if node_count(kind) == 1 {
            node += 1;
        } else {
            for (derived_length, derived_nulls) in
                nodes(kind, length, null_count, &buffers).map_err(malformed)?
            {
                if words[2 * node] != derived_length || words[2 * node + 1] != derived_nulls {
                    return Err(Error::NodesDisagree { column: index });
                }

                node += 1;
            }
        }

        columns.push(column::decode(kind, &buffers, row_count, null_count).map_err(malformed)?);
    }

    Ok((row_count, columns))
}

fn pad8(bytes: &mut Vec<u8>) {
    bytes.resize(bytes.len().div_ceil(8) * 8, 0);
}
