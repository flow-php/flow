//! The one node/buffer layout of a column (`Flow\ETL\Column\BufferLayout`): the pre-order nodes and buffers a kind
//! spans, and the child nodes derived from a column's buffers.

use crate::error::{Error, Of};
use crate::kind::Kind;

pub fn node_count(kind: &Kind) -> usize {
    match kind {
        Kind::List(element) => 1 + node_count(&element.kind),
        Kind::Map(key, value) => 2 + node_count(&key.kind) + node_count(&value.kind),
        Kind::Struct(fields) => {
            1 + fields
                .iter()
                .map(|field| node_count(&field.kind))
                .sum::<usize>()
        }
        _ => 1,
    }
}

pub fn buffer_count(kind: &Kind) -> usize {
    match kind {
        Kind::Null => 0,
        Kind::List(element) => 2 + buffer_count(&element.kind),
        Kind::Map(key, value) => 3 + buffer_count(&key.kind) + buffer_count(&value.kind),
        Kind::Struct(fields) => {
            1 + fields
                .iter()
                .map(|field| buffer_count(&field.kind))
                .sum::<usize>()
        }
        Kind::Bytes => 3,
        _ => 2,
    }
}

/// Pre-order `(length, null count)` of one column's tree: the first node as given, every child derived from the
/// buffers - a null kind is `(length, length)`, list and map children span the last offset, a map entries node holds
/// no nulls and structure children span their parent.
pub fn nodes(
    kind: &Kind,
    len: u32,
    null_count: u32,
    buffers: &[&[u8]],
) -> Result<Vec<(u32, u32)>, Error> {
    nodes_at(kind, 0, len, null_count, buffers)
}

pub(crate) fn nodes_at(
    kind: &Kind,
    node: usize,
    len: u32,
    null_count: u32,
    buffers: &[&[u8]],
) -> Result<Vec<(u32, u32)>, Error> {
    let expected = buffer_count(kind);

    if buffers.len() != expected {
        return Err(Error::BufferCount {
            node,
            buffers: buffers.len(),
            expected,
        });
    }

    let mut nodes = vec![(len, null_count)];
    let mut children: Vec<(&Kind, u32)> = Vec::new();
    let mut position = 1;

    match kind {
        Kind::List(element) => {
            children.push((&element.kind, last_offset(buffers[1], len, Of::List)?));
            position = 2;
        }
        Kind::Map(key, value) => {
            let child_len = last_offset(buffers[1], len, Of::Map)?;

            if !buffers[2].is_empty() {
                return Err(Error::MapEntriesValidity {
                    bytes: buffers[2].len() as u64,
                });
            }

            nodes.push((child_len, 0));
            children.push((&key.kind, child_len));
            children.push((&value.kind, child_len));
            position = 3;
        }
        Kind::Struct(fields) => {
            children.extend(fields.iter().map(|field| (&field.kind, len)));
        }
        _ => {}
    }

    for (child, child_len) in children {
        let child_buffers = &buffers[position..position + buffer_count(child)];
        position += child_buffers.len();
        let child_nulls = match child {
            Kind::Null => child_len,
            _ => null_count_of(child_buffers[0], child_len)?,
        };

        nodes.extend(nodes_at(
            child,
            node + nodes.len(),
            child_len,
            child_nulls,
            child_buffers,
        )?);
    }

    Ok(nodes)
}

/// `Validity::nullCount()`: '' holds no nulls; a bitmap may be longer than its rows.
pub fn null_count_of(bitmap: &[u8], len: u32) -> Result<u32, Error> {
    if bitmap.is_empty() {
        return Ok(0);
    }

    let needed = (len as usize).div_ceil(8);

    if bitmap.len() < needed {
        return Err(Error::ValidityTooShort {
            bytes: bitmap.len() as u64,
            rows: len as u64,
        });
    }

    let full = len as usize / 8;
    let mut valid: u32 = bitmap[..full].iter().map(|byte| byte.count_ones()).sum();
    let tail = len % 8;

    if tail != 0 {
        valid += (bitmap[full] & ((1u8 << tail) - 1)).count_ones();
    }

    Ok(len - valid)
}

fn last_offset(offsets: &[u8], len: u32, of: Of) -> Result<u32, Error> {
    let expected = (len as u64 + 1) * 4;

    if offsets.len() as u64 != expected {
        return Err(Error::OffsetsLength {
            of,
            bytes: offsets.len() as u64,
            expected,
            rows: len as u64,
        });
    }

    let at = len as usize * 4;

    Ok(u32::from_le_bytes(
        offsets[at..at + 4].try_into().expect("4-byte slice"),
    ))
}
