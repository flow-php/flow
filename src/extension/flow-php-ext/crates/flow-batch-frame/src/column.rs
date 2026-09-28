//! One column's buffers in the canonical form of `Column::encode()`, and back (`Backend::decode()`).
//!
//! Canonical: validity '' when the rows hold no null, else packed LSB-first from bit 0 with zero padding; a null slot
//! zeroed (0, 0.0, bit 0, 16 zero bytes, offset not advanced, structure children null under a null parent); offsets
//! from 0 over exactly the rows' bytes and children, whatever slice or selection the array is.

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType,
};
use arrow_array::{make_array, Array, ArrowPrimitiveType};
use arrow_buffer::{Buffer, ToByteSlice};
use arrow_data::ArrayData;
use arrow_schema::DataType;

use crate::error::{Error, Of, Role, Values};
use crate::kind::{data_type, Kind};
use crate::layout::{buffer_count, node_count, nodes_at, null_count_of};

pub fn encode(data: &ArrayData, kind: &Kind) -> Result<Vec<Vec<u8>>, Error> {
    let expected = data_type(kind);

    if data.data_type() != &expected {
        return Err(Error::DataTypeMismatch {
            expected,
            actual: data.data_type().clone(),
        });
    }

    let array = make_array(data.clone());
    let mut out = Vec::with_capacity(buffer_count(kind));
    encode_node(
        array.as_ref(),
        kind,
        &Rows::Range {
            start: 0,
            len: array.len(),
        },
        None,
        &mut out,
    );

    Ok(out)
}

/// `ColumnDecoder::decode()` + `PhpBackend::decode()`: the same refusals in the same order. The result's data type is
/// `data_type(kind)`. A top-level null under NOT NULL is not refused here (`Rows::fromColumns` does).
pub fn decode(
    kind: &Kind,
    buffers: &[&[u8]],
    len: u32,
    null_count: u32,
) -> Result<ArrayData, Error> {
    let mut cursor = Cursor::new(buffers);
    let data = decode_node(kind, 0, &mut cursor, len, null_count)?;

    if cursor.remaining() != 0 {
        return Err(Error::LeftoverBuffers {
            count: cursor.remaining(),
        });
    }

    Ok(data)
}

/// The rows of an array a node encodes, as logical indexes: the whole array or a slice, or the compacted children of
/// the valid list/map slots.
enum Rows {
    Range { start: usize, len: usize },
    Indices(Vec<usize>),
}

impl Rows {
    fn len(&self) -> usize {
        match self {
            Rows::Range { len, .. } => *len,
            Rows::Indices(indices) => indices.len(),
        }
    }

    fn at(&self, j: usize) -> usize {
        match self {
            Rows::Range { start, .. } => start + j,
            Rows::Indices(indices) => indices[j],
        }
    }
}

fn encode_node(
    array: &dyn Array,
    kind: &Kind,
    rows: &Rows,
    parent: Option<&[bool]>,
    out: &mut Vec<Vec<u8>>,
) {
    if let Kind::Null = kind {
        return;
    }

    let valid = effective_validity(array, rows, parent);
    out.push(valid.as_deref().map_or_else(Vec::new, pack_bits));
    let valid = valid.as_deref();

    match kind {
        Kind::Null => unreachable!("returned above"),
        Kind::Int64 => out.push(fixed::<Int64Type>(array, rows, valid)),
        Kind::Timestamp => out.push(fixed::<TimestampMicrosecondType>(array, rows, valid)),
        Kind::Duration => out.push(fixed::<DurationMicrosecondType>(array, rows, valid)),
        Kind::Int32 => out.push(fixed::<Date32Type>(array, rows, valid)),
        Kind::Float64 => out.push(fixed::<Float64Type>(array, rows, valid)),
        Kind::Boolean => {
            let values = array.as_boolean().values();
            let mut bits = vec![0u8; rows.len().div_ceil(8)];

            for j in 0..rows.len() {
                if is_valid(valid, j) && values.value(rows.at(j)) {
                    bits[j >> 3] |= 1 << (j & 7);
                }
            }

            out.push(bits);
        }
        Kind::Uuid => {
            let values = array.as_fixed_size_binary();
            let mut bytes = Vec::with_capacity(rows.len() * 16);

            for j in 0..rows.len() {
                if is_valid(valid, j) {
                    bytes.extend_from_slice(values.value(rows.at(j)));
                } else {
                    bytes.extend_from_slice(&[0; 16]);
                }
            }

            out.push(bytes);
        }
        Kind::Bytes => {
            let values = array.as_binary::<i32>();
            let offsets = values.value_offsets();
            let data = values.value_data();

            if let (Rows::Range { start, len }, None) = (rows, valid) {
                out.push(rebased(&offsets[*start..=start + len]));
                out.push(data[offsets[*start] as usize..offsets[start + len] as usize].to_vec());
            } else {
                let mut packed = Vec::with_capacity((rows.len() + 1) * 4);
                let mut bytes = Vec::new();
                packed.extend_from_slice(&0u32.to_le_bytes());

                for j in 0..rows.len() {
                    if is_valid(valid, j) {
                        let i = rows.at(j);
                        bytes
                            .extend_from_slice(&data[offsets[i] as usize..offsets[i + 1] as usize]);
                    }

                    packed.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                }

                out.push(packed);
                out.push(bytes);
            }
        }
        Kind::List(element) => {
            let list = array.as_list::<i32>();
            let (offsets, children) = compact(list.value_offsets(), rows, valid);
            out.push(offsets);
            encode_node(list.values().as_ref(), &element.kind, &children, None, out);
        }
        Kind::Map(key, value) => {
            let map = array.as_map();
            let (offsets, children) = compact(map.value_offsets(), rows, valid);
            out.push(offsets);
            out.push(Vec::new());
            encode_node(map.keys().as_ref(), &key.kind, &children, None, out);
            encode_node(map.values().as_ref(), &value.kind, &children, None, out);
        }
        Kind::Struct(fields) => {
            let structure = array.as_struct();

            for (i, field) in fields.iter().enumerate() {
                encode_node(structure.column(i).as_ref(), &field.kind, rows, valid, out);
            }
        }
    }
}

/// The null of a row is its own or its parent structure's; `None` when every row is valid.
fn effective_validity(
    array: &dyn Array,
    rows: &Rows,
    parent: Option<&[bool]>,
) -> Option<Vec<bool>> {
    let nulls = array.nulls();

    if nulls.is_none() && parent.is_none() {
        return None;
    }

    let valid: Vec<bool> = (0..rows.len())
        .map(|j| parent.is_none_or(|p| p[j]) && nulls.is_none_or(|n| n.is_valid(rows.at(j))))
        .collect();

    if valid.iter().all(|v| *v) {
        None
    } else {
        Some(valid)
    }
}

fn is_valid(valid: Option<&[bool]>, j: usize) -> bool {
    valid.is_none_or(|v| v[j])
}

fn pack_bits(valid: &[bool]) -> Vec<u8> {
    let mut bits = vec![0u8; valid.len().div_ceil(8)];

    for (j, _) in valid.iter().enumerate().filter(|(_, v)| **v) {
        bits[j >> 3] |= 1 << (j & 7);
    }

    bits
}

fn fixed<T: ArrowPrimitiveType>(array: &dyn Array, rows: &Rows, valid: Option<&[bool]>) -> Vec<u8> {
    let values = array.as_primitive::<T>().values();

    if let (Rows::Range { start, len }, None) = (rows, valid) {
        return values[*start..start + len].to_byte_slice().to_vec();
    }

    let width = std::mem::size_of::<T::Native>();
    let mut bytes = Vec::with_capacity(rows.len() * width);

    for j in 0..rows.len() {
        if is_valid(valid, j) {
            bytes.extend_from_slice(values[rows.at(j)].to_byte_slice());
        } else {
            bytes.resize(bytes.len() + width, 0);
        }
    }

    bytes
}

/// Offsets from 0 over the valid rows' ranges (a null row's range is dropped), and the child rows those ranges select.
/// Compaction only shrinks an i32 array's ranges, so the offsets stay within i32.
fn compact(offsets: &[i32], rows: &Rows, valid: Option<&[bool]>) -> (Vec<u8>, Rows) {
    if let (Rows::Range { start, len }, None) = (rows, valid) {
        return (
            rebased(&offsets[*start..=start + len]),
            Rows::Range {
                start: offsets[*start] as usize,
                len: (offsets[start + len] - offsets[*start]) as usize,
            },
        );
    }

    let mut packed = Vec::with_capacity((rows.len() + 1) * 4);
    let mut children = Vec::new();
    packed.extend_from_slice(&0u32.to_le_bytes());

    for j in 0..rows.len() {
        if is_valid(valid, j) {
            let i = rows.at(j);
            children.extend(offsets[i] as usize..offsets[i + 1] as usize);
        }

        packed.extend_from_slice(&(children.len() as u32).to_le_bytes());
    }

    (packed, Rows::Indices(children))
}

fn rebased(offsets: &[i32]) -> Vec<u8> {
    offsets
        .iter()
        .flat_map(|offset| ((offset - offsets[0]) as u32).to_le_bytes())
        .collect()
}

struct Cursor<'b, 'a> {
    buffers: &'b [&'a [u8]],
    position: usize,
}

impl<'b, 'a> Cursor<'b, 'a> {
    fn new(buffers: &'b [&'a [u8]]) -> Self {
        Self {
            buffers,
            position: 0,
        }
    }

    fn next(&mut self) -> Result<&'a [u8], Error> {
        let buffer = self
            .buffers
            .get(self.position)
            .ok_or(Error::BuffersExhausted)?;
        self.position += 1;

        Ok(buffer)
    }

    fn remaining(&self) -> usize {
        self.buffers.len() - self.position
    }
}

fn decode_node(
    kind: &Kind,
    node: usize,
    cursor: &mut Cursor<'_, '_>,
    len: u32,
    null_count: u32,
) -> Result<ArrayData, Error> {
    if let Kind::List(_) | Kind::Map(_, _) | Kind::Struct(_) = kind {
        let mut subtree = Vec::with_capacity(buffer_count(kind));

        for _ in 0..buffer_count(kind) {
            subtree.push(cursor.next()?);
        }

        let nodes = nodes_at(kind, node, len, null_count, &subtree)?;

        return decode_nested(
            kind,
            node,
            &mut Cursor::new(&subtree),
            len,
            null_count,
            &nodes,
        );
    }

    if let Kind::Null = kind {
        if null_count != len {
            return Err(Error::NullKindNullCount {
                rows: len as u64,
                null_count: null_count as u64,
            });
        }

        return Ok(ArrayData::new_null(&DataType::Null, len as usize));
    }

    let nulls = validity(cursor.next()?, len, null_count)?;
    let builder = ArrayData::builder(data_type(kind))
        .len(len as usize)
        .null_bit_buffer(nulls);
    let builder = match kind {
        Kind::Int64 | Kind::Timestamp | Kind::Duration => {
            builder.add_buffer(fixed_values(cursor.next()?, 8, len, Values::Int64)?)
        }
        Kind::Int32 => builder.add_buffer(fixed_values(cursor.next()?, 4, len, Values::Int32)?),
        Kind::Float64 => builder.add_buffer(fixed_values(cursor.next()?, 8, len, Values::Float64)?),
        Kind::Uuid => builder.add_buffer(fixed_values(
            cursor.next()?,
            16,
            len,
            Values::FixedBinary16,
        )?),
        Kind::Boolean => {
            let buffer = cursor.next()?;
            let expected = (len as u64).div_ceil(8);

            if (buffer.len() as u64) < expected {
                return Err(Error::ValuesLength {
                    values: Values::Boolean,
                    bytes: buffer.len() as u64,
                    expected,
                    rows: len as u64,
                });
            }

            builder.add_buffer(Buffer::from_slice_ref(buffer))
        }
        Kind::Bytes => {
            let offsets = cursor.next()?;
            let last = check_offsets(offsets, len, Of::Utf8)?;
            let data = cursor.next()?;

            if last as u64 != data.len() as u64 {
                return Err(Error::Utf8DataLength {
                    bytes: data.len() as u64,
                    last_offset: last as u64,
                });
            }

            fits_i32(last)?;

            builder
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_buffer(Buffer::from_slice_ref(data))
        }
        Kind::Null | Kind::List(_) | Kind::Map(_, _) | Kind::Struct(_) => {
            unreachable!("returned above")
        }
    };

    builder.build().map_err(Error::Arrow)
}

fn decode_nested(
    kind: &Kind,
    node: usize,
    own: &mut Cursor<'_, '_>,
    len: u32,
    null_count: u32,
    nodes: &[(u32, u32)],
) -> Result<ArrayData, Error> {
    let nulls = validity(own.next()?, len, null_count)?;
    let builder = ArrayData::builder(data_type(kind))
        .len(len as usize)
        .null_bit_buffer(nulls);

    let builder = match kind {
        Kind::List(element) => {
            let offsets = own.next()?;
            let last = check_offsets(offsets, len, Of::List)?;
            let (length, nulls) = nodes[1];

            if nulls > 0 && !element.optional && element.kind != Kind::Null {
                return Err(Error::NestedNulls {
                    node: node + 1,
                    role: Role::ListElement,
                    nulls: nulls as u64,
                });
            }

            let child = decode_node(&element.kind, node + 1, own, length, nulls)?;
            fits_i32(last)?;

            builder
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_child_data(child)
        }
        Kind::Map(key, value) => {
            let offsets = own.next()?;
            let last = check_offsets(offsets, len, Of::Map)?;
            own.next()?;
            let key_node = node + 2;
            let value_node = key_node + node_count(&key.kind);
            let (length, key_nulls) = nodes[key_node - node];
            let (_, value_nulls) = nodes[value_node - node];

            if key_nulls > 0 {
                return Err(Error::NestedNulls {
                    node: key_node,
                    role: Role::MapKey,
                    nulls: key_nulls as u64,
                });
            }

            if value_nulls > 0 && !value.optional && value.kind != Kind::Null {
                return Err(Error::NestedNulls {
                    node: value_node,
                    role: Role::MapValue,
                    nulls: value_nulls as u64,
                });
            }

            let keys = decode_node(&key.kind, key_node, own, length, key_nulls)?;
            let values = decode_node(&value.kind, value_node, own, length, value_nulls)?;
            fits_i32(last)?;
            let DataType::Map(entries, _) = data_type(kind) else {
                unreachable!("a map kind has a map data type")
            };
            let entries = ArrayData::builder(entries.data_type().clone())
                .len(length as usize)
                .add_child_data(keys)
                .add_child_data(values)
                .build()
                .map_err(Error::Arrow)?;

            builder
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_child_data(entries)
        }
        Kind::Struct(fields) => {
            let mut child_node = node + 1;
            let mut children = Vec::with_capacity(fields.len());

            for field in fields {
                let (length, nulls) = nodes[child_node - node];
                children.push(decode_node(&field.kind, child_node, own, length, nulls)?);
                child_node += node_count(&field.kind);
            }

            builder.child_data(children)
        }
        _ => unreachable!("decode_nested takes a nested kind"),
    };

    builder.build().map_err(Error::Arrow)
}

/// `Some` bitmap only when it holds a null: the canonical form omits it otherwise.
fn validity(bitmap: &[u8], len: u32, declared: u32) -> Result<Option<Buffer>, Error> {
    let derived = null_count_of(bitmap, len)?;

    if declared != derived {
        return Err(Error::NullCountMismatch {
            declared: declared as u64,
            derived: derived as u64,
            rows: len as u64,
        });
    }

    Ok((derived > 0).then(|| Buffer::from_slice_ref(bitmap)))
}

fn fixed_values(buffer: &[u8], width: u64, len: u32, values: Values) -> Result<Buffer, Error> {
    let expected = width * len as u64;

    if buffer.len() as u64 != expected {
        return Err(Error::ValuesLength {
            values,
            bytes: buffer.len() as u64,
            expected,
            rows: len as u64,
        });
    }

    Ok(Buffer::from_slice_ref(buffer))
}

/// `Offsets::unpack()`: exactly (len + 1) * 4 bytes, from 0, monotonic. Returns the last offset.
fn check_offsets(buffer: &[u8], len: u32, of: Of) -> Result<u32, Error> {
    let expected = (len as u64 + 1) * 4;

    if buffer.len() as u64 != expected {
        return Err(Error::OffsetsLength {
            of,
            bytes: buffer.len() as u64,
            expected,
            rows: len as u64,
        });
    }

    let offset =
        |i: usize| u32::from_le_bytes(buffer[i * 4..i * 4 + 4].try_into().expect("4-byte slice"));

    if offset(0) != 0 {
        return Err(Error::OffsetsStart {
            of,
            first: offset(0) as u64,
        });
    }

    for i in 1..=len as usize {
        if offset(i) < offset(i - 1) {
            return Err(Error::OffsetsNotMonotonic {
                of,
                index: i as u64,
                previous: offset(i - 1) as u64,
                current: offset(i) as u64,
            });
        }
    }

    Ok(offset(len as usize))
}

/// Native offsets are i32 (D5): the PHP decoder takes them unsigned, so a last offset past i32::MAX is refused here.
fn fits_i32(last: u32) -> Result<(), Error> {
    if last > i32::MAX as u32 {
        return Err(Error::OffsetOverflow { last: last as u64 });
    }

    Ok(())
}
