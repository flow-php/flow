//! Arrow storage for one column kind, appended row by row and finished into the `ArrayData` whose data type is
//! `flow_batch_frame::kind::data_type(kind)`. A null row is stored the canonical way: zeroed slot, offset not advanced,
//! every structure child null.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::Array;
use arrow_buffer::{BooleanBufferBuilder, Buffer, NullBufferBuilder};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;
use flow_batch_frame::kind::{data_type, Kind};

/// Offsets are i32 (D5): the last offset a row would reach past `i32::MAX`.
pub struct OffsetOverflow(pub u64);

pub enum KindBuilder {
    Null {
        len: usize,
    },
    Fixed {
        width: usize,
        values: Vec<u8>,
        validity: NullBufferBuilder,
    },
    Boolean {
        values: BooleanBufferBuilder,
        validity: NullBufferBuilder,
    },
    Bytes {
        offsets: Vec<i32>,
        data: Vec<u8>,
        validity: NullBufferBuilder,
    },
    List {
        offsets: Vec<i32>,
        validity: NullBufferBuilder,
        element: Box<KindBuilder>,
    },
    Map {
        offsets: Vec<i32>,
        validity: NullBufferBuilder,
        keys: Box<KindBuilder>,
        values: Box<KindBuilder>,
    },
    Struct {
        len: usize,
        validity: NullBufferBuilder,
        children: Vec<KindBuilder>,
    },
}

fn width(kind: &Kind) -> usize {
    match kind {
        Kind::Int32 => 4,
        Kind::Uuid => 16,
        _ => 8,
    }
}

fn offset(len: usize) -> Result<i32, OffsetOverflow> {
    i32::try_from(len).map_err(|_| OffsetOverflow(len as u64))
}

impl KindBuilder {
    pub fn new(kind: &Kind) -> Self {
        match kind {
            Kind::Null => KindBuilder::Null { len: 0 },
            Kind::Boolean => KindBuilder::Boolean {
                values: BooleanBufferBuilder::new(0),
                validity: NullBufferBuilder::new(0),
            },
            Kind::Bytes => KindBuilder::Bytes {
                offsets: vec![0],
                data: Vec::new(),
                validity: NullBufferBuilder::new(0),
            },
            Kind::List(element) => KindBuilder::List {
                offsets: vec![0],
                validity: NullBufferBuilder::new(0),
                element: Box::new(KindBuilder::new(&element.kind)),
            },
            Kind::Map(key, value) => KindBuilder::Map {
                offsets: vec![0],
                validity: NullBufferBuilder::new(0),
                keys: Box::new(KindBuilder::new(&key.kind)),
                values: Box::new(KindBuilder::new(&value.kind)),
            },
            Kind::Struct(fields) => KindBuilder::Struct {
                len: 0,
                validity: NullBufferBuilder::new(0),
                children: fields.iter().map(|field| KindBuilder::new(&field.kind)).collect(),
            },
            _ => KindBuilder::Fixed {
                width: width(kind),
                values: Vec::new(),
                validity: NullBufferBuilder::new(0),
            },
        }
    }

    pub fn len(&self) -> usize {
        match self {
            KindBuilder::Null { len } | KindBuilder::Struct { len, .. } => *len,
            KindBuilder::Fixed { validity, .. }
            | KindBuilder::Boolean { validity, .. }
            | KindBuilder::Bytes { validity, .. }
            | KindBuilder::List { validity, .. }
            | KindBuilder::Map { validity, .. } => validity.len(),
        }
    }

    pub fn append_null(&mut self) {
        match self {
            KindBuilder::Null { len } => *len += 1,
            KindBuilder::Fixed { width, values, validity } => {
                values.resize(values.len() + *width, 0);
                validity.append_null();
            }
            KindBuilder::Boolean { values, validity } => {
                values.append(false);
                validity.append_null();
            }
            KindBuilder::Bytes { offsets, validity, .. }
            | KindBuilder::List { offsets, validity, .. }
            | KindBuilder::Map { offsets, validity, .. } => {
                offsets.push(*offsets.last().expect("offsets start at 0"));
                validity.append_null();
            }
            KindBuilder::Struct { len, validity, children } => {
                *len += 1;
                validity.append_null();

                for child in children {
                    child.append_null();
                }
            }
        }
    }

    /// A fixed-width value in its little-endian bytes (i64, i32 days, f64 bits, 16 uuid bytes).
    pub fn append_fixed(&mut self, bytes: &[u8]) {
        let KindBuilder::Fixed { values, validity, .. } = self else {
            unreachable!("append_fixed on a non fixed-width kind");
        };

        values.extend_from_slice(bytes);
        validity.append_non_null();
    }

    pub fn append_bool(&mut self, value: bool) {
        let KindBuilder::Boolean { values, validity } = self else {
            unreachable!("append_bool on a non boolean kind");
        };

        values.append(value);
        validity.append_non_null();
    }

    pub fn append_bytes(&mut self, bytes: &[u8]) -> Result<(), OffsetOverflow> {
        let KindBuilder::Bytes { offsets, data, validity } = self else {
            unreachable!("append_bytes on a non bytes kind");
        };

        let end = offset(data.len() + bytes.len())?;
        data.extend_from_slice(bytes);
        offsets.push(end);
        validity.append_non_null();

        Ok(())
    }

    /// The element builder of a list; call `end_entries` once the row's elements are appended.
    pub fn list_element(&mut self) -> &mut KindBuilder {
        let KindBuilder::List { element, .. } = self else {
            unreachable!("list_element on a non list kind");
        };

        element
    }

    pub fn map_entries(&mut self) -> (&mut KindBuilder, &mut KindBuilder) {
        let KindBuilder::Map { keys, values, .. } = self else {
            unreachable!("map_entries on a non map kind");
        };

        (keys, values)
    }

    /// Closes a valid list or map row over the elements appended since the previous row.
    pub fn end_entries(&mut self) -> Result<(), OffsetOverflow> {
        let (offsets, validity, len) = match self {
            KindBuilder::List { offsets, validity, element } => (offsets, validity, element.len()),
            KindBuilder::Map { offsets, validity, keys, .. } => (offsets, validity, keys.len()),
            _ => unreachable!("end_entries on a non list/map kind"),
        };

        offsets.push(offset(len)?);
        validity.append_non_null();

        Ok(())
    }

    pub fn struct_children(&mut self) -> &mut [KindBuilder] {
        let KindBuilder::Struct { children, .. } = self else {
            unreachable!("struct_children on a non structure kind");
        };

        children
    }

    /// Closes a valid structure row whose children were each appended once.
    pub fn end_struct(&mut self) {
        let KindBuilder::Struct { len, validity, .. } = self else {
            unreachable!("end_struct on a non structure kind");
        };

        *len += 1;
        validity.append_non_null();
    }

    /// Row `i` of `array`, whose data type is `data_type(kind)`.
    pub fn append_from(&mut self, array: &dyn Array, kind: &Kind, i: usize) -> Result<(), OffsetOverflow> {
        if matches!(kind, Kind::Null) || array.is_null(i) {
            self.append_null();

            return Ok(());
        }

        match kind {
            Kind::Null => unreachable!("appended above"),
            Kind::Int64 => self.append_fixed(&array.as_primitive::<Int64Type>().value(i).to_le_bytes()),
            Kind::Timestamp => self.append_fixed(&array.as_primitive::<TimestampMicrosecondType>().value(i).to_le_bytes()),
            Kind::Duration => self.append_fixed(&array.as_primitive::<DurationMicrosecondType>().value(i).to_le_bytes()),
            Kind::Int32 => self.append_fixed(&array.as_primitive::<Date32Type>().value(i).to_le_bytes()),
            Kind::Float64 => self.append_fixed(&array.as_primitive::<Float64Type>().value(i).to_le_bytes()),
            Kind::Uuid => self.append_fixed(array.as_fixed_size_binary().value(i)),
            Kind::Boolean => self.append_bool(array.as_boolean().value(i)),
            Kind::Bytes => self.append_bytes(array.as_binary::<i32>().value(i))?,
            Kind::List(element) => {
                let list = array.as_list::<i32>();
                let offsets = list.value_offsets();

                for j in offsets[i] as usize..offsets[i + 1] as usize {
                    self.list_element().append_from(list.values().as_ref(), &element.kind, j)?;
                }

                self.end_entries()?;
            }
            Kind::Map(key, value) => {
                let map = array.as_map();
                let offsets = map.value_offsets();

                for j in offsets[i] as usize..offsets[i + 1] as usize {
                    let (keys, values) = self.map_entries();
                    keys.append_from(map.keys().as_ref(), &key.kind, j)?;
                    values.append_from(map.values().as_ref(), &value.kind, j)?;
                }

                self.end_entries()?;
            }
            Kind::Struct(fields) => {
                let structure = array.as_struct();

                for ((child, field), column) in self.struct_children().iter_mut().zip(fields).zip(structure.columns()) {
                    child.append_from(column.as_ref(), &field.kind, i)?;
                }

                self.end_struct();
            }
        }

        Ok(())
    }

    /// The rows appended so far; the builder keeps them and may be appended to again.
    pub fn finish(&self, kind: &Kind) -> Result<ArrayData, ArrowError> {
        let builder = ArrayData::builder(data_type(kind)).len(self.len());

        let builder = match (self, kind) {
            (KindBuilder::Null { .. }, _) => builder,
            (KindBuilder::Fixed { values, validity, .. }, _) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(values)),
            (KindBuilder::Boolean { values, validity }, _) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(values.finish_cloned().into_inner()),
            (KindBuilder::Bytes { offsets, data, validity }, _) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_buffer(Buffer::from_slice_ref(data)),
            (KindBuilder::List { offsets, validity, element }, Kind::List(field)) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_child_data(element.finish(&field.kind)?),
            (KindBuilder::Map { offsets, validity, keys, values }, Kind::Map(key, value)) => {
                let arrow_schema::DataType::Map(entries, _) = data_type(kind) else {
                    unreachable!("a map kind has a map data type")
                };
                let entries = ArrayData::builder(entries.data_type().clone())
                    .len(keys.len())
                    .add_child_data(keys.finish(&key.kind)?)
                    .add_child_data(values.finish(&value.kind)?)
                    .build()?;

                builder
                    .nulls(validity.finish_cloned())
                    .add_buffer(Buffer::from_slice_ref(offsets))
                    .add_child_data(entries)
            }
            (KindBuilder::Struct { validity, children, .. }, Kind::Struct(fields)) => builder
                .nulls(validity.finish_cloned())
                .child_data(
                    children
                        .iter()
                        .zip(fields)
                        .map(|(child, field)| child.finish(&field.kind))
                        .collect::<Result<_, _>>()?,
                ),
            _ => unreachable!("a builder is finished with the kind it was built for"),
        };

        builder.build()
    }
}
