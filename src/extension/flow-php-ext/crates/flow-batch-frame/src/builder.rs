//! Arrow storage for one column kind; a null row is a zeroed slot with its offset not advanced.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::Array;
use arrow_buffer::{BooleanBufferBuilder, Buffer, MutableBuffer, NullBufferBuilder};
use arrow_data::ArrayData;
use arrow_schema::ArrowError;

use crate::kind::{data_type, Kind};

/// List and map offsets are i32, string offsets i64.
pub struct OffsetOverflow(pub u64);

pub enum KindBuilder {
    Null {
        len: usize,
    },
    /// `values` is a `MutableBuffer`, not a `Vec<u8>`: arrow reads it as i64/f64/i32 words, which need their alignment.
    Fixed {
        width: usize,
        values: MutableBuffer,
        validity: NullBufferBuilder,
    },
    Boolean {
        values: BooleanBufferBuilder,
        validity: NullBufferBuilder,
    },
    Bytes {
        offsets: Vec<i64>,
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
                values: MutableBuffer::new(0),
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn append_null(&mut self) {
        match self {
            KindBuilder::Null { len } => *len += 1,
            KindBuilder::Fixed {
                width,
                values,
                validity,
            } => {
                values.resize(values.len() + *width, 0);
                validity.append_null();
            }
            KindBuilder::Boolean { values, validity } => {
                values.append(false);
                validity.append_null();
            }
            KindBuilder::Bytes { offsets, validity, .. } => {
                offsets.push(*offsets.last().expect("offsets start at 0"));
                validity.append_null();
            }
            KindBuilder::List { offsets, validity, .. } | KindBuilder::Map { offsets, validity, .. } => {
                offsets.push(*offsets.last().expect("offsets start at 0"));
                validity.append_null();
            }
            KindBuilder::Struct {
                len,
                validity,
                children,
            } => {
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

    pub fn append_i64(&mut self, value: i64) {
        self.append_fixed(&value.to_le_bytes());
    }

    pub fn append_f64(&mut self, value: f64) {
        self.append_fixed(&value.to_le_bytes());
    }

    pub fn append_i32(&mut self, value: i32) {
        self.append_fixed(&value.to_le_bytes());
    }

    pub fn append_bool(&mut self, value: bool) {
        let KindBuilder::Boolean { values, validity } = self else {
            unreachable!("append_bool on a non boolean kind");
        };

        values.append(value);
        validity.append_non_null();
    }

    pub fn append_bytes(&mut self, bytes: &[u8]) {
        let KindBuilder::Bytes {
            offsets,
            data,
            validity,
        } = self
        else {
            unreachable!("append_bytes on a non bytes kind");
        };

        data.extend_from_slice(bytes);
        offsets.push(data.len() as i64);
        validity.append_non_null();
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
            KindBuilder::List {
                offsets,
                validity,
                element,
            } => (offsets, validity, element.len()),
            KindBuilder::Map {
                offsets,
                validity,
                keys,
                ..
            } => (offsets, validity, keys.len()),
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

    /// Drops every row past the first `len`: a row refused half-appended leaves no trace.
    pub fn truncate(&mut self, len: usize) {
        match self {
            KindBuilder::Null { len: rows } => *rows = len,
            KindBuilder::Fixed {
                width,
                values,
                validity,
            } => {
                values.truncate(len * *width);
                validity.truncate(len);
            }
            KindBuilder::Boolean { values, validity } => {
                values.truncate(len);
                validity.truncate(len);
            }
            KindBuilder::Bytes {
                offsets,
                data,
                validity,
            } => {
                offsets.truncate(len + 1);
                data.truncate(offsets[len] as usize);
                validity.truncate(len);
            }
            KindBuilder::List {
                offsets,
                validity,
                element,
            } => {
                offsets.truncate(len + 1);
                element.truncate(offsets[len] as usize);
                validity.truncate(len);
            }
            KindBuilder::Map {
                offsets,
                validity,
                keys,
                values,
            } => {
                offsets.truncate(len + 1);
                keys.truncate(offsets[len] as usize);
                values.truncate(offsets[len] as usize);
                validity.truncate(len);
            }
            KindBuilder::Struct {
                len: rows,
                validity,
                children,
            } => {
                *rows = len;
                validity.truncate(len);

                for child in children {
                    child.truncate(len);
                }
            }
        }
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
            Kind::Timestamp => {
                self.append_fixed(&array.as_primitive::<TimestampMicrosecondType>().value(i).to_le_bytes())
            }
            Kind::Duration => {
                self.append_fixed(&array.as_primitive::<DurationMicrosecondType>().value(i).to_le_bytes())
            }
            Kind::Int32 => self.append_fixed(&array.as_primitive::<Date32Type>().value(i).to_le_bytes()),
            Kind::Float64 => self.append_fixed(&array.as_primitive::<Float64Type>().value(i).to_le_bytes()),
            Kind::Uuid => self.append_fixed(array.as_fixed_size_binary().value(i)),
            Kind::Boolean => self.append_bool(array.as_boolean().value(i)),
            Kind::Bytes => self.append_bytes(array.as_binary::<i64>().value(i)),
            Kind::List(element) => {
                let list = array.as_list::<i32>();
                let offsets = list.value_offsets();

                for j in offsets[i] as usize..offsets[i + 1] as usize {
                    self.list_element()
                        .append_from(list.values().as_ref(), &element.kind, j)?;
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

    /// Moves the rows out without a copy and keeps the capacity; built unchecked, re-validated under `arrow-validate`.
    pub fn take(&mut self, kind: &Kind) -> ArrayData {
        let builder = ArrayData::builder(data_type(kind)).len(self.len());

        let builder = match (self, kind) {
            (KindBuilder::Null { len }, _) => {
                *len = 0;

                builder
            }
            (KindBuilder::Fixed { values, validity, .. }, _) => {
                let taken = std::mem::replace(values, MutableBuffer::with_capacity(values.len()));

                builder.nulls(validity.finish()).add_buffer(Buffer::from(taken))
            }
            (KindBuilder::Boolean { values, validity }, _) => {
                let bits = values.finish();
                *values = BooleanBufferBuilder::new(bits.len());

                builder.nulls(validity.finish()).add_buffer(bits.into_inner())
            }
            (
                KindBuilder::Bytes {
                    offsets,
                    data,
                    validity,
                },
                _,
            ) => builder
                .nulls(validity.finish())
                .add_buffer(Buffer::from_vec(taken_offsets(offsets)))
                .add_buffer(Buffer::from_vec(taken(data))),
            (
                KindBuilder::List {
                    offsets,
                    validity,
                    element,
                },
                Kind::List(field),
            ) => builder
                .nulls(validity.finish())
                .add_buffer(Buffer::from_vec(taken_offsets(offsets)))
                .add_child_data(element.take(&field.kind)),
            (
                KindBuilder::Map {
                    offsets,
                    validity,
                    keys,
                    values,
                },
                Kind::Map(key, value),
            ) => {
                let arrow_schema::DataType::Map(entries, _) = data_type(kind) else {
                    unreachable!("a map kind has a map data type")
                };
                let entries = ArrayData::builder(entries.data_type().clone())
                    .len(keys.len())
                    .add_child_data(keys.take(&key.kind))
                    .add_child_data(values.take(&value.kind));

                builder
                    .nulls(validity.finish())
                    .add_buffer(Buffer::from_vec(taken_offsets(offsets)))
                    .add_child_data(unsafe { entries.build_unchecked() })
            }
            (
                KindBuilder::Struct {
                    len,
                    validity,
                    children,
                },
                Kind::Struct(fields),
            ) => {
                *len = 0;

                builder.nulls(validity.finish()).child_data(
                    children
                        .iter_mut()
                        .zip(fields)
                        .map(|(child, field)| child.take(&field.kind))
                        .collect(),
                )
            }
            _ => unreachable!("a builder is taken with the kind it was built for"),
        };

        unsafe { builder.build_unchecked() }
    }

    /// The rows appended so far; the builder keeps them and may be appended to again.
    pub fn finish(&self, kind: &Kind) -> Result<ArrayData, ArrowError> {
        let builder = ArrayData::builder(data_type(kind)).len(self.len());

        let builder = match (self, kind) {
            (KindBuilder::Null { .. }, _) => builder,
            (KindBuilder::Fixed { values, validity, .. }, _) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(values.as_slice())),
            (KindBuilder::Boolean { values, validity }, _) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(values.finish_cloned().into_inner()),
            (
                KindBuilder::Bytes {
                    offsets,
                    data,
                    validity,
                },
                _,
            ) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_buffer(Buffer::from_slice_ref(data)),
            (
                KindBuilder::List {
                    offsets,
                    validity,
                    element,
                },
                Kind::List(field),
            ) => builder
                .nulls(validity.finish_cloned())
                .add_buffer(Buffer::from_slice_ref(offsets))
                .add_child_data(element.finish(&field.kind)?),
            (
                KindBuilder::Map {
                    offsets,
                    validity,
                    keys,
                    values,
                },
                Kind::Map(key, value),
            ) => {
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
            (KindBuilder::Struct { validity, children, .. }, Kind::Struct(fields)) => {
                builder.nulls(validity.finish_cloned()).child_data(
                    children
                        .iter()
                        .zip(fields)
                        .map(|(child, field)| child.finish(&field.kind))
                        .collect::<Result<_, _>>()?,
                )
            }
            _ => unreachable!("a builder is finished with the kind it was built for"),
        };

        builder.build()
    }
}

/// `values` moved out, a vector of the same capacity left behind.
fn taken<T>(values: &mut Vec<T>) -> Vec<T> {
    std::mem::replace(values, Vec::with_capacity(values.len()))
}

/// `offsets` moved out, a vector of the same capacity holding the first offset `0` left behind.
fn taken_offsets<T: From<u8>>(offsets: &mut Vec<T>) -> Vec<T> {
    let mut next = Vec::with_capacity(offsets.len());
    next.push(T::from(0));

    std::mem::replace(offsets, next)
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Float64Type, Int64Type};
    use arrow_array::{make_array, Array, ArrayRef};

    use super::KindBuilder;
    use crate::kind::{Field, Kind};

    fn field(kind: Kind) -> Box<Field> {
        Box::new(Field {
            name: "item".into(),
            kind,
            optional: true,
        })
    }

    #[test]
    fn truncate_drops_the_rows_past_the_length_in_every_nested_builder() {
        let kind = Kind::Struct(vec![
            Field {
                name: "id".into(),
                kind: Kind::Int64,
                optional: true,
            },
            Field {
                name: "tags".into(),
                kind: Kind::List(field(Kind::Bytes)),
                optional: true,
            },
            Field {
                name: "flags".into(),
                kind: Kind::Map(field(Kind::Bytes), field(Kind::Boolean)),
                optional: true,
            },
        ]);
        let mut builder = KindBuilder::new(&kind);
        let mut row = |id: i64, tag: &[u8]| {
            let children = builder.struct_children();
            children[0].append_fixed(&id.to_le_bytes());
            children[1].list_element().append_bytes(tag);
            children[1].end_entries().ok();
            let (keys, values) = children[2].map_entries();
            keys.append_bytes(tag);
            values.append_bool(true);
            children[2].end_entries().ok();
            builder.end_struct();
        };

        row(1, b"kept");
        row(2, b"dropped");
        builder.truncate(1);
        let array = make_array(builder.finish(&kind).unwrap());
        let structure = array.as_struct();

        assert_eq!(array.len(), 1);
        assert_eq!(
            structure.column(0).as_primitive::<Int64Type>().values().to_vec(),
            vec![1]
        );
        assert_eq!(
            structure
                .column(1)
                .as_list::<i32>()
                .values()
                .as_binary::<i64>()
                .value(0),
            b"kept"
        );
        assert_eq!(structure.column(1).as_list::<i32>().values().len(), 1);
        assert_eq!(structure.column(2).as_map().keys().len(), 1);
    }

    fn nested() -> Kind {
        Kind::Struct(vec![
            Field {
                name: "id".into(),
                kind: Kind::Int64,
                optional: true,
            },
            Field {
                name: "ratio".into(),
                kind: Kind::Float64,
                optional: true,
            },
            Field {
                name: "tags".into(),
                kind: Kind::List(field(Kind::Bytes)),
                optional: true,
            },
            Field {
                name: "flags".into(),
                kind: Kind::Map(field(Kind::Bytes), field(Kind::Boolean)),
                optional: true,
            },
        ])
    }

    fn append_rows(builder: &mut KindBuilder, rows: &[(i64, &[u8])]) {
        for (id, tag) in rows {
            let children = builder.struct_children();
            children[0].append_i64(*id);
            children[1].append_f64(*id as f64 / 2.0);
            children[2].list_element().append_bytes(tag);
            children[2].end_entries().ok();
            let (keys, values) = children[3].map_entries();
            keys.append_bytes(tag);
            values.append_bool(*id % 2 == 0);
            children[3].end_entries().ok();
            builder.end_struct();
        }

        builder.append_null();
    }

    #[test]
    fn take_of_bytes_keeps_offsets_and_validity() {
        let mut builder = KindBuilder::new(&Kind::Bytes);
        builder.append_bytes(b"ab");
        builder.append_null();
        builder.append_bytes(b"");
        builder.append_bytes(b"cde");

        let array = make_array(builder.take(&Kind::Bytes));
        let strings = array.as_binary::<i64>();

        assert_eq!(strings.value_offsets(), &[0, 2, 2, 2, 5]);
        assert_eq!(strings.value(0), b"ab");
        assert!(strings.is_null(1));
        assert_eq!(strings.value(2), b"");
        assert_eq!(strings.value(3), b"cde");
        assert_eq!(array.null_count(), 1);
        assert!(builder.is_empty());
    }

    #[test]
    fn take_leaves_capacity_for_the_next_batch() {
        let kind = nested();
        let mut builder = KindBuilder::new(&kind);
        append_rows(&mut builder, &[(1, b"kept"), (2, b"also")]);

        let first = make_array(builder.take(&kind));

        assert_eq!(first.len(), 3);
        assert!(builder.is_empty());

        let KindBuilder::Struct { children, .. } = &builder else {
            unreachable!("a structure builder")
        };
        let KindBuilder::Fixed { values, .. } = &children[0] else {
            unreachable!("an int builder")
        };
        let KindBuilder::List { element, .. } = &children[2] else {
            unreachable!("a list builder")
        };
        let KindBuilder::Bytes { offsets, data, .. } = element.as_ref() else {
            unreachable!("a bytes builder")
        };

        assert!(values.capacity() >= 3 * 8);
        assert_eq!(offsets.as_slice(), &[0]);
        assert_eq!(offsets.capacity(), 3);
        assert_eq!(data.capacity(), 8);
    }

    #[test]
    fn take_twice_yields_two_independent_arrays() {
        let kind = nested();
        let mut builder = KindBuilder::new(&kind);
        append_rows(&mut builder, &[(1, b"first")]);
        let first = make_array(builder.take(&kind));
        append_rows(&mut builder, &[(2, b"second"), (3, b"third")]);
        let second = make_array(builder.take(&kind));

        let ids = |array: &ArrayRef| {
            array
                .as_struct()
                .column(0)
                .as_primitive::<Int64Type>()
                .iter()
                .collect::<Vec<_>>()
        };
        let tags = |array: &ArrayRef| {
            let list = array.as_struct().column(2).as_list::<i32>();

            (0..list.len())
                .filter(|row| !list.is_null(*row))
                .map(|row| list.value(row).as_binary::<i64>().value(0).to_vec())
                .collect::<Vec<_>>()
        };

        assert_eq!(ids(&first), vec![Some(1), None]);
        assert_eq!(ids(&second), vec![Some(2), Some(3), None]);
        assert_eq!(tags(&first), vec![b"first".to_vec()]);
        assert_eq!(tags(&second), vec![b"second".to_vec(), b"third".to_vec()]);
        assert_eq!(
            first.as_struct().column(1).as_primitive::<Float64Type>().values(),
            &[0.5, 0.0]
        );
        assert_eq!(
            second.as_struct().column(3).as_map().keys().as_binary::<i64>().value(1),
            b"third"
        );
    }
}
