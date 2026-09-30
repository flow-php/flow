//! Thrift compact-protocol bytes as the PHP objects of the classes the Thrift compiler generates: each class's static
//! `$_TSPEC` says which field id is which property and of which type, as the generated `read()` does. No Parquet
//! knowledge here.

use std::collections::HashMap;
use std::rc::Rc;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ArrayKey, ZendHashTable, ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;

use crate::ctx::{self, find_class, property_offset, zval_long, zval_str};
use crate::exception::ext_exception;

/// Nesting deeper than this is refused instead of recursing further.
const DEPTH: usize = 64;

// the compact protocol's type ids (field headers, list headers)
const BOOLEAN_TRUE: u8 = 1;
const BOOLEAN_FALSE: u8 = 2;
const BYTE: u8 = 3;
const I16: u8 = 4;
const I32: u8 = 5;
const I64: u8 = 6;
const DOUBLE: u8 = 7;
const BINARY: u8 = 8;
const LIST: u8 = 9;
const SET: u8 = 10;
const MAP: u8 = 11;
const STRUCT: u8 = 12;

/// A `$_TSPEC` field type; a struct is the index of its class in `Specs`.
enum Value {
    Bool,
    Byte,
    I16,
    I32,
    I64,
    Double,
    String,
    Struct(usize),
    List(Box<Value>),
}

impl Value {
    /// Whether a field of this type arrives as compact type `wire`; any other is skipped, as the generated `read()`.
    fn arrives_as(&self, wire: u8) -> bool {
        match self {
            Value::Bool => wire == BOOLEAN_TRUE || wire == BOOLEAN_FALSE,
            Value::Byte => wire == BYTE,
            Value::I16 => wire == I16,
            Value::I32 => wire == I32,
            Value::I64 => wire == I64,
            Value::Double => wire == DOUBLE,
            Value::String => wire == BINARY,
            Value::Struct(_) => wire == STRUCT,
            Value::List(_) => wire == LIST,
        }
    }
}

struct Class {
    ce: &'static ClassEntry,
    /// By field id: the property's slot offset and its type.
    fields: Vec<Option<(u32, Value)>>,
}

/// Every class reachable from a root class, by index.
pub struct Specs {
    classes: Vec<Class>,
}

impl Specs {
    /// The root class and every class its `$_TSPEC` reaches, root at index 0.
    fn build(root: &str) -> Result<Self, PhpException> {
        let mut specs = Self { classes: Vec::new() };
        specs.class(root, &mut HashMap::new())?;

        Ok(specs)
    }

    fn class(&mut self, name: &str, seen: &mut HashMap<String, usize>) -> Result<usize, PhpException> {
        let name = name.trim_start_matches('\\');

        if let Some(index) = seen.get(name) {
            return Ok(*index);
        }

        let ce = find_class(name)?;
        let index = self.classes.len();
        seen.insert(name.to_string(), index);
        self.classes.push(Class { ce, fields: Vec::new() });

        let spec = ce
            .get_static_property::<&ZendHashTable>("_TSPEC")
            .ok_or_else(|| ext_exception(format!("flow_php requires {name}::$_TSPEC to be an array")))?;
        let mut fields: Vec<Option<(u32, Value)>> = Vec::new();

        for (key, entry) in spec.iter() {
            let ArrayKey::Long(id) = key else {
                return Err(ext_exception(format!("flow_php requires {name}::$_TSPEC to be keyed by field id")));
            };
            let entry = entry
                .array()
                .ok_or_else(|| ext_exception(format!("flow_php requires {name}::$_TSPEC[{id}] to be an array")))?;
            let var = entry
                .get("var")
                .and_then(Zval::str)
                .ok_or_else(|| ext_exception(format!("flow_php requires {name}::$_TSPEC[{id}]['var']")))?;
            let offset = property_offset(ce, var)?;
            let value = self.value(entry, name, seen)?;
            let id = usize::try_from(id)
                .ok()
                .filter(|id| *id <= i16::MAX as usize)
                .ok_or_else(|| ext_exception(format!("flow_php requires {name}::$_TSPEC field ids in 0..=32767")))?;

            if fields.len() <= id {
                fields.resize_with(id + 1, || None);
            }

            fields[id] = Some((offset, value));
        }

        self.classes[index].fields = fields;

        Ok(index)
    }

    /// A `$_TSPEC` entry's type (`'type'`, with `'class'` for a struct and `'elem'` for a list's element).
    fn value(&mut self, entry: &ZendHashTable, class: &str, seen: &mut HashMap<String, usize>) -> Result<Value, PhpException> {
        let unsupported = |what: String| ext_exception(format!("flow_php cannot decode {class}::$_TSPEC {what}"));

        // Thrift\Type\TType
        Ok(match entry.get("type").and_then(Zval::long) {
            Some(2) => Value::Bool,
            Some(3) => Value::Byte,
            Some(4) => Value::Double,
            Some(6) => Value::I16,
            Some(8) => Value::I32,
            Some(10) => Value::I64,
            Some(11) => Value::String,
            Some(12) => Value::Struct(self.class(
                entry
                    .get("class")
                    .and_then(Zval::str)
                    .ok_or_else(|| unsupported("struct without a class".to_string()))?,
                seen,
            )?),
            Some(15) => Value::List(Box::new(
                self.value(
                    entry
                        .get("elem")
                        .and_then(Zval::array)
                        .ok_or_else(|| unsupported("list without an element".to_string()))?,
                    class,
                    seen,
                )?,
            )),
            other => return Err(unsupported(format!("type {other:?}"))),
        })
    }
}

/// `class`'s instance decoded from `bytes`; with `field`, only that field of the root is decoded and returned (null
/// when absent), the fields before it skipped and the ones after it never read. STRUCT → object (created without its constructor, public properties set by
/// `var`), LIST → list, BOOL/BYTE/I16/I32/I64/DOUBLE/STRING (binary) → scalars; an unknown id, or a known one of another
/// type, is skipped by the compact skip rules.
pub fn decode(bytes: &[u8], class: &str, field: Option<i16>) -> Result<Zval, PhpException> {
    let specs = match ctx::thrift_specs(class)? {
        Some(specs) => specs,
        None => {
            let specs = Rc::new(Specs::build(class)?);
            ctx::store_thrift_specs(class, &specs)?;
            specs
        }
    };
    let mut reader = Reader { bytes, position: 0 };

    match field {
        None => reader.object(&specs, 0, 0),
        Some(field) => reader.field(&specs, field),
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

fn truncated() -> PhpException {
    ext_exception("flow_php failed to decode thrift: the bytes end inside a value")
}

impl Reader<'_> {
    fn byte(&mut self) -> Result<u8, PhpException> {
        let byte = *self.bytes.get(self.position).ok_or_else(truncated)?;
        self.position += 1;

        Ok(byte)
    }

    fn take(&mut self, length: usize) -> Result<&[u8], PhpException> {
        let end = self.position.checked_add(length).filter(|end| *end <= self.bytes.len()).ok_or_else(truncated)?;
        let taken = &self.bytes[self.position..end];
        self.position = end;

        Ok(taken)
    }

    fn varint(&mut self) -> Result<u64, PhpException> {
        let mut value = 0u64;

        for shift in (0..70).step_by(7) {
            let byte = self.byte()?;
            value |= u64::from(byte & 0x7f) << shift;

            if byte & 0x80 == 0 {
                return Ok(value);
            }
        }

        Err(ext_exception("flow_php failed to decode thrift: a varint longer than 10 bytes"))
    }

    fn zigzag(&mut self) -> Result<i64, PhpException> {
        let value = self.varint()?;

        Ok((value >> 1) as i64 ^ -((value & 1) as i64))
    }

    /// The next field's id and compact type, `None` at the struct's STOP.
    fn field_header(&mut self, last: &mut i16) -> Result<Option<(i16, u8)>, PhpException> {
        let header = self.byte()?;

        if header == 0 {
            return Ok(None);
        }

        let delta = header >> 4;
        *last = if delta == 0 {
            i16::try_from(self.zigzag()?).map_err(|_| ext_exception("flow_php failed to decode thrift: a field id"))?
        } else {
            last.wrapping_add(i16::from(delta))
        };

        Ok(Some((*last, header & 0x0f)))
    }

    /// A list or set header: its size and element type.
    fn list_header(&mut self) -> Result<(usize, u8), PhpException> {
        let header = self.byte()?;
        let size = match header >> 4 {
            15 => usize::try_from(self.varint()?).map_err(|_| truncated())?,
            size => usize::from(size),
        };

        Ok((size, header & 0x0f))
    }

    fn skip(&mut self, wire: u8, depth: usize) -> Result<(), PhpException> {
        if depth > DEPTH {
            return Err(ext_exception("flow_php failed to decode thrift: nested deeper than 64"));
        }

        match wire {
            BOOLEAN_TRUE | BOOLEAN_FALSE => {}
            BYTE => {
                self.byte()?;
            }
            I16 | I32 | I64 => {
                self.varint()?;
            }
            DOUBLE => {
                self.take(8)?;
            }
            BINARY => {
                let length = usize::try_from(self.varint()?).map_err(|_| truncated())?;
                self.take(length)?;
            }
            LIST | SET => {
                let (size, element) = self.list_header()?;

                for _ in 0..size {
                    self.skip_element(element, depth + 1)?;
                }
            }
            MAP => {
                let size = usize::try_from(self.varint()?).map_err(|_| truncated())?;

                if size > 0 {
                    let types = self.byte()?;

                    for _ in 0..size {
                        self.skip_element(types >> 4, depth + 1)?;
                        self.skip_element(types & 0x0f, depth + 1)?;
                    }
                }
            }
            STRUCT => {
                let mut last = 0;

                while let Some((_, wire)) = self.field_header(&mut last)? {
                    self.skip(wire, depth + 1)?;
                }
            }
            other => return Err(ext_exception(format!("flow_php failed to decode thrift: compact type {other}"))),
        }

        Ok(())
    }

    /// A container element: a bool is one byte there, not a field-header type.
    fn skip_element(&mut self, wire: u8, depth: usize) -> Result<(), PhpException> {
        match wire {
            BOOLEAN_TRUE | BOOLEAN_FALSE => self.byte().map(|_| ()),
            wire => self.skip(wire, depth),
        }
    }

    /// A value of type `value` that arrived as `wire`; `element`: inside a list, where a bool is a byte.
    fn value(&mut self, specs: &Specs, value: &Value, wire: u8, element: bool, depth: usize) -> Result<Option<Zval>, PhpException> {
        let mut zv = Zval::new();

        match value {
            Value::Bool => zv.set_bool(if element { self.byte()? == BOOLEAN_TRUE } else { wire == BOOLEAN_TRUE }),
            Value::Byte => zv = zval_long(i64::from(self.byte()? as i8)),
            Value::I16 | Value::I32 | Value::I64 => zv = zval_long(self.zigzag()?),
            Value::Double => {
                let bytes = self.take(8)?;
                zv.set_double(f64::from_le_bytes(bytes.try_into().map_err(|_| truncated())?));
            }
            Value::String => {
                let length = usize::try_from(self.varint()?).map_err(|_| truncated())?;
                zv = zval_str(self.take(length)?);
            }
            Value::Struct(class) => zv = self.object(specs, *class, depth + 1)?,
            Value::List(item) => {
                let (size, wire) = self.list_header()?;

                if !item.arrives_as(wire) {
                    for _ in 0..size {
                        self.skip_element(wire, depth + 1)?;
                    }

                    return Ok(None);
                }

                let mut list = ZendHashTable::with_capacity(u32::try_from(size).unwrap_or(u32::MAX));

                for _ in 0..size {
                    let value = self.value(specs, item, wire, true, depth + 1)?.unwrap_or_default();
                    list.push(value)
                        .map_err(|e| ext_exception(format!("flow_php failed to collect a thrift list: {e:?}")))?;
                }

                zv.set_hashtable(list);
            }
        }

        Ok(Some(zv))
    }

    /// An instance of `specs`' class `class`, its properties set from the fields that arrive.
    fn object(&mut self, specs: &Specs, class: usize, depth: usize) -> Result<Zval, PhpException> {
        if depth > DEPTH {
            return Err(ext_exception("flow_php failed to decode thrift: nested deeper than 64"));
        }

        let class = &specs.classes[class];
        let mut object = ZendObject::new(class.ce);
        let mut last = 0;

        while let Some((id, wire)) = self.field_header(&mut last)? {
            match field_of(class, id) {
                Some((offset, value)) if value.arrives_as(wire) => {
                    if let Some(decoded) = self.value(specs, value, wire, false, depth)? {
                        replace_slot(&mut object, *offset, decoded);
                    }
                }
                _ => self.skip(wire, depth)?,
            }
        }

        let mut zv = Zval::new();
        zv.set_object(&mut object);

        Ok(zv)
    }

    /// Field `field` of the root class, the fields before it skipped; null when it never arrives.
    fn field(&mut self, specs: &Specs, field: i16) -> Result<Zval, PhpException> {
        let class = &specs.classes[0];
        let mut last = 0;

        while let Some((id, wire)) = self.field_header(&mut last)? {
            match field_of(class, id) {
                Some((_, value)) if id == field && value.arrives_as(wire) => {
                    if let Some(decoded) = self.value(specs, value, wire, false, 0)? {
                        return Ok(decoded);
                    }
                }
                _ => self.skip(wire, 0)?,
            }
        }

        Ok(Zval::new())
    }
}

fn field_of(class: &Class, id: i16) -> Option<&(u32, Value)> {
    usize::try_from(id).ok().and_then(|id| class.fields.get(id)).and_then(Option::as_ref)
}

/// Moves `value` into a declared-property slot, releasing what the slot held (null on a fresh object, an earlier
/// value when a field id repeats).
fn replace_slot(object: &mut ZendObject, offset: u32, value: Zval) {
    let slot = unsafe { std::ptr::from_mut(object).cast::<u8>().add(offset as usize).cast::<Zval>() };

    drop(unsafe { std::ptr::replace(slot, value) });
}
