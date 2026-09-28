//! Arrow rows as the zvals the PHP columns hold and return: `at()` (the physical form `PhysicalFor` produces),
//! `value()` (the logical form `Physical::fromPhysical()` builds), and a physical zval appended back into storage.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::Array;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::kind::Kind;

use crate::ctx::{self, call_method, ht_for_each, ht_insert, null_zval, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::kind_builder::{KindBuilder, OffsetOverflow};
use crate::plan::ValueNode;
use crate::render::{invalid_argument, offset_overflow};
use crate::values::{date_from_days, datetime_from_micros, interval_from_micros, json_from_bytes, uuid_from_bytes};

fn long(array: &dyn Array, kind: &Kind, i: usize) -> i64 {
    match kind {
        Kind::Int64 => array.as_primitive::<Int64Type>().value(i),
        Kind::Timestamp => array.as_primitive::<TimestampMicrosecondType>().value(i),
        Kind::Duration => array.as_primitive::<DurationMicrosecondType>().value(i),
        Kind::Int32 => i64::from(array.as_primitive::<Date32Type>().value(i)),
        _ => unreachable!("long() reads integer kinds only"),
    }
}

fn double(value: f64) -> Zval {
    let mut zv = Zval::new();
    zv.set_double(value);
    zv
}

fn boolean(value: bool) -> Zval {
    let mut zv = Zval::new();
    zv.set_bool(value);
    zv
}

fn array_zval(ht: ZBox<ZendHashTable>) -> Zval {
    let mut zv = Zval::new();
    zv.set_hashtable(ht);
    zv
}

fn push(ht: &mut ZendHashTable, value: Zval) -> Result<(), PhpException> {
    ht.push(value)
        .map_err(|e| ext_exception(format!("flow_php failed to collect column values: {e:?}")))
}

/// A map key as `array_keys()` returns it: the int or string key `array_combine()` coerces.
fn map_key(ht: &mut ZendHashTable, keys: &dyn Array, key: &Kind, j: usize, value: Zval) {
    match key {
        Kind::Bytes => ht_insert(ht, keys.as_binary::<i32>().value(j), value),
        _ => {
            let index = long(keys, key, j);
            crate::ctx::ht_insert_long(ht, index, value);
        }
    }
}

/// `Column::at($i)`: null, int (µs, days), float, bool, string (16 bytes for a uuid), or nested arrays.
pub fn physical_at(array: &dyn Array, kind: &Kind, i: usize) -> Result<Zval, PhpException> {
    if matches!(kind, Kind::Null) || array.is_null(i) {
        return Ok(null_zval());
    }

    Ok(match kind {
        Kind::Null => unreachable!("returned above"),
        Kind::Int64 | Kind::Timestamp | Kind::Duration | Kind::Int32 => zval_long(long(array, kind, i)),
        Kind::Float64 => double(array.as_primitive::<Float64Type>().value(i)),
        Kind::Boolean => boolean(array.as_boolean().value(i)),
        Kind::Uuid => zval_str(array.as_fixed_size_binary().value(i)),
        Kind::Bytes => zval_str(array.as_binary::<i32>().value(i)),
        Kind::List(element) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let mut ht = ZendHashTable::with_capacity((offsets[i + 1] - offsets[i]) as u32);

            for j in offsets[i] as usize..offsets[i + 1] as usize {
                push(&mut ht, physical_at(list.values().as_ref(), &element.kind, j)?)?;
            }

            array_zval(ht)
        }
        Kind::Map(key, value) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let mut ht = ZendHashTable::with_capacity((offsets[i + 1] - offsets[i]) as u32);

            for j in offsets[i] as usize..offsets[i + 1] as usize {
                let physical = physical_at(map.values().as_ref(), &value.kind, j)?;
                map_key(&mut ht, map.keys().as_ref(), &key.kind, j, physical);
            }

            array_zval(ht)
        }
        Kind::Struct(fields) => {
            let structure = array.as_struct();
            let mut ht = ZendHashTable::with_capacity(fields.len() as u32);

            for (field, child) in fields.iter().zip(structure.columns()) {
                if matches!(field.kind, Kind::Null) || child.is_null(i) {
                    if !field.optional {
                        ht_insert(&mut ht, field.name.as_bytes(), null_zval());
                    }

                    continue;
                }

                ht_insert(&mut ht, field.name.as_bytes(), physical_at(child.as_ref(), &field.kind, i)?);
            }

            array_zval(ht)
        }
    })
}

/// `Column::value($i)`: the logical value of row `i`, built natively except markup (D4).
pub fn value_at(array: &dyn Array, kind: &Kind, node: &ValueNode, i: usize) -> Result<Zval, PhpException> {
    if matches!(kind, Kind::Null) || array.is_null(i) {
        return Ok(null_zval());
    }

    match (node, kind) {
        (ValueNode::DateTime(zone), _) => datetime_from_micros(long(array, kind, i), zone),
        (ValueNode::Date, _) => date_from_days(long(array, kind, i)),
        (ValueNode::Time, _) => interval_from_micros(long(array, kind, i)),
        (ValueNode::Uuid, _) => uuid_from_bytes(array.as_fixed_size_binary().value(i)),
        (ValueNode::Json, _) => json_from_bytes(array.as_binary::<i32>().value(i)),
        (ValueNode::Enum(class), _) => ctx::enum_case(class, array.as_binary::<i32>().value(i)),
        (ValueNode::TimeZone, _) => ctx::timezone(array.as_binary::<i32>().value(i)),
        (ValueNode::Markup(physical), _) => call_method(
            physical,
            "fromPhysical",
            &mut [zval_str(array.as_binary::<i32>().value(i))],
        ),
        (ValueNode::List(element_node), Kind::List(element)) => {
            let list = array.as_list::<i32>();
            let offsets = list.value_offsets();
            let mut ht = ZendHashTable::with_capacity((offsets[i + 1] - offsets[i]) as u32);

            for j in offsets[i] as usize..offsets[i + 1] as usize {
                push(&mut ht, value_at(list.values().as_ref(), &element.kind, element_node, j)?)?;
            }

            Ok(array_zval(ht))
        }
        (ValueNode::Map(value_node), Kind::Map(key, value)) => {
            let map = array.as_map();
            let offsets = map.value_offsets();
            let mut ht = ZendHashTable::with_capacity((offsets[i + 1] - offsets[i]) as u32);

            for j in offsets[i] as usize..offsets[i + 1] as usize {
                let logical = value_at(map.values().as_ref(), &value.kind, value_node, j)?;
                map_key(&mut ht, map.keys().as_ref(), &key.kind, j, logical);
            }

            Ok(array_zval(ht))
        }
        (ValueNode::Struct(nodes), Kind::Struct(fields)) => {
            let structure = array.as_struct();
            let mut ht = ZendHashTable::with_capacity(fields.len() as u32);

            for ((field, child), child_node) in fields.iter().zip(structure.columns()).zip(nodes) {
                if matches!(field.kind, Kind::Null) || child.is_null(i) {
                    if !field.optional {
                        ht_insert(&mut ht, field.name.as_bytes(), null_zval());
                    }

                    continue;
                }

                ht_insert(&mut ht, field.name.as_bytes(), value_at(child.as_ref(), &field.kind, child_node, i)?);
            }

            Ok(array_zval(ht))
        }
        _ => physical_at(array, kind, i),
    }
}

fn wrong_physical(kind: &Kind, physical: &Zval) -> PhpException {
    invalid_argument(format!(
        "flow_php cannot store a {} physical in a {kind:?} column",
        physical.get_type()
    ))
}

pub fn overflow(OffsetOverflow(last): OffsetOverflow) -> PhpException {
    offset_overflow(last)
}

/// Appends one physical zval (what `Physical::toPhysical()` or `Column::at()` returns) to `builder`.
pub fn append_physical(builder: &mut KindBuilder, kind: &Kind, physical: &Zval) -> Result<(), PhpException> {
    let physical = physical.dereference();

    if physical.is_null() || matches!(kind, Kind::Null) {
        builder.append_null();

        return Ok(());
    }

    match kind {
        Kind::Null => unreachable!("appended above"),
        Kind::Int64 | Kind::Timestamp | Kind::Duration => {
            let value = physical.long().ok_or_else(|| wrong_physical(kind, physical))?;
            builder.append_fixed(&value.to_le_bytes());
        }
        Kind::Int32 => {
            let value = physical
                .long()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(|| wrong_physical(kind, physical))?;
            builder.append_fixed(&value.to_le_bytes());
        }
        Kind::Float64 => {
            let value = physical.double().ok_or_else(|| wrong_physical(kind, physical))?;
            builder.append_fixed(&value.to_le_bytes());
        }
        Kind::Boolean => builder.append_bool(physical.bool().ok_or_else(|| wrong_physical(kind, physical))?),
        Kind::Uuid => {
            let bytes = physical
                .zend_str()
                .map(|string| string.as_bytes())
                .filter(|bytes| bytes.len() == 16)
                .ok_or_else(|| wrong_physical(kind, physical))?;
            builder.append_fixed(bytes);
        }
        Kind::Bytes => builder
            .append_bytes(physical.zend_str().ok_or_else(|| wrong_physical(kind, physical))?.as_bytes())
            .map_err(overflow)?,
        Kind::List(element) => {
            let values = physical.array().ok_or_else(|| wrong_physical(kind, physical))?;

            ht_for_each(values, |_, _, value| append_physical(builder.list_element(), &element.kind, value))?;
            builder.end_entries().map_err(overflow)?;
        }
        Kind::Map(key, value) => {
            let entries = physical.array().ok_or_else(|| wrong_physical(kind, physical))?;

            ht_for_each(entries, |string_key, index, item| {
                let (keys, values) = builder.map_entries();
                let key_zv = match string_key {
                    Some(name) => zval_str(name.as_bytes()),
                    None => zval_long(index as i64),
                };
                // array keys are ints for numeric strings: a string-keyed map stores their decimal text
                let key_zv = match (&key.kind, key_zv.get_type()) {
                    (Kind::Bytes, DataType::Long) => zval_str(index.to_string().as_bytes()),
                    _ => key_zv,
                };

                append_physical(keys, &key.kind, &key_zv)?;
                append_physical(values, &value.kind, item)
            })?;
            builder.end_entries().map_err(overflow)?;
        }
        Kind::Struct(fields) => {
            let elements = physical.array().ok_or_else(|| wrong_physical(kind, physical))?;
            let null = null_zval();

            for (child, field) in builder.struct_children().iter_mut().zip(fields) {
                let element = crate::ctx::ht_get(elements, field.name.as_bytes()).unwrap_or(&null);
                append_physical(child, &field.kind, element)?;
            }

            builder.end_struct();
        }
    }

    Ok(())
}
