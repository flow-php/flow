//! `Flow\ETL\Column\NativeColumn`: one arrow array of the column's kind, read as the PHP columns read.

use std::rc::Rc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, UInt32Array};
use arrow_schema::ArrowError;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::column::encode;
use flow_batch_frame::kind::{data_type, Kind};

use crate::ctx::{call_method, expect_object, find_class, ht_for_each, instance, read_property, zval_long, zval_str};
use crate::exception::ext_exception;
use crate::interfaces::column_ce;
use crate::physical::{physical_at, value_at};
use crate::plan::{type_plan, TypePlan};
use crate::render::{invalid_argument, offset_overflow};

/// A return typed `Flow\Types\Type`.
pub struct TypeObject(Zval);

impl IntoZval for TypeObject {
    const TYPE: DataType = DataType::Object(Some("Flow\\Types\\Type"));
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Column\\NativeColumn",
    flags = ClassFlags::Final,
    implements(ce = column_ce, stub = "Flow\\ETL\\Column\\Column")
)]
pub struct NativeColumn {
    data: ArrayRef,
    plan: Rc<TypePlan>,
}

impl NativeColumn {
    pub fn new(data: ArrayRef, plan: Rc<TypePlan>) -> Self {
        debug_assert_eq!(data.data_type(), &data_type(&plan.kind));

        Self { data, plan }
    }

    pub fn data(&self) -> &ArrayRef {
        &self.data
    }

    pub fn plan(&self) -> &Rc<TypePlan> {
        &self.plan
    }

    fn kind(&self) -> &Kind {
        &self.plan.kind
    }

    fn row(&self, i: i64) -> Result<usize, PhpException> {
        usize::try_from(i)
            .ok()
            .filter(|i| *i < self.data.len())
            .ok_or_else(|| invalid_argument(format!("flow_php column row {i} is outside [0, {})", self.data.len())))
    }

    fn rows(&self, read: impl Fn(usize) -> Result<Zval, PhpException>) -> Result<ZBox<ZendHashTable>, PhpException> {
        let mut rows = ZendHashTable::with_capacity(self.data.len() as u32);

        for i in 0..self.data.len() {
            rows.push(read(i)?)
                .map_err(|e| ext_exception(format!("flow_php failed to collect column rows: {e:?}")))?;
        }

        Ok(rows)
    }
}

fn null_count(array: &dyn Array, kind: &Kind) -> usize {
    match kind {
        Kind::Null => array.len(),
        _ => array.null_count(),
    }
}

/// The child rows of a list or map slice, compacted as the PHP columns hold them.
fn entries(offsets: &[i32], child: &ArrayRef) -> ArrayRef {
    let start = offsets[0] as usize;

    child.slice(start, offsets[offsets.len() - 1] as usize - start)
}

fn base(type_zv: Zval) -> Result<Zval, PhpException> {
    if expect_object(&type_zv, "a Type")?.instance_of(find_class("Flow\\Types\\Type\\Logical\\OptionalType")?) {
        return call_method(&type_zv, "base", &mut []);
    }

    Ok(type_zv)
}

/// `Column::withType()` of the PHP columns over the same storage: `Retype::assert()` at every level, and a required
/// structure element refuses the rows its optional one left absent.
fn retype(from: Zval, to: Zval, array: &ArrayRef, kind: &Kind) -> Result<(), PhpException> {
    call_method(
        &instance("Flow\\ETL\\Column\\Php\\Retype")?,
        "assert",
        &mut [from.shallow_clone(), to.shallow_clone(), zval_long(null_count(array.as_ref(), kind) as i64)],
    )?;

    match kind {
        Kind::List(element) => {
            let list = array.as_list::<i32>();

            retype(
                call_method(&base(from)?, "element", &mut [])?,
                call_method(&base(to)?, "element", &mut [])?,
                &entries(list.value_offsets(), list.values()),
                &element.kind,
            )
        }
        Kind::Map(key, value) => {
            let map = array.as_map();
            let (from, to) = (base(from)?, base(to)?);

            retype(
                call_method(&from, "key", &mut [])?,
                call_method(&to, "key", &mut [])?,
                &entries(map.value_offsets(), map.keys()),
                &key.kind,
            )?;
            retype(
                call_method(&from, "value", &mut [])?,
                call_method(&to, "value", &mut [])?,
                &entries(map.value_offsets(), map.values()),
                &value.kind,
            )
        }
        Kind::Struct(fields) => {
            let structure = array.as_struct();
            let from_elements = call_method(&base(from)?, "elements", &mut [])?;
            let to_elements = call_method(&base(to)?, "elements", &mut [])?;
            let from_elements: Vec<&Zval> = from_elements.array().map(|a| a.values().collect()).unwrap_or_default();
            let to_elements: Vec<&Zval> = to_elements.array().map(|a| a.values().collect()).unwrap_or_default();

            for (((field, child), from_element), to_element) in
                fields.iter().zip(structure.columns()).zip(from_elements).zip(to_elements)
            {
                let from_element = expect_object(from_element, "a StructureElement")?;
                let to_element = expect_object(to_element, "a StructureElement")?;
                let optional = |element| -> Result<bool, PhpException> {
                    Ok(read_property(element, "optional")?.bool().unwrap_or(false))
                };

                if optional(from_element)? && !optional(to_element)? {
                    let absent = (0..child.len())
                        .filter(|i| !array.is_null(*i) && (matches!(field.kind, Kind::Null) || child.is_null(*i)))
                        .count();

                    if absent > 0 {
                        return Err(invalid_argument(format!(
                            "{absent} absent values under the required element \"{}\"",
                            field.name
                        )));
                    }
                }

                retype(
                    read_property(from_element, "type")?,
                    read_property(to_element, "type")?,
                    child,
                    &field.kind,
                )?;
            }

            Ok(())
        }
        _ => Ok(()),
    }
}

fn class_name(zv: &Zval) -> String {
    zv.object()
        .and_then(|object| object.get_class_name().ok())
        .unwrap_or_else(|| zv.get_type().to_string())
}

#[php_impl]
impl NativeColumn {
    pub fn __construct() -> PhpResult<Self> {
        Err(ext_exception("Flow\\ETL\\Column\\NativeColumn is built by DefaultBackend"))
    }

    #[php(name = "type")]
    pub fn column_type(&self) -> TypeObject {
        TypeObject(self.plan.type_zv.shallow_clone())
    }

    pub fn count(&self) -> i64 {
        self.data.len() as i64
    }

    #[php(name = "isNull")]
    pub fn is_null(&self, i: i64) -> PhpResult<bool> {
        let row = self.row(i)?;

        Ok(matches!(self.kind(), Kind::Null) || self.data.is_null(row))
    }

    #[php(name = "nullCount")]
    pub fn null_count(&self) -> i64 {
        null_count(self.data.as_ref(), self.kind()) as i64
    }

    pub fn at(&self, i: i64) -> PhpResult<Zval> {
        physical_at(self.data.as_ref(), self.kind(), self.row(i)?)
    }

    pub fn physicals(&self) -> PhpResult<ZBox<ZendHashTable>> {
        self.rows(|i| physical_at(self.data.as_ref(), self.kind(), i))
    }

    pub fn value(&self, i: i64) -> PhpResult<Zval> {
        value_at(self.data.as_ref(), self.kind(), &self.plan.values, self.row(i)?)
    }

    pub fn values(&self) -> PhpResult<ZBox<ZendHashTable>> {
        self.rows(|i| value_at(self.data.as_ref(), self.kind(), &self.plan.values, i))
    }

    pub fn slice(&self, offset: i64, length: i64) -> PhpResult<NativeColumn> {
        let (Ok(offset), Ok(length)) = (usize::try_from(offset), usize::try_from(length)) else {
            return Err(invalid_argument(format!("flow_php cannot slice {length} rows from row {offset}")));
        };

        if offset.checked_add(length).is_none_or(|end| end > self.data.len()) {
            return Err(invalid_argument(format!(
                "flow_php cannot slice {length} rows from row {offset} of {}",
                self.data.len()
            )));
        }

        Ok(NativeColumn::new(self.data.slice(offset, length), Rc::clone(&self.plan)))
    }

    pub fn take(&self, indices: &ZendHashTable) -> PhpResult<NativeColumn> {
        let mut rows = Vec::with_capacity(indices.len());

        ht_for_each(indices, |_, _, index| {
            let row = index
                .long()
                .ok_or_else(|| invalid_argument("flow_php column indices must be ints".to_string()))?;
            rows.push(self.row(row)? as u32);

            Ok(())
        })?;

        let data = arrow_select::take::take(self.data.as_ref(), &UInt32Array::from(rows), None)
            .map_err(|e| invalid_argument(format!("flow_php failed to take column rows: {e}")))?;

        Ok(NativeColumn::new(data, Rc::clone(&self.plan)))
    }

    #[php(name = "withType")]
    pub fn with_type(&self, r#type: &Zval) -> PhpResult<NativeColumn> {
        retype(self.plan.type_zv.shallow_clone(), r#type.shallow_clone(), &self.data, self.kind())?;
        let plan = type_plan(r#type)?;

        if data_type(&plan.kind) != data_type(self.kind()) {
            return Err(invalid_argument(format!(
                "flow_php cannot store {:?} in a {:?} column",
                plan.kind,
                self.kind()
            )));
        }

        Ok(NativeColumn::new(self.data.clone(), plan))
    }

    pub fn concat(&self, others: &[&Zval]) -> PhpResult<NativeColumn> {
        let mut arrays: Vec<&dyn Array> = vec![self.data.as_ref()];

        for other in others {
            let native = other
                .extract::<&NativeColumn>()
                .filter(|native| native.data.data_type() == self.data.data_type())
                .ok_or_else(|| {
                    invalid_argument(format!(
                        "Flow\\ETL\\Column\\NativeColumn cannot concat {}",
                        class_name(other)
                    ))
                })?;
            arrays.push(native.data.as_ref());
        }

        let data = arrow_select::concat::concat(&arrays).map_err(|e| match e {
            ArrowError::OffsetOverflowError(last) => offset_overflow(last as u64),
            e => invalid_argument(format!("flow_php failed to concat columns: {e}")),
        })?;

        Ok(NativeColumn::new(data, Rc::clone(&self.plan)))
    }

    pub fn encode(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let buffers = encode(&self.data.to_data(), self.kind())
            .map_err(|e| invalid_argument(format!("flow_php failed to encode a column: {e}")))?;
        let mut list = ZendHashTable::with_capacity(buffers.len() as u32);

        for buffer in buffers {
            list.push(zval_str(&buffer))
                .map_err(|e| ext_exception(format!("flow_php failed to collect column buffers: {e:?}")))?;
        }

        Ok(list)
    }
}
