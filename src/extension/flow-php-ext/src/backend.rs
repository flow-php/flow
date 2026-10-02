//! `Flow\ETL\Column\RustBackend`: every column is a `RustColumn`.

// a parameter's Rust name is its PHP name, and `Backend::decode()` names it `$nullCount`
#![allow(non_snake_case)]

use std::rc::Rc;

use arrow_array::ArrayRef;

use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::column::decode;
use flow_batch_frame::layout::buffer_count;

use crate::batch_columns::HeldBatch;
use crate::builder::{throw, RustColumnBuilder};
use crate::column::RustColumn;
use crate::ctx::{
    call_method, call_static, column_key, expect_object, find_class, ht_for_each, ht_get, ht_insert, zval_long,
};
use crate::exception::ext_exception;
use crate::interfaces::backend_ce;
use crate::plan::{physical_for_definition, type_plan, TypePlan};
use crate::render::{decode_exception, invalid_argument};

const COLUMN_MISMATCH: &str = "Flow\\ETL\\Exception\\ColumnMismatchException";
const MIXED_TYPE: &str = "Flow\\Types\\Type\\Native\\MixedType";

/// A return typed `Flow\ETL\Column\Column`: `adopt()` hands back its argument itself.
pub struct ColumnValue(pub Zval);

impl IntoZval for ColumnValue {
    const TYPE: DataType = DataType::object("Flow\\ETL\\Column\\Column");
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

fn definition_plan(definition: &Zval) -> PhpResult<Rc<TypePlan>> {
    type_plan(&call_method(definition, "type", &mut [])?)
}

fn rows(count: i64, name: &str) -> PhpResult<u32> {
    u32::try_from(count).map_err(|_| invalid_argument(format!("flow_php {name} must be between 0 and {}", u32::MAX)))
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Column\\RustBackend",
    flags = ClassFlags::Final,
    implements(ce = backend_ce, stub = "Flow\\ETL\\Column\\Backend")
)]
pub struct RustBackend;

#[php_impl]
impl RustBackend {
    pub fn __construct() -> Self {
        RustBackend
    }

    pub fn builder(&self, definition: &Zval) -> PhpResult<RustColumnBuilder> {
        physical_for_definition(definition)?;

        RustColumnBuilder::for_definition(definition, definition_plan(definition)?)
    }

    pub fn constant(&self, definition: &Zval, value: &Zval, count: i64) -> PhpResult<RustColumn> {
        let mut builder = self.builder(definition)?;
        let pending = builder.pending(value)?;

        builder.append_repeated(pending, rows(count, "constant count")?)?;
        builder.finish_column()
    }

    pub fn decode(
        &self,
        definition: &Zval,
        buffers: &ZendHashTable,
        count: i64,
        nullCount: i64,
    ) -> PhpResult<RustColumn> {
        let plan = definition_plan(definition)?;
        let bytes = buffers
            .values()
            .map(|buffer| {
                buffer
                    .zend_str()
                    .map(|buffer| buffer.as_bytes())
                    .ok_or_else(|| invalid_argument("flow_php column buffers must be strings".to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let needed = buffer_count(&plan.kind).min(bytes.len());
        let data = decode(
            &plan.kind,
            &bytes[..needed],
            rows(count, "row count")?,
            rows(nullCount, "null count")?,
        )
        .map_err(|e| decode_exception(e, &plan.type_zv, &plan.kind))?;

        if bytes.len() > needed {
            let entry = call_method(definition, "entry", &mut [])?;
            let name = call_method(&entry, "name", &mut [])?;

            return Err(invalid_argument(format!(
                "Column \"{}\": {} buffers left after decoding",
                String::from_utf8_lossy(name.zend_str().map_or(&b""[..], |name| name.as_bytes())),
                bytes.len() - needed
            )));
        }

        Ok(RustColumn::new(arrow_array::make_array(data), plan))
    }

    pub fn adopt(&self, definition: &Zval, column: &Zval) -> PhpResult<ColumnValue> {
        if column.extract::<&RustColumn>().is_some() {
            return Ok(ColumnValue(column.shallow_clone()));
        }

        if expect_object(&call_method(column, "type", &mut [])?, "a Type")?.instance_of(find_class(MIXED_TYPE)?) {
            return Err(throw(call_static(
                COLUMN_MISMATCH,
                "untypedColumn",
                &mut [definition.shallow_clone()],
            )?));
        }

        let mut builder = self.builder(definition)?;
        let count = call_method(column, "count", &mut [])?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected Column::count() to return an int"))?;

        if count > 0 {
            let mut indices = ZendHashTable::with_capacity(count as u32);

            for i in 0..count {
                indices
                    .push(zval_long(i))
                    .map_err(|e| ext_exception(format!("flow_php failed to collect row indices: {e:?}")))?;
            }

            builder.append_take(column, &indices)?;
        }

        let mut adopted = builder.finish_column()?.into_zval(false)?;

        Ok(ColumnValue(std::mem::take(&mut adopted)))
    }

    #[php(name = "allocatedBytes")]
    pub fn allocated_bytes(&self) -> i64 {
        crate::alloc::allocated_bytes()
    }
}

/// `$held` as `Rows` with every column adopted by `$backend` - what a Rust open source yields: the configured backend
/// keeps its own columns and copies the native ones.
pub fn adopt_batch(schema: &Zval, backend: &Zval, held: HeldBatch) -> PhpResult<Zval> {
    let definitions = call_method(schema, "definitions", &mut [])?;
    let definitions = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
    let mut columns = ZendHashTable::with_capacity(definitions.len() as u32);

    ht_for_each(definitions, |name, index, definition| {
        let key = column_key(name, index);
        let column = ht_get(&held.columns, &key).ok_or_else(|| {
            ext_exception(format!(
                "flow_php expected the batch to hold the column \"{}\"",
                String::from_utf8_lossy(&key)
            ))
        })?;
        ht_insert(
            &mut columns,
            &key,
            call_method(
                backend,
                "adopt",
                &mut [definition.shallow_clone(), column.shallow_clone()],
            )?,
        );

        Ok(())
    })?;

    let mut columns_zv = Zval::new();
    columns_zv.set_hashtable(columns);

    call_static(
        "Flow\\ETL\\Rows",
        "fromColumns",
        &mut [schema.shallow_clone(), columns_zv, zval_long(held.count as i64)],
    )
}

/// `$column` as a `RustColumn`'s plan and data: itself, or `RustBackend`'s adoption of it under `$definition`.
pub fn native_column(definition: &Zval, column: &Zval) -> PhpResult<(Rc<TypePlan>, ArrayRef)> {
    let adopted;
    let native = match column.extract::<&RustColumn>() {
        Some(native) => native,
        None => {
            adopted = RustBackend.adopt(definition, column)?.0;
            adopted
                .extract::<&RustColumn>()
                .ok_or_else(|| ext_exception("flow_php expected RustBackend::adopt() to build a RustColumn"))?
        }
    };

    Ok((Rc::clone(native.plan()), ArrayRef::clone(native.data())))
}
