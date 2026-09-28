//! `Flow\ETL\Column\NativeColumnBuilder`: `CastingColumnBuilder` over arrow storage. A value takes the native lane
//! where `cast.rs` casts it natively; every other value, and every value the native lane refuses, runs PHP's
//! `$type->cast()` then `$physical->toPhysical()`, so results and refusals are PHP's own.

use std::rc::Rc;

use arrow_array::Array;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};
use flow_batch_frame::kind::Kind;

use crate::cast::{cast_value, CastKind};
use crate::column::NativeColumn;
use crate::ctx::{
    self, call_handle_catching, call_method, call_static, construct_with_zvals, expect_object, find_class,
    ht_for_each, null_zval, read_slot, transparent_exception, zval_long,
};
use crate::date_check::iso_instant_micros;
use crate::exception::ext_exception;
use crate::interfaces::column_builder_ce;
use crate::kind_builder::KindBuilder;
use crate::physical::{append_physical, overflow};
use crate::plan::TypePlan;
use crate::render::invalid_argument;
use crate::values::{datetime_days, datetime_micros, uuid_bytes};

const COLUMN_MISMATCH: &str = "Flow\\ETL\\Exception\\ColumnMismatchException";
const SCHEMA_MISMATCH: &str = "Flow\\ETL\\Exception\\SchemaMismatchException";
const TYPES_EXCEPTION: &str = "Flow\\Types\\Exception\\Exception";
const NULL_DEFINITION: &str = "Flow\\ETL\\Schema\\Definition\\NullDefinition";

/// A value in its physical form, produced by the native lane without a PHP call.
pub enum Native {
    Fixed([u8; 8]),
    Days(i32),
    Bool(bool),
    Uuid([u8; 16]),
    Bytes(Zval),
}

/// One value ready to append: `appendMany()` builds all of them before appending any.
pub enum Pending {
    Null,
    Native(Native),
    Physical(Zval),
}

/// Why the PHP lane refused a value: its cast, or its `toPhysical()`.
pub enum Refusal {
    Cast(ZBox<ZendObject>),
    Physical(ZBox<ZendObject>),
}

fn string_slot(object: &ZendObject, slot: u32) -> Option<Zval> {
    let value = read_slot(object, slot);

    value.is_string().then(|| value.shallow_clone())
}

/// The physical form of `value` for `plan`, when `cast.rs` casts it natively; `None` hands it to the PHP lane.
pub fn native_lane(plan: &TypePlan, value: &Zval) -> Result<Option<Native>, PhpException> {
    if let (Kind::Timestamp, CastKind::DateTime(_)) = (&plan.kind, &plan.cast) {
        if let Some(micros) = value.zend_str().and_then(|string| iso_instant_micros(string.as_bytes())) {
            return Ok(Some(Native::Fixed(micros.to_le_bytes())));
        }
    }

    let Some(cast) = cast_value(&plan.cast, value)? else {
        return Ok(None);
    };

    Ok(match (&plan.kind, &plan.cast) {
        (Kind::Int64, CastKind::Integer | CastKind::PositiveInteger) => {
            cast.long().map(|long| Native::Fixed(long.to_le_bytes()))
        }
        (Kind::Float64, CastKind::Float) => cast.double().map(|double| Native::Fixed(double.to_le_bytes())),
        (Kind::Boolean, CastKind::Boolean) => cast.bool().map(Native::Bool),
        (Kind::Bytes, CastKind::String | CastKind::NonEmptyString) => cast.is_string().then_some(Native::Bytes(cast)),
        (Kind::Timestamp, CastKind::DateTime(_)) => cast
            .object()
            .and_then(datetime_micros)
            .map(|micros| Native::Fixed(micros.to_le_bytes())),
        (Kind::Int32, CastKind::Date) => cast
            .object()
            .and_then(datetime_days)
            .map(|days| Native::Days(days as i32)),
        (Kind::Uuid, CastKind::Uuid) => {
            let (_, value_slot) = ctx::uuid()?;

            cast.object()
                .and_then(|uuid| string_slot(uuid, value_slot))
                .and_then(|text| uuid_bytes(text.zend_str()?.as_bytes()))
                .map(Native::Uuid)
        }
        (Kind::Bytes, CastKind::Json) => {
            let (_, value_slot, _) = ctx::json()?;

            cast.object()
                .and_then(|json| string_slot(json, value_slot))
                .map(Native::Bytes)
        }
        _ => None,
    })
}

/// `$type->cast($value)`, then `$physical->toPhysical($cast)` unless the physical is the identity.
pub fn php_lane(plan: &TypePlan, value: &Zval) -> Result<Zval, Refusal> {
    let type_obj = plan.type_zv.object().expect("a plan's type is an object");
    let cast = call_handle_catching(plan.cast_fn, Some(type_obj), &mut [value.shallow_clone()]).map_err(Refusal::Cast)?;

    if plan.identity {
        return Ok(cast);
    }

    let physical = plan.physical.object().expect("a plan's physical is an object");

    call_handle_catching(plan.to_physical, Some(physical), &mut [cast]).map_err(Refusal::Physical)
}

fn append_native(builder: &mut KindBuilder, native: Native) -> Result<(), PhpException> {
    match native {
        Native::Fixed(bytes) => builder.append_fixed(&bytes),
        Native::Days(days) => builder.append_fixed(&days.to_le_bytes()),
        Native::Bool(value) => builder.append_bool(value),
        Native::Uuid(bytes) => builder.append_fixed(&bytes),
        Native::Bytes(string) => builder
            .append_bytes(string.zend_str().expect("a native string").as_bytes())
            .map_err(overflow)?,
    }

    Ok(())
}

fn append_pending(builder: &mut KindBuilder, kind: &Kind, pending: Pending) -> Result<(), PhpException> {
    match pending {
        Pending::Null => {
            builder.append_null();
            Ok(())
        }
        Pending::Native(native) => append_native(builder, native),
        Pending::Physical(physical) => append_physical(builder, kind, &physical),
    }
}

/// `ColumnMismatchException::valueDoesNotMatch($definition, $value[, $reason])`.
pub fn value_does_not_match(definition: &Zval, value: Zval, reason: Option<Zval>) -> Result<Zval, PhpException> {
    let mut args = vec![definition.shallow_clone(), value];
    args.extend(reason);

    call_static(COLUMN_MISMATCH, "valueDoesNotMatch", &mut args)
}

pub fn throw(mut exception: Zval) -> PhpException {
    match exception.object_mut() {
        Some(object) => transparent_exception(object),
        None => ext_exception("flow_php expected an exception object"),
    }
}

/// `new SchemaMismatchException($position, $cause)`.
pub fn schema_mismatch(position: Zval, cause: Zval) -> PhpException {
    let built = find_class(SCHEMA_MISMATCH)
        .and_then(|ce| construct_with_zvals(ce, &mut [position, cause], "a SchemaMismatchException"));

    match built {
        Ok(mut exception) => transparent_exception(&mut exception),
        Err(e) => e,
    }
}

/// Whether `definition` admits a null row: `isNullable()`, or a `NullDefinition`.
pub fn definition_nullable(definition: &Zval) -> Result<bool, PhpException> {
    Ok(call_method(definition, "isNullable", &mut [])?.bool().unwrap_or(false)
        || expect_object(definition, "a Definition")?.instance_of(find_class(NULL_DEFINITION)?))
}

pub fn is_types_exception(exception: &ZendObject) -> Result<bool, PhpException> {
    Ok(exception.instance_of(find_class(TYPES_EXCEPTION)?))
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Column\\NativeColumnBuilder",
    flags = ClassFlags::Final,
    implements(ce = column_builder_ce, stub = "Flow\\ETL\\Column\\ColumnBuilder")
)]
pub struct NativeColumnBuilder {
    definition: Zval,
    nullable: bool,
    plan: Rc<TypePlan>,
    values: KindBuilder,
}

impl NativeColumnBuilder {
    pub fn for_definition(definition: &Zval, plan: Rc<TypePlan>) -> Result<Self, PhpException> {
        Ok(Self {
            definition: definition.shallow_clone(),
            nullable: definition_nullable(definition)?,
            values: KindBuilder::new(&plan.kind),
            plan,
        })
    }

    /// One value for `append()`/`DefaultBackend::constant()`: refusals escape as PHP threw them.
    pub fn pending(&self, value: &Zval) -> Result<Pending, PhpException> {
        let value = value.dereference();

        if value.is_null() {
            if !self.nullable {
                return Err(throw(value_does_not_match(&self.definition, null_zval(), None)?));
            }

            return Ok(Pending::Null);
        }

        if let Some(native) = native_lane(&self.plan, value)? {
            return Ok(Pending::Native(native));
        }

        match php_lane(&self.plan, value) {
            Ok(physical) => Ok(Pending::Physical(physical)),
            Err(Refusal::Cast(mut exception) | Refusal::Physical(mut exception)) => {
                Err(transparent_exception(&mut exception))
            }
        }
    }

    pub fn append_pending(&mut self, pending: Pending) -> Result<(), PhpException> {
        append_pending(&mut self.values, &self.plan.kind, pending)
    }

    fn not_null(&self, is_null: bool) -> Result<(), PhpException> {
        if is_null && !self.nullable {
            return Err(throw(value_does_not_match(&self.definition, null_zval(), None)?));
        }

        Ok(())
    }

    fn same_kind<'a>(&self, column: &'a Zval) -> Option<&'a NativeColumn> {
        column
            .extract::<&NativeColumn>()
            .filter(|native| native.data().data_type() == &flow_batch_frame::kind::data_type(&self.plan.kind))
    }

    /// `count` copies of one value, for `DefaultBackend::constant()`.
    pub fn append_repeated(&mut self, pending: Pending, count: u32) -> Result<(), PhpException> {
        match pending {
            Pending::Null => (0..count).for_each(|_| self.values.append_null()),
            Pending::Physical(physical) => {
                for _ in 0..count {
                    append_physical(&mut self.values, &self.plan.kind, &physical)?;
                }
            }
            Pending::Native(native) => {
                let mut single = KindBuilder::new(&self.plan.kind);
                append_native(&mut single, native)?;
                let one = arrow_array::make_array(
                    single
                        .finish(&self.plan.kind)
                        .map_err(|e| invalid_argument(format!("flow_php built an invalid column: {e}")))?,
                );

                for _ in 0..count {
                    self.values.append_from(one.as_ref(), &self.plan.kind, 0).map_err(overflow)?;
                }
            }
        }

        Ok(())
    }

    pub fn finish_column(&self) -> Result<NativeColumn, PhpException> {
        let data = self
            .values
            .finish(&self.plan.kind)
            .map_err(|e| invalid_argument(format!("flow_php built an invalid column: {e}")))?;

        Ok(NativeColumn::new(arrow_array::make_array(data), Rc::clone(&self.plan)))
    }
}

fn index(i: i64, count: usize) -> Result<usize, PhpException> {
    usize::try_from(i)
        .ok()
        .filter(|i| *i < count)
        .ok_or_else(|| invalid_argument(format!("flow_php column index {i} is outside [0, {count})")))
}

#[php_impl]
impl NativeColumnBuilder {
    pub fn __construct() -> PhpResult<Self> {
        Err(ext_exception("Flow\\ETL\\Column\\NativeColumnBuilder is built by DefaultBackend"))
    }

    pub fn append(&mut self, value: &Zval) -> PhpResult<()> {
        let pending = self.pending(value)?;

        self.append_pending(pending)
    }

    #[php(name = "appendMany")]
    pub fn append_many(&mut self, values: &ZendHashTable) -> PhpResult<()> {
        let mut pendings = Vec::with_capacity(values.len());

        ht_for_each(values, |string_key, index, value| {
            let value = value.dereference();
            let position = || match string_key {
                Some(name) => crate::ctx::zval_str(name.as_bytes()),
                None => zval_long(index as i64),
            };

            if value.is_null() {
                if !self.nullable {
                    return Err(schema_mismatch(
                        position(),
                        value_does_not_match(&self.definition, null_zval(), None)?,
                    ));
                }

                pendings.push(Pending::Null);

                return Ok(());
            }

            if let Some(native) = native_lane(&self.plan, value)? {
                pendings.push(Pending::Native(native));

                return Ok(());
            }

            match php_lane(&self.plan, value) {
                Ok(physical) => {
                    pendings.push(Pending::Physical(physical));

                    Ok(())
                }
                Err(Refusal::Cast(mut exception)) => {
                    if !is_types_exception(&exception)? {
                        return Err(transparent_exception(&mut exception));
                    }

                    // the cast's own reason is dropped, as CastingColumnBuilder::appendMany drops it
                    Err(schema_mismatch(
                        position(),
                        value_does_not_match(&self.definition, value.shallow_clone(), None)?,
                    ))
                }
                Err(Refusal::Physical(mut exception)) => {
                    if !is_types_exception(&exception)? {
                        return Err(transparent_exception(&mut exception));
                    }

                    let mut reason = Zval::new();
                    reason.set_object(&mut exception);

                    // the value cast, so only the physical form's reason (a range) tells the user why it was refused
                    Err(schema_mismatch(
                        position(),
                        value_does_not_match(&self.definition, value.shallow_clone(), Some(reason))?,
                    ))
                }
            }
        })?;

        for pending in pendings {
            self.append_pending(pending)?;
        }

        Ok(())
    }

    #[php(name = "appendFrom")]
    pub fn append_from(&mut self, column: &Zval, i: i64) -> PhpResult<()> {
        if let Some(native) = self.same_kind(column) {
            let i = index(i, native.data().len())?;
            self.not_null(matches!(self.plan.kind, Kind::Null) || native.data().is_null(i))?;

            return self
                .values
                .append_from(native.data().as_ref(), &self.plan.kind, i)
                .map_err(overflow);
        }

        self.not_null(call_method(column, "isNull", &mut [zval_long(i)])?.bool().unwrap_or(false))?;
        let physical = call_method(column, "at", &mut [zval_long(i)])?;

        append_physical(&mut self.values, &self.plan.kind, &physical)
    }

    #[php(name = "appendTake")]
    pub fn append_take(&mut self, column: &Zval, indices: &ZendHashTable) -> PhpResult<()> {
        if let Some(native) = self.same_kind(column) {
            let data = native.data();
            let mut rows = Vec::with_capacity(indices.len());

            ht_for_each(indices, |_, _, i| {
                let row = index(i.long().ok_or_else(|| invalid_argument("flow_php column indices must be ints".into()))?, data.len())?;
                self.not_null(matches!(self.plan.kind, Kind::Null) || data.is_null(row))?;
                rows.push(row);

                Ok(())
            })?;

            for row in rows {
                self.values.append_from(data.as_ref(), &self.plan.kind, row).map_err(overflow)?;
            }

            return Ok(());
        }

        let all = call_method(column, "physicals", &mut [])?;
        let all = all.array().ok_or_else(|| ext_exception("flow_php expected Column::physicals() to return an array"))?;
        let mut physicals = Vec::with_capacity(indices.len());

        ht_for_each(indices, |_, _, i| {
            let physical = i
                .long()
                .and_then(|i| all.get_index(i))
                .map_or_else(null_zval, Zval::shallow_clone);
            self.not_null(physical.is_null())?;
            physicals.push(physical);

            Ok(())
        })?;

        for physical in physicals {
            append_physical(&mut self.values, &self.plan.kind, &physical)?;
        }

        Ok(())
    }

    pub fn count(&self) -> i64 {
        self.values.len() as i64
    }

    pub fn finish(&self) -> PhpResult<NativeColumn> {
        self.finish_column()
    }
}
