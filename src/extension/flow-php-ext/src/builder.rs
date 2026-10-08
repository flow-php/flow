//! `Flow\ETL\Column\RustColumnBuilder`: `CastingColumnBuilder` over arrow storage. A value takes the native lane
//! where `cast.rs` casts it natively; every other value, and every value the native lane refuses, runs PHP's
//! `$type->cast()` then `$physical->toPhysical()`, so results and refusals are PHP's own.

// `appendPhysicals()`'s `$nullCount`: ext-php-rs names a PHP parameter after its Rust identifier and binds it in a
// generated handler, which an `allow` on the method, the impl block or the parameter does not reach
#![allow(non_snake_case)]

use std::rc::Rc;

use arrow_array::Array;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};
use flow_batch_frame::kind::Kind;

use crate::cast::{cast_value, CastKind, MapKeyKind};
use crate::column::RustColumn;
use crate::ctx::{
    self, call_handle_catching, call_method, call_static, construct, expect_object, find_class, ht_for_each, ht_get,
    null_zval, read_slot, transparent_exception, zval_long,
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

/// One value ready to append, for `RustBackend::constant()`.
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

/// What `append_one()` did with a value.
pub enum Append {
    Done,
    /// `cause` is the `ColumnMismatchException` a positioned append wraps; `raised` the `Flow\Types` exception an
    /// unpositioned append rethrows (`None` for a NOT NULL null).
    Refused {
        cause: Zval,
        raised: Option<ZBox<ZendObject>>,
    },
}

fn string_slot(object: &ZendObject, slot: u32) -> Option<Zval> {
    let value = read_slot(object, slot);

    value.is_string().then(|| value.shallow_clone())
}

/// The physical form of `value` for `plan`, when `cast.rs` casts it natively; `None` hands it to the PHP lane.
pub fn native_lane(plan: &TypePlan, value: &Zval) -> Result<Option<Native>, PhpException> {
    native_leaf(&plan.kind, &plan.cast, value)
}

pub(crate) fn native_leaf(kind: &Kind, cast_kind: &CastKind, value: &Zval) -> Result<Option<Native>, PhpException> {
    if let (Kind::Timestamp, CastKind::DateTime(_)) = (kind, cast_kind) {
        if let Some(micros) = value
            .zend_str()
            .and_then(|string| iso_instant_micros(string.as_bytes()))
        {
            return Ok(Some(Native::Fixed(micros.to_le_bytes())));
        }
    }

    if nested(kind) {
        return Ok(None);
    }

    let Some(cast) = cast_value(cast_kind, value)? else {
        return Ok(None);
    };

    Ok(match (kind, cast_kind) {
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

fn nested(kind: &Kind) -> bool {
    matches!(kind, Kind::List(_) | Kind::Map(..) | Kind::Struct(_))
}

/// Casts `value` straight into `builder`, element by element. `Ok(false)`: Rust does not cast this value, and the row
/// is left half-appended for the caller to truncate before the PHP lane casts the whole value.
fn append_cast(
    builder: &mut KindBuilder,
    kind: &Kind,
    cast_kind: &CastKind,
    value: &Zval,
) -> Result<bool, PhpException> {
    let value = value.dereference();

    match (kind, cast_kind) {
        (_, CastKind::Optional(inner)) => {
            if value.is_null() {
                builder.append_null();

                return Ok(true);
            }

            append_cast(builder, kind, inner, value)
        }
        (Kind::List(element), CastKind::List(inner)) => {
            let Some(values) = value.array() else {
                return Ok(false);
            };
            let mut expected = 0;
            let mut cast = true;

            ht_for_each(values, |string_key, index, item| {
                cast = cast
                    && string_key.is_none()
                    && index == expected
                    && append_cast(builder.list_element(), &element.kind, inner, item)?;
                expected += 1;

                Ok(())
            })?;

            if cast {
                builder.end_entries().map_err(overflow)?;
            }

            Ok(cast)
        }
        (Kind::Map(key, item_kind), CastKind::Map(key_cast, inner)) => {
            let Some(values) = value.array() else {
                return Ok(false);
            };
            let mut cast = true;

            ht_for_each(values, |string_key, index, item| {
                if !cast {
                    return Ok(());
                }

                let (keys, items) = builder.map_entries();

                cast = match (key_cast, string_key, &key.kind) {
                    (MapKeyKind::Int, None, Kind::Int64) => {
                        keys.append_fixed(&(index as i64).to_le_bytes());

                        true
                    }
                    (MapKeyKind::Str, Some(name), Kind::Bytes) => {
                        keys.append_bytes(name.as_bytes());

                        true
                    }
                    _ => false,
                } && append_cast(items, &item_kind.kind, inner, item)?;

                Ok(())
            })?;

            if cast {
                builder.end_entries().map_err(overflow)?;
            }

            Ok(cast)
        }
        (Kind::Struct(fields), CastKind::Structure(elements)) => {
            let Some(values) = value.array() else {
                return Ok(false);
            };

            for ((child, field), element) in builder.struct_children().iter_mut().zip(fields).zip(elements) {
                match ht_get(values, field.name.as_bytes()) {
                    None if element.required => return Ok(false),
                    None => child.append_null(),
                    Some(item) if item.dereference().is_null() && !matches!(element.kind, CastKind::Optional(_)) => {
                        return Ok(false);
                    }
                    Some(item) => {
                        if !append_cast(child, &field.kind, &element.kind, item)? {
                            return Ok(false);
                        }
                    }
                }
            }

            builder.end_struct();

            Ok(true)
        }
        _ => match native_leaf(kind, cast_kind, value)? {
            Some(native) => {
                append_native(builder, native)?;

                Ok(true)
            }
            None => Ok(false),
        },
    }
}

/// One value, as `RustColumnBuilder::appendMany()` appends it: a null is refused under NOT NULL; a nested value
/// takes `append_cast()`, a leaf the native lane, and what neither casts the PHP lane. Exceptions other than
/// `Flow\Types` ones escape as thrown.
pub fn append_one(
    values: &mut KindBuilder,
    plan: &TypePlan,
    definition: &Zval,
    nullable: bool,
    value: &Zval,
) -> Result<Append, PhpException> {
    let value = value.dereference();

    if value.is_null() {
        if !nullable {
            return Ok(Append::Refused {
                cause: value_does_not_match(definition, null_zval(), None)?,
                raised: None,
            });
        }

        values.append_null();

        return Ok(Append::Done);
    }

    if nested(&plan.kind) {
        let row = values.len();

        if append_cast(values, &plan.kind, &plan.cast, value)? {
            return Ok(Append::Done);
        }

        values.truncate(row);
    } else if let Some(native) = native_lane(plan, value)? {
        append_native(values, native)?;

        return Ok(Append::Done);
    }

    match php_lane(plan, value) {
        Ok(physical) => {
            append_physical(values, &plan.kind, &physical)?;

            Ok(Append::Done)
        }
        Err(Refusal::Cast(mut exception) | Refusal::Physical(mut exception)) if !is_types_exception(&exception)? => {
            Err(transparent_exception(&mut exception))
        }
        // the cast's own reason is dropped, as CastingColumnBuilder::appendMany drops it
        Err(Refusal::Cast(exception)) => Ok(Append::Refused {
            cause: value_does_not_match(definition, value.shallow_clone(), None)?,
            raised: Some(exception),
        }),
        Err(Refusal::Physical(mut exception)) => {
            let mut reason = Zval::new();
            reason.set_object(&mut exception);

            // the value cast, so only the physical form's reason (a range) tells the user why it was refused
            Ok(Append::Refused {
                cause: value_does_not_match(definition, value.shallow_clone(), Some(reason))?,
                raised: Some(exception),
            })
        }
    }
}

/// `$type->cast($value)`, then `$physical->toPhysical($cast)` unless the physical is the identity.
pub fn php_lane(plan: &TypePlan, value: &Zval) -> Result<Zval, Refusal> {
    let type_obj = plan.type_zv.object().expect("a plan's type is an object");
    let cast =
        call_handle_catching(plan.cast_fn, Some(type_obj), &mut [value.shallow_clone()]).map_err(Refusal::Cast)?;

    if plan.identity {
        return Ok(cast);
    }

    let physical = plan.physical.object().expect("a plan's physical is an object");

    call_handle_catching(plan.to_physical, Some(physical), &mut [cast]).map_err(Refusal::Physical)
}

pub(crate) fn append_native(builder: &mut KindBuilder, native: Native) -> Result<(), PhpException> {
    match native {
        Native::Fixed(bytes) => builder.append_fixed(&bytes),
        Native::Days(days) => builder.append_fixed(&days.to_le_bytes()),
        Native::Bool(value) => builder.append_bool(value),
        Native::Uuid(bytes) => builder.append_fixed(&bytes),
        Native::Bytes(string) => builder.append_bytes(string.zend_str().expect("a native string").as_bytes()),
    }

    Ok(())
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
    let built = find_class(SCHEMA_MISMATCH).and_then(|ce| construct(ce, &mut [position, cause]));

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
    name = "Flow\\ETL\\Column\\RustColumnBuilder",
    flags = ClassFlags::Final,
    implements(ce = column_builder_ce, stub = "Flow\\ETL\\Column\\ColumnBuilder")
)]
pub struct RustColumnBuilder {
    definition: Zval,
    nullable: bool,
    plan: Rc<TypePlan>,
    values: KindBuilder,
}

impl RustColumnBuilder {
    pub fn for_definition(definition: &Zval, plan: Rc<TypePlan>) -> Result<Self, PhpException> {
        Ok(Self {
            definition: definition.shallow_clone(),
            nullable: definition_nullable(definition)?,
            values: KindBuilder::new(&plan.kind),
            plan,
        })
    }

    /// One value for `RustBackend::constant()`: refusals escape as PHP threw them.
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

    /// One value cast and appended. With a `position`, refusals are `appendMany()`'s `SchemaMismatchException` at it;
    /// without, they escape as PHP threw them, as `append()` lets them.
    fn append_value(&mut self, value: &Zval, position: Option<&dyn Fn() -> Zval>) -> Result<(), PhpException> {
        match append_one(&mut self.values, &self.plan, &self.definition, self.nullable, value)? {
            Append::Done => Ok(()),
            Append::Refused { cause, raised } => Err(match (position, raised) {
                (Some(position), _) => schema_mismatch(position(), cause),
                (None, Some(mut raised)) => transparent_exception(&mut raised),
                (None, None) => throw(cause),
            }),
        }
    }

    fn not_null(&self, is_null: bool) -> Result<(), PhpException> {
        if is_null && !self.nullable {
            return Err(throw(value_does_not_match(&self.definition, null_zval(), None)?));
        }

        Ok(())
    }

    fn same_kind<'a>(&self, column: &'a Zval) -> Option<&'a RustColumn> {
        column
            .extract::<&RustColumn>()
            .filter(|native| native.data().data_type() == &flow_batch_frame::kind::data_type(&self.plan.kind))
    }

    /// `count` copies of one value, for `RustBackend::constant()`.
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
                    self.values
                        .append_from(one.as_ref(), &self.plan.kind, 0)
                        .map_err(overflow)?;
                }
            }
        }

        Ok(())
    }

    pub fn finish_column(&self) -> Result<RustColumn, PhpException> {
        let data = self
            .values
            .finish(&self.plan.kind)
            .map_err(|e| invalid_argument(format!("flow_php built an invalid column: {e}")))?;

        Ok(RustColumn::new(arrow_array::make_array(data), Rc::clone(&self.plan)))
    }
}

fn index(i: i64, count: usize) -> Result<usize, PhpException> {
    usize::try_from(i)
        .ok()
        .filter(|i| *i < count)
        .ok_or_else(|| invalid_argument(format!("flow_php column index {i} is outside [0, {count})")))
}

#[php_impl]
impl RustColumnBuilder {
    pub fn __construct() -> PhpResult<Self> {
        Err(ext_exception(
            "Flow\\ETL\\Column\\RustColumnBuilder is built by RustBackend",
        ))
    }

    pub fn append(&mut self, value: &Zval) -> PhpResult<()> {
        let start = self.values.len();
        let result = self.append_value(value, None);

        if result.is_err() {
            self.values.truncate(start);
        }

        result
    }

    #[php(name = "appendMany")]
    pub fn append_many(&mut self, values: &ZendHashTable) -> PhpResult<()> {
        let start = self.values.len();
        let result = ht_for_each(values, |string_key, index, value| {
            self.append_value(
                value,
                Some(&|| match string_key {
                    Some(name) => crate::ctx::zval_str(name.as_bytes()),
                    None => zval_long(index as i64),
                }),
            )
        });

        if result.is_err() {
            self.values.truncate(start);
        }

        result
    }

    /// `ColumnBuilder::appendPhysicals()`: physical values as they are. A null under NOT NULL is refused at its
    /// position, a physical of another kind as `append_physical()` refuses it; `$nullCount` is the PHP builders' hint,
    /// this builder counts nulls as it appends.
    #[php(name = "appendPhysicals")]
    pub fn append_physicals(&mut self, physicals: &ZendHashTable, nullCount: Option<i64>) -> PhpResult<()> {
        let _ = nullCount;
        let start = self.values.len();
        let result = ht_for_each(physicals, |_, index, physical| {
            if physical.is_null() && !self.nullable {
                return Err(schema_mismatch(
                    zval_long(index as i64),
                    value_does_not_match(&self.definition, null_zval(), None)?,
                ));
            }

            append_physical(&mut self.values, &self.plan.kind, physical)
        });

        if result.is_err() {
            self.values.truncate(start);
        }

        result
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

        self.not_null(
            call_method(column, "isNull", &mut [zval_long(i)])?
                .bool()
                .unwrap_or(false),
        )?;
        let physical = call_method(column, "at", &mut [zval_long(i)])?;

        append_physical(&mut self.values, &self.plan.kind, &physical)
    }

    #[php(name = "appendTake")]
    pub fn append_take(&mut self, column: &Zval, indices: &ZendHashTable) -> PhpResult<()> {
        if let Some(native) = self.same_kind(column) {
            let data = native.data();
            let mut rows = Vec::with_capacity(indices.len());

            ht_for_each(indices, |_, _, i| {
                let row = index(
                    i.long()
                        .ok_or_else(|| invalid_argument("flow_php column indices must be ints".into()))?,
                    data.len(),
                )?;
                self.not_null(matches!(self.plan.kind, Kind::Null) || data.is_null(row))?;
                rows.push(row);

                Ok(())
            })?;

            for row in rows {
                self.values
                    .append_from(data.as_ref(), &self.plan.kind, row)
                    .map_err(overflow)?;
            }

            return Ok(());
        }

        let all = call_method(column, "physicals", &mut [])?;
        let all = all
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Column::physicals() to return an array"))?;
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

    pub fn finish(&self) -> PhpResult<RustColumn> {
        self.finish_column()
    }
}
