//! A batch of native columns, one builder per schema definition, with `RowsBuilder::appendRows()`'s refusals: each
//! column keeps its first, the lowest refused row wins (then the earliest definition), and a NOT NULL column a row
//! lacks is refused only when nothing else was. The readers differ only in where a cell comes from.

use std::rc::Rc;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::kind::Kind;

use crate::builder::{definition_nullable, schema_mismatch};
use crate::cast::{
    bool_from_str, float_from_str, integer_from_str, json_gate, parse_iso_date, parse_iso_datetime, CastKind,
};
use crate::column::RustColumn;
use crate::ctx::{self, call_method, call_static, ht_for_each, ht_insert, ht_insert_long, zval_long};
use crate::date_check::iso_instant_micros;
use crate::exception::ext_exception;
use crate::json_check::json_valid;
use crate::kind_builder::KindBuilder;
use crate::plan::{type_plan, TypePlan};
use crate::render::invalid_argument;
use crate::uuid_check::is_uuid;
use crate::values::{datetime_days, datetime_micros, uuid_bytes};

/// A `Schema::definitions()` key: an int for a numeric name, as PHP keys it.
pub enum Key {
    Index(i64),
    Name(Vec<u8>),
}

impl Key {
    pub fn name(&self) -> Vec<u8> {
        match self {
            Key::Index(index) => index.to_string().into_bytes(),
            Key::Name(name) => name.clone(),
        }
    }
}

pub struct BatchColumn {
    pub key: Key,
    pub definition: Zval,
    pub nullable: bool,
    pub plan: Rc<TypePlan>,
    pub values: KindBuilder,
    /// The first refused row and its `ColumnMismatchException`.
    pub refusal: Option<(usize, Zval)>,
    /// The first row lacking this NOT NULL column.
    pub absent: Option<usize>,
}

/// The batch being built, and the schema and batch size it is built for.
pub struct BatchColumns {
    schema: Zval,
    schema_json: Vec<u8>,
    pub batch_size: usize,
    pub columns: Vec<BatchColumn>,
    pub rows: usize,
}

/// A PHP `$batchSize` as a row count: refused unless greater than 0.
pub fn batch_size_of(batch_size: i64, what: &str) -> Result<usize, PhpException> {
    usize::try_from(batch_size)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| ext_exception(format!("flow_php {what} batch size must be greater than 0")))
}

fn schema_json(schema: &Zval) -> Result<Vec<u8>, PhpException> {
    let normalized = call_method(schema, "normalize", &mut [])?;
    let json = ctx::call_handle(ctx::json_encode()?, None, &mut [normalized], "encode a schema as JSON")?;

    Ok(json
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected schema JSON to be a string"))?
        .as_bytes()
        .to_vec())
}

impl BatchColumns {
    fn new(schema: &Zval, schema_json: Vec<u8>, batch_size: usize) -> Result<Self, PhpException> {
        let definitions = call_method(schema, "definitions", &mut [])?;
        let definitions = definitions
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
        let mut columns = Vec::with_capacity(definitions.len());

        ht_for_each(definitions, |name, index, definition| {
            let plan = type_plan(&call_method(definition, "type", &mut [])?)?;

            columns.push(BatchColumn {
                key: match name {
                    Some(name) => Key::Name(name.as_bytes().to_vec()),
                    None => Key::Index(index as i64),
                },
                definition: definition.shallow_clone(),
                nullable: definition_nullable(definition)?,
                values: KindBuilder::new(&plan.kind),
                plan,
                refusal: None,
                absent: None,
            });

            Ok(())
        })?;

        Ok(Self {
            schema: schema.shallow_clone(),
            schema_json,
            batch_size,
            columns,
            rows: 0,
        })
    }

    /// The state for `schema` and `batch_size`: the same object or the same `normalize()` JSON keeps it (`false`), any
    /// other schema rebuilds it (`true`); changing the schema or the batch size of a batch with rows pending throws.
    pub fn prepare<'a>(
        pending: &'a mut Option<Self>,
        schema: &Zval,
        batch_size: usize,
        what: &str,
    ) -> Result<(&'a mut Self, bool), PhpException> {
        let same_object = pending.as_ref().is_some_and(|state| {
            state.schema.object().map(|object| object.handle) == schema.object().map(|object| object.handle)
        });
        let mut rebuilt = false;

        if !same_object {
            let json = schema_json(schema)?;

            match pending {
                Some(state) if state.schema_json == json => state.schema = schema.shallow_clone(),
                Some(state) if state.rows > 0 => {
                    return Err(ext_exception(format!(
                        "flow_php cannot change the schema of a pending {what} batch"
                    )));
                }
                _ => {
                    *pending = Some(Self::new(schema, json, batch_size)?);
                    rebuilt = true;
                }
            }
        }

        let state = pending.as_mut().expect("built above");

        if state.batch_size != batch_size {
            if state.rows > 0 {
                return Err(ext_exception(format!(
                    "flow_php cannot change the batch size of a pending {what} batch"
                )));
            }

            state.batch_size = batch_size;
        }

        Ok((state, rebuilt))
    }

    pub fn reset(&mut self) {
        self.rows = 0;

        for column in &mut self.columns {
            column.values = KindBuilder::new(&column.plan.kind);
            column.refusal = None;
            column.absent = None;
        }
    }

    /// The batch's columns, or its refusal: the lowest refused row (ties: the earliest definition), else the lowest
    /// absent row (ties: the earliest definition) as `missingColumn`. The state is reset either way.
    pub fn finish(&mut self) -> Result<HeldBatch, PhpException> {
        let batch = self.build();
        self.reset();

        batch
    }

    fn build(&self) -> Result<HeldBatch, PhpException> {
        // min_by_key() keeps the first of equal rows: the earliest definition
        if let Some((row, cause)) = self
            .columns
            .iter()
            .filter_map(|column| column.refusal.as_ref())
            .min_by_key(|(row, _)| *row)
        {
            return Err(schema_mismatch(zval_long(*row as i64), cause.shallow_clone()));
        }

        if let Some((row, column)) = self
            .columns
            .iter()
            .filter_map(|column| Some((column.absent?, column)))
            .min_by_key(|(row, _)| *row)
        {
            let cause = call_static(
                "Flow\\ETL\\Exception\\ColumnMismatchException",
                "missingColumn",
                &mut [column.definition.shallow_clone()],
            )?;

            return Err(schema_mismatch(zval_long(row as i64), cause));
        }

        let mut columns = ZendHashTable::with_capacity(self.columns.len() as u32);

        for column in &self.columns {
            let data = column
                .values
                .finish(&column.plan.kind)
                .map_err(|e| invalid_argument(format!("flow_php built an invalid column: {e}")))?;
            let mut native = Zval::new();
            ext_php_rs::convert::IntoZval::set_zval(
                RustColumn::new(arrow_array::make_array(data), Rc::clone(&column.plan)),
                &mut native,
                false,
            )?;

            match &column.key {
                Key::Index(index) => ht_insert_long(&mut columns, *index, native),
                Key::Name(name) => ht_insert(&mut columns, name, native),
            }
        }

        Ok(HeldBatch {
            columns,
            count: self.rows,
        })
    }
}

/// A batch's native columns keyed as its schema's definitions, before the configured backend adopts them.
pub struct HeldBatch {
    pub columns: ZBox<ZendHashTable>,
    pub count: usize,
}

/// A text value `cast.rs` casts natively, appended in its physical form; `false` hands it to the PHP lane.
pub fn native_text(values: &mut KindBuilder, kind: &Kind, cast: &CastKind, bytes: &[u8]) -> Result<bool, PhpException> {
    match (kind, cast) {
        (Kind::Int64, CastKind::Integer) => Ok(integer_from_str(bytes)
            .map(|value| values.append_fixed(&value.to_le_bytes()))
            .is_some()),
        (Kind::Float64, CastKind::Float) => Ok(float_from_str(bytes)
            .map(|value| values.append_fixed(&value.to_le_bytes()))
            .is_some()),
        (Kind::Boolean, CastKind::Boolean) => Ok(bool_from_str(bytes).map(|value| values.append_bool(value)).is_some()),
        (Kind::Bytes, CastKind::String) => {
            values.append_bytes(bytes);

            Ok(true)
        }
        (Kind::Bytes, CastKind::NonEmptyString) if !bytes.is_empty() => {
            values.append_bytes(bytes);

            Ok(true)
        }
        (Kind::Bytes, CastKind::Json) if json_gate(bytes) && json_valid(bytes) => {
            values.append_bytes(bytes);

            Ok(true)
        }
        (Kind::Uuid, CastKind::Uuid) if is_uuid(bytes) => {
            Ok(uuid_bytes(bytes).map(|uuid| values.append_fixed(&uuid)).is_some())
        }
        (Kind::Timestamp, CastKind::DateTime(zone)) => Ok(match iso_instant_micros(bytes) {
            Some(micros) => Some(micros),
            None => {
                parse_iso_datetime(bytes, zone)?.and_then(|(_, instant)| instant.object().and_then(datetime_micros))
            }
        }
        .map(|micros| values.append_fixed(&micros.to_le_bytes()))
        .is_some()),
        (Kind::Int32, CastKind::Date) => Ok(parse_iso_date(bytes)?
            .and_then(|instant| instant.object().and_then(datetime_days))
            .map(|days| values.append_fixed(&(days as i32).to_le_bytes()))
            .is_some()),
        _ => Ok(false),
    }
}
