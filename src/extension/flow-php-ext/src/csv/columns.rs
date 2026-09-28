//! `RustCSVReaderNative::nextColumns()`: CSV cells straight into native columns, one builder per schema definition,
//! with `RowsBuilder::appendRows()`'s refusals - each column keeps its first, the lowest row wins (then the earliest
//! definition), and a NOT NULL column absent from the header is refused only when nothing else was.

use std::rc::Rc;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::kind::Kind;

use crate::builder::{
    definition_nullable, is_types_exception, php_lane, schema_mismatch, value_does_not_match, Refusal,
};
use crate::cast::{bool_from_str, float_from_str, integer_from_str, json_gate, parse_iso_date, parse_iso_datetime, CastKind};
use crate::column::NativeColumn;
use crate::csv::CsvReader;
use crate::ctx::{
    self, call_method, call_static, ht_for_each, ht_insert, ht_insert_long, null_zval, transparent_exception, zval_long,
    zval_str,
};
use crate::date_check::iso_instant_micros;
use crate::exception::ext_exception;
use crate::json_check::json_valid;
use crate::kind_builder::KindBuilder;
use crate::physical::{append_physical, overflow};
use crate::plan::{type_plan, TypePlan};
use crate::render::invalid_argument;
use crate::uuid_check::is_uuid;
use crate::values::{datetime_days, datetime_micros, uuid_bytes};

/// A `Schema::definitions()` key: an int for a numeric name, as PHP keys it.
enum Key {
    Index(i64),
    Name(Vec<u8>),
}

impl Key {
    fn name(&self) -> Vec<u8> {
        match self {
            Key::Index(index) => index.to_string().into_bytes(),
            Key::Name(name) => name.clone(),
        }
    }
}

struct Column {
    key: Key,
    definition: Zval,
    nullable: bool,
    plan: Rc<TypePlan>,
    /// `None`: the header has no column of this name.
    field: Option<usize>,
    values: KindBuilder,
    /// The first refused row and its `ColumnMismatchException`.
    refusal: Option<(usize, Zval)>,
}

/// The batch being built, and the schema and batch size it is built for.
pub struct CsvColumns {
    schema: Zval,
    schema_json: Vec<u8>,
    batch_size: usize,
    columns: Vec<Column>,
    rows: usize,
}

fn schema_json(schema: &Zval) -> Result<Vec<u8>, PhpException> {
    let normalized = call_method(schema, "normalize", &mut [])?;
    let json = crate::ctx::call_handle(ctx::json_encode()?, None, &mut [normalized], "encode a schema as JSON")?;

    Ok(json
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected schema JSON to be a string"))?
        .as_bytes()
        .to_vec())
}

impl CsvColumns {
    fn new(reader: &mut CsvReader, schema: &Zval, schema_json: Vec<u8>, batch_size: usize) -> Result<Self, PhpException> {
        let definitions = call_method(schema, "definitions", &mut [])?;
        let definitions = definitions
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
        let mut columns = Vec::with_capacity(definitions.len());

        ht_for_each(definitions, |name, index, definition| {
            let key = match name {
                Some(name) => Key::Name(name.as_bytes().to_vec()),
                None => Key::Index(index as i64),
            };
            let plan = type_plan(&call_method(definition, "type", &mut [])?)?;

            columns.push(Column {
                field: reader.field_of(&key.name()),
                key,
                definition: definition.shallow_clone(),
                nullable: definition_nullable(definition)?,
                values: KindBuilder::new(&plan.kind),
                plan,
                refusal: None,
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

    fn reset(&mut self) {
        self.rows = 0;

        for column in &mut self.columns {
            column.values = KindBuilder::new(&column.plan.kind);
            column.refusal = None;
        }
    }
}

/// A cell `cast.rs` casts natively, appended in its physical form; `false` hands it to the PHP lane.
fn native_cell(values: &mut KindBuilder, plan: &TypePlan, bytes: &[u8]) -> Result<bool, PhpException> {
    match (&plan.kind, &plan.cast) {
        (Kind::Int64, CastKind::Integer) => Ok(integer_from_str(bytes).map(|value| values.append_fixed(&value.to_le_bytes())).is_some()),
        (Kind::Float64, CastKind::Float) => Ok(float_from_str(bytes).map(|value| values.append_fixed(&value.to_le_bytes())).is_some()),
        (Kind::Boolean, CastKind::Boolean) => Ok(bool_from_str(bytes).map(|value| values.append_bool(value)).is_some()),
        (Kind::Bytes, CastKind::String) => values.append_bytes(bytes).map(|()| true).map_err(overflow),
        (Kind::Bytes, CastKind::NonEmptyString) if !bytes.is_empty() => values.append_bytes(bytes).map(|()| true).map_err(overflow),
        (Kind::Bytes, CastKind::Json) if json_gate(bytes) && json_valid(bytes) => {
            values.append_bytes(bytes).map(|()| true).map_err(overflow)
        }
        (Kind::Uuid, CastKind::Uuid) if is_uuid(bytes) => {
            Ok(uuid_bytes(bytes).map(|uuid| values.append_fixed(&uuid)).is_some())
        }
        (Kind::Timestamp, CastKind::DateTime(zone)) => Ok(match iso_instant_micros(bytes) {
            Some(micros) => Some(micros),
            None => parse_iso_datetime(bytes, zone)?.and_then(|(_, instant)| instant.object().and_then(datetime_micros)),
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

/// One cell into its column: a NOT NULL null or a PHP-lane `Flow\Types` refusal becomes the column's refusal, any
/// other exception escapes as thrown.
fn append_cell(column: &mut Column, row: usize, cell: Option<&[u8]>) -> Result<(), PhpException> {
    let Some(bytes) = cell else {
        if !column.nullable {
            column.refusal = Some((row, value_does_not_match(&column.definition, null_zval(), None)?));

            return Ok(());
        }

        column.values.append_null();

        return Ok(());
    };

    if native_cell(&mut column.values, &column.plan, bytes)? {
        return Ok(());
    }

    let value = zval_str(bytes);

    match php_lane(&column.plan, &value) {
        Ok(physical) => append_physical(&mut column.values, &column.plan.kind, &physical),
        Err(Refusal::Cast(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            column.refusal = Some((row, value_does_not_match(&column.definition, value, None)?));

            Ok(())
        }
        Err(Refusal::Physical(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            let mut reason = Zval::new();
            reason.set_object(&mut exception);
            column.refusal = Some((row, value_does_not_match(&column.definition, value, Some(reason))?));

            Ok(())
        }
    }
}

fn finish_batch(state: &mut CsvColumns) -> Result<Zval, PhpException> {
    let refusal = state
        .columns
        .iter()
        .filter_map(|column| column.refusal.as_ref())
        .fold(None::<&(usize, Zval)>, |lowest, refusal| match lowest {
            Some(lowest) if lowest.0 <= refusal.0 => Some(lowest),
            _ => Some(refusal),
        });

    if let Some((row, cause)) = refusal {
        return Err(schema_mismatch(zval_long(*row as i64), cause.shallow_clone()));
    }

    if let Some(absent) = state.columns.iter().find(|column| column.field.is_none() && !column.nullable) {
        let cause = call_static(
            "Flow\\ETL\\Exception\\ColumnMismatchException",
            "missingColumn",
            &mut [absent.definition.shallow_clone()],
        )?;

        return Err(schema_mismatch(zval_long(0), cause));
    }

    let mut columns = ZendHashTable::with_capacity(state.columns.len() as u32);

    for column in &state.columns {
        let data = column
            .values
            .finish(&column.plan.kind)
            .map_err(|e| invalid_argument(format!("flow_php built an invalid column: {e}")))?;
        let mut native = Zval::new();
        ext_php_rs::convert::IntoZval::set_zval(
            NativeColumn::new(arrow_array::make_array(data), Rc::clone(&column.plan)),
            &mut native,
            false,
        )?;

        match &column.key {
            Key::Index(index) => ht_insert_long(&mut columns, *index, native),
            Key::Name(name) => ht_insert(&mut columns, name, native),
        }
    }

    let mut columns_zv = Zval::new();
    columns_zv.set_hashtable(columns);

    call_static(
        "Flow\\ETL\\Rows",
        "fromColumns",
        &mut [state.schema.shallow_clone(), columns_zv, zval_long(state.rows as i64)],
    )
}

pub fn next_columns(
    reader: &mut CsvReader,
    pending: &mut Option<CsvColumns>,
    schema: &Zval,
    batch_size: usize,
) -> Result<Zval, PhpException> {
    if reader.headers().is_empty() {
        return Ok(null_zval());
    }

    let same_object = pending.as_ref().is_some_and(|state| {
        state.schema.object().map(|object| object.handle) == schema.object().map(|object| object.handle)
    });

    if !same_object {
        let json = schema_json(schema)?;

        match pending {
            Some(state) if state.schema_json == json => state.schema = schema.shallow_clone(),
            Some(state) if state.rows > 0 => {
                return Err(ext_exception("flow_php cannot change the schema of a pending CSV batch"));
            }
            _ => *pending = Some(CsvColumns::new(reader, schema, json, batch_size)?),
        }
    }

    let state = pending.as_mut().expect("built above");

    if state.batch_size != batch_size {
        if state.rows > 0 {
            return Err(ext_exception("flow_php cannot change the batch size of a pending CSV batch"));
        }

        state.batch_size = batch_size;
    }

    while state.rows < state.batch_size && reader.next_record() {
        let row = state.rows;

        for column in &mut state.columns {
            if column.refusal.is_some() {
                continue;
            }

            let Some(field) = column.field else {
                column.values.append_null();

                continue;
            };

            if let Err(e) = append_cell(column, row, reader.cell(field)) {
                state.reset();

                return Err(e);
            }
        }

        state.rows += 1;
    }

    if state.rows == 0 || (state.rows < state.batch_size && !reader.is_finished()) {
        return Ok(null_zval());
    }

    let batch = finish_batch(state);
    state.reset();

    batch
}
