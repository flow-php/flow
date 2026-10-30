//! `Flow\ETL\Values\Column\RustBackend`: every column is a `RustColumn`.

// a parameter's Rust name is its PHP name, and `Backend::decode()` names it `$nullCount`
#![allow(non_snake_case)]

use std::rc::Rc;

use arrow_array::{make_array, ArrayRef};

use ext_php_rs::binary::Binary;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::error::Result as ZvalResult;
use ext_php_rs::flags::{ClassFlags, DataType};
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};
use flow_batch_frame::column::decode;
use flow_batch_frame::frame;
use flow_batch_frame::kind::Kind;
use flow_batch_frame::layout::buffer_count;

use crate::batch_columns::HeldBatch;
use crate::builder::{throw, RustColumnBuilder};
use crate::column::RustColumn;
use crate::ctx::{
    call_handle_catching, call_method, call_static, ce_method_ref, column_key, construct, expect_object, find_class,
    ht_for_each, ht_get, ht_insert, transparent_exception, zval_long, zval_str,
};
use crate::exception::ext_exception;
use crate::frame::{string_of, FrameBody, PhpCodec};
use crate::interfaces::{backend_ce, describable_ce};
use crate::plan::{physical_for_definition, type_plan, TypePlan};
use crate::render::{decode_exception, encode_exception, floe_exception, frame_exception, invalid_argument};

const COLUMN_MISMATCH: &str = "Flow\\ETL\\Foundation\\Exception\\ColumnMismatchException";
const DESCRIPTION: &str = "Flow\\ETL\\Foundation\\Describe\\Description";
const MIXED_TYPE: &str = "Flow\\Types\\Type\\Native\\MixedType";
const ROWS: &str = "Flow\\ETL\\Values\\Rows";
const RUST_BACKEND: &str = "Flow\\ETL\\Values\\Column\\RustBackend";
const SCHEMA_MISMATCH: &str = "Flow\\ETL\\Foundation\\Exception\\SchemaMismatchException";

/// A return typed `Flow\ETL\Values\Column\Column`: `adopt()` hands back its argument itself.
pub struct ColumnValue(pub Zval);

impl IntoZval for ColumnValue {
    const TYPE: DataType = DataType::object("Flow\\ETL\\Values\\Column\\Column");
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

/// A return typed `Flow\ETL\Values\Rows`.
pub struct RowsValue(pub Zval);

impl IntoZval for RowsValue {
    const TYPE: DataType = DataType::object("Flow\\ETL\\Values\\Rows");
    const NULLABLE: bool = false;

    fn set_zval(self, zv: &mut Zval, _persistent: bool) -> ZvalResult<()> {
        *zv = self.0;

        Ok(())
    }
}

fn definition_plan(definition: &Zval) -> PhpResult<Rc<TypePlan>> {
    type_plan(&call_method(definition, "type", &mut [])?)
}

/// One schema column: the key `Rows` holds it by (an int key is a numeric name), its Definition and its type's plan.
pub struct SchemaColumn {
    pub key: Vec<u8>,
    pub definition: Zval,
    pub plan: Rc<TypePlan>,
}

/// `Schema::definitions()` resolved once, in order.
pub struct Definitions {
    pub columns: Vec<SchemaColumn>,
}

impl Definitions {
    pub fn of(schema: &Zval) -> PhpResult<Self> {
        let definitions = call_method(schema, "definitions", &mut [])?;
        let definitions = definitions
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
        let mut columns = Vec::with_capacity(definitions.len());

        ht_for_each(definitions, |name, index, definition| {
            columns.push(SchemaColumn {
                key: column_key(name, index),
                definition: definition.shallow_clone(),
                plan: definition_plan(definition)?,
            });

            Ok(())
        })?;

        Ok(Self { columns })
    }

    /// The (key, plan) pairs the Arrow C Data check reads.
    pub fn plans(&self) -> Vec<(Vec<u8>, Rc<TypePlan>)> {
        self.columns
            .iter()
            .map(|column| (column.key.clone(), Rc::clone(&column.plan)))
            .collect()
    }
}

fn rows(count: i64, name: &str) -> PhpResult<u32> {
    u32::try_from(count).map_err(|_| invalid_argument(format!("flow_php {name} must be between 0 and {}", u32::MAX)))
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Column\\RustBackend",
    flags = ClassFlags::Final,
    implements(ce = backend_ce, stub = "Flow\\ETL\\Values\\Column\\Backend"),
    implements(ce = describable_ce, stub = "Flow\\ETL\\Foundation\\Describe\\Describable")
)]
pub struct RustBackend;

#[php_impl]
impl RustBackend {
    pub fn __construct() -> Self {
        RustBackend
    }

    #[php(name = "fromDescription")]
    pub fn from_description(_data: &Zval, _rebuild: &Zval) -> Self {
        RustBackend
    }

    /// `new Description(RustBackend::class, [])`: a worker rebuilds the same lane, as `PhpBackend` describes itself.
    pub fn describe(&self) -> PhpResult<Zval> {
        let mut data = Zval::new();
        data.set_hashtable(ZendHashTable::new());

        Ok(construct(find_class(DESCRIPTION)?, &mut [zval_str(RUST_BACKEND.as_bytes()), data])?.into_zval(false)?)
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
            let name = call_method(definition, "name", &mut [])?;

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
                &mut [call_method(definition, "name", &mut [])?],
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

    /// One native call per Floe BATCH frame: the directory is walked in Rust and every raw, 8-byte aligned buffer is an
    /// Arrow buffer into `$body` itself, which stays alive for as long as the columns do. `$codec` is called for the
    /// buffers stored with a length prefix only.
    #[php(name = "decodeFrame")]
    pub fn decode_frame(&self, body: &Zval, schema: &Zval, codec: &Zval) -> PhpResult<RowsValue> {
        let (string, bytes) =
            string_of(body).ok_or_else(|| invalid_argument("flow_php frame body must be a string".to_string()))?;
        let definitions = Definitions::of(schema)?;
        let kinds = definitions
            .columns
            .iter()
            .map(|column| &column.plan.kind)
            .collect::<Vec<&Kind>>();
        let owner = FrameBody::retain(string);
        let frame = frame::decode(bytes, owner.as_ref(), &kinds, &mut PhpCodec(codec))
            .map_err(|failure| frame_exception(failure, &definitions))?;
        let mut columns = ZendHashTable::with_capacity(definitions.columns.len() as u32);

        for (column, data) in definitions.columns.iter().zip(frame.columns) {
            ht_insert(
                &mut columns,
                &column.key,
                RustColumn::new(make_array(data), Rc::clone(&column.plan)).into_zval(false)?,
            );
        }

        match rows_from_columns(schema, columns, frame.rows as usize) {
            Ok(rows) => Ok(RowsValue(rows)),
            Err(NoRows::Thrown(exception)) if exception.instance_of(find_class(SCHEMA_MISMATCH)?) => {
                let previous = exception.into_zval(false)?;
                let message = call_method(&previous, "getMessage", &mut [])?;

                Err(floe_exception(
                    format!(
                        "Floe BATCH frame is malformed: {}",
                        String::from_utf8_lossy(message.zend_str().map_or(&b""[..], |message| message.as_bytes()))
                    ),
                    previous,
                ))
            }
            Err(no_rows) => Err(no_rows.surfaced()),
        }
    }

    #[php(name = "encodeFrame")]
    pub fn encode_frame(&self, rows: &Zval, codec: &Zval) -> PhpResult<Binary<u8>> {
        let schema = call_method(rows, "schema", &mut [])?;
        let definitions = Definitions::of(&schema)?;
        let count = call_method(rows, "count", &mut [])?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))?;
        let mut held = Vec::with_capacity(definitions.columns.len());

        for column in &definitions.columns {
            let value = call_method(rows, "column", &mut [zval_str(&column.key)])?;
            held.push(native_column(&column.definition, &value)?);
        }

        let data = held.iter().map(|(_, array)| array.to_data()).collect::<Vec<_>>();
        let columns = data
            .iter()
            .zip(&held)
            .map(|(data, (plan, _))| (data, &plan.kind))
            .collect::<Vec<_>>();

        Ok(Binary::from(
            frame::encode(&columns, count as u32, &mut PhpCodec(codec)).map_err(encode_exception)?,
        ))
    }

    #[php(name = "allocatedBytes")]
    pub fn allocated_bytes(&self) -> i64 {
        crate::alloc::allocated_bytes()
    }
}

/// The schema and backend every batch of one source is adopted under.
pub struct Adoption {
    schema: Zval,
    backend: Zval,
    definitions: Definitions,
}

impl Adoption {
    pub fn new(definitions: Definitions, schema: &Zval, backend: &Zval) -> Self {
        Self {
            schema: schema.shallow_clone(),
            backend: backend.shallow_clone(),
            definitions,
        }
    }
}

/// What `Rows::fromColumns()` left instead of rows: the exception it threw, or why it could not be called.
enum NoRows {
    Thrown(ZBox<ZendObject>),
    Unreachable(PhpException),
}

impl NoRows {
    fn surfaced(self) -> PhpException {
        match self {
            NoRows::Thrown(mut exception) => transparent_exception(&mut exception),
            NoRows::Unreachable(e) => e,
        }
    }
}

/// `Rows::fromColumns($schema, $columns, $count)`.
fn rows_from_columns(schema: &Zval, columns: ZBox<ZendHashTable>, count: usize) -> Result<Zval, NoRows> {
    let mut columns_zv = Zval::new();
    columns_zv.set_hashtable(columns);
    let from_columns = find_class(ROWS)
        .and_then(|ce| ce_method_ref(ce, "fromColumns"))
        .map_err(NoRows::Unreachable)?;

    call_handle_catching(
        from_columns,
        None,
        &mut [schema.shallow_clone(), columns_zv, zval_long(count as i64)],
    )
    .map_err(NoRows::Thrown)
}

/// `held` as `Rows` with every column adopted by the backend - what a Rust open source yields: the configured backend
/// keeps its own columns and copies the native ones.
pub fn adopt_batch(adoption: &Adoption, held: HeldBatch) -> PhpResult<Zval> {
    let mut columns = ZendHashTable::with_capacity(adoption.definitions.columns.len() as u32);

    for SchemaColumn { key, definition, .. } in &adoption.definitions.columns {
        let column = ht_get(&held.columns, key).ok_or_else(|| {
            ext_exception(format!(
                "flow_php expected the batch to hold the column \"{}\"",
                String::from_utf8_lossy(key)
            ))
        })?;
        ht_insert(
            &mut columns,
            key,
            call_method(
                &adoption.backend,
                "adopt",
                &mut [definition.shallow_clone(), column.shallow_clone()],
            )?,
        );
    }

    rows_from_columns(&adoption.schema, columns, held.count).map_err(NoRows::surfaced)
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
