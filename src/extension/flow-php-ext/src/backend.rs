//! `Flow\ETL\Values\Column\RustBackend`: every column is a `RustColumn`.

// a parameter's Rust name is its PHP name, and `Backend::decode()` names it `$nullCount`
#![allow(non_snake_case)]

use std::rc::Rc;
use std::sync::Arc;

use ahash::AHashMap;

use arrow_array::{make_array, ArrayRef, BooleanArray, LargeBinaryArray, UInt32Array};
use arrow_select::take::take;

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
use flow_column_kernels::arithmetic::Operator;
use flow_column_kernels::canonical::{md5_row_lanes, row_lanes, sha256_row_lanes};
use flow_column_kernels::compare::{contains, ends_with, is_not_null, is_null, starts_with};
use flow_column_kernels::grouping::bucket_of;

use crate::arithmetic::{native_arithmetic, native_modulo};
use crate::batch_columns::HeldBatch;
use crate::builder::{throw, RustColumnBuilder};
use crate::canonical_rows::{named, named_column, ordered, row_columns, STRING_TYPE};
use crate::column::RustColumn;
use crate::compare::{boolean_column, native_comparison, native_text_match};
use crate::ctx::{
    call_handle_catching, call_method, call_static, ce_method_ref, column_key, construct, expect_object, find_class,
    ht_for_each, ht_get, ht_insert, ht_insert_long, instance, read_property, transparent_exception, zval_long,
    zval_str,
};
use crate::exception::ext_exception;
use crate::frame::{string_of, FrameBody, PhpCodec};
use crate::group_table::RustGroupTable;
use crate::grouped_counts::RustGroupedCounts;
use crate::grouped_totals::RustGroupedTotals;
use crate::interfaces::{backend_ce, describable_ce};
use crate::multiset_hashes::{RowLanes, RustGroupedMultisetHashes};
use crate::plan::{physical_for_definition, type_plan, TypePlan};
use crate::render::{decode_exception, encode_exception, floe_exception, frame_exception, invalid_argument};
use crate::sort::{handed_off_native_bytes, native_sort, native_sort_working_bytes};

const COLUMN_MISMATCH: &str = "Flow\\ETL\\Foundation\\Exception\\ColumnMismatchException";
const DESCRIPTION: &str = "Flow\\ETL\\Foundation\\Describe\\Description";
const DEFINITIONS: &str = "Flow\\ETL\\Values\\Schema\\Definitions";
const MIXED_TYPE: &str = "Flow\\Types\\Type\\Native\\MixedType";
const ROWS: &str = "Flow\\ETL\\Values\\Rows";
const PHP_BACKEND: &str = "Flow\\ETL\\Values\\Column\\PhpBackend";
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
        let count = rows(count, "constant count")?;
        let mut builder = self.builder(definition)?;
        let pending = builder.pending(value)?;

        builder.append_pending(pending)?;
        let one = builder.finish_column()?;
        // one native gather of row 0, not `count` appends
        let repeated = take(one.data().as_ref(), &UInt32Array::from(vec![0; count as usize]), None)
            .map_err(|e| invalid_argument(format!("flow_php failed to repeat a constant: {e}")))?;

        Ok(RustColumn::new(repeated, Rc::clone(one.plan())))
    }

    pub fn compare(
        &self,
        left: &Zval,
        operator: &Zval,
        right: &Zval,
        leftName: &Zval,
        rightName: &Zval,
    ) -> PhpResult<ColumnValue> {
        match native_comparison(left, operator, right)? {
            Some(compared) => Ok(ColumnValue(compared.into_zval(false)?)),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                "compare",
                &mut [
                    left.shallow_clone(),
                    operator.shallow_clone(),
                    right.shallow_clone(),
                    leftName.shallow_clone(),
                    rightName.shallow_clone(),
                ],
            )?),
        }
    }

    pub fn add(&self, left: &Zval, right: &Zval) -> PhpResult<ColumnValue> {
        self.arithmetic(left, Operator::Add, right, "add")
    }

    pub fn subtract(&self, left: &Zval, right: &Zval) -> PhpResult<ColumnValue> {
        self.arithmetic(left, Operator::Subtract, right, "subtract")
    }

    pub fn multiply(&self, left: &Zval, right: &Zval) -> PhpResult<ColumnValue> {
        self.arithmetic(left, Operator::Multiply, right, "multiply")
    }

    pub fn modulo(&self, left: &Zval, right: &Zval) -> PhpResult<ColumnValue> {
        match native_modulo(left, right)? {
            Some(result) => Ok(ColumnValue(result.into_zval(false)?)),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                "modulo",
                &mut [left.shallow_clone(), right.shallow_clone()],
            )?),
        }
    }

    #[php(name = "startsWith")]
    pub fn starts_with(&self, haystack: &Zval, needle: &Zval) -> PhpResult<ColumnValue> {
        self.text_match(haystack, needle, starts_with, "startsWith")
    }

    #[php(name = "endsWith")]
    pub fn ends_with(&self, haystack: &Zval, needle: &Zval) -> PhpResult<ColumnValue> {
        self.text_match(haystack, needle, ends_with, "endsWith")
    }

    pub fn contains(&self, haystack: &Zval, needle: &Zval) -> PhpResult<ColumnValue> {
        self.text_match(haystack, needle, contains, "contains")
    }

    pub fn sort(&self, rows: &Zval, columns: &Zval) -> PhpResult<RowsValue> {
        match native_sort(rows, columns)? {
            Some(sorted) => Ok(RowsValue(sorted)),
            None => Ok(RowsValue(call_method(
                &instance(PHP_BACKEND)?,
                "sort",
                &mut [rows.shallow_clone(), columns.shallow_clone()],
            )?)),
        }
    }

    #[php(name = "sortWorkingBytes")]
    pub fn sort_working_bytes(&self, rows: &Zval, columns: &Zval) -> PhpResult<i64> {
        match native_sort_working_bytes(rows, columns)? {
            Some(bytes) => Ok(bytes),
            None => Ok(handed_off_native_bytes(rows, columns)?
                + call_method(
                    &instance(PHP_BACKEND)?,
                    "sortWorkingBytes",
                    &mut [rows.shallow_clone(), columns.shallow_clone()],
                )?
                .long()
                .ok_or_else(|| ext_exception("flow_php expected PhpBackend::sortWorkingBytes() to return an int"))?),
        }
    }

    #[php(name = "groupedTotals")]
    pub fn grouped_totals(&self) -> RustGroupedTotals {
        RustGroupedTotals::default()
    }

    #[php(name = "groupedCounts")]
    pub fn grouped_counts(&self) -> RustGroupedCounts {
        RustGroupedCounts::default()
    }

    #[php(name = "rowHashes")]
    pub fn row_hashes(&self, columns: &ZendHashTable, count: i64) -> PhpResult<RustColumn> {
        let resolved = row_columns(columns)?;
        let count = usize::try_from(count).map_err(|_| ext_exception("flow_php expected a row count of 0 or more"))?;
        let mut row = Vec::new();
        let mut entries = Vec::new();
        let hashes = (0..count)
            .map(|index| {
                named(&mut row, &mut entries, &resolved, index);
                let [low, high] = row_lanes(&row);

                format!("{high:016x}{low:016x}")
            })
            .collect::<Vec<_>>();

        RustColumn::of_type(Arc::new(LargeBinaryArray::from_iter_values(hashes)), STRING_TYPE)
    }

    #[php(name = "groupedMultisetHashes")]
    pub fn grouped_multiset_hashes(&self, algorithm: &Zval) -> PhpResult<RustGroupedMultisetHashes> {
        let name = read_property(expect_object(algorithm, "a MultisetHashAlgorithm")?, "value")?;
        let row_lanes: RowLanes = match name.zend_str().map(|name| name.as_bytes()) {
            Some(b"xxh128") => row_lanes,
            Some(b"md5") => md5_row_lanes,
            Some(b"sha256") => sha256_row_lanes,
            unknown => {
                return Err(ext_exception(format!(
                    "flow_php does not know the multiset hash algorithm \"{}\", update the extension",
                    String::from_utf8_lossy(unknown.unwrap_or_default())
                )))
            }
        };

        Ok(RustGroupedMultisetHashes::of(row_lanes))
    }

    #[php(name = "groupTable")]
    pub fn group_table(&self, columns: &ZendHashTable, declared: &Zval) -> PhpResult<RustGroupTable> {
        RustGroupTable::new(columns, declared)
    }

    pub fn partition(
        &self,
        rows: &Zval,
        keys: &ZendHashTable,
        buckets: i64,
        hashOffset: i64,
    ) -> PhpResult<ZBox<ZendHashTable>> {
        let bucket_count = u64::try_from(buckets)
            .ok()
            .filter(|buckets| *buckets >= 1)
            .ok_or_else(|| invalid_argument(format!("Buckets count must be greater than 0, given: {buckets}")))?;
        let hash_offset = u32::try_from(hashOffset)
            .ok()
            .filter(|offset| *offset <= 24)
            .ok_or_else(|| invalid_argument(format!("Hash offset must be between 0 and 24, given: {hashOffset}")))?;
        let count = call_method(rows, "count", &mut [])?
            .long()
            .and_then(|count| usize::try_from(count).ok())
            .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))?;
        let mut parts = ZendHashTable::new();

        if count == 0 {
            return Ok(parts);
        }

        if bucket_count == 1 {
            ht_insert_long(&mut parts, 0, rows.shallow_clone());

            return Ok(parts);
        }

        let columns = keys
            .values()
            .map(|column| named_column(Vec::new(), column))
            .collect::<PhpResult<Vec<_>>>()?;
        let mut slots = AHashMap::<u64, usize>::new();
        let mut indices: Vec<(u64, Vec<i64>)> = Vec::new();
        let mut key = Vec::new();
        let mut entries = Vec::new();

        for row in 0..count {
            ordered(&mut key, &mut entries, &columns, row);
            let bucket = bucket_of(&key, hash_offset, bucket_count);
            let slot = *slots.entry(bucket).or_insert_with(|| {
                indices.push((bucket, Vec::new()));

                indices.len() - 1
            });
            indices[slot].1.push(row as i64);
        }

        for (bucket, rows_of_bucket) in indices {
            let mut list = ZendHashTable::with_capacity(rows_of_bucket.len() as u32);

            for row in rows_of_bucket {
                list.push(row)?;
            }

            let mut list_zv = Zval::new();
            list_zv.set_hashtable(list);
            ht_insert_long(&mut parts, bucket as i64, call_method(rows, "gather", &mut [list_zv])?);
        }

        Ok(parts)
    }

    #[php(name = "isNull")]
    pub fn is_null(&self, column: &Zval) -> PhpResult<ColumnValue> {
        match column.extract::<&RustColumn>() {
            Some(native) => Ok(ColumnValue(boolean_column(is_null(native.data()))?.into_zval(false)?)),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                "isNull",
                &mut [column.shallow_clone()],
            )?),
        }
    }

    #[php(name = "isNotNull")]
    pub fn is_not_null(&self, column: &Zval) -> PhpResult<ColumnValue> {
        match column.extract::<&RustColumn>() {
            Some(native) => Ok(ColumnValue(
                boolean_column(is_not_null(native.data()))?.into_zval(false)?,
            )),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                "isNotNull",
                &mut [column.shallow_clone()],
            )?),
        }
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

    /// One native call per Floe BATCH frame; aligned raw buffers point into `$body`, `$codec` decodes prefixed ones.
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
        crate::alloc::own_bytes()
    }

    #[php(name = "allocatedPeakBytes")]
    pub fn allocated_peak_bytes(&self) -> i64 {
        crate::alloc::own_peak_bytes()
    }

    #[php(name = "resetAllocatedPeak")]
    pub fn reset_allocated_peak(&self) {
        crate::alloc::reset_peak();
    }

    #[php(name = "releaseFreedMemory")]
    pub fn release_freed_memory(&self) {
        crate::alloc::release_freed();
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
pub enum NoRows {
    Thrown(ZBox<ZendObject>),
    Unreachable(PhpException),
}

impl NoRows {
    pub fn surfaced(self) -> PhpException {
        match self {
            NoRows::Thrown(mut exception) => transparent_exception(&mut exception),
            NoRows::Unreachable(e) => e,
        }
    }
}

/// `Rows::fromColumns($schema, $columns, $count)`.
pub fn rows_from_columns(schema: &Zval, columns: ZBox<ZendHashTable>, count: usize) -> Result<Zval, NoRows> {
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

impl RustBackend {
    pub(crate) fn adopt_result(&self, column: Zval) -> PhpResult<ColumnValue> {
        let definition = call_static(
            DEFINITIONS,
            "of",
            &mut [
                zval_str(b"value"),
                call_method(&column, "type", &mut [])?,
                Zval::from(true),
            ],
        )?;

        self.adopt(&definition, &column)
    }

    fn text_match(
        &self,
        haystack: &Zval,
        needle: &Zval,
        kernel: fn(&LargeBinaryArray, &LargeBinaryArray) -> BooleanArray,
        method: &str,
    ) -> PhpResult<ColumnValue> {
        match native_text_match(haystack, needle, kernel)? {
            Some(matched) => Ok(ColumnValue(matched.into_zval(false)?)),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                method,
                &mut [haystack.shallow_clone(), needle.shallow_clone()],
            )?),
        }
    }

    fn arithmetic(&self, left: &Zval, operator: Operator, right: &Zval, method: &str) -> PhpResult<ColumnValue> {
        match native_arithmetic(left, operator, right)? {
            Some(result) => Ok(ColumnValue(result.into_zval(false)?)),
            None => self.adopt_result(call_method(
                &instance(PHP_BACKEND)?,
                method,
                &mut [left.shallow_clone(), right.shallow_clone()],
            )?),
        }
    }
}
