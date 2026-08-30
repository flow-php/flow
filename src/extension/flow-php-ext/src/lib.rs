mod cast;
mod ctx;
mod encode;
mod exception;
mod format;
mod hydrate;
mod plan;
mod values;

use std::alloc::System;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};

use crate::ctx::{zval_str, Ctx};
use crate::encode::{build_encode_plan, encode_typed_row, expect_object, ht_for_each, read_slot, EncodePlan};
use crate::exception::ext_exception;
use crate::format::Reader;
use crate::plan::Plan;

#[global_allocator]
static GLOBAL: System = System;

pub extern "C" fn php_module_info(_module: *mut ModuleEntry) {
    info_table_start!();
    info_table_row!("flow_php.enabled", "true");
    info_table_row!("flow_php.extension_version", env!("FLOW_PHP_EXT_VERSION"));
    info_table_end!();
}

/// # Safety
///
/// Only called by the PHP engine during module startup (MINIT).
pub unsafe extern "C" fn module_startup(_type: i32, _module_number: i32) -> i32 {
    if let Err(e) = exception::register() {
        eprintln!("flow_php: failed to register Flow\\Floe\\Exception\\ExtensionException: {e}");
        return -1;
    }
    0
}

/// A built plan together with the schema JSON it was built from; the schema
/// bytes decide when a new schema requires a plan rebuild. Generic over the plan
/// kind so encode (`EncodePlan`) and decode (`Plan`) share one rebind path.
struct BoundPlan<P> {
    schema: Vec<u8>,
    plan: P,
}

/// Rebuilds the bound plan when the schema JSON changed, `Ctx` caches survive rebinds.
fn ensure_plan<P>(
    bound: &mut Option<BoundPlan<P>>,
    ctx: &mut Ctx,
    schema: &[u8],
    build: fn(&[u8], &mut Ctx) -> PhpResult<P>,
) -> PhpResult<()> {
    if bound.as_ref().is_some_and(|b| b.schema == schema) {
        return Ok(());
    }

    *bound = Some(BoundPlan {
        schema: schema.to_vec(),
        plan: build(schema, ctx)?,
    });

    Ok(())
}

/// Floe's consolidated binary codec, both directions: ROW frame bodies to/from
/// `Flow\ETL\Row\RawRowValues` (decode) and `Flow\ETL\Row\TypedRowValues` (encode).
/// The PHP side keeps buffering/framing/sectioning and hands over bare frame
/// bodies; the extension owns value encode/decode against a primed schema.
#[php_class]
#[php(name = "Flow\\Floe\\RustFloeEncoderNative")]
pub struct RustFloeEncoderNative {
    ctx: Ctx,
    encode_bound: Option<BoundPlan<EncodePlan>>,
    decode_bound: Option<BoundPlan<Plan>>,
    row_values_class: hydrate::RowValuesClass,
}

#[php_impl]
impl RustFloeEncoderNative {
    pub fn __construct() -> PhpResult<Self> {
        Ok(Self {
            ctx: Ctx::new()?,
            encode_bound: None,
            decode_bound: None,
            row_values_class: hydrate::RowValuesClass::resolve()?,
        })
    }

    /// Encodes a list of `Flow\ETL\Row\TypedRowValues` into a list of bare ROW
    /// frame bodies against the plan bound to `schema_body`.
    pub fn encode(&mut self, batch: &Zval, schema_body: BinarySlice<u8>) -> PhpResult<Zval> {
        ensure_plan(
            &mut self.encode_bound,
            &mut self.ctx,
            &schema_body,
            build_encode_plan,
        )?;

        let batch_ht = batch
            .array()
            .ok_or_else(|| ext_exception("flow_php expected a list of typed row values"))?;

        let (values_slot, metadata_slot) = self.ctx.typed_row_values_slots()?;

        let plan = &self.encode_bound.as_ref().expect("plan bound above").plan;
        let ctx = &mut self.ctx;

        let mut encoded = ZendHashTable::with_capacity(batch_ht.len() as u32);

        ht_for_each(batch_ht, |_, _, item_zv| {
            let typed = expect_object(item_zv, "a TypedRowValues")?;

            let values_ht = read_slot(typed, values_slot).array().ok_or_else(|| {
                ext_exception("flow_php expected TypedRowValues::values to be an array")
            })?;
            let metadata_ht = read_slot(typed, metadata_slot).array().ok_or_else(|| {
                ext_exception("flow_php expected TypedRowValues::metadata to be an array")
            })?;

            let body = encode_typed_row(plan, values_ht, metadata_ht, ctx)?;

            encoded.push(zval_str(&body)).map_err(|e| {
                ext_exception(format!("flow_php failed to collect a row body: {e:?}"))
            })?;

            Ok(())
        })?;

        let mut zv = Zval::new();
        zv.set_hashtable(encoded);

        Ok(zv)
    }

    /// Decodes a list of ROW frame bodies written with the given SCHEMA frame
    /// body into `Flow\ETL\Row\RawRowValues` objects.
    pub fn decode(&mut self, frame_bodies: &Zval, schema_body: BinarySlice<u8>) -> PhpResult<Zval> {
        ensure_plan(
            &mut self.decode_bound,
            &mut self.ctx,
            &schema_body,
            plan::build_plan,
        )?;

        let bodies_ht = frame_bodies
            .array()
            .ok_or_else(|| ext_exception("flow_php expected a list of row frame bodies"))?;

        let plan = &self.decode_bound.as_ref().expect("plan bound above").plan;
        let class = &self.row_values_class;
        let ctx = &mut self.ctx;

        let mut decoded = ZendHashTable::with_capacity(bodies_ht.len() as u32);

        ht_for_each(bodies_ht, |_, _, body_zv| {
            let bytes = body_zv
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected a row frame body to be a string"))?
                .as_bytes();

            let mut reader = Reader::new(bytes);
            let row_values = hydrate::decode_row_values(plan, &mut reader, ctx, class)?;

            if !reader.is_eof() {
                return Err(ext_exception(
                    "flow_php row frame length does not match its content",
                ));
            }

            decoded.push(row_values).map_err(|e| {
                ext_exception(format!("flow_php failed to collect row values: {e:?}"))
            })?;

            Ok(())
        })?;

        let mut zv = Zval::new();
        zv.set_hashtable(decoded);

        Ok(zv)
    }
}

/// Native counterpart of `PhpRowHydrator`: `hydrate` builds `Flow\ETL\Rows` from
/// a trusted list of `RawRowValues` against a `Schema` object (values moved
/// verbatim); `cast` does the same from RAW scalars, casting each value against
/// the schema first; `dehydrate` turns `Rows` into a list of `TypedRowValues`.
#[php_class]
#[php(name = "Flow\\ETL\\Row\\RustRowHydratorNative")]
pub struct RustRowHydratorNative {
    ctx: Ctx,
    rows_classes: hydrate::RowsClasses,
    typed_row_values_class: hydrate::TypedRowValuesClass,
    raw_row_values_class: hydrate::RowValuesClass,
    assembly: hydrate::AssemblyClasses,
    def_dehydrate_fns: hydrate::DefFnCache,
    hydrate_plan: Option<hydrate::HydratePlan>,
    cast_plan: Option<cast::CastPlan>,
}

#[php_impl]
impl RustRowHydratorNative {
    pub fn __construct() -> PhpResult<Self> {
        Ok(Self {
            ctx: Ctx::new()?,
            rows_classes: hydrate::RowsClasses::resolve()?,
            typed_row_values_class: hydrate::TypedRowValuesClass::resolve()?,
            raw_row_values_class: hydrate::RowValuesClass::resolve()?,
            assembly: hydrate::AssemblyClasses::resolve()?,
            def_dehydrate_fns: hydrate::DefFnCache::new(),
            hydrate_plan: None,
            cast_plan: None,
        })
    }

    /// Casts a list of raw-scalar `RawRowValues` against a `Schema` and builds
    /// a `Flow\ETL\Rows` in a single pass.
    pub fn cast(&mut self, batch: &Zval, schema: &Zval) -> PhpResult<Zval> {
        cast::cast_rows(
            batch,
            schema,
            &mut self.cast_plan,
            &self.raw_row_values_class,
            &self.assembly,
            &mut self.ctx,
        )
    }

    /// Builds a `Flow\ETL\Rows` from a trusted list of `RawRowValues` against a `Schema`.
    pub fn hydrate(&mut self, batch: &Zval, schema: &Zval) -> PhpResult<Zval> {
        hydrate::hydrate_rows(
            batch,
            schema,
            &mut self.hydrate_plan,
            &self.raw_row_values_class,
            &self.assembly,
            &mut self.ctx,
        )
    }

    /// Turns a `Flow\ETL\Rows` into a list of `Flow\ETL\Row\TypedRowValues`.
    pub fn dehydrate(&mut self, rows: &Zval) -> PhpResult<Zval> {
        hydrate::dehydrate_rows(
            rows,
            &self.rows_classes,
            &self.typed_row_values_class,
            &mut self.def_dehydrate_fns,
            &mut self.ctx,
        )
    }
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .info_function(php_module_info)
        .class::<RustFloeEncoderNative>()
        .class::<RustRowHydratorNative>()
}
