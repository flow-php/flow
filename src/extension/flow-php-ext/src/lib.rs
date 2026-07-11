mod ctx;
mod encode;
mod exception;
mod format;
mod hydrate;
mod plan;
mod section;
mod values;

use std::alloc::System;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendObject, Zval};
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};

use crate::ctx::{call_handle_on, zval_long, zval_null, zval_str, Ctx};
use crate::encode::{encode_row_body, expect_object, ht_for_each, read_slot, EncodePlan};
use crate::exception::ext_exception;
use crate::format::{write_frame, Reader, FRAME_ROW};
use crate::plan::Plan;
use crate::section::SectionTracker;

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

/// Stateful frame decoder for streaming Floe reads (`FloeReader` fast path):
/// the PHP side keeps buffering/framing and hands over bare frame bodies.
#[php_class]
#[php(name = "Flow\\Floe\\RowsDecoder")]
pub struct RowsDecoder {
    ctx: Ctx,
    plan: Option<Plan>,
}

#[php_impl]
impl RowsDecoder {
    pub fn __construct() -> PhpResult<Self> {
        Ok(Self {
            ctx: Ctx::new()?,
            plan: None,
        })
    }

    /// Replaces the current column plan with the given SCHEMA frame body.
    pub fn schema(&mut self, frame_body: BinarySlice<u8>) -> PhpResult<()> {
        self.plan = Some(plan::build_plan(&frame_body, &mut self.ctx)?);

        Ok(())
    }

    /// Decodes one ROW frame body against the current plan into a `Flow\ETL\Row`.
    pub fn row(&mut self, frame_body: BinarySlice<u8>) -> PhpResult<Zval> {
        let Some(plan) = self.plan.as_ref() else {
            return Err(ext_exception(
                "flow_php found a row frame before any schema frame",
            ));
        };

        let mut reader = Reader::new(&frame_body);
        let row = hydrate::hydrate_row(plan, &mut reader, &mut self.ctx)?;

        if !reader.is_eof() {
            return Err(ext_exception(
                "flow_php row frame length does not match its content",
            ));
        }

        row.into_zval(false)
            .map_err(|e| ext_exception(format!("flow_php failed to return a Row: {e:?}")))
    }

    /// Decodes a list of ROW frame bodies against the current plan into one
    /// `Flow\ETL\Rows`, graph-identical to calling `row()` per body.
    pub fn rows(&mut self, frame_bodies: &Zval) -> PhpResult<Zval> {
        let bodies_ht = frame_bodies
            .array()
            .ok_or_else(|| ext_exception("flow_php expected a list of row frame bodies"))?;

        let Some(plan) = self.plan.as_ref() else {
            return Err(ext_exception(
                "flow_php found a row frame before any schema frame",
            ));
        };
        let ctx = &mut self.ctx;

        let mut rows = Vec::with_capacity(bodies_ht.len());

        ht_for_each(bodies_ht, |_, _, body_zv| {
            let bytes = body_zv
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected a row frame body to be a string"))?
                .as_bytes();

            let mut reader = Reader::new(bytes);
            let row = hydrate::hydrate_row(plan, &mut reader, ctx)?;

            if !reader.is_eof() {
                return Err(ext_exception(
                    "flow_php row frame length does not match its content",
                ));
            }

            rows.push(row);

            Ok(())
        })?;

        hydrate::build_rows(rows, ctx)
    }
}

/// Stateful frame encoder for streaming Floe writes (`FloeWriter` fast path).
/// The `rows()` segment state is independent from the `schema()`/`row()`
/// per-frame state; the two modes must not be mixed on one instance.
#[php_class]
#[php(name = "Flow\\Floe\\RowsEncoder")]
pub struct RowsEncoder {
    ctx: Ctx,
    plan: Option<EncodePlan>,
    stream: SectionTracker,
}

#[php_impl]
impl RowsEncoder {
    pub fn __construct() -> PhpResult<Self> {
        Ok(Self {
            ctx: Ctx::new()?,
            plan: None,
            stream: SectionTracker::new(),
        })
    }

    /// Primes the encode plan from a SCHEMA frame body.
    pub fn schema(&mut self, frame_body: BinarySlice<u8>) -> PhpResult<()> {
        self.plan = Some(encode::build_encode_plan(&frame_body)?);

        Ok(())
    }

    /// Encodes one `Flow\ETL\Row` into a bare ROW frame body against the plan.
    pub fn row(&mut self, row: &Zval) -> PhpResult<Zval> {
        let Some(plan) = self.plan.as_mut() else {
            return Err(ext_exception(
                "flow_php found a row before any schema frame",
            ));
        };

        Ok(zval_str(&encode::encode_row_body(
            plan,
            row,
            &mut self.ctx,
        )?))
    }

    /// Encodes a whole `Flow\ETL\Rows` into a list of `Flow\Floe\FrameSegment`
    /// in one call, mirroring `Flow\Floe\PhpRowFrameEncoder`.
    pub fn rows(&mut self, rows: &Zval) -> PhpResult<Zval> {
        struct Segment {
            schema_body: Option<Vec<u8>>,
            frames: Vec<u8>,
            row_count: i64,
        }

        let rows_obj = expect_object(rows, "Rows")?;
        let rows_ht = read_slot(rows_obj, self.ctx.rows_rows_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Rows to hold an array"))?;

        let ctx = &mut self.ctx;
        let stream = &mut self.stream;

        let mut segments: Vec<Segment> = Vec::new();
        let mut current: Option<Segment> = None;

        ht_for_each(rows_ht, |_, _, row_zv| {
            let entries_ht = SectionTracker::row_entries(row_zv, ctx)?;

            if let Some(schema_body) = stream.ensure_section(row_zv, entries_ht, ctx)? {
                if let Some(segment) = current.take() {
                    segments.push(segment);
                }

                current = Some(Segment {
                    schema_body: Some(schema_body),
                    frames: Vec::new(),
                    row_count: 0,
                });
            } else if current.is_none() {
                current = Some(Segment {
                    schema_body: None,
                    frames: Vec::new(),
                    row_count: 0,
                });
            }

            let body = encode_row_body(stream.plan_mut()?, row_zv, ctx)?;

            let segment = current
                .as_mut()
                .ok_or_else(|| ext_exception("flow_php has no active segment"))?;
            write_frame(&mut segment.frames, FRAME_ROW, &body);
            segment.row_count += 1;

            Ok(())
        })?;

        if let Some(segment) = current.take() {
            segments.push(segment);
        }

        let (segment_ce, segment_ctor) = ctx.frame_segment()?;
        let mut out = ext_php_rs::types::ZendHashTable::with_capacity(segments.len() as u32);

        for segment in segments {
            let schema_body_zv = match &segment.schema_body {
                Some(body) => zval_str(body),
                None => zval_null(),
            };

            let obj = ZendObject::new(segment_ce);
            call_handle_on(
                segment_ctor,
                &obj,
                &mut [
                    schema_body_zv,
                    zval_str(&segment.frames),
                    zval_long(segment.row_count),
                ],
                "construct a FrameSegment",
            )?;

            let obj_zv = obj.into_zval(false).map_err(|e| {
                ext_exception(format!("flow_php failed to return a FrameSegment: {e:?}"))
            })?;

            out.push(obj_zv).map_err(|e| {
                ext_exception(format!("flow_php failed to build a segment list: {e:?}"))
            })?;
        }

        let mut zv = Zval::new();
        zv.set_hashtable(out);

        Ok(zv)
    }
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .info_function(php_module_info)
        .class::<RowsDecoder>()
        .class::<RowsEncoder>()
}
