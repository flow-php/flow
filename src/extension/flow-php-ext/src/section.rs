use std::collections::HashMap;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};

use crate::ctx::{
    call_handle, call_handle_on, ce_method_ref, find_class, property_offset, zval_null, zval_str,
    Ctx,
};
use crate::encode::{build_encode_plan, expect_object, ht_for_each, read_slot, EncodePlan};
use crate::exception::ext_exception;

struct Tracking {
    columns: HashMap<Vec<u8>, Vec<u8>>,
}

const CONSTANT_NORMALIZE_TYPES: [&str; 18] = [
    "Flow\\Types\\Type\\Native\\IntegerType",
    "Flow\\Types\\Type\\Logical\\PositiveIntegerType",
    "Flow\\Types\\Type\\Native\\FloatType",
    "Flow\\Types\\Type\\Native\\BooleanType",
    "Flow\\Types\\Type\\Native\\StringType",
    "Flow\\Types\\Type\\Logical\\NonEmptyStringType",
    "Flow\\Types\\Type\\Logical\\NumericStringType",
    "Flow\\Types\\Type\\Logical\\ScalarType",
    "Flow\\Types\\Type\\Logical\\DateTimeType",
    "Flow\\Types\\Type\\Logical\\DateType",
    "Flow\\Types\\Type\\Logical\\TimeType",
    "Flow\\Types\\Type\\Logical\\HTMLType",
    "Flow\\Types\\Type\\Logical\\HTMLElementType",
    "Flow\\Types\\Type\\Logical\\TimeZoneType",
    "Flow\\Types\\Type\\Logical\\UuidType",
    "Flow\\Types\\Type\\Logical\\JsonType",
    "Flow\\Types\\Type\\Logical\\XMLType",
    "Flow\\Types\\Type\\Logical\\XMLElementType",
];

#[derive(Default)]
struct FingerprintCache {
    entry_definition_slots: HashMap<usize, u32>,
    definition_type_slots: HashMap<usize, u32>,
    fingerprints: HashMap<usize, Option<Vec<u8>>>,
}

/// `json_encode($type->normalize())`.
fn compute_fingerprint(type_obj: &ZendObject, ctx: &mut Ctx) -> Result<Vec<u8>, PhpException> {
    let type_ce = unsafe { type_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve a type class"))?;

    let normalized = call_handle_on(
        ce_method_ref(type_ce, "normalize")?,
        type_obj,
        &mut [],
        "normalize a type",
    )?;

    let json = call_handle(
        ctx.json_encode()?,
        None,
        &mut [normalized],
        "encode a type fingerprint",
    )?;

    Ok(json
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected json_encode to return a string"))?
        .as_bytes()
        .to_vec())
}

fn fingerprint_matches(
    entry: &ZendObject,
    expected: &[u8],
    ctx: &mut Ctx,
    cache: &mut FingerprintCache,
) -> Result<bool, PhpException> {
    let entry_ce_ptr = entry.ce as usize;
    let definition_slot = match cache.entry_definition_slots.get(&entry_ce_ptr) {
        Some(slot) => *slot,
        None => {
            let entry_ce = unsafe { entry.ce.as_ref() }
                .ok_or_else(|| ext_exception("flow_php failed to resolve an entry class"))?;
            let slot = property_offset(entry_ce, "definition")?;
            cache.entry_definition_slots.insert(entry_ce_ptr, slot);
            slot
        }
    };

    let def_obj = expect_object(read_slot(entry, definition_slot), "an entry definition")?;
    let def_ce_ptr = def_obj.ce as usize;
    let type_slot = match cache.definition_type_slots.get(&def_ce_ptr) {
        Some(slot) => *slot,
        None => {
            let def_ce = unsafe { def_obj.ce.as_ref() }
                .ok_or_else(|| ext_exception("flow_php failed to resolve a definition class"))?;
            let slot = property_offset(def_ce, "type")?;
            cache.definition_type_slots.insert(def_ce_ptr, slot);
            slot
        }
    };

    let type_obj = expect_object(read_slot(def_obj, type_slot), "a definition type")?;
    let type_ce_ptr = type_obj.ce as usize;

    if let Some(cached) = cache.fingerprints.get(&type_ce_ptr) {
        return match cached {
            Some(fingerprint) => Ok(fingerprint.as_slice() == expected),
            None => Ok(compute_fingerprint(type_obj, ctx)?.as_slice() == expected),
        };
    }

    let type_ce = unsafe { type_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve a type class"))?;
    let constant = type_ce
        .name()
        .is_some_and(|name| CONSTANT_NORMALIZE_TYPES.contains(&name));
    let fingerprint = compute_fingerprint(type_obj, ctx)?;
    let matches = fingerprint.as_slice() == expected;

    cache
        .fingerprints
        .insert(type_ce_ptr, constant.then_some(fingerprint));

    Ok(matches)
}

/// Mirrors `SchemaTracker::fits`: a narrower row still fits; a new column or an
/// incompatible type forces a new section.
fn fits(
    tracking: &Tracking,
    entries_ht: &ZendHashTable,
    ctx: &mut Ctx,
    cache: &mut FingerprintCache,
) -> Result<bool, PhpException> {
    let mut fits = true;

    ht_for_each(entries_ht, |key, index, entry_zv| {
        if !fits {
            return Ok(());
        }

        let index_name;
        let name: &[u8] = match key {
            Some(name) => name.as_bytes(),
            None => {
                index_name = (index as i64).to_string();
                index_name.as_bytes()
            }
        };

        match tracking.columns.get(name) {
            None => fits = false,
            Some(expected) => {
                let entry = expect_object(entry_zv, "a row entry")?;

                if !fingerprint_matches(entry, expected, ctx, cache)? {
                    fits = false;
                }
            }
        }

        Ok(())
    })?;

    Ok(fits)
}

/// Fingerprints come straight from `EncoderColumn::typeFingerprint`, the same
/// value `fits` compares against, so parity with `SchemaTracker::fits` holds.
fn build_tracking(plan_obj: &ZendObject, ctx: &mut Ctx) -> Result<Tracking, PhpException> {
    let columns_slot = ctx.encoder_plan_slots()?.columns;
    let (name_slot, fingerprint_slot) = {
        let slots = ctx.encoder_column_slots()?;
        (slots.name, slots.type_fingerprint)
    };

    let columns_ht = read_slot(plan_obj, columns_slot)
        .array()
        .ok_or_else(|| ext_exception("flow_php expected EncoderPlan::columns to be an array"))?;

    let mut columns = HashMap::with_capacity(columns_ht.len());
    ht_for_each(columns_ht, |_, _, column_zv| {
        let column = expect_object(column_zv, "an EncoderColumn")?;

        let name = read_slot(column, name_slot)
            .zend_str()
            .ok_or_else(|| ext_exception("flow_php expected EncoderColumn::name to be a string"))?
            .as_bytes()
            .to_vec();
        let fingerprint = read_slot(column, fingerprint_slot)
            .zend_str()
            .ok_or_else(|| {
                ext_exception("flow_php expected EncoderColumn::typeFingerprint to be a string")
            })?
            .as_bytes()
            .to_vec();

        columns.insert(name, fingerprint);

        Ok(())
    })?;

    Ok(Tracking { columns })
}

pub struct SectionTracker {
    tracking: Option<Tracking>,
    current_schema_body: Option<Vec<u8>>,
    plan: Option<EncodePlan>,
    fingerprints: FingerprintCache,
}

impl SectionTracker {
    pub fn new() -> Self {
        Self {
            tracking: None,
            current_schema_body: None,
            plan: None,
            fingerprints: FingerprintCache::default(),
        }
    }

    /// Reads a Row's entries hashtable through the cached slot offsets.
    pub fn row_entries<'a>(row_zv: &'a Zval, ctx: &Ctx) -> Result<&'a ZendHashTable, PhpException> {
        let row_obj = expect_object(row_zv, "a Row")?;
        let entries_obj = expect_object(read_slot(row_obj, ctx.row_entries_slot), "Row entries")?;

        read_slot(entries_obj, ctx.entries_entries_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Entries to hold an array"))
    }

    /// Returns the new section's SCHEMA frame body when the row does not fit,
    /// grown via `FloeWriter::growSectionPlan`; None when the row rides the section.
    pub fn ensure_section(
        &mut self,
        row_zv: &Zval,
        entries_ht: &ZendHashTable,
        ctx: &mut Ctx,
    ) -> Result<Option<Vec<u8>>, PhpException> {
        let changed = match &self.tracking {
            None => true,
            Some(current) => !fits(current, entries_ht, ctx, &mut self.fingerprints)?,
        };

        if !changed {
            return Ok(None);
        }

        let grow_fn = ce_method_ref(find_class("Flow\\Floe\\FloeWriter")?, "growSectionPlan")?;
        let body_arg = match &self.current_schema_body {
            Some(body) => zval_str(body),
            None => zval_null(),
        };

        let plan_zv = call_handle(
            grow_fn,
            None,
            &mut [body_arg, row_zv.shallow_clone()],
            "grow a section plan",
        )?;

        let plan_obj = expect_object(&plan_zv, "an EncoderPlan")?;
        let schema_body = read_slot(plan_obj, ctx.encoder_plan_slots()?.schema_body)
            .zend_str()
            .ok_or_else(|| {
                ext_exception("flow_php expected EncoderPlan::schemaBody to be a string")
            })?
            .as_bytes()
            .to_vec();

        self.tracking = Some(build_tracking(plan_obj, ctx)?);
        self.current_schema_body = Some(schema_body.clone());
        self.plan = Some(build_encode_plan(&schema_body)?);

        Ok(Some(schema_body))
    }

    pub fn plan_mut(&mut self) -> Result<&mut EncodePlan, PhpException> {
        self.plan
            .as_mut()
            .ok_or_else(|| ext_exception("flow_php has no active encode plan"))
    }
}
