//! Object-graph assembly mirroring `Flow\ETL\Row\PhpRowHydrator` and
//! `EntryInstantiator`: constructor-less instantiation with direct
//! property-slot writes.

use std::collections::HashMap;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};

use ext_php_rs::zend::{ClassEntry, Function};

use crate::ctx::{
    array_key_index, call_handle, call_handle_on, call_handle_transparent, ce_method_ref,
    construct_with_zvals, find_class, ht_add, ht_find_key, ht_insert, ht_insert_key,
    property_offset, write_slot, zval_str, Ctx, HtKey,
};
use crate::encode::{expect_object, ht_for_each, read_slot};
use crate::exception::ext_exception;
use crate::format::{
    Reader, VALUE_ABSENT, VALUE_NULL, VALUE_NULL_WITH_META, VALUE_PRESENT, VALUE_PRESENT_WITH_META,
};
use crate::plan::Plan;
use crate::values::decode_value;

/// Resolved `Flow\ETL\Row\RawRowValues` class handles, shared by the binary decoder and the hydrator.
pub struct RowValuesClass {
    pub ce: &'static ClassEntry,
    pub values_slot: u32,
    pub metadata_slot: u32,
}

impl RowValuesClass {
    pub fn resolve() -> Result<Self, PhpException> {
        let ce = find_class("Flow\\ETL\\Row\\RawRowValues")?;

        Ok(Self {
            ce,
            values_slot: property_offset(ce, "values")?,
            metadata_slot: property_offset(ce, "metadata")?,
        })
    }
}

/// Reads one per-value metadata blob (`u32 len` + JSON) and rebuilds the
/// `Flow\ETL\Schema\Metadata` value object via the canonical PHP
/// `json_decode` + `Metadata::fromArray` - the rare diverging path only.
fn read_metadata(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let length = reader.u32("per-value metadata length")? as usize;
    let blob = reader.bytes(length, "per-value metadata")?;

    let json_zv = zval_str(blob);
    let mut assoc_zv = Zval::new();
    assoc_zv.set_bool(true);

    let map = {
        let json_decode = ctx.json_decode()?;
        call_handle(
            json_decode,
            None,
            &mut [json_zv.shallow_clone(), assoc_zv],
            "decode per-value metadata",
        )?
    };

    let from_array = ctx.metadata_from_array()?;
    call_handle(from_array, None, &mut [map], "build per-value metadata")
}

/// Decodes one ROW frame body into a `Flow\ETL\Row\RawRowValues` object - values keyed by
/// column name, absent columns omitted, diverging per-value metadata recorded in `metadata`.
pub fn decode_row_values(
    plan: &Plan,
    reader: &mut Reader,
    ctx: &mut Ctx,
    class: &RowValuesClass,
) -> Result<ZBox<ZendObject>, PhpException> {
    let mut values_ht = ZendHashTable::with_capacity(plan.columns.len() as u32);
    let mut metadata_ht = ZendHashTable::new();

    for column in &plan.columns {
        let flag = reader.u8("row value flag")?;

        if flag == VALUE_ABSENT {
            continue;
        }

        let value = match flag {
            VALUE_PRESENT => decode_value(&column.decoder, reader, ctx)?,
            VALUE_NULL => Zval::new(),
            VALUE_PRESENT_WITH_META => {
                let metadata = read_metadata(reader, ctx)?;
                ht_insert(&mut metadata_ht, column.name.as_bytes(), metadata);

                decode_value(&column.decoder, reader, ctx)?
            }
            VALUE_NULL_WITH_META => {
                let metadata = read_metadata(reader, ctx)?;
                ht_insert(&mut metadata_ht, column.name.as_bytes(), metadata);

                Zval::new()
            }
            other => {
                return Err(ext_exception(format!(
                    "flow_php found unknown value flag 0x{other:02X}"
                )));
            }
        };

        if !ht_add(&mut values_ht, column.name.as_bytes(), value) {
            return Err(ext_exception(format!(
                "flow_php found duplicated entry name \"{}\" in a row frame",
                column.name
            )));
        }
    }

    let mut row_values = ZendObject::new(class.ce);

    let mut values_zv = Zval::new();
    values_zv.set_hashtable(values_ht);
    write_slot(&mut row_values, class.values_slot, values_zv);

    let mut metadata_zv = Zval::new();
    metadata_zv.set_hashtable(metadata_ht);
    write_slot(&mut row_values, class.metadata_slot, metadata_zv);

    Ok(row_values)
}

/// Resolved property-table slot offsets for the `Rows -> Row -> Entries` graph
/// traversed by `dehydrate`.
pub struct RowsClasses {
    pub rows_slot: u32,
    pub row_entries_slot: u32,
    pub entries_slot: u32,
}

impl RowsClasses {
    pub fn resolve() -> Result<Self, PhpException> {
        Ok(Self {
            rows_slot: property_offset(find_class("Flow\\ETL\\Rows")?, "rows")?,
            row_entries_slot: property_offset(find_class("Flow\\ETL\\Row")?, "entries")?,
            entries_slot: property_offset(find_class("Flow\\ETL\\Row\\Entries")?, "entries")?,
        })
    }
}

/// Resolved `Flow\ETL\Row\TypedRowValues` class handle + its three slot offsets.
pub struct TypedRowValuesClass {
    pub ce: &'static ClassEntry,
    pub values_slot: u32,
    pub types_slot: u32,
    pub metadata_slot: u32,
}

impl TypedRowValuesClass {
    pub fn resolve() -> Result<Self, PhpException> {
        let ce = find_class("Flow\\ETL\\Row\\TypedRowValues")?;

        Ok(Self {
            ce,
            values_slot: property_offset(ce, "values")?,
            types_slot: property_offset(ce, "types")?,
            metadata_slot: property_offset(ce, "metadata")?,
        })
    }
}

/// `(name, value, definition)` slot offsets cached per entry class - entry classes
/// declare these three properties in differing orders, so offsets are per-class.
pub type EntrySlotCache = HashMap<usize, (u32, u32, u32)>;

/// `(type(), metadata())` method handles cached per definition class.
pub type DefFnCache = HashMap<usize, (&'static Function, &'static Function)>;

pub(crate) fn entry_slots(
    cache: &mut EntrySlotCache,
    ce: &ClassEntry,
) -> Result<(u32, u32, u32), PhpException> {
    let key = std::ptr::from_ref(ce) as usize;

    if let Some(slots) = cache.get(&key) {
        return Ok(*slots);
    }

    let slots = (
        property_offset(ce, "name")?,
        property_offset(ce, "value")?,
        property_offset(ce, "definition")?,
    );
    cache.insert(key, slots);

    Ok(slots)
}

pub(crate) fn def_dehydrate_fns(
    cache: &mut DefFnCache,
    ce: &ClassEntry,
) -> Result<(&'static Function, &'static Function), PhpException> {
    let key = std::ptr::from_ref(ce) as usize;

    if let Some(fns) = cache.get(&key) {
        return Ok(*fns);
    }

    let fns = (ce_method_ref(ce, "type")?, ce_method_ref(ce, "metadata")?);
    cache.insert(key, fns);

    Ok(fns)
}

/// Mirrors `PhpRowHydrator::dehydrate` for one row: collects each entry's value
/// (verbatim), `definition()->type()`, and `definition()->metadata()` (only when
/// non-empty), then builds a constructor-less `TypedRowValues`.
fn dehydrate_row(
    entries_ht: &ZendHashTable,
    class: &TypedRowValuesClass,
    entry_slot_cache: &mut EntrySlotCache,
    def_fn_cache: &mut DefFnCache,
    metadata_map_slot: u32,
) -> Result<ZBox<ZendObject>, PhpException> {
    let mut values_ht = ZendHashTable::with_capacity(entries_ht.len() as u32);
    let mut types_ht = ZendHashTable::with_capacity(entries_ht.len() as u32);
    let mut metadata_ht = ZendHashTable::new();

    ht_for_each(entries_ht, |_, _, entry_zv| {
        let entry = expect_object(entry_zv, "a row entry")?;
        let entry_ce = unsafe { entry.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve an entry class"))?;

        let (name_slot, value_slot, definition_slot) = entry_slots(entry_slot_cache, entry_ce)?;

        let name = HtKey::from_zend_str(
            read_slot(entry, name_slot)
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected an entry name to be a string"))?,
        );

        let value = read_slot(entry, value_slot).shallow_clone();

        let definition = read_slot(entry, definition_slot)
            .object()
            .ok_or_else(|| ext_exception("flow_php expected an entry definition to be an object"))?;
        let definition_ce = unsafe { definition.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a definition class"))?;

        let (type_fn, metadata_fn) = def_dehydrate_fns(def_fn_cache, definition_ce)?;

        let type_zv = call_handle_on(type_fn, definition, &mut [], "read a definition type")?;
        let metadata_zv =
            call_handle_on(metadata_fn, definition, &mut [], "read definition metadata")?;

        ht_insert_key(&mut values_ht, &name, value);
        ht_insert_key(&mut types_ht, &name, type_zv);

        let metadata = expect_object(&metadata_zv, "definition metadata")?;
        let non_empty = read_slot(metadata, metadata_map_slot)
            .array()
            .is_some_and(|map| !map.is_empty());

        if non_empty {
            ht_insert_key(&mut metadata_ht, &name, metadata_zv);
        }

        Ok(())
    })?;

    let mut row_values = ZendObject::new(class.ce);

    let mut values_zv = Zval::new();
    values_zv.set_hashtable(values_ht);
    write_slot(&mut row_values, class.values_slot, values_zv);

    let mut types_zv = Zval::new();
    types_zv.set_hashtable(types_ht);
    write_slot(&mut row_values, class.types_slot, types_zv);

    let mut metadata_zv = Zval::new();
    metadata_zv.set_hashtable(metadata_ht);
    write_slot(&mut row_values, class.metadata_slot, metadata_zv);

    Ok(row_values)
}

/// Native `PhpRowHydrator::dehydrate`: turns a `Flow\ETL\Rows` into a list of
/// `Flow\ETL\Row\TypedRowValues`, value zvals moved verbatim (no casting).
pub fn dehydrate_rows(
    rows: &Zval,
    rows_classes: &RowsClasses,
    class: &TypedRowValuesClass,
    entry_slot_cache: &mut EntrySlotCache,
    def_fn_cache: &mut DefFnCache,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let rows_obj = expect_object(rows, "Rows")?;
    let rows_ht = read_slot(rows_obj, rows_classes.rows_slot)
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Rows::rows to be an array"))?;

    let metadata_map_slot = ctx.metadata_map_slot()?;

    let mut out = ZendHashTable::with_capacity(rows_ht.len() as u32);

    ht_for_each(rows_ht, |_, _, row_zv| {
        let row = expect_object(row_zv, "a Row")?;
        let entries = read_slot(row, rows_classes.row_entries_slot)
            .object()
            .ok_or_else(|| ext_exception("flow_php expected Row::entries to be an object"))?;
        let entries_ht = read_slot(entries, rows_classes.entries_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Entries::entries to be an array"))?;

        let row_values = dehydrate_row(
            entries_ht,
            class,
            entry_slot_cache,
            def_fn_cache,
            metadata_map_slot,
        )?;

        out.push(row_values)
            .map_err(|e| ext_exception(format!("flow_php failed to collect row values: {e:?}")))?;

        Ok(())
    })?;

    let mut zv = Zval::new();
    zv.set_hashtable(out);

    Ok(zv)
}

/// `Entries::recreate` handle + the `Row`/`Rows` class entries used to assemble
/// a hydrated `Rows` through the canonical PHP constructors/factory.
pub struct AssemblyClasses {
    pub entries_recreate: &'static Function,
    pub row_ce: &'static ClassEntry,
    pub rows_ce: &'static ClassEntry,
}

impl AssemblyClasses {
    pub fn resolve() -> Result<Self, PhpException> {
        Ok(Self {
            entries_recreate: ce_method_ref(find_class("Flow\\ETL\\Row\\Entries")?, "recreate")?,
            row_ce: find_class("Flow\\ETL\\Row")?,
            rows_ce: find_class("Flow\\ETL\\Rows")?,
        })
    }
}

/// Definition method handles used only on the rare hydrate paths, cached per
/// definition class.
#[derive(Clone, Copy)]
pub struct DefRareFns {
    is_nullable: &'static Function,
    make_nullable: &'static Function,
    set_metadata: &'static Function,
    entry_class: &'static Function,
}

pub type DefRareFnCache = HashMap<usize, DefRareFns>;

fn def_rare_fns(cache: &mut DefRareFnCache, ce: &ClassEntry) -> Result<DefRareFns, PhpException> {
    let key = std::ptr::from_ref(ce) as usize;

    if let Some(fns) = cache.get(&key) {
        return Ok(*fns);
    }

    let fns = DefRareFns {
        is_nullable: ce_method_ref(ce, "isNullable")?,
        make_nullable: ce_method_ref(ce, "makeNullable")?,
        set_metadata: ce_method_ref(ce, "setMetadata")?,
        entry_class: ce_method_ref(ce, "entryClass")?,
    };
    cache.insert(key, fns);

    Ok(fns)
}

/// A per-column hydrate plan entry. `base_def` retains the actual schema
/// `Definition` object (refcount++), so the common path shares one instance
/// across every row - matching `PhpRowHydrator`'s object-sharing topology.
pub(crate) struct HydrateColumn {
    numeric_key: Option<i64>,
    pub(crate) name_zv: Zval,
    pub(crate) base_def: Zval,
    pub(crate) nullable: bool,
    pub(crate) entry_ce: &'static ClassEntry,
    pub(crate) entry_slots: (u32, u32, u32),
    /// A union column's concrete type is a per-row property, so `entry_ce` above - resolved
    /// once per column from the value-blind `entryClass()` - does not apply to it.
    pub(crate) member_for: Option<&'static Function>,
}

impl HydrateColumn {
    pub(crate) fn key(&self) -> HtKey<'_> {
        match self.numeric_key {
            Some(index) => HtKey::Index(index),
            None => HtKey::Str(self.name_zv.zend_str().expect("column names are strings")),
        }
    }
}

/// `Schema` is immutable - `add()`/`keep()`/`makeNullable()` return a new
/// instance carrying a freshly built `definitions` array - so the array's
/// address identifies the definition set; retaining it (refcount++) prevents
/// address reuse. Keying on the array rather than on `Schema` identity also
/// survives a caller that hands the same definitions to a new `Schema`.
/// Per-value `setMetadata` returns a new `Definition` and leaves the retained
/// ones untouched, so it needs no rebuild.
pub struct HydratePlan {
    pub(crate) definitions_slot: u32,
    pub(crate) definitions_retained: Zval,
    pub(crate) columns: Vec<HydrateColumn>,
}

fn entry_class_ce(
    fns: DefRareFns,
    def_obj: &ZendObject,
) -> Result<&'static ClassEntry, PhpException> {
    let entry_class = call_handle_on(fns.entry_class, def_obj, &mut [], "read an entry class")?;
    let name = entry_class
        .zend_str()
        .and_then(|s| std::str::from_utf8(s.as_bytes()).ok())
        .ok_or_else(|| ext_exception("flow_php expected entryClass to return a string"))?
        .to_string();

    find_class(&name)
}

/// Builds the per-column hydrate plan from a `Schema` object, reusing the actual
/// `Definition` zvals (no reconstruction in Rust). Method calls are fine here -
/// this runs once per schema, never per row.
pub(crate) fn build_hydrate_plan(
    schema: &Zval,
    entry_slot_cache: &mut EntrySlotCache,
) -> Result<HydratePlan, PhpException> {
    let schema_obj = expect_object(schema, "a Schema")?;
    let schema_ce = unsafe { schema_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve the Schema class"))?;

    let definitions_slot = property_offset(schema_ce, "definitions")?;
    let definitions_fn = ce_method_ref(schema_ce, "definitions")?;
    let definitions = call_handle_on(
        definitions_fn,
        schema_obj,
        &mut [],
        "read schema definitions",
    )?;
    let definitions_ht = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions to return an array"))?;

    let mut columns = Vec::with_capacity(definitions_ht.len());
    let union_ce = find_class("Flow\\ETL\\Schema\\Definition\\UnionDefinition")?;

    ht_for_each(definitions_ht, |_, _, def_zv| {
        let def_obj = expect_object(def_zv, "a Definition")?;
        let def_ce = unsafe { def_obj.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a Definition class"))?;

        let entry_fn = ce_method_ref(def_ce, "entry")?;
        let reference = call_handle_on(entry_fn, def_obj, &mut [], "read a definition reference")?;
        let reference_obj = expect_object(&reference, "a definition reference")?;
        let reference_ce = unsafe { reference_obj.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a Reference class"))?;
        let name_fn = ce_method_ref(reference_ce, "name")?;
        let name_zv = call_handle_on(name_fn, reference_obj, &mut [], "read a definition name")?;
        let numeric_key = array_key_index(
            name_zv
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected a definition name to be a string"))?
                .as_bytes(),
        );

        let is_nullable_fn = ce_method_ref(def_ce, "isNullable")?;
        let nullable = call_handle_on(is_nullable_fn, def_obj, &mut [], "read a definition nullability")?
            .bool()
            .ok_or_else(|| ext_exception("flow_php expected isNullable to return a bool"))?;

        let entry_class_fn = ce_method_ref(def_ce, "entryClass")?;
        let entry_class = call_handle_on(entry_class_fn, def_obj, &mut [], "read an entry class")?;
        let entry_class_name = entry_class
            .zend_str()
            .and_then(|s| std::str::from_utf8(s.as_bytes()).ok())
            .ok_or_else(|| ext_exception("flow_php expected entryClass to return a string"))?
            .to_string();
        let entry_ce = find_class(&entry_class_name)?;
        let entry_slots = entry_slots(entry_slot_cache, entry_ce)?;

        let member_for = if def_obj.instance_of(union_ce) {
            Some(ce_method_ref(def_ce, "memberFor")?)
        } else {
            None
        };

        columns.push(HydrateColumn {
            numeric_key,
            name_zv,
            base_def: def_zv.shallow_clone(),
            nullable,
            entry_ce,
            entry_slots,
            member_for,
        });

        Ok(())
    })?;

    Ok(HydratePlan {
        definitions_slot,
        definitions_retained: definitions,
        columns,
    })
}

/// Resolves the `(definition, entry class, entry slots)` triple for one column
/// occurrence, mirroring `PhpRowHydrator::instantiate` + `EntryFactory::fromDefinition`:
/// the common path shares the retained base `Definition`; a union column resolves its
/// member from the value; per-value metadata takes the new instance
/// `$def->setMetadata(...)` returns, and a null value on a non-nullable definition
/// produces a fresh `makeNullable()` variant.
pub(crate) fn resolve_entry_definition(
    column: &HydrateColumn,
    metadata: Option<&Zval>,
    value: Option<&Zval>,
    entry_slot_cache: &mut EntrySlotCache,
    def_rare_cache: &mut DefRareFnCache,
) -> Result<(Zval, &'static ClassEntry, (u32, u32, u32)), PhpException> {
    let value_is_null = value.is_none_or(Zval::is_null);

    if column.member_for.is_none() && metadata.is_none() && (!value_is_null || column.nullable) {
        return Ok((column.base_def.shallow_clone(), column.entry_ce, column.entry_slots));
    }

    let resolved_def = match column.member_for {
        Some(member_for) => {
            let union_obj = column
                .base_def
                .object()
                .ok_or_else(|| ext_exception("flow_php expected a Definition object"))?;
            let mut args = [match value {
                Some(value) => value.shallow_clone(),
                None => {
                    let mut null = Zval::new();
                    null.set_null();
                    null
                }
            }];

            call_handle_transparent(member_for, Some(union_obj), &mut args)?
        }
        None => column.base_def.shallow_clone(),
    };

    let base_obj = resolved_def
        .object()
        .ok_or_else(|| ext_exception("flow_php expected a Definition object"))?;

    let variant = if let Some(metadata) = metadata {
        let base_ce = unsafe { base_obj.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a Definition class"))?;
        let fns = def_rare_fns(def_rare_cache, base_ce)?;
        call_handle_on(
            fns.set_metadata,
            base_obj,
            &mut [metadata.shallow_clone()],
            "set per-value metadata",
        )?
    } else {
        resolved_def.shallow_clone()
    };

    let variant_obj = variant
        .object()
        .ok_or_else(|| ext_exception("flow_php expected a Definition object"))?;
    let variant_ce = unsafe { variant_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve a Definition class"))?;
    let fns = def_rare_fns(def_rare_cache, variant_ce)?;

    let final_def = if value_is_null
        && !call_handle_on(fns.is_nullable, variant_obj, &mut [], "read nullability")?
            .bool()
            .unwrap_or(false)
    {
        call_handle_on(
            fns.make_nullable,
            variant_obj,
            &mut [],
            "make a definition nullable",
        )?
    } else {
        variant
    };

    let final_obj = final_def
        .object()
        .ok_or_else(|| ext_exception("flow_php expected a Definition object"))?;
    let final_ce = unsafe { final_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve a Definition class"))?;
    let final_fns = def_rare_fns(def_rare_cache, final_ce)?;
    let entry_ce = entry_class_ce(final_fns, final_obj)?;
    let entry_slots = entry_slots(entry_slot_cache, entry_ce)?;

    Ok((final_def, entry_ce, entry_slots))
}

pub(crate) fn ensure_hydrate_plan(
    plan: &mut Option<HydratePlan>,
    schema: &Zval,
    entry_slot_cache: &mut EntrySlotCache,
) -> Result<(), PhpException> {
    let schema_obj = expect_object(schema, "a Schema")?;

    let current = plan.as_ref().and_then(|p| {
        read_slot(schema_obj, p.definitions_slot)
            .array()
            .zip(p.definitions_retained.array())
    });

    if current.is_some_and(|(definitions, retained)| std::ptr::eq(definitions, retained)) {
        return Ok(());
    }

    *plan = Some(build_hydrate_plan(schema, entry_slot_cache)?);

    Ok(())
}

/// Native `PhpRowHydrator::hydrate`: builds `Flow\ETL\Rows` from a trusted list
/// of `RawRowValues` against a `Schema`. The common column path (present,
/// non-null-or-nullable, no per-value metadata) is a plain slot write reusing
/// the retained `Definition`; only the null-on-non-nullable and metadata-diverging
/// paths reconstruct a fresh variant, mirroring `EntryFactory::fromDefinition`.
pub fn hydrate_rows(
    batch: &Zval,
    schema: &Zval,
    plan_slot: &mut Option<HydratePlan>,
    raw_class: &RowValuesClass,
    assembly: &AssemblyClasses,
    entry_slot_cache: &mut EntrySlotCache,
    def_rare_cache: &mut DefRareFnCache,
) -> Result<Zval, PhpException> {
    ensure_hydrate_plan(plan_slot, schema, entry_slot_cache)?;
    let plan = plan_slot.as_ref().expect("plan built above");

    let batch_ht = batch
        .array()
        .ok_or_else(|| ext_exception("flow_php expected a list of raw row values"))?;

    let mut rows_args: Vec<Zval> = Vec::with_capacity(batch_ht.len());

    ht_for_each(batch_ht, |_, _, rv_zv| {
        let rv = expect_object(rv_zv, "a RawRowValues")?;
        let values_ht = read_slot(rv, raw_class.values_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected RawRowValues::values to be an array"))?;
        let metadata_ht = read_slot(rv, raw_class.metadata_slot).array().ok_or_else(|| {
            ext_exception("flow_php expected RawRowValues::metadata to be an array")
        })?;

        let mut entries_ht = ZendHashTable::with_capacity(plan.columns.len() as u32);
        let has_metadata = !metadata_ht.is_empty();

        for column in &plan.columns {
            let key = column.key();

            let Some(value) = ht_find_key(values_ht, &key) else {
                continue;
            };

            let metadata = if has_metadata {
                ht_find_key(metadata_ht, &key)
            } else {
                None
            };
            let (definition, entry_ce, entry_slots) = resolve_entry_definition(
                column,
                metadata,
                Some(value),
                entry_slot_cache,
                def_rare_cache,
            )?;

            let mut entry = ZendObject::new(entry_ce);
            write_slot(&mut entry, entry_slots.0, column.name_zv.shallow_clone());
            write_slot(&mut entry, entry_slots.1, value.shallow_clone());
            write_slot(&mut entry, entry_slots.2, definition);

            let mut entry_zv = Zval::new();
            entry_zv.set_object(&mut entry);
            ht_insert_key(&mut entries_ht, &key, entry_zv);
        }

        let mut entries_zv = Zval::new();
        entries_zv.set_hashtable(entries_ht);
        let entries = call_handle(
            assembly.entries_recreate,
            None,
            &mut [entries_zv],
            "recreate row entries",
        )?;

        let mut row = construct_with_zvals(assembly.row_ce, &mut [entries], "a Row")?;
        let mut row_zv = Zval::new();
        row_zv.set_object(&mut row);
        rows_args.push(row_zv);

        Ok(())
    })?;

    let mut rows = construct_with_zvals(assembly.rows_ce, &mut rows_args, "Rows")?;
    let mut zv = Zval::new();
    zv.set_object(&mut rows);

    Ok(zv)
}
