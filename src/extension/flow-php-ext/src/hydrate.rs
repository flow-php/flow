//! Object-graph assembly mirroring `Flow\ETL\Row\PhpRowHydrator` and
//! `EntryInstantiator`: constructor-less instantiation with direct
//! property-slot writes.

use std::collections::HashMap;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};

use ext_php_rs::zend::{ClassEntry, Function};

use crate::ctx::{
    array_key_index, call_handle, call_handle_on, ce_method_ref,
    find_class, ht_add, ht_find_key, ht_insert, ht_insert_key,
    property_offset, write_slot, zval_str, Ctx, HtKey,
};
use crate::encode::{expect_object, ht_for_each, read_slot};
use crate::exception::ext_exception;
use crate::format::{
    Reader, VALUE_NULL, VALUE_NULL_WITH_META, VALUE_PRESENT, VALUE_PRESENT_WITH_META,
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

/// Resolved property-table slot offsets for the `Rows -> Schema` / `Rows -> Row`
/// graph traversed by `dehydrate`. The Schema carries the column types now, so
/// they are read once per batch instead of once per cell.
pub struct RowsClasses {
    pub rows_slot: u32,
    pub schema_slot: u32,
    pub row_values_slot: u32,
    pub definitions_slot: u32,
}

impl RowsClasses {
    pub fn resolve() -> Result<Self, PhpException> {
        let rows_ce = find_class("Flow\\ETL\\Rows")?;

        Ok(Self {
            rows_slot: property_offset(rows_ce, "rows")?,
            schema_slot: property_offset(rows_ce, "schema")?,
            row_values_slot: property_offset(find_class("Flow\\ETL\\Row")?, "values")?,
            definitions_slot: property_offset(find_class("Flow\\ETL\\Schema")?, "definitions")?,
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

/// `(type(), metadata())` method handles cached per definition class.
pub type DefFnCache = HashMap<usize, (&'static Function, &'static Function)>;

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

/// One column of a batch's dehydrate plan: the key, and the `type()` /
/// `metadata()` zvals read once from the batch Schema.
struct DehydrateColumn {
    numeric_key: Option<i64>,
    name_zv: Zval,
    type_zv: Zval,
    metadata_zv: Option<Zval>,
}

impl DehydrateColumn {
    fn key(&self) -> HtKey<'_> {
        match self.numeric_key {
            Some(index) => HtKey::Index(index),
            None => HtKey::Str(self.name_zv.zend_str().expect("column names are strings")),
        }
    }
}

/// Mirrors `PhpRowHydrator::dehydrate` for one row: values come from
/// `Row::values`, types and metadata from the batch Schema.
fn dehydrate_row(
    values_ht: &ZendHashTable,
    columns: &[DehydrateColumn],
    class: &TypedRowValuesClass,
) -> Result<ZBox<ZendObject>, PhpException> {
    let mut values_ht_out = ZendHashTable::with_capacity(columns.len() as u32);
    let mut types_ht = ZendHashTable::with_capacity(columns.len() as u32);
    let mut metadata_ht = ZendHashTable::new();

    for column in columns {
        let key = column.key();

        let Some(value) = ht_find_key(values_ht, &key) else {
            return Err(ext_exception(format!(
                "flow_php found a row that does not carry the declared column \"{}\"",
                column.name_zv.str().unwrap_or("?")
            )));
        };

        ht_insert_key(&mut values_ht_out, &key, value.shallow_clone());
        ht_insert_key(&mut types_ht, &key, column.type_zv.shallow_clone());

        if let Some(metadata) = &column.metadata_zv {
            ht_insert_key(&mut metadata_ht, &key, metadata.shallow_clone());
        }
    }

    let mut row_values = ZendObject::new(class.ce);

    let mut values_zv = Zval::new();
    values_zv.set_hashtable(values_ht_out);
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
    def_fn_cache: &mut DefFnCache,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let rows_obj = expect_object(rows, "Rows")?;
    let rows_ht = read_slot(rows_obj, rows_classes.rows_slot)
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Rows::rows to be an array"))?;

    let schema = read_slot(rows_obj, rows_classes.schema_slot)
        .object()
        .ok_or_else(|| ext_exception("flow_php expected Rows::schema to be an object"))?;
    let definitions_ht = read_slot(schema, rows_classes.definitions_slot)
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions to be an array"))?;

    let metadata_map_slot = ctx.metadata_map_slot()?;

    // one type()/metadata() call per column per batch, not per cell
    let mut columns: Vec<DehydrateColumn> = Vec::with_capacity(definitions_ht.len());

    ht_for_each(definitions_ht, |_, _, def_zv| {
        let definition = expect_object(def_zv, "a Definition")?;
        let definition_ce = unsafe { definition.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a definition class"))?;

        let entry_fn = ce_method_ref(definition_ce, "entry")?;
        let reference = call_handle_on(entry_fn, definition, &mut [], "read a definition reference")?;
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

        let (type_fn, metadata_fn) = def_dehydrate_fns(def_fn_cache, definition_ce)?;

        let type_zv = call_handle_on(type_fn, definition, &mut [], "read a definition type")?;
        let metadata_zv =
            call_handle_on(metadata_fn, definition, &mut [], "read definition metadata")?;

        let metadata = expect_object(&metadata_zv, "definition metadata")?;
        let non_empty = read_slot(metadata, metadata_map_slot)
            .array()
            .is_some_and(|map| !map.is_empty());

        columns.push(DehydrateColumn {
            numeric_key,
            name_zv,
            type_zv,
            metadata_zv: if non_empty { Some(metadata_zv) } else { None },
        });

        Ok(())
    })?;

    let mut out = ZendHashTable::with_capacity(rows_ht.len() as u32);

    ht_for_each(rows_ht, |_, _, row_zv| {
        let row = expect_object(row_zv, "a Row")?;
        let values_ht = read_slot(row, rows_classes.row_values_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Row::values to be an array"))?;

        let row_values = dehydrate_row(values_ht, &columns, class)?;

        out.push(row_values)
            .map_err(|e| ext_exception(format!("flow_php failed to collect row values: {e:?}")))?;

        Ok(())
    })?;

    let mut zv = Zval::new();
    zv.set_hashtable(out);

    Ok(zv)
}

/// The `Row`/`Rows` class entries used to assemble a hydrated `Rows` through
/// the canonical PHP `Row` constructor and `Rows::conformed()` door, plus the
/// handles the cast path re-raises a refused value through.
pub struct AssemblyClasses {
    pub row_ce: &'static ClassEntry,
    /// `Rows::conformed()` - the shape-only door `HydratedBatch` returns through,
    /// so both hydrators assemble the batch by the same rule
    pub rows_conformed: &'static Function,
    pub schema_mismatch_ce: &'static ClassEntry,
    /// the base `PhpRowHydrator`'s guard catches, so a refusal is recognised on
    /// both paths by the same rule and anything else stays the caller's exception
    pub types_exception_ce: &'static ClassEntry,
    pub value_does_not_match: &'static Function,
}

impl AssemblyClasses {
    pub fn resolve() -> Result<Self, PhpException> {
        Ok(Self {
            row_ce: find_class("Flow\\ETL\\Row")?,
            rows_conformed: ce_method_ref(find_class("Flow\\ETL\\Rows")?, "conformed")?,
            schema_mismatch_ce: find_class("Flow\\ETL\\Exception\\SchemaMismatchException")?,
            types_exception_ce: find_class("Flow\\Types\\Exception\\Exception")?,
            value_does_not_match: ce_method_ref(
                find_class("Flow\\ETL\\Exception\\ColumnMismatchException")?,
                "valueDoesNotMatch",
            )?,
        })
    }
}

/// A per-column hydrate plan entry. Storage is name-keyed and the Schema owns
/// the column's type, so a column is just its key.
pub(crate) struct HydrateColumn {
    numeric_key: Option<i64>,
    pub(crate) name_zv: Zval,
    /// retained (refcount++) so the cast plan can read `type()` once per column
    pub(crate) base_def: Zval,
    /// `Definition::isNullable()`, read once at plan build - never per row
    pub(crate) nullable: bool,
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

/// Builds the per-column hydrate plan from a `Schema` object. Storage is
/// name-keyed and the Schema owns the column type, so a column is just its key.
/// Method calls are fine here - this runs once per schema, never per row.
pub(crate) fn build_hydrate_plan(schema: &Zval) -> Result<HydratePlan, PhpException> {
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

        let nullable = call_handle_on(
            ce_method_ref(def_ce, "isNullable")?,
            def_obj,
            &mut [],
            "read a definition nullability",
        )?
        .bool()
        .ok_or_else(|| ext_exception("flow_php expected Definition::isNullable to return a bool"))?;

        columns.push(HydrateColumn {
            numeric_key,
            name_zv,
            base_def: def_zv.shallow_clone(),
            nullable,
        });

        Ok(())
    })?;

    Ok(HydratePlan {
        definitions_slot,
        definitions_retained: definitions,
        columns,
    })
}

/// `HydratedBatch` folds `RawRowValues::metadata` into the batch Schema before building rows, so a
/// column carries one Metadata rather than a per-row divergent one. The native hydrate and cast
/// paths have to fold identically or a natively hydrated batch silently loses column metadata that
/// the pure-PHP one keeps. Last write wins, matching the PHP loop order.
pub fn fold_metadata_into_schema(
    schema: &Zval,
    batch_ht: &ZendHashTable,
    raw_class: &RowValuesClass,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let mut pending: Vec<(Vec<u8>, Zval)> = Vec::new();

    ht_for_each(batch_ht, |_, _, rv_zv| {
        let rv = expect_object(rv_zv, "a RawRowValues")?;

        let Some(metadata_ht) = read_slot(rv, raw_class.metadata_slot).array() else {
            return Ok(());
        };

        ht_for_each(metadata_ht, |key, _, metadata_zv| {
            if let Some(name) = key {
                pending.push((name.as_bytes().to_vec(), metadata_zv.shallow_clone()));
            }

            Ok(())
        })
    })?;

    if pending.is_empty() {
        return Ok(schema.shallow_clone());
    }

    let find_definition = ctx.schema_find_definition()?;
    let set_metadata = ctx.schema_set_metadata()?;
    let mut current = schema.shallow_clone();

    for (name, metadata) in pending {
        let object = current
            .object()
            .ok_or_else(|| ext_exception("flow_php expected a Schema object"))?;

        let found = call_handle_on(
            find_definition,
            object,
            &mut [zval_str(&name)],
            "Flow\\ETL\\Schema::findDefinition",
        )?;

        if found.is_null() {
            continue;
        }

        current = call_handle_on(
            set_metadata,
            object,
            &mut [zval_str(&name), metadata],
            "Flow\\ETL\\Schema::setMetadata",
        )?;
    }

    Ok(current)
}
