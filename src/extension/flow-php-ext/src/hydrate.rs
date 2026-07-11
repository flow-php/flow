//! Object-graph assembly mirroring `Flow\Floe\RowHydrator` and `EntryInstantiator`:
//! constructor-less instantiation with direct property-slot writes.

use ext_php_rs::boxed::ZBox;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};

use crate::ctx::{clone_object, construct_object, ht_add, write_slot, Ctx};
use crate::exception::ext_exception;
use crate::format::{Reader, VALUE_ABSENT, VALUE_NULL, VALUE_NULL_FROM_NULL, VALUE_PRESENT};
use crate::plan::Plan;
use crate::values::decode_value;

/// Decodes one ROW frame body into a `Flow\ETL\Row` object.
pub fn hydrate_row(
    plan: &Plan,
    reader: &mut Reader,
    ctx: &mut Ctx,
) -> Result<ZBox<ZendObject>, PhpException> {
    let mut entries_ht = ZendHashTable::with_capacity(plan.columns.len() as u32);

    for column in &plan.columns {
        let flag = reader.u8("row value flag")?;

        if flag == VALUE_ABSENT {
            continue;
        }

        let (value, definition_source) = match flag {
            VALUE_PRESENT => (
                decode_value(&column.decoder, reader, ctx)?,
                &column.definition,
            ),
            VALUE_NULL => (Zval::new(), &column.nullable_definition),
            VALUE_NULL_FROM_NULL => (Zval::new(), &column.from_null_definition),
            other => {
                return Err(ext_exception(format!(
                    "flow_php found unknown value flag 0x{other:02X}"
                )));
            }
        };

        let definition_zv = clone_object(definition_source, "a column definition")?
            .into_zval(false)
            .map_err(|e| {
                ext_exception(format!("flow_php failed to hydrate a definition: {e:?}"))
            })?;

        let mut entry = ZendObject::new(column.entry_ce);
        write_slot(&mut entry, column.name_slot, column.name_zv.shallow_clone());
        write_slot(&mut entry, column.value_slot, value);
        write_slot(&mut entry, column.definition_slot, definition_zv);

        let entry_zv = entry
            .into_zval(false)
            .map_err(|e| ext_exception(format!("flow_php failed to collect row entries: {e:?}")))?;

        // `new Entries(...)` rejects duplicated names; a name-keyed hash would
        // silently collapse them instead, so duplicates must fail loudly here.
        if !ht_add(&mut entries_ht, column.name.as_bytes(), entry_zv) {
            return Err(ext_exception(format!(
                "flow_php found duplicated entry name \"{}\" in a row frame",
                column.name
            )));
        }
    }

    let mut entries = ZendObject::new(ctx.entries_ce);
    let mut entries_ht_zv = Zval::new();
    entries_ht_zv.set_hashtable(entries_ht);
    write_slot(&mut entries, ctx.entries_entries_slot, entries_ht_zv);

    let mut row = ZendObject::new(ctx.row_ce);
    let entries_zv = entries
        .into_zval(false)
        .map_err(|e| ext_exception(format!("flow_php failed to hydrate a row: {e:?}")))?;
    write_slot(&mut row, ctx.row_entries_slot, entries_zv);

    Ok(row)
}

/// Builds a `Flow\ETL\Rows` with empty `Partitions`, matching `new Rows(...$rows)`.
pub fn build_rows(rows: Vec<ZBox<ZendObject>>, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let mut rows_ht = ZendHashTable::with_capacity(rows.len() as u32);

    for row in rows {
        rows_ht
            .push(row)
            .map_err(|e| ext_exception(format!("flow_php failed to collect rows: {e:?}")))?;
    }

    let partitions_zv =
        construct_object(ctx.partitions_ce, vec![], "Flow\\Filesystem\\Partitions")?
            .into_zval(false)
            .map_err(|e| ext_exception(format!("flow_php failed to hydrate Rows: {e:?}")))?;

    let mut rows_ht_zv = Zval::new();
    rows_ht_zv.set_hashtable(rows_ht);

    let mut rows_obj = ZendObject::new(ctx.rows_ce);
    write_slot(&mut rows_obj, ctx.rows_rows_slot, rows_ht_zv);
    write_slot(&mut rows_obj, ctx.rows_partitions_slot, partitions_zv);

    let mut zv = Zval::new();
    zv.set_object(&mut rows_obj);

    Ok(zv)
}
