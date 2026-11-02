//! `RustBackend::sort()`: native in the lanes PHP orders as arrow does, through `PhpBackend::sort()` otherwise.

use std::rc::Rc;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Float64Type;
use arrow_select::take::take;
use ext_php_rs::convert::IntoZval;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_column_kernels::sort::{php_ordered_floats, stable_indices, SortColumn, SortOptions};

use crate::backend::rows_from_columns;
use crate::column::RustColumn;
use crate::ctx::{call_method, column_key, expect_object, ht_for_each, ht_get, ht_insert, read_property};
use crate::exception::ext_exception;

// physicals RowOrder sorts by value or by strcmp bytes, the way arrow orders them
const ORDERED: [&str; 10] = [
    "integer", "float", "boolean", "string", "json", "enum", "uuid", "datetime", "date", "time",
];

pub fn native_sort(rows: &Zval, sort_columns: &Zval) -> Result<Option<Zval>, PhpException> {
    let columns = call_method(rows, "columns", &mut [])?;
    let columns = columns
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Rows::columns() to return an array"))?;

    if !every_column_native(columns)? {
        return Ok(None);
    }

    let Some(keys) = sort_keys(columns, sort_columns)? else {
        return Ok(None);
    };

    let indices = stable_indices(&keys).map_err(|e| ext_exception(format!("flow_php failed to sort rows: {e}")))?;
    let mut sorted = ZendHashTable::with_capacity(columns.len() as u32);

    ht_for_each(columns, |name, index, column| {
        let native = column
            .extract::<&RustColumn>()
            .ok_or_else(|| ext_exception("flow_php expected a RustColumn"))?;
        let gathered = take(native.data().as_ref(), &indices, None)
            .map_err(|e| ext_exception(format!("flow_php failed to gather sorted rows: {e}")))?;
        let mut zv = Zval::new();
        RustColumn::new(gathered, Rc::clone(native.plan())).set_zval(&mut zv, false)?;
        ht_insert(&mut sorted, &column_key(name, index), zv);

        Ok(())
    })?;

    rows_from_columns(&call_method(rows, "schema", &mut [])?, sorted, indices.len())
        .map(Some)
        .map_err(|no_rows| no_rows.surfaced())
}

fn every_column_native(columns: &ZendHashTable) -> Result<bool, PhpException> {
    let mut native = true;

    ht_for_each(columns, |_, _, column| {
        native &= column.extract::<&RustColumn>().is_some();

        Ok(())
    })?;

    Ok(native)
}

fn sort_keys(columns: &ZendHashTable, sort_columns: &Zval) -> Result<Option<Vec<SortColumn>>, PhpException> {
    let sort_columns = sort_columns
        .array()
        .ok_or_else(|| ext_exception("flow_php expected the sort columns as an array"))?;
    let mut keys = Vec::with_capacity(sort_columns.len());

    for sort_column in sort_columns.values() {
        let sort_column = expect_object(sort_column, "a SortColumn")?;
        let name = read_property(sort_column, "name")?;
        let Some(column) = name
            .zend_str()
            .and_then(|name| ht_get(columns, name.as_bytes()))
            .and_then(|column| column.extract::<&RustColumn>())
        else {
            return Ok(None);
        };

        if !ORDERED.contains(&column.plan().type_name.as_str()) {
            return Ok(None);
        }

        let values = match column.data().as_primitive_opt::<Float64Type>() {
            Some(floats) => Arc::new(php_ordered_floats(floats)),
            None => Arc::clone(column.data()),
        };

        keys.push(SortColumn {
            values,
            options: Some(SortOptions {
                descending: case_name(&read_property(sort_column, "order")?)? == "DESC",
                nulls_first: case_name(&read_property(sort_column, "nulls")?)? == "FIRST",
            }),
        });
    }

    Ok((!keys.is_empty()).then_some(keys))
}

fn case_name(case: &Zval) -> Result<String, PhpException> {
    read_property(expect_object(case, "an enum case")?, "name")?
        .zend_str()
        .map(|name| name.as_bytes().to_vec())
        .and_then(|name| String::from_utf8(name).ok())
        .ok_or_else(|| ext_exception("flow_php expected an enum case name"))
}
