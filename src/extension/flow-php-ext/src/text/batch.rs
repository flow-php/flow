//! A `Rows` batch as the native text writers read it: the schema's columns in order, each either an arrow array the
//! writer renders or left to the cells PHP rendered for it.

use std::rc::Rc;

use arrow_array::ArrayRef;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::backend::native_column;
use crate::column::RustColumn;
use crate::ctx::{call_method, ht_get, ht_insert, zval_str};
use crate::exception::ext_exception;
use crate::plan::{type_plan, TypePlan};
use crate::render::invalid_argument;
use crate::text::value::{renders, Formats, Renderer};

pub struct Column {
    pub name: Vec<u8>,
    /// `None`: the column is not rendered natively.
    pub native: Option<(Rc<TypePlan>, ArrayRef)>,
}

pub struct Batch {
    pub count: usize,
    pub columns: Vec<Column>,
}

fn definitions(schema: &Zval) -> PhpResult<Zval> {
    call_method(schema, "definitions", &mut [])
}

/// The names of the columns of `$schema` the native lane does not render.
fn unrendered_names(schema: &Zval, formats: &Formats, json_column_is_text: bool) -> PhpResult<Vec<Vec<u8>>> {
    let definitions = definitions(schema)?;
    let definitions = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
    let mut names = Vec::new();

    for (name, definition) in definitions.iter() {
        if !renders(
            &type_plan(&call_method(definition, "type", &mut [])?)?.values,
            formats,
            json_column_is_text,
        ) {
            names.push(name.to_string().into_bytes());
        }
    }

    Ok(names)
}

/// The unrendered columns of the last `Schema` object a writer saw; consecutive batches share one.
#[derive(Default)]
pub struct Unrendered {
    schema: Option<Zval>,
    names: Vec<Vec<u8>>,
}

impl Unrendered {
    /// `$php->$method($schema->get($name)->type(), $rows->column($name))` of every unrendered column, by name.
    pub fn render(
        &mut self,
        rows: &Zval,
        php: &Zval,
        method: &str,
        formats: &Formats,
        json_column_is_text: bool,
    ) -> PhpResult<ZBox<ZendHashTable>> {
        let schema = call_method(rows, "schema", &mut [])?;
        let held = self.schema.as_ref().and_then(Zval::object);

        if !matches!((held, schema.object()), (Some(held), Some(object)) if std::ptr::eq(held, object)) {
            self.names = unrendered_names(&schema, formats, json_column_is_text)?;
            self.schema = Some(schema.shallow_clone());
        }

        let mut rendered = ZendHashTable::with_capacity(self.names.len() as u32);

        for name in &self.names {
            let definition = call_method(&schema, "get", &mut [zval_str(name)])?;
            let mut args = [
                call_method(&definition, "type", &mut [])?,
                call_method(rows, "column", &mut [zval_str(name)])?,
            ];

            ht_insert(&mut rendered, name, call_method(php, method, &mut args)?);
        }

        Ok(rendered)
    }
}

/// `$rows` in schema order; a rendered column that is not a `RustColumn` is adopted into one.
pub fn batch(rows: &Zval, formats: &Formats, json_column_is_text: bool) -> PhpResult<Batch> {
    let count = call_method(rows, "count", &mut [])?
        .long()
        .ok_or_else(|| ext_exception("flow_php expected Rows::count() to return an int"))? as usize;
    let held = call_method(rows, "columns", &mut [])?;
    let held = held
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Rows::columns() to return an array"))?;
    let definitions = definitions(&call_method(rows, "schema", &mut [])?)?;
    let definitions = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions() to return an array"))?;
    let mut columns = Vec::with_capacity(definitions.len());

    for (name, definition) in definitions.iter() {
        let name = name.to_string().into_bytes();
        let column = ht_get(held, &name).ok_or_else(|| {
            ext_exception(format!(
                "flow_php expected Rows to hold the column \"{}\"",
                String::from_utf8_lossy(&name)
            ))
        })?;

        let native = match column.extract::<&RustColumn>() {
            Some(native) => renders(&native.plan().values, formats, json_column_is_text)
                .then(|| (Rc::clone(native.plan()), ArrayRef::clone(native.data()))),
            None => {
                if renders(
                    &type_plan(&call_method(definition, "type", &mut [])?)?.values,
                    formats,
                    json_column_is_text,
                ) {
                    Some(native_column(definition, column)?)
                } else {
                    None
                }
            }
        };

        columns.push(Column { name, native });
    }

    Ok(Batch { count, columns })
}

/// The renderer of a native column of `batch()`, its named zones resolved.
pub fn renderer<'a>(plan: &'a TypePlan, data: &'a ArrayRef, formats: &'a Formats) -> PhpResult<Renderer<'a>> {
    let mut renderer = Renderer::new(&plan.values, &plan.kind, data.as_ref(), formats)
        .ok_or_else(|| ext_exception(format!("flow_php cannot render a column stored as {:?}", plan.kind)))?;

    renderer.resolve()?;

    Ok(renderer)
}

/// The cells PHP rendered for the column `name`: `count` values, each a string or null.
pub fn cells<'a>(cells: &'a ZendHashTable, name: &[u8], count: usize, what: &str) -> PhpResult<Vec<Option<&'a [u8]>>> {
    let refused = |problem: &str| {
        invalid_argument(format!(
            "flow_php expected {count} {what} of the column \"{}\", {problem}",
            String::from_utf8_lossy(name)
        ))
    };
    let list = ht_get(cells, name)
        .ok_or_else(|| refused("which it does not render, got none"))?
        .array()
        .ok_or_else(|| refused("got no list"))?;

    if list.len() != count {
        return Err(refused(&format!("got {}", list.len())));
    }

    list.values()
        .map(|cell| match cell.zend_str() {
            Some(text) => Ok(Some(text.as_bytes())),
            None if cell.is_null() => Ok(None),
            None => Err(refused(&format!("got a {}", cell.get_type()))),
        })
        .collect()
}
