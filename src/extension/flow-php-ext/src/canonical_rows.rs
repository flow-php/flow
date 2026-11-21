//! The canonical bytes of a row from native columns: `ordered` for group, join and bucket keys, `named` for row
//! hashes. The Rust side of PHP's `CanonicalEncoding::ordered()` and `named()`.

use std::rc::Rc;
use std::sync::Arc;

use arrow_array::ArrayRef;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_column_kernels::canonical::{append_pair, append_value};

use crate::backend::RustBackend;
use crate::column::RustColumn;
use crate::exception::ext_exception;
use crate::plan::TypePlan;

pub const STRING_TYPE: &str = "Flow\\Types\\Type\\Native\\StringType";

/// (name, native array, the plan that holds its encoding)
pub type RowColumn = (Vec<u8>, ArrayRef, Rc<TypePlan>);

pub fn ordered(out: &mut Vec<u8>, entries: &mut Vec<Vec<u8>>, columns: &[RowColumn], row: usize) {
    out.clear();

    for (_, array, plan) in columns {
        append_value(out, entries, &plan.encoding, array.as_ref(), row);
    }
}

/// `columns` must be sorted by name bytes.
pub fn named(out: &mut Vec<u8>, entries: &mut Vec<Vec<u8>>, columns: &[RowColumn], row: usize) {
    out.clear();

    for (name, array, plan) in columns {
        append_pair(out, entries, name, &plan.encoding, array.as_ref(), row);
    }
}

/// The columns of `array<array-key, Column>`, sorted by name bytes.
pub fn row_columns(columns: &ZendHashTable) -> PhpResult<Vec<RowColumn>> {
    let mut resolved = columns
        .iter()
        .map(|(name, column)| named_column(name.to_string().into_bytes(), column))
        .collect::<PhpResult<Vec<_>>>()?;
    resolved.sort_unstable_by(|(left, ..), (right, ..)| left.cmp(right));

    Ok(resolved)
}

pub fn named_column(name: Vec<u8>, column: &Zval) -> PhpResult<RowColumn> {
    let adopted;
    let native = match column.extract::<&RustColumn>() {
        Some(native) => native,
        None => {
            adopted = RustBackend.adopt_result(column.shallow_clone())?.0;
            adopted
                .extract::<&RustColumn>()
                .ok_or_else(|| ext_exception("flow_php expected RustBackend::adopt() to return a RustColumn"))?
        }
    };

    Ok((name, Arc::clone(native.data()), Rc::clone(native.plan())))
}
