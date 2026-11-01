//! `RustBackend::compare()`, `isNull()` and `isNotNull()`: native where arrow answers exactly as `PhpBackend` does.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{ArrayRef, BooleanArray, Float64Array};
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use flow_column_kernels::compare::{compare_floats, compare_ordered, floats_of_ints, Operator};

use crate::column::RustColumn;
use crate::ctx::{expect_object, read_property};
use crate::exception::ext_exception;

const BOOLEAN_TYPE: &str = "Flow\\Types\\Type\\Native\\BooleanType";

// types whose physicals PHP orders and equates as arrow does: ints, bools, bytes by strcmp, micros, days, uuid bytes
const ORDERED: [&str; 8] = [
    "integer", "boolean", "string", "json", "datetime", "date", "time", "uuid",
];

pub fn native_comparison(left: &Zval, operator: &Zval, right: &Zval) -> Result<Option<RustColumn>, PhpException> {
    let (Some(left), Some(right), Some(operator)) = (
        left.extract::<&RustColumn>(),
        right.extract::<&RustColumn>(),
        native_operator(operator)?,
    ) else {
        return Ok(None);
    };

    let compared = match (left.plan().type_name.as_str(), right.plan().type_name.as_str()) {
        (left_type, right_type) if left_type == right_type && ORDERED.contains(&left_type) => {
            compare_ordered(left.data(), operator, right.data())
                .map_err(|e| ext_exception(format!("flow_php failed to compare columns: {e}")))?
        }
        ("float" | "integer", "float") | ("float", "integer") => {
            compare_floats(&floats(left.data()), operator, &floats(right.data()))
        }
        _ => return Ok(None),
    };

    Ok(Some(boolean_column(compared)?))
}

pub fn boolean_column(booleans: BooleanArray) -> Result<RustColumn, PhpException> {
    RustColumn::of_type(Arc::new(booleans), BOOLEAN_TYPE)
}

fn native_operator(operator: &Zval) -> Result<Option<Operator>, PhpException> {
    let value = read_property(expect_object(operator, "an Operator")?, "value")?;

    Ok(match value.zend_str().map(|value| value.as_bytes()) {
        Some(b"==") => Some(Operator::Equal),
        Some(b"!=") => Some(Operator::NotEqual),
        Some(b"<") => Some(Operator::LessThan),
        Some(b"<=") => Some(Operator::LessThanOrEqual),
        Some(b">") => Some(Operator::GreaterThan),
        Some(b">=") => Some(Operator::GreaterThanOrEqual),
        _ => None,
    })
}

pub fn floats(data: &ArrayRef) -> Float64Array {
    match data.as_primitive_opt::<Int64Type>() {
        Some(ints) => floats_of_ints(ints),
        None => data.as_primitive::<Float64Type>().clone(),
    }
}
