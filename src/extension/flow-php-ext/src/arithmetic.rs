//! `RustBackend::add()`, `subtract()`, `multiply()` and `modulo()`: native over null-free integer and float columns.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;
use flow_column_kernels::arithmetic::{floats, ints, modulo, Operator};

use crate::column::RustColumn;
use crate::compare;
use crate::exception::ext_exception;

const INTEGER_TYPE: &str = "Flow\\Types\\Type\\Native\\IntegerType";
const FLOAT_TYPE: &str = "Flow\\Types\\Type\\Native\\FloatType";

pub fn native_arithmetic(left: &Zval, operator: Operator, right: &Zval) -> Result<Option<RustColumn>, PhpException> {
    let Some((left, right)) = null_free(left, right) else {
        return Ok(None);
    };

    match (left.plan().type_name.as_str(), right.plan().type_name.as_str()) {
        ("integer", "integer") => ints(
            left.data().as_primitive::<Int64Type>(),
            operator,
            right.data().as_primitive::<Int64Type>(),
        )
        .map(|result| RustColumn::of_type(Arc::new(result), INTEGER_TYPE))
        .transpose(),
        ("integer" | "float", "integer" | "float") => {
            let result = floats(&compare::floats(left.data()), operator, &compare::floats(right.data()))
                .map_err(|e| ext_exception(format!("flow_php failed to compute columns: {e}")))?;

            Ok(Some(RustColumn::of_type(Arc::new(result), FLOAT_TYPE)?))
        }
        _ => Ok(None),
    }
}

pub fn native_modulo(left: &Zval, right: &Zval) -> Result<Option<RustColumn>, PhpException> {
    let Some((left, right)) = null_free(left, right) else {
        return Ok(None);
    };

    if left.plan().type_name != "integer" || right.plan().type_name != "integer" {
        return Ok(None);
    }

    modulo(
        left.data().as_primitive::<Int64Type>(),
        right.data().as_primitive::<Int64Type>(),
    )
    .map(|result| RustColumn::of_type(Arc::new(result), INTEGER_TYPE))
    .transpose()
}

// a null operand is PHP's refusal, raised at its row by the PHP lane
fn null_free<'a>(left: &'a Zval, right: &'a Zval) -> Option<(&'a RustColumn, &'a RustColumn)> {
    let (left, right) = (left.extract::<&RustColumn>()?, right.extract::<&RustColumn>()?);

    (left.data().null_count() == 0 && right.data().null_count() == 0).then_some((left, right))
}
