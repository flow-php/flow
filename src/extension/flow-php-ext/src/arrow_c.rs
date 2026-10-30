//! `Flow\ETL\Values\Column\RustColumnsBatch` and the PHP side of the Arrow C Data hand-off: any extension's internal class
//! exposing the two addresses is accepted, a refusal is worded with the column's schema type.

use std::ffi::c_char;
use std::rc::Rc;

use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::ArrayRef;
use arrow_schema::DataType;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;
use flow_arrow_c::{check, take, Expected, Refusal};
use flow_batch_frame::kind::data_type;

use crate::ctx::{call_handle_transparent, call_method, ce_method_ref};
use crate::plan::TypePlan;
use crate::render::invalid_argument;

/// Child field metadata: the arrow type of the file column the canonical child was read from.
const SOURCE_TYPE: &str = "parquet.source_type";

/// `ZEND_INTERNAL_CLASS` (zend_compile.h): a class an extension registered, which userland can neither declare nor
/// extend once final.
const ZEND_INTERNAL_CLASS: c_char = 1;

/// One exported batch, until its consumer moves the array out.
#[php_class]
#[php(name = "Flow\\ETL\\Values\\Column\\RustColumnsBatch", flags = ClassFlags::Final)]
pub struct RustColumnsBatch {
    schema: FFI_ArrowSchema,
    array: FFI_ArrowArray,
    rows: usize,
}

#[php_impl]
impl RustColumnsBatch {
    pub fn count(&self) -> i64 {
        self.rows as i64
    }

    #[php(name = "arrowSchemaAddress")]
    pub fn arrow_schema_address(&self) -> i64 {
        std::ptr::from_ref(&self.schema) as i64
    }

    #[php(name = "arrowArrayAddress")]
    pub fn arrow_array_address(&self) -> i64 {
        std::ptr::from_ref(&self.array) as i64
    }
}

fn refused(refusal: Refusal, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpException {
    let message = match refusal {
        Refusal::NotStruct => "Arrow C Data batch must be a struct".to_string(),
        Refusal::Count { children, columns } => {
            format!("Arrow C Data batch of {children} children does not match the {columns} columns read")
        }
        Refusal::Name { child, column } => format!(
            "Arrow C Data batch child \"{child}\" is not the column \"{}\"",
            String::from_utf8_lossy(&columns[column].0)
        ),
        Refusal::Type { column, child } => {
            let (name, plan) = &columns[column];
            let type_name = match call_method(&plan.type_zv, "toString", &mut []) {
                Ok(type_name) => type_name,
                Err(exception) => return exception,
            };

            format!(
                "Parquet column \"{}\" ({}) is read as {}, its schema type {} stores {}",
                String::from_utf8_lossy(name),
                child.metadata().get(SOURCE_TYPE).map_or("", String::as_str),
                child.data_type(),
                String::from_utf8_lossy(type_name.zend_str().map_or(&b""[..], |name| name.as_bytes())),
                data_type(&plan.kind),
            )
        }
        Refusal::Schema(error) => format!("Arrow C Data schema is refused: {error}"),
        Refusal::Released => "Arrow C Data batch is already imported".to_string(),
        Refusal::Length { children, schema } => {
            format!("Arrow C Data batch of {children} children does not match its schema of {schema}")
        }
        Refusal::Invalid(error) => {
            format!("Arrow C Data batch is not a valid struct array: {error}")
        }
    };

    invalid_argument(message)
}

/// The object of an exporter's internal class.
fn exporter(carrier: &Zval) -> PhpResult<(&ZendObject, &ClassEntry)> {
    let object = carrier
        .object()
        .ok_or_else(|| invalid_argument("Arrow C Data carrier must be an object".to_string()))?;
    let ce = unsafe { object.ce.as_ref() }
        .ok_or_else(|| invalid_argument("Arrow C Data carrier has no class".to_string()))?;

    if ce.type_ != ZEND_INTERNAL_CLASS {
        return Err(invalid_argument(format!(
            "Arrow C Data carrier must be an extension's class, got {}",
            ce.name().unwrap_or_default()
        )));
    }

    Ok((object, ce))
}

/// `$object->$method()` of an address method: a pointer the exporter owns.
fn address(object: &ZendObject, ce: &ClassEntry, method: &str) -> PhpResult<usize> {
    let address = call_handle_transparent(ce_method_ref(ce, method)?, Some(object), &mut [])?;

    address
        .long()
        .and_then(|address| usize::try_from(address).ok())
        .filter(|address| *address != 0)
        .ok_or_else(|| invalid_argument(format!("Arrow C Data {method}() must return a non-null address")))
}

fn stored(columns: &[(Vec<u8>, Rc<TypePlan>)]) -> Vec<(&[u8], DataType)> {
    columns
        .iter()
        .map(|(name, plan)| (name.as_slice(), data_type(&plan.kind)))
        .collect()
}

/// Refuses a schema carrier (`arrowSchemaAddress()`) whose struct children are not `columns`, by name and stored type;
/// what every batch of the source is then imported as.
pub fn check_schema(schema: &Zval, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<Expected> {
    let (object, ce) = exporter(schema)?;
    let schema = unsafe { &*(address(object, ce, "arrowSchemaAddress")? as *const FFI_ArrowSchema) };

    check(schema, &stored(columns)).map_err(|refusal| refused(refusal, columns))
}

/// The batch `batch` carries (`arrowSchemaAddress()` read by reference, `arrowArrayAddress()` moved): the row count
/// and one array per column of `columns`, as `expected` describes.
pub fn import(
    batch: &Zval,
    expected: &Expected,
    columns: &[(Vec<u8>, Rc<TypePlan>)],
) -> PhpResult<(usize, Vec<ArrayRef>)> {
    let (object, ce) = exporter(batch)?;
    let schema = unsafe { &*(address(object, ce, "arrowSchemaAddress")? as *const FFI_ArrowSchema) };
    let source = unsafe { &mut *(address(object, ce, "arrowArrayAddress")? as *mut FFI_ArrowArray) };

    take(schema, source, expected).map_err(|refusal| refused(refusal, columns))
}

/// `columns` as one struct batch of `rows` rows, every child nullable.
pub fn export(rows: usize, columns: Vec<(String, ArrayRef)>) -> PhpResult<RustColumnsBatch> {
    let (schema, array) = flow_arrow_c::export(rows, columns)
        .map_err(|error| invalid_argument(format!("flow_php failed to export an Arrow C Data batch: {error}")))?;

    Ok(RustColumnsBatch { schema, array, rows })
}
