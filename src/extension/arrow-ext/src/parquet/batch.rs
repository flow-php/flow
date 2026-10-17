//! `Flow\Arrow\Parquet\RustBatchReader` and the Arrow C Data carriers: one batch = one struct-typed `FFI_ArrowSchema` (one
//! child per column) plus one struct-typed `FFI_ArrowArray`. The carrier owns both until a consumer moves the array
//! out; any extension's internal class exposing the two addresses is accepted.

use std::collections::HashMap;
use std::ffi::{c_char, CStr};
use std::sync::Arc;

use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, StructArray};
use arrow_schema::{DataType, Field, Fields};
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;

use crate::parquet::error::Error;
use crate::parquet::library::{batch_size, read_plan, RustParquetFileReader, INVALID_ARGUMENT};
use crate::parquet::read::ParquetReader;
use crate::parquet::source::PhpStream;
use crate::php::{call_handle_transparent, ce_method_ref};
use crate::render::{exception, parquet_exception, Side};

/// Child field metadata: the arrow type of the file column the canonical child was read from.
pub const SOURCE_TYPE: &str = "parquet.source_type";

/// `ZEND_INTERNAL_CLASS` (zend_compile.h): a class an extension registered, which userland can neither declare nor
/// extend once final.
const ZEND_INTERNAL_CLASS: c_char = 1;

#[php_class]
#[php(name = "Flow\\Arrow\\RustParquetBatch", flags = ClassFlags::Final)]
pub struct RustParquetBatch {
    schema: FFI_ArrowSchema,
    array: FFI_ArrowArray,
    rows: usize,
}

#[php_impl]
impl RustParquetBatch {
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

#[php_class]
#[php(name = "Flow\\Arrow\\RustArrowSchema", flags = ClassFlags::Final)]
pub struct RustArrowSchema {
    schema: FFI_ArrowSchema,
}

#[php_impl]
impl RustArrowSchema {
    #[php(name = "arrowSchemaAddress")]
    pub fn arrow_schema_address(&self) -> i64 {
        std::ptr::from_ref(&self.schema) as i64
    }
}

#[php_class]
#[php(name = "Flow\\Arrow\\Parquet\\RustBatchReader", flags = ClassFlags::Final)]
pub struct RustBatchReader {
    reader: Option<ParquetReader>,
    stream: Arc<PhpStream>,
    /// The projection: canonical children named by column, each with its `SOURCE_TYPE`.
    fields: Fields,
}

#[php_impl]
impl RustBatchReader {
    /// `$columns`: root names; `$batchSize` rows at most per `next()`.
    pub fn __construct(
        file: &RustParquetFileReader,
        columns: Vec<String>,
        batch_size: i64,
        plan: &Zval,
    ) -> PhpResult<Self> {
        let batch_size = self::batch_size(batch_size)?;
        let (stream, size, meta) = file.read()?;
        let stream = Arc::clone(stream);
        let plan = read_plan(meta, plan, &stream)?;
        let reader = ParquetReader::open(Arc::clone(&stream), size, meta, &columns, plan, batch_size)
            .map_err(|error| parquet_exception(error, &stream, Side::Read))?;
        let fields = columns
            .iter()
            .zip(reader.fields())
            .zip(reader.types())
            .map(|((name, field), canonical)| {
                Field::new(name, canonical.clone(), true).with_metadata(HashMap::from([(
                    SOURCE_TYPE.to_string(),
                    field.data_type().to_string(),
                )]))
            })
            .collect();

        Ok(Self {
            reader: Some(reader),
            stream,
            fields,
        })
    }

    /// The struct schema of every batch, known before the first.
    pub fn schema(&self) -> PhpResult<RustArrowSchema> {
        Ok(RustArrowSchema {
            schema: FFI_ArrowSchema::try_from(&DataType::Struct(self.fields.clone()))
                .map_err(|error| parquet_exception(Error::Arrow(error), &self.stream, Side::Read))?,
        })
    }

    /// At most `$batchSize` rows as one struct batch, null after the last.
    pub fn next(&mut self) -> PhpResult<Option<RustParquetBatch>> {
        let Some(reader) = self.reader.as_mut() else {
            return Ok(None);
        };

        let (rows, arrays) = match reader.next() {
            None => {
                self.reader = None;

                return Ok(None);
            }
            Some(batch) => batch.map_err(|error| parquet_exception(error, &self.stream, Side::Read))?,
        };

        let exported = StructArray::try_new_with_length(self.fields.clone(), arrays, None, rows)
            .and_then(|array| to_ffi(&array.to_data()))
            .map_err(|error| parquet_exception(Error::Arrow(error), &self.stream, Side::Read))?;

        Ok(Some(RustParquetBatch {
            array: exported.0,
            schema: exported.1,
            rows,
        }))
    }

    pub fn close(&mut self) {
        self.reader = None;
    }
}

fn refused(message: String) -> PhpException {
    exception(INVALID_ARGUMENT, message)
}

/// `$object->$method()` of an address method: a pointer the exporter owns.
fn address(object: &ZendObject, ce: &ClassEntry, method: &str) -> PhpResult<usize> {
    let address = call_handle_transparent(ce_method_ref(ce, method)?, Some(object), &mut [])?;

    address
        .long()
        .and_then(|address| usize::try_from(address).ok())
        .filter(|address| *address != 0)
        .ok_or_else(|| refused(format!("Arrow C Data {method}() must return a non-null address")))
}

/// The struct batch `$batch` carries, moved out of it: an internal class's `arrowSchemaAddress()` (read by reference)
/// and `arrowArrayAddress()` (moved, its release left NULL), refused before the move unless the schema is a struct
/// of as many children as the array and the array was not imported before.
pub fn import(batch: &Zval) -> PhpResult<StructArray> {
    let object = batch
        .object()
        .ok_or_else(|| refused("Arrow C Data batch must be an object".to_string()))?;
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| refused("Arrow C Data batch has no class".to_string()))?;

    if ce.type_ != ZEND_INTERNAL_CLASS {
        return Err(refused(format!(
            "Arrow C Data batch must be an extension's class, got {}",
            ce.name().unwrap_or_default()
        )));
    }

    let schema = address(object, ce, "arrowSchemaAddress")? as *const FFI_ArrowSchema;
    let source = address(object, ce, "arrowArrayAddress")? as *mut FFI_ArrowArray;

    unsafe { check(schema, source) }.map_err(refused)?;

    let schema = unsafe { &*schema };
    let moved = unsafe { FFI_ArrowArray::from_raw(source) };

    let data = unsafe { from_ffi(moved, schema) }
        .and_then(|data| data.validate().map(|()| data))
        .map_err(|error| refused(format!("Arrow C Data batch is not a valid struct array: {error}")))?;

    Ok(StructArray::from(data))
}

/// The leading fields of `struct ArrowSchema` and `struct ArrowArray` as the Arrow C Data Interface specification
/// declares them (https://arrow.apache.org/docs/format/CDataInterface.html#structure-definitions): read before the
/// arrow-rs accessors, which panic on a null format and cast a negative count to `usize`.
#[repr(C)]
struct SchemaHead {
    format: *const c_char,
    name: *const c_char,
    metadata: *const c_char,
    flags: i64,
    n_children: i64,
}

#[repr(C)]
struct ArrayHead {
    length: i64,
    null_count: i64,
    offset: i64,
    n_buffers: i64,
    n_children: i64,
}

/// # Safety
///
/// `schema` and `array` point to live C Data Interface structs.
unsafe fn check(schema: *const FFI_ArrowSchema, array: *const FFI_ArrowArray) -> Result<(), String> {
    let schema_head = unsafe { &*(schema as *const SchemaHead) };
    let array_head = unsafe { &*(array as *const ArrayHead) };

    if schema_head.format.is_null() || unsafe { CStr::from_ptr(schema_head.format) }.to_bytes() != b"+s" {
        return Err("Arrow C Data batch must be a struct array".to_string());
    }

    if unsafe { &*array }.is_released() {
        return Err("Arrow C Data batch is already imported".to_string());
    }

    if array_head.length < 0 {
        return Err(format!(
            "Arrow C Data batch length must not be negative, {} given",
            array_head.length
        ));
    }

    if array_head.n_children < 0 || schema_head.n_children < 0 || array_head.n_children != schema_head.n_children {
        return Err(format!(
            "Arrow C Data batch of {} children does not match its schema of {}",
            array_head.n_children, schema_head.n_children
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::ffi::{from_ffi, to_ffi};
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};

    use crate::alloc::allocated_bytes;

    fn batch() -> StructArray {
        let ids: ArrayRef = Arc::new(Int64Array::from_iter((0..1000).map(|i| (i % 7 != 0).then_some(i))));
        let lists: ArrayRef = Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
            (0..1000).map(|i| Some(vec![Some(i), None, Some(i * 2)])),
        ));

        StructArray::new(
            Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("list", lists.data_type().clone(), true),
            ]),
            vec![ids, lists],
            None,
        )
    }

    #[test]
    fn an_exported_batch_dropped_unimported_frees_everything() {
        let baseline = allocated_bytes();

        {
            let exported = to_ffi(&batch().to_data()).unwrap();
            assert!(allocated_bytes() > baseline);
            drop(exported);
        }

        assert_eq!(allocated_bytes(), baseline);
    }

    #[test]
    fn an_exported_batch_imported_then_dropped_frees_everything() {
        let baseline = allocated_bytes();

        {
            let (array, schema) = to_ffi(&batch().to_data()).unwrap();
            let imported = StructArray::from(unsafe { from_ffi(array, &schema) }.unwrap());
            assert_eq!(imported.len(), 1000);
            assert_eq!(imported, batch());
            drop(schema);
            drop(imported);
        }

        assert_eq!(allocated_bytes(), baseline);
    }

    fn refusal(mutate: impl FnOnce(*mut super::SchemaHead, *mut super::ArrayHead)) -> Result<(), String> {
        let (mut array, mut schema) = to_ffi(&batch().to_data()).unwrap();
        let schema_ptr = &mut schema as *mut arrow_array::ffi::FFI_ArrowSchema;
        let array_ptr = &mut array as *mut arrow_array::ffi::FFI_ArrowArray;
        let schema_head = schema_ptr as *mut super::SchemaHead;
        let array_head = array_ptr as *mut super::ArrayHead;
        let (format, length, children) =
            unsafe { ((*schema_head).format, (*array_head).length, (*array_head).n_children) };

        mutate(schema_head, array_head);
        let result = unsafe { super::check(schema_ptr, array_ptr) };

        unsafe {
            (*schema_head).format = format;
            (*array_head).length = length;
            (*array_head).n_children = children;
        }

        result
    }

    #[test]
    fn a_null_format_is_refused_before_the_accessors() {
        assert_eq!(
            refusal(|schema, _| unsafe { (*schema).format = std::ptr::null() }),
            Err("Arrow C Data batch must be a struct array".to_string())
        );
    }

    #[test]
    fn a_negative_length_is_refused() {
        assert_eq!(
            refusal(|_, array| unsafe { (*array).length = -1 }),
            Err("Arrow C Data batch length must not be negative, -1 given".to_string())
        );
    }

    #[test]
    fn a_negative_or_mismatched_child_count_is_refused() {
        assert_eq!(
            refusal(|_, array| unsafe { (*array).n_children = -1 }),
            Err("Arrow C Data batch of -1 children does not match its schema of 2".to_string())
        );
        assert_eq!(
            refusal(|_, array| unsafe { (*array).n_children = 1 }),
            Err("Arrow C Data batch of 1 children does not match its schema of 2".to_string())
        );
    }
}
