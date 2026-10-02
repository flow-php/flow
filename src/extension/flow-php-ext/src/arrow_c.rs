//! Batches across the Arrow C Data Interface, to and from another extension (arrow-ext): one batch = one struct-typed
//! `FFI_ArrowSchema` (one child per column) plus one struct-typed `FFI_ArrowArray`. Any extension's internal class
//! exposing the two addresses is accepted. An import moves the producer's array behind a release forwarder that counts
//! its buffers in `allocatedBytes()` for as long as they live.

use std::ffi::{c_char, CStr};
use std::rc::Rc;

use arrow_array::ffi::{from_ffi, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Fields};
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;
use flow_batch_frame::kind::data_type;

use crate::alloc;
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
#[php(name = "Flow\\ETL\\Column\\RustColumnsBatch", flags = ClassFlags::Final)]
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

/// What an import refuses; nothing was taken from the producer.
#[derive(Debug)]
enum Refusal {
    NotStruct,
    Count {
        children: i64,
        columns: usize,
    },
    Name {
        child: String,
        column: usize,
    },
    /// Column `column` arrives as `child`, which its plan does not store.
    Type {
        column: usize,
        child: Field,
    },
    Schema(ArrowError),
    Released,
    Length {
        children: i64,
        schema: i64,
    },
    Invalid(ArrowError),
}

/// The struct schema `schema`, its children named and typed as `columns` (name, stored type) are.
fn check(schema: &FFI_ArrowSchema, columns: &[(&[u8], DataType)]) -> Result<(), Refusal> {
    if schema.format.is_null() || unsafe { CStr::from_ptr(schema.format) }.to_bytes() != b"+s" {
        return Err(Refusal::NotStruct);
    }

    if schema.n_children != columns.len() as i64 {
        return Err(Refusal::Count {
            children: schema.n_children,
            columns: columns.len(),
        });
    }

    for (column, (child, (name, stored))) in schema.children().zip(columns).enumerate() {
        let child = Field::try_from(child).map_err(Refusal::Schema)?;

        if child.name().as_bytes() != *name {
            return Err(Refusal::Name {
                child: child.name().clone(),
                column,
            });
        }

        if child.data_type() != stored {
            return Err(Refusal::Type { column, child });
        }
    }

    Ok(())
}

/// An imported array: the producer's, moved here, and the bytes it counts while arrow-rs holds its buffers.
struct Counted {
    inner: FFI_ArrowArray,
    bytes: i64,
}

/// The forwarder's release: arrow-rs calls it once, when the batch's last buffer drops; dropping `inner` runs the
/// producer's release.
unsafe extern "C" fn counted_release(array: *mut FFI_ArrowArray) {
    let Some(array) = (unsafe { array.as_mut() }) else {
        return;
    };
    let Counted { inner, bytes } = *unsafe { Box::from_raw(array.private_data.cast::<Counted>()) };

    alloc::imported(-bytes);
    drop(inner);
    array.release = None;
}

/// `source` moved out (its release left NULL) behind the counting forwarder, as the struct `schema` describes, after
/// [`check`]: the row count and one array per column. A refusal before the move takes nothing; after it the forwarder
/// owns the producer's array and releases it.
fn take(
    schema: &FFI_ArrowSchema,
    source: &mut FFI_ArrowArray,
    columns: &[(&[u8], DataType)],
) -> Result<(usize, Vec<ArrayRef>), Refusal> {
    check(schema, columns)?;

    if source.release.is_none() {
        return Err(Refusal::Released);
    }

    if source.n_children != schema.n_children || source.length < 0 {
        return Err(Refusal::Length {
            children: source.n_children,
            schema: schema.n_children,
        });
    }

    let moved = unsafe { std::ptr::read(source) };
    source.release = None;

    let counted = Box::into_raw(Box::new(Counted { inner: moved, bytes: 0 }));
    let inner = unsafe { &(*counted).inner };
    let forward = FFI_ArrowArray {
        length: inner.length,
        null_count: inner.null_count,
        offset: inner.offset,
        n_buffers: inner.n_buffers,
        n_children: inner.n_children,
        buffers: inner.buffers,
        children: inner.children,
        dictionary: inner.dictionary,
        release: Some(counted_release),
        private_data: counted.cast(),
    };

    let data = unsafe { from_ffi(forward, schema) }
        .and_then(|data| data.validate().map(|()| data))
        .map_err(Refusal::Invalid)?;
    let bytes = data.get_buffer_memory_size() as i64;

    // the forwarder lives in arrow-rs's owner of `data`'s buffers, so `counted` does too
    unsafe { (*counted).bytes = bytes };
    alloc::imported(bytes);

    let batch = StructArray::from(data);

    Ok((batch.len(), batch.into_parts().1))
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

/// Refuses a schema carrier (`arrowSchemaAddress()`) whose struct children are not `columns`, by name and stored type.
pub fn check_schema(schema: &Zval, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<()> {
    let (object, ce) = exporter(schema)?;
    let schema = unsafe { &*(address(object, ce, "arrowSchemaAddress")? as *const FFI_ArrowSchema) };

    check(schema, &stored(columns)).map_err(|refusal| refused(refusal, columns))
}

/// The batch `batch` carries (`arrowSchemaAddress()` read by reference, `arrowArrayAddress()` moved): the row count
/// and one array per column of `columns`.
pub fn import(batch: &Zval, columns: &[(Vec<u8>, Rc<TypePlan>)]) -> PhpResult<(usize, Vec<ArrayRef>)> {
    let (object, ce) = exporter(batch)?;
    let schema = unsafe { &*(address(object, ce, "arrowSchemaAddress")? as *const FFI_ArrowSchema) };
    let source = unsafe { &mut *(address(object, ce, "arrowArrayAddress")? as *mut FFI_ArrowArray) };

    take(schema, source, &stored(columns)).map_err(|refusal| refused(refusal, columns))
}

/// `columns` as one struct batch of `rows` rows, every child nullable.
pub fn export(rows: usize, columns: Vec<(String, ArrayRef)>) -> PhpResult<RustColumnsBatch> {
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect::<Fields>();
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let (array, schema) = StructArray::try_new_with_length(fields, arrays, None, rows)
        .and_then(|batch| to_ffi(&batch.to_data()))
        .map_err(|error| invalid_argument(format!("flow_php failed to export an Arrow C Data batch: {error}")))?;

    Ok(RustColumnsBatch { schema, array, rows })
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use arrow_array::ffi::{to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};

    use super::{take, Refusal};
    use crate::alloc;

    /// `IMPORTED` is process-wide: the tests that import run one at a time.
    static SERIAL: Mutex<()> = Mutex::new(());

    static RELEASES: AtomicUsize = AtomicUsize::new(0);

    /// The producer: arrow-rs's export, wrapped so its release counts how often it ran.
    unsafe extern "C" fn counting_release(array: *mut FFI_ArrowArray) {
        let array = unsafe { &mut *array };
        drop(unsafe { Box::from_raw(array.private_data.cast::<FFI_ArrowArray>()) });
        array.release = None;
        RELEASES.fetch_add(1, Ordering::SeqCst);
    }

    fn columns() -> Vec<(String, ArrayRef)> {
        vec![
            (
                "id".to_string(),
                Arc::new(Int64Array::from_iter((0..1000).map(|i| (i % 3 != 0).then_some(i)))) as ArrayRef,
            ),
            (
                "list".to_string(),
                Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                    (0..1000).map(|i| (i % 5 != 0).then(|| vec![Some(i), None])),
                )) as ArrayRef,
            ),
        ]
    }

    fn produced() -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let fields = columns()
            .iter()
            .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
            .collect::<Fields>();
        let batch = StructArray::new(fields, columns().into_iter().map(|(_, array)| array).collect(), None);
        let (inner, schema) = to_ffi(&batch.to_data()).unwrap();
        let inner = Box::into_raw(Box::new(inner));
        let exported = unsafe { &*inner };
        let array = FFI_ArrowArray {
            length: exported.length,
            null_count: exported.null_count,
            offset: exported.offset,
            n_buffers: exported.n_buffers,
            n_children: exported.n_children,
            buffers: exported.buffers,
            children: exported.children,
            dictionary: exported.dictionary,
            release: Some(counting_release),
            private_data: inner.cast::<c_void>(),
        };

        (array, schema)
    }

    fn expected() -> Vec<(&'static [u8], DataType)> {
        vec![(b"id", DataType::Int64), (b"list", columns()[1].1.data_type().clone())]
    }

    fn imported() -> i64 {
        alloc::imported_bytes()
    }

    #[test]
    fn an_imported_batch_counts_its_buffers_until_the_last_one_drops() {
        let _serial = SERIAL.lock().unwrap();
        let (mut array, schema) = produced();
        let releases = RELEASES.load(Ordering::SeqCst);

        let (rows, arrays) = take(&schema, &mut array, &expected()).unwrap();

        assert!(array.release.is_none());
        assert_eq!(rows, 1000);
        assert_eq!(arrays.len(), 2);
        assert_eq!(arrays[0].as_ref(), columns()[0].1.as_ref());
        assert_eq!(arrays[1].as_ref(), columns()[1].1.as_ref());
        assert_eq!(
            imported(),
            arrays
                .iter()
                .map(|array| array.to_data().get_buffer_memory_size() as i64)
                .sum::<i64>()
        );

        let first = arrays.into_iter().next().unwrap();
        assert!(imported() > 0);
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases);

        drop(first);
        assert_eq!(imported(), 0);
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases + 1);

        drop(array);
        drop(schema);
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases + 1);
    }

    fn refused(columns: &[(&[u8], DataType)], refusal: impl Fn(&Refusal) -> bool) {
        let _serial = SERIAL.lock().unwrap();
        let (mut array, schema) = produced();
        let releases = RELEASES.load(Ordering::SeqCst);

        assert!(refusal(&take(&schema, &mut array, columns).unwrap_err()));
        assert_eq!(imported(), 0);
        assert!(array.release.is_some());
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases);

        drop(array);
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases + 1);
    }

    #[test]
    fn a_child_of_another_type_is_refused_before_the_move() {
        refused(
            &[(b"id", DataType::Float64), (b"list", expected()[1].1.clone())],
            |refusal| matches!(refusal, Refusal::Type { column: 0, .. }),
        );
    }

    #[test]
    fn a_child_of_another_name_is_refused_before_the_move() {
        refused(
            &[(b"id", DataType::Int64), (b"other", expected()[1].1.clone())],
            |refusal| matches!(refusal, Refusal::Name { column: 1, .. }),
        );
    }

    #[test]
    fn another_child_count_is_refused_before_the_move() {
        refused(&[(b"id", DataType::Int64)], |refusal| {
            matches!(
                refusal,
                Refusal::Count {
                    children: 2,
                    columns: 1
                }
            )
        });
    }

    #[test]
    fn an_already_imported_batch_is_refused() {
        let _serial = SERIAL.lock().unwrap();
        let (mut array, schema) = produced();

        let (_, arrays) = take(&schema, &mut array, &expected()).unwrap();
        let counted = imported();

        assert!(matches!(
            take(&schema, &mut array, &expected()).unwrap_err(),
            Refusal::Released
        ));
        assert_eq!(imported(), counted);

        drop(arrays);
        assert_eq!(imported(), 0);
    }
}
