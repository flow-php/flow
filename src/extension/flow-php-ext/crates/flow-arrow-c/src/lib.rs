//! Batches across the Arrow C Data Interface, to and from another extension (arrow-ext): one batch = one struct-typed
//! `FFI_ArrowSchema` (one child per column) plus one struct-typed `FFI_ArrowArray`. An import moves the producer's
//! array behind a release forwarder that counts its buffers in [`imported_bytes`] for as long as they live; flow_php
//! adds that count to its allocator's.

use std::ffi::{CStr, CString};
use std::sync::atomic::{AtomicI64, Ordering};

use arrow_array::ffi::{from_ffi_and_data_type, to_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_schema::{ArrowError, DataType, Field, Fields};

/// Bytes of the imported batches alive right now: added on import, subtracted when a batch's last buffer drops.
static IMPORTED: AtomicI64 = AtomicI64::new(0);

pub fn imported_bytes() -> i64 {
    IMPORTED.load(Ordering::Relaxed)
}

/// What an import refuses; nothing was taken from the producer.
#[derive(Debug)]
pub enum Refusal {
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

/// What every batch of one source must carry, resolved once by [`check`]: the struct type the arrays are imported
/// as, and each child's name and format string as the producer writes them.
#[derive(Debug)]
pub struct Expected {
    data_type: DataType,
    children: Vec<(CString, CString)>,
}

/// The struct schema `schema`, its children named and typed as `columns` (name, stored type) are.
pub fn check(schema: &FFI_ArrowSchema, columns: &[(&[u8], DataType)]) -> Result<Expected, Refusal> {
    if schema.format.is_null() || unsafe { CStr::from_ptr(schema.format) }.to_bytes() != b"+s" {
        return Err(Refusal::NotStruct);
    }

    if schema.n_children != columns.len() as i64 {
        return Err(Refusal::Count {
            children: schema.n_children,
            columns: columns.len(),
        });
    }

    let mut fields = Vec::with_capacity(columns.len());
    let mut children = Vec::with_capacity(columns.len());

    for (column, (child, (name, stored))) in schema.children().zip(columns).enumerate() {
        let field = Field::try_from(child).map_err(Refusal::Schema)?;

        if field.name().as_bytes() != *name {
            return Err(Refusal::Name {
                child: field.name().clone(),
                column,
            });
        }

        if field.data_type() != stored {
            return Err(Refusal::Type { column, child: field });
        }

        children.push((
            unsafe { CStr::from_ptr(child.name) }.to_owned(),
            unsafe { CStr::from_ptr(child.format) }.to_owned(),
        ));
        fields.push(field);
    }

    Ok(Expected {
        data_type: DataType::Struct(Fields::from(fields)),
        children,
    })
}

/// Refuses a struct schema whose children are not `expected`'s by count, name or format: the per-batch guard, with no
/// `Field` built.
pub fn same_children(schema: &FFI_ArrowSchema, expected: &Expected) -> Result<(), Refusal> {
    if schema.format.is_null() || unsafe { CStr::from_ptr(schema.format) }.to_bytes() != b"+s" {
        return Err(Refusal::NotStruct);
    }

    if schema.n_children != expected.children.len() as i64 {
        return Err(Refusal::Count {
            children: schema.n_children,
            columns: expected.children.len(),
        });
    }

    for (column, (child, (name, format))) in schema.children().zip(&expected.children).enumerate() {
        if unsafe { CStr::from_ptr(child.name) } != name.as_c_str() {
            return Err(Refusal::Name {
                child: String::from_utf8_lossy(unsafe { CStr::from_ptr(child.name) }.to_bytes()).into_owned(),
                column,
            });
        }

        if unsafe { CStr::from_ptr(child.format) } != format.as_c_str() {
            return Err(Refusal::Type {
                column,
                child: Field::try_from(child).map_err(Refusal::Schema)?,
            });
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

    IMPORTED.fetch_sub(bytes, Ordering::Relaxed);
    drop(inner);
    array.release = None;
}

/// `source` moved out (its release left NULL) behind the counting forwarder, as `expected` describes once its struct
/// `schema` passed [`same_children`]: the row count and one array per column. A refusal before the move takes
/// nothing; after it the forwarder owns the producer's array and releases it.
pub fn take(
    schema: &FFI_ArrowSchema,
    source: &mut FFI_ArrowArray,
    expected: &Expected,
) -> Result<(usize, Vec<ArrayRef>), Refusal> {
    same_children(schema, expected)?;

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

    let data = unsafe { from_ffi_and_data_type(forward, expected.data_type.clone()) }
        .and_then(|data| data.validate().map(|()| data))
        .map_err(Refusal::Invalid)?;
    let bytes = data.get_buffer_memory_size() as i64;

    // the forwarder lives in arrow-rs's owner of `data`'s buffers, so `counted` does too
    unsafe { (*counted).bytes = bytes };
    IMPORTED.fetch_add(bytes, Ordering::Relaxed);

    let batch = StructArray::from(data);

    Ok((batch.len(), batch.into_parts().1))
}

/// `columns` as one struct batch of `rows` rows, every child nullable.
pub fn export(rows: usize, columns: Vec<(String, ArrayRef)>) -> Result<(FFI_ArrowSchema, FFI_ArrowArray), ArrowError> {
    let fields = columns
        .iter()
        .map(|(name, array)| Field::new(name, array.data_type().clone(), true))
        .collect::<Fields>();
    let arrays = columns.into_iter().map(|(_, array)| array).collect();
    let (array, schema) =
        StructArray::try_new_with_length(fields, arrays, None, rows).and_then(|batch| to_ffi(&batch.to_data()))?;

    Ok((schema, array))
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

    use super::{check, imported_bytes, same_children, take, Expected, Refusal};

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

    fn stored() -> Vec<(&'static [u8], DataType)> {
        vec![(b"id", DataType::Int64), (b"list", columns()[1].1.data_type().clone())]
    }

    /// From a producer whose release is arrow-rs's own, so building it counts no release of `produced()`'s.
    fn expected() -> Expected {
        let (_, schema) = other_producer(&stored());

        check(&schema, &stored()).unwrap()
    }

    fn imported() -> i64 {
        imported_bytes()
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

    /// `check` refuses the open, `take` through `same_children` refuses the batch; neither moves the array.
    fn refused(columns: &[(&[u8], DataType)], refusal: impl Fn(&Refusal) -> bool) {
        let _serial = SERIAL.lock().unwrap();
        let (mut array, schema) = produced();
        let releases = RELEASES.load(Ordering::SeqCst);

        assert!(refusal(&check(&schema, columns).unwrap_err()));

        let (_, other) = other_producer(columns);
        let expected = check(&other, columns).unwrap();

        assert!(refusal(&take(&schema, &mut array, &expected).unwrap_err()));
        assert_eq!(imported(), 0);
        assert!(array.release.is_some());
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases);

        drop(array);
        assert_eq!(RELEASES.load(Ordering::SeqCst), releases + 1);
    }

    /// A producer whose batch has exactly `columns`.
    fn other_producer(columns: &[(&[u8], DataType)]) -> (FFI_ArrowArray, FFI_ArrowSchema) {
        let fields = columns
            .iter()
            .map(|(name, data_type)| Field::new(String::from_utf8_lossy(name), data_type.clone(), true))
            .collect::<Fields>();
        let arrays = fields
            .iter()
            .map(|field| arrow_array::new_empty_array(field.data_type()))
            .collect::<Vec<_>>();

        to_ffi(&StructArray::new(fields, arrays, None).to_data()).unwrap()
    }

    #[test]
    fn a_child_of_another_type_is_refused_before_the_move() {
        refused(
            &[(b"id", DataType::Float64), (b"list", stored()[1].1.clone())],
            |refusal| matches!(refusal, Refusal::Type { column: 0, .. }),
        );
    }

    #[test]
    fn a_child_of_another_name_is_refused_before_the_move() {
        refused(
            &[(b"id", DataType::Int64), (b"other", stored()[1].1.clone())],
            |refusal| matches!(refusal, Refusal::Name { column: 1, .. }),
        );
    }

    #[test]
    fn same_children_compares_names_and_formats_without_building_fields() {
        let _serial = SERIAL.lock().unwrap();
        let (_array, schema) = produced();

        assert!(same_children(&schema, &expected()).is_ok());

        let (_, renamed) = other_producer(&[(b"id", DataType::Int64), (b"other", stored()[1].1.clone())]);
        assert!(matches!(
            same_children(&renamed, &expected()),
            Err(Refusal::Name { column: 1, .. })
        ));

        let (_, retyped) = other_producer(&[(b"id", DataType::Float64), (b"list", stored()[1].1.clone())]);
        assert!(matches!(
            same_children(&retyped, &expected()),
            Err(Refusal::Type { column: 0, .. })
        ));

        let (_, fewer) = other_producer(&[(b"id", DataType::Int64)]);
        assert!(matches!(
            same_children(&fewer, &expected()),
            Err(Refusal::Count {
                children: 1,
                columns: 2
            })
        ));
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
