//! The global allocator: `System`, counting the bytes it holds. A cargo test counts per thread, so it pins what
//! arrow's native heap keeps while the other tests allocate on their own threads.

use std::alloc::{GlobalAlloc, Layout, System};

#[cfg(test)]
use std::cell::Cell;
#[cfg(not(test))]
use std::sync::atomic::{AtomicI64, Ordering};

#[cfg(test)]
thread_local! {
    static ALLOCATED: Cell<i64> = const { Cell::new(0) };
}

#[cfg(not(test))]
static ALLOCATED: AtomicI64 = AtomicI64::new(0);

#[cfg(test)]
fn counted(delta: i64) {
    // a thread being torn down has no counter left: nothing reads it again
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get() + delta));
}

#[cfg(not(test))]
fn counted(delta: i64) {
    ALLOCATED.fetch_add(delta, Ordering::Relaxed);
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);

        if !ptr.is_null() {
            counted(layout.size() as i64);
        }

        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);

        if !ptr.is_null() {
            counted(layout.size() as i64);
        }

        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        counted(-(layout.size() as i64));
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = System.realloc(ptr, layout, new_size);

        if !new.is_null() {
            counted(new_size as i64 - layout.size() as i64);
        }

        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// What the current thread holds.
#[cfg(test)]
pub fn allocated_bytes() -> i64 {
    ALLOCATED.with(Cell::get)
}

/// What the process holds through this allocator.
#[cfg(not(test))]
pub fn allocated_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed)
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
}
