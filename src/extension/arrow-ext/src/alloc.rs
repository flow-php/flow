//! The global allocator: `System`, counting the bytes each thread holds.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATED: Cell<i64> = const { Cell::new(0) };
}

fn counted(delta: i64) {
    // a thread being torn down has no counter left: nothing reads it again
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get() + delta));
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
pub fn allocated_bytes() -> i64 {
    ALLOCATED.with(Cell::get)
}

#[cfg(test)]
mod tests {
    use std::sync::{mpsc, Arc};
    use std::thread;

    use arrow_array::ffi::{from_ffi, to_ffi};
    use arrow_array::types::Int64Type;
    use arrow_array::{Array, ArrayRef, Int64Array, ListArray, StructArray};
    use arrow_schema::{DataType, Field, Fields};

    use crate::alloc::allocated_bytes;

    const HELD: usize = 1 << 20;

    fn on_another_thread_while_it_holds<T>(bytes: usize, read: impl FnOnce() -> T) -> T {
        let (held_tx, held_rx) = mpsc::sync_channel(0);
        let (release_tx, release_rx) = mpsc::sync_channel::<()>(0);
        thread::scope(|scope| {
            scope.spawn(move || {
                let held = vec![1u8; bytes];
                held_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                drop(held);
            });
            held_rx.recv().unwrap();
            let seen = read();
            release_tx.send(()).unwrap();
            seen
        })
    }

    #[test]
    fn allocations_of_another_thread_leave_the_current_thread_bytes_unchanged() {
        let before = allocated_bytes();
        let during = on_another_thread_while_it_holds(HELD, allocated_bytes);
        assert!(during - before < HELD as i64);
    }

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
