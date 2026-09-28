//! The global allocator: `System`, counting the bytes it holds for `DefaultBackend::allocatedBytes()`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

static ALLOCATED: AtomicI64 = AtomicI64::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);

        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }

        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);

        if !ptr.is_null() {
            ALLOCATED.fetch_add(layout.size() as i64, Ordering::Relaxed);
        }

        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout);
        ALLOCATED.fetch_sub(layout.size() as i64, Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new = System.realloc(ptr, layout, new_size);

        if !new.is_null() {
            ALLOCATED.fetch_add(new_size as i64 - layout.size() as i64, Ordering::Relaxed);
        }

        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Process-wide: on ZTS every thread's allocations.
pub fn allocated_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed)
}
