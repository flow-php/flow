//! The global allocator: `System`, counting the bytes it holds for `DefaultBackend::allocatedBytes()`, plus the bytes
//! of the Arrow C Data batches imported from another extension's allocator while they live.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

static ALLOCATED: AtomicI64 = AtomicI64::new(0);
static IMPORTED: AtomicI64 = AtomicI64::new(0);

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

/// Bytes of imported batches: added on import, subtracted when the batch is released.
pub fn imported(delta: i64) {
    IMPORTED.fetch_add(delta, Ordering::Relaxed);
}

#[cfg(test)]
pub fn imported_bytes() -> i64 {
    IMPORTED.load(Ordering::Relaxed)
}

/// Process-wide: on ZTS every thread's allocations.
pub fn allocated_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed) + IMPORTED.load(Ordering::Relaxed)
}
