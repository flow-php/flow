//! The global allocator: `System`, counting the bytes it holds for `RustBackend::allocatedBytes()`, plus the bytes
//! of the Arrow C Data batches imported from another extension's allocator while they live (`flow_arrow_c`).

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicI64, Ordering};

static ALLOCATED: AtomicI64 = AtomicI64::new(0);
static PEAK: AtomicI64 = AtomicI64::new(0);

fn added(bytes: i64) {
    let now = ALLOCATED.fetch_add(bytes, Ordering::Relaxed) + bytes;

    if now > PEAK.load(Ordering::Relaxed) {
        PEAK.fetch_max(now, Ordering::Relaxed);
    }
}

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc(layout);

        if !ptr.is_null() {
            added(layout.size() as i64);
        }

        ptr
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = System.alloc_zeroed(layout);

        if !ptr.is_null() {
            added(layout.size() as i64);
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
            added(new_size as i64 - layout.size() as i64);
        }

        new
    }
}

#[global_allocator]
static GLOBAL: Counting = Counting;

/// Process-wide: on ZTS every thread's allocations.
pub fn allocated_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed) + flow_arrow_c::imported_bytes()
}

/// Imported Arrow buffers are another allocator's: they count as they are now, not at their own peak.
pub fn allocated_peak_bytes() -> i64 {
    PEAK.load(Ordering::Relaxed).max(allocated_bytes())
}

pub fn reset_peak() {
    PEAK.store(ALLOCATED.load(Ordering::Relaxed), Ordering::Relaxed);
}
