//! The global allocator: `System`, counting the bytes it holds for `RustBackend::allocatedBytes()`.

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
pub fn own_bytes() -> i64 {
    ALLOCATED.load(Ordering::Relaxed)
}

#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn release_freed() {
    extern "C" {
        fn malloc_trim(pad: usize) -> i32;
    }

    // SAFETY: malloc_trim takes no pointer and is safe to call at any time
    unsafe { malloc_trim(0) };
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn release_freed() {}

#[cfg(any(test, all(target_os = "linux", target_env = "gnu")))]
fn fixes_mmap_threshold(environment_threshold: Option<std::ffi::OsString>) -> bool {
    environment_threshold.is_none()
}

// glibc raises its mmap threshold as large blocks are freed, and freed large buffers then stay on the heap
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn fix_mmap_threshold() {
    const M_MMAP_THRESHOLD: i32 = -3;
    const MMAP_THRESHOLD_BYTES: i32 = 128 * 1024;

    extern "C" {
        fn mallopt(param: i32, value: i32) -> i32;
    }

    if fixes_mmap_threshold(std::env::var_os("MALLOC_MMAP_THRESHOLD_")) {
        // SAFETY: mallopt takes no pointer and is safe to call at any time
        unsafe { mallopt(M_MMAP_THRESHOLD, MMAP_THRESHOLD_BYTES) };
    }
}

#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn fix_mmap_threshold() {}

pub fn own_peak_bytes() -> i64 {
    PEAK.load(Ordering::Relaxed).max(own_bytes())
}

pub fn reset_peak() {
    PEAK.store(ALLOCATED.load(Ordering::Relaxed), Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::fixes_mmap_threshold;
    use std::ffi::OsString;

    #[test]
    fn fixes_mmap_threshold_when_the_environment_leaves_it_unset() {
        assert!(fixes_mmap_threshold(None));
    }

    #[test]
    fn leaves_mmap_threshold_to_the_environment_when_it_is_set() {
        assert!(!fixes_mmap_threshold(Some(OsString::from("65536"))));
    }
}
