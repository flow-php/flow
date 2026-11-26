//! The global allocator: `System`, counting the bytes each thread holds for `RustBackend::allocatedBytes()`.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ALLOCATED: Cell<i64> = const { Cell::new(0) };
    static PEAK: Cell<i64> = const { Cell::new(0) };
}

fn counted(delta: i64) {
    // a thread being torn down has no counter left: nothing reads it again
    let _ = ALLOCATED.try_with(|allocated| {
        let now = allocated.get() + delta;
        allocated.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now)
            }
        });
    });
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
pub fn own_bytes() -> i64 {
    ALLOCATED.with(Cell::get)
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
    PEAK.with(Cell::get).max(own_bytes())
}

pub fn reset_peak() {
    PEAK.with(|peak| peak.set(own_bytes()))
}

#[cfg(test)]
mod tests {
    use super::{fixes_mmap_threshold, own_bytes, own_peak_bytes, reset_peak};
    use std::ffi::OsString;
    use std::sync::mpsc;
    use std::thread;

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
        let before = own_bytes();
        let during = on_another_thread_while_it_holds(HELD, own_bytes);
        assert!(during - before < HELD as i64);
    }

    #[test]
    fn allocations_of_a_thread_count_on_that_thread() {
        thread::spawn(|| {
            let before = own_bytes();
            let held = vec![1u8; HELD];
            assert!(own_bytes() - before >= HELD as i64);
            drop(held);
            assert_eq!(own_bytes(), before);
        })
        .join()
        .unwrap();
    }

    #[test]
    fn peak_of_another_thread_leaves_the_current_thread_peak_unchanged() {
        reset_peak();
        on_another_thread_while_it_holds(4 * HELD, || {});
        assert!(own_peak_bytes() - own_bytes() < HELD as i64);
    }

    #[test]
    fn fixes_mmap_threshold_when_the_environment_leaves_it_unset() {
        assert!(fixes_mmap_threshold(None));
    }

    #[test]
    fn leaves_mmap_threshold_to_the_environment_when_it_is_set() {
        assert!(!fixes_mmap_threshold(Some(OsString::from("65536"))));
    }
}
