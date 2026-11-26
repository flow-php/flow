// a [[test]] entry in Cargo.toml would break the build from a release archive, which ships without tests/
#![cfg(feature = "zts-embed")]

use ext_php_rs::builders::SapiBuilder;
use ext_php_rs::embed::{ext_php_rs_sapi_shutdown, ext_php_rs_sapi_startup, Embed, PhpThreadGuard, SapiModule};
use ext_php_rs::ffi::{
    php_module_shutdown, php_module_startup, php_request_shutdown, php_request_startup, sapi_shutdown, sapi_startup,
};
use std::ffi::{c_char, CString};
use std::ptr::null_mut;

const THREADS: usize = 8;
const REQUESTS: usize = 5;
const NOT_ZTS: i64 = -2;
const NOTHING_HELD: i64 = -1;

extern "C" fn discard_output(_text: *const c_char, length: usize) -> usize {
    length
}

fn parquet_read_then_release() -> String {
    let phpt = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/phpt");

    format!(
        "!PHP_ZTS ? {NOT_ZTS} : (function () {{ require_once '{phpt}/bootstrap.php'; \
         $file = new \\Flow\\Parquet\\Engine\\RustParquetFileReader(\\Flow\\Filesystem\\DSL\\native_local_filesystem()\
         ->readFrom(\\Flow\\Filesystem\\DSL\\path('{phpt}/fixtures/arrow_schema_hints.parquet'))); \
         $columns = $file->readColumns(['id', 'plain', 'binary'], 10, arrow_read_plan($file)); \
         $columns->rewind(); $chunk = $columns->current(); $held = \\Flow\\Arrow\\allocated_bytes(); \
         unset($chunk, $columns, $file); $released = \\Flow\\Arrow\\allocated_bytes(); \
         return $held > $released ? $released : {NOTHING_HELD}; }})()"
    )
}

fn start_php_with_the_extension() {
    let extension = concat!(env!("CARGO_MANIFEST_DIR"), "/ext/modules/arrow.so");
    let sapi = Box::leak(Box::new(
        SapiBuilder::new("zts-request-threads", "ZTS request threads")
            .ub_write_function(discard_output)
            .build()
            .unwrap(),
    )) as *mut SapiModule;

    unsafe {
        ext_php_rs_sapi_startup();
        sapi_startup(sapi);
        // sapi_startup clears ini_entries, so the extension line goes in after it
        (*sapi).ini_entries = CString::new(format!("extension={extension}\n")).unwrap().into_raw();
        assert_eq!(
            php_module_startup(sapi, null_mut()),
            0,
            "php_module_startup failed, is {extension} built?"
        );
    }
}

fn shut_php_down() {
    unsafe {
        php_module_shutdown();
        sapi_shutdown();
        ext_php_rs_sapi_shutdown();
    }
}

fn request(code: &str) -> Option<i64> {
    assert_eq!(unsafe { php_request_startup() }, 0, "php_request_startup failed");
    let bytes = Embed::eval(code).ok().and_then(|value| value.long());
    unsafe { php_request_shutdown(null_mut()) };

    bytes
}

#[test]
fn every_request_on_a_thread_that_did_not_load_the_extension_releases_its_native_bytes() {
    start_php_with_the_extension();
    let workload = parquet_read_then_release();

    let threads: Vec<Vec<Option<i64>>> = (0..THREADS)
        .map(|_| {
            let workload = workload.clone();
            std::thread::spawn(move || {
                let _thread = PhpThreadGuard::new();
                (0..REQUESTS).map(|_| request(&workload)).collect()
            })
            .join()
            .unwrap()
        })
        .collect();

    shut_php_down();

    for (thread, bytes) in threads.iter().enumerate() {
        println!("thread {thread} native bytes after each request: {bytes:?}");
    }

    for (thread, bytes) in threads.iter().enumerate() {
        assert!(
            !bytes.contains(&Some(NOT_ZTS)),
            "this test needs a ZTS PHP with the embed SAPI"
        );
        assert!(
            bytes.iter().all(|b| b.is_some_and(|b| b >= 0)),
            "thread {thread}: a request failed or held no native bytes: {bytes:?}"
        );
        // the first request on a thread keeps the per-thread state of its first reader
        assert!(
            bytes[1..].iter().all(|b| *b == bytes[1]),
            "thread {thread}: native bytes differ between requests: {bytes:?}"
        );
    }
}
