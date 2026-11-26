// a [[test]] entry in Cargo.toml would break the build from a release archive, which ships without tests/
#![cfg(feature = "zts-embed")]

use ext_php_rs::builders::SapiBuilder;
use ext_php_rs::embed::{ext_php_rs_sapi_shutdown, ext_php_rs_sapi_startup, Embed, PhpThreadGuard, SapiModule};
use ext_php_rs::ffi::{
    php_module_shutdown, php_module_startup, php_request_shutdown, php_request_startup, sapi_shutdown, sapi_startup,
};
use std::ffi::{c_char, CString};
use std::path::Path;
use std::ptr::null_mut;

const THREADS: usize = 8;
const REQUESTS: usize = 5;

const WORKLOAD: &str = r#"(static function (): int {
    $bytes = (new \Flow\ETL\Values\Column\RustBackend())->allocatedBytes();
    require_once getenv('FLOW_PHP_AUTOLOAD');

    $backend = new \Flow\ETL\Values\Column\RustBackend();
    $orders = \Flow\ETL\DSL\structure_schema('order', \Flow\Types\DSL\type_structure([
        'id' => \Flow\Types\DSL\structure_element('id', \Flow\Types\DSL\type_integer()),
        'tags' => \Flow\Types\DSL\structure_element('tags', \Flow\Types\DSL\type_list(\Flow\Types\DSL\type_string())),
        'at' => \Flow\Types\DSL\structure_element('at', \Flow\Types\DSL\type_optional(\Flow\Types\DSL\type_datetime())),
        'note' => \Flow\Types\DSL\structure_element('note', \Flow\Types\DSL\type_string(), optional: true),
    ]));
    $lists = \Flow\ETL\DSL\list_schema('lists', \Flow\Types\DSL\type_list(\Flow\Types\DSL\type_list(\Flow\Types\DSL\type_integer())));
    $maps = \Flow\ETL\DSL\map_schema('maps', \Flow\Types\DSL\type_map(\Flow\Types\DSL\type_string(), \Flow\Types\DSL\type_list(\Flow\Types\DSL\type_string())));

    $builder = $backend->builder($orders);
    $builder->appendMany([
        ['id' => 1, 'tags' => ['a', 'b'], 'at' => '2026-01-02T03:04:05Z', 'note' => 'n'],
        ['id' => '2', 'tags' => [], 'at' => null],
        ['id' => 3, 'tags' => '["json", "text"]', 'at' => null],
    ]);
    $builder->finish()->values();
    $backend->builder($lists)->appendMany([[[1, 2], [3]], [[4, '5']], [[6, 7], [8]]]);
    $backend->builder($maps)->appendMany([['a' => ['x'], 'b' => []], ['c' => ['y', 'z']]]);

    return $bytes;
})()"#;

extern "C" fn discard_output(_text: *const c_char, length: usize) -> usize {
    length
}

fn start(extension: &Path) {
    let sapi = Box::leak(Box::new(
        SapiBuilder::new("zts-request-threads", "ZTS request threads")
            .ub_write_function(discard_output)
            .build()
            .expect("the SAPI module builds"),
    )) as *mut SapiModule;

    unsafe {
        ext_php_rs_sapi_startup();
        sapi_startup(sapi);
        (*sapi).ini_entries = CString::new(format!("extension={}\n", extension.display()))
            .unwrap()
            .into_raw();
        assert_eq!(php_module_startup(sapi, null_mut()), 0, "php_module_startup failed");
    }
}

fn stop() {
    unsafe {
        php_module_shutdown();
        sapi_shutdown();
        ext_php_rs_sapi_shutdown();
    }
}

fn request() -> Option<i64> {
    assert_eq!(unsafe { php_request_startup() }, 0, "php_request_startup failed");
    let bytes = Embed::eval(WORKLOAD).ok().and_then(|value| value.long());
    unsafe { php_request_shutdown(null_mut()) };

    bytes
}

#[test]
fn every_request_on_a_thread_that_did_not_load_the_extension_releases_its_native_bytes() {
    assert!(
        ext_php_rs::PHP_ZTS,
        "zts_request_threads needs a ZTS PHP, this one is NTS"
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let extension = root.join("ext/modules/flow_php.so");
    let autoload = root.join("../../../vendor/autoload.php");
    assert!(
        extension.is_file(),
        "{} is missing, run make build first",
        extension.display()
    );
    assert!(
        autoload.is_file(),
        "{} is missing, run composer install in the repository root",
        autoload.display()
    );
    std::env::set_var("FLOW_PHP_AUTOLOAD", &autoload);

    start(&extension);
    let threads: Vec<Vec<Option<i64>>> = (0..THREADS)
        .map(|_| {
            std::thread::spawn(|| {
                let _thread = PhpThreadGuard::new();
                (0..REQUESTS).map(|_| request()).collect()
            })
            .join()
            .expect("a request thread panicked")
        })
        .collect();
    stop();

    for (thread, bytes) in threads.iter().enumerate() {
        assert!(
            bytes.iter().all(Option::is_some),
            "thread {thread}: a request failed: {bytes:?}"
        );
        // the first request of a thread may keep per-thread state, so the comparison starts at the second
        assert!(
            bytes[1..].iter().all(|b| *b == bytes[1]),
            "thread {thread}: native bytes grew across requests: {bytes:?}"
        );
    }
}
