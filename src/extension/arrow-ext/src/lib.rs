mod alloc;

use ext_php_rs::prelude::*;
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};
use flow_parquet_engine::{engine, interfaces, library, php};

#[php_function]
#[php(name = "Flow\\Arrow\\allocated_bytes")]
pub fn allocated_bytes() -> i64 {
    alloc::allocated_bytes()
}

pub extern "C" fn php_module_info(_module: *mut ModuleEntry) {
    info_table_start!();
    info_table_row!("arrow.enabled", "true");
    info_table_row!("arrow.extension_version", env!("ARROW_VERSION"));
    info_table_row!("arrow.library_version", env!("ARROW_LIB_VERSION"));
    info_table_row!("arrow.parquet_library_version", env!("PARQUET_LIB_VERSION"));
    info_table_end!();
}

/// The contract version flow-php/parquet expects (`Flow\Parquet\Engine\ArrowExtension::ABI`); both change together
/// whenever a registered class or interface, or the schema, options or values crossing them, change.
const FLOW_ARROW_ABI: i64 = 3;

/// # Safety
///
/// Invoked by the PHP/Zend engine during module startup. Must only be called by
/// the engine through the registered startup hook, never directly.
pub unsafe extern "C" fn module_startup(_type: i32, _module_number: i32) -> i32 {
    if let Err(e) = interfaces::register() {
        eprintln!("arrow: failed to register the Flow\\Parquet interfaces: {e}");
        return -1;
    }
    0
}

pub extern "C" fn request_startup(_type: i32, _module_number: i32) -> i32 {
    php::request_startup();

    0
}

pub extern "C" fn request_shutdown(_type: i32, _module_number: i32) -> i32 {
    php::request_shutdown();

    0
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .version(env!("ARROW_VERSION"))
        .info_function(php_module_info)
        .constant(("FLOW_ARROW_ABI", FLOW_ARROW_ABI, &[]))
        .request_startup_function(request_startup)
        .request_shutdown_function(request_shutdown)
        .function(wrap_function!(allocated_bytes))
        .class::<library::RustColumnsReader>()
        .class::<library::RustParquetFooter>()
        .class::<library::RustParquetFileReader>()
        .class::<library::RustParquetFileWriter>()
        .class::<engine::RustParquetEngine>()
}
