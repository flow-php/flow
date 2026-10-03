#[cfg(test)]
mod alloc;
mod exception;
mod interfaces;
mod parquet;
mod php;
mod render;
mod thrift;
mod values;

#[cfg(not(test))]
use std::alloc::System;

use ext_php_rs::prelude::*;
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};

#[cfg(not(test))]
#[global_allocator]
static GLOBAL: System = System;

pub extern "C" fn php_module_info(_module: *mut ModuleEntry) {
    info_table_start!();
    info_table_row!("arrow.enabled", "true");
    info_table_row!("arrow.extension_version", env!("ARROW_VERSION"));
    info_table_row!("arrow.library_version", env!("ARROW_LIB_VERSION"));
    info_table_row!("arrow.parquet_library_version", env!("PARQUET_LIB_VERSION"));
    info_table_end!();
}

/// The contract version flow-php/parquet expects (`Flow\Parquet\Engine\ArrowExtension::ABI`); both change together
/// whenever a registered class or interface changes.
const FLOW_ARROW_ABI: i64 = 1;

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

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .version(env!("ARROW_VERSION"))
        .info_function(php_module_info)
        .constant(("FLOW_ARROW_ABI", FLOW_ARROW_ABI, &[]))
        .globals(&php::GLOBALS)
        .request_startup_function(php::request_startup)
        .request_shutdown_function(php::request_shutdown)
        .class::<parquet::library::RustColumnsReader>()
        .class::<parquet::library::RustParquetFileReader>()
        .class::<parquet::library::RustParquetFileWriter>()
        .class::<parquet::engine::RustParquetEngine>()
        .class::<parquet::batch::RustBatchReader>()
        .class::<parquet::batch::RustParquetBatch>()
        .class::<parquet::batch::RustArrowSchema>()
}
