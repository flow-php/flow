mod alloc;
mod arrow_c;
mod backend;
mod batch_columns;
mod builder;
mod cast;
mod column;
mod csv;
mod ctx;
mod date_check;
mod exception;
mod globals;
mod interfaces;
mod iterator;
mod json;
mod json_check;
mod kind_builder;
mod parquet;
mod physical;
mod plan;
mod render;
mod source;
mod text;
mod uuid_check;
mod values;

use ext_php_rs::prelude::*;
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};

pub extern "C" fn php_module_info(_module: *mut ModuleEntry) {
    info_table_start!();
    info_table_row!("flow_php.enabled", "true");
    info_table_row!("flow_php.extension_version", env!("FLOW_PHP_EXT_VERSION"));
    info_table_end!();
}

/// # Safety
///
/// Only called by the PHP engine during module startup (MINIT).
pub unsafe extern "C" fn module_startup(_type: i32, _module_number: i32) -> i32 {
    if let Err(e) = interfaces::register() {
        eprintln!("flow_php: failed to register the Flow\\ETL interfaces: {e}");
        return -1;
    }

    0
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .version(env!("FLOW_PHP_EXT_VERSION"))
        .info_function(php_module_info)
        .globals(&globals::GLOBALS)
        .request_startup_function(globals::request_startup)
        .request_shutdown_function(globals::request_shutdown)
        .class::<column::RustColumn>()
        .class::<builder::RustColumnBuilder>()
        .class::<backend::RustBackend>()
        .class::<iterator::RustIterator>()
        .class::<csv::source::RustCSVOpenSource>()
        .class::<csv::write::RustCSVEncoder>()
        .class::<json::source::RustJsonOpenSource>()
        .class::<json::write::RustJSONEncoder>()
        .class::<parquet::etl::RustParquetOpenSource>()
        .class::<parquet::etl::RustParquetOpenSink>()
        .class::<arrow_c::RustColumnsBatch>()
}
