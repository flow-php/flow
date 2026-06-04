mod parquet;
mod stream;

use std::alloc::System;

use ext_php_rs::prelude::*;
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};
use parquet::reader::Reader;
use parquet::writer::Writer;

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

/// # Safety
///
/// Invoked by the PHP/Zend engine during module startup. Must only be called by
/// the engine through the registered startup hook, never directly.
pub unsafe extern "C" fn module_startup(_type: i32, _module_number: i32) -> i32 {
    if let Err(e) = stream::output_stream::register() {
        eprintln!("arrow: failed to register Flow\\Arrow\\OutputStream: {e}");
        return -1;
    }
    if let Err(e) = stream::random_access_file::register() {
        eprintln!("arrow: failed to register Flow\\Arrow\\RandomAccessFile: {e}");
        return -1;
    }
    if let Err(e) = parquet::exception::register() {
        eprintln!("arrow: failed to register Flow\\Arrow\\Parquet\\Exception: {e}");
        return -1;
    }
    0
}

#[php_module]
#[php(startup = "module_startup")]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .info_function(php_module_info)
        .class::<Reader>()
        .class::<Writer>()
}
