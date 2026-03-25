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

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .info_function(php_module_info)
        .class::<Reader>()
        .class::<Writer>()
}
