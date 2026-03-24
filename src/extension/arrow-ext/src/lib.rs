mod parquet;
mod stream;

use std::alloc::System;

use ext_php_rs::prelude::*;
use parquet::reader::Reader;
use parquet::writer::Writer;

#[global_allocator]
static GLOBAL: System = System;

#[php_module]
pub fn get_module(module: ModuleBuilder) -> ModuleBuilder {
    module
        .class::<Reader>()
        .class::<Writer>()
}
