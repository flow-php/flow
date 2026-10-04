//! The `flow-php/parquet` converters arrow calls by name: mago cannot see these calls, the phpts pin each one.

use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;

use crate::exception::ext_exception;
use crate::php::{call_handle_transparent, ce_method_ref, find_class};

pub const METADATA_FROM_THRIFT: &str = "Flow\\Parquet\\ParquetFile\\Metadata::fromThrift";
pub const SCHEMA_FROM_THRIFT: &str = "Flow\\Parquet\\ParquetFile\\Schema::fromThrift";
pub const SCHEMA_TO_EXTENSION: &str = "Flow\\Parquet\\Engine\\Arrow\\SchemaConverter::toExtension";
pub const OPTIONS_TO_EXTENSION: &str = "Flow\\Parquet\\Engine\\Arrow\\OptionsConverter::toExtension";
pub const OPTIONS_TO_ENGINE: &str = "Flow\\Parquet\\Engine\\Arrow\\OptionsConverter::toEngine";

/// `Class::method(...$args)`; an exception the method throws surfaces as itself.
pub fn call_static(name: &str, args: &mut [Zval]) -> PhpResult<Zval> {
    let (class, method) = name
        .rsplit_once("::")
        .ok_or_else(|| ext_exception(format!("arrow expected a static method name, got \"{name}\"")))?;

    call_handle_transparent(ce_method_ref(find_class(class)?, method)?, None, args)
}
