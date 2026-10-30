//! `Flow\Parquet\Engine\RustParquetEngine`: `flow-php/parquet`'s arrow engine, its files read and written by
//! `RustParquetFileReader` / `RustParquetFileWriter`.

use ext_php_rs::boxed::ZBox;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::exception::ext_exception;
use crate::interfaces::parquet_engine_ce;
use crate::parquet::lib_calls::{
    call_static, OPTIONS_DEFAULT, OPTIONS_FROM_ARRAY, OPTIONS_TO_ENGINE, OPTIONS_TO_EXTENSION, SCHEMA_TO_EXTENSION,
};
use crate::parquet::library::{RustParquetFileReader, RustParquetFileWriter};
use crate::php::{call_method, ht_get, ht_insert, IterableArg};
use crate::render::{exception, INVALID_ARGUMENT};

#[php_class]
#[php(
    name = "Flow\\Parquet\\Engine\\RustParquetEngine",
    flags = ClassFlags::Final,
    implements(ce = parquet_engine_ce, stub = "Flow\\Parquet\\ParquetEngine")
)]
pub struct RustParquetEngine {
    int96_as_datetime: bool,
    /// `Options::normalize()` of the options this engine was built with, what `normalize()` hands back
    options: Zval,
}

#[php_impl]
impl RustParquetEngine {
    pub fn __construct(options: Option<&Zval>) -> PhpResult<Self> {
        let options = match options {
            Some(options) if !options.is_null() => options.shallow_clone(),
            _ => call_static(OPTIONS_DEFAULT, &mut [])?,
        };
        let engine = call_static(OPTIONS_TO_ENGINE, &mut [options.shallow_clone()])?;
        let engine = engine
            .array()
            .ok_or_else(|| ext_exception("arrow expected OptionsConverter::toEngine() to return an array"))?;
        let normalized = call_method(&options, "normalize", &mut [])?;

        if normalized.array().is_none() {
            return Err(ext_exception("arrow expected Options::normalize() to return an array"));
        }

        Ok(Self {
            int96_as_datetime: ht_get(engine, b"INT_96_AS_DATETIME")
                .and_then(Zval::bool)
                .ok_or_else(|| {
                    ext_exception("arrow expected OptionsConverter::toEngine() to hold INT_96_AS_DATETIME")
                })?,
            options: normalized,
        })
    }

    /// `['options' => Options::normalize()]`, what `fromArray()` rebuilds this engine from.
    pub fn normalize(&self) -> ZBox<ZendHashTable> {
        let mut configuration = ZendHashTable::new();
        ht_insert(&mut configuration, b"options", self.options.shallow_clone());

        configuration
    }

    #[php(name = "fromArray")]
    pub fn from_array(configuration: &ZendHashTable) -> PhpResult<Self> {
        let options = ht_get(configuration, b"options")
            .filter(|options| options.array().is_some())
            .ok_or_else(|| {
                exception(
                    INVALID_ARGUMENT,
                    "Parquet engine configuration must hold an \"options\" array".to_string(),
                )
            })?;
        let options = call_static(OPTIONS_FROM_ARRAY, &mut [options.shallow_clone()])?;

        Self::__construct(Some(&options))
    }

    #[php(name = "openForRead")]
    pub fn open_for_read(&self, stream: &Zval) -> PhpResult<RustParquetFileReader> {
        RustParquetFileReader::__construct(stream, self.int96_as_datetime)
    }

    #[php(name = "openForWrite")]
    pub fn open_for_write(
        &self,
        stream: &Zval,
        schema: &Zval,
        compression: &Zval,
        options: &Zval,
    ) -> PhpResult<RustParquetFileWriter> {
        let schema = call_static(SCHEMA_TO_EXTENSION, &mut [schema.shallow_clone()])?;
        let options = call_static(OPTIONS_TO_EXTENSION, &mut [options.shallow_clone()])?;
        let options = options
            .array()
            .ok_or_else(|| ext_exception("arrow expected OptionsConverter::toExtension() to return an array"))?;

        RustParquetFileWriter::__construct(
            stream,
            schema
                .array()
                .ok_or_else(|| ext_exception("arrow expected SchemaConverter::toExtension() to return an array"))?,
            compression,
            options,
            ht_get(options, b"ARROW_WRITE_BATCH_SIZE")
                .and_then(Zval::long)
                .ok_or_else(|| {
                    ext_exception("arrow expected OptionsConverter::toExtension() to hold ARROW_WRITE_BATCH_SIZE")
                })?,
        )
    }

    /// The writer is closed even when a row is refused; the refusal is what throws.
    #[php(name = "writeRows")]
    pub fn write_rows(
        &self,
        stream: &Zval,
        schema: &Zval,
        compression: &Zval,
        options: &Zval,
        rows: IterableArg,
    ) -> PhpResult<()> {
        let mut writer = self.open_for_write(stream, schema, compression, options)?;

        match writer.write_batch(rows) {
            Ok(()) => writer.close(),
            Err(e) => {
                let _ = writer.close();

                Err(e)
            }
        }
    }
}
