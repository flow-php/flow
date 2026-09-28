mod alloc;
mod backend;
mod builder;
mod cast;
mod column;
mod csv;
mod ctx;
mod date_check;
mod exception;
mod globals;
mod interfaces;
mod json_check;
mod kind_builder;
mod physical;
mod plan;
mod render;
mod uuid_check;
mod values;

use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use ext_php_rs::zend::ModuleEntry;
use ext_php_rs::{info_table_end, info_table_row, info_table_start};

use crate::ctx::{ht_insert, zval_str};
use crate::exception::ext_exception;

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
    if let Err(e) = exception::register() {
        eprintln!("flow_php: failed to register Flow\\Floe\\Exception\\ExtensionException: {e}");
        return -1;
    }
    if let Err(e) = interfaces::register() {
        eprintln!("flow_php: failed to register the Flow\\ETL\\Column interfaces: {e}");
        return -1;
    }

    0
}

fn csv_batch_size(batch_size: i64) -> PhpResult<usize> {
    usize::try_from(batch_size)
        .ok()
        .filter(|size| *size > 0)
        .ok_or_else(|| ext_exception("flow_php CSV batch size must be greater than 0"))
}

/// Native counterpart of `CSVLineReader` + `CSVEncoder::decode()`: bytes in, rows or native columns out,
/// byte-identical to the PHP path. Resumable - `feed` any chunk size, `finish` at EOF.
#[php_class]
#[php(name = "Flow\\ETL\\Adapter\\CSV\\RustCSVReaderNative")]
pub struct RustCSVReaderNative {
    reader: csv::CsvReader,
    columns: Option<csv::columns::CsvColumns>,
}

#[php_impl]
impl RustCSVReaderNative {
    pub fn __construct(
        separator: BinarySlice<u8>,
        enclosure: BinarySlice<u8>,
        escape: BinarySlice<u8>,
        with_header: bool,
        empty_to_null: bool,
        remove_bom: bool,
    ) -> PhpResult<Self> {
        Ok(Self {
            reader: csv::CsvReader::new(&separator, &enclosure, &escape, with_header, empty_to_null, remove_bom)?,
            columns: None,
        })
    }

    pub fn feed(&mut self, chunk: BinarySlice<u8>) {
        self.reader.feed(&chunk);
    }

    pub fn finish(&mut self) {
        self.reader.finish();
    }

    pub fn headers(&mut self) -> PhpResult<Zval> {
        let headers = self.reader.headers();
        let mut list = ZendHashTable::with_capacity(headers.len() as u32);

        for header in headers {
            list.push(zval_str(header))
                .map_err(|e| ext_exception(format!("flow_php failed to collect a CSV header: {e:?}")))?;
        }

        let mut zv = Zval::new();
        zv.set_hashtable(list);

        Ok(zv)
    }

    /// Folds up to `limit` buffered rows (-1: all) into `fold`; returns how many it folded.
    pub fn fold(&mut self, fold: &mut RustColumnFoldNative, limit: i64) -> PhpResult<i64> {
        let limit = match limit {
            -1 => None,
            limit => Some(
                u64::try_from(limit).map_err(|_| ext_exception("flow_php CSV fold limit must be -1 or at least 0"))?,
            ),
        };

        Ok(self.reader.fold(&mut fold.fold, limit)? as i64)
    }

    #[php(name = "consumedBytes")]
    pub fn consumed_bytes(&self) -> i64 {
        self.reader.consumed_bytes() as i64
    }

    pub fn next(&mut self, batch_size: i64) -> PhpResult<Zval> {
        let mut zv = Zval::new();
        zv.set_hashtable(self.reader.next(csv_batch_size(batch_size)?)?);

        Ok(zv)
    }

    /// Exactly `$batchSize` rows keyed and ordered by `$schema` while that many are buffered, the remainder after
    /// `finish()`, else null. Refusals are `RowsBuilder::appendRows()`'s, row indexes relative to the batch.
    #[php(name = "nextColumns")]
    pub fn next_columns(&mut self, schema: &Zval, batch_size: i64) -> PhpResult<Zval> {
        csv::columns::next_columns(&mut self.reader, &mut self.columns, schema, csv_batch_size(batch_size)?)
    }
}

/// `ColumnTypes::observe()` over native CSV rows: a column fold seeded with the header names and gated by the
/// candidate types' `toString()` codes. HTML and XML candidates are never passed - the PHP side folds those itself.
#[php_class]
#[php(name = "Flow\\ETL\\Adapter\\CSV\\RustColumnFoldNative")]
pub struct RustColumnFoldNative {
    fold: csv::fold::Fold,
}

#[php_impl]
impl RustColumnFoldNative {
    pub fn __construct(names: &ZendHashTable, candidates: &ZendHashTable) -> PhpResult<Self> {
        Ok(Self {
            fold: csv::fold::Fold::new(csv::fold::string_values(names), &csv::fold::string_values(candidates))?,
        })
    }

    /// `StringTypeNarrower::narrow($value)->toString()` for the fold's candidates.
    #[php(name = "narrowOne")]
    pub fn narrow_one(&mut self, value: BinarySlice<u8>) -> PhpResult<String> {
        Ok(self.fold.narrower.narrow(&value)?.code().to_string())
    }

    /// Column name => type `toString()`, first-seen order.
    pub fn types(&self) -> Zval {
        let mut types = ZendHashTable::new();

        for (name, code) in self.fold.types() {
            ht_insert(&mut types, name, zval_str(code.as_bytes()));
        }

        let mut zv = Zval::new();
        zv.set_hashtable(types);

        zv
    }

    pub fn rows(&self) -> i64 {
        self.fold.rows() as i64
    }
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
        .class::<column::NativeColumn>()
        .class::<builder::NativeColumnBuilder>()
        .class::<backend::DefaultBackend>()
        .class::<RustCSVReaderNative>()
        .class::<RustColumnFoldNative>()
}
