//! `Flow\ETL\Column\{Column, ColumnBuilder, Backend}` and the CSV / JSON / Parquet adapters' encoder, open source and
//! open sink contracts: registered at MINIT with the package
//! files' exact signatures, so the extension's classes implement them and the package files (autoloaded only without
//! flow_php) stay unused.

use std::sync::atomic::{AtomicPtr, Ordering};

use ext_php_rs::args::{Arg, ArgInfoTables};
use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::error::Result;
use ext_php_rs::flags::{ClassFlags, DataType, MethodFlags};
use ext_php_rs::zend::ClassEntry;

const DEFINITION: DataType = DataType::object("Flow\\ETL\\Schema\\Definition");
const TYPE: DataType = DataType::object("Flow\\Types\\Type");
const COLUMN: DataType = DataType::object("Flow\\ETL\\Column\\Column");
const COLUMN_BUILDER: DataType = DataType::object("Flow\\ETL\\Column\\ColumnBuilder");
const SELF: DataType = DataType::object("self");
const ROWS: DataType = DataType::object("Flow\\ETL\\Rows");
const SCHEMA: DataType = DataType::object("Flow\\ETL\\Schema");
const BACKEND: DataType = DataType::object("Flow\\ETL\\Column\\Backend");
const ITERATOR: DataType = DataType::object("Iterator");
const COLUMN_TYPES: DataType = DataType::object("Flow\\ETL\\Schema\\Inference\\ColumnTypes");
const SCHEMA_INFERENCE: DataType = DataType::object("Flow\\ETL\\Schema\\Inference\\SchemaInference");
const TYPE_NARROWER: DataType = DataType::object("Flow\\Types\\Type\\TypeNarrower");

// ClassEntry::try_find reads EG(class_table), which MINIT has not set, so each CE is kept from its registration
static COLUMN_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static COLUMN_BUILDER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static BACKEND_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static PARQUET_OPEN_SINK_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static CSV_ENCODER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static JSON_ENCODER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static CSV_OPEN_SOURCE_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static JSON_OPEN_SOURCE_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static PARQUET_OPEN_SOURCE_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());

fn store_column(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    COLUMN_CE.store(ce, Ordering::Release);
}

fn store_column_builder(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    COLUMN_BUILDER_CE.store(ce, Ordering::Release);
}

fn store_backend(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    BACKEND_CE.store(ce, Ordering::Release);
}

fn store_parquet_open_sink(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    PARQUET_OPEN_SINK_CE.store(ce, Ordering::Release);
}

fn store_csv_encoder(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    CSV_ENCODER_CE.store(ce, Ordering::Release);
}

fn store_json_encoder(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    JSON_ENCODER_CE.store(ce, Ordering::Release);
}

fn store_csv_open_source(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    CSV_OPEN_SOURCE_CE.store(ce, Ordering::Release);
}

fn store_json_open_source(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    JSON_OPEN_SOURCE_CE.store(ce, Ordering::Release);
}

fn store_parquet_open_source(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    PARQUET_OPEN_SOURCE_CE.store(ce, Ordering::Release);
}

/// The engine borrows an interface's argument info tables for the life of the process.
fn keep(arg_info: ArgInfoTables) {
    std::mem::forget(arg_info);
}

fn load(ce: &AtomicPtr<ClassEntry>, name: &str) -> &'static ClassEntry {
    unsafe { ce.load(Ordering::Acquire).as_ref() }.unwrap_or_else(|| panic!("{name} not registered"))
}

pub fn column_ce() -> &'static ClassEntry {
    load(&COLUMN_CE, "Flow\\ETL\\Column\\Column")
}

pub fn column_builder_ce() -> &'static ClassEntry {
    load(&COLUMN_BUILDER_CE, "Flow\\ETL\\Column\\ColumnBuilder")
}

pub fn backend_ce() -> &'static ClassEntry {
    load(&BACKEND_CE, "Flow\\ETL\\Column\\Backend")
}

pub fn parquet_open_sink_ce() -> &'static ClassEntry {
    load(&PARQUET_OPEN_SINK_CE, "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSink")
}

pub fn csv_encoder_ce() -> &'static ClassEntry {
    load(&CSV_ENCODER_CE, "Flow\\ETL\\Adapter\\CSV\\CSVEncoder")
}

pub fn json_encoder_ce() -> &'static ClassEntry {
    load(&JSON_ENCODER_CE, "Flow\\ETL\\Adapter\\JSON\\JSONEncoder")
}

pub fn csv_open_source_ce() -> &'static ClassEntry {
    load(&CSV_OPEN_SOURCE_CE, "Flow\\ETL\\Adapter\\CSV\\CSVOpenSource")
}

pub fn json_open_source_ce() -> &'static ClassEntry {
    load(&JSON_OPEN_SOURCE_CE, "Flow\\ETL\\Adapter\\JSON\\JsonOpenSource")
}

pub fn parquet_open_source_ce() -> &'static ClassEntry {
    load(
        &PARQUET_OPEN_SOURCE_CE,
        "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSource",
    )
}

fn method(name: &str, args: Vec<Arg<'static>>, returns: DataType) -> FunctionBuilder<'static> {
    args.into_iter()
        .fold(FunctionBuilder::new_abstract(name), FunctionBuilder::arg)
        .returns(returns, false, false)
}

fn interface(
    name: &str,
    store: fn(&'static mut ClassEntry, ArgInfoTables),
    methods: Vec<FunctionBuilder<'static>>,
) -> Result<()> {
    methods
        .into_iter()
        .fold(
            ClassBuilder::new(name).flags(ClassFlags::Interface).registration(store),
            |class, method| class.method(method, MethodFlags::Public | MethodFlags::Abstract),
        )
        .register()
}

pub fn register() -> Result<()> {
    interface(
        "Flow\\ETL\\Column\\Column",
        store_column,
        vec![
            method("type", vec![], TYPE),
            method("count", vec![], DataType::Long),
            method("isNull", vec![Arg::new("i", DataType::Long)], DataType::Bool),
            method("nullCount", vec![], DataType::Long),
            method("at", vec![Arg::new("i", DataType::Long)], DataType::Mixed),
            method("physicals", vec![], DataType::Array),
            method("value", vec![Arg::new("i", DataType::Long)], DataType::Mixed),
            method("values", vec![], DataType::Array),
            method(
                "slice",
                vec![Arg::new("offset", DataType::Long), Arg::new("length", DataType::Long)],
                SELF,
            ),
            method("take", vec![Arg::new("indices", DataType::Array)], SELF),
            method("withType", vec![Arg::new("type", TYPE)], SELF),
            method("concat", vec![Arg::new("others", SELF).is_variadic()], SELF),
            method("encode", vec![], DataType::Array),
        ],
    )?;
    interface(
        "Flow\\ETL\\Column\\ColumnBuilder",
        store_column_builder,
        vec![
            method("append", vec![Arg::new("value", DataType::Mixed)], DataType::Void),
            method("appendMany", vec![Arg::new("values", DataType::Array)], DataType::Void),
            method(
                "appendFrom",
                vec![Arg::new("column", COLUMN), Arg::new("i", DataType::Long)],
                DataType::Void,
            ),
            method(
                "appendTake",
                vec![Arg::new("column", COLUMN), Arg::new("indices", DataType::Array)],
                DataType::Void,
            ),
            method("count", vec![], DataType::Long),
            method("finish", vec![], COLUMN),
        ],
    )?;
    interface(
        "Flow\\ETL\\Column\\Backend",
        store_backend,
        vec![
            method("builder", vec![Arg::new("definition", DEFINITION)], COLUMN_BUILDER),
            method(
                "constant",
                vec![
                    Arg::new("definition", DEFINITION),
                    Arg::new("value", DataType::Mixed),
                    Arg::new("count", DataType::Long),
                ],
                COLUMN,
            ),
            method(
                "decode",
                vec![
                    Arg::new("definition", DEFINITION),
                    Arg::new("buffers", DataType::Array),
                    Arg::new("count", DataType::Long),
                    Arg::new("nullCount", DataType::Long),
                ],
                COLUMN,
            ),
            method(
                "adopt",
                vec![Arg::new("definition", DEFINITION), Arg::new("column", COLUMN)],
                COLUMN,
            ),
            method("allocatedBytes", vec![], DataType::Long),
        ],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSink",
        store_parquet_open_sink,
        vec![
            method("close", vec![], DataType::Void),
            method("write", vec![Arg::new("rows", ROWS)], DataType::Void),
        ],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\CSV\\CSVEncoder",
        store_csv_encoder,
        vec![
            method("encode", vec![Arg::new("rows", ROWS)], DataType::String),
            method(
                "encodeHeader",
                vec![Arg::new("headers", DataType::Array)],
                DataType::String,
            ),
        ],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\JSON\\JSONEncoder",
        store_json_encoder,
        vec![method(
            "encode",
            vec![Arg::new("rows", ROWS), Arg::new("separator", DataType::String)],
            DataType::String,
        )],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\CSV\\CSVOpenSource",
        store_csv_open_source,
        vec![
            method("close", vec![], DataType::Void),
            method("producedBytes", vec![], DataType::Long),
            method("producedRows", vec![], DataType::Long),
            method("columns", vec![], DataType::Array),
            method(
                "batches",
                vec![
                    Arg::new("schema", SCHEMA),
                    Arg::new("batchSize", DataType::Long),
                    Arg::new("backend", BACKEND),
                ],
                ITERATOR,
            ),
            method("headers", vec![], DataType::Array),
            method("records", vec![], ITERATOR),
            method(
                "sniff",
                vec![
                    Arg::new("names", DataType::Array),
                    Arg::new("rowBudget", DataType::Long),
                    Arg::new("inference", SCHEMA_INFERENCE),
                    Arg::new("typer", TYPE_NARROWER),
                ],
                COLUMN_TYPES,
            ),
        ],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\JSON\\JsonOpenSource",
        store_json_open_source,
        vec![
            method("close", vec![], DataType::Void),
            method(
                "batches",
                vec![
                    Arg::new("schema", SCHEMA),
                    Arg::new("batchSize", DataType::Long),
                    Arg::new("backend", BACKEND),
                ],
                ITERATOR,
            ),
        ],
    )?;
    interface(
        "Flow\\ETL\\Adapter\\Parquet\\ParquetOpenSource",
        store_parquet_open_source,
        vec![
            method(
                "batches",
                vec![
                    Arg::new("schema", SCHEMA),
                    Arg::new("batchSize", DataType::Long),
                    Arg::new("offset", DataType::Long).allow_null(),
                    Arg::new("limit", DataType::Long).allow_null(),
                    Arg::new("backend", BACKEND),
                ],
                ITERATOR,
            ),
            method("close", vec![], DataType::Void),
        ],
    )
}
