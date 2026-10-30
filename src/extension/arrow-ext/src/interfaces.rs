//! `Flow\Parquet\{ParquetEngine, ParquetFileReader, ParquetFileWriter}` and `Flow\Parquet\ParquetFile\Footer`: registered at MINIT with the package files'
//! exact signatures, so the extension's classes implement them and the package files (autoloaded only without arrow)
//! stay unused.

use std::sync::atomic::{AtomicPtr, Ordering};

use ext_php_rs::args::{Arg, ArgInfoTables};
use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::error::Result;
use ext_php_rs::flags::{ClassFlags, DataType, MethodFlags};
use ext_php_rs::zend::ClassEntry;

const SOURCE_STREAM: DataType = DataType::object("Flow\\Filesystem\\SourceStream");
const DESTINATION_STREAM: DataType = DataType::object("Flow\\Filesystem\\DestinationStream");
const SCHEMA: DataType = DataType::object("Flow\\Parquet\\ParquetFile\\Schema");
const METADATA: DataType = DataType::object("Flow\\Parquet\\ParquetFile\\Metadata");
const READ_PLAN: DataType = DataType::object("Flow\\Parquet\\Reader\\ReadPlan");
const ROW_GROUP_CHUNKS: DataType = DataType::object("Flow\\Parquet\\ParquetFile\\RowGroupChunks");
const COMPRESSIONS: DataType = DataType::object("Flow\\Parquet\\ParquetFile\\Compressions");
const OPTIONS: DataType = DataType::object("Flow\\Parquet\\Options");
const FOOTER: DataType = DataType::object("Flow\\Parquet\\ParquetFile\\Footer");
const PARQUET_FILE_READER: DataType = DataType::object("Flow\\Parquet\\ParquetFileReader");
const PARQUET_FILE_WRITER: DataType = DataType::object("Flow\\Parquet\\ParquetFileWriter");
const PARQUET_ENGINE: DataType = DataType::object("Flow\\Parquet\\ParquetEngine");
const ITERATOR: DataType = DataType::object("Iterator");

// ClassEntry::try_find reads EG(class_table), which MINIT has not set, so each CE is kept from its registration
static PARQUET_ENGINE_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static FOOTER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static PARQUET_FILE_READER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static PARQUET_FILE_WRITER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());

fn store_parquet_engine(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    PARQUET_ENGINE_CE.store(ce, Ordering::Release);
}

fn store_footer(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    FOOTER_CE.store(ce, Ordering::Release);
}

fn store_parquet_file_reader(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    PARQUET_FILE_READER_CE.store(ce, Ordering::Release);
}

fn store_parquet_file_writer(ce: &'static mut ClassEntry, arg_info: ArgInfoTables) {
    keep(arg_info);
    PARQUET_FILE_WRITER_CE.store(ce, Ordering::Release);
}

/// The engine borrows an interface's argument info tables for the life of the process.
pub fn keep(arg_info: ArgInfoTables) {
    std::mem::forget(arg_info);
}

fn load(ce: &AtomicPtr<ClassEntry>, name: &str) -> &'static ClassEntry {
    unsafe { ce.load(Ordering::Acquire).as_ref() }.unwrap_or_else(|| panic!("{name} not registered"))
}

pub fn parquet_engine_ce() -> &'static ClassEntry {
    load(&PARQUET_ENGINE_CE, "Flow\\Parquet\\ParquetEngine")
}

pub fn footer_ce() -> &'static ClassEntry {
    load(&FOOTER_CE, "Flow\\Parquet\\ParquetFile\\Footer")
}

pub fn parquet_file_reader_ce() -> &'static ClassEntry {
    load(&PARQUET_FILE_READER_CE, "Flow\\Parquet\\ParquetFileReader")
}

pub fn parquet_file_writer_ce() -> &'static ClassEntry {
    load(&PARQUET_FILE_WRITER_CE, "Flow\\Parquet\\ParquetFileWriter")
}

fn method(name: &str, args: Vec<Arg<'static>>, returns: DataType) -> FunctionBuilder<'static> {
    args.into_iter()
        .fold(FunctionBuilder::new_abstract(name), FunctionBuilder::arg)
        .returns(returns, false, false)
}

fn nullable_method(name: &str, args: Vec<Arg<'static>>, returns: DataType) -> FunctionBuilder<'static> {
    method(name, args, returns).returns(returns, false, true)
}

/// `$static_methods` are registered `public static abstract` first, the rest `public abstract` after them - the
/// package files declare them in that order, and 051 reflects both sides.
fn interface(
    name: &str,
    store: fn(&'static mut ClassEntry, ArgInfoTables),
    methods: Vec<FunctionBuilder<'static>>,
    static_methods: Vec<FunctionBuilder<'static>>,
) -> Result<()> {
    let class = static_methods.into_iter().fold(
        ClassBuilder::new(name).flags(ClassFlags::Interface).registration(store),
        |class, method| {
            class.method(
                method,
                MethodFlags::Public | MethodFlags::Abstract | MethodFlags::Static,
            )
        },
    );

    methods
        .into_iter()
        .fold(class, |class, method| {
            class.method(method, MethodFlags::Public | MethodFlags::Abstract)
        })
        .register()
}

pub fn register() -> Result<()> {
    interface(
        "Flow\\Parquet\\ParquetFile\\Footer",
        store_footer,
        vec![
            method("metadata", vec![], METADATA),
            method(
                "rowGroupChunks",
                vec![Arg::new("columns", DataType::Array)],
                ROW_GROUP_CHUNKS,
            ),
            method("rowsNumber", vec![], DataType::Long),
            method("schema", vec![], SCHEMA),
            method("totalByteSize", vec![], DataType::Long),
        ],
        vec![],
    )?;
    interface(
        "Flow\\Parquet\\ParquetFileReader",
        store_parquet_file_reader,
        vec![
            method("close", vec![], DataType::Void),
            method("footer", vec![], FOOTER),
            method(
                "readColumns",
                vec![
                    Arg::new("columns", DataType::Array),
                    Arg::new("batchSize", DataType::Long),
                    Arg::new("plan", READ_PLAN),
                ],
                ITERATOR,
            ),
        ],
        vec![],
    )?;
    interface(
        "Flow\\Parquet\\ParquetFileWriter",
        store_parquet_file_writer,
        vec![
            method("close", vec![], DataType::Void),
            method("writeBatch", vec![Arg::new("rows", DataType::Iterable)], DataType::Void),
            method(
                "writeColumns",
                vec![Arg::new("columns", DataType::Array)],
                DataType::Void,
            ),
            method("writeRow", vec![Arg::new("row", DataType::Array)], DataType::Void),
        ],
        vec![],
    )?;
    interface(
        "Flow\\Parquet\\ParquetEngine",
        store_parquet_engine,
        vec![
            nullable_method("normalize", vec![], DataType::Array),
            method(
                "openForRead",
                vec![Arg::new("stream", SOURCE_STREAM)],
                PARQUET_FILE_READER,
            ),
            method(
                "openForWrite",
                vec![
                    Arg::new("stream", DESTINATION_STREAM),
                    Arg::new("schema", SCHEMA),
                    Arg::new("compression", COMPRESSIONS),
                    Arg::new("options", OPTIONS),
                ],
                PARQUET_FILE_WRITER,
            ),
            method(
                "writeRows",
                vec![
                    Arg::new("stream", DESTINATION_STREAM),
                    Arg::new("schema", SCHEMA),
                    Arg::new("compression", COMPRESSIONS),
                    Arg::new("options", OPTIONS),
                    Arg::new("rows", DataType::Iterable),
                ],
                DataType::Void,
            ),
        ],
        vec![method(
            "fromArray",
            vec![Arg::new("configuration", DataType::Array)],
            PARQUET_ENGINE,
        )],
    )
}
