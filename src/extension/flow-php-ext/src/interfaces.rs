//! `Flow\ETL\Column\{Column, ColumnBuilder, Backend}`: registered at MINIT with the library files' exact signatures, so
//! the extension's classes implement them and the library files (autoloaded only without flow_php) stay unused.

use std::sync::atomic::{AtomicPtr, Ordering};

use ext_php_rs::args::Arg;
use ext_php_rs::builders::{ClassBuilder, FunctionBuilder};
use ext_php_rs::error::Result;
use ext_php_rs::flags::{ClassFlags, DataType, MethodFlags};
use ext_php_rs::zend::ClassEntry;

const DEFINITION: DataType = DataType::Object(Some("Flow\\ETL\\Schema\\Definition"));
const TYPE: DataType = DataType::Object(Some("Flow\\Types\\Type"));
const COLUMN: DataType = DataType::Object(Some("Flow\\ETL\\Column\\Column"));
const COLUMN_BUILDER: DataType = DataType::Object(Some("Flow\\ETL\\Column\\ColumnBuilder"));
const SELF: DataType = DataType::Object(Some("self"));

// ClassEntry::try_find reads EG(class_table), which MINIT has not set, so each CE is kept from its registration
static COLUMN_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static COLUMN_BUILDER_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());
static BACKEND_CE: AtomicPtr<ClassEntry> = AtomicPtr::new(std::ptr::null_mut());

fn store_column(ce: &'static mut ClassEntry) {
    COLUMN_CE.store(ce, Ordering::Release);
}

fn store_column_builder(ce: &'static mut ClassEntry) {
    COLUMN_BUILDER_CE.store(ce, Ordering::Release);
}

fn store_backend(ce: &'static mut ClassEntry) {
    BACKEND_CE.store(ce, Ordering::Release);
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

fn method(name: &str, args: Vec<Arg<'static>>, returns: DataType) -> FunctionBuilder<'static> {
    args.into_iter()
        .fold(FunctionBuilder::new_abstract(name), FunctionBuilder::arg)
        .returns(returns, false, false)
}

fn interface(name: &str, store: fn(&'static mut ClassEntry), methods: Vec<FunctionBuilder<'static>>) -> Result<()> {
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
    )
}
