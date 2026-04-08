use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::error::Result;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::{ce, ClassEntry};

fn noop(_ce: &'static mut ClassEntry) {}

pub fn register() -> Result<()> {
    ClassBuilder::new("Flow\\Arrow\\Parquet\\Exception")
        .extends((ce::exception, "Exception"))
        .flags(ClassFlags::Final)
        .registration(noop)
        .register()?;

    Ok(())
}

fn parquet_exception_ce() -> &'static ClassEntry {
    ClassEntry::try_find("Flow\\Arrow\\Parquet\\Exception")
        .unwrap_or_else(|| ce::exception())
}

pub fn parquet_exception(message: impl Into<String>) -> PhpException {
    PhpException::new(message.into(), 0, parquet_exception_ce())
}
