use ext_php_rs::exception::PhpException;
use ext_php_rs::zend::{ce, ClassEntry};

fn parquet_exception_ce() -> &'static ClassEntry {
    ClassEntry::try_find("Flow\\Arrow\\Parquet\\Exception")
        .unwrap_or_else(|| ce::exception())
}

pub fn parquet_exception(message: impl Into<String>) -> PhpException {
    PhpException::new(message.into(), 0, parquet_exception_ce())
}
