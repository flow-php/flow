use ext_php_rs::exception::PhpException;
use ext_php_rs::zend::{ce, ClassEntry};

/// What arrow throws for its own failures: the lib's runtime exception, plain `Exception` when the lib is not
/// autoloadable.
const EXCEPTION_CLASS: &str = "Flow\\Parquet\\Exception\\RuntimeException";

fn exception_ce() -> &'static ClassEntry {
    ClassEntry::try_find(EXCEPTION_CLASS).unwrap_or_else(|| ce::exception())
}

pub fn ext_exception(message: impl Into<String>) -> PhpException {
    PhpException::new(message.into(), 0, exception_ce())
}
