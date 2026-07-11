use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::error::Result;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::{ce, ClassEntry};

const EXCEPTION_CLASS: &str = "Flow\\Floe\\Exception\\ExtensionException";

fn noop(_ce: &'static mut ClassEntry) {}

pub fn register() -> Result<()> {
    ClassBuilder::new(EXCEPTION_CLASS)
        .extends((ce::exception, "Exception"))
        .flags(ClassFlags::Final)
        .registration(noop)
        .register()?;

    Ok(())
}

fn exception_ce() -> &'static ClassEntry {
    ClassEntry::try_find(EXCEPTION_CLASS).unwrap_or_else(|| ce::exception())
}

pub fn ext_exception(message: impl Into<String>) -> PhpException {
    PhpException::new(message.into(), 0, exception_ce())
}
