use ext_php_rs::builders::ClassBuilder;
use ext_php_rs::error::Result;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::zend::{ce, ClassEntry};
use flow_batch_frame::error::Part;
use flow_batch_frame::Error;

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

/// A flow-batch-frame refusal of schema or type JSON, which only `kind` returns.
pub fn json_exception(error: Error) -> PhpException {
    ext_exception(match error {
        Error::SchemaJsonUtf8 => "flow_php failed to decode schema JSON: invalid UTF-8".to_string(),
        Error::SchemaJson(e) => format!("flow_php failed to decode schema JSON: {e}"),
        Error::TypeJsonUtf8 => "flow_php failed to decode type JSON: invalid UTF-8".to_string(),
        Error::TypeJson(e) => format!("flow_php failed to decode type JSON: {e}"),
        Error::MissingPart { type_name, part } => format!(
            "flow_php schema JSON for type \"{type_name}\" is missing its {}",
            match part {
                Part::Element => "element",
                Part::Key => "key",
                Part::Value => "value",
                Part::Base => "base",
            }
        ),
        Error::UnknownType { type_name } => format!("flow_php cannot build a column for type \"{type_name}\""),
        error => unreachable!("kind returns JSON refusals only, got {error}"),
    })
}
