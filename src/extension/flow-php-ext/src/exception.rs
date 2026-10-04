use ext_php_rs::exception::PhpException;
use ext_php_rs::zend::{ce, ClassEntry};
use flow_batch_frame::error::Part;
use flow_batch_frame::Error;

use crate::render::RUNTIME;

/// Never through `ctx::find_class()`: its not-found branch calls this function, so a missing class would recurse
/// until the stack overflows; without `flow-php/etl` the failure is a plain `\Exception`.
pub fn ext_exception(message: impl Into<String>) -> PhpException {
    PhpException::new(
        message.into(),
        0,
        ClassEntry::try_find(RUNTIME).unwrap_or_else(ce::exception),
    )
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
        Error::UnknownType { type_name } => {
            format!("flow_php cannot build a column for type \"{type_name}\"")
        }
        error => unreachable!("kind returns JSON refusals only, got {error}"),
    })
}
