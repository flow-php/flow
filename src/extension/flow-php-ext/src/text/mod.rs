//! The PHP side of the native text writers: a `Rows` batch as `flow_column_text` renders it (`batch`), and the time
//! zone transitions a named zone reads from `DateTimeZone` (`PhpTransitions`).

pub mod batch;

use ext_php_rs::exception::PhpException;
use flow_column_text::date::{Transition, Transitions};

use crate::ctx::{self, call_method, ht_get, zval_long};
use crate::exception::ext_exception;

/// `DateTimeZone::getTransitions($min, $max + 1)`; system tzdata builds drop a transition exactly at the range end.
pub struct PhpTransitions;

impl Transitions for PhpTransitions {
    type Error = PhpException;

    fn of(&self, zone: &[u8], min_second: i64, max_second: i64) -> Result<Vec<Transition>, PhpException> {
        let listed = call_method(
            &ctx::timezone(zone)?,
            "getTransitions",
            &mut [zval_long(min_second), zval_long(max_second.saturating_add(1))],
        )?;
        let listed = listed
            .array()
            .ok_or_else(|| ext_exception("flow_php expected DateTimeZone::getTransitions() to return an array"))?;
        let mut transitions = Vec::with_capacity(listed.len());

        for transition in listed.values() {
            let field = |name: &[u8]| {
                transition
                    .array()
                    .and_then(|transition| ht_get(transition, name))
                    .ok_or_else(|| ext_exception("flow_php expected a time zone transition of ts, offset and abbr"))
            };

            transitions.push(Transition {
                from: field(b"ts")?.long().unwrap_or(min_second),
                offset: field(b"offset")?.long().unwrap_or(0) as i32,
                abbreviation: field(b"abbr")?
                    .zend_str()
                    .map(|abbr| abbr.as_bytes().to_vec())
                    .unwrap_or_default(),
            });
        }

        if transitions.is_empty() {
            return Err(ext_exception(
                "flow_php expected DateTimeZone::getTransitions() to list the offset in force",
            ));
        }

        Ok(transitions)
    }
}
