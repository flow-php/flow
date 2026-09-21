//! Schema inference over CSV cells, exactly as `ColumnTypes::observe()` + `StringTypeNarrower::narrow()` +
//! `TypeWidener::widen()` fold them, for the closed set of types a CSV cell can narrow to. `json_validate()`,
//! `date_parse()` and `new DateTimeZone('+HH:MM')` are called back into PHP, each behind a Rust pre-filter that is a
//! necessary condition of the PHP predicate, so parity on those rungs holds by construction.

use std::collections::HashMap;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::ctx::{call_handle, zval_str, Ctx};
use crate::csv::php_trim;
use crate::csv::tokenizer::is_space;
use crate::exception::ext_exception;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Leaf {
    Null,
    String,
    Json,
    Uuid,
    Float,
    Integer,
    DateTime,
    Date,
    Boolean,
    TimeZone,
}

impl Leaf {
    pub fn code(self) -> &'static str {
        match self {
            Leaf::Null => "null",
            Leaf::String => "string",
            Leaf::Json => "json",
            Leaf::Uuid => "uuid",
            Leaf::Float => "float",
            Leaf::Integer => "integer",
            Leaf::DateTime => "datetime",
            Leaf::Date => "date",
            Leaf::Boolean => "boolean",
            Leaf::TimeZone => "timezone",
        }
    }
}

/// A leaf, optionally nullable; `Null` itself is never optional.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Kind {
    leaf: Leaf,
    optional: bool,
}

impl Kind {
    fn of(leaf: Leaf) -> Self {
        Self { leaf, optional: false }
    }

    pub fn code(self) -> String {
        if self.optional {
            format!("?{}", self.leaf.code())
        } else {
            self.leaf.code().to_string()
        }
    }

    fn is_string(self) -> bool {
        self.leaf == Leaf::String
    }
}

/// `TypeWidener::widen()` over the CSV leaves; pinned against it pair by pair (`048_csv_widen_parity.phpt`).
fn widen(left: Kind, right: Kind) -> Kind {
    if left == right {
        return left;
    }

    let optional = left.optional || right.optional || left.leaf == Leaf::Null || right.leaf == Leaf::Null;

    let leaf = match (left.leaf, right.leaf) {
        (Leaf::Null, leaf) | (leaf, Leaf::Null) => leaf,
        (a, b) if a == b => a,
        (Leaf::Integer | Leaf::Float, Leaf::Integer | Leaf::Float) => Leaf::Float,
        (Leaf::Date | Leaf::DateTime, Leaf::Date | Leaf::DateTime) => Leaf::DateTime,
        _ => Leaf::String,
    };

    Kind { leaf, optional }
}

/// The rungs `StringTypeNarrower::emitsClass()` lets run, by the candidate types' `toString()`.
struct Candidates {
    json: bool,
    uuid: bool,
    float: bool,
    integer: bool,
    datetime: bool,
    date: bool,
    boolean: bool,
    timezone: bool,
}

impl Candidates {
    /// The fold has no HTML or XML rung, so a candidate set that allows either cannot be folded natively.
    fn from_codes(codes: &[Vec<u8>]) -> Result<Self, PhpException> {
        let has = |code: &[u8]| codes.iter().any(|candidate| candidate == code);

        if has(b"html") || has(b"xml") {
            return Err(ext_exception("flow_php cannot fold html or xml candidates natively"));
        }

        Ok(Self {
            json: has(b"json"),
            uuid: has(b"uuid"),
            float: has(b"float"),
            integer: has(b"integer"),
            datetime: has(b"datetime"),
            date: has(b"date"),
            boolean: has(b"boolean"),
            timezone: has(b"timezone"),
        })
    }
}

/// `StringTypeNarrower::narrow()` for a string cell, without the HTML and XML rungs.
pub struct Narrower {
    candidates: Candidates,
    ctx: Ctx,
}

impl Narrower {
    fn new(candidates: Candidates) -> Result<Self, PhpException> {
        Ok(Self {
            candidates,
            ctx: Ctx::new()?,
        })
    }

    pub fn narrow(&mut self, value: &[u8]) -> Result<Leaf, PhpException> {
        let value = php_trim(value);

        if value.is_empty() {
            return Ok(Leaf::String);
        }

        if is_null(value) {
            return Ok(Leaf::Null);
        }

        if self.candidates.json && is_json_shaped(value) && self.is_json(value)? {
            return Ok(Leaf::Json);
        }

        if self.candidates.uuid && is_uuid(value) {
            return Ok(Leaf::Uuid);
        }

        if self.candidates.float && is_float(value) {
            return Ok(Leaf::Float);
        }

        if self.candidates.integer && is_integer(value) {
            return Ok(Leaf::Integer);
        }

        if (self.candidates.datetime || self.candidates.date) && has_explicit_day(value) {
            match self.temporal(value)? {
                Some(Leaf::DateTime) if self.candidates.datetime => return Ok(Leaf::DateTime),
                Some(Leaf::Date) if self.candidates.date => return Ok(Leaf::Date),
                _ => {}
            }
        }

        if self.candidates.boolean && (value.eq_ignore_ascii_case(b"true") || value.eq_ignore_ascii_case(b"false")) {
            return Ok(Leaf::Boolean);
        }

        if self.candidates.timezone && self.is_timezone(value)? {
            return Ok(Leaf::TimeZone);
        }

        Ok(Leaf::String)
    }

    fn is_json(&mut self, value: &[u8]) -> Result<bool, PhpException> {
        let valid = call_handle(self.ctx.json_validate()?, None, &mut [zval_str(value)], "validate a JSON cell")?;

        Ok(valid.bool().unwrap_or(false))
    }

    /// `StringTemporalParts::from()` past its `hasExplicitDay()` gate: `Date`, `DateTime`, or `None` when the value
    /// is not a calendar date.
    fn temporal(&mut self, value: &[u8]) -> Result<Option<Leaf>, PhpException> {
        let parts_zv = call_handle(self.ctx.date_parse()?, None, &mut [zval_str(value)], "parse a temporal cell")?;
        let parts = parts_zv
            .array()
            .ok_or_else(|| ext_exception("flow_php expected date_parse() to return an array"))?;

        let long = |key: &str| parts.get(key).and_then(Zval::long);
        let present = |key: &str| parts.get(key).is_some_and(|zv| !zv.is_false());

        if long("error_count").unwrap_or(0) > 0 {
            return Ok(None);
        }

        let (Some(year), Some(month), Some(day)) = (long("year"), long("month"), long("day")) else {
            return Ok(None);
        };

        if !checkdate(month, day, year) {
            return Ok(None);
        }

        let mut time = present("hour") || present("minute") || present("second") || present("fraction");

        if !time {
            if let Some(relative) = parts.get("relative").and_then(Zval::array) {
                time = ["hour", "minute", "second"]
                    .iter()
                    .any(|key| relative.get(*key).is_some_and(|zv| !is_zero(zv)));
            }
        }

        Ok(Some(if time { Leaf::DateTime } else { Leaf::Date }))
    }

    fn is_timezone(&mut self, value: &[u8]) -> Result<bool, PhpException> {
        if self.ctx.timezone_identifiers()?.contains(value) {
            return Ok(true);
        }

        Ok(is_offset(value) && self.ctx.timezone_accepts(value)?)
    }
}

fn is_zero(zv: &Zval) -> bool {
    zv.long() == Some(0)
}

/// `in_array(mb_strtolower($value), ['null', 'nil'], true)`: no non-ASCII codepoint lowercases into these letters.
fn is_null(value: &[u8]) -> bool {
    value.is_ascii() && (value.eq_ignore_ascii_case(b"null") || value.eq_ignore_ascii_case(b"nil"))
}

/// `Json::isValid()`'s shape gate before `json_validate()`.
fn is_json_shaped(value: &[u8]) -> bool {
    matches!((value.first(), value.last()), (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']')))
}

/// `Uuid::isValid()`: 36 bytes, `/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/`.
fn is_uuid(value: &[u8]) -> bool {
    value.len() == 36
        && value.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(byte),
        })
}

/// PHP 8 `is_numeric()` on a string: optional leading and trailing whitespace, optional sign,
/// `digits[.digits] | .digits | digits.`, optional exponent. No hex, no `INF`/`NAN`.
fn is_numeric(value: &[u8]) -> bool {
    let mut position = value.iter().take_while(|byte| is_space(**byte)).count();
    let end = value.len() - value[position..].iter().rev().take_while(|byte| is_space(**byte)).count();

    if matches!(value.get(position), Some(b'+' | b'-')) {
        position += 1;
    }

    let digits = |from: usize| value[from..end].iter().take_while(|byte| byte.is_ascii_digit()).count();

    let integral = digits(position);
    position += integral;

    let mut fractional = 0;

    if value.get(position) == Some(&b'.') && position < end {
        fractional = digits(position + 1);
        position += 1 + fractional;
    }

    if integral == 0 && fractional == 0 {
        return false;
    }

    if position < end && matches!(value[position], b'e' | b'E') {
        let mut exponent = position + 1;

        if exponent < end && matches!(value[exponent], b'+' | b'-') {
            exponent += 1;
        }

        let exponent_digits = digits(exponent);

        if exponent_digits == 0 {
            return false;
        }

        position = exponent + exponent_digits;
    }

    position == end
}

/// `StringTypeNarrower::isFloat()`.
fn is_float(value: &[u8]) -> bool {
    is_numeric(value) && value.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

/// `is_numeric($value) && (string) (int) $value === $value`: the canonical decimal form of an int64.
fn is_integer(value: &[u8]) -> bool {
    std::str::from_utf8(value)
        .ok()
        .and_then(|string| string.parse::<i64>().ok())
        .is_some_and(|parsed| parsed.to_string().as_bytes() == value)
}

/// `StringTemporalParts::hasExplicitDay()`.
fn has_explicit_day(value: &[u8]) -> bool {
    let groups = value
        .iter()
        .enumerate()
        .filter(|(index, byte)| byte.is_ascii_digit() && (*index == 0 || !value[index - 1].is_ascii_digit()))
        .count();

    if groups >= 3 {
        return true;
    }

    if groups == 1 && value.len() == 8 && value.iter().all(u8::is_ascii_digit) {
        return true;
    }

    groups >= 2 && value.windows(3).any(|window| window.iter().all(u8::is_ascii_alphabetic))
}

/// `StringTypeNarrower::isTimeZone()`'s `/^[+-]\d{2}:\d{2}$/` on a trimmed value.
fn is_offset(value: &[u8]) -> bool {
    matches!(value, [b'+' | b'-', h1, h2, b':', m1, m2] if [h1, h2, m1, m2].iter().all(|byte| byte.is_ascii_digit()))
}

/// `checkdate()`.
fn checkdate(month: i64, day: i64, year: i64) -> bool {
    if !(1..=32767).contains(&year) || !(1..=12).contains(&month) || day < 1 {
        return false;
    }

    let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    let days = match month {
        2 if leap => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };

    day <= days
}

struct Column {
    name: Vec<u8>,
    kind: Kind,
}

/// `ColumnTypes::observe()` over whole rows: names seeded `null` (then widened), names met only in rows start at
/// their first observed type; a `string` / `?string` column skips non-null cells.
pub struct Fold {
    pub narrower: Narrower,
    columns: Vec<Column>,
    index: HashMap<Vec<u8>, usize>,
    rows: u64,
}

impl Fold {
    pub fn new(names: Vec<Vec<u8>>, candidates: &[Vec<u8>]) -> Result<Self, PhpException> {
        let mut fold = Self {
            narrower: Narrower::new(Candidates::from_codes(candidates)?)?,
            columns: Vec::new(),
            index: HashMap::new(),
            rows: 0,
        };

        for name in names {
            if !fold.index.contains_key(&name) {
                fold.index.insert(name.clone(), fold.columns.len());
                fold.columns.push(Column {
                    name,
                    kind: Kind::of(Leaf::Null),
                });
            }
        }

        Ok(fold)
    }

    /// One cell of the current row; `None` is a null cell. `position` caches the cell's column across rows.
    pub fn observe(&mut self, name: &[u8], position: &mut Option<usize>, value: Option<&[u8]>) -> Result<(), PhpException> {
        if position.is_none() {
            *position = self.index.get(name).copied();
        }

        if value.is_some() && position.is_some_and(|position| self.columns[position].kind.is_string()) {
            return Ok(());
        }

        let observed = Kind::of(match value {
            None => Leaf::Null,
            Some(value) if php_trim(value).len() != value.len() => Leaf::String,
            Some(value) => match self.narrower.narrow(value)? {
                Leaf::Null => Leaf::String,
                leaf => leaf,
            },
        });

        match *position {
            Some(position) => self.columns[position].kind = widen(self.columns[position].kind, observed),
            None => {
                self.index.insert(name.to_vec(), self.columns.len());
                *position = Some(self.columns.len());
                self.columns.push(Column {
                    name: name.to_vec(),
                    kind: observed,
                });
            }
        }

        Ok(())
    }

    pub fn end_row(&mut self) {
        self.rows += 1;
    }

    pub fn rows(&self) -> u64 {
        self.rows
    }

    /// Column name => type code, first-seen order.
    pub fn types(&self) -> impl Iterator<Item = (&[u8], String)> {
        self.columns.iter().map(|column| (column.name.as_slice(), column.kind.code()))
    }
}

pub fn string_values(list: &ZendHashTable) -> Vec<Vec<u8>> {
    list.values()
        .filter_map(|zv| zv.zend_str().map(|value| value.as_bytes().to_vec()))
        .collect()
}
