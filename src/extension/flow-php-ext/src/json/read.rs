//! The reader behind `RustJsonOpenSource`: JSON lines or a top-level JSON array, fed in chunks, read into native
//! columns by a schema with `RowsBuilder::appendRows()`'s results and refusals. This file frames records and maps their
//! members to columns; serde_json parses and validates.

use std::borrow::Cow;
use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::fmt;

use ext_php_rs::exception::PhpException;
use ext_php_rs::prelude::*;
use ext_php_rs::types::Zval;
use flow_batch_frame::kind::{Field, Kind};
use serde::de::{self, DeserializeSeed, Deserializer as _, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::Deserialize;
use serde_json::de::{SliceRead, StrRead};
use serde_json::value::RawValue;
use serde_json::Deserializer;

use crate::batch_columns::{native_text, BatchColumns, HeldBatch, Key};
use crate::builder::{append_native, append_one, native_leaf, Append};
use crate::cast::{CastKind, MapKeyKind};
use crate::ctx::{self, array_key_index, null_zval, zval_str};
use crate::exception::ext_exception;
use crate::kind_builder::KindBuilder;
use crate::physical::overflow;
use crate::render::runtime;

/// JSON's insignificant whitespace.
const WHITESPACE: &[u8] = b" \t\n\r";
/// `JsonFileReader::C_ISSPACE`: a line of only these bytes is no record.
const BLANK: &[u8] = b" \t\n\r\x0B\x0C";
const BOM: &[u8] = b"\xEF\xBB\xBF";
/// `json_decode()`'s default depth: a record nested this deep is refused, one level less is read.
const DEPTH: usize = 512;

fn is_whitespace(byte: &u8) -> bool {
    WHITESPACE.contains(byte)
}

/// Why a record cannot be read; `ordinal` is its 1-based line or 0-based element.
#[derive(Debug, PartialEq)]
pub struct Malformed {
    pub ordinal: usize,
    pub message: String,
}

/// One framed record: `records[start..end]`.
pub struct Record {
    pub start: usize,
    pub end: usize,
    pub ordinal: usize,
}

/// The document scanner: where the element being framed stands.
#[derive(Default)]
struct Scan {
    depth: usize,
    in_string: bool,
    escape: bool,
}

struct TooDeep;

impl Scan {
    /// From `bytes[from..]` on: `Some(i)` is the depth-0 `,` or `]` at `i`, `None` means `bytes` ends inside the value.
    fn run(&mut self, bytes: &[u8], mut i: usize) -> Result<Option<usize>, TooDeep> {
        while i < bytes.len() {
            if self.in_string {
                if self.escape {
                    self.escape = false;
                    i += 1;

                    continue;
                }

                let Some(found) = memchr::memchr2(b'"', b'\\', &bytes[i..]) else {
                    return Ok(None);
                };

                i += found;

                if bytes[i] == b'\\' {
                    self.escape = true;
                } else {
                    self.in_string = false;
                }

                i += 1;

                continue;
            }

            match bytes[i] {
                b'"' => self.in_string = true,
                b'{' | b'[' => {
                    self.depth += 1;

                    if self.depth >= DEPTH {
                        return Err(TooDeep);
                    }
                }
                b'}' | b']' if self.depth > 0 => self.depth -= 1,
                b']' | b',' if self.depth == 0 => return Ok(Some(i)),
                _ => {}
            }

            i += 1;
        }

        Ok(None)
    }
}

const TOO_DEEP: &str = "Maximum stack depth exceeded";
const LONE_SURROGATE: &str = "Single unpaired UTF-16 surrogate in unicode escape";

/// `{}` or `[]`: a record with no fields, skipped as `JsonFileReader::sample()` skips it.
fn empty_record(bytes: &[u8]) -> bool {
    let start = bytes
        .iter()
        .position(|byte| !is_whitespace(byte))
        .unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !is_whitespace(byte))
        .map_or(start, |end| end + 1);
    let trimmed = &bytes[start..end];

    matches!(
        (trimmed.first(), trimmed.last()),
        (Some(b'{'), Some(b'}')) | (Some(b'['), Some(b']'))
    ) && trimmed.len() >= 2
        && trimmed[1..trimmed.len() - 1].iter().all(is_whitespace)
}

enum Document {
    Start,
    Elements,
    Closed,
}

/// Splits fed bytes into records without parsing values: lines on `\n`, a document's elements on its depth-0 `,` and
/// `]`. Holds the unframed input only.
pub struct Framing {
    lines: bool,
    input: Vec<u8>,
    /// The first byte of `input` not yet framed.
    start: usize,
    /// How far past `start` the current record's end was searched.
    scanned: usize,
    scan: Scan,
    document: Document,
    bom: bool,
    /// The next line (1-based) or element (0-based).
    ordinal: usize,
    finished: bool,
}

impl Framing {
    pub fn new(lines: bool) -> Self {
        Self {
            lines,
            input: Vec::new(),
            start: 0,
            scanned: 0,
            scan: Scan::default(),
            document: Document::Start,
            bom: true,
            ordinal: usize::from(lines),
            finished: false,
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        if self.start > 0 && self.start * 2 >= self.input.len() {
            self.input.drain(..self.start);
            self.start = 0;
        }

        self.input.extend_from_slice(chunk);
    }

    pub fn finish(&mut self) {
        self.finished = true;
    }

    /// Copies the next record into `records`; `false` when no complete record is buffered.
    pub fn next(&mut self, records: &mut Vec<u8>, bounds: &mut Vec<Record>) -> Result<bool, Malformed> {
        if self.bom {
            let pending = &self.input[self.start..];

            if pending.len() < BOM.len() && BOM.starts_with(pending) && !self.finished {
                return Ok(false);
            }

            if pending.starts_with(BOM) {
                self.start += BOM.len();
            }

            self.bom = false;
        }

        loop {
            let framed = if self.lines { self.line() } else { self.element()? };

            let Some((start, end, ordinal)) = framed else {
                return Ok(false);
            };

            let bytes = &self.input[start..end];

            if empty_record(bytes) {
                continue;
            }

            if lone_surrogate(bytes) {
                return Err(Malformed {
                    ordinal,
                    message: LONE_SURROGATE.to_string(),
                });
            }

            bounds.push(Record {
                start: records.len(),
                end: records.len() + bytes.len(),
                ordinal,
            });
            records.extend_from_slice(bytes);

            return Ok(true);
        }
    }

    fn line(&mut self) -> Option<(usize, usize, usize)> {
        loop {
            let pending = &self.input[self.start..];
            let (end, next) = match memchr::memchr(b'\n', &pending[self.scanned..]) {
                Some(found) => (self.scanned + found, self.scanned + found + 1),
                None if self.finished && !pending.is_empty() => (pending.len(), pending.len()),
                None => {
                    self.scanned = pending.len();

                    return None;
                }
            };
            let line = (self.start, self.start + end, self.ordinal);

            self.ordinal += 1;
            self.start += next;
            self.scanned = 0;

            if !self.input[line.0..line.1].iter().all(|byte| BLANK.contains(byte)) {
                return Some(line);
            }
        }
    }

    fn element(&mut self) -> Result<Option<(usize, usize, usize)>, Malformed> {
        loop {
            match self.document {
                Document::Start => {
                    let pending = &self.input[self.start..];
                    let Some(first) = pending.iter().position(|byte| !is_whitespace(byte)) else {
                        self.start = self.input.len();

                        return match self.finished {
                            true => Err(self.malformed("EOF while parsing a value".to_string())),
                            false => Ok(None),
                        };
                    };

                    if pending[first] != b'[' {
                        return Err(self.malformed("expected `[`".to_string()));
                    }

                    self.start += first + 1;
                    self.document = Document::Elements;
                }
                Document::Elements => {
                    let pending = &self.input[self.start..];
                    let found = self
                        .scan
                        .run(pending, self.scanned)
                        .map_err(|TooDeep| self.malformed(TOO_DEEP.to_string()))?;

                    let Some(found) = found else {
                        self.scanned = pending.len();

                        if !self.finished {
                            return Ok(None);
                        }

                        return Err(self.malformed(
                            Deserializer::from_slice(pending)
                                .deserialize_ignored_any(IgnoredAny)
                                .err()
                                .map_or_else(|| "EOF while parsing a list".to_string(), |e| e.to_string()),
                        ));
                    };

                    let element = (self.start, self.start + found, self.ordinal);
                    let closing = pending[found] == b']';

                    self.start += found + 1;
                    self.scanned = 0;
                    self.scan = Scan::default();

                    if closing {
                        self.document = Document::Closed;

                        if self.ordinal == 0 && self.input[element.0..element.1].iter().all(is_whitespace) {
                            continue;
                        }
                    }

                    self.ordinal += 1;

                    return Ok(Some(element));
                }
                Document::Closed => {
                    let pending = &self.input[self.start..];
                    let trailing = pending.iter().any(|byte| !is_whitespace(byte));

                    self.start = self.input.len();

                    if trailing {
                        return Err(self.malformed("trailing characters".to_string()));
                    }

                    return Ok(None);
                }
            }
        }
    }

    fn malformed(&self, message: String) -> Malformed {
        Malformed {
            ordinal: self.ordinal,
            message,
        }
    }
}

/// The code unit of the `\uXXXX` escape at `bytes[at]`.
fn code_unit(bytes: &[u8], at: usize) -> Option<u16> {
    let hex = bytes.get(at + 2..at + 6).filter(|_| bytes.get(at + 1) == Some(&b'u'))?;

    u16::from_str_radix(std::str::from_utf8(hex).ok()?, 16).ok()
}

/// Whether a `\u` escape of `bytes` is a UTF-16 surrogate without its pair, which `json_decode()` refuses and serde
/// checks only when it unescapes a string. A backslash outside a string is serde's syntax error, so every one is walked.
fn lone_surrogate(bytes: &[u8]) -> bool {
    let mut i = 0;
    let mut low_expected_at = None;

    while let Some(found) = memchr::memchr(b'\\', &bytes[i..]) {
        let at = i + found;

        match (low_expected_at.take(), code_unit(bytes, at)) {
            (Some(expected), Some(0xDC00..=0xDFFF)) if expected == at => {}
            (Some(_), _) | (None, Some(0xDC00..=0xDFFF)) => return true,
            (None, Some(0xD800..=0xDBFF)) => low_expected_at = Some(at + 6),
            (None, _) => {}
        }

        i = (at + if bytes.get(at + 1) == Some(&b'u') { 6 } else { 2 }).min(bytes.len());
    }

    low_expected_at.is_some()
}

/// A line's nesting checked as the document scanner checks an element's.
fn line_too_deep(line: &[u8]) -> bool {
    memchr::memchr2_iter(b'[', b'{', line).count() >= DEPTH && Scan::default().run(line, 0).is_err()
}

/// A JSON number as `json_decode()` reads it: integer-shaped text within the i64 range is an int, any other a float.
#[derive(Debug, PartialEq)]
pub enum Number {
    Long(i64),
    Double(f64),
}

pub fn number(text: &str) -> Number {
    if !text.bytes().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
        if let Ok(long) = text.parse::<i64>() {
            return Number::Long(long);
        }
    }

    Number::Double(text.parse::<f64>().unwrap_or(f64::NAN))
}

fn number_zval(text: &str) -> Zval {
    let mut zval = Zval::new();

    match number(text) {
        Number::Long(long) => zval.set_long(long),
        Number::Double(double) => zval.set_double(double),
    }

    zval
}

fn bool_zval(value: bool) -> Zval {
    let mut zval = Zval::new();
    zval.set_bool(value);

    zval
}

/// `get_debug_type()` of the scalar `json_decode()` reads from `text`.
fn scalar_type(text: &str) -> &'static str {
    match text.as_bytes()[0] {
        b'"' => "string",
        b't' | b'f' => "bool",
        b'n' => "null",
        _ => match number(text) {
            Number::Long(_) => "int",
            Number::Double(_) => "float",
        },
    }
}

#[derive(Debug, PartialEq)]
pub enum RecordError {
    Malformed(String),
    /// The record is a scalar of this `get_debug_type()`.
    Scalar(&'static str),
}

fn deserializer(text: &[u8]) -> Deserializer<SliceRead<'_>> {
    let mut deserializer = Deserializer::from_slice(text);
    deserializer.disable_recursion_limit();

    deserializer
}

fn str_deserializer(text: &str) -> Deserializer<StrRead<'_>> {
    let mut deserializer = Deserializer::from_str(text);
    deserializer.disable_recursion_limit();

    deserializer
}

/// A JSON string, borrowed when it has no escape.
struct Text;

impl<'de> Visitor<'de> for Text {
    type Value = Cow<'de, str>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON string")
    }

    fn visit_borrowed_str<E: de::Error>(self, value: &'de str) -> Result<Self::Value, E> {
        Ok(Cow::Borrowed(value))
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<Self::Value, E> {
        Ok(Cow::Owned(value.to_owned()))
    }
}

impl<'de> DeserializeSeed<'de> for Text {
    type Value = Cow<'de, str>;

    fn deserialize<D: de::Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(self)
    }
}

/// The column each member feeds, resolved once per schema: an object's members by name, an array record's elements
/// by position (a definition keyed by that int).
#[derive(Default)]
pub struct MemberColumns {
    names: HashMap<Vec<u8>, usize>,
    positions: Vec<Option<usize>>,
}

impl MemberColumns {
    pub fn of<'a>(keys: impl Iterator<Item = &'a Key>) -> Self {
        let mut columns = Self::default();

        for (column, key) in keys.enumerate() {
            if let Key::Index(index) = key {
                if let Ok(position) = usize::try_from(*index) {
                    columns
                        .positions
                        .resize(columns.positions.len().max(position + 1), None);
                    columns.positions[position] = Some(column);
                }
            }

            columns.names.insert(key.name(), column);
        }

        columns
    }
}

/// A record's members, each stored as the span of its value in `slots[column]`.
struct Members<'a> {
    columns: &'a MemberColumns,
    slots: &'a mut [Option<(usize, usize)>],
    base: usize,
}

impl Members<'_> {
    fn store(&mut self, column: Option<usize>, value: &RawValue) {
        if let Some(column) = column {
            let start = value.get().as_ptr() as usize - self.base;

            self.slots[column] = Some((start, start + value.get().len()));
        }
    }
}

impl<'de> Visitor<'de> for Members<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object or array")
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        while let Some(name) = map.next_key_seed(Text)? {
            let value: &'de RawValue = map.next_value()?;

            self.store(self.columns.names.get(name.as_bytes()).copied(), value);
        }

        Ok(())
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        let mut position = 0;

        while let Some(value) = seq.next_element::<&'de RawValue>()? {
            self.store(self.columns.positions.get(position).copied().flatten(), value);
            position += 1;
        }

        Ok(())
    }
}

/// Maps one record's members onto `slots`, spans relative to `base` (the address of the batch's first byte).
pub fn map_record(
    record: &[u8],
    columns: &MemberColumns,
    slots: &mut [Option<(usize, usize)>],
    base: usize,
) -> Result<(), RecordError> {
    let malformed = |e: serde_json::Error| RecordError::Malformed(e.to_string());

    if let Some(b'{' | b'[') = record.iter().find(|byte| !is_whitespace(byte)) {
        let mut deserializer = deserializer(record);
        deserializer
            .deserialize_any(Members { columns, slots, base })
            .map_err(malformed)?;

        return deserializer.end().map_err(malformed);
    }

    let mut deserializer = deserializer(record);
    let value = <&RawValue>::deserialize(&mut deserializer).map_err(malformed)?;
    deserializer.end().map_err(malformed)?;

    Err(RecordError::Scalar(scalar_type(value.get())))
}

fn custom<E: de::Error>(failure: &mut Option<PhpException>, cause: PhpException) -> E {
    *failure = Some(cause);

    E::custom("flow_php refused a JSON value")
}

/// A value `map_record()` validated (surrogates by `Framing`) that serde reads again cannot fail.
fn reread(error: serde_json::Error) -> PhpException {
    ext_exception(format!("flow_php failed to read validated JSON again: {error}"))
}

/// One value appended as `append_cast()` would append what `json_decode()` reads from it; `false` leaves a partial row
/// for the caller to truncate before the PHP lane takes the whole value.
fn into_column(values: &mut KindBuilder, kind: &Kind, cast: &CastKind, text: &str) -> Result<bool, PhpException> {
    if let CastKind::Optional(inner) = cast {
        if text == "null" {
            values.append_null();

            return Ok(true);
        }

        return into_column(values, kind, inner, text);
    }

    let leaf = |values: &mut KindBuilder, value: Zval| -> Result<bool, PhpException> {
        match native_leaf(kind, cast, &value)? {
            Some(native) => {
                append_native(values, native)?;

                Ok(true)
            }
            None => Ok(false),
        }
    };

    match text.as_bytes()[0] {
        b'"' => {
            let string = match text.contains('\\') {
                true => str_deserializer(text).deserialize_str(Text).map_err(reread)?,
                false => Cow::Borrowed(&text[1..text.len() - 1]),
            };

            native_text(values, kind, cast, string.as_bytes())
        }
        b'n' => Ok(false),
        b't' | b'f' => leaf(values, bool_zval(text == "true")),
        b'[' => match (kind, cast) {
            (Kind::List(element), CastKind::List(inner)) => {
                let mut failure = None;
                let cast = str_deserializer(text).deserialize_seq(Elements {
                    values: values.list_element(),
                    kind: &element.kind,
                    cast: inner,
                    failure: &mut failure,
                });

                match (cast, failure) {
                    (_, Some(failure)) => Err(failure),
                    (Err(e), None) => Err(reread(e)),
                    (Ok(false), None) => Ok(false),
                    (Ok(true), None) => {
                        values.end_entries().map_err(overflow)?;

                        Ok(true)
                    }
                }
            }
            _ => Ok(false),
        },
        b'{' => match (kind, cast) {
            (Kind::Map(key, item), CastKind::Map(key_cast, inner)) => {
                let entries = str_deserializer(text).deserialize_map(Entries).map_err(reread)?;

                for (name, value) in last_wins(entries) {
                    let (keys, items) = values.map_entries();
                    let cast = match (key_cast, array_key_index(name.as_bytes()), &key.kind) {
                        (MapKeyKind::Int, Some(index), Kind::Int64) => {
                            keys.append_fixed(&index.to_le_bytes());

                            true
                        }
                        (MapKeyKind::Str, None, Kind::Bytes) => {
                            keys.append_bytes(name.as_bytes()).map_err(overflow)?;

                            true
                        }
                        _ => false,
                    } && into_column(items, &item.kind, inner, value.get())?;

                    if !cast {
                        return Ok(false);
                    }
                }

                values.end_entries().map_err(overflow)?;

                Ok(true)
            }
            (Kind::Struct(fields), CastKind::Structure(elements)) => {
                let members = str_deserializer(text).deserialize_map(Fields(fields)).map_err(reread)?;

                for (((child, field), element), member) in values
                    .struct_children()
                    .iter_mut()
                    .zip(fields)
                    .zip(elements)
                    .zip(members)
                {
                    match member.map(RawValue::get) {
                        None if element.required => return Ok(false),
                        None => child.append_null(),
                        Some("null") if !matches!(element.kind, CastKind::Optional(_)) => return Ok(false),
                        Some(value) => {
                            if !into_column(child, &field.kind, &element.kind, value)? {
                                return Ok(false);
                            }
                        }
                    }
                }

                values.end_struct();

                Ok(true)
            }
            _ => Ok(false),
        },
        _ => leaf(values, number_zval(text)),
    }
}

/// The elements of a list, each through `into_column()`; the rest is skipped once one is not cast.
struct Elements<'a> {
    values: &'a mut KindBuilder,
    kind: &'a Kind,
    cast: &'a CastKind,
    failure: &'a mut Option<PhpException>,
}

impl<'de> Visitor<'de> for Elements<'_> {
    type Value = bool;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<bool, A::Error> {
        let Elements {
            values,
            kind,
            cast,
            failure,
        } = self;

        while let Some(value) = seq.next_element::<&'de RawValue>()? {
            match into_column(values, kind, cast, value.get()) {
                Ok(true) => {}
                Ok(false) => {
                    while seq.next_element::<IgnoredAny>()?.is_some() {}

                    return Ok(false);
                }
                Err(cause) => return Err(custom(failure, cause)),
            }
        }

        Ok(true)
    }
}

/// An object's members in document order, duplicates included.
struct Entries;

impl<'de> Visitor<'de> for Entries {
    type Value = Vec<(Cow<'de, str>, &'de RawValue)>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut entries = Vec::with_capacity(map.size_hint().unwrap_or(0));

        while let Some(name) = map.next_key_seed(Text)? {
            entries.push((name, map.next_value::<&'de RawValue>()?));
        }

        Ok(entries)
    }
}

/// An object's members by structure field, the last duplicate winning; a member no field names is skipped unread.
struct Fields<'a>(&'a [Field]);

impl<'de> Visitor<'de> for Fields<'_> {
    type Value = Vec<Option<&'de RawValue>>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
        let mut members = vec![None; self.0.len()];

        while let Some(name) = map.next_key_seed(Text)? {
            match self.0.iter().position(|field| field.name == name) {
                Some(position) => members[position] = Some(map.next_value::<&'de RawValue>()?),
                None => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }

        Ok(members)
    }
}

/// Up to this many members a repeated key is looked for pair by pair (at most 120 comparisons of short keys), past it
/// through a hash set: hashing every key and allocating the set costs more than the comparisons it saves.
const PAIRWISE_DUPLICATE_CHECK: usize = 16;

/// `json_decode()`'s array of the members: a repeated key keeps its first position and its last value.
fn last_wins<'a>(entries: Vec<(Cow<'a, str>, &'a RawValue)>) -> Vec<(Cow<'a, str>, &'a RawValue)> {
    let repeated = if entries.len() <= PAIRWISE_DUPLICATE_CHECK {
        entries
            .iter()
            .enumerate()
            .any(|(i, (name, _))| entries[..i].iter().any(|(earlier, _)| earlier == name))
    } else {
        let mut seen = HashSet::with_capacity(entries.len());

        !entries.iter().all(|(name, _)| seen.insert(name.as_ref()))
    };

    if !repeated {
        return entries;
    }

    let mut positions: HashMap<&str, usize> = HashMap::with_capacity(entries.len());
    let mut kept: Vec<(usize, usize)> = Vec::with_capacity(entries.len());

    for (index, (name, _)) in entries.iter().enumerate() {
        match positions.entry(name.as_ref()) {
            Entry::Occupied(position) => kept[*position.get()].1 = index,
            Entry::Vacant(position) => {
                position.insert(kept.len());
                kept.push((index, index));
            }
        }
    }

    kept.into_iter()
        .map(|(key, value)| (entries[key].0.clone(), entries[value].1))
        .collect()
}

/// The batch being built and the column each member feeds.
#[derive(Default)]
struct JsonColumns {
    batch: Option<BatchColumns>,
    members: MemberColumns,
}

pub struct JsonReader {
    uri: String,
    framing: Framing,
    /// The framed records of the pending batch, copied out of the input.
    records: Vec<u8>,
    bounds: Vec<Record>,
    slots: Vec<Option<(usize, usize)>>,
    columns: JsonColumns,
}

/// `Malformed JSON in "<uri>" at line N: …` (lines, 1-based) or `at element N` (a document, 0-based).
fn malformed(uri: &str, lines: bool, ordinal: usize, message: &str) -> PhpException {
    runtime(format!(
        "Malformed JSON in \"{uri}\" at {} {ordinal}: {message}",
        if lines { "line" } else { "element" },
    ))
}

impl JsonReader {
    fn clear(&mut self) {
        self.records.clear();
        self.bounds.clear();

        if let Some(batch) = self.columns.batch.as_mut() {
            batch.reset();
        }
    }

    fn read(&mut self, schema: &Zval, batch_size: usize) -> PhpResult<Option<HeldBatch>> {
        let (state, rebuilt) = BatchColumns::prepare(&mut self.columns.batch, schema, batch_size, "JSON")?;

        if rebuilt {
            self.columns.members = MemberColumns::of(state.columns.iter().map(|column| &column.key));
        }

        let batch_size = state.batch_size;
        let lines = self.framing.lines;

        while self.bounds.len() < batch_size {
            match self.framing.next(&mut self.records, &mut self.bounds) {
                Ok(true) => {}
                Ok(false) => break,
                Err(Malformed { ordinal, message }) => return Err(malformed(&self.uri, lines, ordinal, &message)),
            }

            if lines {
                let record = self.bounds.last().expect("framed above");

                if line_too_deep(&self.records[record.start..record.end]) {
                    return Err(malformed(&self.uri, lines, record.ordinal, TOO_DEEP));
                }
            }
        }

        let state = self.columns.batch.as_mut().expect("prepared above");
        state.rows = self.bounds.len();

        if self.bounds.is_empty() || (self.bounds.len() < state.batch_size && !self.framing.finished) {
            return Ok(None);
        }

        let width = state.columns.len();
        let base = self.records.as_ptr() as usize;

        self.slots.clear();
        self.slots.resize(self.bounds.len() * width, None);

        for (row, record) in self.bounds.iter().enumerate() {
            let slots = &mut self.slots[row * width..(row + 1) * width];

            match map_record(
                &self.records[record.start..record.end],
                &self.columns.members,
                slots,
                base,
            ) {
                Ok(()) => {}
                Err(RecordError::Malformed(message)) => {
                    return Err(malformed(&self.uri, lines, record.ordinal, &message))
                }
                Err(RecordError::Scalar(given)) => {
                    return Err(runtime(format!(
                        "A JSON record must be an object or an array, {given} given in \"{}\".",
                        self.uri,
                    )));
                }
            }
        }

        for (position, column) in state.columns.iter_mut().enumerate() {
            for row in 0..self.bounds.len() {
                if column.refusal.is_some() {
                    break;
                }

                let Some((start, end)) = self.slots[row * width + position] else {
                    if !column.nullable {
                        column.absent = column.absent.or(Some(row));
                    }

                    column.values.append_null();

                    continue;
                };
                // SAFETY: every record of the batch passed map_record(), whose serde parse checked the UTF-8 of each
                // member value; the span is one of those values, so it starts and ends on character boundaries
                let text = unsafe { std::str::from_utf8_unchecked(&self.records[start..end]) };

                let value = if text == "null" {
                    null_zval()
                } else {
                    let len = column.values.len();

                    if into_column(&mut column.values, &column.plan.kind, &column.plan.cast, text)? {
                        continue;
                    }

                    column.values.truncate(len);

                    decode(text)?
                };

                if let Append::Refused { cause, .. } = append_one(
                    &mut column.values,
                    &column.plan,
                    &column.definition,
                    column.nullable,
                    &value,
                )? {
                    column.refusal = Some((row, cause));
                }
            }
        }

        self.records.clear();
        self.bounds.clear();

        state.finish().map(Some)
    }
}

/// `json_decode($text, true)`: PHP's own value of a JSON value `into_column()` did not cast.
fn decode(text: &str) -> Result<Zval, PhpException> {
    let mut associative = Zval::new();
    associative.set_bool(true);
    let value = ctx::call_handle(
        ctx::json_decode()?,
        None,
        &mut [zval_str(text.as_bytes()), associative],
        "decode JSON",
    )?;

    if value.is_null() {
        return Err(ext_exception("flow_php expected json_decode() to read validated JSON"));
    }

    Ok(value)
}

impl JsonReader {
    /// `uri` names the file in refusals.
    pub fn new(lines: bool, uri: &[u8]) -> Self {
        Self {
            uri: String::from_utf8_lossy(uri).into_owned(),
            framing: Framing::new(lines),
            records: Vec::new(),
            bounds: Vec::new(),
            slots: Vec::new(),
            columns: JsonColumns::default(),
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.framing.feed(chunk);
    }

    /// Malformed input left in the buffer (a truncated record, a missing `]`) is refused by the next `nextColumns()`.
    pub fn finish(&mut self) {
        self.framing.finish();
    }

    /// Exactly `batch_size` rows keyed and ordered by `schema` while that many are buffered, the remainder after
    /// `finish()`, else none. Refusals are `RowsBuilder::appendRows()`'s, row indexes relative to the batch.
    pub fn next_columns(&mut self, schema: &Zval, batch_size: usize) -> PhpResult<Option<HeldBatch>> {
        let batch = self.read(schema, batch_size);

        if batch.is_err() {
            self.clear();
        }

        batch
    }
}

#[cfg(test)]
mod tests {
    use serde::de::Deserializer as _;

    use super::{
        last_wins, map_record, number, str_deserializer, Entries, Framing, Key, Malformed, MemberColumns, Number,
        Record, RecordError,
    };
    use crate::ctx::array_key_index;

    fn framed(lines: bool, chunks: &[&[u8]]) -> Result<Vec<(String, usize)>, Malformed> {
        let mut framing = Framing::new(lines);
        let mut records = Vec::new();
        let mut bounds: Vec<Record> = Vec::new();

        for chunk in chunks {
            framing.feed(chunk);

            while framing.next(&mut records, &mut bounds)? {}
        }

        framing.finish();

        while framing.next(&mut records, &mut bounds)? {}

        Ok(bounds
            .iter()
            .map(|record| {
                (
                    String::from_utf8(records[record.start..record.end].to_vec()).unwrap(),
                    record.ordinal,
                )
            })
            .collect())
    }

    fn bytes(text: &str) -> Vec<&[u8]> {
        text.as_bytes().chunks(1).collect()
    }

    fn nested(depth: usize) -> String {
        format!("{}1{}", "[".repeat(depth), "]".repeat(depth))
    }

    fn malformed(ordinal: usize, message: &str) -> Result<Vec<(String, usize)>, Malformed> {
        Err(Malformed {
            ordinal,
            message: message.to_string(),
        })
    }

    #[test]
    fn a_nested_record_fed_byte_by_byte_is_one_record() {
        let document = r#"[{"a":{"b":[1,"x]\"",{"c":2}]}},{"d":3}]"#;

        assert_eq!(
            framed(false, &bytes(document)).unwrap(),
            vec![
                (r#"{"a":{"b":[1,"x]\"",{"c":2}]}}"#.to_string(), 0),
                (r#"{"d":3}"#.to_string(), 1)
            ],
        );
        assert_eq!(
            framed(true, &bytes("{\"a\":[1,\n")).unwrap(),
            vec![(r#"{"a":[1,"#.to_string(), 1)]
        );
    }

    #[test]
    fn an_element_waits_for_its_end_across_chunks() {
        assert_eq!(
            framed(false, &[b"[{\"id\":1", b"2}, {\"id\"", b":3}\n]"]).unwrap(),
            vec![(r#"{"id":12}"#.to_string(), 0), (r#" {"id":3}"#.to_string() + "\n", 1)],
        );
    }

    #[test]
    fn one_leading_bom_is_skipped() {
        assert_eq!(
            framed(false, &[b"\xEF\xBB", b"\xBF [1]"]).unwrap(),
            vec![("1".to_string(), 0)]
        );
        assert_eq!(
            framed(true, &[b"\xEF\xBB\xBF{\"a\":1}\n"]).unwrap(),
            vec![(r#"{"a":1}"#.to_string(), 1)]
        );
        assert_eq!(
            framed(true, &[b"{\"a\":1}\n\xEF\xBB\xBF{\"a\":2}"]).unwrap(),
            vec![(r#"{"a":1}"#.to_string(), 1), ("\u{FEFF}{\"a\":2}".to_string(), 2)],
        );
    }

    #[test]
    fn blank_lines_and_empty_records_are_skipped_and_still_counted() {
        assert_eq!(
            framed(true, &[b"\n \t\x0B\x0C\r\n{}\n[ ]\n{\"a\":1}\r\n\n"]).unwrap(),
            vec![("{\"a\":1}\r".to_string(), 5)],
        );
        assert_eq!(
            framed(false, &[b"[{}, [], {\"a\":1}]"]).unwrap(),
            vec![(r#" {"a":1}"#.to_string(), 2)]
        );
    }

    #[test]
    fn an_empty_array_has_no_records_and_whitespace_may_follow_it() {
        assert_eq!(framed(false, &[b" \n[ \n] \n\t"]).unwrap(), vec![]);
        assert_eq!(framed(false, &[b"[1] \n"]).unwrap(), vec![("1".to_string(), 0)]);
    }

    #[test]
    fn content_after_the_closing_bracket_is_malformed() {
        assert_eq!(framed(false, &[b"[{\"id\":1}] x"]), malformed(1, "trailing characters"));
    }

    #[test]
    fn a_truncated_document_is_malformed() {
        assert_eq!(
            framed(false, &[b"[{\"id\":1}"]),
            malformed(0, "EOF while parsing a list")
        );
        assert_eq!(
            framed(false, &[b"[{\"id\":"]),
            malformed(0, "EOF while parsing a value at line 1 column 6")
        );
        assert_eq!(framed(false, &[b""]), malformed(0, "EOF while parsing a value"));
        assert_eq!(framed(false, &[b"{}"]), malformed(0, "expected `[`"));
    }

    #[test]
    fn nesting_deeper_than_json_decode_reads_is_refused() {
        assert_eq!(
            framed(false, &[format!("[{}]", nested(511)).as_bytes()]).unwrap().len(),
            1
        );
        assert_eq!(
            framed(false, &[format!("[{}]", nested(512)).as_bytes()]),
            malformed(0, "Maximum stack depth exceeded"),
        );
        assert!(!super::line_too_deep(nested(511).as_bytes()));
        assert!(super::line_too_deep(nested(512).as_bytes()));
        assert!(!super::line_too_deep(format!("[\"{}\"]", "[".repeat(600)).as_bytes()));
    }

    #[test]
    fn numbers_follow_json_decode() {
        assert_eq!(number("-0"), Number::Long(0));
        assert_eq!(number("9223372036854775807"), Number::Long(i64::MAX));
        assert_eq!(number("-9223372036854775808"), Number::Long(i64::MIN));
        assert_eq!(
            number("9223372036854775808"),
            Number::Double(9_223_372_036_854_775_808.0)
        );
        assert_eq!(
            number("-9223372036854775809"),
            Number::Double(-9_223_372_036_854_775_809.0)
        );
        assert_eq!(number("1E2"), Number::Double(100.0));
        assert_eq!(number("1e400"), Number::Double(f64::INFINITY));
        assert_eq!(number("1e-400"), Number::Double(0.0));

        let Number::Double(negative_zero) = number("-0.0") else {
            panic!("-0.0 is a float");
        };

        assert!(negative_zero == 0.0 && negative_zero.is_sign_negative());
    }

    /// Columns keyed as `Schema::definitions()` keys them: a canonical int name by its int.
    fn columns(names: &[&str]) -> MemberColumns {
        let keys: Vec<Key> = names
            .iter()
            .map(|name| match array_key_index(name.as_bytes()) {
                Some(index) => Key::Index(index),
                None => Key::Name(name.as_bytes().to_vec()),
            })
            .collect();

        MemberColumns::of(keys.iter())
    }

    fn mapped(record: &str, names: &[&str]) -> Result<Vec<Option<String>>, RecordError> {
        let mut slots = vec![None; names.len()];

        map_record(record.as_bytes(), &columns(names), &mut slots, record.as_ptr() as usize)?;

        Ok(slots
            .iter()
            .map(|slot| slot.map(|(start, end)| record[start..end].to_string()))
            .collect())
    }

    #[test]
    fn members_map_to_columns_by_name_and_the_last_duplicate_wins() {
        assert_eq!(
            mapped(
                r#" {"b": [1, {"x":2}], "zz": 0, "a": "1", "a": null} "#,
                &["a", "b", "c"]
            )
            .unwrap(),
            vec![Some("null".to_string()), Some(r#"[1, {"x":2}]"#.to_string()), None],
        );
        assert_eq!(
            mapped(r#"{"\u00e9\/":1,"e\u0301":2}"#, &["é/", "é"]).unwrap(),
            vec![Some("1".to_string()), None],
        );
    }

    #[test]
    fn an_array_record_maps_elements_to_numeric_names() {
        assert_eq!(
            mapped(r#"[10, "x", true]"#, &["2", "0"]).unwrap(),
            vec![Some("true".to_string()), Some("10".to_string())]
        );
    }

    #[test]
    fn a_scalar_record_names_its_type() {
        assert_eq!(mapped(" 5 ", &[]), Err(RecordError::Scalar("int")));
        assert_eq!(mapped("-0", &[]), Err(RecordError::Scalar("int")));
        assert_eq!(mapped("9223372036854775808", &[]), Err(RecordError::Scalar("float")));
        assert_eq!(mapped(r#""x""#, &[]), Err(RecordError::Scalar("string")));
        assert_eq!(mapped("false", &[]), Err(RecordError::Scalar("bool")));
        assert_eq!(mapped("null", &[]), Err(RecordError::Scalar("null")));
    }

    #[test]
    fn invalid_json_is_malformed() {
        for record in [
            r#"{"id":1}{"id":9}"#,
            "{\"id\":1}\x0B",
            "\0",
            "{\"a\":\"\x01\"}",
            "01",
            "NaN",
            "True",
            "{\"skipped\":\"\u{0}\"}",
        ] {
            assert!(
                matches!(mapped(record, &["id"]), Err(RecordError::Malformed(_))),
                "{record:?}"
            );
        }

        let invalid_utf8 = b"{\"skipped\":\"\xFF\"}";

        assert!(matches!(
            map_record(invalid_utf8, &columns(&[]), &mut [], invalid_utf8.as_ptr() as usize),
            Err(RecordError::Malformed(_)),
        ));
    }

    #[test]
    fn a_lone_surrogate_escape_is_malformed_in_any_member() {
        for record in [
            r#"{"skipped":"\ud800"}"#,
            r#"{"skipped":"\udc00"}"#,
            r#"{"skipped":"\ud83dx"}"#,
            r#"{"skipped":"\ud83d\u0041"}"#,
            r#"{"\ud800":1}"#,
            r#"{"a":"\ud83d"}"#,
        ] {
            assert_eq!(
                framed(true, &[record.as_bytes()]),
                malformed(1, "Single unpaired UTF-16 surrogate in unicode escape"),
                "{record}",
            );
        }

        assert_eq!(
            framed(true, &[r#"{"a":"\ud83d\ude00","b":"\\","c":"\\ud800"}"#.as_bytes()])
                .unwrap()
                .len(),
            1,
            "a pair, an escaped backslash, and an escaped backslash before u are no lone surrogates",
        );
    }

    fn deduplicated(object: &str) -> Vec<(String, String)> {
        let entries = str_deserializer(object).deserialize_map(Entries).unwrap();

        last_wins(entries)
            .into_iter()
            .map(|(name, value)| (name.into_owned(), value.get().to_string()))
            .collect()
    }

    fn object(members: &[(String, usize)]) -> String {
        format!(
            "{{{}}}",
            members
                .iter()
                .map(|(name, value)| format!("\"{name}\":{value}"))
                .collect::<Vec<_>>()
                .join(",")
        )
    }

    #[test]
    fn a_repeated_key_keeps_its_first_position_and_its_last_value_below_and_past_the_pairwise_check() {
        for size in [3, 40] {
            let mut members: Vec<(String, usize)> = (0..size).map(|i| (format!("k{i}"), i)).collect();

            assert_eq!(
                deduplicated(&object(&members)),
                members
                    .iter()
                    .map(|(name, value)| (name.clone(), value.to_string()))
                    .collect::<Vec<_>>(),
                "{size} unique keys stay as read",
            );

            members.push(("k1".to_string(), 99));
            let mut expected: Vec<(String, String)> = members[..size]
                .iter()
                .map(|(name, value)| (name.clone(), value.to_string()))
                .collect();
            expected[1].1 = "99".to_string();

            assert_eq!(deduplicated(&object(&members)), expected, "{size} keys and k1 again");
        }
    }
}
