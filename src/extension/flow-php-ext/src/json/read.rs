//! The reader behind `RustJsonOpenSource`: records framed and mapped by `flow_json_records`, each member appended as
//! `append_cast()` would append what `json_decode()` reads from it, with `RowsBuilder::appendRows()`'s results and
//! refusals.

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;

use ext_php_rs::exception::PhpException;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};
use flow_batch_frame::builder::KindBuilder;
use flow_batch_frame::key::array_key_index;
use flow_batch_frame::kind::Kind;
use flow_json_records::{
    last_wins, line_too_deep, map_record, number, str_deserializer, Entries, Fields, Framing, Malformed, MemberColumns,
    Number, Record, RecordError, Text, TOO_DEEP,
};
use serde::de::{self, Deserializer as _, IgnoredAny, SeqAccess, Visitor};
use serde_json::value::RawValue;

use crate::batch_columns::{native_text, BatchColumns};
use crate::builder::{append_native, append_one, native_leaf, Append};
use crate::cast::{CastKind, MapKeyKind};
use crate::corrupted::{from_values, missing_column, outside_the_sample, reason_of, unexpected_column};
use crate::ctx::{self, null_zval, zval_str};
use crate::exception::ext_exception;
use crate::physical::overflow;
use crate::render::runtime;
use crate::source::Fetched;

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

fn custom<E: de::Error>(failure: &mut Option<PhpException>, cause: PhpException) -> E {
    *failure = Some(cause);

    E::custom("flow_php refused a JSON value")
}

/// A value `map_record()` validated (surrogates by `Framing`) that serde reads again cannot fail.
fn reread(error: serde_json::Error) -> PhpException {
    ext_exception(format!("flow_php failed to read validated JSON again: {error}"))
}

/// One value as `append_cast()` appends what `json_decode()` reads; `false` leaves a partial row to truncate.
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
                            keys.append_i64(index);

                            true
                        }
                        (MapKeyKind::Str, None, Kind::Bytes) => {
                            keys.append_bytes(name.as_bytes());

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

/// The batch being built and the column each member feeds.
#[derive(Default)]
struct JsonColumns {
    batch: Option<BatchColumns>,
    members: MemberColumns,
}

pub struct JsonReader {
    uri: String,
    /// The byte of its file the stream begins at: a window's start, else 0.
    start: u64,
    framing: Framing,
    /// The framed records of the pending batch, copied out of the input.
    records: Vec<u8>,
    bounds: Vec<Record>,
    slots: Vec<Option<(usize, usize)>>,
    columns: JsonColumns,
    /// A read batch's corrupted records, then the batch itself.
    ready: VecDeque<Fetched>,
}

/// `Malformed JSON in "<uri>" at line N: …` (lines, 1-based) or `at element N` (a document, 0-based).
fn malformed(uri: &str, lines: bool, ordinal: usize, message: &str) -> PhpException {
    runtime(format!(
        "Malformed JSON in \"{uri}\" at {} {ordinal}: {message}",
        if lines { "line" } else { "element" },
    ))
}

/// A line by its byte offset in the file (and its line when the read began at the file's first byte), an array
/// element by its 1-based number.
fn located(start: u64, lines: bool, record: &Record) -> (u64, Option<u64>) {
    match lines {
        true => (start + record.offset, (start == 0).then_some(record.ordinal as u64)),
        false => (record.ordinal as u64 + 1, None),
    }
}

/// A record of a batch refused: by a value (the earliest definition wins) or, ranked after every refused value, by a
/// NOT NULL column it lacks - RowsBuilder's order.
struct Refused {
    row: usize,
    absent: bool,
    cause: Zval,
}

/// Each record's members into the batch's columns, the rows of `skip` (ascending) left out; the refusals, one per
/// refused record, ascending by row.
fn fill(
    state: &mut BatchColumns,
    records: &[u8],
    slots: &[Option<(usize, usize)>],
    rows: usize,
    skip: &[usize],
) -> PhpResult<Vec<Refused>> {
    let width = state.columns.len();
    let mut refusals: Vec<Refused> = Vec::new();

    for (position, column) in state.columns.iter_mut().enumerate() {
        for row in (0..rows).filter(|row| skip.binary_search(row).is_err()) {
            let Some((start, end)) = slots[row * width + position] else {
                if !column.nullable {
                    refusals.push(Refused {
                        row,
                        absent: true,
                        cause: missing_column(&column.definition)?,
                    });
                }

                column.values.append_null();

                continue;
            };
            // SAFETY: every record of the batch passed map_record(), whose serde parse checked the UTF-8 of each
            // member value; the span is one of those values, so it starts and ends on character boundaries
            let text = unsafe { std::str::from_utf8_unchecked(&records[start..end]) };

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
                refusals.push(Refused {
                    row,
                    absent: false,
                    cause,
                });
            }
        }
    }

    // stable: within a row and kind, the earliest definition stays first
    refusals.sort_by_key(|refused| (refused.row, refused.absent));
    refusals.dedup_by_key(|refused| refused.row);

    Ok(refusals)
}

impl JsonReader {
    fn clear(&mut self) {
        self.records.clear();
        self.bounds.clear();

        if let Some(batch) = self.columns.batch.as_mut() {
            batch.reset();
        }
    }

    /// Reads the next batch into `ready`: its corrupted records, then its rows.
    fn read(&mut self, schema: &Zval, batch_size: usize, inference: Option<&Zval>, known: &[Vec<u8>]) -> PhpResult<()> {
        let (state, rebuilt) = BatchColumns::prepare(&mut self.columns.batch, schema, batch_size, "JSON")?;

        if rebuilt {
            let members = MemberColumns::of(state.columns.iter().map(|column| &column.key));

            self.columns.members = match inference {
                Some(_) => members.knowing(known.iter().cloned()),
                None => members,
            };
        }

        let batch_size = state.batch_size;
        let lines = self.framing.lines();

        // a refused record fills its place in the batch, as RecordBatch counts it
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

        if self.bounds.is_empty() || (self.bounds.len() < state.batch_size && !self.framing.finished()) {
            return Ok(());
        }

        let width = state.columns.len();
        let base = self.records.as_ptr() as usize;
        let mut unknown = None;

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
                Ok(None) => {}
                Ok(Some(name)) => {
                    unknown = unknown.or(Some((row, name)));
                }
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

        let uri = || zval_str(self.uri.as_bytes());

        if let (Some((row, name)), Some(inference)) = (unknown, inference) {
            let (offset, line) = located(self.start, lines, &self.bounds[row]);

            return Err(outside_the_sample(
                &uri(),
                offset,
                line,
                inference,
                row,
                unexpected_column(&name)?,
            ));
        }

        let rows = self.bounds.len();
        let refusals = fill(state, &self.records, &self.slots, rows, &[])?;

        if !refusals.is_empty() {
            if let Some(inference) = inference {
                let first = refusals.iter().find(|refused| !refused.absent).unwrap_or(&refusals[0]);
                let (offset, line) = located(self.start, lines, &self.bounds[first.row]);

                return Err(outside_the_sample(
                    &uri(),
                    offset,
                    line,
                    inference,
                    first.row,
                    first.cause.shallow_clone(),
                ));
            }

            for refused in &refusals {
                let record = &self.bounds[refused.row];
                let text = &self.records[record.start..record.end];
                let (offset, line) = located(self.start, lines, record);
                let reason = reason_of(&refused.cause)?;

                // a line is its own text; an array element, which the source holds no text of alone, is its values
                self.ready.push_back(Fetched::Corrupted(match lines {
                    true => from_values(&uri(), offset, line, &reason, empty_array(), Some(text))?,
                    false => from_values(
                        &uri(),
                        offset,
                        line,
                        &reason,
                        // SAFETY: map_record() parsed the record, checking its UTF-8
                        decode(unsafe { std::str::from_utf8_unchecked(text) })?,
                        None,
                    )?,
                }));
            }

            let skip: Vec<usize> = refusals.iter().map(|refused| refused.row).collect();

            state.reset();
            fill(state, &self.records, &self.slots, rows, &skip)?;
            state.rows = rows - skip.len();
        } else {
            state.rows = rows;
        }

        if state.rows > 0 {
            self.ready.push_back(Fetched::Batch(state.finish()?));
        }

        self.records.clear();
        self.bounds.clear();

        Ok(())
    }
}

fn empty_array() -> Zval {
    let mut array = Zval::new();
    array.set_hashtable(ZendHashTable::new());

    array
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
    /// `uri` names the file in malformed-input refusals; `start` is the byte of its file the stream begins at.
    pub fn new(lines: bool, uri: &[u8], start: u64) -> Self {
        Self {
            uri: String::from_utf8_lossy(uri).into_owned(),
            start,
            framing: Framing::new(lines),
            records: Vec::new(),
            bounds: Vec::new(),
            slots: Vec::new(),
            columns: JsonColumns::default(),
            ready: VecDeque::new(),
        }
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.framing.feed(chunk);
    }

    /// Malformed input left in the buffer (a truncated record, a missing `]`) is refused by the next `nextColumns()`.
    pub fn finish(&mut self) {
        self.framing.finish();
    }

    /// Exactly `batch_size` records while buffered, the remainder after `finish()`; corrupted records come first.
    pub fn next_columns(
        &mut self,
        schema: &Zval,
        batch_size: usize,
        inference: Option<&Zval>,
        known: &[Vec<u8>],
    ) -> PhpResult<Option<Fetched>> {
        if self.ready.is_empty() {
            if let Err(e) = self.read(schema, batch_size, inference, known) {
                self.clear();

                return Err(e);
            }
        }

        Ok(self.ready.pop_front())
    }
}
