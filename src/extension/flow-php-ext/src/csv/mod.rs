//! Native CSV reading: records and fields from `flow_csv_tokenizer`, rows shaped exactly like
//! `CSVDecoder::decode()` (`Flow\ETL\IO\Extractor\Grid\RecordDecoder`) shapes them.

pub mod columns;
pub mod fold;
#[allow(
    non_snake_case,
    reason = "PHP takes a parameter name from its Rust identifier: the contracts' are camelCase"
)]
pub mod source;
pub mod write;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendStr, Zval};
use flow_batch_frame::key::array_key_index;
use flow_csv_tokenizer::{Dialect, Field, Record, Tokenizer};

use crate::ctx::{ht_insert_key, null_zval, zval_str, HtKey};
use crate::exception::ext_exception;

/// A header name resolved once into the array key `array_combine()` would use for it.
enum HeaderKey {
    Index(i64),
    Str(ZBox<ZendStr>),
}

struct Header {
    name: Vec<u8>,
    key: HeaderKey,
}

impl Header {
    fn new(name: Vec<u8>) -> Self {
        let key = match array_key_index(&name) {
            Some(index) => HeaderKey::Index(index),
            None => HeaderKey::Str(ZendStr::new(&name, false)),
        };

        Self { name, key }
    }

    fn key(&self) -> HtKey<'_> {
        match &self.key {
            HeaderKey::Index(index) => HtKey::Index(*index),
            HeaderKey::Str(name) => HtKey::Str(name),
        }
    }
}

/// One key of a row: `array_combine()` keeps a duplicated header at its first position with the
/// value of its last one.
struct Cell {
    header_index: usize,
    field_index: usize,
}

pub struct CsvReader {
    tokenizer: Tokenizer,
    record: Record,
    /// The first record sized the generated header and is still to be handed out as a row.
    first_row_pending: bool,
    /// The byte the next record starts at in the source: the window's start plus every record read since.
    position: u64,
    /// Lines are counted only when the read began at the source's first byte.
    from_first_byte: bool,
    record_offset: u64,
    /// The bytes, as read, of every row handed out: line endings included, the header excluded.
    consumed_bytes: u64,
    consumed_rows: u64,
    with_header: bool,
    empty_to_null: bool,
    missing_columns_as_null: bool,
    headers: Option<Vec<Header>>,
    cells: Vec<Cell>,
    finished: bool,
}

impl CsvReader {
    /// `start` is the source byte the stream begins at: a window's start, else 0.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        separator: &[u8],
        enclosure: &[u8],
        escape: &[u8],
        with_header: bool,
        empty_to_null: bool,
        remove_bom: bool,
        headers: Option<Vec<Vec<u8>>>,
        missing_columns_as_null: bool,
        start: u64,
    ) -> Result<Self, PhpException> {
        let [separator] = separator else {
            return Err(ext_exception("flow_php CSV separator must be exactly one byte"));
        };
        let [enclosure] = enclosure else {
            return Err(ext_exception("flow_php CSV enclosure must be exactly one byte"));
        };
        let escape = match escape {
            [] => None,
            [escape] => Some(*escape),
            _ => return Err(ext_exception("flow_php CSV escape must be empty or exactly one byte")),
        };

        if headers.is_some() && with_header {
            return Err(ext_exception(
                "flow_php CSV preset headers replace the header record, withHeader must be false",
            ));
        }

        let mut reader = Self {
            tokenizer: Tokenizer::new(
                Dialect {
                    separator: *separator,
                    enclosure: *enclosure,
                    escape,
                },
                remove_bom,
            ),
            record: Record::default(),
            first_row_pending: false,
            position: start,
            from_first_byte: start == 0,
            record_offset: start,
            consumed_bytes: 0,
            consumed_rows: 0,
            with_header,
            empty_to_null,
            missing_columns_as_null,
            headers: None,
            cells: Vec::new(),
            finished: false,
        };

        if let Some(names) = headers {
            reader.headers = Some(names.into_iter().map(Header::new).collect());
            reader.resolve_cells();
        }

        Ok(reader)
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.tokenizer.feed(chunk);
    }

    /// Up to `len` bytes read straight into the tokenizer's buffer by `read`; see `Tokenizer::fill_with`.
    pub fn fill_with(
        &mut self,
        len: usize,
        read: impl FnOnce(&mut [u8]) -> std::io::Result<usize>,
    ) -> std::io::Result<usize> {
        self.tokenizer.fill_with(len, read)
    }

    pub fn finish(&mut self) {
        self.tokenizer.finish();
        self.finished = true;
    }

    pub fn is_finished(&self) -> bool {
        self.finished
    }

    /// The field of the header named `name`, the last of duplicates as `array_combine()` keeps it.
    pub fn field_of(&mut self, name: &[u8]) -> Option<usize> {
        self.resolve_headers();
        let headers = self.headers.as_ref()?;

        self.cells
            .iter()
            .find(|cell| headers[cell.header_index].name == name)
            .map(|cell| cell.field_index)
    }

    /// Advances to the next record that is not an empty line, counting a row's bytes: `RecordDecoder::decode()`'s
    /// outcome, the reason a corrupted record is refused, `None` when no complete record is buffered.
    pub fn next_record(&mut self) -> Option<Result<(), String>> {
        self.resolve_headers();

        let headers = self.headers.as_ref()?.len();

        if !self.next_nonempty() {
            return None;
        }

        if let Some(reason) = self.corruption(headers) {
            return Some(Err(reason));
        }

        self.consumed_bytes += self.tokenizer.last_record_bytes() as u64;
        self.consumed_rows += 1;

        Some(Ok(()))
    }

    /// The current row's cell of `field`, as `Records::decode()` shapes it; `None` is null.
    pub fn cell(&self, field: usize) -> Option<&[u8]> {
        cell_value(&self.tokenizer, &self.record, field, self.empty_to_null)
    }

    /// The current record's byte offset in the source.
    pub fn record_offset(&self) -> u64 {
        self.record_offset
    }

    /// The current record's line; `None` when the read began past the source's first byte.
    pub fn record_line(&mut self) -> Option<u64> {
        self.from_first_byte.then(|| self.tokenizer.line(&self.record))
    }

    /// The current record as the source holds it, its line ending excluded.
    pub fn record_text(&self) -> &[u8] {
        self.tokenizer.text(&self.record)
    }

    /// The current record's fields as split, `''` for an empty one.
    pub fn record_fields(&self) -> Vec<&[u8]> {
        (0..self.record.field_count())
            .map(|index| match self.tokenizer.field(&self.record, index) {
                Field::Value(value) => value,
                Field::Null | Field::Missing => b"",
            })
            .collect()
    }

    /// The bytes, as read, of every row `next` and `fold` produced so far: line endings included, the header
    /// record excluded.
    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }

    /// How many rows `next_record` handed out, a refused cell's included.
    pub fn consumed_rows(&self) -> u64 {
        self.consumed_rows
    }

    /// The resolved header, `[]` until the first record is available.
    pub fn headers(&mut self) -> Vec<&[u8]> {
        self.resolve_headers();

        self.headers
            .as_ref()
            .map(|headers| headers.iter().map(|header| header.name.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Up to `batch_size` rows keyed by header, corrupted records left out; an empty list when no complete record is
    /// buffered.
    pub fn next(&mut self, batch_size: usize) -> Result<ZBox<ZendHashTable>, PhpException> {
        let mut batch = ZendHashTable::new();

        while batch.len() < batch_size {
            match self.next_record() {
                None => break,
                Some(Err(_)) => continue,
                Some(Ok(())) => {}
            }

            let headers = self.headers.as_ref().expect("resolved by next_record()");

            batch
                .push(row_values(headers, &self.tokenizer, &self.record, self.empty_to_null))
                .map_err(|e| ext_exception(format!("flow_php failed to collect CSV row values: {e:?}")))?;
        }

        Ok(batch)
    }

    /// Folds up to `limit` rows (all when `None`), corrupted records left out, into `fold`; fewer only when no further
    /// complete record is buffered.
    pub fn fold(&mut self, fold: &mut fold::Fold, limit: Option<u64>) -> Result<(), PhpException> {
        let mut folded = 0;
        let mut positions = vec![None; self.cells.len()];

        while limit.is_none_or(|limit| folded < limit) {
            match self.next_record() {
                None => break,
                Some(Err(_)) => continue,
                Some(Ok(())) => {}
            }

            let headers = self.headers.as_ref().expect("resolved by next_record()");

            if positions.len() != self.cells.len() {
                positions = vec![None; self.cells.len()];
            }

            for (cell, position) in self.cells.iter().zip(positions.iter_mut()) {
                fold.observe(
                    &headers[cell.header_index].name,
                    position,
                    cell_value(&self.tokenizer, &self.record, cell.field_index, self.empty_to_null),
                )?;
            }

            fold.end_row();
            folded += 1;
        }

        Ok(())
    }

    /// The tokenizer's next record, its offset taken; `false` when none is buffered.
    fn advance(&mut self) -> bool {
        if !self.tokenizer.next(&mut self.record) {
            return false;
        }

        self.record_offset = self.position;
        self.position += self.tokenizer.last_record_bytes() as u64;

        true
    }

    /// The next record that is not an empty line: the pending first one when auto headers were sized by it.
    fn next_nonempty(&mut self) -> bool {
        if std::mem::take(&mut self.first_row_pending) {
            return true;
        }

        while self.advance() {
            if !self.record.is_empty() {
                return true;
            }
        }

        false
    }

    /// `RecordDecoder::decode()`'s verdict on the current record: why it is corrupted, or `None`.
    fn corruption(&self, headers: usize) -> Option<String> {
        let fields = self.record.field_count();
        let extra = (headers..fields).any(|index| match self.tokenizer.field(&self.record, index) {
            Field::Value(value) => !value.is_empty(),
            Field::Null | Field::Missing => false,
        });

        (extra || (fields < headers && !self.missing_columns_as_null))
            .then(|| format!("{fields} fields, the header has {headers}"))
    }

    /// `Records::decode()`'s first record that is not an empty line: the header when `withHeader`, else
    /// `e00, e01, ...` sized by that record, which then stays pending as the first row.
    fn resolve_headers(&mut self) {
        if self.headers.is_some() {
            return;
        }

        loop {
            if !self.advance() {
                return;
            }

            if !self.record.is_empty() {
                break;
            }
        }

        let (tokenizer, record) = (&self.tokenizer, &self.record);

        self.headers = Some(if self.with_header {
            (0..record.field_count())
                .map(|index| {
                    let trimmed = php_trim(match tokenizer.field(record, index) {
                        Field::Value(value) => value,
                        Field::Null | Field::Missing => b"",
                    });

                    Header::new(if trimmed.is_empty() {
                        format!("e{index:02}").into_bytes()
                    } else {
                        trimmed.to_vec()
                    })
                })
                .collect()
        } else {
            self.first_row_pending = true;

            (0..record.field_count())
                .map(|index| Header::new(format!("e{index:02}").into_bytes()))
                .collect()
        });

        self.resolve_cells();
    }

    fn resolve_cells(&mut self) {
        let headers = self.headers.as_ref().expect("resolved before the cells");

        for (index, header) in headers.iter().enumerate() {
            match self
                .cells
                .iter_mut()
                .find(|cell| headers[cell.header_index].name == header.name)
            {
                Some(cell) => cell.field_index = index,
                None => self.cells.push(Cell {
                    header_index: index,
                    field_index: index,
                }),
            }
        }
    }
}

/// `Records::decode()` for one cell; `None` is null.
fn cell_value<'a>(tokenizer: &'a Tokenizer, record: &'a Record, index: usize, empty_to_null: bool) -> Option<&'a [u8]> {
    match tokenizer.field(record, index) {
        Field::Value(value) if !(empty_to_null && value.is_empty()) => Some(value),
        Field::Value(_) | Field::Null | Field::Missing => None,
    }
}

/// `Records::decode()` for one record: one value per header, a duplicated header keeps its first position and its
/// last value.
fn row_values(headers: &[Header], tokenizer: &Tokenizer, record: &Record, empty_to_null: bool) -> Zval {
    let mut values = ZendHashTable::with_capacity(headers.len() as u32);

    for (index, header) in headers.iter().enumerate() {
        let value = cell_value(tokenizer, record, index, empty_to_null).map_or_else(null_zval, zval_str);

        ht_insert_key(&mut values, &header.key(), value);
    }

    let mut values_zv = Zval::new();
    values_zv.set_hashtable(values);

    values_zv
}

/// PHP's `trim()`: `" \t\n\r\0\x0B"`, nothing else.
pub fn php_trim(bytes: &[u8]) -> &[u8] {
    let is_trimmed = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | b'\0' | 0x0B);
    let start = bytes.iter().position(|byte| !is_trimmed(byte)).unwrap_or(bytes.len());
    let end = bytes
        .iter()
        .rposition(|byte| !is_trimmed(byte))
        .map_or(start, |end| end + 1);

    &bytes[start..end]
}
