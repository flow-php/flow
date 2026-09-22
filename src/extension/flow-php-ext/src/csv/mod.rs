//! Native CSV reading: records and fields from `tokenizer`, rows shaped exactly like
//! `CSVEncoder::decode()` + `CSVRowNormalizer::normalize()` shape them.

pub mod fold;
pub mod tokenizer;

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};

use crate::ctx::{array_key_index, ht_insert_key, null_zval, write_slot, zval_str, HtKey};
use crate::exception::ext_exception;
use crate::hydrate::RowValuesClass;
use tokenizer::{Dialect, Field, Record, Tokenizer};

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

/// One key of `RawRowValues::$values`: `array_combine()` keeps a duplicated header at its first position with the
/// value of its last one.
struct Cell {
    header_index: usize,
    field_index: usize,
}

pub struct CsvReader {
    tokenizer: Tokenizer,
    with_header: bool,
    empty_to_null: bool,
    headers: Option<Vec<Header>>,
    cells: Vec<Cell>,
    record: Record,
    first_row_pending: bool,
    consumed_bytes: u64,
}

impl CsvReader {
    pub fn new(
        separator: &[u8],
        enclosure: &[u8],
        escape: &[u8],
        with_header: bool,
        empty_to_null: bool,
        remove_bom: bool,
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

        Ok(Self {
            tokenizer: Tokenizer::new(
                Dialect {
                    separator: *separator,
                    enclosure: *enclosure,
                    escape,
                },
                remove_bom,
            ),
            with_header,
            empty_to_null,
            headers: None,
            cells: Vec::new(),
            record: Record::default(),
            first_row_pending: false,
            consumed_bytes: 0,
        })
    }

    pub fn feed(&mut self, chunk: &[u8]) {
        self.tokenizer.feed(chunk);
    }

    pub fn finish(&mut self) {
        self.tokenizer.finish();
    }

    /// The bytes, as read, of every row `next` and `fold` produced so far: line endings included, the header
    /// record excluded.
    pub fn consumed_bytes(&self) -> u64 {
        self.consumed_bytes
    }

    /// The resolved header, `[]` until the first record is available.
    pub fn headers(&mut self) -> Vec<&[u8]> {
        self.resolve_headers();

        self.headers
            .as_ref()
            .map(|headers| headers.iter().map(|header| header.name.as_slice()).collect())
            .unwrap_or_default()
    }

    /// Up to `batch_size` `RawRowValues`; an empty list when no complete record is buffered.
    pub fn next(&mut self, batch_size: usize, class: &RowValuesClass) -> Result<ZBox<ZendHashTable>, PhpException> {
        self.resolve_headers();

        let mut batch = ZendHashTable::new();

        let Some(headers) = self.headers.as_ref() else {
            return Ok(batch);
        };

        while batch.len() < batch_size {
            if !next_row(&mut self.first_row_pending, &mut self.tokenizer, &mut self.record, &mut self.consumed_bytes) {
                break;
            }

            batch
                .push(row_values(headers, &self.record, self.empty_to_null, class))
                .map_err(|e| ext_exception(format!("flow_php failed to collect CSV row values: {e:?}")))?;
        }

        Ok(batch)
    }

    /// Folds up to `limit` rows (all when `None`) into `fold`; returns how many it folded - fewer only when no
    /// further complete record is buffered.
    pub fn fold(&mut self, fold: &mut fold::Fold, limit: Option<u64>) -> Result<u64, PhpException> {
        self.resolve_headers();

        let Some(headers) = self.headers.as_ref() else {
            return Ok(0);
        };

        let mut folded = 0;
        let mut positions = vec![None; self.cells.len()];

        while limit.is_none_or(|limit| folded < limit) {
            if !next_row(&mut self.first_row_pending, &mut self.tokenizer, &mut self.record, &mut self.consumed_bytes) {
                break;
            }

            for (cell, position) in self.cells.iter().zip(positions.iter_mut()) {
                fold.observe(
                    &headers[cell.header_index].name,
                    position,
                    cell_value(&self.record, cell.field_index, self.empty_to_null),
                )?;
            }

            fold.end_row();
            folded += 1;
        }

        Ok(folded)
    }

    /// `CSVEncoder::decode()`'s first line: the header when `withHeader`, else `e00, e01, ...` sized by
    /// the first record, which then stays pending as the first row.
    fn resolve_headers(&mut self) {
        if self.headers.is_some() || !self.tokenizer.next(&mut self.record) {
            return;
        }

        let record = &self.record;

        self.headers = Some(if self.with_header {
            (0..record.len())
                .map(|index| {
                    let trimmed = php_trim(match record.field(index) {
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

            (0..record.len()).map(|index| Header::new(format!("e{index:02}").into_bytes())).collect()
        });

        let headers = self.headers.as_ref().expect("resolved above");

        for (index, header) in headers.iter().enumerate() {
            match self.cells.iter_mut().find(|cell| headers[cell.header_index].name == header.name) {
                Some(cell) => cell.field_index = index,
                None => self.cells.push(Cell {
                    header_index: index,
                    field_index: index,
                }),
            }
        }
    }
}

/// The first row is pending when auto headers were sized by it; otherwise the next complete record. Either way it is
/// the tokenizer's last record, so its bytes are added to `consumed_bytes`.
fn next_row(
    first_row_pending: &mut bool,
    tokenizer: &mut Tokenizer,
    record: &mut Record,
    consumed_bytes: &mut u64,
) -> bool {
    if !(std::mem::take(first_row_pending) || tokenizer.next(record)) {
        return false;
    }

    *consumed_bytes += tokenizer.last_record_bytes() as u64;

    true
}

/// `CSVRowNormalizer::normalize()` for one cell: padding past the record's end, `emptyToNull`; `None` is null.
fn cell_value(record: &Record, index: usize, empty_to_null: bool) -> Option<&[u8]> {
    match record.field(index) {
        Field::Value(value) if !(empty_to_null && value.is_empty()) => Some(value),
        Field::Value(_) | Field::Null => None,
        Field::Missing if empty_to_null => None,
        Field::Missing => Some(b""),
    }
}

/// `new RawRowValues(array_combine($headers, $normalizer->normalize($fields, count($headers))))`.
fn row_values(headers: &[Header], record: &Record, empty_to_null: bool, class: &RowValuesClass) -> ZBox<ZendObject> {
    let mut values = ZendHashTable::with_capacity(headers.len() as u32);

    for (index, header) in headers.iter().enumerate() {
        let value = cell_value(record, index, empty_to_null).map_or_else(null_zval, zval_str);

        ht_insert_key(&mut values, &header.key(), value);
    }

    let mut row_values = ZendObject::new(class.ce);

    let mut values_zv = Zval::new();
    values_zv.set_hashtable(values);
    write_slot(&mut row_values, class.values_slot, values_zv);

    let mut metadata_zv = Zval::new();
    metadata_zv.set_hashtable(ZendHashTable::new());
    write_slot(&mut row_values, class.metadata_slot, metadata_zv);

    row_values
}

/// PHP's `trim()`: `" \t\n\r\0\x0B"`, nothing else.
pub fn php_trim(bytes: &[u8]) -> &[u8] {
    let is_trimmed = |byte: &u8| matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | b'\0' | 0x0B);
    let start = bytes.iter().position(|byte| !is_trimmed(byte)).unwrap_or(bytes.len());
    let end = bytes.iter().rposition(|byte| !is_trimmed(byte)).map_or(start, |end| end + 1);

    &bytes[start..end]
}
