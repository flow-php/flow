//! `Flow\ETL\Adapter\CSV\RustCSVEncoder`: a `Rows` batch as the bytes `PhpCSVEncoder::encode()` returns, rendered
//! from the arrow arrays; the columns it does not render are the cells the held `PhpCSVEncoder::cells()` produces.

use ext_php_rs::binary::Binary;
use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::ctx::{call_method, find_class};
use crate::exception::ext_exception;
use crate::interfaces::csv_encoder_ce;
use crate::render::invalid_argument;
use crate::text::batch::{batch, cells, renderer, Unrendered};
use crate::text::value::Formats;
use crate::text::Error;

/// `json_encode()`'s refusal of a nested cell, as `JSON_THROW_ON_ERROR` throws it.
fn json_exception(error: Error) -> PhpException {
    match find_class("JsonException") {
        Ok(ce) => PhpException::new(error.message().to_string(), error.code(), ce),
        Err(e) => e,
    }
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\CSV\\RustCSVEncoder",
    flags = ClassFlags::Final,
    implements(ce = csv_encoder_ce, stub = "Flow\\ETL\\Adapter\\CSV\\CSVEncoder")
)]
pub struct RustCSVEncoder {
    separator: u8,
    enclosure: u8,
    escape: Option<u8>,
    new_line: Vec<u8>,
    /// The bytes that make `fputcsv()` enclose a field.
    enclosing: [bool; 256],
    /// The separator, the enclosure and the escape character cannot occur in a number, a boolean or a uuid.
    plain_kinds: bool,
    formats: Formats,
    /// The `PhpCSVEncoder` that renders the unrendered columns and the header.
    php: Zval,
    unrendered: Unrendered,
}

impl RustCSVEncoder {
    /// One field as `fputcsv()` writes it (ext/standard/file.c `php_fputcsv()`).
    fn field(&self, out: &mut Vec<u8>, text: &[u8]) {
        if !text.iter().any(|byte| self.enclosing[usize::from(*byte)]) {
            out.extend_from_slice(text);

            return;
        }

        let mut escaped = false;

        out.push(self.enclosure);

        for byte in text {
            if self.escape == Some(*byte) {
                escaped = true;
            } else if !escaped && *byte == self.enclosure {
                out.push(self.enclosure);
            } else {
                escaped = false;
            }

            out.push(*byte);
        }

        out.push(self.enclosure);
    }
}

#[php_impl]
impl RustCSVEncoder {
    pub fn __construct(
        separator: BinarySlice<u8>,
        enclosure: BinarySlice<u8>,
        escape: BinarySlice<u8>,
        new_line_separator: BinarySlice<u8>,
        date_time_format: BinarySlice<u8>,
        date_format: BinarySlice<u8>,
        php: &Zval,
    ) -> PhpResult<Self> {
        if separator.len() != 1 {
            return Err(invalid_argument("Separator must be a single character".to_string()));
        }

        if enclosure.len() != 1 {
            return Err(invalid_argument("Enclosure must be a single character".to_string()));
        }

        if escape.len() > 1 {
            return Err(invalid_argument(
                "Escape must be empty or a single character".to_string(),
            ));
        }

        let (separator, enclosure, escape) = (separator[0], enclosure[0], escape.first().copied());
        let mut enclosing = [false; 256];

        for byte in [
            Some(separator),
            Some(enclosure),
            escape,
            Some(b'\n'),
            Some(b'\r'),
            Some(b'\t'),
            Some(b' '),
        ]
        .into_iter()
        .flatten()
        {
            enclosing[usize::from(byte)] = true;
        }

        Ok(Self {
            separator,
            enclosure,
            escape,
            new_line: new_line_separator.to_vec(),
            enclosing,
            plain_kinds: ![Some(separator), Some(enclosure), escape]
                .into_iter()
                .flatten()
                .any(|byte| byte.is_ascii_alphanumeric() || b"_+./:-".contains(&byte) || byte >= 0x80),
            formats: Formats::parse(&date_time_format, &date_format),
            php: php.shallow_clone(),
            unrendered: Unrendered::default(),
        })
    }

    /// The rows as `PhpCSVEncoder::encode()` writes them; the unrendered columns through the held encoder's `cells()`.
    pub fn encode(&mut self, rows: &Zval) -> PhpResult<Binary<u8>> {
        let cells = self.unrendered.render(rows, &self.php, "cells", &self.formats, true)?;

        self.render_rows(rows, &cells)
    }

    #[php(name = "encodeHeader")]
    pub fn encode_header(&self, headers: &Zval) -> PhpResult<Binary<u8>> {
        call_method(&self.php, "encodeHeader", &mut [headers.shallow_clone()])?
            .zend_str()
            .map(|header| Binary::new(header.as_bytes()))
            .ok_or_else(|| ext_exception("flow_php expected PhpCSVEncoder::encodeHeader() to return a string"))
    }
}

impl RustCSVEncoder {
    /// `$cells_of`: `PhpCSVEncoder::cells()` of the unrendered columns, each of `$rows->count()` values.
    fn render_rows(&self, rows: &Zval, cells_of: &ZendHashTable) -> PhpResult<Binary<u8>> {
        let batch = batch(rows, &self.formats, true)?;
        let mut fields = Vec::with_capacity(batch.columns.len());
        let mut scratch = Vec::new();

        for column in &batch.columns {
            let mut arena = Vec::with_capacity(batch.count * 16);
            let mut ends = Vec::with_capacity(batch.count);

            match &column.native {
                Some((plan, data)) => {
                    let renderer = renderer(plan, data, &self.formats)?;
                    let plain = self.plain_kinds && renderer.is_plain();

                    for i in 0..batch.count {
                        if !renderer.is_null(i) {
                            if plain {
                                renderer.text(&mut arena, i).map_err(json_exception)?;
                            } else if let Some(text) = renderer.bytes(i) {
                                self.field(&mut arena, text);
                            } else {
                                scratch.clear();
                                renderer.text(&mut scratch, i).map_err(json_exception)?;
                                self.field(&mut arena, &scratch);
                            }
                        }

                        ends.push(arena.len());
                    }
                }
                None => {
                    for cell in cells(cells_of, &column.name, batch.count, "cells")? {
                        if let Some(text) = cell {
                            self.field(&mut arena, text);
                        }

                        ends.push(arena.len());
                    }
                }
            }

            fields.push((arena, ends));
        }

        let mut out = Vec::with_capacity(
            fields.iter().map(|(arena, _)| arena.len()).sum::<usize>()
                + batch.count * (fields.len() + self.new_line.len()),
        );

        for i in 0..batch.count {
            for (position, (arena, ends)) in fields.iter().enumerate() {
                if position > 0 {
                    out.push(self.separator);
                }

                out.extend_from_slice(&arena[if i == 0 { 0 } else { ends[i - 1] }..ends[i]]);
            }

            out.extend_from_slice(&self.new_line);
        }

        Ok(Binary::new(out))
    }
}
