//! `Flow\ETL\Adapter\JSON\RustJsonEncoder`: a `Rows` batch as the text `PhpJsonEncoder::encode()` returns, rendered
//! from the arrow arrays; the columns it does not render are the fragments the held `PhpJsonEncoder::fragments()`
//! produces.

use ext_php_rs::binary::Binary;
use ext_php_rs::binary_slice::BinarySlice;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::interfaces::json_encoder_ce;
use crate::render::{invalid_argument, runtime};
use crate::text::batch::{batch, cells, renderer, Unrendered};
use crate::text::json::{self, Flags};
use crate::text::value::{Formats, Renderer};
use crate::text::Error;

const JSON_UNESCAPED_SLASHES: i64 = 64;
const JSON_UNESCAPED_UNICODE: i64 = 256;
const JSON_PRESERVE_ZERO_FRACTION: i64 = 1024;
const JSON_THROW_ON_ERROR: i64 = 4_194_304;

fn refused(error: Error) -> PhpException {
    runtime(format!("Failed to encode JSON: {}", error.message()))
}

enum Cells<'a> {
    Native(Renderer<'a>),
    Fragments(Vec<Option<&'a [u8]>>),
}

#[php_class]
#[php(
    name = "Flow\\ETL\\Adapter\\JSON\\RustJsonEncoder",
    flags = ClassFlags::Final,
    implements(ce = json_encoder_ce, stub = "Flow\\ETL\\Adapter\\JSON\\JsonEncoder")
)]
pub struct RustJsonEncoder {
    flags: Flags,
    formats: Formats,
    /// The `PhpJsonEncoder` that renders the unrendered columns.
    php: Zval,
    unrendered: Unrendered,
}

#[php_impl]
impl RustJsonEncoder {
    pub fn __construct(
        flags: i64,
        date_time_format: BinarySlice<u8>,
        date_format: BinarySlice<u8>,
        php: &Zval,
    ) -> PhpResult<Self> {
        if flags
            & !(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION)
            != 0
        {
            return Err(invalid_argument(
                "flow_php JSON writer supports only the JSON_THROW_ON_ERROR, JSON_UNESCAPED_SLASHES, JSON_UNESCAPED_UNICODE and JSON_PRESERVE_ZERO_FRACTION flags"
                    .to_string(),
            ));
        }

        Ok(Self {
            flags: Flags {
                unescaped_slashes: flags & JSON_UNESCAPED_SLASHES != 0,
                unescaped_unicode: flags & JSON_UNESCAPED_UNICODE != 0,
                preserve_zero_fraction: flags & JSON_PRESERVE_ZERO_FRACTION != 0,
            },
            formats: Formats::parse(&date_time_format, &date_format),
            php: php.shallow_clone(),
            unrendered: Unrendered::default(),
        })
    }

    /// One object per row, joined by `$separator`; the unrendered columns through the held encoder's `fragments()`.
    pub fn encode(&mut self, rows: &Zval, separator: BinarySlice<u8>) -> PhpResult<Binary<u8>> {
        let fragments = self
            .unrendered
            .render(rows, &self.php, "fragments", &self.formats, false)?;

        self.render_rows(rows, &fragments, &separator)
    }
}

impl RustJsonEncoder {
    /// `$fragments`: `PhpJsonEncoder::fragments()` of the unrendered columns.
    fn render_rows(&self, rows: &Zval, fragments: &ZendHashTable, separator: &[u8]) -> PhpResult<Binary<u8>> {
        let batch = batch(rows, &self.formats, false)?;
        let mut keys = Vec::with_capacity(batch.columns.len());
        let mut columns = Vec::with_capacity(batch.columns.len());

        for (position, column) in batch.columns.iter().enumerate() {
            let mut key = vec![if position == 0 { b'{' } else { b',' }];

            json::string(&mut key, &column.name, &self.flags).map_err(refused)?;
            key.push(b':');
            keys.push(key);
            columns.push(match &column.native {
                Some((plan, data)) => Cells::Native(renderer(plan, data, &self.formats)?),
                None => Cells::Fragments(cells(fragments, &column.name, batch.count, "JSON fragments")?),
            });
        }

        let mut out = Vec::with_capacity(batch.count * 32 * columns.len().max(1));

        for i in 0..batch.count {
            let mut non_finite = false;

            if i > 0 {
                out.extend_from_slice(separator);
            }

            if columns.is_empty() {
                out.push(b'{');
            }

            for (key, column) in keys.iter().zip(&columns) {
                out.extend_from_slice(key);

                match column {
                    Cells::Native(renderer) => renderer
                        .json(&mut out, i, &self.flags, &mut non_finite)
                        .map_err(refused)?,
                    Cells::Fragments(fragments) => out.extend_from_slice(fragments[i].ok_or_else(|| {
                        invalid_argument("flow_php expected a JSON fragment of every row, got null".to_string())
                    })?),
                }
            }

            out.push(b'}');

            if non_finite {
                return Err(refused(Error::NonFinite));
            }
        }

        Ok(Binary::new(out))
    }
}
