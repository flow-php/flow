//! `RustCSVReaderNative::nextColumns()`: CSV cells straight into native columns through `BatchColumns`; a NOT NULL
//! column absent from the header is absent from every row.

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;

use crate::batch_columns::{native_text, BatchColumn, BatchColumns};
use crate::builder::{is_types_exception, php_lane, value_does_not_match, Refusal};
use crate::csv::CsvReader;
use crate::ctx::{null_zval, transparent_exception, zval_str};
use crate::physical::append_physical;

/// The batch being built, and the field of the header each of its columns reads.
#[derive(Default)]
pub struct CsvColumns {
    batch: Option<BatchColumns>,
    /// `None`: the header has no column of this name.
    fields: Vec<Option<usize>>,
}

/// One cell into its column: a NOT NULL null or a PHP-lane `Flow\Types` refusal becomes the column's refusal, any
/// other exception escapes as thrown.
fn append_cell(column: &mut BatchColumn, row: usize, cell: Option<&[u8]>) -> Result<(), PhpException> {
    let Some(bytes) = cell else {
        if !column.nullable {
            column.refusal = Some((row, value_does_not_match(&column.definition, null_zval(), None)?));

            return Ok(());
        }

        column.values.append_null();

        return Ok(());
    };

    if native_text(&mut column.values, &column.plan.kind, &column.plan.cast, bytes)? {
        return Ok(());
    }

    let value = zval_str(bytes);

    match php_lane(&column.plan, &value) {
        Ok(physical) => append_physical(&mut column.values, &column.plan.kind, &physical),
        Err(Refusal::Cast(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            column.refusal = Some((row, value_does_not_match(&column.definition, value, None)?));

            Ok(())
        }
        Err(Refusal::Physical(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            let mut reason = Zval::new();
            reason.set_object(&mut exception);
            column.refusal = Some((row, value_does_not_match(&column.definition, value, Some(reason))?));

            Ok(())
        }
    }
}

pub fn next_columns(
    reader: &mut CsvReader,
    pending: &mut CsvColumns,
    schema: &Zval,
    batch_size: usize,
) -> Result<Zval, PhpException> {
    if reader.headers().is_empty() {
        return Ok(null_zval());
    }

    let (state, rebuilt) = BatchColumns::prepare(&mut pending.batch, schema, batch_size, "CSV")?;

    if rebuilt {
        pending.fields = state.columns.iter().map(|column| reader.field_of(&column.key.name())).collect();
    }

    while state.rows < state.batch_size && reader.next_record() {
        let row = state.rows;

        for (column, field) in state.columns.iter_mut().zip(&pending.fields) {
            if column.refusal.is_some() {
                continue;
            }

            let Some(field) = field else {
                if !column.nullable {
                    column.absent = column.absent.or(Some(row));
                }

                column.values.append_null();

                continue;
            };

            if let Err(e) = append_cell(column, row, reader.cell(*field)) {
                state.reset();

                return Err(e);
            }
        }

        state.rows += 1;
    }

    if state.rows == 0 || (state.rows < state.batch_size && !reader.is_finished()) {
        return Ok(null_zval());
    }

    state.finish()
}
