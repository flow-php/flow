//! `RustCSVOpenSource::batches()`: CSV cells straight into native columns through `BatchColumns`, with
//! `RecordBatch`'s outcomes: a record whose field count differs from the header, and under a declared schema a record
//! with a refused cell, takes its place in the batch and is handed out before its rows, in source order; under an
//! inferred schema a refused cell is a schema error.

use std::collections::VecDeque;

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::Zval;

use crate::batch_columns::{native_text, BatchColumn, BatchColumns, HeldBatch};
use crate::builder::{is_types_exception, php_lane, value_does_not_match, Refusal};
use crate::corrupted::{from_cells, missing_column, outside_the_sample, reason_of};
use crate::csv::CsvReader;
use crate::ctx::{null_zval, transparent_exception, zval_str};
use crate::physical::append_physical;
use crate::source::Fetched;

/// The batch being built, the field of the header each of its columns reads, and what it hands out before it.
#[derive(Default)]
pub struct CsvColumns {
    batch: Option<BatchColumns>,
    /// `None`: the header has no column of this name.
    fields: Vec<Option<usize>>,
    /// The records of the batch being built that were refused, in source order.
    refused: Vec<Zval>,
    /// A finished batch's refused records, then the batch itself.
    ready: VecDeque<Zval>,
    held: Option<HeldBatch>,
}

/// One cell into its column; a non-`Flow\Types` PHP-lane exception escapes as thrown.
fn append_cell(column: &mut BatchColumn, cell: Option<&[u8]>) -> Result<Option<Zval>, PhpException> {
    let Some(bytes) = cell else {
        if !column.nullable {
            return value_does_not_match(&column.definition, null_zval(), None).map(Some);
        }

        column.values.append_null();

        return Ok(None);
    };

    if native_text(&mut column.values, &column.plan.kind, &column.plan.cast, bytes)? {
        return Ok(None);
    }

    let value = zval_str(bytes);

    match php_lane(&column.plan, &value) {
        Ok(physical) => append_physical(&mut column.values, &column.plan.kind, &physical).map(|()| None),
        Err(Refusal::Cast(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            value_does_not_match(&column.definition, value, None).map(Some)
        }
        Err(Refusal::Physical(mut exception)) => {
            if !is_types_exception(&exception)? {
                return Err(transparent_exception(&mut exception));
            }

            let mut reason = Zval::new();
            reason.set_object(&mut exception);

            value_does_not_match(&column.definition, value, Some(reason)).map(Some)
        }
    }
}

/// `CorruptedRecordException::fromCells()` of the reader's current record.
fn corrupted(reader: &mut CsvReader, uri: &Zval, reason: &str) -> Result<Zval, PhpException> {
    from_cells(
        uri,
        reader.record_offset(),
        reader.record_line(),
        reason,
        &reader.record_fields(),
        reader.record_text(),
    )
}

/// The next batch or corrupted record of `reader`, none while more input is needed.
pub fn next_columns(
    reader: &mut CsvReader,
    pending: &mut CsvColumns,
    schema: &Zval,
    batch_size: usize,
    uri: &Zval,
    inference: Option<&Zval>,
) -> Result<Option<Fetched>, PhpException> {
    if let Some(record) = pending.ready.pop_front() {
        return Ok(Some(Fetched::Corrupted(record)));
    }

    if let Some(batch) = pending.held.take() {
        return Ok(Some(Fetched::Batch(batch)));
    }

    if reader.headers().is_empty() {
        return Ok(None);
    }

    let (state, rebuilt) = BatchColumns::prepare(&mut pending.batch, schema, batch_size, "CSV")?;

    if rebuilt {
        pending.fields = state
            .columns
            .iter()
            .map(|column| reader.field_of(&column.key.name()))
            .collect();
    }

    // a refused record fills its place in the batch, as RecordBatch counts it
    while state.rows + pending.refused.len() < state.batch_size {
        match reader.next_record() {
            None => break,
            Some(Err(reason)) => {
                pending.refused.push(corrupted(reader, uri, &reason)?);

                continue;
            }
            Some(Ok(())) => {}
        }

        let row = state.rows;
        let mut absence = None;
        let mut refusal = None;

        for (column, field) in state.columns.iter_mut().zip(&pending.fields) {
            let Some(field) = field else {
                if !column.nullable && absence.is_none() {
                    absence = Some(missing_column(&column.definition)?);
                }

                column.values.append_null();

                continue;
            };

            match append_cell(column, reader.cell(*field)) {
                Ok(None) => {}
                Ok(Some(cause)) => {
                    refusal = Some(cause);

                    break;
                }
                Err(e) => {
                    state.reset();

                    return Err(e);
                }
            }
        }

        // a refused value wins over an absence, as RowsBuilder::appendRows() decides
        let Some(cause) = refusal.or(absence) else {
            state.rows += 1;

            continue;
        };

        for column in &mut state.columns {
            column.values.truncate(row);
        }

        if let Some(inference) = inference {
            state.reset();
            pending.refused.clear();

            return Err(outside_the_sample(
                uri,
                reader.record_offset(),
                reader.record_line(),
                inference,
                row,
                cause,
            ));
        }

        let reason = reason_of(&cause)?;
        pending.refused.push(corrupted(reader, uri, &reason)?);
    }

    if state.rows + pending.refused.len() < state.batch_size && !reader.is_finished() {
        return Ok(None);
    }

    pending.ready.extend(pending.refused.drain(..));

    if state.rows > 0 {
        pending.held = Some(state.finish()?);
    }

    if let Some(record) = pending.ready.pop_front() {
        return Ok(Some(Fetched::Corrupted(record)));
    }

    Ok(pending.held.take().map(Fetched::Batch))
}
