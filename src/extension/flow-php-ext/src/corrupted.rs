//! `RecordBatch`'s outcomes for a record a reader cannot hand out as a row: a `CorruptedRecordException` under a
//! declared schema, an `InferredSchemaException` under an inferred one.

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, Zval};

use crate::builder::throw;
use crate::ctx::{
    call_method, call_static, construct, expect_object, find_class, null_zval, read_property, zval_long, zval_str,
};
use crate::exception::ext_exception;

const COLUMN_MISMATCH: &str = "Flow\\ETL\\Foundation\\Exception\\ColumnMismatchException";
const CORRUPTED_RECORD: &str = "Flow\\ETL\\Foundation\\Exception\\CorruptedRecordException";
const INFERRED_SCHEMA: &str = "Flow\\ETL\\Foundation\\Exception\\InferredSchemaException";
const SCHEMA_MISMATCH: &str = "Flow\\ETL\\Foundation\\Exception\\SchemaMismatchException";

/// A record's `uri, offset, line` as the exceptions take them.
fn located(uri: &Zval, offset: u64, line: Option<u64>) -> [Zval; 3] {
    [
        uri.shallow_clone(),
        zval_long(offset as i64),
        line.map_or_else(null_zval, |line| zval_long(line as i64)),
    ]
}

/// `ColumnMismatchException::missingColumn()` of `definition`.
pub fn missing_column(definition: &Zval) -> Result<Zval, PhpException> {
    call_static(
        COLUMN_MISMATCH,
        "missingColumn",
        &mut [call_method(definition, "name", &mut [])?],
    )
}

/// `ColumnMismatchException::unexpectedColumn()` of `name`.
pub fn unexpected_column(name: &str) -> Result<Zval, PhpException> {
    call_static(COLUMN_MISMATCH, "unexpectedColumn", &mut [zval_str(name.as_bytes())])
}

/// `ColumnMismatchException::reason()`.
pub fn reason_of(cause: &Zval) -> Result<String, PhpException> {
    Ok(call_method(cause, "reason", &mut [])?.string().unwrap_or_default())
}

/// `CorruptedRecordException::fromCells()`: a grid record's fields and its text.
pub fn from_cells(
    uri: &Zval,
    offset: u64,
    line: Option<u64>,
    reason: &str,
    fields: &[&[u8]],
    record: &[u8],
) -> Result<Zval, PhpException> {
    let mut cells = ZendHashTable::new();

    for field in fields {
        cells
            .push(zval_str(field))
            .map_err(|e| ext_exception(format!("flow_php failed to collect a record's fields: {e:?}")))?;
    }

    let mut cells_zv = Zval::new();
    cells_zv.set_hashtable(cells);
    let [uri, offset, line] = located(uri, offset, line);

    call_static(
        CORRUPTED_RECORD,
        "fromCells",
        &mut [
            uri,
            offset,
            line,
            zval_str(reason.as_bytes()),
            cells_zv,
            zval_str(record),
        ],
    )
}

/// `CorruptedRecordException::fromValues()`: a keyed record's values, its text when the source holds one.
pub fn from_values(
    uri: &Zval,
    offset: u64,
    line: Option<u64>,
    reason: &str,
    values: Zval,
    record: Option<&[u8]>,
) -> Result<Zval, PhpException> {
    let [uri, offset, line] = located(uri, offset, line);

    call_static(
        CORRUPTED_RECORD,
        "fromValues",
        &mut [
            uri,
            offset,
            line,
            zval_str(reason.as_bytes()),
            values,
            record.map_or_else(null_zval, zval_str),
        ],
    )
}

/// `InferredSchemaException::outsideTheSample()` for the record at `row` of its batch, refused by `cause`.
pub fn outside_the_sample(
    uri: &Zval,
    offset: u64,
    line: Option<u64>,
    inference: &Zval,
    row: usize,
    cause: Zval,
) -> PhpException {
    let exception = find_class(SCHEMA_MISMATCH)
        .and_then(|ce| construct(ce, &mut [zval_long(row as i64), cause]))
        .and_then(|mut mismatch| {
            let mut mismatch_zv = Zval::new();
            mismatch_zv.set_object(&mut mismatch);
            let [uri, offset, line] = located(uri, offset, line);
            let sample_size = read_property(expect_object(inference, "a SchemaInference")?, "sampleSize")?;

            call_static(
                INFERRED_SCHEMA,
                "outsideTheSample",
                &mut [uri, offset, line, sample_size, mismatch_zv],
            )
        });

    match exception {
        Ok(exception) => throw(exception),
        Err(e) => e,
    }
}
