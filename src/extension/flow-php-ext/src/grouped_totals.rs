//! `Flow\ETL\Values\Column\RustGroupedTotals`: per-group running totals held natively between batches.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::Float64Array;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};
use flow_column_kernels::totals::{GroupTotals, Sum};
use flow_numeric_string::{self as numeric, Numeric};

use crate::column::RustColumn;
use crate::ctx::{call_method, ht_get, ht_insert, zval_long};
use crate::exception::ext_exception;
use crate::interfaces::grouped_totals_ce;
use crate::render::invalid_argument;

const FLOAT_TYPE: &str = "Flow\\Types\\Type\\Native\\FloatType";

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Column\\RustGroupedTotals",
    flags = ClassFlags::Final,
    implements(ce = grouped_totals_ce, stub = "Flow\\ETL\\Values\\Column\\GroupedTotals")
)]
#[derive(Default)]
pub struct RustGroupedTotals {
    totals: GroupTotals,
}

#[php_impl]
impl RustGroupedTotals {
    pub fn add(&mut self, values: &Zval, ids: &Zval) -> PhpResult<()> {
        let ids = group_ids(ids)?;
        let native = values.extract::<&RustColumn>();

        match native.map(|column| (column.plan().type_name.as_str(), column.data())) {
            Some(("integer", data)) => {
                for (row, value) in data.as_primitive::<Int64Type>().iter().enumerate() {
                    match value {
                        Some(value) => self.totals.add_int(ids[row], value),
                        None => self.totals.grow(ids[row]),
                    }
                }
            }
            Some(("float", data)) => {
                for (row, value) in data.as_primitive::<Float64Type>().iter().enumerate() {
                    match value {
                        Some(value) => self.totals.add_float(ids[row], value),
                        None => self.totals.grow(ids[row]),
                    }
                }
            }
            _ => self.add_values(&call_method(values, "values", &mut [])?, &ids)?,
        }

        Ok(())
    }

    pub fn sums(&self) -> PhpResult<RustColumn> {
        let sums = (0..self.totals.counts.len())
            .map(|id| self.totals.sum(id))
            .collect::<Float64Array>();

        RustColumn::of_type(Arc::new(sums), FLOAT_TYPE)
    }

    #[php(name = "absorbEarlier")]
    pub fn absorb_earlier(&mut self, earlier: &Zval, into: &ZendHashTable) -> PhpResult<()> {
        let states = match earlier.extract::<&RustGroupedTotals>() {
            Some(native) => (0..native.totals.counts.len())
                .map(|id| {
                    (
                        native.totals.sums[id],
                        native.totals.compensations[id],
                        native.totals.counts[id],
                    )
                })
                .collect::<Vec<_>>(),
            None => partial_states(&call_method(earlier, "partials", &mut [])?)?,
        };

        for (earlier_id, id) in into.iter() {
            let earlier_id = match earlier_id {
                ArrayKey::Long(id) => usize::try_from(id).ok(),
                _ => None,
            }
            .ok_or_else(|| ext_exception("flow_php expected earlier group ids as int keys"))?;
            let id = id
                .long()
                .and_then(|id| usize::try_from(id).ok())
                .ok_or_else(|| ext_exception("flow_php expected group ids as ints of 0 or more"))?;
            let state = *states
                .get(earlier_id)
                .ok_or_else(|| invalid_argument(format!("Earlier group {earlier_id} has no totals")))?;

            self.totals.absorb(id, state);
        }

        Ok(())
    }

    pub fn partials(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let mut partials = ZendHashTable::with_capacity(self.totals.counts.len() as u32);

        for id in 0..self.totals.counts.len() {
            let mut sum = ZendHashTable::with_capacity(3);
            let (int, float) = match self.totals.sums[id] {
                Sum::Int(int) => (zval_long(int), Zval::null()),
                Sum::Float(float) => (Zval::null(), Zval::from(float)),
            };
            ht_insert(&mut sum, b"int", int);
            ht_insert(&mut sum, b"float", float);
            ht_insert(&mut sum, b"compensation", Zval::from(self.totals.compensations[id]));

            let mut partial = ZendHashTable::with_capacity(2);
            ht_insert(&mut partial, b"count", zval_long(self.totals.counts[id]));
            let mut sum_zv = Zval::new();
            sum_zv.set_hashtable(sum);
            ht_insert(&mut partial, b"sum", sum_zv);

            let mut partial_zv = Zval::new();
            partial_zv.set_hashtable(partial);
            partials.push(partial_zv)?;
        }

        Ok(partials)
    }
}

impl RustGroupedTotals {
    // values a column holds as PHP values: ints, floats and numeric strings count, as NumericTotal counts them
    fn add_values(&mut self, values: &Zval, ids: &[usize]) -> PhpResult<()> {
        let values = values
            .array()
            .ok_or_else(|| ext_exception("flow_php expected Column::values() to return an array"))?;

        for (row, value) in values.values().enumerate() {
            let id = ids[row];

            if let Some(int) = value.long() {
                self.totals.add_int(id, int);
            } else if let Some(float) = value.double() {
                self.totals.add_float(id, float);
            } else if let Some(number) = value.zend_str().and_then(|text| numeric::parse(text.as_bytes())) {
                self.totals.add_float(
                    id,
                    match number {
                        Numeric::Long(int) => int as f64,
                        Numeric::Double(float) => float,
                    },
                );
            } else {
                self.totals.grow(id);
            }
        }

        Ok(())
    }
}

fn partial_states(partials: &Zval) -> PhpResult<Vec<(Sum, f64, i64)>> {
    let malformed = || ext_exception("flow_php expected GroupedTotals::partials() in NumericTotal::partial()'s shape");

    partials
        .array()
        .ok_or_else(malformed)?
        .values()
        .map(|partial| {
            let partial = partial.array().ok_or_else(malformed)?;
            let sum = ht_get(partial, b"sum").and_then(Zval::array).ok_or_else(malformed)?;
            let total = match (
                ht_get(sum, b"int").and_then(Zval::long),
                ht_get(sum, b"float").and_then(Zval::double),
            ) {
                (Some(int), _) => Sum::Int(int),
                (None, Some(float)) => Sum::Float(float),
                (None, None) => return Err(malformed()),
            };

            Ok((
                total,
                ht_get(sum, b"compensation")
                    .and_then(Zval::double)
                    .ok_or_else(malformed)?,
                ht_get(partial, b"count").and_then(Zval::long).ok_or_else(malformed)?,
            ))
        })
        .collect()
}

pub fn group_ids(ids: &Zval) -> PhpResult<Vec<usize>> {
    if let Some(native) = ids.extract::<&RustColumn>() {
        if let Some(ints) = native.data().as_primitive_opt::<Int64Type>() {
            return ints
                .values()
                .iter()
                .map(|id| {
                    usize::try_from(*id).map_err(|_| ext_exception("flow_php group ids must be ints of 0 or more"))
                })
                .collect();
        }
    }

    let physicals = call_method(ids, "physicals", &mut [])?;

    physicals
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Column::physicals() to return an array"))?
        .values()
        .map(|id| {
            id.long()
                .and_then(|id| usize::try_from(id).ok())
                .ok_or_else(|| ext_exception("flow_php group ids must be ints of 0 or more"))
        })
        .collect()
}
