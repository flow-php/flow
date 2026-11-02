//! `Flow\ETL\Values\Column\RustGroupedCounts`: per-group non-null counts held natively between batches.

use std::sync::Arc;

use arrow_array::Int64Array;
use ext_php_rs::boxed::ZBox;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};

use crate::column::RustColumn;
use crate::ctx::{call_method, zval_long};
use crate::exception::ext_exception;
use crate::grouped_totals::group_ids;
use crate::interfaces::grouped_counts_ce;
use crate::render::invalid_argument;

const INTEGER_TYPE: &str = "Flow\\Types\\Type\\Native\\IntegerType";

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Column\\RustGroupedCounts",
    flags = ClassFlags::Final,
    implements(ce = grouped_counts_ce, stub = "Flow\\ETL\\Values\\Column\\GroupedCounts")
)]
#[derive(Default)]
pub struct RustGroupedCounts {
    counts: Vec<i64>,
}

#[php_impl]
impl RustGroupedCounts {
    pub fn add(&mut self, values: &Zval, ids: &Zval) -> PhpResult<()> {
        let ids = group_ids(ids)?;

        if let Some(&last) = ids.iter().max() {
            if self.counts.len() <= last {
                self.counts.resize(last + 1, 0);
            }
        }

        match values.extract::<&RustColumn>() {
            Some(native) => {
                for (row, id) in ids.iter().enumerate() {
                    self.counts[*id] += i64::from(!native.is_null_at(row));
                }
            }
            None if call_method(values, "nullCount", &mut [])?.long() == Some(0) => {
                for id in &ids {
                    self.counts[*id] += 1;
                }
            }
            // a PHP column decides what null means for its kind, so each row asks it
            None => {
                for (row, id) in ids.iter().enumerate() {
                    let null = call_method(values, "isNull", &mut [zval_long(row as i64)])?;
                    self.counts[*id] += i64::from(!null.bool().unwrap_or(false));
                }
            }
        }

        Ok(())
    }

    pub fn counts(&self) -> PhpResult<RustColumn> {
        RustColumn::of_type(Arc::new(Int64Array::from(self.counts.clone())), INTEGER_TYPE)
    }

    #[php(name = "absorbEarlier")]
    pub fn absorb_earlier(&mut self, earlier: &Zval, into: &ZendHashTable) -> PhpResult<()> {
        let counts = match earlier.extract::<&RustGroupedCounts>() {
            Some(native) => native.counts.clone(),
            None => call_method(earlier, "partials", &mut [])?
                .array()
                .ok_or_else(|| ext_exception("flow_php expected GroupedCounts::partials() to return an array"))?
                .values()
                .map(|count| count.long().unwrap_or(0))
                .collect(),
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

            if self.counts.len() <= id {
                self.counts.resize(id + 1, 0);
            }

            self.counts[id] += counts
                .get(earlier_id)
                .copied()
                .ok_or_else(|| invalid_argument(format!("Earlier group {earlier_id} has no count")))?;
        }

        Ok(())
    }

    pub fn partials(&self) -> PhpResult<ZBox<ZendHashTable>> {
        let mut partials = ZendHashTable::with_capacity(self.counts.len() as u32);

        for count in &self.counts {
            partials.push(zval_long(*count))?;
        }

        Ok(partials)
    }
}
