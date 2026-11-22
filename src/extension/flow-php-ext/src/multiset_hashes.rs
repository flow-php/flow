//! `Flow\ETL\Values\Column\RustGroupedMultisetHashes`: wrapping sums of the row hash lanes, held natively between
//! batches, one sum per group.

use std::sync::Arc;

use arrow_array::LargeBinaryArray;
use ext_php_rs::flags::ClassFlags;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ArrayKey, ZendHashTable, Zval};
use flow_column_kernels::canonical::row_lanes;

use crate::canonical_rows::{named, row_columns, STRING_TYPE};
use crate::column::RustColumn;
use crate::ctx::call_method;
use crate::exception::ext_exception;
use crate::grouped_totals::group_ids;
use crate::interfaces::grouped_multiset_hashes_ce;
use crate::render::invalid_argument;

/// [low, high]
type Lanes = [u64; 2];

#[php_class]
#[php(
    name = "Flow\\ETL\\Values\\Column\\RustGroupedMultisetHashes",
    flags = ClassFlags::Final,
    implements(ce = grouped_multiset_hashes_ce, stub = "Flow\\ETL\\Values\\Column\\GroupedMultisetHashes")
)]
#[derive(Default)]
pub struct RustGroupedMultisetHashes {
    lanes: Vec<Lanes>,
}

#[php_impl]
impl RustGroupedMultisetHashes {
    pub fn add(&mut self, columns: &ZendHashTable, ids: &Zval) -> PhpResult<()> {
        let resolved = row_columns(columns)?;
        let ids = group_ids(ids)?;
        let mut row = Vec::new();
        let mut entries = Vec::new();

        if let Some(&last) = ids.iter().max() {
            self.grow(last);
        }

        for (index, id) in ids.into_iter().enumerate() {
            named(&mut row, &mut entries, &resolved, index);
            add_lanes(&mut self.lanes[id], row_lanes(&row).into());
        }

        Ok(())
    }

    pub fn hashes(&self) -> PhpResult<RustColumn> {
        let hashes = self
            .lanes
            .iter()
            .map(|[low, high]| format!("{high:016x}{low:016x}"))
            .collect::<Vec<_>>();

        RustColumn::of_type(Arc::new(LargeBinaryArray::from_iter_values(hashes)), STRING_TYPE)
    }

    #[php(name = "absorbEarlier")]
    pub fn absorb_earlier(&mut self, earlier: &Zval, into: &ZendHashTable) -> PhpResult<()> {
        let lanes = match earlier.extract::<&RustGroupedMultisetHashes>() {
            Some(native) => native.lanes.clone(),
            None => call_method(&call_method(earlier, "hashes", &mut [])?, "physicals", &mut [])?
                .array()
                .ok_or_else(|| ext_exception("flow_php expected Column::physicals() to return an array"))?
                .values()
                .map(lanes_of)
                .collect::<PhpResult<Vec<_>>>()?,
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
            let earlier = lanes
                .get(earlier_id)
                .copied()
                .ok_or_else(|| invalid_argument(format!("Earlier group {earlier_id} has no hash")))?;

            self.grow(id);
            add_lanes(&mut self.lanes[id], earlier);
        }

        Ok(())
    }
}

impl RustGroupedMultisetHashes {
    fn grow(&mut self, id: usize) {
        if self.lanes.len() <= id {
            self.lanes.resize(id + 1, [0, 0]);
        }
    }
}

fn add_lanes(lanes: &mut Lanes, [low, high]: Lanes) {
    lanes[0] = lanes[0].wrapping_add(low);
    lanes[1] = lanes[1].wrapping_add(high);
}

fn lanes_of(hex: &Zval) -> PhpResult<Lanes> {
    let hex = hex
        .str()
        .ok_or_else(|| ext_exception("flow_php expected a multiset hash as a string"))?;
    let lowercase_hex = hex.len() == 32 && hex.bytes().all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'));
    let digest = u128::from_str_radix(hex, 16)
        .ok()
        .filter(|_| lowercase_hex)
        .ok_or_else(|| {
            invalid_argument(format!(
                "A multiset digest is 32 lowercase hex characters, high lane first, given: \"{hex}\""
            ))
        })?;

    Ok([digest as u64, (digest >> 64) as u64])
}
