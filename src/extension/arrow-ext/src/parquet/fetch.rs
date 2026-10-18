//! What a planned read asks of the stream: the byte ranges arrow-rs's push decoder needs, adjacent ones merged into one
//! read.

use std::ops::Range;

/// Sorted ranges with every run of adjacent or overlapping ones merged.
pub fn coalesced(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());

    for range in ranges {
        match merged.last_mut() {
            Some(last) if last.end >= range.start => last.end = last.end.max(range.end),
            _ => merged.push(range),
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::coalesced;

    #[test]
    fn adjacent_ranges_are_coalesced() {
        assert_eq!(coalesced(vec![10..20, 0..10, 30..40, 20..25]), vec![0..25, 30..40]);
        assert_eq!(coalesced(vec![0..10, 5..8, 8..12]), vec![0..12]);
        assert!(coalesced(vec![]).is_empty());
    }
}
