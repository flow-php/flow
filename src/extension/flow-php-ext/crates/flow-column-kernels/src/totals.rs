//! Running totals per group, added in row order as Flow's RunningSum adds them one group at a time.

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Sum {
    Int(i64),
    Float(f64),
}

#[derive(Debug, Default)]
pub struct GroupTotals {
    pub sums: Vec<Sum>,
    pub compensations: Vec<f64>,
    pub counts: Vec<i64>,
}

impl GroupTotals {
    pub fn grow(&mut self, id: usize) {
        if id >= self.counts.len() {
            self.sums.resize(id + 1, Sum::Int(0));
            self.compensations.resize(id + 1, 0.0);
            self.counts.resize(id + 1, 0);
        }
    }

    pub fn add_int(&mut self, id: usize, value: i64) {
        self.grow(id);
        self.counts[id] += 1;
        self.accumulate(id, Sum::Int(value));
    }

    pub fn add_float(&mut self, id: usize, value: f64) {
        self.grow(id);
        self.counts[id] += 1;
        self.accumulate(id, Sum::Float(value));
    }

    /// NumericTotal::merge as earlier.merge(this): group `into` becomes `earlier`'s state followed by its own.
    pub fn absorb(&mut self, into: usize, earlier: (Sum, f64, i64)) {
        self.grow(into);
        let (sum, compensation, count) = (self.sums[into], self.compensations[into], self.counts[into]);
        (self.sums[into], self.compensations[into], self.counts[into]) = earlier;

        if count > 0 {
            self.accumulate(into, sum);
            self.compensations[into] += compensation;
            self.counts[into] += count;
        }
    }

    fn accumulate(&mut self, id: usize, value: Sum) {
        match (self.sums[id], value) {
            // an int overflow promotes to float, as PHP's + does, without a compensation step
            (Sum::Int(sum), Sum::Int(value)) => {
                self.sums[id] = sum
                    .checked_add(value)
                    .map_or_else(|| Sum::Float(sum as f64 + value as f64), Sum::Int);
            }
            (sum, value) => self.add_compensated(id, float(sum), float(value)),
        }
    }

    pub fn sum(&self, id: usize) -> Option<f64> {
        if self.counts[id] == 0 {
            return None;
        }

        Some(match self.sums[id] {
            Sum::Float(sum) if sum.is_finite() => sum + self.compensations[id],
            Sum::Float(sum) => sum,
            Sum::Int(sum) => sum as f64,
        })
    }

    fn add_compensated(&mut self, id: usize, sum: f64, value: f64) {
        let total = sum + value;

        if total.is_finite() {
            self.compensations[id] += if sum.abs() >= value.abs() {
                sum - total + value
            } else {
                value - total + sum
            };
        }

        self.sums[id] = Sum::Float(total);
    }
}

fn float(sum: Sum) -> f64 {
    match sum {
        Sum::Int(int) => int as f64,
        Sum::Float(float) => float,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ints_stay_ints_until_they_overflow() {
        let mut totals = GroupTotals::default();
        totals.add_int(0, i64::MAX);
        totals.add_int(1, 2);
        totals.add_int(1, 3);
        totals.add_int(0, 1);

        assert_eq!(totals.sums, [Sum::Float(i64::MAX as f64 + 1.0), Sum::Int(5)]);
        assert_eq!(totals.compensations, [0.0, 0.0]);
        assert_eq!(totals.counts, [2, 2]);
    }

    #[test]
    fn floats_carry_a_compensation_per_group() {
        let mut totals = GroupTotals::default();

        for _ in 0..10 {
            totals.add_float(0, 0.1);
        }

        totals.add_float(1, 1e16);
        totals.add_float(1, 1.0);

        assert_eq!(totals.sum(0), Some(1.0));
        assert_eq!(totals.sum(1), Some(1e16 + 1.0));
    }

    #[test]
    fn a_group_without_values_has_no_sum() {
        let mut totals = GroupTotals::default();
        totals.grow(2);
        totals.add_int(1, 4);

        assert_eq!([totals.sum(0), totals.sum(1), totals.sum(2)], [None, Some(4.0), None]);
    }

    #[test]
    fn absorbing_puts_the_earlier_state_in_front() {
        let mut totals = GroupTotals::default();
        totals.add_int(0, 3);
        totals.add_float(1, 0.25);
        totals.absorb(0, (Sum::Int(i64::MAX), 0.0, 2));
        totals.absorb(1, (Sum::Float(1e16), 0.5, 1));
        totals.absorb(3, (Sum::Int(7), 0.0, 1));

        assert_eq!(totals.sums[0], Sum::Float(i64::MAX as f64 + 3.0));
        assert_eq!(totals.counts, [3, 2, 0, 1]);
        assert_eq!(totals.sums[3], Sum::Int(7));
        assert_eq!(totals.sum(1), Some(1e16 + 0.25 + 0.5));
    }

    #[test]
    fn a_group_without_values_takes_the_earlier_state() {
        let mut totals = GroupTotals::default();
        totals.grow(0);
        totals.absorb(0, (Sum::Float(2.5), 0.125, 4));

        assert_eq!(
            (totals.sums[0], totals.compensations[0], totals.counts[0]),
            (Sum::Float(2.5), 0.125, 4)
        );
    }

    #[test]
    fn a_non_finite_sum_is_not_compensated() {
        let mut totals = GroupTotals::default();
        totals.add_float(0, f64::INFINITY);
        totals.add_float(0, 1.0);

        assert_eq!(totals.sum(0), Some(f64::INFINITY));
        assert_eq!(totals.compensations, [0.0]);
    }
}
