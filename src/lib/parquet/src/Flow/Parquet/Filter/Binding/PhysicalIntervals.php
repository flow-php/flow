<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Operator;

use function array_key_last;
use function count;
use function is_float;
use function is_nan;
use function usort;

/**
 * The physical values whose decoded value satisfies a leaf, as a union of intervals.
 */
final readonly class PhysicalIntervals
{
    /**
     * @param list<PhysicalInterval> $intervals sorted by lower bound, disjoint
     */
    public function __construct(
        public array $intervals,
    ) {}

    /**
     * Null when the column's decoded values cannot be compared with the literal exactly.
     */
    public static function preImage(
        PreImage $preImage,
        PhysicalOrder $order,
        Operator $operator,
        bool|DateTimeImmutable|float|int|string $literal,
    ): ?self {
        $equal = $preImage->equal($literal);

        if ($equal === null) {
            return null;
        }

        $interval = match ($operator) {
            Operator::Equal => self::interval($order, $equal->low, true, $equal->high, true),
            Operator::LessThan => self::interval($order, OutOfDomain::Below, false, $equal->low, false),
            Operator::LessThanOrEqual => self::interval($order, OutOfDomain::Below, false, $equal->high, true),
            Operator::GreaterThan => self::interval($order, $equal->high, false, OutOfDomain::Above, false),
            Operator::GreaterThanOrEqual => self::interval($order, $equal->low, true, OutOfDomain::Above, false),
        };

        return new self($interval === null ? [] : [$interval]);
    }

    /**
     * Sorted by lower bound, each once: the pre-images of distinct values are disjoint, of equal values the same.
     *
     * @param list<bool|DateTimeImmutable|float|int|string> $literals
     */
    public static function preImageOfSet(PreImage $preImage, PhysicalOrder $order, array $literals): ?self
    {
        $intervals = [];

        foreach ($literals as $literal) {
            $equal = self::preImage($preImage, $order, Operator::Equal, $literal);

            if ($equal === null) {
                return null;
            }

            foreach ($equal->intervals as $interval) {
                $intervals[] = $interval;
            }
        }

        usort($intervals, static fn(PhysicalInterval $a, PhysicalInterval $b): int => self::compareLower(
            $order,
            $a,
            $b,
        ));
        $distinct = [];

        foreach ($intervals as $interval) {
            $last = array_key_last($distinct);

            if ($last === null || self::compareLower($order, $distinct[$last], $interval) !== 0) {
                $distinct[] = $interval;
            }
        }

        return new self($distinct);
    }

    /**
     * The value lies in an interval: only the last one starting at or below it can hold it. NaN lies in none, -0.0 and
     * +0.0 compare equal.
     */
    public function contains(PhysicalOrder $order, bool|float|int|string $value): bool
    {
        if (is_float($value) && is_nan($value)) {
            return false;
        }

        $low = 0;
        $high = count($this->intervals);

        while ($low < $high) {
            $middle = ($low + $high) >> 1;
            $lower = $this->intervals[$middle]->lower;

            if ($lower === null || self::compare($order, $lower, $value) <= 0) {
                $low = $middle + 1;
            } else {
                $high = $middle;
            }
        }

        if ($low === 0) {
            return false;
        }

        $interval = $this->intervals[$low - 1];

        return (
            (
                $interval->lowerInclusive
                || $interval->lower === null
                || self::compare($order, $interval->lower, $value) < 0
            )
            && (
                $interval->upper === null
                || self::compare($order, $value, $interval->upper) < 0
                || $interval->upperInclusive
                && self::compare($order, $value, $interval->upper) === 0
            )
        );
    }

    /**
     * [$min, $max] lies inside one interval. -0.0 and +0.0 compare equal.
     */
    public function covers(PhysicalOrder $order, bool|float|int|string $min, bool|float|int|string $max): bool
    {
        foreach ($this->intervals as $interval) {
            $lower =
                $interval->lower === null
                || self::compare($order, $interval->lower, $min) < 0
                || $interval->lowerInclusive && self::compare($order, $interval->lower, $min) === 0;
            $upper =
                $interval->upper === null
                || self::compare($order, $max, $interval->upper) < 0
                || $interval->upperInclusive && self::compare($order, $max, $interval->upper) === 0;

            if ($lower && $upper) {
                return true;
            }
        }

        return false;
    }

    /**
     * No interval meets [$min, $max]. -0.0 and +0.0 compare equal.
     */
    public function disjoint(PhysicalOrder $order, bool|float|int|string $min, bool|float|int|string $max): bool
    {
        foreach ($this->intervals as $interval) {
            $below =
                $interval->upper !== null
                && (
                    self::compare($order, $interval->upper, $min) < 0
                    || !$interval->upperInclusive
                    && self::compare($order, $interval->upper, $min) === 0
                );
            $above =
                $interval->lower !== null
                && (
                    self::compare($order, $interval->lower, $max) > 0
                    || !$interval->lowerInclusive
                    && self::compare($order, $interval->lower, $max) === 0
                );

            if (!$below && !$above) {
                return false;
            }
        }

        return true;
    }

    public function isEmpty(): bool
    {
        return $this->intervals === [];
    }

    /**
     * Null when the interval holds no physical value.
     */
    private static function interval(
        PhysicalOrder $order,
        bool|float|int|string|OutOfDomain $lower,
        bool $lowerInclusive,
        bool|float|int|string|OutOfDomain $upper,
        bool $upperInclusive,
    ): ?PhysicalInterval {
        if ($lower === OutOfDomain::Above || $upper === OutOfDomain::Below) {
            return null;
        }

        $lower = $lower === OutOfDomain::Below ? null : $lower;
        $upper = $upper === OutOfDomain::Above ? null : $upper;

        if ($lower !== null && $upper !== null) {
            $comparison = self::compare($order, $lower, $upper);

            if ($comparison > 0 || $comparison === 0 && (!$lowerInclusive || !$upperInclusive)) {
                return null;
            }
        }

        return new PhysicalInterval($lower, $lowerInclusive, $upper, $upperInclusive);
    }

    private static function compareLower(PhysicalOrder $order, PhysicalInterval $a, PhysicalInterval $b): int
    {
        return $a->lower === null || $b->lower === null
            ? ($b->lower === null) <=> ($a->lower === null)
            : self::compare($order, $a->lower, $b->lower);
    }

    private static function compare(PhysicalOrder $order, bool|float|int|string $a, bool|float|int|string $b): int
    {
        if (is_float($a) && is_float($b) && !is_nan($a) && !is_nan($b)) {
            return $a <=> $b;
        }

        return $order->compare($a, $b);
    }
}
