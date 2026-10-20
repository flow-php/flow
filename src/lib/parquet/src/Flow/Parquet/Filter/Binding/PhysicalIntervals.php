<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Operator;

use function array_key_exists;
use function array_key_last;
use function count;
use function is_float;
use function is_nan;
use function usort;

/**
 * The physical values whose decoded value satisfies a leaf, as a union of intervals. Floats compare as IEEE 754 does
 * outside NaN: -0.0 equals +0.0.
 */
final readonly class PhysicalIntervals
{
    /**
     * @var list<array{null|int|string, null|int|string}> each interval's lower and upper bound key
     */
    private array $keys;

    /**
     * @var null|array<int|string, true> the key of each interval's only value, when every interval holds one
     */
    private ?array $points;

    /**
     * @param list<PhysicalInterval> $intervals sorted by lower bound, disjoint
     */
    public function __construct(
        public PhysicalOrder $order,
        public array $intervals,
    ) {
        $keys = [];
        $points = [];

        foreach ($intervals as $interval) {
            $lower = $interval->lower === null ? null : self::key($order, $interval->lower);
            $upper = $interval->upper === null ? null : self::key($order, $interval->upper);
            $keys[] = [$lower, $upper];

            if (
                $points !== null
                && $lower !== null
                && $lower === $upper
                && $interval->lowerInclusive
                && $interval->upperInclusive
            ) {
                $points[$lower] = true;
            } else {
                $points = null;
            }
        }

        $this->keys = $keys;
        $this->points = $points;
    }

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

        return new self($order, $interval === null ? [] : [$interval]);
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

        return new self($order, $distinct);
    }

    /**
     * NaN lies in no interval.
     */
    public function contains(bool|float|int|string $value): bool
    {
        if (is_float($value) && is_nan($value)) {
            return false;
        }

        $key = self::key($this->order, $value);

        if ($this->points !== null) {
            return array_key_exists($key, $this->points);
        }

        return $this->covering($key, $key);
    }

    /**
     * [$min, $max] lies inside one interval.
     */
    public function covers(bool|float|int|string $min, bool|float|int|string $max): bool
    {
        return $this->covering(self::key($this->order, $min), self::key($this->order, $max));
    }

    /**
     * No interval meets [$min, $max]: the intervals are disjoint and sorted, so only those starting at or below $max
     * can, the last of them first.
     */
    public function disjoint(bool|float|int|string $min, bool|float|int|string $max): bool
    {
        $minKey = self::key($this->order, $min);
        $maxKey = self::key($this->order, $max);

        for ($index = $this->startingAtOrBelow($maxKey) - 1; $index >= 0; $index--) {
            [$lower, $upper] = $this->keys[$index];
            $interval = $this->intervals[$index];
            $upperToMin = $upper === null ? 1 : $this->order->compareKeys($upper, $minKey);
            $lowerToMax = $lower === null ? -1 : $this->order->compareKeys($lower, $maxKey);

            if ($upperToMin < 0 || $upperToMin === 0 && !$interval->upperInclusive) {
                return true;
            }

            if ($lowerToMax < 0 || $lowerToMax === 0 && $interval->lowerInclusive) {
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
            $comparison = $order->compareKeys(self::key($order, $lower), self::key($order, $upper));

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
            : $order->compareKeys(self::key($order, $a->lower), self::key($order, $b->lower));
    }

    /**
     * -0.0 keyed as +0.0, so the physical order's keys compare floats as IEEE 754 does outside NaN.
     */
    private static function key(PhysicalOrder $order, bool|float|int|string $value): int|string
    {
        return $order->key(is_float($value) && $value === 0.0 ? 0.0 : $value);
    }

    /**
     * Only the last interval starting at or below $minKey can hold it, or the one before when that one starts at
     * $minKey exclusively.
     */
    private function covering(int|string $minKey, int|string $maxKey): bool
    {
        for ($index = $this->startingAtOrBelow($minKey) - 1; $index >= 0; $index--) {
            if ($this->inside($index, $minKey, $maxKey)) {
                return true;
            }

            $lower = $this->keys[$index][0];

            if ($lower === null || $this->order->compareKeys($lower, $minKey) < 0) {
                return false;
            }
        }

        return false;
    }

    /**
     * How many intervals start at or below $key.
     */
    private function startingAtOrBelow(int|string $key): int
    {
        $low = 0;
        $high = count($this->keys);

        while ($low < $high) {
            $middle = ($low + $high) >> 1;
            $lower = $this->keys[$middle][0];

            if ($lower === null || $this->order->compareKeys($lower, $key) <= 0) {
                $low = $middle + 1;
            } else {
                $high = $middle;
            }
        }

        return $low;
    }

    private function inside(int $index, int|string $minKey, int|string $maxKey): bool
    {
        [$lower, $upper] = $this->keys[$index];
        $interval = $this->intervals[$index];
        $lowerToMin = $lower === null ? -1 : $this->order->compareKeys($lower, $minKey);
        $maxToUpper = $upper === null ? -1 : $this->order->compareKeys($maxKey, $upper);

        return (
            ($lowerToMin < 0 || $lowerToMin === 0 && $interval->lowerInclusive)
            && ($maxToUpper < 0 || $maxToUpper === 0 && $interval->upperInclusive)
        );
    }
}
