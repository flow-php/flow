<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Operator;

use function is_float;

/**
 * The physical values whose decoded value satisfies a leaf, as a union of intervals.
 */
final readonly class PhysicalIntervals
{
    /**
     * @param list<PhysicalInterval> $intervals
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

        return new self($intervals);
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

    private static function compare(PhysicalOrder $order, bool|float|int|string $a, bool|float|int|string $b): int
    {
        if (is_float($a) && is_float($b) && $order->isZero($a) && $order->isZero($b)) {
            return 0;
        }

        return $order->compare($a, $b);
    }
}
