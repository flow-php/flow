<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Row\SortOrder;

use function arsort;
use function asort;
use function is_array;
use function is_numeric;
use function is_string;
use function uasort;

use const SORT_NUMERIC;
use const SORT_STRING;

/**
 * Sorts a column of extracted values preserving keys. Homogeneous columns are detected once and
 * sorted by the engine without any userland comparator - a per-pair comparison callback is orders
 * of magnitude slower than asort()/arsort(). Mixed columns fall back to the pairwise comparator.
 */
final readonly class ValuesSorter
{
    /**
     * @param array<array-key, mixed> $values
     *
     * @return array<array-key, mixed> the same key => value pairs, sorted, keys preserved
     */
    public static function sort(array $values, SortOrder $order): array
    {
        $allNumeric = true;
        $allStrings = true;
        $allDateTimes = true;

        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $value) {
            if ($allNumeric && !is_numeric($value)) {
                $allNumeric = false;
            }

            if ($allStrings && !is_string($value)) {
                $allStrings = false;
            }

            if ($allDateTimes && !$value instanceof DateTimeInterface) {
                $allDateTimes = false;
            }

            if (!$allNumeric && !$allStrings && !$allDateTimes) {
                break;
            }
        }

        if ($allNumeric) {
            $order === SortOrder::ASC ? asort($values, SORT_NUMERIC) : arsort($values, SORT_NUMERIC);

            return $values;
        }

        if ($allStrings) {
            $order === SortOrder::ASC ? asort($values, SORT_STRING) : arsort($values, SORT_STRING);

            return $values;
        }

        if ($allDateTimes) {
            $keys = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($values as $index => $value) {
                if ($value instanceof DateTimeInterface) {
                    // integer microsecond timestamp - exact chronological order, no float precision loss
                    $keys[$index] = ((int) $value->format('U') * 1_000_000) + (int) $value->format('u');
                }
            }

            $order === SortOrder::ASC ? asort($keys, SORT_NUMERIC) : arsort($keys, SORT_NUMERIC);

            $sorted = [];

            foreach ($keys as $index => $_) {
                $sorted[$index] = $values[$index];
            }

            return $sorted;
        }

        uasort($values, static function (mixed $valueA, mixed $valueB) use ($order): int {
            $result = self::compare($valueA, $valueB);

            return $order === SortOrder::ASC ? $result : -$result;
        });

        return $values;
    }

    private static function compare(mixed $valueA, mixed $valueB): int
    {
        if (is_numeric($valueA) && is_numeric($valueB)) {
            return (float) $valueA <=> (float) $valueB;
        }

        if (is_string($valueA) && is_string($valueB)) {
            return $valueA <=> $valueB;
        }

        if ($valueA instanceof DateTimeInterface && $valueB instanceof DateTimeInterface) {
            return $valueA <=> $valueB;
        }

        if ($valueA instanceof DateInterval && $valueB instanceof DateInterval) {
            // DateInterval objects are not comparable directly, anchor both to the epoch
            $epoch = new DateTimeImmutable('@0');

            return $epoch->add($valueA) <=> $epoch->add($valueB);
        }

        if (is_array($valueA) && is_array($valueB)) {
            return $valueA <=> $valueB;
        }

        return 0;
    }
}
