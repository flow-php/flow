<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;
use Generator;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function ksort;
use function serialize;
use function sort;

/**
 * `category` / `amount` batches for a group by, and its output keyed by the group key.
 */
final class GroupedRows
{
    /**
     * @param iterable<Rows> $batches
     *
     * @return array<string, array<array-key, mixed>> output rows by the serialized value of $key, sorted
     */
    public static function byKey(iterable $batches, string $key): array
    {
        $rows = [];

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $row) {
                $rows[serialize($row[$key])] = $row;
            }
        }

        ksort($rows);

        return $rows;
    }

    /**
     * @return Generator<Rows> two batches: a 10, b 15, then a 20
     */
    public static function categories(): Generator
    {
        yield array_to_rows(
            [['category' => 'a', 'amount' => 10], ['category' => 'b', 'amount' => 15]],
            schema(str_schema('category'), int_schema('amount')),
        );
        yield array_to_rows(
            [['category' => 'a', 'amount' => 20]],
            schema(str_schema('category'), int_schema('amount')),
        );
    }

    /**
     * @param iterable<Rows> $batches
     *
     * @return list<string> every batch serialized, sorted - the batches as a set, whatever order they came in
     */
    public static function batchesAsSet(iterable $batches): array
    {
        $serialized = [];

        foreach ($batches as $batch) {
            $serialized[] = serialize($batch->toArray());
        }

        sort($serialized);

        return $serialized;
    }

    /**
     * @param iterable<Rows> $batches
     *
     * @return array<array-key, mixed> amount_sum per category
     */
    public static function sums(iterable $batches): array
    {
        $sums = [];

        foreach ($batches as $batch) {
            foreach ($batch->toArray() as $row) {
                $sums[$row['category']] = $row['amount_sum'];
            }
        }

        ksort($sums);

        return $sums;
    }
}
