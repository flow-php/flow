<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;
use Generator;

use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

/**
 * Batches of `score` / `name` rows, and the merged rows a processor emits for them.
 */
final class ScoredRows
{
    /**
     * @param list<array{int, string}> ...$batches
     *
     * @return Generator<int, Rows>
     */
    public static function batches(array ...$batches): Generator
    {
        foreach ($batches as $batch) {
            $rows = [];

            foreach ($batch as [$score, $name]) {
                $rows[] = ['score' => $score, 'name' => $name];
            }

            yield array_to_rows($rows, schema(int_schema('score'), str_schema('name')));
        }
    }

    /**
     * @param Generator<int, Rows> $batches
     *
     * @return array<int, array<array-key, mixed>>
     */
    public static function merged(Generator $batches): array
    {
        $all = [];

        foreach ($batches as $batch) {
            $all = array_merge($all, $batch->toArray());
        }

        return $all;
    }
}
