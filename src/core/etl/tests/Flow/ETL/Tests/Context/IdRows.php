<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Rows;
use Generator;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;

/**
 * Batches of `id` rows, and the ids a processor emits for them.
 */
final class IdRows
{
    /**
     * @param list<int> ...$batches
     *
     * @return Generator<int, Rows>
     */
    public static function batches(array ...$batches): Generator
    {
        foreach ($batches as $batch) {
            yield array_to_rows(array_map(static fn(int $id): array => [
                'id' => $id,
            ], $batch), schema(int_schema('id')));
        }
    }

    /**
     * @param Generator<Rows> $batches
     *
     * @return list<mixed>
     */
    public static function ids(Generator $batches): array
    {
        $ids = [];

        foreach ($batches as $batch) {
            $ids = array_merge($ids, $batch->reduceToArray('id'));
        }

        return $ids;
    }
}
