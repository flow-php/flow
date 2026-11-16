<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Values\Rows;
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
     * Every frame reports 1_000_000 native bytes, the way a decoded frame of a stored run grows a memory budget.
     *
     * @param list<int> ...$frames
     *
     * @return Generator<int, Rows>
     */
    public static function reportedFrames(ReportedMemoryBackend $backend, int &$yields, array ...$frames): Generator
    {
        foreach (self::batches(...$frames) as $frame) {
            $backend->reported += 1_000_000;
            $yields++;

            yield $frame;
        }
    }

    /**
     * @param Generator<CorruptedRecordException|Rows> $batches
     *
     * @return list<mixed>
     */
    public static function ids(Generator $batches): array
    {
        $ids = [];

        foreach ($batches as $batch) {
            $ids = array_merge($ids, ($batch instanceof Rows ? $batch : throw $batch)->reduceToArray('id'));
        }

        return $ids;
    }
}
