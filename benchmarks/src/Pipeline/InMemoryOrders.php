<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Pipeline;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;

/**
 * Memoised per process: a BeforeClassMethods hook runs in a separate process and would discard the
 * array, leaving the timed window to rebuild it.
 */
final class InMemoryOrders
{
    /** @var array<int, list<array<string, mixed>>> */
    private static array $rows = [];

    public static function clear(): void
    {
        self::$rows = [];
    }

    /**
     * @return list<array<string, mixed>>
     */
    public static function of(int $rows): array
    {
        if (array_key_exists($rows, self::$rows)) {
            return self::$rows[$rows];
        }

        $data = [];

        foreach (data_frame()->read(from_parquet(Datasets::orders($rows)->parquet()))->get() as $batch) {
            foreach ($batch->all() as $row) {
                // Row::toArray() is declared array<array-key, mixed>; names() is list<string>, so
                // recombining them is what makes the string keys ArrayMemory requires provable.
                $data[] = array_combine($row->names(), array_values($row->toArray()));
            }
        }

        return self::$rows[$rows] = $data;
    }
}
