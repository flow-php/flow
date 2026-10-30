<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Kernels\Sort\ExternalSort;

use Flow\ETL\Storage\FilesystemRowsStorage;
use Flow\ETL\Tests\Double\RecordingRowsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Memory\Unit;
use Generator;

use function array_filter;
use function array_map;
use function array_slice;
use function array_sum;
use function array_values;
use function chr;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function memory_get_usage;
use function str_repeat;
use function str_starts_with;

use const ARRAY_FILTER_USE_KEY;

final class ExternalSortRunSizeTest extends FlowIntegrationTestCase
{
    public function test_after_a_spill_the_next_run_fills_up_again_rather_than_spilling_every_batch(): void
    {
        $storage = new RecordingRowsStorage(
            new FilesystemRowsStorage($this->fs, path($this->cacheDir->path() . '/sort-runs'), new PhpBackend()),
        );

        // one ~1 MB row a batch against ~4.5 MB: a run holds a few batches - once spilled to disk, its memory is free
        $input = (static function (): Generator {
            for ($id = 40; $id > 0; $id--) {
                yield array_to_rows(
                    [['id' => $id, 'payload' => str_repeat(chr(65 + ($id % 26)), 1_000_000)]],
                    schema(int_schema('id'), str_schema('payload')),
                );
            }
        })();

        $sorted = iterator_to_array(
            ExternalSortMother::with(
                [ref('id')->asc()],
                $storage,
                Unit::fromBytes(memory_get_usage(false) + 4_500_000),
            )->sort($input, config()->backend()),
            false,
        );

        // spilled runs only: a merge appends its output batch by batch into a sort-merge- bucket
        $runs = array_values(array_map(
            static fn(array $counts): int => $counts[0],
            array_filter(
                $storage->rowCounts,
                static fn(string $bucket): bool => !str_starts_with($bucket, 'sort-merge-'),
                ARRAY_FILTER_USE_KEY,
            ),
        ));

        static::assertSame(40, array_sum(array_map(static fn($batch): int => $batch->count(), $sorted)));
        static::assertGreaterThanOrEqual(2, count($runs));

        foreach (array_slice($runs, 0, -1) as $rows) {
            static::assertGreaterThan(1, $rows, 'a run after a spill must hold more than one batch');
        }
    }
}
