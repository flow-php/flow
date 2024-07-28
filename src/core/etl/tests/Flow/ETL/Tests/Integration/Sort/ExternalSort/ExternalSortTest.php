<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use function Flow\ETL\DSL\{flow_context, from_array, ref, refs};
use function Flow\Filesystem\DSL\native_local_filesystem;
use Flow\ETL\Cache\RowCache\FilesystemCache;
use Flow\ETL\Pipeline\{SynchronousPipeline};
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Tests\Integration\IntegrationTestCase;
use Flow\Filesystem\Path;

final class ExternalSortTest extends IntegrationTestCase
{
    public function test_memory_implementation_of_external_sort_algorithm() : void
    {
        $cacheDir = new Path(__DIR__ . '/var/test_memory_implementation_of_external_sort_algorithm');

        $this->fs()->rm($cacheDir);

        $input = [];

        for ($j = 10; $j > 0; $j--) {
            for ($i = 10; $i > 0; $i--) {
                $input[] = ['id' => str_pad((string) $j, 5, '0', STR_PAD_LEFT) . '-' . str_pad((string) $i, 3, '0', STR_PAD_LEFT)];
            }
        }

        $randomizedInput = $input;
        \shuffle($randomizedInput);

        $sort = new ExternalSort(
            new SynchronousPipeline(from_array($randomizedInput)),
            new FilesystemCache(
                native_local_filesystem(),
                cacheDir: $cacheDir
            ),
        );

        $sortedOutput = \iterator_to_array(
            $sort->sortBy(
                flow_context(),
                refs(ref('id')->desc())
            )->extract(
                flow_context()
            )
        );

        self::assertEquals(
            $input,
            \array_merge(...\array_map(
                fn ($row) => $row->toArray(),
                $sortedOutput
            ))
        );

        $this->fs()->rm($cacheDir);
    }
}
