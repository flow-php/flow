<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowIntegrationTestCase;
use Flow\Filesystem\Path\Filter\KeepAll;

use function array_column;
use function array_filter;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_array;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;
use function str_starts_with;

final class ExternalSortMergeTest extends FlowIntegrationTestCase
{
    public function test_external_sort_over_three_merge_generations_loses_no_rows(): void
    {
        $input = [];

        for ($id = 100; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(config_builder()->sort(external_sort()->runSize(1)->bucketsCount(2))->build())
            ->read(from_array($input))
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 100), array_column($output, 'id'));
    }

    public function test_memory_storage_spills_nothing_to_disk(): void
    {
        $input = [];

        for ($id = 100; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(config_builder()->sort(external_sort()->storage(new MemoryBuckets())->runSize(1)->bucketsCount(2))->build())
            ->read(from_array($input))
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 100), array_column($output, 'id'));
        // .floe files are removed by the run's own clear(), so the pin is that nothing was ever created:
        // a filesystem-backed spill or merge side leaves /flow-php-sort/flow-php-buckets/ behind (measured)
        static::assertSame(
            [],
            iterator_to_array($this->fs->list(path($this->cacheDir->path() . '/**/*'), new KeepAll()), false),
        );
    }

    public function test_merge_storage_receives_only_merged_runs(): void
    {
        $spill = new RecordingBucketsStorage(new MemoryBuckets());
        $merge = new RecordingBucketsStorage(new MemoryBuckets());

        $input = [];

        for ($id = 100; $id > 0; $id--) {
            $input[] = ['id' => $id];
        }

        $output = [];
        df(
            config_builder()
                ->sort(external_sort()->storage($spill)->mergeStorage($merge)->runSize(1)->bucketsCount(2))
                ->build(),
        )
            ->read(from_array($input))
            ->sortBy([ref('id')])
            ->write(to_array($output))
            ->run();

        static::assertSame(range(1, 100), array_column($output, 'id'));
        static::assertNotSame([], $merge->appended, 'the run count must force at least one merged generation');
        static::assertSame([], array_filter($spill->read, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertCount(0, array_filter(
            $merge->read,
            static fn(string $id): bool => !str_starts_with($id, 'sort-merge-'),
        ));
    }
}
