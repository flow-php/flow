<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Integration\Sort\ExternalSort;

use Flow\ETL\Pipeline;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Sort\ExternalSort\BucketsCache\FilesystemBucketsCache;
use Flow\ETL\Tests\FlowIntegrationTestCase;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;
use function range;
use function shuffle;

final class ExternalSortTest extends FlowIntegrationTestCase
{
    public function test_batches_output_by_configured_batch_size(): void
    {
        $cacheDir = path(__DIR__ . '/var/test_batches_output_by_configured_batch_size');

        $this->fs()->rm($cacheDir);

        $input = array_map(static fn(int $id): array => ['id' => $id], range(1, 100));
        $randomizedInput = $input;
        shuffle($randomizedInput);

        $sort = new ExternalSort(
            new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir),
            bucketsCount: 2,
            batchSize: 7,
            runSize: 10,
        );

        $context = flow_context();
        $pipeline = new Pipeline(from_array($randomizedInput));

        $sortedOutput = iterator_to_array($sort->sortGenerator(
            $pipeline->process($context),
            $context,
            refs(ref('id')->asc()),
        ));

        static::assertSame(
            [7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 2],
            array_map(static fn($rows) => $rows->count(), $sortedOutput),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_multi_level_merge_with_small_runs_and_buckets(): void
    {
        $cacheDir = path(__DIR__ . '/var/test_multi_level_merge_with_small_runs_and_buckets');

        $this->fs()->rm($cacheDir);

        $input = array_map(static fn(int $id): array => ['id' => $id], range(1, 105));
        $randomizedInput = $input;
        shuffle($randomizedInput);

        $sort = new ExternalSort(
            new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir),
            bucketsCount: 2,
            batchSize: 1_000,
            runSize: 10,
        );

        $context = flow_context();
        $pipeline = new Pipeline(from_array($randomizedInput));

        $sortedOutput = iterator_to_array($sort->sortGenerator(
            $pipeline->process($context),
            $context,
            refs(ref('id')->asc()),
        ));

        static::assertEquals($input, array_merge(...array_map(static fn($row) => $row->toArray(), $sortedOutput)));
        static::assertCount(0, iterator_to_array($this->fs()->list($cacheDir->suffix('/**/*.floe'))));

        $this->fs()->rm($cacheDir);
    }

    public function test_sorting_empty_input(): void
    {
        $cacheDir = path(__DIR__ . '/var/test_sorting_empty_input');

        $this->fs()->rm($cacheDir);

        $sort = new ExternalSort(new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir));

        $context = flow_context();
        $pipeline = new Pipeline(from_array([]));

        static::assertCount(
            0,
            iterator_to_array($sort->sortGenerator($pipeline->process($context), $context, refs(ref('id')->asc()))),
        );

        $this->fs()->rm($cacheDir);
    }

    public function test_memory_implementation_of_external_sort_algorithm(): void
    {
        $cacheDir = path(__DIR__ . '/var/test_memory_implementation_of_external_sort_algorithm');

        $this->fs()->rm($cacheDir);

        $input = [];

        for ($j = 10; $j > 0; $j--) {
            for ($i = 10; $i > 0; $i--) {
                $input[] = [
                    'id' =>
                        str_pad((string) $j, 5, '0', STR_PAD_LEFT) . '-' . str_pad((string) $i, 3, '0', STR_PAD_LEFT),
                ];
            }
        }

        $randomizedInput = $input;
        shuffle($randomizedInput);

        $sort = new ExternalSort(new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir));

        $context = flow_context();
        $pipeline = new Pipeline(from_array($randomizedInput));

        $sortedOutput = iterator_to_array($sort->sortGenerator(
            $pipeline->process($context),
            $context,
            refs(ref('id')->desc()),
        ));

        static::assertEquals($input, array_merge(...array_map(static fn($row) => $row->toArray(), $sortedOutput)));

        $this->fs()->rm($cacheDir);
    }
}
