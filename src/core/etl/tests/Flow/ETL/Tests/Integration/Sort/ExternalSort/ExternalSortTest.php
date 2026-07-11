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
use function shuffle;

final class ExternalSortTest extends FlowIntegrationTestCase
{
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

    public function test_sorting_with_multiple_merge_rounds(): void
    {
        $cacheDir = path(__DIR__ . '/var/test_sorting_with_multiple_merge_rounds');

        $this->fs()->rm($cacheDir);

        $input = [];

        for ($i = 1; $i <= 100; $i++) {
            $input[] = ['id' => str_pad((string) $i, 5, '0', STR_PAD_LEFT)];
        }

        $randomizedInput = $input;
        shuffle($randomizedInput);

        $sort = new ExternalSort(
            new FilesystemBucketsCache($this->fs(), cacheDir: $cacheDir),
            bucketsCount: 2,
            bucketSize: 10,
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
}
