<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Sort\ExternalSort;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function array_filter;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function iterator_to_array;
use function range;
use function str_starts_with;

final class SortStepsTest extends FlowTestCase
{
    public function test_external_sort_config_builds_one_merge_sort_processor(): void
    {
        $steps = SortSteps::of(
            refs(ref('id')),
            config_builder()->sort(external_sort()->storage(new MemoryBuckets()))->build(),
        );

        static::assertCount(1, $steps);
        static::assertInstanceOf(MergeSortProcessor::class, $steps[0]);
    }

    public function test_external_carries_the_configured_memory_limit(): void
    {
        $sort = SortSteps::external(
            refs(ref('id')),
            config_builder()->sort(external_sort()->memoryLimit(Unit::fromMb(3)))->build(),
        );

        static::assertInstanceOf(ExternalSort::class, $sort);
        static::assertSame(Unit::fromMb(3)->inBytes(), $sort->memoryLimit->inBytes());
    }

    public function test_external_is_null_for_the_memory_sort(): void
    {
        static::assertNull(SortSteps::external(refs(ref('id')), config_builder()->sort(memory_sort())->build()));
    }

    public function test_pinned_algorithm_wins_over_configuration(): void
    {
        static::assertNull(SortSteps::external(refs(ref('id')), config_builder()->build(), memory_sort()));
    }

    public function test_external_sort_resolves_both_phases_over_the_same_storage_by_default(): void
    {
        // asserting processor classes alone cannot see which storage each phase resolved to
        $spill = new RecordingBucketsStorage(new MemoryBuckets());

        $steps = SortSteps::of(
            refs(ref('id')),
            config_builder()
                ->sort(external_sort()->storage($spill)->memoryLimit(Unit::fromBytes(1))->bucketsCount(2))
                ->build(),
        );

        $context = flow_context(config());
        $input = (static function (): Generator {
            foreach (range(9, 0) as $i) {
                yield array_to_rows([['id' => $i]], schema(int_schema('id')));
            }
        })();

        static::assertInstanceOf(MergeSortProcessor::class, $steps[0]);

        iterator_to_array($steps[0]->process($input, $context), false);

        static::assertNotSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )), 'merged runs must land in the spill storage when no mergeStorage() was set');
    }

    public function test_external_sort_routes_merged_runs_to_the_merge_storage_when_set(): void
    {
        $spill = new RecordingBucketsStorage(new MemoryBuckets());
        $merge = new RecordingBucketsStorage(new MemoryBuckets());

        $steps = SortSteps::of(
            refs(ref('id')),
            config_builder()
                ->sort(
                    external_sort()
                        ->storage($spill)
                        ->mergeStorage($merge)
                        ->memoryLimit(Unit::fromBytes(1))
                        ->bucketsCount(2),
                )
                ->build(),
        );

        $context = flow_context(config());
        $input = (static function (): Generator {
            foreach (range(9, 0) as $i) {
                yield array_to_rows([['id' => $i]], schema(int_schema('id')));
            }
        })();

        static::assertInstanceOf(MergeSortProcessor::class, $steps[0]);

        iterator_to_array($steps[0]->process($input, $context), false);

        static::assertSame([], array_filter($spill->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
        static::assertNotSame([], array_filter($merge->appended, static fn(string $id): bool => str_starts_with(
            $id,
            'sort-merge-',
        )));
    }

    public function test_external_sort_is_the_default(): void
    {
        $steps = SortSteps::of(refs(ref('id')), config_builder()->build());

        static::assertCount(1, $steps);
        static::assertInstanceOf(MergeSortProcessor::class, $steps[0]);
    }

    public function test_memory_sort_config_builds_a_memory_sort_processor(): void
    {
        $steps = SortSteps::of(refs(ref('id')), config_builder()->sort(memory_sort())->build());

        static::assertCount(1, $steps);
        static::assertInstanceOf(MemorySortProcessor::class, $steps[0]);
    }
}
