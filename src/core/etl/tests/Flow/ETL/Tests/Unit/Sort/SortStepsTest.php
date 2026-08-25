<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Row;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Tests\Double\RecordingBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Generator;

use function array_filter;
use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function iterator_to_array;
use function range;
use function str_starts_with;

final class SortStepsTest extends FlowTestCase
{
    public function test_external_sort_config_builds_bucketing_and_merge_processors(): void
    {
        $steps = SortSteps::of(
            refs(ref('id')),
            config_builder()->sort(external_sort()->storage(new MemoryBuckets()))->build(),
        );

        static::assertCount(2, $steps);
        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(MergeSortProcessor::class, $steps[1]);
    }

    public function test_external_sort_resolves_both_phases_over_the_same_storage_by_default(): void
    {
        // asserting processor classes alone cannot see which storage each phase resolved to
        $spill = new RecordingBucketsStorage(new MemoryBuckets());

        $steps = SortSteps::of(
            refs(ref('id')),
            config_builder()->sort(external_sort()->storage($spill)->runSize(1)->bucketsCount(2))->build(),
        );

        $context = flow_context(config());
        $input = (static function (): Generator {
            yield rows(...array_map(static fn(int $i): Row => row(int_entry('id', $i)), range(9, 0)));
        })();

        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(MergeSortProcessor::class, $steps[1]);

        iterator_to_array($steps[1]->process($steps[0]->process($input, $context), $context), false);

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
                ->sort(external_sort()->storage($spill)->mergeStorage($merge)->runSize(1)->bucketsCount(2))
                ->build(),
        );

        $context = flow_context(config());
        $input = (static function (): Generator {
            yield rows(...array_map(static fn(int $i): Row => row(int_entry('id', $i)), range(9, 0)));
        })();

        static::assertInstanceOf(MergeSortProcessor::class, $steps[1]);

        iterator_to_array($steps[1]->process($steps[0]->process($input, $context), $context), false);

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

        static::assertCount(2, $steps);
        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(MergeSortProcessor::class, $steps[1]);
    }

    public function test_memory_sort_config_builds_a_memory_sort_processor(): void
    {
        $steps = SortSteps::of(refs(ref('id')), config_builder()->sort(memory_sort())->build());

        static::assertCount(1, $steps);
        static::assertInstanceOf(MemorySortProcessor::class, $steps[0]);
    }
}
