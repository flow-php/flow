<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Sort;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Sort\SortSteps;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\external_sort;
use function Flow\ETL\DSL\memory_sort;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;

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
