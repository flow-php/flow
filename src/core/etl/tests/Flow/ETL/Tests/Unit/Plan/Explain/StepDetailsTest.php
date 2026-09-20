<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Plan\Explain;

use Flow\ETL\Bucketing\Buckets;
use Flow\ETL\Bucketing\HashBucketing;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Constraint\UniqueConstraint;
use Flow\ETL\Executor;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\DeclaredPivotValues;
use Flow\ETL\Join\Comparison\Equal;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\NativePHPRandomValueGenerator;
use Flow\ETL\Plan\Explain\StepDetails;
use Flow\ETL\Processor\BatchingByProcessor;
use Flow\ETL\Processor\BatchingProcessor;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\CachingProcessor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\ConstrainedProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\MemorySortProcessor;
use Flow\ETL\Processor\MergeSortProcessor;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Processor\RepartitionProcessor;
use Flow\ETL\Processor\TopNProcessor;
use Flow\ETL\Processor\VoidProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\HashJoinProcessorMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Transformer\CrossJoinRowsTransformer;

use function Flow\ETL\DSL\from_array;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\row_number;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

final class StepDetailsTest extends FlowTestCase
{
    public function test_a_step_is_labelled_by_what_it_is(): void
    {
        $details = new StepDetails();

        static::assertSame(['Extractor: ArrayExtractor'], $details->lines(from_array([['id' => 1]])));
        static::assertSame(['Processor: VoidProcessor'], $details->lines(new VoidProcessor()));
        static::assertSame(
            ['Processor: BatchingProcessor', '   Batch: 100'],
            $details->lines(new BatchingProcessor(100)),
        );
    }

    public function test_a_hash_join_lists_its_condition_and_the_storage_it_was_given(): void
    {
        static::assertSame(
            [
                'Processor: HashJoinProcessor',
                '   Join: left',
                '   On: id = id',
                '   Prefix: joined_',
                '   Storage: MemoryBuckets',
                '   Buckets: 8',
                '   Batch: 500',
            ],
            (new StepDetails())->lines(HashJoinProcessorMother::with(
                PhysicalPlanMother::reading(from_array([['id' => 1]])),
                Expression::on([new Equal('id', 'id')], 'joined_'),
                Join::left,
                new MemoryBuckets(),
                8,
                500,
            )),
        );
    }

    public function test_a_cross_join_lists_its_prefix_only_when_it_was_given(): void
    {
        $details = new StepDetails();
        $right = PhysicalPlanMother::reading(from_array([['id' => 1]]));

        static::assertSame(['Join: cross'], $details->settings(new CrossJoinRowsTransformer($right, new Executor())));
        static::assertSame(
            ['Join: cross', 'Prefix: r_'],
            $details->settings(new CrossJoinRowsTransformer($right, new Executor(), 'r_')),
        );
    }

    public function test_sorting_steps_list_their_columns_with_the_direction(): void
    {
        $details = new StepDetails();

        static::assertSame(
            ['Sort: id asc, name desc'],
            $details->settings(new MemorySortProcessor(refs(ref('id'), ref('name')->desc()))),
        );
        static::assertSame(
            ['Sort: id desc', 'Spill: MemoryBuckets', 'Merge: 4 ways', 'Batch: 50'],
            $details->settings(
                new MergeSortProcessor(
                    refs(ref('id')->desc()),
                    new Buckets(new MemoryBuckets()),
                    new Buckets(new MemoryBuckets()),
                    new NativePHPRandomValueGenerator(),
                    4,
                    50,
                ),
            ),
        );
        static::assertSame(['Top: 3', 'Sort: id asc'], $details->settings(new TopNProcessor(refs(ref('id')), 3)));
    }

    public function test_a_group_by_lists_its_columns_and_aggregations(): void
    {
        $groupBy = new GroupBy(ref('name'));
        $groupBy->aggregate(sum(ref('id')));

        static::assertSame(
            ['Group by: name', 'Aggregations: Sum', 'Storage: MemoryBuckets', 'Batch: 25'],
            (new StepDetails())->settings(
                new GroupByAggregationProcessor($groupBy, new Buckets(new MemoryBuckets()), 25),
            ),
        );
    }

    public function test_a_pivot_lists_the_column_it_pivots_on(): void
    {
        $grouped = new GroupBy(ref('name'));
        $grouped->aggregate(sum(ref('id')));

        $pivoted = new GroupBy(ref('name'));
        $pivoted->aggregate(sum(ref('id')));
        $pivoted->pivot(ref('country'), new DeclaredPivotValues('USA', 'China'));

        $details = new StepDetails();

        static::assertSame(['Group by: name', 'Batch: 1000'], $details->settings(new PivotProcessor($grouped)));
        static::assertSame(
            ['Group by: name', 'Pivot: country', 'Batch: 500'],
            $details->settings(new PivotProcessor($pivoted, 500)),
        );
    }

    public function test_a_repartition_lists_its_columns_hasher_and_storage(): void
    {
        static::assertSame(
            ['By: id', 'Hasher: NativeHasher', 'Storage: MemoryBuckets'],
            (new StepDetails())->settings(
                new RepartitionProcessor(refs(ref('id')), new Buckets(new MemoryBuckets()), new NativeHasher()),
            ),
        );
    }

    public function test_batching_by_a_column_lists_the_minimum_size_only_when_it_was_given(): void
    {
        $details = new StepDetails();

        static::assertSame(['Batch by: id'], $details->settings(new BatchingByProcessor(ref('id'))));
        static::assertSame(
            ['Batch by: id', 'Min size: 10'],
            $details->settings(new BatchingByProcessor(ref('id'), 10)),
        );
    }

    public function test_a_caching_step_lists_its_id_only_when_it_was_given(): void
    {
        $details = new StepDetails();

        static::assertSame([], $details->settings(new CachingProcessor()));
        static::assertSame(['Id: orders'], $details->settings(new CachingProcessor('orders')));
    }

    public function test_a_collecting_step_says_when_a_schema_was_declared(): void
    {
        $details = new StepDetails();

        static::assertSame([], $details->settings(new CollectingProcessor()));
        static::assertSame(['Schema: declared'], $details->settings(new CollectingProcessor(schema(int_schema('id')))));
    }

    public function test_the_remaining_steps_list_what_they_were_given(): void
    {
        $details = new StepDetails();

        static::assertSame(['Skip: 7'], $details->settings(new OffsetProcessor(7)));
        static::assertSame([], $details->settings(new VoidProcessor()));
        static::assertSame([], $details->settings(new ConstrainedProcessor()));
        static::assertSame(
            ['Constraints: UniqueConstraint'],
            $details->settings(new ConstrainedProcessor([new UniqueConstraint(ref('id'))])),
        );
    }

    public function test_a_window_lists_the_column_it_writes_and_its_function(): void
    {
        static::assertSame(
            ['Column: rn', 'Function: RowNumber'],
            (new StepDetails())->settings(
                new WindowProcessor('rn', row_number()->over(window()->partitionBy(ref('id')))),
            ),
        );
    }

    public function test_a_step_the_explain_does_not_know_lists_nothing(): void
    {
        static::assertSame([], (new StepDetails())->settings(from_array([['id' => 1]])));
    }

    public function test_bucketing_lists_its_strategy_and_storage(): void
    {
        static::assertSame(
            ['Strategy: HashBucketing', 'Storage: MemoryBuckets'],
            (new StepDetails())->settings(
                new BucketingProcessor(
                    new HashBucketing([ref('id')], 4, new NativeHasher(), new NativePHPRandomValueGenerator(), 'test'),
                    new Buckets(new MemoryBuckets()),
                ),
            ),
        );
    }
}
