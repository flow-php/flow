<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Planning\Explain;

use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Functions\Join\Condition\Equal;
use Flow\ETL\Functions\Join\Expression;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Operators\Constraint\UniqueConstraint;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\Join\CrossJoin;
use Flow\ETL\Operators\Processor\BatchingByProcessor;
use Flow\ETL\Operators\Processor\BatchingProcessor;
use Flow\ETL\Operators\Processor\CachingProcessor;
use Flow\ETL\Operators\Processor\CollectingProcessor;
use Flow\ETL\Operators\Processor\ConstrainedProcessor;
use Flow\ETL\Operators\Processor\MemorySortProcessor;
use Flow\ETL\Operators\Processor\MergeSortProcessor;
use Flow\ETL\Operators\Processor\OffsetProcessor;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Operators\Processor\TopNProcessor;
use Flow\ETL\Operators\Processor\VoidProcessor;
use Flow\ETL\Operators\Processor\WindowProcessor;
use Flow\ETL\Planning\Explain\StepDetails;
use Flow\ETL\Planning\Physical\JoinStep;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Double\DeclaringExtractor;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ExternalSortMother;
use Flow\ETL\Tests\Mother\GroupByAggregationProcessorMother;
use Flow\ETL\Tests\Mother\HashJoinMother;
use Flow\ETL\Tests\Mother\PhysicalPlanMother;
use Flow\ETL\Tests\Mother\RepartitionProcessorMother;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Cardinality;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\NullsOrder;

use function Flow\ETL\DSL\config;
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

        static::assertSame(
            ['Extractor: ArrayExtractor', '   Statistics: rows exact 1 · size unknown'],
            $details->lines(from_array([['id' => 1]])),
        );
        static::assertSame(['Processor: VoidProcessor'], $details->lines(new VoidProcessor()));
        static::assertSame(
            ['Processor: BatchingProcessor', '   Batch: 100'],
            $details->lines(new BatchingProcessor(new BatchSize(100))),
        );
    }

    public function test_a_hash_join_lists_its_condition_and_the_storage_it_was_given(): void
    {
        static::assertSame(
            [
                'Join: HashJoin',
                '   Type: left',
                '   On: id = id',
                '   Prefix: joined_',
                '   Storage: MemoryRowsStorage',
                '   Buckets: 8',
                '   Batch: 500',
            ],
            (new StepDetails())->lines(
                new JoinStep(
                    HashJoinMother::with(
                        Expression::on([new Equal('id', 'id')], 'joined_'),
                        JoinType::left,
                        new MemoryRowsStorage(),
                        8,
                        500,
                    ),
                    PhysicalPlanMother::reading(from_array([['id' => 1]])),
                ),
            ),
        );
    }

    public function test_a_cross_join_lists_its_prefix_only_when_it_was_given(): void
    {
        $details = new StepDetails();
        $right = PhysicalPlanMother::reading(from_array([['id' => 1]]));

        static::assertSame(['Type: cross'], $details->settings(new JoinStep(new CrossJoin(), $right)));
        static::assertSame(
            ['Type: cross', 'Prefix: r_'],
            $details->settings(new JoinStep(new CrossJoin('r_'), $right)),
        );
    }

    public function test_sorting_steps_list_their_columns_with_the_direction(): void
    {
        $details = new StepDetails();

        static::assertSame(
            ['Sort by: id asc, name desc'],
            $details->settings(new MemorySortProcessor([ref('id')->asc(), ref('name')->desc()])),
        );
        static::assertSame(
            ['Sort by: id desc', 'Memory: 64 MB', 'Spill: MemoryRowsStorage', 'Merge: 4 ways', 'Batch: 50'],
            $details->settings(
                new MergeSortProcessor(
                    ExternalSortMother::config(new MemoryRowsStorage(), Unit::fromMb(64), 4, 50),
                    [ref('id')->desc()],
                    new NativePHPRandomValueGenerator(),
                ),
            ),
        );
        static::assertSame(
            ['Top: 3', 'Sort by: id asc'],
            $details->settings(
                new TopNProcessor([ref('id')->asc()], 3, new MemorySortConfig(), new NativePHPRandomValueGenerator()),
            ),
        );
        static::assertSame(
            ['Sort by: id asc nulls last, name desc nulls first'],
            $details->settings(new MemorySortProcessor([
                ref('id')->asc(NullsOrder::LAST),
                ref('name')->desc(NullsOrder::FIRST),
            ])),
        );
    }

    public function test_a_group_by_lists_its_columns_and_aggregations(): void
    {
        static::assertSame(
            ['Group by: name', 'Aggregations: Sum', 'Memory: 64 MB', 'Storage: MemoryRowsStorage', 'Batch: 25'],
            (new StepDetails())->settings(GroupByAggregationProcessorMother::with(
                new GroupBy(refs(ref('name')), new Aggregators(sum(ref('id')))),
                new MemoryRowsStorage(),
                Unit::fromMb(64),
                batchSize: 25,
            )),
        );
    }

    public function test_a_pivot_lists_the_column_it_pivots_on(): void
    {
        $details = new StepDetails();

        static::assertSame(
            ['Group by: name', 'Batch: 1000'],
            $details->settings(
                new PivotProcessor(new GroupBy(refs(ref('name')), new Aggregators(sum(ref('id')))), config()->grouping),
            ),
        );
        static::assertSame(
            ['Group by: name', 'Pivot: country', 'Batch: 500'],
            $details->settings(
                new PivotProcessor(
                    new GroupBy(
                        refs(ref('name')),
                        new Aggregators(sum(ref('id'))),
                        new Pivot(ref('country'), new DeclaredPivotValues('USA', 'China')),
                    ),
                    new BucketingConfig(new MemoryRowsStorage(), 4, 500, Unit::fromMb(64)),
                ),
            ),
        );
    }

    public function test_a_repartition_lists_its_columns_and_storage(): void
    {
        static::assertSame(
            ['By: id', 'Memory: 64 MB', 'Storage: MemoryRowsStorage'],
            (new StepDetails())->settings(RepartitionProcessorMother::with(
                refs(ref('id')),
                new MemoryRowsStorage(),
                Unit::fromMb(64),
            )),
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

        static::assertSame(
            ['Cache: MemoryRowsStorage'],
            $details->settings(new CachingProcessor('flow-php-config-id', new MemoryRowsStorage(), idGiven: false)),
        );
        static::assertSame(
            ['Cache: MemoryRowsStorage', 'Id: orders'],
            $details->settings(new CachingProcessor('orders', new MemoryRowsStorage())),
        );
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
        static::assertSame([], (new StepDetails())->settings(new VoidProcessor()));
    }

    public function test_a_source_lists_the_statistics_it_declares(): void
    {
        static::assertSame(
            ['Statistics: rows exact 1 000 · size ~309 188 B ±50%'],
            (new StepDetails())->settings(new DeclaringExtractor(
                new Statistics(Cardinality::exact(1_000), Cardinality::approximately(309_188)),
            )),
        );
    }

    public function test_a_source_that_declares_nothing_lists_no_statistics(): void
    {
        static::assertSame([], (new StepDetails())->settings(new DeclaringExtractor(new Statistics())));
    }
}
