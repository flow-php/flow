<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\Processor\BucketingProcessor;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Processor\PivotProcessor;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Transformer\PruneEntriesTransformer;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count as count_agg;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;

final class GroupByStepsTest extends FlowTestCase
{
    public function test_pivot_group_by_is_a_single_pivot_processor(): void
    {
        $groupBy = new GroupBy(ref('date'));
        $groupBy->pivot(ref('user'), pivot_values('norbert'));
        $groupBy->aggregate(sum(ref('contributions')));

        $steps = GroupBySteps::of($groupBy, config());

        static::assertCount(1, $steps);
        static::assertInstanceOf(PivotProcessor::class, $steps[0]);
    }

    public function test_enumerable_references_add_a_pruning_transformer(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $steps = GroupBySteps::of($groupBy, config());

        static::assertCount(3, $steps);
        static::assertInstanceOf(PruneEntriesTransformer::class, $steps[0]);
        static::assertInstanceOf(BucketingProcessor::class, $steps[1]);
        static::assertInstanceOf(GroupByAggregationProcessor::class, $steps[2]);
    }

    public function test_non_enumerable_references_skip_the_pruning_transformer(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount'), exact: ref('flag')));

        $steps = GroupBySteps::of($groupBy, config());

        static::assertCount(2, $steps);
        static::assertInstanceOf(BucketingProcessor::class, $steps[0]);
        static::assertInstanceOf(GroupByAggregationProcessor::class, $steps[1]);
    }

    public function test_groups_across_multiple_batches(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets()))->build()),
            rows(schema(str_schema('category'), int_schema('amount')), row(['category' => 'a', 'amount' => 10])),
            rows(schema(str_schema('category'), int_schema('amount')), row(['category' => 'a', 'amount' => 20])),
            rows(schema(str_schema('category'), int_schema('amount')), row(['category' => 'b', 'amount' => 15])),
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[$groupRow['category']] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(['a' => 30.0, 'b' => 15.0], $aggregated);
    }

    public function test_aggregates_each_bucket_separately_with_single_bucket(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(
                config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets())->bucketsCount(1))->build(),
            ),
            rows(
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'b', 'amount' => 15]),
                row(['category' => 'a', 'amount' => 20]),
            ),
        );

        static::assertCount(1, $result);

        $aggregated = $result[0]->toArray();
        usort($aggregated, static fn(array $a, array $b): int => (string) $a['category'] <=> (string) $b['category']);

        static::assertSame(
            [
                ['category' => 'a', 'amount_sum' => 30.0],
                ['category' => 'b', 'amount_sum' => 15.0],
            ],
            $aggregated,
        );
    }

    public function test_aggregates_many_buckets_and_flushes_per_bucket(): void
    {
        $groupBy = new GroupBy(ref('id'));
        $groupBy->aggregate(sum(ref('amount')));

        $batches = [];

        for ($id = 0; $id < 20; $id++) {
            $batches[] = rows(
                schema(int_schema('id'), int_schema('amount')),
                row(['id' => $id, 'amount' => 1]),
                row(['id' => $id, 'amount' => 2]),
            );
        }

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(
                config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets())->bucketsCount(4))->build(),
            ),
            ...$batches,
        );

        static::assertGreaterThan(1, count($result));

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[$groupRow['id']] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(array_fill(0, 20, 3.0), $aggregated);
    }

    public function test_removes_storage_buckets_after_last_bucket(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $storage = new SpyBucketsStorage(new MemoryBuckets());

        GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage($storage))->build()),
            rows(
                schema(str_schema('category'), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'b', 'amount' => 15]),
            ),
        );

        static::assertNotSame([], $storage->readBucketIds());
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_groups_rows_missing_group_column_under_null(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(count_agg());

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets()))->build()),
            rows(
                schema(str_schema('category', nullable: true), int_schema('amount')),
                row(['category' => 'a', 'amount' => 10]),
                row(['amount' => 15]),
                row(['amount' => 20]),
            ),
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[$groupRow['category'] ?? '__null__'] = $groupRow['_count'];
            }
        }

        ksort($aggregated);

        static::assertSame(['__null__' => 2, 'a' => 1], $aggregated);
    }

    public function test_spills_only_grouping_and_aggregation_columns(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $storage = new SpyBucketsStorage(new MemoryBuckets());

        GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage($storage))->build()),
            rows(
                schema(str_schema('category'), int_schema('amount'), str_schema('noise'), int_schema('id')),
                row(['category' => 'a', 'amount' => 10, 'noise' => 'x', 'id' => 1]),
                row(['category' => 'b', 'amount' => 15, 'noise' => 'y', 'id' => 2]),
            ),
        );

        foreach ($storage->appendedRows() as $batches) {
            foreach ($batches as $batch) {
                foreach ($batch as $spilledRow) {
                    static::assertSame(['category', 'amount'], $spilledRow->names());
                }
            }
        }
    }

    public function test_spills_all_columns_when_aggregator_references_cannot_be_enumerated(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount'), exact: ref('flag')));

        $storage = new SpyBucketsStorage(new MemoryBuckets());

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage($storage))->build()),
            rows(
                schema(str_schema('category'), float_schema('amount'), bool_schema('flag')),
                row(['category' => 'a', 'amount' => 0.1, 'flag' => true]),
                row(['category' => 'a', 'amount' => 0.2, 'flag' => true]),
            ),
        );

        foreach ($storage->appendedRows() as $batches) {
            foreach ($batches as $batch) {
                foreach ($batch as $spilledRow) {
                    static::assertSame(['category', 'amount', 'flag'], $spilledRow->names());
                }
            }
        }

        static::assertSame(0.3, $result[0]->first()->get('amount_sum'));
    }

    public function test_missing_aggregated_column_is_treated_as_null_in_strict_mode(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $context = flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets()))->build());
        $result = GroupByContext::aggregate(
            $groupBy,
            $context,
            rows(
                schema(str_schema('category'), int_schema('amount', nullable: true)),
                row(['category' => 'a', 'amount' => 10]),
                row(['category' => 'a']),
            ),
        );

        static::assertSame(10.0, $result[0]->first()->get('amount_sum'));
    }

    public function test_handles_empty_input(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $result = GroupByContext::aggregate(
            $groupBy,
            flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryBuckets()))->build()),
        );

        $total = 0;

        foreach ($result as $batch) {
            $total += $batch->count();
        }

        static::assertSame(0, $total);
    }
}
