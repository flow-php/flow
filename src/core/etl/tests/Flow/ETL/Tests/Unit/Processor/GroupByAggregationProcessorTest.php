<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Bucketing\Storage\MemoryBuckets;
use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByAggregationProcessor;
use Flow\ETL\Tests\Context\GroupedRows;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyBucketsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\GroupByAggregationProcessorMother;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function iterator_to_array;
use function memory_get_usage;
use function mt_rand;
use function mt_srand;
use function serialize;

final class GroupByAggregationProcessorTest extends FlowTestCase
{
    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 50; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_under_the_limit_it_aggregates_in_one_pass_without_touching_the_buckets(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame(
            ['a' => 30.0, 'b' => 15.0],
            GroupedRows::sums(GroupByAggregationProcessorMother::inMemory($groupBy, $storage)->process(
                GroupedRows::categories(),
                flow_context(),
            )),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_past_the_limit_the_rest_is_partitioned_and_the_result_is_the_same(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame(
            ['a' => 30.0, 'b' => 15.0],
            GroupedRows::sums(GroupByAggregationProcessorMother::with(
                $groupBy,
                $storage,
                Unit::fromBytes(1),
                batchSize: 1,
            )->process(GroupedRows::categories(), flow_context())),
        );
        static::assertNotSame([], $storage->appendedRows());
        static::assertSame([], $storage->liveBucketIds());
    }

    /**
     * Any batch but the last can be the one that passes the limit: aggregation before it and partitioned aggregation
     * after it give the result one in-memory pass gives - many keys, nulls, order-dependent aggregates included.
     */
    #[DataProvider('random_datasets')]
    public function test_crossing_the_limit_at_any_batch_equals_one_in_memory_pass(int $seed): void
    {
        mt_srand($seed);
        $schema = schema(int_schema('k', nullable: true), int_schema('v'));
        $batches = [];

        for ($b = 0, $count = mt_rand(3, 8); $b < $count; $b++) {
            $rows = [];

            for ($r = 0, $size = mt_rand(1, 20); $r < $size; $r++) {
                $rows[] = ['k' => mt_rand(0, 6) === 0 ? null : mt_rand(0, 40), 'v' => mt_rand(-50, 50)];
            }

            $batches[] = array_to_rows($rows, $schema);
        }

        $crossAfter = mt_rand(0, $count - 2);
        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(
            sum(ref('v')),
            count(ref('v')),
            min(ref('v')),
            max(ref('v')),
            first(ref('v')),
            last(ref('v')),
            collect(ref('v')),
        );

        $backend = new ReportedMemoryBackend();
        $input = (static function () use ($batches, $crossAfter, $backend): Generator {
            foreach ($batches as $position => $batch) {
                if ($position === $crossAfter) {
                    $backend->reported = 1_000_000_000_000;
                }

                yield $batch;
            }
        })();

        $storage = new SpyBucketsStorage(new MemoryBuckets());
        $spilled = GroupByAggregationProcessorMother::with(
            $groupBy,
            $storage,
            Unit::fromBytes(memory_get_usage(false) + 500_000_000),
            batchSize: 3,
        )->process($input, flow_context(config_builder()->backend($backend)->build()));
        $inMemory = GroupByAggregationProcessorMother::inMemory($groupBy)->process(
            (static fn(): Generator => yield from $batches)(),
            flow_context(),
        );

        static::assertSame(serialize(GroupedRows::byKey($inMemory, 'k')), serialize(GroupedRows::byKey($spilled, 'k')));
        static::assertNotSame([], $storage->appendedRows());
    }

    public function test_a_key_aggregated_before_the_limit_is_merged_with_its_rows_after_it(): void
    {
        $schema = schema(str_schema('k'), int_schema('v'));
        $backend = new ReportedMemoryBackend();
        $input = (static function () use ($schema, $backend): Generator {
            yield array_to_rows([['k' => 'a', 'v' => 1], ['k' => 'b', 'v' => 2]], $schema);
            $backend->reported = 1_000_000_000_000;

            yield array_to_rows([['k' => 'a', 'v' => 3], ['k' => 'c', 'v' => 4]], $schema);
            yield array_to_rows([['k' => 'a', 'v' => 5]], $schema);
        })();
        $groupBy = new GroupBy(ref('k'));
        $groupBy->aggregate(first(ref('v')), last(ref('v')), collect(ref('v')), sum(ref('v')));
        $storage = new SpyBucketsStorage(new MemoryBuckets());

        static::assertSame(
            [
                's:1:"a";' => ['k' => 'a', 'v_first' => 1, 'v_last' => 5, 'v_collection' => [1, 3, 5], 'v_sum' => 9.0],
                's:1:"b";' => ['k' => 'b', 'v_first' => 2, 'v_last' => 2, 'v_collection' => [2], 'v_sum' => 2.0],
                's:1:"c";' => ['k' => 'c', 'v_first' => 4, 'v_last' => 4, 'v_collection' => [4], 'v_sum' => 4.0],
            ],
            GroupedRows::byKey(
                GroupByAggregationProcessorMother::with(
                    $groupBy,
                    $storage,
                    Unit::fromBytes(memory_get_usage(false) + 500_000_000),
                )->process($input, flow_context(config_builder()->backend($backend)->build())),
                'k',
            ),
        );
        static::assertNotSame([], $storage->appendedRows());
    }

    public function test_bind_derives_the_group_by_columns_followed_by_the_aggregations(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        static::assertEquals(
            schema(str_schema('category'), float_schema('amount_sum', nullable: true)),
            GroupByAggregationProcessorMother::inMemory($groupBy)->bind(schema(
                str_schema('category'),
                int_schema('amount'),
                str_schema('dropped'),
            ))->output,
        );
    }

    public function test_bind_refuses_a_group_by_column_missing_from_the_input(): void
    {
        $groupBy = new GroupBy(ref('missing'));
        $groupBy->aggregate(sum(ref('amount')));

        $this->expectException(SchemaDefinitionNotFoundException::class);

        GroupByAggregationProcessorMother::inMemory($groupBy)->bind(schema(int_schema('amount')));
    }

    public function test_throws_when_batch_size_below_one(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0, given: 0');

        // @mago-ignore analysis:invalid-argument
        GroupByAggregationProcessorMother::with(
            new GroupBy(ref('category')),
            new MemoryBuckets(),
            Unit::fromGb(1),
            4,
            0,
        );
    }

    public function test_handles_an_empty_stream(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        static::assertSame(
            [],
            iterator_to_array(
                GroupByAggregationProcessorMother::inMemory($groupBy)->process(
                    (static fn(): Generator => yield from [])(),
                    flow_context(),
                ),
                false,
            ),
        );
    }

    public function test_a_bound_global_aggregate_over_an_empty_stream_emits_one_row_of_defaults(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        $bound = GroupByAggregationProcessorMother::inMemory($groupBy)->bind(schema(int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process((static fn(): Generator => yield from [])(), flow_context()),
            false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => null, 'amount_count' => 0]], $result[0]->toArray());
        static::assertEquals($bound->output, $result[0]->schema());
    }

    public function test_a_bound_keyed_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $bound = GroupByAggregationProcessorMother::inMemory($groupBy)->bind(schema(
            str_schema('category'),
            int_schema('amount'),
        ));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        static::assertSame(
            [],
            iterator_to_array(
                $bound->step->process((static fn(): Generator => yield from [])(), flow_context()),
                false,
            ),
        );
    }

    public function test_an_unbound_global_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        static::assertSame(
            [],
            iterator_to_array(
                GroupByAggregationProcessorMother::inMemory($groupBy)->process(
                    (static fn(): Generator => yield from [])(),
                    flow_context(),
                ),
                false,
            ),
        );
    }

    public function test_a_bound_global_aggregate_with_input_emits_no_extra_row(): void
    {
        $groupBy = new GroupBy();
        $groupBy->aggregate(sum(ref('amount')), count(ref('amount')));

        $bound = GroupByAggregationProcessorMother::inMemory($groupBy)->bind(schema(int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static fn(): Generator => yield array_to_rows([
                    ['amount' => 10],
                    ['amount' => 20],
                ], schema(int_schema('amount'))))(),
                flow_context(),
            ),
            false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => 30.0, 'amount_count' => 2]], $result[0]->toArray());
    }

    public function test_output_builds_with_the_configured_backend(): void
    {
        $backend = new SpyBackend();
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        iterator_to_array(
            GroupByAggregationProcessorMother::inMemory($groupBy)->process(
                GroupedRows::categories(),
                flow_context(config_builder()->backend($backend)->build()),
            ),
            false,
        );

        static::assertGreaterThan(0, $backend->builders());
    }
}
