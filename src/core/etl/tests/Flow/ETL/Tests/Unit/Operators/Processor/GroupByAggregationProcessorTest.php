<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Foundation\NativePHPRandomValueGenerator;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\DeclaredPivotValues;
use Flow\ETL\Operators\GroupBy\MergedAggregate;
use Flow\ETL\Operators\GroupBy\PartialAggregate;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\Context\GroupByContext;
use Flow\ETL\Tests\Context\GroupedRows;
use Flow\ETL\Tests\Double\FixedRandomValueGenerator;
use Flow\ETL\Tests\Double\ReportedMemoryBackend;
use Flow\ETL\Tests\Double\SpyBackend;
use Flow\ETL\Tests\Double\SpyRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\GroupByAggregationProcessorMother;
use Flow\ETL\Values\Memory\Unit;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\hash_group_by;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function iterator_to_array;
use function memory_get_usage;
use function mt_rand;
use function mt_srand;
use function serialize;

final class GroupByAggregationProcessorTest extends FlowTestCase
{
    public function test_a_bound_group_by_is_clustered_on_its_keys_with_partial_and_merged_aggregates(): void
    {
        $distribution = type_instance_of(GroupByAggregationProcessor::class)
            ->assert((new GroupByAggregationProcessor(
                new GroupBy(refs('country'), new Aggregators(sum(ref('amount')))),
                config()->grouping,
                new NativePHPRandomValueGenerator(),
            ))->bind(schema(str_schema('country'), int_schema('amount')))->step)
            ->distribution();

        static::assertNotNull($distribution);
        static::assertSame(['country'], $distribution->keys);
        static::assertNull($distribution->rightKeys);
        static::assertSame(
            [PartialAggregate::class],
            array_map(static fn(object $aggregate): string => $aggregate::class, [...type_instance_of(
                GroupByAggregationProcessor::class,
            )
                ->assert($distribution->partial)
                ->groupBy->aggregations()]),
        );
        static::assertSame(
            [MergedAggregate::class],
            array_map(static fn(object $aggregate): string => $aggregate::class, [...type_instance_of(
                GroupByAggregationProcessor::class,
            )
                ->assert($distribution->final)
                ->groupBy->aggregations()]),
        );
    }

    public function test_a_bound_global_group_by_meets_in_one_process(): void
    {
        $distribution = type_instance_of(GroupByAggregationProcessor::class)
            ->assert((new GroupByAggregationProcessor(
                new GroupBy(refs(), new Aggregators(sum(ref('amount')))),
                config()->grouping,
                new NativePHPRandomValueGenerator(),
            ))->bind(schema(int_schema('amount')))->step)
            ->distribution();

        static::assertNotNull($distribution);
        static::assertSame([], $distribution->keys);
        static::assertInstanceOf(GroupByAggregationProcessor::class, $distribution->partial);
        static::assertInstanceOf(GroupByAggregationProcessor::class, $distribution->final);
    }

    public function test_an_unbound_group_by_has_no_distribution(): void
    {
        static::assertNull(
            (new GroupByAggregationProcessor(
                new GroupBy(refs('country'), new Aggregators(sum(ref('amount')))),
                config()->grouping,
                new NativePHPRandomValueGenerator(),
            ))->distribution(),
        );
    }

    public function test_a_pivoting_group_by_has_no_distribution(): void
    {
        static::assertNull(
            type_instance_of(GroupByAggregationProcessor::class)
                ->assert((new GroupByAggregationProcessor(
                    new GroupBy(
                        refs('name'),
                        new Aggregators(sum(ref('id'))),
                        new Pivot(ref('country'), new DeclaredPivotValues('USA')),
                    ),
                    config()->grouping,
                    new NativePHPRandomValueGenerator(),
                ))->bind(schema(str_schema('name'), int_schema('id'), str_schema('country')))->step)
                ->distribution(),
        );
    }

    public function test_a_processor_with_an_undescribable_random_value_generator_does_not_describe(): void
    {
        static::assertNull(
            (new GroupByAggregationProcessor(
                new GroupBy(refs('id')),
                config()->grouping,
                new FixedRandomValueGenerator('fixed'),
            ))->describe(),
        );
    }

    public static function random_datasets(): Generator
    {
        for ($seed = 1; $seed <= 50; $seed++) {
            yield "seed {$seed}" => [$seed];
        }
    }

    public function test_under_the_limit_it_aggregates_in_one_pass_without_touching_the_buckets(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            ['a' => 30.0, 'b' => 15.0],
            GroupedRows::sums(GroupByAggregationProcessorMother::inMemory(
                new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
                $storage,
            )->process(GroupedRows::categories(), flow_context()->backend())),
        );
        static::assertSame([], $storage->appendedRows());
    }

    public function test_past_the_limit_the_rest_is_partitioned_and_the_result_is_the_same(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            ['a' => 30.0, 'b' => 15.0],
            GroupedRows::sums(GroupByAggregationProcessorMother::with(
                new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
                $storage,
                Unit::fromBytes(1),
                batchSize: 1,
            )->process(GroupedRows::categories(), flow_context()->backend())),
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
        $groupBy = new GroupBy(
            refs(ref('k')),
            new Aggregators(
                sum(ref('v')),
                count(ref('v')),
                min(ref('v')),
                max(ref('v')),
                first(ref('v')),
                last(ref('v')),
                collect(ref('v')),
            ),
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

        $storage = new SpyRowsStorage(new MemoryRowsStorage());
        static::assertSame(
            serialize(GroupedRows::byKey(
                GroupByAggregationProcessorMother::inMemory($groupBy)->process(
                    (static fn(): Generator => yield from $batches)(),
                    flow_context()->backend(),
                ),
                'k',
            )),
            serialize(GroupedRows::byKey(
                GroupByAggregationProcessorMother::with(
                    $groupBy,
                    $storage,
                    Unit::fromBytes(memory_get_usage(false) + 500_000_000),
                    batchSize: 1,
                )->process($input, flow_context(config_builder()->backend($backend)->build())->backend()),
                'k',
            )),
        );
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
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        static::assertSame(
            [
                's:1:"a";' => ['k' => 'a', 'v_first' => 1, 'v_last' => 5, 'v_collection' => [1, 3, 5], 'v_sum' => 9.0],
                's:1:"b";' => ['k' => 'b', 'v_first' => 2, 'v_last' => 2, 'v_collection' => [2], 'v_sum' => 2.0],
                's:1:"c";' => ['k' => 'c', 'v_first' => 4, 'v_last' => 4, 'v_collection' => [4], 'v_sum' => 4.0],
            ],
            GroupedRows::byKey(
                GroupByAggregationProcessorMother::with(
                    new GroupBy(
                        refs(ref('k')),
                        new Aggregators(first(ref('v')), last(ref('v')), collect(ref('v')), sum(ref('v'))),
                    ),
                    $storage,
                    Unit::fromBytes(memory_get_usage(false) + 500_000_000),
                    batchSize: 1,
                )->process($input, flow_context(config_builder()->backend($backend)->build())->backend()),
                'k',
            ),
        );
        static::assertNotSame([], $storage->appendedRows());
    }

    public function test_bind_derives_the_group_by_columns_followed_by_the_aggregations(): void
    {
        static::assertEquals(
            schema(str_schema('category'), float_schema('amount_sum', nullable: true)),
            GroupByAggregationProcessorMother::inMemory(
                new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            )->bind(schema(str_schema('category'), int_schema('amount'), str_schema('dropped')))->output,
        );
    }

    public function test_bind_refuses_a_group_by_column_missing_from_the_input(): void
    {
        $this->expectException(SchemaDefinitionNotFoundException::class);

        GroupByAggregationProcessorMother::inMemory(
            new GroupBy(refs(ref('missing')), new Aggregators(sum(ref('amount')))),
        )->bind(schema(int_schema('amount')));
    }

    public function test_handles_an_empty_stream(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                GroupByAggregationProcessorMother::inMemory(
                    new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
                )->process((static fn(): Generator => yield from [])(), flow_context()->backend()),
                false,
            ),
        );
    }

    public function test_a_bound_global_aggregate_over_an_empty_stream_emits_one_row_of_defaults(): void
    {
        $bound = GroupByAggregationProcessorMother::inMemory(
            new GroupBy(refs(), new Aggregators(sum(ref('amount')), count(ref('amount')))),
        )->bind(schema(int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process((static fn(): Generator => yield from [])(), flow_context()->backend()),
            false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => null, 'amount_count' => 0]], $result[0]->toArray());
        static::assertEquals($bound->output, $result[0]->schema());
    }

    public function test_a_bound_keyed_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        $bound = GroupByAggregationProcessorMother::inMemory(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
        )->bind(schema(str_schema('category'), int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        static::assertSame(
            [],
            iterator_to_array(
                $bound->step->process((static fn(): Generator => yield from [])(), flow_context()->backend()),
                false,
            ),
        );
    }

    public function test_an_unbound_global_aggregate_over_an_empty_stream_emits_nothing(): void
    {
        static::assertSame(
            [],
            iterator_to_array(
                GroupByAggregationProcessorMother::inMemory(
                    new GroupBy(refs(), new Aggregators(sum(ref('amount')), count(ref('amount')))),
                )->process((static fn(): Generator => yield from [])(), flow_context()->backend()),
                false,
            ),
        );
    }

    public function test_a_bound_global_aggregate_with_input_emits_no_extra_row(): void
    {
        $bound = GroupByAggregationProcessorMother::inMemory(
            new GroupBy(refs(), new Aggregators(sum(ref('amount')), count(ref('amount')))),
        )->bind(schema(int_schema('amount')));

        static::assertInstanceOf(GroupByAggregationProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static fn(): Generator => yield array_to_rows([
                    ['amount' => 10],
                    ['amount' => 20],
                ], schema(int_schema('amount'))))(),
                flow_context()->backend(),
            ),
            false,
        );

        static::assertCount(1, $result);
        static::assertSame([['amount_sum' => 30.0, 'amount_count' => 2]], $result[0]->toArray());
    }

    public function test_output_builds_with_the_configured_backend(): void
    {
        $backend = new SpyBackend();

        iterator_to_array(
            GroupByAggregationProcessorMother::inMemory(
                new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            )->process(
                GroupedRows::categories(),
                flow_context(config_builder()->backend($backend)->build())->backend(),
            ),
            false,
        );

        static::assertGreaterThan(0, $backend->builders());
    }

    public function test_a_date_key_aggregated_before_the_limit_is_merged_with_its_rows_after_it(): void
    {
        $schema = schema(date_schema('k'), int_schema('v'));
        $backend = new ReportedMemoryBackend();
        $input = (static function () use ($schema, $backend): Generator {
            yield array_to_rows([
                ['k' => new DateTimeImmutable('2026-01-01'), 'v' => 1],
                ['k' => new DateTimeImmutable('2026-01-02'), 'v' => 2],
            ], $schema);
            $backend->reported = 1_000_000_000_000;

            yield array_to_rows([['k' => new DateTimeImmutable('2026-01-01'), 'v' => 3]], $schema);
            yield array_to_rows([['k' => new DateTimeImmutable('2026-01-02'), 'v' => 4]], $schema);
        })();
        $output = [];

        foreach (GroupByAggregationProcessorMother::with(
            new GroupBy(refs(ref('k')), new Aggregators(sum(ref('v')))),
            new SpyRowsStorage(new MemoryRowsStorage()),
            Unit::fromBytes(memory_get_usage(false) + 500_000_000),
            batchSize: 1,
        )->process($input, flow_context(config_builder()->backend($backend)->build())->backend()) as $batch) {
            $output = array_merge($output, $batch->toArray());
        }

        static::assertEqualsCanonicalizing(
            [
                ['k' => new DateTimeImmutable('2026-01-01'), 'v_sum' => 4.0],
                ['k' => new DateTimeImmutable('2026-01-02'), 'v_sum' => 6.0],
            ],
            $output,
        );
    }

    public function test_groups_across_multiple_batches(): void
    {
        $result = GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryRowsStorage()))->build()),
            array_to_rows([['category' => 'a', 'amount' => 10]], schema(str_schema('category'), int_schema('amount'))),
            array_to_rows([['category' => 'a', 'amount' => 20]], schema(str_schema('category'), int_schema('amount'))),
            array_to_rows([['category' => 'b', 'amount' => 15]], schema(str_schema('category'), int_schema('amount'))),
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[type_string()->assert($groupRow['category'])] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(['a' => 30.0, 'b' => 15.0], $aggregated);
    }

    public function test_aggregates_each_bucket_separately_with_single_bucket(): void
    {
        $result = GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            flow_context(
                config_builder()->groupBy(hash_group_by()->storage(new MemoryRowsStorage())->bucketsCount(1))->build(),
            ),
            array_to_rows(
                [
                    ['category' => 'a', 'amount' => 10],
                    ['category' => 'b', 'amount' => 15],
                    ['category' => 'a', 'amount' => 20],
                ],
                schema(str_schema('category'), int_schema('amount')),
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
        $batches = [];

        for ($id = 0; $id < 20; $id++) {
            $batches[] = array_to_rows(
                [['id' => $id, 'amount' => 1], ['id' => $id, 'amount' => 2]],
                schema(int_schema('id'), int_schema('amount')),
            );
        }

        $result = GroupByContext::aggregate(
            new GroupBy(refs(ref('id')), new Aggregators(sum(ref('amount')))),
            flow_context(
                config_builder()
                    ->groupBy(
                        hash_group_by()
                            ->storage(new MemoryRowsStorage())
                            ->memoryLimit(Unit::fromBytes(1))
                            ->bucketsCount(4),
                    )
                    ->build(),
            ),
            ...$batches,
        );

        static::assertArrayHasKey(1, $result, 'more than one batch');

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[type_integer()->assert($groupRow['id'])] = $groupRow['amount_sum'];
            }
        }

        ksort($aggregated);

        static::assertSame(array_fill(0, 20, 3.0), $aggregated);
    }

    public function test_removes_storage_buckets_after_last_bucket(): void
    {
        $storage = new SpyRowsStorage(new MemoryRowsStorage());

        GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
            flow_context(
                config_builder()
                    ->groupBy(hash_group_by()->storage($storage)->memoryLimit(Unit::fromBytes(1))->batchSize(1))
                    ->build(),
            ),
            array_to_rows([['category' => 'a', 'amount' => 10]], schema(str_schema('category'), int_schema('amount'))),
            array_to_rows([['category' => 'b', 'amount' => 15]], schema(str_schema('category'), int_schema('amount'))),
        );

        static::assertNotSame([], $storage->readBucketIds());
        static::assertSame([], $storage->liveBucketIds());
    }

    public function test_groups_rows_missing_group_column_under_null(): void
    {
        $result = GroupByContext::aggregate(
            new GroupBy(refs(ref('category')), new Aggregators(count())),
            flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryRowsStorage()))->build()),
            array_to_rows(
                [['category' => 'a', 'amount' => 10], ['amount' => 15], ['amount' => 20]],
                schema(str_schema('category', nullable: true), int_schema('amount')),
            ),
        );

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[type_string()->assert($groupRow['category'] ?? '__null__')] = $groupRow['_count'];
            }
        }

        ksort($aggregated);

        static::assertSame(['__null__' => 2, 'a' => 1], $aggregated);
    }

    public function test_missing_aggregated_column_is_treated_as_null_in_strict_mode(): void
    {
        static::assertSame(
            10.0,
            GroupByContext::aggregate(
                new GroupBy(refs(ref('category')), new Aggregators(sum(ref('amount')))),
                flow_context(config_builder()->groupBy(hash_group_by()->storage(new MemoryRowsStorage()))->build()),
                array_to_rows(
                    [['category' => 'a', 'amount' => 10], ['category' => 'a']],
                    schema(str_schema('category'), int_schema('amount', nullable: true)),
                ),
            )[0]
                ->column('amount_sum')
                ->value(0),
        );
    }
}
