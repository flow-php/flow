<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Operators\Processor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\AggregatingFunction;
use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\GroupBy\Aggregators;
use Flow\ETL\Operators\GroupBy\Pivot;
use Flow\ETL\Operators\GroupBy\PivotShape;
use Flow\ETL\Operators\Processor\PivotProcessor;
use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Storage\MemoryRowsStorage;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\Unit;
use Flow\ETL\Values\Rows;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\collect;
use function Flow\ETL\DSL\collect_unique;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\first;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\last;
use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\pivot_values;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\refs;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\string_agg;
use function Flow\ETL\DSL\sum;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_string;

final class PivotProcessorTest extends FlowTestCase
{
    public function test_a_bound_pivot_is_clustered_on_its_keys(): void
    {
        $distribution = type_instance_of(PivotProcessor::class)
            ->assert((new PivotProcessor(
                new GroupBy(
                    refs('name'),
                    new Aggregators(sum(ref('id'))),
                    new Pivot(ref('country'), pivot_values('USA')),
                ),
                config()->grouping,
            ))->bind(schema(str_schema('name'), int_schema('id'), str_schema('country')))->step)
            ->distribution();

        static::assertSame(['name'], $distribution?->keys);
        static::assertNull($distribution->partial);
        static::assertNull($distribution->final);
    }

    public function test_a_global_pivot_has_no_distribution(): void
    {
        static::assertNull(
            type_instance_of(PivotProcessor::class)
                ->assert((new PivotProcessor(
                    new GroupBy(
                        refs(),
                        new Aggregators(sum(ref('id'))),
                        new Pivot(ref('country'), pivot_values('USA')),
                    ),
                    config()->grouping,
                ))->bind(schema(int_schema('id'), str_schema('country')))->step)
                ->distribution(),
        );
    }

    public function test_bind_derives_the_group_by_columns_and_one_nullable_column_per_pivot_value(): void
    {
        static::assertEquals(
            schema(str_schema('date'), float_schema('norbert', nullable: true), float_schema('stloyd', nullable: true)),
            (new PivotProcessor(
                new GroupBy(
                    refs(ref('date')),
                    new Aggregators(sum(ref('contributions'))),
                    new Pivot(ref('user'), pivot_values('norbert', 'stloyd')),
                ),
                config()->grouping,
            ))->bind(schema(str_schema('date'), str_schema('user'), int_schema('contributions')))->output,
        );
    }

    public function test_bind_refuses_a_pivot_value_colliding_with_a_group_by_column(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Pivot value "date" collides with the group-by column of the same name');

        (new PivotProcessor(
            new GroupBy(
                refs(ref('date')),
                new Aggregators(sum(ref('contributions'))),
                new Pivot(ref('user'), pivot_values('date')),
            ),
            config()->grouping,
        ))->bind(schema(str_schema('date'), str_schema('user'), int_schema('contributions')));
    }

    public function test_bind_declares_the_pivot_columns_with_the_resolved_aggregate_type(): void
    {
        static::assertEquals(
            schema(str_schema('date'), int_schema('norbert', nullable: true), int_schema('stloyd', nullable: true)),
            (new PivotProcessor(
                new GroupBy(
                    refs(ref('date')),
                    new Aggregators(min(ref('contributions'))),
                    new Pivot(ref('user'), pivot_values('norbert', 'stloyd')),
                ),
                config()->grouping,
            ))->bind(schema(str_schema('date'), str_schema('user'), int_schema('contributions')))->output,
        );
    }

    /**
     * @return Generator<string, array{AggregatingFunction, string}>
     */
    public static function pivotAggregates(): Generator
    {
        yield 'min' => [min(ref('contributions')), 'integer'];
        yield 'max' => [max(ref('contributions')), 'integer'];
        yield 'first' => [first(ref('contributions')), 'integer'];
        yield 'last' => [last(ref('contributions')), 'integer'];
        yield 'collect' => [collect(ref('contributions')), 'list<integer>'];
        yield 'collect_unique' => [collect_unique(ref('contributions')), 'list<integer>'];
        yield 'sum' => [sum(ref('contributions')), 'float'];
        yield 'count' => [count(ref('contributions')), 'integer'];
        yield 'average' => [average(ref('contributions')), 'float'];
        yield 'string_agg' => [string_agg(ref('note')), 'string'];
    }

    #[DataProvider('pivotAggregates')]
    public function test_bind_resolves_every_pivot_aggregate(AggregatingFunction $aggregate, string $resolvedType): void
    {
        $output = (new PivotProcessor(
            new GroupBy(
                refs(ref('date')),
                new Aggregators($aggregate),
                new Pivot(ref('user'), pivot_values('norbert', 'stloyd')),
            ),
            config()->grouping,
        ))->bind(schema(
            str_schema('date'),
            str_schema('user'),
            int_schema('contributions'),
            str_schema('note'),
        ))->output;

        static::assertSame(['date', 'norbert', 'stloyd'], $output->names());

        foreach (['norbert', 'stloyd'] as $column) {
            static::assertSame($resolvedType, $output->get($column)->type()->toString());
            static::assertTrue($output->get($column)->isNullable());
        }
    }

    public function test_a_bound_processor_pivots_grouped_rows(): void
    {
        $input = schema(str_schema('date'), str_schema('user'), int_schema('contributions'));
        $bound = (new PivotProcessor(
            new GroupBy(
                refs(ref('date')),
                new Aggregators(sum(ref('contributions'))),
                new Pivot(ref('user'), pivot_values('norbert', 'stloyd')),
            ),
            config()->grouping,
        ))->bind($input);

        static::assertInstanceOf(PivotProcessor::class, $bound->step);

        $result = iterator_to_array(
            $bound->step->process(
                (static function () use ($input): Generator {
                    yield array_to_rows([
                        ['date' => '2024-01-01', 'user' => 'norbert', 'contributions' => 2],
                        ['date' => '2024-01-01', 'user' => 'stloyd', 'contributions' => 3],
                        ['date' => '2024-01-02', 'user' => 'norbert', 'contributions' => 5],
                    ], $input);
                })(),
                flow_context()->backend(),
            ),
            preserve_keys: false,
        );

        static::assertCount(1, $result);
        static::assertSame(
            [
                ['date' => '2024-01-01', 'norbert' => 2.0, 'stloyd' => 3.0],
                ['date' => '2024-01-02', 'norbert' => 5.0, 'stloyd' => null],
            ],
            $result[0]->toArray(),
        );
        static::assertEquals($bound->output, $result[0]->schema());
    }

    public function test_pivots_grouped_rows(): void
    {
        $result = iterator_to_array(
            (new PivotProcessor(
                new GroupBy(
                    refs(ref('date')),
                    new Aggregators(sum(ref('contributions'))),
                    new Pivot(ref('user'), pivot_values('norbert', 'stloyd')),
                ),
                config()->grouping,
            ))->process(
                (static function (): Generator {
                    yield array_to_rows(
                        [
                            ['date' => '2024-01-01', 'user' => 'norbert', 'contributions' => 2],
                            ['date' => '2024-01-01', 'user' => 'stloyd', 'contributions' => 3],
                            ['date' => '2024-01-02', 'user' => 'norbert', 'contributions' => 5],
                        ],
                        schema(str_schema('date'), str_schema('user'), int_schema('contributions')),
                    );
                })(),
                flow_context()->backend(),
            ),
            preserve_keys: false,
        );

        $pivoted = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $pivotRow) {
                $pivoted[type_string()->assert($pivotRow['date'])] = $pivotRow;
            }
        }

        static::assertSame(
            [
                '2024-01-01' => ['date' => '2024-01-01', 'norbert' => 2.0, 'stloyd' => 3.0],
                '2024-01-02' => ['date' => '2024-01-02', 'norbert' => 5.0, 'stloyd' => null],
            ],
            $pivoted,
        );
    }

    public function test_a_bound_aggregation_pivots_without_reading_a_schema_from_the_batches(): void
    {
        $groupBy = new GroupBy(
            refs(ref('product')),
            new Aggregators(min(ref('amount'))),
            new Pivot(ref('country'), pivot_values('USA', 'PL')),
        );

        $input = schema(str_schema('product'), str_schema('country'), int_schema('amount'));

        /** @var list<Rows> $batches */
        $batches = iterator_to_array((new PivotProcessor(
            $groupBy,
            config()->grouping,
            PivotShape::of($groupBy, $input),
        ))->process(
            (static function () use ($input): Generator {
                yield array_to_rows([['product' => 'Banana', 'country' => 'USA', 'amount' => 30]], $input);
                yield array_to_rows([['product' => 'Banana', 'country' => 'USA', 'amount' => 10]], $input);
            })(),
            flow_context(config())->backend(),
        ));

        static::assertSame([['product' => 'Banana', 'USA' => 10, 'PL' => null]], $batches[0]->toArray());
    }

    public function test_a_bound_aggregation_over_an_empty_stream_yields_nothing(): void
    {
        $groupBy = new GroupBy(
            refs(ref('product')),
            new Aggregators(sum(ref('amount'))),
            new Pivot(ref('country'), pivot_values('USA')),
        );

        static::assertSame(
            [],
            iterator_to_array((new PivotProcessor(
                $groupBy,
                config()->grouping,
                PivotShape::of($groupBy, schema(str_schema('product'), str_schema('country'), int_schema('amount'))),
            ))->process((static fn(): Generator => yield from [])(), flow_context(config())->backend())),
        );
    }

    public function test_it_splits_the_result_into_batches_of_the_requested_size(): void
    {
        $groupBy = new GroupBy(
            refs(ref('product')),
            new Aggregators(sum(ref('amount'))),
            new Pivot(ref('country'), pivot_values('USA')),
        );

        $input = schema(str_schema('product'), str_schema('country'), int_schema('amount'));

        static::assertCount(
            2,
            iterator_to_array((new PivotProcessor(
                $groupBy,
                new BucketingConfig(new MemoryRowsStorage(), 4, 2, Unit::fromMb(64)),
                PivotShape::of($groupBy, $input),
            ))->process(
                (static function () use ($input): Generator {
                    yield array_to_rows([
                        ['product' => 'Banana', 'country' => 'USA', 'amount' => 1],
                        ['product' => 'Apple', 'country' => 'USA', 'amount' => 2],
                        ['product' => 'Cherry', 'country' => 'USA', 'amount' => 3],
                    ], $input);
                })(),
                flow_context(config())->backend(),
            )),
        );
    }
}
