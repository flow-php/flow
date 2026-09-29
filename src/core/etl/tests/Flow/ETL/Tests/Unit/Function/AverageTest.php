<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;
use PHPUnit\Framework\Attributes\TestWith;

use function array_fill;
use function array_map;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function range;

final class AverageTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('int')], average(ref('int'))->references());
    }

    public function test_aggregation_average_from_numeric_values(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => '10']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '20']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '30']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '25']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context(),
        );

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_including_null_value(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(
            array_to_rows([[
                'int' => null,
            ]], schema(int_schema('int', nullable: true))),
            [0],
            flow_context(),
        );

        static::assertSame(20.0, $aggregator->value());
    }

    public function test_aggregation_average_with_float_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 25]], schema(int_schema('int'))), [0], flow_context());

        static::assertSame(21.25, $aggregator->value());
    }

    public function test_aggregation_average_with_integer_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 30]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 40]], schema(int_schema('int'))), [0], flow_context());

        static::assertSame(25.0, $aggregator->value());
    }

    public function test_aggregation_average_of_nothing_is_null(): void
    {
        $aggregator = average(ref('int'));

        static::assertNull($aggregator->value());
    }

    public function test_window_function_average_on_partitioned_rows(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'value' => 1],
                ['id' => 2, 'value' => 100],
                ['id' => 3, 'value' => 25],
                ['id' => 4, 'value' => 64],
                ['id' => 5, 'value' => 23],
            ],
            schema(int_schema('id'), int_schema('value')),
        );

        $avg = average(ref('value'))->over(window()->orderBy(ref('value')));

        static::assertSame(42.6, $avg->apply(WindowContextMother::atIndex($rows, 0)));
    }

    public function test_window_function_average_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Average window function error:');

        $rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));

        $avg = average(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $avg->apply(WindowContextMother::atIndex($rows, 0, context: $context));
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = average(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = average(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
                0.5,
                1.25,
                2.0,
                3.0,
            ]), schema(float_schema('v'))),
            [0, 2],
            flow_context(),
        );

        static::assertSame(1.25, $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            0.5,
            1.25,
            2.0,
            3.0,
        ]), schema(float_schema('v')));
        $onePass = average(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = average(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = average(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        average(ref('v'))->merge(sum(ref('v')), flow_context());
    }

    #[TestWith([false])]
    #[TestWith([true])]
    public function test_ten_tenths_average_to_a_tenth(bool $exact): void
    {
        $aggregator = average(ref('v'), 17, exact: $exact);
        $aggregator->aggregate(array_to_rows(array_fill(0, 10, [
            'v' => 0.1,
        ]), schema(float_schema('v'))), range(0, 9), flow_context());

        // naive IEEE summation would give 0.09999999999999999
        static::assertSame(0.1, $aggregator->value());
    }

    public function test_merged_partial_averages_keep_their_compensation(): void
    {
        $left = average(ref('v'), 17);
        $right = average(ref('v'), 17);
        $rows = array_to_rows(array_fill(0, 5, ['v' => 0.1]), schema(float_schema('v')));
        $left->aggregate($rows, range(0, 4), flow_context());
        $right->aggregate($rows, range(0, 4), flow_context());

        $left->merge($right, flow_context());

        static::assertSame(0.1, $left->value());
    }
}
