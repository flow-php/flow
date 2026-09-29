<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

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

final class SumTest extends FlowTestCase
{
    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], sum(ref('value'))->references());
        static::assertEquals([ref('value')], sum(ref('value'), exact: true)->references());
    }

    public function test_aggregation_sum_from_numeric_values(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => '10']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '20']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '55']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => '25']], schema(str_schema('int'))), [0], flow_context());
        $aggregator->aggregate(
            array_to_rows([[
                'not_int' => null,
            ]], schema(str_schema('not_int', nullable: true))),
            [0],
            flow_context(),
        );

        static::assertSame(110.0, $aggregator->value());
    }

    public function test_aggregation_sum_including_null_value(): void
    {
        $aggregator = sum(ref('int'));

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

        static::assertSame(60.0, $aggregator->value());
    }

    public function test_aggregation_sum_with_float_result(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(array_to_rows([['int' => 10.25]], schema(float_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 20]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 305]], schema(int_schema('int'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['int' => 25]], schema(int_schema('int'))), [0], flow_context());

        static::assertSame(360.25, $aggregator->value());
    }

    public function test_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_rows([['value' => 0.1]], schema(float_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 0.2]], schema(float_schema('value'))), [0], flow_context());

        static::assertSame(0.30000000000000004, $aggregator->value());
    }

    public function test_ten_tenths_sum_to_one_on_the_ieee_path(): void
    {
        $aggregator = sum(ref('value'));
        $aggregator->aggregate(array_to_rows(array_fill(0, 10, [
            'value' => 0.1,
        ]), schema(float_schema('value'))), range(0, 9), flow_context());

        // plain IEEE addition drifts to 0.9999999999999999
        static::assertSame(1.0, $aggregator->value());
    }

    public function test_window_function_sum_of_decimal_fractions_uses_float_arithmetic_by_default(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'value' => 0.1],
                [
                    'id' => 2,
                    'value' => 0.2,
                ],
            ],
            schema(int_schema('id'), float_schema('value')),
        );

        $sum = sum(ref('value'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.1 + 0.2, $sum->apply(WindowContextMother::atIndex($rows, 0)));
    }

    public function test_aggregation_sum_of_floats_stays_float_when_sum_is_whole(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_rows([['value' => 2.5]], schema(float_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 2.5]], schema(float_schema('value'))), [0], flow_context());

        static::assertSame(5.0, $aggregator->value());
        static::assertSame('?float', $aggregator->returns()->toString());
    }

    public function test_sum_over_an_int_column_declares_float(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_rows([['value' => 1]], schema(int_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 2]], schema(int_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 3]], schema(int_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 4]], schema(int_schema('value'))), [0], flow_context());

        static::assertSame(10.0, $aggregator->value());
        static::assertSame('?float', $aggregator->returns()->toString());
    }

    public function test_exact_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'), exact: true);

        $aggregator->aggregate(array_to_rows([['value' => 0.1]], schema(float_schema('value'))), [0], flow_context());
        $aggregator->aggregate(array_to_rows([['value' => 0.2]], schema(float_schema('value'))), [0], flow_context());

        static::assertSame(0.3, $aggregator->value());
    }

    public function test_window_function_sum_on_partitioned_rows(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'value' => 1],
                ['id' => 2, 'value' => 1],
                ['id' => 3, 'value' => 1],
                ['id' => 4, 'value' => 1],
                ['id' => 5, 'value' => 1],
            ],
            schema(int_schema('id'), int_schema('value')),
        );

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(15, $sum->apply(WindowContextMother::atIndex($rows, 0)));
    }

    public function test_window_function_sum_of_decimal_fractions_in_exact_mode(): void
    {
        $rows = array_to_rows(
            [
                ['id' => 1, 'value' => 0.1],
                [
                    'id' => 2,
                    'value' => 0.2,
                ],
            ],
            schema(int_schema('id'), float_schema('value')),
        );

        $sum = sum(ref('value'), exact: true)->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::atIndex($rows, 0)));
    }

    public function test_window_function_sum_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sum window function error:');

        $rows = array_to_rows([['id' => 1], ['id' => 2]], schema(int_schema('id')));

        $sum = sum(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $sum->apply(WindowContextMother::atIndex($rows, 0, context: $context));
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = sum(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_output_name_is_not_affected_by_a_shared_reference(): void
    {
        $shared = ref('value');
        $aggregate = sum($shared);
        $sibling = sum($shared->as('renamed'));

        static::assertSame('value_sum', $aggregate->outputName());
        static::assertSame('renamed', $sibling->outputName());
        static::assertSame('value_sum', $aggregate->outputName());
    }

    public function test_sum_of_nothing_is_null(): void
    {
        static::assertNull(sum(ref('value'))->value());
    }

    public function test_aggregate_reads_only_given_indices(): void
    {
        $aggregator = sum(ref('v'));
        $aggregator->aggregate(
            array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
                0.5,
                1.25,
                2.0,
                3.0,
            ]), schema(float_schema('v'))),
            [1, 3],
            flow_context(),
        );

        static::assertSame(4.25, $aggregator->value());
    }

    public function test_merge_of_two_halves_equals_one_pass(): void
    {
        $rows = array_to_rows(array_map(static fn(mixed $v): array => ['v' => $v], [
            0.5,
            1.25,
            2.0,
            3.0,
        ]), schema(float_schema('v')));
        $onePass = sum(ref('v'));
        $onePass->aggregate($rows, [0, 1, 2, 3], flow_context());
        $left = sum(ref('v'));
        $left->aggregate($rows, [0, 1], flow_context());
        $right = sum(ref('v'));
        $right->aggregate($rows, [2, 3], flow_context());

        $left->merge($right, flow_context());

        static::assertSame($onePass->value(), $left->value());
    }

    public function test_merge_refuses_another_class(): void
    {
        $this->expectException(InvalidArgumentException::class);

        sum(ref('v'))->merge(average(ref('v')), flow_context());
    }

    public function test_merge_of_an_empty_half_keeps_null(): void
    {
        $left = sum(ref('v'));
        $left->merge(sum(ref('v')), flow_context());

        static::assertNull($left->value());
    }

    public function test_exact_merge_of_decimal_fractions(): void
    {
        $rows = array_to_rows([['v' => 0.1], ['v' => 0.2]], schema(float_schema('v')));
        $left = sum(ref('v'), exact: true);
        $left->aggregate($rows, [0], flow_context());
        $right = sum(ref('v'), exact: true);
        $right->aggregate($rows, [1], flow_context());

        $left->merge($right, flow_context());

        static::assertSame(0.3, $left->value());
    }
}
