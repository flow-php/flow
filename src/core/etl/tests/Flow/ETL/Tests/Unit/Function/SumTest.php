<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\array_to_row;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

final class SumTest extends FlowTestCase
{
    public function test_references_is_null_when_exact_is_a_scalar_function(): void
    {
        static::assertNull(sum(ref('value'), exact: ref('flag'))->references());
    }

    public function test_references_returns_the_aggregated_reference(): void
    {
        static::assertEquals([ref('value')], sum(ref('value'))->references());
        static::assertEquals([ref('value')], sum(ref('value'), exact: true)->references());
    }

    public function test_aggregation_sum_from_numeric_values(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => '10'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '20'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '55'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => '25'], schema(str_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'not_int' => null,
        ], schema(str_schema('not_int', nullable: true))), flow_context());

        static::assertSame(110.0, $aggregator->value());
    }

    public function test_aggregation_sum_including_null_value(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 30], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row([
            'int' => null,
        ], schema(int_schema('int', nullable: true))), flow_context());

        static::assertSame(60.0, $aggregator->value());
    }

    public function test_aggregation_sum_with_float_result(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(array_to_row(['int' => 10.25], schema(float_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 20], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 305], schema(int_schema('int'))), flow_context());
        $aggregator->aggregate(array_to_row(['int' => 25], schema(int_schema('int'))), flow_context());

        static::assertSame(360.25, $aggregator->value());
    }

    public function test_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_row(['value' => 0.1], schema(float_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 0.2], schema(float_schema('value'))), flow_context());

        static::assertSame(0.30000000000000004, $aggregator->value());
    }

    public function test_window_function_sum_of_decimal_fractions_uses_float_arithmetic_by_default(): void
    {
        $rows = rows(
            schema(int_schema('id'), float_schema('value')),
            $row1 = array_to_row(['id' => 1, 'value' => 0.1], schema(int_schema('id'), float_schema('value'))),
            array_to_row(
                [
                    'id' => 2,
                    'value' => 0.2,
                ],
                schema(int_schema('id'), float_schema('value')),
            ),
        );

        $sum = sum(ref('value'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.1 + 0.2, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_aggregation_sum_of_floats_stays_float_when_sum_is_whole(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_row(['value' => 2.5], schema(float_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 2.5], schema(float_schema('value'))), flow_context());

        static::assertSame(5.0, $aggregator->value());
        static::assertSame('?float', $aggregator->returns()->toString());
    }

    public function test_sum_over_an_int_column_declares_float(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(array_to_row(['value' => 1], schema(int_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 2], schema(int_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 3], schema(int_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 4], schema(int_schema('value'))), flow_context());

        static::assertSame(10.0, $aggregator->value());
        static::assertSame('?float', $aggregator->returns()->toString());
    }

    public function test_a_malformed_exact_operand_throws_wrapped_as_a_sum_error(): void
    {
        $aggregator = sum(ref('value'), exact: ref('exact'));

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sum error:');

        $aggregator->aggregate(
            array_to_row(
                ['value' => 0.1, 'exact' => ['not' => 'a boolean']],
                schema(float_schema('value'), map_schema('exact', type_map(type_string(), type_string()))),
            ),
            flow_context(),
        );
    }

    public function test_exact_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'), exact: true);

        $aggregator->aggregate(array_to_row(['value' => 0.1], schema(float_schema('value'))), flow_context());
        $aggregator->aggregate(array_to_row(['value' => 0.2], schema(float_schema('value'))), flow_context());

        static::assertSame(0.3, $aggregator->value());
    }

    public function test_window_function_sum_on_partitioned_rows(): void
    {
        $rows = rows(
            schema(int_schema('id'), int_schema('value')),
            $row1 = array_to_row(['id' => 1, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 2, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 3, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 4, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
            array_to_row(['id' => 5, 'value' => 1], schema(int_schema('id'), int_schema('value'))),
        );

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(15, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_of_decimal_fractions_in_exact_mode(): void
    {
        $rows = rows(
            schema(int_schema('id'), float_schema('value')),
            $row1 = array_to_row(['id' => 1, 'value' => 0.1], schema(int_schema('id'), float_schema('value'))),
            array_to_row(
                [
                    'id' => 2,
                    'value' => 0.2,
                ],
                schema(int_schema('id'), float_schema('value')),
            ),
        );

        $sum = sum(ref('value'), exact: true)->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_exact_mode_from_column(): void
    {
        $rows = rows(
            schema(int_schema('id'), float_schema('value'), bool_schema('is_exact')),
            $row1 = array_to_row(
                ['id' => 1, 'value' => 0.1, 'is_exact' => true],
                schema(int_schema('id'), float_schema('value'), bool_schema('is_exact')),
            ),
            array_to_row(
                [
                    'id' => 2,
                    'value' => 0.2,
                    'is_exact' => true,
                ],
                schema(int_schema('id'), float_schema('value'), bool_schema('is_exact')),
            ),
        );

        $sum = sum(ref('value'), exact: ref('is_exact'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_exact_mode_from_literal(): void
    {
        $rows = rows(
            schema(int_schema('id'), float_schema('value')),
            $row1 = array_to_row(['id' => 1, 'value' => 0.1], schema(int_schema('id'), float_schema('value'))),
            array_to_row(
                [
                    'id' => 2,
                    'value' => 0.2,
                ],
                schema(int_schema('id'), float_schema('value')),
            ),
        );

        $sum = sum(ref('value'), exact: lit(true))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sum window function error:');

        $rows = rows(
            schema(int_schema('id')),
            $row1 = array_to_row(['id' => 1], schema(int_schema('id'))),
            array_to_row(['id' => 2], schema(int_schema('id'))),
        );

        $sum = sum(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $sum->apply(WindowContextMother::forRow($row1, $rows, context: $context));
    }

    public function test_with_children_rebuilds_the_aggregate_with_the_given_reference(): void
    {
        $aggregate = sum(ref('a'));

        static::assertEquals([ref('a')], $aggregate->children());

        $rebuilt = $aggregate->withChildren([ref('b')]);

        static::assertNotSame($aggregate, $rebuilt);
        static::assertEquals([ref('b')], $rebuilt->references());
    }

    public function test_with_children_carries_the_exact_operand_as_a_child(): void
    {
        $aggregate = sum(ref('a'), lit(true));

        static::assertCount(2, $aggregate->children());
        static::assertNull($aggregate->withChildren([ref('b'), lit(false)])->references());
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
}
