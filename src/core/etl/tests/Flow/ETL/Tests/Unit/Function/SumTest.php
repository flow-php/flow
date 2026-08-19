<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\WindowContextMother;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;

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

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        static::assertSame(110.0, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_sum_including_null_value(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(str_entry('int', null)), flow_context());

        static::assertSame(60, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_sum_with_float_result(): void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 305)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        static::assertSame(360.25, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(row(float_entry('value', 0.1)), flow_context());
        $aggregator->aggregate(row(float_entry('value', 0.2)), flow_context());

        static::assertSame(0.3, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_window_function_sum_of_decimal_fractions_uses_float_arithmetic_by_default(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), float_entry('value', 0.1)),
            row(int_entry('id', 2), float_entry('value', 0.2)),
        );

        $sum = sum(ref('value'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.1 + 0.2, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_aggregation_sum_of_floats_stays_float_when_sum_is_whole(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(row(float_entry('value', 2.5)), flow_context());
        $aggregator->aggregate(row(float_entry('value', 2.5)), flow_context());

        $result = $aggregator->result(flow_context(config())->entryFactory());

        static::assertSame(5.0, $result->value());
        static::assertSame('float', $result->definition()->type()->toString());
    }

    public function test_sum_over_int_column_stays_int(): void
    {
        $aggregator = sum(ref('value'));

        $aggregator->aggregate(row(int_entry('value', 1)), flow_context());
        $aggregator->aggregate(row(int_entry('value', 2)), flow_context());
        $aggregator->aggregate(row(int_entry('value', 3)), flow_context());
        $aggregator->aggregate(row(int_entry('value', 4)), flow_context());

        $result = $aggregator->result(flow_context(config())->entryFactory());

        static::assertSame(10, $result->value());
        static::assertSame('integer', $result->definition()->type()->toString());
    }

    public function test_exact_aggregation_sum_of_decimal_fractions(): void
    {
        $aggregator = sum(ref('value'), exact: true);

        $aggregator->aggregate(row(float_entry('value', 0.1)), flow_context());
        $aggregator->aggregate(row(float_entry('value', 0.2)), flow_context());

        static::assertSame(0.3, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_window_function_sum_on_partitioned_rows(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1)),
            row(int_entry('id', 2), int_entry('value', 1)),
            row(int_entry('id', 3), int_entry('value', 1)),
            row(int_entry('id', 4), int_entry('value', 1)),
            row(int_entry('id', 5), int_entry('value', 1)),
        );

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(15, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_of_decimal_fractions_in_exact_mode(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), float_entry('value', 0.1)),
            row(int_entry('id', 2), float_entry('value', 0.2)),
        );

        $sum = sum(ref('value'), exact: true)->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_exact_mode_from_column(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), float_entry('value', 0.1), bool_entry('is_exact', true)),
            row(int_entry('id', 2), float_entry('value', 0.2), bool_entry('is_exact', true)),
        );

        $sum = sum(ref('value'), exact: ref('is_exact'))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_exact_mode_from_literal(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), float_entry('value', 0.1)),
            row(int_entry('id', 2), float_entry('value', 0.2)),
        );

        $sum = sum(ref('value'), exact: lit(true))->over(window()->orderBy(ref('id')->desc()));

        static::assertSame(0.3, $sum->apply(WindowContextMother::forRow($row1, $rows)));
    }

    public function test_window_function_sum_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sum window function error:');

        $rows = rows($row1 = row(int_entry('id', 1)), row(int_entry('id', 2)));

        $sum = sum(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $sum->apply(WindowContextMother::forRow($row1, $rows, context: $context));
    }
}
