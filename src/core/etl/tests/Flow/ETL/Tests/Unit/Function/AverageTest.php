<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\average;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\window;

final class AverageTest extends FlowTestCase
{
    public function test_aggregation_average_from_numeric_values(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '30')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        static::assertSame(21.25, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_average_including_null_value(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', null)), flow_context());

        static::assertSame(20, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_average_with_float_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        static::assertSame(21.25, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_average_with_integer_result(): void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 40)), flow_context());

        static::assertSame(25, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_aggregation_average_with_zero_result(): void
    {
        $aggregator = average(ref('int'));

        static::assertSame(0, $aggregator->result(flow_context(config())->entryFactory())->value());
    }

    public function test_window_function_average_on_partitioned_rows(): void
    {
        $rows = rows(
            $row1 = row(int_entry('id', 1), int_entry('value', 1)),
            row(int_entry('id', 2), int_entry('value', 100)),
            row(int_entry('id', 3), int_entry('value', 25)),
            row(int_entry('id', 4), int_entry('value', 64)),
            row(int_entry('id', 5), int_entry('value', 23)),
        );

        $avg = average(ref('value'))->over(window()->orderBy(ref('value')));

        static::assertSame(42.6, $avg->apply($row1, $rows, flow_context()));
    }

    public function test_window_function_average_with_missing_reference_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Average window function error:');

        $rows = rows($row1 = row(int_entry('id', 1)), row(int_entry('id', 2)));

        $avg = average(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $avg->apply($row1, $rows, $context);
    }
}
