<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, float_entry, flow_context, int_entry, ref, row, rows, str_entry, sum, window};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Tests\FlowTestCase;

final class SumTest extends FlowTestCase
{
    public function test_aggregation_sum_from_numeric_values() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        self::assertSame(
            110,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_sum_including_null_value() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(str_entry('int', null)), flow_context());

        self::assertSame(
            60,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_sum_with_float_result() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 305)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        self::assertSame(
            360.25,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_window_function_sum_on_partitioned_rows() : void
    {
        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1)), row(int_entry('id', 2), int_entry('value', 1)), row(int_entry('id', 3), int_entry('value', 1)), row(int_entry('id', 4), int_entry('value', 1)), row(int_entry('id', 5), int_entry('value', 1)));

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        self::assertSame(15, $sum->apply($row1, $rows, flow_context()));
    }

    public function test_window_function_sum_with_missing_reference_in_strict_mode() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Sum window function error:');

        $rows = rows($row1 = row(int_entry('id', 1)), row(int_entry('id', 2)));

        $sum = sum(ref('missing_column'))->over(window()->orderBy(ref('id')));

        $context = flow_context(config());
        $context->functions()->setMode(ExecutionMode::STRICT);

        $sum->apply($row1, $rows, $context);
    }
}
