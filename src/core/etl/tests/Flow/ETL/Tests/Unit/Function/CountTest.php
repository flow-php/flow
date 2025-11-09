<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{config, count, float_entry, flow_context, int_entry, ref, row, rows, str_entry, window};
use Flow\ETL\Tests\FlowTestCase;

final class CountTest extends FlowTestCase
{
    public function test_aggregation_count_from_numeric_values() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        self::assertSame(
            4,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_count_with_float_result() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 305)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 25)), flow_context());

        self::assertSame(
            4,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_count_without_reference() : void
    {
        $aggregator = count();

        $aggregator->aggregate(row(str_entry('int', '10')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '20')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '55')), flow_context());
        $aggregator->aggregate(row(str_entry('int', '25')), flow_context());
        $aggregator->aggregate(row(str_entry('not_int', null)), flow_context());

        self::assertSame(
            5,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_aggregation_when_row_does_not_have_entry() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 20)), flow_context());
        $aggregator->aggregate(row(int_entry('int', 30)), flow_context());
        $aggregator->aggregate(row(int_entry('int', null)), flow_context());
        $aggregator->aggregate(row(str_entry('test', null)), flow_context());

        self::assertSame(
            4,
            $aggregator->result(flow_context(config())->entryFactory())->value()
        );
    }

    public function test_window_function_count_on_partitioned_rows() : void
    {
        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1)), $row2 = row(int_entry('id', 2), int_entry('value', 1)), row(int_entry('id', 3), int_entry('value', 1)), row(int_entry('id', 4), int_entry('value', 1)), row(int_entry('id', 1), int_entry('value', 1)));

        $count = count(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        self::assertSame(2, $count->apply($row1, $rows));
        self::assertSame(1, $count->apply($row2, $rows));
    }
}
