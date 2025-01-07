<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{average, int_entry, ref, str_entry, window};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class AverageTest extends FlowTestCase
{
    public function test_aggregation_average_from_numeric_values() : void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '30')));
        $aggregator->aggregate(row(str_entry('int', '25')));
        $aggregator->aggregate(row(str_entry('not_int', null)));

        self::assertSame(
            21.25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_average_including_null_value() : void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', null)));

        self::assertSame(
            20,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_average_with_float_result() : void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', 25)));

        self::assertSame(
            21.25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_average_with_integer_result() : void
    {
        $aggregator = average(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', 40)));

        self::assertSame(
            25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_average_with_zero_result() : void
    {
        $aggregator = average(ref('int'));

        self::assertSame(
            0,
            $aggregator->result()->value()
        );
    }

    public function test_window_function_average_on_partitioned_rows() : void
    {
        $rows = rows($row1 = row(int_entry('id', 1), int_entry('value', 1)), row(int_entry('id', 2), int_entry('value', 100)), row(int_entry('id', 3), int_entry('value', 25)), row(int_entry('id', 4), int_entry('value', 64)), row(int_entry('id', 5), int_entry('value', 23)));

        $avg = average(ref('value'))->over(window()->orderBy(ref('value')));

        self::assertSame(42.6, $avg->apply($row1, $rows));
    }
}
