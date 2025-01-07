<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{count, float_entry, int_entry, ref, str_entry, window};
use function Flow\ETL\DSL\{row, rows};
use Flow\ETL\{Tests\FlowTestCase};

final class CountTest extends FlowTestCase
{
    public function test_aggregation_count_from_numeric_values() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '55')));
        $aggregator->aggregate(row(str_entry('int', '25')));
        $aggregator->aggregate(row(str_entry('not_int', null)));

        self::assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_count_with_float_result() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 305)));
        $aggregator->aggregate(row(int_entry('int', 25)));

        self::assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_when_row_does_not_have_entry() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', null)));
        $aggregator->aggregate(row(str_entry('test', null)));

        self::assertSame(
            4,
            $aggregator->result()->value()
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
