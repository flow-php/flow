<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{float_entry, int_entry, ref, str_entry, sum, window};
use Flow\ETL\{Row, Rows};
use PHPUnit\Framework\TestCase;

final class SumTest extends TestCase
{
    public function test_aggregation_sum_from_numeric_values() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(str_entry('int', '10')));
        $aggregator->aggregate(Row::create(str_entry('int', '20')));
        $aggregator->aggregate(Row::create(str_entry('int', '55')));
        $aggregator->aggregate(Row::create(str_entry('int', '25')));
        $aggregator->aggregate(Row::create(str_entry('not_int', null)));

        self::assertSame(
            110,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_sum_including_null_value() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(int_entry('int', 10)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 30)));
        $aggregator->aggregate(Row::create(str_entry('int', null)));

        self::assertSame(
            60,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_sum_with_float_result() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(float_entry('int', 10.25)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 305)));
        $aggregator->aggregate(Row::create(int_entry('int', 25)));

        self::assertSame(
            360.25,
            $aggregator->result()->value()
        );
    }

    public function test_window_function_sum_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1)),
            Row::create(int_entry('id', 2), int_entry('value', 1)),
            Row::create(int_entry('id', 3), int_entry('value', 1)),
            Row::create(int_entry('id', 4), int_entry('value', 1)),
            Row::create(int_entry('id', 5), int_entry('value', 1)),
        );

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        self::assertSame(15, $sum->apply($row1, $rows));
    }
}
