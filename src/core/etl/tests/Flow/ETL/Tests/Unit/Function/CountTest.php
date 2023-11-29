<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\window;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CountTest extends TestCase
{
    public function test_aggregation_count_from_numeric_values() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(str_entry('int', '10')));
        $aggregator->aggregate(Row::create(str_entry('int', '20')));
        $aggregator->aggregate(Row::create(str_entry('int', '55')));
        $aggregator->aggregate(Row::create(str_entry('int', '25')));
        $aggregator->aggregate(Row::create(null_entry('not_int')));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_count_with_float_result() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(float_entry('int', 10.25)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 305)));
        $aggregator->aggregate(Row::create(int_entry('int', 25)));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_when_row_does_not_have_entry() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(int_entry('int', 10)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 30)));
        $aggregator->aggregate(Row::create(null_entry('int')));
        $aggregator->aggregate(Row::create(null_entry('test')));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_window_function_count_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(int_entry('id', 1), int_entry('value', 1)),
            $row2 = Row::create(int_entry('id', 2), int_entry('value', 1)),
            Row::create(int_entry('id', 3), int_entry('value', 1)),
            Row::create(int_entry('id', 4), int_entry('value', 1)),
            Row::create(int_entry('id', 1), int_entry('value', 1)),
        );

        $count = count(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        $this->assertSame(2, $count->apply($row1, $rows));
        $this->assertSame(1, $count->apply($row2, $rows));
    }
}
