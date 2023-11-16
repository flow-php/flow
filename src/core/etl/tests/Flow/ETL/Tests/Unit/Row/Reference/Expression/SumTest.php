<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\sum;
use function Flow\ETL\DSL\window;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class SumTest extends TestCase
{
    public function test_aggregation_sum_from_numeric_values() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            110,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_sum_including_null_value() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));

        $this->assertSame(
            60,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_sum_with_float_result() : void
    {
        $aggregator = sum(ref('int'));

        $aggregator->aggregate(Row::create(Entry::float('int', 10.25)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 305)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            360.25,
            $aggregator->result()->value()
        );
    }

    public function test_window_function_sum_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            Row::create(Entry::int('id', 2), Entry::int('value', 1)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1)),
            Row::create(Entry::int('id', 5), Entry::int('value', 1)),
        );

        $sum = sum(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        $this->assertSame(15, $sum->apply($row1, $rows));
    }
}
