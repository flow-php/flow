<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\count;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\window;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use PHPUnit\Framework\TestCase;

final class CountTest extends TestCase
{
    public function test_aggregation_count_from_numeric_values() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_count_with_float_result() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(Entry::float('int', 10.25)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 305)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_when_row_does_not_have_entry() : void
    {
        $aggregator = count(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));
        $aggregator->aggregate(Row::create(Entry::null('test')));

        $this->assertSame(
            4,
            $aggregator->result()->value()
        );
    }

    public function test_count_array_entry() : void
    {
        $this->assertSame(
            2,
            count(ref('array'))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
        $this->assertSame(
            2,
            count(ref('array'))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
        $this->assertTrue(
            ref('array')->count()->equals(lit(2))->eval(Row::create(Entry::array('array', ['foo', 'bar'])))
        );
    }

    public function test_count_string() : void
    {
        $this->assertNull(
            count(ref('string'))->eval(Row::create(Entry::string('string', 'foo')))
        );
    }

    public function test_window_function_count_on_partitioned_rows() : void
    {
        $rows = new Rows(
            $row1 = Row::create(Entry::int('id', 1), Entry::int('value', 1)),
            $row2 = Row::create(Entry::int('id', 2), Entry::int('value', 1)),
            Row::create(Entry::int('id', 3), Entry::int('value', 1)),
            Row::create(Entry::int('id', 4), Entry::int('value', 1)),
            Row::create(Entry::int('id', 1), Entry::int('value', 1)),
        );

        $count = count(ref('id'))->over(window()->orderBy(ref('id')->desc()));

        $this->assertSame(2, $count->apply($row1, $rows));
        $this->assertSame(1, $count->apply($row2, $rows));
    }
}
