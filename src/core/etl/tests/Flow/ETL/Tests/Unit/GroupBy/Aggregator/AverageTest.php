<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use function Flow\ETL\DSL\entry;
use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregator\Average;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class AverageTest extends TestCase
{
    public function test_average_from_numeric_values() : void
    {
        $aggregator = new Average(entry('int'));

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '30')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            21.25,
            $aggregator->result()->value()
        );
    }

    public function test_average_including_null_value() : void
    {
        $aggregator = new Average('int');

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));

        $this->assertSame(
            20,
            $aggregator->result()->value()
        );
    }

    public function test_average_with_float_result() : void
    {
        $aggregator = new Average('int');

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            21.25,
            $aggregator->result()->value()
        );
    }

    public function test_average_with_integer_result() : void
    {
        $aggregator = new Average('int');

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 40)));

        $this->assertSame(
            25,
            $aggregator->result()->value()
        );
    }

    public function test_average_with_zero_result() : void
    {
        $aggregator = new Average('int');

        $this->assertSame(
            0,
            $aggregator->result()->value()
        );
    }
}
