<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\GroupBy\Aggregator;

use Flow\ETL\DSL\Entry;
use Flow\ETL\GroupBy\Aggregator\Sum;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class SumTest extends TestCase
{
    public function test_sum_from_numeric_values() : void
    {
        $aggregator = new Sum('int');

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

    public function test_sum_including_null_value() : void
    {
        $aggregator = new Sum('int');

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));

        $this->assertSame(
            60,
            $aggregator->result()->value()
        );
    }

    public function test_sum_with_float_result() : void
    {
        $aggregator = new Sum('int');

        $aggregator->aggregate(Row::create(Entry::float('int', 10.25)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 305)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            360.25,
            $aggregator->result()->value()
        );
    }
}
