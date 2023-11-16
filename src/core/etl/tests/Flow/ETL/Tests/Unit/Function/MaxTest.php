<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class MaxTest extends TestCase
{
    public function test_aggregation_max_from_numeric_values() : void
    {
        $aggregator = \Flow\ETL\DSL\max(ref('int'));

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            55,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_including_null_value() : void
    {
        $aggregator = \Flow\ETL\DSL\max(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));

        $this->assertSame(
            30,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_with_float_result() : void
    {
        $aggregator = \Flow\ETL\DSL\max(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::float('int', 30.5)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            30.5,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_with_integer_result() : void
    {
        $aggregator = \Flow\ETL\DSL\max(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 40)));

        $this->assertSame(
            40,
            $aggregator->result()->value()
        );
    }
}
