<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class MinTest extends TestCase
{
    public function test_aggregation_min_from_numeric_values() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(Entry::string('int', '10')));
        $aggregator->aggregate(Row::create(Entry::string('int', '20')));
        $aggregator->aggregate(Row::create(Entry::string('int', '55')));
        $aggregator->aggregate(Row::create(Entry::string('int', '25')));
        $aggregator->aggregate(Row::create(Entry::null('not_int')));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_including_null_value() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::null('int')));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_float_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(Entry::float('int', 10.25)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 305)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 25)));

        $this->assertSame(
            10.25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_integer_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(Entry::integer('int', 10)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 20)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 30)));
        $aggregator->aggregate(Row::create(Entry::integer('int', 40)));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }
}
