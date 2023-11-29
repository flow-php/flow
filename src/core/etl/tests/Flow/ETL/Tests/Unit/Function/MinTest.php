<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\min;
use function Flow\ETL\DSL\null_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class MinTest extends TestCase
{
    public function test_aggregation_min_from_numeric_values() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(str_entry('int', '10')));
        $aggregator->aggregate(Row::create(str_entry('int', '20')));
        $aggregator->aggregate(Row::create(str_entry('int', '55')));
        $aggregator->aggregate(Row::create(str_entry('int', '25')));
        $aggregator->aggregate(Row::create(null_entry('not_int')));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_including_null_value() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(int_entry('int', 10)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 30)));
        $aggregator->aggregate(Row::create(null_entry('int')));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_float_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(float_entry('int', 10.25)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 305)));
        $aggregator->aggregate(Row::create(int_entry('int', 25)));

        $this->assertSame(
            10.25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_integer_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(Row::create(int_entry('int', 10)));
        $aggregator->aggregate(Row::create(int_entry('int', 20)));
        $aggregator->aggregate(Row::create(int_entry('int', 30)));
        $aggregator->aggregate(Row::create(int_entry('int', 40)));

        $this->assertSame(
            10,
            $aggregator->result()->value()
        );
    }
}
