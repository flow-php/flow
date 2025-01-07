<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\max;
use function Flow\ETL\DSL\{datetime_entry, float_entry, int_entry, ref, row, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class MaxTest extends FlowTestCase
{
    public function test_aggregation_max_from_numeric_values() : void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '55')));
        $aggregator->aggregate(row(str_entry('int', '25')));
        $aggregator->aggregate(row(str_entry('not_int', null)));

        self::assertSame(
            55,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_including_null_value() : void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(str_entry('int', null)));

        self::assertSame(
            30,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_with_datetime_values() : void
    {
        $aggregator = max(ref('datetime'));

        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-01 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-02 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-03 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-04 00:00:00')));

        self::assertEquals(
            new \DateTimeImmutable('2021-01-04 00:00:00'),
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_with_float_result() : void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(float_entry('int', 30.5)));
        $aggregator->aggregate(row(int_entry('int', 25)));

        self::assertSame(
            30.5,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_max_with_integer_result() : void
    {
        $aggregator = max(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', 40)));

        self::assertSame(
            40,
            $aggregator->result()->value()
        );
    }
}
