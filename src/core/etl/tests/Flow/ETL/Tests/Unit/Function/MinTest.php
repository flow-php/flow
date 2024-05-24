<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{datetime_entry, float_entry, int_entry, min, ref, row, str_entry};
use PHPUnit\Framework\TestCase;

final class MinTest extends TestCase
{
    public function test_aggregation_min_from_numeric_values() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(str_entry('int', '10')));
        $aggregator->aggregate(row(str_entry('int', '20')));
        $aggregator->aggregate(row(str_entry('int', '55')));
        $aggregator->aggregate(row(str_entry('int', '25')));
        $aggregator->aggregate(row(str_entry('not_int', null)));

        self::assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_including_null_value() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(str_entry('int', null)));

        self::assertSame(
            10,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_datetime_values() : void
    {
        $aggregator = min(ref('datetime'));

        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-01 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-02 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-03 00:00:00')));
        $aggregator->aggregate(row(datetime_entry('datetime', '2021-01-04 00:00:00')));

        self::assertEquals(
            new \DateTimeImmutable('2021-01-01 00:00:00'),
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_float_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(float_entry('int', 10.25)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 305)));
        $aggregator->aggregate(row(int_entry('int', 25)));

        self::assertSame(
            10.25,
            $aggregator->result()->value()
        );
    }

    public function test_aggregation_min_with_integer_result() : void
    {
        $aggregator = min(ref('int'));

        $aggregator->aggregate(row(int_entry('int', 10)));
        $aggregator->aggregate(row(int_entry('int', 20)));
        $aggregator->aggregate(row(int_entry('int', 30)));
        $aggregator->aggregate(row(int_entry('int', 40)));

        self::assertSame(
            10,
            $aggregator->result()->value()
        );
    }
}
