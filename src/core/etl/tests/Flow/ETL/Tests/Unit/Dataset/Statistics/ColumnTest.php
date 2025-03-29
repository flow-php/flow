<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use function Flow\ETL\DSL\{date_entry, datetime_entry, int_entry};
use Flow\ETL\Dataset\Statistics\Column;
use Flow\ETL\Tests\FlowTestCase;

final class ColumnTest extends FlowTestCase
{
    public function test_collecting_column_statistics() : void
    {
        $statistics = new Column(int_entry('a', 1));
        self::assertEquals(1, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(1, $statistics->max());
        self::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', 2));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(2, $statistics->max());
        self::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', null));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(1, $statistics->nullCount());
        self::assertEquals(2, $statistics->max());
        self::assertEquals(1, $statistics->min());
    }

    public function test_collecting_column_statistics_for_date_entries() : void
    {
        $statistics = new Column(date_entry('a', '2024-01-01'));
        self::assertEquals(1, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->calculate(date_entry('a', '2024-01-05'));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-05'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->calculate(date_entry('a', null));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(1, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-05'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());
    }

    public function test_collecting_column_statistics_for_datetime_entries() : void
    {
        $statistics = new Column(datetime_entry('a', '2024-01-01 00:00:01'));
        self::assertEquals(1, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->calculate(datetime_entry('a', '2024-01-01 01:00:00'));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(0, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->calculate(datetime_entry('a', null));

        self::assertEquals(2, $statistics->distinctCount());
        self::assertEquals(1, $statistics->nullCount());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        self::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());
    }
}
