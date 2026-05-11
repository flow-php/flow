<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use Flow\ETL\Dataset\Statistics\Column;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\int_entry;

final class ColumnTest extends FlowTestCase
{
    public function test_collecting_column_statistics(): void
    {
        $statistics = new Column(int_entry('a', 1));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(1, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', 2));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->calculate(int_entry('a', null));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());
    }

    public function test_collecting_column_statistics_for_date_entries(): void
    {
        $statistics = new Column(date_entry('a', '2024-01-01'));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->calculate(date_entry('a', '2024-01-05'));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->calculate(date_entry('a', null));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01'), $statistics->min());
    }

    public function test_collecting_column_statistics_for_datetime_entries(): void
    {
        $statistics = new Column(datetime_entry('a', '2024-01-01 00:00:01'));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->calculate(datetime_entry('a', '2024-01-01 01:00:00'));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->calculate(datetime_entry('a', null));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new \DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());
    }
}
