<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Statistics;

use DateTimeImmutable;
use Flow\ETL\Dataset\Statistics\Column;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\time_zone_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_time_zone;

final class ColumnTest extends FlowTestCase
{
    public function test_collecting_column_statistics(): void
    {
        $definition = int_schema('a', true);

        $statistics = new Column($definition, 1);
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(1, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->add($definition, 2);

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());

        $statistics->add($definition, null);

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(2, $statistics->max());
        static::assertEquals(1, $statistics->min());
    }

    public function test_collecting_column_statistics_for_time_zone_entries(): void
    {
        $definition = time_zone_schema('tz', true);

        $statistics = new Column($definition, type_time_zone()->cast('UTC'));
        static::assertEquals(1, $statistics->distinctCount());

        $statistics->add($definition, type_time_zone()->cast('Europe/Warsaw'));
        static::assertEquals(2, $statistics->distinctCount());

        $statistics->add($definition, type_time_zone()->cast('UTC'));
        static::assertEquals(2, $statistics->distinctCount());

        $statistics->add($definition, null);
        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
    }

    public function test_collecting_column_statistics_for_date_entries(): void
    {
        $definition = date_schema('a', true);

        $statistics = new Column($definition, type_date()->cast('2024-01-01'));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->add($definition, type_date()->cast('2024-01-05'));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());

        $statistics->add($definition, null);

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-05'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01'), $statistics->min());
    }

    public function test_collecting_column_statistics_for_datetime_entries(): void
    {
        $definition = datetime_schema('a', true);

        $statistics = new Column($definition, type_datetime()->cast('2024-01-01 00:00:01'));
        static::assertEquals(1, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->add($definition, type_datetime()->cast('2024-01-01 01:00:00'));

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(0, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());

        $statistics->add($definition, null);

        static::assertEquals(2, $statistics->distinctCount());
        static::assertEquals(1, $statistics->nullCount());
        static::assertEquals(new DateTimeImmutable('2024-01-01 01:00:00'), $statistics->max());
        static::assertEquals(new DateTimeImmutable('2024-01-01 00:00:01'), $statistics->min());
    }
}
