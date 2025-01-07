<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{date_time_format, datetime_entry, now, ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class DateTimeFormatTest extends FlowTestCase
{
    public function test_date_time_format() : void
    {
        self::assertEquals(
            '2020-01-01 00:00:00',
            date_time_format(ref('date_time'), 'Y-m-d H:i:s')->eval(row(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))))
        );
    }

    public function test_formatting_now() : void
    {
        self::assertInstanceOf(
            \DateTimeImmutable::class,
            now()->eval(row(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))))
        );
    }

    public function test_invalid_date_time_format() : void
    {
        self::assertNull(
            date_time_format(ref('date_time'), 'Y-m-d H:i:s')->eval(row(str_entry('date_time', '2020-01-01 00:00:00')))
        );
    }
}
