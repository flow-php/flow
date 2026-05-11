<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\date_time_format;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\now;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class DateTimeFormatTest extends FlowTestCase
{
    public function test_date_time_format(): void
    {
        static::assertEquals('2020-01-01 00:00:00', date_time_format(ref('date_time'), 'Y-m-d H:i:s')->eval(
            row(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))),
            flow_context(),
        ));
    }

    public function test_formatting_now(): void
    {
        static::assertInstanceOf(\DateTimeImmutable::class, now()->eval(
            row(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))),
            flow_context(),
        ));
    }

    public function test_invalid_date_time_format(): void
    {
        static::assertNull(date_time_format(ref('date_time'), 'Y-m-d H:i:s')->eval(
            row(str_entry('date_time', '2020-01-01 00:00:00')),
            flow_context(),
        ));
    }
}
