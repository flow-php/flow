<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{datetime_entry, int_entry, ref, str_entry, to_date_time};
use Flow\ETL\Tests\FlowTestCase;

final class ToDateTimeTest extends FlowTestCase
{
    public function test_date_time_to_date_time() : void
    {
        self::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('date_time'))->eval(row(datetime_entry('date_time', new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))))
        );
    }

    public function test_int_to_date_time() : void
    {
        self::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('int'))->eval(row(int_entry('int', (int) (new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')))->format('U'))))
        );
    }

    public function test_string_to_date_time() : void
    {
        self::assertEquals(
            new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('UTC')),
            to_date_time(ref('string'), 'Y-m-d H:i:s')->eval(row(str_entry('string', '2020-01-01 00:00:00')))
        );
    }
}
