<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, ref, str_entry, to_timezone};
use Flow\ETL\Tests\FlowTestCase;

final class ToTimeZoneTest extends FlowTestCase
{
    public function test_casting_date_time_pst_to_utc_time_zone() : void
    {
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            to_timezone(
                lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
                lit(new \DateTimeZone('UTC'))
            )->eval(row())->format('Y-m-d H:i:s.u')
        );
    }

    public function test_casting_date_time_pst_to_utc_time_zone_from_entry_ref() : void
    {
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            to_timezone(
                lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
                ref('tz')
            )->eval(row(str_entry('tz', 'UTC')))->format('Y-m-d H:i:s.u')
        );
    }

    public function test_casting_date_time_pst_to_utc_time_zone_from_string_tz() : void
    {
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            to_timezone(
                lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
                lit('UTC')
            )->eval(row())->format('Y-m-d H:i:s.u')
        );
    }
}
