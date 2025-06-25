<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{lit, ref, str_entry, to_timezone};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class ToTimeZoneTest extends FlowTestCase
{
    public function test_casting_date_time_pst_to_utc_time_zone() : void
    {
        $result = to_timezone(
            lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
            lit(new \DateTimeZone('UTC'))
        )->eval(row());
        self::assertInstanceOf(\DateTimeInterface::class, $result);
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            $result->format('Y-m-d H:i:s.u')
        );
    }

    public function test_casting_date_time_pst_to_utc_time_zone_from_entry_ref() : void
    {
        $result = to_timezone(
            lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
            ref('tz')
        )->eval(row(str_entry('tz', 'UTC')));
        self::assertInstanceOf(\DateTimeInterface::class, $result);
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            $result->format('Y-m-d H:i:s.u')
        );
    }

    public function test_casting_date_time_pst_to_utc_time_zone_from_string_tz() : void
    {
        $result = to_timezone(
            lit(new \DateTimeImmutable('2020-01-01 00:00:00', new \DateTimeZone('PST'))),
            lit('UTC')
        )->eval(row());
        self::assertInstanceOf(\DateTimeInterface::class, $result);
        self::assertSame(
            '2020-01-01 08:00:00.000000',
            $result->format('Y-m-d H:i:s.u')
        );
    }
}
