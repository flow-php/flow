<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DateTimeZone;
use Flow\ETL\Column\Physical\TimeZonePhysical;
use PHPUnit\Framework\TestCase;

final class TimeZonePhysicalTest extends TestCase
{
    public function test_the_zone_name(): void
    {
        $physical = new TimeZonePhysical();

        static::assertSame('Europe/Warsaw', $physical->toPhysical(new DateTimeZone('Europe/Warsaw')));
        static::assertEquals(new DateTimeZone('Europe/Warsaw'), $physical->fromPhysical('Europe/Warsaw'));
        static::assertEquals([new DateTimeZone('UTC'), null], $physical->fromPhysicalAll(['UTC', null]));
    }
}
