<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\ListPhysical;
use Flow\ETL\Column\Php\TimeZonePhysical;
use PHPUnit\Framework\TestCase;

final class ListPhysicalTest extends TestCase
{
    public function test_converts_every_element(): void
    {
        $physical = new ListPhysical(new TimeZonePhysical());

        static::assertSame(['UTC', null], $physical->toPhysical([1 => new DateTimeZone('UTC'), 3 => null]));
        static::assertEquals([new DateTimeZone('UTC'), null], $physical->fromPhysical(['UTC', null]));
        static::assertEquals([[new DateTimeZone('UTC')], null], $physical->fromPhysicalAll([['UTC'], null]));
    }
}
