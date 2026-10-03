<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Physical;

use DateTimeZone;
use Flow\ETL\Column\Physical\MapPhysical;
use Flow\ETL\Column\Physical\TimeZonePhysical;
use PHPUnit\Framework\TestCase;

final class MapPhysicalTest extends TestCase
{
    public function test_converts_every_value_and_keeps_the_keys(): void
    {
        $physical = new MapPhysical(new TimeZonePhysical());

        static::assertSame(
            ['a' => 'UTC', 'b' => null],
            $physical->toPhysical(['a' => new DateTimeZone('UTC'), 'b' => null]),
        );
        static::assertEquals(
            ['a' => new DateTimeZone('UTC'), 'b' => null],
            $physical->fromPhysical(['a' => 'UTC', 'b' => null]),
        );
        static::assertEquals([[5 => new DateTimeZone('UTC')], null], $physical->fromPhysicalAll([[5 => 'UTC'], null]));
    }
}
