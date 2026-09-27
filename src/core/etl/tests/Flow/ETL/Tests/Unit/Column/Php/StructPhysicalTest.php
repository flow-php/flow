<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Column\Php;

use DateTimeZone;
use Flow\ETL\Column\Php\IdentityPhysical;
use Flow\ETL\Column\Php\StructPhysical;
use Flow\ETL\Column\Php\TimeZonePhysical;
use PHPUnit\Framework\TestCase;

final class StructPhysicalTest extends TestCase
{
    public function test_converts_present_elements_and_keeps_absent_ones_absent(): void
    {
        $physical = new StructPhysical([
            'id' => new IdentityPhysical(),
            'zone' => new TimeZonePhysical(),
            'note' => new IdentityPhysical(),
        ]);

        static::assertSame(
            ['id' => 1, 'zone' => 'UTC'],
            $physical->toPhysical(['zone' => new DateTimeZone('UTC'), 'id' => 1]),
        );
        static::assertSame(['id' => 1, 'zone' => null], $physical->toPhysical(['id' => 1, 'zone' => null]));
        static::assertEquals(
            ['id' => 1, 'zone' => new DateTimeZone('UTC')],
            $physical->fromPhysical(['id' => 1, 'zone' => 'UTC']),
        );
        static::assertEquals(
            [['id' => 1, 'zone' => null], null],
            $physical->fromPhysicalAll([['id' => 1, 'zone' => null], null]),
        );
    }
}
