<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\Filesystem\Partition;
use Flow\Filesystem\{Partitions};
use PHPUnit\Framework\TestCase;

final class PartitionsTest extends TestCase
{
    public function test_has_get_partitions() : void
    {
        $partitions = new Partitions(
            $year = new Partition('year', '2022'),
            $month = new Partition('month', '12'),
            $day = new Partition('day', '30')
        );

        self::assertTrue($partitions->has('year'));
        self::assertTrue($partitions->has('month'));
        self::assertTrue($partitions->has('day'));
        self::assertFalse($partitions->has('hour'));
        self::assertSame($year, $partitions->get('year'));
        self::assertSame($month, $partitions->get('month'));
        self::assertSame($day, $partitions->get('day'));
    }
}
