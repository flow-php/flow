<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\Filesystem\Partition;
use Flow\Filesystem\Partitions;
use PHPUnit\Framework\TestCase;

final class PartitionsTest extends TestCase
{
    public function test_has_get_partitions(): void
    {
        $partitions = new Partitions(
            $year = new Partition('year', '2022'),
            $month = new Partition('month', '12'),
            $day = new Partition('day', '30'),
        );

        static::assertTrue($partitions->has('year'));
        static::assertTrue($partitions->has('month'));
        static::assertTrue($partitions->has('day'));
        static::assertFalse($partitions->has('hour'));
        static::assertSame($year, $partitions->get('year'));
        static::assertSame($month, $partitions->get('month'));
        static::assertSame($day, $partitions->get('day'));
    }
}
