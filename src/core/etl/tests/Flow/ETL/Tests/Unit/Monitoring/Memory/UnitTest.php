<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Monitoring\Memory;

use Flow\ETL\Monitoring\Memory\Unit;
use PHPUnit\Framework\TestCase;

final class UnitTest extends TestCase
{
    public function test_calculating_percentage_from_value() : void
    {
        self::assertEquals(
            Unit::fromString('10M'),
            Unit::fromString('100M')->percentage(10)
        );
        self::assertEquals(
            Unit::fromString('90M'),
            Unit::fromString('100M')->percentage(90)
        );
    }

    public function test_conversion_from_bytes_to_kbs() : void
    {
        self::assertSame(1.0, Unit::fromBytes(1000)->inKb());
        self::assertSame(2.0, Unit::fromBytes(2000)->inKb());
        self::assertSame(0.13, Unit::fromBytes(128)->inKb());
        self::assertSame(2000, Unit::fromKb(2)->inBytes());
    }

    public function test_conversion_from_bytes_to_mbs() : void
    {
        self::assertSame(1.0, Unit::fromKb(1000)->inMb());
        self::assertSame(2.0, Unit::fromKb(2000)->inMb());
        self::assertSame(0.0, Unit::fromBytes(128)->inMb());
        self::assertSame(2_000_000, Unit::fromMb(2)->inBytes());
    }

    public function test_memory_diff() : void
    {
        self::assertSame(
            -5.0,
            Unit::fromMb(5)->diff(Unit::fromMb(10))->inMb()
        );
        self::assertSame(
            5.0,
            Unit::fromMb(5)->diff(Unit::fromMb(10))->absolute()->inMb()
        );
    }
}
