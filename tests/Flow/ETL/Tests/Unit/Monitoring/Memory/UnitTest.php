<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Monitoring\Memory;

use Flow\ETL\Monitoring\Memory\Unit;
use PHPUnit\Framework\TestCase;

final class UnitTest extends TestCase
{
    public function test_calculating_percentage_from_value() : void
    {
        $this->assertEquals(
            Unit::fromString('10M'),
            Unit::fromString('100M')->percentage(10)
        );
        $this->assertEquals(
            Unit::fromString('90M'),
            Unit::fromString('100M')->percentage(90)
        );
    }

    public function test_conversion_from_bytes_to_kbs() : void
    {
        $this->assertSame(1.0, Unit::fromBytes(1000)->inKb());
        $this->assertSame(2.0, Unit::fromBytes(2000)->inKb());
        $this->assertSame(0.13, Unit::fromBytes(128)->inKb());
        $this->assertSame(2000, Unit::fromKb(2)->inBytes());
    }

    public function test_conversion_from_bytes_to_mbs() : void
    {
        $this->assertSame(1.0, Unit::fromKb(1000)->inMb());
        $this->assertSame(2.0, Unit::fromKb(2000)->inMb());
        $this->assertSame(0.0, Unit::fromBytes(128)->inMb());
        $this->assertSame(2_000_000, Unit::fromMb(2)->inBytes());
    }

    public function test_memory_diff() : void
    {
        $this->assertSame(
            -5.0,
            Unit::fromMb(5)->diff(Unit::fromMb(10))->inMb()
        );
        $this->assertSame(
            5.0,
            Unit::fromMb(5)->diff(Unit::fromMb(10))->absolute()->inMb()
        );
    }
}
