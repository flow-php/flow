<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Dataset\Memory;

use Flow\ETL\Dataset\Memory\Unit;
use Flow\ETL\Tests\FlowTestCase;

final class UnitTest extends FlowTestCase
{
    public function test_calculating_percentage_from_value(): void
    {
        static::assertEquals(Unit::fromString('10M'), Unit::fromString('100M')->percentage(10));
        static::assertEquals(Unit::fromString('90M'), Unit::fromString('100M')->percentage(90));
    }

    public function test_conversion_from_bytes_to_kbs(): void
    {
        static::assertSame(1.0, Unit::fromBytes(1000)->inKb());
        static::assertSame(2.0, Unit::fromBytes(2000)->inKb());
        static::assertSame(0.13, Unit::fromBytes(128)->inKb());
        static::assertSame(2000, Unit::fromKb(2)->inBytes());
    }

    public function test_conversion_from_bytes_to_mbs(): void
    {
        static::assertSame(1.0, Unit::fromKb(1000)->inMb());
        static::assertSame(2.0, Unit::fromKb(2000)->inMb());
        static::assertSame(0.0, Unit::fromBytes(128)->inMb());
        static::assertSame(2_000_000, Unit::fromMb(2)->inBytes());
    }

    public function test_memory_diff(): void
    {
        static::assertSame(-5.0, Unit::fromMb(5)->diff(Unit::fromMb(10))->inMb());
        static::assertSame(5.0, Unit::fromMb(5)->diff(Unit::fromMb(10))->absolute()->inMb());
    }
}
