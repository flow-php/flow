<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\Unit;

use function memory_get_peak_usage;

final class UnitTest extends FlowTestCase
{
    public function test_the_process_peak_is_in_bytes_on_every_system(): void
    {
        $peak = Unit::processPeak()->inBytes();

        // the resident set holds the whole PHP heap; a KiB value read as bytes, or bytes scaled as KiB, misses by 1024x
        static::assertGreaterThanOrEqual(memory_get_peak_usage(true), $peak);
        static::assertLessThan(memory_get_peak_usage(true) * 64, $peak);
    }

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
