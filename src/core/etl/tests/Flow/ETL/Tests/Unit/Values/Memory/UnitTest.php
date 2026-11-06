<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Memory;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Memory\Unit;
use PHPUnit\Framework\Attributes\TestWith;

use function memory_get_peak_usage;

final class UnitTest extends FlowTestCase
{
    #[TestWith(['500', 500])]
    #[TestWith(['500B', 500])]
    #[TestWith(['500b', 500])]
    #[TestWith(['2K', 2_048])]
    #[TestWith(['2KB', 2_048])]
    #[TestWith(['2kb', 2_048])]
    #[TestWith(['64M', 67_108_864])]
    #[TestWith(['64MB', 67_108_864])]
    #[TestWith(['64m', 67_108_864])]
    #[TestWith(['64 MB', 67_108_864])]
    #[TestWith(['128M', 134_217_728])]
    #[TestWith(['1G', 1_073_741_824])]
    #[TestWith(['1GB', 1_073_741_824])]
    #[TestWith(['1gb', 1_073_741_824])]
    #[TestWith([' 1 G ', 1_073_741_824])]
    public function test_reading_memory_string(string $memoryString, int $bytes): void
    {
        static::assertSame($bytes, Unit::fromString($memoryString)->inBytes());
    }

    public function test_memory_string_reads_like_the_named_constructors(): void
    {
        static::assertEquals(Unit::fromKb(64), Unit::fromString('64K'));
        static::assertEquals(Unit::fromMb(64), Unit::fromString('64M'));
        static::assertEquals(Unit::fromGb(2), Unit::fromString('2G'));
    }

    public function test_php_memory_limit_reads_as_php_enforces_it(): void
    {
        static::assertSame(134_217_728, Unit::fromString('128M')->inBytes());
    }

    #[TestWith([''])]
    #[TestWith(['abc'])]
    #[TestWith(['10X'])]
    #[TestWith(['10TB'])]
    #[TestWith(['GB'])]
    #[TestWith(['M'])]
    #[TestWith(['1.5G'])]
    public function test_rejecting_unreadable_memory_string(string $memoryString): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't extract memory limit in bytes from php ini value: ");

        Unit::fromString($memoryString);
    }

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
        static::assertSame(1.0, Unit::fromBytes(1024)->inKb());
        static::assertSame(2.0, Unit::fromBytes(2048)->inKb());
        static::assertSame(0.13, Unit::fromBytes(128)->inKb());
        static::assertSame(2048, Unit::fromKb(2)->inBytes());
    }

    public function test_conversion_from_bytes_to_mbs(): void
    {
        static::assertSame(1.0, Unit::fromKb(1024)->inMb());
        static::assertSame(2.0, Unit::fromKb(2048)->inMb());
        static::assertSame(0.0, Unit::fromBytes(128)->inMb());
        static::assertSame(1.5, Unit::fromKb(1536)->inMb());
        static::assertSame(2_097_152, Unit::fromMb(2)->inBytes());
    }

    public function test_conversion_from_bytes_to_gbs(): void
    {
        static::assertSame(1.0, Unit::fromMb(1024)->inGb());
        static::assertSame(1.5, Unit::fromMb(1536)->inGb());
        static::assertSame(0.98, Unit::fromMb(1000)->inGb());
        static::assertSame(1_073_741_824, Unit::fromGb(1)->inBytes());
    }

    public function test_memory_diff(): void
    {
        static::assertSame(-5.0, Unit::fromMb(5)->diff(Unit::fromMb(10))->inMb());
        static::assertSame(5.0, Unit::fromMb(5)->diff(Unit::fromMb(10))->absolute()->inMb());
    }
}
