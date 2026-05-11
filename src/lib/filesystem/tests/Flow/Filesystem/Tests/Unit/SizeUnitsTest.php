<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit;

use Flow\Filesystem\SizeUnits;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class SizeUnitsTest extends TestCase
{
    public function test_gb_to_bytes(): void
    {
        static::assertSame(1073741824, SizeUnits::gbToBytes(1));
        static::assertSame(2147483648, SizeUnits::gbToBytes(2));
    }

    public function test_human_readable_caps_at_pib_for_huge_values(): void
    {
        $eib = SizeUnits::GiB_SIZE * 1024 * 1024 * 1024;
        static::assertStringEndsWith(' PiB', SizeUnits::humanReadable($eib));
    }

    #[TestWith([0, '0 B'])]
    #[TestWith([1, '1 B'])]
    #[TestWith([512, '512 B'])]
    #[TestWith([1023, '1,023 B'])]
    public function test_human_readable_prints_raw_bytes_below_1_kib(int $bytes, string $expected): void
    {
        static::assertSame($expected, SizeUnits::humanReadable($bytes));
    }

    public function test_human_readable_respects_decimals_argument(): void
    {
        static::assertSame('1.50000 KiB', SizeUnits::humanReadable(1536, decimals: 5));
        static::assertSame('2 KiB', SizeUnits::humanReadable(2048, decimals: 0));
    }

    public function test_human_readable_respects_separators(): void
    {
        static::assertSame('1,50 KiB', SizeUnits::humanReadable(1536, decimalSeparator: ','));
        static::assertSame('1 023 B', SizeUnits::humanReadable(1023, thousandsSeparator: ' '));
    }

    public function test_human_readable_returns_custom_placeholder_when_null(): void
    {
        static::assertSame('n/a', SizeUnits::humanReadable(null, null: 'n/a'));
    }

    public function test_human_readable_returns_placeholder_when_null(): void
    {
        static::assertSame('-', SizeUnits::humanReadable(null));
    }

    #[TestWith([1024, '1.00 KiB'])]
    #[TestWith([1536, '1.50 KiB'])]
    #[TestWith([SizeUnits::MiB_SIZE, '1.00 MiB'])]
    #[TestWith([SizeUnits::GiB_SIZE, '1.00 GiB'])]
    #[TestWith([SizeUnits::GiB_SIZE * 1024, '1.00 TiB'])]
    #[TestWith([SizeUnits::GiB_SIZE * 1024 * 1024, '1.00 PiB'])]
    public function test_human_readable_scales_up_through_binary_units(int $bytes, string $expected): void
    {
        static::assertSame($expected, SizeUnits::humanReadable($bytes));
    }

    public function test_kb_to_bytes(): void
    {
        static::assertSame(1024, SizeUnits::kbToBytes(1));
        static::assertSame(2048, SizeUnits::kbToBytes(2));
    }

    public function test_mb_to_bytes(): void
    {
        static::assertSame(1048576, SizeUnits::mbToBytes(1));
        static::assertSame(5242880, SizeUnits::mbToBytes(5));
    }
}
