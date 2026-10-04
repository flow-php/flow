<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\ParquetFile\Data\Converter\Int96DateTimeConverter;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function pack;

final class Int96DateTimeConverterTest extends TestCase
{
    /**
     * INT96: 8 bytes of nanoseconds within the day, then 4 bytes of the Julian day.
     *
     * @return Generator<string, array{string, string}>
     */
    public static function int96_values(): Generator
    {
        yield 'nanoseconds floored' => [
            pack('PV', 45_296_789_012_345, 19_782 + 2_440_588),
            '2024-02-29 12:34:56.789012',
        ];
        yield 'epoch' => [pack('PV', 0, 2_440_588), '1970-01-01 00:00:00.000000'];
        yield 'before the epoch, never rounded up' => [
            pack('PV', 86_399_999_999_999, -3_653 + 2_440_588),
            '1960-01-01 23:59:59.999999',
        ];
        yield 'first microsecond of year 1' => [pack('PV', 1_000, -719_162 + 2_440_588), '0001-01-01 00:00:00.000001'];
        yield 'sub-microsecond of year 9999' => [pack('PV', 999, 2_932_896 + 2_440_588), '9999-12-31 00:00:00.000000'];
        yield 'remainder of 999 nanoseconds' => [
            pack('PV', 123_456_789_999, 19_000 + 2_440_588),
            '2022-01-08 00:02:03.456789',
        ];
    }

    #[DataProvider('int96_values')]
    public function test_int96_reads_as_the_microsecond_it_falls_in(string $bytes, string $datetime): void
    {
        static::assertSame(
            $datetime,
            (new Int96DateTimeConverter())
                ->fromParquetType($bytes)
                ->format('Y-m-d H:i:s.u'),
        );
    }
}
