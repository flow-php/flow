<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile;

use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Statistics;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use function pack;

final class StatisticsTest extends TestCase
{
    public function test_binary_statistics_decode_without_a_length_prefix(): void
    {
        $statistics = new Statistics("\x00\xff", "\x00\x01", 0, null, "\x00\xff", "\x00\x01");
        $column = new FlatColumn('b', PhysicalType::BYTE_ARRAY);

        static::assertSame("\x00\x01", $statistics->minValue($column));
        static::assertSame("\x00\xff", $statistics->maxValue($column));
        static::assertSame("\x00\x01", $statistics->min($column));
        static::assertSame("\x00\xff", $statistics->max($column));
    }

    public function test_byte_array_decimal_statistics_decode_to_unscaled_bytes(): void
    {
        $statistics = new Statistics(null, null, 0, null, "\x12\xD6\x87", "\xED\x29\x79");

        static::assertSame("\xED\x29\x79", $statistics->minValue(FlatColumnMother::byteArrayDecimal()));
        static::assertSame("\x12\xD6\x87", $statistics->maxValue(FlatColumnMother::byteArrayDecimal()));
    }

    public function test_int64_decimal_statistics_decode_to_the_unscaled_int(): void
    {
        $statistics = new Statistics(null, null, 0, null, null, pack('P', -350));

        static::assertSame(-350, $statistics->minValue(FlatColumn::decimal('d', 10, 2)));
        static::assertNull($statistics->maxValue(FlatColumn::decimal('d', 10, 2)));
    }

    public function test_counts_and_exactness_are_exposed(): void
    {
        $statistics = new Statistics(null, null, 3, 7, null, null, false, true, 2);

        static::assertSame(3, $statistics->nullCount);
        static::assertSame(7, $statistics->distinctCount);
        static::assertSame(2, $statistics->nanCount);
        static::assertFalse($statistics->isMaxValueExact);
        static::assertTrue($statistics->isMinValueExact);
        static::assertNull($statistics->min(FlatColumn::int64('i')));
        static::assertNull($statistics->max(FlatColumn::int64('i')));
    }

    public function test_a_statistic_of_the_wrong_width_reads_as_absent(): void
    {
        $doubled = pack('P', 99) . pack('P', 99);
        $statistics = new Statistics($doubled, $doubled, 0, null, $doubled, "\x01");

        static::assertNull($statistics->min(FlatColumn::int64('i')));
        static::assertNull($statistics->max(FlatColumn::int64('i')));
        static::assertNull($statistics->maxValue(FlatColumn::int64('i')));
        static::assertNull($statistics->minValue(FlatColumn::int32('i')));
    }
}
