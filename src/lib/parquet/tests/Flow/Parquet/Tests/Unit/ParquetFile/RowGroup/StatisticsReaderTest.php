<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroup;

use Flow\Parquet\ParquetFile\RowGroup\StatisticsReader;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Statistics;
use Flow\Parquet\Tests\Mother\FlatColumnMother;
use PHPUnit\Framework\TestCase;

use function pack;

final class StatisticsReaderTest extends TestCase
{
    public function test_binary_statistics_decode_without_a_length_prefix(): void
    {
        $reader = new StatisticsReader(new Statistics("\x00\xff", "\x00\x01", 0, null, "\x00\xff", "\x00\x01"));
        $column = new FlatColumn('b', PhysicalType::BYTE_ARRAY);

        static::assertSame("\x00\x01", $reader->minValue($column));
        static::assertSame("\x00\xff", $reader->maxValue($column));
        static::assertSame("\x00\x01", $reader->min($column));
        static::assertSame("\x00\xff", $reader->max($column));
    }

    public function test_byte_array_decimal_statistics_decode_to_unscaled_bytes(): void
    {
        $reader = new StatisticsReader(new Statistics(null, null, 0, null, "\x12\xD6\x87", "\xED\x29\x79"));

        static::assertSame("\xED\x29\x79", $reader->minValue(FlatColumnMother::byteArrayDecimal()));
        static::assertSame("\x12\xD6\x87", $reader->maxValue(FlatColumnMother::byteArrayDecimal()));
    }

    public function test_int64_decimal_statistics_decode_to_the_unscaled_int(): void
    {
        $reader = new StatisticsReader(new Statistics(null, null, 0, null, null, pack('P', -350)));

        static::assertSame(-350, $reader->minValue(FlatColumn::decimal('d', 10, 2)));
        static::assertNull($reader->maxValue(FlatColumn::decimal('d', 10, 2)));
    }

    public function test_counts_and_exactness_are_exposed(): void
    {
        $reader = new StatisticsReader(new Statistics(null, null, 3, 7, null, null, false, true, 2));

        static::assertSame(3, $reader->nullCount());
        static::assertSame(7, $reader->distinctCount());
        static::assertSame(2, $reader->nanCount());
        static::assertFalse($reader->isMaxValueExact());
        static::assertTrue($reader->isMinValueExact());
        static::assertNull($reader->min(FlatColumn::int64('i')));
        static::assertNull($reader->max(FlatColumn::int64('i')));
    }

    public function test_a_statistic_of_the_wrong_width_reads_as_absent(): void
    {
        $doubled = pack('P', 99) . pack('P', 99);
        $reader = new StatisticsReader(new Statistics($doubled, $doubled, 0, null, $doubled, "\x01"));

        static::assertNull($reader->min(FlatColumn::int64('i')));
        static::assertNull($reader->max(FlatColumn::int64('i')));
        static::assertNull($reader->maxValue(FlatColumn::int64('i')));
        static::assertNull($reader->minValue(FlatColumn::int32('i')));
    }
}
