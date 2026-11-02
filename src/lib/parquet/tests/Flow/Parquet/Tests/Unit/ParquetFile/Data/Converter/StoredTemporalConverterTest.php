<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\StoredTemporalConverter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function pack;

final class StoredTemporalConverterTest extends TestCase
{
    /**
     * @return Generator<string, array{FlatColumn, Options, ?StoredTemporalConverter}>
     */
    public static function columns(): Generator
    {
        yield 'DATE' => [FlatColumn::date('d'), Options::default(), StoredTemporalConverter::days()];
        yield 'DATE as a converted type only' => [
            new FlatColumn('d', PhysicalType::INT32, ConvertedType::DATE),
            Options::default(),
            StoredTemporalConverter::days(),
        ];
        yield 'TIMESTAMP(MILLIS)' => [
            new FlatColumn(
                't',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::MILLISECONDS),
                ),
            ),
            Options::default(),
            StoredTemporalConverter::micros(TimeUnit::MILLISECONDS),
        ];
        yield 'TIMESTAMP(NANOS)' => [
            new FlatColumn(
                't',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                ),
            ),
            Options::default(),
            StoredTemporalConverter::micros(TimeUnit::NANOSECONDS),
        ];
        yield 'INT32 TIME(MILLIS)' => [
            new FlatColumn(
                't',
                PhysicalType::INT32,
                logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS)),
            ),
            Options::default(),
            StoredTemporalConverter::micros(TimeUnit::MILLISECONDS),
        ];
        yield 'INT64 TIME(MICROS)' => [
            FlatColumn::time('t'),
            Options::default(),
            StoredTemporalConverter::micros(TimeUnit::MICROSECONDS),
        ];
        yield 'INT96 read as a datetime' => [
            new FlatColumn('t', PhysicalType::INT96),
            Options::default()->set(Option::INT_96_AS_DATETIME, true),
            StoredTemporalConverter::int96(),
        ];
        yield 'INT96 without the option' => [
            new FlatColumn('t', PhysicalType::INT96),
            Options::default()->set(Option::INT_96_AS_DATETIME, false),
            null,
        ];
        yield 'a plain INT64' => [FlatColumn::int64('n'), Options::default(), null];
    }

    #[DataProvider('columns')]
    public function test_a_column_gets_the_converter_of_its_stored_shape(
        FlatColumn $column,
        Options $options,
        ?StoredTemporalConverter $expected,
    ): void {
        static::assertEquals($expected, StoredTemporalConverter::forColumn($column, $options));
    }

    #[TestWith([TimeUnit::MILLISECONDS, 1_234_567_890_123, 1_234_567_890_123_000])]
    #[TestWith([TimeUnit::MICROSECONDS, 1_234_567_890_123_456, 1_234_567_890_123_456])]
    #[TestWith([TimeUnit::NANOSECONDS, 1_234_567_890_123_456_789, 1_234_567_890_123_456])]
    #[TestWith([TimeUnit::NANOSECONDS, -1, -1])]
    public function test_a_stored_value_reads_as_microseconds(TimeUnit $unit, int $stored, int $micros): void
    {
        static::assertSame($micros, StoredTemporalConverter::micros($unit)->fromParquetType($stored));
    }

    #[TestWith([TimeUnit::MILLISECONDS, 1_234_567_890_123_456, 1_234_567_890_123])]
    #[TestWith([TimeUnit::MILLISECONDS, -1, -1])]
    #[TestWith([TimeUnit::MICROSECONDS, 1_234_567_890_123_456, 1_234_567_890_123_456])]
    #[TestWith([TimeUnit::NANOSECONDS, 1_234_567_890_123_456, 1_234_567_890_123_456_000])]
    public function test_microseconds_write_in_the_unit_of_the_column(TimeUnit $unit, int $micros, int $stored): void
    {
        static::assertSame($stored, StoredTemporalConverter::micros($unit)->toParquetType($micros));
    }

    public function test_days_are_stored_as_they_are(): void
    {
        static::assertSame(-1, StoredTemporalConverter::days()->fromParquetType(-1));
        static::assertSame(19_782, StoredTemporalConverter::days()->toParquetType(19_782));
    }

    public function test_an_instant_outside_the_nanosecond_range_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('outside the TIMESTAMP(NANOS) range');

        StoredTemporalConverter::micros(TimeUnit::NANOSECONDS)->toParquetType(9_300_000_000_000_000);
    }

    public function test_a_value_that_is_no_integer_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected int, got string');

        StoredTemporalConverter::micros(TimeUnit::MICROSECONDS)->toParquetType('1');
    }

    public function test_int96_bytes_read_as_microseconds_since_the_epoch(): void
    {
        static::assertSame(
            1_234_567_890_123_456,
            StoredTemporalConverter::int96()->fromParquetType(pack('PV', 84_690_123_456_000, 2_440_588 + 14_288)),
        );
    }

    public function test_int96_bytes_of_another_length_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected 12 INT96 bytes');

        StoredTemporalConverter::int96()->fromParquetType('short');
    }

    public function test_an_int96_write_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('INT96 timestamps are read only');

        StoredTemporalConverter::int96()->toParquetType(1);
    }
}
