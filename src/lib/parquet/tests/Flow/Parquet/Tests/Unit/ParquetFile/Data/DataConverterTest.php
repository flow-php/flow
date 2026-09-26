<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use DateTimeImmutable;
use Flow\Parquet\Exception\DataConversionException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use PHPUnit\Framework\TestCase;

final class DataConverterTest extends TestCase
{
    public function test_column_without_logical_type_passes_values_through(): void
    {
        $converter = DataConverter::initialize(new Options());
        $column = Schema::with(FlatColumn::int64('id'))->getFlat('id');

        static::assertNull($converter->resolveConverter($column));
        static::assertSame(42, $converter->fromParquetType($column, 42));
        static::assertSame(42, $converter->toParquetType($column, 42));
        static::assertNull($converter->fromParquetType($column, null));
        static::assertNull($converter->toParquetType($column, null));
    }

    public function test_conversion_failure_is_wrapped_on_every_value(): void
    {
        $converter = DataConverter::initialize(new Options());
        $column = Schema::with(FlatColumn::date('d'))->getFlat('d');

        static::assertEquals(new DateTimeImmutable('1970-01-02 00:00:00 UTC'), $converter->fromParquetType($column, 1));

        $this->expectException(DataConversionException::class);
        $this->expectExceptionMessage(
            "Failed to convert data from parquet type for column 'd'. Expected int, got string",
        );

        $converter->fromParquetType($column, 'not a date');
    }

    public function test_resolves_a_converter_per_column(): void
    {
        $converter = DataConverter::initialize(new Options());
        $schema = Schema::with(
            new FlatColumn(
                'ts_ms',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(false, TimeUnit::MILLISECONDS),
                ),
            ),
            new FlatColumn(
                'ts_ns',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(false, TimeUnit::NANOSECONDS),
                ),
            ),
        );

        static::assertEquals(
            new DateTimeImmutable('2020-01-02 03:04:05.678000 UTC'),
            $converter->fromParquetType($schema->getFlat('ts_ms'), 1577934245678),
        );
        static::assertEquals(
            new DateTimeImmutable('2020-01-02 03:04:05.678901 UTC'),
            $converter->fromParquetType($schema->getFlat('ts_ns'), 1577934245678901234),
        );
    }
}
