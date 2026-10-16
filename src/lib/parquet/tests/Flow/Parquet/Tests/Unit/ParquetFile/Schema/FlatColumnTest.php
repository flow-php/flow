<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\ThriftModel\LogicalType as ThriftLogicalType;
use Flow\Parquet\ThriftModel\MicroSeconds;
use Flow\Parquet\ThriftModel\SchemaElement;
use Flow\Parquet\ThriftModel\TimestampType;
use Flow\Parquet\ThriftModel\TimeUnit as ThriftTimeUnit;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class FlatColumnTest extends TestCase
{
    #[TestWith([19, 9])]
    #[TestWith([23, 10])]
    #[TestWith([24, 11])]
    #[TestWith([35, 15])]
    #[TestWith([36, 16])]
    #[TestWith([38, 16])]
    public function test_decimal_byte_length_follows_the_spec(int $precision, int $length): void
    {
        $column = FlatColumn::decimal('d', $precision, 0);

        static::assertSame(PhysicalType::FIXED_LEN_BYTE_ARRAY, $column->type());
        static::assertSame($length, $column->typeLength());
    }

    #[TestWith([1, PhysicalType::INT32])]
    #[TestWith([9, PhysicalType::INT32])]
    #[TestWith([10, PhysicalType::INT64])]
    #[TestWith([18, PhysicalType::INT64])]
    public function test_decimal_physical_type_follows_precision(int $precision, PhysicalType $type): void
    {
        $column = FlatColumn::decimal('d', $precision, 2);

        static::assertSame($type, $column->type());
        static::assertNull($column->typeLength());
        static::assertSame($precision, $column->logicalType()?->decimalData()?->precision());
        static::assertSame(2, $column->logicalType()?->decimalData()?->scale());
    }

    #[TestWith([0])]
    #[TestWith([39])]
    public function test_decimal_rejects_precision_out_of_range(int $precision): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Precision must be between 1 and 38, {$precision} given.");

        FlatColumn::decimal('x', $precision, 2);
    }

    public function test_from_thrift_derives_logical_type_from_converted_type(): void
    {
        $timestamp = FlatColumn::fromThrift(new SchemaElement([
            'name' => 'ts',
            'type' => PhysicalType::INT64->value,
            'converted_type' => ConvertedType::TIMESTAMP_MILLIS->value,
            'repetition_type' => Repetition::OPTIONAL->value,
        ]))
            ->logicalType()
            ?->timestampData();
        $decimal = FlatColumn::fromThrift(new SchemaElement([
            'name' => 'dec',
            'type' => PhysicalType::INT32->value,
            'converted_type' => ConvertedType::DECIMAL->value,
            'repetition_type' => Repetition::OPTIONAL->value,
            'scale' => 2,
            'precision' => 9,
        ]))
            ->logicalType()
            ?->decimalData();

        static::assertSame(TimeUnit::MILLISECONDS, $timestamp?->unit());
        static::assertTrue($timestamp?->isAdjustedToUTC());
        static::assertSame(2, $decimal?->scale());
        static::assertSame(9, $decimal?->precision());
    }

    public function test_from_thrift_keeps_the_logical_type_over_the_converted_type(): void
    {
        $timestamp = FlatColumn::fromThrift(new SchemaElement([
            'name' => 'ts',
            'type' => PhysicalType::INT64->value,
            'converted_type' => ConvertedType::TIMESTAMP_MICROS->value,
            'logicalType' => new ThriftLogicalType([
                'TIMESTAMP' => new TimestampType([
                    'isAdjustedToUTC' => false,
                    'unit' => new ThriftTimeUnit(['MICROS' => new MicroSeconds()]),
                ]),
            ]),
            'repetition_type' => Repetition::OPTIONAL->value,
        ]))
            ->logicalType()
            ?->timestampData();

        static::assertFalse($timestamp?->isAdjustedToUTC());
        static::assertSame(TimeUnit::MICROSECONDS, $timestamp?->unit());
    }

    public function test_is_map_on_a_non_map_column(): void
    {
        static::assertFalse(FlatColumn::int32('int32')->isMap());
    }

    public function test_repetitions(): void
    {
        static::assertSame(
            [Repetition::OPTIONAL],
            Schema::with(FlatColumn::int32('int32'))->get('int32')->repetitions()->toArray(),
        );
    }

    public function test_enum_is_annotated_enum(): void
    {
        $column = FlatColumn::enum('color');

        static::assertSame(ConvertedType::ENUM, $column->convertedType());
        static::assertSame(LogicalType::ENUM, $column->logicalType()?->name());
        static::assertNotNull($column->toThrift()->logicalType?->ENUM);
    }

    /**
     * @return Generator<string, array{LogicalType, ConvertedType}>
     */
    public static function agreeing_annotations(): Generator
    {
        yield 'string' => [LogicalType::string(), ConvertedType::UTF8];
        yield 'enum' => [LogicalType::enum(), ConvertedType::ENUM];
        yield 'json' => [LogicalType::json(), ConvertedType::JSON];
        yield 'bson' => [LogicalType::bson(), ConvertedType::BSON];
        yield 'decimal' => [LogicalType::decimal(2, 10), ConvertedType::DECIMAL];
        yield 'date' => [LogicalType::date(), ConvertedType::DATE];
        yield 'time millis' => [
            new LogicalType(LogicalType::TIME, time: new Time(true, TimeUnit::MILLISECONDS)),
            ConvertedType::TIME_MILLIS,
        ];
        yield 'time micros' => [LogicalType::time(), ConvertedType::TIME_MICROS];
        yield 'timestamp millis' => [
            new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::MILLISECONDS)),
            ConvertedType::TIMESTAMP_MILLIS,
        ];
        yield 'timestamp micros' => [LogicalType::timestamp(), ConvertedType::TIMESTAMP_MICROS];
        yield 'int8' => [LogicalType::integer(8, true), ConvertedType::INT_8];
        yield 'int16' => [LogicalType::integer(16, true), ConvertedType::INT_16];
        yield 'int32' => [LogicalType::integer(32, true), ConvertedType::INT_32];
        yield 'int64' => [LogicalType::integer(64, true), ConvertedType::INT_64];
        yield 'uint8' => [LogicalType::integer(8, false), ConvertedType::UINT_8];
        yield 'uint16' => [LogicalType::integer(16, false), ConvertedType::UINT_16];
        yield 'uint32' => [LogicalType::integer(32, false), ConvertedType::UINT_32];
        yield 'uint64' => [LogicalType::integer(64, false), ConvertedType::UINT_64];
    }

    /**
     * @return Generator<string, array{LogicalType}>
     */
    public static function logical_types_without_a_converted_type(): Generator
    {
        yield 'uuid' => [LogicalType::uuid()];
        yield 'float16' => [new LogicalType(LogicalType::FLOAT16)];
        yield 'time nanos' => [new LogicalType(LogicalType::TIME, time: new Time(true, TimeUnit::NANOSECONDS))];
        yield 'timestamp nanos' => [
            new LogicalType(LogicalType::TIMESTAMP, timestamp: new Timestamp(true, TimeUnit::NANOSECONDS)),
        ];
        yield 'unknown' => [LogicalType::unknown()];
    }

    #[DataProvider('agreeing_annotations')]
    public function test_a_logical_type_writes_its_converted_type(LogicalType $logical, ConvertedType $converted): void
    {
        static::assertSame($converted, $logical->convertedType());
        static::assertSame(
            $converted->value,
            (new FlatColumn('c', PhysicalType::BYTE_ARRAY, logicalType: $logical))->toThrift()->converted_type,
        );
        static::assertSame(
            $converted->value,
            (new FlatColumn('c', PhysicalType::BYTE_ARRAY, $converted, $logical))->toThrift()->converted_type,
        );
    }

    #[DataProvider('logical_types_without_a_converted_type')]
    public function test_a_logical_type_without_a_converted_equivalent_writes_none(LogicalType $logical): void
    {
        static::assertNull($logical->convertedType());
        static::assertNull((new FlatColumn(
            'c',
            PhysicalType::BYTE_ARRAY,
            logicalType: $logical,
        ))->toThrift()->converted_type);
    }

    public function test_a_converted_type_alone_is_written_as_given(): void
    {
        static::assertSame(
            ConvertedType::INTERVAL->value,
            (new FlatColumn(
                'c',
                PhysicalType::FIXED_LEN_BYTE_ARRAY,
                ConvertedType::INTERVAL,
                typeLength: 12,
            ))->toThrift()->converted_type,
        );
    }

    /**
     * @return Generator<string, array{ConvertedType, LogicalType}>
     */
    public static function contradicting_annotations(): Generator
    {
        yield 'UINT_64 with INTEGER(32, signed)' => [ConvertedType::UINT_64, LogicalType::integer(32, true)];
        yield 'UTF8 with ENUM' => [ConvertedType::UTF8, LogicalType::enum()];
    }

    #[DataProvider('contradicting_annotations')]
    public function test_contradicting_annotations_are_refused(ConvertedType $converted, LogicalType $logical): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column "c": converted type ' . $converted->name . ' contradicts logical type ' . $logical->name(),
        );

        new FlatColumn('c', PhysicalType::INT64, $converted, $logical);
    }

    public function test_a_leaf_without_a_physical_type_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Schema element "c" is a leaf without a physical type');

        FlatColumn::fromThrift(new SchemaElement(['name' => 'c']));
    }
}
