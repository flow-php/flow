<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Double\ToStringUuid;

use function str_repeat;

/**
 * The write-acceptance matrix: one optional column `c` of each target type against each input value. Targets and
 * inputs are named, so data providers carry names and tests build the objects.
 */
final class WriteAcceptance
{
    public static function input(string $name): mixed
    {
        return match ($name) {
            'true' => true,
            'int 1' => 1,
            'int -1' => -1,
            'int 4000000000' => 4_000_000_000,
            'int PHP_INT_MAX' => PHP_INT_MAX,
            'float 1.5' => 1.5,
            'float 1.0' => 1.0,
            'float 12345678.915' => 12_345_678.915,
            'string "1"' => '1',
            'string "1.25"' => '1.25',
            'string "abc"' => 'abc',
            'string 36 uuid' => '0190a6b0-6b1f-7c5e-9a3b-0123456789ab',
            'string 16 bytes' => str_repeat("\x01", 16),
            'string 4 bytes' => "\x00\x01\x02\x03",
            'string 5 bytes' => '12345',
            'string \xFF\xFE' => "\xFF\xFE",
            'DateTimeImmutable Warsaw' => new DateTimeImmutable(
                '2024-02-29 23:30:00.123456',
                new DateTimeZone('Europe/Warsaw'),
            ),
            'DateInterval PT25H' => new DateInterval('PT25H'),
            'DateInterval P1M' => new DateInterval('P1M'),
            'DateInterval -PT1H' => (new DateTimeImmutable('@3600'))->diff(new DateTimeImmutable('@0')),
            'toString() uuid object' => new ToStringUuid('0190a6b0-6b1f-7c5e-9a3b-0123456789ab'),
            'array [1]' => [1],
            'array [a=>1]' => ['a' => 1],
            'null' => null,
            default => throw new InvalidArgumentException("Unknown input \"{$name}\""),
        };
    }

    /**
     * @return list<string>
     */
    public static function inputs(): array
    {
        return [
            'true',
            'int 1',
            'int -1',
            'int 4000000000',
            'int PHP_INT_MAX',
            'float 1.5',
            'float 1.0',
            'float 12345678.915',
            'string "1"',
            'string "1.25"',
            'string "abc"',
            'string 36 uuid',
            'string 16 bytes',
            'string 4 bytes',
            'string 5 bytes',
            'string \xFF\xFE',
            'DateTimeImmutable Warsaw',
            'DateInterval PT25H',
            'DateInterval P1M',
            'DateInterval -PT1H',
            'toString() uuid object',
            'array [1]',
            'array [a=>1]',
            'null',
        ];
    }

    public static function target(string $name): Column
    {
        return match ($name) {
            'BOOLEAN' => FlatColumn::boolean('c'),
            'INT32' => FlatColumn::int32('c'),
            'INT32 INT_8' => new FlatColumn('c', PhysicalType::INT32, ConvertedType::INT_8),
            'INT32 UINT_32' => new FlatColumn('c', PhysicalType::INT32, ConvertedType::UINT_32),
            'INT64' => FlatColumn::int64('c'),
            'INT64 REQUIRED' => FlatColumn::int64('c', Repetition::REQUIRED),
            'INT64 UINT_64' => new FlatColumn('c', PhysicalType::INT64, ConvertedType::UINT_64),
            'FLOAT' => FlatColumn::float('c'),
            'DOUBLE' => FlatColumn::double('c'),
            'STRING' => FlatColumn::string('c'),
            'JSON' => FlatColumn::json('c'),
            'ENUM' => FlatColumn::enum('c'),
            'BYTE_ARRAY' => new FlatColumn('c', PhysicalType::BYTE_ARRAY),
            'UUID' => FlatColumn::uuid('c'),
            'FIXED(4)' => FlatColumn::fixedSizeByteArray('c', 4),
            'DECIMAL(10,2)' => FlatColumn::decimal('c', 10, 2),
            'DATE' => FlatColumn::date('c'),
            'TIMESTAMP MILLIS' => new FlatColumn(
                'c',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::MILLISECONDS),
                ),
            ),
            'TIMESTAMP MICROS' => FlatColumn::dateTime('c'),
            'TIMESTAMP NANOS' => new FlatColumn(
                'c',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                ),
            ),
            'TIME MILLIS' => new FlatColumn(
                'c',
                PhysicalType::INT32,
                logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS)),
            ),
            'TIME MICROS' => FlatColumn::time('c'),
            'TIME NANOS' => new FlatColumn(
                'c',
                PhysicalType::INT64,
                logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::NANOSECONDS)),
            ),
            'LIST<INT64>' => NestedColumn::list('c', ListElement::int64()),
            'MAP<STRING,INT64>' => NestedColumn::map('c', MapKey::string(), MapValue::int64()),
            'STRUCT{a INT64}' => NestedColumn::struct('c', [FlatColumn::int64('a')]),
            default => throw new InvalidArgumentException("Unknown target \"{$name}\""),
        };
    }

    /**
     * @return list<string>
     */
    public static function targets(): array
    {
        return [
            'BOOLEAN',
            'INT32',
            'INT32 INT_8',
            'INT32 UINT_32',
            'INT64',
            'INT64 REQUIRED',
            'INT64 UINT_64',
            'FLOAT',
            'DOUBLE',
            'STRING',
            'JSON',
            'ENUM',
            'BYTE_ARRAY',
            'UUID',
            'FIXED(4)',
            'DECIMAL(10,2)',
            'DATE',
            'TIMESTAMP MILLIS',
            'TIMESTAMP MICROS',
            'TIMESTAMP NANOS',
            'TIME MILLIS',
            'TIME MICROS',
            'TIME NANOS',
            'LIST<INT64>',
            'MAP<STRING,INT64>',
            'STRUCT{a INT64}',
        ];
    }
}
