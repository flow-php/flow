<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Reader;
use Flow\Parquet\Writer;

use function str_repeat;

use const NAN;

final class StatisticsParityRows
{
    /**
     * @return list<array<string, mixed>>
     */
    public static function rows(): array
    {
        return [
            [
                'digits' => '9',
                'long' => str_repeat('a', 101),
                'double' => -0.0,
                'float' => 1.5,
                'decimal' => -3.5,
                'int32' => 7,
                'int64' => 1,
                'uint64' => 0,
                'boolean' => true,
                'date' => new DateTimeImmutable('2024-01-02'),
                'uuid' => 'ffffffff-0000-0000-0000-000000000001',
                'decimal30' => -3.5,
                'binary' => "\x00\xff",
                'timestamp' => new DateTimeImmutable('2024-01-02 03:04:05.123456 UTC'),
                'enum' => 'RED',
            ],
            [
                'digits' => '10',
                'long' => str_repeat('c', 101),
                'double' => NAN,
                'float' => NAN,
                'decimal' => 1.25,
                'int32' => null,
                'int64' => null,
                'uint64' => '18446744073709551615',
                'boolean' => false,
                'date' => null,
                'uuid' => null,
                'decimal30' => 12_345_678_901_234_567.25,
                'binary' => "\x80",
                'timestamp' => null,
                'enum' => 'BLUE',
            ],
            [
                'digits' => '100',
                'long' => str_repeat('b', 101),
                'double' => 0.0,
                'float' => -2.0,
                'decimal' => null,
                'int32' => -7,
                'int64' => 3,
                'uint64' => 5,
                'boolean' => null,
                'date' => new DateTimeImmutable('1999-12-31'),
                'uuid' => '00000000-0000-0000-0000-000000000002',
                'decimal30' => null,
                'binary' => "\x7f",
                'timestamp' => new DateTimeImmutable('1969-12-31 23:59:59 UTC'),
                'enum' => 'GREEN',
            ],
        ];
    }

    public static function schema(): Schema
    {
        return Schema::with(
            FlatColumn::string('digits'),
            FlatColumn::string('long'),
            FlatColumn::double('double'),
            FlatColumn::float('float'),
            FlatColumn::decimal('decimal', 10, 2),
            FlatColumn::int32('int32'),
            FlatColumn::int64('int64'),
            new FlatColumn('uint64', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)),
            FlatColumn::boolean('boolean'),
            FlatColumn::date('date'),
            FlatColumn::uuid('uuid'),
            FlatColumn::decimal('decimal30', 30, 2),
            new FlatColumn('binary', PhysicalType::BYTE_ARRAY),
            FlatColumn::dateTime('timestamp'),
            FlatColumn::enum('enum'),
        );
    }

    /**
     * Per leaf flat path, the column chunk statistics `engine` writes for these rows, read back by the PHP footer
     * decoder: min, max, min_value, max_value, null_count, nan_count, exactness flags and the column order.
     *
     * @return array<string, list<mixed>>
     */
    public static function statistics(ParquetEngine $engine): array
    {
        $metadata = MemoryParquetFile::read(Reader::php(), MemoryParquetFile::written(
            new Writer(engine: $engine),
            self::schema(),
            self::rows(),
        ))->metadata();
        $statistics = [];

        foreach ($metadata->schema()->columnsFlat() as $index => $column) {
            $chunk = $metadata->columnChunks()[$index]->statistics() ?? throw new RuntimeException(
                'Column "' . $column->flatPath() . '" has no statistics',
            );
            $statistics[$column->flatPath()] = [
                $chunk->min($column),
                $chunk->max($column),
                $chunk->minValue($column),
                $chunk->maxValue($column),
                $chunk->nullCount(),
                $chunk->nanCount(),
                $chunk->isMinValueExact(),
                $chunk->isMaxValueExact(),
                $metadata->columnOrder($column),
            ];
        }

        return $statistics;
    }
}
