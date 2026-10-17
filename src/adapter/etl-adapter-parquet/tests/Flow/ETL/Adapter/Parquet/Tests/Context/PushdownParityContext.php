<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Filesystem\Filesystem;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Writer;

use function array_map;
use function Flow\Filesystem\DSL\path;
use function range;
use function sprintf;

use const NAN;

final class PushdownParityContext
{
    public const string GLOB = 'memory://parity/*.parquet';

    /**
     * `memory://parity/a.parquet`: `id` 0..29 in row groups of 10, a column of every pushable kind and `extra`;
     * `memory://parity/b.parquet`: `id` 30..39 without `extra`, so a union-by-name read sees it absent there.
     */
    public static function write(Filesystem $filesystem): void
    {
        $options = Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            10,
        );
        $columns = [
            FlatColumn::int64('id'),
            FlatColumn::string('name'),
            FlatColumn::double('score'),
            FlatColumn::decimal('d92', 9, 2),
            FlatColumn::decimal('d182', 18, 2),
            FlatColumn::date('day'),
            FlatColumn::dateTime('ts_us'),
            new FlatColumn(
                'ts_ns',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                ),
            ),
            FlatColumn::uuid('uuid'),
        ];
        $row = static fn(int $id): array => [
            'id' => $id,
            'name' => (string) $id,
            'score' => $id < 10 ? NAN : $id * 1.5,
            'd92' => $id + 0.25,
            'd182' => $id + 0.25,
            'day' => (new DateTimeImmutable('2024-01-01'))->modify(sprintf('+%d days', $id)),
            'ts_us' => (new DateTimeImmutable('2024-01-01 00:00:00.123456'))->modify(sprintf('+%d hours', $id)),
            'ts_ns' => (new DateTimeImmutable('2024-01-01 00:00:00.123456'))->modify(sprintf('+%d hours', $id)),
            'uuid' => sprintf('00000000-0000-4000-8000-%012d', $id),
        ];

        foreach ([
            'a' => [
                [...$columns, FlatColumn::string('extra')],
                static fn(int $id): array => [...$row($id), 'extra' => 'x'],
            ],
            'b' => [$columns, $row],
        ] as $file => [$schema, $values]) {
            $writer = new Writer(options: $options, engine: new PhpParquetEngine(options: $options));
            $writer->openForStream(
                $filesystem->writeTo(path(sprintf('memory://parity/%s.parquet', $file))),
                Schema::with(...$schema),
            );
            $writer->writeBatch(array_map($values, $file === 'a' ? range(0, 29) : range(30, 39)));
            $writer->close();
        }
    }
}
