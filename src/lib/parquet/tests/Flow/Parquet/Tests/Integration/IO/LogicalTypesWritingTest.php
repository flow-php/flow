<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TemporalValues;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function iterator_to_array;

final class LogicalTypesWritingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_pair_provider')]
    public function test_dates_are_the_wall_clock_day(ParquetEngine $writer, ParquetEngine $reader): void
    {
        $path = TestParquetFile::path($this);

        (new Writer(engine: $writer))->write($path, Schema::with(FlatColumn::date('d')), [
            ['d' => new DateTimeImmutable('2024-01-01 00:00:00', new DateTimeZone('Europe/Warsaw'))],
            ['d' => new DateTimeImmutable('1969-12-31 12:00:00', new DateTimeZone('UTC'))],
            ['d' => new DateTimeImmutable('1900-02-28 00:00:00', new DateTimeZone('UTC'))],
        ]);

        static::assertSame(
            [
                ['d' => '2024-01-01 00:00:00.000000 +00:00'],
                ['d' => '1969-12-31 00:00:00.000000 +00:00'],
                ['d' => '1900-02-28 00:00:00.000000 +00:00'],
            ],
            TemporalValues::format(
                (new Reader(engine: $reader))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_decimals_round_half_away_from_zero(ParquetEngine $writer, ParquetEngine $reader): void
    {
        $path = TestParquetFile::path($this);

        (new Writer(engine: $writer))->write($path, Schema::with(FlatColumn::decimal('d', 9, 2)), [
            ['d' => 0.125],
            ['d' => 2.675],
            ['d' => -0.125],
            ['d' => 1.005],
            ['d' => 9.995],
            ['d' => -99.995],
        ]);

        static::assertSame(
            [['d' => 0.13], ['d' => 2.68], ['d' => -0.13], ['d' => 1.01], ['d' => 10.0], ['d' => -100.0]],
            iterator_to_array(
                (new Reader(engine: $reader))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_decimals_round_trip(ParquetEngine $writer, ParquetEngine $reader): void
    {
        $path = TestParquetFile::path($this);
        $rows = [
            ['d9' => 12345.67, 'd38' => 12345.67, 'i32' => 12345.67, 'i64' => 12345.67, 'ba' => 12345.67],
            ['d9' => -12345.67, 'd38' => -12345.67, 'i32' => -12345.67, 'i64' => -12345.67, 'ba' => -12345.67],
            ['d9' => null, 'd38' => 1.2345678901234568E+18, 'i32' => null, 'i64' => null, 'ba' => null],
        ];

        (new Writer(engine: $writer))->write(
            $path,
            Schema::with(
                FlatColumn::decimal('d9', 9, 2),
                FlatColumn::decimal('d38', 38, 10),
                new FlatColumn(
                    'i32',
                    PhysicalType::INT32,
                    null,
                    LogicalType::decimal(2, 9),
                    Repetition::OPTIONAL,
                    9,
                    2,
                ),
                new FlatColumn(
                    'i64',
                    PhysicalType::INT64,
                    null,
                    LogicalType::decimal(2, 18),
                    Repetition::OPTIONAL,
                    18,
                    2,
                ),
                new FlatColumn(
                    'ba',
                    PhysicalType::BYTE_ARRAY,
                    null,
                    LogicalType::decimal(2, 9),
                    Repetition::OPTIONAL,
                    9,
                    2,
                ),
            ),
            $rows,
        );

        static::assertSame(
            $rows,
            iterator_to_array(
                (new Reader(engine: $reader))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_times_round_trip_in_every_unit(ParquetEngine $writer, ParquetEngine $reader): void
    {
        $path = TestParquetFile::path($this);
        $time = (new DateTimeImmutable('2020-01-01 00:00:00 UTC'))->diff(
            new DateTimeImmutable('2020-01-01 03:04:05.678901 UTC'),
        );

        (new Writer(engine: $writer))->write(
            $path,
            Schema::with(
                FlatColumn::time('t'),
                new FlatColumn(
                    't_ms',
                    PhysicalType::INT32,
                    logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS)),
                ),
                new FlatColumn(
                    't_ns',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::NANOSECONDS)),
                ),
            ),
            [['t' => $time, 't_ms' => $time, 't_ns' => $time]],
        );

        static::assertSame(
            [['t' => '03:04:05.678901', 't_ms' => '03:04:05.678000', 't_ns' => '03:04:05.678901']],
            TemporalValues::format(
                (new Reader(engine: $reader))
                    ->read($path)
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_pair_provider')]
    public function test_timestamps_round_trip_in_every_unit(ParquetEngine $writer, ParquetEngine $reader): void
    {
        $path = TestParquetFile::path($this);
        $instant = new DateTimeImmutable('2020-01-02 04:04:05.678901 +01:00');
        $preEpoch = new DateTimeImmutable('1969-12-31 23:59:58.5 UTC');

        (new Writer(engine: $writer))->write(
            $path,
            Schema::with(
                FlatColumn::dateTime('ts'),
                new FlatColumn(
                    'ts_ms_local',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(false, TimeUnit::MILLISECONDS),
                    ),
                ),
                new FlatColumn(
                    'ts_ns_utc',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                    ),
                ),
            ),
            [
                ['ts' => $instant, 'ts_ms_local' => $instant, 'ts_ns_utc' => $instant],
                ['ts' => $preEpoch, 'ts_ms_local' => $preEpoch, 'ts_ns_utc' => $preEpoch],
            ],
        );

        static::assertSame(
            [
                [
                    'ts' => '2020-01-02 03:04:05.678901 +00:00',
                    'ts_ms_local' => '2020-01-02 03:04:05.678000 +00:00',
                    'ts_ns_utc' => '2020-01-02 03:04:05.678901 +00:00',
                ],
                [
                    'ts' => '1969-12-31 23:59:58.500000 +00:00',
                    'ts_ms_local' => '1969-12-31 23:59:58.500000 +00:00',
                    'ts_ns_utc' => '1969-12-31 23:59:58.500000 +00:00',
                ],
            ],
            TemporalValues::format(
                (new Reader(engine: $reader))
                    ->read($path)
                    ->values(),
            ),
        );

        $schema = (new Reader())
            ->read($path)
            ->metadata()
            ->schema();

        static::assertSame(
            [
                'ts' => [TimeUnit::MICROSECONDS, true],
                'ts_ms_local' => [TimeUnit::MILLISECONDS, false],
                'ts_ns_utc' => [TimeUnit::NANOSECONDS, true],
            ],
            array_map(static fn(string $column): array => [
                $schema->get($column)->logicalType()?->timestampData()?->unit(),
                $schema->get($column)->logicalType()?->timestampData()?->isAdjustedToUTC(),
            ], ['ts' => 'ts', 'ts_ms_local' => 'ts_ms_local', 'ts_ns_utc' => 'ts_ns_utc']),
        );
    }
}
