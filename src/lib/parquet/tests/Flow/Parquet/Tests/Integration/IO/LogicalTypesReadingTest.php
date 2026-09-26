<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TemporalValues;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use PHPUnit\Framework\Attributes\DataProvider;

use function iterator_to_array;

final class LogicalTypesReadingTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_converted_type_only_columns(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        (new Writer(engine: new PhpParquetEngine()))->write(
            $path,
            Schema::with(
                new FlatColumn(
                    'ts_ms',
                    PhysicalType::INT64,
                    ConvertedType::TIMESTAMP_MILLIS,
                    null,
                    Repetition::OPTIONAL,
                ),
                new FlatColumn(
                    'ts_us',
                    PhysicalType::INT64,
                    ConvertedType::TIMESTAMP_MICROS,
                    null,
                    Repetition::OPTIONAL,
                ),
                new FlatColumn('t_ms', PhysicalType::INT32, ConvertedType::TIME_MILLIS, null, Repetition::OPTIONAL),
                new FlatColumn('t_us', PhysicalType::INT64, ConvertedType::TIME_MICROS, null, Repetition::OPTIONAL),
                new FlatColumn('dec9', PhysicalType::INT32, ConvertedType::DECIMAL, null, Repetition::OPTIONAL, 9, 2),
            ),
            [[
                'ts_ms' => 1577934245678,
                'ts_us' => 1577934245678901,
                't_ms' => 11045678,
                't_us' => 11045678901,
                'dec9' => 1234567,
            ]],
        );

        $file = (new Reader(engine: $engine))->read($path);

        static::assertSame(
            [[
                'ts_ms' => '2020-01-02 03:04:05.678000 +00:00',
                'ts_us' => '2020-01-02 03:04:05.678901 +00:00',
                't_ms' => '03:04:05.678000',
                't_us' => '03:04:05.678901',
                'dec9' => 12345.67,
            ]],
            TemporalValues::format($file->values()),
        );
        static::assertTrue($file->schema()->get('ts_ms')->logicalType()?->timestampData()?->isAdjustedToUTC());
    }

    #[DataProvider('engine_provider')]
    public function test_reading_dates(ParquetEngine $engine): void
    {
        static::assertSame(
            [
                ['d' => '2020-01-02 00:00:00.000000 +00:00'],
                ['d' => '1969-12-31 00:00:00.000000 +00:00'],
                ['d' => null],
            ],
            TemporalValues::format(
                (new Reader(engine: $engine))
                    ->read(__DIR__ . '/Fixtures/logical_types.parquet')
                    ->values(['d']),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_fixed_len_decimals(ParquetEngine $engine): void
    {
        static::assertSame(
            [
                [
                    'dec9' => 12345.67,
                    'dec18' => 1234567890123456.8,
                    'dec38' => 1.2345678901234568E+18,
                    'dec50' => 1.2345678901234568E+39,
                ],
                ['dec9' => -12345.67, 'dec18' => -0.01, 'dec38' => -12345.67, 'dec50' => -12345.67],
                ['dec9' => null, 'dec18' => null, 'dec38' => null, 'dec50' => null],
            ],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read(__DIR__ . '/Fixtures/decimals_fixed_len.parquet')
                    ->values(),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_int_backed_decimals(ParquetEngine $engine): void
    {
        static::assertSame(
            [
                ['dec9' => 12345.67, 'dec18' => 1234567890123456.8],
                ['dec9' => -12345.67, 'dec18' => -0.01],
                ['dec9' => null, 'dec18' => null],
            ],
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read(__DIR__ . '/Fixtures/logical_types.parquet')
                    ->values(['dec9', 'dec18']),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_times_in_every_unit(ParquetEngine $engine): void
    {
        static::assertSame(
            [
                ['t_ms' => '03:04:05.678000', 't_us' => '03:04:05.678901', 't_ns' => '03:04:05.678901'],
                ['t_ms' => '00:00:00.000000', 't_us' => '23:59:59.999999', 't_ns' => '00:00:00.000000'],
                ['t_ms' => null, 't_us' => null, 't_ns' => null],
            ],
            TemporalValues::format(
                (new Reader(engine: $engine))
                    ->read(__DIR__ . '/Fixtures/logical_types.parquet')
                    ->values(['t_ms', 't_us', 't_ns']),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_timestamps_in_every_unit(ParquetEngine $engine): void
    {
        static::assertSame(
            [
                [
                    'ts_ms' => '2020-01-02 03:04:05.678000 +00:00',
                    'ts_us' => '2020-01-02 03:04:05.678901 +00:00',
                    'ts_ns' => '2020-01-02 03:04:05.678901 +00:00',
                    'ts_us_utc' => '2020-01-02 03:04:05.678901 +00:00',
                    'ts_us_far' => '2262-04-11 23:47:16.854775 +00:00',
                ],
                [
                    'ts_ms' => '1969-12-31 23:59:58.500000 +00:00',
                    'ts_us' => '1900-01-01 00:00:00.000001 +00:00',
                    'ts_ns' => '1969-12-31 23:59:59.999998 +00:00',
                    'ts_us_utc' => '1969-12-31 23:59:58.500000 +00:00',
                    'ts_us_far' => '1677-09-21 00:12:43.145225 +00:00',
                ],
                ['ts_ms' => null, 'ts_us' => null, 'ts_ns' => null, 'ts_us_utc' => null, 'ts_us_far' => null],
            ],
            TemporalValues::format((new Reader(engine: $engine))
                ->read(__DIR__ . '/Fixtures/logical_types.parquet')
                ->values(['ts_ms', 'ts_us', 'ts_ns', 'ts_us_utc', 'ts_us_far'])),
        );
    }
}
