<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use DateInterval;
use DateTimeInterface;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\Writer;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path;

final class ConvertedTypeOnlyColumnsTest extends FlowTestCase
{
    public function test_converted_type_only_columns_read_as_temporal_and_float_entries(): void
    {
        $memory = memory_filesystem();
        $path = path('memory://var/converted_type_only_columns.parquet');
        $writer = new Writer(engine: new PhpParquetEngine());

        $writer->openForStream(
            $memory->writeTo($path),
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
        );
        $writer->writeBatch([[
            'ts_ms' => 1577934245678,
            'ts_us' => 1577934245678901,
            't_ms' => 11045678,
            't_us' => 11045678901,
            'dec9' => 1234567,
        ]]);
        $writer->close();

        $rows = data_frame()->read(from_parquet($path, filesystem: $memory))->fetch();

        static::assertEquals(
            schema(
                datetime_schema('ts_ms', true),
                datetime_schema('ts_us', true),
                time_schema('t_ms', true),
                time_schema('t_us', true),
                float_schema('dec9', true),
            ),
            $rows->schema(),
        );

        $row = $rows[0];
        $tsMs = $row->get('ts_ms');
        $tsUs = $row->get('ts_us');
        $tMs = $row->get('t_ms');
        $tUs = $row->get('t_us');

        static::assertInstanceOf(DateTimeInterface::class, $tsMs);
        static::assertInstanceOf(DateTimeInterface::class, $tsUs);
        static::assertInstanceOf(DateInterval::class, $tMs);
        static::assertInstanceOf(DateInterval::class, $tUs);
        static::assertSame('2020-01-02 03:04:05.678000 +00:00', $tsMs->format('Y-m-d H:i:s.u P'));
        static::assertSame('2020-01-02 03:04:05.678901 +00:00', $tsUs->format('Y-m-d H:i:s.u P'));
        static::assertSame('03:04:05.678000', $tMs->format('%H:%I:%S.%F'));
        static::assertSame('03:04:05.678901', $tUs->format('%H:%I:%S.%F'));
        static::assertSame(12345.67, $row->get('dec9'));
    }
}
