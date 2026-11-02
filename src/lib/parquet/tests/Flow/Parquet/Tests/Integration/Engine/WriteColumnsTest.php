<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Engine;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\ValidationException;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\Tests\Context\ColumnDoor;
use Flow\Parquet\Tests\Integration\IO\ParquetIntegrationTestCase;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use PHPUnit\Framework\Attributes\DataProvider;

final class WriteColumnsTest extends ParquetIntegrationTestCase
{
    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_columns_write_the_file_their_temporal_and_uuid_values_write(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $schema = Schema::with(
            FlatColumn::date('d'),
            FlatColumn::dateTime('us'),
            new FlatColumn(
                'ms',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::MILLISECONDS),
                ),
            ),
            new FlatColumn(
                'ns',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                ),
            ),
            FlatColumn::time('t'),
            NestedColumn::list('instants', ListElement::datetime()),
            NestedColumn::struct('s', [FlatColumn::date('day')]),
            FlatColumn::uuid('u'),
            NestedColumn::list('uuids', ListElement::uuid()),
        );
        $instant = new DateTimeImmutable('2009-02-13 23:31:30.123456', new DateTimeZone('UTC'));
        $objects = ColumnDoor::stream();
        $stored = ColumnDoor::stream();

        $writer = ColumnDoor::open($engine, $objects, $schema);
        $writer->writeColumns([
            'd' => [new DateTimeImmutable('2024-02-29', new DateTimeZone('UTC')), null],
            'us' => [$instant, null],
            'ms' => [$instant, null],
            'ns' => [$instant, null],
            't' => [(new DateTimeImmutable('@0'))->diff(new DateTimeImmutable('@3723.000004')), null],
            'instants' => [
                [$instant, new DateTimeImmutable('1969-12-31 23:59:59.999999', new DateTimeZone('UTC'))],
                null,
            ],
            's' => [['day' => new DateTimeImmutable('1969-12-31', new DateTimeZone('UTC'))], null],
            'u' => ['00112233-4455-6677-8899-aabbccddeeff', null],
            'uuids' => [['00112233-4455-6677-8899-aabbccddeeff'], null],
        ]);
        $writer->close();
        $writer = ColumnDoor::open($engine, $stored, $schema);
        $writer->writeStoredColumns([
            'd' => [19_782, null],
            'us' => [1_234_567_890_123_456, null],
            'ms' => [1_234_567_890_123_456, null],
            'ns' => [1_234_567_890_123_456, null],
            't' => [3_723_000_004, null],
            'instants' => [[1_234_567_890_123_456, -1], null],
            's' => [['day' => -1], null],
            'u' => ["\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff", null],
            'uuids' => [["\x00\x11\x22\x33\x44\x55\x66\x77\x88\x99\xaa\xbb\xcc\xdd\xee\xff"], null],
        ]);
        $writer->close();

        static::assertSame($objects->content(), $stored->content());
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_columns_refuse_a_temporal_object(string $engineClass): void
    {
        $writer = ColumnDoor::open(
            ParquetEngineMother::create($engineClass),
            ColumnDoor::stream(),
            Schema::with(FlatColumn::dateTime('us')),
        );

        $this->expectException(ValidationException::class);

        $writer->writeStoredColumns(['us' => [new DateTimeImmutable('2009-02-13 23:31:30')]]);
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_columns_refuse_a_uuid_that_is_not_16_bytes(string $engineClass): void
    {
        $writer = ColumnDoor::open(
            ParquetEngineMother::create($engineClass),
            ColumnDoor::stream(),
            Schema::with(FlatColumn::uuid('u')),
        );

        $this->expectException(ValidationException::class);

        $writer->writeStoredColumns(['u' => ['00112233-4455-6677-8899-aabbccddeeff']]);
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_a_datetime_outside_the_nanosecond_range_is_refused_as_an_invalid_argument(string $engineClass): void
    {
        $writer = ColumnDoor::open(
            ParquetEngineMother::create($engineClass),
            ColumnDoor::stream(),
            Schema::with(
                new FlatColumn(
                    'ns',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                    ),
                ),
            ),
        );

        $this->expectException(InvalidArgumentException::class);

        $writer->writeColumns(['ns' => [new DateTimeImmutable('2300-01-01', new DateTimeZone('UTC'))]]);
        $writer->close();
    }

    /**
     * @param class-string<ParquetEngine> $engineClass
     */
    #[DataProvider('engine_provider')]
    public function test_stored_columns_refuse_an_instant_outside_the_nanosecond_range(string $engineClass): void
    {
        $writer = ColumnDoor::open(
            ParquetEngineMother::create($engineClass),
            ColumnDoor::stream(),
            Schema::with(
                new FlatColumn(
                    'ns',
                    PhysicalType::INT64,
                    logicalType: new LogicalType(
                        LogicalType::TIMESTAMP,
                        timestamp: new Timestamp(true, TimeUnit::NANOSECONDS),
                    ),
                ),
            ),
        );

        $this->expectException(InvalidArgumentException::class);

        $writer->writeStoredColumns(['ns' => [9_300_000_000_000_000]]);
        $writer->close();
    }
}
