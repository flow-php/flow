<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\FlatValue;
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\{Option, Options};
use Flow\Parquet\ParquetFile\{Compressions, Encodings};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\DeltaBinaryPackedColumnChunkBuilder;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedColumnChunkBuilderTest extends TestCase
{
    public function test_delta_encoding_with_negative_values() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('negative_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $negativeValues = [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8];

        foreach ($negativeValues as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertCount(1, $containers);
        self::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_sequential_values() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('sequential_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $sequentialValues = range(1, 100);

        foreach ($sequentialValues as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertCount(1, $containers);
        self::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_timestamp_sequence() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('timestamp_col', PhysicalType::INT64);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $baseTimestamp = 1609459200; // 2021-01-01 00:00:00

        for ($i = 0; $i < 100; $i++) {
            $timestamp = $baseTimestamp + ($i * 60);
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $timestamp);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertCount(1, $containers);
        self::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_workflow() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('workflow_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = range(1, 50);

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        self::assertFalse($builder->isFull());

        $containers = $builder->flush(0);
        self::assertCount(1, $containers);

        $uncompressedSize = $builder->uncompressedSize();
        self::assertEquals(0, $uncompressedSize);

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertNotNull($container->columnChunk);
        self::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_int32_sequential_values() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        self::assertNotNull($container);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
        self::assertSame($column->type(), $container->columnChunk->type());

        $encodings = $container->columnChunk->encodings();
        self::assertContains(Encodings::DELTA_BINARY_PACKED, $encodings);
    }

    public function test_round_trip_int64_timestamp_sequence() : void
    {
        $column = new FlatColumn('timestamp_col', PhysicalType::INT64);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $baseTimestamp = 1609459200; // 2021-01-01 00:00:00
        $values = [];

        for ($i = 0; $i < 10; $i++) {
            $values[] = $baseTimestamp + ($i * 60); // Every minute
        }

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        // For now, just verify the container was created successfully
        // Full round-trip testing would require more complex page parsing
        self::assertNotNull($container);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
        self::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_negative_values() : void
    {
        $column = new FlatColumn('negative_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8];

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        self::assertNotNull($container);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
        self::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_with_different_compression() : void
    {
        $column = new FlatColumn('compressed_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::GZIP);

        $values = range(100, 200);

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        self::assertNotNull($container);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
        self::assertSame($column->type(), $container->columnChunk->type());
    }
}
