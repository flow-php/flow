<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\FlatValue;
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\{Options};
use Flow\Parquet\ParquetFile\{Compressions};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\DeltaColumnChunkBuilder;
use PHPUnit\Framework\TestCase;

final class DeltaColumnChunkBuilderTest extends TestCase
{
    public function test_delta_encoding_with_negative_values() : void
    {
        $column = new FlatColumn('negative_col', PhysicalType::INT32);
        $builder = new DeltaColumnChunkBuilder($column, new Options(), Compressions::UNCOMPRESSED);

        $negativeValues = [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8];

        foreach ($negativeValues as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertCount(1, $containers);
        self::assertGreaterThan(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_sequential_values() : void
    {
        $column = new FlatColumn('sequential_col', PhysicalType::INT32);
        $builder = new DeltaColumnChunkBuilder($column, new Options(), Compressions::UNCOMPRESSED);

        $sequentialValues = range(1, 100);

        foreach ($sequentialValues as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertCount(1, $containers);
        self::assertGreaterThan(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_timestamp_sequence() : void
    {
        $column = new FlatColumn('timestamp_col', PhysicalType::INT64);
        $builder = new DeltaColumnChunkBuilder($column, new Options(), Compressions::UNCOMPRESSED);

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
        self::assertGreaterThan(0, $builder->uncompressedSize());

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_workflow() : void
    {
        $column = new FlatColumn('workflow_col', PhysicalType::INT32);
        $builder = new DeltaColumnChunkBuilder($column, new Options(), Compressions::UNCOMPRESSED);

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
        self::assertGreaterThan(0, $uncompressedSize);

        $container = $containers[0];
        self::assertNotEmpty($container->binaryBuffer);
        self::assertNotNull($container->columnChunk);
        self::assertSame($column->type(), $container->columnChunk->type());
    }
}
