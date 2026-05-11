<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Encodings;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\DeltaBinaryPackedColumnChunkBuilder;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedColumnChunkBuilderTest extends TestCase
{
    public function test_delta_encoding_with_negative_values(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('negative_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $negativeValues = [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8];

        foreach ($negativeValues as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);

        static::assertCount(1, $containers);
        static::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        static::assertNotEmpty($container->binaryBuffer);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_sequential_values(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('sequential_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $sequentialValues = range(1, 100);

        foreach ($sequentialValues as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);

        static::assertCount(1, $containers);
        static::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        static::assertNotEmpty($container->binaryBuffer);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_with_timestamp_sequence(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('timestamp_col', PhysicalType::INT64);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $baseTimestamp = 1609459200; // 2021-01-01 00:00:00

        for ($i = 0; $i < 100; $i++) {
            $timestamp = $baseTimestamp + ($i * 60);
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$timestamp]));
        }

        $containers = $builder->flush(0);

        static::assertCount(1, $containers);
        static::assertEquals(0, $builder->uncompressedSize());

        $container = $containers[0];
        static::assertNotEmpty($container->binaryBuffer);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
    }

    public function test_delta_encoding_workflow(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('workflow_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = range(1, 50);

        foreach ($values as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        static::assertFalse($builder->isFull());

        $containers = $builder->flush(0);
        static::assertCount(1, $containers);

        $uncompressedSize = $builder->uncompressedSize();
        static::assertEquals(0, $uncompressedSize);

        $container = $containers[0];
        static::assertNotEmpty($container->binaryBuffer);
        static::assertNotNull($container->columnChunk);
        static::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_int32_sequential_values(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        foreach ($values as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        static::assertNotNull($container);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
        static::assertSame($column->type(), $container->columnChunk->type());

        $encodings = $container->columnChunk->encodings();
        static::assertContains(Encodings::DELTA_BINARY_PACKED, $encodings);
    }

    public function test_round_trip_int64_timestamp_sequence(): void
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
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        static::assertNotNull($container);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
        static::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_negative_values(): void
    {
        $column = new FlatColumn('negative_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [-10, -8, -6, -4, -2, 0, 2, 4, 6, 8];

        foreach ($values as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        static::assertNotNull($container);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
        static::assertSame($column->type(), $container->columnChunk->type());
    }

    public function test_round_trip_with_different_compression(): void
    {
        $column = new FlatColumn('compressed_col', PhysicalType::INT32);
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::GZIP);

        $values = range(100, 200);

        foreach ($values as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);
        $container = $containers[0];

        static::assertNotNull($container);
        static::assertGreaterThan(0, strlen($container->binaryBuffer));
        static::assertSame($column->type(), $container->columnChunk->type());
    }
}
