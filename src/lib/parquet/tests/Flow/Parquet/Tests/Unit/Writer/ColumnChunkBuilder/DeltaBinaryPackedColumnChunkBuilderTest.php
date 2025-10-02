<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\FlatValue;
use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\Option;
use Flow\Parquet\{Options};
use Flow\Parquet\ParquetFile\{Compressions};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, PhysicalType};
use Flow\Parquet\Writer\ColumnChunkBuilder\DeltaBinaryPackedColumnChunkBuilder;
use Flow\Parquet\Writer\{ColumnChunkContainer};
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedColumnChunkBuilderTest extends TestCase
{
    public static function compression_types_provider() : \Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function page_size_provider() : \Generator
    {
        yield 'small page' => [1024];
        yield 'medium page' => [8192];
        yield 'large page' => [65536];
    }

    public static function physical_types_provider() : \Generator
    {
        yield 'int32' => [PhysicalType::INT32, 42];
        yield 'int64' => [PhysicalType::INT64, 1234567890123];
    }

    public static function writer_version_provider() : \Generator
    {
        yield 'version 1' => [1];
        yield 'version 2' => [2];
    }

    public function test_constructor_rejects_unsupported_types() : void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Delta encoding only supports INT32 and INT64 physical types');

        new DeltaBinaryPackedColumnChunkBuilder(
            new FlatColumn('test_col', PhysicalType::FLOAT),
            new Options(),
            Compressions::UNCOMPRESSED
        );
    }

    public function test_empty_builder_properties() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        self::assertSame($column, $builder->column());
        self::assertFalse($builder->isFull());
        self::assertSame(0, $builder->uncompressedSize());
    }

    public function test_flush_cleans_up_builder_state() : void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compression);

        $columnData = WriteColumnData::initialize($column);
        $flatValue1 = new FlatValue($column, 0, 1, 42);
        $flatValue2 = new FlatValue($column, 0, 1, 84);
        $columnData->addValue($flatValue1, $flatValue2);
        $builder->addRow($columnData);

        self::assertFalse($builder->isEmpty());
        self::assertGreaterThan(0, $builder->uncompressedSize());

        $containers = $builder->flush(0);
        self::assertCount(1, $containers);

        self::assertTrue($builder->isEmpty());
        self::assertEquals(0, $builder->uncompressedSize());

        $columnData2 = WriteColumnData::initialize($column);
        $flatValue3 = new FlatValue($column, 0, 1, 126);
        $columnData2->addValue($flatValue3);
        $builder->addRow($columnData2);

        self::assertFalse($builder->isEmpty());
        self::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_flush_empty_builder() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_flush_with_data() : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [1, 2, 3, 4, 5];

        foreach ($values as $value) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $value);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        $containers = $builder->flush(0);

        self::assertIsArray($containers);
        self::assertCount(1, $containers);
        self::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        // After flush, builder should be clean (no leftovers)
        self::assertEquals(0, $builder->uncompressedSize());
    }

    public function test_is_full_calculation_for_int32() : void
    {
        $options = (new Options())
            ->set(Option::PAGE_SIZE_BYTES, 100)
            ->set(Option::WRITER_VERSION, 2);

        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        // Add enough INT32 values to exceed page size (100 bytes / 4 bytes per int = 25 values)
        for ($i = 0; $i < 30; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        self::assertTrue($builder->isFull());
    }

    public function test_is_full_calculation_for_int64() : void
    {
        $options = (new Options())
            ->set(Option::PAGE_SIZE_BYTES, 100)
            ->set(Option::WRITER_VERSION, 2);

        $column = new FlatColumn('test_col', PhysicalType::INT64);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        // Add enough INT64 values to exceed page size (100 bytes / 8 bytes per int = 12.5, so 13 values)
        for ($i = 0; $i < 15; $i++) {
            $columnData = WriteColumnData::initialize($column);
            $flatValue = new FlatValue($column, 0, 1, $i);
            $columnData->addValue($flatValue);
            $builder->addRow($columnData);
        }

        self::assertTrue($builder->isFull());
    }

    #[DataProvider('compression_types_provider')]
    public function test_supports_different_compression_types(Compressions $compression) : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compression);

        self::assertSame($column, $builder->column());
        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    #[DataProvider('writer_version_provider')]
    public function test_supports_different_writer_versions(int $version) : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, $version);

        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        self::assertSame($column, $builder->column());
        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    #[DataProvider('physical_types_provider')]
    public function test_supports_integer_types(PhysicalType $type, int $value) : void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', $type);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        self::assertSame($column, $builder->column());
        self::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }
}
