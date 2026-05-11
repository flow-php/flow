<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\ColumnChunkBuilder\DeltaBinaryPackedColumnChunkBuilder;
use Flow\Parquet\Writer\ColumnChunkContainer;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

final class DeltaBinaryPackedColumnChunkBuilderTest extends TestCase
{
    public static function compression_types_provider(): \Generator
    {
        yield 'uncompressed' => [Compressions::UNCOMPRESSED];
        yield 'gzip' => [Compressions::GZIP];
        yield 'snappy' => [Compressions::SNAPPY];
    }

    public static function page_size_provider(): \Generator
    {
        yield 'small page' => [1024];
        yield 'medium page' => [8192];
        yield 'large page' => [65536];
    }

    public static function physical_types_provider(): \Generator
    {
        yield 'int32' => [PhysicalType::INT32, 42];
        yield 'int64' => [PhysicalType::INT64, 1234567890123];
    }

    public static function writer_version_provider(): \Generator
    {
        yield 'version 1' => [1];
        yield 'version 2' => [2];
    }

    public function test_constructor_rejects_unsupported_types(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Delta encoding only supports INT32 and INT64 physical types');

        new DeltaBinaryPackedColumnChunkBuilder(
            new FlatColumn('test_col', PhysicalType::FLOAT),
            new Options(),
            Compressions::UNCOMPRESSED,
        );
    }

    public function test_empty_builder_properties(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        static::assertSame($column, $builder->column());
        static::assertFalse($builder->isFull());
        static::assertSame(0, $builder->uncompressedSize());
    }

    public function test_flush_cleans_up_builder_state(): void
    {
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $options = new Options();
        $compression = Compressions::UNCOMPRESSED;
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compression);

        $builder->addColumn(new WriteFlatColumnValues($column, [0, 0], [1, 1], [42, 84]));

        static::assertFalse($builder->isEmpty());
        static::assertGreaterThan(0, $builder->uncompressedSize());

        $containers = $builder->flush(0);
        static::assertCount(1, $containers);

        static::assertTrue($builder->isEmpty());
        static::assertEquals(0, $builder->uncompressedSize());

        $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [126]));

        static::assertFalse($builder->isEmpty());
        static::assertGreaterThan(0, $builder->uncompressedSize());
    }

    public function test_flush_empty_builder(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
    }

    public function test_flush_with_data(): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        $values = [1, 2, 3, 4, 5];

        foreach ($values as $value) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$value]));
        }

        $containers = $builder->flush(0);

        static::assertIsArray($containers);
        static::assertCount(1, $containers);
        static::assertInstanceOf(ColumnChunkContainer::class, $containers[0]);
        static::assertEquals(0, $builder->uncompressedSize());
    }

    public function test_is_full_calculation_for_int32(): void
    {
        $options = (new Options())->set(Option::PAGE_SIZE_BYTES, 100)->set(Option::WRITER_VERSION, 2);

        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        for ($i = 0; $i < 30; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$i]));
        }

        static::assertTrue($builder->isFull());
    }

    public function test_is_full_calculation_for_int64(): void
    {
        $options = (new Options())->set(Option::PAGE_SIZE_BYTES, 100)->set(Option::WRITER_VERSION, 2);

        $column = new FlatColumn('test_col', PhysicalType::INT64);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        for ($i = 0; $i < 15; $i++) {
            $builder->addColumn(new WriteFlatColumnValues($column, [0], [1], [$i]));
        }

        static::assertTrue($builder->isFull());
    }

    #[DataProvider('compression_types_provider')]
    public function test_supports_different_compression_types(Compressions $compression): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, $compression);

        static::assertSame($column, $builder->column());
        static::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    #[DataProvider('writer_version_provider')]
    public function test_supports_different_writer_versions(int $version): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, $version);

        $column = new FlatColumn('test_col', PhysicalType::INT32);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        static::assertSame($column, $builder->column());
        static::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }

    #[DataProvider('physical_types_provider')]
    public function test_supports_integer_types(PhysicalType $type, int $value): void
    {
        $options = (new Options())->set(Option::WRITER_VERSION, 2);
        $column = new FlatColumn('test_col', $type);
        $builder = new DeltaBinaryPackedColumnChunkBuilder($column, $options, Compressions::UNCOMPRESSED);

        static::assertSame($column, $builder->column());
        static::assertInstanceOf(DeltaBinaryPackedColumnChunkBuilder::class, $builder);
    }
}
